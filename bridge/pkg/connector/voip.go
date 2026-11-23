package connector

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/purpshell/meowcaller"
	"go.mau.fi/whatsmeow/types"

	"go.mau.fi/mautrix-whatsapp/pkg/connector/voipbridge"
)

const groupVideoSourceHold = 2 * time.Second

var (
	ErrVoIPDisabled       = errors.New("WhatsApp VoIP adapter is disabled")
	ErrVoIPCallNotFound   = errors.New("WhatsApp VoIP call not found")
	ErrVoIPMediaNotReady  = errors.New("attach a MatrixRTC media transport before accepting a WhatsApp call")
	ErrVoIPSelfCall       = errors.New("calls to your own WhatsApp account are not supported")
	ErrVoIPPeerUncallable = errors.New("WhatsApp reports that calls to this destination are disabled")
)

// initExperimentalVoIP is called from LoadUserLogin only before whatsmeow
// connects. Changing the active client underneath the hooks is not supported.
func (wa *WhatsAppClient) initExperimentalVoIP() {
	client := meowcaller.NewClient(wa.Client, meowcaller.WithFixedRoomBinding())
	if err := client.VerifyCallAdapter(); err != nil {
		wa.UserLogin.Log.Error().Err(err).Msg("WhatsApp VoIP hook is incompatible with this whatsmeow build; adapter unavailable")
		return
	}
	wa.voipCalls = make(map[string]*meowcaller.Call)
	wa.voipMedia = make(map[string]*voipbridge.MediaBus)
	wa.voipGatewayCancel = make(map[string]context.CancelFunc)
	wa.voipClient = client
	client.OnIncomingCall(func(call *meowcaller.Call) {
		if wa.IsOwnJID(call.Peer().ToNonAD()) {
			wa.UserLogin.Log.Debug().Str("call_id", call.ID()).Msg("Ignoring reflected Note-to-Self WhatsApp call")
			go func() { _ = call.Reject() }()
			return
		}
		wa.registerVoIPCall(call, voipbridge.Incoming)
		if wa.Main.Config.RTCGatewaySocket != "" {
			wa.dispatchIncomingToRTCGateway(call)
		}
		wa.UserLogin.Log.Info().Str("call_id", call.ID()).
			Msg("Incoming WhatsApp VoIP session received; dispatching to the configured MatrixRTC gateway")
	})
	if wa.Main.Config.RTCGatewaySocket == "" {
		wa.UserLogin.Log.Warn().Msg("Experimental WhatsApp VoIP adapter installed without MatrixRTC gateway; calls will remain disabled")
	} else {
		wa.UserLogin.Log.Info().Str("socket", wa.Main.Config.RTCGatewaySocket).Msg("Experimental WhatsApp VoIP adapter connected to MatrixRTC gateway path")
	}
}

func (wa *WhatsAppClient) callKey(callID string) voipbridge.ID {
	return voipbridge.ID{LoginID: string(wa.UserLogin.ID), CallID: callID}
}

func (wa *WhatsAppClient) registerVoIPCall(call *meowcaller.Call, direction voipbridge.Direction) {
	if call == nil || call.ID() == "" {
		return
	}
	wa.voipCallsMu.Lock()
	if wa.voipCalls == nil {
		wa.voipCalls = make(map[string]*meowcaller.Call)
	}
	if wa.voipMedia == nil {
		wa.voipMedia = make(map[string]*voipbridge.MediaBus)
	}
	wa.voipCalls[call.ID()] = call
	wa.voipCallsMu.Unlock()

	id := wa.callKey(call.ID())
	if wa.callManager != nil {
		if _, err := wa.callManager.Start(id, call.Peer().ToNonAD().String(), direction); err != nil {
			wa.UserLogin.Log.Debug().Err(err).Str("call_id", call.ID()).Msg("Could not track live call")
		}
	}
	call.OnStateChange(func(phase meowcaller.CallPhase) {
		if wa.callManager == nil {
			return
		}
		switch phase {
		case meowcaller.CallPhaseConnecting:
			_, _ = wa.callManager.Transition(id, voipbridge.Connecting)
		case meowcaller.CallPhaseActive:
			_, _ = wa.callManager.Transition(id, voipbridge.Connecting)
			_, _ = wa.callManager.Transition(id, voipbridge.Active)
		case meowcaller.CallPhaseEnded:
			_, _ = wa.callManager.End(id, "whatsapp-ended")
		}
	})
	call.OnReady(func() {
		if wa.callManager != nil {
			_, _ = wa.callManager.Transition(id, voipbridge.Connecting)
			_, _ = wa.callManager.Transition(id, voipbridge.Active)
		}
	})
	call.OnEnd(func(reason string) {
		peer := call.Peer().ToNonAD()
		if !call.IsGroup() && wa.rememberMatrixRTCCallEnd(peer, reason) {
			go wa.refreshMatrixRTCCallPermissionForJID(context.Background(), peer)
		}
		wa.voipCallsMu.Lock()
		delete(wa.voipCalls, call.ID())
		bus := wa.voipMedia[call.ID()]
		delete(wa.voipMedia, call.ID())
		stopGateway := wa.voipGatewayCancel[call.ID()]
		delete(wa.voipGatewayCancel, call.ID())
		wa.voipCallsMu.Unlock()
		if stopGateway != nil {
			stopGateway()
		}
		if bus != nil {
			_ = bus.Close()
		}
		if wa.callManager != nil {
			_, _ = wa.callManager.End(id, reason)
		}
	})
}

// WhatsAppVoIPCall returns a handle to an actual WhatsApp call. It does not
// advertise a MatrixRTC call or generate SFU/E2EE keys by itself.
func (wa *WhatsAppClient) WhatsAppVoIPCall(callID string) (*meowcaller.Call, bool) {
	wa.voipCallsMu.RLock()
	defer wa.voipCallsMu.RUnlock()
	call, ok := wa.voipCalls[callID]
	return call, ok
}

// AttachVoIPMedia creates a bounded, bidirectional PCM/H.264 handoff for a
// future MatrixRTC transport. The transport must start before AnswerVoIPCall.
func (wa *WhatsAppClient) AttachVoIPMedia(callID string) (*voipbridge.MediaBus, error) {
	wa.voipCallsMu.Lock()
	call, ok := wa.voipCalls[callID]
	if !ok {
		wa.voipCallsMu.Unlock()
		return nil, ErrVoIPCallNotFound
	}
	if existing := wa.voipMedia[callID]; existing != nil {
		wa.voipCallsMu.Unlock()
		return existing, nil
	}
	bus := voipbridge.NewMediaBus(voipbridge.DefaultJitterFrames)
	wa.voipMedia[callID] = bus
	wa.voipCallsMu.Unlock()

	call.Receive(bus)
	selector := &groupVideoSourceSelector{started: time.Now()}
	call.OnParticipantVideoFrame(func(frame meowcaller.ParticipantVideoFrame) {
		selector.forward(frame, time.Now(), bus)
	})
	call.Play(bus)
	return bus, nil
}

// AnswerVoIPCall refuses to accept a WhatsApp call until there is a media
// handoff. A real MatrixRTC client still needs to be wired to that handoff.
func (wa *WhatsAppClient) AnswerVoIPCall(callID string) error {
	wa.voipCallsMu.RLock()
	call, found := wa.voipCalls[callID]
	media := wa.voipMedia[callID]
	wa.voipCallsMu.RUnlock()
	if !found {
		return ErrVoIPCallNotFound
	}
	if media == nil {
		return ErrVoIPMediaNotReady
	}
	return call.Answer()
}

func (wa *WhatsAppClient) RejectVoIPCall(callID string) error {
	call, found := wa.WhatsAppVoIPCall(callID)
	if !found {
		return ErrVoIPCallNotFound
	}
	return call.Reject()
}

func (wa *WhatsAppClient) HangupVoIPCall(callID string) error {
	call, found := wa.WhatsAppVoIPCall(callID)
	if !found {
		return ErrVoIPCallNotFound
	}
	return call.Hangup()
}

func (wa *WhatsAppClient) SendVoIPVideo(callID string, encodedH264AnnexB []byte) error {
	return wa.SendVoIPVideoWithDuration(callID, encodedH264AnnexB, 0)
}

func (wa *WhatsAppClient) SendVoIPVideoWithDuration(callID string, encodedH264AnnexB []byte, duration time.Duration) error {
	call, found := wa.WhatsAppVoIPCall(callID)
	if !found {
		return ErrVoIPCallNotFound
	}
	return call.SendVideoWithDuration(encodedH264AnnexB, duration)
}

// StartVoIPCall initiates a WhatsApp call only when deliberately invoked by a
// future MatrixRTC signalling gateway. It will never run for chat messages.
func (wa *WhatsAppClient) StartVoIPCall(ctx context.Context, peer string, video bool) (*meowcaller.Call, *voipbridge.MediaBus, error) {
	if wa.voipClient == nil {
		return nil, nil, ErrVoIPDisabled
	}
	if peer == "" {
		return nil, nil, fmt.Errorf("WhatsApp VoIP peer must not be empty")
	}
	peerJID, err := types.ParseJID(peer)
	if err != nil {
		return nil, nil, fmt.Errorf("invalid WhatsApp VoIP peer: %w", err)
	}
	peerJID = peerJID.ToNonAD()
	if wa.IsOwnJID(peerJID) {
		return nil, nil, ErrVoIPSelfCall
	}
	if disabled, source := wa.matrixRTCCallHardDisabled(ctx, peerJID); disabled {
		go wa.refreshMatrixRTCCallPermissionForJID(context.Background(), peerJID)
		if source == "" {
			return nil, nil, ErrVoIPPeerUncallable
		}
		return nil, nil, fmt.Errorf("%w (%s)", ErrVoIPPeerUncallable, source)
	}
	call, err := wa.voipClient.CallWithOptions(ctx, peer, meowcaller.CallOptions{Video: video})
	if err != nil {
		return nil, nil, err
	}
	wa.registerVoIPCall(call, voipbridge.Outgoing)
	bus, err := wa.AttachVoIPMedia(call.ID())
	if err != nil {
		_ = call.Hangup()
		return nil, nil, err
	}
	return call, bus, nil
}

// StartVoIPGroupCall places a WhatsApp group call bound to an existing group
// portal. The group roster is resolved by meowcaller from the canonical @g.us
// JID; MatrixRTC represents the WhatsApp side as one aggregate bridge participant.
func (wa *WhatsAppClient) StartVoIPGroupCall(ctx context.Context, groupJID string, video bool, targets ...string) (*meowcaller.Call, *voipbridge.MediaBus, error) {
	if wa.voipClient == nil {
		return nil, nil, ErrVoIPDisabled
	}
	jid, err := types.ParseJID(groupJID)
	if err != nil || jid.Server != types.GroupServer || jid.User == "" {
		return nil, nil, fmt.Errorf("invalid WhatsApp group JID %q", groupJID)
	}
	call, err := wa.voipClient.GroupCallByIDWithOptions(ctx, jid.ToNonAD().String(), meowcaller.GroupCallOptions{
		GroupJID: jid.ToNonAD().String(), AvailableOnly: true, Targets: targets,
		Video: video,
	})
	if err != nil {
		return nil, nil, err
	}
	wa.registerVoIPCall(call, voipbridge.Outgoing)
	bus, err := wa.AttachVoIPMedia(call.ID())
	if err != nil {
		_ = call.Hangup()
		return nil, nil, err
	}
	return call, bus, nil
}

func (wa *WhatsAppClient) teardownVoIP() {
	wa.voipCallsMu.Lock()
	calls := wa.voipCalls
	media := wa.voipMedia
	cancelGateway := wa.voipGatewayCancel
	wa.voipGatewayCancel = make(map[string]context.CancelFunc)
	wa.voipCalls = make(map[string]*meowcaller.Call)
	wa.voipMedia = make(map[string]*voipbridge.MediaBus)
	wa.voipCallsMu.Unlock()
	for _, cancel := range cancelGateway {
		cancel()
	}
	for _, bus := range media {
		_ = bus.Close()
	}
	for _, call := range calls {
		if call.State() != meowcaller.CallPhaseEnded {
			if err := call.Hangup(); err != nil {
				wa.UserLogin.Log.Debug().Err(err).Str("call_id", call.ID()).Msg("Failed to end WhatsApp VoIP call on disconnect")
			}
		}
	}
}
