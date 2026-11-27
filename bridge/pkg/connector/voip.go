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
	ErrVoIPDisconnected   = errors.New("WhatsApp VoIP connection changed during call setup")
	ErrVoIPDisabled       = errors.New("WhatsApp VoIP adapter is disabled")
	ErrVoIPCallNotFound   = errors.New("WhatsApp VoIP call not found")
	ErrVoIPMediaNotReady  = errors.New("attach a MatrixRTC media transport before accepting a WhatsApp call")
	ErrVoIPSelfCall       = errors.New("calls to your own WhatsApp account are not supported")
	ErrVoIPPeerUncallable = errors.New("WhatsApp reports that calls to this destination are disabled")
)

// initExperimentalVoIP is called from LoadUserLogin only before whatsmeow
// connects. Changing the active client underneath the hooks is not supported.
func (wa *WhatsAppClient) initExperimentalVoIP() {
	client := meowcaller.NewClient(wa.Client, meowcaller.WithFixedRoomBinding(), meowcaller.WithLogger(wa.UserLogin.Log.With().Str("component", "meowcaller").Logger()))
	if err := client.VerifyCallAdapter(); err != nil {
		wa.UserLogin.Log.Error().Err(err).Msg("WhatsApp VoIP hook is incompatible with this whatsmeow build; adapter unavailable")
		return
	}
	wa.voipCalls = make(map[string]*meowcaller.Call)
	wa.voipReactionFeeds = make(map[string]*rtcReactionFeed)
	wa.voipMedia = make(map[string]*voipbridge.MediaBus)
	wa.voipGatewayCancel = make(map[string]context.CancelFunc)
	wa.voipClient = client
	// Scope both callbacks to the generation at offer ingress, not to the
	// generation observed after potentially blocking provider preparation.
	rawClient := wa.Client
	client.SetIncomingOfferFactory(func() (func(*meowcaller.Call) bool, func(*meowcaller.Call)) {
		generation, open := wa.voipOfferGeneration()
		admit := func(call *meowcaller.Call) bool {
			return open && rawClient.IsConnected() && rawClient.IsLoggedIn() &&
				wa.voipAdmissionCurrent(generation) && !wa.IsOwnJID(rtcRoomPeerJID(call.Peer())) &&
				incomingRTCSocketPresent(wa.Main.Config.RTCGatewaySocket)
		}
		return admit, func(call *meowcaller.Call) {
			// Never send a remote decline for local unavailability.
			if !admit(call) || !wa.registerVoIPCall(call, voipbridge.Incoming, generation) {
				call.DismissUnanswered()
				return
			}
			wa.dispatchIncomingToRTCGateway(call)
			wa.UserLogin.Log.Info().Str("call_id", call.ID()).
				Msg("Incoming WhatsApp VoIP session received; dispatching to the configured MatrixRTC gateway")
		}
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

func (wa *WhatsAppClient) currentVoIPGeneration() uint64 {
	wa.voipCallsMu.RLock()
	defer wa.voipCallsMu.RUnlock()
	return wa.voipGeneration
}

// Admission is independent of ordinary message dispatch. A disconnect closes
// it until a subsequent connected event; an explicitly stopped login cannot
// be reopened by a late connected callback.
func (wa *WhatsAppClient) allowVoIPReconnect() {
	wa.voipCallsMu.Lock()
	wa.voipExplicitStop = false
	wa.voipCallsMu.Unlock()
}

func (wa *WhatsAppClient) openVoIPAdmission() {
	wa.voipCallsMu.Lock()
	if !wa.voipExplicitStop && !wa.voipAdmissionOpen {
		wa.voipAdmissionOpen = true
	}
	wa.voipCallsMu.Unlock()
}

func (wa *WhatsAppClient) stopVoIPAdmission() {
	wa.voipCallsMu.Lock()
	wa.voipAdmissionOpen = false
	wa.voipExplicitStop = true
	wa.cancelVoIPConnectionLocked()
	for attempt := range wa.voipDialAttempts {
		attempt.cancel()
	}
	wa.voipCallsMu.Unlock()
}

// Caller holds voipCallsMu. Connection-owned call work is separate from
// refresh batches, which may be restarted by duplicate Connected events.
func (wa *WhatsAppClient) cancelVoIPConnectionLocked() {
	for work := range wa.voipConnectionWork {
		work.cancel()
	}
	wa.voipConnectionWork = nil
	wa.rtcProbeLifetime.stop()
	wa.rtcReconnectRefresh.stop()
}

// The whole outgoing gateway (including pre-dial admission) belongs to the
// original connection. A reconnect must not revive it.
func (wa *WhatsAppClient) bindVoIPConnection(parent context.Context) (context.Context, context.CancelFunc, bool) {
	wa.voipCallsMu.Lock()
	defer wa.voipCallsMu.Unlock()
	if !wa.voipAdmissionOpen || parent.Err() != nil {
		return nil, nil, false
	}
	ctx, cancel := context.WithCancel(parent)
	work := &voipDialAttempt{cancel: cancel}
	if wa.voipConnectionWork == nil {
		wa.voipConnectionWork = make(map[*voipDialAttempt]struct{})
	}
	wa.voipConnectionWork[work] = struct{}{}
	return ctx, func() {
		cancel()
		wa.voipCallsMu.Lock()
		delete(wa.voipConnectionWork, work)
		wa.voipCallsMu.Unlock()
	}, true
}

// Serialize refresh-scope installation with terminal admission closure.
// The schedule callback may only enqueue work, not wait for it.
func (wa *WhatsAppClient) startRTCConnectionWork(parent context.Context, schedule func(context.Context)) {
	wa.voipCallsMu.Lock()
	defer wa.voipCallsMu.Unlock()
	if !wa.voipAdmissionOpen || wa.voipExplicitStop {
		return
	}
	schedule(wa.rtcProbeLifetime.start(parent))
}

func (wa *WhatsAppClient) voipOfferGeneration() (uint64, bool) {
	wa.voipCallsMu.RLock()
	defer wa.voipCallsMu.RUnlock()
	return wa.voipGeneration, wa.voipAdmissionOpen
}

func (wa *WhatsAppClient) voipAdmissionCurrent(generation uint64) bool {
	wa.voipCallsMu.RLock()
	defer wa.voipCallsMu.RUnlock()
	return wa.voipAdmissionOpen && wa.voipGeneration == generation
}

// A dial attempt owns only its setup context, not the resulting call lifetime.
type voipDialAttempt struct {
	cancel context.CancelFunc
}

func (wa *WhatsAppClient) beginVoIPDial(parent context.Context) (context.Context, uint64, func()) {
	ctx, cancel := context.WithCancel(parent)
	attempt := &voipDialAttempt{cancel: cancel}
	wa.voipCallsMu.Lock()
	generation := wa.voipGeneration
	if !wa.voipAdmissionOpen {
		wa.voipCallsMu.Unlock()
		cancel()
		return ctx, generation, cancel
	}
	if wa.voipDialAttempts == nil {
		wa.voipDialAttempts = make(map[*voipDialAttempt]struct{})
	}
	wa.voipDialAttempts[attempt] = struct{}{}
	wa.voipCallsMu.Unlock()
	return ctx, generation, func() {
		cancel()
		wa.voipCallsMu.Lock()
		delete(wa.voipDialAttempts, attempt)
		wa.voipCallsMu.Unlock()
	}
}

// Caller holds voipCallsMu. The generation must match the one captured
// before the provider dial or incoming callback started.
func (wa *WhatsAppClient) tryRegisterVoIPCallLocked(callID string, call *meowcaller.Call, generation uint64) bool {
	if generation != wa.voipGeneration || call == nil || callID == "" || call.State() == meowcaller.CallPhaseEnded || wa.voipCalls[callID] != nil {
		return false
	}
	if wa.voipCalls == nil {
		wa.voipCalls = make(map[string]*meowcaller.Call)
	}
	if wa.voipMedia == nil {
		wa.voipMedia = make(map[string]*voipbridge.MediaBus)
	}
	wa.voipCalls[callID] = call
	if wa.voipReactionFeeds == nil {
		wa.voipReactionFeeds = make(map[string]*rtcReactionFeed)
	}
	wa.voipReactionFeeds[callID] = newRTCReactionFeed()
	return true
}

func (wa *WhatsAppClient) registerVoIPCall(call *meowcaller.Call, direction voipbridge.Direction, generation uint64) bool {
	if call == nil {
		return false
	}
	wa.voipCallsMu.Lock()
	if !wa.voipAdmissionOpen || !wa.tryRegisterVoIPCallLocked(call.ID(), call, generation) {
		wa.voipCallsMu.Unlock()
		return false
	}

	id := wa.callKey(call.ID())
	if wa.callManager != nil {
		wa.callManager.PruneEnded(24 * time.Hour)
		if _, err := wa.callManager.Start(id, call.Peer().ToNonAD().String(), direction); err != nil {
			wa.UserLogin.Log.Debug().Err(err).Str("call_id", call.ID()).Msg("Could not track live call")
		}
	}
	feed := wa.voipReactionFeeds[call.ID()]
	wa.voipCallsMu.Unlock()
	if feed != nil {
		call.OnReaction(func(reaction meowcaller.CallReaction) { wa.receiveRTCReaction(call, feed, reaction) })
	}
	call.OnStateChange(func(phase meowcaller.CallPhase) {
		wa.voipCallsMu.RLock()
		defer wa.voipCallsMu.RUnlock()
		if wa.voipCalls[call.ID()] != call || wa.callManager == nil {
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
		wa.voipCallsMu.RLock()
		defer wa.voipCallsMu.RUnlock()
		if wa.voipCalls[call.ID()] == call && wa.callManager != nil {
			_, _ = wa.callManager.Transition(id, voipbridge.Connecting)
			_, _ = wa.callManager.Transition(id, voipbridge.Active)
		}
	})
	call.OnEnd(func(reason string) {
		wa.UserLogin.Log.Info().Str("call_id", call.ID()).Str("reason", reason).Msg("WhatsApp VoIP session ended")
		wa.voipCallsMu.Lock()
		if wa.voipCalls[call.ID()] != call {
			wa.voipCallsMu.Unlock()
			return
		}
		delete(wa.voipCalls, call.ID())
		delete(wa.voipReactionFeeds, call.ID())
		bus := wa.voipMedia[call.ID()]
		delete(wa.voipMedia, call.ID())
		stopGateway := wa.voipGatewayCancel[call.ID()]
		delete(wa.voipGatewayCancel, call.ID())
		if wa.callManager != nil {
			_, _ = wa.callManager.End(id, reason)
		}
		wa.voipCallsMu.Unlock()
		peer := call.Peer().ToNonAD()
		if _, link := call.WaitingRoomState(); !link && !call.IsGroup() && wa.rememberMatrixRTCCallEnd(peer, reason) {
			matrixRTCCallAvailabilityCache.requestReconcile(string(wa.UserLogin.ID))
		}
		if stopGateway != nil {
			stopGateway()
		}
		if bus != nil {
			_ = bus.Close()
		}
	})
	return true
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
	call.OnParticipantMediaEpoch(func(epoch meowcaller.ParticipantMediaEpoch) {
		bus.ReconcileParticipantEpoch(epoch.RosterTransaction, epoch.KeyTransaction, epoch.Valid)
	})
	call.OnGroupState(func(state meowcaller.GroupCallState) { wa.reconcileParticipantRoster(bus, state) })
	call.OnParticipantAudioFrame(func(frame meowcaller.ParticipantAudioFrame) {
		state, group := call.GroupState()
		if !group {
			return
		}
		wa.reconcileParticipantRoster(bus, state)
		if !frame.Epoch.Valid || frame.Epoch.RosterTransaction != state.TransactionID {
			return
		}
		bus.PublishParticipantMedia(frame.Epoch.RosterTransaction, frame.Epoch.KeyTransaction, frame.PID, frame.HasPID, voipbridge.ParticipantMediaKey{ParticipantID: frame.ParticipantID, Device: frame.Device.String(), SSRC: frame.SSRC}, voipbridge.ParticipantMediaPacket{Timestamp: frame.Timestamp, ClockRate: frame.RTPClockRate, PCM: frame.PCM})
	})
	call.OnParticipantVideoFrame(func(frame meowcaller.ParticipantVideoFrame) {
		state, group := call.GroupState()
		if group {
			wa.reconcileParticipantRoster(bus, state)
		}
		if group && frame.Epoch.Valid && frame.Epoch.RosterTransaction == state.TransactionID && bus.PublishParticipantMedia(frame.Epoch.RosterTransaction, frame.Epoch.KeyTransaction, frame.PID, frame.HasPID, voipbridge.ParticipantMediaKey{ParticipantID: frame.ParticipantID, Device: frame.Device.String(), SSRC: frame.SSRC, Video: true}, voipbridge.ParticipantMediaPacket{Timestamp: frame.Timestamp, ClockRate: 90000, H264: frame.AccessUnit, Orientation: frame.Orientation, ScreenShare: frame.ScreenShare, RoleRevision: frame.RoleRevision}) && frame.RequestKeyframe != nil {
			frame.RequestKeyframe()
		}
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
	ctx, generation, finishDial := wa.beginVoIPDial(ctx)
	defer finishDial()
	if wa.voipClient == nil {
		return nil, nil, ErrVoIPDisabled
	}
	if !wa.voipAdmissionCurrent(generation) || ctx.Err() != nil {
		return nil, nil, ErrVoIPDisconnected
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
		matrixRTCCallAvailabilityCache.requestReconcile(string(wa.UserLogin.ID))
		if source == "" {
			return nil, nil, ErrVoIPPeerUncallable
		}
		return nil, nil, fmt.Errorf("%w (%s)", ErrVoIPPeerUncallable, source)
	}
	if !wa.voipAdmissionCurrent(generation) || ctx.Err() != nil {
		return nil, nil, ErrVoIPDisconnected
	}
	call, err := wa.voipClient.CallWithOptions(ctx, peer, meowcaller.CallOptions{Video: video})
	if err != nil {
		return nil, nil, err
	}
	if ctx.Err() != nil || !wa.registerVoIPCall(call, voipbridge.Outgoing, generation) {
		if call != nil {
			_ = call.Hangup()
		}
		return nil, nil, ErrVoIPDisconnected
	}
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
	ctx, generation, finishDial := wa.beginVoIPDial(ctx)
	defer finishDial()
	if wa.voipClient == nil {
		return nil, nil, ErrVoIPDisabled
	}
	if !wa.voipAdmissionCurrent(generation) || ctx.Err() != nil {
		return nil, nil, ErrVoIPDisconnected
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
	if ctx.Err() != nil || !wa.registerVoIPCall(call, voipbridge.Outgoing, generation) {
		if call != nil {
			_ = call.Hangup()
		}
		return nil, nil, ErrVoIPDisconnected
	}
	bus, err := wa.AttachVoIPMedia(call.ID())
	if err != nil {
		_ = call.Hangup()
		return nil, nil, err
	}
	return call, bus, nil
}

// startVoIPAdHocGroupCall uses the existing room only as the Matrix authorization
// context. The WhatsApp offer has no group JID, and can never expand the roster.
func (wa *WhatsAppClient) startVoIPAdHocGroupCall(ctx context.Context, groupJID string, video bool, targets []string) (*meowcaller.Call, *voipbridge.MediaBus, error) {
	ctx, generation, finishDial := wa.beginVoIPDial(ctx)
	defer finishDial()
	if wa.voipClient == nil {
		return nil, nil, ErrVoIPDisabled
	}
	if !wa.voipAdmissionCurrent(generation) || ctx.Err() != nil {
		return nil, nil, ErrVoIPDisconnected
	}
	call, err := wa.voipClient.AdHocGroupCallFromRoster(ctx, groupJID, targets, video)
	if err != nil {
		return nil, nil, err
	}
	if ctx.Err() != nil || !wa.registerVoIPCall(call, voipbridge.Outgoing, generation) {
		if call != nil {
			_ = call.Hangup()
		}
		return nil, nil, ErrVoIPDisconnected
	}
	bus, err := wa.AttachVoIPMedia(call.ID())
	if err != nil {
		_ = call.Hangup()
		return nil, nil, err
	}
	return call, bus, nil
}

// detachVoIPRegistry terminates the old generation's tracked sessions before
// another registration can acquire the registry lock. External callbacks and
// provider operations are deliberately performed after releasing that lock.
func (wa *WhatsAppClient) detachVoIPRegistry(loginID string) (map[string]*meowcaller.Call, map[string]*voipbridge.MediaBus, map[string]context.CancelFunc) {
	return wa.detachVoIPRegistryForConnectionEnd(loginID, false)
}

func (wa *WhatsAppClient) detachVoIPRegistryForConnectionEnd(loginID string, replaced bool) (map[string]*meowcaller.Call, map[string]*voipbridge.MediaBus, map[string]context.CancelFunc) {
	wa.voipCallsMu.Lock()
	defer wa.voipCallsMu.Unlock()
	wa.voipGeneration++
	wa.voipAdmissionOpen = false
	if replaced {
		wa.voipExplicitStop = true
	}
	wa.cancelVoIPConnectionLocked()
	for attempt := range wa.voipDialAttempts {
		attempt.cancel()
	}
	wa.voipDialAttempts = nil
	calls := wa.voipCalls
	media := wa.voipMedia
	cancelGateway := wa.voipGatewayCancel
	wa.voipGatewayCancel = make(map[string]context.CancelFunc)
	wa.voipCalls = make(map[string]*meowcaller.Call)
	wa.voipReactionFeeds = make(map[string]*rtcReactionFeed)
	wa.voipMedia = make(map[string]*voipbridge.MediaBus)
	if wa.callManager != nil {
		reason := "whatsapp-disconnect"
		if replaced {
			reason = "whatsapp-stream-replaced"
		}
		wa.callManager.EndLogin(loginID, reason)
	}
	return calls, media, cancelGateway
}

func (wa *WhatsAppClient) teardownVoIP() {
	wa.teardownVoIPConnection(false)
}

// Stream replacement is terminal until explicit reconnect. The old linked
// session no longer owns the stream, so release calls without signaling.
func (wa *WhatsAppClient) replaceVoIPStream() {
	wa.teardownVoIPConnection(true)
}

func (wa *WhatsAppClient) teardownVoIPConnection(localOnly bool) {
	calls, media, cancelGateway := wa.detachVoIPRegistryForConnectionEnd(string(wa.UserLogin.ID), localOnly)
	for _, cancel := range cancelGateway {
		cancel()
	}
	for _, bus := range media {
		_ = bus.Close()
	}
	for _, call := range calls {
		if call.State() != meowcaller.CallPhaseEnded {
			if localOnly {
				call.EndLocally("whatsapp-stream-replaced")
				continue
			}
			if err := call.Hangup(); err != nil {
				wa.UserLogin.Log.Debug().Err(err).Str("call_id", call.ID()).Msg("Failed to end WhatsApp VoIP call on disconnect")
			}
		}
	}
}

// The reservation is already consumed and worker admission completed. There is
// no automatic rejoin after a cancellation or unknown provider outcome.
func (wa *WhatsAppClient) startVoIPCallLink(ctx context.Context, token string, video bool) (*meowcaller.Call, *voipbridge.MediaBus, error) {
	ctx, generation, finish := wa.beginVoIPDial(ctx)
	defer finish()
	if wa.voipClient == nil || !wa.voipAdmissionCurrent(generation) || ctx.Err() != nil {
		return nil, nil, ErrVoIPDisconnected
	}
	call, err := wa.voipClient.JoinCallLink(ctx, token, meowcaller.CallLinkOptions{Video: video})
	if err != nil {
		return nil, nil, err
	}
	if ctx.Err() != nil || !wa.registerVoIPCall(call, voipbridge.Outgoing, generation) {
		if call != nil {
			_ = call.Hangup()
		}
		return nil, nil, ErrVoIPDisconnected
	}
	bus, err := wa.AttachVoIPMedia(call.ID())
	if err != nil {
		_ = call.Hangup()
		return nil, nil, err
	}
	return call, bus, nil
}
