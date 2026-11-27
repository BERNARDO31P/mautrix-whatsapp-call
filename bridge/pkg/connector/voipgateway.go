package connector

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/event"
	"maunium.net/go/mautrix/id"
	"net"
	"os"
	"strings"
	"time"

	"github.com/purpshell/meowcaller"
	"github.com/purpshell/meowcaller/rtp"
	"go.mau.fi/mautrix-whatsapp/pkg/connector/voipbridge"
	"go.mau.fi/mautrix-whatsapp/pkg/connector/voipbridge/rtcwire"
	"go.mau.fi/whatsmeow/types"
	"maunium.net/go/mautrix/bridgev2/database"
)

// dispatchIncomingToRTCGateway is opt-in. Until a real MatrixRTC worker is
// deployed at the configured private Unix socket, normal WhatsApp messaging
// continues and no call is answered by the bridge.
func (wa *WhatsAppClient) dispatchIncomingToRTCGateway(call *meowcaller.Call) {
	ctx, cancel := context.WithCancel(wa.Main.Bridge.BackgroundCtx)
	if !wa.tryRegisterIncomingRTCGateway(call.ID(), call, cancel) {
		cancel()
		return
	}

	go func() {
		defer cancel()
		if err := wa.runIncomingRTCGateway(ctx, call); err != nil && !errors.Is(err, context.Canceled) && !errors.Is(err, io.EOF) {
			wa.UserLogin.Log.Warn().Err(err).Str("call_id", call.ID()).Msg("MatrixRTC gateway unavailable or disconnected; WhatsApp call not automatically answered")
		}
	}()
}

// Register under the same lock as teardownVoIP. A removed or replaced call
// must not recreate its gateway cancellation entry after the disconnect snapshot.
func (wa *WhatsAppClient) tryRegisterIncomingRTCGateway(callID string, call *meowcaller.Call, cancel context.CancelFunc) bool {
	wa.voipCallsMu.Lock()
	defer wa.voipCallsMu.Unlock()
	if callID == "" || call == nil || wa.voipCalls[callID] != call || call.State() == meowcaller.CallPhaseEnded {
		return false
	}
	if _, exists := wa.voipGatewayCancel[callID]; exists {
		return false
	}
	if wa.voipGatewayCancel == nil {
		wa.voipGatewayCancel = make(map[string]context.CancelFunc)
	}
	wa.voipGatewayCancel[callID] = cancel
	return true
}

// rtcRoomPeerJID follows Whatsmeow's hosted sender normalization for DM
// lookup and reflected-self detection only. Keep the original live address: its
// device/domain can still be significant to signaling and capability checks.
func rtcRoomPeerJID(peer types.JID) types.JID {
	peer = peer.ToNonAD()
	switch peer.Server {
	case types.HostedServer:
		peer.Server = types.DefaultUserServer
	case types.HostedLIDServer:
		peer.Server = types.HiddenUserServer
	}
	return peer
}

// Lookup is read-only; never create a portal just because an unknown caller
// reached the linked WhatsApp device. A matching existing Matrix room is
// mandatory before announcing a native MatrixRTC call to the worker.
func (wa *WhatsAppClient) rtcRoomForPeer(ctx context.Context, peer types.JID) (string, string, string, error) {
	peer = rtcRoomPeerJID(peer)
	if peer.IsEmpty() || peer.User == "" {
		return "", "", "", errors.New("missing WhatsApp peer")
	}
	// Prefer the canonical LID portal, but retain the original PN even
	// if a reverse alias lookup is temporarily unavailable.
	candidates := wa.matrixRTCCallCandidateJIDs(ctx, peer)
	for i, candidate := range candidates {
		if candidate.Server == types.HiddenUserServer {
			candidates[0], candidates[i] = candidates[i], candidates[0]
			break
		}
	}
	for _, jid := range candidates {
		portal, err := wa.Main.Bridge.DB.Portal.GetByKey(ctx, wa.makeWAPortalKey(jid))
		if err != nil {
			return "", "", "", fmt.Errorf("portal lookup for %s: %w", jid, err)
		}
		if portal != nil && portal.MXID != "" && portal.RoomType == database.RoomTypeDM {
			if portal.OtherUserID == "" {
				return "", "", "", fmt.Errorf("DM portal %s has no WhatsApp ghost identity", portal.MXID)
			}
			if portal.Receiver != "" && portal.Receiver != wa.UserLogin.ID {
				return "", "", "", errors.New("existing DM portal belongs to another WhatsApp login")
			}
			if wa.UserLogin.UserMXID == "" {
				return "", "", "", errors.New("WhatsApp login has no Matrix user identity")
			}
			return string(portal.MXID), wa.Main.Bridge.Matrix.FormatGhostMXID(portal.OtherUserID).String(), wa.UserLogin.UserMXID.String(), nil
		}
	}
	return "", "", "", fmt.Errorf("no existing Matrix room for WhatsApp peer %s", peer)
}

func (wa *WhatsAppClient) rtcRoomForCall(ctx context.Context, call *meowcaller.Call) (roomID, rtcMXID, expectedMXID, peer string, group bool, groupJID string, err error) {
	if call == nil {
		return "", "", "", "", false, "", errors.New("missing WhatsApp call")
	}
	if state, ok := call.GroupState(); ok {
		if state.GroupJID.IsEmpty() {
			return "", "", "", "", true, "", errors.New("WhatsApp group call has no group JID")
		}
		jid := state.GroupJID.ToNonAD()
		portal, lookupErr := wa.Main.Bridge.DB.Portal.GetByKey(ctx, wa.makeWAPortalKey(jid))
		if lookupErr != nil {
			return "", "", "", "", true, jid.String(), fmt.Errorf("group portal lookup for %s: %w", jid, lookupErr)
		}
		if portal == nil || portal.MXID == "" || portal.RoomType != database.RoomTypeDefault {
			return "", "", "", "", true, jid.String(), fmt.Errorf("no existing Matrix room for WhatsApp group %s", jid)
		}
		if portal.Receiver != "" && portal.Receiver != wa.UserLogin.ID {
			return "", "", "", "", true, jid.String(), errors.New("existing group portal belongs to another WhatsApp login")
		}
		if wa.UserLogin.UserMXID == "" {
			return "", "", "", "", true, jid.String(), errors.New("WhatsApp login has no Matrix user identity")
		}
		return string(portal.MXID), wa.Main.Bridge.Bot.GetMXID().String(), wa.UserLogin.UserMXID.String(), jid.String(), true, jid.String(), nil
	}
	roomID, rtcMXID, expectedMXID, err = wa.rtcRoomForPeer(ctx, call.Peer())
	if err != nil {
		return "", "", "", "", false, "", err
	}
	return roomID, rtcMXID, expectedMXID, call.Peer().ToNonAD().String(), false, "", nil
}

func installRTCMediaCallbacks(ctx context.Context, call *meowcaller.Call, wire *rtcwire.Peer, wireCallID string, stop context.CancelFunc) {
	type update struct {
		msg     rtcwire.Message
		upgrade uint64
	}
	queue := make(chan update, 32)
	enqueue := func(msg rtcwire.Message, upgrade uint64) {
		msg.CallID = wireCallID
		if ctx.Err() != nil {
			return
		}
		select {
		case <-ctx.Done():
		case queue <- update{msg, upgrade}:
		default:
			stop()
		}
	}
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case u := <-queue:
				if ctx.Err() != nil {
					return
				}
				if u.upgrade != 0 {
					if err := call.AutoAcceptVideo(ctx, u.upgrade); err != nil && !errors.Is(err, meowcaller.ErrVideoDecisionMode) {
						if errors.Is(err, meowcaller.ErrNoPendingVideoUpgrade) {
							// Ordered callbacks can outlive a withdrawn or accepted request.
							continue
						}
						stop()
						return
					}
				}
				if err := wire.SendContext(ctx, u.msg); err != nil {
					stop()
					return
				}
			}
		}
	}()
	call.OnVideoKeyframeRequest(func() { enqueue(rtcwire.Message{Type: "whatsapp_video_keyframe"}, 0) })
	call.OnVideoState(func(s meowcaller.VideoState) {
		var generation uint64
		if s.Upgrade {
			generation = s.UpgradeGeneration
		}
		enqueue(rtcwire.Message{Type: "whatsapp_video_state", State: s.Raw, Orientation: s.Orientation}, generation)
	})
	call.OnMuteState(func(muted bool) {
		kind := "whatsapp_audio_unmute"
		if muted {
			kind = "whatsapp_audio_mute"
		}
		enqueue(rtcwire.Message{Type: kind, Muted: muted}, 0)
	})
}

// outboundVideoRole belongs to one ordered gateway receive loop. Generation
// zero remains the existing direct/aggregate wire mode.
type outboundVideoRole struct {
	screen         bool
	generation     uint64
	lastGeneration uint64
}

func (v *outboundVideoRole) acceptsFrame(generation uint64) bool {
	if v.lastGeneration != 0 && v.generation == 0 {
		return false
	}
	return generation == v.generation
}

func (v *outboundVideoRole) apply(ctx context.Context, call *meowcaller.Call, event rtcwire.Message) error {
	screen := event.Type == "matrix_video_screen_start"
	starting := screen || event.Type == "matrix_video_start" || event.Type == "matrix_video_unmute"
	if starting && event.Generation != 0 {
		if !event.Group || !call.IsGroup() || event.Generation <= v.lastGeneration {
			return fmt.Errorf("invalid outbound video source generation")
		}
	} else if screen || starting && v.lastGeneration != 0 {
		return fmt.Errorf("missing outbound video source generation")
	}
	// A screen-share start requires active video, not merely a successful
	// SetVideoEnabled call (which is a no-op for audio-only group calls).
	// Keep the existing primary sender alive during camera/display switches.
	if starting {
		if err := call.SetVideoEnabled(true); err != nil {
			return err
		}
		if screen && !call.IsVideoTransmitReady() {
			return fmt.Errorf("screen sharing requires negotiated group video")
		}
	}
	if v.screen != screen {
		// The provider uses a single primary video stream. Do not send
		// frames for the new role until its signalling is submitted.
		shareCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
		defer cancel()
		if err := call.SetScreenShareContext(shareCtx, screen, nil); err != nil {
			return err
		}
		v.screen = screen
	}
	if starting {
		v.generation = event.Generation
		if event.Generation != 0 {
			v.lastGeneration = event.Generation
		}
	} else {
		v.generation = 0
	}
	return nil
}

func applyMatrixRTCMediaControl(ctx context.Context, call *meowcaller.Call, bus *voipbridge.MediaBus, role *outboundVideoRole, event rtcwire.Message) error {
	switch event.Type {
	case "matrix_video_resync":
		bus.RequestVideoResync()
		return nil
	case "matrix_video_start", "matrix_video_unmute", "matrix_video_screen_start":
		return role.apply(ctx, call, event)
	case "matrix_video_stop":
		if err := role.apply(ctx, call, event); err != nil {
			return err
		}
		if call.IsGroup() {
			return call.SetVideoEnabled(false)
		}
		return call.StopVideo()
	case "matrix_video_mute":
		if err := role.apply(ctx, call, event); err != nil {
			return err
		}
		return call.SetVideoEnabled(false)
	case "matrix_audio_mute":
		bus.SetAudioMuted(true)
		return call.SetMuted(true)
	case "matrix_audio_unmute":
		if err := call.SetMuted(false); err != nil {
			return err
		}
		bus.SetAudioMuted(false)
		return nil
	default:
		return fmt.Errorf("unsupported MatrixRTC media control %q", event.Type)
	}
}

// Element X currently uses MSC3401 room-state membership and legacy SFU
// authorization. A server supporting newer MSCs does not imply that the
// receiving client uses them. Explicitly opt into MSC for newer clients.
func incomingRTCMembershipFormat(value string) (string, error) {
	switch value {
	case "", "room_state":
		return "room_state", nil
	case "msc":
		return "msc", nil
	default:
		return "", errors.New("invalid MATRIXRTC_INCOMING_MEMBERSHIP_FORMAT (expected room_state or msc)")
	}
}

func (wa *WhatsAppClient) runIncomingRTCGateway(ctx context.Context, call *meowcaller.Call) error {
	ctx, stop := context.WithCancel(ctx)
	defer stop()
	answered := false
	pickupStarted := false
	defer func() {
		if call.State() == meowcaller.CallPhaseEnded {
			return
		}
		if pickupStarted {
			// Pickup may have been submitted even if its response was lost.
			// Never retry an accept; retire the original call and settle its terminal signal.
			if err := call.Hangup(); err != nil {
				wa.UserLogin.Log.Warn().Err(err).Str("call_id", call.ID()).Msg("WhatsApp pickup terminal settlement unconfirmed")
			}
		} else {
			// A failed local gateway is not a user decision to decline.
			call.DismissUnanswered()
		}
	}()
	format, err := incomingRTCMembershipFormat(os.Getenv("MATRIXRTC_INCOMING_MEMBERSHIP_FORMAT"))
	if err != nil {
		return err
	}
	roomID, ghostMXID, expectedMXID, peer, group, groupJID, err := wa.rtcRoomForCall(ctx, call)
	if err != nil {
		return err
	}
	dialer := &net.Dialer{Timeout: 3 * time.Second}
	conn, err := dialer.DialContext(ctx, "unix", wa.Main.Config.RTCGatewaySocket)
	if err != nil {
		return fmt.Errorf("connect to private RTC worker: %w", err)
	}
	defer conn.Close()
	// Closing this connection interrupts a blocked Read on call end/disconnect.
	stopClose := context.AfterFunc(ctx, func() { _ = conn.Close() })
	defer stopClose()
	wire := rtcwire.NewPeer(conn, conn)
	err = wire.Send(rtcwire.Message{
		Type: "incoming", LoginID: string(wa.UserLogin.ID),
		CallID: call.ID(), RoomID: roomID, SlotID: matrixRTCSlotDefault, MembershipFormat: format, GhostMXID: ghostMXID, BridgeBotMXID: wa.Main.Bridge.Bot.GetMXID().String(), ExpectedUserMXID: expectedMXID,
		Peer: peer, Group: group, GroupJID: groupJID, Video: call.IsVideo(),
	})
	if err != nil {
		return err
	}
	installRTCMediaCallbacks(ctx, call, wire, call.ID(), stop)
	// Do not hold an unattended incoming WhatsApp call indefinitely. The
	// worker must explicitly report user acceptance after RTC setup.
	_ = conn.SetReadDeadline(time.Now().Add(120 * time.Second))
	frames := rtcwire.StreamFrames(ctx, wire.Read)
	var videoForward rtcwire.VideoForwardState
	var outboundRole outboundVideoRole
	var handoff *gatewayParticipantHandoff
	for {
		var event rtcwire.Message
		select {
		case <-ctx.Done():
			return ctx.Err()
		case msg, ok := <-frames:
			if !ok {
				return io.EOF
			}
			event = msg
		}
		if err := ctx.Err(); err != nil {
			return err
		}

		if event.CallID != call.ID() {
			return fmt.Errorf("call ID mismatch from RTC worker")
		}
		if event.Type == "answer" && !answered {
			bus, err := wa.AttachVoIPMedia(call.ID())
			if err != nil {
				return err
			}
			_ = conn.SetReadDeadline(time.Time{})
			pickupStarted = true
			wa.UserLogin.Log.Info().Str("call_id", call.ID()).Msg("MatrixRTC authorized WhatsApp pickup")
			pickup, done := context.WithTimeout(ctx, 20*time.Second)
			pending, err := rtcwire.AwaitPickup(pickup, call.ID(), frames, call.AnswerContext)
			done()
			if err != nil {
				return err
			}
			if err := ctx.Err(); err != nil {
				return err
			}
			answered = true
			wa.UserLogin.Log.Info().Str("call_id", call.ID()).Msg("MatrixRTC pickup submitted to WhatsApp")
			for _, control := range pending.Controls {
				if err := ctx.Err(); err != nil {
					return err
				}
				if err := applyMatrixRTCMediaControl(ctx, call, bus, &outboundRole, control); err != nil {
					return err
				}
				videoForward.Control(control.Type)
			}
			if pending.DroppedVideo {
				if err := videoForward.RequestKeyframe(func() error {
					return wire.SendContext(ctx, rtcwire.Message{Type: "whatsapp_video_keyframe", CallID: call.ID()})
				}); err != nil {
					return err
				}
			}
			handoff = wa.startGatewayParticipantHandoff(ctx, stop, wire, call.ID(), bus, call, group && ghostMXID == wa.Main.Bridge.Bot.GetMXID().String())
		} else if answered && (event.Type == "matrix_participant_request" || event.Type == "matrix_participant_ready" || event.Type == "matrix_participant_keyframe") {
			if err := handoff.handle(event); err != nil {
				return err
			}
		} else if event.Type == "reject" && !answered {
			return call.Reject()
		} else if event.Type == "hangup" {
			if !answered {
				return fmt.Errorf("RTC worker ended before pickup")
			}
			return call.Hangup()
		} else if event.Type == "matrix_audio" && answered {
			bus, found := wa.currentVoIPMedia(call.ID())
			if !found {
				return io.EOF
			}
			samples, err := rtcwire.DecodePCM(event.Data)
			if err != nil {
				return err
			}
			if err = bus.SendAudioToWhatsApp(samples); err != nil {
				return err
			}
		} else if event.Type == "matrix_video" && answered {
			if !outboundRole.acceptsFrame(event.Generation) {
				continue
			}
			if err = videoForward.Forward(call.IsVideoTransmitReady(), rtp.AUHasIDR(event.Data), event.Orientation, func() error {
				return wire.SendContext(ctx, rtcwire.Message{Type: "whatsapp_video_keyframe", CallID: call.ID()})
			}, func(orientation int) error {
				err := call.SetVideoOrientation(orientation)
				if errors.Is(err, meowcaller.ErrVideoFrameDropped) {
					return rtcwire.ErrVideoFrameDropped
				}
				return err
			}, func() error {
				err := call.SendVideoWithDuration(event.Data, time.Duration(event.DurationUS)*time.Microsecond)
				if errors.Is(err, meowcaller.ErrVideoFrameDropped) {
					return rtcwire.ErrVideoFrameDropped
				}
				return err
			}); err != nil {
				return err
			}
		} else if answered && (strings.HasPrefix(event.Type, "matrix_video_") || strings.HasPrefix(event.Type, "matrix_audio_")) {
			bus, ok := wa.currentVoIPMedia(call.ID())
			if !ok {
				return io.EOF
			}
			if err = applyMatrixRTCMediaControl(ctx, call, bus, &outboundRole, event); err != nil {
				return err
			}
			videoForward.Control(event.Type)
		} else if event.Type == "error" {
			return fmt.Errorf("RTC worker: %s", event.Reason)
		} else {
			return fmt.Errorf("unexpected RTC worker event %q in answered=%t state", event.Type, answered)
		}
	}
}

func formatWhatsAppEndReason(raw string) string {
	defaultReason := "WhatsApp peer rejected or ended the call"
	if raw == "" {
		return defaultReason
	}
	if code, found := strings.CutPrefix(raw, "server:"); found && code != "" {
		return fmt.Sprintf("WhatsApp rejected this destination (server %s)", code)
	}
	switch raw {
	case "reject", "remote-rejected":
		return "WhatsApp peer rejected the call"
	case "remote-hangup", "hangup":
		return "WhatsApp peer ended the call"
	case "whatsapp-disconnect":
		return "WhatsApp disconnected while placing the call"
	case "whatsapp-ended":
		return defaultReason
	default:
		return fmt.Sprintf("WhatsApp call ended: %s", raw)
	}
}

func (wa *WhatsAppClient) outgoingWhatsAppEndReason(callID string) string {
	if wa.callManager == nil {
		return formatWhatsAppEndReason("")
	}
	session, ok := wa.callManager.Get(wa.callKey(callID))
	if !ok {
		return formatWhatsAppEndReason("")
	}
	return formatWhatsAppEndReason(session.EndReason)
}

// Ring lifetime cancels unanswered calls, not active calls. stop always cancels
// on explicit decline, bridge shutdown or normal hangup. accept disarms only
// the ring timer; it does NOT detach the bridge's parent cancellation.
func newRTCRingLifetime(parent context.Context, deadline time.Time) (context.Context, context.CancelFunc, func()) {
	ctx, cancel := context.WithCancel(parent)
	timer := time.AfterFunc(time.Until(deadline), cancel)
	stop := func() { timer.Stop(); cancel() }
	accept := func() { timer.Stop() }
	return ctx, stop, accept
}

// Keep monitoring WhatsApp after pickup, including while Matrix is still
// joining the SFU. Cancellation closes the gateway socket without waiting for
// a potentially blocked control-frame write.
func monitorRTCPeerEnd(ctx context.Context, interval time.Duration, ended func() bool, stop context.CancelFunc) {
	if ctx.Err() != nil {
		return
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		if ended() {
			stop()
			return
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func (wa *WhatsAppClient) runOutgoingRTCGateway(ctx context.Context, sessionID, roomID, slotID, membershipFormat, ghostMXID, expectedMXID, expectedDevice, expectedSigningKey, expectedIdentityKey, memberEvent, memberJoinID string, memberJoinTS uint64, peer string, group bool, groupJID string, intent string, ringDeadline time.Time, targets []string, groupUpdates <-chan rtcGroupUpdate, adHoc bool, linkToken string) error {
	parent := ctx
	matrixConnected := false
	defer func() {
		if group || matrixConnected || parent.Err() != nil {
			return
		}
		notify, cancel := context.WithTimeout(wa.Main.Bridge.BackgroundCtx, 8*time.Second)
		defer cancel()
		ghostID, ok := wa.Main.Bridge.Matrix.ParseGhostMXID(id.UserID(ghostMXID))
		if !ok {
			return
		}
		ghost, err := wa.Main.Bridge.GetGhostByID(notify, ghostID)
		if err != nil || ghost == nil {
			return
		}
		tx := fmt.Sprintf("wa-rtc-terminal-%x", sha256.Sum256([]byte(roomID+"\x00"+sessionID)))
		for attempt := 0; attempt < 2; attempt++ {
			_, err = ghost.Intent.SendMessage(notify, id.RoomID(roomID), rtcEventType(rtcDeclineStable, event.MessageEventType), &event.Content{Raw: map[string]any{"m.relates_to": map[string]any{"rel_type": "m.reference", "event_id": sessionID}}}, &bridgev2.MatrixSendExtra{TransactionID: tx})
			if err == nil {
				break
			}
		}
		if err != nil {
			wa.UserLogin.Log.Warn().Err(err).Msg("Failed to send terminal MatrixRTC decline")
		}
	}()
	ctx, stop, ringAccepted := newRTCRingLifetime(ctx, ringDeadline)
	defer stop()
	dialer := &net.Dialer{Timeout: 3 * time.Second}
	conn, err := dialer.DialContext(ctx, "unix", wa.Main.Config.RTCGatewaySocket)
	if err != nil {
		return fmt.Errorf("connect to private RTC worker: %w", err)
	}
	defer conn.Close()
	stopClose := context.AfterFunc(ctx, func() { _ = conn.Close() })
	defer stopClose()
	wire := rtcwire.NewPeer(conn, conn)
	if err = wire.Send(rtcwire.Message{
		Type: "outgoing", LoginID: string(wa.UserLogin.ID), CallID: sessionID,
		RoomID: roomID, SlotID: slotID, MembershipFormat: membershipFormat, ExpectedDevice: expectedDevice, ExpectedSigningKey: expectedSigningKey, ExpectedIdentityKey: expectedIdentityKey, MemberEvent: memberEvent, MemberJoinID: memberJoinID, MemberJoinTS: memberJoinTS, GhostMXID: ghostMXID, BridgeBotMXID: wa.Main.Bridge.Bot.GetMXID().String(), ExpectedUserMXID: expectedMXID, Peer: peer, Group: group && linkToken == "", GroupJID: groupJID, Intent: intent,
	}); err != nil {
		return err
	}

	_ = conn.SetReadDeadline(ringDeadline)
	ready, err := wire.Read()
	if err != nil {
		return err
	}
	if ready.CallID != sessionID {
		return fmt.Errorf("call ID mismatch from RTC worker")
	}
	if ready.Type == "error" || ready.Type == "reject" {
		return fmt.Errorf("RTC worker: %s", ready.Reason)
	}
	if ready.Type != "dial" {
		return fmt.Errorf("unexpected RTC worker event %q before WhatsApp dial", ready.Type)
	}
	// Require the worker to acknowledge the resolved mode before any WhatsApp dial.
	if (ready.Intent != "audio" && ready.Intent != "video") || ready.Video != (ready.Intent == "video") {
		return fmt.Errorf("RTC worker did not resolve outgoing media intent")
	}
	video := ready.Video
	_ = conn.SetReadDeadline(time.Time{})

	reading := make(chan error, 1)
	go func() {
		// Listen before starting the dial. Revoked admission cancels capability
		// discovery or an in-flight WhatsApp offer even before remote pickup.
		event, err := wire.Read()
		if err == nil && event.Type == "answer" && event.CallID == sessionID {
			reading <- nil
			return
		}
		if err == nil {
			err = fmt.Errorf("worker ended admission: %s", event.Reason)
		}
		reading <- err
		stop()
	}()
	var call *meowcaller.Call
	var bus *voipbridge.MediaBus
	if linkToken != "" {
		if ready.Intent != intent {
			return errors.New("RTC worker call-link intent mismatch")
		}
		call, bus, err = wa.startVoIPCallLink(ctx, linkToken, video)
	} else if group && adHoc {
		call, bus, err = wa.startVoIPAdHocGroupCall(ctx, groupJID, video, targets)
	} else if group {
		call, bus, err = wa.StartVoIPGroupCall(ctx, groupJID, video, targets...)
	} else {
		call, bus, err = wa.StartVoIPCall(ctx, peer, video)
	}
	if err != nil {
		_ = wire.Send(rtcwire.Message{Type: "reject", CallID: sessionID, Reason: func() string {
			if linkToken != "" {
				return "WhatsApp call-link outcome unavailable"
			}
			return err.Error()
		}()})
		return err
	}
	defer func() {
		if call.State() != meowcaller.CallPhaseEnded {
			_ = call.Hangup()
		}
	}()
	if linkToken != "" && !wa.bindRTCLinkCall(roomID, slotID, sessionID, call) {
		return errors.New("call-link room session no longer current")
	}
	installRTCMediaCallbacks(ctx, call, wire, sessionID, stop)

	accepted := make(chan struct{}, 1)
	markAccepted := func() {
		if ctx.Err() != nil || call.State() == meowcaller.CallPhaseEnded {
			return
		}
		ringAccepted()
		select {
		case accepted <- struct{}{}:
		default:
		}
	}
	if linkToken == "" {
		call.OnPeerAccept(markAccepted)
	}
	if group && linkToken == "" {
		call.OnReady(markAccepted)
	}
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	acceptTimer := time.NewTimer(120 * time.Second)
	defer acceptTimer.Stop()
	waiting := true
	for waiting {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case err := <-reading:
			if err == nil {
				return fmt.Errorf("unexpected early Matrix answer")
			}
			return err
		case <-accepted:
			waiting = false
		case <-ticker.C:
			if call.State() == meowcaller.CallPhaseEnded {
				_ = wire.Send(rtcwire.Message{Type: "reject", CallID: sessionID, Reason: wa.outgoingWhatsAppEndReason(call.ID())})
				return nil
			}
			if linkToken != "" && call.IsCallLinkAdmitted() {
				markAccepted()
			}
		case <-acceptTimer.C:
			_ = wire.Send(rtcwire.Message{Type: "reject", CallID: sessionID, Reason: "WhatsApp call was not answered before timeout"})
			return fmt.Errorf("WhatsApp call was not answered before timeout")
		}
	}

	if ctx.Err() != nil {
		return ctx.Err()
	}
	if call.State() == meowcaller.CallPhaseEnded || (linkToken != "" && !call.IsCallLinkAdmitted()) {
		_ = wire.Send(rtcwire.Message{Type: "reject", CallID: sessionID, Reason: wa.outgoingWhatsAppEndReason(call.ID())})
		return nil
	}
	ringAccepted() // Do not cancel an established call when the ring expires.
	go monitorRTCPeerEnd(ctx, 100*time.Millisecond, func() bool {
		return call.State() == meowcaller.CallPhaseEnded
	}, stop)
	if err = wire.Send(rtcwire.Message{Type: "whatsapp_answered", CallID: sessionID}); err != nil {
		return err
	}
	wa.UserLogin.Log.Info().Str("call_id", call.ID()).Msg("WhatsApp acceptance forwarded to MatrixRTC worker")
	_ = conn.SetReadDeadline(time.Now().Add(60 * time.Second))
	select {
	case err := <-reading:
		if err != nil {
			return err
		}
	case <-ctx.Done():
		return ctx.Err()
	}
	matrixConnected = true
	_ = conn.SetReadDeadline(time.Time{})
	handoff := wa.startGatewayParticipantHandoff(ctx, stop, wire, sessionID, bus, call, group && linkToken == "" && ghostMXID == wa.Main.Bridge.Bot.GetMXID().String())

	if group && !adHoc {
		go func() {
			for {
				select {
				case <-ctx.Done():
					return
				case update := <-groupUpdates:
					if ctx.Err() != nil {
						return
					}
					if !time.Now().Before(update.deadline) {
						continue
					}
					if wire.SendContext(ctx, rtcwire.Message{Type: "group_check", CallID: sessionID, Targets: update.targets}) != nil {
						return
					}
				}
			}
		}()
	}
	var videoForward rtcwire.VideoForwardState
	var outboundRole outboundVideoRole
	for {
		event, readErr := wire.Read()
		if readErr != nil {
			if ctx.Err() != nil {
				return nil
			}
			return readErr
		}
		if event.CallID != sessionID {
			return fmt.Errorf("call ID mismatch from RTC worker")
		}
		switch event.Type {
		case "matrix_participant_request", "matrix_participant_ready", "matrix_participant_keyframe":
			if err := handoff.handle(event); err != nil {
				return err
			}
		case "matrix_audio":
			samples, decodeErr := rtcwire.DecodePCM(event.Data)
			if decodeErr != nil {
				return decodeErr
			}
			if err = bus.SendAudioToWhatsApp(samples); err != nil {
				return err
			}
		case "matrix_group_invite":
			if !group || adHoc {
				return fmt.Errorf("RTC worker requested a group invitation outside a chat-bound group call")
			}
			if err := wa.voipClient.InviteGroupMembers(ctx, call, event.Targets); err != nil {
				wa.UserLogin.Log.Warn().Err(err).Msg("Group participant invitation failed")
			}
		case "matrix_video":
			if !outboundRole.acceptsFrame(event.Generation) {
				continue
			}
			if err = videoForward.Forward(call.IsVideoTransmitReady(), rtp.AUHasIDR(event.Data), event.Orientation, func() error {
				return wire.SendContext(ctx, rtcwire.Message{Type: "whatsapp_video_keyframe", CallID: sessionID})
			}, func(orientation int) error {
				err := call.SetVideoOrientation(orientation)
				if errors.Is(err, meowcaller.ErrVideoFrameDropped) {
					return rtcwire.ErrVideoFrameDropped
				}
				return err
			}, func() error {
				err := call.SendVideoWithDuration(event.Data, time.Duration(event.DurationUS)*time.Microsecond)
				if errors.Is(err, meowcaller.ErrVideoFrameDropped) {
					return rtcwire.ErrVideoFrameDropped
				}
				return err
			}); err != nil {
				return err
			}
		case "matrix_video_resync", "matrix_video_start", "matrix_video_screen_start", "matrix_video_stop", "matrix_video_mute", "matrix_video_unmute", "matrix_audio_mute", "matrix_audio_unmute":
			bus, ok := wa.currentVoIPMedia(call.ID())
			if !ok {
				return io.EOF
			}
			if err = applyMatrixRTCMediaControl(ctx, call, bus, &outboundRole, event); err != nil {
				return err
			}
			videoForward.Control(event.Type)
		case "hangup", "reject":
			return call.Hangup()
		case "error":
			return fmt.Errorf("RTC worker: %s", event.Reason)
		default:
			return fmt.Errorf("unexpected RTC worker event %q in outgoing call", event.Type)
		}
	}
}

func (wa *WhatsAppClient) currentVoIPMedia(callID string) (bus *voipbridge.MediaBus, found bool) {
	wa.voipCallsMu.RLock()
	bus = wa.voipMedia[callID]
	wa.voipCallsMu.RUnlock()
	return bus, bus != nil
}

func (wa *WhatsAppClient) pumpWhatsAppAudio(ctx context.Context, stop context.CancelFunc, wire *rtcwire.Peer, callID string, bus *voipbridge.MediaBus) {
	wa.pumpWhatsAppAudioUntil(ctx, ctx, stop, wire, callID, bus)
}

// The read context can be retired independently. Never cancel the write
// context merely to switch publication modes: a partial IPC frame is fatal.
func (wa *WhatsAppClient) pumpWhatsAppAudioUntil(readCtx, writeCtx context.Context, stop context.CancelFunc, wire *rtcwire.Peer, callID string, bus *voipbridge.MediaBus) {
	for {
		samples, err := bus.NextAudioFromWhatsApp(readCtx)
		if err != nil {
			return
		}
		pcm, err := rtcwire.EncodePCM(samples)
		if err != nil {
			continue
		}
		if readCtx.Err() != nil {
			return
		}
		if err = wire.SendContext(writeCtx, rtcwire.Message{Type: "whatsapp_audio", CallID: callID, Data: pcm}); err != nil {
			if rtcwire.IsLocalFrameError(err) {
				continue
			}
			stop()
			return
		}
	}
}

func (wa *WhatsAppClient) pumpWhatsAppVideo(ctx context.Context, stop context.CancelFunc, wire *rtcwire.Peer, callID string, bus *voipbridge.MediaBus) {
	wa.pumpWhatsAppVideoUntil(ctx, ctx, stop, wire, callID, bus)
}

func (wa *WhatsAppClient) pumpWhatsAppVideoUntil(readCtx, writeCtx context.Context, stop context.CancelFunc, wire *rtcwire.Peer, callID string, bus *voipbridge.MediaBus) {
	for {
		frame, err := bus.NextVideoFromWhatsApp(readCtx)
		if err != nil {
			return
		}
		if readCtx.Err() != nil {
			return
		}
		if err = wire.SendContext(writeCtx, rtcwire.Message{Type: "whatsapp_video", CallID: callID, Data: frame.Data, Orientation: frame.Orientation, Timestamp: frame.Timestamp, Generation: frame.Generation}); err != nil {
			if rtcwire.IsLocalFrameError(err) {
				bus.RequestVideoResync()
				continue
			}
			stop()
			return
		}
	}
}
