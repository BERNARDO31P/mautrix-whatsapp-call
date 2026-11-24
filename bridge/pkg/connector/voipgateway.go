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
	wa.voipCallsMu.Lock()
	if wa.voipGatewayCancel == nil {
		wa.voipGatewayCancel = make(map[string]context.CancelFunc)
	}
	if call.State() == meowcaller.CallPhaseEnded {
		wa.voipCallsMu.Unlock()
		cancel()
		return
	}
	if _, exists := wa.voipGatewayCancel[call.ID()]; exists {
		wa.voipCallsMu.Unlock()
		cancel()
		return
	}
	wa.voipGatewayCancel[call.ID()] = cancel
	wa.voipCallsMu.Unlock()

	go func() {
		defer cancel()
		if err := wa.runIncomingRTCGateway(ctx, call); err != nil && !errors.Is(err, context.Canceled) && !errors.Is(err, io.EOF) {
			wa.UserLogin.Log.Warn().Err(err).Str("call_id", call.ID()).Msg("MatrixRTC gateway unavailable or disconnected; WhatsApp call not automatically answered")
		}
	}()
}

// Lookup is read-only; never create a portal just because an unknown caller
// reached the linked WhatsApp device. A matching existing Matrix room is
// mandatory before announcing a native MatrixRTC call to the worker.
func (wa *WhatsAppClient) rtcRoomForPeer(ctx context.Context, peer types.JID) (string, string, string, error) {
	peer = peer.ToNonAD()
	if peer.IsEmpty() {
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
		upgrade bool
	}
	queue := make(chan update, 32)
	enqueue := func(msg rtcwire.Message, upgrade bool) {
		msg.CallID = wireCallID
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
				if u.upgrade {
					if err := call.AcceptVideo(); err != nil {
						stop()
						return
					}
				}
				if err := wire.Send(u.msg); err != nil {
					stop()
					return
				}
			}
		}
	}()
	call.OnVideoKeyframeRequest(func() { enqueue(rtcwire.Message{Type: "whatsapp_video_keyframe"}, false) })
	call.OnVideoState(func(s meowcaller.VideoState) {
		enqueue(rtcwire.Message{Type: "whatsapp_video_state", State: s.Raw, Orientation: s.Orientation}, s.Upgrade)
	})
	call.OnMuteState(func(muted bool) {
		kind := "whatsapp_audio_unmute"
		if muted {
			kind = "whatsapp_audio_mute"
		}
		enqueue(rtcwire.Message{Type: kind, Muted: muted}, false)
	})
}

func applyMatrixRTCMediaControl(call *meowcaller.Call, bus *voipbridge.MediaBus, event rtcwire.Message) error {
	switch event.Type {
	case "matrix_video_resync":
		bus.RequestVideoResync()
		return nil
	case "matrix_video_start", "matrix_video_unmute":
		return call.SetVideoEnabled(true)
	case "matrix_video_stop":
		if call.IsGroup() {
			return call.SetVideoEnabled(false)
		}
		return call.StopVideo()
	case "matrix_video_mute":
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
	defer func() {
		if call.State() == meowcaller.CallPhaseEnded {
			return
		}
		if answered {
			_ = call.Hangup()
		} else {
			_ = call.Reject()
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
		CallID: call.ID(), RoomID: roomID, SlotID: matrixRTCSlotDefault, MembershipFormat: format, GhostMXID: ghostMXID, ExpectedUserMXID: expectedMXID,
		Peer: peer, Group: group, GroupJID: groupJID, Video: call.IsVideo(),
	})
	if err != nil {
		return err
	}
	installRTCMediaCallbacks(ctx, call, wire, call.ID(), stop)
	// Do not hold an unattended incoming WhatsApp call indefinitely. The
	// worker must explicitly report user acceptance after RTC setup.
	_ = conn.SetReadDeadline(time.Now().Add(120 * time.Second))
	frames := make(chan rtcwire.Message, 1)
	go func() {
		for {
			msg, err := wire.Read()
			if err != nil {
				stop()
				return
			}
			select {
			case frames <- msg:
			case <-ctx.Done():
				return
			}
		}
	}()
	matrixVideoOrientation := -1
	for {
		var event rtcwire.Message
		select {
		case <-ctx.Done():
			return ctx.Err()
		case event = <-frames:
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
			pickup, done := context.WithTimeout(ctx, 20*time.Second)
			err = call.AnswerContext(pickup)
			done()
			if err != nil {
				return err
			}
			answered = true
			_ = conn.SetReadDeadline(time.Time{})
			go wa.pumpWhatsAppAudio(ctx, stop, wire, call.ID(), bus)
			go wa.pumpWhatsAppVideo(ctx, stop, wire, call.ID(), bus)
		} else if event.Type == "reject" && !answered {
			return wa.RejectVoIPCall(call.ID())
		} else if event.Type == "hangup" {
			return wa.HangupVoIPCall(call.ID())
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
			if !call.IsSendingVideo() {
				continue
			}
			if event.Orientation != matrixVideoOrientation {
				if err = call.SetVideoOrientation(event.Orientation); err != nil {
					return err
				}
				matrixVideoOrientation = event.Orientation
			}
			if err = wa.SendVoIPVideoWithDuration(call.ID(), event.Data, time.Duration(event.DurationUS)*time.Microsecond); err != nil {
				return err
			}
		} else if answered && (strings.HasPrefix(event.Type, "matrix_video_") || strings.HasPrefix(event.Type, "matrix_audio_")) {
			bus, ok := wa.currentVoIPMedia(call.ID())
			if !ok {
				return io.EOF
			}
			if err = applyMatrixRTCMediaControl(call, bus, event); err != nil {
				return err
			}
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

func (wa *WhatsAppClient) runOutgoingRTCGateway(ctx context.Context, sessionID, roomID, slotID, membershipFormat, ghostMXID, expectedMXID, expectedDevice, memberEvent, peer string, group bool, groupJID string, video bool, ringDeadline time.Time, targets []string, groupUpdates <-chan []string) error {
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
		RoomID: roomID, SlotID: slotID, MembershipFormat: membershipFormat, ExpectedDevice: expectedDevice, MemberEvent: memberEvent, GhostMXID: ghostMXID, ExpectedUserMXID: expectedMXID, Peer: peer, Group: group, GroupJID: groupJID, Video: video,
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
	if ready.Video {
		video = true
	}
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
	if group {
		call, bus, err = wa.StartVoIPGroupCall(ctx, groupJID, video, targets...)
	} else {
		call, bus, err = wa.StartVoIPCall(ctx, peer, video)
	}
	if err != nil {
		_ = wire.Send(rtcwire.Message{Type: "reject", CallID: sessionID, Reason: err.Error()})
		return err
	}
	defer func() {
		if call.State() != meowcaller.CallPhaseEnded {
			_ = call.Hangup()
		}
	}()
	installRTCMediaCallbacks(ctx, call, wire, sessionID, stop)

	accepted := make(chan struct{}, 1)
	markAccepted := func() {
		ringAccepted()
		select {
		case accepted <- struct{}{}:
		default:
		}
	}
	call.OnPeerAccept(markAccepted)
	if group {
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
		case <-acceptTimer.C:
			_ = wire.Send(rtcwire.Message{Type: "reject", CallID: sessionID, Reason: "WhatsApp call was not answered before timeout"})
			return fmt.Errorf("WhatsApp call was not answered before timeout")
		}
	}

	if ctx.Err() != nil {
		return ctx.Err()
	}
	ringAccepted() // Do not cancel an established call when the ring expires.
	if err = wire.Send(rtcwire.Message{Type: "whatsapp_answered", CallID: sessionID}); err != nil {
		return err
	}
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
	go wa.pumpWhatsAppAudio(ctx, stop, wire, sessionID, bus)
	go wa.pumpWhatsAppVideo(ctx, stop, wire, sessionID, bus)
	go func() {
		endTicker := time.NewTicker(100 * time.Millisecond)
		defer endTicker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-endTicker.C:
				if call.State() == meowcaller.CallPhaseEnded {
					_ = wire.Send(rtcwire.Message{Type: "ended", CallID: sessionID})
					stop()
					return
				}
			}
		}
	}()

	if group {
		go func() {
			for {
				select {
				case <-ctx.Done():
					return
				case targets := <-groupUpdates:
					if wire.Send(rtcwire.Message{Type: "group_check", CallID: sessionID, Targets: targets}) != nil {
						return
					}
				}
			}
		}()
	}
	matrixVideoOrientation := -1
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
		case "matrix_audio":
			samples, decodeErr := rtcwire.DecodePCM(event.Data)
			if decodeErr != nil {
				return decodeErr
			}
			if err = bus.SendAudioToWhatsApp(samples); err != nil {
				return err
			}
		case "matrix_group_invite":
			if group {
				if err := wa.voipClient.InviteGroupMembers(ctx, call, event.Targets); err != nil {
					wa.UserLogin.Log.Warn().Err(err).Msg("Group participant invitation failed")
				}
			}
		case "matrix_video":
			if !call.IsSendingVideo() {
				continue
			}
			if event.Orientation != matrixVideoOrientation {
				if err = call.SetVideoOrientation(event.Orientation); err != nil {
					return err
				}
				matrixVideoOrientation = event.Orientation
			}
			if err = call.SendVideoWithDuration(event.Data, time.Duration(event.DurationUS)*time.Microsecond); err != nil {
				return err
			}
		case "matrix_video_resync", "matrix_video_start", "matrix_video_stop", "matrix_video_mute", "matrix_video_unmute", "matrix_audio_mute", "matrix_audio_unmute":
			bus, ok := wa.currentVoIPMedia(call.ID())
			if !ok {
				return io.EOF
			}
			if err = applyMatrixRTCMediaControl(call, bus, event); err != nil {
				return err
			}
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
	for {
		samples, err := bus.NextAudioFromWhatsApp(ctx)
		if err != nil {
			return
		}
		pcm, err := rtcwire.EncodePCM(samples)
		if err != nil {
			return
		}
		if err = wire.Send(rtcwire.Message{Type: "whatsapp_audio", CallID: callID, Data: pcm}); err != nil {
			stop()
			return
		}
	}
}

func (wa *WhatsAppClient) pumpWhatsAppVideo(ctx context.Context, stop context.CancelFunc, wire *rtcwire.Peer, callID string, bus *voipbridge.MediaBus) {
	for {
		frame, err := bus.NextVideoFromWhatsApp(ctx)
		if err != nil {
			return
		}
		if err = wire.Send(rtcwire.Message{Type: "whatsapp_video", CallID: callID, Data: frame.Data, Orientation: frame.Orientation, Timestamp: frame.Timestamp, Generation: frame.Generation}); err != nil {
			stop()
			return
		}
	}
}
