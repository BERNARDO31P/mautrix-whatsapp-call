package meowcaller

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"time"
	"unsafe"

	"github.com/purpshell/meowcaller/signaling"
	"go.mau.fi/whatsmeow"
	waBinary "go.mau.fi/whatsmeow/binary"
	"go.mau.fi/whatsmeow/proto/waE2E"
	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"
	"google.golang.org/protobuf/proto"
)

// engine is the internal media + signaling engine behind Client/Call. It owns the
// whatsmeow event wiring (offer / preaccept / accept / relaylatency / mute_v2 / ack /
// terminate), the low-level <ack>/<call> node interception, the relay election and the
// per-frame media loop (encode a Player's frames out, decode the peer's frames into a
// sink). This is where the orchestration formerly hand-rolled in examples/cli has been
// lifted to; Client and Call are the public face over it.
type engine struct {
	c *Client

	mu                sync.Mutex
	calls             map[string]*engineCall // keyed by call-id
	ended             map[string]time.Time
	earlyTerminations map[earlyTerminationIdentity]time.Time
	sendCallNode      func(context.Context, waBinary.Node) error
	requestCallNode   func(context.Context, waBinary.Node, string) (*waBinary.Node, error)
	rawCallHookErr    error
	callDispatch      callDispatchQueue
}

// engineCall is the engine's per-call state: the public Call handle plus the inputs
// needed to bring media up (the decrypted callKey and the relay endpoint, both of
// which can arrive separately), the media goroutine cancel handle, and the deferred
// accept bookkeeping.
type engineCall struct {
	call    *Call
	callKey []byte
	relay   *relayData
	selfLID string
	peerLID string

	creator types.JID // call-creator JID (for accept/relaylatency)
	from    types.JID // the <call> "from" — where stanzas are addressed

	direction                 CallDirection
	codec                     AudioCodec // audio codec for this call, selected from voip_settings (MLow default)
	audioFrameMs              int
	audioBitrate              int
	mediaCodecFailure         func(AudioCodec, int, int) // installed by the exact media owner
	localVideo                bool                       // this client is sending, or has requested to send, video
	remoteVideo               bool                       // the peer is sending video to this client
	videoGate                 bool                       // outbound upgrade is waiting for peer acceptance
	peerVideoUpgrade          bool                       // the peer's inbound upgrade is waiting for local acceptance
	peerVideoGeneration       uint64
	manualVideoDecision       bool
	peerVideoDecisionInFlight bool
	videoTx                   *videoSender // video send pipeline, live while media runs
	appDataTx                 *appDataSender
	rekeyPeer                 func(string) error
	group                     bool
	explicitAdHoc             bool
	linkCall                  bool
	linkTokenHash             [32]byte
	linkVideo                 bool
	linkAdmitted              bool
	groupUpdate               *groupCallUpdate
	groupReceivers            *participantReceiveRegistry
	groupEpochState
	started           bool
	cancel            context.CancelFunc // tears down this call's media goroutine
	waitingRoomCancel context.CancelFunc
	inviteSelfDevice  groupCallDevice
	invitePeerDevice  groupCallDevice

	// Pickup is authorized by the integrator, independently of remote mute state.
	acceptPending       bool
	pickupCancel        context.CancelFunc
	pickupSendDone      chan struct{} // installed under engine.mu before accept submission
	terminalReservation time.Time     // exact reservation installed at terminal removal
	seenMute            bool
	videoMode           bool

	// offerNodeID is the outer stanza id of an outbound <call><offer>. WhatsApp
	// correlates the server <ack class="call" type="offer"> by this id, while
	// error acks are not guaranteed to repeat the inner call-id.
	offerNodeID string
}

// A valid outbound offer gets a call ACK/relay allocation quickly. If no usable
// ACK or peer call event arrives within this window, fail closed instead of
// leaving the Matrix side ringing for the full unanswered-call timeout.
var outgoingOfferAckTimeout = 12 * time.Second

// newEngine creates the engine for a Client.
func newEngine(c *Client) *engine {
	e := &engine{c: c, calls: map[string]*engineCall{}}
	if c != nil && c.wa != nil {
		e.sendCallNode = func(ctx context.Context, node waBinary.Node) error {
			return c.wa.DangerousInternals().SendNode(ctx, node)
		}
		e.requestCallNode = func(ctx context.Context, node waBinary.Node, requestID string) (*waBinary.Node, error) {
			di := c.wa.DangerousInternals()
			waiter := di.WaitResponse(requestID)
			defer di.CancelResponse(requestID, waiter)
			if err := di.SendNode(ctx, node); err != nil {
				return nil, err
			}
			select {
			case response := <-waiter:
				if response == nil {
					return nil, errors.New("meowcaller: call request returned no response")
				}
				return response, nil
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}
	}
	return e
}

func (e *engine) requireRawCallAdapter() error {
	// Source of truth: https://github.com/tulir/whatsmeow/blob/e9a033b24933cc8d90fa5ff8991b1268ba80a140/client.go#L110-L120
	if e == nil {
		return errors.New("meowcaller: raw call adapter is unavailable")
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.rawCallHookErr != nil {
		return fmt.Errorf("meowcaller: raw call adapter is unavailable: %w", e.rawCallHookErr)
	}
	return nil
}

// onEndFn returns the Call's OnEnd listener under its lock (the field is unexported
// and guarded by Call.mu; same-package engine code reads it through here).
func (c *Call) onEndFn() func(string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.onEnd
}

// onReadyFn returns the Call's OnReady listener under its lock.
func (c *Call) onReadyFn() func() {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.phase == CallPhaseEnded {
		return nil
	}
	c.mediaReady = true
	return c.onReady
}

// playerAndSink returns the Call's current Player and sink under its lock (the engine's
// media loop reads them every frame so a later Subscribe/Receive takes effect live).
func (c *Call) playerAndSink() (*Player, AudioSink) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.player, c.sink
}

// install wires the whatsmeow call event handlers and the <ack>/<call> interception.
// Call before the whatsmeow client connects.
func (e *engine) install() {
	if err := e.installCallAckHook(); err != nil {
		e.mu.Lock()
		e.rawCallHookErr = err
		e.mu.Unlock()
		e.c.log.Error().Err(err).Msg("raw call adapter is unavailable")
	}
	e.c.wa.AddEventHandler(func(evt any) {
		switch ev := evt.(type) {
		case *events.Disconnected, *events.StreamReplaced, *events.LoggedOut:
			e.callDispatch.discardPending()
		case *events.CallOffer:
			e.onOffer(ev)
		case *events.CallPreAccept:
			e.onPreAccept(ev)
		case *events.CallAccept:
			e.onAccept(ev)
		case *events.CallRelayLatency:
			e.onRelay(ev.BasicCallMeta, ev.Data)
			e.onRelayLatency(ev)
		case *events.CallTransport:
			e.onRelay(ev.BasicCallMeta, ev.Data)
		case *events.CallTerminate:
			e.onTerminateEvent(ev)
		case *events.CallReject:
			e.onReject(ev)
		case *events.UnknownCallEvent:
			e.onUnknownCallEvent(ev.Node)
		}
	})
}

func (e *engine) sendReaction(callID, emoji string) error {
	e.mu.Lock()
	m := e.calls[callID]
	if m == nil || m.call == nil || m.call.State() == CallPhaseEnded {
		e.mu.Unlock()
		return errors.New("meowcaller: call is not active")
	}
	sender := m.appDataTx
	e.mu.Unlock()
	if sender == nil {
		return errAppDataUnavailable
	}
	return sender.sendReaction(emoji)
}

func (e *engine) sendReactionContext(ctx context.Context, call *Call, emoji string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	e.mu.Lock()
	m := e.calls[call.ID()]
	if m == nil || m.call != call || call.State() != CallPhaseActive {
		e.mu.Unlock()
		return errAppDataUnavailable
	}
	sender := m.appDataTx
	e.mu.Unlock()
	if sender == nil {
		return errAppDataUnavailable
	}
	return sender.sendReactionContext(ctx, emoji)
}

// entry returns (creating if needed) the per-call state for callID.
func (e *engine) entry(callID string) *engineCall {
	if e.calls[callID] == nil {
		e.calls[callID] = &engineCall{}
	}
	return e.calls[callID]
}

// lookup returns the per-call state for callID, or nil.
func (e *engine) lookup(callID string) *engineCall {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.calls[callID]
}

func (e *engine) callIsVideo(callID string) bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	m := e.calls[callID]
	return m != nil && (m.localVideo || m.remoteVideo)
}

func (e *engine) callIsSendingVideo(callID string) bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.calls[callID] != nil && e.calls[callID].localVideo
}

func (e *engine) callIsReceivingVideo(callID string) bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.calls[callID] != nil && e.calls[callID].remoteVideo
}

func (e *engine) transmitCallNode(ctx context.Context, node waBinary.Node) error {
	if e.sendCallNode == nil {
		return errors.New("meowcaller: call signaling is unavailable")
	}
	return e.sendCallNode(ctx, node)
}

func (e *engine) nextCallNodeID() string {
	if e.c != nil && e.c.wa != nil {
		return e.c.wa.GenerateMessageID()
	}
	var id [8]byte
	_, _ = rand.Read(id[:])
	return strings.ToUpper(hex.EncodeToString(id[:]))
}

// sendVideoFrame packetizes one encoded H.264 access unit and sends it to the relay, if a
// video send pipeline is live for the call.
func (e *engine) sendVideoFrame(callID string, au []byte, duration time.Duration) error {
	e.mu.Lock()
	m := e.calls[callID]
	if m == nil || m.call == nil || m.call.State() == CallPhaseEnded {
		e.mu.Unlock()
		return errors.New("meowcaller: call is not active")
	}
	vs := m.videoTx
	e.mu.Unlock()
	if vs == nil {
		// Camera negotiation can finish before relay setup installs the RTP
		// sender. Do not end the call for a frame from that startup window.
		return ErrVideoFrameDropped
	}
	return vs.send(au, duration)
}

func (e *engine) transitionVideo(callID string, transition int) error {
	return e.transitionVideoContext(context.Background(), callID, nil, transition, 0, 0)
}

// decisionMode: 0 = existing provider API, 1 = automatic gateway, 2 = explicit manual decision.
func (e *engine) transitionVideoContext(ctx context.Context, callID string, expected *Call, transition int, generation uint64, decisionMode int) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	e.mu.Lock()
	m := e.calls[callID]
	if m == nil || m.call == nil || m.call.State() == CallPhaseEnded || (expected != nil && m.call != expected) {
		e.mu.Unlock()
		return errors.New("meowcaller: call is not active")
	}
	if decisionMode != 0 {
		if m.group || generation == 0 || m.peerVideoGeneration != generation || !m.peerVideoUpgrade {
			e.mu.Unlock()
			return ErrNoPendingVideoUpgrade
		}
		if m.manualVideoDecision != (decisionMode == 2) {
			e.mu.Unlock()
			return ErrVideoDecisionMode
		}
		if m.peerVideoDecisionInFlight {
			e.mu.Unlock()
			return ErrVideoDecisionBusy
		}
	}
	if err := ctx.Err(); err != nil {
		e.mu.Unlock()
		return err
	}
	to, creator, sender := m.from, m.creator, m.videoTx
	localVideoActive := m.localVideo
	// A stopped pending outbound upgrade is a cancellation, not a camera
	// stop. A peer's independent inbound request is not affected.
	if transition == signaling.VideoStateStopped && m.videoGate {
		transition = signaling.VideoStateUpgradeCancel
	}
	switch transition {
	case signaling.VideoStateUpgradeRequestV2:
		if m.group || m.videoGate || m.videoMode {
			e.mu.Unlock()
			return nil
		}
		m.localVideo = true
		m.videoGate = true
	case signaling.VideoStateUpgradeAccept:
		if !m.peerVideoUpgrade {
			e.mu.Unlock()
			return ErrNoPendingVideoUpgrade
		}
		m.peerVideoUpgrade = false
	case signaling.VideoStateUpgradeReject:
		if !m.peerVideoUpgrade {
			e.mu.Unlock()
			return ErrNoPendingVideoUpgrade
		}
		m.peerVideoUpgrade = false
	case signaling.VideoStateUpgradeCancel:
		m.localVideo = false
		m.videoGate = false
	case signaling.VideoStateStopped:
		if !m.group {
			m.videoMode = m.remoteVideo
		}
		m.localVideo = false
		m.videoGate = false
	default:
		e.mu.Unlock()
		return fmt.Errorf("meowcaller: unsupported local video transition %d", transition)
	}
	if decisionMode != 0 {
		m.peerVideoDecisionInFlight = true
		defer func() {
			e.mu.Lock()
			if e.calls[callID] == m {
				m.peerVideoDecisionInFlight = false
			}
			e.mu.Unlock()
		}()
	}
	e.mu.Unlock()
	if sender != nil {
		if transition == signaling.VideoStateStopped || transition == signaling.VideoStateUpgradeCancel {
			sender.disable()
		} else if transition == signaling.VideoStateUpgradeRequestV2 {
			sender.enable(true)
		}
	}

	build := func(state int, dec string, orientation *int) waBinary.Node {
		return signaling.BuildVideoStateWithParams(signaling.VideoStateParams{
			CallID: callID, To: to, CallCreator: creator, WrapperID: e.nextCallNodeID(),
			State: state, Dec: dec, DeviceOrientation: orientation,
		})
	}
	send := func(state int, dec string, orientation *int) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		e.mu.Lock()
		current := e.calls[callID] == m && m.call.State() != CallPhaseEnded
		if decisionMode != 0 {
			current = current && m.peerVideoGeneration == generation
		}
		e.mu.Unlock()
		if !current {
			return errors.New("meowcaller: video call ownership changed")
		}
		return e.transmitCallNode(ctx, build(state, dec, orientation))
	}

	var err error
	switch transition {
	case signaling.VideoStateUpgradeRequestV2:
		orientation := 0
		err = send(transition, signaling.VideoDecRequest, &orientation)
	case signaling.VideoStateUpgradeAccept:
		if !localVideoActive {
			orientation := 0
			err = send(signaling.VideoStateStopped, "", &orientation)
		}
		if err == nil {
			err = send(transition, signaling.VideoDecAccept, nil)
		}
	case signaling.VideoStateStopped:
		orientation := 0
		err = send(transition, "", &orientation)
	case signaling.VideoStateUpgradeReject, signaling.VideoStateUpgradeCancel:
		err = send(transition, "", nil)
	}
	if err == nil && transition == signaling.VideoStateUpgradeAccept {
		e.mu.Lock()
		if e.calls[callID] == m {
			m.videoMode = true
		}
		e.mu.Unlock()
	}
	// Do not reopen a consumed peer decision or locally cancelled request after an ambiguous send.
	if err == nil || transition == signaling.VideoStateStopped || transition == signaling.VideoStateUpgradeCancel || transition == signaling.VideoStateUpgradeReject || transition == signaling.VideoStateUpgradeAccept {
		return err
	}

	e.mu.Lock()
	var currentSender *videoSender
	if current := e.calls[callID]; current == m {
		current.localVideo = false
		current.videoGate = false
		currentSender = current.videoTx
	}
	e.mu.Unlock()
	if currentSender != nil {
		currentSender.disable()
	}
	return err
}

func (e *engine) setVideoEnabled(callID string, enabled bool) error {
	e.mu.Lock()
	m := e.calls[callID]
	if m == nil || m.call == nil || m.call.State() == CallPhaseEnded {
		e.mu.Unlock()
		return errors.New("meowcaller: call is not active")
	}
	if m.videoGate {
		e.mu.Unlock()
		if enabled {
			return nil
		}
		return e.transitionVideo(callID, signaling.VideoStateStopped)
	}
	if !m.videoMode && !m.localVideo && enabled {
		group := m.group
		e.mu.Unlock()
		if group {
			return nil
		}
		return e.transitionVideo(callID, signaling.VideoStateUpgradeRequestV2)
	}
	if enabled && m.localVideo {
		e.mu.Unlock()
		return nil
	}
	if m.localVideo && !m.videoGate {
		m.videoMode = true
	}
	m.localVideo = enabled
	m.videoGate = false
	to, creator, sender := m.from, m.creator, m.videoTx
	e.mu.Unlock()

	if sender != nil {
		if enabled {
			sender.enable(false)
		} else {
			sender.disable()
		}
	}
	state, dec := signaling.VideoStateDisabled, ""
	if enabled {
		state, dec = signaling.VideoStateEnabled, signaling.VideoStateDecH264
	}
	node := signaling.BuildVideoStateWithParams(signaling.VideoStateParams{
		CallID: callID, To: to, CallCreator: creator, WrapperID: e.nextCallNodeID(),
		State: state, Dec: dec,
	})
	err := e.transmitCallNode(context.Background(), node)
	if err == nil || !enabled {
		return err
	}

	e.mu.Lock()
	if current := e.calls[callID]; current == m {
		current.localVideo = false
	}
	e.mu.Unlock()
	if sender != nil {
		sender.disable()
	}
	return err
}

func (e *engine) setMuted(callID string, muted bool) error {
	e.mu.Lock()
	m := e.calls[callID]
	if m == nil || m.call == nil || m.call.State() == CallPhaseEnded {
		e.mu.Unlock()
		return errors.New("meowcaller: call is not active")
	}
	to, creator := m.from, m.creator
	e.mu.Unlock()
	muteState := "0"
	if muted {
		muteState = "1"
	}
	node := signaling.BuildMuteV2(callID, to, creator, muteState)
	return e.transmitCallNode(context.Background(), node)
}

func (e *engine) setVideoOrientation(callID string, orientation int) error {
	if orientation < 0 || orientation > 3 {
		return fmt.Errorf("meowcaller: video orientation %d is outside 0..3", orientation)
	}
	e.mu.Lock()
	m := e.calls[callID]
	if m == nil || m.call == nil || m.call.State() == CallPhaseEnded || !m.localVideo || m.videoGate {
		e.mu.Unlock()
		return ErrVideoFrameDropped
	}
	to, creator := m.from, m.creator
	e.mu.Unlock()
	node := signaling.BuildVideoStateWithParams(signaling.VideoStateParams{
		CallID: callID, To: to, CallCreator: creator, WrapperID: e.nextCallNodeID(),
		State: signaling.VideoStateEnabled, DeviceOrientation: &orientation,
	})
	return e.transmitCallNode(context.Background(), node)
}

// placeCall resolves target to a LID, builds and sends the <offer>, registers the Call,
// and returns it; media starts when the peer answers and the relay endpoint arrives.
func (e *engine) placeCall(ctx context.Context, target string, opts CallOptions) (*Call, error) {
	cli := e.c.wa
	self := cli.Store.GetLID()
	if self.IsEmpty() {
		return nil, errors.New("meowcaller: no own LID on this session")
	}
	peerLID, err := resolvePeerLID(ctx, cli, target)
	if err != nil {
		return nil, err
	}
	e.c.log.Info().Str("peer_lid", peerLID.String()).Str("self_lid", self.String()).Msg("resolved peer LID")

	devices, err := cli.GetUserDevices(ctx, []types.JID{peerLID})
	if err != nil {
		return nil, fmt.Errorf("device discovery: %w", err)
	}
	if len(devices) == 0 {
		return nil, fmt.Errorf("peer %s has no devices (unreachable / not on WhatsApp)", peerLID)
	}

	var callKey [32]byte
	if _, err := rand.Read(callKey[:]); err != nil {
		return nil, err
	}
	deviceKeys := make([]signaling.OfferDeviceKey, 0, len(devices))
	needIdentity := false
	for _, dev := range devices {
		ct, encType, ni, err := encryptCallKeyForDevice(ctx, cli, dev, callKey[:])
		if err != nil {
			return nil, fmt.Errorf("encrypt callKey for %s: %w", dev, err)
		}
		needIdentity = needIdentity || ni
		deviceKeys = append(deviceKeys, signaling.OfferDeviceKey{DeviceJid: dev, Ciphertext: ct, EncType: encType})
	}

	// pkmsg offers must carry our signed device identity so the peer can verify the new
	// session; the server drops the offer (no ack) otherwise.
	var deviceIdentity []byte
	if needIdentity {
		deviceIdentity, err = proto.Marshal(cli.Store.Account)
		if err != nil {
			return nil, fmt.Errorf("marshal device identity: %w", err)
		}
	}

	// Include the peer's privacy token when we have one (the server requires it to
	// place a call to a contact with privacy enabled).
	var privacyToken []byte
	if pt, err := cli.Store.PrivacyTokens.GetPrivacyToken(ctx, peerLID); err == nil && pt != nil {
		privacyToken = pt.Token
	}

	callID := newCallID()
	offer := signaling.BuildOffer(&signaling.OfferParams{
		CallID:         callID,
		To:             peerLID,
		CallCreator:    self,
		DeviceKeys:     deviceKeys,
		PrivacyToken:   privacyToken,
		Capability:     signaling.CapabilityOffer,
		DeviceIdentity: deviceIdentity,
		Video:          opts.Video,
	})
	// The builder leaves the <call> stanza id to the I/O layer; without it the server
	// can't route/ack the offer, so it never reaches the callee.
	offerNodeID := cli.GenerateMessageID()
	offer.Attrs["id"] = offerNodeID

	call := &Call{eng: e, id: callID, peer: peerLID, phase: CallPhaseCalling}

	e.mu.Lock()
	m := e.entry(callID)
	m.call = call
	m.callKey = callKey[:]
	m.selfLID = self.String()
	m.peerLID = peerLID.String()
	m.creator = self
	m.from = peerLID
	m.direction = CallDirectionOutgoing
	m.videoMode = opts.Video
	m.localVideo = opts.Video
	m.remoteVideo = opts.Video
	m.offerNodeID = offerNodeID
	m.inviteSelfDevice = groupCallDevice{
		JID: self, CapabilityVersion: 1,
		Capability: append([]byte(nil), signaling.CapabilityOffer...),
	}
	e.mu.Unlock()

	e.c.diag.Emit("keying", map[string]any{
		"call_id": callID, "direction": "out", "self_lid": self.String(),
		"peer_lid": peerLID.String(), "device_count": len(deviceKeys),
		"call_key_hex": hex.EncodeToString(callKey[:]),
	})

	if err := cli.DangerousInternals().SendNode(ctx, offer); err != nil {
		e.finishCall(callID, "offer_failed")
		return nil, fmt.Errorf("send offer: %w", err)
	}
	e.c.log.Info().Str("call_id", callID).Str("offer_node_id", offerNodeID).Bool("video", opts.Video).Msg("offer sent; media starts when the relay endpoint arrives")
	e.c.diag.Emit("meta", map[string]any{"event": "offer_sent", "call_id": callID, "offer_node_id": offerNodeID, "peer_lid": peerLID.String(), "direction": "out", "video": opts.Video})
	go e.watchOutgoingOfferAck(call, offerNodeID)
	return call, nil
}

func (e *engine) watchOutgoingOfferAck(call *Call, offerNodeID string) {
	if call == nil || offerNodeID == "" {
		return
	}
	timer := time.NewTimer(outgoingOfferAckTimeout)
	defer timer.Stop()
	<-timer.C

	if e.finishPendingOffer(call, offerNodeID, "server:no-offer-ack", true) {
		e.c.log.Warn().
			Str("call_id", call.ID()).
			Str("offer_node_id", offerNodeID).
			Dur("timeout", outgoingOfferAckTimeout).
			Msg("outbound WhatsApp offer received no usable call ACK; ended call")
	}
}

// onOffer handles an inbound <offer> event: it decrypts the callKey, captures any relay
// data, registers the Call in the Ringing phase, checks local admission, then sends the <preaccept> eagerly (a
// preparation step, independent of the later Answer/Reject), and fires the
// OnIncomingCall listener. Only the <accept> is deferred to Answer.
func (e *engine) onOffer(ev *events.CallOffer) {
	// Source of truth: https://github.com/purpshell/meowcaller/blob/33854919e64bdd4b053054ac9764d8fc63027b57/datasheets/voip-group-invite-accept.md#L28-L40
	if ev.Data == nil || ev.CallID == "" || (!ev.Timestamp.IsZero() && time.Since(ev.Timestamp) > 2*time.Minute) {
		return
	}
	e.mu.Lock()
	active := e.recentlyEnded(ev.CallID) || (e.calls[ev.CallID] != nil && e.calls[ev.CallID].call != nil)
	e.mu.Unlock()
	if active {
		return
	}
	attrs := ev.Data.AttrGetter()
	if attrs.OptionalString("is_call_ended") == "1" || attrs.OptionalString("terminate_reason") != "" {
		return
	}
	admit, deliver := e.c.incomingOfferCallbacks()
	groupSnapshot, isGroup, groupErr := signaling.ParseGroupInviteSnapshot(ev.Data)
	if groupErr != nil {
		e.c.log.Warn().Err(groupErr).Str("call_id", ev.CallID).Msg("parse inbound group offer failed")
		return
	}
	if isGroup {
		if groupSnapshot.GroupJID.IsEmpty() {
			groupSnapshot.GroupJID = ev.GroupJID
		}
		if !ev.GroupJID.IsEmpty() && groupSnapshot.GroupJID != ev.GroupJID {
			return
		}
		e.onGroupOffer(ev, groupCallUpdateFromSignaling(*groupSnapshot), admit, deliver)
		return
	}

	e.mu.Lock()
	terminated := hasEarlyTermination(e.earlyTerminations, ev.BasicCallMeta, false, time.Now())
	e.mu.Unlock()
	if terminated {
		return
	}

	callKey, err := decryptInboundCallKey(context.Background(), e.c.wa, ev)
	if err != nil {
		e.c.log.Warn().Err(err).Str("call_id", ev.CallID).Msg("decrypt callKey failed")
		return
	}
	e.c.log.Info().Int("key_bytes", len(callKey)).Str("call_id", ev.CallID).Msg("decrypted inbound callKey")
	e.c.diag.Emit("keying", map[string]any{
		"call_id": ev.CallID, "direction": "in", "from": ev.From.String(),
		"call_key_hex": hex.EncodeToString(callKey),
	})

	peer := ev.CallCreator
	if peer.IsEmpty() {
		peer = ev.From
	}
	e.c.diag.Emit("meta", map[string]any{
		"event": "offer_received", "call_id": ev.CallID,
		"from": ev.From.String(), "peer": peer.String(),
	})
	call := &Call{eng: e, id: ev.CallID, peer: peer, phase: CallPhaseRinging}

	e.mu.Lock()
	if e.recentlyEnded(ev.CallID) || hasEarlyTermination(e.earlyTerminations, ev.BasicCallMeta, false, time.Now()) {
		e.mu.Unlock()
		return
	}
	m := e.entry(ev.CallID)
	if m.call != nil {
		e.mu.Unlock()
		return
	}
	m.call = call
	m.callKey = callKey
	m.selfLID = e.c.wa.Store.GetLID().String()
	m.peerLID = peer.String()
	m.creator = ev.CallCreator
	m.from = ev.From
	m.direction = CallDirectionIncoming
	// A <video> child marks a call that starts with both video directions enabled.
	isVideo := signaling.OfferHasVideo(ev.Data)
	m.videoMode = isVideo
	m.localVideo = isVideo
	m.remoteVideo = isVideo
	m.inviteSelfDevice = groupCallDevice{
		JID: e.c.wa.Store.GetLID(), CapabilityVersion: 1,
		Capability: append([]byte(nil), signaling.CapabilityOffer...),
	}
	if device, ok := inviteDeviceCapability(ev.From, ev.Data); ok {
		m.invitePeerDevice = device
	}
	if r := findRelay(ev.Data); r != nil {
		rd := parseRelayData(r)
		if directRelayPeerMatches(m.from, rd.peerJID) {
			m.relay = rd
			m.peerLID = preferQualifiedPeer(m.peerLID, rd.peerJID)
		}
	}
	e.applyVoipSettingsCodec(m, ev.Data, ev.CallID)
	e.mu.Unlock()
	if isVideo {
		e.c.log.Info().Str("call_id", ev.CallID).Msg("inbound call advertises video")
	}

	if !e.admitIncomingPreparationWith(call, admit) {
		return
	}

	// Preaccept eagerly: it is a preparation step, done independently of the later
	// Answer/Reject decision. It keeps the offer alive and joins the relay election while
	// the integrator decides — even a call the user goes on to decline has usually already
	// been preaccepted.
	if err := e.sendPreaccept(ev.CallID, ev.From, ev.CallCreator, isVideo); err != nil {
		e.c.log.Warn().Err(err).Str("call_id", ev.CallID).Msg("preaccept failed")
	}

	if deliver != nil {
		deliver(call)
	}
}

func (e *engine) onGroupOffer(ev *events.CallOffer, update groupCallUpdate, admit func(*Call) bool, deliver func(*Call)) {
	// Source of truth: https://github.com/purpshell/meowcaller/blob/33854919e64bdd4b053054ac9764d8fc63027b57/datasheets/voip-group-invite-accept.md#L28-L40
	peer := ev.CallCreator
	if peer.IsEmpty() {
		peer = ev.From
	}
	call := &Call{eng: e, id: ev.CallID, peer: peer, phase: CallPhaseRinging}
	isVideo := signaling.OfferHasVideo(ev.Data)
	meta := ev.BasicCallMeta
	if meta.GroupJID.IsEmpty() {
		meta.GroupJID = update.GroupJID
	}
	e.mu.Lock()
	if e.recentlyEnded(ev.CallID) || hasEarlyTermination(e.earlyTerminations, meta, true, time.Now()) {
		e.mu.Unlock()
		return
	}
	m := e.entry(ev.CallID)
	if m.call != nil {
		e.mu.Unlock()
		return
	}
	m.call = call
	m.selfLID = e.c.wa.Store.GetLID().String()
	m.peerLID = peer.String()
	m.creator = ev.CallCreator
	if m.creator.IsEmpty() {
		m.creator = peer
	}
	m.from = types.NewJID(ev.CallID, "call")
	m.direction = CallDirectionIncoming
	m.group = true
	m.videoMode = isVideo
	m.localVideo = isVideo
	m.remoteVideo = isVideo
	creator := m.creator
	e.mu.Unlock()
	if !e.admitIncomingPreparationWith(call, admit) {
		return
	}
	e.applyGroupUpdate(update)
	if current := e.lookup(call.ID()); current != m || call.State() == CallPhaseEnded {
		return
	}

	preaccept, err := signaling.BuildActiveGroupPreaccept(
		ev.CallID,
		creator,
		e.nextCallNodeID(),
		isVideo,
	)
	if err != nil {
		e.c.log.Warn().Err(err).Str("call_id", ev.CallID).Msg("build group preaccept failed")
		e.finishCallIfCurrent(call, "group-preaccept-build-failed")
		return
	}
	if err = e.transmitCallNode(context.Background(), preaccept); err != nil {
		e.c.log.Warn().Err(err).Str("call_id", ev.CallID).Msg("send group preaccept failed")
		e.finishCallIfCurrent(call, "group-preaccept-send-failed")
		return
	}
	e.c.log.Info().Str("call_id", ev.CallID).Bool("video", isVideo).Msg("active group invite preaccepted")
	if deliver != nil {
		deliver(call)
	}
}

// The optional bridge policy is checked before any preaccept is sent. Keep
// Signal offer decryption in its existing order, even for unadmitted calls.
func (e *engine) admitIncomingPreparation(call *Call) bool {
	return e.admitIncomingPreparationWith(call, e.c.admitsIncomingCall)
}

func (e *engine) admitIncomingPreparationWith(call *Call, admit func(*Call) bool) bool {
	if !e.incomingPreparationCurrent(call) {
		return false
	}
	if admit == nil || !admit(call) {
		call.DismissUnanswered()
		return false
	}
	return e.incomingPreparationCurrent(call)
}

func (e *engine) incomingPreparationCurrent(call *Call) bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	m := e.calls[call.ID()]
	return m != nil && m.call == call && m.direction == CallDirectionIncoming &&
		call.State() == CallPhaseRinging && !m.acceptPending && !m.started
}

// sendPreaccept sends the <preaccept> for an inbound call — a preparation step done
// eagerly when the offer arrives (see onOffer), independent of the later Answer/Reject
// decision. Video calls also advertise the H.264 decoder before the final accept.
func (e *engine) sendPreaccept(callID string, to, creator types.JID, video bool) error {
	pre := signaling.BuildPreaccept(
		callID,
		to,
		creator,
		e.c.wa.DangerousInternals().GenerateRequestID(),
		[]string{"16000"},
		video,
	)
	if err := e.c.wa.DangerousInternals().SendNode(context.Background(), pre); err != nil {
		return fmt.Errorf("send preaccept: %w", err)
	}
	e.c.log.Info().Str("call_id", callID).Msg("preaccepted (preparation; awaiting Answer/Reject)")
	return nil
}

// answer submits the final accept after the integrator authorizes pickup.
// A remote mute indication is optional and may arrive only after acceptance.
// Preaccept remains a separate preparation step.
func (e *engine) answer(c *Call) error {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	return e.answerContext(ctx, c)
}

// releaseUnsentPickup permits a retry only when no accept was submitted.
// The reservation is owned by the exact registered call, not just its ID.
func (e *engine) releaseUnsentPickup(c *Call, m *engineCall) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.calls[c.id] == m && m.call == c {
		m.acceptPending = false
		m.pickupCancel = nil
	}
}

func (e *engine) answerContext(ctx context.Context, c *Call) error {
	if err := ctx.Err(); err != nil {
		return pickupNotSubmitted(err)
	}
	e.mu.Lock()
	m := e.calls[c.id]
	if m == nil || m.call != c {
		e.mu.Unlock()
		return pickupNotSubmitted(errors.New("meowcaller: call ended"))
	}
	if m.acceptPending {
		e.mu.Unlock()
		return errors.New("meowcaller: pickup is already reserved or submitted")
	}
	m.acceptPending = true
	ctx, cancel := context.WithCancel(ctx)
	m.pickupCancel = cancel
	group, groupCreator := m.group, m.creator
	e.mu.Unlock()

	// Once submission begins, even an error may mean the peer received the
	// accept. Do not automatically unlock that ambiguous outcome for retry.
	submitted := false
	defer func() {
		cancel()
		if !submitted {
			e.releaseUnsentPickup(c, m)
		} else {
			e.mu.Lock()
			if e.calls[c.id] == m {
				m.pickupCancel = nil
			}
			e.mu.Unlock()
		}
	}()
	var accept waBinary.Node
	if group {
		var err error
		accept, err = signaling.BuildActiveGroupAccept(c.id, groupCreator, e.nextCallNodeID())
		if err != nil {
			return pickupNotSubmitted(err)
		}
	} else {
		e.mu.Lock()
		if e.calls[c.id] != m {
			e.mu.Unlock()
			return pickupNotSubmitted(errors.New("meowcaller: call ended before pickup"))
		}
		to, creator, isVideo := m.from, m.creator, m.localVideo || m.remoteVideo
		e.mu.Unlock()
		accept = signaling.BuildAccept(&signaling.AcceptParams{
			CallID: c.id, To: to, CallCreator: creator, AudioRates: []string{"16000"},
			Metadata: waBinary.Attrs{"peer_abtest_bucket_id_list": "125208,94276"}, Video: isVideo,
		})
		accept.Attrs["id"] = e.nextCallNodeID()
	}
	// Commit submission under the same registry lock as terminal removal.
	// A terminal action that wins first cancels this attempt without any send.
	e.mu.Lock()
	if e.calls[c.id] != m {
		e.mu.Unlock()
		return pickupNotSubmitted(errors.New("meowcaller: call ended before pickup"))
	}
	if err := ctx.Err(); err != nil {
		e.mu.Unlock()
		return pickupNotSubmitted(err)
	}
	if e.sendCallNode == nil {
		e.mu.Unlock()
		return pickupNotSubmitted(errors.New("meowcaller: call signaling is unavailable"))
	}
	done := make(chan struct{})
	m.pickupSendDone = done
	submitted = true
	e.mu.Unlock()
	e.c.log.Info().Str("call_id", c.id).Bool("group", group).Msg("submitting authorized WhatsApp pickup")
	err := e.transmitCallNode(ctx, accept)
	close(done)
	if err != nil {
		return pickupOutcomeUnknown(fmt.Errorf("send accept: %w", err))
	}
	e.mu.Lock()
	current := e.calls[c.id] == m
	e.mu.Unlock()
	if !current {
		return pickupOutcomeUnknown(errors.New("meowcaller: call ended during pickup"))
	}
	if err := ctx.Err(); err != nil {
		return pickupOutcomeUnknown(err)
	}
	e.c.log.Info().Str("call_id", c.id).Msg("WhatsApp pickup submitted")
	c.advancePhase(CallPhaseConnecting)
	e.maybeStartMediaMatching(c.id, m)
	return nil
}

// terminalTarget captures routing for the exact registered call. A stale handle
// must never derive the destination from a newer registration with the same ID.
func (e *engine) terminalTarget(c *Call) (to, creator types.JID, owner *engineCall) {
	e.mu.Lock()
	defer e.mu.Unlock()
	m := e.calls[c.id]
	if m == nil || m.call != c {
		return types.JID{}, types.JID{}, nil
	}
	return m.from, m.creator, m
}

func (e *engine) reject(c *Call) error { return e.endLocally(c, true) }

// hangup ends a call (either direction) and tears down its media.
func (e *engine) hangup(c *Call) error { return e.endLocally(c, false) }

func (e *engine) endLocally(c *Call, reject bool) error {
	reason := "hangup"
	if reject {
		reason = "rejected"
	}
	return e.endLocallyWithReason(c, reject, reason)
}

// Media failures must retire the exact call and signal its remote termination,
// just like a user hangup. Preserve the failure reason for local observers.
func (e *engine) endLocallyWithReason(c *Call, reject bool, reason string) error {
	if c == nil {
		return nil
	}
	to, creator, owner := e.terminalTarget(c)
	if owner == nil || !e.finishCallMatching(c.id, reason, false, c) {
		return nil
	}
	// Removal cancels any pending pickup and prevents future submissions.
	// Read after removal: a pickup could have committed between target lookup
	// and removal. An already-submitted accept makes a reject ambiguous; use
	// terminate, never another accept, and order it after provider settlement.
	e.mu.Lock()
	done := owner.pickupSendDone
	reservation := owner.terminalReservation
	e.mu.Unlock()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if err := waitPickupSettlement(ctx, done); err != nil {
		return err
	}
	if done != nil {
		reject = false
	}
	// A replaced stream is marked logged out before dispatching StreamReplaced.
	if e.c != nil && e.c.wa != nil && !e.c.wa.IsLoggedIn() {
		return nil
	}
	e.mu.Lock()
	current := e.calls[c.id] == nil && time.Now().Before(reservation) && e.ended[c.id] == reservation
	e.mu.Unlock()
	if !current {
		return nil
	}
	var node waBinary.Node
	if reject {
		node = signaling.BuildReject(c.id, to, creator)
	} else {
		node = signaling.BuildTerminate(&signaling.TerminateParams{CallID: c.id, To: to, CallCreator: creator})
	}
	node.Attrs["id"] = e.nextCallNodeID()
	if err := e.transmitCallNode(ctx, node); err != nil {
		return fmt.Errorf("send terminal call signal: %w", err)
	}
	return nil
}

// onRelay records relay data from a relaylatency/transport/ack stanza and starts media
// once both the callKey and the relay endpoint are known.
func (e *engine) onRelay(meta types.BasicCallMeta, data *waBinary.Node) {
	e.onRelayMatching(meta.CallID, data, false, &meta)
}

// For a direct offer ACK, correlate and commit under the registry lock.
func (e *engine) onRelayMatching(callID string, data *waBinary.Node, offerAck bool, meta *types.BasicCallMeta) {
	r := findRelay(data)
	if r == nil {
		return
	}
	rd := parseRelayData(r)
	var rekeyPeer func(string) error
	var peerLID string
	e.mu.Lock()
	m := e.calls[callID]
	if m == nil || m.call == nil || m.call.State() == CallPhaseEnded || (offerAck && (m.group ||
		m.direction != CallDirectionOutgoing ||
		m.offerNodeID == "" || data.AttrGetter().String("id") != m.offerNodeID)) {
		e.mu.Unlock()
		return
	}
	if !m.group && ((!offerAck && (meta == nil || !directLifecycleMatches(m.from, m.creator, *meta))) || !directRelayPeerMatches(m.from, rd.peerJID)) {
		e.mu.Unlock()
		return
	}
	if offerAck {
		m.offerNodeID = ""
		e.applyVoipSettingsCodec(m, data, callID)
		e.c.log.Info().Str("call_id", callID).Int("relay_candidates", len(rd.endpoints)).Msg("correlated outgoing relay allocation")
	}
	// Peer-only updates may omit allocation credentials. Preserve a complete
	// direct allocation rather than replacing it with an incomplete snapshot.
	if m.group || len(mediaRelayCandidates(rd, m.direction == CallDirectionIncoming, false)) > 0 ||
		len(mediaRelayCandidates(m.relay, m.direction == CallDirectionIncoming, false)) == 0 {
		m.relay = rd
	}
	if !rd.peerJID.IsEmpty() {
		peerLID = rd.peerJID.String()
		if !m.group {
			peerLID = preferQualifiedPeer(m.peerLID, rd.peerJID)
		}
		if peerLID != m.peerLID {
			m.peerLID = peerLID
			rekeyPeer = m.rekeyPeer
		}
	}
	e.mu.Unlock()
	if rekeyPeer != nil {
		if err := rekeyPeer(peerLID); err != nil {
			e.c.log.Warn().Err(err).Str("call_id", callID).Str("peer_lid", peerLID).
				Msg("failed to rekey media to relay-elected peer")
		} else {
			e.c.log.Info().Str("call_id", callID).Str("peer_lid", peerLID).
				Msg("rekeyed media to relay-elected peer")
		}
	}
	e.maybeStartMediaMatching(callID, m)
}

// onRelayLatency answers the caller's relaylatency probes (the callee's half of the
// relay election). Only authorized pickup submits the final accept.
func (e *engine) onRelayLatency(ev *events.CallRelayLatency) {
	e.mu.Lock()
	m := e.calls[ev.CallID]
	allowed := m != nil && m.call != nil && m.direction == CallDirectionIncoming && m.call.State() != CallPhaseEnded && (m.group || directLifecycleMatches(m.from, m.creator, ev.BasicCallMeta))
	e.mu.Unlock()
	if !allowed {
		return
	}
	rl := findChild(ev.Data, "relaylatency")
	if rl == nil {
		return
	}
	var probes []rlProbe
	for i := range rl.GetChildren() {
		te := &rl.GetChildren()[i]
		if te.Tag != "te" {
			continue
		}
		ag := te.AttrGetter()
		probes = append(probes, rlProbe{
			latency:   decodeLatency(ag.String("latency")),
			relayName: ag.String("relay_name"),
			addr:      nodeBytes(te),
		})
	}
	for _, p := range probes {
		e.mu.Lock()
		current := e.calls[ev.CallID] == m && m.call.State() != CallPhaseEnded && (m.group || directLifecycleMatches(m.from, m.creator, ev.BasicCallMeta))
		e.mu.Unlock()
		if !current {
			return
		}
		resp := signaling.BuildRelayLatency(&signaling.RelayLatencyParams{
			CallID:       ev.CallID,
			To:           ev.From,
			CallCreator:  ev.CallCreator,
			LatencyMs:    p.latency,
			RelayName:    p.relayName,
			AddressBytes: p.addr,
		})
		resp.Attrs["id"] = e.c.wa.GenerateMessageID()
		if err := e.c.wa.DangerousInternals().SendNode(context.Background(), resp); err != nil {
			e.c.log.Error().Err(err).Str("call_id", ev.CallID).Msg("send relaylatency failed")
			return
		}
	}
}

// onPreAccept records that the peer's device received and started preparing an outgoing call.
func (e *engine) onPreAccept(ev *events.CallPreAccept) {
	e.onPreAcceptMatching(ev, e.lookup(ev.CallID))
}

func (e *engine) onPreAcceptMatching(ev *events.CallPreAccept, m *engineCall) {
	e.mu.Lock()
	if m == nil || e.calls[ev.CallID] != m || m.direction != CallDirectionOutgoing {
		e.mu.Unlock()
		return
	}
	if !m.group && !directLifecycleMatches(m.from, m.creator, ev.BasicCallMeta) {
		e.mu.Unlock()
		return
	}
	call := m.call
	if device, ok := inviteDeviceCapability(ev.From, ev.Data); ok {
		m.invitePeerDevice = device
	}
	e.mu.Unlock()
	if call != nil {
		call.advancePhase(CallPhaseRinging)
	}
	e.c.log.Info().
		Str("call_id", ev.CallID).
		Str("from", ev.From.String()).
		Str("platform", ev.RemotePlatform).
		Msg("peer preaccepted outgoing call")
	e.c.diag.Emit("meta", map[string]any{
		"event": "peer_preaccept", "call_id": ev.CallID,
		"from": ev.From.String(), "platform": ev.RemotePlatform,
	})
}

// onAccept records that the peer answered an outgoing call. Media may already be running
// from relay allocation, but inbound RTP is still what marks the call ready/active.
func (e *engine) onAccept(ev *events.CallAccept) {
	e.onAcceptMatching(ev, e.lookup(ev.CallID))
}

func (e *engine) onAcceptMatching(ev *events.CallAccept, m *engineCall) {
	e.mu.Lock()
	if m == nil || e.calls[ev.CallID] != m || m.direction != CallDirectionOutgoing {
		e.mu.Unlock()
		e.c.log.Warn().Str("call_id", ev.CallID).Msg("outgoing accept ignored: no current outgoing call")
		return
	}
	if !m.group && !directLifecycleMatches(m.from, m.creator, ev.BasicCallMeta) {
		e.mu.Unlock()
		e.c.log.Warn().Str("call_id", ev.CallID).Msg("outgoing accept ignored: peer or creator mismatch")
		return
	}
	call, group := m.call, m.group
	video := m.localVideo || m.remoteVideo
	if call != nil && call.State() == CallPhaseEnded {
		e.mu.Unlock()
		return
	}
	var rekeyPeer func(string) error
	answeringPeer := ev.From.String()
	if !group {
		if device, ok := inviteDeviceCapability(ev.From, ev.Data); ok {
			m.invitePeerDevice = device
		}
		e.applyVoipSettingsCodec(m, ev.Data, ev.CallID)
		if !ev.From.IsEmpty() {
			// A bare replay must not discard the already selected answering device.
			if m.from.Device == 0 || ev.From.Device != 0 {
				m.from = ev.From
			}
			answeringPeer = preferQualifiedPeer(m.peerLID, ev.From)
		}
		if answeringPeer != "" && answeringPeer != m.peerLID {
			m.peerLID = answeringPeer
			rekeyPeer = m.rekeyPeer
		}
	}
	e.mu.Unlock()
	if rekeyPeer != nil {
		if err := rekeyPeer(answeringPeer); err != nil {
			e.c.log.Warn().Err(err).Str("call_id", ev.CallID).Str("peer_lid", answeringPeer).Msg("failed to rekey media to answering device")
		} else {
			e.c.log.Info().Str("call_id", ev.CallID).Str("peer_lid", answeringPeer).Msg("rekeyed media to answering device")
		}
	}
	if call != nil {
		call.advancePhase(CallPhaseConnecting)
		call.markPeerAccepted()
	}
	if group {
		// Source of truth: https://github.com/purpshell/meowcaller/blob/676ebee3eca513b5348fab36cae5c560cc791238/datasheets/voip-group-invite-accept.md#L26-L45
		e.c.log.Info().Str("call_id", ev.CallID).Str("from", ev.From.String()).Msg("participant accepted group call")
	} else {
		e.c.log.Info().
			Str("call_id", ev.CallID).
			Str("from", ev.From.String()).
			Str("platform", ev.RemotePlatform).
			Bool("video", video).
			Msg("peer accepted outgoing call")
		e.c.diag.Emit("meta", map[string]any{
			"event": "peer_accept", "call_id": ev.CallID,
			"from": ev.From.String(), "platform": ev.RemotePlatform, "video": video,
		})
	}
	e.maybeStartMediaMatching(ev.CallID, m)
}

func inviteDeviceCapability(device types.JID, node *waBinary.Node) (groupCallDevice, bool) {
	// Source of truth: https://github.com/purpshell/meowcaller/blob/1ebd064663ac336ff3d1fc65d9baa974148fe73e/datasheets/voip-group-participant-invite.md#L36-L72
	if device.IsEmpty() {
		return groupCallDevice{}, false
	}
	capability := findChild(node, "capability")
	if capability == nil {
		return groupCallDevice{}, false
	}
	value, ok := capability.Content.([]byte)
	if !ok || len(value) == 0 {
		return groupCallDevice{}, false
	}
	version, err := strconv.ParseUint(capability.AttrGetter().String("ver"), 10, 32)
	if err != nil {
		return groupCallDevice{}, false
	}
	return groupCallDevice{
		JID: device, CapabilityVersion: uint32(version),
		Capability: append([]byte(nil), value...),
	}, true
}

func preferQualifiedPeer(current string, signaled types.JID) string {
	if signaled.IsEmpty() {
		return current
	}
	parsed, err := types.ParseJID(current)
	if err == nil &&
		parsed.User == signaled.User &&
		parsed.Server == signaled.Server &&
		parsed.Device != 0 &&
		signaled.Device == 0 {
		return current
	}
	return signaled.String()
}

// onReject tears down an outgoing call when the peer declines it.
func (e *engine) onReject(ev *events.CallReject) {
	e.onRejectMatching(ev, e.lookup(ev.CallID))
}

func (e *engine) onRejectMatching(ev *events.CallReject, m *engineCall) {
	e.mu.Lock()
	if m == nil || e.calls[ev.CallID] != m || m.call == nil {
		e.mu.Unlock()
		return
	}
	if !m.group && !directLifecycleMatches(m.from, m.creator, ev.BasicCallMeta) {
		e.mu.Unlock()
		return
	}
	call, group := m.call, m.group
	e.mu.Unlock()
	e.c.log.Info().
		Str("call_id", ev.CallID).
		Str("from", ev.From.String()).
		Msg("peer rejected call")
	e.c.diag.Emit("meta", map[string]any{
		"event": "peer_reject", "call_id": ev.CallID, "from": ev.From.String(),
	})
	if group && ev.From.Server != "call" && ev.From.Server != types.GroupServer {
		return
	}
	e.finishCallMatchingOffer(ev.CallID, "rejected", false, call, "", false, false, &ev.BasicCallMeta)
}

// rlProbe is one relay candidate from a relaylatency probe.
type rlProbe struct {
	latency   uint32
	relayName string
	addr      []byte
}

// applyVoipSettingsCodec finds the <voip_settings> blob under node (an inbound
// <offer> or an outbound call <ack>), parses it, and records the selected audio
// codec on the call. Absent or unparseable settings leave the call on MLow. The
// caller holds e.mu.
func (e *engine) applyVoipSettingsCodec(m *engineCall, node *waBinary.Node, callID string) {
	vsNode := findChild(node, "voip_settings")
	if vsNode == nil {
		return
	}
	content, _ := vsNode.Content.([]byte)
	vs, err := signaling.ParseVoipSettings(content, e.c.log)
	if err != nil {
		e.c.log.Debug().Err(err).Str("call_id", callID).Msg("voip_settings parse failed; keeping mlow")
		return
	}
	if !vs.Present {
		return
	}
	// Partial settings updates must not reset an already selected codec.
	if vs.CodecExplicit {
		m.codec = selectAudioCodec(vs)
	}
	if vs.FrameMs != 0 {
		m.audioFrameMs = vs.FrameMs
	}
	if vs.TargetBitrate != 0 {
		m.audioBitrate = vs.TargetBitrate
	}
	if m.mediaCodecFailure != nil {
		m.mediaCodecFailure(m.codec, m.audioFrameMs, m.audioBitrate)
	}
	e.c.log.Info().
		Str("call_id", callID).
		Str("codec", m.codec.String()).
		Bool("use_mlow_codec_v1", vs.UseMlowCodecV1).
		Msg("selected audio codec from voip_settings")
}

func callAckTerminalNode(node *waBinary.Node) *waBinary.Node {
	if node == nil {
		return nil
	}
	if node.Tag == "error" || node.Tag == "nack" || node.Tag == "reject" {
		return node
	}
	for i := range node.GetChildren() {
		child := &node.GetChildren()[i]
		if found := callAckTerminalNode(child); found != nil {
			return found
		}
	}
	return nil
}

func callAckErrorCode(ack *waBinary.Node) string {
	if ack == nil {
		return ""
	}
	ag := ack.AttrGetter()
	if code := ag.String("error"); code != "" {
		return code
	}
	terminal := callAckTerminalNode(ack)
	if terminal != nil {
		tag := terminal.AttrGetter()
		for _, key := range []string{"error", "code", "status"} {
			if code := tag.String(key); code != "" {
				return code
			}
		}
		switch value := terminal.Content.(type) {
		case []byte:
			if code := strings.TrimSpace(string(value)); code != "" {
				return code
			}
		case string:
			if code := strings.TrimSpace(value); code != "" {
				return code
			}
		}
	}
	if typ := strings.ToLower(ag.String("type")); typ == "error" || typ == "reject" || typ == "nack" {
		return typ
	}
	return ""
}

func callAckEmbeddedCallID(node *waBinary.Node) string {
	if node == nil {
		return ""
	}
	if callID := node.AttrGetter().String("call-id"); callID != "" {
		return callID
	}
	for i := range node.GetChildren() {
		child := &node.GetChildren()[i]
		if callID := callAckEmbeddedCallID(child); callID != "" {
			return callID
		}
	}
	return ""
}

// callForOfferError returns only a pending outgoing offer with a correlated ACK.
// A lone active call is not evidence that an unrelated ACK belongs to it.
func (e *engine) callForOfferError(ack *waBinary.Node) (*Call, string) {
	if ack == nil {
		return nil, ""
	}
	embeddedID := callAckEmbeddedCallID(ack)
	stanzaID := ack.AttrGetter().String("id")
	if embeddedID == "" && stanzaID == "" {
		return nil, ""
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	var matched *Call
	var matchedOffer string
	for callID, m := range e.calls {
		if m == nil || m.call == nil || m.direction != CallDirectionOutgoing ||
			m.offerNodeID == "" || m.call.State() == CallPhaseEnded {
			continue
		}
		if embeddedID != "" && embeddedID != callID {
			continue
		}
		if stanzaID != "" && stanzaID != m.offerNodeID {
			continue
		}
		if matched != nil {
			return nil, ""
		}
		matched = m.call
		matchedOffer = m.offerNodeID
	}
	return matched, matchedOffer
}

// onCallAck handles an <ack class="call"> node. For an outbound offer the relay
// allocation arrives here (whatsmeow otherwise drops the ack), which is what lets the
// caller bring up media. Error ACKs are correlated by the outer offer stanza id when
// WhatsApp omits the inner call-id.
func (e *engine) onCallAck(ack *waBinary.Node) {
	if errCode := callAckErrorCode(ack); errCode != "" {
		call, offerNodeID := e.callForOfferError(ack)
		if call == nil {
			e.c.log.Debug().Str("error_code", errCode).Msg("ignored uncorrelated call error ACK")
			return
		}
		if e.finishPendingOffer(call, offerNodeID, "server:"+errCode, false) {
			e.c.log.Warn().Str("call_id", call.ID()).Str("error_code", errCode).Str("ack_id", ack.AttrGetter().String("id")).Msg("call rejected by server")
		}
		return
	}
	groupUpdate, isGroup, groupErr := signaling.ParseInitialGroupCallAck(ack)
	if groupErr != nil {
		e.c.log.Warn().Err(groupErr).Msg("parse initial group call ack failed")
		return
	}
	if isGroup {
		// Source of truth: https://github.com/purpshell/meowcaller/blob/7cb6045001dafd2514f53e85cd8c3e419c13adbe/datasheets/voip-initial-group-call.md#L27-L33
		update := groupCallUpdateFromSignaling(*groupUpdate)
		if ack.AttrGetter().OptionalString("type") == "offer" {
			e.applyGroupUpdateMatching(update, ack.AttrGetter().OptionalString("id"), true)
		} else {
			e.applyGroupUpdate(update)
		}
		return
	}
	r := findRelay(ack)
	if r == nil {
		return
	}
	callID := r.AttrGetter().String("call-id")
	if callID == "" {
		return
	}
	e.onRelayMatching(callID, ack, true, nil)
}

// onCallRaw inspects a raw <call> node before whatsmeow processes it. It returns true when
// it has fully handled the node (including sending the appropriate ack), so the caller skips
// whatsmeow's generic typeless ack.
func (e *engine) onCallRaw(callNode *waBinary.Node) bool {
	kids := callNode.GetChildren()
	if len(kids) == 0 {
		return false
	}
	switch kids[0].Tag {
	case "group_update", "enc_rekey", "waiting_room_update", "user_action", "screen_share":
		// Source of truth: https://github.com/purpshell/meowcaller/blob/36d54857c74e45ccb08f6444a32d2afa13f20be9/datasheets/group-video-reactions.md#L31-L56
		ack, ok := signaling.BuildCallControlAck(callNode, kids[0].Tag)
		if !ok {
			e.c.log.Warn().
				Str("action", kids[0].Tag).
				Msg("call control ack: missing id/from")
		} else if err := e.transmitCallNode(context.Background(), ack); err != nil {
			e.c.log.Warn().
				Err(err).
				Str("action", kids[0].Tag).
				Str("id", ack.AttrGetter().String("id")).
				Msg("send typed call control ack failed")
		}
		e.onUnknownCallEvent(callNode)
		return true
	case "mute_v2":
		mv := kids[0].AttrGetter()
		callID := mv.String("call-id")
		if callID == "" {
			return false
		}
		muteState := mv.String("mute-state")
		if muteState != "0" && muteState != "1" {
			return false
		}
		e.mu.Lock()
		m := e.calls[callID]
		var call *Call
		isGroup := m != nil && m.group
		var groupTransaction uint32
		if isGroup && m.groupUpdate != nil {
			groupTransaction = m.groupUpdate.TransactionID
		}
		if m != nil && (isGroup || matchesDirectControlNode(m, callNode, &kids[0])) {
			m.seenMute = true
			if !m.group {
				call = m.call
			}
		}
		e.mu.Unlock()
		if isGroup {
			value := "unmuted"
			if muteState == "1" {
				value = "muted"
			}
			e.observeGroupDeviceMedia(callNode, m, groupTransaction, "mute_v2", value)
		} else if call != nil {
			call.recordMute(muteState == "1")
		}
		return false
	case "video":
		// Acknowledge the <video> stanza with type="video" — the mid-call video-upgrade
		// (state=11) acceptance signal. whatsmeow's generic typeless ack does not satisfy the
		// sender, which then cancels the upgrade after ~5s before streaming any video.
		callID := kids[0].AttrGetter().String("call-id")
		e.mu.Lock()
		m := e.calls[callID]
		allowed := m != nil && (m.group || matchesDirectControlNode(m, callNode, &kids[0]))
		var groupTransaction uint32
		if m != nil && m.group && m.groupUpdate != nil {
			groupTransaction = m.groupUpdate.TransactionID
		}
		e.mu.Unlock()
		if !allowed {
			return false
		}
		e.ackVideoStanza(callNode)
		e.onVideoStanza(&kids[0], m, callNode, groupTransaction)
		return true
	}
	return false
}

// matchesDirectControlNode is evaluated while holding the engine lock.
// The outer sender and inner call creator are independent routing fields.
func matchesDirectControlNode(m *engineCall, outer, action *waBinary.Node) bool {
	if m == nil || m.call == nil || outer == nil || action == nil {
		return false
	}
	outerAttrs, actionAttrs := outer.AttrGetter(), action.AttrGetter()
	sender := outerAttrs.JID("from")
	participant := outerAttrs.OptionalJIDOrEmpty("participant")
	creator := actionAttrs.JID("call-creator")
	group := actionAttrs.OptionalJIDOrEmpty("group-jid")
	return group.IsEmpty() && len(outerAttrs.Errors) == 0 && len(actionAttrs.Errors) == 0 &&
		directControlMatches(m.from, m.creator, sender, creator, participant)
}

// ackVideoStanza sends the typed <ack class="call" type="video"> for an inbound <video>
// <call> node (replicating the real WhatsApp client; whatsmeow would otherwise send a bare
// typeless ack that the peer treats as non-acceptance of a video upgrade).
func (e *engine) ackVideoStanza(callNode *waBinary.Node) {
	ack, ok := signaling.BuildVideoAck(callNode)
	if !ok {
		e.c.log.Warn().Msg("video ack: missing id/from, not acking")
		return
	}
	if err := e.c.wa.DangerousInternals().SendNode(context.Background(), ack); err != nil {
		e.c.log.Warn().Err(err).Str("id", ack.AttrGetter().String("id")).Msg("send video ack failed")
		return
	}
	e.c.log.Debug().Str("id", ack.AttrGetter().String("id")).Msg("sent type=video ack")
}

// onVideoStanza handles an inbound standalone <video> state stanza — the peer's video
// on/off and device orientation — and fires the Call's OnVideoState listener. whatsmeow
// surfaces no event for it (same as mute_v2), so it is intercepted here.
func (e *engine) onVideoStanza(v *waBinary.Node, expected *engineCall, outer *waBinary.Node, groupTransaction uint32) {
	ag := v.AttrGetter()
	callID := ag.String("call-id")
	if callID == "" {
		return
	}
	state, err := strconv.Atoi(ag.OptionalString("state"))
	if err != nil {
		return
	}
	switch state {
	case 0, 1, 3, 4, 5, 6, 8, 11:
	default:
		return
	}
	orientation := 0
	if raw := ag.OptionalString("device_orientation"); raw != "" {
		orientation, err = strconv.Atoi(raw)
		if err != nil || orientation < 0 || orientation > 3 {
			return
		}
	}
	e.c.log.Debug().Str("call_id", callID).Int("state", state).Int("orientation", orientation).Msg("inbound video state")
	e.mu.Lock()
	m := e.calls[callID]
	if m == nil || m != expected || (!m.group && !matchesDirectControlNode(m, outer, v)) {
		e.mu.Unlock()
		return
	}
	if m.group {
		// Group media is mixed/selected per authenticated receiver. One
		// participant's camera state cannot switch or mute the aggregate.
		e.mu.Unlock()
		switch state {
		case signaling.VideoStateEnabled:
			e.observeGroupDeviceMedia(outer, m, groupTransaction, "video", "on")
		case signaling.VideoStateDisabled, signaling.VideoStateStopped:
			e.observeGroupDeviceMedia(outer, m, groupTransaction, "video", "off")
		}
		return
	}
	sender := m.videoTx
	call := m.call
	to, creator := m.from, m.creator
	requestKeyframe := false
	disableSender := false
	enableSender := false
	announceEnabled := false
	switch state {
	case signaling.VideoStateUpgradeRequest, signaling.VideoStateUpgradeRequestV2:
		if !m.peerVideoUpgrade {
			m.peerVideoGeneration++
			if m.peerVideoGeneration == 0 {
				m.peerVideoGeneration++
			}
		}
		m.peerVideoUpgrade = true
	case signaling.VideoStateEnabled:
		m.remoteVideo = true
		if m.localVideo && m.videoGate {
			m.videoMode = true
			m.videoGate = false
			enableSender = true
			requestKeyframe = true
		}
	case signaling.VideoStateDisabled, signaling.VideoStateStopped:
		m.remoteVideo = false
	case signaling.VideoStateUpgradeAccept:
		if !m.videoGate {
			e.mu.Unlock()
			return
		}
		m.videoMode = true
		m.localVideo = true
		m.videoGate = false
		announceEnabled = true
	case signaling.VideoStateUpgradeCancel:
		// The peer withdrew its own request. Our outbound request is independent.
		if m.peerVideoUpgrade || m.peerVideoDecisionInFlight {
			m.peerVideoGeneration++
			if m.peerVideoGeneration == 0 {
				m.peerVideoGeneration++
			}
		}
		m.peerVideoUpgrade = false
	case signaling.VideoStateUpgradeReject:
		// The peer rejected our request, not its own simultaneous request.
		if m.videoGate {
			m.localVideo = false
			m.videoGate = false
			disableSender = true
		}
	}
	generation := m.peerVideoGeneration
	e.mu.Unlock()
	if announceEnabled {
		orientation := 0
		node := signaling.BuildVideoStateWithParams(signaling.VideoStateParams{
			CallID: callID, To: to, CallCreator: creator, WrapperID: e.nextCallNodeID(),
			State: signaling.VideoStateEnabled, DeviceOrientation: &orientation,
		})
		if err := e.transmitCallNode(context.Background(), node); err != nil {
			e.mu.Lock()
			if current := e.calls[callID]; current == m {
				current.localVideo = false
				current.videoGate = false
			}
			e.mu.Unlock()
			disableSender = true
			announceEnabled = false
			if e.c != nil {
				e.c.log.Warn().Err(err).Str("call_id", callID).Msg("video enabled announcement failed")
			}
		} else {
			enableSender = true
			requestKeyframe = true
		}
	}
	if sender != nil {
		if enableSender {
			sender.enable(false)
		} else if disableSender {
			sender.disable()
		}
	}
	if requestKeyframe && call != nil {
		call.requestVideoKeyframe()
	}
	if call != nil {
		call.recordVideoState(VideoState{
			Active:            state == signaling.VideoStateEnabled,
			Upgrade:           state == signaling.VideoStateUpgradeRequest || state == signaling.VideoStateUpgradeRequestV2,
			Orientation:       orientation,
			Raw:               state,
			UpgradeGeneration: generation,
		})
	}
}

// Termination before offer admission is scoped to the provider's signaling
// identity. Existing direct calls are checked again at registry removal, so a
// concurrent answer selecting a device cannot invalidate an earlier check.
func (e *engine) onTerminateEvent(ev *events.CallTerminate) {
	if ev == nil || ev.CallID == "" {
		return
	}
	e.mu.Lock()
	m := e.calls[ev.CallID]
	if m == nil || m.call == nil {
		e.earlyTerminations = rememberEarlyTermination(e.earlyTerminations, ev.BasicCallMeta, time.Now())
		e.mu.Unlock()
		return
	}
	call := m.call
	e.mu.Unlock()
	e.finishCallMatchingOffer(ev.CallID, ev.Reason, false, call, "", false, false, &ev.BasicCallMeta)
}

// recentlyEnded is called with the engine lock held.
func (e *engine) recentlyEnded(id string) bool { return time.Now().Before(e.ended[id]) }
func (e *engine) finishCall(callID, reason string) {
	e.finishCallWithReservation(callID, reason, false)
}

// Remote termination may precede the offer or arrive while it is being decoded.
func (e *engine) finishCallWithReservation(callID, reason string, reserveUnknown bool) {
	e.finishCallMatching(callID, reason, reserveUnknown, nil)
}

// A failed asynchronous preparation owns only the call it originally created.
func (e *engine) finishCallIfCurrent(call *Call, reason string) {
	if call != nil {
		e.finishCallMatching(call.ID(), reason, false, call)
	}
}

// finishPendingOffer atomically checks the original pending offer during registry removal.
func (e *engine) finishPendingOffer(call *Call, offerNodeID, reason string, requireCalling bool) bool {
	if call == nil || offerNodeID == "" {
		return false
	}
	return e.finishCallMatchingOffer(call.ID(), reason, false, call, offerNodeID, requireCalling, false, nil)
}

func (e *engine) finishCallMatching(callID, reason string, reserveUnknown bool, expected *Call) bool {
	return e.finishCallMatchingOffer(callID, reason, reserveUnknown, expected, "", false, false, nil)
}

func (e *engine) finishCallMatchingOffer(callID, reason string, reserveUnknown bool, expected *Call, expectedOffer string, requireCalling, requireUnanswered bool, terminalMeta *types.BasicCallMeta) bool {
	if callID == "" {
		return false
	}
	e.mu.Lock()
	m := e.calls[callID]
	if expected != nil && (m == nil || m.call != expected) {
		e.mu.Unlock()
		return false
	}
	if terminalMeta != nil && (m == nil || m.call == nil || (!m.group && !directLifecycleMatches(m.from, m.creator, *terminalMeta))) {
		e.mu.Unlock()
		return false
	}
	if requireUnanswered && (m == nil || m.call == nil || m.direction != CallDirectionIncoming ||
		m.call.State() != CallPhaseRinging || m.acceptPending || m.started) {
		e.mu.Unlock()
		return false
	}
	if expectedOffer != "" && (m == nil || m.call == nil || m.direction != CallDirectionOutgoing ||
		m.offerNodeID != expectedOffer || (requireCalling && (m.call.State() != CallPhaseCalling ||
		m.started || (m.group && m.groupUpdate != nil))) ||
		m.call.State() == CallPhaseEnded) {
		e.mu.Unlock()
		return false
	}
	if m != nil || reserveUnknown {
		e.ended = rememberEndedCall(e.ended, callID, time.Now())
	}
	if m != nil {
		m.terminalReservation = e.ended[callID]
		delete(e.calls, callID)
		if m.pickupCancel != nil {
			m.pickupCancel()
			m.pickupCancel = nil
		}
		clear(m.callKey)
	}
	var cancel, waitingRoomCancel context.CancelFunc
	var call *Call
	var receivers *participantReceiveRegistry
	if m != nil {
		cancel = m.cancel
		m.cancel = nil
		waitingRoomCancel = m.waitingRoomCancel
		m.waitingRoomCancel = nil
		m.clearGroupEpoch()
		receivers = m.groupReceivers
		m.groupReceivers = nil
		call = m.call
	}
	e.mu.Unlock()
	if cancel != nil {
		cancel()
	}
	if receivers != nil {
		receivers.clear()
	}
	if waitingRoomCancel != nil {
		waitingRoomCancel()
	}
	if call == nil {
		return false
	}
	call.mu.Lock()
	if call.phase == CallPhaseEnded {
		call.mu.Unlock()
		return false
	}
	// Publish the reason together with the terminal phase before potentially
	// blocking cleanup. Keep end callback delivery ordered after cleanup.
	if reason == "" {
		reason = "ended"
	}
	call.terminalReason = reason
	call.phase = CallPhaseEnded
	onState := call.onState
	player := call.player
	call.player = nil
	sink := call.sink
	call.sink = nil
	call.mu.Unlock()
	if player != nil {
		player.Stop()
	}
	if sink != nil {
		if err := sink.Close(); err != nil {
			e.c.log.Warn().Err(err).Str("call_id", callID).Msg("close call audio sink failed")
		}
	}
	if onState != nil {
		onState(CallPhaseEnded)
	}
	call.mu.Lock()
	call.terminalCleanupComplete = true
	fn := call.onEnd
	call.onEnd = nil
	call.mu.Unlock()
	if fn != nil {
		fn(reason)
	}
	return true
}

// installCallAckHook injects an "ack" entry into whatsmeow's unexported nodeHandlers map
// and wraps the "call" handler so the engine also sees the raw <call> node (with its
// stanza id, which the CallOffer event drops). whatsmeow has no <ack> handler — it
// silently drops <ack> nodes — but an outbound call's relay allocation arrives only
// inside <ack class="call" type="offer">, so without intercepting it the caller never
// learns the relay endpoint and media never starts. Called before Connect so the map
// write never races the receive loop.
//
// NOT VALIDATED: reaches into the client's unexported nodeHandlers via reflection +
// unsafe; covered only by a live call against the real relay.
func (e *engine) installCallAckHook() error {
	// Source of truth: https://github.com/tulir/whatsmeow/blob/e9a033b24933cc8d90fa5ff8991b1268ba80a140/client.go#L110-L120
	cli := e.c.wa
	if cli == nil {
		return errors.New("meowcaller: raw call adapter has no whatsmeow client")
	}
	if cli.IsConnected() {
		return errors.New("meowcaller: construct the client before connecting whatsmeow")
	}
	clientValue := reflect.ValueOf(cli)
	if clientValue.Kind() != reflect.Pointer || clientValue.IsNil() {
		return errors.New("meowcaller: raw call adapter received an invalid whatsmeow client")
	}
	clientValue = clientValue.Elem()
	if !clientValue.IsValid() || clientValue.Kind() != reflect.Struct {
		return errors.New("meowcaller: raw call adapter cannot inspect whatsmeow client")
	}
	field := clientValue.FieldByName("nodeHandlers")
	contextType := reflect.TypeOf((*context.Context)(nil)).Elem()
	nodeType := reflect.TypeOf((*waBinary.Node)(nil))
	if !field.IsValid() ||
		!field.CanAddr() ||
		field.Kind() != reflect.Map ||
		field.Type().Key().Kind() != reflect.String ||
		field.Type().Elem().Kind() != reflect.Func ||
		field.Type().Elem().NumIn() != 2 ||
		field.Type().Elem().In(0) != contextType ||
		field.Type().Elem().In(1) != nodeType ||
		field.Type().Elem().NumOut() != 0 {
		return errors.New("meowcaller: upstream whatsmeow node handler layout changed")
	}
	handlers := reflect.NewAt(field.Type(), unsafe.Pointer(field.UnsafeAddr())).Elem()
	if handlers.IsNil() {
		return errors.New("meowcaller: upstream whatsmeow node handlers are unavailable")
	}
	origAck := handlers.MapIndex(reflect.ValueOf("ack"))
	ackHandler := reflect.MakeFunc(field.Type().Elem(), func(args []reflect.Value) []reflect.Value {
		node := args[1].Interface().(*waBinary.Node)
		if node.AttrGetter().String("class") != "call" {
			if origAck.IsValid() && !origAck.IsNil() {
				origAck.Call(args)
			}
			return nil
		}
		e.queueRawCall(args[0].Interface().(context.Context), func() { e.onCallAck(node) })
		return nil
	})
	handlers.SetMapIndex(reflect.ValueOf("ack"), ackHandler)
	origCall := handlers.MapIndex(reflect.ValueOf("call"))
	callHandler := reflect.MakeFunc(field.Type().Elem(), func(args []reflect.Value) []reflect.Value {
		node := args[1].Interface().(*waBinary.Node)
		ctx := args[0].Interface().(context.Context)
		// Each decoded node is owned by this dispatch. The original handler
		// emits typed events synchronously, so run it in the same FIFO as raw
		// controls and call ACKs, not on a separate competing event queue.
		e.queueRawCall(ctx, func() {
			// A typed control ACK must not be followed by a generic ACK.
			if !e.onCallRaw(node) && origCall.IsValid() && !origCall.IsNil() {
				origCall.Call([]reflect.Value{reflect.ValueOf(ctx), reflect.ValueOf(node)})
			}
		})
		return nil
	})
	handlers.SetMapIndex(reflect.ValueOf("call"), callHandler)
	return nil
}

// queueRawCall is only used by the installed call/ACK hooks. Non-call
// ACKs and all other provider node handlers retain their original dispatch.
func (e *engine) queueRawCall(ctx context.Context, work func()) {
	if !e.callDispatch.submit(ctx, func() {
		if !e.c.wa.IsConnected() || !e.c.wa.IsLoggedIn() {
			return
		}
		// One malformed call must not kill the FIFO and strand later calls.
		defer func() {
			if recovered := recover(); recovered != nil {
				e.c.log.Error().Msg("panic in call node handler; discarded call node")
			}
		}()
		work()
	}) {
		e.c.log.Warn().Msg("call node queue full or cancelled; discarded call node")
	}
}

// ---- whatsmeow glue (ported from examples/cli/call.go) ----

// parseCallTarget turns a CLI-style target into a JID. A string with '@' is a real JID
// (a LID to call directly, or a phone JID to resolve); a bare string is a phone number.
func parseCallTarget(target string) (types.JID, error) {
	target = strings.TrimSpace(target)
	if target == "" {
		return types.EmptyJID, errors.New("empty call target")
	}
	if strings.ContainsRune(target, '@') {
		jid, err := types.ParseJID(target)
		if err != nil {
			return types.EmptyJID, fmt.Errorf("parse target JID %q: %w", target, err)
		}
		// c.us is the legacy phone-number spelling still commonly used by
		// integrations. Call signaling resolves phone peers through the modern
		// s.whatsapp.net namespace, so normalize the alias before resolving the
		// target and building the call offer.
		if jid.Server == types.LegacyUserServer {
			jid.Server = types.DefaultUserServer
		}
		return jid, nil
	}
	return types.NewJID(strings.TrimPrefix(target, "+"), types.DefaultUserServer), nil
}

// resolvePeerLID turns a target (phone number, phone JID, or @lid JID) into the peer's
// LID — the address the call's E2E keys and SSRCs derive from. A LID is used directly;
// a phone JID is mapped via the LID store, seeded by a usync query if not cached.
func resolvePeerLID(ctx context.Context, cli *whatsmeow.Client, target string) (types.JID, error) {
	jid, err := parseCallTarget(target)
	if err != nil {
		return types.EmptyJID, err
	}
	if jid.Server == types.HiddenUserServer {
		return jid, nil // already a LID — call it directly
	}
	if lid, err := cli.Store.LIDs.GetLIDForPN(ctx, jid); err == nil && !lid.IsEmpty() {
		return lid, nil
	}
	info, err := cli.GetUserInfo(ctx, []types.JID{jid})
	if err != nil {
		return types.EmptyJID, fmt.Errorf("usync %s: %w", jid.User, err)
	}
	for _, ui := range info {
		if !ui.LID.IsEmpty() {
			return ui.LID, nil
		}
	}
	if lid, err := cli.Store.LIDs.GetLIDForPN(ctx, jid); err == nil && !lid.IsEmpty() {
		return lid, nil
	}
	return types.EmptyJID, fmt.Errorf("usync returned no LID for %s (peer unreachable or not on WhatsApp)", jid.User)
}

// callKeyPlaintext wraps the raw callKey as the Signal message body
// Message{Call{CallKey}} (whatsmeow adds Signal padding during encryption).
func callKeyPlaintext(callKey []byte) ([]byte, error) {
	return proto.Marshal(&waE2E.Message{Call: &waE2E.Call{CallKey: callKey}})
}

// encryptCallKeyForDevice encrypts the callKey to one peer device's Signal session,
// fetching a pre-key bundle if no session exists yet. Returns the ciphertext, the enc
// type ("pkmsg" for a fresh session, "msg" for an existing one), and whether the offer
// must carry our <device-identity> (true for pkmsg).
func encryptCallKeyForDevice(ctx context.Context, cli *whatsmeow.Client, dev types.JID, callKey []byte) ([]byte, string, bool, error) {
	pt, err := callKeyPlaintext(callKey)
	if err != nil {
		return nil, "", false, err
	}
	di := cli.DangerousInternals()
	enc, needIdentity, err := di.EncryptMessageForDevice(ctx, pt, dev, nil, nil, nil)
	if err != nil {
		bundles := di.FetchPreKeysNoError(ctx, []types.JID{dev})
		enc, needIdentity, err = di.EncryptMessageForDevice(ctx, pt, dev, bundles[dev], nil, nil)
		if err != nil {
			return nil, "", false, err
		}
	}
	ct, ok := enc.Content.([]byte)
	if !ok {
		return nil, "", false, errors.New("enc node has no ciphertext")
	}
	return ct, enc.AttrGetter().String("type"), needIdentity, nil
}

// decryptInboundCallKey pulls the <enc> from the offer node and decrypts the
// Message{Call{CallKey}} under our Signal session.
func decryptInboundCallKey(ctx context.Context, cli *whatsmeow.Client, ev *events.CallOffer) ([]byte, error) {
	if ev.Data == nil {
		return nil, errors.New("offer has no data node")
	}
	var enc *waBinary.Node
	for i := range ev.Data.GetChildren() {
		if c := &ev.Data.GetChildren()[i]; c.Tag == "enc" {
			enc = c
			break
		}
	}
	if enc == nil {
		return nil, errors.New("offer has no enc node")
	}
	isPreKey := enc.AttrGetter().String("type") == "pkmsg"
	pt, _, err := cli.DangerousInternals().DecryptDM(ctx, enc, ev.From, isPreKey, ev.Timestamp)
	if err != nil {
		return nil, err
	}
	var msg waE2E.Message
	if err := proto.Unmarshal(pt, &msg); err != nil {
		return nil, err
	}
	key := msg.GetCall().GetCallKey()
	if len(key) == 0 {
		return nil, errors.New("offer message carried no callKey")
	}
	return key, nil
}

// newCallID returns a call id in WhatsApp's shape: 16 random bytes as uppercase hex.
func newCallID() string {
	var b [16]byte
	_, _ = rand.Read(b[:])
	return strings.ToUpper(hex.EncodeToString(b[:]))
}

// ---- relay signaling parse (ported from examples/cli/media.go) ----

type relayAddress struct {
	ipv4 string
	port uint16
}

type relayEndpoint struct {
	relayID     uint32
	relayName   string
	tokenID     uint32
	authTokenID uint32
	isFNA       bool
	addresses   []relayAddress
}

type relayData struct {
	relayKeyASCII []byte   // raw <key> content — the STUN MESSAGE-INTEGRITY key
	relayTokens   [][]byte // indexed <token id=…>
	endpoints     []relayEndpoint
	peerJID       types.JID
}

func nodeBytes(n *waBinary.Node) []byte {
	switch c := n.Content.(type) {
	case []byte:
		return c
	case string:
		return []byte(c)
	}
	return nil
}

func childByTag(n *waBinary.Node, tag string) *waBinary.Node {
	kids := n.GetChildren()
	for i := range kids {
		if kids[i].Tag == tag {
			return &kids[i]
		}
	}
	return nil
}

// findRelay recursively locates the <relay> node anywhere under n (it can sit under
// <offer> or a sibling <relaylatency>/<transport>).
func findRelay(n *waBinary.Node) *waBinary.Node {
	if n == nil {
		return nil
	}
	if n.Tag == "relay" {
		return n
	}
	kids := n.GetChildren()
	for i := range kids {
		if r := findRelay(&kids[i]); r != nil {
			return r
		}
	}
	return nil
}

// findChild recursively locates the first node with the given tag under n.
func findChild(n *waBinary.Node, tag string) *waBinary.Node {
	if n == nil {
		return nil
	}
	if n.Tag == tag {
		return n
	}
	kids := n.GetChildren()
	for i := range kids {
		if r := findChild(&kids[i], tag); r != nil {
			return r
		}
	}
	return nil
}

// decodeLatency reverses the relay-latency wire encoding (0x2000000 + rttMs).
func decodeLatency(enc string) uint32 {
	v, err := strconv.ParseUint(enc, 10, 32)
	if err != nil || v < 0x0200_0000 {
		return 0
	}
	return uint32(v) - 0x0200_0000
}

func attrUint(n *waBinary.Node, key string) uint32 {
	v, _ := strconv.ParseUint(n.AttrGetter().String(key), 10, 32)
	return uint32(v)
}

const maxRelayTokens = 64

func parseIndexedTokens(node *waBinary.Node, tag string) [][]byte {
	var tokens [][]byte
	kids := node.GetChildren()
	for i := range kids {
		c := &kids[i]
		if c.Tag != tag {
			continue
		}
		b := nodeBytes(c)
		if len(b) == 0 {
			continue
		}
		id := len(tokens)
		if s := c.AttrGetter().String("id"); s != "" {
			n, err := strconv.Atoi(s)
			if err != nil || n < 0 || n >= maxRelayTokens {
				continue
			}
			id = n
		}
		if id < 0 || id >= maxRelayTokens {
			continue
		}
		for len(tokens) <= id {
			tokens = append(tokens, nil)
		}
		tokens[id] = b
	}
	return tokens
}

// parseRelayData ports parse_relay_data: <key>, indexed <token>, and te2 endpoints.
func parseRelayData(node *waBinary.Node) *relayData {
	rd := &relayData{}
	if key := childByTag(node, "key"); key != nil {
		rd.relayKeyASCII = nodeBytes(key)
	}
	rd.relayTokens = parseIndexedTokens(node, "token")

	kids := node.GetChildren()
	peerPID := node.AttrGetter().String("peer_pid")
	for i := range kids {
		child := &kids[i]
		if child.Tag == "participant" && peerPID != "" && child.AttrGetter().String("pid") == peerPID {
			rd.peerJID = child.AttrGetter().JID("jid")
			continue
		}
		if child.Tag != "te2" {
			continue
		}
		ab := nodeBytes(child)
		if len(ab) != 6 { // IPv4:port only (IPv6 endpoints skipped)
			continue
		}
		ep := relayEndpoint{
			relayID:     attrUint(child, "relay_id"),
			relayName:   child.AttrGetter().String("relay_name"),
			tokenID:     attrUint(child, "token_id"),
			authTokenID: attrUint(child, "auth_token_id"),
			isFNA:       child.AttrGetter().String("is_fna") == "1",
			addresses: []relayAddress{{
				ipv4: fmt.Sprintf("%d.%d.%d.%d", ab[0], ab[1], ab[2], ab[3]),
				port: binary.BigEndian.Uint16(ab[4:6]),
			}},
		}
		rd.endpoints = append(rd.endpoints, ep)
	}
	return rd
}

// getMediaRelayEndpoint prefers an outbound (non-FNA, auth_token_id≠0) endpoint, else
// any non-FNA, else the first. For an inbound call the caller's uplink RTP lands on their
// FNA-marked relay, so we must allocate on that same relay or the relay never bridges the
// peer's media (the callee connects but hears nothing).
func getMediaRelayEndpoint(rd *relayData, inbound bool) *relayEndpoint {
	if inbound {
		for i := range rd.endpoints {
			if e := &rd.endpoints[i]; e.isFNA {
				return e
			}
		}
	}
	for i := range rd.endpoints {
		if e := &rd.endpoints[i]; !e.isFNA && e.authTokenID != 0 {
			return e
		}
	}
	for i := range rd.endpoints {
		if e := &rd.endpoints[i]; !e.isFNA {
			return e
		}
	}
	if len(rd.endpoints) > 0 {
		return &rd.endpoints[0]
	}
	return nil
}
