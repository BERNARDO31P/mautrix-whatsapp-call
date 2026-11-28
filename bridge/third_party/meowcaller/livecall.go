package meowcaller

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/purpshell/meowcaller/signaling"
	"go.mau.fi/whatsmeow/types"
)

// Call is one live direct or group call. A direct call may become an ad-hoc group
// call through AddParticipant. All methods are safe for concurrent use.
type Call struct {
	eng  *engine
	id   string
	peer types.JID

	mu                                              sync.Mutex
	phase                                           CallPhase
	player                                          *Player
	sink                                            AudioSink
	onReady                                         func()
	mediaReady                                      bool
	relayReady                                      bool
	onEnd                                           func(reason string)
	terminalReason                                  string
	terminalCleanupComplete                         bool
	onState                                         func(CallPhase)
	onPeerAccept                                    func()
	peerAccepted                                    bool
	acceptNotified                                  bool
	onMuteState                                     func(muted bool)
	lastMute                                        *bool
	lastVideoState                                  *VideoState
	muteCallbackGeneration, videoCallbackGeneration uint64
	videoSink                                       VideoSink
	onVideoState                                    func(VideoState)
	onVideoKeyframeRequest                          func()
	onParticipantVideoFrame                         func(ParticipantVideoFrame)
	onParticipantAudioFrame                         func(ParticipantAudioFrame)
	participantEpochMu                              sync.Mutex
	participantEpoch                                ParticipantMediaEpoch
	onParticipantMediaEpoch                         func(ParticipantMediaEpoch)
	onReaction                                      func(CallReaction)
	groupState                                      *GroupCallState
	onGroupState                                    func(GroupCallState)
	groupCallbackGeneration                         uint64
	waitingRoomState                                *WaitingRoomState
	onWaitingRoomState                              func(WaitingRoomState)
	waitingRoomGeneration                           uint64
	waitingControlGeneration                        uint64
	waitingCallbackGeneration                       uint64
	notifyMu                                        sync.Mutex
	notifyRunning                                   bool
	notifyQueue                                     []func()
	onHandRaise                                     func(HandRaiseState)
	handRaises                                      map[types.JID]bool
	onScreenShare                                   func(ScreenShareState)
	screenShares                                    map[types.JID]ScreenShareState
	screenRoleRevisions                             map[types.JID]uint64
}

// GroupCallState is a sanitized group-call roster. Transaction zero may contain
// selected outgoing targets before the first authoritative server transaction.
type GroupCallState struct {
	GroupJID       types.JID
	TransactionID  uint32
	HandRevision   uint64 // local snapshot sequence, advances on roster and hand changes
	MediaRevision  uint64 // advances on roster and observed device media changes
	RekeyRequested bool
	Participants   []GroupCallParticipant
}

// GroupCallParticipant is one participant in an authoritative group-call roster.
type GroupCallParticipant struct {
	JID        types.JID
	PN         types.JID
	State      string
	HandRaised bool
	Devices    []GroupCallDevice
}

// GroupCallDevice is one participant device selected or considered by WhatsApp.
type GroupCallDevice struct {
	JID      types.JID
	Platform string
	PID      uint32
	HasPID   bool
	Media    GroupDeviceMediaState
}

// ParticipantAudioFrame is decoded, authenticated audio attributed to a WhatsApp
// participant device before mixing. PCM is owned by the callback and may be retained.
// Timestamp uses RTPClockRate, while PCM uses SampleRate (mono float32).
type ParticipantAudioFrame struct {
	Epoch         ParticipantMediaEpoch
	ParticipantID string
	Sender        types.JID
	Device        types.JID
	PID           uint32
	HasPID        bool
	SSRC          uint32
	Timestamp     uint32
	RTPClockRate  uint32
	SampleRate    uint32
	PCM           []float32
}

// CallReaction is a transient emoji reaction received over the call's RTC app-data stream.
// An empty Emoji with Removed set means the sender cleared their reaction.
type CallReaction struct {
	Emoji         string
	ParticipantID string
	Sender        types.JID
	Device        types.JID
	PID           uint32
	HasPID        bool
	Removed       bool
}

// WaitingRoomState is sanitized call-link admission state. It never exposes the link token.
type WaitingRoomState struct {
	Enabled       bool
	IsAdmin       bool
	InWaitingRoom bool
	TransactionID uint32
	Users         []WaitingRoomUser
}

// WaitingRoomUser is one user in the authoritative pending roster.
type WaitingRoomUser struct {
	JID   types.JID
	PN    types.JID
	State string
}

// HandRaiseState is one participant's persistent raised/lowered state.
type HandRaiseState struct {
	Participant types.JID
	Raised      bool
}

// ScreenShareState is one participant's source-role signalling state. It does not advertise a second video stream.
type ScreenShareState struct {
	Participant types.JID
	// Device/PID are bound to the authoritative roster when the control arrives.
	// An empty device means the signal is participant-level only.
	Device           types.JID
	PID              uint32
	HasPID           bool
	Active           bool
	Version          uint32
	ScreenShareID    uint32
	HasScreenShareID bool
	Synthetic        bool
}

// ID returns the call-id (32 uppercase hex chars).
func (c *Call) ID() string { return c.id }

// Peer returns the remote party's LID.
func (c *Call) Peer() types.JID {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.peer
}

// GroupState returns the latest group-call roster, if this is a group call.
func (c *Call) IsGroup() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.groupState != nil
}

// GroupState returns the latest group-call roster, if this is a group call.
func (c *Call) GroupState() (GroupCallState, bool) {
	// Source of truth: https://github.com/purpshell/meowcaller/blob/a95ab63017f996313ca7e4cfbbfb96fead1717a7/datasheets/api-group-call-state.md#L28-L72
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.groupState == nil {
		return GroupCallState{}, false
	}
	return cloneGroupCallState(*c.groupState), true
}

// GroupVideoRoles snapshots the roster and active screen shares under the
// same lock. Explicit device controls are bound to their exact roster device.
// Participant-only controls are bound only if the roster has exactly one device.
type GroupVideoRole struct {
	ScreenShare bool
	Revision    uint64
}

func (c *Call) screenRoleRevisionLocked(key types.JID) uint64 {
	if rev := c.screenRoleRevisions[key]; rev != 0 {
		return rev
	}
	return 1
}

func (c *Call) advanceScreenRoleLocked(key types.JID) {
	if c.screenRoleRevisions == nil {
		c.screenRoleRevisions = make(map[types.JID]uint64)
	}
	c.screenRoleRevisions[key] = c.screenRoleRevisionLocked(key) + 1
}

// Resolve an explicit device or a unique participant-only device. Do not use
// screen-share IDs as PIDs: they are unrelated signalling identifiers.
func screenShareDevice(group *GroupCallState, key, requested types.JID) (GroupCallDevice, bool) {
	if !connectedHandParticipant(group, key) {
		return GroupCallDevice{}, false
	}
	for _, participant := range group.Participants {
		if participant.JID.ToNonAD() != key {
			continue
		}
		if requested.IsEmpty() && len(participant.Devices) != 1 {
			return GroupCallDevice{}, false
		}
		var selected GroupCallDevice
		count := 0
		for _, d := range participant.Devices {
			if !requested.IsEmpty() && d.JID != requested {
				continue
			}
			if d.JID.IsEmpty() || !d.HasPID {
				continue
			}
			selected = d
			count++
		}
		return selected, count == 1
	}
	return GroupCallDevice{}, false
}

func screenShareBindingCurrent(group *GroupCallState, key types.JID, share ScreenShareState) bool {
	if !share.HasPID || share.Device.IsEmpty() {
		return false
	}
	d, ok := screenShareDevice(group, key, share.Device)
	return ok && d.PID == share.PID
}

func (c *Call) GroupVideoRoles() (GroupCallState, map[string]GroupVideoRole, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.groupState == nil || c.phase == CallPhaseEnded {
		return GroupCallState{}, nil, false
	}
	roles := make(map[string]GroupVideoRole)
	for _, participant := range c.groupState.Participants {
		if participant.State != "connected" {
			continue
		}
		share, exists := c.screenShares[participant.JID.ToNonAD()]
		for _, device := range participant.Devices {
			roles[device.JID.String()] = GroupVideoRole{ScreenShare: exists && share.Active && share.HasPID && device.HasPID && share.Device == device.JID && share.PID == device.PID, Revision: c.screenRoleRevisionLocked(participant.JID.ToNonAD())}
		}
	}
	return cloneGroupCallState(*c.groupState), roles, true
}

// OnGroupState registers a callback for group-call roster state. The latest
// cached state, including a selected-target seed, is replayed immediately.
func (c *Call) OnGroupState(fn func(GroupCallState)) {
	// Source of truth: https://github.com/purpshell/meowcaller/blob/a95ab63017f996313ca7e4cfbbfb96fead1717a7/datasheets/api-group-call-state.md#L28-L72
	c.mu.Lock()
	c.onGroupState = fn
	c.groupCallbackGeneration++
	callbackGeneration := c.groupCallbackGeneration
	var replay *GroupCallState
	if fn != nil && c.groupState != nil {
		state := cloneGroupCallState(*c.groupState)
		replay = &state
	}
	c.mu.Unlock()
	if replay == nil {
		return
	}
	c.enqueueNotification(func() {
		c.mu.Lock()
		if c.groupCallbackGeneration != callbackGeneration ||
			c.onGroupState == nil {
			c.mu.Unlock()
			return
		}
		currentFn := c.onGroupState
		c.mu.Unlock()
		currentFn(*replay)
	})
}

func (c *Call) setGroupState(state GroupCallState) {
	// Source of truth: https://github.com/purpshell/meowcaller/blob/a95ab63017f996313ca7e4cfbbfb96fead1717a7/datasheets/api-group-call-state.md#L58-L72
	stored := cloneGroupCallState(state)
	c.mu.Lock()
	if c.groupState != nil &&
		c.groupState.TransactionID != 0 &&
		state.TransactionID <= c.groupState.TransactionID {
		c.mu.Unlock()
		return
	}
	// A hand belongs to one continuous connected roster presence. Discard it
	// on departure, disconnection, ambiguous identity, or the first authoritative roster.
	if stored.TransactionID != 0 {
		// Source roles cannot outlive continuous, unambiguous roster presence.
		for key, share := range c.screenShares {
			if !connectedHandParticipant(c.groupState, key) || !connectedHandParticipant(&stored, key) ||
				(share.HasPID && (!screenShareBindingCurrent(c.groupState, key, share) || !screenShareBindingCurrent(&stored, key, share))) {
				delete(c.screenShares, key)
				c.advanceScreenRoleLocked(key)
			}
		}
		for key := range c.handRaises {
			if c.groupState == nil || c.groupState.TransactionID == 0 ||
				!connectedHandParticipant(c.groupState, key) || !connectedHandParticipant(&stored, key) {
				delete(c.handRaises, key)
			}
		}
	}
	for i := range stored.Participants {
		key := stored.Participants[i].JID.ToNonAD()
		stored.Participants[i].HandRaised = connectedHandParticipant(&stored, key) && c.handRaises[key]
	}
	preserveGroupDeviceMedia(c.groupState, &stored)
	if c.groupState != nil {
		stored.MediaRevision = c.groupState.MediaRevision + 1
		stored.HandRevision = c.groupState.HandRevision + 1
	} else {
		stored.HandRevision = 1
		stored.MediaRevision = 1
	}
	c.groupState = &stored
	callbackGeneration := c.groupCallbackGeneration
	hasCallback := c.onGroupState != nil
	// The live roster is subsequently updated in place by hand events.
	// The queued callback owns an immutable, independent snapshot.
	var notification GroupCallState
	if hasCallback {
		notification = cloneGroupCallState(stored)
	}
	c.mu.Unlock()
	if !hasCallback {
		return
	}
	c.enqueueNotification(func() {
		c.mu.Lock()
		if c.groupCallbackGeneration != callbackGeneration ||
			c.onGroupState == nil {
			c.mu.Unlock()
			return
		}
		fn := c.onGroupState
		c.mu.Unlock()
		fn(notification)
	})
}

func groupCallStateFromUpdate(update groupCallUpdate) GroupCallState {
	// Source of truth: https://github.com/purpshell/meowcaller/blob/a95ab63017f996313ca7e4cfbbfb96fead1717a7/datasheets/api-group-call-state.md#L58-L68
	state := GroupCallState{
		GroupJID:       update.GroupJID,
		TransactionID:  update.TransactionID,
		RekeyRequested: update.RekeyRequested,
		Participants:   make([]GroupCallParticipant, len(update.Participants)),
	}
	for participantIndex, participant := range update.Participants {
		devices := make([]GroupCallDevice, len(participant.Devices))
		for deviceIndex, device := range participant.Devices {
			devices[deviceIndex] = GroupCallDevice{
				JID:      device.JID,
				Platform: device.Platform,
				PID:      device.PID,
				HasPID:   device.HasPID,
			}
		}
		state.Participants[participantIndex] = GroupCallParticipant{
			JID: participant.JID, PN: participant.PN, State: participant.State,
			Devices: devices,
		}
	}
	return state
}

func cloneGroupCallState(state GroupCallState) GroupCallState {
	// Source of truth: https://github.com/purpshell/meowcaller/blob/a95ab63017f996313ca7e4cfbbfb96fead1717a7/datasheets/api-group-call-state.md#L58-L72
	clone := state
	clone.Participants = make([]GroupCallParticipant, len(state.Participants))
	for participantIndex, participant := range state.Participants {
		clone.Participants[participantIndex] = participant
		clone.Participants[participantIndex].Devices = append(
			[]GroupCallDevice(nil),
			participant.Devices...,
		)
	}
	return clone
}

func (c *Call) setPeer(peer types.JID) {
	if peer.IsEmpty() {
		return
	}
	c.mu.Lock()
	c.peer = peer
	c.mu.Unlock()
}

// State returns the call's current phase.
func (c *Call) State() CallPhase {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.phase
}

// TerminalReason returns the reason recorded with the terminal phase, even
// while media cleanup or the end callback is still in progress.
func (c *Call) TerminalReason() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.terminalReason
}

// IsVideo reports whether either direction of the call currently has video.
func (c *Call) IsVideo() bool {
	return c.eng.callIsVideo(c.id)
}

// IsSendingVideo reports whether this client currently owns an active or pending
// outbound video flow.
func (c *Call) IsSendingVideo() bool {
	return c.eng.callIsSendingVideo(c.id)
}

// IsReceivingVideo reports whether the peer currently owns an active inbound video flow.
func (c *Call) IsReceivingVideo() bool {
	return c.eng.callIsReceivingVideo(c.id)
}

// Answer accepts an inbound call (preaccept + accept) and brings media up. No-op error
// if the call is not in a ringing state. ErrPickupNotSubmitted describes an
// unsent attempt; ErrPickupOutcomeUnknown must not be retried. End the exact
// call with Hangup if the submitted outcome cannot be established.
func (c *Call) AnswerContext(ctx context.Context) error { return c.eng.answerContext(ctx, c) }

func (c *Call) Answer() error { return c.eng.answer(c) }

// DismissUnanswered releases an incoming ringing call locally without sending a
// reject or terminate. It cannot dismiss a call with an active or submitted pickup.
// The caller may use this when its local frontend is unavailable.
func (c *Call) DismissUnanswered() bool {
	return c.eng.finishCallMatchingOffer(c.id, "local-frontend-unavailable", false, c, "", false, true, nil)
}

// EndLocally retires this exact call without sending provider signaling.
// Use after loss of stream ownership, not for a user's hangup or decline.
func (c *Call) EndLocally(reason string) bool {
	return c.eng.finishCallMatching(c.id, reason, false, c)
}

// Reject declines an inbound call.
func (c *Call) Reject() error { return c.eng.reject(c) }

// Hangup ends the call (either direction) and tears down media.
func (c *Call) Hangup() error { return c.eng.hangup(c) }

// StartVideo requests an audio-to-video upgrade. Outbound video remains gated until
// the peer acknowledges the transition with state 4 or state 1.
func (c *Call) StartVideo() error {
	return c.eng.transitionVideo(c.id, signaling.VideoStateUpgradeRequestV2)
}

// ErrNoPendingVideoUpgrade indicates that an upgrade notification was already
// accepted or withdrawn before the caller attempted to accept it.
var ErrNoPendingVideoUpgrade = errors.New("meowcaller: no pending peer video upgrade")

// ErrVideoDecisionMode indicates that the call uses the other decision policy.
var ErrVideoDecisionMode = errors.New("meowcaller: video decision mode mismatch")

// ErrVideoDecisionBusy means an older provider submission has not returned.
var ErrVideoDecisionBusy = errors.New("meowcaller: video decision in flight")

// VideoNegotiation is a complete call-scoped snapshot, not a delivery receipt.
type VideoNegotiation struct {
	Manual             bool   `json:"manual"`
	LocalCamera        bool   `json:"local_camera"`
	PeerCamera         bool   `json:"peer_camera"`
	OutgoingPending    bool   `json:"outgoing_pending"`
	IncomingGeneration uint64 `json:"incoming_generation"`
}

// VideoNegotiationState optionally enables manual decisions for the rest of this
// direct call. A decision already reserved by the automatic path cannot be undone.
func (c *Call) VideoNegotiationState(manual bool) (VideoNegotiation, error) {
	c.eng.mu.Lock()
	defer c.eng.mu.Unlock()
	m := c.eng.calls[c.id]
	if m == nil || m.call != c || m.group || c.State() == CallPhaseEnded {
		return VideoNegotiation{}, errors.New("meowcaller: direct call unavailable")
	}
	if manual {
		m.manualVideoDecision = true
	}
	state := VideoNegotiation{Manual: m.manualVideoDecision, LocalCamera: m.localVideo && !m.videoGate, PeerCamera: m.remoteVideo, OutgoingPending: m.videoGate}
	if m.peerVideoUpgrade {
		state.IncomingGeneration = m.peerVideoGeneration
	}
	return state, nil
}

// AutoAcceptVideo accepts only the generation associated with the callback and
// only while automatic decisions remain selected.
func (c *Call) AutoAcceptVideo(ctx context.Context, generation uint64) error {
	return c.eng.transitionVideoContext(ctx, c.id, c, signaling.VideoStateUpgradeAccept, generation, 1)
}

// DecideVideoUpgrade consumes exactly one current manual request, even if the
// transport outcome is ambiguous. The caller must not retry an uncertain send.
func (c *Call) DecideVideoUpgrade(ctx context.Context, generation uint64, accept bool) error {
	transition := signaling.VideoStateUpgradeReject
	if accept {
		transition = signaling.VideoStateUpgradeAccept
	}
	return c.eng.transitionVideoContext(ctx, c.id, c, transition, generation, 2)
}

// IsVideoTransmitReady excludes an unacknowledged outgoing upgrade. In particular
// the gateway must not announce an enabled orientation before the peer accepts.
func (c *Call) IsVideoTransmitReady() bool {
	c.eng.mu.Lock()
	defer c.eng.mu.Unlock()
	m := c.eng.calls[c.id]
	return m != nil && m.call == c && m.localVideo && !m.videoGate && c.State() != CallPhaseEnded
}

// AcceptVideo accepts a pending peer video upgrade without changing this client's
// independent outbound video state.
func (c *Call) AcceptVideo() error {
	return c.eng.transitionVideo(c.id, signaling.VideoStateUpgradeAccept)
}

// RejectVideo declines a pending peer upgrade without altering local camera state.
// A failed transmission is ambiguous; the same decision is not automatically retried.
func (c *Call) RejectVideo() error {
	return c.eng.transitionVideo(c.id, signaling.VideoStateUpgradeReject)
}

// StopVideo stops this client's outbound video while preserving peer video and audio.
func (c *Call) StopVideo() error {
	return c.eng.transitionVideo(c.id, signaling.VideoStateStopped)
}

// SetVideoEnabled mutes or unmutes this client's outbound camera flow with state 0/1.
// It does not change whether the peer's video is received.
func (c *Call) SetVideoEnabled(enabled bool) error {
	return c.eng.setVideoEnabled(c.id, enabled)
}

// SetVideoOrientation announces the local camera rotation as quarter turns clockwise.
func (c *Call) SetVideoOrientation(orientation int) error {
	return c.eng.setVideoOrientation(c.id, orientation)
}

// SetMuted announces this client's microphone mute state to WhatsApp while
// keeping the audio transport alive.
func (c *Call) SetMuted(muted bool) error {
	return c.eng.setMuted(c.id, muted)
}

// AddParticipant invites one person to this established call.
func (c *Call) AddParticipant(ctx context.Context, target string) error {
	// Source of truth: https://github.com/purpshell/meowcaller/blob/160912971e6bc2a4aa79ac3aafcf08360075e3fc/datasheets/api-group-participant-invite.md#L23-L100
	return c.eng.inviteParticipant(ctx, c.id, target)
}

// AddParticipants invites each person independently and returns index-aligned results.
func (c *Call) AddParticipants(ctx context.Context, targets ...string) []error {
	// Source of truth: https://github.com/purpshell/meowcaller/blob/160912971e6bc2a4aa79ac3aafcf08360075e3fc/datasheets/api-group-participant-invite.md#L23-L100
	results := make([]error, len(targets))
	for i, target := range targets {
		results[i] = c.AddParticipant(ctx, target)
	}
	return results
}

// RingParticipant rings one non-connected participant already present in this call's roster.
func (c *Call) RingParticipant(ctx context.Context, target string) error {
	// Source of truth: https://github.com/purpshell/meowcaller/blob/160912971e6bc2a4aa79ac3aafcf08360075e3fc/datasheets/api-group-participant-invite.md#L23-L100
	return c.eng.ringParticipant(ctx, c.id, target)
}

// SendReaction sends an emoji over this call's RTC app-data stream. Pass an empty
// string to clear the reaction previously sent by this client.
func (c *Call) SendReaction(emoji string) error {
	return c.eng.sendReaction(c.id, emoji)
}

// SendReactionContext submits only for this exact active call.
func (c *Call) SendReactionContext(ctx context.Context, emoji string) error {
	return c.eng.sendReactionContext(ctx, c, emoji)
}

// SetHandRaised announces this client's persistent hand state.
func (c *Call) SetHandRaised(raised bool) error {
	return c.eng.setHandRaised(c.id, raised)
}

// SetHandRaisedContext submits a hand action only for this exact live group
// call and its current connected local participant. Success is submission,
// not a correlated remote acknowledgement.
func (c *Call) SetHandRaisedContext(ctx context.Context, raised bool) error {
	return c.eng.setGroupHandRaised(ctx, c, raised)
}

// StartScreenShare announces a version-2 source switch on the primary video stream.
// The caller must switch the H.264 input; this does not create a second stream.
func (c *Call) StartScreenShare(screenShareID *uint32) error {
	return c.SetScreenShareContext(context.Background(), true, screenShareID)
}

// SetScreenShareContext submits a source-role change for the exact live group call.
// A successful return means submission, not an acknowledged remote media switch.
func (c *Call) SetScreenShareContext(ctx context.Context, active bool, screenShareID *uint32) error {
	return c.eng.setScreenShare(ctx, c, active, screenShareID)
}

// StopScreenShare stops screen sharing without stopping camera video or ending the call.
func (c *Call) StopScreenShare() error {
	return c.SetScreenShareContext(context.Background(), false, nil)
}

// OnHandRaise registers a participant hand-state listener.
func (c *Call) OnHandRaise(fn func(HandRaiseState)) {
	c.mu.Lock()
	c.onHandRaise = fn
	c.mu.Unlock()
}

// OnScreenShare registers a screen-share signalling listener. Consumers must reconcile the authoritative roster as well.
func (c *Call) OnScreenShare(fn func(ScreenShareState)) {
	c.mu.Lock()
	c.onScreenShare = fn
	c.mu.Unlock()
}

// ScreenShares returns the currently active participant screen shares.
func (c *Call) ScreenShares() []ScreenShareState {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.phase == CallPhaseEnded {
		return nil
	}
	shares := make([]ScreenShareState, 0, len(c.screenShares))
	for _, share := range c.screenShares {
		shares = append(shares, share)
	}
	return shares
}

// WaitingRoomState returns the latest sanitized call-link admission state.
func (c *Call) WaitingRoomState() (WaitingRoomState, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.waitingRoomState == nil {
		return WaitingRoomState{}, false
	}
	return cloneWaitingRoomState(*c.waitingRoomState), true
}

// OnWaitingRoomState registers a waiting-room listener and replays current state.
func (c *Call) OnWaitingRoomState(fn func(WaitingRoomState)) {
	// Source of truth: https://github.com/purpshell/meowcaller/blob/36d54857c74e45ccb08f6444a32d2afa13f20be9/datasheets/call-links-waiting-room.md#L62-L94
	c.mu.Lock()
	c.onWaitingRoomState = fn
	c.waitingCallbackGeneration++
	callbackGeneration := c.waitingCallbackGeneration
	var replay *WaitingRoomState
	if fn != nil && c.waitingRoomState != nil {
		state := cloneWaitingRoomState(*c.waitingRoomState)
		replay = &state
	}
	c.mu.Unlock()
	if replay == nil {
		return
	}
	c.enqueueNotification(func() {
		c.mu.Lock()
		if c.waitingCallbackGeneration != callbackGeneration ||
			c.onWaitingRoomState == nil {
			c.mu.Unlock()
			return
		}
		currentFn := c.onWaitingRoomState
		c.mu.Unlock()
		currentFn(*replay)
	})
}

// SetApprovalRequired changes call-link approval requirements for an admin call.
func (c *Call) SetApprovalRequired(ctx context.Context, enabled bool) error {
	return c.eng.setApprovalRequired(ctx, c.id, enabled)
}

// AdmitParticipant admits one waiting-room user.
func (c *Call) AdmitParticipant(ctx context.Context, user string) error {
	return c.eng.controlWaitingParticipant(ctx, c.id, user, true)
}

// DenyParticipant denies one waiting-room user.
func (c *Call) DenyParticipant(ctx context.Context, user string) error {
	return c.eng.controlWaitingParticipant(ctx, c.id, user, false)
}

func (c *Call) setWaitingRoomState(state WaitingRoomState) {
	c.storeWaitingRoomState(state, false)
}

func (c *Call) storeWaitingRoomState(state WaitingRoomState, preserveAdmission bool) {
	// Source of truth: https://github.com/purpshell/meowcaller/blob/36d54857c74e45ccb08f6444a32d2afa13f20be9/datasheets/call-links-waiting-room.md#L62-L94
	stored := cloneWaitingRoomState(state)
	c.mu.Lock()
	if c.waitingRoomState != nil &&
		c.waitingRoomState.TransactionID != 0 &&
		state.TransactionID <= c.waitingRoomState.TransactionID {
		c.mu.Unlock()
		return
	}
	if preserveAdmission && c.waitingRoomState != nil {
		stored.InWaitingRoom = c.waitingRoomState.InWaitingRoom
	}
	c.waitingRoomState = &stored
	c.waitingRoomGeneration++
	callbackGeneration := c.waitingCallbackGeneration
	c.mu.Unlock()
	c.enqueueNotification(func() {
		c.mu.Lock()
		if c.waitingCallbackGeneration != callbackGeneration ||
			c.onWaitingRoomState == nil {
			c.mu.Unlock()
			return
		}
		fn := c.onWaitingRoomState
		c.mu.Unlock()
		fn(cloneWaitingRoomState(stored))
	})
}

func (c *Call) setWaitingRoomAdmission() {
	// Source of truth: https://github.com/purpshell/meowcaller/blob/36d54857c74e45ccb08f6444a32d2afa13f20be9/datasheets/call-links-waiting-room.md#L62-L94
	c.mu.Lock()
	if c.waitingRoomState == nil || !c.waitingRoomState.InWaitingRoom {
		c.mu.Unlock()
		return
	}
	c.waitingRoomState.InWaitingRoom = false
	c.waitingRoomGeneration++
	callbackGeneration := c.waitingCallbackGeneration
	state := cloneWaitingRoomState(*c.waitingRoomState)
	c.mu.Unlock()
	c.enqueueNotification(func() {
		c.mu.Lock()
		if c.waitingCallbackGeneration != callbackGeneration ||
			c.onWaitingRoomState == nil {
			c.mu.Unlock()
			return
		}
		fn := c.onWaitingRoomState
		c.mu.Unlock()
		fn(state)
	})
}

// queueNotification appends without executing user callbacks. Callers may hold
// c.mu while queuing to preserve state-mutation order; the elected drainer must
// release that lock before calling drainNotifications.
func (c *Call) queueNotification(notification func()) bool {
	if notification == nil {
		return false
	}
	c.notifyMu.Lock()
	defer c.notifyMu.Unlock()
	c.notifyQueue = append(c.notifyQueue, notification)
	if c.notifyRunning {
		return false
	}
	c.notifyRunning = true
	return true
}

func (c *Call) enqueueNotification(notification func()) {
	if c.queueNotification(notification) {
		c.drainNotifications()
	}
}

func (c *Call) drainNotifications() {
	c.notifyMu.Lock()
	for len(c.notifyQueue) > 0 {
		next := c.notifyQueue[0]
		c.notifyQueue[0] = nil
		c.notifyQueue = c.notifyQueue[1:]
		c.notifyMu.Unlock()
		next()
		c.notifyMu.Lock()
	}
	c.notifyRunning = false
	c.notifyQueue = nil
	c.notifyMu.Unlock()
}

// A participant identity must be unique in an authoritative roster. Do not
// infer identity from an untrusted phone/LID alias or from the call JID.
func connectedHandParticipant(group *GroupCallState, key types.JID) bool {
	if group == nil || group.TransactionID == 0 || key.IsEmpty() ||
		(key.Server != types.HiddenUserServer && key.Server != types.DefaultUserServer) {
		return false
	}
	match := -1
	for i, participant := range group.Participants {
		if participant.JID.ToNonAD() == key {
			if match != -1 || participant.State != "connected" {
				return false
			}
			match = i
		}
	}
	if match == -1 {
		return false
	}
	alias := group.Participants[match].PN.ToNonAD()
	for i, participant := range group.Participants {
		if i == match {
			continue
		}
		other, otherAlias := participant.JID.ToNonAD(), participant.PN.ToNonAD()
		if otherAlias == key || (!alias.IsEmpty() && (other == alias || otherAlias == alias)) {
			return false
		}
	}
	return true
}

func (c *Call) dispatchHandRaise(state HandRaiseState) {
	c.dispatchHandRaiseAt(state, 0)
}

// A local completion must not update a replacement roster after submission.
// A zero transaction is reserved for provider notifications (no roster epoch
// is carried in the provider hand-action envelope).
func (c *Call) dispatchHandRaiseAt(state HandRaiseState, transaction uint32) bool {
	// Source of truth: https://github.com/purpshell/meowcaller/blob/36d54857c74e45ccb08f6444a32d2afa13f20be9/datasheets/group-video-reactions.md#L31-L43
	c.mu.Lock()
	if c.handRaises == nil {
		c.handRaises = make(map[types.JID]bool)
	}
	key := state.Participant.ToNonAD()
	if c.phase == CallPhaseEnded || key.IsEmpty() ||
		(key.Server != types.HiddenUserServer && key.Server != types.DefaultUserServer) ||
		(transaction != 0 && (c.groupState == nil || c.groupState.TransactionID != transaction)) ||
		!connectedHandParticipant(c.groupState, key) {
		c.mu.Unlock()
		return false
	}
	if c.handRaises[key] == state.Raised {
		c.mu.Unlock()
		return true
	}
	state.Participant = key
	if state.Raised {
		c.handRaises[key] = true
	} else {
		delete(c.handRaises, key)
	}
	if c.groupState != nil {
		c.groupState.HandRevision++
		for i := range c.groupState.Participants {
			if c.groupState.Participants[i].JID.ToNonAD() == key {
				c.groupState.Participants[i].HandRaised = state.Raised
			}
		}
	}
	fn := c.onHandRaise
	c.mu.Unlock()
	if fn != nil {
		fn(state)
	}
	return true
}

func (c *Call) dispatchScreenShare(state ScreenShareState) {
	c.dispatchScreenShareAt(state, 0)
}

func (c *Call) dispatchScreenShareAt(state ScreenShareState, transaction uint32) bool {
	c.mu.Lock()
	key := state.Participant.ToNonAD()
	if c.phase == CallPhaseEnded || !connectedHandParticipant(c.groupState, key) ||
		(transaction != 0 && c.groupState.TransactionID != transaction) ||
		(state.Active && state.Version != 2) {
		c.mu.Unlock()
		return false
	}
	if c.screenShares == nil {
		c.screenShares = make(map[types.JID]ScreenShareState)
	}
	state.Participant = key
	requested := state.Device
	if !requested.IsEmpty() && requested.ToNonAD() != key {
		c.mu.Unlock()
		return false
	}
	state.Device, state.PID, state.HasPID = types.EmptyJID, 0, false
	previous, exists := c.screenShares[key]
	if state.Active {
		// Replayed bare controls cannot acquire a device merely because the
		// roster changed since an ambiguous start. A fresh explicit device
		// control may disambiguate the same share.
		if requested.IsEmpty() && exists && !previous.HasPID &&
			previous.HasScreenShareID == state.HasScreenShareID && previous.ScreenShareID == state.ScreenShareID {
			c.mu.Unlock()
			return true
		}
		if d, ok := screenShareDevice(c.groupState, key, requested); ok {
			state.Device, state.PID, state.HasPID = d.JID, d.PID, true
		} else if !requested.IsEmpty() {
			c.mu.Unlock()
			return false
		}
		// An ambiguous participant-only refresh cannot replace an explicit
		// binding. In particular it cannot make another device the source.
		if exists && previous.HasPID && !state.HasPID {
			c.mu.Unlock()
			return false
		}
		if exists && previous == state {
			c.mu.Unlock()
			return true
		}
		c.screenShares[key] = state
	} else {
		// A stop from a different explicit device cannot retire the current share.
		if !requested.IsEmpty() && exists && (!previous.HasPID || previous.Device != requested) {
			c.mu.Unlock()
			return false
		}
		// A delayed, identified stop must not retire a newer share.
		if state.HasScreenShareID && (!exists || !previous.HasScreenShareID || previous.ScreenShareID != state.ScreenShareID) {
			c.mu.Unlock()
			return false
		}
		if !exists {
			c.mu.Unlock()
			return true
		}
		state.Device, state.PID, state.HasPID = previous.Device, previous.PID, previous.HasPID
		delete(c.screenShares, key)
	}
	c.advanceScreenRoleLocked(key)
	fn := c.onScreenShare
	c.mu.Unlock()
	if fn != nil {
		fn(state)
	}
	return true
}

func cloneWaitingRoomState(state WaitingRoomState) WaitingRoomState {
	state.Users = append([]WaitingRoomUser(nil), state.Users...)
	return state
}

// Subscribe attaches p as the call's outbound audio player, replacing any previous one.
// While the player is Playing, its source frames are encoded and sent to the peer;
// otherwise silence is sent (the call must keep sending to hold the relay bridge).
func (c *Call) Subscribe(p *Player) {
	c.mu.Lock()
	c.player = p
	c.mu.Unlock()
}

// Play is a shortcut: it creates a Player, subscribes it, starts src, and returns the
// Player (use it for Pause/Stop/OnFinish).
func (c *Call) Play(src AudioSource) *Player {
	p := NewPlayer()
	c.Subscribe(p)
	p.Play(src)
	return p
}

// Receive attaches a sink for the peer's decoded audio (16 kHz mono frames), replacing
// any previous one. Without a sink the inbound audio is decoded and discarded.
func (c *Call) Receive(sink AudioSink) {
	c.mu.Lock()
	c.sink = sink
	c.mu.Unlock()
}

// ReceiveVideo attaches a sink for the peer's H.264 video, delivered as Annex-B access units
// (one per frame, reassembled on the RTP marker), replacing any previous one. Without a sink
// the inbound video is discarded. The video analog of Receive; AnnexBRecorder records to a
// .h264 file, or use VideoSinkFunc to forward to a callback.
//
// Live inbound video interoperability remains unverified; no captured video RTP test vector is available.
func (c *Call) ReceiveVideo(sink VideoSink) {
	c.mu.Lock()
	c.videoSink = sink
	c.mu.Unlock()
}

// videoSinkRef returns the Call's current video sink under its lock.
func (c *Call) videoSinkRef() VideoSink {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.videoSink
}

// OnVideoState registers a callback for ordered inbound video transitions,
// including camera stop/start and upgrade negotiation. Registration replays the
// last known state, not the earlier history. Replacing the callback discards
// queued notifications for the old registration (an in-flight callback may
// finish). No new notifications are delivered after observing call termination.
func (c *Call) OnVideoState(fn func(VideoState)) {
	c.mu.Lock()
	c.onVideoState = fn
	c.videoCallbackGeneration++
	drain := false
	if c.lastVideoState != nil && fn != nil && c.phase != CallPhaseEnded {
		drain = c.queueVideoStateLocked(*c.lastVideoState)
	}
	c.mu.Unlock()
	if drain {
		c.drainNotifications()
	}
}

// OnVideoKeyframeRequest registers a callback for authenticated WhatsApp PLI/FIR
// feedback. Encoded video sources should make their next access unit an IDR.
func (c *Call) OnVideoKeyframeRequest(fn func()) {
	c.mu.Lock()
	c.onVideoKeyframeRequest = fn
	c.mu.Unlock()
}

// OnParticipantMediaEpoch installs a synchronous local fence and replays its
// latest value. Callbacks execute under the media registry/epoch locks: they
// must only update local admission state, never call provider methods or do I/O.
func (c *Call) OnParticipantMediaEpoch(fn func(ParticipantMediaEpoch)) {
	c.participantEpochMu.Lock()
	defer c.participantEpochMu.Unlock()
	c.onParticipantMediaEpoch = fn
	if fn != nil {
		fn(c.participantEpoch)
	}
}

func (c *Call) recordParticipantMediaEpoch(epoch ParticipantMediaEpoch) {
	c.participantEpochMu.Lock()
	defer c.participantEpochMu.Unlock()
	if c.participantEpoch == epoch {
		return
	}
	c.participantEpoch = epoch
	if c.onParticipantMediaEpoch != nil {
		c.onParticipantMediaEpoch(epoch)
	}
}

// OnParticipantAudioFrame registers a listener for independent inbound audio.
// The callback runs on the receive goroutine, must return promptly, and is
// additive to Receive. Passing nil detaches the listener. An already-running
// callback may finish after detachment or call termination.
func (c *Call) OnParticipantAudioFrame(fn func(ParticipantAudioFrame)) {
	c.mu.Lock()
	c.onParticipantAudioFrame = fn
	c.mu.Unlock()
}

func (c *Call) dispatchParticipantAudioFrame(audio decodedParticipantAudio, clockRate uint32) {
	c.mu.Lock()
	fn := c.onParticipantAudioFrame
	ended := c.phase == CallPhaseEnded
	c.mu.Unlock()
	if fn == nil || ended || len(audio.PCM) == 0 {
		return
	}
	fn(ParticipantAudioFrame{
		Epoch:         audio.Epoch,
		ParticipantID: audio.ParticipantID,
		Sender:        audio.UserJID,
		Device:        audio.DeviceJID,
		PID:           audio.PID,
		HasPID:        audio.HasPID,
		SSRC:          audio.SSRC,
		Timestamp:     audio.Timestamp,
		RTPClockRate:  clockRate,
		SampleRate:    SampleRate,
		PCM:           append([]float32(nil), audio.PCM...),
	})
}

// OnParticipantVideoFrame registers a callback for authenticated H.264 access
// units with their group participant identity. It is additive to ReceiveVideo.
func (c *Call) OnParticipantVideoFrame(fn func(ParticipantVideoFrame)) {
	// Source of truth: https://github.com/purpshell/meowcaller/blob/36d54857c74e45ccb08f6444a32d2afa13f20be9/datasheets/group-video-reactions.md#L32-L54
	c.mu.Lock()
	c.onParticipantVideoFrame = fn
	c.mu.Unlock()
}

// Snapshot the signalled role before H.264 assembly; the role is not an RTP-authenticated field.
func (c *Call) videoRoleLocked(epoch ParticipantMediaEpoch, sender, device types.JID, pid uint32, hasPID bool) GroupVideoRole {
	if !epoch.Valid || c.groupState == nil || c.groupState.TransactionID != epoch.RosterTransaction || !hasPID {
		return GroupVideoRole{}
	}
	key := sender.ToNonAD()
	role := GroupVideoRole{Revision: c.screenRoleRevisionLocked(key)}
	if share, exists := c.screenShares[key]; exists && share.Active && screenShareBindingCurrent(c.groupState, key, share) {
		role.ScreenShare = share.Device == device && share.PID == pid
	}
	return role
}

func (c *Call) videoRole(epoch ParticipantMediaEpoch, sender, device types.JID, pid uint32, hasPID bool) GroupVideoRole {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.videoRoleLocked(epoch, sender, device, pid, hasPID)
}

func (c *Call) dispatchParticipantVideoFrame(frame ParticipantVideoFrame) bool {
	c.mu.Lock()
	fn := c.onParticipantVideoFrame
	if frame.Epoch.Valid {
		role := c.videoRoleLocked(frame.Epoch, frame.Sender, frame.Device, frame.PID, frame.HasPID)
		if role.Revision == 0 || frame.RoleRevision != role.Revision || frame.ScreenShare != role.ScreenShare {
			c.mu.Unlock()
			return false
		}
	}
	c.mu.Unlock()
	if fn == nil {
		return false
	}
	fn(cloneParticipantVideoFrame(frame))
	return true
}

// OnReaction registers a callback for emoji reactions targeting this call.
func (c *Call) OnReaction(fn func(CallReaction)) {
	c.mu.Lock()
	c.onReaction = fn
	c.mu.Unlock()
}

func (c *Call) onReactionFn() func(CallReaction) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.onReaction
}

func (c *Call) requestVideoKeyframe() {
	c.mu.Lock()
	fn := c.onVideoKeyframeRequest
	c.mu.Unlock()
	if fn != nil {
		fn()
	}
}

func (c *Call) recordVideoState(state VideoState) {
	c.mu.Lock()
	if c.phase == CallPhaseEnded {
		c.mu.Unlock()
		return
	}
	c.lastVideoState = &state
	drain := c.queueVideoStateLocked(state)
	c.mu.Unlock()
	if drain {
		c.drainNotifications()
	}
}

// Caller holds c.mu. Queue insertion and recording are one ordered operation.
func (c *Call) queueVideoStateLocked(state VideoState) bool {
	fn := c.onVideoState
	if fn == nil {
		return false
	}
	generation := c.videoCallbackGeneration
	return c.queueNotification(func() {
		c.mu.Lock()
		fresh := generation == c.videoCallbackGeneration && c.phase != CallPhaseEnded
		c.mu.Unlock()
		if fresh {
			fn(state)
		}
	})
}

// ErrVideoFrameDropped indicates that an access unit was not submitted to the relay.
// The encoder should supply a fresh recovery IDR; this is not a call failure.
var ErrVideoFrameDropped = errors.New("meowcaller: video access unit dropped")

// SendVideo sends one already-encoded H.264 access unit (Annex-B) to the peer — fed from an
// external encoder (browser WebCodecs, ffmpeg, hardware). Returns an error if the call has no
// active video media yet, ErrVideoFrameDropped for a discarded access unit, or a
// transport error if relay submission fails. meowcaller does not encode pixels (no pure-Go H.264 encoder); this
// is the video analog of writing a sample to a track.
//
// Live outbound video interoperability remains unverified.
func (c *Call) SendVideo(accessUnit []byte) error {
	return c.SendVideoWithDuration(accessUnit, 0)
}

// SendVideoWithDuration sends one already-encoded H.264 access unit and advances the
// outgoing RTP timestamp by duration for the next frame. A zero duration uses the sender
// fallback frame step.
func (c *Call) SendVideoWithDuration(accessUnit []byte, duration time.Duration) error {
	return c.eng.sendVideoFrame(c.id, accessUnit, duration)
}

// RelayReady reports acknowledged allocation and consent, or authenticated peer media.
// It is a transport gate, not proof of bidirectional audio or video.
func (c *Call) RelayReady() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.relayReady && c.phase != CallPhaseEnded
}

func (c *Call) markRelayReady() {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.phase != CallPhaseEnded {
		c.relayReady = true
	}
}

// OnReady registers a callback fired once media is flowing (relay bound, first frames
// exchanged).
func (c *Call) OnReady(fn func()) {
	c.mu.Lock()
	c.onReady = fn
	ready := c.mediaReady && c.phase != CallPhaseEnded
	c.mu.Unlock()
	if ready && fn != nil {
		fn()
	}
}

// OnEnd registers a callback fired when the call ends, with a short reason string.
func (c *Call) OnEnd(fn func(reason string)) {
	c.mu.Lock()
	reason, complete := c.terminalReason, c.terminalCleanupComplete
	if !complete {
		c.onEnd = fn
	}
	c.mu.Unlock()
	if complete && fn != nil {
		fn(reason)
	}
}

// OnStateChange registers a callback fired on each phase transition.
func (c *Call) OnStateChange(fn func(CallPhase)) {
	c.mu.Lock()
	c.onState = fn
	c.mu.Unlock()
}

// PeerAccepted reports the recorded acceptance independently of callback delivery.
// A terminal call must never be treated as accepted.
func (c *Call) PeerAccepted() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.peerAccepted && c.phase != CallPhaseEnded
}

// OnPeerAccept registers a one-shot callback for the remote peer accepting an outgoing
// call. If acceptance arrived before registration, the callback is invoked immediately.
func (c *Call) OnPeerAccept(fn func()) {
	c.mu.Lock()
	c.onPeerAccept = fn
	shouldNotify := c.phase != CallPhaseEnded && c.peerAccepted && !c.acceptNotified && fn != nil
	if shouldNotify {
		c.acceptNotified = true
	}
	c.mu.Unlock()
	if shouldNotify {
		fn()
	}
}

func (c *Call) markPeerAccepted() {
	c.mu.Lock()
	if c.phase == CallPhaseEnded {
		c.mu.Unlock()
		return
	}
	c.peerAccepted = true
	fn := c.onPeerAccept
	shouldNotify := !c.acceptNotified && fn != nil
	if shouldNotify {
		c.acceptNotified = true
	}
	c.mu.Unlock()
	if shouldNotify {
		fn()
	}
}

// OnMuteState registers a callback for ordered inbound WhatsApp mute_v2 states.
// True means the remote microphone is muted. Registration replays only the last
// known state. Callback replacement and termination follow OnVideoState semantics.
func (c *Call) OnMuteState(fn func(muted bool)) {
	c.mu.Lock()
	c.onMuteState = fn
	c.muteCallbackGeneration++
	drain := false
	if c.lastMute != nil && fn != nil && c.phase != CallPhaseEnded {
		drain = c.queueMuteLocked(*c.lastMute)
	}
	c.mu.Unlock()
	if drain {
		c.drainNotifications()
	}
}

func (c *Call) recordMute(muted bool) {
	c.mu.Lock()
	if c.phase == CallPhaseEnded {
		c.mu.Unlock()
		return
	}
	c.lastMute = &muted
	drain := c.queueMuteLocked(muted)
	c.mu.Unlock()
	if drain {
		c.drainNotifications()
	}
}

// Caller holds c.mu. Queue insertion and recording are one ordered operation.
func (c *Call) queueMuteLocked(muted bool) bool {
	fn := c.onMuteState
	if fn == nil {
		return false
	}
	generation := c.muteCallbackGeneration
	return c.queueNotification(func() {
		c.mu.Lock()
		fresh := generation == c.muteCallbackGeneration && c.phase != CallPhaseEnded
		c.mu.Unlock()
		if fresh {
			fn(muted)
		}
	})
}

// advancePhase atomically prevents delayed signaling from regressing a call.
func (c *Call) advancePhase(next CallPhase) {
	c.mu.Lock()
	if c.phase >= next || (next == CallPhaseRinging && c.phase != CallPhaseCalling) {
		c.mu.Unlock()
		return
	}
	c.phase = next
	fn := c.onState
	c.mu.Unlock()
	if fn != nil {
		fn(next)
	}
}

// setPhase advances the call's phase and fires OnStateChange (used by the engine).
func (c *Call) setPhase(next CallPhase) {
	c.mu.Lock()
	if c.phase == next || c.phase == CallPhaseEnded {
		c.mu.Unlock()
		return
	}
	c.phase = next
	fn := c.onState
	c.mu.Unlock()
	if fn != nil {
		fn(next)
	}
}
