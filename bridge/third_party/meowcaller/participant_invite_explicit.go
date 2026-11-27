package meowcaller

import (
	"context"
	"errors"
	"reflect"

	"github.com/purpshell/meowcaller/signaling"
	waBinary "go.mau.fi/whatsmeow/binary"
	"go.mau.fi/whatsmeow/types"
)

var ErrGroupParticipantNotAllowed = errors.New("group participant action is not allowed in the current call")
var ErrGroupParticipantRejected = errors.New("provider rejected group participant action")
var ErrGroupParticipantUnknown = errors.New("group participant action outcome unknown")

// ExplicitGroupParticipant targets exactly one member of an existing chat-bound
// group call. Unlike InviteGroupMembers, it never chooses the action implicitly.
// A nil error means the matching offer ACK was received, not that a phone rang.
func (c *Client) ExplicitGroupParticipant(ctx context.Context, call *Call, target types.JID, ring bool) error {
	if c == nil || c.eng == nil || c.wa == nil || call == nil || call.eng != c.eng || c.getGroupInfo == nil || ctx.Err() != nil {
		return ErrGroupParticipantNotAllowed
	}
	if err := c.eng.requireRawCallAdapter(); err != nil {
		return err
	}
	state, ok := call.GroupState()
	if !ok || state.GroupJID.IsEmpty() || state.GroupJID.Server != types.GroupServer || call.State() != CallPhaseActive {
		return ErrGroupParticipantNotAllowed
	}
	info, err := c.getGroupInfo(ctx, state.GroupJID)
	if err != nil {
		return err
	}
	if info == nil || info.JID.ToNonAD() != state.GroupJID {
		return ErrGroupParticipantNotAllowed
	}
	canonical, err := explicitGroupTarget(info.Participants, c.groupSelfJIDs(), target)
	if err != nil {
		return err
	}
	lid, err := resolvePeerLID(ctx, c.wa, canonical.String())
	if err != nil {
		return err
	}
	lid = lid.ToNonAD()
	if lid.IsEmpty() || lid.Server != types.HiddenUserServer {
		return ErrGroupParticipantNotAllowed
	}
	for _, self := range c.groupSelfJIDs() {
		if !self.IsEmpty() && self.ToNonAD() == lid {
			return ErrGroupParticipantNotAllowed
		}
	}
	before, err := c.eng.explicitInviteSnapshot(call, state.GroupJID)
	if err != nil {
		return err
	}
	participants, err := explicitInviteParticipants(before.participants, lid, ring)
	if err != nil {
		return err
	}
	devices, err := c.eng.discoverTargetDevices(ctx, lid)
	if err != nil {
		return err
	}
	for _, device := range devices {
		if device.ToNonAD() != lid {
			return ErrGroupParticipantNotAllowed
		}
	}
	node, err := signaling.BuildGroupInviteOffer(signaling.GroupInviteOfferParams{CallID: call.ID(), To: lid, CallCreator: before.creator, TargetDevices: devices, Participants: participants, Video: before.video})
	if err != nil {
		return err
	}
	id := c.eng.nextCallNodeID()
	node.Attrs["id"] = id
	// Device discovery can block; never submit a stale roster or reused call ID.
	after, err := c.eng.explicitInviteSnapshot(call, state.GroupJID)
	if err != nil || !reflect.DeepEqual(before, after) {
		return ErrGroupParticipantNotAllowed
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	ack, err := c.eng.requestCall(ctx, node, id)
	if err != nil {
		return ErrGroupParticipantUnknown
	}
	if _, err = c.eng.explicitInviteSnapshot(call, state.GroupJID); err != nil {
		return ErrGroupParticipantUnknown
	}
	return explicitInviteAck(ack, id, call.ID())
}

// Both the linked account and the target must be current group members. Reject
// ambiguous alias matches rather than choosing an arbitrary participant.
func explicitGroupTarget(members []types.GroupParticipant, own []types.JID, target types.JID) (types.JID, error) {
	if target.IsEmpty() || target != target.ToNonAD() || (target.Server != types.DefaultUserServer && target.Server != types.HiddenUserServer) {
		return types.EmptyJID, ErrGroupParticipantNotAllowed
	}
	ownPresent, targetCount := false, 0
	var canonical types.JID
	for _, member := range members {
		ids := []types.JID{member.JID, member.LID, member.PhoneNumber}
		self, match := false, false
		for _, id := range ids {
			if id.IsEmpty() {
				continue
			}
			id = id.ToNonAD()
			if id == target {
				match = true
			}
			for _, o := range own {
				if !o.IsEmpty() && id == o.ToNonAD() {
					self = true
				}
			}
		}
		ownPresent = ownPresent || self
		if !match {
			continue
		}
		if self {
			return types.EmptyJID, ErrGroupParticipantNotAllowed
		}
		targetCount++
		for _, id := range []types.JID{member.LID, member.JID, member.PhoneNumber} {
			if !id.IsEmpty() && (id.Server == types.HiddenUserServer || id.Server == types.DefaultUserServer) {
				canonical = id.ToNonAD()
				break
			}
		}
	}
	if !ownPresent || targetCount != 1 || canonical.IsEmpty() {
		return types.EmptyJID, ErrGroupParticipantNotAllowed
	}
	canonicalCount := 0
	for _, member := range members {
		for _, id := range []types.JID{member.JID, member.LID, member.PhoneNumber} {
			if !id.IsEmpty() && id.ToNonAD() == canonical {
				canonicalCount++
				break
			}
		}
	}
	if canonicalCount != 1 {
		return types.EmptyJID, ErrGroupParticipantNotAllowed
	}
	return canonical, nil
}

type explicitInviteState struct {
	creator      types.JID
	participants []signaling.GroupCallParticipant
	video        bool
	transaction  uint32
}

func (e *engine) explicitInviteSnapshot(call *Call, group types.JID) (explicitInviteState, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	m := e.calls[call.ID()]
	if m == nil || m.call != call || !m.group || m.groupUpdate == nil || m.creator.IsEmpty() || call.State() != CallPhaseActive {
		return explicitInviteState{}, ErrGroupParticipantNotAllowed
	}
	// A seed or mismatched call/group is not an authoritative connected roster.
	if m.groupUpdate.GroupJID != group || m.groupUpdate.CallID != call.ID() {
		return explicitInviteState{}, ErrGroupParticipantNotAllowed
	}
	self, err := types.ParseJID(m.selfLID)
	if err != nil || self.IsEmpty() {
		return explicitInviteState{}, ErrGroupParticipantNotAllowed
	}
	participants := signalingParticipantsFromUpdate(*m.groupUpdate)
	local := false
	for _, p := range participants {
		if p.JID.ToNonAD() == self.ToNonAD() && p.State == "connected" {
			local = true
		}
	}
	if !local {
		return explicitInviteState{}, ErrGroupParticipantNotAllowed
	}
	return explicitInviteState{m.creator, participants, m.localVideo || m.remoteVideo, m.groupUpdate.TransactionID}, nil
}

func explicitInviteParticipants(roster []signaling.GroupCallParticipant, target types.JID, ring bool) ([]signaling.GroupCallParticipant, error) {
	found := 0
	var connected []signaling.GroupCallParticipant
	for _, p := range roster {
		if p.JID.ToNonAD() == target || (!p.PN.IsEmpty() && p.PN.ToNonAD() == target) {
			found++
			if p.State == "connected" {
				return nil, ErrGroupParticipantNotAllowed
			}
		} else if p.State == "connected" {
			connected = append(connected, p)
		}
	}
	if (!ring && found != 0) || (ring && found != 1) || len(connected) == 0 {
		return nil, ErrGroupParticipantNotAllowed
	}
	if ring {
		return connected, nil
	}
	return roster, nil
}

func explicitInviteAck(ack *waBinary.Node, id, callID string) error {
	if ack == nil || ack.Tag != "ack" {
		return ErrGroupParticipantUnknown
	}
	a := ack.AttrGetter()
	if a.String("class") != "call" || a.String("type") != "offer" || a.String("id") != id || a.Error() != nil {
		return ErrGroupParticipantUnknown
	}
	if embedded := callAckEmbeddedCallID(ack); embedded != "" && embedded != callID {
		return ErrGroupParticipantUnknown
	}
	if _, hasError := ack.Attrs["error"]; hasError || callAckTerminalNode(ack) != nil || callAckErrorCode(ack) != "" {
		return ErrGroupParticipantRejected
	}
	return nil
}
