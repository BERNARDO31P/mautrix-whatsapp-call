package meowcaller

import (
	"context"
	"errors"

	"github.com/purpshell/meowcaller/signaling"
	waBinary "go.mau.fi/whatsmeow/binary"
	"go.mau.fi/whatsmeow/types"
)

var ErrWaitingRoomStale = errors.New("meowcaller: waiting room control is not authorized for this snapshot")

// WaitingRoomSnapshot identifies the exact local observation, including admission
// changes. A generation is eligible for at most one submission.
func (c *Call) WaitingRoomSnapshot() (WaitingRoomState, uint64, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.waitingRoomState == nil || c.phase == CallPhaseEnded {
		return WaitingRoomState{}, 0, false
	}
	return cloneWaitingRoomState(*c.waitingRoomState), c.waitingRoomGeneration, true
}

// SubmitWaitingRoomControl validates the exact active call and current admin
// roster before selecting a protocol node. Success means submission, not ACK.
func (c *Call) SubmitWaitingRoomControl(ctx context.Context, generation uint64, action, target string, enabled *bool) error {
	if c == nil || c.eng == nil {
		return ErrWaitingRoomStale
	}
	e := c.eng
	if err := e.requireRawCallAdapter(); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	e.mu.Lock()
	m := e.calls[c.id]
	if m == nil || m.call != c || !m.linkCall || !m.linkAdmitted || m.creator.IsEmpty() {
		e.mu.Unlock()
		return ErrWaitingRoomStale
	}
	c.mu.Lock()
	creator := m.creator
	jid, ok := c.reserveWaitingRoomControlLocked(generation, action, target, enabled)
	c.mu.Unlock()
	e.mu.Unlock()
	if !ok {
		return ErrWaitingRoomStale
	}
	var node waBinary.Node
	switch action {
	case "approval":
		node = signaling.BuildWaitingRoomToggle(c.id, creator, *enabled, e.nextCallNodeID())
	case "admit":
		node = signaling.BuildWaitingRoomAdmit(c.id, creator, jid, e.nextCallNodeID())
	case "deny":
		node = signaling.BuildWaitingRoomDeny(c.id, creator, jid, e.nextCallNodeID())
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	return e.transmitCallNode(ctx, node)
}

// c.mu held. Reject stale, duplicate or ambiguous targets; never infer a PN
// from a LID. Only current provider pending entries may be admitted/denied.
func (c *Call) reserveWaitingRoomControlLocked(generation uint64, action, target string, enabled *bool) (types.JID, bool) {
	s := c.waitingRoomState
	if s == nil || !s.IsAdmin || s.InWaitingRoom || (c.phase != CallPhaseActive && c.phase != CallPhaseConnecting) || generation == 0 || generation != c.waitingRoomGeneration || generation == c.waitingControlGeneration {
		return types.EmptyJID, false
	}
	var jid types.JID
	switch action {
	case "approval":
		if enabled == nil || target != "" || *enabled == s.Enabled {
			return types.EmptyJID, false
		}
	case "admit", "deny":
		if enabled != nil || !s.Enabled || target == "" || len(s.Users) > 256 {
			return types.EmptyJID, false
		}
		for _, u := range s.Users {
			if u.JID.ToNonAD().String() != target {
				continue
			}
			if !jid.IsEmpty() || u.State != "waiting_room_joined" || u.JID.IsEmpty() {
				return types.EmptyJID, false
			}
			jid = u.JID.ToNonAD()
		}
		if jid.IsEmpty() || (jid.Server != types.HiddenUserServer && jid.Server != types.DefaultUserServer) {
			return types.EmptyJID, false
		}
	default:
		return types.EmptyJID, false
	}
	c.waitingControlGeneration = generation
	return jid, true
}
