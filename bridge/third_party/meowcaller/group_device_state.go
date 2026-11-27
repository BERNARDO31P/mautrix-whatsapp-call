package meowcaller

import (
	"github.com/purpshell/meowcaller/signaling"
	waBinary "go.mau.fi/whatsmeow/binary"
	"go.mau.fi/whatsmeow/types"
)

// GroupDeviceMediaState represents a provider observation, not a guess from
// silence or from the aggregate Matrix track. Empty fields mean unknown.
type GroupDeviceMediaState struct {
	Microphone string // muted, unmuted
	Camera     string // on, off
}

// Only one exact roster device may own an observation. Do not treat a bare
// address as a wildcard for all devices, nor accept a phone alias as a LID.
func groupMediaDeviceIndex(g *GroupCallState, jid types.JID) (int, int, bool) {
	if g == nil || g.TransactionID == 0 || jid.IsEmpty() || jid.User == "" {
		return 0, 0, false
	}
	if !boundedGroupMediaRoster(g) {
		return 0, 0, false
	}
	pi, di := -1, -1
	for i, p := range g.Participants {
		for j, d := range p.Devices {
			if d.JID != jid {
				continue
			}
			if pi >= 0 || p.State != "connected" || !connectedHandParticipant(g, p.JID.ToNonAD()) || jid.ToNonAD() != p.JID.ToNonAD() {
				return 0, 0, false
			}
			pi, di = i, j
		}
	}
	return pi, di, pi >= 0
}

// Preserve observations only over continuous presence of the same exact device
// and PID. A new or ambiguous roster device starts unknown.
func boundedGroupMediaRoster(g *GroupCallState) bool {
	if g == nil || len(g.Participants) > 256 {
		return false
	}
	count := 0
	for _, p := range g.Participants {
		count += len(p.Devices)
		if len(p.Devices) > 32 || count > 256 {
			return false
		}
	}
	return true
}

func preserveGroupDeviceMedia(previous, next *GroupCallState) {
	bounded := boundedGroupMediaRoster(next) && boundedGroupMediaRoster(previous)
	for i := range next.Participants {
		for j := range next.Participants[i].Devices {
			d := &next.Participants[i].Devices[j]
			d.Media = GroupDeviceMediaState{}
			if !bounded {
				continue
			}
			ni, nj, ok := groupMediaDeviceIndex(next, d.JID)
			if !ok || ni != i || nj != j {
				continue
			}
			pi, di, ok := groupMediaDeviceIndex(previous, d.JID)
			if !ok {
				continue
			}
			old := previous.Participants[pi].Devices[di]
			if old.HasPID && d.HasPID && old.PID == d.PID {
				d.Media = old.Media
			}
		}
	}
}

// Capture the engine's committed roster transaction, then apply only if the
// public roster still refers to that transaction. No aggregate media control
// or direct-call pickup state is changed here.
func (e *engine) observeGroupDeviceMedia(outer *waBinary.Node, expected *engineCall, transaction uint32, kind, value string) {
	envelope, err := signaling.ParseCallControlEnvelope(outer)
	if err != nil || envelope.Action.Tag != kind {
		return
	}
	sender := envelope.From
	if sender == types.NewJID(envelope.CallID, "call") {
		sender = envelope.Participant
	} else if !envelope.Participant.IsEmpty() && envelope.Participant != sender {
		return
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	m := e.calls[envelope.CallID]
	if m == nil || m != expected || !m.group || m.call == nil || m.groupUpdate == nil || transaction == 0 || m.groupUpdate.TransactionID != transaction || !sameDirectControlIdentity(m.creator, envelope.CallCreator) {
		return
	}
	m.call.observeGroupDeviceMedia(transaction, sender, kind, value)
}

func (c *Call) observeGroupDeviceMedia(transaction uint32, sender types.JID, kind, value string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	g := c.groupState
	if c.phase == CallPhaseEnded || transaction == 0 || g == nil || g.TransactionID != transaction {
		return false
	}
	i, j, ok := groupMediaDeviceIndex(g, sender)
	if !ok {
		return false
	}
	state := &g.Participants[i].Devices[j].Media
	switch kind {
	case "mute_v2":
		if value != "muted" && value != "unmuted" {
			return false
		}
		if state.Microphone == value {
			return true
		}
		state.Microphone = value
	case "video":
		if value != "on" && value != "off" {
			return false
		}
		if state.Camera == value {
			return true
		}
		state.Camera = value
	default:
		return false
	}
	g.MediaRevision++
	return true
}
