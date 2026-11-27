package meowcaller

import (
	"github.com/purpshell/meowcaller/signaling"
	"go.mau.fi/whatsmeow/types"
	"testing"
)

func TestGroupHandConnectedPresenceAndGeneration(t *testing.T) {
	a := types.NewJID("100", types.HiddenUserServer)
	b := types.NewJID("200", types.HiddenUserServer)
	c := &Call{phase: CallPhaseActive}
	callbacks := 0
	c.OnHandRaise(func(HandRaiseState) { callbacks++ })
	set := func(tx uint32, participants ...GroupCallParticipant) {
		c.setGroupState(GroupCallState{TransactionID: tx, Participants: participants})
	}
	connected := GroupCallParticipant{JID: a, State: "connected"}
	set(1, connected)
	if !c.dispatchHandRaiseAt(HandRaiseState{Participant: a, Raised: true}, 1) || callbacks != 1 {
		t.Fatal("initial hand rejected")
	}
	first, _ := c.GroupState()
	c.dispatchHandRaise(HandRaiseState{Participant: a, Raised: true})
	duplicate, _ := c.GroupState()
	if callbacks != 1 || first.HandRevision != duplicate.HandRevision {
		t.Fatal("duplicate callback")
	}
	if c.dispatchHandRaiseAt(HandRaiseState{Participant: b, Raised: true}, 1) {
		t.Fatal("unknown participant")
	}
	set(2, connected)
	g, _ := c.GroupState()
	if !g.Participants[0].HandRaised || g.HandRevision <= duplicate.HandRevision {
		t.Fatal("refresh dropped hand or did not advance revision")
	}
	set(3, GroupCallParticipant{JID: a, State: "ringing"})
	g, _ = c.GroupState()
	if g.Participants[0].HandRaised || c.dispatchHandRaiseAt(HandRaiseState{Participant: a, Raised: true}, 3) {
		t.Fatal("disconnected hand retained")
	}
	set(4, connected)
	g, _ = c.GroupState()
	if g.Participants[0].HandRaised || c.dispatchHandRaiseAt(HandRaiseState{Participant: a, Raised: true}, 2) {
		t.Fatal("stale hand restored")
	}
	set(5, connected, connected)
	if c.dispatchHandRaiseAt(HandRaiseState{Participant: a, Raised: true}, 5) {
		t.Fatal("ambiguous participant")
	}
	set(6, connected)
	if !c.dispatchHandRaiseAt(HandRaiseState{Participant: a, Raised: true}, 6) {
		t.Fatal("new hand rejected")
	}
	set(7)
	set(8, connected)
	g, _ = c.GroupState()
	if g.Participants[0].HandRaised {
		t.Fatal("departed hand restored")
	}
}
func TestGroupHandEnvelopeOwnership(t *testing.T) {
	eng, call, creator := testGroupEngine("GROUP")
	participant := types.NewJID("200", types.HiddenUserServer)
	call.setGroupState(GroupCallState{TransactionID: 1, Participants: []GroupCallParticipant{{JID: participant, State: "connected"}}})
	ev := &signaling.CallControlEnvelope{CallID: "GROUP", From: types.NewJID("GROUP", "call"), CallCreator: types.NewJID("999", types.HiddenUserServer), Participant: participant}
	eng.dispatchRemoteHandState(ev, true)
	g, _ := call.GroupState()
	if g.Participants[0].HandRaised {
		t.Fatal("wrong creator")
	}
	ev.CallCreator = creator
	ev.From = types.NewJID("999", types.HiddenUserServer)
	eng.dispatchRemoteHandState(ev, true)
	g, _ = call.GroupState()
	if g.Participants[0].HandRaised {
		t.Fatal("wrong sender")
	}
	ev.From = types.NewJID("GROUP", "call")
	eng.dispatchRemoteHandState(ev, true)
	g, _ = call.GroupState()
	if !g.Participants[0].HandRaised {
		t.Fatal("valid envelope rejected")
	}
}
