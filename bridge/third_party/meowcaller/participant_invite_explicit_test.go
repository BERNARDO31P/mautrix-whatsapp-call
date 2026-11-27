package meowcaller

import (
	"errors"
	"testing"

	"github.com/purpshell/meowcaller/signaling"
	waBinary "go.mau.fi/whatsmeow/binary"
	"go.mau.fi/whatsmeow/types"
)

func TestExplicitGroupTargetMembership(t *testing.T) {
	self := types.NewJID("100", types.HiddenUserServer)
	target := types.NewJID("200", types.HiddenUserServer)
	pn := types.NewJID("300", types.DefaultUserServer)
	members := []types.GroupParticipant{{JID: self}, {JID: target, LID: target, PhoneNumber: pn}}
	got, err := explicitGroupTarget(members, []types.JID{self}, pn)
	if err != nil || got != target {
		t.Fatal(got, err)
	}
	for _, x := range []types.JID{self, types.NewJID("999", types.HiddenUserServer), types.NewADJID("200", 0, 2)} {
		if _, err := explicitGroupTarget(members, []types.JID{self}, x); err == nil {
			t.Fatal("unauthorized target accepted", x)
		}
	}
	if _, err := explicitGroupTarget(members, nil, pn); err == nil {
		t.Fatal("absent local member")
	}
	members = append(members, types.GroupParticipant{JID: target})
	if _, err := explicitGroupTarget(members, []types.JID{self}, pn); err == nil {
		t.Fatal("ambiguous canonical alias")
	}
}
func TestExplicitInviteParticipantStates(t *testing.T) {
	self := types.NewJID("100", types.HiddenUserServer)
	target := types.NewJID("200", types.HiddenUserServer)
	other := types.NewJID("300", types.HiddenUserServer)
	roster := []signaling.GroupCallParticipant{{JID: self, State: "connected"}, {JID: target, State: "ringing"}}
	if got, err := explicitInviteParticipants(roster, target, true); err != nil || len(got) != 1 || got[0].JID != self {
		t.Fatal(got, err)
	}
	if got, err := explicitInviteParticipants(roster, other, false); err != nil || len(got) != 2 {
		t.Fatal(got, err)
	}
	for _, test := range []struct {
		jid  types.JID
		ring bool
	}{{target, false}, {self, true}, {other, true}} {
		if _, err := explicitInviteParticipants(roster, test.jid, test.ring); err == nil {
			t.Fatal("invalid state accepted", test)
		}
	}
}
func TestExplicitInviteSnapshotFencesReusedCall(t *testing.T) {
	eng, call, creator := testGroupEngine("GROUP")
	call.phase = CallPhaseActive
	group := types.NewJID("123", types.GroupServer)
	m := eng.calls[call.ID()]
	m.selfLID = creator.String()
	m.groupUpdate = &groupCallUpdate{GroupJID: group, CallID: call.ID(), Participants: []groupCallParticipant{{JID: creator, State: "connected"}}}
	if _, err := eng.explicitInviteSnapshot(call, group); err != nil {
		t.Fatal(err)
	}
	m.call = &Call{eng: eng, id: call.ID(), phase: CallPhaseActive}
	if _, err := eng.explicitInviteSnapshot(call, group); err == nil {
		t.Fatal("stale handle accepted")
	}
}
func TestExplicitInviteAckCorrelation(t *testing.T) {
	ack := &waBinary.Node{Tag: "ack", Attrs: waBinary.Attrs{"class": "call", "type": "offer", "id": "REQUEST"}}
	if err := explicitInviteAck(ack, "REQUEST", "CALL"); err != nil {
		t.Fatal(err)
	}
	if err := explicitInviteAck(ack, "OTHER", "CALL"); !errors.Is(err, ErrGroupParticipantUnknown) {
		t.Fatal(err)
	}
	ack.Attrs["error"] = "403"
	if err := explicitInviteAck(ack, "REQUEST", "CALL"); !errors.Is(err, ErrGroupParticipantRejected) {
		t.Fatal(err)
	}
	delete(ack.Attrs, "error")
	ack.Content = []waBinary.Node{{Tag: "group_info", Attrs: waBinary.Attrs{"call-id": "OTHER"}}}
	if err := explicitInviteAck(ack, "REQUEST", "CALL"); !errors.Is(err, ErrGroupParticipantUnknown) {
		t.Fatal(err)
	}
}
