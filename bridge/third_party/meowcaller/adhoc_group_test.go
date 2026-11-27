package meowcaller

import (
	"go.mau.fi/whatsmeow/types"
	"testing"
)

func TestExplicitAdHocRoster(t *testing.T) {
	self := types.NewJID("100", types.HiddenUserServer)
	alice := types.NewJID("200", types.HiddenUserServer)
	bob := types.NewJID("300", types.HiddenUserServer)
	phone := types.NewJID("400", types.DefaultUserServer)
	other := types.NewJID("500", types.HiddenUserServer)
	members := []types.GroupParticipant{{JID: self}, {JID: alice, LID: alice, PhoneNumber: phone}, {JID: bob}, {JID: other}}
	own := []types.JID{self}
	got, err := explicitAdHocRoster(members, own, []string{phone.String(), bob.String()})
	if err != nil || len(got) != 2 || got[0] != alice || got[1] != bob {
		t.Fatal(got, err)
	}
	for _, targets := range [][]string{nil, {alice.String()}, {alice.String(), alice.String()}, {alice.String(), phone.String(), bob.String()}, {self.String(), bob.String()}, {"999@lid", bob.String()}, {types.NewADJID("200", 0, 2).String(), bob.String()}} {
		if got, err := explicitAdHocRoster(members, own, targets); err == nil || got != nil {
			t.Fatal("invalid selection accepted", targets, got, err)
		}
	}
	if _, err := explicitAdHocRoster(members, nil, []string{alice.String(), bob.String()}); err == nil {
		t.Fatal("missing local member accepted")
	}
}

func TestAdHocGroupCannotBecomeChatBound(t *testing.T) {
	eng, call, creator := testGroupEngine("GROUP")
	eng.calls[call.ID()].explicitAdHoc = true
	update := groupCallUpdate{CallID: call.ID(), CallCreator: creator, GroupJID: types.NewJID("123", types.GroupServer), TransactionID: 1}
	if eng.applyGroupUpdate(update) || call.State() != CallPhaseEnded {
		t.Fatal("ad hoc room changed", call.State())
	}
}
