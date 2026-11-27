package meowcaller

import (
	"go.mau.fi/whatsmeow/types"
	"testing"
)

func TestLinkAdmissionRequiresExactConnectedDevice(t *testing.T) {
	self := types.JID{User: "123", Server: types.HiddenUserServer, Device: 2}
	other := self
	other.Device++
	update := groupCallUpdate{Participants: []groupCallParticipant{{JID: self.ToNonAD(), State: "connected", Devices: []groupCallDevice{{JID: other, HasPID: true}}}}}
	if linkRosterAdmitsDevice(update, self) {
		t.Fatal("other device admitted")
	}
	update.Participants[0].Devices[0].JID = self
	update.Participants[0].Devices[0].HasPID = false
	if linkRosterAdmitsDevice(update, self) {
		t.Fatal("missing PID admitted")
	}
	update.Participants[0].Devices[0].HasPID = true
	update.Participants[0].State = "pending"
	if linkRosterAdmitsDevice(update, self) {
		t.Fatal("waiting device admitted")
	}
	update.Participants[0].State = "connected"
	if !linkRosterAdmitsDevice(update, self) {
		t.Fatal("exact device not admitted")
	}
}

func TestLinkRosterAdmissionAndDeparture(t *testing.T) {
	eng, call, creator := testGroupEngine("LINK")
	self := types.JID{User: "222", Server: types.HiddenUserServer, Device: 2}
	owner := eng.calls["LINK"]
	owner.linkCall = true
	owner.explicitAdHoc = true
	owner.inviteSelfDevice.JID = self
	call.setPhase(CallPhaseWaitingRoom)
	call.setWaitingRoomState(WaitingRoomState{Enabled: true, InWaitingRoom: true, TransactionID: 9})
	roster := groupCallUpdate{CallID: "LINK", CallCreator: creator, TransactionID: 1, Participants: []groupCallParticipant{{JID: self.ToNonAD(), State: "pending", Devices: []groupCallDevice{{JID: self, HasPID: true}}}}}
	if !eng.applyGroupUpdate(roster) || call.IsCallLinkAdmitted() || call.State() != CallPhaseWaitingRoom || owner.started {
		t.Fatal("pending device admitted")
	}
	roster.TransactionID++
	roster.Participants[0].State = "connected"
	wrong := roster
	wrong.CallCreator = types.NewJID("333", types.HiddenUserServer)
	if eng.applyGroupUpdate(wrong) || call.IsCallLinkAdmitted() {
		t.Fatal("wrong creator admitted")
	}
	if !eng.applyGroupUpdate(roster) || !call.IsCallLinkAdmitted() || call.State() != CallPhaseConnecting {
		t.Fatal("admission not committed")
	}
	if state, _ := call.WaitingRoomState(); state.InWaitingRoom {
		t.Fatal("waiting room not cleared")
	}
	roster.TransactionID++
	roster.Participants = nil
	eng.applyGroupUpdate(roster)
	if call.State() != CallPhaseEnded || call.IsCallLinkAdmitted() {
		t.Fatal("departed device retained admission")
	}
}
