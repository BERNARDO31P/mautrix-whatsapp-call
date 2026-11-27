package meowcaller

import (
	"go.mau.fi/whatsmeow/types"
	"testing"
)

func TestGroupDeviceObservationsFollowExactConnectedRoster(t *testing.T) {
	p := types.NewJID("100", types.HiddenUserServer)
	a, b := p, p
	a.Device = 1
	b.Device = 2
	c := &Call{phase: CallPhaseActive}
	set := func(tx uint32, state string, pid uint32) {
		c.setGroupState(GroupCallState{TransactionID: tx, Participants: []GroupCallParticipant{{JID: p, State: state, Devices: []GroupCallDevice{{JID: a, HasPID: true, PID: pid}, {JID: b, HasPID: true, PID: 8}}}}})
	}
	set(1, "connected", 7)
	if c.observeGroupDeviceMedia(1, p, "video", "on") || !c.observeGroupDeviceMedia(1, a, "video", "on") || !c.observeGroupDeviceMedia(1, b, "mute_v2", "muted") {
		t.Fatal("device attribution")
	}
	g, _ := c.GroupState()
	if g.Participants[0].Devices[0].Media.Camera != "on" || g.Participants[0].Devices[0].Media.Microphone != "" || g.Participants[0].Devices[1].Media.Microphone != "muted" || g.Participants[0].Devices[1].Media.Camera != "" {
		t.Fatal(g)
	}
	g.Participants[0].Devices[0].Media.Camera = "off"
	set(2, "connected", 7)
	next, _ := c.GroupState()
	if next.Participants[0].Devices[0].Media.Camera != "on" || next.MediaRevision <= g.MediaRevision || c.observeGroupDeviceMedia(1, a, "video", "off") {
		t.Fatal(next)
	}
	set(3, "connected", 9)
	next, _ = c.GroupState()
	if next.Participants[0].Devices[0].Media.Camera != "" || next.Participants[0].Devices[1].Media.Microphone != "muted" {
		t.Fatal(next)
	}
	set(4, "ringing", 9)
	set(5, "connected", 9)
	next, _ = c.GroupState()
	if next.Participants[0].Devices[1].Media.Microphone != "" {
		t.Fatal(next)
	}
}
