package meowcaller

import (
	"fmt"
	"testing"
	"time"

	"go.mau.fi/whatsmeow/types"
)

func TestDirectControlIdentity(t *testing.T) {
	peer := types.JID{User: "42", Server: types.HiddenUserServer, Device: 5}
	creator := types.JID{User: "51", Server: types.HiddenUserServer, Device: 2}
	wrongDevice := peer
	wrongDevice.Device++
	wrongNamespace := peer
	wrongNamespace.Server = types.DefaultUserServer
	for _, tc := range []struct {
		name                                string
		sender, claimedCreator, participant types.JID
		want                                bool
	}{
		{"exact", peer, creator, types.JID{}, true},
		{"bare", peer.ToNonAD(), creator.ToNonAD(), types.JID{}, true},
		{"matching participant", peer.ToNonAD(), creator, peer, true},
		{"missing sender", types.JID{}, creator, types.JID{}, false},
		{"missing creator", peer, types.JID{}, types.JID{}, false},
		{"wrong peer", creator, creator, types.JID{}, false},
		{"wrong creator", peer, peer, types.JID{}, false},
		{"wrong namespace", wrongNamespace, creator, types.JID{}, false},
		{"wrong device", wrongDevice, creator, types.JID{}, false},
		{"wrong participant", peer.ToNonAD(), creator, wrongDevice, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := directControlMatches(peer, creator, tc.sender, tc.claimedCreator, tc.participant); got != tc.want {
				t.Fatalf("match = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestDirectLifecycleIdentity(t *testing.T) {
	peer := types.NewJID("42", types.HiddenUserServer)
	creator := types.NewJID("51", types.HiddenUserServer)
	device := peer
	device.Device = 7
	other := device
	other.Device++
	for _, tc := range []struct {
		name                           string
		expected, sender, owner, group types.JID
		want                           bool
	}{
		{"new answering device", peer, device, creator, types.JID{}, true},
		{"selected device", device, device, creator, types.JID{}, true},
		{"bare replay", device, peer, creator, types.JID{}, true},
		{"different device", device, other, creator, types.JID{}, false},
		{"missing creator", peer, device, types.JID{}, types.JID{}, false},
		{"wrong creator", peer, device, peer, types.JID{}, false},
		{"wrong sender", peer, creator, creator, types.JID{}, false},
		{"group signal", peer, device, creator, types.NewJID("group", types.GroupServer), false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			meta := types.BasicCallMeta{From: tc.sender, CallCreator: tc.owner, GroupJID: tc.group}
			if got := directLifecycleMatches(tc.expected, creator, meta); got != tc.want {
				t.Fatalf("match=%v want=%v", got, tc.want)
			}
		})
	}
}

func TestEarlyTerminationIdentityAndLifetime(t *testing.T) {
	now := time.Unix(1000, 0)
	sender := types.JID{User: "42", Server: types.HiddenUserServer, Device: 3}
	creator := sender.ToNonAD()
	terminal := types.BasicCallMeta{CallID: "cid", From: sender, CallCreator: creator}
	entries := rememberEarlyTermination(nil, terminal, now)
	if len(entries) != 1 {
		t.Fatal("missing reservation")
	}
	for _, tc := range []struct {
		name        string
		edit        func(*types.BasicCallMeta)
		group, want bool
	}{
		{"same", func(m *types.BasicCallMeta) {}, false, true},
		{"bare sender", func(m *types.BasicCallMeta) { m.From = creator }, false, true},
		{"different sender device", func(m *types.BasicCallMeta) { m.From.Device++ }, false, false},
		{"different sender user", func(m *types.BasicCallMeta) { m.From.User = "43" }, false, false},
		{"different creator", func(m *types.BasicCallMeta) { m.CallCreator.User = "43" }, false, false},
		{"different namespace", func(m *types.BasicCallMeta) { m.From.Server = types.DefaultUserServer }, false, false},
		{"different ID", func(m *types.BasicCallMeta) { m.CallID = "other" }, false, false},
		{"group offer", func(m *types.BasicCallMeta) { m.GroupJID = types.NewJID("g", types.GroupServer) }, true, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			m := terminal
			tc.edit(&m)
			if got := hasEarlyTermination(entries, m, tc.group, now); got != tc.want {
				t.Fatalf("match %v want %v", got, tc.want)
			}
		})
	}
	rememberEarlyTermination(entries, terminal, now.Add(time.Second))
	if hasEarlyTermination(entries, terminal, false, now.Add(endedCallRetention)) || len(entries) != 0 {
		t.Fatal("duplicate extended expiry")
	}
}

func TestEarlyTerminationGroupIsolation(t *testing.T) {
	now := time.Unix(1000, 0)
	creator := types.NewJID("42", types.HiddenUserServer)
	group := types.NewJID("group", types.GroupServer)
	offer := types.BasicCallMeta{CallID: "cid", From: creator, CallCreator: creator, GroupJID: group}
	terminal := offer
	terminal.From = types.NewJID("cid", "call")
	entries := rememberEarlyTermination(nil, terminal, now)
	if !hasEarlyTermination(entries, offer, true, now) {
		t.Fatal("group route not reserved")
	}
	other := offer
	other.GroupJID = types.NewJID("other", types.GroupServer)
	if hasEarlyTermination(entries, other, true, now) || hasEarlyTermination(entries, offer, false, now) {
		t.Fatal("group reservation crossed identity")
	}
	terminal.From.User = "unrelated"
	if got := rememberEarlyTermination(nil, terminal, now); len(got) != 0 {
		t.Fatal("invalid call route admitted")
	}
}

func TestEarlyTerminationBoundAndMalformed(t *testing.T) {
	now := time.Unix(1000, 0)
	valid := types.BasicCallMeta{CallID: "cid", From: types.NewJID("42", types.HiddenUserServer), CallCreator: types.NewJID("42", types.HiddenUserServer)}
	for _, bad := range []types.BasicCallMeta{{}, {CallID: "cid", From: valid.From}, {CallID: "cid", CallCreator: valid.CallCreator}} {
		if len(rememberEarlyTermination(nil, bad, now)) != 0 {
			t.Fatal("invalid identity stored")
		}
	}
	entries := map[earlyTerminationIdentity]time.Time{}
	for i := 0; i < maxEndedCalls; i++ {
		m := valid
		m.CallID = fmt.Sprintf("old-%d", i)
		key, _ := earlyTerminationKey(m)
		entries[key] = now.Add(time.Minute)
	}
	entries = rememberEarlyTermination(entries, valid, now)
	if len(entries) != maxEndedCalls || !hasEarlyTermination(entries, valid, false, now) {
		t.Fatal("capacity not enforced")
	}
}

func TestDirectRelayPeerCorrelation(t *testing.T) {
	peer := types.NewJID("42", types.HiddenUserServer)
	device := peer
	device.Device = 3
	other := device
	other.Device = 4
	unrelated := peer
	unrelated.User = "43"
	for _, tc := range []struct {
		expected, elected types.JID
		want              bool
	}{
		{peer, device, true}, {device, device, true}, {device, peer, true}, {device, types.JID{}, true}, {device, other, false}, {peer, unrelated, false},
	} {
		if got := directRelayPeerMatches(tc.expected, tc.elected); got != tc.want {
			t.Fatalf("%s %s: %v want %v", tc.expected, tc.elected, got, tc.want)
		}
	}
}
