package connector

import (
	"maunium.net/go/mautrix/id"
	"testing"
	"time"
)

func TestRTCGroupFollowupRouting(t *testing.T) {
	now := time.Unix(100, 0)
	for _, tc := range []struct {
		name, slot, sticky, ref, notification, sender, device, mode string
		group, room, expired, duplicate, saturated, limit           bool
		targets                                                     []string
		matched, queued                                             bool
	}{
		{name: "legacy", ref: "$invite", notification: "notification", sender: "@alice:example.org", device: "A", group: true, targets: []string{"1@s.whatsapp.net"}, matched: true, queued: true},
		{name: "modern", slot: matrixRTCSlotDefault, ref: "$invite", sender: "@alice:example.org", device: "A", group: true, targets: []string{"1@s.whatsapp.net"}, matched: true, queued: true},
		{name: "membership reference", ref: "$membership", sender: "@alice:example.org", device: "A", group: true, targets: []string{"1"}},
		{name: "wrong device", ref: "$invite", sender: "@alice:example.org", device: "B", group: true, targets: []string{"1"}, matched: true},
		{name: "wrong sender", ref: "$invite", sender: "@bob:example.org", device: "A", group: true, targets: []string{"1"}, matched: true},
		{name: "missing device", ref: "$invite", sender: "@alice:example.org", group: true, targets: []string{"1"}, matched: true},
		{name: "direct call", ref: "$invite", sender: "@alice:example.org", device: "A", targets: []string{"1"}, matched: true},
		{name: "wrong slot", slot: "m.call#other", ref: "$invite", sender: "@alice:example.org", device: "A", group: true, targets: []string{"1"}, matched: true},
		{name: "wrong sticky slot", sticky: "m.call#other", ref: "$invite", sender: "@alice:example.org", device: "A", group: true, targets: []string{"1"}, matched: true},
		{name: "ring", ref: "$invite", notification: "ring", sender: "@alice:example.org", device: "A", group: true, targets: []string{"1"}, matched: true},
		{name: "room broadcast", ref: "$invite", sender: "@alice:example.org", device: "A", group: true, room: true, targets: []string{"1"}, matched: true},
		{name: "ad hoc followup refused", mode: "ad_hoc", ref: "$invite", sender: "@alice:example.org", device: "A", group: true, targets: []string{"1"}, matched: true},
		{name: "empty targets", ref: "$invite", sender: "@alice:example.org", device: "A", group: true, matched: true},
		{name: "expired", ref: "$invite", sender: "@alice:example.org", device: "A", group: true, expired: true, targets: []string{"1"}, matched: true},
		{name: "duplicate", ref: "$invite", sender: "@alice:example.org", device: "A", group: true, duplicate: true, targets: []string{"1"}, matched: true},
		{name: "saturated", ref: "$invite", sender: "@alice:example.org", device: "A", group: true, saturated: true, targets: []string{"1"}, matched: true},
		{name: "target cap", ref: "$invite", sender: "@alice:example.org", device: "A", group: true, limit: true, matched: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			current := rtcOutgoingSession{eventID: "$invite", sender: "@alice:example.org", device: "A", seenUpdates: map[string]bool{}}
			if tc.group {
				current.groupUpdates = make(chan rtcGroupUpdate, 1)
			}
			if tc.duplicate {
				current.seenUpdates["$update"] = true
			}
			if tc.saturated {
				current.groupUpdates <- rtcGroupUpdate{}
			}
			before := len(current.groupUpdates)
			var content rtcInviteContent
			content.GroupMode = tc.mode
			content.SlotID = tc.slot
			content.StickyKey = tc.sticky
			content.Mentions.Room = tc.room
			content.RelatesTo.RelType = "m.reference"
			content.RelatesTo.EventID = tc.ref
			deadline := now.Add(time.Second)
			if tc.expired {
				deadline = now
			}
			if tc.limit {
				tc.targets = make([]string, 32)
			}
			got := routeRTCGroupFollowup(current, content, tc.notification, "$update", tc.sender, id.DeviceID(tc.device), tc.targets, deadline, now)
			if got != tc.matched || (len(current.groupUpdates) > before) != tc.queued {
				t.Fatalf("matched %v queue size %d", got, len(current.groupUpdates))
			}
			if tc.queued {
				update := <-current.groupUpdates
				if !current.seenUpdates["$update"] || !update.deadline.Equal(deadline) || update.targets[0] != tc.targets[0] {
					t.Fatal("wrong queued update")
				}
			}
			if !tc.queued && !tc.duplicate && current.seenUpdates["$update"] {
				t.Fatal("refused update marked delivered")
			}
		})
	}
}
