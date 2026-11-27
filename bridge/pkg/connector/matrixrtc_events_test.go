package connector

import (
	"context"
	"encoding/json"
	"testing"
	"time"
)

func TestMatrixRTCMemberForNotification(t *testing.T) {
	wa := &WhatsAppConnector{
		rtcMembers: map[string]rtcMemberRecord{},
	}
	roomID := "!room:example.org"
	eventID := "$member"
	sender := "@alice:example.org"
	key := rtcMemberEventKey(roomID, eventID)
	wa.rtcMembers[key] = rtcMemberRecord{
		slotID:  "m.call#room",
		sender:  sender,
		expires: time.Now().Add(time.Minute),
	}

	record, ok := wa.matrixRTCMemberForNotification(roomID, eventID, sender)
	if !ok {
		t.Fatal("expected referenced MatrixRTC membership to resolve")
	}
	if record.slotID != "m.call#room" {
		t.Fatalf("unexpected record: %+v", record)
	}
	if _, ok = wa.matrixRTCMemberForNotification(roomID, eventID, "@mallory:example.org"); ok {
		t.Fatal("accepted a notification referencing another sender membership")
	}
}

func TestMatrixRTCMemberForNotificationExpires(t *testing.T) {
	wa := &WhatsAppConnector{
		rtcMembers: map[string]rtcMemberRecord{},
	}
	roomID := "!room:example.org"
	eventID := "$expired"
	sender := "@alice:example.org"
	key := rtcMemberEventKey(roomID, eventID)
	wa.rtcMembers[key] = rtcMemberRecord{
		slotID:  "m.call#room",
		sender:  sender,
		expires: time.Now().Add(-time.Second),
	}

	if _, ok := wa.matrixRTCMemberForNotification(roomID, eventID, sender); ok {
		t.Fatal("accepted an expired MatrixRTC membership reference")
	}
	if _, exists := wa.rtcMembers[key]; exists {
		t.Fatal("expired MatrixRTC membership was not evicted")
	}
}

func TestMentionsGhost(t *testing.T) {
	var content rtcInviteContent
	content.Mentions.UserIDs = []string{"@whatsapp_123:example.org"}
	if !mentionsGhost(&content, "@whatsapp_123:example.org") {
		t.Fatal("explicit ghost mention was not accepted")
	}
	if mentionsGhost(&content, "@whatsapp_456:example.org") {
		t.Fatal("invite for another user was accepted")
	}

	content = rtcInviteContent{}
	content.NotificationType = "ring"
	if !mentionsGhost(&content, "@whatsapp_123:example.org") {
		t.Fatal("legacy room-state notification without m.mentions was rejected")
	}
}

func TestMatrixRTCLegacyMemberShapeIsJoin(t *testing.T) {
	var content rtcMemberContent
	content.SlotID = "m.call#ROOM"
	content.Member.ID = "legacy-membership-id"
	content.Application.Type = "m.call"
	content.Application.Intent = "video"
	isJoin := content.Member.Membership == "join" || (content.Member.Membership == "" && content.Member.ID != "")
	if !isJoin {
		t.Fatal("Element Call legacy member shape was not recognized as a join")
	}
}

func TestInlineRTCFormatUsesAuthenticatedStickyMembership(t *testing.T) {
	wa := &WhatsAppConnector{rtcMembers: make(map[string]rtcMemberRecord)}
	room, slot, sender, device := "!room:example.org", "m.call#room", "@alice:example.org", "A"
	record := rtcMemberRecord{sticky: true, slotID: slot, sender: sender, deviceID: device, format: "room_state", expires: time.Now().Add(time.Minute)}
	wa.rtcMembers[rtcMemberEventKey(room, "$old")] = record
	if record := wa.inlineRTCMembership(room, slot, sender, device); record.format != "room_state" || record.eventID != "$old" {
		t.Fatal("missed legacy membership on canonical inline slot")
	}
	if wa.inlineRTCMembership(room, slot, sender, "B").format != "" || wa.inlineRTCMembership("!other:example.org", slot, sender, device).format != "" {
		t.Fatal("matched another device or room")
	}
	record.format = "msc"
	record.expires = record.expires.Add(time.Second)
	wa.rtcMembers[rtcMemberEventKey(room, "$new")] = record
	if wa.inlineRTCMembership(room, slot, sender, device).format != "msc" {
		t.Fatal("older generation selected")
	}
}

func TestIncomingRTCMembershipFormat(t *testing.T) {
	for _, tc := range []struct{ value, want string }{
		{"", "room_state"}, {"room_state", "room_state"}, {"msc", "msc"},
	} {
		got, err := incomingRTCMembershipFormat(tc.value)
		if err != nil || got != tc.want {
			t.Fatalf("format %q: got %q, %v", tc.value, got, err)
		}
	}
	if _, err := incomingRTCMembershipFormat("unsupported"); err == nil {
		t.Fatal("unsupported incoming format accepted")
	}
}

func TestRTCNotificationPolicy(t *testing.T) {
	for _, tc := range []struct {
		name, raw, want string
		valid           bool
	}{
		{"bare", `{}`, "", true},
		{"flat ring", `{"notification_type":"ring"}`, "ring", true},
		{"nested ring", `{"application":{"notification_type":"ring"}}`, "ring", true},
		{"matching ring", `{"notification_type":"ring","application":{"notification_type":"ring"}}`, "ring", true},
		{"flat visual", `{"notification_type":"notification"}`, "notification", true},
		{"nested visual", `{"application":{"notification_type":"notification"}}`, "notification", true},
		{"matching visual", `{"notification_type":"notification","application":{"notification_type":"notification"}}`, "notification", true},
		{"conflicting flat ring", `{"notification_type":"ring","application":{"notification_type":"notification"}}`, "", false},
		{"conflicting nested ring", `{"notification_type":"notification","application":{"notification_type":"ring"}}`, "", false},
		{"unknown flat", `{"notification_type":"unknown"}`, "", false},
		{"unknown nested", `{"notification_type":"ring","application":{"notification_type":"unknown"}}`, "", false},
		{"unknown nested alone", `{"application":{"notification_type":"unknown"}}`, "", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var content rtcInviteContent
			if err := json.Unmarshal([]byte(tc.raw), &content); err != nil {
				t.Fatal(err)
			}
			got, valid := rtcNotificationPolicy(&content)
			if got != tc.want || valid != tc.valid {
				t.Fatalf("got %q, %v; want %q, %v", got, valid, tc.want, tc.valid)
			}
		})
	}
}

func TestWaitRTCReferencedMember(t *testing.T) {
	for _, tc := range []struct {
		name           string
		availableAfter int
		recordDevice   string
		cancelled      bool
		want           bool
	}{
		{"already available", 1, "A", false, true},
		{"notification first", 3, "A", false, true},
		{"legacy device absent", 2, "", false, true},
		{"different device", 1, "B", false, false},
		{"missing membership", 100000, "A", false, false},
		{"cancelled before lookup", 1, "A", true, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
			defer cancel()
			if tc.cancelled {
				cancel()
			}
			calls := 0
			record, ok := waitRTCReferencedMember(ctx, time.Millisecond, "A", func() (rtcMemberRecord, bool) {
				calls++
				return rtcMemberRecord{deviceID: tc.recordDevice}, calls >= tc.availableAfter
			})
			if ok != tc.want || (ok && record.deviceID != tc.recordDevice) {
				t.Fatalf("unexpected membership: %+v, %v", record, ok)
			}
			if tc.cancelled && calls != 0 {
				t.Fatal("lookup after cancellation")
			}
			if tc.recordDevice == "B" && calls != 1 {
				t.Fatal("retried wrong device")
			}
		})
	}
}

func TestWaitRTCReferencedMemberCancellationDuringLookup(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	_, ok := waitRTCReferencedMember(ctx, time.Millisecond, "A", func() (rtcMemberRecord, bool) {
		cancel()
		return rtcMemberRecord{deviceID: "A"}, true
	})
	if ok {
		t.Fatal("accepted membership after cancellation")
	}
}

func TestLegacyRTCJoinTimestamp(t *testing.T) {
	old, future, zero := uint64(10), uint64(30), uint64(0)
	for _, test := range []struct {
		server  int64
		created *uint64
		want    uint64
	}{
		{20, &old, 10}, {25, &old, 10}, {20, &future, 20},
		{20, nil, 0}, {20, &zero, 0}, {-1, &old, 0},
	} {
		if got := legacyRTCJoinTimestamp(test.server, test.created); got != test.want {
			t.Fatalf("got %d, want %d", got, test.want)
		}
	}
}

func TestRTCInvitationIntent(t *testing.T) {
	for _, tc := range []struct {
		top, nested, want string
		ok                bool
	}{
		{"", "", "auto", true}, {"unknown", "", "auto", true},
		{"audio", "", "audio", true}, {"", "video", "video", true},
		{"audio", "audio", "audio", true}, {"video", "audio", "", false},
		{"audio", "video", "", false}, {"unknown", "audio", "audio", true},
	} {
		got, ok := rtcInvitationIntent(tc.top, tc.nested)
		if got != tc.want || ok != tc.ok {
			t.Fatalf("%q/%q: %q %v", tc.top, tc.nested, got, ok)
		}
	}
}

func TestRTCWithdrawalScope(t *testing.T) {
	record := rtcMemberRecord{slotID: "m.call#room", sender: "@alice:example.org", deviceID: "A", memberID: "join", joinTS: 20}
	key := rtcMemberEventKey("!room:example.org", "$member")
	for _, tc := range []struct {
		room, slot, sender, device, member string
		sticky                             bool
		join                               uint64
		want                               bool
	}{
		{"!room:example.org", "m.call#room", "@alice:example.org", "A", "join", false, 20, true},
		{"!room:example.org", "m.call#room", "@alice:example.org", "B", "join", false, 20, false},
		{"!room:example.org", "m.call#room", "@alice:example.org", "A", "join", false, 10, false},
		{"!room:example.org", "m.call#room", "@alice:example.org", "A", "join", false, 0, false},
		{"!room:example.org", "m.call#room", "@alice:example.org", "A", "join", true, 20, false},
		{"!other:example.org", "m.call#room", "@alice:example.org", "A", "join", false, 20, false},
		{"!room:example.org", "m.call#other", "@alice:example.org", "A", "join", false, 20, false},
	} {
		if rtcWithdrawalMatches(key, record, tc.room, tc.slot, tc.sender, tc.device, tc.member, tc.sticky, tc.join) != tc.want {
			t.Fatalf("unexpected withdrawal result: %+v", tc)
		}
	}
	record.sticky = true
	if !rtcWithdrawalMatches(key, record, "!room:example.org", "m.call#room", "@alice:example.org", "A", "join", true, 0) || rtcWithdrawalMatches(key, record, "!room:example.org", "m.call#room", "@alice:example.org", "", "join", true, 0) {
		t.Fatal("incorrect sticky withdrawal scope")
	}
}
