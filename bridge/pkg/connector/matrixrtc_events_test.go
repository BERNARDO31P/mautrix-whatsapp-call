package connector

import (
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
		intent:  "audio",
		expires: time.Now().Add(time.Minute),
	}

	record, ok := wa.matrixRTCMemberForNotification(roomID, eventID, sender)
	if !ok {
		t.Fatal("expected referenced MatrixRTC membership to resolve")
	}
	if record.slotID != "m.call#room" || record.intent != "audio" {
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
		intent:  "video",
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
