package connector

import (
	"context"
	"fmt"
	"net"
	"testing"
	"time"

	waBinary "go.mau.fi/whatsmeow/binary"
	"go.mau.fi/whatsmeow/types"
	"maunium.net/go/mautrix/appservice"
	"maunium.net/go/mautrix/event"
	"maunium.net/go/mautrix/id"

	"go.mau.fi/mautrix-whatsapp/pkg/connector/voipbridge"
)

// Validate the same dispatch map used by the pinned appservice transaction
// processor; calling our handlers directly would miss the Type.Class defect.
func TestRTCEventProcessorDispatchClasses(t *testing.T) {
	cases := []struct {
		name  string
		class event.TypeClass
	}{
		{rtcMemberStable, event.MessageEventType},
		{rtcMemberStable, event.StateEventType},
		{rtcMemberWire, event.MessageEventType},
		{rtcMemberWire, event.StateEventType},
		{rtcMemberLegacy, event.StateEventType},
		{rtcInviteStable, event.MessageEventType},
		{rtcNotificationStable, event.MessageEventType},
		{rtcInviteWire, event.MessageEventType},
		{rtcDeclineStable, event.MessageEventType},
		{rtcDeclineWire, event.MessageEventType},
	}
	for _, tc := range cases {
		t.Run(fmt.Sprintf("%s/%s", tc.name, tc.class.Name()), func(t *testing.T) {
			processor := appservice.NewEventProcessor(nil)
			processor.ExecMode = appservice.Sync
			processor.ExecSyncWarnTime = 0
			processor.ExecSyncTimeout = 0
			calls := 0
			registered := rtcEventType(tc.name, tc.class)
			if registered.Class != tc.class || registered.Type != tc.name {
				t.Fatal("incorrect registration type/class")
			}
			processor.On(registered, func(_ context.Context, _ *event.Event) { calls++ })
			processor.Dispatch(context.Background(), &event.Event{Type: event.Type{Type: tc.name, Class: tc.class}})
			if calls != 1 {
				t.Fatalf("expected one callback, got %d", calls)
			}
			// Decryption preserves the event-envelope class, so also verify
			// that timeline events do not require UnknownEventType.
			processor.Dispatch(context.Background(), &event.Event{Type: event.Type{Type: tc.name, Class: event.UnknownEventType}})
			if calls != 1 {
				t.Fatalf("dispatched unknown class: %d", calls)
			}
		})
	}
}

func TestMatrixRTCDMPowerLevels(t *testing.T) {
	existing := event.Type{Type: "m.room.name", Class: event.StateEventType}
	events := matrixRTCDMPowerLevels(map[event.Type]int{existing: 0})

	for _, eventType := range []event.Type{
		rtcEventType(rtcSlotStable, event.StateEventType),
		rtcEventType(rtcSlotWire, event.StateEventType),
		rtcEventType(rtcMemberStable, event.StateEventType),
		rtcEventType(rtcMemberWire, event.StateEventType),
		rtcEventType(rtcMemberLegacy, event.StateEventType),
		rtcEventType(rtcMemberLegacyAlias, event.StateEventType),
		rtcEventType(rtcInviteStable, event.MessageEventType),
		rtcEventType(rtcNotificationStable, event.MessageEventType),
		rtcEventType(rtcInviteWire, event.MessageEventType),
		rtcEventType(rtcDeclineStable, event.MessageEventType),
		rtcEventType(rtcDeclineWire, event.MessageEventType),
	} {
		if got, ok := events[eventType]; !ok || got != 0 {
			t.Fatalf("RTC DM event %s missing power-level override: %v %v", eventType.Type, got, ok)
		}
	}
	if got, ok := events[existing]; !ok || got != 0 {
		t.Fatalf("existing power-level override was not preserved: %v %v", got, ok)
	}

	selfEvents := matrixRTCDMPowerLevelsForPeer(true, map[event.Type]int{existing: 0})
	for _, eventType := range []event.Type{
		rtcEventType(rtcSlotStable, event.StateEventType),
		rtcEventType(rtcSlotWire, event.StateEventType),
		rtcEventType(rtcMemberStable, event.StateEventType),
		rtcEventType(rtcMemberWire, event.StateEventType),
		rtcEventType(rtcMemberLegacy, event.StateEventType),
		rtcEventType(rtcMemberLegacyAlias, event.StateEventType),
		rtcEventType(rtcInviteStable, event.MessageEventType),
		rtcEventType(rtcNotificationStable, event.MessageEventType),
		rtcEventType(rtcInviteWire, event.MessageEventType),
		rtcEventType(rtcDeclineStable, event.MessageEventType),
		rtcEventType(rtcDeclineWire, event.MessageEventType),
	} {
		if got := selfEvents[eventType]; got != 100 {
			t.Fatalf("Note-to-Self RTC event %s power level = %d, want 100", eventType.Type, got)
		}
	}
}

func TestRTCRingLifetimeAfterAcceptAndCancellation(t *testing.T) {
	parent, parentCancel := context.WithCancel(context.Background())
	defer parentCancel()
	ringEnd := time.Now().Add(65 * time.Millisecond)
	ctx, stop, accept := newRTCRingLifetime(parent, ringEnd)
	defer stop()
	worker, bridge := net.Pipe()
	defer worker.Close()
	defer bridge.Close()
	closeOnEnd := context.AfterFunc(ctx, func() { _ = bridge.Close() })
	defer closeOnEnd()
	accept()
	time.Sleep(time.Until(ringEnd) + 25*time.Millisecond)
	if err := ctx.Err(); err != nil {
		t.Fatalf("answered call expired with ring deadline: %v", err)
	}
	go func() { _, _ = worker.Write([]byte("media after original ring expiry")) }()
	buf := make([]byte, 64)
	_ = bridge.SetReadDeadline(time.Now().Add(time.Second))
	n, err := bridge.Read(buf)
	if err != nil || string(buf[:n]) != "media after original ring expiry" {
		t.Fatalf("no media after original ring expiry: %q %v", buf[:n], err)
	}
	parentCancel()
	select {
	case <-ctx.Done():
	case <-time.After(time.Second):
		t.Fatal("parent shutdown not propagated")
	}
}

func TestRTCUnansweredRingExpires(t *testing.T) {
	ctx, stop, _ := newRTCRingLifetime(context.Background(), time.Now().Add(20*time.Millisecond))
	defer stop()
	select {
	case <-ctx.Done():
	case <-time.After(time.Second):
		t.Fatal("unanswered ring survived deadline")
	}
}

func TestWhatsAppEndReasonFormatting(t *testing.T) {
	cases := map[string]string{
		"server:406":          "WhatsApp rejected this destination (server 406)",
		"remote-rejected":     "WhatsApp peer rejected the call",
		"remote-hangup":       "WhatsApp peer ended the call",
		"whatsapp-disconnect": "WhatsApp disconnected while placing the call",
	}
	for raw, want := range cases {
		if got := formatWhatsAppEndReason(raw); got != want {
			t.Fatalf("formatWhatsAppEndReason(%q) = %q, want %q", raw, got, want)
		}
	}
}

func TestVoIPManagerRefinesEndReason(t *testing.T) {
	manager := voipbridge.NewManager()
	id := voipbridge.ID{LoginID: "login", CallID: "call"}
	if _, err := manager.Start(id, "123@lid", voipbridge.Outgoing); err != nil {
		t.Fatalf("start: %v", err)
	}
	if _, err := manager.End(id, "whatsapp-ended"); err != nil {
		t.Fatalf("first end: %v", err)
	}
	if _, err := manager.End(id, "server:406"); err != nil {
		t.Fatalf("refine end: %v", err)
	}
	session, ok := manager.Get(id)
	if !ok || session.EndReason != "server:406" {
		t.Fatalf("end reason = %q, want server:406", session.EndReason)
	}
}

func TestWhatsAppBusinessCallOptionClassification(t *testing.T) {
	cases := []struct {
		name    string
		options map[string]string
		want    matrixRTCCallAvailability
	}{
		{
			name:    "business calling attribute disabled",
			options: map[string]string{"business_calling_enabled": "false"},
			want:    matrixRTCCallAvailabilityDisabled,
		},
		{
			name:    "business calling attribute enabled",
			options: map[string]string{"business_calling_enabled": "true"},
			want:    matrixRTCCallAvailabilityEnabled,
		},
		{
			name:    "calling disabled",
			options: map[string]string{"calling_status": "DISABLED"},
			want:    matrixRTCCallAvailabilityDisabled,
		},
		{
			name:    "calling enabled",
			options: map[string]string{"calling_status": "ENABLED"},
			want:    matrixRTCCallAvailabilityEnabled,
		},
		{
			name:    "chat icon hidden",
			options: map[string]string{"calling_status": "ENABLED", "call_icon_visibility": "HIDE_IN_CHAT"},
			want:    matrixRTCCallAvailabilityHidden,
		},
		{
			name:    "all icons disabled",
			options: map[string]string{"calling_status": "ENABLED", "call_icon_visibility": "DISABLE_ALL"},
			want:    matrixRTCCallAvailabilityHidden,
		},
		{
			name:    "nested settings",
			options: map[string]string{"calling": `{"status":"DISABLED","call_icon_visibility":"DEFAULT"}`},
			want:    matrixRTCCallAvailabilityDisabled,
		},
		{
			name:    "unrelated business options",
			options: map[string]string{"commerce": "enabled"},
			want:    matrixRTCCallAvailabilityUnknown,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, _ := classifyWhatsAppBusinessCallOptions(tc.options)
			if got != tc.want {
				t.Fatalf("availability = %d, want %d", got, tc.want)
			}
		})
	}
}

func matrixRTCTestBusinessProfileResponse(callingChildren []waBinary.Node) *waBinary.Node {
	return &waBinary.Node{
		Tag: "iq",
		Content: []waBinary.Node{{
			Tag: "business_profile",
			Content: []waBinary.Node{{
				Tag: "profile",
				Content: []waBinary.Node{{
					Tag: "profile_options",
					Content: []waBinary.Node{{
						Tag:     "calling",
						Content: callingChildren,
					}},
				}},
			}},
		}},
	}
}

func matrixRTCTestBusinessProfileOptionsResponse(profileOptions []waBinary.Node) *waBinary.Node {
	return &waBinary.Node{
		Tag: "iq",
		Content: []waBinary.Node{{
			Tag: "business_profile",
			Content: []waBinary.Node{{
				Tag: "profile",
				Content: []waBinary.Node{{
					Tag:     "profile_options",
					Content: profileOptions,
				}},
			}},
		}},
	}
}

func TestWhatsAppBusinessCallProfileResponseClassification(t *testing.T) {
	cases := []struct {
		name string
		resp *waBinary.Node
		want matrixRTCCallAvailability
	}{
		{
			name: "explicit business_calling false attribute",
			resp: matrixRTCTestBusinessProfileOptionsResponse([]waBinary.Node{{
				Tag:   "business_calling",
				Attrs: waBinary.Attrs{"enabled": "false"},
			}}),
			want: matrixRTCCallAvailabilityDisabled,
		},
		{
			name: "explicit business_calling true attribute",
			resp: matrixRTCTestBusinessProfileOptionsResponse([]waBinary.Node{{
				Tag:   "business_calling",
				Attrs: waBinary.Attrs{"enabled": "true"},
			}}),
			want: matrixRTCCallAvailabilityEnabled,
		},
		{
			name: "business calling disabled",
			resp: matrixRTCTestBusinessProfileResponse([]waBinary.Node{
				{Tag: "status", Content: []byte("DISABLED")},
				{Tag: "call_icon_visibility", Content: []byte("DEFAULT")},
			}),
			want: matrixRTCCallAvailabilityDisabled,
		},
		{
			name: "business chat call icon hidden",
			resp: matrixRTCTestBusinessProfileResponse([]waBinary.Node{
				{Tag: "status", Content: []byte("ENABLED")},
				{Tag: "call_icon_visibility", Content: []byte("HIDE_IN_CHAT")},
			}),
			want: matrixRTCCallAvailabilityHidden,
		},
		{
			name: "business calling enabled",
			resp: matrixRTCTestBusinessProfileResponse([]waBinary.Node{
				{Tag: "status", Content: []byte("ENABLED")},
				{Tag: "call_icon_visibility", Content: []byte("DEFAULT")},
			}),
			want: matrixRTCCallAvailabilityEnabled,
		},
		{
			name: "no profile options stays unknown",
			resp: &waBinary.Node{Tag: "iq"},
			want: matrixRTCCallAvailabilityUnknown,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, _ := classifyWhatsAppBusinessCallProfileResponse(tc.resp)
			if got != tc.want {
				t.Fatalf("availability = %d, want %d", got, tc.want)
			}
		})
	}
}

func TestCombineWhatsAppMatrixRTCCallAvailabilityPrecedence(t *testing.T) {
	cases := []struct {
		name       string
		device     matrixRTCCallDeviceClass
		feature    matrixRTCCallAvailability
		profile    matrixRTCCallAvailability
		want       matrixRTCCallAvailability
		wantSource string
	}{
		{
			name:       "business negative beats consumer fallback",
			device:     matrixRTCCallDeviceClassConsumer,
			feature:    matrixRTCCallAvailabilityUnknown,
			profile:    matrixRTCCallAvailabilityDisabled,
			want:       matrixRTCCallAvailabilityDisabled,
			wantSource: "business",
		},
		{
			name:       "business negative beats feature positive",
			device:     matrixRTCCallDeviceClassConsumer,
			feature:    matrixRTCCallAvailabilityEnabled,
			profile:    matrixRTCCallAvailabilityDisabled,
			want:       matrixRTCCallAvailabilityDisabled,
			wantSource: "business",
		},
		{
			name:       "feature negative beats business positive",
			device:     matrixRTCCallDeviceClassConsumer,
			feature:    matrixRTCCallAvailabilityDisabled,
			profile:    matrixRTCCallAvailabilityEnabled,
			want:       matrixRTCCallAvailabilityDisabled,
			wantSource: "feature",
		},
		{
			name:       "feature timeout leaves consumer callable",
			device:     matrixRTCCallDeviceClassConsumer,
			feature:    matrixRTCCallAvailabilityUnknown,
			profile:    matrixRTCCallAvailabilityUnknown,
			want:       matrixRTCCallAvailabilityEnabled,
			wantSource: "devices:consumer:fallback",
		},
		{
			name:       "hosted unknown fails closed",
			device:     matrixRTCCallDeviceClassHosted,
			feature:    matrixRTCCallAvailabilityUnknown,
			profile:    matrixRTCCallAvailabilityUnknown,
			want:       matrixRTCCallAvailabilityDisabled,
			wantSource: "devices:hosted:calling_not_explicitly_enabled",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, source := combineWhatsAppMatrixRTCCallAvailability(
				tc.device, map[matrixRTCCallDeviceClass]string{
					matrixRTCCallDeviceClassConsumer: "devices:consumer",
					matrixRTCCallDeviceClassHosted:   "devices:hosted",
				}[tc.device],
				tc.feature, "feature", tc.profile, "business",
			)
			if got != tc.want {
				t.Fatalf("availability = %d, want %d", got, tc.want)
			}
			if source != tc.wantSource {
				t.Fatalf("source = %q, want %q", source, tc.wantSource)
			}
		})
	}
}

func matrixRTCTestFeatureResponse(featureChildren []waBinary.Node) *waBinary.Node {
	return &waBinary.Node{
		Tag: "iq",
		Content: []waBinary.Node{{
			Tag: "usync",
			Content: []waBinary.Node{{
				Tag: "list",
				Content: []waBinary.Node{{
					Tag: "user",
					Content: []waBinary.Node{{
						Tag:     "feature",
						Content: featureChildren,
					}},
				}},
			}},
		}},
	}
}

func TestWhatsAppVoIPFeatureResponseClassification(t *testing.T) {
	cases := []struct {
		name string
		resp *waBinary.Node
		want matrixRTCCallAvailability
	}{
		{
			name: "voip feature present",
			resp: matrixRTCTestFeatureResponse([]waBinary.Node{{Tag: "voip"}}),
			want: matrixRTCCallAvailabilityEnabled,
		},
		{
			name: "voip explicitly enabled",
			resp: matrixRTCTestFeatureResponse([]waBinary.Node{{Tag: "voip", Attrs: waBinary.Attrs{"enabled": "true"}}}),
			want: matrixRTCCallAvailabilityEnabled,
		},
		{
			name: "voip explicitly disabled",
			resp: matrixRTCTestFeatureResponse([]waBinary.Node{{Tag: "voip", Attrs: waBinary.Attrs{"enabled": "false"}}}),
			want: matrixRTCCallAvailabilityDisabled,
		},
		{
			name: "valid feature response without voip",
			resp: matrixRTCTestFeatureResponse([]waBinary.Node{{Tag: "encrypt_v2"}}),
			want: matrixRTCCallAvailabilityDisabled,
		},
		{
			name: "feature protocol error stays unknown",
			resp: matrixRTCTestFeatureResponse([]waBinary.Node{{Tag: "error", Attrs: waBinary.Attrs{"code": "429"}}}),
			want: matrixRTCCallAvailabilityUnknown,
		},
		{
			name: "missing usync response stays unknown",
			resp: &waBinary.Node{Tag: "iq"},
			want: matrixRTCCallAvailabilityUnknown,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, _ := classifyWhatsAppVoIPFeatureResponse(tc.resp)
			if got != tc.want {
				t.Fatalf("availability = %d, want %d", got, tc.want)
			}
		})
	}
}

func TestMatrixRTCCallDeviceClassification(t *testing.T) {
	cases := []struct {
		name    string
		devices []types.JID
		want    matrixRTCCallDeviceClass
	}{
		{name: "no devices", want: matrixRTCCallDeviceClassUnavailable},
		{
			name:    "consumer device",
			devices: []types.JID{{User: "1", Server: types.HiddenUserServer, Device: 1}},
			want:    matrixRTCCallDeviceClassConsumer,
		},
		{
			name:    "hosted PN device",
			devices: []types.JID{{User: "1", Server: types.HostedServer, Device: 1}},
			want:    matrixRTCCallDeviceClassHosted,
		},
		{
			name: "hosted LID devices",
			devices: []types.JID{
				{User: "1", Server: types.HostedLIDServer, Device: 1},
				{User: "1", Server: types.HostedLIDServer, Device: 2},
			},
			want: matrixRTCCallDeviceClassHosted,
		},
		{
			name: "coexistence prefers consumer path",
			devices: []types.JID{
				{User: "1", Server: types.HostedLIDServer, Device: 1},
				{User: "1", Server: types.HiddenUserServer, Device: 2},
			},
			want: matrixRTCCallDeviceClassConsumer,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, _ := classifyMatrixRTCCallDevices(tc.devices)
			if got != tc.want {
				t.Fatalf("device class = %d, want %d", got, tc.want)
			}
		})
	}
}

func TestMatrixRTCCallStaticJIDAvailability(t *testing.T) {
	for _, jid := range []types.JID{
		{User: "1", Server: types.BotServer},
		{User: "1", Server: types.MessengerServer},
		{User: "1", Server: types.InteropServer},
	} {
		got, source := matrixRTCCallStaticJIDAvailability(jid)
		if got != matrixRTCCallAvailabilityDisabled {
			t.Fatalf("%s availability = %d, want disabled", jid, got)
		}
		if source == "" {
			t.Fatalf("%s missing disable source", jid)
		}
	}

	got, _ := matrixRTCCallStaticJIDAvailability(types.JID{User: "1", Server: types.DefaultUserServer})
	if got != matrixRTCCallAvailabilityUnknown {
		t.Fatalf("normal user availability = %d, want unknown", got)
	}
}

func TestCanonicalMatrixRTCSlotID(t *testing.T) {
	if got := canonicalMatrixRTCSlotID("m.call#ROOM"); got != "m.call#room" {
		t.Fatalf("legacy room slot canonicalized to %q", got)
	}
	if got := canonicalMatrixRTCSlotID("m.call#customSlot"); got != "m.call#customSlot" {
		t.Fatalf("custom slot was unexpectedly rewritten to %q", got)
	}
	if rtcSessionKey("!room:example.org", "m.call#ROOM") != rtcSessionKey("!room:example.org", "m.call#room") {
		t.Fatal("legacy and canonical room slot IDs produced different session keys")
	}
	if rtcIntentKey("!room:example.org", "m.call#ROOM", "@alice:example.org") != rtcIntentKey("!room:example.org", "m.call#room", "@alice:example.org") {
		t.Fatal("legacy and canonical room slot IDs produced different intent keys")
	}
}

func TestMatrixRTCBridgeBotControlMembership(t *testing.T) {
	bot := id.UserID("@whatsappbot:example.org")
	user := id.UserID("@bernardo:example.org")
	ghost := id.UserID("@whatsapp_123:example.org")
	isGhost := func(userID id.UserID) bool { return userID == ghost }
	if !isMatrixRTCBridgeBotControlMembership([]id.UserID{bot, user}, bot, user, isGhost) {
		t.Fatal("two-member bridge bot control room was not detected")
	}
	if isMatrixRTCBridgeBotControlMembership([]id.UserID{bot, user, ghost}, bot, user, isGhost) {
		t.Fatal("portal/group room was misclassified as bridge bot control room")
	}
	if isMatrixRTCBridgeBotControlMembership([]id.UserID{bot, ghost}, bot, ghost, isGhost) {
		t.Fatal("ghost room was misclassified as bridge bot control room")
	}
}

func TestRTCStricterPermissionsAndTrustedInvitingDevice(t *testing.T) {
	pl := &event.PowerLevelsEventContent{Events: map[string]int{rtcInviteStable: 75}}
	mergeRTCPortalPower(pl, true)
	if pl.Events[rtcInviteStable] != 100 {
		t.Fatal("eligible rules not restricted")
	}
	mergeRTCPortalPower(pl, false)
	if pl.Events[rtcInviteStable] != 75 {
		t.Fatal("administrator rule was lost")
	}
	mergeRTCPortalPower(pl, true)
	pl.Events[rtcInviteStable] = 85
	mergeRTCPortalPower(pl, false)
	if pl.Events[rtcInviteStable] != 85 {
		t.Fatal("concurrent administrator edit lost")
	}
	evt := &event.Event{Sender: "@alice:example.org"}
	evt.Mautrix.TrustState = id.TrustStateCrossSignedTOFU
	evt.Mautrix.TrustSource = &id.Device{UserID: evt.Sender, DeviceID: "PHONE"}
	if rtcTrustedDevice(evt) != "" {
		t.Fatal("plaintext invite admitted")
	}
	evt.Mautrix.WasEncrypted = true
	if rtcTrustedDevice(evt) != "PHONE" {
		t.Fatal("legitimate device missing")
	}
	evt.Mautrix.ForwardedKeys = true
	if rtcTrustedDevice(evt) != "" {
		t.Fatal("forwarded key admitted")
	}
}
