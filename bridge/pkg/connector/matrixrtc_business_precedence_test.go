package connector

import (
	waBinary "go.mau.fi/whatsmeow/binary"
	"go.mau.fi/whatsmeow/types"
	"testing"
)

func TestMatrixRTCBusinessOptionConflicts(t *testing.T) {
	cases := []struct {
		name    string
		options map[string]string
		want    matrixRTCCallAvailability
		source  string
	}{
		{"different status aliases", map[string]string{"business_calling_enabled": "true", "calling_status": "disabled"}, matrixRTCCallAvailabilityDisabled, "business_profile:calling_status=disabled"},
		{"normalized key collision", map[string]string{"calling-status": "false", "calling_status": "true"}, matrixRTCCallAvailabilityDisabled, "business_profile:calling_status=false"},
		{"nested collision", map[string]string{"calling_status": "enabled", "calling": `{"status":"disabled"}`}, matrixRTCCallAvailabilityDisabled, "business_profile:calling_status=disabled"},
		{"unknown alias", map[string]string{"business_calling_enabled": "pending", "calling_status": "enabled"}, matrixRTCCallAvailabilityEnabled, "business_profile:calling_status=enabled"},
		{"visibility alias", map[string]string{"calling_status": "enabled", "call_icon_visibility": "default", "calling_icon_visibility": "hidden"}, matrixRTCCallAvailabilityHidden, "business_profile:calling_icon_visibility=hidden"},
		{"disabled outranks hidden", map[string]string{"calling_status": "false", "call_icon_visibility": "hidden"}, matrixRTCCallAvailabilityDisabled, "business_profile:calling_status=false"},
		{"deterministic diagnostic", map[string]string{"calling_status": "false", "call_enabled": "false"}, matrixRTCCallAvailabilityDisabled, "business_profile:call_enabled=false"},
		{"unknown only", map[string]string{"business_calling_enabled": "pending"}, matrixRTCCallAvailabilityUnknown, ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			for i := 0; i < 100; i++ {
				got, source := classifyWhatsAppBusinessCallOptions(tc.options)
				if got != tc.want || source != tc.source {
					t.Fatalf("got (%d, %q), want (%d, %q)", got, source, tc.want, tc.source)
				}
			}
		})
	}
}

func TestMatrixRTCBusinessRepeatedProfileValues(t *testing.T) {
	enabled := waBinary.Node{Tag: "status", Content: []byte("enabled")}
	disabled := waBinary.Node{Tag: "status", Content: []byte("disabled")}
	for _, children := range [][]waBinary.Node{{enabled, disabled}, {disabled, enabled}} {
		resp := matrixRTCTestBusinessProfileResponse(children)
		if got, _ := classifyWhatsAppBusinessCallProfileResponse(resp, matrixRTCTestBusinessPeer); got != matrixRTCCallAvailabilityDisabled {
			t.Fatalf("repeated status lost disabled value: %d", got)
		}
	}
	resp := matrixRTCTestBusinessProfileOptionsResponse([]waBinary.Node{
		{Tag: "calling", Content: []byte(`{"status":"disabled"}`)},
		{Tag: "calling", Attrs: waBinary.Attrs{"status": "enabled"}},
	})
	if got, _ := classifyWhatsAppBusinessCallProfileResponse(resp, matrixRTCTestBusinessPeer); got != matrixRTCCallAvailabilityDisabled {
		t.Fatalf("JSON profile value lost disabled status: %d", got)
	}
}

func TestMatrixRTCBusinessDuplicateJSONProperties(t *testing.T) {
	cases := []struct {
		name    string
		options map[string]string
		want    matrixRTCCallAvailability
	}{
		{"negative first", map[string]string{"calling": `{"status":"disabled","status":"enabled"}`}, matrixRTCCallAvailabilityDisabled},
		{"negative last", map[string]string{"calling": `{"status":"enabled","status":"disabled"}`}, matrixRTCCallAvailabilityDisabled},
		{"repeated object", map[string]string{"calling": `{"status":{"status":false},"status":{"status":true}}`}, matrixRTCCallAvailabilityDisabled},
		{"hidden first", map[string]string{"calling": `{"status":true,"call_icon_visibility":"hidden","call_icon_visibility":"default"}`}, matrixRTCCallAvailabilityHidden},
		{"escaped property name", map[string]string{"calling": `{"sta\u0074us":false,"status":true}`}, matrixRTCCallAvailabilityDisabled},
		{"numeric values", map[string]string{"calling": `{"status":0,"status":1}`}, matrixRTCCallAvailabilityDisabled},
		{"array ignored", map[string]string{"calling": `{"ignored":[{"status":"disabled"},[0]],"status":"enabled"}`}, matrixRTCCallAvailabilityEnabled},
		{"malformed object ignored", map[string]string{"calling": `{"status":"enabled",`}, matrixRTCCallAvailabilityUnknown},
		{"trailing object ignored", map[string]string{"calling": `{"status":"enabled"}{}`}, matrixRTCCallAvailabilityUnknown},
		{"overflow number discards partial object", map[string]string{"calling": `{"status":"enabled","unknown":1e9999}`}, matrixRTCCallAvailabilityUnknown},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got, _ := classifyWhatsAppBusinessCallOptions(tc.options); got != tc.want {
				t.Fatalf("got %d, want %d", got, tc.want)
			}
		})
	}
}

func TestMatrixRTCBusinessRepeatedProfileContainers(t *testing.T) {
	container := func(value string) waBinary.Node {
		return waBinary.Node{Tag: "profile_options", Content: []waBinary.Node{{Tag: "calling", Content: []byte(value)}}}
	}
	enabled := container(`{"status":"enabled"}`)
	disabled := container(`{"status":"disabled","status":"enabled"}`)
	for _, nodes := range [][]waBinary.Node{{enabled, disabled}, {disabled, enabled}} {
		resp := &waBinary.Node{Tag: "iq", Content: []waBinary.Node{{Tag: "business_profile", Content: []waBinary.Node{{Tag: "profile", Attrs: waBinary.Attrs{"jid": matrixRTCTestBusinessPeer}, Content: nodes}}}}}
		if got, _ := classifyWhatsAppBusinessCallProfileResponse(resp, matrixRTCTestBusinessPeer); got != matrixRTCCallAvailabilityDisabled {
			t.Fatalf("repeated profile containers lost disabled status: %d", got)
		}
	}
}

func TestMatrixRTCBusinessProfileAttribution(t *testing.T) {
	peer := matrixRTCTestBusinessPeer
	other := types.NewJID("67890", types.DefaultUserServer)
	profile := func(jid any, status string) waBinary.Node {
		return waBinary.Node{Tag: "profile", Attrs: waBinary.Attrs{"jid": jid}, Content: []waBinary.Node{{Tag: "profile_options", Content: []waBinary.Node{{Tag: "calling", Content: []byte(`{"status":"` + status + `"}`)}}}}}
	}
	cases := []struct {
		name  string
		nodes []waBinary.Node
		want  matrixRTCCallAvailability
	}{
		{"foreign disable ignored", []waBinary.Node{profile(other, "disabled"), profile(peer, "enabled")}, matrixRTCCallAvailabilityEnabled},
		{"foreign enable ignored", []waBinary.Node{profile(other, "enabled"), profile(peer, "disabled")}, matrixRTCCallAvailabilityDisabled},
		{"foreign only", []waBinary.Node{profile(other, "enabled")}, matrixRTCCallAvailabilityUnknown},
		{"missing jid", []waBinary.Node{profile(nil, "enabled")}, matrixRTCCallAvailabilityUnknown},
		{"wrong attribute type", []waBinary.Node{profile(peer.String(), "enabled")}, matrixRTCCallAvailabilityUnknown},
		{"different server", []waBinary.Node{profile(types.NewJID(peer.User, types.HiddenUserServer), "enabled")}, matrixRTCCallAvailabilityUnknown},
		{"same peer repeats", []waBinary.Node{profile(peer, "disabled"), profile(peer, "enabled")}, matrixRTCCallAvailabilityDisabled},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			resp := &waBinary.Node{Tag: "iq", Content: []waBinary.Node{{Tag: "business_profile", Content: tc.nodes}}}
			if got, _ := classifyWhatsAppBusinessCallProfileResponse(resp, peer); got != tc.want {
				t.Fatalf("got %d, want %d", got, tc.want)
			}
		})
	}
}
