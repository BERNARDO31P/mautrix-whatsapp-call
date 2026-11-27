package connector

import (
	waBinary "go.mau.fi/whatsmeow/binary"
	"go.mau.fi/whatsmeow/types"
	"testing"
)

func TestMatrixRTCCallPeerDeviceAttribution(t *testing.T) {
	pn := types.NewJID("12345", types.DefaultUserServer)
	lid := types.NewJID("98765", types.HiddenUserServer)
	hosted := types.NewJID(pn.User, types.HostedServer)
	hostedLID := types.NewJID(lid.User, types.HostedLIDServer)
	foreign := types.NewJID("55555", types.DefaultUserServer)
	collision := types.NewJID(pn.User, types.HiddenUserServer)
	cases := []struct {
		name    string
		devices []types.JID
		want    matrixRTCCallDeviceClass
	}{
		{"empty", nil, matrixRTCCallDeviceClassUnavailable},
		{"foreign only", []types.JID{foreign}, matrixRTCCallDeviceClassUnknown},
		{"numeric collision", []types.JID{collision}, matrixRTCCallDeviceClassUnknown},
		{"foreign consumer cannot override hosted", []types.JID{foreign, hosted}, matrixRTCCallDeviceClassHosted},
		{"hosted lid attributed", []types.JID{hostedLID}, matrixRTCCallDeviceClassHosted},
		{"known coexistence", []types.JID{hosted, lid}, matrixRTCCallDeviceClassConsumer},
		{"unsupported domain", []types.JID{types.NewJID(pn.User, types.InteropServer)}, matrixRTCCallDeviceClassUnknown},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got, _ := classifyMatrixRTCCallPeerDevices(tc.devices, []types.JID{pn, lid}); got != tc.want {
				t.Fatalf("got %d want %d", got, tc.want)
			}
		})
	}
}

func TestMatrixRTCFeatureAccountAndConflict(t *testing.T) {
	pn := matrixRTCTestBusinessPeer
	lid := types.NewJID("98765", types.HiddenUserServer)
	foreign := types.NewJID("55555", types.DefaultUserServer)
	enabled := waBinary.Node{Tag: "voip", Attrs: waBinary.Attrs{"enabled": "true"}}
	disabled := waBinary.Node{Tag: "voip", Attrs: waBinary.Attrs{"enabled": "false"}}
	conflict := waBinary.Node{Tag: "voip", Attrs: waBinary.Attrs{"enabled": "true", "supported": "false"}}
	scalarConflict := waBinary.Node{Tag: "voip", Attrs: waBinary.Attrs{"enabled": "true"}, Content: []byte("false")}
	unknown := waBinary.Node{Tag: "voip", Attrs: waBinary.Attrs{"status": "pending"}}
	failure := waBinary.Node{Tag: "error"}
	user := func(jid types.JID, features ...waBinary.Node) waBinary.Node {
		return waBinary.Node{Tag: "user", Attrs: waBinary.Attrs{"jid": jid}, Content: []waBinary.Node{{Tag: "feature", Content: features}}}
	}
	response := func(users ...waBinary.Node) *waBinary.Node {
		return &waBinary.Node{Tag: "iq", Content: []waBinary.Node{{Tag: "usync", Content: []waBinary.Node{{Tag: "list", Content: users}}}}}
	}
	cases := []struct {
		name     string
		response *waBinary.Node
		want     matrixRTCCallAvailability
	}{
		{"foreign positive", response(user(foreign, enabled)), matrixRTCCallAvailabilityUnknown},
		{"foreign negative cannot override", response(user(foreign, disabled), user(pn, enabled)), matrixRTCCallAvailabilityEnabled},
		{"alias negative overrides positive", response(user(pn, enabled), user(lid, disabled)), matrixRTCCallAvailabilityDisabled},
		{"reversed duplicate feature", response(user(pn, disabled, enabled)), matrixRTCCallAvailabilityDisabled},
		{"conflicting attrs", response(user(pn, conflict)), matrixRTCCallAvailabilityDisabled},
		{"conflicting scalar", response(user(pn, scalarConflict)), matrixRTCCallAvailabilityDisabled},
		{"unknown explicit value", response(user(pn, unknown)), matrixRTCCallAvailabilityUnknown},
		{"incomplete alias", response(user(pn, enabled), user(lid, failure)), matrixRTCCallAvailabilityUnknown},
		{"valid negative beats incomplete alias", response(user(pn, failure), user(lid, disabled)), matrixRTCCallAvailabilityDisabled},
		{"missing feature", response(waBinary.Node{Tag: "user", Attrs: waBinary.Attrs{"jid": pn}}), matrixRTCCallAvailabilityUnknown},
		{"wrong identity type", response(waBinary.Node{Tag: "user", Attrs: waBinary.Attrs{"jid": pn.String()}}), matrixRTCCallAvailabilityUnknown},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got, _ := classifyWhatsAppVoIPFeatureResponse(tc.response, pn, lid); got != tc.want {
				t.Fatalf("got %d want %d", got, tc.want)
			}
		})
	}
}

func TestMatrixRTCBusinessAliasAndCoexistencePolicy(t *testing.T) {
	pn := matrixRTCTestBusinessPeer
	lid := types.NewJID("98765", types.HiddenUserServer)
	profile := func(jid types.JID, status string) waBinary.Node {
		return waBinary.Node{Tag: "profile", Attrs: waBinary.Attrs{"jid": jid}, Content: []waBinary.Node{{Tag: "profile_options", Content: []waBinary.Node{{Tag: "calling", Content: []byte(`{"status":"` + status + `"}`)}}}}}
	}
	resp := &waBinary.Node{Tag: "iq", Content: []waBinary.Node{{Tag: "business_profile", Content: []waBinary.Node{profile(pn, "enabled"), profile(lid, "disabled")}}}}
	a, source := classifyWhatsAppBusinessCallProfileResponse(resp, pn, lid)
	if a != matrixRTCCallAvailabilityDisabled {
		t.Fatalf("alias disable lost: %d", a)
	}
	for _, device := range []matrixRTCCallDeviceClass{matrixRTCCallDeviceClassConsumer, matrixRTCCallDeviceClassHosted, matrixRTCCallDeviceClassUnknown} {
		for _, feature := range []matrixRTCCallAvailability{matrixRTCCallAvailabilityEnabled, matrixRTCCallAvailabilityDisabled, matrixRTCCallAvailabilityUnknown, matrixRTCCallAvailabilityHidden} {
			if got, _ := combineWhatsAppMatrixRTCCallAvailability(device, "devices", feature, "feature", a, source); got != matrixRTCCallAvailabilityDisabled {
				t.Fatalf("device %d feature %d permitted: %d", device, feature, got)
			}
		}
	}
	if got, _ := classifyWhatsAppBusinessCallProfileResponse(resp, pn); got != matrixRTCCallAvailabilityEnabled {
		t.Fatalf("unmapped alias influenced profile: %d", got)
	}
	if got, _ := combineWhatsAppMatrixRTCCallAvailability(matrixRTCCallDeviceClassHosted, "devices:hosted", matrixRTCCallAvailabilityUnknown, "", matrixRTCCallAvailabilityEnabled, "business"); got != matrixRTCCallAvailabilityEnabled {
		t.Fatalf("explicit business enable refused: %d", got)
	}
}
