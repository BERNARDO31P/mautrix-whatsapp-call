package connector

import (
	"context"
	"net/http/httptest"
	"strings"
	"testing"
)

func rosterFixture() rtcGroupRosterResult {
	return rtcGroupRosterResult{CallID: "C", Transaction: 2, Revision: 3, Participants: []rtcGroupRosterParticipant{{ID: "2@lid", State: "connected", Devices: []rtcGroupRosterDevice{{ID: "2:2@lid", Camera: "on"}, {ID: "2:1@lid", Microphone: "muted"}}}, {ID: "1@lid", Self: true, State: "connected"}, {ID: "3@lid", State: "ringing", Devices: []rtcGroupRosterDevice{{ID: "3:1@lid", Camera: "on"}}}}}
}
func TestGroupRosterSnapshotExplicitUnknownAndOrdering(t *testing.T) {
	w := httptest.NewRecorder()
	serveRTCGroupRoster(w, httptest.NewRequest("POST", "/roster", strings.NewReader(`{"call_id":"C"}`)), func(ctx context.Context, id string) (rtcGroupRosterResult, error) {
		if _, ok := ctx.Deadline(); !ok || id != "C" {
			t.Fatal("missing request scope")
		}
		return rosterFixture(), nil
	})
	if w.Code != 200 || w.Header().Get("Cache-Control") != "no-store" || !strings.Contains(w.Body.String(), `"media_representation":"aggregate"`) {
		t.Fatal(w.Code, w.Body.String())
	}
	r := rosterFixture()
	if err := validateRTCGroupRoster(&r); err != nil {
		t.Fatal(err)
	}
	if r.Participants[0].ID != "1@lid" || r.Participants[1].Devices[0].ID != "2:1@lid" || r.Participants[1].Devices[0].Camera != "unknown" || r.Participants[1].Devices[1].Microphone != "unknown" || r.Participants[2].Devices[0].Camera != "unknown" {
		t.Fatal(r)
	}
}
func TestGroupRosterRejectsMalformedAndAmbiguous(t *testing.T) {
	for _, body := range []string{`{}`, `{"call_id":"C","raised":false}`, `{"call_id":"C","call_id":"D"}`, `{"call_id":"C"}{}`, `{"call_id":"C/x"}`} {
		w := httptest.NewRecorder()
		serveRTCGroupRoster(w, httptest.NewRequest("POST", "/roster", strings.NewReader(body)), func(context.Context, string) (rtcGroupRosterResult, error) {
			t.Fatal("invalid request executed")
			return rtcGroupRosterResult{}, nil
		})
		if w.Code != 400 {
			t.Fatal(w.Code, body)
		}
	}
	for _, mutate := range []func(*rtcGroupRosterResult){func(r *rtcGroupRosterResult) { r.Transaction = 0 }, func(r *rtcGroupRosterResult) { r.Participants[1].Self = false }, func(r *rtcGroupRosterResult) { r.Participants[0].Devices[1].ID = "2:2@lid" }, func(r *rtcGroupRosterResult) { r.Participants[0].Devices[1].ID = "3:1@lid" }, func(r *rtcGroupRosterResult) { r.Participants[0].Devices[1].Camera = "maybe" }} {
		r := rosterFixture()
		mutate(&r)
		if validateRTCGroupRoster(&r) == nil {
			t.Fatal("invalid roster accepted", r)
		}
	}
}
