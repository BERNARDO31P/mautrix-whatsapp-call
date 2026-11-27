package connector

import (
	"context"
	"errors"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestRTCGroupHandStrictRequest(t *testing.T) {
	for _, body := range []string{`{}`, `null`, `{"call_id":"a","raised":null}`, `{"call_id":"a","raised":"false"}`, `{"call_id":"a","raised":true,"raised":false}`, `{"call_id":"a","participant_id":"123@lid","raised":true}`, `{"call_id":"a","call_id":"b"}`, `{"call_id":"a/b"}`, `{"call_id":"a"}{}`, strings.Repeat("x", 2000)} {
		w := httptest.NewRecorder()
		serveRTCGroupHand(w, httptest.NewRequest("POST", "/hand", strings.NewReader(body)), func(context.Context, rtcGroupHandRequest) (rtcGroupHandResult, error) {
			t.Fatal("invalid request executed")
			return rtcGroupHandResult{}, nil
		})
		if w.Code != 400 {
			t.Fatalf("%d: %q", w.Code, body)
		}
	}
}
func TestRTCGroupHandSnapshotAndExplicitLower(t *testing.T) {
	for _, tc := range []struct {
		body   string
		raised *bool
	}{{`{"call_id":"C"}`, nil}, {`{"call_id":"C","raised":false}`, new(bool)}, {`{"call_id":"C","raised":true}`, func() *bool { v := true; return &v }()}} {
		w := httptest.NewRecorder()
		calls := 0
		serveRTCGroupHand(w, httptest.NewRequest("POST", "/hand", strings.NewReader(tc.body)), func(ctx context.Context, r rtcGroupHandRequest) (rtcGroupHandResult, error) {
			calls++
			if r.CallID != "C" || (r.Raised == nil) != (tc.raised == nil) || (r.Raised != nil && *r.Raised != *tc.raised) {
				t.Fatal(r)
			}
			if _, ok := ctx.Deadline(); !ok {
				t.Fatal("missing deadline")
			}
			return rtcGroupHandSnapshot("C", 2, 1, []rtcGroupHandParticipant{{ParticipantID: "2@lid", Raised: true}, {ParticipantID: "1@lid", Self: true}})
		})
		if w.Code != 200 || calls != 1 || !strings.Contains(w.Body.String(), `"roster_transaction":2`) || w.Header().Get("Cache-Control") != "no-store" {
			t.Fatal(w.Code, w.Body.String())
		}
	}
}
func TestRTCGroupHandRosterValidationAndReplacement(t *testing.T) {
	own := rtcGroupHandParticipant{ParticipantID: "1@lid", Self: true}
	remote := rtcGroupHandParticipant{ParticipantID: "2@lid", Raised: true}
	for _, rows := range [][]rtcGroupHandParticipant{nil, {remote}, {own, own}, {own, {ParticipantID: "3@lid", Self: true}}, {own, {ParticipantID: "2:3@lid"}}, {own, remote, remote}} {
		if _, err := rtcGroupHandSnapshot("C", 1, 1, rows); !errors.Is(err, errRTCGroupNotAllowed) {
			t.Fatalf("accepted %+v", rows)
		}
	}
	if _, err := rtcGroupHandSnapshot("C", 0, 1, []rtcGroupHandParticipant{own}); err == nil {
		t.Fatal("accepted seed")
	}
	a, err := rtcGroupHandSnapshot("C", 1, 1, []rtcGroupHandParticipant{remote, own})
	if err != nil || a.Participants[0] != own || a.Participants[1] != remote {
		t.Fatal(a, err)
	}
	b, err := rtcGroupHandSnapshot("C", 2, 1, []rtcGroupHandParticipant{own})
	if err != nil || len(b.Participants) != 1 || b.Participants[0] != own {
		t.Fatal(b, err)
	}
}
func TestRTCGroupHandUnknownAndCancelled(t *testing.T) {
	for _, tc := range []struct {
		err  error
		code int
	}{{errRTCGroupUnknown, 504}, {errRTCGroupUnavailable, 503}, {errRTCGroupBusy, 409}, {errRTCGroupNotAllowed, 409}, {errors.New("private detail"), 502}} {
		w := httptest.NewRecorder()
		serveRTCGroupHand(w, httptest.NewRequest("POST", "/hand", strings.NewReader(`{"call_id":"C","raised":true}`)), func(context.Context, rtcGroupHandRequest) (rtcGroupHandResult, error) {
			return rtcGroupHandResult{}, tc.err
		})
		if w.Code != tc.code || strings.Contains(w.Body.String(), "private detail") {
			t.Fatal(w.Code, w.Body.String())
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	w := httptest.NewRecorder()
	serveRTCGroupHand(w, httptest.NewRequest("POST", "/hand", strings.NewReader(`{"call_id":"C"}`)).WithContext(ctx), func(context.Context, rtcGroupHandRequest) (rtcGroupHandResult, error) {
		t.Fatal("cancelled request executed")
		return rtcGroupHandResult{}, nil
	})
	if w.Code != 504 {
		t.Fatal(w.Code)
	}
}
