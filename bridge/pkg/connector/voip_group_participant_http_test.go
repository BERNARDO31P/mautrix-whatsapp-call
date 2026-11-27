package connector

import (
	"context"
	"errors"
	"net/http/httptest"
	"strings"
	"testing"
)

const groupParticipantBody = `{"call_id":"CALL123","target":"123456@lid","action":"ring"}`

func TestRTCGroupParticipantStrictRequest(t *testing.T) {
	for _, raw := range []string{`{}`, `null`, `{"call_id":"a","target":"123@lid","action":"ring","action":"invite"}`, `{"call_id":"a","target":"123:2@lid","action":"ring"}`, `{"call_id":"a","target":"123@g.us","action":"ring"}`, `{"call_id":"a","target":"123@lid","action":"auto"}`, `{"call_id":"a","target":"123@lid","action":null}`, `{"call_id":"a","target":"123@lid","action":"ring","extra":""}`, groupParticipantBody + ` {}`, strings.Repeat("x", 3000)} {
		w := httptest.NewRecorder()
		serveRTCGroupParticipant(w, httptest.NewRequest("POST", "/participant", strings.NewReader(raw)), func(context.Context, rtcGroupParticipantRequest) error {
			t.Fatal("invalid request invoked operation")
			return nil
		})
		if w.Code != 400 {
			t.Errorf("got %d for %q", w.Code, raw)
		}
	}
}
func TestRTCGroupParticipantOutcome(t *testing.T) {
	for _, test := range []struct {
		err  error
		want int
		body string
	}{{nil, 200, `"outcome":"acknowledged"`}, {errRTCGroupNotAllowed, 409, "not allowed"}, {errRTCGroupBusy, 409, "already in progress"}, {errRTCGroupUnavailable, 503, "unavailable"}, {errRTCGroupRejected, 502, "rejected"}, {errRTCGroupUnknown, 504, "outcome unknown"}, {errors.New("private provider detail"), 502, "failed"}, {context.DeadlineExceeded, 504, "outcome unknown"}} {
		w := httptest.NewRecorder()
		calls := 0
		serveRTCGroupParticipant(w, httptest.NewRequest("POST", "/participant", strings.NewReader(groupParticipantBody)), func(ctx context.Context, req rtcGroupParticipantRequest) error {
			calls++
			if req.CallID != "CALL123" || req.Target != "123456@lid" || req.Action != "ring" {
				t.Fatal(req)
			}
			if _, ok := ctx.Deadline(); !ok {
				t.Fatal("missing deadline")
			}
			return test.err
		})
		if calls != 1 || w.Code != test.want || !strings.Contains(w.Body.String(), test.body) || strings.Contains(w.Body.String(), "private provider detail") || w.Header().Get("Cache-Control") != "no-store" {
			t.Fatal(calls, w.Code, w.Body.String())
		}
	}
}
func TestRTCGroupParticipantCancelled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	w := httptest.NewRecorder()
	serveRTCGroupParticipant(w, httptest.NewRequest("POST", "/participant", strings.NewReader(groupParticipantBody)).WithContext(ctx), func(context.Context, rtcGroupParticipantRequest) error {
		t.Fatal("cancelled request invoked operation")
		return nil
	})
	if w.Code != 504 {
		t.Fatal(w.Code)
	}
}
func TestRTCGroupParticipantInviteAction(t *testing.T) {
	w := httptest.NewRecorder()
	serveRTCGroupParticipant(w, httptest.NewRequest("POST", "/participant", strings.NewReader(`{"call_id":"CALL123","target":"123456@s.whatsapp.net","action":"invite"}`)), func(_ context.Context, r rtcGroupParticipantRequest) error {
		if r.Action != "invite" {
			t.Fatal(r)
		}
		return nil
	})
	if w.Code != 200 || !strings.Contains(w.Body.String(), `"action":"invite"`) {
		t.Fatal(w.Code, w.Body.String())
	}
}
