package connector

import (
	"context"
	"errors"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestRTCWaitingRoomRejectsInvalidActions(t *testing.T) {
	for _, body := range []string{`{}`, `{"room_id":"!r:x","action":"snapshot","generation":0}`, `{"room_id":"!r:x","action":"approval","enabled":false}`, `{"room_id":"!r:x","action":"approval","generation":1,"enabled":null}`, `{"room_id":"!r:x","action":"deny","generation":1,"target":"3:1@lid"}`, `{"room_id":"!r:x","action":"admit","generation":1,"target":"2@lid","enabled":false}`, `{"room_id":"!r:x","action":"snapshot","action":"deny"}`, `{"room_id":"!r:x","action":"snapshot","token":"secret"}`, `{"room_id":"!r:x","action":"snapshot"} {}`} {
		w := httptest.NewRecorder()
		serveRTCWaitingRoom(w, httptest.NewRequest("POST", "/waiting", strings.NewReader(body)), func(context.Context, rtcWaitingRoomRequest) (rtcWaitingRoomResult, error) {
			t.Fatal("invalid request reached provider")
			return rtcWaitingRoomResult{}, nil
		})
		if w.Code != 400 || w.Header().Get("Cache-Control") != "no-store" {
			t.Fatal(w.Code, body)
		}
	}
}

func TestRTCWaitingRoomActionsAndSubmissionOutcome(t *testing.T) {
	for _, body := range []string{`{"room_id":"!r:x","action":"snapshot"}`, `{"room_id":"!r:x","action":"approval","generation":3,"enabled":false}`, `{"room_id":"!r:x","action":"admit","generation":3,"target":"22@lid"}`, `{"room_id":"!r:x","action":"deny","generation":3,"target":"22@s.whatsapp.net"}`} {
		calls := 0
		w := httptest.NewRecorder()
		serveRTCWaitingRoom(w, httptest.NewRequest("POST", "/waiting", strings.NewReader(body)), func(ctx context.Context, req rtcWaitingRoomRequest) (rtcWaitingRoomResult, error) {
			calls++
			if req.RoomID != "!r:x" {
				t.Fatal(req)
			}
			if _, ok := ctx.Deadline(); !ok {
				t.Fatal("missing deadline")
			}
			outcome := "snapshot"
			if req.Action != "snapshot" {
				if req.Generation != 3 {
					t.Fatal(req)
				}
				outcome = "submitted"
			}
			if req.Action == "approval" && (req.Enabled == nil || *req.Enabled) {
				t.Fatal(req)
			}
			return rtcWaitingRoomResult{RoomID: req.RoomID, Generation: 3, Pending: []string{}, Outcome: outcome}, nil
		})
		if calls != 1 || w.Code != 200 || strings.Contains(w.Body.String(), "confirmed") {
			t.Fatal(calls, w.Code, w.Body.String())
		}
	}
}

func TestRTCWaitingRoomNoRetryOrProviderErrorLeak(t *testing.T) {
	for _, tc := range []struct {
		err    error
		status int
	}{{errRTCGroupUnknown, 504}, {errRTCGroupNotAllowed, 409}, {errRTCCallLinkUnavailable, 503}, {errRTCCallLinkBusy, 409}, {errors.New("secret-token"), 502}} {
		calls := 0
		w := httptest.NewRecorder()
		serveRTCWaitingRoom(w, httptest.NewRequest("POST", "/waiting", strings.NewReader(`{"room_id":"!r:x","action":"deny","generation":1,"target":"22@lid"}`)), func(context.Context, rtcWaitingRoomRequest) (rtcWaitingRoomResult, error) {
			calls++
			return rtcWaitingRoomResult{}, tc.err
		})
		if calls != 1 || w.Code != tc.status || strings.Contains(w.Body.String(), "secret-token") {
			t.Fatal(calls, w.Code, w.Body.String())
		}
	}
}
