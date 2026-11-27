package connector

import (
	"context"
	"errors"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestRTCVideoDecisionStrictRequests(t *testing.T) {
	for _, body := range []string{`{}`, `{"call_id":"a","action":"accept"}`, `{"call_id":"a","action":"reject","generation":0}`, `{"call_id":"a","action":"accept","generation":null}`, `{"call_id":"a","action":"snapshot","generation":1}`, `{"call_id":"a","action":"manual","action":"accept"}`, `{"call_id":"a","action":"accept","generation":-1}`, `{"call_id":"a","action":"accept","generation":1.1}`, `{"call_id":"a/b","action":"manual"}`, `{"call_id":"a","action":"manual"}{}`} {
		w := httptest.NewRecorder()
		serveRTCVideoDecision(w, httptest.NewRequest("POST", "/", strings.NewReader(body)), func(context.Context, rtcVideoDecisionRequest) (rtcVideoDecisionResult, error) {
			t.Fatal("invalid request executed")
			return rtcVideoDecisionResult{}, nil
		})
		if w.Code != 400 {
			t.Fatal(body, w.Code)
		}
	}
}
func TestRTCVideoDecisionSnapshotAndManualActions(t *testing.T) {
	for _, tc := range []struct {
		body, action string
		generation   uint64
	}{{`{"call_id":"C","action":"snapshot"}`, "snapshot", 0}, {`{"call_id":"C","action":"manual"}`, "manual", 0}, {`{"call_id":"C","action":"accept","generation":7}`, "accept", 7}, {`{"call_id":"C","action":"reject","generation":7}`, "reject", 7}} {
		w := httptest.NewRecorder()
		calls := 0
		serveRTCVideoDecision(w, httptest.NewRequest("POST", "/", strings.NewReader(tc.body)), func(ctx context.Context, r rtcVideoDecisionRequest) (rtcVideoDecisionResult, error) {
			calls++
			if r.CallID != "C" || r.Action != tc.action || r.Generation != tc.generation {
				t.Fatal(r)
			}
			if _, ok := ctx.Deadline(); !ok {
				t.Fatal("missing deadline")
			}
			return rtcVideoDecisionResult{CallID: r.CallID, Manual: true, IncomingGeneration: 7, Outcome: "snapshot"}, nil
		})
		if w.Code != 200 || calls != 1 || w.Header().Get("Cache-Control") != "no-store" || !strings.Contains(w.Body.String(), `"incoming_generation":7`) {
			t.Fatal(w.Code, w.Body.String())
		}
	}
}
func TestRTCVideoDecisionFailureAndCancelled(t *testing.T) {
	for _, tc := range []struct {
		err  error
		code int
	}{{errRTCGroupUnavailable, 503}, {errRTCGroupNotAllowed, 409}, {errRTCGroupUnknown, 504}, {errors.New("private"), 502}} {
		w := httptest.NewRecorder()
		serveRTCVideoDecision(w, httptest.NewRequest("POST", "/", strings.NewReader(`{"call_id":"C","action":"manual"}`)), func(context.Context, rtcVideoDecisionRequest) (rtcVideoDecisionResult, error) {
			return rtcVideoDecisionResult{}, tc.err
		})
		if w.Code != tc.code || strings.Contains(w.Body.String(), "private") {
			t.Fatal(w.Code, w.Body.String())
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	w := httptest.NewRecorder()
	serveRTCVideoDecision(w, httptest.NewRequest("POST", "/", strings.NewReader(`{"call_id":"C","action":"manual"}`)).WithContext(ctx), func(context.Context, rtcVideoDecisionRequest) (rtcVideoDecisionResult, error) {
		t.Fatal("cancelled request executed")
		return rtcVideoDecisionResult{}, nil
	})
	if w.Code != 504 {
		t.Fatal(w.Code)
	}
}
