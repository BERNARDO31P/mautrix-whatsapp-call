package connector

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestRTCReactionDecodeAndClear(t *testing.T) {
	f := newRTCReactionFeed()
	if f == nil {
		t.Fatal("no stream")
	}
	for _, tc := range []struct {
		body string
		ok   bool
	}{
		{`{"call_id":"CALL"}`, true},
		{fmt.Sprintf(`{"call_id":"CALL","stream":"%s","emoji":""}`, f.stream), true},
		{fmt.Sprintf(`{"call_id":"CALL","stream":"%s","emoji":"❤️"}`, f.stream), true},
		{`{"call_id":"CALL","emoji":"👍"}`, false},
		{`{"call_id":"CALL","after":2}`, false},
		{`{"call_id":"CALL","stream":"bad","emoji":""}`, false},
		{fmt.Sprintf(`{"call_id":"CALL","stream":"%s","emoji":"ordinary message"}`, f.stream), false},
		{fmt.Sprintf(`{"call_id":"CALL","stream":"%s","emoji":"\ud800"}`, f.stream), false},
		{`{"call_id":"CALL","call_id":"OTHER"}`, false},
		{`{"call_id":"CALL","emoji":null}`, false},
		{`{"call_id":"CALL","after":null}`, false},
		{`{"call_id":"CALL"} {}`, false},
	} {
		r := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(tc.body))
		_, ok := decodeRTCReaction(httptest.NewRecorder(), r)
		if ok != tc.ok {
			t.Fatalf("%s got %v", tc.body, ok)
		}
	}
}
func TestRTCReactionFeedRateClearAndRoster(t *testing.T) {
	f := newRTCReactionFeed()
	now := time.Now()
	req := rtcReactionRequest{CallID: "CALL", Stream: f.stream}
	f.append(now, 1, "100@lid", "👍", false)
	f.append(now, 1, "100@lid", "👎", false)
	f.append(now, 1, "100@lid", "", true)
	f.append(now, 1, "100@lid", "", true)
	f.append(now, 1, "invalid", "👍", false)
	out := f.snapshot(now, 1, req)
	if len(out.Events) != 2 || !out.Events[1].Removed || out.Cursor != 2 {
		t.Fatalf("unexpected feed %+v", out)
	}
	req.After = out.Cursor
	if out = f.snapshot(now, 1, req); len(out.Events) != 0 || out.Reset {
		t.Fatalf("replayed %+v", out)
	}
	req.After = 0
	if out = f.snapshot(now, 2, req); len(out.Events) != 0 || !out.Reset {
		t.Fatalf("stale roster %+v", out)
	}
	f.append(now, 2, "100@lid", "👍", false)
	f.append(now, 1, "200@lid", "👍", false)
	if out = f.snapshot(now, 2, req); len(out.Events) != 1 || out.Roster != 2 {
		t.Fatalf("old roster resurrected %+v", out)
	}
	if out = f.snapshot(now.Add(rtcReactionTTL), 2, req); len(out.Events) != 0 || !out.Reset {
		t.Fatalf("expired %+v", out)
	}
}
func TestRTCReactionBoundedConcurrentFeed(t *testing.T) {
	f := newRTCReactionFeed()
	now := time.Now()
	var wg sync.WaitGroup
	for i := 0; i < 300; i++ {
		wg.Add(1)
		go func(i int) { defer wg.Done(); f.append(now, 1, fmt.Sprintf("%d@lid", i), "👍", false) }(i)
	}
	wg.Wait()
	out := f.snapshot(now, 1, rtcReactionRequest{Stream: f.stream})
	if len(out.Events) != rtcReactionCapacity || !out.Reset || out.Cursor != 256 {
		t.Fatalf("not bounded %+v", out)
	}
	for i := 1; i < len(out.Events); i++ {
		if out.Events[i].Sequence != out.Events[i-1].Sequence+1 {
			t.Fatal("noncontiguous events")
		}
	}
}
func TestRTCReactionSubmissionFence(t *testing.T) {
	f := newRTCReactionFeed()
	now := time.Now()
	if err := f.begin(now, "wrong"); !errors.Is(err, errRTCGroupNotAllowed) {
		t.Fatal(err)
	}
	if err := f.begin(now, f.stream); err != nil {
		t.Fatal(err)
	}
	if err := f.begin(now.Add(2*time.Second), f.stream); !errors.Is(err, errRTCGroupBusy) {
		t.Fatal(err)
	}
	f.finish()
	if err := f.begin(now.Add(time.Millisecond), f.stream); !errors.Is(err, errRTCGroupBusy) {
		t.Fatal(err)
	}
	if err := f.begin(now.Add(time.Second), f.stream); err != nil {
		t.Fatal(err)
	}
	f.finish()
}
func TestRTCReactionHTTPUnknownAndNoEcho(t *testing.T) {
	f := newRTCReactionFeed()
	body := fmt.Sprintf(`{"call_id":"CALL","stream":"%s","emoji":"👍"}`, f.stream)
	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(body))
	serveRTCReaction(w, r, func(_ context.Context, req rtcReactionRequest) (rtcReactionResult, error) {
		out := f.snapshot(time.Now(), 0, req)
		out.Outcome = "submitted"
		return out, nil
	})
	var out rtcReactionResult
	if w.Code != 200 || json.Unmarshal(w.Body.Bytes(), &out) != nil || len(out.Events) != 0 || out.Outcome != "submitted" {
		t.Fatal(w.Body.String())
	}
	w = httptest.NewRecorder()
	r = httptest.NewRequest(http.MethodPost, "/", strings.NewReader(body))
	serveRTCReaction(w, r, func(context.Context, rtcReactionRequest) (rtcReactionResult, error) {
		return rtcReactionResult{}, errRTCGroupUnknown
	})
	if w.Code != 504 || !strings.Contains(w.Body.String(), "no automatic retry") {
		t.Fatal(w.Body.String())
	}
}
