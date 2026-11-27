package rtcwire

import (
	"errors"
	"reflect"
	"testing"
	"time"
)

func TestVideoForwardCameraGenerations(t *testing.T) {
	var state VideoForwardState
	var trace []string
	request := func() error { trace = append(trace, "request"); return nil }
	orientation := func(int) error { trace = append(trace, "orientation"); return nil }
	send := func() error { trace = append(trace, "send"); return nil }
	frame := func(enabled, idr bool) {
		t.Helper()
		if err := state.Forward(enabled, idr, 0, request, orientation, send); err != nil {
			t.Fatal(err)
		}
	}
	frame(true, false)
	frame(true, false)
	frame(true, true)
	frame(true, false)
	state.Control("matrix_audio_mute")
	state.Control("matrix_video_resync")
	frame(true, false)
	state.Control("matrix_video_stop")
	frame(false, true)
	state.Control("matrix_video_start")
	frame(true, false)
	frame(true, true)
	state.Control("matrix_video_mute")
	state.Control("matrix_video_unmute")
	frame(true, false)
	frame(true, true)
	want := []string{"request", "orientation", "send", "send", "send", "request", "orientation", "send", "request", "orientation", "send"}
	if !reflect.DeepEqual(trace, want) {
		t.Fatalf("got %v want %v", trace, want)
	}
}

func TestVideoForwardFailuresRemainRetryable(t *testing.T) {
	var state VideoForwardState
	failure := errors.New("failure")
	ok := func() error { return nil }
	bad := func() error { return failure }
	orient := func(int) error { return nil }
	if err := state.Forward(true, false, 0, bad, orient, ok); !errors.Is(err, failure) || !state.lastRequest.IsZero() {
		t.Fatal("failed request was committed", err)
	}
	if err := state.Forward(true, false, 0, ok, orient, ok); err != nil || state.lastRequest.IsZero() {
		t.Fatal("request retry failed", err)
	}
	if err := state.Forward(true, true, 0, ok, func(int) error { return failure }, ok); !errors.Is(err, failure) || state.ready || state.orientationSent {
		t.Fatal("failed orientation was committed", err)
	}
	if err := state.Forward(true, true, 0, ok, orient, bad); !errors.Is(err, failure) || state.ready || state.orientationSent || !state.lastRequest.IsZero() {
		t.Fatal("failed send did not reset", err)
	}
	if err := state.Forward(true, true, 0, ok, orient, ok); err != nil || !state.ready {
		t.Fatal("IDR retry failed", err)
	}
	if err := state.Forward(false, true, 0, bad, orient, bad); err != nil || state.ready {
		t.Fatal("disabled camera forwarded", err)
	}
}

func TestVideoForwardRetriesLostKeyframeRequest(t *testing.T) {
	var state VideoForwardState
	now := time.Unix(100, 0)
	var requests, sends, orientations int
	request := func() error { requests++; return nil }
	orient := func(int) error { orientations++; return nil }
	send := func() error { sends++; return nil }
	frame := func(offset time.Duration, enabled, idr bool) {
		t.Helper()
		if err := state.forwardAt(now.Add(offset), enabled, idr, 0, request, orient, send); err != nil {
			t.Fatal(err)
		}
	}
	frame(0, true, false)
	frame(time.Millisecond, true, false)
	frame(videoKeyframeRetryInterval-time.Nanosecond, true, false)
	if requests != 1 || sends != 0 {
		t.Fatalf("early retry or dependent frame forwarded: %d/%d", requests, sends)
	}
	frame(videoKeyframeRetryInterval, true, false)
	frame(videoKeyframeRetryInterval+time.Millisecond, true, false)
	frame(2*videoKeyframeRetryInterval, true, false)
	if requests != 3 || sends != 0 {
		t.Fatalf("lost request did not retry: %d/%d", requests, sends)
	}
	frame(2*videoKeyframeRetryInterval+time.Millisecond, true, true)
	frame(3*videoKeyframeRetryInterval, true, false)
	if requests != 3 || sends != 2 || orientations != 1 {
		t.Fatalf("IDR did not end recovery: %d/%d/%d", requests, sends, orientations)
	}
	state.Control("matrix_video_stop")
	frame(3*videoKeyframeRetryInterval, false, false)
	state.Control("matrix_video_start")
	frame(3*videoKeyframeRetryInterval+time.Millisecond, true, false)
	if requests != 4 || sends != 2 {
		t.Fatalf("new camera did not restart recovery: %d/%d", requests, sends)
	}
}

func TestVideoForwardFailedRetryDoesNotExtendInterval(t *testing.T) {
	var state VideoForwardState
	now := time.Unix(100, 0)
	ok := func() error { return nil }
	failure := errors.New("retry failed")
	if err := state.requestKeyframeAt(now, ok); err != nil {
		t.Fatal(err)
	}
	if err := state.requestKeyframeAt(now.Add(videoKeyframeRetryInterval), func() error { return failure }); !errors.Is(err, failure) {
		t.Fatal(err)
	}
	if !state.lastRequest.Equal(now) {
		t.Fatal("failed retry extended recovery interval")
	}
	calls := 0
	if err := state.requestKeyframeAt(now.Add(videoKeyframeRetryInterval), func() error { calls++; return nil }); err != nil || calls != 1 {
		t.Fatal("retry suppressed", err, calls)
	}
}

func TestVideoForwardSenderDropRequiresAnotherIDR(t *testing.T) {
	var state VideoForwardState
	now := time.Unix(100, 0)
	var requests, sends, orientations int
	request := func() error { requests++; return nil }
	orient := func(int) error { orientations++; return nil }
	send := func() error { sends++; return nil }
	dropped := func() error { sends++; return ErrVideoFrameDropped }
	frame := func(offset time.Duration, idr bool, write func() error) {
		t.Helper()
		if err := state.forwardAt(now.Add(offset), true, idr, 0, request, orient, write); err != nil {
			t.Fatal(err)
		}
	}
	frame(0, true, dropped)
	frame(time.Millisecond, false, send)
	frame(2*time.Millisecond, true, dropped)
	if state.ready || state.orientationSent || requests != 1 || sends != 2 || orientations != 2 {
		t.Fatalf("dropped IDR incorrectly committed: %+v, %d/%d/%d", state, requests, sends, orientations)
	}
	frame(videoKeyframeRetryInterval, false, send)
	frame(videoKeyframeRetryInterval+time.Millisecond, true, send)
	frame(videoKeyframeRetryInterval+2*time.Millisecond, false, send)
	if !state.ready || requests != 2 || sends != 4 || orientations != 3 {
		t.Fatalf("recovery did not commit: %+v, %d/%d/%d", state, requests, sends, orientations)
	}
	frame(videoKeyframeRetryInterval+3*time.Millisecond, false, dropped)
	if state.ready || requests != 3 {
		t.Fatal("dependent-frame drop did not restart recovery")
	}
}

func TestVideoForwardOrientationRaceDropsFrame(t *testing.T) {
	var state VideoForwardState
	requests, sends := 0, 0
	request := func() error { requests++; return nil }
	send := func() error { sends++; return nil }
	if err := state.Forward(true, true, 1, request, func(int) error { return ErrVideoFrameDropped }, send); err != nil || requests != 1 || sends != 0 || state.ready {
		t.Fatal(err, requests, sends, state)
	}
	if err := state.Forward(true, true, 1, request, func(int) error { return nil }, send); err != nil || sends != 1 || !state.ready {
		t.Fatal(err, sends, state)
	}
}
