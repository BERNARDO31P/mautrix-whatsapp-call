package rtcwire

import (
	"context"
	"errors"
	"io"
	"reflect"
	"testing"
	"time"
)

func TestAwaitPickupDrainsMediaAndPreservesControlOrder(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	frames := make(chan Message)
	accepted := make(chan struct{})
	type result struct {
		pending PickupBuffer
		err     error
	}
	finished := make(chan result, 1)
	go func() {
		pending, err := AwaitPickup(ctx, "call", frames, func(ctx context.Context) error {
			select {
			case <-accepted:
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		})
		finished <- result{pending, err}
	}()
	send := func(kind string) {
		t.Helper()
		select {
		case frames <- Message{Type: kind, CallID: "call"}:
		case <-ctx.Done():
			t.Fatal("pickup stopped draining", ctx.Err())
		}
	}
	controls := []string{"matrix_audio_mute", "matrix_video_start", "matrix_video_stop", "matrix_audio_unmute"}
	for _, ctrl := range controls {
		send(ctrl)
		for i := 0; i < 100; i++ {
			send("matrix_audio")
			send("matrix_video")
		}
	}
	close(accepted)
	select {
	case got := <-finished:
		if got.err != nil || !got.pending.DroppedVideo {
			t.Fatalf("pickup: %+v", got)
		}
		var kinds []string
		for _, msg := range got.pending.Controls {
			kinds = append(kinds, msg.Type)
		}
		if !reflect.DeepEqual(kinds, controls) {
			t.Fatalf("controls: %v", kinds)
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
}

func TestAwaitPickupCancelsProviderOnTerminalOrInvalidInput(t *testing.T) {
	cases := []struct {
		name       string
		frames     []Message
		closeInput bool
	}{
		{name: "hangup", frames: []Message{{Type: "hangup", CallID: "call"}}},
		{name: "reject", frames: []Message{{Type: "reject", CallID: "call"}}},
		{name: "worker-error", frames: []Message{{Type: "error", CallID: "call", Reason: "failure"}}},
		{name: "mismatch", frames: []Message{{Type: "matrix_audio", CallID: "different"}}},
		{name: "duplicate-answer", frames: []Message{{Type: "answer", CallID: "call"}}},
		{name: "closed", closeInput: true},
		{name: "overflow", frames: make([]Message, maxPickupControls+1)},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			frames := make(chan Message, len(c.frames))
			for _, msg := range c.frames {
				if msg.Type == "" {
					msg = Message{Type: "matrix_audio_mute", CallID: "call"}
				}
				frames <- msg
			}
			if c.closeInput {
				close(frames)
			}
			ended := make(chan struct{})
			pending, err := AwaitPickup(ctx, "call", frames, func(ctx context.Context) error { defer close(ended); <-ctx.Done(); return ctx.Err() })
			if err == nil || len(pending.Controls) != 0 {
				t.Fatalf("unexpected success: %+v %v", pending, err)
			}
			if c.closeInput && !errors.Is(err, io.EOF) {
				t.Fatalf("wanted EOF: %v", err)
			}
			select {
			case <-ended:
			case <-ctx.Done():
				t.Fatal("provider not canceled")
			}
		})
	}
}

func TestAwaitPickupDeadlineDoesNotWaitForProviderReturn(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	release := make(chan struct{})
	defer close(release)
	_, err := AwaitPickup(ctx, "call", nil, func(context.Context) error { <-release; return nil })
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("wanted deadline: %v", err)
	}
}

func TestAwaitPickupProviderFailureAndPrecancel(t *testing.T) {
	failure := errors.New("provider refused")
	if _, err := AwaitPickup(context.Background(), "call", nil, func(context.Context) error { return failure }); !errors.Is(err, failure) {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := AwaitPickup(ctx, "call", nil, func(context.Context) error { panic("must not start") }); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
}

func TestStreamFramesPreservesTerminalBeforeEOF(t *testing.T) {
	for _, kind := range []string{"reject", "error", "hangup"} {
		t.Run(kind, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			n := 0
			finished := make(chan struct{})
			frames := StreamFrames(ctx, func() (Message, error) {
				n++
				if n == 1 {
					return Message{Type: kind, CallID: "call"}, nil
				}
				close(finished)
				return Message{}, io.EOF
			})
			select {
			case <-finished:
			case <-ctx.Done():
				t.Fatal("reader did not reach EOF")
			}
			msg, ok := <-frames
			if !ok || msg.Type != kind || ctx.Err() != nil {
				t.Fatalf("terminal frame lost: %#v %v %v", msg, ok, ctx.Err())
			}
			if _, ok := <-frames; ok {
				t.Fatal("stream did not close after terminal frame")
			}
		})
	}
}

func TestStreamFramesCancellationUnblocksFullQueue(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	second := make(chan struct{})
	n := 0
	frames := StreamFrames(ctx, func() (Message, error) {
		n++
		if n == 2 {
			close(second)
		}
		return Message{Type: "answer", CallID: "call"}, nil
	})
	select {
	case <-second:
	case <-time.After(time.Second):
		t.Fatal("reader never filled queue")
	}
	cancel()
	deadline := time.After(time.Second)
	for {
		select {
		case _, ok := <-frames:
			if !ok {
				return
			}
		case <-deadline:
			t.Fatal("reader remained blocked after cancellation")
		}
	}
}
