package meowcaller

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestRelayConnectLateResultDisposed(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	started, release, disposed := make(chan struct{}), make(chan struct{}), make(chan int, 1)
	result := make(chan error, 1)
	go func() {
		_, err := awaitRelayConnection(ctx, func() (int, error) {
			close(started)
			<-release
			return 42, nil
		}, func(v int) { disposed <- v })
		result <- err
	}()
	<-started
	cancel()
	if err := <-result; !errors.Is(err, context.Canceled) {
		t.Fatalf("unexpected result: %v", err)
	}
	close(release)
	select {
	case v := <-disposed:
		if v != 42 {
			t.Fatalf("wrong disposed result: %d", v)
		}
	case <-time.After(time.Second):
		t.Fatal("abandoned connection not disposed")
	}
}

func TestRelayConnectTransfersOwnership(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	disposed := make(chan int, 1)
	v, err := awaitRelayConnection(ctx, func() (int, error) { return 42, nil }, func(v int) { disposed <- v })
	if v != 42 || err != nil {
		t.Fatalf("value=%d, err=%v", v, err)
	}
	cancel()
	select {
	case <-disposed:
		t.Fatal("transferred connection disposed by producer")
	default:
	}
}

func TestRelayConnectFailureDisposesPartialResult(t *testing.T) {
	want := errors.New("failed")
	disposed := 0
	v, err := awaitRelayConnection(context.Background(), func() (int, error) { return 42, want }, func(v int) { disposed = v })
	if v != 0 || !errors.Is(err, want) || disposed != 42 {
		t.Fatalf("value=%d, err=%v, disposed=%d", v, err, disposed)
	}
}

func TestRelayConnectAlreadyCancelled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	called := false
	_, err := awaitRelayConnection(ctx, func() (int, error) { called = true; return 42, nil }, func(int) {})
	if called || !errors.Is(err, context.Canceled) {
		t.Fatalf("called=%v, err=%v", called, err)
	}
}

func TestRelayAttemptTimeout(t *testing.T) {
	for _, tc := range []struct {
		count int
		want  time.Duration
	}{
		{0, 12 * time.Second}, {1, 12 * time.Second}, {2, 12 * time.Second}, {3, 8 * time.Second},
	} {
		if got := relayAttemptTimeout(tc.count); got != tc.want {
			t.Errorf("%d candidates: got %s, want %s", tc.count, got, tc.want)
		}
	}
}

func TestDirectRelayPortFallback(t *testing.T) {
	for _, tc := range []struct {
		name             string
		port             uint16
		group, failFirst bool
		wantPort         uint16
		wantCalls        int
		wantErr          bool
	}{
		{"advertised works", 3478, false, false, 3478, 1, false},
		{"alternate works", 3478, false, true, 3480, 2, false},
		{"group unchanged", 3478, true, true, 3478, 1, true},
		{"already web port", 3480, false, true, 3480, 1, true},
		{"other advertised port succeeds", 9999, false, false, 9999, 1, false},
		{"other advertised port falls back", 9999, false, true, 3480, 2, false},
		{"other group port unchanged", 9999, true, true, 9999, 1, true},
		{"invalid zero port rejected", 0, false, false, 0, 0, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			calls := 0
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			v, port, err := dialDirectRelayPorts(ctx, tc.port, tc.group, func(ctx context.Context, port uint16) (int, error) {
				calls++
				if calls == 1 && tc.failFirst {
					return 0, errors.New("dial failed")
				}
				return 42, nil
			})
			if port != tc.wantPort || calls != tc.wantCalls || (err != nil) != tc.wantErr || (err == nil && v != 42) {
				t.Fatalf("port=%d calls=%d value=%d err=%v", port, calls, v, err)
			}
		})
	}
}

func TestDirectRelayFallbackReservesDeadline(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	calls := 0
	_, port, err := dialDirectRelayPorts(ctx, 3478, false, func(ctx context.Context, port uint16) (int, error) {
		calls++
		if calls == 1 {
			<-ctx.Done()
			return 0, ctx.Err()
		}
		return 42, ctx.Err()
	})
	if err != nil || port != 3480 || calls != 2 {
		t.Fatalf("port=%d calls=%d err=%v", port, calls, err)
	}
}

func TestDirectRelayCancellationPreventsFallback(t *testing.T) {
	cause := errors.New("media cancelled by signalling")
	transportErr := errors.New("advertised transport interrupted")
	ctx, cancel := context.WithCancelCause(context.Background())
	defer cancel(nil)
	calls := 0
	_, _, err := dialDirectRelayPorts(ctx, 3478, false, func(ctx context.Context, port uint16) (int, error) {
		calls++
		cancel(cause)
		return 0, transportErr
	})
	if !errors.Is(err, context.Canceled) || !errors.Is(err, cause) || !errors.Is(err, transportErr) || calls != 1 {
		t.Fatalf("calls=%d err=%v", calls, err)
	}
}

func TestDirectRelayFallbackPreservesBothErrors(t *testing.T) {
	first := errors.New("advertised transport failed")
	second := errors.New("web transport failed")
	var ports []uint16
	_, port, err := dialDirectRelayPorts(context.Background(), 9999, false, func(ctx context.Context, port uint16) (int, error) {
		ports = append(ports, port)
		if port == 9999 {
			return 0, first
		}
		return 0, second
	})
	if port != 3480 || len(ports) != 2 || ports[0] != 9999 || ports[1] != 3480 || !errors.Is(err, first) || !errors.Is(err, second) {
		t.Fatalf("port=%d attempts=%v err=%v", port, ports, err)
	}
}

func TestAwaitRelayPackets(t *testing.T) {
	watch := func(ctx context.Context) func() bool { return func() bool { return ctx.Err() == nil } }
	reject := errors.New("allocation rejected")
	ready := func(p []byte) (bool, error) {
		if p[0] == 2 {
			return false, reject
		}
		return p[0] == 3, nil
	}
	for _, tc := range []struct {
		name    string
		input   []byte
		wantErr error
	}{
		{"preserves preceding pong", []byte{1, 3}, nil},
		{"pong cannot hide rejection", []byte{1, 2}, reject},
	} {
		t.Run(tc.name, func(t *testing.T) {
			i := 0
			packets, err := awaitRelayPackets(context.Background(), func(buf []byte) (int, error) {
				buf[0] = tc.input[i]
				i++
				return 1, nil
			}, watch, ready)
			if !errors.Is(err, tc.wantErr) || i != len(tc.input) {
				t.Fatalf("reads=%d err=%v", i, err)
			}
			if err == nil && (len(packets) != 2 || packets[0][0] != 1 || packets[1][0] != 3) {
				t.Fatalf("packets=%v", packets)
			}
		})
	}
	t.Run("silent attempt can fall back", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
		defer cancel()
		var attempts []uint16
		packets, port, err := dialDirectRelayPorts(ctx, 3478, false, func(attempt context.Context, port uint16) ([][]byte, error) {
			attempts = append(attempts, port)
			return awaitRelayPackets(attempt, func(buf []byte) (int, error) {
				if port == 3478 {
					<-attempt.Done()
					return 0, attempt.Err()
				}
				buf[0] = 3
				return 1, nil
			}, watch, ready)
		})
		if err != nil || port != 3480 || len(attempts) != 2 || len(packets) != 1 || packets[0][0] != 3 {
			t.Fatalf("attempts=%v port=%d packets=%v err=%v", attempts, port, packets, err)
		}
	})
	t.Run("bounded unrelated packets", func(t *testing.T) {
		reads := 0
		_, err := awaitRelayPackets(context.Background(), func(buf []byte) (int, error) { reads++; buf[0] = 1; return 1, nil }, watch, ready)
		if err == nil || reads != 32 {
			t.Fatalf("reads=%d err=%v", reads, err)
		}
	})
	t.Run("cancelled call does not read", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		_, err := awaitRelayPackets(ctx, func([]byte) (int, error) { t.Fatal("read after cancellation"); return 0, nil }, watch, ready)
		if !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
	})
}

func TestRelayCandidateFailuresAndCancellation(t *testing.T) {
	first := errors.New("DTLS failure")
	second := errors.New("allocation failure")
	for _, cancelAt := range []int{0, 1, 2} {
		ctx, cancel := context.WithCancel(context.Background())
		calls, reported := 0, 0
		_, err := tryRelayCandidates(ctx, []int{1, 2}, func(candidate int, timeout time.Duration) (int, error) {
			calls++
			if timeout != 12*time.Second {
				t.Fatalf("unexpected timeout %v", timeout)
			}
			if candidate == cancelAt {
				cancel()
			}
			if candidate == 1 {
				return 0, first
			}
			return 0, second
		}, func(i, candidate int, err error) {
			reported++
			if i != candidate || err == nil {
				t.Fatalf("report %d %d %v", i, candidate, err)
			}
		}, func(int) { t.Fatal("disposed failed attempt") })
		cancel()
		wantCalls := 2
		if cancelAt == 1 {
			wantCalls = 1
		}
		if calls != wantCalls || reported != wantCalls || !errors.Is(err, first) || errors.Is(err, second) != (wantCalls == 2) || errors.Is(err, context.Canceled) != (cancelAt != 0) {
			t.Fatalf("cancelAt=%d calls=%d reported=%d err=%v", cancelAt, calls, reported, err)
		}
	}
}

func TestRelayCandidateSuccessfulHandoff(t *testing.T) {
	for _, cancelled := range []bool{false, true} {
		cause := errors.New("media owner replaced")
		ctx, cancel := context.WithCancelCause(context.Background())
		calls, disposed := 0, 0
		got, err := tryRelayCandidates(ctx, []int{1, 2}, func(candidate int, _ time.Duration) (int, error) {
			calls++
			if cancelled {
				cancel(cause)
			}
			return 42, nil
		}, func(int, int, error) { t.Fatal("reported successful attempt") }, func(value int) {
			if value != 42 {
				t.Fatalf("disposed %d", value)
			}
			disposed++
		})
		cancel(cause)
		if calls != 1 {
			t.Fatalf("calls=%d", calls)
		}
		if cancelled {
			if got != 0 || !errors.Is(err, context.Canceled) || !errors.Is(err, cause) || disposed != 1 {
				t.Fatalf("cancelled handoff: got=%d err=%v disposed=%d", got, err, disposed)
			}
		} else if got != 42 || err != nil || disposed != 0 {
			t.Fatalf("active handoff: got=%d err=%v disposed=%d", got, err, disposed)
		}
	}
}
