package connector

import (
	"bytes"
	"context"
	"errors"
	"go.mau.fi/mautrix-whatsapp/pkg/connector/voipbridge"
	"go.mau.fi/mautrix-whatsapp/pkg/connector/voipbridge/rtcwire"
	"io"
	"math"
	"net"
	"testing"
	"time"
)

func TestPumpAudioInvalidThenValid(t *testing.T) {
	b := voipbridge.NewMediaBus(4)
	bad := make([]float32, 960)
	bad[0] = float32(math.NaN())
	good := make([]float32, 960)
	good[0] = 0.25
	if b.WriteFrame(bad) != nil || b.WriteFrame(good) != nil {
		t.Fatal("queue")
	}
	a, z := net.Pipe()
	defer a.Close()
	defer z.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go func() {
		defer close(done)
		(&WhatsAppClient{}).pumpWhatsAppAudio(ctx, cancel, rtcwire.NewPeer(a, a), "call", b)
	}()
	z.SetReadDeadline(time.Now().Add(time.Second))
	msg, e := rtcwire.Read(z)
	if e != nil {
		t.Fatal(e)
	}
	samples, e := rtcwire.DecodePCM(msg.Data)
	if e != nil || samples[0] != 0.25 || msg.CallID != "call" {
		t.Fatal("wrong audio")
	}
	if ctx.Err() != nil {
		t.Fatal("call cancelled")
	}
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("pump stuck")
	}
}
func TestPumpVideoResync(t *testing.T) {
	b := voipbridge.NewMediaBus(4)
	b.TryVideo(voipbridge.EncodedVideoFrame{Data: []byte{1}, Generation: 1})
	b.TryVideo(voipbridge.EncodedVideoFrame{Data: []byte{0, 0, 1, 0x41}, Generation: 1})
	a, z := net.Pipe()
	defer a.Close()
	defer z.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go func() {
		defer close(done)
		(&WhatsAppClient{}).pumpWhatsAppVideo(ctx, cancel, rtcwire.NewPeer(a, a), "call", b)
	}()
	deadline := time.After(time.Second)
	for !b.TakeVideoResync() {
		select {
		case <-deadline:
			t.Fatal("no resync")
		case <-time.After(time.Millisecond):
		}
	}
	fresh := []byte{0, 0, 1, 0x65}
	if !b.TryVideo(voipbridge.EncodedVideoFrame{Data: fresh, Generation: 2}) {
		t.Fatal("fresh frame rejected")
	}
	z.SetReadDeadline(time.Now().Add(time.Second))
	msg, e := rtcwire.Read(z)
	if e != nil || msg.Generation != 2 || !bytes.Equal(msg.Data, fresh) {
		t.Fatalf("wrong video: %+v %v", msg, e)
	}
	if ctx.Err() != nil {
		t.Fatal("call cancelled")
	}
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("pump stuck")
	}
}

func TestAwaitRTCRelay(t *testing.T) {
	ready := func() bool { return true }
	notReady := func() bool { return false }
	if err := awaitRTCRelay(context.Background(), time.Second, time.Millisecond, ready, notReady); err != nil {
		t.Fatal(err)
	}
	if err := awaitRTCRelay(context.Background(), time.Second, time.Millisecond, ready, ready); err == nil {
		t.Fatal("ended call accepted as ready")
	}
	if err := awaitRTCRelay(context.Background(), 5*time.Millisecond, time.Millisecond, notReady, notReady); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("unexpected timeout: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := awaitRTCRelay(ctx, time.Second, time.Millisecond, ready, notReady); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancellation ignored: %v", err)
	}
}

func TestCheckRTCEarlyAnswer(t *testing.T) {
	reading := make(chan error, 1)
	if err := checkRTCEarlyAnswer(reading); err != nil {
		t.Fatal(err)
	}
	reading <- nil
	if err := checkRTCEarlyAnswer(reading); err == nil {
		t.Fatal("accepted premature answer")
	}
	failure := errors.New("worker disconnected")
	reading <- failure
	if err := checkRTCEarlyAnswer(reading); !errors.Is(err, failure) {
		t.Fatalf("lost worker failure: %v", err)
	}
}

func TestAwaitRTCRelayCancellationDuringReady(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := awaitRTCRelay(ctx, time.Second, time.Millisecond, func() bool {
		cancel()
		return true
	}, func() bool { return false }); !errors.Is(err, context.Canceled) {
		t.Fatalf("readiness won over cancellation: %v", err)
	}
}

func TestAwaitRTCRelayEndDuringReady(t *testing.T) {
	ended := false
	if err := awaitRTCRelay(context.Background(), time.Second, time.Millisecond, func() bool {
		ended = true
		return true
	}, func() bool { return ended }); err == nil {
		t.Fatal("readiness won over terminal state")
	}
}

func TestOutgoingRelayGateRejectsAnswerQueuedDuringReadiness(t *testing.T) {
	reading := make(chan error, 1)
	err := awaitRTCRelayWithReader(context.Background(), time.Second, time.Millisecond, func() bool {
		reading <- nil
		return true
	}, func() bool { return false }, reading)
	if err == nil {
		t.Fatal("relay readiness accepted a premature worker answer")
	}
}

func TestOutgoingRelayGatePreservesWorkerError(t *testing.T) {
	reading := make(chan error, 1)
	failure := errors.New("worker ended admission")
	err := awaitRTCRelayWithReader(context.Background(), time.Second, time.Millisecond, func() bool {
		reading <- failure
		return false
	}, func() bool { return false }, reading)
	if !errors.Is(err, failure) {
		t.Fatalf("worker failure lost: %v", err)
	}
}

func TestOutgoingRelayGatePreservesWorkerErrorWithCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	reading := make(chan error, 1)
	failure := errors.New("worker ended admission")
	reading <- failure
	cancel()
	err := awaitRTCRelayWithReader(ctx, time.Second, time.Millisecond, func() bool { return false }, func() bool { return false }, reading)
	if !errors.Is(err, failure) || !errors.Is(err, context.Canceled) {
		t.Fatalf("lost worker failure or cancellation: %v", err)
	}
}

func TestRelayGateDeadlinePreservesTerminalOutcome(t *testing.T) {
	if err := rtcRelayGateCancellation(context.Background(), context.DeadlineExceeded, func() bool { return true }, nil); !errors.Is(err, errRTCRelayEnded) {
		t.Fatalf("deadline hid peer termination: %v", err)
	}
	reading := make(chan error, 1)
	failure := errors.New("worker admission revoked")
	reading <- failure
	if err := rtcRelayGateCancellation(context.Background(), context.DeadlineExceeded, func() bool { return false }, reading); !errors.Is(err, failure) || errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("deadline hid worker failure: %v", err)
	}
}

func TestRTCPreAnswerPreservesRevocationAtTimeout(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	reading := make(chan error, 1)
	if err := checkRTCPreAnswer(ctx, reading); err != nil {
		t.Fatal(err)
	}
	failure := errors.New("worker admission revoked")
	reading <- failure
	cancel()
	if err := checkRTCPreAnswer(ctx, reading); !errors.Is(err, failure) || !errors.Is(err, context.Canceled) {
		t.Fatalf("lost pre-answer revocation: %v", err)
	}
}

func TestAwaitRTCMatrixAnswerPreservesTerminalOutcome(t *testing.T) {
	workerErr := errors.New("worker rejected media join")
	for i := 0; i < 100; i++ {
		ctx, cancel := context.WithCancel(context.Background())
		reading := make(chan error, 1)
		reading <- workerErr
		cancel()
		if err := awaitRTCMatrixAnswer(ctx, reading); !errors.Is(err, workerErr) || !errors.Is(err, context.Canceled) {
			t.Fatalf("lost worker error or cancellation: %v", err)
		}
		reading <- nil
		if err := awaitRTCMatrixAnswer(ctx, reading); !errors.Is(err, context.Canceled) {
			t.Fatalf("accepted answer after cancellation: %v", err)
		}
	}
	reading := make(chan error, 1)
	reading <- nil
	if err := awaitRTCMatrixAnswer(context.Background(), reading); err != nil {
		t.Fatalf("rejected valid answer: %v", err)
	}
	close(reading)
	if err := awaitRTCMatrixAnswer(context.Background(), reading); !errors.Is(err, io.EOF) {
		t.Fatalf("closed reader accepted: %v", err)
	}
}

func TestRelayGatePreservesConcurrentWorkerAndPeerTermination(t *testing.T) {
	failure := errors.New("worker revoked call admission")
	for _, duringReady := range []bool{false, true} {
		reading := make(chan error, 1)
		ended := !duringReady
		if ended {
			reading <- failure
		}
		err := awaitRTCRelayWithReader(context.Background(), time.Second, time.Millisecond, func() bool {
			ended = true
			reading <- failure
			return true
		}, func() bool { return ended }, reading)
		if !errors.Is(err, failure) {
			t.Fatalf("worker termination lost (during readiness=%v): %v", duringReady, err)
		}
	}
}
