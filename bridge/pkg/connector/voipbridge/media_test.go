package voipbridge

import (
	"context"
	"errors"
	"io"
	"math"
	"sync"
	"testing"
	"time"

	"go.mau.fi/mautrix-whatsapp/pkg/connector/voipbridge/rtcwire"
)

func TestMediaBusCopyAndRoundTrip(t *testing.T) {
	bus := NewMediaBus(2)
	defer bus.Close()
	out := make([]float32, AudioFrameSamples)
	out[0] = 0.25
	if err := bus.SendAudioToWhatsApp(out); err != nil {
		t.Fatal(err)
	}
	out[0] = 0.9
	got, err := bus.ReadFrame()
	if err != nil || got[0] != 0.25 {
		t.Fatalf("send audio: %v, %v", got[0], err)
	}

	in := make([]float32, AudioFrameSamples)
	in[0] = -0.3
	if err := bus.WriteFrame(in); err != nil {
		t.Fatal(err)
	}
	in[0] = 0.9
	got, err = bus.NextAudioFromWhatsApp(context.Background())
	if err != nil || got[0] != -0.3 {
		t.Fatalf("received audio: %v, %v", got[0], err)
	}

	video := []byte{0, 0, 0, 1, 0x65}
	bus.SetOrientation(3)
	if err := bus.WriteVideo(video); err != nil {
		t.Fatal(err)
	}
	video[4] = 0
	frame, err := bus.NextVideoFromWhatsApp(context.Background())
	if err != nil || frame.Data[4] != 0x65 || frame.Orientation != 3 {
		t.Fatalf("video frame: %+v, %v", frame, err)
	}
}

func TestMediaBusVideoOrientationRejectsInvalidUpdate(t *testing.T) {
	bus := NewMediaBus(1)
	defer bus.Close()
	bus.SetOrientation(2)
	bus.SetOrientation(4)
	if err := bus.WriteVideo([]byte{0, 0, 0, 1, 0x65}); err != nil {
		t.Fatal(err)
	}
	frame, err := bus.NextVideoFromWhatsApp(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if frame.Orientation != 2 {
		t.Fatalf("orientation=%d, want 2", frame.Orientation)
	}
}

func TestMediaBusDropsOldFrames(t *testing.T) {
	bus := NewMediaBus(1)
	defer bus.Close()
	for i := range 10 {
		frame := make([]float32, AudioFrameSamples)
		frame[0] = float32(i) / 10
		if err := bus.WriteFrame(frame); err != nil {
			t.Fatal(err)
		}
	}
	got, err := bus.NextAudioFromWhatsApp(context.Background())
	if err != nil || got[0] != 0.9 {
		t.Fatalf("wanted newest frame, got %v, %v", got[0], err)
	}
}

func TestMediaBusClosingAndErrors(t *testing.T) {
	bus := NewMediaBus(1)
	if err := bus.WriteFrame([]float32{1}); !errors.Is(err, ErrBadFrame) {
		t.Fatal(err)
	}
	if err := bus.WriteVideo(nil); !errors.Is(err, ErrBadFrame) {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	_, err := bus.NextAudioFromWhatsApp(ctx)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal(err)
	}

	var wg sync.WaitGroup
	for range 10 {
		wg.Add(1)
		go func() { defer wg.Done(); _ = bus.Close() }()
	}
	wg.Wait()
	if _, err := bus.ReadFrame(); !errors.Is(err, io.EOF) {
		t.Fatal(err)
	}
	if err := bus.SendAudioToWhatsApp(make([]float32, AudioFrameSamples)); !errors.Is(err, io.ErrClosedPipe) {
		t.Fatal(err)
	}
}

func TestMediaBusNoMatrixAudioIsSilence(t *testing.T) {
	bus := NewMediaBus(1)
	defer bus.Close()
	start := time.Now()
	frame, err := bus.ReadFrame()
	if err != nil || len(frame) != AudioFrameSamples || frame[0] != 0 {
		t.Fatalf("wanted silent 16 kHz 60 ms frame, got len=%d err=%v", len(frame), err)
	}
	if time.Since(start) > 50*time.Millisecond {
		t.Fatal("outgoing RTP frame retrieval must not block for absent Matrix audio")
	}
}

func TestMediaBusCloseFencesAllWriters(t *testing.T) {
	bus := NewMediaBus(2)
	audio := make([]float32, AudioFrameSamples)
	video := []byte{1, 2, 3}
	if err := bus.WriteFrame(audio); err != nil {
		t.Fatal(err)
	}
	if err := bus.SendAudioToWhatsApp(audio); err != nil {
		t.Fatal(err)
	}
	if err := bus.WriteVideo(video); err != nil {
		t.Fatal(err)
	}
	bus.SetAudioMuted(true)
	if err := bus.Close(); err != nil {
		t.Fatal(err)
	}
	for range 100 {
		if err := bus.WriteFrame(audio); !errors.Is(err, io.ErrClosedPipe) {
			t.Fatal(err)
		}
		if err := bus.SendAudioToWhatsApp(audio); !errors.Is(err, io.ErrClosedPipe) {
			t.Fatal(err)
		}
		if err := bus.WriteVideo(video); !errors.Is(err, io.ErrClosedPipe) {
			t.Fatal(err)
		}
		if bus.TryVideo(EncodedVideoFrame{Data: video}) {
			t.Fatal("video admitted after close")
		}
	}
	if len(bus.fromWhatsAppAudio)+len(bus.toWhatsAppAudio)+len(bus.fromWhatsAppVideo) != 0 {
		t.Fatal("closed bus retained media")
	}
	if _, err := bus.NextAudioFromWhatsApp(context.Background()); !errors.Is(err, io.EOF) {
		t.Fatal(err)
	}
	if _, err := bus.NextVideoFromWhatsApp(context.Background()); !errors.Is(err, io.EOF) {
		t.Fatal(err)
	}
}

func TestMediaBusCloseConcurrentProducers(t *testing.T) {
	bus := NewMediaBus(2)
	start := make(chan struct{})
	var wg sync.WaitGroup
	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			audio := make([]float32, AudioFrameSamples)
			for range 100 {
				_ = bus.WriteFrame(audio)
				_ = bus.SendAudioToWhatsApp(audio)
				_ = bus.WriteVideo([]byte{1})
				bus.TryVideo(EncodedVideoFrame{Data: []byte{1}})
			}
		}()
	}
	close(start)
	_ = bus.Close()
	wg.Wait()
	if len(bus.fromWhatsAppAudio)+len(bus.toWhatsAppAudio)+len(bus.fromWhatsAppVideo) != 0 {
		t.Fatal("producer crossed close fence")
	}
}

func TestMediaBusResyncDropsDependencyChain(t *testing.T) {
	bus := NewMediaBus(2)
	defer bus.Close()
	old := EncodedVideoFrame{Generation: 1, Data: []byte{1}}
	if !bus.TryVideo(old) {
		t.Fatal("initial frame rejected")
	}
	bus.RequestVideoResync()
	if len(bus.fromWhatsAppVideo) != 0 || bus.TryVideo(old) {
		t.Fatal("old chain survived resync")
	}
	if !bus.TakeVideoResync() {
		t.Fatal("missing resync notification")
	}
	fresh := EncodedVideoFrame{Generation: 2, Data: []byte{5}}
	if !bus.TryVideo(fresh) {
		t.Fatal("new chain rejected")
	}
	got, err := bus.NextVideoFromWhatsApp(context.Background())
	if err != nil || got.Generation != 2 {
		t.Fatalf("got %+v, %v", got, err)
	}
}

func TestMediaBusOverflowRequiresResync(t *testing.T) {
	bus := NewMediaBus(1)
	defer bus.Close()
	frame := EncodedVideoFrame{Data: []byte{1}}
	if !bus.TryVideo(frame) || bus.TryVideo(frame) || bus.TryVideo(frame) {
		t.Fatal("overflow admitted dependent frame")
	}
	if len(bus.fromWhatsAppVideo) != 0 || !bus.TakeVideoResync() {
		t.Fatal("overflow did not invalidate chain")
	}
	if !bus.TryVideo(frame) {
		t.Fatal("new chain rejected")
	}
}

func TestMediaBusCanceledReadsRejectReadyFrames(t *testing.T) {
	bus := NewMediaBus(2)
	defer bus.Close()
	if err := bus.WriteFrame(make([]float32, AudioFrameSamples)); err != nil {
		t.Fatal(err)
	}
	if !bus.TryVideo(EncodedVideoFrame{Data: []byte{1}}) {
		t.Fatal("video rejected")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if frame, err := bus.NextAudioFromWhatsApp(ctx); frame != nil || !errors.Is(err, context.Canceled) {
		t.Fatalf("audio: %v", err)
	}
	if frame, err := bus.NextVideoFromWhatsApp(ctx); frame.Data != nil || !errors.Is(err, context.Canceled) {
		t.Fatalf("video: %v", err)
	}
	if len(bus.fromWhatsAppAudio) != 1 || len(bus.fromWhatsAppVideo) != 1 {
		t.Fatal("canceled reader consumed queued frames")
	}
}

func TestMediaBusTryVideoOwnsData(t *testing.T) {
	bus := NewMediaBus(1)
	defer bus.Close()
	data := []byte{0, 0, 1, 0x65}
	if !bus.TryVideo(EncodedVideoFrame{Data: data, Generation: 7, Timestamp: 123, Orientation: 2}) {
		t.Fatal("frame rejected")
	}
	data[3] = 0x41
	frame, err := bus.NextVideoFromWhatsApp(context.Background())
	if err != nil || frame.Data[3] != 0x65 || frame.Generation != 7 || frame.Timestamp != 123 || frame.Orientation != 2 {
		t.Fatalf("queued data or metadata changed: %+v %v", frame, err)
	}
}

func TestMediaBusLegacyVideoSharesResyncFence(t *testing.T) {
	bus := NewMediaBus(1)
	defer bus.Close()
	old := []byte{0, 0, 1, 0x41}
	if err := bus.WriteVideo(old); err != nil {
		t.Fatal(err)
	}
	if err := bus.WriteVideo(old); err != nil {
		t.Fatal(err)
	}
	if len(bus.fromWhatsAppVideo) != 0 {
		t.Fatal("legacy overflow retained partial GOP")
	}
	if err := bus.WriteVideo(old); err != nil {
		t.Fatal(err)
	}
	if bus.TryVideo(EncodedVideoFrame{Data: old}) || len(bus.fromWhatsAppVideo) != 0 {
		t.Fatal("legacy write bypassed pending resync")
	}
	if !bus.TakeVideoResync() {
		t.Fatal("missing resync request")
	}
	fresh := []byte{0, 0, 1, 0x65}
	if err := bus.WriteVideo(fresh); err != nil {
		t.Fatal(err)
	}
	frame, err := bus.NextVideoFromWhatsApp(context.Background())
	if err != nil || frame.Data[3] != 0x65 {
		t.Fatalf("fresh frame not recovered: %+v %v", frame, err)
	}
}

func TestMediaBusOversizedVideoInvalidatesChain(t *testing.T) {
	for _, direct := range []bool{false, true} {
		bus := NewMediaBus(3)
		if !bus.TryVideo(EncodedVideoFrame{Data: []byte{1}, Generation: 1}) {
			t.Fatal("initial frame refused")
		}
		oversized := make([]byte, rtcwire.MaxQueuedVideoBytes+1)
		if direct {
			if err := bus.WriteVideo(oversized); err != nil {
				t.Fatal(err)
			}
		} else if bus.TryVideo(EncodedVideoFrame{Data: oversized, Generation: 1}) {
			t.Fatal("oversized frame admitted")
		}
		if len(bus.fromWhatsAppVideo) != 0 || bus.TryVideo(EncodedVideoFrame{Data: []byte{1}}) || !bus.TakeVideoResync() {
			t.Fatal("oversized video did not invalidate the dependency chain")
		}
		if !bus.TryVideo(EncodedVideoFrame{Data: oversized[:rtcwire.MaxQueuedVideoBytes], Generation: 2}) {
			t.Fatal("boundary frame refused after resync")
		}
		got, err := bus.NextVideoFromWhatsApp(context.Background())
		if err != nil || len(got.Data) != rtcwire.MaxQueuedVideoBytes || got.Generation != 2 {
			t.Fatal("boundary frame not recovered", err)
		}
		if err := bus.WriteFrame(make([]float32, AudioFrameSamples)); err != nil {
			t.Fatal("video loss interrupted audio", err)
		}
		bus.Close()
	}
}

func TestMediaBusInvalidDecoderOutputDoesNotEvictValidAudio(t *testing.T) {
	bus := NewMediaBus(1)
	defer bus.Close()
	good := make([]float32, AudioFrameSamples)
	good[0], good[len(good)-1] = -1, 1
	if err := bus.WriteFrame(good); err != nil {
		t.Fatal(err)
	}
	bad := make([]float32, AudioFrameSamples)
	for _, value := range []float32{float32(math.NaN()), float32(math.Inf(1)), float32(math.Inf(-1)), 1.01, -1.01} {
		bad[len(bad)-1] = value
		if allocs := testing.AllocsPerRun(20, func() {
			if err := bus.WriteFrame(bad); err != nil {
				t.Fatal(err)
			}
		}); allocs != 0 {
			t.Fatalf("invalid PCM allocated %v objects", allocs)
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	got, err := bus.NextAudioFromWhatsApp(ctx)
	if err != nil || got[0] != -1 || got[len(got)-1] != 1 {
		t.Fatalf("valid frame evicted: %v", err)
	}
	if err := bus.WriteFrame(bad); err != nil || len(bus.fromWhatsAppAudio) != 0 {
		t.Fatalf("invalid frame queued: %v", err)
	}
	good[0] = 0.5
	if err := bus.WriteFrame(good); err != nil {
		t.Fatal(err)
	}
	got, err = bus.NextAudioFromWhatsApp(ctx)
	if err != nil || got[0] != 0.5 {
		t.Fatalf("valid audio did not recover: %v", err)
	}
}

func TestAudioGenerationInvalidatesBufferedPCM(t *testing.T) {
	bus := NewMediaBus(2)
	defer bus.Close()
	original := bus.AudioFrameGeneration()
	bus.SetAudioMuted(false)
	if bus.AudioFrameGeneration() != original {
		t.Fatal("unchanged mute invalidated PCM")
	}
	bus.SetAudioMuted(true)
	muted := bus.AudioFrameGeneration()
	if muted == original {
		t.Fatal("mute did not invalidate PCM")
	}
	bus.SetAudioMuted(true)
	if bus.AudioFrameGeneration() != muted {
		t.Fatal("duplicate mute invalidated PCM")
	}
	bus.SetAudioMuted(false)
	if bus.AudioFrameGeneration() == muted {
		t.Fatal("unmute did not invalidate PCM")
	}
}
