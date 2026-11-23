package voipbridge

import (
	"context"
	"errors"
	"io"
	"sync"
	"testing"
	"time"
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
		frame[0] = float32(i)
		if err := bus.WriteFrame(frame); err != nil {
			t.Fatal(err)
		}
	}
	got, err := bus.NextAudioFromWhatsApp(context.Background())
	if err != nil || got[0] != 9 {
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
