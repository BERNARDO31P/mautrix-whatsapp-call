package connector

import (
	"bytes"
	"context"
	"go.mau.fi/mautrix-whatsapp/pkg/connector/voipbridge"
	"go.mau.fi/mautrix-whatsapp/pkg/connector/voipbridge/rtcwire"
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
