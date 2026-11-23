package rtcwire

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"math"
	"net"
	"sync"
	"testing"
)

func TestRoundTrip(t *testing.T) {
	got := Message{Type: "incoming", RoomID: "!room:example.org", SlotID: "m.call#room", MembershipFormat: "msc", CallID: "123", LoginID: "a", GhostMXID: "@whatsapp_123:example.org", Video: true, Orientation: 3, DurationUS: 33_333}
	var buf bytes.Buffer
	if err := Write(&buf, got); err != nil {
		t.Fatal(err)
	}
	received, err := Read(&buf)
	if err != nil || received.Version != 1 || received.RoomID != got.RoomID || received.SlotID != got.SlotID || received.MembershipFormat != got.MembershipFormat || received.GhostMXID != got.GhostMXID || !received.Video || received.Orientation != got.Orientation || received.DurationUS != got.DurationUS {
		t.Fatalf("got %+v, %v", received, err)
	}
}

func TestRejectInvalidVideoOrientation(t *testing.T) {
	if err := Write(io.Discard, Message{Type: "matrix_video", Orientation: 4}); err == nil {
		t.Fatal("accepted video orientation outside 0..3")
	}
}

func TestRejectInvalidVideoDuration(t *testing.T) {
	if err := Write(io.Discard, Message{Type: "matrix_video", DurationUS: 1_000_001}); err == nil {
		t.Fatal("accepted video duration over one second")
	}
}

func TestFrameErrors(t *testing.T) {
	oversized := make([]byte, 4)
	binary.BigEndian.PutUint32(oversized, uint32(MaxPayloadSize+1))
	if _, err := Read(bytes.NewReader(oversized)); !errors.Is(err, ErrTooLarge) {
		t.Fatalf("err=%v", err)
	}
	if _, err := Read(bytes.NewReader([]byte{0, 0, 0, 9, 'x'})); !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("err=%v", err)
	}
	if err := Write(io.Discard, Message{}); err == nil {
		t.Fatal("accepted missing type")
	}
}

func TestPCMRoundTripAndInvalid(t *testing.T) {
	pcm := make([]float32, AudioSamples)
	pcm[0], pcm[42] = -0.75, 0.25
	raw, err := EncodePCM(pcm)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := DecodePCM(raw)
	if err != nil || decoded[0] != pcm[0] || decoded[42] != pcm[42] {
		t.Fatalf("pcm error %v", err)
	}
	pcm[0] = float32(math.NaN())
	if _, err = EncodePCM(pcm); !errors.Is(err, ErrBadAudio) {
		t.Fatalf("want invalid NaN, got %v", err)
	}
	if _, err = DecodePCM(raw[:len(raw)-1]); !errors.Is(err, ErrBadAudio) {
		t.Fatalf("want short frame, got %v", err)
	}
}

func TestConcurrentSend(t *testing.T) {
	conn1, conn2 := net.Pipe()
	defer conn1.Close()
	defer conn2.Close()
	peer := NewPeer(conn1, conn1)
	const producers = 5
	const count = 20
	errs := make(chan error, producers)
	var wg sync.WaitGroup
	for range producers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range count {
				if err := peer.Send(Message{Type: "matrix_audio", Data: make([]byte, AudioBytes)}); err != nil {
					errs <- err
					return
				}
			}
		}()
	}
	for range producers * count {
		m, err := Read(conn2)
		if err != nil || m.Type != "matrix_audio" {
			t.Fatalf("got %v, %v", m.Type, err)
		}
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
}

func TestGroupAndMediaStateRoundTrip(t *testing.T) {
	msg := Message{Type: "whatsapp_video_state", CallID: "c", Group: true, GroupJID: "123@g.us", State: 11, Muted: true, Orientation: 2}
	var buf bytes.Buffer
	if err := Write(&buf, msg); err != nil {
		t.Fatal(err)
	}
	got, err := Read(&buf)
	if err != nil {
		t.Fatal(err)
	}
	if !got.Group || got.GroupJID != "123@g.us" || got.State != 11 || !got.Muted || got.Orientation != 2 {
		t.Fatalf("roundtrip = %#v", got)
	}
}
