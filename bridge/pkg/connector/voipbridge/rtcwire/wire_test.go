package rtcwire

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"io"
	"math"
	"net"
	"strings"
	"sync"
	"testing"
	"time"
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

func TestReferencedJoinPinRoundTrip(t *testing.T) {
	message := Message{Type: "outgoing", MembershipFormat: "msc", MemberEvent: "$original", MemberJoinID: "join-1", ExpectedDevice: "DEVICE"}
	var buf bytes.Buffer
	if err := Write(&buf, message); err != nil {
		t.Fatal(err)
	}
	got, err := Read(&buf)
	if err != nil || got.MemberJoinID != message.MemberJoinID {
		t.Fatalf("pin not preserved: %+v, %v", got, err)
	}
	for _, invalid := range []Message{
		{Type: "incoming", MembershipFormat: "msc", MemberEvent: "$original", MemberJoinID: "join-1", ExpectedDevice: "DEVICE"},
		{Type: "outgoing", MembershipFormat: "room_state", MemberEvent: "$original", MemberJoinID: "join-1", ExpectedDevice: "DEVICE"},
		{Type: "outgoing", MembershipFormat: "msc", MemberJoinID: "join-1", ExpectedDevice: "DEVICE"},
		{Type: "outgoing", MembershipFormat: "msc", MemberEvent: "$original", MemberJoinID: "join-1"},
		{Type: "outgoing", MembershipFormat: "msc", MemberEvent: "$original", MemberJoinID: strings.Repeat("a", 513), ExpectedDevice: "DEVICE"},
	} {
		if err := Write(io.Discard, invalid); err == nil {
			t.Fatal("accepted invalid join pin")
		}
	}
}

func TestLegacyReferencedJoinPinRoundTrip(t *testing.T) {
	message := Message{Type: "outgoing", MembershipFormat: "room_state", MemberEvent: "$original", MemberJoinID: "join-1", MemberJoinTS: 12345, ExpectedDevice: "DEVICE"}
	var buf bytes.Buffer
	if err := Write(&buf, message); err != nil {
		t.Fatal(err)
	}
	got, err := Read(&buf)
	if err != nil || got.MemberJoinID != message.MemberJoinID || got.MemberJoinTS != message.MemberJoinTS {
		t.Fatalf("pin not preserved: %+v, %v", got, err)
	}
	for _, change := range []func(*Message){
		func(m *Message) { m.MemberJoinID = "" },
		func(m *Message) { m.MemberJoinTS = 0 },
		func(m *Message) { m.MembershipFormat = "msc" },
		func(m *Message) { m.Type = "incoming" },
		func(m *Message) { m.MemberEvent = "" },
		func(m *Message) { m.ExpectedDevice = "" },
	} {
		bad := message
		change(&bad)
		if err := Write(io.Discard, bad); err == nil {
			t.Fatalf("accepted invalid legacy pin: %+v", bad)
		}
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

// Simulate a body write that failed after the length and part of the body.
type brokenFrameWriter struct {
	calls  int
	closed bool
}

func (w *brokenFrameWriter) Write(p []byte) (int, error) {
	w.calls++
	if w.calls == 1 {
		return len(p), nil
	}
	return 1, io.ErrUnexpectedEOF
}
func (w *brokenFrameWriter) Close() error { w.closed = true; return nil }
func TestPartialWritePoisonsPeer(t *testing.T) {
	w := &brokenFrameWriter{}
	peer := NewPeer(bytes.NewReader(nil), w)
	if err := peer.Send(Message{Type: "answer"}); !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatal(err)
	}
	if !w.closed {
		t.Fatal("failed stream was not closed")
	}
	if err := peer.Send(Message{Type: "hangup"}); !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatal(err)
	}
	if w.calls != 2 {
		t.Fatalf("poisoned stream reused: %d writes", w.calls)
	}
}
func TestRejectRustIncompatibleIdentifiers(t *testing.T) {
	for _, m := range []Message{
		{Type: strings.Repeat("a", 41)},
		{Type: "incoming", RoomID: strings.Repeat("a", 513)},
		{Type: "incoming", SlotID: strings.Repeat("a", 257)},
		{Type: "incoming", MembershipFormat: "unknown"},
		{Type: "incoming", ExpectedUserMXID: strings.Repeat("a", 513)},
	} {
		if err := Write(io.Discard, m); err == nil {
			t.Fatalf("accepted invalid frame: %s", m.Type)
		}
	}
}

func TestCanceledSendDoesNotWaitForActiveWriter(t *testing.T) {
	var buf bytes.Buffer
	peer := NewPeer(bytes.NewReader(nil), &buf)
	// Hold the same admission token as a currently active writer.
	peer.writeGate <- struct{}{}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	result := make(chan error, 1)
	go func() { result <- peer.SendContext(ctx, Message{Type: "answer"}) }()
	cancel()
	select {
	case err := <-result:
		if !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("canceled sender remained blocked")
	}
	<-peer.writeGate
	if buf.Len() != 0 {
		t.Fatal("canceled waiter wrote bytes")
	}
	if err := peer.Send(Message{Type: "hangup"}); err != nil {
		t.Fatal(err)
	}
	got, err := Read(&buf)
	if err != nil || got.Type != "hangup" {
		t.Fatalf("peer not reusable after canceled admission: %+v, %v", got, err)
	}
}

func TestAlreadyCanceledSendDoesNotWrite(t *testing.T) {
	var buf bytes.Buffer
	peer := NewPeer(bytes.NewReader(nil), &buf)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	for range 100 {
		if err := peer.SendContext(ctx, Message{Type: "answer"}); !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
	}
	if buf.Len() != 0 {
		t.Fatal("canceled send wrote bytes")
	}
}

func TestSendContextDeadlineInterruptsBlockedWire(t *testing.T) {
	left, right := net.Pipe()
	defer left.Close()
	defer right.Close()
	peer := NewPeer(left, left)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	start := time.Now()
	if err := peer.SendContext(ctx, Message{Type: "answer"}); err == nil {
		t.Fatal("blocked write succeeded")
	}
	if time.Since(start) > time.Second {
		t.Fatal("context deadline was not used for write")
	}
	if err := peer.Send(Message{Type: "hangup"}); err == nil {
		t.Fatal("failed stream was reused")
	}
}

func TestCancelInFlightFrame(t *testing.T) {
	left, right := net.Pipe()
	defer left.Close()
	defer right.Close()
	peer := NewPeer(left, left)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	result := make(chan error, 1)
	go func() { result <- peer.SendContext(ctx, Message{Type: "answer"}) }()
	// Read only the header, proving that transmission has begun. The body
	// remains blocked until cancellation interrupts the write.
	if err := right.SetReadDeadline(time.Now().Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	var header [4]byte
	if _, err := io.ReadFull(right, header[:]); err != nil {
		t.Fatal(err)
	}
	cancel()
	select {
	case err := <-result:
		if err == nil {
			t.Fatal("partial frame reported success")
		}
	case <-time.After(time.Second):
		t.Fatal("cancel did not interrupt the active write")
	}
	if err := peer.Send(Message{Type: "hangup"}); err == nil {
		t.Fatal("partial stream reused")
	}
}

func TestInvalidLocalFrameDoesNotPoisonPeer(t *testing.T) {
	var buf bytes.Buffer
	peer := NewPeer(bytes.NewReader(nil), &buf)
	for _, msg := range []Message{
		{},
		{Type: "matrix_audio", Data: []byte{0}},
		{Type: "matrix_video", Orientation: 4},
		{Type: "answer", Reason: strings.Repeat("x", 4097)},
	} {
		if err := peer.Send(msg); err == nil {
			t.Fatal("invalid local frame accepted")
		}
		if buf.Len() != 0 {
			t.Fatal("invalid local frame wrote bytes")
		}
	}
	if err := peer.Send(Message{Type: "hangup", CallID: "other-call"}); err != nil {
		t.Fatal(err)
	}
	got, err := Read(&buf)
	if err != nil || got.Type != "hangup" || got.CallID != "other-call" {
		t.Fatalf("valid frame lost after invalid frame: %+v, %v", got, err)
	}
}

func TestOversizedLocalFrameDoesNotCloseSocket(t *testing.T) {
	left, right := net.Pipe()
	defer left.Close()
	defer right.Close()
	peer := NewPeer(left, left)
	data := make([]byte, MaxPayloadSize)
	data[2] = 1 // Annex B prefix, but base64 representation exceeds limit.
	if err := peer.Send(Message{Type: "matrix_video", Data: data}); !errors.Is(err, ErrTooLarge) {
		t.Fatalf("expected size error, got %v", err)
	}
	result := make(chan error, 1)
	go func() { result <- peer.Send(Message{Type: "answer"}) }()
	if err := right.SetReadDeadline(time.Now().Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	got, err := Read(right)
	if err != nil || got.Type != "answer" {
		t.Fatalf("socket unusable after rejected local frame: %+v, %v", got, err)
	}
	if err := <-result; err != nil {
		t.Fatal(err)
	}
}

func TestBridgeBotIdentityOnlyOnCallAdmission(t *testing.T) {
	for _, kind := range []string{"incoming", "outgoing"} {
		var buf bytes.Buffer
		if err := Write(&buf, Message{Type: kind, BridgeBotMXID: "@custombot:example.org"}); err != nil {
			t.Fatal(err)
		}
		got, err := Read(&buf)
		if err != nil || got.BridgeBotMXID != "@custombot:example.org" {
			t.Fatalf("bot identity roundtrip: %v %v", got, err)
		}
	}
	for _, msg := range []Message{
		{Type: "answer", BridgeBotMXID: "@custombot:example.org"},
		{Type: "incoming", BridgeBotMXID: strings.Repeat("a", 513)},
	} {
		if err := Write(io.Discard, msg); err == nil {
			t.Fatal("invalid bot identity accepted")
		}
	}
}

func TestQueuedVideoBoundaryFitsWireEnvelope(t *testing.T) {
	data := make([]byte, MaxQueuedVideoBytes)
	data[2] = 1
	// JSON escapes control bytes into six-byte sequences. The queue cap
	// must still leave room for the longest supported call ID and clocks.
	msg := Message{Type: "whatsapp_video", CallID: strings.Repeat("\x00", 256), Data: data,
		Generation: ^uint64(0), Timestamp: ^uint64(0), Orientation: 3}
	body, err := encodeMessage(msg)
	if err != nil || len(body) > MaxPayloadSize {
		t.Fatalf("queue boundary does not fit wire: size=%d err=%v", len(body), err)
	}
}

func TestOutgoingMediaIntent(t *testing.T) {
	for _, m := range []Message{{Type: "outgoing", Intent: "auto"}, {Type: "outgoing", Intent: "audio"}, {Type: "outgoing", Intent: "video"}, {Type: "dial", Intent: "audio"}, {Type: "dial", Intent: "video", Video: true}, {Type: "outgoing", Video: true}} {
		var buf bytes.Buffer
		if err := Write(&buf, m); err != nil {
			t.Fatal(err)
		}
		got, err := Read(&buf)
		if err != nil || got.Intent != m.Intent || got.Video != m.Video {
			t.Fatalf("%+v %v", got, err)
		}
	}
	for _, m := range []Message{{Type: "incoming", Intent: "auto"}, {Type: "outgoing", Intent: "bogus"}, {Type: "outgoing", Intent: "auto", Video: true}, {Type: "dial", Intent: "auto"}, {Type: "dial", Intent: "audio", Video: true}, {Type: "dial", Intent: "video"}} {
		if err := Write(io.Discard, m); err == nil {
			t.Fatalf("accepted %+v", m)
		}
	}
}

func TestInvitationDeviceKeyFields(t *testing.T) {
	key := strings.Repeat("A", 43)
	msg := Message{Type: "outgoing", CallID: "call", ExpectedDevice: "DEVICE", ExpectedSigningKey: key, ExpectedIdentityKey: key}
	var buf bytes.Buffer
	if err := Write(&buf, msg); err != nil {
		t.Fatal(err)
	}
	got, err := Read(&buf)
	if err != nil || got.ExpectedSigningKey != key || got.ExpectedIdentityKey != key {
		t.Fatalf("key round trip: %v", err)
	}
	for _, change := range []func(*Message){
		func(m *Message) { m.ExpectedSigningKey = "" },
		func(m *Message) { m.ExpectedIdentityKey = key + "=" },
		func(m *Message) { m.ExpectedIdentityKey = strings.Repeat("A", 42) + "B" },
		func(m *Message) { m.ExpectedDevice = "" },
		func(m *Message) { m.Type = "incoming" },
	} {
		bad := msg
		change(&bad)
		if err := Write(io.Discard, bad); err == nil {
			t.Fatalf("accepted invalid invitation keys: %+v", bad)
		}
	}
}

// Cancel synchronously during deadline installation, before submission.
type cancelOnDeadlineWriter struct {
	bytes.Buffer
	cancel context.CancelFunc
	closed bool
}

func (w *cancelOnDeadlineWriter) SetWriteDeadline(deadline time.Time) error {
	if !deadline.IsZero() && w.cancel != nil {
		w.cancel()
		w.cancel = nil
	}
	return nil
}

func (w *cancelOnDeadlineWriter) Close() error {
	w.closed = true
	return nil
}

func TestCancelDuringDeadlineSetupDoesNotWriteOrPoisonPeer(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	w := &cancelOnDeadlineWriter{cancel: cancel}
	peer := NewPeer(bytes.NewReader(nil), w)
	if err := peer.SendContext(ctx, Message{Type: "answer"}); !errors.Is(err, context.Canceled) {
		t.Fatalf("expected pre-write cancellation, got %v", err)
	}
	if w.Len() != 0 || w.closed {
		t.Fatal("canceled pre-write frame modified or closed stream")
	}
	if err := peer.Send(Message{Type: "hangup"}); err != nil {
		t.Fatal(err)
	}
	got, err := Read(w)
	if err != nil || got.Type != "hangup" || w.Len() != 0 {
		t.Fatalf("unexpected remaining frame: %+v, %v", got, err)
	}
}

func TestOversizedBase64PreflightAllocations(t *testing.T) {
	data := make([]byte, MaxPayloadSize*3/4+1)
	data[2] = 1
	msg := Message{Type: "matrix_video", Data: data}
	// Marshal may reuse scratch storage, but still copies its result.
	result := testing.Benchmark(func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			if _, err := encodeMessage(msg); !errors.Is(err, ErrTooLarge) {
				b.Fatalf("expected oversize error, got %v", err)
			}
		}
	})
	if result.AllocedBytesPerOp() >= int64(MaxPayloadSize) {
		t.Fatalf("oversize frame allocated %d bytes/op", result.AllocedBytesPerOp())
	}
	msg.Data = data[:MaxQueuedVideoBytes]
	if _, err := encodeMessage(msg); err != nil {
		t.Fatalf("valid queued video rejected: %v", err)
	}
}

func TestAudioWireAdmissionWithoutDecodeAllocation(t *testing.T) {
	raw := make([]byte, AudioBytes)
	for _, kind := range []string{"matrix_audio", "whatsapp_audio"} {
		msg := Message{Type: kind, Data: raw}
		if allocs := testing.AllocsPerRun(100, func() {
			if !validBody(msg) {
				t.Fatal("valid PCM rejected")
			}
		}); allocs != 0 {
			t.Fatalf("%s validation allocated %v objects", kind, allocs)
		}
		for _, value := range []float32{-1, 1, 0, 1.01, -1.01, float32(math.NaN()), float32(math.Inf(1)), float32(math.Inf(-1))} {
			binary.LittleEndian.PutUint32(raw[len(raw)-4:], math.Float32bits(value))
			expected := value >= -1 && value <= 1
			_, err := DecodePCM(raw)
			if validBody(msg) != expected || (err == nil) != expected {
				t.Fatalf("%s validation/decode mismatch for %v: %v", kind, value, err)
			}
		}
		binary.LittleEndian.PutUint32(raw[len(raw)-4:], 0)
		msg.Data = raw[:len(raw)-1]
		if validBody(msg) {
			t.Fatal("short PCM accepted")
		}
	}
}

// Even Send without a caller deadline must have bounded admission. A frame
// which times out while waiting must not be submitted or poison the stream.
func TestDefaultSendBudgetIncludesAdmission(t *testing.T) {
	var buf bytes.Buffer
	peer := NewPeer(bytes.NewReader(nil), &buf)
	peer.writeGate <- struct{}{}
	started := time.Now()
	result := make(chan error, 1)
	go func() { result <- peer.Send(Message{Type: "answer"}) }()
	select {
	case err := <-result:
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatal(err)
		}
		if time.Since(started) < wireSendBudget {
			t.Fatal("budget expired too early")
		}
	case <-time.After(wireSendBudget + time.Second):
		t.Fatal("default sender remained blocked")
	}
	<-peer.writeGate
	if buf.Len() != 0 {
		t.Fatal("timed-out waiter wrote bytes")
	}
	if err := peer.Send(Message{Type: "hangup"}); err != nil {
		t.Fatal(err)
	}
	got, err := Read(&buf)
	if err != nil || got.Type != "hangup" {
		t.Fatalf("unexpected frame: %+v, %v", got, err)
	}
}
