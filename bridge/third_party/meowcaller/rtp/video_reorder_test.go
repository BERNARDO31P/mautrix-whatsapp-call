package rtp

import (
	"bytes"
	"testing"
	"time"
)

func TestVideoReorderCompletesFragmentedIDR(t *testing.T) {
	var r VideoReorder
	var a H264AccessUnitAssembler
	a.RequireKeyframe()
	now := time.Unix(1, 0)
	nal := append([]byte{0x65}, bytes.Repeat([]byte{0x42}, 1700)...)
	frags := PackageH264NALU(nal)
	var result []byte
	for _, i := range []int{0, 2, 1} {
		for _, p := range r.Push(VideoPacket{Sequence: uint16(65535 + i), Timestamp: 42, Marker: i == 2, Payload: frags[i]}, now) {
			if frame, complete, _ := a.Push(p.Sequence, p.Timestamp, p.Marker, p.Payload); complete {
				result = frame
			}
		}
	}
	if !bytes.Equal(result, append([]byte{0, 0, 0, 1}, nal...)) {
		t.Fatal("fragmented IDR not recovered across wrap")
	}
}

func TestVideoReorderSkipsExpiredGap(t *testing.T) {
	var r VideoReorder
	now := time.Unix(1, 0)
	p := func(seq uint16, when time.Time) []VideoPacket {
		return r.Push(VideoPacket{Sequence: seq, Payload: []byte{1}}, when)
	}
	if len(p(10, now)) != 1 || len(p(12, now)) != 0 {
		t.Fatal("in-order or gap behavior")
	}
	got := p(13, now.Add(videoReorderDelay))
	if len(got) != 2 || got[0].Sequence != 12 || got[1].Sequence != 13 || len(p(11, now)) != 0 {
		t.Fatal("gap not skipped or late packet replayed")
	}
}

func TestVideoReorderBoundsAndOwnsPayload(t *testing.T) {
	var r VideoReorder
	now := time.Unix(1, 0)
	r.Push(VideoPacket{Sequence: 1}, now)
	payload := []byte{3}
	r.Push(VideoPacket{Sequence: 3, Payload: payload}, now)
	payload[0] = 9
	r.Push(VideoPacket{Sequence: 3, Payload: payload}, now)
	got := r.Push(VideoPacket{Sequence: 2}, now)
	if len(got) != 2 || got[1].Payload[0] != 3 {
		t.Fatal("duplicate or reused buffer corrupted data")
	}
	for i := 5; i < 200; i++ {
		r.Push(VideoPacket{Sequence: uint16(i)}, now)
	}
	if len(r.pending) >= videoReorderWindow {
		t.Fatal("window exceeded")
	}
}
