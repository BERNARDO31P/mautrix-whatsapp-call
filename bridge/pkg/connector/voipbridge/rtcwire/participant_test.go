package rtcwire

import (
	"bytes"
	"io"
	"math"
	"reflect"
	"testing"
)

func TestParticipantFrames(t *testing.T) {
	pcm, err := EncodeParticipantPCM([]float32{0, 0.5, -1})
	if err != nil {
		t.Fatal(err)
	}
	a := Message{Version: Version, Type: "whatsapp_participant_audio", CallID: "call", Group: true, Generation: 1, Timestamp: 0xffffffff, Data: pcm, ParticipantSource: &ParticipantSource{ParticipantID: "alice", Device: "alice:1@lid", SSRC: 42, RTPClockRate: 48000, SampleRate: 16000, Discontinuity: true}}
	v := Message{Version: Version, Type: "whatsapp_participant_video", CallID: "call", Group: true, Generation: 1, Timestamp: 17, Orientation: 3, Data: []byte{0, 0, 1, 0x65, 0x01}, ParticipantSource: &ParticipantSource{ParticipantID: "alice", Device: "alice:1@lid", SSRC: 43, RTPClockRate: 90000}}
	for _, msg := range []Message{a, v} {
		var b bytes.Buffer
		if err := Write(&b, msg); err != nil {
			t.Fatal(err)
		}
		got, err := Read(&b)
		if err != nil || !reflect.DeepEqual(got, msg) {
			t.Fatalf("round trip: %+v %v", got, err)
		}
	}
	for _, mutate := range []func(*Message){
		func(m *Message) { m.Group = false },
		func(m *Message) { m.CallID = "" },
		func(m *Message) { m.Generation = 0 },
		func(m *Message) { m.Timestamp = 1 << 32 },
		func(m *Message) { m.ParticipantSource = nil },
		func(m *Message) { m.ParticipantSource.Device = "" },
		func(m *Message) { m.ParticipantSource.SSRC = 0 },
		func(m *Message) { m.ParticipantSource.SampleRate = 48000 },
		func(m *Message) { m.ParticipantSource.RTPClockRate = 90000 },
		func(m *Message) { m.Type = "whatsapp_audio" },
		func(m *Message) { m.Data = []byte{0, 0, 0, 0x7f} },
		func(m *Message) { m.Data = make([]byte, 1921*4) },
	} {
		bad := a
		source := *a.ParticipantSource
		bad.ParticipantSource = &source
		mutate(&bad)
		if err := Write(io.Discard, bad); err == nil {
			t.Fatalf("accepted invalid frame: %+v", bad)
		}
	}
	if _, err := EncodeParticipantPCM([]float32{float32(math.Inf(1))}); err == nil {
		t.Fatal("accepted infinite PCM")
	}
	if _, err := EncodePCM([]float32{0}); err == nil {
		t.Fatal("aggregate frame size weakened")
	}
}
