//go:build liveopus && cgo

package liveopus

import (
	"math"
	"os"
	"strings"
	"testing"
)

func TestRoundTripNegotiatedFrameLengths(t *testing.T) {
	for _, size := range []int{40, 80, 160, 320, 640, 960} {
		e, err := NewEncoder(24000)
		if err != nil {
			t.Fatal(err)
		}
		d, err := NewDecoder()
		if err != nil {
			e.Close()
			t.Fatal(err)
		}
		var decoded []float32
		for offset := 0; offset < SampleRate; offset += size {
			pcm := make([]float32, size)
			for i := range pcm {
				pcm[i] = float32(0.3 * math.Sin(2*math.Pi*440*float64(i+offset)/SampleRate))
			}
			packet, err := e.Encode(pcm)
			if err != nil {
				t.Fatal(err)
			}
			out, err := d.Decode(packet)
			if err != nil || len(out) != size {
				t.Fatalf("size %d: decoded %d: %v", size, len(out), err)
			}
			decoded = append(decoded, out...)
		}
		var i, q, energy float64
		for j, x := range decoded[SampleRate/5:] {
			angle := 2 * math.Pi * 440 * float64(j) / SampleRate
			i += float64(x) * math.Cos(angle)
			q += float64(x) * math.Sin(angle)
			energy += float64(x) * float64(x)
		}
		count := float64(len(decoded) - SampleRate/5)
		if energy/count < 0.002 || 2*(i*i+q*q)/(count*energy) < 0.6 {
			t.Fatalf("size %d: tone not preserved", size)
		}
		e.Close()
		d.Close()
		if _, err = e.Encode(make([]float32, size)); err == nil {
			t.Fatal("closed encoder accepted frame")
		}
		if _, err = d.Decode([]byte{0x10}); err == nil {
			t.Fatal("closed decoder accepted packet")
		}
	}
}

func TestRejectInvalidInput(t *testing.T) {
	if _, err := NewEncoder(-1); err == nil {
		t.Fatal("invalid bitrate accepted")
	}
	e, err := NewEncoder(0)
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	d, err := NewDecoder()
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	for _, size := range []int{0, 1, 100, 1920} {
		if _, err := e.Encode(make([]float32, size)); err == nil {
			t.Fatal("invalid frame accepted", size)
		}
	}
	pcm := make([]float32, 320)
	pcm[0] = float32(math.NaN())
	if _, err := e.Encode(pcm); err == nil {
		t.Fatal("invalid PCM accepted")
	}
	for _, packet := range [][]byte{nil, make([]byte, MaxPacketBytes+1), {0xff}} {
		if _, err := d.Decode(packet); err == nil {
			t.Fatal("invalid packet accepted")
		}
	}
}

func TestLinkedOpusVersion(t *testing.T) {
	version := Version()
	t.Logf("linked codec: %s", version)
	if !strings.HasPrefix(version, "libopus ") {
		t.Fatalf("unexpected libopus identity: %s", version)
	}
	if expected := os.Getenv("MAUTRIX_WHATSAPP_CALL_EXPECT_OPUS_VERSION"); expected != "" && version != "libopus "+expected {
		t.Fatalf("linked %s; expected pinned libopus %s", version, expected)
	}
}
