package meowcaller

import "testing"

func TestOpusPacketizerExactPCM(t *testing.T) {
	for _, samples := range []int{40, 80, 160, 320, 640, 960} {
		var p opusPCMPacketizer
		produced, consumed := 0, 0
		for i := 0; i < 40; i++ {
			packet := p.next(samples, func() []float32 {
				pcm := make([]float32, FrameSamples)
				for j := range pcm {
					pcm[j] = float32(produced)
					produced++
				}
				return pcm
			})
			if len(packet) != samples {
				t.Fatal("wrong packet size")
			}
			for _, x := range packet {
				if x != float32(consumed) {
					t.Fatal("PCM skipped or repeated", samples, consumed, x)
				}
				consumed++
			}
			if len(p.pending) >= FrameSamples {
				t.Fatal("packetizer exceeded bound")
			}
		}
		p.reset()
		packet := p.next(samples, func() []float32 { return nil })
		for _, x := range packet {
			if x != 0 {
				t.Fatal("reset leaked buffered PCM")
			}
		}
	}
}

func TestOpusPlayoutUses48kClockAnd960SampleSink(t *testing.T) {
	p := newAudioPlayoutBuffer()
	p.clockScale = 3
	s := &playoutTestSink{}
	ts := ^uint32(0) - 959 // wraps at the next 20 ms packet
	for i := 0; i < 10; i++ {
		pcm := make([]float32, 320)
		for j := range pcm {
			pcm[j] = float32(i*320 + j)
		}
		if _, err := p.Push(ts, pcm, s); err != nil {
			t.Fatal(err)
		}
		ts += 960
	}
	if len(s.frames) != 3 {
		t.Fatalf("got %d framed writes, want 3", len(s.frames))
	}
	n := 0
	for _, frame := range s.frames {
		if len(frame) != 960 {
			t.Fatal("invalid bridge frame")
		}
		for _, x := range frame {
			if x != float32(n) {
				t.Fatal("clock alignment failed", n, x)
			}
			n++
		}
	}
	p.reset(ts, make([]float32, 320))
	if len(p.partialPCM) != 0 {
		t.Fatal("reset retained partial PCM")
	}
}

type packetizerTestSource struct{ generation uint64 }

func (s *packetizerTestSource) AudioFrameGeneration() uint64 { return s.generation }
func (*packetizerTestSource) ReadFrame() ([]float32, error) {
	return make([]float32, FrameSamples), nil
}
func (*packetizerTestSource) Close() error { return nil }

func TestOpusPacketizerSourceGeneration(t *testing.T) {
	player := NewPlayer()
	source := &packetizerTestSource{}
	player.Play(source)
	before := opusGeneration(player)
	if before != opusGeneration(player) {
		t.Fatal("unstable unchanged generation")
	}
	source.generation++
	muted := opusGeneration(player)
	if muted == before {
		t.Fatal("source mute generation ignored")
	}
	player.Pause()
	if opusGeneration(player) == muted {
		t.Fatal("player pause ignored")
	}
	player.Play(&packetizerTestSource{})
	if opusGeneration(player) == before {
		t.Fatal("replacement source reused old generation")
	}
}
