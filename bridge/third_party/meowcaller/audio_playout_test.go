package meowcaller

import (
	"errors"
	"testing"
)

type playoutTestSink struct {
	frames [][]float32
}

func (s *playoutTestSink) WriteFrame(frame []float32) error {
	clone := append([]float32(nil), frame...)
	s.frames = append(s.frames, clone)
	return nil
}

func (s *playoutTestSink) Close() error { return nil }

func TestAudioPlayoutPadsShortFrameToCodecInterval(t *testing.T) {
	playout := newAudioPlayoutBuffer()
	sink := &playoutTestSink{}
	first := make([]float32, FrameSamples)
	short := make([]float32, 160)
	current := make([]float32, FrameSamples)
	for i := range short {
		short[i] = 0.25
	}

	if started, err := playout.Push(0, first, sink); err != nil || started {
		t.Fatalf("first push = (%v, %v), want (false, nil)", started, err)
	}
	if started, err := playout.Push(FrameSamples, short, sink); err != nil || started {
		t.Fatalf("second push = (%v, %v), want (false, nil)", started, err)
	}
	started, err := playout.Push(3*FrameSamples, current, sink)
	if err != nil || !started {
		t.Fatalf("third push = (%v, %v), want (true, nil)", started, err)
	}
	if len(sink.frames) != 2 {
		t.Fatalf("writes = %d, want 2", len(sink.frames))
	}
	if len(sink.frames[0]) != FrameSamples || len(sink.frames[1]) != FrameSamples {
		t.Fatalf("write lengths = [%d %d], want [%d %d]", len(sink.frames[0]), len(sink.frames[1]), FrameSamples, FrameSamples)
	}
	for i, sample := range sink.frames[1] {
		want := float32(0)
		if i < len(short) {
			want = 0.25
		}
		if sample != want {
			t.Fatalf("padded frame[%d] = %f, want %f", i, sample, want)
		}
	}

	if started, err = playout.Push(4*FrameSamples, current, sink); err != nil || started {
		t.Fatalf("steady push = (%v, %v), want (false, nil)", started, err)
	}
	if len(sink.frames) != 3 || len(sink.frames[2]) != FrameSamples {
		t.Fatalf("steady writes = %v, want third %d-sample frame", frameLengths(sink.frames), FrameSamples)
	}
}

func TestAudioPlayoutDoesNotReplayElapsedGap(t *testing.T) {
	playout := newAudioPlayoutBuffer()
	sink := &playoutTestSink{}
	frame := make([]float32, FrameSamples)

	if _, err := playout.Push(0, frame, sink); err != nil {
		t.Fatal(err)
	}
	if _, err := playout.Push(FrameSamples, frame, sink); err != nil {
		t.Fatal(err)
	}
	started, err := playout.Push(8*FrameSamples, frame, sink)
	if err != nil || !started {
		t.Fatalf("gap push = (%v, %v), want (true, nil)", started, err)
	}
	if got := frameLengths(sink.frames); len(got) != 2 || got[0] != FrameSamples || got[1] != FrameSamples {
		t.Fatalf("write lengths = %v, want [%d %d]", got, FrameSamples, FrameSamples)
	}
}

func TestAlignAudioFrameKeepsNaturalMultiFrameOutput(t *testing.T) {
	frame := make([]float32, 2*FrameSamples)
	aligned := alignAudioFrame(frame, 8*FrameSamples)
	if len(aligned) != len(frame) {
		t.Fatalf("aligned length = %d, want natural length %d", len(aligned), len(frame))
	}
}

func TestAudioPlayoutHandlesTimestampWrap(t *testing.T) {
	playout := newAudioPlayoutBuffer()
	sink := &playoutTestSink{}
	start := ^uint32(0) - uint32(FrameSamples-1)
	if _, err := playout.Push(start, make([]float32, FrameSamples), sink); err != nil {
		t.Fatal(err)
	}
	if _, err := playout.Push(0, make([]float32, FrameSamples), sink); err != nil {
		t.Fatal(err)
	}
	started, err := playout.Push(FrameSamples, make([]float32, FrameSamples), sink)
	if err != nil || !started {
		t.Fatalf("wrap push = (%v, %v), want (true, nil)", started, err)
	}
}

func TestAudioPlayoutResetsOnTimestampDiscontinuity(t *testing.T) {
	playout := newAudioPlayoutBuffer()
	sink := &playoutTestSink{}
	for _, timestamp := range []uint32{0, FrameSamples, 2 * FrameSamples} {
		if _, err := playout.Push(timestamp, make([]float32, FrameSamples), sink); err != nil {
			t.Fatal(err)
		}
	}
	writes := len(sink.frames)
	started, err := playout.Push(2*FrameSamples+audioPlayoutMaxGapSamples+1, make([]float32, FrameSamples), sink)
	if err != nil || started {
		t.Fatalf("discontinuity push = (%v, %v), want (false, nil)", started, err)
	}
	if playout.started || len(sink.frames) != writes {
		t.Fatal("timestamp discontinuity did not reset prefill state")
	}
}

func TestAudioPlayoutDrainHandsOffBufferedDirectFrames(t *testing.T) {
	playout := newAudioPlayoutBuffer()
	sink := &playoutTestSink{}
	first := constantPCM(FrameSamples, 0.2)
	second := constantPCM(FrameSamples, 0.3)
	if _, err := playout.Push(0, first, sink); err != nil {
		t.Fatal(err)
	}
	if _, err := playout.Push(FrameSamples, second, sink); err != nil {
		t.Fatal(err)
	}
	if len(sink.frames) != 0 {
		t.Fatalf("prefill unexpectedly wrote %d frames", len(sink.frames))
	}
	if err := playout.Drain(sink); err != nil {
		t.Fatalf("drain: %v", err)
	}
	if len(sink.frames) != 2 || len(sink.frames[0]) != FrameSamples || len(sink.frames[1]) != FrameSamples {
		t.Fatalf("drained frame lengths = %v, want [%d %d]", frameLengths(sink.frames), FrameSamples, FrameSamples)
	}
	if sink.frames[0][0] != 0.2 || sink.frames[1][0] != 0.3 {
		t.Fatalf("drained frame starts = [%f %f], want [0.2 0.3]", sink.frames[0][0], sink.frames[1][0])
	}
	if playout.pending != nil || len(playout.prefill) != 0 || playout.prefillSamples != 0 || playout.started {
		t.Fatal("drain left direct playout state behind")
	}
}

func frameLengths(frames [][]float32) []int {
	lengths := make([]int, len(frames))
	for i, frame := range frames {
		lengths[i] = len(frame)
	}
	return lengths
}

func TestAudioPlayoutDuplicateDoesNotResetPrefillOrReplacePCM(t *testing.T) {
	p := newAudioPlayoutBuffer()
	s := &playoutTestSink{}
	for _, ts := range []uint32{0, FrameSamples} {
		if _, err := p.Push(ts, constantPCM(FrameSamples, 0.25), s); err != nil {
			t.Fatal(err)
		}
	}
	if started, err := p.Push(FrameSamples, constantPCM(FrameSamples, 0.9), s); started || err != nil {
		t.Fatal("duplicate changed prefill")
	}
	if started, err := p.Push(2*FrameSamples, constantPCM(FrameSamples, 0.25), s); !started || err != nil {
		t.Fatal("prefill was reset")
	}
	if started, err := p.Push(2*FrameSamples, constantPCM(FrameSamples, 0.9), s); started || err != nil {
		t.Fatal("duplicate changed steady playout")
	}
	if _, err := p.Push(3*FrameSamples, constantPCM(FrameSamples, 0.25), s); err != nil {
		t.Fatal(err)
	}
	if !p.started || len(s.frames) != 3 {
		t.Fatal("duplicate altered sink writes")
	}
	for _, frame := range s.frames {
		if frame[0] != 0.25 {
			t.Fatal("duplicate replaced original PCM")
		}
	}
}

// MLow timestamp deltas need not match the 60 ms AudioSink frame size.
func TestMLowPlayoutFramesVariableTimestampIntervals(t *testing.T) {
	p := newAudioPlayoutBuffer()
	p.clockScale = 1
	s := &playoutTestSink{}
	timestamp := uint32(0)
	offset := 0
	for _, samples := range []int{320, 640, 1920, 480, 480, 960, 960} {
		pcm := make([]float32, samples)
		for i := range pcm {
			pcm[i] = float32(offset + i)
		}
		if _, err := p.Push(timestamp, pcm, s); err != nil {
			t.Fatal(err)
		}
		timestamp += uint32(samples)
		offset += samples
	}
	if err := p.Flush(s); err != nil {
		t.Fatal(err)
	}
	if len(s.frames) != offset/FrameSamples {
		t.Fatalf("frames = %d, want %d", len(s.frames), offset/FrameSamples)
	}
	n := 0
	for _, frame := range s.frames {
		if len(frame) != FrameSamples {
			t.Fatalf("invalid sink frame length %d", len(frame))
		}
		for _, sample := range frame {
			if sample != float32(n) {
				t.Fatalf("sample %d = %f", n, sample)
			}
			n++
		}
	}
	if len(p.partialPCM) != offset%FrameSamples {
		t.Fatal("incorrect PCM remainder")
	}
}

type failingPlayoutSink struct {
	playoutTestSink
	fail bool
}

func (s *failingPlayoutSink) WriteFrame(frame []float32) error {
	if s.fail {
		s.fail = false
		return errors.New("sink rejected frame")
	}
	return s.playoutTestSink.WriteFrame(frame)
}

func TestAudioPlayoutDoesNotReplayFailedPrefill(t *testing.T) {
	p := newAudioPlayoutBuffer()
	s := &failingPlayoutSink{fail: true}
	for i := 0; i < 3; i++ {
		_, err := p.Push(uint32(i*FrameSamples), constantPCM(FrameSamples, float32(i)), s)
		if (err != nil) != (i == 2) {
			t.Fatalf("push %d: %v", i, err)
		}
	}
	for i := 3; i < 5; i++ {
		if _, err := p.Push(uint32(i*FrameSamples), constantPCM(FrameSamples, float32(i)), s); err != nil {
			t.Fatal(err)
		}
	}
	if len(s.frames) != 2 || s.frames[0][0] != 2 || s.frames[1][0] != 3 {
		t.Fatal("failed prefill was replayed")
	}
}

func TestAudioPlayoutLatePacketsPreservePrefillAndSteadyState(t *testing.T) {
	for _, scale := range []uint32{1, 3} {
		for _, origin := range []uint32{0, ^uint32(0) - uint32(FrameSamples)*scale + 1} {
			p := newAudioPlayoutBuffer()
			p.clockScale = scale
			s := &playoutTestSink{}
			step := uint32(FrameSamples) * scale
			push := func(ts uint32, value float32) bool {
				t.Helper()
				started, err := p.Push(ts, constantPCM(FrameSamples, value), s)
				if err != nil {
					t.Fatal(err)
				}
				return started
			}
			push(origin, 0.25)
			push(origin+step, 0.25)
			if push(origin, 0.9) || p.pending.timestamp != origin+step {
				t.Fatal("late packet replaced pending PCM")
			}
			if !push(origin+2*step, 0.25) {
				t.Fatal("late packet reset prefill")
			}
			if push(origin+step, 0.9) || !p.started {
				t.Fatal("late packet reset steady playout")
			}
			push(origin+3*step, 0.25)
			if len(s.frames) != 3 {
				t.Fatalf("writes = %d", len(s.frames))
			}
			for _, frame := range s.frames {
				if frame[0] != 0.25 {
					t.Fatal("late PCM was replayed")
				}
			}
		}
	}
}

func TestAudioPlayoutLargeBackwardDiscontinuityStillResets(t *testing.T) {
	p := newAudioPlayoutBuffer()
	s := &playoutTestSink{}
	start := uint32(4 * audioPlayoutMaxGapSamples)
	for i := uint32(0); i < 3; i++ {
		if _, err := p.Push(start+i*FrameSamples, constantPCM(FrameSamples, 0.25), s); err != nil {
			t.Fatal(err)
		}
	}
	if !p.started {
		t.Fatal("prefill did not start")
	}
	if started, err := p.Push(0, constantPCM(FrameSamples, 0.5), s); err != nil || started || p.started || p.pending.timestamp != 0 {
		t.Fatal("large backward discontinuity did not reset", err)
	}
}
