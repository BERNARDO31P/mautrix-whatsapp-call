package meowcaller

import (
	"io"
	"sync/atomic"
	"testing"
	"time"
)

type generationAudioSource struct {
	entered chan struct{}
	release chan struct{}
	frame   []float32
	err     error
	closes  atomic.Int32
}

func (s *generationAudioSource) ReadFrame() ([]float32, error) {
	if s.entered != nil {
		close(s.entered)
		<-s.release
	}
	return s.frame, s.err
}
func (s *generationAudioSource) Close() error { s.closes.Add(1); return nil }

func TestPlayerRejectsStaleRead(t *testing.T) {
	for _, action := range []string{"pause", "pause-resume", "stop", "replace", "replace-eof", "stop-eof"} {
		t.Run(action, func(t *testing.T) {
			p := NewPlayer()
			old := &generationAudioSource{entered: make(chan struct{}), release: make(chan struct{}), frame: []float32{1}}
			replacement := &generationAudioSource{frame: []float32{2}}
			if action == "replace-eof" || action == "stop-eof" {
				old.err = io.EOF
			}
			var finished atomic.Int32
			p.OnFinish(func() { finished.Add(1) })
			p.Play(old)
			result := make(chan []float32, 1)
			go func() { result <- p.nextFrame() }()
			select {
			case <-old.entered:
			case <-time.After(time.Second):
				t.Fatal("read did not start")
			}
			switch action {
			case "pause":
				p.Pause()
			case "pause-resume":
				p.Pause()
				p.Resume()
			case "stop", "stop-eof":
				p.Stop()
			case "replace", "replace-eof":
				p.Play(replacement)
			}
			close(old.release)
			select {
			case frame := <-result:
				if frame != nil {
					t.Fatalf("stale frame admitted: %v", frame)
				}
			case <-time.After(time.Second):
				t.Fatal("read did not finish")
			}
			if finished.Load() != 0 {
				t.Fatal("stale completion callback")
			}
			if action == "replace" || action == "replace-eof" {
				if frame := p.nextFrame(); len(frame) != 1 || frame[0] != 2 {
					t.Fatalf("replacement lost: %v", frame)
				}
			}
			p.Stop()
			if old.closes.Load() != 1 {
				t.Fatalf("old source closed %d times", old.closes.Load())
			}
		})
	}
}

func TestPlayerCurrentFinalFrame(t *testing.T) {
	p := NewPlayer()
	source := &generationAudioSource{frame: []float32{3}, err: io.EOF}
	var finished int
	p.OnFinish(func() { finished++ })
	p.Play(source)
	if frame := p.nextFrame(); len(frame) != 1 || frame[0] != 3 {
		t.Fatalf("final frame lost: %v", frame)
	}
	if p.State() != PlayerIdle || finished != 1 || source.closes.Load() != 1 {
		t.Fatal("EOF ownership not released exactly once")
	}
	if p.nextFrame() != nil {
		t.Fatal("idle player emitted audio")
	}
}
