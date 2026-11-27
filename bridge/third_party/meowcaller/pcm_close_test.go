package meowcaller

import (
	"bytes"
	"errors"
	"io"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type observedPCMReader struct {
	*io.PipeReader
	started chan struct{}
	once    sync.Once
	closes  atomic.Int32
}

func (r *observedPCMReader) Read(p []byte) (int, error) {
	r.once.Do(func() { close(r.started) })
	return r.PipeReader.Read(p)
}
func (r *observedPCMReader) Close() error { r.closes.Add(1); return r.PipeReader.Close() }

func TestPCMCloseInterruptsBlockedRead(t *testing.T) {
	r, w := io.Pipe()
	defer w.Close()
	observed := &observedPCMReader{PipeReader: r, started: make(chan struct{})}
	source := PCMStream(observed)
	result := make(chan error, 1)
	go func() {
		frame, err := source.ReadFrame()
		if frame != nil {
			result <- errors.New("unexpected frame after closure")
			return
		}
		result <- err
	}()
	select {
	case <-observed.started:
	case <-time.After(time.Second):
		t.Fatal("read never started")
	}
	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := source.Close(); err != nil {
				t.Error(err)
			}
		}()
	}
	wg.Wait()
	select {
	case err := <-result:
		if !errors.Is(err, errSourceClosed) {
			t.Fatalf("unexpected read result: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("close did not unblock read")
	}
	if observed.closes.Load() != 1 {
		t.Fatal("underlying reader closed more than once")
	}
	if _, err := source.ReadFrame(); !errors.Is(err, errSourceClosed) {
		t.Fatalf("unexpected post-close result: %v", err)
	}
}

func TestPCMConcurrentReadsAndPadding(t *testing.T) {
	data := make([]byte, FrameSamples*2+2)
	data[0] = 1
	data[FrameSamples*2] = 2
	source := PCMStream(io.NopCloser(bytes.NewReader(data)))
	defer source.Close()
	frames := make(chan []float32, 2)
	var wg sync.WaitGroup
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			frame, err := source.ReadFrame()
			if err != nil {
				t.Error(err)
			}
			frames <- frame
		}()
	}
	wg.Wait()
	close(frames)
	seen := map[float32]bool{}
	for f := range frames {
		if len(f) != FrameSamples {
			t.Fatal("incorrect frame length")
		}
		seen[f[0]] = true
		for _, v := range f[1:] {
			if v != 0 {
				t.Fatal("padding or frame corruption")
			}
		}
	}
	if !seen[1.0/32768] || !seen[2.0/32768] {
		t.Fatal("lost or duplicated frame")
	}
	if _, err := source.ReadFrame(); err != io.EOF {
		t.Fatalf("expected EOF, got %v", err)
	}
}
