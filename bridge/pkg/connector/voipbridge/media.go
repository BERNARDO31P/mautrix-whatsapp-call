package voipbridge

import (
	"context"
	"errors"
	"io"
	"sync"
	"sync/atomic"
)

// WhatsApp's MLow decoder and encoder exchange 960 mono float32 samples
// at 16 kHz (60 ms/frame). MatrixRTC's audio is usually 48 kHz Opus, so the
// eventual RTC adapter must resample, encode/decode and maintain a media clock.
const (
	AudioSampleRate     = 16000
	AudioFrameSamples   = 960
	DefaultJitterFrames = 6
)

var ErrBadFrame = errors.New("voipbridge: invalid audio/video frame")

// MediaBus is a bounded, memory-only handoff between a meowcaller call and a
// future MatrixRTC transport. No MatrixRTC encryption/signalling occurs here.
// WriteFrame/WriteVideo are called by the WhatsApp receive media loop;
// ReadFrame is called by the WhatsApp send media loop.
// Each operation makes an ownership-safe copy of its input before enqueuing.
type EncodedVideoFrame struct {
	Timestamp   uint64
	Generation  uint64
	Data        []byte
	Orientation int
}

type MediaBus struct {
	fromWhatsAppAudio chan []float32
	toWhatsAppAudio   chan []float32
	fromWhatsAppVideo chan EncodedVideoFrame
	audioMu           sync.Mutex
	audioMuted        bool
	videoResync       atomic.Bool
	videoOrientation  atomic.Int32
	closed            chan struct{}
	closeOnce         sync.Once
}

func NewMediaBus(capacity int) *MediaBus {
	if capacity < 1 {
		capacity = DefaultJitterFrames
	}
	return &MediaBus{
		fromWhatsAppAudio: make(chan []float32, capacity),
		toWhatsAppAudio:   make(chan []float32, capacity),
		fromWhatsAppVideo: make(chan EncodedVideoFrame, capacity),
		closed:            make(chan struct{}),
	}
}

// enqueueLatest drops an old frame if the consumer cannot keep up: blocking a
// real-time receive loop on an unrelated transport would accumulate latency.
func enqueueLatest[T any](queue chan T, item T, done <-chan struct{}) error {
	select {
	case <-done:
		return io.ErrClosedPipe
	default:
	}
	select {
	case queue <- item:
		return nil
	default:
	}
	select {
	case <-queue:
	default:
	}
	select {
	case <-done:
		return io.ErrClosedPipe
	case queue <- item:
		return nil
	default:
		return nil // competing writers already filled the queue
	}
}

func (m *MediaBus) WriteFrame(frame []float32) error {
	if len(frame) != AudioFrameSamples {
		return ErrBadFrame
	}
	return enqueueLatest(m.fromWhatsAppAudio, append([]float32(nil), frame...), m.closed)
}

// SetOrientation implements meowcaller's VideoOrientationSink. The orientation
// is clockwise quarter turns and is sampled when each encoded access unit is
// enqueued, so the local RTC wire preserves the peer's display metadata.
func (m *MediaBus) SetOrientation(orientation int) {
	if orientation < 0 || orientation > 3 {
		return
	}
	m.videoOrientation.Store(int32(orientation))
}

func (m *MediaBus) RequestVideoResync()   { m.videoResync.Store(true) }
func (m *MediaBus) TakeVideoResync() bool { return m.videoResync.Swap(false) }

// A truncated GOP is unusable. Discard the queued dependency chain and wait for a fresh IDR.
func (m *MediaBus) TryVideo(frame EncodedVideoFrame) bool {
	select {
	case <-m.closed:
		return false
	case m.fromWhatsAppVideo <- frame:
		return true
	default:
	}
	for {
		select {
		case <-m.fromWhatsAppVideo:
		default:
			return false
		}
	}
}

func (m *MediaBus) WriteVideo(accessUnit []byte) error {
	if len(accessUnit) == 0 {
		return ErrBadFrame
	}
	frame := EncodedVideoFrame{
		Data:        append([]byte(nil), accessUnit...),
		Orientation: int(m.videoOrientation.Load()),
	}
	return enqueueLatest(m.fromWhatsAppVideo, frame, m.closed)
}

// ReadFrame supplies decoded MatrixRTC audio (after resampling) to WhatsApp.
// Meowcaller invokes this on its paced RTP send loop: it MUST NOT block while
// waiting for Matrix audio or it would stall outgoing RTP and incoming processing.
// Missing frames become silence instead of unbounded queue latency.
func (m *MediaBus) SetAudioMuted(muted bool) {
	m.audioMu.Lock()
	defer m.audioMu.Unlock()
	m.audioMuted = muted
	if muted {
		for {
			select {
			case <-m.toWhatsAppAudio:
			default:
				return
			}
		}
	}
}

func (m *MediaBus) ReadFrame() ([]float32, error) {
	m.audioMu.Lock()
	defer m.audioMu.Unlock()
	select {
	case <-m.closed:
		return nil, io.EOF
	default:
	}
	select {
	case <-m.closed:
		return nil, io.EOF
	case frame := <-m.toWhatsAppAudio:
		return frame, nil
	default:
		return make([]float32, AudioFrameSamples), nil
	}
}

func (m *MediaBus) SendAudioToWhatsApp(frame []float32) error {
	if len(frame) != AudioFrameSamples {
		return ErrBadFrame
	}
	m.audioMu.Lock()
	defer m.audioMu.Unlock()
	if m.audioMuted {
		return nil
	}
	return enqueueLatest(m.toWhatsAppAudio, append([]float32(nil), frame...), m.closed)
}

func (m *MediaBus) NextAudioFromWhatsApp(ctx context.Context) ([]float32, error) {
	select {
	case <-m.closed:
		return nil, io.EOF
	default:
	}
	select {
	case <-m.closed:
		return nil, io.EOF
	case <-ctx.Done():
		return nil, ctx.Err()
	case frame := <-m.fromWhatsAppAudio:
		return frame, nil
	}
}

func (m *MediaBus) NextVideoFromWhatsApp(ctx context.Context) (EncodedVideoFrame, error) {
	select {
	case <-m.closed:
		return EncodedVideoFrame{}, io.EOF
	default:
	}
	select {
	case <-m.closed:
		return EncodedVideoFrame{}, io.EOF
	case <-ctx.Done():
		return EncodedVideoFrame{}, ctx.Err()
	case frame := <-m.fromWhatsAppVideo:
		return frame, nil
	}
}

// Close unblocks pending media readers and guarantees late writers cannot panic.
func (m *MediaBus) Close() error {
	m.closeOnce.Do(func() { close(m.closed) })
	return nil
}
