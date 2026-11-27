package voipbridge

import (
	"context"
	"errors"
	"io"
	"math"
	"sync"
	"sync/atomic"

	"go.mau.fi/mautrix-whatsapp/pkg/connector/voipbridge/rtcwire"
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
	fromWhatsAppAudio            chan []float32
	toWhatsAppAudio              chan []float32
	fromWhatsAppVideo            chan EncodedVideoFrame
	audioMu                      sync.Mutex
	audioMuted                   bool
	audioGeneration              atomic.Uint64
	videoResync                  atomic.Bool
	videoOrientation             atomic.Int32
	closed                       chan struct{}
	admissionMu                  sync.RWMutex
	closeOnce                    sync.Once
	participantMedia             map[ParticipantMediaKey]*ParticipantMediaTrack
	participantRosterTransaction uint32
	participantEpochRoster       uint32
	participantEpochValid        bool
	participantKeyTransaction    uint32
	participantObservedRoster    uint32
	participantRoster            map[string]ParticipantRosterDevice
	participantEpochRevision     uint64
	participantEpochChanged      chan struct{}
	participantSources           map[ParticipantMediaKey]struct{}
	participantSourcesChanged    chan struct{}
}

func NewMediaBus(capacity int) *MediaBus {
	if capacity < 1 {
		capacity = DefaultJitterFrames
	}
	return &MediaBus{
		fromWhatsAppAudio:         make(chan []float32, capacity),
		toWhatsAppAudio:           make(chan []float32, capacity),
		fromWhatsAppVideo:         make(chan EncodedVideoFrame, capacity),
		closed:                    make(chan struct{}),
		participantEpochChanged:   make(chan struct{}),
		participantSourcesChanged: make(chan struct{}),
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
	m.admissionMu.RLock()
	defer m.admissionMu.RUnlock()
	select {
	case <-m.closed:
		return io.ErrClosedPipe
	default:
	}
	if len(frame) != AudioFrameSamples {
		return ErrBadFrame
	}
	// Invalid decoder output was previously discarded by the wire pump.
	// Drop it here instead, before it can evict valid queued audio. Keep
	// the sink running (including the upstream playout prefill).
	for _, sample := range frame {
		if math.IsNaN(float64(sample)) || sample < -1 || sample > 1 {
			return nil
		}
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

// RequestVideoResync invalidates the queued dependency chain before the next IDR.
// Serialize with video admission so an old producer cannot refill the drained queue.
func (m *MediaBus) RequestVideoResync() {
	m.admissionMu.Lock()
	defer m.admissionMu.Unlock()
	m.videoResync.Store(true)
	drainMediaQueue(m.fromWhatsAppVideo)
}
func (m *MediaBus) TakeVideoResync() bool { return m.videoResync.Swap(false) }

// A truncated GOP is unusable. Discard the queued dependency chain and wait for a fresh IDR.
func (m *MediaBus) TryVideo(frame EncodedVideoFrame) bool {
	m.admissionMu.Lock()
	defer m.admissionMu.Unlock()
	return m.tryVideoLocked(frame)
}

// tryVideoLocked is shared by both video admission paths. Unlike PCM, video
// frames cannot be individually evicted without invalidating later frames.
func (m *MediaBus) tryVideoLocked(frame EncodedVideoFrame) bool {
	// A new request may arrive after the selector checked TakeVideoResync.
	// Reject that frame and let the selector observe the new request.
	if m.videoResync.Load() {
		return false
	}
	select {
	case <-m.closed:
		return false
	default:
	}
	// All writers hold admissionMu exclusively, so only a consumer can
	// change occupancy here. Avoid copying a frame we will drop.
	// A frame that cannot fit on the wire is also a dependency loss.
	// Refuse it before copying into the queue, leaving audio unaffected.
	if len(frame.Data) > rtcwire.MaxQueuedVideoBytes || len(m.fromWhatsAppVideo) == cap(m.fromWhatsAppVideo) {
		m.videoResync.Store(true)
		drainMediaQueue(m.fromWhatsAppVideo)
		return false
	}
	frame.Data = append([]byte(nil), frame.Data...)
	m.fromWhatsAppVideo <- frame
	return true
}

func (m *MediaBus) WriteVideo(accessUnit []byte) error {
	m.admissionMu.Lock()
	defer m.admissionMu.Unlock()
	select {
	case <-m.closed:
		return io.ErrClosedPipe
	default:
	}
	if len(accessUnit) == 0 {
		return ErrBadFrame
	}
	frame := EncodedVideoFrame{
		Data:        accessUnit,
		Orientation: int(m.videoOrientation.Load()),
	}
	// Drop a frame while resynchronising, rather than retaining a partial GOP.
	m.tryVideoLocked(frame)
	return nil
}

// ReadFrame supplies decoded MatrixRTC audio (after resampling) to WhatsApp.
// Meowcaller invokes this on its paced RTP send loop: it MUST NOT block while
// waiting for Matrix audio or it would stall outgoing RTP and incoming processing.
// Missing frames become silence instead of unbounded queue latency.
func (m *MediaBus) SetAudioMuted(muted bool) {
	m.audioMu.Lock()
	defer m.audioMu.Unlock()
	if m.audioMuted != muted {
		m.audioMuted = muted
		m.audioGeneration.Add(1)
	}
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

// AudioFrameGeneration invalidates any pre-read PCM held by a codec packetizer
// after a microphone mute or unmute transition. It does not revoke a packet
// already handed to the transport.
func (m *MediaBus) AudioFrameGeneration() uint64 { return m.audioGeneration.Load() }

func (m *MediaBus) ReadFrame() ([]float32, error) {
	m.admissionMu.RLock()
	defer m.admissionMu.RUnlock()
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
	m.admissionMu.RLock()
	defer m.admissionMu.RUnlock()
	select {
	case <-m.closed:
		return io.ErrClosedPipe
	default:
	}
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
	if err := ctx.Err(); err != nil {
		return nil, err
	}
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
		select {
		case <-m.closed:
			return nil, io.EOF
		default:
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			return frame, nil
		}
	}
}

func (m *MediaBus) NextVideoFromWhatsApp(ctx context.Context) (EncodedVideoFrame, error) {
	if err := ctx.Err(); err != nil {
		return EncodedVideoFrame{}, err
	}
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
		select {
		case <-m.closed:
			return EncodedVideoFrame{}, io.EOF
		default:
			if err := ctx.Err(); err != nil {
				return EncodedVideoFrame{}, err
			}
			return frame, nil
		}
	}
}

// Close unblocks pending media readers and guarantees late writers cannot panic.
func (m *MediaBus) Close() error {
	m.closeOnce.Do(func() {
		m.admissionMu.Lock()
		defer m.admissionMu.Unlock()
		close(m.closed)
		m.participantRoster = nil
		// Producers cannot enter after this point. Drop retained media rather
		// than leaving a closed bus holding the final audio/video frames.
		drainMediaQueue(m.fromWhatsAppAudio)
		drainMediaQueue(m.toWhatsAppAudio)
		drainMediaQueue(m.fromWhatsAppVideo)
		m.revokeParticipantMediaLocked()
	})
	return nil
}

func drainMediaQueue[T any](queue chan T) {
	for {
		select {
		case <-queue:
		default:
			return
		}
	}
}
