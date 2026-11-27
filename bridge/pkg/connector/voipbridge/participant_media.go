package voipbridge

import (
	"bytes"
	"context"
	"io"
	"math"

	"go.mau.fi/mautrix-whatsapp/pkg/connector/voipbridge/rtcwire"
)

// ParticipantMediaKey identifies one authenticated device and RTP source. Audio
// and video keys are distinct even if their numeric SSRCs happen to coincide.
// A future Matrix publisher must authorize the exact device against its roster.
type ParticipantMediaKey struct {
	ParticipantID string
	Device        string
	SSRC          uint32
	Video         bool
}

type ParticipantMediaPacket struct {
	Timestamp     uint32
	ClockRate     uint32
	PCM           []float32
	H264          []byte
	Orientation   int
	Discontinuity bool
	ScreenShare   bool
	RoleRevision  uint64
}

// ParticipantMediaTrack is an opt-in, single-consumer ingress queue. Its owner
// must close the subscription on roster departure or a key epoch transition.
// Video consumers must reset their decoder on Discontinuity and wait for a
// complete independently decodable access unit (including parameter sets).
type ParticipantMediaTrack struct {
	generation        uint64
	queue             chan ParticipantMediaPacket
	done              chan struct{}
	discontinuity     bool // protected by parent admissionMu
	keyframeRequested bool // protected by parent admissionMu
	screenShare       bool // last queued role, protected by parent admissionMu
	roleRevision      uint64
}

const maxParticipantMediaTracks = 64
const participantMediaQueueSize = 3

// SubscribeParticipantMedia admits only an exact device from the latest
// authoritative roster. Matrix room/ghost authorization is a separate gate.
func (m *MediaBus) SubscribeParticipantMedia(key ParticipantMediaKey) (*ParticipantMediaTrack, error) {
	return m.subscribeParticipantMedia(key, 0, 0)
}

func (m *MediaBus) subscribeParticipantMedia(key ParticipantMediaKey, generation uint64, revision uint64) (*ParticipantMediaTrack, error) {
	if key.ParticipantID == "" || len(key.ParticipantID) > 512 || key.Device == "" || len(key.Device) > 512 || key.SSRC == 0 {
		return nil, ErrBadFrame
	}
	m.admissionMu.Lock()
	defer m.admissionMu.Unlock()
	select {
	case <-m.closed:
		return nil, io.ErrClosedPipe
	default:
	}
	device, ok := m.participantRoster[key.Device]
	if !ok || device.ParticipantID != key.ParticipantID || !m.participantEpochValid || m.participantEpochRoster != m.participantRosterTransaction || m.participantObservedRoster != m.participantRosterTransaction {
		return nil, ErrBadFrame
	}
	current := uint64(m.participantRosterTransaction)<<32 | uint64(m.participantKeyTransaction)
	if _, observed := m.participantSources[key]; !observed || (generation != 0 && (generation != current || revision != m.participantEpochRevision)) {
		return nil, ErrBadFrame
	}
	if m.participantMedia[key] != nil || len(m.participantMedia) >= maxParticipantMediaTracks {
		return nil, ErrBadFrame
	}
	if m.participantMedia == nil {
		m.participantMedia = make(map[ParticipantMediaKey]*ParticipantMediaTrack)
	}
	track := &ParticipantMediaTrack{generation: uint64(m.participantRosterTransaction)<<32 | uint64(m.participantKeyTransaction), queue: make(chan ParticipantMediaPacket, participantMediaQueueSize), done: make(chan struct{}), discontinuity: true}
	m.participantMedia[key] = track
	return track, nil
}

// Unsubscribe fences in-flight writers and discards retained media. The handle
// check prevents a stale owner from removing a replacement subscription.
func (m *MediaBus) UnsubscribeParticipantMedia(key ParticipantMediaKey, track *ParticipantMediaTrack) {
	m.admissionMu.Lock()
	defer m.admissionMu.Unlock()
	if track != nil && m.participantMedia[key] == track {
		delete(m.participantMedia, key)
		close(track.done)
		drainMediaQueue(track.queue)
	}
}

func (t *ParticipantMediaTrack) Next(ctx context.Context) (ParticipantMediaPacket, error) {
	if err := ctx.Err(); err != nil {
		return ParticipantMediaPacket{}, err
	}
	select {
	case <-t.done:
		return ParticipantMediaPacket{}, io.EOF
	case <-ctx.Done():
		return ParticipantMediaPacket{}, ctx.Err()
	case packet := <-t.queue:
		select {
		case <-t.done:
			return ParticipantMediaPacket{}, io.EOF
		default:
		}
		if err := ctx.Err(); err != nil {
			return ParticipantMediaPacket{}, err
		}
		return packet, nil
	}
}

// PublishParticipantMedia returns true when a subscribed video source needs a
// new keyframe. The caller requests it outside the bus lock. Frames are copied
// only for subscribed sources, and pressure on one source cannot evict another.
func (m *MediaBus) PublishParticipantMedia(transaction uint32, keyTransaction uint32, pid uint32, hasPID bool, key ParticipantMediaKey, packet ParticipantMediaPacket) (requestKeyframe bool) {
	m.admissionMu.Lock()
	defer m.admissionMu.Unlock()
	device, ok := m.participantRoster[key.Device]
	if !hasPID || !m.participantEpochValid || transaction != m.participantRosterTransaction || transaction != m.participantEpochRoster || keyTransaction != m.participantKeyTransaction || !ok || device.PID != pid || device.ParticipantID != key.ParticipantID {
		return false
	}
	track := m.participantMedia[key]
	if key.SSRC == 0 {
		return false
	}
	if key.Video {
		if len(packet.H264) <= 3 || (!bytes.HasPrefix(packet.H264, []byte{0, 0, 1}) && !bytes.HasPrefix(packet.H264, []byte{0, 0, 0, 1})) || len(packet.H264) > 1024*1024 || len(packet.PCM) != 0 || packet.Orientation < 0 || packet.Orientation > 3 || packet.ClockRate != 90000 {
			if track != nil {
				drainMediaQueue(track.queue)
				track.discontinuity = true
				return true
			}
			return false
		}
	} else {
		if len(packet.PCM) == 0 || len(packet.PCM) > 1920 || len(packet.H264) != 0 || (packet.ClockRate != 16000 && packet.ClockRate != 48000) || packet.Orientation != 0 || packet.ScreenShare {
			return false
		}
		for _, sample := range packet.PCM {
			if math.IsNaN(float64(sample)) || sample < -1 || sample > 1 {
				return false
			}
		}
	}
	m.participantObservedRoster = transaction
	if !m.observeParticipantSourceLocked(key) || track == nil {
		return false
	}
	if key.Video && packet.RoleRevision < track.roleRevision {
		return false
	}
	if key.Video && (track.screenShare != packet.ScreenShare || track.roleRevision != packet.RoleRevision) {
		drainMediaQueue(track.queue)
		track.discontinuity = true
		track.screenShare = packet.ScreenShare
		track.roleRevision = packet.RoleRevision
	}
	if len(track.queue) == cap(track.queue) {
		drainMediaQueue(track.queue)
		track.discontinuity = true
	}
	if key.Video {
		packet.H264 = append([]byte(nil), packet.H264...)
	} else {
		packet.PCM = append([]float32(nil), packet.PCM...)
	}
	packet.Discontinuity = track.discontinuity
	track.discontinuity = false
	track.queue <- packet
	request := key.Video && (packet.Discontinuity || track.keyframeRequested)
	track.keyframeRequested = false
	return request
}

// ParticipantWireFrame preserves the exact source clock and discontinuity marker.
// The generation must come from a roster-authorized subscription, not a packet
// counter. This prepares a frame but intentionally does not start publication.
func ParticipantWireFrame(callID string, key ParticipantMediaKey, generation uint64, packet ParticipantMediaPacket) (rtcwire.Message, error) {
	source := &rtcwire.ParticipantSource{ParticipantID: key.ParticipantID, Device: key.Device, SSRC: key.SSRC, RTPClockRate: packet.ClockRate, Discontinuity: packet.Discontinuity}
	m := rtcwire.Message{CallID: callID, Group: true, Generation: generation, Timestamp: uint64(packet.Timestamp), ParticipantSource: source}
	if key.Video {
		if len(packet.PCM) != 0 {
			return rtcwire.Message{}, ErrBadFrame
		}
		source.ScreenShare = packet.ScreenShare
		source.RoleRevision = packet.RoleRevision
		m.Type = "whatsapp_participant_video"
		m.Orientation = packet.Orientation
		m.Data = packet.H264
	} else {
		if len(packet.H264) != 0 || packet.Orientation != 0 || packet.ScreenShare {
			return rtcwire.Message{}, ErrBadFrame
		}
		m.Type = "whatsapp_participant_audio"
		source.SampleRate = AudioSampleRate
		raw, err := rtcwire.EncodeParticipantPCM(packet.PCM)
		if err != nil {
			return rtcwire.Message{}, err
		}
		m.Data = raw
	}
	if err := rtcwire.ValidateParticipantFrame(m); err != nil {
		return rtcwire.Message{}, err
	}
	return m, nil
}

// RequestParticipantKeyframe coalesces requests for one subscribed video
// source. The next authenticated provider callback performs the request;
// no provider closure is retained across a roster/key epoch or call teardown.
// A stale request is harmless (the track may have retired in transit).
func (m *MediaBus) RequestParticipantKeyframe(key ParticipantMediaKey, generation, revision uint64) bool {
	m.admissionMu.Lock()
	defer m.admissionMu.Unlock()
	track := m.participantMedia[key]
	if !key.Video || generation == 0 || track == nil || track.generation != generation || revision != m.participantEpochRevision || !m.participantEpochValid || m.participantEpochRoster != m.participantRosterTransaction || generation != uint64(m.participantEpochRoster)<<32|uint64(m.participantKeyTransaction) {
		return false
	}
	select {
	case <-m.closed:
		return false
	default:
	}
	track.keyframeRequested = true
	return true
}
