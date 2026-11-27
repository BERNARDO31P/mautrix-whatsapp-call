package connector

import (
	"bytes"
	"github.com/purpshell/meowcaller"
	"github.com/purpshell/meowcaller/rtp"
	"go.mau.fi/mautrix-whatsapp/pkg/connector/voipbridge"
	"time"
)

type videoSource struct {
	participant string
	ssrc        uint32
}
type videoParameters struct{ sps, pps []byte }
type groupVideoSourceSelector struct {
	source               videoSource
	started, lastFrame   time.Time
	generation, clock    uint64
	lastRTP, observedRTP uint32
	params               map[videoSource]videoParameters
	waiting              bool
}

// Called by the authenticated RTP receive loop, once per complete access unit.
func (s *groupVideoSourceSelector) forward(f meowcaller.ParticipantVideoFrame, now time.Time, b *voipbridge.MediaBus) {
	if f.ParticipantID == "" || len(f.AccessUnit) == 0 {
		return
	}
	key := videoSource{f.ParticipantID, f.SSRC}
	if s.params == nil {
		s.params = make(map[videoSource]videoParameters)
	}
	if b.TakeVideoResync() {
		s.waiting = true
	}
	// Reject stale complete access units before they change cached parameters
	// or extend the source hold timer. Signed subtraction handles RTP wrap.
	if key == s.source && s.generation != 0 && now.Sub(s.lastFrame) < groupVideoSourceHold {
		// While recovering, newer parameter-only or rejected units may have
		// advanced the observed timestamp without advancing the displayed clock.
		// Allow another unit at that timestamp, but never replay an older unit.
		if int32(f.Timestamp-s.lastRTP) <= 0 || int32(f.Timestamp-s.observedRTP) < 0 {
			return
		}
		s.observedRTP = f.Timestamp
	}
	previous := s.params[key]
	param := previous
	idr, hasVCL := false, false
	invalidSPS, invalidPPS := false, false
	for _, nal := range rtp.SplitAnnexB(f.AccessUnit) {
		switch nal[0] & 31 {
		case 1, 2, 3, 4:
			hasVCL = true
		case 5:
			hasVCL = true
			idr = true
		case 7:
			if len(nal) <= 4096 {
				param.sps = append([]byte(nil), nal...)
			} else {
				invalidSPS = true
			}
		case 8:
			if len(nal) <= 4096 {
				param.pps = append([]byte(nil), nal...)
			} else {
				invalidPPS = true
			}
		}
	}
	if len(s.params) >= 64 {
		if _, ok := s.params[key]; !ok {
			for other := range s.params {
				if other != s.source {
					delete(s.params, other)
					break
				}
			}
		}
	}
	// An uncacheable parameter set must not be forwarded with an older
	// cached set. Invalidate that set until the encoder supplies a valid one.
	if invalidSPS {
		param.sps = nil
	}
	if invalidPPS {
		param.pps = nil
	}
	s.params[key] = param
	if invalidSPS || invalidPPS {
		if key == s.source && s.generation != 0 {
			s.waiting = true
		}
		if (s.generation == 0 || key == s.source || now.Sub(s.lastFrame) >= groupVideoSourceHold) && f.RequestKeyframe != nil {
			f.RequestKeyframe()
		}
		return
	}
	// A parameter change requires a new independently decodable picture,
	// even when the new parameters share an access unit with a slice.
	if key == s.source && s.generation != 0 && (!bytes.Equal(previous.sps, param.sps) || !bytes.Equal(previous.pps, param.pps)) {
		s.waiting = true
	}
	// Non-picture units must not advance the clock or extend ownership.
	if !hasVCL {
		return
	}
	if key != s.source && !s.lastFrame.IsZero() && now.Sub(s.lastFrame) < groupVideoSourceHold {
		return
	}
	restart := s.generation == 0 || key != s.source || s.waiting || now.Sub(s.lastFrame) >= groupVideoSourceHold
	data := f.AccessUnit
	clock, generation := s.clock, s.generation
	if restart {
		if !idr || len(param.sps) == 0 || len(param.pps) == 0 {
			if f.RequestKeyframe != nil {
				f.RequestKeyframe()
			}
			return
		}
		prefix := make([]byte, 0, len(data)+len(param.sps)+len(param.pps)+8)
		for _, nal := range [][]byte{param.sps, param.pps} {
			prefix = append(prefix, 0, 0, 0, 1)
			prefix = append(prefix, nal...)
		}
		data = append(prefix, data...)
		generation++
		elapsed := uint64(0)
		if duration := now.Sub(s.started); duration > 0 {
			elapsed = uint64(duration.Microseconds()) * 9 / 100
		}
		if elapsed <= clock {
			elapsed = clock + 4
		}
		clock = elapsed
	} else {
		delta := int32(f.Timestamp - s.lastRTP)
		if delta <= 0 {
			return
		}
		clock += uint64(delta)
	}
	if !b.TryVideo(voipbridge.EncodedVideoFrame{Data: data, Orientation: f.Orientation, Timestamp: clock, Generation: generation}) {
		s.waiting = true
		if f.RequestKeyframe != nil {
			f.RequestKeyframe()
		}
		return
	}
	// Only an admitted frame may establish or extend source ownership.
	s.source, s.clock, s.generation = key, clock, generation
	s.lastRTP, s.observedRTP, s.lastFrame = f.Timestamp, f.Timestamp, now
	s.waiting = false
}
