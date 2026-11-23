package connector

import (
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
	source             videoSource
	started, lastFrame time.Time
	generation, clock  uint64
	lastRTP            uint32
	params             map[videoSource]videoParameters
	waiting            bool
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
	param := s.params[key]
	idr := false
	for _, nal := range rtp.SplitAnnexB(f.AccessUnit) {
		switch nal[0] & 31 {
		case 5:
			idr = true
		case 7:
			if len(nal) <= 4096 {
				param.sps = append([]byte(nil), nal...)
			}
		case 8:
			if len(nal) <= 4096 {
				param.pps = append([]byte(nil), nal...)
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
	s.params[key] = param
	if b.TakeVideoResync() {
		s.waiting = true
	}
	if key != s.source && !s.lastFrame.IsZero() && now.Sub(s.lastFrame) < groupVideoSourceHold {
		return
	}
	restart := s.generation == 0 || key != s.source || s.waiting || now.Sub(s.lastFrame) >= groupVideoSourceHold
	data := f.AccessUnit
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
		s.source = key
		s.generation++
		elapsed := uint64(now.Sub(s.started).Microseconds()) * 9 / 100
		if elapsed <= s.clock {
			elapsed = s.clock + 4
		}
		s.clock = elapsed
		s.waiting = false
	} else {
		delta := int64(int32(f.Timestamp - s.lastRTP))
		if delta < 0 && uint64(-delta) > s.clock {
			return
		}
		s.clock = uint64(int64(s.clock) + delta)
	}
	s.lastRTP = f.Timestamp
	s.lastFrame = now
	if !b.TryVideo(voipbridge.EncodedVideoFrame{Data: data, Orientation: f.Orientation, Timestamp: s.clock, Generation: s.generation}) {
		s.waiting = true
		if f.RequestKeyframe != nil {
			f.RequestKeyframe()
		}
	}
}
