package rtcwire

import (
	"errors"
	"time"
)

// ErrVideoFrameDropped is recoverable sender feedback, not a transport failure.
var ErrVideoFrameDropped = errors.New("RTC video access unit dropped")

const videoKeyframeRetryInterval = 2 * time.Second

// VideoForwardState belongs to one gateway receive loop. A new camera
// generation must start at an IDR and announce its orientation again, even
// when the numeric orientation has not changed. This does not prove delivery
// of the resulting RTP packets to the remote decoder.
type VideoForwardState struct {
	ready           bool
	lastRequest     time.Time
	orientation     int
	orientationSent bool
}

func (s *VideoForwardState) Reset() {
	*s = VideoForwardState{}
}

// Control is called after successful application of a Matrix camera control.
// matrix_video_resync concerns the opposite media direction.
func (s *VideoForwardState) Control(kind string) {
	switch kind {
	case "matrix_video_start", "matrix_video_screen_start", "matrix_video_stop", "matrix_video_mute", "matrix_video_unmute":
		s.Reset()
	}
}

func (s *VideoForwardState) RequestKeyframe(request func() error) error {
	return s.requestKeyframeAt(time.Now(), request)
}

// Retry only while frames are arriving, with a monotonic interval between requests.
func (s *VideoForwardState) requestKeyframeAt(now time.Time, request func() error) error {
	if !s.lastRequest.IsZero() && now.Sub(s.lastRequest) < videoKeyframeRetryInterval {
		return nil
	}
	if err := request(); err != nil {
		return err
	}
	s.lastRequest = now
	return nil
}

func (s *VideoForwardState) Forward(enabled, idr bool, orientation int, request func() error, setOrientation func(int) error, send func() error) error {
	return s.forwardAt(time.Now(), enabled, idr, orientation, request, setOrientation, send)
}

func (s *VideoForwardState) forwardAt(now time.Time, enabled, idr bool, orientation int, request func() error, setOrientation func(int) error, send func() error) error {
	if !enabled {
		s.Reset()
		return nil
	}
	if !s.ready && !idr {
		return s.requestKeyframeAt(now, request)
	}
	if !s.orientationSent || s.orientation != orientation {
		if err := setOrientation(orientation); err != nil {
			if errors.Is(err, ErrVideoFrameDropped) {
				// A recoverable orientation race invalidates this frame, not the
				// outstanding keyframe request. Preserve the retry interval just
				// as for a recoverable send failure.
				s.ready = false
				s.orientationSent = false
				return s.requestKeyframeAt(now, request)
			}
			return err
		}
		s.orientation = orientation
		s.orientationSent = true
	}
	if err := send(); err != nil {
		if errors.Is(err, ErrVideoFrameDropped) {
			s.ready = false
			s.orientationSent = false
			return s.requestKeyframeAt(now, request)
		}
		s.Reset()
		return err
	}
	s.ready = true
	s.lastRequest = time.Time{}
	return nil
}
