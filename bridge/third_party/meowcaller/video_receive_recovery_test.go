package meowcaller

import (
	"testing"
	"time"
)

func TestVideoReceivePLIRecoverySurvivesThrottleAndLoss(t *testing.T) {
	var s videoReceiveState
	last := make(map[uint32]time.Time)
	now := time.Unix(100, 0)
	// A previous explicit request uses the interval before RTP loss.
	s.requestKeyframe.Store(true)
	if send, explicit := s.takeVideoPLI(last, 1, now); !send || !explicit {
		t.Fatal("initial explicit request missing")
	}
	s.assembler.Push(1, 100, false, []byte{0x65, 1})
	s.assembler.Push(3, 100, true, []byte{0x41, 2})
	if send, _ := s.takeVideoPLI(last, 1, now.Add(time.Millisecond)); send {
		t.Fatal("loss bypassed throttle")
	}
	for i := 1; i <= 3; i++ {
		s.assembler.Push(uint16(3+i), uint32(100+i), true, []byte{0x41, 2})
		if send, explicit := s.takeVideoPLI(last, 1, now.Add(time.Duration(i)*videoPLIInterval)); !send || explicit {
			t.Fatal("pending assembly recovery did not retry")
		}
	}
	if _, complete, _ := s.assembler.Push(7, 200, true, []byte{0x65, 3}); !complete {
		t.Fatal("complete IDR not accepted")
	}
	if send, _ := s.takeVideoPLI(last, 1, now.Add(4*videoPLIInterval)); send {
		t.Fatal("assembly retry continued after IDR")
	}
}

func TestVideoReceivePLIExplicitRequestSurvivesThrottle(t *testing.T) {
	var s videoReceiveState
	last := make(map[uint32]time.Time)
	now := time.Unix(100, 0)
	shouldSendVideoPLI(last, 1, now)
	s.requestKeyframe.Store(true)
	if send, _ := s.takeVideoPLI(last, 1, now.Add(time.Millisecond)); send || !s.requestKeyframe.Load() {
		t.Fatal("throttle discarded explicit request")
	}
	if send, explicit := s.takeVideoPLI(last, 1, now.Add(videoPLIInterval)); !send || !explicit || s.requestKeyframe.Load() {
		t.Fatal("pending explicit request not consumed on admission")
	}
	if send, _ := s.takeVideoPLI(last, 1, now.Add(2*videoPLIInterval)); send {
		t.Fatal("successful explicit request unexpectedly retried")
	}
}

func TestNewVideoReceiveStateRequiresInitialIDR(t *testing.T) {
	s := newVideoReceiveState()
	if s.orientation != -1 || !s.assembler.NeedsKeyframe() {
		t.Fatal("new receiver lacks initial decoder gate")
	}
	if _, complete, _ := s.assembler.Push(1, 100, true, []byte{0x41, 1}); complete {
		t.Fatal("dependent frame reached decoder without an IDR")
	}
	last := make(map[uint32]time.Time)
	if send, _ := s.takeVideoPLI(last, 1, time.Unix(100, 0)); !send {
		t.Fatal("initial recovery did not request keyframe")
	}
	if _, complete, _ := s.assembler.Push(2, 200, true, []byte{0x65, 1}); !complete {
		t.Fatal("initial IDR not accepted")
	}
	s.noteVideoDelivery(true)
	if _, complete, _ := s.assembler.Push(3, 300, true, []byte{0x41, 1}); !complete {
		t.Fatal("dependent frame blocked after successful IDR")
	}
}

func TestVideoReceiveStateRecoversAfterUndeliveredFrame(t *testing.T) {
	s := newVideoReceiveState()
	if _, complete, _ := s.assembler.Push(1, 100, true, []byte{0x65, 1}); !complete {
		t.Fatal("initial IDR not accepted")
	}
	s.noteVideoDelivery(false)
	if _, complete, _ := s.assembler.Push(2, 200, true, []byte{0x41, 1}); complete {
		t.Fatal("dependent frame delivered after discarded IDR")
	}
	if _, complete, _ := s.assembler.Push(3, 300, true, []byte{0x65, 2}); !complete {
		t.Fatal("replacement IDR not accepted")
	}
	s.noteVideoDelivery(true)
	if _, complete, _ := s.assembler.Push(4, 400, true, []byte{0x41, 2}); !complete {
		t.Fatal("dependent frame not restored")
	}
	s.noteVideoDelivery(false)
	if _, complete, _ := s.assembler.Push(5, 500, true, []byte{0x41, 3}); complete {
		t.Fatal("dependent frame accepted after reference-frame loss")
	}
}
