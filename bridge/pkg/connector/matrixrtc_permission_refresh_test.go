package connector

import (
	"context"
	"errors"
	"testing"
)

func TestRTCPermissionRefreshReconcilesAmbiguousSend(t *testing.T) {
	for _, committed := range []bool{false, true} {
		reads, sends := 0, 0
		desired, state, unrelated := 100, 0, 3
		err := reconcileRTCPermission(context.Background(), func() bool { return true }, func() (func() error, error) {
			reads++
			if reads == 2 && !committed {
				desired = 0
				state = 50
			}
			if state == desired {
				return nil, nil
			}
			snapshot, original := desired, unrelated
			return func() error {
				sends++
				if sends == 1 {
					if committed {
						state = snapshot
					}
					unrelated = 7
					return errors.New("response lost")
				}
				if original != 7 || snapshot != 0 {
					t.Fatal("stale state or policy reused")
				}
				state = snapshot
				return nil
			}, nil
		})
		want := 2
		if committed {
			want = 1
		}
		if err != nil || reads != 2 || sends != want || unrelated != 7 || state != desired {
			t.Fatalf("committed=%v reads=%d sends=%d state=%d err=%v", committed, reads, sends, state, err)
		}
	}
}

func TestRTCPermissionRefreshBoundAndRoomReplacement(t *testing.T) {
	reads, sends := 0, 0
	err := reconcileRTCPermission(context.Background(), func() bool { return true }, func() (func() error, error) {
		reads++
		return func() error { sends++; return errors.New("unavailable") }, nil
	})
	if err == nil || reads != 3 || sends != 2 {
		t.Fatalf("reads=%d sends=%d err=%v", reads, sends, err)
	}
	current := true
	err = reconcileRTCPermission(context.Background(), func() bool { return current }, func() (func() error, error) {
		current = false
		return func() error { t.Fatal("sent to retired room"); return nil }, nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestRTCPermissionRefreshLockCancellation(t *testing.T) {
	var locks rtcPermissionRefreshLocks
	release, ok := locks.acquire(context.Background(), "room")
	if !ok {
		t.Fatal("initial acquire")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, ok = locks.acquire(ctx, "room"); ok {
		t.Fatal("cancelled acquire")
	}
	release()
	release, ok = locks.acquire(context.Background(), "room")
	if !ok {
		t.Fatal("lock not reusable")
	}
	release()
}
