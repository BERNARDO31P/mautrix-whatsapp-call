package connector

import (
	"context"
	"fmt"
	"hash/fnv"
	"sync"
)

// Fixed stripes bound lock storage even when rooms are removed. All call-only
// refresh paths in this process serialize their read/merge/send for a room.
type rtcPermissionRefreshLocks struct {
	once  sync.Once
	slots [64]chan struct{}
}

func (l *rtcPermissionRefreshLocks) acquire(ctx context.Context, room string) (func(), bool) {
	l.once.Do(func() {
		for i := range l.slots {
			l.slots[i] = make(chan struct{}, 1)
		}
	})
	h := fnv.New32a()
	_, _ = h.Write([]byte(room))
	slot := l.slots[h.Sum32()%uint32(len(l.slots))]
	select {
	case <-ctx.Done():
		return nil, false
	case slot <- struct{}{}:
	}
	if ctx.Err() != nil {
		<-slot
		return nil, false
	}
	return func() { <-slot }, true
}

// A send failure may mean that the server applied the state but the response
// was lost. Never resend the old event: recompute policy and read again. A
// final read-only reconciliation can establish that the desired state exists.
// This does not provide CAS against other Matrix writers.
func reconcileRTCPermission(ctx context.Context, current func() bool, prepare func() (func() error, error)) error {
	var last error
	for attempt := 0; attempt < 3; attempt++ {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if !current() {
			return nil
		}
		send, err := prepare()
		if err != nil {
			return err
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if !current() || send == nil {
			return nil
		}
		if attempt == 2 {
			return fmt.Errorf("RTC permission remains unsettled after two sends: %w", last)
		}
		last = send()
		if last == nil {
			return nil
		}
	}
	return last
}
