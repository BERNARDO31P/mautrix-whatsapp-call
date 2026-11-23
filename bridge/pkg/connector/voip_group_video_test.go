package connector

import (
	"context"
	"github.com/purpshell/meowcaller"
	"go.mau.fi/mautrix-whatsapp/pkg/connector/voipbridge"
	"testing"
	"time"
)

func TestGroupVideoSourceSelectorKeepsOneEncoderUntilStale(t *testing.T) {
	now := time.Unix(100, 0)
	sel := groupVideoSourceSelector{started: now}
	bus := voipbridge.NewMediaBus(4)
	idr := []byte{0, 0, 1, 103, 1, 0, 0, 1, 104, 1, 0, 0, 1, 101, 1}
	delta := []byte{0, 0, 1, 65, 1}
	requests := 0
	feed := func(user string, au []byte, offset time.Duration, ts uint32) {
		sel.forward(meowcaller.ParticipantVideoFrame{ParticipantID: user, Timestamp: ts, AccessUnit: au, RequestKeyframe: func() { requests++ }}, now.Add(offset), bus)
	}
	feed("A", idr, 0, 4000)
	feed("B", idr, time.Second, 90)
	feed("A", delta, 1500*time.Millisecond, 7000)
	feed("B", delta, 4*time.Second, 180)
	feed("B", idr, 4100*time.Millisecond, 270)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	a, _ := bus.NextVideoFromWhatsApp(ctx)
	b, _ := bus.NextVideoFromWhatsApp(ctx)
	c, _ := bus.NextVideoFromWhatsApp(ctx)
	if requests != 1 || a.Generation != 1 || b.Timestamp-a.Timestamp != 3000 || c.Generation != 2 {
		t.Fatalf("PLI=%d generations %d/%d/%d", requests, a.Generation, b.Generation, c.Generation)
	}
}
