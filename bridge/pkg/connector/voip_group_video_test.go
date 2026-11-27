package connector

import (
	"bytes"
	"context"
	"fmt"
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

func TestGroupVideoSourceSelectorRTPWrapAndReordering(t *testing.T) {
	now := time.Unix(100, 0)
	sel := groupVideoSourceSelector{started: now}
	bus := voipbridge.NewMediaBus(8)
	defer bus.Close()
	idr := []byte{0, 0, 1, 103, 1, 0, 0, 1, 104, 1, 0, 0, 1, 101, 1}
	delta := []byte{0, 0, 1, 65, 1}
	feed := func(au []byte, offset time.Duration, ts uint32) {
		sel.forward(meowcaller.ParticipantVideoFrame{ParticipantID: "A", Timestamp: ts, AccessUnit: au}, now.Add(offset), bus)
	}
	feed(idr, 0, 0xfffffff0)
	feed(delta, time.Millisecond, 0x20)
	feed(delta, 2*time.Millisecond, 0x20)
	feed(delta, 3*time.Millisecond, 0xfffffff5)
	if sel.lastRTP != 0x20 || !sel.lastFrame.Equal(now.Add(time.Millisecond)) {
		t.Fatal("stale access unit updated source state")
	}
	feed(delta, 4*time.Millisecond, 0x50)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	for i, want := range []uint64{4, 52, 100} {
		got, err := bus.NextVideoFromWhatsApp(ctx)
		if err != nil || got.Timestamp != want || got.Generation != 1 {
			t.Fatalf("frame %d: %+v %v", i, got, err)
		}
	}
	ctx2, cancel2 := context.WithTimeout(context.Background(), time.Millisecond)
	defer cancel2()
	if _, err := bus.NextVideoFromWhatsApp(ctx2); err == nil {
		t.Fatal("stale access unit reached media queue")
	}
}

func TestGroupVideoSourceSelectorRestartAfterTimestampReset(t *testing.T) {
	now := time.Unix(100, 0)
	sel := groupVideoSourceSelector{started: now}
	bus := voipbridge.NewMediaBus(4)
	defer bus.Close()
	idr := []byte{0, 0, 1, 103, 1, 0, 0, 1, 104, 1, 0, 0, 1, 101, 1}
	feed := func(at time.Time, ts uint32) {
		sel.forward(meowcaller.ParticipantVideoFrame{ParticipantID: "A", Timestamp: ts, AccessUnit: idr}, at, bus)
	}
	feed(now.Add(-time.Millisecond), 90000)
	feed(now.Add(3*time.Second), 100)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	a, err := bus.NextVideoFromWhatsApp(ctx)
	if err != nil || a.Timestamp != 4 {
		t.Fatalf("initial clock: %+v %v", a, err)
	}
	b, err := bus.NextVideoFromWhatsApp(ctx)
	if err != nil || b.Timestamp != 270000 || b.Generation != 2 {
		t.Fatalf("restart clock: %+v %v", b, err)
	}
}

func TestGroupVideoSourceSelectorRejectedFrameDoesNotExtendHold(t *testing.T) {
	now := time.Unix(100, 0)
	sel := groupVideoSourceSelector{started: now}
	bus := voipbridge.NewMediaBus(1)
	defer bus.Close()
	idr := []byte{0, 0, 1, 103, 1, 0, 0, 1, 104, 1, 0, 0, 1, 101, 1}
	delta := []byte{0, 0, 1, 65, 1}
	feed := func(user string, au []byte, offset time.Duration, ts uint32) {
		sel.forward(meowcaller.ParticipantVideoFrame{ParticipantID: user, Timestamp: ts, AccessUnit: au}, now.Add(offset), bus)
	}
	feed("A", idr, 0, 100)
	feed("A", delta, 1900*time.Millisecond, 200)
	if !sel.waiting || !sel.lastFrame.Equal(now) || sel.lastRTP != 100 || sel.clock != 4 {
		t.Fatalf("rejected frame extended ownership: %+v", sel)
	}
	feed("B", idr, 2100*time.Millisecond, 10)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	f, err := bus.NextVideoFromWhatsApp(ctx)
	if err != nil || f.Generation != 2 || sel.source.participant != "B" {
		t.Fatalf("replacement frame: %+v %v, source=%+v", f, err, sel.source)
	}
}

func TestGroupVideoSourceSelectorRejectedInitialFrameDoesNotClaimSource(t *testing.T) {
	now := time.Unix(100, 0)
	sel := groupVideoSourceSelector{started: now}
	bus := voipbridge.NewMediaBus(1)
	defer bus.Close()
	if !bus.TryVideo(voipbridge.EncodedVideoFrame{Data: []byte{1}}) {
		t.Fatal("prefill failed")
	}
	idr := []byte{0, 0, 1, 103, 1, 0, 0, 1, 104, 1, 0, 0, 1, 101, 1}
	sel.forward(meowcaller.ParticipantVideoFrame{ParticipantID: "A", Timestamp: 100, AccessUnit: idr}, now, bus)
	if sel.generation != 0 || !sel.lastFrame.IsZero() || sel.source.participant != "" {
		t.Fatal("rejected frame claimed source")
	}
	sel.forward(meowcaller.ParticipantVideoFrame{ParticipantID: "B", Timestamp: 200, AccessUnit: idr}, now.Add(time.Millisecond), bus)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	f, err := bus.NextVideoFromWhatsApp(ctx)
	if err != nil || f.Generation != 1 || sel.source.participant != "B" {
		t.Fatalf("replacement frame: %+v %v", f, err)
	}
}

func TestGroupVideoSourceSelectorParameterOnlyDoesNotAdvanceClock(t *testing.T) {
	now := time.Unix(100, 0)
	sel := groupVideoSourceSelector{started: now}
	bus := voipbridge.NewMediaBus(8)
	defer bus.Close()
	idr := []byte{0, 0, 1, 103, 1, 0, 0, 1, 104, 1, 0, 0, 1, 101, 1}
	params := []byte{0, 0, 1, 103, 2, 0, 0, 1, 104, 2}
	picture := []byte{0, 0, 1, 101, 1}
	delta := []byte{0, 0, 1, 65, 1}
	feed := func(user string, au []byte, offset time.Duration, ts uint32) {
		sel.forward(meowcaller.ParticipantVideoFrame{ParticipantID: user, Timestamp: ts, AccessUnit: au}, now.Add(offset), bus)
	}
	feed("A", idr, 0, 100)
	feed("A", params, time.Second, 200)
	if !sel.waiting || sel.lastRTP != 100 || !sel.lastFrame.Equal(now) || sel.clock != 4 {
		t.Fatalf("parameter-only unit changed picture state: %+v", sel)
	}
	feed("A", delta, time.Second, 200)
	if !sel.waiting || sel.lastRTP != 100 {
		t.Fatal("dependent picture admitted after parameter change")
	}
	feed("A", picture, time.Second, 200)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	first, err := bus.NextVideoFromWhatsApp(ctx)
	if err != nil || first.Generation != 1 {
		t.Fatalf("first picture: %+v %v", first, err)
	}
	second, err := bus.NextVideoFromWhatsApp(ctx)
	if err != nil || second.Generation != 2 || second.Timestamp != 90000 || !bytes.Contains(second.Data, []byte{103, 2}) || !bytes.Contains(second.Data, []byte{104, 2}) {
		t.Fatalf("restarted picture: %+v %v", second, err)
	}
}

func TestGroupVideoSourceSelectorParameterOnlyCannotHoldSource(t *testing.T) {
	now := time.Unix(100, 0)
	sel := groupVideoSourceSelector{started: now}
	bus := voipbridge.NewMediaBus(8)
	defer bus.Close()
	params := []byte{0, 0, 1, 103, 1, 0, 0, 1, 104, 1}
	idr := []byte{0, 0, 1, 101, 1}
	feed := func(user string, au []byte, offset time.Duration, ts uint32) {
		sel.forward(meowcaller.ParticipantVideoFrame{ParticipantID: user, Timestamp: ts, AccessUnit: au}, now.Add(offset), bus)
	}
	feed("A", params, 0, 100)
	if sel.generation != 0 || !sel.lastFrame.IsZero() {
		t.Fatal("parameters claimed ownership")
	}
	feed("A", idr, 0, 100)
	feed("A", params, 1900*time.Millisecond, 200)
	if sel.waiting || !sel.lastFrame.Equal(now) {
		t.Fatal("unchanged parameters extended ownership")
	}
	feed("B", params, 2100*time.Millisecond, 300)
	feed("B", idr, 2100*time.Millisecond, 300)
	if sel.source.participant != "B" || sel.generation != 2 {
		t.Fatalf("replacement blocked: %+v", sel)
	}
}

func TestGroupVideoSourceSelectorInBandParameterChangeRequiresIDR(t *testing.T) {
	now := time.Unix(100, 0)
	sel := groupVideoSourceSelector{started: now}
	bus := voipbridge.NewMediaBus(8)
	defer bus.Close()
	initial := []byte{0, 0, 1, 103, 1, 0, 0, 1, 104, 1, 0, 0, 1, 101, 1}
	changedDelta := []byte{0, 0, 1, 103, 2, 0, 0, 1, 104, 2, 0, 0, 1, 65, 1}
	changedIDR := []byte{0, 0, 1, 103, 3, 0, 0, 1, 104, 3, 0, 0, 1, 101, 1}
	requests := 0
	feed := func(au []byte, offset time.Duration, ts uint32) {
		sel.forward(meowcaller.ParticipantVideoFrame{ParticipantID: "A", Timestamp: ts, AccessUnit: au, RequestKeyframe: func() { requests++ }}, now.Add(offset), bus)
	}
	feed(initial, 0, 100)
	feed(changedDelta, time.Millisecond, 200)
	if !sel.waiting || sel.generation != 1 || sel.lastRTP != 100 || requests != 1 {
		t.Fatalf("dependent picture admitted after in-band change: %+v, requests=%d", sel, requests)
	}
	feed(changedIDR, 2*time.Millisecond, 300)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	first, err := bus.NextVideoFromWhatsApp(ctx)
	if err != nil || first.Generation != 1 {
		t.Fatalf("initial: %+v %v", first, err)
	}
	second, err := bus.NextVideoFromWhatsApp(ctx)
	if err != nil || second.Generation != 2 || !bytes.Contains(second.Data, []byte{103, 3}) || !bytes.Contains(second.Data, []byte{104, 3}) {
		t.Fatalf("restart: %+v %v", second, err)
	}
	if sel.waiting || sel.lastRTP != 300 {
		t.Fatalf("restart failed: %+v", sel)
	}
}

func TestGroupVideoSourceSelectorOversizedParametersInvalidateCache(t *testing.T) {
	for _, nalType := range []byte{103, 104} {
		t.Run(fmt.Sprint(nalType), func(t *testing.T) {
			now := time.Unix(100, 0)
			sel := groupVideoSourceSelector{started: now}
			bus := voipbridge.NewMediaBus(8)
			defer bus.Close()
			initial := []byte{0, 0, 1, 103, 1, 0, 0, 1, 104, 1, 0, 0, 1, 101, 1}
			oversized := append([]byte{0, 0, 1, nalType}, bytes.Repeat([]byte{2}, 4096)...)
			oversized = append(oversized, 0, 0, 1, 101, 1)
			idrOnly := []byte{0, 0, 1, 101, 1}
			requests := 0
			feed := func(participant string, au []byte, ts uint32) {
				sel.forward(meowcaller.ParticipantVideoFrame{ParticipantID: participant, Timestamp: ts, AccessUnit: au, RequestKeyframe: func() { requests++ }}, now.Add(time.Duration(ts)*time.Millisecond), bus)
			}
			feed("A", initial, 1)
			feed("B", oversized, 2)
			if sel.waiting || requests != 0 {
				t.Fatal("inactive source disrupted active source")
			}
			feed("A", oversized, 3)
			feed("A", idrOnly, 4)
			if !sel.waiting || sel.generation != 1 || sel.lastRTP != 1 || requests != 2 {
				t.Fatalf("invalid parameter or incomplete restart admitted: %+v, requests=%d", sel, requests)
			}
			feed("A", initial, 5)
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			for _, generation := range []uint64{1, 2} {
				frame, err := bus.NextVideoFromWhatsApp(ctx)
				if err != nil || frame.Generation != generation || len(frame.Data) > 100 {
					t.Fatalf("frame: %+v %v", frame, err)
				}
			}
			if sel.waiting || sel.lastRTP != 5 {
				t.Fatal("valid recovery did not complete")
			}
			shortCtx, shortCancel := context.WithTimeout(context.Background(), time.Millisecond)
			defer shortCancel()
			if _, err := bus.NextVideoFromWhatsApp(shortCtx); err == nil {
				t.Fatal("unexpected extra frame")
			}
		})
	}
}

func TestGroupVideoSourceSelectorRejectsReorderedParametersDuringRecovery(t *testing.T) {
	now := time.Unix(100, 0)
	sel := groupVideoSourceSelector{started: now}
	bus := voipbridge.NewMediaBus(8)
	defer bus.Close()
	old := []byte{0, 0, 1, 103, 1, 0, 0, 1, 104, 1, 0, 0, 1, 101, 1}
	params := []byte{0, 0, 1, 103, 2, 0, 0, 1, 104, 2}
	idr := []byte{0, 0, 1, 101, 1}
	feed := func(au []byte, ts uint32, offset time.Duration) {
		sel.forward(meowcaller.ParticipantVideoFrame{ParticipantID: "A", Timestamp: ts, AccessUnit: au}, now.Add(offset), bus)
	}
	feed(old, 100, 0)
	feed(params, 200, time.Millisecond)
	feed(old, 150, 2*time.Millisecond)
	feed(old, 100, 3*time.Millisecond)
	if !sel.waiting || sel.generation != 1 || sel.observedRTP != 200 {
		t.Fatalf("reordered IDR interrupted recovery: %+v", sel)
	}
	feed(idr, 200, 4*time.Millisecond)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if _, err := bus.NextVideoFromWhatsApp(ctx); err != nil {
		t.Fatal(err)
	}
	frame, err := bus.NextVideoFromWhatsApp(ctx)
	if err != nil || frame.Generation != 2 || !bytes.Contains(frame.Data, []byte{103, 2}) || !bytes.Contains(frame.Data, []byte{104, 2}) {
		t.Fatalf("recovered with wrong parameters: %+v %v", frame, err)
	}
	if sel.waiting || sel.lastRTP != 200 {
		t.Fatal("recovery did not finish")
	}
}
