package meowcaller

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"go.mau.fi/whatsmeow"
	waBinary "go.mau.fi/whatsmeow/binary"
	"go.mau.fi/whatsmeow/store"
	waLog "go.mau.fi/whatsmeow/util/log"
)

func TestInstallCallAckHookMatchesPinnedUpstreamLayout(t *testing.T) {
	wa := whatsmeow.NewClient(&store.Device{}, waLog.Noop)
	client := &Client{wa: wa, log: zerolog.Nop()}
	eng := newEngine(client)
	if err := eng.installCallAckHook(); err != nil {
		t.Fatalf("install raw call adapter: %v", err)
	}
}

func TestInstallCallAckHookRejectsMissingClient(t *testing.T) {
	client := &Client{log: zerolog.Nop()}
	eng := newEngine(client)
	if err := eng.installCallAckHook(); err == nil {
		t.Fatal("raw call adapter accepted a missing whatsmeow client")
	}
}

func TestGroupFeaturesRejectUnavailableRawAdapter(t *testing.T) {
	client := &Client{log: zerolog.Nop()}
	eng := newEngine(client)
	eng.rawCallHookErr = errors.New("upstream layout changed")
	if _, err := eng.placeGroupCall(
		context.Background(),
		[]string{"1", "2"},
		GroupCallOptions{},
	); err == nil {
		t.Fatal("group call continued without its raw call adapter")
	}
}

func TestCallAckErrorCorrelatesByOfferStanzaID(t *testing.T) {
	client := &Client{log: zerolog.Nop()}
	eng := newEngine(client)
	call := &Call{eng: eng, id: "CALL-1", phase: CallPhaseCalling}
	var reason string
	call.OnEnd(func(got string) { reason = got })
	eng.calls[call.id] = &engineCall{
		call:        call,
		direction:   CallDirectionOutgoing,
		offerNodeID: "OFFER-STANZA-1",
	}

	eng.onCallAck(&waBinary.Node{
		Tag: "ack",
		Attrs: waBinary.Attrs{
			"class": "call",
			"type":  "offer",
			"id":    "OFFER-STANZA-1",
			"error": "406",
		},
	})

	if call.State() != CallPhaseEnded {
		t.Fatalf("call state = %v, want ended", call.State())
	}
	if reason != "server:406" {
		t.Fatalf("end reason = %q, want server:406", reason)
	}
	if eng.lookup(call.id) != nil {
		t.Fatal("ended call remained in engine registry")
	}
}

func TestCallAckNestedErrorCodeEndsCorrelatedOffer(t *testing.T) {
	client := &Client{log: zerolog.Nop()}
	eng := newEngine(client)
	call := &Call{eng: eng, id: "CALL-2", phase: CallPhaseCalling}
	var reason string
	call.OnEnd(func(got string) { reason = got })
	eng.calls[call.id] = &engineCall{
		call:        call,
		direction:   CallDirectionOutgoing,
		offerNodeID: "OFFER-STANZA-2",
	}

	eng.onCallAck(&waBinary.Node{
		Tag: "ack",
		Attrs: waBinary.Attrs{
			"class": "call",
			"type":  "offer",
			"id":    "OFFER-STANZA-2",
		},
		Content: []waBinary.Node{{
			Tag:   "error",
			Attrs: waBinary.Attrs{"code": "406"},
		}},
	})

	if call.State() != CallPhaseEnded {
		t.Fatalf("call state = %v, want ended", call.State())
	}
	if reason != "server:406" {
		t.Fatalf("end reason = %q, want server:406", reason)
	}
}

func TestOutgoingOfferAckWatchdogStopsAfterUsableAck(t *testing.T) {
	client := &Client{log: zerolog.Nop()}
	eng := newEngine(client)
	call := &Call{eng: eng, id: "CALL-ACKED", phase: CallPhaseCalling}
	eng.calls[call.id] = &engineCall{
		call:        call,
		direction:   CallDirectionOutgoing,
		offerNodeID: "OFFER-STANZA-ACKED",
	}

	eng.markOutgoingOfferAcked(call.id)
	oldTimeout := outgoingOfferAckTimeout
	outgoingOfferAckTimeout = 5 * time.Millisecond
	defer func() { outgoingOfferAckTimeout = oldTimeout }()
	eng.watchOutgoingOfferAck(call.id, "OFFER-STANZA-ACKED")

	if call.State() != CallPhaseCalling {
		t.Fatalf("call state = %v, want calling", call.State())
	}
	if eng.lookup(call.id) == nil {
		t.Fatal("acknowledged call was removed by watchdog")
	}
}

func TestOutgoingOfferAckWatchdogEndsUnacknowledgedCall(t *testing.T) {
	client := &Client{log: zerolog.Nop()}
	eng := newEngine(client)
	call := &Call{eng: eng, id: "CALL-3", phase: CallPhaseCalling}
	var reason string
	call.OnEnd(func(got string) { reason = got })
	eng.calls[call.id] = &engineCall{
		call:        call,
		direction:   CallDirectionOutgoing,
		offerNodeID: "OFFER-STANZA-3",
	}

	oldTimeout := outgoingOfferAckTimeout
	outgoingOfferAckTimeout = 5 * time.Millisecond
	defer func() { outgoingOfferAckTimeout = oldTimeout }()
	eng.watchOutgoingOfferAck(call.id, "OFFER-STANZA-3")

	if call.State() != CallPhaseEnded {
		t.Fatalf("call state = %v, want ended", call.State())
	}
	if reason != "server:no-offer-ack" {
		t.Fatalf("end reason = %q, want server:no-offer-ack", reason)
	}
}
