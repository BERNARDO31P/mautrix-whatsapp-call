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

	eng.onCallAck(directOfferAck(call.id, "OFFER-STANZA-ACKED"))
	oldTimeout := outgoingOfferAckTimeout
	outgoingOfferAckTimeout = 5 * time.Millisecond
	defer func() { outgoingOfferAckTimeout = oldTimeout }()
	eng.watchOutgoingOfferAck(call, "OFFER-STANZA-ACKED")

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
	eng.watchOutgoingOfferAck(call, "OFFER-STANZA-3")

	if call.State() != CallPhaseEnded {
		t.Fatalf("call state = %v, want ended", call.State())
	}
	if reason != "server:no-offer-ack" {
		t.Fatalf("end reason = %q, want server:no-offer-ack", reason)
	}
}

func TestCallErrorAckDoesNotEndUnrelatedOffer(t *testing.T) {
	for _, tc := range []struct {
		name  string
		attrs waBinary.Attrs
		acked bool
	}{
		{name: "old stanza", attrs: waBinary.Attrs{"id": "OLD"}},
		{name: "no correlation", attrs: waBinary.Attrs{}},
		{name: "conflicting call ID", attrs: waBinary.Attrs{"id": "CURRENT", "call-id": "OTHER"}},
		{name: "conflicting stanza ID", attrs: waBinary.Attrs{"id": "OLD", "call-id": "CALL"}},
		{name: "already acknowledged", attrs: waBinary.Attrs{"id": "CURRENT", "call-id": "CALL"}, acked: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			eng := newEngine(&Client{log: zerolog.Nop()})
			call := &Call{eng: eng, id: "CALL", phase: CallPhaseCalling}
			eng.calls[call.ID()] = &engineCall{call: call, direction: CallDirectionOutgoing, offerNodeID: "CURRENT"}
			if tc.acked {
				eng.onCallAck(directOfferAck(call.ID(), "CURRENT"))
			}
			tc.attrs["error"] = "406"
			eng.onCallAck(&waBinary.Node{Tag: "ack", Attrs: tc.attrs})
			if eng.lookup(call.ID()) == nil || call.State() != CallPhaseCalling {
				t.Fatal("unrelated or stale error ACK ended the call")
			}
		})
	}
}

func TestCallErrorAckCanCorrelateByEmbeddedCallID(t *testing.T) {
	eng := newEngine(&Client{log: zerolog.Nop()})
	call := &Call{eng: eng, id: "CALL", phase: CallPhaseCalling}
	eng.calls[call.ID()] = &engineCall{call: call, direction: CallDirectionOutgoing, offerNodeID: "CURRENT"}
	eng.onCallAck(&waBinary.Node{Tag: "ack", Attrs: waBinary.Attrs{"error": "406"}, Content: []waBinary.Node{{Tag: "error", Attrs: waBinary.Attrs{"call-id": "CALL"}}}})
	if call.State() != CallPhaseEnded || eng.lookup(call.ID()) != nil {
		t.Fatal("correlated embedded call ID did not end pending offer")
	}
}

func TestPendingOfferTerminationRechecksAcknowledgement(t *testing.T) {
	eng := newEngine(&Client{log: zerolog.Nop()})
	call := &Call{eng: eng, id: "CALL", phase: CallPhaseCalling}
	eng.calls[call.ID()] = &engineCall{call: call, direction: CallDirectionOutgoing, offerNodeID: "CURRENT"}
	matched, offer := eng.callForOfferError(&waBinary.Node{Tag: "ack", Attrs: waBinary.Attrs{"id": "CURRENT", "error": "406"}})
	if matched != call || offer != "CURRENT" {
		t.Fatal("expected pending offer")
	}
	eng.onCallAck(directOfferAck(call.ID(), "CURRENT"))
	if eng.finishPendingOffer(matched, offer, "server:406", false) || eng.finishPendingOffer(call, offer, "server:no-offer-ack", true) {
		t.Fatal("stale failure ended acknowledged call")
	}
	if call.State() != CallPhaseCalling || eng.lookup(call.ID()) == nil {
		t.Fatal("acknowledged call lost")
	}
}

func TestPendingOfferTerminationCannotEndReplacement(t *testing.T) {
	eng := newEngine(&Client{log: zerolog.Nop()})
	old := &Call{eng: eng, id: "CALL", phase: CallPhaseCalling}
	replacement := &Call{eng: eng, id: "CALL", phase: CallPhaseCalling}
	eng.calls[old.ID()] = &engineCall{call: replacement, direction: CallDirectionOutgoing, offerNodeID: "CURRENT"}
	if eng.finishPendingOffer(old, "CURRENT", "server:no-offer-ack", true) {
		t.Fatal("stale watchdog ended replacement")
	}
	if replacement.State() != CallPhaseCalling || eng.lookup(old.ID()) == nil {
		t.Fatal("replacement call lost")
	}
}

func directOfferAck(callID, stanzaID string) *waBinary.Node {
	return &waBinary.Node{Tag: "ack", Attrs: waBinary.Attrs{"id": stanzaID, "class": "call", "type": "offer"}, Content: []waBinary.Node{{Tag: "relay", Attrs: waBinary.Attrs{"call-id": callID}}}}
}

func TestDirectOfferAckCorrelatesBeforeRelayCommit(t *testing.T) {
	eng := newEngine(&Client{log: zerolog.Nop()})
	call := &Call{eng: eng, id: "CALL", phase: CallPhaseCalling}
	m := &engineCall{call: call, direction: CallDirectionOutgoing, offerNodeID: "CURRENT"}
	eng.calls[call.ID()] = m
	for _, id := range []string{"OLD", ""} {
		eng.onCallAck(directOfferAck(call.ID(), id))
		if m.offerNodeID != "CURRENT" || m.relay != nil {
			t.Fatal("unrelated ACK changed pending offer or relay")
		}
	}
	eng.onCallAck(directOfferAck(call.ID(), "CURRENT"))
	if m.offerNodeID != "" || m.relay == nil {
		t.Fatal("correlated ACK did not commit relay")
	}
	relay := m.relay
	eng.onCallAck(directOfferAck(call.ID(), "CURRENT"))
	if m.relay != relay {
		t.Fatal("duplicate ACK replaced the committed relay")
	}
}

// A group offer uses the same bounded ACK wait as a direct offer. A committed
// group roster or admitted media is progress even before the phase callback.
func TestGroupOfferAckWatchdog(t *testing.T) {
	oldTimeout := outgoingOfferAckTimeout
	outgoingOfferAckTimeout = 5 * time.Millisecond
	defer func() { outgoingOfferAckTimeout = oldTimeout }()
	for _, tc := range []struct {
		name                          string
		acked, roster, started, ended bool
	}{
		{name: "unacknowledged", ended: true},
		{name: "acknowledged", acked: true},
		{name: "roster committed", roster: true},
		{name: "media admitted", started: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			eng, call, _ := testGroupEngine("GROUP-WATCHDOG")
			call.phase = CallPhaseCalling
			m := eng.calls[call.ID()]
			m.direction = CallDirectionOutgoing
			m.offerNodeID = "OFFER"
			if tc.acked {
				m.offerNodeID = ""
			}
			if tc.roster {
				m.groupUpdate = &groupCallUpdate{CallID: call.ID(), TransactionID: 1}
			}
			m.started = tc.started
			eng.watchOutgoingOfferAck(call, "OFFER")
			if (call.State() == CallPhaseEnded) != tc.ended {
				t.Fatalf("unexpected terminal state: %v", call.State())
			}
			if (eng.lookup(call.ID()) == nil) != tc.ended {
				t.Fatal("unexpected registry removal")
			}
		})
	}
}
