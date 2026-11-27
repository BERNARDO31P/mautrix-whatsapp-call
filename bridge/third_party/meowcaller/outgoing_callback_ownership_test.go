package meowcaller

import (
	waBinary "go.mau.fi/whatsmeow/binary"
	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"
	"testing"
)

func TestStaleOutgoingCallbacksCannotAffectReplacement(t *testing.T) {
	eng, old := testEngineWithOutgoingCall()
	original := eng.lookup(old.ID())
	replacement := &Call{eng: eng, id: old.ID(), phase: CallPhaseCalling}
	current := &engineCall{call: replacement, direction: CallDirectionOutgoing, from: peerJID()}
	eng.calls[old.ID()] = current
	from := peerJID()
	from.Device = 7
	meta := types.BasicCallMeta{CallID: old.ID(), From: from}
	data := &waBinary.Node{Tag: "accept", Content: []waBinary.Node{{Tag: "capability", Attrs: waBinary.Attrs{"ver": "1"}, Content: []byte{1}}}}
	eng.onPreAcceptMatching(&events.CallPreAccept{BasicCallMeta: meta, Data: data}, original)
	eng.onAcceptMatching(&events.CallAccept{BasicCallMeta: meta, Data: data}, original)
	eng.onRejectMatching(&events.CallReject{BasicCallMeta: meta}, original)
	if eng.lookup(old.ID()) != current || current.from != peerJID() || replacement.State() != CallPhaseCalling || old.State() != CallPhaseCalling || current.peerLID != "" {
		t.Fatal("stale callback modified a call")
	}
}

func TestLateSignalingPhaseCannotRegressActiveCall(t *testing.T) {
	_, call := testEngineWithOutgoingCall()
	call.setPhase(CallPhaseActive)
	call.advancePhase(CallPhaseRinging)
	call.advancePhase(CallPhaseConnecting)
	if call.State() != CallPhaseActive {
		t.Fatal("phase regressed")
	}
	call.setPhase(CallPhaseEnded)
	call.advancePhase(CallPhaseConnecting)
	if call.State() != CallPhaseEnded {
		t.Fatal("terminal phase changed")
	}
}
