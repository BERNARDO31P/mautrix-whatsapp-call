package meowcaller

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/purpshell/meowcaller/signaling"
	waBinary "go.mau.fi/whatsmeow/binary"
	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"
)

type lifecycleAudioSource struct {
	closeCount int
}

func (s *lifecycleAudioSource) ReadFrame() ([]float32, error) {
	return nil, errors.New("unused")
}

func (s *lifecycleAudioSource) Close() error {
	s.closeCount++
	return nil
}

type lifecycleAudioSink struct {
	closeCount int
}

func (s *lifecycleAudioSink) WriteFrame([]float32) error {
	return nil
}

func (s *lifecycleAudioSink) Close() error {
	s.closeCount++
	return nil
}

func testEngineWithOutgoingCall() (*engine, *Call) {
	c := &Client{}
	c.eng = newEngine(c)
	call := &Call{eng: c.eng, id: "CID", peer: peerJID(), phase: CallPhaseCalling}
	c.eng.calls["CID"] = &engineCall{
		call:        call,
		direction:   CallDirectionOutgoing,
		from:        peerJID(),
		creator:     creatorJID(),
		localVideo:  true,
		remoteVideo: true,
	}
	return c.eng, call
}

func videoStateNode(state int) *waBinary.Node {
	return &waBinary.Node{Tag: "video", Attrs: waBinary.Attrs{
		"call-id": "CID", "call-creator": creatorJID(), "state": strconv.Itoa(state),
	}}
}

func senderVideoState(sender *videoSender) (active, gated bool) {
	sender.mu.Lock()
	defer sender.mu.Unlock()
	return sender.active, sender.sendGated
}

func TestCallVideoUpgradeGatesUntilPeerAcceptAndCanStop(t *testing.T) {
	eng, call := testEngineWithOutgoingCall()
	m := eng.calls[call.ID()]
	m.localVideo = false
	m.videoTx = &videoSender{}
	var sent []waBinary.Node
	eng.sendCallNode = func(_ context.Context, node waBinary.Node) error {
		sent = append(sent, node)
		return nil
	}

	if err := call.StartVideo(); err != nil {
		t.Fatalf("StartVideo: %v", err)
	}
	if len(sent) != 1 || sent[0].GetChildren()[0].AttrGetter().Int("state") != signaling.VideoStateUpgradeRequestV2 {
		t.Fatalf("StartVideo sent %#v, want one state=11 stanza", sent)
	}
	if orientation, ok := sent[0].GetChildren()[0].Attrs["device_orientation"]; !ok || orientation != "0" {
		t.Fatalf("StartVideo orientation = %v, want explicit 0", orientation)
	}
	if active, gated := senderVideoState(m.videoTx); !active || !gated {
		t.Fatalf("upgrade sender state = active:%v gated:%v, want true,true", active, gated)
	}

	eng.onVideoStanza(videoStateNode(signaling.VideoStateUpgradeAccept), eng.lookup("CID"), &waBinary.Node{Attrs: waBinary.Attrs{"from": peerJID()}}, 0)
	if len(sent) != 2 || sent[1].GetChildren()[0].AttrGetter().Int("state") != signaling.VideoStateEnabled {
		t.Fatalf("peer acceptance sent %#v, want state=1 announcement after state=4", sent)
	}
	if orientation, ok := sent[1].GetChildren()[0].Attrs["device_orientation"]; !ok || orientation != "0" {
		t.Fatalf("enabled announcement orientation = %v, want explicit 0", orientation)
	}
	if active, gated := senderVideoState(m.videoTx); !active || gated {
		t.Fatalf("accepted sender state = active:%v gated:%v, want true,false", active, gated)
	}

	if err := call.StopVideo(); err != nil {
		t.Fatalf("StopVideo: %v", err)
	}
	if len(sent) != 3 || sent[2].GetChildren()[0].AttrGetter().Int("state") != signaling.VideoStateStopped {
		t.Fatalf("StopVideo sent %#v, want trailing state=6 stanza", sent)
	}
	if active, _ := senderVideoState(m.videoTx); active {
		t.Fatal("video sender remained active after StopVideo")
	}
	if call.IsSendingVideo() {
		t.Fatal("call remained marked as sending video after StopVideo")
	}
	if !call.IsReceivingVideo() || !call.IsVideo() {
		t.Fatal("stopping local video also stopped the peer video flow")
	}
}

func TestSetVideoEnabledOnlyTogglesLocalFlow(t *testing.T) {
	eng, call := testEngineWithOutgoingCall()
	m := eng.calls[call.ID()]
	m.videoTx = &videoSender{active: true}
	var states []int
	eng.sendCallNode = func(_ context.Context, node waBinary.Node) error {
		states = append(states, node.GetChildren()[0].AttrGetter().Int("state"))
		return nil
	}

	if err := call.SetVideoEnabled(false); err != nil {
		t.Fatalf("SetVideoEnabled(false): %v", err)
	}
	if active, _ := senderVideoState(m.videoTx); active {
		t.Fatal("disabling local video left sender active")
	}
	if call.IsSendingVideo() || !call.IsReceivingVideo() {
		t.Fatalf("disabled state = send:%v receive:%v, want false,true", call.IsSendingVideo(), call.IsReceivingVideo())
	}

	if err := call.SetVideoEnabled(true); err != nil {
		t.Fatalf("SetVideoEnabled(true): %v", err)
	}
	if active, gated := senderVideoState(m.videoTx); !active || gated {
		t.Fatalf("enabled sender state = active:%v gated:%v, want true,false", active, gated)
	}
	if !call.IsSendingVideo() || !call.IsReceivingVideo() {
		t.Fatalf("enabled state = send:%v receive:%v, want true,true", call.IsSendingVideo(), call.IsReceivingVideo())
	}
	if len(states) != 2 || states[0] != signaling.VideoStateDisabled || states[1] != signaling.VideoStateEnabled {
		t.Fatalf("toggle states = %v, want [0 1]", states)
	}
}

func TestCallAcceptVideoPreservesDisabledLocalFlow(t *testing.T) {
	eng, call := testEngineWithOutgoingCall()
	m := eng.calls[call.ID()]
	m.localVideo = false
	m.peerVideoUpgrade = true
	m.videoTx = &videoSender{}
	var states []int
	eng.sendCallNode = func(_ context.Context, node waBinary.Node) error {
		states = append(states, node.GetChildren()[0].AttrGetter().Int("state"))
		return nil
	}

	if err := call.AcceptVideo(); err != nil {
		t.Fatalf("AcceptVideo: %v", err)
	}
	if len(states) != 2 || states[0] != signaling.VideoStateStopped || states[1] != signaling.VideoStateUpgradeAccept {
		t.Fatalf("AcceptVideo states = %v, want [6 4]", states)
	}
	if active, gated := senderVideoState(m.videoTx); active || gated {
		t.Fatalf("accepted sender state = active:%v gated:%v, want false,false", active, gated)
	}
	if call.IsSendingVideo() {
		t.Fatal("accepting peer video enabled the local sender")
	}
}

func TestCallAcceptVideoPreservesEnabledLocalFlow(t *testing.T) {
	eng, call := testEngineWithOutgoingCall()
	m := eng.calls[call.ID()]
	m.peerVideoUpgrade = true
	m.videoTx = &videoSender{active: true}
	var states []int
	eng.sendCallNode = func(_ context.Context, node waBinary.Node) error {
		states = append(states, node.GetChildren()[0].AttrGetter().Int("state"))
		return nil
	}

	if err := call.AcceptVideo(); err != nil {
		t.Fatalf("AcceptVideo: %v", err)
	}
	if len(states) != 1 || states[0] != signaling.VideoStateUpgradeAccept {
		t.Fatalf("AcceptVideo states = %v, want [4]", states)
	}
	if active, gated := senderVideoState(m.videoTx); !active || gated {
		t.Fatalf("accepted sender state = active:%v gated:%v, want true,false", active, gated)
	}
}

func TestInboundVideoStopOnlyDisablesRemoteFlow(t *testing.T) {
	eng, call := testEngineWithOutgoingCall()
	m := eng.calls[call.ID()]
	m.videoTx = &videoSender{active: true}
	var got VideoState
	call.OnVideoState(func(state VideoState) { got = state })

	eng.onVideoStanza(videoStateNode(signaling.VideoStateStopped), eng.lookup("CID"), &waBinary.Node{Attrs: waBinary.Attrs{"from": peerJID()}}, 0)

	if got.Raw != signaling.VideoStateStopped {
		t.Fatalf("video callback state = %d, want 6", got.Raw)
	}
	if active, _ := senderVideoState(m.videoTx); !active {
		t.Fatal("peer stopping video disabled the local sender")
	}
	if !call.IsSendingVideo() || call.IsReceivingVideo() || !call.IsVideo() {
		t.Fatalf("directional state after peer stop = send:%v receive:%v any:%v, want true,false,true",
			call.IsSendingVideo(), call.IsReceivingVideo(), call.IsVideo())
	}
}

func TestInboundVideoEnabledDoesNotRestartLocalFlow(t *testing.T) {
	eng, call := testEngineWithOutgoingCall()
	m := eng.calls[call.ID()]
	m.localVideo = false
	m.remoteVideo = false
	m.videoTx = &videoSender{}

	eng.onVideoStanza(videoStateNode(signaling.VideoStateEnabled), eng.lookup("CID"), &waBinary.Node{Attrs: waBinary.Attrs{"from": peerJID()}}, 0)

	if active, _ := senderVideoState(m.videoTx); active {
		t.Fatal("peer enabling video restarted the local sender")
	}
	if call.IsSendingVideo() || !call.IsReceivingVideo() {
		t.Fatalf("directional state after peer enable = send:%v receive:%v, want false,true",
			call.IsSendingVideo(), call.IsReceivingVideo())
	}
}

func TestInboundVideoEnabledAcceptsPendingLocalUpgrade(t *testing.T) {
	eng, call := testEngineWithOutgoingCall()
	m := eng.calls[call.ID()]
	m.localVideo = true
	m.remoteVideo = false
	m.videoGate = true
	m.videoTx = &videoSender{active: true, sendGated: true}
	var keyframeRequests int
	call.OnVideoKeyframeRequest(func() { keyframeRequests++ })

	eng.onVideoStanza(videoStateNode(signaling.VideoStateEnabled), eng.lookup("CID"), &waBinary.Node{Attrs: waBinary.Attrs{"from": peerJID()}}, 0)

	if active, gated := senderVideoState(m.videoTx); !active || gated {
		t.Fatalf("pending sender after peer enabled = active:%v gated:%v, want true,false", active, gated)
	}
	if !call.IsSendingVideo() || !call.IsReceivingVideo() {
		t.Fatalf("video flows after peer enabled = send:%v receive:%v, want true,true",
			call.IsSendingVideo(), call.IsReceivingVideo())
	}
	if keyframeRequests != 1 {
		t.Fatalf("keyframe requests = %d, want 1", keyframeRequests)
	}
}

func TestInboundVideoUpgradeWaitsForExplicitAcceptance(t *testing.T) {
	eng, call := testEngineWithOutgoingCall()
	m := eng.calls[call.ID()]
	m.localVideo = false
	m.videoTx = &videoSender{}
	var sent int
	eng.sendCallNode = func(context.Context, waBinary.Node) error {
		sent++
		return nil
	}
	var got VideoState
	call.OnVideoState(func(state VideoState) { got = state })

	eng.onVideoStanza(videoStateNode(signaling.VideoStateUpgradeRequestV2), eng.lookup("CID"), &waBinary.Node{Attrs: waBinary.Attrs{"from": peerJID()}}, 0)

	if !got.Upgrade || got.Raw != signaling.VideoStateUpgradeRequestV2 {
		t.Fatalf("video callback = %+v, want pending state 11", got)
	}
	if sent != 0 {
		t.Fatalf("inbound upgrade auto-sent %d signaling stanzas", sent)
	}
	if active, _ := senderVideoState(m.videoTx); active {
		t.Fatal("inbound upgrade activated video before explicit acceptance")
	}
	if !m.peerVideoUpgrade {
		t.Fatal("inbound upgrade was not marked pending")
	}
}

func TestVideoAcceptanceRequestsSourceKeyframe(t *testing.T) {
	eng, call := testEngineWithOutgoingCall()
	eng.calls[call.ID()].videoTx = &videoSender{}
	eng.sendCallNode = func(context.Context, waBinary.Node) error { return nil }
	var requests int
	call.OnVideoKeyframeRequest(func() { requests++ })

	eng.onVideoStanza(videoStateNode(signaling.VideoStateUpgradeAccept), eng.lookup("CID"), &waBinary.Node{Attrs: waBinary.Attrs{"from": peerJID()}}, 0)
	if requests != 0 {
		t.Fatal("unsolicited acceptance opened camera")
	}
	if err := call.StartVideo(); err != nil {
		t.Fatal(err)
	}
	eng.onVideoStanza(videoStateNode(signaling.VideoStateUpgradeAccept), eng.lookup("CID"), &waBinary.Node{Attrs: waBinary.Attrs{"from": peerJID()}}, 0)

	if requests != 1 {
		t.Fatalf("keyframe requests = %d, want 1", requests)
	}
}

func TestCallSetsVideoOrientation(t *testing.T) {
	eng, call := testEngineWithOutgoingCall()
	var sent waBinary.Node
	eng.sendCallNode = func(_ context.Context, node waBinary.Node) error {
		sent = node
		return nil
	}

	if err := call.SetVideoOrientation(2); err != nil {
		t.Fatalf("SetVideoOrientation: %v", err)
	}
	video := sent.GetChildren()[0].AttrGetter()
	if video.Int("state") != signaling.VideoStateEnabled || video.Int("device_orientation") != 2 {
		t.Fatalf("orientation stanza attrs = %#v", sent.GetChildren()[0].Attrs)
	}
	if err := call.SetVideoOrientation(4); err == nil {
		t.Fatal("SetVideoOrientation accepted orientation 4")
	}
}

func TestHangupTearsDownLocallyWhenSignalingFails(t *testing.T) {
	eng, call := testEngineWithOutgoingCall()
	var canceled bool
	eng.calls[call.ID()].cancel = func() { canceled = true }
	eng.sendCallNode = func(context.Context, waBinary.Node) error {
		return errors.New("network unavailable")
	}
	var reason string
	call.OnEnd(func(got string) { reason = got })

	err := call.Hangup()

	if err == nil {
		t.Fatal("Hangup returned nil after signaling failure")
	}
	if !canceled {
		t.Fatal("Hangup did not cancel local media")
	}
	if call.State() != CallPhaseEnded || reason != "hangup" {
		t.Fatalf("local end state = (%d, %q), want (Ended, hangup)", call.State(), reason)
	}
	if eng.lookup(call.ID()) != nil {
		t.Fatal("Hangup retained ended call in engine registry")
	}
}

func TestOutgoingPeerAcceptLifecycle(t *testing.T) {
	eng, call := testEngineWithOutgoingCall()
	from := types.JID{User: "222222222222222", Server: types.HiddenUserServer}

	eng.onPreAccept(&events.CallPreAccept{
		BasicCallMeta: types.BasicCallMeta{CallID: "CID", From: from, CallCreator: creatorJID()},
	})
	if got := call.State(); got != CallPhaseRinging {
		t.Fatalf("after preaccept phase = %d, want Ringing", got)
	}

	eng.onAccept(&events.CallAccept{
		BasicCallMeta: types.BasicCallMeta{CallID: "CID", From: from, CallCreator: creatorJID()},
		Data:          &waBinary.Node{Tag: "accept"},
	})
	if got := call.State(); got != CallPhaseConnecting {
		t.Fatalf("after accept phase = %d, want Connecting", got)
	}
}

func TestOutgoingAcceptRekeysToAnsweringDevice(t *testing.T) {
	eng, call := testEngineWithOutgoingCall()
	m := eng.calls[call.ID()]
	m.peerLID = peerJID().String()
	answeringDevice := peerJID()
	answeringDevice.Device = 7
	var rekeyed string
	m.rekeyPeer = func(peer string) error {
		rekeyed = peer
		return nil
	}

	eng.onAccept(&events.CallAccept{
		BasicCallMeta: types.BasicCallMeta{CallID: call.ID(), From: answeringDevice, CallCreator: creatorJID()},
		Data:          &waBinary.Node{Tag: "accept"},
	})

	if rekeyed != answeringDevice.String() {
		t.Fatalf("rekeyed peer = %q, want %q", rekeyed, answeringDevice.String())
	}
	eng.mu.Lock()
	gotPeer, gotFrom := m.peerLID, m.from
	eng.mu.Unlock()
	if gotPeer != answeringDevice.String() || gotFrom != answeringDevice {
		t.Fatalf("accepted routing = (%q, %s), want (%q, %s)", gotPeer, gotFrom, answeringDevice.String(), answeringDevice)
	}
}

func TestParseRelayDataResolvesElectedPeerDevice(t *testing.T) {
	primary := peerJID()
	companion := peerJID()
	companion.Device = 7
	relay := &waBinary.Node{
		Tag:   "relay",
		Attrs: waBinary.Attrs{"peer_pid": "2", "self_pid": "1"},
		Content: []waBinary.Node{
			{Tag: "participant", Attrs: waBinary.Attrs{"pid": "0", "jid": primary}},
			{Tag: "participant", Attrs: waBinary.Attrs{"pid": "2", "jid": companion}},
		},
	}

	rd := parseRelayData(relay)

	if rd.peerJID != companion {
		t.Fatalf("relay peer = %s, want %s", rd.peerJID, companion)
	}
}

func TestRelayRekeysToElectedPeerDevice(t *testing.T) {
	eng, call := testEngineWithOutgoingCall()
	m := eng.calls[call.ID()]
	m.peerLID = peerJID().String()
	companion := peerJID()
	companion.Device = 7
	var rekeyed string
	m.rekeyPeer = func(peer string) error {
		rekeyed = peer
		return nil
	}
	relay := &waBinary.Node{
		Tag:   "relay",
		Attrs: waBinary.Attrs{"peer_pid": "2"},
		Content: []waBinary.Node{
			{Tag: "participant", Attrs: waBinary.Attrs{"pid": "2", "jid": companion}},
		},
	}

	eng.onRelay(types.BasicCallMeta{CallID: call.ID(), From: peerJID(), CallCreator: creatorJID()}, relay)

	if rekeyed != companion.String() {
		t.Fatalf("rekeyed peer = %q, want %q", rekeyed, companion.String())
	}
	eng.mu.Lock()
	gotPeer := m.peerLID
	eng.mu.Unlock()
	if gotPeer != companion.String() {
		t.Fatalf("stored peer = %q, want %q", gotPeer, companion.String())
	}
}

func TestUnqualifiedAcceptPreservesRelayElectedPeerDevice(t *testing.T) {
	eng, call := testEngineWithOutgoingCall()
	m := eng.calls[call.ID()]
	m.peerLID = peerJID().String()
	companion := peerJID()
	companion.Device = 7
	var rekeyed []string
	m.rekeyPeer = func(peer string) error {
		rekeyed = append(rekeyed, peer)
		return nil
	}
	relay := &waBinary.Node{
		Tag:   "relay",
		Attrs: waBinary.Attrs{"peer_pid": "2"},
		Content: []waBinary.Node{
			{Tag: "participant", Attrs: waBinary.Attrs{"pid": "2", "jid": companion}},
		},
	}
	eng.onRelay(types.BasicCallMeta{CallID: call.ID(), From: peerJID(), CallCreator: creatorJID()}, relay)

	eng.onAccept(&events.CallAccept{
		BasicCallMeta: types.BasicCallMeta{CallID: call.ID(), From: peerJID(), CallCreator: creatorJID()},
		Data:          &waBinary.Node{Tag: "accept"},
	})

	if len(rekeyed) != 1 || rekeyed[0] != companion.String() {
		t.Fatalf("rekeyed peers = %v, want only %q", rekeyed, companion.String())
	}
	eng.mu.Lock()
	gotPeer := m.peerLID
	eng.mu.Unlock()
	if gotPeer != companion.String() {
		t.Fatalf("stored peer after accept = %q, want %q", gotPeer, companion.String())
	}
}

func TestOutgoingPeerAcceptCallbackFiresOnceAfterMediaStarted(t *testing.T) {
	eng, call := testEngineWithOutgoingCall()
	call.setPhase(CallPhaseConnecting)
	var accepted int
	call.OnPeerAccept(func() { accepted++ })
	event := &events.CallAccept{
		BasicCallMeta: types.BasicCallMeta{CallID: "CID", From: peerJID(), CallCreator: creatorJID()},
		Data:          &waBinary.Node{Tag: "accept"},
	}

	eng.onAccept(event)
	eng.onAccept(event)

	if accepted != 1 {
		t.Fatalf("peer accept callbacks = %d, want 1", accepted)
	}
}

func TestOutgoingPeerAcceptCallbackReplaysAfterRegistration(t *testing.T) {
	eng, call := testEngineWithOutgoingCall()
	eng.onAccept(&events.CallAccept{
		BasicCallMeta: types.BasicCallMeta{CallID: "CID", From: peerJID(), CallCreator: creatorJID()},
		Data:          &waBinary.Node{Tag: "accept"},
	})
	var accepted int

	call.OnPeerAccept(func() { accepted++ })

	if accepted != 1 {
		t.Fatalf("late peer accept callbacks = %d, want 1", accepted)
	}
}

func TestOutgoingPeerAcceptIgnoredAfterCallEnded(t *testing.T) {
	eng, call := testEngineWithOutgoingCall()
	call.setPhase(CallPhaseEnded)
	var accepted int
	call.OnPeerAccept(func() { accepted++ })

	eng.onAccept(&events.CallAccept{
		BasicCallMeta: types.BasicCallMeta{CallID: "CID", From: peerJID(), CallCreator: creatorJID()},
		Data:          &waBinary.Node{Tag: "accept"},
	})

	if accepted != 0 {
		t.Fatalf("peer accept callbacks after end = %d, want 0", accepted)
	}
}

func TestOutgoingPeerAcceptDoesNotRegressActiveCall(t *testing.T) {
	eng, call := testEngineWithOutgoingCall()
	call.setPhase(CallPhaseActive)

	eng.onPreAccept(&events.CallPreAccept{BasicCallMeta: types.BasicCallMeta{CallID: "CID", CallCreator: creatorJID(), From: peerJID()}})
	eng.onAccept(&events.CallAccept{
		BasicCallMeta: types.BasicCallMeta{CallID: "CID", CallCreator: creatorJID(), From: peerJID()},
		Data:          &waBinary.Node{Tag: "accept"},
	})
	if got := call.State(); got != CallPhaseActive {
		t.Fatalf("phase = %d, want Active", got)
	}
}

func TestPeerRejectEndsCall(t *testing.T) {
	eng, call := testEngineWithOutgoingCall()
	var reason string
	call.OnEnd(func(r string) { reason = r })

	eng.onReject(&events.CallReject{BasicCallMeta: types.BasicCallMeta{CallID: "CID", CallCreator: creatorJID(), From: peerJID()}})
	if got := call.State(); got != CallPhaseEnded {
		t.Fatalf("phase = %d, want Ended", got)
	}
	if reason != "rejected" {
		t.Fatalf("reason = %q, want rejected", reason)
	}
}

func TestFinishCallClosesAttachedAudioDevices(t *testing.T) {
	source := &lifecycleAudioSource{}
	sink := &lifecycleAudioSink{}
	call := &Call{id: "CALL", phase: CallPhaseActive}
	call.Play(source)
	call.Receive(sink)
	eng := &engine{calls: map[string]*engineCall{
		"CALL": {call: call},
	}}

	eng.finishCall("CALL", "done")

	if source.closeCount != 1 {
		t.Fatalf("audio source close count = %d, want 1", source.closeCount)
	}
	if sink.closeCount != 1 {
		t.Fatalf("audio sink close count = %d, want 1", sink.closeCount)
	}
}

func TestSetMutedSendsMuteV2WithoutChangingCallLifecycle(t *testing.T) {
	eng, call := testEngineWithOutgoingCall()
	call.setPhase(CallPhaseActive)
	var states []string
	eng.sendCallNode = func(_ context.Context, node waBinary.Node) error {
		children := node.GetChildren()
		if len(children) != 1 || children[0].Tag != "mute_v2" {
			t.Fatalf("unexpected mute stanza: %#v", node)
		}
		states = append(states, children[0].AttrGetter().String("mute-state"))
		return nil
	}
	if err := call.SetMuted(true); err != nil {
		t.Fatalf("SetMuted(true): %v", err)
	}
	if err := call.SetMuted(false); err != nil {
		t.Fatalf("SetMuted(false): %v", err)
	}
	if len(states) != 2 || states[0] != "1" || states[1] != "0" {
		t.Fatalf("mute states = %v, want [1 0]", states)
	}
	if call.State() != CallPhaseActive {
		t.Fatalf("mute changed lifecycle phase to %d", call.State())
	}
}

func TestMatrixPickupLatchesMuteAndReturnsAcceptError(t *testing.T) {
	eng, call := testEngineWithOutgoingCall()
	recv := &waBinary.Node{Tag: "call", Attrs: waBinary.Attrs{"from": peerJID()}, Content: []waBinary.Node{{Tag: "mute_v2", Attrs: waBinary.Attrs{"call-id": call.ID(), "call-creator": creatorJID(), "mute-state": "1"}}}}
	eng.onCallRaw(recv)
	muted := false
	call.OnMuteState(func(state bool) { muted = state })
	if !muted {
		t.Fatal("lost early remote mute")
	}
	eng.sendCallNode = func(context.Context, waBinary.Node) error { return fmt.Errorf("accept fixture") }
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := call.AnswerContext(ctx); err == nil || !strings.Contains(err.Error(), "accept fixture") {
		t.Fatalf("pickup result %v", err)
	}
	eng.finishCall(call.ID(), "ended")
	reason := ""
	call.OnEnd(func(value string) { reason = value })
	if reason != "ended" || !eng.recentlyEnded(call.ID()) {
		t.Fatal("terminal event not retained")
	}
}

func TestTerminalCallRejectsLatePhaseAndReady(t *testing.T) {
	call := &Call{phase: CallPhaseConnecting}
	var stateCalls, readyCalls int
	call.OnStateChange(func(CallPhase) { stateCalls++ })
	call.OnReady(func() { readyCalls++ })
	call.setPhase(CallPhaseEnded)
	for _, phase := range []CallPhase{CallPhaseRinging, CallPhaseConnecting, CallPhaseActive, CallPhaseWaitingRoom} {
		call.setPhase(phase)
	}
	if fn := call.onReadyFn(); fn != nil {
		fn()
	}
	call.OnReady(func() { readyCalls++ })
	if call.State() != CallPhaseEnded || stateCalls != 1 || readyCalls != 0 || call.mediaReady {
		t.Fatal("terminal call resurrected or emitted readiness")
	}
}

func TestTerminalCallSuppressesAcceptanceReplay(t *testing.T) {
	call := &Call{phase: CallPhaseConnecting}
	call.markPeerAccepted()
	call.setPhase(CallPhaseEnded)
	var accepted int
	call.OnPeerAccept(func() { accepted++ })
	call.markPeerAccepted()
	if accepted != 0 {
		t.Fatal("terminal call replayed acceptance")
	}
}

func TestTerminalCallSuppressesReadyReplay(t *testing.T) {
	call := &Call{phase: CallPhaseActive}
	call.onReadyFn()
	call.setPhase(CallPhaseEnded)
	call.OnReady(func() { t.Fatal("terminal call replayed readiness") })
}

func TestConcurrentLatePhaseCannotResurrectTerminalCall(t *testing.T) {
	call := &Call{phase: CallPhaseConnecting}
	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; i < 1000; i++ {
			call.setPhase(CallPhaseActive)
			call.markPeerAccepted()
			call.onReadyFn()
		}
	}()
	call.setPhase(CallPhaseEnded)
	<-done
	if call.State() != CallPhaseEnded {
		t.Fatal("terminal phase changed")
	}
}

func TestTerminateBeforeOfferReservesCallIdentity(t *testing.T) {
	c := &Client{}
	eng := newEngine(c)
	meta := types.BasicCallMeta{CallID: "early", From: peerJID(), CallCreator: peerJID()}
	eng.onTerminateEvent(&events.CallTerminate{BasicCallMeta: meta, Reason: "hangup"})
	eng.mu.Lock()
	reserved := hasEarlyTermination(eng.earlyTerminations, meta, false, time.Now())
	eng.mu.Unlock()
	if !reserved {
		t.Fatal("early termination not remembered")
	}
	// No WA client is installed: a late offer must return before key decoding.
	offer := &events.CallOffer{BasicCallMeta: meta}
	offer.Data = &waBinary.Node{Tag: "offer"}
	eng.onOffer(offer)
	if eng.lookup("early") != nil {
		t.Fatal("late offer recreated terminal call")
	}
	eng.finishCall("unrelated", "local")
	eng.mu.Lock()
	defer eng.mu.Unlock()
	if eng.recentlyEnded("unrelated") {
		t.Fatal("unrelated local cleanup reserved an unknown call")
	}
}

func TestDirectControlsRejectUnrelatedSenderAndCreator(t *testing.T) {
	eng, call := testEngineWithOutgoingCall()
	m := eng.lookup(call.ID())
	unrelated := types.NewJID("333333333333333", types.HiddenUserServer)
	mute := waBinary.Node{Tag: "mute_v2", Attrs: waBinary.Attrs{"call-id": call.ID(), "call-creator": creatorJID(), "mute-state": "1"}}
	eng.onCallRaw(&waBinary.Node{Tag: "call", Attrs: waBinary.Attrs{"from": unrelated}, Content: []waBinary.Node{mute}})
	if m.seenMute {
		t.Fatal("unrelated sender released pickup mute latch")
	}
	eng.onVideoStanza(videoStateNode(signaling.VideoStateStopped), m, &waBinary.Node{Attrs: waBinary.Attrs{"from": unrelated}}, 0)
	if !m.remoteVideo {
		t.Fatal("unrelated sender disabled remote video")
	}
	eng.onTerminateEvent(&events.CallTerminate{BasicCallMeta: types.BasicCallMeta{CallID: call.ID(), From: peerJID(), CallCreator: unrelated}, Reason: "hangup"})
	if call.State() == CallPhaseEnded {
		t.Fatal("unrelated creator ended call")
	}
	eng.onTerminateEvent(&events.CallTerminate{BasicCallMeta: types.BasicCallMeta{CallID: call.ID(), From: peerJID(), CallCreator: creatorJID()}, Reason: "hangup"})
	if call.State() != CallPhaseEnded {
		t.Fatal("matching termination ignored")
	}
}

func TestDismissUnansweredIsLocalAndPickupFenced(t *testing.T) {
	for _, tc := range []struct {
		name                                     string
		group, pending, started, outgoing, stale bool
		phase                                    CallPhase
		want                                     bool
	}{
		{name: "direct", phase: CallPhaseRinging, want: true},
		{name: "group", group: true, phase: CallPhaseRinging, want: true},
		{name: "pickup pending", pending: true, phase: CallPhaseRinging},
		{name: "media started", started: true, phase: CallPhaseRinging},
		{name: "connecting", phase: CallPhaseConnecting},
		{name: "outgoing", outgoing: true, phase: CallPhaseRinging},
		{name: "replacement", stale: true, phase: CallPhaseRinging},
	} {
		t.Run(tc.name, func(t *testing.T) {
			eng, call := testEngineWithOutgoingCall()
			m := eng.calls[call.ID()]
			call.phase = tc.phase
			if !tc.outgoing {
				m.direction = CallDirectionIncoming
			}
			m.group, m.acceptPending, m.started = tc.group, tc.pending, tc.started
			if tc.stale {
				m.call = &Call{eng: eng, id: call.ID(), phase: tc.phase}
			}
			sends := 0
			eng.sendCallNode = func(context.Context, waBinary.Node) error { sends++; return nil }
			var reason string
			call.OnEnd(func(got string) { reason = got })
			if got := call.DismissUnanswered(); got != tc.want {
				t.Fatalf("dismiss = %t, want %t", got, tc.want)
			}
			if sends != 0 {
				t.Fatalf("local dismissal emitted %d signaling nodes", sends)
			}
			if tc.want {
				if eng.lookup(call.ID()) != nil || call.State() != CallPhaseEnded || reason != "local-frontend-unavailable" {
					t.Fatal("local call was not retired")
				}
				if call.DismissUnanswered() {
					t.Fatal("dismissal repeated")
				}
			} else if eng.lookup(call.ID()) != m || call.State() != tc.phase || reason != "" {
				t.Fatal("dismissal disturbed another call or pickup")
			}
		})
	}
}

func TestDirectLifecycleCorrelatesSenderAndCreator(t *testing.T) {
	eng, call := testEngineWithOutgoingCall()
	m := eng.lookup(call.ID())
	m.peerLID = peerJID().String()
	unrelated := types.NewJID("333333333333333", types.HiddenUserServer)
	for _, meta := range []types.BasicCallMeta{
		{CallID: call.ID(), From: unrelated, CallCreator: creatorJID()},
		{CallID: call.ID(), From: peerJID(), CallCreator: unrelated},
		{CallID: call.ID(), From: peerJID(), CallCreator: creatorJID(), GroupJID: types.NewJID("group", types.GroupServer)},
		{CallID: call.ID(), From: peerJID()},
	} {
		eng.onPreAccept(&events.CallPreAccept{BasicCallMeta: meta})
		eng.onAccept(&events.CallAccept{BasicCallMeta: meta, Data: &waBinary.Node{Tag: "accept"}})
		eng.onReject(&events.CallReject{BasicCallMeta: meta})
		if eng.lookup(call.ID()) != m || call.State() != CallPhaseCalling || m.from != peerJID() || m.peerLID != peerJID().String() {
			t.Fatal("unrelated lifecycle event modified direct call")
		}
	}
	answering := peerJID()
	answering.Device = 7
	meta := types.BasicCallMeta{CallID: call.ID(), From: answering, CallCreator: creatorJID()}
	eng.onPreAccept(&events.CallPreAccept{BasicCallMeta: meta})
	eng.onAccept(&events.CallAccept{BasicCallMeta: meta, Data: &waBinary.Node{Tag: "accept"}})
	meta.From = peerJID()
	eng.onAccept(&events.CallAccept{BasicCallMeta: meta, Data: &waBinary.Node{Tag: "accept"}})
	if m.from != answering || call.State() != CallPhaseConnecting {
		t.Fatal("bare accept replay lost answering device")
	}
	meta.From = answering
	meta.From.Device++
	eng.onReject(&events.CallReject{BasicCallMeta: meta})
	if call.State() == CallPhaseEnded {
		t.Fatal("other device rejected selected call")
	}
}

func TestIncomingPreparationAdmission(t *testing.T) {
	for _, group := range []bool{false, true} {
		for _, allow := range []bool{false, true} {
			eng, call := testEngineWithOutgoingCall()
			m := eng.calls[call.ID()]
			m.direction, m.group, call.phase = CallDirectionIncoming, group, CallPhaseRinging
			checks, sends := 0, 0
			eng.sendCallNode = func(context.Context, waBinary.Node) error { sends++; return nil }
			eng.c.SetIncomingCallAdmission(func(got *Call) bool {
				checks++
				if got != call {
					t.Fatal("wrong admission owner")
				}
				return allow
			})
			if got := eng.admitIncomingPreparation(call); got != allow {
				t.Fatalf("admitted %t, want %t", got, allow)
			}
			if checks != 1 || sends != 0 {
				t.Fatalf("checks=%d sends=%d", checks, sends)
			}
			if !allow && (eng.lookup(call.ID()) != nil || call.State() != CallPhaseEnded) {
				t.Fatal("declined preparation retained call")
			}
			if allow && eng.lookup(call.ID()) != m {
				t.Fatal("admitted preparation lost call")
			}
		}
	}
}

func TestIncomingPreparationChecksOwnershipAfterPolicy(t *testing.T) {
	eng, call := testEngineWithOutgoingCall()
	eng.calls[call.ID()].direction, call.phase = CallDirectionIncoming, CallPhaseRinging
	eng.c.SetIncomingCallAdmission(func(got *Call) bool {
		got.DismissUnanswered()
		return true
	})
	if eng.admitIncomingPreparation(call) {
		t.Fatal("admitted retired call")
	}
}

func TestIncomingOfferFactoryScopesCallbacks(t *testing.T) {
	eng, call := testEngineWithOutgoingCall()
	eng.calls[call.ID()].direction, call.phase = CallDirectionIncoming, CallPhaseRinging
	generation, delivered := 1, 0
	eng.c.SetIncomingOfferFactory(func() (func(*Call) bool, func(*Call)) {
		captured := generation
		return func(got *Call) bool { return got == call && generation == captured }, func(got *Call) {
			if got == call && generation == captured {
				delivered++
			}
		}
	})
	admit, deliver := eng.c.incomingOfferCallbacks()
	if !eng.admitIncomingPreparationWith(call, admit) {
		t.Fatal("fresh scope rejected")
	}
	generation++
	deliver(call)
	if delivered != 0 || admit(call) {
		t.Fatal("old scope crossed generation")
	}
	freshAdmit, freshDeliver := eng.c.incomingOfferCallbacks()
	if !freshAdmit(call) {
		t.Fatal("fresh scope rejected")
	}
	freshDeliver(call)
	if delivered != 1 {
		t.Fatal("fresh delivery lost")
	}
}

func TestEndLocallyDoesNotSignalOrRetireReplacement(t *testing.T) {
	eng, call := testEngineWithOutgoingCall()
	eng.sendCallNode = func(context.Context, waBinary.Node) error { t.Fatal("local teardown sent a stanza"); return nil }
	if !call.EndLocally("whatsapp-stream-replaced") || call.State() != CallPhaseEnded {
		t.Fatal("old call not retired")
	}
	replacement := &Call{eng: eng, id: call.id, phase: CallPhaseCalling}
	eng.calls[call.id] = &engineCall{call: replacement}
	if call.EndLocally("stale") || replacement.State() == CallPhaseEnded {
		t.Fatal("stale local teardown retired replacement")
	}
}

func TestRelayRejectsUnrelatedDirectEventAndElectedPeer(t *testing.T) {
	eng, call := testEngineWithOutgoingCall()
	m := eng.lookup(call.ID())
	m.peerLID = peerJID().String()
	unrelated := types.NewJID("333333333333333", types.HiddenUserServer)
	relay := func(peer types.JID) *waBinary.Node {
		return &waBinary.Node{Tag: "relay", Attrs: waBinary.Attrs{"peer_pid": "2"}, Content: []waBinary.Node{{Tag: "participant", Attrs: waBinary.Attrs{"pid": "2", "jid": peer}}}}
	}
	meta := types.BasicCallMeta{CallID: call.ID(), From: unrelated, CallCreator: creatorJID()}
	eng.onRelay(meta, relay(peerJID()))
	if m.relay != nil {
		t.Fatal("unrelated sender installed relay")
	}
	meta.From = peerJID()
	eng.onRelay(meta, relay(unrelated))
	if m.relay != nil {
		t.Fatal("unrelated elected peer installed relay")
	}
	eng.onRelay(meta, relay(peerJID()))
	if m.relay == nil || m.relay.peerJID != peerJID() {
		t.Fatal("matching relay ignored")
	}
}

func TestEarlyTerminateDoesNotReserveUnrelatedCallIdentity(t *testing.T) {
	eng := newEngine(&Client{})
	meta := types.BasicCallMeta{CallID: "cid", From: peerJID(), CallCreator: peerJID()}
	eng.onTerminateEvent(&events.CallTerminate{BasicCallMeta: meta, Reason: "hangup"})
	if eng.recentlyEnded(meta.CallID) {
		t.Fatal("unknown termination globally reserved call ID")
	}
	other := meta
	other.From = creatorJID()
	other.CallCreator = creatorJID()
	if hasEarlyTermination(eng.earlyTerminations, other, false, time.Now()) {
		t.Fatal("unrelated offer reserved")
	}
	if !hasEarlyTermination(eng.earlyTerminations, meta, false, time.Now()) {
		t.Fatal("matching offer not reserved")
	}
}

func TestTerminalCommitRechecksSelectedDirectDevice(t *testing.T) {
	eng, call := testEngineWithOutgoingCall()
	m := eng.lookup(call.ID())
	old := peerJID()
	old.Device = 3
	selected := old
	selected.Device = 4
	m.from = selected
	meta := types.BasicCallMeta{CallID: call.ID(), From: old, CallCreator: creatorJID()}
	if eng.finishCallMatchingOffer(call.ID(), "hangup", false, call, "", false, false, &meta) || eng.lookup(call.ID()) != m {
		t.Fatal("different device retired current call")
	}
	meta.From = selected
	if !eng.finishCallMatchingOffer(call.ID(), "hangup", false, call, "", false, false, &meta) {
		t.Fatal("matching selected device could not end call")
	}
}

func TestCrossedVideoUpgradeDecisionsAreDirectional(t *testing.T) {
	for _, tc := range []struct {
		name                string
		state               int
		wantLocal, wantPeer bool
	}{
		{"peer cancels its request", signaling.VideoStateUpgradeCancel, true, false},
		{"peer rejects our request", signaling.VideoStateUpgradeReject, false, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			eng, call := testEngineWithOutgoingCall()
			m := eng.calls[call.ID()]
			m.localVideo, m.videoGate, m.peerVideoUpgrade = true, true, true
			m.videoTx = &videoSender{active: true, sendGated: true}
			eng.onVideoStanza(videoStateNode(tc.state), eng.lookup("CID"), &waBinary.Node{Attrs: waBinary.Attrs{"from": peerJID()}}, 0)
			if m.localVideo != tc.wantLocal || m.videoGate != tc.wantLocal || m.peerVideoUpgrade != tc.wantPeer {
				t.Fatalf("local=%v gate=%v peer=%v", m.localVideo, m.videoGate, m.peerVideoUpgrade)
			}
			if active, gated := senderVideoState(m.videoTx); active != tc.wantLocal || gated != tc.wantLocal {
				t.Fatalf("sender=%v gate=%v", active, gated)
			}
			if !call.IsReceivingVideo() {
				t.Fatal("upgrade decision stopped existing remote video")
			}
		})
	}
}

func TestLocalVideoCancelAndRejectPreserveOtherDirection(t *testing.T) {
	eng, call := testEngineWithOutgoingCall()
	m := eng.calls[call.ID()]
	m.localVideo, m.videoGate, m.peerVideoUpgrade = true, true, true
	m.videoTx = &videoSender{active: true, sendGated: true}
	var states []int
	eng.sendCallNode = func(_ context.Context, node waBinary.Node) error {
		states = append(states, node.GetChildren()[0].AttrGetter().Int("state"))
		return nil
	}
	if err := call.SetVideoEnabled(false); err != nil {
		t.Fatal(err)
	}
	if m.videoGate || m.localVideo || !m.peerVideoUpgrade || !call.IsReceivingVideo() {
		t.Fatal("local cancellation affected peer")
	}
	if err := call.RejectVideo(); err != nil {
		t.Fatal(err)
	}
	if err := call.RejectVideo(); !errors.Is(err, ErrNoPendingVideoUpgrade) {
		t.Fatalf("duplicate reject: %v", err)
	}
	if len(states) != 2 || states[0] != signaling.VideoStateUpgradeCancel || states[1] != signaling.VideoStateUpgradeReject {
		t.Fatalf("states %v", states)
	}
	if m.peerVideoUpgrade || !call.IsReceivingVideo() {
		t.Fatal("reject changed existing peer media")
	}
}

func TestManualVideoDecisionGenerationAndAmbiguousSend(t *testing.T) {
	eng, call := testEngineWithOutgoingCall()
	m := eng.calls[call.ID()]
	m.localVideo = true
	outer := &waBinary.Node{Attrs: waBinary.Attrs{"from": peerJID()}}
	states := []int{}
	fail := false
	eng.sendCallNode = func(_ context.Context, node waBinary.Node) error {
		states = append(states, node.GetChildren()[0].AttrGetter().Int("state"))
		if fail {
			return errors.New("uncertain")
		}
		return nil
	}
	eng.onVideoStanza(videoStateNode(signaling.VideoStateUpgradeRequestV2), m, outer, 0)
	first, err := call.VideoNegotiationState(true)
	if err != nil || !first.Manual || first.IncomingGeneration == 0 {
		t.Fatal(first, err)
	}
	if err = call.AutoAcceptVideo(context.Background(), first.IncomingGeneration); !errors.Is(err, ErrVideoDecisionMode) {
		t.Fatal(err)
	}
	eng.onVideoStanza(videoStateNode(signaling.VideoStateUpgradeCancel), m, outer, 0)
	eng.onVideoStanza(videoStateNode(signaling.VideoStateUpgradeRequestV2), m, outer, 0)
	second, err := call.VideoNegotiationState(false)
	if err != nil || second.IncomingGeneration <= first.IncomingGeneration {
		t.Fatal(second, err)
	}
	if err = call.DecideVideoUpgrade(context.Background(), first.IncomingGeneration, true); !errors.Is(err, ErrNoPendingVideoUpgrade) {
		t.Fatal(err)
	}
	if len(states) != 0 {
		t.Fatal(states)
	}
	fail = true
	if err = call.DecideVideoUpgrade(context.Background(), second.IncomingGeneration, true); err == nil {
		t.Fatal("ambiguous send reported success")
	}
	if err = call.DecideVideoUpgrade(context.Background(), second.IncomingGeneration, true); !errors.Is(err, ErrNoPendingVideoUpgrade) {
		t.Fatal(err)
	}
	if len(states) != 1 || states[0] != signaling.VideoStateUpgradeAccept || !call.IsSendingVideo() {
		t.Fatal(states)
	}
}

func TestPendingVideoCannotAnnounceEnabledOrientation(t *testing.T) {
	eng, call := testEngineWithOutgoingCall()
	m := eng.calls[call.ID()]
	m.localVideo = true
	m.videoGate = true
	eng.sendCallNode = func(_ context.Context, _ waBinary.Node) error {
		t.Fatal("pending camera announced as active")
		return nil
	}
	if call.IsVideoTransmitReady() || !errors.Is(call.SetVideoOrientation(1), ErrVideoFrameDropped) {
		t.Fatal("pending camera was not gated")
	}
	m.videoGate = false
	if !call.IsVideoTransmitReady() {
		t.Fatal("ready camera was gated")
	}
}
