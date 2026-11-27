package meowcaller

import (
	"context"
	"errors"
	"testing"
	"time"

	waBinary "go.mau.fi/whatsmeow/binary"
)

func TestCancelledUnsentPickupCanRetry(t *testing.T) {
	eng, call := testEngineWithOutgoingCall()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := call.AnswerContext(ctx); !errors.Is(err, context.Canceled) || !errors.Is(err, ErrPickupNotSubmitted) {
		t.Fatalf("pickup error: %v", err)
	}
	if eng.calls[call.ID()].acceptPending {
		t.Fatal("unsent pickup reservation leaked")
	}
	sendErr := errors.New("ambiguous send")
	sends := 0
	eng.sendCallNode = func(context.Context, waBinary.Node) error { sends++; return sendErr }
	if err := call.AnswerContext(context.Background()); !errors.Is(err, sendErr) || !errors.Is(err, ErrPickupOutcomeUnknown) {
		t.Fatalf("retry: %v", err)
	}
	if err := call.AnswerContext(context.Background()); err == nil || sends != 1 {
		t.Fatal("ambiguous submission was retried")
	}
}

func TestUnsentPickupReleaseDoesNotAlterReplacement(t *testing.T) {
	eng, call := testEngineWithOutgoingCall()
	old := eng.calls[call.ID()]
	replacement := &Call{eng: eng, id: call.ID()}
	eng.calls[call.ID()] = &engineCall{call: replacement, acceptPending: true}
	eng.releaseUnsentPickup(call, old)
	if !eng.calls[call.ID()].acceptPending {
		t.Fatal("stale release altered new reservation")
	}
	if err := call.AnswerContext(context.Background()); err == nil {
		t.Fatal("stale handle could answer replacement")
	}
}

// A successful return from a provider that ignored cancellation must not
// revive a terminated call or start media for a replacement registration.
func TestPickupTeardownCancelsProviderAndRejectsLateSuccess(t *testing.T) {
	eng, call := testEngineWithOutgoingCall()
	m := eng.calls[call.ID()]
	m.seenMute = true
	entered := make(chan struct{})
	cancelled := make(chan struct{})
	release := make(chan struct{}, 1)
	defer close(release)
	eng.sendCallNode = func(ctx context.Context, _ waBinary.Node) error {
		close(entered)
		<-ctx.Done()
		close(cancelled)
		<-release
		return nil
	}
	result := make(chan error, 1)
	go func() { result <- call.AnswerContext(context.Background()) }()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("pickup not submitted")
	}
	eng.finishCallIfCurrent(call, "remote hangup")
	select {
	case <-cancelled:
	case <-time.After(time.Second):
		t.Fatal("provider not cancelled")
	}
	if call.State() != CallPhaseEnded {
		t.Fatal("call not ended")
	}
	// A buffered send can report success even though teardown already won.
	// Release the provider without closing the test cleanup channel twice.
	release <- struct{}{}
	select {
	case err := <-result:
		if err == nil {
			t.Fatal("late success reported pickup success")
		}
	case <-time.After(time.Second):
		t.Fatal("pickup did not finish")
	}
	if call.State() != CallPhaseEnded {
		t.Fatal("late pickup revived call")
	}
}

// A local decline racing a submitted accept must retire the call immediately,
// but cannot send a reject/terminate ahead of that in-flight accept.
func TestTerminalSignalFollowsSubmittedPickup(t *testing.T) {
	for _, reject := range []bool{false, true} {
		t.Run(map[bool]string{false: "hangup", true: "reject"}[reject], func(t *testing.T) {
			eng, call := testEngineWithOutgoingCall()
			eng.calls[call.ID()].seenMute = true
			entered, cancelled := make(chan struct{}), make(chan struct{})
			terminal := make(chan string, 1)
			providerReturned := make(chan struct{})
			defer func() {
				select {
				case <-providerReturned:
				default:
					close(providerReturned)
				}
			}()
			eng.sendCallNode = func(ctx context.Context, node waBinary.Node) error {
				children := node.GetChildren()
				if len(children) == 0 {
					return errors.New("empty call node")
				}
				kind := children[0].Tag
				if kind == "accept" {
					close(entered)
					<-ctx.Done()
					close(cancelled)
					<-providerReturned
					return errors.New("ambiguous accept fixture")
				}
				terminal <- kind
				return nil
			}
			result := make(chan error, 1)
			go func() { result <- call.AnswerContext(context.Background()) }()
			select {
			case <-entered:
			case <-time.After(time.Second):
				t.Fatal("no accept submission")
			}
			ended := make(chan error, 1)
			go func() {
				if reject {
					ended <- call.Reject()
				} else {
					ended <- call.Hangup()
				}
			}()
			select {
			case <-cancelled:
			case <-time.After(time.Second):
				t.Fatal("pickup not cancelled")
			}
			select {
			case kind := <-terminal:
				t.Fatalf("premature %s", kind)
			case <-time.After(time.Millisecond):
			}
			close(providerReturned)
			select {
			case err := <-result:
				if !errors.Is(err, ErrPickupOutcomeUnknown) {
					t.Fatalf("pickup: %v", err)
				}
			case <-time.After(time.Second):
				t.Fatal("pickup stuck")
			}
			select {
			case err := <-ended:
				if err != nil {
					t.Fatal(err)
				}
			case <-time.After(time.Second):
				t.Fatal("terminal stuck")
			}
			select {
			case kind := <-terminal:
				if kind != "terminate" {
					t.Fatalf("terminal signal %s", kind)
				}
			default:
				t.Fatal("missing terminate")
			}
			if call.State() != CallPhaseEnded || eng.lookup(call.ID()) != nil {
				t.Fatal("call resurrected")
			}
		})
	}
}

func TestAuthorizedPickupDoesNotWaitForRemoteMute(t *testing.T) {
	eng, call := testEngineWithOutgoingCall()
	m := eng.calls[call.ID()]
	m.direction = CallDirectionIncoming
	call.phase = CallPhaseRinging
	sends := 0
	eng.sendCallNode = func(_ context.Context, node waBinary.Node) error {
		sends++
		if children := node.GetChildren(); len(children) != 1 || children[0].Tag != "accept" {
			t.Fatalf("expected accept, got %v", node.Tag)
		}
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := call.AnswerContext(ctx); err != nil || sends != 1 || call.State() != CallPhaseConnecting || m.seenMute {
		t.Fatalf("pickup without mute: err=%v sends=%d state=%v", err, sends, call.State())
	}
}

func TestDirectRelaySetupCandidatesRemainBoundedAndPreserveFNA(t *testing.T) {
	endpoint := func(name string, token uint32, fna bool) relayEndpoint {
		return relayEndpoint{relayName: name, tokenID: token, isFNA: fna,
			addresses: []relayAddress{{ipv4: "192.0.2.1", port: 3478}}}
	}
	rd := &relayData{relayKeyASCII: []byte("key"), relayTokens: [][]byte{{1}, {2}, {3}, {4}, {5}}, endpoints: []relayEndpoint{
		endpoint("first", 1, false), endpoint("second", 0, false), endpoint("third", 2, false), endpoint("fourth", 3, false), endpoint("fna", 4, true),
	}}
	out := mediaRelayCandidates(rd, false, false)
	if len(out) != 3 || out[0].tokenID != 1 || out[1].tokenID != 0 || out[2].tokenID != 2 {
		t.Fatal("outgoing relay candidates were not bounded or reordered incorrectly")
	}
	in := mediaRelayCandidates(rd, true, false)
	if len(in) != 1 || !in[0].isFNA {
		t.Fatal("inbound explicit FNA selection was lost")
	}
	group := mediaRelayCandidates(rd, false, true)
	if len(group) != 1 || group[0] != out[0] {
		t.Fatal("group relay selection was changed")
	}
	// Invalid preferred and subsequent candidates do not exhaust retry budget.
	rd.endpoints[0].tokenID = 99
	rd.endpoints[1].addresses[0].port = 0
	out = mediaRelayCandidates(rd, false, false)
	if len(out) != 2 || out[0].tokenID != 2 || out[1].tokenID != 3 {
		t.Fatal("unusable endpoints consumed direct relay attempts")
	}
	rd.endpoints[4].tokenID = 99
	if len(mediaRelayCandidates(rd, true, false)) != 0 {
		t.Fatal("invalid FNA allocation fell back to unrelated non-FNA relay")
	}
	if len(mediaRelayCandidates(nil, false, false)) != 0 {
		t.Fatal("nil relay produced candidates")
	}
}

func TestDirectRelayCandidatesSkipDuplicateAllocations(t *testing.T) {
	endpoint := func(name, ip string, port uint16, token uint32, fna bool) relayEndpoint {
		return relayEndpoint{relayName: name, tokenID: token, isFNA: fna,
			addresses: []relayAddress{{ipv4: ip, port: port}}}
	}
	rd := &relayData{relayKeyASCII: []byte("key"), relayTokens: [][]byte{{1}, {2}}, endpoints: []relayEndpoint{
		endpoint("first", "192.0.2.1", 3478, 0, false),
		endpoint("duplicate", "192.0.2.1", 3478, 0, false),
		endpoint("duplicate again", "192.0.2.1", 3478, 0, false),
		endpoint("different token", "192.0.2.1", 3478, 1, false),
		endpoint("different port", "192.0.2.1", 3480, 0, false),
		endpoint("fna", "192.0.2.2", 3478, 0, true),
		endpoint("fna duplicate", "192.0.2.2", 3478, 0, true),
		endpoint("fna alternate", "192.0.2.3", 3478, 0, true),
	}}
	out := mediaRelayCandidates(rd, false, false)
	if len(out) != 3 || out[0] != &rd.endpoints[0] || out[1] != &rd.endpoints[3] || out[2] != &rd.endpoints[4] {
		t.Fatalf("duplicate allocations displaced distinct direct candidates: %v", out)
	}
	in := mediaRelayCandidates(rd, true, false)
	if len(in) != 2 || in[0] != &rd.endpoints[5] || in[1] != &rd.endpoints[7] {
		t.Fatalf("duplicate or non-FNA candidates in incoming call: %v", in)
	}
	group := mediaRelayCandidates(rd, true, true)
	if len(group) != 1 || group[0] != &rd.endpoints[5] {
		t.Fatal("group election changed")
	}
}
