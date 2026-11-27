package meowcaller

import (
	"context"
	"crypto/sha256"
	"errors"
	"testing"

	"github.com/purpshell/meowcaller/signaling"
	waBinary "go.mau.fi/whatsmeow/binary"
	"go.mau.fi/whatsmeow/types"
)

func TestWaitingRoomControlAdminGenerationAndExactOwner(t *testing.T) {
	eng, call, _ := testGroupEngine("LINK")
	owner := eng.calls[call.id]
	owner.linkCall = true
	owner.linkAdmitted = true
	call.setPhase(CallPhaseActive)
	user := types.NewJID("222", types.HiddenUserServer)
	state := WaitingRoomState{Enabled: true, IsAdmin: true, TransactionID: 1, Users: []WaitingRoomUser{{JID: user, State: "waiting_room_joined"}}}
	call.setWaitingRoomState(state)
	_, gen, _ := call.WaitingRoomSnapshot()
	sent := 0
	eng.sendCallNode = func(_ context.Context, node waBinary.Node) error {
		sent++
		if node.GetChildren()[0].Tag != "waiting_room_admit" {
			t.Fatal(node.Tag)
		}
		return nil
	}
	if err := call.SubmitWaitingRoomControl(context.Background(), gen, "admit", user.String(), nil); err != nil {
		t.Fatal(err)
	}
	if err := call.SubmitWaitingRoomControl(context.Background(), gen, "admit", user.String(), nil); !errors.Is(err, ErrWaitingRoomStale) {
		t.Fatal(err)
	}
	state.TransactionID++
	state.IsAdmin = false
	call.setWaitingRoomState(state)
	_, gen, _ = call.WaitingRoomSnapshot()
	if err := call.SubmitWaitingRoomControl(context.Background(), gen, "deny", user.String(), nil); !errors.Is(err, ErrWaitingRoomStale) {
		t.Fatal(err)
	}
	state.TransactionID++
	state.IsAdmin = true
	call.setWaitingRoomState(state)
	_, gen, _ = call.WaitingRoomSnapshot()
	owner.linkAdmitted = false
	if err := call.SubmitWaitingRoomControl(context.Background(), gen, "deny", user.String(), nil); !errors.Is(err, ErrWaitingRoomStale) {
		t.Fatal(err)
	}
	owner.linkAdmitted = true
	owner.call = &Call{}
	if err := call.SubmitWaitingRoomControl(context.Background(), gen, "deny", user.String(), nil); !errors.Is(err, ErrWaitingRoomStale) {
		t.Fatal(err)
	}
	if sent != 1 {
		t.Fatal(sent)
	}
}

func TestWaitingRoomUpdatePreservesAdmissionAndChecksLink(t *testing.T) {
	eng, call, creator := testGroupEngine("LINK")
	owner := eng.calls[call.id]
	owner.linkCall = true
	owner.linkTokenHash = sha256.Sum256([]byte("expected"))
	call.setWaitingRoomState(WaitingRoomState{InWaitingRoom: true, TransactionID: 1})
	call.setWaitingRoomAdmission()
	update := signaling.WaitingRoom{CallID: call.id, CallCreator: creator, LinkToken: "wrong", Media: signaling.CallLinkMediaAudio, TransactionID: 2, IsAdmin: true}
	eng.applyWaitingRoomUpdate(update)
	state, _ := call.WaitingRoomState()
	if state.TransactionID != 1 {
		t.Fatal(state)
	}
	update.LinkToken = "expected"
	eng.applyWaitingRoomUpdate(update)
	state, _ = call.WaitingRoomState()
	if state.InWaitingRoom || state.TransactionID != 2 || !state.IsAdmin {
		t.Fatal(state)
	}
}
