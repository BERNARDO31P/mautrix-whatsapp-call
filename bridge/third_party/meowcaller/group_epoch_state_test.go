package meowcaller

import (
	"bytes"
	"errors"
	"testing"
)

func TestGroupEpochStateRosterBoundary(t *testing.T) {
	var state groupEpochState
	key := func(n byte) []byte { return bytes.Repeat([]byte{n}, 32) }
	noInstall := func(uint32, []byte) error { t.Fatal("future epoch installed"); return nil }
	for _, tx := range []uint32{30, 10, 20} {
		if err := state.acceptGroupEpoch(false, 0, tx, key(byte(tx)), "sender-A", noInstall); err != nil {
			t.Fatal(err)
		}
	}
	if state.hasGroupEpoch || len(state.groupRawEpoch) != 0 {
		t.Fatal("staged epoch marked active")
	}
	if err := state.advanceGroupEpoch(15, func(uint32, string) bool { return true }, func(tx uint32, raw []byte) error {
		if tx != 10 || !bytes.Equal(raw, key(10)) {
			t.Fatalf("selected %d", tx)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if !state.hasGroupEpoch || state.groupEpochTxID != 10 || len(state.pendingGroupEpochs) != 2 {
		t.Fatal("incorrect first commit")
	}
	fail := errors.New("roster refused")
	if err := state.advanceGroupEpoch(25, func(uint32, string) bool { return true }, func(tx uint32, raw []byte) error { return fail }); !errors.Is(err, fail) {
		t.Fatal(err)
	}
	if state.groupEpochTxID != 10 || len(state.pendingGroupEpochs) != 2 {
		t.Fatal("failed roster committed")
	}
	if err := state.advanceGroupEpoch(30, func(uint32, string) bool { return true }, func(tx uint32, raw []byte) error {
		if tx != 30 || !bytes.Equal(raw, key(30)) {
			t.Fatalf("selected %d", tx)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if state.groupEpochTxID != 30 || len(state.pendingGroupEpochs) != 0 || !bytes.Equal(state.groupRawEpoch, key(30)) {
		t.Fatal("incorrect final commit")
	}
}

func TestGroupEpochStateDirectInstallAndConflict(t *testing.T) {
	var state groupEpochState
	key := bytes.Repeat([]byte{1}, 32)
	other := bytes.Repeat([]byte{2}, 32)
	fail := errors.New("install refused")
	if err := state.acceptGroupEpoch(true, 20, 10, key, "sender-A", func(uint32, []byte) error { return fail }); !errors.Is(err, fail) {
		t.Fatal(err)
	}
	if state.hasGroupEpoch {
		t.Fatal("failed install committed")
	}
	var calls int
	install := func(uint32, []byte) error { calls++; return nil }
	if err := state.acceptGroupEpoch(true, 20, 10, key, "sender-A", install); err != nil {
		t.Fatal(err)
	}
	if err := state.acceptGroupEpoch(true, 20, 10, key, "sender-A", install); err != nil {
		t.Fatal(err)
	}
	if err := state.acceptGroupEpoch(true, 20, 9, other, "sender-A", install); err != nil {
		t.Fatal(err)
	}
	if err := state.acceptGroupEpoch(true, 20, 10, other, "sender-A", install); err == nil {
		t.Fatal("active conflict accepted")
	}
	if err := state.acceptGroupEpoch(true, 20, 30, key, "sender-A", install); err != nil {
		t.Fatal(err)
	}
	if err := state.acceptGroupEpoch(true, 20, 30, other, "sender-A", install); err == nil {
		t.Fatal("pending conflict accepted")
	}
	if calls != 1 || state.groupEpochTxID != 10 {
		t.Fatal("unexpected activation")
	}
}

func TestGroupEpochStateLimitAndRetirement(t *testing.T) {
	var state groupEpochState
	key := bytes.Repeat([]byte{3}, 32)
	install := func(uint32, []byte) error { return nil }
	if err := state.acceptGroupEpoch(true, 1, 1, key, "sender-A", install); err != nil {
		t.Fatal(err)
	}
	for tx := uint32(2); tx <= maxPendingGroupEpochs+1; tx++ {
		if err := state.acceptGroupEpoch(true, 1, tx, key, "sender-A", install); err != nil {
			t.Fatal(err)
		}
	}
	if err := state.acceptGroupEpoch(true, 1, 2, key, "sender-A", install); err != nil {
		t.Fatal("duplicate at limit", err)
	}
	if err := state.acceptGroupEpoch(true, 1, maxPendingGroupEpochs+2, key, "sender-A", install); err == nil {
		t.Fatal("overflow accepted")
	}
	active, pending := state.groupRawEpoch, state.pendingGroupEpochs[2]
	state.clearGroupEpoch()
	if state.hasGroupEpoch || state.pendingGroupEpochs != nil || state.groupRawEpoch != nil {
		t.Fatal("retirement retained state")
	}
	if !bytes.Equal(active, make([]byte, 32)) || !bytes.Equal(pending, make([]byte, 32)) {
		t.Fatal("retirement retained key bytes")
	}
	if key[0] != 3 {
		t.Fatal("caller key was modified")
	}
}

func TestGroupEpochDistributorBindingAndRosterAdmission(t *testing.T) {
	var state groupEpochState
	key := bytes.Repeat([]byte{4}, 32)
	never := func(uint32, []byte) error { t.Fatal("future key installed"); return nil }
	if err := state.acceptGroupEpoch(true, 10, 12, key, "device-A", never); err != nil {
		t.Fatal(err)
	}
	if err := state.acceptGroupEpoch(true, 10, 12, key, "device-B", never); err == nil {
		t.Fatal("second distributor accepted")
	}
	if err := state.acceptGroupEpoch(true, 10, 13, key, "device-B", never); err != nil {
		t.Fatal(err)
	}
	retired := state.pendingGroupEpochs[13]
	allowA := func(_ uint32, sender string) bool { return sender == "device-A" }
	if err := state.advanceGroupEpoch(13, allowA, func(tx uint32, raw []byte) error {
		if tx != 12 || !bytes.Equal(raw, key) {
			t.Fatalf("activated wrong epoch: %d", tx)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if state.groupEpochDistributor != "device-A" || len(state.pendingGroupEpochs) != 0 || len(state.pendingGroupDistributors) != 0 || !bytes.Equal(retired, make([]byte, 32)) {
		t.Fatal("distributor ownership or rejected key cleanup failed")
	}
	if err := state.acceptGroupEpoch(true, 13, 12, key, "device-B", never); err == nil {
		t.Fatal("active distributor replaced")
	}
}
