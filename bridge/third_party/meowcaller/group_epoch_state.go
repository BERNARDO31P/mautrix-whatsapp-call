package meowcaller

import (
	"bytes"
	"fmt"
)

// groupEpochState is protected by the engine mutex. A future epoch is not
// usable media material until its roster transaction has been committed.
type groupEpochState struct {
	groupRawEpoch            []byte
	groupEpochTxID           uint32
	hasGroupEpoch            bool
	pendingGroupEpochs       map[uint32][]byte
	groupEpochDistributor    string
	pendingGroupDistributors map[uint32]string
}

const maxPendingGroupEpochs = 32

func (s *groupEpochState) acceptGroupEpoch(hasRoster bool, rosterTx, tx uint32, key []byte, distributor string, install func(uint32, []byte) error) error {
	if tx == 0 || len(key) != 32 || distributor == "" {
		return fmt.Errorf("meowcaller: invalid group key epoch")
	}
	if s.hasGroupEpoch && tx <= s.groupEpochTxID {
		if tx == s.groupEpochTxID && (!bytes.Equal(s.groupRawEpoch, key) || s.groupEpochDistributor != distributor) {
			return fmt.Errorf("meowcaller: conflicting group epoch for transaction %d", tx)
		}
		return nil
	}
	if previous, ok := s.pendingGroupEpochs[tx]; ok && (!bytes.Equal(previous, key) || s.pendingGroupDistributors[tx] != distributor) {
		return fmt.Errorf("meowcaller: conflicting pending group epoch for transaction %d", tx)
	}
	if !hasRoster || tx > rosterTx {
		if _, ok := s.pendingGroupEpochs[tx]; ok {
			return nil
		}
		if len(s.pendingGroupEpochs) >= maxPendingGroupEpochs {
			return fmt.Errorf("meowcaller: too many pending group epochs")
		}
		if s.pendingGroupEpochs == nil {
			s.pendingGroupEpochs = make(map[uint32][]byte)
			s.pendingGroupDistributors = make(map[uint32]string)
		}
		s.pendingGroupEpochs[tx] = bytes.Clone(key)
		s.pendingGroupDistributors[tx] = distributor
		return nil
	}
	if err := install(tx, key); err != nil {
		return err
	}
	s.commitGroupEpoch(tx, key, distributor)
	return nil
}

// advanceGroupEpoch performs the roster change and the newest eligible key
// activation together. The callback must not reenter the engine mutex.
func (s *groupEpochState) advanceGroupEpoch(rosterTx uint32, authorized func(uint32, string) bool, apply func(uint32, []byte) error) error {
	var selected uint32
	for tx := range s.pendingGroupEpochs {
		if tx <= rosterTx && tx > selected && (!s.hasGroupEpoch || tx > s.groupEpochTxID) && authorized(tx, s.pendingGroupDistributors[tx]) {
			selected = tx
		}
	}
	key := s.pendingGroupEpochs[selected]
	if err := apply(selected, key); err != nil {
		return err
	}
	if selected != 0 {
		s.commitGroupEpoch(selected, key, s.pendingGroupDistributors[selected])
	}
	// Retire rejected eligible keys only after the roster was applied.
	for tx, raw := range s.pendingGroupEpochs {
		if tx <= rosterTx && !authorized(tx, s.pendingGroupDistributors[tx]) {
			clear(raw)
			delete(s.pendingGroupEpochs, tx)
			delete(s.pendingGroupDistributors, tx)
		}
	}
	return nil
}

func (s *groupEpochState) commitGroupEpoch(tx uint32, key []byte, distributor string) {
	// The incoming key can alias one of our pending entries.
	next := bytes.Clone(key)
	clear(s.groupRawEpoch)
	s.groupRawEpoch = next
	s.groupEpochTxID = tx
	s.groupEpochDistributor = distributor
	s.hasGroupEpoch = true
	for pending, raw := range s.pendingGroupEpochs {
		if pending <= tx {
			clear(raw)
			delete(s.pendingGroupEpochs, pending)
			delete(s.pendingGroupDistributors, pending)
		}
	}
}

func (s *groupEpochState) clearGroupEpoch() {
	clear(s.groupRawEpoch)
	for _, raw := range s.pendingGroupEpochs {
		clear(raw)
	}
	*s = groupEpochState{}
}
