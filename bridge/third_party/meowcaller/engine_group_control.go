package meowcaller

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"errors"
	"fmt"

	"github.com/purpshell/meowcaller/signaling"
	waBinary "go.mau.fi/whatsmeow/binary"
	"go.mau.fi/whatsmeow/proto/waE2E"
	"go.mau.fi/whatsmeow/types"
	"google.golang.org/protobuf/proto"
)

func (e *engine) onUnknownCallEvent(node *waBinary.Node) {
	// Source of truth: https://github.com/purpshell/meowcaller/blob/48c2391ce9f7dcc2b3f223f72f1b5f0c627ad943/datasheets/voip-group-update-ingest.md#L105-L148
	envelope, err := signaling.ParseCallControlEnvelope(node)
	if err != nil {
		return
	}
	switch envelope.Action.Tag {
	case "group_update":
		parsed, parseErr := signaling.ParseGroupUpdate(&envelope.Action)
		if parseErr != nil {
			e.c.log.Warn().Err(parseErr).Str("call_id", envelope.CallID).Msg("parse group update failed")
			return
		}
		update := groupCallUpdateFromSignaling(*parsed)
		if owner := e.applyGroupUpdateOwner(update, "", false); owner != nil && update.RekeyRequested {
			if fanoutErr := e.distributeRequestedGroupEpoch(context.Background(), update, owner); fanoutErr != nil {
				e.c.log.Warn().Err(fanoutErr).Str("call_id", update.CallID).Msg("group rekey fanout failed")
			}
		}
	case "waiting_room_update":
		room, parseErr := signaling.ParseWaitingRoomUpdate(&envelope.Action)
		if parseErr != nil {
			e.c.log.Warn().Err(parseErr).Str("call_id", envelope.CallID).Msg("parse waiting room update failed")
			return
		}
		e.applyWaitingRoomUpdate(*room)
	case "user_action":
		raised, parseErr := signaling.ParseRaiseHand(&envelope.Action)
		if parseErr != nil {
			e.c.log.Warn().Err(parseErr).Str("call_id", envelope.CallID).Msg("parse hand state failed")
			return
		}
		e.dispatchRemoteHandState(envelope, raised)
	case "screen_share":
		screenShare, parseErr := signaling.ParseScreenShare(&envelope.Action)
		if parseErr != nil {
			e.c.log.Warn().Err(parseErr).Str("call_id", envelope.CallID).Msg("parse screen share failed")
			return
		}
		e.dispatchRemoteScreenShare(envelope, *screenShare)
	case "enc_rekey":
		e.c.log.Debug().
			Str("call_id", envelope.CallID).
			Msg("group rekey received")
		if err = e.ingestGroupEpoch(context.Background(), envelope); err != nil {
			e.c.log.Warn().Err(err).Str("call_id", envelope.CallID).Msg("group rekey ingest failed")
			return
		}
		e.c.log.Debug().
			Str("call_id", envelope.CallID).
			Msg("group rekey processed")
	}
}

func (e *engine) applyWaitingRoomUpdate(room signaling.WaitingRoom) {
	// Source of truth: https://github.com/tulir/whatsmeow/blob/3775fbadf88fdf44ada62ae5c5db5d7cc6f26259/call_link.go#L388-L441
	state := WaitingRoomState{
		Enabled: room.Enabled, IsAdmin: room.IsAdmin,
		TransactionID: room.TransactionID,
		Users:         make([]WaitingRoomUser, len(room.Users)),
	}
	for i, user := range room.Users {
		state.Users[i] = WaitingRoomUser{JID: user.JID, PN: user.PN, State: user.State}
	}
	e.mu.Lock()
	m := e.calls[room.CallID]
	if m != nil && m.call != nil {
		if m.linkCall && (m.creator.IsEmpty() || m.creator.ToNonAD() != room.CallCreator.ToNonAD() ||
			m.linkTokenHash != sha256.Sum256([]byte(room.LinkToken)) ||
			(room.Media == signaling.CallLinkMediaVideo) != m.linkVideo) {
			e.mu.Unlock()
			return
		}
	}
	var call *Call
	if m != nil {
		call = m.call
	}
	e.mu.Unlock()
	if call != nil {
		call.storeWaitingRoomState(state, true)
	}
}

func (e *engine) dispatchRemoteHandState(envelope *signaling.CallControlEnvelope, raised bool) {
	// Source of truth: https://github.com/purpshell/meowcaller/blob/36d54857c74e45ccb08f6444a32d2afa13f20be9/datasheets/group-video-reactions.md#L31-L43
	if envelope == nil {
		return
	}
	participant := callControlParticipant(envelope)
	e.mu.Lock()
	m := e.calls[envelope.CallID]
	var call *Call
	// Only the matching call creator and either its call-scoped envelope
	// or the participant's own address may attribute a hand update.
	if m != nil && m.group && !m.creator.IsEmpty() &&
		m.creator.ToNonAD() == envelope.CallCreator.ToNonAD() &&
		(envelope.From == types.NewJID(envelope.CallID, "call") || envelope.From.ToNonAD() == participant) {
		call = m.call
	}
	e.mu.Unlock()
	if call != nil {
		call.dispatchHandRaise(HandRaiseState{Participant: participant, Raised: raised})
	}
}

func (e *engine) dispatchRemoteScreenShare(
	envelope *signaling.CallControlEnvelope,
	screenShare signaling.ScreenShare,
) {
	// Source of truth: https://github.com/purpshell/meowcaller/blob/36d54857c74e45ccb08f6444a32d2afa13f20be9/datasheets/group-video-reactions.md#L44-L56
	if envelope == nil || screenShare.Version != 2 {
		return
	}
	participant := callControlParticipant(envelope)
	e.mu.Lock()
	m := e.calls[envelope.CallID]
	var call *Call
	if m != nil && m.group && !m.creator.IsEmpty() &&
		m.creator.ToNonAD() == envelope.CallCreator.ToNonAD() &&
		(envelope.From == types.NewJID(envelope.CallID, "call") || envelope.From.ToNonAD() == participant) {
		call = m.call
	}
	e.mu.Unlock()
	if call != nil {
		device := types.EmptyJID
		// Preserve an explicit device on the participant or direct sender.
		// A relay carrying only a bare participant does not identify a device.
		if envelope.Participant.Device != 0 {
			device = envelope.Participant
		}
		if envelope.From.Device != 0 {
			if !device.IsEmpty() && device != envelope.From {
				return
			}
			device = envelope.From
		}
		call.dispatchScreenShare(ScreenShareState{
			Participant:      participant,
			Device:           device,
			Active:           screenShare.State == signaling.ScreenShareStarted,
			Version:          screenShare.Version,
			ScreenShareID:    screenShare.ScreenShareID,
			HasScreenShareID: screenShare.HasScreenShareID,
		})
	}
}

func callControlParticipant(envelope *signaling.CallControlEnvelope) types.JID {
	// Source of truth: https://github.com/purpshell/meowcaller/blob/36d54857c74e45ccb08f6444a32d2afa13f20be9/datasheets/group-video-reactions.md#L31-L56
	if envelope == nil {
		return types.EmptyJID
	}
	if !envelope.Participant.IsEmpty() {
		return envelope.Participant.ToNonAD()
	}
	return envelope.From.ToNonAD()
}

func (e *engine) applyGroupUpdate(update groupCallUpdate) bool {
	return e.applyGroupUpdateMatching(update, "", false)
}

func (e *engine) applyGroupUpdateMatching(update groupCallUpdate, offerID string, offerAck bool) bool {
	return e.applyGroupUpdateOwner(update, offerID, offerAck) != nil
}

func (e *engine) applyGroupUpdateOwner(update groupCallUpdate, offerID string, offerAck bool) *engineCall {
	// Source of truth: https://github.com/purpshell/meowcaller/blob/65b1dbf33f365db7392e438c3e3bf3651decb6cf/datasheets/group-media-receive.md#L83-L100
	if update.CallID == "" || (offerAck && offerID == "") {
		return nil
	}
	e.mu.Lock()
	m := e.calls[update.CallID]
	if m == nil {
		e.mu.Unlock()
		e.c.log.Debug().
			Str("call_id", update.CallID).
			Uint32("transaction_id", update.TransactionID).
			Msg("dropping group update for unknown or ended call")
		return nil
	}
	if offerAck && !matchesGroupOffer(m, offerID) {
		e.mu.Unlock()
		return nil
	}
	if m.groupUpdate != nil && update.TransactionID <= m.groupUpdate.TransactionID {
		if offerAck {
			m.offerNodeID = ""
		}
		e.mu.Unlock()
		return nil
	}
	if !m.group && e.c != nil && e.c.fixedRoom {
		call := m.call
		e.mu.Unlock()
		e.finishCallIfCurrent(call, "group_promotion")
		return nil
	}
	if m.linkCall && (!update.GroupJID.IsEmpty() || (!update.CallCreator.IsEmpty() && !sameDirectControlIdentity(m.creator, update.CallCreator))) {
		e.mu.Unlock()
		return nil
	}
	if m.explicitAdHoc && !update.GroupJID.IsEmpty() {
		call := m.call
		e.mu.Unlock()
		e.finishCallIfCurrent(call, "group_changed")
		return nil
	}
	if m.call != nil {
		if previous, ok := m.call.GroupState(); ok && !previous.GroupJID.IsEmpty() {
			if !update.GroupJID.IsEmpty() && update.GroupJID != previous.GroupJID {
				call := m.call
				e.mu.Unlock()
				e.finishCallIfCurrent(call, "group_changed")
				return nil
			}
			update.GroupJID = previous.GroupJID
		}
	}
	admitted := !m.linkCall || linkRosterAdmitsDevice(update, m.inviteSelfDevice.JID)
	if m.linkCall && m.linkAdmitted && !admitted {
		call := m.call
		e.mu.Unlock()
		e.finishCallIfCurrent(call, "link_membership_ended")
		return nil
	}
	stored := cloneGroupCallUpdate(update)
	// Roster-only updates retain the last authoritative relay credentials.
	if m.groupUpdate != nil {
		stored.Relay = selectGroupRelaySnapshot(m.groupUpdate.Relay, stored.Relay)
	}
	receivers := m.groupReceivers
	expected := m
	// Serialize the local roster and key commit with engine ownership and
	// media-registry publication. These registry operations perform no I/O.
	if err := m.advanceGroupEpoch(stored.TransactionID, func(tx uint32, distributor string) bool {
		return (stored.CallCreator.IsEmpty() || sameDirectControlIdentity(m.creator, stored.CallCreator)) && !(stored.RekeyRequested && tx == stored.TransactionID) && groupRosterContainsDistributor(&stored, distributor)
	}, func(tx uint32, key []byte) error {
		if receivers == nil {
			return nil
		}
		if tx != 0 {
			if err := receivers.ApplyGroupRawEpoch(tx, key); err != nil {
				return err
			}
		}
		return receivers.ApplyGroupUpdate(stored)
	}); err != nil {
		e.mu.Unlock()
		e.c.log.Warn().Err(err).Str("call_id", update.CallID).Msg("apply group media roster failed")
		return nil
	}
	if offerAck {
		m.offerNodeID = ""
	}
	m.group = true
	m.groupUpdate = &stored
	if m.linkCall {
		m.linkAdmitted = admitted
	}
	if !update.CallCreator.IsEmpty() {
		m.creator = update.CallCreator
	}
	m.from = types.NewJID(update.CallID, "call")
	call := m.call
	var waitingRoomCancel context.CancelFunc
	if admitted {
		waitingRoomCancel = m.waitingRoomCancel
		m.waitingRoomCancel = nil
	}
	e.mu.Unlock()
	e.c.log.Debug().
		Str("call_id", update.CallID).
		Uint32("transaction_id", update.TransactionID).
		Bool("has_relay", stored.Relay != nil).
		Bool("rekey_requested", update.RekeyRequested).
		Msg("group update installed")

	if waitingRoomCancel != nil {
		waitingRoomCancel()
	}
	if call != nil {
		if admitted && call.State() == CallPhaseWaitingRoom {
			call.setWaitingRoomAdmission()
			call.setPhase(CallPhaseConnecting)
		}
		call.setGroupState(groupCallStateFromUpdate(stored))
	}
	e.maybeStartMediaMatching(update.CallID, expected)
	return expected
}

func matchesGroupOffer(m *engineCall, offerID string) bool {
	return m != nil && m.call != nil && m.group && m.direction == CallDirectionOutgoing &&
		m.call.State() != CallPhaseEnded && offerID != "" && m.offerNodeID == offerID
}

func (e *engine) ingestGroupEpoch(ctx context.Context, envelope *signaling.CallControlEnvelope) error {
	// Source of truth: https://github.com/purpshell/meowcaller/blob/747c6a1b8a0370358ef18bbaa5e029b960c2f836/datasheets/voip-group-enc-rekey-ingest.md#L39-L123
	if envelope == nil {
		return errors.New("meowcaller: group rekey envelope is nil")
	}
	rekey, err := signaling.ParseGroupCallEncRekey(&envelope.Action)
	if err != nil {
		return err
	}
	// Capture the exact call owner before the potentially blocking Signal operation.
	e.mu.Lock()
	expected := e.calls[rekey.CallID]
	allowed := groupRemoteRekeyAllowed(expected, envelope, rekey)
	e.mu.Unlock()
	if !allowed {
		return errors.New("meowcaller: unauthorized group rekey distributor")
	}
	enc, ok := envelope.Action.GetOptionalChildByTag("enc")
	if !ok {
		return errors.New("meowcaller: group rekey has no encrypted payload")
	}
	plaintext, _, err := e.c.wa.DangerousInternals().DecryptDM(
		ctx,
		&enc,
		envelope.From,
		rekey.EncryptionType == "pkmsg",
		envelope.Timestamp,
	)
	if err != nil {
		return fmt.Errorf("meowcaller: decrypt group rekey: %w", err)
	}
	defer clear(plaintext)
	var message waE2E.Message
	if err = proto.Unmarshal(plaintext, &message); err != nil {
		return fmt.Errorf("meowcaller: decode group rekey: %w", err)
	}
	rawKey := message.GetCall().GetCallKey()
	if len(rawKey) != 32 {
		return fmt.Errorf("meowcaller: group rekey key is %d bytes, want 32", len(rawKey))
	}
	defer clear(rawKey)
	return e.installGroupEpoch(rekey.CallID, rekey.TransactionID, rawKey, expected, envelope.From, envelope, rekey)
}

// The outer sender is the distributor, not the creator or a media key owner.
// Only exact connected roster devices can distribute a remote epoch.
func groupRosterContainsDistributor(roster *groupCallUpdate, distributor string) bool {
	if roster == nil || distributor == "" {
		return false
	}
	for _, participant := range roster.Participants {
		if participant.State != "connected" {
			continue
		}
		for _, device := range participant.Devices {
			if device.HasPID && !device.JID.IsEmpty() && device.JID.String() == distributor {
				return true
			}
		}
	}
	return false
}

func groupRemoteRekeyAllowed(m *engineCall, envelope *signaling.CallControlEnvelope, rekey *signaling.GroupCallEncRekey) bool {
	if m == nil || !m.group || m.call == nil || m.call.State() == CallPhaseEnded || envelope == nil || rekey == nil ||
		envelope.CallID != rekey.CallID || m.call.ID() != rekey.CallID || rekey.TransactionID == 0 ||
		!sameDirectControlIdentity(m.creator, rekey.CallCreator) || !sameDirectControlIdentity(rekey.CallCreator, envelope.CallCreator) ||
		envelope.From.IsEmpty() || (!envelope.Participant.IsEmpty() && !sameDirectControlIdentity(envelope.From, envelope.Participant)) {
		return false
	}
	roster := m.groupUpdate
	if roster != nil && rekey.TransactionID <= roster.TransactionID {
		return !(roster.RekeyRequested && rekey.TransactionID == roster.TransactionID) && groupRosterContainsDistributor(roster, envelope.From.String())
	}
	// The distributor may have just joined and its roster may arrive later.
	// A future key is staged, not activated, until that device is in the roster.
	if envelope.From.Server != types.HiddenUserServer && envelope.From.Server != types.DefaultUserServer {
		return false
	}
	if pending, exists := m.pendingGroupDistributors[rekey.TransactionID]; exists {
		return pending == envelope.From.String()
	}
	return len(m.pendingGroupEpochs) < maxPendingGroupEpochs
}

func groupLocalRekeyAllowed(m *engineCall, tx uint32) bool {
	return m != nil && m.group && m.call != nil && m.call.State() != CallPhaseEnded && m.groupUpdate != nil && m.groupUpdate.TransactionID == tx && m.groupUpdate.RekeyRequested
}

func (e *engine) installGroupEpoch(callID string, transactionID uint32, rawKey []byte, expected *engineCall, distributor types.JID, envelope *signaling.CallControlEnvelope, rekey *signaling.GroupCallEncRekey) error {
	// Source of truth: https://github.com/purpshell/meowcaller/blob/d9df3eb9d96ea5260ffcd4036b6669499a1c1bc2/datasheets/voip-group-key-epoch-fanout.md#L85-L143
	if callID == "" || transactionID == 0 || len(rawKey) != 32 {
		return errors.New("meowcaller: invalid group key epoch")
	}
	e.mu.Lock()
	m := e.calls[callID]
	if m == nil || m != expected || distributor.IsEmpty() ||
		(envelope != nil && (rekey == nil || rekey.CallID != callID || rekey.TransactionID != transactionID || envelope.From != distributor || !groupRemoteRekeyAllowed(m, envelope, rekey))) ||
		(envelope == nil && !groupLocalRekeyAllowed(m, transactionID)) {
		e.mu.Unlock()
		return fmt.Errorf("meowcaller: stale or unauthorized group epoch for %s", callID)
	}
	var rosterTx uint32
	if m.groupUpdate != nil {
		rosterTx = m.groupUpdate.TransactionID
	}
	err := m.acceptGroupEpoch(m.groupUpdate != nil, rosterTx, transactionID, rawKey, distributor.String(), func(tx uint32, key []byte) error {
		if m.groupReceivers == nil {
			return nil
		}
		return m.groupReceivers.ApplyGroupRawEpoch(tx, key)
	})
	e.mu.Unlock()
	if err != nil {
		return err
	}
	e.maybeStartMediaMatching(callID, m)
	return nil
}

func (e *engine) distributeRequestedGroupEpoch(ctx context.Context, update groupCallUpdate, expected *engineCall) error {
	// Source of truth: https://github.com/purpshell/meowcaller/blob/d9df3eb9d96ea5260ffcd4036b6669499a1c1bc2/datasheets/voip-group-key-epoch-fanout.md#L99-L162
	self := e.c.wa.Store.GetLID()
	e.mu.Lock()
	allowed := e.calls[update.CallID] == expected && groupLocalRekeyAllowed(expected, update.TransactionID) && !self.IsEmpty()
	e.mu.Unlock()
	if !allowed {
		return errors.New("meowcaller: stale or unrequested local group rekey")
	}
	stillCurrent := func() bool {
		e.mu.Lock()
		defer e.mu.Unlock()
		return e.calls[update.CallID] == expected && groupLocalRekeyAllowed(expected, update.TransactionID)
	}
	recipients := groupRekeyRecipients(update, self)
	if len(recipients) == 0 {
		return errors.New("meowcaller: group rekey has no remote connected devices")
	}
	rawKey := make([]byte, 32)
	defer clear(rawKey)
	if _, err := rand.Read(rawKey); err != nil {
		return err
	}
	nodes := make([]waBinary.Node, len(recipients))
	for i, recipient := range recipients {
		if !stillCurrent() {
			return errors.New("meowcaller: group rekey owner changed")
		}
		ciphertext, encType, _, err := encryptCallKeyForDevice(ctx, e.c.wa, recipient, rawKey)
		if err != nil {
			return fmt.Errorf("meowcaller: encrypt group epoch for %s: %w", recipient, err)
		}
		node, err := signaling.BuildGroupEncRekey(signaling.GroupEncRekeyParams{
			CallID: update.CallID, To: recipient, CallCreator: update.CallCreator,
			TransactionID: update.TransactionID, RequestID: e.nextCallNodeID(),
			DeviceKey: signaling.OfferDeviceKey{
				DeviceJid: recipient, Ciphertext: ciphertext, EncType: encType,
			},
		})
		if err != nil {
			return err
		}
		nodes[i] = node
	}
	return completeGroupEpochFanout(ctx, len(nodes), func(i int) error {
		if !stillCurrent() {
			return errors.New("meowcaller: group rekey owner changed")
		}
		if err := e.transmitCallNode(ctx, nodes[i]); err != nil {
			return fmt.Errorf("meowcaller: send group epoch to %s: %w", recipients[i], err)
		}
		return nil
	}, func() error {
		if err := e.installGroupEpoch(update.CallID, update.TransactionID, rawKey, expected, self, nil, nil); err != nil {
			return fmt.Errorf("meowcaller: install local group epoch: %w", err)
		}
		return nil
	})
}

// completeGroupEpochFanout only commits the local epoch after every required
// submission has succeeded. A successful submission is not a remote install
// acknowledgement; partial delivery still needs protocol-level reconciliation.
func completeGroupEpochFanout(ctx context.Context, count int, send func(int) error, commit func() error) error {
	if count <= 0 {
		return errors.New("meowcaller: group rekey has no recipients")
	}
	var failures []error
	for i := 0; i < count; i++ {
		if err := ctx.Err(); err != nil {
			return errors.Join(append(failures, err)...)
		}
		if err := send(i); err != nil {
			failures = append(failures, err)
		}
	}
	if err := ctx.Err(); err != nil {
		failures = append(failures, err)
	}
	if err := errors.Join(failures...); err != nil {
		return err
	}
	return commit()
}

func groupRekeyRecipients(update groupCallUpdate, self types.JID) []types.JID {
	// Source of truth: https://github.com/purpshell/meowcaller/blob/d9df3eb9d96ea5260ffcd4036b6669499a1c1bc2/datasheets/voip-group-key-epoch-fanout.md#L145-L162
	seen := make(map[types.JID]struct{})
	var recipients []types.JID
	for _, participant := range update.Participants {
		if participant.State != "connected" {
			continue
		}
		for _, device := range participant.Devices {
			if !device.HasPID || device.JID.IsEmpty() || device.JID == self {
				continue
			}
			if _, exists := seen[device.JID]; exists {
				continue
			}
			seen[device.JID] = struct{}{}
			recipients = append(recipients, device.JID)
		}
	}
	return recipients
}

func cloneGroupCallUpdate(update groupCallUpdate) groupCallUpdate {
	// Source of truth: https://github.com/purpshell/meowcaller/blob/a9e4195fb846a730f30ce98c26a7d1c03993fdb2/datasheets/group-media-relay-refresh.md#L59-L69
	out := update
	out.Participants = make([]groupCallParticipant, len(update.Participants))
	for i, participant := range update.Participants {
		out.Participants[i] = participant
		out.Participants[i].Devices = make([]groupCallDevice, len(participant.Devices))
		for j, device := range participant.Devices {
			out.Participants[i].Devices[j] = device
			out.Participants[i].Devices[j].Capability = bytes.Clone(device.Capability)
		}
	}
	if update.Relay != nil {
		relay := *update.Relay
		relay.Key = bytes.Clone(update.Relay.Key)
		relay.HBHKey = bytes.Clone(update.Relay.HBHKey)
		relay.Tokens = cloneByteSlices(update.Relay.Tokens)
		relay.AuthTokens = cloneByteSlices(update.Relay.AuthTokens)
		relay.Endpoints = append([]groupCallRelayEndpoint(nil), update.Relay.Endpoints...)
		out.Relay = &relay
	}
	return out
}

// Admission is an exact connected device in the authoritative group roster.
// The existence of a relay, another connected participant or an epoch is not
// proof that this linked device has left the waiting room.
func linkRosterAdmitsDevice(update groupCallUpdate, self types.JID) bool {
	if self.IsEmpty() || self.User == "" {
		return false
	}
	for _, participant := range update.Participants {
		if participant.State != "connected" || participant.JID.IsEmpty() {
			continue
		}
		for _, device := range participant.Devices {
			if device.JID == self && device.HasPID {
				return true
			}
		}
	}
	return false
}
