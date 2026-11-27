package connector

import (
	"errors"

	"github.com/purpshell/meowcaller"
	"github.com/purpshell/meowcaller/rtp"
	"go.mau.fi/mautrix-whatsapp/pkg/connector/voipbridge"
	"go.mau.fi/mautrix-whatsapp/pkg/connector/voipbridge/rtcwire"
	"go.mau.fi/mautrix-whatsapp/pkg/waid"
	"go.mau.fi/whatsmeow/types"
)

// Only authoritative, connected, exact provider devices qualify. This is not
// a Matrix ghost admission decision and does not create any new identities.
func (wa *WhatsAppClient) reconcileParticipantRoster(bus *voipbridge.MediaBus, state meowcaller.GroupCallState) {
	if !bus.NeedsParticipantRoster(state.TransactionID) {
		return
	}
	var devices []voipbridge.ParticipantRosterDevice
	seenUsers := make(map[string]bool)
	seenDevices := make(map[string]bool)
	valid := len(state.Participants) <= 256
	total := 0
	for _, p := range state.Participants {
		user := p.JID.ToNonAD()
		total += len(p.Devices)
		if user.IsEmpty() || user.User == "" || seenUsers[user.String()] || total > 256 || len(p.Devices) > 32 {
			valid = false
			break
		}
		seenUsers[user.String()] = true
		for _, d := range p.Devices {
			if d.JID.IsEmpty() || d.JID.User == "" || d.JID.ToNonAD() != user || seenDevices[d.JID.String()] {
				valid = false
				break
			}
			seenDevices[d.JID.String()] = true
			if p.State != "connected" || !d.HasPID || wa.IsOwnJID(d.JID) {
				continue
			}
			devices = append(devices, voipbridge.ParticipantRosterDevice{Device: d.JID.String(), ParticipantID: rtp.FormatE2ESrtpParticipantID(d.JID.String()), PID: d.PID})
		}
		if !valid {
			break
		}
	}
	if !valid {
		devices = nil
	}
	bus.ReconcileParticipantRoster(state.TransactionID, devices)
}

// participantRosterProposal captures the exact live roster, without creating a
// portal, looking up contacts, or provisioning an identity. The remote worker
// must validate the prepared Matrix identity and joined room independently.
// Multiple devices of the same canonical WhatsApp user share its existing
// ghost. Reject a collision between distinct canonical users.
func (wa *WhatsAppClient) participantRosterProposal(bus *voipbridge.MediaBus, callID string) (rtcwire.Message, uint64, <-chan struct{}, error) {
	if bus == nil {
		return rtcwire.Message{}, 0, nil, errors.New("missing participant media bus")
	}
	generation, revision, devices, changed, ok := bus.ParticipantRosterSnapshot()
	if !ok || len(devices) > 32 || callID == "" || len(callID) > 256 {
		return rtcwire.Message{}, 0, changed, errors.New("group participant roster unavailable")
	}
	msg := rtcwire.Message{Type: "whatsapp_participant_roster", CallID: callID, Group: true, Generation: generation}
	seen := make(map[string]string, len(devices))
	for _, device := range devices {
		jid, err := types.ParseJID(device.Device)
		if err != nil || jid.IsEmpty() || jid.String() != device.Device || device.ParticipantID != rtp.FormatE2ESrtpParticipantID(device.Device) {
			return rtcwire.Message{}, 0, changed, errors.New("invalid authoritative provider binding")
		}
		user := rtcRoomPeerJID(jid)
		if user.Server != types.DefaultUserServer && user.Server != types.HiddenUserServer || wa.IsOwnJID(user) {
			return rtcwire.Message{}, 0, changed, errors.New("unsupported provider participant identity")
		}
		networkID := waid.MakeUserID(user)
		if networkID == "" {
			return rtcwire.Message{}, 0, changed, errors.New("missing provider ghost identity")
		}
		ghost := wa.Main.Bridge.Matrix.FormatGhostMXID(networkID).String()
		if ghost == "" || ghost == wa.Main.Bridge.Bot.GetMXID().String() || ghost == wa.UserLogin.UserMXID.String() || (seen[ghost] != "" && seen[ghost] != user.String()) {
			return rtcwire.Message{}, 0, changed, errors.New("ambiguous participant ghost identity")
		}
		seen[ghost] = user.String()
		msg.ParticipantBindings = append(msg.ParticipantBindings, rtcwire.ParticipantBinding{ParticipantID: device.ParticipantID, Device: device.Device, GhostMXID: ghost})
	}
	if !bus.ParticipantRosterCurrent(generation, revision) {
		return rtcwire.Message{}, 0, changed, errors.New("participant roster changed during mapping")
	}
	return msg, revision, changed, nil
}
