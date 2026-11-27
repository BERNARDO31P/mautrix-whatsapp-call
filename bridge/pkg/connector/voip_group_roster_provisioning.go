package connector

import (
	"context"
	"github.com/purpshell/meowcaller"
)

func (wa *WhatsAppClient) performRTCGroupRoster(parent context.Context, callID string) (rtcGroupRosterResult, error) {
	fail := rtcGroupRosterResult{}
	if !wa.groupParticipantAvailable() || wa.Client.Store == nil {
		return fail, errRTCGroupUnavailable
	}
	ctx, release, ok := wa.bindVoIPConnection(parent)
	if !ok {
		return fail, errRTCGroupUnavailable
	}
	defer release()
	call, ok := wa.WhatsAppVoIPCall(callID)
	if !ok || call == nil || call.State() != meowcaller.CallPhaseActive {
		return fail, errRTCGroupNotAllowed
	}
	group, ok := call.GroupState()
	if !ok || group.TransactionID == 0 || len(group.Participants) > 256 {
		return fail, errRTCGroupNotAllowed
	}
	own := wa.Client.Store.GetLID().ToNonAD()
	if own.IsEmpty() {
		return fail, errRTCGroupUnavailable
	}
	result := rtcGroupRosterResult{CallID: callID, Transaction: group.TransactionID, Revision: group.MediaRevision, Participants: make([]rtcGroupRosterParticipant, 0, len(group.Participants))}
	aliases := map[string]int{}
	for i, p := range group.Participants {
		jid := p.JID.ToNonAD()
		identities := []string{jid.String()}
		if !p.PN.IsEmpty() {
			identities = append(identities, p.PN.ToNonAD().String())
		}
		for _, id := range identities {
			if previous, exists := aliases[id]; !validRTCGroupTarget(id) || exists && previous != i {
				return fail, errRTCGroupNotAllowed
			}
			aliases[id] = i
		}
		if len(p.Devices) > 32 {
			return fail, errRTCGroupNotAllowed
		}
		row := rtcGroupRosterParticipant{ID: jid.String(), State: p.State, Self: jid == own, Devices: make([]rtcGroupRosterDevice, 0, len(p.Devices))}
		for _, d := range p.Devices {
			row.Devices = append(row.Devices, rtcGroupRosterDevice{ID: d.JID.String(), Microphone: d.Media.Microphone, Camera: d.Media.Camera})
		}
		result.Participants = append(result.Participants, row)
	}
	if ctx.Err() != nil {
		return fail, ctx.Err()
	}
	current, ok := wa.WhatsAppVoIPCall(callID)
	if !ok || current != call || call.State() != meowcaller.CallPhaseActive {
		return fail, errRTCGroupUnknown
	}
	latest, ok := call.GroupState()
	if !ok || latest.TransactionID != result.Transaction || latest.MediaRevision != result.Revision {
		return fail, errRTCGroupUnknown
	}
	return result, nil
}
