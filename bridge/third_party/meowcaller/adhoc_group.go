package meowcaller

import (
	"context"
	"errors"

	"go.mau.fi/whatsmeow/types"
)

// AdHocGroupCallFromRoster starts an unbound group call to an explicit subset
// of a current chat roster. The chat is the authorization boundary, not the
// call destination. No unavailable target is silently dropped, and no roster
// expansion is permitted. A direct call cannot be converted by this API.
func (c *Client) AdHocGroupCallFromRoster(ctx context.Context, groupID string, targets []string, video bool) (*Call, error) {
	if c == nil || c.eng == nil || c.wa == nil || c.getGroupInfo == nil || ctx.Err() != nil || len(targets) < 2 || len(targets) > 31 {
		return nil, ErrGroupParticipantNotAllowed
	}
	group, err := parseGroupCallID(groupID)
	if err != nil {
		return nil, err
	}
	info, err := c.getGroupInfo(ctx, group)
	if err != nil {
		return nil, err
	}
	if info == nil || info.JID.ToNonAD() != group {
		return nil, ErrGroupParticipantNotAllowed
	}
	own := c.groupSelfJIDs()
	canonical, err := explicitAdHocRoster(info.Participants, own, targets)
	if err != nil {
		return nil, err
	}
	resolved := make([]string, 0, len(canonical))
	seen := make(map[types.JID]bool, len(canonical))
	for _, target := range canonical {
		lid, err := resolvePeerLID(ctx, c.wa, target.String())
		if err != nil {
			return nil, err
		}
		lid = lid.ToNonAD()
		if lid.IsEmpty() || lid.Server != types.HiddenUserServer || seen[lid] {
			return nil, ErrGroupParticipantNotAllowed
		}
		for _, self := range own {
			if !self.IsEmpty() && self.ToNonAD() == lid {
				return nil, ErrGroupParticipantNotAllowed
			}
		}
		seen[lid] = true
		resolved = append(resolved, lid.String())
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return c.GroupCallWithOptions(ctx, resolved, GroupCallOptions{Video: video, strictAdHoc: true})
}

func explicitAdHocRoster(members []types.GroupParticipant, own []types.JID, targets []string) ([]types.JID, error) {
	if len(targets) < 2 || len(targets) > 31 {
		return nil, ErrGroupParticipantNotAllowed
	}
	result := make([]types.JID, 0, len(targets))
	seen := make(map[types.JID]bool, len(targets))
	for _, raw := range targets {
		jid, err := types.ParseJID(raw)
		if err != nil || jid.String() != raw {
			return nil, ErrGroupParticipantNotAllowed
		}
		selected, err := explicitGroupTarget(members, own, jid)
		if err != nil {
			return nil, err
		}
		if seen[selected] {
			return nil, errors.New("duplicate ad-hoc group participant")
		}
		seen[selected] = true
		result = append(result, selected)
	}
	return result, nil
}
