package connector

// Ad-hoc calling is an explicit client extension, never inferred from a
// normal group invite or a failed direct call. All targets must be distinct
// recognized ghost users; the WhatsApp roster is independently checked later.
func rtcAdHocTargets(group, roomMention bool, users []string, resolve func(string) (string, bool)) ([]string, bool) {
	if !group || roomMention || len(users) < 2 || len(users) > 31 {
		return nil, false
	}
	targets := make([]string, 0, len(users))
	seen := make(map[string]bool, len(users))
	for _, user := range users {
		target, ok := resolve(user)
		if !ok || target == "" || seen[target] {
			return nil, false
		}
		seen[target] = true
		targets = append(targets, target)
	}
	return targets, true
}
