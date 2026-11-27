package connector

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/purpshell/meowcaller"
	"go.mau.fi/whatsmeow"
	waBinary "go.mau.fi/whatsmeow/binary"
	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/database"
	bridgev2matrix "maunium.net/go/mautrix/bridgev2/matrix"
	"maunium.net/go/mautrix/event"
	"maunium.net/go/mautrix/id"

	"go.mau.fi/whatsmeow/types"

	"go.mau.fi/mautrix-whatsapp/pkg/connector/voipbridge/rtcwire"
	"go.mau.fi/mautrix-whatsapp/pkg/waid"
)

const (
	rtcMemberStable       = "m.rtc.member"
	rtcMemberWire         = "org.matrix.msc4143.rtc.member"
	rtcMemberLegacy       = "org.matrix.msc3401.call.member"
	rtcMemberLegacyAlias  = "m.call.member"
	rtcSlotStable         = "m.rtc.slot"
	rtcSlotWire           = "org.matrix.msc4143.rtc.slot"
	rtcInviteStable       = "m.rtc.invite"
	rtcNotificationStable = "m.rtc.notification"
	rtcInviteWire         = "org.matrix.msc4075.rtc.notification"
	rtcDeclineStable      = "m.rtc.decline"
	rtcDeclineWire        = "org.matrix.msc4310.rtc.decline"
	rtcMaxLifetimeMS      = int64(120_000)
	rtcClockSkew          = 30 * time.Second
	rtcMemberLifetime     = 5 * time.Minute
	rtcReferenceWait      = 5 * time.Second
	rtcPendingInviteLimit = 64
	matrixRTCSlotDefault  = "m.call#room"
)

func matrixRTCDMPowerLevels(events map[event.Type]int) map[event.Type]int {
	return matrixRTCDMPowerLevelsForPeer(false, events)
}

type matrixRTCCallDeviceClass uint8

const (
	matrixRTCCallDeviceClassUnknown matrixRTCCallDeviceClass = iota
	matrixRTCCallDeviceClassConsumer
	matrixRTCCallDeviceClassHosted
	matrixRTCCallDeviceClassUnavailable
)

func matrixRTCCallStaticJIDAvailability(jid types.JID) (matrixRTCCallAvailability, string) {
	jid = jid.ToNonAD()
	if jid.IsBot() || jid.Server == types.MessengerServer || jid.Server == types.InteropServer {
		return matrixRTCCallAvailabilityDisabled, "jid_server:" + jid.Server
	}
	return matrixRTCCallAvailabilityUnknown, ""
}

// Only known WhatsApp account device domains can establish a consumer or
// hosted classification. Unknown domains never imply a consumer fallback.
func classifyMatrixRTCCallDevices(devices []types.JID) (matrixRTCCallDeviceClass, string) {
	if len(devices) == 0 {
		return matrixRTCCallDeviceClassUnavailable, "devices:none"
	}
	hosted := false
	for _, device := range devices {
		if device.User == "" || device.ToNonAD().IsBot() {
			continue
		}
		switch device.Server {
		case types.HostedServer, types.HostedLIDServer:
			hosted = true
		case types.DefaultUserServer, types.HiddenUserServer:
			return matrixRTCCallDeviceClassConsumer, "devices:consumer"
		}
	}
	if hosted {
		return matrixRTCCallDeviceClassHosted, "devices:hosted"
	}
	return matrixRTCCallDeviceClassUnknown, "devices:unknown"
}

// A hosted device belongs to the corresponding PN or LID account. Do not
// equate a PN and LID merely because their numeric user parts match.
func matrixRTCCallAccount(jid types.JID) types.JID {
	jid = jid.ToNonAD()
	switch jid.Server {
	case types.HostedServer:
		jid.Server = types.DefaultUserServer
	case types.HostedLIDServer:
		jid.Server = types.HiddenUserServer
	}
	return jid
}

func matrixRTCMatchesCallAccount(jid types.JID, peers []types.JID) bool {
	jid = matrixRTCCallAccount(jid)
	if jid.User == "" || (jid.Server != types.DefaultUserServer && jid.Server != types.HiddenUserServer) {
		return false
	}
	for _, peer := range peers {
		if matrixRTCCallAccount(peer) == jid {
			return true
		}
	}
	return false
}

func classifyMatrixRTCCallPeerDevices(devices []types.JID, peers []types.JID) (matrixRTCCallDeviceClass, string) {
	if len(devices) == 0 {
		return classifyMatrixRTCCallDevices(devices)
	}
	matched := make([]types.JID, 0, len(devices))
	for _, device := range devices {
		if matrixRTCMatchesCallAccount(device, peers) {
			matched = append(matched, device)
		}
	}
	if len(matched) == 0 {
		return matrixRTCCallDeviceClassUnknown, "devices:unattributed"
	}
	return classifyMatrixRTCCallDevices(matched)
}

func matrixRTCCallPortalServer(server string) bool {
	switch server {
	case types.DefaultUserServer,
		types.HiddenUserServer,
		types.BotServer,
		types.HostedServer,
		types.HostedLIDServer,
		types.MessengerServer,
		types.InteropServer:
		return true
	default:
		return false
	}
}

func normalizeMatrixRTCCallOption(value string) string {
	value = strings.TrimSpace(strings.ToLower(value))
	value = strings.NewReplacer("-", "_", ".", "_", " ", "_").Replace(value)
	return strings.Trim(value, "_\"'")
}

// Keep every value: flat aliases, nested options and repeated profile nodes
// can describe the same capability with contradictory values.
type matrixRTCCallOptions map[string][]string

func (options matrixRTCCallOptions) add(key, value string) {
	key = normalizeMatrixRTCCallOption(key)
	options[key] = append(options[key], value)
}

// Decode object members in wire order instead of using a map, which would
// silently overwrite repeated JSON properties before precedence is applied.
func flattenMatrixRTCCallJSON(prefix string, decoder *json.Decoder, out matrixRTCCallOptions) bool {
	token, err := decoder.Token()
	if err != nil {
		return false
	}
	switch typed := token.(type) {
	case json.Delim:
		switch typed {
		case '{':
			for decoder.More() {
				key, err := decoder.Token()
				name, ok := key.(string)
				if err != nil || !ok {
					return false
				}
				normalizedKey := normalizeMatrixRTCCallOption(name)
				if prefix != "" {
					normalizedKey = prefix + "_" + normalizedKey
				}
				if !flattenMatrixRTCCallJSON(normalizedKey, decoder, out) {
					return false
				}
			}
		case '[':
			// Arrays are not capability aliases. Consume them without
			// interpreting any nested value as authoritative evidence.
			for decoder.More() {
				var ignored json.RawMessage
				if decoder.Decode(&ignored) != nil {
					return false
				}
			}
		default:
			return false
		}
		end, err := decoder.Token()
		return err == nil && ((typed == '{' && end == json.Delim('}')) || (typed == '[' && end == json.Delim(']')))
	case string:
		out.add(prefix, typed)
	case bool:
		if typed {
			out.add(prefix, "true")
		} else {
			out.add(prefix, "false")
		}
	case float64:
		if typed == 0 {
			out.add(prefix, "0")
		} else if typed == 1 {
			out.add(prefix, "1")
		}
	}
	return true
}

func expandMatrixRTCCallOptions(options matrixRTCCallOptions) matrixRTCCallOptions {
	expanded := make(matrixRTCCallOptions, len(options)*2)
	for key, rawValues := range options {
		for _, rawValue := range rawValues {
			normalizedKey := normalizeMatrixRTCCallOption(key)
			trimmed := strings.TrimSpace(rawValue)
			expanded.add(normalizedKey, trimmed)
			if strings.HasPrefix(trimmed, "{") && json.Valid([]byte(trimmed)) {
				// Commit only after the whole object has decoded successfully.
				nested := make(matrixRTCCallOptions)
				if flattenMatrixRTCCallJSON(normalizedKey, json.NewDecoder(strings.NewReader(trimmed)), nested) {
					for nestedKey, values := range nested {
						expanded[nestedKey] = append(expanded[nestedKey], values...)
					}
				}
			}
		}
	}
	return expanded
}

func matrixRTCCallBool(value string) (enabled bool, known bool) {
	switch normalizeMatrixRTCCallOption(value) {
	case "enabled", "enable", "true", "1", "on", "available", "allowed":
		return true, true
	case "disabled", "disable", "false", "0", "off", "unavailable", "not_enabled", "not_allowed":
		return false, true
	default:
		return false, false
	}
}

// classifyWhatsAppBusinessCallOptions mirrors the distinction made by the
// WhatsApp consumer UI: calling.status controls whether business calling is
// actually enabled, while call_icon_visibility can hide the chat call button
// even though other WhatsApp call entry points may still work.
func classifyWhatsAppBusinessCallOptions(options map[string]string) (matrixRTCCallAvailability, string) {
	values := make(matrixRTCCallOptions, len(options))
	for key, value := range options {
		values.add(key, value)
	}
	return classifyWhatsAppBusinessCallValues(values)
}

func classifyWhatsAppBusinessCallValues(options matrixRTCCallOptions) (matrixRTCCallAvailability, string) {
	expanded := expandMatrixRTCCallOptions(options)
	availability, source := matrixRTCCallAvailabilityUnknown, ""
	// Explicit disabled > hidden > enabled > unknown. The enum ordering is
	// not used as a policy: keep the precedence explicit. Within a category,
	// choose a deterministic diagnostic even when map iteration is random.
	rank := func(a matrixRTCCallAvailability) int {
		switch a {
		case matrixRTCCallAvailabilityDisabled:
			return 3
		case matrixRTCCallAvailabilityHidden:
			return 2
		case matrixRTCCallAvailabilityEnabled:
			return 1
		default:
			return 0
		}
	}
	consider := func(a matrixRTCCallAvailability, key, value string) {
		candidate := "business_profile:" + key + "=" + value
		if rank(a) > rank(availability) || (a == availability && candidate < source) {
			availability, source = a, candidate
		}
	}
	for _, key := range []string{
		"business_calling_enabled",
		"calling_status", "calling_status_status", "calling_calling_status",
		"call_status", "calling_enabled", "call_enabled", "calls_enabled", "voice_call_enabled",
	} {
		for _, rawValue := range expanded[key] {
			value := normalizeMatrixRTCCallOption(rawValue)
			if enabled, known := matrixRTCCallBool(value); known {
				if enabled {
					consider(matrixRTCCallAvailabilityEnabled, key, value)
				} else {
					consider(matrixRTCCallAvailabilityDisabled, key, value)
				}
			}
		}
	}
	for _, key := range []string{
		"call_icon_visibility", "calling_call_icon_visibility", "calling_icon_visibility",
	} {
		for _, rawValue := range expanded[key] {
			value := normalizeMatrixRTCCallOption(rawValue)
			switch value {
			case "hide_in_chat", "disable_all", "hidden", "hide", "disabled":
				consider(matrixRTCCallAvailabilityHidden, key, value)
			}
		}
	}
	return availability, source
}

func (wa *WhatsAppClient) matrixRTCBusinessProfileJID(ctx context.Context, jid types.JID) types.JID {
	lookup := jid.ToNonAD()
	if lookup.Server == types.HiddenUserServer {
		if alt, err := wa.GetStore().GetAltJID(ctx, lookup); err == nil && !alt.IsEmpty() {
			lookup = alt.ToNonAD()
		}
	}
	return lookup
}

func (wa *WhatsAppClient) matrixRTCCallCandidateJIDs(ctx context.Context, jid types.JID) []types.JID {
	seen := make(map[string]struct{}, 3)
	candidates := make([]types.JID, 0, 3)
	add := func(candidate types.JID) {
		candidate = candidate.ToNonAD()
		if candidate.IsEmpty() {
			return
		}
		key := candidate.String()
		if _, ok := seen[key]; ok {
			return
		}
		seen[key] = struct{}{}
		candidates = append(candidates, candidate)
	}
	add(jid)
	add(wa.maybeConvertJIDToLID(ctx, jid))
	add(wa.matrixRTCBusinessProfileJID(ctx, jid))
	return candidates
}

func matrixRTCCallAvailabilityCacheKey(loginID string, jid types.JID) string {
	return loginID + "\x00" + jid.ToNonAD().String()
}

func (wa *WhatsAppClient) matrixRTCCallAvailabilityKeys(ctx context.Context, jid types.JID) []string {
	loginID := string(wa.UserLogin.ID)
	if loginID == "" {
		return nil
	}
	candidates := wa.matrixRTCCallCandidateJIDs(ctx, jid)
	keys := make([]string, 0, len(candidates))
	for _, candidate := range candidates {
		keys = append(keys, matrixRTCCallAvailabilityCacheKey(loginID, candidate))
	}
	return keys
}

func (wa *WhatsAppClient) rememberMatrixRTCCallAvailabilityForPeer(ctx context.Context, jid types.JID, availability matrixRTCCallAvailability, source string, ttl time.Duration) matrixRTCCallAvailabilityCacheEntry {
	keys := wa.matrixRTCCallAvailabilityKeys(ctx, jid)
	if ctx.Err() != nil {
		return matrixRTCCallAvailabilityCacheEntry{}
	}
	now := time.Now()
	return matrixRTCCallAvailabilityCache.rememberContext(ctx, keys, matrixRTCCallAvailabilityCacheEntry{availability: availability, source: source, expires: now.Add(ttl)}, now)
}

func matrixRTCNodeStringValue(value any) (string, bool) {
	switch typed := value.(type) {
	case string:
		return normalizeMatrixRTCCallOption(typed), true
	case []byte:
		return normalizeMatrixRTCCallOption(string(typed)), true
	case bool:
		if typed {
			return "true", true
		}
		return "false", true
	case int:
		if typed == 0 {
			return "0", true
		} else if typed == 1 {
			return "1", true
		}
	}
	return "", false
}

func matrixRTCVoIPFeatureValue(node waBinary.Node) (matrixRTCCallAvailability, string) {
	availability, source := matrixRTCCallAvailabilityUnknown, ""
	seenValue := false
	consider := func(raw any, prefix string) {
		seenValue = true
		if value, ok := matrixRTCNodeStringValue(raw); ok {
			if enabled, known := matrixRTCCallBool(value); known {
				if !enabled {
					availability, source = matrixRTCCallAvailabilityDisabled, prefix+value
				} else if availability == matrixRTCCallAvailabilityUnknown {
					availability, source = matrixRTCCallAvailabilityEnabled, prefix+value
				}
			}
		}
	}
	for _, key := range []string{"enabled", "supported", "available", "status", "value"} {
		if raw, ok := node.Attrs[key]; ok {
			consider(raw, "usync:feature:voip:"+key+"=")
		}
	}
	if node.Content != nil {
		children, isList := node.Content.([]waBinary.Node)
		if value, ok := matrixRTCNodeStringValue(node.Content); (!ok || value != "") && !(isList && len(children) == 0) {
			consider(node.Content, "usync:feature:voip=")
		}
	}
	if !seenValue {
		return matrixRTCCallAvailabilityEnabled, "usync:feature:voip=present"
	}
	return availability, source
}

func matrixRTCFeatureNodes(node waBinary.Node, tag string) (matches []waBinary.Node) {
	for _, child := range node.GetChildren() {
		if child.Tag == tag {
			matches = append(matches, child)
		} else {
			matches = append(matches, matrixRTCFeatureNodes(child, tag)...)
		}
	}
	return
}

func classifyWhatsAppVoIPFeatureResponse(resp *waBinary.Node, peers ...types.JID) (matrixRTCCallAvailability, string) {
	if resp == nil || resp.Tag != "iq" || len(peers) == 0 {
		return matrixRTCCallAvailabilityUnknown, ""
	}
	availability, source := matrixRTCCallAvailabilityUnknown, ""
	matched := false
	incomplete := false
	for _, usync := range resp.GetChildrenByTag("usync") {
		for _, list := range usync.GetChildrenByTag("list") {
			for _, user := range list.GetChildrenByTag("user") {
				jid, ok := user.Attrs["jid"].(types.JID)
				if !ok || !matrixRTCMatchesCallAccount(jid, peers) {
					continue
				}
				matched = true
				features := user.GetChildrenByTag("feature")
				if len(features) == 0 || len(user.GetChildrenByTag("error")) != 0 {
					incomplete = true
					continue
				}
				for _, feature := range features {
					if len(matrixRTCFeatureNodes(feature, "error")) != 0 {
						incomplete = true
						continue
					}
					if feature.Content != nil {
						if _, ok := feature.Content.([]waBinary.Node); !ok {
							incomplete = true
							continue
						}
					}
					voips := matrixRTCFeatureNodes(feature, "voip")
					if len(voips) == 0 {
						return matrixRTCCallAvailabilityDisabled, "usync:feature:voip=absent"
					}
					for _, voip := range voips {
						if len(voip.GetChildrenByTag("error")) != 0 {
							incomplete = true
							continue
						}
						a, detail := matrixRTCVoIPFeatureValue(voip)
						if a == matrixRTCCallAvailabilityDisabled {
							return a, detail
						}
						if a == matrixRTCCallAvailabilityUnknown {
							incomplete = true
						} else if availability == matrixRTCCallAvailabilityUnknown {
							availability, source = a, detail
						}
					}
				}
			}
		}
	}
	if !matched || incomplete {
		return matrixRTCCallAvailabilityUnknown, ""
	}
	return availability, source
}

func (wa *WhatsAppClient) queryWhatsAppCallDevices(ctx context.Context, jid types.JID, peers []types.JID) (matrixRTCCallDeviceClass, string) {
	if wa.Client == nil || !wa.Client.IsConnected() {
		return matrixRTCCallDeviceClassUnknown, ""
	}
	if jid.IsEmpty() {
		return matrixRTCCallDeviceClassUnknown, ""
	}
	queryCtx, cancel := context.WithTimeout(ctx, 4*time.Second)
	defer cancel()
	devices, err := wa.Client.GetUserDevices(queryCtx, []types.JID{jid})
	if err != nil {
		wa.UserLogin.Log.Debug().Err(err).Stringer("peer", jid).
			Msg("WhatsApp device lookup unavailable; leaving MatrixRTC device class unknown")
		return matrixRTCCallDeviceClassUnknown, ""
	}
	class, source := classifyMatrixRTCCallPeerDevices(devices, peers)
	wa.UserLogin.Log.Debug().Stringer("peer", jid).Str("source", source).Int("devices", len(devices)).
		Msg("WhatsApp MatrixRTC device class refreshed")
	return class, source
}

func (wa *WhatsAppClient) queryWhatsAppVoIPFeature(ctx context.Context, jid types.JID, peers []types.JID) (matrixRTCCallAvailability, string) {
	if wa.Client == nil || !wa.Client.IsConnected() {
		return matrixRTCCallAvailabilityUnknown, ""
	}
	if jid.IsEmpty() {
		return matrixRTCCallAvailabilityUnknown, ""
	}
	queryCtx, cancel := context.WithTimeout(ctx, 4*time.Second)
	defer cancel()
	resp, err := wa.Client.DangerousInternals().SendIQ(queryCtx, whatsmeow.DangerousInfoQuery{
		Namespace: "usync",
		Type:      "get",
		To:        types.ServerJID,
		Content: []waBinary.Node{{
			Tag: "usync",
			Attrs: waBinary.Attrs{
				"sid":     wa.Client.DangerousInternals().GenerateRequestID(),
				"mode":    "query",
				"last":    "true",
				"index":   "0",
				"context": "interactive",
			},
			Content: []waBinary.Node{
				{Tag: "query", Content: []waBinary.Node{{Tag: "feature", Content: []waBinary.Node{{Tag: "voip"}}}}},
				{Tag: "list", Content: []waBinary.Node{{Tag: "user", Attrs: waBinary.Attrs{"jid": jid}}}},
			},
		}},
	})
	if err != nil {
		wa.UserLogin.Log.Debug().Err(err).Stringer("peer", jid).Msg("WhatsApp voip feature lookup unavailable; leaving MatrixRTC call availability unknown")
		return matrixRTCCallAvailabilityUnknown, ""
	}
	return classifyWhatsAppVoIPFeatureResponse(resp, peers...)
}

// Keep JSON-valued business options intact until expansion; the generic
// node helper normalizes punctuation and is intended for scalar features.
func matrixRTCProfileOptionValue(value any) (string, bool) {
	switch typed := value.(type) {
	case string:
		return typed, true
	case []byte:
		return string(typed), true
	default:
		return matrixRTCNodeStringValue(value)
	}
}

func flattenMatrixRTCBusinessProfileNode(prefix string, node waBinary.Node, out matrixRTCCallOptions) {
	key := normalizeMatrixRTCCallOption(node.Tag)
	if prefix != "" {
		if key != "" {
			key = prefix + "_" + key
		} else {
			key = prefix
		}
	}
	for attrKey, attrValue := range node.Attrs {
		if value, ok := matrixRTCProfileOptionValue(attrValue); ok {
			out.add(strings.Trim(key+"_"+normalizeMatrixRTCCallOption(attrKey), "_"), value)
		}
	}
	if value, ok := matrixRTCProfileOptionValue(node.Content); ok && key != "" && value != "" {
		out.add(key, value)
	}
	for _, child := range node.GetChildren() {
		flattenMatrixRTCBusinessProfileNode(key, child, out)
	}
}

// Collect repeated option containers only from profiles attributed to the
// requested account. The pinned Whatsmeow profile parser requires a JID.
func classifyWhatsAppBusinessCallProfileResponse(resp *waBinary.Node, peers ...types.JID) (matrixRTCCallAvailability, string) {
	if resp == nil || resp.Tag != "iq" || len(peers) == 0 {
		return matrixRTCCallAvailabilityUnknown, ""
	}
	options := make(matrixRTCCallOptions)
	for _, container := range resp.GetChildren() {
		if container.Tag != "business_profile" {
			continue
		}
		for _, profile := range container.GetChildren() {
			jid, ok := profile.Attrs["jid"].(types.JID)
			if profile.Tag != "profile" || !ok || !matrixRTCMatchesCallAccount(jid, peers) {
				continue
			}
			for _, child := range profile.GetChildren() {
				if child.Tag == "profile_options" {
					for _, option := range child.GetChildren() {
						flattenMatrixRTCBusinessProfileNode("", option, options)
					}
				}
			}
		}
	}
	return classifyWhatsAppBusinessCallValues(options)
}

func (wa *WhatsAppClient) queryWhatsAppBusinessCallProfile(ctx context.Context, jid types.JID, peers []types.JID) (matrixRTCCallAvailability, string) {
	if wa.Client == nil || !wa.Client.IsConnected() {
		return matrixRTCCallAvailabilityUnknown, ""
	}
	if jid.IsEmpty() {
		return matrixRTCCallAvailabilityUnknown, ""
	}
	queryCtx, cancel := context.WithTimeout(ctx, 4*time.Second)
	defer cancel()
	resp, err := wa.Client.DangerousInternals().SendIQ(queryCtx, whatsmeow.DangerousInfoQuery{
		Namespace: "w:biz",
		Type:      "get",
		To:        types.ServerJID,
		Content: []waBinary.Node{{
			Tag:   "business_profile",
			Attrs: waBinary.Attrs{"v": "244"},
			Content: []waBinary.Node{{
				Tag:   "profile",
				Attrs: waBinary.Attrs{"jid": jid},
			}},
		}},
	})
	if err != nil {
		wa.UserLogin.Log.Debug().Err(err).Stringer("peer", jid).
			Msg("WhatsApp business calling profile lookup unavailable; keeping MatrixRTC availability from voip feature query")
		return matrixRTCCallAvailabilityUnknown, ""
	}
	availability, source := classifyWhatsAppBusinessCallProfileResponse(resp, peers...)
	if availability != matrixRTCCallAvailabilityUnknown {
		wa.UserLogin.Log.Debug().Stringer("peer", jid).Str("source", source).
			Msg("WhatsApp business calling profile classified for MatrixRTC")
	}
	return availability, source
}

func (wa *WhatsAppClient) rememberMatrixRTCCallEnd(jid types.JID, reason string) bool {
	if !strings.HasPrefix(reason, "server:") {
		return false
	}
	code := strings.TrimPrefix(reason, "server:")
	// 406 is the server response observed when a destination does not accept
	// this consumer WhatsApp call offer. Cache it briefly and refresh the
	// portal so Element stops advertising a call that WhatsApp just rejected.
	// The short TTL lets a changed business calling configuration recover.
	if code == "406" {
		wa.rememberMatrixRTCCallAvailabilityForPeer(context.Background(), jid, matrixRTCCallAvailabilityDisabled, "server:406", 6*time.Hour)
		return true
	}
	return false
}

func combineWhatsAppMatrixRTCCallAvailability(
	deviceClass matrixRTCCallDeviceClass, deviceSource string,
	featureAvailability matrixRTCCallAvailability, featureSource string,
	profileAvailability matrixRTCCallAvailability, profileSource string,
) (matrixRTCCallAvailability, string) {
	availability, source := featureAvailability, featureSource

	// Authoritative business-profile negatives always win. In particular,
	// <business_calling enabled="false"/> must hard-disable calling even when
	// device discovery looks like a normal consumer account or feature/voip
	// is unavailable. An explicit feature/voip negative remains stronger than
	// a positive business-profile fallback.
	switch profileAvailability {
	case matrixRTCCallAvailabilityDisabled:
		availability, source = profileAvailability, profileSource
	case matrixRTCCallAvailabilityHidden:
		if featureAvailability != matrixRTCCallAvailabilityDisabled {
			availability, source = profileAvailability, profileSource
		}
	case matrixRTCCallAvailabilityEnabled:
		if featureAvailability == matrixRTCCallAvailabilityUnknown {
			availability, source = profileAvailability, profileSource
		}
	}

	if deviceClass == matrixRTCCallDeviceClassHosted && profileAvailability != matrixRTCCallAvailabilityEnabled {
		// Cloud API/hosted numbers are not consumer-callable by default. The
		// consumer WhatsApp UI only exposes the call icon after business calling
		// has been explicitly enabled, so unknown must fail closed here.
		return matrixRTCCallAvailabilityDisabled, deviceSource + ":calling_not_explicitly_enabled"
	}

	// A feature/voip transport timeout is unknown, not a negative capability
	// result. If device discovery proves this is a consumer WhatsApp account and
	// no stronger negative exists, keep the call controls available.
	if availability == matrixRTCCallAvailabilityUnknown && deviceClass == matrixRTCCallDeviceClassConsumer {
		return matrixRTCCallAvailabilityEnabled, deviceSource + ":fallback"
	}

	return availability, source
}

func (wa *WhatsAppClient) matrixRTCCallAvailabilityForPeer(ctx context.Context, jid types.JID) (matrixRTCCallAvailability, string) {
	if ctx.Err() != nil {
		return matrixRTCCallAvailabilityUnknown, ""
	}
	jid = jid.ToNonAD()
	if wa.IsOwnJID(jid) {
		return matrixRTCCallAvailabilityDisabled, "note_to_self"
	}
	if availability, source := matrixRTCCallStaticJIDAvailability(jid); availability != matrixRTCCallAvailabilityUnknown {
		return availability, source
	}

	if cached, ok := matrixRTCCallAvailabilityCache.get(wa.matrixRTCCallAvailabilityKeys(ctx, jid), time.Now()); ok {
		return cached.availability, cached.source
	}
	if wa.Client == nil || !wa.Client.IsConnected() {
		return matrixRTCCallAvailabilityUnknown, ""
	}

	// Query the same WhatsApp-side capability data before advertising a Matrix
	// call button. Device discovery is important for Cloud API/coexistence numbers:
	// WhatsApp marks hosted devices explicitly. Hosted-only destinations fail
	// closed unless their business profile explicitly advertises calling enabled.
	lookup := matrixRTCCallAccount(jid)
	peers := wa.matrixRTCCallCandidateJIDs(ctx, lookup)
	if lookup.Server == types.HiddenUserServer {
		for _, peer := range peers {
			if peer.Server == types.DefaultUserServer && peer.User != "" {
				lookup = peer
				break
			}
		}
	}
	deviceClass, deviceSource := wa.queryWhatsAppCallDevices(ctx, lookup, peers)
	if ctx.Err() != nil {
		return matrixRTCCallAvailabilityUnknown, ""
	}
	if deviceClass == matrixRTCCallDeviceClassUnknown && (jid.Server == types.HostedServer || jid.Server == types.HostedLIDServer) {
		deviceClass, deviceSource = matrixRTCCallDeviceClassHosted, "jid:hosted"
	}
	if deviceClass == matrixRTCCallDeviceClassUnavailable {
		cached := wa.rememberMatrixRTCCallAvailabilityForPeer(ctx, jid, matrixRTCCallAvailabilityDisabled, deviceSource, 30*time.Minute)
		return cached.availability, cached.source
	}

	featureAvailability, featureSource := wa.queryWhatsAppVoIPFeature(ctx, lookup, peers)
	if ctx.Err() != nil {
		return matrixRTCCallAvailabilityUnknown, ""
	}
	profileAvailability := matrixRTCCallAvailabilityUnknown
	profileSource := ""
	if featureAvailability != matrixRTCCallAvailabilityDisabled {
		profileAvailability, profileSource = wa.queryWhatsAppBusinessCallProfile(ctx, lookup, peers)
	}

	availability, source := combineWhatsAppMatrixRTCCallAvailability(
		deviceClass, deviceSource,
		featureAvailability, featureSource,
		profileAvailability, profileSource,
	)

	ttl := 6 * time.Hour
	if availability == matrixRTCCallAvailabilityUnknown {
		ttl = time.Hour
	}
	cached := wa.rememberMatrixRTCCallAvailabilityForPeer(ctx, jid, availability, source, ttl)
	availability, source = cached.availability, cached.source
	if availability != matrixRTCCallAvailabilityUnknown {
		wa.UserLogin.Log.Debug().Stringer("peer", jid).Str("source", source).
			Msg("WhatsApp MatrixRTC call availability refreshed")
	}
	return availability, source
}

var rtcProbes = newRTCProbeAdmission(64, 16, 4)

// Chat creation may read the local cache, but capability probes run off the message path.
func (wa *WhatsAppClient) rtcCachedRestriction(ctx context.Context, jid types.JID) bool {
	if wa.IsOwnJID(jid) {
		return true
	}
	if a, _ := matrixRTCCallStaticJIDAvailability(jid); a == matrixRTCCallAvailabilityDisabled {
		return true
	}
	key := matrixRTCCallAvailabilityCacheKey(string(wa.UserLogin.ID), jid.ToNonAD())
	// Resolve only locally stored aliases, using the same lookup as the dial gate.
	// Capability network queries remain asynchronous below.
	aliasCtx, cancel := context.WithTimeout(ctx, 250*time.Millisecond)
	keys := wa.matrixRTCCallAvailabilityKeys(aliasCtx, jid)
	cancel()
	if entry, ok := matrixRTCCallAvailabilityCache.get(keys, time.Now()); ok {
		return entry.availability == matrixRTCCallAvailabilityDisabled || entry.availability == matrixRTCCallAvailabilityHidden
	}
	probeCtx := wa.rtcProbeLifetime.current()
	if probeCtx != nil && probeCtx.Err() == nil && wa.Client != nil && wa.Client.IsConnected() {
		if release, admitted := rtcProbes.admit(string(wa.UserLogin.ID), key); admitted {
			go func() {
				defer release()
				queueCtx, cancelQueue := context.WithTimeout(probeCtx, 20*time.Second)
				defer cancelQueue()
				if !rtcProbes.acquire(queueCtx) {
					return
				}
				defer rtcProbes.releaseSlot()
				if probeCtx.Err() != nil || wa.Client == nil || !wa.Client.IsConnected() {
					return
				}
				ctx, cancel := context.WithTimeout(probeCtx, 20*time.Second)
				defer cancel()
				wa.matrixRTCCallAvailabilityForPeer(ctx, jid)
				if ctx.Err() == nil {
					wa.refreshMatrixRTCCallPermissionForJID(ctx, jid)
				}
			}()
		}
	}
	return true
}

func (wa *WhatsAppClient) rtcPortalPower(ctx context.Context, jid types.JID, dm bool) func(*event.PowerLevelsEventContent) bool {
	return wa.rtcPortalPowerForRoom(ctx, jid, dm, true)
}

func (wa *WhatsAppClient) rtcPortalPowerForRoom(ctx context.Context, jid types.JID, dm, allowInitial bool) func(*event.PowerLevelsEventContent) bool {
	mx, ok := wa.Main.Bridge.Matrix.(*bridgev2matrix.Connector)
	restrict := true
	initial := false
	if ok {
		row, err := wa.Main.Bridge.DB.Portal.GetByKey(ctx, wa.makeWAPortalKey(jid))
		if err == nil {
			initial = allowInitial && (row == nil || row.MXID == "")
			if wa.Main.Config.ExperimentalWhatsAppVoIP && wa.Main.Config.RTCGatewaySocket != "" && mx.Config.Encryption.Allow {
				encrypted := mx.Config.Encryption.Default
				if row != nil && row.MXID != "" {
					encrypted, err = mx.StateStore.IsEncrypted(ctx, row.MXID)
				}
				if encrypted && err == nil {
					restrict = dm && wa.rtcCachedRestriction(ctx, jid)
				}
			}
		}
	}
	return func(power *event.PowerLevelsEventContent) bool {
		return mergeRTCPortalPowerWithInitial(power, restrict, initial)
	}
}

// Record the prior explicit rule together with each restriction in the same
// state event. An administrator's later change takes precedence.
func mergeRTCPortalPower(power *event.PowerLevelsEventContent, restrict bool) bool {
	return mergeRTCPortalPowerWithInitial(power, restrict, false)
}

func mergeRTCPortalPowerWithInitial(power *event.PowerLevelsEventContent, restrict, initial bool) bool {
	if power.Events == nil {
		power.Events = make(map[string]int)
	}
	changed := false
	for typ := range matrixRTCPowerLevels(false, map[event.Type]int{}) {
		key := typ.Type
		marker := "org.mautrix.whatsapp.rtc.previous." + key
		inheritedMarker := "org.mautrix.whatsapp.rtc.inherited." + key
		old, exists := power.Events[key]
		previous, managed := power.Events[marker]
		_, inherited := power.Events[inheritedMarker]
		if (managed || inherited) && (!exists || old != 100) {
			delete(power.Events, marker)
			delete(power.Events, inheritedMarker)
			managed, inherited = false, false
			changed = true
		}
		// New bridge-owned rooms deliberately enable call state events at PL0.
		// An existing room's missing rule, however, is an inherited admin rule.
		if initial && !exists {
			power.Events[key] = 0
			old, exists = 0, true
			changed = true
		}
		if restrict {
			effective := old
			if !exists {
				effective = power.GetEventLevel(typ)
			}
			if effective < 100 {
				if exists {
					power.Events[marker] = old
				} else {
					power.Events[inheritedMarker] = 0
				}
				power.Events[key] = 100
				changed = true
			}
		} else if inherited {
			delete(power.Events, key)
			delete(power.Events, inheritedMarker)
			delete(power.Events, marker)
			changed = true
		} else if managed {
			power.Events[key] = previous
			delete(power.Events, marker)
			changed = true
		}
	}
	return changed
}

func (wa *WhatsAppClient) matrixRTCCallHardDisabled(ctx context.Context, jid types.JID) (bool, string) {
	availability, source := wa.matrixRTCCallAvailabilityForPeer(ctx, jid)
	return availability == matrixRTCCallAvailabilityDisabled || availability == matrixRTCCallAvailabilityHidden, source
}

var rtcPermissionLocks rtcPermissionRefreshLocks

func (wa *WhatsAppClient) refreshMatrixRTCCallPermission(ctx context.Context, portal *bridgev2.Portal) {
	if ctx.Err() != nil || portal == nil || portal.MXID == "" || portal.RoomType != database.RoomTypeDM || portal.OtherUserID == "" {
		return
	}
	jid, err := waid.ParsePortalID(portal.ID)
	if err != nil || !matrixRTCCallPortalServer(jid.Server) {
		return
	}
	mx, ok := portal.Bridge.Matrix.(*bridgev2matrix.Connector)
	if !ok {
		return
	}
	roomID := portal.MXID
	ctx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()
	release, ok := rtcPermissionLocks.acquire(ctx, string(roomID))
	if !ok {
		return
	}
	defer release()
	currentRoom := func() bool { return portal.MXID == roomID }
	err = reconcileRTCPermission(ctx, currentRoom, func() (func() error, error) {
		// Recompute the policy for every attempt, including after an ambiguous
		// send. The current power levels are never reused across attempts.
		if ctx.Err() != nil || !currentRoom() {
			return nil, ctx.Err()
		}
		var current event.PowerLevelsEventContent
		if err := mx.Bot.StateEvent(ctx, roomID, event.StatePowerLevels, "", &current); err != nil {
			return nil, fmt.Errorf("read MatrixRTC power levels: %w", err)
		}
		power := current.Clone()
		// Observe the current alias-aware policy after the state read, not before
		// a potentially slow homeserver response.
		apply := wa.rtcPortalPowerForRoom(ctx, jid, true, false)
		if ctx.Err() != nil || !currentRoom() {
			return nil, ctx.Err()
		}
		if !apply(power) {
			return nil, nil
		}
		return func() error {
			_, err := portal.Bridge.Bot.SendState(ctx, roomID, event.StatePowerLevels, "", &event.Content{Parsed: power}, time.Time{})
			return err
		}, nil
	})
	if err != nil && ctx.Err() == nil {
		wa.UserLogin.Log.Warn().Err(err).Stringer("room_id", roomID).Msg("Failed to refresh MatrixRTC call permission")
	}
}

func (wa *WhatsAppClient) refreshMatrixRTCCallPermissionForJID(ctx context.Context, jid types.JID) {
	jid = jid.ToNonAD()
	if jid.IsEmpty() {
		return
	}
	for _, candidate := range wa.matrixRTCCallCandidateJIDs(ctx, jid) {
		portal, err := wa.Main.Bridge.GetExistingPortalByKey(ctx, wa.makeWAPortalKey(candidate))
		if err != nil {
			wa.UserLogin.Log.Debug().Err(err).Stringer("jid", candidate).
				Msg("Failed to find portal candidate for MatrixRTC call permission refresh")
			continue
		}
		if portal != nil && portal.MXID != "" {
			wa.refreshMatrixRTCCallPermission(ctx, portal)
		}
	}
}

// Only query capabilities for eligible encrypted call portals, and share the
// same active network probe limit as opportunistic chat-view refreshes.
func (wa *WhatsAppClient) refreshMatrixRTCPortalCapability(ctx context.Context, portal *bridgev2.Portal, jid types.JID) {
	if ctx.Err() != nil || !wa.Main.Config.ExperimentalWhatsAppVoIP || wa.Main.Config.RTCGatewaySocket == "" {
		return
	}
	mx, ok := portal.Bridge.Matrix.(*bridgev2matrix.Connector)
	if !ok || !mx.Config.Encryption.Allow {
		return
	}
	if wa.IsOwnJID(jid) {
		return
	}
	if a, _ := matrixRTCCallStaticJIDAvailability(jid); a == matrixRTCCallAvailabilityDisabled {
		return
	}
	room := portal.MXID
	lookupCtx, cancelLookup := context.WithTimeout(ctx, 5*time.Second)
	encrypted, err := mx.StateStore.IsEncrypted(lookupCtx, room)
	cancelLookup()
	if err != nil || !encrypted || ctx.Err() != nil || portal.MXID != room {
		return
	}
	aliasCtx, cancelAliases := context.WithTimeout(ctx, 250*time.Millisecond)
	keys := wa.matrixRTCCallAvailabilityKeys(aliasCtx, jid)
	cancelAliases()
	if _, ok := matrixRTCCallAvailabilityCache.get(keys, time.Now()); ok {
		return
	}
	if !rtcProbes.acquire(ctx) {
		return
	}
	defer rtcProbes.releaseSlot()
	if ctx.Err() != nil || portal.MXID != room || wa.Client == nil || !wa.Client.IsConnected() {
		return
	}
	queryCtx, cancelQuery := context.WithTimeout(ctx, 20*time.Second)
	defer cancelQuery()
	wa.matrixRTCCallAvailabilityForPeer(queryCtx, jid)
}

// Re-evaluate every existing 1:1 portal after WhatsApp reconnects and periodically. WhatsApp's
// own consumer clients refresh business calling metadata independently of a
// call attempt; doing the same here makes Element's call controls reflect the
// remote account state before the user presses Call.
func (wa *WhatsAppClient) refreshMatrixRTCExistingDMPortals(ctx context.Context) {
	if ctx.Err() != nil || wa.Client == nil || !wa.Client.IsConnected() {
		return
	}
	portals, err := wa.Main.Bridge.GetAllPortalsWithMXID(ctx)
	if err != nil {
		wa.UserLogin.Log.Warn().Err(err).Msg("Failed to enumerate portals for MatrixRTC callability refresh")
		return
	}
	refresh := newRTCPortalRefreshBatch(ctx, 4)
	defer refresh.wait()
	for _, portal := range portals {
		if ctx.Err() != nil {
			return
		}
		if portal == nil || portal.MXID == "" || portal.RoomType != database.RoomTypeDM || portal.OtherUserID == "" {
			continue
		}
		if portal.Receiver != "" && portal.Receiver != wa.UserLogin.ID {
			continue
		}
		jid, err := waid.ParsePortalID(portal.ID)
		if err != nil || !matrixRTCCallPortalServer(jid.Server) {
			continue
		}
		portal := portal
		if !refresh.submit(func() {
			// Refresh an expired observation directly in this bounded batch.
			// Do not rely on the opportunistic chat-view probe queue.
			wa.refreshMatrixRTCPortalCapability(ctx, portal, jid)
			if ctx.Err() == nil {
				wa.refreshMatrixRTCCallPermission(ctx, portal)
			}
		}) {
			return
		}
	}
}

// Restricted DM portals must not advertise call permission. Element derives
// call availability from these event power levels, so using PL100 makes the
// call controls unavailable while ordinary messaging remains unchanged.
func matrixRTCPowerLevels(restrictCalls bool, events map[event.Type]int) map[event.Type]int {
	if events == nil {
		events = make(map[event.Type]int)
	}
	level := 0
	if restrictCalls {
		level = 100
	}
	for _, eventType := range []event.Type{
		rtcEventType(rtcSlotStable, event.StateEventType),
		rtcEventType(rtcSlotWire, event.StateEventType),
		rtcEventType(rtcMemberStable, event.StateEventType),
		rtcEventType(rtcMemberWire, event.StateEventType),
		rtcEventType(rtcMemberLegacy, event.StateEventType),
		rtcEventType(rtcMemberLegacyAlias, event.StateEventType),
		rtcEventType(rtcInviteStable, event.MessageEventType),
		rtcEventType(rtcNotificationStable, event.MessageEventType),
		rtcEventType(rtcInviteWire, event.MessageEventType),
		rtcEventType(rtcDeclineStable, event.MessageEventType),
		rtcEventType(rtcDeclineWire, event.MessageEventType),
	} {
		events[eventType] = level
	}
	return events
}

func matrixRTCDMPowerLevelsForPeer(restrictCalls bool, events map[event.Type]int) map[event.Type]int {
	return matrixRTCPowerLevels(restrictCalls, events)
}

type rtcGroupUpdate struct {
	targets  []string
	deadline time.Time
}

type rtcOutgoingSession struct {
	linkOwner    *WhatsAppClient
	linkCall     *meowcaller.Call
	ctx          context.Context
	groupUpdates chan rtcGroupUpdate
	seenUpdates  map[string]bool
	device       id.DeviceID
	eventID      string
	sender       string
	cancel       context.CancelFunc
}

type rtcPendingInvite struct {
	sender string
	device id.DeviceID
	cancel context.CancelFunc
}

type rtcMemberRecord struct {
	eventID  string
	sticky   bool
	memberID string
	joinTS   uint64
	deviceID string
	format   string
	slotID   string
	sender   string
	expires  time.Time
}

type rtcLegacyMemberContent struct {
	DeviceID     string  `json:"device_id"`
	Application  string  `json:"application"`
	CallID       string  `json:"call_id"`
	MembershipID string  `json:"membershipID"`
	CreatedTS    *uint64 `json:"created_ts"`
	Intent       string  `json:"m.call.intent"`
}

type rtcMemberContent struct {
	SlotID string `json:"slot_id"`
	Member struct {
		ID         string `json:"id"`
		Membership string `json:"membership"`
	} `json:"member"`
	Application struct {
		Type   string `json:"type"`
		Intent string `json:"m.call.intent"`
	} `json:"application"`
}

type rtcInviteContent struct {
	GroupMode        string `json:"fi.mau.whatsapp.group_call_mode"`
	SlotID           string `json:"slot_id"`
	SenderTS         int64  `json:"sender_ts"`
	Lifetime         int64  `json:"lifetime"`
	StickyKey        string `json:"msc4354_sticky_key"`
	Intent           string `json:"m.call.intent"`
	NotificationType string `json:"notification_type"`
	Mentions         struct {
		UserIDs []string `json:"user_ids"`
		Room    bool     `json:"room"`
	} `json:"m.mentions"`
	Application struct {
		Type             string `json:"type"`
		NotificationType string `json:"notification_type"`
		Intent           string `json:"m.call.intent"`
		SenderTS         int64  `json:"sender_ts"`
		Lifetime         int64  `json:"lifetime"`
	} `json:"application"`
	RelatesTo struct {
		RelType string `json:"rel_type"`
		EventID string `json:"event_id"`
	} `json:"m.relates_to"`
}

// RTC events are custom names. mautrix.NewEventType guesses UnknownEventType,
// but transactions deliver timeline events with MessageEventType and state
// membership with StateEventType. The class is part of EventProcessor's key.
func rtcEventType(name string, class event.TypeClass) event.Type {
	return event.Type{Type: name, Class: class}
}

func canonicalMatrixRTCSlotID(slotID string) string {
	// Element X / Element Call compatibility signalling can use the historical
	// room-wide sentinel m.call#ROOM. The MatrixRTC SDK requires the
	// application slot component to be a lowercase Common Namespaced
	// Identifier, whose canonical room-wide spelling is m.call#room.
	// Canonicalize only this known compatibility sentinel instead of blindly
	// lowercasing arbitrary application-defined slot IDs.
	if slotID == "m.call#ROOM" {
		return matrixRTCSlotDefault
	}
	return slotID
}

func rtcSessionKey(roomID, slotID string) string {
	return roomID + "\x00" + canonicalMatrixRTCSlotID(slotID)
}

func rtcMemberEventKey(roomID, eventID string) string {
	return roomID + "\x00" + eventID
}

func (wa *WhatsAppConnector) initMatrixRTCEvents() {
	mx, ok := wa.Bridge.Matrix.(*bridgev2matrix.Connector)
	if !ok || mx.EventProcessor == nil {
		wa.Bridge.Log.Warn().Msg("MatrixRTC gateway enabled, but the Matrix connector does not expose an event processor")
		return
	}
	wa.rtcMu.Lock()
	if wa.rtcOutgoing == nil {
		wa.rtcOutgoing = make(map[string]rtcOutgoingSession)
	}
	if wa.rtcMembers == nil {
		wa.rtcMembers = make(map[string]rtcMemberRecord)
	}
	wa.rtcMu.Unlock()

	for _, name := range []string{rtcMemberStable, rtcMemberWire} {
		mx.EventProcessor.On(rtcEventType(name, event.MessageEventType), wa.handleMatrixRTCMember)
		mx.EventProcessor.On(rtcEventType(name, event.StateEventType), wa.handleMatrixRTCMember)
	}
	for _, name := range []string{rtcMemberLegacy, rtcMemberLegacyAlias} {
		mx.EventProcessor.On(rtcEventType(name, event.StateEventType), wa.handleMatrixRTCLegacyMember)
	}
	for _, name := range []string{rtcInviteStable, rtcNotificationStable, rtcInviteWire} {
		mx.EventProcessor.On(rtcEventType(name, event.MessageEventType), wa.handleMatrixRTCInvite)
	}
	// A decline from our own ghost is consumed by the MatrixRTC worker. These
	// handlers are registered only to make an explicit user-side decline or a
	// future stable spelling cancel any matching pending outbound gateway job.
	for _, name := range []string{rtcDeclineStable, rtcDeclineWire} {
		mx.EventProcessor.On(rtcEventType(name, event.MessageEventType), wa.handleMatrixRTCDecline)
	}
	wa.Bridge.Log.Info().Msg("MatrixRTC appservice event gateway registered")
}

func (wa *WhatsAppConnector) handleMatrixRTCMember(ctx context.Context, evt *event.Event) {
	if evt == nil || evt.RoomID == "" || evt.Sender == "" {
		return
	}
	var content rtcMemberContent
	if err := json.Unmarshal(evt.Content.VeryRaw, &content); err != nil || content.SlotID == "" {
		return
	}
	content.SlotID = canonicalMatrixRTCSlotID(content.SlotID)
	wa.rtcMu.Lock()
	defer wa.rtcMu.Unlock()
	wa.pruneRTCRecords(time.Now())
	isJoin := content.Member.Membership == "join" || (content.Member.Membership == "" && content.Member.ID != "")
	if !isJoin || content.Application.Type != "m.call" {
		for k, v := range wa.rtcMembers {
			if rtcWithdrawalMatches(k, v, evt.RoomID.String(), content.SlotID, evt.Sender.String(), string(rtcTrustedDevice(evt)), content.Member.ID, true, 0) {
				delete(wa.rtcMembers, k)
			}
		}
		return
	}
	expires := time.Now().Add(rtcMemberLifetime)
	if evt.ID != "" {
		format := "msc"
		if content.Member.Membership == "" {
			format = "room_state"
		}
		wa.rtcMembers[rtcMemberEventKey(evt.RoomID.String(), evt.ID.String())] = rtcMemberRecord{
			sticky: true, memberID: content.Member.ID, deviceID: string(rtcTrustedDevice(evt)), format: format,
			slotID:  content.SlotID,
			sender:  evt.Sender.String(),
			expires: expires,
		}
	}
}

// An inline notification may use the canonical slot even when the sending
// device published an older Element Call membership. Only a sticky event
// authenticated to this device can select its dialect.
func (wa *WhatsAppConnector) inlineRTCMembership(room, slot, sender, device string) rtcMemberRecord {
	wa.rtcMu.Lock()
	defer wa.rtcMu.Unlock()
	wa.pruneRTCRecords(time.Now())
	var latest rtcMemberRecord
	for key, record := range wa.rtcMembers {
		if !record.sticky || device == "" || record.deviceID != device ||
			record.sender != sender || record.slotID != slot || !strings.HasPrefix(key, room+"\x00") {
			continue
		}
		if record.expires.After(latest.expires) || (record.expires.Equal(latest.expires) && record.format == "msc") {
			latest = record
			latest.eventID = strings.TrimPrefix(key, room+"\x00")
		}
	}
	return latest
}

func legacyMatrixRTCSlot(application, callID string) string {
	if application == "" {
		return ""
	}
	if callID == "" {
		return application + "#room"
	}
	return canonicalMatrixRTCSlotID(application + "#" + callID)
}

// Match the worker's legacy state translator: a refresh keeps the original
// created_ts, clamped to the server timestamp. Without created_ts a replacement
// event cannot prove that it is a refresh of the same generation.
func legacyRTCJoinTimestamp(serverTS int64, createdTS *uint64) uint64 {
	if serverTS <= 0 || createdTS == nil || *createdTS == 0 {
		return 0
	}
	return min(uint64(serverTS), *createdTS)
}

func (wa *WhatsAppConnector) handleMatrixRTCLegacyMember(ctx context.Context, evt *event.Event) {
	if evt == nil || evt.RoomID == "" || evt.Sender == "" || evt.ID == "" {
		return
	}
	var content rtcLegacyMemberContent
	if err := json.Unmarshal(evt.Content.VeryRaw, &content); err != nil {
		return
	}
	// Empty legacy state content is a leave. The invite withdrawal is the
	// primary cancellation signal, but when we still have the previous state
	// content Matrix exposes it in unsigned.prev_content, so use that as a
	// prompt cancellation path too.
	if content.Application == "" && evt.Unsigned.PrevContent != nil {
		if err := json.Unmarshal(evt.Unsigned.PrevContent.VeryRaw, &content); err != nil {
			return
		}
		if content.MembershipID == "" && content.DeviceID != "" {
			content.MembershipID = evt.Sender.String() + ":" + content.DeviceID
		}
		wa.rtcMu.Lock()
		for k, v := range wa.rtcMembers {
			if rtcWithdrawalMatches(k, v, evt.RoomID.String(), legacyMatrixRTCSlot(content.Application, content.CallID), evt.Sender.String(), content.DeviceID, content.MembershipID, false, legacyRTCJoinTimestamp(evt.Timestamp, content.CreatedTS)) {
				delete(wa.rtcMembers, k)
			}
		}
		wa.rtcMu.Unlock()
		return
	}
	if content.Application != "m.call" || content.DeviceID == "" {
		return
	}
	if content.MembershipID == "" {
		// The legacy SFU identity is user:device. Still require an encrypted
		// matching notification and the worker's independent roster check.
		content.MembershipID = evt.Sender.String() + ":" + content.DeviceID
	}
	slotID := legacyMatrixRTCSlot(content.Application, content.CallID)
	if slotID == "" {
		return
	}
	expires := time.Now().Add(rtcMemberLifetime)
	record := rtcMemberRecord{memberID: content.MembershipID, joinTS: legacyRTCJoinTimestamp(evt.Timestamp, content.CreatedTS), deviceID: content.DeviceID, format: "room_state", slotID: slotID, sender: evt.Sender.String(), expires: expires}
	wa.rtcMu.Lock()
	wa.pruneRTCRecords(time.Now())
	wa.rtcMembers[rtcMemberEventKey(evt.RoomID.String(), evt.ID.String())] = record
	wa.rtcMu.Unlock()
}

// An ambiguous withdrawal is not evidence of a specific cached join ending.
// The worker separately verifies the current roster before dialing.
func rtcWithdrawalMatches(key string, record rtcMemberRecord, room, slot, sender, device, member string, sticky bool, join uint64) bool {
	return device != "" && member != "" && slot != "" && strings.HasPrefix(key, room+"\x00") && record.slotID == slot && record.sender == sender && record.deviceID == device && record.memberID == member && record.sticky == sticky && (sticky || (join != 0 && record.joinTS == join))
}

// Called with rtcMu held. Event IDs are generally looked up only once.
func (wa *WhatsAppConnector) pruneRTCRecords(now time.Time) {
	for k, v := range wa.rtcMembers {
		if !v.expires.After(now) {
			delete(wa.rtcMembers, k)
		}
	}
}

func rtcTrustedDevice(evt *event.Event) id.DeviceID {
	d := evt.Mautrix.TrustSource
	if !evt.Mautrix.WasEncrypted || evt.Mautrix.ForwardedKeys || d == nil || d.Deleted || d.UserID != evt.Sender || evt.Mautrix.TrustState < id.TrustStateCrossSignedTOFU {
		return ""
	}
	return d.DeviceID
}

func (wa *WhatsAppConnector) matrixRTCMemberForNotification(roomID, eventID, sender string) (rtcMemberRecord, bool) {
	if eventID == "" {
		return rtcMemberRecord{}, false
	}
	key := rtcMemberEventKey(roomID, eventID)
	wa.rtcMu.Lock()
	defer wa.rtcMu.Unlock()
	record, ok := wa.rtcMembers[key]
	if !ok {
		return rtcMemberRecord{}, false
	}
	if time.Now().After(record.expires) {
		delete(wa.rtcMembers, key)
		return rtcMemberRecord{}, false
	}
	if record.sender != sender {
		return rtcMemberRecord{}, false
	}
	return record, true
}

// The encrypted invitation may explicitly choose a mode. Otherwise the worker
// resolves the current authenticated membership; historical cache entries are
// never used to infer media intent.
func rtcInvitationIntent(top, nested string) (string, bool) {
	valid := func(s string) bool { return s == "audio" || s == "video" }
	if valid(top) && valid(nested) && top != nested {
		return "", false
	}
	if valid(top) {
		return top, true
	}
	if valid(nested) {
		return nested, true
	}
	return "auto", true
}

func isMatrixRTCBridgeBotControlMembership(joined []id.UserID, bot, sender id.UserID, isGhost func(id.UserID) bool) bool {
	if bot == "" || sender == "" || bot.Homeserver() != sender.Homeserver() || sender == bot || isGhost(sender) || len(joined) != 2 {
		return false
	}
	seenBot := false
	seenSender := false
	for _, userID := range joined {
		switch userID {
		case bot:
			seenBot = true
		case sender:
			seenSender = true
		default:
			return false
		}
	}
	return seenBot && seenSender
}

func (wa *WhatsAppConnector) rejectMatrixRTCBridgeBotControlCall(ctx context.Context, evt *event.Event) bool {
	mx, ok := wa.Bridge.Matrix.(*bridgev2matrix.Connector)
	if !ok || mx.Bot == nil || evt == nil || evt.RoomID == "" || evt.Sender == "" || evt.ID == "" {
		return false
	}
	members, err := mx.Bot.JoinedMembers(ctx, evt.RoomID)
	if err != nil {
		wa.Bridge.Log.Debug().Err(err).Stringer("room_id", evt.RoomID).Msg("Failed to inspect possible WhatsApp bridge-bot control room")
		return false
	}
	joined := make([]id.UserID, 0, len(members.Joined))
	for userID := range members.Joined {
		joined = append(joined, userID)
	}
	botMXID := wa.Bridge.Bot.GetMXID()
	if !isMatrixRTCBridgeBotControlMembership(joined, botMXID, evt.Sender, wa.Bridge.IsGhostMXID) {
		return false
	}
	// A small WhatsApp group can have the same joined roster as a bot DM.
	if portal, err := wa.Bridge.DB.Portal.GetByMXID(ctx, evt.RoomID); err != nil || portal != nil {
		return false
	}
	user, err := wa.Bridge.GetExistingUserByMXID(ctx, evt.Sender)
	if err != nil {
		return false
	}
	direct := user != nil && user.ManagementRoom == evt.RoomID
	if !direct && user != nil {
		if puppet, ok := user.DoublePuppet(ctx).(*bridgev2matrix.ASIntent); ok {
			var rooms map[id.UserID][]id.RoomID
			if puppet.Matrix().GetAccountData(ctx, "m.direct", &rooms) == nil {
				for _, room := range rooms[botMXID] {
					if room == evt.RoomID {
						direct = true
						break
					}
				}
			}
		}
	}
	if !direct {
		for _, member := range []id.UserID{evt.Sender, botMXID} {
			var content event.MemberEventContent
			if mx.Bot.StateEvent(ctx, evt.RoomID, event.StateMember, member.String(), &content) == nil && content.IsDirect {
				direct = true
				break
			}
		}
	}
	if !direct {
		var invite rtcInviteContent
		if json.Unmarshal(evt.Content.VeryRaw, &invite) == nil && invite.NotificationType != "notification" && invite.Application.NotificationType != "notification" && !invite.Mentions.Room {
			for _, target := range invite.Mentions.UserIDs {
				if target == botMXID.String() {
					direct = true
					break
				}
			}
		}
	}
	if !direct {
		return false
	}
	content := map[string]any{"m.relates_to": map[string]any{"rel_type": "m.reference", "event_id": evt.ID.String()}}
	tx := fmt.Sprintf("wa-rtc-decline-%x", sha256.Sum256([]byte(evt.RoomID.String()+"\x00"+evt.ID.String())))
	for attempt := 0; attempt < 2; attempt++ {
		_, err = wa.Bridge.Bot.SendMessage(ctx, evt.RoomID, rtcEventType(rtcDeclineStable, event.MessageEventType), &event.Content{Raw: content}, &bridgev2.MatrixSendExtra{TransactionID: tx})
		if err == nil || ctx.Err() != nil {
			break
		}
	}
	if err != nil {
		wa.Bridge.Log.Warn().Err(err).Stringer("room_id", evt.RoomID).Stringer("event_id", evt.ID).Msg("Failed to decline MatrixRTC call to WhatsApp bridge bot")
	} else {
		wa.Bridge.Log.Info().Stringer("room_id", evt.RoomID).Stringer("event_id", evt.ID).Msg("Declined MatrixRTC call to WhatsApp bridge bot")
	}
	return true
}

func mentionsGhost(content *rtcInviteContent, ghostMXID string) bool {
	if content.Mentions.Room {
		return true
	}
	for _, userID := range content.Mentions.UserIDs {
		if userID == ghostMXID {
			return true
		}
	}
	// Legacy room-state-format notifications may not contain m.mentions.
	return len(content.Mentions.UserIDs) == 0 && (content.NotificationType != "" || content.Application.NotificationType != "")
}

// Resolve the two MSC4075 spellings without letting a conflicting copy turn a
// visual-only notification into an audible ring. Empty values retain the
// existing compatibility path for an invite without a notification_type.
func rtcNotificationPolicy(content *rtcInviteContent) (string, bool) {
	top, nested := content.NotificationType, content.Application.NotificationType
	for _, value := range []string{top, nested} {
		if value != "" && value != "ring" && value != "notification" {
			return "", false
		}
	}
	if top != "" && nested != "" && top != nested {
		return "", false
	}
	if top != "" {
		return top, true
	}
	return nested, true
}

// A reference to an active invitation is not a membership reference. Handle
// it exclusively, including refusals, so a rejected follow-up cannot start a
// second session. Caller holds rtcMu for the current room/slot generation.
func routeRTCGroupFollowup(current rtcOutgoingSession, content rtcInviteContent, notificationType, eventID, sender string, device id.DeviceID, targets []string, deadline, now time.Time) bool {
	if current.eventID == "" || content.RelatesTo.RelType != "m.reference" || content.RelatesTo.EventID != current.eventID {
		return false
	}
	if content.GroupMode != "" || (current.ctx != nil && current.ctx.Err() != nil) || current.groupUpdates == nil || current.seenUpdates == nil || eventID == "" || eventID == current.eventID || current.sender != sender || device == "" || current.device != device ||
		(content.SlotID != "" && content.SlotID != matrixRTCSlotDefault) || (content.StickyKey != "" && content.StickyKey != matrixRTCSlotDefault) ||
		(notificationType != "" && notificationType != "notification") || content.Mentions.Room || len(targets) == 0 || len(targets) > 31 ||
		len(current.seenUpdates) >= 128 || current.seenUpdates[eventID] || !now.Before(deadline) {
		return true
	}
	select {
	case current.groupUpdates <- rtcGroupUpdate{targets: targets, deadline: deadline}:
		current.seenUpdates[eventID] = true
	default:
	}
	return true
}

func (wa *WhatsAppConnector) routeActiveRTCGroupFollowup(ctx context.Context, evt *event.Event, content rtcInviteContent, notificationType string, device id.DeviceID, deadline time.Time) bool {
	if content.RelatesTo.RelType != "m.reference" || content.RelatesTo.EventID == "" {
		return false
	}
	var targets []string
	if !content.Mentions.Room && len(content.Mentions.UserIDs) <= 31 {
		seen := make(map[string]bool)
		for _, mxid := range content.Mentions.UserIDs {
			if user, isGhost := wa.Bridge.Matrix.ParseGhostMXID(id.UserID(mxid)); isGhost {
				jid := waid.ParseUserID(user).ToNonAD()
				if !jid.IsEmpty() && !seen[jid.String()] {
					targets = append(targets, jid.String())
					seen[jid.String()] = true
				}
			}
		}
	}
	wa.rtcMu.Lock()
	defer wa.rtcMu.Unlock()
	if ctx.Err() != nil {
		return true
	}
	current := wa.rtcOutgoing[rtcSessionKey(evt.RoomID.String(), matrixRTCSlotDefault)]
	return routeRTCGroupFollowup(current, content, notificationType, evt.ID.String(), evt.Sender.String(), device, targets, deadline, time.Now())
}

func (wa *WhatsAppConnector) handleMatrixRTCInvite(ctx context.Context, evt *event.Event) {
	if evt == nil || evt.RoomID == "" || evt.Sender == "" || evt.ID == "" {
		return
	}
	var content rtcInviteContent
	if err := json.Unmarshal(evt.Content.VeryRaw, &content); err != nil {
		return
	}
	if content.GroupMode != "" && content.GroupMode != "ad_hoc" && content.GroupMode != "call_link" {
		return
	}
	notificationType, validNotification := rtcNotificationPolicy(&content)
	if !validNotification {
		return
	}
	lifetime := content.Lifetime
	if lifetime <= 0 {
		lifetime = content.Application.Lifetime
	}
	if lifetime <= 0 {
		lifetime = 90_000
	}
	if lifetime > rtcMaxLifetimeMS {
		lifetime = rtcMaxLifetimeMS
	}
	senderTS := content.SenderTS
	if senderTS <= 0 {
		senderTS = content.Application.SenderTS
	}
	if senderTS <= 0 {
		senderTS = evt.Timestamp
	}
	now := time.Now()
	sent := time.UnixMilli(senderTS)
	if sent.After(now.Add(rtcClockSkew)) || now.After(sent.Add(time.Duration(lifetime)*time.Millisecond)) {
		return
	}

	originalSlot := content.SlotID
	content.SlotID = canonicalMatrixRTCSlotID(content.SlotID)
	content.StickyKey = canonicalMatrixRTCSlotID(content.StickyKey)
	// A bare sticky withdrawal has no generation. The observed RTC membership
	// handles its departure, including during the pre-dial/ringing phase.
	if content.SlotID == "" && content.StickyKey != "" {
		return
	}
	device := rtcTrustedDevice(evt)
	ringDeadline := sent.Add(time.Duration(lifetime) * time.Millisecond)
	// Visual-only notices cannot trigger a control-room decline. Preserve the
	// existing control-room handling for rings even when no trusted device is
	// available, but never admit an untrusted outgoing call.
	if device == "" && notificationType == "notification" {
		return
	}
	// All network/DB operations run off the appservice event processor. Register
	// pending ownership synchronously so a subsequent decline can cancel it.
	snapshot := *evt
	if evt.Mautrix.TrustSource != nil {
		trust := *evt.Mautrix.TrustSource
		snapshot.Mautrix.TrustSource = &trust
	}
	snapshot.Content.VeryRaw = append(json.RawMessage(nil), evt.Content.VeryRaw...)
	wa.queueRTCInvite(&snapshot, device, ringDeadline, func(pendingCtx context.Context) {
		if pendingCtx.Err() != nil {
			return
		}
		if notificationType != "notification" && wa.rejectMatrixRTCBridgeBotControlCall(pendingCtx, &snapshot) {
			return
		}
		if device == "" || pendingCtx.Err() != nil {
			return
		}
		if wa.routeActiveRTCGroupFollowup(pendingCtx, &snapshot, content, notificationType, device, ringDeadline) {
			return
		}
		if content.SlotID == "" || ((snapshot.Type.Type == rtcInviteWire || snapshot.Type.Type == rtcNotificationStable) && content.RelatesTo.RelType == "m.reference" && content.RelatesTo.EventID != "") {
			if content.RelatesTo.RelType != "m.reference" || content.RelatesTo.EventID == "" {
				return
			}
			wait, stop := context.WithTimeout(pendingCtx, rtcReferenceWait)
			record, ok := waitRTCReferencedMember(wait, 25*time.Millisecond, string(device), func() (rtcMemberRecord, bool) {
				return wa.matrixRTCMemberForNotification(snapshot.RoomID.String(), content.RelatesTo.EventID, snapshot.Sender.String())
			})
			stop()
			if ok && pendingCtx.Err() == nil {
				wa.completeMatrixRTCInvite(pendingCtx, &snapshot, content, notificationType, device, ringDeadline, originalSlot, &record)
			}
			return
		}
		wa.completeMatrixRTCInvite(pendingCtx, &snapshot, content, notificationType, device, ringDeadline, originalSlot, nil)
	})
}

// waitRTCReferencedMember returns only a matching, still-live membership. A
// mismatching device is a terminal refusal rather than a reason to retry.
func waitRTCReferencedMember(ctx context.Context, interval time.Duration, device string, lookup func() (rtcMemberRecord, bool)) (rtcMemberRecord, bool) {
	tick := time.NewTicker(interval)
	defer tick.Stop()
	for {
		if ctx.Err() != nil {
			return rtcMemberRecord{}, false
		}
		if record, ok := lookup(); ok {
			if ctx.Err() != nil || (record.deviceID != "" && record.deviceID != device) {
				return rtcMemberRecord{}, false
			}
			return record, true
		}
		select {
		case <-ctx.Done():
			return rtcMemberRecord{}, false
		case <-tick.C:
		}
	}
}

func (wa *WhatsAppConnector) queueRTCInvite(evt *event.Event, device id.DeviceID, deadline time.Time, resume func(context.Context)) {
	key := rtcMemberEventKey(evt.RoomID.String(), evt.ID.String())
	ctx, cancel := context.WithDeadline(wa.Bridge.BackgroundCtx, deadline)
	wa.rtcMu.Lock()
	if wa.rtcPendingInvites == nil {
		wa.rtcPendingInvites = make(map[string]rtcPendingInvite)
	}
	if _, exists := wa.rtcPendingInvites[key]; exists || len(wa.rtcPendingInvites) >= rtcPendingInviteLimit || ctx.Err() != nil {
		wa.rtcMu.Unlock()
		cancel()
		return
	}
	wa.rtcPendingInvites[key] = rtcPendingInvite{sender: evt.Sender.String(), device: device, cancel: cancel}
	wa.rtcMu.Unlock()
	go func() {
		defer cancel()
		defer func() {
			wa.rtcMu.Lock()
			delete(wa.rtcPendingInvites, key)
			wa.rtcMu.Unlock()
		}()
		if ctx.Err() == nil {
			resume(ctx)
		}
	}()
}

func (wa *WhatsAppConnector) completeMatrixRTCInvite(ctx context.Context, evt *event.Event, content rtcInviteContent, notificationType string, device id.DeviceID, ringDeadline time.Time, originalSlot string, record *rtcMemberRecord) {
	// The worker independently refreshes the actual device keys. A reused device
	// ID must not authorize a different cryptographic identity.
	trust := evt.Mautrix.TrustSource
	if rtcTrustedDevice(evt) != device || device == "" || trust == nil || !rtcwire.ValidDeviceKey(string(trust.SigningKey)) || !rtcwire.ValidDeviceKey(string(trust.IdentityKey)) {
		return
	}
	signingKey, identityKey := string(trust.SigningKey), string(trust.IdentityKey)
	membershipFormat := "msc"
	memberEvent := ""
	memberJoinID := ""
	var memberJoinTS uint64
	if record != nil {
		memberEvent = content.RelatesTo.EventID
		// The referenced event was independently observed in this room, and
		// the encrypted invitation must bind its sender and device. Legacy
		// member IDs alone are reusable: require the original join timestamp.
		if record.sender == evt.Sender.String() && record.deviceID == string(device) && device != "" {
			if record.format == "msc" && record.sticky {
				memberJoinID = record.memberID
			} else if record.format == "room_state" && record.memberID != "" && record.joinTS != 0 {
				memberJoinID, memberJoinTS = record.memberID, record.joinTS
			}
		}
		membershipFormat = record.format
		if membershipFormat == "" {
			membershipFormat = "room_state"
		}
		relatedSlot := canonicalMatrixRTCSlotID(record.slotID)
		if content.SlotID != "" && content.SlotID != relatedSlot {
			return
		}
		content.SlotID = relatedSlot
	}
	if content.SlotID != matrixRTCSlotDefault || (content.StickyKey != "" && content.StickyKey != content.SlotID) {
		return
	}
	if content.GroupMode == "call_link" {
		wa.completeRTCCallLinkInvite(ctx, evt, content, notificationType, device, ringDeadline, membershipFormat, memberEvent, memberJoinID, memberJoinTS)
		return
	}
	portal, err := wa.Bridge.GetPortalByMXID(ctx, evt.RoomID)
	if err != nil || portal == nil {
		return
	}
	peer, err := waid.ParsePortalID(portal.ID)
	if err != nil || peer.IsEmpty() {
		return
	}
	isGroup := portal.RoomType == database.RoomTypeDefault && peer.Server == types.GroupServer
	if !isGroup && (portal.RoomType != database.RoomTypeDM || portal.OtherUserID == "") {
		return
	}
	if isGroup {
		if notificationType != "" && notificationType != "notification" {
			return
		}
	} else if notificationType != "" && notificationType != "ring" {
		return
	}
	user, err := wa.Bridge.GetUserByMXID(ctx, evt.Sender)
	if err != nil || user == nil {
		return
	}
	login, _, err := portal.FindPreferredLogin(ctx, user, false)
	if err != nil || login == nil || login.Client == nil {
		return
	}
	client, ok := login.Client.(*WhatsAppClient)
	if login.UserMXID != evt.Sender || !ok || client.voipClient == nil {
		return
	}
	ghostMXID := wa.Bridge.Bot.GetMXID().String()
	if !isGroup {
		ghostMXID = wa.Bridge.Matrix.FormatGhostMXID(portal.OtherUserID).String()
	}
	// The appservice event processor also sees RTC signalling emitted by the
	// WhatsApp ghost itself. Never feed those events back into WhatsApp or an
	// incoming WhatsApp -> Matrix ring could recursively start an outgoing call.
	var targets []string
	adHoc := content.GroupMode == "ad_hoc"
	if adHoc {
		var valid bool
		targets, valid = rtcAdHocTargets(isGroup, content.Mentions.Room, content.Mentions.UserIDs, func(mxid string) (string, bool) {
			ghost, ok := wa.Bridge.Matrix.ParseGhostMXID(id.UserID(mxid))
			if !ok {
				return "", false
			}
			jid := waid.ParseUserID(ghost).ToNonAD()
			return jid.String(), !jid.IsEmpty() && (jid.Server == types.DefaultUserServer || jid.Server == types.HiddenUserServer) && !client.IsOwnJID(jid)
		})
		if !valid {
			return
		}
	} else if isGroup && !content.Mentions.Room {
		for _, mxid := range content.Mentions.UserIDs {
			if user, isGhost := wa.Bridge.Matrix.ParseGhostMXID(id.UserID(mxid)); isGhost {
				targets = append(targets, waid.ParseUserID(user).ToNonAD().String())
			}
		}
		if len(targets) > 31 {
			return
		}
	}
	if evt.Sender.String() == ghostMXID || (!mentionsGhost(&content, ghostMXID) && len(targets) == 0) {
		return
	}

	intent, validIntent := rtcInvitationIntent(content.Intent, content.Application.Intent)
	if !validIntent {
		return
	}

	// The invitation deadline applies only until WhatsApp answers. An active
	// session must outlive the original ring deadline.
	callCtx, cancel, admitted := client.bindVoIPConnection(wa.Bridge.BackgroundCtx)
	if !admitted {
		return
	}
	key := rtcSessionKey(evt.RoomID.String(), content.SlotID)
	wa.rtcMu.Lock()
	// Declines of pending invites cancel under this same lock, so
	// a cancelled continuation cannot cross the outgoing admission boundary.
	if ctx.Err() != nil || callCtx.Err() != nil || !time.Now().Before(ringDeadline) {
		wa.rtcMu.Unlock()
		cancel()
		return
	}
	if current, exists := wa.rtcOutgoing[key]; exists && (current.ctx == nil || current.ctx.Err() == nil || current.eventID == evt.ID.String()) {
		// Follow-ups are routed before membership resolution; this path must
		// never replace or mutate an already-owned session.
		wa.rtcMu.Unlock()
		cancel()
		return
	}
	var groupUpdates chan rtcGroupUpdate
	if isGroup && !adHoc {
		groupUpdates = make(chan rtcGroupUpdate, 4)
	}
	wa.rtcOutgoing[key] = rtcOutgoingSession{ctx: callCtx, groupUpdates: groupUpdates, seenUpdates: make(map[string]bool), eventID: evt.ID.String(), sender: evt.Sender.String(), device: device, cancel: cancel}
	wa.rtcMu.Unlock()

	go func() {
		defer cancel()
		defer wa.finishMatrixRTCOutgoing(key, evt.ID.String())
		if callCtx.Err() != nil {
			return
		}
		callID := evt.ID.String()
		roomID := evt.RoomID.String()
		if originalSlot != "" && memberEvent == "" {
			// Matrix can deliver the invitation ahead of the membership. A
			// slot spelling alone never downgrades the admission rules.
			wait, stop := context.WithTimeout(callCtx, 1500*time.Millisecond)
			tick := time.NewTicker(100 * time.Millisecond)
		waitForMember:
			for {
				member := wa.inlineRTCMembership(roomID, content.SlotID, evt.Sender.String(), string(device))
				if member.format != "" {
					membershipFormat = member.format
					if member.format == "room_state" {
						memberEvent = member.eventID
					}
					break
				}
				select {
				case <-tick.C:
				case <-wait.Done():
					break waitForMember
				}
			}
			tick.Stop()
			stop()
			if callCtx.Err() != nil {
				return
			}
		}
		groupJID := ""
		if isGroup {
			groupJID = peer.ToNonAD().String()
		}
		if err := client.runOutgoingRTCGateway(
			callCtx, callID, roomID, content.SlotID, membershipFormat, ghostMXID, evt.Sender.String(), string(device), signingKey, identityKey, memberEvent, memberJoinID, memberJoinTS, peer.ToNonAD().String(), isGroup, groupJID, intent, ringDeadline, targets, groupUpdates, adHoc, "",
		); err != nil && callCtx.Err() == nil {
			client.UserLogin.Log.Warn().Err(err).
				Str("matrix_event_id", callID).
				Str("room_id", roomID).
				Msg("Outgoing MatrixRTC to WhatsApp call failed")
		}
	}()
}

func (wa *WhatsAppConnector) finishMatrixRTCOutgoing(key, eventID string) {
	wa.rtcMu.Lock()
	defer wa.rtcMu.Unlock()
	if current, ok := wa.rtcOutgoing[key]; ok && current.eventID == eventID {
		delete(wa.rtcOutgoing, key)
	}
}

func (wa *WhatsAppConnector) cancelMatrixRTCOutgoing(roomID, slotID, sender string) {
	key := rtcSessionKey(roomID, slotID)
	wa.rtcMu.Lock()
	current, ok := wa.rtcOutgoing[key]
	if ok && current.sender == sender {
		delete(wa.rtcOutgoing, key)
	} else {
		ok = false
	}
	wa.rtcMu.Unlock()
	if ok {
		current.cancel()
	}
}

func (wa *WhatsAppConnector) handleMatrixRTCDecline(ctx context.Context, evt *event.Event) {
	if evt == nil || evt.RoomID == "" || evt.Sender == "" {
		return
	}
	raw := evt.Content.GetRaw()
	relatesTo, _ := raw["m.relates_to"].(map[string]any)
	referenced, _ := relatesTo["event_id"].(string)
	if referenced == "" || relatesTo["rel_type"] != "m.reference" {
		return
	}
	wa.rtcMu.Lock()
	var cancel context.CancelFunc
	device := rtcTrustedDevice(evt)
	if pending, ok := wa.rtcPendingInvites[rtcMemberEventKey(evt.RoomID.String(), referenced)]; ok && pending.sender == evt.Sender.String() && device != "" && pending.device == device {
		pending.cancel()
	}
	for key, current := range wa.rtcOutgoing {
		if strings.HasPrefix(key, evt.RoomID.String()+"\x00") && current.eventID == referenced && current.sender == evt.Sender.String() && rtcTrustedDevice(evt) == current.device {
			cancel = current.cancel
			delete(wa.rtcOutgoing, key)
			break
		}
	}
	wa.rtcMu.Unlock()
	if cancel != nil {
		cancel()
	}
}
