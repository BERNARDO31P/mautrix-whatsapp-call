package connector

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"

	"go.mau.fi/whatsmeow"
	waBinary "go.mau.fi/whatsmeow/binary"
	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/database"
	bridgev2matrix "maunium.net/go/mautrix/bridgev2/matrix"
	"maunium.net/go/mautrix/event"
	"maunium.net/go/mautrix/id"

	"go.mau.fi/whatsmeow/types"

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
	rtcIntentWait         = 750 * time.Millisecond
	rtcIntentLifetime     = 5 * time.Minute
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

func classifyMatrixRTCCallDevices(devices []types.JID) (matrixRTCCallDeviceClass, string) {
	if len(devices) == 0 {
		return matrixRTCCallDeviceClassUnavailable, "devices:none"
	}
	hosted := 0
	for _, device := range devices {
		switch device.Server {
		case types.HostedServer, types.HostedLIDServer:
			hosted++
		default:
			return matrixRTCCallDeviceClassConsumer, "devices:consumer"
		}
	}
	if hosted == len(devices) {
		return matrixRTCCallDeviceClassHosted, "devices:hosted"
	}
	return matrixRTCCallDeviceClassUnknown, "devices:unknown"
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

func flattenMatrixRTCCallOptions(prefix string, value any, out map[string]string) {
	switch typed := value.(type) {
	case map[string]any:
		for key, child := range typed {
			normalizedKey := normalizeMatrixRTCCallOption(key)
			if prefix != "" {
				normalizedKey = prefix + "_" + normalizedKey
			}
			flattenMatrixRTCCallOptions(normalizedKey, child, out)
		}
	case string:
		out[prefix] = typed
	case bool:
		if typed {
			out[prefix] = "true"
		} else {
			out[prefix] = "false"
		}
	case float64:
		if typed == 0 {
			out[prefix] = "0"
		} else if typed == 1 {
			out[prefix] = "1"
		}
	}
}

func expandMatrixRTCCallOptions(options map[string]string) map[string]string {
	expanded := make(map[string]string, len(options)*2)
	for key, rawValue := range options {
		normalizedKey := normalizeMatrixRTCCallOption(key)
		expanded[normalizedKey] = strings.TrimSpace(rawValue)
		trimmed := strings.TrimSpace(rawValue)
		if strings.HasPrefix(trimmed, "{") {
			var nested map[string]any
			if json.Unmarshal([]byte(trimmed), &nested) == nil {
				flattenMatrixRTCCallOptions(normalizedKey, nested, expanded)
			}
		}
	}
	return expanded
}

func matrixRTCCallOptionValue(options map[string]string, keys ...string) (string, string, bool) {
	for _, key := range keys {
		if value, ok := options[normalizeMatrixRTCCallOption(key)]; ok {
			return normalizeMatrixRTCCallOption(value), key, true
		}
	}
	return "", "", false
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
	if len(options) == 0 {
		return matrixRTCCallAvailabilityUnknown, ""
	}
	expanded := expandMatrixRTCCallOptions(options)
	if value, key, ok := matrixRTCCallOptionValue(expanded,
		"business_calling_enabled",
		"calling_status", "calling_status_status", "calling_calling_status",
		"call_status", "calling_enabled", "call_enabled", "calls_enabled", "voice_call_enabled",
	); ok {
		if enabled, known := matrixRTCCallBool(value); known {
			if !enabled {
				return matrixRTCCallAvailabilityDisabled, "business_profile:" + key + "=" + value
			}
			// Keep looking for a visibility override before returning enabled.
		}
	}
	if value, key, ok := matrixRTCCallOptionValue(expanded,
		"call_icon_visibility", "calling_call_icon_visibility", "calling_icon_visibility",
	); ok {
		switch value {
		case "hide_in_chat", "disable_all", "hidden", "hide", "disabled":
			return matrixRTCCallAvailabilityHidden, "business_profile:" + key + "=" + value
		}
	}
	if value, key, ok := matrixRTCCallOptionValue(expanded,
		"business_calling_enabled",
		"calling_status", "calling_status_status", "calling_calling_status",
		"call_status", "calling_enabled", "call_enabled", "calls_enabled", "voice_call_enabled",
	); ok {
		if enabled, known := matrixRTCCallBool(value); known && enabled {
			return matrixRTCCallAvailabilityEnabled, "business_profile:" + key + "=" + value
		}
	}
	return matrixRTCCallAvailabilityUnknown, ""
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
	now := time.Now()
	return matrixRTCCallAvailabilityCache.remember(wa.matrixRTCCallAvailabilityKeys(ctx, jid), matrixRTCCallAvailabilityCacheEntry{availability: availability, source: source, expires: now.Add(ttl)}, now)
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
	for _, key := range []string{"enabled", "supported", "available", "status", "value"} {
		if raw, ok := node.Attrs[key]; ok {
			if value, ok := matrixRTCNodeStringValue(raw); ok {
				if enabled, known := matrixRTCCallBool(value); known {
					if enabled {
						return matrixRTCCallAvailabilityEnabled, "usync:feature:voip:" + key + "=" + value
					}
					return matrixRTCCallAvailabilityDisabled, "usync:feature:voip:" + key + "=" + value
				}
			}
		}
	}
	if value, ok := matrixRTCNodeStringValue(node.Content); ok && value != "" {
		if enabled, known := matrixRTCCallBool(value); known {
			if enabled {
				return matrixRTCCallAvailabilityEnabled, "usync:feature:voip=" + value
			}
			return matrixRTCCallAvailabilityDisabled, "usync:feature:voip=" + value
		}
	}
	// Feature protocols represent support by returning the requested feature
	// node. A present, valueless <voip/> therefore means supported.
	return matrixRTCCallAvailabilityEnabled, "usync:feature:voip=present"
}

func findMatrixRTCFeatureNode(node waBinary.Node, tag string) (waBinary.Node, bool) {
	if node.Tag == tag {
		return node, true
	}
	for _, child := range node.GetChildren() {
		if found, ok := findMatrixRTCFeatureNode(child, tag); ok {
			return found, true
		}
	}
	return waBinary.Node{}, false
}

func classifyWhatsAppVoIPFeatureResponse(resp *waBinary.Node) (matrixRTCCallAvailability, string) {
	if resp == nil {
		return matrixRTCCallAvailabilityUnknown, ""
	}
	list, ok := resp.GetOptionalChildByTag("usync", "list")
	if !ok {
		return matrixRTCCallAvailabilityUnknown, ""
	}
	users := list.GetChildrenByTag("user")
	if len(users) != 1 {
		return matrixRTCCallAvailabilityUnknown, ""
	}
	feature, ok := findMatrixRTCFeatureNode(users[0], "feature")
	if !ok {
		return matrixRTCCallAvailabilityUnknown, ""
	}
	if _, hasError := findMatrixRTCFeatureNode(feature, "error"); hasError {
		return matrixRTCCallAvailabilityUnknown, ""
	}
	if voip, ok := findMatrixRTCFeatureNode(feature, "voip"); ok {
		return matrixRTCVoIPFeatureValue(voip)
	}
	// The request asks for only the voip feature. A valid feature response with
	// no voip child is explicit negative capability, not a transport failure.
	return matrixRTCCallAvailabilityDisabled, "usync:feature:voip=absent"
}

func (wa *WhatsAppClient) queryWhatsAppCallDevices(ctx context.Context, jid types.JID) (matrixRTCCallDeviceClass, string) {
	if wa.Client == nil || !wa.Client.IsConnected() {
		return matrixRTCCallDeviceClassUnknown, ""
	}
	jid = wa.matrixRTCBusinessProfileJID(ctx, jid)
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
	class, source := classifyMatrixRTCCallDevices(devices)
	wa.UserLogin.Log.Debug().Stringer("peer", jid).Str("source", source).Int("devices", len(devices)).
		Msg("WhatsApp MatrixRTC device class refreshed")
	return class, source
}

func (wa *WhatsAppClient) queryWhatsAppVoIPFeature(ctx context.Context, jid types.JID) (matrixRTCCallAvailability, string) {
	if wa.Client == nil || !wa.Client.IsConnected() {
		return matrixRTCCallAvailabilityUnknown, ""
	}
	jid = wa.matrixRTCBusinessProfileJID(ctx, jid)
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
	return classifyWhatsAppVoIPFeatureResponse(resp)
}

func flattenMatrixRTCBusinessProfileNode(prefix string, node waBinary.Node, out map[string]string) {
	key := normalizeMatrixRTCCallOption(node.Tag)
	if prefix != "" {
		if key != "" {
			key = prefix + "_" + key
		} else {
			key = prefix
		}
	}
	for attrKey, attrValue := range node.Attrs {
		if value, ok := matrixRTCNodeStringValue(attrValue); ok {
			out[strings.Trim(key+"_"+normalizeMatrixRTCCallOption(attrKey), "_")] = value
		}
	}
	if value, ok := matrixRTCNodeStringValue(node.Content); ok && key != "" && value != "" {
		out[key] = value
	}
	for _, child := range node.GetChildren() {
		flattenMatrixRTCBusinessProfileNode(key, child, out)
	}
}

func classifyWhatsAppBusinessCallProfileResponse(resp *waBinary.Node) (matrixRTCCallAvailability, string) {
	if resp == nil {
		return matrixRTCCallAvailabilityUnknown, ""
	}
	profileOptions, ok := findMatrixRTCFeatureNode(*resp, "profile_options")
	if !ok {
		return matrixRTCCallAvailabilityUnknown, ""
	}
	options := make(map[string]string)
	for _, child := range profileOptions.GetChildren() {
		flattenMatrixRTCBusinessProfileNode("", child, options)
	}
	return classifyWhatsAppBusinessCallOptions(options)
}

func (wa *WhatsAppClient) queryWhatsAppBusinessCallProfile(ctx context.Context, jid types.JID) (matrixRTCCallAvailability, string) {
	if wa.Client == nil || !wa.Client.IsConnected() {
		return matrixRTCCallAvailabilityUnknown, ""
	}
	jid = wa.matrixRTCBusinessProfileJID(ctx, jid)
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
	availability, source := classifyWhatsAppBusinessCallProfileResponse(resp)
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
	deviceClass, deviceSource := wa.queryWhatsAppCallDevices(ctx, jid)
	if deviceClass == matrixRTCCallDeviceClassUnknown && (jid.Server == types.HostedServer || jid.Server == types.HostedLIDServer) {
		deviceClass, deviceSource = matrixRTCCallDeviceClassHosted, "jid:hosted"
	}
	if deviceClass == matrixRTCCallDeviceClassUnavailable {
		cached := wa.rememberMatrixRTCCallAvailabilityForPeer(ctx, jid, matrixRTCCallAvailabilityDisabled, deviceSource, 30*time.Minute)
		return cached.availability, cached.source
	}

	featureAvailability, featureSource := wa.queryWhatsAppVoIPFeature(ctx, jid)
	profileAvailability := matrixRTCCallAvailabilityUnknown
	profileSource := ""
	if featureAvailability != matrixRTCCallAvailabilityDisabled {
		profileAvailability, profileSource = wa.queryWhatsAppBusinessCallProfile(ctx, jid)
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

var rtcProbes sync.Map
var rtcProbeSlots = make(chan struct{}, 4)

// Chat creation may read the local cache, but capability probes run off the message path.
func (wa *WhatsAppClient) rtcCachedRestriction(jid types.JID) bool {
	if wa.IsOwnJID(jid) {
		return true
	}
	if a, _ := matrixRTCCallStaticJIDAvailability(jid); a == matrixRTCCallAvailabilityDisabled {
		return true
	}
	key := matrixRTCCallAvailabilityCacheKey(string(wa.UserLogin.ID), jid.ToNonAD())
	if entry, ok := matrixRTCCallAvailabilityCache.get([]string{key}, time.Now()); ok {
		return entry.availability == matrixRTCCallAvailabilityDisabled || entry.availability == matrixRTCCallAvailabilityHidden
	}
	if wa.Client != nil && wa.Client.IsConnected() {
		if _, loaded := rtcProbes.LoadOrStore(key, true); !loaded {
			go func() {
				defer rtcProbes.Delete(key)
				rtcProbeSlots <- struct{}{}
				defer func() { <-rtcProbeSlots }()
				ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
				defer cancel()
				wa.matrixRTCCallAvailabilityForPeer(ctx, jid)
				wa.refreshMatrixRTCCallPermissionForJID(ctx, jid)
			}()
		}
	}
	return true
}

func (wa *WhatsAppClient) rtcPortalPower(ctx context.Context, jid types.JID, dm bool) func(*event.PowerLevelsEventContent) bool {
	mx, ok := wa.Main.Bridge.Matrix.(*bridgev2matrix.Connector)
	restrict := true
	if ok && wa.Main.Config.ExperimentalWhatsAppVoIP && wa.Main.Config.RTCGatewaySocket != "" && mx.Config.Encryption.Allow {
		row, err := wa.Main.Bridge.DB.Portal.GetByKey(ctx, wa.makeWAPortalKey(jid))
		if err == nil {
			encrypted := mx.Config.Encryption.Default
			if row != nil && row.MXID != "" {
				encrypted, err = mx.StateStore.IsEncrypted(ctx, row.MXID)
			}
			if encrypted && err == nil {
				restrict = dm && wa.rtcCachedRestriction(jid)
			}
		}
	}
	return func(power *event.PowerLevelsEventContent) bool { return mergeRTCPortalPower(power, restrict) }
}

// Record the prior explicit rule together with each restriction in the same
// state event. An administrator's later change takes precedence.
func mergeRTCPortalPower(power *event.PowerLevelsEventContent, restrict bool) bool {
	if power.Events == nil {
		power.Events = make(map[string]int)
	}
	changed := false
	for typ := range matrixRTCPowerLevels(false, map[event.Type]int{}) {
		key := typ.Type
		marker := "org.mautrix.whatsapp.rtc.previous." + key
		old, exists := power.Events[key]
		previous, managed := power.Events[marker]
		if managed && (!exists || old != 100) {
			delete(power.Events, marker)
			managed = false
			changed = true
		}
		if restrict {
			if !exists || old < 100 {
				if !exists {
					old = 0
				}
				power.Events[marker] = old
				power.Events[key] = 100
				changed = true
			}
		} else if managed {
			power.Events[key] = previous
			delete(power.Events, marker)
			changed = true
		} else if !exists {
			power.Events[key] = 0
			changed = true
		}
	}
	return changed
}

func (wa *WhatsAppClient) matrixRTCCallHardDisabled(ctx context.Context, jid types.JID) (bool, string) {
	availability, source := wa.matrixRTCCallAvailabilityForPeer(ctx, jid)
	return availability == matrixRTCCallAvailabilityDisabled || availability == matrixRTCCallAvailabilityHidden, source
}

func (wa *WhatsAppClient) refreshMatrixRTCCallPermission(ctx context.Context, portal *bridgev2.Portal) {
	if portal == nil || portal.OtherUserID == "" {
		return
	}
	jid, err := waid.ParsePortalID(portal.ID)
	if err != nil || !matrixRTCCallPortalServer(jid.Server) {
		return
	}
	portal.UpdateInfo(ctx, wa.wrapDMInfo(ctx, jid), wa.UserLogin, nil, time.Time{})
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

// Re-evaluate every existing 1:1 portal after WhatsApp reconnects. WhatsApp's
// own consumer clients refresh business calling metadata independently of a
// call attempt; doing the same here makes Element's call controls reflect the
// remote account state before the user presses Call.
func (wa *WhatsAppClient) refreshMatrixRTCExistingDMPortals(ctx context.Context) {
	portals, err := wa.Main.Bridge.GetAllPortalsWithMXID(ctx)
	if err != nil {
		wa.UserLogin.Log.Warn().Err(err).Msg("Failed to enumerate portals for MatrixRTC callability refresh")
		return
	}
	sem := make(chan struct{}, 4)
	var wg sync.WaitGroup
	for _, portal := range portals {
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
		jid = jid.ToNonAD()
		wg.Add(1)
		go func() {
			defer wg.Done()
			select {
			case sem <- struct{}{}:
				defer func() { <-sem }()
			case <-ctx.Done():
				return
			}
			wa.refreshMatrixRTCCallPermission(ctx, portal)
		}()
	}
	wg.Wait()
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

type rtcOutgoingSession struct {
	groupUpdates chan []string
	seenUpdates  map[string]bool
	device       id.DeviceID
	eventID      string
	sender       string
	cancel       context.CancelFunc
}

type rtcIntentRecord struct {
	intent  string
	expires time.Time
}

type rtcMemberRecord struct {
	eventID  string
	sticky   bool
	memberID string
	deviceID string
	format   string
	slotID   string
	sender   string
	intent   string
	expires  time.Time
}

type rtcLegacyMemberContent struct {
	DeviceID     string `json:"device_id"`
	Application  string `json:"application"`
	CallID       string `json:"call_id"`
	MembershipID string `json:"membershipID"`
	Intent       string `json:"m.call.intent"`
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

func rtcIntentKey(roomID, slotID, sender string) string {
	return roomID + "\x00" + canonicalMatrixRTCSlotID(slotID) + "\x00" + sender
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
	if wa.rtcIntents == nil {
		wa.rtcIntents = make(map[string]rtcIntentRecord)
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
	key := rtcIntentKey(evt.RoomID.String(), content.SlotID, evt.Sender.String())
	wa.rtcMu.Lock()
	defer wa.rtcMu.Unlock()
	wa.pruneRTCRecords(time.Now())
	isJoin := content.Member.Membership == "join" || (content.Member.Membership == "" && content.Member.ID != "")
	if !isJoin || content.Application.Type != "m.call" {
		delete(wa.rtcIntents, key)
		for k, v := range wa.rtcMembers {
			if strings.HasPrefix(k, evt.RoomID.String()+"\x00") && v.sender == evt.Sender.String() && v.slotID == content.SlotID && (content.Member.ID == "" || v.memberID == content.Member.ID) {
				delete(wa.rtcMembers, k)
			}
		}
		return
	}
	expires := time.Now().Add(rtcIntentLifetime)
	intent := content.Application.Intent
	if intent == "audio" || intent == "video" {
		wa.rtcIntents[key] = rtcIntentRecord{intent: intent, expires: expires}
	}
	if evt.ID != "" {
		format := "msc"
		if content.Member.Membership == "" {
			format = "room_state"
		}
		wa.rtcMembers[rtcMemberEventKey(evt.RoomID.String(), evt.ID.String())] = rtcMemberRecord{
			sticky: true, memberID: content.Member.ID, deviceID: string(rtcTrustedDevice(evt)), format: format,
			slotID:  content.SlotID,
			sender:  evt.Sender.String(),
			intent:  intent,
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
		_ = json.Unmarshal(evt.Unsigned.PrevContent.VeryRaw, &content)
		if content.MembershipID == "" && content.DeviceID != "" {
			content.MembershipID = evt.Sender.String() + ":" + content.DeviceID
		}
		wa.rtcMu.Lock()
		key := rtcIntentKey(evt.RoomID.String(), legacyMatrixRTCSlot(content.Application, content.CallID), evt.Sender.String())
		delete(wa.rtcIntents, key)
		for k, v := range wa.rtcMembers {
			if strings.HasPrefix(k, evt.RoomID.String()+"\x00") && v.sender == evt.Sender.String() && v.memberID == content.MembershipID && v.deviceID == content.DeviceID {
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
	intent := content.Intent
	if intent != "audio" && intent != "video" {
		intent = "video"
	}
	expires := time.Now().Add(rtcIntentLifetime)
	record := rtcMemberRecord{memberID: content.MembershipID, deviceID: content.DeviceID, format: "room_state", slotID: slotID, sender: evt.Sender.String(), intent: intent, expires: expires}
	wa.rtcMu.Lock()
	wa.pruneRTCRecords(time.Now())
	wa.rtcMembers[rtcMemberEventKey(evt.RoomID.String(), evt.ID.String())] = record
	wa.rtcIntents[rtcIntentKey(evt.RoomID.String(), slotID, evt.Sender.String())] = rtcIntentRecord{intent: intent, expires: expires}
	wa.rtcMu.Unlock()
}

// Called with rtcMu held. Event IDs are generally looked up only once.
func (wa *WhatsAppConnector) pruneRTCRecords(now time.Time) {
	for k, v := range wa.rtcMembers {
		if !v.expires.After(now) {
			delete(wa.rtcMembers, k)
		}
	}
	for k, v := range wa.rtcIntents {
		if !v.expires.After(now) {
			delete(wa.rtcIntents, k)
		}
	}
}

func rtcTrustedDevice(evt *event.Event) id.DeviceID {
	d := evt.Mautrix.TrustSource
	if !evt.Mautrix.WasEncrypted || evt.Mautrix.ForwardedKeys || d == nil || d.UserID != evt.Sender || evt.Mautrix.TrustState < id.TrustStateCrossSignedTOFU {
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

func (wa *WhatsAppConnector) matrixRTCIntent(ctx context.Context, roomID, slotID, sender, hinted string) string {
	if hinted == "audio" || hinted == "video" {
		return hinted
	}
	deadline := time.NewTimer(rtcIntentWait)
	defer deadline.Stop()
	ticker := time.NewTicker(25 * time.Millisecond)
	defer ticker.Stop()
	key := rtcIntentKey(roomID, slotID, sender)
	lookup := func() string {
		wa.rtcMu.Lock()
		defer wa.rtcMu.Unlock()
		record, ok := wa.rtcIntents[key]
		if !ok {
			return ""
		}
		if time.Now().After(record.expires) {
			delete(wa.rtcIntents, key)
			return ""
		}
		return record.intent
	}
	if intent := lookup(); intent != "" {
		return intent
	}
	for {
		select {
		case <-ctx.Done():
			return ""
		case <-deadline.C:
			// MatrixRTC's own CallIntent default is video when no participant
			// states an intent, so mirror that instead of silently downgrading.
			return "video"
		case <-ticker.C:
			if intent := lookup(); intent != "" {
				return intent
			}
		}
	}
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

func (wa *WhatsAppConnector) handleMatrixRTCInvite(ctx context.Context, evt *event.Event) {
	if evt == nil || evt.RoomID == "" || evt.Sender == "" || evt.ID == "" {
		return
	}
	var content rtcInviteContent
	if err := json.Unmarshal(evt.Content.VeryRaw, &content); err != nil {
		return
	}
	notificationType := content.NotificationType
	if notificationType == "" {
		notificationType = content.Application.NotificationType
	}
	if notificationType != "" && notificationType != "ring" && notificationType != "notification" {
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
	if wa.rejectMatrixRTCBridgeBotControlCall(ctx, evt) {
		return
	}
	device := rtcTrustedDevice(evt)
	if device == "" {
		return
	}
	membershipFormat := "msc"
	memberEvent := ""
	// MSC4075 relates the ring to its membership event. Some versions also
	// include a slot_id: that does not make the ring a new MSC4143 invite.
	if content.SlotID == "" || ((evt.Type.Type == rtcInviteWire || evt.Type.Type == rtcNotificationStable) && content.RelatesTo.RelType == "m.reference" && content.RelatesTo.EventID != "") {
		if content.RelatesTo.RelType != "m.reference" {
			return
		}
		record, ok := wa.matrixRTCMemberForNotification(evt.RoomID.String(), content.RelatesTo.EventID, evt.Sender.String())
		if !ok || (record.deviceID != "" && record.deviceID != string(device)) {
			return
		}
		memberEvent = content.RelatesTo.EventID
		membershipFormat = record.format
		if membershipFormat == "" {
			membershipFormat = "room_state"
		}
		relatedSlot := canonicalMatrixRTCSlotID(record.slotID)
		if content.SlotID != "" && content.SlotID != relatedSlot {
			return
		}
		content.SlotID = relatedSlot
		if content.Intent == "" && content.Application.Intent == "" {
			content.Intent = record.intent
		}
	}
	if content.SlotID != matrixRTCSlotDefault || (content.StickyKey != "" && content.StickyKey != content.SlotID) {
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
	if isGroup && !content.Mentions.Room {
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

	ringDeadline := sent.Add(time.Duration(lifetime) * time.Millisecond)
	// The invitation deadline applies only until WhatsApp answers. An active
	// session must outlive the original ring deadline.
	callCtx, cancel := context.WithCancel(wa.Bridge.BackgroundCtx)
	key := rtcSessionKey(evt.RoomID.String(), content.SlotID)
	wa.rtcMu.Lock()
	if current, exists := wa.rtcOutgoing[key]; exists {
		// Only a signed follow-up for this active group invitation may ring named
		// roster members. An unrelated invite never replaces the running call.
		if isGroup && current.groupUpdates != nil && len(targets) > 0 && current.sender == evt.Sender.String() && current.device == device &&
			content.RelatesTo.RelType == "m.reference" && content.RelatesTo.EventID == current.eventID && !current.seenUpdates[evt.ID.String()] && len(current.seenUpdates) < 128 {
			select {
			case current.groupUpdates <- targets:
				current.seenUpdates[evt.ID.String()] = true
			default:
			}
		}
		wa.rtcMu.Unlock()
		cancel()
		return
	}
	var groupUpdates chan []string
	if isGroup {
		groupUpdates = make(chan []string, 4)
	}
	wa.rtcOutgoing[key] = rtcOutgoingSession{groupUpdates: groupUpdates, seenUpdates: make(map[string]bool), eventID: evt.ID.String(), sender: evt.Sender.String(), device: device, cancel: cancel}
	wa.rtcMu.Unlock()

	hintedIntent := content.Intent
	if hintedIntent == "" {
		hintedIntent = content.Application.Intent
	}
	go func() {
		defer cancel()
		defer wa.finishMatrixRTCOutgoing(key, evt.ID.String())
		intent := wa.matrixRTCIntent(callCtx, evt.RoomID.String(), content.SlotID, evt.Sender.String(), hintedIntent)
		if intent == "" || callCtx.Err() != nil {
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
			callCtx, callID, roomID, content.SlotID, membershipFormat, ghostMXID, evt.Sender.String(), string(device), memberEvent, peer.ToNonAD().String(), isGroup, groupJID, intent == "video", ringDeadline, targets, groupUpdates,
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
