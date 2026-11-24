#!/usr/bin/env bash
set -euo pipefail

curl() { command curl --connect-timeout 5 --max-time 30 "$@"; }

RTC_EVENT_TYPES=(
    "m.rtc.slot"
    "org.matrix.msc4143.rtc.slot"
    "m.rtc.member"
    "org.matrix.msc4143.rtc.member"
    "org.matrix.msc3401.call.member"
    "m.call.member"
    "m.rtc.invite"
    "m.rtc.notification"
    "org.matrix.msc4075.rtc.notification"
    "m.rtc.decline"
    "org.matrix.msc4310.rtc.decline"
)

set_rtc_power_levels() {
    local json="$1"
    local level="$2"
    local preserve_existing="${3:-0}"
    local jq_program='.events = (.events // {})'
    local event_type
    for event_type in "${RTC_EVENT_TYPES[@]}"; do
        # Add missing RTC permissions at PL0, but never lower an explicit
        # administrator/bridge restriction (Note-to-Self uses PL100).
        if [[ "$preserve_existing" == "1" ]]; then
            jq_program+=" | .events[\"$event_type\"] = (.events[\"$event_type\"] // $level)"
        else
            jq_program+=" | .events[\"$event_type\"] = $level"
        fi
    done
    jq -c "$jq_program" <<<"$json"
}

merge_power_levels() {
    set_rtc_power_levels "$1" 0 1
}

restrict_power_levels() {
    # Preserve any explicit stronger rule and remember what this installer changed.
    jq -c --argjson types "$(printf '%s\n' "${RTC_EVENT_TYPES[@]}" | jq -R . | jq -s .)" '
        .events //= {} | reduce $types[] as $key (.;
        if (.events[$key] // 0)<100 then
            .events["org.mautrix.whatsapp.rtc.previous."+$key]=(.events[$key] // 0) | .events[$key]=100
        else . end)' <<<"$1"
}

bot_control_levels() {
    local json="$1"
    local human="$2"
    local bot="$3"
    jq -c --arg human "$human" --arg bot "$bot" '{
        human: ((.users[$human] // .users_default // 0) | tonumber),
        bot: ((.users[$bot] // .users_default // 0) | tonumber)
    }' <<<"$json"
}

restrict_bot_control_power_levels() {
    local json="$1"
    local human="$2"
    local bot="$3"
    local levels human_level bot_level jq_program event_type

    levels="$(bot_control_levels "$json" "$human" "$bot")"
    human_level="$(jq -r '.human' <<<"$levels")"
    bot_level="$(jq -r '.bot' <<<"$levels")"

    if (( bot_level <= human_level )); then
        printf '%s\n' "$json"
        return 0
    fi

    # The bot already outranks the human. Require at least the bot's existing
    # level for every RTC event, while preserving any stronger restriction.
    jq_program='.events = (.events // {})'
    for event_type in "${RTC_EVENT_TYPES[@]}"; do
        jq_program+=" | .events[\"$event_type\"] = ([((.events[\"$event_type\"] // 0) | tonumber), $bot_level] | max)"
    done
    jq -c "$jq_program" <<<"$json"
}

transfer_bot_control_power_to_bot() {
    local json="$1"
    local human="$2"
    local bot="$3"
    local levels human_level bot_level target_level new_human_level jq_program event_type

    levels="$(bot_control_levels "$json" "$human" "$bot")"
    human_level="$(jq -r '.human' <<<"$levels")"
    bot_level="$(jq -r '.bot' <<<"$levels")"
    (( human_level > 0 )) || { printf '%s\n' "$json"; return 0; }

    target_level="$human_level"
    if (( bot_level > target_level )); then
        target_level="$bot_level"
    fi
    new_human_level=$((target_level - 1))

    # This event is sent as the human through the local double-puppet
    # appservice. Matrix auth allows a sender to lower its own PL and to raise
    # another user no higher than the sender's current PL. For a 100/100 legacy
    # room this becomes human=99, bot=100, RTC=100 in one authorised event.
    jq_program='.users = (.users // {})'
    jq_program+=" | .users[\"$human\"] = $new_human_level"
    if (( bot_level < target_level )); then
        jq_program+=" | .users[\"$bot\"] = $target_level"
    fi
    jq_program+=' | .events = (.events // {})'
    for event_type in "${RTC_EVENT_TYPES[@]}"; do
        jq_program+=" | .events[\"$event_type\"] = ([((.events[\"$event_type\"] // 0) | tonumber), $target_level] | max)"
    done
    jq -c "$jq_program" <<<"$json"
}

merge_direct_mapping() {
    local direct_json="$1"
    local ghost="$2"
    local room="$3"
    jq -c --arg ghost "$ghost" --arg room "$room" \
        '.[$ghost] = (((.[$ghost] // []) + [$room]) | unique)' <<<"$direct_json"
}

select_candidate() {
    local members_json="$1"
    local hints_json="$2"
    local encryption_json="$3"
    local bot_mxid="$4"
    local server="$5"
    local ghost_regexes_json="$6"

    jq -cn \
        --argjson members "$members_json" \
        --argjson hints "$hints_json" \
        --argjson encryption "$encryption_json" \
        --argjson ghost_regexes "$ghost_regexes_json" \
        --arg bot "$bot_mxid" \
        --arg server "$server" '
        def is_ghost($u): any($ghost_regexes[]; . as $rx | ($u | test($rx)));
        select($encryption.algorithm == "m.megolm.v1.aes-sha2")
        | ($hints.service_members // []) as $service
        | ($members.joined | keys) as $joined
        | [$joined[] as $u | select(($service | index($u)) == null) | $u] as $humans
        | [$humans[] as $u | select(is_ghost($u)) | $u] as $ghosts
        | [$humans[] as $u | select(is_ghost($u) | not) | $u] as $realHumans
        | if (
            ($service | index($bot)) != null
            and ($humans | length) == 2
            and ($ghosts | length) == 1
            and ($realHumans | length) == 1
            and ($realHumans[0] | endswith(":" + $server))
          )
          then {ghost:$ghosts[0], human:$realHumans[0]}
          else empty
          end
    '
}

# The protocol and channel come from the bridge info it published for its portal.
# Fail closed on ambiguous or foreign bridge metadata. A group can have one remaining ghost.
portal_kind() {
    jq -r --arg bot "$2" --argjson ghosts "$3" '
      [ .[] | select(.type=="m.bridge" or .type=="uk.half-shot.bridge") ] as $all
      | [$all[] | . as $e | select(.content.bridgebot==$bot and .content.protocol.id=="whatsapp"
          and (.sender==$bot or any($ghosts[]; . as $r | $e.sender|test($r))))
          | .content | if (.channel.id|endswith("@g.us")) then "group"
            elif (.["com.beeper.room_type.v2"] // .["com.beeper.room_type"])=="dm" and
                 (.channel.id|test("@(s\\.whatsapp\\.net|lid|hosted|hosted\\.lid|bot)$")) then "dm" else "other" end] | unique
      | if length==1 then .[0] elif ($all|length)==0 then "none" else "other" end' <<<"$1"
}

select_group_candidate() {
    local members_json="$1"
    local hints_json="$2"
    local encryption_json="$3"
    local bot_mxid="$4"
    local server="$5"
    local ghost_regexes_json="$6"
    jq -cn \
        --argjson members "$members_json" \
        --argjson hints "$hints_json" \
        --argjson encryption "$encryption_json" \
        --argjson ghost_regexes "$ghost_regexes_json" \
        --arg bot "$bot_mxid" \
        --arg server "$server" '
        def is_ghost($u): any($ghost_regexes[]; . as $rx | ($u | test($rx)));
        select($encryption.algorithm == "m.megolm.v1.aes-sha2")
        | ($hints.service_members // []) as $service
        | ($members.joined | keys) as $joined
        | [$joined[] as $u | select(($service | index($u)) == null) | $u] as $humans
        | [$humans[] as $u | select(is_ghost($u)) | $u] as $ghosts
        | [$humans[] as $u | select(is_ghost($u) | not) | $u] as $realHumans
        | if (
            ($service | index($bot)) != null
            and ($joined | index($bot)) != null
            and any($realHumans[]; endswith(":" + $server))
          ) then {human:$realHumans[0], ghosts:$ghosts} else empty end
    '
}

select_bot_control_room() {
    local members_json="$1"
    local bot_mxid="$2"
    local server="$3"
    local ghost_regexes_json="$4"
    jq -cn \
        --argjson members "$members_json" \
        --argjson ghost_regexes "$ghost_regexes_json" \
        --arg bot "$bot_mxid" \
        --arg server "$server" '
        def is_ghost($u): any($ghost_regexes[]; . as $rx | ($u | test($rx)));
        ($members.joined | keys) as $joined
        | [$joined[] | select(. != $bot)] as $others
        | [$others[] as $u | select(is_ghost($u)) | $u] as $ghosts
        | [$others[] as $u | select(is_ghost($u) | not) | $u] as $humans
        | if (($joined | index($bot)) != null and ($ghosts|length)==0 and ($humans|length)==1 and ($humans[0]|endswith(":"+$server)))
          then {human:$humans[0]} else empty end
    '
}

run_self_test() {
    local b="@whatsappbot:example.org" state input_kind
    state='[{"type":"m.bridge","sender":"@whatsappbot:example.org","content":{"bridgebot":"@whatsappbot:example.org","protocol":{"id":"whatsapp"},"channel":{"id":"42@g.us"},"com.beeper.room_type.v2":"dm"}}]'
    [[ "$(portal_kind "$state" "$b" '[]')" == group ]]
    input_kind="$(jq -c '.[0].content.channel.id="1@s.whatsapp.net"' <<<"$state")"
    [[ "$(portal_kind "$input_kind" "$b" '[]')" == dm ]]
    [[ "$(portal_kind "$(jq -c '.[0].sender="@foreign:example.org"' <<<"$state")" "$b" '[]')" == other ]]
    [[ "$(portal_kind '[]' "$b" '[]')" == none ]]
    local input='{"users":{"@whatsappbot:example.org":100},"users_default":0,"events_default":0,"state_default":50,"events":{"m.room.name":0}}'
    local merged
    merged="$(merge_power_levels "$input")"
    jq -e '
        .events["m.room.name"] == 0 and
        .events["m.rtc.slot"] == 0 and
        .events["org.matrix.msc4143.rtc.slot"] == 0 and
        .events["m.rtc.member"] == 0 and
        .events["org.matrix.msc4143.rtc.member"] == 0 and
        .events["org.matrix.msc3401.call.member"] == 0 and
        .events["m.call.member"] == 0 and
        .events["m.rtc.invite"] == 0 and
        .events["m.rtc.notification"] == 0 and
        .events["org.matrix.msc4075.rtc.notification"] == 0 and
        .events["m.rtc.decline"] == 0 and
        .events["org.matrix.msc4310.rtc.decline"] == 0 and
        .state_default == 50 and
        .users["@whatsappbot:example.org"] == 100
    ' <<<"$merged" >/dev/null

    local restricted='{"events":{"m.rtc.slot":100,"m.rtc.invite":50}}'
    restricted="$(merge_power_levels "$restricted")"
    jq -e '.events["m.rtc.slot"] == 100 and .events["m.rtc.invite"] == 50 and .events["m.rtc.member"] == 0' <<<"$restricted" >/dev/null

    local tied_admin='{"users":{"@bernardo:example.org":100,"@whatsappbot:example.org":100},"users_default":0,"events":{"m.rtc.slot":100,"m.rtc.invite":100}}'
    local tied_after
    tied_after="$(restrict_bot_control_power_levels "$tied_admin" '@bernardo:example.org' '@whatsappbot:example.org')"
    jq -e --argjson before "$tied_admin" '. == $before' <<<"$tied_after" >/dev/null
    tied_after="$(transfer_bot_control_power_to_bot "$tied_admin" '@bernardo:example.org' '@whatsappbot:example.org')"
    jq -e '
        .users["@bernardo:example.org"] == 99 and
        .users["@whatsappbot:example.org"] == 100 and
        .events["m.rtc.slot"] == 100 and
        .events["m.rtc.invite"] == 100 and
        .events["m.rtc.decline"] == 100
    ' <<<"$tied_after" >/dev/null

    local human_above='{"users":{"@bernardo:example.org":100,"@whatsappbot:example.org":50},"users_default":0,"events":{}}'
    human_above="$(transfer_bot_control_power_to_bot "$human_above" '@bernardo:example.org' '@whatsappbot:example.org')"
    jq -e '.users["@bernardo:example.org"] == 99 and .users["@whatsappbot:example.org"] == 100 and .events["m.rtc.slot"] == 100' <<<"$human_above" >/dev/null

    local elevated_bot='{"users":{"@bernardo:example.org":75,"@whatsappbot:example.org":9001},"users_default":0,"events":{"m.rtc.slot":100,"m.rtc.invite":9500}}'
    elevated_bot="$(restrict_bot_control_power_levels "$elevated_bot" '@bernardo:example.org' '@whatsappbot:example.org')"
    jq -e '
        .users["@bernardo:example.org"] == 75 and
        .users["@whatsappbot:example.org"] == 9001 and
        .events["m.rtc.slot"] == 9001 and
        .events["m.rtc.invite"] == 9500 and
        .events["m.rtc.member"] == 9001 and
        .events["m.rtc.decline"] == 9001
    ' <<<"$elevated_bot" >/dev/null

    local default_tie='{"users_default":100,"events":{}}'
    local default_after
    default_after="$(restrict_bot_control_power_levels "$default_tie" '@bernardo:example.org' '@whatsappbot:example.org')"
    jq -e --argjson before "$default_tie" '. == $before' <<<"$default_after" >/dev/null

    local direct='{"@existing:example.org":["!existing:example.org"]}'
    direct="$(merge_direct_mapping "$direct" '@whatsapp_123:example.org' '!wa:example.org')"
    jq -e '."@existing:example.org" == ["!existing:example.org"] and ."@whatsapp_123:example.org" == ["!wa:example.org"]' <<<"$direct" >/dev/null

    local members='{"joined":{"@whatsappbot:example.org":{},"@bernardo:example.org":{},"@whatsapp_123:example.org":{}}}'
    local hints='{"service_members":["@whatsappbot:example.org"]}'
    local encryption='{"algorithm":"m.megolm.v1.aes-sha2"}'
    local candidate
    local ghost_regexes='["^@whatsapp_.+:example\\.org$"]'
    candidate="$(select_candidate "$members" "$hints" "$encryption" '@whatsappbot:example.org' 'example.org' "$ghost_regexes")"
    jq -e '.ghost == "@whatsapp_123:example.org" and .human == "@bernardo:example.org"' <<<"$candidate" >/dev/null

    local group='{"joined":{"@whatsappbot:example.org":{},"@bernardo:example.org":{},"@alice:example.org":{},"@whatsapp_123:example.org":{}}}'
    [[ -z "$(select_candidate "$group" "$hints" "$encryption" '@whatsappbot:example.org' 'example.org' "$ghost_regexes")" ]]
    [[ -z "$(select_candidate "$members" "$hints" '{}' '@whatsappbot:example.org' 'example.org' "$ghost_regexes")" ]]

    local group_members='{"joined":{"@whatsappbot:example.org":{},"@bernardo:example.org":{},"@whatsapp_123:example.org":{},"@whatsapp_456:example.org":{}}}'
    local group_candidate
    group_candidate="$(select_group_candidate "$group_members" "$hints" "$encryption" '@whatsappbot:example.org' 'example.org' "$ghost_regexes")"
    jq -e '.human == "@bernardo:example.org" and (.ghosts|length) == 2' <<<"$group_candidate" >/dev/null

    local bot_control='{"joined":{"@whatsappbot:example.org":{},"@bernardo:example.org":{}}}'
    jq -e '.human == "@bernardo:example.org"' <<<"$(select_bot_control_room "$bot_control" '@whatsappbot:example.org' 'example.org' "$ghost_regexes")" >/dev/null
    [[ -z "$(select_bot_control_room "$members" '@whatsappbot:example.org' 'example.org' "$ghost_regexes")" ]]

    local custom_members='{"joined":{"@whatsappbot:example.org":{},"@bernardo:example.org":{},"@wa-custom-123:example.org":{}}}'
    local custom_regexes='["^@wa-custom-.+:example\\.org$"]'
    candidate="$(select_candidate "$custom_members" "$hints" "$encryption" '@whatsappbot:example.org' 'example.org' "$custom_regexes")"
    jq -e '.ghost == "@wa-custom-123:example.org" and .human == "@bernardo:example.org"' <<<"$candidate" >/dev/null

    echo "migrate-rooms self-test: PASS"
}

if [[ "${1:-}" == "--self-test" ]]; then
    command -v jq >/dev/null
    run_self_test
    exit 0
fi

ROOT="${MATRIX_ROOT:-/opt/matrix}"
REGISTRATION="${MATRIXRTC_WHATSAPP_REGISTRATION:-$ROOT/mautrix-whatsapp/registration.yaml}"
CONFIG="${MATRIXRTC_WHATSAPP_CONFIG:-$ROOT/mautrix-whatsapp/config.yaml}"
SERVER="${MATRIXRTC_SERVER_NAME:-}"
DP_TOKEN_FILE="${MATRIXRTC_DOUBLE_PUPPET_TOKEN_FILE:-$ROOT/mautrix-whatsapp-call/generated/double_puppet_as_token}"
METADATA_HELPER="${MATRIXRTC_WHATSAPP_METADATA_HELPER:-$ROOT/mautrix-whatsapp-call/resolve-appservice.py}"

command -v curl >/dev/null
command -v jq >/dev/null
command -v python3 >/dev/null

if [[ -z "$SERVER" && -f "$ROOT/.env" ]]; then
    SERVER="$(sed -n 's/^MATRIXRTC_SERVER_NAME=//p' "$ROOT/.env" | tail -1 | tr -d '\r\"')"
fi
[[ -n "$SERVER" ]] || { echo "MATRIXRTC_SERVER_NAME is required" >&2; exit 2; }
[[ -f "$REGISTRATION" ]] || { echo "WhatsApp appservice registration not found: $REGISTRATION" >&2; exit 2; }
[[ -s "$DP_TOKEN_FILE" ]] || { echo "Double-puppet appservice token not found: $DP_TOKEN_FILE" >&2; exit 2; }

[[ -f "$METADATA_HELPER" ]] || { echo "WhatsApp metadata helper not found: $METADATA_HELPER" >&2; exit 2; }

META_JSON="$(python3 "$METADATA_HELPER" \
    --registration "$REGISTRATION" \
    --config "$CONFIG" \
    --server "$SERVER")"
AS_TOKEN="$(jq -r '.as_token // empty' <<<"$META_JSON")"
BOT_LOCALPART="$(jq -r '.bot_localpart // empty' <<<"$META_JSON")"
GHOST_REGEXES_JSON="$(jq -c '.ghost_regexes // []' <<<"$META_JSON")"
DP_TOKEN="$(tr -d '\r\n' < "$DP_TOKEN_FILE")"
[[ -n "$AS_TOKEN" && -n "$BOT_LOCALPART" && -n "$DP_TOKEN" && "$GHOST_REGEXES_JSON" != "[]" ]] || { echo "Could not read required appservice metadata" >&2; exit 2; }
umask 077
PRIVATE_DIR="$(mktemp -d)"
trap 'rm -rf -- "$PRIVATE_DIR"' EXIT
printf "Authorization: Bearer %s\n" "$AS_TOKEN" > "$PRIVATE_DIR/wa.header"
printf "Authorization: Bearer %s\n" "$DP_TOKEN" > "$PRIVATE_DIR/dp.header"
unset AS_TOKEN DP_TOKEN META_JSON
DP_AUTH=(-H "@$PRIVATE_DIR/dp.header")

BOT_MXID="@${BOT_LOCALPART}:${SERVER}"
BOT_ESC="$(jq -rn --arg v "$BOT_MXID" '$v|@uri')"
HS="${MATRIXRTC_PUBLIC_HOMESERVER:-https://$SERVER}"
[[ "$HS" == https://* ]] || { echo "Migration requires an HTTPS homeserver URL" >&2; exit 2; }
AUTH=(-H "@$PRIVATE_DIR/wa.header")
AS_QUERY="user_id=$BOT_ESC"
MAP_FILE="$PRIVATE_DIR/direct.tsv"
: > "$MAP_FILE"
chmod 0600 "$MAP_FILE"

WHOAMI="$(curl -fsS "${AUTH[@]}" "$HS/_matrix/client/v3/account/whoami?$AS_QUERY")"
ACTUAL="$(jq -r '.user_id // empty' <<<"$WHOAMI")"
[[ "$ACTUAL" == "$BOT_MXID" ]] || {
    echo "WhatsApp appservice token authenticated as $ACTUAL, expected $BOT_MXID" >&2
    exit 1
}

JOINED="$(curl -fsS "${AUTH[@]}" "$HS/_matrix/client/v3/joined_rooms?$AS_QUERY")"
SCANNED=0
ELIGIBLE=0
UNENCRYPTED_DMS=0
CHANGED=0
UNCHANGED=0
GROUP_ELIGIBLE=0
BOT_CONTROL_RESTRICTED=0
BOT_CONTROL_RUNTIME_ONLY=0
BOT_CONTROL_HUMAN_TRANSFER=0

while IFS= read -r ROOM; do
    [[ -n "$ROOM" ]] || continue
    SCANNED=$((SCANNED + 1))
    ROOM_ESC="$(jq -rn --arg v "$ROOM" '$v|@uri')"

    MEMBERS="$(curl -fsS "${AUTH[@]}" "$HS/_matrix/client/v3/rooms/$ROOM_ESC/joined_members?$AS_QUERY" 2>/dev/null)" || continue

    STATE="$(curl -fsS "${AUTH[@]}" "$HS/_matrix/client/v3/rooms/$ROOM_ESC/state?$AS_QUERY")" || continue
    PORTAL_KIND="$(portal_kind "$STATE" "$BOT_MXID" "$GHOST_REGEXES_JSON")" || continue
    BOT_CONTROL="$(select_bot_control_room "$MEMBERS" "$BOT_MXID" "$SERVER" "$GHOST_REGEXES_JSON" 2>/dev/null || true)"
    if [[ "$PORTAL_KIND" == none && -n "$BOT_CONTROL" ]]; then
        POWER="$(curl -fsS "${AUTH[@]}" "$HS/_matrix/client/v3/rooms/$ROOM_ESC/state/m.room.power_levels/?$AS_QUERY" 2>/dev/null || true)"
        if [[ -n "$POWER" ]]; then
            HUMAN="$(jq -r '.human' <<<"$BOT_CONTROL")"
            HUMAN_ESC="$(jq -rn --arg v "$HUMAN" '$v|@uri')"
            DIRECT_EVIDENCE="$(curl -fsS "${DP_AUTH[@]}" "$HS/_matrix/client/v3/user/$HUMAN_ESC/account_data/m.direct?user_id=$HUMAN_ESC" 2>/dev/null || echo '{}')"
            if ! jq -e --arg bot "$BOT_MXID" --arg room "$ROOM" '.[$bot] // [] | index($room)!=null' <<<"$DIRECT_EVIDENCE" >/dev/null &&
               ! jq -e --arg bot "$BOT_MXID" --arg human "$HUMAN" 'any(.[]; .type=="m.room.member" and (.state_key==$bot or .state_key==$human) and (.content.is_direct==true or .unsigned.prev_content.is_direct==true))' <<<"$STATE" >/dev/null; then
                continue
            fi
            if jq -e --arg human "$HUMAN" 'any(.[]; .type=="m.room.create" and (.content.room_version // "1" | tonumber? // 0)>=12 and ((.content.creator // .sender)==$human or ((.content.additional_creators // []) | index($human)!=null)))' <<<"$STATE" >/dev/null; then
                echo "Creator power is immutable in $ROOM; runtime decline remains active" >&2
                BOT_CONTROL_RUNTIME_ONLY=$((BOT_CONTROL_RUNTIME_ONLY+1))
                continue
            fi
            LEVELS="$(bot_control_levels "$POWER" "$HUMAN" "$BOT_MXID")"
            HUMAN_LEVEL="$(jq -r '.human' <<<"$LEVELS")"
            BOT_LEVEL="$(jq -r '.bot' <<<"$LEVELS")"
            if (( BOT_LEVEL > HUMAN_LEVEL )); then
                UPDATED="$(restrict_bot_control_power_levels "$POWER" "$HUMAN" "$BOT_MXID")"
                if ! jq -e --argjson updated "$UPDATED" '. == $updated' <<<"$POWER" >/dev/null 2>&1; then
                    PUT_WITH_CODE="$(curl -sS -w '\n%{http_code}' -X PUT "${AUTH[@]}" -H "Content-Type: application/json" --data "$UPDATED" \
                        "$HS/_matrix/client/v3/rooms/$ROOM_ESC/state/m.room.power_levels/?$AS_QUERY" 2>/dev/null || true)"
                    PUT_CODE="${PUT_WITH_CODE##*$'\n'}"
                    if [[ "$PUT_CODE" == "200" ]]; then
                        echo "RTC disabled for WhatsApp bridge-bot control room: $ROOM"
                        BOT_CONTROL_RESTRICTED=$((BOT_CONTROL_RESTRICTED + 1))
                    else
                        echo "RTC UI restriction could not be written for WhatsApp bridge-bot control room: $ROOM (HTTP $PUT_CODE); runtime decline remains active" >&2
                        BOT_CONTROL_RUNTIME_ONLY=$((BOT_CONTROL_RUNTIME_ONLY + 1))
                    fi
                fi
            elif (( HUMAN_LEVEL > 0 )); then
                # Equal/lower bot PL legacy rooms can be fixed by sending one
                # power-level event as the human through the double-puppet AS:
                # elevate/retain the bot at the old human level, lower only the
                # human's own PL by one, and require that level for RTC events.
                HUMAN_ESC="$(jq -rn --arg v "$HUMAN" '$v|@uri')"
                DP_QUERY="user_id=$HUMAN_ESC"
                DP_WHOAMI_WITH_CODE="$(curl -sS -w '\n%{http_code}' "${DP_AUTH[@]}" "$HS/_matrix/client/v3/account/whoami?$DP_QUERY" 2>/dev/null || true)"
                DP_WHOAMI_CODE="${DP_WHOAMI_WITH_CODE##*$'\n'}"
                DP_WHOAMI_BODY="${DP_WHOAMI_WITH_CODE%$'\n'*}"
                DP_ACTUAL="$(jq -r '.user_id // empty' <<<"$DP_WHOAMI_BODY" 2>/dev/null || true)"
                if [[ "$DP_WHOAMI_CODE" == "200" && "$DP_ACTUAL" == "$HUMAN" ]]; then
                    UPDATED="$(transfer_bot_control_power_to_bot "$POWER" "$HUMAN" "$BOT_MXID")"
                    PUT_WITH_CODE="$(curl -sS -w '\n%{http_code}' -X PUT "${DP_AUTH[@]}" -H "Content-Type: application/json" --data "$UPDATED" \
                        "$HS/_matrix/client/v3/rooms/$ROOM_ESC/state/m.room.power_levels/?$DP_QUERY" 2>/dev/null || true)"
                    PUT_CODE="${PUT_WITH_CODE##*$'\n'}"
                    if [[ "$PUT_CODE" == "200" ]]; then
                        NEW_LEVELS="$(bot_control_levels "$UPDATED" "$HUMAN" "$BOT_MXID")"
                        echo "RTC disabled for legacy WhatsApp bridge-bot control room via human self-demotion: $ROOM (human_pl=$(jq -r '.human' <<<"$NEW_LEVELS") bot_pl=$(jq -r '.bot' <<<"$NEW_LEVELS"))"
                        BOT_CONTROL_HUMAN_TRANSFER=$((BOT_CONTROL_HUMAN_TRANSFER + 1))
                    else
                        echo "RTC UI restriction unavailable for legacy WhatsApp bridge-bot control room: $ROOM (human_pl=$HUMAN_LEVEL bot_pl=$BOT_LEVEL HTTP=$PUT_CODE); runtime decline remains active" >&2
                        BOT_CONTROL_RUNTIME_ONLY=$((BOT_CONTROL_RUNTIME_ONLY + 1))
                    fi
                else
                    echo "RTC UI restriction unavailable for legacy WhatsApp bridge-bot control room: $ROOM (double-puppet impersonation failed HTTP=$DP_WHOAMI_CODE); runtime decline remains active" >&2
                    BOT_CONTROL_RUNTIME_ONLY=$((BOT_CONTROL_RUNTIME_ONLY + 1))
                fi
            else
                echo "RTC UI restriction unavailable for legacy WhatsApp bridge-bot control room: $ROOM (human_pl=$HUMAN_LEVEL bot_pl=$BOT_LEVEL); runtime decline remains active" >&2
                BOT_CONTROL_RUNTIME_ONLY=$((BOT_CONTROL_RUNTIME_ONLY + 1))
            fi
        fi
        continue
    fi

    [[ "$PORTAL_KIND" == dm || "$PORTAL_KIND" == group ]] || continue
    HINTS_WITH_CODE="$(curl -sS -w '\n%{http_code}' "${AUTH[@]}" "$HS/_matrix/client/v3/rooms/$ROOM_ESC/state/io.element.functional_members/?$AS_QUERY" 2>/dev/null || true)"
    HINT_CODE="${HINTS_WITH_CODE##*$'\n'}"
    HINT_BODY="${HINTS_WITH_CODE%$'\n'*}"
    if [[ "$HINT_CODE" == 404 ]]; then HINT_BODY="{}"; elif [[ "$HINT_CODE" != 200 ]]; then continue; fi

    # Restrict the RTC events in unencrypted, authoritative WhatsApp portals.
    ENCRYPTION_WITH_CODE="$(curl -sS -w '\n%{http_code}' "${AUTH[@]}" "$HS/_matrix/client/v3/rooms/$ROOM_ESC/state/m.room.encryption/?$AS_QUERY" 2>/dev/null || true)"
    ENCRYPTION_CODE="${ENCRYPTION_WITH_CODE##*$'\n'}"
    ENCRYPTION_BODY="${ENCRYPTION_WITH_CODE%$'\n'*}"
    if [[ "$ENCRYPTION_CODE" != "200" ]] || [[ "$(jq -r '.algorithm // empty' <<<"$ENCRYPTION_BODY" 2>/dev/null || true)" != "m.megolm.v1.aes-sha2" ]]; then
        if [[ "$ENCRYPTION_CODE" == 404 || "$ENCRYPTION_CODE" == 200 ]]; then
            POWER="$(curl -fsS "${AUTH[@]}" "$HS/_matrix/client/v3/rooms/$ROOM_ESC/state/m.room.power_levels/?$AS_QUERY")" || continue
            UPDATED="$(restrict_power_levels "$POWER")"
            if ! jq -e --argjson updated "$UPDATED" '.==$updated' <<<"$POWER" >/dev/null; then
                curl -fsS -X PUT "${AUTH[@]}" -H "Content-Type: application/json" --data "$UPDATED" "$HS/_matrix/client/v3/rooms/$ROOM_ESC/state/m.room.power_levels/?$AS_QUERY" >/dev/null || echo "Could not restrict unencrypted portal $ROOM" >&2
            fi
        fi
        if [[ "$PORTAL_KIND" == dm ]]; then
            UNENCRYPTED_DMS=$((UNENCRYPTED_DMS + 1))
            echo "Legacy unencrypted WhatsApp DM cannot be made MatrixRTC-eligible in-place: $ROOM" >&2
        fi
        continue
    fi

    GROUP_CANDIDATE="$(select_group_candidate "$MEMBERS" "$HINT_BODY" "$ENCRYPTION_BODY" "$BOT_MXID" "$SERVER" "$GHOST_REGEXES_JSON" 2>/dev/null || true)"
    if [[ "$PORTAL_KIND" == group && -n "$GROUP_CANDIDATE" ]]; then
        GROUP_ELIGIBLE=$((GROUP_ELIGIBLE + 1))
        POWER="$(curl -fsS "${AUTH[@]}" "$HS/_matrix/client/v3/rooms/$ROOM_ESC/state/m.room.power_levels/?$AS_QUERY")" || continue
        UPDATED="$(merge_power_levels "$POWER")"
        if jq -e --argjson updated "$UPDATED" '. == $updated' <<<"$POWER" >/dev/null 2>&1; then
            UNCHANGED=$((UNCHANGED + 1))
        else
            curl -fsS -X PUT "${AUTH[@]}" -H "Content-Type: application/json" --data "$UPDATED" \
                "$HS/_matrix/client/v3/rooms/$ROOM_ESC/state/m.room.power_levels/?$AS_QUERY" >/dev/null || { echo "Power-level update skipped for $ROOM" >&2; continue; }
            echo "Group RTC permissions: $ROOM"
            CHANGED=$((CHANGED + 1))
        fi
        continue
    fi

    [[ "$PORTAL_KIND" == dm ]] || continue
    CANDIDATE="$(select_candidate "$MEMBERS" "$HINT_BODY" "$ENCRYPTION_BODY" "$BOT_MXID" "$SERVER" "$GHOST_REGEXES_JSON" 2>/dev/null || true)"
    [[ -n "$CANDIDATE" ]] || continue
    ELIGIBLE=$((ELIGIBLE + 1))
    GHOST="$(jq -r '.ghost' <<<"$CANDIDATE")"
    HUMAN="$(jq -r '.human' <<<"$CANDIDATE")"
    printf '%s\t%s\t%s\n' "$HUMAN" "$GHOST" "$ROOM" >> "$MAP_FILE"

    POWER="$(curl -fsS "${AUTH[@]}" "$HS/_matrix/client/v3/rooms/$ROOM_ESC/state/m.room.power_levels/?$AS_QUERY")" || continue
    UPDATED="$(merge_power_levels "$POWER")"

    if jq -e --argjson updated "$UPDATED" '. == $updated' <<<"$POWER" >/dev/null 2>&1; then
        UNCHANGED=$((UNCHANGED + 1))
    else
        curl -fsS -X PUT "${AUTH[@]}" \
            -H "Content-Type: application/json" \
            --data "$UPDATED" \
            "$HS/_matrix/client/v3/rooms/$ROOM_ESC/state/m.room.power_levels/?$AS_QUERY" >/dev/null || { echo "Power-level update skipped for $ROOM" >&2; continue; }
        echo "RTC permissions: $ROOM"
        CHANGED=$((CHANGED + 1))
    fi
done < <(jq -r '.joined_rooms[]' <<<"$JOINED")

DIRECT_USERS=0
DIRECT_MAPPINGS=0
DIRECT_CHANGED=0
if [[ -s "$MAP_FILE" ]]; then
    while IFS= read -r HUMAN; do
        [[ -n "$HUMAN" ]] || continue
        HUMAN_ESC="$(jq -rn --arg v "$HUMAN" '$v|@uri')"
        DP_QUERY="user_id=$HUMAN_ESC"

        DP_WHOAMI="$(curl -fsS "${DP_AUTH[@]}" "$HS/_matrix/client/v3/account/whoami?$DP_QUERY")"
        DP_ACTUAL="$(jq -r '.user_id // empty' <<<"$DP_WHOAMI")"
        [[ "$DP_ACTUAL" == "$HUMAN" ]] || { echo "Double-puppet appservice cannot impersonate $HUMAN" >&2; exit 1; }

        DIRECT_WITH_CODE="$(curl -sS -w '\n%{http_code}' "${DP_AUTH[@]}" "$HS/_matrix/client/v3/user/$HUMAN_ESC/account_data/m.direct?$DP_QUERY")"
        DIRECT_CODE="${DIRECT_WITH_CODE##*$'\n'}"
        DIRECT_BODY="${DIRECT_WITH_CODE%$'\n'*}"
        if [[ "$DIRECT_CODE" == "404" ]]; then
            DIRECT_BODY='{}'
        elif [[ "$DIRECT_CODE" != "200" ]]; then
            echo "Failed to read m.direct for $HUMAN (HTTP $DIRECT_CODE)" >&2
            exit 1
        fi

        UPDATED_DIRECT="$DIRECT_BODY"
        USER_MAPPINGS=0
        while IFS=$'\t' read -r MAP_HUMAN MAP_GHOST MAP_ROOM; do
            [[ "$MAP_HUMAN" == "$HUMAN" ]] || continue
            UPDATED_DIRECT="$(merge_direct_mapping "$UPDATED_DIRECT" "$MAP_GHOST" "$MAP_ROOM")"
            USER_MAPPINGS=$((USER_MAPPINGS + 1))
        done < "$MAP_FILE"

        if ! jq -e --argjson updated "$UPDATED_DIRECT" '. == $updated' <<<"$DIRECT_BODY" >/dev/null 2>&1; then
            LATEST="$(curl -sS -w '\n%{http_code}' "${DP_AUTH[@]}" "$HS/_matrix/client/v3/user/$HUMAN_ESC/account_data/m.direct?$DP_QUERY")" || continue
            CODE="${LATEST##*$'\n'}"
            if [[ "$CODE" == 200 ]]; then
                UPDATED_DIRECT="${LATEST%$'\n'*}"
                while IFS=$'\t' read -r H G R; do
                    [[ "$H" != "$HUMAN" ]] || UPDATED_DIRECT="$(merge_direct_mapping "$UPDATED_DIRECT" "$G" "$R")"
                done < "$MAP_FILE"
            elif [[ "$CODE" != 404 ]]; then continue; fi
            curl -fsS -X PUT "${DP_AUTH[@]}" \
                -H "Content-Type: application/json" \
                --data "$UPDATED_DIRECT" \
                "$HS/_matrix/client/v3/user/$HUMAN_ESC/account_data/m.direct?$DP_QUERY" >/dev/null || { echo "Direct mapping update skipped for $HUMAN" >&2; continue; }
            echo "m.direct: $HUMAN ($USER_MAPPINGS WhatsApp DM mappings)"
            DIRECT_CHANGED=$((DIRECT_CHANGED + 1))
        fi
        DIRECT_USERS=$((DIRECT_USERS + 1))
        DIRECT_MAPPINGS=$((DIRECT_MAPPINGS + USER_MAPPINGS))
    done < <(cut -f1 "$MAP_FILE" | sort -u)
fi

echo "WhatsApp RTC migration complete: scanned=$SCANNED eligible=$ELIGIBLE groups=$GROUP_ELIGIBLE bot_control_restricted=$BOT_CONTROL_RESTRICTED bot_control_human_transfer=$BOT_CONTROL_HUMAN_TRANSFER bot_control_runtime_only=$BOT_CONTROL_RUNTIME_ONLY unencrypted_legacy_dms=$UNENCRYPTED_DMS power_changed=$CHANGED power_unchanged=$UNCHANGED direct_users=$DIRECT_USERS direct_changed=$DIRECT_CHANGED direct_mappings=$DIRECT_MAPPINGS"

case "${MATRIXRTC_FAIL_ON_UNENCRYPTED_DMS:-0}" in
    1|true|TRUE|yes|YES) STRICT_UNENCRYPTED=1 ;;
    *) STRICT_UNENCRYPTED=0 ;;
esac
if [[ "$STRICT_UNENCRYPTED" -eq 1 && "$UNENCRYPTED_DMS" -gt 0 ]]; then
    echo "ERROR: found $UNENCRYPTED_DMS legacy unencrypted WhatsApp 1:1 portal(s). MatrixRTC intentionally requires E2EE and Matrix cannot safely retrofit encryption into existing rooms. New portals are configured to be encrypted automatically; recreate/migrate those legacy portals separately before enabling calls for them." >&2
    exit 3
fi
