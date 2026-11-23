#!/usr/bin/env bash
set -euo pipefail

candidate_from_room_json() {
    local members_json="$1"
    local hints_json="$2"
    local human_mxid="$3"
    local ghost_regexes_json="$4"

    jq -rn \
        --argjson members "$members_json" \
        --argjson hints "$hints_json" \
        --argjson ghost_regexes "$ghost_regexes_json" \
        --arg human "$human_mxid" '
        def is_ghost($u): any($ghost_regexes[]; . as $rx | ($u | test($rx)));
        ($hints.service_members // []) as $service
        | ($members.joined | keys) as $joined
        | [$joined[] as $u | select(($service | index($u)) == null) | $u] as $humanMembers
        | [$humanMembers[] as $u | select(is_ghost($u)) | $u] as $ghosts
        | if (($humanMembers | index($human)) != null and ($humanMembers|length) == 2 and ($ghosts|length) == 1)
          then $ghosts[0]
          else empty
          end
    '
}

merge_direct_mapping() {
    local direct_json="$1"
    local ghost_mxid="$2"
    local room_id="$3"

    jq -c \
        --arg ghost "$ghost_mxid" \
        --arg room "$room_id" \
        '.[$ghost] = (((.[$ghost] // []) + [$room]) | unique)' \
        <<<"$direct_json"
}

run_self_test() {
    local human='@bernardo:example.org'
    local whatsapp='@whatsapp_lid-123:example.org'
    local service='@whatsappbot:example.org'
    local wa_room='!wa:example.org'

    local wa_members='{"joined":{"@bernardo:example.org":{},"@whatsapp_lid-123:example.org":{},"@whatsappbot:example.org":{}}}'
    local wa_hints='{"service_members":["@whatsappbot:example.org"]}'
    local group_members='{"joined":{"@bernardo:example.org":{},"@whatsapp_lid-456:example.org":{},"@mallory:example.org":{}}}'
    local group_hints='{"service_members":[]}'
    local direct='{"@existing:example.org":["!existing:example.org"]}'
    local ghost_regexes='["^@whatsapp_.+:example\\.org$"]'

    local candidate
    candidate="$(candidate_from_room_json "$wa_members" "$wa_hints" "$human" "$ghost_regexes")"
    [[ "$candidate" == "$whatsapp" ]] || {
        echo "self-test: service-member WhatsApp DM was not recognized" >&2
        exit 1
    }

    candidate="$(candidate_from_room_json "$group_members" "$group_hints" "$human" "$ghost_regexes")"
    [[ -z "$candidate" ]] || {
        echo "self-test: real third human was incorrectly ignored" >&2
        exit 1
    }

    local custom_members='{"joined":{"@bernardo:example.org":{},"@wa-custom-123:example.org":{},"@whatsappbot:example.org":{}}}'
    local custom_regexes='["^@wa-custom-.+:example\\.org$"]'
    candidate="$(candidate_from_room_json "$custom_members" "$wa_hints" "$human" "$custom_regexes")"
    [[ "$candidate" == '@wa-custom-123:example.org' ]] || {
        echo "self-test: custom WhatsApp ghost template was not recognized" >&2
        exit 1
    }

    local merged
    merged="$(merge_direct_mapping "$direct" "$whatsapp" "$wa_room")"

    jq -e \
        --arg existing_room '!existing:example.org' \
        --arg ghost "$whatsapp" \
        --arg wa_room "$wa_room" '
        .["@existing:example.org"] == [$existing_room]
        and .[$ghost] == [$wa_room]
        and has("@whatsapp_lid-456:example.org") == false
    ' <<<"$merged" >/dev/null || {
        echo "self-test: m.direct merge result was incorrect: $merged" >&2
        exit 1
    }

    echo "sync-direct-chats self-test: PASS"
}

if [[ "${1:-}" == "--self-test" ]]; then
    command -v jq >/dev/null
    run_self_test
    exit 0
fi

if [[ $# -lt 1 || $# -gt 2 ]]; then
    echo "Usage: $0 <@human:server> [https://matrix.example]" >&2
    echo "       $0 --self-test" >&2
    exit 2
fi

USER_MXID="$1"
HS="${2:-https://${USER_MXID#*:}}"

[[ "$USER_MXID" == @*:* ]] || { echo "Human Matrix ID must be a full MXID" >&2; exit 2; }
command -v curl >/dev/null
command -v jq >/dev/null
command -v python3 >/dev/null
ROOT="${MATRIX_ROOT:-/opt/matrix}"
REGISTRATION="${MATRIXRTC_WHATSAPP_REGISTRATION:-$ROOT/mautrix-whatsapp/registration.yaml}"
CONFIG="${MATRIXRTC_WHATSAPP_CONFIG:-$ROOT/mautrix-whatsapp/config.yaml}"
METADATA_HELPER="${MATRIXRTC_WHATSAPP_METADATA_HELPER:-$ROOT/mautrix-whatsapp-call/resolve-appservice.py}"
[[ -f "$REGISTRATION" ]] || { echo "WhatsApp appservice registration not found: $REGISTRATION" >&2; exit 2; }
[[ -f "$METADATA_HELPER" ]] || { echo "WhatsApp metadata helper not found: $METADATA_HELPER" >&2; exit 2; }
SERVER="${USER_MXID#*:}"
META_JSON="$(python3 "$METADATA_HELPER" --registration "$REGISTRATION" --config "$CONFIG" --server "$SERVER")"
GHOST_REGEXES_JSON="$(jq -c '.ghost_regexes // []' <<<"$META_JSON")"
[[ "$GHOST_REGEXES_JSON" != "[]" ]] || { echo "No usable WhatsApp ghost namespace patterns found" >&2; exit 2; }

read -rsp "Matrix access token for $USER_MXID: " TOKEN
echo
trap 'unset TOKEN' EXIT
[[ -n "$TOKEN" ]] || { echo "Access token is empty" >&2; exit 2; }

AUTH=( -H "Authorization: Bearer $TOKEN" )
USER_ESC="$(jq -rn --arg v "$USER_MXID" '$v|@uri')"

WHOAMI="$(curl -fsS "${AUTH[@]}" "$HS/_matrix/client/v3/account/whoami")"
ACTUAL="$(jq -r '.user_id // empty' <<<"$WHOAMI")"
[[ "$ACTUAL" == "$USER_MXID" ]] || {
    echo "Token belongs to $ACTUAL, expected $USER_MXID" >&2
    exit 1
}

DIRECT="$(curl -sS -w '\n%{http_code}' "${AUTH[@]}" \
    "$HS/_matrix/client/v3/user/$USER_ESC/account_data/m.direct")"
DIRECT_CODE="${DIRECT##*$'\n'}"
DIRECT_BODY="${DIRECT%$'\n'*}"
if [[ "$DIRECT_CODE" == "404" ]]; then
    DIRECT_BODY='{}'
elif [[ "$DIRECT_CODE" != "200" ]]; then
    echo "Failed to read m.direct (HTTP $DIRECT_CODE): $DIRECT_BODY" >&2
    exit 1
fi

JOINED="$(curl -fsS "${AUTH[@]}" "$HS/_matrix/client/v3/joined_rooms")"
UPDATED="$DIRECT_BODY"
FOUND=0

while IFS= read -r ROOM; do
    ROOM_ESC="$(jq -rn --arg v "$ROOM" '$v|@uri')"
    MEMBERS="$(curl -fsS "${AUTH[@]}" "$HS/_matrix/client/v3/rooms/$ROOM_ESC/joined_members" 2>/dev/null)" || continue

    # Mautrix encrypted portal DMs have the human, one @whatsapp_* ghost and
    # the bridge bot. Only service members declared by io.element.functional_members
    # are ignored; arbitrary third users remain a hard stop.
    HINTS="$(curl -sS -w '\n%{http_code}' "${AUTH[@]}" \
        "$HS/_matrix/client/v3/rooms/$ROOM_ESC/state/io.element.functional_members/" 2>/dev/null || true)"
    HINT_CODE="${HINTS##*$'\n'}"
    HINT_BODY="${HINTS%$'\n'*}"
    if [[ "$HINT_CODE" != "200" ]]; then
        HINT_BODY='{"service_members":[]}'
    fi

    CANDIDATE="$(candidate_from_room_json "$MEMBERS" "$HINT_BODY" "$USER_MXID" "$GHOST_REGEXES_JSON" 2>/dev/null || true)"
    [[ -n "$CANDIDATE" ]] || continue

    UPDATED="$(merge_direct_mapping "$UPDATED" "$CANDIDATE" "$ROOM")"
    echo "DM: $CANDIDATE -> $ROOM"
    FOUND=$((FOUND + 1))
done < <(jq -r '.joined_rooms[]' <<<"$JOINED")

if [[ "$FOUND" -eq 0 ]]; then
    echo "No eligible WhatsApp 1:1 portal rooms found; m.direct was not changed."
    exit 0
fi

curl -fsS -X PUT "${AUTH[@]}" \
    -H "Content-Type: application/json" \
    --data "$UPDATED" \
    "$HS/_matrix/client/v3/user/$USER_ESC/account_data/m.direct" >/dev/null

echo "Updated m.direct with $FOUND WhatsApp 1:1 portal room(s)."
echo "Force-stop and reopen Element X so it refreshes DM/call eligibility."
