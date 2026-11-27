#!/usr/bin/env python3
"""Classify call migration/provisioning candidates from the complete room state."""
import json
import re
import sys

BRIDGE_TYPES = ("m.bridge", "uk.half-shot.bridge")
DM_JID = re.compile(r"[^@\s]+@(s\.whatsapp\.net|lid|hosted|hosted\.lid|bot)")
GROUP_JID = re.compile(r"[^@\s]+@g\.us")


def portal_kind(state, bot, patterns):
    if not isinstance(state, list) or not all(isinstance(e, dict) for e in state):
        return "other"
    identities = set()
    for event in state:
        if event.get("type") not in BRIDGE_TYPES:
            continue
        content = event.get("content")
        sender = event.get("sender")
        if not isinstance(content, dict) or not isinstance(sender, str):
            return "other"
        protocol, channel = content.get("protocol"), content.get("channel")
        if (content.get("bridgebot") != bot or not isinstance(protocol, dict)
                or protocol.get("id") != "whatsapp" or not isinstance(channel, dict)
                or (sender != bot and not any(p.fullmatch(sender) for p in patterns))):
            return "other"
        jid = channel.get("id")
        if not isinstance(jid, str):
            return "other"
        room_type = content.get("com.beeper.room_type.v2", content.get("com.beeper.room_type"))
        if GROUP_JID.fullmatch(jid):
            kind = "group"
        elif room_type == "dm" and DM_JID.fullmatch(jid):
            kind = "dm"
        else:
            return "other"
        identities.add((kind, jid))
    if not identities:
        return "none"
    return next(iter(identities))[0] if len(identities) == 1 else "other"


if __name__ == "__main__":
    try:
        bot, raw_patterns = sys.argv[1:]
        regexes = json.loads(raw_patterns)
        if not isinstance(regexes, list) or not all(isinstance(p, str) for p in regexes):
            raise ValueError("Invalid namespace patterns")
        print(portal_kind(json.load(sys.stdin), bot, [re.compile(p) for p in regexes]))
    except (ValueError, TypeError, re.error, UnicodeError, RecursionError):
        print("other")
