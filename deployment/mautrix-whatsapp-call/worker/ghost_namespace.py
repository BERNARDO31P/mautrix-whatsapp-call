"""Shared call-only appservice identity policy for provisioning and migration."""
import re
from pathlib import Path
import yaml


class StrictLoader(yaml.SafeLoader):
    def construct_mapping(self, node, deep=False):
        seen = set()
        for key_node, _ in node.value:
            if key_node.tag == "tag:yaml.org,2002:merge":
                continue
            key = self.construct_object(key_node, deep=False)
            try:
                if key in seen:
                    raise ValueError("duplicate explicit YAML mapping key")
                seen.add(key)
            except TypeError:
                raise ValueError("unhashable YAML mapping key") from None
        return super().construct_mapping(node, deep=deep)


def read_mapping(path: Path) -> dict:
    try:
        data = yaml.load(path.read_text(encoding="utf-8"), Loader=StrictLoader)
    except (yaml.YAMLError, UnicodeError):
        raise ValueError("invalid appservice YAML") from None
    if not isinstance(data, dict):
        raise ValueError("expected appservice YAML mapping")
    return data


def username_template_pattern(template: str, server: str) -> re.Pattern:
    marker = "{{.}}"
    if not isinstance(template, str) or template.count(marker) != 1:
        raise ValueError("unsupported mautrix-whatsapp username_template")
    prefix, suffix = template.split(marker, 1)
    if not (prefix or suffix) or any(c in "@:" or c.isspace() for c in prefix + suffix):
        raise ValueError("username_template must define a distinct ghost namespace")
    if not server or any(c.isspace() or c in "@/" for c in server):
        raise ValueError("invalid Matrix server name")
    return re.compile(rf"\A@{re.escape(prefix)}[^:\s]+{re.escape(suffix)}:{re.escape(server)}(?![\s\S])")


def resolve_metadata(registration: Path, config: Path, server: str) -> dict:
    reg = read_mapping(registration)
    cfg = read_mapping(config)
    token, sender = reg.get("as_token"), reg.get("sender_localpart")
    if not isinstance(token, str) or not token or any(c.isspace() for c in token):
        raise ValueError("invalid appservice token")
    appservice = cfg.get("appservice")
    if not isinstance(appservice, dict):
        raise ValueError("missing appservice mapping")
    bot_config = appservice.get("bot", {})
    if not isinstance(bot_config, dict):
        raise ValueError("invalid appservice bot mapping")
    bot = bot_config.get("username") or sender
    if not isinstance(bot, str) or not bot or any(c in "@:" or c.isspace() for c in bot):
        raise ValueError("invalid appservice bot username")
    scope = username_template_pattern(appservice.get("username_template"), server).pattern
    namespaces = reg.get("namespaces")
    entries = namespaces.get("users") if isinstance(namespaces, dict) else None
    if not isinstance(entries, list):
        raise ValueError("missing registered user namespaces")
    patterns = []
    for entry in entries:
        if not isinstance(entry, dict) or entry.get("exclusive") is not True:
            continue
        regex = entry.get("regex")
        if not isinstance(regex, str) or not regex:
            raise ValueError("invalid exclusive user namespace")
        try:
            re.compile(regex)
            # Synapse tests registration regexes from the start of the MXID.
            # Intersect with the full configured local ghost template.
            pattern = rf"\A(?=(?:{regex})){scope[2:]}"
            re.compile(pattern)
        except re.error:
            raise ValueError("invalid exclusive user namespace regex") from None
        patterns.append(pattern)
    if not patterns:
        raise ValueError("no exclusive registered ghost namespace")
    return {"as_token": token, "bot_localpart": bot, "ghost_regexes": patterns,
            "source": "registration"}
