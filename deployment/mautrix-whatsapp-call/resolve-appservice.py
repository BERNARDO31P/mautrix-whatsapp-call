#!/usr/bin/env python3
import argparse
import json
import re
from pathlib import Path


from config_files import load


def template_regex(template: str, server: str) -> str:
    marker = "{{.}}"
    if template.count(marker) != 1:
        raise ValueError("unsupported mautrix-whatsapp username_template")
    prefix, suffix = template.split(marker, 1)
    if not (prefix or suffix) or any(c in prefix + suffix for c in "@:\r\n"):
        raise ValueError("username_template must define a distinct ghost namespace")
    return rf"^@{re.escape(prefix)}[^:]+{re.escape(suffix)}:{re.escape(server)}$"


def resolve(registration: Path, config: Path | None, server: str) -> dict:
    reg = load(registration.read_text(encoding="utf-8"))
    token, sender = reg.get("as_token"), reg.get("sender_localpart")
    if not isinstance(token, str) or not token or any(c.isspace() for c in token):
        raise ValueError("invalid appservice token")
    entries = reg.get("namespaces", {}).get("users", [])
    patterns = [entry["regex"] for entry in entries
                if isinstance(entry, dict) and entry.get("exclusive") is True
                and isinstance(entry.get("regex"), str)]
    bot = sender
    template = None
    if config is not None:
        cfg = load(config.read_text(encoding="utf-8"))
        appservice = cfg.get("appservice", {})
        bot = appservice.get("bot", {}).get("username") or sender
        template = appservice.get("username_template")
        if not isinstance(template, str):
            raise ValueError("appservice.username_template is required")
    if not isinstance(bot, str) or not bot or any(c in bot for c in "@:\r\n"):
        raise ValueError("invalid appservice bot username")
    source = "registration" if patterns else "username_template"
    if template:
        scope = template_regex(template, server)
        patterns = [rf"^(?=(?:{p})){scope[1:]}" for p in patterns] or [scope]
    else:
        # Without a config only explicitly exclusive, anchored namespaces are usable.
        patterns = [p for p in patterns if p.startswith("^@") and p.endswith("$")]
    if not patterns:
        raise ValueError("no usable ghost namespace")
    for pattern in patterns:
        re.compile(pattern)
    return {"as_token": token, "bot_localpart": bot, "ghost_regexes": patterns, "source": source}


def self_test() -> None:
    import tempfile

    with tempfile.TemporaryDirectory() as temp:
        root = Path(temp)
        reg = root / "registration.yaml"
        cfg = root / "config.yaml"
        reg.write_text(
            "id: whatsapp\n"
            "as_token: test-token\n"
            "sender_localpart: intentionally-not-the-bot\n"
            "namespaces:\n"
            "  users: []\n",
            encoding="utf-8",
        )
        cfg.write_text(
            "appservice:\n"
            "  bot:\n"
            "    username: customwhatsappbot\n"
            "  username_template: wa-custom-{{.}}\n",
            encoding="utf-8",
        )
        result = resolve(reg, cfg, "example.org")
        assert result["as_token"] == "test-token"
        assert result["bot_localpart"] == "customwhatsappbot"
        assert result["source"] == "username_template"
        regex = re.compile(result["ghost_regexes"][0])
        assert regex.fullmatch("@wa-custom-lid-123:example.org")
        assert not regex.fullmatch("@whatsapp_lid-123:example.org")

        reg.write_text(
            "id: whatsapp\n"
            "as_token: quoted-token\n"
            "sender_localpart: whatsappbot\n"
            "namespaces:\n"
            "  users:\n"
            "    - regex: '^@whatsapp_.+:example\\\\.org$'\n"
            "      exclusive: true\n",
            encoding="utf-8",
        )
        # JSON is also valid YAML, including flow-style mappings.
        reg.write_text(json.dumps({"as_token":"test-token", "sender_localpart":"customwhatsappbot", "namespaces":{"users":[{"exclusive":True,"regex":"^@.*$"}]}}))
        scoped = resolve(reg, cfg, "example.org")["ghost_regexes"][0]
        assert re.fullmatch(scoped, "@wa-custom-123:example.org")
        assert not re.fullmatch(scoped, "@alice:example.org")
        reg.write_text(json.dumps({"as_token":"quoted-token", "sender_localpart":"whatsappbot", "namespaces":{"users":[{"exclusive":True,"regex":r"^@whatsapp_.+:example\.org$"}]}}))
        result = resolve(reg, None, "example.org")
        assert result["bot_localpart"] == "whatsappbot"
        assert result["source"] == "registration"
        assert re.compile(result["ghost_regexes"][0]).fullmatch("@whatsapp_123:example.org")

    print("resolve-appservice self-test: PASS")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--registration", type=Path)
    parser.add_argument("--config", type=Path)
    parser.add_argument("--server")
    parser.add_argument("--self-test", action="store_true")
    args = parser.parse_args()

    if args.self_test:
        self_test()
        return
    if not args.registration or not args.server:
        parser.error("--registration and --server are required unless --self-test is used")

    try:
        result = resolve(args.registration, args.config, args.server)
    except (OSError, ValueError, re.error) as exc:
        raise SystemExit(str(exc)) from exc
    print(json.dumps(result, separators=(",", ":")))


if __name__ == "__main__":
    main()
