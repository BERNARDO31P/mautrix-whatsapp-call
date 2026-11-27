#!/usr/bin/env python3
import argparse
import json
import re
from pathlib import Path


import sys
sys.path.insert(0, str(Path(__file__).resolve().parent / "worker"))
from ghost_namespace import resolve_metadata as resolve


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
        try:
            resolve(reg, cfg, "example.org")
        except ValueError:
            pass
        else:
            raise AssertionError("template alone must not grant ownership")
        reg.write_text(json.dumps({"as_token":"test-token", "sender_localpart":"customwhatsappbot", "namespaces":{"users":[{"exclusive":True,"regex":"^@.*$"}]}}))
        result = resolve(reg, cfg, "example.org")
        assert result["as_token"] == "test-token"
        assert result["bot_localpart"] == "customwhatsappbot"
        assert result["source"] == "registration"
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
        cfg.write_text(json.dumps({"appservice": {"username_template": "whatsapp_{{.}}"}}))
        result = resolve(reg, cfg, "example.org")
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
    if not args.registration or not args.config or not args.server:
        parser.error("--registration, --config and --server are required unless --self-test is used")

    try:
        result = resolve(args.registration, args.config, args.server)
    except (OSError, ValueError, re.error) as exc:
        raise SystemExit(str(exc)) from exc
    print(json.dumps(result, separators=(",", ":")))


if __name__ == "__main__":
    main()
