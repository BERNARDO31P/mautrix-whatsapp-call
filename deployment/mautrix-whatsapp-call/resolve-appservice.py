#!/usr/bin/env python3
import argparse
import ast
import json
import re
from pathlib import Path


def scalar_value(raw: str) -> str:
    value = raw.strip()
    if not value:
        return ""
    if value[0:1] in {"\"", "'"} and value[-1:] == value[0:1]:
        try:
            parsed = ast.literal_eval(value)
            if isinstance(parsed, str):
                return parsed
        except (SyntaxError, ValueError):
            pass
        if value[0] == "'":
            return value[1:-1].replace("''", "'")
        return value[1:-1].replace("\\\"", "\"").replace("\\\\", "\\")
    return value.split(" #", 1)[0].strip()


def top_level_scalar(path: Path, key: str) -> str | None:
    pattern = re.compile(rf"^{re.escape(key)}:\s*(.*?)\s*$")
    for line in path.read_text(encoding="utf-8").splitlines():
        match = pattern.match(line)
        if match:
            return scalar_value(match.group(1))
    return None


def nested_scalar(path: Path, wanted: tuple[str, ...]) -> str | None:
    stack: list[tuple[int, str]] = []
    key_pattern = re.compile(r"^(\s*)([A-Za-z0-9_.-]+):(?:\s*(.*?))?\s*$")
    for line in path.read_text(encoding="utf-8").splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("#") or stripped.startswith("-"):
            continue
        match = key_pattern.match(line)
        if not match:
            continue
        indent = len(match.group(1).replace("\t", "    "))
        key = match.group(2)
        raw_value = match.group(3) or ""
        while stack and indent <= stack[-1][0]:
            stack.pop()
        path_keys = tuple(item[1] for item in stack) + (key,)
        if raw_value.strip():
            if path_keys == wanted:
                return scalar_value(raw_value)
        else:
            stack.append((indent, key))
    return None


def registration_user_regexes(path: Path) -> list[str]:
    lines = path.read_text(encoding="utf-8").splitlines()
    in_namespaces = False
    in_users = False
    patterns: list[str] = []
    for line in lines:
        stripped = line.strip()
        indent = len(line) - len(line.lstrip(" "))
        if indent == 0:
            in_namespaces = stripped == "namespaces:"
            in_users = False
            continue
        if not in_namespaces:
            continue
        if stripped and indent <= 2 and stripped.endswith(":") and not stripped.startswith("-"):
            in_users = stripped == "users:"
            continue
        if not in_users:
            continue
        match = re.match(r"^\s*(?:-\s*)?regex:\s*(.*?)\s*$", line)
        if not match:
            continue
        value = scalar_value(match.group(1))
        re.compile(value)
        patterns.append(value)
    return patterns


def template_regex(template: str, server: str) -> str:
    marker = "{{.}}"
    if template.count(marker) != 1:
        raise ValueError(f"unsupported mautrix-whatsapp username_template: {template!r}")
    prefix, suffix = template.split(marker, 1)
    return rf"^@{re.escape(prefix)}.+{re.escape(suffix)}:{re.escape(server)}$"


def resolve(registration: Path, config: Path | None, server: str) -> dict:
    if not registration.is_file():
        raise FileNotFoundError(f"WhatsApp appservice registration not found: {registration}")

    as_token = top_level_scalar(registration, "as_token") or ""
    sender = top_level_scalar(registration, "sender_localpart") or ""
    patterns = registration_user_regexes(registration)

    bot = None
    template = None
    if config and config.is_file():
        bot = nested_scalar(config, ("appservice", "bot", "username"))
        template = nested_scalar(config, ("appservice", "username_template"))

    bot = bot or sender
    if not patterns and template:
        patterns = [template_regex(template, server)]

    if not as_token:
        raise ValueError(f"could not read as_token from {registration}")
    if not bot:
        raise ValueError(f"could not determine mautrix-whatsapp bot username from {config or registration}")
    if not patterns:
        raise ValueError(
            f"no usable user namespace regexes in {registration} and no appservice.username_template in {config}"
        )

    for pattern in patterns:
        re.compile(pattern)

    return {
        "as_token": as_token,
        "bot_localpart": bot,
        "ghost_regexes": patterns,
        "source": "registration" if registration_user_regexes(registration) else "username_template",
    }


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
