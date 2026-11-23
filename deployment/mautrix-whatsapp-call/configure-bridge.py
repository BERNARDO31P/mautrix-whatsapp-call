#!/usr/bin/env python3
"""Enable MatrixRTC, encrypted new DMs, automatic m.direct sync and double puppeting.

Edits only the required scalar/map entries in mautrix-whatsapp config.yaml and
preserves unrelated YAML values. Secrets are read from a root-only file
and are never printed. Existing unencrypted rooms are not recreated.
"""
import argparse
from datetime import datetime, timezone
from pathlib import Path
import re
import shutil

from config_files import load, dump, atomic_write, protected_mode, backup_file


def update_config(original: str, server: str, secret: str) -> str:
    cfg = load(original)
    network = cfg.get("network")
    if not isinstance(network, dict):
        raise ValueError("network mapping required")
    network.update(call_start_notices=False, experimental_whatsapp_voip=True, rtc_gateway_socket="/run/mautrix-whatsapp-call/worker.sock")
    cfg.setdefault("matrix", {})["sync_direct_chat_list"] = True
    cfg.setdefault("encryption", {}).update(allow=True, default=True)
    puppet = cfg.setdefault("double_puppet", {})
    puppet.setdefault("servers", {})
    puppet.setdefault("allow_discovery", False)
    secrets = puppet.setdefault("secrets", {})
    if secrets.get(server) not in (None, "", secret):
        raise ValueError("existing double-puppet credential differs")
    secrets[server] = secret
    return original if load(original) == cfg else dump(cfg)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, required=True, help="mautrix-whatsapp config.yaml")
    parser.add_argument("--server-name", required=True)
    parser.add_argument("--double-puppet-secret-file", type=Path, required=True)
    parser.add_argument("--apply", action="store_true", help="backup and update the file")
    args = parser.parse_args()

    if not args.config.is_file():
        raise SystemExit(f"config not found: {args.config}")
    if not args.double_puppet_secret_file.is_file():
        raise SystemExit(f"double-puppet secret file not found: {args.double_puppet_secret_file}")
    server = args.server_name.strip().lower()
    if not re.fullmatch(r"[a-z0-9](?:[a-z0-9.-]{0,251}[a-z0-9])?", server) or "." not in server or ".." in server:
        raise SystemExit("invalid server name")
    secret = args.double_puppet_secret_file.read_text().strip()
    if not secret.startswith("as_token:") or len(secret) <= len("as_token:"):
        raise SystemExit("double-puppet secret file has invalid format")

    original = args.config.read_text()
    updated = update_config(original, server, secret)
    if args.apply:
        args.config.chmod(protected_mode(args.config.stat().st_mode))
    if updated == original:
        print("WhatsApp MatrixRTC, encrypted new portals, automatic m.direct sync and double puppeting are already configured.")
        return

    print("Ready to enable WhatsApp MatrixRTC, encrypted new portals, matrix.sync_direct_chat_list and automatic appservice double puppeting.")
    if not args.apply:
        print("Dry-run only. Add --apply to create a backup and update the file.")
        return

    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    backup = args.config.with_name(args.config.name + ".backup.matrixrtc." + stamp)
    backup_file(args.config, backup)
    atomic_write(args.config, updated.encode(), args.config.stat(), protected_mode(args.config.stat().st_mode))
    print("Backup:", backup)
    print("Updated:", args.config)
    print("No services were restarted.")


if __name__ == "__main__":
    main()
