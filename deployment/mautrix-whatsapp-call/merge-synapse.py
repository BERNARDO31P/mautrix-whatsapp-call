#!/usr/bin/env python3
"""Idempotently install MatrixRTC and double-puppet appservice registrations.

Run in /opt/matrix: python3 mautrix-whatsapp-call/merge-synapse.py --apply
Existing unrelated Synapse configuration is preserved. No service is restarted.
"""
import argparse
from datetime import datetime, timezone
import os
from pathlib import Path
import re
import shutil
import stat

HERE = Path(__file__).resolve().parent
ROOT = HERE.parent
HOMESERVER = ROOT / "synapse" / "homeserver.yaml"
ADDITIONS = HERE / "synapse-additions.yaml"
REGISTRATIONS = (
    (HERE / "generated" / "matrixrtc-registration.yaml", ROOT / "synapse" / "matrixrtc-registration.yaml", "/data/matrixrtc-registration.yaml"),
    (HERE / "generated" / "matrixrtc-doublepuppet-registration.yaml", ROOT / "synapse" / "matrixrtc-doublepuppet-registration.yaml", "/data/matrixrtc-doublepuppet-registration.yaml"),
)


from config_files import load, dump, atomic_write, protected_mode, backup_file


def merge(original: str, extra: str) -> tuple[str, list[str]]:
    cfg = load(original)
    requested = load(extra)
    paths = cfg.setdefault("app_service_config_files", [])
    if not isinstance(paths, list):
        raise ValueError("app_service_config_files must be a list")
    for _, _, path in REGISTRATIONS:
        if path not in paths:
            paths.append(path)
    def merge_mapping(target, updates):
        for key, value in updates.items():
            if isinstance(value, dict):
                merge_mapping(target.setdefault(key, {}), value)
            elif key == "transports" and isinstance(value, list):
                items = target.setdefault(key, [])
                for wanted in value:
                    matches = [item for item in items if isinstance(item, dict) and item.get("type")==wanted["type"]]
                    if matches:
                        if len(matches) != 1 or any(k in matches[0] and matches[0][k] != v for k, v in wanted.items()):
                            raise ValueError("conflicting existing MatrixRTC transport: " + wanted["type"])
                        matches[0].update(wanted)
                    else:
                        items.append(wanted)
            else:
                target[key] = value
    # Existing explicit delay budgets are respected.
    if "max_event_delay_duration" in cfg:
        requested.pop("max_event_delay_duration", None)
    merge_mapping(cfg, requested)
    if cfg == load(original):
        return original, []
    return dump(cfg), ["MatrixRTC settings and registrations"]


def install_registration(source: Path, destination: Path, stamp: str, owner_stat: os.stat_result) -> bool:
    if source.is_symlink() or destination.is_symlink():
        raise ValueError("refusing symlink registration")
    content = source.read_bytes()
    st = destination.stat() if destination.exists() else owner_stat
    mode = protected_mode(st.st_mode) & protected_mode(owner_stat.st_mode)
    if destination.exists() and content == destination.read_bytes():
        os.chown(destination,owner_stat.st_uid,owner_stat.st_gid)
        destination.chmod(mode)
        return False
    if destination.exists():
        backup_file(destination, destination.with_name(destination.name + ".backup." + stamp))
    atomic_write(destination,content,owner_stat,mode)
    return True


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--apply", action="store_true", help="Backup/write config and registrations; no restart")
    options = parser.parse_args()

    if not HOMESERVER.is_file() or HOMESERVER.is_symlink() or not ADDITIONS.is_file():
        raise SystemExit("Missing Synapse config or MatrixRTC additions")
    for source, _, _ in REGISTRATIONS:
        if not source.is_file():
            raise SystemExit(f"Missing {source}. Run mautrix-whatsapp-call/prepare.sh first.")

    original = HOMESERVER.read_text()
    updated, changes = merge(original, ADDITIONS.read_text())
    print("Synapse MatrixRTC merge check:", HOMESERVER)
    print("Changes:", ", ".join(changes) if changes else "none")
    if not options.apply:
        print("Dry-run only. Add --apply to write changes and registrations.")
        return

    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
    owner_stat = HOMESERVER.stat()
    # Install registrations first; a partial failure must not make Synapse
    # reference registration files that have never been written.
    for source, destination, _ in REGISTRATIONS:
        changed = install_registration(source, destination, stamp, owner_stat)
        print(("Installed: " if changed else "Registration already current: ") + str(destination))
    if updated != original:
        backup = HOMESERVER.with_name("homeserver.yaml.backup." + stamp)
        backup_file(HOMESERVER, backup)
        atomic_write(HOMESERVER, updated.encode(), owner_stat, stat.S_IMODE(owner_stat.st_mode))
        print("Backup:", backup)

    print("Synapse files updated. Validate before restarting; no services were restarted.")


if __name__ == "__main__":
    main()
