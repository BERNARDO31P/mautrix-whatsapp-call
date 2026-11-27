#!/usr/bin/env python3
"""Read-only configuration prevalidation before pausing the call stack."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import runpy
import stat
import sys

sys.dont_write_bytecode = True
HERE = Path(__file__).resolve().parent


def validate(root, server, applied=False):
    # Use the same transformations as the actual installer, not approximations.
    render = runpy.run_path(str(HERE / "render-hostname.py"))["plan_render"]
    credentials = runpy.run_path(str(HERE / "reconcile-credentials.py"))["plan_credentials"]
    update_bridge = runpy.run_path(str(HERE / "configure-bridge.py"))["update_config"]
    merge_synapse = runpy.run_path(str(HERE / "merge-synapse.py"))["merge"]
    root = Path(root)
    for folder in (root, root / "bridge", root / "worker", root / "generated", root / "worker-state", root.parent / "synapse", root.parent / "mautrix-whatsapp"):
        if folder.is_symlink() or (folder.exists() and not folder.is_dir()):
            raise ValueError("invalid deployment directory")
    for path in (root.parent / "synapse/homeserver.yaml", root.parent / "mautrix-whatsapp/config.yaml"):
        if path.is_symlink() or not path.is_file():
            raise ValueError("missing or invalid existing configuration")
    for base, names in ((root / "generated", ("livekit.yaml", "matrixrtc-registration.yaml", "matrixrtc-doublepuppet-registration.yaml", "double_puppet_secret", "as_token", "hs_token", "double_puppet_as_token", "double_puppet_hs_token", "livekit_secret", "matrixrtc_store_passphrase")),
                        (root.parent / "synapse", ("matrixrtc-registration.yaml", "matrixrtc-doublepuppet-registration.yaml"))):
        for name in names:
            path = base / name
            if path.is_symlink() or (path.exists() and not stat.S_ISREG(path.stat().st_mode)):
                raise ValueError("invalid credential or registration path")
    updates = dict(render(root, server))
    additions = root / "synapse-additions.yaml"
    candidates = credentials(root, server)
    # A new token has no existing value to match. The placeholder is only used
    # in memory and cannot become a real credential or be written to disk.
    token = next(iter(candidates["double_puppet_as_token"]), "candidate-preview-only")
    bridge_text = (root.parent / "mautrix-whatsapp/config.yaml").read_text()
    synapse_text = (root.parent / "synapse/homeserver.yaml").read_text()
    bridge_updated = update_bridge(bridge_text, server.strip().lower(), "as_token:" + token)
    synapse_updated, _ = merge_synapse(synapse_text, updates.get(additions, additions.read_text()))
    if applied:
        from config_files import load
        if updates or bridge_updated != bridge_text or synapse_updated != synapse_text:
            raise ValueError("configuration application incomplete")
        generated = root / "generated"
        for name, values in candidates.items():
            path = generated / name
            if len(values) != 1 or not path.is_file() or path.read_text().strip() not in values:
                raise ValueError("canonical credential missing")
        def secret(name):
            return (generated / name).read_text().strip()
        if (generated / "double_puppet_secret").read_text().strip() != "as_token:" + secret("double_puppet_as_token"):
            raise ValueError("double puppet credential not applied")
        for prefix, name in (("", "matrixrtc-registration.yaml"),
                             ("double_puppet_", "matrixrtc-doublepuppet-registration.yaml")):
            source = (generated / name).read_bytes()
            if source != (root.parent / "synapse" / name).read_bytes():
                raise ValueError("installed registration differs from generated registration")
            registration = load(source.decode())
            for key in ("as_token", "hs_token"):
                if registration.get(key) != secret(prefix + key):
                    raise ValueError("registration credential missing or inconsistent")
        livekit = load((generated / "livekit.yaml").read_text())
        if livekit.get("keys", {}).get("lk-matrixrtc") != secret("livekit_secret"):
            raise ValueError("LiveKit credential not applied")


# Only live service configuration is covered. Generated credentials are kept
# intact so retrying an installation cannot rotate an existing identity.
LIVE_CONFIG = ("mautrix-whatsapp/config.yaml", "synapse/homeserver.yaml",
               "synapse/matrixrtc-registration.yaml", "synapse/matrixrtc-doublepuppet-registration.yaml")


def config_state(path):
    if path.parent.is_symlink() or path.is_symlink():
        raise ValueError("invalid live configuration path")
    try:
        st = path.stat()
    except FileNotFoundError:
        return None, None
    if not stat.S_ISREG(st.st_mode):
        raise ValueError("invalid live configuration file")
    return path.read_bytes(), st


def sync_directory(path):
    fd = os.open(path, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def snapshot_live(root, server, journal):
    from config_files import atomic_write, protected_mode
    root, journal = Path(root), Path(journal)
    if journal.is_symlink() or not journal.is_dir() or stat.S_IMODE(journal.stat().st_mode) != 0o700 or any(journal.iterdir()):
        raise ValueError("invalid rollback journal")
    update = runpy.run_path(str(HERE / "configure-bridge.py"))["update_config"]
    merge = runpy.run_path(str(HERE / "merge-synapse.py"))["merge"]
    originals = [config_state(root.parent / rel) for rel in LIVE_CONFIG]
    secret = (root / "generated/double_puppet_secret").read_text().strip()
    expected = [update(originals[0][0].decode(), server.strip().lower(), secret).encode(),
                merge(originals[1][0].decode(), (root / "synapse-additions.yaml").read_text())[0].encode()]
    expected.extend((root / "generated" / Path(rel).name).read_bytes() for rel in LIVE_CONFIG[2:])
    entries = []
    hs_stat = originals[1][1]
    for index, ((content, st), planned) in enumerate(zip(originals, expected)):
        owner = st if index < 2 else hs_stat
        mode = (protected_mode(st.st_mode) if index == 0 else
                stat.S_IMODE(st.st_mode) if index == 1 else
                protected_mode((st or hs_stat).st_mode) & protected_mode(hs_stat.st_mode))
        if content is not None:
            atomic_write(journal / str(index), content)
        entries.append({"present": content is not None, "uid": st.st_uid if st else None,
                        "gid": st.st_gid if st else None, "mode": stat.S_IMODE(st.st_mode) if st else None,
                        "original": hashlib.sha256(content).hexdigest() if content is not None else None,
                        "planned": hashlib.sha256(planned).hexdigest(),
                        "planned_metadata": [owner.st_uid, owner.st_gid, mode]})
    # The manifest is committed last, before the first live file is modified.
    atomic_write(journal / "manifest.json", json.dumps(entries).encode())
    # Persist the new journal directory entry before permitting live writes.
    # A failed parent flush is a failed snapshot, not a usable transaction.
    sync_directory(journal.parent)


def mark_restart(root, journal):
    """Persist the boundary before a service can consume new configuration."""
    from config_files import atomic_write
    root, journal = Path(root), Path(journal)
    if journal.is_symlink() or not journal.is_dir() or stat.S_IMODE(journal.stat().st_mode) != 0o700:
        raise ValueError("invalid rollback journal")
    marker = journal / "restart-started"
    if marker.exists() or marker.is_symlink():
        raise ValueError("restart already marked")
    # Never cross the restart boundary with an unusable recovery snapshot.
    entries, _ = load_journal_snapshot(journal)
    # Compose checks and other work can take time after --applied. Require
    # the live configuration to still be the exact journalled candidate at
    # the last pre-restart boundary, including owner and permissions.
    for rel, entry in zip(LIVE_CONFIG, entries):
        content, st = config_state(root.parent / rel)
        if (content is None or hashlib.sha256(content).hexdigest() != entry["planned"]
                or [st.st_uid, st.st_gid, stat.S_IMODE(st.st_mode)] != entry["planned_metadata"]):
            raise ValueError("live configuration differs from restart plan")
    atomic_write(marker, b"Service restart may have begun. Manual recovery required.\n")
    # Persist the journal directory entry as well as the marker itself.
    sync_directory(journal.parent)


def read_journal_file(path):
    if path.is_symlink():
        raise ValueError("invalid rollback journal file")
    st = path.stat()
    if not stat.S_ISREG(st.st_mode) or stat.S_IMODE(st.st_mode) != 0o600:
        raise ValueError("invalid rollback journal file")
    return path.read_bytes()


def validate_journal_entries(entries):
    def digest(value):
        return (isinstance(value, str) and len(value) == 64
                and all(c in "0123456789abcdef" for c in value))
    def integer(value, maximum):
        return type(value) is int and 0 <= value <= maximum
    if not isinstance(entries, list) or len(entries) != len(LIVE_CONFIG):
        raise ValueError("invalid rollback manifest")
    for entry in entries:
        if (not isinstance(entry, dict) or set(entry) !=
                {"present", "uid", "gid", "mode", "original", "planned", "planned_metadata"}
                or type(entry["present"]) is not bool or not digest(entry["planned"])):
            raise ValueError("invalid rollback manifest entry")
        planned = entry["planned_metadata"]
        if (not isinstance(planned, list) or len(planned) != 3
                or not all(integer(value, maximum) for value, maximum in zip(planned, (0xfffffffe, 0xfffffffe, 0o7777)))):
            raise ValueError("invalid planned rollback metadata")
        if entry["present"]:
            if (not digest(entry["original"]) or not integer(entry["uid"], 0xfffffffe)
                    or not integer(entry["gid"], 0xfffffffe) or not integer(entry["mode"], 0o7777)):
                raise ValueError("invalid rollback manifest metadata")
        elif any(entry[key] is not None for key in ("uid", "gid", "mode", "original")):
            raise ValueError("invalid absent-file rollback metadata")



def load_journal_snapshot(journal):
    entries = json.loads(read_journal_file(journal / "manifest.json"))
    validate_journal_entries(entries)
    saved = []
    for index, entry in enumerate(entries):
        content = read_journal_file(journal / str(index)) if entry["present"] else None
        if (hashlib.sha256(content).hexdigest() if content is not None else None) != entry["original"]:
            raise ValueError("rollback snapshot is incomplete")
        saved.append(content)
    return entries, saved


def restore_live(root, journal):
    from config_files import atomic_write
    from types import SimpleNamespace
    root, journal = Path(root), Path(journal)
    if journal.is_symlink() or not journal.is_dir() or stat.S_IMODE(journal.stat().st_mode) != 0o700:
        raise ValueError("invalid rollback journal")
    marker = journal / "restart-started"
    if marker.exists() or marker.is_symlink():
        raise ValueError("service restart may have begun; manual recovery required")
    entries, saved = load_journal_snapshot(journal)
    # Check every current file before restoring any. An unrelated content
    # change is not ours to overwrite. Retain the journal for manual recovery.
    def check(path, entry, index):
        current, st = config_state(path)
        digest = hashlib.sha256(current).hexdigest() if current is not None else None
        if digest not in (entry["original"], entry["planned"]):
            raise ValueError("live configuration changed outside installer")
        if st is not None:
            actual = [st.st_uid, st.st_gid, stat.S_IMODE(st.st_mode)]
            original = [entry["uid"], entry["gid"], entry["mode"]]
            planned = entry["planned_metadata"]
            allowed = []
            if digest == entry["original"]:
                allowed.append(original)
                # configure-bridge chmods before replacing content.
                if index == 0:
                    allowed.append(planned)
            if digest == entry["planned"]:
                allowed.append(planned)
                # Existing identical registration: chown precedes chmod.
                if index >= 2 and digest == entry["original"]:
                    allowed.append([planned[0], planned[1], original[2]])
            if actual not in allowed:
                raise ValueError("live configuration metadata changed outside installer")
        return current, st
    for index, (rel, entry) in enumerate(zip(LIVE_CONFIG, entries)):
        check(root.parent / rel, entry, index)
    for index, (rel, entry, content) in enumerate(zip(LIVE_CONFIG, entries, saved)):
        path = root.parent / rel
        current, st = check(path, entry, index)
        if content is None:
            if current is not None:
                path.unlink()
                fd = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY)
                try:
                    os.fsync(fd)
                finally:
                    os.close(fd)
        else:
            if current != content or (st.st_uid, st.st_gid, stat.S_IMODE(st.st_mode)) != (entry["uid"], entry["gid"], entry["mode"]):
                atomic_write(path, content, SimpleNamespace(st_uid=entry["uid"], st_gid=entry["gid"]), entry["mode"])


def fingerprint(root):
    """Hash candidate inputs in memory; never disclose their contents."""
    root = Path(root)
    matrix = root.parent
    paths = [matrix / name for name in (".env", "docker-compose.yml",
             "synapse/homeserver.yaml", "mautrix-whatsapp/config.yaml",
             "synapse/matrixrtc-registration.yaml", "synapse/matrixrtc-doublepuppet-registration.yaml")]
    registration = os.environ.get("MATRIXRTC_WHATSAPP_REGISTRATION_PATH")
    if registration:
        paths.append(Path(registration))
    paths.extend(root / name for name in ("compose.yml", "synapse-additions.yaml",
                 "verify.sh", "provision-ghost.sh", "nginx-443-locations.conf"))
    paths.extend(root / "generated" / name for name in ("livekit.yaml",
                 "matrixrtc-registration.yaml", "matrixrtc-doublepuppet-registration.yaml",
                 "double_puppet_secret", "as_token", "hs_token", "double_puppet_as_token",
                 "double_puppet_hs_token", "livekit_secret", "matrixrtc_store_passphrase"))
    # Include the exact Docker COPY inputs, recipes and ignore files, so a
    # concurrent export/upload cannot silently replace the validated payload.
    for directory, names in (
            ("bridge", ("Dockerfile", ".dockerignore", "mautrix-whatsapp-call-bridge")),
            ("worker", ("Dockerfile", ".dockerignore", "mautrix-whatsapp-call-worker",
                        "mautrix-whatsapp-call-provision", "portal_kind.py", "ghost_namespace.py",
                        "session_files.py", "provision-ghost.py", "auto-provision-whatsapp-ghosts.py"))):
        if (root / directory).is_symlink():
            raise ValueError("invalid build directory")
        paths.extend(root / directory / name for name in names)
    # These scripts implement validation and subsequent configuration writes.
    paths.extend(root / name for name in ("preflight.sh", "install-or-upgrade.sh",
                 "validate-candidate.py", "config_files.py", "configure-bridge.py",
                 "merge-synapse.py", "prepare.sh", "reconcile-credentials.py", "render-hostname.py"))
    digest = hashlib.sha256()
    for path in paths:
        if path.is_symlink():
            raise ValueError("invalid candidate input")
        try:
            before = path.stat()
        except FileNotFoundError:
            digest.update(b"missing\x00")
            continue
        if not stat.S_ISREG(before.st_mode):
            raise ValueError("invalid candidate input")
        content = hashlib.sha256()
        with path.open("rb") as stream:
            for chunk in iter(lambda: stream.read(65536), b""):
                content.update(chunk)
        after = path.stat()
        def identity(info):
            return (info.st_dev, info.st_ino, info.st_size, info.st_mtime_ns, info.st_ctime_ns)
        if path.is_symlink() or identity(before) != identity(after):
            raise ValueError("candidate changed while reading")
        digest.update(b"present\x00" + content.digest())
    return digest.hexdigest()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--server-name", required=True)
    parser.add_argument("--fingerprint", action="store_true", help="emit only the validated input digest for installer comparison")
    parser.add_argument("--applied", action="store_true", help="verify persisted configuration and credential copies before restarting services")
    transaction = parser.add_mutually_exclusive_group()
    transaction.add_argument("--snapshot-live", type=Path)
    transaction.add_argument("--restore-live", type=Path)
    transaction.add_argument("--mark-restart", type=Path)
    args = parser.parse_args()
    if args.snapshot_live or args.restore_live or args.mark_restart:
        try:
            if args.snapshot_live:
                snapshot_live(HERE, args.server_name, args.snapshot_live)
            elif args.restore_live:
                restore_live(HERE, args.restore_live)
            else:
                validate(HERE, args.server_name, applied=True)
                mark_restart(HERE, args.mark_restart)
        except (ValueError, OSError, TypeError, AttributeError, KeyError, RuntimeError):
            raise SystemExit("Live configuration journal operation failed; preserve journal for recovery.") from None
        print("Live configuration journal operation: PASS")
        sys.exit(0)
    try:
        before = fingerprint(HERE) if args.fingerprint else None
        validate(HERE, args.server_name, applied=args.applied)
        if args.fingerprint and before != fingerprint(HERE):
            raise ValueError("candidate changed during validation")
    except (ValueError, OSError, TypeError, AttributeError, KeyError, RuntimeError):
        # YAML values and credential contents must not be written to logs.
        if args.applied:
            raise SystemExit("MatrixRTC applied configuration is incomplete or inconsistent; refusing service restart. Calls remain paused.") from None
        raise SystemExit("MatrixRTC candidate configuration is invalid; no call services have been paused. Review existing configuration, templates and surviving credentials.") from None
    print(before if args.fingerprint else "MatrixRTC candidate configuration: PASS (read-only)")
