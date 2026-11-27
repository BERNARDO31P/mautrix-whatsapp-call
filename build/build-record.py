#!/usr/bin/env python3
"""Bind a local tested build to its immutable input snapshot and outputs.

This is local integrity evidence, not a signature or a remote attestation.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import runpy
import shutil
import stat

ROOT = Path(__file__).resolve().parent
BUILD_LOGS = ("opus-build.log", "rust-build.log", "go-build.log", "rust-tls-release.log", "worker-process.log")
TEST_LOGS = ("project-fixtures.log", "deployment-fixtures.log", "wire-interop.log",
             "go-focused.log", "go-connector.log", "meowcaller.log", "matrix-rtc-core.log",
             "matrix-rtc-call-transport.log", "matrix-rtc-call-sdk.log", "matrix-rtc-native-backends.log",
             "rust-core.log", "rust-rtc.log")
BINARIES = ("mautrix-whatsapp-call-bridge", "mautrix-whatsapp-call-worker", "mautrix-whatsapp-call-provision")
HEX = re.compile(r"[0-9a-f]{64}\Z")


def regular(path):
    if not stat.S_ISREG(path.lstat().st_mode):
        raise ValueError(f"Expected regular file: {path}")
    return path


def sha(path, nonempty=False):
    path = regular(path)
    if nonempty and path.stat().st_size == 0:
        raise ValueError(f"Empty build evidence: {path.name}")
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def inventory(root):
    layout = runpy.run_path(str(ROOT / "runtime-layout.py"))
    result = set(layout["LICENSE_FILES"]) | {layout["DEPLOY"] + p for p in layout["DEPLOY_FILES"]}
    for base in ("bridge", "rtc", "build", "bootstrap", "tests"):
        folder = root / base
        if folder.is_symlink() or not folder.is_dir():
            raise ValueError(f"Invalid source directory: {base}")
        for path in folder.rglob("*"):
            rel = path.relative_to(root)
            if "__pycache__" in rel.parts:
                continue
            if path.is_symlink():
                raise ValueError(f"Unexpected source symlink: {rel}")
            if path.is_dir():
                continue
            regular(path)
            result.add(rel.as_posix())
    for rel in sorted(result):
        path = root / rel
        if any(p.is_symlink() for p in path.parents if p != root.parent):
            raise ValueError(f"Unexpected source parent symlink: {rel}")
        yield rel, regular(path)


def source_digest(root):
    h = hashlib.sha256()
    for rel, path in inventory(root):
        h.update(rel.encode() + b"\0" + bytes.fromhex(sha(path)) + b"\n")
    return h.hexdigest()


def snapshot(source, destination):
    if destination.exists():
        raise ValueError("Build snapshot destination already exists")
    before = source_digest(source)
    destination.mkdir(mode=0o700)
    for rel, path in inventory(source):
        target = destination / rel
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(path, target)
    if source_digest(destination) != before or source_digest(source) != before:
        raise ValueError("Build source changed during snapshot")
    return before


def expected_record(source, binaries, logs, original_digest):
    if source_digest(source) != original_digest:
        raise ValueError("Build source changed after snapshot")
    return {"format": 2, "test_status": "passed", "source_sha256": original_digest,
            "versions_sha256": sha(source / "build/versions.env", True),
            "binaries": {name: sha(binaries / name, True) for name in BINARIES},
            "logs": {name: sha(logs / name, True) for name in (*BUILD_LOGS, *TEST_LOGS)}}


def no_duplicates(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("Duplicate build-record field")
        result[key] = value
    return result


def read_record(path):
    record = json.loads(regular(path).read_text(encoding="utf-8"), object_pairs_hook=no_duplicates)
    if (not isinstance(record, dict) or set(record) != {"format", "test_status", "source_sha256", "versions_sha256", "binaries", "logs"}
            or type(record["format"]) is not int or record["format"] != 2 or record["test_status"] != "passed"):
        raise ValueError("Missing or unsupported tested build record")
    def valid_digest(value):
        return isinstance(value, str) and HEX.fullmatch(value) is not None
    if not all(valid_digest(record[k]) for k in ("source_sha256", "versions_sha256")):
        raise ValueError("Invalid source digest in build record")
    for field, names in (("binaries", BINARIES), ("logs", (*BUILD_LOGS, *TEST_LOGS))):
        values = record[field]
        if not isinstance(values, dict) or set(values) != set(names) or not all(valid_digest(v) for v in values.values()):
            raise ValueError(f"Incomplete build record: {field}")
    return record


def verify_local(path, source, binaries, logs, original_digest):
    if read_record(path) != expected_record(source, binaries, logs, original_digest):
        raise ValueError("Build record does not match the current source, binaries or logs")


def verify_release(release):
    record = read_record(release / "BUILD-RECORD.json")
    if record["versions_sha256"] != sha(release / "SOURCE-VERSIONS.env", True):
        raise ValueError("Release source versions differ from build record")
    for name in BINARIES:
        if record["binaries"][name] != sha(release / "bin" / name, True):
            raise ValueError(f"Release binary differs from build record: {name}")


if __name__ == "__main__":
    p = argparse.ArgumentParser(description=__doc__)
    sub = p.add_subparsers(dest="action", required=True)
    s = sub.add_parser("snapshot")
    s.add_argument("source", type=Path); s.add_argument("destination", type=Path)
    s = sub.add_parser("digest"); s.add_argument("source", type=Path)
    for action in ("create", "verify-local"):
        s = sub.add_parser(action)
        s.add_argument("record", type=Path); s.add_argument("source", type=Path)
        s.add_argument("binaries", type=Path); s.add_argument("logs", type=Path)
        s.add_argument("original_digest")
    s = sub.add_parser("verify-release"); s.add_argument("release", type=Path)
    args = p.parse_args()
    try:
        if args.action == "snapshot":
            print(snapshot(args.source, args.destination))
        elif args.action == "digest":
            print(source_digest(args.source))
        elif args.action == "create":
            record = expected_record(args.source, args.binaries, args.logs, args.original_digest)
            with args.record.open("x", encoding="utf-8") as f:
                json.dump(record, f, indent=2, sort_keys=True); f.write("\n")
                f.flush(); os.fsync(f.fileno())
        elif args.action == "verify-local":
            verify_local(args.record, args.source, args.binaries, args.logs, args.original_digest)
        else:
            verify_release(args.release)
    except (OSError, ValueError, TypeError) as error:
        p.exit(2, f"Build record verification failed: {error}\n")
