#!/usr/bin/env python3
"""Read-only inspection of one stopped ghost's delayed membership journal."""
import argparse
import json
import os
from pathlib import Path
import re
import stat
import sys

TOKEN = re.compile(r"[0-9a-f]{32}")
FIELDS = {"token", "room_id", "slot_id", "member_id", "user_id", "device_id"}
LIMIT = 65536


def unique_object(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("duplicate JSON field")
        result[key] = value
    return result


def read_record(directory_fd, name):
    token = name.removesuffix(".identified")
    if not TOKEN.fullmatch(token):
        raise ValueError("unexpected journal filename")
    fd = os.open(name, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=directory_fd)
    with os.fdopen(fd, "rb") as stream:
        metadata = os.fstat(stream.fileno())
        mode = metadata.st_mode
        if not stat.S_ISREG(mode) or stat.S_IMODE(mode) & 0o077 or metadata.st_nlink != 1:
            raise ValueError("record is not a private regular file with a single link")
        data = stream.read(LIMIT + 1)
    if len(data) > LIMIT:
        raise ValueError("journal record exceeds inspection limit")
    record = json.loads(data, object_pairs_hook=unique_object)
    if not isinstance(record, dict) or set(record) != {"operation", "delay_id"}:
        raise ValueError("invalid journal record fields")
    operation = record["operation"]
    if not isinstance(operation, dict) or set(operation) != FIELDS:
        raise ValueError("invalid operation fields")
    if any(not isinstance(value, str) or not value for value in operation.values()) or operation["token"] != token:
        raise ValueError("invalid operation identity")
    delay_id = record["delay_id"]
    if delay_id is not None and (not isinstance(delay_id, str) or not delay_id):
        raise ValueError("invalid delayed event ID")
    if name.endswith(".identified") and delay_id is None:
        raise ValueError("interrupted ID update has no ID")
    return {"file": name, **record, "status": "interrupted-identification" if name.endswith(".identified") else ("unknown" if delay_id is None else "known")}


def open_private_directory(path, label, *, dir_fd=None):
    flags = os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW
    try:
        fd = os.open(path, flags, dir_fd=dir_fd)
    except OSError as error:
        raise ValueError(f"{label} directory is not a private real directory: {error}") from error
    try:
        meta = os.fstat(fd)
        if not stat.S_ISDIR(meta.st_mode) or stat.S_IMODE(meta.st_mode) & 0o077:
            raise ValueError(f"{label} directory is not a private real directory")
        return fd
    except BaseException:
        os.close(fd)
        raise


def inspect(identity):
    records, errors = [], []
    identity_fd = open_private_directory(identity, "identity")
    try:
        directory_fd = open_private_directory("delayed-membership-operations", "journal", dir_fd=identity_fd)
        try:
            for name in sorted(os.listdir(directory_fd)):
                try:
                    records.append(read_record(directory_fd, name))
                except (OSError, ValueError) as error:
                    errors.append({"file": name, "error": str(error)})
        finally:
            os.close(directory_fd)
    finally:
        os.close(identity_fd)
    by_token = {}
    for record in records:
        by_token.setdefault(record["operation"]["token"], []).append(record)
    for token, entries in by_token.items():
        if len(entries) > 1 and (entries[0]["operation"] != entries[1]["operation"] or
            (entries[0]["delay_id"] is not None and entries[0]["delay_id"] != entries[1]["delay_id"])):
            errors.append({"file": token, "error": "conflicting operation records"})
    return {"records": records, "errors": errors, "automatic_settlement": False}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("identity", type=Path)
    parser.add_argument("--require-empty", action="store_true",
                        help="return status 3 if unresolved records remain")
    args = parser.parse_args()
    try:
        report = inspect(args.identity)
    except (OSError, ValueError) as error:
        print(f"Cannot inspect delayed membership journal: {error}", file=sys.stderr)
        return 2
    print(json.dumps(report, indent=2, ensure_ascii=True))
    if report["errors"]:
        return 2
    if args.require_empty and report["records"]:
        return 3
    return 0


if __name__ == "__main__":
    sys.exit(main())
