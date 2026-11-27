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


def read_record(path):
    token = path.name.removesuffix(".identified")
    if not TOKEN.fullmatch(token):
        raise ValueError("unexpected journal filename")
    fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    with os.fdopen(fd, "rb") as stream:
        mode = os.fstat(stream.fileno()).st_mode
        if not stat.S_ISREG(mode) or stat.S_IMODE(mode) & 0o077:
            raise ValueError("record is not a private regular file")
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
    if path.name.endswith(".identified") and delay_id is None:
        raise ValueError("interrupted ID update has no ID")
    return {"file": path.name, **record, "status": "interrupted-identification" if path.name.endswith(".identified") else ("unknown" if delay_id is None else "known")}


def inspect(identity):
    directory = identity / "delayed-membership-operations"
    meta = directory.lstat()
    if not stat.S_ISDIR(meta.st_mode) or stat.S_IMODE(meta.st_mode) & 0o077:
        raise ValueError("journal directory is not a private real directory")
    records, errors = [], []
    for path in sorted(directory.iterdir()):
        try:
            records.append(read_record(path))
        except (OSError, ValueError) as error:
            errors.append({"file": path.name, "error": str(error)})
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
    args = parser.parse_args()
    try:
        report = inspect(args.identity)
    except (OSError, ValueError) as error:
        print(f"Cannot inspect delayed membership journal: {error}", file=sys.stderr)
        return 2
    print(json.dumps(report, indent=2, ensure_ascii=True))
    return 2 if report["errors"] else 0


if __name__ == "__main__":
    sys.exit(main())
