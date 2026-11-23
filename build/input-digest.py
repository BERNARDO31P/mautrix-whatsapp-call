#!/usr/bin/env python3
"""Digest names, contents and symlink targets of pinned Linux build inputs."""
import hashlib
import os
from pathlib import Path
import sys

def selected(kind, name):
    if kind != "rust":
        return True
    return (name in ("bin/rustc", "bin/cargo", "bin/rustdoc")
            or name.startswith(("lib/lib", "lib/rustlib/x86_64-unknown-linux-gnu/lib/",
                                "lib/rustlib/x86_64-unknown-linux-gnu/bin/")))

def digest_file(stream):
    result = hashlib.sha256()
    while chunk := stream.read(1024 * 1024):
        result.update(chunk)
    return result.digest()

def tree(kind, root):
    records = {}
    for directory, dirs, files in os.walk(root):
        for name in dirs + files:
            path = Path(directory, name)
            rel = path.relative_to(root).as_posix()
            if not selected(kind, rel):
                continue
            if path.is_symlink():
                records[rel] = b"L" + os.fsencode(os.readlink(path))
            elif path.is_file():
                with path.open("rb") as stream:
                    records[rel] = b"F" + digest_file(stream)
    return records

def digest(records):
    result = hashlib.sha256()
    if not records:
        raise ValueError("empty toolchain")
    for path, value in sorted(records.items()):
        result.update(os.fsencode(path) + b"\0" + value + b"\0")
    return result.hexdigest()

if __name__ == "__main__":
    print(digest(tree(sys.argv[1], Path(sys.argv[2]))))
