#!/usr/bin/env python3
"""Inspect/prune this project's disposable build data, never runtime state."""
import argparse
import fcntl
import os
from pathlib import Path
import re
import shutil
import subprocess
import sys

MARKER = ".mautrix-whatsapp-call-scratch"
MARKER_VALUE = "mautrix-whatsapp-call scratch v1"
RUN = re.compile(r"build\.[A-Za-z0-9]{8}\Z")


def real_dir(path):
    return path.is_dir() and not path.is_symlink()


def safe_remove(path):
    # Refuse directories containing detected mount points before
    # recursively deleting. shutil.rmtree does not follow nested symlinks.
    for folder, dirs, _ in os.walk(path, followlinks=False):
        for name in dirs:
            child = Path(folder) / name
            if not child.is_symlink() and os.path.ismount(child):
                raise RuntimeError(f"Refusing mounted directory: {child}")
    if os.path.ismount(path):
        raise RuntimeError(f"Refusing mounted directory: {path}")
    shutil.rmtree(path)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--apply", action="store_true", help="Delete listed generated data; default is preview")
    parser.add_argument("--compiler-cache", choices=("keep", "debug", "all"), default="keep",
                        help="debug: remove Rust debug target; all: also release target and Go build cache")
    parser.add_argument("--cache", type=Path, default=Path(os.environ.get("MAUTRIX_WHATSAPP_CALL_CACHE", Path.home() / ".cache/mautrix-whatsapp-call")))
    args = parser.parse_args()
    cache = args.cache.absolute()
    if not real_dir(cache):
        raise RuntimeError(f"Not a real project cache directory: {cache}")
    lock = cache / "build.lock"
    fd = os.open(lock, os.O_RDWR | os.O_CREAT | os.O_NOFOLLOW, 0o600)
    with os.fdopen(fd, "r+") as handle:
        try:
            fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            raise RuntimeError("A build is active. Stop or finish it before cleanup.")
        candidates = []
        work = cache / "work"
        runs = work / "runs"
        if real_dir(work) and real_dir(runs):
            for run in sorted(runs.iterdir()):
                if RUN.fullmatch(run.name) and real_dir(run):
                    for name in ("source", "opus", "bin", "package"):
                        child = run / name
                        if real_dir(child):
                            candidates.append(child)
        scratch = cache / "build-scratch-v1"
        marker = scratch / MARKER
        if real_dir(scratch) and marker.is_file() and not marker.is_symlink() and marker.read_text().strip() == MARKER_VALUE:
            candidates.append(scratch)
        rust = cache / "rust-target"
        if real_dir(rust) and args.compiler_cache in ("debug", "all"):
            for name in (("debug",) if args.compiler_cache == "debug" else ("debug", "release")):
                child = rust / name
                if real_dir(child):
                    candidates.append(child)
        go = cache / "go-build"
        if args.compiler_cache == "all" and real_dir(go):
            candidates.append(go)
        print("DELETE" if args.apply else "PREVIEW (nothing deleted)")
        total = 0
        for path in candidates:
            size = int(subprocess.check_output(["du", "-skx", "--", str(path)], text=True).split()[0])
            total += size
            print(f"{size / 1048576:9.3f} GiB  {path}", flush=True)
            if args.apply:
                safe_remove(path)
        print(f"{'Removed' if args.apply else 'Reclaimable'}: approximately {total / 1048576:.3f} GiB inside WSL")
        print("Preserved: tools, downloads, dependency sources, logs, build records, published dist archives and runtime state.")
        print("Old unpublished packages and intermediate binaries are included in the cleanup. WSL VHDX may require separate trimming/compaction.")


if __name__ == "__main__":
    try:
        main()
    except (OSError, RuntimeError, subprocess.SubprocessError) as error:
        print(f"Cleanup failed: {error}", file=sys.stderr)
        sys.exit(2)
