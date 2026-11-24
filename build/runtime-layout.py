#!/usr/bin/env python3
"""Copy only published deployment files; validate the exact release layout."""
import argparse
import hashlib
import re
import shutil
import stat
from pathlib import Path, PurePosixPath
from zipfile import ZipFile

DEPLOY = "deployment/mautrix-whatsapp-call/"
DEPLOY_FILES = tuple("""INSTALLATION.md OPERATIONS.md README.md
bridge/Dockerfile bridge/.dockerignore compose.yml config_files.py configure-bridge.py
install-or-upgrade.sh merge-synapse.py migrate-rooms.sh nginx-443-locations.conf
preflight.sh prepare.sh provision-ghost.sh reconcile-credentials.py render-hostname.py
resolve-appservice.py server-cleanup.sh synapse-additions.yaml sync-direct-chats.sh
verify-worker.sh verify.sh worker/Dockerfile worker/.dockerignore
worker/auto-provision-whatsapp-ghosts.py worker/provision-ghost.py worker/session_files.py""".split())
LICENSE_FILES = tuple("""LICENSES.md licenses/AGPL-3.0.txt licenses/MAUTRIX-LICENSE-EXCEPTIONS.txt
licenses/MATRIX-RUST-RTC-COMMERCIAL-NOTICE.txt licenses/MEOWCALLER-MIT.txt""".split())
BINARIES = {
    "mautrix-whatsapp-call-bridge": "bridge",
    "mautrix-whatsapp-call-worker": "worker",
    "mautrix-whatsapp-call-provision": "worker",
}
FILES = {DEPLOY + path for path in DEPLOY_FILES} | set(LICENSE_FILES) | {
    "SOURCE-VERSIONS.env", "SHA256SUMS"
} | {f"bin/{name}" for name in BINARIES} | {
    DEPLOY + f"{directory}/{name}" for name, directory in BINARIES.items()
}
DIRS = {str(parent) for path in FILES for parent in PurePosixPath(path).parents if str(parent) != "."}


def copy_regular(source, target):
    if source.is_symlink() or not source.is_file():
        raise ValueError(f"Expected regular source file: {source}")
    target.parent.mkdir(parents=True, exist_ok=True)
    shutil.copyfile(source, target)


def stage(repo, release):
    for rel in [*(DEPLOY + path for path in DEPLOY_FILES), *LICENSE_FILES]:
        # A symlink in an intermediate source directory could expose another tree.
        if any((repo / p).is_symlink() for p in PurePosixPath(rel).parents):
            raise ValueError(f"Unexpected source symlink: {rel}")
        copy_regular(repo / rel, release / rel)


def verify_tree(root):
    if root.is_symlink() or not root.is_dir():
        raise ValueError("Invalid release root")
    actual = set()
    for path in root.rglob("*"):
        rel = path.relative_to(root).as_posix()
        mode = path.lstat().st_mode
        if stat.S_ISDIR(mode):
            if rel not in DIRS:
                raise ValueError(f"Unexpected release directory: {rel}")
        elif stat.S_ISREG(mode):
            actual.add(rel)
        else:
            raise ValueError(f"Non-regular release path: {rel}")
    if actual != FILES:
        raise ValueError(f"Release layout mismatch: missing={sorted(FILES-actual)}, extra={sorted(actual-FILES)}")
    manifest = {}
    for line in (root / "SHA256SUMS").read_text().splitlines():
        match = re.fullmatch(r"([a-f0-9]{64})  \./(.+)", line)
        if not match or match[2] not in FILES - {"SHA256SUMS"} or match[2] in manifest:
            raise ValueError("Invalid or duplicate release manifest entry")
        manifest[match[2]] = match[1]
    if set(manifest) != FILES - {"SHA256SUMS"}:
        raise ValueError("Release manifest does not cover every shipped file")
    for rel, expected in manifest.items():
        with (root / rel).open("rb") as stream:
            digest = hashlib.sha256()
            for chunk in iter(lambda: stream.read(1024 * 1024), b""):
                digest.update(chunk)
            actual_hash = digest.hexdigest()
        if actual_hash != expected:
            raise ValueError(f"Release checksum mismatch: {rel}")
    for name, directory in BINARIES.items():
        if manifest[f"bin/{name}"] != manifest[DEPLOY + f"{directory}/{name}"]:
            raise ValueError(f"Deployment binary differs from release binary: {name}")
        with (root / "bin" / name).open("rb") as stream:
            elf = stream.read(20)
        if len(elf) != 20 or elf[:6] != b"\x7fELF\x02\x01" or elf[18:20] != b"\x3e\x00":
            raise ValueError(f"Not a 64-bit little-endian AMD64 ELF: {name}")


def verify_zip(path, destination=None):
    """Reject malformed, duplicate, traversal and symlink entries before unzip."""
    with ZipFile(path) as archive:
        names = set()
        roots = set()
        files = set()
        for entry in archive.infolist():
            name = entry.filename
            parts = name.rstrip("/").split("/")
            if (name in names or "\\" in name or any(p in ("", ".", "..") for p in parts)
                    or not re.fullmatch(r"mautrix-whatsapp-call-[0-9]{8}-[0-9]{6}", parts[0])):
                raise ValueError(f"Invalid release archive entry: {name}")
            names.add(name)
            roots.add(parts[0])
            mode = (entry.external_attr >> 16) & 0xFFFF
            kind = stat.S_IFMT(mode)
            rel = "/".join(parts[1:])
            if entry.is_dir():
                if kind not in (0, stat.S_IFDIR) or (rel and rel not in DIRS):
                    raise ValueError(f"Unexpected archive directory: {name}")
            else:
                if kind not in (0, stat.S_IFREG) or rel not in FILES or rel in files:
                    raise ValueError(f"Unexpected archive file: {name}")
                files.add(rel)
        if len(roots) != 1 or files != FILES:
            raise ValueError("Unexpected release archive layout")
        if destination is not None:
            # Extract exactly the names validated above using the same open
            # ZIP handle and parser. Do not let a second ZIP implementation
            # reinterpret Unicode extra fields or replace validated paths.
            if destination.is_symlink() or not destination.is_dir() or any(destination.iterdir()):
                raise ValueError("Extraction destination must be an empty directory")
            for entry in archive.infolist():
                target = destination.joinpath(*entry.filename.rstrip("/").split("/"))
                if entry.is_dir():
                    target.mkdir(parents=True, exist_ok=True)
                    continue
                target.parent.mkdir(parents=True, exist_ok=True)
                with archive.open(entry) as src, target.open("xb") as dst:
                    shutil.copyfileobj(src, dst, 1024 * 1024)
                mode = (entry.external_attr >> 16) & 0o777
                target.chmod(mode)
            verify_tree(destination / next(iter(roots)))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=("stage", "verify-tree", "verify-zip", "extract-zip"))
    parser.add_argument("path", type=Path)
    parser.add_argument("destination", type=Path, nargs="?")
    args = parser.parse_args()
    if args.action == "stage":
        stage(args.path, args.destination)
    elif args.action == "verify-tree":
        verify_tree(args.path)
    else:
        verify_zip(args.path, args.destination if args.action == "extract-zip" else None)
