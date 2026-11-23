"""Persistent Matrix device files, shared by both provisioners."""
from contextlib import contextmanager
import fcntl
import json
import os
from pathlib import Path
import tempfile

@contextmanager
def identity_lock(folder):
    folder.mkdir(parents=True, exist_ok=True, mode=0o700)
    if folder.is_symlink():
        raise ValueError("identity folder is a symbolic link")
    os.chmod(folder,0o700)
    fd=os.open(folder/".provision.lock",os.O_RDWR|os.O_CREAT|os.O_NOFOLLOW,0o600)
    with os.fdopen(fd,"r+") as lock:
        fcntl.flock(lock,fcntl.LOCK_EX)
        yield

def has_crypto(folder):
    crypto=folder/"crypto"
    return crypto.is_dir() and any(p.is_file() and p.stat().st_size for p in crypto.glob("**/*"))

def read_session(folder,ghost):
    path=folder/"session.json"
    if path.is_symlink(): raise ValueError("identity session is a symbolic link")
    try:
        value=json.loads(path.read_text())
    except (FileNotFoundError,ValueError,UnicodeError):
        return None
    if not isinstance(value,dict): return None
    if value.get("user_id") and value["user_id"]!=ghost:
        raise ValueError("stored session belongs to a different identity")
    if value.get("user_id")==ghost and all(isinstance(value.get(k),str) and value[k] for k in ("access_token","device_id")):
        os.chmod(path,0o600)
        return value
    return None

def atomic_file(path,content):
    fd,name=tempfile.mkstemp(dir=path.parent,prefix="."+path.name+".")
    try:
        with os.fdopen(fd,"wb") as file:
            file.write(content)
            file.flush()
            os.fsync(file.fileno())
        os.replace(name,path)
        fd=os.open(path.parent,os.O_RDONLY|os.O_DIRECTORY)
        try: os.fsync(fd)
        finally: os.close(fd)
    finally:
        Path(name).unlink(missing_ok=True)

def has_store(folder):
    db = folder / "crypto" / "matrix-sdk-crypto.sqlite3"
    return db.is_file() and not db.is_symlink() and db.stat().st_size > 0


def identity_evidence(folder):
    return any((folder / name).exists() for name in (
        "matrixrtc-ready-v1", "matrixrtc-ready", "recovery-key.txt"
    ))


def ensure_session(folder, ghost, login):
    current = read_session(folder, ghost)
    if current:
        if identity_evidence(folder) and not has_store(folder):
            raise ValueError("restore the matching crypto store for this prepared device")
        return current
    if identity_evidence(folder) or has_crypto(folder):
        raise ValueError("restore the matching session.json for the existing crypto store")
    session = login()
    if session.get("user_id") != ghost or not all(
        isinstance(session.get(k), str) and session[k] for k in ("access_token", "device_id")
    ):
        raise ValueError("homeserver returned an incomplete or mismatched device")
    session = {k: session[k] for k in ("user_id", "device_id", "access_token")}
    atomic_file(folder / "session.json", (json.dumps(session) + "\n").encode())
    return session


def mark_ready(folder, ghost):
    session = read_session(folder, ghost)
    if not session or not has_store(folder):
        raise ValueError("prepared identity has no matching session or crypto store")
    marker = {k: session[k] for k in ("user_id", "device_id")}
    atomic_file(folder / "matrixrtc-ready-v1", (json.dumps(marker) + "\n").encode())


def is_ready(folder, ghost):
    try:
        session = read_session(folder, ghost)
        marker = json.loads((folder / "matrixrtc-ready-v1").read_text())
        return bool(session and has_store(folder) and
                    marker == {k: session[k] for k in ("user_id", "device_id")})
    except (ValueError, FileNotFoundError, UnicodeError):
        return False
