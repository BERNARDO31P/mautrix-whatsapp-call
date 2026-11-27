"""Persistent Matrix device files, shared by both provisioners."""
from contextlib import contextmanager
import fcntl
import json
import os
import stat
from pathlib import Path
import tempfile

@contextmanager
def identity_lock(folder):
    folder.mkdir(parents=True, exist_ok=True, mode=0o700)
    if folder.is_symlink():
        raise ValueError("identity folder is a symbolic link")
    fd=os.open(folder/".provision.lock",os.O_RDWR|os.O_CREAT|os.O_NOFOLLOW,0o600)
    with os.fdopen(fd,"r+") as lock:
        try:
            fcntl.flock(lock,fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            raise BlockingIOError("ghost identity is busy; retry later or stop the worker before repair") from exc
        info = os.fstat(lock.fileno())
        current = (folder/".provision.lock").lstat()
        if not stat.S_ISREG(info.st_mode) or (info.st_dev, info.st_ino) != (current.st_dev, current.st_ino):
            raise ValueError("identity lock path changed")
        os.chmod(folder,0o700)
        yield

def path_present(path):
    # lstat sees dangling symlinks too; permission failures must not look like
    # an empty identity.
    try:
        path.lstat()
    except FileNotFoundError:
        return False
    return True


def has_crypto(folder):
    # Even an empty or partial crypto directory is evidence of prior setup.
    return path_present(folder / "crypto")


def unique_json_object(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("duplicate identity metadata field")
        result[key] = value
    return result


def read_session(folder, ghost):
    path = folder / "session.json"
    try:
        info = path.lstat()
    except FileNotFoundError:
        return None
    if not stat.S_ISREG(info.st_mode):
        raise ValueError("identity session.json must be a regular file")
    try:
        value = json.loads(path.read_text(), object_pairs_hook=unique_json_object)
    except (ValueError, UnicodeError, RecursionError) as exc:
        raise ValueError("restore valid session.json; existing identity metadata is unreadable") from exc
    if not isinstance(value, dict) or value.get("user_id") != ghost:
        raise ValueError("stored session belongs to a different or unknown identity")
    if not all(isinstance(value.get(k), str) and value[k] for k in ("access_token", "device_id")):
        raise ValueError("restore complete session.json for the existing identity")
    os.chmod(path, 0o600)
    return value


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

def store_identity(folder):
    crypto = folder / "crypto"
    if crypto.is_symlink():
        return None
    try:
        info = (crypto / "matrix-sdk-crypto.sqlite3").lstat()
    except FileNotFoundError:
        return None
    if not stat.S_ISREG(info.st_mode) or info.st_size <= 0:
        return None
    # The database changes in place during normal SDK usage. Bind to its
    # filesystem identity, not its contents or modification timestamp.
    return {"device": info.st_dev, "inode": info.st_ino}


def has_store(folder):
    return store_identity(folder) is not None


def identity_evidence(folder):
    return any(path_present(folder / name) for name in (
        "matrixrtc-ready-v1", "matrixrtc-ready", "recovery-key.txt"
    ))


def ensure_session(folder, ghost, login):
    current = read_session(folder, ghost)
    if current:
        crypto = folder / "crypto"
        if path_present(crypto) and not stat.S_ISDIR(crypto.lstat().st_mode):
            raise ValueError("crypto store directory must not be replaced or linked")
        store = store_identity(folder)
        try:
            marker = read_ready_marker(folder, current, store)
        except ValueError as exc:
            if store is None:
                raise ValueError("restore the matching crypto store and reconcile readiness metadata") from exc
            raise
        # A durable first-preparation intent is retryable only while no
        # database or other recovery evidence exists. An interrupted process
        # with an unbound store must not silently adopt a replacement.
        empty_intent = (marker is not None and marker.get("ready") is False
                        and marker.get("store") is None
                        and not any(path_present(folder / name) for name in (
                            "matrixrtc-ready", "recovery-key.txt")))
        if store is None and ((identity_evidence(folder) and not empty_intent)
                              or (has_crypto(folder) and any(crypto.iterdir()))):
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


def read_ready_marker(folder, session, store):
    """Reject conflicting or malformed persisted identity evidence before repair."""
    marker = folder / "matrixrtc-ready-v1"
    if not path_present(marker):
        return None
    if not stat.S_ISREG(marker.lstat().st_mode):
        raise ValueError("readiness marker must be a regular file")
    try:
        recorded = json.loads(marker.read_text(), object_pairs_hook=unique_json_object)
    except (ValueError, UnicodeError, RecursionError) as exc:
        raise ValueError("reconcile invalid readiness metadata before preparation") from exc
    if recorded == {}:  # legacy interrupted preparation, no retained store binding
        return recorded
    expected = {k: session[k] for k in ("user_id", "device_id")}
    expected["store"] = store
    if (store is None and isinstance(recorded, dict)
            and recorded == {**expected, "ready": False}
            and recorded.get("ready") is False):
        return recorded  # first-preparation intent, never a ready identity
    if store is None or not isinstance(recorded, dict):
        raise ValueError("reconcile conflicting readiness identity before preparation")
    binding = recorded.get("store")
    if (not isinstance(binding, dict) or set(binding) != {"device", "inode"}
            or any(type(value) is not int or value < 0 for value in binding.values())
            or binding["inode"] == 0):
        raise ValueError("reconcile invalid readiness store metadata before preparation")
    if recorded != expected and recorded != {**expected, "ready": False}:
        raise ValueError("reconcile conflicting readiness identity before preparation")
    if "ready" in recorded and recorded["ready"] is not False:
        raise ValueError("reconcile invalid readiness status before preparation")
    return recorded


def invalidate_ready(folder):
    # Preserve the original binding even when preparation fails.
    marker = folder / "matrixrtc-ready-v1"
    if path_present(marker):
        if not stat.S_ISREG(marker.lstat().st_mode):
            raise ValueError("readiness marker must be a regular file")
        recorded = json.loads(marker.read_text(), object_pairs_hook=unique_json_object)
        if not isinstance(recorded, dict):
            raise ValueError("invalid readiness metadata")
        if recorded:
            recorded["ready"] = False
        atomic_file(marker, (json.dumps(recorded) + "\n").encode())


def mark_ready(folder, ghost):
    session = read_session(folder, ghost)
    store = store_identity(folder)
    if not session or store is None:
        raise ValueError("prepared identity has no matching session or crypto store")
    marker = {k: session[k] for k in ("user_id", "device_id")}
    marker["store"] = store
    atomic_file(folder / "matrixrtc-ready-v1", (json.dumps(marker) + "\n").encode())


@contextmanager
def prepare_ready(folder, ghost):
    """Commit readiness only for the session and existing store we prepared."""
    session = read_session(folder, ghost)
    if session is None:
        raise ValueError("missing session before identity preparation")
    store = store_identity(folder)
    read_ready_marker(folder, session, store)
    # Write intent even before the first native store creation. A hard kill
    # leaves the unbound marker, which cannot authorize an existing store.
    pending = {k: session[k] for k in ("user_id", "device_id")}
    pending.update(store=store, ready=False)
    atomic_file(folder / "matrixrtc-ready-v1", (json.dumps(pending) + "\n").encode())
    try:
        yield
    except Exception:
        # After an ordinary failed provisioning command, retain the observed
        # first database binding so the same device can retry. Do not bind
        # if the session changed or if the store is absent/invalid. Preserve
        # the original exception and the pending intent on metadata errors.
        if store is None:
            try:
                created = store_identity(folder)
                if (created is not None and read_session(folder, ghost) == session
                        and read_ready_marker(folder, session, None) == pending):
                    pending["store"] = created
                    atomic_file(folder / "matrixrtc-ready-v1", (json.dumps(pending) + "\n").encode())
            except (OSError, ValueError):
                pass
        raise
    if read_session(folder, ghost) != session:
        raise ValueError("identity session changed during preparation")
    if store is not None and store_identity(folder) != store:
        raise ValueError("crypto store changed during preparation")
    mark_ready(folder, ghost)


def is_ready(folder, ghost):
    try:
        session = read_session(folder, ghost)
        if session is None:
            return False
        store = store_identity(folder)
        marker = read_ready_marker(folder, session, store)
        return bool(store is not None and marker and "ready" not in marker)
    except (ValueError, FileNotFoundError, UnicodeError, RecursionError):
        return False
