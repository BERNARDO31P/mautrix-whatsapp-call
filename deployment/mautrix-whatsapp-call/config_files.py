"""YAML and atomic file writes used by the MatrixRTC installer."""
import os
import tempfile
from pathlib import Path
import yaml

def load(text):
    data=yaml.safe_load(text)
    if not isinstance(data,dict):
        raise ValueError("expected a YAML mapping")
    return data

def dump(data):
    return yaml.safe_dump(data,sort_keys=False, allow_unicode=True)

def protected_mode(mode):
    return 0o640 if mode & 0o040 else 0o600

def atomic_write(path:Path, content:bytes, owner=None, mode=0o600):
    fd, tmp=tempfile.mkstemp(dir=path.parent,prefix="."+path.name+".")
    try:
        with os.fdopen(fd,"wb") as f:
            if owner:
                os.fchown(f.fileno(),owner.st_uid,owner.st_gid)
            os.fchmod(f.fileno(),mode)
            f.write(content)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp,path)
    finally:
        Path(tmp).unlink(missing_ok=True)

def backup_file(path, backup):
    atomic_write(backup,path.read_bytes(),path.stat(),0o600)
