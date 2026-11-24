"""YAML and atomic file writes used by the MatrixRTC installer."""
import os
import tempfile
from pathlib import Path
import yaml

class StrictLoader(yaml.SafeLoader):
    def construct_mapping(self, node, deep=False):
        # Duplicate explicit keys are ambiguous configuration, not overrides.
        # YAML merge keys still allow an explicit mapping to override defaults.
        seen = set()
        for key_node, _ in node.value:
            if key_node.tag == "tag:yaml.org,2002:merge":
                continue
            key = self.construct_object(key_node, deep=False)
            try:
                if key in seen:
                    raise ValueError("duplicate explicit YAML mapping key")
                seen.add(key)
            except TypeError:
                raise ValueError("unhashable YAML mapping key") from None
        return super().construct_mapping(node, deep=deep)


def load(text):
    try:
        data=yaml.load(text, Loader=StrictLoader)
    except yaml.YAMLError:
        # YAML parser diagnostics can contain source lines with credentials.
        raise ValueError("invalid YAML configuration") from None
    if not isinstance(data,dict):
        raise ValueError("expected a YAML mapping")
    return data

def dump(data):
    return yaml.safe_dump(data,sort_keys=False, allow_unicode=True)

def protected_mode(mode):
    return 0o640 if mode & 0o040 else 0o600

def atomic_write(path:Path, content:bytes, owner=None, mode=0o600):
    if path.is_symlink():
        raise ValueError(f"refusing to replace symlink: {path}")
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
        parent_fd = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY)
        try:
            os.fsync(parent_fd)
        finally:
            os.close(parent_fd)
    finally:
        Path(tmp).unlink(missing_ok=True)

def backup_file(path, backup):
    atomic_write(backup,path.read_bytes(),path.stat(),0o600)
