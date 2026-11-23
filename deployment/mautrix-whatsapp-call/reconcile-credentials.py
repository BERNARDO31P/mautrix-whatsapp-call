"""Reconcile existing credential values before creating missing files."""
import sys
import secrets
from pathlib import Path
from config_files import atomic_write,load

def reconcile(root,server):
    dest=root/"generated"
    dest.mkdir(mode=0o700,exist_ok=True)
    def yaml_at(path):
        return load(path.read_text()) if path.is_file() else {}
    names=("as_token","hs_token","double_puppet_as_token","double_puppet_hs_token","livekit_secret","matrixrtc_store_passphrase")
    candidates={name:set() for name in names}
    def add(key,value):
        if isinstance(value,str) and value.strip():
            candidates[key].add(value.strip())
    for key in names:
        p=dest/key
        if p.exists(): add(key,p.read_text())
    for prefix,filename in [("","matrixrtc-registration.yaml"),("double_puppet_","matrixrtc-doublepuppet-registration.yaml")]:
        for path in [dest/filename,root.parent/"synapse"/filename]:
            reg=yaml_at(path)
            for key in ("as_token","hs_token"):
                add(prefix+key,reg.get(key))
    add("livekit_secret",yaml_at(dest/"livekit.yaml").get("keys",{}).get("lk-matrixrtc"))
    dp=dest/"double_puppet_secret"
    existing=yaml_at(root.parent/"mautrix-whatsapp/config.yaml").get("double_puppet") or {}
    for value in [dp.read_text().strip() if dp.exists() else "",(existing.get("secrets") or {}).get(server,"")]:
        if value.startswith("as_token:"):add("double_puppet_as_token",value[9:])
    for k, values in candidates.items():
        if len(values)>1: raise ValueError(f"conflicting surviving copies of {k}; existing credentials preserved")
    if not candidates["matrixrtc_store_passphrase"] and any((root/"worker-state"/"identities").glob("*/crypto/*")):
        raise ValueError("restore the existing matrixrtc_store_passphrase for the encrypted worker store")
    for k,values in candidates.items():
        p=dest/k
        if p.is_file() and p.read_text().strip():
            p.chmod(0o600)
        else:
            value=next(iter(values),None) or secrets.token_hex(32 if k in ("livekit_secret","matrixrtc_store_passphrase") else 48)
            atomic_write(p,(value+"\n").encode())
if __name__=="__main__":
    reconcile(Path(sys.argv[1]),sys.argv[2])
