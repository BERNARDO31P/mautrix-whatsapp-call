# Migration from an existing deployment

This migration preserves the existing Matrix/Synapse installation, WhatsApp bridge database/configuration, MatrixRTC registrations/secrets, room state and worker state. Only project-owned deployment paths, Compose service/image names and the private worker socket path change.

## Windows

Extract the repository as `mautrix-whatsapp-call`, verify it, then build it with the existing WSL distribution:

```powershell
cd "$HOME\Downloads\mautrix-whatsapp-call"
powershell.exe -NoProfile -ExecutionPolicy Bypass -File ".\Build.ps1" -DistroName "Ubuntu-22.04"
```

The build output is `dist/mautrix-whatsapp-call-<timestamp>.zip` plus a sibling `.sha256` file.

## Server cutover

Upload the new runtime ZIP and checksum to `/root/`, verify both the outer ZIP and its internal `SHA256SUMS`, then stop/remove only the old call worker and provisioner. Do not delete the Matrix stack or the mautrix-whatsapp database/configuration.

The installer uses `/opt/matrix/mautrix-whatsapp-call/`. Preserve `generated/` and `worker-state/` from the previous deployment directory and move them unchanged into the new project directory. Older MatrixRTC deployments commonly stored those directories under `/opt/matrix/matrixrtc/`.

Copy the new deployment tree into `/opt/matrix/mautrix-whatsapp-call/`, validate the merged Compose configuration, then run:

```bash
cd /opt/matrix
bash mautrix-whatsapp-call/install-or-upgrade.sh
bash mautrix-whatsapp-call/verify-worker.sh
```

After successful verification, the previous project deployment directory, its old worker/provisioner containers and socket volume, and any unused old images may be removed.

## Permissions

Runtime packaging and installation normalize project code to non-writable Linux modes even when the source checkout comes from Windows/DrvFS. `generated/` and `worker-state/` remain private (`0700` directories, `0600` regular files).

## Compatibility

`MATRIXRTC_*` environment variables, MatrixRTC appservice registration filenames/IDs and upstream MatrixRTC crate/protocol names remain unchanged for compatibility with existing deployments.

The room migration intentionally enables missing call-event permissions in legacy encrypted WhatsApp portals with standard Matrix defaults (`state_default: 50`, `events_default: 0`). Custom inherited defaults for the relevant event class, existing explicit permissions and bridge-managed restrictions are retained. Restriction markers distinguish inherited rules from explicitly configured rules so the bridge can restore the correct origin. Existing explicit restrictions are not automatically relaxed by migration. A standard default deliberately chosen by an administrator is indistinguishable from an ordinary default; review eligible rooms before migration if that distinction matters.
