# Operations, updates, recovery and acceptance testing

Use [INSTALLATION.md](INSTALLATION.md) for the complete first-install procedure, generated credentials, configuration, proxy and ports. This guide applies to the installed `/opt/matrix/mautrix-whatsapp-call/` overlay and is also included in future runtime packages.

## 1. Everyday administration

Use the same Compose project and both files every time. Run these commands from a root Bash shell:

```bash
cd /opt/matrix
compose() {
    docker compose -f docker-compose.yml -f mautrix-whatsapp-call/compose.yml "$@"
}
compose ps
bash mautrix-whatsapp-call/verify-worker.sh
compose logs --tail=100 mautrix-whatsapp mautrix-whatsapp-call-worker \
    mautrix-whatsapp-call-provisioner livekit lk-jwt-service synapse
```

`verify-worker.sh` checks the Unix socket in both containers and runs the worker's protocol health check. A healthy worker does not guarantee public ICE/media connectivity, valid MatrixRTC membership, or WhatsApp callability.

The components are:

| Service | Role |
| --- | --- |
| `mautrix-whatsapp` | Existing WhatsApp sessions, portals, message bridge and call signaling; custom local image `mautrix-whatsapp-call-bridge:local`. |
| `mautrix-whatsapp-call-worker` | Rust MatrixRTC participant, encrypted ghost identity store and media conversion; local image `mautrix-whatsapp-call-worker:local`. |
| `mautrix-whatsapp-call-provisioner` | Separate out-of-band ghost identity provisioning using the original WhatsApp registration, not a human token. |
| `livekit` | SFU for Matrix-side media; pinned v1.13.10. |
| `lk-jwt-service` | Matrix-authenticated LiveKit token service; pinned 0.9.0. |
| `synapse` | Existing Matrix homeserver, registration and room state. |

The bridge and worker share a named socket volume, not a public TCP control port. Default socket owner/group/mode is `0:29319 0660`. The provisioner alone receives the ordinary WhatsApp appservice registration. Do not give that registration to the media worker or expose the socket to other containers.

### Disable or re-enable calling

Change `MATRIXRTC_ENABLE_CALLS` to `0` or `1` in `/opt/matrix/.env` and rerun `bash mautrix-whatsapp-call/install-or-upgrade.sh`, followed by `verify-worker.sh`. This recreates services and may interrupt calls/messaging. For an immediate worker-only toggle after a healthy installation, update `.env` first and use:

```bash
compose up -d --no-deps --force-recreate mautrix-whatsapp-call-worker
bash mautrix-whatsapp-call/verify-worker.sh
```

Do not use a plain `restart` to apply a changed environment; it does not recreate the container. Do not assume disabled media also immediately hides every client call button. Verify `RTC_WORKER_ENABLE_CALLS` inside the recreated worker (see installation guide). Normal messaging should continue. To re-enable after a failed installation, diagnose and rerun the full installer rather than bypassing its checks.

### Provisioning and existing rooms

The installer migrates eligible existing rooms (RTC power levels, direct-chat mappings) and performs one provisioning scan before enabling the background provisioner. Future eligible encrypted DMs receive persistent ghost identities asynchronously. No user access token needs to be stored in `.env`.

To request an immediate additional scan after correcting a room or registration, during a maintenance window stop the provisioner to avoid competing scans, then run the same one-shot command used by the installer:

```bash
compose stop mautrix-whatsapp-call-provisioner
compose run --rm --no-deps mautrix-whatsapp-call-provisioner \
    python3 /usr/local/lib/mautrix-whatsapp-call/auto-provision-whatsapp-ghosts.py --once
compose up -d --no-deps mautrix-whatsapp-call-provisioner
```

Restart the provisioner even if the one-shot command fails. Do not create replacement ghosts or delete their crypto directories as a first troubleshooting step. The scanner validates encryption, room participants, the functional bridge bot and registered ghost namespace. Old unencrypted DMs are not silently converted or recreated. Review migration output and determine an appropriate encrypted-room migration separately. Existing group-call handling uses different bridge-bot/participant semantics; a group is not a one-to-one ghost room.

## 2. Upgrade to a new project release

Prefer tested prebuilt Linux AMD64 runtime assets from <https://github.com/BERNARDO31P/mautrix-whatsapp-call/releases/latest>. Keep the prior known-good ZIP and its checksum. Download and verify the new ZIP and internal `SHA256SUMS`, make a consistent backup, and follow the `rsync` and installer steps in [INSTALLATION.md](INSTALLATION.md). Preserve the original `.env` call-enable setting for an ordinary upgrade, unless intentionally disabling calls for a maintenance window. Never delete `generated/` or `worker-state/`.

- Read release notes and confirm compatibility with your Synapse, bridge database schema, clients and pinned base images. Avoid updates while a call is active.
- If you customize the reverse proxy, firewall or deployment templates, review the new versions and reapply supported changes. The installer regenerates configuration and rsync replaces project-owned files; it does not rewrite the host nginx vhost or firewall.
- The installer temporarily disables calls, updates runtime wiring and room metadata, provisions eligible ghosts, restores the configured worker flag and verifies the socket. An error may leave partially updated configuration; the installer attempts to stop the call worker and background provisioner. Inspect the failure and confirm both are stopped; do not re-enable calls until the configuration and migration are repaired.
- Recheck public JWT/SFU/transport endpoints, worker health, media and normal messages. Force-stop/reopen Matrix clients after room changes.

For updating unrelated registry-backed services:

```bash
cd /opt/matrix
compose pull --ignore-buildable
compose up -d
bash mautrix-whatsapp-call/verify-worker.sh
```

Perform this only with compatible configured versions. A pull does not advance pinned image tags; a new upstream bridge tag does not update the compiled bridge inside the local custom image. Do not use the base Compose file alone to recreate mautrix-whatsapp: it may remove the call-enabled image/socket integration. Do not prune the known-good local images before verifying and accepting the new release. Maintainers should use the repository's `docs/UPDATING.md` for source pins and dependency updates.

## 3. Backups, rollback and disaster recovery

The complete recoverable state includes all original Matrix/WhatsApp service configurations and databases (including WhatsApp login/session state), Synapse signing keys and media, existing appservice registrations, reverse proxy/TLS configuration, `.env`, the old verified runtime package, and **both** `generated/` and `worker-state/` together. The generated store passphrase is necessary to decrypt the worker's persistent ghost crypto state. The generated MatrixRTC registrations and copies under Synapse must agree. Treat all backups as sensitive production credentials.

Use your database-specific backup process. For PostgreSQL, use `pg_dump`/`pg_dumpall` or your existing tested backup mechanism; service name, database and role depend on your base Compose setup. Do not tar live PostgreSQL data files. For SQLite, use SQLite's online backup facility or stop the service before copying the database and its journal/WAL files. For a consistent point-in-time recovery across services, quiesce writes and coordinate the snapshots. Restore-test backups separately before relying on them.

Example cold snapshot of the call overlay only (not a replacement for database and Matrix backups):

```bash
cd /opt/matrix
umask 077
BACKUP="/root/mautrix-call-backup-$(date -u +%Y%m%d-%H%M%S)"
mkdir -m 700 "$BACKUP"
# During a maintenance window, stop the call components and bridge so state is not being changed.
compose stop mautrix-whatsapp mautrix-whatsapp-call-provisioner mautrix-whatsapp-call-worker
# Preserve all call deployment files, including secrets and state; archive retains permissions.
tar -C /opt/matrix -czf "$BACKUP/call-deployment.tar.gz" mautrix-whatsapp-call
cp -a .env docker-compose.yml "$BACKUP/"
# Complete the coordinated backups for the base stack before resuming service.
compose up -d mautrix-whatsapp-call-worker mautrix-whatsapp mautrix-whatsapp-call-provisioner
```

This example assumes an existing call overlay and `.env`. Restart services even if the backup procedure is interrupted, once safe. Keep backups outside the rsync destination and restrict access. Verify the archive and keep an encrypted off-host copy. For first installation, back up the base stack rather than trying to archive nonexistent call state.

### Roll back the extension release

If a new release fails, first preserve diagnostics (redacted), disable calls and keep the failed release and all state for inspection. For a compatible binary/config rollback, verify the previous runtime ZIP, rsync its deployment directory excluding `generated/` and `worker-state/`, then rerun the installer with calls disabled and verify messages/calls. Enable only after checks pass.

If an install fails after stopping the call services, the saved `.env` may still request enabled calls. The fail-closed trap stops the worker and provisioner but cannot roll back all other mutations. Do not run a blanket Compose restart/up before inspecting and repairing the failure. Verify the two services are stopped; if the administrator intends to keep calls disabled, explicitly set `MATRIXRTC_ENABLE_CALLS=0` before restarting anything.

Do not assume every older binary can read newer persistent stores or bridge schemas. If there were incompatible migrations, restore a coordinated pre-upgrade backup of database, configuration, registrations, call state and matching runtime under maintenance instead. Restoring a database snapshot can discard messages/events after the snapshot; assess that impact first. The installer does not automatically undo Matrix room state or database migrations. Do not restore random subsets of credentials. If reverting entirely to the original upstream bridge, restore compatible original configuration and Compose setup, and review the MatrixRTC registrations, client discovery and room-state changes before removing services. Keep the backup; no blanket `docker compose down -v` or `docker system prune --volumes`.

### Secret recovery and rotation

- Missing standalone LiveKit key/secret files: check for surviving consistent copies before running `prepare.sh`; it reconciles from generated configuration and registration files. It refuses contradictions rather than choosing a new secret.
- Missing `matrixrtc_store_passphrase`: restore the exact old passphrase with the existing encrypted worker-state. A newly generated passphrase cannot unlock old stores.
- Compromised LiveKit key or appservice credentials: isolate the deployment and schedule coordinated rotation across every consumer (LiveKit, JWT, Synapse, double-puppet bridge) and revoke compromised access. The ordinary installer preserves credentials; it is not a rotation command. Do not delete `generated/` to force rotation. Recreate affected services only after all matching copies/configuration have been reconciled and validated. Plan for invalidated sessions/active calls.
- Never expose registrations, access tokens, recovery keys, full worker logs or crypto store files in public issue reports. Use private permissions and review any shared diagnostics.

### Migrate an older MatrixRTC directory

The supported new names are `mautrix-whatsapp-call/`, `compose.yml`, `mautrix-whatsapp-call-worker`, `mautrix-whatsapp-call-provisioner` and `/run/mautrix-whatsapp-call/worker.sock`. Older deployments used `matrixrtc/`, `compose.matrixrtc.yml`, different service/image names and socket paths. The MatrixRTC appservice IDs, token files and persistent identity format are intentionally retained.

Back up the old stack, stop the old worker/provisioner and call-enabled bridge using their **old** combined Compose files, and ensure no scanner or worker still writes the old state. Copy the old `generated/` and `worker-state/` directories together into the new directory, preserving owners and permissions. If the new directory already contains either directory, do not overwrite or merge blindly; resolve which is authoritative before proceeding. Use the new runtime deployment files and combined Compose command in the installation guide. Remove old containers, socket volumes and old deployment paths only after successful acceptance tests and after confirming they are not in use. Do not remove shared Matrix, WhatsApp or LiveKit volumes. Keep the old backup until rollback is no longer required.

## 4. Troubleshooting

First inspect `compose ps`, the worker health and `verify-worker.sh`, the public HTTPS JWT health endpoint, then relevant logs. Check that you are operating on the correct Compose project, release and service names. Redact all Matrix/WhatsApp identifiers, addresses, tokens, headers, session data and secrets before sharing logs. Avoid dumping complete `docker compose config` or `docker inspect` output publicly: they may include environment credentials.

| Symptom | Checks and action |
| --- | --- |
| Installer says missing runtime files | Use the timestamped runtime ZIP, not the source archive. Verify both manifests, deployment directory, and the three compiled executables. |
| `MATRIXRTC_SERVER_NAME is required` | Set the actual Matrix server name in `/opt/matrix/.env` (no scheme), correct duplicate entries/export overrides. |
| `Missing ... appservice registration` | Supply the existing host path via `MATRIXRTC_WHATSAPP_REGISTRATION_PATH`; do not create a substitute WhatsApp registration. |
| Python `ModuleNotFoundError: yaml` | Install host `python3-yaml`; the installer runs Python helpers on the host. |
| Credential conflict | Restore matching surviving credential copies from backup. Do not regenerate everything or overwrite the old worker passphrase. |
| Synapse fails after merge | Check its logs for registration paths/permissions and supported MSC features. Confirm its config directory is mounted at `/data` and loopback 8008 is reachable. Compare backed-up config; do not replace the entire homeserver YAML. |
| JWT endpoint 404 | Check `/livekit/jwt` prefix stripping, upstream port and nginx location order. |
| JWT/SFU endpoint 502 | Check `compose ps`, service logs, loopback bindings and proxy backend. If nginx is containerized, check its network namespace. |
| TLS or WebSocket failure | Check public DNS, certificate chain, HTTPS vhost and Upgrade/Connection headers. Confirm client can reach WSS; avoid TLS verification bypass. |
| Token/transport API 401 or 403 | Use a valid local Matrix user token, confirm homeserver identity and JWT appservice registration/scopes, internal Synapse URL override and room membership. Migration-specific 403s need room permission/registration diagnostics; do not grant arbitrary admin permissions. |
| Signaling works but no audio/video | Check UDP 7882 and TCP 7881 reachability from the client network, advertised external IP, NAT/firewall/CDN, permissions and worker/SFU logs. Try a separate network to distinguish media connectivity from signaling. |
| Port already in use | Identify existing LiveKit/JWT deployment before starting the overlay; do not run duplicate servers. JWT loopback port is configurable; media port changes require matching LiveKit and Docker settings. |
| Socket missing or permission denied | Verify shared volume in both containers, bridge PID 1 GID, worker `0:<gid>`, socket mode 660, and both Compose files. Do not chmod the socket to 777. |
| Call flags not reflected | Check `MATRIXRTC_ENABLE_CALLS` in `.env` and `RTC_WORKER_ENABLE_CALLS` in the recreated worker. A `restart` does not apply new environment. |
| Call icons absent/grey | Confirm client supports MatrixRTC, room encryption/direct mapping and eligibility, ghost provisioning logs, public transports and room permissions. Force-stop/reopen client after migration. Do not enable unsupported rooms by manually raising power levels. |
| Business/Cloud contact cannot be called | WhatsApp may explicitly disable business calling. Authoritative `business_calling enabled="false"` is a hard negative; a `feature/voip` query timeout alone is not proof of a blocked account. Test with a known callable consumer contact first. |
| Note-to-Self or bridge control room | Not a normal callable remote WhatsApp contact. Use a real eligible contact, not your own account or management bot. |
| New ghost not yet ready | Check provisioner is running and scan interval, encrypted eligible room, local human and registered ghost namespace. Do not copy a ghost device from another user. |
| Call fails after upgrade | Confirm custom bridge image and socket integration have not been replaced by a base-only Compose update. Check worker logs, RTC membership/room identity and the actual destination. |
| Calls stopped but messages work | Verify worker flag, worker health, custom bridge image and client discovery. Run installer again after resolving the cause. |
| Repeated calls fail after disconnect | Check prior session cleanup and both endpoint logs. Capture a minimal reproduction including call direction, modes and whether decline/timeout/hangup occurred; do not delete crypto state. |
| Permission warnings after Windows extraction | Deploy a verified runtime package and run the installer, which normalizes permissions. Generated and worker-state directories must remain private. |
| Wrong architecture / exec format error | Use the Linux AMD64 runtime on x86_64; current release does not provide native ARM64 binaries. |

Useful non-secret checks (default ports/group):

```bash
cd /opt/matrix
compose ps
ss -lntup | grep -E ":(443|7880|7881|7882|8008|18080)\\b" || true
compose exec -T mautrix-whatsapp stat -c "%u:%g %a %n" /run/mautrix-whatsapp-call/worker.sock
compose exec -T mautrix-whatsapp sh -c "grep \"^Gid:\" /proc/1/status"
compose logs --since=10m --tail=200 mautrix-whatsapp-call-worker livekit lk-jwt-service
```

`ss` output depends on Docker's proxy/NAT implementation; validate published bindings with Compose and actual network connectivity as well. HTTP checks alone do not verify media. `verify.sh` accepts a Matrix access token interactively; use a trusted administrator session, not a public terminal/CI log.

## 5. Live acceptance checklist for every deployment or dependency update

Use test accounts and consenting participants; do not call arbitrary business numbers as test targets. Record date, release tag/ZIP checksum, Synapse and client versions, network, direction and observed result. A source/build test pass is not evidence that a real cross-network media call passed. The project was reported working by its operator after installing the initial release; the checklist below is a repeatable validation procedure, not a claim that every item was independently tested on every device.

| Test | Expected result |
| --- | --- |
| Matrix to WhatsApp audio and video | Remote device rings, accepts; two-way sound and video as applicable. |
| WhatsApp to Matrix audio and video | Matrix client rings, accepts; media reaches both parties. |
| Decline / cancel / no answer | Correct termination from either side and no lingering ringing or stale session; next call works. |
| Hang up after connection | Either side terminates; media/session and RTC membership are cleaned up. |
| Long accepted call | Continues beyond the original ring timeout. |
| Audio to video, video to audio | Supported mode changes propagate without requiring a new call. |
| Mute/unmute and camera off/on | Local changes work; remote state reflected where WhatsApp protocol exposes it. |
| Portrait/orientation and keyframe recovery | Correct video orientation; image resumes after mode change or recoverable loss. |
| A/V sync and both media directions | No persistent one-way audio/video, excessive drift or frozen image. |
| Wi-Fi vs cellular / different networks | Validate both media paths and observe ICE/TCP fallback where applicable. |
| Unreachable/offline destination | Clear failure/timeout and clean next-call behavior. |
| Business and hosted/Cloud account | Explicitly forbidden calls are unavailable; unknown capability does not alone block a consumer account. |
| Note-to-Self and bridge management room | Not treated as callable WhatsApp DMs. |
| Group / relay configuration if used | Test your actual configured eligible participants, group membership and signaling; no wrong-account or wrong-participant call. |
| Existing encrypted DM and newly created DM | Direct mapping, permissions, ghost provisioning and eligibility work without manual token injection. |
| Unencrypted old DM | Remains intact; no silent recreation or insecure bypass. |
| Room membership changes / already active call | Reject wrong participant or competing RTC slot without leaking media. |
| Repeated calls after all termination paths | No stale reservation, membership, device or worker state blocking subsequent calls. |
| Network interruption and LiveKit reconnect | Observe recovery or clean termination, then verify next call. |
| Worker and bridge restart | Persistent ghost stores remain usable; no assumption of seamless active-call survival. |
| MatrixRTC room scope canonicalization | Upper/lowercase `m.call#ROOM` / `m.call#room` handling does not cause call failure. |
| Ordinary encrypted messages | Send and receive before, during and after calls, and after restarting. |
| Fresh install and repeat installer | Keys remain unchanged; eligible rooms are handled idempotently; no duplicate registrations. |
| Upgrade with retained state | Prior WhatsApp sessions and encrypted RTC identities remain usable; worker flag restored. |

Only mark tests passed after observing the expected outcome. Record not-applicable/unsupported cases explicitly, including client or account limitations.

## 6. Reporting and security

For a bug report include the release tag, platform/architecture, relevant component versions, exact failing step, expected vs actual behavior and sanitized logs around the event. For a security problem use the repository's security reporting guidance; do not publish a proof containing real tokens or session data. Never upload `generated/`, `worker-state/`, bridge/Synapse databases, full `.env`, production registrations or private keys.
