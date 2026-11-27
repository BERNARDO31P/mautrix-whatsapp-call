# Complete server installation: WhatsApp calls, MatrixRTC and LiveKit

This guide starts with a working, self-hosted Synapse and mautrix-whatsapp Docker Compose installation and ends with a call-enabled deployment. It also applies if neither LiveKit nor the Element JWT service is installed yet. The supplied installer installs both using Docker, generates credentials, merges configuration and provisions eligible WhatsApp ghosts. Do not install a second WhatsApp bridge or run a separate LiveKit installer alongside it.

For the source repository and current prebuilt releases, see <https://github.com/BERNARDO31P/mautrix-whatsapp-call> and <https://github.com/BERNARDO31P/mautrix-whatsapp-call/releases/latest>. For day-to-day operation and recovery, see [OPERATIONS.md](OPERATIONS.md).

## 1. Prerequisites and supported topology

- Linux AMD64 (`uname -m` returns `x86_64`), Docker Engine and Docker Compose v2. A runtime ZIP does not require Go, Rust, LLVM or a compiler on the server.
- Existing Synapse and mautrix-whatsapp Compose services named exactly `synapse` and `mautrix-whatsapp`, on a shared Docker network where `http://synapse:8008` is reachable from the added services. Synapse must support the MatrixRTC transports API and experimental features below (target Synapse v1.165.0 or newer; verify compatibility before upgrades).
- The installer probes `http://127.0.0.1:8008/_matrix/client/versions` on the host. Publish Synapse's HTTP listener to loopback if not already available there. Both `synapse` and `mautrix-whatsapp` must have writable `/data` bind mounts pointing at their respective existing directories under the Matrix root. The installer refuses alternate or shadowed configuration mounts (including separate mounts over the generated Synapse registrations) before building or pausing services; custom layouts require manual adaptation. Preserve the existing database, appservice registrations, signing key, media, and bridge sessions.
- The WhatsApp bridge must already be logged in and successfully bridging messages. The release is based on mautrix-whatsapp v0.2611.0; do not downgrade an incompatible newer database/schema or silently change the base image. Back up first.
- A public DNS name, trusted HTTPS certificate, reverse proxy with WebSocket support, and reachable LiveKit media ports. This overlay assumes the actual Matrix `server_name` is also the public HTTPS name (e.g. `matrix.example.com`); a split server-name/delegation topology requires adapting and testing the URLs and JWT homeserver mapping, not changing the existing Matrix identity domain.
- A MatrixRTC-capable client, e.g. Element X (target Android 26.11.0). Grant microphone/camera permissions. Eligible existing DMs must be encrypted; the installer does not silently encrypt old unencrypted rooms or recreate them.
- Administrative/root access to the server and a maintenance window. The installer restarts Synapse and the bridge and interrupts active calls.

If starting with a completely empty server, install Docker Engine/Compose using the official instructions for your distribution (<https://docs.docker.com/engine/install/>), then configure and register Synapse (<https://element-hq.github.io/synapse/latest/setup/installation.html>) and mautrix-whatsapp (<https://docs.mau.fi/bridges/go/whatsapp/index.html>). Confirm ordinary messages work in both directions before proceeding. Those services and their databases are not created by this extension.

Example layout (keep your existing Compose services and mounts):

```text
/opt/matrix/
  docker-compose.yml
  .env
  synapse/
    homeserver.yaml
  mautrix-whatsapp/
    config.yaml
    registration.yaml
  mautrix-whatsapp-call/        # installed from runtime release
    compose.yml
    bridge/
    worker/
    generated/                  # created automatically; private
    worker-state/               # created automatically; private
```

The existing base Compose file must satisfy these requirements, conceptually:

```yaml
services:
  synapse:
    # Keep existing image, environment, database and volumes.
    ports:
      - "127.0.0.1:8008:8008"
    volumes:
      - ./synapse:/data
  mautrix-whatsapp:
    # Keep existing image, appservice config, database and session mounts.
    volumes:
      - ./mautrix-whatsapp:/data
```

This is a partial example, NOT a complete Compose file. Do not replace your existing file. Ensure no duplicate/conflicting port or mount entries. On Debian/Ubuntu, install the host utilities if needed:

```bash
sudo apt-get update
sudo apt-get install -y ca-certificates curl wget unzip rsync python3 python3-yaml util-linux coreutils tar
```

The rest of the server commands assume a root Bash shell (`sudo -i` if necessary). Check the existing deployment:

```bash
uname -m
docker version
docker compose version
cd /opt/matrix
# Before first installation only; on upgrades use both Compose files.
docker compose -f docker-compose.yml config --services
curl -fsS http://127.0.0.1:8008/_matrix/client/versions
python3 -c "import yaml; print(yaml.__version__)"
```

Check Synapse's actual `server_name` without printing the rest of its config:

```bash
python3 -c 'import yaml; print(yaml.safe_load(open("/opt/matrix/synapse/homeserver.yaml"))["server_name"])'
```

The deployment preflight (`preflight.sh`) requires Docker Compose v2, both existing base services and successful merged configuration validation. It does not report success for an offline YAML-only check. The installer also verifies that both base services exist and all required build inputs and installation helpers are present and nonempty before pausing calls. `bash mautrix-whatsapp-call/preflight.sh --payload-only` performs only that local payload check, without requiring generated credentials or Docker. This check does not prove that the binaries can run or the images can build.

Do not change that name on an existing installation. Check DNS A/AAAA and routing; do not publish an unreachable AAAA record. Confirm your reverse proxy already serves the Matrix Client-Server API with trusted TLS.

## 2. Download and verify the prebuilt runtime

Download the timestamped runtime ZIP and its matching `.zip.sha256` from GitHub Releases, not GitHub's automatically generated **Source code (zip)**. The initial v0.1.0 release can be installed as follows. For later versions change both `TAG` and `ZIP` to the values shown on the release page. Never mix assets from different releases.

```bash
set -euo pipefail
umask 077
TAG="v0.1.0"
ZIP="mautrix-whatsapp-call-20261123-233131.zip"
STAGE="$(mktemp -d /root/mautrix-call-release.XXXXXX)"
cd "$STAGE"
BASE="https://github.com/BERNARDO31P/mautrix-whatsapp-call/releases/download/$TAG"
wget "$BASE/$ZIP"
wget "$BASE/$ZIP.sha256"
sha256sum -c "$ZIP.sha256"
unzip -q "$ZIP"
RELEASE="$STAGE/${ZIP%.zip}"
cd "$RELEASE"
sha256sum -c SHA256SUMS
DEPLOY="$RELEASE/deployment/mautrix-whatsapp-call"
test -f "$DEPLOY/install-or-upgrade.sh"
test -f "$DEPLOY/compose.yml"
test -f "$DEPLOY/bridge/mautrix-whatsapp-call-bridge"
test -f "$DEPLOY/worker/mautrix-whatsapp-call-worker"
test -f "$DEPLOY/worker/mautrix-whatsapp-call-provision"
```

The checksums detect corrupt or mismatched downloads; they do not independently authenticate the publisher. Obtain both assets from the trusted official project release. A source-built runtime can be uploaded with `scp` instead, then verified identically. The archive already contains a top-level release directory; do not add a duplicate directory level. Keep the original verified release for rollback.

## 3. Back up the existing server

Before editing `.env`, copying files or running the installer, make a consistent backup of Synapse and bridge databases (PostgreSQL dump or a cold SQLite backup as appropriate), their config, appservice registrations, encryption/signing keys, media, and the current call deployment if present. See [OPERATIONS.md](OPERATIONS.md) for the complete backup and rollback workflow. Do not rely solely on the installer's YAML backups; migration also changes room state on the homeserver.

For a quick configuration/deployment snapshot in addition to database backups:

```bash
cd /opt/matrix
umask 077
BACKUP="/root/mautrix-call-config-$(date -u +%Y%m%d-%H%M%S)"
mkdir -m 700 "$BACKUP"
cp -a docker-compose.yml "$BACKUP/"
if [ -f .env ]; then cp -a .env "$BACKUP/"; fi
mkdir -m 700 "$BACKUP/synapse" "$BACKUP/mautrix-whatsapp"
cp -a synapse/homeserver.yaml "$BACKUP/synapse/"
cp -a mautrix-whatsapp/config.yaml "$BACKUP/mautrix-whatsapp/"
if [ -f mautrix-whatsapp/registration.yaml ]; then
    cp -a mautrix-whatsapp/registration.yaml "$BACKUP/mautrix-whatsapp/"
fi
# If the bridge registration lives elsewhere, back up that file too.
```

Also save reverse proxy configuration and any existing MatrixRTC registration YAML. Backups contain secrets; keep them encrypted or root-only, off-host where appropriate. Do not upload them to GitHub.

## 4. Copy only the release deployment files

For first installation or upgrade, use the verified `DEPLOY` directory from step 2:

```bash
cd /opt/matrix
mkdir -p mautrix-whatsapp-call
rsync -an --delete --exclude="generated/" --exclude="worker-state/" \
    "$DEPLOY/" /opt/matrix/mautrix-whatsapp-call/
# Inspect dry-run output; then apply:
rsync -a --delete --exclude="generated/" --exclude="worker-state/" \
    "$DEPLOY/" /opt/matrix/mautrix-whatsapp-call/
```

On upgrades never use `--delete-excluded`, delete the complete deployment directory, or copy new empty state over existing state. Do not copy source-only ZIP contents in place of the runtime; the three compiled binaries are required. Save any local modifications outside the deployment directory before rsync; a release replaces project-owned templates and scripts.

If migrating from `/opt/matrix/matrixrtc/` or a differently named former overlay, follow the migration section in [OPERATIONS.md](OPERATIONS.md) before starting the new services.

## 5. Set the environment

Edit `/opt/matrix/.env` and retain unrelated entries. Use one assignment per setting, without duplicate keys. For a first install:

```dotenv
MATRIXRTC_SERVER_NAME=matrix.example.com
MATRIXRTC_ENABLE_CALLS=0
MATRIXRTC_SOCKET_GID=29319
```

Replace `matrix.example.com` with the actual Matrix server name from step 1. Do not include a URL scheme or slash. Do not change the configured numeric group for an existing deployment unless intentionally migrating socket permissions. Optional settings supported by the overlay:

| Variable | Default | Purpose |
| --- | --- | --- |
| `MATRIXRTC_WHATSAPP_REGISTRATION_PATH` | `./mautrix-whatsapp/registration.yaml` | Existing WhatsApp registration host path; relative to `/opt/matrix` or absolute. Not a generated replacement. |
| `MATRIXRTC_JWT_HOST_PORT` | `18080` | Loopback port mapped to JWT service 8080. Update proxy if changed. |
| `MATRIXRTC_BRIDGE_BASE_IMAGE` | `dock.mau.dev/mautrix/whatsapp:v0.2611.0` | Compatible upstream base image for the patched bridge. Not a source update switch. |
| `MATRIXRTC_REPLAY_RETENTION_DAYS` | `90` | Worker replay protection retention. |
| `MATRIXRTC_AUTOPROVISION_INTERVAL_SECONDS` | `15` | Background ghost scan interval. |
| `MATRIXRTC_AUTOPROVISION_CACHE_TTL_SECONDS` | `21600` | Provisioner cache TTL. |
| `MATRIXRTC_AUTOPROVISION_JOBS` | `4` | Concurrent provisioning jobs. |

The installer reads `MATRIXRTC_ENABLE_CALLS` from `.env`. Use literal `0` or `1`. Avoid conflicting exported shell variables overriding Compose interpolation. Keep `.env` private (`chmod 600 /opt/matrix/.env` as root if compatible with your administration setup). If you already manage multiple Compose files, retain those settings; the installer explicitly uses the base file and this overlay.

## 6. LiveKit installation and credential generation

**No separate LiveKit package, API account, manual JWT signing or Redis server is required for this single-node overlay.** Element X supplies the MatrixRTC client; any other client must provide its own compatible calling interface. Docker pulls the pinned LiveKit server `livekit/livekit-server:v1.13.10` and Element JWT service `ghcr.io/element-hq/lk-jwt-service:0.9.0`. The installer builds local bridge/worker images using the shipped binaries and starts the services. Do not start another stack listening on the same ports.

The canonical way to generate credentials is `install-or-upgrade.sh`, which calls `prepare.sh`. If you want to create and inspect the configuration before starting services, use the following optional preparation step (substitute your actual name):

```bash
cd /opt/matrix
export MATRIXRTC_SERVER_NAME="matrix.example.com"
bash mautrix-whatsapp-call/prepare.sh
unset MATRIXRTC_SERVER_NAME
```

`prepare.sh` runs `reconcile-credentials.py` under a lock with a restrictive umask. It creates missing credentials using Python's cryptographically secure `secrets.token_hex`. It reuses existing consistent files, recovers surviving copies from generated/Synapse registrations and the bridge double-puppet config, and refuses conflicting copies. It does not rotate working keys on reinstall.

| File under `generated/` | Purpose / fresh generation |
| --- | --- |
| `livekit_secret` | 32 random bytes encoded as 64 hex characters. LiveKit signing secret. |
| `as_token`, `hs_token` | Each 48 random bytes / 96 hex characters, for the MatrixRTC JWT appservice. |
| `double_puppet_as_token`, `double_puppet_hs_token` | Separate 48-byte tokens for local-user double puppeting. |
| `matrixrtc_store_passphrase` | 32 random bytes / 64 hex characters. Unlocks persistent encrypted Matrix ghost stores. |
| `livekit.yaml` | Rendered SFU config using the LiveKit secret. |
| `matrixrtc-registration.yaml` | JWT appservice registration for Synapse, including MSC scopes and proxy routing. |
| `matrixrtc-doublepuppet-registration.yaml` | Separate non-exclusive, local-domain appservice registration. |
| `double_puppet_secret` | `as_token:<double_puppet_as_token>` for bridge configuration. |

For reference, `openssl rand -hex 32` and `openssl rand -hex 48` generate equivalent-length random hex values, but do not run these commands to overwrite any installed secrets. The project generates them for you, and no example secret in these docs should be used in production. If worker crypto state exists and the passphrase is missing, restore it from backup; generating a replacement cannot decrypt the old state. Do not paste actual values into logs, shell history or issues. The secret and passphrase files end with a newline; the consuming configuration uses the appropriate value.

Check generation without revealing values:

```bash
cd /opt/matrix/mautrix-whatsapp-call
stat -c "%a %n" generated worker-state
for f in livekit_secret as_token hs_token double_puppet_as_token \
    double_puppet_hs_token matrixrtc_store_passphrase; do
    test -s "generated/$f"
    stat -c "%a %n" "generated/$f"
done
```

Expected: private directories `700`, regular secret files `600`. Preserve both directories together across all upgrades and backups.

### Actual generated LiveKit configuration

`generated/livekit.yaml` is rendered from the installer and mounted read-only into `/etc/livekit.yaml` in the LiveKit container:

```yaml
port: 7880
rtc:
  tcp_port: 7881
  udp_port: 7882
  use_external_ip: true
keys:
  lk-matrixrtc: "<generated value from livekit_secret>"
room:
  auto_create: false
webhook:
  api_key: lk-matrixrtc
  urls:
    - http://lk-jwt-service:8080/sfu_webhook
```

`lk-matrixrtc` is the matching API key identifier used by both services, not the secret. `room.auto_create: false` is intentional; room creation is managed through authenticated JWT service integration. The configured webhook travels internally over Docker networking; the generic JWT reverse-proxy route may also reach that path publicly, so signature validation remains required. `use_external_ip` requires working public ICE connectivity and address discovery. The installer regenerates this YAML; editing it directly is not a durable way to customize it. Do not add TURN, Redis, alternate ports or external SFUs without adapting the template, matching Compose and verifying connectivity.

### Element JWT service configuration

The Compose service sets the following configuration (example name only):

```yaml
LIVEKIT_URL: "wss://matrix.example.com/livekit/sfu"
LIVEKIT_KEY: "lk-matrixrtc"
LIVEKIT_SECRET_FROM_FILE: "/run/secrets/livekit_secret"
LIVEKIT_FULL_ACCESS_HOMESERVERS: "matrix.example.com"
LIVEKIT_JWT_BIND: ":8080"
LIVEKIT_HS_SERVER_NAME: "matrix.example.com"
LIVEKIT_AS_REGISTRATION_FILE: "/run/secrets/matrixrtc-registration.yaml"
LIVEKIT_CS_API_URL_OVERRIDES: "matrix.example.com=http://synapse:8008"
```

The LiveKit secret and JWT registration are mounted read-only. The override connects the local Matrix homeserver identity to the internal Synapse URL. The JWT service authenticates Matrix requests and issues scoped LiveKit access; clients never receive the LiveKit API secret. Do not broaden full-access homeservers or expose unauthenticated room creation as a workaround. In the pinned [JWT service v0.9.0](https://github.com/element-hq/lk-jwt-service/blob/v0.9.0/README.md), the proxied application-service endpoints check joined room membership; the standalone compatibility endpoints additionally depend on restricting full access to this exact homeserver. The full-access setting is not a general federation access control. The image does not include the later documented remote-publisher allowlist. Keep the supplied `room.auto_create: false` and confirm that a valid token for a non-member cannot obtain room access.

## 7. Synapse and mautrix-whatsapp configuration

The installer renders the hostname placeholders, runs `configure-bridge.py --apply` and `merge-synapse.py --apply`, copies registrations into the Synapse data directory and creates backups before changing existing configuration. You do not need to hand-edit the YAML or generate new WhatsApp registrations. The following examples show what to expect, not whole replacement files.

Synapse (`/opt/matrix/synapse/homeserver.yaml`):

```yaml
experimental_features:
  msc4143_enabled: true
  msc4354_enabled: true
  msc4222_enabled: true
  msc4502_enabled: true
  msc4512_enabled: true
max_event_delay_duration: 24h
matrix_rtc:
  transports:
    - type: m.livekit
      url: wss://matrix.example.com/livekit/sfu
      livekit_service_url: https://matrix.example.com/livekit/jwt
    - type: livekit
      url: wss://matrix.example.com/livekit/sfu
      livekit_service_url: https://matrix.example.com/livekit/jwt
```

MSC4354 sticky events must be enabled for modern RTC membership and notifications. The worker checks the advertised MSC support before modern call setup; legacy room-state calls do not require sticky events. Existing explicit `max_event_delay_duration` is retained. Existing unrelated YAML and appservice paths are preserved. Additions to `app_service_config_files` are:

```yaml
  - /data/matrixrtc-registration.yaml
  - /data/matrixrtc-doublepuppet-registration.yaml
```

The JWT registration uses ID `matrixrtc-jwt-service`, sender `_matrixrtc_jwt_service`, `url: null`, a non-exclusive user namespace, MSC4502 scope `urn:matrix:client:io.element.msc4502:rooms:is_joined`, MSC4512 proxy prefix `rtc/livekit` and proxy URL `http://lk-jwt-service:8080`. The double-puppet registration uses ID `matrixrtc-whatsapp-doublepuppet`, sender `_matrixrtc_doublepuppet`, `url: null`, `rate_limited: false`, and a non-exclusive namespace restricted to local MXIDs. Their tokens must match their corresponding generated copies. Neither replaces the ordinary WhatsApp appservice registration.

For reference, the full registration shapes rendered by `prepare.sh` are below. These are explanatory templates, **not files to copy over generated credentials**. The generated files contain real, locally generated tokens.

JWT registration (`matrixrtc-registration.yaml`):

```yaml
id: "matrixrtc-jwt-service"
as_token: "<generated as_token>"
hs_token: "<generated hs_token>"
sender_localpart: "_matrixrtc_jwt_service"
url: null
namespaces:
  users:
    - exclusive: false
      regex: ".*"
io.element.msc4502.scopes:
  - "urn:matrix:client:io.element.msc4502:rooms:is_joined"
io.element.msc4512.proxy_prefix: "rtc/livekit"
io.element.msc4512.proxy_url: "http://lk-jwt-service:8080"
```

Local double-puppet registration (`matrixrtc-doublepuppet-registration.yaml`, example domain; the generated regex escapes the actual domain):

```yaml
id: "matrixrtc-whatsapp-doublepuppet"
url: null
as_token: "<generated double_puppet_as_token>"
hs_token: "<generated double_puppet_hs_token>"
sender_localpart: "_matrixrtc_doublepuppet"
rate_limited: false
namespaces:
  users:
    - regex: "^@[^:]+:matrix[.]example[.]com$"
      exclusive: false
```

Bridge (`/opt/matrix/mautrix-whatsapp/config.yaml`):

```yaml
network:
  call_start_notices: false
  experimental_whatsapp_voip: true
  rtc_gateway_socket: /run/mautrix-whatsapp-call/worker.sock
matrix:
  sync_direct_chat_list: true
encryption:
  allow: true
  default: true
double_puppet:
  servers: {}
  allow_discovery: false
  secrets:
    matrix.example.com: "as_token:<generated double puppet AS token>"
```

Existing unrelated mappings and existing discovery settings are retained. The script rejects conflicting double-puppet credentials instead of overwriting them. Enabling encryption by default applies to new portals; existing unencrypted rooms are not converted. Use the automatic appservice double-puppet flow rather than copying a human access token into the deployment.

## 8. HTTPS reverse proxy, WebSockets and firewall

The overlay exposes signaling through the existing public Matrix HTTPS virtual host; media is separate and does not traverse the HTTP reverse proxy. The public endpoints are:

| Endpoint/port | Purpose |
| --- | --- |
| `https://matrix.example.com/livekit/jwt` | Authenticated MatrixRTC JWT service |
| `wss://matrix.example.com/livekit/sfu` | LiveKit signaling and WebSocket |
| TCP 443 | Matrix API and public signaling via TLS proxy |
| TCP 7881 | Direct ICE/TCP media fallback |
| UDP 7882 | Direct ICE/UDP media |
| `127.0.0.1:7880/tcp` | LiveKit HTTP and signaling backend, loopback only |
| `127.0.0.1:18080/tcp` | JWT backend, loopback only (unless configured differently) |
| `127.0.0.1:8008/tcp` | Synapse HTTP endpoint required by installer, loopback only |

Open inbound TCP 443 and 7881, UDP 7882 in host/provider firewalls and forward media ports through NAT if required. Keep existing Matrix federation and SSH rules as appropriate. Do not expose 7880, 18080 or 8008 publicly. Docker published ports may bypass ordinary UFW input rules; also configure provider/firewall or Docker-aware forwarding policy. For example, if UFW is already used:

```bash
ufw allow 443/tcp
ufw allow 7881/tcp
ufw allow 7882/udp
ufw status verbose
```

Do not enable/reset a firewall over SSH without ensuring management access. Test externally, not only from the server. Some restrictive client networks require TURN; this overlay does not deploy TURN/TLS for LiveKit by default, and a Synapse TURN server alone does not configure LiveKit TURN. Do not promise connectivity on every network.

### nginx (host-installed reverse proxy)

Paste the supplied `nginx-443-locations.conf` into the existing Matrix TLS `server` block on port 443 (not the federation-only 8448 block), or add an `include` inside that block. Preserve existing Matrix routes, TLS and security settings. Both paths must strip the public prefix before proxying:

```nginx
location ^~ /livekit/jwt {
    rewrite ^/livekit/jwt/?(.*)$ /$1 break;
    proxy_pass http://127.0.0.1:18080;
    proxy_http_version 1.1;
    proxy_set_header Host $host;
    proxy_set_header X-Real-IP $remote_addr;
    proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
    proxy_set_header X-Forwarded-Proto https;
    proxy_read_timeout 60s;
}
location ^~ /livekit/sfu {
    rewrite ^/livekit/sfu/?(.*)$ /$1 break;
    proxy_pass http://127.0.0.1:7880;
    proxy_http_version 1.1;
    proxy_set_header Host $host;
    proxy_set_header X-Real-IP $remote_addr;
    proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
    proxy_set_header X-Forwarded-Proto https;
    proxy_set_header Upgrade $http_upgrade;
    proxy_set_header Connection "upgrade";
    proxy_read_timeout 120s;
    proxy_send_timeout 120s;
    proxy_buffering off;
}
```

If you changed `MATRIXRTC_JWT_HOST_PORT`, replace `18080` in the proxy. If nginx runs in another container, `127.0.0.1` means that container; use a compatible network/backend address and secure access instead of blindly copying host-loopback settings. Do not send UDP 7882 through an HTTP reverse proxy or CDN.

Validate and reload (host nginx):

```bash
nginx -t && systemctl reload nginx
```

A 502 response before starting LiveKit/JWT is expected; it must disappear after install. For other reverse proxies implement equivalent prefix stripping, WebSocket upgrade, trusted TLS and appropriate timeouts. The supplied nginx snippet is the canonical proxy example.

## 9. Run the installer (first install)

Check that the `.env` still contains `MATRIXRTC_ENABLE_CALLS=0`. Ensure the proxy changes are active and firewall/network access is ready. Use the existing Compose project name and working directory consistently.

```bash
cd /opt/matrix
docker compose -f docker-compose.yml -f mautrix-whatsapp-call/compose.yml config >/dev/null
bash mautrix-whatsapp-call/install-or-upgrade.sh
bash mautrix-whatsapp-call/verify-worker.sh
```

The installer prevalidates the deployment and builds the local images before changing configuration or pausing calling. A failed build leaves existing services running; local image tags and build cache may have changed. After a successful build, it normalizes filesystem permissions, temporarily pauses calling, reconciles credentials, merges configs/registrations, validates Compose again, starts LiveKit/JWT, restarts Synapse and the bridge, migrates existing eligible rooms and provisions ghost identities. It starts the background provisioner for future eligible portals and restores the call-enable setting from `.env` on success. On an error after pausing the worker it attempts to stop both the worker and the provisioner, leaving the saved `.env` unchanged. This is not an atomic rollback of configuration or room changes. Investigate nonzero exit status and confirm both services are stopped before retrying. The installer rejects a conflicting existing MatrixRTC transport rather than replacing another SFU silently.

Room migration uses the HTTPS homeserver URL and requires the actual mautrix-whatsapp configuration, including its ghost username template. Provisioning checks the registered namespace, functional bot, local encrypted DM membership and bridge-published portal metadata before creating a new ghost device; an existing valid prepared device remains reusable. Custom or older rooms missing that metadata may require bridge-side repair rather than forced provisioning. A conflicting MatrixRTC transport must be reconciled manually.

There is no need to run `docker compose up` for the new services before generating their config, manually create LiveKit rooms, manually generate a JWT, or use the WhatsApp registration in the main worker. The provisioner has the required read-only registration access; the worker has persistent identity state and the store passphrase instead.

## 10. Validate before enabling calls

Use the following Bash helper from `/opt/matrix` for all subsequent commands:

```bash
cd /opt/matrix
compose() {
    docker compose -f docker-compose.yml -f mautrix-whatsapp-call/compose.yml "$@"
}
compose ps
bash mautrix-whatsapp-call/verify-worker.sh
curl -fsS http://127.0.0.1:18080/healthz
```

The JWT check assumes default port 18080. Inspect public routes (replace the domain):

```bash
HOST="https://matrix.example.com"
curl -fsS "$HOST/_matrix/client/versions"
curl -fsS "$HOST/livekit/jwt/healthz"
curl -sS -o /dev/null -w "%{http_code}\n" "$HOST/livekit/sfu/"
```

The JWT health endpoint should respond successfully; the SFU root is an HTTP reachability check, not proof of a successful RTC session. The supplied `verify.sh` also requests the authenticated transports API. Run it interactively with a valid local Matrix access token and do not share the token or the unredacted output:

```bash
bash mautrix-whatsapp-call/verify.sh
```

Expect LiveKit transport entries advertising your own public HTTPS JWT and WSS SFU URLs. Check socket permissions and call toggle:

```bash
WORKER_ID="$(compose ps -q mautrix-whatsapp-call-worker)"
docker inspect "$WORKER_ID" --format "{{.State.Health.Status}}"
docker inspect "$WORKER_ID" --format "{{range .Config.Env}}{{println .}}{{end}}" |
    grep "^RTC_WORKER_ENABLE_CALLS="
compose exec -T mautrix-whatsapp stat -c "%u:%g %a %n" /run/mautrix-whatsapp-call/worker.sock
compose exec -T mautrix-whatsapp sh -c "grep \"^Gid:\" /proc/1/status"
```

Expected once ready: worker `healthy`, calls `0`, socket `0:29319 660 /run/mautrix-whatsapp-call/worker.sock` and bridge primary GID `29319` (substitute configured GID if changed). Confirm ordinary WhatsApp messaging remains functional. If not, leave calls disabled and use the troubleshooting guide.

## 11. Enable calling and test

Edit `/opt/matrix/.env` to set `MATRIXRTC_ENABLE_CALLS=1`, then run:

```bash
cd /opt/matrix
bash mautrix-whatsapp-call/install-or-upgrade.sh
bash mautrix-whatsapp-call/verify-worker.sh
```

Repeat the inspection from step 10. Expect `RTC_WORKER_ENABLE_CALLS=1` and installer success. Force-stop and reopen the Matrix client once so cached room/call eligibility refreshes. Test against a consenting WhatsApp contact with an eligible encrypted DM. Test incoming and outgoing audio/video, mute, camera changes, accept, decline and hangup in both directions. Check media on Wi-Fi and a separate mobile network. Run the full acceptance checklist in [OPERATIONS.md](OPERATIONS.md) before declaring a new server or dependency upgrade production-ready.

For existing groups, relay use and business contacts, call eligibility depends on room configuration and WhatsApp capabilities. Do not assume that a rendered call button means the destination accepts calls. Unsupported/self/control rooms and explicit WhatsApp business-call restrictions should not be bypassed. Do not confuse a successful health check with working media.

Before pausing call services, the installer performs read-only candidate validation of the hostname templates, existing credentials, proposed bridge configuration and Synapse merge. This detects common configuration conflicts without generating credentials or editing live configuration. It does not replace validation of the actual generated files, native service startup, or staging rollback tests.
