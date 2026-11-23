# Deployment

Building does not enable calls by itself. The runtime overlay keeps the call worker controlled by `MATRIXRTC_ENABLE_CALLS`, whose default is `0`.

The release deployment directory belongs at:

```text
/opt/matrix/mautrix-whatsapp-call/
```

Persistent runtime state:

```text
/opt/matrix/mautrix-whatsapp-call/generated/
/opt/matrix/mautrix-whatsapp-call/worker-state/
```

These directories must be preserved across upgrades and kept private.

Validate Compose before installation:

```bash
cd /opt/matrix
docker compose -f docker-compose.yml -f mautrix-whatsapp-call/compose.yml config >/dev/null
```

Install or upgrade:

```bash
cd /opt/matrix
bash mautrix-whatsapp-call/install-or-upgrade.sh
```

Verify the worker afterward:

```bash
cd /opt/matrix
bash mautrix-whatsapp-call/verify-worker.sh
```

The installer pauses calls while Synapse/bridge state and existing rooms are migrated, provisions existing eligible ghost identities, starts the continuous provisioner, restores the previous call-enable setting and checks the worker protocol/socket wiring before reporting success.
