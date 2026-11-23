# Deployment overlay

This directory is copied to `/opt/matrix/mautrix-whatsapp-call/` beside the existing Matrix/Synapse Compose project.

The overlay adds LiveKit, the Element JWT service, the encrypted RTC worker, the automatic WhatsApp ghost identity provisioner, and a locally built mautrix-whatsapp image that contains the call bridge.

Persistent data is kept in:

- `generated/` — appservice registrations, LiveKit secret/config and worker store passphrase.
- `worker-state/` — persistent MatrixRTC ghost devices and encrypted crypto stores.

Both directories are private runtime state and must never be committed.

The supported server entry point is:

```bash
cd /opt/matrix
bash mautrix-whatsapp-call/install-or-upgrade.sh
```

The installer preserves existing generated credentials and worker state, temporarily disables calls while room/state migration runs, then restores the previous `MATRIXRTC_ENABLE_CALLS` value and verifies the worker protocol/socket wiring.

`MATRIXRTC_*` environment names and MatrixRTC registration filenames are retained for compatibility with the MatrixRTC protocol and existing deployments. They are not release/session identifiers.
