# Architecture

The existing mautrix-whatsapp process remains the owner of WhatsApp sessions, portals and message bridging. Calling is added as an extension rather than a second WhatsApp bridge.

For a direct call, mautrix-whatsapp validates the Matrix room, caller, device and RTC slot, checks WhatsApp callability, starts or accepts the WhatsApp call, and exchanges media/control messages with the RTC worker over a private Unix socket. The Rust worker joins the MatrixRTC/LiveKit side with persistent encrypted Matrix ghost identities.

For WhatsApp groups, the bridge bot is one aggregate MatrixRTC participant. Group audio is mixed and one authenticated WhatsApp video source is selected at a time to avoid interleaving independent H264 encoders.

The deployment uses:

- `mautrix-whatsapp` — existing bridge service, rebuilt with `mautrix-whatsapp-call-bridge:local`.
- `mautrix-whatsapp-call-worker` — encrypted MatrixRTC media worker.
- `mautrix-whatsapp-call-provisioner` — persistent cross-signed ghost identity provisioning sidecar.
- `livekit` — SFU.
- `lk-jwt-service` — MatrixRTC LiveKit token service.
- `synapse` — existing homeserver.

The bridge and worker communicate through `/run/mautrix-whatsapp-call/worker.sock` with mode `0660` and a shared numeric group. The main call worker never receives the mautrix-whatsapp appservice registration; only the provisioner does.
