# Security

Do not commit Matrix access tokens, mautrix-whatsapp appservice registrations, LiveKit secrets, generated MatrixRTC registrations, worker crypto stores or `.env` files.

The deployment keeps generated credentials under `deployment/mautrix-whatsapp-call/generated/` at runtime and persistent encrypted worker identity state under `worker-state/`; both are ignored by Git and should be mode `0700`/`0600` as appropriate.
