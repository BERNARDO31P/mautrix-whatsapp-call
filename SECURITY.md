# Security

Do not commit Matrix access tokens, mautrix-whatsapp appservice registrations, LiveKit secrets, generated MatrixRTC registrations, worker crypto stores or `.env` files.

The deployment keeps generated credentials under `deployment/mautrix-whatsapp-call/generated/` at runtime and persistent encrypted worker identity state under `worker-state/`; both are ignored by Git and should be mode `0700`/`0600` as appropriate.

See the [installation guide](deployment/mautrix-whatsapp-call/INSTALLATION.md#6-livekit-installation-and-credential-generation) for how credentials are generated and the [operations guide](deployment/mautrix-whatsapp-call/OPERATIONS.md#secret-recovery-and-rotation) for safe recovery. Preserve generated credentials and encrypted state as a pair; never rotate the worker passphrase by deleting files.
