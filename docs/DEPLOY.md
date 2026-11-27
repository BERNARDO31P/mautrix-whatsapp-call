# Deployment

Follow the [installation guide](../deployment/mautrix-whatsapp-call/INSTALLATION.md) for first-time setup and the [operations guide](../deployment/mautrix-whatsapp-call/OPERATIONS.md) for upgrades and maintenance.

Calls are disabled by default (`MATRIXRTC_ENABLE_CALLS=0`). The installer preserves generated credentials and worker state, performs room migration and verifies the worker connection before restoring the configured calling state.

## Ghost crypto ownership

The worker keeps exclusive per-identity locks while its SDK client is cached. The automatic provisioner may prepare other identities concurrently, but it must retry an identity that is already owned. The native provisioner and worker use the same persistent `.crypto-owner.lock`; the worker also holds `.provision.lock` to exclude changes to the session or readiness marker. Do not delete either lock file. Stop the call worker before manually repairing a cached identity, and restart it after preparation succeeds. These locks coordinate cooperating processes on the same local filesystem, not independent copies of the identity store.

## Complete installation and operations reference

- [First installation, including LiveKit, JWT, keys, Synapse, reverse proxy and firewall](../deployment/mautrix-whatsapp-call/INSTALLATION.md)
- [Operations, upgrades, backup/restore, migration, troubleshooting and live acceptance](../deployment/mautrix-whatsapp-call/OPERATIONS.md)

The installer manages the LiveKit and JWT containers.

### Legacy MatrixRTC membership ownership recovery

The calling worker records ownership of each legacy room-state membership key in
`legacy-membership-ownership` inside the provisioned ghost identity directory,
before sending a join. Preserve this directory with the rest of the worker state.
A successful explicit leave and confirmed cancellation of its delayed leave clear
the record. Failed or interrupted operations retain it, including across restarts.
The worker refuses a new legacy join using a retained key rather than risking a
late departure overwriting the new membership. An independent room or key can
still be used; MSC-format memberships do not use this journal.

Drain existing calls before upgrading and reconcile any older
unconfirmed membership writes: the journal cannot reconstruct operations that
predate it. If reuse is refused, do not blindly delete the marker, replace the
crypto store, or restart the worker expecting the refusal to disappear. Stop the
worker and reconcile the exact room/device membership and all outstanding delayed
state writes with the homeserver before removing the corresponding record as an
explicit operator recovery action. If that cannot be established, retain the
record. Automatic reconciliation of unknown delayed-event IDs is not provided.

Each delayed creation also has an independent operation record under
`delayed-membership-operations` in the same identity directory. The operation
contains a local token, room, slot, member, user and device; `delay_id: null`
means submission may have happened without an identifiable response. A known
`delay_id` is the exact server handle, not a Matrix event ID. A remaining
`.identified` file is evidence of an interrupted ID update. Both directories
must be preserved in backups. An unresolved operation independently fences
reuse of the corresponding legacy key, even if its ownership marker is absent.
Malformed journal data causes conservative refusal rather than guessing.

A worker join with ambiguous delayed creation is refused before publishing the
membership; subsequent arm attempts for that unresolved generation are refused.
A definitive unsupported response still allows a degraded-lifetime join. A
successful cancellation settles only its matching operation; `M_NOT_FOUND`
terminates cancellation retries but does not establish that a legacy state write
cannot still arrive. A restart does not automatically cancel outstanding IDs
because doing so before confirming membership departure could remove the dead
man's switch from a still-live membership. For recovery, inspect the records
locally as an administrator with the worker stopped, reconcile the exact IDs and
state key against the authoritative homeserver and preserve unresolved evidence.
Never infer an unknown ID or remove a record merely because a timeout elapsed.
