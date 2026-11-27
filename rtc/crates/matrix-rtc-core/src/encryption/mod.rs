// Copyright 2026 Element Creations Ltd.
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE in the repository root for full details.

// GNU Affero General License for more details.

//! Encryption manager for Matrix RTC sessions.
//!
//! This module provides key management for encrypted RTC sessions, implementing
//! the key distribution architecture described in [MSC4143](https://github.com/matrix-org/matrix-spec-proposals/pull/4143).
//!
//! # Architecture
//!
//! The encryption manager is responsible for:
//!
//! 1. **Key Generation**: Creating secure random 32-byte keys for media encryption
//! 2. **Key Distribution**: Sending keys to other participants via to-device messages (MSC4143)
//! 3. **Key Storage**: Maintaining inbound keys from other participants
//! 4. **Key Rotation**: Rotating keys when participants join/leave with grace period support
//! 5. **Signaling**: Notifying the application layer when new key material is available
//!
//! # Key Distribution Strategy (MSC4143)
//!
//! A rotation is not free: every member present has to be sent the new key, so
//! the traffic of one is proportional to the size of the call, and a policy that
//! rotates per event is quadratic in a busy one. What the rules below are for is
//! keeping the count down without letting anybody read what they should not.
//!
//! Every key is minted with an **expiry**, and rotating is nothing more than that
//! expiry arriving. Membership changes do not decide to rotate; they bring the
//! expiry forward:
//!
//! | event | effect on the expiry |
//! |---|---|
//! | minted | `creation + max_key_lifetime_ms` |
//! | a member holding it leaves, or their participation changes | `min(expiry, creation + grace)` |
//! | a member arrives and the key is no longer fresh | `now` |
//! | a member arrives and the key is still fresh | none — just send them the key |
//!
//! The departure rule needs no test for whether the key is fresh, which is what
//! makes this one instant rather than several rules: `min(expiry, creation + grace)`
//! lands in the future while the key is fresh (deferring, and coalescing every later
//! change into the same instant) and in the past once it has settled (rotating now).
//! It is idempotent, so a second leaver recomputes the same deadline.
//!
//! The asymmetry between the last two rows is the point. A joiner handed the current
//! key gains the past it has already encrypted, bounded by how old the key is — so
//! while it is young, sharing it is cheap, and a bulk join costs one rotation rather
//! than one per arrival. A leaver keeps the *future* of everything their key goes on
//! encrypting, which grows without end — so their key must always be retired; only
//! *when* is negotiable. An arrival therefore never leaves a rotation owed.
//!
//! Deferring to the end of freshness makes a burst cost two rotations however large
//! it is: the one that minted the key, and the one that expires it. Anchoring that
//! deadline to the key's own age rather than to the last change is what bounds it —
//! a call with a leaver every few seconds would otherwise push it back for ever. It
//! also caps a churning call at one rotation per grace period.
//!
//! The lifetime cap is what a quiet call relies on: without it one key would encrypt
//! a whole meeting, and anything that later recovered it would recover all of them.
//!
//! [`EncryptionManager::flush_due_rotation`] performs a deferred rotation; the
//! slot session's upkeep runs it at [`EncryptionManager::subscribe_rotation_due`]'s
//! deadline.
//!
//! Membership changes that arrive *together* are cheaper still: a rollout sees a
//! whole set of joined memberships, so any number of simultaneous changes cost one rotation between
//! them. Hosts should feed complete state rather than event by event — see
//! `BaseRtcRoom::set_current_sticky_state`.
//!
//! `KEY_ROTATION.md` at the repository root states the whole trade, with the costs
//! measured.
//!
//! # Key Usage Delay (MSC4143: delayBeforeUse)
//!
//! A newly distributed key is not necessarily used for encryption at once: the
//! members who hold the outgoing one need `delay_before_use_ms` to install its
//! replacement, and until they have, whatever we stamp with the new key is
//! undecryptable to them.
//!
//! Every rotation therefore waits, including one caused by a departure — which
//! does leave the member who left able to decrypt for up to
//! `delay_before_use_ms` after they are gone. That is a deliberate trade, not an
//! oversight: switching the moment somebody hangs up freezes video for everyone
//! still in the call while the replacement key travels, and a few seconds
//! readable to someone who has already left costs less than a call that stutters
//! whenever the joined memberships change.
//!
//! Whoever *arrives* at a rotation is handed the outgoing key as well as its
//! replacement. Without it the wait would blind them completely — they hold
//! nothing else — turning every join into an established call into a
//! `delay_before_use_ms` media blackout.
//!
//! Two cases carry no delay at all: the first key, signalled on the first
//! `on_memberships_update()` so the transport is listening, and a rotation whose
//! outgoing key no member present holds.
//!
//! This module does not *wait*, it only says how long to wait: the delay travels
//! to the consumer as [`KeyMaterialSignal::use_after_ms`], and the media layer
//! schedules activation, since only it knows when the transport installed the
//! key. Enforcing the delay is therefore a consumer obligation; the
//! `MediaKeyHandler` in `matrix-rtc-media` is the reference implementation.
//!
//! # Outdated Key Filtering
//!
//! In scenarios where participants quickly join/leave/join, keys might arrive
//! out of order. The `OutdatedKeyFilter` detects and drops outdated keys to
//! prevent decryption issues. If we receive a key at index N with timestamp T2,
//! then a key at the same index N with timestamp T1 < T2, the older key is dropped.
//!
//! # Integration with Application Layer
//!
//! The encryption manager signals new key material to the application layer via
//! the `EncryptionKeySignalHandler` trait. The application is responsible for:
//!
//! 1. Receiving the raw key bytes
//! 2. Applying key derivation/stretching as needed (e.g., using HKDF)
//! 3. Using the derived keys with the media encryption layer
//!
//! The raw bytes provided by the signal handler are the direct key material
//! that should be used with the application's key derivation function.
//!
//! # Example Usage
//!
//! ```no_run
//! use matrix_rtc_core::{CommandError, EncryptionConfig, EncryptionManager, JoinedMembership, KeyMaterialSignal, KeyOrigin, ReceivedEncryptionKey, MatrixBackend, ToDeviceDelivery, ToDeviceRecipient};
//! use async_trait::async_trait;
//! use std::sync::Arc;
//! use base64::{Engine as _, engine::general_purpose};
//!
//! // Implement MatrixBackend for your platform
//! struct MyBackend;
//!
//! #[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
//! #[cfg_attr(not(target_arch = "wasm32"), async_trait)]
//! impl MatrixBackend for MyBackend {
//!     fn own_user_id(&self) -> String { "@alice:example.org".to_owned() }
//!     fn own_device_id(&self) -> String { "DEVICE".to_owned() }
//!     async fn send_sticky_event(&self, _room_id: String, _event_type: String, _content: serde_json::Value, _duration_ms: u64) -> Result<String, CommandError> {
//!         Ok("$event".to_owned())
//!     }
//!     async fn send_delayed_state_event(&self, _room_id: String, _event_type: String, _state_key: String, _content: serde_json::Value, _delay_ms: u64) -> Result<String, CommandError> {
//!         Ok(String::new())
//!     }
//!     async fn send_delayed_sticky_event(&self, _room_id: String, _event_type: String, _content: serde_json::Value, _sticky_duration_ms: u64, _delay_ms: u64) -> Result<String, CommandError> {
//!         Ok(String::new())
//!     }
//!     async fn restart_delayed_event(&self, _room_id: String, _event_id: String) -> Result<(), CommandError> {
//!         Ok(())
//!     }
//!     async fn cancel_delayed_event(&self, _room_id: String, _event_id: String) -> Result<(), CommandError> {
//!         Ok(())
//!     }
//!     async fn send_to_device_message(&self, recipients: Vec<ToDeviceRecipient>, _message_type: String, _content: serde_json::Value) -> Result<Vec<ToDeviceDelivery>, CommandError> {
//!         Ok(recipients.into_iter().map(ToDeviceDelivery::sent).collect())
//!     }
//!     async fn send_state_event(&self, _room_id: String, _event_type: String, _state_key: String, _content: serde_json::Value) -> Result<String, CommandError> {
//!         Ok("$event".to_owned())
//!     }
//!     async fn send_room_event(&self, _room_id: String, _event_type: String, _content: serde_json::Value) -> Result<String, CommandError> {
//!         Ok("$event".to_owned())
//!     }
//!     async fn redact_event(&self, _room_id: String, _event_id: String, _reason: Option<String>) -> Result<(), CommandError> {
//!         Ok(())
//!     }
//! }
//!
//! // Create an encryption manager
//! let backend = Arc::new(MyBackend);
//! let get_memberships = || vec![];
//!
//! let mut manager = EncryptionManager::new(
//!     backend,
//!     "@alice:example.org".to_string(),
//!     "device123".to_string(),
//!     "xyzABCDEF0123".to_string(),  // member_id
//!     "!room:example.org".to_string(),
//!     "m.call#room".to_string(),
//!     get_memberships,
//! );
//!
//! // Configure (optional). Spread the defaults rather than listing every field,
//! // so a new policy knob does not break every caller that only cares about one.
//! manager.set_config(EncryptionConfig {
//!     delay_before_use_ms: 5000,
//!     key_rotation_grace_period_ms: 10000,
//!     ..EncryptionConfig::default()
//! });
//!
//! // Join the session (creates first key)
//! # tokio::runtime::Runtime::new().unwrap().block_on(async {
//! manager.join().await.unwrap();
//!
//! // Handle received keys (from to-device messages). `origin` carries the Olm
//! // decryption metadata, which is what the MSC4143 checks are made against.
//! manager.receive_key(ReceivedEncryptionKey {
//!     origin: KeyOrigin::Encrypted {
//!         sender_user_id: "@bob:example.org".to_string(),
//!         sender_device_id: Some("device456".to_string()),
//!         sender_is_confirmed: true,
//!     },
//!     room_id: "!room:example.org".to_string(),
//!     member_id: "bob-member-id".to_string(),
//!     key_b64: general_purpose::STANDARD.encode(vec![1u8; 32]),
//!     key_index: 0,
//! }).await.unwrap();
//!
//! // Keys reach the media layer through the signal handler installed above.
//! // A handler attached after keys arrived (the normal case — media connects
//! // some time after the slot is joined) calls `replay_keys_to_handler()` to
//! // receive the ones it missed.
//! # });
//! ```

pub mod types;

use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, Mutex, RwLock};

use async_trait::async_trait;
use base64::{Engine as _, engine::general_purpose};
use serde_json::json;
use tokio::sync::watch;
use types::*;

/// Closure type for getting current memberships, wrapped for thread-safety.
type GetMembershipsFn = Arc<Mutex<Box<dyn Fn() -> Vec<JoinedMembership> + Send>>>;

/// Source of "now", in epoch milliseconds (monotonic for local deadlines by default).
///
/// Every timing decision the manager makes — the key-rotation grace period, the
/// `delayBeforeUse` still owed on a replay — is a comparison against this, so
/// swapping it is what lets those decisions be exercised without waiting in real
/// time. See [`EncryptionManager::set_clock`].
pub type RtcClock = Arc<dyn Fn() -> u64 + Send + Sync>;

use crate::error::CommandError;
use crate::host::backend::{MatrixBackend, ToDeviceRecipient};
use crate::host::event::EventOrigin;
use crate::maybe_send::MaybeSend;
use crate::outbox::{SendFailure, SendKind, SendPolicy};
use crate::own_membership::{OwnMembershipMachine, OwnMembershipState};
use crate::session::JoinedMembership;
use crate::transport::{MIN_MEDIA_KEY_LEN, RtcTransport};

/// Message type for to-device encryption key distribution (MSC4143).
///
/// The stable id, as the core speaks everywhere; [`crate::wire_event_type`]
/// maps it to the unstable `io.element.call.encryption_keys` MSC4143 names, the
/// same type Element Call's legacy key shape travels under.
pub const KEY_MESSAGE_TYPE: &str = "m.rtc.encryption_key";

/// The engine for `media_key.key`: MSC4143 says unpadded base64, so we encode
/// without padding, and decode with or without it.
pub(crate) const MEDIA_KEY_BASE64: general_purpose::GeneralPurpose =
    general_purpose::GeneralPurpose::new(
        &base64::alphabet::STANDARD,
        general_purpose::GeneralPurposeConfig::new()
            .with_encode_padding(false)
            .with_decode_padding_mode(base64::engine::DecodePaddingMode::Indifferent),
    );

/// Maximum number of unmatched inbound media keys retained per session.
const MAX_PENDING_INBOUND_KEYS: usize = 4096;
/// Allow the membership event to catch up during call setup, not indefinitely.
/// This local retention limit is independent of the lifetime of admitted keys.
const MAX_PENDING_INBOUND_KEY_AGE_MS: u64 = 120_000;

fn pending_inbound_key_is_fresh(first_seen_ms: u64, now_ms: u64) -> bool {
    now_ms.saturating_sub(first_seen_ms) < MAX_PENDING_INBOUND_KEY_AGE_MS
}
/// Bound encoded key material before decoding (legacy 16-byte keys still work).
pub(crate) const MAX_ENCODED_MEDIA_KEY_LEN: usize = 4096;
/// Bound the decoded host DTO as well as the raw feeder envelope. Direct SDK
/// callers do not necessarily pass through the bounded to-device feeder.
const MAX_INBOUND_MEDIA_KEY_BYTES: usize = 16 * 1024;

pub(crate) fn inbound_key_fits_budget(key: &ReceivedEncryptionKey) -> bool {
    if key.key_b64.len() > MAX_ENCODED_MEDIA_KEY_LEN {
        return false;
    }
    let origin_bytes = match &key.origin {
        KeyOrigin::Encrypted { sender_user_id, sender_device_id, .. } => {
            sender_user_id.len().saturating_add(sender_device_id.as_ref().map_or(0, String::len))
        }
        KeyOrigin::Cleartext => 0,
    };
    key.room_id.len()
        .saturating_add(key.member_id.len())
        .saturating_add(key.key_b64.len())
        .saturating_add(origin_bytes) <= MAX_INBOUND_MEDIA_KEY_BYTES
}


/// Trait for handlers that receive key material signals.
///
/// Implementations receive notifications when new key material is available
/// for use by the media layer.
///
/// [`MaybeSend`] rather than `Send + Sync`: on the web the media layer is
/// livekit-client, so this handler is a JS callback holding a `JsValue`.
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
pub trait EncryptionKeySignalHandler: MaybeSend {
    /// Called when new key material is available for a participant.
    ///
    /// The application layer should use these raw bytes with key derivation
    /// to produce the actual encryption keys needed for media encryption/decryption.
    ///
    /// # Arguments
    /// * `signal` - Contains the raw key bytes, key index, and RTC backend identity
    async fn on_new_key_material(&self, signal: KeyMaterialSignal);

    /// An outbound activation has a single core-owned deadline. Native media
    /// handlers can carry the clock through to their scheduled task instead of
    /// starting a second relative timer. Other consumers retain the old API.
    async fn on_outbound_key_material(
        &self,
        mut signal: KeyMaterialSignal,
        activation_at_ms: u64,
        clock: RtcClock,
        is_current: Arc<dyn Fn() -> bool + Send + Sync>,
    ) {
        if is_current() {
            signal.use_after_ms = activation_at_ms.saturating_sub(clock());
            self.on_new_key_material(signal).await;
        }
    }

    /// Last successfully applied outbound key, if the handler can observe it.
    /// A handler with no media readback leaves timing-based tracking in place.
    fn active_outbound_key(&self, _identity: &str) -> Option<(u8, Vec<u8>)> {
        None
    }

    /// A native handler may still be applying the key after the planned
    /// activation instant. Keep a subsequent rotation from overtaking it.
    fn outbound_activation_pending(&self, _identity: &str, _index: u8, _key: &[u8]) -> bool {
        false
    }

    /// Called when a key was received but refused, with the reason.
    ///
    /// The media layer can only report *that* a participant's frames will not
    /// decrypt; the reason is visible here and nowhere else, because this is the
    /// only layer that sees the to-device message and its provenance. A refused
    /// key and a key that never arrived look identical downstream, yet one is a
    /// trust or configuration problem and the other a delivery one.
    ///
    /// Defaulted to a no-op so existing handlers need not change.
    async fn on_key_discarded(&self, discarded: DiscardedKey) {
        let _ = discarded;
    }
}

/// A media key that was received and refused, and why.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DiscardedKey {
    /// The member the key claimed to be for.
    pub member_id: String,
    /// The key's index, when the message got far enough for it to be meaningful.
    pub key_index: Option<u8>,
    /// The user the message was attributed to, if it was attributable at all.
    pub sender_user_id: Option<String>,
    /// The device it was attributed to, if known.
    pub sender_device_id: Option<String>,
    /// Why it was refused.
    pub reason: KeyRejection,
}

impl DiscardedKey {
    fn new(
        member_id: &str,
        key_index: Option<u8>,
        origin: &KeyOrigin,
        reason: KeyRejection,
    ) -> Self {
        let (sender_user_id, sender_device_id) = match origin {
            KeyOrigin::Encrypted {
                sender_user_id,
                sender_device_id,
                ..
            } => (Some(sender_user_id.clone()), sender_device_id.clone()),
            KeyOrigin::Cleartext => (None, None),
        };
        Self {
            member_id: member_id.to_owned(),
            key_index,
            sender_user_id,
            sender_device_id,
            reason,
        }
    }
}

/// What a member's RTC-backend participant identity is derived from.
pub struct IdentitySubject<'a> {
    /// The member's user id (the member event's `sender`).
    pub user_id: &'a str,
    /// The device that sent the member event, when it can be attributed.
    pub device_id: Option<&'a str>,
    /// The member's `member.id`.
    pub member_id: &'a str,
    /// The transports the member publishes on: how a backend issued its token,
    /// and so which identity it holds, can depend on them.
    pub transports: &'a [RtcTransport],
}

/// Maps a member to the RTC-backend participant identity that the media layer
/// addresses keys by; `None` when the member lacks what the derivation needs.
///
/// `matrix-rtc-core` stays agnostic about how that identity is derived: a
/// consumer (e.g. the LiveKit transport, whose identity follows the token flow
/// the published transport selects) injects the derivation via
/// [`EncryptionManager::set_identity_mapper`]. When no mapper is set, the
/// manager falls back to its default identity (`user_id:device_id` for the own
/// key, `member_id` for inbound keys).
pub type RtcIdentityMapper = Arc<dyn Fn(&IdentitySubject<'_>) -> Option<String> + Send + Sync>;

/// Serialize admission and the final follow-up decision. No mutex guard is
/// held over network awaits. Only an independently scheduled owner may accept
/// a fire-and-forget follow-up. An inline owner has no such lifetime guarantee.
struct DistributionState {
    running: bool,
    pending: bool,
    detached: bool,
    released: watch::Sender<()>,
}

impl Default for DistributionState {
    fn default() -> Self {
        Self {
            running: false,
            pending: false,
            detached: false,
            released: watch::Sender::new(()),
        }
    }
}

struct DistributionOwner {
    state: Arc<Mutex<DistributionState>>,
    active: bool,
}

enum DistributionAdmission {
    Owner(DistributionOwner),
    Scheduled,
    Wait(watch::Receiver<()>),
}

impl DistributionOwner {
    fn acquire(state: &Arc<Mutex<DistributionState>>, detached: bool) -> DistributionAdmission {
        let mut current = state.lock().unwrap();
        if current.running {
            if current.detached {
                current.pending = true;
                return DistributionAdmission::Scheduled;
            }
            // Subscribe while holding the admission lock. An inline owner's
            // release cannot race between observing it and subscribing.
            return DistributionAdmission::Wait(current.released.subscribe());
        }
        current.running = true;
        current.pending = false;
        current.detached = detached;
        DistributionAdmission::Owner(Self { state: state.clone(), active: true })
    }

    // The owner either consumes the pending request or releases the slot,
    // atomically with respect to a concurrent acquire.
    fn finish_pass(&mut self) -> bool {
        let mut current = self.state.lock().unwrap();
        if std::mem::take(&mut current.pending) {
            return true;
        }
        current.running = false;
        current.detached = false;
        self.active = false;
        current.released.send_replace(());
        false
    }
}

impl Drop for DistributionOwner {
    fn drop(&mut self) {
        if self.active {
            // An aborted owner must wake surviving inline waiters, even if it
            // never reached its first network await. There is no detached
            // work to recover once an entire executor has shut down.
            let mut current = self.state.lock().unwrap();
            current.running = false;
            current.detached = false;
            current.released.send_replace(());
        }
    }
}

/// Identity of the join against which an inbound key was authenticated.
/// Latest event IDs and refresh timestamps deliberately do not participate.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize)]
struct InboundJoin {
    member_id: String,
    sender: String,
    origin: EventOrigin,
    membership_ts: Option<u64>,
}

impl InboundJoin {
    fn of(membership: &JoinedMembership) -> Self {
        Self {
            member_id: membership.member_id.clone(),
            sender: membership.sender.clone(),
            origin: membership.origin.clone(),
            membership_ts: membership.membership_ts,
        }
    }

    fn matches(&self, membership: &JoinedMembership) -> bool {
        self.member_id == membership.member_id
            && self.sender == membership.sender
            && self.origin == membership.origin
            && self.membership_ts == membership.membership_ts
    }
}

#[derive(Clone)]
struct CachedInboundKeys {
    join: InboundJoin,
    keys: Vec<InboundEncryptionKey>,
}

fn unique_inbound_member<'a>(memberships: &'a [JoinedMembership], member_id: &str) -> Option<&'a JoinedMembership> {
    let mut matching = memberships.iter().filter(|membership| membership.member_id == member_id);
    let member = matching.next()?;
    if matching.next().is_some() { None } else { Some(member) }
}

/// The EncryptionManager manages encryption keys for an RTC session.
///
/// This implementation follows the architecture of the JS SDK's RTCEncryptionManager
/// but is implemented in a Rust-idiomatic way and complies with MSC4143.
///
/// # Responsibilities
///
/// - Generate and manage outbound encryption keys (32 secure random bytes)
/// - Distribute keys to participants via to-device messages (MSC4143)
/// - Receive and store inbound keys from other participants
/// - Signal new key material to the application layer
/// - Handle key rotation with grace period support
/// - Filter outdated keys to prevent decryption issues
pub struct EncryptionManager<T: MatrixBackend> {
    /// Command sender for sending to-device messages
    backend: Arc<T>,

    /// Session-scoped send admission; standalone encryption managers have no machine.
    membership_machine: Option<Arc<OwnMembershipMachine<T>>>,

    /// Our own user ID (e.g., "@alice:example.org")
    own_user_id: String,

    /// Our own member ID from the `m.rtc.member` event (MSC4143)
    own_member_id: String,

    /// Our device ID
    own_device_id: String,

    /// Function to get current memberships (joined participants)
    /// Wrapped in Arc<Mutex<...>> to allow cloning and Send even if the closure is not Sync.
    get_memberships: GetMembershipsFn,

    /// Room ID for this session
    room_id: String,

    /// Slot ID for this session
    slot_id: String,

    /// Current outbound key (None if not joined)
    outbound_key: Arc<RwLock<Option<OutboundEncryptionKey>>>,

    /// Inbound keys indexed by member ID and bound to the authenticated join.
    inbound_keys: Arc<RwLock<HashMap<String, CachedInboundKeys>>>,

    /// Configuration
    config: EncryptionConfig,

    /// Handler for key material signals
    signal_handler: Arc<RwLock<Option<Arc<dyn EncryptionKeySignalHandler>>>>,

    /// Optional consumer-supplied mapper from `(user_id, device_id, member_id)`
    /// to the RTC-backend participant identity (see [`RtcIdentityMapper`]).
    identity_mapper: Arc<RwLock<Option<RtcIdentityMapper>>>,
    /// The transports we publish on, from which our own identity derives.
    own_transports: Vec<RtcTransport>,

    /// The running owner and its queued follow-up share one admission lock.
    key_distribution: Arc<Mutex<DistributionState>>,

    /// Next key index to use (wraps at 256)
    next_key_index: Arc<Mutex<u8>>,

    /// Filter for detecting outdated keys
    key_buffer: Arc<Mutex<OutdatedKeyFilter>>,

    /// Keys that arrived before their membership was known (waiting for RTC membership)
    keys_without_membership: Arc<Mutex<Vec<PendingInboundKey>>>,

    /// Where "now" comes from (see [`RtcClock`]).
    clock: RtcClock,

    /// The key we are still encrypting with while the current one waits out its
    /// `delayBeforeUse`, and the instant that wait ends.
    ///
    /// `Some` exactly while a switch is in flight, which is the window
    /// [`Self::rollout_outbound_key`] coalesces rotations into.
    superseded_key: Arc<RwLock<Option<SupersededKey>>>,

    /// When a rotation deferred into the current switch window falls due.
    ///
    /// Set when a membership change that warrants a new key arrives while one is
    /// already propagating; cleared by the rotation that answers it. See
    /// [`Self::flush_due_rotation`], which is what makes it happen in a call
    /// where nothing else moves. A watch, so a scheduler re-arms on every move
    /// ([`Self::subscribe_rotation_due`]).
    rotation_due_at: Arc<watch::Sender<Option<u64>>>,

    /// Our outbound key as last signalled to the app, so the same rotation is
    /// not installed twice and a replay can honour the remaining
    /// `delayBeforeUse`.
    ///
    /// The eager first-key signal used to be gated on `shared_with.is_empty()`,
    /// which is also the state a rollout leaves behind when there was nobody to
    /// distribute to — so in a solo call every membership update re-signalled the
    /// same index, and each one reached the transport as a fresh key import.
    signalled_key: Arc<Mutex<Option<SignalledKey>>>,

    /// The members a rollout could not reach, by member id, with the key
    /// index each is owed (034 R32). Retried on backoff by the session's
    /// upkeep until delivered; a rotation replaces every entry, a departure
    /// drops its own.
    undelivered: Arc<watch::Sender<BTreeMap<String, Undelivered>>>,
}

/// A key a rollout could not hand to one member.
#[derive(Clone, Debug)]
pub(crate) struct Undelivered {
    target: ParticipantDeviceInfo,
    key_index: u8,
    // Identity of this obligation, not just the wrapping wire index.
    token: Arc<()>,
}

impl PartialEq for Undelivered {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.token, &other.token)
    }
}

/// What one retry of an undelivered key came to.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum KeyRetry {
    /// The member now holds the key.
    Delivered,
    /// Nothing is owed any more: the member left, or a rotation addressed them.
    Moot,
    /// The host answered but did not deliver to this member.
    Undelivered,
}

/// A key that has been replaced but is still what we encrypt with, because its
/// replacement is inside its `delayBeforeUse`.
///
/// Kept so that a member arriving mid-switch can be handed the key that is
/// actually on the wire. Without it they would hold only the pending one and
/// decrypt nothing until the window closed.
#[derive(Clone, Debug)]
struct SupersededKey {
    key: Vec<u8>,
    key_index: u8,
    /// When its replacement takes over.
    in_use_until_ms: u64,
}

/// The original activation timing of the current outbound key.
///
/// The timestamp is what makes a replay honest: a rotation signalled with a
/// `delayBeforeUse` has an install pending in the media layer, so replaying it at
/// `0` would install that index a second time *and* start encrypting with it
/// before peers have had their delay.
#[derive(Clone, Copy, Debug)]
struct SignalledKey {
    key_index: u8,
    /// When the activation window began, in local monotonic epoch milliseconds.
    at_ms: u64,
    /// The `delayBeforeUse` it was signalled with.
    use_after_ms: u64,
    dispatched: bool,
}

impl<T: MatrixBackend + 'static> EncryptionManager<T> {
    /// Creates a new EncryptionManager.
    ///
    /// # Arguments
    /// * `backend` - For sending to-device messages
    /// * `own_user_id` - Our Matrix user ID (for RTC backend identity)
    /// * `own_device_id` - Our device ID (for RTC backend identity)
    /// * `own_member_id` - Our `member.id` from the `m.rtc.member` event (MSC4143)
    /// * `room_id` - The room ID for this session
    /// * `slot_id` - The slot ID for this session
    /// * `get_memberships` - Function to get current joined memberships
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        backend: Arc<T>,
        own_user_id: String,
        own_device_id: String,
        own_member_id: String,
        room_id: String,
        slot_id: String,
        get_memberships: impl Fn() -> Vec<JoinedMembership> + Send + 'static,
    ) -> Self {
        Self {
            backend,
            membership_machine: None,
            own_user_id,
            own_member_id,
            own_device_id,
            get_memberships: Arc::new(Mutex::new(Box::new(get_memberships))),
            room_id,
            slot_id,
            outbound_key: Arc::new(RwLock::new(None)),
            inbound_keys: Arc::new(RwLock::new(HashMap::new())),
            config: EncryptionConfig::default(),
            signal_handler: Arc::new(RwLock::new(None)),
            identity_mapper: Arc::new(RwLock::new(None)),
            own_transports: Vec::new(),
            key_distribution: Arc::new(Mutex::new(DistributionState::default())),
            next_key_index: Arc::new(Mutex::new(0)),
            key_buffer: Arc::new(Mutex::new(OutdatedKeyFilter::default())),
            keys_without_membership: Arc::new(Mutex::new(Vec::new())),
            clock: Arc::new(crate::own_membership::now_ms),
            superseded_key: Arc::new(RwLock::new(None)),
            rotation_due_at: Arc::new(watch::Sender::new(None)),
            signalled_key: Arc::new(Mutex::new(None)),
            undelivered: Arc::new(watch::Sender::new(BTreeMap::new())),
        }
    }

    /// Replaces the source of "now" (see [`RtcClock`]).
    ///
    /// The manager's timing rules are pure comparisons against this value, and
    /// the intervals they compare against are seconds long: a test that had to
    /// pass real time to cross a grace period would be slow and would race the
    /// boundary it is checking. Tests therefore drive a clock they advance
    /// themselves.
    pub fn set_clock(&mut self, clock: RtcClock) {
        self.clock = clock;
    }

    /// Bind the manager to the exact join before cloning it into upkeep.
    pub(crate) fn set_membership_machine(&mut self, machine: Arc<OwnMembershipMachine<T>>) {
        self.membership_machine = Some(machine);
    }

    fn join_ended(&self) -> bool {
        self.membership_machine.as_ref().is_some_and(|machine| {
            machine.state() != OwnMembershipState::Joined
        })
    }

    /// Sets the configuration.
    pub fn set_config(&mut self, config: EncryptionConfig) {
        self.config = config;
    }

    /// Sets the handler for key material signals.
    ///
    /// Keys that arrived before this was installed were stored but not
    /// signalled — see [`Self::replay_keys_to_handler`], which the consumer
    /// should call once the identity mapper is in place too.
    pub fn set_signal_handler(&mut self, handler: Arc<dyn EncryptionKeySignalHandler>) {
        *self.signal_handler.write().unwrap() = Some(handler);
    }

    // Upkeep is cloned before the media handler is registered. Every clone
    // must observe the current handler; release the lock before invoking it.
    fn current_signal_handler(&self) -> Option<Arc<dyn EncryptionKeySignalHandler>> {
        self.signal_handler.read().unwrap().clone()
    }

    /// Re-signals every key we already hold to the current handler.
    ///
    /// Signals are dropped when no handler is installed, and a key is otherwise
    /// only re-signalled by a rotation — which needs a membership change. So a
    /// handler attached after keys arrived (the normal case: media connects some
    /// time after the slot is joined) would never hear about them, and the
    /// transport would fail to decrypt those participants for the rest of the
    /// call.
    ///
    /// Install the identity mapper **before** calling this. Identities are
    /// derived here exactly as the live signal paths derive them, so replaying
    /// without the mapper imports keys under fallback identities the transport
    /// never sees — which looks identical to not replaying at all.
    pub async fn replay_keys_to_handler(&self) {
        let Some(handler) = self.current_signal_handler() else {
            log::debug!("no signal handler to replay keys to");
            return;
        };

        let mut signals: Vec<(Option<InboundJoin>, KeyMaterialSignal)> = Vec::new();

        // Replay the current outbound key using its original activation deadline.
        // The helper rechecks exact key material before dispatch, so a snapshot
        // taken before a rotation cannot re-install its predecessor afterward.
        if let Some(outbound) = self.get_outbound_key() {
            self.signal_outbound_to_handler(&outbound, handler.clone()).await;
        }

        // Snapshot the generation along with the key. Recheck it before each
        // callback, since a preceding callback may itself await and allow a
        // roster update or a replacement at the same index.
        let held = self.inbound_keys.read().unwrap().clone();
        for cached in held.into_values() {
            for key in cached.keys {
                signals.push((Some(cached.join.clone()), KeyMaterialSignal {
                    key: key.key,
                    key_index: key.key_index,
                    rtc_backend_identity: String::new(),
                    use_after_ms: 0,
                }));
            }
        }

        for (join, mut signal) in signals {
            if let Some(join) = join {
                let Some(identity) = self.current_inbound_identity(&join, signal.key_index, &signal.key) else {
                    continue;
                };
                signal.rtc_backend_identity = identity;
            }
            handler.on_new_key_material(signal).await;
        }
    }

    /// Sets the identity mapper used to derive the RTC-backend participant
    /// identity carried in [`KeyMaterialSignal`]s (see [`RtcIdentityMapper`]).
    pub fn set_identity_mapper(&mut self, mapper: RtcIdentityMapper) {
        *self.identity_mapper.write().unwrap() = Some(mapper);
    }

    /// Whether this manager sends and installs media keys at all: `false` in
    /// a room whose encryption is off.
    pub fn manages_media_keys(&self) -> bool {
        self.config.manage_media_keys
    }

    /// Sets the transports we publish on, from which the mapper derives our
    /// own identity. Empty for a receive-only participation.
    pub fn set_own_transports(&mut self, transports: Vec<RtcTransport>) {
        self.own_transports = transports;
    }

    /// Our RTC backend identity: the mapper's, else `user_id:device_id`.
    fn own_rtc_identity(&self) -> String {
        let mapper = self.identity_mapper.read().unwrap().clone();
        mapper.as_ref()
            .and_then(|mapper| {
                mapper(&IdentitySubject {
                    user_id: &self.own_user_id,
                    device_id: Some(&self.own_device_id),
                    member_id: &self.own_member_id,
                    transports: &self.own_transports,
                })
            })
            .unwrap_or_else(|| self.get_own_rtc_backend_identity())
    }

    /// A peer's RTC backend identity: the mapper's, else their `member_id`.
    fn peer_rtc_identity(&self, membership: &JoinedMembership) -> Option<String> {
        let mapper = self.identity_mapper.read().unwrap().clone();
        match mapper {
            Some(mapper) => mapper(&IdentitySubject {
                user_id: &membership.sender,
                device_id: membership.origin.sender_device_id(),
                member_id: &membership.member_id,
                transports: &membership.transports,
            }),
            None => Some(membership.member_id.clone()),
        }
    }

    /// Gets the current timestamp in milliseconds.
    fn timestamp_ms(&self) -> u64 {
        (self.clock)()
    }

    /// The length of the keys we send: the longest any transport we publish
    /// on needs, and never under [`MIN_MEDIA_KEY_LEN`] (spec 038 R16).
    fn own_key_len(&self) -> usize {
        self.own_transports
            .iter()
            .filter_map(RtcTransport::required_key_len)
            .fold(MIN_MEDIA_KEY_LEN, usize::max)
    }

    /// Generates a new secure random key, as long as our transports need.
    ///
    /// Uses cryptographically secure random number generation via `OsRng`.
    pub fn generate_random_key(&self) -> Vec<u8> {
        use rand::RngCore;
        use rand_core::OsRng;

        let mut key = vec![0u8; self.own_key_len()];
        OsRng.fill_bytes(&mut key);
        key
    }

    /// Gets the next key index (0-255, wraps around).
    pub fn next_key_index(&self) -> u8 {
        let mut index = self.next_key_index.lock().unwrap();
        let result = *index;
        *index = index.wrapping_add(1);
        result
    }

    /// Gets our RTC backend identity.
    ///
    /// For now, uses the simple format: "user_id:device_id"
    /// In the future, this can be extended to use hashed identities as per MSC4143.
    pub fn get_own_rtc_backend_identity(&self) -> String {
        format!("{}:{}", self.own_user_id, self.own_device_id)
    }

    /// Gets our user ID.
    pub fn own_user_id(&self) -> &str {
        &self.own_user_id
    }

    /// Called when joining a session.
    ///
    /// Creates the first outbound key but does NOT signal it yet.
    /// The first key is signaled on the first `on_memberships_update()` call
    /// to ensure the transport is listening (as per JS SDK behavior).
    pub async fn join(&self) -> Result<(), CommandError> {
        log::info!(
            "[{}/{}] EncryptionManager joining",
            self.room_id,
            self.slot_id
        );

        if !self.config.manage_media_keys {
            log::debug!(
                "[{}/{}] Media keys management disabled",
                self.room_id,
                self.slot_id
            );
            return Ok(());
        }

        // Create the first outbound key
        let first_key = OutboundEncryptionKey {
            key: self.generate_random_key(),
            key_index: 0,
            creation_ts: self.timestamp_ms(),
            shared_with: Vec::new(),
        };

        // Store it
        {
            let mut outbound = self.outbound_key.write().unwrap();
            *outbound = Some(first_key.clone());
            *self.signalled_key.lock().unwrap() = None;
        }
        *self.next_key_index.lock().unwrap() = 1; // Next will be 1

        log::debug!(
            "[{}/{}] First outbound key created with index {}",
            self.room_id,
            self.slot_id,
            0
        );

        Ok(())
    }

    /// Called when leaving a session.
    ///
    /// Cleans up all state.
    pub fn leave(&self) {
        log::info!(
            "[{}/{}] EncryptionManager leaving",
            self.room_id,
            self.slot_id
        );

        *self.outbound_key.write().unwrap() = None;
        *self.inbound_keys.write().unwrap() = HashMap::new();
        *self.next_key_index.lock().unwrap() = 0;
        *self.signalled_key.lock().unwrap() = None;
        *self.superseded_key.write().unwrap() = None;
        self.set_rotation_due_at(None);
        self.undelivered.send_replace(BTreeMap::new());

        let mut buffer = self.key_buffer.lock().unwrap();
        buffer.buffer.clear();

        *self.keys_without_membership.lock().unwrap() = Vec::new();

        log::debug!(
            "[{}/{}] EncryptionManager state cleaned up",
            self.room_id,
            self.slot_id
        );
    }

    /// Called when memberships change (join/leave events).
    ///
    /// This triggers key distribution and signals the first key if not already done.
    pub async fn on_memberships_update(&self) -> Result<(), CommandError> {
        if !self.config.manage_media_keys {
            return Ok(());
        }

        // Purge departed or replaced joins before accepting buffered keys.
        self.prune_inbound_keys();
        self.check_keys_without_membership().await;

        // Check if we have an outbound key (i.e., we've joined)
        {
            let guard = self.outbound_key.read().unwrap();
            if guard.is_none() {
                // No outbound key yet, nothing to distribute
                return Ok(());
            }
        }

        // Signal our current key as soon as we have one, so the transport has a
        // key ring before any media flows — even when there is nobody to
        // distribute to yet.
        //
        // Gated on the index we last signalled, not on `shared_with.is_empty()`:
        // a rollout with no recipients also leaves `shared_with` empty, so the
        // old gate re-signalled the same index on every membership update. The
        // key material is identical, but each signal reaches the transport as a
        // fresh import of that index — which is the duplicate
        // `key index N imported` an Android integration observed.
        let should_signal = {
            let key = self.outbound_key.read().unwrap();
            let signalled = self.signalled_key.lock().unwrap();
            match key.as_ref() {
                Some(key) => signalled.filter(|signal| signal.dispatched).map(|signal| signal.key_index) != Some(key.key_index),
                None => false,
            }
        };

        if should_signal {
            // Clone the key to avoid holding the lock across await
            let key_to_signal = {
                let guard = self.outbound_key.read().unwrap();
                guard.clone()
            };
            if let Some(key) = key_to_signal {
                // The first key: signalled eagerly and usable at once, so the
                // transport has a key ring before any media flows.
                if let Some(handler) = self.current_signal_handler() {
                    self.signal_outbound_to_handler(&key, handler).await;
                }
            }
        }

        // Ensure key distribution
        self.ensure_key_distribution().await
    }

    /// Reclaim unmatched key material even when no membership or key events arrive.
    /// The session upkeep invokes this on its existing keep-alive beat; no network
    /// operation or independent task is needed.
    pub(crate) fn prune_expired_pending_keys(&self) {
        let mut waiting = self.keys_without_membership.lock().unwrap();
        let now = self.timestamp_ms();
        waiting.retain(|pending| pending_inbound_key_is_fresh(pending.first_seen_ms, now));
    }

    /// Checks keys that arrived before their membership was known.
    async fn check_keys_without_membership(&self) {
        let known_memberships = {
            let guard = self.get_memberships.lock().unwrap();
            (guard)()
        };
        // Partition under the lock, leaving unmatched entries in place. Do not
        // temporarily empty the queue across awaits: new arrivals would see a
        // false capacity reserve and old entries could overfill the queue.
        let keys_to_process = {
            let mut waiting = self.keys_without_membership.lock().unwrap();
            let mut ready = Vec::new();
            let now = self.timestamp_ms();
            waiting.retain(|pending| {
                if !pending_inbound_key_is_fresh(pending.first_seen_ms, now) {
                    return false;
                }
                if known_memberships.iter().any(|m| m.member_id == pending.key.member_id) {
                    ready.push(pending.clone());
                    false
                } else {
                    true
                }
            });
            ready
        };
        for pending in keys_to_process {
            // A preceding asynchronous import may have consumed the remaining
            // retention window. Do not start processing an expired entry.
            if !pending_inbound_key_is_fresh(pending.first_seen_ms, self.timestamp_ms()) {
                continue;
            }
            if let Some(membership) = unique_inbound_member(&known_memberships, &pending.key.member_id) {
                self.accept_verified_key(pending.key, &pending.origin, membership).await;
            }
        }
    }

    /// Ensures key distribution happens (with coalescing).
    ///
    /// If an independently scheduled distribution is already running, this
    /// coalesces rapid membership changes into a single follow-up. When the
    /// owner runs inline, concurrent requests instead wait for admission so
    /// aborting the first caller cannot lose acknowledged work.
    pub async fn ensure_key_distribution(&self) -> Result<(), CommandError> {
        if !self.config.manage_media_keys {
            return Ok(());
        }

        // Only an attached session has an outbox owning cancellation and send
        // deadlines. Its independently scheduled task can safely accept a
        // follow-up even if the original caller stops waiting. Standalone
        // managers and native hosts without an executor keep inline sends;
        // their concurrent callers must not acknowledge work that an aborted
        // owner could silently lose.
        let detached = self.membership_machine.is_some() && crate::executor::can_spawn();
        let owner = loop {
            match DistributionOwner::acquire(&self.key_distribution, detached) {
                DistributionAdmission::Owner(owner) => break owner,
                DistributionAdmission::Scheduled => {
                    log::debug!(
                        "[{}/{}] Key distribution in progress, scheduling follow-up",
                        self.room_id, self.slot_id
                    );
                    return Ok(());
                }
                DistributionAdmission::Wait(mut released) => {
                    // No mutex is held across the wait. A successful return
                    // requires this caller to run a pass or hand it to an
                    // independent owner, even if the previous owner vanished.
                    released.changed().await.map_err(|_| CommandError::ClientRejected(
                        "key distribution admission closed".to_owned()
                    ))?;
                }
            }
        };
        if detached {
            let manager = self.clone();
            crate::executor::spawn(async move {
                manager.run_distribution(owner).await;
            })
            .await
            .map_err(|error| CommandError::ClientRejected(format!(
                "key distribution task stopped: {error}"
            )))?;
        } else {
            self.run_distribution(owner).await;
        }
        Ok(())
    }

    async fn run_distribution(&self, mut owner: DistributionOwner) {
        loop {
            if let Err(error) = self.rollout_outbound_key().await {
                log::error!(
                    "[{}/{}] Failed to rollout key: {error:?}",
                    self.room_id,
                    self.slot_id,
                );
            }
            if !owner.finish_pass() {
                break;
            }
            log::debug!(
                "[{}/{}] Starting follow-up distribution",
                self.room_id,
                self.slot_id
            );
        }
    }

    /// Creates and distributes a new outbound key if needed.
    ///
    /// This implements the key rotation strategy from MSC4143:
    /// - If someone left OR membership timestamp changed: Always rotate key
    /// - If new joiners AND current key is young: Reuse current key (only send to new joiners)
    /// - If new joiners AND current key is old: Rotate key (send to all)
    async fn rollout_outbound_key(&self) -> Result<(), CommandError> {
        if self.join_ended() {
            return Ok(());
        }
        let current_memberships = (self.get_memberships.lock().unwrap())();

        // Build the list of recipients: every membership but this device's.
        //
        // Matching on `member_id` alone is not enough. It is fresh on every join
        // (MSC4143), so a stale membership of *our own* device — we died without
        // leaving and rejoined inside its sticky lifetime — reads as a peer, and
        // we would send our own key to ourselves. Olm has no session with the
        // sending device, so that send fails, on every rollout for as long as
        // the ghost stays visible.
        //
        // Other devices of our own user are ordinary recipients and stay.
        let current_participants: Vec<ParticipantDeviceInfo> = current_memberships
            .iter()
            .filter(|m| {
                let is_this_join = m.member_id == self.own_member_id;
                let is_this_device = m.sender == self.own_user_id
                    && m.origin.sender_device_id() == Some(self.own_device_id.as_str());
                !is_this_join && !is_this_device
            })
            .map(|m| ParticipantDeviceInfo {
                user_id: m.sender.clone(),
                device_id: m.origin.sender_device_id().unwrap_or_default().to_owned(),
                member_id: m.member_id.clone(),
                membership_ts: m.membership_ts,
            })
            .collect();

        // A member who left is owed nothing (034 R32).
        self.undelivered.send_if_modified(|undelivered| {
            let before = undelivered.len();
            undelivered.retain(|_, owed| {
                current_participants
                    .iter()
                    .any(|participant| participant.same_join(&owed.target))
            });
            undelivered.len() != before
        });

        let current_key = {
            let guard = self.outbound_key.read().unwrap();
            guard.clone()
        };

        if current_key.is_none() {
            log::warn!(
                "[{}/{}] No outbound key available, cannot distribute",
                self.room_id,
                self.slot_id
            );
            return Ok(());
        }

        let current_key = current_key.unwrap();
        let already_shared_with = current_key.shared_with.clone();

        // Both diffs compare participations with `same_join`, never `member_id`
        // alone: the pre-sticky Element Call dialect reuses `{user}:{device}` as
        // the member id, so a device that leaves and rejoins is a *new*
        // participation — a fresh session holding no keys — under the id we
        // already served. Matched by id alone, the return lands in neither list:
        // nothing is sent, and the rejoined member can never decrypt us. Matched
        // by join, the old participation stays in `left` (its rotation stays
        // owed) and the new one is in `joined` (it is served the current key).

        // Find participations that ended (previously shared with but no longer present)
        let left: Vec<&ParticipantDeviceInfo> = already_shared_with
            .iter()
            .filter(|x| !current_participants.iter().any(|o| o.same_join(x)))
            .collect();

        // Find new participations (present now but not previously shared with)
        let joined: Vec<&ParticipantDeviceInfo> = current_participants
            .iter()
            .filter(|x| !already_shared_with.iter().any(|o| o.same_join(x)))
            .collect();

        // Does anyone actually still holding the outgoing key stand to be
        // disrupted by us switching away from it?
        //
        // `delayBeforeUse` buys the *recipients of a new key* time to install it
        // while we keep encrypting with the previous one, so members already in
        // the call never see a gap. That trade only pays when someone present can
        // decrypt what we are currently stamping. When nobody can — the call sat
        // empty and the key we hold was distributed to nobody, or every member who
        // had it has left — holding on protects no one and costs the arriving
        // member the entire delay, during which they have no key that works at
        // all. Switching at once then leaves only the unavoidable wait for their
        // own to-device delivery.
        let outgoing_key_is_live = current_participants.iter().any(|participant| {
            already_shared_with
                .iter()
                .any(|holder| holder.same_join(participant))
        });

        // Kept past the branch below, which may consume `current_key`.
        let joined_participants: Vec<ParticipantDeviceInfo> =
            joined.iter().copied().cloned().collect();

        let now = self.timestamp_ms();
        let key_age = now.saturating_sub(current_key.creation_ts);

        // How long a key counts as *fresh*: young enough that handing it to a
        // newcomer leaks only a little of the past, and young enough that replacing
        // it would throw away most of its life.
        //
        // `delay_before_use_ms` is a floor because a key that has not even come
        // into use yet is certainly fresh — a grace period shorter than the
        // propagation delay would have us abandon keys that were distributed to
        // every member and never encrypted a frame.
        let freshness_ms = self
            .config
            .key_rotation_grace_period_ms
            .max(self.config.delay_before_use_ms);
        let key_is_fresh = key_age < freshness_ms;

        // When this key must be gone. Every rotation decision is an adjustment to
        // this one instant, and the rotation itself is just "it has arrived" — so
        // there is no separate notion of a key being dirty, only of expiring sooner.
        //
        // It starts at the lifetime cap, set when the key was minted, and is only
        // ever brought forward.
        let lifetime_ms = self.config.max_key_lifetime_ms.max(freshness_ms);
        let mut expires_at = current_key.creation_ts.saturating_add(lifetime_ms);
        let mut reason = (now >= expires_at).then(|| {
            format!(
                "the key has reached its maximum lifetime ({}ms)",
                self.config.max_key_lifetime_ms,
            )
        });

        if !left.is_empty() {
            // A participation that holds this key has ended — its member left, or
            // rejoined as a fresh session — so it has to be replaced, but not
            // necessarily this instant. Pulling the expiry back to the end of
            // freshness does both cases at once: while the key is fresh that is in
            // the future, which defers and coalesces every other change until then;
            // once it has settled that is in the past, which rotates now.
            //
            // Anchoring to the key's own age rather than to the departure is what
            // bounds the wait — a call with a leaver every few seconds would
            // otherwise push the deadline back for ever and never rotate at all —
            // and it makes a burst cost two rotations however large it is: the one
            // that minted this key, and the one that expires it.
            //
            // The participations that ended are not forgotten by deferring: they
            // stay in `shared_with` (only served recipients are ever added to it),
            // so every later rollout still sees them in `left` and keeps the
            // expiry pulled in. Nothing has to remember *why* a rotation is owed,
            // only when.
            expires_at = expires_at.min(current_key.creation_ts.saturating_add(freshness_ms));
            reason = reason.or_else(|| {
                Some(format!(
                    "{} member(s) who held the key have left",
                    left.len()
                ))
            });
        } else if !joined.is_empty() && !key_is_fresh {
            // An arrival is a reason to rotate only once the key has settled:
            // re-sharing it would hand over more of the call than the freshness
            // window allows. While it is fresh a newcomer is simply sent it, which
            // is what keeps a bulk join to one rotation — and, unlike a departure,
            // leaves nothing owed afterwards.
            expires_at = now;
            reason = reason.or_else(|| {
                Some(format!(
                    "a member arrived and the key is {key_age}ms old (fresh for {freshness_ms}ms)"
                ))
            });
        }

        if let Some(signal) = *self.signalled_key.lock().unwrap() {
            if signal.key_index == current_key.key_index {
                expires_at = expires_at.max(signal.at_ms.saturating_add(signal.use_after_ms));
            }
        }
        if now >= expires_at && self.current_signal_handler().is_some_and(|handler| {
            handler.outbound_activation_pending(&self.own_rtc_identity(), current_key.key_index, &current_key.key)
        }) {
            // The provider has not acknowledged activation yet. A bounded
            // timer recheck preserves owed rotations even without a callback.
            expires_at = now.saturating_add(100);
        }
        let rotating = now >= expires_at;
        match (&reason, rotating) {
            (Some(reason), true) => {
                log::info!("[{}/{}] rotating: {reason}", self.room_id, self.slot_id)
            }
            (Some(reason), false) => log::info!(
                "[{}/{}] rotation deferred to +{}ms ({reason}): key index {} is still fresh \
                 ({key_age}ms of {freshness_ms}ms)",
                self.room_id,
                self.slot_id,
                expires_at.saturating_sub(now),
                current_key.key_index,
            ),
            (None, _) => {}
        }

        // What a consumer's timer should fire on, describing the key this rollout
        // leaves in place — a fresh one expires on its own lifetime, not on the
        // deadline that just retired its predecessor.
        //
        // Always set, because every key expires: a scheduler always has a next
        // wake-up, and an owed rotation is simply one whose instant has not come yet.
        self.set_rotation_due_at(Some(if rotating {
            now.saturating_add(lifetime_ms)
        } else {
            expires_at
        }));

        if !rotating && joined_participants.is_empty() {
            // Nothing to rotate to and nobody waiting for what we already have.
            log::debug!(
                "[{}/{}] no distribution needed{}",
                self.room_id,
                self.slot_id,
                if reason.is_some() {
                    ", rotation deferred"
                } else {
                    ""
                },
            );
            return Ok(());
        }

        // How long we go on encrypting with the key we already have (MSC4143
        // `delayBeforeUse`), so that members who hold it keep decrypting while its
        // replacement propagates.
        //
        // Skipped when the key we are leaving behind is not live for anyone present
        // (see `outgoing_key_is_live`): there is no continuity to protect, and
        // waiting would only extend how long the arriving member has nothing it can
        // decrypt.
        //
        // A departure gets the same delay as an arrival, which does mean a member
        // who has left can go on decrypting for up to `delay_before_use_ms` after
        // they left. That is deliberate, and not a bug to be fixed: switching the
        // instant they go freezes video for everyone remaining while the
        // replacement key travels, and a few seconds readable to someone who just
        // hung up is a far better trade than a call that stutters every time
        // somebody leaves.
        let use_after_ms = match (rotating, outgoing_key_is_live) {
            (true, true) => self.config.delay_before_use_ms,
            _ => 0,
        };


        let to_distribute_to: Vec<ParticipantDeviceInfo>;
        let use_new_key = rotating;
        let outbound_key_to_use: OutboundEncryptionKey;

        if rotating {
            to_distribute_to = current_participants.clone();
            outbound_key_to_use = self.create_new_outbound_key();
        } else {
            // Either the key is inside its grace period, or a rotation is owed and
            // waiting: serve whoever does not hold what we have.
            log::debug!(
                "[{}/{}] keeping key index {} (age {key_age}ms) for {} arrival(s)",
                self.room_id,
                self.slot_id,
                current_key.key_index,
                joined_participants.len(),
            );
            to_distribute_to = joined_participants.clone();
            outbound_key_to_use = current_key;
        }

        // Send the key to every recipient in one call, and record only the
        // recipients it actually reached.
        //
        // This used to be a loop of single-recipient sends whose failures were
        // logged and then forgotten: `shared_with` was set to the full list
        // regardless, so a member the send never reached was treated as holding
        // the key and never re-sent to — they could not decrypt us for the rest
        // of the call. Recording only the served recipients makes the next
        // rollout see the rest as newly joined and try again.
        let key_b64 = MEDIA_KEY_BASE64.encode(&outbound_key_to_use.key);
        let Some(served) = self
            .send_key_to_participants(&key_b64, outbound_key_to_use.key_index, &to_distribute_to)
            .await
        else {
            // The leave cancelled this rollout; never commit or signal its key.
            return Ok(());
        };
        if self.join_ended() {
            return Ok(());
        }

        let unreached: Vec<&str> = to_distribute_to
            .iter()
            .map(|participant| participant.member_id.as_str())
            .filter(|member_id| {
                !served
                    .iter()
                    .any(|participant| participant.member_id == *member_id)
            })
            .collect();

        // One line per rollout saying how it went as a whole: per-recipient
        // errors are easy to miss in a busy log, and "distributed to 1 of 3" is
        // the shape of the problem.
        if unreached.is_empty() {
            log::info!(
                "[{}/{}] distributed key index {} to {} member(s)",
                self.room_id,
                self.slot_id,
                outbound_key_to_use.key_index,
                to_distribute_to.len(),
            );
        } else {
            log::warn!(
                "[{}/{}] distributed key index {} to {} of {} member(s); not delivered to {:?}, \
                 who cannot decrypt our media until a retry reaches them",
                self.room_id,
                self.slot_id,
                outbound_key_to_use.key_index,
                served.len(),
                to_distribute_to.len(),
                unreached,
            );
        }

        // An arrival is never handed the key still in use during a switch
        // window: it was minted before they joined, possibly long before, and
        // would let them read back over everything it encrypted, which is what
        // rotating on their arrival exists to prevent. They decrypt us once the
        // window closes, at most `delay_before_use_ms` after joining.

        // Update or store the outbound key
        if use_new_key {
            // Delivery can take longer than a previous switch window. Observe
            // the predecessor and start the next window only after delivery,
            // not at the earlier rotation decision.
            let switch_started_at = self.timestamp_ms();
            let key_still_in_use = if use_after_ms == 0 {
                None
            } else {
                self.key_in_use(switch_started_at)
            };
            {
                let mut guard = self.outbound_key.write().unwrap();
                if guard.is_none() || self.join_ended() {
                    return Ok(());
                }
                let mut new_key = outbound_key_to_use.clone();
                new_key.shared_with = served.clone();
                *guard = Some(new_key);
                *self.signalled_key.lock().unwrap() = Some(SignalledKey {
                    key_index: outbound_key_to_use.key_index,
                    at_ms: switch_started_at,
                    use_after_ms,
                    dispatched: false,
                });
                // Publish debt only after the matching key is committed. The
                // retry reads the key and debt under the same lock ordering.
                self.record_delivery_debt(
                    true,
                    outbound_key_to_use.key_index,
                    &to_distribute_to,
                    &served,
                );
                // Remember what stays on the wire for the length of the delay, and
                // until when. This is the window later rollouts coalesce into, and the
                // key a member arriving inside it has to be handed.
                *self.superseded_key.write().unwrap() = match (use_after_ms, key_still_in_use) {
                    (0, _) | (_, None) => None,
                    (delay, Some((in_use_index, in_use_key))) => Some(SupersededKey {
                        key: in_use_key,
                        key_index: in_use_index,
                        in_use_until_ms: switch_started_at.saturating_add(delay),
                    }),
                };
            }

            // Signal the new key immediately, telling the consumer how long to
            // wait before encrypting with it (`use_after_ms`, decided above). The
            // wait used to happen here, as a `tokio::time::sleep` — but this runs
            // on the caller's task, which for the FFI is a *synchronous* host call,
            // so it blocked a host thread for the whole delay (and panicked
            // outright when that thread had no runtime installed). Timing belongs
            // where a scheduler already exists: the media layer.
            //
            // The first key carries no delay — it is signalled on the first
            // `on_memberships_update()` so the transport is listening.
            log::debug!(
                "[{}/{}] Signalling key index {}, usable in {}ms ({})",
                self.room_id,
                self.slot_id,
                outbound_key_to_use.key_index,
                use_after_ms,
                if outgoing_key_is_live {
                    "a member present still holds the key we are replacing"
                } else {
                    "nobody present holds the key we are replacing"
                },
            );

            if let Some(handler) = self.current_signal_handler() {
                self.signal_outbound_to_handler(&outbound_key_to_use, handler).await;
            }
        } else {
            // Reusing the existing key: record only the recipients we reached,
            // so the rest are retried on the next rollout.
            //
            // Deduplicated per *join*, not per member id: a rejoined device is a
            // new participation under an old id, and its entry must land here or
            // every later rollout would re-send to it. The ended participation's
            // entry stays alongside it — that is what keeps the rotation it made
            // owed (via `left`) until the rotation mints a clean `shared_with`.
            {
                let mut guard = self.outbound_key.write().unwrap();
                if let Some(ref mut key) = *guard
                    && key.key_index == outbound_key_to_use.key_index
                    && key.key == outbound_key_to_use.key
                {
                    for recipient in &served {
                        if !key.shared_with.iter().any(|x| x.same_join(recipient)) {
                            key.shared_with.push(recipient.clone());
                        }
                    }
                    self.record_delivery_debt(false, key.key_index, &to_distribute_to, &served);
                }
            }
        }

        log::trace!(
            "[{}/{}] Key index:{} sent to {}",
            self.room_id,
            self.slot_id,
            outbound_key_to_use.key_index,
            to_distribute_to
                .iter()
                .map(|p| p.member_id.clone())
                .collect::<Vec<_>>()
                .join(",")
        );

        Ok(())
    }

    // Caller holds the outbound-key write lock until this update completes.
    fn record_delivery_debt(
        &self,
        rotating: bool,
        key_index: u8,
        recipients: &[ParticipantDeviceInfo],
        served: &[ParticipantDeviceInfo],
    ) {
        self.undelivered.send_if_modified(|undelivered| {
            let before = undelivered.clone();
            if rotating {
                undelivered.clear();
            }
            for participant in recipients {
                if served
                    .iter()
                    .any(|recipient| recipient.same_join(participant))
                {
                    undelivered.remove(&participant.member_id);
                } else {
                    undelivered.insert(
                        participant.member_id.clone(),
                        Undelivered {
                            target: participant.clone(),
                            key_index,
                            token: Arc::new(()),
                        },
                    );
                }
            }
            *undelivered != before
        });
    }

    /// The members a rollout could not reach, by member id (034 R32).
    pub fn undelivered_members(&self) -> Vec<String> {
        self.undelivered.borrow().keys().cloned().collect()
    }

    /// Follows the undelivered members; the upkeep runs one retry per member.
    pub(crate) fn subscribe_undelivered(&self) -> watch::Receiver<BTreeMap<String, Undelivered>> {
        self.undelivered.subscribe()
    }

    /// Capture the exact obligation before an outbox attempt is registered.
    pub(crate) fn delivery_debt(&self, member_id: &str) -> Option<Undelivered> {
        self.undelivered.borrow().get(member_id).cloned()
    }

    /// One retry of the captured obligation. Never substitute a later one.
    pub(crate) async fn retry_delivery_debt(
        &self,
        owed: &Undelivered,
    ) -> Result<KeyRetry, CommandError> {
        let member_id = &owed.target.member_id;
        let key = {
            let outbound = self.outbound_key.read().unwrap();
            if self.undelivered.borrow().get(member_id) != Some(owed) {
                return Ok(KeyRetry::Moot);
            }
            outbound.as_ref()
                .filter(|key| key.key_index == owed.key_index)
                .map(|key| key.key.clone())
        };
        let Some(key) = key else {
            self.drop_owed(owed);
            return Ok(KeyRetry::Moot);
        };

        let served = self
            .deliver_key(
                &MEDIA_KEY_BASE64.encode(&key),
                owed.key_index,
                std::slice::from_ref(&owed.target),
            )
            .await?;
        if served.is_empty() {
            return Ok(KeyRetry::Undelivered);
        }
        log::info!(
            "[{}/{}] key index {} delivered to member {} on retry",
            self.room_id,
            self.slot_id,
            owed.key_index,
            member_id,
        );
        if let Some(current) = self.outbound_key.write().unwrap().as_mut()
            && current.key_index == owed.key_index
            && current.key == key
            && self.undelivered.borrow().get(member_id) == Some(owed)
            && !current.shared_with.iter().any(|x| x.same_join(&owed.target))
        {
            current.shared_with.push(owed.target.clone());
        }
        self.drop_owed(owed);
        Ok(KeyRetry::Delivered)
    }

    /// Removes `owed`, unless a newer rollout has replaced it meanwhile.
    pub(crate) fn drop_owed(&self, owed: &Undelivered) {
        self.undelivered.send_if_modified(|undelivered| {
            let current = undelivered.get(&owed.target.member_id) == Some(owed);
            if current {
                undelivered.remove(&owed.target.member_id);
            }
            current
        });
    }

    /// The key we are encrypting with at `now`, and its index.
    ///
    /// Not always the one in `outbound_key`: a rotation is stored there as soon as
    /// it is made, but MSC4143 has us go on using its predecessor until
    /// `delayBeforeUse` has elapsed.
    fn key_in_use(&self, now: u64) -> Option<(u8, Vec<u8>)> {
        // Native media reports the last accepted outbound installation. Timer
        // expiry alone does not prove that the scheduled task has run.
        if let Some(handler) = self.current_signal_handler() {
            if let Some(active) = handler.active_outbound_key(&self.own_rtc_identity()) {
                return Some(active);
            }
        }
        if let Some(superseded) = self.superseded_key.read().unwrap().as_ref()
            && now < superseded.in_use_until_ms
        {
            return Some((superseded.key_index, superseded.key.clone()));
        }
        self.outbound_key
            .read()
            .unwrap()
            .as_ref()
            .map(|key| (key.key_index, key.key.clone()))
    }

    /// When a rotation deferred into the current switch window falls due, if one
    /// is owed.
    ///
    /// The session's scheduler drives [`Self::flush_due_rotation`] at the
    /// instant it comes up; see [`Self::subscribe_rotation_due`].
    /// [`Self::retry_delivery_debt`], for tests standing in for the upkeep
    /// that drives it: `true` when the member now holds the key.
    #[cfg(feature = "testing")]
    pub async fn retry_undelivered_key_now(&self, member_id: &str) -> Result<bool, CommandError> {
        let Some(owed) = self.delivery_debt(member_id) else {
            return Ok(false);
        };
        Ok(matches!(
            self.retry_delivery_debt(&owed).await?,
            KeyRetry::Delivered
        ))
    }

    pub fn rotation_due_at_ms(&self) -> Option<u64> {
        *self.rotation_due_at.borrow()
    }

    /// Follows [`Self::rotation_due_at_ms`]; wakes on every change.
    pub fn subscribe_rotation_due(&self) -> watch::Receiver<Option<u64>> {
        self.rotation_due_at.subscribe()
    }

    fn set_rotation_due_at(&self, due_at: Option<u64>) {
        self.rotation_due_at.send_if_modified(|current| {
            let changed = *current != due_at;
            *current = due_at;
            changed
        });
    }

    /// Performs a rotation that was deferred into a switch window, once it is due.
    ///
    /// A membership change arriving while a key is still propagating does not
    /// rotate — it records that a rotation is owed when that key comes into use
    /// (see [`Self::rollout_outbound_key`]). Something has to come back for it:
    /// the deferral is what keeps a burst of departures from costing a key each,
    /// and if nothing ever collected on it, a member who left during the window
    /// would keep the key they hold until the joined memberships happened to change again.
    ///
    /// A no-op when nothing is owed or the window has not closed, so it is safe to
    /// call on any tick a consumer already has.
    pub async fn flush_due_rotation(&self) -> Result<(), CommandError> {
        let Some(due_at) = self.rotation_due_at_ms() else {
            return Ok(());
        };
        let now = self.timestamp_ms();
        if now < due_at {
            return Ok(());
        }

        log::info!(
            "[{}/{}] performing the rotation deferred to {due_at} ({}ms ago)",
            self.room_id,
            self.slot_id,
            now.saturating_sub(due_at),
        );
        self.ensure_key_distribution().await
    }

    /// Creates a new outbound key.
    fn create_new_outbound_key(&self) -> OutboundEncryptionKey {
        OutboundEncryptionKey {
            key: self.generate_random_key(),
            key_index: self.next_key_index(),
            creation_ts: self.timestamp_ms(),
            shared_with: Vec::new(),
        }
    }

    /// Sends the key to every reachable participant, returning those the send
    /// actually reached.
    ///
    /// Recipients whose membership names no sending device are unreachable and
    /// are dropped before the call — MSC4143 puts key material on the device that
    /// encrypted the member event, and there is deliberately no `*` fallback
    /// (that hands the key to devices outside the call, and for our own user to
    /// this very device, which Olm cannot encrypt to).
    async fn send_key_to_participants(
        &self,
        key_b64: &str,
        index: u8,
        targets: &[ParticipantDeviceInfo],
    ) -> Option<Vec<ParticipantDeviceInfo>> {
        if targets.is_empty() {
            return (!self.join_ended()).then(Vec::new);
        }
        let outcome = if let Some(machine) = &self.membership_machine {
            // Register under the machine's state lock. A leave either cancels
            // this batch or prevents its registration entirely.
            let pending = machine.begin_if_joined(SendKind::KeyRollout)?;
            match pending
                .run(SendPolicy::single_attempt(), || self.deliver_key(key_b64, index, targets))
                .await
            {
                Ok(served) => Ok(served),
                Err(SendFailure::Cancelled | SendFailure::Superseded) => return None,
                Err(error) => Err(error.into_command_error()),
            }
        } else {
            // The standalone manager has no membership/outbox lifecycle.
            self.deliver_key(key_b64, index, targets).await
        };
        Some(outcome.unwrap_or_else(|error| {
            // A failed or ambiguous batch is not proof of delivery. Record
            // nobody as served and leave a retry obligation for the same key.
            log::error!(
                "[{}/{}] could not confirm key index {index} for {} recipient(s): {error}",
                self.room_id,
                self.slot_id,
                targets.len(),
            );
            Vec::new()
        }))
    }

    /// [`Self::send_key_to_participants`], with the host's error when the
    /// batch itself failed: a retry tells a signalling failure from a member
    /// the host could not deliver to.
    async fn deliver_key(
        &self,
        key_b64: &str,
        index: u8,
        targets: &[ParticipantDeviceInfo],
    ) -> Result<Vec<ParticipantDeviceInfo>, CommandError> {
        if targets.is_empty() {
            return Ok(Vec::new());
        }

        // The same content for every recipient: it names *our* member id and key,
        // not theirs.
        let content = json!({
            "room_id": self.room_id,
            "member_id": self.own_member_id,
            "media_key": {
                "index": index,
                "key": key_b64
            },
        });

        let memberships = (self.get_memberships.lock().unwrap())();
        let mut recipients = Vec::with_capacity(targets.len());
        let mut addressed = Vec::with_capacity(targets.len());

        for target in targets {
            // A retry is for a particular participation, not whichever device
            // happens to claim that member id now. Sticky refreshes retain the
            // generation; a legacy rejoin changes membership_ts.
            let mut candidates = memberships.iter().filter(|membership| {
                membership.room_id == self.room_id
                    && membership.slot_id == self.slot_id
                    && membership.member_id == target.member_id
            });
            let matches_target = |membership: &JoinedMembership| {
                !target.device_id.is_empty()
                    && membership.sender == target.user_id
                    && membership.origin.sender_device_id() == Some(target.device_id.as_str())
                    && membership.membership_ts == target.membership_ts
            };
            if !candidates.next().is_some_and(&matches_target)
                || !candidates.all(matches_target)
            {
                log::warn!(
                    "[{}/{}] cannot send key to member {}: no unambiguous matching join and device",
                    self.room_id,
                    self.slot_id,
                    target.member_id,
                );
                continue;
            }
            let user_id = &target.user_id;
            let device_id = target.device_id.as_str();

            // Recipient *and* index, on one line: an Android integration had to
            // add exactly this to discover that the core was addressing a key to
            // the device it was running on. Whether a member was addressed at
            // all, and with which index, is not answerable from any other line.
            log::debug!(
                "[{}/{}] sending key index {index} for member {} to {user_id}/{device_id}{}",
                self.room_id,
                self.slot_id,
                target.member_id,
                if user_id == &self.own_user_id && device_id == self.own_device_id {
                    " (ourselves — a homeserver will drop this)"
                } else {
                    ""
                },
            );

            recipients.push(ToDeviceRecipient::new(user_id.clone(), device_id));
            addressed.push(target.clone());
        }

        if recipients.is_empty() {
            return Ok(Vec::new());
        }

        let deliveries = self
            .backend
            .send_to_device_message(recipients, KEY_MESSAGE_TYPE.to_string(), content)
            .await?;

        // Match results back by recipient rather than by position: an
        // implementation is free to reorder or to answer for only some of them,
        // and treating an unanswered recipient as served is the failure this
        // whole shape exists to prevent.
        Ok(addressed
            .into_iter()
            .filter(|target| {
                let delivered = deliveries.iter().find(|delivery| {
                    delivery.recipient.user_id == target.user_id
                        && delivery.recipient.device_id == target.device_id
                });
                match delivered {
                    Some(delivery) if delivery.is_sent() => true,
                    Some(delivery) => {
                        log::error!(
                            "[{}/{}] key index {index} was not delivered to member {} ({}/{}): {}",
                            self.room_id,
                            self.slot_id,
                            target.member_id,
                            target.user_id,
                            target.device_id,
                            delivery.error.as_deref().unwrap_or("no reason given"),
                        );
                        false
                    }
                    None => {
                        log::error!(
                            "[{}/{}] the host reported no outcome for member {} ({}/{}); treating \
                             key index {index} as undelivered so it is retried",
                            self.room_id,
                            self.slot_id,
                            target.member_id,
                            target.user_id,
                            target.device_id,
                        );
                        false
                    }
                }
            })
            .collect())
    }

    /// Signals a key to the application layer.
    /// Reports a refused key to the handler, so the reason leaves the core.
    async fn signal_discarded_key(&self, discarded: DiscardedKey) {
        if let Some(handler) = self.current_signal_handler() {
            handler.clone().on_key_discarded(discarded).await;
        }
    }

    /// Signal or replay an outbound key using its original activation deadline.
    /// Record the first signal before invoking host code, rather than after an
    /// arbitrary callback delay. Rotation commits install the same marker while
    /// holding the outbound lock, before any racing membership replay sees it.
    async fn signal_outbound_to_handler(
        &self,
        key: &OutboundEncryptionKey,
        handler: Arc<dyn EncryptionKeySignalHandler>,
    ) {
        if self.join_ended() { return; }
        let activation_at_ms = {
            let outbound = self.outbound_key.read().unwrap();
            if !outbound.as_ref().is_some_and(|current| {
                current.key_index == key.key_index && current.key == key.key
            }) { return; }
            let mut marker = self.signalled_key.lock().unwrap();
            let signal = marker.get_or_insert(SignalledKey {
                key_index: key.key_index,
                at_ms: self.timestamp_ms(),
                use_after_ms: 0,
                dispatched: false,
            });
            if signal.key_index != key.key_index { return; }
            signal.at_ms.saturating_add(signal.use_after_ms)
        };
        let outbound = self.outbound_key.clone();
        let expected_index = key.key_index;
        let expected_key = key.key.clone();
        let joined = self.membership_machine.as_ref().map(|machine| machine.joined_probe());
        let is_current = Arc::new(move || {
            if joined.as_ref().is_some_and(|probe| !probe()) { return false; }
            outbound.read().unwrap().as_ref().is_some_and(|current| {
                current.key_index == expected_index && current.key == expected_key
            })
        });
        let signal = KeyMaterialSignal {
            key: key.key.clone(),
            key_index: key.key_index,
            rtc_backend_identity: self.own_rtc_identity(),
            use_after_ms: activation_at_ms.saturating_sub(self.timestamp_ms()),
        };
        handler.on_outbound_key_material(signal, activation_at_ms, self.clock.clone(), is_current).await;
        let outbound = self.outbound_key.read().unwrap();
        if outbound.as_ref().is_some_and(|current| current.key_index == key.key_index && current.key == key.key) {
            if let Some(marker) = self.signalled_key.lock().unwrap().as_mut() {
                marker.dispatched = true;
            }
        }
    }

    /// Checks an inbound key against the sender's `m.rtc.member` event.
    ///
    /// MSC4143: having matched the message's `member_id` to a member event,
    /// "clients verify that the sender and device that was used to send the
    /// member event match the sender and device of the to-device message.
    /// Otherwise the message MUST be discarded."
    fn verify_against_membership(
        origin: &KeyOrigin,
        membership: &JoinedMembership,
    ) -> Result<(), KeyRejection> {
        let KeyOrigin::Encrypted {
            sender_user_id,
            sender_device_id,
            ..
        } = origin
        else {
            return Err(KeyRejection::Cleartext);
        };

        if sender_user_id != &membership.sender {
            return Err(KeyRejection::SenderMismatch {
                expected: membership.sender.clone(),
                actual: sender_user_id.clone(),
            });
        }

        let expected_device = match &membership.origin {
            EventOrigin::Encrypted {
                sender_device_id: Some(device),
            } => device.as_str(),

            // A device the member event only claims (see `EventOrigin::Claimed`
            // — the pre-2026 Element Call path). Checking against it is weaker
            // than MSC4143 wants, but not vacuous: the key still has to arrive
            // Olm-encrypted from that exact device, so a forged claim only ever
            // names a device its author cannot send as.
            EventOrigin::Claimed { device_id } => device_id.as_str(),

            // Either the member event was encrypted but could not be attributed
            // to a device — which should not happen, since Olm messages carry
            // the sender's device keys — or it arrived in the clear. Both leave
            // no device to bind this key to, and MSC4143 makes that match a
            // MUST, so the check has not been satisfied. This is a backstop
            // rather than a path traffic is expected to take: skipping it would
            // quietly downgrade the requirement to a user-only match.
            EventOrigin::Encrypted {
                sender_device_id: None,
            }
            | EventOrigin::Cleartext => return Err(KeyRejection::UnverifiableDevice),
        };

        if sender_device_id.as_deref() != Some(expected_device) {
            return Err(KeyRejection::DeviceMismatch {
                expected: expected_device.to_owned(),
                actual: sender_device_id.clone(),
            });
        }

        Ok(())
    }

    /// Checks the parts of an inbound key that do not depend on the membership.
    fn verify_origin(&self, key: &ReceivedEncryptionKey) -> Result<(), KeyRejection> {
        if key.room_id != self.room_id {
            return Err(KeyRejection::RoomMismatch {
                claimed: key.room_id.clone(),
            });
        }

        match &key.origin {
            KeyOrigin::Cleartext => Err(KeyRejection::Cleartext),
            KeyOrigin::Encrypted {
                sender_is_confirmed,
                ..
            } if self.config.require_confirmed_sender && !sender_is_confirmed => {
                Err(KeyRejection::SenderNotConfirmed)
            }
            KeyOrigin::Encrypted { .. } => Ok(()),
        }
    }

    /// Receives an encryption key from a to-device message.
    ///
    /// This is called when we receive a to-device message with type
    /// `m.rtc.encryption_key` (on the wire, `io.element.call.encryption_keys`).
    ///
    /// Keys that fail the MSC4143 checks are discarded, and keys whose member
    /// event has not arrived yet are buffered together with their provenance so
    /// they can be checked once it does — a key is never signalled to the
    /// application before it has been verified.
    pub async fn receive_key(&self, received: ReceivedEncryptionKey) -> Result<(), CommandError> {
        if !inbound_key_fits_budget(&received) {
            log::warn!("Ignoring oversized inbound media key or provenance");
            return Ok(());
        }
        // What the host claimed, logged before we decide anything about it, so a
        // discard can be read against the message that caused it.
        match &received.origin {
            KeyOrigin::Encrypted {
                sender_user_id,
                sender_device_id,
                sender_is_confirmed,
            } => log::debug!(
                "[{}/{}] key index {} for member {} from {sender_user_id}/{} \
                 confirmed={sender_is_confirmed}",
                self.room_id,
                self.slot_id,
                received.key_index,
                received.member_id,
                sender_device_id.as_deref().unwrap_or("<unknown>"),
            ),
            KeyOrigin::Cleartext => log::debug!(
                "[{}/{}] key index {} for member {} arrived in cleartext",
                self.room_id,
                self.slot_id,
                received.key_index,
                received.member_id,
            ),
        }

        if let Err(rejection) = self.verify_origin(&received) {
            log::warn!(
                "[{}/{}] Discarding key index {} for member {}: {}",
                self.room_id,
                self.slot_id,
                received.key_index,
                received.member_id,
                rejection
            );
            self.signal_discarded_key(DiscardedKey::new(
                &received.member_id,
                Some(received.key_index),
                &received.origin,
                rejection,
            ))
            .await;
            return Ok(());
        }

        let key_bytes = MEDIA_KEY_BASE64.decode(&received.key_b64).map_err(|e| {
            CommandError::SerializationError(format!("Failed to decode key: {}", e))
        })?;

        // Only a room-state key can be short here: an MSC room's parser drops
        // one under 32 bytes (R16), while Element Call's room-state keys were
        // 16 bytes until js-sdk #5649 and are taken as they come.
        if key_bytes.len() < MIN_MEDIA_KEY_LEN {
            log::warn!(
                "[{}/{}] Received a key of {} bytes, shorter than LiveKit needs",
                self.room_id,
                self.slot_id,
                key_bytes.len()
            );
        }

        let inbound_key = InboundEncryptionKey {
            key: key_bytes,
            key_index: received.key_index,
            member_id: received.member_id.clone(),
            creation_ts: self.timestamp_ms(),
        };

        // Check if we know about this membership
        let known_memberships = {
            let guard = self.get_memberships.lock().unwrap();
            (guard)()
        };
        let membership = unique_inbound_member(&known_memberships, &received.member_id);
        if membership.is_none() && known_memberships.iter().any(|m| m.member_id == received.member_id) {
            log::warn!("Ignoring media key for ambiguous membership");
            return Ok(());
        }

        match membership {
            Some(membership) => {
                self.accept_verified_key(inbound_key, &received.origin, membership)
                    .await
            }
            None => {
                log::debug!(
                    "[{}/{}] No matching RTC membership for key from member {}, buffering",
                    self.room_id,
                    self.slot_id,
                    received.member_id
                );
                let mut waiting = self.keys_without_membership.lock().unwrap();
                let now = self.timestamp_ms();
                waiting.retain(|pending| pending_inbound_key_is_fresh(pending.first_seen_ms, now));
                // Only collapse an exact consecutive retransmission. Different
                // origins or material must still be verified in arrival order.
                if let Some(last) = waiting.last_mut() {
                    if last.key.member_id == inbound_key.member_id
                        && last.key.key_index == inbound_key.key_index
                        && last.key.key == inbound_key.key
                        && last.origin == received.origin
                    {
                        last.key.creation_ts = inbound_key.creation_ts;
                        return Ok(());
                    }
                }
                if waiting.len() >= MAX_PENDING_INBOUND_KEYS {
                    log::debug!("Pending inbound media key capacity reached; dropping unmatched key");
                    return Ok(());
                }
                waiting.push(PendingInboundKey {
                    key: inbound_key,
                    // Clone only the admitted bytes, not any spare allocation
                    // capacity retained by a direct SDK caller.
                    origin: received.origin.clone(),
                    first_seen_ms: now,
                });
            }
        }

        Ok(())
    }

    /// Verifies a key against its member event and, if it passes, stores and
    /// signals it.
    ///
    /// The outdated-key filter is only consulted for keys that got this far, so
    /// a rejected key cannot poison the filter and suppress the genuine key at
    /// the same index.
    async fn accept_verified_key(
        &self,
        key: InboundEncryptionKey,
        origin: &KeyOrigin,
        membership: &JoinedMembership,
    ) {
        if let Err(rejection) = Self::verify_against_membership(origin, membership) {
            log::warn!(
                "[{}/{}] Discarding key index {} for member {}: {}",
                self.room_id,
                self.slot_id,
                key.key_index,
                key.member_id,
                rejection
            );
            self.signal_discarded_key(DiscardedKey::new(
                &key.member_id,
                Some(key.key_index),
                origin,
                rejection,
            ))
            .await;
            return;
        }

        let join = InboundJoin::of(membership);
        // A legacy member ID is reused on rejoin. Its old timestamp filter
        // must not suppress new keys belonging to the replacement join.
        let Ok(filter_identity) = serde_json::to_string(&join) else {
            return;
        };
        let outdated = {
            let mut guard = self.key_buffer.lock().unwrap();
            // Entries older than the filter's TTL can no longer make anything
            // look outdated, so drop them here rather than keeping one per
            // (member, index) for the life of the session. This is the only place
            // keys are added, so it is the only place the buffer can grow.
            guard.cleanup(key.creation_ts);
            guard.check_and_add(filter_identity, key.key_index, key.creation_ts)
        };

        if outdated {
            log::info!(
                "[{}/{}] Received outdated key from member {}, index {}, dropping",
                self.room_id,
                self.slot_id,
                key.member_id,
                key.key_index
            );
            return;
        }

        self.add_key_to_participant(key, membership).await;
    }

    /// Adds a key to a participant.
    async fn add_key_to_participant(
        &self,
        key: InboundEncryptionKey,
        membership: &JoinedMembership,
    ) {
        let join = InboundJoin::of(membership);
        // Store the key: at most one entry per index, holding the newest material
        // we have seen for it.
        //
        // Both halves matter. A re-delivered *identical* key — the sender
        // re-distributing after a membership change, or two sticky updates racing
        // — must not be stored or signalled again: `OutdatedKeyFilter` cannot
        // catch it, because with no sender timestamp on the wire `creation_ts` is
        // stamped at receive time and a re-delivery never looks stale. Appending
        // instead grew a duplicate per delivery, signalled each as a fresh
        // import, and made every later replay re-signal all the copies.
        //
        // A *different* key at an index we already hold is a rekey, and the newer
        // material is what the sender is encrypting with, so it replaces the old
        // one rather than joining it. Keeping both would leave the replay order
        // deciding which one the transport ends up installed with — and replaying
        // the older one last silently downgrades us to a key the sender has
        // stopped using.
        let map_key = key.member_id.clone();
        {
            let mut guard = self.inbound_keys.write().unwrap();
            // Check under the cache admission lock so a delayed old snapshot
            // cannot overwrite a newer generation's already committed key.
            let current = self.current_memberships();
            if !unique_inbound_member(&current, &join.member_id).is_some_and(|m| join.matches(m)) {
                return;
            }
            let cached = guard.entry(map_key).or_insert_with(|| CachedInboundKeys {
                join: join.clone(),
                keys: Vec::new(),
            });
            if cached.join != join {
                *cached = CachedInboundKeys { join: join.clone(), keys: Vec::new() };
            }
            let held = &mut cached.keys;
            match held
                .iter_mut()
                .find(|existing| existing.key_index == key.key_index)
            {
                Some(existing) if existing.key == key.key => {
                    log::trace!(
                        "[{}/{}] key index {} for member {} is already held; not re-importing it",
                        self.room_id,
                        self.slot_id,
                        key.key_index,
                        key.member_id,
                    );
                    return;
                }
                Some(existing) => {
                    log::debug!(
                        "[{}/{}] member {} rekeyed index {}; replacing the key we held",
                        self.room_id,
                        self.slot_id,
                        key.member_id,
                        key.key_index,
                    );
                    *existing = key.clone();
                }
                None => held.push(key.clone()),
            }
        }

        // Signal to application
        match self.current_inbound_identity(&join, key.key_index, &key.key) {
            Some(rtc_backend_id) => self.signal_inbound_key_to_app(key, rtc_backend_id).await,
            None => log::debug!(
                "[{}/{}] key from {} has no derivable participant identity; stored only",
                self.room_id,
                self.slot_id,
                membership.member_id
            ),
        }
    }

    /// Signals an inbound key to the application layer.
    async fn signal_inbound_key_to_app(
        &self,
        key: InboundEncryptionKey,
        rtc_backend_identity: String,
    ) {
        if let Some(handler) = self.current_signal_handler() {
            let signal = KeyMaterialSignal {
                key: key.key.clone(),
                key_index: key.key_index,
                rtc_backend_identity,
                // Inbound keys are only ever used to decrypt; delaying one would
                // just drop the sender's frames until it elapsed.
                use_after_ms: 0,
            };

            // Note: We don't use tokio::spawn here because the handler's future might not be Send
            let handler_clone = handler.clone();
            let _ = handler_clone.on_new_key_material(signal).await;
        }
    }

    fn current_memberships(&self) -> Vec<JoinedMembership> {
        let guard = self.get_memberships.lock().unwrap();
        guard()
    }

    fn prune_inbound_keys(&self) {
        let mut cache = self.inbound_keys.write().unwrap();
        let memberships = self.current_memberships();
        cache.retain(|member_id, cached| {
            unique_inbound_member(&memberships, member_id).is_some_and(|m| cached.join.matches(m))
        });
    }

    fn current_inbound_identity(&self, join: &InboundJoin, index: u8, key: &[u8]) -> Option<String> {
        let memberships = self.current_memberships();
        let membership = unique_inbound_member(&memberships, &join.member_id)?;
        if !join.matches(membership) {
            return None;
        }
        {
            let held = self.inbound_keys.read().unwrap();
            let cached = held.get(&join.member_id)?;
            if &cached.join != join || !cached.keys.iter().any(|k| k.key_index == index && k.key.as_slice() == key) {
                return None;
            }
        }
        self.peer_rtc_identity(membership)
    }

    /// Gets inbound keys only for the currently authenticated join.
    pub fn get_inbound_keys(&self, member_id: &str) -> Vec<InboundEncryptionKey> {
        let memberships = self.current_memberships();
        let inbound_keys = self.inbound_keys.read().unwrap();
        inbound_keys.get(member_id)
            .filter(|cached| unique_inbound_member(&memberships, member_id).is_some_and(|m| cached.join.matches(m)))
            .map(|cached| cached.keys.clone())
            .unwrap_or_default()
    }

    /// Gets the current outbound key.
    pub fn get_outbound_key(&self) -> Option<OutboundEncryptionKey> {
        self.outbound_key.read().unwrap().clone()
    }

    /// Gets all inbound keys for the current, unambiguous roster.
    pub fn get_all_inbound_keys(&self) -> HashMap<String, Vec<InboundEncryptionKey>> {
        let memberships = self.current_memberships();
        self.inbound_keys.read().unwrap().iter()
            .filter(|(id, cached)| unique_inbound_member(&memberships, id).is_some_and(|m| cached.join.matches(m)))
            .map(|(id, cached)| (id.clone(), cached.keys.clone()))
            .collect()
    }
}

impl<T: MatrixBackend + 'static> Clone for EncryptionManager<T> {
    fn clone(&self) -> Self {
        Self {
            backend: self.backend.clone(),
            membership_machine: self.membership_machine.clone(),
            own_user_id: self.own_user_id.clone(),
            own_member_id: self.own_member_id.clone(),
            own_device_id: self.own_device_id.clone(),
            get_memberships: self.get_memberships.clone(),
            room_id: self.room_id.clone(),
            slot_id: self.slot_id.clone(),
            outbound_key: self.outbound_key.clone(),
            inbound_keys: self.inbound_keys.clone(),
            config: self.config.clone(),
            signal_handler: self.signal_handler.clone(),
            identity_mapper: self.identity_mapper.clone(),
            own_transports: self.own_transports.clone(),
            key_distribution: self.key_distribution.clone(),
            next_key_index: self.next_key_index.clone(),
            key_buffer: self.key_buffer.clone(),
            keys_without_membership: self.keys_without_membership.clone(),
            clock: self.clock.clone(),
            superseded_key: self.superseded_key.clone(),
            rotation_due_at: self.rotation_due_at.clone(),
            signalled_key: self.signalled_key.clone(),
            undelivered: self.undelivered.clone(),
        }
    }
}

impl<T: MatrixBackend + 'static> EncryptionManager<T> {
    /// Creates an Arc-wrapped clone of self.
    pub fn clone_arc(&self) -> Arc<Self> {
        Arc::new(self.clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::host::backend::{MockBackend, NoopBackend, ToDeviceDelivery};
    use crate::host::event::EventOrigin;
    use crate::session::JoinedMembership;
    use std::sync::Arc;

    fn admitted_owner(admission: DistributionAdmission) -> DistributionOwner {
        match admission {
            DistributionAdmission::Owner(owner) => owner,
            _ => panic!("expected distribution owner"),
        }
    }

    #[test]
    fn distribution_owner_coalesces_and_releases_atomically() {
        let state = Arc::new(Mutex::new(DistributionState::default()));
        let mut owner = admitted_owner(DistributionOwner::acquire(&state, true));
        assert!(matches!(DistributionOwner::acquire(&state, true), DistributionAdmission::Scheduled));
        assert!(matches!(DistributionOwner::acquire(&state, false), DistributionAdmission::Scheduled));
        assert!(owner.finish_pass());
        assert!(!owner.finish_pass());
        let next = admitted_owner(DistributionOwner::acquire(&state, true));
        // Dropping an already released owner must not release its successor.
        drop(owner);
        assert!(matches!(DistributionOwner::acquire(&state, true), DistributionAdmission::Scheduled));
        drop(next);
        assert!(matches!(DistributionOwner::acquire(&state, true), DistributionAdmission::Owner(_)));
    }

    #[test]
    fn cancelled_distribution_owner_does_not_latch_admission() {
        let state = Arc::new(Mutex::new(DistributionState::default()));
        let owner = admitted_owner(DistributionOwner::acquire(&state, true));
        assert!(matches!(DistributionOwner::acquire(&state, true), DistributionAdmission::Scheduled));
        drop(owner);
        let mut replacement = admitted_owner(DistributionOwner::acquire(&state, true));
        assert!(!replacement.finish_pass());
        assert!(matches!(DistributionOwner::acquire(&state, true), DistributionAdmission::Owner(_)));
    }

    #[test]
    fn inline_owner_release_is_observable_without_a_runtime() {
        let state = Arc::new(Mutex::new(DistributionState::default()));
        let owner = admitted_owner(DistributionOwner::acquire(&state, false));
        let DistributionAdmission::Wait(waiter) = DistributionOwner::acquire(&state, false) else {
            panic!("inline requests must not report scheduled work");
        };
        let DistributionAdmission::Wait(detached_waiter) = DistributionOwner::acquire(&state, true) else {
            panic!("a runtime does not make an existing inline owner independent");
        };
        assert!(!waiter.has_changed().unwrap());
        drop(owner);
        assert!(waiter.has_changed().unwrap());
        assert!(detached_waiter.has_changed().unwrap());
        let mut successor = admitted_owner(DistributionOwner::acquire(&state, false));
        let DistributionAdmission::Wait(waiter) = DistributionOwner::acquire(&state, false) else {
            panic!("expected waiter");
        };
        assert!(!successor.finish_pass());
        assert!(waiter.has_changed().unwrap());
    }

    const ROOM_ID: &str = "!room:example.org";
    const SLOT_ID: &str = "m.call#room";
    const USER_ID: &str = "@alice:example.org";
    const DEVICE_ID: &str = "device123";
    const MEMBER_ID: &str = "alice-device123-uuid";

    /// A clock the test advances by hand, so a grace period or a `delayBeforeUse`
    /// window can be crossed without waiting for it.
    #[derive(Clone)]
    struct TestClock(Arc<Mutex<u64>>);

    impl TestClock {
        fn new() -> Self {
            Self(Arc::new(Mutex::new(1_700_000_000_000)))
        }

        fn now(&self) -> u64 {
            *self.0.lock().unwrap()
        }

        fn advance(&self, ms: u64) {
            *self.0.lock().unwrap() += ms;
        }

        fn as_rtc_clock(&self) -> RtcClock {
            let now = self.0.clone();
            Arc::new(move || *now.lock().unwrap())
        }
    }

    fn bob_membership() -> JoinedMembership {
        JoinedMembership {
            room_id: ROOM_ID.to_string(),
            slot_id: SLOT_ID.to_string(),
            sender: "@bob:example.org".to_string(),
            origin: EventOrigin::encrypted(Some("device456".to_string())),
            sticky_key: "bob-device456-uuid".to_string(),
            member_id: "bob-device456-uuid".to_string(),
            membership_event_id: None,
            membership_ts: None,
            origin_server_ts: None,
            application: "m.call".into(),
            transports: Vec::new(),
            can_subscribe: Vec::new(),
        }
    }

    fn bob_key(key: Vec<u8>, index: u8) -> ReceivedEncryptionKey {
        ReceivedEncryptionKey {
            origin: KeyOrigin::Encrypted {
                sender_user_id: "@bob:example.org".to_string(),
                sender_device_id: Some("device456".to_string()),
                sender_is_confirmed: true,
            },
            room_id: ROOM_ID.to_string(),
            member_id: "bob-device456-uuid".to_string(),
            key_b64: general_purpose::STANDARD.encode(key),
            key_index: index,
        }
    }

    fn create_mock_get_memberships(
        participants: Vec<JoinedMembership>,
    ) -> impl Fn() -> Vec<JoinedMembership> + Send + Sync + 'static {
        move || participants.clone()
    }

    /// Who a rollout actually addressed, in order.
    fn to_device_recipients(sender: &MockBackend) -> Vec<(String, String)> {
        sender
            .to_device_messages
            .lock()
            .unwrap()
            .iter()
            .map(|(user_id, device_id, _, _)| (user_id.clone(), device_id.clone()))
            .collect()
    }

    /// Records what the media layer would have been told.
    #[derive(Default)]
    struct RecordingHandler {
        signals: Mutex<Vec<KeyMaterialSignal>>,
        discarded: Mutex<Vec<DiscardedKey>>,
        pending: Mutex<Option<(u8, Vec<u8>)>>,
    }

    impl RecordingHandler {
        fn identities(&self) -> Vec<String> {
            self.signals
                .lock()
                .unwrap()
                .iter()
                .map(|signal| signal.rtc_backend_identity.clone())
                .collect()
        }
    }

    #[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
    #[cfg_attr(not(target_arch = "wasm32"), async_trait)]
    impl EncryptionKeySignalHandler for RecordingHandler {
        async fn on_new_key_material(&self, signal: KeyMaterialSignal) {
            self.signals.lock().unwrap().push(signal);
        }

        async fn on_key_discarded(&self, discarded: DiscardedKey) {
            self.discarded.lock().unwrap().push(discarded);
        }

        fn outbound_activation_pending(&self, _identity: &str, index: u8, key: &[u8]) -> bool {
            self.pending.lock().unwrap().as_ref().is_some_and(|(pending_index, material)| {
                *pending_index == index && material.as_slice() == key
            })
        }
    }

    /// Media attaches after the slot is joined, so keys routinely arrive with no
    /// handler installed. They must not be lost: nothing re-signals them until a
    /// rotation, which needs a membership change.
    #[tokio::test]
    async fn keys_held_before_a_handler_exists_are_replayed_on_attach() {
        let mock_sender = Arc::new(NoopBackend);
        let get_memberships = create_mock_get_memberships(vec![bob_membership()]);
        let mut manager = EncryptionManager::new(
            mock_sender,
            USER_ID.to_string(),
            DEVICE_ID.to_string(),
            MEMBER_ID.to_string(),
            ROOM_ID.to_string(),
            SLOT_ID.to_string(),
            get_memberships,
        );

        // Join and receive a peer key with nothing listening.
        manager.join().await.expect("join should succeed");
        manager
            .receive_key(bob_key(vec![7u8; 32], 0))
            .await
            .expect("bob's key should be accepted");

        let handler = Arc::new(RecordingHandler::default());
        manager.set_signal_handler(handler.clone());
        assert!(
            handler.signals.lock().unwrap().is_empty(),
            "installing a handler must not itself signal"
        );

        manager.replay_keys_to_handler().await;

        let signals = handler.signals.lock().unwrap();
        assert_eq!(signals.len(), 2, "our own key and bob's");
        // Replayed keys are already in use by peers; delaying them again would
        // only stall media that could be decrypted now.
        assert!(signals.iter().all(|signal| signal.use_after_ms == 0));
        assert!(signals.iter().any(|signal| signal.key == vec![7u8; 32]));
    }

    /// Builds a manager whose memberships come from a list a test can mutate,
    /// so a departure or arrival can be staged between rollouts.
    fn manager_over(
        sender: Arc<MockBackend>,
        memberships: Arc<Mutex<Vec<JoinedMembership>>>,
    ) -> EncryptionManager<MockBackend> {
        EncryptionManager::new(
            sender,
            USER_ID.to_string(),
            DEVICE_ID.to_string(),
            MEMBER_ID.to_string(),
            ROOM_ID.to_string(),
            SLOT_ID.to_string(),
            move || memberships.lock().unwrap().clone(),
        )
    }

    #[tokio::test]
    async fn stale_delivery_attempt_cannot_remove_replacement_with_same_index() {
        let backend = Arc::new(MockBackend::new());
        let manager = manager_over(backend.clone(), Arc::new(Mutex::new(vec![bob_membership()])));
        manager.join().await.unwrap();
        let recipient = ParticipantDeviceInfo {
            user_id: "@bob:example.org".into(),
            device_id: "device456".into(),
            member_id: "bob-device456-uuid".into(),
            membership_ts: None,
        };
        let recipients = std::slice::from_ref(&recipient);
        let old = {
            let guard = manager.outbound_key.write().unwrap();
            manager.record_delivery_debt(false, guard.as_ref().unwrap().key_index, recipients, &[]);
            manager.delivery_debt("bob-device456-uuid").unwrap()
        };
        let replacement = {
            let mut guard = manager.outbound_key.write().unwrap();
            let mut new_key = guard.as_ref().unwrap().clone();
            new_key.key = vec![42; new_key.key.len()];
            *guard = Some(new_key);
            // Simulate a wrapped wire index, which must not recycle debt identity.
            manager.record_delivery_debt(true, guard.as_ref().unwrap().key_index, recipients, &[]);
            manager.delivery_debt("bob-device456-uuid").unwrap()
        };
        assert_ne!(old, replacement);
        assert_eq!(old.key_index, replacement.key_index);
        manager.drop_owed(&old);
        assert_eq!(manager.delivery_debt("bob-device456-uuid"), Some(replacement.clone()));
        assert_eq!(manager.retry_delivery_debt(&old).await.unwrap(), KeyRetry::Moot);
        assert!(to_device_recipients(&backend).is_empty());
        assert_eq!(manager.retry_delivery_debt(&replacement).await.unwrap(), KeyRetry::Delivered);
        assert_eq!(to_device_recipients(&backend), [(recipient.user_id.clone(), recipient.device_id.clone())]);
        assert!(manager.delivery_debt("bob-device456-uuid").is_none());
        assert!(manager.get_outbound_key().unwrap().shared_with.contains(&recipient));
    }

    #[tokio::test]
    async fn delivery_debt_never_resolves_to_a_replacement_join() {
        let backend = Arc::new(MockBackend::new());
        let original = JoinedMembership {
            membership_ts: Some(1000),
            ..bob_membership()
        };
        let memberships = Arc::new(Mutex::new(vec![original.clone()]));
        let manager = manager_over(backend.clone(), memberships.clone());
        manager.join().await.unwrap();
        let target = ParticipantDeviceInfo {
            user_id: original.sender.clone(),
            device_id: "device456".into(),
            member_id: original.member_id.clone(),
            membership_ts: original.membership_ts,
        };
        let owed = {
            let guard = manager.outbound_key.write().unwrap();
            manager.record_delivery_debt(
                false, guard.as_ref().unwrap().key_index, std::slice::from_ref(&target), &[],
            );
            manager.delivery_debt(&target.member_id).unwrap()
        };
        let rejoined = JoinedMembership { membership_ts: Some(2000), ..original.clone() };
        let other_device = JoinedMembership {
            origin: EventOrigin::encrypted(Some("different-device".into())),
            ..original.clone()
        };
        let other_user = JoinedMembership { sender: "@other:example.org".into(), ..original.clone() };
        let other_slot = JoinedMembership { slot_id: "different-slot".into(), ..original.clone() };
        for replacement in [
            vec![],
            vec![rejoined.clone()],
            vec![other_device.clone()],
            vec![other_user],
            vec![other_slot],
            vec![original.clone(), rejoined],
            vec![other_device, original.clone()],
        ] {
            *memberships.lock().unwrap() = replacement;
            assert_eq!(manager.retry_delivery_debt(&owed).await.unwrap(), KeyRetry::Undelivered);
            assert!(to_device_recipients(&backend).is_empty());
            assert_eq!(manager.delivery_debt(&target.member_id), Some(owed.clone()));
        }
        // An ordinary sticky refresh is the same join, even with a new event id.
        *memberships.lock().unwrap() = vec![JoinedMembership {
            membership_event_id: Some("$refreshed".into()),
            origin_server_ts: Some(3000),
            ..original
        }];
        assert_eq!(manager.retry_delivery_debt(&owed).await.unwrap(), KeyRetry::Delivered);
        assert_eq!(to_device_recipients(&backend), [(target.user_id.clone(), target.device_id.clone())]);
        assert!(manager.delivery_debt(&target.member_id).is_none());
    }

    /// One membership update per arriving peer, but only ever one signal per key
    /// index. Each signal reaches the transport as a key *import*, so a repeat is
    /// a duplicate `set_key` on the frame cryptor for material it already has.
    #[tokio::test]
    async fn a_key_index_is_signalled_once_however_often_memberships_update() {
        let memberships = Arc::new(Mutex::new(Vec::new()));
        let mut manager = manager_over(Arc::new(MockBackend::new()), memberships.clone());
        let handler = Arc::new(RecordingHandler::default());
        manager.set_signal_handler(handler.clone());
        manager.join().await.expect("join should succeed");

        // Alone in the call: a rollout with no recipients leaves `shared_with`
        // empty, which used to be read as "not signalled yet".
        for _ in 0..3 {
            manager
                .on_memberships_update()
                .await
                .expect("update should succeed");
        }

        let signals = handler.signals.lock().unwrap();
        assert_eq!(
            signals.len(),
            1,
            "the same key index was signalled {} times: {:?}",
            signals.len(),
            signals.iter().map(|s| s.key_index).collect::<Vec<_>>(),
        );
        assert_eq!(signals[0].key_index, 0);
        assert_eq!(
            signals[0].use_after_ms, 0,
            "the first key must be usable at once, or nothing can be encrypted"
        );
    }

    /// A departure rotates the key even with nobody left to send it to, and that
    /// eagerness is the point: the next arrival is then handed a key that is
    /// already the one we are stamping frames with, so they decrypt immediately.
    ///
    /// Deferring the rotation until someone arrives looks tidier — no index burnt
    /// on a key no peer will hold — but it is worse. The arrival would be handed a
    /// freshly rotated key while we keep stamping the *previous* one for the whole
    /// of its `delayBeforeUse`, and they hold nothing that decrypts it. Rotating
    /// on departure instead lets the grace-period reuse path below hand them the
    /// live key.
    ///
    /// Forward secrecy still holds: the departed member's key is retired at once,
    /// and the key the newcomer receives only ever encrypted media from after the
    /// call emptied.
    #[tokio::test]
    async fn a_departure_rotates_so_the_next_arrival_gets_the_live_key() {
        let sender = Arc::new(MockBackend::new());
        let memberships = Arc::new(Mutex::new(vec![bob_membership()]));
        let mut manager = manager_over(sender.clone(), memberships.clone());
        let handler = Arc::new(RecordingHandler::default());
        manager.set_signal_handler(handler.clone());
        let clock = TestClock::new();
        manager.set_clock(clock.as_rtc_clock());
        manager.join().await.expect("join should succeed");

        // Bob is in the call and gets index 0.
        manager
            .on_memberships_update()
            .await
            .expect("update should succeed");
        let index_bob_got = sender
            .last_to_device_message()
            .expect("bob should have been sent a key")
            .3
            .pointer("/media_key/index")
            .and_then(|index| index.as_u64())
            .expect("the key message carries an index");

        // The call settles, so bob's departure is answered at once rather than
        // being coalesced into the freshness of the key he was just given.
        clock.advance(EncryptionConfig::default().key_rotation_grace_period_ms);

        // Bob leaves. Nobody is left.
        memberships.lock().unwrap().clear();
        manager
            .on_memberships_update()
            .await
            .expect("update should succeed");
        assert_ne!(
            manager.get_outbound_key().map(|key| key.key_index),
            Some(index_bob_got as u8),
            "the departed member's key must be retired, or media sent after they \
             left stays readable to them"
        );

        // Carol arrives while that key is still fresh, so it is reused rather than
        // rotated again — and it is exactly what we encrypt with, so she decrypts
        // from her first frame.
        let carol = JoinedMembership {
            sender: "@carol:example.org".to_string(),
            origin: EventOrigin::encrypted(Some("CAROLDEV".to_string())),
            sticky_key: "carol-a".to_string(),
            member_id: "carol-a".to_string(),
            ..bob_membership()
        };
        *memberships.lock().unwrap() = vec![carol];
        manager
            .on_memberships_update()
            .await
            .expect("update should succeed");

        let sent_to_carol = sender
            .to_device_messages_for("@carol:example.org", "CAROLDEV")
            .into_iter()
            .filter_map(|(_, content)| {
                content
                    .pointer("/media_key/index")
                    .and_then(|index| index.as_u64())
            })
            .collect::<Vec<_>>();
        assert_eq!(
            sent_to_carol.len(),
            1,
            "carol should be handed exactly one key index, got {sent_to_carol:?}"
        );
        assert_ne!(
            sent_to_carol[0], index_bob_got,
            "the key bob held must not be reused for carol"
        );
        assert_eq!(
            manager.get_outbound_key().map(|key| key.key_index),
            Some(sent_to_carol[0] as u8),
            "we must encrypt with exactly the index carol was handed"
        );
    }

    /// `delayBeforeUse` is there to keep members who hold the outgoing key
    /// decrypting while its replacement propagates. When the call has sat empty,
    /// the key we hold went to nobody, so there is no continuity to protect — and
    /// waiting would leave the arriving member, who holds nothing at all, blind
    /// for the whole delay instead of just for their own key's delivery.
    #[tokio::test]
    async fn a_rotation_nobody_can_decrypt_is_usable_at_once() {
        let sender = Arc::new(MockBackend::new());
        let memberships = Arc::new(Mutex::new(vec![bob_membership()]));
        let mut manager = manager_over(sender.clone(), memberships.clone());
        let handler = Arc::new(RecordingHandler::default());
        manager.set_signal_handler(handler.clone());
        // No grace period, so an arrival rotates as soon as the key has settled.
        // "Settled" is still at least `delay_before_use_ms` — a key that has not
        // come into use cannot meaningfully be replaced — which is what every
        // `clock.advance` below waits out.
        manager.set_config(EncryptionConfig {
            key_rotation_grace_period_ms: 0,
            ..EncryptionConfig::default()
        });
        let settled = EncryptionConfig::default().delay_before_use_ms;
        let clock = TestClock::new();
        manager.set_clock(clock.as_rtc_clock());
        manager.join().await.expect("join should succeed");

        // Bob is here and holds our key.
        manager
            .on_memberships_update()
            .await
            .expect("update should succeed");
        clock.advance(settled);

        // Carol joins alongside him. Bob can decrypt what we are stamping, so the
        // rotation must wait for her to install it before we switch.
        let carol = JoinedMembership {
            sender: "@carol:example.org".to_string(),
            origin: EventOrigin::encrypted(Some("CAROLDEV".to_string())),
            sticky_key: "carol-a".to_string(),
            member_id: "carol-a".to_string(),
            ..bob_membership()
        };
        memberships.lock().unwrap().push(carol.clone());
        manager
            .on_memberships_update()
            .await
            .expect("update should succeed");
        assert_eq!(
            handler
                .signals
                .lock()
                .unwrap()
                .last()
                .expect("the rotation should have been signalled")
                .use_after_ms,
            EncryptionConfig::default().delay_before_use_ms,
            "bob still holds the outgoing key, so switching away from it early \
             would cut him off"
        );

        // Now everyone leaves and, later, carol arrives alone. The key we hold
        // reached nobody who is present, so there is nothing to keep alive.
        clock.advance(settled);
        memberships.lock().unwrap().clear();
        manager
            .on_memberships_update()
            .await
            .expect("update should succeed");
        clock.advance(settled);
        *memberships.lock().unwrap() = vec![carol];
        manager
            .on_memberships_update()
            .await
            .expect("update should succeed");

        assert_eq!(
            handler
                .signals
                .lock()
                .unwrap()
                .last()
                .expect("the rotation should have been signalled")
                .use_after_ms,
            0,
            "nobody present holds the outgoing key, so waiting only keeps the \
             arriving member undecryptable for longer"
        );
    }

    /// A departure keeps `delayBeforeUse`, and the leaver keeps reading for its
    /// duration.
    ///
    /// Switching the instant somebody hangs up would freeze every remaining
    /// member's video while the replacement key travels. Continuity wins: the
    /// members who hold the outgoing key keep decrypting, and the cost is that the
    /// member who just left decrypts along with them until the delay expires.
    ///
    /// This test exists to keep that from being "fixed" — the exposure is chosen,
    /// and the thing to protect is that it stays *bounded* by the delay.
    #[tokio::test]
    async fn a_departure_keeps_the_delay_that_protects_the_members_who_stay() {
        let carol = JoinedMembership {
            sender: "@carol:example.org".to_string(),
            origin: EventOrigin::encrypted(Some("CAROLDEV".to_string())),
            sticky_key: "carol-a".to_string(),
            member_id: "carol-a".to_string(),
            ..bob_membership()
        };
        let memberships = Arc::new(Mutex::new(vec![bob_membership(), carol]));
        let mut manager = manager_over(Arc::new(MockBackend::new()), memberships.clone());
        let handler = Arc::new(RecordingHandler::default());
        manager.set_signal_handler(handler.clone());
        let clock = TestClock::new();
        manager.set_clock(clock.as_rtc_clock());
        manager.join().await.expect("join should succeed");

        // Bob and carol both hold our key.
        manager
            .on_memberships_update()
            .await
            .expect("update should succeed");

        // The call settles, so the departure below rotates rather than being
        // coalesced into the freshness of the key it would replace.
        clock.advance(EncryptionConfig::default().key_rotation_grace_period_ms);

        // Carol leaves. Bob is still here and still holds the outgoing key.
        memberships
            .lock()
            .unwrap()
            .retain(|membership| membership.member_id != "carol-a");
        manager
            .on_memberships_update()
            .await
            .expect("update should succeed");

        assert_eq!(
            handler
                .signals
                .lock()
                .unwrap()
                .last()
                .expect("the rotation should have been signalled")
                .use_after_ms,
            EncryptionConfig::default().delay_before_use_ms,
            "bob is still in the call and still holds the outgoing key; switching early to lock \
             carol out would stall his video while the replacement reaches him"
        );
    }

    /// A departure pulls the key's expiry in, and something has to collect it —
    /// nothing in the core can.
    ///
    /// Deferring is what keeps a burst of departures from costing a key each, but it
    /// only holds together if something comes back when the expiry arrives. If a
    /// quiet membership meant the rotation never happened, the members who left would
    /// keep the key they hold indefinitely — strictly worse than rotating per
    /// departure.
    #[tokio::test]
    async fn a_departure_pulls_the_expiry_in_and_it_is_collected_then() {
        let carol = JoinedMembership {
            sender: "@carol:example.org".to_string(),
            origin: EventOrigin::encrypted(Some("CAROLDEV".to_string())),
            sticky_key: "carol-a".to_string(),
            member_id: "carol-a".to_string(),
            ..bob_membership()
        };
        let memberships = Arc::new(Mutex::new(vec![bob_membership(), carol]));
        let mut manager = manager_over(Arc::new(MockBackend::new()), memberships.clone());
        let clock = TestClock::new();
        manager.set_clock(clock.as_rtc_clock());
        manager.join().await.expect("join should succeed");

        // Bob and carol hold our key.
        manager
            .on_memberships_update()
            .await
            .expect("update should succeed");
        let lifetime = EncryptionConfig::default().max_key_lifetime_ms;
        assert_eq!(
            manager.rotation_due_at_ms(),
            Some(clock.now() + lifetime),
            "with nobody gone, the key expires on its lifetime cap and nothing sooner"
        );

        // Bob leaves while the key is fresh, so the rotation is owed, not made.
        let held = manager.get_outbound_key().expect("we hold a key").key_index;
        memberships
            .lock()
            .unwrap()
            .retain(|membership| membership.member_id != "bob-device456-uuid");
        manager
            .on_memberships_update()
            .await
            .expect("update should succeed");
        assert_eq!(
            manager.get_outbound_key().map(|key| key.key_index),
            Some(held),
            "the key is fresh, so bob's departure is answered when it expires"
        );

        // Carol leaves too, inside the same window: still one rotation owed.
        clock.advance(100);
        memberships.lock().unwrap().clear();
        manager
            .on_memberships_update()
            .await
            .expect("update should succeed");
        assert_eq!(
            manager.get_outbound_key().map(|key| key.key_index),
            Some(held),
            "carol left inside the same window, so she is coalesced into the same rotation"
        );
        let due_at = manager
            .rotation_due_at_ms()
            .expect("a key always has an expiry");
        assert!(
            due_at < clock.now() + lifetime,
            "carol's departure must pull the expiry in from the lifetime cap"
        );

        // Collecting early changes nothing; collecting once due rotates.
        manager
            .flush_due_rotation()
            .await
            .expect("flush should succeed");
        assert_eq!(
            manager.get_outbound_key().map(|key| key.key_index),
            Some(held),
            "the expiry has not arrived yet"
        );

        clock.advance(due_at - clock.now() + 1);
        manager
            .flush_due_rotation()
            .await
            .expect("flush should succeed");
        assert_ne!(
            manager.get_outbound_key().map(|key| key.key_index),
            Some(held),
            "the owed rotation must happen once the expiry arrives, or carol keeps the key she \
             holds for as long as the joined memberships stay still"
        );
        assert_eq!(
            manager.rotation_due_at_ms(),
            Some(clock.now() + lifetime),
            "the replacement is nobody's dirty key, so it expires on the lifetime cap again"
        );
    }

    /// The pre-sticky Element Call dialect reuses `{user}:{device}` as the
    /// member id, so a device that leaves and rejoins comes back under the id we
    /// already served — as a fresh session holding no keys. Matched by member id
    /// alone, the return reads as "already holds the key": nothing is sent, and
    /// the far end can never decrypt us again. The membership timestamp is what
    /// tells the two joins apart.
    #[tokio::test]
    async fn a_rejoin_under_the_same_member_id_is_resent_the_current_key() {
        let sender = Arc::new(MockBackend::new());
        let memberships = Arc::new(Mutex::new(vec![JoinedMembership {
            membership_ts: Some(1_000),
            origin_server_ts: None,
            ..bob_membership()
        }]));
        let mut manager = manager_over(sender.clone(), memberships.clone());
        let clock = TestClock::new();
        manager.set_clock(clock.as_rtc_clock());
        manager.join().await.expect("join should succeed");
        manager
            .on_memberships_update()
            .await
            .expect("update should succeed");
        let first_index = manager
            .get_outbound_key()
            .map(|key| key.key_index)
            .expect("joining mints a key");
        assert_eq!(
            sender
                .to_device_messages_for("@bob:example.org", "device456")
                .len(),
            1,
        );

        // Bob's client dies without leaving; the departure is seen, but the key
        // is fresh, so the rotation it owes is deferred rather than performed.
        memberships.lock().unwrap().clear();
        manager
            .on_memberships_update()
            .await
            .expect("update should succeed");
        assert_eq!(
            manager.get_outbound_key().map(|k| k.key_index),
            Some(first_index)
        );

        // Bob rejoins: the same user, device and member id, but a new
        // `created_ts` — and a fresh session that holds nothing from before.
        clock.advance(2_000);
        *memberships.lock().unwrap() = vec![JoinedMembership {
            membership_ts: Some(3_000),
            origin_server_ts: None,
            ..bob_membership()
        }];
        manager
            .on_memberships_update()
            .await
            .expect("update should succeed");

        let sent_to_bob = sender.to_device_messages_for("@bob:example.org", "device456");
        assert_eq!(
            sent_to_bob.len(),
            2,
            "the rejoined session holds no keys and must be re-sent the current one"
        );
        assert_eq!(
            manager.get_outbound_key().map(|key| key.key_index),
            Some(first_index),
            "the key is still fresh, so the rejoin is served it rather than a rotation"
        );

        // The previous join held this key while outside the call, so the
        // rotation owed to its departure must still fall due — the rejoin must
        // not read as the leaver coming back and cancel it.
        clock.advance(EncryptionConfig::default().key_rotation_grace_period_ms);
        manager
            .flush_due_rotation()
            .await
            .expect("flush should succeed");
        assert_ne!(
            manager.get_outbound_key().map(|key| key.key_index),
            Some(first_index),
            "the rotation owed to the departure was cancelled by the rejoin"
        );
        assert_eq!(
            sender
                .to_device_messages_for("@bob:example.org", "device456")
                .len(),
            3,
            "the rotation must hand the rejoined session the replacement key"
        );
    }

    /// A leave and rejoin can both happen between two membership feeds, so no
    /// rollout ever sees the member absent — only the membership timestamp
    /// moves. That alone must read as a new participation.
    #[tokio::test]
    async fn a_rejoin_no_feed_ever_saw_leave_is_still_served() {
        let sender = Arc::new(MockBackend::new());
        let memberships = Arc::new(Mutex::new(vec![JoinedMembership {
            membership_ts: Some(1_000),
            origin_server_ts: None,
            ..bob_membership()
        }]));
        let manager = manager_over(sender.clone(), memberships.clone());
        manager.join().await.expect("join should succeed");
        manager
            .on_memberships_update()
            .await
            .expect("update should succeed");

        *memberships.lock().unwrap() = vec![JoinedMembership {
            membership_ts: Some(4_000),
            origin_server_ts: None,
            ..bob_membership()
        }];
        manager
            .on_memberships_update()
            .await
            .expect("update should succeed");

        assert_eq!(
            sender
                .to_device_messages_for("@bob:example.org", "device456")
                .len(),
            2,
            "a moved membership timestamp is a rejoin and must be served the key"
        );
    }

    /// A membership re-sent to extend its lifetime keeps its `created_ts`: the
    /// same participation restated must not read as a rejoin and cost a send or
    /// an owed rotation.
    #[tokio::test]
    async fn a_membership_refresh_with_an_unchanged_timestamp_distributes_nothing() {
        let sender = Arc::new(MockBackend::new());
        let memberships = Arc::new(Mutex::new(vec![JoinedMembership {
            membership_ts: Some(1_000),
            origin_server_ts: None,
            ..bob_membership()
        }]));
        let manager = manager_over(sender.clone(), memberships.clone());
        manager.join().await.expect("join should succeed");
        for _ in 0..3 {
            manager
                .on_memberships_update()
                .await
                .expect("update should succeed");
        }

        assert_eq!(
            sender
                .to_device_messages_for("@bob:example.org", "device456")
                .len(),
            1,
            "an unchanged participation must be sent the key exactly once"
        );
    }

    /// An arrival that rotates hands the newcomer the new key only.
    ///
    /// The rotation is delayed to keep the members already present decrypting, so
    /// for the length of the delay we go on encrypting with a key minted before
    /// the newcomer joined. Handing it over would let them read back over
    /// everything it encrypted, which is what rotating on their arrival prevents;
    /// they decrypt us once the new key comes into use.
    #[tokio::test]
    async fn an_arrival_is_not_handed_the_key_it_arrived_after() {
        let sender = Arc::new(MockBackend::new());
        let memberships = Arc::new(Mutex::new(vec![bob_membership()]));
        let mut manager = manager_over(sender.clone(), memberships.clone());
        let handler = Arc::new(RecordingHandler::default());
        manager.set_signal_handler(handler.clone());
        // No grace period, so an arrival rotates as soon as the key has settled —
        // which is still at least `delay_before_use_ms`, waited out below.
        manager.set_config(EncryptionConfig {
            key_rotation_grace_period_ms: 0,
            ..EncryptionConfig::default()
        });
        let clock = TestClock::new();
        manager.set_clock(clock.as_rtc_clock());
        manager.join().await.expect("join should succeed");
        manager
            .on_memberships_update()
            .await
            .expect("update should succeed");
        clock.advance(EncryptionConfig::default().delay_before_use_ms);

        let index_in_use = manager.get_outbound_key().expect("we hold a key").key_index;

        // Carol arrives. Bob holds the key in use, so the rotation waits — and
        // what we keep stamping frames with meanwhile is that same key.
        memberships.lock().unwrap().push(JoinedMembership {
            sender: "@carol:example.org".to_string(),
            origin: EventOrigin::encrypted(Some("CAROLDEV".to_string())),
            sticky_key: "carol-a".to_string(),
            member_id: "carol-a".to_string(),
            ..bob_membership()
        });
        manager
            .on_memberships_update()
            .await
            .expect("update should succeed");

        let signal = handler
            .signals
            .lock()
            .unwrap()
            .last()
            .cloned()
            .expect("the rotation should have been signalled");
        assert_eq!(
            signal.use_after_ms,
            EncryptionConfig::default().delay_before_use_ms,
            "bob holds the outgoing key, so the switch waits for him"
        );

        let sent_to_carol: Vec<u64> = sender
            .to_device_messages_for("@carol:example.org", "CAROLDEV")
            .into_iter()
            .filter_map(|(_, content)| {
                content
                    .pointer("/media_key/index")
                    .and_then(|index| index.as_u64())
            })
            .collect();
        assert!(
            sent_to_carol.contains(&(signal.key_index as u64)),
            "carol needs the key we are rotating to, got {sent_to_carol:?}"
        );
        assert!(
            !sent_to_carol.contains(&(index_in_use as u64)),
            "carol must not get the key minted before she joined, which we go on \
             encrypting with for {}ms; got {sent_to_carol:?}",
            signal.use_after_ms,
        );
    }

    async fn joined_distribution_manager(
        sender: Arc<MockBackend>,
        memberships: Arc<Mutex<Vec<JoinedMembership>>>,
    ) -> (EncryptionManager<MockBackend>, Arc<OwnMembershipMachine<MockBackend>>) {
        let machine = Arc::new(OwnMembershipMachine::new(
            sender.clone(), ROOM_ID.to_owned(), SLOT_ID.to_owned(),
            MEMBER_ID.to_owned(), "m.call".to_owned(),
            crate::own_membership::MembershipTimings::default(),
        ));
        machine.join(crate::transport::MemberTransports::default(), None).await.unwrap();
        let mut manager = manager_over(sender, memberships);
        manager.set_membership_machine(machine.clone());
        manager.join().await.unwrap();
        (manager, machine)
    }

    #[tokio::test(start_paused = true)]
    async fn cancelled_distribution_waiter_preserves_queued_followup() {
        use crate::host::backend::{MockMethod, MockOutcome};
        use std::time::Duration;

        let sender = Arc::new(MockBackend::new());
        let memberships = Arc::new(Mutex::new(vec![bob_membership()]));
        let (manager, _machine) = joined_distribution_manager(sender.clone(), memberships.clone()).await;
        sender.script(MockMethod::ToDevice, [MockOutcome::After(
            Duration::from_secs(3), Box::new(MockOutcome::Ok),
        )]);
        let waiter = tokio::spawn({
            let manager = manager.clone();
            async move { manager.ensure_key_distribution().await }
        });
        for _ in 0..32 {
            if !sender.calls_to(MockMethod::ToDevice).is_empty() { break; }
            tokio::task::yield_now().await;
        }
        assert_eq!(sender.calls_to(MockMethod::ToDevice).len(), 1);
        memberships.lock().unwrap().push(JoinedMembership {
            sender: "@carol:example.org".to_owned(),
            origin: EventOrigin::encrypted(Some("CAROLDEV".to_owned())),
            sticky_key: "carol-a".to_owned(), member_id: "carol-a".to_owned(),
            ..bob_membership()
        });
        manager.ensure_key_distribution().await.unwrap();
        waiter.abort();
        assert!(waiter.await.unwrap_err().is_cancelled());
        tokio::time::sleep(Duration::from_secs(4)).await;
        let shared = manager.get_outbound_key().unwrap().shared_with;
        assert!(shared.iter().any(|p| p.member_id == "carol-a"));
        assert!(shared.iter().any(|p| p.member_id == bob_membership().member_id));
        assert_eq!(sender.calls_to(MockMethod::ToDevice).len(), 2);
        assert!(!manager.key_distribution.lock().unwrap().running);
    }

    #[tokio::test(start_paused = true)]
    async fn cancelled_inline_distribution_transfers_admission_to_surviving_waiter() {
        use crate::host::backend::{MockMethod, MockOutcome};
        use std::time::Duration;

        let sender = Arc::new(MockBackend::new());
        let memberships = Arc::new(Mutex::new(vec![bob_membership()]));
        let manager = manager_over(sender.clone(), memberships.clone());
        manager.join().await.unwrap();
        sender.script(MockMethod::ToDevice, [MockOutcome::Hang, MockOutcome::Ok]);
        let owner = tokio::spawn({
            let manager = manager.clone();
            async move { manager.ensure_key_distribution().await }
        });
        for _ in 0..32 {
            if !sender.calls_to(MockMethod::ToDevice).is_empty() { break; }
            tokio::task::yield_now().await;
        }
        assert_eq!(sender.calls_to(MockMethod::ToDevice).len(), 1);
        let successor = tokio::spawn({
            let manager = manager.clone();
            async move { manager.ensure_key_distribution().await }
        });
        tokio::task::yield_now().await;
        assert!(!successor.is_finished());
        owner.abort();
        assert!(owner.await.unwrap_err().is_cancelled());
        tokio::time::timeout(Duration::from_secs(1), successor).await.unwrap().unwrap().unwrap();
        assert_eq!(sender.calls_to(MockMethod::ToDevice).len(), 2);
        assert!(manager.get_outbound_key().unwrap().shared_with.iter().any(|p| p.member_id == bob_membership().member_id));
        assert!(!manager.key_distribution.lock().unwrap().running);
    }

    #[tokio::test(start_paused = true)]
    async fn leaving_cancels_distribution_even_after_waiter_is_gone() {
        use crate::host::backend::{MockMethod, MockOutcome};
        use std::time::Duration;

        let sender = Arc::new(MockBackend::new());
        let memberships = Arc::new(Mutex::new(vec![bob_membership()]));
        let (manager, machine) = joined_distribution_manager(sender.clone(), memberships).await;
        sender.script(MockMethod::ToDevice, [MockOutcome::Hang]);
        let waiter = tokio::spawn({
            let manager = manager.clone();
            async move { manager.ensure_key_distribution().await }
        });
        for _ in 0..32 {
            if !sender.calls_to(MockMethod::ToDevice).is_empty() { break; }
            tokio::task::yield_now().await;
        }
        assert_eq!(sender.calls_to(MockMethod::ToDevice).len(), 1);
        manager.ensure_key_distribution().await.unwrap();
        waiter.abort();
        assert!(waiter.await.unwrap_err().is_cancelled());
        machine.leave(None).await.unwrap();
        manager.leave();
        // The outbox cancels the generation, but intentionally drains an
        // already-submitted host request until its bounded attempt completes.
        // Leaving must immediately discard key material and must never send
        // the queued follow-up. A one-second owner-release deadline would
        // contradict the outbox's 30-second attempt contract.
        tokio::time::sleep(Duration::from_secs(1)).await;
        assert_eq!(sender.calls_to(MockMethod::ToDevice).len(), 1);
        assert!(manager.get_outbound_key().is_none());
        assert!(manager.key_distribution.lock().unwrap().running,
            "the in-flight host attempt is still draining");

        // Observe the actual owner release (not an arbitrary sleep or a
        // weakened assertion), with an upper bound tied to the send policy.
        let mut released = manager.key_distribution.lock().unwrap().released.subscribe();
        tokio::time::timeout(crate::outbox::ATTEMPT_TIMEOUT, released.changed())
            .await.expect("distribution must release at the attempt deadline")
            .expect("distribution owner must signal release");
        {
            let state = manager.key_distribution.lock().unwrap();
            assert!(!state.running);
            assert!(!state.pending);
        }
        tokio::time::sleep(crate::outbox::ATTEMPT_TIMEOUT).await;
        assert_eq!(sender.calls_to(MockMethod::ToDevice).len(), 1,
            "the cancelled follow-up must never submit another key");
        assert!(manager.get_outbound_key().is_none());
    }

    #[tokio::test(start_paused = true)]
    async fn slow_delivery_does_not_consume_the_media_switch_window() {
        use crate::host::backend::{MockMethod, MockOutcome};
        use std::time::Duration;

        let sender = Arc::new(MockBackend::new());
        let memberships = Arc::new(Mutex::new(vec![bob_membership()]));
        let mut manager = manager_over(sender.clone(), memberships.clone());
        let handler = Arc::new(RecordingHandler::default());
        manager.set_signal_handler(handler.clone());
        let config = EncryptionConfig {
            key_rotation_grace_period_ms: 0,
            delay_before_use_ms: 1_000,
            ..EncryptionConfig::default()
        };
        manager.set_config(config);
        let origin = tokio::time::Instant::now();
        manager.set_clock(Arc::new(move || {
            1_700_000_000_000 + origin.elapsed().as_millis() as u64
        }));
        manager.join().await.unwrap();
        manager.on_memberships_update().await.unwrap();
        let old_index = manager.get_outbound_key().unwrap().key_index;
        tokio::time::advance(Duration::from_secs(2)).await;
        memberships.lock().unwrap().push(JoinedMembership {
            sender: "@carol:example.org".to_string(),
            origin: EventOrigin::encrypted(Some("CAROLDEV".to_string())),
            sticky_key: "carol-a".to_string(),
            member_id: "carol-a".to_string(),
            ..bob_membership()
        });
        sender.script(MockMethod::ToDevice, [MockOutcome::After(
            Duration::from_secs(3), Box::new(MockOutcome::Ok),
        )]);
        manager.on_memberships_update().await.unwrap();
        let new_index = manager.get_outbound_key().unwrap().key_index;
        assert_ne!(new_index, old_index);
        assert_eq!(manager.key_in_use(manager.timestamp_ms()).unwrap().0, old_index);
        tokio::time::advance(Duration::from_millis(999)).await;
        assert_eq!(manager.key_in_use(manager.timestamp_ms()).unwrap().0, old_index);
        manager.replay_keys_to_handler().await;
        assert_eq!(handler.signals.lock().unwrap().last().unwrap().use_after_ms, 1);
        // Distribution was slow enough that the creation-time grace period has
        // already elapsed. A departure must still not replace the pending key
        // before the actual post-delivery propagation deadline.
        memberships.lock().unwrap().retain(|member| member.member_id != "carol-a");
        manager.on_memberships_update().await.unwrap();
        assert_eq!(manager.get_outbound_key().unwrap().key_index, new_index);
        tokio::time::advance(Duration::from_millis(1)).await;
        assert_eq!(manager.key_in_use(manager.timestamp_ms()).unwrap().0, new_index);
        manager.replay_keys_to_handler().await;
        assert_eq!(handler.signals.lock().unwrap().last().unwrap().use_after_ms, 0);
        let committed = manager.get_outbound_key().unwrap();
        *handler.pending.lock().unwrap() = Some((committed.key_index, committed.key));
        manager.flush_due_rotation().await.unwrap();
        assert_eq!(manager.get_outbound_key().unwrap().key_index, new_index);
        assert_eq!(manager.rotation_due_at_ms(), Some(manager.timestamp_ms() + 100));
        *handler.pending.lock().unwrap() = None;
        tokio::time::advance(Duration::from_millis(100)).await;
        manager.flush_due_rotation().await.unwrap();
        assert_ne!(manager.get_outbound_key().unwrap().key_index, new_index);
    }

    /// MSC4143 sends the same key to a member whenever their membership event
    /// changes, so a receiver sees repeats. Storing each one grows the key list
    /// without bound and re-imports identical material; `OutdatedKeyFilter`
    /// cannot catch it, because with no sender timestamp on the wire a
    /// re-delivery always looks newer than what we hold.
    #[tokio::test]
    async fn an_identical_key_redelivery_is_not_reimported() {
        let get_memberships = create_mock_get_memberships(vec![bob_membership()]);
        let mut manager = EncryptionManager::new(
            Arc::new(NoopBackend),
            USER_ID.to_string(),
            DEVICE_ID.to_string(),
            MEMBER_ID.to_string(),
            ROOM_ID.to_string(),
            SLOT_ID.to_string(),
            get_memberships,
        );
        let handler = Arc::new(RecordingHandler::default());
        manager.set_signal_handler(handler.clone());

        manager
            .receive_key(bob_key(vec![7u8; 32], 0))
            .await
            .expect("bob's key should be accepted");
        manager
            .receive_key(bob_key(vec![7u8; 32], 0))
            .await
            .expect("the redelivery should be accepted too");

        assert_eq!(
            handler.signals.lock().unwrap().len(),
            1,
            "an identical key was imported twice"
        );
        assert_eq!(
            manager.get_inbound_keys("bob-device456-uuid").len(),
            1,
            "an identical key was stored twice"
        );
    }

    /// A *different* key at the same index is a real rotation and must still get
    /// through — the dedupe keys on the material, not the index alone.
    #[tokio::test]
    async fn a_different_key_at_the_same_index_is_still_imported() {
        let get_memberships = create_mock_get_memberships(vec![bob_membership()]);
        let mut manager = EncryptionManager::new(
            Arc::new(NoopBackend),
            USER_ID.to_string(),
            DEVICE_ID.to_string(),
            MEMBER_ID.to_string(),
            ROOM_ID.to_string(),
            SLOT_ID.to_string(),
            get_memberships,
        );
        let handler = Arc::new(RecordingHandler::default());
        manager.set_signal_handler(handler.clone());

        manager
            .receive_key(bob_key(vec![7u8; 32], 0))
            .await
            .expect("bob's key should be accepted");
        manager
            .receive_key(bob_key(vec![9u8; 32], 0))
            .await
            .expect("bob's replacement key should be accepted");

        assert_eq!(handler.signals.lock().unwrap().len(), 2);
    }

    /// The upkeep manager is cloned before the media layer attaches. A
    /// timer-triggered rollover must signal the replacement under the live
    /// backend identity, not silently update only the core key ring.
    #[tokio::test]
    async fn upkeep_clone_observes_late_media_handler_and_mapper() {
        let memberships = Arc::new(Mutex::new(vec![bob_membership()]));
        let mut manager = manager_over(Arc::new(MockBackend::new()), memberships);
        let clock = TestClock::new();
        manager.set_clock(clock.as_rtc_clock());
        manager.join().await.unwrap();
        let upkeep = manager.clone();
        manager.on_memberships_update().await.unwrap();
        let held = manager.get_outbound_key().unwrap().key_index;
        let handler = Arc::new(RecordingHandler::default());
        manager.set_signal_handler(handler.clone());
        manager.set_identity_mapper(Arc::new(|subject: &IdentitySubject<'_>| {
            Some(format!("mapped:{}", subject.member_id))
        }));
        manager.replay_keys_to_handler().await;
        let due = manager.rotation_due_at_ms().unwrap();
        clock.advance(due.saturating_sub(clock.now()) + 1);
        upkeep.flush_due_rotation().await.unwrap();
        let replacement = manager.get_outbound_key().unwrap();
        assert_ne!(replacement.key_index, held);
        assert!(handler.signals.lock().unwrap().iter().any(|signal| {
            signal.key_index == replacement.key_index
                && signal.key == replacement.key
                && signal.rtc_backend_identity == format!("mapped:{MEMBER_ID}")
        }));
    }

    #[tokio::test]
    async fn existing_clone_observes_replaced_media_handler() {
        let mut manager = manager_over(
            Arc::new(MockBackend::new()),
            Arc::new(Mutex::new(vec![bob_membership()])),
        );
        manager.join().await.unwrap();
        let clone = manager.clone();
        let old = Arc::new(RecordingHandler::default());
        manager.set_signal_handler(old.clone());
        let current = Arc::new(RecordingHandler::default());
        manager.set_signal_handler(current.clone());
        manager.set_identity_mapper(Arc::new(|subject: &IdentitySubject<'_>| {
            Some(format!("mapped:{}", subject.member_id))
        }));
        clone.receive_key(bob_key(vec![7u8; 32], 0)).await.unwrap();
        assert!(old.signals.lock().unwrap().is_empty());
        assert_eq!(current.identities(), vec!["mapped:bob-device456-uuid".to_string()]);
    }

    /// The identity is the whole point of the replay: importing a key under one
    /// the transport never uses is indistinguishable from not importing it.
    #[tokio::test]
    async fn replayed_keys_use_the_installed_identity_mapper() {
        let mock_sender = Arc::new(NoopBackend);
        let get_memberships = create_mock_get_memberships(vec![bob_membership()]);
        let mut manager = EncryptionManager::new(
            mock_sender,
            USER_ID.to_string(),
            DEVICE_ID.to_string(),
            MEMBER_ID.to_string(),
            ROOM_ID.to_string(),
            SLOT_ID.to_string(),
            get_memberships,
        );
        manager.join().await.expect("join should succeed");
        manager
            .receive_key(bob_key(vec![7u8; 32], 0))
            .await
            .expect("bob's key should be accepted");

        let handler = Arc::new(RecordingHandler::default());
        manager.set_signal_handler(handler.clone());
        manager.set_identity_mapper(Arc::new(|subject: &IdentitySubject<'_>| {
            Some(format!(
                "mapped:{}/{}/{}",
                subject.user_id,
                subject.device_id.unwrap_or_default(),
                subject.member_id
            ))
        }));

        manager.replay_keys_to_handler().await;

        let identities = handler.identities();
        assert!(
            identities.contains(&format!("mapped:{USER_ID}/{DEVICE_ID}/{MEMBER_ID}")),
            "our own key must replay under the mapped identity, got {identities:?}"
        );
        assert!(
            identities
                .contains(&"mapped:@bob:example.org/device456/bob-device456-uuid".to_string()),
            "bob's key must replay under his mapped identity, not his member_id, got {identities:?}"
        );
    }

    #[tokio::test]
    async fn replaying_without_a_handler_is_a_no_op() {
        let mock_sender = Arc::new(NoopBackend);
        let get_memberships = create_mock_get_memberships(vec![]);
        let manager = EncryptionManager::new(
            mock_sender,
            USER_ID.to_string(),
            DEVICE_ID.to_string(),
            MEMBER_ID.to_string(),
            ROOM_ID.to_string(),
            SLOT_ID.to_string(),
            get_memberships,
        );
        manager.join().await.expect("join should succeed");

        // Must not panic on the missing handler.
        manager.replay_keys_to_handler().await;
    }

    #[tokio::test]
    async fn test_manager_join_creates_first_key() {
        let mock_sender = Arc::new(MockBackend::new());
        let get_memberships = create_mock_get_memberships(vec![]);

        let manager = EncryptionManager::new(
            mock_sender.clone(),
            USER_ID.to_string(),
            DEVICE_ID.to_string(),
            MEMBER_ID.to_string(),
            ROOM_ID.to_string(),
            SLOT_ID.to_string(),
            get_memberships,
        );

        manager.join().await.expect("join should succeed");

        // Check that first key was created
        let outbound_key = manager.get_outbound_key();
        assert!(outbound_key.is_some());

        let key = outbound_key.unwrap();
        assert_eq!(key.key_index, 0);
        assert_eq!(key.key.len(), 32);
        assert!(key.creation_ts > 0);
    }

    #[tokio::test]
    async fn test_key_index_increments() {
        let mock_sender = Arc::new(NoopBackend);
        let get_memberships = create_mock_get_memberships(vec![]);

        let manager = EncryptionManager::new(
            mock_sender.clone(),
            USER_ID.to_string(),
            DEVICE_ID.to_string(),
            MEMBER_ID.to_string(),
            ROOM_ID.to_string(),
            SLOT_ID.to_string(),
            get_memberships,
        );

        manager.join().await.expect("join should succeed");

        // First key should have index 0
        assert_eq!(manager.get_outbound_key().unwrap().key_index, 0);

        // Create a new key (simulating rotation)
        let new_key = manager.create_new_outbound_key();
        assert_eq!(new_key.key_index, 1);

        let another_key = manager.create_new_outbound_key();
        assert_eq!(another_key.key_index, 2);
    }

    #[tokio::test]
    async fn test_key_index_wraps() {
        let mock_sender = Arc::new(NoopBackend);
        let get_memberships = create_mock_get_memberships(vec![]);

        let manager = EncryptionManager::new(
            mock_sender.clone(),
            USER_ID.to_string(),
            DEVICE_ID.to_string(),
            MEMBER_ID.to_string(),
            ROOM_ID.to_string(),
            SLOT_ID.to_string(),
            get_memberships,
        );

        // Create 256 keys, next should wrap to 0
        for _ in 0..256 {
            manager.create_new_outbound_key();
        }

        let key = manager.create_new_outbound_key();
        assert_eq!(key.key_index, 0);
    }

    #[tokio::test]
    async fn test_receive_key_stores_inbound() {
        let mock_sender = Arc::new(NoopBackend);
        let get_memberships = create_mock_get_memberships(vec![bob_membership()]);

        let manager = EncryptionManager::new(
            mock_sender.clone(),
            USER_ID.to_string(),
            DEVICE_ID.to_string(),
            MEMBER_ID.to_string(),
            ROOM_ID.to_string(),
            SLOT_ID.to_string(),
            get_memberships,
        );

        manager
            .receive_key(bob_key(vec![1u8; 32], 0))
            .await
            .expect("receive_key should succeed");

        // Check that key was stored
        let keys = manager.get_inbound_keys("bob-device456-uuid");
        assert_eq!(keys.len(), 1);
        assert_eq!(keys[0].key_index, 0);
        assert_eq!(keys[0].key, vec![1u8; 32]);
    }

    /// A peer rekeying an index we already hold must win: their newer key is what
    /// they are encrypting with, so keeping the first one guarantees their media
    /// never decrypts.
    ///
    /// This used to assert the opposite — that the second key was "outdated" and
    /// dropped — which only held because both receives landed in the same
    /// millisecond. MSC4143 puts no creation timestamp on the wire, so
    /// `OutdatedKeyFilter` stamps receive time and cannot tell a stale key from a
    /// fresh one; treating an equal timestamp as stale threw away the good key.
    #[tokio::test]
    async fn a_rekey_at_the_same_index_replaces_the_key_we_held() {
        let mock_sender = Arc::new(NoopBackend);
        let get_memberships = create_mock_get_memberships(vec![bob_membership()]);

        let manager = EncryptionManager::new(
            mock_sender.clone(),
            USER_ID.to_string(),
            DEVICE_ID.to_string(),
            MEMBER_ID.to_string(),
            ROOM_ID.to_string(),
            SLOT_ID.to_string(),
            get_memberships,
        );

        manager
            .receive_key(bob_key(vec![1u8; 32], 1))
            .await
            .expect("receive_key should succeed");
        manager
            .receive_key(bob_key(vec![2u8; 32], 1))
            .await
            .expect("receive_key should succeed");

        // One entry per index, holding the newest material — so a later replay
        // cannot reinstall the superseded key.
        let keys = manager.get_inbound_keys("bob-device456-uuid");
        assert_eq!(keys.len(), 1, "an index should not accumulate keys");
        assert_eq!(keys[0].key_index, 1);
        assert_eq!(keys[0].key, vec![2u8; 32], "the rekey should have won");
    }

    /// Helper: a manager with Bob joined, using the default (strict) config.
    fn manager_with_bob() -> EncryptionManager<NoopBackend> {
        EncryptionManager::new(
            Arc::new(NoopBackend),
            USER_ID.to_string(),
            DEVICE_ID.to_string(),
            MEMBER_ID.to_string(),
            ROOM_ID.to_string(),
            SLOT_ID.to_string(),
            create_mock_get_memberships(vec![bob_membership()]),
        )
    }

    async fn assert_discarded(
        manager: &EncryptionManager<NoopBackend>,
        key: ReceivedEncryptionKey,
    ) {
        let member_id = key.member_id.clone();
        manager
            .receive_key(key)
            .await
            .expect("a discarded key is not an error");
        assert!(
            manager.get_inbound_keys(&member_id).is_empty(),
            "key should have been discarded"
        );
    }

    /// A discarded key must reach the host with its reason attached. Downstream,
    /// a refused key and one that never arrived both surface as `MISSING_KEY`, so
    /// without this the host cannot tell a trust problem from a delivery one —
    /// the reason was warn-logged inside the core and went nowhere.
    #[tokio::test]
    async fn a_refused_key_is_reported_with_its_reason() {
        let get_memberships = create_mock_get_memberships(vec![bob_membership()]);
        let mut manager = EncryptionManager::new(
            Arc::new(NoopBackend),
            USER_ID.to_string(),
            DEVICE_ID.to_string(),
            MEMBER_ID.to_string(),
            ROOM_ID.to_string(),
            SLOT_ID.to_string(),
            get_memberships,
        );
        let handler = Arc::new(RecordingHandler::default());
        manager.set_signal_handler(handler.clone());

        // MSC4153: bob's device is not cross-signed, so his key is refused.
        let mut key = bob_key(vec![1u8; 32], 3);
        key.origin = KeyOrigin::Encrypted {
            sender_user_id: "@bob:example.org".to_string(),
            sender_device_id: Some("device456".to_string()),
            sender_is_confirmed: false,
        };
        manager
            .receive_key(key)
            .await
            .expect("a discarded key is not an error");

        let discarded = handler.discarded.lock().unwrap();
        assert_eq!(
            *discarded,
            vec![DiscardedKey {
                member_id: "bob-device456-uuid".to_string(),
                key_index: Some(3),
                sender_user_id: Some("@bob:example.org".to_string()),
                sender_device_id: Some("device456".to_string()),
                reason: KeyRejection::SenderNotConfirmed,
            }],
        );
        assert!(
            handler.signals.lock().unwrap().is_empty(),
            "a refused key must not also be signalled as usable"
        );
    }

    /// The other rejection stage — the key passed the origin checks but does not
    /// match the member event it claims — must report too, and say who sent it.
    #[tokio::test]
    async fn a_key_from_the_wrong_device_is_reported_with_its_reason() {
        let get_memberships = create_mock_get_memberships(vec![bob_membership()]);
        let mut manager = EncryptionManager::new(
            Arc::new(NoopBackend),
            USER_ID.to_string(),
            DEVICE_ID.to_string(),
            MEMBER_ID.to_string(),
            ROOM_ID.to_string(),
            SLOT_ID.to_string(),
            get_memberships,
        );
        let handler = Arc::new(RecordingHandler::default());
        manager.set_signal_handler(handler.clone());

        let mut key = bob_key(vec![1u8; 32], 0);
        key.origin = KeyOrigin::Encrypted {
            sender_user_id: "@bob:example.org".to_string(),
            sender_device_id: Some("SOMEOTHERDEV".to_string()),
            sender_is_confirmed: true,
        };
        manager
            .receive_key(key)
            .await
            .expect("a discarded key is not an error");

        let discarded = handler.discarded.lock().unwrap();
        assert_eq!(
            discarded.len(),
            1,
            "the rejection should have been reported"
        );
        assert_eq!(
            discarded[0].sender_device_id.as_deref(),
            Some("SOMEOTHERDEV"),
            "the host needs the device that actually sent it, to act on this",
        );
        assert!(matches!(
            discarded[0].reason,
            KeyRejection::DeviceMismatch { .. }
        ));
    }

    /// MSC4143: "clients SHOULD discard any m.rtc.encryption_key events that
    /// were sent in cleartext" — an unencrypted message has no authenticated
    /// sender, so nothing can be checked against the member event.
    #[tokio::test]
    async fn cleartext_key_is_discarded() {
        let manager = manager_with_bob();
        let mut key = bob_key(vec![1u8; 32], 0);
        key.origin = KeyOrigin::Cleartext;
        assert_discarded(&manager, key).await;
    }

    /// MSC4143 MUST: the to-device sender has to match the sender of the member
    /// event it claims. Otherwise any room member could publish keys as anyone.
    #[tokio::test]
    async fn key_from_wrong_user_is_discarded() {
        let manager = manager_with_bob();
        let mut key = bob_key(vec![1u8; 32], 0);
        key.origin = KeyOrigin::Encrypted {
            sender_user_id: "@mallory:example.org".to_string(),
            sender_device_id: Some("device456".to_string()),
            sender_is_confirmed: true,
        };
        assert_discarded(&manager, key).await;
    }

    /// MSC4143 MUST: same for the device — another of Bob's own devices cannot
    /// speak for the device that published the membership.
    #[tokio::test]
    async fn key_from_wrong_device_is_discarded() {
        let manager = manager_with_bob();
        let mut key = bob_key(vec![1u8; 32], 0);
        key.origin = KeyOrigin::Encrypted {
            sender_user_id: "@bob:example.org".to_string(),
            sender_device_id: Some("someOtherDevice".to_string()),
            sender_is_confirmed: true,
        };
        assert_discarded(&manager, key).await;
    }

    /// A member event that was encrypted but could not be attributed to a device
    /// gives nothing to bind the key to, so the MSC4143 device match cannot be
    /// satisfied. Accepting anyway would downgrade it to a user-only check,
    /// which any of that user's devices could then pass.
    #[tokio::test]
    async fn key_is_discarded_when_the_member_event_has_no_attributable_device() {
        let unattributable = JoinedMembership {
            origin: EventOrigin::encrypted(None),
            ..bob_membership()
        };
        let manager = EncryptionManager::new(
            Arc::new(NoopBackend),
            USER_ID.to_string(),
            DEVICE_ID.to_string(),
            MEMBER_ID.to_string(),
            ROOM_ID.to_string(),
            SLOT_ID.to_string(),
            create_mock_get_memberships(vec![unattributable]),
        );

        assert_discarded(&manager, bob_key(vec![1u8; 32], 0)).await;
    }

    /// Same for a member event known to have arrived in the clear.
    #[tokio::test]
    async fn key_is_discarded_when_the_member_event_was_cleartext() {
        let cleartext = JoinedMembership {
            origin: EventOrigin::Cleartext,
            ..bob_membership()
        };
        let manager = EncryptionManager::new(
            Arc::new(NoopBackend),
            USER_ID.to_string(),
            DEVICE_ID.to_string(),
            MEMBER_ID.to_string(),
            ROOM_ID.to_string(),
            SLOT_ID.to_string(),
            create_mock_get_memberships(vec![cleartext]),
        );

        assert_discarded(&manager, bob_key(vec![1u8; 32], 0)).await;
    }

    /// A member event that only *claims* its device still binds the key to that
    /// device — the pre-2026 Element Call path, where no authenticated device
    /// exists to be had. The claim narrows rather than widens: the key must
    /// still arrive from the named device.
    #[tokio::test]
    async fn key_is_checked_against_a_claimed_device() {
        let claimed = |device_id: &str| {
            EncryptionManager::new(
                Arc::new(NoopBackend),
                USER_ID.to_string(),
                DEVICE_ID.to_string(),
                MEMBER_ID.to_string(),
                ROOM_ID.to_string(),
                SLOT_ID.to_string(),
                create_mock_get_memberships(vec![JoinedMembership {
                    origin: EventOrigin::claimed(device_id),
                    ..bob_membership()
                }]),
            )
        };

        // Bob's key arrives Olm-encrypted from the device his member event
        // named, which is exactly the match MSC4143 asks for.
        let manager = claimed("device456");
        manager
            .receive_key(bob_key(vec![1u8; 32], 0))
            .await
            .expect("should succeed");
        assert_eq!(manager.get_inbound_keys("bob-device456-uuid").len(), 1);

        // A claim naming some other device is not a licence to accept anything:
        // the key came from `device456`, so it does not match.
        assert_discarded(&claimed("someOtherDevice"), bob_key(vec![1u8; 32], 0)).await;
    }

    /// MSC4153: keys from devices that are not cross-signed are discarded when
    /// the client is configured to exclude insecure devices.
    #[tokio::test]
    async fn key_from_unconfirmed_sender_is_discarded_when_required() {
        let manager = manager_with_bob();
        let mut key = bob_key(vec![1u8; 32], 0);
        key.origin = KeyOrigin::Encrypted {
            sender_user_id: "@bob:example.org".to_string(),
            sender_device_id: Some("device456".to_string()),
            sender_is_confirmed: false,
        };
        assert_discarded(&manager, key).await;
    }

    /// ...and accepted when the deployment has opted out of that requirement.
    #[tokio::test]
    async fn key_from_unconfirmed_sender_is_accepted_when_not_required() {
        let mut manager = manager_with_bob();
        manager.set_config(EncryptionConfig {
            require_confirmed_sender: false,
            ..EncryptionConfig::default()
        });

        let mut key = bob_key(vec![1u8; 32], 0);
        key.origin = KeyOrigin::Encrypted {
            sender_user_id: "@bob:example.org".to_string(),
            sender_device_id: Some("device456".to_string()),
            sender_is_confirmed: false,
        };
        manager.receive_key(key).await.expect("should succeed");

        assert_eq!(manager.get_inbound_keys("bob-device456-uuid").len(), 1);
    }

    /// The manager fans keys out to every session in a room, so a key naming a
    /// different room is not ours to hold.
    #[tokio::test]
    async fn key_for_another_room_is_discarded() {
        let manager = manager_with_bob();
        let mut key = bob_key(vec![1u8; 32], 0);
        key.room_id = "!other:example.org".to_string();
        assert_discarded(&manager, key).await;
    }

    /// A key arriving before its member event is held, then verified once the
    /// membership shows up — the check cannot simply be skipped for these.
    #[tokio::test]
    async fn buffered_key_is_verified_when_membership_arrives() {
        let memberships = Arc::new(Mutex::new(Vec::new()));
        let manager = {
            let memberships = memberships.clone();
            EncryptionManager::new(
                Arc::new(NoopBackend),
                USER_ID.to_string(),
                DEVICE_ID.to_string(),
                MEMBER_ID.to_string(),
                ROOM_ID.to_string(),
                SLOT_ID.to_string(),
                move || memberships.lock().unwrap().clone(),
            )
        };

        // Two keys claiming Bob's membership: one genuinely from Bob's device,
        // one from an impostor. Neither can be checked yet.
        let mut impostor = bob_key(vec![9u8; 32], 0);
        impostor.origin = KeyOrigin::Encrypted {
            sender_user_id: "@mallory:example.org".to_string(),
            sender_device_id: Some("device456".to_string()),
            sender_is_confirmed: true,
        };
        manager.receive_key(impostor).await.unwrap();
        manager
            .receive_key(bob_key(vec![1u8; 32], 1))
            .await
            .unwrap();
        assert!(manager.get_inbound_keys("bob-device456-uuid").is_empty());

        // Bob's membership arrives; the buffer is drained and checked.
        *memberships.lock().unwrap() = vec![bob_membership()];
        manager.on_memberships_update().await.unwrap();

        let keys = manager.get_inbound_keys("bob-device456-uuid");
        assert_eq!(keys.len(), 1, "only the genuine key should survive");
        assert_eq!(keys[0].key, vec![1u8; 32]);
    }

    #[tokio::test]
    async fn inbound_keys_do_not_follow_a_reused_membership_id() {
        let mut bob = bob_membership();
        bob.membership_ts = Some(100);
        let memberships = Arc::new(Mutex::new(vec![bob.clone()]));
        let mut manager = {
            let memberships = memberships.clone();
            EncryptionManager::new(
                Arc::new(NoopBackend), USER_ID.into(), DEVICE_ID.into(),
                MEMBER_ID.into(), ROOM_ID.into(), SLOT_ID.into(),
                move || memberships.lock().unwrap().clone(),
            )
        };
        let handler = Arc::new(RecordingHandler::default());
        manager.set_signal_handler(handler.clone());
        manager.receive_key(bob_key(vec![1u8; 32], 0)).await.unwrap();
        bob.membership_event_id = Some("$refresh".into());
        bob.origin_server_ts = Some(120);
        *memberships.lock().unwrap() = vec![bob.clone()];
        manager.on_memberships_update().await.unwrap();
        assert_eq!(manager.get_inbound_keys(&bob.member_id).len(), 1);
        handler.signals.lock().unwrap().clear();
        manager.replay_keys_to_handler().await;
        assert_eq!(handler.signals.lock().unwrap().len(), 1);

        bob.membership_ts = Some(200);
        *memberships.lock().unwrap() = vec![bob.clone()];
        // Even before the asynchronous roster callback, the old key cannot replay.
        handler.signals.lock().unwrap().clear();
        assert!(manager.get_inbound_keys(&bob.member_id).is_empty());
        assert!(manager.get_all_inbound_keys().is_empty());
        manager.replay_keys_to_handler().await;
        assert!(handler.signals.lock().unwrap().is_empty());
        manager.on_memberships_update().await.unwrap();
        assert!(manager.inbound_keys.read().unwrap().is_empty());
        manager.receive_key(bob_key(vec![2u8; 32], 0)).await.unwrap();
        assert_eq!(manager.get_inbound_keys(&bob.member_id)[0].key, vec![2u8; 32]);

        // The same member ID and timestamp do not authorize a different device.
        let mut changed_device = bob.clone();
        changed_device.origin = EventOrigin::encrypted(Some("other-device".into()));
        *memberships.lock().unwrap() = vec![changed_device];
        handler.signals.lock().unwrap().clear();
        manager.replay_keys_to_handler().await;
        assert!(handler.signals.lock().unwrap().is_empty());
        assert!(manager.get_inbound_keys(&bob.member_id).is_empty());

        // An ambiguous same-ID roster cannot admit or replay a key.

        let mut other = bob.clone();
        other.membership_ts = Some(300);
        *memberships.lock().unwrap() = vec![bob.clone(), other];
        manager.receive_key(bob_key(vec![3u8; 32], 0)).await.unwrap();
        manager.replay_keys_to_handler().await;
        assert!(handler.signals.lock().unwrap().is_empty());
        manager.on_memberships_update().await.unwrap();
        assert!(manager.inbound_keys.read().unwrap().is_empty());
    }

    #[tokio::test]
    async fn unmatched_inbound_keys_are_bounded_and_known_keys_bypass_capacity() {
        let memberships = Arc::new(Mutex::new(Vec::new()));
        let manager = {
            let memberships = memberships.clone();
            EncryptionManager::new(
                Arc::new(NoopBackend), USER_ID.into(), DEVICE_ID.into(),
                MEMBER_ID.into(), ROOM_ID.into(), SLOT_ID.into(),
                move || memberships.lock().unwrap().clone(),
            )
        };
        for i in 0..MAX_PENDING_INBOUND_KEYS {
            let mut key = bob_key(vec![1u8; 32], 0);
            key.member_id = format!("pending-{i}");
            manager.receive_key(key.clone()).await.unwrap();
            manager.receive_key(key).await.unwrap();
        }
        manager.receive_key(bob_key(vec![1u8; 32], 0)).await.unwrap();
        assert_eq!(manager.keys_without_membership.lock().unwrap().len(), MAX_PENDING_INBOUND_KEYS);
        *memberships.lock().unwrap() = vec![bob_membership()];
        manager.on_memberships_update().await.unwrap();
        manager.receive_key(bob_key(vec![2u8; 32], 1)).await.unwrap();
        assert_eq!(manager.get_inbound_keys("bob-device456-uuid").len(), 1);
        assert_eq!(manager.keys_without_membership.lock().unwrap().len(), MAX_PENDING_INBOUND_KEYS);
        manager.leave();
        assert!(manager.keys_without_membership.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn unmatched_key_expiry_is_not_extended_by_duplicate() {
        let memberships = Arc::new(Mutex::new(Vec::new()));
        let mut manager = manager_over(Arc::new(MockBackend::new()), memberships.clone());
        let clock = TestClock::new();
        manager.set_clock(clock.as_rtc_clock());
        let old = bob_key(vec![1u8; 32], 0);
        manager.receive_key(old.clone()).await.unwrap();
        clock.advance(MAX_PENDING_INBOUND_KEY_AGE_MS - 1);
        manager.receive_key(old).await.unwrap();
        manager.receive_key(bob_key(vec![2u8; 32], 1)).await.unwrap();
        assert_eq!(manager.keys_without_membership.lock().unwrap().len(), 2);
        clock.advance(1);
        *memberships.lock().unwrap() = vec![bob_membership()];
        manager.on_memberships_update().await.unwrap();
        let keys = manager.get_inbound_keys("bob-device456-uuid");
        assert_eq!(keys.len(), 1);
        assert_eq!(keys[0].key, vec![2u8; 32]);
        assert!(manager.keys_without_membership.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn idle_pending_key_prune_preserves_unexpired_and_admitted_keys() {
        let memberships = Arc::new(Mutex::new(vec![bob_membership()]));
        let mut manager = manager_over(Arc::new(MockBackend::new()), memberships.clone());
        let clock = TestClock::new();
        manager.set_clock(clock.as_rtc_clock());
        manager.receive_key(bob_key(vec![3u8; 32], 0)).await.unwrap();
        let mut old = bob_key(vec![1u8; 32], 0);
        old.member_id = "pending-old".into();
        manager.receive_key(old).await.unwrap();
        clock.advance(MAX_PENDING_INBOUND_KEY_AGE_MS - 1);
        manager.prune_expired_pending_keys();
        assert_eq!(manager.keys_without_membership.lock().unwrap().len(), 1);
        let mut fresh = bob_key(vec![2u8; 32], 1);
        fresh.member_id = "pending-fresh".into();
        manager.receive_key(fresh).await.unwrap();
        clock.advance(1);
        manager.prune_expired_pending_keys();
        {
            let waiting = manager.keys_without_membership.lock().unwrap();
            assert_eq!(waiting.len(), 1);
            assert_eq!(waiting[0].key.member_id, "pending-fresh");
        }
        assert_eq!(manager.get_inbound_keys("bob-device456-uuid")[0].key, vec![3u8; 32]);
        clock.advance(MAX_PENDING_INBOUND_KEY_AGE_MS);
        manager.prune_expired_pending_keys();
        assert!(manager.keys_without_membership.lock().unwrap().is_empty());
        assert_eq!(manager.get_inbound_keys("bob-device456-uuid").len(), 1);
    }

    #[tokio::test]
    async fn expired_unmatched_entries_release_capacity_on_next_admission() {
        let memberships = Arc::new(Mutex::new(Vec::new()));
        let mut manager = manager_over(Arc::new(MockBackend::new()), memberships.clone());
        let clock = TestClock::new();
        manager.set_clock(clock.as_rtc_clock());
        for i in 0..MAX_PENDING_INBOUND_KEYS {
            let mut key = bob_key(vec![1u8; 32], 0);
            key.member_id = format!("expired-{i}");
            manager.receive_key(key).await.unwrap();
        }
        clock.advance(MAX_PENDING_INBOUND_KEY_AGE_MS);
        manager.receive_key(bob_key(vec![2u8; 32], 1)).await.unwrap();
        assert_eq!(manager.keys_without_membership.lock().unwrap().len(), 1);
        *memberships.lock().unwrap() = vec![bob_membership()];
        manager.on_memberships_update().await.unwrap();
        assert_eq!(manager.get_inbound_keys("bob-device456-uuid")[0].key, vec![2u8; 32]);
    }

    #[tokio::test]
    async fn oversized_inbound_key_is_not_stored() {
        let manager = manager_with_bob();
        manager.receive_key(bob_key(vec![1u8; MAX_ENCODED_MEDIA_KEY_LEN], 0)).await.unwrap();
        assert!(manager.get_inbound_keys("bob-device456-uuid").is_empty());
    }

    #[tokio::test]
    async fn direct_key_admission_bounds_all_retained_metadata() {
        let memberships = Arc::new(Mutex::new(Vec::new()));
        let manager = manager_over(Arc::new(MockBackend::new()), memberships);
        let normal = bob_key(vec![1u8; 32], 0);
        let mut boundary = normal.clone();
        let normal_member_len = boundary.member_id.len();
        let mut low = normal_member_len;
        let mut high = MAX_INBOUND_MEDIA_KEY_BYTES;
        while low < high {
            let mid = (low + high + 1) / 2;
            boundary.member_id = "x".repeat(mid);
            if inbound_key_fits_budget(&boundary) { low = mid; } else { high = mid - 1; }
        }
        boundary.member_id = "x".repeat(low);
        assert!(inbound_key_fits_budget(&boundary));
        boundary.member_id.push('x');
        assert!(!inbound_key_fits_budget(&boundary));
        manager.receive_key(boundary).await.unwrap();
        for field in 0..3 {
            let mut oversized = normal.clone();
            let value = "x".repeat(MAX_INBOUND_MEDIA_KEY_BYTES + 1);
            match field {
                0 => oversized.room_id = value,
                1 => oversized.origin = KeyOrigin::Encrypted {
                    sender_user_id: value, sender_device_id: None, sender_is_confirmed: true,
                },
                _ => oversized.origin = KeyOrigin::Encrypted {
                    sender_user_id: "@bob:example.org".into(),
                    sender_device_id: Some(value), sender_is_confirmed: true,
                },
            }
            manager.receive_key(oversized).await.unwrap();
        }
        assert!(manager.keys_without_membership.lock().unwrap().is_empty());
        manager.receive_key(normal).await.unwrap();
        assert_eq!(manager.keys_without_membership.lock().unwrap().len(), 1);
    }

    /// A discarded key must not occupy its (member, index) slot in the
    /// outdated-key filter, or an impostor could suppress the genuine key.
    #[tokio::test]
    async fn discarded_key_does_not_block_the_genuine_one() {
        let manager = manager_with_bob();

        let mut impostor = bob_key(vec![9u8; 32], 0);
        impostor.origin = KeyOrigin::Encrypted {
            sender_user_id: "@mallory:example.org".to_string(),
            sender_device_id: Some("device456".to_string()),
            sender_is_confirmed: true,
        };
        manager.receive_key(impostor).await.unwrap();

        manager
            .receive_key(bob_key(vec![1u8; 32], 0))
            .await
            .unwrap();

        let keys = manager.get_inbound_keys("bob-device456-uuid");
        assert_eq!(keys.len(), 1);
        assert_eq!(keys[0].key, vec![1u8; 32]);
    }

    /// The to-device payload states its encoding in `format`, which MSC4143
    /// defines as a number; `0` is raw bytes, base64 encoded.
    #[tokio::test]
    async fn distributed_key_has_exactly_the_msc_fields_and_an_unpadded_key() {
        let sender = Arc::new(MockBackend::new());
        let manager = EncryptionManager::new(
            sender.clone(),
            USER_ID.to_string(),
            DEVICE_ID.to_string(),
            MEMBER_ID.to_string(),
            ROOM_ID.to_string(),
            SLOT_ID.to_string(),
            create_mock_get_memberships(vec![bob_membership()]),
        );

        manager.join().await.unwrap();
        manager.on_memberships_update().await.unwrap();

        let messages = sender.to_device_messages.lock().unwrap();
        let (_, _, message_type, content) = messages.first().expect("a key should have been sent");
        assert_eq!(message_type, KEY_MESSAGE_TYPE);
        let mut fields: Vec<&str> = content
            .as_object()
            .unwrap()
            .keys()
            .map(String::as_str)
            .collect();
        fields.sort();
        assert_eq!(fields, ["media_key", "member_id", "room_id"]);
        let key = content.pointer("/media_key/key").unwrap().as_str().unwrap();
        assert!(!key.ends_with('='), "MSC4143 media keys are unpadded");
        assert_eq!(MEDIA_KEY_BASE64.decode(key).unwrap().len(), 32);
    }

    #[tokio::test]
    async fn test_leave_cleans_up() {
        let mock_sender = Arc::new(NoopBackend);
        let get_memberships = create_mock_get_memberships(vec![]);

        let manager = EncryptionManager::new(
            mock_sender.clone(),
            USER_ID.to_string(),
            DEVICE_ID.to_string(),
            MEMBER_ID.to_string(),
            ROOM_ID.to_string(),
            SLOT_ID.to_string(),
            get_memberships,
        );

        manager.join().await.expect("join should succeed");

        // Verify we have state
        assert!(manager.get_outbound_key().is_some());

        // Leave
        manager.leave();

        // Verify state is cleaned up
        assert!(manager.get_outbound_key().is_none());
        assert!(manager.get_all_inbound_keys().is_empty());
    }

    /// A membership of our own device under a *different* `member_id` — the
    /// ghost we leave behind by dying without leaving and rejoining inside its
    /// sticky lifetime. It must not be treated as a recipient: Olm has no
    /// session with the sending device, so the send fails, and the same
    /// user+device under a new id also forces a rotation — which the failure
    /// would then abandon.
    #[tokio::test]
    async fn our_own_stale_membership_is_never_a_key_recipient() {
        let ghost = JoinedMembership {
            member_id: "alice-device123-previous-join".to_string(),
            sticky_key: "alice-device123-previous-join".to_string(),
            sender: USER_ID.to_string(),
            origin: EventOrigin::encrypted(Some(DEVICE_ID.to_string())),
            ..bob_membership()
        };
        let mock_sender = Arc::new(MockBackend::new());
        let get_memberships = create_mock_get_memberships(vec![ghost, bob_membership()]);
        let manager = EncryptionManager::new(
            mock_sender.clone(),
            USER_ID.to_string(),
            DEVICE_ID.to_string(),
            MEMBER_ID.to_string(),
            ROOM_ID.to_string(),
            SLOT_ID.to_string(),
            get_memberships,
        );

        manager.join().await.expect("join should succeed");
        manager
            .on_memberships_update()
            .await
            .expect("distribution should succeed");

        assert!(
            to_device_recipients(&mock_sender)
                .iter()
                .all(|(user_id, device_id)| !(user_id == USER_ID && device_id == DEVICE_ID)),
            "we must never send our own media key to our own device, got {:?}",
            to_device_recipients(&mock_sender),
        );
        assert_eq!(
            to_device_recipients(&mock_sender),
            vec![("@bob:example.org".to_string(), "device456".to_string())],
            "bob is the only real recipient",
        );
    }

    /// Another device of our own user is an ordinary recipient — only *this*
    /// device is excluded.
    #[tokio::test]
    async fn another_device_of_our_own_user_still_receives_the_key() {
        let other_device = JoinedMembership {
            member_id: "alice-tablet-uuid".to_string(),
            sticky_key: "alice-tablet-uuid".to_string(),
            sender: USER_ID.to_string(),
            origin: EventOrigin::encrypted(Some("TABLET".to_string())),
            ..bob_membership()
        };
        let mock_sender = Arc::new(MockBackend::new());
        let get_memberships = create_mock_get_memberships(vec![other_device]);
        let manager = EncryptionManager::new(
            mock_sender.clone(),
            USER_ID.to_string(),
            DEVICE_ID.to_string(),
            MEMBER_ID.to_string(),
            ROOM_ID.to_string(),
            SLOT_ID.to_string(),
            get_memberships,
        );

        manager.join().await.expect("join should succeed");
        manager
            .on_memberships_update()
            .await
            .expect("distribution should succeed");

        assert_eq!(
            to_device_recipients(&mock_sender),
            vec![(USER_ID.to_string(), "TABLET".to_string())],
        );
    }

    /// No `"*"` fallback: a membership that names no sending device is
    /// unreachable, and broadcasting the key to every device of that user would
    /// hand it to devices that are not in the call.
    #[tokio::test]
    async fn a_membership_with_no_sending_device_is_skipped_not_broadcast() {
        let deviceless = JoinedMembership {
            origin: EventOrigin::encrypted(None),
            ..bob_membership()
        };
        let mock_sender = Arc::new(MockBackend::new());
        let get_memberships = create_mock_get_memberships(vec![deviceless]);
        let manager = EncryptionManager::new(
            mock_sender.clone(),
            USER_ID.to_string(),
            DEVICE_ID.to_string(),
            MEMBER_ID.to_string(),
            ROOM_ID.to_string(),
            SLOT_ID.to_string(),
            get_memberships,
        );

        manager.join().await.expect("join should succeed");
        manager
            .on_memberships_update()
            .await
            .expect("distribution should succeed");

        assert!(
            to_device_recipients(&mock_sender).is_empty(),
            "expected no send at all, got {:?}",
            to_device_recipients(&mock_sender),
        );
    }

    /// One unreachable recipient must not cost the others their key, must not
    /// abandon the rotation (the key still has to be stored and signalled) —
    /// and must not be recorded as holding the key, or it is never retried and
    /// that member cannot decrypt us for the rest of the call.
    #[tokio::test]
    async fn a_failed_recipient_does_not_abort_the_rollout() {
        struct FailsFirstRecipient {
            attempted: Mutex<Vec<String>>,
        }

        #[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
        #[cfg_attr(not(target_arch = "wasm32"), async_trait)]
        impl MatrixBackend for FailsFirstRecipient {
            fn own_user_id(&self) -> String {
                "@alice:example.org".to_owned()
            }
            fn own_device_id(&self) -> String {
                "DEVICE".to_owned()
            }
            async fn send_sticky_event(
                &self,
                _room_id: String,
                _event_type: String,
                _content: serde_json::Value,
                _duration_ms: u64,
            ) -> Result<String, CommandError> {
                Ok("$sticky".to_string())
            }
            async fn send_delayed_state_event(
                &self,
                room_id: String,
                event_type: String,
                _state_key: String,
                content: serde_json::Value,
                delay_ms: u64,
            ) -> Result<String, CommandError> {
                self.send_delayed_sticky_event(room_id, event_type, content, 0, delay_ms)
                    .await
            }

            async fn send_delayed_sticky_event(
                &self,
                _room_id: String,
                _event_type: String,
                _content: serde_json::Value,
                _sticky_duration_ms: u64,
                _delay_ms: u64,
            ) -> Result<String, CommandError> {
                Ok("$delay".to_string())
            }
            async fn send_room_event(
                &self,
                _room_id: String,
                _event_type: String,
                _content: serde_json::Value,
            ) -> Result<String, CommandError> {
                Ok("$room".to_string())
            }
            async fn redact_event(
                &self,
                _room_id: String,
                _event_id: String,
                _reason: Option<String>,
            ) -> Result<(), CommandError> {
                Ok(())
            }
            async fn restart_delayed_event(
                &self,
                _room_id: String,
                _delay_id: String,
            ) -> Result<(), CommandError> {
                Ok(())
            }
            async fn cancel_delayed_event(
                &self,
                _room_id: String,
                _delay_id: String,
            ) -> Result<(), CommandError> {
                Ok(())
            }
            async fn send_to_device_message(
                &self,
                recipients: Vec<ToDeviceRecipient>,
                _message_type: String,
                _content: serde_json::Value,
            ) -> Result<Vec<ToDeviceDelivery>, CommandError> {
                let mut attempted = self.attempted.lock().unwrap();
                Ok(recipients
                    .into_iter()
                    .map(|recipient| {
                        attempted.push(recipient.user_id.clone());
                        if recipient.user_id == "@bob:example.org" {
                            ToDeviceDelivery::failed(recipient, "no olm session")
                        } else {
                            ToDeviceDelivery::sent(recipient)
                        }
                    })
                    .collect())
            }
            async fn send_state_event(
                &self,
                _room_id: String,
                _event_type: String,
                _state_key: String,
                _content: serde_json::Value,
            ) -> Result<String, CommandError> {
                Ok("$state".to_string())
            }
        }

        let carol = JoinedMembership {
            member_id: "carol-device789-uuid".to_string(),
            sticky_key: "carol-device789-uuid".to_string(),
            sender: "@carol:example.org".to_string(),
            origin: EventOrigin::encrypted(Some("device789".to_string())),
            ..bob_membership()
        };
        let sender = Arc::new(FailsFirstRecipient {
            attempted: Mutex::new(Vec::new()),
        });
        let handler = Arc::new(RecordingHandler::default());
        let get_memberships = create_mock_get_memberships(vec![bob_membership(), carol]);
        let mut manager = EncryptionManager::new(
            sender.clone(),
            USER_ID.to_string(),
            DEVICE_ID.to_string(),
            MEMBER_ID.to_string(),
            ROOM_ID.to_string(),
            SLOT_ID.to_string(),
            get_memberships,
        );
        manager.set_signal_handler(handler.clone());

        manager.join().await.expect("join should succeed");
        manager
            .on_memberships_update()
            .await
            .expect("a failed recipient must not fail the rollout");

        assert_eq!(
            *sender.attempted.lock().unwrap(),
            vec!["@bob:example.org", "@carol:example.org"],
            "carol must still be attempted after bob failed",
        );
        assert!(
            manager.get_outbound_key().is_some(),
            "the key must still be stored, or we rotate again next update and never converge",
        );
        assert!(
            !handler.signals.lock().unwrap().is_empty(),
            "the key must still reach the media layer, or we encrypt with nothing",
        );

        let shared_with: Vec<String> = manager
            .get_outbound_key()
            .expect("the key is stored")
            .shared_with
            .into_iter()
            .map(|participant| participant.user_id)
            .collect();
        assert_eq!(
            shared_with,
            vec!["@carol:example.org"],
            "only the recipient the send actually reached may be recorded as holding \
             the key; recording bob would mean never retrying him",
        );

        // Next rollout: bob is not in `shared_with`, so he counts as newly
        // joined and is addressed again.
        sender.attempted.lock().unwrap().clear();
        manager
            .on_memberships_update()
            .await
            .expect("the retry rollout should succeed");
        assert_eq!(
            *sender.attempted.lock().unwrap(),
            vec!["@bob:example.org"],
            "the unreached recipient should be retried, and the served one left alone",
        );
    }
}
