// Copyright 2026 Element Creations Ltd.
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE in the repository root for full details.

//! The `matrix_sdk::Client` implementation of [`MatrixBackend`]: sends as
//! Client-Server requests, room subjects re-read from the SDK on every sync
//! that touches the room, to-device and timeline events from event handlers.
//! Dialects and MatrixRTC parsing are not here: the wrapper and the feeder do
//! those for every backend. Requires the `matrix-sdk` feature.

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex as StdMutex};
use std::time::Duration;

use async_trait::async_trait;
use matrix_rtc_core::{
    BackendError, CommandError, EventEncryption, EventIn, LiveKitTokenRequest, MatrixBackend,
    OpenIdToken, RoomSink, RoomSubjects, Subscription, ToDeviceDelivery, ToDeviceMessageIn,
    ToDeviceRecipient, ToDeviceSink,
};
use matrix_sdk::deserialized_responses::{
    AlgorithmInfo, DeviceLinkProblem, EncryptionInfo, VerificationLevel,
    VerificationState,
};
use matrix_sdk::encryption::identities::Device;
use matrix_sdk::event_handler::EventHandlerDropGuard;
use matrix_sdk::room::{IncludeRelations, RelationsOptions};
use matrix_sdk::ruma;
use matrix_sdk::ruma::api::client::delayed_events::update_delayed_event::UpdateAction;
use matrix_sdk::ruma::api::client::delayed_events::{
    DelayParameters, delayed_state_event, update_delayed_event,
};
use matrix_sdk::ruma::api::client::state::{get_state_events, send_state_event};
use matrix_sdk::ruma::api::client::keys::get_keys;
use matrix_sdk::ruma::api::error::{ErrorKind, RetryAfter};
use matrix_sdk::ruma::events::relation::RelationType;
use matrix_sdk::ruma::events::{
    AnyStateEventContent, AnySyncMessageLikeEvent, AnyToDeviceEvent, AnyToDeviceEventContent,
    MessageLikeEventType, StateEventType, TimelineEventType,
};
use matrix_sdk::ruma::serde::Raw;
use matrix_sdk::ruma::{DeviceId, EventId, OwnedEventId, OwnedUserId, RoomId, UInt, UserId};
use matrix_sdk::{Client, Room, RoomMemberships};

use crate::{rtc_requests, CallKeyRecipients, CallRecipientKeys};
use matrix_sdk_base::crypto::{CollectStrategy, OlmError, types::{MasterPubkey, SelfSigningPubkey, Signatures}};
use serde_json::Value;
use tokio::sync::broadcast::error::RecvError;
use tokio::task::JoinHandle;

// The sticky duration for `m.rtc.member` comes from the core
// (`JoinSessionParams::sticky_duration_ms`), which re-sends the membership at
// half that interval to stay in the map.
//
// NOTE: this host sends no delayed leave (`send_delayed_sticky_event` refuses
// until matrix-rust-sdk#6973): a plain delayed event would fire without ending
// the sticky membership (spec 038 R13). The core then joins on a 5 minute
// membership lifetime, which bounds a crash (R43). Both
// `org.matrix.msc4140.delay` and `org.matrix.msc4354.sticky_duration_ms` are
// query parameters on the same `PUT /send`; ruma's request types cannot
// express both yet.

/// An SDK error that may carry the server's standard error body.
trait ClientApiError: std::fmt::Display {
    fn client_api_error(&self) -> Option<&ruma::api::error::Error>;

    /// Whether our own crypto refused the recipients before anything was sent.
    fn refused_by_crypto(&self) -> bool {
        false
    }
}

impl ClientApiError for matrix_sdk::Error {
    fn client_api_error(&self) -> Option<&ruma::api::error::Error> {
        self.as_client_api_error()
    }

    /// MSC4153: our own device unsigned, or a verified peer's identity changed.
    /// Hit by an encrypted to-device send and by a room event's key share alike
    /// (the sticky membership included).
    fn refused_by_crypto(&self) -> bool {
        matches!(self, matrix_sdk::Error::OlmError(olm)
            if matches!(**olm, OlmError::SessionRecipientCollectionError(_)))
    }
}

impl ClientApiError for matrix_sdk::HttpError {
    fn client_api_error(&self) -> Option<&ruma::api::error::Error> {
        self.as_client_api_error()
    }
}

/// What the homeserver said, for the core to classify (034 R3, R7): the
/// `errcode`, the HTTP status and a rate limit's delay. The SDK's own request
/// config still retries underneath, bounded by [`rtc_request_config`].
fn sdk_backend_error(error: &impl ClientApiError) -> BackendError {
    let api_error = error.client_api_error();
    let kind = api_error.and_then(ruma::api::error::Error::error_kind);
    let retry_after_ms = match kind {
        Some(ErrorKind::LimitExceeded(limit)) => limit.retry_after.as_ref().map(|retry_after| {
            let delay = match retry_after {
                RetryAfter::Delay(delay) => *delay,
                RetryAfter::DateTime(at) => at
                    .duration_since(std::time::SystemTime::now())
                    .unwrap_or_default(),
            };
            delay.as_millis() as u64
        }),
        _ => None,
    };
    BackendError::with_matrix_error(
        kind.map(|kind| kind.errcode().to_string()),
        api_error.map(|error| error.status_code.as_u16()),
        error.to_string(),
    )
    .with_retry_after_ms(retry_after_ms)
}

/// A crypto refusal is decided here before anything is sent: final, and no
/// word on the homeserver, so it must not read as signalling unreachable (R3).
/// A refused membership fails the join rather than retrying to its deadline.
fn command_error(error: impl ClientApiError) -> CommandError {
    let backend = sdk_backend_error(&error);
    CommandError::Backend(if error.refused_by_crypto() {
        backend.permanent()
    } else {
        backend
    })
}

/// A request this backend could not even build: final, nothing to retry.
fn local_error(error: impl std::fmt::Display) -> CommandError {
    CommandError::ClientRejected(error.to_string())
}

/// [`command_error`] for the MSC4140 endpoints: `M_UNRECOGNIZED` is a
/// homeserver without the endpoint, `M_FORBIDDEN` one that switched it off
/// (matrix.org's "Sending delayed events has been disallowed"). Either makes
/// the core stop asking for the rest of the session.
fn delayed_command_error(error: matrix_sdk::HttpError) -> CommandError {
    match error.client_api_error_kind() {
        Some(ErrorKind::Unrecognized | ErrorKind::Forbidden) => {
            CommandError::DelayedEventsNotSupported(error.to_string())
        }
        _ => command_error(error),
    }
}

fn backend_error(error: impl std::fmt::Display) -> BackendError {
    BackendError::new(error.to_string())
}

/// Ruma owns the wire spelling: the core's `m.rtc.member` becomes the MSC4143
/// id and follows the stable one once ruma flips.
fn wire_event_type(event_type: String) -> MessageLikeEventType {
    MessageLikeEventType::from(event_type)
}

/// Bounded request behaviour for RTC signalling sends, kept inside the core's
/// 30 s attempt deadline (034 R2): the core's outbox does the retrying. The
/// SDK's one retry resends the same request, so the same transaction id, and
/// cannot duplicate an event whose first attempt landed but timed out. 10 s
/// is far past a large homeserver's send time (250–700 ms, tail to ~3 s);
/// two attempts with the SDK's 500 ms backoff fit in about 21 s. A rate limit
/// still makes the SDK wait the server's `Retry-After` before its retry.
fn rtc_request_config() -> matrix_sdk::config::RequestConfig {
    matrix_sdk::config::RequestConfig::new()
        .timeout(Duration::from_secs(10))
        .retry_limit(2)
}

/// How long wakes are coalesced before a room's subjects are re-read: a sync
/// that touches the room can wake twice, and each re-read is a state fetch.
const WAKE_DEBOUNCE: Duration = Duration::from_millis(250);

/// How long to wait before re-reading a subject whose fetch failed.
const RETRY_AFTER: Duration = Duration::from_secs(5);

/// Bound the entire authoritative snapshot read, including SDK-internal
/// retries and member loading. No partial snapshot is published on timeout.
const ROOM_SNAPSHOT_TIMEOUT: Duration = Duration::from_secs(30);

async fn bounded_room_snapshot(read: impl std::future::Future<Output = bool>) -> bool {
    match tokio::time::timeout(ROOM_SNAPSHOT_TIMEOUT, read).await {
        Ok(complete) => complete,
        Err(_) => false,
    }
}

/// A failed read must wait independently of sync wakes or legacy polling.
/// Subscription cancellation aborts this wait along with the feeder task.
async fn retry_incomplete_snapshot(complete: bool) -> bool {
    if complete {
        return false;
    }
    tokio::time::sleep(RETRY_AFTER).await;
    true
}

/// One bounded refresh per user in a delivery batch. A failed refresh is
/// also remembered: later devices of the same user must not fall back to a
/// cached record, or turn one failing user into an unbounded retry loop.
const DEVICE_REFRESH_TIMEOUT: Duration = Duration::from_secs(5);
// Leave time within the core's 30-second delivery attempt for Olm and send.
const DEVICE_REFRESH_BATCH_TIMEOUT: Duration = Duration::from_secs(10);

// request_user_identity applies the SDK's cryptographic validation, but its
// public return value omits the federation failures and the actual device list.
// Independently observe the current response, then require the validated store
// to agree. A raw response alone never establishes cryptographic trust.
pub struct FreshUserKeys {
    user: OwnedUserId,
    response: get_keys::v3::Response,
}

fn same_signed_device(mut observed: Value, mut stored: Value) -> bool {
    let (Some(observed), Some(stored)) = (observed.as_object_mut(), stored.as_object_mut()) else {
        return false;
    };
    // Display names are unsigned and may be rewritten by the homeserver.
    observed.remove("unsigned");
    stored.remove("unsigned");
    observed == stored
}

fn observed_master(raw: Option<&Raw<ruma::encryption::CrossSigningKey>>, user: &UserId)
    -> Result<Option<String>, &'static str> {
    let Some(raw) = raw else { return Ok(None) };
    let key = raw.deserialize().map_err(|_| "invalid master key response")?;
    if key.user_id.as_str() != user.as_str() || key.usage != [ruma::encryption::KeyUsage::Master]
        || key.keys.len() != 1 {
        return Err("invalid master key identity");
    }
    let (id, value) = key.keys.iter().next().ok_or("missing master key")?;
    if id.as_str() != format!("ed25519:{value}") || value.is_empty() {
        return Err("invalid master key identifier");
    }
    Ok(Some(value.clone()))
}

// The SDK may still hold an old self-signing key under the same master.
// For a cross-signed device require the freshly returned self-signing key to
// be signed by the validated master and to sign the exact validated device.
fn fresh_cross_signature(raw: &Raw<ruma::encryption::CrossSigningKey>,
    master: &MasterPubkey, device: &Device) -> Result<(), String> {
    let self_key: SelfSigningPubkey = serde_json::from_str(raw.json().get()).map_err(|e| e.to_string())?;
    if self_key.user_id() != device.user_id() || master.user_id() != device.user_id()
        || self_key.keys().iter().count() != 1 || master.keys().iter().count() != 1 {
        return Err("invalid self-signing key identity".into());
    }
    let (self_id, self_public) = self_key.keys().iter().next().ok_or("missing self-signing key")?;
    let self_public = self_public.ed25519().ok_or("invalid self-signing algorithm")?;
    if self_id.as_str() != format!("ed25519:{}", self_public.to_base64()) {
        return Err("invalid self-signing key identifier".into());
    }
    let (id, key) = master.keys().iter().next().ok_or("missing master key")?;
    let key = key.ed25519().ok_or("invalid master key algorithm")?;
    let signatures = raw.get_field::<Signatures>("signatures").map_err(|e| e.to_string())?
        .ok_or("missing self-signing signatures")?;
    let signature = signatures.get_signature(master.user_id(), id).ok_or("missing master signature")?;
    let mut canonical: ruma::CanonicalJsonValue = serde_json::from_str(raw.json().get()).map_err(|e| e.to_string())?;
    let fields = canonical.as_object_mut().ok_or("invalid self-signing key object")?;
    fields.remove("signatures"); fields.remove("unsigned");
    key.verify(canonical.to_string().as_bytes(), &signature).map_err(|_| "invalid master signature")?;
    self_key.verify_device_keys(device.as_device_keys()).map_err(|e| e.to_string())
}

fn require_complete_key_response(response: &get_keys::v3::Response, user: &UserId) -> Result<(), String> {
    if !response.failures.is_empty() || !response.device_keys.contains_key(user) {
        return Err("incomplete device key query".into());
    }
    observed_master(response.master_keys.get(user), user).map_err(str::to_owned)?;
    Ok(())
}

pub async fn refresh_user_keys(client: &Client, user: &UserId) -> Result<FreshUserKeys, String> {
    let mut request = get_keys::v3::Request::new();
    request.device_keys.insert(user.to_owned(), Vec::new());
    request.timeout = Some(Duration::from_secs(4));
    let encryption = client.encryption();
    // Both requests are read-only on the server. Run in parallel within the
    // original caller budget, not two serial five-second waits.
    let (response, _) = tokio::try_join!(
        async { client.send(request).with_request_config(rtc_request_config().disable_retry()).await.map_err(|e| e.to_string()) },
        async { encryption.request_user_identity(user).await.map_err(|e| e.to_string()) },
    )?;
    require_complete_key_response(&response, user)?;
    Ok(FreshUserKeys { user: user.to_owned(), response })
}

impl FreshUserKeys {
    pub async fn confirmed_device(&self, client: &Client, device_id: &DeviceId)
        -> Result<(Device, Option<String>), String> {
        let raw = self.response.device_keys.get(&self.user)
            .and_then(|devices| devices.get(device_id)).ok_or("device absent from fresh query")?;
        let encryption = client.encryption();
        let identity = encryption.get_user_identity(&self.user).await.map_err(|e| e.to_string())?;
        if identity.as_ref().is_some_and(|id| id.has_verification_violation()) {
            return Err("user identity verification was lost".into());
        }
        let master = identity.as_ref().and_then(|id| id.master_key().get_first_key().map(|key| key.to_base64()));
        if master != observed_master(self.response.master_keys.get(&self.user), &self.user)? {
            return Err("fresh and validated master keys disagree".into());
        }
        let device = encryption.get_device(&self.user, device_id).await.map_err(|e| e.to_string())?
            .filter(|d| !d.is_deleted() && !d.is_blacklisted()).ok_or("device is unavailable")?;
        let observed: Value = serde_json::from_str(raw.json().get()).map_err(|e| e.to_string())?;
        let stored = serde_json::to_value(device.as_device_keys()).map_err(|e| e.to_string())?;
        if !same_signed_device(observed, stored) {
            return Err("fresh and validated device keys disagree".into());
        }
        if device.is_cross_signed_by_owner() {
            let raw = self.response.self_signing_keys.get(&self.user).ok_or("fresh self-signing key missing")?;
            let identity = identity.as_ref().ok_or("validated master key missing")?;
            fresh_cross_signature(raw, identity.master_key(), &device)?;
        }
        Ok((device, master))
    }
}

async fn refresh_user_once<T>(
    queried: &mut HashMap<OwnedUserId, Option<T>>,
    user: &OwnedUserId,
    batch_deadline: tokio::time::Instant,
    query: impl std::future::Future<Output = Option<T>>,
) {
    if queried.contains_key(user) { return; }
    let now = tokio::time::Instant::now();
    let result = if now >= batch_deadline {
        None
    } else {
        let deadline = batch_deadline.min(now + DEVICE_REFRESH_TIMEOUT);
        tokio::time::timeout_at(deadline, query).await.ok().flatten()
    };
    queried.insert(user.clone(), result);
}

/// A [`MatrixBackend`] over a logged-in `matrix_sdk::Client`. Clone-cheap.
#[derive(Clone)]
pub struct SdkMatrixBackend {
    client: Client,
    call_key_recipients: Option<CallKeyRecipients>,
}

impl SdkMatrixBackend {
    pub fn new(client: Client) -> Self {
        Self { client, call_key_recipients: None }
    }

    pub fn with_call_key_recipients(mut self, scope: Option<CallKeyRecipients>) -> Self {
        self.call_key_recipients = scope;
        self
    }

    pub fn client(&self) -> &Client {
        &self.client
    }

    /// The homeserver's `unstable_features` that are on, as `RtcClient::msc_support`
    /// takes them (spec 038 R45).
    pub async fn server_features(&self) -> Result<Vec<String>, BackendError> {
        Ok(self
            .client
            .unstable_features()
            .await
            .map_err(backend_error)?
            .into_iter()
            .map(|feature| feature.to_string())
            .collect())
    }

    fn room(&self, room_id: &str) -> Result<Room, CommandError> {
        let room_id = RoomId::parse(room_id).map_err(local_error)?;
        // The client knows the room; one it does not know is final (R3).
        self.client.get_room(&room_id).ok_or_else(|| {
            CommandError::Backend(
                BackendError::new(format!("room {room_id} not found")).permanent(),
            )
        })
    }

    fn room_for_read(&self, room_id: &str) -> Result<Room, BackendError> {
        self.room(room_id).map_err(backend_error)
    }

    /// Send a plain message-like room event, bounded like every other RTC send.
    /// `send_raw` encrypts in an encrypted room like any other message send.
    async fn send_room_message(
        &self,
        room: &Room,
        event_type: &str,
        content: &Value,
    ) -> Result<String, CommandError> {
        let response = room
            .send_raw(event_type, content)
            .with_request_config(rtc_request_config())
            .await
            .map_err(command_error)?;
        Ok(response.response.event_id.to_string())
    }
}

/// What the client's decryption metadata says, as the backend reports it.
///
/// MSC4153 asks whether the sending device is cross-signed, not whether we
/// trust its owner: an unverified *identity* still signs its own devices.
/// States that leave the device unattributable count as not cross-signed.
fn event_encryption(info: Option<&EncryptionInfo>) -> EventEncryption {
    let Some(info) = info else {
        return EventEncryption::Cleartext;
    };
    let cross_signed = !matches!(
        info.verification_state,
        VerificationState::Unverified(
            VerificationLevel::UnsignedDevice
                | VerificationLevel::None(_)
                | VerificationLevel::MismatchedSender
        )
    );
    EventEncryption::Encrypted {
        sender_device_id: info.sender_device.as_ref().map(|d| d.to_string()),
        sender_confirmed: Some(cross_signed),
    }
}

/// Whether the SDK's verdict on the sending device is still provisional: the
/// device was not in the store, or it was (an Olm message carries its own
/// device keys) but the sender's cross-signing identity was not downloaded
/// yet, so "unsigned" may only mean "not checked yet".
async fn sender_trust_pending(client: &Client, info: &EncryptionInfo) -> bool {
    match info.verification_state {
        VerificationState::Unverified(VerificationLevel::None(
            DeviceLinkProblem::MissingDevice,
        )) => true,
        VerificationState::Unverified(VerificationLevel::UnsignedDevice) => client
            .encryption()
            .get_user_identity(&info.sender)
            .await
            .ok()
            .flatten()
            .is_none(),
        _ => false,
    }
}

/// [`event_encryption`] for a device looked up after the fact.
fn device_encryption(device: &Device) -> EventEncryption {
    EventEncryption::Encrypted {
        sender_device_id: Some(device.device_id().to_string()),
        sender_confirmed: Some(device.is_verified() || device.is_cross_signed_by_owner()),
    }
}

/// The decryption information of a to-device message whose sender trust was
/// pending ([`sender_trust_pending`]), after querying the sender's keys.
///
/// A peer who joins a call right after joining the room sends its media key
/// before our client has queried its keys; reported as-is, the library reads
/// that as "not cross-signed" and refuses the key for good. A bounded fresh
/// response checked against the SDK trust state can settle it; failures remain
/// unconfirmed.
async fn resolve_to_device_encryption(client: &Client, info: &EncryptionInfo) -> EventEncryption {
    let AlgorithmInfo::OlmV1Curve25519AesSha2 {
        curve25519_public_key_base64: sender_key,
    } = &info.algorithm_info
    else {
        return event_encryption(Some(info));
    };
    let pending = async {
        let fresh = refresh_user_keys(client, &info.sender).await.ok()?;
        let devices = fresh.response.device_keys.get(&info.sender)?;
        // Bind the original Olm sender key to exactly one freshly checked device.
        let mut matches = devices.iter().filter(|(id, raw)| {
            raw.get_field::<Value>("keys").ok().flatten().and_then(|keys|
                keys.get(format!("curve25519:{id}").as_str()).and_then(Value::as_str).map(str::to_owned))
                .as_deref() == Some(sender_key.as_str())
        });
        let (id, _) = matches.next()?;
        if matches.next().is_some() || info.sender_device.as_ref().is_some_and(|expected| expected.as_str() != id.as_str()) { return None; }
        let (device, _) = fresh.confirmed_device(client, id).await.ok()?;
        Some(device_encryption(&device))
    };
    tokio::time::timeout(DEVICE_REFRESH_TIMEOUT, pending).await.ok().flatten()
        .unwrap_or(EventEncryption::Encrypted {
            sender_device_id: info.sender_device.as_ref().map(|d| d.to_string()),
            sender_confirmed: Some(false),
        })
}

#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
impl MatrixBackend for SdkMatrixBackend {
    fn own_user_id(&self) -> String {
        self.client
            .user_id()
            .map(|id| id.to_string())
            .unwrap_or_default()
    }

    fn own_device_id(&self) -> String {
        self.client
            .device_id()
            .map(|id| id.to_string())
            .unwrap_or_default()
    }

    /// `duration_ms` is the core's value, not a constant of ours: it schedules
    /// the refresh against exactly this lifetime.
    async fn send_sticky_event(
        &self,
        room_id: String,
        event_type: String,
        content: Value,
        duration_ms: u64,
    ) -> Result<String, CommandError> {
        let room = self.room(&room_id)?;
        let event_type = wire_event_type(event_type).to_string();
        let response = room
            .send_raw(&event_type, &content)
            .with_sticky_duration(Duration::from_millis(duration_ms))
            .with_request_config(rtc_request_config())
            .await
            .map_err(command_error)?;
        Ok(response.response.event_id.to_string())
    }

    async fn send_delayed_state_event(
        &self,
        room_id: String,
        event_type: String,
        state_key: String,
        content: Value,
        delay_ms: u64,
    ) -> Result<String, CommandError> {
        let room_id = RoomId::parse(&room_id).map_err(local_error)?;
        let delay = DelayParameters::Timeout {
            timeout: Duration::from_millis(delay_ms),
        };
        let raw = serde_json::value::to_raw_value(&content).map_err(local_error)?;
        // NOTE the argument order: `state_key` comes *before* `event_type`
        // here, unlike every other ruma send.
        let request = delayed_state_event::unstable::Request::new_raw(
            room_id,
            state_key,
            StateEventType::from(event_type),
            delay,
            Raw::<AnyStateEventContent>::from_json(raw),
        );
        // Returns the MSC4140 delay id, which is what restart/cancel take.
        Ok(self
            .client
            .send(request)
            // Delayed state creation has no transaction ID. A lost response
            // may already have armed a server-side event; HTTP retry would
            // allocate another one whose predecessor we cannot cancel.
            .with_request_config(rtc_request_config().disable_retry())
            .await
            .map_err(delayed_command_error)?
            .delay_id)
    }

    /// Not yet: the SDK cannot attach a sticky duration to a delayed send
    /// (matrix-org/matrix-rust-sdk#6973), and a plain delayed event would fire
    /// without replacing the sticky membership. The core then joins without a
    /// dead man's switch, on a shorter membership lifetime.
    async fn send_delayed_sticky_event(
        &self,
        _room_id: String,
        _event_type: String,
        _content: Value,
        _sticky_duration_ms: u64,
        _delay_ms: u64,
    ) -> Result<String, CommandError> {
        Err(CommandError::DelayedEventsNotSupported(
            "the SDK cannot send a delayed sticky event yet (matrix-rust-sdk#6973)".to_owned(),
        ))
    }

    async fn restart_delayed_event(
        &self,
        _room_id: String,
        delay_id: String,
    ) -> Result<(), CommandError> {
        // MSC4140's "heartbeat ping": one request, never a moment with no
        // delayed leave armed.
        let request =
            update_delayed_event::unstable_v1::Request::new(delay_id, UpdateAction::Restart);
        self.client
            .send(request)
            .with_request_config(rtc_request_config())
            .await
            .map_err(delayed_command_error)?;
        Ok(())
    }

    async fn cancel_delayed_event(
        &self,
        _room_id: String,
        delay_id: String,
    ) -> Result<(), CommandError> {
        let request =
            update_delayed_event::unstable_v1::Request::new(delay_id, UpdateAction::Cancel);
        self.client
            .send(request)
            .with_request_config(rtc_request_config())
            .await
            .map_err(delayed_command_error)?;
        Ok(())
    }

    async fn send_to_device_message(
        &self,
        recipients: Vec<ToDeviceRecipient>,
        message_type: String,
        content: Value,
    ) -> Result<Vec<ToDeviceDelivery>, CommandError> {
        // Olm-encrypted, to exactly the devices that published the memberships;
        // no `"*"` fan-out. One SDK call for the batch, which answers with the
        // devices it could not reach — the per-recipient outcome the core needs.
        let encryption = self.client.encryption();
        let mut devices = Vec::with_capacity(recipients.len());
        let mut unknown = Vec::new();
        // One paired refresh (SDK application plus observed response) per user per batch.
        let mut queried: HashMap<OwnedUserId, Option<FreshUserKeys>> = HashMap::new();
        let batch_deadline = tokio::time::Instant::now() + DEVICE_REFRESH_BATCH_TIMEOUT;

        for recipient in &recipients {
            if self.call_key_recipients.as_ref().is_some_and(|scope| !scope.allows(&recipient.user_id, &recipient.device_id)) {
                // Not permanent: the worker may not have admitted the peer yet.
                unknown.push(ToDeviceDelivery::failed(recipient.clone(), "call recipient not pinned"));
                continue;
            }
            let user = match UserId::parse(&recipient.user_id) {
                Ok(user) => user,
                Err(error) => {
                    unknown.push(ToDeviceDelivery::failed(
                        recipient.clone(),
                        format!("unparseable user id: {error}"),
                    ));
                    continue;
                }
            };
            let device_id = <&DeviceId>::from(recipient.device_id.as_str());

            refresh_user_once(&mut queried, &user, batch_deadline, async {
                refresh_user_keys(&self.client, &user).await.ok()
            }).await;
            let Some(fresh) = queried.get(&user).and_then(Option::as_ref) else {
                unknown.push(ToDeviceDelivery::failed(recipient.clone(), "device key refresh failed or timed out"));
                continue;
            };
            // Store reads are also part of the existing absolute batch budget.
            if tokio::time::Instant::now() >= batch_deadline {
                unknown.push(ToDeviceDelivery::failed(recipient.clone(), "device key batch deadline expired"));
                continue;
            }
            match tokio::time::timeout_at(batch_deadline, fresh.confirmed_device(&self.client, device_id)).await {
                Ok(Ok((device, master))) if tokio::time::Instant::now() < batch_deadline => {
                    if let Some(scope) = self.call_key_recipients.as_ref().filter(|scope| scope.is_group()) {
                        let keys = match (device.ed25519_key(), device.curve25519_key()) {
                            (Some(signing), Some(identity)) => Some(CallRecipientKeys {
                                signing: signing.to_base64(), identity: identity.to_base64(), master,
                            }),
                            _ => None,
                        };
                        if !keys.as_ref().is_some_and(|keys| scope.allows_keys(
                            &recipient.user_id, &recipient.device_id, keys)) {
                            unknown.push(ToDeviceDelivery::failed(recipient.clone(), "group recipient keys no longer match admission"));
                            continue;
                        }
                    }
                    devices.push(device);
                },
                _ => unknown.push(ToDeviceDelivery::failed(recipient.clone(), "fresh device validation failed")),
            }
        }

        // A lookup can await network I/O while the call is ending. Recheck
        // immediately before dispatch, recording revoked recipients as failed.
        if let Some(scope) = &self.call_key_recipients {
            devices.retain(|device| {
                let allowed = scope.allows(device.user_id().as_str(), device.device_id().as_str());
                if !allowed {
                    unknown.push(ToDeviceDelivery::failed(
                        ToDeviceRecipient::new(device.user_id().as_str(), device.device_id().as_str()),
                        "call recipient no longer authorised",
                    ));
                }
                allowed
            });
        }
        if devices.is_empty() {
            return Ok(unknown);
        }

        let raw: Raw<AnyToDeviceEventContent> =
            Raw::new(&content).map_err(local_error)?.cast_unchecked();
        let failures = encryption
            .encrypt_and_send_raw_to_device(
                devices.iter().collect(),
                &message_type,
                raw,
                // MSC4153: refuse to hand keys to unverified identities.
                CollectStrategy::IdentityBasedStrategy,
            )
            .await
            .map_err(command_error)?;

        let mut deliveries = unknown;
        for device in &devices {
            let recipient =
                ToDeviceRecipient::new(device.user_id().as_str(), device.device_id().as_str());
            let failed = failures
                .iter()
                .any(|(user, dev)| user == device.user_id() && dev == device.device_id());
            deliveries.push(if failed {
                ToDeviceDelivery::failed(recipient, "the homeserver did not accept the message")
            } else {
                ToDeviceDelivery::sent(recipient)
            });
        }

        if !failures.is_empty() {
            log::warn!(
                "to-device {message_type}: {} of {} device(s) did not receive the key",
                failures.len(),
                devices.len(),
            );
        }
        Ok(deliveries)
    }

    async fn send_state_event(
        &self,
        room_id: String,
        event_type: String,
        state_key: String,
        content: Value,
    ) -> Result<String, CommandError> {
        let room = self.room(&room_id)?;
        // Bounded by `rtc_request_config`: the pre-sticky membership comes
        // through here, as time-critical as the sticky one. The type is
        // normalised through ruma, which registers `m.rtc.slot` as
        // an alias of the MSC4143 id.
        let raw = serde_json::value::to_raw_value(&content).map_err(local_error)?;
        let request = send_state_event::v3::Request::new_raw(
            room.room_id().to_owned(),
            StateEventType::from(event_type),
            state_key,
            Raw::<AnyStateEventContent>::from_json(raw),
        );
        let response = self
            .client
            .send(request)
            .with_request_config(rtc_request_config())
            .await
            .map_err(command_error)?;
        Ok(response.event_id.to_string())
    }

    async fn send_room_event(
        &self,
        room_id: String,
        event_type: String,
        content: Value,
    ) -> Result<String, CommandError> {
        let room = self.room(&room_id)?;
        self.send_room_message(&room, &event_type, &content).await
    }

    async fn redact_event(
        &self,
        room_id: String,
        event_id: String,
        reason: Option<String>,
    ) -> Result<(), CommandError> {
        let room = self.room(&room_id)?;
        let event_id = EventId::parse(&event_id).map_err(local_error)?;
        room.redact(&event_id, reason.as_deref(), None)
            .await
            .map_err(command_error)?;
        Ok(())
    }

    async fn subscribe_room(
        &self,
        room_id: String,
        subjects: RoomSubjects,
        sink: Arc<dyn RoomSink>,
    ) -> Result<Arc<dyn Subscription>, BackendError> {
        let room = self.room_for_read(&room_id)?;

        let mut guards = Vec::new();
        if !subjects.timeline_event_types.is_empty() {
            let handle = register_timeline_receiver(
                &room,
                sink.clone(),
                subjects.timeline_event_types.clone(),
            );
            guards.push(self.client.event_handler_drop_guard(handle));
        }

        let task = tokio::spawn(feed_room_subjects(room, subjects, sink));
        Ok(Arc::new(SdkSubscription::new(task, guards)))
    }

    async fn subscribe_to_device(
        &self,
        event_types: Vec<String>,
        sink: Arc<dyn ToDeviceSink>,
    ) -> Result<Arc<dyn Subscription>, BackendError> {
        // Raw rather than typed: ruma has no event for the MSC4143 rewrite nor
        // for the legacy key type, and a typed handler silently never fires
        // when the content does not match its model.
        let event_types = Arc::new(event_types);
        let client = self.client.clone();
        let handle = self.client.add_event_handler(
            move |event: Raw<AnyToDeviceEvent>, encryption_info: Option<EncryptionInfo>| {
                let sink = sink.clone();
                let event_types = event_types.clone();
                let client = client.clone();
                async move {
                    let Some(event_type) = event.get_field::<String>("type").ok().flatten() else {
                        return;
                    };
                    if !event_types.contains(&event_type) {
                        return;
                    }
                    let sender = encryption_info
                        .as_ref()
                        .map(|info| info.sender.to_string())
                        .or_else(|| event.get_field::<String>("sender").ok().flatten());
                    let (Some(sender), Ok(Some(content))) =
                        (sender, event.get_field::<Value>("content"))
                    else {
                        log::warn!(
                            "ignoring a {event_type} to-device message with no sender or content"
                        );
                        return;
                    };
                    log::debug!(
                        "to-device {event_type} from {sender}: {:?}",
                        encryption_info
                            .as_ref()
                            .map(|info| (&info.verification_state, &info.sender_device))
                    );
                    let deliver = move |encryption| {
                        sink.on_to_device_message(ToDeviceMessageIn {
                            sender,
                            event_type,
                            content,
                            encryption,
                        })
                    };
                    match encryption_info {
                        // A keys query inside the handler would stall the sync
                        // loop; resolve it on the side.
                        Some(info) if sender_trust_pending(&client, &info).await => {
                            tokio::spawn(async move {
                                deliver(resolve_to_device_encryption(&client, &info).await)
                            });
                        }
                        info => deliver(event_encryption(info.as_ref())),
                    }
                }
            },
        );
        let guard = self.client.event_handler_drop_guard(handle);
        Ok(Arc::new(SdkSubscription::new(
            tokio::spawn(std::future::pending()),
            vec![guard],
        )))
    }

    async fn relations(
        &self,
        room_id: String,
        event_id: String,
        rel_type: String,
        event_type: String,
    ) -> Result<Vec<EventIn>, BackendError> {
        let room = self.room_for_read(&room_id)?;
        let target = OwnedEventId::try_from(event_id.as_str()).map_err(backend_error)?;
        let options = RelationsOptions {
            limit: Some(UInt::from(RELATIONS_LOOKUP_LIMIT)),
            include_relations: IncludeRelations::RelationsOfTypeAndEventType(
                RelationType::from(rel_type.as_str()),
                TimelineEventType::from(event_type.as_str()),
            ),
            ..RelationsOptions::default()
        };
        let wanted = std::slice::from_ref(&event_type);
        // The pinned SDK exposes next_batch_token; use the same relation
        // filter and direction for every subsequent request.
        let events = collect_relation_pages(|from| {
            let room = room.clone();
            let target = target.clone();
            let mut options = options.clone();
            options.from = from;
            async move {
                let response = room.relations(target, options).await.map_err(backend_error)?;
                Ok((response.chunk, response.next_batch_token))
            }
        }).await?;
        let mut seen = HashSet::new();
        Ok(events
            .iter()
            .filter_map(|event| {
                match timeline_ingest_from_raw(
                    event.raw(),
                    event.encryption_info().map(Arc::as_ref),
                    wanted,
                ) {
                    Some(TimelineIngest::Event(event)) if seen.insert(event.event_id.clone()) => Some(event),
                    _ => None,
                }
            })
            .collect())
    }

    async fn openid_token(&self) -> Result<OpenIdToken, BackendError> {
        let response = self
            .client
            .account()
            .request_openid_token()
            .await
            .map_err(backend_error)?;
        Ok(OpenIdToken {
            access_token: response.access_token,
            token_type: response.token_type.to_string(),
            matrix_server_name: response.matrix_server_name.to_string(),
            expires_in: response.expires_in.as_secs(),
        })
    }

    /// The homeserver's MSC4195 `get_token`, under its unstable path (the only
    /// one homeservers serve so far).
    async fn livekit_token(&self, request: &LiveKitTokenRequest) -> Result<String, BackendError> {
        use ruma::api::client::rtc::livekit::get_token::v1 as get_token;

        let room_id = RoomId::parse(&request.room_id).map_err(|e| backend_error(e).permanent())?;
        let mut sdk_request = get_token::Request::new(
            request.url.clone(),
            room_id,
            request.slot_id.clone(),
            request.member_id.clone(),
        );
        sdk_request.server_name = request
            .server_name
            .as_deref()
            .map(ruma::ServerName::parse)
            .transpose()
            .map_err(|e| backend_error(e).permanent())?;
        let response = self
            .client
            .send(sdk_request)
            .await
            .map_err(|error| sdk_backend_error(&error))?;
        Ok(response.jwt)
    }

    /// The array exactly as the homeserver serves it (see [`rtc_requests`]):
    /// the v1 endpoint, else the unstable one, else the SDK's discovery, which
    /// falls back to the well-known `rtc_foci`. Nothing discovered is an empty
    /// array ("none advertised"); a failed request stays an error rather than
    /// reading as "none".
    async fn rtc_transports(&self) -> Result<Value, BackendError> {
        if let Some(transports) = rtc_requests::transports(&self.client).await? {
            return Ok(transports);
        }
        let transports = self
            .client
            .discover_rtc_transports()
            .await
            .map_err(backend_error)?
            .unwrap_or_default();
        serde_json::to_value(transports).map_err(backend_error)
    }
}

/// Bounded call-only relation lookup. Return no partial result: the room
/// marks a membership's relations fetched only after a successful result.
const RELATIONS_LOOKUP_LIMIT: u32 = 50;
const RELATIONS_MAX_PAGES: usize = 8;
const RELATIONS_TIMEOUT: Duration = Duration::from_secs(10);

async fn collect_relation_pages<T, F, Fut>(mut fetch: F) -> Result<Vec<T>, BackendError>
where
    F: FnMut(Option<String>) -> Fut,
    Fut: std::future::Future<Output = Result<(Vec<T>, Option<String>), BackendError>>,
{
    tokio::time::timeout(RELATIONS_TIMEOUT, async {
        let mut from = None;
        let mut tokens = HashSet::new();
        let mut events = Vec::new();
        for _ in 0..RELATIONS_MAX_PAGES {
            let (chunk, next) = fetch(from.take()).await?;
            if chunk.len() > RELATIONS_LOOKUP_LIMIT as usize {
                return Err(BackendError::new("RTC relations response exceeded page limit"));
            }
            events.extend(chunk);
            match next {
                None => return Ok(events),
                Some(token) if !token.is_empty() && token.len() <= 4096 && tokens.insert(token.clone()) => {
                    from = Some(token);
                }
                Some(_) => return Err(BackendError::new("invalid or repeated RTC relations pagination token")),
            }
        }
        Err(BackendError::new("RTC relations lookup exceeded page budget"))
    }).await.map_err(|_| BackendError::new("RTC relations lookup timed out"))?
}

/// A subscription that aborts its task and drops its handler guards on cancel.
struct SdkSubscription {
    live: StdMutex<Option<(JoinHandle<()>, Vec<EventHandlerDropGuard>)>>,
}

impl SdkSubscription {
    fn new(task: JoinHandle<()>, guards: Vec<EventHandlerDropGuard>) -> Self {
        Self {
            live: StdMutex::new(Some((task, guards))),
        }
    }
}

impl Subscription for SdkSubscription {
    fn cancel(&self) {
        if let Some((task, guards)) = self.live.lock().ok().and_then(|mut live| live.take()) {
            task.abort();
            drop(guards);
        }
    }
}

impl Drop for SdkSubscription {
    fn drop(&mut self) {
        self.cancel();
    }
}

/// Whether a wake source's `recv()` means "keep going": a lagged receiver
/// still means something changed.
fn keep_going<T>(result: Result<T, RecvError>) -> bool {
    matches!(result, Ok(_) | Err(RecvError::Lagged(_)))
}

/// Delivers the room's subjects: the current sets first, then again on every
/// sync that touches the room. Each delivery is the complete set; the core
/// replaces rather than merges. A subject whose read failed is skipped for
/// that round and retried, so a failed slot fetch never reads as "no slots".
async fn feed_room_subjects(room: Room, subjects: RoomSubjects, sink: Arc<dyn RoomSink>) {
    let room_id = room.room_id().to_string();
    log::info!("[{room_id}] room subscription started");

    let mut sticky = room.sticky_events().subscribe();
    let mut updates = room.subscribe_to_updates();
    // Ghost clients use sliding sync and may not subscribe to every portal's
    // legacy member state. Poll authoritative state while a legacy room is
    // attached, even when that room produces no sync wakeup. This is not a
    // global background poll: dropping the RTC subscription stops it.
    let legacy_membership = subjects.state_event_types.iter().any(|name|
        name == "org.matrix.msc3401.call.member" || name == "m.call.member");

    loop {
        let all_read = bounded_room_snapshot(emit_room_subjects(&room, &subjects, &sink)).await;
        if !all_read {
            sink.on_room_snapshot_failed();
            log::debug!("[{room_id}] RTC snapshot incomplete; retaining last complete snapshot but invalidating readiness and retrying");
        }
        if retry_incomplete_snapshot(all_read).await {
            // The failed read did not publish a snapshot. Retry after a real
            // cooldown even if sync wakes keep arriving or legacy polling is
            // enabled. The next read observes the current authoritative state.
            continue;
        }

        let woken = tokio::select! {
            result = sticky.recv() => keep_going(result),
            result = updates.recv() => keep_going(result),
            _ = tokio::time::sleep(Duration::from_secs(1)), if legacy_membership => true,
        };
        if !woken {
            break;
        }
        tokio::time::sleep(WAKE_DEBOUNCE).await;
    }

    log::info!("[{room_id}] room subscription stopped");
}

/// One delivery of every subject, in the order the feeder wants them:
/// encryption, state, members, sticky. Returns whether every read succeeded.
async fn emit_room_subjects(
    room: &Room,
    subjects: &RoomSubjects,
    sink: &Arc<dyn RoomSink>,
) -> bool {
    let room_id = room.room_id().to_string();
    // Gather every required subject before delivering any. A failed state
    // fetch must not open the feeder's gate with a falsely empty roster.
    let encrypted = match room.latest_encryption_state().await {
        Ok(state) => state.is_encrypted(),
        Err(error) => {
            log::warn!("[{room_id}] failed to read room encryption state: {error}");
            return false;
        }
    };
    let fetched: Vec<&String> = subjects.state_event_types.iter().collect();
    let snapshot = if fetched.is_empty() {
        Vec::new()
    } else {
        let Some(events) = state_snapshot(room, &fetched).await else {
            return false;
        };
        fetched.into_iter().map(|event_type| {
            let of_type = events.iter()
                .filter(|event| &event.event_type == event_type)
                .cloned().collect();
            (event_type.clone(), of_type)
        }).collect()
    };
    let members = match room.members(RoomMemberships::JOIN).await {
        Ok(members) => members.into_iter()
            // Bridge bots are service members, not human participants.
            .filter(|member| member.user_id() == room.own_user_id() || !member.is_service_member())
            .map(|member| member.user_id().to_string())
            .collect::<Vec<_>>(),
        Err(error) => {
            log::warn!("[{room_id}] failed to read room members: {error}");
            return false;
        }
    };
    if !members.iter().any(|id| id == room.own_user_id().as_str()) {
        log::warn!("[{room_id}] joined-members snapshot has not loaded our own user");
        return false;
    }
    let Some(sticky) = sticky_snapshot(room) else {
        return false;
    };
    sink.on_room_snapshot(encrypted, snapshot, members, sticky);
    true
}

/// RTC membership sticky events must not silently disappear
/// from an authoritative snapshot merely because their envelope is malformed.
fn required_sticky_type(event_type: &str) -> bool {
    matches!(event_type, "m.rtc.member" | "org.matrix.msc4143.rtc.member")
}

fn sticky_fields<T>(raw: &Raw<T>, required: bool) -> Result<(Value, u64), &'static str> {
    let content: Value = raw.get_field("content").ok().flatten()
        .filter(|value: &Value| !required || value.is_object())
        .ok_or("invalid sticky content")?;
    let timestamp = raw.get_field::<u64>("origin_server_ts").ok().flatten();
    if required && timestamp.is_none() {
        return Err("missing sticky origin timestamp");
    }
    Ok((content, timestamp.unwrap_or(0)))
}

/// The room's live sticky events, raw. The sticky map only files plaintext
/// and successfully decrypted events, so the presence of decryption metadata
/// is exactly whether the event arrived encrypted.
fn sticky_snapshot(room: &Room) -> Option<Vec<EventIn>> {
    let room_id = room.room_id().to_string();
    let mut events = Vec::new();
    for entry in room.sticky_events().live() {
        let event_type = entry.key.event_type.to_string();
        let required = required_sticky_type(&event_type);
        let (content, origin_server_ts) = match sticky_fields(&entry.raw(), required) {
            Ok(fields) => fields,
            Err(reason) if required => {
                log::warn!("[{room_id}] incomplete RTC sticky snapshot: {event_type}: {reason}");
                return None;
            }
            Err(reason) => {
                log::debug!("[{room_id}] ignoring unrelated {event_type} sticky: {reason}");
                continue;
            }
        };
        events.push(EventIn {
            event_id: entry.event_id.to_string(),
            sender: entry.key.sender.to_string(),
            event_type,
            state_key: None,
            origin_server_ts,
            content,
            encryption: event_encryption(entry.encryption_info().map(Arc::as_ref)),
        });
    }
    Some(events)
}

/// The room's state events of `event_types` (RTC slot and legacy membership
/// spellings), or `None` when it could not be read.
///
/// Ask the homeserver (`GET /rooms/{id}/state`): the sliding-sync cache is
/// not guaranteed to hold every requested state type for a ghost's room.
/// A failed fetch is `None`, never an empty authoritative snapshot.
async fn state_snapshot(room: &Room, event_types: &[&String]) -> Option<Vec<EventIn>> {
    let room_id = room.room_id().to_string();
    let request = get_state_events::v3::Request::new(room.room_id().to_owned());
    let response = match room.client().send(request).await {
        Ok(response) => response,
        Err(error) => {
            log::warn!("[{room_id}] failed to fetch room state: {error}");
            #[cfg(not(target_arch = "wasm32"))]
            eprintln!("RTC membership state request failed for {room_id}; retrying");
            return None;
        }
    };

    let mut events = Vec::new();
    let mut keys = HashSet::new();
    for raw in response.room_state {
        match state_event_from_raw(&raw, event_types) {
            Ok(Some(event)) => {
                // State is a map of (type, state_key). Conflicting entries
                // are not an authoritative roster, regardless of their order.
                if !keys.insert((event.event_type.clone(), event.state_key.clone())) {
                    log::warn!("[{room_id}] duplicate RTC state key in snapshot");
                    return None;
                }
                events.push(event);
            }
            Ok(None) => {},
            Err(reason) => {
                log::warn!("[{room_id}] incomplete RTC state snapshot: {reason}");
                return None;
            }
        }
    }

    log::debug!(
        "[{room_id}] room state fetched: {} event(s) of {event_types:?}",
        events.len(),
    );
    Some(events)
}

/// A malformed requested state event invalidates the complete snapshot. In
/// particular, do not interpret an undecodable membership as a withdrawal.
fn state_event_from_raw<T>(raw: &Raw<T>, event_types: &[&String]) -> Result<Option<EventIn>, &'static str> {
    let event_type: String = raw.get_field("type").ok().flatten().ok_or("missing state event type")?;
    if !event_types.iter().any(|wanted| **wanted == event_type) {
        return Ok(None);
    }
    let event_id: String = raw.get_field("event_id").ok().flatten().filter(|id: &String| !id.is_empty()).ok_or("missing event ID")?;
    let sender: String = raw.get_field("sender").ok().flatten().filter(|id: &String| !id.is_empty()).ok_or("missing sender")?;
    let state_key: String = raw.get_field("state_key").ok().flatten().ok_or("missing state key")?;
    let origin_server_ts: u64 = raw.get_field("origin_server_ts").ok().flatten().ok_or("missing origin timestamp")?;
    let content: Value = raw.get_field("content").ok().flatten().filter(|value: &Value| value.is_object()).ok_or("invalid state content")?;
    Ok(Some(EventIn {
        event_id, sender, event_type, state_key: Some(state_key), origin_server_ts, content,
        encryption: EventEncryption::Cleartext,
    }))
}

/// A timeline event as the feeder takes it, or a redaction.
#[derive(Clone, Debug)]
pub enum TimelineIngest {
    Event(EventIn),
    /// A redaction, by the id of the event it redacts.
    Redacted {
        event_id: String,
    },
}

/// A redaction, or an event whose type is in `event_types`; `None` otherwise.
///
/// Generic over the `Raw` payload because the SDK hands the same JSON out under
/// different type parameters. The redaction target is read from both places
/// the room versions put it (`redacts` before v11, `content.redacts` from v11).
pub fn timeline_ingest_from_raw<T>(
    raw: &Raw<T>,
    encryption_info: Option<&EncryptionInfo>,
    event_types: &[String],
) -> Option<TimelineIngest> {
    let event_type: String = raw.get_field("type").ok().flatten()?;
    match event_type.as_str() {
        "m.room.redaction" => {
            let redacts: Option<String> = raw.get_field("redacts").ok().flatten().or_else(|| {
                raw.get_field::<Value>("content")
                    .ok()
                    .flatten()
                    .and_then(|content| content.get("redacts")?.as_str().map(str::to_owned))
            });
            redacts.map(|event_id| TimelineIngest::Redacted { event_id })
        }
        _ if event_types.contains(&event_type) => Some(TimelineIngest::Event(EventIn {
            event_id: raw.get_field("event_id").ok().flatten()?,
            sender: raw.get_field("sender").ok().flatten()?,
            event_type,
            state_key: None,
            origin_server_ts: raw
                .get_field("origin_server_ts")
                .ok()
                .flatten()
                .unwrap_or(0),
            content: raw
                .get_field("content")
                .ok()
                .flatten()
                .unwrap_or(Value::Null),
            encryption: event_encryption(encryption_info),
        })),
        _ => None,
    }
}

/// Forwards redactions and the events of `event_types` to the sink. The
/// handler runs on the sync task; the sink only enqueues.
fn register_timeline_receiver(
    room: &Room,
    sink: Arc<dyn RoomSink>,
    event_types: Vec<String>,
) -> matrix_sdk::event_handler::EventHandlerHandle {
    let event_types = Arc::new(event_types);
    room.add_event_handler(
        move |event: Raw<AnySyncMessageLikeEvent>, encryption_info: Option<EncryptionInfo>| {
            let sink = sink.clone();
            let event_types = event_types.clone();
            async move {
                match timeline_ingest_from_raw(&event, encryption_info.as_ref(), &event_types) {
                    Some(TimelineIngest::Event(event)) => sink.on_timeline_events(vec![event]),
                    Some(TimelineIngest::Redacted { event_id }) => sink.on_redaction(event_id),
                    None => {}
                }
            }
        },
    )
}

#[cfg(test)]
mod tests {
    use matrix_sdk::ruma::events::AnySyncTimelineEvent;

    use super::*;

    fn wanted() -> Vec<String> {
        vec![
            "io.element.call.reaction".to_owned(),
            "m.reaction".to_owned(),
        ]
    }

    fn raw_event(json: &str) -> Raw<AnySyncTimelineEvent> {
        Raw::from_json_string(json.to_owned()).expect("valid json")
    }

    #[tokio::test]
    async fn relations_follow_next_tokens_even_after_empty_page() {
        let mut requests = Vec::new();
        let result = collect_relation_pages(|from| {
            requests.push(from.clone());
            std::future::ready(Ok(match from.as_deref() {
                None => (vec![1], Some("next".into())),
                Some("next") => (vec![], Some("last".into())),
                Some("last") => (vec![2], None),
                _ => panic!("unexpected cursor"),
            }))
        }).await.unwrap();
        assert_eq!(result, vec![1, 2]);
        assert_eq!(requests, vec![None, Some("next".into()), Some("last".into())]);
    }

    #[tokio::test]
    async fn relations_reject_partial_results_on_error_cycle_or_budget() {
        let mut count = 0;
        assert!(collect_relation_pages(|_| {
            count += 1;
            std::future::ready(Ok((vec![count], Some("same".into()))))
        }).await.is_err());
        assert_eq!(count, 2);
        count = 0;
        assert!(collect_relation_pages(|_| {
            count += 1;
            std::future::ready(Ok((vec![count], Some(count.to_string()))))
        }).await.is_err());
        assert_eq!(count, RELATIONS_MAX_PAGES);
        assert!(collect_relation_pages(|from| {
            std::future::ready(if from.is_none() {
                Ok((vec![1], Some("next".into())))
            } else {
                Err(BackendError::new("second page failed"))
            })
        }).await.is_err());
        assert!(collect_relation_pages(|_| std::future::ready(Ok((
            vec![0; RELATIONS_LOOKUP_LIMIT as usize + 1], None,
        )))).await.is_err());
    }

    #[tokio::test(start_paused = true)]
    async fn relations_have_one_total_deadline() {
        let start = tokio::time::Instant::now();
        let result = collect_relation_pages(|from| async move {
            if from.is_none() {
                tokio::time::sleep(Duration::from_secs(6)).await;
                Ok((vec![1], Some("next".into())))
            } else {
                tokio::time::sleep(Duration::from_secs(6)).await;
                Ok((vec![2], None))
            }
        }).await;
        assert!(result.is_err());
        assert_eq!(start.elapsed(), RELATIONS_TIMEOUT);
    }

    #[tokio::test(start_paused = true)]
    async fn device_refresh_reuses_batch_result_including_failure_and_timeout() {
        let mut queried = HashMap::new();
        let batch_deadline = tokio::time::Instant::now() + DEVICE_REFRESH_BATCH_TIMEOUT;
        let alice = UserId::parse("@alice:example.org").unwrap();
        let bob = UserId::parse("@bob:example.org").unwrap();
        let carol = UserId::parse("@carol:example.org").unwrap();
        refresh_user_once(&mut queried, &alice, batch_deadline, async { Some(1) }).await;
        refresh_user_once(&mut queried, &alice, batch_deadline, async { panic!("duplicate query") }).await;
        refresh_user_once(&mut queried, &bob, batch_deadline, async { None }).await;
        refresh_user_once(&mut queried, &bob, batch_deadline, async { panic!("failed query retried") }).await;
        let start = tokio::time::Instant::now();
        refresh_user_once(&mut queried, &carol, batch_deadline, std::future::pending()).await;
        assert_eq!(start.elapsed(), DEVICE_REFRESH_TIMEOUT);
        refresh_user_once(&mut queried, &carol, batch_deadline, async { panic!("timed-out query retried") }).await;
        let dan = UserId::parse("@dan:example.org").unwrap();
        refresh_user_once(&mut queried, &dan, batch_deadline, std::future::pending()).await;
        assert_eq!(tokio::time::Instant::now(), batch_deadline);
        let eve = UserId::parse("@eve:example.org").unwrap();
        refresh_user_once(&mut queried, &eve, batch_deadline, async { panic!("expired batch queried") }).await;
        assert_eq!(queried.get(&alice), Some(&Some(1)));
        for user in [&bob, &carol, &dan, &eve] { assert_eq!(queried.get(user), Some(&None)); }
    }

    #[test]
    fn freshness_compares_signed_device_data_not_display_name() {
        let original = serde_json::json!({"user_id":"@a:example.org", "device_id":"A", "keys":{"ed25519:A":"key"}, "signatures":{"@a:example.org":{"ed25519:A":"signature"}}});
        let mut changed = original.clone();
        changed["unsigned"] = serde_json::json!({"device_display_name":"New"});
        assert!(same_signed_device(changed.clone(), original.clone()));
        for (field, value) in [("user_id", serde_json::json!("@b:example.org")), ("device_id", serde_json::json!("B")), ("keys", serde_json::json!({})), ("signatures", serde_json::json!({}))] {
            let mut changed = changed.clone(); changed[field] = value;
            assert!(!same_signed_device(changed, original.clone()));
        }
        assert!(!same_signed_device(Value::Null, original));
    }

    #[test]
    fn freshness_rejects_partial_or_misattributed_queries() {
        let user = UserId::parse("@a:example.org").unwrap();
        let mut response = get_keys::v3::Response::new();
        assert!(require_complete_key_response(&response, &user).is_err());
        response.device_keys.insert(user.clone(), Default::default());
        assert!(require_complete_key_response(&response, &user).is_ok());
        response.failures.insert("example.org".into(), serde_json::json!({"errcode":"M_UNAVAILABLE"}));
        assert!(require_complete_key_response(&response, &user).is_err());
        response.failures.clear();
        let malformed = Raw::from_json_string(r#"{"user_id":"@b:example.org","usage":["master"],"keys":{"ed25519:abc":"abc"}}"#.into()).unwrap();
        response.master_keys.insert(user.clone(), malformed);
        assert!(require_complete_key_response(&response, &user).is_err());
        let wrong_usage = Raw::from_json_string(r#"{"user_id":"@a:example.org","usage":["self_signing"],"keys":{"ed25519:abc":"abc"}}"#.into()).unwrap();
        response.master_keys.insert(user.clone(), wrong_usage);
        assert!(require_complete_key_response(&response, &user).is_err());
        let good = Raw::from_json_string(r#"{"user_id":"@a:example.org","usage":["master"],"keys":{"ed25519:abc":"abc"}}"#.into()).unwrap();
        response.master_keys.insert(user.clone(), good);
        assert_eq!(observed_master(response.master_keys.get(&user), &user), Ok(Some("abc".to_owned())));
    }

    #[tokio::test(start_paused = true)]
    async fn stalled_room_snapshot_is_cancelled_at_deadline() {
        struct DropFlag(Arc<std::sync::atomic::AtomicBool>);
        impl Drop for DropFlag {
            fn drop(&mut self) {
                self.0.store(true, std::sync::atomic::Ordering::SeqCst);
            }
        }
        let cancelled = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let guard = DropFlag(cancelled.clone());
        let start = tokio::time::Instant::now();
        assert!(!bounded_room_snapshot(async move {
            let _guard = guard;
            std::future::pending::<bool>().await
        }).await);
        assert_eq!(start.elapsed(), ROOM_SNAPSHOT_TIMEOUT);
        assert!(cancelled.load(std::sync::atomic::Ordering::SeqCst));
        assert!(bounded_room_snapshot(async { true }).await);
        assert!(!bounded_room_snapshot(async { false }).await);
    }

    #[tokio::test(start_paused = true)]
    async fn incomplete_snapshot_has_mandatory_cooldown() {
        let start = tokio::time::Instant::now();
        assert!(!retry_incomplete_snapshot(true).await);
        assert_eq!(start.elapsed(), Duration::ZERO);
        assert!(retry_incomplete_snapshot(false).await);
        assert_eq!(start.elapsed(), RETRY_AFTER);
        assert!(retry_incomplete_snapshot(false).await);
        assert_eq!(start.elapsed(), RETRY_AFTER + RETRY_AFTER);
    }

    #[test]
    fn requested_state_distinguishes_malformed_events_from_empty_leave_content() {
        let kind = "m.call.member".to_owned();
        let types = [&kind];
        let valid = raw_event(r#"{"type":"m.call.member","event_id":"$leave","sender":"@bob:example.org","state_key":"","origin_server_ts":123,"content":{}}"#);
        let event = state_event_from_raw(&valid, &types).unwrap().unwrap();
        assert_eq!(event.state_key.as_deref(), Some(""));
        assert_eq!(event.content, serde_json::json!({}));
        for invalid in [
            r#"{"type":"m.call.member","sender":"@bob:example.org","state_key":"","origin_server_ts":123,"content":{}}"#,
            r#"{"type":"m.call.member","event_id":"$e","sender":"@bob:example.org","origin_server_ts":123,"content":{}}"#,
            r#"{"type":"m.call.member","event_id":"$e","sender":"@bob:example.org","state_key":"","content":{}}"#,
            r#"{"type":"m.call.member","event_id":"$e","sender":"@bob:example.org","state_key":"","origin_server_ts":123,"content":null}"#,
            r#"{"content":{}}"#,
        ] {
            assert!(state_event_from_raw(&raw_event(invalid), &types).is_err());
        }
        assert!(state_event_from_raw(&raw_event(r#"{"type":"m.room.name"}"#), &types).unwrap().is_none());
    }

    #[test]
    fn required_sticky_envelopes_fail_closed_without_rejecting_empty_objects() {
        assert!(required_sticky_type("m.rtc.member"));
        assert!(required_sticky_type("org.matrix.msc4143.rtc.member"));
        assert!(!required_sticky_type("m.room.message"));
        let valid = raw_event(r#"{"content":{},"origin_server_ts":123}"#);
        assert_eq!(sticky_fields(&valid, true).unwrap(), (serde_json::json!({}), 123));
        for malformed in [r#"{"origin_server_ts":123}"#, r#"{"content":null,"origin_server_ts":123}"#,
            r#"{"content":[],"origin_server_ts":123}"#, r#"{"content":{}}"#,
            r#"{"content":{},"origin_server_ts":"123"}"#] {
            assert!(sticky_fields(&raw_event(malformed), true).is_err());
        }
        assert_eq!(sticky_fields(&raw_event(r#"{"content":{}}"#), false).unwrap().1, 0);
    }

    #[test]
    fn a_refused_recipient_collection_is_permanent_and_other_failures_are_not() {
        let refused =
            matrix_sdk::Error::OlmError(Box::new(OlmError::SessionRecipientCollectionError(
                matrix_sdk_base::crypto::SessionRecipientCollectionError::SendingFromUnverifiedDevice,
            )));
        assert!(command_error(refused).is_permanent());

        assert!(!command_error(matrix_sdk::Error::InsufficientData).is_permanent());
    }

    #[test]
    fn a_reaction_event_is_read_into_the_inbound_dto() {
        let raw = raw_event(
            r#"{
                "type": "io.element.call.reaction",
                "event_id": "$reaction",
                "sender": "@bob:example.org",
                "origin_server_ts": 1234,
                "content": {
                    "m.relates_to": { "rel_type": "m.reference", "event_id": "$member" },
                    "emoji": "👏",
                    "name": "clapping"
                }
            }"#,
        );
        let Some(TimelineIngest::Event(event)) = timeline_ingest_from_raw(&raw, None, &wanted())
        else {
            panic!("a reaction must be read as an event");
        };
        assert_eq!(event.event_id, "$reaction");
        assert_eq!(event.sender, "@bob:example.org");
        assert_eq!(event.event_type, "io.element.call.reaction");
        assert_eq!(event.origin_server_ts, 1234);
        assert_eq!(event.encryption, EventEncryption::Cleartext);
        assert_eq!(event.content["emoji"], "👏");
        assert_eq!(event.content["m.relates_to"]["event_id"], "$member");
    }

    #[test]
    fn a_redaction_names_its_target_in_either_room_version_shape() {
        let pre_v11 = raw_event(
            r#"{
                "type": "m.room.redaction",
                "event_id": "$redaction",
                "sender": "@bob:example.org",
                "origin_server_ts": 6,
                "redacts": "$hand",
                "content": {}
            }"#,
        );
        assert!(matches!(
            timeline_ingest_from_raw(&pre_v11, None, &wanted()),
            Some(TimelineIngest::Redacted { event_id }) if event_id == "$hand"
        ));

        let v11 = raw_event(
            r#"{
                "type": "m.room.redaction",
                "event_id": "$redaction",
                "sender": "@bob:example.org",
                "origin_server_ts": 6,
                "content": { "redacts": "$hand" }
            }"#,
        );
        assert!(matches!(
            timeline_ingest_from_raw(&v11, None, &wanted()),
            Some(TimelineIngest::Redacted { event_id }) if event_id == "$hand"
        ));
    }

    #[test]
    fn other_message_like_events_are_not_forwarded() {
        let message = raw_event(
            r#"{
                "type": "m.room.message",
                "event_id": "$msg",
                "sender": "@bob:example.org",
                "origin_server_ts": 7,
                "content": { "msgtype": "m.text", "body": "🖐️" }
            }"#,
        );
        assert!(timeline_ingest_from_raw(&message, None, &wanted()).is_none());
    }

    #[test]
    fn an_application_that_wants_no_timeline_gets_only_redactions() {
        let hand = raw_event(
            r#"{
                "type": "m.reaction",
                "event_id": "$hand",
                "sender": "@bob:example.org",
                "origin_server_ts": 5,
                "content": {}
            }"#,
        );
        assert!(timeline_ingest_from_raw(&hand, None, &[]).is_none());
    }
}
