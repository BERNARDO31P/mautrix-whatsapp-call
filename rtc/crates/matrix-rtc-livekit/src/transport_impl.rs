// Copyright 2026 Element Creations Ltd.
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE in the repository root for full details.

//! [`MediaTransport`] implementation for the MSC4195 LiveKit transport.
//!
//! This is where LiveKit stops being visible: everything above this module
//! (the `matrix_rtc_call_sdk::CallEngine`, the `LiveKitCall` facade's unified event
//! stream, and eventually the FFI) speaks the transport-neutral vocabulary of
//! `matrix-rtc-media`, and this module translates it to SFU reality —
//! the token exchange, the E2EE room connection, `RoomEvent`s, and
//! `NativeAudioStream` frames.
//!
//! The connection grouping key is the `livekit_service_url`: MSC4195 says
//! members announcing the same focus share one LiveKit room (same
//! `livekit_alias`), while every other focus needs its own subscribe-side
//! connection.

use std::any::Any;
use std::borrow::Cow;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, Weak};

use async_trait::async_trait;
use futures_util::StreamExt;
use livekit::e2ee::EncryptionType;
use livekit::id::ParticipantIdentity;
use livekit::options::TrackPublishOptions;
use livekit::prelude::{
    LocalAudioTrack, LocalTrack, LocalVideoTrack, Participant, RemoteTrack, RemoteTrackPublication,
    RoomEvent, RtcAudioSource, TrackDimension, TrackKind, TrackPublication, TrackSource,
};
use livekit::track::VideoQuality as LkVideoQuality;
use livekit::webrtc::audio_source::AudioSourceOptions;
use livekit::webrtc::audio_source::native::NativeAudioSource;
use livekit::webrtc::audio_stream::native::NativeAudioStream;
use livekit::webrtc::native::frame_cryptor::EncryptionState as LkEncryptionState;
use livekit::webrtc::prelude::AudioFrame as LkAudioFrame;
use livekit::webrtc::stats::{QualityLimitationReason, RtcStats};
use livekit::webrtc::video_frame::{
    I420Buffer as LkI420Buffer, VideoBuffer, VideoFrame as LkVideoFrame,
    VideoRotation as LkVideoRotation,
};
use livekit::webrtc::video_source::native::NativeVideoSource;
use livekit::webrtc::video_source::{RtcVideoSource, VideoResolution};
use livekit::webrtc::video_stream::native::NativeVideoStream;
use livekit_signaling::SignalError;
use matrix_rtc_transport::livekit::TokenServiceError;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender, unbounded_channel};
use tokio::sync::{oneshot, watch};

use matrix_rtc_core::MatrixBackend;
use matrix_rtc_core::{IdentitySubject, JoinedMembership, RtcIdentityMapper, RtcTransport};
use matrix_rtc_transport::{
    AudioFrame, ConnectionContext, ConnectionEvent, FrameEncryptionState, I420Buffer,
    LocalTrackHandle, MediaStreamKind, MediaTransport, OwnFocusTransport, PublishOptions,
    QualityLimit, QualityLimitation, ReceiveStats, RemoteTrackHandle, ResolvedConstraints,
    SendLayerStats, SendStats, SpeakingParticipant, StreamDemand, TransportConnection,
    TransportError, VideoCodecPreference, VideoDetail, VideoFrame, VideoRotation,
};

use crate::session::LiveKitSession;
use crate::video_encoding::{supported_video_codecs, video_publish_options};
use crate::{
    LiveKitTarget, LiveKitTransportConfig, SfuToken, TrustedFocus, acquire_token, identity_mapper, room_options,
};

/// Sample rate remote audio is resampled to before crossing the transport
/// boundary, matching what the recording helpers in [`crate::media`] use.
const AUDIO_SAMPLE_RATE: i32 = 48_000;
const AUDIO_CHANNELS: i32 = 1;

/// The MSC4195 LiveKit backend of [`MediaTransport`].
///
/// One instance serves a whole call: [`MediaTransport::connect`] can be
/// invoked once per focus. Every connection has its own E2EE `KeyProvider`,
/// since LiveKit keeps the SFU's SIF trailer there, and every key is installed
/// in each (keys are indexed by participant identity, which is unique per
/// membership, so the same keys serve all foci).
///
/// A connection key names a [`LiveKitTarget`]: how its token is obtained (the
/// legacy JWT service, or the homeserver) and for which SFU.
pub struct LiveKitMediaTransport {
    http: reqwest::Client,
    backend: Arc<dyn MatrixBackend>,
    key_providers: crate::Msc4195KeyProviders,
    trusted_focus: Option<TrustedFocus>,
    auto_subscribe: bool,
    /// How a member becomes an SFU participant identity: the one its published
    /// transport's token flow mints, by default.
    ///
    /// MUST be the same value installed on the core's encryption manager. A
    /// divergence does not error — it silences: every `identity_map` lookup
    /// misses, so peers sit in the roster with no media and their keys are
    /// installed under an identity the SFU never assigned.
    identity_mapper: RtcIdentityMapper,
}

impl LiveKitMediaTransport {
    /// `key_providers` MUST be the same handle given to the
    /// [`crate::MediaKeyBridge`] importing the core's media keys.
    pub fn new(
        http: reqwest::Client,
        backend: Arc<dyn MatrixBackend>,
        key_providers: crate::Msc4195KeyProviders,
    ) -> Self {
        Self {
            http,
            backend,
            key_providers,
            trusted_focus: None,
            auto_subscribe: true,
            identity_mapper: identity_mapper(),
        }
    }

    /// Restrict credential exchanges to deployment-owned endpoints. A fresh
    /// client deliberately disables redirects; a caller's generic HTTP client
    /// must not forward the service-account OpenID exchange to another URL.
    pub fn with_trusted_focus(mut self, policy: TrustedFocus) -> Result<Self, TransportError> {
        self.http = reqwest::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .map_err(|error| TransportError::Connect(error.to_string()))?;
        self.trusted_focus = Some(policy);
        Ok(self)
    }

    /// Make every connection of this transport publish-only: remote tracks are
    /// never subscribed, so no remote media flows and no decoder is created.
    /// See [`crate::connect_e2ee`].
    pub fn with_auto_subscribe(mut self, auto_subscribe: bool) -> Self {
        self.auto_subscribe = auto_subscribe;
        self
    }

    /// Substitute the participant-identity derivation.
    ///
    /// A builder rather than a `new` parameter: the transport-driven derivation
    /// is the default. `matrix_rtc_call_sdk::attach_livekit` passes the same
    /// `Arc` it gives the core's encryption manager, which is the point: the
    /// derivation sites must not skew.
    pub fn with_identity_mapper(mut self, identity_mapper: RtcIdentityMapper) -> Self {
        self.identity_mapper = identity_mapper;
        self
    }
}

/// The typed connect, for callers that need the underlying [`LiveKitSession`]
/// (the `LiveKitCall` facade's deprecated raw accessors).
#[async_trait]
impl OwnFocusTransport for LiveKitMediaTransport {
    type Connection = LiveKitTransportConnection;

    async fn connect_own(
        &self,
        connection_key: &str,
        ctx: &ConnectionContext,
    ) -> Result<
        (
            LiveKitTransportConnection,
            UnboundedReceiver<ConnectionEvent>,
        ),
        TransportError,
    > {
        let connection = self.prepare_own(connection_key, ctx);
        let events = connection.reconnect(false).await?;
        Ok((connection, events))
    }

    fn prepare_own(
        &self,
        connection_key: &str,
        ctx: &ConnectionContext,
    ) -> LiveKitTransportConnection {
        // A bare URL, as a host connecting outside a call may pass, names a
        // legacy JWT service.
        let target = LiveKitTarget::from_connection_key(connection_key).unwrap_or_else(|| {
            LiveKitTarget::SfuGet {
                service_url: connection_key.to_owned(),
            }
        });
        let own_identity = target.own_identity(
            &ctx.member.user_id,
            &ctx.member.device_id,
            &ctx.member.member_id,
        );
        let config = LiveKitTransportConfig {
            target,
            room_id: ctx.room_id.clone(),
            slot_id: ctx.slot_id.clone(),
            member_id: ctx.member.member_id.clone(),
            device_id: ctx.member.device_id.clone(),
        };
        LiveKitTransportConnection {
            connection_key: connection_key.to_owned(),
            shared: Arc::new(ConnectionShared {
                session: watch::Sender::new(None),
                own_identity,
                local_key_index: Mutex::new(None),
                local_tracks: Mutex::new(Vec::new()),
                connector: Connector {
                    http: self.http.clone(),
                    backend: self.backend.clone(),
                    trusted_focus: self.trusted_focus.clone(),
                    key_provider: self.key_providers.connection_provider(),
                    e2ee: ctx.e2ee,
                    auto_subscribe: self.auto_subscribe,
                    config: Mutex::new(config),
                    token: Mutex::new(None),
                },
                wanted: Mutex::new(Default::default()),
                installs: Mutex::new(Some(0)),
                installed_generation: AtomicU64::new(0),
                source_sizes: Mutex::new(Default::default()),
            }),
        }
    }
}

#[async_trait]
impl MediaTransport for LiveKitMediaTransport {
    fn transport_type(&self) -> &'static str {
        "livekit"
    }

    fn connection_key(&self, transport: &RtcTransport, publisher_user_id: &str) -> Option<String> {
        let key = matrix_rtc_transport::livekit::connection_key(transport, publisher_user_id)?;
        if let Some(policy) = &self.trusted_focus {
            if !policy.allows_target(&LiveKitTarget::from_connection_key(&key)?) {
                return None;
            }
        }
        Some(key)
    }

    fn shares_identity(&self, a: &str, b: &str) -> bool {
        match (
            LiveKitTarget::from_connection_key(a),
            LiveKitTarget::from_connection_key(b),
        ) {
            (Some(a), Some(b)) => a.same_flow(&b),
            _ => false,
        }
    }

    fn remote_identity(&self, member: &JoinedMembership) -> Option<String> {
        // Through the mapper rather than a derivation of its own, so peers'
        // identities are derived exactly as the core addresses their keys.
        (self.identity_mapper)(&IdentitySubject {
            user_id: &member.sender,
            device_id: member.origin.sender_device_id(),
            member_id: &member.member_id,
            transports: &member.transports,
        })
    }

    async fn connect(
        &self,
        connection_key: &str,
        ctx: &ConnectionContext,
    ) -> Result<
        (
            Box<dyn TransportConnection>,
            UnboundedReceiver<ConnectionEvent>,
        ),
        TransportError,
    > {
        let (connection, events) = self.connect_own(connection_key, ctx).await?;
        Ok((Box::new(connection), events))
    }
}

/// One SFU connection, behind a handle that outlives each connect (034 R24).
///
/// A cheap clonable handle: the caller keeps one clone (for raw session
/// access and the final close) while another is owned by the engine's pool.
/// The LiveKit room behind it is replaced on every [`TransportConnection::
/// reconnect`], and every local publication made through it is carried onto
/// the new room, so the host's capture handles never go stale.
#[derive(Clone)]
pub struct LiveKitTransportConnection {
    connection_key: String,
    shared: Arc<ConnectionShared>,
}

struct ConnectionShared {
    /// The connected room; `None` before the first connect.
    session: watch::Sender<Option<Arc<LiveKitSession>>>,
    /// Our own participant identity on this connection: what its target's
    /// token flow mints. A take-over keeps it, since only a target of the same
    /// flow is ever taken over.
    own_identity: String,
    /// The key index our own frames must carry, once a rotation has activated.
    ///
    /// Held because a frame cryptor is created per published track and starts at
    /// index 0: a track published after a rotation, or republished on a new
    /// room, would otherwise be stamped with an index no peer holds a key for.
    local_key_index: Mutex<Option<u8>>,
    /// Every publication made through this handle, republished on each new
    /// room. Weak: an unpublished or dropped one simply goes.
    local_tracks: Mutex<Vec<Weak<LiveKitLocalTrack>>>,
    connector: Connector,
    /// The remote identities this connection subscribes to
    /// ([`TransportConnection::set_wanted`]); nobody until the pool says.
    wanted: Mutex<std::collections::HashSet<String>>,
    /// Bumped for every reconnect or take-over. None is terminal; neither a
    /// late connection nor a new attempt can install a room after close.
    installs: Mutex<Option<u64>>,
    // The installed room may precede a reserved but not yet installed attempt.
    // Read/write this only under the installation fence.
    installed_generation: AtomicU64,
    /// Each remote video publication's full size, by track sid, as the
    /// publisher announced it: livekit overwrites `dimension()` with the last
    /// size we asked for, so it is read before our first request. One entry
    /// per publication this connection was asked to constrain.
    source_sizes: Mutex<std::collections::HashMap<String, (u32, u32)>>,
}

/// Once a room is installed, cancellation of its restoration must not leave
/// it advertising a partially restored set of local tracks.
struct RestorationGuard {
    connection: LiveKitTransportConnection,
    generation: u64,
    session: Arc<LiveKitSession>,
    armed: bool,
}

impl RestorationGuard {
    fn new(connection: &LiveKitTransportConnection, generation: u64, session: &Arc<LiveKitSession>) -> Self {
        Self { connection: connection.clone(), generation, session: session.clone(), armed: true }
    }

    fn finish(mut self, result: Result<(), TransportError>) -> Result<(), TransportError> {
        let result = self.connection.finish_reconnect(self.generation, &self.session, result);
        self.armed = false;
        if result.is_ok() {
            self.connection.reassert_local_key_index();
        }
        result
    }
}

impl Drop for RestorationGuard {
    fn drop(&mut self) {
        if self.armed {
            let _ = self.connection.finish_reconnect(self.generation, &self.session,
                Err(TransportError::Closed("publication restoration cancelled".into())));
        }
    }
}

/// What [`TransportConnection::hand_over`] gives: the connected room and how
/// to reach its focus again.
struct Handover {
    session: Arc<LiveKitSession>,
    config: LiveKitTransportConfig,
    token: Option<SfuToken>,
}

/// What a (re)connect needs: the token exchange and the room options.
struct Connector {
    http: reqwest::Client,
    backend: Arc<dyn MatrixBackend>,
    trusted_focus: Option<TrustedFocus>,
    /// This connection's own: a reconnect sets the SFU's trailer on it again.
    key_provider: crate::ConnectionKeyProvider,
    /// Whether frames are encrypted (spec 038 R11); when they are, a track
    /// declared unencrypted is refused (R12).
    e2ee: bool,
    /// Whether this connection receives at all; when it does, only the wanted
    /// participants ([`ConnectionShared::wanted`]). The room itself never
    /// auto-subscribes.
    auto_subscribe: bool,
    /// The focus connects go to; a take-over moves it (034 R25).
    config: Mutex<LiveKitTransportConfig>,
    /// The token the last connect used, reused until the SFU rejects it.
    token: Mutex<Option<SfuToken>>,
}

fn begin_install(installs: &Mutex<Option<u64>>) -> Result<u64, TransportError> {
    let mut generation = installs.lock().expect("installs mutex poisoned");
    let next = generation
        .and_then(|value| value.checked_add(1))
        .ok_or_else(|| TransportError::Closed("connection closed or generation exhausted".into()))?;
    *generation = Some(next);
    Ok(next)
}

// Use the same installation generation as the room commit. Take-over holds
// this lock while replacing the configuration, cached token and room. Never
// hold it across the HTTP exchange, but reject a stale result before caching it.
async fn token_for_generation<F, Fut>(
    installs: &Mutex<Option<u64>>,
    started: u64,
    config: &Mutex<LiveKitTransportConfig>,
    cache: &Mutex<Option<SfuToken>>,
    fresh: bool,
    acquire: F,
) -> Result<SfuToken, TransportError>
where
    F: FnOnce(LiveKitTransportConfig) -> Fut,
    Fut: std::future::Future<Output = Result<SfuToken, TransportError>>,
{
    let abandoned = || TransportError::Closed("abandoned: focus changed during token exchange".into());
    let snapshot = {
        let generation = installs.lock().expect("installs mutex poisoned");
        if *generation != Some(started) {
            return Err(abandoned());
        }
        if !fresh {
            if let Some(token) = cache.lock().expect("token mutex poisoned").clone() {
                return Ok(token);
            }
        }
        config.lock().expect("config mutex poisoned").clone()
    };
    let result = acquire(snapshot).await;
    let generation = installs.lock().expect("installs mutex poisoned");
    if *generation != Some(started) {
        return Err(abandoned());
    }
    let acquired = result?;
    *cache.lock().expect("token mutex poisoned") = Some(acquired.clone());
    Ok(acquired)
}

impl Connector {
    async fn token(&self, installs: &Mutex<Option<u64>>, started: u64, fresh: bool) -> Result<SfuToken, TransportError> {
        token_for_generation(installs, started, &self.config, &self.token, fresh, |config| async move {
            if self.trusted_focus.as_ref().is_some_and(|policy| !policy.allows_target(&config.target)) {
                return Err(TransportError::Refused("focus is not authorized by deployment policy".into()));
            }
            let token = acquire_token(&self.http, &config, self.backend.as_ref())
                .await
                .map_err(classify_token_error)?;
            if self.trusted_focus.as_ref().is_some_and(|policy| !policy.allows_sfu_url(&token.url)) {
                return Err(TransportError::Refused("SFU token names an unauthorized endpoint".into()));
            }
            Ok(token)
        }).await
    }
}

/// A token failure: a homeserver without MSC4195's `get_token` ends the call
/// (spec 038 R44); a 4xx from the JWT service, another 4xx other than a rate
/// limit from the homeserver, or a host refusal, is a refusal; anything else
/// may pass.
fn classify_token_error(error: crate::Error) -> TransportError {
    match &error {
        crate::Error::TokenEndpointUnsupported(_) => {
            TransportError::TokenEndpointUnsupported(error.to_string())
        }
        crate::Error::Service(TokenServiceError::Rejected { status, .. })
            if (400..500).contains(status) =>
        {
            TransportError::Refused(error.to_string())
        }
        crate::Error::Backend(backend)
            if backend.is_permanent()
                || backend
                    .status
                    .is_some_and(|status| (400..500).contains(&status) && status != 429) =>
        {
            TransportError::Refused(error.to_string())
        }
        _ => TransportError::Connect(error.to_string()),
    }
}

/// An SFU connect failure: a 401 is the token, as is a 404. Our own SFU's
/// token service creates the room when it issues our token (`auto_create`
/// off), so a 404 there means the SFU restarted and forgot it, and a fresh
/// token creates it again (on a peer's SFU taken over under 034 R25, its token
/// service may not create rooms for us: the second 404 is then a refusal).
/// Another 4xx is a refusal; anything else may pass.
fn classify_room_error(error: crate::Error) -> TransportError {
    if let crate::Error::Room(livekit::RoomError::Engine(
        livekit::rtc_engine::EngineError::Signal(SignalError::Client(status, _)),
    )) = &error
    {
        return match status.as_u16() {
            401 | 404 => TransportError::TokenRejected(error.to_string()),
            _ => TransportError::Refused(error.to_string()),
        };
    }
    TransportError::Connect(error.to_string())
}

impl LiveKitTransportConnection {
    /// The full size `publication`'s publisher announced, cached on first
    /// sight (see `ConnectionShared::source_sizes`); `None` while unknown.
    fn source_size(&self, publication: &RemoteTrackPublication) -> Option<(u32, u32)> {
        let mut sizes = self
            .shared
            .source_sizes
            .lock()
            .expect("source sizes mutex poisoned");
        let sid = String::from(publication.sid());
        if let Some(size) = sizes.get(&sid) {
            return Some(*size);
        }
        let TrackDimension(width, height) = publication.dimension();
        (width > 0 && height > 0).then(|| {
            sizes.insert(sid, (width, height));
            (width, height)
        })
    }

    /// The connected session, for the `LiveKitCall` facade's transition-period
    /// raw accessors; `None` before the first connect.
    pub fn session(&self) -> Option<Arc<LiveKitSession>> {
        self.current_session()
    }

    /// Waits until a session is connected and returns it.
    pub async fn wait_connected(&self) -> Arc<LiveKitSession> {
        let mut session = self.shared.session.subscribe();
        let connected = session
            .wait_for(Option::is_some)
            .await
            .expect("the sender lives in the handle");
        connected.clone().expect("waited for it")
    }

    fn current_session(&self) -> Option<Arc<LiveKitSession>> {
        self.shared.session.borrow().clone()
    }

    fn wants(&self, identity: &str) -> bool {
        self.shared.connector.auto_subscribe
            && self
                .shared
                .wanted
                .lock()
                .expect("wanted mutex poisoned")
                .contains(identity)
    }

    /// Subscribes to the tracks of every wanted participant already in
    /// `session`; later ones are subscribed as they are published.
    ///
    /// A participant leaving the set is not unsubscribed: it leaves the set
    /// because its member left or moved, and so its participant leaves the
    /// room. Unsubscribing would also risk a later resubscribe, which livekit
    /// does not recover from (see `apply_constraints`).
    fn subscribe_wanted(&self, session: &LiveKitSession) {
        for (identity, participant) in session.room().remote_participants() {
            if !self.wants(identity.as_str()) {
                continue;
            }
            for publication in participant.track_publications().into_values() {
                if !publication.is_desired() {
                    log::debug!(
                        "subscribing to {identity}'s {:?} on {}",
                        publication.source(),
                        self.connection_key
                    );
                    publication.set_subscribed(true);
                }
            }
        }
    }

    /// Re-assert the current index on our senders, for a track published after a
    /// rotation: its cryptor is new, and new cryptors start at index 0.
    fn reassert_local_key_index(&self) {
        let current = self
            .shared
            .local_key_index
            .lock()
            .expect("local key index mutex poisoned");
        if let Some(key_index) = *current {
            self.apply_local_key_index(key_index);
        }
        // Serialize the SDK updates as well as the stored index. A stale
        // reassertion must not run after a concurrent rotation commits.
        drop(current);
    }

    fn apply_local_key_index(&self, key_index: u8) {
        let Some(session) = self.current_session() else {
            log::debug!("not connected; key index {key_index} applies at the connect");
            return;
        };
        let mut switched = 0usize;
        for ((identity, _track_sid), cryptor) in session.room().e2ee_manager().frame_cryptors() {
            if identity.as_str() == self.shared.own_identity {
                cryptor.set_key_index(i32::from(key_index));
                switched += 1;
            }
        }
        if switched == 0 {
            // Nothing published yet; `reassert_local_key_index` picks it up when
            // a track is.
            log::debug!(
                "no local frame cryptor to move to key index {key_index} yet (nothing published)"
            );
        } else {
            log::debug!("our own frames now carry key index {key_index} ({switched} cryptor(s))");
        }
    }

    /// Called with the installation fence held, before replacing the room.
    /// Keep the old handle for retraction, but stop feeding its publication
    /// while the asynchronous room close and new publish are pending.
    fn mute_replaced_publications(&self, replacement: &Arc<LiveKitSession>) {
        let tracks = self.shared.local_tracks.lock().expect("local tracks mutex poisoned");
        for track in tracks.iter().filter_map(Weak::upgrade) {
            let published = track.published.lock().expect("published mutex poisoned");
            if let Some((local, owner)) = published.as_ref() {
                if !publication_on_current_room(owner, Some(replacement)) {
                    LiveKitLocalTrack::apply_mute(local, true);
                }
            }
        }
    }

    /// Carries every live publication onto the room just connected, with the
    /// mute it holds.
    async fn republish_local_tracks(&self, session: &Arc<LiveKitSession>) -> Result<(), TransportError> {
        let tracks: Vec<Arc<LiveKitLocalTrack>> = {
            let mut tracks = self
                .shared
                .local_tracks
                .lock()
                .expect("local tracks mutex poisoned");
            tracks.retain(|track| track.strong_count() > 0);
            tracks.iter().filter_map(Weak::upgrade).collect()
        };
        for track in tracks {
            if track.closed.load(Ordering::Acquire) || track.stop_requested.load(Ordering::Acquire) {
                continue;
            }
            track.publish_on(session, false).await?;
        }
        Ok(())
    }

    /// Commit a reconnect or handover result only while its generation owns this
    /// room. A partial publication failure must not report a healthy room.
    fn finish_reconnect(
        &self,
        started: u64,
        session: &Arc<LiveKitSession>,
        result: Result<(), TransportError>,
    ) -> Result<(), TransportError> {
        let failed = {
            let installs = self.shared.installs.lock().expect("installs mutex poisoned");
            let current = self.current_session();
            let owns_room = self.shared.installed_generation.load(Ordering::Relaxed) == started
                && current.as_ref().is_some_and(|current| Arc::ptr_eq(current, session));
            let result = reconnect_publication_result(*installs == Some(started) && owns_room, result);
            // A new reservation may precede installation of its room. Retire
            // an abandoned partial restoration while it still owns the room,
            // but never detach a room installed by a subsequent take-over.
            if !owns_room || installs.is_none() {
                return result;
            }
            if let Err(error) = result {
                // Prevent capture or mute changes from making a partially
                // restored room transmit while its asynchronous close runs.
                let tracks = self.shared.local_tracks.lock().expect("local tracks mutex poisoned");
                for track in tracks.iter().filter_map(Weak::upgrade) {
                    let mut published = track.published.lock().expect("published mutex poisoned");
                    if let Some((local, room)) = published.as_ref() {
                        if Arc::ptr_eq(room, session) {
                            LiveKitLocalTrack::apply_mute(local, true);
                            *published = None;
                        }
                    }
                }
                self.shared.session.send_replace(None);
                Some(error)
            } else {
                None
            }
        };
        if let Some(error) = failed {
            close_room(session.clone());
            return Err(error);
        }
        Ok(())
    }
}

fn reconnect_publication_result(
    current: bool,
    result: Result<(), TransportError>,
) -> Result<(), TransportError> {
    if current {
        result
    } else {
        Err(TransportError::Closed("abandoned: reconnect superseded or closed".into()))
    }
}

#[async_trait]
impl TransportConnection for LiveKitTransportConnection {
    fn connection_key(&self) -> &str {
        &self.connection_key
    }

    fn set_wanted(&self, identities: &[String]) {
        let added = {
            let mut wanted = self.shared.wanted.lock().expect("wanted mutex poisoned");
            let added = identities.iter().any(|identity| !wanted.contains(identity));
            *wanted = identities.iter().cloned().collect();
            added
        };
        log::debug!("{} receives {identities:?}", self.connection_key);
        if added && let Some(session) = self.current_session() {
            self.subscribe_wanted(&session);
        }
    }

    /// Also remembered for tracks published later. The key provider's
    /// `set_key` only fills the key *ring*; the index a sender stamps lives on
    /// its frame cryptor and changes only here.
    fn set_local_key_index(&self, key_index: u8) {
        let mut current = self
            .shared
            .local_key_index
            .lock()
            .expect("local key index mutex poisoned");
        *current = Some(key_index);
        self.apply_local_key_index(key_index);
        drop(current);
    }

    /// Connects a new room with the token of the last connect (or a fresh one)
    /// and the same key provider, so the key ring is intact; the previous room
    /// is closed, and our publications are carried over.
    async fn reconnect(
        &self,
        fresh_token: bool,
    ) -> Result<UnboundedReceiver<ConnectionEvent>, TransportError> {
        let reservation = self.reserve_reconnect()?;
        self.reconnect_reserved(fresh_token, reservation).await
    }

    fn reserve_reconnect(&self) -> Result<Option<u64>, TransportError> {
        begin_install(&self.shared.installs).map(Some)
    }

    async fn reconnect_reserved(
        &self,
        fresh_token: bool,
        reservation: Option<u64>,
    ) -> Result<UnboundedReceiver<ConnectionEvent>, TransportError> {
        let started = reservation.ok_or_else(|| TransportError::Closed("missing reconnect reservation".into()))?;
        let connector = &self.shared.connector;
        let token = connector.token(&self.shared.installs, started, fresh_token).await?;
        let connection = LiveKitSession::connect_with_options(
            &token,
            room_options(
                connector
                    .e2ee
                    .then(|| connector.key_provider.provider().clone()),
                // Subscriptions are ours to make: see `subscribe_wanted`.
                false,
            ),
        )
        .await;
        // A refusal from a superseded attempt must not stop the current call.
        let (connection, session, previous) = {
            let installs = self
                .shared
                .installs
                .lock()
                .expect("installs mutex poisoned");
            if *installs != Some(started) {
                drop(installs);
                if let Ok(connection) = connection {
                    close_room(Arc::new(connection.session));
                }
                return Err(TransportError::Closed(
                    "abandoned: connection attempt superseded or closed".into(),
                ));
            }
            let connection = connection.map_err(classify_room_error)?;
            // Move the event receiver separately; the session is not Clone.
            let session = Arc::new(connection.session);
            self.mute_replaced_publications(&session);
            self.shared.installed_generation.store(started, Ordering::Relaxed);
            let previous = self.shared.session.send_replace(Some(session.clone()));
            (connection.events, session, previous)
        };
        if let Some(previous) = previous {
            close_room(previous);
        }
        // The receiver returned by the room connect carries events from the
        // very beginning (nothing can be missed); hand it to the translator.
        let (events_tx, events_rx) = unbounded_channel();
        tokio::spawn(translate_room_events(
            connection,
            events_tx,
            self.clone(),
            Arc::downgrade(&session),
        ));
        let restoration = RestorationGuard::new(self, started, &session);
        self.subscribe_wanted(&session);
        let result = self.republish_local_tracks(&session).await;
        restoration.finish(result)?;
        Ok(events_rx)
    }

    fn hand_over(&self) -> Option<Box<dyn Any>> {
        // Snapshot the room, configuration and token under the same fence as take-over.
        let generation = self.shared.installs.lock().expect("installs mutex poisoned");
        if generation.is_none() {
            return None;
        }
        let session = self.current_session()?;
        let connector = &self.shared.connector;
        Some(Box::new(Handover {
            session,
            config: connector
                .config
                .lock()
                .expect("config mutex poisoned")
                .clone(),
            token: connector
                .token
                .lock()
                .expect("token mutex poisoned")
                .clone(),
        }))
    }

    /// Installs the handed-over room as this handle's, abandoning any
    /// reconnect in flight; our publications are carried onto it in the
    /// background, as after a reconnect.
    fn take_over(&self, handover: Box<dyn Any>) -> Result<Option<oneshot::Receiver<Result<(), TransportError>>>, TransportError> {
        let handover = handover.downcast::<Handover>().map_err(|_| {
            TransportError::Unsupported("not a LiveKit connection's handover".into())
        })?;
        let Handover {
            session,
            config,
            token,
        } = *handover;
        if self.shared.connector.trusted_focus.as_ref().is_some_and(|policy| {
            !policy.allows_target(&config.target)
                || token.as_ref().is_none_or(|token| !policy.allows_sfu_url(&token.url))
        }) {
            return Err(TransportError::Refused("unauthorized focus handover".into()));
        }
        log::info!(
            "our own handle takes over the room of {}",
            config.target.describe()
        );
        let (started, previous) = {
            let mut installs = self
                .shared
                .installs
                .lock()
                .expect("installs mutex poisoned");
            let next = installs.and_then(|value| value.checked_add(1))
                .ok_or_else(|| TransportError::Closed("connection closed or generation exhausted".into()))?;
            *installs = Some(next);
            let connector = &self.shared.connector;
            *connector.config.lock().expect("config mutex poisoned") = config;
            *connector.token.lock().expect("token mutex poisoned") = token;
            self.mute_replaced_publications(&session);
            self.shared.installed_generation.store(next, Ordering::Relaxed);
            (next, self.shared.session.send_replace(Some(session.clone())))
        };
        if let Some(previous) = previous {
            if !Arc::ptr_eq(&previous, &session) {
                close_room(previous);
            }
        }
        // The room was the other connection's, subscribed to its members.
        self.subscribe_wanted(&session);
        let this = self.clone();
        let restoration = RestorationGuard::new(self, started, &session);
        let (mut completed, receiver) = oneshot::channel();
        tokio::spawn(async move {
            let result = tokio::select! {
                biased;
                _ = completed.closed() => Err(TransportError::Closed("handover abandoned".into())),
                result = this.republish_local_tracks(&session) => result,
            };
            let _ = completed.send(restoration.finish(result));
        });
        Ok(Some(receiver))
    }

    /// Before the first connect the publication is held, and goes out with
    /// it; afterwards it is published at once, and carried onto every later
    /// room.
    async fn publish(
        &self,
        options: PublishOptions,
    ) -> Result<Arc<dyn LocalTrackHandle>, TransportError> {
        let kind = options.kind;
        let source = match kind {
            MediaStreamKind::Microphone | MediaStreamKind::ScreenShareAudio => {
                let config = options.audio.unwrap_or_default();
                LocalSource::Audio(NativeAudioSource::new(
                    AudioSourceOptions::default(),
                    config.sample_rate,
                    config.num_channels,
                    1000,
                ))
            }
            MediaStreamKind::Camera | MediaStreamKind::ScreenShare => {
                let config = options.video.ok_or_else(|| {
                    TransportError::Unsupported(
                        "publishing video requires a VideoSourceConfig".into(),
                    )
                })?;
                // livekit falls back to its default without a word when the
                // codec cannot be encoded here; say so, at least.
                let mut codec = options.video_codec;
                if codec != VideoCodecPreference::Auto && !supported_video_codecs().contains(&codec)
                {
                    log::warn!(
                        "{codec:?} cannot be encoded on this platform; publishing {kind:?} with the default codec"
                    );
                    codec = VideoCodecPreference::Auto;
                }
                LocalSource::Video {
                    source: NativeVideoSource::new(
                        VideoResolution {
                            width: config.width,
                            height: config.height,
                        },
                        matches!(kind, MediaStreamKind::ScreenShare),
                    ),
                    options: video_publish_options(
                        livekit_track_source(kind),
                        config,
                        // Below 480px livekit computes a single simulcast
                        // encoding (rid "q" only) — a degenerate shape the SFU
                        // delivered no frames for in testing. One layer wants
                        // a plain encoding.
                        options.simulcast && u32::max(config.width, config.height) >= 480,
                        codec,
                        options.video_encoding,
                    ),
                }
            }
            MediaStreamKind::Data => {
                return Err(TransportError::Unsupported(
                    "data publishing is not implemented yet".into(),
                ));
            }
        };
        let track = Arc::new(LiveKitLocalTrack {
            kind,
            source,
            published: Mutex::new(None),
            publication_op: tokio::sync::Mutex::new(()),
            connection: Arc::downgrade(&self.shared),
            muted: AtomicBool::new(options.muted),
            closed: AtomicBool::new(false),
            stop_requested: AtomicBool::new(false),
            retraction_failed: AtomicBool::new(false),
        });
        // Register under the installation fence before the first await. A
        // concurrent room install either precedes this snapshot or includes
        // this track in its subsequent republish pass.
        let session = {
            let installs = self.shared.installs.lock().expect("installs mutex poisoned");
            if installs.is_none() {
                return Err(TransportError::Closed("connection closed".into()));
            }
            self.shared.local_tracks.lock().expect("local tracks mutex poisoned")
                .push(Arc::downgrade(&track));
            self.current_session()
        };
        if let Some(session) = session {
            track.publish_on(&session, true).await?;
            // A frame cryptor is created per published track, at index 0.
            // If we have already rotated, this one must be moved onto the
            // current index or its frames carry a key nobody holds.
            self.reassert_local_key_index();
        }
        Ok(track)
    }

    async fn apply_constraints(
        &self,
        identity: &str,
        kind: MediaStreamKind,
        resolved: ResolvedConstraints,
    ) -> Result<(), TransportError> {
        // The participant or publication being gone is not an error: the
        // engine re-applies constraints when the stream (re)appears.
        let Some(session) = self.current_session() else {
            return Ok(());
        };
        let participants = session.room().remote_participants();
        let Some(participant) = participants.get(&ParticipantIdentity::from(identity)) else {
            return Ok(());
        };
        let Some(publication) = participant
            .track_publications()
            .into_values()
            .find(|publication| stream_kind(publication.source(), publication.kind()) == kind)
        else {
            return Ok(());
        };

        // `Off` SHOULD be a full unsubscribe (`set_subscribed(false)`), but
        // livekit 0.7.48's client-side *re*subscribe is unreliable: the SFU
        // resumes RTP on the previous receiver without a new OnTrack, so no
        // `TrackSubscribed` fires, `publication.track()` stays `None`, and
        // every subsequent settings call no-ops — the stream is stranded
        // (observed against livekit-server v1.10.1). Until that works
        // upstream, `Off` maps to the pause path too: identical zero
        // bandwidth (dynacast even stops the publisher's encoder), only an
        // idle decoder object is retained. `Paused`/`Off` keep the
        // subscription and set the `disabled` flag server-side — no data,
        // instant resume.
        let active = matches!(resolved.demand, StreamDemand::Active);
        if !matches!(kind, MediaStreamKind::Camera | MediaStreamKind::ScreenShare) {
            publication.set_enabled(active);
            return Ok(());
        }

        // ONE `UpdateTrackSettings` per change. Every livekit setter spawns
        // its own task sending a full-replacement message, and nothing orders
        // two of them: `set_enabled` carries the size last asked for,
        // `set_video_quality` none at all, so resuming with a quality could
        // land as the stale size and pin the stream to an old layer. Instead
        // the pause flag is flipped locally (`enable`/`disable` send nothing)
        // and a single `update_video_dimensions` sends `{disabled, width,
        // height}` as they stand. A quality becomes a size of the publisher's
        // full one.
        let source = self.source_size(&publication);
        match requested_size(resolved.detail, source) {
            Some((width, height)) => {
                if let Some(track) = publication.track() {
                    if active {
                        track.enable();
                    } else {
                        track.disable();
                    }
                }
                publication.update_video_dimensions(TrackDimension(width, height));
            }
            None => {
                publication.set_enabled(active);
                // Only when the publisher's size is unknown. `set_video_quality`
                // sends zeroed dimensions and `disabled: false`, so it must
                // never follow a pause. A no-op on a non-simulcast track.
                if let (VideoDetail::Quality(limit), true) = (resolved.detail, active) {
                    publication.set_video_quality(match limit {
                        QualityLimit::Low => LkVideoQuality::Low,
                        QualityLimit::Medium => LkVideoQuality::Medium,
                        QualityLimit::High => LkVideoQuality::High,
                    });
                }
            }
        }
        Ok(())
    }

    /// Idempotent: closing a connection that is already closed succeeds.
    ///
    /// LiveKit answers `RoomError::AlreadyClosed` both for a second `close()`
    /// and for a room the SFU already tore down — the latter being the normal
    /// case when the server disconnected us before the user hung up. Either way
    /// the postcondition callers want ("this connection is not open") already
    /// holds, so reporting a failure only makes a clean hangup look broken.
    async fn close(&self) -> Result<(), TransportError> {
        let session = {
            let mut installs = self.shared.installs.lock().expect("installs mutex poisoned");
            *installs = None;
            // Fence mute changes and publication commits before starting the
            // asynchronous room close. A failed close must not resume media.
            let tracks: Vec<_> = self.shared.local_tracks.lock()
                .expect("local tracks mutex poisoned")
                .iter().filter_map(Weak::upgrade).collect();
            for track in tracks {
                let published = track.published.lock().expect("published mutex poisoned");
                if let Some((local, _)) = published.as_ref() {
                    LiveKitLocalTrack::apply_mute(local, true);
                }
            }
            self.current_session()
        };
        let Some(session) = session else {
            return Ok(());
        };
        match session.room().close().await {
            Ok(()) => {
                self.shared.session.send_replace(None);
                Ok(())
            },
            Err(livekit::RoomError::AlreadyClosed) => {
                log::debug!(
                    "connection {} was already closed; treating as success",
                    self.connection_key,
                );
                self.shared.session.send_replace(None);
                Ok(())
            }
            Err(error) => Err(TransportError::Closed(error.to_string())),
        }
    }
}

/// Closes a room this handle no longer uses, in the background.
fn close_room(session: Arc<LiveKitSession>) {
    tokio::spawn(async move {
        if let Err(error) = session.room().close().await {
            log::debug!("closing the previous room: {error}");
        }
    });
}

/// The native source behind a local publication, which outlives every room
/// it is published on.
enum LocalSource {
    Audio(NativeAudioSource),
    Video {
        source: NativeVideoSource,
        /// Fixed at publish, so every later room encodes the same way.
        options: TrackPublishOptions,
    },
}

/// A local LiveKit publication accepting raw frames from the application.
/// The source is the stable part; the LiveKit track over it is created anew
/// for each room it is published on (034 R24).
struct LiveKitLocalTrack {
    kind: MediaStreamKind,
    source: LocalSource,
    /// The track published now and the session it is on, kept so it can be
    /// muted and retracted; `None` before the first connect. The room never
    /// holds this wrapper, so no cycle.
    published: Mutex<Option<(LocalTrack, Arc<LiveKitSession>)>>,
    /// Serialize asynchronous publish and unpublish on this handle.
    publication_op: tokio::sync::Mutex<()>,
    connection: Weak<ConnectionShared>,
    /// The mute the transport was last told: applied to every room.
    muted: AtomicBool,
    /// Set once unpublish is confirmed. A failed retraction retains the
    /// handle for retry, but never resumes capture or publication.
    closed: AtomicBool,
    /// Irreversible stop intent, set before waiting for the publication gate.
    stop_requested: AtomicBool,
    /// Set when an `unpublish` attempt starts, including cancelled attempts. LiveKit drops the publication
    /// from its local map *before* the fallible transport work, with no
    /// rollback, so after a failure a retry sees "track not found" — which
    /// must not be mistaken for the benign already-retracted case, because
    /// the track may still be live at the SFU.
    retraction_failed: AtomicBool,
}

#[derive(Debug, PartialEq, Eq)]
enum PublicationFailure {
    Closed,
    Superseded,
    Initial,
    Republish,
}

fn publication_failure(open: bool, current: bool, initial: bool) -> PublicationFailure {
    if !open {
        PublicationFailure::Closed
    } else if !current {
        PublicationFailure::Superseded
    } else if initial {
        PublicationFailure::Initial
    } else {
        PublicationFailure::Republish
    }
}

// Session identity is pointer identity, not focus URL or room name: reconnects
// to the same focus still create a different native room.
fn publication_on_current_room<T>(owner: &Arc<T>, current: Option<&Arc<T>>) -> bool {
    current.is_some_and(|current| Arc::ptr_eq(current, owner))
}

impl LiveKitLocalTrack {
    fn connection_for_capture(&self) -> Result<Arc<ConnectionShared>, TransportError> {
        let connection = self.connection.upgrade().ok_or_else(|| {
            TransportError::Closed("connection dropped".into())
        })?;
        if connection.installs.lock().expect("installs mutex poisoned").is_none()
            || self.closed.load(Ordering::Acquire)
            || self.stop_requested.load(Ordering::Acquire)
        {
            return Err(TransportError::Closed("publication or connection closed".into()));
        }
        Ok(connection)
    }

    /// Publishes a new track over the source on `session`.
    ///
    /// Always announce it muted in AddTrackRequest. A mute change during
    /// the asynchronous publish must not leave the new track transmitting.
    /// Reconcile with the latest intent under the same lock as set_muted
    /// only after publishing succeeds.
    async fn publish_on(
        &self,
        session: &Arc<LiveKitSession>,
        initial: bool,
    ) -> Result<(), TransportError> {
        let _operation = self.publication_op.lock().await;
        if self.closed.load(Ordering::Acquire) || self.stop_requested.load(Ordering::Acquire) {
            return Ok(());
        }
        let connection = self.connection.upgrade().ok_or_else(|| {
            TransportError::Closed("connection dropped".into())
        })?;
        {
            let installs = connection.installs.lock().expect("installs mutex poisoned");
            if installs.is_none() {
                return Err(TransportError::Closed("connection closed".into()));
            }
            if !connection.session.borrow().as_ref().is_some_and(|current| Arc::ptr_eq(current, session)) {
                return Ok(()); // The current room's republish pass owns this track.
            }
            let published = self.published.lock().expect("published mutex poisoned");
            if published.as_ref().is_some_and(|(_, previous)| Arc::ptr_eq(previous, session)) {
                return Ok(());
            }
        }
        let source_kind = livekit_track_source(self.kind);
        let (track, options) = match &self.source {
            LocalSource::Audio(source) => {
                let track = LocalAudioTrack::create_audio_track(
                    "audio",
                    RtcAudioSource::Native(source.clone()),
                );
                track.mute();
                (
                    LocalTrack::Audio(track),
                    TrackPublishOptions {
                        source: source_kind,
                        ..Default::default()
                    },
                )
            }
            LocalSource::Video { source, options } => {
                let track = LocalVideoTrack::create_video_track(
                    "video",
                    RtcVideoSource::Native(source.clone()),
                );
                track.mute();
                (LocalTrack::Video(track), options.clone())
            }
        };
        let result = session
            .room()
            .local_participant()
            .publish_track(track.clone(), options)
            .await;
        if let Err(error) = result {
            // A replacement room owns the registered track once installed.
            // An obsolete room's failure must not drop the initial handle.
            // Keep the publication operation locked until the disposition is
            // decided so a concurrent republish cannot revive a failed handle.
            let installs = connection.installs.lock().expect("installs mutex poisoned");
            let current = connection.session.borrow().as_ref()
                .is_some_and(|current| Arc::ptr_eq(current, session));
            match publication_failure(installs.is_some(), current, initial) {
                PublicationFailure::Closed => {
                    if initial {
                        self.finish_retraction();
                    }
                    return Err(TransportError::Closed("connection closed".into()));
                }
                PublicationFailure::Superseded => {
                    log::debug!("ignoring publication failure on replaced room: {error}");
                    return Ok(());
                }
                PublicationFailure::Initial => self.finish_retraction(),
                PublicationFailure::Republish => (),
            }
            return Err(TransportError::Connect(error.to_string()));
        }
        // Never unmute a publication completed on an abandoned room. The
        // installation fence also excludes a concurrent terminal close at
        // the moment we commit the publication.
        let terminal = {
            let installs = connection.installs.lock().expect("installs mutex poisoned");
            if installs.is_some() && connection.session.borrow().as_ref()
                .is_some_and(|current| Arc::ptr_eq(current, session))
            {
                let mut published = self.published.lock().expect("published mutex poisoned");
                // Stop may have been requested while native publish was
                // pending. Keep the new track muted but retain ownership so
                // the waiting unpublish can retract its exact SID.
                Self::apply_mute(&track, self.muted.load(Ordering::Acquire)
                    || self.stop_requested.load(Ordering::Acquire));
                *published = Some((track, Arc::clone(session)));
                self.retraction_failed.store(false, Ordering::Release);
                return Ok(());
            }
            installs.is_none()
        };
        // The abandoned publication is still muted; try to retract it even
        // if the old room's asynchronous shutdown has not completed yet.
        if let Err(error) = session.room().local_participant().unpublish_track(&track.sid()).await {
            log::debug!("could not retract abandoned muted publication: {error}");
        }
        if terminal {
            Err(TransportError::Closed("connection closed".into()))
        } else {
            Ok(())
        }
    }

    fn apply_mute(track: &LocalTrack, muted: bool) {
        match (track, muted) {
            (LocalTrack::Audio(track), true) => track.mute(),
            (LocalTrack::Audio(track), false) => track.unmute(),
            (LocalTrack::Video(track), true) => track.mute(),
            (LocalTrack::Video(track), false) => track.unmute(),
        }
    }

    fn finish_retraction(&self) {
        let mut published = self.published.lock().expect("published mutex poisoned");
        self.closed.store(true, Ordering::Release);
        *published = None;
    }

    fn current_track(&self) -> Option<LocalTrack> {
        self.published
            .lock()
            .expect("published mutex poisoned")
            .as_ref()
            .map(|(track, _)| track.clone())
    }
}

#[async_trait]
impl LocalTrackHandle for LiveKitLocalTrack {
    fn kind(&self) -> MediaStreamKind {
        self.kind
    }

    /// Held for the rooms it is published on later, too.
    fn set_muted(&self, muted: bool) -> Result<(), TransportError> {
        let connection = self.connection.upgrade().ok_or_else(|| {
            TransportError::Closed("connection dropped".into())
        })?;
        let installs = connection.installs.lock().expect("installs mutex poisoned");
        let published = self.published.lock().expect("published mutex poisoned");
        if installs.is_none() || self.closed.load(Ordering::Acquire)
            || self.stop_requested.load(Ordering::Acquire) {
            return Err(TransportError::Closed("publication or connection closed".into()));
        }
        self.muted.store(muted, Ordering::Release);
        if let Some((track, owner)) = published.as_ref() {
            let current = publication_on_current_room(owner, connection.session.borrow().as_ref());
            // Save the requested intent for republish, but never unmute an
            // obsolete room while its asynchronous close is still pending.
            Self::apply_mute(track, muted || !current);
        }
        Ok(())
    }

    /// Idempotent for the same reason `close` is: "track not found" means the
    /// publication is already gone (a second unpublish, or the room closed and
    /// retracted everything), and the postcondition callers want holds — with
    /// one exception. After a *failed* attempt on this handle, "track not
    /// found" only reflects LiveKit's local map (emptied before the fallible
    /// work, without rollback), not the SFU, so it stays an error rather than
    /// reporting a retraction that may never have happened.
    async fn unpublish(&self) -> Result<(), TransportError> {
        // Fence an in-flight publication commit and mute changes immediately,
        // before waiting for the asynchronous per-track gate.
        {
            let connection = self.connection.upgrade();
            let _installation = connection.as_ref().map(|connection|
                connection.installs.lock().expect("installs mutex poisoned"));
            let published = self.published.lock().expect("published mutex poisoned");
            self.stop_requested.store(true, Ordering::Release);
            if let Some((track, _)) = published.as_ref() {
                Self::apply_mute(track, true);
            }
        }
        let _operation = self.publication_op.lock().await;
        if self.closed.load(Ordering::Acquire) {
            return Ok(());
        }
        let published = self
            .published
            .lock()
            .expect("published mutex poisoned")
            .clone();
        let Some((track, session)) = published else {
            // Never went out: nothing to retract, and no room gets it later.
            self.finish_retraction();
            return Ok(());
        };
        let sid = track.sid();
        // Cancellation after submission is as ambiguous as an error.
        let previous_attempt = self.retraction_failed.swap(true, Ordering::AcqRel);
        match session
            .room()
            .local_participant()
            .unpublish_track(&sid)
            .await
        {
            Ok(_) => {
                self.finish_retraction();
                Ok(())
            }
            Err(livekit::RoomError::Internal(message))
                if message.contains("track not found")
                    && !previous_attempt =>
            {
                log::debug!("{sid} was already unpublished; treating as success");
                self.finish_retraction();
                Ok(())
            }
            Err(error) => {
                self.retraction_failed.store(true, Ordering::Release);
                Err(TransportError::Closed(error.to_string()))
            }
        }
    }

    async fn capture_audio(&self, frame: AudioFrame) -> Result<(), TransportError> {
        let _connection = self.connection_for_capture()?;
        let LocalSource::Audio(source) = &self.source else {
            return Err(TransportError::Unsupported(
                "this publication does not accept audio frames".into(),
            ));
        };
        let frame = LkAudioFrame {
            data: Cow::Owned(frame.data),
            sample_rate: frame.sample_rate,
            num_channels: frame.num_channels,
            samples_per_channel: frame.samples_per_channel,
        };
        source
            .capture_frame(&frame)
            .await
            .map_err(|error| TransportError::Closed(error.to_string()))
    }

    fn capture_video(&self, frame: VideoFrame) -> Result<(), TransportError> {
        let _connection = self.connection_for_capture()?;
        let LocalSource::Video { source, .. } = &self.source else {
            return Err(TransportError::Unsupported(
                "this publication does not accept video frames".into(),
            ));
        };
        let buffer = to_libwebrtc_i420(&frame.buffer)?;
        source.capture_frame(&LkVideoFrame {
            rotation: to_livekit_rotation(frame.rotation),
            timestamp_us: frame.timestamp_us,
            frame_metadata: None,
            buffer,
        });
        Ok(())
    }

    async fn send_stats(&self) -> Option<SendStats> {
        if !matches!(self.source, LocalSource::Video { .. }) {
            return None;
        }
        let track = self.current_track()?;
        let stats = match track.get_stats().await {
            Ok(stats) => stats,
            Err(error) => {
                log::debug!("could not read send stats: {error}");
                return None;
            }
        };
        // One `OutboundRtp` entry per simulcast encoding (RTX rides inside
        // its media stream's entry, unlike on the receive side).
        // Every layer shares one codec; read it off an active layer, since a
        // paused one may still name the codec it last used.
        let codec = stats
            .iter()
            .filter_map(|entry| match entry {
                RtcStats::OutboundRtp(outbound) => Some(outbound),
                _ => None,
            })
            .max_by_key(|outbound| outbound.outbound.active)
            .and_then(|outbound| codec_mime(&stats, &outbound.stream.codec_id));
        let mut layers: Vec<SendLayerStats> = stats
            .iter()
            .filter_map(|entry| match entry {
                RtcStats::OutboundRtp(outbound) => Some(SendLayerStats {
                    rid: outbound.outbound.rid.clone(),
                    frame_width: outbound.outbound.frame_width,
                    frame_height: outbound.outbound.frame_height,
                    frames_per_second: outbound.outbound.frames_per_second,
                    frames_encoded: u64::from(outbound.outbound.frames_encoded),
                    bytes_sent: outbound.sent.bytes_sent,
                    active: outbound.outbound.active,
                    quality_limitation: match outbound.outbound.quality_limitation_reason {
                        QualityLimitationReason::None => QualityLimitation::None,
                        QualityLimitationReason::Cpu => QualityLimitation::Cpu,
                        QualityLimitationReason::Bandwidth => QualityLimitation::Bandwidth,
                        QualityLimitationReason::Other => QualityLimitation::Other,
                    },
                }),
                _ => None,
            })
            .collect();
        if layers.is_empty() {
            return None;
        }
        // A paused layer reports no current size, so order by the rid
        // LiveKit assigns smallest-first (`q`, `h`, `f`) rather than by height.
        layers.sort_by_key(|layer| match layer.rid.as_str() {
            "q" => 0,
            "h" => 1,
            "f" => 2,
            _ => 3,
        });
        Some(SendStats { layers, codec })
    }
}

/// One currently translated publication per participant and stream kind.
/// Removing an older publication must not erase its replacement.
#[derive(Default)]
struct RemotePublications(HashMap<(String, MediaStreamKind), String>);

impl RemotePublications {
    fn added(&mut self, identity: &str, kind: MediaStreamKind, sid: String) {
        self.0.insert((identity.to_owned(), kind), sid);
    }

    fn removed(&mut self, identity: &str, kind: MediaStreamKind, sid: &str) -> bool {
        let key = (identity.to_owned(), kind);
        if self.0.get(&key).is_some_and(|current| current == sid) {
            self.0.remove(&key);
            true
        } else {
            false
        }
    }

    fn participant_left(&mut self, identity: &str) {
        self.0.retain(|(owner, _), _| owner != identity);
    }
}

/// Translate the LiveKit room event stream into the transport-neutral
/// [`ConnectionEvent`] vocabulary. Runs until the room closes its channel;
/// the outbound channel closing (engine gone) stops it early.
///
/// `session` is the room the events come from, read for the speaker list.
async fn translate_room_events(
    mut events: UnboundedReceiver<RoomEvent>,
    tx: UnboundedSender<ConnectionEvent>,
    owner: LiveKitTransportConnection,
    session: std::sync::Weak<LiveKitSession>,
) {
    let e2ee = owner.shared.connector.e2ee;
    let mut publications = RemotePublications::default();
    while let Some(event) = events.recv().await {
        let translated = match event {
            RoomEvent::ParticipantConnected(participant) => Some(ConnectionEvent::RemoteJoined {
                identity: participant.identity().to_string(),
            }),
            RoomEvent::ParticipantDisconnected(participant) => {
                let identity = participant.identity().to_string();
                publications.participant_left(&identity);
                Some(ConnectionEvent::RemoteLeft { identity })
            }
            RoomEvent::TrackPublished {
                publication,
                participant,
            } => {
                if owner.wants(participant.identity().as_str()) {
                    publication.set_subscribed(true);
                }
                None
            }
            RoomEvent::TrackSubscribed {
                track,
                publication,
                participant,
            } => {
                let kind = stream_kind(publication.source(), publication.kind());
                // In an encrypted call livekit attaches no frame cryptor to a
                // track declared unencrypted: its frames would be played as
                // they come. The SFU relays that declaration, so it could also
                // inject frames of its own under this name (spec 038 R12).
                if e2ee && publication.encryption_type() == EncryptionType::None {
                    publication.set_subscribed(false);
                    Some(ConnectionEvent::TrackNotEncrypted {
                        identity: participant.identity().to_string(),
                        kind,
                    })
                } else {
                    let identity = participant.identity().to_string();
                    publications.added(&identity, kind, String::from(publication.sid()));
                    Some(ConnectionEvent::TrackAdded {
                        identity,
                        kind,
                        track: Arc::new(LiveKitRemoteTrack { kind, track }),
                    })
                }
            }
            // A track refused as unencrypted was never added.
            RoomEvent::TrackUnsubscribed { publication, .. }
                if e2ee && publication.encryption_type() == EncryptionType::None =>
            {
                None
            }
            RoomEvent::TrackUnsubscribed {
                publication,
                participant,
                ..
            } => {
                let identity = participant.identity().to_string();
                let kind = stream_kind(publication.source(), publication.kind());
                let sid = String::from(publication.sid());
                publications.removed(&identity, kind, &sid)
                    .then_some(ConnectionEvent::TrackRemoved { identity, kind })
            }
            RoomEvent::TrackMuted {
                participant,
                publication,
            } => remote_mute_event(&participant, &publication, true),
            RoomEvent::TrackUnmuted {
                participant,
                publication,
            } => remote_mute_event(&participant, &publication, false),
            RoomEvent::ActiveSpeakersChanged { .. } => {
                // The SFU sends speaker *changes* (who started, whose level
                // moved, who stopped) and the room re-emits each as if it were
                // the whole list, so a speaker holding a steady level is
                // missing from it. The engine reads the list as a snapshot, so
                // it is rebuilt from every participant's flag, which the room
                // keeps per change. `audio_level` rides along: without it a
                // host has to meter the PCM itself to answer "how loud".
                session.upgrade().map(|session| {
                    let room = session.room();
                    let local = room.local_participant();
                    let mut speakers: Vec<SpeakingParticipant> = room
                        .remote_participants()
                        .values()
                        .filter(|p| p.is_speaking())
                        .map(|p| SpeakingParticipant {
                            identity: p.identity().to_string(),
                            level: p.audio_level(),
                        })
                        .collect();
                    if local.is_speaking() {
                        speakers.push(SpeakingParticipant {
                            identity: local.identity().to_string(),
                            level: local.audio_level(),
                        });
                    }
                    ConnectionEvent::ActiveSpeakers { speakers }
                })
            }
            RoomEvent::Reconnecting => Some(ConnectionEvent::Reconnecting),
            RoomEvent::Reconnected => {
                // When LiveKit gives up resuming it restarts the room itself and
                // republishes our tracks on new peer connections, before this
                // event, each with a new frame cryptor at index 0. Nothing of
                // ours publishes them, so nothing else moves them onto the
                // current index: after a rotation, peers that joined since held
                // no key for index 0 and saw none of our media. After a resume,
                // which keeps the cryptors, this changes nothing.
                owner.reassert_local_key_index();
                Some(ConnectionEvent::Reconnected)
            }
            RoomEvent::Disconnected { reason } => Some(ConnectionEvent::Closed {
                message: format!("{reason:?}"),
            }),
            RoomEvent::E2eeStateChanged { participant, state } => {
                // Media that is subscribed but never decodes is invisible
                // without this (MissingKey / DecryptionFailed = the key
                // exchange or identity mapping went wrong for that stream).
                // Forwarded rather than merely logged: a host cannot build
                // diagnostics on our log output.
                encryption_state(state).map(|state| ConnectionEvent::EncryptionStateChanged {
                    identity: participant.identity().to_string(),
                    state,
                })
            }
            // Everything else (data, transcriptions, metadata, local echoes
            // of our own publications, ...) stays LiveKit-internal for now.
            _ => None,
        };

        let Some(event) = translated else { continue };

        match &event {
            // High frequency; would drown everything else at `debug`.
            ConnectionEvent::ActiveSpeakers { .. } => log::trace!("sfu event: {event:?}"),
            _ => log::debug!("sfu event: {event:?}"),
        }

        if tx.send(event).is_err() {
            log::debug!("sfu event stream closed by the engine; stopping the translator");
            return;
        }
    }

    log::debug!("sfu closed its event stream; stopping the translator");
}

/// Mute translation, shared by the muted/unmuted arms. Mute events fire for
/// local tracks too; those map onto our own membership via the identity, so
/// they are forwarded like any other.
fn remote_mute_event(
    participant: &Participant,
    publication: &TrackPublication,
    muted: bool,
) -> Option<ConnectionEvent> {
    let identity = participant.identity().to_string();
    let kind = stream_kind(publication.source(), publication.kind());
    Some(if muted {
        ConnectionEvent::TrackMuted { identity, kind }
    } else {
        ConnectionEvent::TrackUnmuted { identity, kind }
    })
}

/// The LiveKit track source a publication of `kind` is announced under
/// (the reverse of [`stream_kind`]).
fn livekit_track_source(kind: MediaStreamKind) -> TrackSource {
    match kind {
        MediaStreamKind::Microphone => TrackSource::Microphone,
        MediaStreamKind::Camera => TrackSource::Camera,
        MediaStreamKind::ScreenShare => TrackSource::Screenshare,
        MediaStreamKind::ScreenShareAudio => TrackSource::ScreenshareAudio,
        MediaStreamKind::Data => TrackSource::Unknown,
    }
}

/// The size to ask the SFU for under `detail`, given the publisher's full
/// `source` size; `None` for `Auto`, or a quality while the source is unknown.
/// A quality is a fraction of the source matching livekit's default simulcast
/// layers (full, half, quarter): the SFU forwards the layer that fits.
fn requested_size(detail: VideoDetail, source: Option<(u32, u32)>) -> Option<(u32, u32)> {
    match detail {
        VideoDetail::Auto => None,
        VideoDetail::Dimensions(dimensions) => Some((dimensions.width, dimensions.height)),
        VideoDetail::Quality(limit) => {
            let (width, height) = source?;
            let divisor = match limit {
                QualityLimit::High => 1,
                QualityLimit::Medium => 2,
                QualityLimit::Low => 4,
            };
            Some(((width / divisor).max(1), (height / divisor).max(1)))
        }
    }
}

/// Map LiveKit's track source/kind pair onto the transport-neutral stream
/// kind. `Unknown` sources fall back on the media kind.
fn stream_kind(source: TrackSource, kind: TrackKind) -> MediaStreamKind {
    match source {
        TrackSource::Microphone => MediaStreamKind::Microphone,
        TrackSource::Camera => MediaStreamKind::Camera,
        TrackSource::Screenshare => MediaStreamKind::ScreenShare,
        TrackSource::ScreenshareAudio => MediaStreamKind::ScreenShareAudio,
        TrackSource::Unknown => match kind {
            TrackKind::Audio => MediaStreamKind::Microphone,
            TrackKind::Video => MediaStreamKind::Camera,
        },
    }
}

/// The MIME type of the codec entry `codec_id` refers to in the same stats
/// report, if the report has one and names a type.
fn codec_mime(stats: &[RtcStats], codec_id: &str) -> Option<String> {
    if codec_id.is_empty() {
        return None;
    }
    stats.iter().find_map(|entry| match entry {
        RtcStats::Codec(codec) if codec.rtc.id == codec_id && !codec.codec.mime_type.is_empty() => {
            Some(codec.codec.mime_type.clone())
        }
        _ => None,
    })
}

/// Map the frame cryptor's state onto the transport-neutral vocabulary.
///
/// `None` for the two states that carry no diagnostic value: `New` is the
/// cryptor's initial state (fired before anything has been attempted), and
/// `KeyRatcheted` cannot occur with the MSC4195 per-participant key provider,
/// which never ratchets.
fn encryption_state(state: LkEncryptionState) -> Option<FrameEncryptionState> {
    match state {
        LkEncryptionState::Ok => Some(FrameEncryptionState::Ok),
        LkEncryptionState::MissingKey => Some(FrameEncryptionState::MissingKey),
        LkEncryptionState::DecryptionFailed => Some(FrameEncryptionState::DecryptionFailed),
        LkEncryptionState::EncryptionFailed => Some(FrameEncryptionState::EncryptionFailed),
        LkEncryptionState::InternalError => Some(FrameEncryptionState::InternalError),
        LkEncryptionState::New => None,
        LkEncryptionState::KeyRatcheted => {
            log::warn!("frame cryptor reported a key ratchet, which MSC4195 never asks for");
            None
        }
    }
}

/// A subscribed LiveKit track behind the transport-neutral handle.
struct LiveKitRemoteTrack {
    kind: MediaStreamKind,
    track: RemoteTrack,
}

#[async_trait]
impl RemoteTrackHandle for LiveKitRemoteTrack {
    fn kind(&self) -> MediaStreamKind {
        self.kind
    }

    fn audio_frames(&self) -> Option<futures_util::stream::BoxStream<'static, AudioFrame>> {
        let RemoteTrack::Audio(track) = &self.track else {
            return None;
        };
        let stream = NativeAudioStream::new(track.rtc_track(), AUDIO_SAMPLE_RATE, AUDIO_CHANNELS);
        Some(
            stream
                .map(|frame| AudioFrame {
                    data: frame.data.into_owned(),
                    sample_rate: frame.sample_rate,
                    num_channels: frame.num_channels,
                    samples_per_channel: frame.samples_per_channel,
                })
                .boxed(),
        )
    }

    fn video_frames(&self) -> Option<futures_util::stream::BoxStream<'static, VideoFrame>> {
        let RemoteTrack::Video(track) = &self.track else {
            return None;
        };
        // The native stream keeps only the latest frame (queue of 1), so a
        // slow consumer drops frames instead of buffering.
        let stream = NativeVideoStream::new(track.rtc_track());
        Some(stream.map(to_media_video_frame).boxed())
    }

    async fn receive_stats(&self) -> Option<ReceiveStats> {
        let stats = match self.track.get_stats().await {
            Ok(stats) => stats,
            Err(error) => {
                log::debug!("could not read receive stats: {error}");
                return None;
            }
        };
        // One RTCP report per SSRC; the RTX and FEC streams get their own
        // `InboundRtp` entries, so sum rather than taking the first.
        let inbound: Vec<_> = stats
            .iter()
            .filter_map(|entry| match entry {
                RtcStats::InboundRtp(inbound) => Some(inbound),
                _ => None,
            })
            .collect();
        if inbound.is_empty() {
            // Before the first RTCP report there is nothing to report; a
            // caller polling early gets `None` rather than a misleading zero.
            return None;
        }
        Some(ReceiveStats {
            packets_received: inbound.iter().map(|s| s.received.packets_received).sum(),
            packets_lost: inbound.iter().map(|s| s.received.packets_lost).sum(),
            bytes_received: inbound.iter().map(|s| s.inbound.bytes_received).sum(),
            // Jitter is a per-stream measurement, not a total: report the
            // worst of them.
            jitter: inbound
                .iter()
                .map(|s| s.received.jitter)
                .fold(0.0_f64, f64::max),
            frames_decoded: inbound
                .iter()
                .map(|s| u64::from(s.inbound.frames_decoded))
                .sum(),
            frames_dropped: inbound
                .iter()
                .map(|s| u64::from(s.inbound.frames_dropped))
                .sum(),
            total_samples_received: inbound
                .iter()
                .map(|s| s.inbound.total_samples_received)
                .sum(),
            concealed_samples: inbound.iter().map(|s| s.inbound.concealed_samples).sum(),
            silent_concealed_samples: inbound
                .iter()
                .map(|s| s.inbound.silent_concealed_samples)
                .sum(),
            concealment_events: inbound.iter().map(|s| s.inbound.concealment_events).sum(),
            // The media stream is the busiest one; RTX and FEC may name their
            // own codec entries.
            codec: inbound
                .iter()
                .max_by_key(|s| s.received.packets_received)
                .and_then(|s| codec_mime(&stats, &s.stream.codec_id)),
        })
    }
}

/// Convert a decoded LiveKit frame into the owned, transport-neutral I420
/// frame (decoder buffers may be NV12/native; normalize once here).
fn to_media_video_frame(frame: livekit::webrtc::video_frame::BoxVideoFrame) -> VideoFrame {
    let i420 = frame.buffer.to_i420();
    let (stride_y, stride_u, stride_v) = i420.strides();
    let (data_y, data_u, data_v) = i420.data();
    VideoFrame {
        buffer: I420Buffer {
            width: i420.width(),
            height: i420.height(),
            data_y: data_y.to_vec(),
            stride_y,
            data_u: data_u.to_vec(),
            stride_u,
            data_v: data_v.to_vec(),
            stride_v,
        },
        rotation: match frame.rotation {
            LkVideoRotation::VideoRotation0 => VideoRotation::Deg0,
            LkVideoRotation::VideoRotation90 => VideoRotation::Deg90,
            LkVideoRotation::VideoRotation180 => VideoRotation::Deg180,
            LkVideoRotation::VideoRotation270 => VideoRotation::Deg270,
        },
        timestamp_us: frame.timestamp_us,
    }
}

fn to_livekit_rotation(rotation: VideoRotation) -> LkVideoRotation {
    match rotation {
        VideoRotation::Deg0 => LkVideoRotation::VideoRotation0,
        VideoRotation::Deg90 => LkVideoRotation::VideoRotation90,
        VideoRotation::Deg180 => LkVideoRotation::VideoRotation180,
        VideoRotation::Deg270 => LkVideoRotation::VideoRotation270,
    }
}

/// Copy an application-provided I420 buffer into a libwebrtc one, honouring
/// both sides' strides. Errors (rather than panicking) on inconsistent plane
/// sizes — the buffer comes from the application.
fn to_libwebrtc_i420(buffer: &I420Buffer) -> Result<LkI420Buffer, TransportError> {
    let mut out = LkI420Buffer::new(buffer.width, buffer.height);
    let (dst_stride_y, dst_stride_u, dst_stride_v) = out.strides();
    let (dst_y, dst_u, dst_v) = out.data_mut();

    let chroma_width = buffer.width.div_ceil(2) as usize;
    let chroma_height = buffer.height.div_ceil(2) as usize;
    copy_plane(
        dst_y,
        dst_stride_y,
        &buffer.data_y,
        buffer.stride_y,
        buffer.width as usize,
        buffer.height as usize,
    )?;
    copy_plane(
        dst_u,
        dst_stride_u,
        &buffer.data_u,
        buffer.stride_u,
        chroma_width,
        chroma_height,
    )?;
    copy_plane(
        dst_v,
        dst_stride_v,
        &buffer.data_v,
        buffer.stride_v,
        chroma_width,
        chroma_height,
    )?;
    Ok(out)
}

fn copy_plane(
    dst: &mut [u8],
    dst_stride: u32,
    src: &[u8],
    src_stride: u32,
    width: usize,
    rows: usize,
) -> Result<(), TransportError> {
    let dst_stride = dst_stride as usize;
    let src_stride = src_stride as usize;
    if width > src_stride
        || rows.saturating_sub(1) * src_stride + width > src.len()
        || rows.saturating_sub(1) * dst_stride + width > dst.len()
    {
        return Err(TransportError::Unsupported(
            "video frame plane does not match its declared dimensions".into(),
        ));
    }
    for row in 0..rows {
        dst[row * dst_stride..][..width].copy_from_slice(&src[row * src_stride..][..width]);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn publication_owner_is_native_room_identity_not_focus_equality() {
        let old = Arc::new("same-focus");
        let replacement = Arc::new("same-focus");
        assert!(publication_on_current_room(&old, Some(&old.clone())));
        assert!(!publication_on_current_room(&old, Some(&replacement)));
        assert!(!publication_on_current_room(&old, None));
        assert!(publication_on_current_room(&replacement, Some(&replacement)));
    }

    #[test]
    fn reconnect_cannot_succeed_after_republish_failure_or_supersession() {
        assert!(reconnect_publication_result(true, Ok(())).is_ok());
        assert!(matches!(
            reconnect_publication_result(true, Err(TransportError::Connect("publish failed".into()))),
            Err(TransportError::Connect(_))
        ));
        assert!(matches!(reconnect_publication_result(false, Ok(())), Err(TransportError::Closed(_))));
        assert!(matches!(
            reconnect_publication_result(false, Err(TransportError::Connect("old room".into()))),
            Err(TransportError::Closed(_))
        ));
    }

    #[test]
    fn publication_failure_is_scoped_to_the_installed_room() {
        use PublicationFailure::*;
        for initial in [false, true] {
            for current in [false, true] {
                assert_eq!(publication_failure(false, current, initial), Closed);
            }
            assert_eq!(publication_failure(true, false, initial), Superseded);
        }
        assert_eq!(publication_failure(true, true, true), Initial);
        assert_eq!(publication_failure(true, true, false), Republish);
    }

    #[test]
    fn old_publication_cannot_remove_replacement() {
        let mut publications = RemotePublications::default();
        let kind = MediaStreamKind::Camera;
        publications.added("alice", kind, "old".into());
        publications.added("alice", kind, "new".into());
        assert!(!publications.removed("alice", kind, "old"));
        assert!(publications.removed("alice", kind, "new"));
        assert!(!publications.removed("alice", kind, "new"));
    }

    #[test]
    fn publication_removal_is_scoped_to_participant_and_kind() {
        let mut publications = RemotePublications::default();
        publications.added("alice", MediaStreamKind::Camera, "camera".into());
        publications.added("alice", MediaStreamKind::Microphone, "mic".into());
        publications.added("bob", MediaStreamKind::Camera, "bob-camera".into());
        assert!(!publications.removed("alice", MediaStreamKind::Microphone, "camera"));
        assert!(!publications.removed("bob", MediaStreamKind::Camera, "camera"));
        publications.participant_left("alice");
        assert!(!publications.removed("alice", MediaStreamKind::Camera, "camera"));
        assert!(!publications.removed("alice", MediaStreamKind::Microphone, "mic"));
        assert!(publications.removed("bob", MediaStreamKind::Camera, "bob-camera"));
    }

    fn codec_entry(id: &str, mime_type: &str) -> RtcStats {
        let mut codec = livekit::webrtc::stats::CodecStats::default();
        codec.rtc.id = id.to_owned();
        codec.codec.mime_type = mime_type.to_owned();
        RtcStats::Codec(codec)
    }

    #[test]
    fn a_codec_is_named_by_the_entry_its_id_refers_to() {
        let stats = vec![
            codec_entry("COT01_96", "video/VP8"),
            codec_entry("CIT01_98", "video/VP9"),
            codec_entry("CIT01_100", ""),
        ];
        assert_eq!(codec_mime(&stats, "CIT01_98").as_deref(), Some("video/VP9"));
        assert_eq!(codec_mime(&stats, "CIT01_100"), None);
        assert_eq!(codec_mime(&stats, "CIT01_102"), None);
        assert_eq!(codec_mime(&stats, ""), None);
    }

    #[test]
    fn a_quality_is_a_fraction_of_the_source_size() {
        let source = Some((960, 720));
        let quality = |limit| requested_size(VideoDetail::Quality(limit), source);
        assert_eq!(quality(QualityLimit::High), Some((960, 720)));
        assert_eq!(quality(QualityLimit::Medium), Some((480, 360)));
        assert_eq!(quality(QualityLimit::Low), Some((240, 180)));
    }

    #[test]
    fn a_size_is_asked_as_is_and_auto_asks_nothing() {
        let dimensions = VideoDetail::Dimensions(matrix_rtc_transport::Dimensions {
            width: 320,
            height: 240,
        });
        assert_eq!(requested_size(dimensions, None), Some((320, 240)));
        assert_eq!(requested_size(VideoDetail::Auto, Some((960, 720))), None);
    }

    #[test]
    fn a_quality_without_a_known_source_asks_no_size() {
        assert_eq!(
            requested_size(VideoDetail::Quality(QualityLimit::High), None),
            None
        );
        assert_eq!(
            requested_size(VideoDetail::Quality(QualityLimit::Low), Some((2, 2))),
            Some((1, 1))
        );
    }

    fn signal_client_error(status: u16) -> crate::Error {
        crate::Error::Room(livekit::RoomError::Engine(
            livekit::rtc_engine::EngineError::Signal(SignalError::Client(
                reqwest::StatusCode::from_u16(status).unwrap(),
                "refused".into(),
            )),
        ))
    }

    /// A restarted SFU has forgotten the room our token names (LiveKit keeps
    /// rooms in memory, and `auto_create` is off where the token service
    /// creates them): a fresh token brings it back, so it is no refusal.
    #[test]
    fn a_forgotten_room_is_the_token_not_a_refusal() {
        assert!(matches!(
            classify_room_error(signal_client_error(404)),
            TransportError::TokenRejected(_)
        ));
        assert!(matches!(
            classify_room_error(signal_client_error(401)),
            TransportError::TokenRejected(_)
        ));
        assert!(matches!(
            classify_room_error(signal_client_error(403)),
            TransportError::Refused(_)
        ));
    }

    fn cs_api_config() -> LiveKitTransportConfig {
        LiveKitTransportConfig {
            target: LiveKitTarget::CsApi {
                url: "wss://sfu.other.org".into(),
                server_name: "other.org".into(),
            },
            room_id: "!room:example.org".into(),
            slot_id: "m.call#room".into(),
            member_id: "member-1".into(),
            device_id: "DEVICE".into(),
        }
    }

    fn test_token(label: &str) -> SfuToken {
        SfuToken { jwt: label.into(), url: format!("wss://{label}.example.org") }
    }

    #[test]
    fn install_generations_supersede_previous_attempts_and_close_is_terminal() {
        let installs = Mutex::new(Some(0));
        assert_eq!(begin_install(&installs).unwrap(), 1);
        assert_eq!(begin_install(&installs).unwrap(), 2);
        *installs.lock().unwrap() = None;
        assert!(matches!(begin_install(&installs), Err(TransportError::Closed(_))));
        assert_eq!(*installs.lock().unwrap(), None);
    }

    #[tokio::test]
    async fn close_during_token_exchange_rejects_the_result() {
        let installs = Mutex::new(Some(0));
        let config = Mutex::new(cs_api_config());
        let cache = Mutex::new(Some(test_token("existing")));
        let started = begin_install(&installs).unwrap();
        let result = token_for_generation(&installs, started, &config, &cache, true, |_| async {
            *installs.lock().unwrap() = None;
            Ok(test_token("late"))
        }).await;
        assert!(matches!(result, Err(TransportError::Closed(_))));
        assert_eq!(cache.lock().unwrap().as_ref().unwrap().jwt, "existing");
    }

    #[tokio::test]
    async fn obsolete_token_result_cannot_replace_the_new_focus_cache() {
        for fail in [false, true] {
            let installs = Mutex::new(Some(0));
            let config = Mutex::new(cs_api_config());
            let cache = Mutex::new(None);
            let result = token_for_generation(&installs, 0, &config, &cache, true, |_| async {
                let mut generation = installs.lock().unwrap();
                *generation = Some(1);
                config.lock().unwrap().target = LiveKitTarget::CsApi {
                    url: "wss://new.example.org".into(), server_name: "new.example.org".into(),
                };
                *cache.lock().unwrap() = Some(test_token("new"));
                if fail { Err(TransportError::Refused("old focus refused".into())) }
                else { Ok(test_token("old")) }
            }).await;
            assert!(matches!(result, Err(TransportError::Closed(_))));
            assert_eq!(cache.lock().unwrap().as_ref().unwrap().jwt, "new");
        }
    }

    #[tokio::test]
    async fn stale_attempt_cannot_use_new_cache_and_current_attempt_can_refresh_it() {
        let installs = Mutex::new(Some(1));
        let config = Mutex::new(cs_api_config());
        let cache = Mutex::new(Some(test_token("cached")));
        for (generation, expected) in [(0, false), (1, true)] {
            let result = token_for_generation(&installs, generation, &config, &cache, false, |_| async {
                panic!("cached or abandoned attempts must not request a token");
            }).await;
            assert_eq!(result.is_ok(), expected);
        }
        let result = token_for_generation(&installs, 1, &config, &cache, true, |_| async {
            Ok(test_token("fresh"))
        }).await.unwrap();
        assert_eq!(result.jwt, "fresh");
        assert_eq!(cache.lock().unwrap().as_ref().unwrap().jwt, "fresh");
    }

    /// A transport naming only an SFU `url` gets its token from the homeserver,
    /// for that SFU's server, and connects to that very `url`: the answer
    /// carries no SFU URL of its own.
    #[tokio::test]
    async fn a_url_only_target_asks_the_homeserver_for_its_sfu() {
        let backend = matrix_rtc_core::testing::MockBackend::new();
        let token = acquire_token(&reqwest::Client::new(), &cs_api_config(), &backend)
            .await
            .expect("a token");
        assert_eq!(token.jwt, "mock-jwt");
        assert_eq!(token.url, "wss://sfu.other.org");
        assert_eq!(
            *backend.livekit_token_requests.lock().unwrap(),
            [matrix_rtc_core::LiveKitTokenRequest {
                server_name: Some("other.org".into()),
                url: "wss://sfu.other.org".into(),
                room_id: "!room:example.org".into(),
                slot_id: "m.call#room".into(),
                member_id: "member-1".into(),
            }]
        );
    }

    /// A homeserver without MSC4195 ends the call (spec 038 R44); another
    /// refusal is one; a rate limit or a server error may pass.
    #[tokio::test]
    async fn a_homeserver_without_the_endpoint_is_told_from_a_refusal() {
        let backend = matrix_rtc_core::testing::MockBackend::new();
        for (errcode, status, expected) in [
            (None, 404, "unsupported"),
            (Some("M_UNRECOGNIZED"), 404, "unsupported"),
            (Some("M_FORBIDDEN"), 403, "refused"),
            (None, 429, "connect"),
            (None, 502, "connect"),
        ] {
            *backend.livekit_token_answer.lock().unwrap() =
                Err(matrix_rtc_core::BackendError::with_matrix_error(
                    errcode.map(str::to_owned),
                    Some(status),
                    "no",
                ));
            let error = acquire_token(&reqwest::Client::new(), &cs_api_config(), &backend)
                .await
                .expect_err("no token");
            let kind = match classify_token_error(error) {
                TransportError::TokenEndpointUnsupported(_) => "unsupported",
                TransportError::Refused(_) => "refused",
                TransportError::Connect(_) => "connect",
                other => panic!("{other:?}"),
            };
            assert_eq!(kind, expected, "{errcode:?} / {status}");
        }
    }
}
