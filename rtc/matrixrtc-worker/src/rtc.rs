//! Authenticated, E2EE-aware RTC call path. No new Matrix users or devices
//! are created during a call; provisioning is an explicit offline step.
#[path = "participant_publisher.rs"]
mod participant_publisher;
#[path = "group_publication_admission.rs"]
mod group_publication_admission;
#[path = "group_publication_session.rs"]
mod group_publication_session;
#[path = "group_publication_router.rs"]
mod group_publication_router;
use crate::{audio, protocol::{self, Message}, send, video, Writer};
use futures_util::StreamExt;
use matrix_rtc_call::{CallEnd, CallIntent, CallKind, CallStatus, GroupNotify, RingConfig, notification::Mentions};
use matrix_rtc_call_sdk::{refresh_user_keys, CallKeyRecipients, CallEvent, ConnectionState, LiveKitCall, LiveKitCallOptions, Participant, SdkMatrixBackend, TrustedFocus};
use matrix_rtc_call::{RtcClient, RtcRoom};
use matrix_rtc_core::{RoomOptions, SlotState, RawSlotEventContent, JoinedMembership, IdentitySubject, RtcTransport, Outbox};
use matrix_sdk::ruma::{EventId, api::client::state::get_state_events};
use matrix_rtc_core::{SlotEncryption, compat::MembershipFormat, wire_event_type};
use matrix_rtc_transport::{LocalTrackHandle, RemoteTrackHandle, AudioFrame, MediaStreamKind, PublishOptions, ReceiveStats, VideoSourceConfig, livekit::identity_for};
use matrix_sdk::{Client, encryption::EncryptionSettings, authentication::matrix::MatrixSession, ruma::{RoomId, UserId}, RoomMemberships};
use serde_json::json;
use matrix_sdk_ui::sync_service::SyncService;
use sha2::{Digest, Sha256};
use std::{collections::HashMap, io, path::{Path, PathBuf}, sync::{Arc, atomic::{AtomicBool, AtomicU64, Ordering}}, time::Duration};
use tokio::{io::ReadHalf, net::UnixStream, sync::Mutex};

struct RemoteForwarder {
    track: Arc<dyn RemoteTrackHandle>,
    task: tokio::task::JoinHandle<io::Result<()>>,
}
struct AbortOnDrop(HashMap<String,RemoteForwarder>);
impl Drop for AbortOnDrop {
    fn drop(&mut self) { for forwarder in self.0.values() { forwarder.task.abort(); } }
}

fn track_replaced<T: ?Sized>(previous: &Arc<T>, current: Option<&Arc<T>>) -> bool {
    current.is_none_or(|track| !Arc::ptr_eq(previous, track))
}
// Keep a retry budget per actual track, not per transient roster event.
const REMOTE_RETRY_LIMIT: u8 = 2;
const REMOTE_RETRY_DELAY: Duration = Duration::from_secs(1);
#[derive(Default)]
struct RemoteRetry {
    attempts: u8,
    not_before: Option<tokio::time::Instant>,
}
impl RemoteRetry {
    fn failed(&mut self, now: tokio::time::Instant) -> io::Result<()> {
        if self.attempts >= REMOTE_RETRY_LIMIT {
            return Err(error("remote media stream repeatedly ended or could not attach"));
        }
        self.attempts += 1;
        self.not_before = Some(now + REMOTE_RETRY_DELAY);
        Ok(())
    }
    fn ready(&self, now: tokio::time::Instant) -> bool {
        self.not_before.is_none_or(|deadline| now >= deadline)
    }
}
struct RemoteRecovery {
    track: Arc<dyn RemoteTrackHandle>,
    retry: RemoteRetry,
}
struct MediaGate(Arc<AtomicBool>);
impl Drop for MediaGate {
    fn drop(&mut self) { self.0.store(false, Ordering::Release); }
}
// A cached SDK client owns its original database and session. Never silently
// use it for a replacement identity on disk (or open a second competing client).
#[derive(Clone, PartialEq, Eq)]
struct IdentitySnapshot {
    session_digest: [u8; 32],
    store_device: u64,
    store_inode: u64,
}
struct IdentityFence {
    folder: PathBuf,
    original: IdentitySnapshot,
    invalidated: AtomicBool,
}
impl IdentityFence {
    fn new(folder: PathBuf, original: IdentitySnapshot) -> Self {
        Self { folder, original, invalidated: AtomicBool::new(false) }
    }
    fn require_current(&self) -> io::Result<()> {
        if self.invalidated.load(Ordering::Acquire) {
            return Err(error("ghost identity changed or became unavailable while worker was running; restart worker after preparation"));
        }
        Ok(())
    }
    async fn check(&self) -> io::Result<()> {
        self.require_current()?;
        // Once observed, replacement or loss invalidates this cached SDK
        // client even if the original files are subsequently restored.
        if !matches!(identity_snapshot(&self.folder).await, Ok(current) if current == self.original) {
            self.invalidated.store(true, Ordering::Release);
        }
        self.require_current()
    }
}
struct Ghost {
    client: Client,
    _sync: SyncService,
    identity: IdentityFence,
    outbox: Outbox,
    journal: Arc<crate::legacy_leave::Journal>,
    // Held for the lifetime of the cached SDK client and its sync service.
    _ownership: (std::fs::File, std::fs::File),
}
pub struct Accounts {
    root: PathBuf,
    clients: Mutex<HashMap<String, Arc<tokio::sync::OnceCell<Arc<Ghost>>>>>,
    slots: std::sync::Mutex<HashMap<(String,String), std::sync::Weak<Mutex<()>>>>,
}

fn error(reason: impl Into<String>) -> io::Error { io::Error::other(reason.into()) }

// The gateway waits 120s for an incoming answer and 60s after an
// outgoing WhatsApp pickup. Reserve ten seconds for socket delivery and
// teardown rather than starting a new 100s timer after the SDK join.
const INCOMING_ANSWER_BUDGET: Duration = Duration::from_secs(110);
const OUTGOING_ANSWER_BUDGET: Duration = Duration::from_secs(50);
// Local upper bound only; the gateway may expire its invitation sooner.
const OUTGOING_ADMISSION_BUDGET: Duration = Duration::from_secs(50);
const JOIN_BUDGET: Duration = Duration::from_secs(35);

fn check_answer_deadline(deadline: tokio::time::Instant) -> io::Result<()> {
    if tokio::time::Instant::now() >= deadline {
        return Err(error("RTC answer setup deadline exceeded"));
    }
    Ok(())
}

// Network and store operations during ringing and pickup must share the
// corresponding original deadline. Even an immediately ready operation cannot
// authorize progress after expiry.
async fn within_answer_budget<T>(
    deadline: tokio::time::Instant,
    operation: impl std::future::Future<Output = io::Result<T>>,
) -> io::Result<T> {
    check_answer_deadline(deadline)?;
    let result = tokio::time::timeout_at(deadline, operation).await
        .map_err(|_| error("RTC answer setup deadline exceeded"))??;
    check_answer_deadline(deadline)?;
    Ok(result)
}

fn join_deadline(now: tokio::time::Instant, answer_deadline: tokio::time::Instant) -> tokio::time::Instant {
    (now + JOIN_BUDGET).min(answer_deadline)
}

// A media-control command gets one absolute budget, including any sequential
// retraction, publication and mute operations. Before pickup it must also fit
// inside the original answer deadline. A timeout ends the session rather than
// retrying a possibly submitted SDK operation with an ambiguous outcome.
const MEDIA_CONTROL_BUDGET: Duration = Duration::from_secs(10);

fn media_control_deadline(now: tokio::time::Instant, answered: bool,
    answer_deadline: tokio::time::Instant) -> tokio::time::Instant {
    let limit = now + MEDIA_CONTROL_BUDGET;
    if answered { limit } else { limit.min(answer_deadline) }
}

async fn bounded_media_control<T>(deadline: tokio::time::Instant,
    operation: impl std::future::Future<Output = io::Result<T>>) -> io::Result<T> {
    let expired = || io::Error::new(io::ErrorKind::TimedOut, "RTC media control timed out");
    if tokio::time::Instant::now() >= deadline { return Err(expired()); }
    let value = tokio::time::timeout_at(deadline, operation).await
        .map_err(|_| expired())??;
    if tokio::time::Instant::now() >= deadline { return Err(expired()); }
    Ok(value)
}

// One in-flight 60 ms packet, no additional backlog. Poll capture alongside
// IPC and membership events instead of awaiting six native frames in the IPC arm.
const AUDIO_CAPTURE_BUDGET: Duration = Duration::from_millis(250);
type PendingAudio = std::pin::Pin<Box<dyn std::future::Future<Output = io::Result<()>> + Send>>;

fn capture_audio_packet(track: Arc<dyn LocalTrackHandle>, samples: Vec<i16>) -> PendingAudio {
    Box::pin(async move {
        tokio::time::timeout(AUDIO_CAPTURE_BUDGET, async {
            for part in samples.chunks_exact(audio::RTC_FRAME) {
                track.capture_audio(AudioFrame {
                    data: part.to_vec(),
                    sample_rate: 48_000,
                    num_channels: 1,
                    samples_per_channel: audio::RTC_FRAME as u32,
                }).await.map_err(io::Error::other)?;
            }
            Ok::<(), io::Error>(())
        }).await.map_err(|_| error("native microphone capture timed out"))?
    })
}

async fn poll_audio_capture(pending: &mut Option<PendingAudio>) -> io::Result<()> {
    match pending.as_mut() {
        Some(capture) => capture.as_mut().await,
        None => std::future::pending().await,
    }
}

fn trusted_focus() -> io::Result<TrustedFocus> {
    let server_name = std::env::var("MATRIXRTC_SERVER_NAME")
        .map_err(|_| error("MATRIXRTC_SERVER_NAME is required for RTC focus authorization"))?;
    TrustedFocus::local_deployment(&server_name).map_err(error)
}

fn membership_format(message: &Message) -> io::Result<MembershipFormat> {
    match message.membership_format.as_str() {
        "msc" => Ok(MembershipFormat::Msc),
        "room_state" => Ok(MembershipFormat::RoomState),
        _ => Err(error("unsupported MatrixRTC membership format")),
    }
}


fn required_secret(name: &str) -> io::Result<String> {
    let file_name = format!("{name}_FILE");
    if let Ok(path) = std::env::var(&file_name) {
        let path = PathBuf::from(path);
        if !path.is_absolute() {
            return Err(error(format!("{file_name} must be an absolute path")));
        }
        let metadata = std::fs::metadata(&path).map_err(|err| {
            io::Error::new(err.kind(), format!("cannot read {file_name} {}: {err}", path.display()))
        })?;
        use std::os::unix::fs::PermissionsExt;
        if metadata.permissions().mode() & 0o077 != 0 {
            return Err(error(format!("{} must have mode 0600 or stricter", path.display())));
        }
        let value = std::fs::read_to_string(&path)?.trim().to_owned();
        if value.is_empty() {
            return Err(error(format!("{file_name} points to an empty file")));
        }
        return Ok(value);
    }
    std::env::var(name).map_err(|_| error(format!("{name} or {file_name} is required")))
}

fn identity_folder(root: &Path, ghost_mxid: &str) -> PathBuf {
    let hash = hex::encode(Sha256::digest(ghost_mxid.as_bytes()));
    root.join(hash)
}

async fn identity_snapshot(folder: &Path) -> io::Result<IdentitySnapshot> {
    use std::os::unix::fs::{MetadataExt, PermissionsExt};
    let session = folder.join("session.json");
    let session_meta = tokio::fs::symlink_metadata(&session).await?;
    if !session_meta.is_file() || session_meta.permissions().mode() & 0o077 != 0 {
        return Err(error("ghost session.json must be a regular file with mode 0600 or stricter"));
    }
    let crypto_dir = folder.join("crypto");
    if !tokio::fs::symlink_metadata(folder).await?.is_dir()
        || !tokio::fs::symlink_metadata(&crypto_dir).await?.is_dir() {
        return Err(error("ghost identity and crypto directories must not be linked or replaced"));
    }
    let crypto = crypto_dir.join("matrix-sdk-crypto.sqlite3");
    let meta = tokio::fs::symlink_metadata(&crypto).await?;
    if !meta.is_file() || meta.len() == 0 {
        return Err(error("restore the prepared ghost crypto store before calling"));
    }
    let bytes = tokio::fs::read(&session).await?;
    Ok(IdentitySnapshot {
        session_digest: Sha256::digest(&bytes).into(),
        store_device: meta.dev(),
        store_inode: meta.ino(),
    })
}

async fn load_identity_client(root: &Path, ghost_mxid: &str) -> io::Result<Client> {
    let folder = identity_folder(root, ghost_mxid);
    let session_path = folder.join("session.json");
    let meta = tokio::fs::metadata(&session_path).await.map_err(|err| {
        io::Error::new(
            err.kind(),
            format!(
                "ghost identity {ghost_mxid} is not provisioned at {}: {err}",
                session_path.display()
            ),
        )
    })?;
    use std::os::unix::fs::PermissionsExt;
    if meta.permissions().mode() & 0o077 != 0 {
        return Err(error("ghost session.json must have mode 0600 or stricter"));
    }
    let session_bytes = tokio::fs::read(&session_path).await?;
    let session: MatrixSession = serde_json::from_slice(&session_bytes).map_err(io::Error::other)?;
    if session.meta.user_id.as_str() != ghost_mxid {
        return Err(error("stored ghost account does not match requested Matrix identity"));
    }
    let homeserver = std::env::var("MATRIX_HOMESERVER")
        .map_err(|_| error("MATRIX_HOMESERVER is required"))?;
    let passphrase = required_secret("RTC_WORKER_STORE_PASSPHRASE")?;
    let client = Client::builder()
        .homeserver_url(&homeserver)
        .sqlite_store(folder.join("crypto"), Some(&passphrase))
        .with_encryption_settings(EncryptionSettings {
            auto_enable_cross_signing: false,
            ..Default::default()
        })
        .build()
        .await
        .map_err(io::Error::other)?;
    client.restore_session(session).await.map_err(io::Error::other)?;
    client.encryption().wait_for_e2ee_initialization_tasks().await;
    Ok(client)
}

async fn require_prepared_identity(client: &Client) -> io::Result<()> {
    let status = client
        .encryption()
        .cross_signing_status()
        .await
        .ok_or_else(|| error("ghost crypto machine is not initialized"))?;
    if !status.is_complete() {
        return Err(error(
            "ghost MatrixRTC identity is not cross-signing prepared; run matrixrtc-ghost-provision before enabling calls",
        ));
    }
    let own_device = client
        .encryption()
        .get_own_device()
        .await
        .map_err(io::Error::other)?
        .ok_or_else(|| error("ghost MatrixRTC device is missing from the crypto store"))?;
    if !own_device.is_cross_signed_by_owner() {
        return Err(error("ghost MatrixRTC device is not cross-signed by its owner identity"));
    }
    Ok(())
}

pub async fn prepare_identity(root: PathBuf, ghost_mxid: &str) -> io::Result<()> {
    if !ghost_mxid.starts_with('@') || !ghost_mxid.contains(':') {
        return Err(error("prepare-identity requires a full Matrix user ID"));
    }
    let _ownership = crate::file_lock::lock_identity(&identity_folder(&root, ghost_mxid))?;
    let client = load_identity_client(&root, ghost_mxid).await?;
    client
        .encryption()
        .bootstrap_cross_signing_if_needed(None)
        .await
        .map_err(io::Error::other)?;

    let own_device = client
        .encryption()
        .get_own_device()
        .await
        .map_err(io::Error::other)?
        .ok_or_else(|| error("ghost MatrixRTC device is missing from the crypto store"))?;

    if !own_device.is_cross_signed_by_owner() {
        own_device.verify().await.map_err(|err| {
            error(format!("failed to cross-sign ghost MatrixRTC device: {err}"))
        })?;

        let own_user_id = client
            .user_id()
            .ok_or_else(|| error("ghost MatrixRTC client has no user ID after session restore"))?
            .to_owned();

        client
            .encryption()
            .request_user_identity(&own_user_id)
            .await
            .map_err(io::Error::other)?;
    }

    require_prepared_identity(&client).await.map_err(|err| {
        io::Error::other(format!(
            "failed to prepare {ghost_mxid}: {err}. If this ghost already had a different cross-signing identity, recover that identity instead of replacing it"
        ))
    })?;
    Ok(())
}

impl Accounts {
    pub fn new(root: PathBuf) -> Self {
        Self { root, clients: Mutex::new(HashMap::new()), slots: Default::default() }
    }

    fn reserve(&self, room:&str, slot:&str) -> io::Result<tokio::sync::OwnedMutexGuard<()>> {
        let mut slots=self.slots.lock().map_err(|_| error("slot registry unavailable"))?;
        slots.retain(|_, lock| lock.strong_count()!=0);
        let entry=slots.entry((room.to_owned(),slot.to_owned())).or_default();
        let lock=entry.upgrade().unwrap_or_else(|| { let lock=Arc::new(Mutex::new(())); *entry=Arc::downgrade(&lock); lock });
        lock.try_lock_owned().map_err(|_| error("another call already owns this room and slot"))
    }

    async fn get(&self, ghost_mxid: &str) -> io::Result<Arc<Ghost>> {
        let folder = identity_folder(&self.root, ghost_mxid);
        let cell=self.clients.lock().await.entry(ghost_mxid.to_owned()).or_default().clone();
        let ghost=cell.get_or_try_init(|| async {
            let ownership=crate::file_lock::lock_identity(&folder)?;
            let identity=identity_snapshot(&folder).await?;
            let client=load_identity_client(&self.root,ghost_mxid).await?;
            require_prepared_identity(&client).await?;
            if identity_snapshot(&folder).await? != identity {
                return Err(error("ghost identity changed during initialization; retry after preparation"));
            }
            let sync=SyncService::builder(client.clone()).build().await.map_err(io::Error::other)?;
            sync.start().await;
            let journal = Arc::new(crate::legacy_leave::Journal::new(folder.clone())?);
            Ok::<_,io::Error>(Arc::new(Ghost { client, _sync:sync, identity: IdentityFence::new(folder.clone(), identity), outbox: Outbox::with_legacy_journal(journal.clone()), journal, _ownership: ownership }))
        }).await?;
        ghost.identity.check().await?;
        Ok(ghost.clone())
    }

    fn lease_dir(&self)->PathBuf { self.root.parent().unwrap_or(&self.root).join("owned-slots") }
    fn lease_path(&self,req:&Message)->PathBuf {
        self.lease_dir().join(hex::encode(Sha256::digest(format!("{}\0{}",req.room_id,req.slot_id).as_bytes())))
    }

    pub async fn recover(&self) -> io::Result<()> {
        let dir=self.lease_dir();
        tokio::fs::create_dir_all(&dir).await?;
        let mut files=tokio::fs::read_dir(&dir).await?;
        while let Some(file)=files.next_entry().await? {
            let path=file.path();
            if path.extension().is_some() { continue }
            let req = match tokio::fs::read(&path).await
                .and_then(|bytes| serde_json::from_slice::<Message>(&bytes).map_err(io::Error::other)) {
                Ok(req) => req,
                Err(err) => {
                    eprintln!("could not read a MatrixRTC slot lease: {err}");
                    continue;
                }
            };
            if path != self.lease_path(&req) {
                eprintln!("ignoring a MatrixRTC slot lease with a mismatched filename");
                continue;
            }
            let Ok(_owned)=self.reserve(&req.room_id,&req.slot_id) else {continue};
            let work=async {
                // The lease may have been replaced between directory enumeration
                // and acquiring the slot reservation. Do not recover its successor.
                if !lease_matches(&path, &req).await? { return Ok(()); }
                let ghost=self.get(&req.ghost_mxid).await?;
                let room=wait_room(&req,&ghost).await?;
                release_slot(&room,&req,&path).await
            };
            if !matches!(tokio::time::timeout(Duration::from_secs(30),work).await,Ok(Ok(()))) {
                eprintln!("deferred cleanup of a worker-owned MatrixRTC slot");
            }
        }
        Ok(())
    }

}

pub async fn handle(incoming: Message, reader: ReadHalf<UnixStream>, writer: Writer, accounts: Arc<Accounts>, replay: Arc<crate::ReplayGuard>) -> io::Result<()> {
    let mut reader = protocol::FrameReader::new(reader);
    eprintln!("RTC call start: direction={} format={} slot={} member_event={}",
        incoming.kind, incoming.membership_format, incoming.slot_id, incoming.member_event);
    let result = async {
    let _reservation=accounts.reserve(&incoming.room_id,&incoming.slot_id)?;
    match incoming.kind.as_str() {
        "incoming" => run_incoming(&incoming, &mut reader, &writer, &accounts, &replay).await,
        "outgoing" => run_outgoing(&incoming, &mut reader, &writer, &accounts, &replay).await,
        _ => Err(error("unsupported call direction")),
    }}.await;
    if let Err(err) = &result {
        // Never answer WhatsApp if MatrixRTC setup, trust, or media fails.
        let mut reject = Message::new("error", &incoming.call_id);
        reject.reason = err.to_string();
        let _ = send(&writer, &reject).await;
    }
    result
}

// Check the same feature/transport contract as the RTC core before any
// call-specific slot mutation, notification, or WhatsApp dial. Legacy calls
// do not need MSC4354; never silently change the requested membership format.
async fn require_membership_support(client: &Client, format: MembershipFormat) -> io::Result<()> {
    if format != MembershipFormat::Msc {
        return Ok(());
    }
    tokio::time::timeout(Duration::from_secs(10), async {
        let backend = Arc::new(SdkMatrixBackend::new(client.clone()));
        let features = backend.server_features().await.map_err(io::Error::other)?;
        let rtc = RtcClient::new(backend);
        let support = rtc.msc_support(&features).await.map_err(io::Error::other)?;
        if !support.is_supported() {
            return Err(error(format!("homeserver lacks required modern MatrixRTC support: {:?}", support.missing)));
        }
        if !support.warnings.is_empty() {
            eprintln!("MatrixRTC optional homeserver support missing: {:?}", support.warnings);
        }
        Ok(())
    }).await.map_err(|_| error("timed out checking modern MatrixRTC homeserver support"))?
}

async fn synced_room(incoming: &Message, accounts: &Accounts) -> io::Result<(Arc<Ghost>, matrix_sdk::Room)> {
    let ghost = accounts.get(&incoming.ghost_mxid).await?;
    require_prepared_identity(&ghost.client).await?;
    require_membership_support(&ghost.client, membership_format(incoming)?).await?;
    let room=wait_room(incoming,&ghost).await?;
    validate_room(incoming,&room).await?;
    if membership_format(incoming)?.reads_state_membership() {
        use matrix_rtc_core::{MatrixBackend, CommandError, compat::RoomStateDialect};
        let backend = SdkMatrixBackend::new(ghost.client.clone());
        let key = RoomStateDialect::new(backend.own_user_id(), backend.own_device_id(), incoming.room_id.clone(), incoming.slot_id.clone()).state_key();
        tokio::time::timeout(Duration::from_secs(25), ghost.outbox.reconcile_legacy(&incoming.room_id, &key, || async {
            ghost.identity.check().await.map_err(|err| CommandError::ClientRejected(err.to_string()))?;
            ghost.journal.recover_known(&ghost.client, &incoming.room_id, &incoming.slot_id).await
                .map_err(|err| CommandError::ClientRejected(format!("membership reconciliation failed: {err}")))
        })).await.map_err(|_| error("membership reconciliation timed out"))?.map_err(io::Error::other)?;
    }
    Ok((ghost,room))
}

async fn wait_room(incoming:&Message,ghost:&Ghost)->io::Result<matrix_sdk::Room> {
    let room_id=RoomId::parse(&incoming.room_id).map_err(io::Error::other)?;
    tokio::time::timeout(Duration::from_secs(20),async {
        loop {
            if let Some(room)=ghost.client.get_room(&room_id) {return room}
            tokio::time::sleep(Duration::from_millis(150)).await;
        }
    }).await.map_err(|_| error("bridge identity could not sync the call room"))
}

async fn validate_room(incoming:&Message,room:&matrix_sdk::Room)->io::Result<()> {
    if room.state()!=matrix_sdk::RoomState::Joined { return Err(error("bridge identity left the room")) }
    let expected = UserId::parse(&incoming.expected_user_mxid).map_err(io::Error::other)?;
    let own = UserId::parse(&incoming.ghost_mxid).map_err(io::Error::other)?;
    if expected == own { return Err(error("RTC human and bridge identities must differ")); }
    if !room.latest_encryption_state().await.map_err(io::Error::other)?.is_encrypted() {
        return Err(error("RTC calls require an encrypted WhatsApp portal room"));
    }
    let members = room.members(RoomMemberships::JOIN).await.map_err(io::Error::other)?;
    if incoming.group {
        if !members.iter().any(|m| m.user_id().as_str() == expected.as_str())
            || !members.iter().any(|m| m.user_id().as_str() == own.as_str()) {
            return Err(error("group RTC room is missing the expected Matrix user or bridge RTC identity"));
        }
    } else {
        // Only the bot identified by the trusted private gateway may be
        // excluded. Room-controlled functional-member flags are not authority.
        // An older gateway without this field can admit only the exact pair.
        let bot = if incoming.bridge_bot_mxid.is_empty() {
            None
        } else {
            Some(UserId::parse(&incoming.bridge_bot_mxid).map_err(io::Error::other)?)
        };
        if !exclusive_dm_members(
            members.iter().map(|m| m.user_id().as_str()),
            expected.as_str(), own.as_str(), bot.as_ref().map(|id| id.as_str()),
        ) {
            return Err(error("RTC room is not an exclusive two-participant encrypted DM"));
        }
    }
    Ok(())
}

// Require both intended participants even if room metadata labels either as
// functional. Allow at most one additional, gateway-authenticated bridge bot.
fn exclusive_dm_members<'a>(members: impl IntoIterator<Item = &'a str>, expected: &str, own: &str, bot: Option<&str>) -> bool {
    if expected == own { return false; }
    let (mut seen_expected, mut seen_own, mut seen_bot) = (false, false, false);
    for member in members {
        if member == expected && !seen_expected { seen_expected = true; }
        else if member == own && !seen_own { seen_own = true; }
        else if bot == Some(member) && member != expected && member != own && !seen_bot { seen_bot = true; }
        else { return false; }
    }
    seen_expected && seen_own
}

struct PassiveSlot {
    room: RtcRoom<SdkMatrixBackend>,
    members: tokio::sync::watch::Receiver<Vec<JoinedMembership>>,
}

impl PassiveSlot {
    async fn read(room: &matrix_sdk::Room, req: &Message, format: MembershipFormat) -> io::Result<Self> {
        let client = RtcClient::new(Arc::new(SdkMatrixBackend::new(room.client())));
        let room = client.room(req.room_id.clone(), RoomOptions { format })
            .await.map_err(io::Error::other)?;
        tokio::time::timeout(Duration::from_secs(12), room.seeded()).await
            .map_err(|_| error("timed out reading RTC room state"))?;
        if !room.is_seeded() {
            return Err(error("RTC room state unavailable"));
        }
        let members = room.observe(&req.slot_id).await;
        Ok(Self { room, members })
    }
}

struct Admission {
    primary: PassiveSlot,
    alternate: PassiveSlot,
}

impl Admission {
    async fn inspect(room: &matrix_sdk::Room, req: &Message) -> io::Result<Self> {
        let format = membership_format(req)?;
        let other = match format {
            MembershipFormat::Msc => MembershipFormat::RoomState,
            MembershipFormat::RoomState => MembershipFormat::Msc,
        };
        let (primary, alternate) = tokio::try_join!(
            PassiveSlot::read(room, req, format), PassiveSlot::read(room, req, other)
        )?;
        Ok(Self { primary, alternate })
    }

    fn check_feeds(&self) -> io::Result<()> {
        if !self.primary.room.is_seeded() || !self.alternate.room.is_seeded() {
            return Err(error("RTC admission room feed unavailable"));
        }
        Ok(())
    }

    // The two readers can see the same underlying membership. Accept the
    // same pinned device/member, and reject an occupant visible in only the
    // other signalling generation. Neither reader joins the slot.
    fn check_other_members(&self, req: &Message, own: Option<(&str, &str)>,
                           remote: Option<(&str, &str)>) -> io::Result<()> {
        self.check_feeds()?;
        let members = self.alternate.members.borrow();
        let rows = members.iter().map(|m| (m.sender.as_str(), m.member_id.as_str(), m.origin.sender_device_id()));
        let allowed = [
            own.map(|(id, dev)| (req.ghost_mxid.as_str(), id, Some(dev))),
            remote.map(|(id, dev)| (req.expected_user_mxid.as_str(), id, Some(dev))),
        ];
        if !only_pinned_members(rows, allowed) {
            return Err(error("extra RTC membership in alternate signalling format"));
        }
        Ok(())
    }

    async fn open(&self,req:&Message)->bool {
        matches!(self.primary.room.slot_state(&req.slot_id).await,Some(SlotState::Open(open)) if open.application_type=="m.call" && matches!(open.mechanism,Some(matrix_rtc_core::EncryptionMechanism::PerMember)))
    }

    async fn caller(&self,req:&Message,room:&matrix_sdk::Room,first:bool)->io::Result<(String,Option<u64>,DeviceKeys)>{
        let member={
            let members=self.primary.members.borrow();
            let [m]=members.as_slice() else {return Err(error("expected exactly one calling membership before dial"))};
            m.clone()
        };
        if req.expected_device.is_empty() || member.application.application_type()!=Some("m.call") || member.sender!=req.expected_user_mxid || member.slot_id!=req.slot_id ||
           member.origin.sender_device_id()!=Some(&req.expected_device) ||
           (member.origin.was_encrypted()!=Some(true) && (req.membership_format!="room_state" || req.member_event.is_empty())) ||
           (first && !matches_invited_join(req, member.membership_event_id.as_deref(), &member.member_id, member.membership_ts)) {
            return Err(error("caller device or membership does not match the encrypted invitation"))
        }
        if !self.open(req).await && req.membership_format!="room_state" {return Err(error("RTC slot closed or lacks encryption"))}
        self.check_other_members(req, None, Some((&member.member_id, &req.expected_device)))?;
        let keys = if first {
            refresh_trusted_device(room,&member.sender,&req.expected_device).await?
        } else {
            trusted_device(room,&member.sender,&req.expected_device).await?
        };
        keys.require_invitation(req)?;
        // Both trust lookup and slot reads await external work. A failed
        // snapshot during either await must not authorize a dial from cache.
        if !self.open(req).await && req.membership_format!="room_state" {
            return Err(error("RTC slot closed during device validation"));
        }
        self.check_other_members(req, None, Some((&member.member_id, &req.expected_device)))?;
        {
            let latest = self.primary.members.borrow();
            if !matches!(latest.as_slice(), [m] if m.sender == member.sender
                && m.slot_id == member.slot_id && m.member_id == member.member_id
                && m.membership_ts == member.membership_ts
                && m.application.application_type() == Some("m.call")
                && m.origin.sender_device_id() == Some(&req.expected_device)
                && (m.origin.was_encrypted() == Some(true)
                    || (req.membership_format == "room_state" && !req.member_event.is_empty()))) {
                return Err(error("RTC caller changed during device validation"));
            }
        }
        self.check_feeds()?;
        Ok((member.member_id,member.membership_ts,keys))
    }
}

// A gateway pin comes from the independently observed referenced membership.
// MSC has a unique join ID; legacy also needs its original join timestamp.
// Without a pin, retain exact-event matching for referenced invitations.
fn matches_invited_join(req: &Message, event_id: Option<&str>, member_id: &str, membership_ts: Option<u64>) -> bool {
    if !req.member_join_id.is_empty() {
        return !req.member_event.is_empty() && member_id == req.member_join_id
            && match req.membership_format.as_str() {
                "msc" => req.member_join_ts == 0 && membership_ts.is_none(),
                "room_state" => req.member_join_ts != 0 && membership_ts == Some(req.member_join_ts),
                _ => false,
            };
    }
    req.member_event.is_empty() || event_id == Some(req.member_event.as_str())
}

fn only_pinned_members<'a>(
    rows: impl Iterator<Item = (&'a str, &'a str, Option<&'a str>)>,
    mut allowed: [Option<(&str, &str, Option<&str>)>; 2],
) -> bool {
    for row in rows {
        let Some(entry) = allowed.iter_mut().find(|entry| **entry == Some(row)) else {
            return false;
        };
        entry.take();
    }
    true
}

// Refresh at admission, pickup and a bounded interval during active calls,
// not on each one-second media/topology tick.
// Failed or timed-out queries cannot authorize from a stale cache.
#[derive(Clone, Debug, PartialEq, Eq)]
struct DeviceKeys { signing: String, identity: String, master: Option<String> }
impl DeviceKeys {
    fn require_invitation(&self, req: &Message) -> io::Result<()> {
        if self.signing != req.expected_signing_key || self.identity != req.expected_identity_key {
            return Err(error("RTC invitation device keys no longer match"));
        }
        Ok(())
    }
    fn require_same(&self, current: &Self) -> io::Result<()> {
        if self != current { return Err(error("RTC peer device keys changed during call")); }
        Ok(())
    }
}

async fn refresh_trusted_device(room:&matrix_sdk::Room,user:&str,device:&str)->io::Result<DeviceKeys> {
    let user_id=UserId::parse(user).map_err(io::Error::other)?;
    let client=room.client();
    tokio::time::timeout(Duration::from_secs(5), async {
        let fresh = refresh_user_keys(&client, &user_id).await.map_err(error)?;
        let (d, master) = fresh.confirmed_device(&client, device.into()).await.map_err(error)?;
        if !d.is_verified() && !d.is_cross_signed_by_owner() { return Err(error("RTC device is not trusted")); }
        Ok(DeviceKeys {
            signing: d.ed25519_key().ok_or_else(|| error("RTC device signing key missing"))?.to_base64(),
            identity: d.curve25519_key().ok_or_else(|| error("RTC device identity key missing"))?.to_base64(),
            master,
        })
    }).await.map_err(|_| error("RTC device key refresh timed out"))?
}

const ACTIVE_PEER_REFRESH_INTERVAL: Duration = Duration::from_secs(30);
const ACTIVE_PEER_REFRESH_BUDGET: Duration = Duration::from_secs(5);
type PendingPeerRefresh<'a> = std::pin::Pin<Box<dyn std::future::Future<Output = io::Result<DeviceKeys>> + Send + 'a>>;

fn start_peer_refresh<'a>(room: &'a matrix_sdk::Room, user: String, device: String) -> PendingPeerRefresh<'a> {
    Box::pin(async move {
        tokio::time::timeout(ACTIVE_PEER_REFRESH_BUDGET, refresh_trusted_device(room, &user, &device))
            .await.map_err(|_| error("RTC active peer key refresh timed out"))?
    })
}

async fn poll_peer_refresh(pending: &mut Option<PendingPeerRefresh<'_>>) -> io::Result<DeviceKeys> {
    match pending.as_mut() {
        Some(refresh) => refresh.as_mut().await,
        None => std::future::pending().await,
    }
}

// Keep periodic store/network checks session-owned, rather than awaiting
// them inside the topology tick. The absolute deadline includes time spent
// waiting for the control loop to poll the check again.
const TOPOLOGY_CHECK_BUDGET: Duration = Duration::from_secs(5);
type PendingTopology<'a> = std::pin::Pin<Box<dyn std::future::Future<Output = io::Result<Option<DeviceKeys>>> + Send + 'a>>;

async fn bounded_topology<T>(deadline: tokio::time::Instant,
    operation: impl std::future::Future<Output = io::Result<T>>) -> io::Result<T> {
    if tokio::time::Instant::now() >= deadline {
        return Err(error("RTC topology check timed out"));
    }
    let value = tokio::time::timeout_at(deadline, operation).await
        .map_err(|_| error("RTC topology check timed out"))??;
    if tokio::time::Instant::now() >= deadline {
        return Err(error("RTC topology check timed out"));
    }
    Ok(value)
}

fn start_topology_check<'a>(incoming: &'a Message, room: &'a matrix_sdk::Room, identity: &'a IdentityFence,
    device: Option<String>, deadline: tokio::time::Instant) -> PendingTopology<'a> {
    Box::pin(bounded_topology(deadline, async move {
        identity.check().await?;
        validate_room(incoming, room).await?;
        require_prepared_identity(&room.client()).await?;
        match device {
            Some(device) => trusted_device(room, &incoming.expected_user_mxid, &device).await.map(Some),
            None => Ok(None),
        }
    }))
}

async fn poll_topology_check(pending: &mut Option<PendingTopology<'_>>) -> io::Result<Option<DeviceKeys>> {
    match pending.as_mut() {
        Some(check) => check.as_mut().await,
        None => std::future::pending().await,
    }
}

// Group invitation checks are independent of the IPC reader. Preserve FIFO
// order and bound both the waiting requests and their total lifetime.
const GROUP_CHECK_CAPACITY: usize = 8;
type GroupCheck = (Vec<String>, tokio::time::Instant);
type PendingGroupCheck<'a> = std::pin::Pin<Box<dyn std::future::Future<Output = io::Result<(Vec<String>, DeviceKeys)>> + Send + 'a>>;

fn enqueue_group_check(queue: &mut std::collections::VecDeque<GroupCheck>, in_flight: bool,
    targets: Vec<String>, deadline: tokio::time::Instant) -> io::Result<()> {
    if queue.len() + (in_flight as usize) >= GROUP_CHECK_CAPACITY {
        return Err(error("RTC group invitation queue full"));
    }
    queue.push_back((targets, deadline));
    Ok(())
}

fn start_group_check<'a>(incoming: &'a Message, room: &'a matrix_sdk::Room,
    device: String, targets: Vec<String>, deadline: tokio::time::Instant) -> PendingGroupCheck<'a> {
    Box::pin(bounded_topology(deadline, async move {
        validate_room(incoming, room).await?;
        require_prepared_identity(&room.client()).await?;
        let keys = refresh_trusted_device(room, &incoming.expected_user_mxid, &device).await?;
        Ok((targets, keys))
    }))
}

async fn poll_group_check(pending: &mut Option<PendingGroupCheck<'_>>) -> io::Result<(Vec<String>, DeviceKeys)> {
    match pending.as_mut() {
        Some(check) => check.as_mut().await,
        None => std::future::pending().await,
    }
}

async fn trusted_device(room:&matrix_sdk::Room,user:&str,device:&str)->io::Result<DeviceKeys> {
    let user=UserId::parse(user).map_err(io::Error::other)?;
    let client = room.client();
    let encryption = client.encryption();
    let identity = encryption.get_user_identity(&user).await.map_err(io::Error::other)?;
    if identity.as_ref().is_some_and(|i| i.has_verification_violation()) {
        return Err(error("RTC user identity verification was lost"));
    }
    let master = identity.and_then(|i| i.master_key().get_first_key().map(|k| k.to_base64()));
    let device=encryption.get_device(&user,device.into()).await.map_err(io::Error::other)?;
    let d = device.filter(|d| !d.is_deleted() && !d.is_blacklisted() && (d.is_verified() || d.is_cross_signed_by_owner()))
        .ok_or_else(|| error("RTC device is not trusted"))?;
    Ok(DeviceKeys {
        signing: d.ed25519_key().ok_or_else(|| error("RTC device signing key missing"))?.to_base64(),
        identity: d.curve25519_key().ok_or_else(|| error("RTC device identity key missing"))?.to_base64(),
        master,
    })
}

async fn lease_matches(path: &Path, expected: &Message) -> io::Result<bool> {
    let bytes = match tokio::fs::read(path).await {
        Ok(bytes) => bytes,
        Err(err) if err.kind() == io::ErrorKind::NotFound => return Ok(false),
        Err(err) => return Err(err),
    };
    let actual: Message = serde_json::from_slice(&bytes).map_err(io::Error::other)?;
    Ok(&actual == expected)
}

// Callers hold the slot reservation across the lease check, network cleanup
// and removal. The extra checks also detect replacement by an external writer
// when visible, but are not an interprocess lock.
async fn release_slot(room:&matrix_sdk::Room,req:&Message,path:&Path)->io::Result<()> {
    if room.room_id().as_str() != req.room_id || !lease_matches(path, req).await? {
        return Err(error("MatrixRTC slot lease changed before cleanup"));
    }
    let response=room.client().send(get_state_events::v3::Request::new(room.room_id().to_owned())).await.map_err(io::Error::other)?;
    for ev in response.room_state {
        let field=|k:&str| ev.get_field::<String>(k).ok().flatten();
        if field("type").as_deref()!=Some(wire_event_type("m.rtc.slot")) || field("state_key").as_deref()!=Some(&req.slot_id) ||
           field("sender").as_deref()!=Some(&req.ghost_mxid) {continue}
        let content=ev.get_field::<serde_json::Value>("content").map_err(io::Error::other)?.unwrap_or_default();
        if content["org.mautrix.whatsapp.call_id"].as_str()==Some(&req.call_id) {
            let id=EventId::parse(field("event_id").ok_or_else(||error("slot event ID missing"))?).map_err(io::Error::other)?;
            room.redact(&id,Some("WhatsApp call ended"),None).await.map_err(io::Error::other)?;
        }
    }
    if !lease_matches(path, req).await? {
        return Err(error("MatrixRTC slot lease changed during cleanup"));
    }
    match tokio::fs::remove_file(path).await { Err(e) if e.kind()==io::ErrorKind::NotFound=>Ok(()),other=>other }
}

async fn create_slot(room:&matrix_sdk::Room,req:&Message,path:&Path)->io::Result<()> {
    tokio::fs::create_dir_all(path.parent().unwrap()).await?;
    let temp=path.with_extension("tmp");
    let mut file=tokio::fs::OpenOptions::new().create(true).truncate(true).write(true).mode(0o600).open(&temp).await?;
    tokio::io::AsyncWriteExt::write_all(&mut file,&serde_json::to_vec(req).map_err(io::Error::other)?).await?;
    file.sync_all().await?;
    drop(file);
    tokio::fs::rename(temp,path).await?;
    tokio::fs::File::open(path.parent().unwrap()).await?.sync_all().await?;
    let mut content=serde_json::to_value(RawSlotEventContent::for_open("m.call".to_owned(),Some(SlotEncryption{
        encryption_type:"m.per_member".to_owned(),extra:Default::default()
    }))).map_err(io::Error::other)?;
    content["org.mautrix.whatsapp.call_id"]=json!(req.call_id);
    room.send_state_event_raw(wire_event_type("m.rtc.slot"),&req.slot_id,content).await.map_err(io::Error::other)?;
    Ok(())
}

async fn run_incoming(incoming: &Message, reader: &mut protocol::FrameReader<ReadHalf<UnixStream>>, writer: &Writer, accounts: &Accounts, replay: &crate::ReplayGuard) -> io::Result<()> {
    let focus = trusted_focus()?;
    let answer_deadline = tokio::time::Instant::now() + INCOMING_ANSWER_BUDGET;
    let format = membership_format(incoming)?;
    if incoming.video {
        within_answer_budget(answer_deadline, video::verify_ffmpeg()).await?;
    }
    let (ghost, room) = within_answer_budget(answer_deadline, synced_room(incoming, accounts)).await?;
    let path=accounts.lease_path(incoming);
    if tokio::fs::try_exists(&path).await? {
        let old:Message=serde_json::from_slice(&tokio::fs::read(&path).await?).map_err(io::Error::other)?;
        if old.ghost_mxid!=incoming.ghost_mxid || accounts.lease_path(&old)!=path {
            return Err(error("previous slot owner requires recovery"));
        }
        within_answer_budget(answer_deadline, ghost.identity.check()).await?;
        release_slot(&room,&old,&path).await?;
    }
    let admission=within_answer_budget(answer_deadline, Admission::inspect(&room,incoming)).await?;
    admission.check_feeds()?;
    if !admission.primary.members.borrow().is_empty() || !admission.alternate.members.borrow().is_empty() {return Err(error("incoming RTC slot already occupied"))}
    // MSC owns an explicit encrypted slot. Legacy Element Call has no slot
    // state at all; creating an MSC slot for it would use a different SFU room
    // and publish events its client cannot interpret.
    let owned = format == MembershipFormat::Msc && !within_answer_budget(answer_deadline, async { Ok(admission.open(incoming).await) }).await?;
    if owned && matches!(within_answer_budget(answer_deadline, async { Ok(admission.primary.room.slot_state(&incoming.slot_id).await) }).await?,Some(SlotState::Open(_))) {return Err(error("existing RTC slot has an incompatible application or encryption mechanism"));}
    if replay.contains(&incoming.login_id, &incoming.call_id)? {
        return Err(error("duplicate or replayed call"));
    }
    let result=async {
    check_answer_deadline(answer_deadline)?;
    admission.check_feeds()?;
    within_answer_budget(answer_deadline, ghost.identity.check()).await?;
    if owned { create_slot(&room, incoming, &path).await?; }
    check_answer_deadline(answer_deadline)?;
    // Creating the slot does not ring the Matrix user. Commit replay protection
    // only once setup succeeded, immediately before joining and notifying.
    if !replay.register(&incoming.login_id, &incoming.call_id)? {
        return Err(error("duplicate or replayed call"));
    }
    check_answer_deadline(answer_deadline)?;
    admission.check_feeds()?;
    let recipients = CallKeyRecipients::new(incoming.expected_user_mxid.clone(),
        (!incoming.expected_device.is_empty()).then(|| incoming.expected_device.clone()));
    within_answer_budget(answer_deadline, ghost.identity.check()).await?;
    let mut call = LiveKitCall::join_until_with_outbox(&room, LiveKitCallOptions {
        trusted_focus: Some(focus),
        call_key_recipients: Some(recipients.clone()),
        slot_id: incoming.slot_id.clone(),
        format: membership_format(incoming)?,
        intent: Some(if incoming.video { CallIntent::Video } else { CallIntent::Audio }),
        kind: if incoming.group {
            CallKind::Group { notify: Some(GroupNotify {
                intent: Some(if incoming.video { "video" } else { "audio" }.to_owned()),
                lifetime_ms: Some(90_000),
                mentions: Mentions { user_ids: vec![incoming.expected_user_mxid.clone()], room: false },
            }) }
        } else {
            CallKind::Direct { ring: Some(RingConfig {
                intent: Some(if incoming.video { "video" } else { "audio" }.to_owned()),
                lifetime_ms: Some(90_000),
            }) }
        },
        ..Default::default()
    },Some(join_deadline(tokio::time::Instant::now(), answer_deadline)),ghost.outbox.clone()).await.map_err(io::Error::other)?;
    let result = connected_session(incoming, reader, writer, &mut call, &room, &ghost.identity, &admission, accounts, recipients, None, None, None, answer_deadline).await;
    let leave_result = call.leave().await;
    if let Err(ref err) = leave_result {
        eprintln!("RTC call cleanup failed: {err}");
    }
    if result.is_ok() { leave_result.map_err(io::Error::other)?; }
    result
    }.await;
    if owned && tokio::fs::try_exists(&path).await? { release_slot(&room,incoming,&path).await?; }
    result
}

// Older paired builds sent only the boolean. New builds delegate absent
// explicit hints to the membership selected by admission, defaulting to video.
fn outgoing_video(req: &Message, membership: Option<CallIntent>) -> bool {
    match req.intent.as_str() {
        "audio" => false,
        "video" => true,
        "auto" => membership.unwrap_or_default() == CallIntent::Video,
        _ => req.video,
    }
}

async fn run_outgoing(incoming: &Message, reader: &mut protocol::FrameReader<ReadHalf<UnixStream>>, writer: &Writer, accounts: &Accounts, replay: &crate::ReplayGuard) -> io::Result<()> {
    let admission_deadline = tokio::time::Instant::now() + OUTGOING_ADMISSION_BUDGET;
    let focus = trusted_focus()?;
    let (ghost, room) = within_answer_budget(admission_deadline, synced_room(incoming, accounts)).await?;
    let mut admission=within_answer_budget(admission_deadline, Admission::inspect(&room,incoming)).await?;
    if admission.primary.members.borrow().is_empty(){
        // Drop the watch Ref before awaiting either diagnostic snapshot.
        let ready = tokio::time::timeout_at((tokio::time::Instant::now() + Duration::from_secs(6)).min(admission_deadline), admission.primary.members.wait_for(|m| !m.is_empty()))
            .await.map(|result| result.map(|_| ()));
        match ready {
            Ok(result) => { result.map_err(io::Error::other)?; }
            Err(_) => {
                // Do not bypass the authenticated membership check. Capture
                // both readers' candidate/slot state (no key material) so an
                // absent, expired, or wrong-dialect member is distinguishable.
                if let Ok((primary, alternate)) = within_answer_budget(admission_deadline, async {
                    Ok((admission.primary.room.debug_snapshot().await,
                        admission.alternate.room.debug_snapshot().await))
                }).await {
                    eprintln!("RTC admission timeout: format={} member_event={} primary={} alternate={}",
                        incoming.membership_format, incoming.member_event, primary, alternate);
                }
                return Err(error("calling membership absent"));
            }
        }
    }
    let pin=within_answer_budget(admission_deadline, admission.caller(incoming,&room,true)).await?;
    if within_answer_budget(admission_deadline, admission.caller(incoming,&room,false)).await? != pin {
        return Err(error("RTC caller changed during initial device refresh"));
    }
    // Select only from the independently observed, verified caller generation.
    // No bridge-side membership cache participates in the intent decision.
    let member_intent = {
        let members = admission.primary.members.borrow();
        let [member] = members.as_slice() else { return Err(error("caller disappeared before intent resolution")); };
        if member.sender != incoming.expected_user_mxid || member.slot_id != incoming.slot_id
            || member.origin.sender_device_id() != Some(&incoming.expected_device)
            || member.member_id != pin.0 || member.membership_ts != pin.1 {
            return Err(error("caller changed before intent resolution"));
        }
        CallIntent::of(member)
    };
    let mut effective = incoming.clone();
    effective.video = outgoing_video(incoming, member_intent);
    if effective.video { within_answer_budget(admission_deadline, video::verify_ffmpeg()).await?; }
    if within_answer_budget(admission_deadline, admission.caller(incoming,&room,false)).await? != pin {
        return Err(error("RTC caller changed during media preparation"));
    }
    let incoming = &effective;
    eprintln!("RTC outgoing: authenticated caller admitted; requesting WhatsApp dial");
    let mut wa_audio=None;
    let mut wa_video=None;
    check_answer_deadline(admission_deadline)?;
    if !replay.register(&incoming.login_id,&incoming.call_id)? {return Err(error("duplicate or replayed call"));}
    let mut dial = Message::new("dial", &incoming.call_id);
    dial.video = incoming.video;
    dial.intent = if incoming.video { "video" } else { "audio" }.into();
    check_answer_deadline(admission_deadline)?;
    within_answer_budget(admission_deadline, ghost.identity.check()).await?;
    send(writer, &dial).await?;

    // A PLI/FIR can legitimately arrive before the remote WhatsApp peer accepts
    // (for example when the video transport is enabled). Keep one absolute
    // acceptance deadline and tolerate those early keyframe requests instead of
    // treating them as a protocol violation.
    let mut recheck=tokio::time::interval(Duration::from_secs(1));
    let acceptance_deadline = tokio::time::Instant::now() + Duration::from_secs(125);
    loop {
        check_answer_deadline(acceptance_deadline)?;
        let frame=tokio::select! {
            read=tokio::time::timeout_at(acceptance_deadline,reader.read())=>read,
            _=recheck.tick()=>{
                within_answer_budget(acceptance_deadline, ghost.identity.check()).await?;
                within_answer_budget(acceptance_deadline, validate_room(incoming,&room)).await?;
                if within_answer_budget(acceptance_deadline, admission.caller(incoming,&room,false)).await?!=pin {return Err(error("RTC caller changed while ringing"))}
                continue;
            }
            changed=admission.primary.members.changed()=>{
                changed.map_err(io::Error::other)?;
                let roster=admission.primary.members.borrow();
                if !matches!(roster.as_slice(),[m] if m.sender==incoming.expected_user_mxid && m.member_id==pin.0 && m.membership_ts==pin.1 && m.origin.sender_device_id()==Some(&incoming.expected_device)) {
                    return Err(error("calling membership changed while ringing"));
                }
                continue;
            }
        };
        let accepted = match frame {
            Ok(Ok(message)) => message,
            Ok(Err(err)) => return Err(err),
            Err(_) => {
                return Err(error("WhatsApp peer did not answer before timeout"));
            }
        };
        check_answer_deadline(acceptance_deadline)?;
        if accepted.call_id != incoming.call_id {
            return Err(error("call ID mismatch"));
        }
        match accepted.kind.as_str() {
            "whatsapp_answered" => {
                eprintln!("RTC outgoing: WhatsApp peer accepted; joining MatrixRTC media");
                break;
            },
            "whatsapp_video_keyframe" => continue,
            "whatsapp_audio_mute" | "whatsapp_audio_unmute" => {wa_audio=Some(accepted.kind=="whatsapp_audio_mute");continue},
            "whatsapp_video_state" => {if matches!(accepted.state,0|1|6) {wa_video=Some(accepted.state);} continue},
            "reject" => {
                return Ok(());
            }
            "ended" | "hangup" | "cancel" => return Ok(()),
            _ => return Err(error("unexpected WhatsApp event before remote acceptance")),
        }
    }

    let answer_deadline = tokio::time::Instant::now() + OUTGOING_ANSWER_BUDGET;
    within_answer_budget(answer_deadline, validate_room(incoming,&room)).await?;
    if within_answer_budget(answer_deadline, admission.caller(incoming,&room,false)).await? != pin {
        return Err(error("RTC caller changed before WhatsApp acceptance"));
    }
    let refreshed = within_answer_budget(answer_deadline,
        refresh_trusted_device(&room,&incoming.expected_user_mxid,&incoming.expected_device)).await?;
    refreshed.require_invitation(incoming)?;
    pin.2.require_same(&refreshed)?;
    if within_answer_budget(answer_deadline, admission.caller(incoming,&room,false)).await? != pin {
        return Err(error("RTC caller changed during device refresh"));
    }
    check_answer_deadline(answer_deadline)?;
    let recipients = CallKeyRecipients::new(incoming.expected_user_mxid.clone(),
        (!incoming.expected_device.is_empty()).then(|| incoming.expected_device.clone()));
    within_answer_budget(answer_deadline, ghost.identity.check()).await?;
    let mut call = LiveKitCall::join_until_with_outbox(&room, LiveKitCallOptions {
        trusted_focus: Some(focus),
        call_key_recipients: Some(recipients.clone()),
        slot_id: incoming.slot_id.clone(),
        format: membership_format(incoming)?,
        intent: Some(if incoming.video { CallIntent::Video } else { CallIntent::Audio }),
        kind: if incoming.group { CallKind::Group { notify: None } } else { CallKind::Direct { ring: None } },
        ..Default::default()
    },Some(join_deadline(tokio::time::Instant::now(), answer_deadline)),ghost.outbox.clone()).await.map_err(io::Error::other)?;
    let result = connected_session(incoming, reader, writer, &mut call, &room, &ghost.identity, &admission, accounts, recipients, Some(pin), wa_audio, wa_video, answer_deadline).await;
    let leave_result = call.leave().await;
    if let Err(ref err) = leave_result {
        eprintln!("RTC call cleanup failed: {err}");
    }
    if result.is_ok() { leave_result.map_err(io::Error::other)?; }
    result
}

/// Select the only permitted remote member and its device from the SDK roster.
/// Verify the result against the core membership snapshot, then derive the
/// SFU identity from that membership's published transport.
fn authorized_peer(roster: &[Participant], own_member_id: &str, expected: &str) -> io::Result<Option<(String, String)>> {
    if roster.len() > 2 { return Err(error("RTC slot occupied by extra membership")); }
    let mut remote = None;
    for participant in roster {
        if participant.is_local {
            if participant.member_id != own_member_id { return Err(error("unexpected local RTC membership")); }
            continue;
        }
        if participant.user_id != expected {
            return Err(error("unrecognized RTC participant joined the call"));
        }
        let Some(device) = participant.device_id.as_deref().filter(|d| !d.is_empty()) else {
            return Err(error("RTC participant device identity is unavailable"));
        };
        if remote.is_some() { return Err(error("multiple RTC devices or memberships")); }
        remote = Some((participant.member_id.clone(), device.to_owned()));
    }
    Ok(remote)
}

// Use the exact mapper that the SDK uses for its encryption key ring and SFU
// connections. The token flow is a property of the peer's published transport,
// not the local call's signalling dialect.
fn peer_media_identity(user: &str, device: &str, member: &str, transports: &[RtcTransport]) -> Option<String> {
    identity_for(&IdentitySubject {
        user_id: user,
        device_id: Some(device),
        member_id: member,
        transports,
    })
}

async fn ensure_whatsapp_video_track(
    call: &mut LiveKitCall,
    decoder: &mut Option<video::WhatsAppVideoDecoder>,
) -> io::Result<()> {
    if decoder.is_some() {
        return Ok(());
    }
    video::verify_ffmpeg().await?;
    let camera = call.publish(PublishOptions::camera(VideoSourceConfig {
        width: video::MATRIX_VIDEO_WIDTH,
        height: video::MATRIX_VIDEO_HEIGHT,
    })).await.map_err(io::Error::other)?;
    *decoder = Some(video::WhatsAppVideoDecoder::spawn(camera)?);
    Ok(())
}

async fn stop_whatsapp_video_track(
    call: &mut LiveKitCall,
    decoder: &mut Option<video::WhatsAppVideoDecoder>,
) -> io::Result<()> {
    if decoder.take().is_some() {
        call.engine().unpublish(MediaStreamKind::Camera).await.map_err(io::Error::other)?;
    }
    Ok(())
}

// Connection state is evidence of reachability, not proof of decoded RTP.
// Muted clients can legitimately have no published tracks at pickup.
fn peer_answer_ready(status: &CallStatus, roster: &[Participant], own: &str, own_user: &str, own_device: &str, remote: &str, device: &str, user: &str) -> bool {
    if !matches!(status, CallStatus::Connected { degraded: false, .. }) || roster.len() != 2
        || own == remote || own_device.is_empty() {
        return false;
    }
    let local = roster.iter().any(|p| p.is_local && p.member_id == own && p.user_id == own_user
        && p.device_id.as_deref() == Some(own_device) && p.connection == ConnectionState::Connected);
    let peer = roster.iter().any(|p| !p.is_local && p.member_id == remote && p.user_id == user
        && p.device_id.as_deref() == Some(device) && p.connection == ConnectionState::Connected
        && p.unable_to_decrypt.is_none());
    local && peer
}

// An unmuted, advertised microphone or camera must deliver real media, not
// merely a connected SFU, imported key or a stream of concealed PCM. Muted
// or unpublished streams cannot provide samples and do not block pickup.
const PICKUP_SAMPLE_INTERVAL: Duration = Duration::from_millis(400);
const PICKUP_SAMPLE_BUDGET: Duration = Duration::from_secs(2);
const PICKUP_RETRY_INTERVAL: Duration = Duration::from_secs(1);
type PickupTracks = Vec<(MediaStreamKind, Arc<dyn RemoteTrackHandle>)>;

fn active_pickup_streams(peer: &Participant) -> Option<Vec<MediaStreamKind>> {
    let mut active = Vec::new();
    for kind in [MediaStreamKind::Microphone, MediaStreamKind::Camera] {
        let mut streams = peer.streams.iter().filter(|s| s.kind == kind);
        if let Some(stream) = streams.next() {
            if streams.next().is_some() { return None; }
            if !stream.muted { active.push(kind); }
        }
    }
    Some(active)
}

fn pickup_publications_ready(roster: &[Participant], own: &str, mic_muted: bool, camera: bool) -> bool {
    let Some(local) = roster.iter().find(|p| p.is_local && p.member_id == own) else { return false; };
    let mut mic = local.streams.iter().filter(|s| s.kind == MediaStreamKind::Microphone);
    if !mic.next().is_some_and(|s| s.muted == mic_muted) || mic.next().is_some() { return false; }
    local.streams.iter().filter(|s| s.kind == MediaStreamKind::Camera).count() == if camera { 1 } else { 0 }
}

fn decoded_since(kind: MediaStreamKind, first: &ReceiveStats, last: &ReceiveStats) -> bool {
    if last.packets_received <= first.packets_received || last.bytes_received <= first.bytes_received {
        return false;
    }
    match kind {
        MediaStreamKind::Microphone => {
            let Some(samples) = last.total_samples_received.checked_sub(first.total_samples_received) else { return false; };
            let Some(concealed) = last.concealed_samples.checked_sub(first.concealed_samples) else { return false; };
            samples > concealed
        }
        MediaStreamKind::Camera => last.frames_decoded > first.frames_decoded,
        _ => false,
    }
}

fn pickup_tracks(call: &LiveKitCall, peer: &Participant) -> Option<PickupTracks> {
    active_pickup_streams(peer)?.into_iter().map(|kind| {
        call.remote_track(&peer.member_id, kind).map(|track| (kind, track))
    }).collect()
}

fn same_pickup_tracks(before: &PickupTracks, after: &PickupTracks) -> bool {
    before.len() == after.len() && before.iter().zip(after).all(|((kind, track), (other_kind, other))|
        kind == other_kind && Arc::ptr_eq(track, other))
}

async fn sample_pickup_tracks(tracks: &PickupTracks) -> bool {
    if tracks.is_empty() { return true; }
    let sample = || futures_util::future::join_all(tracks.iter().map(|(_, track)| track.receive_stats()));
    tokio::time::timeout(PICKUP_SAMPLE_BUDGET, async {
        let first = sample().await;
        tokio::time::sleep(PICKUP_SAMPLE_INTERVAL).await;
        let last = sample().await;
        tracks.iter().zip(first).zip(last).all(|(((kind, _), first), last)| {
            matches!((first, last), (Some(first), Some(last)) if decoded_since(*kind, &first, &last))
        })
    }).await.unwrap_or(false)
}

// Before pickup only an actual Matrix decline represents a user decision to
// reject the WhatsApp call. Local setup errors/timeouts must not do so.
fn ended_event(answered: bool, reason: &CallEnd) -> &'static str {
    if answered { "hangup" }
    else if matches!(reason, CallEnd::Declined) { "reject" }
    else { "error" }
}

// Close the scope even if the connected-session future is cancelled or returns
// an error. Already submitted network requests cannot be recalled.
struct RecipientGuard(CallKeyRecipients);
impl Drop for RecipientGuard {
    fn drop(&mut self) { self.0.revoke(); }
}

async fn connected_session(incoming: &Message, reader: &mut protocol::FrameReader<ReadHalf<UnixStream>>, writer: &Writer, call: &mut LiveKitCall, room:&matrix_sdk::Room, identity_fence:&IdentityFence, admission:&Admission, accounts:&Accounts, recipients:CallKeyRecipients, expected_join:Option<(String,Option<u64>,DeviceKeys)>, wa_audio:Option<bool>, wa_video:Option<u8>, answer_deadline:tokio::time::Instant) -> io::Result<()> {
    let recipient_guard = RecipientGuard(recipients);
    within_answer_budget(answer_deadline, identity_fence.check()).await?;
    let mut status = call.subscribe_status();
    let mut events = call.subscribe_call_events();
    let mut roster_updates = call.subscribe_participants();
    let mut memberships = within_answer_budget(answer_deadline, async {
        Ok(call.subscribe_memberships().await)
    }).await?;
    let own_member_id = call.membership_id().to_owned();
    let own_device_id = room.client().device_id().map(|d| d.to_string())
        .filter(|d| !d.is_empty()).ok_or_else(|| error("RTC local device identity unavailable"))?;
    let mut pinned = None;
    let mut pinned_keys: Option<DeviceKeys> = None;
    let mut pending_peer_refresh: Option<PendingPeerRefresh<'_>> = None;
    let mut next_peer_refresh = None;
    let mut pending_topology: Option<PendingTopology<'_>> = None;
    let mut group_checks = std::collections::VecDeque::<GroupCheck>::new();
    let mut pending_group_check: Option<PendingGroupCheck<'_>> = None;
    let mut topology=tokio::time::interval(Duration::from_secs(1));
    topology.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut answered = false;
    let mut next_pickup_probe = tokio::time::Instant::now();
    let mut states=HashMap::<String,Option<bool>>::new();
    let mut pending_tracks = std::collections::VecDeque::<CallEvent>::new();
    let mut last_sample = 0f32;
    let mut pending_audio: Option<PendingAudio> = None;
    let mut microphone_muted = wa_audio == Some(true);
    let mut remote_tasks = AbortOnDrop(HashMap::new());
    let mut remote_recovery = HashMap::<String, RemoteRecovery>::new();
    let media_active = Arc::new(AtomicBool::new(false));
    let _media_gate = MediaGate(media_active.clone());
    let video_keyframe_generation = Arc::new(AtomicU64::new(0));

    admission.check_feeds()?;
    if !call.room_feed_ready() { return Err(error("RTC call room feed unavailable")); }
    // Initial media publication shares the same budget as membership and
    // pickup. A stalled transport must not extend the ringing lifetime.
    let mut video_decoder = None;
    let mut rejected_generation=0;
    let audio_track = within_answer_budget(answer_deadline, async {
        let track = call.publish(PublishOptions::microphone()).await.map_err(io::Error::other)?;
        if wa_audio == Some(true) {
            call.engine().set_local_muted(MediaStreamKind::Microphone, true).await.map_err(io::Error::other)?;
        }
        if wa_video == Some(1) || (incoming.video && wa_video != Some(6)) {
            ensure_whatsapp_video_track(call, &mut video_decoder).await?;
        }
        if wa_video == Some(0) && video_decoder.is_some() {
            call.engine().set_local_muted(MediaStreamKind::Camera, true).await.map_err(io::Error::other)?;
        }
        Ok(track)
    }).await?;
    let mut expiry = Box::pin(tokio::time::sleep_until(answer_deadline));
    // Independent publication is opt-in pending native interoperability.
    // Keep the previously verified aggregate call behaviour by default.
    let independent_enabled = std::env::var("MATRIXRTC_EXPERIMENTAL_INDEPENDENT_GROUP_MEDIA")
        .is_ok_and(|value| value == "1") && incoming.group && !incoming.group_jid.is_empty()
        && incoming.ghost_mxid == incoming.bridge_bot_mxid;
    // WhatsApp's captured v2 screen-share mode replaces its primary video
    // source. Enable only for the opt-in, chat-bound video group path.
    let screen_switch_enabled = independent_enabled && incoming.video;
    let mut selected_video_kind = MediaStreamKind::Camera;
    let mut outbound_video_generation = 0u64;
    let mut outbound_video_started = false;
    let mut video_switch_deadline: Option<tokio::time::Instant> = None;
    let mut request_at = None;
    let mut group_attempts = 0u8;
    let mut group_deadline = None;
    let mut group_session: Option<Arc<group_publication_session::GroupPublicationSession>> = None;
    let mut group_mode_started = false;
    let mut latest_group_generation = 0u64;
    let mut group_router: Option<group_publication_router::GroupPublicationRouter> = None;
    let mut next_group_refresh = None;
    let mut pending_group_refresh: Option<std::pin::Pin<Box<dyn std::future::Future<Output = io::Result<()>> + Send + '_>>> = None;
    let result: io::Result<()> = async {
    loop {
        identity_fence.require_current()?;
        admission.check_feeds()?;
        if !call.room_feed_ready() { return Err(error("RTC call room feed unavailable")); }
        if !answered { check_answer_deadline(answer_deadline)?; }
        let connected = matches!(*status.borrow(), CallStatus::Connected { degraded: false, .. });
        if let Some(decoder) = video_decoder.as_mut() {
            decoder.ensure_running()?;
        }
        // The roster is derived asynchronously from the core membership watch.
        // Read each once: a join may reach one before the other. Do not enable
        // media until both agree, and fail closed if an admitted peer leaves.
        let mut roster = call.participants();
        let mut members = memberships.borrow().clone();
        let independent = group_router.is_some();
        if independent {
            let session = group_session.as_ref().ok_or_else(|| error("missing active group session"))?;
            if !session.controller_view(incoming, admission, &members, &roster)? {
                return Err(error("admitted independent group membership disappeared"));
            }
            // Only the pinned human uplink is forwarded by the controller.
            // All other admitted participants own distinct child publishers.
            roster.retain(|p| p.member_id == own_member_id || p.user_id == incoming.expected_user_mxid);
            members.retain(|m| m.member_id == own_member_id || m.sender == incoming.expected_user_mxid);
        }
        if members.len() > 2 { return Err(error("RTC slot contains extra memberships")); }
        let mut peer = None;
        if let Some((id, device)) = authorized_peer(&roster, &own_member_id, &incoming.expected_user_mxid)? {
            if let Some(membership) = members.iter().find(|m| m.member_id == id
                && m.sender == incoming.expected_user_mxid && m.origin.sender_device_id() == Some(device.as_str())) {
                let generation = membership.membership_ts;
                let current = (id.clone(), device.clone(), generation);
                if (!incoming.expected_device.is_empty() && incoming.expected_device != device)
                    || expected_join.as_ref().is_some_and(|(wanted, ts, _)| wanted != &id || *ts != generation)
                    || pinned.as_ref().is_some_and(|value| value != &current) {
                    return Err(error("RTC device or membership replaced"));
                }
                if pinned.is_none() {
                    let keys = within_answer_budget(answer_deadline,
                        refresh_trusted_device(room, &membership.sender, &device)).await?;
                    if incoming.kind == "outgoing" { keys.require_invitation(incoming)?; }
                    if let Some((_, _, expected)) = &expected_join { expected.require_same(&keys)?; }
                    admission.check_feeds()?;
                    if !call.room_feed_ready() { return Err(error("RTC call room feed unavailable")); }
                    check_answer_deadline(answer_deadline)?;
                    let client = room.client();
                    {
                        let latest = memberships.borrow();
                        let allowed = [
                            client.device_id().map(|d| (incoming.ghost_mxid.as_str(), own_member_id.as_str(), Some(d.as_str()))),
                            Some((incoming.expected_user_mxid.as_str(), id.as_str(), Some(device.as_str()))),
                        ];
                        let rows = latest.iter().map(|m| (m.sender.as_str(), m.member_id.as_str(), m.origin.sender_device_id()));
                        if !only_pinned_members(rows, allowed) || !latest.iter().any(|m|
                            m.member_id == id && m.sender == incoming.expected_user_mxid
                            && m.membership_ts == generation
                            && m.origin.sender_device_id() == Some(device.as_str())) {
                            return Err(error("RTC peer changed during device refresh"));
                        }
                    }
                    admission.check_other_members(incoming, client.device_id().map(|d| (own_member_id.as_str(), d.as_str())), Some((id.as_str(), device.as_str())))?;
                    if !recipient_guard.0.pin(&incoming.expected_user_mxid, &device) {
                        return Err(error("RTC media key recipient does not match the authorised peer"));
                    }
                    pinned_keys = Some(keys);
                    pinned = Some(current);
                }
                if let Some(identity) = peer_media_identity(&membership.sender, &device, &id, &membership.transports) {
                    if answered && !call.imported_key_for(&identity) { return Err(error("RTC peer media key no longer available")); }
                    peer = Some((id, identity));
                }
            }
        }
        if answered && peer.is_none() { return Err(error("authorized Matrix participant left the RTC slot or no longer has an eligible transport")); }
        // Use the existing watch instead of acquiring another core store lock.
        if !independent && memberships.borrow().len() > 2 { return Err(error("RTC slot contains extra memberships")); }
        // An already imported and verified key is queried as state, not only
        // through a transient broadcast which may predate subscription.
        let encrypted_peer = peer.as_ref().is_some_and(|(_, identity)| call.imported_key_for(identity));
        let peer_ready = pinned.as_ref().is_some_and(|(id, device, _)|
            peer_answer_ready(&status.borrow(), &roster, &own_member_id, &incoming.ghost_mxid, &own_device_id, id, device, &incoming.expected_user_mxid));
        if !answered && encrypted_peer && connected && peer_ready && memberships.borrow().len() == 2
            && tokio::time::Instant::now() >= next_pickup_probe
            && pickup_publications_ready(&roster, &own_member_id, microphone_muted, video_decoder.is_some()) {
            next_pickup_probe = tokio::time::Instant::now() + PICKUP_RETRY_INTERVAL;
            within_answer_budget(answer_deadline, validate_room(incoming, room)).await?;
            let client = room.client();
            within_answer_budget(answer_deadline, require_prepared_identity(&client)).await?;
            admission.check_other_members(incoming, client.device_id().map(|d| (own_member_id.as_str(), d.as_str())),
                pinned.as_ref().map(|(id, device, _)| (id.as_str(), device.as_str())))?;
            check_answer_deadline(answer_deadline)?;
            if !call.room_feed_ready() { return Err(error("RTC call room feed unavailable")); }
            // Room validation awaits network/store work. Re-read the live
            // membership, transport and key state after it, before pickup.
            let (id, device, generation) = pinned.as_ref().expect("peer readiness requires a pin");
            // The initial pin may be minutes old. Do not authorize pickup
            // using only the local crypto store after the ringing interval.
            let current_keys = within_answer_budget(answer_deadline,
                refresh_trusted_device(room, &incoming.expected_user_mxid, device)).await?;
            pinned_keys.as_ref().ok_or_else(|| error("RTC peer key pin missing"))?.require_same(&current_keys)?;
            within_answer_budget(answer_deadline, identity_fence.check()).await?;
            check_answer_deadline(answer_deadline)?;
            admission.check_feeds()?;
            if !call.room_feed_ready() { return Err(error("RTC call room feed unavailable")); }
            admission.check_other_members(incoming, client.device_id().map(|d| (own_member_id.as_str(), d.as_str())), Some((id.as_str(),device.as_str())))?;
            let (_, identity) = peer.as_ref().expect("encrypted peer requires an identity");
            // Exercise the current outbound microphone publication with a
            // synthetic silence packet, never WhatsApp media before pickup.
            // A muted microphone is already acknowledged by the SDK roster.
            if !microphone_muted {
                within_answer_budget(answer_deadline,
                    capture_audio_packet(audio_track.clone(), vec![0; audio::RTC_FRAME])).await?;
            }
            // A connection or membership transition during sampling invalidates
            // the observation even when the latest values happen to match.
            let mut probe_status = call.subscribe_status();
            let _ = probe_status.borrow_and_update();
            let mut probe_memberships = memberships.clone();
            let _ = probe_memberships.borrow_and_update();
            let before = call.participants().iter().find(|p| !p.is_local && p.member_id == *id)
                .and_then(|p| pickup_tracks(call, p));
            let decoded = if let Some(tracks) = &before {
                within_answer_budget(answer_deadline, async { Ok(sample_pickup_tracks(tracks).await) }).await?
            } else { false };
            // No earlier snapshot or replaced track can authorize pickup.
            identity_fence.require_current()?;
            admission.check_feeds()?;
            if !call.room_feed_ready() { return Err(error("RTC call room feed unavailable")); }
            admission.check_other_members(incoming, client.device_id().map(|d| (own_member_id.as_str(), d.as_str())), Some((id.as_str(),device.as_str())))?;
            let current = {
                let latest = memberships.borrow();
                let allowed = [
                    Some((incoming.ghost_mxid.as_str(), own_member_id.as_str(), Some(own_device_id.as_str()))),
                    Some((incoming.expected_user_mxid.as_str(), id.as_str(), Some(device.as_str()))),
                ];
                let rows = latest.iter().map(|m| (m.sender.as_str(), m.member_id.as_str(), m.origin.sender_device_id()));
                latest.len() == 2 && only_pinned_members(rows, allowed)
                    && latest.iter().all(|m| m.application.application_type() == Some("m.call")
                        && m.slot_id == incoming.slot_id && m.room_id == incoming.room_id)
                    && latest.iter().any(|m| m.member_id == *id && m.sender == incoming.expected_user_mxid
                        && m.origin.sender_device_id() == Some(device.as_str()) && m.membership_ts == *generation
                        && peer_media_identity(&m.sender, device, id, &m.transports).as_ref() == Some(identity))
            };
            let latest_roster = call.participants();
            let after = latest_roster.iter().find(|p| !p.is_local && p.member_id == *id)
                .and_then(|p| pickup_tracks(call, p));
            let same_tracks = matches!((&before, &after), (Some(before), Some(after)) if same_pickup_tracks(before, after));
            if current && decoded && same_tracks
                && matches!(probe_status.has_changed(), Ok(false))
                && matches!(probe_memberships.has_changed(), Ok(false))
                && call.imported_key_for(identity)
                && pickup_publications_ready(&latest_roster, &own_member_id, microphone_muted, video_decoder.is_some())
                && peer_answer_ready(&status.borrow(), &latest_roster, &own_member_id, &incoming.ghost_mxid, &own_device_id, id, device, &incoming.expected_user_mxid) {
                check_answer_deadline(answer_deadline)?;
                send(writer, &Message::new("answer", &incoming.call_id)).await?;
                answered = true;
                if independent_enabled {
                    // Give provider pickup and initial roster signalling time
                    // to settle; the gateway still verifies the committed epoch.
                    request_at = Some(tokio::time::Instant::now() + Duration::from_secs(3));
                }
                roster = latest_roster;
                next_peer_refresh = Some(tokio::time::Instant::now() + ACTIVE_PEER_REFRESH_INTERVAL);
                eprintln!("RTC answer gate: authenticated membership and key; current publications and active peer media ready");
                // The local socket write above is serialized and completes before
                // media is enabled, so Go can never observe media before answer.
                media_active.store(true, Ordering::Release);
            } else {
                next_pickup_probe = tokio::time::Instant::now() + PICKUP_RETRY_INTERVAL;
            }
        }

        if answered {
            if let Some((id,identity))=&peer {
                if !call.imported_key_for(identity) {return Err(error("RTC peer key unavailable"))}
                let participant=roster.iter().find(|p| &p.member_id==id).ok_or_else(||error("RTC peer left"))?;
                let video_kind = if screen_switch_enabled && participant.streams.iter().any(|s| s.kind == MediaStreamKind::ScreenShare && !s.muted) {
                    MediaStreamKind::ScreenShare
                } else { MediaStreamKind::Camera };
                for (key,kind) in [("audio",MediaStreamKind::Microphone),("video",video_kind)] {
                    let state=participant.streams.iter().find(|s|s.kind==kind).map(|s|s.muted);
                    // A replacement can preserve the same roster kind and mute state.
                    // Reconcile against the actual pooled handle, not a delayed stop event.
                    let track = if state == Some(false) { call.remote_track(id,kind) } else { None };
                    let role_changed = key == "video" && selected_video_kind != kind;
                    let replaced = remote_tasks.0.get(key).is_some_and(|forwarder| track_replaced(&forwarder.track, track.as_ref()));
                    if role_changed || replaced {
                        if let Some(forwarder) = remote_tasks.0.remove(key) {
                            forwarder.task.abort();
                            let _ = forwarder.task.await;
                        }
                    }
                    if key == "video" {
                        selected_video_kind = kind;
                        // A real camera/display handoff should keep the provider's
                        // primary sender alive. The next generation's start retires
                        // the old source. If attachment stalls, stop instead of
                        // leaving a phantom share or camera indefinitely.
                        if screen_switch_enabled && outbound_video_started && role_changed && track.is_some() {
                            video_switch_deadline = Some(tokio::time::Instant::now() + Duration::from_secs(2));
                        }
                        let switching = video_switch_deadline.is_some_and(|deadline| tokio::time::Instant::now() < deadline);
                        if screen_switch_enabled && outbound_video_started && (track.is_none()
                            || (!switching && (role_changed || replaced || !remote_tasks.0.contains_key(key)))
                            || remote_tasks.0.get(key).is_some_and(|forwarder| forwarder.task.is_finished())) {
                            send(writer, &Message::new("matrix_video_stop", &incoming.call_id)).await?;
                            outbound_video_started = false;
                            video_switch_deadline = None;
                        }
                    }
                    if role_changed || remote_recovery.get(key).is_some_and(|entry| track_replaced(&entry.track, track.as_ref())) {
                        remote_recovery.remove(key);
                    }
                    if let Some(track) = track.as_ref() {
                        let recovery = remote_recovery.entry(key.to_owned()).or_insert_with(|| RemoteRecovery {
                            track: track.clone(), retry: RemoteRetry::default(),
                        });
                        if remote_tasks.0.get(key).is_some_and(|forwarder| forwarder.task.is_finished()) {
                            let forwarder = remote_tasks.0.remove(key).expect("finished task remains owned by session");
                            forwarder.task.await.map_err(io::Error::other)??;
                            recovery.retry.failed(tokio::time::Instant::now())?;
                        }
                    }
                    if states.get(key)!=Some(&state) {
                        let previous=states.insert(key.to_owned(),state);
                        let control=match (key,previous,state) {
                            ("audio",_,None|Some(true))=>Some("matrix_audio_mute"),
                            ("audio",_,Some(false))=>Some("matrix_audio_unmute"),
                            ("video",Some(Some(_)),None)=>Some("matrix_video_stop"),
                            ("video",_,Some(true))=>Some("matrix_video_mute"),
                            ("video",Some(Some(true)),Some(false))=>Some("matrix_video_unmute"),
                            ("video",_,Some(false))=>Some("matrix_video_start"),
                            _=>None
                        };
                        // For source-switching sessions, start only after a real
                        // track is available and stop through the ordered fence above.
                        if !(screen_switch_enabled && key == "video") {
                            if let Some(ctrl)=control {send(writer,&Message::new(ctrl,&incoming.call_id)).await?;}
                        }
                    }
                    if state == Some(false) && !remote_tasks.0.contains_key(key) && pending_tracks.is_empty() && track.is_some()
                        && remote_recovery.get(key).is_some_and(|entry| entry.retry.ready(tokio::time::Instant::now())) {
                        pending_tracks.push_back(CallEvent::StreamStarted{member_id:id.clone(),kind});
                    }
                }
            }
        }
        // The refresh future is owned by this session, polled beside IPC and
        // cancelled on session exit. Never spawn overlapping refreshes.
        if answered && pending_peer_refresh.is_none()
            && next_peer_refresh.is_some_and(|deadline| tokio::time::Instant::now() >= deadline) {
            let (_, device, _) = pinned.as_ref().ok_or_else(|| error("RTC peer key pin missing"))?;
            pending_peer_refresh = Some(start_peer_refresh(room, incoming.expected_user_mxid.clone(), device.clone()));
            next_peer_refresh = None;
        }
        if answered && !independent && group_deadline.is_none()
            && request_at.is_some_and(|when| tokio::time::Instant::now() >= when)
            && pending_group_check.is_none() && group_checks.is_empty() && encrypted_peer && connected {
            request_at = None;
            group_attempts += 1;
            let deadline = tokio::time::Instant::now() + Duration::from_secs(40);
            within_answer_budget(deadline, send(writer, &group_publication_session::GroupPublicationSession::request(incoming)?)).await?;
            group_deadline = Some(deadline);
        }
        if let Some(deadline) = group_deadline {
            check_answer_deadline(deadline)?;
        }
        if independent && pending_group_refresh.is_none()
            && next_group_refresh.is_some_and(|when| tokio::time::Instant::now() >= when) {
            let session = group_session.as_ref().ok_or_else(|| error("missing group refresh session"))?.clone();
            pending_group_refresh = Some(Box::pin(async move { session.refresh(tokio::time::Instant::now() + Duration::from_secs(15)).await }));
            next_group_refresh = None;
        }
        if pending_group_check.is_none() && group_deadline.is_none() {
            if let Some((targets, deadline)) = group_checks.pop_front() {
                let (_, device, _) = pinned.as_ref().ok_or_else(|| error("RTC group caller pin missing"))?;
                pending_group_check = Some(start_group_check(incoming, room, device.clone(), targets, deadline));
            }
        }
        tokio::select! {
            result = async {
                match pending_group_refresh.as_mut() {
                    Some(check) => check.await,
                    None => std::future::pending::<io::Result<()>>().await,
                }
            } => {
                pending_group_refresh = None;
                result?;
                next_group_refresh = Some(tokio::time::Instant::now() + ACTIVE_PEER_REFRESH_INTERVAL);
            }
            result = poll_group_check(&mut pending_group_check) => {
                pending_group_check = None;
                let (targets, keys) = result?;
                pinned_keys.as_ref().ok_or_else(|| error("RTC peer key pin missing"))?.require_same(&keys)?;
                let (id,device,generation)=pinned.as_ref().ok_or_else(||error("RTC group caller pin missing"))?;
                admission.check_feeds()?;
                if !call.room_feed_ready() { return Err(error("RTC call room feed unavailable")); }
                let full_roster = call.participants();
                let full_members = memberships.borrow().clone();
                if let Some(session) = group_session.as_ref() {
                    if !session.controller_view(incoming, admission, &full_members, &full_roster)? {
                        return Err(error("group invitation membership changed"));
                    }
                } else {
                    admission.check_other_members(incoming, Some((own_member_id.as_str(), own_device_id.as_str())),
                        Some((id.as_str(), device.as_str())))?;
                }
                let pair_roster: Vec<_> = full_roster.into_iter().filter(|p| p.member_id == own_member_id || p.user_id == incoming.expected_user_mxid).collect();
                let current = {
                    let latest: Vec<_> = full_members.iter().filter(|m| m.member_id == own_member_id || m.sender == incoming.expected_user_mxid).collect();
                    let allowed = [
                        Some((incoming.ghost_mxid.as_str(), own_member_id.as_str(), Some(own_device_id.as_str()))),
                        Some((incoming.expected_user_mxid.as_str(), id.as_str(), Some(device.as_str()))),
                    ];
                    let rows = latest.iter().map(|m| (m.sender.as_str(), m.member_id.as_str(), m.origin.sender_device_id()));
                    latest.len() == 2 && only_pinned_members(rows, allowed)
                        && latest.iter().all(|m| m.application.application_type() == Some("m.call")
                            && m.slot_id == incoming.slot_id && m.room_id == incoming.room_id)
                        && latest.iter().any(|m| m.member_id == *id && m.sender == incoming.expected_user_mxid
                            && m.membership_ts == *generation && m.origin.sender_device_id() == Some(device.as_str())
                            && peer_media_identity(&m.sender, device, id, &m.transports)
                                .is_some_and(|identity| call.imported_key_for(&identity)))
                };
                if !current || !peer_answer_ready(&status.borrow(), &pair_roster, &own_member_id,
                    &incoming.ghost_mxid, &own_device_id, id, device, &incoming.expected_user_mxid) {
                    return Err(error("RTC group invitation caller changed"));
                }
                identity_fence.require_current()?;
                let mut command=Message::new("matrix_group_invite",&incoming.call_id);
                command.targets=targets;
                send(writer,&command).await?;
            }
            result = poll_peer_refresh(&mut pending_peer_refresh) => {
                pending_peer_refresh = None;
                let current = result?;
                pinned_keys.as_ref().ok_or_else(|| error("RTC peer key pin missing"))?.require_same(&current)?;
                next_peer_refresh = Some(tokio::time::Instant::now() + ACTIVE_PEER_REFRESH_INTERVAL);
            }
            _=topology.tick()=> {
                // Keep synchronous alternate-membership checks on every tick,
                // even while the independent store read is pending.
                if !independent && (answered || pinned.is_some()) {
                    admission.check_other_members(incoming, Some((own_member_id.as_str(), own_device_id.as_str())),
                        pinned.as_ref().map(|(id, device, _)| (id.as_str(), device.as_str())))?;
                }
                if pending_topology.is_none() {
                    let limit = tokio::time::Instant::now() + TOPOLOGY_CHECK_BUDGET;
                    let deadline = if answered { limit } else { limit.min(answer_deadline) };
                    pending_topology = Some(start_topology_check(incoming, room, identity_fence,
                        pinned.as_ref().map(|(_, device, _)| device.clone()), deadline));
                }
            }
            result = poll_topology_check(&mut pending_topology) => {
                pending_topology = None;
                if let Some(current) = result? {
                    pinned_keys.as_ref().ok_or_else(|| error("RTC peer key pin missing"))?.require_same(&current)?;
                }
                if !independent && (answered || pinned.is_some()) {
                    admission.check_other_members(incoming, Some((own_member_id.as_str(), own_device_id.as_str())),
                        pinned.as_ref().map(|(id, device, _)| (id.as_str(), device.as_str())))?;
                }
            }
            change=memberships.changed()=>{change.map_err(|_|error("RTC membership watch closed"))?;}
            change = roster_updates.changed() => {
                change.map_err(|_| error("MatrixRTC roster watch closed"))?;
                let _ = roster_updates.borrow_and_update();
            }
            event = status.changed() => {
                event.map_err(|_| error("call status channel closed"))?;
                let end = match &*status.borrow_and_update() {
                    CallStatus::Ended { reason } => Some(ended_event(answered, reason)),
                    _ => None,
                };
                if let Some(kind) = end {
                    let mut event = Message::new(kind, &incoming.call_id);
                    if kind == "error" { event.reason = "MatrixRTC session ended before pickup".to_owned(); }
                    send(writer, &event).await?;
                    break;
                }
            }
            event = async {
                if let Some(event)=pending_tracks.pop_front(){(true,Ok(event))}else{(false,events.recv().await)}
            } => match event {
                (false,Ok(CallEvent::StreamStarted {..}))=>{},
                (_,Ok(CallEvent::KeyImported { .. })) => { /* re-read SDK's key state + roster */ }
                (_,Ok(CallEvent::KeyDiscarded { member_id, .. })) if peer.as_ref().is_some_and(|(id,_)| id == &member_id) => {
                    return Err(error("authorized participant's MatrixRTC media key was discarded"));
                }
                (true,Ok(CallEvent::StreamStarted { member_id, kind: MediaStreamKind::Microphone }))
                    if states.get("audio") == Some(&Some(false)) && !remote_tasks.0.contains_key("audio") && peer.as_ref().is_some_and(|(id, identity)| id == &member_id && call.imported_key_for(identity)) => {
                    let Some(track) = call.remote_track(&member_id, MediaStreamKind::Microphone) else { continue; };
                    let Some(mut frames) = track.audio_frames() else {
                        if let Some(entry) = remote_recovery.get_mut("audio") {
                            entry.retry.failed(tokio::time::Instant::now())?;
                        }
                        continue;
                    };
                    let writer = writer.clone();
                    let call_id = incoming.call_id.clone();
                    let active = media_active.clone();
                    let task = tokio::spawn(async move {
                        let mut buffered = Vec::<i16>::with_capacity(2880);
                        while let Some(frame) = frames.next().await {
                            if !active.load(Ordering::Acquire) {
                                buffered.clear();
                                continue;
                            }
                            if frame.sample_rate != 48_000 || frame.num_channels != 1 {
                                return Err(error(format!(
                                    "unexpected MatrixRTC audio format: {} Hz / {} channels",
                                    frame.sample_rate, frame.num_channels
                                )));
                            }
                            buffered.extend_from_slice(&frame.data);
                            while buffered.len() >= 2880 {
                                let chunk: Vec<i16> = buffered.drain(..2880).collect();
                                let pcm = audio::downsample_48_to_16(&chunk);
                                let data = protocol::encode_pcm(&pcm)?;
                                let mut message = Message::new("matrix_audio", &call_id);
                                message.data = data;
                                send(&writer, &message).await?;
                            }
                        }
                        Ok(())
                    });
                    remote_tasks.0.insert("audio".to_owned(), RemoteForwarder { track, task });
                }
                (true,Ok(CallEvent::StreamStarted { member_id, kind }))
                    if kind == selected_video_kind && matches!(kind, MediaStreamKind::Camera | MediaStreamKind::ScreenShare) && states.get("video") == Some(&Some(false)) && !remote_tasks.0.contains_key("video") && peer.as_ref().is_some_and(|(id, identity)| id == &member_id && call.imported_key_for(identity)) => {
                    video::verify_ffmpeg().await?;
                    let Some(track) = call.remote_track(&member_id, kind) else { continue; };
                    let Some(frames) = track.video_frames() else {
                        if let Some(entry) = remote_recovery.get_mut("video") {
                            entry.retry.failed(tokio::time::Instant::now())?;
                        }
                        continue;
                    };
                    let writer = writer.clone();
                    let call_id = incoming.call_id.clone();
                    let active = media_active.clone();
                    let keyframe_generation = video_keyframe_generation.clone();
                    // A replacement or retry may retain the same roster state.
                    // Reset the bridge's dependency chain before this encoder sends.
                    let mut start = Message::new(if kind == MediaStreamKind::ScreenShare { "matrix_video_screen_start" } else { "matrix_video_start" }, &call_id);
                    if screen_switch_enabled {
                        outbound_video_generation = outbound_video_generation.checked_add(1).ok_or_else(|| error("video source generation exhausted"))?;
                        start.generation = outbound_video_generation;
                        start.group = true;
                    }
                    let source_generation = start.generation;
                    send(&writer, &start).await?;
                    outbound_video_started = screen_switch_enabled;
                    video_switch_deadline = None;
                    let task = tokio::spawn(async move {
                        video::matrix_video_to_whatsapp(
                            frames, writer, call_id, active, keyframe_generation, source_generation,
                        ).await
                    });
                    remote_tasks.0.insert("video".to_owned(), RemoteForwarder { track, task });
                }
                // Stop/start notifications can be queued across replacements.
                // The next loop reconciles the current roster and pooled handle.
                (_,Ok(CallEvent::StreamStopped { .. })) => {},
                (_,Ok(_))=>{},
                (_,Err(err))=>return Err(error(format!("RTC event stream interrupted: {err}"))),
            },
            result = poll_audio_capture(&mut pending_audio) => {
                pending_audio = None;
                result?;
            }
            result = reader.read() => {
                let msg = result?;
                if msg.call_id != incoming.call_id { return Err(error("call ID mismatch")); }
                match msg.kind.as_str() {
                    "group_check" if incoming.group && answered && encrypted_peer => {
                        enqueue_group_check(&mut group_checks, pending_group_check.is_some(), msg.targets,
                            tokio::time::Instant::now() + TOPOLOGY_CHECK_BUDGET)?;
                    }
                    "whatsapp_participant_unavailable" if answered && !independent && group_deadline.is_some() => {
                        // No proposal was emitted; the aggregate path remains
                        // intact. Retry only while the original call is admitted.
                        let deadline = group_deadline.take().ok_or_else(|| error("unsolicited group negotiation response"))?;
                        check_answer_deadline(deadline)?;
                        if group_attempts < 3 {
                            request_at = Some(tokio::time::Instant::now() + Duration::from_secs(5));
                        } else if group_mode_started { return Err(error("independent group re-admission unavailable")); }
                    }
                    "whatsapp_participant_roster" if answered && !independent && group_deadline.is_some() => {
                        let deadline = group_deadline.take().ok_or_else(|| error("unsolicited participant roster"))?;
                        if msg.generation <= latest_group_generation { return Err(error("group roster generation replay")); }
                        let (human_id, human_device, human_generation) = pinned.as_ref().ok_or_else(|| error("group human pin missing"))?;
                        let (human, controller_pin) = {
                            let latest = memberships.borrow();
                            if latest.len() != 2 { return Err(error("group controller changed before handoff")); }
                            let human = latest.iter().find(|m| m.member_id == *human_id && m.sender == incoming.expected_user_mxid
                                && m.origin.sender_device_id() == Some(human_device.as_str()) && m.membership_ts == *human_generation)
                                .ok_or_else(|| error("group human membership changed"))?.clone();
                            let controller_pin = latest.iter().find(|m| m.member_id == own_member_id && m.sender == incoming.bridge_bot_mxid
                                && m.origin.sender_device_id() == Some(own_device_id.as_str()))
                                .ok_or_else(|| error("group controller membership changed"))?.clone();
                            (human, controller_pin)
                        };
                        admission.check_other_members(incoming, Some((own_member_id.as_str(), own_device_id.as_str())),
                            Some((human_id.as_str(), human_device.as_str())))?;
                        identity_fence.require_current()?;
                        let keys = pinned_keys.as_ref().ok_or_else(|| error("group human key pin missing"))?;
                        let fresh = within_answer_budget(deadline, refresh_trusted_device(room, &incoming.expected_user_mxid, human_device)).await?;
                        keys.require_same(&fresh)?;
                        let bindings = group_publication_session::GroupPublicationSession::resolve_bindings(accounts, incoming, &msg, deadline).await?;
                        // The roster proposal follows the gateway's final aggregate
                        // frame. Quiesce the old local publications before joining
                        // distinct ghosts. Human uplink forwarders remain owned by
                        // the controller and are not restarted by the handoff.
                        media_active.store(false, Ordering::Release);
                        if !group_mode_started {
                        pending_audio = None;
                        microphone_muted = true;
                        last_sample = 0.0;
                        within_answer_budget(deadline, async {
                            stop_whatsapp_video_track(call, &mut video_decoder).await?;
                            call.engine().unpublish(MediaStreamKind::Microphone).await.map_err(io::Error::other)
                        }).await?;
                        }
                        group_mode_started = true;
                        latest_group_generation = msg.generation;
                        let session = group_publication_session::GroupPublicationSession::join(accounts, incoming,
                            call, &human, &controller_pin, keys, msg.generation, bindings, deadline).await?;
                        if group_session.is_some() { let _ = session.close().await; return Err(error("independent group session already installed")); }
                        group_session = Some(Arc::new(session));
                        let session = group_session.as_ref().ok_or_else(|| error("group session installation failed"))?;
                        loop {
                            check_answer_deadline(deadline)?;
                            let latest = memberships.borrow().clone();
                            if session.controller_view(incoming, admission, &latest, &call.participants())? { break; }
                            tokio::time::sleep(Duration::from_millis(100)).await;
                        }
                        let router = group_publication_router::GroupPublicationRouter::new(session.clone(), incoming, &msg)?;
                        let ready = session.ready(incoming, &msg, deadline).await?;
                        identity_fence.require_current()?;
                        if !session.controller_view(incoming, admission, &memberships.borrow().clone(), &call.participants())? {
                            return Err(error("group controller view changed before acknowledgement"));
                        }
                        group_router = Some(router);
                        within_answer_budget(deadline, send(writer, &ready)).await?;
                        media_active.store(true, Ordering::Release);
                        next_group_refresh = Some(tokio::time::Instant::now() + ACTIVE_PEER_REFRESH_INTERVAL);
                    }
                    "whatsapp_participant_epoch" | "whatsapp_participant_audio" | "whatsapp_participant_video" | "whatsapp_participant_state" if independent => {
                        let router = group_router.as_mut().ok_or_else(|| error("missing independent router"))?;
                        match router.dispatch(msg).await? {
                            group_publication_router::RouteResult::RequestKeyframe(request) => send(writer, &request).await?,
                            group_publication_router::RouteResult::Retired => {
                                media_active.store(false, Ordering::Release);
                                let previous = memberships.borrow().clone();
                                pending_group_refresh = None;
                                next_group_refresh = None;
                                retire_independent_group(&mut group_router, &mut group_session).await?;
                                // Child leave events may arrive asynchronously. Permit only
                                // the exact prior memberships until the original pair remains.
                                let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
                                loop {
                                    check_answer_deadline(deadline)?;
                                    identity_fence.require_current()?;
                                    admission.check_feeds()?;
                                    let latest = memberships.borrow().clone();
                                    if latest.iter().any(|m| !previous.iter().any(|p| p.same_participation(m) && p.origin.was_encrypted() == m.origin.was_encrypted())) {
                                        return Err(error("group membership changed during retirement"));
                                    }
                                    let pair = latest.iter().filter(|m| m.sender == incoming.expected_user_mxid || m.member_id == own_member_id).count();
                                    if pair != 2 { return Err(error("original group controller pair left")); }
                                    if latest.len() == 2 && call.participants().len() == 2 {
                                        let (id,device,_) = pinned.as_ref().ok_or_else(|| error("missing group caller pin"))?;
                                        admission.check_other_members(incoming,Some((own_member_id.as_str(),own_device_id.as_str())),Some((id.as_str(),device.as_str())))?;
                                        break;
                                    }
                                    tokio::time::sleep(Duration::from_millis(100)).await;
                                }
                                group_attempts = 0;
                                request_at = Some(tokio::time::Instant::now() + Duration::from_secs(1));
                                media_active.store(true, Ordering::Release);
                            },
                            _ => {},
                        }
                    }
                    "whatsapp_audio" | "whatsapp_video" if group_mode_started => {
                        return Err(error("aggregate media received after independent barrier"));
                    }
                    "whatsapp_audio_mute" | "whatsapp_audio_unmute" | "whatsapp_video_state" if group_mode_started => {
                        // Aggregate provider state must not resurrect controller
                        // publications. The child media sources own their tracks.
                    }
                    "whatsapp_audio" if answered => {
                        let samples = protocol::pcm(&msg.data)?;
                        if !microphone_muted && pending_audio.is_none() {
                            let upsampled = audio::upsample_16_to_48(&samples, last_sample);
                            last_sample = *samples.last().unwrap_or(&last_sample);
                            pending_audio = Some(capture_audio_packet(audio_track.clone(), upsampled));
                        }
                        // Discard overload rather than accumulate latency or block control.
                    }
                    "whatsapp_video" if answered => {
                        if msg.generation<=rejected_generation {continue}
                        let deadline = media_control_deadline(tokio::time::Instant::now(), answered, answer_deadline);
                        let resync = bounded_media_control(deadline, async {
                            if video_decoder.as_ref().is_some_and(|d|d.generation!=0 && d.generation!=msg.generation){
                                stop_whatsapp_video_track(call,&mut video_decoder).await?;
                            }
                            ensure_whatsapp_video_track(call,&mut video_decoder).await?;
                            let decoder=video_decoder.as_mut().ok_or_else(||error("decoder unavailable"))?;
                            decoder.generation=msg.generation;
                            if !decoder.push(msg.data,msg.orientation,msg.timestamp)? {
                                rejected_generation=msg.generation;
                                stop_whatsapp_video_track(call,&mut video_decoder).await?;
                                return Ok(true);
                            }
                            Ok(false)
                        }).await?;
                        if resync {
                            send(writer,&Message::new("matrix_video_resync",&incoming.call_id)).await?;
                        }
                    }
                    "whatsapp_video_state" => {
                        let deadline = media_control_deadline(tokio::time::Instant::now(), answered, answer_deadline);
                        bounded_media_control(deadline, async {
                            match msg.state {
                                1 => {
                                    ensure_whatsapp_video_track(call, &mut video_decoder).await?;
                                    call.engine().set_local_muted(MediaStreamKind::Camera, false).await.map_err(io::Error::other)?;
                                }
                                0 => {
                                    if video_decoder.is_some() {
                                        call.engine().set_local_muted(MediaStreamKind::Camera, true).await.map_err(io::Error::other)?;
                                    }
                                }
                                6 => stop_whatsapp_video_track(call, &mut video_decoder).await?,
                                3 | 4 | 5 | 8 | 11 => { /* negotiation state; media changes on 0/1/6 */ }
                                _ => return Err(error(format!("unsupported WhatsApp video state {}", msg.state))),
                            }
                            Ok(())
                        }).await?;
                    }
                    "whatsapp_audio_mute" => {
                        microphone_muted = true;
                        pending_audio = None;
                        last_sample = 0.0;
                        let deadline = media_control_deadline(tokio::time::Instant::now(), answered, answer_deadline);
                        bounded_media_control(deadline, async {
                            call.engine().set_local_muted(MediaStreamKind::Microphone, true).await.map_err(io::Error::other)
                        }).await?;
                    }
                    "whatsapp_audio_unmute" => {
                        let deadline = media_control_deadline(tokio::time::Instant::now(), answered, answer_deadline);
                        bounded_media_control(deadline, async {
                            call.engine().set_local_muted(MediaStreamKind::Microphone, false).await.map_err(io::Error::other)
                        }).await?;
                        microphone_muted = false;
                    }
                    // PLI/FIR may race ahead of local answer/video negotiation.
                    "whatsapp_video_keyframe" => {
                        video_keyframe_generation.fetch_add(1, Ordering::AcqRel);
                    }
                    "ended" | "hangup" | "cancel" => break,
                    _ => return Err(error("unexpected WhatsApp event in current state")),
                }
            }
            _ = &mut expiry, if !answered => {
                eprintln!("RTC answer gate timeout: connected={connected} membership_count={} roster_count={} pinned={} identity_resolved={} peer_key_imported={encrypted_peer}",
                    members.len(), roster.len(), pinned.is_some(), peer.is_some());
                return Err(error("RTC answer setup deadline exceeded"));
            }
        }
    }

    Ok(())
    }.await;

    media_active.store(false, Ordering::Release);
    drop(remote_tasks);
    drop(pending_audio);
    drop(video_decoder);
    drop(pending_group_refresh);
    result.and(retire_independent_group(&mut group_router,&mut group_session).await)
}

async fn retire_independent_group(router: &mut Option<group_publication_router::GroupPublicationRouter>,
    session: &mut Option<Arc<group_publication_session::GroupPublicationSession>>) -> io::Result<()> {
    let retired = match router.take() { Some(router) => router.retire().await, None => Ok(()) };
    let closed = match session.take() {
        Some(session) => match Arc::try_unwrap(session) {
            Ok(session) => session.close().await,
            Err(session) => { session.revoke(); Err(error("group session still referenced during retirement")) },
        },
        None => Ok(()),
    };
    retired.and(closed)
}

#[cfg(test)]
mod authorization_tests {
    use super::*;

    #[tokio::test]
    async fn recovery_lease_revalidation_rejects_replaced_or_missing_generation() {
        let folder = std::env::temp_dir().join(format!("matrixrtc-lease-test-{}-{}",
            std::process::id(), std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos()));
        std::fs::create_dir_all(&folder).unwrap();
        struct Cleanup(PathBuf);
        impl Drop for Cleanup { fn drop(&mut self) { let _ = std::fs::remove_dir_all(&self.0); } }
        let _cleanup = Cleanup(folder.clone());
        let accounts = Accounts::new(folder.join("identities"));
        let mut original = Message::new("incoming", "old-call");
        original.room_id = "!room:example.org".to_owned();
        original.slot_id = "slot".to_owned();
        let path = accounts.lease_path(&original);
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(&path, serde_json::to_vec(&original).unwrap()).unwrap();
        assert!(lease_matches(&path, &original).await.unwrap());
        let mut next = original.clone();
        next.call_id = "new-call".to_owned();
        let owned = accounts.reserve(&next.room_id, &next.slot_id).unwrap();
        std::fs::write(&path, serde_json::to_vec(&next).unwrap()).unwrap();
        drop(owned);
        let _recovery = accounts.reserve(&original.room_id, &original.slot_id).unwrap();
        assert!(!lease_matches(&path, &original).await.unwrap());
        assert!(lease_matches(&path, &next).await.unwrap());
        std::fs::write(&path, b"invalid").unwrap();
        assert!(lease_matches(&path, &next).await.is_err());
        std::fs::remove_file(&path).unwrap();
        assert!(!lease_matches(&path, &next).await.unwrap());
    }

    #[test]
    fn exclusive_dm_only_excludes_gateway_bot() {
        let (human, ghost, bot, other) = ("@human:example.org", "@ghost:example.org", "@bot:example.org", "@other:example.org");
        assert!(exclusive_dm_members([human, ghost], human, ghost, None));
        assert!(exclusive_dm_members([bot, human, ghost], human, ghost, Some(bot)));
        assert!(!exclusive_dm_members([human, ghost, other], human, ghost, Some(bot)));
        assert!(!exclusive_dm_members([human, ghost, bot, other], human, ghost, Some(bot)));
        assert!(!exclusive_dm_members([human, bot], human, ghost, Some(bot)));
        assert!(!exclusive_dm_members([human, ghost, bot], human, ghost, None));
        assert!(!exclusive_dm_members([human, ghost, bot, bot], human, ghost, Some(bot)));
        assert!(!exclusive_dm_members([human, ghost, human], human, ghost, Some(human)));
    }

    #[test]
    fn remote_forwarder_retry_is_delayed_and_bounded() {
        let mut retry = RemoteRetry::default();
        let start = tokio::time::Instant::now();
        assert!(retry.ready(start));
        retry.failed(start).unwrap();
        assert!(!retry.ready(start));
        assert!(retry.ready(start + REMOTE_RETRY_DELAY));
        retry.failed(start + REMOTE_RETRY_DELAY).unwrap();
        assert!(!retry.ready(start + REMOTE_RETRY_DELAY));
        assert!(retry.ready(start + REMOTE_RETRY_DELAY * 2));
        assert!(retry.failed(start + REMOTE_RETRY_DELAY * 2).is_err());
        assert!(RemoteRetry::default().ready(start));
    }

    #[test]
    fn remote_track_identity_distinguishes_replacement_from_duplicate() {
        let old = Arc::new(7u8);
        let duplicate = old.clone();
        let replacement = Arc::new(7u8);
        assert!(!track_replaced(&old, Some(&duplicate)));
        assert!(track_replaced(&old, Some(&replacement)));
        assert!(track_replaced(&old, None));
    }

    #[tokio::test]
    async fn blocked_audio_capture_does_not_block_ready_control() {
        let mut capture: Option<PendingAudio> = Some(Box::pin(std::future::pending()));
        let control = tokio::select! {
            biased;
            _ = poll_audio_capture(&mut capture) => false,
            _ = std::future::ready(()) => true,
        };
        assert!(control);
        assert!(capture.is_some());
        capture = None;
        assert!(tokio::time::timeout(Duration::from_millis(1), poll_audio_capture(&mut capture)).await.is_err());
    }

    #[tokio::test]
    async fn active_peer_refresh_does_not_block_ready_control_or_lose_future() {
        let mut refresh: Option<PendingPeerRefresh<'_>> = Some(Box::pin(std::future::pending()));
        let control = tokio::select! {
            biased;
            _ = poll_peer_refresh(&mut refresh) => false,
            _ = std::future::ready(()) => true,
        };
        assert!(control);
        assert!(refresh.is_some());
        refresh = Some(Box::pin(async { Err(error("refresh failed")) }));
        assert_eq!(poll_peer_refresh(&mut refresh).await.unwrap_err().to_string(), "refresh failed");
        refresh = None;
        assert!(tokio::time::timeout(Duration::from_millis(1), poll_peer_refresh(&mut refresh)).await.is_err());
    }

    #[tokio::test]
    async fn pending_topology_retains_check_without_blocking_control() {
        let mut check: Option<PendingTopology<'_>> = Some(Box::pin(std::future::pending()));
        let control = tokio::select! {
            biased;
            _ = poll_topology_check(&mut check) => false,
            _ = std::future::ready(()) => true,
        };
        assert!(control);
        assert!(check.is_some());
        check = Some(Box::pin(async { Err(error("store unavailable")) }));
        assert_eq!(poll_topology_check(&mut check).await.unwrap_err().to_string(), "store unavailable");
        check = None;
        assert!(tokio::time::timeout(Duration::from_millis(1), poll_topology_check(&mut check)).await.is_err());
    }

    #[test]
    fn group_check_queue_counts_in_flight_and_preserves_order() {
        let mut queue = std::collections::VecDeque::new();
        let deadline = tokio::time::Instant::now() + TOPOLOGY_CHECK_BUDGET;
        for i in 0..GROUP_CHECK_CAPACITY - 1 {
            enqueue_group_check(&mut queue, true, vec![i.to_string()], deadline).unwrap();
        }
        assert!(enqueue_group_check(&mut queue, true, vec!["overflow".to_owned()], deadline).is_err());
        for i in 0..GROUP_CHECK_CAPACITY - 1 {
            assert_eq!(queue.pop_front(), Some((vec![i.to_string()], deadline)));
        }
    }

    #[tokio::test]
    async fn group_check_does_not_block_control_or_lose_request() {
        let mut check: Option<PendingGroupCheck<'_>> = Some(Box::pin(std::future::pending()));
        let control = tokio::select! {
            biased;
            _ = poll_group_check(&mut check) => false,
            _ = std::future::ready(()) => true,
        };
        assert!(control);
        assert!(check.is_some());
        let keys = DeviceKeys { signing: "s".to_owned(), identity: "i".to_owned(), master: None };
        check = Some(Box::pin(std::future::ready(Ok((vec!["target".to_owned()], keys.clone())))));
        assert_eq!(poll_group_check(&mut check).await.unwrap(), (vec!["target".to_owned()], keys));
        check = None;
        assert!(tokio::time::timeout(Duration::from_millis(1), poll_group_check(&mut check)).await.is_err());
    }

    #[tokio::test]
    async fn topology_deadline_rejects_ready_result_after_expiry() {
        let expired = tokio::time::Instant::now();
        assert!(bounded_topology(expired, std::future::ready(Ok(()))).await.is_err());
        let deadline = tokio::time::Instant::now() + Duration::from_millis(5);
        assert!(bounded_topology(deadline, std::future::pending::<io::Result<()>>()).await.is_err());
    }

    #[tokio::test]
    async fn completed_audio_capture_propagates_error() {
        let mut capture: Option<PendingAudio> = Some(Box::pin(async { Err(error("capture failed")) }));
        assert_eq!(poll_audio_capture(&mut capture).await.unwrap_err().to_string(), "capture failed");
    }

    #[test]
    fn join_deadline_never_extends_answer_budget() {
        let start = tokio::time::Instant::now();
        for budget in [INCOMING_ANSWER_BUDGET, OUTGOING_ANSWER_BUDGET] {
            let deadline = start + budget;
            assert_eq!(join_deadline(start, deadline), start + JOIN_BUDGET);
            let late = deadline - Duration::from_secs(3);
            assert_eq!(join_deadline(late, deadline), deadline);
            assert_eq!(join_deadline(deadline, deadline), deadline);
        }
    }

    #[test]
    fn expired_answer_budget_refuses_setup() {
        let now = tokio::time::Instant::now();
        assert!(check_answer_deadline(now - Duration::from_secs(1)).is_err());
        assert!(check_answer_deadline(now + Duration::from_secs(30)).is_ok());
    }

    #[tokio::test]
    async fn final_answer_lookup_shares_original_deadline() {
        let now = tokio::time::Instant::now();
        let expired: io::Result<()> = within_answer_budget(now, async {
            panic!("expired pickup must not poll its lookup");
        }).await;
        assert!(expired.is_err());
        let result = within_answer_budget(now + Duration::from_secs(5), async {
            Ok::<_, io::Error>(42)
        }).await.unwrap();
        assert_eq!(result, 42);
        let failed = within_answer_budget(now + Duration::from_secs(5), async {
            Err::<(), _>(error("refresh failed"))
        }).await.unwrap_err();
        assert_eq!(failed.to_string(), "refresh failed");
        let timed_out = within_answer_budget(tokio::time::Instant::now() + Duration::from_millis(20),
            std::future::pending::<io::Result<()>>()).await.unwrap_err();
        assert_eq!(timed_out.to_string(), "RTC answer setup deadline exceeded");
    }

    #[tokio::test]
    async fn consecutive_ring_and_pickup_checks_do_not_renew_budget() {
        let deadline = tokio::time::Instant::now() + Duration::from_millis(100);
        within_answer_budget(deadline, async { Ok(()) }).await.unwrap();
        tokio::time::sleep_until(deadline).await;
        // A deadline inherited from an earlier phase must not be renewed by
        // another successful operation or a continuously ready event stream.
        assert!(within_answer_budget(deadline, async { Ok(()) }).await.is_err());
        assert!(check_answer_deadline(deadline).is_err());
    }

    #[test]
    fn media_control_budget_never_extends_ringing() {
        let now = tokio::time::Instant::now();
        let answer = now + Duration::from_secs(2);
        assert_eq!(media_control_deadline(now, false, answer), answer);
        assert_eq!(media_control_deadline(now, true, answer), now + MEDIA_CONTROL_BUDGET);
        assert_eq!(media_control_deadline(now, false, now + Duration::from_secs(60)), now + MEDIA_CONTROL_BUDGET);
    }

    #[tokio::test]
    async fn media_control_timeout_drops_waiter_and_rejects_expired_ready_result() {
        let expired = tokio::time::Instant::now();
        assert_eq!(bounded_media_control(expired, std::future::ready(Ok(()))).await.unwrap_err().kind(), io::ErrorKind::TimedOut);
        struct MarkDropped(Arc<AtomicBool>);
        impl Drop for MarkDropped {
            fn drop(&mut self) { self.0.store(true, Ordering::Release); }
        }
        let dropped = Arc::new(AtomicBool::new(false));
        let guard = MarkDropped(dropped.clone());
        let deadline = tokio::time::Instant::now() + Duration::from_millis(50);
        let result = bounded_media_control(deadline, async move {
            let _guard = guard;
            std::future::pending::<io::Result<()>>().await
        }).await;
        assert_eq!(result.unwrap_err().kind(), io::ErrorKind::TimedOut);
        assert!(dropped.load(Ordering::Acquire));
    }

    #[tokio::test]
    async fn initial_media_setup_timeout_drops_in_flight_operation() {
        struct MarkDropped(Arc<AtomicBool>);
        impl Drop for MarkDropped {
            fn drop(&mut self) { self.0.store(true, Ordering::Release); }
        }
        let dropped = Arc::new(AtomicBool::new(false));
        let observed = dropped.clone();
        let deadline = tokio::time::Instant::now() + Duration::from_millis(50);
        let result = within_answer_budget(deadline, async move {
            let _guard = MarkDropped(observed);
            std::future::pending::<io::Result<()>>().await
        }).await;
        assert!(result.is_err());
        assert!(dropped.load(Ordering::Acquire));
    }

    #[test]
    fn outgoing_mode_comes_from_admitted_membership_or_explicit_hint() {
        let mut req = Message::new("outgoing", "call");
        req.intent = "auto".into();
        assert!(!outgoing_video(&req, Some(CallIntent::Audio)));
        assert!(outgoing_video(&req, Some(CallIntent::Video)));
        assert!(outgoing_video(&req, None));
        req.intent = "audio".into();
        assert!(!outgoing_video(&req, Some(CallIntent::Video)));
        req.intent = "video".into();
        assert!(outgoing_video(&req, Some(CallIntent::Audio)));
        req.intent.clear();
        assert!(!outgoing_video(&req, None));
        req.video = true;
        assert!(outgoing_video(&req, None));
    }

    #[test]
    fn invitation_pin_accepts_only_refresh_of_the_authenticated_msc_join() {
        let mut req = Message::new("outgoing", "call");
        req.membership_format = "msc".into();
        req.expected_device = "DEVICE".into();
        req.member_event = "$original".into();
        req.member_join_id = "join-1".into();
        assert!(matches_invited_join(&req, Some("$original"), "join-1", None));
        assert!(matches_invited_join(&req, Some("$refreshed"), "join-1", None));
        assert!(!matches_invited_join(&req, Some("$original"), "join-2", None));
        assert!(!matches_invited_join(&req, Some("$refreshed"), "join-1", Some(10)));
        req.member_join_id.clear();
        assert!(!matches_invited_join(&req, Some("$refreshed"), "join-1", None));
        assert!(matches_invited_join(&req, Some("$original"), "join-1", None));
        req.membership_format = "room_state".into();
        req.member_join_id = "join-1".into();
        assert!(!matches_invited_join(&req, Some("$refreshed"), "join-1", Some(10)));
        req.member_join_ts = 10;
        assert!(matches_invited_join(&req, Some("$original"), "join-1", Some(10)));
        assert!(matches_invited_join(&req, Some("$refreshed"), "join-1", Some(10)));
        assert!(!matches_invited_join(&req, Some("$refreshed"), "join-1", Some(11)));
        assert!(!matches_invited_join(&req, Some("$original"), "join-2", Some(10)));
        assert!(!matches_invited_join(&req, Some("$refreshed"), "join-1", None));
    }

    #[test]
    fn mixed_dialect_roster_accepts_only_each_exact_pinned_membership_once() {
        let own = ("@bot:example.org", "local", Some("BOT"));
        let human = ("@alice:example.org", "remote", Some("ALICE"));
        let pins = [Some(own), Some(human)];
        assert!(only_pinned_members([human, own].into_iter(), pins));
        assert!(!only_pinned_members([human, human].into_iter(), pins));
        assert!(!only_pinned_members([human, ("@other:example.org", own.1, own.2)].into_iter(), pins));
        assert!(!only_pinned_members([human, (own.0, own.1, Some("OTHER"))].into_iter(), pins));
        assert!(!only_pinned_members([human, (own.0, own.1, None)].into_iter(), pins));
        assert!(!only_pinned_members([(human.0, human.1, Some("TABLET"))].into_iter(), pins));
        assert!(!only_pinned_members([(own.0, "old-local", own.2)].into_iter(), pins));
        assert!(!only_pinned_members([own].into_iter(), [None, Some(human)]));
    }

    #[test]
    fn only_one_call_can_own_a_room_and_slot() {
        let accounts=Accounts::new(PathBuf::from("/unused"));
        let first=accounts.reserve("room","slot").unwrap();
        assert!(accounts.reserve("room","slot").is_err());
        assert!(accounts.reserve("another-room","slot").is_ok());
        drop(first);
        assert!(accounts.reserve("room","slot").is_ok());
    }

    fn participant(
        member_id: &str,
        user_id: &str,
        device_id: Option<&str>,
        is_local: bool,
    ) -> Participant {
        Participant {
            member_id: member_id.to_owned(),
            user_id: user_id.to_owned(),
            device_id: device_id.map(str::to_owned),
            is_local,
            connection: ConnectionState::Connected,
            unable_to_decrypt: None,
            streams: Vec::new(),
            hand_raised_at_ms: None,
            hand_raised_position: None,
            joined_at_ms: None,
        }
    }

    #[test]
    fn pickup_requires_current_healthy_roster_but_allows_muted_peer() {
        let own = participant("own", "@ghost:example.org", Some("G"), true);
        let peer = participant("peer", "@human:example.org", Some("H"), false);
        let status = CallStatus::Connected { since_ms: 1, degraded: false };
        let ready = |status: &CallStatus, roster: &[Participant]| peer_answer_ready(status, roster, "own", "@ghost:example.org", "G", "peer", "H", "@human:example.org");
        assert!(ready(&status, &[own.clone(), peer.clone()]));
        assert!(!ready(&CallStatus::Connected { since_ms: 1, degraded: true }, &[own.clone(), peer.clone()]));
        assert!(!ready(&status, &[peer.clone()]));
        for (user, device) in [("@other:example.org", Some("G")), ("@ghost:example.org", Some("OTHER")), ("@ghost:example.org", None)] {
            let impostor = participant("own", user, device, true);
            assert!(!ready(&status, &[impostor, peer.clone()]));
        }
        let same_id = participant("own", "@human:example.org", Some("H"), false);
        assert!(!peer_answer_ready(&status, &[own.clone(), same_id], "own", "@ghost:example.org", "G", "own", "H", "@human:example.org"));
        for connection in [ConnectionState::Connecting, ConnectionState::Reconnecting, ConnectionState::Unreachable] {
            let mut bad = peer.clone();
            bad.connection = connection;
            assert!(!ready(&status, &[own.clone(), bad]));
        }
        let mut bad = peer.clone();
        bad.unable_to_decrypt = Some(matrix_rtc_call_sdk::UnableToDecrypt::BadKey);
        assert!(!ready(&status, &[own.clone(), bad]));
        let mut changed = peer;
        changed.device_id = Some("OTHER".into());
        assert!(!ready(&status, &[own, changed]));
    }

    #[test]
    fn pickup_requires_real_rtp_and_not_concealment_or_stale_counters() {
        let first = ReceiveStats { packets_received: 10, bytes_received: 100,
            total_samples_received: 1000, concealed_samples: 900, frames_decoded: 1, ..Default::default() };
        let mut last = first.clone();
        last.total_samples_received += 480;
        assert!(!decoded_since(MediaStreamKind::Microphone, &first, &last));
        last.packets_received += 1;
        last.bytes_received += 50;
        last.concealed_samples += 480;
        assert!(!decoded_since(MediaStreamKind::Microphone, &first, &last));
        last.concealed_samples -= 1;
        assert!(decoded_since(MediaStreamKind::Microphone, &first, &last));
        assert!(!decoded_since(MediaStreamKind::Camera, &first, &last));
        last.frames_decoded += 1;
        assert!(decoded_since(MediaStreamKind::Camera, &first, &last));
        last.concealed_samples = 0;
        assert!(!decoded_since(MediaStreamKind::Microphone, &first, &last));
        assert!(!decoded_since(MediaStreamKind::Microphone, &last, &first));
    }

    #[test]
    fn pickup_checks_active_streams_and_current_local_publications() {
        use matrix_rtc_call_sdk::StreamState;
        let mut peer = participant("peer", "@human:example.org", Some("H"), false);
        assert_eq!(active_pickup_streams(&peer), Some(vec![]));
        peer.streams.push(StreamState { kind: MediaStreamKind::Microphone, muted: true });
        assert_eq!(active_pickup_streams(&peer), Some(vec![]));
        peer.streams.push(StreamState { kind: MediaStreamKind::Camera, muted: false });
        assert_eq!(active_pickup_streams(&peer), Some(vec![MediaStreamKind::Camera]));
        peer.streams.push(StreamState { kind: MediaStreamKind::Camera, muted: false });
        assert_eq!(active_pickup_streams(&peer), None);
        let mut own = participant("own", "@ghost:example.org", Some("G"), true);
        assert!(!pickup_publications_ready(&[own.clone()], "own", false, false));
        own.streams.push(StreamState { kind: MediaStreamKind::Microphone, muted: false });
        assert!(pickup_publications_ready(&[own.clone()], "own", false, false));
        assert!(!pickup_publications_ready(&[own.clone()], "own", true, false));
        assert!(!pickup_publications_ready(&[own.clone()], "own", false, true));
        own.streams.push(StreamState { kind: MediaStreamKind::Camera, muted: false });
        assert!(pickup_publications_ready(&[own.clone()], "own", false, true));
        assert!(!pickup_publications_ready(&[own], "own", false, false));
    }

    #[test]
    fn only_explicit_decline_rejects_unanswered_whatsapp_call() {
        assert_eq!(ended_event(false, &CallEnd::Declined), "reject");
        for reason in [CallEnd::Unanswered, CallEnd::RingNotSent, CallEnd::Left, CallEnd::OthersLeft, CallEnd::Empty, CallEnd::SlotClosed] {
            assert_eq!(ended_event(false, &reason), "error");
            assert_eq!(ended_event(true, &reason), "hangup");
        }
        assert_eq!(ended_event(true, &CallEnd::Declined), "hangup");
    }

    #[test]
    fn expected_participant_is_authorized_but_wrong_user_is_rejected() {
        let own = participant("ghost-member", "@ghost:example.org", Some("GHOSTDEV"), true);
        let expected = participant("alice-member", "@alice:example.org", Some("ALICEDEV"), false);

        let authorized = authorized_peer(
            &[own.clone(), expected],
            "ghost-member",
            "@alice:example.org",
        )
        .unwrap()
        .expect("expected participant must be authorized");

        assert_eq!(authorized.0, "alice-member");

        let wrong = participant("mallory-member", "@mallory:example.org", Some("MALLORYDEV"), false);
        let error = authorized_peer(
            &[own, wrong],
            "ghost-member",
            "@alice:example.org",
        )
        .unwrap_err();

        assert!(
            error.to_string().contains("unrecognized RTC participant"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn missing_or_multiple_remote_device_memberships_fail_closed() {
        let own = participant("ghost-member", "@ghost:example.org", Some("GHOSTDEV"), true);
        let no_device = participant("alice-member", "@alice:example.org", None, false);

        let error = authorized_peer(
            &[own.clone(), no_device],
            "ghost-member",
            "@alice:example.org",
        )
        .unwrap_err();

        assert!(
            error.to_string().contains("device identity is unavailable"),
            "unexpected error: {error}"
        );

        let alice_a = participant("alice-a", "@alice:example.org", Some("ALICE-A"), false);
        let alice_b = participant("alice-b", "@alice:example.org", Some("ALICE-B"), false);

        let error = authorized_peer(
            &[own, alice_a, alice_b],
            "ghost-member",
            "@alice:example.org",
        )
        .unwrap_err();

        assert!(
            error.to_string().contains("extra membership"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn roster_identity_is_bound_to_the_authenticated_device() {
        let own = participant("ghost-member", "@ghost:example.org", Some("GHOSTDEV"), true);
        let expected = participant("alice-member", "@alice:example.org", Some("ALICEDEV"), false);

        let authorized = authorized_peer(
            &[own, expected],
            "ghost-member",
            "@alice:example.org",
        )
        .unwrap()
        .expect("expected legacy participant must be authorized");

        assert_eq!(authorized.1, "ALICEDEV");
    }

    #[tokio::test]
    async fn cached_identity_snapshot_distinguishes_replacement_from_normal_store_writes() {
        use std::os::unix::fs::PermissionsExt;
        let folder = std::env::temp_dir().join(format!("matrixrtc-identity-test-{}-{}",
            std::process::id(), std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos()));
        std::fs::create_dir_all(folder.join("crypto")).unwrap();
        struct Cleanup(PathBuf);
        impl Drop for Cleanup { fn drop(&mut self) { let _ = std::fs::remove_dir_all(&self.0); } }
        let _cleanup = Cleanup(folder.clone());
        let session = folder.join("session.json");
        let db = folder.join("crypto/matrix-sdk-crypto.sqlite3");
        std::fs::write(&session, b"fixture-session").unwrap();
        std::fs::set_permissions(&session, std::fs::Permissions::from_mode(0o600)).unwrap();
        std::fs::write(&db, b"initial").unwrap();
        let original = identity_snapshot(&folder).await.unwrap();
        std::fs::write(&db, b"ordinary in-place change").unwrap();
        assert!(identity_snapshot(&folder).await.unwrap() == original);
        let fence = IdentityFence::new(folder.clone(), original.clone());
        fence.check().await.unwrap();
        std::fs::write(&session, b"replacement-session").unwrap();
        assert!(identity_snapshot(&folder).await.unwrap() != original);
        assert!(fence.check().await.is_err());
        std::fs::write(&session, b"fixture-session").unwrap();
        assert!(identity_snapshot(&folder).await.unwrap() == original);
        assert!(fence.check().await.is_err());
        let replacement_fence = IdentityFence::new(folder.clone(), original.clone());
        let replacement = db.with_extension("replacement");
        std::fs::write(&replacement, b"new store").unwrap();
        std::fs::rename(&replacement, &db).unwrap();
        assert!(identity_snapshot(&folder).await.unwrap() != original);
        assert!(replacement_fence.check().await.is_err());
        let unavailable_fence = IdentityFence::new(folder.clone(), identity_snapshot(&folder).await.unwrap());
        std::fs::rename(&db, &replacement).unwrap();
        assert!(unavailable_fence.check().await.is_err());
        std::fs::rename(&replacement, &db).unwrap();
        assert!(unavailable_fence.check().await.is_err());
    }

    #[test]
    fn peer_media_identity_follows_token_flow_not_membership_format() {
        use matrix_rtc_core::LiveKitTransport;
        use matrix_rtc_transport::livekit::identity::{legacy_identity, msc4195_identity};
        let user = "@alice:example.org";
        let device = "ALICEDEV";
        let member = "custom-membership-id";
        let legacy = RtcTransport::LiveKit(LiveKitTransport::service("https://example.org/jwt"));
        let modern = RtcTransport::LiveKit(LiveKitTransport::sfu("wss://example.org/sfu"));
        let both = RtcTransport::LiveKit(LiveKitTransport::new(
            Some("wss://example.org/sfu".into()), Some("https://example.org/jwt".into()), None,
        ).unwrap());
        assert_eq!(peer_media_identity(user, device, member, &[legacy.clone()]), Some(legacy_identity(user, device)));
        assert_eq!(peer_media_identity(user, device, member, &[modern.clone()]), Some(msc4195_identity(user, member)));
        assert_eq!(peer_media_identity(user, device, member, &[both]), Some(legacy_identity(user, device)));
        assert_eq!(peer_media_identity(user, device, member, &[modern, legacy]), Some(msc4195_identity(user, member)));
        assert_eq!(peer_media_identity(user, device, member, &[]), None);
    }
}

#[cfg(test)]
#[test]
fn invitation_and_live_peer_key_replacement_is_rejected() {
    let original = DeviceKeys { signing: "signing".into(), identity: "identity".into(), master: Some("master".into()) };
    let mut req = Message::new("outgoing", "call");
    req.expected_signing_key = original.signing.clone();
    req.expected_identity_key = original.identity.clone();
    assert!(original.require_invitation(&req).is_ok());
    assert!(original.require_same(&original).is_ok());
    let mut changed = original.clone();
    changed.signing = "new".into();
    assert!(changed.require_invitation(&req).is_err());
    assert!(original.require_same(&changed).is_err());
    changed = original.clone();
    changed.identity = "new".into();
    assert!(changed.require_invitation(&req).is_err());
    changed = original.clone();
    changed.master = Some("new".into());
    assert!(original.require_same(&changed).is_err());
}
