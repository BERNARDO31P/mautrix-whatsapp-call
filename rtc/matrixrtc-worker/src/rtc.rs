//! Authenticated, E2EE-aware RTC call path. No new Matrix users or devices
//! are created during a call; provisioning is an explicit offline step.
use crate::{audio, protocol::{self, Message}, send, video, Writer};
use futures_util::StreamExt;
use matrix_rtc_call::{CallKind, CallStatus, GroupNotify, RingConfig, notification::Mentions};
use matrix_rtc_call_sdk::{CallEvent, LiveKitCall, LiveKitCallOptions, Participant, SdkMatrixBackend};
use matrix_rtc_call::{RtcClient, RtcRoom};
use matrix_rtc_core::{RoomOptions, SlotState, RawSlotEventContent, JoinedMembership};
use matrix_sdk::ruma::{EventId, api::client::state::get_state_events};
use matrix_rtc_core::{SlotEncryption, compat::MembershipFormat, wire_event_type};
use matrix_rtc_transport::{AudioFrame, MediaStreamKind, PublishOptions, VideoSourceConfig, livekit::identity::{msc4195_identity, legacy_identity}};
use matrix_sdk::{Client, encryption::EncryptionSettings, authentication::matrix::MatrixSession, ruma::{RoomId, UserId}, RoomMemberships};
use serde_json::json;
use matrix_sdk_ui::sync_service::SyncService;
use sha2::{Digest, Sha256};
use std::{collections::HashMap, io, path::{Path, PathBuf}, sync::{Arc, atomic::{AtomicBool, AtomicU64, Ordering}}, time::Duration};
use tokio::{io::ReadHalf, net::UnixStream, sync::Mutex};

struct AbortOnDrop(HashMap<String,tokio::task::JoinHandle<()>>);
impl Drop for AbortOnDrop {
    fn drop(&mut self) { for task in self.0.values() { task.abort(); } }
}
struct MediaGate(Arc<AtomicBool>);
impl Drop for MediaGate {
    fn drop(&mut self) { self.0.store(false, Ordering::Release); }
}
struct Ghost { client: Client, _sync: SyncService }
pub struct Accounts {
    root: PathBuf,
    clients: Mutex<HashMap<String, Arc<tokio::sync::OnceCell<Arc<Ghost>>>>>,
    slots: std::sync::Mutex<HashMap<(String,String), std::sync::Weak<Mutex<()>>>>,
}

fn error(reason: impl Into<String>) -> io::Error { io::Error::other(reason.into()) }

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
        let cell=self.clients.lock().await.entry(ghost_mxid.to_owned()).or_default().clone();
        let ghost=cell.get_or_try_init(|| async {
            let folder = identity_folder(&self.root, ghost_mxid);
            let crypto = folder.join("crypto").join("matrix-sdk-crypto.sqlite3");
            let meta = tokio::fs::symlink_metadata(&crypto).await?;
            if !meta.is_file() || meta.len() == 0 {
                return Err(error("restore the prepared ghost crypto store before calling"));
            }
            let client=load_identity_client(&self.root,ghost_mxid).await?;
            require_prepared_identity(&client).await?;
            let sync=SyncService::builder(client.clone()).build().await.map_err(io::Error::other)?;
            sync.start().await;
            Ok::<_,io::Error>(Arc::new(Ghost { client, _sync:sync }))
        }).await?;
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
            let Ok(_owned)=self.reserve(&req.room_id,&req.slot_id) else {continue};
            let work=async {
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

async fn synced_room(incoming: &Message, accounts: &Accounts) -> io::Result<(Arc<Ghost>, matrix_sdk::Room)> {
    let ghost = accounts.get(&incoming.ghost_mxid).await?;
    require_prepared_identity(&ghost.client).await?;
    let room=wait_room(incoming,&ghost).await?;
    validate_room(incoming,&room).await?;
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
        // Mautrix keeps its bridge bot joined in encrypted portal rooms so it can
        // perform end-to-bridge encryption. It is a service member, not a human.
        let human_members: Vec<_> = members.iter().filter(|member| !member.is_service_member()).collect();
        if human_members.len() != 2
            || !human_members.iter().any(|m| m.user_id().as_str() == expected.as_str())
            || !human_members.iter().any(|m| m.user_id().as_str() == own.as_str()) {
            return Err(error("RTC room is not an exclusive two-human-user encrypted DM"));
        }
    }
    Ok(())
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

    // The two readers can see the same underlying membership. Accept the
    // same pinned device/member, and reject an occupant visible in only the
    // other signalling generation. Neither reader joins the slot.
    fn check_other_members(&self, req: &Message, own: Option<(&str, &str)>,
                           remote: Option<(&str, &str)>) -> io::Result<()> {
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

    async fn caller(&self,req:&Message,room:&matrix_sdk::Room,first:bool)->io::Result<(String,Option<u64>)>{
        let member={
            let members=self.primary.members.borrow();
            let [m]=members.as_slice() else {return Err(error("expected exactly one calling membership before dial"))};
            m.clone()
        };
        if req.expected_device.is_empty() || member.application.application_type()!=Some("m.call") || member.sender!=req.expected_user_mxid || member.slot_id!=req.slot_id ||
           member.origin.sender_device_id()!=Some(&req.expected_device) ||
           (member.origin.was_encrypted()!=Some(true) && (req.membership_format!="room_state" || req.member_event.is_empty())) ||
           (first && !req.member_event.is_empty() && member.membership_event_id.as_deref()!=Some(&req.member_event)) {
            return Err(error("caller device or membership does not match the encrypted invitation"))
        }
        if !self.open(req).await && req.membership_format!="room_state" {return Err(error("RTC slot closed or lacks encryption"))}
        self.check_other_members(req, None, Some((&member.member_id, &req.expected_device)))?;
        trusted_device(room,&member.sender,&req.expected_device).await?;
        Ok((member.member_id,member.membership_ts))
    }
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

async fn trusted_device(room:&matrix_sdk::Room,user:&str,device:&str)->io::Result<()> {
    let user=UserId::parse(user).map_err(io::Error::other)?;
    let device=room.client().encryption().get_device(&user,device.into()).await.map_err(io::Error::other)?;
    if !device.is_some_and(|d| !d.is_blacklisted() && (d.is_verified() || d.is_cross_signed_by_owner())) {
        return Err(error("RTC device is not trusted"))
    }
    Ok(())
}

// Redact the exact state event created by this worker. This cannot close a
// newer slot owned by another participant, even if the room changes mid-cleanup.
async fn release_slot(room:&matrix_sdk::Room,req:&Message,path:&Path)->io::Result<()> {
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
    if membership_format(incoming)? != MembershipFormat::Msc {
        return Err(error("incoming WhatsApp calls require current MSC MatrixRTC signalling"));
    }
    if incoming.video {
        video::verify_ffmpeg().await?;
    }
    let (_, room) = synced_room(incoming, accounts).await?;
    let path=accounts.lease_path(incoming);
    if tokio::fs::try_exists(&path).await? {
        let old:Message=serde_json::from_slice(&tokio::fs::read(&path).await?).map_err(io::Error::other)?;
        if old.ghost_mxid!=incoming.ghost_mxid {return Err(error("previous slot owner requires recovery"))}
        release_slot(&room,&old,&path).await?;
    }
    let admission=Admission::inspect(&room,incoming).await?;
    if !admission.primary.members.borrow().is_empty() || !admission.alternate.members.borrow().is_empty() {return Err(error("incoming RTC slot already occupied"))}
    let owned=!admission.open(incoming).await;
    if owned && matches!(admission.primary.room.slot_state(&incoming.slot_id).await,Some(SlotState::Open(_))) {return Err(error("existing RTC slot has an incompatible application or encryption mechanism"));}
    if replay.contains(&incoming.login_id, &incoming.call_id)? {
        return Err(error("duplicate or replayed call"));
    }
    let result=async {
    if owned { create_slot(&room, incoming, &path).await?; }
    // Creating the slot does not ring the Matrix user. Commit replay protection
    // only once setup succeeded, immediately before joining and notifying.
    if !replay.register(&incoming.login_id, &incoming.call_id)? {
        return Err(error("duplicate or replayed call"));
    }
    let mut call = LiveKitCall::join_until(&room, LiveKitCallOptions {
        slot_id: incoming.slot_id.clone(),
        format: membership_format(incoming)?,
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
    },Some(tokio::time::Instant::now()+Duration::from_secs(35))).await.map_err(io::Error::other)?;
    let result = connected_session(incoming, reader, writer, &mut call, &room, &admission, None, None, None).await;
    let leave_result = call.leave().await;
    if result.is_ok() { leave_result.map_err(io::Error::other)?; }
    result
    }.await;
    if owned && tokio::fs::try_exists(&path).await? { release_slot(&room,incoming,&path).await?; }
    result
}

async fn run_outgoing(incoming: &Message, reader: &mut protocol::FrameReader<ReadHalf<UnixStream>>, writer: &Writer, accounts: &Accounts, replay: &crate::ReplayGuard) -> io::Result<()> {
    if incoming.video {
        video::verify_ffmpeg().await?;
    }
    let (_ghost, room) = synced_room(incoming, accounts).await?;
    let mut admission=Admission::inspect(&room,incoming).await?;
    if admission.primary.members.borrow().is_empty(){
        tokio::time::timeout(Duration::from_secs(6),admission.primary.members.wait_for(|m|!m.is_empty())).await.map_err(|_|error("calling membership absent"))?.map_err(io::Error::other)?;
    }
    let pin=admission.caller(incoming,&room,true).await?;
    let mut wa_audio=None;
    let mut wa_video=None;
    if !replay.register(&incoming.login_id,&incoming.call_id)? {return Err(error("duplicate or replayed call"));}
    let mut dial = Message::new("dial", &incoming.call_id);
    dial.video = incoming.video;
    send(writer, &dial).await?;

    // A PLI/FIR can legitimately arrive before the remote WhatsApp peer accepts
    // (for example when the video transport is enabled). Keep one absolute
    // acceptance deadline and tolerate those early keyframe requests instead of
    // treating them as a protocol violation.
    let mut recheck=tokio::time::interval(Duration::from_secs(1));
    let acceptance_deadline = tokio::time::Instant::now() + Duration::from_secs(125);
    loop {
        let frame=tokio::select! {
            read=tokio::time::timeout_at(acceptance_deadline,reader.read())=>read,
            _=recheck.tick()=>{
                validate_room(incoming,&room).await?;
                if admission.caller(incoming,&room,false).await?!=pin {return Err(error("RTC caller changed while ringing"))}
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
        if accepted.call_id != incoming.call_id {
            return Err(error("call ID mismatch"));
        }
        match accepted.kind.as_str() {
            "whatsapp_answered" => break,
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

    validate_room(incoming,&room).await?;
    if admission.caller(incoming,&room,false).await? != pin {
        return Err(error("RTC caller changed before WhatsApp acceptance"));
    }
    let mut call = LiveKitCall::join_until(&room, LiveKitCallOptions {
        slot_id: incoming.slot_id.clone(),
        format: membership_format(incoming)?,
        kind: if incoming.group { CallKind::Group { notify: None } } else { CallKind::Direct { ring: None } },
        ..Default::default()
    },Some(tokio::time::Instant::now()+Duration::from_secs(35))).await.map_err(io::Error::other)?;
    let result = connected_session(incoming, reader, writer, &mut call, &room, &admission, Some(pin), wa_audio, wa_video).await;
    let leave_result = call.leave().await;
    if result.is_ok() { leave_result.map_err(io::Error::other)?; }
    result
}

/// Derive the only permitted remote membership from authenticated Matrix
/// membership signalling. A member ID is NOT a user ID. It must be mapped
/// through the SDK roster and the verified E2EE key for its SFU identity.
fn authorized_peer(roster: &[Participant], own_member_id: &str, expected: &str,
                   format: MembershipFormat) -> io::Result<Option<(String, String)>> {
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
        let identity = match format {
            MembershipFormat::Msc => msc4195_identity(&participant.user_id, &participant.member_id),
            MembershipFormat::RoomState => legacy_identity(&participant.user_id, device),
        };
        remote = Some((participant.member_id.clone(), identity));
    }
    Ok(remote)
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

async fn connected_session(incoming: &Message, reader: &mut protocol::FrameReader<ReadHalf<UnixStream>>, writer: &Writer, call: &mut LiveKitCall, room:&matrix_sdk::Room, admission:&Admission, expected_join:Option<(String,Option<u64>)>, wa_audio:Option<bool>, wa_video:Option<u8>) -> io::Result<()> {
    let mut status = call.subscribe_status();
    let mut events = call.subscribe_call_events();
    let mut roster_updates = call.subscribe_participants();
    let mut memberships=call.subscribe_memberships().await;
    let own_member_id = call.membership_id().to_owned();
    let format = membership_format(incoming)?;
    let mut peer;
    let mut pinned = None;
    let mut topology=tokio::time::interval(Duration::from_secs(1));
    topology.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut answered = false;
    let mut states=HashMap::<String,Option<bool>>::new();
    let mut pending_tracks = std::collections::VecDeque::<CallEvent>::new();
    let mut last_sample = 0f32;
    let mut remote_tasks = AbortOnDrop(HashMap::new());
    let media_active = Arc::new(AtomicBool::new(false));
    let _media_gate = MediaGate(media_active.clone());
    let video_keyframe_generation = Arc::new(AtomicU64::new(0));

    let audio_track = call.publish(PublishOptions::microphone()).await.map_err(io::Error::other)?;
    let mut video_decoder = None;
    let mut rejected_generation=0;
    if wa_audio==Some(true) { call.engine().set_local_muted(MediaStreamKind::Microphone,true).await.map_err(io::Error::other)?; }
    if wa_video==Some(1) || (incoming.video && wa_video!=Some(6)) {
        ensure_whatsapp_video_track(call, &mut video_decoder).await?;
    }

    if wa_video==Some(0) && video_decoder.is_some() {call.engine().set_local_muted(MediaStreamKind::Camera,true).await.map_err(io::Error::other)?;}
    let mut expiry = Box::pin(tokio::time::sleep(Duration::from_secs(100)));
    loop {
        let connected = matches!(*status.borrow(), CallStatus::Connected { .. });
        if let Some(decoder) = video_decoder.as_mut() {
            decoder.ensure_running()?;
        }
        peer = authorized_peer(&call.participants(), &own_member_id, &incoming.expected_user_mxid, format)?;
        if let Some((id,identity))=&peer {
            let row=call.participants().into_iter().find(|p| &p.member_id==id).ok_or_else(||error("RTC membership changed"))?;
            let device=row.device_id.as_deref().ok_or_else(||error("missing RTC device"))?;
            let members=memberships.borrow().clone();
            let membership=members.iter().find(|m|&m.member_id==id && m.sender==row.user_id && m.origin.sender_device_id()==Some(device))
                .ok_or_else(||error("RTC device disappeared from the current membership"))?;
            let generation=membership.membership_ts;
            let current=(id.clone(),device.to_owned(),generation);
            if (!incoming.expected_device.is_empty() && incoming.expected_device!=device) || expected_join.as_ref().is_some_and(|(wanted,ts)| wanted!=id || *ts!=generation)
                || pinned.as_ref().is_some_and(|value| value!=&current) {return Err(error("RTC device or membership replaced"))}
            if pinned.is_none() {trusted_device(room,&row.user_id,device).await?;pinned=Some(current);}
            if answered && !call.imported_key_for(identity) {return Err(error("RTC peer media key no longer available"))}
        }
        if answered && peer.is_none() { return Err(error("authorized Matrix participant left the RTC slot")); }
        if call.member_count().await > 2 { return Err(error("RTC slot contains extra memberships")); }
        // An already imported and verified key is queried as state, not only
        // through a transient broadcast which may predate subscription.
        let encrypted_peer = peer.as_ref().is_some_and(|(_, identity)| call.imported_key_for(identity));
        if !answered && encrypted_peer && connected && call.member_count().await == 2 {
            validate_room(incoming, room).await?;
            let client = room.client();
            admission.check_other_members(incoming, client.device_id().map(|d| (own_member_id.as_str(), d.as_str())),
                pinned.as_ref().map(|(id, device, _)| (id.as_str(), device.as_str())))?;
            send(writer, &Message::new("answer", &incoming.call_id)).await?;
            answered = true;
            // The local socket write above is serialized and completes before
            // media is enabled, so Go can never observe media before answer.
            media_active.store(true, Ordering::Release);
        }

        if answered {
            if let Some((id,identity))=&peer {
                if !call.imported_key_for(identity) {return Err(error("RTC peer key unavailable"))}
                let parts=call.participants();
                let participant=parts.iter().find(|p| &p.member_id==id).ok_or_else(||error("RTC peer left"))?;
                for (key,kind) in [("audio",MediaStreamKind::Microphone),("video",MediaStreamKind::Camera)] {
                    let state=participant.streams.iter().find(|s|s.kind==kind).map(|s|s.muted);
                    if state != Some(false) {
                        if let Some(task) = remote_tasks.0.remove(key) { task.abort(); let _ = task.await; }
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
                        if let Some(ctrl)=control {send(writer,&Message::new(ctrl,&incoming.call_id)).await?;}
                    }
                    if state == Some(false) && !remote_tasks.0.contains_key(key) && pending_tracks.is_empty() && call.remote_track(id,kind).is_some() {
                        pending_tracks.push_back(CallEvent::StreamStarted{member_id:id.clone(),kind});
                    }
                }
            }
        }
        tokio::select! {
            _=topology.tick()=> {
                validate_room(incoming,room).await?;
                let client = room.client();
                require_prepared_identity(&client).await?;
                // During incoming setup the roster may still be publishing the
                // same remote member in its other format. Once pinned, verify
                // both views on every topology tick.
                if answered || pinned.is_some() {
                    admission.check_other_members(incoming, client.device_id().map(|d| (own_member_id.as_str(), d.as_str())),
                        pinned.as_ref().map(|(id, device, _)| (id.as_str(), device.as_str())))?;
                }
                if let Some((_,device,_))=&pinned {trusted_device(room,&incoming.expected_user_mxid,device).await?;}
            }
            change=memberships.changed()=>{change.map_err(|_|error("RTC membership watch closed"))?;}
            change = roster_updates.changed() => {
                change.map_err(|_| error("MatrixRTC roster watch closed"))?;
                let _ = roster_updates.borrow_and_update();
            }
            event = status.changed() => {
                event.map_err(|_| error("call status channel closed"))?;
                let ended = matches!(&*status.borrow_and_update(), CallStatus::Ended { .. });
                if ended {
                    let kind = if answered {"hangup"} else {"reject"};
                    send(writer, &Message::new(kind, &incoming.call_id)).await?;
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
                    let Some(mut frames) = call
                        .remote_track(&member_id, MediaStreamKind::Microphone)
                        .and_then(|track| track.audio_frames())
                    else {
                        continue;
                    };
                    let writer = writer.clone();
                    let call_id = incoming.call_id.clone();
                    let active = media_active.clone();
                    remote_tasks.0.insert("audio".to_owned(),tokio::spawn(async move {
                        let mut buffered = Vec::<i16>::with_capacity(2880);
                        while let Some(frame) = frames.next().await {
                            if !active.load(Ordering::Acquire) {
                                buffered.clear();
                                continue;
                            }
                            if frame.sample_rate != 48_000 || frame.num_channels != 1 {
                                let mut message = Message::new("error", &call_id);
                                message.reason = format!(
                                    "unexpected MatrixRTC audio format: {} Hz / {} channels",
                                    frame.sample_rate, frame.num_channels
                                );
                                let _ = send(&writer, &message).await;
                                return;
                            }
                            buffered.extend_from_slice(&frame.data);
                            while buffered.len() >= 2880 {
                                let chunk: Vec<i16> = buffered.drain(..2880).collect();
                                let pcm = audio::downsample_48_to_16(&chunk);
                                let Ok(data) = protocol::encode_pcm(&pcm) else { return; };
                                let mut message = Message::new("matrix_audio", &call_id);
                                message.data = data;
                                if send(&writer, &message).await.is_err() { return; }
                            }
                        }
                    }));
                }
                (true,Ok(CallEvent::StreamStarted { member_id, kind: MediaStreamKind::Camera }))
                    if states.get("video") == Some(&Some(false)) && !remote_tasks.0.contains_key("video") && peer.as_ref().is_some_and(|(id, identity)| id == &member_id && call.imported_key_for(identity)) => {
                    video::verify_ffmpeg().await?;
                    let Some(frames) = call
                        .remote_track(&member_id, MediaStreamKind::Camera)
                        .and_then(|track| track.video_frames())
                    else {
                        continue;
                    };
                    let writer = writer.clone();
                    let error_writer = writer.clone();
                    let call_id = incoming.call_id.clone();
                    let error_call_id = call_id.clone();
                    let active = media_active.clone();
                    let keyframe_generation = video_keyframe_generation.clone();
                    remote_tasks.0.insert("video".to_owned(),tokio::spawn(async move {
                        if let Err(err) = video::matrix_video_to_whatsapp(
                            frames, writer, call_id, active, keyframe_generation,
                        ).await {
                            let mut message = Message::new("error", &error_call_id);
                            message.reason = format!("MatrixRTC video encoder failed: {err}");
                            let _ = send(&error_writer, &message).await;
                        }
                    }));
                }
                (_,Ok(CallEvent::StreamStopped{member_id,kind})) if peer.as_ref().is_some_and(|(id,_)| *id==member_id)=>{
                    let key=if kind==MediaStreamKind::Microphone {"audio"} else if kind==MediaStreamKind::Camera {"video"} else {continue};
                    if let Some(task)=remote_tasks.0.remove(key){task.abort();}
                }
                (_,Ok(_))=>{},
                (_,Err(err))=>return Err(error(format!("RTC event stream interrupted: {err}"))),
            },
            result = reader.read() => {
                let msg = result?;
                if msg.call_id != incoming.call_id { return Err(error("call ID mismatch")); }
                match msg.kind.as_str() {
                    "group_check" if incoming.group && answered && encrypted_peer => {
                        validate_room(incoming,room).await?;
                        let (_,device,_)=pinned.as_ref().ok_or_else(||error("caller device missing"))?;
                        trusted_device(room,&incoming.expected_user_mxid,device).await?;
                        let mut command=Message::new("matrix_group_invite",&incoming.call_id);
                        command.targets=msg.targets;
                        send(writer,&command).await?;
                    }
                    "whatsapp_audio" if answered => {
                        let samples = protocol::pcm(&msg.data)?;
                        let upsampled = audio::upsample_16_to_48(&samples, last_sample);
                        last_sample = *samples.last().unwrap_or(&last_sample);
                        for part in upsampled.chunks_exact(audio::RTC_FRAME) {
                            audio_track.capture_audio(AudioFrame {
                                data: part.to_vec(),
                                sample_rate: 48_000,
                                num_channels: 1,
                                samples_per_channel: audio::RTC_FRAME as u32,
                            }).await.map_err(io::Error::other)?;
                        }
                    }
                    "whatsapp_video" if answered => {
                        if msg.generation<=rejected_generation {continue}
                        if video_decoder.as_ref().is_some_and(|d|d.generation!=0 && d.generation!=msg.generation){
                            stop_whatsapp_video_track(call,&mut video_decoder).await?;
                        }
                        ensure_whatsapp_video_track(call,&mut video_decoder).await?;
                        let decoder=video_decoder.as_mut().ok_or_else(||error("decoder unavailable"))?;
                        decoder.generation=msg.generation;
                        if !decoder.push(msg.data,msg.orientation,msg.timestamp)? {
                            rejected_generation=msg.generation;
                            stop_whatsapp_video_track(call,&mut video_decoder).await?;
                            send(writer,&Message::new("matrix_video_resync",&incoming.call_id)).await?;
                        }
                    }
                    "whatsapp_video_state" => {
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
                    }
                    "whatsapp_audio_mute" => {
                        call.engine().set_local_muted(MediaStreamKind::Microphone, true).await.map_err(io::Error::other)?;
                    }
                    "whatsapp_audio_unmute" => {
                        call.engine().set_local_muted(MediaStreamKind::Microphone, false).await.map_err(io::Error::other)?;
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
                send(writer, &Message::new("reject", &incoming.call_id)).await?;
                break;
            }
        }
    }

    media_active.store(false, Ordering::Release);
    drop(video_decoder);
    Ok(())
}

#[cfg(test)]
mod authorization_tests {
    use super::*;
    use matrix_rtc_call_sdk::ConnectionState;

    #[test]
    fn mixed_dialect_roster_accepts_only_each_exact_pinned_membership_once() {
        let own = ("@bot:example.org", "local", Some("BOT"));
        let human = ("@alice:example.org", "remote", Some("ALICE"));
        let pins = [Some(own), Some(human)];
        assert!(only_pinned_members([human, own].into_iter(), pins));
        assert!(!only_pinned_members([human, human].into_iter(), pins));
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
    fn expected_participant_is_authorized_but_wrong_user_is_rejected() {
        let own = participant("ghost-member", "@ghost:example.org", Some("GHOSTDEV"), true);
        let expected = participant("alice-member", "@alice:example.org", Some("ALICEDEV"), false);

        let authorized = authorized_peer(
            &[own.clone(), expected],
            "ghost-member",
            "@alice:example.org",
            MembershipFormat::Msc,
        )
        .unwrap()
        .expect("expected participant must be authorized");

        assert_eq!(authorized.0, "alice-member");

        let wrong = participant("mallory-member", "@mallory:example.org", Some("MALLORYDEV"), false);
        let error = authorized_peer(
            &[own, wrong],
            "ghost-member",
            "@alice:example.org",
            MembershipFormat::Msc,
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
            MembershipFormat::Msc,
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
            MembershipFormat::Msc,
        )
        .unwrap_err();

        assert!(
            error.to_string().contains("extra membership"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn legacy_identity_is_bound_to_the_authenticated_device() {
        let own = participant("ghost-member", "@ghost:example.org", Some("GHOSTDEV"), true);
        let expected = participant("alice-member", "@alice:example.org", Some("ALICEDEV"), false);

        let authorized = authorized_peer(
            &[own, expected],
            "ghost-member",
            "@alice:example.org",
            MembershipFormat::RoomState,
        )
        .unwrap()
        .expect("expected legacy participant must be authorized");

        assert_eq!(
            authorized.1,
            legacy_identity("@alice:example.org", "ALICEDEV")
        );
    }
}
