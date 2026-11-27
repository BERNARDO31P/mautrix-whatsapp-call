// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
//! Actual worker executable + actual Go IPC codec + real MatrixRTC peer.
//! Only the WhatsApp provider boundary is emulated. Staging opt-in required.
use super::*;
use matrix_rtc_call::{CallIntent, CallKind, RtcClient};
use matrix_rtc_call_sdk::{SdkMatrixBackend, TrustedFocus};
use matrix_rtc_core::RoomOptions;
use std::sync::Arc;
use tokio::process::{Child, Command};

#[test]
#[ignore = "requires explicitly provisioned disposable MatrixRTC deployment and Go harness"]
fn e2e_worker_bridge_staging() {
    run_to_completion(Duration::from_secs(360), run_worker);
}

struct Control(PathBuf);
impl Drop for Control {
    fn drop(&mut self) { let _ = std::fs::remove_dir_all(&self.0); }
}
impl Control {
    fn new() -> Result<Self, Box<dyn Error>> {
        use std::os::unix::fs::DirBuilderExt;
        let path = env::temp_dir().join(format!("rtc-worker-control-{}", provision::run_suffix()));
        std::fs::DirBuilder::new().mode(0o700).create(&path)?;
        Ok(Self(path))
    }
    fn mark(&self, name: &str) -> Result<(), Box<dyn Error>> {
        let tmp=self.0.join(format!(".{name}.pending"));
        std::fs::write(&tmp, b"PASS\n")?;
        std::fs::rename(tmp,self.0.join(name))?;
        Ok(())
    }
    async fn wait(&self, name: &str, child: &mut Child) -> Result<(), Box<dyn Error>> {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(90);
        loop {
            match std::fs::read(self.0.join(name)) {
                Ok(bytes) if bytes == b"PASS\n" => return Ok(()),
                Ok(_) => return Err("invalid worker harness rendezvous".into()),
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => (),
                Err(e) => return Err(e.into()),
            }
            if let Some(status) = child.try_wait()? { return Err(format!("Go harness exited before {name}: {status}").into()); }
            if tokio::time::Instant::now() >= deadline { return Err(format!("worker harness did not reach {name}").into()); }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }
}

async fn worker_join(cfg: &Config, room: &matrix_sdk::Room, format: MembershipFormat, server: &str) -> Result<LiveKitCall, Box<dyn Error>> {
    let transport = cfg.transport(format, Sfu::First);
    Ok(LiveKitCall::join(room, LiveKitCallOptions {
        slot_id: cfg.slot_id.clone(), format, livekit_transport: Some(transport),
        trusted_focus: Some(TrustedFocus::local_deployment(server)?),
        intent: Some(CallIntent::Audio), kind: CallKind::Direct { ring: None },
        ..Default::default()
    }).await?)
}

async fn finish_child(child: &mut Child) -> Result<(), Box<dyn Error>> {
    let status = tokio::time::timeout(Duration::from_secs(100), child.wait()).await??;
    if !status.success() { return Err(format!("Go worker harness failed: {status}").into()); }
    Ok(())
}

async fn run_worker(mut cfg: Config) -> Result<(), Box<dyn Error>> {
    if env::var("RTC_WORKER_E2E_DISPOSABLE").as_deref() != Ok("1") { return Err("disposable staging opt-in required".into()); }
    let server = env::var("RTC_WORKER_E2E_SERVER")?;
    let ghost = env::var("RTC_WORKER_E2E_GHOST")?;
    let expected_room = env::var("RTC_WORKER_E2E_ROOM")?;
    let direction = env::var("RTC_WORKER_E2E_DIRECTION")?;
    let end = env::var("RTC_WORKER_E2E_END")?;
    let dialect = env::var("RTC_WORKER_E2E_FORMAT")?;
    let format = match dialect.as_str() { "msc" => MembershipFormat::Msc, "room_state" => MembershipFormat::RoomState, _ => return Err("invalid staging format".into()) };
    if !matches!(direction.as_str(),"incoming"|"outgoing") || !matches!(end.as_str(),"gateway"|"peer"|"cancel") { return Err("invalid staging scenario".into()); }
    let state = PathBuf::from(env::var("RTC_WORKER_E2E_STATE")?);
    if !state.is_absolute() || std::fs::canonicalize(&state)? != state || !state.file_name().and_then(|s| s.to_str()).is_some_and(|s| s.starts_with("rtc-worker-e2e-"))
        || std::fs::read(state.join(".matrixrtc-e2e-disposable"))? != b"disposable\n" { return Err("dedicated disposable worker state required".into()); }
    let focus = TrustedFocus::local_deployment(&server)?;
    drop(focus);
    // Never silently use generic E2E defaults or credentials for this product test.
    cfg.homeserver = format!("https://{server}");
    cfg.livekit_service_url = format!("https://{server}/livekit/jwt");
    cfg.sfu_url = format!("wss://{server}/livekit/sfu");
    cfg.insecure_tls = false;
    cfg.slot_id = env::var("RTC_WORKER_E2E_SLOT")?;
    // Restore an already cross-signed disposable peer device. A password
    // login on every scenario would create a new untrusted device and could
    // fail the worker admission for reasons unrelated to the call harness.
    let peer_dir = state.join("test-peer");
    use std::os::unix::fs::PermissionsExt;
    let session_path = peer_dir.join("session.json");
    let pass_path = PathBuf::from(env::var("RTC_WORKER_E2E_PASSPHRASE_FILE")?);
    for path in [&session_path, &pass_path] {
        let meta = std::fs::symlink_metadata(path)?;
        if !meta.is_file() || meta.permissions().mode() & 0o077 != 0 { return Err("fixture session/passphrase must be private regular files".into()); }
    }
    if !pass_path.is_absolute() || !std::fs::symlink_metadata(peer_dir.join("crypto"))?.is_dir() || !std::fs::symlink_metadata(peer_dir.join("crypto/matrix-sdk-crypto.sqlite3"))?.is_file() {
        return Err("existing prepared peer crypto store required".into());
    }
    let session: matrix_sdk::authentication::matrix::MatrixSession = serde_json::from_slice(&std::fs::read(&session_path)?)?;
    if !session.meta.user_id.as_str().ends_with(&format!(":{server}")) || session.meta.user_id.as_str() == ghost { return Err("peer session does not belong to the staging user".into()); }
    let pass = std::fs::read_to_string(pass_path)?;
    if pass.trim().is_empty() {return Err("empty fixture store passphrase".into());}
    let client = Client::builder().homeserver_url(&cfg.homeserver)
        .sqlite_store(peer_dir.join("crypto"), Some(pass.trim()))
        .with_encryption_settings(EncryptionSettings { auto_enable_cross_signing: false, ..Default::default() }).build().await?;
    client.restore_session(session).await?;
    client.encryption().wait_for_e2ee_initialization_tasks().await;
    let device = client.encryption().get_own_device().await?.ok_or("missing prepared peer device")?;
    if !device.is_cross_signed_by_owner() { return Err("fixture peer must be cross-signed".into()); }
    let sync = SyncService::builder(client.clone()).build().await?;
    sync.start().await;
    let synced = SyncedClient {client, sync};
    let own = synced.client.user_id().ok_or("missing peer user")?.to_string();
    if own == ghost || !own.ends_with(&format!(":{server}")) || !ghost.ends_with(&format!(":{server}")) || !expected_room.ends_with(&format!(":{server}")) { return Err("staging room/identities not on explicit staging server".into()); }
    let room_id = RoomId::parse(&expected_room)?;
    let room = wait_for_room(&synced.client, &room_id).await?;
    if room.state() != matrix_sdk::RoomState::Joined || !room.latest_encryption_state().await?.is_encrypted() { return Err("fixture room must already be joined and encrypted".into()); }
    let members = room.members(RoomMemberships::JOIN).await?;
    if members.len() != 2 || !members.iter().any(|m| m.user_id().as_str() == ghost) || !members.iter().any(|m| m.user_id().as_str() == own) { return Err("fixture room must contain exactly the two disposable participants".into()); }
    let rtc = RtcClient::new(Arc::new(SdkMatrixBackend::new(synced.client.clone())));
    let passive = rtc.room(expected_room.clone(), RoomOptions { format }).await?;
    tokio::time::timeout(Duration::from_secs(20), passive.seeded()).await?;
    if !passive.is_seeded() { return Err("fixture room feed unavailable".into()); }
    let mut roster = passive.observe(&cfg.slot_id).await;
    if !roster.borrow().is_empty() { return Err("fixture slot already occupied".into()); }
    let control = Control::new()?;
    let mut req = serde_json::json!({
        "type": direction, "call_id": format!("worker-e2e-{}", provision::run_suffix()),
        "login_id":"worker-e2e", "room_id":expected_room,"slot_id":cfg.slot_id,
        "membership_format":dialect,"ghost_mxid":ghost,"expected_user_mxid":own,
        "peer":"123@s.whatsapp.net","video":false,
    });
    let mut call = None;
    if direction == "outgoing" {
        if format == MembershipFormat::Msc {
            open_slot(&synced.client,&expected_room,&cfg.slot_id,"m.call",Some(SlotEncryption { encryption_type:"m.per_member".to_owned(), extra:Default::default() })).await?;
        }
        let joined = worker_join(&cfg,&room,format,&server).await?;
        let mut own_members = joined.subscribe_memberships().await;
        let event = tokio::time::timeout(Duration::from_secs(30), own_members.wait_for(|ms|ms.iter().any(|m|m.member_id==joined.membership_id()&&m.membership_event_id.is_some()))).await??;
        let member = event.iter().find(|m|m.member_id==joined.membership_id()).ok_or("no current membership")?;
        req["member_event"] = serde_json::json!(member.membership_event_id);
        req["member_join_id"] = serde_json::json!(member.member_id);
        if format == MembershipFormat::RoomState { req["member_join_ts"] = serde_json::json!(member.membership_ts.ok_or("missing legacy membership generation")?); }
        drop(event);
        let device = synced.client.encryption().get_own_device().await?.ok_or("missing fixture peer device")?;
        if !device.is_cross_signed_by_owner() {return Err("fixture peer must be cross-signed".into());}
        req["expected_device"] = serde_json::json!(synced.client.device_id().ok_or("missing device ID")?.as_str());
        req["expected_signing_key"] = serde_json::json!(device.ed25519_key().ok_or("missing signing key")?.to_base64());
        req["expected_identity_key"] = serde_json::json!(device.curve25519_key().ok_or("missing identity key")?.to_base64());
        req["intent"] = serde_json::json!("audio");
        call=Some(joined);
    }
    let fixture=control.0.join("request.json");
    std::fs::write(&fixture,serde_json::to_vec(&serde_json::json!({"request":req,"end":end,"control_dir":control.0}))?)?;
    let exe=PathBuf::from(env::var("RTC_WORKER_E2E_GO_HARNESS")?);
    if !exe.is_absolute() {return Err("absolute Go harness path required".into());}
    let mut child=Command::new(exe).args(["-test.run=^TestRTCWorkerStaging$","-test.v","-test.timeout=330s"]).env("RTC_TEST_STAGING_FIXTURE",fixture).kill_on_drop(true).spawn()?;
    if direction=="incoming" {
        tokio::time::timeout(Duration::from_secs(75),roster.wait_for(|ms|ms.len()==1&&ms[0].sender==ghost)).await??;
        if end=="cancel" { control.mark("ringing")?; }
        else { call=Some(worker_join(&cfg,&room,format,&server).await?); }
    }
    if end=="cancel" {
        finish_child(&mut child).await?;
        if let Some(joined)=call.take(){joined.leave().await?;}
    } else {
        let joined=call.take().ok_or("missing Matrix peer call")?;
        // Publish real encrypted audio so the worker's pickup probe has media.
        let tone=media::publish_tone(&joined.session(),660.0).await?;
        if !record_peer_tone(&joined,"worker-to-Matrix",440.0).await? {return Err("Matrix peer did not decode the worker tone".into());}
        control.mark("peer-media-ok")?;
        control.wait("gateway-media-ok",&mut child).await?;
        if end=="peer" {
            control.wait("ready-for-peer-end",&mut child).await?;
            drop(tone);joined.leave().await?;finish_child(&mut child).await?;
        } else {
            finish_child(&mut child).await?;drop(tone);joined.leave().await?;
        }
    }
    if direction=="outgoing"&&format==MembershipFormat::Msc {close_slot(&synced.client,&expected_room,&cfg.slot_id).await?;}
    tokio::time::timeout(Duration::from_secs(40),roster.wait_for(|ms|ms.is_empty())).await??;
    if !passive.is_seeded(){return Err("fixture roster feed lost during cleanup".into());}
    Ok(())
}
