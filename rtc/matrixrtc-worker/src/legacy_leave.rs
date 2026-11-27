//! Durable legacy membership state-key ownership. The worker holds the
//! identity's provision and crypto-owner locks throughout the journal's life.
use matrix_rtc_core::{CommandError, compat::RoomStateDialect, outbox::{DelayOperation, LegacyLeaveJournal}};
use sha2::{Digest, Sha256};
use std::{fs::{self, File, OpenOptions}, io::{self, Write}, os::unix::fs::{DirBuilderExt, OpenOptionsExt, PermissionsExt}, path::{Path, PathBuf}, sync::Mutex};

pub(crate) struct Journal { dir: PathBuf, operations: PathBuf, access: Mutex<()> }

// One record per non-idempotent create, not per room. An unknown ID must
// remain distinguishable from a confirmed cancellation, including after a
// crash. The identity locks serialize cooperating processes.
#[derive(Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct DelayRecord {
    operation: DelayOperation,
    delay_id: Option<String>,
}

#[derive(Debug, Default)]
struct MembershipSnapshot {
    active: bool,
    sent_delay_id: Option<String>,
}

impl MembershipSnapshot {
    fn proves_sent(&self, id: &str) -> bool {
        !self.active && !id.is_empty() && self.sent_delay_id.as_deref() == Some(id)
    }
}

fn missing_delayed_event(error: &CommandError) -> bool {
    error.backend_error().is_some_and(|error| {
        error.status == Some(404) && error.errcode.as_deref() == Some("M_NOT_FOUND")
    })
}

fn membership_snapshot(
    events: Vec<serde_json::Value>, key: &str, user: &str, device: &str, member_id: &str,
) -> io::Result<MembershipSnapshot> {
    use matrix_rtc_core::compat::STATE_MEMBER_EVENT_TYPE;
    let mut snapshot = MembershipSnapshot::default();
    let mut found = false;
    for event in events {
        let kind = event["type"].as_str().ok_or_else(|| io::Error::other("state event type missing"))?;
        if kind != STATE_MEMBER_EVENT_TYPE { continue; }
        let state_key = event["state_key"].as_str().ok_or_else(|| io::Error::other("membership state key missing"))?;
        if state_key != key { continue; }
        if found { return Err(io::Error::other("duplicate membership state key")); }
        found = true;
        let content = event["content"].as_object().ok_or_else(|| io::Error::other("membership content missing or invalid"))?;
        if event["sender"].as_str() != Some(user) {
            return Err(io::Error::other("current membership has a different sender"));
        }
        if content.is_empty() {
            // Only a server-returned event carrying this delay ID is proof of
            // publication. An empty state event without a delay ID is not.
            let unsigned = &event["unsigned"];
            let stable = unsigned.get("delay_id");
            let unstable = unsigned.get("org.matrix.msc4140.delay_id");
            if stable.is_some() && unstable.is_some() && stable != unstable {
                return Err(io::Error::other("conflicting delayed event IDs"));
            }
            if event["event_id"].as_str().is_some_and(|id| !id.is_empty()) {
                snapshot.sent_delay_id = stable.or(unstable).and_then(serde_json::Value::as_str)
                    .filter(|id| !id.is_empty()).map(str::to_owned);
            }
        } else {
            if content.get("device_id").and_then(serde_json::Value::as_str) != Some(device)
                || content.get("membershipID").and_then(serde_json::Value::as_str) != Some(member_id) {
                return Err(io::Error::other("current membership has a different owner"));
            }
            snapshot.active = true;
        }
    }
    Ok(snapshot)
}

impl Journal {
    pub(crate) fn new(identity_folder: PathBuf) -> io::Result<Self> {
        let dir = identity_folder.join("legacy-membership-ownership");
        let operations = identity_folder.join("delayed-membership-operations");
        Self::create_private_dir(&identity_folder, &dir)?;
        Self::create_private_dir(&identity_folder, &operations)?;
        Ok(Self { dir, operations, access: Mutex::new(()) })
    }

    fn create_private_dir(parent: &Path, dir: &Path) -> io::Result<()> {
        match fs::DirBuilder::new().mode(0o700).create(dir) {
            Ok(()) => File::open(parent)?.sync_all()?,
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {},
            Err(error) => return Err(error),
        }
        let meta = fs::symlink_metadata(dir)?;
        if !meta.is_dir() || meta.permissions().mode() & 0o077 != 0 {
            return Err(io::Error::other("membership journal is not a private real directory"));
        }
        Ok(())
    }

    fn operation_path(&self, operation: &DelayOperation) -> io::Result<PathBuf> {
        if operation.token.len() != 32 || !operation.token.bytes().all(|b| b.is_ascii_hexdigit() && !b.is_ascii_uppercase()) {
            return Err(io::Error::other("invalid delayed operation token"));
        }
        Ok(self.operations.join(&operation.token))
    }

    fn load_operation(&self, operation: &DelayOperation) -> io::Result<DelayRecord> {
        let path = self.operation_path(operation)?;
        if !fs::symlink_metadata(&path)?.is_file() {
            return Err(io::Error::other("delayed operation record is not a regular file"));
        }
        let record: DelayRecord = serde_json::from_slice(&fs::read(path)?).map_err(io::Error::other)?;
        if record.operation != *operation {
            return Err(io::Error::other("delayed operation owner changed"));
        }
        Ok(record)
    }

    fn write_operation_file(path: &Path, record: &DelayRecord) -> io::Result<()> {
        let mut file = OpenOptions::new().write(true).create_new(true).mode(0o600).open(path)?;
        serde_json::to_writer(&mut file, record).map_err(io::Error::other)?;
        file.write_all(b"\n")?;
        file.sync_all()
    }

    fn begin_operation(&self, operation: &DelayOperation) -> io::Result<()> {
        let record = DelayRecord { operation: operation.clone(), delay_id: None };
        Self::write_operation_file(&self.operation_path(operation)?, &record)?;
        File::open(&self.operations)?.sync_all()
    }

    fn identify_operation(&self, operation: &DelayOperation, id: &str) -> io::Result<()> {
        if id.is_empty() {
            return Err(io::Error::other("empty delayed event ID"));
        }
        let mut record = self.load_operation(operation)?;
        if record.delay_id.is_some() {
            return Err(io::Error::other("delayed operation already identified"));
        }
        record.delay_id = Some(id.to_owned());
        let path = self.operation_path(operation)?;
        // Atomic replacement: either the original unknown intent or the
        // complete exact ID survives. A leftover temporary file is evidence,
        // not authority to discard the original record.
        let temp = path.with_extension("identified");
        Self::write_operation_file(&temp, &record)?;
        fs::rename(temp, path)?;
        File::open(&self.operations)?.sync_all()
    }

    fn settle_operation(&self, operation: &DelayOperation, id: Option<&str>) -> io::Result<()> {
        let record = self.load_operation(operation)?;
        if record.delay_id.as_deref() != id {
            return Err(io::Error::other("delayed operation settlement ID mismatch"));
        }
        fs::remove_file(self.operation_path(operation)?)?;
        File::open(&self.operations)?.sync_all()
    }

    // A surviving operation is an independent fence even if the ownership
    // marker was lost or removed. Corruption cannot be assumed unrelated.
    // Include an interrupted atomic ID update; it is additional evidence.
    fn pending_operation(&self, room: &str, key: &str) -> io::Result<bool> {
        for entry in fs::read_dir(&self.operations)? {
            let entry = entry?;
            if !entry.file_type()?.is_file() {
                return Err(io::Error::other("invalid delayed membership operation file"));
            }
            let record: DelayRecord = serde_json::from_slice(&fs::read(entry.path())?).map_err(io::Error::other)?;
            let path = self.operation_path(&record.operation)?;
            if entry.path() != path && entry.path() != path.with_extension("identified") {
                return Err(io::Error::other("delayed membership operation filename mismatch"));
            }
            if record.delay_id.as_ref().is_some_and(|id| id.is_empty()) {
                return Err(io::Error::other("empty delayed membership operation ID"));
            }
            let operation = record.operation;
            if operation.room_id == room && RoomStateDialect::new(
                operation.user_id, operation.device_id, operation.room_id, operation.slot_id,
            ).state_key() == key {
                return Ok(true);
            }
        }
        Ok(false)
    }

    // Called with the room/slot reservation and the outbox leave fence held.
    // Never reconcile another room or a currently active call.
    pub(crate) async fn recover_known(&self, client: &matrix_sdk::Client, room: &str, slot: &str) -> io::Result<()> {
        use matrix_rtc_core::{MatrixBackend, compat::STATE_MEMBER_EVENT_TYPE};
        use matrix_rtc_call_sdk::SdkMatrixBackend;
        use matrix_sdk::ruma::{RoomId, api::client::state::get_state_events};
        let backend = SdkMatrixBackend::new(client.clone());
        let user = backend.own_user_id();
        let device = backend.own_device_id();
        let dialect = RoomStateDialect::new(user.clone(), device.clone(), room.to_owned(), slot.to_owned());
        let key = dialect.state_key();
        let (marker, expected) = self.record(room, &key);
        if !fs::symlink_metadata(&marker)?.is_file() || fs::read(&marker)? != expected {
            return Err(io::Error::other("membership ownership marker not confirmed"));
        }
        let mut records = Vec::new();
        {
            let _access = self.access.lock().map_err(|_| io::Error::other("journal lock poisoned"))?;
            for entry in fs::read_dir(&self.operations)? {
                let entry = entry?;
                if !entry.file_type()?.is_file() {
                    return Err(io::Error::other("invalid delayed membership operation file"));
                }
                let record: DelayRecord = serde_json::from_slice(&fs::read(entry.path())?).map_err(io::Error::other)?;
                let op = &record.operation;
                let op_key = RoomStateDialect::new(op.user_id.clone(), op.device_id.clone(), op.room_id.clone(), op.slot_id.clone()).state_key();
                if op.room_id != room || op_key != key { continue; }
                if entry.path() != self.operation_path(op)? || op.user_id != user || op.device_id != device
                    || op.slot_id != slot || op.member_id != dialect.membership_id()
                    || record.delay_id.as_deref().is_none_or(str::is_empty) {
                    return Err(io::Error::other("unsettled delayed membership identity or ID"));
                }
                records.push(record);
            }
        }
        let room_id = RoomId::parse(room).map_err(io::Error::other)?;
        let member_id = dialect.membership_id();
        // Read authoritative state, not a possibly stale SDK cache. Check it
        // again after cancellation, since a delayed leave can fire meanwhile.
        let read_membership = || async {
            let response = client.send(get_state_events::v3::Request::new(room_id.clone())).await.map_err(io::Error::other)?;
            let events = response.room_state.into_iter()
                .map(|ev| serde_json::from_str(ev.json().get()).map_err(io::Error::other))
                .collect::<io::Result<Vec<serde_json::Value>>>()?;
            membership_snapshot(events, &key, &user, &device, &member_id)
        };
        read_membership().await?;
        for record in &records {
            let id = record.delay_id.as_ref().expect("validated above");
            match backend.cancel_delayed_event(room.to_owned(), id.clone()).await {
                Ok(()) => {},
                Err(error) if missing_delayed_event(&error) => {
                    // A 404 alone is not enough: Synapse excludes events that
                    // are still being processed from its pending-event lookup.
                    // Accept only the exact already-published empty state event.
                    let snapshot = read_membership().await?;
                    if !snapshot.proves_sent(id) {
                        return Err(io::Error::other("delayed leave outcome is unconfirmed"));
                    }
                }
                Err(error) => return Err(io::Error::other(error)),
            }
            self.settle_delay(&record.operation, Some(id.as_str())).map_err(io::Error::other)?;
        }
        if read_membership().await?.active {
            backend.send_state_event(room.to_owned(), STATE_MEMBER_EVENT_TYPE.to_owned(), key.clone(), serde_json::json!({})).await.map_err(io::Error::other)?;
        }
        if read_membership().await?.active {
            return Err(io::Error::other("membership remains active after withdrawal"));
        }
        self.clear(room, &key).map_err(io::Error::other)?;
        eprintln!("RTC membership recovery: previous ownership settled");
        Ok(())
    }

    fn record(&self, room: &str, key: &str) -> (PathBuf, Vec<u8>) {
        let data = serde_json::to_vec(&(room, key)).expect("string pair serializes");
        (self.dir.join(hex::encode(Sha256::digest(&data))), data)
    }

    fn sync_dir(&self) -> io::Result<()> { File::open(&self.dir)?.sync_all() }

    fn present(path: &Path) -> io::Result<bool> {
        match fs::symlink_metadata(path) {
            Ok(_) => Ok(true), // malformed records also block reuse
            Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(false),
            Err(error) => Err(error),
        }
    }

    fn mark_record(&self, room: &str, key: &str) -> io::Result<()> {
        let (path, data) = self.record(room, key);
        let mut file = OpenOptions::new().write(true).create_new(true).mode(0o600).open(path)?;
        file.write_all(&data)?;
        file.sync_all()?;
        self.sync_dir()
    }

    fn clear_record(&self, room: &str, key: &str) -> io::Result<()> {
        let (path, data) = self.record(room, key);
        // Never remove a malformed, unrelated or substituted record.
        if !fs::symlink_metadata(&path)?.is_file() || fs::read(&path)? != data {
            return Err(io::Error::other("legacy membership ownership record changed"));
        }
        fs::remove_file(&path)?;
        self.sync_dir()
    }
}

fn journal_error(error: io::Error) -> CommandError {
    CommandError::ClientRejected(format!("legacy membership ownership journal: {error}"))
}

impl LegacyLeaveJournal for Journal {
    fn pending(&self, room: &str, key: &str) -> Result<bool, CommandError> {
        let _access = self.access.lock().map_err(|_| journal_error(io::Error::other("journal lock poisoned")))?;
        if Self::present(&self.record(room, key).0).map_err(journal_error)? {
            return Ok(true);
        }
        self.pending_operation(room, key).map_err(journal_error)
    }
    fn mark(&self, room: &str, key: &str) -> Result<(), CommandError> {
        let _access = self.access.lock().map_err(|_| journal_error(io::Error::other("journal lock poisoned")))?;
        self.mark_record(room, key).map_err(journal_error)
    }
    fn clear(&self, room: &str, key: &str) -> Result<(), CommandError> {
        let _access = self.access.lock().map_err(|_| journal_error(io::Error::other("journal lock poisoned")))?;
        if self.pending_operation(room, key).map_err(journal_error)? {
            return Err(CommandError::ClientRejected("delayed membership operations remain unsettled".to_owned()));
        }
        self.clear_record(room, key).map_err(journal_error)
    }
    fn records_delays(&self) -> bool { true }
    fn begin_delay(&self, operation: &DelayOperation) -> Result<(), CommandError> {
        let _access = self.access.lock().map_err(|_| journal_error(io::Error::other("journal lock poisoned")))?;
        self.begin_operation(operation).map_err(journal_error)
    }
    fn identify_delay(&self, operation: &DelayOperation, id: &str) -> Result<(), CommandError> {
        let _access = self.access.lock().map_err(|_| journal_error(io::Error::other("journal lock poisoned")))?;
        self.identify_operation(operation, id).map_err(journal_error)
    }
    fn settle_delay(&self, operation: &DelayOperation, id: Option<&str>) -> Result<(), CommandError> {
        let _access = self.access.lock().map_err(|_| journal_error(io::Error::other("journal lock poisoned")))?;
        self.settle_operation(operation, id).map_err(journal_error)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn operation(token: &str) -> DelayOperation {
        DelayOperation {
            token: token.to_owned(), room_id: "!room:a".to_owned(),
            slot_id: "slot".to_owned(), member_id: "member".to_owned(),
            user_id: "@ghost:a".to_owned(), device_id: "DEVICE".to_owned(),
        }
    }
    #[test]
    fn missing_delay_requires_exact_published_leave() {
        use matrix_rtc_core::{BackendError, compat::STATE_MEMBER_EVENT_TYPE};
        let error = |status, code: &str| CommandError::Backend(BackendError::with_matrix_error(
            Some(code.to_owned()), Some(status), "test"));
        assert!(missing_delayed_event(&error(404, "M_NOT_FOUND")));
        assert!(!missing_delayed_event(&error(404, "M_UNRECOGNIZED")));
        assert!(!missing_delayed_event(&error(500, "M_NOT_FOUND")));
        let inspect = |events| membership_snapshot(events, "key", "@ghost:a", "DEVICE", "member");
        let event = serde_json::json!({
            "type": STATE_MEMBER_EVENT_TYPE, "state_key": "key", "sender": "@ghost:a",
            "event_id": "$leave", "content": {}, "unsigned": {"org.matrix.msc4140.delay_id": "expected"}
        });
        assert!(inspect(vec![event.clone()]).unwrap().proves_sent("expected"));
        assert!(!inspect(vec![event.clone()]).unwrap().proves_sent("other"));
        assert!(!inspect(vec![]).unwrap().proves_sent("expected"));
        let mut unknown = event.clone();
        unknown["unsigned"] = serde_json::json!({});
        assert!(!inspect(vec![unknown]).unwrap().proves_sent("expected"));
        let mut conflicting = event.clone();
        conflicting["unsigned"]["delay_id"] = serde_json::json!("other");
        assert!(inspect(vec![conflicting]).is_err());
        let mut foreign = event.clone();
        foreign["sender"] = serde_json::json!("@other:a");
        assert!(inspect(vec![foreign]).is_err());
        let mut active = event.clone();
        active["content"] = serde_json::json!({"device_id":"DEVICE","membershipID":"member"});
        let snapshot = inspect(vec![active]).unwrap();
        assert!(snapshot.active && !snapshot.proves_sent("expected"));
        assert!(inspect(vec![event.clone(), event]).is_err());
    }
    #[test]
    fn delayed_intent_and_exact_id_survive_reopen_and_settle_independently() {
        let root = std::env::temp_dir().join(format!("matrixrtc-delay-journal-{}", std::process::id()));
        fs::create_dir(&root).unwrap();
        let first = operation("00000000000000000000000000000001");
        let second = operation("00000000000000000000000000000002");
        let journal = Journal::new(root.clone()).unwrap();
        journal.begin_delay(&first).unwrap();
        journal.begin_delay(&second).unwrap();
        assert!(journal.begin_delay(&first).is_err());
        journal.identify_delay(&second, "delay-2").unwrap();
        drop(journal);
        let journal = Journal::new(root.clone()).unwrap();
        let key = RoomStateDialect::new("@ghost:a", "DEVICE", "!room:a", "slot").state_key();
        assert!(journal.pending("!room:a", &key).unwrap());
        assert!(!journal.pending("!other:a", &key).unwrap());
        journal.mark("!room:a", &key).unwrap();
        assert!(journal.clear("!room:a", &key).is_err());
        assert_eq!(journal.load_operation(&first).unwrap().delay_id, None);
        assert_eq!(journal.load_operation(&second).unwrap().delay_id.as_deref(), Some("delay-2"));
        assert!(journal.settle_delay(&first, Some("delay-2")).is_err());
        assert!(journal.settle_delay(&second, None).is_err());
        assert!(journal.settle_delay(&second, Some("wrong")).is_err());
        let mut other = second.clone(); other.device_id = "OTHER".to_owned();
        assert!(journal.settle_delay(&other, Some("delay-2")).is_err());
        journal.settle_delay(&second, Some("delay-2")).unwrap();
        assert!(journal.load_operation(&first).is_ok());
        assert!(journal.load_operation(&second).is_err());
        journal.settle_delay(&first, None).unwrap(); // definitive unsupported response only
        journal.clear("!room:a", &key).unwrap();
        assert!(!journal.pending("!room:a", &key).unwrap());
        fs::remove_dir_all(root).unwrap();
    }
    #[test]
    fn ownership_survives_reopen_and_isolated_keys_can_proceed() {
        let root = std::env::temp_dir().join(format!("matrixrtc-legacy-ownership-{}", std::process::id()));
        fs::create_dir(&root).unwrap();
        let journal = Journal::new(root.clone()).unwrap();
        journal.mark("!room:a", "_device_a").unwrap();
        drop(journal);
        let journal = Journal::new(root.clone()).unwrap();
        assert!(journal.pending("!room:a", "_device_a").unwrap());
        assert!(!journal.pending("!room:b", "_device_a").unwrap());
        assert!(!journal.pending("!room:a", "_device_b").unwrap());
        assert!(journal.mark("!room:a", "_device_a").is_err());
        journal.clear("!room:a", "_device_a").unwrap();
        assert!(!Journal::new(root.clone()).unwrap().pending("!room:a", "_device_a").unwrap());
        journal.mark("!room:a", "_device_a").unwrap();
        let path = journal.record("!room:a", "_device_a").0;
        fs::write(&path, b"corrupt").unwrap();
        assert!(journal.pending("!room:a", "_device_a").unwrap());
        assert!(journal.clear("!room:a", "_device_a").is_err());
        assert!(path.exists());
        fs::remove_dir_all(root).unwrap();
    }
}
