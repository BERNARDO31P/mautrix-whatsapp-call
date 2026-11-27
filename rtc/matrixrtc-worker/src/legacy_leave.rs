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
