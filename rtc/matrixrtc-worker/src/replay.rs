//! Replay guard persists recent call IDs across worker restarts. No live call is ever resumed.
use sha2::{Digest, Sha256};
use std::{
    fs,
    io,
    io::Write,
    os::unix::fs::{OpenOptionsExt, PermissionsExt},
    path::{Path, PathBuf},
    sync::atomic::{AtomicU64, Ordering},
    time::{Duration, SystemTime},
};

const DEFAULT_RETENTION_DAYS: u64 = 90;
const MAX_RETENTION_DAYS: u64 = 3650;
const PRUNE_EVERY_REGISTRATIONS: u64 = 128;

pub struct ReplayGuard {
    dir: PathBuf,
    retention: Duration,
    registrations: AtomicU64,
}

impl ReplayGuard {
    pub fn open(dir: &Path) -> io::Result<Self> {
        let retention_days = match std::env::var("RTC_REPLAY_RETENTION_DAYS") {
            Ok(value) => value.parse::<u64>().map_err(|_| {
                io::Error::new(io::ErrorKind::InvalidInput, "RTC_REPLAY_RETENTION_DAYS must be an integer")
            })?,
            Err(std::env::VarError::NotPresent) => DEFAULT_RETENTION_DAYS,
            Err(err) => return Err(io::Error::new(io::ErrorKind::InvalidInput, err)),
        };
        if !(1..=MAX_RETENTION_DAYS).contains(&retention_days) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("RTC_REPLAY_RETENTION_DAYS must be within 1..={MAX_RETENTION_DAYS}"),
            ));
        }
        Self::open_with_retention(dir, Duration::from_secs(retention_days * 24 * 60 * 60))
    }

    fn open_with_retention(dir: &Path, retention: Duration) -> io::Result<Self> {
        fs::create_dir_all(dir)?;
        fs::set_permissions(dir, fs::Permissions::from_mode(0o700))?;
        let guard = Self {
            dir: dir.to_path_buf(),
            retention,
            registrations: AtomicU64::new(0),
        };
        guard.prune_expired()?;
        Ok(guard)
    }

    fn is_seen_filename(name: &str) -> bool {
        name.len() == 69
            && name.ends_with(".seen")
            && name[..64].bytes().all(|value| value.is_ascii_hexdigit())
    }

    fn prune_expired(&self) -> io::Result<()> {
        let cutoff = SystemTime::now()
            .checked_sub(self.retention)
            .unwrap_or(SystemTime::UNIX_EPOCH);
        for entry in fs::read_dir(&self.dir)? {
            let entry = entry?;
            let file_name = entry.file_name();
            let Some(file_name) = file_name.to_str() else { continue };
            if !Self::is_seen_filename(file_name) {
                continue;
            }
            let metadata = entry.metadata()?;
            if !metadata.file_type().is_file() {
                continue;
            }
            if metadata.modified()? < cutoff {
                match fs::remove_file(entry.path()) {
                    Ok(()) => {}
                    Err(err) if err.kind() == io::ErrorKind::NotFound => {}
                    Err(err) => return Err(err),
                }
            }
        }
        Ok(())
    }

    fn entry_path(&self, login_id: &str, call_id: &str) -> io::Result<PathBuf> {
        if login_id.is_empty() || call_id.is_empty() || login_id.len() > 256 || call_id.len() > 256 {
            return Err(io::Error::new(io::ErrorKind::InvalidInput, "invalid call identity"));
        }
        let mut hash = Sha256::new();
        hash.update(login_id.as_bytes());
        hash.update([0]);
        hash.update(call_id.as_bytes());
        let filename = format!("{}.seen", hex::encode(hash.finalize()));
        Ok(self.dir.join(filename))
    }

    #[cfg(any(test, feature = "rtc"))]
    pub fn contains(&self, login_id: &str, call_id: &str) -> io::Result<bool> {
        self.entry_path(login_id, call_id)?.try_exists()
    }

    pub fn register(&self, login_id: &str, call_id: &str) -> io::Result<bool> {
        let registrations = self.registrations.fetch_add(1, Ordering::Relaxed) + 1;
        if registrations % PRUNE_EVERY_REGISTRATIONS == 0 {
            self.prune_expired()?;
        }

        let path = self.entry_path(login_id, call_id)?;
        let mut file = match fs::OpenOptions::new().write(true).create_new(true).mode(0o600).open(path) {
            Ok(file) => file,
            Err(err) if err.kind() == io::ErrorKind::AlreadyExists => return Ok(false),
            Err(err) => return Err(err),
        };
        file.write_all(format!("{login_id}\n{call_id}\n").as_bytes())?;
        file.sync_all()?;
        fs::File::open(&self.dir)?.sync_all()?;
        Ok(true)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_dir(name: &str) -> PathBuf {
        std::env::temp_dir().join(format!("matrixrtc-replay-{}-{name}", std::process::id()))
    }

    #[test]
    fn replay_is_rejected_after_restart() {
        let dir = test_dir("restart");
        let _ = fs::remove_dir_all(&dir);
        let guard = ReplayGuard::open_with_retention(&dir, Duration::from_secs(86400)).unwrap();
        assert!(!guard.contains("login-a", "call-a").unwrap());
        assert!(guard.register("login-a", "call-a").unwrap());
        assert!(guard.contains("login-a", "call-a").unwrap());
        assert!(!ReplayGuard::open_with_retention(&dir, Duration::from_secs(86400))
            .unwrap()
            .register("login-a", "call-a")
            .unwrap());
        assert!(guard.register("login-b", "call-a").unwrap());
        fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn expired_replay_entries_are_pruned_but_current_entries_survive() {
        let dir = test_dir("prune");
        let _ = fs::remove_dir_all(&dir);
        let guard = ReplayGuard::open_with_retention(&dir, Duration::from_secs(3600)).unwrap();
        assert!(guard.register("old-login", "old-call").unwrap());

        let old_path = fs::read_dir(&dir)
            .unwrap()
            .next()
            .unwrap()
            .unwrap()
            .path();
        let file = fs::OpenOptions::new().write(true).open(&old_path).unwrap();
        file.set_times(fs::FileTimes::new().set_modified(SystemTime::UNIX_EPOCH)).unwrap();

        guard.prune_expired().unwrap();
        assert!(!old_path.exists());

        assert!(guard.register("new-login", "new-call").unwrap());
        assert!(!guard.register("new-login", "new-call").unwrap());
        assert_eq!(fs::read_dir(&dir).unwrap().count(), 1);
        fs::remove_dir_all(&dir).unwrap();
    }
}
