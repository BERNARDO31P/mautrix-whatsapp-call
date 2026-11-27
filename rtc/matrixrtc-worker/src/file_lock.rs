//! Persistent advisory locks shared by both native executables.
use std::{fs::{File, Metadata, OpenOptions}, io, os::unix::fs::{MetadataExt, OpenOptionsExt}, path::Path};

pub(crate) fn same_inode(left: &Metadata, right: &Metadata) -> bool {
    left.dev() == right.dev() && left.ino() == right.ino()
}

// Never unlink lock files: every cooperating process must lock the same inode.
// The parent directory is deployment-owned. On Linux File::try_lock and
// Python fcntl.flock use the same advisory locking mechanism.
pub(crate) fn lock_file(path: &Path) -> io::Result<File> {
    if let Ok(metadata) = std::fs::symlink_metadata(path) {
        if !metadata.file_type().is_file() {
            return Err(io::Error::new(io::ErrorKind::InvalidInput, "lock is not a regular file"));
        }
    }
    let file = OpenOptions::new().read(true).write(true).create(true)
        .truncate(false).mode(0o600).open(path)?;
    file.try_lock().map_err(io::Error::other)?;
    let metadata = std::fs::symlink_metadata(path)?;
    if !metadata.file_type().is_file() || !same_inode(&file.metadata()?, &metadata) {
        return Err(io::Error::other("lock path changed"));
    }
    Ok(file)
}

#[cfg(any(feature = "rtc", feature = "provision"))]
fn check_identity_folder(folder: &Path) -> io::Result<()> {
    if !std::fs::symlink_metadata(folder)?.is_dir() {
        return Err(io::Error::other("identity folder is not a real directory"));
    }
    Ok(())
}

// Native provisioner is invoked while Python already owns .provision.lock
// (possibly across Docker). The child therefore takes only this second lock.
// Every native SDK crypto-store owner must take this exact path.
#[cfg(any(feature = "rtc", feature = "provision"))]
pub(crate) fn lock_crypto(folder: &Path) -> io::Result<File> {
    check_identity_folder(folder)?;
    lock_file(&folder.join(".crypto-owner.lock"))
        .map_err(|err| io::Error::other(format!("ghost crypto store is unavailable: {err}")))
}

// Worker and standalone prepare-identity must additionally exclude Python
// session/marker updates, in the same order used by the provisioning wrapper.
#[cfg(feature = "rtc")]
pub(crate) fn lock_identity(folder: &Path) -> io::Result<(File, File)> {
    check_identity_folder(folder)?;
    let provision = lock_file(&folder.join(".provision.lock"))
        .map_err(|err| io::Error::other(format!("ghost identity is busy; retry after provisioning or stop the worker before repair: {err}")))?;
    let crypto = lock_crypto(folder)?;
    Ok((provision, crypto))
}

#[cfg(all(test, feature = "rtc"))]
mod tests {
    use super::*;
    #[test]
    fn crypto_and_provision_locks_exclude_both_native_paths() {
        let folder = std::env::temp_dir().join(format!("matrixrtc-identity-lock-{}", std::process::id()));
        std::fs::create_dir(&folder).unwrap();
        let provision = lock_file(&folder.join(".provision.lock")).unwrap();
        assert!(lock_identity(&folder).is_err());
        let crypto = lock_crypto(&folder).unwrap();
        assert!(lock_crypto(&folder).is_err());
        drop(provision);
        assert!(lock_identity(&folder).is_err());
        drop(crypto);
        let owner = lock_identity(&folder).unwrap();
        assert!(lock_crypto(&folder).is_err());
        assert!(lock_file(&folder.join(".provision.lock")).is_err());
        drop(owner);
        assert!(lock_identity(&folder).is_ok());
        std::fs::remove_dir_all(folder).unwrap();
    }
}
