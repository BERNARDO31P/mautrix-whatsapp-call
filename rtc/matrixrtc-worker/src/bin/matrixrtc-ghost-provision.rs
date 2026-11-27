//! Finalize a persistent appservice ghost device for MatrixRTC.
//!
//! `provision-ghost.py` obtains the appservice-scoped Matrix session and stores
//! it under the worker state directory. This binary opens the same encrypted
//! SDK store, bootstraps or recovers cross-signing and verifies that the
//! persistent device is cross-signed before it can ever be used for MSC4153
//! media-key exchange.

#[path = "../file_lock.rs"]
mod file_lock;

use matrix_sdk::{
    Client,
    authentication::matrix::MatrixSession,
    encryption::{EncryptionSettings, recovery::RecoveryState},
};
use sha2::{Digest, Sha256};
use std::{
    env, fs, io,
    io::{Read, Write},
    os::unix::fs::{OpenOptionsExt, PermissionsExt},
    path::{Path, PathBuf},
    time::Duration,
};

fn error(reason: impl Into<String>) -> io::Error {
    io::Error::other(reason.into())
}

fn required(name: &str) -> io::Result<String> {
    env::var(name).map_err(|_| error(format!("{name} is required")))
}


fn required_secret(name: &str) -> io::Result<String> {
    let file_name = format!("{name}_FILE");
    if let Ok(path) = env::var(&file_name) {
        let path = PathBuf::from(path);
        if !path.is_absolute() {
            return Err(error(format!("{file_name} must be an absolute path")));
        }
        strict_file(&path)?;
        let value = fs::read_to_string(&path)?.trim().to_owned();
        if value.is_empty() {
            return Err(error(format!("{file_name} points to an empty file")));
        }
        return Ok(value);
    }
    required(name).map_err(|_| error(format!("{name} or {file_name} is required")))
}

fn identity_dir(root: &Path, ghost_mxid: &str) -> PathBuf {
    let hash = hex::encode(Sha256::digest(ghost_mxid.as_bytes()));
    root.join("identities").join(hash)
}

fn strict_file(path: &Path) -> io::Result<()> {
    let metadata = fs::metadata(path)?;
    if metadata.permissions().mode() & 0o077 != 0 {
        return Err(error(format!("{} must have mode 0600 or stricter", path.display())));
    }
    Ok(())
}

fn read_recovery_key(folder: &Path) -> io::Result<Option<String>> {
    if let Ok(key) = env::var("RTC_GHOST_RECOVERY_KEY") {
        let key = key.trim().to_owned();
        if !key.is_empty() {
            return Ok(Some(key));
        }
    }
    let path = folder.join("recovery-key.txt");
    match fs::symlink_metadata(&path) {
        Ok(metadata) if metadata.file_type().is_file() => {},
        Ok(_) => return Err(error("recovery-key.txt must be a regular file")),
        Err(err) if err.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(err) => return Err(err),
    }
    strict_file(&path)?;
    let key = fs::read_to_string(path)?.trim().to_owned();
    if key.is_empty() {
        return Err(error("stored recovery-key.txt is empty"));
    }
    Ok(Some(key))
}

// The SDK can use a passphrase supplied before the first server mutation.
// Keep the existing filename: recover() accepts either a recovery key or a
// passphrase. Never overwrite a previously stored recovery credential.
fn prepare_recovery_passphrase(folder: &Path) -> io::Result<String> {
    let mut entropy = [0u8; 32];
    fs::File::open("/dev/urandom")?.read_exact(&mut entropy)?;
    let passphrase = hex::encode(entropy);
    let path = folder.join("recovery-key.txt");
    let mut file = fs::OpenOptions::new()
        .write(true).create_new(true).mode(0o600).open(&path)?;
    file.write_all(passphrase.as_bytes())?;
    file.write_all(b"\n")?;
    file.sync_all()?;
    // Persist the directory entry as well as the file contents. If any write
    // fails, do not call enable(); retain the file as evidence for diagnosis.
    fs::File::open(folder)?.sync_all()?;
    Ok(passphrase)
}

#[cfg(test)]
mod recovery_file_tests {
    use super::*;

    #[test]
    fn credential_is_persisted_before_use_and_never_replaced() {
        let folder = env::temp_dir().join(format!("matrixrtc-recovery-test-{}", std::process::id()));
        fs::create_dir(&folder).unwrap();
        let passphrase = prepare_recovery_passphrase(&folder).unwrap();
        let path = folder.join("recovery-key.txt");
        assert_eq!(passphrase.len(), 64);
        assert_eq!(fs::read_to_string(&path).unwrap().trim(), passphrase);
        assert_eq!(fs::metadata(&path).unwrap().permissions().mode() & 0o777, 0o600);
        assert!(prepare_recovery_passphrase(&folder).is_err());
        assert_eq!(fs::read_to_string(&path).unwrap().trim(), passphrase);
        fs::remove_dir_all(folder).unwrap();
    }
}

async fn wait_for_cross_signed_device(client: &Client) -> io::Result<()> {
    for _ in 0..50 {
        if let Some(device) = client.encryption().get_own_device().await.map_err(io::Error::other)?
            && device.is_cross_signed_by_owner()
        {
            return Ok(());
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    Err(error(
        "ghost device is not cross-signed after provisioning; refusing MatrixRTC use",
    ))
}

#[tokio::main]
async fn main() -> io::Result<()> {
    // Release builds enable rtc for both binaries, unifying rustls features.
    #[cfg(feature = "rtc")]
    let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
    let homeserver = required("MATRIX_HOMESERVER")?;
    if !(homeserver.starts_with("https://") || (homeserver.starts_with("http://") && env::var("RTC_ALLOW_INSECURE_HOMESERVER").as_deref() == Ok("1"))) {
        return Err(error("MATRIX_HOMESERVER must use HTTPS"));
    }
    let state_root = PathBuf::from(required("RTC_WORKER_STATE_DIR")?);
    if !state_root.is_absolute() {
        return Err(error("RTC_WORKER_STATE_DIR must be absolute"));
    }
    let ghost_mxid = required("RTC_GHOST_MXID")?;
    if !ghost_mxid.starts_with('@') || !ghost_mxid.contains(':') {
        return Err(error("RTC_GHOST_MXID must be a full Matrix user ID"));
    }
    let passphrase = required_secret("RTC_WORKER_STORE_PASSPHRASE")?;
    let folder = identity_dir(&state_root, &ghost_mxid);
    let _crypto_owner = file_lock::lock_crypto(&folder)?;
    let session_path = folder.join("session.json");
    strict_file(&session_path)?;
    fs::create_dir_all(folder.join("crypto"))?;
    fs::set_permissions(&folder, fs::Permissions::from_mode(0o700))?;
    fs::set_permissions(folder.join("crypto"), fs::Permissions::from_mode(0o700))?;

    let session: MatrixSession = serde_json::from_slice(&fs::read(&session_path)?)
        .map_err(|err| error(format!("invalid session.json: {err}")))?;
    if session.meta.user_id.as_str() != ghost_mxid {
        return Err(error("stored Matrix session belongs to a different ghost"));
    }

    let builder = Client::builder()
        .homeserver_url(&homeserver)
        .sqlite_store(folder.join("crypto"), Some(&passphrase))
        .with_encryption_settings(EncryptionSettings {
            auto_enable_cross_signing: true,
            ..Default::default()
        });
    // Allow explicitly configured internal HTTP, never bypass HTTPS certificate validation.
    let client = builder.build().await.map_err(io::Error::other)?;
    client.restore_session(session).await.map_err(io::Error::other)?;
    client.encryption().wait_for_e2ee_initialization_tasks().await;

    let recovery = client.encryption().recovery();
    let secret_storage = client.encryption().secret_storage();
    let credential = match read_recovery_key(&folder)? {
        Some(key) => key,
        None if recovery.state() == RecoveryState::Disabled => {
            // A cached Disabled state alone is not authorization to replace
            // secret storage. Any existing default-key event, even malformed,
            // requires operator reconciliation rather than automatic rotation.
            if secret_storage.fetch_default_key_id().await.map_err(io::Error::other)?.is_some() {
                return Err(error("existing secret storage needs its original recovery credential"));
            }
            let passphrase = prepare_recovery_passphrase(&folder)?;
            // The passphrase is already durable, including on response loss or
            // process death after the server accepts the default-key update.
            recovery.enable().with_passphrase(&passphrase).await.map_err(|_| {
                error("recovery enablement incomplete; preserve recovery-key.txt and reconcile existing server recovery before retry")
            })?;
            passphrase
        }
        None => return Err(error(
            "existing ghost recovery needs its credential; set RTC_GHOST_RECOVERY_KEY or restore recovery-key.txt",
        )),
    };
    // Even an Enabled cached SDK state does not prove the saved credential
    // opens the current server-side secret store. Do not mark such an identity
    // ready until that check succeeds. Never reset recovery automatically.
    secret_storage.open_secret_store(&credential).await.map_err(|_| {
        error("cannot open existing ghost secret storage; preserve the credential and reconcile recovery")
    })?;
    if recovery.state() != RecoveryState::Enabled {
        recovery.recover(&credential).await.map_err(|_| error("ghost recovery incomplete; preserve the recovery credential"))?;
    }
    if recovery.state() != RecoveryState::Enabled {
        return Err(error("ghost recovery remains incomplete; refusing readiness"));
    }

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

    wait_for_cross_signed_device(&client).await?;
    eprintln!(
        "MatrixRTC ghost device {} for {} is persistent and cross-signed",
        client.device_id().map_or("<unknown>", |id| id.as_str()),
        ghost_mxid,
    );
    Ok(())
}
