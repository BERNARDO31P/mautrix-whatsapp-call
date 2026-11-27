//! WhatsApp <-> MatrixRTC sidecar. Calls remain explicitly opt-in at runtime.
mod audio;
mod protocol;
mod participant_epoch;
mod replay;
mod video;
mod file_lock;
use file_lock::{lock_file as lock_worker_file, same_inode};
#[cfg(feature = "rtc")]
mod rtc;
#[cfg(feature = "rtc")]
mod legacy_leave;

use protocol::Message;
use replay::ReplayGuard;
use std::{io, os::unix::fs::{FileTypeExt}, path::{Path, PathBuf}, sync::Arc};
use tokio::{io::{AsyncWrite, AsyncWriteExt, ReadHalf, WriteHalf}, net::{UnixListener, UnixStream}, sync::{mpsc, oneshot}};

// A media producer may be aborted during mute/unpublish or call teardown.
// It must never own the future that writes a partial wire frame. A single
// bounded writer task completes the frame independently of its producer.
#[derive(Clone)]
struct Writer {
    queue: mpsc::Sender<WriteRequest>,
}

type WriteRequest = (Vec<u8>, oneshot::Sender<io::Result<()>>, tokio::time::Instant);
const WIRE_SEND_BUDGET: std::time::Duration = std::time::Duration::from_secs(2);

fn write_timeout() -> io::Error {
    io::Error::new(io::ErrorKind::TimedOut, "RTC send deadline exceeded")
}

impl Writer {
    fn new<W: AsyncWrite + Unpin + Send + 'static>(mut sink: W) -> Self {
        let (queue, mut receiver) = mpsc::channel::<WriteRequest>(8);
        tokio::spawn(async move {
            while let Some((bytes, reply, deadline)) = receiver.recv().await {
                // No bytes have been written for this request yet.
                if reply.is_closed() { continue; }
                if tokio::time::Instant::now() >= deadline {
                    let _ = reply.send(Err(write_timeout()));
                    continue;
                }
                // The same deadline bounds admission, queue residence and
                // the complete socket write. Never restart it at dequeue.
                let result = tokio::time::timeout_at(deadline, protocol::write_encoded(&mut sink, &bytes))
                    .await.unwrap_or_else(|_| Err(write_timeout()));
                let failed = result.is_err();
                let _ = reply.send(result);
                // A failed write may already have put a length or partial
                // body on the wire. No subsequent frame may use this stream.
                if failed { break; }
            }
            // Release queued producers immediately, not after shutdown.
            drop(receiver);
            let _ = tokio::time::timeout(WIRE_SEND_BUDGET, sink.shutdown()).await;
        });
        Self { queue }
    }
}

#[cfg(feature = "rtc")]
fn init_tls_provider() -> io::Result<()> {
    // Matrix SDK and LiveKit/reqwest enable different rustls backends. Select
    // the same provider as the bundled SDK examples before building any TLS
    // client, not only when the first call is received.
    let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
    if rustls::crypto::CryptoProvider::get_default().is_none() {
        return Err(io::Error::other("rustls CryptoProvider initialization failed"));
    }
    Ok(())
}

#[cfg(feature = "rtc")]
fn verify_tls_client() -> io::Result<()> {
    let _ = rustls::ClientConfig::builder()
        .with_root_certificates(rustls::RootCertStore::empty())
        .with_no_client_auth();
    let _ = reqwest::Client::builder().build().map_err(io::Error::other)?;
    Ok(())
}

// Only the legacy decoder's reason codes (never media keys, tokens, raw event
// content or dependency-wide request tracing) are enabled in the sidecar.
#[cfg(feature = "rtc")]
struct LegacyDecoderLogger;
#[cfg(feature = "rtc")]
impl log::Log for LegacyDecoderLogger {
    fn enabled(&self, metadata: &log::Metadata<'_>) -> bool {
        metadata.target() == "matrix_rtc_core::compat::room_state"
            && metadata.level() <= log::Level::Debug
    }
    fn log(&self, record: &log::Record<'_>) {
        if self.enabled(record.metadata()) {
            eprintln!("RTC legacy membership: {}", record.args());
        }
    }
    fn flush(&self) {}
}
#[cfg(feature = "rtc")]
static LEGACY_DECODER_LOGGER: LegacyDecoderLogger = LegacyDecoderLogger;

#[tokio::main]
async fn main() -> io::Result<()> {
    #[cfg(feature = "rtc")]
    {
        init_tls_provider()?;
        if log::set_logger(&LEGACY_DECODER_LOGGER).is_ok() {
            log::set_max_level(log::LevelFilter::Debug);
        }
        if std::env::args().nth(1).as_deref() != Some("healthcheck") {
            verify_tls_client()?;
        }
        if std::env::args().nth(1).as_deref() == Some("tls-self-test") {
            eprintln!("MatrixRTC TLS client construction: PASS");
            return Ok(());
        }
    }
    let state_dir = PathBuf::from(std::env::var("RTC_WORKER_STATE_DIR").map_err(|_| {
        io::Error::new(io::ErrorKind::InvalidInput, "RTC_WORKER_STATE_DIR is required")
    })?);
    if !state_dir.is_absolute() {
        return Err(io::Error::new(io::ErrorKind::InvalidInput, "state path must be absolute"));
    }

    let mut args = std::env::args().skip(1);
    if let Some(command) = args.next() {
        if command=="healthcheck" {
            let socket=std::env::var("RTC_WORKER_SOCKET").map_err(io::Error::other)?;
            return tokio::time::timeout(std::time::Duration::from_secs(2),async{
                let mut stream=UnixStream::connect(socket).await?;
                protocol::write(&mut stream,&Message::new("ping","health")).await?;
                let reply=protocol::read(&mut stream).await?;
                if reply.kind=="pong" && reply.call_id=="health"{Ok(())}else{Err(io::Error::other("worker protocol probe failed"))}
            }).await.map_err(|_|io::Error::new(io::ErrorKind::TimedOut,"worker protocol probe timed out"))?;
        }
        if command != "prepare-identity" {
            return Err(io::Error::new(io::ErrorKind::InvalidInput, format!("unknown command {command}")));
        }
        let ghost_mxid = args.next().ok_or_else(|| {
            io::Error::new(io::ErrorKind::InvalidInput, "prepare-identity requires a ghost MXID")
        })?;
        if args.next().is_some() {
            return Err(io::Error::new(io::ErrorKind::InvalidInput, "prepare-identity accepts exactly one ghost MXID"));
        }
        #[cfg(feature = "rtc")]
        {
            rtc::prepare_identity(state_dir.join("identities"), &ghost_mxid).await?;
            eprintln!("MatrixRTC ghost identity prepared: {ghost_mxid}");
            return Ok(());
        }
        #[cfg(not(feature = "rtc"))]
        {
            let _ = ghost_mxid;
            return Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "prepare-identity requires a worker built with --features rtc",
            ));
        }
    }

    let socket = std::env::var("RTC_WORKER_SOCKET").map_err(|_| {
        io::Error::new(io::ErrorKind::InvalidInput, "RTC_WORKER_SOCKET is required")
    })?;
    if !PathBuf::from(&socket).is_absolute() {
        return Err(io::Error::new(io::ErrorKind::InvalidInput, "socket path must be absolute"));
    }
    let socket_path = PathBuf::from(&socket);
    use std::os::unix::fs::PermissionsExt;
    let socket_mode = match std::env::var("RTC_WORKER_SOCKET_MODE").as_deref() {
        Err(_) | Ok("0600") | Ok("600") => 0o600,
        Ok("0660") | Ok("660") => 0o660,
        Ok(value) => {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("RTC_WORKER_SOCKET_MODE must be 0600 or 0660, got {value}"),
            ));
        }
    };
    // Keep both locks alive until after socket cleanup. A second daemon using
    // either the same state or the same socket must fail before mutating it.
    std::fs::create_dir_all(&state_dir)?;
    let _state_owner = lock_worker_file(&state_dir.join("worker-owner.lock"))?;
    let mut socket_lock_path = socket_path.as_os_str().to_os_string();
    socket_lock_path.push(".lock");
    let _socket_owner = lock_worker_file(Path::new(&socket_lock_path))?;
    let replay = Arc::new(ReplayGuard::open(&state_dir.join("seen-calls"))?);
    #[cfg(feature = "rtc")]
    let accounts = Arc::new(rtc::Accounts::new(state_dir.join("identities")));
    let listener = bind_worker_socket(&socket_path).await?;
    let _cleanup = SocketCleanup::new(socket_path.clone())?;
    std::fs::set_permissions(&socket_path, std::fs::Permissions::from_mode(socket_mode))?;
    let mut terminate = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
    eprintln!("MatrixRTC sidecar listening on {socket} (mode {socket_mode:04o})");
    let (shutdown, _) = tokio::sync::watch::channel(false);
    let mut jobs = tokio::task::JoinSet::new();
    #[cfg(feature = "rtc")]
    let recovery = {
        let accounts = accounts.clone();
        tokio::spawn(async move {
            loop {
                if let Err(err) = accounts.recover().await { eprintln!("slot recovery: {err}"); }
                tokio::time::sleep(std::time::Duration::from_secs(60)).await;
            }
        })
    };
    let serve_result = loop {
        tokio::select! {
            Some(_) = jobs.join_next(), if !jobs.is_empty() => {}
            accepted = listener.accept() => {
                let (stream, _) = match accepted { Ok(value) => value, Err(err) => break Err(err) };
                let replay = replay.clone();
                #[cfg(feature = "rtc")]
                let accounts = accounts.clone();
                let raw=match stream.into_std() { Ok(value) => value, Err(err) => break Err(err) };
                let closer=match raw.try_clone() { Ok(value) => value, Err(err) => break Err(err) };
                let stream=match UnixStream::from_std(raw) { Ok(value) => value, Err(err) => break Err(err) };
                let mut stopping=shutdown.subscribe();
                jobs.spawn(async move {
                    let session=serve(stream,replay,#[cfg(feature="rtc")] accounts);
                    tokio::pin!(session);
                    let result=tokio::select! {
                        r=&mut session=>r,
                        _=stopping.changed()=>{let _=closer.shutdown(std::net::Shutdown::Both);session.await}
                    };
                    if let Err(error) = result {
                        eprintln!("connection ended: {error}");
                    }
                });
            }
            result = tokio::signal::ctrl_c() => break result,
            _ = terminate.recv() => break Ok(()),
        }
    };
    drop(listener);
    let _ = shutdown.send(true);
    #[cfg(feature = "rtc")]
    {
        recovery.abort();
        let _ = recovery.await;
    }
    if tokio::time::timeout(std::time::Duration::from_secs(45),async {
        while jobs.join_next().await.is_some() {}
    }).await.is_err() {
        jobs.abort_all();
        while jobs.join_next().await.is_some() {}
    }
    serve_result
}

struct SocketCleanup { path: PathBuf, metadata: std::fs::Metadata }
impl SocketCleanup {
    fn new(path: PathBuf) -> io::Result<Self> {
        let metadata = std::fs::symlink_metadata(&path)?;
        if !metadata.file_type().is_socket() {
            return Err(io::Error::other("bound worker socket path changed"));
        }
        Ok(Self { path, metadata })
    }
}
impl Drop for SocketCleanup {
    fn drop(&mut self) {
        if let Ok(current) = std::fs::symlink_metadata(&self.path) {
            if current.file_type().is_socket() && same_inode(&self.metadata, &current) {
                let _ = std::fs::remove_file(&self.path);
            }
        }
    }
}

async fn bind_worker_socket(path: &Path) -> io::Result<UnixListener> {
    match std::fs::symlink_metadata(path) {
        Ok(metadata) => {
            if !metadata.file_type().is_socket() {
                return Err(io::Error::new(
                    io::ErrorKind::AlreadyExists,
                    format!("worker socket path exists and is not a Unix socket: {}", path.display()),
                ));
            }
            match tokio::time::timeout(std::time::Duration::from_millis(500), UnixStream::connect(path)).await {
                Ok(Ok(stream)) => {
                    drop(stream);
                    return Err(io::Error::new(
                        io::ErrorKind::AddrInUse,
                        format!("another MatrixRTC worker is already listening on {}", path.display()),
                    ));
                }
                Ok(Err(err)) if matches!(err.kind(), io::ErrorKind::ConnectionRefused | io::ErrorKind::NotFound) => {
                    match std::fs::symlink_metadata(path) {
                        Ok(current) if current.file_type().is_socket() && same_inode(&metadata, &current) => std::fs::remove_file(path)?,
                        Err(err) if err.kind() == io::ErrorKind::NotFound => {},
                        Ok(_) => return Err(io::Error::other("worker socket path changed during probe")),
                        Err(err) => return Err(err),
                    }
                }
                Ok(Err(err)) => return Err(err),
                Err(_) => {
                    return Err(io::Error::new(
                        io::ErrorKind::TimedOut,
                        format!("timed out probing existing worker socket {}", path.display()),
                    ));
                }
            }
        }
        Err(err) if err.kind() == io::ErrorKind::NotFound => {}
        Err(err) => return Err(err),
    }
    UnixListener::bind(path)
}

async fn send(writer: &Writer, msg: &Message) -> io::Result<()> {
    // Reserve bounded queue capacity before allocating the encoded frame.
    // Dropping the permit on local validation failure leaves the socket intact.
    let deadline = tokio::time::Instant::now() + WIRE_SEND_BUDGET;
    let permit = tokio::time::timeout_at(deadline, writer.queue.reserve()).await
        .map_err(|_| write_timeout())?
        .map_err(|_| io::Error::new(io::ErrorKind::BrokenPipe, "RTC writer stopped"))?;
    if tokio::time::Instant::now() >= deadline { return Err(write_timeout()); }
    let bytes = protocol::encode_frame(msg)?;
    if tokio::time::Instant::now() >= deadline { return Err(write_timeout()); }
    let (reply, completion) = oneshot::channel();
    permit.send((bytes, reply, deadline));
    // Prefer an already delivered outcome to a simultaneous timer expiry.
    // The writer retains the exact deadline even if this producer is dropped.
    tokio::time::timeout_at(deadline, completion).await.map_err(|_| write_timeout())?.map_err(|_| io::Error::new(io::ErrorKind::BrokenPipe, "RTC writer stopped"))?
}

async fn serve(stream: UnixStream, replay: Arc<ReplayGuard>, #[cfg(feature = "rtc")] accounts: Arc<rtc::Accounts>) -> io::Result<()> {
    let (mut reader, writer): (ReadHalf<UnixStream>, WriteHalf<UnixStream>) = tokio::io::split(stream);
    let writer = Writer::new(writer);
    let incoming = tokio::time::timeout(std::time::Duration::from_secs(5), protocol::read(&mut reader))
        .await.map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "no incoming call"))??;
    if incoming.kind=="ping" && incoming.call_id=="health" {return send(&writer,&Message::new("pong","health")).await}
    if !matches!(incoming.kind.as_str(), "incoming" | "outgoing") || incoming.call_id.is_empty() || incoming.login_id.is_empty()
        || !incoming.room_id.starts_with('!') || !incoming.ghost_mxid.starts_with('@')
        || !incoming.expected_user_mxid.starts_with('@') || incoming.expected_user_mxid == incoming.ghost_mxid
        || incoming.slot_id.is_empty() || !matches!(incoming.membership_format.as_str(), "msc" | "room_state")
        || (incoming.kind=="outgoing" && (incoming.expected_device.is_empty() || !protocol::valid_device_key(&incoming.expected_signing_key) || !protocol::valid_device_key(&incoming.expected_identity_key))) || incoming.peer.is_empty() || !incoming.data.is_empty()
        || (incoming.group && (!incoming.group_jid.ends_with("@g.us") || incoming.group_jid.len() < 6 || incoming.peer!=incoming.group_jid))
        || (!incoming.group && !incoming.group_jid.is_empty()) {
        return Err(io::Error::new(io::ErrorKind::InvalidData, "invalid first message: incoming/outgoing call with room, slot, membership format, RTC identity, expected human and valid direct/group target required"));
    }
    #[cfg(feature = "rtc")]
    {
        if std::env::var("RTC_WORKER_ENABLE_CALLS").as_deref() != Ok("1") {
            let mut reject = Message::new("error", &incoming.call_id);
            reject.reason = "RTC calls disabled by configuration".to_owned();
            return send(&writer, &reject).await;
        }
        return rtc::handle(incoming, reader, writer, accounts, replay).await;
    }
    #[cfg(not(feature = "rtc"))]
    {
        let _ = replay;
        let mut reject = Message::new("error", &incoming.call_id);
        reject.reason = "worker was built without encrypted MatrixRTC media support".to_owned();
        send(&writer, &reject).await
    }
}

#[cfg(all(test, not(feature = "rtc")))]
mod tests {
    use super::*;

    #[test]
    fn worker_lock_is_exclusive_and_persistent() {
        let dir = std::env::temp_dir().join(format!("matrixrtc-worker-lock-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("owner.lock");
        let owner = lock_worker_file(&path).unwrap();
        assert!(lock_worker_file(&path).is_err());
        drop(owner);
        assert!(path.is_file());
        let owner = lock_worker_file(&path).unwrap();
        drop(owner);
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn socket_cleanup_preserves_replacement_file() {
        let dir = std::env::temp_dir().join(format!("matrixrtc-worker-cleanup-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("worker.sock");
        let listener = std::os::unix::net::UnixListener::bind(&path).unwrap();
        let cleanup = SocketCleanup::new(path.clone()).unwrap();
        std::fs::remove_file(&path).unwrap();
        std::fs::write(&path, b"replacement").unwrap();
        drop(cleanup);
        assert_eq!(std::fs::read(&path).unwrap(), b"replacement");
        drop(listener);
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[tokio::test]
    async fn recovers_stale_socket_but_never_replaces_live_listener() {
        let dir = std::env::temp_dir().join(format!("matrixrtc-socket-startup-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("worker.sock");

        {
            let stale = std::os::unix::net::UnixListener::bind(&path).unwrap();
            drop(stale);
        }
        let recovered = bind_worker_socket(&path).await.unwrap();
        drop(recovered);
        std::fs::remove_file(&path).unwrap();

        let live = UnixListener::bind(&path).unwrap();
        let error = bind_worker_socket(&path).await.unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::AddrInUse);
        drop(live);
        std::fs::remove_file(&path).unwrap();
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[tokio::test]
    async fn disabled_calls_remain_retryable_across_connections() {
        let dir = std::env::temp_dir().join(format!("matrixrtc-socket-pair-test-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let guard = Arc::new(ReplayGuard::open(&dir).unwrap());
        let mut call = Message::new("incoming", "call-17");
        call.login_id = "user-login".to_string();
        call.ghost_mxid = "@ghost:example.org".to_string();
        call.expected_user_mxid = "@alice:example.org".to_string();
        call.room_id = "!room:example.org".to_string();
        call.slot_id = "m.call#room".to_string();
        call.membership_format = "msc".to_string();
        call.peer = "123@s.whatsapp.net".to_string();
        for expected in ["worker was built without encrypted MatrixRTC media support";2] {
            let (mut peer, worker) = UnixStream::pair().unwrap();
            let guard = guard.clone();
            let job = tokio::spawn(async move { serve(worker, guard).await });
            protocol::write(&mut peer, &call).await.unwrap();
            let response = protocol::read(&mut peer).await.unwrap();
            assert_eq!(response.kind, "error");
            assert_eq!(response.call_id, call.call_id);
            assert_eq!(response.reason, expected);
            drop(peer);
            job.await.unwrap().unwrap();
        }
        std::fs::remove_dir_all(dir).unwrap();
    }
}

#[cfg(test)]
mod writer_tests {
    use super::*;
    use tokio::io::AsyncReadExt;

    #[tokio::test]
    async fn cancelled_producer_cannot_truncate_a_frame() {
        let (sink, mut peer) = tokio::io::duplex(64);
        let writer = Writer::new(sink);
        let mut first = Message::new("error", "first");
        first.reason = "x".repeat(1024);
        let first_copy = first.clone();
        let producer_writer = writer.clone();
        let producer = tokio::spawn(async move { send(&producer_writer, &first_copy).await });
        let size = peer.read_u32().await.unwrap() as usize;
        // The header has arrived, but the body cannot fit into this pipe.
        producer.abort();
        assert!(producer.await.unwrap_err().is_cancelled());
        let second = Message::new("hangup", "second");
        let second_copy = second.clone();
        let next_writer = writer.clone();
        let next = tokio::spawn(async move { send(&next_writer, &second_copy).await });
        let mut body = vec![0; size];
        peer.read_exact(&mut body).await.unwrap();
        assert_eq!(serde_json::from_slice::<Message>(&body).unwrap(), first);
        assert_eq!(protocol::read(&mut peer).await.unwrap(), second);
        next.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn local_rejection_does_not_close_writer_or_emit_bytes() {
        let (sink, mut peer) = tokio::io::duplex(8192);
        let writer = Writer::new(sink);
        let mut invalid = Message::new("error", "call");
        invalid.reason = "x".repeat(4097);
        assert_eq!(send(&writer, &invalid).await.unwrap_err().kind(), io::ErrorKind::InvalidData);
        let mut oversized = Message::new("matrix_video", "call");
        oversized.data = vec![0; protocol::MAX_PAYLOAD];
        oversized.data[2] = 1;
        assert_eq!(send(&writer, &oversized).await.unwrap_err().kind(), io::ErrorKind::InvalidData);
        let valid = Message::new("hangup", "call");
        send(&writer, &valid).await.unwrap();
        assert_eq!(protocol::read(&mut peer).await.unwrap(), valid);
        drop(writer);
        let mut rest = Vec::new();
        peer.read_to_end(&mut rest).await.unwrap();
        assert!(rest.is_empty());
    }

    #[tokio::test]
    async fn expired_queued_frame_is_skipped_without_poisoning_stream() {
        let (sink, mut peer) = tokio::io::duplex(8192);
        let writer = Writer::new(sink);
        let stale = Message::new("error", "stale");
        let (reply, completion) = oneshot::channel();
        writer.queue.send((protocol::encode_frame(&stale).unwrap(), reply,
            tokio::time::Instant::now())).await.unwrap();
        assert_eq!(completion.await.unwrap().unwrap_err().kind(), io::ErrorKind::TimedOut);
        let fresh = Message::new("hangup", "fresh");
        send(&writer, &fresh).await.unwrap();
        assert_eq!(protocol::read(&mut peer).await.unwrap(), fresh);
    }

    #[tokio::test]
    async fn queue_admission_and_residence_share_one_budget() {
        // Deliberately withhold the writer. Admission can never wait forever,
        // nor can a frame acquire a new deadline when eventually dequeued.
        let (queue, mut receiver) = mpsc::channel::<WriteRequest>(1);
        let writer = Writer { queue };
        let stale_writer = writer.clone();
        let first = tokio::spawn(async move { send(&stale_writer, &Message::new("error", "stale")).await });
        while writer.queue.capacity() != 0 { tokio::task::yield_now().await; }
        let second = send(&writer, &Message::new("hangup", "blocked")).await;
        assert_eq!(second.unwrap_err().kind(), io::ErrorKind::TimedOut);
        assert_eq!(first.await.unwrap().unwrap_err().kind(), io::ErrorKind::TimedOut);
        let (_, reply, deadline) = receiver.recv().await.unwrap();
        assert!(reply.is_closed());
        assert!(tokio::time::Instant::now() >= deadline);
        assert!(receiver.try_recv().is_err());
    }

    #[tokio::test]
    async fn write_timeout_closes_poisoned_stream() {
        let (sink, mut peer) = tokio::io::duplex(64);
        let writer = Writer::new(sink);
        let mut first = Message::new("error", "first");
        first.reason = "x".repeat(1024);
        assert_eq!(send(&writer, &first).await.unwrap_err().kind(), io::ErrorKind::TimedOut);
        assert_eq!(send(&writer, &Message::new("hangup", "second")).await.unwrap_err().kind(), io::ErrorKind::BrokenPipe);
        let mut bytes = Vec::new();
        peer.read_to_end(&mut bytes).await.unwrap();
        assert!(!bytes.is_empty());
    }
}

#[cfg(all(test, feature = "rtc"))]
mod tls_provider_tests {
    #[tokio::test]
    async fn combined_rtc_features_can_build_a_tls_client() {
        super::init_tls_provider().unwrap();
        super::verify_tls_client().unwrap();
    }
}
