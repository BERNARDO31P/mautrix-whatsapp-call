//! WhatsApp <-> MatrixRTC sidecar. Calls remain explicitly opt-in at runtime.
mod audio;
mod protocol;
mod replay;
mod video;
#[cfg(feature = "rtc")]
mod rtc;

use protocol::Message;
use replay::ReplayGuard;
use std::{io, os::unix::fs::FileTypeExt, path::{Path, PathBuf}, sync::Arc};
use tokio::{io::{AsyncWrite, AsyncWriteExt, ReadHalf, WriteHalf}, net::{UnixListener, UnixStream}, sync::{mpsc, oneshot}};

// A media producer may be aborted during mute/unpublish or call teardown.
// It must never own the future that writes a partial wire frame. A single
// bounded writer task completes the frame independently of its producer.
#[derive(Clone)]
struct Writer {
    queue: mpsc::Sender<(Message, oneshot::Sender<io::Result<()>>)>,
}

impl Writer {
    fn new<W: AsyncWrite + Unpin + Send + 'static>(mut sink: W) -> Self {
        let (queue, mut receiver) = mpsc::channel::<(Message, oneshot::Sender<io::Result<()>>)>(8);
        tokio::spawn(async move {
            while let Some((message, reply)) = receiver.recv().await {
                // No bytes have been written for this request yet.
                if reply.is_closed() { continue; }
                let result = protocol::write(&mut sink, &message).await;
                let failed = result.is_err();
                let _ = reply.send(result);
                // A failed write may already have put a length or partial
                // body on the wire. No subsequent frame may use this stream.
                if failed { break; }
            }
            let _ = sink.shutdown().await;
        });
        Self { queue }
    }
}

#[tokio::main]
async fn main() -> io::Result<()> {
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
    let replay = Arc::new(ReplayGuard::open(&state_dir.join("seen-calls"))?);
    #[cfg(feature = "rtc")]
    let accounts = Arc::new(rtc::Accounts::new(state_dir.join("identities")));
    let socket_path = PathBuf::from(&socket);
    let listener = bind_worker_socket(&socket_path).await?;
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
    std::fs::set_permissions(&socket_path, std::fs::Permissions::from_mode(socket_mode))?;
    let _cleanup = SocketCleanup(socket_path.clone());
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
    loop {
        tokio::select! {
            Some(_) = jobs.join_next(), if !jobs.is_empty() => {}
            accepted = listener.accept() => {
                let (stream, _) = accepted?;
                let replay = replay.clone();
                #[cfg(feature = "rtc")]
                let accounts = accounts.clone();
                let raw=stream.into_std()?;
                let closer=raw.try_clone()?;
                let stream=UnixStream::from_std(raw)?;
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
            _ = tokio::signal::ctrl_c() => break,
            _ = terminate.recv() => break,
        }
    }
    drop(listener);
    let _ = shutdown.send(true);
    #[cfg(feature = "rtc")]
    recovery.abort();
    let _ = tokio::time::timeout(std::time::Duration::from_secs(45),async {
        while jobs.join_next().await.is_some() {}
    }).await;
    Ok(())
}

struct SocketCleanup(PathBuf);
impl Drop for SocketCleanup {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.0);
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
                    std::fs::remove_file(path)?;
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
    let (reply, completion) = oneshot::channel();
    writer.queue.send((msg.clone(), reply)).await
        .map_err(|_| io::Error::new(io::ErrorKind::BrokenPipe, "RTC writer stopped"))?;
    completion.await.map_err(|_| io::Error::new(io::ErrorKind::BrokenPipe, "RTC writer stopped"))?
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
        || (incoming.kind=="outgoing" && incoming.expected_device.is_empty()) || incoming.peer.is_empty() || !incoming.data.is_empty()
        || (incoming.group && (!incoming.group_jid.ends_with("@g.us") || incoming.group_jid.len() < 6 || incoming.peer!=incoming.group_jid))
        || (!incoming.group && !incoming.group_jid.is_empty()) {
        return Err(io::Error::new(io::ErrorKind::InvalidData, "invalid first message: incoming/outgoing call with room, slot, membership format, RTC identity, expected human and valid direct/group target required"));
    }
    #[cfg(feature = "rtc")]
    {
        if std::env::var("RTC_WORKER_ENABLE_CALLS").as_deref() != Ok("1") {
            let mut reject = Message::new("reject", &incoming.call_id);
            reject.reason = "RTC calls disabled by configuration".to_owned();
            return send(&writer, &reject).await;
        }
        return rtc::handle(incoming, reader, writer, accounts, replay).await;
    }
    #[cfg(not(feature = "rtc"))]
    {
        let _ = replay;
        let mut reject = Message::new("reject", &incoming.call_id);
        reject.reason = "worker was built without encrypted MatrixRTC media support".to_owned();
        send(&writer, &reject).await
    }
}

#[cfg(all(test, not(feature = "rtc")))]
mod tests {
    use super::*;

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
            assert_eq!(response.kind, "reject");
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
