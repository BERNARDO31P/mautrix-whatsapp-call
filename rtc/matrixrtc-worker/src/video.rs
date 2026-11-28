//! Video conversion between WhatsApp H.264 Annex-B access units and MatrixRTC I420 frames.
//! FFmpeg processes are persistent for the lifetime of a stream; no per-frame process spawning.

use std::io;

const MAX_ENCODED_BUFFER: usize = 8 * 1024 * 1024;

#[derive(Default)]
struct AnnexBAccessUnitParser {
    buffer: Vec<u8>,
}

impl AnnexBAccessUnitParser {
    fn push(&mut self, data: &[u8]) -> io::Result<Vec<Vec<u8>>> {
        self.buffer.extend_from_slice(data);
        if self.buffer.len() > MAX_ENCODED_BUFFER {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "H.264 encoder output exceeded 8 MiB without an access-unit boundary"));
        }
        let mut frames = Vec::new();
        loop {
            let aud = aud_offsets(&self.buffer);
            if aud.len() < 2 {
                break;
            }
            let boundary = aud[1];
            let frame: Vec<u8> = self.buffer.drain(..boundary).collect();
            if !frame.is_empty() {
                frames.push(frame);
            }
        }
        Ok(frames)
    }

    fn finish(&mut self) -> Option<Vec<u8>> {
        if self.buffer.is_empty() {
            None
        } else {
            Some(std::mem::take(&mut self.buffer))
        }
    }
}

fn aud_offsets(data: &[u8]) -> Vec<usize> {
    let mut offsets = Vec::new();
    let mut i = 0usize;
    while i + 4 <= data.len() {
        let start_len = if i + 5 <= data.len() && data[i..i + 4] == [0, 0, 0, 1] {
            4
        } else if data[i..i + 3] == [0, 0, 1] {
            3
        } else {
            i += 1;
            continue;
        };
        if i + start_len < data.len() && data[i + start_len] & 0x1f == 9 {
            offsets.push(i);
        }
        i += start_len;
    }
    offsets
}

#[cfg(feature = "rtc")]
mod rtc {
    use super::AnnexBAccessUnitParser;
    use crate::{protocol::Message, send, Writer};
    use futures_util::StreamExt;
    use matrix_rtc_transport::{FrameStream, I420Buffer, LocalTrackHandle, VideoFrame, VideoRotation};
    use std::{
        env,
        io,
        process::Stdio,
        sync::{
            Arc,
            atomic::{AtomicBool, AtomicU64, Ordering},
        },
        time::Duration,
    };
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt, AsyncBufReadExt, BufReader},
        process::{Child, ChildStdin, Command},
        sync::mpsc,
        task::JoinHandle,
    };

    pub const MATRIX_VIDEO_WIDTH: u32 = 640;
    pub const MATRIX_VIDEO_HEIGHT: u32 = 480;
    const DEFAULT_FRAME_DURATION_US: u64 = 33_333;
    const MAX_FRAME_DURATION_US: u64 = 1_000_000;
    const ENCODER_SHUTDOWN_BUDGET: Duration = Duration::from_secs(2);
    const ENCODER_FRAME_BUDGET: Duration = Duration::from_millis(250);
    const DECODER_FRAME_BUDGET: Duration = Duration::from_millis(250);
    // Keep queued video bounded independently of the per-write timeout. A
    // stale H.264 access unit cannot safely be skipped within a prediction
    // chain, so restart the decoder and request an IDR instead of replaying it.
    const VIDEO_QUEUE_FRAMES: usize = 8;
    const DECODER_QUEUE_AGE: Duration = Duration::from_millis(250);
    const ENCODER_QUEUE_AGE: Duration = Duration::from_millis(250);
    const ENCODER_RECOVERY_FRAMES: u32 = 450;

    // This is local pipeline backpressure, not a bandwidth estimator. Reduce
    // the encoded workload on a submission timeout and restore one level only
    // after a sustained run of fast submissions. Each change starts a new GOP.
    #[derive(Default)]
    struct EncoderBudget {
        level: usize,
        fast_frames: u32,
    }

    impl EncoderBudget {
        fn rates(&self) -> [&'static str; 3] {
            [["400k", "500k", "250k"], ["250k", "320k", "160k"], ["150k", "200k", "100k"]][self.level]
        }

        fn stalled(&mut self) {
            self.fast_frames = 0;
            self.level = (self.level + 1).min(2);
        }

        fn submitted(&mut self, elapsed: Duration) -> bool {
            if elapsed > Duration::from_millis(33) || self.level == 0 {
                self.fast_frames = 0;
                return false;
            }
            self.fast_frames += 1;
            if self.fast_frames >= ENCODER_RECOVERY_FRAMES {
                self.fast_frames = 0;
                self.level -= 1;
                return true;
            }
            false
        }
    }

    fn decoder_frame_expired(queued_at: tokio::time::Instant, now: tokio::time::Instant) -> bool {
        now.saturating_duration_since(queued_at) >= DECODER_QUEUE_AGE
    }

    fn video_rotation(orientation: u8) -> io::Result<VideoRotation> {
        match orientation {
            0 => Ok(VideoRotation::Deg0),
            1 => Ok(VideoRotation::Deg90),
            2 => Ok(VideoRotation::Deg180),
            3 => Ok(VideoRotation::Deg270),
            _ => Err(io::Error::new(io::ErrorKind::InvalidData, "video orientation must be 0..3")),
        }
    }

    fn quarter_turns(rotation: VideoRotation) -> u8 {
        match rotation {
            VideoRotation::Deg0 => 0,
            VideoRotation::Deg90 => 1,
            VideoRotation::Deg180 => 2,
            VideoRotation::Deg270 => 3,
        }
    }

    fn ffmpeg_binary() -> String {
        env::var("RTC_WORKER_FFMPEG").unwrap_or_else(|_| "ffmpeg".to_owned())
    }

    // The worker image and its FFmpeg executable are fixed for the process
    // lifetime. Do not launch another encoder probe in the media event loop
    // on each camera start, decoder resync or track replacement. Failed or
    // cancelled initialization is not cached by OnceCell.
    static FFMPEG_VERIFIED: tokio::sync::OnceCell<()> = tokio::sync::OnceCell::const_new();

    pub async fn verify_ffmpeg() -> io::Result<()> {
        FFMPEG_VERIFIED.get_or_try_init(probe_ffmpeg).await.map(|_| ())
    }

    async fn probe_ffmpeg() -> io::Result<()> {
        // A version check alone is insufficient: some minimal FFmpeg builds do
        // not contain libx264, which would make Matrix -> WhatsApp video fail
        // only after the remote party answered. Encode one tiny synthetic frame
        // up front so video calls fail closed before either side is connected.
        let status = tokio::time::timeout(
            Duration::from_secs(8),
            Command::new(ffmpeg_binary())
                // The setup deadline can cancel this probe before its own timeout.
                .kill_on_drop(true)
                .args([
                    "-v", "error",
                    "-f", "lavfi",
                    "-i", "color=c=black:s=16x16:r=1",
                    "-frames:v", "1",
                    "-an",
                    "-c:v", "libx264",
                    "-preset", "ultrafast",
                    "-tune", "zerolatency",
                    "-f", "h264",
                    "pipe:1",
                ])
                .stdin(Stdio::null())
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .status(),
        )
        .await
        .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "ffmpeg H.264 capability check timed out"))??;
        if !status.success() {
            return Err(io::Error::other(
                "ffmpeg with the libx264 H.264 encoder is required for WhatsApp video",
            ));
        }
        Ok(())
    }

    // A PTS stays attached to its decoded picture across reordering or discarded inputs.
    // The two least significant 90 kHz ticks carry quarter turns (maximum quantisation 34 us).
    fn pes(access_unit:&[u8],pts:u64)->Vec<u8> {
        let mut bytes=Vec::with_capacity(access_unit.len()+32);
        for (i,chunk) in access_unit.chunks(65_520).enumerate() {
            bytes.extend_from_slice(&[0,0,1,0xe0]);
            let len=chunk.len()+if i==0{8}else{3};
            bytes.extend_from_slice(&(len as u16).to_be_bytes());
            if i==0 {
                bytes.extend_from_slice(&[0x80,0x80,5,0x21|((pts>>29) as u8&14),(pts>>22) as u8,1|((pts>>14) as u8&254),(pts>>7) as u8,1|((pts<<1) as u8&254)]);
            }else{bytes.extend_from_slice(&[0x80,0,0]);}
            bytes.extend_from_slice(chunk);
        }
        bytes
    }

    pub struct WhatsAppVideoDecoder {
        pub generation:u64,
        input: mpsc::Sender<(Vec<u8>, u64, tokio::time::Instant)>,
        child: Child,
        tasks: Vec<JoinHandle<()>>,
    }

    impl WhatsAppVideoDecoder {
        pub fn spawn(track: Arc<dyn LocalTrackHandle>) -> io::Result<Self> {
            let mut child = Command::new(ffmpeg_binary())
                .args(["-hide_banner","-nostats","-nostdin","-copyts","-analyzeduration","0","-probesize","32","-threads","1","-flags","low_delay"])
                .args(["-f", "mpeg","-c:v","h264", "-i", "pipe:0", "-an"])
                .args([
                    "-vf",
                    "scale=640:480:force_original_aspect_ratio=decrease,pad=640:480:(ow-iw)/2:(oh-ih)/2:color=black,settb=1/90000,showinfo",
                    "-vsync","0","-threads","1",
                    "-pix_fmt",
                    "yuv420p",
                    "-f",
                    "rawvideo",
                    "pipe:1",
                ])
                .stdin(Stdio::piped())
                .stdout(Stdio::piped())
                .stderr(Stdio::piped())
                .kill_on_drop(true)
                .spawn()?;
            let mut stdin = child.stdin.take().ok_or_else(|| io::Error::other("ffmpeg decoder stdin unavailable"))?;
            let mut stdout = child.stdout.take().ok_or_else(|| io::Error::other("ffmpeg decoder stdout unavailable"))?;
            let stderr=child.stderr.take().ok_or_else(||io::Error::other("decoder diagnostics unavailable"))?;
            let (input,mut rx)=mpsc::channel::<(Vec<u8>, u64, tokio::time::Instant)>(VIDEO_QUEUE_FRAMES);
            let (timestamps,mut decoded_pts)=mpsc::channel::<u64>(VIDEO_QUEUE_FRAMES);
            let timing_task=tokio::spawn(async move{
                let mut lines=BufReader::new(stderr).lines();
                while let Ok(Some(line))=lines.next_line().await {
                    if !line.contains("Parsed_showinfo") {continue}
                    if let Some(pts)=line.split_once(" pts:").and_then(|(_,tail)|tail.split_whitespace().next()).and_then(|n|n.parse::<u64>().ok()){
                        if timestamps.send(pts).await.is_err(){break}
                    }
                }
            });
            let writer_task=tokio::spawn(async move{
                while let Some((au, pts, queued_at)) = rx.recv().await {
                    if decoder_frame_expired(queued_at, tokio::time::Instant::now()) {
                        break;
                    }
                    // A partial PES write cannot be retried safely. Stop this
                    // decoder and let the caller recover at a fresh keyframe.
                    if !matches!(tokio::time::timeout(DECODER_FRAME_BUDGET, stdin.write_all(&pes(&au,pts))).await, Ok(Ok(()))) {
                        break;
                    }
                }
            });
            let reader_task = tokio::spawn(async move {
                let y = (MATRIX_VIDEO_WIDTH * MATRIX_VIDEO_HEIGHT) as usize;
                let uv = y / 4;
                let mut raw = vec![0u8; y + uv * 2];

                loop {
                    match stdout.read_exact(&mut raw).await {
                        Ok(_) => {
                            let Some(pts)=decoded_pts.recv().await else {break};
                            let frame = VideoFrame {
                                buffer: I420Buffer {
                                    width: MATRIX_VIDEO_WIDTH,
                                    height: MATRIX_VIDEO_HEIGHT,
                                    data_y: raw[..y].to_vec(),
                                    stride_y: MATRIX_VIDEO_WIDTH,
                                    data_u: raw[y..y + uv].to_vec(),
                                    stride_u: MATRIX_VIDEO_WIDTH / 2,
                                    data_v: raw[y + uv..].to_vec(),
                                    stride_v: MATRIX_VIDEO_WIDTH / 2,
                                },
                                rotation:video_rotation((pts&3) as u8).unwrap(),
                                timestamp_us:((pts & !3)*100/9) as i64,
                            };
                            if track.capture_video(frame).is_err() {
                                break;
                            }
                        }
                        Err(error) if error.kind() == io::ErrorKind::UnexpectedEof => break,
                        Err(_) => break,
                    }
                }
            });
            Ok(Self { generation:0,input, child, tasks: vec![writer_task, reader_task,timing_task] })
        }

        pub fn push(&self,access_unit:Vec<u8>,orientation:u8,timestamp:u64)->io::Result<bool>{
            video_rotation(orientation)?;
            match self.input.try_send((access_unit, (timestamp & !3) | u64::from(orientation), tokio::time::Instant::now())){
                Ok(())=>Ok(true),
                Err(mpsc::error::TrySendError::Full(_))=>Ok(false),
                Err(mpsc::error::TrySendError::Closed(_))=>Ok(false)
            }
        }

        pub fn ensure_running(&mut self) -> io::Result<()> {
            match self.child.try_wait()? {
                None if self.tasks.iter().all(|task| !task.is_finished()) => Ok(()),
                None => Err(io::Error::other("H.264 decoder task stopped unexpectedly")),
                Some(status) => Err(io::Error::other(format!(
                    "H.264 decoder exited before call media was ready: {status}"
                ))),
            }
        }
    }

    impl Drop for WhatsAppVideoDecoder {
        fn drop(&mut self) {
            let _ = self.child.start_kill();
            for task in &self.tasks {
                task.abort();
            }
        }
    }

    #[derive(Clone, Copy)]
    struct FrameMetadata {
        duration_us: u64,
        orientation: u8,
        submitted_at: tokio::time::Instant,
    }

    fn encoder_frame_expired(meta: &FrameMetadata, now: tokio::time::Instant) -> bool {
        now.saturating_duration_since(meta.submitted_at) >= ENCODER_QUEUE_AGE
    }

    // Dropping a JoinHandle detaches its task. Encoder cancellation must
    // instead stop its stdout reader so it cannot emit stale video frames.
    struct EncoderOutput(JoinHandle<io::Result<()>>);
    impl Drop for EncoderOutput {
        fn drop(&mut self) { self.0.abort(); }
    }

    async fn finish_encoder_output(output: &mut EncoderOutput, deadline: tokio::time::Instant) -> io::Result<()> {
        match tokio::time::timeout_at(deadline, &mut output.0).await {
            Ok(result) => result.map_err(io::Error::other)?,
            Err(_) => {
                output.0.abort();
                Err(io::Error::new(io::ErrorKind::TimedOut, "H.264 encoder output shutdown timed out"))
            }
        }
    }

    // A stalled raw-frame write can partially fill FFmpeg's stdin. The caller
    // must discard that encoder on timeout, not append another raw frame.
    async fn submit_encoder_frame(write: impl std::future::Future<Output = io::Result<()>>, budget: Duration) -> io::Result<()> {
        tokio::time::timeout(budget, write).await.map_err(|_| {
            io::Error::new(io::ErrorKind::TimedOut, "H.264 encoder frame submission timed out")
        })?
    }

    struct EncoderProcess {
        width: u32,
        height: u32,
        stdin: ChildStdin,
        child: Child,
        metadata: mpsc::Sender<FrameMetadata>,
        output_task: EncoderOutput,
        stale_output: Arc<AtomicBool>,
    }

    impl EncoderProcess {
        fn spawn(
            width: u32,
            height: u32,
            writer: Writer,
            call_id: String,
            active: Arc<AtomicBool>,
            source_generation: u64,
            rates: [&str; 3],
        ) -> io::Result<Self> {
            if width == 0 || height == 0 || width % 2 != 0 || height % 2 != 0 {
                return Err(io::Error::new(io::ErrorKind::InvalidData, "Matrix video dimensions must be non-zero and even"));
            }
            let size = format!("{width}x{height}");
            let mut child = Command::new(ffmpeg_binary())
                .args(["-v", "error", "-nostdin"])
                .args(["-f", "rawvideo", "-pix_fmt", "yuv420p", "-video_size", &size, "-framerate", "30", "-i", "pipe:0"])
                .args([
                    "-an",
                    "-c:v",
                    "libx264",
                    "-preset",
                    "ultrafast",
                    "-tune",
                    "zerolatency",
                    "-profile:v",
                    "baseline",
                    // Bound transcoding CPU and video bursts on the shared audio/video
                    // transport. Keep the camera aspect ratio and never upscale.
                    "-vf",
                    "scale=w=min(640\\,iw):h=min(640\\,ih):force_original_aspect_ratio=decrease:force_divisible_by=2",
                    "-filter_threads",
                    "1",
                    "-threads",
                    "2",
                    "-b:v",
                    rates[0],
                    "-maxrate",
                    rates[1],
                    "-bufsize",
                    rates[2],
                    "-pix_fmt",
                    "yuv420p",
                    "-x264-params",
                    "aud=1:repeat-headers=1:keyint=30:min-keyint=30:scenecut=0:bframes=0",
                    "-f",
                    "h264",
                    "pipe:1",
                ])
                .stdin(Stdio::piped())
                .stdout(Stdio::piped())
                .stderr(Stdio::inherit())
                .kill_on_drop(true)
                .spawn()?;
            let stdin = child.stdin.take().ok_or_else(|| io::Error::other("ffmpeg encoder stdin unavailable"))?;
            let mut stdout = child.stdout.take().ok_or_else(|| io::Error::other("ffmpeg encoder stdout unavailable"))?;
            let (metadata, mut metadata_rx) = mpsc::channel::<FrameMetadata>(VIDEO_QUEUE_FRAMES);
            let stale_output = Arc::new(AtomicBool::new(false));
            let expired = stale_output.clone();
            let output_task = tokio::spawn(async move {
                let mut parser = AnnexBAccessUnitParser::default();
                let mut buf = vec![0u8; 64 * 1024];
                loop {
                    let read = stdout.read(&mut buf).await?;
                    if read == 0 {
                        break;
                    }
                    for access_unit in parser.push(&buf[..read])? {
                        let frame_meta = metadata_rx.recv().await.ok_or_else(|| io::Error::other("H.264 encoder output missing frame metadata"))?;
                        if encoder_frame_expired(&frame_meta, tokio::time::Instant::now()) {
                            expired.store(true, Ordering::Release);
                            return Ok(());
                        }
                        if active.load(Ordering::Acquire) {
                            let mut message = Message::new("matrix_video", &call_id);
                        message.generation = source_generation;
                            message.duration_us = frame_meta.duration_us;
                            message.orientation = frame_meta.orientation;
                            message.data = access_unit;
                            send(&writer, &message).await?;
                        }
                    }
                }
                if let Some(access_unit) = parser.finish() {
                    let frame_meta = metadata_rx.recv().await.ok_or_else(|| io::Error::other("H.264 encoder output missing frame metadata"))?;
                    if encoder_frame_expired(&frame_meta, tokio::time::Instant::now()) {
                        expired.store(true, Ordering::Release);
                        return Ok(());
                    }
                    if active.load(Ordering::Acquire) {
                        let mut message = Message::new("matrix_video", &call_id);
                        message.generation = source_generation;
                        message.duration_us = frame_meta.duration_us;
                        message.orientation = frame_meta.orientation;
                        message.data = access_unit;
                        send(&writer, &message).await?;
                    }
                }
                Ok(())
            });
            Ok(Self { width, height, stdin, child, metadata, output_task: EncoderOutput(output_task), stale_output })
        }

        async fn write_frame(&mut self, frame: &VideoFrame, duration_us: u64) -> io::Result<()> {
            if frame.buffer.width != self.width || frame.buffer.height != self.height {
                return Err(io::Error::new(io::ErrorKind::InvalidInput, "video dimensions changed"));
            }
            let submitted_at = tokio::time::Instant::now();
            let raw = pack_i420(&frame.buffer)?;
            self.stdin.write_all(&raw).await?;
            self.metadata
                .send(FrameMetadata {
                    duration_us: duration_us.clamp(1, MAX_FRAME_DURATION_US),
                    orientation: quarter_turns(frame.rotation),
                    submitted_at,
                })
                .await
                .map_err(|_| io::Error::new(io::ErrorKind::BrokenPipe, "H.264 encoder output stopped"))
        }

        async fn shutdown(mut self) -> io::Result<()> {
            drop(self.stdin);
            drop(self.metadata);
            // One budget covers the child and its output task. The output task
            // can still be draining multiple bounded IPC writes after FFmpeg exits.
            let deadline = tokio::time::Instant::now() + ENCODER_SHUTDOWN_BUDGET;
            let status = match tokio::time::timeout_at(deadline, self.child.wait()).await {
                Ok(result) => result?,
                Err(_) => {
                    let _ = self.child.start_kill();
                    return Err(io::Error::new(io::ErrorKind::TimedOut, "H.264 encoder shutdown timed out"));
                }
            };
            finish_encoder_output(&mut self.output_task, deadline).await?;
            if !status.success() {
                return Err(io::Error::other(format!("H.264 encoder exited with {status}")));
            }
            Ok(())
        }
    }

    pub async fn matrix_video_to_whatsapp(
        mut frames: FrameStream<'static, VideoFrame>,
        writer: Writer,
        call_id: String,
        active: Arc<AtomicBool>,
        keyframe_generation: Arc<AtomicU64>,
        source_generation: u64,
    ) -> io::Result<()> {
        let mut encoder: Option<EncoderProcess> = None;
        let mut budget = EncoderBudget::default();
        let mut seen_keyframe_generation = keyframe_generation.load(Ordering::Acquire);
        let mut last_timestamp_us: Option<i64> = None;
        while let Some(frame) = frames.next().await {
            if !active.load(Ordering::Acquire) {
                // The remote decoder cannot reconstruct a prediction chain
                // whose pictures were suppressed during media suspension.
                // Do not retain that encoder or its queued output on resume.
                drop(encoder.take());
                budget.fast_frames = 0;
                last_timestamp_us = None;
                continue;
            }
            if encoder.as_ref().is_some_and(|current| current.stale_output.load(Ordering::Acquire)) {
                budget.stalled();
                drop(encoder.take());
                last_timestamp_us = None;
            }
            let requested_generation = keyframe_generation.load(Ordering::Acquire);
            let restart = encoder.as_ref().is_some_and(|current| {
                current.width != frame.buffer.width
                    || current.height != frame.buffer.height
                    || requested_generation != seen_keyframe_generation
            });
            if restart {
                // A resync or resolution change invalidates the old GOP. Do not
                // drain stale encoded pictures before producing the requested IDR.
                // Child kill-on-drop and EncoderOutput abort stop the old producer;
                // any already admitted IPC frame retains its own writer deadline.
                drop(encoder.take());
                seen_keyframe_generation = requested_generation;
                last_timestamp_us = None;
            }
            if encoder.is_none() {
                match EncoderProcess::spawn(
                    frame.buffer.width,
                    frame.buffer.height,
                    writer.clone(),
                    call_id.clone(),
                    active.clone(),
                    source_generation,
                    budget.rates(),
                ) {
                    Ok(created) => {
                        encoder = Some(created);
                        // A fresh encoder already starts with an IDR. Consume
                        // the request observed before spawning, including when
                        // recovering from an earlier timeout or suspension.
                        seen_keyframe_generation = requested_generation;
                    },
                    Err(error) => return Err(error),
                }
            }
            let duration_us = last_timestamp_us
                .and_then(|previous| frame.timestamp_us.checked_sub(previous))
                .and_then(|delta| u64::try_from(delta).ok())
                .filter(|delta| *delta > 0 && *delta <= MAX_FRAME_DURATION_US)
                .unwrap_or(DEFAULT_FRAME_DURATION_US);
            last_timestamp_us = Some(frame.timestamp_us);
            if let Some(current) = encoder.as_mut() {
                let started = std::time::Instant::now();
                match submit_encoder_frame(current.write_frame(&frame, duration_us), ENCODER_FRAME_BUDGET).await {
                    Ok(()) => {
                        if budget.submitted(started.elapsed()) {
                            eprintln!("RTC video encoder local workload recovery: target {}", budget.rates()[0]);
                            drop(encoder.take());
                            last_timestamp_us = None;
                        }
                    },
                    Err(err) if err.kind() == io::ErrorKind::TimedOut || current.stale_output.load(Ordering::Acquire) => {
                        eprintln!("RTC video encoder stalled; discarding partial frame and restarting: {err}");
                        // A cancelled write_all is not frame-atomic. Dropping
                        // the child and output task prevents reuse of its pipe.
                        budget.stalled();
                        eprintln!("RTC video encoder local workload fallback: target {}", budget.rates()[0]);
                        drop(encoder.take());
                        last_timestamp_us = None;
                    },
                    Err(err) => return Err(err),
                }
            }
        }
        if let Some(current) = encoder {
            current.shutdown().await?;
        }
        Ok(())
    }

    fn pack_i420(buffer: &I420Buffer) -> io::Result<Vec<u8>> {
        let width = buffer.width as usize;
        let height = buffer.height as usize;
        let chroma_width = width.div_ceil(2);
        let chroma_height = height.div_ceil(2);
        let stride_y = buffer.stride_y as usize;
        let stride_u = buffer.stride_u as usize;
        let stride_v = buffer.stride_v as usize;
        if stride_y < width || stride_u < chroma_width || stride_v < chroma_width
            || buffer.data_y.len() < stride_y.saturating_mul(height)
            || buffer.data_u.len() < stride_u.saturating_mul(chroma_height)
            || buffer.data_v.len() < stride_v.saturating_mul(chroma_height)
        {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "invalid I420 plane dimensions or strides"));
        }
        let mut raw = Vec::with_capacity(width * height + chroma_width * chroma_height * 2);
        for row in 0..height {
            raw.extend_from_slice(&buffer.data_y[row * stride_y..row * stride_y + width]);
        }
        for row in 0..chroma_height {
            raw.extend_from_slice(&buffer.data_u[row * stride_u..row * stride_u + chroma_width]);
        }
        for row in 0..chroma_height {
            raw.extend_from_slice(&buffer.data_v[row * stride_v..row * stride_v + chroma_width]);
        }
        Ok(raw)
    }


    #[cfg(test)]
    mod orientation_tests {
        use super::*;

        #[test]
        fn h264_decoder_retains_pts_for_each_emitted_picture() {
            use std::io::Write;
            let encoded=std::process::Command::new(ffmpeg_binary()).args([
                "-v","error","-f","lavfi","-i","testsrc2=s=32x24:r=10","-frames:v","5","-c:v","libx264","-threads","1",
                "-x264-params","aud=1:repeat-headers=1:bframes=0","-f","h264","pipe:1"
            ]).output().unwrap();
            assert!(encoded.status.success());
            let mut offsets=super::super::aud_offsets(&encoded.stdout);
            offsets.push(encoded.stdout.len());
            let mut stream=Vec::new();
            let mut expected=Vec::new();
            for (i,pair) in offsets.windows(2).enumerate(){
                let pts=90_000+i as u64*9000+i as u64%4;
                expected.push(pts);
                stream.extend_from_slice(&pes(&encoded.stdout[pair[0]..pair[1]],pts));
            }
            let mut ffmpeg=std::process::Command::new(ffmpeg_binary()).args([
                "-hide_banner","-nostats","-copyts","-analyzeduration","0","-probesize","32","-threads","1","-f","mpeg",
                "-c:v","h264","-i","pipe:0","-vf","settb=1/90000,showinfo","-vsync","0","-f","null","-"
            ]).stdin(Stdio::piped()).stdout(Stdio::null()).stderr(Stdio::piped()).spawn().unwrap();
            ffmpeg.stdin.take().unwrap().write_all(&stream).unwrap();
            let out=ffmpeg.wait_with_output().unwrap();
            assert!(out.status.success());
            let pts:Vec<u64>=String::from_utf8_lossy(&out.stderr).lines().filter(|s|s.contains("Parsed_showinfo")).filter_map(|line|
                line.split_once(" pts:")?.1.split_whitespace().next()?.parse().ok()).collect();
            assert_eq!(pts.len(),5);
            assert_eq!(pts,expected);
        }

        #[tokio::test]
        async fn encoder_releases_stdin_on_size_change_and_keyframe_restart() {
            let (worker,_peer)=tokio::net::UnixStream::pair().unwrap();
            let (_,writer)=tokio::io::split(worker);
            let writer=Writer::new(writer);
            for size in [16u32,32,32] {
                let mut process=EncoderProcess::spawn(size,size,writer.clone(),"test".into(),Arc::new(AtomicBool::new(true)),1,EncoderBudget::default().rates()).unwrap();
                let n=(size*size) as usize;
                let frame=VideoFrame{buffer:I420Buffer{width:size,height:size,data_y:vec![16;n],data_u:vec![128;n/4],data_v:vec![128;n/4],stride_y:size,stride_u:size/2,stride_v:size/2},rotation:VideoRotation::Deg0,timestamp_us:100000};
                process.write_frame(&frame,33_333).await.unwrap();
                process.shutdown().await.unwrap();
            }
        }

        #[tokio::test]
        async fn ffmpeg_probe_failure_can_retry_and_success_is_reused() {
            let verified = tokio::sync::OnceCell::<()>::new();
            let failed = verified.get_or_try_init(|| async {
                Err::<(), _>(io::Error::other("probe failed"))
            }).await;
            assert!(failed.is_err());
            assert!(verified.get().is_none());
            verified.get_or_try_init(|| async { Ok::<(), io::Error>(()) }).await.unwrap();
            verified.get_or_try_init(|| async {
                panic!("a successful probe must not be repeated");
                #[allow(unreachable_code)]
                Ok::<(), io::Error>(())
            }).await.unwrap();
        }

        #[test]
        fn encoder_budget_is_bounded_and_recovers_gradually() {
            let mut budget = EncoderBudget::default();
            assert_eq!(budget.rates()[0], "400k");
            budget.stalled();
            assert_eq!(budget.rates()[0], "250k");
            budget.stalled();
            budget.stalled();
            assert_eq!(budget.rates()[0], "150k");
            for _ in 0..ENCODER_RECOVERY_FRAMES - 1 {
                assert!(!budget.submitted(Duration::from_millis(10)));
            }
            assert!(!budget.submitted(Duration::from_millis(50)));
            for expected in ["250k", "400k"] {
                for _ in 0..ENCODER_RECOVERY_FRAMES - 1 {
                    assert!(!budget.submitted(Duration::from_millis(10)));
                }
                assert!(budget.submitted(Duration::from_millis(10)));
                assert_eq!(budget.rates()[0], expected);
            }
            assert!(!budget.submitted(Duration::from_millis(10)));
        }

        #[test]
        fn queued_decoder_frames_expire_at_latency_budget() {
            let start = tokio::time::Instant::now();
            assert!(!decoder_frame_expired(start, start));
            assert!(!decoder_frame_expired(start, start + DECODER_QUEUE_AGE - Duration::from_millis(1)));
            assert!(decoder_frame_expired(start, start + DECODER_QUEUE_AGE));
            assert!(decoder_frame_expired(start, start + Duration::from_secs(1)));
        }

        #[test]
        fn queued_encoder_output_expires_at_latency_budget() {
            let start = tokio::time::Instant::now();
            let meta = FrameMetadata { duration_us: 33_333, orientation: 0, submitted_at: start };
            assert!(!encoder_frame_expired(&meta, start));
            assert!(!encoder_frame_expired(&meta, start + ENCODER_QUEUE_AGE - Duration::from_millis(1)));
            assert!(encoder_frame_expired(&meta, start + ENCODER_QUEUE_AGE));
        }

        #[tokio::test]
        async fn encoder_frame_submission_has_a_deadline() {
            let stalled = submit_encoder_frame(std::future::pending(), Duration::from_millis(10)).await;
            assert_eq!(stalled.unwrap_err().kind(), io::ErrorKind::TimedOut);
            submit_encoder_frame(async { Ok(()) }, Duration::from_secs(1)).await.unwrap();
            let broken = submit_encoder_frame(async { Err(io::Error::from(io::ErrorKind::BrokenPipe)) }, Duration::from_secs(1)).await;
            assert_eq!(broken.unwrap_err().kind(), io::ErrorKind::BrokenPipe);
        }

        #[tokio::test]
        async fn encoder_output_shutdown_has_a_shared_deadline() {
            let mut output = EncoderOutput(tokio::spawn(std::future::pending::<io::Result<()>>()));
            let deadline = tokio::time::Instant::now() + Duration::from_millis(10);
            let err = finish_encoder_output(&mut output, deadline).await.unwrap_err();
            assert_eq!(err.kind(), io::ErrorKind::TimedOut);
            assert!((&mut output.0).await.unwrap_err().is_cancelled());
            let mut output = EncoderOutput(tokio::spawn(async { Ok(()) }));
            finish_encoder_output(&mut output, tokio::time::Instant::now() + Duration::from_secs(1)).await.unwrap();
        }

        #[test]
        fn video_rotation_round_trips_quarter_turns() {
            for (orientation, rotation) in [
                (0, VideoRotation::Deg0),
                (1, VideoRotation::Deg90),
                (2, VideoRotation::Deg180),
                (3, VideoRotation::Deg270),
            ] {
                assert_eq!(video_rotation(orientation).unwrap(), rotation);
                assert_eq!(quarter_turns(rotation), orientation);
            }
            assert!(video_rotation(4).is_err());
        }
    }
}

#[cfg(feature = "rtc")]
pub use rtc::*;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn annex_b_parser_emits_one_access_unit_per_aud() {
        let first = [0, 0, 0, 1, 7, 1, 2, 0, 0, 0, 1, 9, 0xf0, 0, 0, 1, 5, 1, 2, 3];
        let second = [0, 0, 0, 1, 9, 0xf0, 0, 0, 1, 1, 4, 5, 6];
        let third = [0, 0, 1, 9, 0xf0, 0, 0, 1, 1, 7, 8, 9];
        let mut parser = AnnexBAccessUnitParser::default();
        assert!(parser.push(&first).unwrap().is_empty());
        let frames = parser.push(&second).unwrap();
        assert_eq!(frames.len(), 1);
        assert!(frames[0].starts_with(&[0, 0, 0, 1, 7]));
        let frames = parser.push(&third).unwrap();
        assert_eq!(frames.len(), 1);
        assert!(frames[0].windows(5).any(|part| part == [0, 0, 0, 1, 9]));
        assert!(parser.finish().is_some());
    }

    #[test]
    fn aud_offsets_support_three_and_four_byte_start_codes_without_duplicates() {
        let data = [0, 0, 0, 1, 9, 1, 0, 0, 1, 9, 2, 0, 0, 0, 1, 1, 3];
        assert_eq!(aud_offsets(&data), vec![0, 6]);
    }
}
