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

    pub async fn verify_ffmpeg() -> io::Result<()> {
        // A version check alone is insufficient: some minimal FFmpeg builds do
        // not contain libx264, which would make Matrix -> WhatsApp video fail
        // only after the remote party answered. Encode one tiny synthetic frame
        // up front so video calls fail closed before either side is connected.
        let status = tokio::time::timeout(
            Duration::from_secs(8),
            Command::new(ffmpeg_binary())
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
        input: mpsc::Sender<(Vec<u8>, u64)>,
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
            let (input,mut rx)=mpsc::channel::<(Vec<u8>,u64)>(24);
            let (timestamps,mut decoded_pts)=mpsc::channel::<u64>(32);
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
                while let Some((au,pts))=rx.recv().await {if stdin.write_all(&pes(&au,pts)).await.is_err(){break}}
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
            match self.input.try_send((access_unit,(timestamp & !3)|u64::from(orientation))){
                Ok(())=>Ok(true),
                Err(mpsc::error::TrySendError::Full(_))=>Ok(false),
                Err(mpsc::error::TrySendError::Closed(_))=>Err(io::Error::new(io::ErrorKind::BrokenPipe,"H.264 decoder stopped"))
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
    }

    struct EncoderProcess {
        width: u32,
        height: u32,
        stdin: ChildStdin,
        child: Child,
        metadata: mpsc::Sender<FrameMetadata>,
        output_task: JoinHandle<io::Result<()>>,
    }

    impl EncoderProcess {
        fn spawn(
            width: u32,
            height: u32,
            writer: Writer,
            call_id: String,
            active: Arc<AtomicBool>,
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
            let (metadata, mut metadata_rx) = mpsc::channel::<FrameMetadata>(120);
            let output_task = tokio::spawn(async move {
                let mut parser = AnnexBAccessUnitParser::default();
                let mut buf = vec![0u8; 64 * 1024];
                loop {
                    let read = stdout.read(&mut buf).await?;
                    if read == 0 {
                        break;
                    }
                    for access_unit in parser.push(&buf[..read])? {
                        let frame_meta = metadata_rx.recv().await.unwrap_or(FrameMetadata {
                            duration_us: DEFAULT_FRAME_DURATION_US,
                            orientation: 0,
                        });
                        if active.load(Ordering::Acquire) {
                            let mut message = Message::new("matrix_video", &call_id);
                            message.duration_us = frame_meta.duration_us;
                            message.orientation = frame_meta.orientation;
                            message.data = access_unit;
                            send(&writer, &message).await?;
                        }
                    }
                }
                if let Some(access_unit) = parser.finish() {
                    let frame_meta = metadata_rx.recv().await.unwrap_or(FrameMetadata {
                        duration_us: DEFAULT_FRAME_DURATION_US,
                        orientation: 0,
                    });
                    if active.load(Ordering::Acquire) {
                        let mut message = Message::new("matrix_video", &call_id);
                        message.duration_us = frame_meta.duration_us;
                        message.orientation = frame_meta.orientation;
                        message.data = access_unit;
                        send(&writer, &message).await?;
                    }
                }
                Ok(())
            });
            Ok(Self { width, height, stdin, child, metadata, output_task })
        }

        async fn write_frame(&mut self, frame: &VideoFrame, duration_us: u64) -> io::Result<()> {
            if frame.buffer.width != self.width || frame.buffer.height != self.height {
                return Err(io::Error::new(io::ErrorKind::InvalidInput, "video dimensions changed"));
            }
            let raw = pack_i420(&frame.buffer)?;
            self.stdin.write_all(&raw).await?;
            self.metadata
                .send(FrameMetadata {
                    duration_us: duration_us.clamp(1, MAX_FRAME_DURATION_US),
                    orientation: quarter_turns(frame.rotation),
                })
                .await
                .map_err(|_| io::Error::new(io::ErrorKind::BrokenPipe, "H.264 encoder output stopped"))
        }

        async fn shutdown(mut self) -> io::Result<()> {
            drop(self.stdin);
            drop(self.metadata);
            let (status, forced_shutdown) = match tokio::time::timeout(Duration::from_secs(2), self.child.wait()).await {
                Ok(result) => (result?, false),
                Err(_) => {
                    let _ = self.child.start_kill();
                    (self.child.wait().await?, true)
                }
            };
            let output_result = self.output_task.await.map_err(io::Error::other)?;
            output_result?;
            if !status.success() && !forced_shutdown {
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
    ) -> io::Result<()> {
        let mut encoder: Option<EncoderProcess> = None;
        let mut seen_keyframe_generation = keyframe_generation.load(Ordering::Acquire);
        let mut last_timestamp_us: Option<i64> = None;
        while let Some(frame) = frames.next().await {
            if !active.load(Ordering::Acquire) {
                last_timestamp_us = None;
                continue;
            }
            let requested_generation = keyframe_generation.load(Ordering::Acquire);
            let restart = encoder.as_ref().is_some_and(|current| {
                current.width != frame.buffer.width
                    || current.height != frame.buffer.height
                    || requested_generation != seen_keyframe_generation
            });
            if restart {
                if let Some(current) = encoder.take() {
                    current.shutdown().await?;
                }
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
                ) {
                    Ok(created) => encoder = Some(created),
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
                current.write_frame(&frame, duration_us).await?;
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
            let writer=Arc::new(tokio::sync::Mutex::new(writer));
            for size in [16u32,32,32] {
                let mut process=EncoderProcess::spawn(size,size,writer.clone(),"test".into(),Arc::new(AtomicBool::new(true))).unwrap();
                let n=(size*size) as usize;
                let frame=VideoFrame{buffer:I420Buffer{width:size,height:size,data_y:vec![16;n],data_u:vec![128;n/4],data_v:vec![128;n/4],stride_y:size,stride_u:size/2,stride_v:size/2},rotation:VideoRotation::Deg0,timestamp_us:100000};
                process.write_frame(&frame,33_333).await.unwrap();
                process.shutdown().await.unwrap();
            }
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
