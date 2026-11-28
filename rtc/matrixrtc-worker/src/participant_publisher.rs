//! Media publication for one independently admitted MatrixRTC ghost.
//! The group-session owner must hold its membership/identity authorization,
//! serialize epoch controls and media, and leave the ghost on retirement.
//! This module does not create or admit a Matrix membership.
use super::group_publication_admission::GroupPublicationAdmission;
use super::{audio, capture_audio_packet, error, video, Ghost};
use crate::{participant_epoch::ParticipantEpochFence, protocol::Message};
use matrix_rtc_call_sdk::LiveKitCall;
use matrix_rtc_transport::{LocalTrackHandle, MediaStreamKind, PublishOptions, VideoSourceConfig};
use std::{collections::HashMap, io, sync::Arc, time::Duration};
use tokio::time::Instant;

const CONTROL_BUDGET: Duration = Duration::from_secs(5);
const PCM_CHUNK: usize = 160; // 10ms at the provider's decoded 16kHz rate

#[derive(Default)]
struct Clock {
    last: Option<u32>,
    elapsed: u64,
}
impl Clock {
    fn advance(&mut self, raw: u32) -> Option<u64> {
        if let Some(last) = self.last {
            let delta = raw.wrapping_sub(last);
            if delta == 0 || delta >= (1u32 << 31) { return None; }
            self.elapsed = self.elapsed.checked_add(u64::from(delta))?;
        }
        self.last = Some(raw);
        Some(self.elapsed)
    }
}

// The decoder may not reuse parameter sets from a previous source or epoch.
fn independent_access_unit(data: &[u8]) -> bool {
    let mut sps = false;
    let mut pps = false;
    let mut i = 0;
    while i + 3 < data.len() {
        if data[i..].starts_with(&[0, 0, 1]) {
            match data[i + 3] & 0x1f {
                7 => sps = true,
                8 => pps = true,
                5 => return sps && pps,
                1..=4 => return false,
                _ => {}
            }
            i += 4;
        } else {
            i += 1;
        }
    }
    false
}

#[derive(Default)]
struct DeviceState { microphone_muted: Option<bool>, camera_off: Option<bool>, screen_share: Option<bool>, role_revision: u64 }

pub(super) enum PublicationResult {
    Consumed,
    Dropped,
    RequestKeyframe,
}

/// A publisher is tied to exactly one provider device and one already joined
/// ghost membership. Its streams are not shared with any other publisher.
/// Callers must not attach two publishers to the same LiveKitCall.
pub(super) struct ParticipantPublisher {
    call: Arc<LiveKitCall>,
    ghost: Arc<Ghost>,
    admission: Arc<GroupPublicationAdmission>,
    generation: u64,
    sources: HashMap<(String, String), DeviceState>,
    selected_audio: Option<((String, String), Instant)>,
    selected_video: Option<((String, String), Instant)>,
    member: String,
    microphone: Option<Arc<dyn LocalTrackHandle>>,
    microphone_muted: bool,
    camera: Option<video::WhatsAppVideoDecoder>,
    screen_share: bool,
    audio_source: Option<(u32, u32)>,
    video_source: Option<u32>,
    audio_clock: Clock,
    audio_expected: Option<u32>,
    video_clock: Clock,
    pcm: Vec<f32>,
    last_sample: f32,
}

impl ParticipantPublisher {
    // This accepts only an existing admitted call. The session owner verifies
    // that the call is the distinct, authorized ghost for this exact device.
    pub(super) fn new(call: Arc<LiveKitCall>, ghost: Arc<Ghost>,
        admission: Arc<GroupPublicationAdmission>, generation: u64, sources: Vec<(String, String)>) -> io::Result<Self> {
        if generation == 0 || sources.is_empty()
            || call.membership_id().is_empty() || !call.room_feed_ready() {
            return Err(error("participant publisher not admitted"));
        }
        ghost.identity.require_current()?;
        admission.require_active(&call, generation)?;
        if sources.iter().any(|(p,d)| !admission.source_matches(p,d)) { return Err(error("group publisher source is not admitted")); }
        let member = call.membership_id().to_owned();
        let sources = sources.into_iter().map(|key| (key, DeviceState::default())).collect();
        Ok(Self { call, ghost, admission, generation, sources, selected_audio: None, selected_video: None,
            member, microphone: None, microphone_muted: false, camera: None, screen_share: false,
            audio_source: None, video_source: None, audio_clock: Clock::default(),
            audio_expected: None, video_clock: Clock::default(), pcm: Vec::new(), last_sample: 0.0 })
    }

    fn check(&self, fence: &ParticipantEpochFence) -> io::Result<()> {
        self.ghost.identity.require_current()?;
        self.admission.require_active(&self.call, self.generation)?;
        if fence.current() != Some(self.generation) || !self.call.room_feed_ready()
            || self.call.membership_id() != self.member.as_str() {
            return Err(error("participant publisher retired"));
        }
        Ok(())
    }

    async fn stop_camera(&mut self) -> io::Result<()> {
        // Drop the decoder first to stop asynchronous frame capture.
        if self.camera.take().is_some() {
            let kind = if self.screen_share { MediaStreamKind::ScreenShare } else { MediaStreamKind::Camera };
            tokio::time::timeout(CONTROL_BUDGET, self.call.engine().unpublish(kind))
                .await.map_err(|_| error("participant camera unpublish timed out"))?
                .map_err(io::Error::other)?;
        }
        Ok(())
    }

    async fn stop_microphone(&mut self) -> io::Result<()> {
        self.microphone_muted = false;
        self.pcm.clear();
        self.last_sample = 0.0;
        self.audio_expected = None;
        if self.microphone.take().is_some() {
            tokio::time::timeout(CONTROL_BUDGET, self.call.engine().unpublish(MediaStreamKind::Microphone))
                .await.map_err(|_| error("participant microphone unpublish timed out"))?
                .map_err(io::Error::other)?;
        }
        Ok(())
    }

    /// Call before releasing the independent membership, even on error. The
    /// owner must leave the call if either unpublish operation fails.
    pub(super) async fn retire(&mut self) -> io::Result<()> {
        let camera = self.stop_camera().await;
        let microphone = self.stop_microphone().await;
        camera.and(microphone)
    }

    /// Reflect explicit provider state even when no subsequent packet arrives.
    /// Unknown state is not inferred from silence. Turning camera off drops
    /// the decoder; a later camera-on must restart from an independent IDR.
    pub(super) async fn apply_state(&mut self, fence: &ParticipantEpochFence, participant: &str, device: &str, state: u8, role_revision: u64) -> io::Result<()> {
        self.check(fence)?;
        let key = (participant.to_owned(), device.to_owned());
        let observed = self.sources.get_mut(&key).ok_or_else(|| error("unadmitted participant state"))?;
        match state {
            1 | 2 => observed.microphone_muted = Some(state == 1),
            3 | 4 => observed.camera_off = Some(state == 3),
            5 | 6 => {
                let role = state == 5;
                if role_revision < observed.role_revision { return Ok(()); }
                if role_revision == observed.role_revision && observed.screen_share != Some(role) { return Err(error("conflicting participant video role revision")); }
                let changed = observed.screen_share != Some(role) || role_revision != observed.role_revision;
                observed.screen_share = Some(role);
                observed.role_revision = role_revision;
                if changed && self.selected_video.as_ref().is_some_and(|(selected,_)| selected == &key) {
                    self.stop_camera().await?;
                    self.selected_video = None;
                    self.video_source = None;
                    self.video_clock = Clock::default();
                }
                return self.check(fence);
            },
            _ => return Err(error("invalid group media state")),
        }
        if state == 1 && self.selected_audio.as_ref().is_some_and(|(selected,_)| selected == &key) {
            self.selected_audio = None;
            self.audio_source = None;
            self.audio_clock = Clock::default();
            self.stop_microphone().await?;
        }
        if state == 3 && !self.screen_share && self.selected_video.as_ref().is_some_and(|(selected,_)| selected == &key) {
            self.selected_video = None;
            self.video_source = None;
            self.video_clock = Clock::default();
            self.stop_camera().await?;
        }
        // Publish an explicitly muted microphone even before the first frame;
        // do not claim every device is muted if any observation is unknown.
        if self.sources.values().all(|state| state.microphone_muted == Some(true)) {
            if self.microphone.is_none() {
                let track = tokio::time::timeout(CONTROL_BUDGET, self.call.publish(PublishOptions::microphone()))
                    .await.map_err(|_| error("muted microphone publish timed out"))?.map_err(io::Error::other)?;
                self.microphone = Some(track);
            }
            if !self.microphone_muted {
                tokio::time::timeout(CONTROL_BUDGET, self.call.engine().set_local_muted(MediaStreamKind::Microphone, true))
                    .await.map_err(|_| error("participant mute update timed out"))?.map_err(io::Error::other)?;
                self.microphone_muted = true;
            }
        } else if state == 2 && self.microphone_muted {
            tokio::time::timeout(CONTROL_BUDGET, self.call.engine().set_local_muted(MediaStreamKind::Microphone, false))
                .await.map_err(|_| error("participant unmute update timed out"))?.map_err(io::Error::other)?;
            self.microphone_muted = false;
        }
        if !self.screen_share && self.sources.values().all(|state| state.camera_off == Some(true)) { self.stop_camera().await?; }
        self.check(fence)
    }

    /// Feed only after the ordered IPC receiver has applied the epoch barrier.
    /// The caller revalidates its independent Matrix admission on every event
    /// and retires the whole publisher immediately on any revocation.
    pub(super) async fn push(&mut self, fence: &ParticipantEpochFence, msg: Message) -> io::Result<PublicationResult> {
        if !fence.permit(&msg, |s| self.sources.contains_key(&(s.participant_id.clone(), s.device.clone())))? {
            return Ok(PublicationResult::Dropped);
        }
        self.check(fence)?;
        let source = msg.participant_source.as_ref().ok_or_else(|| error("missing participant source"))?;
        let raw_timestamp = msg.timestamp as u32;
        let key = (source.participant_id.clone(), source.device.clone());
        let observed = self.sources.get(&key).ok_or_else(|| error("unadmitted participant source"))?;
        let now = Instant::now();
        if msg.kind == "whatsapp_participant_audio" {
            if observed.microphone_muted == Some(true) { return Ok(PublicationResult::Dropped); }
            let binding = (source.ssrc, source.rtp_clock_rate);
            let switch_source = self.audio_source.is_some_and(|previous| previous != binding);
            if self.selected_audio.as_ref().is_some_and(|(selected, last)| (selected != &key || switch_source) && now.duration_since(*last) < Duration::from_secs(1)) {
                return Ok(PublicationResult::Dropped);
            }
            if switch_source || self.selected_audio.as_ref().is_none_or(|(selected,_)| selected != &key) {
                self.stop_microphone().await?;
                self.audio_source = None;
                self.audio_clock = Clock::default();
            }
            self.selected_audio = Some((key, now));
            self.audio_source = Some(binding);
            if self.audio_clock.advance(raw_timestamp).is_none() {
                return Ok(PublicationResult::Dropped);
            }
            if source.discontinuity || self.audio_expected.is_some_and(|t| t != raw_timestamp) {
                self.stop_microphone().await?;
            }
            self.check(fence)?;
            let count = msg.data.len() / 4;
            self.audio_expected = Some(raw_timestamp.wrapping_add(count as u32 * (source.rtp_clock_rate / 16000)));
            self.pcm.extend(msg.data.chunks_exact(4).map(|part| f32::from_le_bytes(part.try_into().unwrap())));
            let complete = self.pcm.len() / PCM_CHUNK * PCM_CHUNK;
            if complete == 0 { return Ok(PublicationResult::Consumed); }
            if self.microphone.is_none() {
                let track = tokio::time::timeout(CONTROL_BUDGET, self.call.publish(PublishOptions::microphone()))
                    .await.map_err(|_| error("participant microphone publish timed out"))?.map_err(io::Error::other)?;
                self.microphone = Some(track);
            }

            self.check(fence)?;
            let samples: Vec<f32> = self.pcm.drain(..complete).collect();
            let upsampled = audio::upsample_16_to_48(&samples, self.last_sample);
            self.last_sample = *samples.last().unwrap_or(&self.last_sample);
            let track = self.microphone.as_ref().ok_or_else(|| error("participant microphone missing"))?.clone();
            capture_audio_packet(track, upsampled).await?;
            self.check(fence)?;
            return Ok(PublicationResult::Consumed);
        }
        // The provider has one primary video stream. A role change must
        // retire the old publication and restart decoding at an independent IDR.
        let screen_share = source.screen_share;
        // The ordered state channel is authoritative. In-flight frames from
        // a different role cannot recreate the track after a stop.
        if observed.screen_share != Some(screen_share) || source.role_revision == 0 || source.role_revision != observed.role_revision {
            return Ok(PublicationResult::Dropped);
        }
        let camera_off = observed.camera_off == Some(true);
        let switch_source = self.video_source.is_some_and(|previous| previous != source.ssrc);
        if self.selected_video.as_ref().is_some_and(|(selected, last)| (selected != &key || switch_source) && now.duration_since(*last) < Duration::from_secs(2)) {
            return Ok(PublicationResult::Dropped);
        }
        if switch_source || self.selected_video.as_ref().is_none_or(|(selected,_)| selected != &key) {
            self.stop_camera().await?;
            self.video_source = None;
            self.video_clock = Clock::default();
        }
        if self.screen_share != screen_share {
            self.stop_camera().await?;
            self.screen_share = screen_share;
            self.video_clock = Clock::default();
        }
        if !screen_share && camera_off { return Ok(PublicationResult::Dropped); }
        self.selected_video = Some((key, now));
        self.video_source = Some(source.ssrc);
        let Some(timestamp) = self.video_clock.advance(raw_timestamp) else { return Ok(PublicationResult::Dropped); };
        if source.discontinuity { self.stop_camera().await?; }
        self.check(fence)?;
        if self.camera.is_none() {
            if !independent_access_unit(&msg.data) { return Ok(PublicationResult::RequestKeyframe); }
            tokio::time::timeout(CONTROL_BUDGET, video::verify_ffmpeg()).await
                .map_err(|_| error("participant FFmpeg check timed out"))??;
            let config = VideoSourceConfig { width: video::MATRIX_VIDEO_WIDTH, height: video::MATRIX_VIDEO_HEIGHT };
            let options = if screen_share { PublishOptions::screen_share(config) } else { PublishOptions::camera(config) };
            let camera = tokio::time::timeout(CONTROL_BUDGET, self.call.publish(options))
                .await.map_err(|_| error("participant video publish timed out"))?.map_err(io::Error::other)?;
            self.camera = Some(video::WhatsAppVideoDecoder::spawn(camera)?);
        }
        self.check(fence)?;
        let decoder = self.camera.as_mut().ok_or_else(|| error("participant decoder missing"))?;
        if decoder.ensure_running().is_err() || !decoder.push(msg.data, msg.orientation, timestamp)? {
            self.stop_camera().await?;
            return Ok(PublicationResult::RequestKeyframe);
        }
        Ok(PublicationResult::Consumed)
    }
}
