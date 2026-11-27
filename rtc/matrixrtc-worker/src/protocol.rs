//! Exact Go rtcwire v1 framing, with JSON base64 data as Go encoding/json []byte.
use base64::{Engine, engine::general_purpose::STANDARD};
use serde::{Deserialize, Serialize};
use std::io;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

pub const VERSION: u8 = 1;
pub const MAX_PAYLOAD: usize = 2 << 20;
pub const AUDIO_SAMPLES: usize = 960;
pub const AUDIO_BYTES: usize = AUDIO_SAMPLES * 4;

pub fn valid_device_key(key: &str) -> bool {
    use base64::engine::general_purpose::STANDARD_NO_PAD;
    key.len() == 43 && STANDARD_NO_PAD.decode(key).is_ok_and(|bytes|
        bytes.len() == 32 && STANDARD_NO_PAD.encode(bytes) == key)
}

fn missing_state()->u8{255}
fn is_zero(value: &u64) -> bool { *value == 0 }
fn is_zero_u8(value: &u8) -> bool { *value == 0 }

mod base64_data {
    use super::*;
    pub fn serialize<S: serde::Serializer>(bytes: &[u8], serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&STANDARD.encode(bytes))
    }
    pub fn deserialize<'de, D: serde::Deserializer<'de>>(deserializer: D) -> Result<Vec<u8>, D::Error> {
        let value = String::deserialize(deserializer)?;
        STANDARD.decode(value.as_bytes()).map_err(serde::de::Error::custom)
    }
}

/// Device/source metadata for negotiated group participant frames. The worker
/// still needs an authorized source-to-membership mapping before publication.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct ParticipantSource {
    pub participant_id: String,
    pub device: String,
    pub ssrc: u32,
    pub rtp_clock_rate: u32,
    #[serde(default, skip_serializing_if = "is_zero_u32")]
    pub sample_rate: u32,
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub discontinuity: bool,
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub screen_share: bool,
    #[serde(default, skip_serializing_if = "is_zero_role_revision")]
    pub role_revision: u64,
}
fn is_zero_u32(value: &u32) -> bool { *value == 0 }
fn is_zero_role_revision(value: &u64) -> bool { *value == 0 }

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct ParticipantBinding {
    pub participant_id: String,
    pub device: String,
    pub ghost_mxid: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct Message {
    pub version: u8,
    #[serde(rename = "type")]
    pub kind: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub login_id: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub call_id: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub room_id: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub slot_id: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub membership_format: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub ghost_mxid: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub bridge_bot_mxid: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub expected_user_mxid: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub expected_device: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub expected_signing_key: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub expected_identity_key: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub member_event: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub member_join_id: String,
    #[serde(default, skip_serializing_if = "is_zero")]
    pub member_join_ts: u64,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub peer: String,
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub group: bool,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub group_jid: String,
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub video: bool,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub intent: String,
    #[serde(default, skip_serializing_if = "is_zero")]
    pub timestamp: u64,
    #[serde(default, skip_serializing_if = "is_zero")]
    pub generation: u64,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub targets:Vec<String>,
    #[serde(default = "missing_state")]
    pub state: u8,
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub muted: bool,
    #[serde(default, skip_serializing_if = "is_zero_u8")]
    pub orientation: u8,
    #[serde(default, skip_serializing_if = "is_zero")]
    pub duration_us: u64,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub reason: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty", with = "base64_data")]
    pub data: Vec<u8>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub participant_source: Option<ParticipantSource>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub participant_bindings: Vec<ParticipantBinding>,
}
impl Message {
    pub fn new(kind: &str, call_id: &str) -> Self {
        Self { version: VERSION, kind: kind.to_owned(), login_id: String::new(), call_id: call_id.to_owned(), room_id: String::new(), slot_id: String::new(), membership_format: String::new(), ghost_mxid: String::new(), bridge_bot_mxid: String::new(), expected_user_mxid: String::new(), expected_device:String::new(), expected_signing_key:String::new(), expected_identity_key:String::new(), member_event:String::new(), member_join_id:String::new(), member_join_ts:0, peer: String::new(), group: false, group_jid: String::new(), video: false, intent: String::new(), state: 0, timestamp:0,generation:0,targets:vec![], muted: false, orientation: 0, duration_us: 0, reason: String::new(), data: Vec::new(), participant_source: None, participant_bindings: vec![] }
    }
    pub fn validate(&self) -> io::Result<()> {
        if self.version != VERSION || self.kind.is_empty() || self.kind.len() > 40 || self.call_id.len() > 256 || self.login_id.len() > 256 || self.room_id.len() > 512 || self.slot_id.len() > 256 || self.membership_format.len() > 32 || self.ghost_mxid.len() > 512 || self.expected_user_mxid.len() > 512 || self.peer.len() > 512 || self.group_jid.len() > 512 || self.orientation > 3 || self.duration_us > 1_000_000 {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "invalid rtcwire header or identifiers"));
        }
        if (!self.expected_signing_key.is_empty() || !self.expected_identity_key.is_empty())
            && (self.kind != "outgoing" || self.expected_device.is_empty()
                || !valid_device_key(&self.expected_signing_key) || !valid_device_key(&self.expected_identity_key)) {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "invalid invitation device keys"));
        }
        if !self.bridge_bot_mxid.is_empty() && (self.bridge_bot_mxid.len() > 512 || !matches!(self.kind.as_str(), "incoming" | "outgoing")) {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "invalid bridge bot identity field"));
        }
        if !self.intent.is_empty() && !((self.kind == "outgoing" && matches!(self.intent.as_str(), "auto" | "audio" | "video") && !self.video)
            || (self.kind == "dial" && matches!(self.intent.as_str(), "audio" | "video") && self.video == (self.intent == "video"))) {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "invalid outgoing media intent"));
        }
        if (!self.member_join_id.is_empty() && (self.member_join_id.len() > 512 || self.kind != "outgoing" || self.member_event.is_empty() || self.expected_device.is_empty()
            || !((self.membership_format == "msc" && self.member_join_ts == 0) || (self.membership_format == "room_state" && self.member_join_ts != 0))))
            || (self.member_join_id.is_empty() && self.member_join_ts != 0) {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "invalid referenced join identity"));
        }
        if !self.membership_format.is_empty() && !matches!(self.membership_format.as_str(), "msc" | "room_state") {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "unsupported MatrixRTC membership format"));
        }
        if matches!(self.kind.as_str(),"group_check"|"matrix_group_invite") {
            if self.targets.is_empty() || self.targets.len()>31 || self.targets.iter().any(|s|s.is_empty() || s.len()>192) {return Err(io::Error::other("invalid group targets"))}
        } else if !self.targets.is_empty(){return Err(io::Error::other("unexpected group targets"))}
        if (self.kind=="whatsapp_audio_mute" && !self.muted) || (self.kind=="whatsapp_audio_unmute" && self.muted) || self.reason.len()>4096 || self.expected_device.len()>255 || self.member_event.len()>512 || (self.kind=="whatsapp_video_state" && !matches!(self.state,0|1|3|4|5|6|8|11)) {
            return Err(io::Error::new(io::ErrorKind::InvalidData,"invalid video state or identity"));
        }
        if self.kind == "matrix_video_screen_start" && (!self.group || self.generation == 0 || !self.data.is_empty()) {
            return Err(io::Error::other("invalid screen source control"));
        }
        if self.kind == "whatsapp_participant_state" {
            if self.participant_bindings.len() != 1 || !matches!(self.state, 1..=6) || ((self.state >= 5) != (self.timestamp != 0)) {
                return Err(io::Error::other("invalid participant state"));
            }
            let mut envelope = self.clone();
            envelope.kind = "whatsapp_participant_roster".to_owned();
            envelope.state = 0;
            envelope.timestamp = 0;
            envelope.validate_participant_roster()?;
        } else if matches!(self.kind.as_str(), "whatsapp_participant_roster" | "matrix_participant_ready") {
            self.validate_participant_roster()?;
        } else if !self.participant_bindings.is_empty() {
            return Err(io::Error::other("unexpected participant bindings"));
        }
        let participant_frame = matches!(self.kind.as_str(), "whatsapp_participant_audio" | "whatsapp_participant_video");
        let participant_keyframe = self.kind == "matrix_participant_keyframe";
        if (participant_frame || participant_keyframe) != self.participant_source.is_some() {
            return Err(io::Error::other("unexpected or missing participant media source"));
        }
        if participant_keyframe {
            let source = self.participant_source.as_ref().ok_or_else(|| io::Error::other("missing participant keyframe source"))?;
            let roster = (self.generation >> 32) as u32;
            let key = self.generation as u32;
            let mut envelope = self.clone();
            envelope.kind = "matrix_participant_request".to_owned();
            envelope.participant_source = None;
            envelope.generation = 0;
            if roster == 0 || key == 0 || key > roster || source.participant_id.is_empty() || source.participant_id.len() > 512
                || source.device.is_empty() || source.device.len() > 512 || source.ssrc == 0 || source.rtp_clock_rate != 90000
                || source.sample_rate != 0 || source.discontinuity || source.screen_share || source.role_revision != 0 {
                return Err(io::Error::other("invalid participant keyframe source"));
            }
            envelope.validate()?;
        } else if matches!(self.kind.as_str(), "matrix_participant_request" | "whatsapp_participant_unavailable") {
            if !self.group || self.call_id.is_empty() || self.generation != 0 || self.state != 0
                || !self.participant_control_fields_empty() || !self.participant_bindings.is_empty() {
                return Err(io::Error::other("invalid participant roster request"));
            }
        } else if self.kind == "whatsapp_participant_epoch" {
            let roster = (self.generation >> 32) as u32;
            let key = self.generation as u32;
            if !self.group || self.call_id.is_empty() || roster == 0 || key == 0 || key > roster
                || !matches!(self.state, 0 | 1) || !self.data.is_empty() || self.timestamp != 0
                || self.orientation != 0 || self.duration_us != 0 {
                return Err(io::Error::other("invalid participant epoch barrier"));
            }
        } else if participant_frame {
            self.validate_participant_media()?;
        } else if matches!(self.kind.as_str(),"whatsapp_video"|"matrix_video") {
            if self.data.len()<4 || !(self.data.starts_with(&[0,0,1]) || self.data.starts_with(&[0,0,0,1])) || (self.kind=="whatsapp_video" && self.generation==0) {
                return Err(io::Error::new(io::ErrorKind::InvalidData,"invalid H.264 frame"));
            }
        } else if matches!(self.kind.as_str(), "whatsapp_audio" | "matrix_audio") {
            pcm(&self.data)?;
        } else if !self.data.is_empty(){return Err(io::Error::new(io::ErrorKind::InvalidData,"control frame contains media"));}
        Ok(())
    }

    fn participant_control_fields_empty(&self) -> bool {
        self.participant_source.is_none() && self.data.is_empty() && self.timestamp == 0 && self.duration_us == 0 && self.orientation == 0
            && !self.muted && !self.video && self.reason.is_empty() && self.room_id.is_empty() && self.slot_id.is_empty()
            && self.login_id.is_empty() && self.group_jid.is_empty() && self.ghost_mxid.is_empty() && self.expected_user_mxid.is_empty()
            && self.expected_device.is_empty() && self.peer.is_empty() && self.membership_format.is_empty() && self.member_event.is_empty()
            && self.bridge_bot_mxid.is_empty() && self.expected_signing_key.is_empty() && self.expected_identity_key.is_empty()
            && self.member_join_id.is_empty() && self.member_join_ts == 0 && self.intent.is_empty() && self.targets.is_empty()
    }

    fn validate_participant_roster(&self) -> io::Result<()> {
        let roster = (self.generation >> 32) as u32;
        let key = self.generation as u32;
        if !self.group || self.call_id.is_empty() || self.state != 0 || self.participant_source.is_some()
            || !self.data.is_empty() || self.timestamp != 0 || self.duration_us != 0 || self.orientation != 0
            || self.muted || self.video || !self.reason.is_empty() || !self.room_id.is_empty()
            || !self.slot_id.is_empty() || !self.login_id.is_empty() || !self.group_jid.is_empty()
            || !self.ghost_mxid.is_empty() || !self.expected_user_mxid.is_empty() || !self.expected_device.is_empty()
            || !self.peer.is_empty() || !self.membership_format.is_empty() || !self.member_event.is_empty()
            || !self.participant_control_fields_empty() || roster == 0 || key == 0 || key > roster || self.participant_bindings.is_empty() || self.participant_bindings.len() > 32 {
            return Err(io::Error::other("invalid group participant roster"));
        }
        let mut participants = std::collections::HashSet::new();
        let mut devices = std::collections::HashSet::new();
        for binding in &self.participant_bindings {
            if binding.participant_id.is_empty() || binding.participant_id.len() > 512
                || binding.device.is_empty() || binding.device.len() > 512
                || binding.ghost_mxid.len() < 4 || binding.ghost_mxid.len() > 512 || !binding.ghost_mxid.starts_with('@')
                || !participants.insert(&binding.participant_id) || !devices.insert(&binding.device) {
                return Err(io::Error::other("ambiguous group participant roster"));
            }
        }
        Ok(())
    }

    fn validate_participant_media(&self) -> io::Result<()> {
        let source = self.participant_source.as_ref().ok_or_else(|| io::Error::other("missing participant source"))?;
        if !self.group || self.call_id.is_empty() || self.generation == 0 || self.timestamp > u32::MAX as u64
            || source.participant_id.is_empty() || source.participant_id.len() > 512
            || source.device.is_empty() || source.device.len() > 512 || source.ssrc == 0 {
            return Err(io::Error::other("invalid participant media source"));
        }
        let valid = match self.kind.as_str() {
            "whatsapp_participant_audio" => !source.screen_share && source.role_revision == 0 && source.sample_rate == 16000
                && matches!(source.rtp_clock_rate, 16000 | 48000) && self.orientation == 0
                && !self.data.is_empty() && self.data.len() <= 1920 * 4 && self.data.len() % 4 == 0
                && self.data.chunks_exact(4).all(|chunk| {
                    let value = f32::from_le_bytes(chunk.try_into().unwrap());
                    value.is_finite() && (-1.0..=1.0).contains(&value)
                }),
            "whatsapp_participant_video" => source.sample_rate == 0 && source.rtp_clock_rate == 90000
                && self.data.len() > 3 && self.data.len() <= 1024 * 1024
                && (self.data.starts_with(&[0,0,1]) || self.data.starts_with(&[0,0,0,1])),
            _ => false,
        };
        if valid { Ok(()) } else { Err(io::Error::other("invalid participant media payload")) }
    }
}

pub fn pcm(raw: &[u8]) -> io::Result<Vec<f32>> {
    if raw.len() != AUDIO_BYTES {
        return Err(io::Error::new(io::ErrorKind::InvalidData, "audio must be 960 float32 samples"));
    }
    raw.chunks_exact(4).map(|chunk| {
        let value = f32::from_le_bytes(chunk.try_into().unwrap());
        if !value.is_finite() || !(-1.0..=1.0).contains(&value) {
            Err(io::Error::new(io::ErrorKind::InvalidData, "invalid PCM amplitude"))
        } else { Ok(value) }
    }).collect()
}

pub fn encode_pcm(samples: &[f32]) -> io::Result<Vec<u8>> {
    if samples.len() != AUDIO_SAMPLES || samples.iter().any(|v| !v.is_finite() || !(-1.0..=1.0).contains(v)) {
        return Err(io::Error::new(io::ErrorKind::InvalidData, "invalid audio PCM"));
    }
    Ok(samples.iter().flat_map(|sample| sample.to_le_bytes()).collect())
}

/// Incremental, cancellation-safe decoder. A select! branch can drop the
/// current read future after *any* partial header/body; offsets survive it.
/// Only one owner may read from a given stream at a time.
pub struct FrameReader<R> {
    reader: R,
    header: [u8; 4],
    header_read: usize,
    body: Vec<u8>,
    body_read: usize,
}

impl<R: AsyncRead + Unpin> FrameReader<R> {
    pub fn new(reader: R) -> Self {
        Self { reader, header: [0; 4], header_read: 0, body: Vec::new(), body_read: 0 }
    }
    pub async fn read(&mut self) -> io::Result<Message> {
        while self.header_read < 4 {
            let n = self.reader.read(&mut self.header[self.header_read..]).await?;
            if n == 0 { return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "truncated frame header")); }
            self.header_read += n;
        }
        if self.body.is_empty() {
            let size = u32::from_be_bytes(self.header) as usize;
            if size == 0 || size > MAX_PAYLOAD {
                return Err(io::Error::new(io::ErrorKind::InvalidData, "invalid frame length"));
            }
            self.body.resize(size, 0);
        }
        while self.body_read < self.body.len() {
            let n = self.reader.read(&mut self.body[self.body_read..]).await?;
            if n == 0 { return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "truncated frame body")); }
            self.body_read += n;
        }
        let value: Message = serde_json::from_slice(&self.body).map_err(io::Error::other)?;
        value.validate()?;
        self.header_read = 0;
        self.body_read = 0;
        self.body.clear();
        Ok(value)
    }
}

pub async fn read<R: AsyncRead + Unpin>(reader: &mut R) -> io::Result<Message> {
    let length = reader.read_u32().await? as usize;
    if length == 0 || length > MAX_PAYLOAD {
        return Err(io::Error::new(io::ErrorKind::InvalidData, "invalid frame length"));
    }
    let mut bytes = vec![0; length];
    reader.read_exact(&mut bytes).await?;
    let msg: Message = serde_json::from_slice(&bytes).map_err(io::Error::other)?;
    msg.validate()?;
    Ok(msg)
}

// Validation and serialization cannot corrupt a stream. Finish both before
// handing a frame to the single socket writer.
pub(crate) fn encode_frame(msg: &Message) -> io::Result<Vec<u8>> {
    msg.validate()?;
    // Reject impossible base64 sizes before serializing or allocating.
    if msg.data.len() > (MAX_PAYLOAD / 4) * 3 {
        return Err(io::Error::new(io::ErrorKind::InvalidData, "outgoing frame too large"));
    }
    let bytes = serde_json::to_vec(msg).map_err(io::Error::other)?;
    if bytes.is_empty() || bytes.len() > MAX_PAYLOAD {
        return Err(io::Error::new(io::ErrorKind::InvalidData, "outgoing frame too large"));
    }
    Ok(bytes)
}

pub async fn write<W: AsyncWrite + Unpin>(writer: &mut W, msg: &Message) -> io::Result<()> {
    let bytes = encode_frame(msg)?;
    write_encoded(writer, &bytes).await
}

// Only called with an already validated, bounded serialized frame.
pub(crate) async fn write_encoded<W: AsyncWrite + Unpin>(writer: &mut W, bytes: &[u8]) -> io::Result<()> {
    tokio::time::timeout(std::time::Duration::from_secs(2), async {
        writer.write_u32(bytes.len() as u32).await?;
        writer.write_all(bytes).await?;
        writer.flush().await
    }).await.map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "socket write timed out"))?
}

#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn go_wire_base64_roundtrip() {
        let mut msg = Message::new("matrix_audio", "123");
        msg.data = encode_pcm(&vec![0.25; AUDIO_SAMPLES]).unwrap();
        let (mut tx, mut rx) = tokio::io::duplex(8192);
        write(&mut tx, &msg).await.unwrap();
        assert_eq!(read(&mut rx).await.unwrap(), msg);
        assert!(serde_json::to_string(&msg).unwrap().contains("\"data\":\""));
    }
    #[test]
    fn camera_off_requires_an_explicit_state() {
        let make=|state:&str|serde_json::from_str::<Message>(&format!(r#"{{"version":1,"type":"whatsapp_video_state","call_id":"c"{state}}}"#)).unwrap();
        assert!(make("").validate().is_err());
        assert!(make(r#","state":0"#).validate().is_ok());
        assert!(make(r#","state":7"#).validate().is_err());
        assert!(Message::new("whatsapp_video","c").validate().is_err());
    }
    #[tokio::test]
    async fn rejects_oversize_and_invalid_pcm() {
        let (mut tx, mut rx) = tokio::io::duplex(64);
        tx.write_u32((MAX_PAYLOAD + 1) as u32).await.unwrap();
        assert_eq!(read(&mut rx).await.unwrap_err().kind(), io::ErrorKind::InvalidData);
        let mut raw = vec![0; AUDIO_BYTES]; raw[..4].copy_from_slice(&f32::NAN.to_le_bytes());
        assert!(pcm(&raw).is_err());
        let mut invalid = Message::new("incoming", "123");
        invalid.orientation = 4;
        assert!(invalid.validate().is_err());
        invalid.orientation = 0;
        invalid.membership_format = "future_dialect".to_owned();
        assert_eq!(invalid.validate().unwrap_err().kind(), io::ErrorKind::InvalidData);
    }
    #[tokio::test]
    async fn fragmented_frames_survive_cancellation_and_eof() {
        use tokio::io::AsyncWriteExt;
        use std::time::Duration;
        let (mut tx, rx) = tokio::io::duplex(8192);
        let mut reader = FrameReader::new(rx);
        let first = serde_json::to_vec(&Message::new("whatsapp_video_keyframe", "call1")).unwrap();
        let second = serde_json::to_vec(&Message::new("ended", "call1")).unwrap();
        let bytes = [
            (first.len() as u32).to_be_bytes().to_vec(), first,
            (second.len() as u32).to_be_bytes().to_vec(), second,
        ].concat();
        tx.write_all(&bytes[..2]).await.unwrap();
        assert!(tokio::time::timeout(Duration::from_millis(5), reader.read()).await.is_err());
        tx.write_all(&bytes[2..6]).await.unwrap();
        assert!(tokio::time::timeout(Duration::from_millis(5), reader.read()).await.is_err());
        // An unrelated call event can win select! after a partial read.
        tokio::select! {
            biased;
            _ = async {} => {},
            _ = reader.read() => panic!("fragment incorrectly parsed"),
        }
        tx.write_all(&bytes[6..bytes.len()-9]).await.unwrap();
        assert_eq!(reader.read().await.unwrap().kind, "whatsapp_video_keyframe");
        assert!(tokio::time::timeout(Duration::from_millis(5), reader.read()).await.is_err());
        tx.write_all(&bytes[bytes.len()-9..]).await.unwrap();
        assert_eq!(reader.read().await.unwrap().kind, "ended");
        drop(tx);
        assert_eq!(reader.read().await.unwrap_err().kind(), io::ErrorKind::UnexpectedEof);
    }

}

// Shared Go/Rust corpus tests exercise the real codecs on each other's framed
// output, not merely two implementations of the same test-only encoder.
#[cfg(test)]
mod interop_tests {
    use super::*;
    use std::path::Path;
    use tokio::fs::{File, OpenOptions};

    fn corpus() -> Vec<Message> {
        let expected: Vec<Message> = serde_json::from_str(include_str!(
            "../../../bridge/pkg/connector/voipbridge/rtcwire/testdata/interop.json"
        )).expect("shared wire corpus");
        assert!(expected.len() >= 20, "incomplete shared corpus");
        expected
    }

    async fn check<R: AsyncRead + Unpin>(reader: &mut R, expected: &[Message]) {
        for (i, want) in expected.iter().enumerate() {
            let got = read(reader).await.unwrap_or_else(|err| panic!("frame {i} ({}): {err}", want.kind));
            assert!(got == *want, "frame {i} ({}): semantic mismatch", want.kind);
        }
        let mut extra = [0u8; 1];
        assert_eq!(reader.read(&mut extra).await.expect("EOF check"), 0, "unexpected trailing bytes");
    }

    async fn emit<W: AsyncWrite + Unpin>(writer: &mut W, expected: &[Message]) {
        for (i, msg) in expected.iter().enumerate() {
            write(writer, msg).await.unwrap_or_else(|err| panic!("frame {i} ({}): {err}", msg.kind));
        }
    }

    #[tokio::test]
    async fn shared_corpus_roundtrip() {
        let expected = corpus();
        let (mut writer, mut reader) = tokio::io::duplex(8192);
        let ((), ()) = tokio::join!(
            async { emit(&mut writer, &expected).await; drop(writer); },
            check(&mut reader, &expected),
        );
    }

    #[tokio::test]
    async fn cross_language_exchange() {
        let Some(dir) = std::env::var_os("RTC_WIRE_INTEROP_DIR") else { return; };
        let dir = Path::new(&dir);
        let expected = corpus();
        let mut from_go = File::open(dir.join("go.frames")).await.expect("open Go wire output");
        check(&mut from_go, &expected).await;
        let mut to_go = OpenOptions::new().write(true).create_new(true)
            .open(dir.join("rust.frames")).await.expect("create Rust wire output");
        emit(&mut to_go, &expected).await;
        to_go.flush().await.expect("flush Rust wire output");
    }
}
