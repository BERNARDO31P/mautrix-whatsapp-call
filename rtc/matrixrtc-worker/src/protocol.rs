//! Exact Go rtcwire v1 framing, with JSON base64 data as Go encoding/json []byte.
use base64::{Engine, engine::general_purpose::STANDARD};
use serde::{Deserialize, Serialize};
use std::io;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

pub const VERSION: u8 = 1;
pub const MAX_PAYLOAD: usize = 2 << 20;
pub const AUDIO_SAMPLES: usize = 960;
pub const AUDIO_BYTES: usize = AUDIO_SAMPLES * 4;

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
    pub expected_user_mxid: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub expected_device: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub member_event: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub peer: String,
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub group: bool,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub group_jid: String,
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub video: bool,
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
}
impl Message {
    pub fn new(kind: &str, call_id: &str) -> Self {
        Self { version: VERSION, kind: kind.to_owned(), login_id: String::new(), call_id: call_id.to_owned(), room_id: String::new(), slot_id: String::new(), membership_format: String::new(), ghost_mxid: String::new(), expected_user_mxid: String::new(), expected_device:String::new(), member_event:String::new(), peer: String::new(), group: false, group_jid: String::new(), video: false, state: 0, timestamp:0,generation:0,targets:vec![], muted: false, orientation: 0, duration_us: 0, reason: String::new(), data: Vec::new() }
    }
    pub fn validate(&self) -> io::Result<()> {
        if self.version != VERSION || self.kind.is_empty() || self.kind.len() > 40 || self.call_id.len() > 256 || self.login_id.len() > 256 || self.room_id.len() > 512 || self.slot_id.len() > 256 || self.membership_format.len() > 32 || self.ghost_mxid.len() > 512 || self.expected_user_mxid.len() > 512 || self.peer.len() > 512 || self.group_jid.len() > 512 || self.orientation > 3 || self.duration_us > 1_000_000 {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "invalid rtcwire header or identifiers"));
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
        if matches!(self.kind.as_str(),"whatsapp_video"|"matrix_video") {
            if self.data.len()<4 || !(self.data.starts_with(&[0,0,1]) || self.data.starts_with(&[0,0,0,1])) || (self.kind=="whatsapp_video" && self.generation==0) {
                return Err(io::Error::new(io::ErrorKind::InvalidData,"invalid H.264 frame"));
            }
        } else if matches!(self.kind.as_str(), "whatsapp_audio" | "matrix_audio") {
            pcm(&self.data)?;
        } else if !self.data.is_empty(){return Err(io::Error::new(io::ErrorKind::InvalidData,"control frame contains media"));}
        Ok(())
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

pub async fn write<W: AsyncWrite + Unpin>(writer: &mut W, msg: &Message) -> io::Result<()> {
    msg.validate()?;
    let bytes = serde_json::to_vec(msg).map_err(io::Error::other)?;
    if bytes.is_empty() || bytes.len() > MAX_PAYLOAD {
        return Err(io::Error::new(io::ErrorKind::InvalidData, "outgoing frame too large"));
    }
    tokio::time::timeout(std::time::Duration::from_secs(2), async {
        writer.write_u32(bytes.len() as u32).await?;
        writer.write_all(&bytes).await?;
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
