//! Ordered receiver-side fence for a separately admitted group-media session.
//! This does not grant Matrix membership or authorize a publisher by itself.
use crate::protocol::{Message, ParticipantSource};
use std::io;

pub struct ParticipantEpochFence {
    call_id: String,
    highest: u64,
    active: Option<u64>,
    admitted: Option<u64>,
}

impl ParticipantEpochFence {
    // Construct only after the group session and peer capability are admitted.
    pub fn new(call_id: String) -> io::Result<Self> {
        if call_id.is_empty() || call_id.len() > 256 {
            return Err(io::Error::other("invalid participant session identity"));
        }
        Ok(Self { call_id, highest: 0, active: None, admitted: None })
    }

    // A negotiated roster authorizes exactly one provider epoch. Even a
    // valid newer epoch requires a new Matrix admission and receiver fence.
    pub fn scoped(call_id: String, generation: u64) -> io::Result<Self> {
        let roster = (generation >> 32) as u32;
        let key = generation as u32;
        if roster == 0 || key == 0 || key > roster {
            return Err(io::Error::other("invalid admitted participant generation"));
        }
        let mut fence = Self::new(call_id)?;
        fence.admitted = Some(generation);
        Ok(fence)
    }

    pub fn current(&self) -> Option<u64> { self.active }

    // The single IPC reader must apply controls before dispatching subsequent
    // media. The owner must drop old publisher queues on a retirement and
    // recheck current() before asynchronous publication; already published
    // media cannot be retroactively revoked by an IPC barrier.
    pub fn apply(&mut self, msg: &Message) -> io::Result<()> {
        msg.validate()?;
        if msg.call_id != self.call_id || msg.kind != "whatsapp_participant_epoch" {
            return Err(io::Error::other("unexpected participant epoch control"));
        }
        if self.admitted.is_some_and(|generation| msg.generation != generation) {
            return Err(io::Error::other("participant epoch outside admitted scope"));
        }
        match msg.state {
            1 if self.active.is_none() && msg.generation > self.highest => {
                self.highest = msg.generation;
                self.active = Some(msg.generation);
                Ok(())
            }
            0 if self.active == Some(msg.generation) => {
                self.active = None;
                Ok(())
            }
            0 if self.active.is_none() && self.highest == 0 && self.admitted == Some(msg.generation) => {
                self.highest = msg.generation;
                Ok(())
            }
            _ => Err(io::Error::other("invalid participant epoch transition")),
        }
    }

    // authorize must check the independently admitted source/identity mapping.
    // An epoch opening is not permission for arbitrary participants to publish.
    pub fn permit(&self, msg: &Message, authorize: impl FnOnce(&ParticipantSource) -> bool) -> io::Result<bool> {
        msg.validate()?;
        if msg.call_id != self.call_id || !matches!(msg.kind.as_str(), "whatsapp_participant_audio" | "whatsapp_participant_video") {
            return Err(io::Error::other("unexpected participant media event"));
        }
        Ok(self.active == Some(msg.generation) && msg.participant_source.as_ref().is_some_and(authorize))
    }
}
