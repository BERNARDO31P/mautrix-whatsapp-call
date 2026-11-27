//! Ordered routing for an independently admitted group publication session.
//! The controller owns the IPC reader and keeps the original human uplink.
//! This router owns only the child publishers, not the controller's tracks.
use super::{error, group_publication_session::GroupPublicationSession,
    participant_publisher::{ParticipantPublisher, PublicationResult}};
use crate::{participant_epoch::ParticipantEpochFence, protocol::Message};
use std::{collections::HashMap, io, sync::Arc};
use tokio::time::Instant;

#[derive(Debug)]
pub(super) enum RouteResult {
    Opened,
    Consumed,
    Dropped,
    RequestKeyframe(Message),
    Retired,
}

pub(super) struct GroupPublicationRouter {
    session: Arc<GroupPublicationSession>,
    fence: ParticipantEpochFence,
    publishers: Vec<ParticipantPublisher>,
    by_source: HashMap<(String, String), usize>,
    retired: bool,
}

impl GroupPublicationRouter {
    /// Create before sending ready; the owner must not dispatch aggregate
    /// media after acknowledging the independent roster.
    pub(super) fn new(session: Arc<GroupPublicationSession>, req: &Message,
        proposal: &Message) -> io::Result<Self> {
        session.require_proposal(req, proposal)?;
        let fence = session.fence(req)?;
        let publishers = session.publishers()?;
        let mut by_source = HashMap::new();
        for binding in &proposal.participant_bindings {
            let index = session.publisher_index(&binding.ghost_mxid).ok_or_else(|| error("group publisher missing"))?;
            if index >= publishers.len() || by_source.insert((binding.participant_id.clone(), binding.device.clone()), index).is_some() {
                return Err(error("ambiguous group publisher routing"));
            }
        }
        if by_source.len() != proposal.participant_bindings.len() {
            session.revoke();
            return Err(error("independent publisher routing mismatch"));
        }
        Ok(Self { session, fence, publishers, by_source, retired: false })
    }

    /// Route only participant epoch/media events, in the single IPC reader's
    /// order. Never infer source identity from SSRC alone or pick a fallback
    /// publisher. Revocation applies to all child publishers on any failure.
    pub(super) async fn dispatch(&mut self, msg: Message) -> io::Result<RouteResult> {
        if self.retired { return Err(error("group publication router retired")); }
        let result = self.dispatch_inner(msg).await;
        if result.is_err() || matches!(&result, Ok(RouteResult::Retired)) {
            self.retired = true;
            self.session.revoke();
        }
        result
    }

    async fn dispatch_inner(&mut self, msg: Message) -> io::Result<RouteResult> {
        if msg.kind == "whatsapp_participant_epoch" {
            self.fence.apply(&msg)?;
            return if self.fence.current().is_some() { Ok(RouteResult::Opened) }
                else { Ok(RouteResult::Retired) };
        }
        msg.validate()?;
        if msg.kind == "whatsapp_participant_state" {
            let binding = msg.participant_bindings.first().ok_or_else(|| error("missing group state binding"))?;
            if !self.session.matches_state_binding(&msg) || self.fence.current() != Some(msg.generation) {
                return Err(error("group state outside admitted roster or epoch"));
            }
            let index = *self.by_source.get(&(binding.participant_id.clone(), binding.device.clone()))
                .ok_or_else(|| error("group state publisher missing"))?;
            self.publishers.get_mut(index).ok_or_else(|| error("group state publisher missing"))?
                .apply_state(&self.fence, &binding.participant_id, &binding.device, msg.state, msg.timestamp).await?;
            return Ok(RouteResult::Consumed);
        }
        let source = msg.participant_source.as_ref().ok_or_else(|| error("missing participant source"))?;
        let index = *self.by_source.get(&(source.participant_id.clone(), source.device.clone()))
            .ok_or_else(|| error("participant source outside admitted roster"))?;
        if !self.fence.permit(&msg, |_| true)? {
            return Err(error("participant media outside the open epoch"));
        }
        let mut source = source.clone();
        let mut recovery = Message::new("matrix_participant_keyframe", &msg.call_id);
        recovery.group = true;
        recovery.generation = msg.generation;
        let publisher = self.publishers.get_mut(index).ok_or_else(|| error("missing participant publisher"))?;
        match publisher.push(&self.fence, msg).await? {
            PublicationResult::Consumed => Ok(RouteResult::Consumed),
            PublicationResult::Dropped => Ok(RouteResult::Dropped),
            PublicationResult::RequestKeyframe => {
                source.discontinuity = false;
                source.role_revision = 0;
                source.screen_share = false; // keyframe requests address RTP, not a publication role
                recovery.participant_source = Some(source);
                recovery.validate()?;
                Ok(RouteResult::RequestKeyframe(recovery))
            },
        }
    }

    /// Run on the controller's periodic verification path even without media.
    pub(super) async fn refresh(&mut self, deadline: Instant) -> io::Result<()> {
        if self.retired { return Err(error("group publication router retired")); }
        if let Err(err) = self.session.refresh(deadline).await {
            self.retired = true;
            self.session.revoke();
            return Err(err);
        }
        Ok(())
    }

    /// Consume this router before closing its owning session. Retire every
    /// publisher concurrently even if one unpublish fails. Drop revokes the
    /// scope on cancellation; the owner must still leave all child calls.
    pub(super) async fn retire(mut self) -> io::Result<()> {
        self.retired = true;
        self.session.revoke();
        let results = futures_util::future::join_all(self.publishers.iter_mut().map(|p| p.retire())).await;
        for result in results { result?; }
        Ok(())
    }
}

impl Drop for GroupPublicationRouter {
    fn drop(&mut self) { self.session.revoke(); }
}
