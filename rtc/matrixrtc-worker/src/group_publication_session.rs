//! Own the independent ghost memberships for a single admitted group epoch.
//! The gateway must supply authenticated provider bindings and the controller's
//! already pinned human participation. No account or room is created here.
use super::{error, membership_format, require_membership_support, require_prepared_identity,
    trusted_focus, validate_room, wait_room, within_answer_budget, Accounts, DeviceKeys, Ghost,
    JoinedMembership, LiveKitCall, LiveKitCallOptions, CallKind, CallIntent, CallKeyRecipients};
use super::group_publication_admission::{publisher_scope, GroupGhostBinding, GroupPublicationAdmission};
use super::participant_publisher::ParticipantPublisher;
use crate::protocol::Message;
use std::{collections::HashMap, io, sync::{Arc, atomic::{AtomicBool, Ordering}}, time::Duration};
use tokio::time::{Instant, timeout_at};

const LEAVE_BUDGET: Duration = Duration::from_secs(15);

struct JoinedGhost {
    binding: GroupGhostBinding,
    ghost: Arc<Ghost>,
    room: matrix_sdk::Room,
    scope: CallKeyRecipients,
    call: Option<Arc<LiveKitCall>>,
    admission: Option<Arc<GroupPublicationAdmission>>,
}

impl Drop for JoinedGhost {
    fn drop(&mut self) { self.scope.revoke(); }
}

pub(super) struct GroupPublicationSession {
    generation: u64,
    ghosts: Vec<JoinedGhost>,
    bindings: Vec<GroupGhostBinding>,
    publishers_issued: AtomicBool,
}

impl GroupPublicationSession {
    /// Resolve a gateway proposal against existing prepared accounts only.
    /// The caller must receive it on the authenticated, negotiated group
    /// connection and cancel preparation on a retired provider generation.
    /// No account or Matrix membership is created by this function.
    pub(super) async fn resolve_bindings(accounts: &Accounts, req: &Message,
        proposal: &Message, deadline: Instant) -> io::Result<Vec<GroupGhostBinding>> {
        proposal.validate()?;
        if !req.group || req.group_jid.is_empty() || req.ghost_mxid != req.bridge_bot_mxid
            || proposal.kind != "whatsapp_participant_roster" || proposal.call_id != req.call_id {
            return Err(error("roster not bound to group controller"));
        }
        let controller = matrix_sdk::ruma::UserId::parse(&req.bridge_bot_mxid).map_err(io::Error::other)?;
        let mut bindings = Vec::with_capacity(proposal.participant_bindings.len());
        for binding in &proposal.participant_bindings {
            let mxid = matrix_sdk::ruma::UserId::parse(&binding.ghost_mxid).map_err(io::Error::other)?;
            if mxid.server_name() != controller.server_name() || binding.ghost_mxid == req.bridge_bot_mxid
                || binding.ghost_mxid == req.expected_user_mxid {
                return Err(error("participant must be a distinct local prepared ghost"));
            }
            let ghost = within_answer_budget(deadline, accounts.get(&binding.ghost_mxid)).await?;
            if ghost.client.user_id().map(|id| id.as_str()) != Some(binding.ghost_mxid.as_str()) {
                return Err(error("prepared group ghost account mismatch"));
            }
            let matrix_device = ghost.client.device_id().ok_or_else(|| error("prepared group ghost has no device"))?.to_string();
            if matrix_device.is_empty() || matrix_device.len() > 255 { return Err(error("invalid prepared group device")); }
            within_answer_budget(deadline, require_prepared_identity(&ghost.client)).await?;
            let room = within_answer_budget(deadline, wait_room(req, &ghost)).await?;
            let mut ghost_req = req.clone();
            ghost_req.ghost_mxid = binding.ghost_mxid.clone();
            within_answer_budget(deadline, validate_room(&ghost_req, &room)).await?;
            bindings.push(GroupGhostBinding { participant: binding.participant_id.clone(), provider_device: binding.device.clone(), ghost_mxid: binding.ghost_mxid.clone(), matrix_device });
        }
        super::check_answer_deadline(deadline)?;
        Ok(bindings)
    }

    /// The caller retains the original room/slot reservation and controller.
    /// Its existing two-member admission must already have pinned the human
    /// and controller joins. Do not run concurrently with the aggregate loop.
    pub(super) async fn join(accounts: &Accounts, req: &Message,
        controller: &LiveKitCall, human: &JoinedMembership, controller_pin: &JoinedMembership,
        human_keys: &DeviceKeys, generation: u64, bindings: Vec<GroupGhostBinding>,
        deadline: Instant) -> io::Result<Self> {
        let human_device = human.origin.sender_device_id().ok_or_else(|| error("human device missing"))?;
        let controller_device = controller_pin.origin.sender_device_id().ok_or_else(|| error("controller device missing"))?;
        if !req.group || req.ghost_mxid != req.bridge_bot_mxid || human.sender != req.expected_user_mxid
            || controller_pin.sender != req.bridge_bot_mxid || !controller.room_feed_ready()
            || controller_pin.member_id != controller.membership_id()
            || controller.room_id() != req.room_id.as_str() || controller.slot_id() != req.slot_id.as_str()
            || human.room_id != req.room_id || controller_pin.room_id != req.room_id
            || human.slot_id != req.slot_id || controller_pin.slot_id != req.slot_id {
            return Err(error("invalid pinned group controller or human"));
        }
        let current = within_answer_budget(deadline, async { Ok(controller.subscribe_memberships().await) }).await?;
        {
            let members = current.borrow();
            if members.len() != 2 || !members.iter().any(|m| m.same_participation(human))
                || !members.iter().any(|m| m.same_participation(controller_pin)) {
                return Err(error("controller admission changed before group join"));
            }
        }
        if bindings.is_empty() || bindings.len() > 32 { return Err(error("invalid group publisher count")); }
        // All bindings and accounts must be ready before joining any ghost.
        let mut ghosts = Vec::with_capacity(bindings.len());
        for binding in &bindings {
            if ghosts.iter().any(|child: &JoinedGhost| child.binding.ghost_mxid == binding.ghost_mxid) { continue; }
            let scope = publisher_scope(req, generation, human_device, controller_device, &bindings, &binding.ghost_mxid)?;
            let ghost = within_answer_budget(deadline, accounts.get(&binding.ghost_mxid)).await?;
            if ghost.client.user_id().map(|id| id.as_str()) != Some(binding.ghost_mxid.as_str())
                || ghost.client.device_id().map(|id| id.as_str()) != Some(binding.matrix_device.as_str()) {
                return Err(error("prepared group identity does not match binding"));
            }
            within_answer_budget(deadline, require_prepared_identity(&ghost.client)).await?;
            within_answer_budget(deadline, require_membership_support(&ghost.client, membership_format(req)?)).await?;
            let room = within_answer_budget(deadline, wait_room(req, &ghost)).await?;
            let mut ghost_req = req.clone();
            ghost_req.ghost_mxid = binding.ghost_mxid.clone();
            within_answer_budget(deadline, validate_room(&ghost_req, &room)).await?;
            ghosts.push(JoinedGhost { binding: binding.clone(), ghost, room, scope, call: None, admission: None });
        }
        let focus = trusted_focus()?;
        let format = membership_format(req)?;
        let mut session = Self { generation, ghosts, bindings: bindings.clone(), publishers_issued: AtomicBool::new(false) };
        // Each SDK join uses the same absolute deadline. Wait for all join
        // outcomes, including errors, so successful siblings can be left.
        let results = futures_util::future::join_all(session.ghosts.iter_mut().map(|child| {
            let focus = focus.clone();
            async move {
                let call = LiveKitCall::join_until_with_outbox(&child.room, LiveKitCallOptions {
                    trusted_focus: Some(focus), call_key_recipients: Some(child.scope.clone()),
                    slot_id: req.slot_id.clone(), format,
                    intent: Some(if req.video { CallIntent::Video } else { CallIntent::Audio }),
                    kind: CallKind::Group { notify: None }, auto_subscribe: false,
                    ..Default::default()
                }, Some(deadline), child.ghost.outbox.clone()).await.map_err(io::Error::other)?;
                child.call = Some(Arc::new(call));
                Ok::<(), io::Error>(())
            }
        })).await;
        if let Some(failure) = results.into_iter().find_map(Result::err) {
            let _ = session.close().await;
            return Err(failure);
        }
        let admission = session.admit(req, human, controller_pin, human_keys, &bindings, deadline).await;
        if let Err(failure) = admission {
            let _ = session.close().await;
            return Err(failure);
        }
        Ok(session)
    }

    async fn admit(&mut self, req: &Message, human: &JoinedMembership, controller: &JoinedMembership,
        human_keys: &DeviceKeys, bindings: &[GroupGhostBinding], deadline: Instant) -> io::Result<()> {
        let human_device = human.origin.sender_device_id().ok_or_else(|| error("human device missing"))?;
        let controller_device = controller.origin.sender_device_id().ok_or_else(|| error("controller device missing"))?;
        let mut allowed = HashMap::new();
        allowed.insert(human.sender.as_str(), human_device);
        allowed.insert(controller.sender.as_str(), controller_device);
        for binding in bindings { allowed.insert(binding.ghost_mxid.as_str(), binding.matrix_device.as_str()); }
        let generation = self.generation;
        // Wait for each independently synced room to observe the exact roster.
        // Missing participants may catch up; unexpected or replaced joins may not.
        let results = futures_util::future::join_all(self.ghosts.iter_mut().map(|child| {
            let allowed = &allowed;
            async move {
                let call = child.call.as_ref().ok_or_else(|| error("group ghost not joined"))?;
                let mut feed = within_answer_budget(deadline, async { Ok(call.subscribe_memberships().await) }).await?;
                loop {
                    let complete = {
                        let rows = feed.borrow_and_update();
                        let mut seen = std::collections::HashSet::new();
                        for m in rows.iter() {
                            if !seen.insert(m.sender.as_str()) || allowed.get(m.sender.as_str()).copied() != m.origin.sender_device_id()
                                || !allowed.contains_key(m.sender.as_str())
                                || (m.sender == human.sender && !m.same_participation(human))
                                || (m.sender == controller.sender && !m.same_participation(controller)) {
                                return Err(error("unexpected or replaced group membership"));
                            }
                        }
                        rows.len() == allowed.len()
                    };
                    if complete { break; }
                    timeout_at(deadline, feed.changed()).await.map_err(|_| error("group membership deadline exceeded"))?
                        .map_err(|_| error("group membership feed closed"))?;
                }
                let proof = within_answer_budget(deadline, GroupPublicationAdmission::capture(req, &child.room,
                    call, &child.ghost.identity, generation, human_device, controller_device, bindings.to_vec())).await?;
                proof.require_original_joins(human, controller, human_keys)?;
                child.admission = Some(Arc::new(proof));
                Ok::<(), io::Error>(())
            }
        })).await;
        for result in results { result?; }
        // All ghost views must agree on the same joins and keys before any
        // one can start sending keys. A different roster retires the batch.
        let first = self.ghosts.first().and_then(|c| c.admission.as_ref()).ok_or_else(|| error("missing group admission"))?;
        for child in &self.ghosts {
            let proof = child.admission.as_ref().ok_or_else(|| error("missing group admission"))?;
            first.require_same_group(proof)?;
            proof.require_current(child.call.as_ref().ok_or_else(|| error("missing group call"))?, generation)?;
        }
        super::check_answer_deadline(deadline)?;
        for child in &self.ghosts {
            let proof = child.admission.as_ref().ok_or_else(|| error("missing group admission"))?;
            proof.activate_recipients(child.call.as_ref().ok_or_else(|| error("missing group call"))?)?;
        }
        Ok(())
    }

    /// The controller may request an independent roster only after the base
    /// group call has been admitted. This does not alter its membership.
    pub(super) fn request(req: &Message) -> io::Result<Message> {
        if !req.group || req.group_jid.is_empty() || req.ghost_mxid != req.bridge_bot_mxid {
            return Err(error("independent media requires a group controller"));
        }
        let mut msg = Message::new("matrix_participant_request", &req.call_id);
        msg.group = true;
        msg.validate()?;
        Ok(msg)
    }

    /// Acknowledge only after the controller has transitioned and all child
    /// proofs are fresh. The owner must send this before accepting epoch-open.
    pub(super) fn require_proposal(&self, req: &Message, proposal: &Message) -> io::Result<()> {
        proposal.validate()?;
        if proposal.kind != "whatsapp_participant_roster" || proposal.call_id != req.call_id
            || proposal.generation != self.generation || proposal.participant_bindings.len() != self.bindings.len()
            || !proposal.participant_bindings.iter().zip(&self.bindings).all(|(b, admitted)|
                b.participant_id == admitted.participant && b.device == admitted.provider_device && b.ghost_mxid == admitted.ghost_mxid) {
            self.revoke();
            return Err(error("independent roster acknowledgement mismatch"));
        }
        Ok(())
    }

    pub(super) async fn ready(&self, req: &Message, proposal: &Message, deadline: Instant) -> io::Result<Message> {
        self.require_proposal(req, proposal)?;
        self.refresh(deadline).await?;
        let mut msg = proposal.clone();
        msg.kind = "matrix_participant_ready".to_owned();
        msg.validate()?;
        Ok(msg)
    }

    pub(super) fn matches_state_binding(&self, msg: &Message) -> bool {
        msg.kind == "whatsapp_participant_state" && msg.generation == self.generation
            && msg.participant_bindings.len() == 1 && self.bindings.iter().any(|admitted| {
                let binding = &msg.participant_bindings[0];
                binding.participant_id == admitted.participant && binding.device == admitted.provider_device
                    && binding.ghost_mxid == admitted.ghost_mxid
            })
    }

    pub(super) fn fence(&self, req: &Message) -> io::Result<crate::participant_epoch::ParticipantEpochFence> {
        crate::participant_epoch::ParticipantEpochFence::scoped(req.call_id.clone(), self.generation)
    }

    /// Construct exactly one publisher per independently joined ghost. The
    /// session must outlive the publishers; drop/retire them before close().
    pub(super) fn publisher_index(&self, ghost: &str) -> Option<usize> {
        self.ghosts.iter().position(|child| child.binding.ghost_mxid == ghost)
    }

    pub(super) fn publishers(&self) -> io::Result<Vec<ParticipantPublisher>> {
        if self.publishers_issued.swap(true, Ordering::AcqRel) {
            return Err(error("group publishers already issued"));
        }
        let result = self.ghosts.iter().map(|child| ParticipantPublisher::new(
            child.call.as_ref().ok_or_else(|| error("missing group call"))?.clone(), child.ghost.clone(),
            child.admission.as_ref().ok_or_else(|| error("missing group admission"))?.clone(), self.generation,
            self.bindings.iter().filter(|binding| binding.ghost_mxid == child.binding.ghost_mxid)
                .map(|binding| (binding.participant.clone(), binding.provider_device.clone())).collect())).collect::<io::Result<Vec<_>>>();
        if result.is_err() { self.revoke(); }
        result
    }

    /// Check every child and the controller's independently synced views.
    /// Child memberships must already be admitted; only the controller's
    /// observation may be incomplete during the bounded transition.
    pub(super) fn controller_view(&self, req: &Message, admission: &super::Admission,
        members: &[JoinedMembership], roster: &[super::Participant]) -> io::Result<bool> {
        let result = (|| {
            for child in &self.ghosts {
                child.ghost.identity.require_current()?;
                let proof = child.admission.as_ref().ok_or_else(|| error("missing group admission"))?;
                let call = child.call.as_ref().ok_or_else(|| error("missing group call"))?;
                proof.require_active(call, self.generation)?;
            }
            self.ghosts.first().and_then(|child| child.admission.as_ref())
                .ok_or_else(|| error("missing group admission"))?
                .controller_view(req, admission, members, roster)
        })();
        if result.is_err() { self.revoke(); }
        result
    }

    /// The session owner must call this periodically even with no media.
    /// Any error revokes every sibling scope immediately. The owner must
    /// retire the publishers and then call close().
    pub(super) async fn refresh(&self, deadline: Instant) -> io::Result<()> {
        let result = futures_util::future::try_join_all(self.ghosts.iter().map(|child| async move {
            let proof = child.admission.as_ref().ok_or_else(|| error("missing group admission"))?;
            let call = child.call.as_ref().ok_or_else(|| error("missing group call"))?;
            within_answer_budget(deadline, proof.refresh(&child.room, call, &child.ghost.identity)).await
        })).await;
        if let Err(failure) = result { self.revoke(); return Err(failure); }
        Ok(())
    }

    pub(super) fn revoke(&self) {
        for child in &self.ghosts { child.scope.revoke(); }
    }

    /// Do not touch the controller membership, which belongs to the parent.
    /// Revoke all scopes before leaving any child; attempt all leaves even
    /// if one fails. On cancellation the scope is still revoked by Drop.
    pub(super) async fn close(mut self) -> io::Result<()> {
        self.revoke();
        let deadline = Instant::now() + LEAVE_BUDGET;
        let results = futures_util::future::join_all(self.ghosts.iter_mut().filter_map(|child| child.call.take()).map(|call| async move {
            let call = Arc::try_unwrap(call).map_err(|_| error("group call still referenced after publisher retirement"))?;
            timeout_at(deadline, call.leave()).await.map_err(|_| error("group ghost leave timed out"))?.map_err(io::Error::other)
        })).await;
        for result in results { result?; }
        Ok(())
    }
}
