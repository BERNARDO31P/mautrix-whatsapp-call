//! Separate admission contract for independent group publishers. The gateway
//! must supply already-authorized provider-to-ghost bindings; room membership
//! alone is not sufficient authority to represent a WhatsApp device.
use super::{error, membership_format, refresh_trusted_device, require_prepared_identity,
    Admission, DeviceKeys, IdentityFence, JoinedMembership, LiveKitCall, MembershipFormat};
use matrix_sdk::RoomMemberships;
use matrix_rtc_call_sdk::{CallKeyRecipients, CallRecipientKeys};
use crate::protocol::Message;
use std::{collections::{HashMap, HashSet}, io};

/// One provisioned Matrix device for one provider device. Distinct provider
/// devices may not silently share a Matrix ghost or its published tracks.
#[derive(Clone)]
pub(super) struct GroupGhostBinding {
    pub participant: String,
    pub provider_device: String,
    pub ghost_mxid: String,
    pub matrix_device: String,
}

/// Construct the initially closed key scope for one authorized publisher.
/// The coordinator must pass this scope to LiveKitCallOptions before joining.
/// The same group roster is independently checked against actual memberships
/// and device keys by capture() before the scope can send any media keys.
pub(super) fn publisher_scope(req: &Message, generation: u64,
    human_device: &str, controller_device: &str, bindings: &[GroupGhostBinding],
    local_ghost: &str) -> io::Result<CallKeyRecipients> {
    if !req.group || req.group_jid.is_empty() || req.ghost_mxid != req.bridge_bot_mxid
        || req.expected_user_mxid == req.bridge_bot_mxid || generation == 0
        || bindings.is_empty() || bindings.len() > 32 || human_device.is_empty()
        || controller_device.is_empty() || (!req.expected_device.is_empty() && req.expected_device != human_device) {
        return Err(error("invalid group publisher scope"));
    }
    let mut peers = vec![(req.expected_user_mxid.clone(), human_device.to_owned()),
        (req.bridge_bot_mxid.clone(), controller_device.to_owned())];
    let mut users: HashMap<String, String> = peers.iter().cloned().collect();
    let mut participants = HashSet::new();
    let mut devices = HashSet::new();
    let mut found = false;
    for binding in bindings {
        if binding.participant.is_empty() || binding.provider_device.is_empty()
            || binding.ghost_mxid.is_empty() || binding.matrix_device.is_empty()
            || binding.ghost_mxid == req.expected_user_mxid || binding.ghost_mxid == req.bridge_bot_mxid
            || !participants.insert(binding.participant.as_str()) || !devices.insert(binding.provider_device.as_str()) {
            return Err(error("ambiguous group publisher scope"));
        }
        if let Some(previous) = users.get(&binding.ghost_mxid) {
            if previous != &binding.matrix_device { return Err(error("conflicting Matrix device for group ghost")); }
        } else {
            users.insert(binding.ghost_mxid.clone(), binding.matrix_device.clone());
            if binding.ghost_mxid != local_ghost { peers.push((binding.ghost_mxid.clone(), binding.matrix_device.clone())); }
        }
        if binding.ghost_mxid == local_ghost { found = true; }
    }
    if !found { return Err(error("local group ghost is not bound")); }
    CallKeyRecipients::new_group(generation, peers).ok_or_else(|| error("invalid group recipient roster"))
}

pub(super) struct GroupPublicationAdmission {
    feeds: Admission,
    pins: Vec<JoinedMembership>,
    keys: Vec<DeviceKeys>,
    local: GroupGhostBinding,
    local_sources: Vec<(String, String)>,
    local_member: String,
    generation: u64,
    room_id: String,
    slot_id: String,
    format: MembershipFormat,
    scope: CallKeyRecipients,
}

impl GroupPublicationAdmission {
    /// Capture only after the owner has independently authorized the roster,
    /// provisioned each ghost, and joined the exact group slot. This deliberately
    /// supports a single Matrix human and one controller, not arbitrary Matrix
    /// members. A changed roster requires a new admission, never a repin.
    pub(super) async fn capture(req: &Message, room: &matrix_sdk::Room,
        call: &LiveKitCall, identity: &IdentityFence, generation: u64,
        human_device: &str, controller_device: &str,
        bindings: Vec<GroupGhostBinding>) -> io::Result<Self> {
        if !req.group || req.group_jid.is_empty() || generation == 0
            || req.bridge_bot_mxid.is_empty() || req.ghost_mxid != req.bridge_bot_mxid
            || req.expected_user_mxid == req.bridge_bot_mxid
            || human_device.is_empty() || controller_device.is_empty()
            || (!req.expected_device.is_empty() && human_device != req.expected_device)
            || bindings.is_empty() || bindings.len() > 32
            || room.room_id().as_str() != req.room_id.as_str()
            || room.state() != matrix_sdk::RoomState::Joined
            || !room.latest_encryption_state().await.map_err(io::Error::other)?.is_encrypted() {
            return Err(error("invalid independent group publication request"));
        }
        let scope = call.group_recipient_scope().ok_or_else(|| error("group call has no closed recipient scope"))?;
        if scope.is_revoked() || scope.group_active(generation) {
            return Err(error("group recipient scope already used"));
        }
        identity.check().await?;
        let client = room.client();
        require_prepared_identity(&client).await?;
        let own = client.user_id().ok_or_else(|| error("missing group ghost user"))?;
        let own_device = client.device_id().ok_or_else(|| error("missing group ghost device"))?;
        let mut allowed = HashMap::new();
        allowed.insert(req.expected_user_mxid.clone(), human_device.to_owned());
        allowed.insert(req.bridge_bot_mxid.clone(), controller_device.to_owned());
        let mut participants = HashSet::new();
        let mut devices = HashSet::new();
        let mut local = None;
        let mut local_sources = Vec::new();
        for binding in bindings {
            if binding.participant.is_empty() || binding.provider_device.is_empty()
                || binding.ghost_mxid.is_empty() || binding.matrix_device.is_empty()
                || !participants.insert(binding.participant.clone())
                || !devices.insert(binding.provider_device.clone())
                || binding.ghost_mxid == req.expected_user_mxid || binding.ghost_mxid == req.bridge_bot_mxid {
                return Err(error("duplicate or ambiguous group source binding"));
            }
            if let Some(previous) = allowed.insert(binding.ghost_mxid.clone(), binding.matrix_device.clone()) {
                if previous != binding.matrix_device { return Err(error("conflicting Matrix device for group ghost")); }
            }
            if binding.ghost_mxid == own.as_str() {
                if binding.matrix_device != own_device.as_str() {
                    return Err(error("group publisher device does not match prepared identity"));
                }
                local_sources.push((binding.participant.clone(), binding.provider_device.clone()));
                local = Some(binding);
            }
        }
        let local = local.ok_or_else(|| error("local ghost has no authorized provider binding"))?;
        let joined = room.members(RoomMemberships::JOIN).await.map_err(io::Error::other)?;
        if allowed.keys().any(|user| !joined.iter().any(|m| m.user_id().as_str() == user.as_str())) {
            return Err(error("group RTC participant is not joined to the room"));
        }
        let feeds = Admission::inspect(room, req).await?;
        feeds.check_feeds()?;
        if membership_format(req)? == MembershipFormat::Msc && !feeds.open(req).await {
            return Err(error("group slot lacks per-member encryption"));
        }
        let pins = feeds.primary.members.borrow().clone();
        let mut seen = HashSet::new();
        let mut member_ids = HashSet::new();
        for pin in &pins {
            if !seen.insert(pin.sender.clone()) || !member_ids.insert(pin.member_id.clone())
                || pin.member_id.is_empty() || pin.room_id != req.room_id || pin.slot_id != req.slot_id
                || pin.application.application_type() != Some("m.call")
                || allowed.get(&pin.sender).map(String::as_str) != pin.origin.sender_device_id()
                || !allowed.contains_key(&pin.sender)
                || (membership_format(req)? == MembershipFormat::Msc && pin.origin.was_encrypted() != Some(true)) {
                return Err(error("unexpected group membership"));
            }
        }
        if seen.len() != allowed.len() { return Err(error("group membership roster incomplete")); }
        let local_pin = pins.iter().find(|p| p.sender == local.ghost_mxid)
            .ok_or_else(|| error("group ghost did not join"))?;
        let local_member = local_pin.member_id.clone();
        let mut keys = Vec::with_capacity(pins.len());
        for pin in &pins {
            let device = pin.origin.sender_device_id().ok_or_else(|| error("missing group membership device"))?;
            let key = refresh_trusted_device(room, &pin.sender, device).await?;
            if pin.sender == req.expected_user_mxid && !req.expected_device.is_empty() {
                key.require_invitation(req)?;
            }
            keys.push(key);
        }
        let result = Self { feeds, pins, keys, local, local_sources, local_member, generation,
            room_id: req.room_id.clone(), slot_id: req.slot_id.clone(), format: membership_format(req)?, scope };
        identity.check().await?;
        result.require_current(call, generation)?;
        Ok(result)
    }

    /// Require the original participation (including legacy join timestamp),
    /// not merely the same user/device. A refresh of the same join is allowed.
    pub(super) fn require_current(&self, call: &LiveKitCall, generation: u64) -> io::Result<()> {
        let result = self.check_current(call, generation);
        if result.is_err() { self.scope.revoke(); }
        result
    }

    fn check_current(&self, call: &LiveKitCall, generation: u64) -> io::Result<()> {
        self.feeds.check_feeds()?;
        if self.scope.is_revoked() || !call.group_recipient_scope().is_some_and(|scope| self.scope.same_scope(&scope)) {
            return Err(error("group recipient scope revoked or replaced"));
        }
        if generation != self.generation || !call.room_feed_ready()
            || call.room_id() != self.room_id.as_str() || call.slot_id() != self.slot_id.as_str()
            || call.membership_id() != self.local_member.as_str() {
            return Err(error("independent group publisher retired"));
        }
        let roster = call.participants();
        let mut local = roster.iter().filter(|p| p.is_local);
        let own = local.next().ok_or_else(|| error("group publisher local participation missing"))?;
        if local.next().is_some() || own.user_id != self.local.ghost_mxid
            || own.member_id != self.local_member
            || own.device_id.as_deref() != Some(self.local.matrix_device.as_str()) {
            return Err(error("group publisher local membership changed"));
        }
        let primary = self.feeds.primary.members.borrow();
        let alternate = self.feeds.alternate.members.borrow();
        if primary.len() != self.pins.len() || !self.matches(&primary) || !self.matches(&alternate)
            || primary.iter().any(|member| !self.pins.iter().any(|pin| pin.same_participation(member)
                && pin.origin.was_encrypted() == member.origin.was_encrypted())) {
            return Err(error("group membership changed; retire all publishers"));
        }
        Ok(())
    }

    fn matches(&self, members: &[JoinedMembership]) -> bool {
        let mut seen = HashSet::new();
        members.iter().all(|member| seen.insert(member.sender.as_str())
            && member.application.application_type() == Some("m.call")
            && self.pins.iter().any(|pin| pin.same_participation(member)))
    }

    /// Verify the original controller's own view, not just the views of the
    /// independent publishers. During initial sync, missing known members can
    /// catch up. Unknown or replaced members are never a partial success.
    pub(super) fn controller_view(&self, req: &Message, admission: &Admission,
        members: &[JoinedMembership], roster: &[super::Participant]) -> io::Result<bool> {
        admission.check_feeds()?;
        let primary = admission.primary.members.borrow();
        let alternate = admission.alternate.members.borrow();
        if !self.matches(&primary) || !self.matches(&alternate) || !self.matches(members)
            || primary.iter().chain(members.iter()).any(|member| !self.pins.iter().any(|pin|
                pin.same_participation(member) && pin.origin.was_encrypted() == member.origin.was_encrypted())) {
            return Err(error("controller observed an unauthorized or replaced group membership"));
        }
        let mut seen = HashSet::new();
        let mut own = false;
        let mut human = false;
        for participant in roster {
            if !seen.insert(participant.member_id.as_str()) || !self.pins.iter().any(|pin|
                pin.sender == participant.user_id && pin.member_id == participant.member_id
                && pin.origin.sender_device_id() == participant.device_id.as_deref()
                && participant.is_local == (pin.sender == req.bridge_bot_mxid)) {
                return Err(error("controller observed an unauthorized group transport"));
            }
            own |= participant.is_local && participant.user_id == req.bridge_bot_mxid;
            human |= !participant.is_local && participant.user_id == req.expected_user_mxid;
        }
        Ok(primary.len() == self.pins.len() && members.len() == self.pins.len() && own && human)
    }

    pub(super) fn require_original_joins(&self, human: &JoinedMembership,
        controller: &JoinedMembership, human_keys: &DeviceKeys) -> io::Result<()> {
        if !self.pins.iter().zip(&self.keys).any(|(pin, keys)| pin.same_participation(human) && keys == human_keys)
            || !self.pins.iter().any(|pin| pin.same_participation(controller)) {
            return Err(error("group human or controller join replaced"));
        }
        Ok(())
    }

    pub(super) fn require_same_group(&self, other: &Self) -> io::Result<()> {
        if self.generation != other.generation || self.pins.len() != other.pins.len()
            || self.pins.iter().zip(&self.keys).any(|(pin, key)| !other.pins.iter().zip(&other.keys)
                .any(|(candidate, candidate_key)| pin.same_participation(candidate)
                    && pin.origin.was_encrypted() == candidate.origin.was_encrypted() && key == candidate_key)) {
            return Err(error("group admission views disagree"));
        }
        Ok(())
    }

    pub(super) fn source_matches(&self, participant: &str, device: &str) -> bool {
        self.local_sources.iter().any(|(p, d)| p == participant && d == device)
    }

    /// Activate the same initially closed recipient scope supplied to the SDK
    /// at join time. Exclude our own device and pin all admitted remote keys as
    /// one immutable set. A partial roster cannot authorize key distribution.
    pub(super) fn activate_recipients(&self, call: &LiveKitCall) -> io::Result<()> {
        self.require_current(call, self.generation)?;
        let mut peers = Vec::new();
        for (pin, key) in self.pins.iter().zip(&self.keys) {
            if pin.sender == self.local.ghost_mxid { continue; }
            let device = pin.origin.sender_device_id().ok_or_else(|| error("missing group recipient device"))?;
            peers.push((pin.sender.clone(), device.to_owned(), CallRecipientKeys {
                signing: key.signing.clone(), identity: key.identity.clone(), master: key.master.clone(),
            }));
        }
        if !self.scope.activate_group(self.generation, peers) {
            self.scope.revoke();
            return Err(error("group recipient scope disagrees with admission"));
        }
        Ok(())
    }

    pub(super) fn require_active(&self, call: &LiveKitCall, generation: u64) -> io::Result<()> {
        self.require_current(call, generation)?;
        if !self.scope.group_active(generation) { return Err(error("group keys not admitted")); }
        Ok(())
    }

    pub(super) fn revoke(&self) { self.scope.revoke(); }

    /// The owner must also run this on its periodic topology/key refresh and
    /// retire every publication on failure, including when no frames arrive.
    pub(super) async fn refresh(&self, room: &matrix_sdk::Room, call: &LiveKitCall,
        identity: &IdentityFence) -> io::Result<()> {
        let result = self.refresh_inner(room, call, identity).await;
        if result.is_err() { self.scope.revoke(); }
        result
    }

    async fn refresh_inner(&self, room: &matrix_sdk::Room, call: &LiveKitCall,
        identity: &IdentityFence) -> io::Result<()> {
        self.require_active(call, self.generation)?;
        identity.check().await?;
        require_prepared_identity(&room.client()).await?;
        if room.state() != matrix_sdk::RoomState::Joined
            || !room.latest_encryption_state().await.map_err(io::Error::other)?.is_encrypted() {
            return Err(error("group room admission lost"));
        }
        let joined = room.members(RoomMemberships::JOIN).await.map_err(io::Error::other)?;
        if self.pins.iter().any(|pin| pin.room_id != room.room_id().as_str()
            || !joined.iter().any(|m| m.user_id().as_str() == pin.sender.as_str())) {
            return Err(error("group RTC room participant left"));
        }
        if self.format == MembershipFormat::Msc && !matches!(
            self.feeds.primary.room.slot_state(&self.slot_id).await,
            Some(matrix_rtc_core::SlotState::Open(open)) if open.application_type == "m.call"
                && matches!(open.mechanism, Some(matrix_rtc_core::EncryptionMechanism::PerMember))) {
            return Err(error("group RTC slot encryption changed"));
        }
        for (pin, key) in self.pins.iter().zip(&self.keys) {
            let device = pin.origin.sender_device_id().ok_or_else(|| error("missing group device"))?;
            key.require_same(&refresh_trusted_device(room, &pin.sender, device).await?)?;
        }
        identity.check().await?;
        self.require_current(call, self.generation)
    }
}

impl Drop for GroupPublicationAdmission {
    fn drop(&mut self) { self.scope.revoke(); }
}
