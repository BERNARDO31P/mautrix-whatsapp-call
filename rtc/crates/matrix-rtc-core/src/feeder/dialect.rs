// Copyright 2026 Element Creations Ltd.
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE in the repository root for full details.

//! Which generation of MatrixRTC a room is read in: the feeder's only
//! format-specific decisions, behind [`IngestDialect`]. The core implements it
//! for each [`MembershipFormat`](crate::compat::MembershipFormat); an
//! application feeding through [`RoomFeeder`](super::RoomFeeder) may bring its
//! own.

use crate::{EventIn, MaybeSend, RawStickyEvent, ReceivedEncryptionKey, ToDeviceMessageIn};

/// How one room's membership and media keys are read.
pub trait IngestDialect: MaybeSend {
    /// The room state event types membership is also read from.
    fn membership_state_event_types(&self) -> Vec<String> {
        Vec::new()
    }

    /// Whether the room's slots are read; membership then waits for them, so a
    /// member is never briefly joined to a slot the room's state says is closed.
    fn reads_slots(&self) -> bool {
        true
    }

    /// The room's complete current membership, from its sticky member events
    /// and the latest membership state of [`Self::membership_state_event_types`].
    fn current_membership(
        &self,
        room_id: &str,
        sticky: Vec<EventIn>,
        state: Vec<EventIn>,
    ) -> Vec<RawStickyEvent>;

    /// Delay until the next locally enforced membership expiry, if any.
    /// Server-enforced sticky membership needs no local timer.
    fn membership_expiry_in(&self, _state: &[EventIn]) -> Option<std::time::Duration> {
        None
    }

    /// Translate and schedule expiry from the same input. Time-dependent
    /// dialects can override this to use a single clock sample for both.
    fn current_membership_with_expiry(
        &self,
        room_id: &str,
        sticky: Vec<EventIn>,
        state: Vec<EventIn>,
    ) -> (Vec<RawStickyEvent>, Option<std::time::Duration>) {
        let expiry = self.membership_expiry_in(&state);
        (self.current_membership(room_id, sticky, state), expiry)
    }

    /// Translate using the feeder's non-decreasing estimate of Unix time.
    /// Dialects without locally enforced expiry keep their existing behavior.
    fn current_membership_with_expiry_at(
        &self,
        room_id: &str,
        sticky: Vec<EventIn>,
        state: Vec<EventIn>,
        _now_ms: u64,
    ) -> (Vec<RawStickyEvent>, Option<std::time::Duration>) {
        self.current_membership_with_expiry(room_id, sticky, state)
    }

    /// A media key from a to-device message naming this room: one of the spec's
    /// key types, or a type beyond them the client subscribed to. The default
    /// reads MSC4143's body; a dialect overriding it reads its own, and should
    /// call [`super::parse_msc_key`] for anything it does not.
    fn parse_key(&self, message: &ToDeviceMessageIn) -> Option<ReceivedEncryptionKey> {
        super::parse_msc_key(message)
    }
}
