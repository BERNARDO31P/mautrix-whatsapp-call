// Copyright 2026 Element Creations Ltd.
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE in the repository root for full details.

//! Feeds one room's state from a `MatrixBackend`: subscribes to what the room
//! needs in its [`IngestDialect`], seeds room state before membership, and
//! forwards timeline events, redactions and relations to the room's
//! [`ApplicationIntake`]. Media keys arrive on one to-device subscription per
//! client and are routed to the room they name through the [`RoomRegistry`].
//! The one place Matrix becomes core input, on every binding.
//!
//! Sinks only enqueue; `run()` does the work under the room's lock and is a
//! plain future the caller spawns.

use std::collections::HashMap;
use std::sync::{Arc, Mutex as StdMutex, Weak};

use serde::Deserialize;
use serde_json::Value;
use tokio::sync::{Mutex, mpsc, watch};

use base64::Engine as _;

use crate::encryption::MEDIA_KEY_BASE64;
use crate::{
    ApplicationIntake, BackendError, EventEncryption, EventIn, EventOrigin, KEY_MESSAGE_TYPE,
    KeyOrigin, MIN_MEDIA_KEY_LEN, MatrixBackend, RawSlotEvent, RawTimelineEvent,
    ReceivedEncryptionKey, RoomSink, RoomSubjects, SLOT_EVENT_TYPE, Subscription,
    ToDeviceMessageIn, ToDeviceSink,
};

mod dialect;
#[cfg(test)]
mod tests;

pub use dialect::IngestDialect;

const MEMBER_EVENT_TYPES: [&str; 2] = ["m.rtc.member", "org.matrix.msc4143.rtc.member"];
const SLOT_EVENT_TYPES: [&str; 2] = [SLOT_EVENT_TYPE, "org.matrix.msc4143.rtc.slot"];
/// The media key types, stable and as MSC4143's unstable id. Element Call's
/// legacy key shape travels under the same unstable type, so a message's type
/// never says which body it carries: the dialect of the room it names does.
const KEY_EVENT_TYPES: [&str; 2] = [KEY_MESSAGE_TYPE, "io.element.call.encryption_keys"];

/// How long an unseeded room waits on a delivery before the feeder names what
/// it is still waiting on; repeated while nothing arrives. A host that never
/// delivers a subject leaves the open pending, and this says which one.
const SEEDING_WARN_AFTER: std::time::Duration = std::time::Duration::from_secs(5);

/// The rooms a client holds, by room id: each one's dialect, which reads a
/// media key of a type beyond the spec's, and a weak handle the to-device
/// feeder routes keys to. Holding no strong reference is what lets a dropped room stop receiving
/// keys without anyone unregistering it first.
pub struct RoomRegistry<M>(Arc<StdMutex<HashMap<String, RegisteredRoom<M>>>>);

struct RegisteredRoom<M> {
    dialect: Arc<dyn IngestDialect>,
    room: Weak<Mutex<M>>,
    generation: Arc<()>,
}

impl<M> Clone for RoomRegistry<M> {
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}

impl<M> Default for RoomRegistry<M> {
    fn default() -> Self {
        Self(Arc::default())
    }
}

/// Registering a room the registry already holds a live entry for.
#[derive(Debug, thiserror::Error)]
#[error("{0} is already open")]
pub struct RoomAlreadyOpen(pub String);

impl<M> RoomRegistry<M> {
    /// Refused while `room_id` has a live entry; an entry whose room was
    /// dropped without unregistering is replaced.
    pub fn register(
        &self,
        room_id: &str,
        dialect: Arc<dyn IngestDialect>,
        room: &Arc<Mutex<M>>,
    ) -> Result<(), RoomAlreadyOpen> {
        let mut rooms = self.lock();
        if rooms
            .get(room_id)
            .is_some_and(|entry| entry.room.strong_count() > 0)
        {
            return Err(RoomAlreadyOpen(room_id.to_owned()));
        }
        rooms.insert(
            room_id.to_owned(),
            RegisteredRoom {
                dialect,
                room: Arc::downgrade(room),
                generation: Arc::new(()),
            },
        );
        Ok(())
    }

    /// Returns whether the registry is empty afterwards.
    pub fn unregister(&self, room_id: &str) -> bool {
        let mut rooms = self.lock();
        rooms.remove(room_id);
        rooms.is_empty()
    }

    /// The dialect `room_id` was opened in, if it is held.
    pub fn dialect(&self, room_id: &str) -> Option<Arc<dyn IngestDialect>> {
        Some(self.lock().get(room_id)?.dialect.clone())
    }

    /// The live room for `room_id`, if one is held.
    pub fn room(&self, room_id: &str) -> Option<Arc<Mutex<M>>> {
        self.lock().get(room_id)?.room.upgrade()
    }

    // A queue entry carries only a weak generation token: it cannot keep a
    // closed room alive, and re-registering even the same room handle creates
    // a distinct admission generation.
    fn generation(&self, room_id: &str) -> Option<Weak<()>> {
        let rooms = self.lock();
        let entry = rooms.get(room_id)?;
        (entry.room.strong_count() > 0).then(|| Arc::downgrade(&entry.generation))
    }

    fn room_for_generation(&self, room_id: &str, generation: &Weak<()>) -> Option<Arc<Mutex<M>>> {
        let rooms = self.lock();
        let entry = rooms.get(room_id)?;
        if !generation.ptr_eq(&Arc::downgrade(&entry.generation)) {
            return None;
        }
        entry.room.upgrade()
    }

    pub fn is_empty(&self) -> bool {
        self.lock().is_empty()
    }

    /// Never held across an await, so a poisoned lock only means a panic
    /// elsewhere mid-insert; the map itself is still consistent.
    fn lock(&self) -> std::sync::MutexGuard<'_, HashMap<String, RegisteredRoom<M>>> {
        self.0
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }
}

/// What a room needs in `dialect`: the slot state where the dialect reads
/// slots, and the membership state it reads.
pub fn subjects_for(
    dialect: &dyn IngestDialect,
    timeline_event_types: Vec<String>,
) -> RoomSubjects {
    let mut state_event_types: Vec<String> = Vec::new();
    if dialect.reads_slots() {
        state_event_types.extend(SLOT_EVENT_TYPES.iter().map(|t| (*t).to_owned()));
    }
    state_event_types.extend(dialect.membership_state_event_types());
    RoomSubjects {
        state_event_types,
        timeline_event_types,
    }
}

/// The `EventOrigin` of a message-like event, from what the client reported.
pub fn event_origin(encryption: &EventEncryption) -> EventOrigin {
    match encryption {
        EventEncryption::Cleartext => EventOrigin::Cleartext,
        EventEncryption::Encrypted {
            sender_device_id, ..
        } => EventOrigin::encrypted(sender_device_id.clone()),
    }
}

/// The `KeyOrigin` of a to-device message. A confirmation the client did not
/// report counts as unconfirmed.
pub fn key_origin(encryption: &EventEncryption, sender: &str) -> KeyOrigin {
    match encryption {
        EventEncryption::Cleartext => KeyOrigin::Cleartext,
        EventEncryption::Encrypted {
            sender_device_id,
            sender_confirmed,
        } => KeyOrigin::Encrypted {
            sender_user_id: sender.to_owned(),
            sender_device_id: sender_device_id.clone(),
            sender_is_confirmed: sender_confirmed.unwrap_or(false),
        },
    }
}

fn to_timeline_event(room_id: &str, event: EventIn) -> RawTimelineEvent {
    RawTimelineEvent {
        room_id: room_id.to_owned(),
        event_id: event.event_id,
        sender: event.sender,
        origin: event_origin(&event.encryption),
        event_type: event.event_type,
        origin_server_ts: event.origin_server_ts,
        content: event.content,
    }
}

/// One slot's state event under each spelling of the slot type.
#[derive(Default)]
struct SlotEvents {
    stable: Option<EventIn>,
    unstable: Option<EventIn>,
}

impl SlotEvents {
    /// The stable event where the room has one, else the unstable one: never
    /// both.
    fn current(&self) -> Option<&EventIn> {
        self.stable.as_ref().or(self.unstable.as_ref())
    }

    fn of_type(&mut self, stable: bool) -> &mut Option<EventIn> {
        if stable {
            &mut self.stable
        } else {
            &mut self.unstable
        }
    }
}

/// Replaces every slot's event of one spelling with `events`, the room's
/// complete set under that spelling; the other spelling's events are kept.
fn replace_slot_events(
    slots: &mut HashMap<String, SlotEvents>,
    stable: bool,
    events: Vec<EventIn>,
) {
    for slot in slots.values_mut() {
        *slot.of_type(stable) = None;
    }
    for event in events {
        let Some(slot_id) = event.state_key.clone() else {
            continue;
        };
        *slots.entry(slot_id).or_default().of_type(stable) = Some(event);
    }
    slots.retain(|_, slot| slot.current().is_some());
}

/// An unparseable slot content resolves closed rather than vanishing: an
/// unreadable slot is not an open one.
fn to_slot_event(room_id: &str, event: EventIn) -> Option<RawSlotEvent> {
    Some(RawSlotEvent {
        room_id: room_id.to_owned(),
        slot_id: event.state_key?,
        content: serde_json::from_value(event.content).unwrap_or_default(),
    })
}

enum RoomInput {
    Sticky(Vec<EventIn>),
    State(String, Vec<EventIn>),
    Snapshot(Arc<RoomSnapshot>),
    Members(Vec<String>),
    Encryption(bool),
    Timeline(Vec<EventIn>),
    Redaction(String),
}

#[derive(Clone, PartialEq)]
struct RoomSnapshot {
    generation: u64,
    encrypted: bool,
    state: Vec<(String, Vec<EventIn>)>,
    members: Vec<String>,
    sticky: Vec<EventIn>,
}

// Ordered transitions cannot safely be dropped or coalesced across events.
// An overloaded feed is invalid until the room is reopened and reseeded.
const ROOM_INPUT_CAPACITY: usize = 128;
// Bound distinct queued snapshots in serialized size and collection count.
// Losing an ordered input requires reopening and reseeding the feed.
const MAX_ROOM_INPUT_BYTES: usize = 512 * 1024;
const MAX_ROOM_INPUT_ENTRIES: usize = 4096;

impl RoomInput {
    fn fits_queue(&self) -> bool {
        fn fits(value: &impl serde::Serialize) -> bool {
            serde_json::to_writer(EnvelopeSizeBudget(MAX_ROOM_INPUT_BYTES), value).is_ok()
        }
        match self {
            Self::Sticky(events) | Self::Timeline(events) =>
                events.len() <= MAX_ROOM_INPUT_ENTRIES && fits(events),
            Self::State(kind, events) =>
                events.len() <= MAX_ROOM_INPUT_ENTRIES && fits(&(kind, events)),
            Self::Snapshot(snapshot) => {
                let entries = snapshot.state.iter().fold(
                    snapshot.members.len().saturating_add(snapshot.sticky.len()),
                    |count, (_, events)| count.saturating_add(1).saturating_add(events.len()),
                );
                entries <= MAX_ROOM_INPUT_ENTRIES && fits(&(
                    snapshot.encrypted, &snapshot.state, &snapshot.members, &snapshot.sticky,
                ))
            }
            Self::Members(members) => members.len() <= MAX_ROOM_INPUT_ENTRIES && fits(members),
            Self::Redaction(id) => fits(id),
            Self::Encryption(_) => true,
        }
    }
}

// Separate from the initial seeding flag: an incomplete read invalidates
// readiness immediately, even while old snapshots are still queued.
#[derive(Clone, Copy)]
struct SnapshotHealth {
    generation: u64,
    fresh: bool,
}

fn recover_snapshot(health: &watch::Sender<SnapshotHealth>, generation: u64) {
    health.send_if_modified(|current| {
        if current.generation != generation || current.fresh { return false; }
        current.fresh = true;
        true
    });
}

struct ChannelRoomSink {
    health: watch::Sender<SnapshotHealth>,
    tx: mpsc::Sender<RoomInput>,
    stop: watch::Sender<bool>,
    // Only the last queued input may be deduplicated. A weak reference
    // avoids retaining a second snapshot or suppressing a later retry.
    tail_snapshot: StdMutex<Weak<RoomSnapshot>>,
}

impl ChannelRoomSink {
    fn new(tx: mpsc::Sender<RoomInput>, stop: watch::Sender<bool>) -> Self {
        let (health, _) = watch::channel(SnapshotHealth { generation: 0, fresh: true });
        Self { health, tx, stop, tail_snapshot: StdMutex::new(Weak::new()) }
    }

    // Called with the tail mutex held, so all producer inputs and overflow
    // decisions are ordered. Never wait for a consumer from a host callback.
    fn enqueue(&self, input: RoomInput) {
        if *self.stop.borrow() { return; }
        if !input.fits_queue() {
            log::warn!("RTC room feed unavailable: input exceeds queue budget");
            self.stop.send_replace(true);
            return;
        }
        if self.tx.try_send(input).is_err() {
            log::warn!("RTC room feed unavailable: queue full or receiver closed");
            self.stop.send_replace(true);
        }
    }

    fn send(&self, input: RoomInput) {
        let mut tail = self.tail_snapshot.lock().unwrap_or_else(|e| e.into_inner());
        *tail = Weak::new();
        self.enqueue(input);
    }
}

impl RoomSink for ChannelRoomSink {
    fn on_sticky_events(&self, events: Vec<EventIn>) {
        self.send(RoomInput::Sticky(events));
    }

    fn on_state_events(&self, event_type: String, events: Vec<EventIn>) {
        self.send(RoomInput::State(event_type, events));
    }

    fn on_room_snapshot(&self, encrypted: bool, state: Vec<(String, Vec<EventIn>)>,
                        members: Vec<String>, sticky: Vec<EventIn>) {
        let mut tail = self.tail_snapshot.lock().unwrap_or_else(|e| e.into_inner());
        if *self.stop.borrow() { return; }
        let generation = self.health.borrow().generation;
        let snapshot = Arc::new(RoomSnapshot { generation, encrypted, state, members, sticky });
        if tail.upgrade().is_some_and(|previous| previous.as_ref() == snapshot.as_ref()) {
            return;
        }
        *tail = Arc::downgrade(&snapshot);
        self.enqueue(RoomInput::Snapshot(snapshot));
    }

    fn on_room_snapshot_failed(&self) {
        let mut tail = self.tail_snapshot.lock().unwrap_or_else(|e| e.into_inner());
        *tail = Weak::new();
        if *self.stop.borrow() { return; }
        self.health.send_modify(|current| {
            if let Some(next) = current.generation.checked_add(1) {
                current.generation = next;
            } else {
                // Never permit reuse of a freshness generation.
                self.stop.send_replace(true);
            }
            current.fresh = false;
        });
    }

    fn on_joined_members(&self, user_ids: Vec<String>) {
        self.send(RoomInput::Members(user_ids));
    }

    fn on_encryption(&self, encrypted: bool) {
        self.send(RoomInput::Encryption(encrypted));
    }

    fn on_timeline_events(&self, events: Vec<EventIn>) {
        self.send(RoomInput::Timeline(events));
    }

    fn on_redaction(&self, event_id: String) {
        self.send(RoomInput::Redaction(event_id));
    }
}

/// A room the library is feeding. Dropping it detaches: the subscription
/// ends, and nothing delivered afterwards is applied.
pub struct RoomAttachment {
    room_id: String,
    subscription: Arc<dyn Subscription>,
    stop: watch::Sender<bool>,
    seeded: watch::Receiver<bool>,
    health: watch::Receiver<SnapshotHealth>,
}

impl RoomAttachment {
    pub fn room_id(&self) -> &str {
        &self.room_id
    }

    /// Resolves once the current sets have been applied, so a join issued
    /// afterwards sees them. Also resolves if the feeder stops first.
    pub async fn seeded(&self) {
        let mut seeded = self.seeded.clone();
        let mut stop = self.stop.subscribe();
        let mut health = self.health.clone();
        loop {
            let has_seed = *seeded.borrow_and_update();
            let fresh = health.borrow_and_update().fresh;
            if *stop.borrow() || (has_seed && fresh) { return; }
            tokio::select! {
                _ = stop.changed() => return,
                result = seeded.changed() => if result.is_err() { return; },
                result = health.changed() => if result.is_err() { return; },
            }
        }
    }

    pub fn is_seeded(&self) -> bool {
        let ready = *self.seeded.borrow();
        ready && self.seeded.has_changed().is_ok() && !*self.stop.borrow() && self.health.borrow().fresh
    }
}

impl Drop for RoomAttachment {
    fn drop(&mut self) {
        log::info!("[{}] detaching", self.room_id);
        // Signal separately from room inputs before cancelling the producer.
        // A full backlog must not postpone detachment.
        self.stop.send_replace(true);
        self.subscription.cancel();
    }
}

/// Attaches rooms; see the module docs.
pub struct RoomFeeder;

impl RoomFeeder {
    /// Subscribes to what `room` needs in `dialect` and returns the
    /// attachment plus the future that applies what arrives. The caller spawns
    /// [`RoomFeederRun::run`].
    pub async fn attach<B, M>(
        backend: Arc<B>,
        room: Arc<Mutex<M>>,
        dialect: Arc<dyn IngestDialect>,
    ) -> Result<(RoomAttachment, RoomFeederRun<B, M>), BackendError>
    where
        B: MatrixBackend + 'static,
        M: ApplicationIntake<B>,
    {
        let (room_id, timeline_event_types, sticky_event_types) = {
            let mut room = room.lock().await;
            (
                room.rtc().room_id().to_owned(),
                room.timeline_event_types(),
                room.sticky_event_types(),
            )
        };
        let subjects = subjects_for(dialect.as_ref(), timeline_event_types);
        log::info!("[{room_id}] attaching: {subjects:?}");

        let (tx, rx) = mpsc::channel(ROOM_INPUT_CAPACITY);
        let (stop_tx, stop_rx) = watch::channel(false);
        let sink = Arc::new(ChannelRoomSink::new(tx, stop_tx.clone()));
        let health = sink.health.clone();
        let subscription = backend
            .subscribe_room(room_id.clone(), subjects, sink)
            .await?;
        let (seeded_tx, seeded_rx) = watch::channel(false);

        let attachment = RoomAttachment {
            room_id: room_id.clone(),
            subscription: subscription.clone(),
            stop: stop_tx,
            seeded: seeded_rx,
            health: health.subscribe(),
        };
        let run = RoomFeederRun {
            backend,
            room,
            room_id,
            dialect,
            sticky_event_types,
            rx,
            subscription,
            stop: stop_rx,
            seeded: seeded_tx,
            health,
            state: FeedState::default(),
        };
        Ok((attachment, run))
    }
}

#[derive(Default)]
struct FeedState {
    seen_encryption: bool,
    seen_members: bool,
    seen_slots: bool,
    /// The room's slots by slot id (state key), each under both spellings: a
    /// delivery replaces only its own spelling's events.
    slots: HashMap<String, SlotEvents>,
    /// The latest sticky set's member events, once one arrived.
    sticky: Option<Vec<EventIn>>,
    /// The latest membership state, by event type, of the types the dialect
    /// reads it from.
    membership_state: HashMap<String, Vec<EventIn>>,
    membership_dirty: bool,
    membership_expiry: Option<crate::executor::Instant>,
}

/// The future that applies a room's inputs. Ends when the attachment drops.
pub struct RoomFeederRun<B, M> {
    backend: Arc<B>,
    room: Arc<Mutex<M>>,
    room_id: String,
    dialect: Arc<dyn IngestDialect>,
    /// The application's sticky types, read once at attach like its timeline
    /// types.
    sticky_event_types: Vec<String>,
    rx: mpsc::Receiver<RoomInput>,
    subscription: Arc<dyn Subscription>,
    stop: watch::Receiver<bool>,
    seeded: watch::Sender<bool>,
    health: watch::Sender<SnapshotHealth>,
    state: FeedState,
}

impl<B, M> RoomFeederRun<B, M>
where
    B: MatrixBackend + 'static,
    M: ApplicationIntake<B>,
{
    pub async fn run(mut self) {
        enum Wake {
            Input(Option<RoomInput>),
            ExpireMembership,
            ExpireHolds,
            SeedingWarning,
            Stop,
        }
        loop {
            if *self.stop.borrow() { break; }
            let expiry_in = self.state.membership_expiry.map(|at|
                at.saturating_duration_since(crate::executor::Instant::now()));
            let wake = if *self.seeded.borrow() {
                // A delayed-leave hold ending is an input of its own (034 R21).
                // Detachment must also wake a feeder waiting for the room lock.
                let hold = tokio::select! {
                    _ = self.stop.changed() => break,
                    mut room = self.room.lock() => room.rtc().hold_expires_at(),
                };
                let hold_in =
                    hold.map(|at| at.saturating_duration_since(crate::executor::Instant::now()));
                tokio::select! {
                    _ = self.stop.changed() => Wake::Stop,
                    input = self.rx.recv() => Wake::Input(input),
                    () = crate::executor::sleep(expiry_in.unwrap_or_default()), if expiry_in.is_some() => Wake::ExpireMembership,
                    () = crate::executor::sleep(hold_in.unwrap_or_default()), if hold_in.is_some() => Wake::ExpireHolds,
                }
            } else {
                tokio::select! {
                    _ = self.stop.changed() => Wake::Stop,
                    input = self.rx.recv() => Wake::Input(input),
                    () = crate::executor::sleep(expiry_in.unwrap_or_default()), if expiry_in.is_some() => Wake::ExpireMembership,
                    () = crate::executor::sleep(SEEDING_WARN_AFTER) => Wake::SeedingWarning,
                }
            };
            // select is deliberately fair between input and timers. Recheck
            // stop even when a ready input won the race with detachment.
            if *self.stop.borrow() { break; }
            let input = match wake {
                Wake::Stop | Wake::Input(None) => break,
                Wake::Input(Some(input)) => input,
                Wake::ExpireMembership => {
                    self.state.membership_dirty = true;
                    self.apply_membership().await;
                    continue;
                }
                Wake::ExpireHolds => {
                    self.room.lock().await.rtc().expire_holds().await;
                    continue;
                }
                Wake::SeedingWarning => {
                    log::warn!(
                        "[{}] not seeded: nothing delivered for {}s, still waiting on {}",
                        self.room_id,
                        SEEDING_WARN_AFTER.as_secs(),
                        self.missing_subjects().join(", "),
                    );
                    continue;
                }
            };
            let mut applied_generation = None;
            match input {
                RoomInput::Encryption(encrypted) => self.on_encryption(encrypted).await,
                RoomInput::State(event_type, events) => self.on_state(event_type, events).await,
                RoomInput::Snapshot(snapshot) => {
                    // Release the queue-owned Arc before awaiting network work,
                    // so equal subsequent snapshots can retry failed application.
                    let RoomSnapshot { generation, encrypted, state, members, sticky } =
                        Arc::unwrap_or_clone(snapshot);
                    // A newer failed read invalidates older queued snapshots.
                    // Do not apply them as if they represented current room state.
                    if self.health.borrow().generation != generation { continue; }
                    applied_generation = Some(generation);
                    self.on_encryption(encrypted).await;
                    // A complete snapshot may move a slot between aliases.
                    // Stage all slot spellings before publishing any resolved
                    // slot state: an intermediate absence would auto-leave.
                    let mut slots_changed = false;
                    for (event_type, events) in state {
                        if SLOT_EVENT_TYPES.contains(&event_type.as_str()) {
                            replace_slot_events(&mut self.state.slots, event_type == SLOT_EVENT_TYPE, events);
                            slots_changed = true;
                        } else {
                            self.on_state(event_type, events).await;
                        }
                    }
                    if slots_changed {
                        self.publish_slots().await;
                    }
                    self.on_members(members).await;
                    self.on_sticky(sticky).await;
                }
                RoomInput::Members(user_ids) => self.on_members(user_ids).await,
                RoomInput::Sticky(events) => self.on_sticky(events).await,
                RoomInput::Timeline(events) => {
                    let events: Vec<RawTimelineEvent> = events
                        .into_iter()
                        .map(|event| to_timeline_event(&self.room_id, event))
                        .collect();
                    self.room.lock().await.on_timeline_events(&events);
                }
                RoomInput::Redaction(event_id) => {
                    self.room.lock().await.on_event_redacted(&event_id);
                }
            }

            // Do not start another membership apply after detachment. An
            // already-running input is allowed to finish its own transaction.
            if *self.stop.borrow() { break; }
            if self.state.membership_dirty && self.gate_open() {
                self.apply_membership().await;
            }
            if let Some(generation) = applied_generation {
                if !*self.stop.borrow() && *self.seeded.borrow() && !self.state.membership_dirty {
                    recover_snapshot(&self.health, generation);
                }
            }
        }
        // On overflow the attachment may still exist. Cancel the producer
        // here as well; ordinary attachment drop already cancels it.
        self.rx.close();
        self.subscription.cancel();
        log::info!("[{}] feeder stopped", self.room_id);
    }

    async fn on_encryption(&mut self, encrypted: bool) {
        self.room.lock().await.rtc().on_encryption_received(encrypted).await;
        self.state.seen_encryption = true;
    }

    async fn on_sticky(&mut self, events: Vec<EventIn>) {
        self.on_application_sticky(&events).await;
        self.state.sticky = Some(events.into_iter()
            .filter(|event| MEMBER_EVENT_TYPES.contains(&event.event_type.as_str()))
            .collect());
        self.state.membership_dirty = true;
    }

    /// Hands the application its sticky types before the set is narrowed to
    /// member events, which is all the membership dialect reads.
    async fn on_application_sticky(&self, events: &[EventIn]) {
        let events: Vec<RawTimelineEvent> = events
            .iter()
            .filter(|event| self.sticky_event_types.contains(&event.event_type))
            .map(|event| to_timeline_event(&self.room_id, event.clone()))
            .collect();
        if !events.is_empty() {
            self.room.lock().await.on_sticky_events(&events);
        }
    }

    async fn on_state(&mut self, event_type: String, events: Vec<EventIn>) {
        if SLOT_EVENT_TYPES.contains(&event_type.as_str()) {
            replace_slot_events(&mut self.state.slots, event_type == SLOT_EVENT_TYPE, events);
            self.publish_slots().await;
        } else if self
            .dialect
            .membership_state_event_types()
            .contains(&event_type)
        {
            self.state.membership_state.insert(event_type, events);
            self.state.membership_dirty = true;
        } else {
            log::debug!(
                "[{}] ignoring {} state events of a type not asked for: {event_type}",
                self.room_id,
                events.len(),
            );
        }
    }

    async fn publish_slots(&mut self) {
        let slots: Vec<RawSlotEvent> = self
            .state
            .slots
            .values()
            .filter_map(SlotEvents::current)
            .filter_map(|event| to_slot_event(&self.room_id, event.clone()))
            .collect();
        self.room.lock().await.rtc().on_slots_received(slots).await;
        self.state.seen_slots = true;
    }

    /// A joined-members set without the account itself cannot be the room's
    /// current members — the account is in every room it attaches — so it is
    /// a store that has not loaded, and applying it would project everyone
    /// out, us included.
    async fn on_members(&mut self, user_ids: Vec<String>) {
        let own = self.backend.own_user_id();
        if !user_ids.contains(&own) {
            log::warn!(
                "[{}] ignoring a joined-members set of {} that does not contain {own}",
                self.room_id,
                user_ids.len(),
            );
            return;
        }
        self.room
            .lock()
            .await
            .rtc()
            .on_members_received(user_ids)
            .await;
        self.state.seen_members = true;
    }

    /// The `RoomSink` deliveries seeding still waits on, as the host would
    /// name them.
    fn missing_subjects(&self) -> Vec<String> {
        let mut missing = Vec::new();
        if !self.state.seen_encryption {
            missing.push("on_encryption".to_owned());
        }
        if !self.state.seen_members {
            missing.push("on_joined_members (including our own user)".to_owned());
        }
        if self.dialect.reads_slots() && !self.state.seen_slots {
            missing.push(format!(
                "on_state_events({})",
                SLOT_EVENT_TYPES.join(" or ")
            ));
        }
        if self.state.sticky.is_none() && self.state.membership_state.is_empty() {
            let mut membership = vec!["on_sticky_events".to_owned()];
            membership.extend(
                self.dialect
                    .membership_state_event_types()
                    .into_iter()
                    .map(|event_type| format!("on_state_events({event_type})")),
            );
            missing.push(membership.join(" or "));
        }
        missing
    }

    /// Room state before membership: a member is never briefly joined to a
    /// slot the room's state says is closed.
    fn gate_open(&self) -> bool {
        self.state.seen_encryption
            && self.state.seen_members
            && (!self.dialect.reads_slots() || self.state.seen_slots)
    }

    async fn apply_membership(&mut self) {
        let sticky = self.state.sticky.clone().unwrap_or_default();
        let state: Vec<EventIn> = self
            .state
            .membership_state
            .values()
            .flatten()
            .cloned()
            .collect();
        // Capture the deadline before translation or awaiting network work.
        // A membership expiring during either operation gets another pass.
        let sampled_at = crate::executor::Instant::now();
        // Share the process epoch anchor with other local deadlines. Neither
        // wall-clock adjustments nor room reattachment reset this estimate.
        let now_ms = crate::own_membership::now_ms();
        let (current, expiry_in) = self.dialect
            .current_membership_with_expiry_at(&self.room_id, sticky, state, now_ms);
        let next_expiry = expiry_in.map(|delay| sampled_at + delay);
        if let Err(error) = self
            .room
            .lock()
            .await
            .rtc()
            .set_current_sticky_state(current)
            .await
        {
            log::warn!(
                "[{}] the current membership was not applied: {error}",
                self.room_id
            );
            // Keep the failed snapshot dirty so a later delivery retries it,
            // but do not report readiness when it has not been applied.
            // If an expiry already armed the timer, retry with a delay rather
            // than spinning on the expired deadline or losing the wakeup.
            if self.state.membership_expiry.is_some() {
                self.state.membership_expiry = Some(crate::executor::Instant::now()
                    + std::time::Duration::from_secs(1));
            } else {
                // The first failed application can still have a live legacy
                // deadline. Keep its wakeup even before the room is seeded.
                self.state.membership_expiry = next_expiry;
            }
            return;
        }
        self.state.membership_dirty = false;
        self.state.membership_expiry = next_expiry;
        if !*self.seeded.borrow() {
            log::info!("[{}] seeded", self.room_id);
            let _ = self.seeded.send(true);
        }
        self.backfill_relations().await;
    }

    /// The relations the application asks for, fetched without holding the
    /// room's lock. A failed fetch is asked for again on the next apply.
    async fn backfill_relations(&self) {
        let requests = self.room.lock().await.pending_relations();
        for request in requests {
            match self
                .backend
                .relations(
                    self.room_id.clone(),
                    request.event_id.clone(),
                    request.rel_type.clone(),
                    request.event_type.clone(),
                )
                .await
            {
                Ok(events) => {
                    let events: Vec<RawTimelineEvent> = events
                        .into_iter()
                        .filter(|event| event.event_type == request.event_type)
                        .map(|event| to_timeline_event(&self.room_id, event))
                        .collect();
                    self.room
                        .lock()
                        .await
                        .on_relations_received(&request.event_id, &events);
                }
                Err(error) => log::warn!(
                    "[{}] could not fetch the {} relations of {} ({error}); retrying later",
                    self.room_id,
                    request.rel_type,
                    request.event_id,
                ),
            }
        }
    }
}

// ---- To-device keys ----

/// The MSC4143 `m.rtc.encryption_key` content. Anything else in it is ignored.
#[derive(Deserialize)]
struct KeyMessageContent {
    room_id: String,
    member_id: String,
    media_key: MediaKey,
}

#[derive(Deserialize)]
struct MediaKey {
    index: u8,
    key: String,
}

/// A media key from a to-device message, or `None` with the reason logged.
///
/// Read by the dialect of the room the content names, whatever the type: the
/// MSC body and Element Call's legacy body share a type on the wire, so only
/// the room's format can say which one to expect (spec 038 R14, R35).
pub fn parse_key_message(
    dialect_of: impl Fn(&str) -> Option<Arc<dyn IngestDialect>>,
    message: ToDeviceMessageIn,
) -> Option<ReceivedEncryptionKey> {
    if !key_message_fits_queue(&message) {
        log::warn!("Ignoring oversized media key envelope");
        return None;
    }
    let room_id = message
        .content
        .get("room_id")
        .and_then(Value::as_str)
        .unwrap_or_default();
    let Some(dialect) = dialect_of(room_id) else {
        log::debug!(
            "[{room_id}] dropping a {} for a room that is not open",
            message.event_type
        );
        return None;
    };
    dialect.parse_key(&message).filter(crate::encryption::inbound_key_fits_budget)
}

/// A media key in MSC4143's body (`room_id`, `member_id`, `media_key`), the
/// default every [`IngestDialect`] reads.
pub fn parse_msc_key(message: &ToDeviceMessageIn) -> Option<ReceivedEncryptionKey> {
    if !key_message_fits_queue(message) {
        log::warn!("Ignoring oversized MSC media key envelope");
        return None;
    }
    let content: KeyMessageContent = serde_json::from_value(message.content.clone())
        .inspect_err(|error| {
            log::warn!(
                "ignoring a {} from {} whose content is not an MSC4143 key: {error}",
                message.event_type,
                message.sender,
            );
        })
        .ok()?;
    // LiveKit needs 32 bytes (spec 038 R16). Only an MSC room is this strict:
    // Element Call's room-state keys, read by its own parser, may be 16.
    if content.media_key.key.len() > crate::encryption::MAX_ENCODED_MEDIA_KEY_LEN {
        log::warn!("Ignoring oversized MSC media key");
        return None;
    }
    let key_len = MEDIA_KEY_BASE64
        .decode(&content.media_key.key)
        .map(|key| key.len())
        .unwrap_or(0);
    if key_len < MIN_MEDIA_KEY_LEN {
        log::warn!(
            "ignoring a {} from {}: its media key is {key_len} bytes, under {MIN_MEDIA_KEY_LEN}",
            message.event_type,
            message.sender,
        );
        return None;
    }
    Some(ReceivedEncryptionKey {
        origin: key_origin(&message.encryption, &message.sender),
        room_id: content.room_id,
        member_id: content.member_id,
        key_b64: content.media_key.key,
        key_index: content.media_key.index,
    })
}

// The backend callback is synchronous, so it must never block waiting for the
// consumer (which can itself be waiting on a room lock).
const MAX_QUEUED_TO_DEVICE_KEYS: usize = 4096;

// Bound the entire raw envelope, including ignored extension fields and
// sender metadata, rather than just the encoded media key. Counting JSON
// output avoids allocating a second copy of the message before enqueueing.
const MAX_QUEUED_TO_DEVICE_KEY_BYTES: usize = 16 * 1024;

struct EnvelopeSizeBudget(usize);

impl std::io::Write for EnvelopeSizeBudget {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        self.0 = self.0.checked_sub(bytes.len()).ok_or_else(|| {
            std::io::Error::new(std::io::ErrorKind::InvalidData, "RTC envelope exceeds budget")
        })?;
        Ok(bytes.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

pub(crate) fn key_message_fits_queue(message: &ToDeviceMessageIn) -> bool {
    serde_json::to_writer(EnvelopeSizeBudget(MAX_QUEUED_TO_DEVICE_KEY_BYTES), message).is_ok()
}

struct QueuedKey {
    message: ToDeviceMessageIn,
    generation: Weak<()>,
}

struct ChannelToDeviceSink<M> {
    tx: mpsc::Sender<QueuedKey>,
    registry: RoomRegistry<M>,
    event_types: Vec<String>,
    dropped: std::sync::atomic::AtomicU64,
    oversized: std::sync::atomic::AtomicU64,
}

impl<M: crate::MaybeSend + 'static> ToDeviceSink for ChannelToDeviceSink<M> {
    fn on_to_device_message(&self, message: ToDeviceMessageIn) {
        if !self.event_types.contains(&message.event_type) {
            return;
        }
        let Some(room_id) = message.content.get("room_id").and_then(Value::as_str) else {
            return;
        };
        let Some(generation) = self.registry.generation(room_id) else {
            return;
        };
        if !key_message_fits_queue(&message) {
            let count = self.oversized.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
                .wrapping_add(1);
            if count.is_power_of_two() {
                log::warn!("To-device media key envelope too large; {count} messages dropped");
            }
            return;
        }
        if let Err(mpsc::error::TrySendError::Full(_)) = self.tx.try_send(QueuedKey { message, generation }) {
            let count = self.dropped.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
                .wrapping_add(1);
            // Report sustained overflow without one log message per dropped key.
            if count.is_power_of_two() {
                log::warn!("To-device media key queue full; {count} keys dropped");
            }
        }
    }
}

/// The client-wide to-device subscription. Dropping it stops it.
pub struct ToDeviceFeeder {
    subscription: Arc<dyn Subscription>,
    stop: watch::Sender<bool>,
}

impl ToDeviceFeeder {
    /// Subscribes to media keys of the spec's types and of
    /// `extra_key_event_types`, and returns the future that routes each to the
    /// room it names. The caller spawns [`ToDeviceFeederRun::run`].
    pub async fn start<B, M>(
        backend: Arc<B>,
        registry: RoomRegistry<M>,
        extra_key_event_types: Vec<String>,
    ) -> Result<(ToDeviceFeeder, ToDeviceFeederRun<B, M>), BackendError>
    where
        B: MatrixBackend + 'static,
        M: ApplicationIntake<B> + crate::MaybeSend + 'static,
    {
        let (tx, rx) = mpsc::channel(MAX_QUEUED_TO_DEVICE_KEYS);
        let (stop, stopped) = watch::channel(false);
        let mut event_types: Vec<String> =
            KEY_EVENT_TYPES.iter().map(|t| (*t).to_owned()).collect();
        event_types.extend(extra_key_event_types);
        let sink: Arc<dyn ToDeviceSink> = Arc::new(ChannelToDeviceSink {
            tx,
            registry: registry.clone(),
            event_types: event_types.clone(),
            dropped: std::sync::atomic::AtomicU64::new(0),
            oversized: std::sync::atomic::AtomicU64::new(0),
        });
        let subscription = backend
            .subscribe_to_device(event_types.clone(), sink)
            .await?;
        Ok((
            ToDeviceFeeder {
                subscription,
                stop,
            },
            ToDeviceFeederRun {
                registry,
                event_types,
                rx,
                stopped,
                _backend: backend,
            },
        ))
    }

    pub fn stop(self) {
        drop(self);
    }
}

impl Drop for ToDeviceFeeder {
    fn drop(&mut self) {
        self.subscription.cancel();
        let _ = self.stop.send(true);
    }
}

/// The future that routes media keys. Ends on stop.
pub struct ToDeviceFeederRun<B, M> {
    registry: RoomRegistry<M>,
    event_types: Vec<String>,
    rx: mpsc::Receiver<QueuedKey>,
    stopped: watch::Receiver<bool>,
    _backend: Arc<B>,
}

impl<B, M> ToDeviceFeederRun<B, M>
where
    B: MatrixBackend + 'static,
    M: ApplicationIntake<B>,
{
    pub async fn run(mut self) {
        loop {
            if *self.stopped.borrow() {
                break;
            }
            let message = tokio::select! {
                biased;
                _ = self.stopped.changed() => break,
                message = self.rx.recv() => match message {
                    Some(message) => message,
                    None => break,
                },
            };
            let QueuedKey { message, generation } = message;
            if !self.event_types.contains(&message.event_type) {
                continue;
            }
            let Some(room_id) = message.content.get("room_id").and_then(Value::as_str) else {
                continue;
            };
            if self.registry.room_for_generation(room_id, &generation).is_none() {
                continue;
            }
            let Some(key) = parse_key_message(|room_id| self.registry.dialect(room_id), message)
            else {
                continue;
            };
            // A key for a room no live room object holds is dropped: nothing
            // could use it, and keeping it would be state spanning rooms.
            let Some(room) = self.registry.room_for_generation(&key.room_id, &generation) else {
                log::debug!(
                    "[{}] dropping a media key for a room that is not open",
                    key.room_id
                );
                continue;
            };
            let mut room = tokio::select! {
                biased;
                _ = self.stopped.changed() => break,
                room = room.lock() => room,
            };
            if *self.stopped.borrow() {
                break;
            }
            // A room could have closed or reopened while we waited for its
            // async lock. Never route that queued event into the new registration.
            if self.registry.room_for_generation(&key.room_id, &generation).is_none() {
                continue;
            }
            // Do not cancel an admitted receive half way through key application.
            if let Err(error) = room.rtc().receive_encryption_key(key).await {
                log::warn!("a media key was rejected: {error}");
            }
        }
        log::info!("to-device feeder stopped");
    }
}
