// Call-specific recipient gate. Generic MatrixRTC callers do not opt in.
use std::{collections::HashMap, sync::{Arc, OnceLock, atomic::{AtomicBool, Ordering}}};

/// Exact keys of one independently admitted Matrix device.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CallRecipientKeys {
    pub signing: String,
    pub identity: String,
    pub master: Option<String>,
}

#[derive(Debug)]
struct GroupScope {
    generation: u64,
    allowed: HashMap<String, String>,
    admitted: OnceLock<HashMap<String, CallRecipientKeys>>,
}

#[derive(Clone, Debug)]
pub struct CallKeyRecipients(Arc<Scope>);

#[derive(Debug)]
struct Scope {
    user: String,
    expected_device: Option<String>,
    pinned: OnceLock<String>,
    revoked: AtomicBool,
    group: Option<GroupScope>,
}

impl CallKeyRecipients {
    pub fn new(user: String, expected_device: Option<String>) -> Self {
        Self(Arc::new(Scope { user, expected_device, pinned: OnceLock::new(), revoked: AtomicBool::new(false), group: None }))
    }

    /// A group scope starts closed, even for the expected devices. Only an
    /// independently verified complete admission can activate it. Never reuse
    /// a scope for a later roster or for a different join generation.
    pub fn new_group(generation: u64, peers: Vec<(String, String)>) -> Option<Self> {
        if generation == 0 || peers.is_empty() || peers.len() > 33 { return None; }
        let mut allowed = HashMap::new();
        for (user, device) in peers {
            if user.is_empty() || device.is_empty() || allowed.insert(user, device).is_some() {
                return None;
            }
        }
        Some(Self(Arc::new(Scope { user: String::new(), expected_device: None,
            pinned: OnceLock::new(), revoked: AtomicBool::new(false),
            group: Some(GroupScope { generation, allowed, admitted: OnceLock::new() }) })))
    }

    pub fn activate_group(&self, generation: u64,
        peers: Vec<(String, String, CallRecipientKeys)>) -> bool {
        let Some(group) = &self.0.group else { return false; };
        if generation != group.generation || peers.len() != group.allowed.len()
            || self.0.revoked.load(Ordering::Acquire) { return false; }
        let mut admitted = HashMap::new();
        for (user, device, keys) in peers {
            if group.allowed.get(&user) != Some(&device) || keys.signing.is_empty()
                || keys.identity.is_empty() || keys.master.as_ref().is_some_and(|master| master.is_empty())
                || admitted.insert(user, keys).is_some() { return false; }
        }
        let chosen = group.admitted.get_or_init(|| admitted.clone());
        chosen == &admitted && !self.0.revoked.load(Ordering::Acquire)
    }

    /// Called after the fresh SDK device query, not merely before it. A
    /// replacement key for an admitted MXID/device pair is not a recipient.
    pub fn allows_keys(&self, user: &str, device: &str, keys: &CallRecipientKeys) -> bool {
        self.allows(user, device) && match &self.0.group {
            None => true,
            Some(group) => group.admitted.get().and_then(|pins| pins.get(user)) == Some(keys),
        }
    }

    pub fn is_group(&self) -> bool { self.0.group.is_some() }

    pub fn same_scope(&self, other: &Self) -> bool { Arc::ptr_eq(&self.0, &other.0) }

    pub fn group_active(&self, generation: u64) -> bool {
        self.0.group.as_ref().is_some_and(|group| group.generation == generation
            && group.admitted.get().is_some()) && !self.0.revoked.load(Ordering::Acquire)
    }

    pub fn is_revoked(&self) -> bool { self.0.revoked.load(Ordering::Acquire) }


    /// Pin only after the bridge independently admits the membership and device.
    /// Never replace a pin within the same call, including after revocation.
    pub fn pin(&self, user: &str, device: &str) -> bool {
        if self.0.group.is_some() || user.is_empty() || device.is_empty() || self.0.user != user
            || self.0.revoked.load(Ordering::Acquire)
            || self.0.expected_device.as_deref().is_some_and(|expected| expected != device) {
            return false;
        }
        let chosen = self.0.pinned.get_or_init(|| device.to_owned());
        chosen == device && !self.0.revoked.load(Ordering::Acquire)
    }

    pub fn allows(&self, user: &str, device: &str) -> bool {
        if self.0.revoked.load(Ordering::Acquire) { return false; }
        let allowed = match &self.0.group {
            Some(group) => group.allowed.get(user).is_some_and(|chosen| chosen == device)
                && group.admitted.get().is_some_and(|pins| pins.contains_key(user)),
            None => self.0.user == user && self.0.pinned.get().is_some_and(|chosen| chosen == device),
        };
        allowed && !self.0.revoked.load(Ordering::Acquire)
    }

    pub fn revoke(&self) {
        self.0.revoked.store(true, Ordering::Release);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn incoming_fails_closed_until_pin_and_never_repins() {
        let gate = CallKeyRecipients::new("@human:example.org".into(), None);
        let copy = gate.clone();
        assert!(!copy.allows("@human:example.org", "PHONE"));
        assert!(!gate.pin("@other:example.org", "PHONE"));
        assert!(!gate.pin("@human:example.org", ""));
        assert!(gate.pin("@human:example.org", "PHONE"));
        assert!(copy.allows("@human:example.org", "PHONE"));
        assert!(!copy.allows("@other:example.org", "PHONE"));
        assert!(!gate.pin("@human:example.org", "LAPTOP"));
        assert!(!copy.allows("@human:example.org", "LAPTOP"));
        copy.revoke();
        assert!(!gate.allows("@human:example.org", "PHONE"));
        assert!(!gate.pin("@human:example.org", "PHONE"));
    }

    #[test]
    fn outgoing_requires_the_inviting_device_and_call_scopes_are_independent() {
        let gate = CallKeyRecipients::new("@human:example.org".into(), Some("PHONE".into()));
        let other = CallKeyRecipients::new("@human:example.org".into(), None);
        assert!(!gate.pin("@human:example.org", "LAPTOP"));
        assert!(gate.pin("@human:example.org", "PHONE"));
        assert!(!other.allows("@human:example.org", "PHONE"));
        assert!(other.pin("@human:example.org", "LAPTOP"));
        assert!(!gate.allows("@human:example.org", "LAPTOP"));
        assert!(other.allows("@human:example.org", "LAPTOP"));
    }

    #[test]
    fn concurrent_first_pin_has_one_winner() {
        let gate = CallKeyRecipients::new("@human:example.org".into(), None);
        let a = gate.clone();
        let b = gate.clone();
        let first = std::thread::spawn(move || a.pin("@human:example.org", "A"));
        let second = std::thread::spawn(move || b.pin("@human:example.org", "B"));
        let (a, b) = (first.join().unwrap(), second.join().unwrap());
        assert_ne!(a, b);
        assert_eq!(gate.allows("@human:example.org", "A"), a);
        assert_eq!(gate.allows("@human:example.org", "B"), b);
    }
}
