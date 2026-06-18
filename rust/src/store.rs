//! A minimal in-process model store — the seed of a transports session.
//!
//! [`Store::host`] takes ownership of a model value and assigns it a [`ModelId`] and `rev` 0.
//! [`Store::mutate`] replaces a hosted value, **diffs** old→new, bumps the `rev`, and returns the
//! [`Patch`] to send to peers — this is the reactive core: a field change becomes an incremental
//! update with no whole-model resend. [`Store::apply`] applies a peer's patch to a mirrored model.
//!
//! Full sessions — fan-out to many subscribers, backpressure, authorization — are Phase 4; this is
//! the single-owner nucleus they grow from.

use std::collections::BTreeMap;

use crate::diff::{apply as apply_patch, diff, Patch};
use crate::value::{ModelId, Value};

struct Held {
    type_name: String,
    value: Value,
    rev: u64,
}

/// Holds hosted/mirrored models by id.
#[derive(Default)]
pub struct Store {
    models: BTreeMap<ModelId, Held>,
    next_id: u64,
}

impl Store {
    pub fn new() -> Store {
        Store {
            models: BTreeMap::new(),
            next_id: 1,
        }
    }

    /// Host a new model value; returns its freshly assigned id (rev starts at 0).
    pub fn host(&mut self, type_name: impl Into<String>, value: Value) -> ModelId {
        let id = ModelId(self.next_id);
        self.next_id += 1;
        self.models.insert(
            id,
            Held {
                type_name: type_name.into(),
                value,
                rev: 0,
            },
        );
        id
    }

    /// The current `(type_name, value, rev)` of a hosted model.
    pub fn snapshot(&self, id: ModelId) -> Option<(&str, &Value, u64)> {
        self.models
            .get(&id)
            .map(|h| (h.type_name.as_str(), &h.value, h.rev))
    }

    /// Replace a hosted model's value, returning the patch (with the new `rev`) that expresses the
    /// change. Returns `None` if `id` is unknown, or an empty patch (still rev-bumped) if unchanged.
    pub fn mutate(&mut self, id: ModelId, value: Value) -> Option<Patch> {
        let held = self.models.get_mut(&id)?;
        let mut patch = diff(&held.value, &value);
        held.value = value;
        held.rev += 1;
        patch.rev = held.rev;
        Some(patch)
    }

    /// Apply a peer's patch to a mirrored model, adopting the patch's `rev`.
    pub fn apply(&mut self, id: ModelId, patch: &Patch) -> bool {
        match self.models.get_mut(&id) {
            Some(held) => {
                apply_patch(&mut held.value, patch);
                held.rev = patch.rev;
                true
            }
            None => false,
        }
    }

    pub fn contains(&self, id: ModelId) -> bool {
        self.models.contains_key(&id)
    }

    pub fn len(&self) -> usize {
        self.models.len()
    }

    pub fn is_empty(&self) -> bool {
        self.models.is_empty()
    }
}

/*********************************/
#[cfg(test)]
mod store_tests {
    use super::*;

    #[test]
    fn test_host_and_snapshot() {
        let mut s = Store::new();
        let id = s.host("Device", Value::map([("on", Value::from(false))]));
        let (ty, val, rev) = s.snapshot(id).unwrap();
        assert_eq!(ty, "Device");
        assert_eq!(val, &Value::map([("on", Value::from(false))]));
        assert_eq!(rev, 0);
    }

    #[test]
    fn test_mutate_produces_incremental_patch_and_bumps_rev() {
        let mut s = Store::new();
        let id = s.host(
            "Device",
            Value::map([("on", Value::from(false)), ("name", Value::from("lamp"))]),
        );
        let patch = s
            .mutate(
                id,
                Value::map([("on", Value::from(true)), ("name", Value::from("lamp"))]),
            )
            .unwrap();
        assert_eq!(patch.len(), 1); // only `on` changed — not a whole-model resend
        assert_eq!(patch.rev, 1);
        assert_eq!(s.snapshot(id).unwrap().2, 1);
    }

    #[test]
    fn test_mirror_via_apply() {
        // host on one store, mirror on another by replaying snapshot + patch
        let mut server = Store::new();
        let id = server.host("Device", Value::map([("on", Value::from(false))]));

        let mut client = Store::new();
        let (ty, val, rev) = server.snapshot(id).unwrap();
        let cid = client.host(ty, val.clone());
        assert_eq!(rev, 0);

        let patch = server
            .mutate(id, Value::map([("on", Value::from(true))]))
            .unwrap();
        client.apply(cid, &patch);
        assert_eq!(
            client.snapshot(cid).unwrap().1,
            &Value::map([("on", Value::from(true))])
        );
        assert_eq!(client.snapshot(cid).unwrap().2, 1);
    }

    #[test]
    fn test_mutate_unknown_id() {
        let mut s = Store::new();
        assert!(s.mutate(ModelId(999), Value::Null).is_none());
    }
}
