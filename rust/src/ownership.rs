//! Single-server field ownership, fenced writes, and acknowledged ordered commands.

use crate::{Op, Patch, PathSeg, Value};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet, VecDeque};

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "t", rename_all = "snake_case")]
pub enum OwnershipMessage {
    Ownership {
        id: u64,
        fields: Vec<String>,
        owned: bool,
        epoch: u64,
    },
    Owned {
        id: u64,
        epoch: u64,
        sequence: u64,
        patch: Patch,
    },
    OwnedAck {
        id: u64,
        epoch: u64,
        sequence: u64,
        rev: u64,
    },
    Command {
        id: u64,
        epoch: u64,
        command: u64,
        field: String,
        value: Value,
    },
    CommandAck {
        id: u64,
        epoch: u64,
        command: u64,
    },
}

#[derive(Clone)]
struct Command {
    id: u64,
    field: String,
    value: Value,
}

/// One model's client-owned fields share one live connection owner.
pub struct FieldAuthority {
    id: u64,
    fields: BTreeSet<String>,
    connections: Vec<String>,
    epoch: u64,
    sequence: u64,
    commands: VecDeque<Command>,
    next_command: u64,
    delivered: bool,
    command_cap: usize,
}

impl FieldAuthority {
    pub fn new(id: u64, fields: Vec<String>, command_cap: usize) -> Result<Self, String> {
        if fields.is_empty() || fields.iter().any(|field| field.is_empty()) {
            return Err("client-owned fields must be nonempty names".into());
        }
        Ok(Self {
            id,
            fields: fields.into_iter().collect(),
            connections: Vec::new(),
            epoch: 0,
            sequence: 0,
            commands: VecDeque::new(),
            next_command: 1,
            delivered: false,
            command_cap,
        })
    }

    pub fn open(&mut self, connection: &str) -> OwnershipMessage {
        if !self.connections.iter().any(|c| c == connection) {
            if self.connections.is_empty() {
                self.epoch += 1;
                self.sequence = 0;
            }
            self.connections.push(connection.into());
        }
        self.grant(connection)
    }

    pub fn grant(&self, connection: &str) -> OwnershipMessage {
        OwnershipMessage::Ownership {
            id: self.id,
            fields: self.fields.iter().cloned().collect(),
            owned: self.owner() == Some(connection),
            epoch: self.epoch,
        }
    }

    pub fn owner(&self) -> Option<&str> {
        self.connections.first().map(String::as_str)
    }

    pub fn close(&mut self, connection: &str) -> bool {
        let was_owner = self.owner() == Some(connection);
        self.connections.retain(|c| c != connection);
        if was_owner {
            self.epoch += 1;
            self.sequence = 0;
            self.delivered = false;
        }
        was_owner
    }

    pub fn validate(
        &self,
        connection: &str,
        epoch: u64,
        sequence: u64,
        patch: &Patch,
    ) -> Result<bool, String> {
        if self.owner() != Some(connection) || epoch != self.epoch {
            return Err("connection does not own these fields at this epoch".into());
        }
        self.validate_ops(&patch.ops)?;
        Ok(sequence > self.sequence)
    }

    pub fn validate_ops(&self, ops: &[Op]) -> Result<(), String> {
        if ops.is_empty() {
            return Err("client-owned edits require at least one Set operation".into());
        }
        for op in ops {
            match op {
                Op::Set { path, .. } if matches!(path.as_slice(), [PathSeg::Key(key)] if self.fields.contains(key)) =>
                    {}
                _ => return Err("client-owned edits require whole-field Set operations".into()),
            }
        }
        Ok(())
    }

    pub fn commit(&mut self, sequence: u64) {
        self.sequence = sequence;
    }

    pub fn command(&mut self, field: &str, value: Value) -> Result<u64, String> {
        if !self.fields.contains(field) {
            return Err("command target is not client-owned".into());
        }
        if self.commands.len() >= self.command_cap {
            return Err("command queue is full".into());
        }
        let id = self.next_command;
        self.next_command += 1;
        self.commands.push_back(Command {
            id,
            field: field.into(),
            value,
        });
        Ok(id)
    }

    pub fn next_command(&mut self) -> Option<OwnershipMessage> {
        if self.delivered || self.owner().is_none() {
            return None;
        }
        let command = self.commands.front()?;
        self.delivered = true;
        Some(OwnershipMessage::Command {
            id: self.id,
            epoch: self.epoch,
            command: command.id,
            field: command.field.clone(),
            value: command.value.clone(),
        })
    }

    pub fn acknowledge(
        &mut self,
        connection: &str,
        epoch: u64,
        command: u64,
    ) -> Result<(), String> {
        if self.owner() != Some(connection) || epoch != self.epoch {
            return Err("command acknowledgement is from an obsolete owner".into());
        }
        if self
            .commands
            .front()
            .is_some_and(|front| front.id == command)
            && self.delivered
        {
            self.commands.pop_front();
            self.delivered = false;
            return Ok(());
        }
        if command
            < self
                .commands
                .front()
                .map_or(self.next_command, |front| front.id)
        {
            return Ok(());
        }
        Err("command acknowledgement is out of order".into())
    }
}

#[derive(Default)]
struct Grant {
    fields: BTreeSet<String>,
    owned: bool,
    epoch: u64,
    sequence: u64,
}

/// Shared client rules. Host-language adapters own timers and I/O, never authority decisions.
#[derive(Default)]
pub struct OwnedFields {
    grants: BTreeMap<u64, Grant>,
    pending: BTreeMap<u64, BTreeMap<String, Value>>,
}

impl OwnedFields {
    pub fn receive(&mut self, message: &OwnershipMessage) -> bool {
        match message {
            OwnershipMessage::Ownership {
                id,
                fields,
                owned,
                epoch,
            } => {
                let grant = self.grants.entry(*id).or_default();
                if *epoch < grant.epoch {
                    return false;
                }
                if *epoch != grant.epoch || !*owned {
                    grant.sequence = 0;
                    self.pending.remove(id);
                }
                grant.fields = fields.iter().cloned().collect();
                grant.owned = *owned;
                grant.epoch = *epoch;
                true
            }
            OwnershipMessage::Command {
                id, epoch, field, ..
            } => self.grants.get(id).is_some_and(|grant| {
                grant.owned && grant.epoch == *epoch && grant.fields.contains(field)
            }),
            OwnershipMessage::OwnedAck {
                id,
                epoch,
                sequence,
                ..
            } => self.grants.get(id).is_some_and(|grant| {
                grant.owned && grant.epoch == *epoch && *sequence <= grant.sequence
            }),
            _ => false,
        }
    }

    pub fn owns(&self, id: u64, field: &str) -> bool {
        self.grants
            .get(&id)
            .is_some_and(|grant| grant.owned && grant.fields.contains(field))
    }

    /// Classify a patch. Mixing server and client authority in one edit is rejected atomically.
    pub fn classify(&self, id: u64, ops: &[Op]) -> Result<bool, String> {
        let Some(grant) = self.grants.get(&id) else {
            return Ok(false);
        };
        let mut owned = false;
        let mut server = false;
        for op in ops {
            let path = match op {
                Op::Set { path, .. }
                | Op::Remove { path }
                | Op::Insert { path, .. }
                | Op::RemoveAt { path, .. }
                | Op::Move { path, .. }
                | Op::Reorder { path, .. } => path,
            };
            if path.is_empty() {
                return Err("edit fields individually on models with client-owned fields".into());
            }
            if matches!(path.first(), Some(PathSeg::Key(field)) if grant.fields.contains(field)) {
                if !grant.owned {
                    return Err("client does not own this field".into());
                }
                if !matches!(op, Op::Set { path, .. } if path.len() == 1) {
                    return Err("client-owned edits require whole-field Set operations".into());
                }
                owned = true;
            } else {
                server = true;
            }
        }
        if owned && server {
            return Err("split edits by field ownership".into());
        }
        Ok(owned)
    }

    pub fn edit(&mut self, id: u64, ops: Vec<Op>) -> Result<OwnershipMessage, String> {
        if !self.classify(id, &ops)? {
            return Err("edit has no client-owned fields".into());
        }
        let mut fields = self.pending.remove(&id).unwrap_or_default();
        for op in ops {
            if let Op::Set { path, value } = op {
                if let PathSeg::Key(key) = &path[0] {
                    fields.insert(key.clone(), value);
                }
            }
        }
        let ops = fields
            .into_iter()
            .map(|(key, value)| Op::Set {
                path: vec![PathSeg::Key(key)],
                value,
            })
            .collect();
        let grant = self.grants.get_mut(&id).expect("classified owned fields");
        grant.sequence += 1;
        Ok(OwnershipMessage::Owned {
            id,
            epoch: grant.epoch,
            sequence: grant.sequence,
            patch: Patch { rev: 0, ops },
        })
    }

    pub fn disconnect(&mut self) {
        self.pending.clear();
        for grant in self.grants.values_mut() {
            grant.owned = false;
            grant.epoch = 0;
            grant.sequence = 0;
        }
    }
}

impl FieldAuthority {
    pub fn from_json(id: u64, fields: &str, command_cap: usize) -> Result<Self, String> {
        Self::new(
            id,
            serde_json::from_str(fields).map_err(|e| e.to_string())?,
            command_cap,
        )
    }
    pub fn open_json(&mut self, connection: &str) -> Result<String, String> {
        serde_json::to_string(&self.open(connection)).map_err(|e| e.to_string())
    }
    pub fn grant_json(&self, connection: &str) -> Result<String, String> {
        serde_json::to_string(&self.grant(connection)).map_err(|e| e.to_string())
    }
    pub fn command_json(&mut self, field: &str, value: &str) -> Result<u64, String> {
        self.command(
            field,
            serde_json::from_str(value).map_err(|e| e.to_string())?,
        )
    }
    pub fn next_json(&mut self) -> Result<Option<String>, String> {
        self.next_command()
            .map(|message| serde_json::to_string(&message).map_err(|e| e.to_string()))
            .transpose()
    }
    pub fn validate_json(&self, connection: &str, message: &str) -> Result<bool, String> {
        let message: OwnershipMessage = serde_json::from_str(message).map_err(|e| e.to_string())?;
        match message {
            OwnershipMessage::Owned {
                id,
                epoch,
                sequence,
                patch,
            } if id == self.id => self.validate(connection, epoch, sequence, &patch),
            _ => Err("expected an owned edit for this model".into()),
        }
    }
}

impl OwnedFields {
    pub fn receive_json(&mut self, message: &str) -> Result<bool, String> {
        Ok(self.receive(&serde_json::from_str(message).map_err(|e| e.to_string())?))
    }
    pub fn classify_json(&self, id: u64, ops: &str) -> Result<bool, String> {
        self.classify(
            id,
            &serde_json::from_str::<Vec<Op>>(ops).map_err(|e| e.to_string())?,
        )
    }
    pub fn server_ops_json(&self, id: u64, ops: &str) -> Result<String, String> {
        let mut ops: Vec<Op> = serde_json::from_str(ops).map_err(|e| e.to_string())?;
        if let Some(grant) = self.grants.get(&id) {
            ops.retain(|op| {
                let path = match op {
                    Op::Set { path, .. }
                    | Op::Remove { path }
                    | Op::Insert { path, .. }
                    | Op::RemoveAt { path, .. }
                    | Op::Move { path, .. }
                    | Op::Reorder { path, .. } => path,
                };
                matches!(path.first(), Some(PathSeg::Key(key)) if !grant.fields.contains(key))
            });
        }
        serde_json::to_string(&ops).map_err(|e| e.to_string())
    }
    pub fn edit_json(&mut self, id: u64, ops: &str) -> Result<String, String> {
        let message = self.edit(id, serde_json::from_str(ops).map_err(|e| e.to_string())?)?;
        serde_json::to_string(&message).map_err(|e| e.to_string())
    }
}

impl OwnedFields {
    pub fn queue_json(&mut self, id: u64, ops_json: &str) -> Result<String, String> {
        let ops: Vec<Op> = serde_json::from_str(ops_json).map_err(|e| e.to_string())?;
        let message = self.edit(id, ops)?;
        let OwnershipMessage::Owned { patch, .. } = &message else {
            unreachable!()
        };
        let pending = self.pending.entry(id).or_default();
        for op in &patch.ops {
            if let Op::Set { path, value } = op {
                if let PathSeg::Key(key) = &path[0] {
                    pending.insert(key.clone(), value.clone());
                }
            }
        }
        serde_json::to_string(&message).map_err(|e| e.to_string())
    }

    pub fn drain_json(&mut self) -> Result<String, String> {
        let mut messages = Vec::new();
        for (id, fields) in std::mem::take(&mut self.pending) {
            let grant = &self.grants[&id];
            if !grant.owned {
                continue;
            }
            messages.push(OwnershipMessage::Owned {
                id,
                epoch: grant.epoch,
                sequence: grant.sequence,
                patch: Patch {
                    rev: 0,
                    ops: fields
                        .into_iter()
                        .map(|(key, value)| Op::Set {
                            path: vec![PathSeg::Key(key)],
                            value,
                        })
                        .collect(),
                },
            });
        }
        serde_json::to_string(&messages).map_err(|e| e.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn edit(sequence: u64) -> Patch {
        Patch {
            rev: 0,
            ops: vec![Op::Set {
                path: vec![PathSeg::Key("playing".into())],
                value: Value::Bool(sequence % 2 == 0),
            }],
        }
    }

    #[test]
    fn commands_queue_before_connect_and_only_one_owner_receives_them() {
        let mut authority = FieldAuthority::new(1, vec!["playing".into()], 3).unwrap();
        for value in [true, false, true] {
            authority.command("playing", Value::Bool(value)).unwrap();
        }
        assert!(authority.command("playing", Value::Bool(false)).is_err());
        assert!(authority.next_command().is_none());
        assert!(matches!(
            authority.open("first"),
            OwnershipMessage::Ownership { owned: true, .. }
        ));
        assert!(matches!(
            authority.open("second"),
            OwnershipMessage::Ownership { owned: false, .. }
        ));
        let Some(OwnershipMessage::Command {
            command,
            epoch,
            value,
            ..
        }) = authority.next_command()
        else {
            panic!()
        };
        assert_eq!(value, Value::Bool(true));
        assert!(authority.next_command().is_none());
        assert!(authority.acknowledge("second", epoch, command).is_err());
        assert!(authority.acknowledge("first", epoch, command + 1).is_err());
        authority.acknowledge("first", epoch, command).unwrap();
        let Some(OwnershipMessage::Command {
            command: second,
            value,
            ..
        }) = authority.next_command()
        else {
            panic!()
        };
        assert_eq!(value, Value::Bool(false));
        assert!(authority.close("first"));
        assert_eq!(authority.owner(), Some("second"));
        assert!(authority.validate("first", epoch, 1, &edit(1)).is_err());
        assert!(authority.acknowledge("first", epoch, second).is_err());
        let Some(OwnershipMessage::Command {
            command,
            epoch: new_epoch,
            ..
        }) = authority.next_command()
        else {
            panic!()
        };
        assert_eq!(command, second);
        assert!(new_epoch > epoch);
        authority.acknowledge("second", new_epoch, command).unwrap();
        assert!(matches!(
            authority.next_command(),
            Some(OwnershipMessage::Command { command: 3, .. })
        ));
    }

    #[test]
    fn immediate_edits_flush_pending_fields_and_replace_older_values() {
        let mut authority =
            FieldAuthority::new(1, vec!["playing".into(), "beat".into()], 3).unwrap();
        let mut client = OwnedFields::default();
        client.receive(&authority.open("owner"));
        client
            .queue_json(1, &serde_json::to_string(&edit(2).ops).unwrap())
            .unwrap();
        let beat = Op::Set {
            path: vec![PathSeg::Key("beat".into())],
            value: Value::Int(4),
        };
        client
            .queue_json(1, &serde_json::to_string(&vec![beat.clone()]).unwrap())
            .unwrap();
        let OwnershipMessage::Owned {
            patch, sequence, ..
        } = client.edit(1, edit(1).ops).unwrap()
        else {
            panic!()
        };
        assert_eq!(sequence, 3);
        assert_eq!(patch.ops, vec![beat, edit(1).ops.remove(0)]);
        assert_eq!(client.drain_json().unwrap(), "[]");
    }

    #[test]
    fn reconnect_accepts_a_new_servers_epoch() {
        let mut client = OwnedFields::default();
        let grant = |epoch| OwnershipMessage::Ownership {
            id: 1,
            fields: vec!["playing".into()],
            owned: true,
            epoch,
        };
        assert!(client.receive(&grant(8)));
        assert!(!client.receive(&grant(1)));
        client.disconnect();
        assert!(client.receive(&grant(1)));
        assert!(client.owns(1, "playing"));
        assert!(matches!(
            client.edit(1, edit(1).ops).unwrap(),
            OwnershipMessage::Owned { sequence: 1, .. }
        ));
    }

    #[test]
    fn client_coalesces_status_without_proposals_and_fences_obsolete_edits() {
        let mut authority = FieldAuthority::new(1, vec!["playing".into()], 3).unwrap();
        let mut client = OwnedFields::default();
        client.receive(&authority.open("first"));
        for sequence in 1..=1000 {
            client
                .queue_json(1, &serde_json::to_string(&edit(sequence).ops).unwrap())
                .unwrap();
        }
        let messages: Vec<OwnershipMessage> =
            serde_json::from_str(&client.drain_json().unwrap()).unwrap();
        assert_eq!(messages.len(), 1);
        let OwnershipMessage::Owned {
            epoch,
            sequence,
            patch,
            ..
        } = &messages[0]
        else {
            panic!()
        };
        assert_eq!(*sequence, 1000);
        assert_eq!(patch.ops, edit(1000).ops);
        assert!(authority
            .validate("first", *epoch, *sequence, patch)
            .unwrap());
        authority.commit(*sequence);
        assert!(!authority
            .validate("first", *epoch, *sequence, patch)
            .unwrap());
        assert_eq!(client.drain_json().unwrap(), "[]");
        client
            .queue_json(1, &serde_json::to_string(&edit(1).ops).unwrap())
            .unwrap();
        client.disconnect();
        assert!(!client.owns(1, "playing"));
        assert_eq!(client.drain_json().unwrap(), "[]");
        assert!(client.edit(1, edit(1).ops).is_err());
    }
}
