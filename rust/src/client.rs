//! Transport-neutral client state for Python, JavaScript, and future bindings.

use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};

use crate::{Message, Op, Patch};

/// A state transition prepared from one inbound message.
///
/// Bindings apply snapshot values or patches locally before committing the effect. This keeps a
/// malformed patch from advancing the shared revision state while allowing JavaScript to retain
/// structural sharing in its mirror.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "effect", rename_all = "snake_case")]
pub enum ClientEffect {
    Snapshot {
        id: u64,
        rev: u64,
    },
    Patch {
        id: u64,
        rev: u64,
        #[serde(skip_serializing_if = "Option::is_none")]
        proposal: Option<String>,
    },
    StalePatch {
        id: u64,
        rev: u64,
        #[serde(skip_serializing_if = "Option::is_none")]
        proposal: Option<String>,
    },
    Acknowledgement {
        id: u64,
        rev: u64,
        proposal: String,
    },
    Rejection {
        id: u64,
        rev: u64,
        error: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        proposal: Option<String>,
    },
    Ignore,
    Disconnect {
        proposals: Vec<String>,
    },
}

impl ClientEffect {
    pub fn from_json(json: &str) -> Result<Self, String> {
        serde_json::from_str(json).map_err(|error| error.to_string())
    }

    pub fn to_json(&self) -> Result<String, String> {
        serde_json::to_string(self).map_err(|error| error.to_string())
    }
}

/// Revisions and proposal lifecycle shared by every client binding.
#[derive(Default)]
pub struct ClientState {
    revisions: BTreeMap<u64, u64>,
    pending: BTreeSet<String>,
    next_proposal: u64,
}

#[derive(Deserialize)]
#[serde(tag = "t")]
enum ClientMessage {
    #[serde(rename = "snapshot")]
    Snapshot { id: u64, rev: u64 },
    #[serde(rename = "patch")]
    Patch {
        id: u64,
        patch: PatchRevision,
        proposal: Option<String>,
    },
    #[serde(rename = "ack")]
    Ack { id: u64, rev: u64, proposal: String },
    #[serde(rename = "reject")]
    Reject {
        id: u64,
        rev: u64,
        error: String,
        proposal: Option<String>,
    },
    #[serde(rename = "batch")]
    Batch,
    #[serde(other)]
    Unknown,
}

#[derive(Deserialize)]
struct PatchRevision {
    rev: u64,
}

impl ClientState {
    pub fn new() -> Self {
        Self {
            revisions: BTreeMap::new(),
            pending: BTreeSet::new(),
            next_proposal: 1,
        }
    }

    /// Classify one non-batch message without changing state.
    pub fn prepare(&self, message: &Message) -> Result<ClientEffect, String> {
        match message {
            Message::Snapshot { id, rev, .. } => Ok(ClientEffect::Snapshot { id: *id, rev: *rev }),
            Message::Patch {
                id,
                patch,
                proposal,
            } => match self.revisions.get(id) {
                Some(seen) if patch.rev <= *seen => Ok(ClientEffect::StalePatch {
                    id: *id,
                    rev: patch.rev,
                    proposal: proposal.clone(),
                }),
                Some(_) => Ok(ClientEffect::Patch {
                    id: *id,
                    rev: patch.rev,
                    proposal: proposal.clone(),
                }),
                None => Err(format!("patch received before snapshot for model {id}")),
            },
            Message::Ack { id, rev, proposal } => Ok(ClientEffect::Acknowledgement {
                id: *id,
                rev: *rev,
                proposal: proposal.clone(),
            }),
            Message::Reject {
                id,
                rev,
                error,
                proposal,
            } => Ok(ClientEffect::Rejection {
                id: *id,
                rev: *rev,
                error: error.clone(),
                proposal: proposal.clone(),
            }),
            Message::Unknown(_) => Ok(ClientEffect::Ignore),
            Message::Batch { .. } => Err("batch messages must be prepared in order".into()),
        }
    }

    pub fn prepare_json(&self, message_json: &str) -> Result<String, String> {
        let message: ClientMessage =
            serde_json::from_str(message_json).map_err(|error| error.to_string())?;
        let effect = match message {
            ClientMessage::Snapshot { id, rev } => ClientEffect::Snapshot { id, rev },
            ClientMessage::Patch {
                id,
                patch,
                proposal,
            } => match self.revisions.get(&id) {
                Some(seen) if patch.rev <= *seen => ClientEffect::StalePatch {
                    id,
                    rev: patch.rev,
                    proposal,
                },
                Some(_) => ClientEffect::Patch {
                    id,
                    rev: patch.rev,
                    proposal,
                },
                None => return Err(format!("patch received before snapshot for model {id}")),
            },
            ClientMessage::Ack { id, rev, proposal } => {
                ClientEffect::Acknowledgement { id, rev, proposal }
            }
            ClientMessage::Reject {
                id,
                rev,
                error,
                proposal,
            } => ClientEffect::Rejection {
                id,
                rev,
                error,
                proposal,
            },
            ClientMessage::Unknown => ClientEffect::Ignore,
            ClientMessage::Batch => return Err("batch messages must be prepared in order".into()),
        };
        effect.to_json()
    }

    /// Commit an effect after the binding has applied its local value change successfully.
    pub fn commit(&mut self, effect: &ClientEffect) {
        match effect {
            ClientEffect::Snapshot { id, rev } | ClientEffect::Patch { id, rev, .. } => {
                self.revisions.insert(*id, *rev);
            }
            _ => {}
        }
        match effect {
            ClientEffect::Patch { proposal, .. }
            | ClientEffect::StalePatch { proposal, .. }
            | ClientEffect::Rejection { proposal, .. } => {
                if let Some(proposal) = proposal {
                    self.pending.remove(proposal);
                }
            }
            ClientEffect::Acknowledgement { proposal, .. } => {
                self.pending.remove(proposal);
            }
            _ => {}
        }
    }

    pub fn commit_json(&mut self, effect_json: &str) -> Result<(), String> {
        self.commit(&ClientEffect::from_json(effect_json)?);
        Ok(())
    }

    /// Build a revision-zero proposal and begin tracking its identifier.
    pub fn proposal_json(
        &mut self,
        id: u64,
        ops_json: &str,
        proposal: Option<&str>,
    ) -> Result<String, String> {
        let ops: Vec<Op> = serde_json::from_str(ops_json).map_err(|error| error.to_string())?;
        let proposal = self.proposal_id(proposal)?;
        Message::Patch {
            id,
            patch: Patch { rev: 0, ops },
            proposal: Some(proposal),
        }
        .to_json()
    }

    fn proposal_id(&mut self, proposal: Option<&str>) -> Result<String, String> {
        let proposal = match proposal {
            Some(proposal) if reserved_proposal(proposal) => {
                return Err("proposal identifiers matching 'auto-N' are reserved".into());
            }
            Some(proposal) => proposal.to_owned(),
            None => {
                let proposal = format!("auto-{}", self.next_proposal);
                self.next_proposal += 1;
                proposal
            }
        };
        self.pending.insert(proposal.clone());
        Ok(proposal)
    }

    /// Abandon every unsettled proposal while retaining revisions for reconnect resume.
    pub fn disconnect(&mut self) -> ClientEffect {
        ClientEffect::Disconnect {
            proposals: std::mem::take(&mut self.pending).into_iter().collect(),
        }
    }

    pub fn disconnect_json(&mut self) -> Result<String, String> {
        self.disconnect().to_json()
    }

    pub fn revisions_json(&self) -> Result<String, String> {
        serde_json::to_string(&self.revisions).map_err(|error| error.to_string())
    }

    pub fn pending(&self) -> Vec<String> {
        self.pending.iter().cloned().collect()
    }

    pub fn pending_json(&self) -> Result<String, String> {
        serde_json::to_string(&self.pending()).map_err(|error| error.to_string())
    }

    pub fn abandon(&mut self, proposal: &str) -> bool {
        self.pending.remove(proposal)
    }
}

fn reserved_proposal(proposal: &str) -> bool {
    proposal.strip_prefix("auto-").is_some_and(|suffix| {
        !suffix.is_empty() && suffix.bytes().all(|byte| byte.is_ascii_digit())
    })
}

#[cfg(test)]
mod client_tests {
    use super::*;
    use crate::Value;

    fn snapshot(rev: u64) -> Message {
        Message::Snapshot {
            id: 7,
            model_type: "Counter".into(),
            rev,
            value: Value::map([("count", Value::Int(1))]),
        }
    }

    fn patch(rev: u64, proposal: Option<&str>) -> Message {
        Message::Patch {
            id: 7,
            patch: Patch { rev, ops: vec![] },
            proposal: proposal.map(str::to_owned),
        }
    }

    #[test]
    fn revisions_advance_only_when_an_effect_is_committed() {
        let mut state = ClientState::new();
        let effect = state.prepare(&snapshot(3)).unwrap();
        assert_eq!(state.revisions_json().unwrap(), "{}");
        state.commit(&effect);
        assert_eq!(state.revisions_json().unwrap(), r#"{"7":3}"#);

        let effect = state.prepare(&patch(4, None)).unwrap();
        assert!(matches!(effect, ClientEffect::Patch { rev: 4, .. }));
        assert_eq!(state.revisions_json().unwrap(), r#"{"7":3}"#);
        state.commit(&effect);
        assert_eq!(state.revisions_json().unwrap(), r#"{"7":4}"#);
    }

    #[test]
    fn stale_patches_are_classified_without_advancing_revision() {
        let mut state = ClientState::new();
        let effect = state.prepare(&snapshot(3)).unwrap();
        state.commit(&effect);
        assert!(matches!(
            state.prepare(&patch(3, Some("edit-1"))).unwrap(),
            ClientEffect::StalePatch { rev: 3, .. }
        ));
    }

    #[test]
    fn a_patch_before_snapshot_is_rejected() {
        assert_eq!(
            ClientState::new().prepare(&patch(1, None)).unwrap_err(),
            "patch received before snapshot for model 7"
        );
    }

    #[test]
    fn proposal_ids_are_allocated_settled_and_abandoned() {
        let mut state = ClientState::new();
        let first = state.proposal_json(7, "[]", None).unwrap();
        let second = state.proposal_json(7, "[]", Some("editor-1")).unwrap();
        assert!(first.contains(r#""proposal":"auto-1""#));
        assert!(second.contains(r#""proposal":"editor-1""#));
        assert_eq!(state.pending(), vec!["auto-1", "editor-1"]);

        state.commit(&ClientEffect::Acknowledgement {
            id: 7,
            rev: 3,
            proposal: "auto-1".into(),
        });
        assert_eq!(state.pending(), vec!["editor-1"]);
        assert_eq!(
            state.disconnect(),
            ClientEffect::Disconnect {
                proposals: vec!["editor-1".into()]
            }
        );
        assert!(state.pending().is_empty());
    }

    #[test]
    fn generated_proposal_namespace_is_reserved() {
        let error = ClientState::new()
            .proposal_json(7, "[]", Some("auto-42"))
            .unwrap_err();
        assert_eq!(error, "proposal identifiers matching 'auto-N' are reserved");
    }

    #[test]
    fn acknowledgements_and_rejections_are_typed_effects() {
        let state = ClientState::new();
        assert!(matches!(
            state
                .prepare(&Message::Ack {
                    id: 7,
                    rev: 4,
                    proposal: "edit-1".into(),
                })
                .unwrap(),
            ClientEffect::Acknowledgement { .. }
        ));
        assert!(matches!(
            state
                .prepare(&Message::Reject {
                    id: 7,
                    rev: 4,
                    error: "invalid".into(),
                    proposal: Some("edit-2".into()),
                })
                .unwrap(),
            ClientEffect::Rejection { .. }
        ));
    }
}
