//! Bounded replay with content-addressed binary attachments.

use std::collections::{BTreeMap, VecDeque};

use sha2::{Digest, Sha256};

use crate::{BufferFrame, Message, Patch};

struct Blob {
    bytes: Vec<u8>,
    references: usize,
}

enum Attachment {
    Inline(Vec<u8>),
    Shared(String),
}

struct Entry {
    rev: u64,
    header: String,
    attachments: Vec<Attachment>,
}

/// Per-model history. Evicting an entry releases its blobs; missing history requires a snapshot.
pub struct ReplayLog {
    entries: VecDeque<Entry>,
    blobs: BTreeMap<String, Blob>,
    bytes: usize,
    cap: usize,
    max_bytes: usize,
    blob_threshold: usize,
}

impl ReplayLog {
    pub fn new(cap: usize, max_bytes: usize, blob_threshold: usize) -> Self {
        Self {
            entries: VecDeque::new(),
            blobs: BTreeMap::new(),
            bytes: 0,
            cap,
            max_bytes,
            blob_threshold,
        }
    }

    pub fn append(&mut self, patch: Patch) -> Result<(), String> {
        let rev = patch.rev;
        let frame = BufferFrame::from_message(&Message::Patch {
            id: 0,
            patch,
            proposal: None,
        })?;
        let mut attachments = Vec::new();
        self.bytes += frame.header.len();
        for buffer in frame.buffers {
            if buffer.len() < self.blob_threshold {
                self.bytes += buffer.len();
                attachments.push(Attachment::Inline(buffer));
            } else {
                let hash = format!("{:x}", Sha256::digest(&buffer));
                let blob = self.blobs.entry(hash.clone()).or_insert_with(|| {
                    self.bytes += buffer.len();
                    Blob {
                        bytes: buffer,
                        references: 0,
                    }
                });
                blob.references += 1;
                attachments.push(Attachment::Shared(hash));
            }
        }
        self.entries.push_back(Entry {
            rev,
            header: frame.header,
            attachments,
        });
        while self.entries.len() > self.cap || self.bytes > self.max_bytes {
            let Some(entry) = self.entries.pop_front() else {
                break;
            };
            self.bytes -= entry.header.len();
            for attachment in entry.attachments {
                match attachment {
                    Attachment::Inline(bytes) => self.bytes -= bytes.len(),
                    Attachment::Shared(hash) => {
                        let blob = self.blobs.get_mut(&hash).expect("live blob reference");
                        blob.references -= 1;
                        if blob.references == 0 {
                            self.bytes -= blob.bytes.len();
                            self.blobs.remove(&hash);
                        }
                    }
                }
            }
        }
        Ok(())
    }

    pub fn since(&self, rev: u64, current: u64) -> Result<Option<Vec<Patch>>, String> {
        if rev >= current {
            return Ok(Some(Vec::new()));
        }
        let entries: Vec<_> = self
            .entries
            .iter()
            .filter(|entry| entry.rev > rev)
            .collect();
        let mut expected = rev + 1;
        let mut replay_bytes = 0;
        for entry in &entries {
            if entry.rev != expected {
                return Ok(None);
            }
            replay_bytes += entry.header.len();
            for attachment in &entry.attachments {
                replay_bytes += match attachment {
                    Attachment::Inline(bytes) => bytes.len(),
                    Attachment::Shared(hash) => self.blobs[hash].bytes.len(),
                };
            }
            if replay_bytes > self.max_bytes {
                return Ok(None);
            }
            expected += 1;
        }
        if expected != current + 1 {
            return Ok(None);
        }
        let mut patches = Vec::with_capacity(entries.len());
        for entry in entries {
            let buffers = entry
                .attachments
                .iter()
                .map(|attachment| match attachment {
                    Attachment::Inline(bytes) => bytes.clone(),
                    Attachment::Shared(hash) => self.blobs[hash].bytes.clone(),
                })
                .collect();
            let frame = BufferFrame {
                header: entry.header.clone(),
                buffers,
            };
            let Message::Patch { patch, .. } = frame.message()? else {
                unreachable!()
            };
            patches.push(patch);
        }
        Ok(Some(patches))
    }

    pub fn append_json(&mut self, json: &str) -> Result<(), String> {
        self.append(serde_json::from_str(json).map_err(|e| e.to_string())?)
    }

    pub fn since_json(&self, rev: u64, current: u64) -> Result<Option<String>, String> {
        self.since(rev, current)?
            .map(|patches| serde_json::to_string(&patches).map_err(|e| e.to_string()))
            .transpose()
    }

    pub fn bytes(&self) -> usize {
        self.bytes
    }
    pub fn len(&self) -> usize {
        self.entries.len()
    }
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
    pub fn blobs(&self) -> usize {
        self.blobs.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Op, Value};

    fn patch(rev: u64, byte: u8) -> Patch {
        Patch {
            rev,
            ops: vec![Op::Set {
                path: vec![],
                value: Value::Bytes(vec![byte; 1024]),
            }],
        }
    }

    #[test]
    fn repeated_blobs_share_storage_and_evict_with_history() {
        let mut log = ReplayLog::new(512, 256 * 1024, 64);
        for rev in 1..=512 {
            log.append(patch(rev, (rev % 2) as u8)).unwrap();
        }
        assert_eq!(log.blobs(), 2);
        assert_eq!(log.len(), 512);
        assert!(log.since(0, 512).unwrap().is_none());
        assert_eq!(
            log.since(510, 512).unwrap().unwrap(),
            vec![patch(511, 1), patch(512, 0)]
        );
        for rev in 513..=1024 {
            log.append(patch(rev, 2)).unwrap();
        }
        assert_eq!(log.blobs(), 1);
        assert!(log.since(0, 1024).unwrap().is_none());
    }

    #[test]
    fn unique_bytes_are_bounded_and_oversized_entries_require_snapshots() {
        let mut log = ReplayLog::new(512, 4096, 64);
        for rev in 1..=512 {
            log.append(patch(rev, rev as u8)).unwrap();
            assert!(log.bytes() <= 4096);
        }
        assert!(log.len() < 4);
        assert!(log.since(0, 512).unwrap().is_none());
        assert_eq!(log.since(511, 512).unwrap().unwrap(), vec![patch(512, 0)]);
        let mut tiny = ReplayLog::new(512, 10, 64);
        tiny.append(patch(1, 0)).unwrap();
        assert_eq!(tiny.bytes(), 0);
        assert_eq!(tiny.blobs(), 0);
        assert!(tiny.since(0, 1).unwrap().is_none());
    }
}
