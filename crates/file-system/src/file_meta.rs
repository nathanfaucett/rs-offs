use std::collections::BTreeSet;

use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum FileKind {
    File,
    Directory,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(bound(deserialize = "PeerId: Ord + Deserialize<'de>"))]
pub struct FileMeta<PeerId = Uuid> {
    pub file_id: Uuid,
    pub kind: FileKind,
    pub pointer: Option<String>,
    pub providers: BTreeSet<PeerId>,
    pub local: bool,
    pub mode: u32,
    pub owner: String,
    pub group: String,
}

impl<PeerId: Ord> FileMeta<PeerId> {
    pub fn file(file_id: Uuid, node_id: PeerId) -> Self {
        Self::new(file_id, node_id, FileKind::File)
    }

    pub fn directory(file_id: Uuid, node_id: PeerId) -> Self {
        Self::new(file_id, node_id, FileKind::Directory)
    }

    fn new(file_id: Uuid, node_id: PeerId, kind: FileKind) -> Self {
        Self {
            file_id,
            kind,
            pointer: None,
            providers: BTreeSet::from([node_id]),
            local: true,
            mode: match kind {
                FileKind::File => 0o644,
                FileKind::Directory => 0o755,
            },
            owner: String::new(),
            group: String::new(),
        }
    }
}
