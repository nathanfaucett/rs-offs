use std::collections::BTreeSet;

use anyhow::{Context, Result};
use iroh::{EndpointId, SecretKey};
use iroh_gossip::proto::TopicId;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ClientOptions {
    pub relay_url: Option<String>,
    pub secret_key: Option<String>,
}

impl ClientOptions {
    pub fn parse_secret_key(&self) -> Result<SecretKey> {
        match &self.secret_key {
            Some(secret_key) => secret_key.parse().context("failed to parse secret key"),
            None => Ok(SecretKey::generate()),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ChannelTicketOptions {
    #[serde(default = "default_true")]
    pub include_self: bool,
    #[serde(default = "default_true")]
    pub include_bootstrap: bool,
    #[serde(default)]
    pub include_neighbors: bool,
}

impl Default for ChannelTicketOptions {
    fn default() -> Self {
        Self {
            include_self: true,
            include_bootstrap: true,
            include_neighbors: false,
        }
    }
}

fn default_true() -> bool {
    true
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ChannelTicket {
    pub topic_id: TopicId,
    pub bootstrap: BTreeSet<EndpointId>,
}

impl ChannelTicket {
    pub fn new(topic_id: TopicId) -> Self {
        Self {
            topic_id,
            bootstrap: BTreeSet::new(),
        }
    }

    pub fn new_random() -> Self {
        let mut bytes = [0_u8; 32];
        getrandom::fill(&mut bytes).expect("failed to generate topic id");
        Self::new(TopicId::from_bytes(bytes))
    }

    pub fn deserialize(input: &str) -> Result<Self> {
        serde_json::from_str(input).context("failed to decode channel ticket")
    }

    pub fn serialize(&self) -> String {
        serde_json::to_string(self).expect("failed to encode channel ticket")
    }
}
