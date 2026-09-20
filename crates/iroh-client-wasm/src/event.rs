use anyhow::{Context, Result};
use iroh::EndpointId;
use iroh_gossip::api::Event as GossipEvent;
use iroh_gossip::proto::DeliveryScope;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "camelCase")]
pub enum Event {
    Joined {
        neighbors: Vec<EndpointId>,
    },
    MessageReceived {
        from: EndpointId,
        text: Option<String>,
        binary: Option<Vec<u8>>,
        sent_timestamp: u64,
        scope: DeliveryScope,
    },
    NeighborUp {
        endpoint_id: EndpointId,
    },
    NeighborDown {
        endpoint_id: EndpointId,
    },
    Lagged,
    Closed {
        error: Option<String>,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", untagged)]
pub(crate) enum MessageEnvelope {
    Text {
        text: String,
        sent_timestamp: u64,
        from: EndpointId,
        target: Option<EndpointId>,
    },
    Binary {
        binary: Vec<u8>,
        sent_timestamp: u64,
        from: EndpointId,
        target: Option<EndpointId>,
    },
}

impl TryFrom<GossipEvent> for Event {
    type Error = anyhow::Error;

    fn try_from(event: GossipEvent) -> Result<Self> {
        Ok(match event {
            GossipEvent::NeighborUp(endpoint_id) => Self::NeighborUp { endpoint_id },
            GossipEvent::NeighborDown(endpoint_id) => Self::NeighborDown { endpoint_id },
            GossipEvent::Received(message) => {
                let envelope: MessageEnvelope = serde_json::from_slice(&message.content)
                    .context("failed to decode channel message")?;
                match envelope {
                    MessageEnvelope::Text {
                        text,
                        sent_timestamp,
                        from,
                        target: _,
                    } => Self::MessageReceived {
                        from,
                        text: Some(text),
                        binary: None,
                        sent_timestamp,
                        scope: message.scope,
                    },
                    MessageEnvelope::Binary {
                        binary,
                        sent_timestamp,
                        from,
                        target: _,
                    } => Self::MessageReceived {
                        from,
                        text: None,
                        binary: Some(binary),
                        sent_timestamp,
                        scope: message.scope,
                    },
                }
            }
            GossipEvent::Lagged => Self::Lagged,
        })
    }
}

impl Event {
    pub(crate) fn from_gossip_event_with_me(
        event: GossipEvent,
        me: EndpointId,
    ) -> Result<Option<Self>> {
        Ok(match event {
            GossipEvent::NeighborUp(endpoint_id) => Some(Self::NeighborUp { endpoint_id }),
            GossipEvent::NeighborDown(endpoint_id) => Some(Self::NeighborDown { endpoint_id }),
            GossipEvent::Received(message) => {
                let envelope: MessageEnvelope = serde_json::from_slice(&message.content)
                    .context("failed to decode channel message")?;
                match envelope {
                    MessageEnvelope::Text {
                        text,
                        sent_timestamp,
                        from,
                        target,
                    } => {
                        if target.is_some_and(|target| target != me) {
                            return Ok(None);
                        }
                        Some(Self::MessageReceived {
                            from,
                            text: Some(text),
                            binary: None,
                            sent_timestamp,
                            scope: message.scope,
                        })
                    }
                    MessageEnvelope::Binary {
                        binary,
                        sent_timestamp,
                        from,
                        target,
                    } => {
                        if target.is_some_and(|target| target != me) {
                            return Ok(None);
                        }
                        Some(Self::MessageReceived {
                            from,
                            text: None,
                            binary: Some(binary),
                            sent_timestamp,
                            scope: message.scope,
                        })
                    }
                }
            }
            GossipEvent::Lagged => Some(Self::Lagged),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ed25519_dalek::SigningKey;

    #[test]
    fn message_envelope_round_trips_direct_target() {
        let from =
            EndpointId::from_verifying_key(SigningKey::from_bytes(&[1u8; 32]).verifying_key());
        let target =
            EndpointId::from_verifying_key(SigningKey::from_bytes(&[2u8; 32]).verifying_key());

        let envelope = MessageEnvelope::Text {
            text: "hello".into(),
            sent_timestamp: 123,
            from,
            target: Some(target),
        };

        let json = serde_json::to_string(&envelope).unwrap();
        let decoded: MessageEnvelope = serde_json::from_str(&json).unwrap();

        match decoded {
            MessageEnvelope::Text {
                text,
                sent_timestamp,
                from,
                target,
            } => {
                assert_eq!(text, "hello");
                assert_eq!(sent_timestamp, 123);
                assert_eq!(
                    from,
                    EndpointId::from_verifying_key(
                        SigningKey::from_bytes(&[1u8; 32]).verifying_key()
                    )
                );
                assert_eq!(
                    target,
                    Some(EndpointId::from_verifying_key(
                        SigningKey::from_bytes(&[2u8; 32]).verifying_key()
                    ))
                );
            }
            MessageEnvelope::Binary { .. } => panic!("unexpected binary envelope"),
        }
    }
}
