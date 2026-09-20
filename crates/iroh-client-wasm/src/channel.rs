use std::{
    collections::BTreeSet,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    time::{SystemTime, UNIX_EPOCH},
};

use anyhow::{Context, Result, anyhow};
use iroh::EndpointId;
use iroh_gossip::{api::GossipSender, proto::TopicId};
use js_sys::Uint8Array;
use n0_future::{StreamExt, stream};
use serde_wasm_bindgen::to_value;
use tokio::sync::Mutex as TokioMutex;
use wasm_bindgen::{JsError, JsValue, prelude::wasm_bindgen};
use wasm_streams::readable::sys::ReadableStream as JsReadableStream;

use crate::{
    config::{ChannelTicket, ChannelTicketOptions},
    event::{Event, MessageEnvelope},
};

#[wasm_bindgen]
#[derive(Debug)]
pub struct Channel {
    me: EndpointId,
    topic_id: TopicId,
    bootstrap: BTreeSet<EndpointId>,
    neighbors: Arc<Mutex<BTreeSet<EndpointId>>>,
    sender: Arc<TokioMutex<GossipSender>>,
    receiver: Option<iroh_gossip::api::GossipReceiver>,
    pub(crate) closed: Arc<AtomicBool>,
    _router: Arc<iroh::protocol::Router>,
}

#[derive(Debug)]
struct StreamState {
    receiver: iroh_gossip::api::GossipReceiver,
    pending: Option<Event>,
    neighbors: Arc<Mutex<BTreeSet<EndpointId>>>,
    closed: Arc<AtomicBool>,
}

#[wasm_bindgen]
impl Channel {
    pub(crate) fn new(
        me: EndpointId,
        topic_id: TopicId,
        bootstrap: BTreeSet<EndpointId>,
        neighbors: Arc<Mutex<BTreeSet<EndpointId>>>,
        sender: GossipSender,
        receiver: iroh_gossip::api::GossipReceiver,
        closed: Arc<AtomicBool>,
        router: Arc<iroh::protocol::Router>,
    ) -> Self {
        Self {
            me,
            topic_id,
            bootstrap,
            neighbors,
            sender: Arc::new(TokioMutex::new(sender)),
            receiver: Some(receiver),
            closed,
            _router: router,
        }
    }

    #[wasm_bindgen(js_name = id)]
    pub fn id_js(&self) -> String {
        self.topic_id.to_string()
    }

    #[wasm_bindgen(js_name = neighbors)]
    pub fn neighbors_js(&self) -> Vec<String> {
        self.neighbors
            .lock()
            .expect("poisoned")
            .iter()
            .map(ToString::to_string)
            .collect()
    }

    #[wasm_bindgen(js_name = ticket)]
    pub fn ticket_js(&self, options: Option<JsValue>) -> Result<String, JsError> {
        let options = match options {
            Some(options) => serde_wasm_bindgen::from_value::<ChannelTicketOptions>(options)
                .map_err(to_js_error)?,
            None => ChannelTicketOptions::default(),
        };

        let mut ticket = ChannelTicket::new(self.topic_id);
        if options.include_self {
            ticket.bootstrap.insert(self.me);
        }
        if options.include_bootstrap {
            ticket.bootstrap.extend(self.bootstrap.iter().copied());
        }
        if options.include_neighbors {
            ticket
                .bootstrap
                .extend(self.neighbors.lock().expect("poisoned").iter().copied());
        }

        Ok(ticket.serialize())
    }

    #[wasm_bindgen(js_name = events)]
    pub fn events_js(&mut self) -> Result<JsReadableStream, JsError> {
        let receiver = self
            .receiver
            .take()
            .ok_or_else(|| JsError::new("event stream already taken"))?;
        let me = self.me;
        let state = StreamState {
            receiver,
            pending: None,
            neighbors: self.neighbors.clone(),
            closed: self.closed.clone(),
        };

        let stream = stream::try_unfold(state, move |mut state| {
            let me = me;
            async move {
                if state.closed.load(Ordering::SeqCst) {
                    return Ok(None);
                }

                if let Some(event) = state.pending.take() {
                    let value = to_value(&event).map_err(to_js_error)?;
                    return Ok(Some((value, state)));
                }

                loop {
                    let was_joined = state.receiver.is_joined();
                    let Some(event) = state.receiver.try_next().await.map_err(to_js_error)? else {
                        return Ok(None);
                    };

                    let event = match Event::from_gossip_event_with_me(event, me) {
                        Ok(Some(event)) => event,
                        Ok(None) => continue,
                        Err(err) => {
                            tracing::warn!("dropping invalid gossip event: {err}");
                            continue;
                        }
                    };

                    if !was_joined && state.receiver.is_joined() {
                        let neighbors = state
                            .neighbors
                            .lock()
                            .expect("poisoned")
                            .iter()
                            .copied()
                            .collect();
                        state.pending = Some(event);
                        let value = to_value(&Event::Joined { neighbors }).map_err(to_js_error)?;
                        return Ok(Some((value, state)));
                    }

                    if let Event::NeighborUp { endpoint_id } = &event {
                        state
                            .neighbors
                            .lock()
                            .expect("poisoned")
                            .insert(*endpoint_id);
                    }

                    if let Event::NeighborDown { endpoint_id } = &event {
                        state
                            .neighbors
                            .lock()
                            .expect("poisoned")
                            .remove(endpoint_id);
                    }

                    let value = to_value(&event).map_err(to_js_error)?;
                    return Ok(Some((value, state)));
                }
            }
        });

        Ok(wasm_streams::ReadableStream::from_stream(stream).into_raw())
    }

    #[wasm_bindgen(js_name = broadcastNeighbor)]
    pub async fn broadcast_neighbor(&self, value: JsValue) -> Result<(), JsError> {
        let payload = payload_from_js(value).map_err(to_js_error)?;
        self.publish(payload, false, None)
            .await
            .map_err(to_js_error)
    }

    #[wasm_bindgen(js_name = broadcastNeighbors)]
    pub async fn broadcast_neighbors(&self, value: JsValue) -> Result<(), JsError> {
        self.broadcast_neighbor(value).await
    }

    #[wasm_bindgen(js_name = broadcast)]
    pub async fn broadcast(&self, value: JsValue) -> Result<(), JsError> {
        let payload = payload_from_js(value).map_err(to_js_error)?;
        self.publish(payload, true, None).await.map_err(to_js_error)
    }

    #[wasm_bindgen(js_name = send)]
    pub async fn send_direct(&self, target: String, value: JsValue) -> Result<(), JsError> {
        let payload = payload_from_js(value).map_err(to_js_error)?;
        let target = target.trim();
        if target.is_empty() {
            return Err(JsError::new("send requires a peer id or ticket"));
        }

        let parsed_target = if let Ok(peer_id) = target.parse::<EndpointId>() {
            Some(peer_id)
        } else {
            let ticket = ChannelTicket::deserialize(target)
                .map_err(|_| JsError::new("send target must be a peer id or channel ticket"))?;
            ticket.bootstrap.into_iter().next()
        };

        let sender = self.sender.lock().await;
        let peers = if let Ok(peer_id) = target.parse::<EndpointId>() {
            vec![peer_id]
        } else {
            let ticket = ChannelTicket::deserialize(target)
                .map_err(|_| JsError::new("send target must be a peer id or channel ticket"))?;
            ticket.bootstrap.into_iter().collect::<Vec<_>>()
        };

        if !peers.is_empty() {
            sender.join_peers(peers).await.map_err(to_js_error)?;
        }

        drop(sender);
        self.publish(payload, false, parsed_target)
            .await
            .map_err(to_js_error)
    }

    pub async fn close(&self) -> Result<(), JsError> {
        self.closed.store(true, Ordering::SeqCst);
        Ok(())
    }

    async fn publish(
        &self,
        payload: MessagePayload,
        broadcast: bool,
        target: Option<EndpointId>,
    ) -> Result<()> {
        if self.closed.load(Ordering::SeqCst) {
            return Err(anyhow!("channel is closed"));
        }

        let sent_timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .context("system clock is before unix epoch")?
            .as_millis() as u64;
        let envelope = match payload {
            MessagePayload::Text(text) => MessageEnvelope::Text {
                text,
                sent_timestamp,
                from: self.me,
                target,
            },
            MessagePayload::Binary(binary) => MessageEnvelope::Binary {
                binary,
                sent_timestamp,
                from: self.me,
                target,
            },
        };
        let bytes = serde_json::to_vec(&envelope).context("failed to encode channel message")?;

        let sender = self.sender.lock().await;
        if broadcast {
            sender.broadcast(bytes.into()).await?;
        } else {
            sender.broadcast_neighbors(bytes.into()).await?;
        }

        Ok(())
    }
}

#[derive(Debug, Clone)]
enum MessagePayload {
    Text(String),
    Binary(Vec<u8>),
}

fn payload_from_js(value: JsValue) -> Result<MessagePayload> {
    if let Some(text) = value.as_string() {
        return Ok(MessagePayload::Text(text));
    }

    if value.is_object() {
        let array = Uint8Array::new(&value);
        if array.length() > 0 || !value.is_string() {
            return Ok(MessagePayload::Binary(array.to_vec()));
        }
    }

    Err(anyhow!("message payload must be a string or Uint8Array"))
}

fn to_js_error(err: impl Into<anyhow::Error>) -> JsError {
    let err: anyhow::Error = err.into();
    JsError::new(&err.to_string())
}
