use std::{
    collections::BTreeSet,
    sync::{Arc, Mutex, atomic::AtomicBool},
};

use anyhow::{Context, Result};
use iroh::{
    RelayMode, RelayUrl, address_lookup::memory::MemoryLookup, endpoint::presets, protocol::Router,
};
use iroh_gossip::{Gossip, api::GossipSender, net::GOSSIP_ALPN};
use serde_wasm_bindgen::from_value;
use wasm_bindgen::{JsError, JsValue, prelude::wasm_bindgen};

use crate::{
    channel::Channel,
    config::{ChannelTicket, ClientOptions},
};

#[wasm_bindgen]
#[derive(Debug)]
pub struct IrohClient {
    endpoint: iroh::Endpoint,
    router: Arc<Router>,
    gossip: Gossip,
    channels: Arc<Mutex<Vec<Arc<AtomicBool>>>>,
}

#[wasm_bindgen]
impl IrohClient {
    #[wasm_bindgen(js_name = create)]
    pub async fn create_js(options: Option<JsValue>) -> Result<Self, JsError> {
        Self::create_inner(options).await.map_err(to_js_error)
    }

    #[wasm_bindgen(js_name = endpointId)]
    pub fn endpoint_id(&self) -> String {
        self.endpoint.id().to_string()
    }

    #[wasm_bindgen(js_name = createChannel)]
    pub async fn create_channel_js(&self) -> Result<Channel, JsError> {
        self.create_channel_inner().await.map_err(to_js_error)
    }

    #[wasm_bindgen(js_name = joinChannel)]
    pub async fn join_channel_js(&self, ticket: String) -> Result<Channel, JsError> {
        self.join_channel_inner(ticket).await.map_err(to_js_error)
    }

    pub async fn shutdown(&self) -> Result<(), JsError> {
        self.shutdown_inner().await.map_err(to_js_error)
    }
}

impl IrohClient {
    async fn create_inner(options: Option<JsValue>) -> Result<Self> {
        let options = match options {
            Some(options) => {
                from_value::<ClientOptions>(options).context("invalid client options")?
            }
            None => ClientOptions::default(),
        };
        let secret_key = options.parse_secret_key()?;
        let relay_mode = match options.relay_url {
            Some(relay_url) => RelayMode::Custom(relay_url.parse::<RelayUrl>()?.into()),
            None => RelayMode::Default,
        };

        let endpoint = iroh::Endpoint::builder(presets::N0)
            .secret_key(secret_key)
            .address_lookup(MemoryLookup::new())
            .relay_mode(relay_mode)
            .alpns(vec![GOSSIP_ALPN.to_vec()])
            .bind()
            .await?;
        endpoint.online().await;

        let gossip = Gossip::builder().spawn(endpoint.clone());
        let router = Arc::new(
            Router::builder(endpoint.clone())
                .accept(GOSSIP_ALPN, gossip.clone())
                .spawn(),
        );

        Ok(Self {
            endpoint,
            router,
            gossip,
            channels: Arc::new(Mutex::new(Vec::new())),
        })
    }

    async fn create_channel_inner(&self) -> Result<Channel> {
        self.join_topic(ChannelTicket::new_random().topic_id, BTreeSet::new())
            .await
    }

    async fn join_channel_inner(&self, ticket: String) -> Result<Channel> {
        let ticket = ChannelTicket::deserialize(&ticket)?;
        self.join_topic(ticket.topic_id, ticket.bootstrap).await
    }

    async fn shutdown_inner(&self) -> Result<()> {
        for closed in self.channels.lock().expect("poisoned").iter() {
            closed.store(true, std::sync::atomic::Ordering::SeqCst);
        }

        if let Err(err) = self.router.shutdown().await {
            tracing::warn!("failed to shutdown router cleanly: {err}");
        }
        self.endpoint.close().await;
        Ok(())
    }

    async fn join_topic(
        &self,
        topic_id: iroh_gossip::proto::TopicId,
        bootstrap: BTreeSet<iroh::EndpointId>,
    ) -> Result<Channel> {
        let bootstrap_peers: Vec<_> = bootstrap.iter().copied().collect();
        let gossip_topic = self.gossip.subscribe(topic_id, bootstrap_peers).await?;
        let (sender, receiver): (GossipSender, iroh_gossip::api::GossipReceiver) =
            gossip_topic.split();
        let neighbors = Arc::new(Mutex::new(BTreeSet::new()));
        let closed = Arc::new(AtomicBool::new(false));

        let channel = Channel::new(
            self.endpoint.id(),
            topic_id,
            bootstrap,
            neighbors,
            sender,
            receiver,
            closed.clone(),
            self.router.clone(),
        );

        self.channels.lock().expect("poisoned").push(closed);
        Ok(channel)
    }
}

fn to_js_error(err: impl Into<anyhow::Error>) -> JsError {
    let err: anyhow::Error = err.into();
    JsError::new(&err.to_string())
}
