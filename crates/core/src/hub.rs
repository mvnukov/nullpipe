//! Hosted onion service (hub/room).

use arti_client::{config::onion_service::OnionServiceConfigBuilder, TorClient};
use futures::StreamExt;
use safelog::DisplayRedacted;
use std::sync::Arc;
use tor_hsservice::{handle_rend_requests, HsNickname, RunningOnionService};
use tor_rtcompat::PreferredRuntime;
use tracing::info;

use crate::error::{ChatError, Result};

/// A hosted onion service (hub/room).
///
/// Wraps a running v3 onion service and provides the onion address.
pub struct HostedRoom {
    running_svc: Option<Arc<RunningOnionService>>,
    address: Option<String>,
    _join_handle: Option<tokio::task::JoinHandle<()>>,
}

impl HostedRoom {
    /// Create and launch a new v3 onion service on the given port.
    ///
    /// The `tor_client` must already be bootstrapped.
    pub async fn new(tor_client: &TorClient<PreferredRuntime>, _port: u16) -> Result<Self> {
        let nickname = HsNickname::new("chat-room".to_string())
            .map_err(|e| ChatError::OnionService(format!("invalid nickname: {e}")))?;

        let svc_config = OnionServiceConfigBuilder::default()
            .nickname(nickname)
            .build()
            .map_err(|e| ChatError::OnionService(format!("config build failed: {e}")))?;

        let launch_result = tor_client
            .launch_onion_service(svc_config)
            .map_err(|e| ChatError::OnionService(format!("launch failed: {e}")))?;

        let Some((running_svc, rend_stream)) = launch_result else {
            return Err(ChatError::OnionService(
                "onion service disabled in config".into(),
            ));
        };

        let hs_id = running_svc
            .onion_address()
            .ok_or_else(|| ChatError::OnionService("could not get onion address".into()))?;

        let onion_address = hs_id.display_unredacted().to_string();
        info!("onion service ready: {onion_address}");

        // Spawn a task to accept incoming rendezvous requests
        let join_handle = tokio::spawn(Self::accept_loop(rend_stream));

        Ok(Self {
            running_svc: Some(running_svc),
            address: Some(onion_address),
            _join_handle: Some(join_handle),
        })
    }

    async fn accept_loop(
        rend_stream: impl StreamExt<Item = tor_hsservice::RendRequest> + Unpin + Send + 'static,
    ) {
        let mut stream_requests = handle_rend_requests(rend_stream);
        while let Some(_stream_req) = stream_requests.next().await {
            info!("accepted stream request on onion service");
        }
    }

    /// Return the onion address of this room.
    pub fn address(&self) -> &str {
        self.address.as_deref().unwrap_or("")
    }

    /// Shutdown the onion service. Idempotent.
    pub fn shutdown(&mut self) {
        if self.running_svc.take().is_some() {
            info!("onion service shut down");
        }
        self.address = None;
    }

    /// Whether the service is still running.
    pub fn is_running(&self) -> bool {
        self.running_svc.is_some()
    }
}

impl Drop for HostedRoom {
    fn drop(&mut self) {
        self.shutdown();
    }
}
