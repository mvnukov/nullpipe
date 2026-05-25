//! Hosted onion service (hub/room).

use arti_client::{config::onion_service::OnionServiceConfigBuilder, DataStream, TorClient};
use futures::StreamExt;
use safelog::DisplayRedacted;
use std::sync::Arc;
use tokio::sync::mpsc;
use tor_cell::relaycell::msg::Connected;
use tor_hsservice::{handle_rend_requests, HsNickname, RunningOnionService};
use tor_rtcompat::PreferredRuntime;
use tracing::{info, warn};

use crate::error::{ChatError, Result};

/// A hosted onion service (hub/room).
///
/// Wraps a running v3 onion service and provides the onion address.
pub struct HostedRoom {
    running_svc: Option<Arc<RunningOnionService>>,
    address: Option<String>,
    port: u16,
    /// Channel receiver for accepted peer streams.
    stream_rx: Option<mpsc::Receiver<DataStream>>,
    _join_handle: Option<tokio::task::JoinHandle<()>>,
}

impl HostedRoom {
    /// Create and launch a new v3 onion service on the given port.
    ///
    /// The `tor_client` must already be bootstrapped.
    pub async fn new(tor_client: &TorClient<PreferredRuntime>, port: u16) -> Result<Self> {
        // Unique nickname per instance to avoid collisions when restarting.
        // Uses a monotonic counter so each room gets a distinct name.
        static NEXT_ID: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        let id = NEXT_ID.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let nickname_str = format!("chat-room-{id}");
        let nickname = HsNickname::new(nickname_str)
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

        // Create a channel for accepted peer streams
        let (stream_tx, stream_rx) = mpsc::channel::<DataStream>(4);

        // Spawn a task to accept incoming rendezvous requests
        let join_handle = tokio::spawn(Self::accept_loop(rend_stream, stream_tx));

        Ok(Self {
            running_svc: Some(running_svc),
            address: Some(onion_address),
            port,
            stream_rx: Some(stream_rx),
            _join_handle: Some(join_handle),
        })
    }

    async fn accept_loop(
        rend_stream: impl futures::Stream<Item = tor_hsservice::RendRequest> + Unpin + Send + 'static,
        stream_tx: mpsc::Sender<DataStream>,
    ) {
        let mut stream_requests = handle_rend_requests(rend_stream);
        while let Some(stream_req) = stream_requests.next().await {
            match stream_req.accept(Connected::new_empty()).await {
                Ok(stream) => {
                    info!("accepted peer stream on onion service");
                    if stream_tx.send(stream).await.is_err() {
                        info!("stream channel closed, stopping accept loop");
                        break;
                    }
                }
                Err(e) => {
                    warn!("failed to accept stream request: {e}");
                }
            }
        }
        info!("onion service accept loop ended");
    }

    /// Return the onion address of this room.
    pub fn address(&self) -> &str {
        self.address.as_deref().unwrap_or("")
    }

    /// Return the virtual port this room listens on.
    pub fn port(&self) -> u16 {
        self.port
    }

    /// Return a [`ChatEvent::RoomReady`] for this room.
    ///
    /// Returns `RoomReady { onion_address, port }` while the service is running,
    /// or `RoomReady { "", 0 }` after shutdown.
    pub fn ready_event(&self) -> crate::types::ChatEvent {
        crate::types::ChatEvent::RoomReady {
            onion_address: self.address().to_string(),
            port: self.port,
        }
    }

    /// Accept the next incoming peer stream.
    ///
    /// Returns `None` if the onion service has been shut down or the accept
    /// loop has exited.
    pub async fn accept_peer(&mut self) -> Option<DataStream> {
        if let Some(rx) = &mut self.stream_rx {
            rx.recv().await
        } else {
            None
        }
    }

    /// Shutdown the onion service. Idempotent.
    pub fn shutdown(&mut self) {
        if self.running_svc.take().is_some() {
            info!("onion service shut down");
        }
        // Drop the receiver so the accept loop exits
        self.stream_rx = None;
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
