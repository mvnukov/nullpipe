//! Tor bootstrap wrapper using arti-client.

use arti_client::{TorClient, TorClientConfig};
use futures::{Stream, StreamExt};
use tokio::sync::mpsc;
use tor_rtcompat::PreferredRuntime;
use tracing::{error, info};

use crate::error::{ChatError, Result};
use crate::types::ChatEvent;

/// Wrapper around an arti Tor client that handles bootstrap lifecycle.
pub struct TorBootstrap {
    client: Option<TorClient<PreferredRuntime>>,
    bootstrapped: bool,
}

impl TorBootstrap {
    /// Create a new unbootstrapped Tor client wrapper.
    pub fn new() -> Self {
        Self {
            client: None,
            bootstrapped: false,
        }
    }

    /// Bootstrap the Tor client. Returns a stream of [`ChatEvent`] for progress
    /// tracking. Progress events range from 0 to 100.
    ///
    /// Bootstrap runs in the background. Poll the returned stream to receive
    /// `BootstrapProgress` events as they happen. The stream ends with
    /// `BootstrapProgress(100)` on success or `ChatEvent::Error(...)` on failure.
    pub async fn bootstrap(&mut self) -> Result<impl Stream<Item = ChatEvent>> {
        if self.bootstrapped {
            return Err(ChatError::Connection("already bootstrapped".into()));
        }

        info!("bootstrapping Tor client");
        let config = TorClientConfig::default();

        // Build an unbootstrapped client so we can subscribe to progress events
        let client = TorClient::builder()
            .config(config)
            .create_unbootstrapped_async()
            .await
            .map_err(|e| ChatError::Connection(format!("create client failed: {e}")))?;

        // Subscribe to bootstrap events before starting bootstrap
        let events = client.bootstrap_events();

        // Create a channel to forward progress events
        let (tx, rx) = mpsc::channel::<ChatEvent>(32);

        // Spawn bootstrap in background, forwarding progress events
        let client_for_bootstrap = client.clone();
        tokio::spawn(async move {
            // Emit initial progress so consumers always see at least one event
            if tx.send(ChatEvent::BootstrapProgress(0)).await.is_err() {
                return; // receiver dropped immediately
            }

            // Forward live progress events
            let mut event_stream = futures::StreamExt::map(events, |status| {
                ChatEvent::BootstrapProgress((status.as_frac() * 100.0).min(100.0) as u8)
            });

            // Run bootstrap in parallel with event forwarding
            let bootstrap_fut = client_for_bootstrap.bootstrap();
            let mut bootstrap_done = std::pin::pin!(bootstrap_fut);

            loop {
                tokio::select! {
                    biased;
                    result = &mut bootstrap_done => {
                        match result {
                            Ok(()) => {
                                let _ = tx.send(ChatEvent::BootstrapProgress(100)).await;
                                info!("Tor bootstrap complete");
                            }
                            Err(e) => {
                                error!("bootstrap failed: {e}");
                                let _ = tx.send(ChatEvent::Error(
                                    ChatError::Connection(format!("bootstrap failed: {e}"))
                                )).await;
                            }
                        }
                        break;
                    }
                    event = event_stream.next() => {
                        if let Some(event) = event {
                            if tx.send(event).await.is_err() {
                                break; // receiver dropped
                            }
                        }
                    }
                }
            }
        });

        self.client = Some(client);
        self.bootstrapped = true;

        Ok(tokio_stream::wrappers::ReceiverStream::new(rx))
    }

    /// Return a reference to the bootstrapped Tor client.
    pub fn client(&self) -> Result<&TorClient<PreferredRuntime>> {
        self.client
            .as_ref()
            .ok_or_else(|| ChatError::Connection("client not bootstrapped".into()))
    }

    /// Return an Arc reference to the bootstrapped Tor client.
    pub fn client_arc(&self) -> Result<std::sync::Arc<TorClient<PreferredRuntime>>> {
        Ok(std::sync::Arc::new(
            self.client
                .as_ref()
                .ok_or_else(|| ChatError::Connection("client not bootstrapped".into()))?
                .clone(),
        ))
    }

    /// Shutdown the Tor client. Idempotent.
    pub fn shutdown(&mut self) {
        if self.client.take().is_some() {
            info!("Tor client shut down");
        }
        self.bootstrapped = false;
    }

    /// Whether the client is bootstrapped.
    pub fn is_bootstrapped(&self) -> bool {
        self.bootstrapped
    }
}

impl Default for TorBootstrap {
    fn default() -> Self {
        Self::new()
    }
}
