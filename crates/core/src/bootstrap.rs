//! Tor bootstrap wrapper using arti-client.

use arti_client::{TorClient, TorClientConfig};
use futures::Stream;
use tor_rtcompat::PreferredRuntime;
use tracing::info;

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
    /// tracking.
    pub async fn bootstrap(&mut self) -> Result<impl Stream<Item = ChatEvent>> {
        if self.bootstrapped {
            return Err(ChatError::Connection("already bootstrapped".into()));
        }

        info!("bootstrapping Tor client");
        let config = TorClientConfig::default();

        let client = TorClient::create_bootstrapped(config)
            .await
            .map_err(|e| ChatError::Connection(format!("bootstrap failed: {e}")))?;

        self.client = Some(client);
        self.bootstrapped = true;

        info!("Tor bootstrap complete");
        Ok(futures::stream::once(async {
            ChatEvent::BootstrapProgress(100)
        }))
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
