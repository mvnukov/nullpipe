//! Joiner — connects to a hosted onion service via Tor.

use arti_client::{DataStream, TorClient};
use tor_rtcompat::PreferredRuntime;
use tracing::info;

use crate::error::{ChatError, Result};
use crate::invite::decode as decode_invite;

/// A joiner that connects to a hosted onion service via Tor.
pub struct Joiner {
    stream: Option<DataStream>,
    connected: bool,
}

impl Joiner {
    /// Connect to a hosted room using an invite code.
    ///
    /// Parses the invite to get the onion address, then connects via Tor.
    pub async fn connect(
        tor_client: &TorClient<PreferredRuntime>,
        invite_code: &str,
    ) -> Result<Self> {
        let payload = decode_invite(invite_code, None)?;

        info!("connecting to onion service: {}", payload.onion_address);

        // Connect to the onion service on port 80 (default virtual port)
        let target = (payload.onion_address.as_str(), 80);
        let stream: DataStream = tor_client
            .connect(target)
            .await
            .map_err(|e| ChatError::Connection(format!("onion connect failed: {e}")))?;

        info!("connected to onion service");

        Ok(Self {
            stream: Some(stream),
            connected: true,
        })
    }

    /// Connect to a specific onion address and port.
    pub async fn connect_to(
        tor_client: &TorClient<PreferredRuntime>,
        onion_address: &str,
        port: u16,
    ) -> Result<Self> {
        let target = (onion_address, port);
        let stream: DataStream = tor_client
            .connect(target)
            .await
            .map_err(|e| ChatError::Connection(format!("onion connect failed: {e}")))?;

        info!("connected to {onion_address}:{port}");

        Ok(Self {
            stream: Some(stream),
            connected: true,
        })
    }

    /// Return the underlying Tor stream for reading/writing.
    pub fn stream(&mut self) -> Result<&mut DataStream> {
        self.stream
            .as_mut()
            .ok_or_else(|| ChatError::Connection("not connected".into()))
    }

    /// Shutdown the connection. Idempotent.
    pub fn shutdown(&mut self) {
        if self.stream.take().is_some() {
            info!("joiner stream closed");
        }
        self.connected = false;
    }

    /// Whether the joiner is connected.
    pub fn is_connected(&self) -> bool {
        self.connected
    }
}

impl Drop for Joiner {
    fn drop(&mut self) {
        self.shutdown();
    }
}
