//! Joiner — connects to a hosted onion service via Tor.

use arti_client::{DataStream, TorClient};
use tokio::time::{timeout, Duration};
use tor_rtcompat::PreferredRuntime;
use tracing::info;

use crate::error::{ChatError, Result};
use crate::invite::decode as decode_invite;

/// Default connection timeout for joiner (30 seconds).
const DEFAULT_CONNECT_TIMEOUT: Duration = Duration::from_secs(30);

/// A joiner that connects to a hosted onion service via Tor.
pub struct Joiner {
    stream: Option<DataStream>,
    connected: bool,
}

impl Joiner {
    /// Connect to a hosted room using an invite code.
    ///
    /// Parses the invite to get the onion address, then connects via Tor.
    /// Times out after 30 seconds if the connection cannot be established.
    pub async fn connect(
        tor_client: &TorClient<PreferredRuntime>,
        invite_code: &str,
    ) -> Result<Self> {
        Self::connect_with_timeout(tor_client, invite_code, DEFAULT_CONNECT_TIMEOUT).await
    }

    /// Connect to a hosted room using an invite code with a custom timeout.
    pub async fn connect_with_timeout(
        tor_client: &TorClient<PreferredRuntime>,
        invite_code: &str,
        timeout_dur: Duration,
    ) -> Result<Self> {
        let payload = decode_invite(invite_code, None)?;

        info!("connecting to onion service: {}", payload.onion_address);

        let target = (payload.onion_address.as_str(), 80);
        let stream: DataStream = timeout(timeout_dur, tor_client.connect(target))
            .await
            .map_err(|_| ChatError::Connection("connection timed out".into()))?
            .map_err(|e| ChatError::Connection(format!("onion connect failed: {e}")))?;

        info!("connected to onion service");

        Ok(Self {
            stream: Some(stream),
            connected: true,
        })
    }

    /// Connect to a specific onion address and port.
    ///
    /// Times out after 30 seconds if the connection cannot be established.
    pub async fn connect_to(
        tor_client: &TorClient<PreferredRuntime>,
        onion_address: &str,
        port: u16,
    ) -> Result<Self> {
        Self::connect_to_with_timeout(tor_client, onion_address, port, DEFAULT_CONNECT_TIMEOUT)
            .await
    }

    /// Connect to a specific onion address and port with a custom timeout.
    pub async fn connect_to_with_timeout(
        tor_client: &TorClient<PreferredRuntime>,
        onion_address: &str,
        port: u16,
        timeout_dur: Duration,
    ) -> Result<Self> {
        let target = (onion_address, port);
        let stream: DataStream = timeout(timeout_dur, tor_client.connect(target))
            .await
            .map_err(|_| ChatError::Connection("connection timed out".into()))?
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
