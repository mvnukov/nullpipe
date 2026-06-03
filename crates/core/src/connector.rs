//! Adapter over Tor connect operations.

use arti_client::DataStream;
use tor_rtcompat::PreferredRuntime;

use crate::error::{ChatError, Result};

#[async_trait::async_trait]
pub trait TorConnector: Send + Sync {
    async fn connect(&self, addr: &str, port: u16) -> Result<DataStream>;
}

/// Real implementation wrapping arti's TorClient.
pub struct ArtiConnector {
    client: arti_client::TorClient<PreferredRuntime>,
}

impl ArtiConnector {
    pub fn new(client: arti_client::TorClient<PreferredRuntime>) -> Self {
        Self { client }
    }

    pub fn client(&self) -> &arti_client::TorClient<PreferredRuntime> {
        &self.client
    }
}

#[async_trait::async_trait]
impl TorConnector for ArtiConnector {
    async fn connect(&self, addr: &str, port: u16) -> Result<DataStream> {
        self.client
            .connect((addr, port))
            .await
            .map_err(|e| ChatError::Connection(format!("connect failed: {e}")))
    }
}

/// Configurable mock for TorConnector.
pub mod mock {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    /// Configurable mock for TorConnector.
    pub struct MockTorConnector {
        pub connect_result: std::sync::Mutex<Option<Result<DataStream>>>,
        pub connect_calls: AtomicUsize,
        pub last_connect_target: std::sync::Mutex<Option<(String, u16)>>,
    }

    impl MockTorConnector {
        pub fn new() -> Self {
            Self {
                connect_result: std::sync::Mutex::new(Some(Err(ChatError::Connection(
                    "mock: connect not configured".into(),
                )))),
                connect_calls: AtomicUsize::new(0),
                last_connect_target: std::sync::Mutex::new(None),
            }
        }

        pub fn with_connect_result(result: Result<DataStream>) -> Self {
            Self {
                connect_result: std::sync::Mutex::new(Some(result)),
                connect_calls: AtomicUsize::new(0),
                last_connect_target: std::sync::Mutex::new(None),
            }
        }

        pub fn call_count(&self) -> usize {
            self.connect_calls.load(Ordering::SeqCst)
        }

        pub fn last_target(&self) -> Option<(String, u16)> {
            self.last_connect_target.lock().unwrap().clone()
        }
    }

    impl Default for MockTorConnector {
        fn default() -> Self {
            Self::new()
        }
    }

    #[async_trait::async_trait]
    impl TorConnector for MockTorConnector {
        async fn connect(&self, addr: &str, port: u16) -> Result<DataStream> {
            self.connect_calls.fetch_add(1, Ordering::SeqCst);
            *self.last_connect_target.lock().unwrap() = Some((addr.to_string(), port));
            self.connect_result
                .lock()
                .unwrap()
                .take()
                .unwrap_or_else(|| Err(ChatError::Connection("mock: result consumed".into())))
        }
    }
    /// Mock acceptor for testing — receives `DuplexStream`s instead of Tor streams.
    ///
    /// Mirrors `HostedRoom::accept_peer()` but without Tor. Useful for fast
    /// integration tests that exercise the same handshake and wire protocol code.
    pub struct MockAcceptor {
        stream_rx: tokio::sync::mpsc::Receiver<tokio::io::DuplexStream>,
    }

    impl MockAcceptor {
        /// Create a new `MockAcceptor` from a receiver end of a duplex stream channel.
        pub fn new(rx: tokio::sync::mpsc::Receiver<tokio::io::DuplexStream>) -> Self {
            Self { stream_rx: rx }
        }

        /// Accept the next mock stream, or `None` if the sender was dropped.
        pub async fn accept(&mut self) -> Option<tokio::io::DuplexStream> {
            self.stream_rx.recv().await
        }
    }
}
