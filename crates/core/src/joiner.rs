//! Joiner — connects to a hosted onion service via Tor.

use arti_client::{DataStream, TorClient};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::mpsc;
use tokio::time::{timeout, Duration};
use tor_rtcompat::PreferredRuntime;
use tracing::{info, warn};

use crate::error::{ChatError, Result};
use crate::invite::decode as decode_invite;
use crate::types::{ChatEvent, PeerId};
use crate::wire::{encode_message, read_message, WireMessage};

/// Default connection timeout for joiner (30 seconds).
const DEFAULT_CONNECT_TIMEOUT: Duration = Duration::from_secs(30);

/// Read timeout for detecting dead connections.
const READ_TIMEOUT: Duration = Duration::from_secs(60);

/// Write timeout for avoiding blocked writes.
const WRITE_TIMEOUT: Duration = Duration::from_secs(30);

/// A joiner that connects to a hosted onion service via Tor.
///
/// After connecting, the joiner holds a write half of the stream for sending
/// messages and runs a background reader task that pushes events through
/// an internal channel, consumed via [`Joiner::recv`].
pub struct Joiner {
    /// Write half of the Tor stream (owned directly for synchronous access).
    writer: Option<tokio::io::WriteHalf<DataStream>>,
    connected: bool,
    peer_id: Option<PeerId>,
    name: Option<String>,
    /// Channel from the reader task to the recv stream.
    event_rx: mpsc::Receiver<Result<ChatEvent>>,
}

impl Joiner {
    /// Connect to a hosted room using an invite code.
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

        Self::from_stream(stream).await
    }

    /// Connect to a specific onion address and port.
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

        Self::from_stream(stream).await
    }

    /// Build a Joiner from an established stream: handshake, split, spawn reader.
    async fn from_stream(stream: DataStream) -> Result<Self> {
        use base58::ToBase58;

        // -- handshake --
        let nonce: [u8; 16] = rand::random();
        let discriminator: [u8; 16] = rand::random();

        let mut buf = [0u8; 32];
        buf[..16].copy_from_slice(&nonce);
        buf[16..32].copy_from_slice(&discriminator);

        let mut s = stream;
        timeout(WRITE_TIMEOUT, s.write_all(&buf))
            .await
            .map_err(|_| ChatError::Connection("handshake write timed out".into()))?
            .map_err(|e| ChatError::Connection(format!("handshake write: {e}")))?;

        let mut response = [0u8; 1];
        timeout(READ_TIMEOUT, s.read_exact(&mut response))
            .await
            .map_err(|_| ChatError::Connection("handshake read timed out".into()))?
            .map_err(|e| ChatError::Connection(format!("handshake read: {e}")))?;

        if response[0] != 0 {
            return Err(ChatError::Connection("handshake rejected by hub".into()));
        }

        let peer_id = PeerId(discriminator.to_base58());
        let name = format!("peer-{}", hex::encode(&discriminator[..4]));

        // -- split stream --
        let (reader, writer) = tokio::io::split(s);

        // -- event channel --
        let (event_tx, event_rx) = mpsc::channel::<Result<ChatEvent>>(256);

        // -- spawn reader task --
        tokio::spawn(Self::reader_task(reader, event_tx));

        Ok(Self {
            writer: Some(writer),
            connected: true,
            peer_id: Some(peer_id),
            name: Some(name),
            event_rx,
        })
    }

    /// Send a chat message to the hub.
    ///
    /// Encodes the text as a length-prefixed `Chat` wire message and writes
    /// it to the Tor stream. Returns an error if the connection is closed
    /// or the write times out.
    pub async fn send(&mut self, text: &str) -> Result<()> {
        let writer = self
            .writer
            .as_mut()
            .ok_or_else(|| ChatError::Connection("not connected".into()))?;

        let msg = WireMessage::chat(self.name.as_deref().unwrap_or(""), text);
        let frame = encode_message(&msg)?;

        timeout(WRITE_TIMEOUT, writer.write_all(&frame))
            .await
            .map_err(|_| ChatError::Connection("write timed out".into()))?
            .map_err(|e| ChatError::Connection(format!("write failed: {e}")))?;

        Ok(())
    }

    /// Receive incoming chat events from the hub.
    ///
    /// Returns a [`Stream`] that yields `Result<ChatEvent>`. The stream
    /// ends when the connection is closed or an error occurs.
    pub fn recv(&mut self) -> tokio_stream::wrappers::ReceiverStream<Result<ChatEvent>> {
        // We need to move the receiver out temporarily. We put back an
        // empty receiver so the struct remains valid.
        let rx = std::mem::replace(
            &mut self.event_rx,
            mpsc::channel(1).1, // dummy
        );
        tokio_stream::wrappers::ReceiverStream::new(rx)
    }

    /// Shutdown the connection. Idempotent.
    pub fn shutdown(&mut self) {
        self.writer = None;
        self.connected = false;
    }

    /// Whether the joiner is connected.
    pub fn is_connected(&self) -> bool {
        self.connected
    }

    /// Our peer ID assigned during handshake.
    pub fn peer_id(&self) -> Option<&PeerId> {
        self.peer_id.as_ref()
    }

    /// Our human-friendly name assigned during handshake.
    pub fn name(&self) -> Option<&str> {
        self.name.as_deref()
    }

    /// Reader task: runs in the background, reads wire messages from the
    /// Tor stream and pushes them as `ChatEvent`s to the event channel.
    ///
    /// Handles:
    /// - Partial reads (read_message retries until complete frame)
    /// - Connection drops (error → send error → break)
    /// - Read timeout (dead peer detection)
    /// - Ping messages (responds with pong, if possible)
    async fn reader_task(
        mut reader: impl AsyncReadExt + Unpin + Send + 'static,
        event_tx: mpsc::Sender<Result<ChatEvent>>,
    ) {
        loop {
            match timeout(READ_TIMEOUT, read_message(&mut reader)).await {
                Ok(Ok(msg)) => {
                    // Auto-respond to pings
                    if msg.kind == crate::wire::MessageType::Ping {
                        if let Ok(pong_frame) = encode_message(&WireMessage::pong()) {
                            // Can't write pong without writer half; drop it.
                            // The hub uses a separate writer task for pings.
                            let _ = pong_frame;
                        }
                        continue;
                    }

                    // Convert wire message to ChatEvent
                    let event = match msg.kind {
                        crate::wire::MessageType::Chat | crate::wire::MessageType::System => {
                            ChatEvent::Message {
                                from: PeerId(msg.name.clone()),
                                name: msg.name,
                                text: msg.text,
                            }
                        }
                        crate::wire::MessageType::Pong => {
                            continue;
                        }
                        crate::wire::MessageType::Ping => unreachable!("handled above"),
                    };

                    if event_tx.send(Ok(event)).await.is_err() {
                        info!("joiner: event channel closed, stopping reader");
                        break;
                    }
                }
                Ok(Err(e)) => {
                    let _ = event_tx.send(Err(e)).await;
                    break;
                }
                Err(_) => {
                    warn!("joiner: read timeout, connection dead");
                    let _ = event_tx
                        .send(Err(ChatError::Connection(
                            "read timeout, peer disconnected".into(),
                        )))
                        .await;
                    break;
                }
            }
        }
        info!("joiner: reader task ended");
    }
}

impl Drop for Joiner {
    fn drop(&mut self) {
        self.shutdown();
    }
}
