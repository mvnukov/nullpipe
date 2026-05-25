//! Hosted onion service (hub/room) and connection management.

use arti_client::{config::onion_service::OnionServiceConfigBuilder, DataStream, TorClient};
use base58::ToBase58;
use futures::StreamExt;
use safelog::DisplayRedacted;
use std::collections::HashSet;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::{broadcast, mpsc};
use tokio::time::{timeout, Duration};
use tor_cell::relaycell::msg::Connected;
use tor_hsservice::{handle_rend_requests, HsNickname, RunningOnionService};
use tor_rtcompat::PreferredRuntime;
use tracing::{info, warn};

use crate::error::{ChatError, Result};
use crate::types::{ChatEvent, PeerId, PeerInfo};
use crate::wire::{encode_message, read_frame, WireMessage};

/// Handshake size: 16-byte nonce + 16-byte random peer discriminator.
const HANDSHAKE_LEN: usize = 32;

/// Read timeout for detecting dead peers.
const READ_TIMEOUT: Duration = Duration::from_secs(60);

/// Write timeout for avoiding blocked writes.
const WRITE_TIMEOUT: Duration = Duration::from_secs(30);

/// Broadcast channel capacity per peer.
const BROADCAST_CAPACITY: usize = 256;

// ---------------------------------------------------------------------------
// HostedRoom — v3 onion service lifecycle (Phase 2)
// ---------------------------------------------------------------------------

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
    pub fn ready_event(&self) -> crate::types::ChatEvent {
        crate::types::ChatEvent::RoomReady {
            onion_address: self.address().to_string(),
            port: self.port,
        }
    }

    /// Accept the next incoming peer stream.
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

// ---------------------------------------------------------------------------
// Hub — per-connection handshake, reader/writer tasks, peer registry, broadcast
// ---------------------------------------------------------------------------

/// Per-peer sender: writes from this channel are pushed down the Tor stream.
type PeerTx = mpsc::UnboundedSender<Vec<u8>>;

/// Entry in the peer registry.
struct PeerEntry {
    name: String,
    joined_at: std::time::Instant,
    tx: PeerTx,
    /// This peer's broadcast receiver (used to fan-out messages).
    _broadcast_rx: broadcast::Receiver<ChatEvent>,
}

/// Shared peer registry.
type PeerRegistry = Arc<tokio::sync::RwLock<std::collections::HashMap<PeerId, PeerEntry>>>;

/// Shared nonce bookkeeping (single-use enforcement).
type NonceSet = Arc<tokio::sync::Mutex<HashSet<[u8; 16]>>>;

/// Hub wraps a [`HostedRoom`] and manages incoming connections: handshake,
/// per-peer reader/writer tasks, peer registry, broadcast channel, and
/// disconnect cleanup.
pub struct Hub {
    room: HostedRoom,
    peers: PeerRegistry,
    used_nonces: NonceSet,
    /// Hub's own broadcast sender — all peers subscribe from this.
    broadcast_tx: broadcast::Sender<ChatEvent>,
    /// Channel the hub reads from. Each connected peer's reader pushes
    /// `(PeerId, WireMessage)` here.
    msg_rx: mpsc::Receiver<(PeerId, WireMessage)>,
    msg_tx: mpsc::Sender<(PeerId, WireMessage)>,
    running: bool,
}

impl Hub {
    /// Create a new Hub wrapping the given [`HostedRoom`].
    pub fn new(room: HostedRoom) -> Self {
        let (broadcast_tx, _broadcast_rx) = broadcast::channel(BROADCAST_CAPACITY);
        let (msg_tx, msg_rx) = mpsc::channel::<(PeerId, WireMessage)>(128);
        Self {
            room,
            peers: Arc::new(tokio::sync::RwLock::new(std::collections::HashMap::new())),
            used_nonces: Arc::new(tokio::sync::Mutex::new(HashSet::new())),
            broadcast_tx,
            msg_rx,
            msg_tx,
            running: false,
        }
    }

    // -- public API --

    /// Delegate to the underlying [`HostedRoom`].
    pub fn address(&self) -> &str {
        self.room.address()
    }

    /// Delegate to the underlying [`HostedRoom`].
    pub fn port(&self) -> u16 {
        self.room.port()
    }

    /// Snapshot of connected peers.
    pub async fn peers(&self) -> Vec<PeerInfo> {
        self.peers
            .read()
            .await
            .iter()
            .map(|(id, e)| PeerInfo {
                id: id.clone(),
                name: e.name.clone(),
                joined_at: e.joined_at,
            })
            .collect()
    }

    /// Number of connected peers.
    pub async fn peer_count(&self) -> usize {
        self.peers.read().await.len()
    }

    /// Broadcast a message from the hub (e.g. a system announcement) to all
    /// connected peers.
    pub async fn broadcast_hub(&self, text: &str) {
        let msg = WireMessage::system(text);
        let event = ChatEvent::Message {
            from: PeerId("[hub]".into()),
            name: "[system]".into(),
            text: text.to_string(),
        };
        self.broadcast_tx.send(event).ok();

        if let Ok(frame) = encode_message(&msg) {
            for entry in self.peers.read().await.values() {
                let _ = entry.tx.send(frame.clone());
            }
        }
    }

    /// Send a ping to all connected peers.
    pub async fn broadcast_ping(&self) {
        if let Ok(frame) = encode_message(&WireMessage::ping()) {
            for entry in self.peers.read().await.values() {
                let _ = entry.tx.send(frame.clone());
            }
        }
    }

    /// Accept the next incoming [`ChatEvent`] produced by this hub.
    ///
    /// Call this in a loop after [`Self::run()`] has started. Returns
    /// `ChatEvent::Message`, `PeerJoin`, `PeerLeave`, or `None` when the
    /// hub has shut down.
    pub async fn next_event(&mut self) -> Option<ChatEvent> {
        if !self.running {
            return None;
        }

        tokio::select! {
            biased;

            // Peer message
            Some((peer_id, wire_msg)) = self.msg_rx.recv() => {
                Some(ChatEvent::Message {
                    from: peer_id,
                    name: wire_msg.name,
                    text: wire_msg.text,
                })
            }

            else => {
                // Both channels are closed or shutting down
                None
            }
        }
    }

    /// Run the accept loop until the underlying onion service is shut down.
    pub async fn run(&mut self) {
        self.running = true;
        while self.accept_next().await {}
        self.running = false;
    }

    /// Shut down the hub (stops accepting, drops peer registry).
    pub fn shutdown(&mut self) {
        self.room.shutdown();
        self.running = false;
    }

    // -- internal --

    /// Accept one peer stream, perform handshake, spawn tasks.
    /// Returns `true` while the room is still accepting.
    async fn accept_next(&mut self) -> bool {
        let stream = match self.room.accept_peer().await {
            Some(s) => s,
            None => return false,
        };
        info!("hub: new peer stream, starting handshake");

        let peers = Arc::clone(&self.peers);
        let nonces = Arc::clone(&self.used_nonces);
        let msg_tx = self.msg_tx.clone();
        let broadcast_tx = self.broadcast_tx.clone();

        tokio::spawn(async move {
            match Self::handle_connection(stream, peers, nonces, msg_tx, broadcast_tx).await {
                Ok(()) => info!("hub: peer connection handled cleanly"),
                Err(e) => warn!("hub: peer connection ended: {e}"),
            }
        });

        true
    }

    /// Full lifecycle for one accepted stream:
    /// handshake → register → reader + writer → deregister → broadcast leave.
    async fn handle_connection(
        stream: DataStream,
        peers: PeerRegistry,
        nonces: NonceSet,
        msg_tx: mpsc::Sender<(PeerId, WireMessage)>,
        broadcast_tx: broadcast::Sender<ChatEvent>,
    ) -> Result<()> {
        // 1. Handshake
        let mut stream = stream;
        let (peer_id, name) = Self::handshake(&mut stream, nonces).await?;
        let joined_at = std::time::Instant::now();

        // 2. Create broadcast receiver for this peer
        let broadcast_rx = broadcast_tx.subscribe();

        // 3. Register peer
        let (tx, rx) = mpsc::unbounded_channel::<Vec<u8>>();
        {
            let mut map = peers.write().await;
            map.insert(
                peer_id.clone(),
                PeerEntry {
                    name: name.clone(),
                    joined_at,
                    tx: tx.clone(),
                    _broadcast_rx: broadcast_rx,
                },
            );
        }
        info!("hub: peer {peer_id} ({name}) admitted");

        // 4. Broadcast join event
        let join_event = ChatEvent::Message {
            from: peer_id.clone(),
            name: name.clone(),
            text: "joined".into(),
        };
        let _ = broadcast_tx.send(join_event);

        // Send join notification to peer
        if let Ok(frame) = encode_message(&WireMessage::system(&format!("{name} joined"))) {
            let _ = tx.send(frame);
        }

        // 5. Split stream into read/write halves
        let (reader_half, writer_half) = tokio::io::split(stream);

        // 6. Spawn reader task
        let r_peer = peer_id.clone();
        let r_name = name.clone();
        let r_peers = Arc::clone(&peers);
        let r_msg = msg_tx.clone();
        let r_broadcast = broadcast_tx.clone();
        let r_tx = tx.clone();
        tokio::spawn(async move {
            Self::reader_task(
                reader_half,
                r_peer,
                r_name,
                r_peers,
                r_msg,
                r_broadcast,
                r_tx,
            )
            .await;
        });

        // 7. Writer task (runs on this task)
        Self::writer_task(rx, broadcast_tx.subscribe(), writer_half).await;

        // 8. Deregister
        {
            let mut map = peers.write().await;
            map.remove(&peer_id);
        }
        info!("hub: peer {peer_id} ({name}) disconnected");

        // 9. Broadcast leave event
        let leave_event = ChatEvent::Message {
            from: peer_id,
            name: name.clone(),
            text: "left".into(),
        };
        let _ = msg_tx
            .send((
                PeerId("[system]".into()),
                WireMessage::system(&format!("{name} left")),
            ))
            .await;
        let _ = broadcast_tx.send(leave_event);

        Ok(())
    }

    /// Wire-protocol handshake.
    ///
    /// Expects `HANDSHAKE_LEN` bytes: `[16 nonce][16 random]`.
    /// - Rejects if nonce was already seen.
    /// - Sends back `[0]` for accept, `[1]` for reject.
    /// - Derives `PeerId` from the 16 random bytes (base58).
    async fn handshake(
        stream: &mut (impl AsyncReadExt + AsyncWriteExt + Unpin),
        nonces: NonceSet,
    ) -> Result<(PeerId, String)> {
        let mut buf = [0u8; HANDSHAKE_LEN];

        let n = timeout(READ_TIMEOUT, stream.read_exact(&mut buf))
            .await
            .map_err(|_| ChatError::Connection("handshake timed out".into()))?
            .map_err(|e| ChatError::Connection(format!("handshake read: {e}")))?;
        if n != HANDSHAKE_LEN {
            stream.write_all(&[1]).await.ok();
            return Err(ChatError::InvalidInvite("handshake wrong length".into()));
        }

        let nonce: [u8; 16] = buf[..16]
            .try_into()
            .expect("handshake buffer is HANDSHAKE_LEN");
        let discriminator: [u8; 16] = buf[16..32]
            .try_into()
            .expect("handshake buffer is HANDSHAKE_LEN");

        // Single-use nonce check
        {
            let mut set = nonces.lock().await;
            if set.contains(&nonce) {
                stream.write_all(&[1]).await.ok();
                return Err(ChatError::NonceReused);
            }
            set.insert(nonce);
        }

        // Build PeerId from the random discriminator
        let peer_id = PeerId(discriminator.to_base58());

        // Accept
        timeout(WRITE_TIMEOUT, stream.write_all(&[0]))
            .await
            .map_err(|_| ChatError::Connection("handshake write timed out".into()))?
            .map_err(|e| ChatError::Connection(format!("handshake write: {e}")))?;

        // Derive a short human-friendly name from the same bytes
        let name = format!("peer-{}", hex::encode(&discriminator[..4]));

        Ok((peer_id, name))
    }

    /// Reader task: reads length-prefixed wire messages from the Tor stream
    /// and pushes decoded messages to the hub's event channel.
    ///
    /// Handles:
    /// - Partial reads (read_frame retries until complete)
    /// - Connection drops (error → break)
    /// - Read timeout (dead peer detection)
    /// - Oversized / malformed messages (error logged, connection closed)
    /// - Ping messages (auto-responds with pong via writer_tx channel)
    async fn reader_task(
        mut stream: impl AsyncReadExt + Unpin,
        peer_id: PeerId,
        name: String,
        _peers: PeerRegistry,
        msg_tx: mpsc::Sender<(PeerId, WireMessage)>,
        broadcast_tx: broadcast::Sender<ChatEvent>,
        writer_tx: mpsc::UnboundedSender<Vec<u8>>,
    ) {
        loop {
            match timeout(READ_TIMEOUT, read_frame(&mut stream)).await {
                Ok(Ok(frame)) => {
                    match crate::wire::decode_message(&frame) {
                        Ok(wire_msg) => {
                            // Auto-respond to pings
                            if wire_msg.kind == crate::wire::MessageType::Ping {
                                if let Ok(pong_frame) = encode_message(&WireMessage::pong()) {
                                    let _ = writer_tx.send(pong_frame);
                                }
                                continue;
                            }

                            // Forward chat/system messages to hub
                            let _ = msg_tx.send((peer_id.clone(), wire_msg.clone())).await;

                            // Broadcast to all peers
                            if wire_msg.kind == crate::wire::MessageType::Chat {
                                let event = ChatEvent::Message {
                                    from: peer_id.clone(),
                                    name: name.clone(),
                                    text: wire_msg.text.clone(),
                                };
                                let _ = broadcast_tx.send(event);
                            }
                        }
                        Err(e) => {
                            warn!("hub: peer {peer_id} sent malformed message: {e}");
                            break;
                        }
                    }
                }
                Ok(Err(e)) => {
                    warn!("hub: read error for peer {peer_id}: {e}");
                    break;
                }
                Err(_) => {
                    warn!("hub: read timeout for peer {peer_id}, considering dead");
                    break;
                }
            }
        }

        info!("hub: reader task ended for {peer_id} ({name})");
    }

    /// Writer task: drains the per-peer channel and broadcast channel,
    /// writing frames to the stream with write timeout protection.
    ///
    /// Handles:
    /// - Slow/dead connections (write timeout → break)
    /// - Broadcast lag (slow peer falls behind → skip event, not block)
    async fn writer_task(
        mut rx: mpsc::UnboundedReceiver<Vec<u8>>,
        mut broadcast_rx: broadcast::Receiver<ChatEvent>,
        mut stream: impl AsyncWriteExt + Unpin,
    ) {
        loop {
            tokio::select! {
                biased;

                // Per-peer message queue
                maybe_frame = rx.recv() => {
                    let frame = match maybe_frame {
                        Some(f) => f,
                        None => break, // channel closed
                    };
                    if timeout(WRITE_TIMEOUT, stream.write_all(&frame)).await.is_err() {
                        warn!("hub: writer task write timeout");
                        break;
                    }
                }

                // Broadcast messages (single recv, match all outcomes)
                br = broadcast_rx.recv() => {
                    match br {
                        Ok(event) => {
                            let wire_msg = match event {
                                ChatEvent::Message { name, text, .. } => {
                                    WireMessage::chat(&name, &text)
                                }
                                ChatEvent::PeerJoin(info) => {
                                    WireMessage::system(&format!("{} joined", info.name))
                                }
                                ChatEvent::PeerLeave(pid) => {
                                    WireMessage::system(&format!("{pid} left"))
                                }
                                _ => continue,
                            };
                            if let Ok(frame) = encode_message(&wire_msg) {
                                if timeout(WRITE_TIMEOUT, stream.write_all(&frame)).await.is_err() {
                                    warn!("hub: writer task broadcast write timeout");
                                    break;
                                }
                            }
                        }
                        Err(broadcast::error::RecvError::Lagged(n)) => {
                            warn!("hub: writer task broadcast lagged {n} messages");
                        }
                        Err(broadcast::error::RecvError::Closed) => {
                            info!("hub: writer task broadcast channel closed");
                            break;
                        }
                    }
                }
            }
        }
        info!("hub: writer task ended");
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;

    #[test]
    fn handshake_nonce_is_16_bytes() {
        assert_eq!(HANDSHAKE_LEN, 32);
    }

    #[tokio::test]
    async fn nonce_set_rejects_duplicates() {
        let nonces: NonceSet = Arc::new(tokio::sync::Mutex::new(HashSet::new()));
        let nonce = [0xAAu8; 16];

        {
            let mut set = nonces.lock().await;
            assert!(!set.contains(&nonce));
            set.insert(nonce);
        }

        {
            let set = nonces.lock().await;
            assert!(set.contains(&nonce));
        }
    }

    #[tokio::test]
    async fn peer_registry_roundtrip() {
        let (_broadcast_tx, broadcast_rx) = broadcast::channel(BROADCAST_CAPACITY);
        let reg: PeerRegistry =
            Arc::new(tokio::sync::RwLock::new(std::collections::HashMap::new()));
        let pid = PeerId("test".into());
        {
            let mut map = reg.write().await;
            map.insert(
                pid.clone(),
                PeerEntry {
                    name: "alice".into(),
                    joined_at: std::time::Instant::now(),
                    tx: mpsc::unbounded_channel().0,
                    _broadcast_rx: broadcast_rx,
                },
            );
        }
        let snapshot: Vec<PeerInfo> = reg
            .read()
            .await
            .iter()
            .map(|(id, e)| PeerInfo {
                id: id.clone(),
                name: e.name.clone(),
                joined_at: e.joined_at,
            })
            .collect();
        assert_eq!(snapshot.len(), 1);
        assert_eq!(snapshot[0].name, "alice");
    }

    #[tokio::test]
    async fn broadcast_channel_fanout() {
        let (tx, _) = broadcast::channel::<ChatEvent>(BROADCAST_CAPACITY);

        let mut rx1 = tx.subscribe();
        let mut rx2 = tx.subscribe();

        let event = ChatEvent::Message {
            from: PeerId("a".into()),
            name: "alice".into(),
            text: "hello".into(),
        };
        tx.send(event.clone()).unwrap();

        assert!(matches!(
            rx1.recv().await.unwrap(),
            ChatEvent::Message { .. }
        ));
        assert!(matches!(
            rx2.recv().await.unwrap(),
            ChatEvent::Message { .. }
        ));
    }

    #[test]
    fn broadcast_channel_overflow_does_not_block() {
        // Create a tiny channel that will overflow
        let (tx, _rx) = broadcast::channel::<ChatEvent>(2);

        // Send more messages than capacity — should not panic or block
        for i in 0..10 {
            let event = ChatEvent::Message {
                from: PeerId(format!("peer-{i}")),
                name: format!("name-{i}"),
                text: format!("msg-{i}"),
            };
            let _ = tx.send(event);
        }
    }
}
