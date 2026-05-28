//! Hosted onion service (hub/room) and connection management.
//!
//! Algorithm:
//!   Hub::run() → loop: accept_next() until room shutdown
//!
//!   Per-connection lifecycle:
//!     1. handshake(stream, nonces) → (PeerId, name)
//!     2. register_peer(peers, peer_id, name, broadcast_tx) → (tx, rx)
//!     3. run_peer_io(stream, peer_id, name, peers, msg_tx, broadcast_tx, tx, rx)
//!     4. deregister_peer(peers, peer_id, name, msg_tx, broadcast_tx)

use arti_client::{config::onion_service::OnionServiceConfigBuilder, DataStream, TorClient};
use base58::ToBase58;
use futures::StreamExt;
use safelog::DisplayRedacted;
use std::collections::HashSet;
use std::collections::HashMap;
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
use crate::wire::{encode_message, WireMessage};

/// Handshake size: 16-byte nonce + 16-byte random peer discriminator.
const HANDSHAKE_LEN: usize = 32;

/// Read timeout for detecting dead peers.
const READ_TIMEOUT: Duration = Duration::from_secs(60);

/// Write timeout for avoiding blocked writes.
const WRITE_TIMEOUT: Duration = Duration::from_secs(30);

/// Broadcast channel capacity per peer.
const BROADCAST_CAPACITY: usize = 256;

// ---------------------------------------------------------------------------
// HostedRoom — v3 onion service lifecycle (unchanged)
// ---------------------------------------------------------------------------

pub struct HostedRoom {
    running_svc: Option<Arc<RunningOnionService>>,
    address: Option<String>,
    port: u16,
    stream_rx: Option<mpsc::Receiver<DataStream>>,
    _join_handle: Option<tokio::task::JoinHandle<()>>,
}

impl HostedRoom {
    pub async fn new(tor_client: &TorClient<PreferredRuntime>, port: u16) -> Result<Self> {
        static NEXT_ID: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        let id = NEXT_ID.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let nickname = HsNickname::new(format!("chat-room-{id}"))
            .map_err(|e| ChatError::OnionService(format!("invalid nickname: {e}")))?;

        let svc_config = OnionServiceConfigBuilder::default()
            .nickname(nickname)
            .build()
            .map_err(|e| ChatError::OnionService(format!("config build failed: {e}")))?;

        let launch_result = tor_client
            .launch_onion_service(svc_config)
            .map_err(|e| ChatError::OnionService(format!("launch failed: {e}")))?;

        let Some((running_svc, rend_stream)) = launch_result else {
            return Err(ChatError::OnionService("onion service disabled in config".into()));
        };

        let onion_address = running_svc
            .onion_address()
            .ok_or_else(|| ChatError::OnionService("could not get onion address".into()))?
            .display_unredacted()
            .to_string();
        info!("onion service ready: {onion_address}");

        let (stream_tx, stream_rx) = mpsc::channel::<DataStream>(4);
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
                Err(e) => warn!("failed to accept stream request: {e}"),
            }
        }
        info!("onion service accept loop ended");
    }

    pub fn address(&self) -> &str {
        self.address.as_deref().unwrap_or("")
    }

    pub fn port(&self) -> u16 {
        self.port
    }

    pub fn ready_event(&self) -> ChatEvent {
        ChatEvent::RoomReady {
            onion_address: self.address().to_string(),
            port: self.port,
        }
    }

    pub async fn accept_peer(&mut self) -> Option<DataStream> {
        self.stream_rx.as_mut()?.recv().await
    }

    pub fn shutdown(&mut self) {
        if self.running_svc.take().is_some() {
            info!("onion service shut down");
        }
        self.stream_rx = None;
        self.address = None;
    }

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
// PeerRegistry — thread-safe peer tracking
// ---------------------------------------------------------------------------

type PeerTx = mpsc::UnboundedSender<Vec<u8>>;
type NonceSet = Arc<tokio::sync::Mutex<HashSet<[u8; 16]>>>;

struct PeerEntry {
    name: String,
    joined_at: std::time::Instant,
    tx: PeerTx,
}

pub struct PeerRegistry {
    inner: Arc<tokio::sync::RwLock<HashMap<PeerId, PeerEntry>>>,
}

impl PeerRegistry {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(tokio::sync::RwLock::new(HashMap::new())),
        }
    }

    pub async fn register(&self, peer_id: PeerId, name: String, tx: PeerTx) {
        let entry = PeerEntry {
            name,
            joined_at: std::time::Instant::now(),
            tx,
        };
        self.inner.write().await.insert(peer_id, entry);
    }

    pub async fn deregister(&self, peer_id: &PeerId) {
        self.inner.write().await.remove(peer_id);
    }

    pub async fn snapshot(&self) -> Vec<PeerInfo> {
        self.inner
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

    pub async fn len(&self) -> usize {
        self.inner.read().await.len()
    }

    pub async fn broadcast_to_all(&self, frame: &[u8]) {
        for entry in self.inner.read().await.values() {
            let _ = entry.tx.send(frame.to_vec());
        }
    }
}

// ---------------------------------------------------------------------------
// Hub — connection lifecycle orchestrator
// ---------------------------------------------------------------------------

pub struct Hub {
    room: HostedRoom,
    peers: Arc<PeerRegistry>,
    used_nonces: NonceSet,
    broadcast_tx: broadcast::Sender<ChatEvent>,
    msg_rx: mpsc::Receiver<(PeerId, WireMessage)>,
    msg_tx: mpsc::Sender<(PeerId, WireMessage)>,
    running: bool,
}

impl Hub {
    pub fn new(room: HostedRoom) -> Self {
        let (broadcast_tx, _) = broadcast::channel(BROADCAST_CAPACITY);
        let (msg_tx, msg_rx) = mpsc::channel::<(PeerId, WireMessage)>(128);
        Self {
            room,
            peers: Arc::new(PeerRegistry::new()),
            used_nonces: Arc::new(tokio::sync::Mutex::new(HashSet::new())),
            broadcast_tx,
            msg_rx,
            msg_tx,
            running: false,
        }
    }

    // -- public API --

    pub fn address(&self) -> &str {
        self.room.address()
    }

    pub fn port(&self) -> u16 {
        self.room.port()
    }

    pub async fn peers(&self) -> Vec<PeerInfo> {
        self.peers.snapshot().await
    }

    pub async fn peer_count(&self) -> usize {
        self.peers.len().await
    }

    pub async fn broadcast_hub(&self, text: &str) {
        let _ = text;
        todo!("step 2: broadcast message via PeerRegistry")
    }

    pub async fn broadcast_ping(&self) {
        todo!("step 2: broadcast ping via PeerRegistry")
    }

    pub async fn next_event(&mut self) -> Option<ChatEvent> {
        if !self.running {
            return None;
        }

        tokio::select! {
            biased;
            Some((peer_id, wire_msg)) = self.msg_rx.recv() => {
                Some(ChatEvent::Message {
                    from: peer_id,
                    name: wire_msg.name,
                    text: wire_msg.text,
                })
            }
            else => None,
        }
    }

    pub async fn run(&mut self) {
        self.running = true;
        while self.accept_next().await {}
        self.running = false;
    }

    pub fn shutdown(&mut self) {
        self.room.shutdown();
        self.running = false;
    }

    // -- accept loop --

    async fn accept_next(&mut self) -> bool {
        let Some(stream) = self.room.accept_peer().await else {
            return false;
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

    // -- connection lifecycle (stubs — phases implemented incrementally in steps 4-7) --

    /// Orchestrator: handshake → register → run I/O → deregister.
    async fn handle_connection(
        stream: DataStream,
        peers: Arc<PeerRegistry>,
        nonces: NonceSet,
        msg_tx: mpsc::Sender<(PeerId, WireMessage)>,
        broadcast_tx: broadcast::Sender<ChatEvent>,
    ) -> Result<()> {
        let mut stream = stream;

        // Phase 1
        let (peer_id, name) = Self::handshake(&mut stream, nonces).await?;

        // Phase 2
        let (tx, rx) = Self::register_peer(&peers, &peer_id, &name, &broadcast_tx).await;

        // Phase 3
        Self::run_peer_io(stream, &peer_id, &name, &peers, msg_tx.clone(), broadcast_tx.clone(), tx, rx).await;

        // Phase 4
        Self::deregister_peer(&peers, &peer_id, &name, &msg_tx, &broadcast_tx).await;

        Ok(())
    }

    /// Phase 2: register peer, broadcast join, send join frame.
    async fn register_peer(
        peers: &PeerRegistry,
        peer_id: &PeerId,
        name: &str,
        broadcast_tx: &broadcast::Sender<ChatEvent>,
    ) -> (PeerTx, mpsc::UnboundedReceiver<Vec<u8>>) {
        let (tx, rx) = mpsc::unbounded_channel::<Vec<u8>>();
        let _ = (peers, peer_id, name, broadcast_tx, &tx);
        todo!("step 5: implement register_peer")
    }

    /// Phase 3: split stream, spawn reader, run writer.
    async fn run_peer_io(
        stream: DataStream,
        peer_id: &PeerId,
        name: &str,
        peers: &Arc<PeerRegistry>,
        msg_tx: mpsc::Sender<(PeerId, WireMessage)>,
        broadcast_tx: broadcast::Sender<ChatEvent>,
        tx: PeerTx,
        rx: mpsc::UnboundedReceiver<Vec<u8>>,
    ) {
        let _ = (stream, peer_id, name, peers, msg_tx, broadcast_tx, tx, rx);
        todo!("step 6: implement run_peer_io")
    }

    /// Phase 4: deregister peer, broadcast leave.
    async fn deregister_peer(
        peers: &PeerRegistry,
        peer_id: &PeerId,
        name: &str,
        msg_tx: &mpsc::Sender<(PeerId, WireMessage)>,
        broadcast_tx: &broadcast::Sender<ChatEvent>,
    ) {
        let _ = (peers, peer_id, name, msg_tx, broadcast_tx);
        todo!("step 5: implement deregister_peer")
    }

    // -- wire protocol (stub — implemented in step 4) --

    async fn handshake(
        _stream: &mut (impl AsyncReadExt + AsyncWriteExt + Unpin),
        _nonces: NonceSet,
    ) -> Result<(PeerId, String)> {
        todo!("step 4: implement handshake")
    }

    // -- per-peer I/O tasks (stubs — implemented in steps 6-7) --

    async fn reader_task(
        _stream: impl AsyncReadExt + Unpin,
        _peer_id: PeerId,
        _name: String,
        _peers: Arc<PeerRegistry>,
        _msg_tx: mpsc::Sender<(PeerId, WireMessage)>,
        _broadcast_tx: broadcast::Sender<ChatEvent>,
        _writer_tx: mpsc::UnboundedSender<Vec<u8>>,
    ) {
        todo!("step 6: implement reader_task")
    }

    async fn writer_task(
        _rx: mpsc::UnboundedReceiver<Vec<u8>>,
        _broadcast_rx: broadcast::Receiver<ChatEvent>,
        _stream: impl AsyncWriteExt + Unpin,
    ) {
        todo!("step 7: implement writer_task using write_frame")
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;
    use tokio::io::duplex;
    use crate::wire::{decode_message, MessageType};

    // ── sanity / constant tests (should pass) ──

    #[test]
    fn handshake_len_is_32_bytes() {
        assert_eq!(HANDSHAKE_LEN, 32);
    }

    #[test]
    fn read_timeout_is_60_secs() {
        assert_eq!(READ_TIMEOUT, Duration::from_secs(60));
    }

    #[test]
    fn write_timeout_is_30_secs() {
        assert_eq!(WRITE_TIMEOUT, Duration::from_secs(30));
    }

    #[test]
    fn broadcast_capacity_is_256() {
        assert_eq!(BROADCAST_CAPACITY, 256);
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

    // ── PeerRegistry tests (stub — all fail with todo!) ──

    #[tokio::test]
    async fn peer_registry_len_starts_at_zero() {
        let reg = PeerRegistry::new();
        assert_eq!(reg.len().await, 0);
    }

    #[tokio::test]
    async fn peer_registry_register_increments_len() {
        let reg = PeerRegistry::new();
        let (tx, _) = mpsc::unbounded_channel();
        reg.register(PeerId("alice".into()), "alice".into(), tx).await;
        assert_eq!(reg.len().await, 1);
    }

    #[tokio::test]
    async fn peer_registry_register_multiple_peers() {
        let reg = PeerRegistry::new();
        let (tx1, _) = mpsc::unbounded_channel();
        let (tx2, _) = mpsc::unbounded_channel();
        reg.register(PeerId("a".into()), "alice".into(), tx1).await;
        reg.register(PeerId("b".into()), "bob".into(), tx2).await;
        assert_eq!(reg.len().await, 2);
    }

    #[tokio::test]
    async fn peer_registry_register_same_id_twice_overwrites() {
        let reg = PeerRegistry::new();
        let (tx1, _) = mpsc::unbounded_channel();
        let (tx2, _) = mpsc::unbounded_channel();
        reg.register(PeerId("alice".into()), "alice".into(), tx1).await;
        reg.register(PeerId("alice".into()), "alice2".into(), tx2).await;
        assert_eq!(reg.len().await, 1);
    }

    #[tokio::test]
    async fn peer_registry_deregister_decrements_len() {
        let reg = PeerRegistry::new();
        let (tx, _) = mpsc::unbounded_channel();
        reg.register(PeerId("alice".into()), "alice".into(), tx).await;
        reg.deregister(&PeerId("alice".into())).await;
        assert_eq!(reg.len().await, 0);
    }

    #[tokio::test]
    async fn peer_registry_deregister_unknown_is_noop() {
        let reg = PeerRegistry::new();
        let (tx, _) = mpsc::unbounded_channel();
        reg.register(PeerId("alice".into()), "alice".into(), tx).await;
        reg.deregister(&PeerId("ghost".into())).await;
        assert_eq!(reg.len().await, 1);
    }

    #[tokio::test]
    async fn peer_registry_deregister_empty_registry_no_panic() {
        let reg = PeerRegistry::new();
        reg.deregister(&PeerId("ghost".into())).await;
        assert_eq!(reg.len().await, 0);
    }

    #[tokio::test]
    async fn peer_registry_snapshot_returns_all_peers() {
        let reg = PeerRegistry::new();
        let (tx1, _) = mpsc::unbounded_channel();
        let (tx2, _) = mpsc::unbounded_channel();
        reg.register(PeerId("a".into()), "alice".into(), tx1).await;
        reg.register(PeerId("b".into()), "bob".into(), tx2).await;

        let snap = reg.snapshot().await;
        assert_eq!(snap.len(), 2);

        let mut ids: Vec<_> = snap.iter().map(|p| p.id.0.clone()).collect();
        ids.sort();
        assert_eq!(ids, vec!["a", "b"]);

        let mut names: Vec<_> = snap.iter().map(|p| p.name.clone()).collect();
        names.sort();
        assert_eq!(names, vec!["alice", "bob"]);
    }

    #[tokio::test]
    async fn peer_registry_snapshot_empty_registry() {
        let reg = PeerRegistry::new();
        assert!(reg.snapshot().await.is_empty());
    }

    #[tokio::test]
    async fn peer_registry_broadcast_to_all_delivers_to_each_peer() {
        let reg = PeerRegistry::new();
        let (tx1, mut rx1) = mpsc::unbounded_channel();
        let (tx2, mut rx2) = mpsc::unbounded_channel();
        reg.register(PeerId("a".into()), "alice".into(), tx1).await;
        reg.register(PeerId("b".into()), "bob".into(), tx2).await;

        let frame: Vec<u8> = b"hello everyone".to_vec();
        reg.broadcast_to_all(&frame).await;

        assert_eq!(rx1.recv().await, Some(frame.clone()));
        assert_eq!(rx2.recv().await, Some(frame));
    }

    #[tokio::test]
    async fn peer_registry_broadcast_to_empty_registry_no_panic() {
        let reg = PeerRegistry::new();
        reg.broadcast_to_all(b"hello").await;
    }

    #[tokio::test]
    async fn peer_registry_broadcast_skips_deregistered_peers() {
        let reg = PeerRegistry::new();
        let (tx1, mut rx1) = mpsc::unbounded_channel();
        let (tx2, _) = mpsc::unbounded_channel();
        reg.register(PeerId("a".into()), "alice".into(), tx1).await;
        reg.register(PeerId("b".into()), "bob".into(), tx2).await;
        reg.deregister(&PeerId("b".into())).await;

        let frame: Vec<u8> = b"only for alice".to_vec();
        reg.broadcast_to_all(&frame).await;

        assert_eq!(rx1.recv().await, Some(frame));
    }

    #[tokio::test]
    async fn peer_registry_register_and_deregister_toggle() {
        let reg = PeerRegistry::new();
        let (tx, _) = mpsc::unbounded_channel();

        reg.register(PeerId("x".into()), "x".into(), tx.clone()).await;
        assert_eq!(reg.len().await, 1);

        reg.deregister(&PeerId("x".into())).await;
        assert_eq!(reg.len().await, 0);

        reg.register(PeerId("x".into()), "x".into(), tx).await;
        assert_eq!(reg.len().await, 1);
    }

    #[tokio::test]
    async fn peer_registry_snapshot_includes_joined_at() {
        let reg = PeerRegistry::new();
        let (tx, _) = mpsc::unbounded_channel();
        let before = std::time::Instant::now();
        reg.register(PeerId("alice".into()), "alice".into(), tx).await;
        let after = std::time::Instant::now();

        let snap = reg.snapshot().await;
        assert_eq!(snap.len(), 1);
        let info = &snap[0];
        assert!(info.joined_at >= before);
        assert!(info.joined_at <= after);
    }

    // ── Hub::handshake tests (stub — all fail with todo!) ──

    #[tokio::test]
    async fn handshake_accepts_valid_nonce() {
        let nonces: NonceSet = Arc::new(tokio::sync::Mutex::new(HashSet::new()));
        let (mut client, mut server) = duplex(64);

        let nonce = [0x42u8; 16];
        let discriminator = [0xABu8; 16];
        let mut buf = [0u8; 32];
        buf[..16].copy_from_slice(&nonce);
        buf[16..].copy_from_slice(&discriminator);
        client.write_all(&buf).await.unwrap();
        client.shutdown().await.unwrap();

        let result = Hub::handshake(&mut server, nonces).await;
        assert!(result.is_ok());
        let (peer_id, name) = result.unwrap();
        assert_eq!(peer_id, PeerId(discriminator.to_base58()));
        assert_eq!(name, format!("peer-{}", hex::encode(&discriminator[..4])));
    }

    #[tokio::test]
    async fn handshake_rejects_reused_nonce() {
        let nonces: NonceSet = Arc::new(tokio::sync::Mutex::new(HashSet::new()));
        {
            let mut set = nonces.lock().await;
            set.insert([0x42u8; 16]);
        }

        let (mut client, mut server) = duplex(64);

        let mut buf = [0u8; 32];
        buf[..16].copy_from_slice(&[0x42u8; 16]);
        buf[16..].copy_from_slice(&[0xABu8; 16]);
        client.write_all(&buf).await.unwrap();
        client.shutdown().await.unwrap();

        let result = Hub::handshake(&mut server, nonces).await;
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), ChatError::NonceReused));
    }

    #[tokio::test]
    async fn handshake_rejects_wrong_length() {
        let nonces: NonceSet = Arc::new(tokio::sync::Mutex::new(HashSet::new()));
        let (mut client, mut server) = duplex(64);

        // Write only 16 bytes instead of 32
        client.write_all(&[0u8; 16]).await.unwrap();
        client.shutdown().await.unwrap();

        let result = Hub::handshake(&mut server, nonces).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn handshake_nonce_is_consumed_on_success() {
        let nonces: NonceSet = Arc::new(tokio::sync::Mutex::new(HashSet::new()));
        let (mut client, mut server) = duplex(64);

        let mut buf = [0u8; 32];
        buf[16..].copy_from_slice(&[0xABu8; 16]);
        client.write_all(&buf).await.unwrap();
        client.shutdown().await.unwrap();

        let _ = Hub::handshake(&mut server, nonces.clone()).await.unwrap();

        let set = nonces.lock().await;
        assert!(set.contains(&[0x00u8; 16]));
    }

    #[tokio::test]
    async fn handshake_writes_ack_on_success() {
        let nonces: NonceSet = Arc::new(tokio::sync::Mutex::new(HashSet::new()));
        let (mut client, mut server) = duplex(64);

        let mut buf = [0u8; 32];
        buf[16..].copy_from_slice(&[0xABu8; 16]);
        client.write_all(&buf).await.unwrap();

        let _ = Hub::handshake(&mut server, nonces).await.unwrap();

        let mut ack = [0u8; 1];
        client.read_exact(&mut ack).await.unwrap();
        assert_eq!(ack[0], 0);
    }

    #[tokio::test]
    async fn handshake_writes_nack_on_rejected_nonce() {
        let nonces: NonceSet = Arc::new(tokio::sync::Mutex::new(HashSet::new()));
        {
            let mut set = nonces.lock().await;
            set.insert([0x42u8; 16]);
        }
        let (mut client, mut server) = duplex(64);

        let mut buf = [0u8; 32];
        buf[..16].copy_from_slice(&[0x42u8; 16]);
        buf[16..].copy_from_slice(&[0xABu8; 16]);
        client.write_all(&buf).await.unwrap();

        let _ = Hub::handshake(&mut server, nonces).await;

        let mut nack = [0u8; 1];
        client.read_exact(&mut nack).await.unwrap();
        assert_eq!(nack[0], 1);
    }

    #[tokio::test]
    async fn handshake_writes_nack_on_bad_length() {
        let nonces: NonceSet = Arc::new(tokio::sync::Mutex::new(HashSet::new()));
        let (mut client, mut server) = duplex(64);

        client.write_all(&[0u8; 16]).await.unwrap();

        let _ = Hub::handshake(&mut server, nonces).await;

        let mut nack = [0u8; 1];
        client.read_exact(&mut nack).await.unwrap();
        assert_eq!(nack[0], 1);
    }

    // ── Hub::register_peer tests (stub — all fail with todo!) ──

    #[tokio::test]
    async fn register_peer_returns_tx_rx_pair() {
        let reg = PeerRegistry::new();
        let (broadcast_tx, _) = broadcast::channel::<ChatEvent>(16);

        let (tx, mut rx) = Hub::register_peer(&reg, &PeerId("alice".into()), "alice", &broadcast_tx).await;
        let _ = tx.send(vec![1, 2, 3]).unwrap();
        assert_eq!(rx.recv().await, Some(vec![1, 2, 3]));
    }

    #[tokio::test]
    async fn register_peer_adds_to_registry() {
        let reg = PeerRegistry::new();
        let (broadcast_tx, _) = broadcast::channel::<ChatEvent>(16);

        let (tx, _) = Hub::register_peer(&reg, &PeerId("alice".into()), "alice", &broadcast_tx).await;
        let _ = tx;
        assert_eq!(reg.len().await, 1);
    }

    #[tokio::test]
    async fn register_peer_emits_peer_join_event() {
        let reg = PeerRegistry::new();
        let (broadcast_tx, mut broadcast_rx) = broadcast::channel::<ChatEvent>(16);

        let (tx, _) = Hub::register_peer(&reg, &PeerId("bob".into()), "bob", &broadcast_tx).await;
        let _ = tx;

        let event = broadcast_rx.recv().await.unwrap();
        match event {
            ChatEvent::PeerJoin(info) => {
                assert_eq!(info.id, PeerId("bob".into()));
                assert_eq!(info.name, "bob");
            }
            other => panic!("expected PeerJoin, got {other:?}"),
        }
    }

    // ── Hub::deregister_peer tests (stub — all fail with todo!) ──

    #[tokio::test]
    async fn deregister_peer_removes_from_registry() {
        let reg = PeerRegistry::new();
        let (msg_tx, _) = mpsc::channel(1);
        let (broadcast_tx, _) = broadcast::channel::<ChatEvent>(16);

        let (tx, _) = Hub::register_peer(&reg, &PeerId("alice".into()), "alice", &broadcast_tx).await;
        let _ = tx;
        assert_eq!(reg.len().await, 1);

        Hub::deregister_peer(&reg, &PeerId("alice".into()), "alice", &msg_tx, &broadcast_tx).await;
        assert_eq!(reg.len().await, 0);
    }

    #[tokio::test]
    async fn deregister_peer_emits_peer_leave_event() {
        let reg = PeerRegistry::new();
        let (msg_tx, _) = mpsc::channel(1);
        let (broadcast_tx, mut broadcast_rx) = broadcast::channel::<ChatEvent>(16);

        let (tx, _) = Hub::register_peer(&reg, &PeerId("carol".into()), "carol", &broadcast_tx).await;
        let _ = tx;

        Hub::deregister_peer(&reg, &PeerId("carol".into()), "carol", &msg_tx, &broadcast_tx).await;

        let event = broadcast_rx.recv().await.unwrap();
        assert!(matches!(event, ChatEvent::PeerLeave(_)));
        if let ChatEvent::PeerLeave(id) = event {
            assert_eq!(id, PeerId("carol".into()));
        }
    }

    #[tokio::test]
    async fn deregister_peer_deregistering_unknown_is_noop() {
        let reg = PeerRegistry::new();
        let (msg_tx, _) = mpsc::channel(1);
        let (broadcast_tx, _) = broadcast::channel::<ChatEvent>(16);

        Hub::deregister_peer(&reg, &PeerId("ghost".into()), "ghost", &msg_tx, &broadcast_tx).await;
        assert_eq!(reg.len().await, 0);
    }

    // ── Hub::reader_task tests (stub — all fail with todo!) ──

    #[tokio::test]
    async fn reader_task_forwards_chat_message() {
        let (msg_tx, mut msg_rx) = mpsc::channel::<(PeerId, WireMessage)>(4);
        let (broadcast_tx, _) = broadcast::channel::<ChatEvent>(16);
        let (writer_tx, _writer_rx) = mpsc::unbounded_channel();
        let reg = Arc::new(PeerRegistry::new());

        let (mut input, reader) = duplex(128);
        let msg = WireMessage::chat("alice", "hello");
        let frame = encode_message(&msg).unwrap();
        input.write_all(&frame).await.unwrap();
        input.shutdown().await.unwrap();

        Hub::reader_task(
            reader,
            PeerId("p".into()),
            "alice".into(),
            reg,
            msg_tx,
            broadcast_tx,
            writer_tx,
        )
        .await;

        let (peer_id, wire_msg) = msg_rx.recv().await.unwrap();
        assert_eq!(peer_id, PeerId("p".into()));
        assert_eq!(wire_msg.kind, MessageType::Chat);
        assert_eq!(wire_msg.name, "alice");
        assert_eq!(wire_msg.text, "hello");
    }

    #[tokio::test]
    async fn reader_task_responds_to_ping_with_pong() {
        let (msg_tx, _msg_rx) = mpsc::channel::<(PeerId, WireMessage)>(4);
        let (broadcast_tx, _) = broadcast::channel::<ChatEvent>(16);
        let (writer_tx, mut writer_rx) = mpsc::unbounded_channel();
        let reg = Arc::new(PeerRegistry::new());

        let (mut input, reader) = duplex(128);
        let ping = WireMessage::ping();
        let frame = encode_message(&ping).unwrap();
        input.write_all(&frame).await.unwrap();
        input.shutdown().await.unwrap();

        Hub::reader_task(
            reader,
            PeerId("p".into()),
            "alice".into(),
            reg,
            msg_tx,
            broadcast_tx,
            writer_tx,
        )
        .await;

        let pong_frame = writer_rx.recv().await.unwrap();
        let pong = decode_message(&pong_frame).unwrap();
        assert_eq!(pong.kind, MessageType::Pong);
    }

    #[tokio::test]
    async fn reader_task_handles_eof_gracefully() {
        let (msg_tx, _msg_rx) = mpsc::channel::<(PeerId, WireMessage)>(4);
        let (broadcast_tx, _) = broadcast::channel::<ChatEvent>(16);
        let (writer_tx, _writer_rx) = mpsc::unbounded_channel();
        let reg = Arc::new(PeerRegistry::new());

        let (_input, reader) = duplex(64);

        // Reader on an empty closed stream should not panic
        Hub::reader_task(
            reader,
            PeerId("p".into()),
            "alice".into(),
            reg,
            msg_tx,
            broadcast_tx,
            writer_tx,
        )
        .await;
    }

    #[tokio::test]
    async fn reader_task_stops_on_malformed_data() {
        let (msg_tx, _msg_rx) = mpsc::channel::<(PeerId, WireMessage)>(4);
        let (broadcast_tx, _) = broadcast::channel::<ChatEvent>(16);
        let (writer_tx, _writer_rx) = mpsc::unbounded_channel();
        let reg = Arc::new(PeerRegistry::new());

        let (mut input, reader) = duplex(64);
        // Write garbage that's not a valid frame
        input.write_all(b"not-a-frame").await.unwrap();
        input.shutdown().await.unwrap();

        Hub::reader_task(
            reader,
            PeerId("p".into()),
            "alice".into(),
            reg,
            msg_tx,
            broadcast_tx,
            writer_tx,
        )
        .await;
        // Should not panic — reader stops on error
    }

    // ── Hub::writer_task tests (stub — all fail with todo!) ──

    #[tokio::test]
    async fn writer_task_writes_from_direct_channel() {
        let (tx, rx) = mpsc::unbounded_channel::<Vec<u8>>();
        let (broadcast_tx, _broadcast_rx) = broadcast::channel::<ChatEvent>(16);
        let (mut input, writer) = duplex(64);

        let frame = encode_message(&WireMessage::chat("alice", "hi")).unwrap();
        tx.send(frame.clone()).unwrap();
        // After sending one frame, drop tx so writer exits
        drop(tx);

        Hub::writer_task(rx, broadcast_tx.subscribe(), writer).await;

        let mut buf = vec![0u8; 1024];
        let n = input.read(&mut buf).await.unwrap();
        buf.truncate(n);
        assert_eq!(buf, frame);
    }

    #[tokio::test]
    async fn writer_task_writes_from_broadcast_channel() {
        let (tx, rx) = mpsc::unbounded_channel::<Vec<u8>>();
        let (broadcast_tx, _broadcast_rx) = broadcast::channel::<ChatEvent>(16);
        let (mut input, writer) = duplex(64);

        drop(tx);

        let frame = encode_message(&WireMessage::chat("bob", "hey")).unwrap();
        let _ = broadcast_tx.send(ChatEvent::Message {
            from: PeerId("b".into()),
            name: "bob".into(),
            text: "hey".into(),
        });
        // Subscribe before dropping to avoid use-after-move
        let sub = broadcast_tx.subscribe();
        drop(broadcast_tx);

        Hub::writer_task(rx, sub, writer).await;

        let mut buf = vec![0u8; 1024];
        let n = input.read(&mut buf).await.unwrap();
        buf.truncate(n);
        assert_eq!(buf, frame);
    }

    #[tokio::test]
    async fn writer_task_exits_when_all_channels_closed() {
        let (tx, rx) = mpsc::unbounded_channel::<Vec<u8>>();
        let (broadcast_tx, _broadcast_rx) = broadcast::channel::<ChatEvent>(16);
        let (_input, writer) = duplex(64);

        let sub = broadcast_tx.subscribe();
        drop(tx);
        drop(broadcast_tx);

        // Should not panic — exits cleanly when both channels closed
        Hub::writer_task(rx, sub, writer).await;
    }

    // ── Hub::handle_connection tests removed ──
    // These need an actual `DataStream` (arti_client) and cannot be tested with duplex.
    // They would cascade-fail through handshake → register → run_peer_io → deregister.
}
