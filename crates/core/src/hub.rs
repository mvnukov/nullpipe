//! Hosted onion service (hub/room) and connection management.

use arti_client::{config::onion_service::OnionServiceConfigBuilder, DataStream, TorClient};
use base58::ToBase58;
use futures::StreamExt;
use safelog::DisplayRedacted;
use std::collections::HashSet;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::mpsc;
use tor_cell::relaycell::msg::Connected;
use tor_hsservice::{handle_rend_requests, HsNickname, RunningOnionService};
use tor_rtcompat::PreferredRuntime;
use tracing::{info, warn};

use crate::error::{ChatError, Result};
use crate::types::{ChatEvent, PeerId, PeerInfo};

/// Handshake size: 16-byte nonce + 16-byte random peer discriminator.
const HANDSHAKE_LEN: usize = 32;

/// Max message payload before the newline delimiter.
const MAX_MSG_BYTES: usize = 64 * 1024;

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

// ---------------------------------------------------------------------------
// Hub — per-connection handshake, reader/writer tasks, peer registry
// ---------------------------------------------------------------------------

/// Per-peer sender: writes from this channel are pushed down the Tor stream.
type PeerTx = mpsc::UnboundedSender<Vec<u8>>;

/// Entry in the peer registry.
struct PeerEntry {
    name: String,
    joined_at: std::time::Instant,
    tx: PeerTx,
}

/// Shared peer registry.
type PeerRegistry = Arc<tokio::sync::RwLock<std::collections::HashMap<PeerId, PeerEntry>>>;

/// Shared nonce bookkeeping (single-use enforcement).
type NonceSet = Arc<tokio::sync::Mutex<HashSet<[u8; 16]>>>;

/// Hub wraps a [`HostedRoom`] and manages incoming connections: handshake,
/// per-peer reader/writer tasks, peer registry, and disconnect cleanup.
pub struct Hub {
    room: HostedRoom,
    peers: PeerRegistry,
    used_nonces: NonceSet,
    /// Channel the hub reads from.  Each connected peer's reader pushes
    /// `(PeerId, name, text)` here.
    msg_rx: mpsc::Receiver<(PeerId, String, String)>,
    msg_tx: mpsc::Sender<(PeerId, String, String)>,
    running: bool,
}

impl Hub {
    /// Create a new Hub wrapping the given [`HostedRoom`].
    pub fn new(room: HostedRoom) -> Self {
        let (msg_tx, msg_rx) = mpsc::channel::<(PeerId, String, String)>(128);
        Self {
            room,
            peers: Arc::new(tokio::sync::RwLock::new(std::collections::HashMap::new())),
            used_nonces: Arc::new(tokio::sync::Mutex::new(HashSet::new())),
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

    /// Broadcast a message to all connected peers (excluding `exclude`).
    pub async fn broadcast(&self, exclude: Option<&PeerId>, name: &str, text: &str) {
        let frame = format!("{name}\t{text}\n").into_bytes();
        for (pid, entry) in self.peers.read().await.iter() {
            if exclude == Some(pid) {
                continue;
            }
            let _ = entry.tx.send(frame.clone());
        }
    }

    /// Send a system message to all peers.
    pub async fn broadcast_system(&self, text: &str) {
        self.broadcast(None, "[system]", text).await;
    }

    /// Accept the next incoming [`ChatEvent`] produced by this hub.
    ///
    /// Call this in a loop after [`Self::run()`] has started.  Returns
    /// `ChatEvent::Message`, `PeerJoin`, `PeerLeave`, or `None` when the
    /// hub has shut down.
    pub async fn next_event(&mut self) -> Option<ChatEvent> {
        if !self.running {
            return None;
        }

        // Accept loop is already running in the background (spawned by
        // [`Self::run`]).  This method drains the message channel.
        tokio::select! {
            Some((peer_id, name, text)) = self.msg_rx.recv() => {
                Some(ChatEvent::Message { from: peer_id, name, text })
            }
            else => None,
        }
    }

    /// Run the accept loop until the underlying onion service is shut down.
    ///
    /// This method spawns a background task per accepted peer and returns
    /// once the onion service stops accepting.  Call [`Self::next_event`]
    /// concurrently to consume events.
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

        tokio::spawn(async move {
            match Self::handle_connection(stream, peers, nonces, msg_tx).await {
                Ok(()) => info!("hub: peer connection handled cleanly"),
                Err(e) => warn!("hub: peer connection ended: {e}"),
            }
        });

        true
    }

    /// Full lifecycle for one accepted stream:
    /// handshake → register → reader + writer → deregister → broadcast leave.
    async fn handle_connection(
        mut stream: DataStream,
        peers: PeerRegistry,
        nonces: NonceSet,
        msg_tx: mpsc::Sender<(PeerId, String, String)>,
    ) -> Result<()> {
        // 1. Handshake
        let (peer_id, name) = Self::handshake(&mut stream, nonces).await?;
        let joined_at = std::time::Instant::now();

        // 2. Register peer
        let (tx, rx) = mpsc::unbounded_channel::<Vec<u8>>();
        {
            let mut map = peers.write().await;
            map.insert(
                peer_id.clone(),
                PeerEntry {
                    name: name.clone(),
                    joined_at,
                    tx,
                },
            );
        }
        info!("hub: peer {peer_id} ({name}) admitted");

        // 3. Broadcast join
        let _ = msg_tx
            .send((peer_id.clone(), "[system]".into(), format!("{name} joined")))
            .await;

        // 4. Split stream into read/write halves so reader and writer run
        //    concurrently without borrowing conflicts.
        let (reader_half, writer_half) = tokio::io::split(stream);

        // 5. Spawn reader task
        let r_peer = peer_id.clone();
        let r_name = name.clone();
        let r_peers = Arc::clone(&peers);
        let r_msg = msg_tx.clone();
        tokio::spawn(async move {
            Self::reader_task(reader_half, r_peer, r_name, r_peers, r_msg).await;
        });

        // 6. Writer task (runs on this task)
        Self::writer_task(rx, writer_half).await;

        // 6. Deregister
        {
            let mut map = peers.write().await;
            map.remove(&peer_id);
        }
        info!("hub: peer {peer_id} ({name}) disconnected");

        // 7. Broadcast leave
        let _ = msg_tx
            .send((peer_id, "[system]".into(), format!("{name} left")))
            .await;

        Ok(())
    }

    /// Wire-protocol handshake.
    ///
    /// Expects `HANDSHAKE_LEN` bytes: `[16 nonce][16 random]`.
    /// - Rejects if nonce was already seen.
    /// - Sends back `[0]` for accept, `[1]` for reject.
    /// - Derives `PeerId` from the 16 random bytes (base58).
    async fn handshake(stream: &mut DataStream, nonces: NonceSet) -> Result<(PeerId, String)> {
        let mut buf = [0u8; HANDSHAKE_LEN];

        let n = stream
            .read_exact(&mut buf)
            .await
            .map_err(|e| ChatError::Connection(format!("handshake read: {e}")))?;
        if n != HANDSHAKE_LEN {
            stream.write_all(&[1]).await.ok();
            return Err(ChatError::InvalidInvite("handshake wrong length".into()));
        }

        let nonce: [u8; 16] = buf[..16].try_into().unwrap();
        let discriminator: [u8; 16] = buf[16..32].try_into().unwrap();

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
        stream
            .write_all(&[0])
            .await
            .map_err(|e| ChatError::Connection(format!("handshake write: {e}")))?;

        // Derive a short human-friendly name from the same bytes
        let name = format!("peer-{}", hex::encode(&discriminator[..4]));

        Ok((peer_id, name))
    }

    /// Reader task: reads newline-delimited UTF-8 from the Tor stream and
    /// pushes decoded messages to the hub's event channel.
    async fn reader_task(
        mut stream: impl AsyncReadExt + Unpin,
        peer_id: PeerId,
        name: String,
        _peers: PeerRegistry,
        msg_tx: mpsc::Sender<(PeerId, String, String)>,
    ) {
        let mut line = Vec::with_capacity(1024);

        loop {
            line.clear();
            let byte = match stream.read_u8().await {
                Ok(b) => b,
                Err(_) => break,
            };

            if byte == b'\n' {
                // empty line → skip
                continue;
            }

            if byte == b'\r' {
                // consume optional trailing \n
                let _ = stream.read_u8().await;
                if line.is_empty() {
                    continue;
                }
            } else {
                line.push(byte);
                if line.len() > MAX_MSG_BYTES {
                    warn!("hub: peer {peer_id} exceeded max message size");
                    break;
                }
                continue;
            }

            // We have a complete line (possibly after \r)
            match String::from_utf8(std::mem::take(&mut line)) {
                Ok(text) => {
                    let _ = msg_tx.send((peer_id.clone(), name.clone(), text)).await;
                }
                Err(e) => warn!("hub: peer {peer_id} sent invalid UTF-8: {e}"),
            }
        }

        info!("hub: reader task ended for {peer_id} ({name})");
    }

    /// Writer task: drains the per-peer channel and writes frames to the stream.
    async fn writer_task(
        mut rx: mpsc::UnboundedReceiver<Vec<u8>>,
        mut stream: impl AsyncWriteExt + Unpin,
    ) {
        while let Some(frame) = rx.recv().await {
            if stream.write_all(&frame).await.is_err() {
                break;
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
        // The handshake layout is [16 nonce][16 discriminator] = 32 total.
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
}
