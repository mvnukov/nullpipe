//! Unified public API: `RoomHandle` for sending/controlling a room and
//! `EventStream` for receiving events.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use arti_client::DataStream;
use base58::ToBase58;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::{broadcast, mpsc, watch, Mutex, RwLock};
use tokio::time::{timeout, Duration};
use tracing::{info, warn};

use crate::bootstrap::{self, TorBootstrap};
use crate::error::{ChatError, Result};
use crate::hub::HostedRoom;
use crate::invite::{encode as encode_invite, InvitePayload};
use crate::joiner::Joiner;
use crate::types::{ChatEvent, HostConfig, JoinConfig, PeerId, PeerInfo};
use crate::wire::{encode_message, read_frame, MessageType, WireMessage};


// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

const HANDSHAKE_LEN: usize = 32;
const READ_TIMEOUT: Duration = Duration::from_secs(60);
const WRITE_TIMEOUT: Duration = Duration::from_secs(30);
const EVENT_CHAN_CAP: usize = 256;
const SEND_CHAN_CAP: usize = 64;
const MSG_CHAN_CAP: usize = 128;
const BROADCAST_CHAN_CAP: usize = 256;

// ---------------------------------------------------------------------------
// Public types
// ---------------------------------------------------------------------------

/// Unified event stream — receives all room events.
///
/// The stream ends (`None`) when the room shuts down.
pub type EventStream = mpsc::Receiver<ChatEvent>;

/// Handle for sending and controlling a room.
///
/// `Clone`, `Send`, `Sync` — safe to share across tasks.
#[derive(Clone)]
pub struct RoomHandle {
    inner: Arc<RoomInner>,
}

/// Shared room state held behind `Arc`.
struct RoomInner {
    send_tx: mpsc::Sender<String>,
    shutdown_tx: watch::Sender<()>,
    /// Hub-only: onion address + port for invite generation.
    invite: Arc<std::sync::Mutex<Option<InviteInfo>>>,
    peers: Arc<RwLock<HashMap<PeerId, PeerInfo>>>,
    quit_flag: AtomicBool,
    tor: Arc<std::sync::Mutex<Option<TorBootstrap>>>,
    /// Our display name (reserved for future use).
    #[allow(dead_code)]
    name: String,
}

struct InviteInfo {
    onion_address: String,
    #[allow(dead_code)]
    port: u16,
    #[allow(dead_code)]
    ttl_secs: u64,
}



// ---------------------------------------------------------------------------
// RoomHandle implementation
// ---------------------------------------------------------------------------

impl RoomHandle {
    /// Send a message to the room.
    ///
    /// Returns an error if the room has been shut down.
    pub async fn send(&self, text: &str) -> Result<()> {
        if self.inner.quit_flag.load(Ordering::SeqCst) {
            return Err(ChatError::ShuttingDown);
        }
        self.inner
            .send_tx
            .send(text.to_string())
            .await
            .map_err(|_| ChatError::ShuttingDown)
    }

    /// Generate a new invite code.
    ///
    /// Only available on hub rooms. Returns an error when called by a joiner.
    pub async fn invite(&self, suggested_name: Option<&str>) -> Result<String> {
        if self.inner.quit_flag.load(Ordering::SeqCst) {
            return Err(ChatError::ShuttingDown);
        }
        let guard = self
            .inner
            .invite
            .lock()
            .map_err(|e| ChatError::Connection(format!("invite lock poisoned: {e}")))?;
        let info = guard.as_ref().ok_or_else(|| {
            ChatError::Connection("invite generation only available on hub".into())
        })?;
        let nonce: [u8; 16] = rand::random();
        let payload = InvitePayload {
            onion_address: info.onion_address.clone(),
            nonce,
            timestamp: chrono::Utc::now().timestamp() as u64,
            suggested_name: suggested_name.map(|s| s.to_string()),
        };
        encode_invite(&payload)
    }

    /// Snapshot of connected peers (eventually consistent).
    pub async fn peers(&self) -> Vec<PeerInfo> {
        self.inner.peers.read().await.values().cloned().collect()
    }

    /// Graceful shutdown of the room. Idempotent.
    ///
    /// Stops all background tasks, closes connections, tears down Tor.
    pub async fn quit(&self) {
        do_quit(&self.inner);
    }
}

impl Drop for RoomHandle {
    fn drop(&mut self) {
        // Only shut down when the last handle is dropped.
        // Without this check, a temporary clone (e.g. from a spawned send
        // task) would prematurely kill the room.
        if Arc::strong_count(&self.inner) == 1 {
            do_quit(&self.inner);
        }
    }
}

fn do_quit(inner: &RoomInner) {
    if inner.quit_flag.swap(true, Ordering::SeqCst) {
        return; // already quit
    }
    let _ = inner.shutdown_tx.send(());
    if let Ok(mut guard) = inner.tor.lock() {
        if let Some(tb) = guard.take() {
            drop(tb);
        }
    }
}

// ---------------------------------------------------------------------------
// host() entry point
// ---------------------------------------------------------------------------

/// Host a new room.
///
/// Returns immediately. Bootstrap and room setup happen in background tasks.
/// Events arrive via the returned [`EventStream`].
pub fn host(config: HostConfig) -> (RoomHandle, EventStream) {
    let (event_tx, event_rx) = mpsc::channel::<ChatEvent>(EVENT_CHAN_CAP);
    let (shutdown_tx, shutdown_rx) = watch::channel(());
    let (send_tx, send_rx) = mpsc::channel::<String>(SEND_CHAN_CAP);

    let inner = Arc::new(RoomInner {
        send_tx: send_tx.clone(),
        shutdown_tx: shutdown_tx.clone(),
        invite: Arc::new(std::sync::Mutex::new(None)),
        peers: Arc::new(RwLock::new(HashMap::new())),
        quit_flag: AtomicBool::new(false),
        tor: Arc::new(std::sync::Mutex::new(None)),
        name: config.name.clone(),
    });

    tokio::spawn(host_task(
        event_tx,
        shutdown_rx,
        send_rx,
        config,
        Arc::clone(&inner.invite),
        Arc::clone(&inner.peers),
        Arc::clone(&inner.tor),
    ));

    (RoomHandle { inner }, event_rx)
}

async fn host_task(
    mut event_tx: mpsc::Sender<ChatEvent>,
    mut shutdown_rx: watch::Receiver<()>,
    send_rx: mpsc::Receiver<String>,
    config: HostConfig,
    invite_info: Arc<std::sync::Mutex<Option<InviteInfo>>>,
    peers: Arc<RwLock<HashMap<PeerId, PeerInfo>>>,
    tor: Arc<std::sync::Mutex<Option<TorBootstrap>>>,
) {
    info!("host: starting");

    let bootstrap = match bootstrap::bootstrap_with_shutdown(&mut event_tx, &mut shutdown_rx).await {
        Some(b) => b,
        None => {
            info!("host: bootstrap cancelled or failed");
            return;
        }
    };

    if let Ok(mut guard) = tor.lock() {
        *guard = Some(bootstrap);
    }

    let client = {
        let client_opt = tor
            .lock()
            .expect("tor lock poisoned")
            .as_ref()
            .and_then(|b| b.client().ok())
            .cloned();
        match client_opt {
            Some(c) => c,
            None => {
                let _ = event_tx
                    .send(ChatEvent::Error(ChatError::Connection(
                        "Tor client not available".into(),
                    )))
                    .await;
                return;
            }
        }
    };

    run_host_loop(Arc::new(client), event_tx, shutdown_rx, send_rx, config, invite_info, peers).await;

    if let Ok(mut guard) = tor.lock() {
        if let Some(tb) = guard.take() {
            drop(tb);
        }
    }

    info!("host: cleanup complete");
}

#[allow(clippy::too_many_arguments)]
async fn run_host_loop(
    client: Arc<arti_client::TorClient<tor_rtcompat::PreferredRuntime>>,
    event_tx: mpsc::Sender<ChatEvent>,
    mut shutdown_rx: watch::Receiver<()>,
    mut send_rx: mpsc::Receiver<String>,
    config: HostConfig,
    invite_info: Arc<std::sync::Mutex<Option<InviteInfo>>>,
    peers: Arc<RwLock<HashMap<PeerId, PeerInfo>>>,
) {
    let port = 80u16;
    let room = match HostedRoom::new(&client, port).await {
        Ok(r) => r,
        Err(e) => {
            let _ = event_tx.send(ChatEvent::Error(e)).await;
            loop {
                tokio::select! {
                    biased;
                    _ = shutdown_rx.changed() => {
                        info!("host: shutdown signal (after room error)");
                        let _ = event_tx.try_send(ChatEvent::RoomClosed);
                        break;
                    }
                    text = send_rx.recv() => {
                        let Some(_) = text else {
                            info!("host: send channel closed");
                            break;
                        };
                    }
                }
            }
            info!("host: cleanup complete");
            return;
        }
    };

    let onion_address = room.address().to_string();
    let _ = event_tx
        .send(ChatEvent::RoomReady {
            onion_address: onion_address.clone(),
            port,
        })
        .await;

    {
        let mut guard = invite_info.lock().expect("invite lock poisoned");
        *guard = Some(InviteInfo {
            onion_address,
            port,
            ttl_secs: config.invite_ttl_secs,
        });
    }

    let (wire_broadcast_tx, _) = broadcast::channel::<WireMessage>(BROADCAST_CHAN_CAP);
    let (msg_tx, mut msg_rx) = mpsc::channel::<(PeerId, WireMessage)>(MSG_CHAN_CAP);
    let used_nonces: Arc<Mutex<std::collections::HashSet<[u8; 16]>>> =
        Arc::new(Mutex::new(std::collections::HashSet::new()));

    let accept_shutdown = shutdown_rx.clone();
    let accept_peers = Arc::clone(&peers);
    let accept_event_tx = event_tx.clone();
    let accept_broadcast = wire_broadcast_tx.clone();
    let accept_msg_tx = msg_tx.clone();
    let accept_nonces = Arc::clone(&used_nonces);
    let sender_name = config.name.clone();

    let accept_handle = tokio::spawn(async move {
        accept_loop(
            room,
            accept_shutdown,
            accept_peers,
            accept_event_tx,
            accept_broadcast,
            accept_msg_tx,
            accept_nonces,
            sender_name,
        )
        .await;
    });

    let event_tx_main = event_tx;
    loop {
        tokio::select! {
            biased;

            _ = shutdown_rx.changed() => {
                info!("host: shutdown signal received");
                let _ = event_tx_main.try_send(ChatEvent::RoomClosed);
                break;
            }

            text = send_rx.recv() => {
                let Some(text) = text else {
                    info!("host: send channel closed");
                    break;
                };
                let msg = WireMessage::chat(&config.name, &text);
                let event = ChatEvent::Message {
                    from: PeerId("[hub]".into()),
                    name: config.name.clone(),
                    text: text.clone(),
                };
                let _ = event_tx_main.try_send(event);
                let _ = wire_broadcast_tx.send(msg);
            }

            msg = msg_rx.recv() => {
                let Some((peer_id, wire_msg)) = msg else {
                    info!("host: msg channel closed");
                    break;
                };
                if wire_msg.kind == MessageType::Chat {
                    let event = ChatEvent::Message {
                        from: peer_id,
                        name: wire_msg.name,
                        text: wire_msg.text,
                    };
                    let _ = event_tx_main.try_send(event);
                }
            }
        }
    }

    info!("host: main loop ended, waiting for accept loop");

    accept_handle.abort();
    let _ = accept_handle.await;

    info!("host: cleanup complete");
}



/// Accept loop: accepts peer streams and spawns per-connection handlers.
#[allow(clippy::too_many_arguments)]
async fn accept_loop(
    mut room: HostedRoom,
    mut shutdown_rx: watch::Receiver<()>,
    peers: Arc<RwLock<HashMap<PeerId, PeerInfo>>>,
    event_tx: mpsc::Sender<ChatEvent>,
    wire_broadcast: broadcast::Sender<WireMessage>,
    msg_tx: mpsc::Sender<(PeerId, WireMessage)>,
    used_nonces: Arc<Mutex<std::collections::HashSet<[u8; 16]>>>,
    _sender_name: String,
) {
    info!("host: accept loop started");

    loop {
        tokio::select! {
            biased;

            _ = shutdown_rx.changed() => {
                info!("host: accept loop shutdown signal");
                break;
            }

            stream = room.accept_peer() => {
                let Some(stream) = stream else {
                    info!("host: room no longer accepting");
                    break;
                };

                let wire_broadcast = wire_broadcast.clone();
                let msg_tx = msg_tx.clone();
                let nonces = Arc::clone(&used_nonces);
                let peers = Arc::clone(&peers);
                let event_tx = event_tx.clone();
                let sender_name = _sender_name.clone();

                tokio::spawn({
                    let conn_shutdown = shutdown_rx.clone();
                    async move {
                        if let Err(e) = handle_hub_connection(
                            stream, nonces, msg_tx, wire_broadcast, peers, event_tx, sender_name, conn_shutdown,
                        ).await {
                            warn!("host: connection handler error: {e}");
                        }
                    }
                });
            }
        }
    }

    room.shutdown();
    info!("host: accept loop ended");
}

/// Full lifecycle for one accepted peer stream on the hub side.
async fn handle_hub_connection(
    stream: DataStream,
    nonces: Arc<Mutex<std::collections::HashSet<[u8; 16]>>>,
    msg_tx: mpsc::Sender<(PeerId, WireMessage)>,
    wire_broadcast: broadcast::Sender<WireMessage>,
    peers: Arc<RwLock<HashMap<PeerId, PeerInfo>>>,
    event_tx: mpsc::Sender<ChatEvent>,
    _sender_name: String,
    mut shutdown_rx: watch::Receiver<()>,
) -> Result<()> {
    let mut stream = stream;

    // 1. Handshake
    let (peer_id, name) = hub_handshake(&mut stream, nonces).await?;
    let joined_at = std::time::Instant::now();
    info!("host: peer {peer_id} ({name}) admitted");

    // 2. Register peer
    let peer_info = PeerInfo {
        id: peer_id.clone(),
        name: name.clone(),
        joined_at,
    };
    {
        let mut map = peers.write().await;
        map.insert(peer_id.clone(), peer_info.clone());
    }

    // 3. Emit PeerJoin event
    let _ = event_tx.try_send(ChatEvent::PeerJoin(peer_info.clone()));

    // 4. Notify peer about join
    if let Ok(frame) = encode_message(&WireMessage::system(&format!("{name} joined"))) {
        let _ = stream.write_all(&frame).await;
    }

    // 5. Split stream
    let (reader_half, mut writer_half) = tokio::io::split(stream);

    // 6. Spawn reader
    let r_peer = peer_id.clone();
    let r_name = name.clone();
    let r_peers = Arc::clone(&peers);
    let r_msg = msg_tx.clone();
    let r_broadcast = wire_broadcast.clone();
    let r_writer = msg_tx.clone(); // for pong responses via msg_tx

    let mut reader_handle = tokio::spawn(async move {
        hub_reader_task(
            reader_half,
            r_peer,
            r_name,
            r_peers,
            r_msg,
            r_broadcast,
            r_writer,
        )
        .await;
    });

    // 7. Writer: forward broadcast wire messages to this peer
    let mut broadcast_rx = wire_broadcast.subscribe();
    let writer_done = tokio::spawn(async move {
        loop {
            match broadcast_rx.recv().await {
                Ok(wire_msg) => {
                    if let Ok(frame) = encode_message(&wire_msg) {
                        if timeout(WRITE_TIMEOUT, writer_half.write_all(&frame))
                            .await
                            .is_err()
                        {
                            break;
                        }
                        if timeout(WRITE_TIMEOUT, writer_half.flush())
                            .await
                            .is_err()
                        {
                            break;
                        }
                    }
                }
                Err(broadcast::error::RecvError::Lagged(n)) => {
                    warn!("hub: peer writer lagged {n} messages");
                }
                Err(broadcast::error::RecvError::Closed) => {
                    break;
                }
            }
        }
    });

    // 8. Wait for reader to finish or host shutdown
    tokio::select! {
        _ = &mut reader_handle => {
            // reader finished normally
        }
        _ = shutdown_rx.changed() => {
            info!("host: shutting down, aborting peer connection");
            reader_handle.abort();
        }
    }
    writer_done.abort();

    // 9. Deregister peer
    {
        let mut map = peers.write().await;
        map.remove(&peer_id);
    }
    info!("host: peer {peer_id} ({name}) disconnected");

    // 10. Emit PeerLeave
    let _ = event_tx.try_send(ChatEvent::PeerLeave(peer_id));

    Ok(())
}

/// Hub-side wire protocol handshake.
async fn hub_handshake(
    stream: &mut (impl AsyncReadExt + AsyncWriteExt + Unpin),
    nonces: Arc<Mutex<std::collections::HashSet<[u8; 16]>>>,
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

    {
        let mut set = nonces.lock().await;
        if set.contains(&nonce) {
            stream.write_all(&[1]).await.ok();
            return Err(ChatError::NonceReused);
        }
        set.insert(nonce);
    }

    let peer_id = PeerId(discriminator.to_base58());
    timeout(WRITE_TIMEOUT, stream.write_all(&[0]))
        .await
        .map_err(|_| ChatError::Connection("handshake write timed out".into()))?
        .map_err(|e| ChatError::Connection(format!("handshake write: {e}")))?;
    timeout(WRITE_TIMEOUT, stream.flush())
        .await
        .map_err(|_| ChatError::Connection("handshake flush timed out".into()))?
        .map_err(|e| ChatError::Connection(format!("handshake flush: {e}")))?;

    // Read the name sent by the joiner as the first wire message
    let frame = timeout(Duration::from_secs(10), read_frame(stream))
        .await
        .map_err(|_| ChatError::Connection("handshake name read timed out".into()))?
        .map_err(|e| ChatError::Connection(format!("handshake name read: {e}")))?;
    
    let wire_msg = crate::wire::decode_message(&frame)
        .map_err(|e| ChatError::Connection(format!("handshake name decode: {e}")))?;
    
    Ok((peer_id, wire_msg.text))
}

/// Reader task for a hub-connected peer.
async fn hub_reader_task(
    mut stream: impl AsyncReadExt + Unpin,
    peer_id: PeerId,
    name: String,
    _peers: Arc<RwLock<HashMap<PeerId, PeerInfo>>>,
    msg_tx: mpsc::Sender<(PeerId, WireMessage)>,
    _wire_broadcast: broadcast::Sender<WireMessage>,
    _writer_tx: mpsc::Sender<(PeerId, WireMessage)>,
) {
    loop {
        match timeout(READ_TIMEOUT, read_frame(&mut stream)).await {
            Ok(Ok(frame)) => match crate::wire::decode_message(&frame) {
                Ok(wire_msg) => {
                    if wire_msg.kind == MessageType::Ping {
                        continue; // Hub writes via broadcast; skip pong for simplicity
                    }
                    let _ = msg_tx.send((peer_id.clone(), wire_msg)).await;
                }
                Err(e) => {
                    warn!("hub: peer {peer_id} sent malformed: {e}");
                    break;
                }
            },
            Ok(Err(e)) => {
                warn!("hub: read error for peer {peer_id}: {e}");
                break;
            }
            Err(_) => {
                warn!("hub: read timeout for peer {peer_id}");
                break;
            }
        }
    }
    info!("hub: reader ended for {peer_id} ({name})");
}

// ---------------------------------------------------------------------------
// join() entry point
// ---------------------------------------------------------------------------

/// Join an existing room.
///
/// Returns immediately. Bootstrap and connection happen in background tasks.
/// Events arrive via the returned [`EventStream`].
pub fn join(config: JoinConfig) -> (RoomHandle, EventStream) {
    let (event_tx, event_rx) = mpsc::channel::<ChatEvent>(EVENT_CHAN_CAP);
    let (shutdown_tx, shutdown_rx) = watch::channel(());
    let (send_tx, send_rx) = mpsc::channel::<String>(SEND_CHAN_CAP);

    let inner = Arc::new(RoomInner {
        send_tx: send_tx.clone(),
        shutdown_tx: shutdown_tx.clone(),
        invite: Arc::new(std::sync::Mutex::new(None)), // joiners can't generate invites
        peers: Arc::new(RwLock::new(HashMap::new())),
        quit_flag: AtomicBool::new(false),
        tor: Arc::new(std::sync::Mutex::new(None)),
        name: config.name.clone(),
    });

    tokio::spawn(joiner_task(
        event_tx,
        shutdown_rx,
        send_rx,
        config,
        Arc::clone(&inner.peers),
        Arc::clone(&inner.tor),
    ));

    (RoomHandle { inner }, event_rx)
}

async fn joiner_task(
    mut event_tx: mpsc::Sender<ChatEvent>,
    mut shutdown_rx: watch::Receiver<()>,
    send_rx: mpsc::Receiver<String>,
    config: JoinConfig,
    peers: Arc<RwLock<HashMap<PeerId, PeerInfo>>>,
    tor: Arc<std::sync::Mutex<Option<TorBootstrap>>>,
) {
    info!("joiner: starting");

    let bootstrap = match bootstrap::bootstrap_with_shutdown(&mut event_tx, &mut shutdown_rx).await {
        Some(b) => b,
        None => {
            info!("joiner: bootstrap cancelled or failed");
            return;
        }
    };

    if let Ok(mut guard) = tor.lock() {
        *guard = Some(bootstrap);
    }

    let client = {
        let client_opt = tor
            .lock()
            .expect("tor lock poisoned")
            .as_ref()
            .and_then(|b| b.client().ok())
            .cloned();
        match client_opt {
            Some(c) => c,
            None => {
                let _ = event_tx
                    .send(ChatEvent::Error(ChatError::Connection(
                        "Tor client not available".into(),
                    )))
                    .await;
                return;
            }
        }
    };

    // Use the new Joiner API
    let connector = crate::connector::ArtiConnector::new(client);
    
    let mut joiner = match Joiner::connect(&connector, &config.invite_code, &config.name).await {
        Ok(j) => j,
        Err(e) => {
            let _ = event_tx.send(ChatEvent::Error(e)).await;
            return;
        }
    };

    // Register peer
    let joined_at = std::time::Instant::now();
    let my_info = PeerInfo {
        id: joiner.peer_id.clone(),
        name: joiner.name.clone(),
        joined_at,
    };
    {
        let mut map = peers.write().await;
        map.insert(joiner.peer_id.clone(), my_info.clone());
    }
    let _ = event_tx.try_send(ChatEvent::PeerJoin(my_info));

    // Run the joiner main loop
    let result = joiner.run(send_rx, event_tx.clone(), shutdown_rx).await;
    
    if let Err(e) = result {
        let _ = event_tx.try_send(ChatEvent::Error(e));
    }
    
    // Deregister peer
    {
        let mut map = peers.write().await;
        map.remove(&joiner.peer_id);
    }
    
    let _ = event_tx.try_send(ChatEvent::RoomClosed);

    if let Ok(mut guard) = tor.lock() {
        if let Some(tb) = guard.take() {
            drop(tb);
        }
    }

    info!("joiner: cleanup complete");
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn room_handle_is_clone() {
        fn assert_clone<T: Clone>() {}
        assert_clone::<RoomHandle>();
    }

    #[test]
    fn room_handle_is_send_sync() {
        fn assert_send<T: Send>() {}
        fn assert_sync<T: Sync>() {}
        assert_send::<RoomHandle>();
        assert_sync::<RoomHandle>();
    }

    #[test]
    fn event_stream_is_receiver() {
        // EventStream is a type alias for mpsc::Receiver<ChatEvent>
        let _: EventStream = {
            let (_, rx) = mpsc::channel::<ChatEvent>(1);
            rx
        };
    }

    #[tokio::test]
    async fn quit_is_idempotent() {
        let (send_tx, _send_rx) = mpsc::channel::<String>(1);
        let (shutdown_tx, _shutdown_rx) = watch::channel(());
        let inner = Arc::new(RoomInner {
            send_tx,
            shutdown_tx,
            invite: Arc::new(std::sync::Mutex::new(None)),
            peers: Arc::new(RwLock::new(HashMap::new())),
            quit_flag: AtomicBool::new(false),
            tor: Arc::new(std::sync::Mutex::new(None)),
            name: "test".into(),
        });
        let handle = RoomHandle { inner };

        // Call quit() multiple times — should not panic
        handle.quit().await;
        handle.quit().await;
        handle.quit().await;

        assert!(handle.inner.quit_flag.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn send_after_quit_fails() {
        let (send_tx, _send_rx) = mpsc::channel::<String>(1);
        let (shutdown_tx, _shutdown_rx) = watch::channel(());
        let inner = Arc::new(RoomInner {
            send_tx,
            shutdown_tx,
            invite: Arc::new(std::sync::Mutex::new(None)),
            peers: Arc::new(RwLock::new(HashMap::new())),
            quit_flag: AtomicBool::new(false),
            tor: Arc::new(std::sync::Mutex::new(None)),
            name: "test".into(),
        });
        let handle = RoomHandle { inner };

        // Quit first
        handle.quit().await;

        // Send should fail
        let result = handle.send("hello").await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn invite_on_joiner_fails() {
        let (send_tx, _send_rx) = mpsc::channel::<String>(1);
        let (shutdown_tx, _shutdown_rx) = watch::channel(());
        // Joiner has no invite info
        let inner = Arc::new(RoomInner {
            send_tx,
            shutdown_tx,
            invite: Arc::new(std::sync::Mutex::new(None)),
            peers: Arc::new(RwLock::new(HashMap::new())),
            quit_flag: AtomicBool::new(false),
            tor: Arc::new(std::sync::Mutex::new(None)),
            name: "joiner".into(),
        });
        let handle = RoomHandle { inner };

        let result = handle.invite(None).await;
        assert!(result.is_err());
    }

    #[test]
    fn event_stream_ends_on_sender_drop() {
        use tokio::runtime::Runtime;
        let rt = Runtime::new().unwrap();
        rt.block_on(async {
            let (tx, mut rx) = mpsc::channel::<ChatEvent>(4);

            tx.send(ChatEvent::BootstrapProgress(50)).await.unwrap();
            drop(tx); // all senders dropped

            // Should get the one message then None
            assert!(matches!(
                rx.recv().await.unwrap(),
                ChatEvent::BootstrapProgress(50)
            ));
            assert!(rx.recv().await.is_none());
        });
    }

    #[test]
    fn host_config_and_join_config_exist() {
        let _hc = HostConfig {
            name: "test".into(),
            invite_ttl_secs: 300,
        };
        let _jc = JoinConfig {
            name: "test".into(),
            invite_code: "abc".into(),
        };
    }

    #[test]
    fn chat_event_variants_are_pub() {
        let _ = ChatEvent::Message {
            from: PeerId("a".into()),
            name: "alice".into(),
            text: "hi".into(),
        };
        let _ = ChatEvent::PeerJoin(PeerInfo {
            id: PeerId("b".into()),
            name: "bob".into(),
            joined_at: std::time::Instant::now(),
        });
        let _ = ChatEvent::PeerLeave(PeerId("c".into()));
        let _ = ChatEvent::BootstrapProgress(42);
        let _ = ChatEvent::RoomReady {
            onion_address: "x.onion".into(),
            port: 80,
        };
        let _ = ChatEvent::Error(ChatError::ShuttingDown);
    }
}
