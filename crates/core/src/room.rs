//! Unified public API: `RoomHandle` for sending/controlling a room and
//! `EventStream` for receiving events.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use arti_client::DataStream;
use base58::ToBase58;
use futures::StreamExt;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::{broadcast, mpsc, watch, Mutex, RwLock};
use tokio::time::{timeout, Duration};
use tracing::{info, warn};

use crate::bootstrap::TorBootstrap;
use crate::error::{ChatError, Result};
use crate::hub::HostedRoom;
use crate::invite::{decode as decode_invite, encode as encode_invite, InvitePayload};
use crate::types::{ChatEvent, HostConfig, JoinConfig, PeerId, PeerInfo};
use crate::wire::{encode_message, read_frame, read_message, MessageType, WireMessage};

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

/// Retry `client.connect` on transient Tor failures until `timeout_dur` elapses.
async fn connect_with_retry_loop(
    client: &arti_client::TorClient<tor_rtcompat::PreferredRuntime>,
    target: (&str, u16),
    timeout_dur: Duration,
) -> std::result::Result<DataStream, String> {

    let deadline = tokio::time::Instant::now() + timeout_dur;
    let mut last_err = None;
    const MAX_RETRIES: u32 = 3;
    const BASE_DELAY: Duration = Duration::from_secs(2);

    for attempt in 0..MAX_RETRIES {
        if tokio::time::Instant::now() >= deadline {
            break;
        }

        let remaining = deadline - tokio::time::Instant::now();
        match timeout(remaining, client.connect(target)).await {
            Ok(Ok(stream)) => return Ok(stream),
            Ok(Err(e)) => {
                let is_transient = format!("{e}").contains("transient");
                last_err = Some(e.to_string());
                if !is_transient || attempt == MAX_RETRIES - 1 {
                    break;
                }
                let delay = BASE_DELAY.saturating_mul(2u32.pow(attempt));
                let delay = delay.min(remaining / 2);
                info!("connect attempt {}/{} failed (transient), retrying in {:?}", attempt + 1, MAX_RETRIES, delay);
                tokio::time::sleep(delay).await;
            }
            Err(_) => return Err("connection timed out".into()),
        }
    }

    Err(last_err.unwrap_or_else(|| "connection timed out".into()))
}

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

/// A pre-bootstrapped Tor client that can be shared between `host_with_client`
/// and `join_with_client` to avoid redundant Tor bootstraps in tests.
pub struct SharedTorClient {
    inner: Arc<arti_client::TorClient<tor_rtcompat::PreferredRuntime>>,
}

impl SharedTorClient {
    /// Bootstrap a new shared Tor client.
    pub async fn bootstrap() -> Result<Self> {
        use arti_client::TorClientConfig;
        info!("bootstrapping shared Tor client");
        let client = arti_client::TorClient::builder()
            .config(TorClientConfig::default())
            .create_unbootstrapped_async()
            .await
            .map_err(|e| ChatError::Connection(format!("create client: {e}")))?;
        client
            .bootstrap()
            .await
            .map_err(|e| ChatError::Connection(format!("bootstrap: {e}")))?;
        info!("shared Tor bootstrap complete");
        Ok(Self { inner: Arc::new(client) })
    }

    /// Access the inner Tor client.
    pub fn client(&self) -> &arti_client::TorClient<tor_rtcompat::PreferredRuntime> {
        &self.inner
    }
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
    pub async fn invite(&self) -> Result<String> {
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

    let bootstrap = match bootstrap_tor(&mut event_tx, &mut shutdown_rx).await {
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

/// Host a room using a pre-bootstrapped shared Tor client.
/// Avoids redundant Tor bootstraps when running host + joiner in the same test.
pub fn host_with_client(
    config: HostConfig,
    shared: &SharedTorClient,
) -> (RoomHandle, EventStream) {
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

    tokio::spawn(run_host_loop(
        Arc::clone(&shared.inner),
        event_tx,
        shutdown_rx,
        send_rx,
        config,
        Arc::clone(&inner.invite),
        Arc::clone(&inner.peers),
    ));

    (RoomHandle { inner }, event_rx)
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

    let name = format!("peer-{}", hex::encode(&discriminator[..4]));
    Ok((peer_id, name))
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

    let bootstrap = match bootstrap_tor(&mut event_tx, &mut shutdown_rx).await {
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

    run_joiner_loop(Arc::new(client), event_tx, shutdown_rx, send_rx, config, peers).await;

    if let Ok(mut guard) = tor.lock() {
        if let Some(tb) = guard.take() {
            drop(tb);
        }
    }

    info!("joiner: cleanup complete");
}

async fn run_joiner_loop(
    client: Arc<arti_client::TorClient<tor_rtcompat::PreferredRuntime>>,
    event_tx: mpsc::Sender<ChatEvent>,
    mut shutdown_rx: watch::Receiver<()>,
    mut send_rx: mpsc::Receiver<String>,
    config: JoinConfig,
    peers: Arc<RwLock<HashMap<PeerId, PeerInfo>>>,
) {
    let payload = match decode_invite(&config.invite_code, None) {
        Ok(p) => p,
        Err(e) => {
            let _ = event_tx.send(ChatEvent::Error(e)).await;
            return;
        }
    };

    info!("joiner: connecting to {}", payload.onion_address);

    let target = (payload.onion_address.as_str(), 80);
    let stream = match connect_with_retry_loop(&client, target, Duration::from_secs(90)).await {
        Ok(s) => s,
        Err(e) => {
            let _ = event_tx
                .send(ChatEvent::Error(ChatError::Connection(format!(
                    "connect failed: {e}"
                ))))
                .await;
            return;
        }
    };

    info!("joiner: connected to hub");

    let (stream, peer_id, name) = match joiner_handshake(stream).await {
        Ok(result) => result,
        Err(e) => {
            let _ = event_tx.send(ChatEvent::Error(e)).await;
            return;
        }
    };

    info!("joiner: handshake complete, peer_id={peer_id} name={name}");

    let (reader_half, mut writer_half) = tokio::io::split(stream);

    let reader_event_tx = event_tx.clone();
    let reader_peers = Arc::clone(&peers);
    let name_for_reader = name.clone();

    let (reader_done_tx, mut reader_done_rx) = tokio::sync::oneshot::channel::<()>();
    let reader_handle = tokio::spawn(async move {
        joiner_reader_task(reader_half, reader_event_tx, reader_peers, name_for_reader).await;
        let _ = reader_done_tx.send(());
    });

    let joined_at = std::time::Instant::now();
    let my_info = PeerInfo {
        id: peer_id.clone(),
        name: name.clone(),
        joined_at,
    };
    {
        let mut map = peers.write().await;
        map.insert(peer_id.clone(), my_info.clone());
    }
    let _ = event_tx.try_send(ChatEvent::PeerJoin(my_info));

    loop {
        tokio::select! {
            biased;

            _ = shutdown_rx.changed() => {
                info!("joiner: shutdown signal");
                let _ = event_tx.try_send(ChatEvent::RoomClosed);
                break;
            }

            _ = &mut reader_done_rx => {
                info!("joiner: reader task done, connection closed");
                let _ = event_tx.try_send(ChatEvent::RoomClosed);
                break;
            }

            text = send_rx.recv() => {
                let Some(text) = text else {
                    info!("joiner: send channel closed");
                    break;
                };
                let msg = WireMessage::chat(&config.name, &text);
                if let Ok(frame) = encode_message(&msg) {
                    if timeout(WRITE_TIMEOUT, writer_half.write_all(&frame))
                        .await
                        .is_err()
                    {
                        warn!("joiner: write failed, connection likely dead");
                        break;
                    }
                    if timeout(WRITE_TIMEOUT, writer_half.flush())
                        .await
                        .is_err()
                    {
                        warn!("joiner: flush failed, connection likely dead");
                        break;
                    }
                }
            }
        }
    }

    info!("joiner: main loop ended");

    reader_handle.abort();
    let _ = reader_handle.await;

    info!("joiner: cleanup complete");
}

/// Join a room using a pre-bootstrapped shared Tor client.
/// Avoids redundant Tor bootstraps when running host + joiner in the same test.
pub fn join_with_client(
    config: JoinConfig,
    shared: &SharedTorClient,
) -> (RoomHandle, EventStream) {
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

    tokio::spawn(run_joiner_loop(
        Arc::clone(&shared.inner),
        event_tx,
        shutdown_rx,
        send_rx,
        config,
        Arc::clone(&inner.peers),
    ));

    (RoomHandle { inner }, event_rx)
}

/// Joiner-side wire protocol handshake.
async fn joiner_handshake(mut stream: DataStream) -> Result<(DataStream, PeerId, String)> {
    let nonce: [u8; 16] = rand::random();
    let discriminator: [u8; 16] = rand::random();

    let mut buf = [0u8; 32];
    buf[..16].copy_from_slice(&nonce);
    buf[16..32].copy_from_slice(&discriminator);

    timeout(WRITE_TIMEOUT, stream.write_all(&buf))
        .await
        .map_err(|_| ChatError::Connection("handshake write timed out".into()))?
        .map_err(|e| ChatError::Connection(format!("handshake write: {e}")))?;
    timeout(WRITE_TIMEOUT, stream.flush())
        .await
        .map_err(|_| ChatError::Connection("handshake flush timed out".into()))?
        .map_err(|e| ChatError::Connection(format!("handshake flush: {e}")))?;

    let mut response = [0u8; 1];
    timeout(READ_TIMEOUT, stream.read_exact(&mut response))
        .await
        .map_err(|_| ChatError::Connection("handshake read timed out".into()))?
        .map_err(|e| ChatError::Connection(format!("handshake read: {e}")))?;

    if response[0] != 0 {
        return Err(ChatError::Connection("handshake rejected by hub".into()));
    }

    let peer_id = PeerId(discriminator.to_base58());
    let name = format!("peer-{}", hex::encode(&discriminator[..4]));

    Ok((stream, peer_id, name))
}

/// Reader task for the joiner side.
async fn joiner_reader_task(
    mut reader: impl AsyncReadExt + Unpin + Send,
    event_tx: mpsc::Sender<ChatEvent>,
    _peers: Arc<RwLock<HashMap<PeerId, PeerInfo>>>,
    _my_name: String,
) {
    loop {
        match timeout(READ_TIMEOUT, read_message(&mut reader)).await {
            Ok(Ok(msg)) => {
                match msg.kind {
                    MessageType::Chat | MessageType::System => {
                        let event = ChatEvent::Message {
                            from: PeerId(msg.name.clone()),
                            name: msg.name,
                            text: msg.text,
                        };
                        if event_tx.send(event).await.is_err() {
                            break;
                        }
                    }
                    MessageType::Pong => {
                        // ignore
                    }
                    MessageType::Ping => {
                        // Can't respond without writer; ignore
                    }
                }
            }
            Ok(Err(e)) => {
                let _ = event_tx.send(ChatEvent::Error(e)).await;
                break;
            }
            Err(_) => {
                warn!("joiner: read timeout");
                let _ = event_tx
                    .send(ChatEvent::Error(ChatError::Connection(
                        "read timeout".into(),
                    )))
                    .await;
                break;
            }
        }
    }
    info!("joiner: reader ended");
}

// ---------------------------------------------------------------------------
// Shared helpers
// ---------------------------------------------------------------------------

/// Bootstrap Tor, forwarding progress events.
///
/// Returns `Err(())` if shutdown was requested during bootstrap or bootstrap failed.
async fn bootstrap_tor(
    event_tx: &mut mpsc::Sender<ChatEvent>,
    shutdown_rx: &mut watch::Receiver<()>,
) -> Option<TorBootstrap> {
    let mut bootstrap = TorBootstrap::new();
    let mut event_stream = match bootstrap.bootstrap().await {
        Ok(s) => s,
        Err(e) => {
            let _ = event_tx.send(ChatEvent::Error(e)).await;
            return None;
        }
    };

    let mut bootstrap_ok = false;

    loop {
        tokio::select! {
            biased;

            _ = shutdown_rx.changed() => {
                info!("bootstrap: shutdown during bootstrap");
                bootstrap.shutdown();
                return None;
            }

            event = event_stream.next() => {
                match event {
                    Some(e) => {
                        if matches!(&e, ChatEvent::BootstrapProgress(100)) {
                            bootstrap_ok = true;
                        }
                        if event_tx.send(e).await.is_err() {
                            bootstrap.shutdown();
                            return None;
                        }
                    }
                    None => break,
                }
            }
        }
    }

    if bootstrap_ok {
        Some(bootstrap)
    } else {
        bootstrap.shutdown();
        None
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

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

        let result = handle.invite().await;
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
