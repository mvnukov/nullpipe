//! Joiner — connects to a hosted onion service via Tor.
//!
//! Pre-condition: Tor is already bootstrapped.
//!
//! ```text
//! CALLER:
//!     joiner = Joiner::connect(connector, invite_code, name)   // phases 1+2
//!     joiner.run(msg_rx, event_tx, shutdown_rx)                // phase 3
//!     joiner.close()                                            // phase 4
//!
//! ── Joiner::connect(connector, invite_code, name) ──────────────────────────
//!
//!   1. DECODE     base58 decode invite_code → (onion_address, nonce, timestamp)
//!   2. VALIDATE   onion format, nonce 16 bytes, expiry (TTL=300s)
//!   3. CONNECT    connector.connect(onion_address, 80) → DataStream
//!   4. HANDSHAKE  Self::handshake(stream, name) → (PeerId, DataStream)
//!                     generate nonce[16] + discriminator[16]
//!                     write 32 bytes
//!                     read 1 byte (accept=0 / reject≠0)
//!                     send name as first wire message
//!   5. RETURN     Joiner { stream, peer_id, name }
//!
//! ── joiner.run(msg_rx, event_tx, shutdown_rx) ─────────────────────────────
//!
//!   1. SPLIT      stream.split() → (reader, writer)
//!   2. SPAWN      reader task: read_frame → decode_message → event_tx.send(event)
//!   3. LOOP       tokio::select! {
//!                     shutdown signalled     → break Ok(())
//!                     reader done            → break Ok(())
//!                     msg from msg_rx        → encode → write_all → flush
//!                     msg_rx closed          → break Ok(())
//!                 }
//!   4. ABORT      kill reader task, await it
//!
//! ── joiner.close() ─────────────────────────────────────────────────────────
//!
//!   1. CLOSE      stream = None  (idempotent, DataStream drops → closes Tor)
//! ```

use arti_client::DataStream;
use base58::ToBase58;
use tokio::sync::{mpsc, watch};
use tokio::time::{timeout, Duration};
use tracing::warn;

use crate::error::{ChatError, Result};
use crate::connector::TorConnector;
use crate::invite::decode as decode_invite;
use crate::types::{ChatEvent, PeerId};
use crate::wire;
use crate::wire::{encode_message, WireMessage};

/// How long to wait for data before considering the peer dead.
const READ_TIMEOUT: Duration = Duration::from_secs(60);

/// Max time for a single write operation.
const WRITE_TIMEOUT: Duration = Duration::from_secs(30);

/// Joiner state machine. Created by [`Joiner::connect`], runs via [`Joiner::run`].
pub struct Joiner {
    pub(crate) stream: Option<DataStream>,
    pub(crate) peer_id: PeerId,
    pub(crate) name: String,
}

impl Joiner {
    /// Connect to a hosted room using an invite code.
    ///
    /// Decodes the invite, validates it is not expired, connects through Tor,
    /// then performs the handshake. Returns a ready-to-run Joiner.
    pub async fn connect(
        tor: &impl TorConnector,
        invite_code: &str,
        name: &str,
    ) -> Result<Self> {
        // 1. DECODE + 2. VALIDATE (onion format, nonce, TTL=300s expiry)
        let payload = decode_invite(invite_code, Some(300))?;

        // 3. CONNECT to onion service on port 80
        let stream = tor.connect(&payload.onion_address, 80).await?;

        // 4. HANDSHAKE — exchange nonce, register name
        let (peer_id, stream) = Self::handshake(stream, name).await?;

        // 5. RETURN ready-to-run Joiner
        Ok(Self {
            stream: Some(stream),
            peer_id,
            name: name.to_string(),
        })
    }

    /// Internal: wire-level handshake on a fresh stream.
    ///
    /// Called by [`Joiner::connect`]. Generates nonce+discriminator, writes 32 bytes,
    /// reads accept/reject byte, sends display name as first wire message.
    async fn handshake<S>(mut stream: S, name: &str) -> Result<(PeerId, S)>
    where
        S: tokio::io::AsyncReadExt + tokio::io::AsyncWriteExt + Unpin,
    {
        let nonce: [u8; 16] = rand::random();
        let discriminator: [u8; 16] = rand::random();

        // Write 32-byte handshake: nonce || discriminator
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

        // Read accept/reject byte from hub
        let mut response = [0u8; 1];
        timeout(READ_TIMEOUT, stream.read_exact(&mut response))
            .await
            .map_err(|_| ChatError::Connection("handshake read timed out".into()))?
            .map_err(|e| ChatError::Connection(format!("handshake read: {e}")))?;

        if response[0] != 0 {
            return Err(ChatError::Connection("handshake rejected by hub".into()));
        }

        let peer_id = PeerId(discriminator.to_base58());

        // Send display name as first wire message
        let name_msg = WireMessage::system(name);
        let frame = encode_message(&name_msg)?;
        timeout(WRITE_TIMEOUT, stream.write_all(&frame))
            .await
            .map_err(|_| ChatError::Connection("handshake name write timed out".into()))?
            .map_err(|e| ChatError::Connection(format!("handshake name write: {e}")))?;
        timeout(WRITE_TIMEOUT, stream.flush())
            .await
            .map_err(|_| ChatError::Connection("handshake name flush timed out".into()))?
            .map_err(|e| ChatError::Connection(format!("handshake name flush: {e}")))?;

        Ok((peer_id, stream))
    }

    /// Run the joiner main loop until the connection drops or shutdown fires.
    ///
    /// Spawns a background reader task that reads wire frames from the hub
    /// and pushes `ChatEvent`s through `events`. The writer loop consumes
    /// messages from `messages` and writes them to the stream.
    ///
    /// Returns when:
    /// - The hub disconnects (read error or timeout)
    /// - `shutdown` is signalled
    /// - The event channel receiver is dropped
    pub async fn run(
        &mut self,
        messages: mpsc::Receiver<String>,
        events: mpsc::Sender<ChatEvent>,
        shutdown: watch::Receiver<()>,
    ) -> Result<()> {
        match self.stream.take() {
            Some(stream) => {
                self.run_connected(stream, messages, events, shutdown).await
            }
            None => self.run_disconnected(messages, events, shutdown).await,
        }
    }

    /// Full run loop with an active stream: reader task + writer + message loop.
    async fn run_connected(
        &self,
        stream: DataStream,
        messages: mpsc::Receiver<String>,
        events: mpsc::Sender<ChatEvent>,
        shutdown: watch::Receiver<()>,
    ) -> Result<()> {
        let (reader, writer) = tokio::io::split(stream);
        let (reader_done_tx, mut reader_done_rx) = mpsc::channel::<()>(1);

        let reader_handle = Self::spawn_reader(reader, events.clone(), reader_done_tx);

        let result = Self::write_loop(
            writer,
            messages,
            events,
            shutdown,
            &mut reader_done_rx,
            &self.name,
            &self.peer_id,
        )
        .await;

        Self::stop_reader(reader_handle).await;
        result
    }

    /// Minimal run loop when no stream is present — only dispatches messages
    /// to the event channel and listens for shutdown.
    async fn run_disconnected(
        &self,
        mut messages: mpsc::Receiver<String>,
        events: mpsc::Sender<ChatEvent>,
        mut shutdown: watch::Receiver<()>,
    ) -> Result<()> {
        let mut pending: Vec<ChatEvent> = Vec::new();

        loop {
            tokio::select! {
                biased;

                _ = shutdown.changed() => {
                    Self::drain_and_flush(&mut messages, &events, &self.peer_id, &self.name, &mut pending).await;
                    return Ok(());
                }

                msg = messages.recv() => match msg {
                    Some(text) => {
                        let event = Self::make_event(&self.peer_id, &self.name, &text);
                        Self::try_emit(&events, event, &mut pending);
                    }
                    None => {
                        Self::drain_and_flush(&mut messages, &events, &self.peer_id, &self.name, &mut pending).await;
                        return Ok(()); // channel closed
                    }
                },

                _ = events.closed() => {
                    return Err(ChatError::Connection("event receiver dropped".into()));
                }
            }
        }
    }

    /// Build a ChatEvent::Message for an outgoing chat message.
    fn make_event(peer_id: &PeerId, name: &str, text: &str) -> ChatEvent {
        ChatEvent::Message {
            from: peer_id.clone(),
            name: name.to_string(),
            text: text.to_string(),
        }
    }

    /// Drain any remaining buffered messages and flush pending events.
    /// Called on shutdown / channel-close to ensure no in-flight messages are lost.
    async fn drain_and_flush(
        messages: &mut mpsc::Receiver<String>,
        events: &mpsc::Sender<ChatEvent>,
        peer_id: &PeerId,
        name: &str,
        pending: &mut Vec<ChatEvent>,
    ) {
        while let Ok(text) = messages.try_recv() {
            let event = Self::make_event(peer_id, name, &text);
            Self::try_emit(events, event, pending);
        }
        Self::flush_pending(events, pending).await;
    }

    /// Try to send an event immediately.  On back-pressure, buffer locally.
    fn try_emit(events: &mpsc::Sender<ChatEvent>, event: ChatEvent, pending: &mut Vec<ChatEvent>) {
        if events.try_send(event.clone()).is_err() {
            pending.push(event);
        }
    }

    /// Drain all locally buffered events into the channel.
    /// Uses `try_send` — events that can't fit are silently dropped.
    async fn flush_pending(events: &mpsc::Sender<ChatEvent>, pending: &mut Vec<ChatEvent>) {
        while let Some(event) = pending.pop() {
            if events.try_send(event).is_err() {
                warn!("joiner: event channel full on flush, dropping {} pending events", pending.len() + 1);
                pending.clear();
                break;
            }
        }
    }

    /// Spawns a background task that reads wire frames from the hub,
    /// decodes them, and forwards as `ChatEvent::Message` or `ChatEvent::Error`.
    fn spawn_reader<R>(
        mut reader: R,
        events: mpsc::Sender<ChatEvent>,
        done_tx: mpsc::Sender<()>,
    ) -> tokio::task::JoinHandle<()>
    where
        R: tokio::io::AsyncReadExt + Unpin + Send + 'static,
    {
        tokio::spawn(async move {
            Self::reader_loop(&mut reader, &events).await;
            let _ = done_tx.send(()).await;
        })
    }

    /// Inner reader loop: reads frames until error, timeout, or channel close.
    async fn reader_loop<R>(reader: &mut R, events: &mpsc::Sender<ChatEvent>)
    where
        R: tokio::io::AsyncReadExt + Unpin,
    {
        loop {
            match timeout(READ_TIMEOUT, wire::read_frame(reader)).await {
                Ok(Ok(frame)) => {
                    Self::handle_incoming_frame(&frame, events).await;
                }
                Ok(Err(_)) => break, // wire error — hub disconnected or bad frame
                Err(_) => break,     // read timeout — peer considered dead
            }
        }
    }

    /// Decode a single incoming frame and emit the corresponding `ChatEvent`.
    async fn handle_incoming_frame(frame: &[u8], events: &mpsc::Sender<ChatEvent>) {
        match wire::decode_message(frame) {
            Ok(msg) => {
                let event = ChatEvent::Message {
                    from: PeerId(msg.name.clone()),
                    name: msg.name,
                    text: msg.text,
                };
                if events.send(event).await.is_err() {
                    // Event receiver dropped — nothing more to do.
                }
            }
            Err(e) => {
                warn!("joiner: malformed incoming frame: {e}");
            }
        }
    }

    /// Write loop: dispatches between outgoing messages, shutdown,
    /// and reader completion.
    async fn write_loop<W>(
        mut writer: W,
        mut messages: mpsc::Receiver<String>,
        events: mpsc::Sender<ChatEvent>,
        mut shutdown: watch::Receiver<()>,
        reader_done_rx: &mut mpsc::Receiver<()>,
        name: &str,
        peer_id: &PeerId,
    ) -> Result<()>
    where
        W: tokio::io::AsyncWriteExt + Unpin,
    {
        let mut pending: Vec<ChatEvent> = Vec::new();

        loop {
            tokio::select! {
                biased;

                // 1. Shutdown signal — highest priority
                _ = shutdown.changed() => {
                    Self::drain_and_flush(&mut messages, &events, peer_id, name, &mut pending).await;
                    return Ok(());
                }

                // 2. Reader task finished (hub disconnected)
                _ = reader_done_rx.recv() => {
                    Self::drain_and_flush(&mut messages, &events, peer_id, name, &mut pending).await;
                    return Ok(());
                }

                // 3. Outgoing message from the local UI
                msg = messages.recv() => {
                    match msg {
                        Some(text) => {
                            Self::process_outgoing_message(
                                &text, name, peer_id, &events, &mut pending, &mut writer,
                            ).await?;
                        }
                        None => {
                            Self::drain_and_flush(&mut messages, &events, peer_id, name, &mut pending).await;
                            return Ok(()); // channel closed
                        }
                    }
                }

                // 4. Event receiver dropped
                _ = events.closed() => {
                    return Err(ChatError::Connection("event receiver dropped".into()));
                }
            }
        }
    }

    /// Encode an outgoing chat message, write it to the stream, and
    /// emit a local `ChatEvent::Message` so the UI displays it.
    async fn process_outgoing_message<W>(
        text: &str,
        name: &str,
        peer_id: &PeerId,
        events: &mpsc::Sender<ChatEvent>,
        pending: &mut Vec<ChatEvent>,
        writer: &mut W,
    ) -> Result<()>
    where
        W: tokio::io::AsyncWriteExt + Unpin,
    {
        let msg = WireMessage::chat(name, text);
        let frame = encode_message(&msg)?;

        Self::write_frame(writer, &frame).await?;
        let event = Self::make_event(peer_id, name, text);
        Self::try_emit(events, event, pending);

        Ok(())
    }

    /// Write a single encoded frame to the stream with a timeout.
    async fn write_frame<W>(writer: &mut W, frame: &[u8]) -> Result<()>
    where
        W: tokio::io::AsyncWriteExt + Unpin,
    {
        eprintln!("[joiner-write] writing {} bytes", frame.len());
        timeout(WRITE_TIMEOUT, writer.write_all(frame))
            .await
            .map_err(|_| ChatError::Connection("write timed out".into()))?
            .map_err(|e| ChatError::Connection(format!("write failed: {e}")))?;
        eprintln!("[joiner-write] wrote {} bytes, flushing", frame.len());

        timeout(WRITE_TIMEOUT, writer.flush())
            .await
            .map_err(|_| ChatError::Connection("flush timed out".into()))?
            .map_err(|e| ChatError::Connection(format!("flush failed: {e}")))?;
        eprintln!("[joiner-write] flush done");

        Ok(())
    }

    /// Await the reader task handle, swallowing any panic.
    async fn stop_reader(handle: tokio::task::JoinHandle<()>) {
        let _ = handle.await;
    }

    /// Close the connection. Idempotent.
    pub fn close(&mut self) {
        self.stream = None;
    }

    /// Test constructor. Not for production use.
    #[doc(hidden)]
    pub fn new_for_test(peer_id: PeerId, name: String) -> Self {
        Self {
            stream: None,
            peer_id,
            name,
        }
    }
}

impl Drop for Joiner {
    fn drop(&mut self) {
        self.close();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::connector::mock::MockTorConnector;

    fn make_joiner() -> Joiner {
        Joiner {
            stream: None,
            peer_id: PeerId("test".into()),
            name: "test".into(),
        }
    }

    async fn drain(rx: &mut mpsc::Receiver<ChatEvent>) -> Vec<ChatEvent> {
        let mut events = Vec::new();
        while let Ok(Some(e)) = tokio::time::timeout(Duration::from_millis(50), rx.recv()).await {
            events.push(e);
        }
        events
    }

    // ── close / drop ─────────────────────────────────────────────────────────

    #[test]
    fn close_is_idempotent() {
        let mut j = make_joiner();
        j.close();
        j.close();
        assert!(j.stream.is_none());
    }

    #[test]
    fn drop_calls_close() {
        drop(make_joiner());
    }

    // ── handshake: accept / reject ───────────────────────────────────────────

    /// Simulates a hub that reads 32 handshake bytes, then writes the given
    /// accept byte and (on accept) reads the name wire message.
    async fn simulate_hub(stream: tokio::io::DuplexStream, accept_byte: u8) {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let (mut r, mut w) = tokio::io::split(stream);

        // Read 32-byte handshake
        let mut buf = [0u8; 32];
        let n = r.read_exact(&mut buf).await.unwrap();
        assert_eq!(n, 32);

        // Write accept/reject
        w.write_all(&[accept_byte]).await.unwrap();
        w.flush().await.unwrap();

        // On accept, read the name wire message (length prefix + payload)
        if accept_byte == 0 {
            let mut len_buf = [0u8; 4];
            r.read_exact(&mut len_buf).await.unwrap();
            let len = u32::from_be_bytes(len_buf) as usize;
            assert!(len < 65536, "name message too large: {len}");
            let mut payload = vec![0u8; len];
            r.read_exact(&mut payload).await.unwrap();
        }
    }

    #[tokio::test]
    async fn handshake_accept_returns_peer_id() {
        let (client, hub) = tokio::io::duplex(1024);

        let hub_task = tokio::spawn(simulate_hub(hub, 0));
        let (peer_id, _stream) = Joiner::handshake(client, "alice").await.unwrap();
        hub_task.await.unwrap();

        assert!(!peer_id.0.is_empty());
    }

    #[tokio::test]
    async fn handshake_reject_returns_error() {
        let (client, hub) = tokio::io::duplex(1024);

        let hub_task = tokio::spawn(simulate_hub(hub, 1));
        let result = Joiner::handshake(client, "alice").await;
        hub_task.await.unwrap();

        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("rejected"));
    }

    #[tokio::test]
    async fn connect_rejects_empty_invite() {
        let mock = MockTorConnector::new();
        let result = Joiner::connect(&mock, "", "alice").await;
        assert!(result.is_err());
        assert_eq!(mock.call_count(), 0);
    }

    #[tokio::test]
    async fn connect_rejects_garbage_invite() {
        let mock = MockTorConnector::new();
        let result = Joiner::connect(&mock, "!!!not-base58!!!", "alice").await;
        assert!(result.is_err());
        assert_eq!(mock.call_count(), 0);
    }

    #[tokio::test]
    async fn connect_rejects_truncated_invite() {
        let mock = MockTorConnector::new();
        let result = Joiner::connect(&mock, "abc", "alice").await;
        assert!(result.is_err());
        assert_eq!(mock.call_count(), 0);
    }

    #[tokio::test]
    async fn connect_rejects_expired_invite() {
        use crate::invite::{encode, InvitePayload};

        let payload = InvitePayload {
            onion_address: "vww6ybal6bd7szmgncyruucpgfkqahzddi37ktceo3ah7ngmcopnpyyd.onion".into(),
            nonce: [0x42u8; 16],
            timestamp: 1_700_000_000,
        };
        let code = encode(&payload).unwrap();

        let mock = MockTorConnector::new();
        let result = Joiner::connect(&mock, &code, "alice").await;
        assert!(result.is_err());
        assert_eq!(mock.call_count(), 0);
    }

    #[tokio::test]
    async fn connect_rejects_invite_with_bad_onion_address() {
        use base58::ToBase58;

        // Manually craft a token — can't use encode() because it validates.
        // Address ends with .onion but is far too short (real v3 is 56 chars).
        let raw = "short.onion:00000000000000000000000000000000:1700000000";
        let code = raw.as_bytes().to_base58();

        let mock = MockTorConnector::new();
        let result = Joiner::connect(&mock, &code, "alice").await;
        assert!(result.is_err());
        assert_eq!(mock.call_count(), 0);
    }

    #[tokio::test]
    async fn connect_calls_tor_with_correct_target() {
        use crate::invite::{encode, InvitePayload};

        let payload = InvitePayload {
            onion_address: "vww6ybal6bd7szmgncyruucpgfkqahzddi37ktceo3ah7ngmcopnpyyd.onion".into(),
            nonce: [0xABu8; 16],
            timestamp: chrono::Utc::now().timestamp() as u64,
        };
        let code = encode(&payload).unwrap();

        let mock = MockTorConnector::new();
        let _ = Joiner::connect(&mock, &code, "alice").await;
        assert_eq!(mock.call_count(), 1);
        let (addr, port) = mock.last_target().unwrap();
        assert_eq!(addr, "vww6ybal6bd7szmgncyruucpgfkqahzddi37ktceo3ah7ngmcopnpyyd.onion");
        assert_eq!(port, 80);
    }

    #[tokio::test]
    async fn connect_propagates_tor_error() {
        use crate::invite::{encode, InvitePayload};

        let payload = InvitePayload {
            onion_address: "vww6ybal6bd7szmgncyruucpgfkqahzddi37ktceo3ah7ngmcopnpyyd.onion".into(),
            nonce: [0xABu8; 16],
            timestamp: chrono::Utc::now().timestamp() as u64,
        };
        let code = encode(&payload).unwrap();

        let mock = MockTorConnector::with_connect_result(Err(
            crate::error::ChatError::Connection("network down".into())
        ));
        let result = Joiner::connect(&mock, &code, "alice").await;
        assert!(result.is_err());
    }

    // ── run: shutdown signal ─────────────────────────────────────────────────

    #[tokio::test]
    async fn run_exits_on_immediate_shutdown() {
        let (_msg_tx, msg_rx) = mpsc::channel::<String>(16);
        let (evt_tx, _evt_rx) = mpsc::channel::<ChatEvent>(16);
        let (sd_tx, sd_rx) = watch::channel(());
        sd_tx.send(()).unwrap();

        let mut j = make_joiner();
        let result = j.run(msg_rx, evt_tx, sd_rx).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn run_exits_on_shutdown_after_delay() {
        let (_msg_tx, msg_rx) = mpsc::channel::<String>(16);
        let (evt_tx, _evt_rx) = mpsc::channel::<ChatEvent>(16);
        let (sd_tx, sd_rx) = watch::channel(());

        let mut j = make_joiner();
        let handle = tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(100)).await;
            sd_tx.send(()).unwrap();
        });

        let result = j.run(msg_rx, evt_tx, sd_rx).await;
        handle.await.unwrap();
        assert!(result.is_ok());
    }

    // ── run: message channel closed ──────────────────────────────────────────

    #[tokio::test]
    async fn run_exits_when_message_channel_closes() {
        let (msg_tx, msg_rx) = mpsc::channel::<String>(16);
        let (evt_tx, _evt_rx) = mpsc::channel::<ChatEvent>(16);
        let (_sd_tx, sd_rx) = watch::channel(());

        drop(msg_tx);

        let mut j = make_joiner();
        let result = j.run(msg_rx, evt_tx, sd_rx).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn run_exits_when_event_receiver_dropped() {
        let (_msg_tx, msg_rx) = mpsc::channel::<String>(16);
        let (evt_tx, evt_rx) = mpsc::channel::<ChatEvent>(16);
        let (_sd_tx, sd_rx) = watch::channel(());

        drop(evt_rx);

        let mut j = make_joiner();
        let result = j.run(msg_rx, evt_tx, sd_rx).await;
        assert!(result.is_err());
    }

    // ── run: send + receive ──────────────────────────────────────────────────

    #[tokio::test]
    async fn run_forwards_messages_to_events() {
        let (msg_tx, msg_rx) = mpsc::channel::<String>(16);
        let (evt_tx, mut evt_rx) = mpsc::channel::<ChatEvent>(16);
        let (sd_tx, sd_rx) = watch::channel(());

        let mut j = make_joiner();
        let handle = tokio::spawn(async move {
            j.run(msg_rx, evt_tx, sd_rx).await
        });

        msg_tx.send("hello".to_string()).await.unwrap();
        tokio::time::sleep(Duration::from_millis(100)).await;
        sd_tx.send(()).unwrap();
        handle.await.unwrap().unwrap();

        let events = drain(&mut evt_rx).await;
        assert!(events.iter().any(|e| matches!(e, ChatEvent::Message { text, .. } if text == "hello")));
    }

    #[tokio::test]
    async fn run_forwards_multiple_messages_in_order() {
        let (msg_tx, msg_rx) = mpsc::channel::<String>(16);
        let (evt_tx, mut evt_rx) = mpsc::channel::<ChatEvent>(16);
        let (sd_tx, sd_rx) = watch::channel(());

        let mut j = make_joiner();
        let handle = tokio::spawn(async move {
            j.run(msg_rx, evt_tx, sd_rx).await
        });

        msg_tx.send("first".to_string()).await.unwrap();
        msg_tx.send("second".to_string()).await.unwrap();
        msg_tx.send("third".to_string()).await.unwrap();
        tokio::time::sleep(Duration::from_millis(200)).await;
        sd_tx.send(()).unwrap();
        handle.await.unwrap().unwrap();

        let texts: Vec<_> = drain(&mut evt_rx).await
            .into_iter()
            .filter_map(|e| match e {
                ChatEvent::Message { text, .. } => Some(text),
                _ => None,
            })
            .collect();
        assert_eq!(texts, vec!["first", "second", "third"]);
    }

    #[tokio::test]
    async fn run_handles_empty_message() {
        let (msg_tx, msg_rx) = mpsc::channel::<String>(16);
        let (evt_tx, mut evt_rx) = mpsc::channel::<ChatEvent>(16);
        let (sd_tx, sd_rx) = watch::channel(());

        let mut j = make_joiner();
        let handle = tokio::spawn(async move {
            j.run(msg_rx, evt_tx, sd_rx).await
        });

        msg_tx.send("".to_string()).await.unwrap();
        tokio::time::sleep(Duration::from_millis(100)).await;
        sd_tx.send(()).unwrap();
        handle.await.unwrap().unwrap();

        let events = drain(&mut evt_rx).await;
        assert!(events.iter().any(|e| matches!(e, ChatEvent::Message { text, .. } if text.is_empty())));
    }

    #[tokio::test]
    async fn run_handles_unicode_message() {
        let (msg_tx, msg_rx) = mpsc::channel::<String>(16);
        let (evt_tx, mut evt_rx) = mpsc::channel::<ChatEvent>(16);
        let (sd_tx, sd_rx) = watch::channel(());

        let mut j = make_joiner();
        let handle = tokio::spawn(async move {
            j.run(msg_rx, evt_tx, sd_rx).await
        });

        msg_tx.send("你好 🌍".to_string()).await.unwrap();
        tokio::time::sleep(Duration::from_millis(100)).await;
        sd_tx.send(()).unwrap();
        handle.await.unwrap().unwrap();

        let events = drain(&mut evt_rx).await;
        assert!(events.iter().any(|e| matches!(e, ChatEvent::Message { text, .. } if text == "你好 🌍")));
    }

    #[tokio::test]
    async fn run_handles_long_message() {
        let (msg_tx, msg_rx) = mpsc::channel::<String>(16);
        let (evt_tx, mut evt_rx) = mpsc::channel::<ChatEvent>(16);
        let (sd_tx, sd_rx) = watch::channel(());

        let mut j = make_joiner();
        let handle = tokio::spawn(async move {
            j.run(msg_rx, evt_tx, sd_rx).await
        });

        let long = "x".repeat(8192);
        msg_tx.send(long).await.unwrap();
        tokio::time::sleep(Duration::from_millis(100)).await;
        sd_tx.send(()).unwrap();
        handle.await.unwrap().unwrap();

        let events = drain(&mut evt_rx).await;
        assert!(events.iter().any(|e| matches!(e, ChatEvent::Message { text, .. } if text.len() == 8192)));
    }

    // ── run: concurrent send and shutdown ────────────────────────────────────

    #[tokio::test]
    async fn run_shutdown_while_message_in_flight() {
        let (msg_tx, msg_rx) = mpsc::channel::<String>(16);
        let (evt_tx, _evt_rx) = mpsc::channel::<ChatEvent>(16);
        let (sd_tx, sd_rx) = watch::channel(());

        let mut j = make_joiner();
        let handle = tokio::spawn(async move {
            j.run(msg_rx, evt_tx, sd_rx).await
        });

        msg_tx.send("in-flight".to_string()).await.unwrap();
        sd_tx.send(()).unwrap();

        let result = handle.await.unwrap();
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn run_rapid_message_then_shutdown() {
        let (msg_tx, msg_rx) = mpsc::channel::<String>(16);
        let (evt_tx, mut evt_rx) = mpsc::channel::<ChatEvent>(64);
        let (sd_tx, sd_rx) = watch::channel(());

        let mut j = make_joiner();
        let handle = tokio::spawn(async move {
            j.run(msg_rx, evt_tx, sd_rx).await
        });

        for i in 0..50 {
            msg_tx.send(format!("msg-{i}")).await.unwrap();
        }
        sd_tx.send(()).unwrap();
        handle.await.unwrap().unwrap();

        let count = drain(&mut evt_rx).await
            .into_iter()
            .filter(|e| matches!(e, ChatEvent::Message { .. }))
            .count();
        assert_eq!(count, 50);
    }

    // ── run: peer_id and name in emitted events ──────────────────────────────

    #[tokio::test]
    async fn run_emits_correct_peer_name() {
        let (msg_tx, msg_rx) = mpsc::channel::<String>(16);
        let (evt_tx, mut evt_rx) = mpsc::channel::<ChatEvent>(16);
        let (sd_tx, sd_rx) = watch::channel(());

        let mut j = Joiner {
            stream: None,
            peer_id: PeerId("peer-abc".into()),
            name: "alice".into(),
        };
        let handle = tokio::spawn(async move {
            j.run(msg_rx, evt_tx, sd_rx).await
        });

        msg_tx.send("hi".to_string()).await.unwrap();
        tokio::time::sleep(Duration::from_millis(100)).await;
        sd_tx.send(()).unwrap();
        handle.await.unwrap().unwrap();

        let events = drain(&mut evt_rx).await;
        let msg_event = events.iter().find(|e| matches!(e, ChatEvent::Message { .. }));
        assert!(msg_event.is_some());
        if let Some(ChatEvent::Message { name, .. }) = msg_event {
            assert_eq!(name, "alice");
        }
    }
}
