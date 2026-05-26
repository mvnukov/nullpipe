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
use tokio::sync::{mpsc, watch};
use tokio::time::Duration;

use crate::error::Result;
use crate::connector::TorConnector;
use crate::types::{ChatEvent, PeerId};

/// How long to wait for data before considering the peer dead.
const READ_TIMEOUT: Duration = Duration::from_secs(60);

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
        todo!()
    }

    /// Internal: wire-level handshake on a fresh stream.
    ///
    /// Called by [`Joiner::connect`]. Generates nonce+discriminator, writes 32 bytes,
    /// reads accept/reject byte, sends display name as first wire message.
    async fn handshake(stream: DataStream, name: &str) -> Result<(PeerId, DataStream)> {
        todo!()
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
        todo!()
    }

    /// Close the connection. Idempotent.
    pub fn close(&mut self) {
        self.stream = None;
    }

    /// Test constructor. Not for production use.
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

    // ── connect: invite validation (before Tor connect) ──────────────────────

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
        use crate::invite::{encode, InvitePayload};

        let payload = InvitePayload {
            onion_address: "not-an-onion-address".into(),
            nonce: [0x42u8; 16],
            timestamp: chrono::Utc::now().timestamp() as u64,
        };
        let code = encode(&payload).unwrap();

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
        let (evt_tx, mut evt_rx) = mpsc::channel::<ChatEvent>(16);
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
