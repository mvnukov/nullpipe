//! Integration tests for the new Joiner API.
//!
//! Uses MockTorConnector for offline testing — no real Tor network needed.

use std::time::Duration;

use ephemeral_chat_core::connector::mock::MockTorConnector;
use ephemeral_chat_core::invite::{encode, InvitePayload};
use ephemeral_chat_core::joiner::Joiner;
use ephemeral_chat_core::types::{ChatEvent, PeerId};
use tokio::sync::{mpsc, watch};

// ── connect() with mock connector ────────────────────────────────────────────

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
    let payload = InvitePayload {
        onion_address: "vww6ybal6bd7szmgncyruucpgfkqahzddi37ktceo3ah7ngmcopnpyyd.onion".into(),
        nonce: [0xABu8; 16],
        timestamp: chrono::Utc::now().timestamp() as u64,
    };
    let code = encode(&payload).unwrap();

    let mock = MockTorConnector::with_connect_result(Err(
        ephemeral_chat_core::error::ChatError::Connection("network down".into())
    ));
    let result = Joiner::connect(&mock, &code, "alice").await;
    assert!(result.is_err());
}

// ── run() tests ──────────────────────────────────────────────────────────────

fn make_joiner() -> Joiner {
    Joiner::new_for_test(PeerId("test".into()), "test".into())
}

async fn drain(rx: &mut mpsc::Receiver<ChatEvent>) -> Vec<ChatEvent> {
    let mut events = Vec::new();
    while let Ok(Some(e)) = tokio::time::timeout(Duration::from_millis(50), rx.recv()).await {
        events.push(e);
    }
    events
}

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

#[tokio::test]
async fn run_forwards_messages_in_order() {
    let (msg_tx, msg_rx) = mpsc::channel::<String>(16);
    let (evt_tx, mut evt_rx) = mpsc::channel::<ChatEvent>(16);
    let (sd_tx, sd_rx) = watch::channel(());

    let mut j = Joiner::new_for_test(PeerId("p1".into()), "alice".into());
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
async fn run_emits_peer_name_in_events() {
    let (msg_tx, msg_rx) = mpsc::channel::<String>(16);
    let (evt_tx, mut evt_rx) = mpsc::channel::<ChatEvent>(16);
    let (sd_tx, sd_rx) = watch::channel(());

    let mut j = Joiner::new_for_test(PeerId("p1".into()), "alice".into());
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

#[tokio::test]
async fn run_handles_empty_message() {
    let (msg_tx, msg_rx) = mpsc::channel::<String>(16);
    let (evt_tx, mut evt_rx) = mpsc::channel::<ChatEvent>(16);
    let (sd_tx, sd_rx) = watch::channel(());

    let mut j = Joiner::new_for_test(PeerId("p1".into()), "alice".into());
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

    let mut j = Joiner::new_for_test(PeerId("p1".into()), "alice".into());
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
async fn run_rapid_messages_then_shutdown() {
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

// ── close / drop ─────────────────────────────────────────────────────────────

#[test]
fn close_is_idempotent() {
    let mut j = make_joiner();
    j.close();
    j.close();
}

#[test]
fn drop_calls_close() {
    drop(make_joiner());
}
