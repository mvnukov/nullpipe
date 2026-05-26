//! End-to-end tests for the new Joiner API.
//!
//! Full flow: host a room → joiner connects via invite → messages flow → cleanup.

use std::time::Duration;

use ephemeral_chat_core::connector::ArtiConnector;
use ephemeral_chat_core::invite::decode as decode_invite;
use ephemeral_chat_core::types::{ChatEvent, HostConfig, JoinConfig, PeerInfo};
use ephemeral_chat_core::{host, host_with_client, join_with_client, SharedTorClient};
use tokio::sync::{mpsc, watch};
use tokio::time::timeout;

const BOOTSTRAP_TIMEOUT: Duration = Duration::from_secs(30);
const TEST_TIMEOUT: Duration = Duration::from_secs(90);

async fn wait_for<F, T>(
    rx: &mut ephemeral_chat_core::EventStream,
    dur: Duration,
    pred: F,
) -> Option<(T, Vec<ChatEvent>)>
where
    F: Fn(&ChatEvent) -> Option<T>,
{
    let mut others = Vec::new();
    loop {
        match timeout(dur, rx.recv()).await {
            Ok(Some(e)) => {
                if let Some(v) = pred(&e) {
                    return Some((v, others));
                }
                others.push(e);
            }
            Ok(None) => return None,
            Err(_) => return None,
        }
    }
}

fn room_ready(e: &ChatEvent) -> Option<(String, u16)> {
    match e {
        ChatEvent::RoomReady {
            onion_address,
            port,
        } => Some((onion_address.clone(), *port)),
        _ => None,
    }
}

fn peer_join(e: &ChatEvent) -> Option<PeerInfo> {
    match e {
        ChatEvent::PeerJoin(info) => Some(info.clone()),
        _ => None,
    }
}

// ── E2E: joiner connects via new API, receives messages ──────────────────────
// Requires: run() implementation

#[tokio::test]
#[ignore = "requires run() implementation"]
async fn e2e_joiner_connects_receives_messages() {
    let tor = SharedTorClient::bootstrap().await.expect("Tor bootstrap");

    let (host_h, mut host_ev) = host_with_client(
        HostConfig {
            name: "host".into(),
            invite_ttl_secs: 300,
        },
        &tor,
    );
    wait_for(&mut host_ev, BOOTSTRAP_TIMEOUT, room_ready)
        .await
        .expect("host RoomReady");

    let code = host_h.invite().await.expect("host invite");

    let connector = ArtiConnector::new(tor.client().clone());
    let mut joiner = ephemeral_chat_core::joiner::Joiner::connect(&connector, &code, "alice")
        .await
        .expect("Joiner::connect failed");

    let (msg_tx, msg_rx) = mpsc::channel::<String>(16);
    let (evt_tx, mut evt_rx) = mpsc::channel::<ChatEvent>(256);
    let (_sd_tx, sd_rx) = watch::channel(());

    let joiner_task = tokio::spawn(async move {
        let _ = joiner.run(msg_rx, evt_tx, sd_rx).await;
    });

    wait_for(&mut host_ev, TEST_TIMEOUT, peer_join)
        .await
        .expect("host saw joiner");

    msg_tx
        .send("hello from new joiner".to_string())
        .await
        .expect("send message");

    let (text, _) = wait_for(&mut host_ev, TEST_TIMEOUT, |e| match e {
        ChatEvent::Message { text, .. } if text == "hello from new joiner" => Some(text.clone()),
        _ => None,
    })
    .await
    .expect("host received joiner message");
    assert_eq!(text, "hello from new joiner");

    joiner_task.abort();
    host_h.quit().await;
}

// ── E2E: joiner rejects expired invite ───────────────────────────────────────

#[tokio::test]
async fn e2e_joiner_rejects_expired_invite() {
    let tor = SharedTorClient::bootstrap().await.expect("Tor bootstrap");

    let (host_h, mut host_ev) = host_with_client(
        HostConfig {
            name: "host".into(),
            invite_ttl_secs: 300,
        },
        &tor,
    );
    wait_for(&mut host_ev, BOOTSTRAP_TIMEOUT, room_ready)
        .await
        .expect("host RoomReady");

    let code = host_h.invite().await.expect("host invite");
    let payload = decode_invite(&code, None).expect("decode invite");

    let expired_payload = ephemeral_chat_core::invite::InvitePayload {
        onion_address: payload.onion_address.clone(),
        nonce: payload.nonce,
        timestamp: 1_700_000_000,
    };
    let expired_code = ephemeral_chat_core::invite::encode(&expired_payload).unwrap();

    let connector = ArtiConnector::new(tor.client().clone());
    let result = ephemeral_chat_core::joiner::Joiner::connect(&connector, &expired_code, "alice").await;
    assert!(result.is_err(), "joiner should reject expired invite");

    host_h.quit().await;
}

// ── E2E: hub sees PeerJoin after successful handshake ────────────────────────

#[tokio::test]
async fn e2e_joiner_handshake_accepted() {
    let tor = SharedTorClient::bootstrap().await.expect("Tor bootstrap");

    let (host_h, mut host_ev) = host_with_client(
        HostConfig {
            name: "host".into(),
            invite_ttl_secs: 300,
        },
        &tor,
    );
    wait_for(&mut host_ev, BOOTSTRAP_TIMEOUT, room_ready)
        .await
        .expect("host RoomReady");

    let code = host_h.invite().await.expect("host invite");

    let connector = ArtiConnector::new(tor.client().clone());
    let mut joiner = ephemeral_chat_core::joiner::Joiner::connect(&connector, &code, "alice")
        .await
        .expect("Joiner::connect failed");

    // Hub must see PeerJoin — proves accept byte = 0 was sent and processed
    let (info, _) = wait_for(&mut host_ev, TEST_TIMEOUT, peer_join)
        .await
        .expect("host saw joiner join");
    assert!(!info.id.0.is_empty());

    joiner.close();
    host_h.quit().await;
}

// ── E2E: joiner close cleans up properly ─────────────────────────────────────

#[tokio::test]
async fn e2e_joiner_close_cleans_up() {
    let tor = SharedTorClient::bootstrap().await.expect("Tor bootstrap");

    let (host_h, mut host_ev) = host_with_client(
        HostConfig {
            name: "host".into(),
            invite_ttl_secs: 300,
        },
        &tor,
    );
    wait_for(&mut host_ev, BOOTSTRAP_TIMEOUT, room_ready)
        .await
        .expect("host RoomReady");

    let code = host_h.invite().await.expect("host invite");

    let connector = ArtiConnector::new(tor.client().clone());
    let mut joiner = ephemeral_chat_core::joiner::Joiner::connect(&connector, &code, "alice")
        .await
        .expect("Joiner::connect failed");

    joiner.close();

    wait_for(&mut host_ev, TEST_TIMEOUT, |e| match e {
        ChatEvent::PeerLeave(_) => Some(()),
        _ => None,
    })
    .await
    .expect("host saw peer leave");

    host_h.quit().await;
}

// ── E2E: joiner run exits on shutdown signal ─────────────────────────────────
// Requires: run() implementation

#[tokio::test]
#[ignore = "requires run() implementation"]
async fn e2e_joiner_respects_shutdown_signal() {
    let tor = SharedTorClient::bootstrap().await.expect("Tor bootstrap");

    let (host_h, mut host_ev) = host_with_client(
        HostConfig {
            name: "host".into(),
            invite_ttl_secs: 300,
        },
        &tor,
    );
    wait_for(&mut host_ev, BOOTSTRAP_TIMEOUT, room_ready)
        .await
        .expect("host RoomReady");

    let code = host_h.invite().await.expect("host invite");

    let connector = ArtiConnector::new(tor.client().clone());
    let mut joiner = ephemeral_chat_core::joiner::Joiner::connect(&connector, &code, "alice")
        .await
        .expect("Joiner::connect failed");

    let (msg_tx, msg_rx) = mpsc::channel::<String>(16);
    let (evt_tx, evt_rx) = mpsc::channel::<ChatEvent>(256);
    let (sd_tx, sd_rx) = watch::channel(());

    let joiner_task = tokio::spawn(async move {
        joiner.run(msg_rx, evt_tx, sd_rx).await
    });

    sd_tx.send(()).expect("send shutdown");

    let result = timeout(Duration::from_secs(10), joiner_task)
        .await
        .expect("joiner task should complete on shutdown")
        .expect("joiner task panicked");

    assert!(result.is_ok(), "run should return Ok on clean shutdown");

    host_h.quit().await;
}
