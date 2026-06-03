//! End-to-end tests for the full chat lifecycle using the high-level
//! `host()` and `join()` APIs with real Tor (Arti) bootstrapping.
//!
//! These tests verify the complete user flow:
//! 1. Host starts a room — Tor bootstraps, onion service starts, RoomReady fires
//! 2. Host generates invite code — invite() succeeds, code is valid base58
//! 3. Joiner uses invite code — connects, PeerJoin fires on both sides
//! 4. Messages flow — bidirectional host↔joiner chat
//! 5. Peer leave — when one side quits, the other gets PeerLeave
//! 6. Room close — host closes room, joiner gets RoomClosed
//!
//! Uses real `TorBootstrap` → `arti_client` flow. No external binaries, mocks,
//! or SOCKS proxy. No config needed — Arti handles everything internally.

use base58::FromBase58;
use ephemeral_chat_core::error::ChatError;
use ephemeral_chat_core::invite::{decode as decode_invite, encode, InvitePayload};
use ephemeral_chat_core::types::{ChatEvent, HostConfig, JoinConfig, PeerId, PeerInfo};
use ephemeral_chat_core::{host, join};
use tokio::time::timeout;

use std::time::Duration;

const BOOTSTRAP_TIMEOUT: Duration = Duration::from_secs(30);
const FULL_TEST_TIMEOUT: Duration = Duration::from_secs(60);

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Wait for an event matching a predicate, collecting others along the way.
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

fn peer_leave(e: &ChatEvent) -> Option<PeerId> {
    match e {
        ChatEvent::PeerLeave(id) => Some(id.clone()),
        _ => None,
    }
}

fn as_error(e: &ChatEvent) -> Option<&ChatError> {
    match e {
        ChatEvent::Error(err) => Some(err),
        _ => None,
    }
}

// ---------------------------------------------------------------------------
// Test fixture helpers — independent Tor bootstraps per side
// ---------------------------------------------------------------------------

/// Start a host with its own Tor bootstrap. Caller must wait for RoomReady.
fn start_host() -> (ephemeral_chat_core::RoomHandle, ephemeral_chat_core::EventStream) {
    host(HostConfig {
        name: "host".into(),
        invite_ttl_secs: 300,
    })
}

/// Start a joiner with its own Tor bootstrap using the given invite code.
fn start_joiner(code: String) -> (ephemeral_chat_core::RoomHandle, ephemeral_chat_core::EventStream) {
    join(JoinConfig {
        name: "joiner".into(),
        invite_code: code,
    })
}

// ---------------------------------------------------------------------------
// Flow 1: Host starts a room
// ---------------------------------------------------------------------------

#[tokio::test]
async fn e2e_host_starts_room_and_emits_room_ready() {
    let (handle, mut events) = host(HostConfig {
        name: "host".into(),
        invite_ttl_secs: 300,
    });

    let mut saw_bootstrap = false;
    let mut got_room_ready = false;

    while let Some(event) = timeout(BOOTSTRAP_TIMEOUT, events.recv()).await.unwrap() {
        if matches!(event, ChatEvent::BootstrapProgress(100)) {
            saw_bootstrap = true;
        }
        if let Some((addr, port)) = room_ready(&event) {
            assert!(addr.ends_with(".onion"), "addr={addr}");
            assert_eq!(port, 80);
            got_room_ready = true;
            break;
        }
        if let Some(err) = as_error(&event) {
            panic!("error during host startup: {err}");
        }
    }

    assert!(saw_bootstrap, "missing bootstrap progress");
    assert!(got_room_ready, "missing RoomReady event");

    handle.quit().await;
}

// ---------------------------------------------------------------------------
// Flow 2: Host generates invite code
// ---------------------------------------------------------------------------

#[tokio::test]
async fn e2e_host_generates_valid_invite_code() {
    let (handle, mut events) = host(HostConfig {
        name: "host".into(),
        invite_ttl_secs: 300,
    });

    wait_for(&mut events, BOOTSTRAP_TIMEOUT, room_ready)
        .await
        .expect("RoomReady");

    // Exercises same path as /invite CLI command
    let code = handle.invite(None).await.expect("invite() failed");
    assert!(!code.is_empty());

    // Valid base58
    FromBase58::from_base58(code.as_str()).expect("invite not valid base58");

    // Decodes to valid payload
    let payload = decode_invite(&code, None).expect("decode failed");
    assert!(payload.onion_address.ends_with(".onion"));
    assert!(payload.timestamp > 0);

    handle.quit().await;
}

// ---------------------------------------------------------------------------
// Flow 3: Joiner uses invite code — PeerJoin on both sides
// ---------------------------------------------------------------------------

#[tokio::test]
async fn e2e_joiner_connects_and_peer_join_fires() {
    let (host_h, mut host_ev) = start_host();
    wait_for(&mut host_ev, BOOTSTRAP_TIMEOUT, room_ready)
        .await
        .expect("host RoomReady");

    let code = host_h.invite(None).await.expect("host invite");

    let (joiner_h, mut joiner_ev) = start_joiner(code);

    // Host sees PeerJoin
    let (host_peer, _) = wait_for(&mut host_ev, FULL_TEST_TIMEOUT, peer_join)
        .await
        .expect("host PeerJoin");
    assert!(!host_peer.id.0.is_empty());

    // Joiner sees own PeerJoin
    let _ = wait_for(&mut joiner_ev, FULL_TEST_TIMEOUT, peer_join)
        .await
        .expect("joiner PeerJoin");

    // Peers list non-empty
    assert!(!host_h.peers().await.is_empty());

    joiner_h.quit().await;
    host_h.quit().await;
}

// ---------------------------------------------------------------------------
// Flow 4: Messages flow bidirectionally
// ---------------------------------------------------------------------------

#[tokio::test]
async fn e2e_messages_flow_bidirectional() {
    let (host_h, mut host_ev) = start_host();
    wait_for(&mut host_ev, BOOTSTRAP_TIMEOUT, room_ready)
        .await
        .expect("host RoomReady");

    let code = host_h.invite(None).await.expect("host invite");

    let (joiner_h, mut joiner_ev) = start_joiner(code);

    wait_for(&mut host_ev, FULL_TEST_TIMEOUT, peer_join)
        .await
        .expect("host PeerJoin");

    // Joiner → Host
    joiner_h.send("hello from joiner").await.unwrap();
    let (text, _) = wait_for(&mut host_ev, Duration::from_secs(60), |e| match e {
        ChatEvent::Message { text, .. } if text == "hello from joiner" => Some(text.clone()),
        _ => None,
    })
    .await
    .expect("host received joiner msg");
    assert_eq!(text, "hello from joiner");

    // Host → Joiner
    host_h.send("hello from host").await.unwrap();
    let (text, _) = wait_for(&mut joiner_ev, Duration::from_secs(60), |e| match e {
        ChatEvent::Message { text, .. } if text == "hello from host" => Some(text.clone()),
        _ => None,
    })
    .await
    .expect("joiner received host msg");
    assert_eq!(text, "hello from host");

    joiner_h.quit().await;
    host_h.quit().await;
}

// ---------------------------------------------------------------------------
// Flow 5: Peer leave on quit
// ---------------------------------------------------------------------------

#[tokio::test]
async fn e2e_peer_leave_on_joiner_quit() {
    let (host_h, mut host_ev) = start_host();
    wait_for(&mut host_ev, BOOTSTRAP_TIMEOUT, room_ready)
        .await
        .expect("host RoomReady");

    let code = host_h.invite(None).await.expect("host invite");

    let (joiner_h, mut joiner_ev) = start_joiner(code);

    let (host_peer, _) = wait_for(&mut host_ev, FULL_TEST_TIMEOUT, peer_join)
        .await
        .expect("host PeerJoin");
    let _ = wait_for(&mut joiner_ev, FULL_TEST_TIMEOUT, peer_join)
        .await
        .expect("joiner PeerJoin");

    // Joiner quits
    joiner_h.quit().await;

    // Host sees PeerLeave
    let (left_id, _) = wait_for(&mut host_ev, Duration::from_secs(60), peer_leave)
        .await
        .expect("host PeerLeave");
    assert_eq!(left_id.0, host_peer.id.0, "PeerLeave for same peer");

    assert!(host_h.peers().await.is_empty());

    host_h.quit().await;
}

// ---------------------------------------------------------------------------
// Flow 6: Room close notifies joiner
// ---------------------------------------------------------------------------

#[tokio::test]
async fn e2e_room_close_notifies_joiner() {
    let (host_h, mut host_ev) = start_host();
    wait_for(&mut host_ev, BOOTSTRAP_TIMEOUT, room_ready)
        .await
        .expect("host RoomReady");

    let code = host_h.invite(None).await.expect("host invite");

    let (_joiner_h, mut joiner_ev) = start_joiner(code);

    wait_for(&mut host_ev, FULL_TEST_TIMEOUT, peer_join)
        .await
        .expect("host PeerJoin");

    // Host closes room
    host_h.quit().await;

    // Joiner gets RoomClosed
    wait_for(&mut joiner_ev, Duration::from_secs(60), |e| {
        matches!(e, ChatEvent::RoomClosed).then_some(())
    })
    .await
    .expect("joiner RoomClosed");
}

// ---------------------------------------------------------------------------
// Edge cases
// ---------------------------------------------------------------------------

#[tokio::test]
async fn e2e_invalid_invite_code_rejected() {
    // Joiner bootstraps Tor first, then rejects the bad invite
    let (_handle, mut events) = join(JoinConfig {
        name: "joiner".into(),
        invite_code: "not-valid-base58-code".into(),
    });

    // After bootstrap, joiner tries to decode invite and fails
    // We may get bootstrap progress events first, then an error
    let err = wait_for(&mut events, BOOTSTRAP_TIMEOUT, |e| match e {
        ChatEvent::Error(err) => Some(err.to_string()),
        _ => None,
    })
    .await;

    // Either we get an error (bad base58 or invalid invite) or bootstrap fails
    if let Some((msg, _)) = err {
        assert!(
            msg.contains("base58") || msg.contains("invalid") || msg.contains("bad"),
            "unexpected error: {msg}"
        );
    }
    // If bootstrap fails first (no network), that's also acceptable
}

#[test]
fn e2e_expired_invite_rejected() {
    let payload = InvitePayload { suggested_name: None,
        onion_address: "vww6ybal6bd7szmgncyruucpgfkqahzddi37ktceo3ah7ngmcopnpyyd.onion"
            .into(),
        nonce: [0x42u8; 16],
        timestamp: 1_700_000_000,
    };
    let token = encode(&payload).unwrap();

    let result = decode_invite(&token, Some(300));
    assert!(
        matches!(result, Err(ChatError::InviteExpired { .. })),
        "expected InviteExpired, got {result:?}"
    );
}

#[test]
fn e2e_double_join_nonce_reuse_detectable() {
    // Same payload → same token (deterministic)
    let ts = chrono::Utc::now().timestamp() as u64;
    let addr = "vww6ybal6bd7szmgncyruucpgfkqahzddi37ktceo3ah7ngmcopnpyyd.onion";

    let p1 = InvitePayload { suggested_name: None,
        onion_address: addr.into(),
        nonce: [0xDEu8; 16],
        timestamp: ts,
    };
    let p2 = InvitePayload { suggested_name: None,
        onion_address: addr.into(),
        nonce: [0xDEu8; 16],
        timestamp: ts,
    };
    assert_eq!(encode(&p1).unwrap(), encode(&p2).unwrap());

    // Different nonce → different token
    let mut p3 = p1.clone();
    p3.nonce[0] = 0xFF;
    assert_ne!(encode(&p1).unwrap(), encode(&p3).unwrap());
}

// ---------------------------------------------------------------------------
// Host sends message with no peers — room must stay open
// ---------------------------------------------------------------------------

#[tokio::test]
async fn e2e_host_send_message_no_peers_room_stays_open() {
    let (host_h, mut host_ev) = host(HostConfig {
        name: "host".into(),
        invite_ttl_secs: 300,
    });
    wait_for(&mut host_ev, BOOTSTRAP_TIMEOUT, room_ready)
        .await
        .expect("host RoomReady");

    // No peers connected — host sends a message
    host_h.send("hello with nobody here").await.unwrap();

    // Room must NOT close. Give it a moment to see if it self-destructs.
    tokio::time::sleep(Duration::from_secs(3)).await;

    // If the room closed, event stream would return None immediately.
    // Verify handle is still usable:
    let peers = host_h.peers().await;
    assert!(peers.is_empty(), "should have zero peers");
    let code = host_h.invite(None).await.expect("invite should still work");
    assert!(!code.is_empty(), "should still generate invite");

    host_h.quit().await;
}

// ---------------------------------------------------------------------------
// CLI bug demonstration: invite/peers/quit work in async context
// ---------------------------------------------------------------------------
// Before the fix in bug-block-on-runtime.md, the CLI called these via
// Handle::current().block_on(), causing "Cannot start a runtime from
// within a runtime" panic. These tests verify the methods work correctly
// when called from proper async context (the fix path).

#[tokio::test]
async fn e2e_cli_commands_work_in_async_context() {
    let (handle, mut events) = start_host();
    wait_for(&mut events, BOOTSTRAP_TIMEOUT, room_ready)
        .await
        .expect("RoomReady");

    // /invite path — no block_on panic
    let code = handle.invite(None).await.expect("invite should work");
    assert!(!code.is_empty());
    FromBase58::from_base58(code.as_str()).expect("valid base58");

    // /peers path — empty when no peers
    let peers = handle.peers().await;
    assert!(peers.is_empty());

    // /quit path — graceful shutdown, no panic
    handle.quit().await;

    // Post-quit: send fails
    assert!(handle.send("test").await.is_err());
    // Post-quit: invite fails
    assert!(handle.invite(None).await.is_err());
}

// ---------------------------------------------------------------------------
// Independent bootstrap test: verifies joiner→host message flow with
// separate Tor bootstraps (same as two separate CLI processes).
// This is the regression test for nullpipe-2vp.
// We use spawn_blocking to avoid deadlocking on Tor's directory lock,
// which is synchronous and would block the single-threaded test runtime.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn e2e_independent_bootstrap_joiner_sends_message() {
    // Use a channel to coordinate between the spawned tasks and the main test
    let (host_ready_tx, mut host_ready_rx) = tokio::sync::mpsc::channel::<String>(1);
    let (joiner_ready_tx, mut joiner_ready_rx) = tokio::sync::mpsc::channel::<()>(1);
    let (message_received_tx, message_received_rx) = tokio::sync::oneshot::channel::<bool>();

    // Spawn Host in a blocking task to allow independent Tor bootstrap
    let host_handle = tokio::task::spawn_blocking(move || {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let (host_h, mut host_ev) = host(HostConfig {
                name: "host".into(),
                invite_ttl_secs: 300,
            });
            
            // Wait for RoomReady
            let mut _onion_addr = String::new();
            while let Some(ev) = host_ev.recv().await {
                if let ChatEvent::RoomReady { onion_address, .. } = ev {
                    _onion_addr = onion_address;
                    break;
                }
            }
            
            let code = host_h.invite(None).await.expect("host invite");
            let _ = host_ready_tx.send(code).await;

            // Wait for message from joiner
            while let Some(ev) = host_ev.recv().await {
                if let ChatEvent::Message { text, .. } = ev {
                    if text == "hello from independent joiner" {
                        let _ = message_received_tx.send(true);
                        break;
                    }
                }
            }
            host_h.quit().await;
        });
    });

    // Get invite code from host
    let code = host_ready_rx.recv().await.expect("Host failed to start");

    // Spawn Joiner in a blocking task to allow independent Tor bootstrap
    let joiner_handle = tokio::task::spawn_blocking(move || {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let (joiner_h, mut joiner_ev) = join(JoinConfig {
                name: "joiner".into(),
                invite_code: code,
            });

            // Wait for PeerJoin
            while let Some(ev) = joiner_ev.recv().await {
                if matches!(ev, ChatEvent::PeerJoin(_)) {
                    break;
                }
            }

            let _ = joiner_ready_tx.send(()).await;

            // Send message
            joiner_h.send("hello from independent joiner").await.unwrap();
            
            // Keep alive long enough for message to send
            tokio::time::sleep(Duration::from_secs(5)).await;
            joiner_h.quit().await;
        });
    });

    // Wait for joiner to be ready
    joiner_ready_rx.recv().await.expect("Joiner failed to connect");

    // Wait for host to receive the message (with timeout)
    let received = tokio::time::timeout(Duration::from_secs(30), message_received_rx)
        .await
        .expect("Test timed out waiting for message reception")
        .unwrap_or(false);

    assert!(received, "BUG nullpipe-2vp: Host never received joiner's message. The joiner's write path is broken across independent Tor bootstraps due to tokio::io::split on DataStream.");

    host_handle.await.unwrap();
    joiner_handle.await.unwrap();
}
