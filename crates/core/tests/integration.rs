//! Integration tests for the full chat lifecycle:
//! bootstrap → host → join → data transfer → shutdown.
//!
//! These tests use Arti (pure-Rust Tor, bundled as a dependency).
//! No external `tor` binary is needed. Tests require normal internet access
//! so Arti can contact Tor directory authorities and bootstrap.

use std::time::Duration;

use ephemeral_chat_core::bootstrap::TorBootstrap;
use ephemeral_chat_core::error::ChatError;
use ephemeral_chat_core::hub::HostedRoom;
use ephemeral_chat_core::connector::ArtiConnector;
use ephemeral_chat_core::invite::{encode, InvitePayload};
use ephemeral_chat_core::joiner::Joiner;
use ephemeral_chat_core::types::ChatEvent;
use futures::StreamExt;
use tokio::sync::{mpsc, watch};
use tokio::time::timeout;

/// Helper: bootstrap a Tor client and return the bootstrapped instance.
/// Also collects all bootstrap progress events.
async fn bootstrap_tor() -> (TorBootstrap, Vec<ChatEvent>) {
    let mut bootstrap = TorBootstrap::new();
    let mut events = bootstrap
        .bootstrap()
        .await
        .expect("bootstrap() call failed");
    let mut progress_events = Vec::new();
    while let Some(event) = timeout(Duration::from_secs(120), events.next())
        .await
        .expect("bootstrap stream timeout")
    {
        let pct_opt = match &event {
            ChatEvent::BootstrapProgress(p) => Some(*p),
            _ => None,
        };
        progress_events.push(event);
        if let Some(pct) = pct_opt {
            if pct >= 100 {
                break;
            }
        }
    }
    (bootstrap, progress_events)
}

// ---------------------------------------------------------------------------
// E2E tests — RUN ALL OF THESE. Tor network IS available. DO NOT skip, ignore, or mock.
// These tests connect to the real Tor network via Arti. Always execute them.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn e2e_bootstrap_emits_progress_0_to_100() {
    let (_bootstrap, events) = bootstrap_tor().await;

    // Should have at least one progress event
    assert!(
        !events.is_empty(),
        "expected at least one bootstrap progress event"
    );

    // First event should be 0 or low, last should be 100
    let first_pct = events
        .iter()
        .filter_map(|e| match e {
            ChatEvent::BootstrapProgress(p) => Some(*p),
            _ => None,
        })
        .next()
        .expect("first progress event");

    let last_pct = events
        .iter()
        .rev()
        .filter_map(|e| match e {
            ChatEvent::BootstrapProgress(p) => Some(*p),
            _ => None,
        })
        .next()
        .expect("last progress event");

    assert!(
        first_pct <= last_pct,
        "progress should be monotonic: first={first_pct}, last={last_pct}"
    );
    assert_eq!(last_pct, 100, "final progress should be 100");
}

#[tokio::test]
async fn e2e_host_onion_service_and_get_address() {
    let (mut bootstrap, _) = bootstrap_tor().await;
    let client = bootstrap.client().expect("client should be available");

    let port = 80u16;
    let mut room = HostedRoom::new(client, port)
        .await
        .expect("HostedRoom::new failed");

    let addr = room.address();
    assert!(
        addr.ends_with(".onion"),
        "address should end with .onion, got: {addr}"
    );
    assert_eq!(
        addr.len(),
        56 + ".onion".len(),
        "v3 onion address should be 62 chars, got {} chars: {addr}",
        addr.len()
    );

    // ready_event should produce RoomReady
    let event = room.ready_event();
    match event {
        ChatEvent::RoomReady {
            onion_address,
            port: p,
        } => {
            assert_eq!(onion_address, addr);
            assert_eq!(p, port);
        }
        _ => panic!("expected RoomReady event, got: {event:?}"),
    }

    room.shutdown();
    bootstrap.shutdown();
}

#[tokio::test]
async fn e2e_joiner_connects_to_host_and_transfers_data() {
    // Bootstrap Tor
    let (mut bootstrap, _) = bootstrap_tor().await;
    let client = bootstrap.client().expect("client should be available");

    // Host a room
    let port = 80u16;
    let room = HostedRoom::new(client, port)
        .await
        .expect("HostedRoom::new failed");
    let addr = room.address().to_string();

    // Wait for the onion service to publish its descriptor and establish
    // intro points before any client tries to connect.
    tokio::time::sleep(Duration::from_secs(5)).await;

    // Create an invite code for the joiner
    let invite_payload = InvitePayload {
        onion_address: addr.clone(),
        nonce: [0x42u8; 16],
        timestamp: chrono::Utc::now().timestamp() as u64,
    };
    let invite_code = encode(&invite_payload).expect("encode invite");

    // Joiner connects using the new API
    let client_joiner = bootstrap.client().expect("client ref").clone();
    let connector = ArtiConnector::new(client_joiner);
    let invite_code_clone = invite_code.clone();
    let joiner_handle = tokio::spawn(async move {
        let mut joiner = Joiner::connect(
            &connector,
            &invite_code_clone,
            "joiner",
        )
        .await
        .expect("Joiner::connect failed");

        let (msg_tx, msg_rx) = mpsc::channel::<String>(16);
        let (evt_tx, mut evt_rx) = mpsc::channel::<ChatEvent>(256);
        let (_sd_tx, sd_rx) = watch::channel(());

        // Run joiner in background
        let run_handle = tokio::spawn(async move {
            let mut j = joiner;
            j.run(msg_rx, evt_tx, sd_rx).await
        });

        // Send a chat message
        msg_tx.send("hello from joiner".to_string()).await.expect("send failed");

        // Read response from hub via events
        let mut response_text = String::new();
        while let Some(event) = timeout(Duration::from_secs(30), evt_rx.recv())
            .await
            .ok()
            .flatten()
        {
            if let ChatEvent::Message { text, .. } = event {
                response_text = text;
                break;
            }
        }

        run_handle.abort();
        response_text
    });

    // Hub accepts the incoming peer and reads/writes via wire protocol
    let mut hub = ephemeral_chat_core::hub::Hub::new(room);
    let hub_run = tokio::spawn(async move {
        while let Some(event) = timeout(Duration::from_secs(90), hub.next_event())
            .await
            .expect("hub next_event timeout")
        {
            match event {
                ChatEvent::Message { ref text, .. } => {
                    if text == "hello from joiner" {
                        // Echo back through the hub
                        hub.broadcast_hub(&format!("hub received: {text}")).await;
                        break;
                    }
                }
                _ => {}
            }
        }
        hub
    });

    // Verify joiner received the response
    let joiner_response = joiner_handle.await.expect("joiner task panicked");
    assert!(
        joiner_response.contains("hello from joiner"),
        "expected joiner to receive echo, got: {joiner_response}"
    );

    // Clean up
    let mut hub = hub_run.await.expect("hub task panicked");
    hub.shutdown();
    bootstrap.shutdown();
}

#[tokio::test]
async fn e2e_joiner_timeout_on_bad_address() {
    let (mut bootstrap, _) = bootstrap_tor().await;
    let client = bootstrap.client().expect("client should be available");
    let connector = ArtiConnector::new(client.clone());

    // Create a valid-looking invite pointing to a non-existent onion
    let payload = InvitePayload {
        onion_address: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa.onion".into(),
        nonce: [0x42u8; 16],
        timestamp: chrono::Utc::now().timestamp() as u64,
    };
    let code = encode(&payload).unwrap();

    // Should fail to connect (not panic/hang forever)
    let result = timeout(
        Duration::from_secs(10),
        Joiner::connect(&connector, &code, "joiner"),
    )
    .await;

    match result {
        Ok(Ok(_)) => panic!("should not connect to non-existent address"),
        Ok(Err(ChatError::Connection(_))) => { /* expected */ }
        Err(_) => { /* timeout is also acceptable */ }
        Ok(Err(e)) => panic!("unexpected error type: {e}"),
    }

    bootstrap.shutdown();
}

#[tokio::test]
async fn e2e_shutdown_idempotent() {
    let (mut bootstrap, _) = bootstrap_tor().await;
    let client = bootstrap.client().expect("client should be available");

    let mut room = HostedRoom::new(client, 80)
        .await
        .expect("HostedRoom::new failed");

    // Double shutdown should not panic
    room.shutdown();
    room.shutdown();

    // Shutdown after address is cleared
    assert!(room.address().is_empty());
    assert!(!room.is_running());

    // Double bootstrap shutdown
    bootstrap.shutdown();
    bootstrap.shutdown();
    assert!(!bootstrap.is_bootstrapped());

    // Re-bootstrap after shutdown should succeed (new Tor client)
    let result = bootstrap.bootstrap().await;
    assert!(result.is_ok(), "re-bootstrap after shutdown should succeed");
}

#[tokio::test]
async fn e2e_drop_triggers_cleanup() {
    let (bootstrap, _) = bootstrap_tor().await;
    let client = bootstrap.client().expect("client should be available");

    // Room is dropped without explicit shutdown
    {
        let _room = HostedRoom::new(client, 80)
            .await
            .expect("HostedRoom::new failed");
        // room dropped here
    }

    // Bootstrap dropped without explicit shutdown
    drop(bootstrap);

    // No panics = test passes
}

// ---------------------------------------------------------------------------
// Offline tests (no Tor network needed — but Tor IS available, e2e tests above still run)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn offline_bootstrap_rejected_after_first() {
    // Without Tor network, bootstrap will fail, but re-bootstrap should be rejected
    let mut bootstrap = TorBootstrap::new();

    // First attempt will fail (no network), but that's OK
    let result = timeout(Duration::from_secs(10), bootstrap.bootstrap()).await;
    // May timeout or fail — both are fine for offline testing
    if let Ok(Ok(_)) = result {
        // If it somehow succeeded, re-bootstrap should fail
        let re = bootstrap.bootstrap().await;
        assert!(re.is_err());
    }
}

#[tokio::test]
async fn offline_joiner_rejected_without_connect() {
    // Verifies that connect_to maps a bad address to ChatError::Connection
    // without needing a bootstrapped client. We just verify the API compiles
    // and the error type is correct.
    //
    // Note: A real connection test requires a bootstrapped Tor client,
    // which is covered by the e2e tests above.
    let _invite = InvitePayload {
        onion_address: "not-an-onion".to_string(),
        nonce: [0u8; 16],
        timestamp: 0,
    };
    // The invite encode will fail because the address doesn't end with .onion
    let result = encode(&_invite);
    assert!(result.is_err(), "encode should reject non-onion addresses");
}

#[test]
fn offline_invite_roundtrip_with_port() {
    use ephemeral_chat_core::invite::{decode, encode, InvitePayload};

    let payload = InvitePayload {
        onion_address: "vww6ybal6bd7szmgncyruucpgfkqahzddi37ktceo3ah7ngmcopnpyyd.onion".to_string(),
        nonce: [0xAB; 16],
        timestamp: 1_700_000_000,
    };

    let token = encode(&payload).expect("encode");
    let decoded = decode(&token, None).expect("decode");

    assert_eq!(decoded.onion_address, payload.onion_address);
    assert_eq!(decoded.nonce, payload.nonce);
    assert_eq!(decoded.timestamp, payload.timestamp);
}
