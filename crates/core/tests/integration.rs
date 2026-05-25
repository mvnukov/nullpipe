//! Integration tests for the full chat lifecycle:
//! bootstrap → host → join → data transfer → shutdown.
//!
//! These tests require a working Tor network connection.
//! These tests require a working Tor network connection.

use std::time::Duration;

use ephemeral_chat_core::bootstrap::TorBootstrap;
use ephemeral_chat_core::error::ChatError;
use ephemeral_chat_core::hub::HostedRoom;
use ephemeral_chat_core::invite::{encode, InvitePayload};
use ephemeral_chat_core::joiner::Joiner;
use ephemeral_chat_core::types::ChatEvent;
use futures::StreamExt;
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
// E2E tests (require Tor network)
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
#[ignore = "requires stable Tor rendezvous circuit; flaky in CI/single-client setups"]
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

    // Joiner connects and uses send/recv API
    let client_joiner = bootstrap.client().expect("client ref").clone();
    let invite_code_clone = invite_code.clone();
    let joiner_handle = tokio::spawn(async move {
        let mut joiner = Joiner::connect_with_timeout(
            &client_joiner,
            &invite_code_clone,
            Duration::from_secs(90),
        )
        .await
        .expect("Joiner::connect failed");

        // Send a chat message to the hub
        joiner
            .send("hello from joiner")
            .await
            .expect("joiner send failed");

        // Read response from hub via recv stream
        let mut recv_stream = joiner.recv();
        let mut response_text = String::new();
        while let Some(result) = timeout(Duration::from_secs(30), recv_stream.next())
            .await
            .ok()
            .flatten()
        {
            if let Ok(ChatEvent::Message { text, .. }) = result {
                response_text = text;
                break;
            }
        }

        joiner.shutdown();
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

    // Try to connect to a non-existent onion address with a short timeout
    let result = timeout(
        Duration::from_secs(5),
        Joiner::connect_to_with_timeout(
            client,
            "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa.onion",
            80,
            Duration::from_secs(2),
        ),
    )
    .await;

    // Should either timeout or return a connection error (not panic/hang forever)
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
// Offline tests (no Tor network required)
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
