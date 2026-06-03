//! Fast integration tests using mock streams (no Tor bootstrap).
//!
//! These tests exercise the same `Hub`, `Joiner`, handshake, and wire protocol
//! code as the real e2e tests, but use `tokio::io::DuplexStream` instead of
//! Tor `DataStream`. They run in milliseconds instead of 40-80 seconds.

use std::time::Duration;

use ephemeral_chat_core::room::{host_with_mock, join_with_mock};
use ephemeral_chat_core::types::{ChatEvent, HostConfig, JoinConfig};
use tokio::sync::mpsc;
use tokio::time::timeout;

/// Helper: drain events from a receiver with a timeout.
async fn drain_events(rx: &mut mpsc::Receiver<ChatEvent>, max: usize) -> Vec<ChatEvent> {
    let mut events = Vec::new();
    for _ in 0..max {
        match timeout(Duration::from_millis(500), rx.recv()).await {
            Ok(Some(ev)) => events.push(ev),
            Ok(None) => break,
            Err(_) => break,
        }
    }
    events
}

#[tokio::test]
async fn integration_peer_join_fires() {
    // Create paired duplex streams
    let (host_stream, joiner_stream) = tokio::io::duplex(4096);

    // Set up channel to send host stream to host_with_mock
    let (stream_tx, stream_rx) = mpsc::channel(1);
    stream_tx.send(host_stream).await.unwrap();
    drop(stream_tx); // Close sender so accept loop knows no more streams coming

    // Start host and joiner with mock streams
    let host_config = HostConfig {
        name: "Host".to_string(),
        invite_ttl_secs: 300,
    };
    let join_config = JoinConfig {
        invite_code: "".to_string(), // Not used in mock path
        name: "Alice".to_string(),
    };

    let (_host_h, mut host_ev) = host_with_mock(host_config, stream_rx);
    let (_joiner_h, mut joiner_ev) = join_with_mock(join_config, joiner_stream);

    // Host should see RoomReady
    let host_events = drain_events(&mut host_ev, 5).await;
    assert!(
        host_events.iter().any(|e| matches!(e, ChatEvent::RoomReady { .. })),
        "host should see RoomReady, got: {host_events:?}"
    );

    // Host should see PeerJoin
    assert!(
        host_events.iter().any(|e| matches!(e, ChatEvent::PeerJoin(_))),
        "host should see PeerJoin, got: {host_events:?}"
    );

    // Joiner should see PeerJoin (its own)
    let joiner_events = drain_events(&mut joiner_ev, 5).await;
    assert!(
        joiner_events.iter().any(|e| matches!(e, ChatEvent::PeerJoin(_))),
        "joiner should see PeerJoin, got: {joiner_events:?}"
    );
}

#[tokio::test]
async fn integration_messages_flow_bidirectional() {
    let (host_stream, joiner_stream) = tokio::io::duplex(4096);

    let (stream_tx, stream_rx) = mpsc::channel(1);
    stream_tx.send(host_stream).await.unwrap();
    drop(stream_tx);

    let host_config = HostConfig {
        name: "Host".to_string(),
        invite_ttl_secs: 300,
    };
    let join_config = JoinConfig {
        invite_code: "".to_string(),
        name: "Alice".to_string(),
    };

    let (host_h, mut host_ev) = host_with_mock(host_config.clone(), stream_rx);
    let (joiner_h, mut joiner_ev) = join_with_mock(join_config, joiner_stream);

    // Wait for connection to establish
    drain_events(&mut host_ev, 2).await;
    drain_events(&mut joiner_ev, 2).await;

    // Host sends a message
    host_h.send("hello from host").await.unwrap();

    // Joiner should receive it
    let joiner_events = drain_events(&mut joiner_ev, 5).await;
    assert!(
        joiner_events.iter().any(|e| matches!(e, ChatEvent::Message { text, .. } if text == "hello from host")),
        "joiner should receive host message, got: {joiner_events:?}"
    );

    // Joiner sends a message back
    joiner_h.send("hello from joiner").await.unwrap();

    // Host should receive it
    let host_events = drain_events(&mut host_ev, 5).await;
    assert!(
        host_events.iter().any(|e| matches!(e, ChatEvent::Message { text, .. } if text == "hello from joiner")),
        "host should receive joiner message, got: {host_events:?}"
    );
}

#[tokio::test]
async fn integration_peer_leave_on_joiner_quit() {
    let (host_stream, joiner_stream) = tokio::io::duplex(4096);

    let (stream_tx, stream_rx) = mpsc::channel(1);
    stream_tx.send(host_stream).await.unwrap();
    drop(stream_tx);

    let host_config = HostConfig {
        name: "Host".to_string(),
        invite_ttl_secs: 300,
    };
    let join_config = JoinConfig {
        invite_code: "".to_string(),
        name: "Alice".to_string(),
    };

    let (_host_h, mut host_ev) = host_with_mock(host_config, stream_rx);
    let (joiner_h, mut joiner_ev) = join_with_mock(join_config, joiner_stream);

    // Wait for connection
    drain_events(&mut host_ev, 2).await;
    drain_events(&mut joiner_ev, 2).await;

    // Joiner quits
    joiner_h.quit().await;

    // Host should see PeerLeave
    let host_events = drain_events(&mut host_ev, 5).await;
    assert!(
        host_events.iter().any(|e| matches!(e, ChatEvent::PeerLeave(_))),
        "host should see PeerLeave, got: {host_events:?}"
    );
}

#[tokio::test]
async fn integration_room_close_notifies_joiner() {
    let (host_stream, joiner_stream) = tokio::io::duplex(4096);

    let (stream_tx, stream_rx) = mpsc::channel(1);
    stream_tx.send(host_stream).await.unwrap();
    drop(stream_tx);

    let host_config = HostConfig {
        name: "Host".to_string(),
        invite_ttl_secs: 300,
    };
    let join_config = JoinConfig {
        invite_code: "".to_string(),
        name: "Alice".to_string(),
    };

    let (host_h, mut host_ev) = host_with_mock(host_config, stream_rx);
    let (_joiner_h, mut joiner_ev) = join_with_mock(join_config, joiner_stream);

    // Wait for connection
    drain_events(&mut host_ev, 2).await;
    drain_events(&mut joiner_ev, 2).await;

    // Host quits (closes the room)
    host_h.quit().await;

    // Joiner should see RoomClosed
    let joiner_events = drain_events(&mut joiner_ev, 5).await;
    assert!(
        joiner_events.iter().any(|e| matches!(e, ChatEvent::RoomClosed)),
        "joiner should see RoomClosed, got: {joiner_events:?}"
    );
}

#[tokio::test]
async fn integration_host_send_message_no_peers() {
    let (host_stream, _joiner_stream) = tokio::io::duplex(4096);

    let (stream_tx, stream_rx) = mpsc::channel(1);
    stream_tx.send(host_stream).await.unwrap();
    drop(stream_tx);

    let host_config = HostConfig {
        name: "Host".to_string(),
        invite_ttl_secs: 300,
    };

    let (host_h, mut host_ev) = host_with_mock(host_config, stream_rx);

    // Wait for RoomReady
    drain_events(&mut host_ev, 2).await;

    // Host sends a message with no peers connected
    // This should not panic or error
    host_h.send("nobody here").await.unwrap();

    // Host should see its own message
    let host_events = drain_events(&mut host_ev, 5).await;
    assert!(
        host_events.iter().any(|e| matches!(e, ChatEvent::Message { text, .. } if text == "nobody here")),
        "host should see its own message, got: {host_events:?}"
    );
}
