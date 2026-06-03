//! End-to-end tests for the refactored joiner.
//!
//! Full flow: host a room → joiner connects via invite → messages flow → cleanup.

use std::time::Duration;

use ephemeral_chat_core::types::{ChatEvent, HostConfig, JoinConfig};
use ephemeral_chat_core::{host, join};
use tokio::time::timeout;

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

// ── E2E: joiner connects via room::join, receives messages ───────────────────

#[tokio::test]
async fn e2e_joiner_connects_receives_messages() {
    let (host_h, mut host_ev) = host(HostConfig {
        name: "host".into(),
        invite_ttl_secs: 300,
    });
    
    let (_addr, _port) = wait_for(&mut host_ev, Duration::from_secs(30), |e| match e {
        ChatEvent::RoomReady { onion_address, port } => Some((onion_address.clone(), *port)),
        _ => None,
    })
    .await
    .expect("host RoomReady");

    let code = host_h.invite(None).await.expect("host invite");

    let (joiner_h, _joiner_ev) = join(JoinConfig {
        name: "alice".into(),
        invite_code: code,
    });

    // Host should see peer join
    let (_info, _) = wait_for(&mut host_ev, Duration::from_secs(90), |e| match e {
        ChatEvent::PeerJoin(info) => Some(info.clone()),
        _ => None,
    })
    .await
    .expect("host saw joiner");

    // Send message from joiner
    joiner_h.send("hello from joiner").await.expect("send message");

    // Host should receive it
    let (text, _) = wait_for(&mut host_ev, Duration::from_secs(10), |e| match e {
        ChatEvent::Message { text, .. } if text == "hello from joiner" => Some(text.clone()),
        _ => None,
    })
    .await
    .expect("host received joiner message");
    assert_eq!(text, "hello from joiner");

    joiner_h.quit().await;
    host_h.quit().await;
}

// ── E2E: joiner rejects expired invite ───────────────────────────────────────

#[tokio::test]
async fn e2e_joiner_rejects_expired_invite() {
    use ephemeral_chat_core::invite::{encode, InvitePayload};
    
    // Create an expired invite manually
    let expired_payload = InvitePayload { suggested_name: None,
        onion_address: "vww6ybal6bd7szmgncyruucpgfkqahzddi37ktceo3ah7ngmcopnpyyd.onion".into(),
        nonce: [0x42u8; 16],
        timestamp: 1_700_000_000, // Expired timestamp
    };
    let expired_code = encode(&expired_payload).unwrap();

    let (_joiner_h, mut joiner_ev) = join(JoinConfig {
        name: "alice".into(),
        invite_code: expired_code,
    });

    // Should get an error event
    let (_err, _) = wait_for(&mut joiner_ev, Duration::from_secs(10), |e| match e {
        ChatEvent::Error(_) => Some(()),
        _ => None,
    })
    .await
    .expect("joiner should reject expired invite");
}

// ── E2E: hub sees PeerJoin after successful handshake ────────────────────────

#[tokio::test]
async fn e2e_joiner_handshake_accepted() {
    let (host_h, mut host_ev) = host(HostConfig {
        name: "host".into(),
        invite_ttl_secs: 300,
    });
    
    let (_addr, _port) = wait_for(&mut host_ev, Duration::from_secs(30), |e| match e {
        ChatEvent::RoomReady { onion_address, port } => Some((onion_address.clone(), *port)),
        _ => None,
    })
    .await
    .expect("host RoomReady");

    let code = host_h.invite(None).await.expect("host invite");

    let (_joiner_h, mut _joiner_ev) = join(JoinConfig {
        name: "alice".into(),
        invite_code: code,
    });

    // Hub must see PeerJoin — proves accept byte = 0 was sent and processed
    let (info, _) = wait_for(&mut host_ev, Duration::from_secs(90), |e| match e {
        ChatEvent::PeerJoin(info) => Some(info.clone()),
        _ => None,
    })
    .await
    .expect("host saw joiner join");
    assert!(!info.id.0.is_empty());

    host_h.quit().await;
}

// ── E2E: joiner quit cleans up properly ──────────────────────────────────────

#[tokio::test]
async fn e2e_joiner_quit_cleans_up() {
    let (host_h, mut host_ev) = host(HostConfig {
        name: "host".into(),
        invite_ttl_secs: 300,
    });
    
    let (_addr, _port) = wait_for(&mut host_ev, Duration::from_secs(30), |e| match e {
        ChatEvent::RoomReady { onion_address, port } => Some((onion_address.clone(), *port)),
        _ => None,
    })
    .await
    .expect("host RoomReady");

    let code = host_h.invite(None).await.expect("host invite");

    let (joiner_h, _joiner_ev) = join(JoinConfig {
        name: "alice".into(),
        invite_code: code,
    });

    // Wait for peer to join first
    wait_for(&mut host_ev, Duration::from_secs(90), |e| match e {
        ChatEvent::PeerJoin(_) => Some(()),
        _ => None,
    })
    .await
    .expect("host saw joiner");

    // Now quit - host should see peer leave
    joiner_h.quit().await;

    wait_for(&mut host_ev, Duration::from_secs(10), |e| match e {
        ChatEvent::PeerLeave(_) => Some(()),
        _ => None,
    })
    .await
    .expect("host saw peer leave");

    host_h.quit().await;
}


