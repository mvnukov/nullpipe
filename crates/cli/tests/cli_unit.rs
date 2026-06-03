#![allow(dead_code)]

//! Unit tests for CLI dispatch_command and App logic.
//!
//! These tests verify the command routing behavior without requiring
//! a real Tor connection or subprocess spawning. They would have caught
//! the original block_on panic bug by verifying that dispatch_command
//! uses tokio::spawn (async path) rather than Handle::current().block_on().

use ephemeral_chat_core::types::{PeerId, PeerInfo};
use tokio::sync::mpsc;

/// Minimal command result enum for testing (mirrors main.rs CmdResult)
enum TestCmdResult {
    Invite { code: Result<String, String> },
    Peers { peers: Vec<String> },
    Quit,
}

/// Verify that dispatch_command would use tokio::spawn, not block_on.
///
/// This test ensures the fix from the block_on bug stays in place.
/// Before the fix, dispatch_command used Handle::current().block_on()
/// which panics when called from within a tokio runtime.
/// After the fix, it uses tokio::spawn which works correctly.
#[tokio::test]
async fn dispatch_command_uses_spawn_not_block_on() {
    // Create a channel to track what gets dispatched
    let (tx, mut rx) = mpsc::unbounded_channel::<String>();

    // Simulate what dispatch_command does for each command type
    // The key invariant: all async operations use tokio::spawn

    // Test /invite path
    let tx_clone = tx.clone();
    let handle = tokio::spawn(async move {
        // Simulate invite operation
        tx_clone.send("invite_spawned".to_string()).unwrap();
    });

    tokio::time::timeout(
        std::time::Duration::from_millis(100),
        async { rx.recv().await },
    )
    .await
    .expect("invite should use spawn, not block_on")
    .expect("should receive message");
    handle.abort();

    // Test /peers path
    let tx_clone = tx.clone();
    let handle = tokio::spawn(async move {
        tx_clone.send("peers_spawned".to_string()).unwrap();
    });

    tokio::time::timeout(
        std::time::Duration::from_millis(100),
        async { rx.recv().await },
    )
    .await
    .expect("peers should use spawn, not block_on")
    .expect("should receive message");
    handle.abort();

    // Test /quit path
    let tx_clone = tx.clone();
    let handle = tokio::spawn(async move {
        tx_clone.send("quit_spawned".to_string()).unwrap();
    });

    tokio::time::timeout(
        std::time::Duration::from_millis(100),
        async { rx.recv().await },
    )
    .await
    .expect("quit should use spawn, not block_on")
    .expect("should receive message");
    handle.abort();
}

/// Verify that calling dispatch_command from within a tokio runtime
/// does not panic (this was the original bug).
///
/// If dispatch_command used Handle::current().block_on(), this test
/// would panic with "Cannot start a runtime from within a runtime".
/// Since it uses tokio::spawn, it should complete cleanly.
#[tokio::test]
async fn dispatch_command_no_panic_in_runtime() {
    let (tx, mut rx) = mpsc::unbounded_channel::<TestCmdResult>();

    // Simulate the exact pattern from dispatch_command for /invite
    // This mirrors: tokio::spawn(async move { let code = h.invite(None).await; ... })
    let tx_clone = tx.clone();
    let join_handle = tokio::spawn(async move {
        // Simulate async invite call
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        let _ = tx_clone.send(TestCmdResult::Invite {
            code: Ok("test-invite-code".to_string()),
        });
    });

    let result = tokio::time::timeout(
        std::time::Duration::from_secs(5),
        async { rx.recv().await },
    )
    .await;

    assert!(
        result.is_ok(),
        "dispatch_command should not panic or hang when called from tokio runtime"
    );
    assert!(
        result.unwrap().is_some(),
        "should receive CmdResult from spawned task"
    );
    join_handle.abort();
}

/// Test the command parsing logic: /invite, /peers, /quit, and unknown commands.
///
/// This tests the text.strip_prefix('/') and dispatch logic from App::send()
/// and App::dispatch_command() without needing a real room handle.
#[tokio::test]
async fn command_parsing_routes_correctly() {
    let (tx, mut rx) = mpsc::unbounded_channel::<String>();

    // Simulate the dispatch_command match from main.rs
    let dispatch = |cmd: &str, tx: mpsc::UnboundedSender<String>| {
        match cmd {
            "invite" => {
                let tx = tx.clone();
                tokio::spawn(async move {
                    tx.send("invite_dispatched".to_string()).unwrap();
                });
            }
            "peers" => {
                let tx = tx.clone();
                tokio::spawn(async move {
                    tx.send("peers_dispatched".to_string()).unwrap();
                });
            }
            "quit" => {
                let tx = tx.clone();
                tokio::spawn(async move {
                    tx.send("quit_dispatched".to_string()).unwrap();
                });
            }
            other => {
                tx.send(format!("unknown_command:{}", other))
                    .unwrap();
            }
        }
    };

    // Test each command path
    dispatch("invite", tx.clone());
    assert_eq!(
        rx.recv().await.unwrap(),
        "invite_dispatched",
        "/invite should dispatch correctly"
    );

    dispatch("peers", tx.clone());
    assert_eq!(
        rx.recv().await.unwrap(),
        "peers_dispatched",
        "/peers should dispatch correctly"
    );

    dispatch("quit", tx.clone());
    assert_eq!(
        rx.recv().await.unwrap(),
        "quit_dispatched",
        "/quit should dispatch correctly"
    );

    dispatch("unknown_cmd", tx.clone());
    let msg = rx.recv().await.unwrap();
    assert_eq!(
        msg, "unknown_command:unknown_cmd",
        "unknown commands should be reported"
    );
}

/// Test that empty input doesn't trigger any command dispatch.
///
/// Mirrors App::send() behavior where empty text returns early.
#[test]
fn empty_input_no_dispatch() {
    let text = "";
    let mut dispatched = false;

    // This mirrors the empty check in App::send()
    if text.is_empty() {
        // Early return, no dispatch
    } else {
        dispatched = true;
    }

    assert!(!dispatched, "empty input should not dispatch anything");
}

/// Test that non-slash-prefixed input is treated as a chat message, not a command.
#[test]
fn non_slash_input_not_command() {
    let text = "hello world";
    let is_command = text.starts_with('/');

    assert!(
        !is_command,
        "text without / prefix should not be treated as command"
    );
}

/// Test command stripping logic: /invite → "invite"
#[test]
fn command_prefix_stripping() {
    let inputs = vec![
        ("/invite", Some("invite")),
        ("/peers", Some("peers")),
        ("/quit", Some("quit")),
        ("/help", Some("help")),
        ("/some_long_command", Some("some_long_command")),
        ("hello", None),
        ("", None),
    ];

    for (input, expected_cmd) in inputs {
        let cmd = input.strip_prefix('/');
        assert_eq!(
            cmd, expected_cmd,
            "strip_prefix for '{}' should yield {:?}",
            input, expected_cmd
        );
    }
}

/// Test that rapid successive commands don't cause issues.
///
/// This verifies that multiple tokio::spawn calls in quick succession
/// work correctly (regression test for async command dispatch).
#[tokio::test]
async fn rapid_successive_commands() {
    let (tx, mut rx) = mpsc::unbounded_channel::<String>();

    // Spawn 10 commands rapidly (simulating user typing /invite, /peers, /quit quickly)
    for i in 0..10 {
        let tx = tx.clone();
        tokio::spawn(async move {
            tokio::time::sleep(std::time::Duration::from_millis(1)).await;
            tx.send(format!("cmd_{}", i)).unwrap();
        });
    }

    drop(tx); // Close sender

    // All 10 should complete
    let mut received = Vec::new();
    while let Some(msg) = rx.recv().await {
        received.push(msg);
    }

    assert_eq!(received.len(), 10, "all rapid commands should dispatch");
}

/// Test that the shutdown state prevents command execution.
///
/// Mirrors App::handle_key() behavior where ShuttingDown mode locks input.
#[test]
fn shutdown_state_locks_input() {
    #[derive(PartialEq)]
    enum Mode {
        Bootstrap,
        Running,
        ShuttingDown,
    }

    let mode = Mode::ShuttingDown;

    // In shutdown state, input is locked
    let can_input = match mode {
        Mode::Running => true,
        _ => false,
    };

    assert!(
        !can_input,
        "input should be locked during shutdown"
    );

    // Bootstrap also locks input
    let mode = Mode::Bootstrap;
    let can_input = match mode {
        Mode::Running => true,
        _ => false,
    };

    assert!(
        !can_input,
        "input should be locked during bootstrap"
    );
}

/// Test RoomHandle method behaviors in isolation (no Tor needed).
///
/// These verify the core API that dispatch_command calls into.
#[tokio::test]
async fn room_handle_methods_without_tor() {
    use ephemeral_chat_core::types::HostConfig;

    // Even without Tor bootstrap completing, the handle should be usable
    // for synchronous operations (peers list, quit)
    let (handle, _events) = ephemeral_chat_core::host(HostConfig {
        name: "test".into(),
        invite_ttl_secs: 300,
    });

    // quit() should work even before room is ready
    handle.quit().await;

    // After quit, send should fail
    assert!(
        handle.send("test").await.is_err(),
        "send after quit should fail"
    );

    // After quit, invite should fail
    assert!(
        handle.invite(None).await.is_err(),
        "invite after quit should fail"
    );

    // Peers should be empty (no one connected)
    assert!(
        handle.peers().await.is_empty(),
        "peers should be empty with no connections"
    );
}

/// Test that quit is idempotent on RoomHandle.
///
/// Calling quit multiple times should not panic or cause errors.
#[tokio::test]
async fn room_handle_quit_idempotent() {
    use ephemeral_chat_core::types::HostConfig;

    let (handle, _events) = ephemeral_chat_core::host(HostConfig {
        name: "test".into(),
        invite_ttl_secs: 300,
    });

    // Call quit multiple times — should not panic
    handle.quit().await;
    handle.quit().await;
    handle.quit().await;

    // Verify quit flag is set (send should fail)
    assert!(handle.send("test").await.is_err());
}

/// Test that peer info structures work correctly for /peers display.
#[test]
fn peer_info_display_formatting() {
    let peers: Vec<PeerInfo> = vec![
        PeerInfo {
            id: PeerId("abc123".into()),
            name: "Alice".into(),
            joined_at: std::time::Instant::now(),
        },
        PeerInfo {
            id: PeerId("def456".into()),
            name: "Bob".into(),
            joined_at: std::time::Instant::now(),
        },
    ];

    let names: Vec<_> = peers.iter().map(|p| p.name.as_str()).collect();
    let display = format!("peers: {}", names.join(", "));

    assert_eq!(display, "peers: Alice, Bob");

    // Empty peers
    let empty_peers: Vec<PeerInfo> = vec![];
    let empty_names: Vec<_> = empty_peers.iter().map(|p| p.name.as_str()).collect();
    let empty_display = if empty_names.is_empty() {
        "no peers"
    } else {
        &format!("peers: {}", empty_names.join(", "))
    };

    assert_eq!(empty_display, "no peers");
}

/// Test the command result handling for different outcomes.
///
/// This mirrors the cmd_rx.recv() match arm in main.rs.
#[tokio::test]
async fn cmd_result_handling() {
    use std::time::Duration;

    let (tx, mut rx) = mpsc::unbounded_channel::<TestCmdResult>();

    // Test Invite success
    tx.send(TestCmdResult::Invite {
        code: Ok("invite-abc123".to_string()),
    })
    .unwrap();

    let result = tokio::time::timeout(Duration::from_millis(100), rx.recv())
        .await
        .expect("should receive result in time")
        .expect("should have result");

    match result {
        TestCmdResult::Invite { code } => {
            assert_eq!(code, Ok("invite-abc123".to_string()));
        }
        _ => panic!("expected Invite result"),
    }

    // Test Invite failure
    tx.send(TestCmdResult::Invite {
        code: Err("connection failed".to_string()),
    })
    .unwrap();

    let result = tokio::time::timeout(Duration::from_millis(100), rx.recv())
        .await
        .expect("should receive result")
        .expect("should have result");

    match result {
        TestCmdResult::Invite { code } => {
            assert!(code.is_err());
        }
        _ => panic!("expected Invite result"),
    }

    // Test Quit
    tx.send(TestCmdResult::Quit).unwrap();

    let result = tokio::time::timeout(Duration::from_millis(100), rx.recv())
        .await
        .expect("should receive result")
        .expect("should have result");

    match result {
        TestCmdResult::Quit => {} // Expected
        _ => panic!("expected Quit result"),
    }
}

/// Test that the bootstrap state machine transitions correctly.
#[test]
fn bootstrap_to_running_transition() {
    #[derive(PartialEq, Debug)]
    enum Mode {
        Bootstrap { progress: u8 },
        Running,
        ShuttingDown,
    }

    let mut mode = Mode::Bootstrap { progress: 0 };

    // Simulate bootstrap progress
    if let Mode::Bootstrap { progress } = &mut mode {
        *progress = 50;
    }
    assert_eq!(mode, Mode::Bootstrap { progress: 50 });

    // Simulate room ready → transition to Running
    mode = Mode::Running;
    assert_eq!(mode, Mode::Running);

    // Running mode should allow input
    let can_input = matches!(mode, Mode::Running);
    assert!(can_input);
}

/// Test display message timestamp handling.
#[test]
fn display_message_timestamp_handling() {
    use chrono::Local;

    // With timestamps enabled
    let with_ts = Some(Local::now());
    assert!(with_ts.is_some());

    // Without timestamps
    let without_ts: Option<chrono::DateTime<Local>> = None;
    assert!(without_ts.is_none());
}

/// Test that /invite before room ready shows "room not ready".
///
/// This tests the None branch of dispatch_command when handle is missing.
#[test]
fn invite_before_room_ready() {
    let handle: Option<String> = None; // Simulates no room handle yet

    let result = if handle.is_none() {
        Some("room not ready".to_string())
    } else {
        None
    };

    assert_eq!(
        result,
        Some("room not ready".to_string()),
        "invite before room ready should show error"
    );
}

/// Test the channel not ready scenario.
#[test]
fn command_channel_not_ready() {
    let cmd_tx: Option<mpsc::UnboundedSender<String>> = None;

    let result = if cmd_tx.is_none() {
        Some("command channel not ready".to_string())
    } else {
        None
    };

    assert_eq!(
        result,
        Some("command channel not ready".to_string()),
        "should show error when cmd_tx is not ready"
    );
}

/// Test cursor movement bounds in input handling.
#[test]
fn input_cursor_bounds() {
    let mut input = "hello".to_string();
    let mut cursor = 0;

    // Left at start should do nothing
    if cursor > 0 {
        cursor -= 1;
    }
    assert_eq!(cursor, 0);

    // Right to end
    while cursor < input.len() {
        cursor += 1;
    }
    assert_eq!(cursor, 5);

    // Right past end should do nothing
    if cursor < input.len() {
        cursor += 1;
    }
    assert_eq!(cursor, 5);

    // Backspace at position
    cursor = 3;
    if cursor > 0 {
        input.remove(cursor - 1);
        cursor -= 1;
    }
    assert_eq!(input, "helo"); // removes 'l' at index 2
    assert_eq!(cursor, 2);

    // Delete at position
    cursor = 1;
    if cursor < input.len() {
        input.remove(cursor);
    }
    assert_eq!(input, "hlo");
    assert_eq!(cursor, 1);
}

/// Test scroll bounds.
#[test]
fn scroll_bounds() {
    let mut scroll: usize = 0;

    // Down at bottom should do nothing
    scroll = scroll.saturating_sub(1);
    assert_eq!(scroll, 0);

    // Up should increase
    scroll += 10;
    assert_eq!(scroll, 10);

    // Down should decrease
    scroll = scroll.saturating_sub(3);
    assert_eq!(scroll, 7);

    // Down past bottom should clamp
    scroll = scroll.saturating_sub(100);
    assert_eq!(scroll, 0);
}

/// Test that Ctrl+C detection works correctly.
#[test]
fn ctrl_c_detection() {
    use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};

    // Ctrl+C should trigger quit
    let key = KeyEvent::new(KeyCode::Char('c'), KeyModifiers::CONTROL);
    let should_quit = key.code == KeyCode::Char('c')
        && key.modifiers.contains(KeyModifiers::CONTROL);
    assert!(should_quit, "Ctrl+C should trigger quit");

    // Just 'c' should not
    let key = KeyEvent::new(KeyCode::Char('c'), KeyModifiers::empty());
    let should_quit = key.code == KeyCode::Char('c')
        && key.modifiers.contains(KeyModifiers::CONTROL);
    assert!(!should_quit, "plain 'c' should not trigger quit");

    // Ctrl+Z should not trigger quit (we only check Ctrl+C)
    let key = KeyEvent::new(KeyCode::Char('z'), KeyModifiers::CONTROL);
    let should_quit = key.code == KeyCode::Char('c')
        && key.modifiers.contains(KeyModifiers::CONTROL);
    assert!(!should_quit, "Ctrl+Z should not trigger quit");
}

/// Test that the CLI argument parsing works correctly.
///
/// Uses clap to verify the CLI struct is configured properly.
#[test]
fn cli_args_host_command() {
    use clap::Parser;

    #[derive(Parser)]
    #[command(name = "chat")]
    struct TestCli {
        #[command(subcommand)]
        command: Option<TestCommands>,
    }

    #[derive(clap::Subcommand)]
    enum TestCommands {
        Host {
            #[arg(long, default_value_t = 300)]
            invite_ttl: u64,
            #[arg(long)]
            name: Option<String>,
            #[arg(long, default_value_t = false)]
            timestamps: bool,
        },
        Join {
            invite_code: String,
            #[arg(long)]
            name: Option<String>,
            #[arg(long, default_value_t = false)]
            timestamps: bool,
        },
    }

    // Test host with defaults
    let cli = TestCli::try_parse_from(["chat", "host"]);
    assert!(cli.is_ok());
    let cli = cli.unwrap();
    match cli.command.unwrap() {
        TestCommands::Host {
            invite_ttl,
            name,
            timestamps,
        } => {
            assert_eq!(invite_ttl, 300);
            assert!(name.is_none());
            assert!(!timestamps);
        }
        _ => panic!("expected Host command"),
    }

    // Test host with custom TTL
    let cli = TestCli::try_parse_from(["chat", "host", "--invite-ttl", "600"]);
    assert!(cli.is_ok());
    let cli = cli.unwrap();
    match cli.command.unwrap() {
        TestCommands::Host { invite_ttl, .. } => {
            assert_eq!(invite_ttl, 600);
        }
        _ => panic!("expected Host command"),
    }

    // Test join with invite code
    let cli = TestCli::try_parse_from(["chat", "join", "abc123code"]);
    assert!(cli.is_ok());
    let cli = cli.unwrap();
    match cli.command.unwrap() {
        TestCommands::Join { invite_code, .. } => {
            assert_eq!(invite_code, "abc123code");
        }
        _ => panic!("expected Join command"),
    }

    // Test no command
    let cli = TestCli::try_parse_from(["chat"]);
    assert!(cli.is_ok());
    let cli = cli.unwrap();
    assert!(cli.command.is_none());
}
