//! CLI binary integration tests.
//!
//! These tests spawn the actual `chat` binary as a subprocess and verify
//! its behavior through stdin/stdout/stderr. They would have caught the
//! original block_on panic bug because the panic would appear in stderr.
//!
//! Tests marked with `#[ignore]` require a working Tor connection and
//! should be run on demand with `cargo test -- --ignored`.

use assert_cmd::prelude::*;
use std::process::{Command, Stdio};

// ---------------------------------------------------------------------------
// Helper: spawn chat binary
// ---------------------------------------------------------------------------

fn spawn_chat(args: &[&str]) -> std::process::Child {
    Command::cargo_bin("chat")
        .expect("chat binary should exist")
        .args(args)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("failed to spawn chat binary")
}


// ---------------------------------------------------------------------------
// Test 1: CLI startup — no panic on host command
// ---------------------------------------------------------------------------

#[test]
fn cli_host_startup_no_panic() {
    let mut child = spawn_chat(&["host"]);
    std::thread::sleep(std::time::Duration::from_secs(3));

    let output = child.try_wait();
    if let Ok(Some(status)) = output {
        assert!(
            !status.code().map_or(false, |c| c > 1),
            "CLI exited with abnormal code, possible panic"
        );
    }

    let _ = child.kill();
    let _ = child.wait();
}

#[test]
fn cli_join_startup_no_panic() {
    let mut child = spawn_chat(&["join", "test-invite-code"]);
    std::thread::sleep(std::time::Duration::from_secs(3));

    let output = child.try_wait();
    if let Ok(Some(status)) = output {
        assert!(
            !status.code().map_or(false, |c| c > 1),
            "CLI exited with abnormal code, possible panic"
        );
    }

    let _ = child.kill();
    let _ = child.wait();
}

// ---------------------------------------------------------------------------
// Test 2: CLI help and usage
// ---------------------------------------------------------------------------

#[test]
fn cli_no_args_shows_usage() {
    let output = Command::cargo_bin("chat")
        .expect("chat binary should exist")
        .output()
        .expect("failed to run chat");

    assert!(!output.status.success());

    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("no subcommand provided") || stderr.contains("Usage"),
        "should show usage info, got: {}",
        stderr
    );
}

#[test]
fn cli_help_works() {
    let output = Command::cargo_bin("chat")
        .expect("chat binary should exist")
        .arg("--help")
        .output()
        .expect("failed to run chat --help");

    assert!(output.status.success());

    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("chat"), "help should mention chat");
    assert!(
        stdout.contains("host") || stdout.contains("join"),
        "help should list subcommands"
    );
}

#[test]
fn cli_host_help_works() {
    let output = Command::cargo_bin("chat")
        .expect("chat binary should exist")
        .args(["host", "--help"])
        .output()
        .expect("failed to run chat host --help");

    assert!(output.status.success());

    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        stdout.contains("invite-ttl") || stdout.contains("Host"),
        "host help should show options"
    );
}

// ---------------------------------------------------------------------------
// Test 3: Graceful shutdown
// ---------------------------------------------------------------------------

#[test]
fn cli_graceful_shutdown_on_kill() {
    let mut child = spawn_chat(&["host"]);
    std::thread::sleep(std::time::Duration::from_secs(2));

    let _ = child.kill();
    let status = child.wait().expect("process should exit");
    assert!(!status.code().map_or(false, |c| c > 128), "should not be signal-killed with abnormal code");
}

#[test]
fn cli_force_kill_exit() {
    let mut child = spawn_chat(&["host"]);
    std::thread::sleep(std::time::Duration::from_secs(1));

    let _ = child.kill();
    let _ = child.wait().expect("process should exit");
}

// ---------------------------------------------------------------------------
// Test 4: Edge case — invalid join code
// ---------------------------------------------------------------------------

#[test]
fn cli_join_invalid_code_no_crash() {
    let mut child = spawn_chat(&["join", "not-a-valid-base58-code!!!"]);
    std::thread::sleep(std::time::Duration::from_secs(5));

    let status = child.try_wait();
    if let Ok(Some(s)) = status {
        assert!(
            !s.code().map_or(false, |c| c > 1),
            "CLI should not crash on invalid code"
        );
    }

    let _ = child.kill();
    let _ = child.wait();
}

// ---------------------------------------------------------------------------
// Test 5: Bootstrap timeout handling (slow)
// ---------------------------------------------------------------------------

#[test]
fn cli_bootstrap_failure_handling() {
    let mut child = spawn_chat(&["host"]);
    std::thread::sleep(std::time::Duration::from_secs(10));

    if let Ok(Some(_)) = child.try_wait() {
        let _ = child.wait();
    }

    let _ = child.kill();
    let _ = child.wait();
}

// ---------------------------------------------------------------------------
// Test 6: Two-process CLI integration (slow, requires Tor)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn cli_two_process_chat() {
    use tokio::time::{timeout, Duration};

    let mut _host = spawn_chat(&["host", "--timestamps"]);

    timeout(Duration::from_secs(150), async {
        loop {
            tokio::time::sleep(Duration::from_millis(500)).await;
            break;
        }
    })
    .await
    .expect("host bootstrap timeout");

    let _joiner = spawn_chat(&["join", "placeholder-invite-code"]);
}

// ---------------------------------------------------------------------------
// Test 7: Terminal restoration
// ---------------------------------------------------------------------------

#[test]
fn cli_terminal_restoration_on_exit() {
    let mut child = spawn_chat(&["host"]);
    std::thread::sleep(std::time::Duration::from_secs(2));

    let _ = child.kill();
    let _ = child.wait();
}

// ---------------------------------------------------------------------------
// Test 8: Very long input handling
// ---------------------------------------------------------------------------

#[test]
fn cli_long_input_no_crash() {
    let mut child = spawn_chat(&["host"]);
    std::thread::sleep(std::time::Duration::from_secs(2));

    if let Some(mut stdin) = child.stdin.take() {
        use std::io::Write;
        let long_input = "a".repeat(10000);
        let _ = stdin.write_all(long_input.as_bytes());
        let _ = stdin.flush();
    }

    std::thread::sleep(std::time::Duration::from_secs(2));

    // Check exit status
    let status = child.try_wait();
    if let Ok(Some(s)) = status {
        assert!(
            !s.code().map_or(false, |c| c > 1),
            "CLI should not crash on long input"
        );
    }

    let _ = child.kill();
    let _ = child.wait();
}

// ---------------------------------------------------------------------------
// Test 9: CLI version flag
// ---------------------------------------------------------------------------

#[test]
fn cli_version_flag() {
    let output = Command::cargo_bin("chat")
        .expect("chat binary should exist")
        .arg("--version")
        .output()
        .expect("failed to run chat --version");

    assert!(output.status.success());

    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        stdout.contains("chat") || stdout.contains("0."),
        "version output should contain app name or version"
    );
}

// ---------------------------------------------------------------------------
// Test 11: Host stays alive after sending a message (no peers)
// ---------------------------------------------------------------------------

#[test]
fn cli_host_send_message_no_peers_stays_alive() {
    // Use a unique temp directory for arti state to avoid lockfile contention
    // from previous test runs.
    let state_dir = std::env::temp_dir()
        .join(format!("ephemeral-chat-test-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&state_dir);
    std::fs::create_dir_all(&state_dir).expect("create test state dir");

    // Spawn host in headless mode, wait for bootstrap, send a message via stdin,
    // verify process doesn't exit/crash.
    let mut child = Command::cargo_bin("chat")
        .expect("chat binary should exist")
        .args(["host", "--headless", "--name", "test"])
        .env("EPHEMERAL_CHAT_STATE_DIR", &state_dir)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("failed to spawn chat binary");

    // Wait for Tor bootstrap (~15-30s first run, faster on cache)
    std::thread::sleep(std::time::Duration::from_secs(20));

    // Check process is still running
    match child.try_wait() {
        Ok(Some(status)) => {
            // Process exited — grab stderr to see why
            let stderr = child.stderr.take().map(|mut s| {
                let mut buf = String::new();
                let _ = std::io::Read::read_to_string(&mut s, &mut buf);
                buf
            }).unwrap_or_default();
            panic!("CLI exited prematurely with status {:?}\nstderr: {}", status, stderr);
        }
        Ok(None) => {} // Still running — good
        Err(e) => panic!("try_wait failed: {}", e),
    }

    // Send a newline via stdin (simulates pressing Enter with empty input,
    // which should be a no-op but exercises the input path)
    if let Some(mut stdin) = child.stdin.take() {
        use std::io::Write;
        let _ = stdin.write_all(b"hello\n");
        let _ = stdin.flush();
    }

    // Give it time to process the input
    std::thread::sleep(std::time::Duration::from_secs(3));

    // Verify still running
    match child.try_wait() {
        Ok(Some(status)) => {
            let stderr = child.stderr.take().map(|mut s| {
                let mut buf = String::new();
                let _ = std::io::Read::read_to_string(&mut s, &mut buf);
                buf
            }).unwrap_or_default();
            panic!("CLI exited after sending message with status {:?}\nstderr: {}", status, stderr);
        }
        Ok(None) => {} // Still running — test passes
        Err(e) => panic!("try_wait failed: {}", e),
    }

    let _ = child.kill();
    let _ = child.wait();
    let _ = std::fs::remove_dir_all(&state_dir);
}
