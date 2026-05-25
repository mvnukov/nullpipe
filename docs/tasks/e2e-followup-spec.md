# E2E Test Follow-Up: What's Missing and How to Test It

## Current State

Existing tests (`crates/core/tests/e2e.rs` and `integration.rs`) cover the **library API** level. They use `host()`/`join()` directly with `.await` from `#[tokio::test]`. This validates the core chat protocol works correctly over real Tor.

## Critical Gap: CLI Not Tested

The e2e tests **never exercise the CLI binary** (`crates/cli/src/main.rs`). They call `handle.invite().await` directly — the library's async path — which was never broken. The bug was in the CLI's `dispatch_command()` using `Handle::current().block_on()` from a sync event handler.

Tests must cover two layers:

| Layer | What it tests | Covered? |
|-------|--------------|----------|
| Library API | `host()`/`join()`/`invite()`/`peers()`/`quit()` async methods | ✅ e2e.rs |
| CLI binary | Full process lifecycle, TUI event loop, slash command dispatch, terminal rendering | ❌ nothing |

## What Needs to Be Tested

### 1. CLI Binary Integration Test

**Goal**: Spawn `cargo run --bin chat -- host` as a subprocess, interact via stdin/stdout, verify no panics.

**Why**: The only way to exercise the actual bug path — `dispatch_command()` called from sync TUI event handler inside `#[tokio::main]`.

**Test approach**:
```rust
use std::process::{Command, Stdio};

#[tokio::test]
async fn cli_no_block_on_panic() {
    // Start CLI as subprocess
    let mut child = Command::new("cargo")
        .args(["run", "--bin", "chat", "--", "host"])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("failed to start CLI");
    
    // Send /invite command
    // Wait for output
    // Verify no panic in stderr
    // Clean up
}
```

**Test cases**:
1. `/invite` — should produce invite code, not panic
2. `/peers` — should show "no peers" (or peer list if connected)
3. `/quit` — should gracefully shutdown, not hang
4. Unknown command — should show "unknown command" message
5. `/invite` before room ready — should show "room not ready"
6. Ctrl-C during bootstrap — should restore terminal, not leave it broken

### 2. Two-Process CLI Integration Test

**Goal**: Run two CLI processes (host + joiner) and verify they can actually chat through each other.

**Why**: Tests the full user experience, including TUI state machine, event handling, and cross-process communication.

**Test approach**:
1. Start host process
2. Wait for "room ready" output + extract onion address
3. Start joiner process with invite code
4. Verify both show peer join messages
5. Send message from joiner → verify host receives it
6. Send message from host → verify joiner receives it
7. Send `/quit` from joiner → verify host shows peer leave
8. Send `/quit` from host → verify clean shutdown

### 3. Terminal Restoration Test

**Goal**: Verify terminal state is properly restored on exit/crash.

**Why**: The bug causes panic; if terminal isn't restored, user gets broken terminal.

**Test approach**:
1. Start CLI, verify raw mode enabled
2. Trigger `/quit` — verify raw mode disabled, alternate screen exited
3. Start CLI, trigger panic (e.g., malformed input if possible) — verify terminal restored via panic hook

### 4. Edge Cases in CLI

- Empty input handling (Enter with no text)
- Very long invite codes in input buffer
- Rapid successive commands (`/invite`, `/peers`, `/quit` in quick succession)
- Command during shutdown state (input locked)
- Bootstrap timeout handling

## Implementation Details

### Test Location
`crates/cli/tests/cli_e2e.rs` — new file for CLI-specific e2e tests

### Dependencies
- `assert_cmd` — subprocess testing
- `predicates` — output matching
- `tempfile` — temporary config files
- Existing `ephemeral_chat_core` for invite code generation/validation

### Alternative: Unit Tests for CLI Logic

If spawning full CLI processes is too slow/flaky, add targeted unit tests:

```rust
#[test]
fn dispatch_command_spawn_not_block_on() {
    // Verify dispatch_command uses tokio::spawn, not block_on
    // Test with mock cmd_tx channel
    // Verify CmdResult is sent correctly
}
```

### CI Considerations
- CLI tests are faster than library e2e (no Tor bootstrap for unit tests)
- Full CLI integration tests still need Tor network
- Consider `#[ignore]` for slow integration tests, run on demand
- Unit tests for CLI logic should run on every CI

## Success Criteria

1. CLI binary tests exist and pass locally
2. Tests would have caught the `block_on` bug (fail before fix, pass after)
3. Terminal restoration verified
4. Two-process chat flow tested
5. Edge cases covered (empty input, rapid commands, shutdown state)
6. Tests documented in `docs/tasks/task-e2e-tests.md`

## Priority Order

1. **CLI unit tests** for `dispatch_command` logic (fast, no Tor needed)
2. **CLI subprocess test** for `/invite` no-panic (catches regression)
3. **Two-process integration test** (full user flow)
4. **Terminal restoration tests** (critical for UX)
5. **Edge cases** (nice to have)
