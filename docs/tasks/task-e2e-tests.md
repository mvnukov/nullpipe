# Task: Create proper e2e tests

**Priority:** High  
**Status:** Not started

## ⚠️ CRITICAL: TOR NETWORK IS AVAILABLE. RUN ALL E2E TESTS.

The Tor network IS available in this environment. Arti bootstraps against real Tor directory authorities. ALL e2e and integration tests MUST be executed. DO NOT skip, ignore, or mock any test. DO NOT add `#[ignore]`. DO NOT claim "Tor unavailable".

## Problem

The app has no end-to-end tests. Manual testing revealed it "doesn't work at all" — at minimum the `/invite` crash (see `bug-block-on-runtime.md`), but likely more issues in the host↔join flow, invite code exchange, message passing, and peer join/leave.

## Goal

Automated e2e tests that verify the full user flow:

1. **Host starts a room** — Tor bootstraps, onion service starts, `RoomReady` event fires
2. **Host generates invite code** — `/invite` succeeds, code is valid base58
3. **Joiner uses invite code** — connects to host's onion service, `PeerJoin` event fires on both sides
4. **Messages flow** — host→joiner and joiner→host, both receive each other's messages
5. **Peer leave** — when one side quits, the other gets `PeerLeave`
6. **Room close** — host closes room, joiner gets `RoomClosed`

## Approach

**Do NOT mock Tor.** The app uses Arti (`arti-client`) — a pure-Rust Tor implementation built in. There is no external `tor` binary, no `torrc`, no SOCKS proxy. All tests use the real `TorBootstrap` → `arti_client` flow.

### Why the existing `#[ignore]` test existed (NOW REMOVED)

The `e2e_joiner_connects_to_host_and_transfers_data` test was previously marked `#[ignore]`. **This has been removed.** All tests now run. Tor IS available. Execute everything.

### Bootstrap timing

Arti bootstrap takes ~30-90s on first run (downloads consensus, builds circuits). This is normal. Do not "optimize" it away with mocks.

### Test structure

- Use `tokio::test` with real `TorBootstrap::bootstrap()` (Arti)
- Spawn host and joiner in separate tasks
- Wire their `ChatEvent` channels to assertions
- Tests need normal internet access (Arti contacts Tor directory authorities on port 9001/9030)
- No external software, binaries, or configuration required

### Edge cases

- Invalid invite code
- Expired TTL
- Double-join with same nonce (`NonceReused`)

## Acceptance Criteria

- `cargo test --workspace` includes e2e tests that pass locally
- `cargo test --workspace` runs ALL e2e and integration tests on real Tor — they pass
- All six flows above covered
- Tests must be written so they **demonstrate the bug from `bug-block-on-runtime.md`** — i.e., they should call `/invite`, `/peers`, `/quit` and fail with the `block_on` panic before the fix, then pass after
