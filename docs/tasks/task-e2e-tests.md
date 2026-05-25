# Task: Create proper e2e tests

**Priority:** High  
**Status:** Not started

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

- Use `tokio::test` with a real or mocked Tor backend
- Spawn host and joiner in separate tasks/threads
- Wire their `ChatEvent` channels to assertions
- Consider a "mock Tor" or localhost-only mode for CI (real Tor bootstrap is slow)
- Cover edge cases: invalid invite code, expired TTL, double-join with same code

## Acceptance Criteria

- `cargo test --workspace` includes e2e tests that pass locally
- Tests run in CI (with or without real Tor)
- All six flows above covered
- Tests must be written so they **demonstrate the bug from `bug-block-on-runtime.md`** — i.e., they should call `/invite`, `/peers`, `/quit` and fail with the `block_on` panic before the fix, then pass after
