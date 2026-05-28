# Refactoring Plan: Hub

**Target:** `crates/core/src/hub.rs`
**Smell:** God method (`handle_connection`), dead code, scattered peer ops, missing `Drop`
**Goal:** Phased lifecycle, extracted `PeerRegistry`, shared wire I/O, clean API
**Risk:** Medium | **Effort:** L | **Tests:** 67 passing

## Algorithm

```
Hub::run() → loop: accept_next() until room shutdown

Per-connection (handle_connection):
  1. accept_and_handshake(stream) → (peer_id, name, stream)
  2. register_peer(peer_id, name) → insert + broadcast join
  3. spawn_peer_tasks(stream, peer_id, name) → reader (spawned) + writer (runs here)
  4. await_disconnect() → writer completes
  5. deregister_peer(peer_id, name) → remove + broadcast leave
```

## Wire I/O: Hub vs Joiner

| Aspect | Reader | Writer |
|---|---|---|
| Primitives | Identical: `read_frame` + timeout | Identical: `write_all` + timeout (+ flush) |
| Routing | Joiner: 1 output. Hub: 3 outputs (msg_tx, writer_tx, broadcast_tx) | Joiner: 1 input + control. Hub: 2 inputs (per-peer + broadcast) |
| Special | Hub: auto ping→pong | Joiner: shutdown drain + pending flush |

**Decision:** Extract `read_frame_with_timeout` / `write_frame` to `wire.rs`. Keep routing separate.

## Reference Preservation

Before any edits: copy `hub.rs` → `crates/core/src/hub.rs.bak`. Delete after verification.

## Steps

1. **Backup + shared wire I/O**
   - Copy `hub.rs` → `hub.rs.bak`
   - Extract `write_frame(stream, frame)` to `wire.rs` (from joiner's impl)
   - Hub reader/writer will use it instead of inline timeout+write_all
   - Verify: `cargo check && cargo test --lib`

2. **Extract `PeerRegistry` struct**
   - Replace `Arc<RwLock<HashMap>>` alias with proper struct
   - Methods: `register()`, `deregister()`, `snapshot()`, `broadcast_to_all()`
   - Remove dead `_broadcast_rx` from `PeerEntry`
   - Verify: `cargo check && cargo test --lib`

3. **High-level lifecycle skeleton**
   - Replace `handle_connection` body with calls to: `accept_and_handshake`, `register_peer`, `spawn_peer_tasks`, `await_disconnect`, `deregister_peer`
   - Each method exists as a stub calling old code underneath
   - Verify: `cargo check && cargo test --lib`

4. **Implement `accept_and_handshake`**
   - Extract handshake logic from old `handle_connection`
   - Returns `(PeerId, String, DataStream)`
   - Add test with duplex stream
   - Verify: `cargo check && cargo test --lib`

5. **Implement `register_peer` / `deregister_peer`**
   - Use `PeerRegistry` methods
   - Handle join/leave broadcasts
   - Add tests
   - Verify: `cargo check && cargo test --lib`

6. **Implement `spawn_peer_tasks`**
   - Split stream, spawn reader, return writer handle
   - Reader uses shared `write_frame` for pong
   - Add tests
   - Verify: `cargo check && cargo test --lib`

7. **Implement writer with shared `write_frame`**
   - Replace inline timeout+write_all with `write_frame`
   - Add explicit flush (was missing)
   - Keep 2-channel select (per-peer + broadcast)
   - Verify: `cargo check && cargo test --lib`

8. **Add `Drop` + constants**
   - `impl Drop for Hub` → calls `shutdown()`
   - Extract `"[hub]"`, `"[system]"`, `"joined"`, `"left"` to constants
   - Verify: `cargo check && cargo test --lib`

9. **Cleanup**
   - Remove `hub.rs.bak`
   - Run full test suite
   - `cargo clippy`

## Success Criteria
- [ ] All tests pass, no new clippy warnings
- [ ] `Hub` cleans up on drop
- [ ] `handle_connection` < 30 lines
- [ ] Shared `write_frame` used by both hub and joiner
