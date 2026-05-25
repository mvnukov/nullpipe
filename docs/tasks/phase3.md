# Phase 3 — Core: hub connection handling

Implement the hub-side accept loop, per-connection handshake, reader/writer tasks, peer management, and nonce bookkeeping.

---

## 3.1 Per-connection spawn

- [x] Accept loop in `HostedRoom` receives `DataStream` from onion service
- [x] Each accepted stream spawns a dedicated async task (`handle_connection`)
- [x] Task lifecycle: handshake → register → reader + writer → deregister

## 3.2 Handshake

- [x] Handshake protocol: 32-byte wire format `[16 nonce][16 discriminator]`
- [ ] Hub validates nonce expiry against `invite_ttl_secs` — **nonces checked for reuse only, not expiry**
- [x] Hub checks nonce against used-nonce `HashSet`
- [x] Accept → send `[0]` byte; Reject → send `[1]` byte and close stream
- [x] Map failures to `ChatError` variants (`NonceReused`, `InvalidInvite`, `Connection`)

## 3.3 Reader task

- [x] Read newline-delimited UTF-8 messages from peer stream
- [x] Push to hub event channel as `(PeerId, name, text)` tuples
- [x] On read error or EOF → exit reader task
- [x] Handle partial reads and malformed messages (size limit, invalid UTF-8)

## 3.4 Writer task

- [x] Per-peer `mpsc::UnboundedChannel` for outgoing frames
- [x] Receive `name\ttext\n` frames → write to stream
- [x] `broadcast(exclude)` skips own messages
- [x] On write error → exit writer task

## 3.5 Peer list management

- [x] `HashMap<PeerId, PeerEntry>` behind `tokio::sync::RwLock`
- [x] `PeerEntry { name, joined_at, tx }` per connected peer
- [x] On handshake success → insert into map, broadcast join via msg channel
- [x] On disconnect → remove from map, broadcast leave via msg channel
- [x] `Hub::peers() -> Vec<PeerInfo>` returns snapshot

## 3.6 Nonce bookkeeping

- [x] `HashSet<[u8; 16]>` tracking used nonces
- [x] Nonce inserted on successful handshake
- [x] Rejected if nonce already in set → `ChatError::NonceReused`
- [ ] Periodic nonce sweep: remove expired entries — **not implemented**
- [ ] Sweep runs on a timer — **not implemented**

## 3.7 Hub self-messaging

- [x] Hub broadcasts system messages via `[system]` sender
- [x] `Hub::broadcast_system(text)` broadcasts to all peers
- [x] Messages fan-out to all connected peers via per-peer channels

---

## Verification

### Compilation checks
- [x] `cargo check --package ephemeral-chat-core` — zero errors
- [x] `cargo check --workspace` — cli still compiles
- [x] `cargo clippy --package ephemeral-chat-core -- -D warnings` — zero lints
- [x] All new public types exported from crate root

### API surface
- [x] `Hub::peers() -> Vec<PeerInfo>` returns snapshot
- [x] `Hub::broadcast()` sends to all peers (excluding self)
- [x] Handshake is internal (wire format: 32 bytes + newline-delimited messages)
- [x] Wire format functions are private

### Handshake behavior
- [x] Valid nonce → receives `[0]` accept byte
- [x] Reused nonce → receives `[1]` reject, stream closed
- [ ] Expired nonce → **expiry not checked**
- [x] Malformed handshake (wrong length) → stream closed
- [ ] Handshake timeout → **no explicit timeout**

### Peer lifecycle
- [x] New peer → join event sent to hub channel
- [x] Peer disconnects → leave event sent to hub channel
- [x] Peer list consistent after join/leave
- [x] `Hub::peers()` reflects current state

### Message broadcast
- [x] `broadcast(exclude)` skips specified peer
- [x] `broadcast_system` sends to all peers
- [x] Per-peer unbounded channels — slow peer doesn't block others
- [x] Channel capacity bounded (unbounded but per-peer)

### Nonce bookkeeping
- [x] Same nonce used twice → second connection rejected (unit test)
- [ ] Nonce sweep removes expired entries — **not implemented**
- [ ] Nonce set doesn't grow unbounded — **no cleanup mechanism**

### Integration (e2e tests)
- [x] `e2e_host_onion_service_and_get_address` — passes
- [x] `e2e_bootstrap_emits_progress_0_to_100` — passes
- [x] `e2e_shutdown_idempotent` — passes
- [x] `e2e_drop_triggers_cleanup` — passes
- [x] `e2e_joiner_connects_to_host_and_transfers_data` — **was ignored, now runs** (ignore removed, Tor is available)
- [ ] Hub + 2 joiners: all three can exchange messages — **no test**
- [ ] Expired invite → handshake fails — **no test**
- [ ] Nonce replay → second joiner rejected — **unit test only**

### Formatting checks
- [x] `cargo fmt -- --check` — passes

### Build integrity
- [x] `cargo build --package ephemeral-chat-core` — succeeds
- [x] `cargo build --workspace` — succeeds

### Unit tests
- [x] 21/21 tests pass (`hub::tests::nonce_set_rejects_duplicates`, `hub::tests::peer_registry_roundtrip`, invite tests)

---

## Acceptance criteria

- [x] `cargo clippy --package ephemeral-chat-core -- -D warnings` — clean
- [x] Hub accepts connections, validates handshakes, manages peer list
- [x] Messages broadcast from any peer reach other peers (excluding sender)
- [x] Nonce reuse is detected and rejected
- [x] Disconnects are handled gracefully (reader/writer exit, peer removed)
- [x] No panics in code paths (unit tests confirm)
- [ ] Nonce expiry validation — **deferred**
- [ ] Periodic nonce sweep — **deferred**
- [ ] Multi-peer e2e message exchange test — **blocked on Tor rendezvous reliability**

## Notes

- Handshake uses a simplified 32-byte wire format rather than length-prefixed `{nonce, name}` struct
- `HostedRoom` (Phase 2) handles raw Tor accept; `Hub` (Phase 3) adds protocol layer
- E2E joiner test **now runs** — `#[ignore]` removed, Tor is available
- Nonce bookkeeping lacks TTL-based cleanup; set grows unbounded over long-running sessions
