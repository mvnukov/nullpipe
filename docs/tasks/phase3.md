# Phase 3 — Core: hub connection handling

Implement the hub-side accept loop, per-connection handshake, reader/writer tasks, peer management, and nonce bookkeeping.

---

## 3.1 Per-connection spawn

- [ ] Accept loop in `HostedRoom` receives `DataStream` from onion service
- [ ] Each accepted stream spawns a dedicated async task
- [ ] Task lifecycle: handshake → reader loop → disconnect cleanup

## 3.2 Handshake

- [ ] Handshake protocol: peer sends `{nonce: [u8;16], name: String}` (length-prefixed)
- [ ] Hub validates nonce expiry against `invite_ttl_secs`
- [ ] Hub checks nonce against used-nonce `HashSet`
- [ ] Accept → send `Ack { peer_id, current_peers }`
- [ ] Reject → send `Reject { reason }` and close stream
- [ ] Map failures to `ChatError` variants (nonce reuse, expired, timeout)

## 3.3 Reader task

- [ ] Read length-prefixed UTF-8 messages from peer stream
- [ ] Parse into `ChatEvent::Message { from, name, text }`
- [ ] Forward messages to hub broadcast channel (`tokio::sync::broadcast`)
- [ ] On read error or EOF → emit disconnect event, exit reader task
- [ ] Handle partial reads and malformed messages gracefully

## 3.4 Writer task

- [ ] Subscribe to hub broadcast channel per peer
- [ ] Receive `ChatEvent` → serialize to wire format → write to stream
- [ ] Skip own messages (peer doesn't echo back their own)
- [ ] On write error → emit disconnect event, exit writer task

## 3.5 Peer list management

- [ ] `HashMap<PeerId, Sender<ChatEvent>>` behind `tokio::sync::RwLock`
- [ ] `PeerInfo { id, name, joined_at }` per connected peer
- [ ] On handshake success → insert into map, broadcast `PeerJoin`
- [ ] On disconnect → remove from map, broadcast `PeerLeave`
- [ ] `HostedRoom::peers()` returns `Vec<PeerInfo>` snapshot

## 3.6 Nonce bookkeeping

- [ ] `HashSet<[u8; 16]>` tracking used nonces
- [ ] Nonce inserted on successful handshake
- [ ] Rejected if nonce already in set → `ChatError::NonceReused`
- [ ] Periodic nonce sweep: remove expired entries (older than `invite_ttl_secs`)
- [ ] Sweep runs on a timer or triggered every N connections

## 3.7 Hub self-messaging

- [ ] Hub can send system messages (peer join/leave notifications)
- [ ] Hub can broadcast its own messages as a sender
- [ ] Self-messages fan-out to all connected peers

---

## Verification

### Compilation checks
- [ ] `cargo check --package ephemeral-chat-core` — zero errors
- [ ] `cargo check --workspace` — cli still compiles
- [ ] `cargo clippy --package ephemeral-chat-core -- -D warnings` — zero lints
- [ ] All new public types exported from crate root

### API surface
- [ ] `HostedRoom::peers() -> Vec<PeerInfo>` returns snapshot
- [ ] `HostedRoom::send(text: &str) -> Result<()>` broadcasts to all peers
- [ ] Handshake types (`Ack`, `Reject`) are internal (not pub)
- [ ] Wire format functions are `pub(crate)` or private

### Handshake behavior
- [ ] Peer sends valid nonce + name → receives `Ack`
- [ ] Peer sends expired nonce → receives `Reject`, stream closed
- [ ] Peer sends reused nonce → receives `Reject`, stream closed
- [ ] Peer sends malformed handshake → receives `Reject`, stream closed
- [ ] Handshake timeout (no data within N seconds) → stream closed

### Peer lifecycle
- [ ] New peer → `PeerJoin` broadcast to all existing peers
- [ ] Peer disconnects → `PeerLeave` broadcast to remaining peers
- [ ] Peer list is consistent after rapid join/leave cycles
- [ ] `HostedRoom::peers()` reflects current state at call time

### Message broadcast
- [ ] Message from peer A → received by peers B and C, not echoed to A
- [ ] Hub sends message → received by all connected peers
- [ ] Broadcast channel handles backpressure (slow reader doesn't block others)
- [ ] Channel capacity is bounded; slow peers are dropped, not blocked

### Nonce bookkeeping
- [ ] Same nonce used twice → second connection rejected
- [ ] Nonce sweep removes expired entries after TTL
- [ ] Nonce set doesn't grow unbounded over time

### Integration (e2e tests)
- [ ] Hub + 2 joiners: all three can exchange messages
- [ ] Joiner disconnects → hub broadcasts leave event to remaining joiner
- [ ] Expired invite → joiner handshake fails with appropriate error
- [ ] Nonce replay → second joiner with same nonce is rejected

### Formatting checks
- [ ] `cargo fmt -- --check` — passes

### Build integrity
- [ ] `cargo clean && cargo build --package ephemeral-chat-core` — succeeds
- [ ] `cargo clean && cargo build --workspace` — succeeds

---

## Acceptance criteria

- [ ] All verification checks above pass
- [ ] `cargo clippy --package ephemeral-chat-core -- -D warnings` — clean
- [ ] Hub accepts connections, validates handshakes, manages peer list
- [ ] Messages broadcast from any peer reach all other peers
- [ ] Nonce reuse is detected and rejected
- [ ] Disconnects are handled gracefully with no resource leaks
- [ ] No panics in any code path
