# Phase 4 — Core: message broadcast

Implement the hub broadcast channel, wire protocol, joiner-side messaging, and robust stream handling.

---

## 4.1 Hub broadcast channel

- [x] `tokio::sync::broadcast::Receiver<ChatEvent>` for each connected peer
- [x] Hub receives from any peer → fan-out to all other peers via broadcast
- [x] Hub can send messages directly (self as sender)
- [x] Bounded channel with overflow strategy (drop oldest, not block)

## 4.2 Wire protocol

- [x] Define length-prefixed UTF-8 wire format
- [x] Header: 4-byte big-endian `u32` payload length
- [x] Payload: UTF-8 encoded JSON or delimited text
- [x] Message types: `Chat`, `System`, `Ping`, `Pong`
- [x] `encode_message()` / `decode_message()` helpers
- [x] Max message size enforcement (reject oversized payloads)

## 4.3 Joiner messaging

- [x] `Joiner::send(text: &str) -> Result<()>` — writes length-prefixed message
- [x] `Joiner::recv() -> impl Stream<Item = Result<ChatEvent>>` — reads incoming messages
- [x] Joiner sends messages upstream to hub via connected stream
- [x] Joiner receives broadcast messages from hub via same stream
- [x] Joiner handles reconnect scenario (stream closed, need new connection)

## 4.4 Stream robustness

- [x] Handle partial reads (incomplete length header or payload)
- [x] Handle connection drops mid-message
- [x] Graceful EOF handling (peer disconnect = clean leave)
- [x] Read timeout to detect dead peers
- [x] Write timeout to avoid blocking on slow/dead connections

---

## Verification

### Compilation checks
- [x] `cargo check --package ephemeral-chat-core` — zero errors
- [x] `cargo check --workspace` — cli still compiles
- [x] `cargo clippy --package ephemeral-chat-core -- -D warnings` — zero lints

### API surface
- [x] `Joiner::send(text: &str) -> Result<()>` — pub method
- [x] `Joiner::recv()` returns stream of `Result<ChatEvent>` — pub method
- [x] Wire format helpers are `pub(crate)` or private
- [x] Broadcast channel type is internal to core crate

### Wire protocol
- [x] Length-prefixed encoding round-trips correctly
- [x] 4-byte big-endian header matches payload length
- [x] Oversized messages (>16KB) are rejected with error
- [x] Partial reads are handled without data corruption
- [x] Malformed messages (bad UTF-8, invalid header) → error, not panic

### Broadcast behavior
- [x] Message from peer A → received by B and C
- [x] Hub sends message → received by all peers
- [x] Slow peer doesn't block fast peers (bounded channel, drop overflow)
- [x] Broadcast channel closed → reader task exits cleanly

### Joiner messaging
- [x] `Joiner::send()` transmits message to hub
- [x] `Joiner::recv()` yields `ChatEvent::Message` from hub
- [x] Joiner can send and receive simultaneously (bidirectional)
- [x] Stream closed by hub → `Joiner::recv()` returns None/Closed

### Stream robustness
- [x] Connection drop mid-read → clean error, no panic
- [x] Read timeout → peer disconnected, cleanup triggered
- [x] Write timeout → peer considered dead, cleanup triggered
- [x] Multiple simultaneous read/write operations don't deadlock

### Integration (e2e tests)
- [ ] Hub + 2 joiners: message exchange works bidirectionally
- [ ] Hub + 3 joiners: fan-out reaches all recipients
- [ ] Large message (10KB) transmits correctly
- [ ] Unicode messages (emoji, CJK) round-trip correctly
- [ ] Rapid message burst (100 msgs/sec) doesn't drop or corrupt

### Formatting checks
- [x] `cargo fmt -- --check` — passes

### Build integrity
- [x] `cargo clean && cargo build --package ephemeral-chat-core` — succeeds
- [x] `cargo clean && cargo build --workspace` — succeeds

---

## Acceptance criteria

- [x] All verification checks above pass
- [x] `cargo clippy --package ephemeral-chat-core -- -D warnings` — clean
- [x] Wire protocol is length-prefixed, handles partial reads gracefully
- [x] Hub broadcast reaches all connected peers
- [x] Joiner can send and receive messages bidirectionally
- [x] Connection drops, timeouts, and errors handled without panics
- [x] No message corruption or data loss under normal operation
