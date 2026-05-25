# Phase 4 — Core: message broadcast

Implement the hub broadcast channel, wire protocol, joiner-side messaging, and robust stream handling.

---

## 4.1 Hub broadcast channel

- [ ] `tokio::sync::broadcast::Receiver<ChatEvent>` for each connected peer
- [ ] Hub receives from any peer → fan-out to all other peers via broadcast
- [ ] Hub can send messages directly (self as sender)
- [ ] Bounded channel with overflow strategy (drop oldest, not block)

## 4.2 Wire protocol

- [ ] Define length-prefixed UTF-8 wire format
- [ ] Header: 4-byte big-endian `u32` payload length
- [ ] Payload: UTF-8 encoded JSON or delimited text
- [ ] Message types: `Chat`, `System`, `Ping`, `Pong`
- [ ] `encode_message()` / `decode_message()` helpers
- [ ] Max message size enforcement (reject oversized payloads)

## 4.3 Joiner messaging

- [ ] `Joiner::send(text: &str) -> Result<()>` — writes length-prefixed message
- [ ] `Joiner::recv() -> impl Stream<Item = ChatEvent>` — reads incoming messages
- [ ] Joiner sends messages upstream to hub via connected stream
- [ ] Joiner receives broadcast messages from hub via same stream
- [ ] Joiner handles reconnect scenario (stream closed, need new connection)

## 4.4 Stream robustness

- [ ] Handle partial reads (incomplete length header or payload)
- [ ] Handle connection drops mid-message
- [ ] Graceful EOF handling (peer disconnect = clean leave)
- [ ] Read timeout to detect dead peers
- [ ] Write timeout to avoid blocking on slow/dead connections

---

## Verification

### Compilation checks
- [ ] `cargo check --package ephemeral-chat-core` — zero errors
- [ ] `cargo check --workspace` — cli still compiles
- [ ] `cargo clippy --package ephemeral-chat-core -- -D warnings` — zero lints

### API surface
- [ ] `Joiner::send(text: &str) -> Result<()>` — pub method
- [ ] `Joiner::recv()` returns stream of `ChatEvent` — pub method
- [ ] Wire format helpers are `pub(crate)` or private
- [ ] Broadcast channel type is internal to core crate

### Wire protocol
- [ ] Length-prefixed encoding round-trips correctly
- [ ] 4-byte big-endian header matches payload length
- [ ] Oversized messages (>16KB) are rejected with error
- [ ] Partial reads are handled without data corruption
- [ ] Malformed messages (bad UTF-8, invalid header) → error, not panic

### Broadcast behavior
- [ ] Message from peer A → received by B and C
- [ ] Hub sends message → received by all peers
- [ ] Slow peer doesn't block fast peers (bounded channel, drop overflow)
- [ ] Broadcast channel closed → reader task exits cleanly

### Joiner messaging
- [ ] `Joiner::send()` transmits message to hub
- [ ] `Joiner::recv()` yields `ChatEvent::Message` from hub
- [ ] Joiner can send and receive simultaneously (bidirectional)
- [ ] Stream closed by hub → `Joiner::recv()` returns None/Closed

### Stream robustness
- [ ] Connection drop mid-read → clean error, no panic
- [ ] Read timeout → peer disconnected, cleanup triggered
- [ ] Write timeout → peer considered dead, cleanup triggered
- [ ] Multiple simultaneous read/write operations don't deadlock

### Integration (e2e tests)
- [ ] Hub + 2 joiners: message exchange works bidirectionally
- [ ] Hub + 3 joiners: fan-out reaches all recipients
- [ ] Large message (10KB) transmits correctly
- [ ] Unicode messages (emoji, CJK) round-trip correctly
- [ ] Rapid message burst (100 msgs/sec) doesn't drop or corrupt

### Formatting checks
- [ ] `cargo fmt -- --check` — passes

### Build integrity
- [ ] `cargo clean && cargo build --package ephemeral-chat-core` — succeeds
- [ ] `cargo clean && cargo build --workspace` — succeeds

---

## Acceptance criteria

- [ ] All verification checks above pass
- [ ] `cargo clippy --package ephemeral-chat-core -- -D warnings` — clean
- [ ] Wire protocol is length-prefixed, handles partial reads gracefully
- [ ] Hub broadcast reaches all connected peers
- [ ] Joiner can send and receive messages bidirectionally
- [ ] Connection drops, timeouts, and errors handled without panics
- [ ] No message corruption or data loss under normal operation
