# Phase 5 — Core: RoomHandle and EventStream

Implement the unified public API: `RoomHandle` for sending/controlling a room and `EventStream` for receiving events.

---

## 5.1 RoomHandle struct

- [ ] `RoomHandle::send(&self, text: &str) -> Result<()>` — send message to room
- [ ] `RoomHandle::invite(&self) -> Result<String>` — generate new invite code (hub only)
- [ ] `RoomHandle::peers(&self) -> Vec<PeerInfo>` — snapshot of connected peers
- [ ] `RoomHandle::quit(&self) -> impl Future` — graceful shutdown
- [ ] Handle is `Clone`-able, safe to share across tasks
- [ ] Hub-only methods (`invite`) return error when called by joiner

## 5.2 EventStream

- [ ] Unified `EventStream = tokio::sync::mpsc::Receiver<ChatEvent>`
- [ ] Single stream for all event types: messages, peer join/leave, bootstrap, errors
- [ ] Stream ends with `None` on room shutdown
- [ ] No channel lag: sender uses `try_send` or bounded with overflow policy

## 5.3 Entry points

- [ ] `pub fn host(config: HostConfig) -> (RoomHandle, EventStream)` — hub entry
- [ ] `pub fn join(config: JoinConfig) -> (RoomHandle, EventStream)` — joiner entry
- [ ] Both spawn all background tasks internally (bootstrap, accept loop, broadcast)
- [ ] Both return immediately; events arrive via `EventStream`
- [ ] Background tasks stop when `RoomHandle::quit()` is called or `EventStream` dropped

## 5.4 Clean shutdown

- [ ] `quit()` stops all tasks: reader, writer, accept loop, broadcast
- [ ] `quit()` closes onion service (hub) or stream (joiner)
- [ ] `quit()` tears down `TorBootstrap`
- [ ] Terminal shutdown: all resources released, no leaks
- [ ] Dropping `RoomHandle` without `quit()` triggers cleanup via `Drop`
- [ ] Dropping `EventStream` triggers room shutdown (backpressure signal)

---

## Verification

### Compilation checks
- [ ] `cargo check --package ephemeral-chat-core` — zero errors
- [ ] `cargo check --workspace` — cli still compiles
- [ ] `cargo clippy --package ephemeral-chat-core -- -D warnings` — zero lints

### API surface
- [ ] `host()` and `join()` are `pub` at crate root
- [ ] `RoomHandle` is `Clone`, `Send`, `Sync`
- [ ] `EventStream` is a public type alias or newtype
- [ ] `ChatEvent` enum is `pub` with all variants documented
- [ ] `HostConfig` and `JoinConfig` are `pub` with `pub` fields

### RoomHandle behavior
- [ ] `send()` transmits message to all peers
- [ ] `send()` returns error if room is shut down
- [ ] `invite()` returns valid base58 token (hub only)
- [ ] `invite()` returns error when called by joiner
- [ ] `peers()` returns current peer list (eventually consistent)
- [ ] `quit()` completes without hanging

### EventStream behavior
- [ ] Bootstrap progress events arrive during startup
- [ ] `RoomReady` event arrives when hub is listening
- [ ] `Message` events arrive for all peer messages
- [ ] `PeerJoin` / `PeerLeave` events fire correctly
- [ ] Stream ends (`None`) after `quit()` or room shutdown
- [ ] Events arrive in causal order (join before message before leave)

### Entry point behavior
- [ ] `host()` spawns bootstrap, launches onion service, starts accept loop
- [ ] `join()` spawns bootstrap, connects to hub, starts reader/writer
- [ ] Both return `(RoomHandle, EventStream)` without blocking
- [ ] Multiple rooms can be hosted/joined simultaneously (different handles)

### Shutdown behavior
- [ ] `quit()` → all background tasks stop within 5 seconds
- [ ] `quit()` → Tor client shuts down cleanly
- [ ] Drop `RoomHandle` without `quit()` → cleanup triggered
- [ ] Drop `EventStream` → room shuts down automatically
- [ ] No panics on shutdown during active message exchange
- [ ] No resource leaks (file descriptors, tasks, channels)

### Integration (e2e tests)
- [ ] `host()` + 2× `join()` → full message exchange via `RoomHandle::send()`
- [ ] `RoomHandle::invite()` produces code that a new `join()` can use
- [ ] `RoomHandle::quit()` on hub → all joiners see stream end
- [ ] `RoomHandle::quit()` on joiner → hub broadcasts `PeerLeave`

### Formatting checks
- [ ] `cargo fmt -- --check` — passes

### Build integrity
- [ ] `cargo clean && cargo build --package ephemeral-chat-core` — succeeds
- [ ] `cargo clean && cargo build --workspace` — succeeds

---

## Acceptance criteria

- [ ] All verification checks above pass
- [ ] `cargo clippy --package ephemeral-chat-core -- -D warnings` — clean
- [ ] `host()` and `join()` provide clean, simple entry points
- [ ] `RoomHandle` allows sending, inviting, querying peers, quitting
- [ ] `EventStream` delivers all room events in order
- [ ] Shutdown is clean from all paths (quit, drop, error)
- [ ] No panics, no resource leaks, no deadlocks
