# Phase 2 — Core: arti lifecycle

Wire `arti-client` for Tor bootstrap, onion service (hub), and onion connection (joiner).

---

## 2.1 Dependencies

- [x] Add `arti-client` to core crate (tokio-runtime feature)
- [x] Add `arti-hsconfig` to core crate (for v3 onion services)
- [x] Verify `cargo check --workspace` passes with new deps

## 2.2 Tor bootstrap wrapper

- [x] `TorBootstrap` struct wrapping `TorClient< PreferredRuntime >`
- [x] `bootstrap()` method: creates `TorClientConfig`, calls `create_unbootstrapped_async()`
- [x] Emits `ChatEvent::BootstrapProgress(u8)` during bootstrap
- [x] Maps arti errors to `ChatError::Connection`
- [x] Bootstrap is async, cancellable via `tokio::select!`

## 2.3 Hub onion service setup

- [x] `HostedRoom::new(tor_client, port)` — generates ephemeral v3 keypair
- [x] Launches v3 onion service on the given port
- [x] Extracts `.onion` address → emits `ChatEvent::RoomReady`
- [x] `HostedRoom::address()` returns the onion address
- [x] `HostedRoom::shutdown()` tears down the onion service cleanly

## 2.4 Joiner onion connection

- [x] `Joiner::connect(tor_client, invite_code)` — parses invite, connects via Tor
- [x] `tor.connect((onion_address, port))` → returns `DataStream`
- [x] Maps connection failures to `ChatError::Connection`
- [x] `Joiner::shutdown()` closes the stream and cleans up

## 2.5 Shutdown/teardown

- [x] `TorBootstrap::shutdown()` — stops the Tor client
- [x] Both `HostedRoom` and `Joiner` implement `Drop` for safety net cleanup
- [x] Shutdown is idempotent (safe to call multiple times)
- [x] No panics on shutdown during active connections

---

## Verification

### Compilation checks
- [x] `cargo check --package ephemeral-chat-core` — zero errors
- [x] `cargo check --workspace` — cli still compiles
- [x] `cargo clippy --package ephemeral-chat-core -- -D warnings` — zero lints
- [x] All public types exported from crate root

### API surface
- [x] `TorBootstrap` is `pub` with `new()`, `bootstrap()`, `shutdown()` methods
- [x] `HostedRoom` is `pub` with `new()`, `address()`, `shutdown()` methods
- [x] `Joiner` is `pub` with `connect()`, `shutdown()` methods
- [x] `bootstrap()` returns `impl Stream<Item = ChatEvent>`
- [x] `HostedRoom::address()` returns `&str` of onion address
- [x] `Joiner::connect()` returns a `DataStream` (bidirectional)

### Bootstrap behavior
- [x] Bootstrap succeeds on a working network
- [x] Bootstrap failure maps to `ChatError::Connection`, not a panic
- [x] Progress events emit values 0–100
- [x] Bootstrap can be cancelled mid-progress without leaking resources
- [x] Re-bootstrap after shutdown → works

### Onion service (hub)
- [x] `HostedRoom::new()` produces a valid v3 onion address (56 chars + `.onion`)
- [x] Address matches `ChatEvent::RoomReady { onion_address }` via `ready_event()`
- [x] Service listens on the requested port
- [x] `shutdown()` stops accepting new connections
- [x] `shutdown()` is safe to call before any connections are accepted
- [x] Double `shutdown()` is a no-op (no panic)
- [x] `Drop` triggers shutdown if not called explicitly

### Joiner connection
- [x] `Joiner::connect()` succeeds against a running `HostedRoom` on same machine
- [x] Returns a readable/writable stream
- [x] Connection failure (wrong address, service down) → `ChatError::Connection`
- [x] Connection failure does not panic or hang indefinitely (has timeout)
- [x] `shutdown()` closes the stream cleanly
- [x] Double `shutdown()` is a no-op

### Integration (local, no Tor network required)
- [x] Bootstrap + host + join on loopback: hub starts, joiner connects
- [x] Data written by joiner is readable by hub (and vice versa)
- [x] Hub shutdown → joiner sees stream close
- [x] Joiner disconnect → hub accepts loop exits for that peer

### Formatting checks
- [x] `cargo fmt -- --check` — passes

### Build integrity
- [x] `cargo clean && cargo build --package ephemeral-chat-core` — succeeds
- [x] `cargo clean && cargo build --workspace` — succeeds

---

## Acceptance criteria

- [x] All verification checks above pass
- [x] `cargo clippy --package ephemeral-chat-core -- -D warnings` — clean
- [x] Hub starts, emits `RoomReady` with valid `.onion` address
- [x] Joiner connects to hub via Tor stream
- [x] Bidirectional data transfer works over the stream
- [x] Clean shutdown from both sides, no resource leaks
- [x] No panics in any code path
