# Phase 2 — Core: arti lifecycle

Wire `arti-client` for Tor bootstrap, onion service (hub), and onion connection (joiner).

---

## 2.1 Dependencies

- [ ] Add `arti-client` to core crate (tokio-runtime feature)
- [ ] Add `arti-hsconfig` to core crate (for v3 onion services)
- [ ] Verify `cargo check --workspace` passes with new deps

## 2.2 Tor bootstrap wrapper

- [ ] `TorBootstrap` struct wrapping `TorClient< ArtiClient >`
- [ ] `bootstrap()` method: creates `TorClientConfig`, calls `create_bootstrapped()`
- [ ] Emits `ChatEvent::BootstrapProgress(u8)` during bootstrap
- [ ] Maps arti errors to `ChatError::Connection`
- [ ] Bootstrap is async, cancellable via `tokio::select!`

## 2.3 Hub onion service setup

- [ ] `HostedRoom::new(tor_client, port)` — generates ephemeral v3 keypair
- [ ] Launches v3 onion service on the given port
- [ ] Extracts `.onion` address → emits `ChatEvent::RoomReady`
- [ ] `HostedRoom::address()` returns the onion address
- [ ] `HostedRoom::shutdown()` tears down the onion service cleanly

## 2.4 Joiner onion connection

- [ ] `Joiner::connect(tor_client, invite_payload)` — parses invite, connects via Tor
- [ ] `tor.connect((onion_address, port))` → returns `impl AsyncRead + AsyncWrite`
- [ ] Maps connection failures to `ChatError::Connection`
- [ ] `Joiner::shutdown()` closes the stream and cleans up

## 2.5 Shutdown/teardown

- [ ] `TorBootstrap::shutdown()` — stops the Tor client
- [ ] Both `HostedRoom` and `Joiner` implement `Drop` for safety net cleanup
- [ ] Shutdown is idempotent (safe to call multiple times)
- [ ] No panics on shutdown during active connections

---

## Verification

### Compilation checks
- [ ] `cargo check --package ephemeral-chat-core` — zero errors
- [ ] `cargo check --workspace` — cli still compiles
- [ ] `cargo clippy --package ephemeral-chat-core -- -D warnings` — zero lints
- [ ] All public types exported from crate root

### API surface
- [ ] `TorBootstrap` is `pub` with `new()`, `bootstrap()`, `shutdown()` methods
- [ ] `HostedRoom` is `pub` with `new()`, `address()`, `shutdown()` methods
- [ ] `Joiner` is `pub` with `connect()`, `shutdown()` methods
- [ ] `bootstrap()` returns `impl Stream<Item = ChatEvent>` or equivalent
- [ ] `HostedRoom::address()` returns `&str` of onion address
- [ ] `Joiner::connect()` returns a stream (reader/writer pair or bidirectional)

### Bootstrap behavior
- [ ] Bootstrap succeeds on a working network
- [ ] Bootstrap failure maps to `ChatError::Connection`, not a panic
- [ ] Progress events emit values 0–100
- [ ] Bootstrap can be cancelled mid-progress without leaking resources
- [ ] Re-bootstrap after shutdown → error (not undefined behavior)

### Onion service (hub)
- [ ] `HostedRoom::new()` produces a valid v3 onion address (56 chars + `.onion`)
- [ ] Address matches `ChatEvent::RoomReady { onion_address }`
- [ ] Service listens on the requested port
- [ ] `shutdown()` stops accepting new connections
- [ ] `shutdown()` is safe to call before any connections are accepted
- [ ] Double `shutdown()` is a no-op (no panic)
- [ ] `Drop` triggers shutdown if not called explicitly

### Joiner connection
- [ ] `Joiner::connect()` succeeds against a running `HostedRoom` on same machine
- [ ] Returns a readable/writable stream
- [ ] Connection failure (wrong address, service down) → `ChatError::Connection`
- [ ] Connection failure does not panic or hang indefinitely (has timeout)
- [ ] `shutdown()` closes the stream cleanly
- [ ] Double `shutdown()` is a no-op

### Integration (local, no Tor network required)
- [ ] Bootstrap + host + join on loopback: hub starts, joiner connects
- [ ] Data written by joiner is readable by hub (and vice versa)
- [ ] Hub shutdown → joiner sees stream close
- [ ] Joiner disconnect → hub accepts loop exits for that peer

### Formatting checks
- [ ] `cargo fmt -- --check` — passes

### Build integrity
- [ ] `cargo clean && cargo build --package ephemeral-chat-core` — succeeds
- [ ] `cargo clean && cargo build --workspace` — succeeds

---

## Acceptance criteria

- [ ] All verification checks above pass
- [ ] `cargo clippy --package ephemeral-chat-core -- -D warnings` — clean
- [ ] Hub starts, emits `RoomReady` with valid `.onion` address
- [ ] Joiner connects to hub via Tor stream
- [ ] Bidirectional data transfer works over the stream
- [ ] Clean shutdown from both sides, no resource leaks
- [ ] No panics in any code path
