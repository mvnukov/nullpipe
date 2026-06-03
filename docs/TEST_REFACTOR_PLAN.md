# Mock Tor Integration Test Refactor Plan

## Problem

Every integration test bootstraps real Tor. Even with `max-threads = 1`, a host+joiner pair takes 40-80s because each side independently:
1. Creates an Arti state dir
2. Downloads Tor consensus (~1.5MB)
3. Builds circuits (~15-25s each)
4. Host additionally publishes an onion service descriptor (~5-10s)

We need fast (~ms), deterministic integration tests that exercise the same `Hub`, `Joiner`, handshake, and wire protocol code — just without real Tor.

## What Already Exists (good foundation)

| Piece | Status |
|---|---|
| `TorConnector` trait | ✅ Already extracted, `connector.rs` |
| `MockTorConnector` | ✅ Exists, can return canned `DataStream` results |
| `Joiner::connect(tor: &impl TorConnector, ...)` | ✅ Takes trait, mock-compatible |
| `Joiner::handshake<S: AsyncRead+AsyncWrite+Unpin>` | ✅ Generic over stream type |
| `hub_handshake(&mut (impl AsyncRead+AsyncWrite+Unpin))` | ✅ Generic |
| `hub_reader_task(impl AsyncReadExt+Unpin, ...)` | ✅ Generic |
| `Joiner::spawn_reader<R: AsyncReadExt+Unpin+Send+'static>` | ✅ Generic |
| `Joiner::write_loop<W: AsyncWriteExt+Unpin>` | ✅ Generic |

## What Holds It Back

### 1. `handle_hub_connection(stream: DataStream, ...)`

Takes `arti_client::DataStream` by concrete type. Must become generic over `AsyncRead + AsyncWrite + Unpin + Send + 'static` so tests can pass `tokio::io::DuplexStream`.

### 2. `HostedRoom::accept_peer() -> Option<DataStream>`

Only produces Tor `DataStream`s. Need a way to inject mock streams for the host's accept loop.

### 3. `host_task()` / `run_host_loop()` always bootstrap Tor

No code path exists to skip Tor and inject pre-connected streams.

### 4. `joiner_task()` always bootstraps Tor

Even though `Joiner::connect()` is mockable (via `TorConnector` trait), `joiner_task()` bootstraps real Tor and creates a real `ArtiConnector`. No code path to inject a pre-connected stream.

### 5. `RoomInner` stores `Arc<Mutex<Option<TorBootstrap>>>`

The `tor` field in `RoomInner` is only used for cleanup (shutdown/drop). It's an implementation detail but couples the handle lifecycle to Tor.

## Plan (5 Steps)

### Step 1 — Make `handle_hub_connection` generic over stream type

Change signature from `stream: DataStream` to `stream: S where S: AsyncRead + AsyncWrite + Unpin + Send + 'static`.

The function body is already generic (handshake, reader, writer all take generic bounds). Only the parameter type needs changing. All callers pass a `DataStream` so this compiles without changes.

**Files:** `crates/core/src/room.rs`
**Risk:** Low. Pure type generalization, no logic changes.

### Step 2 — Add `MockAcceptor` in `connector.rs`

A simple struct that receives `DuplexStream`s instead of Tor `DataStream`s:

```rust
// in connector::mock
pub struct MockAcceptor {
    stream_rx: mpsc::Receiver<DuplexStream>,
}
impl MockAcceptor {
    pub fn new(rx: mpsc::Receiver<DuplexStream>) -> Self { ... }
    pub async fn accept(&mut self) -> Option<DuplexStream> { ... }
}
```

This mirrors `HostedRoom::accept_peer()` but without Tor. `DuplexStream` implements `AsyncRead + AsyncWrite + Unpin + Send`.

**New file:** add to `crates/core/src/connector.rs` (inside `mock` module)
**Risk:** Low. Just a new type with trivial logic.

### Step 3 — Refactor `accept_loop` to use a generic stream source

Change `accept_loop` from taking `room: HostedRoom` to taking a closure/generic that produces streams. Two approaches:

**Option A (trait):**
```rust
#[async_trait]
trait StreamSource {
    type Stream: AsyncRead + AsyncWrite + Unpin + Send;
    async fn accept(&mut self) -> Option<Self::Stream>;
}
impl StreamSource for HostedRoom { type Stream = DataStream; ... }
impl StreamSource for MockAcceptor { type Stream = DuplexStream; ... }
```

**Option B (mpsc channel):** 
Change `accept_loop` to take `stream_rx: mpsc::Receiver<Box<dyn AsyncRead+AsyncWrite+Unpin+Send>>`, and adapt `HostedRoom` to send through a channel.

Option B is simpler. The ordering overhead of boxing is irrelevant in test context and negligible in production.

**Files:** `crates/core/src/room.rs`, `crates/core/src/hub.rs`
**Risk:** Medium. Requires changing `accept_loop` and `run_host_loop` signatures, and adapting `HostedRoom`.

### Step 4 — Add test-only `host_mock()` / `join_mock()` constructors

```rust
// exposed under `#[cfg(feature = "test-mocks")]` or `#[doc(hidden)]` pub

pub fn host_with_mock(
    config: HostConfig,
    stream_rx: mpsc::Receiver<DuplexStream>,
) -> (RoomHandle, EventStream) { ... }

pub fn join_with_mock(
    config: JoinConfig,
    stream: DuplexStream,
) -> (RoomHandle, EventStream) { ... }
```

`host_with_mock()`:
- Same as `host()` but skips `host_task` → `bootstrap_with_shutdown` → `run_host_loop` sequence
- Instead calls a `run_host_loop_mock()` that passes `stream_rx` to `accept_loop`
- Still fires `ChatEvent::RoomReady` (with a fake address) so downstream API works

`join_with_mock()`:
- Same as `join()` but skips `joiner_task` → `bootstrap_with_shutdown` sequence
- Instead creates a `Joiner` directly from the duplex stream (no `TorConnector`)
- Runs the same handshake and `Joiner::run()` code

**Files:** `crates/core/src/room.rs`
**Risk:** Medium. Adds new code paths that must stay in sync with production paths.

### Step 5 — Write fast integration tests

In `crates/core/tests/`:

```rust
#[tokio::test]
async fn integration_peer_join_fires() {
    let (host_stream, joiner_stream) = tokio::io::duplex(4096);
    let mut stream_rx = /* send host_stream to MockAcceptor */;
    
    let (host_h, mut host_ev) = host_with_mock(config, stream_rx);
    let (joiner_h, mut joiner_ev) = join_with_mock(config, joiner_stream);
    
    // ... await PeerJoin events, verify messages flow ...
    // Takes ~5ms instead of 40-80s
}
```

Test scenarios (replacing the current e2e_* tests):
1. `integration_peer_join_fires` — host sees PeerJoin, joiner sees PeerJoin
2. `integration_messages_flow_bidirectional` — host↔joiner messages
3. `integration_peer_leave_on_joiner_quit` — joiner quits, host sees PeerLeave
4. `integration_room_close_notifies_joiner` — host quits, joiner sees RoomClosed
5. `integration_host_send_message_no_peers` — host sends without peers
6. `integration_full_handshake_roundtrip` — verify handshake byte protocol

These replace the e2e Tor tests as the primary CI gate. The real Tor e2e tests become a smoke suite run separately (nightly or on demand).

## Changes Summary

| File | Change | Risk |
|---|---|---|
| `crates/core/src/room.rs` | Generic stream in `handle_hub_connection`, refactor accept loop, add mock constructors | Medium |
| `crates/core/src/hub.rs` | Adapt `HostedRoom` to work with generic accept loop | Low |
| `crates/core/src/connector.rs` | Add `MockAcceptor` | Low |
| `Cargo.toml` (workspace) | No new deps needed (`DuplexStream` is in tokio already) | None |
| `crates/core/Cargo.toml` | Possibly add `test-mocks` feature | Low |
| `.config/nextest.toml` | Keep serial e2e group for Tor tests | None |
| `crates/core/tests/*.rs` | New fast integration tests | Low |

## Order of Implementation

1. Step 1 — generic `handle_hub_connection` (easiest, unlocks everything)
2. Step 2 — `MockAcceptor` (trivial new code)
3. Step 3 — refactor `accept_loop` (core change, impacts host path)
4. Step 4 — mock constructors (wiring everything together)
5. Step 5 — write the fast tests and verify they pass
6. Move current e2e_* tests to a separate slow-test group or nightly
