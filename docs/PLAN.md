# ephemeral-chat — build plan

## Phase 0: Project setup
- [x] Create workspace with two crates (`ephemeral-chat-core`, `ephemeral-chat`)
- [x] Set up `Cargo.toml` with dependencies: `tokio`, `arti-client`, `arti-hsconfig`, `ratatui`, `crossterm`, `clap`, `base58`, `serde`, `ed25519-dalek`, `chrono`, `base64`, `rand`
- [x] Configure workspace-level `rustfmt.toml` and `clippy` settings

## Phase 1: Core — data types and invite codes
- [x] Define core types: `PeerId`, `PeerInfo`, `ChatEvent`, `ChatError`, `HostConfig`, `JoinConfig`
- [x] Implement invite code encode/decode
  - [x] Struct: `{ onion_address: String, nonce: [u8; 16], timestamp: u64 }`
  - [x] `encode()`: dot-join → base58 → single token (~120 chars)
  - [x] `decode()`: base58 → split → parse → validate format
- [x] Unit tests for invite code round-trips

## Phase 2: Core — arti lifecycle
- [x] Bootstrap wrapper function
  - [x] Create `TorClientConfig`, call `create_bootstrapped()`
  - [x] Stream arti bootstrap progress events → `ChatEvent::BootstrapProgress(u8)`
  - [x] Handle bootstrap failure → `ChatError`
- [x] Hub onion service setup
  - [x] Generate ephemeral keypair
  - [x] Launch v3 onion service on ephemeral port
  - [x] Extract `.onion` address → `ChatEvent::RoomReady`
- [x] Joiner onion connection
  - [x] Parse invite → extract onion address
  - [x] `tor.connect((onion_address, port))` → `AsyncRead + AsyncWrite` stream
- [x] Shutdown/teardown for both roles

## Phase 3: Core — hub connection handling
- [x] Accept loop: listen on onion service, accept incoming streams
- [x] Per-connection spawn:
  - [x] **Handshake**: read 32-byte nonce+discriminator, check used-nonce `HashSet`, reject or admit
  - [x] **Reader task**: read newline-delimited messages from stream → hub channel
  - [x] **Writer task**: receive from per-peer channel → write to stream
- [x] Peer list: `HashMap<PeerId, PeerEntry>` behind `tokio::sync::RwLock`
- [x] Disconnect cleanup: remove peer, broadcast system message
- [x] Nonce bookkeeping: `HashSet<[u8; 16]>` for single-use enforcement
- [x] Unit tests: nonce dedup, peer registry round-trip (21/21 tests pass)
- [x] `cargo clippy -D warnings` clean, `cargo fmt` passes
- [ ] **Deferred** (see Phase 3 notes):
  - [ ] Nonce expiry validation against `invite_ttl_secs`
  - [ ] Periodic nonce sweep (TTL-based cleanup)
  - [ ] Handshake read timeout
  - [ ] Multi-peer e2e message exchange test

## Phase 4: Core — message broadcast
- [x] Hub broadcast channel (`tokio::sync::broadcast`)
- [x] Hub receives from any peer → fan-out to all other peers
- [x] Hub can also send messages (self as sender)
- [x] Joiner: send messages upstream, receive messages from hub
- [x] Define wire protocol: simple length-prefixed UTF-8 or newline-delimited
- [x] Handle partial reads, connection drops gracefully

## Phase 5: Core — `RoomHandle` and `EventStream`
- [x] Implement `RoomHandle` trait/struct
  - [x] `send(&self, text: &str) → Result<()>`
  - [x] `invite(&self) → Result<String>` (hub only)
  - [x] `peers(&self) → Vec<PeerInfo>`
  - [x] `quit(&self) → impl Future`
- [x] Wire up unified `EventStream` (`tokio::sync::mpsc::Receiver<ChatEvent>`)
- [x] Ensure clean shutdown: `quit()` stops all tasks, closes streams, tears down arti
- [x] `pub fn host()` and `pub fn join()` entry points

## Phase 6: Binary — CLI
- [x] `clap` CLI: `chat host [--invite-ttl <s>] [--name <n>]`, `chat join <code> [--name <n>]`
- [x] `--timestamps` flag
- [x] Name persistence: read/write `~/.config/ephemeral-chat/name`, prompt if missing

## Phase 7: Binary — TUI
- [x] Terminal setup: `crossterm` backend, enter raw mode, alternate screen
- [x] Layout: top bar | message area | status bar | input bar
- [x] Top bar: app name + truncated room ID (first 12 chars)
- [x] Message area: scrollable list, `[name] message` format, auto-scroll to bottom
- [x] Status bar: connected peer names
- [x] Input bar: single-line text input with `>` prompt
- [x] `/invite`, `/peers`, `/quit` slash commands
- [x] Bootstrap progress rendering (spinner + percentage)
- [x] Graceful shutdown on `/quit` or Ctrl-C: cleanup, restore terminal, exit

## Phase 8: Integration and polish
- [ ] Manual test: hub + 2 joiners on same machine
- [ ] Manual test: hub + joiner across machines (verify Tor routing)
- [ ] Error scenarios: expired invite, used nonce, bootstrap failure, hub crash
- [ ] `--timestamps` flag wired through
- [ ] Edge cases: rapid connect/disconnect, large messages, unicode

## Phase 9: Defer (out of v1 scope)
- [ ] Persistent consensus cache across restarts
- [ ] Hub handoff
- [ ] Direct peer-to-peer connections
- [ ] Message history for late joiners
- [ ] Restricted discovery mode as nonce alternative
