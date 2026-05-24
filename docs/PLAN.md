# ephemeral-chat — build plan

## Phase 0: Project setup
- [ ] Create workspace with two crates (`ephemeral-chat-core`, `ephemeral-chat`)
- [ ] Set up `Cargo.toml` with dependencies: `tokio`, `arti-client`, `arti-hsconfig`, `ratatui`, `crossterm`, `clap`, `base58`, `serde`, `ed25519-dalek`, `chrono`, `base64`, `rand`
- [ ] Configure workspace-level `rustfmt.toml` and `clippy` settings

## Phase 1: Core — data types and invite codes
- [ ] Define core types: `PeerId`, `PeerInfo`, `ChatEvent`, `ChatError`, `HostConfig`, `JoinConfig`
- [ ] Implement invite code encode/decode
  - [ ] Struct: `{ onion_address: String, nonce: [u8; 16], timestamp: u64 }`
  - [ ] `encode()`: dot-join → base58 → single token (~120 chars)
  - [ ] `decode()`: base58 → split → parse → validate format
- [ ] Unit tests for invite code round-trips

## Phase 2: Core — arti lifecycle
- [ ] Bootstrap wrapper function
  - [ ] Create `TorClientConfig`, call `create_bootstrapped()`
  - [ ] Stream arti bootstrap progress events → `ChatEvent::BootstrapProgress(u8)`
  - [ ] Handle bootstrap failure → `ChatError`
- [ ] Hub onion service setup
  - [ ] Generate ephemeral keypair
  - [ ] Launch v3 onion service on ephemeral port
  - [ ] Extract `.onion` address → `ChatEvent::RoomReady`
- [ ] Joiner onion connection
  - [ ] Parse invite → extract onion address
  - [ ] `tor.connect((onion_address, port))` → `AsyncRead + AsyncWrite` stream
- [ ] Shutdown/teardown for both roles

## Phase 3: Core — hub connection handling
- [ ] Accept loop: listen on onion service, accept incoming streams
- [ ] Per-connection spawn:
  - [ ] **Handshake**: read nonce, validate expiry, check used-nonce `HashSet`, reject or admit
  - [ ] **Reader task**: read messages from stream → broadcast channel
  - [ ] **Writer task**: receive from broadcast channel → write to stream
- [ ] Peer list: `HashMap<PeerId, Sender>` behind `tokio::sync::RwLock`
- [ ] Disconnect cleanup: remove peer, broadcast system message
- [ ] Nonce bookkeeping: `HashSet<[u8; 16]>` for single-use enforcement

## Phase 4: Core — message broadcast
- [ ] Hub broadcast channel (`tokio::sync::broadcast`)
- [ ] Hub receives from any peer → fan-out to all other peers
- [ ] Hub can also send messages (self as sender)
- [ ] Joiner: send messages upstream, receive messages from hub
- [ ] Define wire protocol: simple length-prefixed UTF-8 or newline-delimited
- [ ] Handle partial reads, connection drops gracefully

## Phase 5: Core — `RoomHandle` and `EventStream`
- [ ] Implement `RoomHandle` trait/struct
  - [ ] `send(&self, text: &str) → Result<()>`
  - [ ] `invite(&self) → Result<String>` (hub only)
  - [ ] `peers(&self) → Vec<PeerInfo>`
  - [ ] `quit(&self) → impl Future`
- [ ] Wire up unified `EventStream` (`tokio::sync::mpsc::Receiver<ChatEvent>`)
- [ ] Ensure clean shutdown: `quit()` stops all tasks, closes streams, tears down arti
- [ ] `pub fn host()` and `pub fn join()` entry points

## Phase 6: Binary — CLI
- [ ] `clap` CLI: `chat host [--invite-ttl <s>] [--name <n>]`, `chat join <code> [--name <n>]`
- [ ] `--timestamps` flag
- [ ] Name persistence: read/write `~/.config/ephemeral-chat/name`, prompt if missing

## Phase 7: Binary — TUI
- [ ] Terminal setup: `crossterm` backend, enter raw mode, alternate screen
- [ ] Layout: top bar | message area | status bar | input bar
- [ ] Top bar: app name + truncated room ID (first 12 chars)
- [ ] Message area: scrollable list, `[name] message` format, auto-scroll to bottom
- [ ] Status bar: connected peer names
- [ ] Input bar: single-line text input with `>` prompt
- [ ] `/invite`, `/peers`, `/quit` slash commands
- [ ] Bootstrap progress rendering (spinner + percentage)
- [ ] Graceful shutdown on `/quit` or Ctrl-C: cleanup, restore terminal, exit

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
