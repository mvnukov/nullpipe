# ephemeral-chat spec

A terminal-based ephemeral chat app. No accounts, no servers you have to trust, no public IP required. The hub spins up an ephemeral Tor onion service via an embedded arti instance. Joiners connect through the Tor network using a time-limited invite code. When the hub closes, the room is gone.

---

## goals

- Zero signup. Identity is a keypair generated at runtime by arti.
- One command to start a room, one command to join.
- No public IP required for either peer. Tor handles all routing and NAT traversal.
- Invite codes expire. Used codes are burned.
- No external tor process required. arti is embedded in the binary.
- All traffic encrypted end-to-end by the Tor onion service protocol.
- Ephemeral. No messages written to disk. No logs.
- Single self-contained binary.

## non-goals

- Persistence or message history.
- File transfer.
- Voice or video.
- Mobile clients.
- Anonymity guarantees beyond what Tor provides. The Tor network itself is the trust boundary.

---

## crate structure

The implementation splits into two crates:

```
ephemeral-chat-core/   library, no TUI dependency
ephemeral-chat/        binary, TUI only, depends on core
```

This split exists so the protocol logic can be reused by other frontends (mobile, GUI, headless bot) without pulling in ratatui or any terminal code.

### ephemeral-chat-core

Owns everything protocol-related:

- arti lifecycle (bootstrap, shutdown)
- onion service setup and teardown
- invite code generation, parsing, validation (expiry + nonce)
- connection accept loop and handshake
- peer list management
- message broadcast to all peers
- all state changes emitted as an async event stream

Public API surface:

```rust
// start a room as hub
async fn host(config: HostConfig) -> Result<(RoomHandle, EventStream)>

// join a room as peer
async fn join(invite_code: &str, config: JoinConfig) -> Result<(RoomHandle, EventStream)>

// handle returned from both, used to interact with the room
impl RoomHandle {
    async fn send(&self, text: &str) -> Result<()>
    async fn invite(&self) -> Result<String>   // hub only
    fn peers(&self) -> Vec<PeerInfo>
    async fn quit(&self)
}

// all state changes come through here
enum ChatEvent {
    BootstrapProgress(u8),          // 0-100
    RoomReady { onion_address: String },
    PeerJoined { id: PeerId, name: String },
    PeerLeft { id: PeerId, name: String },
    MessageReceived { from: PeerId, name: String, text: String },
    InviteCreated { code: String, expires_in_secs: u64 },
    RoomClosed,
    Error(ChatError),
}
```

No ratatui, no terminal, no stdout. The library is pure async logic.

### ephemeral-chat (binary)

Depends only on `ephemeral-chat-core` and ratatui. Responsibilities:

- parse CLI args
- call `host()` or `join()` from core
- render `ChatEvent` stream into the TUI
- send user input to `RoomHandle`
- nothing else

A future mobile frontend would depend on `ephemeral-chat-core` and implement its own rendering against the same `ChatEvent` stream.

---

## why tor / arti

Two machines behind NAT cannot reach each other without at least one having a public IP, or a relay. Tor solves this without any single company or server in the middle:

- The hub only makes outbound connections to Tor introduction points. Its real IP never appears in any metadata.
- The joiner only makes outbound connections into the Tor network. Its real IP never appears either.
- The onion address is derived from the hub's keypair. No registration, no DNS, no central directory owned by anyone.
- The Tor network itself is the infrastructure: thousands of volunteer-run nodes, no single operator.

arti is the Tor Project's official Rust implementation. As of 2.0.0 (early 2026) onion service support is stable and production-ready. Embedding arti means zero runtime dependencies for the user.

---

## tech stack

- **Language:** Rust
- **Tor:** arti (embedded, no external tor process)
- **TUI:** ratatui
- **Async runtime:** tokio

---

## architecture

### roles

**Hub** is the peer who starts the room. They:

- start an embedded arti instance
- spin up an ephemeral v3 onion service
- generate invite codes (onion address + nonce + timestamp)
- accept incoming connections from joiners
- maintain a list of active peer connections
- receive a message from any peer and write it to all other peers
- own the room lifecycle: when hub exits, room ends and onion service disappears

**Joiner** is any peer who connects via an invite code. They:

- start an embedded arti instance
- parse the invite code
- connect to the hub's onion address through Tor
- send their nonce as the first message on connect (handshake)
- receive and display messages
- send typed messages to the hub

Joiners have no direct connections to each other. All routing goes through the hub.

### message flow

```
joiner A  -->  [tor network]  -->  hub  -->  [tor network]  -->  joiner B
                                        -->  [tor network]  -->  joiner C
```

Hub receives from A, fans out to B, C. Hub receives from B, fans out to A, C. And so on.

### hub internals

Hub runs a tokio broadcast channel. Each accepted connection spawns two tasks:

- **reader task**: reads from the onion service stream, sends to broadcast channel
- **writer task**: receives from broadcast channel, writes to stream

Peer list is a `HashMap<PeerId, Sender>` behind a `tokio::sync::RwLock`. Reader task removes the peer on disconnect and broadcasts a system message to remaining peers.

---

## invite codes

### structure

An invite code bundles three things:

- the v3 onion address (56 characters, base32, derived from hub's public key)
- a 16-byte random nonce (base32 encoded)
- a Unix timestamp in seconds (u64, when the code was generated)

Concatenated with dots before final encoding:

```
<onion_address>.<nonce>.<timestamp>
```

The whole string is base58 encoded into a single copyable token. Approximate length: 120 characters. No special characters, no spaces.

### expiry

Default TTL is 300 seconds (5 minutes). Configurable via `--invite-ttl <seconds>` on the hub.

On connect, hub checks:

```
now() - timestamp > ttl  =>  reject, close connection
```

### single-use enforcement

Hub keeps an in-memory `HashSet<[u8; 16]>` of used nonces for the session. On connect:

1. Parse nonce from first message (handshake).
2. Check expiry.
3. Check nonce not in used set.
4. If valid: insert nonce into used set, admit peer.
5. If invalid: close connection immediately, before any chat data flows.

The used nonce set lives only in memory. It resets if the hub restarts, which also invalidates all outstanding codes since the onion address changes with the keypair.

### generating codes

Hub generates one code per `/invite` command. Codes are not reused. The hub can generate as many as needed, one per intended joiner.

---

## user flow

### hub

```
$ chat host
starting tor... done
room ready

invite: 3mJkQv8rXpN2...  (expires in 5 minutes)

[system] alice joined
[alice] hey
[you] hello

> _
```

Startup includes a brief tor bootstrap delay (typically 5-15 seconds on first run while arti fetches the network consensus). Subsequent runs are faster as arti caches consensus data in memory for the session.

Typing `/invite` generates a new code and prints it without leaving the chat view.
Typing `/peers` lists connected peers.
Typing `/quit` closes the room and disconnects all peers.

### joiner

```
$ chat join 3mJkQv8rXpN2...
starting tor... done
connecting...
joined room

[system] you joined
[hub] hello
[alice] hey

> _
```

---

## startup time

arti needs to bootstrap into the Tor network before the onion service is reachable. This involves fetching a network consensus document from directory servers. On a typical connection:

- first run: 5-15 seconds
- subsequent runs within a session: faster, consensus cached in memory

The TUI should show a progress indicator during bootstrap rather than a blank screen. arti exposes bootstrap progress events for this purpose.

Consensus is not written to disk (ephemeral session). If persistence across restarts is wanted later, arti supports a configurable state directory.

---

## name selection

On first launch the app prompts for a display name. Stored in `~/.config/ephemeral-chat/name`, reused on subsequent runs. Overridable with `--name <name>`.

Display names are cosmetic only and unauthenticated. The underlying identity is the onion address (hub) or the Tor circuit (joiners, who have no persistent identity in v1).

---

## TUI layout

```
+--------------------------------------------------+
| ephemeral-chat          room: <onion addr 12ch>  |
+--------------------------------------------------+
|                                                  |
|  [system] alice joined                           |
|  [alice] hey                                     |
|  [you] hello                                     |
|  [bob] whats up                                  |
|                                                  |
+--------------------------------------------------+
| peers: alice, bob                                |
+--------------------------------------------------+
| > _                                              |
+--------------------------------------------------+
```

- Top bar: app name, truncated room ID (first 12 chars of onion address)
- Message area: scrollable, newest at bottom
- Status bar: connected peer display names
- Input bar: single-line text input

Message format: `[name] message text`. System messages use `[system]`.

`--timestamps` flag enables `HH:MM` prefix on each message.

---

## cli interface

```
chat host [--invite-ttl <seconds>] [--name <name>]
chat join <invite_code> [--name <name>]
```

---

## arti integration

Hub side:

```rust
// pseudocode
let config = TorClientConfig::default();
let tor = TorClient::create_bootstrapped(config).await?;
let onion_service = tor.launch_onion_service(onion_config).await?;
// onion_service.onion_name() gives the .onion address
// accept incoming streams from onion_service
```

Joiner side:

```rust
let config = TorClientConfig::default();
let tor = TorClient::create_bootstrapped(config).await?;
let stream = tor.connect((onion_address, port)).await?;
// stream is a standard AsyncRead + AsyncWrite
```

arti handles all circuit construction, introduction point negotiation, and rendezvous internally. The app sees a plain async stream on both sides.

---

## encryption and privacy

| layer | mechanism |
|---|---|
| transport encryption | Tor onion service protocol, TLS 1.3 equivalent |
| hub's IP | never exposed, Tor hides it |
| joiner's IP | never exposed, Tor hides it |
| hub sees message content | yes, hub routes plaintext between peers |
| Tor network sees content | no, onion encryption prevents this |
| metadata visible to Tor nodes | circuit timing only, no content, no IPs |

The hub sees plaintext of all messages since it is the routing node. This is a known and accepted property of the hub-and-spoke model. The hub is the person who started the room, so this is a trust relationship the joiner accepts explicitly by joining.

---

## security model

| property | status |
|---|---|
| no account or signup | yes |
| no public IP required | yes, Tor handles routing for both peers |
| no trusted relay company | yes, Tor network has no single owner |
| invite expires | yes, configurable TTL, default 5 minutes |
| invite single-use | yes, nonce burned on connect |
| traffic encrypted | yes, Tor onion service protocol |
| hub IP hidden from joiners | yes |
| joiner IP hidden from hub | yes |
| hub can read messages | yes, hub routes plaintext |
| metadata-free | mostly: Tor nodes see circuit timing, not content or IPs |
| MitM on invite delivery | out of scope, user's responsibility to share invite over trusted channel |

---

## error handling

| condition | behavior |
|---|---|
| tor bootstrap fails | app exits with error and reason from arti |
| expired invite | hub closes connection, joiner sees "invite expired" |
| used nonce | hub closes connection, joiner sees "invite already used" |
| malformed invite code | joiner sees "invalid invite code" before connecting |
| hub exits | all joiners see "[system] room closed", app exits after 3 seconds |
| peer disconnects | hub broadcasts "[system] name left", removes from peer list |
| tor circuit drops mid-session | peer sees disconnect, hub cleans up, others notified |

---

## persistence and privacy

- No messages written to disk.
- No logs written to disk by default.
- arti consensus and state not written to disk (in-memory only for the session).
- Onion service keypair is ephemeral, generated at startup, gone on exit.
- Display name stored in `~/.config/ephemeral-chat/name`, user-deletable.
- Used nonce set lives in memory only.

---

## v1 scope

Ship:

- hub and join modes
- embedded arti, no external tor dependency
- ephemeral onion service per session
- invite code generation with TTL and nonce burning
- multi-peer broadcast via hub
- ratatui TUI with bootstrap progress, message area, status bar, input
- display names
- `/invite`, `/peers`, `/quit` commands
- bootstrap progress indicator during tor startup

Defer:

- persistent consensus cache across restarts (speeds up subsequent startups)
- hub handoff (room survives hub exit)
- direct joiner-to-joiner connections
- message history replay for late joiners
- arti restricted discovery mode as alternative to nonce system