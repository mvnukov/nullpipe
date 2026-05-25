# Ephemeral Chat

Peer-to-peer ephemeral chat over Tor onion services. No servers, no accounts, no message persistence.

## How It Works

One person **hosts** a room, which creates a Tor onion service. The host generates **invite codes** and shares them out-of-band. Others **join** using the invite code, connecting directly through Tor. When the host closes the room, everyone disconnects.

```
┌─────────┐        Tor         ┌─────────┐
│  Host   │ ◄─────────────────► │ Joiner  │
│ (onion) │                     │         │
└─────────┘                     └─────────┘
                                     ▲
                                     │ invite code
                              ┌─────────┐
                              │ Joiner  │
                              └─────────┘
```

## Requirements

- **Rust** 1.75+ (edition 2021)

## Build

```bash
cargo build --release --workspace
```

The binary is at `target/release/chat`.

## Usage

### Host a Room

```bash
chat host
```

The app will prompt for a display name (saved for future use). Once Tor bootstraps and the onion service starts, you'll see `room ready: ...` in the status bar.

Generate an invite code by typing `/invite` and pressing Enter. Share the code with others through any channel (signal, email, qr code, etc.).

Options:
- `--name <name>` — Override display name
- `--invite-ttl <seconds>` — Set invite code expiry (default: 300s / 5 minutes)
- `--timestamps` — Show timestamps on messages

### Join a Room

```bash
chat join <INVITE_CODE>
```

Options:
- `--name <name>` — Override display name
- `--timestamps` — Show timestamps on messages

### Commands

Type `/command` in the input bar:

| Command | Description |
|---------|-------------|
| `/invite` | Generate a new invite code (host only) |
| `/peers` | List connected peers |
| `/quit` | Leave the room |

### Keyboard Controls

| Key | Action |
|-----|--------|
| Enter | Send message |
| Ctrl-C | Quit |
| Up/Down | Scroll message history |
| PageUp/PageDown | Scroll by 10 lines |
| Home/End | Jump to start/end of input |

## Invite Codes

An invite code encodes three fields: the onion address, a single-use nonce, and a timestamp. The code is base58-encoded and looks like a long alphanumeric string.

- **Single-use**: Each nonce can only be used once. Reusing a code results in `nonce already used`.
- **Expiry**: Codes expire after the TTL set by `--invite-ttl`. Default is 5 minutes.
- **Clock skew**: Up to 5 minutes of future clock skew is tolerated.

## Architecture

```
crates/
├── core/   ─── Library: Tor bootstrap, onion services, wire protocol, invite codes
└── cli/    ─── Binary: TUI application using ratatui + crossterm
```

- **Tor**: Uses [`arti-client`](https://crates.io/crates/arti-client) — a **pure-Rust Tor implementation**. The library bundles Tor in-process. No external `tor` binary, `torrc`, or daemon is needed. Just compile and run.
- **Onion services**: v3 ephemeral onion services, one per hosted room
- **Wire protocol**: Length-prefixed JSON messages over Tor data streams
- **Handshake**: 32-byte nonce + discriminator exchange for peer identification

## Development

```bash
# Format
cargo fmt --workspace

# Lint
cargo clippy --workspace -- -D warnings

# Test (includes integration tests requiring Tor)
cargo test --workspace
```

## License

MIT
