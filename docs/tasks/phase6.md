# Phase 6 — Binary: CLI

Implement the `ephemeral-chat` binary with `clap`-based CLI, name persistence, and `--timestamps` flag.

---

## 6.1 CLI structure

- [ ] `clap` derive CLI with two subcommands: `host` and `join`
- [ ] `chat host [--invite-ttl <seconds>] [--name <name>] [--timestamps]`
- [ ] `chat join <invite_code> [--name <name>] [--timestamps]`
- [ ] `--invite-ttl` default: 300 (5 minutes)
- [ ] `invite_code` positional arg for `join`, required
- [ ] Help text for all args and subcommands

## 6.2 Name persistence

- [ ] On first launch: prompt user for display name via stdin
- [ ] Store name in `~/.config/ephemeral-chat/name`
- [ ] On subsequent launches: read name from file
- [ ] `--name` flag overrides persisted name
- [ ] Create config directory if it doesn't exist
- [ ] Handle missing/corrupt name file gracefully (re-prompt)

## 6.3 Timestamps flag

- [ ] `--timestamps` flag parsed and passed through to core/TUI
- [ ] When enabled, messages render with `HH:MM` prefix
- [ ] Off by default

## 6.4 Binary entry point

- [ ] `main()` is async (`#[tokio::main]`)
- [ ] Parse CLI args, resolve name
- [ ] For `host`: call `ephemeral_chat_core::host(HostConfig)`
- [ ] For `join`: call `ephemeral_chat_core::join(JoinConfig)`
- [ ] Wire `RoomHandle` + `EventStream` to TUI (Phase 7)
- [ ] Handle Ctrl-C for graceful shutdown

---

## Verification

### Compilation checks
- [ ] `cargo check --package ephemeral-chat` — zero errors
- [ ] `cargo check --workspace` — zero errors
- [ ] `cargo clippy --workspace -- -D warnings` — zero lints
- [ ] `cargo fmt -- --check` — passes

### CLI behavior
- [ ] `chat host` starts without errors
- [ ] `chat host --help` shows all options
- [ ] `chat join <code>` requires invite code arg
- [ ] `chat join --help` shows all options
- [ ] `chat` (no subcommand) shows usage or error
- [ ] `chat host --invite-ttl 60` parses TTL correctly
- [ ] `chat host --name "alice"` overrides persisted name
- [ ] `chat join --timestamps <code>` enables timestamps

### Name persistence
- [ ] First run prompts for name
- [ ] Name saved to `~/.config/ephemeral-chat/name`
- [ ] Second run reads name without prompt
- [ ] `--name` flag overrides file value
- [ ] Corrupt file → re-prompt, no crash

### Build integrity
- [ ] `cargo clean && cargo build --workspace` — succeeds
- [ ] Binary runs: `cargo run -- host --help` — exits 0

---

## Acceptance criteria

- [ ] All verification checks above pass
- [ ] `cargo clippy --workspace -- -D warnings` — clean
- [ ] `chat host` and `chat join` subcommands work with all flags
- [ ] Name persistence works across runs
- [ ] `--timestamps` flag parsed and available for TUI
- [ ] Clean error messages for invalid usage
