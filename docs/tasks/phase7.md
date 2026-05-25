# Phase 7 — Binary: TUI

Implement the ratatui-based terminal UI with bootstrap progress, message display, input bar, status bar, and slash commands.

---

## 7.1 Terminal setup

- [ ] `crossterm` backend for ratatui
- [ ] Enter raw mode on startup
- [ ] Enter alternate screen on startup
- [ ] Graceful shutdown: restore terminal on exit (normal or panic)
- [ ] Panic hook: restore terminal before printing backtrace

## 7.2 Layout

- [ ] Top bar: `ephemeral-chat` + truncated room ID (first 12 chars of onion address)
- [ ] Message area: scrollable region between top bar and status bar
- [ ] Status bar: connected peer display names (comma-separated)
- [ ] Input bar: single-line text input with `> ` prompt
- [ ] Layout recalculates on terminal resize

## 7.3 Message rendering

- [ ] Format: `[name] message text`
- [ ] System messages: `[system] text`
- [ ] Auto-scroll to bottom on new messages
- [ ] Scrollable with arrow keys or PageUp/PageDown
- [ ] Visual indicator when scrolled away from bottom
- [ ] `--timestamps` mode: `HH:MM [name] message text`

## 7.4 Bootstrap progress

- [ ] Render bootstrap progress as spinner + percentage
- [ ] Display "starting tor..." during bootstrap
- [ ] Replace with message area once `RoomReady` event arrives
- [ ] Handle bootstrap failure: show error, exit gracefully

## 7.5 Input handling

- [ ] Single-line text input at bottom
- [ ] Basic line editing: backspace, left/right arrows, home/end
- [ ] Enter sends message via `RoomHandle::send()`
- [ ] Ctrl-C triggers graceful shutdown

## 7.6 Slash commands

- [ ] `/invite` — calls `RoomHandle::invite()`, prints new code in message area
- [ ] `/peers` — calls `RoomHandle::peers()`, prints peer list in message area
- [ ] `/quit` — calls `RoomHandle::quit()`, triggers shutdown
- [ ] Unknown slash commands: print "unknown command" in message area
- [ ] Commands only work in message mode, not during bootstrap

## 7.7 Event handling

- [ ] `ChatEvent::BootstrapProgress(u8)` → update progress display
- [ ] `ChatEvent::RoomReady` → switch to message area
- [ ] `ChatEvent::PeerJoined` → append `[system] name joined`
- [ ] `ChatEvent::PeerLeft` → append `[system] name left`
- [ ] `ChatEvent::MessageReceived` → append `[name] text`
- [ ] `ChatEvent::InviteCreated` → show new invite code in messages
- [ ] `ChatEvent::RoomClosed` → show message, start shutdown timer
- [ ] `ChatEvent::Error` → show error, optionally exit

## 7.8 Shutdown

- [ ] On `/quit` or Ctrl-C: call `RoomHandle::quit()`
- [ ] Restore terminal (raw mode off, alternate screen off)
- [ ] Wait for background tasks to stop (max 5 seconds)
- [ ] Exit cleanly with code 0

---

## Verification

### Compilation checks
- [ ] `cargo check --package ephemeral-chat` — zero errors
- [ ] `cargo check --workspace` — zero errors
- [ ] `cargo clippy --workspace -- -D warnings` — zero lints
- [ ] `cargo fmt -- --check` — passes

### Terminal behavior
- [ ] Terminal enters raw mode and alternate screen on start
- [ ] Terminal restored on normal exit
- [ ] Terminal restored on panic (panic hook works)
- [ ] Terminal restored on Ctrl-C
- [ ] Layout adjusts on terminal resize

### Display
- [ ] Top bar shows app name and truncated room ID
- [ ] Messages render as `[name] text`
- [ ] System messages render as `[system] text`
- [ ] Auto-scroll works on new messages
- [ ] Manual scroll with arrow keys works
- [ ] Visual indicator when scrolled away from bottom
- [ ] Status bar shows connected peers
- [ ] `--timestamps` renders `HH:MM` prefix

### Bootstrap
- [ ] Progress spinner + percentage visible during bootstrap
- [ ] Progress updates in real-time
- [ ] Switches to message area after RoomReady
- [ ] Bootstrap failure shows error message

### Input
- [ ] Text input at bottom with `> ` prompt
- [ ] Backspace, arrows, home/end work
- [ ] Enter sends message to room
- [ ] Long input wraps or truncates correctly

### Slash commands
- [ ] `/invite` generates and displays new invite code
- [ ] `/peers` displays connected peer list
- [ ] `/quit` triggers clean shutdown
- [ ] Unknown command shows error in message area

### Event handling
- [ ] All ChatEvent variants render correctly
- [ ] Events arrive in order
- [ ] No message duplication on reconnect

### Build integrity
- [ ] `cargo clean && cargo build --workspace` — succeeds

---

## Acceptance criteria

- [ ] All verification checks above pass
- [ ] `cargo clippy --workspace -- -D warnings` — clean
- [ ] TUI renders all layout areas correctly
- [ ] Bootstrap progress is visible and updates
- [ ] Messages display correctly with auto-scroll
- [ ] Input works with basic line editing
- [ ] All three slash commands work
- [ ] Shutdown is clean from all paths
- [ ] No terminal corruption on exit
