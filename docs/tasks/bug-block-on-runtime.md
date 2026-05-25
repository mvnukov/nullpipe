# Bug: `block_on` inside active Tokio runtime crashes on slash commands

**Severity:** Critical — app panics on `/invite`, `/peers`, `/quit`  
**Location:** `crates/cli/src/main.rs:258` (`App::command()`)  
**Reproduction:** Run `cargo run --release --bin chat -- host`, type `/invite`, crash.

## Panic

```
thread 'main' (7689760) panicked at crates/cli/src/main.rs:258:26:
Cannot start a runtime from within a runtime. This happens because a function
(like `block_on`) attempted to block the current thread while the thread is
being used to drive asynchronous tasks.
```

## Root Cause

`main()` uses `#[tokio::main]`, so the app runs inside a Tokio runtime. The main loop drives a `tokio::select!` that processes key events, chat events, and timer ticks — all on the runtime's driver thread.

But `App::command()` is a **synchronous** `&mut self` method that calls `Handle::current().block_on(...)`:

```rust
fn command(&mut self, cmd: &str) {
    let rt = tokio::runtime::Handle::current();
    match rt.block_on(h.invite()) { ... }
}
```

`block_on` tries to synchronously block the current thread. But the current thread **is** the Tokio runtime driver — the very thread executing the `select!` loop that called `handle_key()` → `send()` → `command()`. Tokio detects the self-deadlock and panics.

All three commands are affected:

| Command | Line | Call |
|---------|------|------|
| `/invite` | 258 | `rt.block_on(h.invite())` |
| `/peers` | 266 | `rt.block_on(h.peers())` |
| `/quit` | 276 | `rt.block_on(h.quit())` |

## Why It Happened

The TUI uses a synchronous event loop (`crossterm::event::poll` / `event::read` polled on a `spawn_blocking` thread, results sent via `mpsc` channel). Key events are handled synchronously in the `select!` branch via `app.handle_key(key)`. Slash commands need async execution but `command()` has no `async` context. `block_on` was used as a shortcut — it works from a normal thread but **never** from inside the runtime it's trying to block on.

## Fix

Commands must not block the runtime thread. Options:

1. **`tokio::spawn` + response channel** — spawn the async work, send result back through a channel, process it in the main `select!` loop.
2. **Restructure commands as async** — make `command` async and `await` via the `select!` loop directly.

Approach (1) is minimal: replace each `block_on` with `tokio::spawn`, feed results back as `ChatEvent` variants through the existing `event_rx` channel (or a separate command-result channel merged into `select!`).
