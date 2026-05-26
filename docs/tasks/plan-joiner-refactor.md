# Joiner refactor plan

## Problem
Joiner logic is split and duplicated:
- `joiner.rs` (321 lines) — standalone `Joiner` struct, unused in production
- `room.rs` (~200 lines) — `joiner_task`, `joiner_handshake`, `run_joiner_loop`, `joiner_reader_task`

Production path: `room::join()` → inline `joiner_task`. `Joiner` struct is test-only.

## Step 1: Extract bootstrap into shared utility
Both hub and joiner bootstrap the same way: create client → bootstrap with progress → handle shutdown → return client.

Currently three copies:
- `bootstrap.rs::TorBootstrap` — basic wrapper, no shutdown handling
- `room.rs::bootstrap_tor()` — adds shutdown monitoring + event forwarding (used by both host and joiner)
- `room.rs::SharedTorClient` — test-only duplicate, no progress events

Move `bootstrap_tor()` from `room.rs` into `bootstrap.rs` as `pub async fn bootstrap_with_shutdown(...)`. Delete `SharedTorClient`. Both `host()` and `join()` call the shared function.

## Step 2: Rewrite joiner.rs
Put the algorithm at the top of the file as a map. The file becomes the single joiner implementation:

```
JOIN(invite_code, name)

1. BOOTSTRAP    — shared util, progress → events
2. CONNECT      — decode invite, validate expiry, Tor connect
3. HANDSHAKE    — write nonce+discriminator, read accept byte, send name
4. SPLIT STREAM — reader background, writer on-demand
5. READER LOOP  — wire frames → ChatEvent, timeout = dead
6. WRITER LOOP  — user messages → wire frames → stream
7. CLEANUP      — close stream, drop client
```

## Step 3: Wire room::join() to use new Joiner
Replace inline `joiner_task` / `run_joiner_loop` / `joiner_handshake` / `joiner_reader_task` in room.rs with a call into the new Joiner.

## Step 4: Delete ~400 lines from room.rs
Inline joiner code + SharedTorClient + bootstrap_tor all go away.

## Step 5: Update tests
Tests using `Joiner::connect_to*` switch to `room::join()` or the new clean API.
