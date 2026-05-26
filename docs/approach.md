# Refactoring approach

## Process

1. **Understand the codebase** — read specs, count lines, identify dead code and duplication
2. **Write high-level algorithm** — describe what the component does in plain steps, no implementation details
3. **Create function signatures** — one function per algorithm step, bodies are `todo!()`
4. **Write tests targeting the signatures** — test expected behavior, no `#[should_panic]`, no "not implemented" comments. Tests fail naturally because `todo!()` panics

## Applied to joiner

**Step 1:** Found `joiner.rs` (321 lines) unused in production — duplicate of inline code in `room.rs`

**Step 2:** Wrote the joiner algorithm:
```
JOIN(connector, invite_code, name)
1. CONNECT    — decode invite, validate expiry, connect to onion
2. HANDSHAKE  — exchange nonce, get accepted, register name
3. RUN        — concurrently read messages, send messages, detect dead connection
4. CLEANUP    — close connection
```

**Step 3:** Created function signatures:
- `connect(&impl TorConnector, invite_code, name) -> Result<Self>`
- `handshake(stream, name) -> Result<(PeerId, DataStream)>`
- `run(messages, events, shutdown) -> Result<()>`
- `close(&mut self)`

**Step 4:** Wrote 21 unit tests + 15 integration tests + 4 e2e tests. All compile, all fail at `todo!()`.
