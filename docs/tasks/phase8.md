# Phase 8 — Integration and Polish

End-to-end testing, error scenario validation, edge case handling, and release readiness.

---

## 8.1 Local integration tests

- [ ] Hub + 2 joiners on same machine: full message exchange
- [ ] Hub + 3 joiners: fan-out reaches all recipients
- [ ] Joiner leaves → others see `[system] name left`
- [ ] Hub quits → all joiners see `[system] room closed`
- [ ] New invite generated mid-session → new joiner can use it

## 8.2 Cross-machine test

- [ ] Hub on one machine, joiner on another: Tor routing works
- [ ] Message latency acceptable (<5s on typical connection)
- [ ] Connection survives typical network hiccups

## 8.3 Error scenarios

- [ ] Expired invite → joiner sees clear error message
- [ ] Used nonce → joiner sees "invite already used"
- [ ] Bootstrap failure → app exits with error, reason shown
- [ ] Hub crash mid-session → joiners notified, no hang
- [ ] Joiner crash → hub cleans up, others notified
- [ ] Invalid invite code format → joiner sees parse error before connecting

## 8.4 Feature completeness

- [ ] `--timestamps` flag wired through CLI → TUI
- [ ] `--invite-ttl` flag actually controls invite expiry
- [ ] `--name` flag overrides persisted name
- [ ] Display name persistence works end-to-end
- [ ] All ChatEvent variants exercised in integration

## 8.5 Edge cases

- [ ] Rapid connect/disconnect cycle (10 joiners in 30s)
- [ ] Large messages near 16KB limit
- [ ] Unicode messages (emoji, CJK, RTL)
- [ ] Empty message → rejected or ignored gracefully
- [ ] Very long display names → truncated in UI
- [ ] Terminal too small → layout degrades gracefully
- [ ] Multiple `/invite` calls in rapid succession

## 8.6 Release readiness

- [ ] `cargo clippy --workspace -- -D warnings` — zero lints
- [ ] `cargo fmt -- --check` — passes
- [ ] `cargo test --workspace` — all tests pass
- [ ] `cargo build --release --workspace` — succeeds
- [ ] No `unwrap()` or `panic!()` in non-test code
- [ ] Error messages are user-friendly (not raw `Debug` output)
- [ ] `README.md` updated with build/run instructions

---

## Verification

### E2E test checklist
- [ ] Hub + 2 joiners: bidirectional message exchange works
- [ ] Hub + 3 joiners: all messages reach all peers
- [ ] Joiner disconnect → remaining peers see leave notification
- [ ] Hub disconnect → all joiners see room closed
- [ ] Invite code reuse → second joiner rejected
- [ ] Expired invite → joiner rejected with clear message

### Error resilience
- [ ] No panics in any tested error scenario
- [ ] No resource leaks (checked via `lsof` or similar)
- [ ] Terminal always restored after any exit path
- [ ] App exits with non-zero code on startup failure

### Performance
- [ ] Bootstrap completes in <30s on typical connection
- [ ] Message latency <5s hub→joiner on typical connection
- [ ] Memory usage stable over 10+ minute session
- [ ] CPU usage minimal when idle (no busy loops)

### Build integrity
- [ ] `cargo clean && cargo build --release --workspace` — succeeds
- [ ] Binary size reasonable for a Rust + Tor app
- [ ] `cargo tree` — no unexpected dependencies

### Documentation
- [ ] `README.md` has build instructions
- [ ] `README.md` has usage examples for `host` and `join`
- [ ] `README.md` explains invite code flow
- [ ] Inline docs (`///`) on all public API items

---

## Acceptance criteria

- [ ] All verification checks above pass
- [ ] `cargo clippy --workspace -- -D warnings` — clean
- [ ] Hub + multiple joiners works reliably
- [ ] All error scenarios handled gracefully
- [ ] Edge cases don't cause panics or data corruption
- [ ] Release build succeeds
- [ ] Documentation is complete
