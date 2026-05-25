# TOR POLICY — READ BEFORE RUNNING ANY TESTS

## TOR NETWORK IS AVAILABLE

**The Tor network is available in this environment. All e2e and integration tests MUST run against the real Tor network.**

## RULES FOR AGENTS

1. **NEVER skip e2e tests.** They run on real Tor via Arti (pure-Rust, embedded).
2. **NEVER add `#[ignore]` to any test.** The previously-ignored test has been un-ignored.
3. **NEVER mock Tor.** Arti bootstraps against real Tor directory authorities. This is the correct behavior.
4. **NEVER claim "Tor unavailable" or "requires Tor network" as a reason to skip.** Tor IS available.
5. **Run `cargo test --workspace`** — all tests execute. No exceptions.
6. **Bootstrap takes 30-90s.** This is normal. Do not "optimize" it away.

## WHERE TOR TESTS LIVE

- `crates/core/tests/e2e.rs` — high-level host/join flow tests
- `crates/core/tests/integration.rs` — lower-level Arti/bootstrap/hub tests

## WHAT "REQUIRES TOR NETWORK" MEANS IN COMMENTS

Comments like `// E2E tests (require Tor network)` mean the tests connect to real Tor. They do NOT mean "skip these tests." They mean "these tests need Tor, and Tor IS available, so RUN THEM."

## SUMMARY

Tor = available. Tests = run. No skipping. No mocking. No ignoring.
