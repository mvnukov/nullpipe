# Phase 0 — Project setup

Create the Rust workspace, two crates, dependency wiring, and tooling config.

---

## 0.1 Initialize workspace root

- [x] Create workspace `Cargo.toml` with `members = ["crates/core", "crates/cli"]` and `resolver = "2"`
- [x] Create crate directories: `crates/core/src`, `crates/cli/src`
- [x] Verify directory structure:
  ```
  nullpipe/
  ├── Cargo.toml              # workspace root
  ├── crates/
  │   ├── core/
  │   │   ├── Cargo.toml
  │   │   └── src/
  │   │       └── lib.rs
  │   └── cli/
  │       ├── Cargo.toml
  │       └── src/
  │           └── main.rs
  ├── .cargo/config.toml
  ├── rustfmt.toml
  └── .gitignore
  ```

## 0.2 Core library crate

- [x] Write `crates/core/Cargo.toml`
  - Package: `ephemeral-chat-core`, edition 2021
  - Deps: `tokio` (full), `serde` (derive), `serde_json`, `base58`, `rand`, `thiserror`, `sha2`, `tracing`
  - Note: `arti-client`/`arti-hsconfig` added in Phase 2
- [x] Write `crates/core/src/lib.rs` — `pub mod types;` placeholder
- [x] Write `crates/core/src/types.rs` — `pub struct PeerId(pub String);` placeholder

## 0.3 CLI binary crate

- [x] Write `crates/cli/Cargo.toml`
  - Package: `ephemeral-chat`, edition 2021
  - Binary: `name = "chat"`, `path = "src/main.rs"`
  - Deps: `ephemeral-chat-core` (path), `tokio` (full), `ratatui`, `crossterm`, `clap` (derive), `dirs`
- [x] Write `crates/cli/src/main.rs` — `fn main() { println!("ephemeral-chat v0.1.0"); }`

## 0.4 Tooling

- [x] `rustfmt.toml`: edition 2021, max_width 100
- [x] `.cargo/config.toml`: incremental = true
- [x] `.gitignore`: `target/`, `Cargo.lock`, `*.rs.bk`

## 0.5 Verify build

### Compilation checks
- [x] `cargo check --workspace` — zero errors, zero warnings
- [x] `cargo build --workspace` — produces `target/debug/chat` binary
- [x] `cargo run --package ephemeral-chat` — prints "ephemeral-chat v0.1.0"

### Structure checks
- [x] `cargo metadata --no-deps --format-version 1` — lists both members
- [x] Core crate is lib-only (no `[[bin]]` section)
- [x] CLI crate has `[[bin]]` with `name = "chat"`
- [x] CLI crate depends on core via `path = "../core"`

### Formatting checks
- [x] `cargo fmt -- --check` — all code passes fmt
- [x] `cargo clippy --workspace` — zero lints

### Build integrity
- [x] Clean build from scratch: `cargo clean && cargo build --workspace` succeeds
- [x] Incremental build: second `cargo build --workspace` finishes <1s
- [x] Cross-check: `cargo check --package ephemeral-chat-core` works standalone
- [x] Cross-check: `cargo check --package ephemeral-chat` works standalone

---

## Acceptance criteria

- [x] `cargo check --workspace` succeeds
- [x] `cargo build --workspace` succeeds
- [x] `crates/core/src/lib.rs` exists as library entry
- [x] `crates/cli/src/main.rs` exists as binary entry
- [x] `chat` binary runnable (prints version)
- [x] `rustfmt.toml` present
- [x] `.gitignore` covers `target/` and `Cargo.lock`
