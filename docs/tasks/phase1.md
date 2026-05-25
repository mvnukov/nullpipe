# Phase 1 — Core: data types and invite codes

Define core types and implement invite code encode/decode with round-trip tests.

---

## 1.1 Core types

- [x] `PeerId` — opaque identifier (base58 of public key)
- [x] `PeerInfo` — `{ id: PeerId, name: String, joined_at: Instant }`
- [x] `ChatEvent` — union: `Message`, `PeerJoin`, `PeerLeave`, `BootstrapProgress`, `RoomReady`, `Error`
- [x] `ChatError` — typed errors via `thiserror`
- [x] `HostConfig` — `{ name, invite_ttl_secs }`
- [x] `JoinConfig` — `{ name, invite_code }`

## 1.2 Invite code

- [x] `InvitePayload` struct: `{ onion_address: String, nonce: [u8; 16], timestamp: u64 }`
- [x] `encode()`: serialize → dot-join fields → base58 → single token (~120 chars)
- [x] `decode()`: base58 → split → parse → validate format
- [x] Validation: onion address ends with `.onion`, nonce length, timestamp not too far in future

## 1.3 Tests

- [x] Round-trip: encode → decode → equals original
- [x] Invalid base58 → error
- [x] Malformed payload → error
- [x] Expired invite → error

---

## Verification

### Compilation checks
- [x] `cargo check --package ephemeral-chat-core` — zero errors
- [x] `cargo check --workspace` — zero errors (cli still compiles against updated core)
- [x] `cargo clippy --package ephemeral-chat-core -- -D warnings` — zero lints
- [x] No `unwrap()` in production code (only in tests)

### Type correctness
- [x] `PeerId` derives `Clone`, `Debug`, `PartialEq`, `Eq`, `Hash`
- [x] `PeerInfo` derives `Clone`, `Debug`
- [x] `ChatEvent` derives `Debug`
- [x] `ChatError` derives `Error`, `Debug`
- [x] All public types are `pub` and accessible from crate root
- [x] `ChatError` variants cover all failure modes in the plan
- [x] `Result<T>` type alias exported at crate root
- [x] `HostConfig` and `JoinConfig` derive `Clone`, `Debug`

### Invite code — encoding
- [x] Token is a single contiguous string (no whitespace, no delimiters in output)
- [x] Token decodes via base58 without errors
- [x] Token contains onion address, nonce, timestamp
- [x] Token length is reasonable (~80–160 chars for a 56-char onion v3 address)
- [x] Different inputs produce different tokens (nonce variation)
- [x] Encoding is deterministic for same input

### Invite code — decoding
- [x] Valid token → `InvitePayload` with correct fields
- [x] `.onion` suffix validation: rejects addresses without it
- [x] Nonce is exactly 16 bytes after decode
- [x] Timestamp is parsed as `u64`
- [x] Rejects tokens that are too short to be valid
- [x] Rejects tokens with invalid base58 characters
- [x] Rejects empty string input

### Invite code — validation
- [x] Future timestamp tolerance: accepts invites with timestamp within configurable window (e.g. 300s clock skew)
- [x] Expired invite (timestamp + ttl < now) → `ChatError::InviteExpired`
- [x] TTL of 0 means no expiry check
- [x] Invalid onion address format → `ChatError::InvalidInvite`

### Tests
- [x] Round-trip: `decode(encode(payload)) == payload` for valid inputs
- [x] Round-trip with minimum onion address (v3: 56 chars)
- [x] Round-trip with varying timestamps (past, now, near-future)
- [x] Round-trip with varying nonces (all zeros, all 0xFF, random)
- [x] Invalid base58 string → `ChatError::InvalidInvite`
- [x] Base58 of non-invite data (too short) → `ChatError::InvalidInvite`
- [x] Malformed decoded payload (missing fields) → `ChatError::InvalidInvite`
- [x] Expired invite decode with TTL check → `ChatError::InviteExpired`
- [x] Future timestamp beyond skew window → `ChatError::InvalidInvite`
- [x] `cargo test --package ephemeral-chat-core` — 18 unit tests pass
- [x] `cargo test --package ephemeral-chat-core -- --test-threads=1` — no ordering-dependent failures

### Formatting checks
- [x] `cargo fmt -- --check` — passes
- [x] `cargo fmt --package ephemeral-chat-core -- --check` — passes

### Build integrity
- [x] `cargo clean && cargo build --package ephemeral-chat-core` — succeeds
- [x] `cargo clean && cargo build --workspace` — succeeds
- [x] `cargo test --package ephemeral-chat-core` — passes on clean build

---

## Acceptance criteria

- [x] All verification checks above pass
- [x] `cargo test --package ephemeral-chat-core` — all tests green
- [x] `cargo clippy --package ephemeral-chat-core -- -D warnings` — clean
- [x] Invite code round-trip is lossless and deterministic
- [x] No panics in any test or production path
- [x] Core crate builds standalone without cli dependency
