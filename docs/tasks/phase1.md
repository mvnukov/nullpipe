# Phase 1 — Core: data types and invite codes

Define core types and implement invite code encode/decode with round-trip tests.

---

## 1.1 Core types

- [ ] `PeerId` — opaque identifier (base58 of public key)
- [ ] `PeerInfo` — `{ id: PeerId, name: String, joined_at: Instant }`
- [ ] `ChatEvent` — union: `Message`, `PeerJoin`, `PeerLeave`, `BootstrapProgress`, `RoomReady`, `Error`
- [ ] `ChatError` — typed errors via `thiserror`
- [ ] `HostConfig` — `{ name, invite_ttl_secs }`
- [ ] `JoinConfig` — `{ name, invite_code }`

## 1.2 Invite code

- [ ] `InvitePayload` struct: `{ onion_address: String, nonce: [u8; 16], timestamp: u64 }`
- [ ] `encode()`: serialize → dot-join fields → base58 → single token (~120 chars)
- [ ] `decode()`: base58 → split → parse → validate format
- [ ] Validation: onion address ends with `.onion`, nonce length, timestamp not too far in future

## 1.3 Tests

- [ ] Round-trip: encode → decode → equals original
- [ ] Invalid base58 → error
- [ ] Malformed payload → error
- [ ] Expired invite → error

---

## Verification

### Compilation checks
- [ ] `cargo check --package ephemeral-chat-core` — zero errors
- [ ] `cargo check --workspace` — zero errors (cli still compiles against updated core)
- [ ] `cargo clippy --package ephemeral-chat-core -- -D warnings` — zero lints
- [ ] No `unwrap()` in production code (only in tests)

### Type correctness
- [ ] `PeerId` derives `Clone`, `Debug`, `PartialEq`, `Eq`, `Hash`
- [ ] `PeerInfo` derives `Clone`, `Debug`
- [ ] `ChatEvent` derives `Debug`
- [ ] `ChatError` derives `Error`, `Debug`
- [ ] All public types are `pub` and accessible from crate root
- [ ] `ChatError` variants cover all failure modes in the plan
- [ ] `Result<T>` type alias exported at crate root
- [ ] `HostConfig` and `JoinConfig` derive `Clone`, `Debug`

### Invite code — encoding
- [ ] Token is a single contiguous string (no whitespace, no delimiters in output)
- [ ] Token decodes via base58 without errors
- [ ] Token contains onion address, nonce, timestamp
- [ ] Token length is reasonable (~80–160 chars for a 56-char onion v3 address)
- [ ] Different inputs produce different tokens (nonce variation)
- [ ] Encoding is deterministic for same input

### Invite code — decoding
- [ ] Valid token → `InvitePayload` with correct fields
- [ ] `.onion` suffix validation: rejects addresses without it
- [ ] Nonce is exactly 16 bytes after decode
- [ ] Timestamp is parsed as `u64`
- [ ] Rejects tokens that are too short to be valid
- [ ] Rejects tokens with invalid base58 characters
- [ ] Rejects empty string input

### Invite code — validation
- [ ] Future timestamp tolerance: accepts invites with timestamp within configurable window (e.g. 300s clock skew)
- [ ] Expired invite (timestamp + ttl < now) → `ChatError::InviteExpired`
- [ ] TTL of 0 means no expiry check
- [ ] Invalid onion address format → `ChatError::InvalidInvite`

### Tests
- [ ] Round-trip: `decode(encode(payload)) == payload` for valid inputs
- [ ] Round-trip with minimum onion address (v3: 56 chars)
- [ ] Round-trip with varying timestamps (past, now, near-future)
- [ ] Round-trip with varying nonces (all zeros, all 0xFF, random)
- [ ] Invalid base58 string → `ChatError::InvalidInvite`
- [ ] Base58 of non-invite data (too short) → `ChatError::InvalidInvite`
- [ ] Malformed decoded payload (missing fields) → `ChatError::InvalidInvite`
- [ ] Expired invite decode with TTL check → `ChatError::InviteExpired`
- [ ] Future timestamp beyond skew window → `ChatError::InvalidInvite`
- [ ] `cargo test --package ephemeral-chat-core` — all tests pass
- [ ] `cargo test --package ephemeral-chat-core -- --test-threads=1` — no ordering-dependent failures

### Formatting checks
- [ ] `cargo fmt -- --check` — passes
- [ ] `cargo fmt --package ephemeral-chat-core -- --check` — passes

### Build integrity
- [ ] `cargo clean && cargo build --package ephemeral-chat-core` — succeeds
- [ ] `cargo clean && cargo build --workspace` — succeeds
- [ ] `cargo test --package ephemeral-chat-core` — passes on clean build

---

## Acceptance criteria

- [ ] All verification checks above pass
- [ ] `cargo test --package ephemeral-chat-core` — all tests green
- [ ] `cargo clippy --package ephemeral-chat-core -- -D warnings` — clean
- [ ] Invite code round-trip is lossless and deterministic
- [ ] No panics in any test or production path
- [ ] Core crate builds standalone without cli dependency
