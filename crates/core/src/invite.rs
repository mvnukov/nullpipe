use base58::{FromBase58, ToBase58};
use chrono::Utc;
use serde::{Deserialize, Serialize};

use crate::error::{ChatError, Result};

/// Internal payload for an invite code.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct InvitePayload {
    /// The onion v3 address (e.g. "abc...xyz.onion").
    pub onion_address: String,
    /// Single-use nonce to prevent replay.
    pub nonce: [u8; 16],
    /// Issue time as Unix timestamp seconds.
    pub timestamp: u64,
    /// Suggested display name for the joiner.
    pub suggested_name: Option<String>,
}

/// Encode an `InvitePayload` into a single base58 token.
///
/// Serialises the three fields with colons as delimiters (address:nonce_hex:ts),
/// then base58-encodes the result into one contiguous string.
pub fn encode(payload: &InvitePayload) -> Result<String> {
    validate_address(&payload.onion_address)?;
    let nonce_hex = hex::encode(payload.nonce);
    let name_part = payload.suggested_name.as_deref().unwrap_or("");
    let joined = format!(
        "{}:{}:{}:{}",
        payload.onion_address, nonce_hex, payload.timestamp, name_part
    );
    Ok(joined.as_bytes().to_base58())
}

/// Decode a base58 token back into an `InvitePayload`.
///
/// If `ttl_secs` is `Some`, checks that the invite has not expired.
/// A `ttl_secs` of `0` means no expiry check.
/// Allows up to 300 s of clock skew into the future.
pub fn decode(token: &str, ttl_secs: Option<u64>) -> Result<InvitePayload> {
    if token.is_empty() {
        return Err(ChatError::InvalidInvite("empty token".into()));
    }

    let bytes = token
        .from_base58()
        .map_err(|e| ChatError::InvalidInvite(format!("bad base58: {e:?}")))?;

    let decoded =
        String::from_utf8(bytes).map_err(|_| ChatError::InvalidInvite("not valid UTF-8".into()))?;

    let parts: Vec<&str> = decoded.splitn(4, ':').collect();
    if parts.len() < 3 || parts.len() > 4 {
        return Err(ChatError::InvalidInvite(
            "expected three or four colon-separated fields".into(),
        ));
    }

    let onion_address = parts[0].to_string();
    validate_address(&onion_address)?;

    let nonce_bytes = hex::decode(parts[1])
        .map_err(|_| ChatError::InvalidInvite("nonce is not valid hex".into()))?;
    if nonce_bytes.len() != 16 {
        return Err(ChatError::InvalidInvite(format!(
            "nonce must be 16 bytes, got {}",
            nonce_bytes.len()
        )));
    }
    let mut nonce = [0u8; 16];
    nonce.copy_from_slice(&nonce_bytes);

    let timestamp: u64 = parts[2]
        .parse()
        .map_err(|_| ChatError::InvalidInvite("timestamp is not a valid u64".into()))?;

    let suggested_name = if parts.len() == 4 && !parts[3].is_empty() {
        Some(parts[3].to_string())
    } else {
        None
    };

    // Clock skew tolerance: 300 s into the future.
    let now = Utc::now().timestamp() as u64;
    if timestamp > now + 300 {
        return Err(ChatError::InvalidInvite(format!(
            "timestamp {timestamp} is too far in the future (now {now})"
        )));
    }

    // Expiry check (only if TTL provided and non-zero).
    if let Some(ttl) = ttl_secs {
        if ttl > 0 && now > timestamp + ttl {
            return Err(ChatError::InviteExpired { timestamp });
        }
    }

    Ok(InvitePayload {
        onion_address,
        nonce,
        timestamp,
        suggested_name,
    })
}

fn validate_address(addr: &str) -> Result<()> {
    if !addr.ends_with(".onion") {
        return Err(ChatError::InvalidInvite(
            "address must end with .onion".into(),
        ));
    }
    if addr.len() < 56 + ".onion".len() {
        return Err(ChatError::InvalidInvite(format!(
            "address too short ({} chars)",
            addr.len()
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_payload(addr: &str, ts: u64) -> InvitePayload {
        InvitePayload {
            onion_address: addr.to_string(),
            nonce: [0x42u8; 16],
            timestamp: ts,
            suggested_name: None,
        }
    }

    // v3 onion: 56 chars + .onion
    const ONION: &str = "vww6ybal6bd7szmgncyruucpgfkqahzddi37ktceo3ah7ngmcopnpyyd.onion";

    // ---- round-trip ----

    #[test]
    fn roundtrip_basic() {
        let now = Utc::now().timestamp() as u64;
        let payload = make_payload(ONION, now);
        let token = encode(&payload).unwrap();
        let decoded = decode(&token, None).unwrap();
        assert_eq!(decoded.onion_address, payload.onion_address);
        assert_eq!(decoded.nonce, payload.nonce);
        assert_eq!(decoded.timestamp, payload.timestamp);
    }

    #[test]
    fn roundtrip_zero_nonce() {
        let payload = InvitePayload {
            onion_address: ONION.into(),
            nonce: [0u8; 16],
            timestamp: Utc::now().timestamp() as u64,
            suggested_name: None,
        };
        let token = encode(&payload).unwrap();
        let decoded = decode(&token, None).unwrap();
        assert_eq!(decoded.nonce, [0u8; 16]);
    }

    #[test]
    fn roundtrip_ff_nonce() {
        let payload = InvitePayload {
            onion_address: ONION.into(),
            nonce: [0xFFu8; 16],
            timestamp: Utc::now().timestamp() as u64,
            suggested_name: None,
        };
        let token = encode(&payload).unwrap();
        let decoded = decode(&token, None).unwrap();
        assert_eq!(decoded.nonce, [0xFFu8; 16]);
    }

    #[test]
    fn roundtrip_past_timestamp() {
        let payload = make_payload(ONION, 1_700_000_000);
        let token = encode(&payload).unwrap();
        let decoded = decode(&token, None).unwrap();
        assert_eq!(decoded.timestamp, 1_700_000_000);
    }

    // ---- encoding properties ----

    #[test]
    fn token_is_contiguous_no_whitespace() {
        let payload = make_payload(ONION, Utc::now().timestamp() as u64);
        let token = encode(&payload).unwrap();
        assert!(!token.contains(' '));
        assert!(!token.contains('.'));
        assert!(!token.contains('\n'));
    }

    #[test]
    fn token_is_deterministic() {
        let payload = make_payload(ONION, 12345);
        let t1 = encode(&payload).unwrap();
        let t2 = encode(&payload).unwrap();
        assert_eq!(t1, t2);
    }

    #[test]
    fn different_nonce_different_token() {
        let ts = Utc::now().timestamp() as u64;
        let p1 = make_payload(ONION, ts);
        let mut p2 = p1.clone();
        p2.nonce[0] = 0xFF;
        assert_ne!(encode(&p1).unwrap(), encode(&p2).unwrap());
    }

    // ---- decoding errors ----

    #[test]
    fn decode_empty_string() {
        assert!(matches!(decode("", None), Err(ChatError::InvalidInvite(_))));
    }

    #[test]
    fn decode_invalid_base58() {
        // Contains chars not in base58 alphabet (0, O, I, l)
        assert!(matches!(
            decode("0OIl", None),
            Err(ChatError::InvalidInvite(_))
        ));
    }

    #[test]
    fn decode_garbage_too_short() {
        let tiny = encode(&make_payload(ONION, 1)).unwrap();
        // Manually truncate to make it invalid
        assert!(matches!(
            decode(&tiny[..2], None),
            Err(ChatError::InvalidInvite(_))
        ));
    }

    #[test]
    fn decode_non_onion_address() {
        let payload = InvitePayload {
            onion_address: "example.com".into(),
            nonce: [0u8; 16],
            timestamp: Utc::now().timestamp() as u64,
            suggested_name: None,
        };
        assert!(matches!(encode(&payload), Err(ChatError::InvalidInvite(_))));
    }

    #[test]
    fn decode_short_address() {
        let payload = InvitePayload {
            onion_address: "x.onion".into(),
            nonce: [0u8; 16],
            timestamp: Utc::now().timestamp() as u64,
            suggested_name: None,
        };
        assert!(matches!(encode(&payload), Err(ChatError::InvalidInvite(_))));
    }

    // ---- expiry / clock skew ----

    #[test]
    fn expired_invite() {
        let old_ts = (Utc::now().timestamp() - 600) as u64; // 10 min ago
        let payload = make_payload(ONION, old_ts);
        let token = encode(&payload).unwrap();
        // TTL 300s: invite expired 300s ago
        let err = decode(&token, Some(300)).unwrap_err();
        assert!(matches!(err, ChatError::InviteExpired { .. }));
    }

    #[test]
    fn ttl_zero_skips_expiry() {
        let old_ts = (Utc::now().timestamp() - 999_999) as u64;
        let payload = make_payload(ONION, old_ts);
        let token = encode(&payload).unwrap();
        // TTL 0 = no expiry check
        assert!(decode(&token, Some(0)).is_ok());
    }

    #[test]
    fn no_ttl_skips_expiry() {
        let old_ts = (Utc::now().timestamp() - 999_999) as u64;
        let payload = make_payload(ONION, old_ts);
        let token = encode(&payload).unwrap();
        assert!(decode(&token, None).is_ok());
    }

    #[test]
    fn fresh_invite_with_ttl() {
        let now = Utc::now().timestamp() as u64;
        let payload = make_payload(ONION, now);
        let token = encode(&payload).unwrap();
        assert!(decode(&token, Some(600)).is_ok());
    }

    #[test]
    fn future_beyond_skew_rejected() {
        let far_future = (Utc::now().timestamp() + 600) as u64; // +10 min, skew is 300s
        let payload = make_payload(ONION, far_future);
        let token = encode(&payload).unwrap();
        assert!(matches!(
            decode(&token, None),
            Err(ChatError::InvalidInvite(_))
        ));
    }

    #[test]
    fn future_within_skew_accepted() {
        let near_future = (Utc::now().timestamp() + 60) as u64; // +60s < 300s skew
        let payload = make_payload(ONION, near_future);
        let token = encode(&payload).unwrap();
        assert!(decode(&token, None).is_ok());
    }
}
