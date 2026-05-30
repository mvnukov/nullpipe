//! Length-prefixed wire protocol for chat messages.
//!
//! Wire format:
//!   [4-byte big-endian u32 payload length][UTF-8 JSON payload]
//!
//! Payload JSON schema:
//!   {"type":"chat|system|ping|pong","name":"...","text":"..."}

use serde::{Deserialize, Serialize};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWriteExt};
use tokio::time::{timeout, Duration};

use crate::error::{ChatError, Result};

/// Maximum payload size: 16 KB.
pub const MAX_PAYLOAD_BYTES: usize = 16 * 1024;

/// Wire-level message type.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum MessageType {
    /// Regular chat message.
    Chat,
    /// System / meta message.
    System,
    /// Liveness ping.
    Ping,
    /// Liveness pong.
    Pong,
}

/// Wire-level message exchanged between peers and hub.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WireMessage {
    #[serde(rename = "type")]
    pub kind: MessageType,
    #[serde(default)]
    pub name: String,
    #[serde(default)]
    pub text: String,
}

impl WireMessage {
    /// Create a chat message.
    pub fn chat(name: &str, text: &str) -> Self {
        Self {
            kind: MessageType::Chat,
            name: name.to_string(),
            text: text.to_string(),
        }
    }

    /// Create a system message.
    pub fn system(text: &str) -> Self {
        Self {
            kind: MessageType::System,
            name: String::new(),
            text: text.to_string(),
        }
    }

    /// Create a ping.
    pub fn ping() -> Self {
        Self {
            kind: MessageType::Ping,
            name: String::new(),
            text: String::new(),
        }
    }

    /// Create a pong.
    pub fn pong() -> Self {
        Self {
            kind: MessageType::Pong,
            name: String::new(),
            text: String::new(),
        }
    }
}

/// Encode a `WireMessage` into length-prefixed bytes.
///
/// Returns the 4-byte header + JSON payload.
/// Returns an error if the JSON payload exceeds `MAX_PAYLOAD_BYTES`.
pub fn encode_message(msg: &WireMessage) -> Result<Vec<u8>> {
    let payload =
        serde_json::to_vec(msg).map_err(|e| ChatError::Wire(format!("json encode failed: {e}")))?;

    if payload.len() > MAX_PAYLOAD_BYTES {
        return Err(ChatError::OversizedMessage {
            size: payload.len(),
            limit: MAX_PAYLOAD_BYTES,
        });
    }

    let len = (payload.len() as u32).to_be_bytes();
    let mut out = Vec::with_capacity(4 + payload.len());
    out.extend_from_slice(&len);
    out.extend_from_slice(&payload);
    Ok(out)
}

/// Decode a length-prefixed message from raw bytes.
///
/// Expects the full frame (header + payload).
pub fn decode_message(data: &[u8]) -> Result<WireMessage> {
    if data.len() < 4 {
        return Err(ChatError::Wire("payload too short for header".into()));
    }

    let header: [u8; 4] = data[..4]
        .try_into()
        .map_err(|_| ChatError::Wire("header slice failed".into()))?;
    let payload_len = u32::from_be_bytes(header) as usize;

    if payload_len == 0 {
        return Err(ChatError::Wire("zero-length payload".into()));
    }

    if payload_len > MAX_PAYLOAD_BYTES {
        return Err(ChatError::OversizedMessage {
            size: payload_len,
            limit: MAX_PAYLOAD_BYTES,
        });
    }

    if data.len() < 4 + payload_len {
        return Err(ChatError::Wire("truncated payload".into()));
    }

    let payload = &data[4..4 + payload_len];
    let msg: WireMessage = serde_json::from_slice(payload)
        .map_err(|e| ChatError::Wire(format!("json decode failed: {e}")))?;

    Ok(msg)
}

/// Read a complete length-prefixed frame from an async reader.
///
/// Handles partial reads: reads exactly 4 bytes for the header, then exactly
/// `payload_len` bytes for the payload. Returns an error on oversized payloads,
/// invalid UTF-8, or malformed JSON.
pub async fn read_frame<R: AsyncRead + Unpin>(reader: &mut R) -> Result<Vec<u8>> {
    // Read 4-byte header
    let mut header = [0u8; 4];
    reader
        .read_exact(&mut header)
        .await
        .map_err(|e| ChatError::Wire(format!("header read failed: {e}")))?;

    let payload_len = u32::from_be_bytes(header) as usize;

    if payload_len == 0 {
        return Err(ChatError::Wire("zero-length payload".into()));
    }

    if payload_len > MAX_PAYLOAD_BYTES {
        return Err(ChatError::OversizedMessage {
            size: payload_len,
            limit: MAX_PAYLOAD_BYTES,
        });
    }

    // Read payload
    let mut payload = vec![0u8; payload_len];
    reader
        .read_exact(&mut payload)
        .await
        .map_err(|e| ChatError::Wire(format!("payload read failed: {e}")))?;

    // Validate UTF-8
    std::str::from_utf8(&payload)
        .map_err(|_| ChatError::Wire("payload is not valid UTF-8".into()))?;

    let mut out = Vec::with_capacity(4 + payload_len);
    out.extend_from_slice(&header);
    out.extend_from_slice(&payload);
    Ok(out)
}

/// Read and decode a complete `WireMessage` from an async reader.
pub async fn read_message<R: AsyncRead + Unpin>(reader: &mut R) -> Result<WireMessage> {
    let frame = read_frame(reader).await?;
    decode_message(&frame)
}

/// Write a single encoded frame to an async writer with timeout protection.
///
/// Writes the frame and flushes the stream, both protected by the given timeout.
#[allow(dead_code)]

pub async fn write_frame<W>(writer: &mut W, frame: &[u8], write_timeout: Duration) -> Result<()>
where
    W: AsyncWriteExt + Unpin,
{
    timeout(write_timeout, writer.write_all(frame))
        .await
        .map_err(|_| ChatError::Connection("write timed out".into()))?
        .map_err(|e| ChatError::Connection(format!("write failed: {e}")))?;

    timeout(write_timeout, writer.flush())
        .await
        .map_err(|_| ChatError::Connection("flush timed out".into()))?
        .map_err(|e| ChatError::Connection(format!("flush failed: {e}")))?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::BufReader;

    // ---- encode / decode round-trip ----

    #[test]
    fn roundtrip_chat() {
        let msg = WireMessage::chat("alice", "hello world");
        let encoded = encode_message(&msg).unwrap();
        let decoded = decode_message(&encoded).unwrap();
        assert_eq!(decoded.kind, MessageType::Chat);
        assert_eq!(decoded.name, "alice");
        assert_eq!(decoded.text, "hello world");
    }

    #[test]
    fn roundtrip_system() {
        let msg = WireMessage::system("peer joined");
        let encoded = encode_message(&msg).unwrap();
        let decoded = decode_message(&encoded).unwrap();
        assert_eq!(decoded.kind, MessageType::System);
        assert_eq!(decoded.text, "peer joined");
    }

    #[test]
    fn roundtrip_ping_pong() {
        let ping = WireMessage::ping();
        let pong = WireMessage::pong();
        assert_eq!(
            decode_message(&encode_message(&ping).unwrap())
                .unwrap()
                .kind,
            MessageType::Ping
        );
        assert_eq!(
            decode_message(&encode_message(&pong).unwrap())
                .unwrap()
                .kind,
            MessageType::Pong
        );
    }

    #[test]
    fn roundtrip_unicode() {
        let msg = WireMessage::chat("🌍", "你好世界 🎉");
        let encoded = encode_message(&msg).unwrap();
        let decoded = decode_message(&encoded).unwrap();
        assert_eq!(decoded.name, "🌍");
        assert_eq!(decoded.text, "你好世界 🎉");
    }

    #[test]
    fn header_is_big_endian() {
        let msg = WireMessage::chat("a", "x");
        let encoded = encode_message(&msg).unwrap();
        let payload = serde_json::to_vec(&msg).unwrap();
        let expected_len = (payload.len() as u32).to_be_bytes();
        assert_eq!(&encoded[..4], &expected_len);
    }

    // ---- error cases ----

    #[test]
    fn oversized_encode_rejected() {
        let big = "x".repeat(MAX_PAYLOAD_BYTES + 1);
        let msg = WireMessage::chat("a", &big);
        let err = encode_message(&msg).unwrap_err();
        assert!(matches!(err, ChatError::OversizedMessage { .. }));
    }

    #[test]
    fn oversized_decode_rejected() {
        let big_len = (MAX_PAYLOAD_BYTES as u32 + 1).to_be_bytes();
        let err = decode_message(&big_len).unwrap_err();
        assert!(matches!(err, ChatError::OversizedMessage { .. }));
    }

    #[test]
    fn short_data_rejected() {
        let err = decode_message(&[1, 2]).unwrap_err();
        assert!(matches!(err, ChatError::Wire(_)));
    }

    #[test]
    fn malformed_json_rejected() {
        let payload = b"not json";
        let len = (payload.len() as u32).to_be_bytes();
        let mut data = len.to_vec();
        data.extend_from_slice(payload);
        let err = decode_message(&data).unwrap_err();
        assert!(matches!(err, ChatError::Wire(_)));
    }

    #[test]
    fn truncated_payload_rejected() {
        let payload = serde_json::to_vec(&WireMessage::chat("a", "hello")).unwrap();
        let len = (payload.len() as u32).to_be_bytes();
        let mut data = len.to_vec();
        data.extend_from_slice(&payload[..5]); // truncated
        let err = decode_message(&data).unwrap_err();
        assert!(matches!(err, ChatError::Wire(_)));
    }

    #[test]
    fn zero_length_rejected() {
        let len = 0u32.to_be_bytes();
        let err = decode_message(&len).unwrap_err();
        assert!(matches!(err, ChatError::Wire(_)));
    }

    // ---- async read frame ----

    #[tokio::test]
    async fn read_frame_complete() {
        let msg = WireMessage::chat("bob", "async test");
        let encoded = encode_message(&msg).unwrap();
        let mut reader = BufReader::new(encoded.as_slice());
        let frame = read_frame(&mut reader).await.unwrap();
        let decoded = decode_message(&frame).unwrap();
        assert_eq!(decoded.name, "bob");
        assert_eq!(decoded.text, "async test");
    }

    #[tokio::test]
    async fn write_frame_roundtrip() {
        let msg = WireMessage::chat("alice", "hello");
        let encoded = encode_message(&msg).unwrap();

        let (mut writer, mut reader) = tokio::io::duplex(64);
        write_frame(&mut writer, &encoded, Duration::from_secs(5)).await.unwrap();

        let frame = read_frame(&mut reader).await.unwrap();
        let decoded = decode_message(&frame).unwrap();
        assert_eq!(decoded.name, "alice");
        assert_eq!(decoded.text, "hello");
    }
}
