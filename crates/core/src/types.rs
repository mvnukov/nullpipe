use serde::{Deserialize, Serialize};
use std::time::Instant;

use crate::error::ChatError;

/// Opaque peer identifier (base58-encoded public key).
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct PeerId(pub String);

/// Metadata about a connected peer.
#[derive(Clone, Debug)]
pub struct PeerInfo {
    pub id: PeerId,
    pub name: String,
    pub joined_at: Instant,
}

/// Events emitted by the chat room.
#[derive(Debug)]
pub enum ChatEvent {
    /// A chat message from a peer.
    Message {
        from: PeerId,
        name: String,
        text: String,
    },
    /// A peer joined the room.
    PeerJoin(PeerInfo),
    /// A peer left the room.
    PeerLeave(PeerId),
    /// Tor bootstrap progress (0–100).
    BootstrapProgress(u8),
    /// Room is ready with the given onion address.
    RoomReady { onion_address: String, port: u16 },
    /// An error occurred.
    Error(ChatError),
}

/// Configuration for hosting a room.
#[derive(Clone, Debug)]
pub struct HostConfig {
    pub name: String,
    pub invite_ttl_secs: u64,
}

/// Configuration for joining a room.
#[derive(Clone, Debug)]
pub struct JoinConfig {
    pub name: String,
    pub invite_code: String,
}
