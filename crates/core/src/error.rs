use thiserror::Error;

/// Errors that can occur during chat operations.
///
/// Covers invite validation, connection failures, wire protocol errors,
/// and lifecycle events like shutdown.
#[derive(Error, Debug)]
pub enum ChatError {
    #[error("invite code invalid: {0}")]
    InvalidInvite(String),

    #[error("invite expired (issued at {timestamp})")]
    InviteExpired { timestamp: u64 },

    #[error("nonce already used")]
    NonceReused,

    #[error("onion service error: {0}")]
    OnionService(String),

    #[error("connection failed: {0}")]
    Connection(String),

    #[error("broadcast channel closed")]
    ChannelClosed,

    #[error("wire protocol error: {0}")]
    Wire(String),

    #[error("message too large: {size} bytes (limit {limit})")]
    OversizedMessage { size: usize, limit: usize },

    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    #[error("peer not found: {0}")]
    PeerNotFound(String),

    #[error("shutdown in progress")]
    ShuttingDown,
}

impl Clone for ChatError {
    fn clone(&self) -> Self {
        match self {
            ChatError::InvalidInvite(s) => ChatError::InvalidInvite(s.clone()),
            ChatError::InviteExpired { timestamp } => ChatError::InviteExpired {
                timestamp: *timestamp,
            },
            ChatError::NonceReused => ChatError::NonceReused,
            ChatError::OnionService(s) => ChatError::OnionService(s.clone()),
            ChatError::Connection(s) => ChatError::Connection(s.clone()),
            ChatError::ChannelClosed => ChatError::ChannelClosed,
            ChatError::Wire(s) => ChatError::Wire(s.clone()),
            ChatError::OversizedMessage { size, limit } => ChatError::OversizedMessage {
                size: *size,
                limit: *limit,
            },
            ChatError::Io(e) => ChatError::Io(std::io::Error::new(e.kind(), e.to_string())),
            ChatError::PeerNotFound(s) => ChatError::PeerNotFound(s.clone()),
            ChatError::ShuttingDown => ChatError::ShuttingDown,
        }
    }
}

/// Convenience alias for `Result<T, ChatError>`.
pub type Result<T> = std::result::Result<T, ChatError>;
