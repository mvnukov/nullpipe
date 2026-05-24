use thiserror::Error;

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

    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    #[error("peer not found: {0}")]
    PeerNotFound(String),

    #[error("shutdown in progress")]
    ShuttingDown,
}

pub type Result<T> = std::result::Result<T, ChatError>;
