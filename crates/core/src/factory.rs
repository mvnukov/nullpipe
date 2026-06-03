//! Unified room factory — a single entry point for hosting or joining rooms.
//!
//! Both the CLI and test harnesses use [`RoomFactory`] instead of calling
//! [`host`](crate::room::host) / [`join`](crate::room::join) directly, ensuring
//! consistent setup regardless of how a room is created.
//!
//! # Example
//!
//! ```ignore
//! use ephemeral_chat_core::factory::{RoomConfig, RoomFactory};
//!
//! let (handle, events) = RoomFactory::create(RoomConfig::Host {
//!     name: "my-room".into(),
//!     invite_ttl_secs: 300,
//! });
//! ```

use crate::room::{self, EventStream, RoomHandle};

/// Unified room creation configuration.
///
/// Replace ad‑hoc `HostConfig`/`JoinConfig` construction at call sites with
/// a single enum that makes the host/join decision explicit.
#[derive(Clone, Debug)]
pub enum RoomConfig {
    /// Host a new room.
    Host {
        /// Display name / room label.
        name: String,
        /// Seconds before the generated invite code expires.
        invite_ttl_secs: u64,
    },
    /// Join an existing room via an invite code.
    Join {
        /// Display name for this participant.
        name: String,
        /// The invite code received from the host.
        invite_code: String,
    },
}

/// Factory that turns a [`RoomConfig`] into a live room.
///
/// Stateless — currently a single function, but structured as a struct so
/// that future configuration (e.g. Tor proxy settings, timeout overrides)
/// can be added without breaking callers.
#[derive(Default)]
pub struct RoomFactory;

impl RoomFactory {
    /// Create a room from the given [`RoomConfig`].
    ///
    /// Returns a [`RoomHandle`] for sending/controlling the room and an
    /// [`EventStream`] to receive chat events.
    pub fn create(config: RoomConfig) -> (RoomHandle, EventStream) {
        match config {
            RoomConfig::Host {
                name,
                invite_ttl_secs,
            } => room::host(crate::types::HostConfig {
                name,
                invite_ttl_secs,
            }),
            RoomConfig::Join { name, invite_code } => room::join(crate::types::JoinConfig {
                name,
                invite_code,
            }),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn room_config_is_clone_debug() {
        fn assert_clone_debug<T: Clone + std::fmt::Debug>() {}
        assert_clone_debug::<RoomConfig>();
    }

    #[tokio::test]
    async fn create_with_host_config_returns_handle_and_stream() {
        let (handle, events) = RoomFactory::create(RoomConfig::Host {
            name: "test-host".into(),
            invite_ttl_secs: 300,
        });
        let _ = handle;
        let _ = events;
    }

    #[tokio::test]
    async fn create_with_join_config_returns_handle_and_stream() {
        let (handle, events) = RoomFactory::create(RoomConfig::Join {
            name: "test-joiner".into(),
            invite_code: "dummy-code".into(),
        });
        let _ = handle;
        let _ = events;
    }

    #[test]
    fn factory_is_default() {
        let _ = RoomFactory::default();
    }
}
