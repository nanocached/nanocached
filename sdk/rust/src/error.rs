use std::fmt;

/// Every error this SDK returns on its own behalf.
///
/// `Clone` so a failed reconnect dial's error can be cached (per-address
/// reconnect cooldown, `Options::reconnect_cooldown`) and handed back
/// verbatim to every caller that hits the cooldown window, instead of
/// only the dial's own original caller ever seeing it.
#[derive(Debug, Clone)]
pub enum Error {
    /// get/set/delete after `close()`; `close()` itself is idempotent.
    AlreadyClosed,
    /// A node answered `W` (staged node join): per its own view of cluster
    /// membership it doesn't hold this key — the caller's routing table
    /// is stale. The client catches this internally to refresh the node
    /// list and retry once; it only escapes when that retry also fails,
    /// or in single-node mode where there is no discovery to refresh
    /// from.
    WrongNode,
    /// A discovery server answered `L` with `B` — it is inside its
    /// startup grace (discovery HA), re-learning membership after a restart.
    /// Try another address, or retry shortly.
    DiscoveryBusy,
    /// A connection-level failure; the client redials lazily on the next
    /// use, and in cluster mode retries once through a node-list refresh.
    ConnectionLost(String),
    /// The server rejected the `A` handshake's secret — either the server
    /// requires one and none was configured, or the configured one is
    /// wrong. Never transient: retrying with the same configuration
    /// cannot succeed (issue #47), unlike `ConnectionLost`, which a redial
    /// may recover from. Distinct from `Protocol`, which is reserved for
    /// the server sending something the wire protocol doesn't allow at
    /// all, not a well-formed rejection of credentials.
    Authentication(String),
    /// The server said something the protocol doesn't allow.
    Protocol(String),
    /// The caller passed something that could never be meant.
    InvalidArgument(String),
    /// `get` decodes the value as UTF-8 with a strict decoder; a value
    /// that isn't valid UTF-8 raises this instead of lossily replacing
    /// the invalid bytes. Use `get_bytes` for the raw value.
    InvalidUtf8(std::string::FromUtf8Error),
    /// `get`/`get_bytes` when a value with `compress` enabled can't be
    /// interpreted — almost always a `compress` mismatch between clients
    /// sharing this key (value compression's compatibility caveat: every
    /// client touching a given keyspace must agree on `compress`), not a
    /// transient failure.
    #[cfg(feature = "compression")]
    Decompression(String),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::AlreadyClosed => write!(f, "nanocached: this client is closed"),
            Error::WrongNode => {
                write!(f, "nanocached: this node does not hold the requested key")
            }
            Error::DiscoveryBusy => write!(
                f,
                "nanocached: the discovery server is warming up after a restart"
            ),
            Error::ConnectionLost(message)
            | Error::Protocol(message)
            | Error::Authentication(message) => write!(f, "{message}"),
            Error::InvalidArgument(message) => write!(f, "{message}"),
            Error::InvalidUtf8(error) => {
                write!(f, "nanocached: value is not valid UTF-8: {error}")
            }
            #[cfg(feature = "compression")]
            Error::Decompression(message) => write!(f, "{message}"),
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Error::InvalidUtf8(error) => Some(error),
            _ => None,
        }
    }
}

impl From<std::io::Error> for Error {
    fn from(error: std::io::Error) -> Self {
        Error::ConnectionLost(format!("nanocached: connection failed: {error}"))
    }
}

pub type Result<T> = std::result::Result<T, Error>;
