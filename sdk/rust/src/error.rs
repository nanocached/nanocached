use std::fmt;

/// Every error this SDK returns on its own behalf.
#[derive(Debug)]
pub enum Error {
    /// get/set/delete after `close()`; `close()` itself is idempotent.
    AlreadyClosed,
    /// A node answered `W` (ADR-0008): per its own view of cluster
    /// membership it doesn't hold this key — the caller's routing table
    /// is stale. The client catches this internally to refresh the node
    /// list and retry once; it only escapes when that retry also fails,
    /// or in single-node mode where there is no discovery to refresh
    /// from.
    WrongNode,
    /// A discovery server answered `L` with `B` — it is inside its
    /// startup grace (ADR-0010), re-learning membership after a restart.
    /// Try another address, or retry shortly.
    DiscoveryBusy,
    /// A connection-level failure; the client redials lazily on the next
    /// use, and in cluster mode retries once through a node-list refresh.
    ConnectionLost(String),
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
    /// sharing this key (doc/adr/0013-*.md's compatibility caveat: every
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
            Error::ConnectionLost(message) | Error::Protocol(message) => write!(f, "{message}"),
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
