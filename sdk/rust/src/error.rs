use std::collections::HashMap;
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
    /// `incr`/`decr` (issue #129) answered `T`: the key exists but its
    /// stored value isn't INCR's counter grammar (a plain signed decimal
    /// integer), or applying the delta would overflow `i64`. Never
    /// transient — retrying the same delta against the same stored value
    /// cannot succeed — unlike `ConnectionLost`, which a redial may
    /// recover from.
    NotNumeric,
    /// Retryable-error status `R` (issue #125): this one request failed
    /// transiently three times in a row (e.g. `nanocached-proxy`'s
    /// upstream node was briefly unreachable and survived its own
    /// refresh-and-retry, more than once) even after this SDK's own
    /// bounded, same-connection retry (up to 2 retries, 50ms then 100ms
    /// apart). The connection itself is fine — it was never closed or
    /// redialed to produce this error, and stays usable for whatever the
    /// caller does next. Retrying the operation again, later, is
    /// reasonable; unlike [`Self::ConnectionLost`], this SDK does not
    /// retry it a second time on its own past the bounded retry already
    /// spent producing this error.
    Retryable(String),
    /// A connection-level failure; the client redials lazily on the next
    /// use, and in cluster mode retries once through a node-list refresh.
    ConnectionLost(String),
    /// Internal-only sibling of [`Self::ConnectionLost`] (issue #225):
    /// produced solely by `Connection::single_attempt`'s post-write path,
    /// when the request's frame had already been fully written to the
    /// socket before the reply was lost — the server may or may not have
    /// already received and applied it. `get`/`set`/`delete`/`clear`
    /// (idempotent) fold this back into a plain `ConnectionLost` and keep
    /// retrying via redial exactly as before; `incr`/`decr`, the CAS
    /// methods, and `delete_if_matches` (none of which are idempotent)
    /// use it to skip that retry — replaying them could double-apply an
    /// effect the server already committed — and surface it to their own
    /// caller as a plain `ConnectionLost` instead. No public method ever
    /// returns this variant; if one does, that's a bug.
    ConnectionLostAfterSend(String),
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
    /// issue #151 — `get_many`/`get_many_bytes` when some keys are still
    /// wrong-node after the one bounded refresh-and-retry every batch
    /// gets (the per-key analogue of `get`/`get_bytes`' own `W`
    /// refresh-and-retry). Carries every key that DID resolve — a batch
    /// never fails as a whole (docs/protocol.html#multi), so a handful
    /// of stale placements shouldn't force discarding an otherwise
    /// successful batch. `set_many`/`set_many_bytes` have nothing to
    /// attach, so they return a plain [`Self::WrongNode`] on the same
    /// condition. In single-node/proxy mode a `W` propagates
    /// immediately, exactly as `get`/`get_bytes`' own single-mode
    /// behavior does — there is no ring to refresh against.
    PartialWrongNode(HashMap<String, Vec<u8>>),
    /// As [`Self::PartialWrongNode`], but returned by `get_many` — the
    /// UTF-8-decoded counterpart, produced once
    /// [`Self::PartialWrongNode`]'s own map has been decoded the same
    /// way a successful `get_many` decodes `get_many_bytes`' own result.
    PartialWrongNodeText(HashMap<String, String>),
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
                "nanocached: the discovery server is busy: warming up after a restart, or its replication factor disagrees with the cluster's"
            ),
            Error::NotNumeric => write!(
                f,
                "nanocached: the stored value is not an integer INCR can operate on"
            ),
            Error::ConnectionLost(message)
            | Error::ConnectionLostAfterSend(message)
            | Error::Protocol(message)
            | Error::Authentication(message)
            | Error::Retryable(message) => write!(f, "{message}"),
            Error::InvalidArgument(message) => write!(f, "{message}"),
            Error::InvalidUtf8(error) => {
                write!(f, "nanocached: value is not valid UTF-8: {error}")
            }
            #[cfg(feature = "compression")]
            Error::Decompression(message) => write!(f, "{message}"),
            Error::PartialWrongNode(_) | Error::PartialWrongNodeText(_) => write!(
                f,
                "nanocached: some keys in this batch are still wrong-node after a refresh-and-retry"
            ),
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
