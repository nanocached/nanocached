use bytes::Bytes;
use std::time::Duration;

#[derive(Debug, PartialEq, Eq)]
pub enum Response {
    Value(Bytes),
    Stored,
    Deleted,
    NotFound,
    Busy,
    AuthOk,
    Unauthorized,
    /// ADR-0008: acknowledges an `M` (migrate) request was received and
    /// parsed — not that the handoff it kicks off has finished. That
    /// completion is reported separately, node-to-discovery, via `C`.
    MigrationAccepted,
    /// ADR-0008: acknowledges an `X` (cancel migration) request was
    /// received and parsed — not that any in-progress handoff it names
    /// was actually found and aborted (a cancel for an already-finished
    /// or never-started handoff is a safe no-op on the node's side).
    MigrationCancelled,
    /// ADR-0008: this node no longer (or not yet) owns the key a `G`/`S`/
    /// `D` named, per this node's own current view of cluster membership
    /// (see `NodeContext::known_ring`) — the client's view of `L` is
    /// stale. Carries no forwarding address; the client is expected to
    /// re-fetch `L` from discovery and recompute where the key belongs,
    /// not trust this node to know or proxy the request.
    WrongNode,
    /// Internal-only (ADR-0008), in answer to `Command::ListEntries` —
    /// never encoded for a wire client, see `encode`.
    Entries(Vec<(Bytes, Bytes, Option<Duration>)>),
    /// Internal-only (ADR-0008), in answer to `Command::MarkMigrated`.
    Marked,
    /// Internal-only (ADR-0008), in answer to `Command::UnmarkMigrated`.
    Unmarked,
    /// Internal-only (ADR-0008), in answer to `Command::Sweep` — how many
    /// entries the sweep actually removed.
    Swept(usize),
}

impl Response {
    pub fn encode(&self) -> Vec<u8> {
        match self {
            Self::Stored => b"S\n".to_vec(),
            Self::Deleted => b"D\n".to_vec(),
            Self::NotFound => b"N\n".to_vec(),
            Self::Busy => b"B\n".to_vec(),
            Self::AuthOk => b"On\n".to_vec(),
            Self::Unauthorized => b"En\n".to_vec(),
            Self::MigrationAccepted => b"A\n".to_vec(),
            Self::MigrationCancelled => b"A\n".to_vec(),
            Self::WrongNode => b"W\n".to_vec(),

            Self::Value(value) => {
                let length = value.len().to_string();

                let mut encoded = Vec::with_capacity(2 + length.len() + 1 + value.len());

                encoded.extend_from_slice(b"V ");
                encoded.extend_from_slice(length.as_bytes());
                encoded.push(b'\n');
                encoded.extend_from_slice(value);

                encoded
            }

            Self::Entries(_) | Self::Marked | Self::Unmarked | Self::Swept(_) => {
                unreachable!(
                    "internal-only response (ADR-0008): never sent to a wire client, only \
                     matched directly in Rust by the migration task"
                )
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn encodes_stored_response() {
        assert_eq!(Response::Stored.encode(), b"S\n");
    }

    #[test]
    fn encodes_deleted_response() {
        assert_eq!(Response::Deleted.encode(), b"D\n");
    }

    #[test]
    fn encodes_not_found_response() {
        assert_eq!(Response::NotFound.encode(), b"N\n");
    }

    #[test]
    fn encodes_busy_response() {
        assert_eq!(Response::Busy.encode(), b"B\n");
    }

    #[test]
    fn encodes_auth_ok_response() {
        assert_eq!(Response::AuthOk.encode(), b"On\n");
    }

    #[test]
    fn encodes_unauthorized_response() {
        assert_eq!(Response::Unauthorized.encode(), b"En\n");
    }

    #[test]
    fn encodes_migration_accepted_response() {
        assert_eq!(Response::MigrationAccepted.encode(), b"A\n");
    }

    #[test]
    fn encodes_wrong_node_response() {
        assert_eq!(Response::WrongNode.encode(), b"W\n");
    }

    #[test]
    fn encodes_value_response() {
        let response = Response::Value(Bytes::from_static(b"Alice"));

        assert_eq!(response.encode(), b"V 5\nAlice");
    }

    #[test]
    fn encodes_binary_value_response() {
        let response = Response::Value(Bytes::from(vec![0xff, 0x00, b'\r', b'\n']));

        assert_eq!(response.encode(), b"V 4\n\xff\x00\r\n",);
    }
}
