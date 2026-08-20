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
    /// Carries how many of this node's entries it's the designated
    /// sender for (doc/adr/0017-*.md) — a one-off count taken before any
    /// transfer starts, purely so discovery can size its migration
    /// timeout, not a transfer plan.
    MigrationAccepted(usize),
    /// An `M` arrived while this node's single migration slot was already
    /// occupied by another active (not merely stale-and-unswept) handoff
    /// for a *different* `joining_name` — see `MigrationGuard::new`. A
    /// retry of `M` for the SAME `joining_name` (the expected way to hit
    /// an occupied slot — a discovery retry after a lost ack, see
    /// `send_migrate_with_retry`) is instead answered idempotently with a
    /// repeated `MigrationAccepted` carrying the same entry count, once
    /// the original `M` has computed it; only the brief window before
    /// that count is available still answers a same-name retry with this
    /// rejection. Deliberately doesn't parse as `MigrationAccepted`'s `A
    /// <entries>\n` ack, so `send_migrate`
    /// (`src/bin/nanocached-discovery.rs`) treats it as a failed send and
    /// retries via the existing `send_migrate_with_retry` path instead of
    /// being told the handoff started when it never did.
    MigrationRejected,
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
            Self::MigrationAccepted(entries) => format!("A {entries}\n").into_bytes(),
            Self::MigrationRejected => b"R\n".to_vec(),
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
    fn encodes_migration_accepted_response_with_its_entry_count() {
        assert_eq!(Response::MigrationAccepted(0).encode(), b"A 0\n");
        assert_eq!(Response::MigrationAccepted(42).encode(), b"A 42\n");
    }

    #[test]
    fn encodes_migration_rejected_response() {
        assert_eq!(Response::MigrationRejected.encode(), b"R\n");
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
