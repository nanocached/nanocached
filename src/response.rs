use crate::cache::CacheStats;
use crate::key::Key;
use bytes::Bytes;
use std::time::Duration;

/// Issue #129: `Response::Incremented`'s optional `<ttl-seconds>` header
/// field — a leading space plus the whole-seconds count when `ttl` is
/// `Some`, or nothing at all when it's `None` (same "present field or no
/// field" shape `S`'s own optional TTL field uses on the request side).
/// Whole seconds, rounded down — matches every other TTL this protocol
/// forwards (see `set_message`'s own rounding in `src/server.rs`).
fn ttl_field(ttl: Option<Duration>) -> String {
    ttl.map(|ttl| format!(" {}", ttl.as_secs()))
        .unwrap_or_default()
}

/// One key's outcome inside a `Response::Multi` (issues #128/#150) — the
/// per-key partial-result states a batched `m` request can answer with.
/// `WrongNode` is never produced by `Command::MultiGet`'s own `execute`
/// (which only ever sees keys the connection handler has already
/// confirmed this node owns); the handler splices it in per-key for the
/// ones it doesn't, mirroring the single-key `G`/`Response::WrongNode`
/// path instead of failing the whole frame.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MultiEntry {
    Value(Bytes),
    Miss,
    WrongNode,
}

/// One key's outcome inside a `Response::MultiAck` (issue #150) — `o`'s
/// per-key roster. No `Miss` counterpart: a set can't miss. `WrongNode`
/// is spliced in by the connection handler exactly like `MultiEntry`'s —
/// `Command::MultiSet::execute` only ever produces `Stored`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MultiAckEntry {
    Stored,
    WrongNode,
}

#[derive(Debug, PartialEq, Eq)]
pub enum Response {
    Value(Bytes),
    /// Issues #128/#150: reply to `m` (`Command::MultiGet`) — one
    /// `MultiEntry` per requested key, in request order. Wire form:
    /// `M <n> <r-1>...<r-n> [tag]\n<values of the hits, concatenated in
    /// order>`, where each `<r-i>` is a decimal byte length (hit), `-`
    /// (miss), or `W` (wrong node) — see `encode`.
    Multi(Vec<MultiEntry>),
    /// Issue #150: reply to `o` (`Command::MultiSet`) — one
    /// `MultiAckEntry` per requested key, in request order. Wire form:
    /// `O <n> <r-1>...<r-n> [tag]\n` (no body — nothing to echo back for
    /// a write, unlike `M`'s hit values), where each `<r-i>` is `S`
    /// (stored) or `W` (wrong node). Never confused with the `On`/`OnT`
    /// AuthOk identity reply: that only ever appears as the very first
    /// reply on a connection, right after its `A` frame — no other
    /// request produces an `O`-leading reply.
    MultiAck(Vec<MultiAckEntry>),
    Stored,
    Deleted,
    NotFound,
    Busy,
    AuthOk,
    Unauthorized,
    /// Staged node join: acknowledges an `M` (migrate) request was received and
    /// parsed — not that the handoff it kicks off has finished. That
    /// completion is reported separately, node-to-discovery, via `C`.
    /// Carries how many of this node's entries it's the designated
    /// sender for (size-derived migration timeout) — a one-off count taken before any
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
    /// Staged node join: acknowledges an `X` (cancel migration) request was
    /// received and parsed — not that any in-progress handoff it names
    /// was actually found and aborted (a cancel for an already-finished
    /// or never-started handoff is a safe no-op on the node's side).
    MigrationCancelled,
    /// Staged node join: this node no longer (or not yet) owns the key a `G`/`S`/
    /// `D` named, per this node's own current view of cluster membership
    /// (see `NodeContext::known_ring`) — the client's view of `L` is
    /// stale. Carries no forwarding address; the client is expected to
    /// re-fetch `L` from discovery and recompute where the key belongs,
    /// not trust this node to know or proxy the request.
    WrongNode,
    /// In answer to `c`/`F` (issue #106): how many entries were dropped
    /// — informational, the wire form is a bare `C`.
    Cleared(usize),
    /// In answer to `Incr` (issue #129): the counter's new value plus its
    /// entry's remaining TTL, if it has one. Wire form:
    /// `I <value-length> [ttl-seconds] [tag]\n<value>` — a dedicated
    /// marker (not `V`) because, unlike `Value`, the TTL genuinely has to
    /// be on the wire: an SDK or the proxy fanning the *result* of a
    /// successful INCR out to replicas (client-side replication — see
    /// `src/server.rs`'s `Incr` connection handler for the node-local
    /// mirror of the same "forward the result, never the op itself"
    /// rule) only ever sees wire bytes, and would otherwise silently
    /// strip the counter's TTL on every replica. The optional TTL field
    /// follows `S`'s own `[ttl] [tag]` idiom: a present-but-unlabeled
    /// trailing field, disambiguated only by the connection's
    /// already-known tagged-or-not mode, never guessed frame by frame.
    /// TTL rounds down to whole seconds, same as every other TTL this
    /// protocol forwards.
    Incremented(Bytes, Option<Duration>),
    /// In answer to `Incr` (issue #129) when the key exists but its
    /// stored value isn't INCR's decimal-ASCII `i64` grammar, or applying
    /// `delta` would overflow `i64`. Deliberately distinct from
    /// `NotFound`, so a caller (e.g. the Django adapter's `incr`/`decr`)
    /// can tell "no such key" from "wrong type" apart.
    NotNumeric,
    /// Internal-only (staged node join), in answer to `Command::PeekEntry` — zero
    /// or one entry, each with its remaining TTL. Never encoded for a wire
    /// client, see `encode`.
    Entries(Vec<(Key, Bytes, Option<Duration>)>),
    /// Internal-only (staged node join), in answer to `Command::ListEntries` — a
    /// keys-only snapshot (see `Cache::keys`'s doc comment for why this
    /// carries no values or TTLs). Never encoded for a wire client, see
    /// `encode`.
    Keys(Vec<Key>),
    /// Internal-only (staged node join), in answer to `Command::MarkMigrated`.
    Marked,
    /// Internal-only (staged node join), in answer to `Command::UnmarkMigrated`.
    Unmarked,
    /// Internal-only (staged node join), in answer to `Command::Sweep` — how many
    /// entries the sweep actually removed.
    Swept(usize),
    /// Internal-only (issue #124), in answer to `Command::Stats` — the
    /// metrics endpoint's snapshot. Boxed: the snapshot is by far the
    /// largest variant and would otherwise inflate every `Response`.
    Stats(Box<CacheStats>),
}

/// `Response::Multi`'s shared wire encoding for both `encode` and
/// `encode_with_tag` — see `Response::Multi`'s doc comment for the frame
/// grammar.
fn encode_multi(entries: &[MultiEntry], tag: Option<u32>) -> Vec<u8> {
    let mut header = format!("M {}", entries.len());
    let mut values_len = 0;

    for entry in entries {
        match entry {
            MultiEntry::Value(value) => {
                header.push(' ');
                header.push_str(&value.len().to_string());
                values_len += value.len();
            }
            MultiEntry::Miss => header.push_str(" -"),
            MultiEntry::WrongNode => header.push_str(" W"),
        }
    }

    if let Some(tag) = tag {
        header.push(' ');
        header.push_str(&tag.to_string());
    }
    header.push('\n');

    let mut encoded = Vec::with_capacity(header.len() + values_len);
    encoded.extend_from_slice(header.as_bytes());

    for entry in entries {
        if let MultiEntry::Value(value) = entry {
            encoded.extend_from_slice(value);
        }
    }

    encoded
}

/// `Response::MultiAck`'s shared wire encoding for both `encode` and
/// `encode_with_tag` — see `Response::MultiAck`'s doc comment for the
/// frame grammar. No body: unlike `encode_multi`, a set has no value to
/// echo back.
fn encode_multi_ack(entries: &[MultiAckEntry], tag: Option<u32>) -> Vec<u8> {
    let mut header = format!("O {}", entries.len());

    for entry in entries {
        match entry {
            MultiAckEntry::Stored => header.push_str(" S"),
            MultiAckEntry::WrongNode => header.push_str(" W"),
        }
    }

    if let Some(tag) = tag {
        header.push(' ');
        header.push_str(&tag.to_string());
    }
    header.push('\n');

    header.into_bytes()
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
            Self::Cleared(_) => b"C\n".to_vec(),
            Self::NotNumeric => b"T\n".to_vec(),

            Self::Value(value) => {
                let length = value.len().to_string();

                let mut encoded = Vec::with_capacity(2 + length.len() + 1 + value.len());

                encoded.extend_from_slice(b"V ");
                encoded.extend_from_slice(length.as_bytes());
                encoded.push(b'\n');
                encoded.extend_from_slice(value);

                encoded
            }

            Self::Incremented(value, ttl) => {
                let header = format!("I {}{}\n", value.len(), ttl_field(*ttl));

                let mut encoded = Vec::with_capacity(header.len() + value.len());
                encoded.extend_from_slice(header.as_bytes());
                encoded.extend_from_slice(value);

                encoded
            }

            Self::Multi(entries) => encode_multi(entries, None),
            Self::MultiAck(entries) => encode_multi_ack(entries, None),

            Self::Entries(_)
            | Self::Keys(_)
            | Self::Marked
            | Self::Unmarked
            | Self::Swept(_)
            | Self::Stats(_) => {
                unreachable!(
                    "internal-only response (staged node join): never sent to a wire client, only \
                     matched directly in Rust by the migration task"
                )
            }
        }
    }

    /// Echoed response tags: tagged-mode encoding — echoes the request's tag as the
    /// response's last header field, so the client's read loop can verify
    /// request/response alignment before dispatching to a caller. Only
    /// responses to `G`/`S`/`D` (the pipelined commands) have a tagged
    /// form; everything else stays on `encode`.
    pub fn encode_with_tag(&self, tag: u32) -> Vec<u8> {
        match self {
            Self::Stored => format!("S {tag}\n").into_bytes(),
            Self::Deleted => format!("D {tag}\n").into_bytes(),
            Self::NotFound => format!("N {tag}\n").into_bytes(),
            Self::WrongNode => format!("W {tag}\n").into_bytes(),
            Self::Cleared(_) => format!("C {tag}\n").into_bytes(),
            Self::NotNumeric => format!("T {tag}\n").into_bytes(),

            Self::Value(value) => {
                let header = format!("V {} {tag}\n", value.len());

                let mut encoded = Vec::with_capacity(header.len() + value.len());
                encoded.extend_from_slice(header.as_bytes());
                encoded.extend_from_slice(value);

                encoded
            }

            Self::Incremented(value, ttl) => {
                let header = format!("I {}{} {tag}\n", value.len(), ttl_field(*ttl));

                let mut encoded = Vec::with_capacity(header.len() + value.len());
                encoded.extend_from_slice(header.as_bytes());
                encoded.extend_from_slice(value);

                encoded
            }

            Self::Multi(entries) => encode_multi(entries, Some(tag)),
            Self::MultiAck(entries) => encode_multi_ack(entries, Some(tag)),

            _ => unreachable!("only G/S/D/i responses have a tagged form (echoed response tags)"),
        }
    }

    /// Echoed response tags: identity reply to an extended `A <len> T` — echoes the
    /// tag capability as a `T` between the server-type byte and the LF
    /// (`OnT\n`/`EnT\n`), sent only to clients that asked, so a plain `A`
    /// keeps the exact three-byte reply older SDKs hard-read.
    pub fn encode_identity_tagged(&self) -> Vec<u8> {
        match self {
            Self::AuthOk => b"OnT\n".to_vec(),
            Self::Unauthorized => b"EnT\n".to_vec(),
            _ => unreachable!(
                "only identity replies have a tag-capability form (echoed response tags)"
            ),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cleared_encodes_as_a_bare_c_with_an_optional_tag() {
        assert_eq!(Response::Cleared(3).encode(), b"C\n".to_vec());
        assert_eq!(Response::Cleared(0).encode_with_tag(9), b"C 9\n".to_vec());
    }

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

    #[test]
    fn encodes_incremented_response_without_a_ttl() {
        let response = Response::Incremented(Bytes::from_static(b"11"), None);

        assert_eq!(response.encode(), b"I 2\n11");
        assert_eq!(response.encode_with_tag(4), b"I 2 4\n11");
    }

    #[test]
    fn encodes_incremented_response_with_a_ttl() {
        let response =
            Response::Incremented(Bytes::from_static(b"11"), Some(Duration::from_secs(60)));

        assert_eq!(response.encode(), b"I 2 60\n11");
        assert_eq!(response.encode_with_tag(4), b"I 2 60 4\n11");
    }

    #[test]
    fn encodes_incremented_response_rounds_a_sub_second_ttl_down() {
        let response =
            Response::Incremented(Bytes::from_static(b"11"), Some(Duration::from_millis(1500)));

        assert_eq!(response.encode(), b"I 2 1\n11");
    }

    #[test]
    fn encodes_not_numeric_response() {
        assert_eq!(Response::NotNumeric.encode(), b"T\n");
        assert_eq!(Response::NotNumeric.encode_with_tag(2), b"T 2\n");
    }

    #[test]
    fn encodes_tagged_fixed_responses_with_the_echoed_tag() {
        assert_eq!(Response::Stored.encode_with_tag(7), b"S 7\n");
        assert_eq!(Response::Deleted.encode_with_tag(0), b"D 0\n");
        assert_eq!(Response::NotFound.encode_with_tag(42), b"N 42\n");
        assert_eq!(
            Response::WrongNode.encode_with_tag(u32::MAX),
            b"W 4294967295\n"
        );
    }

    #[test]
    fn encodes_tagged_value_response_with_the_tag_after_the_length() {
        let response = Response::Value(Bytes::from_static(b"Alice"));

        assert_eq!(response.encode_with_tag(9), b"V 5 9\nAlice");
    }

    #[test]
    fn encodes_tagged_identity_replies() {
        assert_eq!(Response::AuthOk.encode_identity_tagged(), b"OnT\n");
        assert_eq!(Response::Unauthorized.encode_identity_tagged(), b"EnT\n");
    }
}
