use crate::cache::{Cache, CasCondition, CasResult, IncrResult, parse_decimal_i64};
use crate::key::Key;
use crate::response::{MultiAckEntry, MultiEntry, Response};
use bytes::{Buf, Bytes, BytesMut};
use std::time::Duration;

#[derive(Debug, PartialEq, Eq)]
pub enum Command {
    Auth {
        secret: Bytes,
        /// Echoed response tags: the client sent `A <len> T\n` — it wants response
        /// tags echoed on this connection's `G`/`S`/`D` replies.
        tagging: bool,
        /// Issue #125: the client sent the trailing `R` — it understands
        /// the retryable-error status, so a server may answer a
        /// transiently failed request `R` (+tag) instead of the fatal
        /// `E`-and-close, and only on such connections. The node itself
        /// currently has no transient per-request failure to report and
        /// never emits `R`; it accepts and records the token so the
        /// negotiation is uniform across node, proxy, and discovery
        /// (the proxy is the emitter today).
        retry_capable: bool,
    },
    Get {
        key: Key,
    },
    /// `m <ns-len> <n> <key-len-1> ... <key-len-n> [tag]\n<ns><key-1>...<key-n>`
    /// (issue #128 measured, issue #150 production): batches `n` gets
    /// under one wire frame and one cache-actor round trip, instead of
    /// `n` independent `g` frames. Always namespaced — one namespace per
    /// frame, since routing keys on `(namespace, key)` means a
    /// multi-namespace frame couldn't route as a single unit anyway, and
    /// every real many-op consumer (Django's prefix, Keyv's namespace, a
    /// Spring cache name) is already single-namespace.
    ///
    /// The connection handler (`src/server.rs`), not `execute`, is
    /// responsible for per-key wrong-node filtering before this reaches
    /// the cache actor — see its `Command::MultiGet` arm. `execute` here
    /// only ever answers `Value`/`Miss` per key, in `keys`' order.
    ///
    /// This node has no retry/hedge of its own — every command here is a
    /// single node's local answer. Retry-on-`WrongNode` and refresh are
    /// the proxy's/SDK's job, exactly as for `Get`; see
    /// `src/bin/nanocached-proxy.rs`'s `finish_multi_get`/
    /// `retry_multi_get`.
    MultiGet {
        namespace: Bytes,
        keys: Vec<Bytes>,
    },
    /// `o <ns-len> <n> <key-len-1> <value-len-1> ... <key-len-n>
    /// <value-len-n> [ttl] [tag]\n<ns><key-1><value-1>...<key-n><value-n>`
    /// (issue #150): batches `n` sets under one wire frame and one
    /// cache-actor round trip. Always namespaced, same reasoning as
    /// `MultiGet`. **One shared TTL for the whole batch, not per-key** —
    /// every real many-set consumer (Django's `set_many`,
    /// cache-manager's `mset`) already passes one TTL per call; per-key
    /// TTLs would meaningfully complicate the frame for no real
    /// consumer, the same simplification `Clear`'s whole-namespace scope
    /// makes over a fine-grained one.
    ///
    /// Per-key wrong-node filtering happens in the connection handler,
    /// same as `MultiGet` — `execute` only ever answers `Stored` per key,
    /// in `keys`' order, via `Response::MultiStored`.
    MultiSet {
        namespace: Bytes,
        keys: Vec<Bytes>,
        values: Vec<Bytes>,
        ttl: Option<Duration>,
    },
    Set {
        key: Key,
        value: Bytes,
        ttl: Option<Duration>,
    },
    Delete {
        key: Key,
    },
    /// `i <namespace-length> <key-length> <delta> [tag]\n<namespace><key>`
    /// (issue #129): adds `delta` (signed decimal ASCII `i64`; a negative
    /// `delta` is a decrement, so there is no separate decr opcode) to the
    /// key's stored value, in place, and returns the new value. Always
    /// namespaced — unlike `G`/`S`/`D`, this op has no pre-namespace
    /// legacy form to stay compatible with, so it carries
    /// `<namespace-length>` unconditionally (a length of 0 addresses the
    /// default namespace, same as everywhere else).
    ///
    /// Runs on the single-threaded cache actor like every other command,
    /// so the read-modify-write is atomic against every other command on
    /// *this node* — but not across a cluster: see `Cache::incr`'s doc
    /// comment and `src/server.rs`'s `Incr` connection-handler arm for how
    /// replication and migration/decommission forwarding stay consistent
    /// despite that. And not durable: a value INCR'd is exactly as
    /// volatile as one SET — LRU eviction or a TTL still reclaims it. See
    /// `docs/protocol.html`'s `INCR` section for the full caveat this
    /// implies (rate limiting and approximate counters are a good fit;
    /// billing or inventory counts are not).
    Incr {
        key: Key,
        delta: i64,
    },
    /// `k <ns-len> <key-len> <val-len> <cond> [ttl] [tag]\n<ns><key><value>`
    /// (issue #141): compare-and-set. `condition` is decoded from the
    /// wire's `A` (absent expected)/`P` (present expected)/32-hex-digit
    /// digest (exact content expected) token — see `CasCondition`. Stores
    /// `value` only if `condition` holds; otherwise nothing changes.
    /// Always namespaced, same reasoning as `Incr`: this op has no
    /// pre-namespace legacy form.
    ///
    /// Atomic against every other command on *this node* (the
    /// single-threaded cache actor), same as `Incr` — but not across a
    /// cluster on its own: see `Cache::cas_set`'s doc comment and
    /// `src/server.rs`'s `CasSet` connection-handler arm for how
    /// replication forwards only the literal result, never `k` itself.
    /// And not a distributed lock: LRU eviction reclaims the key exactly
    /// as it would after a plain `S`, which can silently let two
    /// `condition: Absent` callers both believe they "won" — see
    /// `docs/protocol.html`'s CAS section.
    CasSet {
        key: Key,
        condition: CasCondition,
        value: Bytes,
        ttl: Option<Duration>,
    },
    /// `x <ns-len> <key-len> <cond> [tag]\n<ns><key>` (issue #141):
    /// compare-and-delete. `<cond>` is always a 32-hex-digit digest here
    /// — `parse_with_mode` rejects `A`/`P` for `x` as a fatal parse error,
    /// since an absent- or present-only conditioned delete is already the
    /// plain, unconditional `d`. Same atomicity/replication/eviction
    /// caveats as `CasSet`.
    CasDelete {
        key: Key,
        expected_digest: [u8; 16],
    },
    /// `c <namespace-length> [tag]\n<namespace>` (issue #106): drops one
    /// namespace's every entry. A zero-length namespace clears the
    /// default one. Not key-addressed, so no wrong-node check applies:
    /// clients fan it out to every member and each node drops its own
    /// sub-map.
    Clear {
        namespace: Bytes,
    },
    /// `F [tag]\n` (issue #106): the whole-store flush — every
    /// namespace, the default one included.
    ClearAll,
    /// Every cache command addresses a `Key` — namespace plus name (issue
    /// #105). The legacy `G`/`S`/`D` frames address the default (empty)
    /// namespace; their lowercase `g`/`s`/`d` counterparts carry an
    /// explicit, length-prefixed namespace field first.
    /// Internal-only (staged node join): never produced by `parse()`, constructed
    /// directly by the migration task to snapshot every key this node
    /// currently holds, to compute which ones a newly joining node now
    /// owns. See `Response::Keys` and `Cache::keys`'s doc comment for why
    /// this is keys-only rather than full entries.
    ListEntries,
    /// Internal-only (staged node join): the migration task's live re-check of a
    /// single key's current value right before sending it, instead of
    /// trusting `ListEntries`'s snapshot (which may be stale by the time
    /// this key's turn comes up). Answered with `Response::Entries`
    /// holding zero or one entry — reusing its shape rather than adding a
    /// new one for what's otherwise the exact same data.
    PeekEntry {
        key: Key,
    },
    /// Internal-only (staged node join): marks a key as handed off to another
    /// node during a migration this node was the source for. `Sweep`
    /// reclaims marked entries later.
    MarkMigrated {
        key: Key,
    },
    /// Internal-only (staged node join): reverses `MarkMigrated` for a key whose
    /// migration was cancelled (see `Command::CancelMigration`), so
    /// `Sweep` doesn't reclaim it after all.
    UnmarkMigrated {
        key: Key,
    },
    /// `U <ns-len> <key-len> <val-len> <token-len> [ttl] [A] [tag]\n
    /// <token><ns><key><value>` (issue #124, cluster-internal like
    /// `M`/`X`): a decommissioning node handing one of its entries to the
    /// key's post-leave owner, or (issue #266) a survivor re-replicating
    /// a key to the owner an eviction promoted. Executes as `Set`, unless
    /// the trailing `A` token (put-if-absent, same "content, not
    /// position, disambiguates it" idiom as `k`'s `<cond>`) is present,
    /// in which case it executes as set-if-absent instead —
    /// re-replication runs after the fact and must not clobber a newer
    /// client write that raced it, so it asks for absent semantics; an
    /// ordinary decommission handoff never sets `A`, since nothing else
    /// could have written to an entrant that doesn't own the key yet.
    /// Either way the difference from `Set` is in the connection handler,
    /// which skips the wrong-node check — the receiver becomes this
    /// key's owner only once discovery publishes the post-change roster,
    /// which by design happens *after* the transfer — and the receiver
    /// acks `S\n` either way: a key already present under `A` is a
    /// success for the sender, not a conflict.
    ///
    /// `token` (issue #295) is *this receiving node's own* membership
    /// token, same "the shared secret only proves cluster membership, not
    /// that the sender is entitled to skip the wrong-node check" reasoning
    /// `Migrate::token`'s doc comment gives for `M` — without it any
    /// shared-secret client could forge `U` to write a key here this node
    /// doesn't actually own. It leads the body (before `namespace`/`key`/
    /// `value`, same as `X`'s `<token><joining_name>`) so the connection
    /// handler can verify it before acting.
    HandoffSet {
        key: Key,
        value: Bytes,
        ttl: Option<Duration>,
        if_absent: bool,
        token: String,
    },
    /// `u <ns-len> <key-len> <token-len> [tag]\n<token><ns><key>` (issue
    /// #124, cluster-internal like `U`): a decommissioning node
    /// forwarding a concurrent client delete to the key's post-leave
    /// owner. Executes exactly as `Delete`; the connection handler skips
    /// the wrong-node check for the same reason it does for `U` — the
    /// receiver owns the key only once the post-leave roster publishes.
    ///
    /// `token` (issue #295) is this receiving node's own membership
    /// token, same reasoning and body ordering as `HandoffSet::token`.
    HandoffDelete {
        key: Key,
        token: String,
    },
    /// Internal-only (issue #124): the metrics endpoint's snapshot
    /// request — never produced by `parse()`, constructed by the metrics
    /// server task. Answered with `Response::Stats`.
    Stats,
    /// Internal-only (staged node join): the active-deletion pass, run
    /// periodically by a background task. Since TTL expiry is otherwise
    /// only checked lazily on access, proactively removes anything
    /// already past its TTL; with `marked` also reclaims every marked
    /// entry — withheld (issue #62) while the join those marks belong to
    /// is still undecided, see `run_sweep` in `server.rs`.
    Sweep {
        marked: bool,
    },
    /// Staged node join: sent by discovery to a `Joined` node when a new node is
    /// joining, so this node can compute (via `HashRing`) how each of its
    /// own keys' top-R owner set changes (client-side replication). `joining_name`/
    /// `joining_addr` identify the joining node; `joined` is every
    /// currently-`Joined` node (node identity decoupled from address names, including this one) — the
    /// "before" roster, to which `joining_name` is the "after" addition.
    /// `replication` is discovery's replication factor R (client-side replication) — the
    /// single source nodes learn R from.
    ///
    /// `token` is *this receiving node's own* membership token (issue #34),
    /// echoed back by discovery to prove the `M` really came from a
    /// discovery server this node registered with — the only party that
    /// knows it. This closes the gap where any holder of the shared secret
    /// (every client) could otherwise send `M` directly and make the node
    /// stream its cache to an attacker-chosen address. It does not violate
    /// Per-node membership tokens's "tokens are never sent back out" invariant: the token in
    /// an `M` is the *recipient's* own token (which it already holds and no
    /// client knows), never some other node's.
    ///
    /// `joining_token` (issue #295) is the *joining* node's own membership
    /// token — discovery already holds it from the joiner's own `J`/`P`,
    /// the same way it holds every registered node's. Unlike `token`
    /// above, this genuinely is "some other node's" token, but handing it
    /// to THIS specific receiving node is a deliberate, narrow exception
    /// to Per-node membership tokens's "never sent back out" invariant: discovery is
    /// granting this ready node a scoped, time-boxed trust relationship
    /// with the joiner for the one purpose of authorizing the `U`/`u`
    /// frames this handoff (and any issue #266 re-replication onto the
    /// joiner) may need to send it (see `Command::HandoffSet::token`) —
    /// not a broadcast of the joiner's token to anyone who merely learns
    /// its name (`L` still lists names only). Stored on this node's
    /// `ActiveMigration` for that handoff's duration, never persisted or
    /// forwarded elsewhere.
    Migrate {
        token: String,
        joining_name: String,
        joining_addr: String,
        joining_token: String,
        joined: Vec<(String, String)>,
        replication: usize,
    },
    /// Staged node join: sent by discovery to a ready node to abandon a handoff
    /// it's mid-`Migrate` for — a ready or joining node died, or
    /// discovery gave up waiting on a completion report. `joining_name`
    /// identifies which handoff to abandon (a node only ever has one
    /// active at a time, but a cancel for an already-finished or
    /// never-started one must be a safe no-op — see `run_migration`).
    ///
    /// `token` is this receiving node's own membership token, echoed by
    /// discovery for the same reason as `Migrate::token` — otherwise any
    /// client holding the shared secret could abort a legitimate handoff.
    CancelMigration {
        token: String,
        joining_name: String,
    },
}

impl Command {
    /// Executes a cache operation. `Command::Auth`/`Migrate`/
    /// `CancelMigration` are intercepted by the connection handler before
    /// a command ever reaches this point (none is a plain cache
    /// operation: `Auth` because the actor has no auth state,
    /// `Migrate`/`CancelMigration` because they need network access or
    /// migration-task state the cache actor doesn't have), so none can
    /// appear here.
    pub fn execute(self, cache: &mut Cache) -> Response {
        match self {
            Self::Auth { .. } | Self::Migrate { .. } | Self::CancelMigration { .. } => {
                unreachable!(
                    "Auth, Migrate, and CancelMigration are handled by the connection handler, \
                     not the cache actor"
                )
            }

            Self::Get { key } => match cache.get(&key) {
                Some(value) => Response::Value(value),
                None => Response::NotFound,
            },

            Self::MultiGet { namespace, keys } => {
                let entries = keys
                    .into_iter()
                    .map(|name| {
                        let key = Key::new(namespace.clone(), name);
                        match cache.get(&key) {
                            Some(value) => MultiEntry::Value(value),
                            None => MultiEntry::Miss,
                        }
                    })
                    .collect();
                Response::Multi(entries)
            }
            Self::MultiSet {
                namespace,
                keys,
                values,
                ttl,
            } => {
                let mut entries = Vec::with_capacity(keys.len());
                for (name, value) in keys.into_iter().zip(values) {
                    let key = Key::new(namespace.clone(), name);
                    match ttl {
                        Some(ttl) => cache.set_with_ttl(key, value, ttl),
                        None => cache.set(key, value),
                    }
                    entries.push(MultiAckEntry::Stored);
                }
                Response::MultiAck(entries)
            }

            Self::Set { key, value, ttl } => {
                match ttl {
                    Some(ttl) => cache.set_with_ttl(key, value, ttl),
                    None => cache.set(key, value),
                }
                Response::Stored
            }

            Self::HandoffSet {
                key,
                value,
                ttl,
                if_absent,
                // Verified by the connection handler before this ever
                // reaches `execute` (see `Command::HandoffSet::token`'s
                // doc comment) — nothing left for the cache actor to do
                // with it.
                token: _,
            } => {
                if if_absent {
                    // Issue #266: re-replication after an eviction — must
                    // not regress a newer client write that raced it. An
                    // already-present key is still a success for the
                    // sender (`Response::Stored` either way): the value
                    // that won is by definition at least as new as what
                    // re-replication was carrying.
                    match cache.cas_set(&key, CasCondition::Absent, value, ttl) {
                        CasResult::Stored | CasResult::Mismatch => {}
                    }
                } else {
                    match ttl {
                        Some(ttl) => cache.set_with_ttl(key, value, ttl),
                        None => cache.set(key, value),
                    }
                }
                Response::Stored
            }

            Self::Delete { key }
            | Self::HandoffDelete {
                key,
                // Verified by the connection handler before this ever
                // reaches `execute` — see `Command::HandoffDelete::token`.
                token: _,
            } => {
                if cache.delete(&key) {
                    Response::Deleted
                } else {
                    Response::NotFound
                }
            }

            Self::Incr { key, delta } => match cache.incr(&key, delta) {
                IncrResult::Value(value, remaining_ttl) => {
                    Response::Incremented(Bytes::from(value.to_string()), remaining_ttl)
                }
                IncrResult::NotFound => Response::NotFound,
                IncrResult::NotNumeric => Response::NotNumeric,
            },

            Self::CasSet {
                key,
                condition,
                value,
                ttl,
            } => match cache.cas_set(&key, condition, value, ttl) {
                CasResult::Stored => Response::Stored,
                CasResult::Mismatch => Response::NotFound,
            },

            Self::CasDelete {
                key,
                expected_digest,
            } => {
                if cache.cas_delete(&key, expected_digest) {
                    Response::Deleted
                } else {
                    Response::NotFound
                }
            }

            Self::Clear { namespace } => Response::Cleared(cache.clear(&namespace)),
            Self::ClearAll => Response::Cleared(cache.clear_all()),

            Self::ListEntries => Response::Keys(cache.keys()),

            Self::PeekEntry { key } => {
                Response::Entries(cache.peek_entry(&key).into_iter().collect())
            }

            Self::MarkMigrated { key } => {
                cache.mark_migrated(&key);
                Response::Marked
            }

            Self::UnmarkMigrated { key } => {
                cache.unmark_migrated(&key);
                Response::Unmarked
            }

            Self::Stats => Response::Stats(Box::new(cache.stats())),

            Self::Sweep { marked: true } => Response::Swept(cache.sweep()),
            Self::Sweep { marked: false } => Response::Swept(cache.sweep_expired()),
        }
    }
}

#[derive(Debug, PartialEq, Eq)]
pub enum ParseError {
    InvalidCommand,
    InvalidLength,
    EmptyKey,
    EmptySecret,
    /// A name/address field in `M` (staged node join / node identity decoupled from address) was declared with
    /// length 0.
    EmptyField,
    /// A name/address field in `M` wasn't valid UTF-8.
    InvalidUtf8,
    Incomplete,
}

/// Parses one request from the front of `input`. On success, the consumed
/// bytes are removed from `input` via `BytesMut::split_to`, and the
/// returned command's key/value share that removed chunk's allocation
/// (no copy). On `Incomplete`, `input` is left untouched.
#[cfg(test)]
pub fn parse(input: &mut BytesMut) -> Result<Command, ParseError> {
    parse_with_mode(input, false, &mut MigrateProgress::default()).map(|(command, _)| command)
}

/// Echoed response tags: `parse` for a connection whose `A ... T` negotiation
/// succeeded. `G`/`S`/`D` headers must carry the client's tag as their
/// last field, returned alongside for the response to echo; commands
/// that never carry one (`A`, `M`, `X`) return `None`.
#[cfg(test)]
pub fn parse_tagged(input: &mut BytesMut) -> Result<(Command, Option<u32>), ParseError> {
    parse_with_mode(input, true, &mut MigrateProgress::default())
}

/// `parse`/`parse_tagged` for a connection's read loop, which calls this
/// again every time more bytes arrive after an `Incomplete`. `progress`
/// carries how far an `M` roster scan got on the previous attempt, so a
/// large `M` frame trickling in `READ_CHUNK_SIZE` at a time is validated
/// once overall instead of from entry #1 on every read (which was
/// quadratic in the roster size — and, since this parse runs before the
/// connection authenticates, a cheap pre-auth CPU exhaustion vector on
/// the single-threaded runtime). The caller must reset `progress` (or
/// hand in a fresh one) whenever `input`'s front changes for any reason
/// other than bytes being appended — i.e. after every successfully
/// parsed command.
pub fn parse_resumable(
    input: &mut BytesMut,
    tagged: bool,
    progress: &mut MigrateProgress,
) -> Result<(Command, Option<u32>), ParseError> {
    let parsed = parse_with_mode(input, tagged, progress);

    if !matches!(parsed, Err(ParseError::Incomplete)) {
        *progress = MigrateProgress::default();
    }

    parsed
}

/// How far `parse_migrate`'s read-only roster scan got before running out
/// of buffered bytes — see `parse_resumable`. Only meaningful while the
/// same `M` frame is still the front of the buffer, which `header_end`
/// and `joined_count` guard against (a different frame at the front is
/// vanishingly unlikely to match both, and `input.len() >= cursor` is
/// re-checked before any recorded span is trusted).
#[derive(Debug, Default)]
pub struct MigrateProgress {
    header_end: usize,
    joined_count: usize,
    cursor: usize,
    entry_spans: Vec<(usize, usize, usize, usize)>,
}

fn parse_with_mode(
    input: &mut BytesMut,
    tagged: bool,
    progress: &mut MigrateProgress,
) -> Result<(Command, Option<u32>), ParseError> {
    let header_end = find_lf(&input[..]).ok_or(ParseError::Incomplete)?;
    let header = &input[..header_end];

    let mut parts = header.split(|byte| *byte == b' ');
    let command = parts.next().ok_or(ParseError::InvalidCommand)?;

    match command {
        b"A" => {
            let secret_length = parts.next().ok_or(ParseError::InvalidLength)?;

            // Echoed response tags: an optional literal `T` requests tagged mode.
            // Accepted regardless of the connection's current mode, since
            // this is the field that *establishes* the mode. An optional
            // trailing `R` (issue #125) declares that this client
            // understands the retryable-error status — a server may then
            // answer a failed request `R` instead of the fatal
            // `E`-and-close. Order is fixed (`[T] [R]`), matching how the
            // SDKs probe capabilities in one deterministic frame.
            let mut tagging = false;
            let mut retry_capable = false;
            match parts.next() {
                None => {}
                Some(b"T") => {
                    tagging = true;
                    match parts.next() {
                        None => {}
                        Some(b"R") => retry_capable = true,
                        Some(_) => return Err(ParseError::InvalidCommand),
                    }
                }
                Some(b"R") => retry_capable = true,
                Some(_) => return Err(ParseError::InvalidCommand),
            }

            if parts.next().is_some() {
                return Err(ParseError::InvalidLength);
            }

            let secret_length = parse_length(secret_length)?;

            if secret_length == 0 {
                return Err(ParseError::EmptySecret);
            }

            let secret_start = header_end + 1;
            let secret_end = secret_start
                .checked_add(secret_length)
                .ok_or(ParseError::InvalidLength)?;

            if input.len() < secret_end {
                return Err(ParseError::Incomplete);
            }

            let frame = input.split_to(secret_end).freeze();
            let secret = frame.slice(secret_start..secret_end);

            Ok((
                Command::Auth {
                    secret,
                    tagging,
                    retry_capable,
                },
                None,
            ))
        }

        // Namespaced variants (issue #105): the lowercase letter carries
        // one extra leading `<namespace-length>` field and the namespace
        // bytes lead the body. A length of 0 addresses the default
        // namespace, same as the uppercase form.
        b"G" | b"D" | b"g" | b"d" => {
            let namespaced = command == b"g" || command == b"d";
            let namespace_length = if namespaced {
                parse_length(parts.next().ok_or(ParseError::InvalidLength)?)?
            } else {
                0
            };
            let key_length = parts.next().ok_or(ParseError::InvalidLength)?;
            let tag = parse_trailing_tag(&mut parts, tagged)?;

            if parts.next().is_some() {
                return Err(ParseError::InvalidLength);
            }

            let key_length = parse_length(key_length)?;

            if key_length == 0 {
                return Err(ParseError::EmptyKey);
            }

            let namespace_start = header_end + 1;
            let key_start = namespace_start
                .checked_add(namespace_length)
                .ok_or(ParseError::InvalidLength)?;
            let key_end = key_start
                .checked_add(key_length)
                .ok_or(ParseError::InvalidLength)?;

            if input.len() < key_end {
                return Err(ParseError::Incomplete);
            }

            let is_get = command == b"G" || command == b"g";

            let frame = input.split_to(key_end).freeze();
            let key = Key::new(
                frame.slice(namespace_start..key_start),
                frame.slice(key_start..key_end),
            );

            Ok((
                if is_get {
                    Command::Get { key }
                } else {
                    Command::Delete { key }
                },
                tag,
            ))
        }

        // `u <ns-len> <key-len> <token-len> [tag]\n<token><ns><key>` (issue
        // #124, cluster-internal): always namespaced (a leading
        // `<ns-len>`, same as `d`) — `u` has no unnamespaced legacy form.
        // Issue #295: carries `<token-len>` and leads the body with
        // `token`, same "check it before touching the rest" ordering as
        // `X`'s `<token><joining_name>` — see `Command::HandoffDelete::
        // token`'s doc comment.
        b"u" => {
            let namespace_length = parts.next().ok_or(ParseError::InvalidLength)?;
            let key_length = parts.next().ok_or(ParseError::InvalidLength)?;
            let token_length = parts.next().ok_or(ParseError::InvalidLength)?;
            let tag = parse_trailing_tag(&mut parts, tagged)?;

            if parts.next().is_some() {
                return Err(ParseError::InvalidLength);
            }

            let namespace_length = parse_length(namespace_length)?;
            let key_length = parse_length(key_length)?;
            let token_length = parse_length(token_length)?;

            if key_length == 0 {
                return Err(ParseError::EmptyKey);
            }
            if token_length == 0 {
                return Err(ParseError::EmptyField);
            }

            let token_start = header_end + 1;
            let namespace_start = token_start
                .checked_add(token_length)
                .ok_or(ParseError::InvalidLength)?;
            let key_start = namespace_start
                .checked_add(namespace_length)
                .ok_or(ParseError::InvalidLength)?;
            let key_end = key_start
                .checked_add(key_length)
                .ok_or(ParseError::InvalidLength)?;

            if input.len() < key_end {
                return Err(ParseError::Incomplete);
            }

            let frame = input.split_to(key_end).freeze();
            let token = decode_field(&frame, token_start, token_length)?;
            let key = Key::new(
                frame.slice(namespace_start..key_start),
                frame.slice(key_start..key_end),
            );

            Ok((Command::HandoffDelete { key, token }, tag))
        }

        // Issue #128 measurement prototype: `m <ns-len> <n>
        // <key-len-1>...<key-len-n> [tag]\n<ns><key-1>...<key-n>` — see
        // `Command::MultiGet`'s doc comment. `n` and every `<key-len-i>`
        // are read via `parts.next()`, which fails fast (`?`) the moment
        // a claimed count outruns the actual header fields, so a header
        // lying about a huge `n` never drives a matching-size allocation
        // — `key_lengths` only ever grows to as many fields as are
        // genuinely present in the (already length-bounded) header.
        b"m" => {
            let namespace_length = parse_length(parts.next().ok_or(ParseError::InvalidLength)?)?;
            let count = parse_length(parts.next().ok_or(ParseError::InvalidLength)?)?;

            if count == 0 {
                return Err(ParseError::InvalidLength);
            }

            let mut key_lengths = Vec::new();
            for _ in 0..count {
                let length = parse_length(parts.next().ok_or(ParseError::InvalidLength)?)?;

                if length == 0 {
                    return Err(ParseError::EmptyKey);
                }

                key_lengths.push(length);
            }

            let tag = parse_trailing_tag(&mut parts, tagged)?;

            if parts.next().is_some() {
                return Err(ParseError::InvalidLength);
            }

            let namespace_start = header_end + 1;
            let mut cursor = namespace_start
                .checked_add(namespace_length)
                .ok_or(ParseError::InvalidLength)?;
            let mut key_spans = Vec::with_capacity(key_lengths.len());

            for length in key_lengths {
                let start = cursor;
                cursor = start.checked_add(length).ok_or(ParseError::InvalidLength)?;
                key_spans.push((start, cursor));
            }

            if input.len() < cursor {
                return Err(ParseError::Incomplete);
            }

            let frame = input.split_to(cursor).freeze();
            let namespace = frame.slice(namespace_start..namespace_start + namespace_length);
            let keys = key_spans
                .into_iter()
                .map(|(start, end)| frame.slice(start..end))
                .collect();

            Ok((Command::MultiGet { namespace, keys }, tag))
        }

        // Issue #150: `o <ns-len> <n> <key-len-1> <value-len-1>...
        // <key-len-n> <value-len-n> [ttl] [tag]\n<ns><key-1><value-1>...
        // <key-n><value-n>` — see `Command::MultiSet`'s doc comment. Same
        // "claimed `n` can't outrun the header" defense as `m`'s loop.
        b"o" => {
            let namespace_length = parse_length(parts.next().ok_or(ParseError::InvalidLength)?)?;
            let count = parse_length(parts.next().ok_or(ParseError::InvalidLength)?)?;

            if count == 0 {
                return Err(ParseError::InvalidLength);
            }

            let mut lengths = Vec::new();
            for _ in 0..count {
                let key_length = parse_length(parts.next().ok_or(ParseError::InvalidLength)?)?;
                let value_length = parse_length(parts.next().ok_or(ParseError::InvalidLength)?)?;

                if key_length == 0 {
                    return Err(ParseError::EmptyKey);
                }

                lengths.push((key_length, value_length));
            }

            // Same "[ttl] [tag]" disambiguation `S`/`s` uses: the
            // connection's negotiated mode says whether one trailing
            // field is the tag alone or TTL-then-tag.
            let (ttl, tag) = if tagged {
                let first = parts.next().ok_or(ParseError::InvalidLength)?;
                match parts.next() {
                    Some(second) => (Some(first), Some(parse_tag(second)?)),
                    None => (None, Some(parse_tag(first)?)),
                }
            } else {
                (parts.next(), None)
            };

            if parts.next().is_some() {
                return Err(ParseError::InvalidLength);
            }

            let ttl = match ttl {
                Some(ttl) => {
                    let seconds = parse_length(ttl)?;
                    let seconds = u64::try_from(seconds).map_err(|_| ParseError::InvalidLength)?;
                    Some(Duration::from_secs(seconds))
                }
                None => None,
            };

            let namespace_start = header_end + 1;
            let mut cursor = namespace_start
                .checked_add(namespace_length)
                .ok_or(ParseError::InvalidLength)?;
            let mut spans = Vec::with_capacity(lengths.len());

            for (key_length, value_length) in lengths {
                let key_start = cursor;
                let value_start = key_start
                    .checked_add(key_length)
                    .ok_or(ParseError::InvalidLength)?;
                cursor = value_start
                    .checked_add(value_length)
                    .ok_or(ParseError::InvalidLength)?;
                spans.push((key_start, value_start, cursor));
            }

            if input.len() < cursor {
                return Err(ParseError::Incomplete);
            }

            let frame = input.split_to(cursor).freeze();
            let namespace = frame.slice(namespace_start..namespace_start + namespace_length);
            let mut keys = Vec::with_capacity(spans.len());
            let mut values = Vec::with_capacity(spans.len());

            for (key_start, value_start, value_end) in spans {
                keys.push(frame.slice(key_start..value_start));
                values.push(frame.slice(value_start..value_end));
            }

            Ok((
                Command::MultiSet {
                    namespace,
                    keys,
                    values,
                    ttl,
                },
                tag,
            ))
        }

        // Issue #129: `i <ns-len> <key-len> <delta> [tag]\n<ns><key>` —
        // always namespaced (see `Command::Incr`'s doc comment), so this
        // follows `G`/`D`'s namespaced-only shape (`g`/`d`) rather than
        // needing an uppercase/lowercase pair. `<delta>` is a mandatory
        // field ahead of the optional trailing tag, same position `S`'s
        // `<value-length>` holds — no multi-field trailing ambiguity like
        // `S`'s `[ttl] [tag]` (`parse_trailing_tag` disambiguates below on
        // the connection's negotiated mode alone, same as everywhere
        // else).
        b"i" => {
            let namespace_length = parse_length(parts.next().ok_or(ParseError::InvalidLength)?)?;
            let key_length = parts.next().ok_or(ParseError::InvalidLength)?;
            let delta = parts.next().ok_or(ParseError::InvalidLength)?;
            let tag = parse_trailing_tag(&mut parts, tagged)?;

            if parts.next().is_some() {
                return Err(ParseError::InvalidLength);
            }

            let key_length = parse_length(key_length)?;

            if key_length == 0 {
                return Err(ParseError::EmptyKey);
            }

            // Malformed here means the client's own frame is broken (not
            // a data-dependent condition), so this is a fatal parse error
            // like every other structural field — never the wire's `T`
            // status, which is reserved for a well-formed request whose
            // *stored value* isn't a counter (see `Response::NotNumeric`).
            let delta = parse_decimal_i64(delta).ok_or(ParseError::InvalidLength)?;

            let namespace_start = header_end + 1;
            let key_start = namespace_start
                .checked_add(namespace_length)
                .ok_or(ParseError::InvalidLength)?;
            let key_end = key_start
                .checked_add(key_length)
                .ok_or(ParseError::InvalidLength)?;

            if input.len() < key_end {
                return Err(ParseError::Incomplete);
            }

            let frame = input.split_to(key_end).freeze();
            let key = Key::new(
                frame.slice(namespace_start..key_start),
                frame.slice(key_start..key_end),
            );

            Ok((Command::Incr { key, delta }, tag))
        }

        // Issue #141: `k <ns-len> <key-len> <val-len> <cond> [ttl] [tag]
        // \n<ns><key><value>` — always namespaced, same reasoning as `i`.
        // `<cond>` is a mandatory field ahead of `s`'s own optional
        // trailing `[ttl] [tag]` pair, which this reuses verbatim (the
        // connection's tagged-or-not mode disambiguates a 1-vs-2 trailing
        // field count, never a frame-by-frame guess).
        b"k" => {
            let namespace_length = parse_length(parts.next().ok_or(ParseError::InvalidLength)?)?;
            let key_length = parts.next().ok_or(ParseError::InvalidLength)?;
            let value_length = parts.next().ok_or(ParseError::InvalidLength)?;
            let cond = parts.next().ok_or(ParseError::InvalidLength)?;
            let condition = parse_cas_condition(cond, true)?;

            let (ttl, tag) = if tagged {
                let first = parts.next().ok_or(ParseError::InvalidLength)?;
                match parts.next() {
                    Some(second) => (Some(first), Some(parse_tag(second)?)),
                    None => (None, Some(parse_tag(first)?)),
                }
            } else {
                (parts.next(), None)
            };

            if parts.next().is_some() {
                return Err(ParseError::InvalidLength);
            }

            let key_length = parse_length(key_length)?;
            let value_length = parse_length(value_length)?;

            if key_length == 0 {
                return Err(ParseError::EmptyKey);
            }

            let ttl = match ttl {
                Some(ttl) => {
                    let seconds = parse_length(ttl)?;
                    let seconds = u64::try_from(seconds).map_err(|_| ParseError::InvalidLength)?;
                    Some(Duration::from_secs(seconds))
                }
                None => None,
            };

            let namespace_start = header_end + 1;
            let key_start = namespace_start
                .checked_add(namespace_length)
                .ok_or(ParseError::InvalidLength)?;
            let key_end = key_start
                .checked_add(key_length)
                .ok_or(ParseError::InvalidLength)?;
            let value_end = key_end
                .checked_add(value_length)
                .ok_or(ParseError::InvalidLength)?;

            if input.len() < value_end {
                return Err(ParseError::Incomplete);
            }

            let frame = input.split_to(value_end).freeze();
            let key = Key::new(
                frame.slice(namespace_start..key_start),
                frame.slice(key_start..key_end),
            );
            let value = frame.slice(key_end..value_end);

            Ok((
                Command::CasSet {
                    key,
                    condition,
                    value,
                    ttl,
                },
                tag,
            ))
        }

        // Issue #141: `x <ns-len> <key-len> <cond> [tag]\n<ns><key>` —
        // `<cond>` must be a digest (`allow_absent_present: false`): an
        // absent- or present-only conditioned delete is already the
        // plain, unconditional `d`.
        b"x" => {
            let namespace_length = parse_length(parts.next().ok_or(ParseError::InvalidLength)?)?;
            let key_length = parts.next().ok_or(ParseError::InvalidLength)?;
            let cond = parts.next().ok_or(ParseError::InvalidLength)?;
            let expected_digest = match parse_cas_condition(cond, false)? {
                CasCondition::Digest(digest) => digest,
                CasCondition::Absent | CasCondition::Present => {
                    unreachable!("parse_cas_condition(cond, false) never returns Absent/Present")
                }
            };
            let tag = parse_trailing_tag(&mut parts, tagged)?;

            if parts.next().is_some() {
                return Err(ParseError::InvalidLength);
            }

            let key_length = parse_length(key_length)?;

            if key_length == 0 {
                return Err(ParseError::EmptyKey);
            }

            let namespace_start = header_end + 1;
            let key_start = namespace_start
                .checked_add(namespace_length)
                .ok_or(ParseError::InvalidLength)?;
            let key_end = key_start
                .checked_add(key_length)
                .ok_or(ParseError::InvalidLength)?;

            if input.len() < key_end {
                return Err(ParseError::Incomplete);
            }

            let frame = input.split_to(key_end).freeze();
            let key = Key::new(
                frame.slice(namespace_start..key_start),
                frame.slice(key_start..key_end),
            );

            Ok((
                Command::CasDelete {
                    key,
                    expected_digest,
                },
                tag,
            ))
        }

        b"c" => {
            let namespace_length = parts.next().ok_or(ParseError::InvalidLength)?;
            let tag = parse_trailing_tag(&mut parts, tagged)?;

            if parts.next().is_some() {
                return Err(ParseError::InvalidLength);
            }

            let namespace_length = parse_length(namespace_length)?;

            let namespace_start = header_end + 1;
            let namespace_end = namespace_start
                .checked_add(namespace_length)
                .ok_or(ParseError::InvalidLength)?;

            if input.len() < namespace_end {
                return Err(ParseError::Incomplete);
            }

            let frame = input.split_to(namespace_end).freeze();
            let namespace = frame.slice(namespace_start..namespace_end);

            Ok((Command::Clear { namespace }, tag))
        }

        b"F" => {
            let tag = parse_trailing_tag(&mut parts, tagged)?;

            if parts.next().is_some() {
                return Err(ParseError::InvalidLength);
            }

            input.advance(header_end + 1);

            Ok((Command::ClearAll, tag))
        }

        b"S" | b"s" => {
            // Resolved before the body is consumed (`command` borrows the
            // buffer) — same dance as `G`/`D`'s `is_get`.
            let namespace_length = if command == b"s" {
                parse_length(parts.next().ok_or(ParseError::InvalidLength)?)?
            } else {
                0
            };
            let key_length = parts.next().ok_or(ParseError::InvalidLength)?;
            let value_length = parts.next().ok_or(ParseError::InvalidLength)?;

            // Every field after `<value-len>`, in wire order: `[<ttl>]
            // [<tag>]`. Collected up front — capped, so a frame with
            // extra fields errors here rather than silently reading
            // `parts` past what a human wrote — then peeled back to
            // front: the tag is always last in tagged mode.
            let max_trailing = 2;
            let mut trailing: Vec<&[u8]> = Vec::with_capacity(max_trailing);
            for part in parts.by_ref() {
                trailing.push(part);
                if trailing.len() > max_trailing {
                    return Err(ParseError::InvalidLength);
                }
            }

            let tag = if tagged {
                Some(parse_tag(trailing.pop().ok_or(ParseError::InvalidLength)?)?)
            } else {
                None
            };

            if trailing.len() > 1 {
                return Err(ParseError::InvalidLength);
            }
            let ttl = trailing.pop();

            let key_length = parse_length(key_length)?;
            let value_length = parse_length(value_length)?;

            if key_length == 0 {
                return Err(ParseError::EmptyKey);
            }

            let ttl = match ttl {
                Some(ttl) => {
                    let seconds = parse_length(ttl)?;
                    let seconds = u64::try_from(seconds).map_err(|_| ParseError::InvalidLength)?;

                    Some(Duration::from_secs(seconds))
                }
                None => None,
            };

            let namespace_start = header_end + 1;

            let key_start = namespace_start
                .checked_add(namespace_length)
                .ok_or(ParseError::InvalidLength)?;

            let key_end = key_start
                .checked_add(key_length)
                .ok_or(ParseError::InvalidLength)?;

            let value_end = key_end
                .checked_add(value_length)
                .ok_or(ParseError::InvalidLength)?;

            if input.len() < value_end {
                return Err(ParseError::Incomplete);
            }

            let frame = input.split_to(value_end).freeze();
            let key = Key::new(
                frame.slice(namespace_start..key_start),
                frame.slice(key_start..key_end),
            );
            let value = frame.slice(key_end..value_end);

            Ok((Command::Set { key, value, ttl }, tag))
        }

        // `U <ns-len> <key-len> <val-len> <token-len> [ttl] [A] [tag]\n
        // <token><ns><key><value>` (issue #124, cluster-internal): always
        // namespaced — `U` has no unnamespaced legacy form, same as `u`.
        // Issue #295: carries `<token-len>` and leads the body with
        // `token` (before `ns`/`key`/`value`), same ordering as `X`'s
        // `<token><joining_name>` — see `Command::HandoffSet::token`'s
        // doc comment.
        b"U" => {
            let namespace_length = parts.next().ok_or(ParseError::InvalidLength)?;
            let key_length = parts.next().ok_or(ParseError::InvalidLength)?;
            let value_length = parts.next().ok_or(ParseError::InvalidLength)?;
            let token_length = parts.next().ok_or(ParseError::InvalidLength)?;

            // Every field after `<token-len>`, in wire order: `[<ttl>]
            // [A] [<tag>]`. Collected up front — capped, same reasoning
            // as `S`/`s`'s own trailing-field collection — then peeled
            // back to front: the tag is always last in tagged mode, and
            // `A` (issue #266's put-if-absent handoff) is a literal token
            // distinguishable from a numeric ttl by its content (same
            // "content, not position, disambiguates it" idiom as `k`'s
            // `<cond>`), so it doesn't need a fixed slot the way `tagged`
            // gives the tag one.
            let max_trailing = 3;
            let mut trailing: Vec<&[u8]> = Vec::with_capacity(max_trailing);
            for part in parts.by_ref() {
                trailing.push(part);
                if trailing.len() > max_trailing {
                    return Err(ParseError::InvalidLength);
                }
            }

            let tag = if tagged {
                Some(parse_tag(trailing.pop().ok_or(ParseError::InvalidLength)?)?)
            } else {
                None
            };

            let if_absent = trailing.last().copied() == Some(b"A".as_slice());
            if if_absent {
                trailing.pop();
            }

            if trailing.len() > 1 {
                return Err(ParseError::InvalidLength);
            }
            let ttl = trailing.pop();

            let namespace_length = parse_length(namespace_length)?;
            let key_length = parse_length(key_length)?;
            let value_length = parse_length(value_length)?;
            let token_length = parse_length(token_length)?;

            if key_length == 0 {
                return Err(ParseError::EmptyKey);
            }
            if token_length == 0 {
                return Err(ParseError::EmptyField);
            }

            let ttl = match ttl {
                Some(ttl) => {
                    let seconds = parse_length(ttl)?;
                    let seconds = u64::try_from(seconds).map_err(|_| ParseError::InvalidLength)?;

                    Some(Duration::from_secs(seconds))
                }
                None => None,
            };

            let token_start = header_end + 1;

            let namespace_start = token_start
                .checked_add(token_length)
                .ok_or(ParseError::InvalidLength)?;

            let key_start = namespace_start
                .checked_add(namespace_length)
                .ok_or(ParseError::InvalidLength)?;

            let key_end = key_start
                .checked_add(key_length)
                .ok_or(ParseError::InvalidLength)?;

            let value_end = key_end
                .checked_add(value_length)
                .ok_or(ParseError::InvalidLength)?;

            if input.len() < value_end {
                return Err(ParseError::Incomplete);
            }

            let frame = input.split_to(value_end).freeze();
            let token = decode_field(&frame, token_start, token_length)?;
            let key = Key::new(
                frame.slice(namespace_start..key_start),
                frame.slice(key_start..key_end),
            );
            let value = frame.slice(key_end..value_end);

            Ok((
                Command::HandoffSet {
                    key,
                    value,
                    ttl,
                    if_absent,
                    token,
                },
                tag,
            ))
        }

        b"X" => {
            let joining_name_length = parts.next().ok_or(ParseError::InvalidLength)?;
            let token_length = parts.next().ok_or(ParseError::InvalidLength)?;

            if parts.next().is_some() {
                return Err(ParseError::InvalidLength);
            }

            let joining_name_length = parse_length(joining_name_length)?;
            let token_length = parse_length(token_length)?;

            if joining_name_length == 0 || token_length == 0 {
                return Err(ParseError::EmptyField);
            }

            // Body layout: `<token><joining_name>` — the token comes first
            // so the connection handler can verify it before acting on the
            // cancel (see `Command::CancelMigration::token`).
            let token_start = header_end + 1;
            let joining_name_start = token_start
                .checked_add(token_length)
                .ok_or(ParseError::InvalidLength)?;
            let joining_name_end = joining_name_start
                .checked_add(joining_name_length)
                .ok_or(ParseError::InvalidLength)?;

            if input.len() < joining_name_end {
                return Err(ParseError::Incomplete);
            }

            let frame = input.split_to(joining_name_end);
            let token = decode_field(&frame, token_start, token_length)?;
            let joining_name = decode_field(&frame, joining_name_start, joining_name_length)?;

            Ok((
                Command::CancelMigration {
                    token,
                    joining_name,
                },
                None,
            ))
        }

        b"M" => {
            let joining_name_length = parts.next().ok_or(ParseError::InvalidLength)?;
            let joining_addr_length = parts.next().ok_or(ParseError::InvalidLength)?;
            let joining_token_length = parts.next().ok_or(ParseError::InvalidLength)?;
            let joined_count = parts.next().ok_or(ParseError::InvalidLength)?;
            let replication = parts.next().ok_or(ParseError::InvalidLength)?;
            let token_length = parts.next().ok_or(ParseError::InvalidLength)?;

            if parts.next().is_some() {
                return Err(ParseError::InvalidLength);
            }

            let joining_name_length = parse_length(joining_name_length)?;
            let joining_addr_length = parse_length(joining_addr_length)?;
            let joining_token_length = parse_length(joining_token_length)?;
            let joined_count = parse_length(joined_count)?;
            let replication = parse_length(replication)?;
            let token_length = parse_length(token_length)?;

            // R=0 could never be meant (nothing would own any key) and
            // would make every ownership check vacuously reject.
            if replication == 0 {
                return Err(ParseError::InvalidLength);
            }

            parse_migrate(
                input,
                header_end,
                MigrateHeader {
                    token_length,
                    joining_name_length,
                    joining_addr_length,
                    joining_token_length,
                    joined_count,
                    replication,
                },
                progress,
            )
            .map(|command| (command, None))
        }

        _ => Err(ParseError::InvalidCommand),
    }
}

/// `M`'s already-parsed header fields, as `parse_migrate` needs them.
struct MigrateHeader {
    token_length: usize,
    joining_name_length: usize,
    joining_addr_length: usize,
    /// Issue #295: the joining node's own token's length — see
    /// `Command::Migrate::joining_token`'s doc comment.
    joining_token_length: usize,
    joined_count: usize,
    replication: usize,
}

/// Parses `M`'s body: the joining node's own name+address, followed by
/// `joined_count` entries of the same `<name-length> <addr-length>\n
/// <name><addr>` shape `nanocached-discovery`'s `L` response uses. Unlike
/// a single length-prefixed field, the joined roster's total byte length
/// can't be known from the header alone (each entry has its own embedded
/// length prefix), so this does a read-only scan to confirm everything
/// needed is already buffered before consuming any of it — preserving
/// `parse`'s "untouched on `Incomplete`" contract even though the frame
/// is variable-length and nested.
fn parse_migrate(
    input: &mut BytesMut,
    header_end: usize,
    header: MigrateHeader,
    progress: &mut MigrateProgress,
) -> Result<Command, ParseError> {
    let MigrateHeader {
        token_length,
        joining_name_length,
        joining_addr_length,
        joining_token_length,
        joined_count,
        replication,
    } = header;

    if token_length == 0
        || joining_name_length == 0
        || joining_addr_length == 0
        || joining_token_length == 0
    {
        return Err(ParseError::EmptyField);
    }

    // Body layout: `<token><joining_name><joining_addr><joining_token>
    // <entries>` — the (receiving node's own) token leads so the
    // connection handler can verify it before acting; `joining_token`
    // (issue #295) sits after the fixed `joining_name`/`joining_addr`
    // pair, still ahead of the variable-length `entries` roster, so it
    // stays a plain fixed-offset field unaffected by `entries`' own
    // resumable scan below.
    let token_start = header_end + 1;
    let joining_name_start = token_start
        .checked_add(token_length)
        .ok_or(ParseError::InvalidLength)?;
    let joining_addr_start = joining_name_start
        .checked_add(joining_name_length)
        .ok_or(ParseError::InvalidLength)?;
    let joining_token_start = joining_addr_start
        .checked_add(joining_addr_length)
        .ok_or(ParseError::InvalidLength)?;
    let mut cursor = joining_token_start
        .checked_add(joining_token_length)
        .ok_or(ParseError::InvalidLength)?;

    if input.len() < cursor {
        return Err(ParseError::Incomplete);
    }

    // Read-only pass: record each entry's span and advance `cursor`,
    // without mutating `input`, so a still-arriving trailing entry leaves
    // `input` untouched.
    //
    // `joined_count` is attacker-controlled and unbounded (a header number,
    // not tied to the buffered byte count), and this parse runs before the
    // connection authenticates, so it must not size an allocation: a tiny
    // `M 3 3 <huge>\n...` would otherwise make `Vec::with_capacity` request
    // terabytes and abort the process. Grow as real entries are confirmed
    // present instead — capacity then tracks the buffer, which is bounded.
    //
    // Resume from wherever the previous attempt's scan stopped when it is
    // still describing this very frame (see `MigrateProgress`); otherwise
    // start over from the first entry.
    let resumable = progress.header_end == header_end
        && progress.joined_count == joined_count
        && progress.cursor >= cursor
        && progress.entry_spans.len() <= joined_count
        && input.len() >= progress.cursor;
    let mut entry_spans = if resumable {
        cursor = progress.cursor;
        std::mem::take(&mut progress.entry_spans)
    } else {
        Vec::new()
    };
    *progress = MigrateProgress {
        header_end,
        joined_count,
        cursor,
        entry_spans: Vec::new(),
    };

    let scanned = scan_joined_entries(input, cursor, joined_count, &mut entry_spans);

    // Whatever got confirmed stays confirmed (`Incomplete` included): the
    // next attempt resumes after the last complete entry.
    progress.cursor = entry_spans
        .last()
        .map_or(cursor, |(_, _, addr_start, addr_length)| {
            addr_start + addr_length
        });
    progress.entry_spans = entry_spans;

    cursor = scanned?;
    let entry_spans = std::mem::take(&mut progress.entry_spans);

    // Everything needed is present: consume the whole frame in one go and
    // decode each field from the now-owned `frame`.
    let frame = input.split_to(cursor);

    let token = decode_field(&frame, token_start, token_length)?;
    let joining_name = decode_field(&frame, joining_name_start, joining_name_length)?;
    let joining_addr = decode_field(&frame, joining_addr_start, joining_addr_length)?;
    let joining_token = decode_field(&frame, joining_token_start, joining_token_length)?;

    let mut joined = Vec::with_capacity(joined_count);
    for (name_start, name_length, addr_start, addr_length) in entry_spans {
        let name = decode_field(&frame, name_start, name_length)?;
        let addr = decode_field(&frame, addr_start, addr_length)?;
        joined.push((name, addr));
    }

    Ok(Command::Migrate {
        token,
        joining_name,
        joining_addr,
        joining_token,
        joined,
        replication,
    })
}

/// `parse_migrate`'s read-only roster scan: appends one span per fully
/// buffered entry to `entry_spans`, starting after the ones already there,
/// and returns the cursor just past the last one. On `Incomplete`,
/// `entry_spans` holds every entry confirmed so far.
fn scan_joined_entries(
    input: &[u8],
    mut cursor: usize,
    joined_count: usize,
    entry_spans: &mut Vec<(usize, usize, usize, usize)>,
) -> Result<usize, ParseError> {
    for _ in entry_spans.len()..joined_count {
        let entry_header_end = cursor + find_lf(&input[cursor..]).ok_or(ParseError::Incomplete)?;
        let mut entry_parts = input[cursor..entry_header_end].split(|byte| *byte == b' ');
        let name_length = entry_parts.next().ok_or(ParseError::InvalidLength)?;
        let addr_length = entry_parts.next().ok_or(ParseError::InvalidLength)?;

        if entry_parts.next().is_some() {
            return Err(ParseError::InvalidLength);
        }

        let name_length = parse_length(name_length)?;
        let addr_length = parse_length(addr_length)?;

        if name_length == 0 || addr_length == 0 {
            return Err(ParseError::EmptyField);
        }

        let name_start = entry_header_end + 1;
        let addr_start = name_start
            .checked_add(name_length)
            .ok_or(ParseError::InvalidLength)?;
        let entry_end = addr_start
            .checked_add(addr_length)
            .ok_or(ParseError::InvalidLength)?;

        if input.len() < entry_end {
            return Err(ParseError::Incomplete);
        }

        entry_spans.push((name_start, name_length, addr_start, addr_length));
        cursor = entry_end;
    }

    Ok(cursor)
}

fn decode_field(frame: &[u8], start: usize, length: usize) -> Result<String, ParseError> {
    String::from_utf8(frame[start..start + length].to_vec()).map_err(|_| ParseError::InvalidUtf8)
}

/// Echoed response tags: in tagged mode the next header field is the required
/// request tag; in untagged mode there is nothing to consume.
fn parse_trailing_tag<'a>(
    parts: &mut impl Iterator<Item = &'a [u8]>,
    tagged: bool,
) -> Result<Option<u32>, ParseError> {
    if !tagged {
        return Ok(None);
    }

    parse_tag(parts.next().ok_or(ParseError::InvalidLength)?).map(Some)
}

/// Echoed response tags: a tag is a u32 in the same decimal encoding as every
/// length field.
fn parse_tag(input: &[u8]) -> Result<u32, ParseError> {
    u32::try_from(parse_length(input)?).map_err(|_| ParseError::InvalidLength)
}

/// Issue #141: `k`'s (and `x`'s) `<cond>` field — a fixed-shape bare
/// token, not a length-prefixed body field (same "content, not a length,
/// disambiguates it" idiom as the `A` frame's `[T] [R]` capability
/// tokens): exactly `A`, exactly `P` (only when `allow_absent_present`,
/// which `x` sets false — see `Command::CasDelete`'s doc comment), or
/// exactly 32 lowercase hex digits.
fn parse_cas_condition(
    field: &[u8],
    allow_absent_present: bool,
) -> Result<CasCondition, ParseError> {
    if allow_absent_present && field == b"A" {
        return Ok(CasCondition::Absent);
    }
    if allow_absent_present && field == b"P" {
        return Ok(CasCondition::Present);
    }
    if field.len() == 32 {
        let mut digest = [0u8; 16];
        for (byte, chunk) in digest.iter_mut().zip(field.chunks_exact(2)) {
            *byte = (hex_nibble(chunk[0])? << 4) | hex_nibble(chunk[1])?;
        }
        return Ok(CasCondition::Digest(digest));
    }
    Err(ParseError::InvalidCommand)
}

fn hex_nibble(byte: u8) -> Result<u8, ParseError> {
    match byte {
        b'0'..=b'9' => Ok(byte - b'0'),
        b'a'..=b'f' => Ok(byte - b'a' + 10),
        _ => Err(ParseError::InvalidCommand),
    }
}

fn find_lf(input: &[u8]) -> Option<usize> {
    input.iter().position(|byte| *byte == b'\n')
}

fn parse_length(input: &[u8]) -> Result<usize, ParseError> {
    if input.is_empty() {
        return Err(ParseError::InvalidLength);
    }

    input.iter().try_fold(0usize, |length, byte| {
        if !byte.is_ascii_digit() {
            return Err(ParseError::InvalidLength);
        }

        length
            .checked_mul(10)
            .and_then(|length| length.checked_add((byte - b'0') as usize))
            .ok_or(ParseError::InvalidLength)
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn key(name: &[u8]) -> Key {
        Key::unnamespaced(Bytes::copy_from_slice(name))
    }

    fn namespaced(namespace: &[u8], name: &[u8]) -> Key {
        Key::new(
            Bytes::copy_from_slice(namespace),
            Bytes::copy_from_slice(name),
        )
    }

    fn buf(bytes: &[u8]) -> BytesMut {
        BytesMut::from(bytes)
    }

    #[test]
    fn parses_auth_command() {
        let mut input = buf(b"A 6\nsecret");

        assert_eq!(
            parse(&mut input),
            Ok(Command::Auth {
                secret: Bytes::from_static(b"secret"),
                tagging: false,
                retry_capable: false,
            })
        );
        assert!(input.is_empty());
    }

    #[test]
    fn parses_auth_command_with_retry_capability() {
        // Issue #125: `[T] [R]` in that order; `R` alone is valid too.
        let mut input = buf(b"A 6 T R\nsecret");
        assert_eq!(
            parse(&mut input),
            Ok(Command::Auth {
                secret: Bytes::from_static(b"secret"),
                tagging: true,
                retry_capable: true,
            })
        );
        assert!(input.is_empty());

        let mut input = buf(b"A 6 R\nsecret");
        assert_eq!(
            parse(&mut input),
            Ok(Command::Auth {
                secret: Bytes::from_static(b"secret"),
                tagging: false,
                retry_capable: true,
            })
        );
        assert!(input.is_empty());
    }

    #[test]
    fn rejects_auth_capability_tokens_out_of_order_or_unknown() {
        // `R` before `T` parses `R`, then trips the trailing-field
        // check — a rejection either way, just via the length error.
        let mut input = buf(b"A 6 R T\nsecret");
        assert_eq!(parse(&mut input), Err(ParseError::InvalidLength));

        let mut input = buf(b"A 6 T X\nsecret");
        assert_eq!(parse(&mut input), Err(ParseError::InvalidCommand));
    }

    #[test]
    fn parses_auth_command_with_tagging_flag() {
        let mut input = buf(b"A 6 T\nsecret");

        assert_eq!(
            parse(&mut input),
            Ok(Command::Auth {
                secret: Bytes::from_static(b"secret"),
                tagging: true,
                retry_capable: false,
            })
        );
        assert!(input.is_empty());
    }

    #[test]
    fn rejects_an_auth_flag_other_than_t() {
        let mut input = buf(b"A 6 X\nsecret");

        assert_eq!(parse(&mut input), Err(ParseError::InvalidCommand));
    }

    #[test]
    fn rejects_empty_secret_for_auth() {
        let mut input = buf(b"A 0\n");

        assert_eq!(parse(&mut input), Err(ParseError::EmptySecret));
    }

    #[test]
    fn returns_incomplete_when_auth_secret_is_incomplete() {
        let mut input = buf(b"A 6\nsec");

        assert_eq!(parse(&mut input), Err(ParseError::Incomplete));
        assert_eq!(&input[..], b"A 6\nsec");
    }

    #[test]
    fn parses_get_command() {
        let mut input = buf(b"G 4\nname");

        assert_eq!(parse(&mut input), Ok(Command::Get { key: key(b"name") }));
        assert!(input.is_empty());
    }

    #[test]
    fn parses_set_command_without_ttl() {
        let mut input = buf(b"S 4 5\nnameAlice");

        assert_eq!(
            parse(&mut input),
            Ok(Command::Set {
                key: key(b"name"),
                value: Bytes::from_static(b"Alice"),
                ttl: None,
            })
        );
        assert!(input.is_empty());
    }

    #[test]
    fn parses_set_command_with_ttl() {
        let mut input = buf(b"S 4 5 10\nnameAlice");

        assert_eq!(
            parse(&mut input),
            Ok(Command::Set {
                key: key(b"name"),
                value: Bytes::from_static(b"Alice"),
                ttl: Some(Duration::from_secs(10)),
            })
        );
        assert!(input.is_empty());
    }

    #[test]
    fn parses_delete_command() {
        let mut input = buf(b"D 4\nname");

        assert_eq!(parse(&mut input), Ok(Command::Delete { key: key(b"name") }));
        assert!(input.is_empty());
    }

    #[test]
    fn returns_incomplete_when_header_is_incomplete() {
        let mut input = buf(b"G 4");

        assert_eq!(parse(&mut input), Err(ParseError::Incomplete));
        assert_eq!(&input[..], b"G 4");
    }

    #[test]
    fn returns_incomplete_when_key_is_incomplete() {
        let mut input = buf(b"G 4\nna");

        assert_eq!(parse(&mut input), Err(ParseError::Incomplete));
        assert_eq!(&input[..], b"G 4\nna");
    }

    #[test]
    fn returns_incomplete_when_set_value_is_incomplete() {
        let mut input = buf(b"S 4 5\nnameAli");

        assert_eq!(parse(&mut input), Err(ParseError::Incomplete));
        assert_eq!(&input[..], b"S 4 5\nnameAli");
    }

    #[test]
    fn rejects_non_numeric_key_length() {
        let mut input = buf(b"G abc\nname");

        assert_eq!(parse(&mut input), Err(ParseError::InvalidLength));
    }

    #[test]
    fn rejects_empty_key_for_get() {
        let mut input = buf(b"G 0\n");

        assert_eq!(parse(&mut input), Err(ParseError::EmptyKey));
    }

    #[test]
    fn rejects_empty_key_for_delete() {
        let mut input = buf(b"D 0\n");

        assert_eq!(parse(&mut input), Err(ParseError::EmptyKey));
    }

    #[test]
    fn rejects_empty_key_for_set() {
        let mut input = buf(b"S 0 5\nAlice");

        assert_eq!(parse(&mut input), Err(ParseError::EmptyKey));
    }

    #[test]
    fn rejects_empty_key_without_waiting_for_body() {
        let mut input = buf(b"S 0 1000000\n");

        assert_eq!(parse(&mut input), Err(ParseError::EmptyKey));
    }

    #[test]
    fn parses_set_command_with_empty_value() {
        let mut input = buf(b"S 4 0\nname");

        assert_eq!(
            parse(&mut input),
            Ok(Command::Set {
                key: key(b"name"),
                value: Bytes::new(),
                ttl: None,
            })
        );
    }

    #[test]
    fn rejects_unknown_command() {
        let mut input = buf(b"UNKNOWN 4\nname");

        assert_eq!(parse(&mut input), Err(ParseError::InvalidCommand));
    }

    #[test]
    fn rejects_unknown_command_without_waiting_for_body() {
        let mut input = buf(b"UNKNOWN 100\n");

        assert_eq!(parse(&mut input), Err(ParseError::InvalidCommand));
    }

    #[test]
    fn leaves_the_next_request_untouched() {
        let mut input = buf(b"G 4\nnameG 3\nage");

        parse(&mut input).unwrap();

        assert_eq!(&input[..], b"G 3\nage");
    }

    #[test]
    fn parses_binary_key() {
        let mut input = buf(b"G 3\n\xff\x00a");

        assert_eq!(
            parse(&mut input),
            Ok(Command::Get {
                key: Key::from(Bytes::from(vec![0xff, 0x00, b'a'])),
            })
        );
    }

    #[test]
    fn get_returns_value_for_existing_key() {
        let mut cache = Cache::new(usize::MAX);
        cache.set(key(b"name"), Bytes::from_static(b"Alice"));

        let command = Command::Get { key: key(b"name") };

        assert_eq!(
            command.execute(&mut cache),
            Response::Value(Bytes::from_static(b"Alice")),
        );
    }

    #[test]
    fn get_returns_not_found_for_missing_key() {
        let mut cache = Cache::new(usize::MAX);

        let command = Command::Get { key: key(b"name") };

        assert_eq!(command.execute(&mut cache), Response::NotFound);
    }

    #[test]
    #[should_panic(
        expected = "Auth, Migrate, and CancelMigration are handled by the connection handler"
    )]
    fn execute_panics_on_auth() {
        let mut cache = Cache::new(usize::MAX);

        let command = Command::Auth {
            secret: Bytes::from_static(b"secret"),
            tagging: false,
            retry_capable: false,
        };

        let _ = command.execute(&mut cache);
    }

    #[test]
    fn set_stores_value() {
        let mut cache = Cache::new(usize::MAX);

        let command = Command::Set {
            key: key(b"name"),
            value: Bytes::from_static(b"Alice"),
            ttl: None,
        };

        assert_eq!(command.execute(&mut cache), Response::Stored);
        assert_eq!(cache.get(&key(b"name")), Some(Bytes::from_static(b"Alice")));
    }

    #[test]
    fn set_with_zero_ttl_expires_immediately() {
        let mut cache = Cache::new(usize::MAX);

        let command = Command::Set {
            key: key(b"name"),
            value: Bytes::from_static(b"Alice"),
            ttl: Some(Duration::ZERO),
        };

        assert_eq!(command.execute(&mut cache), Response::Stored);

        assert_eq!(cache.get(&key(b"name")), None);
    }

    #[test]
    fn delete_returns_deleted_for_existing_key() {
        let mut cache = Cache::new(usize::MAX);
        cache.set(key(b"name"), Bytes::from_static(b"Alice"));

        let command = Command::Delete { key: key(b"name") };

        assert_eq!(command.execute(&mut cache), Response::Deleted);
    }

    #[test]
    fn delete_returns_not_found_for_missing_key() {
        let mut cache = Cache::new(usize::MAX);

        let command = Command::Delete { key: key(b"name") };

        assert_eq!(command.execute(&mut cache), Response::NotFound);
    }

    #[test]
    fn list_entries_returns_every_stored_key() {
        let mut cache = Cache::new(usize::MAX);
        cache.set(key(b"name"), Bytes::from_static(b"Alice"));

        assert_eq!(
            Command::ListEntries.execute(&mut cache),
            Response::Keys(vec![key(b"name")])
        );
    }

    #[test]
    fn peek_entry_returns_the_matching_entry() {
        let mut cache = Cache::new(usize::MAX);
        cache.set(key(b"name"), Bytes::from_static(b"Alice"));

        let command = Command::PeekEntry { key: key(b"name") };

        assert_eq!(
            command.execute(&mut cache),
            Response::Entries(vec![(key(b"name"), Bytes::from_static(b"Alice"), None)])
        );
    }

    #[test]
    fn peek_entry_returns_no_entries_for_a_missing_key() {
        let mut cache = Cache::new(usize::MAX);

        let command = Command::PeekEntry {
            key: key(b"missing"),
        };

        assert_eq!(command.execute(&mut cache), Response::Entries(Vec::new()));
    }

    #[test]
    fn mark_migrated_returns_marked() {
        let mut cache = Cache::new(usize::MAX);
        cache.set(key(b"name"), Bytes::from_static(b"Alice"));

        let command = Command::MarkMigrated { key: key(b"name") };

        assert_eq!(command.execute(&mut cache), Response::Marked);
    }

    #[test]
    fn sweep_returns_how_many_entries_it_removed() {
        let mut cache = Cache::new(usize::MAX);
        cache.set(key(b"name"), Bytes::from_static(b"Alice"));
        cache.mark_migrated(&key(b"name"));

        assert_eq!(
            Command::Sweep { marked: true }.execute(&mut cache),
            Response::Swept(1)
        );
    }

    #[test]
    fn sweep_without_marked_leaves_marked_entries_in_place() {
        let mut cache = Cache::new(usize::MAX);
        cache.set(key(b"name"), Bytes::from_static(b"Alice"));
        cache.mark_migrated(&key(b"name"));

        assert_eq!(
            Command::Sweep { marked: false }.execute(&mut cache),
            Response::Swept(0)
        );
        assert_eq!(cache.get(&key(b"name")), Some(Bytes::from_static(b"Alice")));
    }

    #[test]
    fn parses_a_migrate_command_with_no_joined_nodes() {
        let mut input = buf(b"M 6 14 5 0 2 5\ntok-bnode-b127.0.0.1:8357tok-j");

        assert_eq!(
            parse(&mut input),
            Ok(Command::Migrate {
                token: "tok-b".to_string(),
                joining_name: "node-b".to_string(),
                joining_addr: "127.0.0.1:8357".to_string(),
                joining_token: "tok-j".to_string(),
                joined: Vec::new(),
                replication: 2,
            })
        );
        assert!(input.is_empty());
    }

    #[test]
    fn parses_a_migrate_command_with_joined_nodes_and_consumes_only_that_frame() {
        let mut input = buf(
            b"M 6 14 5 2 2 5\ntok-bnode-b127.0.0.1:8357tok-j6 14\nnode-a127.0.0.1:83566 14\nnode-c127.0.0.1:8358G 1\nx",
        );

        assert_eq!(
            parse(&mut input),
            Ok(Command::Migrate {
                token: "tok-b".to_string(),
                joining_name: "node-b".to_string(),
                joining_addr: "127.0.0.1:8357".to_string(),
                joining_token: "tok-j".to_string(),
                joined: vec![
                    ("node-a".to_string(), "127.0.0.1:8356".to_string()),
                    ("node-c".to_string(), "127.0.0.1:8358".to_string()),
                ],
                replication: 2,
            })
        );
        assert_eq!(&input[..], b"G 1\nx");
    }

    #[test]
    fn parse_leaves_a_migrate_command_untouched_when_a_joined_entry_is_incomplete() {
        let original =
            b"M 6 14 5 1 2 5\ntok-bnode-b127.0.0.1:8357tok-j6 14\nnode-a127.0.0".to_vec();
        let mut input = BytesMut::from(&original[..]);

        assert_eq!(parse(&mut input), Err(ParseError::Incomplete));
        assert_eq!(&input[..], &original[..]);
    }

    #[test]
    fn parse_resumable_picks_a_migrate_roster_scan_up_where_it_left_off() {
        // Feed a frame one byte at a time and check (a) the result equals
        // the one-shot parse, (b) each retry only re-scans from the last
        // fully-buffered entry, never from entry #1.
        let frame =
            b"M 6 14 5 3 2 5\ntok-bnode-b127.0.0.1:8357tok-j6 14\nnode-a127.0.0.1:83566 14\n\
                      node-c127.0.0.1:83586 14\nnode-d127.0.0.1:8359G 1\nx";
        let mut expected = BytesMut::from(&frame[..]);
        let expected = parse(&mut expected).unwrap();

        let mut input = BytesMut::new();
        let mut progress = MigrateProgress::default();
        let mut entries_seen = 0;
        let mut parsed = None;

        for (index, byte) in frame.iter().enumerate() {
            input.extend_from_slice(&[*byte]);

            match parse_resumable(&mut input, false, &mut progress) {
                Err(ParseError::Incomplete) => {
                    // Never regresses: the recorded spans only grow.
                    assert!(
                        progress.entry_spans.len() >= entries_seen,
                        "at byte {index}"
                    );
                    entries_seen = progress.entry_spans.len();
                }
                Ok((command, None)) => {
                    parsed = Some(command);
                    break;
                }
                other => panic!("unexpected {other:?} at byte {index}"),
            }
        }

        assert_eq!(parsed.unwrap(), expected);
        assert_eq!(
            entries_seen, 2,
            "the last entry completes together with the frame"
        );
        // Consumed the frame (the trailing `G` hasn't been fed yet) and
        // reset for whatever comes next.
        assert!(input.is_empty());
        assert_eq!(progress.entry_spans.len(), 0);
        assert_eq!(progress.cursor, 0);
    }

    #[test]
    fn parse_resumable_ignores_progress_recorded_for_a_different_frame() {
        let mut progress = MigrateProgress {
            header_end: 12,
            joined_count: 2,
            cursor: 1_000_000,
            entry_spans: vec![(0, 0, 0, 0)],
        };
        let mut input = buf(
            b"M 6 14 5 2 2 5\ntok-bnode-b127.0.0.1:8357tok-j6 14\nnode-a127.0.0.1:83566 14\nnode-c127.0.0.1:8358",
        );

        let (command, _) = parse_resumable(&mut input, false, &mut progress).unwrap();

        assert!(matches!(command, Command::Migrate { joined, .. } if joined.len() == 2));
    }

    #[test]
    fn rejects_an_empty_joining_name_in_migrate() {
        let mut input = buf(b"M 0 14 5 0 2 5\ntok-b127.0.0.1:8357");

        assert_eq!(parse(&mut input), Err(ParseError::EmptyField));
    }

    #[test]
    fn rejects_an_empty_token_in_migrate() {
        let mut input = buf(b"M 6 14 5 0 2 0\nnode-b127.0.0.1:8357");

        assert_eq!(parse(&mut input), Err(ParseError::EmptyField));
    }

    #[test]
    fn rejects_an_empty_joining_token_in_migrate() {
        // Issue #295: `joining_token` gets the same empty-field rejection
        // as `token`/`joining_name`/`joining_addr`.
        let mut input = buf(b"M 6 14 0 0 2 5\ntok-bnode-b127.0.0.1:8357");

        assert_eq!(parse(&mut input), Err(ParseError::EmptyField));
    }

    #[test]
    fn migrate_with_a_huge_joined_count_reports_incomplete_without_pre_allocating() {
        // `joined_count` is attacker-controlled and unbounded; parsing must
        // not size an allocation from it (a `Vec::with_capacity` on this value
        // would request terabytes and abort the process). With the joining
        // node's own fields present but no entries buffered, this must simply
        // report `Incomplete` — cheaply, without touching that huge number.
        let mut input = buf(b"M 6 14 5 999999999999 2 5\ntok-bnode-b127.0.0.1:8357tok-j");

        assert_eq!(parse(&mut input), Err(ParseError::Incomplete));
    }

    #[test]
    fn parse_tagged_requires_and_returns_the_tag_on_get_and_delete() {
        let mut input = buf(b"G 4 7\nnameD 4 4294967295\nname");

        assert_eq!(
            parse_tagged(&mut input),
            Ok((Command::Get { key: key(b"name") }, Some(7),))
        );
        assert_eq!(
            parse_tagged(&mut input),
            Ok((Command::Delete { key: key(b"name") }, Some(u32::MAX),))
        );
        assert!(input.is_empty());
    }

    #[test]
    fn parse_tagged_rejects_a_get_without_a_tag() {
        let mut input = buf(b"G 4\nname");

        assert_eq!(parse_tagged(&mut input), Err(ParseError::InvalidLength));
    }

    #[test]
    fn parse_tagged_reads_a_three_field_set_header_as_untimed() {
        let mut input = buf(b"S 4 5 9\nnameAlice");

        assert_eq!(
            parse_tagged(&mut input),
            Ok((
                Command::Set {
                    key: key(b"name"),
                    value: Bytes::from_static(b"Alice"),
                    ttl: None,
                },
                Some(9),
            ))
        );
        assert!(input.is_empty());
    }

    #[test]
    fn parse_tagged_reads_a_four_field_set_header_as_ttl_then_tag() {
        let mut input = buf(b"S 4 5 10 9\nnameAlice");

        assert_eq!(
            parse_tagged(&mut input),
            Ok((
                Command::Set {
                    key: key(b"name"),
                    value: Bytes::from_static(b"Alice"),
                    ttl: Some(Duration::from_secs(10)),
                },
                Some(9),
            ))
        );
        assert!(input.is_empty());
    }

    #[test]
    fn parse_tagged_rejects_a_tag_beyond_u32() {
        let mut input = buf(b"G 4 4294967296\nname");

        assert_eq!(parse_tagged(&mut input), Err(ParseError::InvalidLength));
    }

    #[test]
    fn parse_tagged_returns_no_tag_for_auth() {
        let mut input = buf(b"A 6 T\nsecret");

        assert_eq!(
            parse_tagged(&mut input),
            Ok((
                Command::Auth {
                    secret: Bytes::from_static(b"secret"),
                    tagging: true,
                    retry_capable: false,
                },
                None,
            ))
        );
    }

    #[test]
    fn parses_namespaced_get_set_and_delete() {
        // Issue #105: `<namespace-length>` leads, namespace bytes lead the body.
        let mut input = buf(b"g 5 4\nusersname");
        assert_eq!(
            parse(&mut input),
            Ok(Command::Get {
                key: namespaced(b"users", b"name"),
            })
        );
        assert!(input.is_empty());

        let mut input = buf(b"s 5 4 5 30\nusersnameAlice");
        assert_eq!(
            parse(&mut input),
            Ok(Command::Set {
                key: namespaced(b"users", b"name"),
                value: Bytes::from_static(b"Alice"),
                ttl: Some(Duration::from_secs(30)),
            })
        );
        assert!(input.is_empty());

        let mut input = buf(b"d 5 4\nusersname");
        assert_eq!(
            parse(&mut input),
            Ok(Command::Delete {
                key: namespaced(b"users", b"name"),
            })
        );
        assert!(input.is_empty());
    }

    #[test]
    fn parses_incr() {
        // Issue #129: always namespaced, `<delta>` ahead of the optional
        // trailing tag — like `g`/`d`, but with an extra mandatory field.
        let mut input = buf(b"i 5 4 3\nusersname");
        assert_eq!(
            parse(&mut input),
            Ok(Command::Incr {
                key: namespaced(b"users", b"name"),
                delta: 3,
            })
        );
        assert!(input.is_empty());

        let mut input = buf(b"i 0 4 -3\nname");
        assert_eq!(
            parse(&mut input),
            Ok(Command::Incr {
                key: key(b"name"),
                delta: -3,
            })
        );
    }

    #[test]
    fn incr_carries_the_tag_last_in_tagged_mode() {
        let mut input = buf(b"i 5 4 3 7\nusersname");
        assert_eq!(
            parse_tagged(&mut input),
            Ok((
                Command::Incr {
                    key: namespaced(b"users", b"name"),
                    delta: 3,
                },
                Some(7),
            ))
        );
    }

    #[test]
    fn incr_rejects_a_malformed_delta_as_a_fatal_parse_error() {
        // Unlike a stored non-numeric value (which answers the wire's `T`
        // status — see execute()'s test below), a malformed `<delta>`
        // field is the client's own frame being broken, same severity as
        // a malformed length field.
        let mut input = buf(b"i 5 4 abc\nusersname");
        assert_eq!(parse(&mut input), Err(ParseError::InvalidLength));

        let mut input = buf(b"i 5 4 +3\nusersname");
        assert_eq!(parse(&mut input), Err(ParseError::InvalidLength));

        let mut input = buf(b"i 5 4 03\nusersname");
        assert_eq!(parse(&mut input), Err(ParseError::InvalidLength));
    }

    #[test]
    fn incr_requires_the_namespace_length_field() {
        let mut input = buf(b"i 4 3\nname");
        assert_eq!(parse(&mut input), Err(ParseError::InvalidLength));
    }

    #[test]
    fn incr_rejects_an_empty_key() {
        let mut input = buf(b"i 5 0 1\nusers");
        assert_eq!(parse(&mut input), Err(ParseError::EmptyKey));
    }

    #[test]
    fn parse_leaves_an_incr_command_untouched_when_incomplete() {
        let original = b"i 5 4 3\nusersnam".to_vec();
        let mut input = buf(&original);

        assert_eq!(parse(&mut input), Err(ParseError::Incomplete));
        assert_eq!(&input[..], &original[..]);
    }

    #[test]
    fn incr_executes_against_the_cache_and_maps_every_outcome() {
        let mut cache = Cache::new(usize::MAX);

        // Missing key -> NotFound, same status as G/D would give.
        assert_eq!(
            Command::Incr {
                key: key(b"counter"),
                delta: 1,
            }
            .execute(&mut cache),
            Response::NotFound
        );

        Command::Set {
            key: key(b"counter"),
            value: Bytes::from_static(b"10"),
            ttl: None,
        }
        .execute(&mut cache);

        assert_eq!(
            Command::Incr {
                key: key(b"counter"),
                delta: 5,
            }
            .execute(&mut cache),
            Response::Incremented(Bytes::from_static(b"15"), None)
        );

        // A stored non-numeric value -> the wire's T status, not NotFound.
        Command::Set {
            key: key(b"name"),
            value: Bytes::from_static(b"Alice"),
            ttl: None,
        }
        .execute(&mut cache);

        assert_eq!(
            Command::Incr {
                key: key(b"name"),
                delta: 1,
            }
            .execute(&mut cache),
            Response::NotNumeric
        );
    }

    #[test]
    fn parses_cas_set_with_absent_condition() {
        let mut input = buf(b"k 5 4 5 A\nusersnameAlice");
        assert_eq!(
            parse(&mut input),
            Ok(Command::CasSet {
                key: namespaced(b"users", b"name"),
                condition: CasCondition::Absent,
                value: Bytes::from_static(b"Alice"),
                ttl: None,
            })
        );
        assert!(input.is_empty());
    }

    #[test]
    fn parses_cas_set_with_present_condition() {
        let mut input = buf(b"k 0 4 3 P\nnameBob");
        assert_eq!(
            parse(&mut input),
            Ok(Command::CasSet {
                key: key(b"name"),
                condition: CasCondition::Present,
                value: Bytes::from_static(b"Bob"),
                ttl: None,
            })
        );
    }

    #[test]
    fn parses_cas_set_with_a_digest_condition_and_a_ttl() {
        let mut input = buf(b"k 0 4 3 3bc51062973c458d5a6f2d8d64a02324 30\nnameBob");
        assert_eq!(
            parse(&mut input),
            Ok(Command::CasSet {
                key: key(b"name"),
                condition: CasCondition::Digest([
                    0x3b, 0xc5, 0x10, 0x62, 0x97, 0x3c, 0x45, 0x8d, 0x5a, 0x6f, 0x2d, 0x8d, 0x64,
                    0xa0, 0x23, 0x24,
                ]),
                value: Bytes::from_static(b"Bob"),
                ttl: Some(Duration::from_secs(30)),
            })
        );
    }

    #[test]
    fn cas_set_carries_the_tag_last_in_tagged_mode() {
        let mut input = buf(b"k 0 4 3 A 7\nnameBob");
        assert_eq!(
            parse_tagged(&mut input),
            Ok((
                Command::CasSet {
                    key: key(b"name"),
                    condition: CasCondition::Absent,
                    value: Bytes::from_static(b"Bob"),
                    ttl: None,
                },
                Some(7),
            ))
        );

        let mut input = buf(b"k 0 4 3 A 30 7\nnameBob");
        assert_eq!(
            parse_tagged(&mut input),
            Ok((
                Command::CasSet {
                    key: key(b"name"),
                    condition: CasCondition::Absent,
                    value: Bytes::from_static(b"Bob"),
                    ttl: Some(Duration::from_secs(30)),
                },
                Some(7),
            ))
        );
    }

    #[test]
    fn cas_set_rejects_a_malformed_condition_as_a_fatal_parse_error() {
        // Wrong length (neither 1 nor 32).
        let mut input = buf(b"k 0 4 3 AB\nnameBob");
        assert_eq!(parse(&mut input), Err(ParseError::InvalidCommand));

        // Uppercase hex is rejected — the digest's canonical form is
        // lowercase only, same rigor as INCR's decimal grammar.
        let mut input = buf(b"k 0 4 3 3BC51062973C458D5A6F2D8D64A02324\nnameBob");
        assert_eq!(parse(&mut input), Err(ParseError::InvalidCommand));

        // A non-hex 32-byte token.
        let mut input = buf(b"k 0 4 3 zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz\nnameBob");
        assert_eq!(parse(&mut input), Err(ParseError::InvalidCommand));
    }

    #[test]
    fn cas_set_requires_the_namespace_length_field() {
        let mut input = buf(b"k 4 3 A\nnameBob");
        assert_eq!(parse(&mut input), Err(ParseError::InvalidLength));
    }

    #[test]
    fn cas_set_rejects_an_empty_key() {
        let mut input = buf(b"k 0 0 3 A\nBob");
        assert_eq!(parse(&mut input), Err(ParseError::EmptyKey));
    }

    #[test]
    fn parse_leaves_a_cas_set_command_untouched_when_incomplete() {
        let original = b"k 0 4 5 A\nnameAli".to_vec();
        let mut input = buf(&original);

        assert_eq!(parse(&mut input), Err(ParseError::Incomplete));
        assert_eq!(&input[..], &original[..]);
    }

    #[test]
    fn cas_set_executes_against_the_cache_and_maps_every_outcome() {
        let mut cache = Cache::new(usize::MAX);

        assert_eq!(
            Command::CasSet {
                key: key(b"name"),
                condition: CasCondition::Present,
                value: Bytes::from_static(b"Bob"),
                ttl: None,
            }
            .execute(&mut cache),
            Response::NotFound
        );

        assert_eq!(
            Command::CasSet {
                key: key(b"name"),
                condition: CasCondition::Absent,
                value: Bytes::from_static(b"Alice"),
                ttl: None,
            }
            .execute(&mut cache),
            Response::Stored
        );

        assert_eq!(
            Command::CasSet {
                key: key(b"name"),
                condition: CasCondition::Digest(crate::cache::content_digest(b"Alice")),
                value: Bytes::from_static(b"Bob"),
                ttl: None,
            }
            .execute(&mut cache),
            Response::Stored
        );
    }

    #[test]
    fn parses_cas_delete() {
        let mut input = buf(b"x 5 4 3bc51062973c458d5a6f2d8d64a02324\nusersname");
        assert_eq!(
            parse(&mut input),
            Ok(Command::CasDelete {
                key: namespaced(b"users", b"name"),
                expected_digest: [
                    0x3b, 0xc5, 0x10, 0x62, 0x97, 0x3c, 0x45, 0x8d, 0x5a, 0x6f, 0x2d, 0x8d, 0x64,
                    0xa0, 0x23, 0x24,
                ],
            })
        );
        assert!(input.is_empty());
    }

    #[test]
    fn cas_delete_carries_the_tag_in_tagged_mode() {
        let mut input = buf(b"x 0 4 3bc51062973c458d5a6f2d8d64a02324 7\nname");
        assert_eq!(
            parse_tagged(&mut input),
            Ok((
                Command::CasDelete {
                    key: key(b"name"),
                    expected_digest: [
                        0x3b, 0xc5, 0x10, 0x62, 0x97, 0x3c, 0x45, 0x8d, 0x5a, 0x6f, 0x2d, 0x8d,
                        0x64, 0xa0, 0x23, 0x24,
                    ],
                },
                Some(7),
            ))
        );
    }

    #[test]
    fn cas_delete_rejects_absent_or_present_tokens_as_a_fatal_parse_error() {
        // `A`/`P` are meaningless for delete — an absent- or
        // present-only conditioned delete is already the plain,
        // unconditional `d`.
        let mut input = buf(b"x 0 4 A\nname");
        assert_eq!(parse(&mut input), Err(ParseError::InvalidCommand));

        let mut input = buf(b"x 0 4 P\nname");
        assert_eq!(parse(&mut input), Err(ParseError::InvalidCommand));
    }

    #[test]
    fn cas_delete_rejects_an_empty_key() {
        let mut input = buf(b"x 5 0 3bc51062973c458d5a6f2d8d64a02324\nusers");
        assert_eq!(parse(&mut input), Err(ParseError::EmptyKey));
    }

    #[test]
    fn parse_leaves_a_cas_delete_command_untouched_when_incomplete() {
        let original = b"x 5 4 3bc51062973c458d5a6f2d8d64a02324\nusersnam".to_vec();
        let mut input = buf(&original);

        assert_eq!(parse(&mut input), Err(ParseError::Incomplete));
        assert_eq!(&input[..], &original[..]);
    }

    #[test]
    fn cas_delete_executes_against_the_cache_and_maps_every_outcome() {
        let mut cache = Cache::new(usize::MAX);

        assert_eq!(
            Command::CasDelete {
                key: key(b"name"),
                expected_digest: crate::cache::content_digest(b"Alice"),
            }
            .execute(&mut cache),
            Response::NotFound
        );

        Command::Set {
            key: key(b"name"),
            value: Bytes::from_static(b"Alice"),
            ttl: None,
        }
        .execute(&mut cache);

        assert_eq!(
            Command::CasDelete {
                key: key(b"name"),
                expected_digest: crate::cache::content_digest(b"someone-else"),
            }
            .execute(&mut cache),
            Response::NotFound
        );

        assert_eq!(
            Command::CasDelete {
                key: key(b"name"),
                expected_digest: crate::cache::content_digest(b"Alice"),
            }
            .execute(&mut cache),
            Response::Deleted
        );
    }

    #[test]
    fn parses_handoff_set_and_delete() {
        // Issue #124: `U`/`u` share `s`/`d`'s namespaced shape (plus,
        // issue #295, a leading `<token-len>`/`<token>`) and map to the
        // handoff variants (the connection handler skips the wrong-node
        // check for them, but verifies `token` first).
        let mut input = buf(b"U 5 4 5 5 30\ntok-husersnameAlice");
        assert_eq!(
            parse(&mut input),
            Ok(Command::HandoffSet {
                key: namespaced(b"users", b"name"),
                value: Bytes::from_static(b"Alice"),
                ttl: Some(Duration::from_secs(30)),
                if_absent: false,
                token: "tok-h".to_string(),
            })
        );
        assert!(input.is_empty());

        let mut input = buf(b"u 5 4 5\ntok-husersname");
        assert_eq!(
            parse(&mut input),
            Ok(Command::HandoffDelete {
                key: namespaced(b"users", b"name"),
                token: "tok-h".to_string(),
            })
        );
        assert!(input.is_empty());
    }

    #[test]
    fn rejects_an_empty_token_in_handoff_set_and_delete() {
        // Issue #295: `token` gets the same empty-field rejection as
        // `key`.
        let mut input = buf(b"U 5 4 5 0\nusersnameAlice");
        assert_eq!(parse(&mut input), Err(ParseError::EmptyField));

        let mut input = buf(b"u 5 4 0\nusersname");
        assert_eq!(parse(&mut input), Err(ParseError::EmptyField));
    }

    #[test]
    fn parses_handoff_set_with_the_trailing_absent_token() {
        // Issue #266: `U`'s optional put-if-absent marker, in every
        // position it can legally appear — with and without a ttl, and
        // (in tagged mode) with and without one.
        let mut input = buf(b"U 5 4 5 5 A\ntok-husersnameAlice");
        assert_eq!(
            parse(&mut input),
            Ok(Command::HandoffSet {
                key: namespaced(b"users", b"name"),
                value: Bytes::from_static(b"Alice"),
                ttl: None,
                if_absent: true,
                token: "tok-h".to_string(),
            })
        );

        let mut input = buf(b"U 5 4 5 5 30 A\ntok-husersnameAlice");
        assert_eq!(
            parse(&mut input),
            Ok(Command::HandoffSet {
                key: namespaced(b"users", b"name"),
                value: Bytes::from_static(b"Alice"),
                ttl: Some(Duration::from_secs(30)),
                if_absent: true,
                token: "tok-h".to_string(),
            })
        );

        // `S`/`s` never take the `A` token — it's read back as (and
        // fails to parse as) a ttl instead.
        let mut input = buf(b"S 4 5 A\nnameAlice");
        assert_eq!(parse(&mut input), Err(ParseError::InvalidLength));
    }

    #[test]
    fn handoff_set_with_absent_condition_stores_only_when_the_key_is_missing() {
        // Issue #266: re-replication after an eviction must not clobber a
        // newer client write that raced it — but the receiver still acks
        // success either way (checked at the connection-handler level,
        // not here; `execute` only needs to leave the winning value
        // alone).
        let mut cache = Cache::new(1024 * 1024);

        assert_eq!(
            Command::HandoffSet {
                key: key(b"name"),
                value: Bytes::from_static(b"first"),
                ttl: None,
                if_absent: true,
                token: "tok-h".to_string(),
            }
            .execute(&mut cache),
            Response::Stored
        );
        assert_eq!(cache.get(&key(b"name")), Some(Bytes::from_static(b"first")));

        // A newer write already raced ahead of the re-replication.
        assert_eq!(
            Command::HandoffSet {
                key: key(b"name"),
                value: Bytes::from_static(b"stale"),
                ttl: None,
                if_absent: true,
                token: "tok-h".to_string(),
            }
            .execute(&mut cache),
            Response::Stored
        );
        assert_eq!(cache.get(&key(b"name")), Some(Bytes::from_static(b"first")));
    }

    #[test]
    fn a_zero_length_namespace_is_the_default_namespace() {
        let mut input = buf(b"g 0 4\nname");
        assert_eq!(parse(&mut input), Ok(Command::Get { key: key(b"name") }));

        let mut input = buf(b"s 0 4 5\nnameAlice");
        assert_eq!(
            parse(&mut input),
            Ok(Command::Set {
                key: key(b"name"),
                value: Bytes::from_static(b"Alice"),
                ttl: None,
            })
        );
    }

    #[test]
    fn namespaces_are_binary_safe() {
        let mut input = buf(b"g 2 1\n\xff\x00k");
        assert_eq!(
            parse(&mut input),
            Ok(Command::Get {
                key: namespaced(b"\xff\x00", b"k"),
            })
        );
    }

    #[test]
    fn namespaced_commands_carry_the_tag_last_in_tagged_mode() {
        let mut input = buf(b"g 5 4 7\nusersname");
        assert_eq!(
            parse_tagged(&mut input),
            Ok((
                Command::Get {
                    key: namespaced(b"users", b"name"),
                },
                Some(7),
            ))
        );

        let mut input = buf(b"s 5 4 5 30 8\nusersnameAlice");
        assert_eq!(
            parse_tagged(&mut input),
            Ok((
                Command::Set {
                    key: namespaced(b"users", b"name"),
                    value: Bytes::from_static(b"Alice"),
                    ttl: Some(Duration::from_secs(30)),
                },
                Some(8),
            ))
        );

        let mut input = buf(b"s 5 4 5 9\nusersnameAlice");
        assert_eq!(
            parse_tagged(&mut input),
            Ok((
                Command::Set {
                    key: namespaced(b"users", b"name"),
                    value: Bytes::from_static(b"Alice"),
                    ttl: None,
                },
                Some(9),
            ))
        );

        let mut input = buf(b"d 5 4 10\nusersname");
        assert_eq!(
            parse_tagged(&mut input),
            Ok((
                Command::Delete {
                    key: namespaced(b"users", b"name"),
                },
                Some(10),
            ))
        );
    }

    #[test]
    fn namespaced_commands_still_reject_an_empty_key() {
        let mut input = buf(b"g 5 0\nusers");
        assert_eq!(parse(&mut input), Err(ParseError::EmptyKey));

        let mut input = buf(b"s 5 0 1\nusersx");
        assert_eq!(parse(&mut input), Err(ParseError::EmptyKey));
    }

    #[test]
    fn namespaced_commands_require_the_namespace_length_field() {
        // `g 4\nname` would be a legacy frame missing its leading field —
        // the key length alone can't stand in for both.
        let mut input = buf(b"g 4\nname");
        assert_eq!(parse(&mut input), Err(ParseError::InvalidLength));

        let mut input = buf(b"s 4 5\nnameAlice");
        assert_eq!(parse(&mut input), Err(ParseError::InvalidLength));
    }

    #[test]
    fn parse_leaves_a_namespaced_command_untouched_when_incomplete() {
        let original = b"s 5 4 5\nusersnameAli".to_vec();
        let mut input = buf(&original);

        assert_eq!(parse(&mut input), Err(ParseError::Incomplete));
        assert_eq!(&input[..], &original[..]);
    }

    #[test]
    fn namespaced_and_unnamespaced_entries_do_not_collide() {
        let mut cache = Cache::new(usize::MAX);

        Command::Set {
            key: key(b"usersname"),
            value: Bytes::from_static(b"flat"),
            ttl: None,
        }
        .execute(&mut cache);
        Command::Set {
            key: namespaced(b"users", b"name"),
            value: Bytes::from_static(b"scoped"),
            ttl: None,
        }
        .execute(&mut cache);

        assert_eq!(
            Command::Get {
                key: key(b"usersname"),
            }
            .execute(&mut cache),
            Response::Value(Bytes::from_static(b"flat"))
        );
        assert_eq!(
            Command::Get {
                key: namespaced(b"users", b"name"),
            }
            .execute(&mut cache),
            Response::Value(Bytes::from_static(b"scoped"))
        );
        assert_eq!(
            Command::Get { key: key(b"name") }.execute(&mut cache),
            Response::NotFound
        );
    }

    #[test]
    fn parses_clear_and_clear_all() {
        // Issue #106.
        let mut input = buf(b"c 5\nusers");
        assert_eq!(
            parse(&mut input),
            Ok(Command::Clear {
                namespace: Bytes::from_static(b"users"),
            })
        );
        assert!(input.is_empty());

        let mut input = buf(b"c 0\n");
        assert_eq!(
            parse(&mut input),
            Ok(Command::Clear {
                namespace: Bytes::new(),
            })
        );
        assert!(input.is_empty());

        let mut input = buf(b"F\nG 1\nk");
        assert_eq!(parse(&mut input), Ok(Command::ClearAll));
        assert_eq!(&input[..], b"G 1\nk");
    }

    #[test]
    fn clear_and_clear_all_carry_the_tag_last_in_tagged_mode() {
        let mut input = buf(b"c 5 7\nusers");
        assert_eq!(
            parse_tagged(&mut input),
            Ok((
                Command::Clear {
                    namespace: Bytes::from_static(b"users"),
                },
                Some(7),
            ))
        );

        let mut input = buf(b"F 8\n");
        assert_eq!(parse_tagged(&mut input), Ok((Command::ClearAll, Some(8))));

        let mut input = buf(b"F\n");
        assert_eq!(parse_tagged(&mut input), Err(ParseError::InvalidLength));
    }

    #[test]
    fn clear_rejects_extra_fields_and_stays_untouched_when_incomplete() {
        let mut input = buf(b"F 1 2\n");
        assert_eq!(parse(&mut input), Err(ParseError::InvalidLength));

        let original = b"c 5\nuse".to_vec();
        let mut input = buf(&original);
        assert_eq!(parse(&mut input), Err(ParseError::Incomplete));
        assert_eq!(&input[..], &original[..]);
    }

    #[test]
    fn clear_executes_against_one_namespace_and_clear_all_against_every_one() {
        let mut cache = Cache::new(usize::MAX);
        cache.set(key(b"a"), Bytes::from_static(b"1"));
        cache.set(namespaced(b"users", b"a"), Bytes::from_static(b"2"));
        cache.set(namespaced(b"users", b"b"), Bytes::from_static(b"3"));

        assert_eq!(
            Command::Clear {
                namespace: Bytes::from_static(b"users"),
            }
            .execute(&mut cache),
            Response::Cleared(2)
        );
        assert_eq!(cache.get(&namespaced(b"users", b"a")), None);
        assert_eq!(cache.get(&key(b"a")), Some(Bytes::from_static(b"1")));

        assert_eq!(Command::ClearAll.execute(&mut cache), Response::Cleared(1));
        assert_eq!(cache.get(&key(b"a")), None);
    }

    #[test]
    fn parses_a_cancel_migration_command() {
        let mut input = buf(b"X 6 5\ntok-bnode-b");

        assert_eq!(
            parse(&mut input),
            Ok(Command::CancelMigration {
                token: "tok-b".to_string(),
                joining_name: "node-b".to_string(),
            })
        );
        assert!(input.is_empty());
    }

    #[test]
    fn parse_leaves_a_cancel_migration_command_untouched_when_the_name_is_incomplete() {
        let original = b"X 6 5\ntok-bnod".to_vec();
        let mut input = buf(&original);

        assert_eq!(parse(&mut input), Err(ParseError::Incomplete));
        assert_eq!(&input[..], &original[..]);
    }

    #[test]
    fn rejects_an_empty_joining_name_in_cancel_migration() {
        let mut input = buf(b"X 0 5\ntok-b");

        assert_eq!(parse(&mut input), Err(ParseError::EmptyField));
    }

    #[test]
    fn rejects_an_empty_token_in_cancel_migration() {
        let mut input = buf(b"X 6 0\nnode-b");

        assert_eq!(parse(&mut input), Err(ParseError::EmptyField));
    }

    // Issue #128 measurement prototype: `m` (multi-get).

    #[test]
    fn parses_an_untagged_multi_get_command() {
        let mut input = buf(b"m 0 3 1 2 2\nabcde");

        assert_eq!(
            parse(&mut input),
            Ok(Command::MultiGet {
                namespace: Bytes::new(),
                keys: vec![
                    Bytes::from_static(b"a"),
                    Bytes::from_static(b"bc"),
                    Bytes::from_static(b"de"),
                ],
            })
        );
        assert!(input.is_empty());
    }

    #[test]
    fn parses_a_namespaced_tagged_multi_get_command() {
        let mut input = buf(b"m 2 2 1 2 9\nnsxyz");

        assert_eq!(
            parse_tagged(&mut input),
            Ok((
                Command::MultiGet {
                    namespace: Bytes::from_static(b"ns"),
                    keys: vec![Bytes::from_static(b"x"), Bytes::from_static(b"yz")],
                },
                Some(9),
            ))
        );
        assert!(input.is_empty());
    }

    #[test]
    fn parse_tagged_rejects_a_multi_get_without_a_tag() {
        let mut input = buf(b"m 0 1 1\na");
        assert_eq!(parse_tagged(&mut input), Err(ParseError::InvalidLength));
    }

    #[test]
    fn multi_get_leaves_input_untouched_when_the_body_is_incomplete() {
        let original = b"m 0 2 3 3\nabc".to_vec();
        let mut input = buf(&original);

        assert_eq!(parse(&mut input), Err(ParseError::Incomplete));
        assert_eq!(&input[..], &original[..]);
    }

    #[test]
    fn rejects_a_multi_get_with_zero_keys() {
        let mut input = buf(b"m 0 0\n");
        assert_eq!(parse(&mut input), Err(ParseError::InvalidLength));
    }

    #[test]
    fn rejects_a_multi_get_with_an_empty_key_length() {
        let mut input = buf(b"m 0 2 1 0\na");
        assert_eq!(parse(&mut input), Err(ParseError::EmptyKey));
    }

    #[test]
    fn rejects_a_multi_get_whose_claimed_count_outruns_the_header() {
        // A header lying about `n` fails on the first missing field,
        // never on an oversized allocation for the claimed count.
        let mut input = buf(b"m 0 999999999 1\na");
        assert_eq!(parse(&mut input), Err(ParseError::InvalidLength));
    }

    #[test]
    fn multi_get_executes_as_a_value_or_miss_per_key_in_request_order() {
        let mut cache = Cache::new(1024);
        cache.set(key(b"a"), Bytes::from_static(b"1"));
        cache.set(key(b"c"), Bytes::from_static(b"3"));

        let command = Command::MultiGet {
            namespace: Bytes::new(),
            keys: vec![
                Bytes::from_static(b"a"),
                Bytes::from_static(b"b"),
                Bytes::from_static(b"c"),
            ],
        };

        assert_eq!(
            command.execute(&mut cache),
            Response::Multi(vec![
                MultiEntry::Value(Bytes::from_static(b"1")),
                MultiEntry::Miss,
                MultiEntry::Value(Bytes::from_static(b"3")),
            ])
        );
    }

    #[test]
    fn multi_value_encodes_hits_misses_and_wrong_node_entries() {
        let response = Response::Multi(vec![
            MultiEntry::Value(Bytes::from_static(b"ab")),
            MultiEntry::Miss,
            MultiEntry::WrongNode,
        ]);

        assert_eq!(response.encode(), b"M 3 2 - W\nab");
        assert_eq!(response.encode_with_tag(9), b"M 3 2 - W 9\nab");
    }

    // Issue #150: `o` (multi-set).

    #[test]
    fn parses_an_untagged_multi_set_command() {
        let mut input = buf(b"o 0 2 1 1 1 2\nabcde");

        assert_eq!(
            parse(&mut input),
            Ok(Command::MultiSet {
                namespace: Bytes::new(),
                keys: vec![Bytes::from_static(b"a"), Bytes::from_static(b"c")],
                values: vec![Bytes::from_static(b"b"), Bytes::from_static(b"de")],
                ttl: None,
            })
        );
        assert!(input.is_empty());
    }

    #[test]
    fn parses_a_namespaced_tagged_multi_set_command_with_a_ttl() {
        let mut input = buf(b"o 2 1 1 1 30 9\nnsab");

        assert_eq!(
            parse_tagged(&mut input),
            Ok((
                Command::MultiSet {
                    namespace: Bytes::from_static(b"ns"),
                    keys: vec![Bytes::from_static(b"a")],
                    values: vec![Bytes::from_static(b"b")],
                    ttl: Some(Duration::from_secs(30)),
                },
                Some(9),
            ))
        );
        assert!(input.is_empty());
    }

    #[test]
    fn parse_tagged_rejects_a_multi_set_without_a_tag() {
        let mut input = buf(b"o 0 1 1 1\na");
        assert_eq!(parse_tagged(&mut input), Err(ParseError::InvalidLength));
    }

    #[test]
    fn multi_set_leaves_input_untouched_when_the_body_is_incomplete() {
        let original = b"o 0 1 3 3\nab".to_vec();
        let mut input = buf(&original);

        assert_eq!(parse(&mut input), Err(ParseError::Incomplete));
        assert_eq!(&input[..], &original[..]);
    }

    #[test]
    fn rejects_a_multi_set_with_zero_keys() {
        let mut input = buf(b"o 0 0\n");
        assert_eq!(parse(&mut input), Err(ParseError::InvalidLength));
    }

    #[test]
    fn rejects_a_multi_set_with_an_empty_key_length() {
        let mut input = buf(b"o 0 1 0 1\na");
        assert_eq!(parse(&mut input), Err(ParseError::EmptyKey));
    }

    #[test]
    fn multi_set_stores_every_key_and_answers_multi_ack() {
        let mut cache = Cache::new(1024);

        let command = Command::MultiSet {
            namespace: Bytes::new(),
            keys: vec![Bytes::from_static(b"a"), Bytes::from_static(b"c")],
            values: vec![Bytes::from_static(b"1"), Bytes::from_static(b"3")],
            ttl: None,
        };

        assert_eq!(
            command.execute(&mut cache),
            Response::MultiAck(vec![MultiAckEntry::Stored, MultiAckEntry::Stored])
        );
        assert_eq!(cache.get(&key(b"a")), Some(Bytes::from_static(b"1")));
        assert_eq!(cache.get(&key(b"c")), Some(Bytes::from_static(b"3")));
    }

    #[test]
    fn multi_ack_encodes_stored_and_wrong_node_entries() {
        let response = Response::MultiAck(vec![MultiAckEntry::Stored, MultiAckEntry::WrongNode]);

        assert_eq!(response.encode(), b"O 2 S W\n");
        assert_eq!(response.encode_with_tag(9), b"O 2 S W 9\n");
    }
}
