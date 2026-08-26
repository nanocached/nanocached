use crate::cache::{Cache, IncrResult, parse_decimal_i64};
use crate::key::Key;
use crate::response::Response;
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
    /// `U <ns-len> <key-len> <val-len> [ttl] [tag]\n<ns><key><value>`
    /// (issue #124, cluster-internal like `M`/`X`): a decommissioning
    /// node handing one of its entries to the key's post-leave owner.
    /// Executes exactly as `Set`; the difference is in the connection
    /// handler, which skips the wrong-node check — the receiver becomes
    /// this key's owner only once discovery publishes the post-leave
    /// roster, which by design happens *after* the transfer.
    HandoffSet {
        key: Key,
        value: Bytes,
        ttl: Option<Duration>,
    },
    /// `u <ns-len> <key-len> [tag]\n<ns><key>` (issue #124,
    /// cluster-internal like `U`): a decommissioning node forwarding a
    /// concurrent client delete to the key's post-leave owner. Executes
    /// exactly as `Delete`; the connection handler skips the wrong-node
    /// check for the same reason it does for `U` — the receiver owns the
    /// key only once the post-leave roster publishes.
    HandoffDelete {
        key: Key,
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
    Migrate {
        token: String,
        joining_name: String,
        joining_addr: String,
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
            Self::Set { key, value, ttl } | Self::HandoffSet { key, value, ttl } => {
                match ttl {
                    Some(ttl) => cache.set_with_ttl(key, value, ttl),
                    None => cache.set(key, value),
                }
                Response::Stored
            }

            Self::Delete { key } | Self::HandoffDelete { key } => {
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
        b"G" | b"D" | b"g" | b"d" | b"u" => {
            let namespaced = command == b"g" || command == b"d" || command == b"u";
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
            // Resolved before the body is consumed, same dance as `is_get`.
            let handoff = command == b"u";

            let frame = input.split_to(key_end).freeze();
            let key = Key::new(
                frame.slice(namespace_start..key_start),
                frame.slice(key_start..key_end),
            );

            Ok((
                if is_get {
                    Command::Get { key }
                } else if handoff {
                    Command::HandoffDelete { key }
                } else {
                    Command::Delete { key }
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

        b"S" | b"s" | b"U" => {
            // Resolved before the body is consumed (`command` borrows the
            // buffer) — same dance as `G`/`D`'s `is_get`.
            let handoff = command == b"U";
            let namespace_length = if command == b"s" || command == b"U" {
                parse_length(parts.next().ok_or(ParseError::InvalidLength)?)?
            } else {
                0
            };
            let key_length = parts.next().ok_or(ParseError::InvalidLength)?;
            let value_length = parts.next().ok_or(ParseError::InvalidLength)?;
            // In tagged mode the tag is the *last* field, so with both
            // optional fields the header is `S <k> <v> [ttl] <tag>`:
            // one trailing field is the tag alone, two are TTL then tag.
            // The connection's negotiated mode is what disambiguates a
            // three-field header — never a guess frame by frame.
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

            let request = if handoff {
                Command::HandoffSet { key, value, ttl }
            } else {
                Command::Set { key, value, ttl }
            };
            Ok((request, tag))
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
            let joined_count = parts.next().ok_or(ParseError::InvalidLength)?;
            let replication = parts.next().ok_or(ParseError::InvalidLength)?;
            let token_length = parts.next().ok_or(ParseError::InvalidLength)?;

            if parts.next().is_some() {
                return Err(ParseError::InvalidLength);
            }

            let joining_name_length = parse_length(joining_name_length)?;
            let joining_addr_length = parse_length(joining_addr_length)?;
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
        joined_count,
        replication,
    } = header;

    if token_length == 0 || joining_name_length == 0 || joining_addr_length == 0 {
        return Err(ParseError::EmptyField);
    }

    // Body layout: `<token><joining_name><joining_addr><entries>` — the
    // token leads so the connection handler can verify it before acting.
    let token_start = header_end + 1;
    let joining_name_start = token_start
        .checked_add(token_length)
        .ok_or(ParseError::InvalidLength)?;
    let joining_addr_start = joining_name_start
        .checked_add(joining_name_length)
        .ok_or(ParseError::InvalidLength)?;
    let mut cursor = joining_addr_start
        .checked_add(joining_addr_length)
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
        let mut input = buf(b"M 6 14 0 2 5\ntok-bnode-b127.0.0.1:8357");

        assert_eq!(
            parse(&mut input),
            Ok(Command::Migrate {
                token: "tok-b".to_string(),
                joining_name: "node-b".to_string(),
                joining_addr: "127.0.0.1:8357".to_string(),
                joined: Vec::new(),
                replication: 2,
            })
        );
        assert!(input.is_empty());
    }

    #[test]
    fn parses_a_migrate_command_with_joined_nodes_and_consumes_only_that_frame() {
        let mut input = buf(
            b"M 6 14 2 2 5\ntok-bnode-b127.0.0.1:83576 14\nnode-a127.0.0.1:83566 14\nnode-c127.0.0.1:8358G 1\nx",
        );

        assert_eq!(
            parse(&mut input),
            Ok(Command::Migrate {
                token: "tok-b".to_string(),
                joining_name: "node-b".to_string(),
                joining_addr: "127.0.0.1:8357".to_string(),
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
        let original = b"M 6 14 1 2 5\ntok-bnode-b127.0.0.1:83576 14\nnode-a127.0.0".to_vec();
        let mut input = BytesMut::from(&original[..]);

        assert_eq!(parse(&mut input), Err(ParseError::Incomplete));
        assert_eq!(&input[..], &original[..]);
    }

    #[test]
    fn parse_resumable_picks_a_migrate_roster_scan_up_where_it_left_off() {
        // Feed a frame one byte at a time and check (a) the result equals
        // the one-shot parse, (b) each retry only re-scans from the last
        // fully-buffered entry, never from entry #1.
        let frame = b"M 6 14 3 2 5\ntok-bnode-b127.0.0.1:83576 14\nnode-a127.0.0.1:83566 14\n\
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
            b"M 6 14 2 2 5\ntok-bnode-b127.0.0.1:83576 14\nnode-a127.0.0.1:83566 14\nnode-c127.0.0.1:8358",
        );

        let (command, _) = parse_resumable(&mut input, false, &mut progress).unwrap();

        assert!(matches!(command, Command::Migrate { joined, .. } if joined.len() == 2));
    }

    #[test]
    fn rejects_an_empty_joining_name_in_migrate() {
        let mut input = buf(b"M 0 14 0 2 5\ntok-b127.0.0.1:8357");

        assert_eq!(parse(&mut input), Err(ParseError::EmptyField));
    }

    #[test]
    fn rejects_an_empty_token_in_migrate() {
        let mut input = buf(b"M 6 14 0 2 0\nnode-b127.0.0.1:8357");

        assert_eq!(parse(&mut input), Err(ParseError::EmptyField));
    }

    #[test]
    fn migrate_with_a_huge_joined_count_reports_incomplete_without_pre_allocating() {
        // `joined_count` is attacker-controlled and unbounded; parsing must
        // not size an allocation from it (a `Vec::with_capacity` on this value
        // would request terabytes and abort the process). With the joining
        // node's own fields present but no entries buffered, this must simply
        // report `Incomplete` — cheaply, without touching that huge number.
        let mut input = buf(b"M 6 14 999999999999 2 5\ntok-bnode-b127.0.0.1:8357");

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
    fn parses_handoff_set_and_delete() {
        // Issue #124: `U`/`u` share `s`/`d`'s namespaced shape and map to
        // the handoff variants (the connection handler skips the
        // wrong-node check for them).
        let mut input = buf(b"U 5 4 5 30\nusersnameAlice");
        assert_eq!(
            parse(&mut input),
            Ok(Command::HandoffSet {
                key: namespaced(b"users", b"name"),
                value: Bytes::from_static(b"Alice"),
                ttl: Some(Duration::from_secs(30)),
            })
        );
        assert!(input.is_empty());

        let mut input = buf(b"u 5 4\nusersname");
        assert_eq!(
            parse(&mut input),
            Ok(Command::HandoffDelete {
                key: namespaced(b"users", b"name"),
            })
        );
        assert!(input.is_empty());
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
}
