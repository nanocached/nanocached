use crate::cache::Cache;
use crate::response::Response;
use bytes::{Bytes, BytesMut};
use std::time::Duration;

#[derive(Debug, PartialEq, Eq)]
pub enum Command {
    Auth {
        secret: Bytes,
        /// ADR-0019: the client sent `A <len> T\n` — it wants response
        /// tags echoed on this connection's `G`/`S`/`D` replies.
        tagging: bool,
    },
    Get {
        key: Bytes,
    },
    Set {
        key: Bytes,
        value: Bytes,
        ttl: Option<Duration>,
    },
    Delete {
        key: Bytes,
    },
    /// Internal-only (ADR-0008): never produced by `parse()`, constructed
    /// directly by the migration task to snapshot every key this node
    /// currently holds, to compute which ones a newly joining node now
    /// owns. See `Response::Keys` and `Cache::keys`'s doc comment for why
    /// this is keys-only rather than full entries.
    ListEntries,
    /// Internal-only (ADR-0008): the migration task's live re-check of a
    /// single key's current value right before sending it, instead of
    /// trusting `ListEntries`'s snapshot (which may be stale by the time
    /// this key's turn comes up). Answered with `Response::Entries`
    /// holding zero or one entry — reusing its shape rather than adding a
    /// new one for what's otherwise the exact same data.
    PeekEntry {
        key: Bytes,
    },
    /// Internal-only (ADR-0008): marks a key as handed off to another
    /// node during a migration this node was the source for. `Sweep`
    /// reclaims marked entries later.
    MarkMigrated {
        key: Bytes,
    },
    /// Internal-only (ADR-0008): reverses `MarkMigrated` for a key whose
    /// migration was cancelled (see `Command::CancelMigration`), so
    /// `Sweep` doesn't reclaim it after all.
    UnmarkMigrated {
        key: Bytes,
    },
    /// Internal-only (ADR-0008): the active-deletion pass, run
    /// periodically by a background task. Reclaims every marked entry
    /// and, since TTL expiry is otherwise only checked lazily on access,
    /// also proactively removes anything already past its TTL.
    Sweep,
    /// ADR-0008: sent by discovery to a `Joined` node when a new node is
    /// joining, so this node can compute (via `HashRing`) how each of its
    /// own keys' top-R owner set changes (ADR-0011). `joining_name`/
    /// `joining_addr` identify the joining node; `joined` is every
    /// currently-`Joined` node (ADR-0009 names, including this one) — the
    /// "before" roster, to which `joining_name` is the "after" addition.
    /// `replication` is discovery's replication factor R (ADR-0011) — the
    /// single source nodes learn R from.
    ///
    /// `token` is *this receiving node's own* membership token (issue #34),
    /// echoed back by discovery to prove the `M` really came from a
    /// discovery server this node registered with — the only party that
    /// knows it. This closes the gap where any holder of the shared secret
    /// (every client) could otherwise send `M` directly and make the node
    /// stream its cache to an attacker-chosen address. It does not violate
    /// ADR-0018's "tokens are never sent back out" invariant: the token in
    /// an `M` is the *recipient's* own token (which it already holds and no
    /// client knows), never some other node's.
    Migrate {
        token: String,
        joining_name: String,
        joining_addr: String,
        joined: Vec<(String, String)>,
        replication: usize,
    },
    /// ADR-0008: sent by discovery to a ready node to abandon a handoff
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
            Self::Set { key, value, ttl } => {
                match ttl {
                    Some(ttl) => cache.set_with_ttl(key, value, ttl),
                    None => cache.set(key, value),
                }
                Response::Stored
            }

            Self::Delete { key } => {
                if cache.delete(&key) {
                    Response::Deleted
                } else {
                    Response::NotFound
                }
            }

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

            Self::Sweep => Response::Swept(cache.sweep()),
        }
    }
}

#[derive(Debug, PartialEq, Eq)]
pub enum ParseError {
    InvalidCommand,
    InvalidLength,
    EmptyKey,
    EmptySecret,
    /// A name/address field in `M` (ADR-0008/0009) was declared with
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
pub fn parse(input: &mut BytesMut) -> Result<Command, ParseError> {
    parse_with_mode(input, false).map(|(command, _)| command)
}

/// ADR-0019: `parse` for a connection whose `A ... T` negotiation
/// succeeded. `G`/`S`/`D` headers must carry the client's tag as their
/// last field, returned alongside for the response to echo; commands
/// that never carry one (`A`, `M`, `X`) return `None`.
pub fn parse_tagged(input: &mut BytesMut) -> Result<(Command, Option<u32>), ParseError> {
    parse_with_mode(input, true)
}

fn parse_with_mode(
    input: &mut BytesMut,
    tagged: bool,
) -> Result<(Command, Option<u32>), ParseError> {
    let header_end = find_lf(&input[..]).ok_or(ParseError::Incomplete)?;
    let header = &input[..header_end];

    let mut parts = header.split(|byte| *byte == b' ');
    let command = parts.next().ok_or(ParseError::InvalidCommand)?;

    match command {
        b"A" => {
            let secret_length = parts.next().ok_or(ParseError::InvalidLength)?;

            // ADR-0019: an optional literal `T` requests tagged mode.
            // Accepted regardless of the connection's current mode, since
            // this is the field that *establishes* the mode.
            let tagging = match parts.next() {
                None => false,
                Some(b"T") => true,
                Some(_) => return Err(ParseError::InvalidCommand),
            };

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

            Ok((Command::Auth { secret, tagging }, None))
        }

        b"G" | b"D" => {
            let key_length = parts.next().ok_or(ParseError::InvalidLength)?;
            let tag = parse_trailing_tag(&mut parts, tagged)?;

            if parts.next().is_some() {
                return Err(ParseError::InvalidLength);
            }

            let key_length = parse_length(key_length)?;

            if key_length == 0 {
                return Err(ParseError::EmptyKey);
            }

            let key_start = header_end + 1;
            let key_end = key_start
                .checked_add(key_length)
                .ok_or(ParseError::InvalidLength)?;

            if input.len() < key_end {
                return Err(ParseError::Incomplete);
            }

            let is_get = command == b"G";

            let frame = input.split_to(key_end).freeze();
            let key = frame.slice(key_start..key_end);

            Ok((
                if is_get {
                    Command::Get { key }
                } else {
                    Command::Delete { key }
                },
                tag,
            ))
        }

        b"S" => {
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

            let key_start = header_end + 1;

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
            let key = frame.slice(key_start..key_end);
            let value = frame.slice(key_end..value_end);

            Ok((Command::Set { key, value, ttl }, tag))
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
                token_length,
                joining_name_length,
                joining_addr_length,
                joined_count,
                replication,
            )
            .map(|command| (command, None))
        }

        _ => Err(ParseError::InvalidCommand),
    }
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
    token_length: usize,
    joining_name_length: usize,
    joining_addr_length: usize,
    joined_count: usize,
    replication: usize,
) -> Result<Command, ParseError> {
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
    let mut entry_spans = Vec::new();

    for _ in 0..joined_count {
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

fn decode_field(frame: &[u8], start: usize, length: usize) -> Result<String, ParseError> {
    String::from_utf8(frame[start..start + length].to_vec()).map_err(|_| ParseError::InvalidUtf8)
}

/// ADR-0019: in tagged mode the next header field is the required
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

/// ADR-0019: a tag is a u32 in the same decimal encoding as every
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
            })
        );
        assert!(input.is_empty());
    }

    #[test]
    fn parses_auth_command_with_tagging_flag() {
        let mut input = buf(b"A 6 T\nsecret");

        assert_eq!(
            parse(&mut input),
            Ok(Command::Auth {
                secret: Bytes::from_static(b"secret"),
                tagging: true,
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

        assert_eq!(
            parse(&mut input),
            Ok(Command::Get {
                key: Bytes::from_static(b"name"),
            })
        );
        assert!(input.is_empty());
    }

    #[test]
    fn parses_set_command_without_ttl() {
        let mut input = buf(b"S 4 5\nnameAlice");

        assert_eq!(
            parse(&mut input),
            Ok(Command::Set {
                key: Bytes::from_static(b"name"),
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
                key: Bytes::from_static(b"name"),
                value: Bytes::from_static(b"Alice"),
                ttl: Some(Duration::from_secs(10)),
            })
        );
        assert!(input.is_empty());
    }

    #[test]
    fn parses_delete_command() {
        let mut input = buf(b"D 4\nname");

        assert_eq!(
            parse(&mut input),
            Ok(Command::Delete {
                key: Bytes::from_static(b"name"),
            })
        );
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
                key: Bytes::from_static(b"name"),
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
                key: Bytes::from(vec![0xff, 0x00, b'a']),
            })
        );
    }

    #[test]
    fn get_returns_value_for_existing_key() {
        let mut cache = Cache::new(usize::MAX);
        cache.set(Bytes::from_static(b"name"), Bytes::from_static(b"Alice"));

        let command = Command::Get {
            key: Bytes::from_static(b"name"),
        };

        assert_eq!(
            command.execute(&mut cache),
            Response::Value(Bytes::from_static(b"Alice")),
        );
    }

    #[test]
    fn get_returns_not_found_for_missing_key() {
        let mut cache = Cache::new(usize::MAX);

        let command = Command::Get {
            key: Bytes::from_static(b"name"),
        };

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
        };

        let _ = command.execute(&mut cache);
    }

    #[test]
    fn set_stores_value() {
        let mut cache = Cache::new(usize::MAX);

        let command = Command::Set {
            key: Bytes::from_static(b"name"),
            value: Bytes::from_static(b"Alice"),
            ttl: None,
        };

        assert_eq!(command.execute(&mut cache), Response::Stored);
        assert_eq!(cache.get(b"name"), Some(Bytes::from_static(b"Alice")));
    }

    #[test]
    fn set_with_zero_ttl_expires_immediately() {
        let mut cache = Cache::new(usize::MAX);

        let command = Command::Set {
            key: Bytes::from_static(b"name"),
            value: Bytes::from_static(b"Alice"),
            ttl: Some(Duration::ZERO),
        };

        assert_eq!(command.execute(&mut cache), Response::Stored);

        assert_eq!(cache.get(b"name"), None);
    }

    #[test]
    fn delete_returns_deleted_for_existing_key() {
        let mut cache = Cache::new(usize::MAX);
        cache.set(Bytes::from_static(b"name"), Bytes::from_static(b"Alice"));

        let command = Command::Delete {
            key: Bytes::from_static(b"name"),
        };

        assert_eq!(command.execute(&mut cache), Response::Deleted);
    }

    #[test]
    fn delete_returns_not_found_for_missing_key() {
        let mut cache = Cache::new(usize::MAX);

        let command = Command::Delete {
            key: Bytes::from_static(b"name"),
        };

        assert_eq!(command.execute(&mut cache), Response::NotFound);
    }

    #[test]
    fn list_entries_returns_every_stored_key() {
        let mut cache = Cache::new(usize::MAX);
        cache.set(Bytes::from_static(b"name"), Bytes::from_static(b"Alice"));

        assert_eq!(
            Command::ListEntries.execute(&mut cache),
            Response::Keys(vec![Bytes::from_static(b"name")])
        );
    }

    #[test]
    fn peek_entry_returns_the_matching_entry() {
        let mut cache = Cache::new(usize::MAX);
        cache.set(Bytes::from_static(b"name"), Bytes::from_static(b"Alice"));

        let command = Command::PeekEntry {
            key: Bytes::from_static(b"name"),
        };

        assert_eq!(
            command.execute(&mut cache),
            Response::Entries(vec![(
                Bytes::from_static(b"name"),
                Bytes::from_static(b"Alice"),
                None
            )])
        );
    }

    #[test]
    fn peek_entry_returns_no_entries_for_a_missing_key() {
        let mut cache = Cache::new(usize::MAX);

        let command = Command::PeekEntry {
            key: Bytes::from_static(b"missing"),
        };

        assert_eq!(command.execute(&mut cache), Response::Entries(Vec::new()));
    }

    #[test]
    fn mark_migrated_returns_marked() {
        let mut cache = Cache::new(usize::MAX);
        cache.set(Bytes::from_static(b"name"), Bytes::from_static(b"Alice"));

        let command = Command::MarkMigrated {
            key: Bytes::from_static(b"name"),
        };

        assert_eq!(command.execute(&mut cache), Response::Marked);
    }

    #[test]
    fn sweep_returns_how_many_entries_it_removed() {
        let mut cache = Cache::new(usize::MAX);
        cache.set(Bytes::from_static(b"name"), Bytes::from_static(b"Alice"));
        cache.mark_migrated(b"name");

        assert_eq!(Command::Sweep.execute(&mut cache), Response::Swept(1));
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
            Ok((
                Command::Get {
                    key: Bytes::from_static(b"name"),
                },
                Some(7),
            ))
        );
        assert_eq!(
            parse_tagged(&mut input),
            Ok((
                Command::Delete {
                    key: Bytes::from_static(b"name"),
                },
                Some(u32::MAX),
            ))
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
                    key: Bytes::from_static(b"name"),
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
                    key: Bytes::from_static(b"name"),
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
                },
                None,
            ))
        );
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
