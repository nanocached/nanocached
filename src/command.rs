use crate::cache::Cache;
use crate::response::Response;
use bytes::{Bytes, BytesMut};
use std::time::Duration;

#[derive(Debug, PartialEq, Eq)]
pub enum Command {
    Auth {
        secret: Bytes,
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
    /// directly by the migration task to snapshot every entry this node
    /// currently holds, to compute which ones a newly joining node now
    /// owns. See `Response::Entries`.
    ListEntries,
    /// Internal-only (ADR-0008): marks a key as handed off to another
    /// node during a migration this node was the source for. `Sweep`
    /// reclaims marked entries later.
    MarkMigrated {
        key: Bytes,
    },
    /// Internal-only (ADR-0008): the active-deletion pass, run
    /// periodically by a background task. Reclaims every marked entry
    /// and, since TTL expiry is otherwise only checked lazily on access,
    /// also proactively removes anything already past its TTL.
    Sweep,
    /// ADR-0008: sent by discovery to a `Joined` node when a new node is
    /// joining, so this node can compute (via `HashRing`) which of its
    /// own keys the joining node now owns. `joining_name`/`joining_addr`
    /// identify the joining node; `joined` is every currently-`Joined`
    /// node (ADR-0009 names, including this one) — the "before" ring,
    /// to which `joining_name` is the "after" addition.
    Migrate {
        joining_name: String,
        joining_addr: String,
        joined: Vec<(String, String)>,
    },
}

impl Command {
    /// Executes a cache operation. `Command::Auth`/`Migrate` are
    /// intercepted by the connection handler before a command ever
    /// reaches this point (neither is a plain cache operation: `Auth`
    /// because the actor has no auth state, `Migrate` because it needs
    /// network access the cache actor doesn't have), so neither can
    /// appear here.
    pub fn execute(self, cache: &mut Cache) -> Response {
        match self {
            Self::Auth { .. } | Self::Migrate { .. } => {
                unreachable!(
                    "Auth and Migrate are handled by the connection handler, not the cache actor"
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

            Self::ListEntries => Response::Entries(cache.entries()),

            Self::MarkMigrated { key } => {
                cache.mark_migrated(&key);
                Response::Marked
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
    let header_end = find_lf(&input[..]).ok_or(ParseError::Incomplete)?;
    let header = &input[..header_end];

    let mut parts = header.split(|byte| *byte == b' ');
    let command = parts.next().ok_or(ParseError::InvalidCommand)?;

    match command {
        b"A" => {
            let secret_length = parts.next().ok_or(ParseError::InvalidLength)?;

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

            Ok(Command::Auth { secret })
        }

        b"G" | b"D" => {
            let key_length = parts.next().ok_or(ParseError::InvalidLength)?;

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

            Ok(if is_get {
                Command::Get { key }
            } else {
                Command::Delete { key }
            })
        }

        b"S" => {
            let key_length = parts.next().ok_or(ParseError::InvalidLength)?;
            let value_length = parts.next().ok_or(ParseError::InvalidLength)?;
            let ttl = parts.next();

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

            Ok(Command::Set { key, value, ttl })
        }

        b"M" => {
            let joining_name_length = parts.next().ok_or(ParseError::InvalidLength)?;
            let joining_addr_length = parts.next().ok_or(ParseError::InvalidLength)?;
            let joined_count = parts.next().ok_or(ParseError::InvalidLength)?;

            if parts.next().is_some() {
                return Err(ParseError::InvalidLength);
            }

            let joining_name_length = parse_length(joining_name_length)?;
            let joining_addr_length = parse_length(joining_addr_length)?;
            let joined_count = parse_length(joined_count)?;

            parse_migrate(
                input,
                header_end,
                joining_name_length,
                joining_addr_length,
                joined_count,
            )
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
    joining_name_length: usize,
    joining_addr_length: usize,
    joined_count: usize,
) -> Result<Command, ParseError> {
    if joining_name_length == 0 || joining_addr_length == 0 {
        return Err(ParseError::EmptyField);
    }

    let joining_name_start = header_end + 1;
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
    let mut entry_spans = Vec::with_capacity(joined_count);

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

    let joining_name = decode_field(&frame, joining_name_start, joining_name_length)?;
    let joining_addr = decode_field(&frame, joining_addr_start, joining_addr_length)?;

    let mut joined = Vec::with_capacity(joined_count);
    for (name_start, name_length, addr_start, addr_length) in entry_spans {
        let name = decode_field(&frame, name_start, name_length)?;
        let addr = decode_field(&frame, addr_start, addr_length)?;
        joined.push((name, addr));
    }

    Ok(Command::Migrate {
        joining_name,
        joining_addr,
        joined,
    })
}

fn decode_field(frame: &[u8], start: usize, length: usize) -> Result<String, ParseError> {
    String::from_utf8(frame[start..start + length].to_vec()).map_err(|_| ParseError::InvalidUtf8)
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
            })
        );
        assert!(input.is_empty());
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
    #[should_panic(expected = "Auth and Migrate are handled by the connection handler")]
    fn execute_panics_on_auth() {
        let mut cache = Cache::new(usize::MAX);

        let command = Command::Auth {
            secret: Bytes::from_static(b"secret"),
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
    fn list_entries_returns_every_stored_entry() {
        let mut cache = Cache::new(usize::MAX);
        cache.set(Bytes::from_static(b"name"), Bytes::from_static(b"Alice"));

        assert_eq!(
            Command::ListEntries.execute(&mut cache),
            Response::Entries(vec![(
                Bytes::from_static(b"name"),
                Bytes::from_static(b"Alice"),
                None
            )])
        );
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
        let mut input = buf(b"M 6 14 0\nnode-b127.0.0.1:8357");

        assert_eq!(
            parse(&mut input),
            Ok(Command::Migrate {
                joining_name: "node-b".to_string(),
                joining_addr: "127.0.0.1:8357".to_string(),
                joined: Vec::new(),
            })
        );
        assert!(input.is_empty());
    }

    #[test]
    fn parses_a_migrate_command_with_joined_nodes_and_consumes_only_that_frame() {
        let mut input = buf(
            b"M 6 14 2\nnode-b127.0.0.1:83576 14\nnode-a127.0.0.1:83566 14\nnode-c127.0.0.1:8358G 1\nx",
        );

        assert_eq!(
            parse(&mut input),
            Ok(Command::Migrate {
                joining_name: "node-b".to_string(),
                joining_addr: "127.0.0.1:8357".to_string(),
                joined: vec![
                    ("node-a".to_string(), "127.0.0.1:8356".to_string()),
                    ("node-c".to_string(), "127.0.0.1:8358".to_string()),
                ],
            })
        );
        assert_eq!(&input[..], b"G 1\nx");
    }

    #[test]
    fn parse_leaves_a_migrate_command_untouched_when_a_joined_entry_is_incomplete() {
        let original = b"M 6 14 1\nnode-b127.0.0.1:83576 14\nnode-a127.0.0".to_vec();
        let mut input = BytesMut::from(&original[..]);

        assert_eq!(parse(&mut input), Err(ParseError::Incomplete));
        assert_eq!(&input[..], &original[..]);
    }

    #[test]
    fn rejects_an_empty_joining_name_in_migrate() {
        let mut input = buf(b"M 0 14 0\n127.0.0.1:8357");

        assert_eq!(parse(&mut input), Err(ParseError::EmptyField));
    }
}
