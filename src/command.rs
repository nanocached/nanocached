use crate::cache::Cache;
use crate::response::Response;
use bytes::{Bytes, BytesMut};
use std::time::Duration;

#[derive(Debug, PartialEq, Eq)]
pub enum Command {
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
}

impl Command {
    pub fn execute(self, cache: &mut Cache) -> Response {
        match self {
            Self::Get { key } => match cache.get(&key) {
                Some(value) => Response::Value(value),
                None => Response::NotFound,
            },
            Self::Set { key, value, ttl } => {
                // Stored entries must not keep a shared receive-buffer chunk
                // (which may span an entire pipelined batch) alive just to
                // retain a few bytes of it, so re-copy into right-sized
                // allocations before inserting into the cache.
                let key = Bytes::copy_from_slice(&key);
                let value = Bytes::copy_from_slice(&value);

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
        }
    }
}

#[derive(Debug, PartialEq, Eq)]
pub enum ParseError {
    InvalidCommand,
    InvalidLength,
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
        b"G" | b"D" => {
            let key_length = parts.next().ok_or(ParseError::InvalidLength)?;

            if parts.next().is_some() {
                return Err(ParseError::InvalidLength);
            }

            let key_length = parse_length(key_length)?;

            let key_start = header_end + 1;
            let key_end = key_start
                .checked_add(key_length)
                .ok_or(ParseError::InvalidLength)?;

            if input.len() < key_end {
                return Err(ParseError::Incomplete);
            }

            let is_get = match command {
                b"G" => true,
                b"D" => false,
                _ => unreachable!(),
            };

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

    fn buf(bytes: &[u8]) -> BytesMut {
        BytesMut::from(bytes)
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
}
