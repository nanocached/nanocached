use std::time::Duration;

#[derive(Debug, PartialEq, Eq)]
pub enum Command {
    Get {
        key: Vec<u8>,
    },
    Set {
        key: Vec<u8>,
        value: Vec<u8>,
        ttl: Option<Duration>,
    },
    Delete {
        key: Vec<u8>,
    },
}

#[derive(Debug, PartialEq, Eq)]
pub enum ParseError {
    InvalidCommand,
    InvalidLength,
    Incomplete,
}

pub fn parse(input: &[u8]) -> Result<(Command, usize), ParseError> {
    let header_end = find_crlf(input).ok_or(ParseError::Incomplete)?;
    let header = &input[..header_end];

    let mut parts = header.split(|byte| *byte == b' ');
    let command = parts.next().ok_or(ParseError::InvalidCommand)?;

    match command {
        b"GET" | b"DEL" => {
            let key_length = parts.next().ok_or(ParseError::InvalidLength)?;

            if parts.next().is_some() {
                return Err(ParseError::InvalidLength);
            }

            let key_length = parse_length(key_length)?;

            let key_start = header_end + 2;
            let key_end = key_start
                .checked_add(key_length)
                .ok_or(ParseError::InvalidLength)?;

            if input.len() < key_end {
                return Err(ParseError::Incomplete);
            }

            let key = input[key_start..key_end].to_vec();

            let command = match command {
                b"GET" => Command::Get { key },
                b"DEL" => Command::Delete { key },
                _ => unreachable!(),
            };

            Ok((command, key_end))
        }

        b"SET" => {
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

            let key_start = header_end + 2;

            let key_end = key_start
                .checked_add(key_length)
                .ok_or(ParseError::InvalidLength)?;

            let value_end = key_end
                .checked_add(value_length)
                .ok_or(ParseError::InvalidLength)?;

            if input.len() < value_end {
                return Err(ParseError::Incomplete);
            }

            let key = input[key_start..key_end].to_vec();
            let value = input[key_end..value_end].to_vec();

            Ok((Command::Set { key, value, ttl }, value_end))
        }

        _ => Err(ParseError::InvalidCommand),
    }
}

fn find_crlf(input: &[u8]) -> Option<usize> {
    input.windows(2).position(|window| window == b"\r\n")
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

    #[test]
    fn parses_get_command() {
        assert_eq!(
            parse(b"GET 4\r\nname"),
            Ok((
                Command::Get {
                    key: b"name".to_vec(),
                },
                11,
            ))
        );
    }

    #[test]
    fn parses_set_command_without_ttl() {
        assert_eq!(
            parse(b"SET 4 5\r\nnameAlice"),
            Ok((
                Command::Set {
                    key: b"name".to_vec(),
                    value: b"Alice".to_vec(),
                    ttl: None,
                },
                18,
            ))
        );
    }

    #[test]
    fn parses_set_command_with_ttl() {
        assert_eq!(
            parse(b"SET 4 5 10\r\nnameAlice"),
            Ok((
                Command::Set {
                    key: b"name".to_vec(),
                    value: b"Alice".to_vec(),
                    ttl: Some(Duration::from_secs(10)),
                },
                21,
            ))
        );
    }

    #[test]
    fn parses_delete_command() {
        assert_eq!(
            parse(b"DEL 4\r\nname"),
            Ok((
                Command::Delete {
                    key: b"name".to_vec(),
                },
                11,
            ))
        );
    }

    #[test]
    fn returns_incomplete_when_header_is_incomplete() {
        assert_eq!(parse(b"GET 4\r"), Err(ParseError::Incomplete));
    }

    #[test]
    fn returns_incomplete_when_key_is_incomplete() {
        assert_eq!(parse(b"GET 4\r\nna"), Err(ParseError::Incomplete));
    }

    #[test]
    fn returns_incomplete_when_set_value_is_incomplete() {
        assert_eq!(parse(b"SET 4 5\r\nnameAli"), Err(ParseError::Incomplete));
    }

    #[test]
    fn rejects_non_numeric_key_length() {
        assert_eq!(parse(b"GET abc\r\nname"), Err(ParseError::InvalidLength));
    }

    #[test]
    fn rejects_unknown_command() {
        assert_eq!(parse(b"UNKNOWN 4\r\nname"), Err(ParseError::InvalidCommand));
    }

    #[test]
    fn rejects_unknown_command_without_waiting_for_body() {
        assert_eq!(parse(b"UNKNOWN 100\r\n"), Err(ParseError::InvalidCommand));
    }

    #[test]
    fn reports_consumed_bytes() {
        let input = b"GET 4\r\nnameGET 3\r\nage";

        let (_, consumed) = parse(input).unwrap();

        assert_eq!(&input[consumed..], b"GET 3\r\nage");
    }

    #[test]
    fn parses_binary_key() {
        assert_eq!(
            parse(b"GET 3\r\n\xff\x00a"),
            Ok((
                Command::Get {
                    key: vec![0xff, 0x00, b'a'],
                },
                10,
            ))
        );
    }
}
