use bytes::Bytes;

#[derive(Debug, PartialEq, Eq)]
pub enum Response {
    Value(Bytes),
    Stored,
    Deleted,
    NotFound,
    Busy,
}

impl Response {
    pub fn encode(&self) -> Vec<u8> {
        match self {
            Self::Stored => b"S\n".to_vec(),
            Self::Deleted => b"D\n".to_vec(),
            Self::NotFound => b"N\n".to_vec(),
            Self::Busy => b"B\n".to_vec(),

            Self::Value(value) => {
                let length = value.len().to_string();

                let mut encoded = Vec::with_capacity(2 + length.len() + 1 + value.len());

                encoded.extend_from_slice(b"V ");
                encoded.extend_from_slice(length.as_bytes());
                encoded.push(b'\n');
                encoded.extend_from_slice(value);

                encoded
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
