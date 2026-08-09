#[derive(Debug, PartialEq, Eq)]
pub enum Response {
    Value(Vec<u8>),
    Stored,
    Deleted,
    NotFound,
    Busy,
}

impl Response {
    pub fn encode(&self) -> Vec<u8> {
        match self {
            Self::Stored => b"STORED\r\n".to_vec(),
            Self::Deleted => b"DELETED\r\n".to_vec(),
            Self::NotFound => b"NOT_FOUND\r\n".to_vec(),
            Self::Busy => b"BUSY\r\n".to_vec(),

            Self::Value(value) => {
                let length = value.len().to_string();

                let mut encoded = Vec::with_capacity(6 + length.len() + 2 + value.len());

                encoded.extend_from_slice(b"VALUE ");
                encoded.extend_from_slice(length.as_bytes());
                encoded.extend_from_slice(b"\r\n");
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
        assert_eq!(Response::Stored.encode(), b"STORED\r\n");
    }

    #[test]
    fn encodes_deleted_response() {
        assert_eq!(Response::Deleted.encode(), b"DELETED\r\n");
    }

    #[test]
    fn encodes_not_found_response() {
        assert_eq!(Response::NotFound.encode(), b"NOT_FOUND\r\n");
    }

    #[test]
    fn encodes_busy_response() {
        assert_eq!(Response::Busy.encode(), b"BUSY\r\n");
    }

    #[test]
    fn encodes_value_response() {
        let response = Response::Value(b"Alice".to_vec());

        assert_eq!(response.encode(), b"VALUE 5\r\nAlice");
    }

    #[test]
    fn encodes_binary_value_response() {
        let response = Response::Value(vec![0xff, 0x00, b'\r', b'\n']);

        assert_eq!(response.encode(), b"VALUE 4\r\n\xff\x00\r\n",);
    }
}
