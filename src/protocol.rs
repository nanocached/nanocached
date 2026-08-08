use crate::cache::Cache;
use crate::command::{ParseError, parse};

pub fn process(input: &[u8], cache: &mut Cache) -> Result<(Vec<u8>, usize), ParseError> {
    let (command, consumed) = parse(input)?;
    let response = command.execute(cache);
    let encoded = response.encode();

    Ok((encoded, consumed))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn processes_set_request() {
        let mut cache = Cache::new();

        let result = process(b"SET 4 5\r\nnameAlice", &mut cache);

        assert_eq!(result, Ok((b"STORED\r\n".to_vec(), 18)));

        assert_eq!(cache.get(b"name"), Some(b"Alice".as_slice()));
    }

    #[test]
    fn processes_get_request() {
        let mut cache = Cache::new();
        cache.set(b"name".to_vec(), b"Alice".to_vec());

        let result = process(b"GET 4\r\nname", &mut cache);

        assert_eq!(result, Ok((b"VALUE 5\r\nAlice".to_vec(), 11)));
    }

    #[test]
    fn processes_get_request_for_missing_key() {
        let mut cache = Cache::new();

        let result = process(b"GET 4\r\nname", &mut cache);

        assert_eq!(result, Ok((b"NOT_FOUND\r\n".to_vec(), 11)));
    }

    #[test]
    fn processes_delete_request() {
        let mut cache = Cache::new();
        cache.set(b"name".to_vec(), b"Alice".to_vec());

        let result = process(b"DEL 4\r\nname", &mut cache);

        assert_eq!(result, Ok((b"DELETED\r\n".to_vec(), 11)));

        assert_eq!(cache.get(b"name"), None);
    }

    #[test]
    fn processes_concatenated_one_at_a_time() {
        let mut cache = Cache::new();
        let input = b"SET 4 5\r\nnameAliceGET 4\r\nname";

        let (set_response, consumed) = process(input, &mut cache).unwrap();

        assert_eq!(set_response, b"STORED\r\n");

        let (get_response, second_consumed) = process(&input[consumed..], &mut cache).unwrap();

        assert_eq!(get_response, b"VALUE 5\r\nAlice");
        assert_eq!(consumed + second_consumed, input.len());
    }

    #[test]
    fn incomplete_request_does_not_modify_cache() {
        let mut cache = Cache::new();

        let result = process(b"SET 4 5\r\nnameAli", &mut cache);

        assert_eq!(result, Err(ParseError::Incomplete));
        assert_eq!(cache.get(b"name"), None);
    }
}
