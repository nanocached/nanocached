#[derive(Debug, PartialEq, Eq)]
pub enum Response {
    Value(Vec<u8>),
    Stored,
    Deleted,
    NotFound,
}
