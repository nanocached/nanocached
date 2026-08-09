mod cache;
mod command;
mod response;
mod server;

use std::io;

#[mutants::skip]
#[tokio::main(flavor = "current_thread")]
async fn main() -> io::Result<()> {
    server::run("0.0.0.0:8356").await
}
