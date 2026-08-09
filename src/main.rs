use std::io;
use tokio::net::TcpListener;

mod cache;
mod command;
mod protocol;
mod response;

#[tokio::main(flavor = "current_thread")]
async fn main() -> io::Result<()> {
    let listener = TcpListener::bind("0.0.0.0:8356").await?;

    loop {
        let (_stream, address) = listener.accept().await?;
        println!("accepted connection from {address}");
    }
}
