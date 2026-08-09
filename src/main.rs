mod cache;
mod command;
mod protocol;
mod response;

use command::{ParseError, parse};
use std::io;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

const MAX_REQUEST_SIZE: usize = 1024 * 1024;

#[tokio::main(flavor = "current_thread")]
async fn main() -> io::Result<()> {
    let listener = TcpListener::bind("0.0.0.0:8356").await?;

    loop {
        let (stream, address) = listener.accept().await?;
        println!("accepted connection from {address}");

        tokio::spawn(async move {
            if let Err(error) = handle_connection(stream).await {
                eprintln!("connection error: {error}");
            }
        });
    }
}

async fn handle_connection(mut stream: TcpStream) -> io::Result<()> {
    let mut received = Vec::new();
    let mut chunk = [0_u8; 1024];

    loop {
        match parse(&received) {
            Ok((_command, consumed)) => {
                stream.write_all(&received[..consumed]).await?;
                return Ok(());
            }
            Err(ParseError::Incomplete) => {}
            Err(error) => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("{error:?}"),
                ));
            }
        }

        let bytes_read = stream.read(&mut chunk).await?;

        if bytes_read == 0 {
            if received.is_empty() {
                return Ok(());
            }

            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "connection closed before request was complete",
            ));
        }

        received.extend_from_slice(&chunk[..bytes_read]);

        if received.len() > MAX_REQUEST_SIZE {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "request is too large",
            ));
        }
    }
}
