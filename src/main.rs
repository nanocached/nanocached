use std::io;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

mod cache;
mod command;
mod protocol;
mod response;

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
    let mut buffer = [0_u8; 1024];

    let bytes_read = stream.read(&mut buffer).await?;

    if bytes_read == 0 {
        return Ok(());
    }

    stream.write_all(&buffer[..bytes_read]).await?;

    Ok(())
}
