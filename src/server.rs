use crate::cache::Cache;
use crate::command::{Command, ParseError, parse};
use crate::response::Response;
use std::io;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{mpsc, oneshot};

const MAX_REQUEST_SIZE: usize = 1024 * 1024;

struct CacheRequest {
    command: Command,
    response_tx: oneshot::Sender<Response>,
}

pub(crate) async fn run(address: &str) -> io::Result<()> {
    let listener = TcpListener::bind(address).await?;

    let (request_tx, request_rx) = mpsc::channel(1024);

    tokio::spawn(run_cache(request_rx));

    loop {
        let (stream, address) = listener.accept().await?;
        let request_tx = request_tx.clone();

        println!("accepted connection from {address}");

        tokio::spawn(async move {
            if let Err(error) = handle_connection(stream, request_tx).await {
                eprintln!("connection error from {address}: {error}");
            }
        });
    }
}

async fn handle_connection(
    mut stream: TcpStream,
    request_tx: mpsc::Sender<CacheRequest>,
) -> io::Result<()> {
    let mut received = Vec::new();
    let mut chunk = [0_u8; 1024];

    loop {
        match parse(&received) {
            Ok((command, consumed)) => {
                let (response_tx, response_rx) = oneshot::channel();

                request_tx
                    .send(CacheRequest {
                        command,
                        response_tx,
                    })
                    .await
                    .map_err(|_| io::Error::other("cache task stopped"))?;

                let response = response_rx
                    .await
                    .map_err(|_| io::Error::other("cache task dropped response"))?;

                stream.write_all(&response.encode()).await?;

                received.drain(..consumed);
                continue;
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

async fn run_cache(mut request_rx: mpsc::Receiver<CacheRequest>) {
    let mut cache = Cache::new();

    while let Some(request) = request_rx.recv().await {
        let response = request.command.execute(&mut cache);

        let _ = request.response_tx.send(response);
    }
}
