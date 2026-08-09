use crate::cache::Cache;
use crate::command::{Command, ParseError, parse};
use crate::response::Response;
use std::io;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{mpsc, oneshot};

const MAX_REQUEST_SIZE: usize = 1024 * 1024;

fn request_is_too_large(size: usize) -> bool {
    size > MAX_REQUEST_SIZE
}

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

        if request_is_too_large(received.len()) {
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

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test(flavor = "current_thread")]
    async fn run_cache_stores_and_retrieves_a_value() {
        let (request_tx, request_rx) = mpsc::channel(1);

        let cache_task = tokio::spawn(run_cache(request_rx));

        let set_response = send_command(
            &request_tx,
            Command::Set {
                key: b"name".to_vec(),
                value: b"Alice".to_vec(),
                ttl: None,
            },
        )
        .await;

        assert_eq!(set_response, Response::Stored);

        let get_response = send_command(
            &request_tx,
            Command::Get {
                key: b"name".to_vec(),
            },
        )
        .await;

        assert_eq!(get_response, Response::Value(b"Alice".to_vec()));

        drop(request_tx);
        cache_task.await.unwrap();
    }

    #[tokio::test(flavor = "current_thread")]
    async fn handle_connection_processes_multiple_commands() {
        let (mut client, server) = tcp_pair().await;

        let (request_tx, request_rx) = mpsc::channel(1);

        let cache_task = tokio::spawn(run_cache(request_rx));
        let connection_task = tokio::spawn(handle_connection(server, request_tx.clone()));

        client
            .write_all(b"SET 4 5\r\nnameAliceGET 4\r\nname")
            .await
            .unwrap();

        client.shutdown().await.unwrap();

        let expected = b"STORED\r\nVALUE 5\r\nAlice";
        let mut response = vec![0_u8; expected.len()];

        client.read_exact(&mut response).await.unwrap();

        assert_eq!(response, expected);

        connection_task.await.unwrap().unwrap();

        drop(request_tx);
        cache_task.await.unwrap();
    }

    #[tokio::test(flavor = "current_thread")]
    async fn handle_connection_returns_unexpected_eof_for_incomplete_request() {
        let (mut client, server) = tcp_pair().await;

        let (request_tx, _request_rx) = mpsc::channel(1);

        let connection_task = tokio::spawn(handle_connection(server, request_tx));

        client.write_all(b"SET 4 5\r\nnameAli").await.unwrap();

        client.shutdown().await.unwrap();

        let error = connection_task.await.unwrap().unwrap_err();

        assert_eq!(error.kind(), io::ErrorKind::UnexpectedEof);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn run_returns_error_when_address_is_already_in_use() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap().to_string();

        let error = run(&address).await.unwrap_err();

        assert_eq!(error.kind(), io::ErrorKind::AddrInUse);
    }

    #[test]
    fn maximum_request_size_is_one_mebibyte() {
        assert_eq!(MAX_REQUEST_SIZE, 1_048_576);
    }

    #[test]
    fn request_size_below_limit_is_allowed() {
        assert!(!request_is_too_large(MAX_REQUEST_SIZE - 1));
    }

    #[test]
    fn request_size_at_limit_is_allowed() {
        assert!(!request_is_too_large(MAX_REQUEST_SIZE));
    }

    #[test]
    fn request_size_above_limit_is_rejected() {
        assert!(request_is_too_large(MAX_REQUEST_SIZE + 1));
    }

    async fn send_command(request_tx: &mpsc::Sender<CacheRequest>, command: Command) -> Response {
        let (response_tx, response_rx) = oneshot::channel();

        request_tx
            .send(CacheRequest {
                command,
                response_tx,
            })
            .await
            .unwrap();

        response_rx.await.unwrap()
    }

    async fn tcp_pair() -> (TcpStream, TcpStream) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();

        let connect = TcpStream::connect(address);
        let accept = listener.accept();

        let (client, server) = tokio::join!(connect, accept);

        let client = client.unwrap();
        let (server, _) = server.unwrap();

        (client, server)
    }
}
