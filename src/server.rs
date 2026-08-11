use crate::cache::Cache;
use crate::command::{Command, ParseError, parse};
use crate::response::Response;
use std::io;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{Semaphore, mpsc, oneshot};
use tokio::task::JoinHandle;
use tokio::time::timeout;

const MAX_REQUEST_SIZE: usize = 1024 * 1024;
const MAX_CONNECTIONS: usize = 1024;
const IDLE_TIMEOUT: Duration = Duration::from_secs(30);

fn request_is_too_large(size: usize) -> bool {
    size > MAX_REQUEST_SIZE
}

struct CacheRequest {
    command: Command,
    response_tx: oneshot::Sender<Response>,
}

async fn shutdown_signal() -> io::Result<()> {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{SignalKind, signal};

        let mut terminate = signal(SignalKind::terminate())?;

        tokio::select! {
            result = tokio::signal::ctrl_c() => result,
            _ = terminate.recv() => Ok(()),
        }
    }

    #[cfg(not(unix))]
    {
        tokio::signal::ctrl_c().await
    }
}

pub(crate) async fn run(address: &str) -> io::Result<()> {
    let listener = TcpListener::bind(address).await?;

    let (request_tx, request_rx) = mpsc::channel(1024);
    let connection_limit = Arc::new(Semaphore::new(MAX_CONNECTIONS));

    tokio::spawn(run_cache(request_rx));

    let shutdown = shutdown_signal();
    tokio::pin!(shutdown);

    loop {
        tokio::select! {
            biased;

            result = &mut shutdown => {
                result?;
                println!("shutdown signal received");
                break;
            }

            result = listener.accept() => {
                let (stream, address) = result?;

                drop(
                    dispatch_connection(
                        stream,
                        address,
                        request_tx.clone(),
                        Arc::clone(&connection_limit),
                        IDLE_TIMEOUT,
                    )
                    .await,
                );
            }
        }
    }

    Ok(())
}

async fn dispatch_connection(
    mut stream: TcpStream,
    address: SocketAddr,
    request_tx: mpsc::Sender<CacheRequest>,
    connection_limit: Arc<Semaphore>,
    idle_timeout: Duration,
) -> Option<JoinHandle<()>> {
    let permit = match connection_limit.try_acquire_owned() {
        Ok(permit) => permit,
        Err(_) => {
            let busy = Response::Busy.encode();

            if let Err(error) = stream.write_all(&busy).await {
                eprintln!("failed to send busy response to {address}: {error}");
            }

            return None;
        }
    };

    println!("accepted connection from {address}");

    Some(tokio::spawn(async move {
        let _connection_permit = permit;

        if let Err(error) = handle_connection(stream, request_tx, idle_timeout).await {
            eprintln!("connection error from {address}: {error}");
        }
    }))
}

async fn handle_connection(
    mut stream: TcpStream,
    request_tx: mpsc::Sender<CacheRequest>,
    idle_timeout: Duration,
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

        let bytes_read = timeout(idle_timeout, stream.read(&mut chunk))
            .await
            .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "connection idle timeout"))??;

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
        let connection_task =
            tokio::spawn(handle_connection(server, request_tx.clone(), IDLE_TIMEOUT));

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

        let connection_task = tokio::spawn(handle_connection(server, request_tx, IDLE_TIMEOUT));

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

    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn handle_connection_times_out_when_client_is_idle() {
        let (_client, server) = tcp_pair().await;

        let (request_tx, _request_rx) = mpsc::channel(1);

        let connection_task = tokio::spawn(handle_connection(server, request_tx, IDLE_TIMEOUT));

        tokio::task::yield_now().await;
        tokio::time::advance(IDLE_TIMEOUT).await;

        let error = connection_task.await.unwrap().unwrap_err();

        assert_eq!(error.kind(), io::ErrorKind::TimedOut);
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

    #[tokio::test(flavor = "current_thread")]
    async fn rejects_connection_when_connection_limit_is_reached() {
        let connection_limit = Arc::new(Semaphore::new(1));
        let (request_tx, _request_rx) = mpsc::channel(1);

        let (_first_client, first_server) = tcp_pair().await;
        let first_address = first_server.peer_addr().unwrap();

        let first_connection = dispatch_connection(
            first_server,
            first_address,
            request_tx.clone(),
            Arc::clone(&connection_limit),
            IDLE_TIMEOUT,
        )
        .await
        .unwrap();

        assert_eq!(connection_limit.available_permits(), 0);

        let (mut second_client, second_server) = tcp_pair().await;
        let second_address = second_server.peer_addr().unwrap();

        let second_connection = dispatch_connection(
            second_server,
            second_address,
            request_tx.clone(),
            Arc::clone(&connection_limit),
            IDLE_TIMEOUT,
        )
        .await;

        assert!(second_connection.is_none());

        let mut response = Vec::new();
        second_client.read_to_end(&mut response).await.unwrap();

        assert_eq!(response, b"BUSY\r\n");

        first_connection.abort();

        let join_error = first_connection.await.unwrap_err();

        assert!(join_error.is_cancelled());
        assert_eq!(connection_limit.available_permits(), 1);
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
