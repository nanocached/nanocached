use crate::cache::Cache;
use crate::command::{Command, ParseError, parse};
use crate::response::Response;
use bytes::BytesMut;
use std::io;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{Semaphore, mpsc, oneshot, watch};
use tokio::task::JoinSet;
use tokio::time::timeout;

const MAX_REQUEST_SIZE: usize = 1024 * 1024;
const MAX_CONNECTIONS: usize = 1024;
const MAX_CACHE_MEMORY_BYTES: usize = 256 * 1024 * 1024;
const IDLE_TIMEOUT: Duration = Duration::from_secs(30);
const SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(10);
const READ_CHUNK_SIZE: usize = 1024;

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
    let mut connection_tasks = JoinSet::new();
    let (shutdown_tx, shutdown_rx) = watch::channel(false);

    let cache_task = tokio::spawn(run_cache(request_rx));

    let shutdown = shutdown_signal();
    tokio::pin!(shutdown);

    loop {
        tokio::select! {
            biased;

            result = &mut shutdown => {
                result?;
                println!("shutdown signal received");
                shutdown_tx.send_replace(true);
                break;
            }

            result = connection_tasks.join_next(), if !connection_tasks.is_empty() => {
                if let Some(Err(error)) = result {
                    eprintln!("connection task failed: {error}");
                }
            }

            result = listener.accept() => {
                let (stream, address) = result?;

                dispatch_connection(
                    stream,
                    address,
                    request_tx.clone(),
                    Arc::clone(&connection_limit),
                    IDLE_TIMEOUT,
                    shutdown_rx.clone(),
                    &mut connection_tasks,
                )
                .await;
            }

        }
    }

    let connections_finished = timeout(SHUTDOWN_TIMEOUT, async {
        while let Some(result) = connection_tasks.join_next().await {
            if let Err(error) = result {
                eprintln!("connection task failed: {error}");
            }
        }
    })
    .await;

    if connections_finished.is_err() {
        eprintln!("shutdown timeout reached");
        connection_tasks.abort_all();

        while connection_tasks.join_next().await.is_some() {}
    }

    drop(request_tx);

    cache_task
        .await
        .map_err(|error| io::Error::other(format!("cache task failed: {error}")))?;

    Ok(())
}

async fn dispatch_connection(
    mut stream: TcpStream,
    address: SocketAddr,
    request_tx: mpsc::Sender<CacheRequest>,
    connection_limit: Arc<Semaphore>,
    idle_timeout: Duration,
    shutdown_rx: watch::Receiver<bool>,
    connection_tasks: &mut JoinSet<()>,
) -> bool {
    // Every request/response is small; without this, the kernel may delay
    // small writes waiting to coalesce with more data (Nagle's algorithm).
    let _ = stream.set_nodelay(true);

    let permit = match connection_limit.try_acquire_owned() {
        Ok(permit) => permit,
        Err(_) => {
            let busy = Response::Busy.encode();

            if let Err(error) = stream.write_all(&busy).await {
                eprintln!("failed to send busy response to {address}: {error}");
            }

            return false;
        }
    };

    println!("accepted connection from {address}");

    connection_tasks.spawn(async move {
        let _connection_permit = permit;

        if let Err(error) = handle_connection(stream, request_tx, idle_timeout, shutdown_rx).await {
            eprintln!("connection error from {address}: {error}");
        }
    });

    true
}

async fn handle_connection(
    mut stream: TcpStream,
    request_tx: mpsc::Sender<CacheRequest>,
    idle_timeout: Duration,
    mut shutdown_rx: watch::Receiver<bool>,
) -> io::Result<()> {
    let mut received = BytesMut::new();

    loop {
        if *shutdown_rx.borrow() {
            return Ok(());
        }

        match parse(&mut received) {
            Ok(command) => {
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

        received.reserve(READ_CHUNK_SIZE);

        let bytes_read = tokio::select! {
            _ = shutdown_rx.changed() => return Ok(()),

            result = timeout(idle_timeout, stream.read_buf(&mut received)) => {
                result.map_err(|_| {
                    io::Error::new(
                    io::ErrorKind::TimedOut,
                    "connection idle timeout",
                )
                })??
            }
        };

        if bytes_read == 0 {
            if received.is_empty() {
                return Ok(());
            }

            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "connection closed before request was complete",
            ));
        }

        if request_is_too_large(received.len()) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "request is too large",
            ));
        }
    }
}

async fn run_cache(mut request_rx: mpsc::Receiver<CacheRequest>) {
    let mut cache = Cache::new(MAX_CACHE_MEMORY_BYTES);

    while let Some(request) = request_rx.recv().await {
        let response = request.command.execute(&mut cache);

        let _ = request.response_tx.send(response);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;

    #[tokio::test(flavor = "current_thread")]
    async fn run_cache_stores_and_retrieves_a_value() {
        let (request_tx, request_rx) = mpsc::channel(1);

        let cache_task = tokio::spawn(run_cache(request_rx));

        let set_response = send_command(
            &request_tx,
            Command::Set {
                key: Bytes::from_static(b"name"),
                value: Bytes::from_static(b"Alice"),
                ttl: None,
            },
        )
        .await;

        assert_eq!(set_response, Response::Stored);

        let get_response = send_command(
            &request_tx,
            Command::Get {
                key: Bytes::from_static(b"name"),
            },
        )
        .await;

        assert_eq!(get_response, Response::Value(Bytes::from_static(b"Alice")));

        drop(request_tx);
        cache_task.await.unwrap();
    }

    #[tokio::test(flavor = "current_thread")]
    async fn handle_connection_processes_multiple_commands() {
        let (mut client, server) = tcp_pair().await;

        let (request_tx, request_rx) = mpsc::channel(1);
        let (_shutdown_tx, shutdown_rx) = watch::channel(false);

        let cache_task = tokio::spawn(run_cache(request_rx));
        let connection_task = tokio::spawn(handle_connection(
            server,
            request_tx.clone(),
            IDLE_TIMEOUT,
            shutdown_rx,
        ));

        client
            .write_all(b"S 4 5\nnameAliceG 4\nname")
            .await
            .unwrap();

        client.shutdown().await.unwrap();

        let expected = b"S\nV 5\nAlice";
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
        let (_shutdown_tx, shutdown_rx) = watch::channel(false);

        let connection_task = tokio::spawn(handle_connection(
            server,
            request_tx,
            IDLE_TIMEOUT,
            shutdown_rx,
        ));

        client.write_all(b"S 4 5\nnameAli").await.unwrap();

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
        let (_shutdown_tx, shutdown_rx) = watch::channel(false);

        let connection_task = tokio::spawn(handle_connection(
            server,
            request_tx,
            IDLE_TIMEOUT,
            shutdown_rx,
        ));

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
    fn maximum_cache_memory_is_256_mebibytes() {
        assert_eq!(MAX_CACHE_MEMORY_BYTES, 268_435_456);
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

        let mut connection_tasks = JoinSet::new();
        let (_shutdown_tx, shutdown_rx) = watch::channel(false);

        let first_connection = dispatch_connection(
            first_server,
            first_address,
            request_tx.clone(),
            Arc::clone(&connection_limit),
            IDLE_TIMEOUT,
            shutdown_rx.clone(),
            &mut connection_tasks,
        )
        .await;

        assert!(first_connection);
        assert_eq!(connection_limit.available_permits(), 0);

        let (mut second_client, second_server) = tcp_pair().await;
        let second_address = second_server.peer_addr().unwrap();

        let second_connection = dispatch_connection(
            second_server,
            second_address,
            request_tx.clone(),
            Arc::clone(&connection_limit),
            IDLE_TIMEOUT,
            shutdown_rx,
            &mut connection_tasks,
        )
        .await;

        assert!(!second_connection);

        let mut response = Vec::new();
        second_client.read_to_end(&mut response).await.unwrap();

        assert_eq!(response, b"B\n");

        connection_tasks.abort_all();

        let join_error = connection_tasks.join_next().await.unwrap().unwrap_err();

        assert!(join_error.is_cancelled());
        assert!(connection_tasks.is_empty());
        assert_eq!(connection_limit.available_permits(), 1);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn handle_connection_stops_when_shutdown_is_requested() {
        let (_client, server) = tcp_pair().await;
        let (request_tx, _request_rx) = mpsc::channel(1);
        let (shutdown_tx, shutdown_rx) = watch::channel(false);

        let connection_task = tokio::spawn(handle_connection(
            server,
            request_tx,
            IDLE_TIMEOUT,
            shutdown_rx,
        ));

        tokio::task::yield_now().await;
        shutdown_tx.send_replace(true);

        connection_task.await.unwrap().unwrap();
    }

    #[tokio::test(flavor = "current_thread")]
    async fn handle_connection_finishes_in_flight_request_during_shutdown() {
        let (mut client, server) = tcp_pair().await;
        let (request_tx, mut request_rx) = mpsc::channel(1);
        let (shutdown_tx, shutdown_rx) = watch::channel(false);

        let connection_task = tokio::spawn(handle_connection(
            server,
            request_tx,
            IDLE_TIMEOUT,
            shutdown_rx,
        ));

        client.write_all(b"G 4\nname").await.unwrap();

        let request = request_rx.recv().await.unwrap();

        assert_eq!(
            request.command,
            Command::Get {
                key: Bytes::from_static(b"name"),
            },
        );

        shutdown_tx.send_replace(true);

        request
            .response_tx
            .send(Response::Value(Bytes::from_static(b"Alice")))
            .unwrap();

        let expected = b"V 5\nAlice";
        let mut response = vec![0_u8; expected.len()];

        client.read_exact(&mut response).await.unwrap();

        assert_eq!(response, expected);

        connection_task.await.unwrap().unwrap();
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
