use crate::cache::Cache;
use crate::command::{Command, ParseError, parse};
use crate::response::Response;
use bytes::{Bytes, BytesMut};
use rustls::pki_types::{CertificateDer, PrivateKeyDer, ServerName};
use rustls::{ClientConfig, RootCertStore, ServerConfig};
use std::io;
use std::io::BufReader;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, ReadBuf};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{Semaphore, mpsc, oneshot, watch};
use tokio::task::JoinSet;
use tokio::time::{sleep, timeout};
use tokio_rustls::{TlsAcceptor, TlsConnector};

const MAX_REQUEST_SIZE: usize = 1024 * 1024;
const MAX_CONNECTIONS: usize = 1024;
const MAX_CACHE_MEMORY_BYTES: usize = 256 * 1024 * 1024;
const IDLE_TIMEOUT: Duration = Duration::from_secs(30);
const SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(10);
const TLS_HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(10);
const READ_CHUNK_SIZE: usize = 1024;

fn request_is_too_large(size: usize) -> bool {
    size > MAX_REQUEST_SIZE
}

/// Compares two byte strings without leaking, via timing, how many leading
/// bytes matched. Length differs openly (no secret ever has a length worth
/// hiding), but once lengths match, every byte is compared regardless of
/// earlier mismatches.
fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }

    let mut diff: u8 = 0;
    for (x, y) in a.iter().zip(b.iter()) {
        diff |= x ^ y;
    }

    diff == 0
}

/// Wraps either a plain TCP connection or one wrapped in TLS behind a
/// single type, so the rest of the connection-handling code doesn't need to
/// know which is in play. `P` is always `TcpStream` here; `T` is the
/// TLS-wrapped stream type, which differs between the accept side
/// (`tokio_rustls::server::TlsStream`, used for client connections this
/// process accepts) and the connect side (`tokio_rustls::client::TlsStream`,
/// used for the heartbeat connection this process makes to a discovery
/// server).
enum MaybeTls<P, T> {
    Plain(P),
    Tls(Box<T>),
}

impl<P: AsyncRead + Unpin, T: AsyncRead + Unpin> AsyncRead for MaybeTls<P, T> {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        match self.get_mut() {
            MaybeTls::Plain(stream) => Pin::new(stream).poll_read(cx, buf),
            MaybeTls::Tls(stream) => Pin::new(stream.as_mut()).poll_read(cx, buf),
        }
    }
}

impl<P: AsyncWrite + Unpin, T: AsyncWrite + Unpin> AsyncWrite for MaybeTls<P, T> {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        match self.get_mut() {
            MaybeTls::Plain(stream) => Pin::new(stream).poll_write(cx, buf),
            MaybeTls::Tls(stream) => Pin::new(stream.as_mut()).poll_write(cx, buf),
        }
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match self.get_mut() {
            MaybeTls::Plain(stream) => Pin::new(stream).poll_flush(cx),
            MaybeTls::Tls(stream) => Pin::new(stream.as_mut()).poll_flush(cx),
        }
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match self.get_mut() {
            MaybeTls::Plain(stream) => Pin::new(stream).poll_shutdown(cx),
            MaybeTls::Tls(stream) => Pin::new(stream.as_mut()).poll_shutdown(cx),
        }
    }
}

/// A connection this process accepted, plaintext or TLS-terminated.
type ServerStream = MaybeTls<TcpStream, tokio_rustls::server::TlsStream<TcpStream>>;
/// A connection this process opened outbound, plaintext or TLS-secured.
type ClientStream = MaybeTls<TcpStream, tokio_rustls::client::TlsStream<TcpStream>>;

/// Loads a certificate chain and private key from PEM files and builds a
/// `TlsAcceptor` for terminating incoming TLS connections.
pub(crate) fn load_tls_acceptor(cert_path: &str, key_path: &str) -> io::Result<TlsAcceptor> {
    let certs = load_cert_chain(cert_path)?;
    let key = load_private_key(key_path)?;

    let config = ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(certs, key)
        .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;

    Ok(TlsAcceptor::from(Arc::new(config)))
}

/// Loads CA certificates from a PEM file and builds a `TlsConnector` that
/// trusts only those CAs (not the system trust store), for connecting out to
/// another nanocached process's TLS-secured port.
pub(crate) fn load_tls_connector(ca_path: &str) -> io::Result<TlsConnector> {
    let certs = load_cert_chain(ca_path)?;
    let mut roots = RootCertStore::empty();

    for cert in certs {
        roots
            .add(cert)
            .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error.to_string()))?;
    }

    let config = ClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth();

    Ok(TlsConnector::from(Arc::new(config)))
}

fn load_cert_chain(path: &str) -> io::Result<Vec<CertificateDer<'static>>> {
    let file = std::fs::File::open(path)?;
    rustls_pemfile::certs(&mut BufReader::new(file)).collect()
}

fn load_private_key(path: &str) -> io::Result<PrivateKeyDer<'static>> {
    let file = std::fs::File::open(path)?;
    rustls_pemfile::private_key(&mut BufReader::new(file))?.ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            format!("no private key found in {path}"),
        )
    })
}

/// Parses the host portion of a `host:port` address into a TLS server name
/// for certificate verification, accepting either a DNS name or IP address.
fn server_name_from_addr(addr: &str) -> io::Result<ServerName<'static>> {
    let host = addr.rsplit_once(':').map_or(addr, |(host, _)| host);

    ServerName::try_from(host.to_string()).map_err(|error| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("invalid TLS server name {host:?}: {error}"),
        )
    })
}

struct CacheRequest {
    command: Command,
    response_tx: oneshot::Sender<Response>,
}

/// Per-connection settings that don't change once `run` starts, grouped so
/// `dispatch_connection`/`handle_connection` take one value instead of two.
#[derive(Clone)]
struct ConnectionConfig {
    idle_timeout: Duration,
    auth_secret: Option<Bytes>,
    /// When set, every accepted connection must complete a TLS handshake
    /// before speaking the cache protocol; there is no plaintext fallback.
    tls_acceptor: Option<TlsAcceptor>,
}

/// Configuration for registering this node with a discovery server (see
/// `src/bin/nanocached-discovery.rs`). When set, `run` sends a heartbeat
/// declaring `advertise_addr` on `interval`, well under the discovery
/// server's own liveness timeout.
pub(crate) struct HeartbeatConfig {
    pub(crate) discovery_addr: String,
    pub(crate) advertise_addr: String,
    pub(crate) interval: Duration,
    /// Sent to the discovery server before the first heartbeat on each
    /// (re)connection, if the discovery server requires auth. This is the
    /// same shared secret this node uses to gate its own connections.
    pub(crate) auth_secret: Option<Bytes>,
    /// When set, the heartbeat connection to the discovery server is made
    /// over TLS, trusting only the CAs loaded into this connector. This is
    /// the same `--tls-ca` this node uses for any other outbound TLS
    /// connections it makes.
    pub(crate) tls_connector: Option<TlsConnector>,
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

pub(crate) async fn run(
    address: &str,
    heartbeat: Option<HeartbeatConfig>,
    auth_secret: Option<Bytes>,
    tls_acceptor: Option<TlsAcceptor>,
) -> io::Result<()> {
    let listener = TcpListener::bind(address).await?;

    let (request_tx, request_rx) = mpsc::channel(1024);
    let connection_limit = Arc::new(Semaphore::new(MAX_CONNECTIONS));
    let mut connection_tasks = JoinSet::new();
    let (shutdown_tx, shutdown_rx) = watch::channel(false);

    let cache_task = tokio::spawn(run_cache(request_rx));
    let heartbeat_task =
        heartbeat.map(|config| tokio::spawn(send_heartbeats(config, shutdown_rx.clone())));

    let connection_config = ConnectionConfig {
        idle_timeout: IDLE_TIMEOUT,
        auth_secret,
        tls_acceptor,
    };

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
                    connection_config.clone(),
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

    if let Some(heartbeat_task) = heartbeat_task {
        heartbeat_task
            .await
            .map_err(|error| io::Error::other(format!("heartbeat task failed: {error}")))?;
    }

    Ok(())
}

async fn dispatch_connection(
    stream: TcpStream,
    address: SocketAddr,
    request_tx: mpsc::Sender<CacheRequest>,
    connection_limit: Arc<Semaphore>,
    config: ConnectionConfig,
    shutdown_rx: watch::Receiver<bool>,
    connection_tasks: &mut JoinSet<()>,
) -> bool {
    // Every request/response is small; without this, the kernel may delay
    // small writes waiting to coalesce with more data (Nagle's algorithm).
    let _ = stream.set_nodelay(true);

    let mut stream: ServerStream = match &config.tls_acceptor {
        Some(acceptor) => match timeout(TLS_HANDSHAKE_TIMEOUT, acceptor.accept(stream)).await {
            Ok(Ok(tls_stream)) => ServerStream::Tls(Box::new(tls_stream)),
            Ok(Err(error)) => {
                eprintln!("TLS handshake with {address} failed: {error}");
                return false;
            }
            Err(_) => {
                eprintln!("TLS handshake with {address} timed out");
                return false;
            }
        },
        None => ServerStream::Plain(stream),
    };

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

        if let Err(error) = handle_connection(stream, request_tx, config, shutdown_rx).await {
            eprintln!("connection error from {address}: {error}");
        }
    });

    true
}

async fn handle_connection(
    mut stream: ServerStream,
    request_tx: mpsc::Sender<CacheRequest>,
    config: ConnectionConfig,
    mut shutdown_rx: watch::Receiver<bool>,
) -> io::Result<()> {
    let mut received = BytesMut::new();
    // No secret configured means auth isn't required, so every connection
    // starts already authenticated.
    let mut authenticated = config.auth_secret.is_none();

    loop {
        if *shutdown_rx.borrow() {
            return Ok(());
        }

        match parse(&mut received) {
            Ok(Command::Auth { secret }) => {
                let accepted = match &config.auth_secret {
                    Some(expected) => constant_time_eq(&secret, expected),
                    None => true,
                };

                if accepted {
                    authenticated = true;
                    stream.write_all(&Response::AuthOk.encode()).await?;
                    continue;
                }

                stream.write_all(&Response::Unauthorized.encode()).await?;
                return Err(io::Error::new(
                    io::ErrorKind::PermissionDenied,
                    "invalid auth secret",
                ));
            }
            Ok(_) if !authenticated => {
                stream.write_all(&Response::Unauthorized.encode()).await?;
                return Err(io::Error::new(
                    io::ErrorKind::PermissionDenied,
                    "command sent before authenticating",
                ));
            }
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

            result = timeout(config.idle_timeout, stream.read_buf(&mut received)) => {
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

fn heartbeat_message(advertise_addr: &str) -> Vec<u8> {
    let mut message = format!("H {}\n", advertise_addr.len()).into_bytes();
    message.extend_from_slice(advertise_addr.as_bytes());
    message
}

fn auth_message(secret: &[u8]) -> Vec<u8> {
    let mut message = format!("A {}\n", secret.len()).into_bytes();
    message.extend_from_slice(secret);
    message
}

/// Holds one long-lived connection to the discovery server and sends a
/// heartbeat on it every `config.interval`, reconnecting on any I/O error
/// after waiting out the interval. Each heartbeat is a self-contained
/// register-or-refresh, so a dropped connection just delays the next
/// heartbeat rather than requiring any resend/replay logic.
/// Connects to the discovery server, upgrading to TLS first if
/// `config.tls_connector` is set. There is no plaintext fallback: if TLS is
/// configured and the handshake fails, the connection attempt fails too.
async fn connect_heartbeat_stream(config: &HeartbeatConfig) -> io::Result<ClientStream> {
    let stream = TcpStream::connect(&config.discovery_addr).await?;
    let _ = stream.set_nodelay(true);

    match &config.tls_connector {
        Some(connector) => {
            let server_name = server_name_from_addr(&config.discovery_addr)?;
            let tls_stream = timeout(
                TLS_HANDSHAKE_TIMEOUT,
                connector.connect(server_name, stream),
            )
            .await
            .map_err(|_| {
                io::Error::new(
                    io::ErrorKind::TimedOut,
                    "TLS handshake with discovery server timed out",
                )
            })??;

            Ok(ClientStream::Tls(Box::new(tls_stream)))
        }
        None => Ok(ClientStream::Plain(stream)),
    }
}

async fn send_heartbeats(config: HeartbeatConfig, mut shutdown_rx: watch::Receiver<bool>) {
    let message = heartbeat_message(&config.advertise_addr);

    loop {
        if *shutdown_rx.borrow() {
            return;
        }

        match connect_heartbeat_stream(&config).await {
            Ok(mut stream) => {
                let authenticated = match &config.auth_secret {
                    Some(secret) => {
                        let auth = auth_message(secret);
                        match stream.write_all(&auth).await {
                            Ok(()) => {
                                let mut ack = [0u8; 2];
                                stream.read_exact(&mut ack).await.is_ok() && &ack == b"O\n"
                            }
                            Err(_) => false,
                        }
                    }
                    None => true,
                };

                if !authenticated {
                    eprintln!(
                        "discovery server at {} rejected the auth secret",
                        config.discovery_addr
                    );
                }

                if authenticated {
                    loop {
                        if stream.write_all(&message).await.is_err() {
                            break;
                        }

                        let mut ack = [0u8; 2];
                        let read_ack = tokio::select! {
                            _ = shutdown_rx.changed() => return,
                            result = stream.read_exact(&mut ack) => result,
                        };

                        if read_ack.is_err() || &ack != b"A\n" {
                            break;
                        }

                        if wait_or_shutdown(config.interval, &mut shutdown_rx).await {
                            return;
                        }
                    }
                }
            }
            Err(error) => {
                eprintln!(
                    "failed to connect to discovery server at {}: {error}",
                    config.discovery_addr
                );
            }
        }

        if wait_or_shutdown(config.interval, &mut shutdown_rx).await {
            return;
        }
    }
}

/// Waits for `duration`, or returns `true` early if shutdown is signaled.
async fn wait_or_shutdown(duration: Duration, shutdown_rx: &mut watch::Receiver<bool>) -> bool {
    tokio::select! {
        _ = sleep(duration) => false,
        _ = shutdown_rx.changed() => true,
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
            ServerStream::Plain(server),
            request_tx.clone(),
            ConnectionConfig {
                idle_timeout: IDLE_TIMEOUT,
                auth_secret: None,
                tls_acceptor: None,
            },
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
            ServerStream::Plain(server),
            request_tx,
            ConnectionConfig {
                idle_timeout: IDLE_TIMEOUT,
                auth_secret: None,
                tls_acceptor: None,
            },
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

        let error = run(&address, None, None, None).await.unwrap_err();

        assert_eq!(error.kind(), io::ErrorKind::AddrInUse);
    }

    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn handle_connection_times_out_when_client_is_idle() {
        let (_client, server) = tcp_pair().await;

        let (request_tx, _request_rx) = mpsc::channel(1);
        let (_shutdown_tx, shutdown_rx) = watch::channel(false);

        let connection_task = tokio::spawn(handle_connection(
            ServerStream::Plain(server),
            request_tx,
            ConnectionConfig {
                idle_timeout: IDLE_TIMEOUT,
                auth_secret: None,
                tls_acceptor: None,
            },
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
            ConnectionConfig {
                idle_timeout: IDLE_TIMEOUT,
                auth_secret: None,
                tls_acceptor: None,
            },
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
            ConnectionConfig {
                idle_timeout: IDLE_TIMEOUT,
                auth_secret: None,
                tls_acceptor: None,
            },
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
            ServerStream::Plain(server),
            request_tx,
            ConnectionConfig {
                idle_timeout: IDLE_TIMEOUT,
                auth_secret: None,
                tls_acceptor: None,
            },
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
            ServerStream::Plain(server),
            request_tx,
            ConnectionConfig {
                idle_timeout: IDLE_TIMEOUT,
                auth_secret: None,
                tls_acceptor: None,
            },
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

    #[tokio::test(flavor = "current_thread")]
    async fn handle_connection_rejects_commands_sent_before_authenticating() {
        let (mut client, server) = tcp_pair().await;
        let (request_tx, _request_rx) = mpsc::channel(1);
        let (_shutdown_tx, shutdown_rx) = watch::channel(false);

        let connection_task = tokio::spawn(handle_connection(
            ServerStream::Plain(server),
            request_tx,
            ConnectionConfig {
                idle_timeout: IDLE_TIMEOUT,
                auth_secret: Some(Bytes::from_static(b"correct-secret")),
                tls_acceptor: None,
            },
            shutdown_rx,
        ));

        client.write_all(b"G 4\nname").await.unwrap();

        let mut response = [0u8; 2];
        client.read_exact(&mut response).await.unwrap();
        assert_eq!(&response, b"E\n");

        let error = connection_task.await.unwrap().unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::PermissionDenied);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn handle_connection_rejects_an_incorrect_auth_secret() {
        let (mut client, server) = tcp_pair().await;
        let (request_tx, _request_rx) = mpsc::channel(1);
        let (_shutdown_tx, shutdown_rx) = watch::channel(false);

        let connection_task = tokio::spawn(handle_connection(
            ServerStream::Plain(server),
            request_tx,
            ConnectionConfig {
                idle_timeout: IDLE_TIMEOUT,
                auth_secret: Some(Bytes::from_static(b"correct-secret")),
                tls_acceptor: None,
            },
            shutdown_rx,
        ));

        client.write_all(b"A 11\nwrong-value").await.unwrap();

        let mut response = [0u8; 2];
        client.read_exact(&mut response).await.unwrap();
        assert_eq!(&response, b"E\n");

        let error = connection_task.await.unwrap().unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::PermissionDenied);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn handle_connection_accepts_commands_after_correct_auth() {
        let (mut client, server) = tcp_pair().await;
        let (request_tx, request_rx) = mpsc::channel(1);
        let (_shutdown_tx, shutdown_rx) = watch::channel(false);

        let cache_task = tokio::spawn(run_cache(request_rx));
        let connection_task = tokio::spawn(handle_connection(
            ServerStream::Plain(server),
            request_tx.clone(),
            ConnectionConfig {
                idle_timeout: IDLE_TIMEOUT,
                auth_secret: Some(Bytes::from_static(b"correct-secret")),
                tls_acceptor: None,
            },
            shutdown_rx,
        ));

        client
            .write_all(b"A 14\ncorrect-secretG 4\nname")
            .await
            .unwrap();
        client.shutdown().await.unwrap();

        let expected = b"O\nN\n";
        let mut response = vec![0_u8; expected.len()];
        client.read_exact(&mut response).await.unwrap();
        assert_eq!(response, expected);

        connection_task.await.unwrap().unwrap();

        drop(request_tx);
        cache_task.await.unwrap();
    }

    #[tokio::test(flavor = "current_thread")]
    async fn handle_connection_treats_auth_as_a_no_op_when_no_secret_is_configured() {
        let (mut client, server) = tcp_pair().await;
        let (request_tx, request_rx) = mpsc::channel(1);
        let (_shutdown_tx, shutdown_rx) = watch::channel(false);

        let cache_task = tokio::spawn(run_cache(request_rx));
        let connection_task = tokio::spawn(handle_connection(
            ServerStream::Plain(server),
            request_tx.clone(),
            ConnectionConfig {
                idle_timeout: IDLE_TIMEOUT,
                auth_secret: None,
                tls_acceptor: None,
            },
            shutdown_rx,
        ));

        client.write_all(b"A 8\nanything").await.unwrap();
        client.shutdown().await.unwrap();

        let expected = b"O\n";
        let mut response = vec![0_u8; expected.len()];
        client.read_exact(&mut response).await.unwrap();
        assert_eq!(response, expected);

        connection_task.await.unwrap().unwrap();

        drop(request_tx);
        cache_task.await.unwrap();
    }

    #[test]
    fn constant_time_eq_matches_identical_byte_strings() {
        assert!(constant_time_eq(b"same-secret", b"same-secret"));
    }

    #[test]
    fn constant_time_eq_rejects_different_content_of_the_same_length() {
        assert!(!constant_time_eq(b"secret-one", b"secret-two"));
    }

    #[test]
    fn constant_time_eq_rejects_different_lengths() {
        assert!(!constant_time_eq(b"short", b"a much longer value"));
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

    #[test]
    fn heartbeat_message_declares_the_address_length_before_the_address() {
        assert_eq!(
            heartbeat_message("127.0.0.1:8356"),
            b"H 14\n127.0.0.1:8356".to_vec()
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn send_heartbeats_stops_immediately_when_already_shut_down() {
        let (_shutdown_tx, shutdown_rx) = watch::channel(true);

        send_heartbeats(
            HeartbeatConfig {
                discovery_addr: "127.0.0.1:1".to_string(),
                advertise_addr: "127.0.0.1:8356".to_string(),
                interval: Duration::from_secs(60),
                auth_secret: None,
                tls_connector: None,
            },
            shutdown_rx,
        )
        .await;
    }

    #[tokio::test(flavor = "current_thread")]
    async fn send_heartbeats_sends_periodic_heartbeats_on_one_connection() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let discovery_addr = listener.local_addr().unwrap().to_string();

        let received: Arc<std::sync::Mutex<Vec<Vec<u8>>>> =
            Arc::new(std::sync::Mutex::new(Vec::new()));
        let fake_discovery_received = Arc::clone(&received);

        let fake_discovery = tokio::spawn(async move {
            let (mut connection, _) = listener.accept().await.unwrap();
            let mut buffer = [0u8; 64];

            loop {
                match connection.read(&mut buffer).await {
                    Ok(0) | Err(_) => return,
                    Ok(bytes_read) => {
                        fake_discovery_received
                            .lock()
                            .unwrap()
                            .push(buffer[..bytes_read].to_vec());

                        if connection.write_all(b"A\n").await.is_err() {
                            return;
                        }
                    }
                }
            }
        });

        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let heartbeat_task = tokio::spawn(send_heartbeats(
            HeartbeatConfig {
                discovery_addr,
                advertise_addr: "127.0.0.1:8356".to_string(),
                interval: Duration::from_millis(20),
                auth_secret: None,
                tls_connector: None,
            },
            shutdown_rx,
        ));

        sleep(Duration::from_millis(150)).await;

        shutdown_tx.send_replace(true);
        heartbeat_task.await.unwrap();
        fake_discovery.abort();

        let received = received.lock().unwrap();
        assert!(
            received.len() >= 3,
            "expected at least 3 heartbeats, got {}",
            received.len()
        );
        for message in received.iter() {
            assert_eq!(message, b"H 14\n127.0.0.1:8356");
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn send_heartbeats_retries_after_the_discovery_server_is_unreachable() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let discovery_addr = listener.local_addr().unwrap().to_string();
        // Close the listener immediately so the first connect attempt fails,
        // then bind a fresh listener on the same address for the retry.
        drop(listener);

        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let heartbeat_task = tokio::spawn(send_heartbeats(
            HeartbeatConfig {
                discovery_addr: discovery_addr.clone(),
                advertise_addr: "127.0.0.1:8356".to_string(),
                interval: Duration::from_millis(20),
                auth_secret: None,
                tls_connector: None,
            },
            shutdown_rx,
        ));

        sleep(Duration::from_millis(50)).await;

        let listener = TcpListener::bind(&discovery_addr).await.unwrap();
        let accepted = timeout(Duration::from_secs(2), listener.accept()).await;

        shutdown_tx.send_replace(true);
        heartbeat_task.await.unwrap();

        assert!(accepted.is_ok(), "expected a retried connection attempt");
    }

    /// Installs rustls's default crypto provider if nothing else has yet;
    /// safe to call from multiple tests since a second, redundant install is
    /// just ignored rather than treated as an error.
    fn ensure_crypto_provider() {
        let _ = rustls::crypto::ring::default_provider().install_default();
    }

    /// A self-signed cert/key pair valid for both "localhost" and
    /// "127.0.0.1", plus a matching acceptor/connector pair that trusts only
    /// that cert, for exercising the TLS accept and connect paths in tests
    /// without touching the filesystem.
    fn self_signed_tls() -> (TlsAcceptor, TlsConnector) {
        ensure_crypto_provider();

        let rcgen::CertifiedKey { cert, signing_key } = rcgen::generate_simple_self_signed(vec![
            "localhost".to_string(),
            "127.0.0.1".to_string(),
        ])
        .unwrap();
        let cert_der = cert.der().clone();
        let key_der = PrivateKeyDer::Pkcs8(signing_key.serialize_der().into());

        let server_config = ServerConfig::builder()
            .with_no_client_auth()
            .with_single_cert(vec![cert_der.clone()], key_der)
            .unwrap();
        let acceptor = TlsAcceptor::from(Arc::new(server_config));

        let mut roots = RootCertStore::empty();
        roots.add(cert_der).unwrap();
        let client_config = ClientConfig::builder()
            .with_root_certificates(roots)
            .with_no_client_auth();
        let connector = TlsConnector::from(Arc::new(client_config));

        (acceptor, connector)
    }

    #[tokio::test(flavor = "current_thread")]
    async fn dispatch_connection_serves_commands_over_tls() {
        let (acceptor, connector) = self_signed_tls();

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();

        let (request_tx, request_rx) = mpsc::channel(1);
        let cache_task = tokio::spawn(run_cache(request_rx));
        let connection_limit = Arc::new(Semaphore::new(1));
        let (_shutdown_tx, shutdown_rx) = watch::channel(false);

        let config = ConnectionConfig {
            idle_timeout: IDLE_TIMEOUT,
            auth_secret: None,
            tls_acceptor: Some(acceptor),
        };

        let server_task = tokio::spawn(async move {
            let (stream, peer_addr) = listener.accept().await.unwrap();
            let mut connection_tasks = JoinSet::new();

            dispatch_connection(
                stream,
                peer_addr,
                request_tx,
                connection_limit,
                config,
                shutdown_rx,
                &mut connection_tasks,
            )
            .await;

            while connection_tasks.join_next().await.is_some() {}
        });

        let tcp = TcpStream::connect(address).await.unwrap();
        let server_name = ServerName::try_from("localhost").unwrap();
        let mut tls = connector.connect(server_name, tcp).await.unwrap();

        tls.write_all(b"S 4 5\nnameAliceG 4\nname").await.unwrap();

        let expected = b"S\nV 5\nAlice";
        let mut response = vec![0_u8; expected.len()];
        tls.read_exact(&mut response).await.unwrap();
        assert_eq!(response, expected);

        server_task.await.unwrap();
        cache_task.await.unwrap();
    }

    #[tokio::test(flavor = "current_thread")]
    async fn send_heartbeats_sends_periodic_heartbeats_over_tls() {
        let (acceptor, connector) = self_signed_tls();

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let discovery_addr = listener.local_addr().unwrap().to_string();

        let received: Arc<std::sync::Mutex<Vec<Vec<u8>>>> =
            Arc::new(std::sync::Mutex::new(Vec::new()));
        let fake_discovery_received = Arc::clone(&received);

        let fake_discovery = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let mut tls = acceptor.accept(stream).await.unwrap();
            let mut buffer = [0u8; 64];

            loop {
                match tls.read(&mut buffer).await {
                    Ok(0) | Err(_) => return,
                    Ok(bytes_read) => {
                        fake_discovery_received
                            .lock()
                            .unwrap()
                            .push(buffer[..bytes_read].to_vec());

                        if tls.write_all(b"A\n").await.is_err() {
                            return;
                        }
                    }
                }
            }
        });

        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let heartbeat_task = tokio::spawn(send_heartbeats(
            HeartbeatConfig {
                discovery_addr,
                advertise_addr: "127.0.0.1:8356".to_string(),
                interval: Duration::from_millis(20),
                auth_secret: None,
                tls_connector: Some(connector),
            },
            shutdown_rx,
        ));

        sleep(Duration::from_millis(150)).await;

        shutdown_tx.send_replace(true);
        heartbeat_task.await.unwrap();
        fake_discovery.abort();

        let received = received.lock().unwrap();
        assert!(
            received.len() >= 3,
            "expected at least 3 heartbeats, got {}",
            received.len()
        );
        for message in received.iter() {
            assert_eq!(message, b"H 14\n127.0.0.1:8356");
        }
    }
}
