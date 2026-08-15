//! Standalone cluster-membership registry for nanocached cache nodes.
//!
//! This binary has no dependency on the cache server's own modules (see
//! `src/bin/bench.rs` for the same independence rule and rationale); its
//! protocol is unrelated to nanocached's cache protocol, so nothing is
//! shared. Run it via `ncd discovery start`, or directly as
//! `nanocached-discovery`.
//!
//! Protocol (ASCII header line, terminated by `\n`; a command may repeat
//! on the same connection):
//!
//!   H <addr-length>\n<addr>   Register or refresh a node's advertised
//!                             address. Idempotent: creates the entry if
//!                             absent, otherwise just refreshes its
//!                             liveness. Response: `A\n`.
//!
//!   L\n                       List currently live node addresses.
//!                             Response: `N <count>\n` followed by
//!                             `count` lines, each `<addr>\n`.
//!
//! If the connection limit has been reached, the server responds with
//! `B\n` and closes the connection instead of accepting the command.
//!
//! A node is expected to hold one long-lived connection and send `H` on it
//! periodically. A client SDK polls with `L`, typically on its own
//! connection. A node that stops sending heartbeats is dropped once
//! `--liveness-timeout` has elapsed since its last heartbeat; no explicit
//! "leave" message is required, so this covers both graceful shutdown and
//! crashes. Because the registry is fully rebuilt from heartbeats, this
//! process can be restarted at any time and self-heals within one
//! heartbeat interval.

use bytes::BytesMut;
use rustc_hash::FxHashMap;
use std::io;
use std::net::SocketAddr;
use std::process::ExitCode;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{Semaphore, watch};
use tokio::task::JoinSet;
use tokio::time::{Instant, interval, timeout};

const READ_CHUNK_SIZE: usize = 256;
const MAX_REQUEST_SIZE: usize = 4096;
const MAX_CONNECTIONS: usize = 1024;
const DEFAULT_LIVENESS_TIMEOUT: Duration = Duration::from_secs(15);
const IDLE_TIMEOUT: Duration = Duration::from_secs(30);
const SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(10);

type Registry = Arc<Mutex<FxHashMap<String, Instant>>>;

struct Args {
    host: String,
    port: u16,
    liveness_timeout: Duration,
}

impl Default for Args {
    fn default() -> Self {
        Self {
            host: "127.0.0.1".to_string(),
            port: 8357,
            liveness_timeout: DEFAULT_LIVENESS_TIMEOUT,
        }
    }
}

fn parse_args() -> Result<Args, String> {
    let mut args = Args::default();
    let mut raw = std::env::args().skip(1);

    while let Some(flag) = raw.next() {
        let mut value = || raw.next().ok_or_else(|| format!("{flag} requires a value"));

        match flag.as_str() {
            "--host" => args.host = value()?,
            "--port" => {
                let raw_port = value()?;
                args.port = raw_port
                    .parse()
                    .map_err(|_| format!("invalid value for --port: {raw_port}"))?;
            }
            "--liveness-timeout" => {
                let raw_secs = value()?;
                let secs: u64 = raw_secs
                    .parse()
                    .map_err(|_| format!("invalid value for --liveness-timeout: {raw_secs}"))?;
                args.liveness_timeout = Duration::from_secs(secs);
            }
            "-h" | "--help" => return Err(usage()),
            other => return Err(format!("unknown flag: {other}\n\n{}", usage())),
        }
    }

    Ok(args)
}

fn usage() -> String {
    "\
Usage: nanocached-discovery [options]

  --host <addr>                 bind address (default 127.0.0.1)
  --port <port>                 bind port (default 8357)
  --liveness-timeout <secs>     drop a node after this many seconds without
                                 a heartbeat (default 15)"
        .to_string()
}

#[derive(Debug, PartialEq, Eq)]
enum DiscoveryCommand {
    Heartbeat(String),
    List,
}

#[derive(Debug, PartialEq, Eq)]
enum ParseError {
    InvalidCommand,
    InvalidLength,
    EmptyAddress,
    InvalidAddress,
    Incomplete,
}

/// Parses one request from the front of `input`, removing the consumed
/// bytes via `BytesMut::split_to`. On `Incomplete`, `input` is left
/// untouched.
fn parse(input: &mut BytesMut) -> Result<DiscoveryCommand, ParseError> {
    let header_end = find_lf(&input[..]).ok_or(ParseError::Incomplete)?;
    let header = &input[..header_end];

    let mut parts = header.split(|byte| *byte == b' ');
    let command = parts.next().ok_or(ParseError::InvalidCommand)?;

    match command {
        b"L" => {
            if parts.next().is_some() {
                return Err(ParseError::InvalidLength);
            }

            let _ = input.split_to(header_end + 1);
            Ok(DiscoveryCommand::List)
        }

        b"H" => {
            let addr_length = parts.next().ok_or(ParseError::InvalidLength)?;

            if parts.next().is_some() {
                return Err(ParseError::InvalidLength);
            }

            let addr_length = parse_length(addr_length)?;

            if addr_length == 0 {
                return Err(ParseError::EmptyAddress);
            }

            let addr_start = header_end + 1;
            let addr_end = addr_start
                .checked_add(addr_length)
                .ok_or(ParseError::InvalidLength)?;

            if input.len() < addr_end {
                return Err(ParseError::Incomplete);
            }

            let frame = input.split_to(addr_end);
            let addr = String::from_utf8(frame[addr_start..addr_end].to_vec())
                .map_err(|_| ParseError::InvalidAddress)?;

            Ok(DiscoveryCommand::Heartbeat(addr))
        }

        _ => Err(ParseError::InvalidCommand),
    }
}

fn find_lf(input: &[u8]) -> Option<usize> {
    input.iter().position(|byte| *byte == b'\n')
}

fn parse_length(input: &[u8]) -> Result<usize, ParseError> {
    if input.is_empty() {
        return Err(ParseError::InvalidLength);
    }

    input.iter().try_fold(0usize, |length, byte| {
        if !byte.is_ascii_digit() {
            return Err(ParseError::InvalidLength);
        }

        length
            .checked_mul(10)
            .and_then(|length| length.checked_add((byte - b'0') as usize))
            .ok_or(ParseError::InvalidLength)
    })
}

fn lock(registry: &Registry) -> std::sync::MutexGuard<'_, FxHashMap<String, Instant>> {
    registry
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
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

async fn run(address: &str, liveness_timeout: Duration) -> io::Result<()> {
    let listener = TcpListener::bind(address).await?;
    let registry: Registry = Arc::new(Mutex::new(FxHashMap::default()));
    let connection_limit = Arc::new(Semaphore::new(MAX_CONNECTIONS));
    let mut connection_tasks = JoinSet::new();
    let (shutdown_tx, shutdown_rx) = watch::channel(false);

    let sweep_task = tokio::spawn(sweep_expired(
        Arc::clone(&registry),
        liveness_timeout,
        shutdown_rx.clone(),
    ));

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
                    Arc::clone(&registry),
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

    let _ = sweep_task.await;

    Ok(())
}

async fn dispatch_connection(
    mut stream: TcpStream,
    address: SocketAddr,
    registry: Registry,
    connection_limit: Arc<Semaphore>,
    idle_timeout: Duration,
    shutdown_rx: watch::Receiver<bool>,
    connection_tasks: &mut JoinSet<()>,
) -> bool {
    let _ = stream.set_nodelay(true);

    let permit = match connection_limit.try_acquire_owned() {
        Ok(permit) => permit,
        Err(_) => {
            if let Err(error) = stream.write_all(b"B\n").await {
                eprintln!("failed to send busy response to {address}: {error}");
            }

            return false;
        }
    };

    connection_tasks.spawn(async move {
        let _connection_permit = permit;

        if let Err(error) = handle_connection(stream, registry, idle_timeout, shutdown_rx).await {
            eprintln!("connection error from {address}: {error}");
        }
    });

    true
}

async fn sweep_expired(
    registry: Registry,
    liveness_timeout: Duration,
    mut shutdown_rx: watch::Receiver<bool>,
) {
    let sweep_interval = (liveness_timeout / 4)
        .min(Duration::from_secs(1))
        .max(Duration::from_millis(1));
    let mut ticker = interval(sweep_interval);

    loop {
        tokio::select! {
            _ = ticker.tick() => {
                let now = Instant::now();
                lock(&registry).retain(|_, last_seen| now.duration_since(*last_seen) < liveness_timeout);
            }
            _ = shutdown_rx.changed() => return,
        }
    }
}

async fn handle_connection(
    mut stream: TcpStream,
    registry: Registry,
    idle_timeout: Duration,
    mut shutdown_rx: watch::Receiver<bool>,
) -> io::Result<()> {
    let mut received = BytesMut::new();

    loop {
        if *shutdown_rx.borrow() {
            return Ok(());
        }

        match parse(&mut received) {
            Ok(DiscoveryCommand::Heartbeat(addr)) => {
                lock(&registry).insert(addr, Instant::now());
                stream.write_all(b"A\n").await?;
                continue;
            }
            Ok(DiscoveryCommand::List) => {
                let nodes: Vec<String> = lock(&registry).keys().cloned().collect();
                let mut response = format!("N {}\n", nodes.len());
                for node in &nodes {
                    response.push_str(node);
                    response.push('\n');
                }
                stream.write_all(response.as_bytes()).await?;
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
                    io::Error::new(io::ErrorKind::TimedOut, "connection idle timeout")
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

        if received.len() > MAX_REQUEST_SIZE {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "request is too large",
            ));
        }
    }
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> ExitCode {
    let args = match parse_args() {
        Ok(args) => args,
        Err(message) => {
            eprintln!("{message}");
            return ExitCode::FAILURE;
        }
    };

    let address = format!("{}:{}", args.host, args.port);
    if let Err(err) = run(&address, args.liveness_timeout).await {
        eprintln!("discovery: {err}");
        return ExitCode::FAILURE;
    }

    ExitCode::SUCCESS
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_reports_incomplete_before_the_header_is_fully_buffered() {
        let mut input = BytesMut::from(&b"H 5"[..]);
        assert_eq!(parse(&mut input), Err(ParseError::Incomplete));
    }

    #[test]
    fn parse_reports_incomplete_while_the_address_body_is_still_arriving() {
        let mut input = BytesMut::from(&b"H 9\n1.2.3"[..]);
        assert_eq!(parse(&mut input), Err(ParseError::Incomplete));
    }

    #[test]
    fn parse_reads_a_heartbeat_and_consumes_only_that_frame() {
        let mut input = BytesMut::from(&b"H 9\n127.0.0.1L\n"[..]);
        let command = parse(&mut input).unwrap();
        assert_eq!(
            command,
            DiscoveryCommand::Heartbeat("127.0.0.1".to_string())
        );
        assert_eq!(&input[..], b"L\n");
    }

    #[test]
    fn parse_reads_a_list_command() {
        let mut input = BytesMut::from(&b"L\n"[..]);
        assert_eq!(parse(&mut input), Ok(DiscoveryCommand::List));
        assert!(input.is_empty());
    }

    #[test]
    fn parse_rejects_list_with_trailing_arguments() {
        let mut input = BytesMut::from(&b"L extra\n"[..]);
        assert_eq!(parse(&mut input), Err(ParseError::InvalidLength));
    }

    #[test]
    fn parse_rejects_an_empty_address() {
        let mut input = BytesMut::from(&b"H 0\n"[..]);
        assert_eq!(parse(&mut input), Err(ParseError::EmptyAddress));
    }

    #[test]
    fn parse_rejects_a_non_numeric_length() {
        let mut input = BytesMut::from(&b"H x\n"[..]);
        assert_eq!(parse(&mut input), Err(ParseError::InvalidLength));
    }

    #[test]
    fn parse_rejects_invalid_utf8_addresses() {
        let mut input = BytesMut::from(&b"H 2\n\xff\xfe"[..]);
        assert_eq!(parse(&mut input), Err(ParseError::InvalidAddress));
    }

    #[test]
    fn parse_rejects_an_unknown_command() {
        let mut input = BytesMut::from(&b"X\n"[..]);
        assert_eq!(parse(&mut input), Err(ParseError::InvalidCommand));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn heartbeat_then_list_reports_the_registered_node() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let registry: Registry = Arc::new(Mutex::new(FxHashMap::default()));
        let (_shutdown_tx, shutdown_rx) = watch::channel(false);

        let server_registry = Arc::clone(&registry);
        tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let _ = handle_connection(stream, server_registry, Duration::from_secs(5), shutdown_rx)
                .await;
        });

        let mut client = TcpStream::connect(address).await.unwrap();
        client.write_all(b"H 14\n127.0.0.1:8356L\n").await.unwrap();

        // The two responses are written in separate calls but the client may
        // observe them coalesced into a single read, so accumulate until the
        // expected byte count has arrived instead of assuming read boundaries.
        let expected = b"A\nN 1\n127.0.0.1:8356\n";
        let mut received = Vec::new();
        let mut chunk = [0u8; 64];

        while received.len() < expected.len() {
            let bytes_read = client.read(&mut chunk).await.unwrap();
            assert!(bytes_read > 0, "connection closed before response arrived");
            received.extend_from_slice(&chunk[..bytes_read]);
        }

        assert_eq!(received, expected);
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

    #[tokio::test(flavor = "current_thread")]
    async fn rejects_connection_when_connection_limit_is_reached() {
        let connection_limit = Arc::new(Semaphore::new(1));
        let registry: Registry = Arc::new(Mutex::new(FxHashMap::default()));
        let (_shutdown_tx, shutdown_rx) = watch::channel(false);
        let mut connection_tasks = JoinSet::new();

        let (_first_client, first_server) = tcp_pair().await;
        let first_address = first_server.peer_addr().unwrap();

        let first_connection = dispatch_connection(
            first_server,
            first_address,
            Arc::clone(&registry),
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
            registry,
            connection_limit,
            IDLE_TIMEOUT,
            shutdown_rx,
            &mut connection_tasks,
        )
        .await;

        assert!(!second_connection);

        let mut response = [0u8; 2];
        second_client.read_exact(&mut response).await.unwrap();
        assert_eq!(&response, b"B\n");
    }

    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn sweep_expired_drops_nodes_past_the_liveness_timeout() {
        let registry: Registry = Arc::new(Mutex::new(FxHashMap::default()));
        lock(&registry).insert("127.0.0.1:8356".to_string(), Instant::now());

        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let sweep_task = tokio::spawn(sweep_expired(
            Arc::clone(&registry),
            Duration::from_secs(1),
            shutdown_rx,
        ));

        tokio::task::yield_now().await;
        tokio::time::advance(Duration::from_secs(2)).await;
        tokio::task::yield_now().await;

        assert!(lock(&registry).is_empty());

        shutdown_tx.send_replace(true);
        sweep_task.await.unwrap();
    }
}
