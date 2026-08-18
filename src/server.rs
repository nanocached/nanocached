use crate::cache::{Cache, SWEEP_BUDGET};
use crate::command::{Command, ParseError, parse};
use crate::hash_ring::HashRing;
use crate::response::Response;
use bytes::{Bytes, BytesMut};
use rustls::pki_types::{CertificateDer, PrivateKeyDer, ServerName};
use rustls::{ClientConfig, RootCertStore, ServerConfig};
use std::future::Future;
use std::io;
use std::io::BufReader;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use std::time::{Duration, Instant};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, ReadBuf};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{Semaphore, mpsc, oneshot, watch};
use tokio::task::JoinSet;
use tokio::time::{sleep, timeout};
use tokio_rustls::{TlsAcceptor, TlsConnector};
use uuid::Uuid;

const MAX_REQUEST_SIZE: usize = 1024 * 1024;
const MAX_CONNECTIONS: usize = 1024;
/// Default for `--max-memory` (issue #19): the cap was previously a fixed
/// constant with no way to tune it, even though the capacity planner
/// (`tools/capacity-planner.html`) already modeled capacity as a function
/// of it.
pub(crate) const MAX_CACHE_MEMORY_BYTES: usize = 256 * 1024 * 1024;
const IDLE_TIMEOUT: Duration = Duration::from_secs(60);
/// How long after this node's own handoff completes it keeps forwarding
/// concurrent writes to the joiner (issue #3) — matches discovery's
/// default --migration-timeout, by which time the join has either
/// completed cluster-wide (the joiner is in `L`, clients route to it
/// directly) or been abandoned (an `X` cleared the slot).
const FORWARDING_GRACE: Duration = Duration::from_secs(60);
/// How often the ADR-0008 active-deletion sweep runs. See `run_sweep`.
const SWEEP_INTERVAL: Duration = Duration::from_secs(5);
const SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(10);
const TLS_HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(10);
const READ_CHUNK_SIZE: usize = 1024;
/// How many times `run_migration` tries to transfer a single key to the
/// joining node (reconnecting between tries) before giving up on the
/// whole migration. See `run_migration`'s own doc comment.
const KEY_TRANSFER_ATTEMPTS: u32 = 3;

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

/// A `run_migration` invocation, boxed so `handle_connection` can hand it to
/// `run`'s own loop over `ConnectionConfig::migration_tx` instead of
/// spawning it directly — see that field's doc comment for why.
type MigrationTask = Pin<Box<dyn Future<Output = ()> + Send>>;

/// Per-connection settings that don't change once `run` starts, grouped so
/// `dispatch_connection`/`handle_connection` take one value instead of two.
#[derive(Clone)]
struct ConnectionConfig {
    idle_timeout: Duration,
    auth_secret: Option<Bytes>,
    /// When set, every accepted connection must complete a TLS handshake
    /// before speaking the cache protocol; there is no plaintext fallback.
    tls_acceptor: Option<TlsAcceptor>,
    /// Only present when this node is configured to register with a
    /// discovery server (ADR-0008/0009) — an `M` arriving otherwise has
    /// nowhere sensible to report `C` to and is rejected.
    node_context: Option<NodeContext>,
    /// Where `handle_connection` hands off a `run_migration` future for
    /// `run`'s own loop to `connection_tasks.spawn` — spawning it directly
    /// from inside a connection task (as opposed to from `run`) would leave
    /// it untracked by `connection_tasks`, so graceful shutdown couldn't
    /// wait for it (or ask it to unwind cleanly) before the process exits.
    migration_tx: mpsc::Sender<MigrationTask>,
}

/// What an ADR-0008 migration task (triggered by an incoming `M`) needs
/// beyond the cache itself: this node's own identity, and how to reach
/// the discovery server to report `C` once the handoff is done.
#[derive(Clone)]
struct NodeContext {
    /// This node's own random per-process identity (ADR-0009), needed to
    /// identify this node as the sender when it reports `C`.
    name: String,
    discovery_addr: String,
    /// Set while `run_migration` is active, cleared when it finishes (see
    /// `MigrationGuard`). Serves two purposes: `run_sweep` checks it and
    /// skips its pass while it's `Some`, per ADR-0008 — a marked-but-not-
    /// yet-swept key may still be needed as the authoritative source for
    /// a subsequent hop while a handoff is in flight — and an incoming
    /// `X` (cancel) uses it to find (by `joining_name`) and abort a
    /// matching in-flight handoff.
    active_migration: Arc<Mutex<Option<ActiveMigration>>>,
    /// This node's best current understanding of cluster membership, used
    /// to reject a client's `G`/`S`/`D` for a key this node no longer
    /// owns (see `wrong_node`) instead of silently serving stale local
    /// data forever to a client whose own view of `L` hasn't caught up
    /// yet. Updated once a handoff this node ran (successfully) finishes
    /// — not the moment `M` arrives — so this node keeps accepting
    /// writes for a key up through the handoff (propagating them, see
    /// `run_migration`) and only starts rejecting once its own share is
    /// actually done; updating any earlier would reject requests for a
    /// joining node that may not even be promoted yet. Runs for every
    /// `M`, whether or not any of this node's own keys route to the
    /// joiner, so it stays current with joins elsewhere in the cluster
    /// too. `None` until this node's first successful handoff — a lone
    /// or freshly-bootstrapped node has no membership to reject against
    /// yet.
    known_ring: KnownRing,
    auth_secret: Option<Bytes>,
    tls_connector: Option<TlsConnector>,
    request_tx: mpsc::Sender<CacheRequest>,
}

/// Configuration for registering this node with discovery servers (see
/// `src/bin/nanocached-discovery.rs`). When set, `run` asks to join once
/// (ADR-0008) using a random per-process name (ADR-0009) and, once
/// promoted, sends a heartbeat declaring that name on `interval`, well
/// under the discovery server's own liveness timeout.
pub(crate) struct HeartbeatConfig {
    /// One or more discovery replicas (ADR-0010). The first is the
    /// primary — the only one this node ever sends `J` (and `C`) to;
    /// the rest learn about this node via `P` announces once the primary
    /// has promoted it. Never empty (main.rs validates).
    pub(crate) discovery_addrs: Vec<String>,
    /// The port this node serves on. `J`/`P` carry only this; the
    /// discovery server derives the full address from the registration
    /// connection's source IP (ADR-0012), so there is nothing to
    /// configure in containerized deployments.
    pub(crate) port: u16,
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
    max_memory_bytes: usize,
) -> io::Result<()> {
    let listener = TcpListener::bind(address).await?;

    let (request_tx, request_rx) = mpsc::channel(1024);
    let connection_limit = Arc::new(Semaphore::new(MAX_CONNECTIONS));
    let mut connection_tasks = JoinSet::new();
    let (shutdown_tx, shutdown_rx) = watch::channel(false);

    let cache_task = tokio::spawn(run_cache(request_rx, max_memory_bytes));

    // Shared with `node_context` (when this node has one) so `run_sweep`
    // can tell whether an ADR-0008 handoff this node is the source for is
    // currently in flight, regardless of discovery configuration — a node
    // running standalone never touches this beyond the initial `None`.
    let active_migration: Arc<Mutex<Option<ActiveMigration>>> = Arc::new(Mutex::new(None));

    let sweep_task = tokio::spawn(run_sweep(
        request_tx.clone(),
        Arc::clone(&active_migration),
        shutdown_rx.clone(),
    ));

    // Shared with `node_context` so the heartbeat tasks can report the
    // replication factor half of this node's belief to discovery (issue
    // #30), the same belief `wrong_node` rejects client requests against.
    let known_ring: KnownRing = Arc::new(Mutex::new(None));

    // Generated once and kept for this process's lifetime (ADR-0009): a
    // restarted node has no data to reclaim its old identity for, so
    // there's nothing a stable name would preserve across a restart that
    // isn't already lost anyway. Only meaningful when this node registers
    // with a discovery server at all.
    let node_context = heartbeat.as_ref().map(|config| NodeContext {
        name: Uuid::new_v4().to_string(),
        // The primary (ADR-0010) — where `C` completion reports go,
        // matching where `J` was sent.
        discovery_addr: config.discovery_addrs[0].clone(),
        active_migration: Arc::clone(&active_migration),
        known_ring: Arc::clone(&known_ring),
        auth_secret: config.auth_secret.clone(),
        tls_connector: config.tls_connector.clone(),
        request_tx: request_tx.clone(),
    });

    let heartbeat_task = match (heartbeat, &node_context) {
        (Some(config), Some(node_context)) => Some(tokio::spawn(send_heartbeats(
            config,
            node_context.name.clone(),
            Arc::clone(&known_ring),
            shutdown_rx.clone(),
        ))),
        _ => None,
    };

    // Buffered rather than unbounded: ADR-0008 allows only one migration in
    // flight per node (see `NodeContext::active_migration`), so a handful of
    // slots is already more than a well-behaved cluster would ever need at
    // once.
    let (migration_tx, mut migration_rx) = mpsc::channel::<MigrationTask>(4);

    let connection_config = ConnectionConfig {
        idle_timeout: IDLE_TIMEOUT,
        auth_secret,
        tls_acceptor,
        node_context,
        migration_tx,
    };

    let shutdown = shutdown_signal();
    tokio::pin!(shutdown);

    loop {
        tokio::select! {
            biased;

            result = &mut shutdown => {
                result?;
                println!("INFO shutdown signal received");
                shutdown_tx.send_replace(true);

                // `run_migration` doesn't watch `shutdown_rx` itself (it's
                // driven entirely by `abort_requested`, the same flag an
                // incoming `X` uses) — ask it to unwind now rather than
                // letting `connection_tasks`'s drain-then-`abort_all`
                // below simply run out the clock on it. A raw task abort
                // wouldn't run `run_migration`'s own rollback of
                // `marked_this_run`, only `MigrationGuard::drop`'s slot
                // clear, so this in-band request is what lets the rest of
                // this shutdown path stay a normal bounded wait instead of
                // a forced kill.
                if let Some(migration) = active_migration
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner())
                    .as_ref()
                {
                    migration.abort_requested.store(true, Ordering::SeqCst);
                }

                break;
            }

            result = connection_tasks.join_next(), if !connection_tasks.is_empty() => {
                if let Some(Err(error)) = result {
                    eprintln!("WARN connection task failed: {error}");
                }
            }

            Some(task) = migration_rx.recv() => {
                connection_tasks.spawn(task);
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
                );
            }

        }
    }

    let connections_finished = timeout(SHUTDOWN_TIMEOUT, async {
        while let Some(result) = connection_tasks.join_next().await {
            if let Err(error) = result {
                eprintln!("WARN connection task failed: {error}");
            }
        }
    })
    .await;

    if connections_finished.is_err() {
        eprintln!("WARN shutdown timeout reached");
        connection_tasks.abort_all();

        while connection_tasks.join_next().await.is_some() {}
    }

    drop(request_tx);
    // `connection_config` (specifically the `NodeContext.request_tx` clone
    // inside it, when this node has a discovery config) is otherwise not
    // dropped until `run` itself returns — after `cache_task.await` below,
    // which needs every sender dropped to see its channel close. Without
    // this, a discovery-configured node deadlocks on shutdown: `cache_task`
    // waits on a sender only `run`'s own return would drop, and `run` can't
    // return until `cache_task` resolves.
    drop(connection_config);

    cache_task
        .await
        .map_err(|error| io::Error::other(format!("cache task failed: {error}")))?;

    sweep_task
        .await
        .map_err(|error| io::Error::other(format!("sweep task failed: {error}")))?;

    if let Some(heartbeat_task) = heartbeat_task {
        heartbeat_task
            .await
            .map_err(|error| io::Error::other(format!("heartbeat task failed: {error}")))?;
    }

    Ok(())
}

fn dispatch_connection(
    stream: TcpStream,
    address: SocketAddr,
    request_tx: mpsc::Sender<CacheRequest>,
    connection_limit: Arc<Semaphore>,
    config: ConnectionConfig,
    shutdown_rx: watch::Receiver<bool>,
    connection_tasks: &mut JoinSet<()>,
) {
    // Every request/response is small; without this, the kernel may delay
    // small writes waiting to coalesce with more data (Nagle's algorithm).
    let _ = stream.set_nodelay(true);

    // Everything below — the TLS handshake and the over-limit "Busy" reply —
    // runs inside the spawned task, never inline in `run`'s accept loop. A
    // client that stalls its handshake (or never reads its "Busy" reply) would
    // otherwise block `run`'s `select!`, freezing new-connection accepts,
    // shutdown detection, and task reaping for the whole server.
    connection_tasks.spawn(async move {
        let mut stream: ServerStream = match &config.tls_acceptor {
            Some(acceptor) => match timeout(TLS_HANDSHAKE_TIMEOUT, acceptor.accept(stream)).await {
                Ok(Ok(tls_stream)) => ServerStream::Tls(Box::new(tls_stream)),
                Ok(Err(error)) => {
                    eprintln!("WARN TLS handshake with {address} failed: {error}");
                    return;
                }
                Err(_) => {
                    eprintln!("WARN TLS handshake with {address} timed out");
                    return;
                }
            },
            None => ServerStream::Plain(stream),
        };

        let permit = match connection_limit.try_acquire_owned() {
            Ok(permit) => permit,
            Err(_) => {
                let busy = Response::Busy.encode();

                // Bound the write: a peer that never reads must not leak this
                // task by leaving the write pending indefinitely.
                match timeout(TLS_HANDSHAKE_TIMEOUT, stream.write_all(&busy)).await {
                    Ok(Ok(())) => {}
                    Ok(Err(error)) => {
                        eprintln!("WARN failed to send busy response to {address}: {error}");
                    }
                    Err(_) => {
                        eprintln!("WARN sending busy response to {address} timed out");
                    }
                }

                return;
            }
        };

        println!("INFO accepted connection from {address}");

        let _connection_permit = permit;

        if let Err(error) = handle_connection(stream, request_tx, config, shutdown_rx).await {
            eprintln!("WARN connection error from {address}: {error}");
        }
    });
}

async fn execute_command(
    request_tx: &mpsc::Sender<CacheRequest>,
    command: Command,
) -> io::Result<Response> {
    let (response_tx, response_rx) = oneshot::channel();

    request_tx
        .send(CacheRequest {
            command,
            response_tx,
        })
        .await
        .map_err(|_| io::Error::other("cache task stopped"))?;

    response_rx
        .await
        .map_err(|_| io::Error::other("cache task dropped response"))
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
            Ok(Command::Migrate {
                joining_name,
                joining_addr,
                joined,
                replication,
            }) => {
                let Some(node_context) = config.node_context.clone() else {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "received M but this node isn't configured with a discovery server",
                    ));
                };

                stream
                    .write_all(&Response::MigrationAccepted.encode())
                    .await?;

                // Handed to `run`'s own loop rather than spawned here
                // directly, so it ends up tracked by `connection_tasks` —
                // see `ConnectionConfig::migration_tx`. If the receiving
                // end is already gone (`run` has moved past its select
                // loop into its own shutdown drain), there's nothing left
                // to hand this to; the migration is simply not started,
                // matching how a same-timing `M` arriving a moment later
                // would find the listener already closed.
                let _ = config
                    .migration_tx
                    .send(Box::pin(run_migration(
                        node_context,
                        joining_name,
                        joining_addr,
                        joined,
                        replication,
                    )))
                    .await;

                continue;
            }
            Ok(Command::CancelMigration { joining_name }) => {
                let Some(node_context) = config.node_context.clone() else {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "received X but this node isn't configured with a discovery server",
                    ));
                };

                // A safe no-op if there's no active migration, or it's for
                // a different `joining_name` (already finished, or this
                // cancel arrived late) — `run_migration` alone decides
                // whether to actually stop.
                {
                    let mut slot = node_context
                        .active_migration
                        .lock()
                        .unwrap_or_else(|poisoned| poisoned.into_inner());
                    if let Some(active) = slot.as_ref()
                        && active.joining_name == joining_name
                    {
                        active.abort_requested.store(true, Ordering::SeqCst);
                        // A completed entry only lingers to forward writes
                        // (issue #3); the join being abandoned ends that.
                        if active.completed_at.is_some() {
                            *slot = None;
                        }
                    }
                }

                stream
                    .write_all(&Response::MigrationCancelled.encode())
                    .await?;

                continue;
            }
            Ok(Command::Get { key }) => {
                if let Some(node_context) = &config.node_context
                    && wrong_node(node_context, &key)
                {
                    stream.write_all(&Response::WrongNode.encode()).await?;
                    continue;
                }

                let response = execute_command(&request_tx, Command::Get { key }).await?;
                stream.write_all(&response.encode()).await?;

                continue;
            }
            Ok(Command::Set { key, value, ttl }) => {
                if let Some(node_context) = &config.node_context
                    && wrong_node(node_context, &key)
                {
                    stream.write_all(&Response::WrongNode.encode()).await?;
                    continue;
                }

                let response = execute_command(
                    &request_tx,
                    Command::Set {
                        key: key.clone(),
                        value: value.clone(),
                        ttl,
                    },
                )
                .await?;
                stream.write_all(&response.encode()).await?;

                // ADR-0008: this key may be one an in-progress handoff is
                // moving to a joining node — see `migration_target_for`.
                if let Some(node_context) = &config.node_context
                    && let Some(joining_addr) = migration_target_for(node_context, &key)
                    && let Err(error) =
                        set_on_joining_node(node_context, &joining_addr, &key, &value, ttl).await
                {
                    eprintln!(
                        "WARN failed to forward a concurrent SET for a migrating key to \
                         {joining_addr}: {error}"
                    );
                }

                continue;
            }
            Ok(Command::Delete { key }) => {
                if let Some(node_context) = &config.node_context
                    && wrong_node(node_context, &key)
                {
                    stream.write_all(&Response::WrongNode.encode()).await?;
                    continue;
                }

                let response =
                    execute_command(&request_tx, Command::Delete { key: key.clone() }).await?;
                stream.write_all(&response.encode()).await?;

                if let Some(node_context) = &config.node_context
                    && let Some(joining_addr) = migration_target_for(node_context, &key)
                    && let Err(error) =
                        delete_on_joining_node(node_context, &joining_addr, &key).await
                {
                    eprintln!(
                        "WARN failed to forward a concurrent DELETE for a migrating key to \
                         {joining_addr}: {error}"
                    );
                }

                continue;
            }
            Ok(command) => {
                // `ListEntries`/`MarkMigrated`/`UnmarkMigrated`/`Sweep`/
                // `PeekEntry`: internal-only, constructed directly by
                // server-side tasks, never by `parse()` — this arm exists
                // only so the match stays exhaustive.
                let response = execute_command(&request_tx, command).await?;
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

async fn run_cache(mut request_rx: mpsc::Receiver<CacheRequest>, max_memory_bytes: usize) {
    let mut cache = Cache::new(max_memory_bytes);

    while let Some(request) = request_rx.recv().await {
        let response = request.command.execute(&mut cache);

        let _ = request.response_tx.send(response);
    }
}

/// ADR-0008: sent once per connection, before any heartbeat. `name` is
/// this node's random per-process identity (ADR-0009); `port` is where it
/// serves — the discovery server composes the reachable address from this
/// connection's own source IP plus that port (ADR-0012). Discovery holds
/// this connection open and pushes `R\n` on it once this node is promoted
/// to `Joined`.
fn join_message(name: &str, port: u16) -> Vec<u8> {
    let mut message = format!("J {} {port}\n", name.len()).into_bytes();
    message.extend_from_slice(name.as_bytes());
    message
}

/// ADR-0010: same shape as `join_message`, but declares an
/// already-promoted member — no handoff orchestration on the other end.
fn announce_message(name: &str, port: u16) -> Vec<u8> {
    let mut message = format!("P {} {port}\n", name.len()).into_bytes();
    message.extend_from_slice(name.as_bytes());
    message
}

/// Only valid once this node has been promoted to `Joined` (ADR-0008); the
/// address was already established by `join_message` on this connection,
/// so a heartbeat only needs to carry `name` to refresh liveness.
/// `replication` is this node's current belief about the cluster's
/// replication factor (issue #30), encoded as `0` when it doesn't have
/// one yet — never a real replication factor (discovery validates every
/// `--replication-factor` is at least 1), so it's an unambiguous "unknown"
/// sentinel on the wire.
fn heartbeat_message(name: &str, replication: Option<usize>) -> Vec<u8> {
    let mut message = format!("H {} {}\n", name.len(), replication.unwrap_or(0)).into_bytes();
    message.extend_from_slice(name.as_bytes());
    message
}

fn auth_message(secret: &[u8]) -> Vec<u8> {
    let mut message = format!("A {}\n", secret.len()).into_bytes();
    message.extend_from_slice(secret);
    message
}

/// Connects out to `addr` as a client — either the discovery server (for
/// heartbeats, ADR-0008's `J`/`C`) or another node (for ADR-0008's
/// `SET`-based handoff) — upgrading to TLS first if `tls_connector` is
/// set. There is no plaintext fallback: if TLS is configured and the
/// handshake fails, the connection attempt fails too.
async fn connect_client_stream(
    addr: &str,
    tls_connector: Option<&TlsConnector>,
) -> io::Result<ClientStream> {
    let stream = TcpStream::connect(addr).await?;
    let _ = stream.set_nodelay(true);

    match tls_connector {
        Some(connector) => {
            let server_name = server_name_from_addr(addr)?;
            let tls_stream = timeout(
                TLS_HANDSHAKE_TIMEOUT,
                connector.connect(server_name, stream),
            )
            .await
            .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "TLS handshake timed out"))??;

            Ok(ClientStream::Tls(Box::new(tls_stream)))
        }
        None => Ok(ClientStream::Plain(stream)),
    }
}

/// This registration task's relationship to one discovery replica
/// (ADR-0010). The primary is the only one ever sent `J` — the ADR-0008
/// join, with its data handoff — and flips the shared `promoted` flag
/// once `R` arrives for it. Standbys (and the primary itself, on any
/// re-registration after that first promotion) send `P` announces, which
/// upsert this node as a member with no handoff.
enum DiscoveryRole {
    Primary(Arc<watch::Sender<bool>>),
    Standby(watch::Receiver<bool>),
}

/// Registers this node with every configured discovery replica
/// (ADR-0010): one `register_with_discovery` task per address, sharing a
/// `promoted` watch so standbys hold off announcing until the primary's
/// ADR-0008 join has actually completed. `name` is this node's own
/// ADR-0009 identity, generated once by `run` and shared with
/// `ConnectionConfig`'s `NodeContext` — not generated here, so a
/// migration task triggered by an incoming `M` on some other connection
/// reports `C` under the same name these tasks register as.
async fn send_heartbeats(
    config: HeartbeatConfig,
    name: String,
    known_ring: KnownRing,
    shutdown_rx: watch::Receiver<bool>,
) {
    let (promoted_tx, promoted_rx) = watch::channel(false);
    let promoted_tx = Arc::new(promoted_tx);

    let mut tasks = JoinSet::new();
    for (index, discovery_addr) in config.discovery_addrs.iter().enumerate() {
        let role = if index == 0 {
            DiscoveryRole::Primary(Arc::clone(&promoted_tx))
        } else {
            DiscoveryRole::Standby(promoted_rx.clone())
        };

        tasks.spawn(register_with_discovery(
            discovery_addr.clone(),
            config.port,
            config.interval,
            config.auth_secret.clone(),
            config.tls_connector.clone(),
            name.clone(),
            Arc::clone(&known_ring),
            role,
            shutdown_rx.clone(),
        ));
    }

    while tasks.join_next().await.is_some() {}
}

/// Holds one long-lived connection to a single discovery replica:
/// registers (`J` for the primary's first time, `P` otherwise — see
/// `DiscoveryRole`), then sends a heartbeat on it every `interval`,
/// reconnecting (and re-registering under the same name) on any I/O
/// error after waiting out the interval.
#[allow(clippy::too_many_arguments)]
async fn register_with_discovery(
    discovery_addr: String,
    port: u16,
    interval: Duration,
    auth_secret: Option<Bytes>,
    tls_connector: Option<TlsConnector>,
    name: String,
    known_ring: KnownRing,
    mut role: DiscoveryRole,
    mut shutdown_rx: watch::Receiver<bool>,
) {
    let join = join_message(&name, port);
    let announce = announce_message(&name, port);

    // A standby must not announce a node the primary hasn't promoted yet:
    // that would make it visible in the standby's `L` before its ADR-0008
    // handoff has run, which is exactly the state `J`'s staging exists to
    // prevent.
    if let DiscoveryRole::Standby(promoted_rx) = &mut role {
        while !*promoted_rx.borrow() {
            tokio::select! {
                _ = shutdown_rx.changed() => return,
                result = promoted_rx.changed() => {
                    if result.is_err() {
                        return;
                    }
                }
            }
        }
    }

    loop {
        if *shutdown_rx.borrow() {
            return;
        }

        match connect_client_stream(&discovery_addr, tls_connector.as_ref()).await {
            Ok(mut stream) => {
                let authenticated = match &auth_secret {
                    Some(secret) => {
                        let auth = auth_message(secret);
                        match stream.write_all(&auth).await {
                            Ok(()) => {
                                let mut ack = [0u8; 3];
                                stream.read_exact(&mut ack).await.is_ok() && &ack == b"Od\n"
                            }
                            Err(_) => false,
                        }
                    }
                    None => true,
                };

                if !authenticated {
                    eprintln!("WARN discovery server at {discovery_addr} rejected the auth secret");
                }

                let sending_join = matches!(
                    &role,
                    DiscoveryRole::Primary(promoted_tx) if !*promoted_tx.borrow()
                );
                let registration = if sending_join { &join } else { &announce };

                if authenticated && stream.write_all(registration).await.is_ok() {
                    // For `J`, ADR-0008: this connection is held open by
                    // discovery (no idle timeout applies) until this node
                    // is promoted, which may take an unbounded amount of
                    // time if another join is already in progress. For
                    // `P`, the same `R\n` comes back immediately.
                    let mut promoted = [0u8; 2];
                    let read_promoted = tokio::select! {
                        _ = shutdown_rx.changed() => return,
                        result = stream.read_exact(&mut promoted) => result,
                    };

                    if read_promoted.is_ok() && &promoted == b"R\n" {
                        if sending_join {
                            println!("INFO joined the cluster via discovery at {discovery_addr}");
                        } else {
                            println!("INFO re-registered with discovery at {discovery_addr}");
                        }

                        if let DiscoveryRole::Primary(promoted_tx) = &role {
                            promoted_tx.send_replace(true);
                        }

                        loop {
                            // Rebuilt every tick, not precomputed once:
                            // this node's belief starts unknown and is set
                            // only once it has sent its own first ADR-0011
                            // handoff `M` (issue #30) — a stale precomputed
                            // buffer would keep reporting "unknown" forever.
                            let replication = known_ring
                                .lock()
                                .unwrap_or_else(|poisoned| poisoned.into_inner())
                                .as_ref()
                                .map(|membership| membership.replication);
                            let heartbeat = heartbeat_message(&name, replication);

                            if stream.write_all(&heartbeat).await.is_err() {
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

                            if wait_or_shutdown(interval, &mut shutdown_rx).await {
                                return;
                            }
                        }
                    }
                }
            }
            Err(error) => {
                eprintln!(
                    "WARN failed to connect to discovery server at {discovery_addr}: {error}"
                );
            }
        }

        if wait_or_shutdown(interval, &mut shutdown_rx).await {
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

fn set_message(key: &[u8], value: &[u8], ttl: Option<Duration>) -> Vec<u8> {
    let mut header = format!("S {} {}", key.len(), value.len());

    if let Some(ttl) = ttl {
        header.push_str(&format!(" {}", ttl.as_secs()));
    }

    header.push('\n');

    let mut message = header.into_bytes();
    message.extend_from_slice(key);
    message.extend_from_slice(value);
    message
}

/// ADR-0008: propagates a client's `D` for a key an in-progress handoff
/// is moving to the joining node too (see `forward_delete_to_joining_node`).
fn delete_message(key: &[u8]) -> Vec<u8> {
    let mut message = format!("D {}\n", key.len()).into_bytes();
    message.extend_from_slice(key);
    message
}

/// ADR-0008: reports to discovery that this node (identified by `name`,
/// ADR-0009) has finished handing off its share of the current join.
/// `C <name-len> <joining-len>\n<name><joining>` — the completion report
/// names both the reporting node and the join it is for (issue #5): a
/// bare name let a stale report from an abandoned handoff be credited to
/// whatever join happened to be pending next.
fn complete_message(name: &str, joining_name: &str) -> Vec<u8> {
    let mut message = format!("C {} {}\n", name.len(), joining_name.len()).into_bytes();
    message.extend_from_slice(name.as_bytes());
    message.extend_from_slice(joining_name.as_bytes());
    message
}

/// This node's view of cluster membership plus the replication factor it
/// came with (ADR-0011) — the two always travel together, since "is this
/// key mine to serve" is a top-R question and R arrived on the same `M`
/// that carried the roster.
struct Membership {
    ring: Arc<HashRing>,
    replication: usize,
}

/// Shared handle to a node's current `Membership` belief, if it has one
/// yet (see `NodeContext::known_ring`) — also handed to the heartbeat
/// tasks so they can report the replication factor half of it to
/// discovery (issue #30).
type KnownRing = Arc<Mutex<Option<Arc<Membership>>>>;

/// A `run_migration` in flight: which handoff it's for, where the joining
/// node is, the ring this handoff computed (so a concurrent client write
/// on another connection can tell whether *its* key is one this handoff
/// is moving — see `handle_connection`'s forwarding of `S`/`D`), and the
/// flag an incoming `X` (cancel) sets to ask it to stop. See
/// `NodeContext::active_migration`.
struct ActiveMigration {
    joining_name: String,
    joining_addr: String,
    after_ring: Arc<HashRing>,
    /// ADR-0011: discovery's replication factor, carried by the `M` that
    /// started this handoff — membership in the joining node's copy set
    /// is "in the key's top-R", not "is the key's owner".
    replication: usize,
    /// `None` while this node's own transfer is running; `Some(when)`
    /// once it finished successfully. Issue #3: discovery only publishes
    /// the joiner after EVERY ready node reports `C`, so this node must
    /// keep forwarding concurrent writes to the joiner after its own
    /// share is done — a still-stale client that can't see the joiner in
    /// `L` yet would otherwise write to this node without the joiner ever
    /// learning of it. The window is bounded by `FORWARDING_GRACE`
    /// (matching discovery's migration timeout: past it the join has
    /// either completed cluster-wide or been abandoned).
    completed_at: Option<Instant>,
    abort_requested: Arc<AtomicBool>,
}

/// Occupies `slot` with an `ActiveMigration` for this guard's lifetime
/// (cleared back to `None` on drop — including an early return or panic),
/// so `run_sweep` can tell to pause, an incoming `X` can find this
/// handoff to cancel it, and a concurrent client write can find it to
/// forward. Exposes `abort_requested` so `run_migration` can poll it
/// directly without re-locking `slot` on every entry.
struct MigrationGuard<'a> {
    slot: &'a Mutex<Option<ActiveMigration>>,
    abort_requested: Arc<AtomicBool>,
}

impl<'a> MigrationGuard<'a> {
    fn new(
        slot: &'a Mutex<Option<ActiveMigration>>,
        joining_name: String,
        joining_addr: String,
        after_ring: Arc<HashRing>,
        replication: usize,
    ) -> Self {
        let abort_requested = Arc::new(AtomicBool::new(false));

        *slot.lock().unwrap_or_else(|poisoned| poisoned.into_inner()) = Some(ActiveMigration {
            joining_name,
            joining_addr,
            after_ring,
            replication,
            completed_at: None,
            abort_requested: Arc::clone(&abort_requested),
        });

        Self {
            slot,
            abort_requested,
        }
    }

    /// Consumes the guard after a successful transfer: instead of
    /// clearing the slot (which would close the write-forwarding window
    /// the moment THIS node finishes — issue #3), it stamps the
    /// completion time so `migration_target_for` keeps forwarding until
    /// `FORWARDING_GRACE` passes or the slot is replaced/cancelled.
    fn completed(self) {
        if let Some(active) = self
            .slot
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .as_mut()
        {
            active.completed_at = Some(Instant::now());
        }
        std::mem::forget(self);
    }
}

impl Drop for MigrationGuard<'_> {
    fn drop(&mut self) {
        *self
            .slot
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = None;
    }
}

/// ADR-0008 (generalized by ADR-0011): triggered by an incoming `M`.
/// Computes, using the same rendezvous-hash algorithm clients use
/// (`HashRing` here is `src/hash_ring.rs`'s copy, see ADR-0006), how each
/// of this node's own entries' top-R owner set changes when the joining
/// node is added. Adding exactly one node can only insert it into a key's
/// ranking, never reorder the existing nodes relative to each other, so
/// per affected key exactly two roles exist among the pre-join owners:
/// the old *primary* sends the joining node its copy (one designated
/// sender — no duplicate transfers), and the node displaced from rank R
/// to R+1 (if any — there is at most one) marks its now-dead copy for
/// the post-handoff sweep. This node may hold either role, both (R=1:
/// sender and displaced coincide, which is exactly the pre-ADR-0011
/// behavior), or neither. There's no need to compare more than a pre-join
/// ring. Transfers each such entry via an ordinary `SET` (reusing the
/// client protocol, not a new one), marks it migrated, and once done
/// reports `C` to discovery.
///
/// All transfers for one migration share a single connection to the
/// joining node instead of opening (and tearing down) one per key —
/// with tens of thousands of keys, one-connection-per-key was observed
/// to exhaust the ephemeral port range mid-migration, at which point
/// every further key was silently skipped while the migration still
/// went on to flip `known_ring` and report completion, quietly losing
/// whatever didn't make it across. A key whose transfer fails is retried
/// up to `KEY_TRANSFER_ATTEMPTS` times, reconnecting each time (the
/// connection's state after a failed write/read is unknown, so it isn't
/// reused as-is). If a key still can't be transferred after that, this
/// gives up on the whole migration: it rolls back every mark from this
/// run (same as the `abort_requested` path below) and returns without
/// flipping `known_ring` or reporting `C`, leaving discovery's own
/// `--migration-timeout` to reap the stalled join rather than have this
/// node claim success over a joining node that's missing data. A lost
/// discovery connection for the final `C` is a separate, already-
/// terminal failure (the transfer itself succeeded) and is just logged.
async fn run_migration(
    node_context: NodeContext,
    joining_name: String,
    joining_addr: String,
    joined: Vec<(String, String)>,
    replication: usize,
) {
    let mut before_members: Vec<String> = joined.into_iter().map(|(name, _)| name).collect();
    // Discovery always lists this node in the roster (it only sends `M` to
    // `Joined` members), but the sender/displaced computations below are
    // meaningless without self in the "before" set — make it structural
    // rather than trusted.
    if !before_members.iter().any(|name| name == &node_context.name) {
        before_members.push(node_context.name.clone());
    }
    let mut after_members = before_members.clone();
    after_members.push(joining_name.clone());

    let before_ring = HashRing::new(before_members);
    let after_ring = Arc::new(HashRing::new(after_members));

    let migration_guard = MigrationGuard::new(
        &node_context.active_migration,
        joining_name.clone(),
        joining_addr.clone(),
        Arc::clone(&after_ring),
        replication,
    );

    println!("INFO migration started: handoff to {joining_name} at {joining_addr}");

    let entries = match list_entries(&node_context.request_tx).await {
        Some(entries) => entries,
        None => {
            eprintln!("WARN migration to {joining_name} aborted: cache task is unavailable");
            return;
        }
    };

    let mut marked_this_run = Vec::new();
    let mut sent_count = 0usize;
    let mut stream: Option<ClientStream> = None;

    let self_name = node_context.name.as_str();

    for (key, _, _) in entries {
        if migration_guard.abort_requested.load(Ordering::SeqCst) {
            break;
        }

        // A key is affected only if the joiner cracks its top-R (HRW
        // insertion can't change the set any other way).
        if !after_ring.is_owner(&key, &joining_name, replication) {
            continue;
        }

        let old_owners = before_ring.owners(&key, replication);
        // ADR-0011: the old primary is the one designated sender.
        let sends = old_owners.first() == Some(&self_name);
        // The (at most one) node the joiner displaced from rank R: its
        // copy is dead once the join completes — mark it for the sweep,
        // whether or not this node also happens to be the sender.
        let displaced =
            old_owners.contains(&self_name) && !after_ring.is_owner(&key, self_name, replication);

        if !sends {
            if displaced {
                mark_migrated(&node_context.request_tx, &key).await;
                marked_this_run.push(key);
            }
            continue;
        }

        // Re-checked live rather than trusting `entries()`'s snapshot: a
        // concurrent client write racing this key's turn (see
        // `handle_connection`'s own forwarding of `S`/`D` for a key this
        // migration is moving) must win over whatever was true when the
        // snapshot was taken, or its update would ship stale to the
        // joining node. If the key is gone by now (deleted, expired, or
        // already forwarded-and-since-removed), there's nothing to send —
        // `handle_connection`'s own delete-forwarding path (or nothing
        // ever existing to send in the first place) already covers it.
        let Some((_, value, ttl)) = peek_entry(&node_context.request_tx, &key).await else {
            continue;
        };

        let mut sent = false;

        for attempt in 1..=KEY_TRANSFER_ATTEMPTS {
            if stream.is_none() {
                match connect_and_authenticate(&node_context, &joining_addr).await {
                    Ok(connected) => stream = Some(connected),
                    Err(error) => {
                        eprintln!(
                            "WARN migration to {joining_addr} failed to connect \
                             (attempt {attempt}/{KEY_TRANSFER_ATTEMPTS}): {error}"
                        );
                        continue;
                    }
                }
            }

            let active_stream = match stream.as_mut() {
                Some(active_stream) => active_stream,
                None => continue,
            };

            match send_set(active_stream, &key, &value, ttl).await {
                Ok(()) => {
                    sent = true;
                    break;
                }
                Err(error) => {
                    eprintln!(
                        "WARN migration to {joining_addr} failed to transfer a key \
                         (attempt {attempt}/{KEY_TRANSFER_ATTEMPTS}): {error}"
                    );
                    // The connection's state after a failed write/read is
                    // unknown (e.g. a partial write) — reconnect rather
                    // than risk a desynced stream on the next attempt.
                    stream = None;
                }
            }
        }

        if !sent {
            eprintln!(
                "WARN migration to {joining_addr} permanently failed to transfer a key after \
                 {KEY_TRANSFER_ATTEMPTS} attempts; abandoning the join for discovery's \
                 migration-timeout to reap"
            );

            for key in marked_this_run {
                unmark_migrated(&node_context.request_tx, &key).await;
            }

            return;
        }

        sent_count += 1;

        // A sender that stays in the key's top-R keeps its copy (it's
        // still a live replica, ADR-0011); only a displaced copy is dead.
        if displaced {
            mark_migrated(&node_context.request_tx, &key).await;
            marked_this_run.push(key);
        }
    }

    if migration_guard.abort_requested.load(Ordering::SeqCst) {
        for key in marked_this_run {
            unmark_migrated(&node_context.request_tx, &key).await;
        }

        eprintln!("WARN migration to {joining_addr} cancelled by discovery; rolled back its marks");

        return;
    }

    // From here on, this node considers the post-join top-R authoritative
    // for every key — see `NodeContext::known_ring`.
    *node_context
        .known_ring
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner()) = Some(Arc::new(Membership {
        ring: after_ring,
        replication,
    }));

    println!(
        "INFO migration completed: {joining_name} (sent {sent_count} keys, marked {} dead \
         copies; forwarding writes for {}s)",
        marked_this_run.len(),
        FORWARDING_GRACE.as_secs()
    );

    if let Err(error) = report_complete(&node_context, &joining_name).await {
        eprintln!(
            "WARN migration to {joining_addr} finished but reporting completion to {} failed: {error}",
            node_context.discovery_addr
        );
    }

    // Keep the write-forwarding window open past this node's own share
    // (issue #3) — see `MigrationGuard::completed`.
    migration_guard.completed();
}

async fn list_entries(
    request_tx: &mpsc::Sender<CacheRequest>,
) -> Option<Vec<(Bytes, Bytes, Option<Duration>)>> {
    let (response_tx, response_rx) = oneshot::channel();

    request_tx
        .send(CacheRequest {
            command: Command::ListEntries,
            response_tx,
        })
        .await
        .ok()?;

    match response_rx.await.ok()? {
        Response::Entries(entries) => Some(entries),
        _ => None,
    }
}

async fn peek_entry(
    request_tx: &mpsc::Sender<CacheRequest>,
    key: &Bytes,
) -> Option<(Bytes, Bytes, Option<Duration>)> {
    let (response_tx, response_rx) = oneshot::channel();

    request_tx
        .send(CacheRequest {
            command: Command::PeekEntry { key: key.clone() },
            response_tx,
        })
        .await
        .ok()?;

    match response_rx.await.ok()? {
        Response::Entries(mut entries) => entries.pop(),
        _ => None,
    }
}

async fn mark_migrated(request_tx: &mpsc::Sender<CacheRequest>, key: &Bytes) {
    let (response_tx, response_rx) = oneshot::channel();

    if request_tx
        .send(CacheRequest {
            command: Command::MarkMigrated { key: key.clone() },
            response_tx,
        })
        .await
        .is_ok()
    {
        let _ = response_rx.await;
    }
}

async fn unmark_migrated(request_tx: &mpsc::Sender<CacheRequest>, key: &Bytes) {
    let (response_tx, response_rx) = oneshot::channel();

    if request_tx
        .send(CacheRequest {
            command: Command::UnmarkMigrated { key: key.clone() },
            response_tx,
        })
        .await
        .is_ok()
    {
        let _ = response_rx.await;
    }
}

/// ADR-0008's active-deletion background task: every `SWEEP_INTERVAL`,
/// reclaims migrated-marked and TTL-expired entries. `Cache::sweep`
/// removes at most `SWEEP_BUDGET` entries per call so it can't stall other
/// cache commands queued behind it on the single-threaded actor for long
/// (measured: ~500ns per removal, so a full-cache sweep over a backlog of
/// hundreds of thousands of entries would otherwise block for 100ms+) — a
/// call that hits that cap likely left more behind, so this loops back
/// immediately (no `SWEEP_INTERVAL` wait) until a call reports less than a
/// full budget removed, meaning the backlog is drained for now. Skips
/// (and doesn't drain a backlog) while `active_migration` is `Some` — this
/// node is the source for an in-progress handoff, so a marked-but-not-yet-
/// swept key may still be needed as the authoritative source for a
/// subsequent hop (see `NodeContext::active_migration`). Also watches
/// `shutdown_rx` while draining a backlog, so a large backlog can't delay
/// this task from dropping its `request_tx` clone past `SHUTDOWN_TIMEOUT`
/// — without it, `run`'s shutdown drain would just be waiting on this task
/// to notice on its own, one `SWEEP_BUDGET`-sized chunk at a time.
async fn run_sweep(
    request_tx: mpsc::Sender<CacheRequest>,
    active_migration: Arc<Mutex<Option<ActiveMigration>>>,
    mut shutdown_rx: watch::Receiver<bool>,
) {
    loop {
        if wait_or_shutdown(SWEEP_INTERVAL, &mut shutdown_rx).await {
            return;
        }

        loop {
            // Pause only while a transfer is actually running: a
            // completed entry lingering for its forwarding grace
            // (issue #3) must not stall TTL/mark sweeping for a minute.
            let migration_active = active_migration
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .as_ref()
                .is_some_and(|active| active.completed_at.is_none());

            if migration_active {
                break;
            }

            tokio::select! {
                result = sweep(&request_tx) => match result {
                    Some(removed) if removed >= SWEEP_BUDGET => continue,
                    _ => break,
                },
                _ = shutdown_rx.changed() => return,
            }
        }
    }
}

async fn sweep(request_tx: &mpsc::Sender<CacheRequest>) -> Option<usize> {
    let (response_tx, response_rx) = oneshot::channel();

    request_tx
        .send(CacheRequest {
            command: Command::Sweep,
            response_tx,
        })
        .await
        .ok()?;

    match response_rx.await.ok()? {
        Response::Swept(removed) => Some(removed),
        _ => None,
    }
}

/// Connects to `addr` and, if `node_context.auth_secret` is set, performs
/// the auth handshake it expects before accepting any other command —
/// shared by every place that opens an outbound node-to-node connection
/// (`run_migration`'s own persistent connection, and the one-shot
/// `set_on_joining_node`/`delete_on_joining_node` calls used to forward a
/// racing client write mid-migration).
async fn connect_and_authenticate(
    node_context: &NodeContext,
    addr: &str,
) -> io::Result<ClientStream> {
    let mut stream = connect_client_stream(addr, node_context.tls_connector.as_ref()).await?;

    if let Some(secret) = &node_context.auth_secret {
        stream.write_all(&auth_message(secret)).await?;

        let mut ack = [0u8; 3];
        stream.read_exact(&mut ack).await?;

        if &ack != b"On\n" {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "joining node rejected the auth secret",
            ));
        }
    }

    Ok(stream)
}

async fn send_set(
    stream: &mut ClientStream,
    key: &[u8],
    value: &[u8],
    ttl: Option<Duration>,
) -> io::Result<()> {
    stream.write_all(&set_message(key, value, ttl)).await?;

    let mut ack = [0u8; 2];
    stream.read_exact(&mut ack).await?;

    if &ack != b"S\n" {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "joining node did not acknowledge the transferred key",
        ));
    }

    Ok(())
}

async fn set_on_joining_node(
    node_context: &NodeContext,
    joining_addr: &str,
    key: &[u8],
    value: &[u8],
    ttl: Option<Duration>,
) -> io::Result<()> {
    let mut stream = connect_and_authenticate(node_context, joining_addr).await?;
    send_set(&mut stream, key, value, ttl).await
}

/// This node's own current view of cluster membership, if it has one yet
/// (see `NodeContext::known_ring`) says `key` isn't this node's to serve
/// anymore.
fn wrong_node(node_context: &NodeContext, key: &[u8]) -> bool {
    node_context
        .known_ring
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .as_ref()
        .is_some_and(|membership| {
            // ADR-0011: this node serves a key when it's anywhere in the
            // key's top-R, not only when it's the primary.
            !membership
                .ring
                .is_owner(key, &node_context.name, membership.replication)
        })
}

/// If a handoff is currently in flight and `key` is one it's moving (per
/// its `after_ring`), returns the joining node's address — for
/// `handle_connection` to also propagate a client's `S`/`D` for that key
/// there, so the joining node doesn't end up serving a stale value once
/// promoted (see doc/adr/0008's Consequences).
fn migration_target_for(node_context: &NodeContext, key: &[u8]) -> Option<String> {
    let mut slot = node_context
        .active_migration
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());

    // Issue #3: a completed handoff keeps forwarding until the grace
    // passes (discovery publishes the joiner — or abandons the join —
    // well within it). Expired entries are cleared lazily here.
    let expired = slot.as_ref().is_some_and(|active| {
        active
            .completed_at
            .is_some_and(|completed_at| completed_at.elapsed() >= FORWARDING_GRACE)
    });
    if expired {
        *slot = None;
    }

    slot.as_ref()
        .filter(|active| {
            // ADR-0011: the joiner is a destination for `key` whenever it
            // entered the key's top-R, not only as its new primary.
            active
                .after_ring
                .is_owner(key, &active.joining_name, active.replication)
        })
        .map(|active| active.joining_addr.clone())
}

/// Forwards a client's `D` for `key` to `joining_addr`, mirroring
/// `set_on_joining_node` but for deletes — see `migration_target_for`.
/// Accepts either `D\n` (the key was present there too) or `N\n` (it
/// hadn't arrived yet, e.g. this delete raced ahead of the migration
/// task's own send of it) as a successful delivery.
async fn delete_on_joining_node(
    node_context: &NodeContext,
    joining_addr: &str,
    key: &[u8],
) -> io::Result<()> {
    let mut stream = connect_and_authenticate(node_context, joining_addr).await?;

    stream.write_all(&delete_message(key)).await?;

    let mut ack = [0u8; 2];
    stream.read_exact(&mut ack).await?;

    if &ack != b"D\n" && &ack != b"N\n" {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "joining node did not acknowledge the forwarded delete",
        ));
    }

    Ok(())
}

async fn report_complete(node_context: &NodeContext, joining_name: &str) -> io::Result<()> {
    let mut stream = connect_client_stream(
        &node_context.discovery_addr,
        node_context.tls_connector.as_ref(),
    )
    .await?;

    if let Some(secret) = &node_context.auth_secret {
        stream.write_all(&auth_message(secret)).await?;

        let mut ack = [0u8; 3];
        stream.read_exact(&mut ack).await?;

        if &ack != b"Od\n" {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "discovery server rejected the auth secret",
            ));
        }
    }

    stream
        .write_all(&complete_message(&node_context.name, joining_name))
        .await?;

    let mut ack = [0u8; 2];
    stream.read_exact(&mut ack).await?;

    if &ack != b"A\n" {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "discovery server did not acknowledge C",
        ));
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;

    #[tokio::test(flavor = "current_thread")]
    async fn run_cache_stores_and_retrieves_a_value() {
        let (request_tx, request_rx) = mpsc::channel(1);

        let cache_task = tokio::spawn(run_cache(request_rx, MAX_CACHE_MEMORY_BYTES));

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

        let cache_task = tokio::spawn(run_cache(request_rx, MAX_CACHE_MEMORY_BYTES));
        let connection_task = tokio::spawn(handle_connection(
            ServerStream::Plain(server),
            request_tx.clone(),
            ConnectionConfig {
                idle_timeout: IDLE_TIMEOUT,
                auth_secret: None,
                tls_acceptor: None,
                node_context: None,
                migration_tx: mpsc::channel(1).0,
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
                node_context: None,
                migration_tx: mpsc::channel(1).0,
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

        let error = run(&address, None, None, None, MAX_CACHE_MEMORY_BYTES)
            .await
            .unwrap_err();

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
                node_context: None,
                migration_tx: mpsc::channel(1).0,
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

        dispatch_connection(
            first_server,
            first_address,
            request_tx.clone(),
            Arc::clone(&connection_limit),
            ConnectionConfig {
                idle_timeout: IDLE_TIMEOUT,
                auth_secret: None,
                tls_acceptor: None,
                node_context: None,
                migration_tx: mpsc::channel(1).0,
            },
            shutdown_rx.clone(),
            &mut connection_tasks,
        );

        // The connection is now handled entirely in a spawned task; let it run
        // far enough to take the sole permit and settle into its read loop.
        tokio::task::yield_now().await;
        assert_eq!(connection_limit.available_permits(), 0);

        let (mut second_client, second_server) = tcp_pair().await;
        let second_address = second_server.peer_addr().unwrap();

        dispatch_connection(
            second_server,
            second_address,
            request_tx.clone(),
            Arc::clone(&connection_limit),
            ConnectionConfig {
                idle_timeout: IDLE_TIMEOUT,
                auth_secret: None,
                tls_acceptor: None,
                node_context: None,
                migration_tx: mpsc::channel(1).0,
            },
            shutdown_rx,
            &mut connection_tasks,
        );

        // Reading to EOF drives the over-limit task to completion: it replies
        // "Busy" and closes without ever acquiring a permit.
        let mut response = Vec::new();
        second_client.read_to_end(&mut response).await.unwrap();

        assert_eq!(response, b"B\n");
        assert_eq!(connection_limit.available_permits(), 0);

        connection_tasks.abort_all();

        while connection_tasks.join_next().await.is_some() {}

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
                node_context: None,
                migration_tx: mpsc::channel(1).0,
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
                node_context: None,
                migration_tx: mpsc::channel(1).0,
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
                node_context: None,
                migration_tx: mpsc::channel(1).0,
            },
            shutdown_rx,
        ));

        client.write_all(b"G 4\nname").await.unwrap();

        let mut response = [0u8; 3];
        client.read_exact(&mut response).await.unwrap();
        assert_eq!(&response, b"En\n");

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
                node_context: None,
                migration_tx: mpsc::channel(1).0,
            },
            shutdown_rx,
        ));

        client.write_all(b"A 11\nwrong-value").await.unwrap();

        let mut response = [0u8; 3];
        client.read_exact(&mut response).await.unwrap();
        assert_eq!(&response, b"En\n");

        let error = connection_task.await.unwrap().unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::PermissionDenied);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn handle_connection_accepts_commands_after_correct_auth() {
        let (mut client, server) = tcp_pair().await;
        let (request_tx, request_rx) = mpsc::channel(1);
        let (_shutdown_tx, shutdown_rx) = watch::channel(false);

        let cache_task = tokio::spawn(run_cache(request_rx, MAX_CACHE_MEMORY_BYTES));
        let connection_task = tokio::spawn(handle_connection(
            ServerStream::Plain(server),
            request_tx.clone(),
            ConnectionConfig {
                idle_timeout: IDLE_TIMEOUT,
                auth_secret: Some(Bytes::from_static(b"correct-secret")),
                tls_acceptor: None,
                node_context: None,
                migration_tx: mpsc::channel(1).0,
            },
            shutdown_rx,
        ));

        client
            .write_all(b"A 14\ncorrect-secretG 4\nname")
            .await
            .unwrap();
        client.shutdown().await.unwrap();

        let expected = b"On\nN\n";
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

        let cache_task = tokio::spawn(run_cache(request_rx, MAX_CACHE_MEMORY_BYTES));
        let connection_task = tokio::spawn(handle_connection(
            ServerStream::Plain(server),
            request_tx.clone(),
            ConnectionConfig {
                idle_timeout: IDLE_TIMEOUT,
                auth_secret: None,
                tls_acceptor: None,
                node_context: None,
                migration_tx: mpsc::channel(1).0,
            },
            shutdown_rx,
        ));

        client.write_all(b"A 8\nanything").await.unwrap();
        client.shutdown().await.unwrap();

        let expected = b"On\n";
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

    #[tokio::test(flavor = "current_thread")]
    async fn migrate_with_replication_marks_displaced_copies_and_keeps_the_senders() {
        let (request_tx, request_rx) = mpsc::channel(1);
        let cache_task = tokio::spawn(run_cache(request_rx, MAX_CACHE_MEMORY_BYTES));

        // Chosen (ready-node / other-node / joiner-0, R=2) so both
        // ADR-0011 roles land on this node at once:
        //   "key-0": pre-join top-2 = [ready-node, other-node]; joiner-0
        //            enters and displaces other-node — ready-node is the
        //            designated sender and STAYS an owner, so it must
        //            transfer the key and keep its own copy unmarked.
        //   "key-3": pre-join top-2 = [other-node, ready-node]; joiner-0
        //            enters and displaces ready-node, which is NOT the
        //            sender — no transfer, but its now-dead copy must be
        //            marked so the post-handoff sweep reclaims it.
        send_command(
            &request_tx,
            Command::Set {
                key: Bytes::from_static(b"key-0"),
                value: Bytes::from_static(b"primary-copy"),
                ttl: None,
            },
        )
        .await;
        send_command(
            &request_tx,
            Command::Set {
                key: Bytes::from_static(b"key-3"),
                value: Bytes::from_static(b"replica-copy"),
                ttl: None,
            },
        )
        .await;

        // Fake joining node: must receive exactly one SET (for "key-0").
        let joining_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let joining_addr = joining_listener.local_addr().unwrap().to_string();
        let joining_received: Arc<std::sync::Mutex<Vec<u8>>> =
            Arc::new(std::sync::Mutex::new(Vec::new()));
        let joining_received_task = Arc::clone(&joining_received);
        let joining_task = tokio::spawn(async move {
            let (mut connection, _) = joining_listener.accept().await.unwrap();
            let mut buffer = [0u8; 256];
            let bytes_read = connection.read(&mut buffer).await.unwrap();
            joining_received_task
                .lock()
                .unwrap()
                .extend_from_slice(&buffer[..bytes_read]);
            connection.write_all(b"S\n").await.unwrap();
        });

        // Fake discovery: expects the C completion report.
        let discovery_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let discovery_addr = discovery_listener.local_addr().unwrap().to_string();
        let discovery_task = tokio::spawn(async move {
            let (mut connection, _) = discovery_listener.accept().await.unwrap();
            let mut buffer = [0u8; 256];
            let _ = connection.read(&mut buffer).await.unwrap();
            connection.write_all(b"A\n").await.unwrap();
        });

        let node_context = NodeContext {
            name: "ready-node".to_string(),
            discovery_addr,
            active_migration: Arc::new(Mutex::new(None)),
            known_ring: Arc::new(Mutex::new(None)),
            auth_secret: None,
            tls_connector: None,
            request_tx: request_tx.clone(),
        };

        run_migration(
            node_context.clone(),
            "joiner-0".to_string(),
            joining_addr.clone(),
            vec![
                ("ready-node".to_string(), "127.0.0.1:1".to_string()),
                ("other-node".to_string(), "127.0.0.1:1".to_string()),
            ],
            2,
        )
        .await;

        // The joiner got exactly the sender's key, nothing else.
        assert_eq!(
            *joining_received.lock().unwrap(),
            set_message(b"key-0", b"primary-copy", None)
        );

        // The displaced copy — and only it — is reclaimed by the sweep.
        assert_eq!(
            send_command(&request_tx, Command::Sweep).await,
            Response::Swept(1)
        );
        match send_command(
            &request_tx,
            Command::PeekEntry {
                key: Bytes::from_static(b"key-0"),
            },
        )
        .await
        {
            Response::Entries(entries) => {
                assert_eq!(entries.len(), 1, "the sender must keep its copy")
            }
            other => panic!("unexpected response: {other:?}"),
        }
        match send_command(
            &request_tx,
            Command::PeekEntry {
                key: Bytes::from_static(b"key-3"),
            },
        )
        .await
        {
            Response::Entries(entries) => {
                assert!(entries.is_empty(), "the displaced copy must be swept")
            }
            other => panic!("unexpected response: {other:?}"),
        }

        // And the flipped membership now rejects the displaced key while
        // still serving the kept one.
        assert!(wrong_node(&node_context, b"key-3"));
        assert!(!wrong_node(&node_context, b"key-0"));

        // Issue #3: this node's own share being done must NOT close the
        // write-forwarding window — discovery hasn't published the joiner
        // yet (other ready nodes may still be transferring), so a
        // concurrent client write for a key in the joiner's top-R still
        // needs forwarding.
        assert_eq!(
            migration_target_for(&node_context, b"key-0").as_deref(),
            Some(joining_addr.as_str()),
        );
        // ...but sweeping must no longer be paused by the lingering entry
        // (only a *running* transfer pauses it).
        assert!(
            node_context
                .active_migration
                .lock()
                .unwrap()
                .as_ref()
                .unwrap()
                .completed_at
                .is_some(),
            "the completed handoff should carry its completion stamp"
        );

        joining_task.await.unwrap();
        discovery_task.await.unwrap();
        drop(node_context);
        drop(request_tx);
        cache_task.await.unwrap();
    }

    #[test]
    fn heartbeat_message_declares_the_name_length_before_the_name() {
        assert_eq!(
            heartbeat_message("some-name", Some(2)),
            b"H 9 2\nsome-name".to_vec()
        );
    }

    #[test]
    fn heartbeat_message_encodes_an_unknown_replication_belief_as_zero() {
        assert_eq!(
            heartbeat_message("some-name", None),
            b"H 9 0\nsome-name".to_vec()
        );
    }

    #[test]
    fn join_message_declares_the_name_length_and_the_port() {
        assert_eq!(
            join_message("some-name", 8356),
            b"J 9 8356\nsome-name".to_vec()
        );
    }

    #[test]
    fn set_message_without_a_ttl_omits_the_third_header_field() {
        assert_eq!(
            set_message(b"name", b"Alice", None),
            b"S 4 5\nnameAlice".to_vec()
        );
    }

    #[test]
    fn set_message_with_a_ttl_rounds_down_to_whole_seconds() {
        assert_eq!(
            set_message(b"name", b"Alice", Some(Duration::from_millis(4900))),
            b"S 4 5 4\nnameAlice".to_vec()
        );
    }

    #[test]
    fn complete_message_declares_the_name_length_before_the_name() {
        assert_eq!(
            complete_message("some-name", "joiner"),
            b"C 9 6\nsome-namejoiner".to_vec()
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn a_completed_forwarding_window_expires_after_the_grace() {
        // Issue #3: the lingering entry forwards only within
        // FORWARDING_GRACE; past it, migration_target_for clears the slot
        // and stops forwarding.
        let (request_tx, _request_rx) = mpsc::channel(1);
        let node_context = NodeContext {
            name: "ready-node".to_string(),
            discovery_addr: "127.0.0.1:1".to_string(),
            active_migration: Arc::new(Mutex::new(None)),
            known_ring: Arc::new(Mutex::new(None)),
            auth_secret: None,
            tls_connector: None,
            request_tx,
        };

        *node_context.active_migration.lock().unwrap() = Some(ActiveMigration {
            joining_name: "joiner-0".to_string(),
            joining_addr: "127.0.0.1:9".to_string(),
            after_ring: Arc::new(HashRing::new(vec![
                "ready-node".to_string(),
                "joiner-0".to_string(),
            ])),
            replication: 2,
            completed_at: Some(Instant::now() - FORWARDING_GRACE - Duration::from_secs(1)),
            abort_requested: Arc::new(AtomicBool::new(false)),
        });

        assert_eq!(migration_target_for(&node_context, b"key-0"), None);
        assert!(
            node_context.active_migration.lock().unwrap().is_none(),
            "an expired forwarding entry should be cleared lazily"
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn migrate_command_transfers_matching_keys_and_reports_completion() {
        let (request_tx, request_rx) = mpsc::channel(1);
        let cache_task = tokio::spawn(run_cache(request_rx, MAX_CACHE_MEMORY_BYTES));

        send_command(
            &request_tx,
            Command::Set {
                key: Bytes::from_static(b"name"),
                value: Bytes::from_static(b"Alice"),
                ttl: None,
            },
        )
        .await;

        // A fake joining node: accepts one connection, expects a SET (no
        // auth configured), and acks it.
        let joining_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let joining_addr = joining_listener.local_addr().unwrap().to_string();
        let joining_received: Arc<std::sync::Mutex<Vec<u8>>> =
            Arc::new(std::sync::Mutex::new(Vec::new()));
        let joining_received_task = Arc::clone(&joining_received);
        let joining_task = tokio::spawn(async move {
            let (mut connection, _) = joining_listener.accept().await.unwrap();
            let mut buffer = [0u8; 256];
            let bytes_read = connection.read(&mut buffer).await.unwrap();
            joining_received_task
                .lock()
                .unwrap()
                .extend_from_slice(&buffer[..bytes_read]);
            connection.write_all(b"S\n").await.unwrap();
        });

        // A fake discovery server: accepts one connection, expects C, acks
        // it.
        let discovery_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let discovery_addr = discovery_listener.local_addr().unwrap().to_string();
        let discovery_received: Arc<std::sync::Mutex<Vec<u8>>> =
            Arc::new(std::sync::Mutex::new(Vec::new()));
        let discovery_received_task = Arc::clone(&discovery_received);
        let discovery_task = tokio::spawn(async move {
            let (mut connection, _) = discovery_listener.accept().await.unwrap();
            let mut buffer = [0u8; 256];
            let bytes_read = connection.read(&mut buffer).await.unwrap();
            discovery_received_task
                .lock()
                .unwrap()
                .extend_from_slice(&buffer[..bytes_read]);
            connection.write_all(b"A\n").await.unwrap();
        });

        let (mut client, server) = tcp_pair().await;
        let (_shutdown_tx, shutdown_rx) = watch::channel(false);

        // Stands in for `run`'s own loop, which is what normally drains
        // `migration_tx` and spawns what it receives (see
        // `ConnectionConfig::migration_tx`) — `handle_connection` itself no
        // longer spawns `run_migration` directly.
        let (migration_tx, mut migration_rx) = mpsc::channel::<MigrationTask>(1);
        let migration_relay = tokio::spawn(async move {
            while let Some(task) = migration_rx.recv().await {
                task.await;
            }
        });

        let connection_task = tokio::spawn(handle_connection(
            ServerStream::Plain(server),
            request_tx.clone(),
            ConnectionConfig {
                idle_timeout: IDLE_TIMEOUT,
                auth_secret: None,
                tls_acceptor: None,
                node_context: Some(NodeContext {
                    name: "ready-node".to_string(),
                    discovery_addr,
                    active_migration: Arc::new(Mutex::new(None)),
                    known_ring: Arc::new(Mutex::new(None)),
                    auth_secret: None,
                    tls_connector: None,
                    request_tx: request_tx.clone(),
                }),
                migration_tx,
            },
            shutdown_rx.clone(),
        ));

        // No other Joined nodes: the after-join ring has only the joining
        // node in it, so every key (including "name") routes to it.
        // Chosen so HRW ranks it above "ready-node" for both test keys
        // ("name", "age") — the transfer set must be non-empty.
        let joining_name = "joiner-107";
        let mut migrate_message =
            format!("M {} {} 0 1\n", joining_name.len(), joining_addr.len()).into_bytes();
        migrate_message.extend_from_slice(joining_name.as_bytes());
        migrate_message.extend_from_slice(joining_addr.as_bytes());

        client.write_all(&migrate_message).await.unwrap();

        let mut ack = [0u8; 2];
        client.read_exact(&mut ack).await.unwrap();
        assert_eq!(&ack, b"A\n");

        for _ in 0..1000 {
            if !discovery_received.lock().unwrap().is_empty() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(1)).await;
        }

        let expected_set = set_message(b"name", b"Alice", None);
        assert_eq!(*joining_received.lock().unwrap(), expected_set);
        assert_eq!(
            *discovery_received.lock().unwrap(),
            complete_message("ready-node", "joiner-107")
        );

        joining_task.await.unwrap();
        discovery_task.await.unwrap();
        // Ends `connection_task` (dropping its `node_context`'s
        // `request_tx` clone) before awaiting `cache_task`, which needs
        // every sender dropped to see its channel close — otherwise this
        // deadlocks until `handle_connection`'s own idle timeout breaks
        // it, wasting `IDLE_TIMEOUT` (30s) on every run for nothing (the
        // same class of ordering bug `run`'s shutdown path had).
        client.shutdown().await.unwrap();
        let _ = connection_task.await;
        migration_relay.await.unwrap();
        drop(request_tx);
        cache_task.await.unwrap();
    }

    #[tokio::test(flavor = "current_thread")]
    async fn migrate_command_cancelled_mid_transfer_rolls_back_marks_and_skips_completion() {
        let (request_tx, request_rx) = mpsc::channel(1);
        let cache_task = tokio::spawn(run_cache(request_rx, MAX_CACHE_MEMORY_BYTES));

        send_command(
            &request_tx,
            Command::Set {
                key: Bytes::from_static(b"name"),
                value: Bytes::from_static(b"Alice"),
                ttl: None,
            },
        )
        .await;

        // A fake joining node: accepts the SET but withholds its ack until
        // told to, via `release_rx` — so the test can be sure `X` is fully
        // processed before `run_migration`'s `set_on_joining_node` call
        // for this key ever returns, making this deterministic regardless
        // of relative timing.
        let joining_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let joining_addr = joining_listener.local_addr().unwrap().to_string();
        let (set_received_tx, set_received_rx) = oneshot::channel();
        let (release_tx, release_rx) = oneshot::channel::<()>();
        let joining_task = tokio::spawn(async move {
            let (mut connection, _) = joining_listener.accept().await.unwrap();
            let mut buffer = [0u8; 256];
            let _ = connection.read(&mut buffer).await.unwrap();
            set_received_tx.send(()).unwrap();
            release_rx.await.unwrap();
            connection.write_all(b"S\n").await.unwrap();
        });

        // A fake discovery server: must receive nothing — a cancelled
        // migration doesn't report completion.
        let discovery_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let discovery_addr = discovery_listener.local_addr().unwrap().to_string();

        let (mut client, server) = tcp_pair().await;
        let (_shutdown_tx, shutdown_rx) = watch::channel(false);

        // Stands in for `run`'s own loop, which is what normally drains
        // `migration_tx` and spawns what it receives (see
        // `ConnectionConfig::migration_tx`) — `handle_connection` itself no
        // longer spawns `run_migration` directly.
        let (migration_tx, mut migration_rx) = mpsc::channel::<MigrationTask>(1);
        let migration_relay = tokio::spawn(async move {
            while let Some(task) = migration_rx.recv().await {
                task.await;
            }
        });

        let connection_task = tokio::spawn(handle_connection(
            ServerStream::Plain(server),
            request_tx.clone(),
            ConnectionConfig {
                idle_timeout: IDLE_TIMEOUT,
                auth_secret: None,
                tls_acceptor: None,
                node_context: Some(NodeContext {
                    name: "ready-node".to_string(),
                    discovery_addr,
                    active_migration: Arc::new(Mutex::new(None)),
                    known_ring: Arc::new(Mutex::new(None)),
                    auth_secret: None,
                    tls_connector: None,
                    request_tx: request_tx.clone(),
                }),
                migration_tx,
            },
            shutdown_rx.clone(),
        ));

        // Chosen so HRW ranks it above "ready-node" for both test keys
        // ("name", "age") — the transfer set must be non-empty.
        let joining_name = "joiner-107";
        let mut migrate_message =
            format!("M {} {} 0 1\n", joining_name.len(), joining_addr.len()).into_bytes();
        migrate_message.extend_from_slice(joining_name.as_bytes());
        migrate_message.extend_from_slice(joining_addr.as_bytes());

        client.write_all(&migrate_message).await.unwrap();

        let mut ack = [0u8; 2];
        client.read_exact(&mut ack).await.unwrap();
        assert_eq!(&ack, b"A\n");

        // The joining node has the SET in hand but hasn't acked it yet, so
        // `run_migration` is still blocked on that ack — send the cancel
        // now, on the same connection (a fresh one-shot connection, as
        // discovery would really use, is equivalent from the node's side).
        set_received_rx.await.unwrap();

        let mut cancel_message = format!("X {}\n", joining_name.len()).into_bytes();
        cancel_message.extend_from_slice(joining_name.as_bytes());
        client.write_all(&cancel_message).await.unwrap();

        let mut cancel_ack = [0u8; 2];
        client.read_exact(&mut cancel_ack).await.unwrap();
        assert_eq!(&cancel_ack, b"A\n");

        // Only now let the joining node's ack through, so `run_migration`
        // resumes with `abort_requested` already set.
        release_tx.send(()).unwrap();
        joining_task.await.unwrap();

        // Nothing should ever connect to "discovery" — poll briefly for
        // the absence of a connection rather than asserting instantly.
        let no_completion_reported =
            timeout(Duration::from_millis(200), discovery_listener.accept())
                .await
                .is_err();
        assert!(
            no_completion_reported,
            "a cancelled migration must not report completion"
        );

        assert_eq!(
            send_command(
                &request_tx,
                Command::Get {
                    key: Bytes::from_static(b"name")
                }
            )
            .await,
            Response::Value(Bytes::from_static(b"Alice"))
        );
        assert_eq!(
            send_command(&request_tx, Command::Sweep).await,
            Response::Swept(0)
        );

        client.shutdown().await.unwrap();
        let _ = connection_task.await;
        migration_relay.await.unwrap();
        drop(request_tx);
        cache_task.await.unwrap();
    }

    #[tokio::test(flavor = "current_thread")]
    async fn migrate_command_reuses_one_connection_for_every_key() {
        let (request_tx, request_rx) = mpsc::channel(1);
        let cache_task = tokio::spawn(run_cache(request_rx, MAX_CACHE_MEMORY_BYTES));

        send_command(
            &request_tx,
            Command::Set {
                key: Bytes::from_static(b"name"),
                value: Bytes::from_static(b"Alice"),
                ttl: None,
            },
        )
        .await;
        send_command(
            &request_tx,
            Command::Set {
                key: Bytes::from_static(b"age"),
                value: Bytes::from_static(b"30"),
                ttl: None,
            },
        )
        .await;

        // A fake joining node: accepts exactly one connection and expects
        // both SETs on it — `run_migration` must reuse one connection
        // across keys rather than reconnecting per key (see its own doc
        // comment on the ephemeral-port exhaustion that used to cause).
        let joining_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let joining_addr = joining_listener.local_addr().unwrap().to_string();
        let joining_received: Arc<std::sync::Mutex<Vec<u8>>> =
            Arc::new(std::sync::Mutex::new(Vec::new()));
        let joining_received_task = Arc::clone(&joining_received);
        let joining_task = tokio::spawn(async move {
            let (mut connection, _) = joining_listener.accept().await.unwrap();

            for _ in 0..2 {
                let mut buffer = [0u8; 256];
                let bytes_read = connection.read(&mut buffer).await.unwrap();
                joining_received_task
                    .lock()
                    .unwrap()
                    .extend_from_slice(&buffer[..bytes_read]);
                connection.write_all(b"S\n").await.unwrap();
            }

            let second_connection =
                timeout(Duration::from_millis(200), joining_listener.accept()).await;
            assert!(
                second_connection.is_err(),
                "expected the same connection to be reused for both keys"
            );
        });

        // A fake discovery server: accepts one connection, expects C, acks
        // it.
        let discovery_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let discovery_addr = discovery_listener.local_addr().unwrap().to_string();
        let discovery_received: Arc<std::sync::Mutex<Vec<u8>>> =
            Arc::new(std::sync::Mutex::new(Vec::new()));
        let discovery_received_task = Arc::clone(&discovery_received);
        let discovery_task = tokio::spawn(async move {
            let (mut connection, _) = discovery_listener.accept().await.unwrap();
            let mut buffer = [0u8; 256];
            let bytes_read = connection.read(&mut buffer).await.unwrap();
            discovery_received_task
                .lock()
                .unwrap()
                .extend_from_slice(&buffer[..bytes_read]);
            connection.write_all(b"A\n").await.unwrap();
        });

        let (mut client, server) = tcp_pair().await;
        let (_shutdown_tx, shutdown_rx) = watch::channel(false);

        // Stands in for `run`'s own loop, which is what normally drains
        // `migration_tx` and spawns what it receives (see
        // `ConnectionConfig::migration_tx`) — `handle_connection` itself no
        // longer spawns `run_migration` directly.
        let (migration_tx, mut migration_rx) = mpsc::channel::<MigrationTask>(1);
        let migration_relay = tokio::spawn(async move {
            while let Some(task) = migration_rx.recv().await {
                task.await;
            }
        });

        let connection_task = tokio::spawn(handle_connection(
            ServerStream::Plain(server),
            request_tx.clone(),
            ConnectionConfig {
                idle_timeout: IDLE_TIMEOUT,
                auth_secret: None,
                tls_acceptor: None,
                node_context: Some(NodeContext {
                    name: "ready-node".to_string(),
                    discovery_addr,
                    active_migration: Arc::new(Mutex::new(None)),
                    known_ring: Arc::new(Mutex::new(None)),
                    auth_secret: None,
                    tls_connector: None,
                    request_tx: request_tx.clone(),
                }),
                migration_tx,
            },
            shutdown_rx.clone(),
        ));

        // Chosen so HRW ranks it above "ready-node" for both test keys
        // ("name", "age") — the transfer set must be non-empty.
        let joining_name = "joiner-107";
        let mut migrate_message =
            format!("M {} {} 0 1\n", joining_name.len(), joining_addr.len()).into_bytes();
        migrate_message.extend_from_slice(joining_name.as_bytes());
        migrate_message.extend_from_slice(joining_addr.as_bytes());

        client.write_all(&migrate_message).await.unwrap();

        let mut ack = [0u8; 2];
        client.read_exact(&mut ack).await.unwrap();
        assert_eq!(&ack, b"A\n");

        for _ in 0..1000 {
            if !discovery_received.lock().unwrap().is_empty() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(1)).await;
        }

        let expected_name = set_message(b"name", b"Alice", None);
        let expected_age = set_message(b"age", b"30", None);
        let received = joining_received.lock().unwrap().clone();
        assert!(
            received
                .windows(expected_name.len())
                .any(|window| window == expected_name.as_slice()),
            "expected the joining node to receive the SET for \"name\""
        );
        assert!(
            received
                .windows(expected_age.len())
                .any(|window| window == expected_age.as_slice()),
            "expected the joining node to receive the SET for \"age\""
        );
        assert_eq!(
            *discovery_received.lock().unwrap(),
            complete_message("ready-node", "joiner-107")
        );

        joining_task.await.unwrap();
        discovery_task.await.unwrap();
        client.shutdown().await.unwrap();
        let _ = connection_task.await;
        migration_relay.await.unwrap();
        drop(request_tx);
        cache_task.await.unwrap();
    }

    #[tokio::test(flavor = "current_thread")]
    async fn migrate_command_gives_up_and_rolls_back_after_permanent_transfer_failure() {
        let (request_tx, request_rx) = mpsc::channel(1);
        let cache_task = tokio::spawn(run_cache(request_rx, MAX_CACHE_MEMORY_BYTES));

        send_command(
            &request_tx,
            Command::Set {
                key: Bytes::from_static(b"name"),
                value: Bytes::from_static(b"Alice"),
                ttl: None,
            },
        )
        .await;

        // A joining node address nothing is listening on: every connect
        // attempt fails immediately, so `run_migration` exhausts
        // `KEY_TRANSFER_ATTEMPTS` and gives up on the whole migration.
        let dead_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let joining_addr = dead_listener.local_addr().unwrap().to_string();
        drop(dead_listener);

        // A fake discovery server: must receive nothing — a migration
        // that permanently failed to transfer a key must not report
        // completion.
        let discovery_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let discovery_addr = discovery_listener.local_addr().unwrap().to_string();

        let (mut client, server) = tcp_pair().await;
        let (_shutdown_tx, shutdown_rx) = watch::channel(false);

        // Stands in for `run`'s own loop, which is what normally drains
        // `migration_tx` and spawns what it receives (see
        // `ConnectionConfig::migration_tx`) — `handle_connection` itself no
        // longer spawns `run_migration` directly.
        let (migration_tx, mut migration_rx) = mpsc::channel::<MigrationTask>(1);
        let migration_relay = tokio::spawn(async move {
            while let Some(task) = migration_rx.recv().await {
                task.await;
            }
        });

        let connection_task = tokio::spawn(handle_connection(
            ServerStream::Plain(server),
            request_tx.clone(),
            ConnectionConfig {
                idle_timeout: IDLE_TIMEOUT,
                auth_secret: None,
                tls_acceptor: None,
                node_context: Some(NodeContext {
                    name: "ready-node".to_string(),
                    discovery_addr,
                    active_migration: Arc::new(Mutex::new(None)),
                    known_ring: Arc::new(Mutex::new(None)),
                    auth_secret: None,
                    tls_connector: None,
                    request_tx: request_tx.clone(),
                }),
                migration_tx,
            },
            shutdown_rx.clone(),
        ));

        // Chosen so HRW ranks it above "ready-node" for both test keys
        // ("name", "age") — the transfer set must be non-empty.
        let joining_name = "joiner-107";
        let mut migrate_message =
            format!("M {} {} 0 1\n", joining_name.len(), joining_addr.len()).into_bytes();
        migrate_message.extend_from_slice(joining_name.as_bytes());
        migrate_message.extend_from_slice(joining_addr.as_bytes());

        client.write_all(&migrate_message).await.unwrap();

        let mut ack = [0u8; 2];
        client.read_exact(&mut ack).await.unwrap();
        assert_eq!(&ack, b"A\n");

        // Nothing should ever connect to "discovery".
        let no_completion_reported =
            timeout(Duration::from_millis(200), discovery_listener.accept())
                .await
                .is_err();
        assert!(
            no_completion_reported,
            "a migration that permanently failed to transfer a key must not report completion"
        );

        assert_eq!(
            send_command(
                &request_tx,
                Command::Get {
                    key: Bytes::from_static(b"name")
                }
            )
            .await,
            Response::Value(Bytes::from_static(b"Alice"))
        );
        assert_eq!(
            send_command(&request_tx, Command::Sweep).await,
            Response::Swept(0)
        );

        client.shutdown().await.unwrap();
        let _ = connection_task.await;
        migration_relay.await.unwrap();
        drop(request_tx);
        cache_task.await.unwrap();
    }

    /// Parses a `J <name-length> <port>\n<name>` message (the only one
    /// whose shape the test doesn't already know, since its name is a
    /// random UUID generated inside `send_heartbeats`) and asserts every
    /// subsequent message in `received` is the matching
    /// `H <name-length> <r>\n<name>` heartbeat for that same name — `r=0`
    /// (unknown), since none of these tests give it a `KnownRing` with a
    /// membership belief set.
    fn assert_join_then_heartbeats(received: &[Vec<u8>], port: u16) {
        assert!(
            received.len() >= 4,
            "expected a join plus at least 3 heartbeats, got {}",
            received.len()
        );

        let join = String::from_utf8(received[0].clone()).unwrap();
        let header_end = join.find('\n').unwrap();
        let mut header = join[..header_end].split(' ');
        assert_eq!(header.next(), Some("J"));

        let name_length: usize = header.next().unwrap().parse().unwrap();
        let sent_port: u16 = header.next().unwrap().parse().unwrap();
        assert_eq!(header.next(), None);
        assert_eq!(sent_port, port);

        let body = &join[header_end + 1..];
        let name = &body[..name_length];
        assert_eq!(body.len(), name_length);

        let expected_heartbeat = format!("H {} 0\n{name}", name.len());
        for message in &received[1..] {
            assert_eq!(message, expected_heartbeat.as_bytes());
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn send_heartbeats_stops_immediately_when_already_shut_down() {
        let (_shutdown_tx, shutdown_rx) = watch::channel(true);

        send_heartbeats(
            HeartbeatConfig {
                discovery_addrs: vec!["127.0.0.1:1".to_string()],
                port: 8356,
                interval: Duration::from_secs(60),
                auth_secret: None,
                tls_connector: None,
            },
            "test-node".to_string(),
            Arc::new(Mutex::new(None)),
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
            let mut buffer = [0u8; 128];
            let mut first = true;

            loop {
                match connection.read(&mut buffer).await {
                    Ok(0) | Err(_) => return,
                    Ok(bytes_read) => {
                        fake_discovery_received
                            .lock()
                            .unwrap()
                            .push(buffer[..bytes_read].to_vec());

                        // The first message is J (join); every one after
                        // that is H (heartbeat, once promoted).
                        let ack: &[u8] = if first { b"R\n" } else { b"A\n" };
                        first = false;

                        if connection.write_all(ack).await.is_err() {
                            return;
                        }
                    }
                }
            }
        });

        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let heartbeat_task = tokio::spawn(send_heartbeats(
            HeartbeatConfig {
                discovery_addrs: vec![discovery_addr],
                port: 8356,
                interval: Duration::from_millis(20),
                auth_secret: None,
                tls_connector: None,
            },
            "test-node".to_string(),
            Arc::new(Mutex::new(None)),
            shutdown_rx,
        ));

        sleep(Duration::from_millis(150)).await;

        shutdown_tx.send_replace(true);
        heartbeat_task.await.unwrap();
        fake_discovery.abort();

        assert_join_then_heartbeats(&received.lock().unwrap(), 8356);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn a_heartbeat_reports_the_replication_factor_once_known_ring_is_set() {
        // Issue #30: the belief starts unknown (`r=0`) and, once this node
        // has sent its own first ADR-0011 handoff `M`, every heartbeat
        // after that must carry the real value — not the buffer built at
        // connection time, which would keep reporting "unknown" forever.
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let discovery_addr = listener.local_addr().unwrap().to_string();

        let received: Arc<std::sync::Mutex<Vec<Vec<u8>>>> =
            Arc::new(std::sync::Mutex::new(Vec::new()));
        let fake_discovery_received = Arc::clone(&received);

        let fake_discovery = tokio::spawn(async move {
            let (mut connection, _) = listener.accept().await.unwrap();
            let mut buffer = [0u8; 128];
            let mut first = true;

            loop {
                match connection.read(&mut buffer).await {
                    Ok(0) | Err(_) => return,
                    Ok(bytes_read) => {
                        fake_discovery_received
                            .lock()
                            .unwrap()
                            .push(buffer[..bytes_read].to_vec());

                        let ack: &[u8] = if first { b"R\n" } else { b"A\n" };
                        first = false;

                        if connection.write_all(ack).await.is_err() {
                            return;
                        }
                    }
                }
            }
        });

        let known_ring: KnownRing = Arc::new(Mutex::new(None));
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let heartbeat_task = tokio::spawn(send_heartbeats(
            HeartbeatConfig {
                discovery_addrs: vec![discovery_addr],
                port: 8356,
                interval: Duration::from_millis(20),
                auth_secret: None,
                tls_connector: None,
            },
            "test-node".to_string(),
            Arc::clone(&known_ring),
            shutdown_rx,
        ));

        // Let at least one "unknown" heartbeat go out before this node
        // learns a replication factor.
        sleep(Duration::from_millis(60)).await;
        *known_ring
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = Some(Arc::new(Membership {
            ring: Arc::new(HashRing::new(vec!["test-node".to_string()])),
            replication: 3,
        }));
        sleep(Duration::from_millis(100)).await;

        shutdown_tx.send_replace(true);
        heartbeat_task.await.unwrap();
        fake_discovery.abort();

        let received = received.lock().unwrap();
        let heartbeats: Vec<&Vec<u8>> = received[1..].iter().collect();

        assert!(
            heartbeats
                .iter()
                .any(|message| message.starts_with(b"H 9 0\n")),
            "expected at least one heartbeat reporting the unknown (0) belief before \
             known_ring was set, got {heartbeats:?}"
        );
        assert!(
            heartbeats
                .iter()
                .any(|message| message.starts_with(b"H 9 3\n")),
            "expected at least one heartbeat reporting the newly known replication factor \
             (3) after known_ring was set, got {heartbeats:?}"
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn a_reconnection_after_promotion_announces_instead_of_rejoining() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let discovery_addr = listener.local_addr().unwrap().to_string();

        let registrations: Arc<std::sync::Mutex<Vec<Vec<u8>>>> =
            Arc::new(std::sync::Mutex::new(Vec::new()));
        let fake_registrations = Arc::clone(&registrations);

        let fake_discovery = tokio::spawn(async move {
            // First connection: J -> R, then hang up mid-heartbeat so the
            // node has to re-register.
            let (mut first, _) = listener.accept().await.unwrap();
            let mut buffer = [0u8; 128];
            let bytes_read = first.read(&mut buffer).await.unwrap();
            fake_registrations
                .lock()
                .unwrap()
                .push(buffer[..bytes_read].to_vec());
            first.write_all(b"R\n").await.unwrap();
            let _ = first.read(&mut buffer).await;
            drop(first);

            // Second connection: the re-registration (ADR-0010: must be P,
            // not another handoff-orchestrating J).
            let (mut second, _) = listener.accept().await.unwrap();
            let bytes_read = second.read(&mut buffer).await.unwrap();
            fake_registrations
                .lock()
                .unwrap()
                .push(buffer[..bytes_read].to_vec());
            second.write_all(b"R\n").await.unwrap();

            loop {
                match second.read(&mut buffer).await {
                    Ok(0) | Err(_) => return,
                    Ok(_) => {
                        if second.write_all(b"A\n").await.is_err() {
                            return;
                        }
                    }
                }
            }
        });

        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let heartbeat_task = tokio::spawn(send_heartbeats(
            HeartbeatConfig {
                discovery_addrs: vec![discovery_addr],
                port: 8356,
                interval: Duration::from_millis(20),
                auth_secret: None,
                tls_connector: None,
            },
            "test-node".to_string(),
            Arc::new(Mutex::new(None)),
            shutdown_rx,
        ));

        for _ in 0..500 {
            if registrations.lock().unwrap().len() >= 2 {
                break;
            }
            sleep(Duration::from_millis(5)).await;
        }

        shutdown_tx.send_replace(true);
        heartbeat_task.await.unwrap();
        fake_discovery.abort();

        let registrations = registrations.lock().unwrap();
        assert!(registrations.len() >= 2, "node never re-registered");
        assert_eq!(registrations[0], b"J 9 8356\ntest-node");
        assert_eq!(registrations[1], b"P 9 8356\ntest-node");
    }

    #[tokio::test(flavor = "current_thread")]
    async fn a_standby_discovery_receives_an_announce_only_after_promotion() {
        let primary_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let standby_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let primary_addr = primary_listener.local_addr().unwrap().to_string();
        let standby_addr = standby_listener.local_addr().unwrap().to_string();

        let events: Arc<std::sync::Mutex<Vec<String>>> =
            Arc::new(std::sync::Mutex::new(Vec::new()));

        let primary_events = Arc::clone(&events);
        let fake_primary = tokio::spawn(async move {
            let (mut connection, _) = primary_listener.accept().await.unwrap();
            let mut buffer = [0u8; 128];
            let bytes_read = connection.read(&mut buffer).await.unwrap();
            assert!(buffer[..bytes_read].starts_with(b"J "));

            // Give a broken standby (one that doesn't wait for promotion)
            // time to announce early, which the event order would expose.
            sleep(Duration::from_millis(50)).await;
            primary_events.lock().unwrap().push("promoted".to_string());
            connection.write_all(b"R\n").await.unwrap();

            loop {
                match connection.read(&mut buffer).await {
                    Ok(0) | Err(_) => return,
                    Ok(_) => {
                        if connection.write_all(b"A\n").await.is_err() {
                            return;
                        }
                    }
                }
            }
        });

        let standby_events = Arc::clone(&events);
        let fake_standby = tokio::spawn(async move {
            let (mut connection, _) = standby_listener.accept().await.unwrap();
            let mut buffer = [0u8; 128];
            let bytes_read = connection.read(&mut buffer).await.unwrap();
            standby_events.lock().unwrap().push("announced".to_string());
            assert_eq!(&buffer[..bytes_read], b"P 9 14\ntest-node127.0.0.1:8356");
            connection.write_all(b"R\n").await.unwrap();

            loop {
                match connection.read(&mut buffer).await {
                    Ok(0) | Err(_) => return,
                    Ok(_) => {
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
                discovery_addrs: vec![primary_addr, standby_addr],
                port: 8356,
                interval: Duration::from_millis(20),
                auth_secret: None,
                tls_connector: None,
            },
            "test-node".to_string(),
            Arc::new(Mutex::new(None)),
            shutdown_rx,
        ));

        for _ in 0..500 {
            if events
                .lock()
                .unwrap()
                .iter()
                .any(|event| event == "announced")
            {
                break;
            }
            sleep(Duration::from_millis(5)).await;
        }

        shutdown_tx.send_replace(true);
        heartbeat_task.await.unwrap();
        fake_primary.abort();
        fake_standby.abort();

        assert_eq!(*events.lock().unwrap(), vec!["promoted", "announced"]);
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
                discovery_addrs: vec![discovery_addr.clone()],
                port: 8356,
                interval: Duration::from_millis(20),
                auth_secret: None,
                tls_connector: None,
            },
            "test-node".to_string(),
            Arc::new(Mutex::new(None)),
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
        let cache_task = tokio::spawn(run_cache(request_rx, MAX_CACHE_MEMORY_BYTES));
        let connection_limit = Arc::new(Semaphore::new(1));
        let (_shutdown_tx, shutdown_rx) = watch::channel(false);

        let config = ConnectionConfig {
            idle_timeout: IDLE_TIMEOUT,
            auth_secret: None,
            tls_acceptor: Some(acceptor),
            node_context: None,
            migration_tx: mpsc::channel(1).0,
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
            );

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
            let mut buffer = [0u8; 128];
            let mut first = true;

            loop {
                match tls.read(&mut buffer).await {
                    Ok(0) | Err(_) => return,
                    Ok(bytes_read) => {
                        fake_discovery_received
                            .lock()
                            .unwrap()
                            .push(buffer[..bytes_read].to_vec());

                        let ack: &[u8] = if first { b"R\n" } else { b"A\n" };
                        first = false;

                        if tls.write_all(ack).await.is_err() {
                            return;
                        }
                    }
                }
            }
        });

        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let heartbeat_task = tokio::spawn(send_heartbeats(
            HeartbeatConfig {
                discovery_addrs: vec![discovery_addr],
                port: 8356,
                interval: Duration::from_millis(20),
                auth_secret: None,
                tls_connector: Some(connector),
            },
            "test-node".to_string(),
            Arc::new(Mutex::new(None)),
            shutdown_rx,
        ));

        sleep(Duration::from_millis(150)).await;

        shutdown_tx.send_replace(true);
        heartbeat_task.await.unwrap();
        fake_discovery.abort();

        assert_join_then_heartbeats(&received.lock().unwrap(), 8356);
    }
}
