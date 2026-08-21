use crate::cache::{Cache, SWEEP_BUDGET};
use crate::command::{Command, MigrateProgress, ParseError, parse_resumable};
use crate::hash_ring::HashRing;
use crate::response::Response;
use bytes::{Bytes, BytesMut};
use rustls::pki_types::{CertificateDer, PrivateKeyDer, ServerName};
use rustls::{ClientConfig, RootCertStore, ServerConfig};
use std::collections::HashMap;
use std::future::Future;
use std::io;
use std::io::BufReader;
use std::net::{IpAddr, SocketAddr};
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use std::time::{Duration, Instant};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, ReadBuf};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{Mutex as AsyncMutex, Semaphore, mpsc, oneshot, watch};
use tokio::task::JoinSet;
use tokio::time::{sleep, timeout};
use tokio_rustls::{TlsAcceptor, TlsConnector};
use uuid::Uuid;

const MAX_REQUEST_SIZE: usize = 1024 * 1024;
const MAX_CONNECTIONS: usize = 1024;
/// Coarse cap on how many live connections a single source IP may hold at
/// once, layered under the global `MAX_CONNECTIONS` semaphore (issue: no
/// per-source-IP limit — a single misbehaving or compromised peer could
/// otherwise claim the entire `MAX_CONNECTIONS` budget by itself,
/// starving every other client, without the global semaphore ever
/// reporting anything unusual). Deliberately coarse, not a tight
/// per-client budget: a pooled application host — many worker processes
/// or threads sharing one egress IP, or a fleet behind one NAT — can
/// legitimately hold a large number of concurrent connections to this
/// cache, and this guard exists only to stop one source from
/// monopolising the whole server, not to bound ordinary legitimate
/// concurrency. See `try_acquire_per_ip`.
const MAX_CONNECTIONS_PER_IP: usize = 256;
/// Default for `--max-memory` (issue #19): the cap was previously a fixed
/// constant with no way to tune it, even though the capacity planner
/// (`tools/capacity-planner.html`) already modeled capacity as a function
/// of it.
pub(crate) const MAX_CACHE_MEMORY_BYTES: usize = 256 * 1024 * 1024;
/// How long a connection may go without completing a full request before
/// `handle_connection` disconnects it. Slowloris resistance: the deadline
/// this bounds is anchored to the last time a full command was *parsed*
/// (or to accept-time, before any command has completed) — not to the
/// last byte read. Resetting it on every read, as an earlier version did,
/// let a client that trickles in one byte just under this interval apart
/// hold a `MAX_CONNECTIONS` permit forever without ever finishing a
/// request. The practical consequence: a legitimate request must arrive
/// in full within this long of the previous one completing, not merely
/// send *some* bytes that often.
const IDLE_TIMEOUT: Duration = Duration::from_secs(60);
/// Bounds a response write (issue #4) — see `write_response`. Shorter
/// than `IDLE_TIMEOUT`: that one tolerates a normal gap between a
/// client's requests, but a peer that has simply stopped draining its
/// receive buffer is a distinct failure that shouldn't get to hold a
/// `MAX_CONNECTIONS` permit for as long as an idle-but-otherwise-fine
/// connection is allowed to sit.
const WRITE_TIMEOUT: Duration = Duration::from_secs(10);
/// Base and per-entry components of how long after this node's own
/// handoff completes it keeps forwarding concurrent writes to the joiner
/// (issue #3) — sized the same way, and for the same reason, as
/// discovery's own size-derived migration timeout (size-derived migration timeout):
/// by the time it elapses the join has either completed cluster-wide
/// (the joiner is in `L`, clients route to it directly) or been
/// abandoned (an `X` cleared the slot), and a bigger handoff needs more
/// of either to happen. See `forwarding_grace`.
const FORWARDING_GRACE_BASE: Duration = Duration::from_secs(60);
const FORWARDING_GRACE_PER_ENTRY: Duration = Duration::from_millis(5);

/// This node's own size-derived forwarding grace (size-derived migration timeout),
/// computed from how many entries `run_migration` actually sent as part
/// of the handoff `entries_sent` came from — never anything reported by
/// another ready node. Saturates rather than overflows for a
/// pathologically large count.
fn forwarding_grace(entries_sent: usize) -> Duration {
    FORWARDING_GRACE_BASE
        + FORWARDING_GRACE_PER_ENTRY.saturating_mul(entries_sent.min(u32::MAX as usize) as u32)
}
/// How often the staged node join active-deletion sweep runs. See `run_sweep`.
const SWEEP_INTERVAL: Duration = Duration::from_secs(5);
const SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(10);
const TLS_HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(10);
/// Bounds each individual outbound dial, write, and ack read this node
/// makes toward another node — the migration handoff (`run_migration`) and
/// the concurrent forwarding of client `S`/`D` to a joiner
/// (`set_on_joining_node`/`delete_on_joining_node`). Without it, a joiner
/// that accepts TCP but never answers (crashed-but-socket-open, a
/// blackholed route, no keepalive configured) blocks a single leg (the
/// dial, the TLS handshake, the auth round trip, or the write/ack)
/// forever while it still holds a `MAX_CONNECTIONS` permit; enough such
/// requests during one stalled migration exhaust every permit. Mirrors
/// discovery's own `OUTBOUND_IO_TIMEOUT`.
///
/// This is a *per-leg* bound, not a bound on the whole forwarding call —
/// `connect_and_authenticate` (dial + optional TLS + optional auth) and
/// `send_set`/the delete equivalent each apply it separately, so a joiner
/// that's merely slow (rather than fully unresponsive) at every leg could
/// still stack up to several multiples of this before failing. See
/// `FORWARD_TIMEOUT` for the bound that actually caps the whole call.
const OUTBOUND_IO_TIMEOUT: Duration = Duration::from_secs(10);
/// Caps the *entire* forward-to-joiner operation — dial, TLS, auth, and
/// the write/ack — that `set_on_joining_node`/`delete_on_joining_node`
/// run synchronously inside a client's connection task while a migration
/// is forwarding concurrent writes (see the `S`/`D` handling in
/// `handle_connection`). Without this outer bound, those calls only had
/// `OUTBOUND_IO_TIMEOUT` applied per leg (connect, then TLS, then auth,
/// then the set/delete round trip), so a joiner that's merely slow — not
/// unresponsive — at every leg could stall the client's whole pipeline
/// for close to 4x `OUTBOUND_IO_TIMEOUT`. Set equal to `OUTBOUND_IO_TIMEOUT`
/// so the worst case for a client write during migration is one multiple
/// of it, not several; `run_migration`'s own connection (a background
/// task, not inline with a client) intentionally keeps the per-leg
/// bounds instead, since it already retries per `KEY_TRANSFER_ATTEMPTS`
/// and stalling it doesn't hold a client's connection open.
const FORWARD_TIMEOUT: Duration = Duration::from_secs(10);
/// How long `run`'s accept loop pauses after an accept error worth
/// backing off from — see `should_backoff_after_accept_error`.
const ACCEPT_ERROR_BACKOFF: Duration = Duration::from_millis(100);
const READ_CHUNK_SIZE: usize = 1024;
/// How many times `run_migration` tries to transfer a single key to the
/// joining node (reconnecting between tries) before giving up on the
/// whole migration. See `run_migration`'s own doc comment.
const KEY_TRANSFER_ATTEMPTS: u32 = 3;
/// Issue #7: `handle_connection`'s read buffer grows (via `reserve`) to fit
/// whatever request it's mid-receiving, up to `MAX_REQUEST_SIZE`, but never
/// shrinks back on its own — a connection that ever sent one large request
/// keeps that capacity allocated for the rest of its (possibly long) life.
/// Once the buffer is fully drained (empty) and its capacity exceeds this,
/// it's reallocated back down rather than kept around on the chance of
/// another large request. Well above ordinary command sizes so typical
/// traffic never churns an allocation on every drained buffer.
const REQUEST_BUFFER_SHRINK_THRESHOLD: usize = 64 * 1024;

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
/// A bracketed IPv6 host (`[::1]:8356`, required so the port's `:` is
/// unambiguous) has its brackets stripped before conversion — left in,
/// `ServerName::try_from` rejects the string both as an IP (brackets
/// aren't part of the address) and as a DNS name (`[`/`]` aren't valid
/// there either), so TLS to an IPv6 address would otherwise always fail.
/// Mirrors `nanocached-discovery`'s own copy in `src/bin/nanocached-discovery.rs`.
fn server_name_from_addr(addr: &str) -> io::Result<ServerName<'static>> {
    let host = addr.rsplit_once(':').map_or(addr, |(host, _)| host);
    let host = host
        .strip_prefix('[')
        .and_then(|host| host.strip_suffix(']'))
        .unwrap_or(host);

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
    /// discovery server (staged node join / node identity decoupled from address) — an `M` arriving otherwise has
    /// nowhere sensible to report `C` to and is rejected.
    node_context: Option<NodeContext>,
    /// Where `handle_connection` hands off a `run_migration` future for
    /// `run`'s own loop to `connection_tasks.spawn` — spawning it directly
    /// from inside a connection task (as opposed to from `run`) would leave
    /// it untracked by `connection_tasks`, so graceful shutdown couldn't
    /// wait for it (or ask it to unwind cleanly) before the process exits.
    migration_tx: mpsc::Sender<MigrationTask>,
}

/// What an staged node join migration task (triggered by an incoming `M`) needs
/// beyond the cache itself: this node's own identity, and how to reach
/// the discovery server to report `C` once the handoff is done.
#[derive(Clone)]
struct NodeContext {
    /// This node's own random per-process identity (node identity decoupled from address), needed to
    /// identify this node as the sender when it reports `C`.
    name: String,
    /// This node's per-process membership token (issue #34), generated
    /// alongside `name` with the same lifetime and never persisted or
    /// shared with anything but the discovery servers — presented on
    /// `J`/`P`/`H`/`C` so nothing that merely knows this node's public
    /// name (every `L` response lists it) can speak for it.
    token: String,
    discovery_addr: String,
    /// Set while `run_migration` is active, cleared when it finishes (see
    /// `MigrationGuard`). Serves two purposes: `run_sweep` checks it and
    /// skips its pass while it's `Some`, per staged node join — a marked-but-not-
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
/// (staged node join) using a random per-process name (node identity decoupled from address) and, once
/// promoted, sends a heartbeat declaring that name on `interval`, well
/// under the discovery server's own liveness timeout.
pub(crate) struct HeartbeatConfig {
    /// One or more discovery replicas (discovery HA). The first is the
    /// primary — the only one this node ever sends `J` (and `C`) to;
    /// the rest learn about this node via `P` announces once the primary
    /// has promoted it. Never empty (main.rs validates).
    pub(crate) discovery_addrs: Vec<String>,
    /// The port this node serves on. `J`/`P` carry only this; the
    /// discovery server derives the full address from the registration
    /// connection's source IP (addresses derived from the registration connection), so there is nothing to
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

/// Whether `error` is the OS reporting the process (EMFILE) or the whole
/// system (ENFILE) is out of file descriptors — the two `accept` failures
/// worth backing off from rather than retrying immediately. Unix-only:
/// there's no portable stable `ErrorKind` for either, and this project's
/// Docker images and dev platforms (Linux, macOS) share these errno
/// values.
fn is_fd_exhaustion(error: &io::Error) -> bool {
    #[cfg(unix)]
    {
        matches!(error.raw_os_error(), Some(23) | Some(24)) // ENFILE, EMFILE
    }

    #[cfg(not(unix))]
    {
        let _ = error;
        false
    }
}

/// Whether `run`'s accept loop should pause briefly (`ACCEPT_ERROR_BACKOFF`,
/// below) after `error` rather than immediately retrying `accept`. On Unix
/// this is exactly `is_fd_exhaustion`: any other accept failure there
/// (`ECONNABORTED` and friends) is typically a one-off, per-connection
/// condition safe to retry right away. `is_fd_exhaustion` is hard-coded
/// `false` on non-Unix targets (no portable stable `ErrorKind` for the
/// underlying errno — see its own doc comment), which would otherwise
/// leave every accept error on those targets retried with *no* backoff at
/// all — including a persistent one (the process genuinely is out of
/// descriptors, or some other sustained condition), busy-looping this
/// branch hot instead of ever yielding. Back off on every accept error on
/// non-Unix instead: more conservative than Unix's precise check (an
/// occasional one-off failure there now also pays the pause), but a
/// bounded 100ms delay per failed accept is a small cost to avoid an
/// unbounded busy-loop on a sustained one.
fn should_backoff_after_accept_error(error: &io::Error) -> bool {
    is_fd_exhaustion(error) || cfg!(not(unix))
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
    let per_ip_connections: PerIpConnections = Arc::new(Mutex::new(HashMap::new()));
    let mut connection_tasks = JoinSet::new();
    let (shutdown_tx, shutdown_rx) = watch::channel(false);

    let cache_task = tokio::spawn(run_cache(request_rx, max_memory_bytes));

    // Shared with `node_context` (when this node has one) so `run_sweep`
    // can tell whether an staged node join handoff this node is the source for is
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

    // Generated once and kept for this process's lifetime (node identity decoupled from address): a
    // restarted node has no data to reclaim its old identity for, so
    // there's nothing a stable name would preserve across a restart that
    // isn't already lost anyway. Only meaningful when this node registers
    // with a discovery server at all.
    let node_context = heartbeat.as_ref().map(|config| NodeContext {
        name: Uuid::new_v4().to_string(),
        // A second, independent UUID: the name is public (`L` lists it),
        // so it can't double as the credential proving this node is the
        // one behind it (issue #34).
        token: Uuid::new_v4().to_string(),
        // The primary (discovery HA) — where `C` completion reports go,
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
            node_context.token.clone(),
            Arc::clone(&known_ring),
            shutdown_rx.clone(),
        ))),
        _ => None,
    };

    // Buffered rather than unbounded: staged node join allows only one migration in
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
                let (stream, address) = match result {
                    Ok(pair) => pair,
                    Err(error) => {
                        // A single failed `accept` (ECONNABORTED, EMFILE,
                        // ENFILE, ENOBUFS, ...) used to be propagated with
                        // `?`, taking the whole listener — and every
                        // connection it was already serving — down over
                        // what is usually a transient, per-attempt
                        // condition. Log and keep the loop running instead.
                        eprintln!("WARN accept failed: {error}");

                        // EMFILE/ENFILE (Unix) mean this process (or the
                        // system) is out of file descriptors; looping
                        // straight back to `accept` would spin this
                        // branch hot until one frees up, so back off
                        // briefly rather than busy-looping. Non-Unix
                        // targets can't identify that specific condition
                        // (see `is_fd_exhaustion`) and back off on every
                        // accept error instead — see
                        // `should_backoff_after_accept_error`.
                        if should_backoff_after_accept_error(&error) {
                            sleep(ACCEPT_ERROR_BACKOFF).await;
                        }

                        continue;
                    }
                };

                dispatch_connection(
                    stream,
                    address,
                    request_tx.clone(),
                    Arc::clone(&connection_limit),
                    Arc::clone(&per_ip_connections),
                    connection_config.clone(),
                    shutdown_rx.clone(),
                    &mut connection_tasks,
                );
            }

        }
    }

    // Keep servicing `migration_rx` while draining: a connection task that
    // is mid-request when shutdown lands may still hand a forwarded write
    // (or the tail of a handoff) to this channel, and with the main loop
    // gone nobody would spawn it — the client would have its `S`/`D` acked
    // and the forward silently dropped, and with the 4-slot buffer full
    // the sender would block until the `abort_all` below killed it.
    let connections_finished = timeout(SHUTDOWN_TIMEOUT, async {
        while !connection_tasks.is_empty() {
            tokio::select! {
                result = connection_tasks.join_next() => {
                    if let Some(Err(error)) = result {
                        eprintln!("WARN connection task failed: {error}");
                    }
                }
                Some(task) = migration_rx.recv() => {
                    connection_tasks.spawn(task);
                }
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

/// Live connection counts per source IP, backing `MAX_CONNECTIONS_PER_IP`
/// (see that constant). A plain `Mutex<HashMap<..>>` rather than anything
/// fancier: every access here is a brief increment/decrement with no I/O
/// under the lock, and every accepted connection already pays for a
/// `Semaphore` acquisition on the shared `connection_limit`, so this adds
/// no bottleneck relative to that existing one.
type PerIpConnections = Arc<Mutex<HashMap<IpAddr, usize>>>;

/// Releases one `MAX_CONNECTIONS_PER_IP` slot on drop — the per-IP
/// counterpart to the `Semaphore` permit `dispatch_connection` already
/// holds for `MAX_CONNECTIONS` (`_connection_permit`, which frees itself
/// the same way).
struct PerIpConnectionGuard {
    counts: PerIpConnections,
    ip: IpAddr,
}

impl Drop for PerIpConnectionGuard {
    fn drop(&mut self) {
        let mut counts = self
            .counts
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());

        if let Some(count) = counts.get_mut(&self.ip) {
            *count -= 1;
            if *count == 0 {
                // Don't let a long-lived server accumulate one entry per
                // distinct IP that has ever connected, most of which will
                // never connect again.
                counts.remove(&self.ip);
            }
        }
    }
}

/// Reserves one of `MAX_CONNECTIONS_PER_IP` slots for `ip`, or `None` if
/// it's already at the cap — see `MAX_CONNECTIONS_PER_IP`.
fn try_acquire_per_ip(counts: &PerIpConnections, ip: IpAddr) -> Option<PerIpConnectionGuard> {
    let mut guard = counts
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());

    let count = guard.entry(ip).or_insert(0);
    if *count >= MAX_CONNECTIONS_PER_IP {
        return None;
    }
    *count += 1;
    drop(guard);

    Some(PerIpConnectionGuard {
        counts: Arc::clone(counts),
        ip,
    })
}

/// Best-effort "Busy" reply on `stream` before the caller drops it —
/// shared by every over-limit rejection in `dispatch_connection`
/// (`MAX_CONNECTIONS` and, per source IP, `MAX_CONNECTIONS_PER_IP`). A
/// TLS-configured server has no plaintext channel to answer on before the
/// handshake completes (TLS support: no plaintext fallback once TLS is set)
/// — it just closes. A plaintext server can still reply on the raw
/// stream. Bounded by `TLS_HANDSHAKE_TIMEOUT` (reused rather than a new
/// constant: a peer that never reads this reply must not leak the task by
/// leaving the write pending indefinitely — the same reasoning as the
/// handshake itself).
async fn reject_over_limit(
    mut stream: TcpStream,
    address: SocketAddr,
    tls_acceptor: &Option<TlsAcceptor>,
) {
    if tls_acceptor.is_none() {
        let busy = Response::Busy.encode();

        match timeout(TLS_HANDSHAKE_TIMEOUT, stream.write_all(&busy)).await {
            Ok(Ok(())) => {}
            Ok(Err(error)) => {
                eprintln!("WARN failed to send busy response to {address}: {error}");
            }
            Err(_) => {
                eprintln!("WARN sending busy response to {address} timed out");
            }
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn dispatch_connection(
    stream: TcpStream,
    address: SocketAddr,
    request_tx: mpsc::Sender<CacheRequest>,
    connection_limit: Arc<Semaphore>,
    per_ip_connections: PerIpConnections,
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
        // Issue #5: acquired *before* the TLS handshake (previously
        // after), so a peer can't spend handshake CPU/fds past
        // `MAX_CONNECTIONS` just by dialing and stalling — only a
        // permit-holding connection ever performs one.
        let permit = match connection_limit.try_acquire_owned() {
            Ok(permit) => permit,
            Err(_) => {
                reject_over_limit(stream, address, &config.tls_acceptor).await;
                return;
            }
        };

        // No per-source-IP connection limit: without this, a single
        // source could hold `MAX_CONNECTIONS` connections all by itself
        // and starve every other client, even though the global
        // semaphore above isn't literally exhausted until the very last
        // one. Reserved before the TLS handshake for the same reason as
        // the global permit (issue #5) — see `MAX_CONNECTIONS_PER_IP`.
        let per_ip_permit = match try_acquire_per_ip(&per_ip_connections, address.ip()) {
            Some(permit) => permit,
            None => {
                reject_over_limit(stream, address, &config.tls_acceptor).await;
                return;
            }
        };

        let stream: ServerStream = match &config.tls_acceptor {
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

        println!("INFO accepted connection from {address}");

        let _connection_permit = permit;
        let _per_ip_permit = per_ip_permit;

        if let Err(error) =
            handle_connection(stream, address, request_tx, config, shutdown_rx).await
        {
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

/// Bounds every response write in `handle_connection` (issue #4): the read
/// side already has `IDLE_TIMEOUT`, but an unbounded `write_all` let a peer
/// that stops reading (without closing the TCP connection — e.g. a full
/// receive buffer) hold this connection's `MAX_CONNECTIONS` permit forever.
/// Uses `WRITE_TIMEOUT` rather than reusing `IDLE_TIMEOUT`: the two are
/// different failure modes (a normal gap between requests vs. a peer that
/// isn't draining its receive buffer at all), and reusing the 60s read
/// timeout let a stuck write hold a permit far longer than necessary.
async fn write_response(stream: &mut ServerStream, data: &[u8]) -> io::Result<()> {
    timeout(WRITE_TIMEOUT, stream.write_all(data))
        .await
        .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "write timed out"))?
}

/// Echoed response tags: a `G`/`S`/`D` response on a tagged-mode connection echoes
/// the request's tag; untagged connections keep the original encoding.
fn encode_response(response: &Response, tag: Option<u32>) -> Vec<u8> {
    match tag {
        Some(tag) => response.encode_with_tag(tag),
        None => response.encode(),
    }
}

async fn handle_connection(
    mut stream: ServerStream,
    address: SocketAddr,
    request_tx: mpsc::Sender<CacheRequest>,
    config: ConnectionConfig,
    mut shutdown_rx: watch::Receiver<bool>,
) -> io::Result<()> {
    let mut received = BytesMut::new();
    // No secret configured means auth isn't required, so every connection
    // starts already authenticated.
    let mut authenticated = config.auth_secret.is_none();

    // Slowloris resistance: anchored to accept-time here, then re-anchored
    // to `now + config.idle_timeout` below every time `parse` completes a
    // full command — see the comment there and `IDLE_TIMEOUT`'s own doc
    // comment for why this is `now + idle_timeout` at those two moments
    // specifically, rather than on every byte read.
    let mut deadline = Instant::now() + config.idle_timeout;

    // Echoed response tags: set once an `A ... T` is accepted. From then on every
    // request must carry a trailing tag (`parse_tagged`) and every
    // `G`/`S`/`D` response echoes it, so the client's read loop can
    // verify request/response alignment before dispatching.
    let mut tagged = false;

    // See `command::parse_resumable`: reset by it on anything but
    // `Incomplete`, which is exactly when `received`'s front changes.
    let mut parse_progress = MigrateProgress::default();

    loop {
        let parsed = parse_resumable(&mut received, tagged, &mut parse_progress);

        // Only a fully parsed command extends the deadline — an
        // `Incomplete` result (more bytes needed) leaves it untouched, so
        // a client that trickles bytes in without ever finishing a
        // command can't renew its own budget one byte at a time. See
        // `IDLE_TIMEOUT`'s doc comment.
        if parsed.is_ok() {
            deadline = Instant::now() + config.idle_timeout;
        }

        match parsed {
            Ok((Command::Auth { secret, tagging }, _)) => {
                let accepted = match &config.auth_secret {
                    Some(expected) => constant_time_eq(&secret, expected),
                    None => true,
                };

                // The identity reply echoes the tag capability only to a
                // client that asked for it (echoed response tags) — a plain `A` keeps
                // the exact three-byte reply older SDKs hard-read.
                let identity = |response: &Response| {
                    if tagging {
                        response.encode_identity_tagged()
                    } else {
                        response.encode()
                    }
                };

                if accepted {
                    authenticated = true;
                    tagged = tagging;
                    write_response(&mut stream, &identity(&Response::AuthOk)).await?;
                    continue;
                }

                write_response(&mut stream, &identity(&Response::Unauthorized)).await?;
                return Err(io::Error::new(
                    io::ErrorKind::PermissionDenied,
                    "invalid auth secret",
                ));
            }
            Ok(_) if !authenticated => {
                write_response(&mut stream, &Response::Unauthorized.encode()).await?;
                return Err(io::Error::new(
                    io::ErrorKind::PermissionDenied,
                    "command sent before authenticating",
                ));
            }
            Ok((
                Command::Migrate {
                    token,
                    joining_name,
                    joining_addr,
                    joined,
                    replication,
                },
                _,
            )) => {
                let Some(node_context) = config.node_context.clone() else {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "received M but this node isn't configured with a discovery server",
                    ));
                };

                // The shared secret proves only "cluster member" (shared-secret authentication),
                // not "the discovery server" — so without this every client
                // holding it could send `M` and make this node stream its
                // cache to an attacker-chosen address. Discovery echoes this
                // node's own membership token (issue #34) on `M`; only a
                // discovery server this node registered with knows it (no
                // client does, and it's never sent back out — per-node membership tokens), so
                // a mismatch means the sender isn't discovery. Reject loudly.
                if !constant_time_eq(token.as_bytes(), node_context.token.as_bytes()) {
                    eprintln!(
                        "WARN rejected M from {address}: membership token mismatch \
                         (sender is not this node's discovery server)"
                    );
                    write_response(&mut stream, &Response::MigrationRejected.encode()).await?;
                    return Err(io::Error::new(
                        io::ErrorKind::PermissionDenied,
                        "M carried the wrong membership token",
                    ));
                }

                let (before_ring, after_ring) =
                    migration_rings(&node_context, &joining_name, &joined);
                let after_ring = Arc::new(after_ring);

                // Reserved *before* acknowledging `M`, not inside
                // `run_migration` after the fact: only this ordering lets
                // a conflicting `M` (issue #3 — this node's single
                // migration slot already occupied) be reported as
                // `MigrationRejected` on the same ack, instead of telling
                // the sender `MigrationAccepted` and then silently doing
                // nothing.
                let migration_guard = match MigrationGuard::new(
                    Arc::clone(&node_context.active_migration),
                    joining_name.clone(),
                    joining_addr.clone(),
                    Arc::clone(&after_ring),
                    replication,
                ) {
                    MigrationOutcome::New(guard) => guard,
                    // A discovery retry of `M` for the handoff already
                    // running here (its ack was lost in transit) — re-ack
                    // with the same count the original `M` computed
                    // instead of starting a second migration. See
                    // `MigrationGuard::new`'s doc comment.
                    MigrationOutcome::DuplicateAcked(entries) => {
                        write_response(&mut stream, &Response::MigrationAccepted(entries).encode())
                            .await?;
                        continue;
                    }
                    MigrationOutcome::Conflict => {
                        write_response(&mut stream, &Response::MigrationRejected.encode()).await?;
                        continue;
                    }
                };

                // Size-derived migration timeout: sizes discovery's migration timeout.
                // Issue (perf): one `list_keys` snapshot here, reused by
                // both `entries_to_send_count` (for this count) and
                // `run_migration` (for the actual transfer) below,
                // instead of each independently cloning the whole cache —
                // see `entries_to_send_count`'s own comment. `None` if
                // the cache task is already gone (shutting down); 0 is a
                // safe default for the count here since `run_migration`
                // independently checks for the same `None` and aborts on
                // its own.
                let keys_snapshot = list_keys(&node_context.request_tx).await;
                let entries_to_send = keys_snapshot.as_deref().map_or(0, |keys| {
                    entries_to_send_count(
                        keys,
                        &before_ring,
                        &after_ring,
                        &node_context.name,
                        &joining_name,
                        replication,
                    )
                });

                // Stamped before acking, not after, so a retry of this
                // same `M` arriving right after the ack goes out (but
                // before this stamp landed) still finds `acked_entries`
                // set — see `MigrationGuard::new`.
                migration_guard.ack(entries_to_send);

                write_response(
                    &mut stream,
                    &Response::MigrationAccepted(entries_to_send).encode(),
                )
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
                        replication,
                        before_ring,
                        after_ring,
                        migration_guard,
                        keys_snapshot,
                    )))
                    .await;

                continue;
            }
            Ok((
                Command::CancelMigration {
                    token,
                    joining_name,
                },
                _,
            )) => {
                let Some(node_context) = config.node_context.clone() else {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "received X but this node isn't configured with a discovery server",
                    ));
                };

                // Same authorization as `M` (see there): without the token
                // check any client holding the shared secret could abort a
                // legitimate in-flight handoff just by sending `X`.
                if !constant_time_eq(token.as_bytes(), node_context.token.as_bytes()) {
                    eprintln!(
                        "WARN rejected X from {address}: membership token mismatch \
                         (sender is not this node's discovery server)"
                    );
                    return Err(io::Error::new(
                        io::ErrorKind::PermissionDenied,
                        "X carried the wrong membership token",
                    ));
                }

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

                write_response(&mut stream, &Response::MigrationCancelled.encode()).await?;

                continue;
            }
            Ok((Command::Get { key }, tag)) => {
                if let Some(node_context) = &config.node_context
                    && wrong_node(node_context, &key)
                {
                    write_response(&mut stream, &encode_response(&Response::WrongNode, tag))
                        .await?;
                    continue;
                }

                let response = execute_command(&request_tx, Command::Get { key }).await?;
                write_response(&mut stream, &encode_response(&response, tag)).await?;

                continue;
            }
            Ok((Command::Set { key, value, ttl }, tag)) => {
                if let Some(node_context) = &config.node_context
                    && wrong_node(node_context, &key)
                {
                    write_response(&mut stream, &encode_response(&Response::WrongNode, tag))
                        .await?;
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
                write_response(&mut stream, &encode_response(&response, tag)).await?;

                // Staged node join: this key may be one an in-progress handoff is
                // moving to a joining node — see `migration_target_for`.
                if let Some(node_context) = &config.node_context
                    && let Some(target) = migration_target_for(node_context, &key)
                {
                    // Handed to `run`'s own loop via `migration_tx`
                    // (mirroring the `M` handler above), not awaited
                    // inline — see `forward_with_retries`'s own doc
                    // comment for why.
                    let _ = config
                        .migration_tx
                        .send(Box::pin(forward_with_retries(
                            node_context.clone(),
                            target,
                            OwnedForwardedWrite::Set {
                                key: key.clone(),
                                value: value.clone(),
                                ttl,
                            },
                        )))
                        .await;
                }

                continue;
            }
            Ok((Command::Delete { key }, tag)) => {
                if let Some(node_context) = &config.node_context
                    && wrong_node(node_context, &key)
                {
                    write_response(&mut stream, &encode_response(&Response::WrongNode, tag))
                        .await?;
                    continue;
                }

                let response =
                    execute_command(&request_tx, Command::Delete { key: key.clone() }).await?;
                write_response(&mut stream, &encode_response(&response, tag)).await?;

                if let Some(node_context) = &config.node_context
                    && let Some(target) = migration_target_for(node_context, &key)
                {
                    let _ = config
                        .migration_tx
                        .send(Box::pin(forward_with_retries(
                            node_context.clone(),
                            target,
                            OwnedForwardedWrite::Delete { key: key.clone() },
                        )))
                        .await;
                }

                continue;
            }
            Ok((command, _)) => {
                // `ListEntries`/`MarkMigrated`/`UnmarkMigrated`/`Sweep`/
                // `PeekEntry`: internal-only, constructed directly by
                // server-side tasks, never by `parse()` — this arm exists
                // only so the match stays exhaustive.
                let response = execute_command(&request_tx, command).await?;
                write_response(&mut stream, &response.encode()).await?;

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

        // Issue #6: checked here — only once `parse` has drained every
        // complete command already buffered (an `Incomplete` result means
        // there isn't one) — rather than at the top of the loop, so a
        // shutdown signal that arrives mid-pipeline doesn't silently drop
        // a second/third request that arrived in the same read as the
        // first and needs no further I/O to answer.
        if *shutdown_rx.borrow() {
            return Ok(());
        }

        // Issue #7: release an oversized buffer once its unparsed
        // remainder drops back under the threshold, instead of only when
        // it's exactly empty — a connection whose next command's lead
        // byte always lands in the same read as the previous command's
        // tail would otherwise never hit an exact-empty check and carry
        // the oversized allocation for the rest of the connection's life.
        if received.capacity() > REQUEST_BUFFER_SHRINK_THRESHOLD
            && received.len() <= REQUEST_BUFFER_SHRINK_THRESHOLD
        {
            let mut shrunk = BytesMut::with_capacity(received.len());
            shrunk.extend_from_slice(&received);
            received = shrunk;
        }

        received.reserve(READ_CHUNK_SIZE);

        // Bounded by time remaining until `deadline`, not by a fresh
        // `config.idle_timeout` on every read — see `deadline`'s own
        // comment above. If `deadline` has already passed (a trickled
        // read landed after it), `remaining` is zero and this fires
        // immediately instead of granting another read.
        let remaining = deadline.saturating_duration_since(Instant::now());

        let bytes_read = tokio::select! {
            _ = shutdown_rx.changed() => return Ok(()),

            result = timeout(remaining, stream.read_buf(&mut received)) => {
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

/// Staged node join: sent once per connection, before any heartbeat. `name` is
/// this node's random per-process identity (node identity decoupled from address); `port` is where it
/// serves — the discovery server composes the reachable address from this
/// connection's own source IP plus that port (addresses derived from the registration connection). Discovery holds
/// this connection open and pushes `R\n` on it once this node is promoted
/// to `Joined`.
/// `token` is this node's per-process membership token (issue #34),
/// generated alongside `name` and presented on every command naming this
/// node — the discovery server binds it to `name` at registration and
/// rejects any later `P`/`H`/`C` for the name that doesn't present it.
fn join_message(name: &str, port: u16, token: &str) -> Vec<u8> {
    let mut message = format!("J {} {port} {}\n", name.len(), token.len()).into_bytes();
    message.extend_from_slice(name.as_bytes());
    message.extend_from_slice(token.as_bytes());
    message
}

/// Discovery HA: same shape as `join_message`, but declares an
/// already-promoted member — no handoff orchestration on the other end.
fn announce_message(name: &str, port: u16, token: &str) -> Vec<u8> {
    let mut message = format!("P {} {port} {}\n", name.len(), token.len()).into_bytes();
    message.extend_from_slice(name.as_bytes());
    message.extend_from_slice(token.as_bytes());
    message
}

/// Only valid once this node has been promoted to `Joined` (staged node join); the
/// address was already established by `join_message` on this connection,
/// so a heartbeat only needs to carry `name` to refresh liveness.
/// `replication` is this node's current belief about the cluster's
/// replication factor (issue #30), encoded as `0` when it doesn't have
/// one yet — never a real replication factor (discovery validates every
/// `--replication-factor` is at least 1), so it's an unambiguous "unknown"
/// sentinel on the wire.
fn heartbeat_message(name: &str, replication: Option<usize>, token: &str) -> Vec<u8> {
    let mut message = format!(
        "H {} {} {}\n",
        name.len(),
        replication.unwrap_or(0),
        token.len()
    )
    .into_bytes();
    message.extend_from_slice(name.as_bytes());
    message.extend_from_slice(token.as_bytes());
    message
}

fn auth_message(secret: &[u8]) -> Vec<u8> {
    let mut message = format!("A {}\n", secret.len()).into_bytes();
    message.extend_from_slice(secret);
    message
}

/// Connects out to `addr` as a client — either the discovery server (for
/// heartbeats, staged node join's `J`/`C`) or another node (for staged node join's
/// `SET`-based handoff) — upgrading to TLS first if `tls_connector` is
/// set. There is no plaintext fallback: if TLS is configured and the
/// handshake fails, the connection attempt fails too.
async fn connect_client_stream(
    addr: &str,
    tls_connector: Option<&TlsConnector>,
) -> io::Result<ClientStream> {
    let stream = timeout(OUTBOUND_IO_TIMEOUT, TcpStream::connect(addr))
        .await
        .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "outbound connect timed out"))??;
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
/// (discovery HA). The primary is the only one ever sent `J` — the staged node join
/// join, with its data handoff — and flips the shared `promoted` flag
/// once `R` arrives for it. Standbys (and the primary itself, on any
/// re-registration after that first promotion) send `P` announces, which
/// upsert this node as a member with no handoff.
enum DiscoveryRole {
    Primary(Arc<watch::Sender<bool>>),
    Standby(watch::Receiver<bool>),
}

/// Registers this node with every configured discovery replica
/// (discovery HA): one `register_with_discovery` task per address, sharing a
/// `promoted` watch so standbys hold off announcing until the primary's
/// Staged node join join has actually completed. `name` is this node's own
/// Node identity decoupled from address identity, generated once by `run` and shared with
/// `ConnectionConfig`'s `NodeContext` — not generated here, so a
/// migration task triggered by an incoming `M` on some other connection
/// reports `C` under the same name these tasks register as.
async fn send_heartbeats(
    config: HeartbeatConfig,
    name: String,
    token: String,
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
            token.clone(),
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
    token: String,
    known_ring: KnownRing,
    mut role: DiscoveryRole,
    mut shutdown_rx: watch::Receiver<bool>,
) {
    let join = join_message(&name, port, &token);
    let announce = announce_message(&name, port, &token);

    // A standby must not announce a node the primary hasn't promoted yet:
    // that would make it visible in the standby's `L` before its staged node join
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

        // Bounded by `OUTBOUND_IO_TIMEOUT` + `TLS_HANDSHAKE_TIMEOUT` on its
        // own, but that is longer than `SHUTDOWN_TIMEOUT` and `run` awaits
        // this task without one — so also give up the moment shutdown
        // lands, rather than making a node whose discovery server is
        // unreachable take ~30s to exit.
        let connected = tokio::select! {
            _ = shutdown_rx.changed() => return,
            result = connect_client_stream(&discovery_addr, tls_connector.as_ref()) => result,
        };

        match connected {
            Ok(mut stream) => {
                let authenticated = match &auth_secret {
                    Some(secret) => {
                        let auth = auth_message(secret);
                        // Bounded so a discovery server that accepts TCP but
                        // never drains can't wedge this task forever (and,
                        // via `heartbeat_task.await`, hang shutdown).
                        timeout(OUTBOUND_IO_TIMEOUT, async {
                            stream.write_all(&auth).await?;
                            let mut ack = [0u8; 3];
                            stream.read_exact(&mut ack).await?;
                            io::Result::Ok(&ack == b"Od\n")
                        })
                        .await
                        .is_ok_and(|result| result.unwrap_or(false))
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

                let registration_sent = authenticated
                    && matches!(
                        timeout(OUTBOUND_IO_TIMEOUT, stream.write_all(registration)).await,
                        Ok(Ok(()))
                    );
                if registration_sent {
                    // For `J`, staged node join: this connection is held open by
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
                            // only once it has sent its own first client-side replication
                            // handoff `M` (issue #30) — a stale precomputed
                            // buffer would keep reporting "unknown" forever.
                            let replication = known_ring
                                .lock()
                                .unwrap_or_else(|poisoned| poisoned.into_inner())
                                .as_ref()
                                .map(|membership| membership.replication);
                            let heartbeat = heartbeat_message(&name, replication, &token);

                            if !matches!(
                                timeout(OUTBOUND_IO_TIMEOUT, stream.write_all(&heartbeat)).await,
                                Ok(Ok(()))
                            ) {
                                break;
                            }

                            // Timed out like the write above: without this,
                            // a discovery server that accepts the
                            // heartbeat but never acks (crashed-but-
                            // socket-open, blackholed route) would hang
                            // this loop forever instead of falling through
                            // to the redial below.
                            let mut ack = [0u8; 2];
                            let read_ack = tokio::select! {
                                _ = shutdown_rx.changed() => return,
                                result = timeout(OUTBOUND_IO_TIMEOUT, stream.read_exact(&mut ack)) => result,
                            };

                            if !matches!(read_ack, Ok(Ok(_))) || &ack != b"A\n" {
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

/// Staged node join: propagates a client's `D` for a key an in-progress handoff
/// is moving to the joining node too (see `forward_delete_to_joining_node`).
fn delete_message(key: &[u8]) -> Vec<u8> {
    let mut message = format!("D {}\n", key.len()).into_bytes();
    message.extend_from_slice(key);
    message
}

/// Staged node join: reports to discovery that this node (identified by `name`,
/// Node identity decoupled from address) has finished handing off its share of the current join.
/// `C <name-len> <joining-len> <token-len>\n<name><joining><token>` — the
/// completion report names both the reporting node and the join it is
/// for (issue #5): a bare name let a stale report from an abandoned
/// handoff be credited to whatever join happened to be pending next.
/// `token` proves the report really comes from `name` (issue #34).
fn complete_message(name: &str, joining_name: &str, token: &str) -> Vec<u8> {
    let mut message =
        format!("C {} {} {}\n", name.len(), joining_name.len(), token.len()).into_bytes();
    message.extend_from_slice(name.as_bytes());
    message.extend_from_slice(joining_name.as_bytes());
    message.extend_from_slice(token.as_bytes());
    message
}

/// This node's view of cluster membership plus the replication factor it
/// came with (client-side replication) — the two always travel together, since "is this
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
    /// Client-side replication: discovery's replication factor, carried by the `M` that
    /// started this handoff — membership in the joining node's copy set
    /// is "in the key's top-R", not "is the key's owner".
    replication: usize,
    /// `None` while this node's own transfer is running; `Some(when)`
    /// once it finished successfully. Issue #3: discovery only publishes
    /// the joiner after EVERY ready node reports `C`, so this node must
    /// keep forwarding concurrent writes to the joiner after its own
    /// share is done — a still-stale client that can't see the joiner in
    /// `L` yet would otherwise write to this node without the joiner ever
    /// learning of it. The window is bounded by `forwarding_grace`
    /// (matching discovery's migration timeout: past it the join has
    /// either completed cluster-wide or been abandoned).
    completed_at: Option<Instant>,
    /// This handoff's own size-derived grace window (`forwarding_grace`),
    /// set together with `completed_at` — not a shared constant, since
    /// different handoffs move different amounts of data. Meaningless
    /// (left at `Duration::ZERO`) until `completed_at` is `Some`.
    forwarding_grace: Duration,
    /// Stamped by `MigrationGuard::ack` once the `M` handler has computed
    /// the entry count it acks (`entries_to_send_count`) — that snapshot
    /// happens after this slot is reserved, so it can't be filled in at
    /// construction. `None` for the brief window before that stamp lands.
    /// Lets `MigrationGuard::new` answer a duplicate `M` for this same
    /// `joining_name` (a discovery retry after a lost ack) with the same
    /// `A <entries>` ack again — see `MigrationOutcome::DuplicateAcked`.
    acked_entries: Option<usize>,
    abort_requested: Arc<AtomicBool>,
    /// Persistent connection to the joining node, shared by every
    /// `set_on_joining_node`/`delete_on_joining_node` call this handoff's
    /// concurrent client writes trigger (see `migration_target_for` and
    /// `ForwardTarget`) — issue: those two used to call
    /// `connect_and_authenticate` fresh per forwarded write, opening (and
    /// leaking to TIME_WAIT) a new TCP+TLS+Auth connection every time,
    /// the same ephemeral-port exhaustion `run_migration`'s own bulk
    /// transfer already learned to avoid by reusing one connection for
    /// every key (see this struct's own doc comment on that, and
    /// `run_migration`'s). `None` when there is no live connection yet,
    /// or right after an I/O error drops the last one; the next forward
    /// reconnects. A `tokio::sync::Mutex`, not `std::sync`, since a
    /// connection is held across the `.await`s of writing a command and
    /// reading its ack, one forwarded write at a time (concurrent
    /// forwards for this same handoff simply queue behind each other —
    /// they'd have to serialize on the wire regardless, being one TCP
    /// stream). Deliberately scoped per-`ActiveMigration`, not shared
    /// across migrations: a fresh migration means a different joining
    /// node, so it must start with no connection rather than possibly
    /// reusing one dialed to whatever node the previous handoff targeted.
    forward_connection: Arc<AsyncMutex<Option<ClientStream>>>,
}

/// Occupies `slot` with an `ActiveMigration` for this guard's lifetime
/// (cleared back to `None` on drop — including an early return or panic),
/// so `run_sweep` can tell to pause, an incoming `X` can find this
/// handoff to cancel it, and a concurrent client write can find it to
/// forward. Exposes `abort_requested` so `run_migration` can poll it
/// directly without re-locking `slot` on every entry. Holds an owned
/// clone of `NodeContext::active_migration`'s `Arc` (rather than
/// borrowing `NodeContext`) so `handle_connection` can create and hold
/// this guard *before* handing the migration off to `run_migration` —
/// see the accept/reject ordering note on `Response::MigrationRejected`.
struct MigrationGuard {
    slot: Arc<Mutex<Option<ActiveMigration>>>,
    abort_requested: Arc<AtomicBool>,
    /// Set by `completed()` so `Drop` leaves the slot's completion info
    /// (stamped by `completed()` itself) intact instead of clearing it.
    completed: bool,
}

/// What `MigrationGuard::new` found when it tried to reserve `slot` for
/// an incoming `M` — see its doc comment for the reasoning behind each
/// case.
enum MigrationOutcome {
    /// A fresh slot was reserved; the caller should ack with the entry
    /// count it computes, then stamp it via `MigrationGuard::ack`.
    New(MigrationGuard),
    /// This `M` names the same `joining_name` as the handoff already
    /// occupying the slot, and that handoff has already stamped what it
    /// acked — a discovery retry after a lost ack. The caller should
    /// resend `MigrationAccepted` with this same count rather than start
    /// a second migration.
    DuplicateAcked(usize),
    /// The slot is occupied by a handoff for a different `joining_name`,
    /// or by the same `joining_name` whose `acked_entries` hasn't been
    /// stamped yet. The caller should reject with `MigrationRejected`.
    Conflict,
}

#[cfg(test)]
impl MigrationOutcome {
    /// Test-only convenience: most tests only care about the successful
    /// path, so this saves a `match` at every one of those call sites.
    fn unwrap_new(self) -> MigrationGuard {
        match self {
            MigrationOutcome::New(guard) => guard,
            MigrationOutcome::DuplicateAcked(entries) => {
                panic!("expected a new migration guard, got a duplicate ack for {entries} entries")
            }
            MigrationOutcome::Conflict => panic!("expected a new migration guard, got a conflict"),
        }
    }
}

impl MigrationGuard {
    /// Reserves `slot` for a new handoff, or reports why it couldn't
    /// (issue #3): unconditionally overwriting an occupied slot would
    /// clobber the active migration's
    /// `completed_at`/`forwarding_grace`/`abort_requested` out from under
    /// its own still-running `run_migration` task (or its post-completion
    /// forwarding window), corrupting `migration_target_for` and `X`/`C`
    /// matching for whichever migration loses the slot.
    ///
    /// A second `M` for the same `joining_name` — a discovery retry after
    /// a lost ack (`send_migrate_with_retry`) — is the expected way to
    /// hit an occupied slot, and is handled idempotently: once the
    /// original `M`'s handler has stamped `acked_entries` (see
    /// `MigrationGuard::ack`), the retry gets back
    /// `MigrationOutcome::DuplicateAcked` carrying the same entry count
    /// the first ack reported, so the caller can resend the same `A
    /// <entries>` ack instead of starting a second migration or
    /// rejecting a retry that's really just a replay. In the brief
    /// window before that stamp lands, and for an `M` naming a
    /// genuinely different `joining_name` while one is already active
    /// (shouldn't happen given discovery's single-join-at-a-time
    /// invariant, but handled the same defensive way regardless of
    /// cause), this returns `MigrationOutcome::Conflict` so the caller
    /// rejects with `R\n` — for the same-name case, the next retry will
    /// find `acked_entries` stamped.
    ///
    /// Reuses `migration_target_for`'s lazy-expiry check first: a slot
    /// left by a prior handoff that finished *and* whose forwarding grace
    /// has already elapsed is stale, not "still active" — without this, a
    /// completed slot that no client `GET`/`SET` has touched since (the
    /// only other place that lazily clears it) would wrongly block the
    /// very next join.
    // Returns `MigrationOutcome`, not `Self`, on purpose: reserving the
    // slot can produce an idempotent re-ack or a rejection instead of a
    // guard — see `MigrationOutcome`.
    #[allow(clippy::new_ret_no_self)]
    fn new(
        slot: Arc<Mutex<Option<ActiveMigration>>>,
        joining_name: String,
        joining_addr: String,
        after_ring: Arc<HashRing>,
        replication: usize,
    ) -> MigrationOutcome {
        let mut guard = slot.lock().unwrap_or_else(|poisoned| poisoned.into_inner());

        let expired = guard.as_ref().is_some_and(|active| {
            active
                .completed_at
                .is_some_and(|completed_at| completed_at.elapsed() >= active.forwarding_grace)
        });
        if expired {
            *guard = None;
        }

        if let Some(existing) = guard.as_ref() {
            if existing.joining_name == joining_name {
                // Same-name retry: re-ack idempotently once the original
                // `M` has computed what to ack, otherwise ask the caller
                // to reject — the next retry will find it stamped.
                return match existing.acked_entries {
                    Some(acked_entries) => MigrationOutcome::DuplicateAcked(acked_entries),
                    None => MigrationOutcome::Conflict,
                };
            }

            let conflicting_joining_name = existing.joining_name.clone();
            // Dropped before logging (unlike the lock this replaced,
            // which held it across the `eprintln!`) since `slot` is also
            // locked by `migration_target_for` on every GET/SET — a
            // backpressured stderr shouldn't stall the hot path.
            drop(guard);
            eprintln!(
                "WARN ignoring M for {joining_name}: a migration to \
                 {conflicting_joining_name} is already active"
            );
            return MigrationOutcome::Conflict;
        }

        let abort_requested = Arc::new(AtomicBool::new(false));

        *guard = Some(ActiveMigration {
            joining_name,
            joining_addr,
            after_ring,
            replication,
            completed_at: None,
            forwarding_grace: Duration::ZERO,
            acked_entries: None,
            abort_requested: Arc::clone(&abort_requested),
            forward_connection: Arc::new(AsyncMutex::new(None)),
        });
        drop(guard);

        MigrationOutcome::New(Self {
            slot,
            abort_requested,
            completed: false,
        })
    }

    /// Stamps the entry count this handoff acks (`entries_to_send_count`,
    /// computed by the caller after this guard exists — the snapshot it
    /// needs isn't available yet at `new`'s call site) into the slot, so
    /// a same-`joining_name` retry of `M` can be answered with the same
    /// ack again — see `MigrationGuard::new`'s doc comment.
    fn ack(&self, entries: usize) {
        if let Some(active) = self
            .slot
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .as_mut()
        {
            active.acked_entries = Some(entries);
        }
    }

    /// Consumes the guard after a successful transfer: instead of
    /// clearing the slot (which would close the write-forwarding window
    /// the moment THIS node finishes — issue #3), it stamps the
    /// completion time and this handoff's own size-derived grace (from
    /// `entries_sent`, size-derived migration timeout) so `migration_target_for` keeps
    /// forwarding until that grace passes or the slot is
    /// replaced/cancelled.
    fn completed(mut self, entries_sent: usize) {
        if let Some(active) = self
            .slot
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .as_mut()
        {
            active.completed_at = Some(Instant::now());
            active.forwarding_grace = forwarding_grace(entries_sent);
        }
        self.completed = true;
    }
}

impl Drop for MigrationGuard {
    fn drop(&mut self) {
        if self.completed {
            return;
        }
        *self
            .slot
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = None;
    }
}

/// Computes this handoff's before/after hash rings from `joined` +
/// `joining_name`, folding this node's own name into the "before" set —
/// discovery always lists this node in the roster (it only sends `M` to
/// `Joined` members), but the sender/displaced computations downstream
/// are meaningless without self in the "before" set, so this makes that
/// structural rather than trusted. Called once by `handle_connection`
/// (to reserve `MigrationGuard` before acknowledging `M` — see
/// `Response::MigrationRejected`, and to size `entries_to_send_count`),
/// with the resulting rings then also handed to `run_migration`.
fn migration_rings(
    node_context: &NodeContext,
    joining_name: &str,
    joined: &[(String, String)],
) -> (HashRing, HashRing) {
    let mut before_members: Vec<String> = joined.iter().map(|(name, _)| name.clone()).collect();
    if !before_members.iter().any(|name| name == &node_context.name) {
        before_members.push(node_context.name.clone());
    }
    let mut after_members = before_members.clone();
    after_members.push(joining_name.to_string());

    (HashRing::new(before_members), HashRing::new(after_members))
}

/// Size-derived migration timeout: counts how many of `keys` this node will actually
/// send to the joining node, mirroring the sender/displaced predicate
/// `run_migration` computes for real (the old primary for a key is the
/// one designated sender — a key can be affected by the join without
/// this node being the one that sends it). Purely to size discovery's
/// migration timeout — not a transfer plan.
///
/// Takes `keys` (a `list_keys` snapshot) rather than fetching its own:
/// this and `run_migration`'s own transfer loop used to each call
/// `list_entries` (then a full key+value+TTL clone, see `Cache::keys`'s
/// doc comment) independently — two full clones of the cache off of the
/// single-threaded cache actor per `M`, back to back, each blocking
/// every other request the actor handles for its duration. `M`'s caller
/// (`handle_connection`) now takes one snapshot and passes it to both,
/// halving that stall. This does mean a concurrent write racing the
/// snapshot could shift the true count slightly by the time transfer
/// actually happens — already true before this change (the snapshot was
/// never a transfer plan either way; `run_migration` re-checks every
/// key's live value before sending it regardless, see its own comment).
/// A chunked/budgeted listing (mirroring `sweep`'s `SWEEP_BUDGET`) would
/// avoid the clone-the-whole-cache cost entirely, but needs the
/// migration protocol to support resuming a partial listing — a larger
/// change than this bug fix warrants.
fn entries_to_send_count(
    keys: &[Bytes],
    before_ring: &HashRing,
    after_ring: &HashRing,
    self_name: &str,
    joining_name: &str,
    replication: usize,
) -> usize {
    keys.iter()
        .filter(|key| {
            after_ring.is_owner(key, joining_name, replication)
                && before_ring.owners(key, replication).first() == Some(&self_name)
        })
        .count()
}

/// Staged node join (generalized by client-side replication): triggered by an incoming `M`.
/// Computes, using the same rendezvous-hash algorithm clients use
/// (`HashRing` here is `src/hash_ring.rs`'s copy, see TLS support), how each
/// of this node's own entries' top-R owner set changes when the joining
/// node is added. Adding exactly one node can only insert it into a key's
/// ranking, never reorder the existing nodes relative to each other, so
/// per affected key exactly two roles exist among the pre-join owners:
/// the old *primary* sends the joining node its copy (one designated
/// sender — no duplicate transfers), and the node displaced from rank R
/// to R+1 (if any — there is at most one) marks its now-dead copy for
/// the post-handoff sweep. This node may hold either role, both (R=1:
/// sender and displaced coincide, which is exactly the pre-replication
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
/// size-derived migration timeout (size-derived migration timeout) to reap the
/// stalled join rather than have this node claim success over a joining
/// node that's missing data. A lost discovery connection for the final
/// `C` is a separate, already-terminal failure (the transfer itself
/// succeeded) and is just logged.
///
/// Takes `migration_guard` (and the `before_ring`/`after_ring` it was
/// created from) already reserved by `handle_connection`, rather than
/// reserving them itself: the guard has to exist *before* `M` is
/// acknowledged, so a rejection (see `Response::MigrationRejected`) can
/// be reported on the same ack instead of the caller being told
/// `MigrationAccepted` and then silently getting nothing.
///
/// Also takes `keys` already fetched by `handle_connection` (the same
/// `list_keys` snapshot it used to compute `entries_to_send_count`),
/// rather than fetching its own — see `entries_to_send_count`'s comment
/// on why one snapshot is now shared instead of each side cloning the
/// whole cache independently. `None` here means the cache task was
/// already unavailable when `handle_connection` took the snapshot. Only
/// ever the key of each candidate entry (never a value or TTL, see
/// `Cache::keys`'s doc comment): this loop re-peeks the live value/TTL
/// for every key it actually sends, below.
#[allow(clippy::too_many_arguments)]
async fn run_migration(
    node_context: NodeContext,
    joining_name: String,
    joining_addr: String,
    replication: usize,
    before_ring: HashRing,
    after_ring: Arc<HashRing>,
    migration_guard: MigrationGuard,
    keys: Option<Vec<Bytes>>,
) {
    println!("INFO migration started: handoff to {joining_name} at {joining_addr}");

    let keys = match keys {
        Some(keys) => keys,
        None => {
            eprintln!("WARN migration to {joining_name} aborted: cache task is unavailable");
            return;
        }
    };

    let mut marked_this_run = Vec::new();
    let mut sent_count = 0usize;
    let mut stream: Option<ClientStream> = None;

    let self_name = node_context.name.as_str();

    for key in keys {
        if migration_guard.abort_requested.load(Ordering::SeqCst) {
            break;
        }

        // A key is affected only if the joiner cracks its top-R (HRW
        // insertion can't change the set any other way).
        if !after_ring.is_owner(&key, &joining_name, replication) {
            continue;
        }

        let old_owners = before_ring.owners(&key, replication);
        // Client-side replication: the old primary is the one designated sender.
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
        // still a live replica, client-side replication); only a displaced copy is dead.
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
        forwarding_grace(sent_count).as_secs()
    );

    if let Err(error) = report_complete(&node_context, &joining_name).await {
        eprintln!(
            "WARN migration to {joining_addr} finished but reporting completion to {} failed: {error}",
            node_context.discovery_addr
        );
    }

    // Keep the write-forwarding window open past this node's own share
    // (issue #3) — see `MigrationGuard::completed`.
    migration_guard.completed(sent_count);
}

async fn list_keys(request_tx: &mpsc::Sender<CacheRequest>) -> Option<Vec<Bytes>> {
    let (response_tx, response_rx) = oneshot::channel();

    request_tx
        .send(CacheRequest {
            command: Command::ListEntries,
            response_tx,
        })
        .await
        .ok()?;

    match response_rx.await.ok()? {
        Response::Keys(keys) => Some(keys),
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

/// Staged node join's active-deletion background task: every `SWEEP_INTERVAL`,
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
        timeout(OUTBOUND_IO_TIMEOUT, async {
            stream.write_all(&auth_message(secret)).await?;

            let mut ack = [0u8; 3];
            stream.read_exact(&mut ack).await?;

            if &ack != b"On\n" {
                return Err(io::Error::new(
                    io::ErrorKind::PermissionDenied,
                    "joining node rejected the auth secret",
                ));
            }
            io::Result::Ok(())
        })
        .await
        .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "outbound auth timed out"))??;
    }

    Ok(stream)
}

async fn send_set(
    stream: &mut ClientStream,
    key: &[u8],
    value: &[u8],
    ttl: Option<Duration>,
) -> io::Result<()> {
    timeout(OUTBOUND_IO_TIMEOUT, async {
        stream.write_all(&set_message(key, value, ttl)).await?;

        let mut ack = [0u8; 2];
        stream.read_exact(&mut ack).await?;

        if &ack != b"S\n" {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "joining node did not acknowledge the transferred key",
            ));
        }
        io::Result::Ok(())
    })
    .await
    .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "outbound set timed out"))?
}

/// What a concurrent client write needs to forward itself to an
/// in-flight handoff's joining node (see `migration_target_for`):
/// where to reach it, and the persistent connection to reuse for the
/// forward — shared with every other forwarded write for this same
/// migration (`ActiveMigration::forward_connection`) rather than each
/// dialing its own.
struct ForwardTarget {
    addr: String,
    connection: Arc<AsyncMutex<Option<ClientStream>>>,
}

/// Bounded by `FORWARD_TIMEOUT` as a single whole — see that constant's
/// doc comment for why this can't just rely on `connect_and_authenticate`
/// and `send_set`'s own per-leg `OUTBOUND_IO_TIMEOUT`s: this call runs
/// synchronously inside a client's connection task (`handle_connection`'s
/// `S` handling), so its worst case directly stalls that client's
/// pipeline.
///
/// Issue: this used to call `connect_and_authenticate` fresh on every
/// call, opening (and, once dropped, leaking into TIME_WAIT) a brand new
/// TCP+TLS+Auth connection per forwarded write — the same ephemeral-port
/// exhaustion `run_migration`'s own bulk transfer already learned to
/// avoid (see `ActiveMigration`'s doc comment on
/// `forward_connection`). Reuses `target.connection` instead, connecting
/// only when there isn't already a live connection there.
async fn set_on_joining_node(
    node_context: &NodeContext,
    target: &ForwardTarget,
    key: &[u8],
    value: &[u8],
    ttl: Option<Duration>,
) -> io::Result<()> {
    forward_on_shared_connection(
        node_context,
        target,
        ForwardedWrite::Set { key, value, ttl },
    )
    .await
}

/// A client write being forwarded to the joining node during its
/// handoff — the two kinds `forward_on_shared_connection` knows how to
/// put on the shared connection.
enum ForwardedWrite<'a> {
    Set {
        key: &'a [u8],
        value: &'a [u8],
        ttl: Option<Duration>,
    },
    Delete {
        key: &'a [u8],
    },
}

impl ForwardedWrite<'_> {
    fn timed_out_message(&self) -> &'static str {
        match self {
            ForwardedWrite::Set { .. } => "forwarding the write to the joining node timed out",
            ForwardedWrite::Delete { .. } => "forwarding the delete to the joining node timed out",
        }
    }

    async fn send(self, stream: &mut ClientStream) -> io::Result<()> {
        match self {
            ForwardedWrite::Set { key, value, ttl } => send_set(stream, key, value, ttl).await,
            ForwardedWrite::Delete { key } => {
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
        }
    }
}

/// Owned counterpart to `ForwardedWrite`, for a forward that must outlive
/// the client connection task that triggered it. `handle_connection`'s
/// `S`/`D` handling hands one of these to `forward_with_retries`, which
/// (see that function's own doc comment) is spawned via `migration_tx`
/// rather than run inline — a spawned `MigrationTask` is `'static`, so it
/// can't borrow `key`/`value` off `handle_connection`'s own stack frame
/// the way the single-shot `ForwardedWrite` does.
enum OwnedForwardedWrite {
    Set {
        key: Bytes,
        value: Bytes,
        ttl: Option<Duration>,
    },
    Delete {
        key: Bytes,
    },
}

impl OwnedForwardedWrite {
    /// Attempts this write once via `set_on_joining_node`/
    /// `delete_on_joining_node` — the same single-attempt primitives
    /// `forward_with_retries` used to call directly, now wrapped in its
    /// retry loop instead of replaced by a new code path.
    async fn attempt(&self, node_context: &NodeContext, target: &ForwardTarget) -> io::Result<()> {
        match self {
            OwnedForwardedWrite::Set { key, value, ttl } => {
                set_on_joining_node(node_context, target, key, value, *ttl).await
            }
            OwnedForwardedWrite::Delete { key } => {
                delete_on_joining_node(node_context, target, key).await
            }
        }
    }

    /// Names the write kind for the WARN logs `forward_with_retries`
    /// emits — mirrors the `SET`/`DELETE` wire command letters informally,
    /// not the actual `S`/`D` protocol bytes.
    fn kind(&self) -> &'static str {
        match self {
            OwnedForwardedWrite::Set { .. } => "SET",
            OwnedForwardedWrite::Delete { .. } => "DELETE",
        }
    }
}

/// The one place a forwarded write touches `ForwardTarget::connection`:
/// takes the shared connection (dialing it if there is none), sends
/// `write` on it, and — crucially — resets the slot to `None` whenever
/// the stream's state is no longer known to be clean: after the send
/// fails, and after the whole forward times out.
///
/// The timeout case is why the guard is taken *outside* the
/// `timeout_at`: cancelling a future that holds the guard drops the
/// guard but runs none of its cleanup, so a forward that timed out
/// mid-write (a partial `S` frame on the wire) would have left the
/// desynced stream in place for the next forward to reuse, which would
/// then read the *previous* forward's late ack as its own. Holding the
/// guard across the deadline lets the timeout branch clear the slot
/// itself. Waiting for the guard still counts against the same
/// `FORWARD_TIMEOUT` deadline, so the call as a whole stays bounded even
/// when another forward is holding the connection.
async fn forward_on_shared_connection(
    node_context: &NodeContext,
    target: &ForwardTarget,
    write: ForwardedWrite<'_>,
) -> io::Result<()> {
    let timed_out_message = write.timed_out_message();
    let timed_out = || io::Error::new(io::ErrorKind::TimedOut, timed_out_message);
    let deadline = tokio::time::Instant::now() + FORWARD_TIMEOUT;

    let mut connection = tokio::time::timeout_at(deadline, target.connection.lock())
        .await
        .map_err(|_| timed_out())?;

    let result = tokio::time::timeout_at(deadline, async {
        if connection.is_none() {
            *connection = Some(connect_and_authenticate(node_context, &target.addr).await?);
        }
        // Just ensured `Some` above (and nothing else can steal the slot
        // back to `None` while this guard is held).
        let stream = connection
            .as_mut()
            .expect("connection was just established");
        write.send(stream).await
    })
    .await;

    match result {
        Ok(Ok(())) => Ok(()),
        Ok(Err(error)) => {
            // The connection's state after a failed write/read is
            // unknown (e.g. a partial write) — drop it so the next
            // forward reconnects rather than risk a desynced stream,
            // mirroring `run_migration`'s own per-key retry on its
            // separate connection.
            *connection = None;
            Err(error)
        }
        Err(_elapsed) => {
            *connection = None;
            Err(timed_out())
        }
    }
}

/// Retries a forwarded write's `forward_on_shared_connection` call up to
/// `KEY_TRANSFER_ATTEMPTS` times — the same budget `run_migration`'s bulk
/// transfer gives each key (see `KEY_TRANSFER_ATTEMPTS`). Previously the
/// concurrent-write forward path (`handle_connection`'s `S`/`D` handling,
/// via `set_on_joining_node`/`delete_on_joining_node`) called
/// `forward_on_shared_connection` exactly once and only logged on
/// failure, unlike the bulk transfer's own per-key retry loop — a single
/// transient failure on the shared forwarding connection (a reset TCP
/// connection, a joining node that's momentarily too busy to ack) would
/// permanently drop a concurrent client's write, even though the exact
/// same blip is tolerated during the bulk handoff.
///
/// Spawned via `migration_tx` (mirroring how the `M` handler already
/// hands `run_migration` itself to `run`'s own loop — see
/// `ConnectionConfig::migration_tx`) rather than awaited inline in
/// `handle_connection`, which is where `set_on_joining_node`/
/// `delete_on_joining_node` used to be called from directly.
/// `forward_on_shared_connection` is deliberately bounded to a single
/// `FORWARD_TIMEOUT` specifically so a client's connection is never
/// stalled by more than one multiple of it (see that constant's own doc
/// comment, which spells out the same "worst case: one multiple, not
/// several" reasoning this respects); retrying it up to
/// `KEY_TRANSFER_ATTEMPTS` times *inline*, the way this now does to match
/// `run_migration`'s own per-key resilience, would multiply that worst
/// case by up to 3x and hold up every command queued behind it on this
/// connection. Running the retries in the background — the client's own
/// `S`/`D` response was already written before this is spawned — keeps
/// this connection's stall at zero while still giving the forward the
/// same retry budget the bulk transfer gets. The trade-off: a client that
/// immediately re-reads the same key from a *different* node than the one
/// this forward eventually reaches can observe a brief window where the
/// forward hasn't landed yet — already true of the single-attempt design
/// this replaces (see `forwarding_grace`, which exists precisely because
/// that window isn't instant), just now bounded by up to
/// `KEY_TRANSFER_ATTEMPTS` x `FORWARD_TIMEOUT` instead of one.
async fn forward_with_retries(
    node_context: NodeContext,
    target: ForwardTarget,
    write: OwnedForwardedWrite,
) {
    for attempt in 1..=KEY_TRANSFER_ATTEMPTS {
        match write.attempt(&node_context, &target).await {
            Ok(()) => return,
            Err(error) => {
                eprintln!(
                    "WARN failed to forward a concurrent {} for a migrating key to {} \
                     (attempt {attempt}/{KEY_TRANSFER_ATTEMPTS}): {error}",
                    write.kind(),
                    target.addr
                );
            }
        }
    }

    eprintln!(
        "WARN permanently failed to forward a concurrent {} for a migrating key to {} after \
         {KEY_TRANSFER_ATTEMPTS} attempts",
        write.kind(),
        target.addr
    );
}

/// This node's own current view of cluster membership, if it has one yet
/// (see `NodeContext::known_ring`) says `key` isn't this node's to serve
/// anymore.
///
/// Not while the handoff that displaced `key` is still forwarding, though
/// (`migration_target_for`): `known_ring` flips as soon as *this node's*
/// share of a join is done, but discovery only publishes the joiner in `L`
/// once *every* ready node has reported `C` — which, with size-derived
/// timeouts (size-derived migration timeout), can be minutes after a small node finishes.
/// In that gap a client's `L` still names this node as the owner and a
/// refresh-and-retry gets the very same list, so rejecting here would make
/// the key plainly unavailable (an error, not a miss) for R=1. Serving it
/// locally — and forwarding `S`/`D` to the joiner, which the handlers
/// already do for exactly this window — is what keeps the join invisible
/// to clients. Once the window closes the rejection below takes over.
fn wrong_node(node_context: &NodeContext, key: &[u8]) -> bool {
    let displaced = node_context
        .known_ring
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .as_ref()
        .is_some_and(|membership| {
            // Client-side replication: this node serves a key when it's anywhere in the
            // key's top-R, not only when it's the primary.
            !membership
                .ring
                .is_owner(key, &node_context.name, membership.replication)
        });

    displaced && migration_target_for(node_context, key).is_none()
}

/// If a handoff is currently in flight and `key` is one it's moving (per
/// its `after_ring`), returns where to forward it — for `handle_connection`
/// to also propagate a client's `S`/`D` for that key there, so the
/// joining node doesn't end up serving a stale value once promoted (see
/// the staged-join handoff design).
fn migration_target_for(node_context: &NodeContext, key: &[u8]) -> Option<ForwardTarget> {
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
            .is_some_and(|completed_at| completed_at.elapsed() >= active.forwarding_grace)
    });
    if expired {
        *slot = None;
    }

    slot.as_ref()
        .filter(|active| {
            // Client-side replication: the joiner is a destination for `key` whenever it
            // entered the key's top-R, not only as its new primary.
            active
                .after_ring
                .is_owner(key, &active.joining_name, active.replication)
        })
        .map(|active| ForwardTarget {
            addr: active.joining_addr.clone(),
            connection: Arc::clone(&active.forward_connection),
        })
}

/// Forwards a client's `D` for `key` to `target`, mirroring
/// `set_on_joining_node` but for deletes — see `migration_target_for` and
/// `ForwardTarget`. Accepts either `D\n` (the key was present there too)
/// or `N\n` (it hadn't arrived yet, e.g. this delete raced ahead of the
/// migration task's own send of it) as a successful delivery. Bounded by
/// `FORWARD_TIMEOUT` as a single whole, same reasoning as
/// `set_on_joining_node`, and reuses `target.connection` the same way.
async fn delete_on_joining_node(
    node_context: &NodeContext,
    target: &ForwardTarget,
    key: &[u8],
) -> io::Result<()> {
    forward_on_shared_connection(node_context, target, ForwardedWrite::Delete { key }).await
}

async fn report_complete(node_context: &NodeContext, joining_name: &str) -> io::Result<()> {
    let mut stream = connect_client_stream(
        &node_context.discovery_addr,
        node_context.tls_connector.as_ref(),
    )
    .await?;

    timeout(OUTBOUND_IO_TIMEOUT, async {
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
            .write_all(&complete_message(
                &node_context.name,
                joining_name,
                &node_context.token,
            ))
            .await?;

        let mut ack = [0u8; 2];
        stream.read_exact(&mut ack).await?;

        if &ack != b"A\n" {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "discovery server did not acknowledge C",
            ));
        }

        io::Result::Ok(())
    })
    .await
    .map_err(|_| {
        io::Error::new(
            io::ErrorKind::TimedOut,
            "reporting migration completion to discovery timed out",
        )
    })?
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;

    /// A stand-in peer address for `handle_connection` in tests — only the
    /// `M`/`X` token-mismatch WARN logs read it, never the control flow.
    fn test_client_addr() -> SocketAddr {
        "127.0.0.1:9000".parse().unwrap()
    }

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
            test_client_addr(),
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
            test_client_addr(),
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
            test_client_addr(),
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

    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn handle_connection_times_out_on_trickled_bytes_that_never_complete_a_request() {
        // Slowloris regression: before this fix, every byte read reset
        // the idle deadline to `now + IDLE_TIMEOUT`, regardless of
        // whether it completed a command — so a client sending one byte
        // just under `IDLE_TIMEOUT` apart could hold a `MAX_CONNECTIONS`
        // permit forever without ever finishing a request. The deadline
        // must instead be anchored to the last *completed* parse (or to
        // accept-time, before any request has completed).
        let (mut client, server) = tcp_pair().await;

        let (request_tx, _request_rx) = mpsc::channel(1);
        let (_shutdown_tx, shutdown_rx) = watch::channel(false);

        let connection_task = tokio::spawn(handle_connection(
            ServerStream::Plain(server),
            test_client_addr(),
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

        // Trickle in a single byte of an otherwise-incomplete `Get`
        // command just under the original deadline (accept +
        // IDLE_TIMEOUT). A read-resetting deadline would treat this as
        // grounds for another full IDLE_TIMEOUT.
        tokio::time::advance(IDLE_TIMEOUT - Duration::from_secs(1)).await;
        client.write_all(b"G").await.unwrap();
        tokio::task::yield_now().await;

        // Past the original deadline, but nowhere near what a
        // read-resetting deadline would have allowed (another ~59s).
        tokio::time::advance(Duration::from_secs(2)).await;

        let error = connection_task.await.unwrap().unwrap_err();

        assert_eq!(error.kind(), io::ErrorKind::TimedOut);
    }

    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn set_on_joining_node_bounds_the_whole_forward_even_when_every_leg_individually_succeeds()
     {
        // Regression: `set_on_joining_node` used to only bound each leg
        // (connect, TLS, auth, the set/ack round trip) separately via
        // `OUTBOUND_IO_TIMEOUT`, so a joining node that responded to each
        // leg just slowly enough to never trip that leg's own timeout
        // could still hold the whole call — and the client connection
        // task forwarding it inline — open for a multiple of
        // `OUTBOUND_IO_TIMEOUT`. `FORWARD_TIMEOUT` now bounds the entire
        // operation as one whole, regardless of how far along it got.
        let joining_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let joining_addr = joining_listener.local_addr().unwrap().to_string();

        let (release_auth_tx, release_auth_rx) = oneshot::channel::<()>();
        let (release_set_tx, release_set_rx) = oneshot::channel::<()>();
        let joining_task = tokio::spawn(async move {
            let (mut connection, _) = joining_listener.accept().await.unwrap();
            // Answers auth, then withholds the SET ack — both gated on
            // the test releasing them, so elapsed *virtual* time is
            // entirely under the test's control.
            let _ = release_auth_rx.await;
            let _ = connection.write_all(b"On\n").await;
            let _ = release_set_rx.await;
            let _ = connection.write_all(b"S\n").await;
        });

        let node_context = NodeContext {
            name: "ready-node".to_string(),
            token: "tk-ready-node".to_string(),
            discovery_addr: "127.0.0.1:0".to_string(),
            active_migration: Arc::new(Mutex::new(None)),
            known_ring: Arc::new(Mutex::new(None)),
            auth_secret: Some(Bytes::from_static(b"shared-secret")),
            tls_connector: None,
            request_tx: mpsc::channel(1).0,
        };

        let target = ForwardTarget {
            addr: joining_addr,
            connection: Arc::new(AsyncMutex::new(None)),
        };

        let forward_task = tokio::spawn(async move {
            set_on_joining_node(&node_context, &target, b"name", b"Alice", None).await
        });

        tokio::task::yield_now().await;

        // The auth leg succeeds well under its own per-leg timeout...
        tokio::time::advance(OUTBOUND_IO_TIMEOUT - Duration::from_secs(1)).await;
        let _ = release_auth_tx.send(());
        tokio::task::yield_now().await;

        // ...so under the old per-leg-only bound, the SET/ack leg would
        // get a fresh `OUTBOUND_IO_TIMEOUT` window of its own starting
        // now, well past `FORWARD_TIMEOUT`'s deadline (measured from the
        // call's start). It's never released here.
        tokio::time::advance(Duration::from_secs(2)).await;

        let result = forward_task.await.unwrap();
        let error = result.expect_err(
            "expected the overall FORWARD_TIMEOUT to fire even though each leg individually \
             stayed under OUTBOUND_IO_TIMEOUT",
        );
        assert_eq!(error.kind(), io::ErrorKind::TimedOut);

        joining_task.abort();
        let _ = release_set_tx.send(());
    }

    #[tokio::test(flavor = "current_thread")]
    async fn forwarded_writes_to_a_joining_node_reuse_one_connection() {
        // Regression: `set_on_joining_node`/`delete_on_joining_node` used
        // to call `connect_and_authenticate` fresh on every call — connect
        // -> auth -> set/delete -> disconnect, once per concurrent client
        // write racing an in-progress handoff — exhausting ephemeral ports
        // the same way `run_migration`'s own bulk transfer already
        // learned to avoid (see `ActiveMigration::forward_connection`'s
        // doc comment). A `ForwardTarget` shared across calls (as
        // `migration_target_for` hands out for one migration) must reuse
        // a single connection instead.
        let joining_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let joining_addr = joining_listener.local_addr().unwrap().to_string();

        let joining_task = tokio::spawn(async move {
            let (mut connection, _) = joining_listener.accept().await.unwrap();

            // Two SETs and a DELETE, all forwarded separately below, must
            // all land on this one accepted connection.
            for _ in 0..2 {
                let mut buffer = [0u8; 256];
                let bytes_read = connection.read(&mut buffer).await.unwrap();
                assert!(bytes_read > 0);
                connection.write_all(b"S\n").await.unwrap();
            }
            let mut buffer = [0u8; 256];
            let bytes_read = connection.read(&mut buffer).await.unwrap();
            assert!(bytes_read > 0);
            connection.write_all(b"D\n").await.unwrap();

            let second_connection =
                timeout(Duration::from_millis(200), joining_listener.accept()).await;
            assert!(
                second_connection.is_err(),
                "expected every forwarded write to reuse the same connection"
            );
        });

        let node_context = NodeContext {
            name: "ready-node".to_string(),
            token: "tk-ready-node".to_string(),
            discovery_addr: "127.0.0.1:0".to_string(),
            active_migration: Arc::new(Mutex::new(None)),
            known_ring: Arc::new(Mutex::new(None)),
            auth_secret: None,
            tls_connector: None,
            request_tx: mpsc::channel(1).0,
        };

        // Exactly what `migration_target_for` would hand every concurrent
        // client write racing the same handoff: the same `addr` and the
        // same shared `connection`.
        let target = ForwardTarget {
            addr: joining_addr,
            connection: Arc::new(AsyncMutex::new(None)),
        };

        set_on_joining_node(&node_context, &target, b"name", b"Alice", None)
            .await
            .unwrap();
        set_on_joining_node(&node_context, &target, b"age", b"30", None)
            .await
            .unwrap();
        delete_on_joining_node(&node_context, &target, b"name")
            .await
            .unwrap();

        joining_task.await.unwrap();
    }

    #[tokio::test(flavor = "current_thread")]
    async fn a_forward_that_times_out_does_not_leave_its_connection_for_the_next_forward() {
        // Regression: the shared forwarding connection was only reset on
        // an *error* from the send, inside the `timeout` future. A forward
        // that timed out instead (frame written, ack never read) had that
        // future — and its cleanup — dropped, so the half-used connection
        // stayed in the slot and the next forward reused it, reading the
        // previous forward's late ack as its own. After a timeout the
        // next forward must dial a fresh connection.
        let joining_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let joining_addr = joining_listener.local_addr().unwrap().to_string();

        // Real I/O (connect, write) runs on the real clock; only the ack
        // wait is fast-forwarded, by pausing the clock once the joining
        // side has the frame in hand — a paused clock auto-advances past
        // `FORWARD_TIMEOUT` the moment nothing else is runnable, but it
        // also races real loopback I/O against timers, so it must not be
        // on while a connect is in flight.
        let (frame_received_tx, frame_received_rx) = oneshot::channel::<()>();
        let joining_task = tokio::spawn(async move {
            let (mut first, _) = joining_listener.accept().await.unwrap();
            let mut buffer = [0u8; 256];
            let bytes_read = first.read(&mut buffer).await.unwrap();
            assert!(bytes_read > 0);
            let _ = frame_received_tx.send(());
            // Never ack the first SET — hold the connection open (no EOF,
            // so the forward can only end by timing out) until a second
            // connection arrives, which is the behaviour under test.
            // Well past FORWARD_TIMEOUT: while the clock is paused the
            // runtime auto-advances to the *earliest* pending timer, so a
            // bound shorter than the forward's own deadline would fire
            // first and abort this side before the forward times out.
            let (mut second, _) = timeout(Duration::from_secs(60), joining_listener.accept())
                .await
                .expect("the forward after a timeout must dial a fresh connection")
                .unwrap();
            let bytes_read = second.read(&mut buffer).await.unwrap();
            assert!(bytes_read > 0);
            second.write_all(b"S\n").await.unwrap();
            drop(first);
        });

        let node_context = Arc::new(NodeContext {
            name: "ready-node".to_string(),
            token: "tk-ready-node".to_string(),
            discovery_addr: "127.0.0.1:0".to_string(),
            active_migration: Arc::new(Mutex::new(None)),
            known_ring: Arc::new(Mutex::new(None)),
            auth_secret: None,
            tls_connector: None,
            request_tx: mpsc::channel(1).0,
        });
        let target = Arc::new(ForwardTarget {
            addr: joining_addr,
            connection: Arc::new(AsyncMutex::new(None)),
        });

        let first_forward = tokio::spawn({
            let node_context = Arc::clone(&node_context);
            let target = Arc::clone(&target);
            async move { set_on_joining_node(&node_context, &target, b"name", b"Alice", None).await }
        });
        frame_received_rx.await.unwrap();
        tokio::time::pause();
        let error = first_forward.await.unwrap().unwrap_err();
        tokio::time::resume();
        assert_eq!(error.kind(), io::ErrorKind::TimedOut);
        assert!(
            target.connection.lock().await.is_none(),
            "a timed-out forward must not leave its connection in the shared slot"
        );

        set_on_joining_node(&node_context, &target, b"age", b"30", None)
            .await
            .unwrap();

        joining_task.await.unwrap();
    }

    #[tokio::test(flavor = "current_thread")]
    async fn forward_with_retries_recovers_from_a_single_transient_failure() {
        // Regression: a concurrent client SET/DELETE forwarded to a
        // joining node mid-migration used to call
        // `forward_on_shared_connection` exactly once (via
        // `set_on_joining_node`/`delete_on_joining_node`) — unlike
        // `run_migration`'s own bulk transfer, which retries each key up
        // to `KEY_TRANSFER_ATTEMPTS` times. A single transient failure on
        // the shared forwarding connection (e.g. the joining node resets
        // it) would silently and permanently drop the client's write.
        // `forward_with_retries` must retry with the same budget and
        // succeed once a later attempt lands.
        let joining_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let joining_addr = joining_listener.local_addr().unwrap().to_string();

        let joining_task = tokio::spawn(async move {
            // First attempt: accept, then drop the connection without
            // ever acknowledging the SET — a transient failure.
            let (first, _) = joining_listener.accept().await.unwrap();
            drop(first);

            // Second attempt (the retry, on a fresh connection — the
            // shared slot is cleared on failure, see
            // `forward_on_shared_connection`): actually acknowledge it.
            let (mut second, _) = joining_listener.accept().await.unwrap();
            let mut buffer = [0u8; 256];
            let bytes_read = second.read(&mut buffer).await.unwrap();
            assert!(bytes_read > 0);
            second.write_all(b"S\n").await.unwrap();
        });

        let node_context = NodeContext {
            name: "ready-node".to_string(),
            token: "tk-ready-node".to_string(),
            discovery_addr: "127.0.0.1:0".to_string(),
            active_migration: Arc::new(Mutex::new(None)),
            known_ring: Arc::new(Mutex::new(None)),
            auth_secret: None,
            tls_connector: None,
            request_tx: mpsc::channel(1).0,
        };

        let target = ForwardTarget {
            addr: joining_addr,
            connection: Arc::new(AsyncMutex::new(None)),
        };

        // If this never retried, the joining task would hang forever
        // waiting for its second `accept` and this test would time out.
        forward_with_retries(
            node_context,
            target,
            OwnedForwardedWrite::Set {
                key: Bytes::from_static(b"name"),
                value: Bytes::from_static(b"Alice"),
                ttl: None,
            },
        )
        .await;

        joining_task.await.unwrap();
    }

    #[test]
    fn maximum_request_size_is_one_mebibyte() {
        assert_eq!(MAX_REQUEST_SIZE, 1_048_576);
    }

    #[test]
    fn maximum_cache_memory_is_256_mebibytes() {
        assert_eq!(MAX_CACHE_MEMORY_BYTES, 268_435_456);
    }

    #[cfg(unix)]
    #[test]
    fn fd_exhaustion_is_detected_for_emfile_and_enfile() {
        assert!(is_fd_exhaustion(&io::Error::from_raw_os_error(24))); // EMFILE
        assert!(is_fd_exhaustion(&io::Error::from_raw_os_error(23))); // ENFILE
    }

    #[test]
    fn fd_exhaustion_is_not_reported_for_other_accept_errors() {
        // ECONNABORTED (Linux: 103) — a recoverable per-connection failure,
        // but not one that means the process is out of descriptors, so it
        // shouldn't trigger the backoff.
        assert!(!is_fd_exhaustion(&io::Error::from(
            io::ErrorKind::ConnectionAborted
        )));
    }

    #[cfg(unix)]
    #[test]
    fn should_backoff_after_accept_error_matches_fd_exhaustion_on_unix() {
        // On Unix the two checks are meant to agree exactly: fd exhaustion
        // (and nothing else) is worth backing off from.
        assert!(should_backoff_after_accept_error(
            &io::Error::from_raw_os_error(24) // EMFILE
        ));
        assert!(!should_backoff_after_accept_error(&io::Error::from(
            io::ErrorKind::ConnectionAborted
        )));
    }

    #[cfg(not(unix))]
    #[test]
    fn should_backoff_after_accept_error_backs_off_on_every_error_on_non_unix() {
        // Regression: `is_fd_exhaustion` is hard-coded `false` off Unix
        // (no portable errno check for EMFILE/ENFILE there), which used
        // to mean the accept loop's backoff never triggered on non-Unix
        // targets at all — a sustained accept failure would busy-loop
        // instead of yielding. Every accept error must back off there,
        // not just the ones `is_fd_exhaustion` can't even recognize.
        assert!(!is_fd_exhaustion(&io::Error::from(
            io::ErrorKind::ConnectionAborted
        )));
        assert!(should_backoff_after_accept_error(&io::Error::from(
            io::ErrorKind::ConnectionAborted
        )));
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
        let per_ip_connections: PerIpConnections = Arc::new(Mutex::new(HashMap::new()));
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
            Arc::clone(&per_ip_connections),
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
            Arc::clone(&per_ip_connections),
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
    async fn rejects_connection_when_the_per_ip_connection_limit_is_reached() {
        // Regression: `MAX_CONNECTIONS` alone lets a single source IP hold
        // every one of the global permits by itself, starving every other
        // client, without the global semaphore ever reporting anything
        // unusual short of the very last permit. `MAX_CONNECTIONS_PER_IP`
        // must reject a source once it individually reaches its own cap,
        // independent of how much global headroom remains.
        let connection_limit = Arc::new(Semaphore::new(10));
        let per_ip_connections: PerIpConnections = Arc::new(Mutex::new(HashMap::new()));
        let (request_tx, _request_rx) = mpsc::channel(1);

        let ip = std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST);
        // Stands in for `MAX_CONNECTIONS_PER_IP - 1` other already-live
        // connections from this IP, without actually dispatching that
        // many for the test.
        per_ip_connections
            .lock()
            .unwrap()
            .insert(ip, MAX_CONNECTIONS_PER_IP - 1);

        let (_first_client, first_server) = tcp_pair().await;
        let first_address = SocketAddr::new(ip, 9000);

        let mut connection_tasks = JoinSet::new();
        let (_shutdown_tx, shutdown_rx) = watch::channel(false);

        dispatch_connection(
            first_server,
            first_address,
            request_tx.clone(),
            Arc::clone(&connection_limit),
            Arc::clone(&per_ip_connections),
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

        // Let the task run far enough to reserve its per-IP slot and
        // settle into its read loop — this is the connection that fills
        // the cap exactly.
        tokio::task::yield_now().await;
        assert_eq!(
            per_ip_connections.lock().unwrap().get(&ip).copied(),
            Some(MAX_CONNECTIONS_PER_IP)
        );

        let (mut second_client, second_server) = tcp_pair().await;
        let second_address = SocketAddr::new(ip, 9001);

        dispatch_connection(
            second_server,
            second_address,
            request_tx.clone(),
            Arc::clone(&connection_limit),
            Arc::clone(&per_ip_connections),
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

        // Reading to EOF drives the over-limit task to completion: it
        // replies "Busy" and closes without acquiring a per-IP slot.
        let mut response = Vec::new();
        second_client.read_to_end(&mut response).await.unwrap();

        assert_eq!(response, b"B\n");
        assert_eq!(
            per_ip_connections.lock().unwrap().get(&ip).copied(),
            Some(MAX_CONNECTIONS_PER_IP),
            "the rejected connection must not have reserved a slot"
        );
        // The global limit was never the bottleneck here.
        assert!(connection_limit.available_permits() >= 8);

        connection_tasks.abort_all();

        while connection_tasks.join_next().await.is_some() {}
    }

    #[test]
    fn try_acquire_per_ip_denies_once_the_cap_is_reached_and_frees_the_slot_on_drop() {
        let counts: PerIpConnections = Arc::new(Mutex::new(HashMap::new()));
        let ip = std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST);

        let mut guards = Vec::new();
        for _ in 0..MAX_CONNECTIONS_PER_IP {
            guards.push(try_acquire_per_ip(&counts, ip).expect("under the per-IP cap"));
        }

        assert!(
            try_acquire_per_ip(&counts, ip).is_none(),
            "the per-IP cap must reject a connection once MAX_CONNECTIONS_PER_IP is reached"
        );

        // A different source IP has its own, independent budget.
        let other_ip = std::net::IpAddr::V4(std::net::Ipv4Addr::new(10, 0, 0, 1));
        assert!(try_acquire_per_ip(&counts, other_ip).is_some());

        // Dropping one guard frees its slot for the same IP again.
        guards.pop();
        assert!(try_acquire_per_ip(&counts, ip).is_some());
    }

    #[tokio::test(flavor = "current_thread")]
    async fn handle_connection_stops_when_shutdown_is_requested() {
        let (_client, server) = tcp_pair().await;
        let (request_tx, _request_rx) = mpsc::channel(1);
        let (shutdown_tx, shutdown_rx) = watch::channel(false);

        let connection_task = tokio::spawn(handle_connection(
            ServerStream::Plain(server),
            test_client_addr(),
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
            test_client_addr(),
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
            test_client_addr(),
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
            test_client_addr(),
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
            test_client_addr(),
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
    async fn handle_connection_echoes_tags_after_a_tagged_auth() {
        let (mut client, server) = tcp_pair().await;
        let (request_tx, request_rx) = mpsc::channel(1);
        let (_shutdown_tx, shutdown_rx) = watch::channel(false);

        let cache_task = tokio::spawn(run_cache(request_rx, MAX_CACHE_MEMORY_BYTES));
        let connection_task = tokio::spawn(handle_connection(
            ServerStream::Plain(server),
            test_client_addr(),
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

        // Echoed response tags: `A ... T` flips the connection into tagged mode; every
        // later request carries a trailing tag and every response echoes
        // it — including the four-field tagged SET-with-TTL form.
        client
            .write_all(b"A 1 T\nxS 4 5 7\nnameAliceG 4 8\nnameS 4 5 60 9\nnameAliceG 5 10\notherD 4 11\nname")
            .await
            .unwrap();
        client.shutdown().await.unwrap();

        let expected = b"OnT\nS 7\nV 5 8\nAliceS 9\nN 10\nD 11\n";
        let mut response = vec![0_u8; expected.len()];
        client.read_exact(&mut response).await.unwrap();
        assert_eq!(response, expected);

        connection_task.await.unwrap().unwrap();

        drop(request_tx);
        cache_task.await.unwrap();
    }

    #[tokio::test(flavor = "current_thread")]
    async fn handle_connection_rejects_an_untagged_request_on_a_tagged_connection() {
        let (mut client, server) = tcp_pair().await;
        let (request_tx, _request_rx) = mpsc::channel(1);
        let (_shutdown_tx, shutdown_rx) = watch::channel(false);

        let connection_task = tokio::spawn(handle_connection(
            ServerStream::Plain(server),
            test_client_addr(),
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

        // A `G` without the trailing tag is a parse error on a tagged
        // connection — dispatching it positionally is exactly the
        // ambiguity tagged mode exists to remove.
        client.write_all(b"A 1 T\nxG 4\nname").await.unwrap();
        client.shutdown().await.unwrap();

        let mut ack = [0_u8; 4];
        client.read_exact(&mut ack).await.unwrap();
        assert_eq!(&ack, b"OnT\n");

        let error = connection_task.await.unwrap().unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn handle_connection_answers_a_tagged_auth_rejection_in_kind() {
        let (mut client, server) = tcp_pair().await;
        let (request_tx, _request_rx) = mpsc::channel(1);
        let (_shutdown_tx, shutdown_rx) = watch::channel(false);

        let connection_task = tokio::spawn(handle_connection(
            ServerStream::Plain(server),
            test_client_addr(),
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

        client.write_all(b"A 5 T\nwrong").await.unwrap();

        let mut ack = [0_u8; 4];
        client.read_exact(&mut ack).await.unwrap();
        assert_eq!(&ack, b"EnT\n");

        let error = connection_task.await.unwrap().unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::PermissionDenied);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn handle_connection_treats_auth_as_a_no_op_when_no_secret_is_configured() {
        let (mut client, server) = tcp_pair().await;
        let (request_tx, request_rx) = mpsc::channel(1);
        let (_shutdown_tx, shutdown_rx) = watch::channel(false);

        let cache_task = tokio::spawn(run_cache(request_rx, MAX_CACHE_MEMORY_BYTES));
        let connection_task = tokio::spawn(handle_connection(
            ServerStream::Plain(server),
            test_client_addr(),
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

    #[test]
    fn server_name_from_addr_strips_brackets_from_a_bracketed_ipv6_host() {
        let name = server_name_from_addr("[::1]:8356").unwrap();

        assert_eq!(name, ServerName::try_from("::1").unwrap());
    }

    #[test]
    fn server_name_from_addr_handles_a_full_bracketed_ipv6_host() {
        let name = server_name_from_addr("[2001:db8::1]:8356").unwrap();

        assert_eq!(name, ServerName::try_from("2001:db8::1").unwrap());
    }

    #[test]
    fn server_name_from_addr_still_handles_a_plain_ipv4_host() {
        let name = server_name_from_addr("127.0.0.1:8356").unwrap();

        assert_eq!(name, ServerName::try_from("127.0.0.1").unwrap());
    }

    #[test]
    fn server_name_from_addr_still_handles_a_dns_name() {
        let name = server_name_from_addr("node-a.example.com:8356").unwrap();

        assert_eq!(name, ServerName::try_from("node-a.example.com").unwrap());
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
        // Client-side replication roles land on this node at once:
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
            token: "tk-ready-node".to_string(),
            discovery_addr,
            active_migration: Arc::new(Mutex::new(None)),
            known_ring: Arc::new(Mutex::new(None)),
            auth_secret: None,
            tls_connector: None,
            request_tx: request_tx.clone(),
        };

        let joined = vec![
            ("ready-node".to_string(), "127.0.0.1:1".to_string()),
            ("other-node".to_string(), "127.0.0.1:1".to_string()),
        ];
        let (before_ring, after_ring) = migration_rings(&node_context, "joiner-0", &joined);
        let after_ring = Arc::new(after_ring);
        let migration_guard = MigrationGuard::new(
            Arc::clone(&node_context.active_migration),
            "joiner-0".to_string(),
            joining_addr.clone(),
            Arc::clone(&after_ring),
            2,
        )
        .unwrap_new();

        let keys = list_keys(&request_tx).await;

        run_migration(
            node_context.clone(),
            "joiner-0".to_string(),
            joining_addr.clone(),
            2,
            before_ring,
            after_ring,
            migration_guard,
            keys,
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

        // The flipped membership says the displaced key is no longer this
        // node's — but it keeps being served (and forwarded) for as long
        // as the handoff's forwarding window is open, since discovery
        // hasn't published the joiner yet; the kept key is served as ever.
        assert!(!wrong_node(&node_context, b"key-3"));
        assert!(!wrong_node(&node_context, b"key-0"));

        // Issue #3: this node's own share being done must NOT close the
        // write-forwarding window — discovery hasn't published the joiner
        // yet (other ready nodes may still be transferring), so a
        // concurrent client write for a key in the joiner's top-R still
        // needs forwarding.
        assert_eq!(
            migration_target_for(&node_context, b"key-0").map(|target| target.addr),
            Some(joining_addr.clone()),
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

        // Window closed: now the displaced key is rejected (and the
        // lingering slot is cleared lazily), the kept one still served.
        {
            let mut slot = node_context.active_migration.lock().unwrap();
            let active = slot.as_mut().unwrap();
            active.completed_at = Some(Instant::now() - active.forwarding_grace);
        }
        assert!(wrong_node(&node_context, b"key-3"));
        assert!(!wrong_node(&node_context, b"key-0"));
        assert!(node_context.active_migration.lock().unwrap().is_none());

        joining_task.await.unwrap();
        discovery_task.await.unwrap();
        drop(node_context);
        drop(request_tx);
        cache_task.await.unwrap();
    }

    #[test]
    fn heartbeat_message_declares_the_name_length_before_the_name() {
        assert_eq!(
            heartbeat_message("some-name", Some(2), "tk-some-name"),
            b"H 9 2 12\nsome-nametk-some-name".to_vec()
        );
    }

    #[test]
    fn heartbeat_message_encodes_an_unknown_replication_belief_as_zero() {
        assert_eq!(
            heartbeat_message("some-name", None, "tk-some-name"),
            b"H 9 0 12\nsome-nametk-some-name".to_vec()
        );
    }

    #[test]
    fn join_message_declares_the_name_length_and_the_port() {
        assert_eq!(
            join_message("some-name", 8356, "tk-some-name"),
            b"J 9 8356 12\nsome-nametk-some-name".to_vec()
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
            complete_message("some-name", "joiner", "tk-some-name"),
            b"C 9 6 12\nsome-namejoinertk-some-name".to_vec()
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn a_completed_forwarding_window_expires_after_the_grace() {
        // Issue #3: the lingering entry forwards only within its own
        // forwarding_grace (size-derived migration timeout); past it,
        // migration_target_for clears the slot and stops forwarding.
        let (request_tx, _request_rx) = mpsc::channel(1);
        let node_context = NodeContext {
            name: "ready-node".to_string(),
            token: "tk-ready-node".to_string(),
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
            completed_at: Some(Instant::now() - forwarding_grace(0) - Duration::from_secs(1)),
            forwarding_grace: forwarding_grace(0),
            acked_entries: Some(0),
            abort_requested: Arc::new(AtomicBool::new(false)),
            forward_connection: Arc::new(AsyncMutex::new(None)),
        });

        assert!(migration_target_for(&node_context, b"key-0").is_none());
        assert!(
            node_context.active_migration.lock().unwrap().is_none(),
            "an expired forwarding entry should be cleared lazily"
        );
    }

    #[test]
    fn migration_guard_reuses_a_stale_completed_slot_instead_of_rejecting_a_new_join() {
        // `MigrationGuard::new` must apply the same lazy-expiry check
        // `migration_target_for` does — a slot left by a fully-completed
        // prior handoff whose forwarding grace already elapsed shouldn't
        // block the very next join just because no client GET/SET
        // happened along to clear it first.
        let slot = Arc::new(Mutex::new(Some(ActiveMigration {
            joining_name: "joiner-0".to_string(),
            joining_addr: "127.0.0.1:9".to_string(),
            after_ring: Arc::new(HashRing::new(vec![
                "ready-node".to_string(),
                "joiner-0".to_string(),
            ])),
            replication: 2,
            completed_at: Some(Instant::now() - forwarding_grace(0) - Duration::from_secs(1)),
            forwarding_grace: forwarding_grace(0),
            acked_entries: Some(0),
            abort_requested: Arc::new(AtomicBool::new(false)),
            forward_connection: Arc::new(AsyncMutex::new(None)),
        })));

        let after_ring = Arc::new(HashRing::new(vec![
            "ready-node".to_string(),
            "joiner-1".to_string(),
        ]));
        let outcome = MigrationGuard::new(
            Arc::clone(&slot),
            "joiner-1".to_string(),
            "127.0.0.1:10".to_string(),
            after_ring,
            2,
        );

        assert!(
            matches!(outcome, MigrationOutcome::New(_)),
            "an expired slot must not block a new join"
        );
        assert_eq!(
            slot.lock().unwrap().as_ref().unwrap().joining_name,
            "joiner-1"
        );
    }

    #[test]
    fn migration_guard_rejects_a_still_active_conflicting_migration() {
        let slot = Arc::new(Mutex::new(Some(ActiveMigration {
            joining_name: "joiner-0".to_string(),
            joining_addr: "127.0.0.1:9".to_string(),
            after_ring: Arc::new(HashRing::new(vec![
                "ready-node".to_string(),
                "joiner-0".to_string(),
            ])),
            replication: 2,
            completed_at: None,
            forwarding_grace: Duration::ZERO,
            acked_entries: Some(0),
            abort_requested: Arc::new(AtomicBool::new(false)),
            forward_connection: Arc::new(AsyncMutex::new(None)),
        })));

        let after_ring = Arc::new(HashRing::new(vec![
            "ready-node".to_string(),
            "joiner-1".to_string(),
        ]));
        let outcome = MigrationGuard::new(
            Arc::clone(&slot),
            "joiner-1".to_string(),
            "127.0.0.1:10".to_string(),
            after_ring,
            2,
        );

        assert!(
            matches!(outcome, MigrationOutcome::Conflict),
            "a still-active migration must not be clobbered"
        );
        assert_eq!(
            slot.lock().unwrap().as_ref().unwrap().joining_name,
            "joiner-0",
            "the original migration must be left untouched"
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
            test_client_addr(),
            request_tx.clone(),
            ConnectionConfig {
                idle_timeout: IDLE_TIMEOUT,
                auth_secret: None,
                tls_acceptor: None,
                node_context: Some(NodeContext {
                    name: "ready-node".to_string(),
                    token: "tk-ready-node".to_string(),
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
        let token = "tk-ready-node";
        let mut migrate_message = format!(
            "M {} {} 0 1 {}\n",
            joining_name.len(),
            joining_addr.len(),
            token.len()
        )
        .into_bytes();
        migrate_message.extend_from_slice(token.as_bytes());
        migrate_message.extend_from_slice(joining_name.as_bytes());
        migrate_message.extend_from_slice(joining_addr.as_bytes());

        client.write_all(&migrate_message).await.unwrap();

        let mut ack = [0u8; 4];
        client.read_exact(&mut ack).await.unwrap();
        assert_eq!(&ack, b"A 1\n");

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
            complete_message("ready-node", "joiner-107", "tk-ready-node")
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
    async fn migrate_command_with_the_wrong_token_is_rejected_and_transfers_nothing() {
        // Security regression (issue #34, node side): the shared secret only
        // proves "cluster member", so an `M` from any client holding it must
        // NOT be honored unless it echoes this node's own membership token —
        // otherwise a client could make the node stream its cache to an
        // attacker-chosen address. A wrong token must be rejected outright,
        // no migration started, no `SET` forwarded anywhere.
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

        // An address the node would stream keys to if it (wrongly) honored
        // the `M`. Nothing must ever connect here.
        let attacker_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let attacker_addr = attacker_listener.local_addr().unwrap().to_string();
        let attacker_connected = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let attacker_flag = Arc::clone(&attacker_connected);
        let attacker_task = tokio::spawn(async move {
            if attacker_listener.accept().await.is_ok() {
                attacker_flag.store(true, Ordering::SeqCst);
            }
        });

        let (mut client, server) = tcp_pair().await;
        let (_shutdown_tx, shutdown_rx) = watch::channel(false);
        let (migration_tx, mut migration_rx) = mpsc::channel::<MigrationTask>(1);
        let migration_relay = tokio::spawn(async move {
            while let Some(task) = migration_rx.recv().await {
                task.await;
            }
        });

        let connection_task = tokio::spawn(handle_connection(
            ServerStream::Plain(server),
            test_client_addr(),
            request_tx.clone(),
            ConnectionConfig {
                idle_timeout: IDLE_TIMEOUT,
                auth_secret: None,
                tls_acceptor: None,
                node_context: Some(NodeContext {
                    name: "ready-node".to_string(),
                    token: "tk-ready-node".to_string(),
                    discovery_addr: "127.0.0.1:1".to_string(),
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

        let joining_name = "joiner-107";
        let wrong_token = "tk-not-mine";
        let mut migrate_message = format!(
            "M {} {} 0 1 {}\n",
            joining_name.len(),
            attacker_addr.len(),
            wrong_token.len()
        )
        .into_bytes();
        migrate_message.extend_from_slice(wrong_token.as_bytes());
        migrate_message.extend_from_slice(joining_name.as_bytes());
        migrate_message.extend_from_slice(attacker_addr.as_bytes());
        client.write_all(&migrate_message).await.unwrap();

        // The node rejects with `R\n` (MigrationRejected) and closes the
        // connection — the read returns EOF right after the rejection.
        let mut response = Vec::new();
        client.read_to_end(&mut response).await.unwrap();
        assert_eq!(response, b"R\n");

        // Give any (wrongly) spawned migration a chance to dial out.
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(
            !attacker_connected.load(Ordering::SeqCst),
            "a rejected M must never stream keys to the attacker address"
        );

        attacker_task.abort();
        client.shutdown().await.unwrap();
        let _ = connection_task.await;
        migration_relay.await.unwrap();
        drop(request_tx);
        cache_task.await.unwrap();
    }

    #[tokio::test(flavor = "current_thread")]
    async fn migrate_command_re_acks_a_duplicate_m_for_the_same_joining_node() {
        // Regression: a discovery retry of `M` after a lost ack
        // (`send_migrate_with_retry`) used to hit the same `R\n` rejection
        // as a genuinely conflicting migration, burning the retry's fixed
        // attempt budget against the still-running original handoff and
        // stalling the join until discovery's own migration timeout. It
        // must instead be re-acked with the same entry count, and only one
        // migration must ever run.
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

        // A fake joining node: accepts exactly one connection. If the
        // duplicate `M` wrongly started a second migration, a second SET
        // would arrive here and the assertion on `joining_received` below
        // would see it duplicated.
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

        // A fake discovery server: accepts one connection, expects one C
        // (not two — only one migration should ever run), acks it.
        let discovery_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let discovery_addr = discovery_listener.local_addr().unwrap().to_string();
        let discovery_task = tokio::spawn(async move {
            let (mut connection, _) = discovery_listener.accept().await.unwrap();
            let mut buffer = [0u8; 256];
            let bytes_read = connection.read(&mut buffer).await.unwrap();
            connection.write_all(b"A\n").await.unwrap();
            buffer[..bytes_read].to_vec()
        });

        let (mut client, server) = tcp_pair().await;
        let (_shutdown_tx, shutdown_rx) = watch::channel(false);

        let (migration_tx, mut migration_rx) = mpsc::channel::<MigrationTask>(1);
        let migration_relay = tokio::spawn(async move {
            while let Some(task) = migration_rx.recv().await {
                task.await;
            }
        });

        let connection_task = tokio::spawn(handle_connection(
            ServerStream::Plain(server),
            test_client_addr(),
            request_tx.clone(),
            ConnectionConfig {
                idle_timeout: IDLE_TIMEOUT,
                auth_secret: None,
                tls_acceptor: None,
                node_context: Some(NodeContext {
                    name: "ready-node".to_string(),
                    token: "tk-ready-node".to_string(),
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

        let joining_name = "joiner-107";
        let token = "tk-ready-node";
        let mut migrate_message = format!(
            "M {} {} 0 1 {}\n",
            joining_name.len(),
            joining_addr.len(),
            token.len()
        )
        .into_bytes();
        migrate_message.extend_from_slice(token.as_bytes());
        migrate_message.extend_from_slice(joining_name.as_bytes());
        migrate_message.extend_from_slice(joining_addr.as_bytes());

        client.write_all(&migrate_message).await.unwrap();
        let mut ack = [0u8; 4];
        client.read_exact(&mut ack).await.unwrap();
        assert_eq!(&ack, b"A 1\n");

        // The discovery retry: identical `M`, sent only after the first
        // ack was read, so the original `M`'s handler has already stamped
        // `acked_entries` by the time this one is processed.
        client.write_all(&migrate_message).await.unwrap();
        let mut second_ack = [0u8; 4];
        client.read_exact(&mut second_ack).await.unwrap();
        assert_eq!(
            &second_ack, b"A 1\n",
            "a duplicate M for the same joining node must be re-acked with the same count"
        );

        let complete = discovery_task.await.unwrap();
        assert_eq!(
            complete,
            complete_message("ready-node", "joiner-107", "tk-ready-node"),
            "exactly one migration must run and report completion"
        );

        joining_task.await.unwrap();
        let expected_set = set_message(b"name", b"Alice", None);
        assert_eq!(
            *joining_received.lock().unwrap(),
            expected_set,
            "the joining node must receive the transfer exactly once"
        );

        client.shutdown().await.unwrap();
        let _ = connection_task.await;
        migration_relay.await.unwrap();
        drop(request_tx);
        cache_task.await.unwrap();
    }

    #[tokio::test(flavor = "current_thread")]
    async fn migrate_command_rejects_a_different_joining_node_while_one_is_active() {
        // A duplicate `M` sharing the active migration's `joining_name` is
        // re-acked (see `migrate_command_re_acks_a_duplicate_m_for_the_same_joining_node`);
        // an `M` for a genuinely different `joining_name` while one is
        // already active must still be rejected with `R\n`.
        let (request_tx, request_rx) = mpsc::channel(1);
        let cache_task = tokio::spawn(run_cache(request_rx, MAX_CACHE_MEMORY_BYTES));

        // No keys in the cache: the active migration has nothing to
        // transfer, so it needs no joining-node connection at all — only
        // a discovery connection to report `C` on. Its slot stays
        // occupied by "joiner-a" for `forwarding_grace(0)` (60s base)
        // after completion, long past this test's lifetime.
        let discovery_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let discovery_addr = discovery_listener.local_addr().unwrap().to_string();
        let discovery_task = tokio::spawn(async move {
            let (mut connection, _) = discovery_listener.accept().await.unwrap();
            let mut buffer = [0u8; 256];
            let _ = connection.read(&mut buffer).await.unwrap();
            connection.write_all(b"A\n").await.unwrap();
        });

        let (mut client, server) = tcp_pair().await;
        let (_shutdown_tx, shutdown_rx) = watch::channel(false);

        let (migration_tx, mut migration_rx) = mpsc::channel::<MigrationTask>(1);
        let migration_relay = tokio::spawn(async move {
            while let Some(task) = migration_rx.recv().await {
                task.await;
            }
        });

        let connection_task = tokio::spawn(handle_connection(
            ServerStream::Plain(server),
            test_client_addr(),
            request_tx.clone(),
            ConnectionConfig {
                idle_timeout: IDLE_TIMEOUT,
                auth_secret: None,
                tls_acceptor: None,
                node_context: Some(NodeContext {
                    name: "ready-node".to_string(),
                    token: "tk-ready-node".to_string(),
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

        let first_joining_addr = "127.0.0.1:1";
        let token = "tk-ready-node";
        let mut first_migrate_message = format!(
            "M {} {} 0 1 {}\n",
            "joiner-a".len(),
            first_joining_addr.len(),
            token.len()
        )
        .into_bytes();
        first_migrate_message.extend_from_slice(token.as_bytes());
        first_migrate_message.extend_from_slice(b"joiner-a");
        first_migrate_message.extend_from_slice(first_joining_addr.as_bytes());

        client.write_all(&first_migrate_message).await.unwrap();
        let mut first_ack = [0u8; 4];
        client.read_exact(&mut first_ack).await.unwrap();
        assert_eq!(&first_ack, b"A 0\n");

        let second_joining_addr = "127.0.0.1:2";
        let mut second_migrate_message = format!(
            "M {} {} 0 1 {}\n",
            "joiner-b".len(),
            second_joining_addr.len(),
            token.len()
        )
        .into_bytes();
        second_migrate_message.extend_from_slice(token.as_bytes());
        second_migrate_message.extend_from_slice(b"joiner-b");
        second_migrate_message.extend_from_slice(second_joining_addr.as_bytes());

        client.write_all(&second_migrate_message).await.unwrap();
        let mut second_ack = [0u8; 2];
        client.read_exact(&mut second_ack).await.unwrap();
        assert_eq!(
            &second_ack, b"R\n",
            "an M for a different joining node must still be rejected while one is active"
        );

        discovery_task.await.unwrap();
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
            test_client_addr(),
            request_tx.clone(),
            ConnectionConfig {
                idle_timeout: IDLE_TIMEOUT,
                auth_secret: None,
                tls_acceptor: None,
                node_context: Some(NodeContext {
                    name: "ready-node".to_string(),
                    token: "tk-ready-node".to_string(),
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
        let token = "tk-ready-node";
        let mut migrate_message = format!(
            "M {} {} 0 1 {}\n",
            joining_name.len(),
            joining_addr.len(),
            token.len()
        )
        .into_bytes();
        migrate_message.extend_from_slice(token.as_bytes());
        migrate_message.extend_from_slice(joining_name.as_bytes());
        migrate_message.extend_from_slice(joining_addr.as_bytes());

        client.write_all(&migrate_message).await.unwrap();

        let mut ack = [0u8; 4];
        client.read_exact(&mut ack).await.unwrap();
        assert_eq!(&ack, b"A 1\n");

        // The joining node has the SET in hand but hasn't acked it yet, so
        // `run_migration` is still blocked on that ack — send the cancel
        // now, on the same connection (a fresh one-shot connection, as
        // discovery would really use, is equivalent from the node's side).
        set_received_rx.await.unwrap();

        let mut cancel_message = format!("X {} {}\n", joining_name.len(), token.len()).into_bytes();
        cancel_message.extend_from_slice(token.as_bytes());
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
            test_client_addr(),
            request_tx.clone(),
            ConnectionConfig {
                idle_timeout: IDLE_TIMEOUT,
                auth_secret: None,
                tls_acceptor: None,
                node_context: Some(NodeContext {
                    name: "ready-node".to_string(),
                    token: "tk-ready-node".to_string(),
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
        let token = "tk-ready-node";
        let mut migrate_message = format!(
            "M {} {} 0 1 {}\n",
            joining_name.len(),
            joining_addr.len(),
            token.len()
        )
        .into_bytes();
        migrate_message.extend_from_slice(token.as_bytes());
        migrate_message.extend_from_slice(joining_name.as_bytes());
        migrate_message.extend_from_slice(joining_addr.as_bytes());

        client.write_all(&migrate_message).await.unwrap();

        let mut ack = [0u8; 4];
        client.read_exact(&mut ack).await.unwrap();
        assert_eq!(&ack, b"A 2\n");

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
            complete_message("ready-node", "joiner-107", "tk-ready-node")
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
            test_client_addr(),
            request_tx.clone(),
            ConnectionConfig {
                idle_timeout: IDLE_TIMEOUT,
                auth_secret: None,
                tls_acceptor: None,
                node_context: Some(NodeContext {
                    name: "ready-node".to_string(),
                    token: "tk-ready-node".to_string(),
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
        let token = "tk-ready-node";
        let mut migrate_message = format!(
            "M {} {} 0 1 {}\n",
            joining_name.len(),
            joining_addr.len(),
            token.len()
        )
        .into_bytes();
        migrate_message.extend_from_slice(token.as_bytes());
        migrate_message.extend_from_slice(joining_name.as_bytes());
        migrate_message.extend_from_slice(joining_addr.as_bytes());

        client.write_all(&migrate_message).await.unwrap();

        let mut ack = [0u8; 4];
        client.read_exact(&mut ack).await.unwrap();
        assert_eq!(&ack, b"A 1\n");

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

    /// Parses a `J <name-length> <port> <token-length>\n<name><token>`
    /// message (the only one whose shape the test doesn't already know,
    /// since its name is a random UUID generated inside `send_heartbeats`)
    /// and asserts every subsequent message in `received` is the matching
    /// `H <name-length> <r> <token-length>\n<name><token>` heartbeat for
    /// that same name and token — `r=0` (unknown), since none of these
    /// tests give it a `KnownRing` with a membership belief set.
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
        let token_length: usize = header.next().unwrap().parse().unwrap();
        assert_eq!(header.next(), None);
        assert_eq!(sent_port, port);

        let body = &join[header_end + 1..];
        let name = &body[..name_length];
        let token = &body[name_length..];
        assert_eq!(body.len(), name_length + token_length);

        let expected_heartbeat = format!("H {} 0 {}\n{name}{token}", name.len(), token.len());
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
            "tk-test-node".to_string(),
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
            "tk-test-node".to_string(),
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
        // has sent its own first client-side replication handoff `M`, every heartbeat
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
            "tk-test-node".to_string(),
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
                .any(|message| message.starts_with(b"H 9 0 12\n")),
            "expected at least one heartbeat reporting the unknown (0) belief before \
             known_ring was set, got {heartbeats:?}"
        );
        assert!(
            heartbeats
                .iter()
                .any(|message| message.starts_with(b"H 9 3 12\n")),
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

            // Second connection: the re-registration (discovery HA: must be P,
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
            "tk-test-node".to_string(),
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
        assert_eq!(registrations[0], b"J 9 8356 12\ntest-nodetk-test-node");
        assert_eq!(registrations[1], b"P 9 8356 12\ntest-nodetk-test-node");
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
            // Recorded as an event (checked by the test body) rather than
            // asserted here: this task is aborted, never awaited, so a
            // panic in it would be silently swallowed — which is exactly
            // how a stale pre-registration-derived-address assertion survived here unnoticed.
            standby_events.lock().unwrap().push(format!(
                "announced:{}",
                String::from_utf8_lossy(&buffer[..bytes_read])
            ));
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
            "tk-test-node".to_string(),
            Arc::new(Mutex::new(None)),
            shutdown_rx,
        ));

        for _ in 0..500 {
            if events
                .lock()
                .unwrap()
                .iter()
                .any(|event| event.starts_with("announced"))
            {
                break;
            }
            sleep(Duration::from_millis(5)).await;
        }

        shutdown_tx.send_replace(true);
        heartbeat_task.await.unwrap();
        fake_primary.abort();
        fake_standby.abort();

        assert_eq!(
            *events.lock().unwrap(),
            vec!["promoted", "announced:P 9 8356 12\ntest-nodetk-test-node"]
        );
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
            "tk-test-node".to_string(),
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

    #[tokio::test(flavor = "current_thread")]
    async fn send_heartbeats_redials_when_the_ack_never_arrives() {
        // Regression: the heartbeat ack's `read_exact` had no timeout,
        // unlike the write right above it — a discovery server that
        // accepted a heartbeat but never acked (crashed-but-socket-open,
        // a blackholed route) would hang this connection forever instead
        // of giving up within `OUTBOUND_IO_TIMEOUT` and redialing.
        //
        // Real time, not `start_paused`, on purpose: this exercises a
        // genuine multi-round-trip TCP handshake (connect, join, "R\n",
        // heartbeat) racing the timeout, and paused time's auto-advance
        // can jump the virtual clock past a timer before that real I/O
        // has actually completed.
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let discovery_addr = listener.local_addr().unwrap().to_string();

        let (redialed_tx, redialed_rx) = oneshot::channel::<()>();
        let fake_discovery = tokio::spawn(async move {
            let (mut first, _) = listener.accept().await.unwrap();
            // Promotes immediately without reading the join request
            // first — the client's write lands in the OS buffer
            // regardless of read order, and this leg only cares about
            // timing the ack that follows, not the join's contents.
            first.write_all(b"R\n").await.unwrap();
            // Deliberately never read or ack the heartbeat that
            // follows, and never close the connection either — only a
            // timeout, not a read error, should end this leg.

            // A second connection means the ack leg gave up and
            // `register_with_discovery` redialed.
            let _ = listener.accept().await;
            let _ = redialed_tx.send(());
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
            "tk-test-node".to_string(),
            Arc::new(Mutex::new(None)),
            shutdown_rx,
        ));

        let redialed = timeout(OUTBOUND_IO_TIMEOUT + Duration::from_secs(5), redialed_rx).await;

        shutdown_tx.send_replace(true);
        heartbeat_task.await.unwrap();
        fake_discovery.abort();

        assert!(
            matches!(redialed, Ok(Ok(()))),
            "expected the ack timeout to fire and trigger a redial within OUTBOUND_IO_TIMEOUT"
        );
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
        let per_ip_connections: PerIpConnections = Arc::new(Mutex::new(HashMap::new()));
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
                per_ip_connections,
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

        // Without this, `server_task` only completes once
        // `handle_connection` hits `IDLE_TIMEOUT` (60s) rather than seeing
        // the client close.
        tls.shutdown().await.unwrap();

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
            "tk-test-node".to_string(),
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
