use crate::cache::{Cache, SWEEP_BUDGET};
use crate::command::{Command, MigrateProgress, ParseError, parse_resumable};
use crate::hash_ring::HashRing;
use crate::key::Key;
use crate::response::{MultiAckEntry, MultiEntry, Response};
use bytes::{Bytes, BytesMut};
use rustls::pki_types::{CertificateDer, PrivateKeyDer, ServerName};
use rustls::{ClientConfig, RootCertStore, ServerConfig};
use std::collections::HashMap;
use std::collections::HashSet;
use std::future::Future;
use std::io;
use std::io::BufReader;
use std::net::{IpAddr, SocketAddr};
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use std::time::{Duration, Instant};
use tokio::io::{AsyncBufReadExt, AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, ReadBuf};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{Mutex as AsyncMutex, Semaphore, mpsc, oneshot, watch};
use tokio::task::JoinSet;
use tokio::time::{sleep, timeout};
use tokio_rustls::{TlsAcceptor, TlsConnector};
use uuid::Uuid;

const MAX_REQUEST_SIZE: usize = 1024 * 1024;
/// Default for `--max-connections` (issue #126): previously a fixed
/// constant with no way to tune it — small deployments couldn't lower
/// it, and large ones couldn't raise it.
pub(crate) const DEFAULT_MAX_CONNECTIONS: usize = 1024;
/// Default for `--max-connections-per-ip` (issue #126): a coarse cap on
/// how many live connections a single source IP may hold at once,
/// layered under the global `--max-connections` semaphore (issue: no
/// per-source-IP limit — a single misbehaving or compromised peer could
/// otherwise claim the entire connection budget by itself, starving
/// every other client, without the global semaphore ever reporting
/// anything unusual). Deliberately coarse, not a tight per-client
/// budget: a pooled application host — many worker processes or threads
/// sharing one egress IP, or a fleet behind one NAT — can legitimately
/// hold a large number of concurrent connections to this cache, and
/// this guard exists only to stop one source from monopolising the
/// whole server, not to bound ordinary legitimate concurrency. Behind
/// NAT or on Kubernetes, where many clients share one source IP, *this*
/// — not the global cap — is the effective fleet ceiling, which is
/// exactly why it's now a flag. See `try_acquire_per_ip`.
pub(crate) const DEFAULT_MAX_CONNECTIONS_PER_IP: usize = 256;

/// The two accepted-connection caps, resolved from `--max-connections` /
/// `--max-connections-per-ip` by `main.rs` (issue #126) and threaded to
/// the global semaphore, the per-IP reservation, and the metrics gauge.
#[derive(Clone, Copy)]
pub(crate) struct ConnectionLimits {
    pub(crate) max_connections: usize,
    pub(crate) max_connections_per_ip: usize,
}

impl Default for ConnectionLimits {
    fn default() -> Self {
        Self {
            max_connections: DEFAULT_MAX_CONNECTIONS,
            max_connections_per_ip: DEFAULT_MAX_CONNECTIONS_PER_IP,
        }
    }
}
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
/// hold a `DEFAULT_MAX_CONNECTIONS` permit forever without ever finishing a
/// request. The practical consequence: a legitimate request must arrive
/// in full within this long of the previous one completing, not merely
/// send *some* bytes that often.
const IDLE_TIMEOUT: Duration = Duration::from_secs(60);
/// Bounds a response write (issue #4) — see `write_response`. Shorter
/// than `IDLE_TIMEOUT`: that one tolerates a normal gap between a
/// client's requests, but a peer that has simply stopped draining its
/// receive buffer is a distinct failure that shouldn't get to hold a
/// `DEFAULT_MAX_CONNECTIONS` permit for as long as an idle-but-otherwise-fine
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
/// forever while it still holds a `DEFAULT_MAX_CONNECTIONS` permit; enough such
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
/// Capacity of `ConnectionConfig::forward_tx`, the channel every per-write
/// forward (`forward_with_retries`, spawned from the `S`/`D`/`U`/`u`/
/// `MultiSet`/`Incr`/`k`/`x`/`c`/`F` handling in `handle_connection`) is
/// handed to (issue #219). Unlike `migration_tx` — reserved for the one
/// singleton bulk-migration task and so never needs more than a handful of
/// slots — an arbitrary number of connections can each be forwarding a
/// write to a migrating key at once, and each forward can run for up to
/// `KEY_TRANSFER_ATTEMPTS` x `FORWARD_TIMEOUT` before it gives up. A
/// generous fixed capacity keeps a burst of concurrent forwards from
/// spilling into `spawn_forward`'s waiter path (see
/// `MAX_PENDING_FORWARD_WAITERS`) under ordinary load, while still
/// bounding the total number of forward tasks `run` will ever have in
/// flight at once.
const FORWARD_CHANNEL_CAPACITY: usize = 256;
/// Caps the number of detached "waiter" tasks `spawn_forward` may have
/// outstanding at once — see `PENDING_FORWARD_WAITERS` and issue #219's
/// follow-up discussion. A forward that finds `forward_tx` full is never
/// simply dropped (that would lose the write on the joiner/entrant, the
/// same class of silent data loss issue #176 fixed for `MultiSet`);
/// instead a waiter task blocks on the channel's ordinary
/// `send(...).await` in the background until a slot frees up, so the
/// connection that triggered the forward still never blocks. This bound
/// exists only to cap unbounded task/memory growth in the pathological
/// case where `forward_tx` stays saturated for a long time — past it, a
/// forward is finally dropped (logged). `4096` is generous relative to
/// `FORWARD_CHANNEL_CAPACITY` (16x) precisely because dropping here is
/// the fallback of last resort, not the normal backpressure path.
const MAX_PENDING_FORWARD_WAITERS: usize = 4096;
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

/// A `run_migration` or `forward_with_retries` invocation, boxed so
/// `handle_connection` can hand it to `run`'s own loop over
/// `ConnectionConfig::migration_tx`/`forward_tx` instead of spawning it
/// directly — see those fields' doc comments for why.
type MigrationTask = Pin<Box<dyn Future<Output = ()> + Send>>;

/// Issue #266: a `spawn_or_supersede_rereplication` invocation, boxed so
/// `register_with_discovery` can hand it to `send_heartbeats`'s own
/// `JoinSet` over its `rereplication_tx` channel — same shape and
/// reasoning as `MigrationTask`, just a different producer/consumer pair.
type RereplicationTask = Pin<Box<dyn Future<Output = ()> + Send>>;

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
    /// Where `handle_connection` hands off the one singleton `run_migration`
    /// future (from an incoming `M`) for `run`'s own loop to
    /// `connection_tasks.spawn` — spawning it directly from inside a
    /// connection task (as opposed to from `run`) would leave it untracked
    /// by `connection_tasks`, so graceful shutdown couldn't wait for it (or
    /// ask it to unwind cleanly) before the process exits. Kept small
    /// (see where `run` creates it) since only one bulk migration ever
    /// runs at a time.
    ///
    /// Per-write forwards (`forward_with_retries`) go through `forward_tx`
    /// instead — see that field and issue #219 for why sharing this one
    /// channel between the two used to cause head-of-line blocking.
    migration_tx: mpsc::Sender<MigrationTask>,
    /// Where `handle_connection` hands off a `forward_with_retries` future
    /// — one per concurrent `S`/`D`/`U`/`u`/`MultiSet`/`Incr`/`k`/`x`/`c`/`F`
    /// that lands on a key mid-handoff or mid-drain — for `run`'s own loop
    /// to `connection_tasks.spawn`, same tracking reason as `migration_tx`.
    ///
    /// Issue #219: this used to be the same channel as `migration_tx`,
    /// sized for "one migration in flight". That's fine for the singleton
    /// bulk-migration task, but every per-write forward shared it too, and
    /// each can occupy a slot for up to `KEY_TRANSFER_ATTEMPTS` x
    /// `FORWARD_TIMEOUT` when the peer is slow — so a handful of stalled
    /// forwards could leave a *different* client connection's
    /// `handle_connection` blocked on `send().await`, well after that
    /// connection's own write had already been acked. Splitting the
    /// channel bounds that stall to zero: `spawn_forward` sends with
    /// `try_send`, never `.await`, so the connection that triggered a
    /// forward never blocks on this channel either way.
    ///
    /// The guarantee this channel's consumer (and `spawn_forward`) upholds
    /// is that a forward is never silently dropped just because
    /// `forward_tx` was momentarily full — see `spawn_forward` and
    /// `MAX_PENDING_FORWARD_WAITERS` for how a full channel is handled
    /// without either blocking the caller or losing the write.
    forward_tx: mpsc::Sender<MigrationTask>,
}

/// Count of `spawn_forward` waiter tasks currently blocked on
/// `forward_tx.send(...).await`, waiting for a slot `forward_tx`'s
/// consumer frees up — see `MAX_PENDING_FORWARD_WAITERS`.
static PENDING_FORWARD_WAITERS: AtomicUsize = AtomicUsize::new(0);

/// Hands a per-write forward (`forward_with_retries`, wrapping a `Set`/
/// `Delete`/`Clear` racing a migration or decommission drain) to `run`'s
/// dedicated `forward_tx` consumer loop via `try_send` — never `.await`,
/// unlike `ConnectionConfig::migration_tx`. See `forward_tx`'s own doc
/// comment (issue #219) for why blocking here would reintroduce the exact
/// head-of-line stall `forward_with_retries` itself exists to avoid: this
/// call happens *after* `handle_connection` has already written the
/// client's response for the command that triggered it, so the only thing
/// waiting on it is that same connection's ability to read its *next*
/// request.
///
/// **Guarantee**: a forward is never dropped short of
/// `MAX_PENDING_FORWARD_WAITERS`. Dropping a forward outright on a full
/// channel would lose that write on the joiner/entrant — the same class
/// of silent data loss issue #176 fixed for `MultiSet` — which is a much
/// worse failure than merely delaying it (the old, pre-#219 shared-channel
/// behavior did exactly that: it stalled the caller rather than dropping,
/// just on the wrong channel). So a `TrySendError::Full` doesn't drop the
/// task — it spawns a detached "waiter" that blocks on the channel's
/// ordinary `send(...).await` in the background, bounded by
/// `PENDING_FORWARD_WAITERS`/`MAX_PENDING_FORWARD_WAITERS` so a channel
/// saturated for a long time can't grow waiter tasks (and their captured
/// `NodeContext`/`Bytes` state) without limit. Only past that bound does a
/// forward actually get dropped, logged with the key (or clear scope) it
/// was for.
///
/// A `TrySendError::Closed` (the consumer — `run`'s own loop — is gone,
/// e.g. mid-shutdown past the point `forward_tx` itself is dropped) is
/// different: nothing will ever drain the channel again regardless of how
/// long a waiter waited, so that case drops immediately without spawning
/// one.
fn spawn_forward(
    config: &ConnectionConfig,
    node_context: NodeContext,
    target: ForwardTarget,
    write: OwnedForwardedWrite,
) {
    // Computed before `write` moves into `forward_with_retries` below —
    // only actually rendered if the rare drop path at the bottom needs it.
    let description = write.describe();
    let task: MigrationTask = Box::pin(forward_with_retries(node_context, target, write));

    match config.forward_tx.try_send(task) {
        Ok(()) => {}
        // The consumer (`run`'s own loop) is gone — nothing will ever
        // drain this channel again regardless of how long a waiter
        // waited, so there's no point spawning one.
        Err(mpsc::error::TrySendError::Closed(_)) => {}
        Err(mpsc::error::TrySendError::Full(task)) => {
            if PENDING_FORWARD_WAITERS.fetch_add(1, Ordering::SeqCst) < MAX_PENDING_FORWARD_WAITERS
            {
                let forward_tx = config.forward_tx.clone();
                tokio::spawn(async move {
                    // Best-effort: if the send itself fails (the consumer
                    // closed the channel while this waiter was queued),
                    // there's nothing left to do — same as the
                    // `TrySendError::Closed` case above.
                    let _ = forward_tx.send(task).await;
                    PENDING_FORWARD_WAITERS.fetch_sub(1, Ordering::SeqCst);
                });
            } else {
                PENDING_FORWARD_WAITERS.fetch_sub(1, Ordering::SeqCst);
                eprintln!(
                    "WARN dropped a concurrent write forward for {description}: forward_tx is \
                     full and {MAX_PENDING_FORWARD_WAITERS} waiters are already queued behind it"
                );
            }
        }
    }
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
    /// too. Also refreshed from the roster the primary discovery server
    /// returns with every heartbeat ack (issue #61, `adopt_membership`),
    /// which is how it follows liveness *evictions* — `M` only ever
    /// carries joins. `None` until the first of either — a lone or
    /// freshly-bootstrapped node has no membership to reject against
    /// yet.
    known_ring: KnownRing,
    auth_secret: Option<Bytes>,
    tls_connector: Option<TlsConnector>,
    request_tx: mpsc::Sender<CacheRequest>,
    /// Issue #124: set for the whole decommission (drain-out handoff +
    /// grace) — `leave_target_for` forwards concurrent writes through
    /// it, the `M` handler rejects new joins while it is set, and
    /// `/readyz` reports not-ready.
    leaving: Arc<Mutex<Option<LeaveState>>>,
    /// Issue #266: a `run_rereplication` in flight, if any — set by
    /// `spawn_or_supersede_rereplication` when `adopt_membership` reports
    /// a ring change that dropped a member (an eviction, or a leave this
    /// node did not itself hand off for), cleared once that run finishes.
    /// `run_migration` waits (bounded) for this to clear before it starts
    /// marking dead copies, so a join's sweep can't reclaim the sender's
    /// only copy of a key the re-replication hasn't delivered to the
    /// ring's newly promoted owner yet — see both functions' doc
    /// comments.
    active_rereplication: Arc<Mutex<Option<Arc<ActiveRereplication>>>>,
    /// Issue #266: how a task that spawns a re-replication (either
    /// `register_with_discovery`, on an eviction-driven ring change, or
    /// `run_migration`, on the join-flip gap below) hands it to
    /// `send_heartbeats`'s own `JoinSet` — see that field's doc comment
    /// on `RereplicationTask` for why neither producer has a `JoinSet`
    /// of its own suitable for a possibly-long-running task like this.
    /// A clone of the same sender `run` creates once and threads to both
    /// this struct and `send_heartbeats`'s receiving end.
    rereplication_tx: mpsc::Sender<RereplicationTask>,
    /// This node's shutdown signal (the same `watch::Receiver` every
    /// other background task observes) — carried here so a task spawned
    /// well after `NodeContext` was built (`run_migration`'s own
    /// join-triggered re-replication, issue #266) can still stop
    /// promptly at shutdown without a bespoke parameter thread of its
    /// own.
    shutdown_rx: watch::Receiver<bool>,
}

/// Issue #124: a decommission in progress. The mirror of
/// `ActiveMigration`, but self-contained: the leaver computes, from the
/// roster alone, which single node newly enters each of its keys' top-R
/// once it is gone (removing a node from an HRW ranking can only
/// promote the previous rank-R+1 node), and hands that node the entry —
/// the surviving owners already hold their copies.
struct LeaveState {
    /// The roster including this node — what routing looked like when
    /// the drain began.
    before_ring: Arc<HashRing>,
    /// The roster minus this node — where each key lives afterwards.
    after_ring: Arc<HashRing>,
    replication: usize,
    /// name → address for every member, to dial entrants.
    addresses: HashMap<String, String>,
    /// Issue #295: name → membership token for every member, so a
    /// concurrent write forwarded to an entrant (`leave_target_for`) can
    /// authorize its `U`/`u` the same way the bulk drain-out's own sends
    /// do — see `Command::HandoffSet::token`.
    tokens: HashMap<String, String>,
    /// One shared forwarding connection per entrant address, reused by
    /// every concurrent write forwarded during the drain (mirrors
    /// `ActiveMigration::forward_connection`).
    connections: Mutex<HashMap<String, Arc<AsyncMutex<Option<ClientStream>>>>>,
}

impl LeaveState {
    /// The node that newly enters `key`'s top-R when this node leaves —
    /// `None` when this node wasn't an owner (nothing moves) or the
    /// cluster is too small for a replacement (the survivors are all
    /// owners already).
    fn entrant_for(&self, key: &Key, self_name: &str) -> Option<String> {
        if !self.before_ring.is_owner(key, self_name, self.replication) {
            return None;
        }
        self.after_ring
            .owners(key, self.replication)
            .into_iter()
            .find(|owner| !self.before_ring.is_owner(key, owner, self.replication))
            .map(|owner| owner.to_string())
    }
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

#[allow(clippy::too_many_arguments)]
pub(crate) async fn run(
    address: &str,
    heartbeat: Option<HeartbeatConfig>,
    auth_secret: Option<Bytes>,
    tls_acceptor: Option<TlsAcceptor>,
    max_memory_bytes: usize,
    metrics_address: Option<String>,
    drain_timeout: Duration,
    limits: ConnectionLimits,
    namespace_budgets: Vec<(Bytes, usize)>,
) -> io::Result<()> {
    let listener = TcpListener::bind(address).await?;

    let (request_tx, request_rx) = mpsc::channel(1024);
    let connection_limit = Arc::new(Semaphore::new(limits.max_connections));
    let per_ip_connections: PerIpConnections = Arc::new(Mutex::new(HashMap::new()));
    let mut connection_tasks = JoinSet::new();
    let (shutdown_tx, shutdown_rx) = watch::channel(false);

    let cache_task = tokio::spawn(run_cache(request_rx, max_memory_bytes, namespace_budgets));

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

    // Issue #266: created once here (mirroring `migration_tx`/
    // `forward_tx` below) rather than inside `send_heartbeats`, so both
    // `NodeContext` (whose clones are how two different producers —
    // `register_with_discovery` on an eviction-driven ring change, and
    // `run_migration` on its own join-flip gap, see its doc comment —
    // hand a re-replication task off) and `send_heartbeats` (which
    // drains it into its own `JoinSet`) share the one channel.
    let (rereplication_tx, rereplication_rx) = mpsc::channel::<RereplicationTask>(4);

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
        leaving: Arc::new(Mutex::new(None)),
        active_rereplication: Arc::new(Mutex::new(None)),
        rereplication_tx: rereplication_tx.clone(),
        shutdown_rx: shutdown_rx.clone(),
    });
    // This function's own copy must go, or `send_heartbeats`'s drain of
    // `rereplication_rx` would never see the channel close — every real
    // producer holds its own clone via `NodeContext` (or a clone of it),
    // and this binding otherwise outlives even `heartbeat_task.await`
    // below, in `run`'s own shutdown sequence.
    drop(rereplication_tx);

    // Issue #124: the operations sidecar — Prometheus-format /metrics
    // plus /healthz//readyz probes on their own listener, so scraping
    // never competes for (or counts against) client connection permits.
    let metrics_task = match &metrics_address {
        Some(metrics_address) => {
            let metrics_listener = TcpListener::bind(metrics_address.as_str()).await?;
            println!("INFO metrics endpoint listening on {metrics_address}");
            Some(tokio::spawn(run_metrics_server(
                metrics_listener,
                request_tx.clone(),
                Arc::clone(&connection_limit),
                limits.max_connections,
                Arc::clone(&known_ring),
                node_context.is_some(),
                shutdown_rx.clone(),
            )))
        }
        None => None,
    };

    // Issue #124: the decommission needs every discovery replica, and
    // `heartbeat` is consumed by the task below — keep a copy.
    let discovery_addrs_for_leave = heartbeat
        .as_ref()
        .map(|config| config.discovery_addrs.clone());

    // Issue #124: heartbeats get their own stop signal, flipped either
    // at normal shutdown or the moment a decommission begins — a
    // heartbeat surviving past the leave would be answered as an
    // unknown node and re-register, quietly re-joining the cluster the
    // node just left.
    let (heartbeat_stop_tx, heartbeat_stop_rx) = watch::channel(false);
    let heartbeat_task = match (heartbeat, &node_context) {
        (Some(config), Some(node_context)) => Some(tokio::spawn(send_heartbeats(
            config,
            node_context.clone(),
            rereplication_rx,
            heartbeat_stop_rx,
        ))),
        _ => None,
    };

    // Buffered rather than unbounded: staged node join allows only one migration in
    // flight per node (see `NodeContext::active_migration`), so a handful of
    // slots is already more than a well-behaved cluster would ever need at
    // once. Per-write forwards do NOT go through this channel — see
    // `forward_tx` below and issue #219.
    let (migration_tx, mut migration_rx) = mpsc::channel::<MigrationTask>(4);
    // Issue #219: a separate, more generously sized channel for per-write
    // forwards (`forward_with_retries`), so a burst of them stalled on a
    // slow peer can never delay the singleton bulk-migration task above —
    // or, before this split, block an unrelated client connection's
    // `spawn_forward` call outright. See `ConnectionConfig::forward_tx`.
    let (forward_tx, mut forward_rx) = mpsc::channel::<MigrationTask>(FORWARD_CHANNEL_CAPACITY);

    let connection_config = ConnectionConfig {
        idle_timeout: IDLE_TIMEOUT,
        auth_secret,
        tls_acceptor,
        node_context: node_context.clone(),
        migration_tx,
        forward_tx,
    };

    let shutdown = shutdown_signal();
    tokio::pin!(shutdown);

    // Issue #124: set once the decommission has been spawned; its
    // completion signal re-enters the loop below to run the ordinary
    // shutdown. A second signal while draining falls through to the
    // immediate path (operator override).
    let mut decommission_started = false;
    let (decommission_done_tx, mut decommission_done_rx) = watch::channel(false);

    loop {
        tokio::select! {
            biased;

            result = &mut shutdown, if !decommission_started => {
                result?;

                // Issue #124: a clustered node with a drain budget
                // decommissions first — hand entries to their post-leave
                // owners, leave membership, then run this same shutdown.
                // The accept loop keeps running throughout (serving and
                // write-forwarding during the drain is the point), so
                // the decommission is spawned and its completion loops
                // back in via `decommission_done`.
                if let (Some(node_context), Some(discovery_addrs), false) = (
                    &node_context,
                    &discovery_addrs_for_leave,
                    decommission_started,
                ) && !drain_timeout.is_zero()
                {
                    println!(
                        "INFO shutdown signal received — decommissioning (budget {}s)",
                        drain_timeout.as_secs()
                    );
                    decommission_started = true;
                    // Stop heartbeating before anything else: a
                    // heartbeat landing after the V would re-register
                    // this node (see heartbeat_stop_tx above).
                    heartbeat_stop_tx.send_replace(true);
                    let node_context = node_context.clone();
                    let discovery_addrs = discovery_addrs.clone();
                    let done = decommission_done_tx.clone();
                    tokio::spawn(async move {
                        run_decommission(node_context, discovery_addrs, drain_timeout).await;
                        let _ = done.send(true);
                    });
                    continue;
                }

                println!("INFO shutdown signal received");
                heartbeat_stop_tx.send_replace(true);
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

            _ = decommission_done_rx.changed(), if decommission_started => {
                println!("INFO decommission finished — shutting down");
                heartbeat_stop_tx.send_replace(true);
                shutdown_tx.send_replace(true);
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

            Some(task) = forward_rx.recv() => {
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
                    limits.max_connections_per_ip,
                    connection_config.clone(),
                    shutdown_rx.clone(),
                    &mut connection_tasks,
                );
            }

        }
    }

    // Keep servicing `migration_rx`/`forward_rx` while draining: a
    // connection task that is mid-request when shutdown lands may still
    // hand a forwarded write (or the tail of a handoff) to one of these
    // channels, and with the main loop gone nobody would spawn it — the
    // client would have its `S`/`D` acked and the forward silently
    // dropped. `forward_tx` sends via `try_send` (see `spawn_forward`) so
    // it was never going to block a connection task regardless, but
    // draining it here still lets a forward queued right before shutdown
    // actually run instead of being dropped.
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
                Some(task) = forward_rx.recv() => {
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
    // Same deadlock, second holder (issue #124): `connection_config` now
    // only *clones* the context (the decommission path needs its own
    // copy), so the original binding still holds a `request_tx` here.
    drop(node_context);

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

    if let Some(metrics_task) = metrics_task {
        // The metrics server observes the same shutdown signal; nothing
        // to flush, just don't leave the task behind.
        metrics_task.abort();
        let _ = metrics_task.await;
    }

    Ok(())
}

/// Live connection counts per source IP, backing `DEFAULT_MAX_CONNECTIONS_PER_IP`
/// (see that constant). A plain `Mutex<HashMap<..>>` rather than anything
/// fancier: every access here is a brief increment/decrement with no I/O
/// under the lock, and every accepted connection already pays for a
/// `Semaphore` acquisition on the shared `connection_limit`, so this adds
/// no bottleneck relative to that existing one.
type PerIpConnections = Arc<Mutex<HashMap<IpAddr, usize>>>;

/// Releases one `DEFAULT_MAX_CONNECTIONS_PER_IP` slot on drop — the per-IP
/// counterpart to the `Semaphore` permit `dispatch_connection` already
/// holds for `DEFAULT_MAX_CONNECTIONS` (`_connection_permit`, which frees itself
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

/// Reserves one of `cap` (`--max-connections-per-ip`) slots for `ip`, or
/// `None` if it's already at the cap — see
/// `DEFAULT_MAX_CONNECTIONS_PER_IP`.
fn try_acquire_per_ip(
    counts: &PerIpConnections,
    ip: IpAddr,
    cap: usize,
) -> Option<PerIpConnectionGuard> {
    let mut guard = counts
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());

    let count = guard.entry(ip).or_insert(0);
    if *count >= cap {
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
/// (`DEFAULT_MAX_CONNECTIONS` and, per source IP, `DEFAULT_MAX_CONNECTIONS_PER_IP`). A
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
    max_connections_per_ip: usize,
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
        // `DEFAULT_MAX_CONNECTIONS` just by dialing and stalling — only a
        // permit-holding connection ever performs one.
        let permit = match connection_limit.try_acquire_owned() {
            Ok(permit) => permit,
            Err(_) => {
                reject_over_limit(stream, address, &config.tls_acceptor).await;
                return;
            }
        };

        // No per-source-IP connection limit: without this, a single
        // source could hold `DEFAULT_MAX_CONNECTIONS` connections all by itself
        // and starve every other client, even though the global
        // semaphore above isn't literally exhausted until the very last
        // one. Reserved before the TLS handshake for the same reason as
        // the global permit (issue #5) — see `DEFAULT_MAX_CONNECTIONS_PER_IP`.
        let per_ip_permit =
            match try_acquire_per_ip(&per_ip_connections, address.ip(), max_connections_per_ip) {
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
            log_connection_error(&address, &error);
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

/// A peer that closes the TCP connection without a TLS `close_notify` —
/// which is how every SDK and node ends a connection — is reported by
/// rustls as an error, and logging that at WARN on every ordinary
/// disconnect buried real problems (issue #68). Noted at INFO instead;
/// everything else stays a WARN.
fn log_connection_error(address: &SocketAddr, error: &io::Error) {
    let text = error.to_string();
    if text.contains("close_notify") {
        println!("INFO connection from {address} closed without TLS close_notify");
    } else {
        eprintln!("WARN connection error from {address}: {error}");
    }
}

/// Bounds every response write in `handle_connection` (issue #4): the read
/// side already has `IDLE_TIMEOUT`, but an unbounded `write_all` let a peer
/// that stops reading (without closing the TCP connection — e.g. a full
/// receive buffer) hold this connection's `DEFAULT_MAX_CONNECTIONS` permit forever.
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
/// Issue #124: the node's operations endpoint — a deliberately minimal,
/// dependency-free HTTP/1.1 responder (the server's zero-dependency
/// policy rules out an HTTP crate, and Prometheus' text exposition
/// format needs nothing more). Three paths:
///
/// - `GET /metrics` — Prometheus text format v0.0.4: memory, entries,
///   operation counters, connection occupancy, and one gauge pair per
///   live namespace (see `Cache::stats`).
/// - `GET /healthz` — liveness: `200` while the process serves at all.
/// - `GET /readyz` — readiness: `200` once this node can serve clients —
///   standalone immediately, a cluster node once it has adopted a
///   membership view (`known_ring`); `503` before that, so an
///   orchestrator keeps it out of rotation while it is still joining.
///
/// Runs on its own listener (`--metrics-port`): scrapes never compete
/// for, or count against, client connection permits, and the port can
/// stay unexposed to clients. No auth — the exposition is operational
/// telemetry, and the deployment guide says to keep the port internal.
async fn run_metrics_server(
    listener: TcpListener,
    request_tx: mpsc::Sender<CacheRequest>,
    connection_limit: Arc<Semaphore>,
    max_connections: usize,
    known_ring: KnownRing,
    is_cluster: bool,
    mut shutdown_rx: watch::Receiver<bool>,
) {
    loop {
        let accepted = tokio::select! {
            accepted = listener.accept() => accepted,
            _ = shutdown_rx.changed() => return,
        };
        let (stream, _) = match accepted {
            Ok(pair) => pair,
            Err(error) => {
                // Issue #184: mirrors `run`'s own accept loop (see
                // `should_backoff_after_accept_error`'s doc comment) — an
                // unadorned `continue` here would busy-loop this task hot
                // under EMFILE/ENFILE instead of backing off, making
                // recovery harder right when file descriptors are already
                // scarce.
                if should_backoff_after_accept_error(&error) {
                    sleep(ACCEPT_ERROR_BACKOFF).await;
                }
                continue;
            }
        };

        let request_tx = request_tx.clone();
        let connection_limit = Arc::clone(&connection_limit);
        let known_ring = Arc::clone(&known_ring);
        tokio::spawn(async move {
            let _ = timeout(
                Duration::from_secs(5),
                serve_metrics_connection(
                    stream,
                    request_tx,
                    connection_limit,
                    max_connections,
                    known_ring,
                    is_cluster,
                ),
            )
            .await;
        });
    }
}

async fn serve_metrics_connection(
    mut stream: TcpStream,
    request_tx: mpsc::Sender<CacheRequest>,
    connection_limit: Arc<Semaphore>,
    max_connections: usize,
    known_ring: KnownRing,
    is_cluster: bool,
) -> io::Result<()> {
    let path = read_http_request_path(&mut stream).await?;

    let (status, body): (&str, String) = match path.as_str() {
        "/metrics" => match execute_command(&request_tx, Command::Stats).await {
            Ok(Response::Stats(stats)) => {
                let connections =
                    max_connections.saturating_sub(connection_limit.available_permits());
                (
                    "200 OK",
                    render_node_metrics(&stats, connections, max_connections),
                )
            }
            _ => (
                "500 Internal Server Error",
                "cache actor unavailable\n".to_string(),
            ),
        },
        "/healthz" => ("200 OK", "ok\n".to_string()),
        "/readyz" => {
            let ready = !is_cluster
                || known_ring
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner())
                    .is_some();
            if ready {
                ("200 OK", "ok\n".to_string())
            } else {
                ("503 Service Unavailable", "joining\n".to_string())
            }
        }
        _ => ("404 Not Found", "not found\n".to_string()),
    };

    write_http_response(&mut stream, status, &body).await
}

/// Reads one HTTP request's head (bounded) and returns the GET path.
/// Anything that isn't a small, well-formed GET is an error — this is a
/// scrape endpoint, not a web server.
async fn read_http_request_path(stream: &mut TcpStream) -> io::Result<String> {
    let mut head = Vec::new();
    let mut chunk = [0u8; 1024];
    while !head.windows(4).any(|window| window == b"\r\n\r\n") {
        if head.len() > 8192 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "oversized http request head",
            ));
        }
        let bytes_read = stream.read(&mut chunk).await?;
        if bytes_read == 0 {
            break;
        }
        head.extend_from_slice(&chunk[..bytes_read]);
    }

    let head = String::from_utf8_lossy(&head);
    let request_line = head.lines().next().unwrap_or_default();
    let mut parts = request_line.split(' ');
    match (parts.next(), parts.next()) {
        (Some("GET"), Some(path)) => Ok(path.to_string()),
        _ => Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "not a GET request",
        )),
    }
}

async fn write_http_response(stream: &mut TcpStream, status: &str, body: &str) -> io::Result<()> {
    let response = format!(
        "HTTP/1.1 {status}\r\nContent-Type: text/plain; version=0.0.4; charset=utf-8\r\n\
         Content-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    );
    stream.write_all(response.as_bytes()).await?;
    stream.shutdown().await
}

/// Prometheus label values allow any UTF-8 with `\\`, `\"` and newline
/// escaped; namespaces are arbitrary bytes, so non-UTF-8 goes through
/// lossy replacement first.
fn metrics_label_escape(raw: &[u8]) -> String {
    String::from_utf8_lossy(raw)
        .replace('\\', "\\\\")
        .replace('"', "\\\"")
        .replace('\n', "\\n")
}

fn render_node_metrics(
    stats: &crate::cache::CacheStats,
    connections: usize,
    max_connections: usize,
) -> String {
    let mut out = String::new();
    let mut metric = |name: &str, kind: &str, help: &str, value: String| {
        out.push_str(&format!(
            "# HELP {name} {help}\n# TYPE {name} {kind}\n{value}"
        ));
    };

    metric(
        "nanocached_node_memory_used_bytes",
        "gauge",
        "Bytes of cache memory in use (keys + values + per-entry overhead).",
        format!("nanocached_node_memory_used_bytes {}\n", stats.used_bytes),
    );
    metric(
        "nanocached_node_memory_max_bytes",
        "gauge",
        "The --max-memory bound.",
        format!(
            "nanocached_node_memory_max_bytes {}\n",
            stats.max_memory_bytes
        ),
    );
    metric(
        "nanocached_node_entries",
        "gauge",
        "Live entries across every namespace.",
        format!("nanocached_node_entries {}\n", stats.entries),
    );
    metric(
        "nanocached_node_connections",
        "gauge",
        "Client connections currently held (of the connection limit).",
        format!("nanocached_node_connections {connections}\n"),
    );
    metric(
        "nanocached_node_connections_max",
        "gauge",
        "The --max-connections bound (issue #126) — the proxy exports the \
         same pair, so utilization dashboards can treat both tiers alike.",
        format!("nanocached_node_connections_max {max_connections}\n"),
    );
    metric(
        "nanocached_node_hits_total",
        "counter",
        "GET requests answered with a value.",
        format!("nanocached_node_hits_total {}\n", stats.hits),
    );
    metric(
        "nanocached_node_misses_total",
        "counter",
        "GET requests answered not-found (expired included).",
        format!("nanocached_node_misses_total {}\n", stats.misses),
    );
    metric(
        "nanocached_node_sets_total",
        "counter",
        "Stored writes.",
        format!("nanocached_node_sets_total {}\n", stats.sets),
    );
    metric(
        "nanocached_node_deletes_total",
        "counter",
        "Deletes that removed a live entry.",
        format!("nanocached_node_deletes_total {}\n", stats.deletes),
    );
    metric(
        "nanocached_node_evictions_total",
        "counter",
        "Entries evicted by the memory bound.",
        format!("nanocached_node_evictions_total {}\n", stats.evictions),
    );
    metric(
        "nanocached_node_expirations_total",
        "counter",
        "Entries removed because their TTL passed.",
        format!("nanocached_node_expirations_total {}\n", stats.expirations),
    );
    metric(
        "nanocached_node_incrs_total",
        "counter",
        "Successful INCR operations (issue #129) — a stored value that \
         wasn't INCR's counter grammar, or an overflowing delta, isn't \
         counted here.",
        format!("nanocached_node_incrs_total {}\n", stats.incrs),
    );
    metric(
        "nanocached_node_cas_sets_total",
        "counter",
        "Successful k (compare-and-set) writes (issue #141) — a mismatched \
         condition isn't counted here.",
        format!("nanocached_node_cas_sets_total {}\n", stats.cas_sets),
    );
    metric(
        "nanocached_node_cas_deletes_total",
        "counter",
        "Successful x (compare-and-delete) removals (issue #141) — a \
         mismatched condition or missing key isn't counted here.",
        format!("nanocached_node_cas_deletes_total {}\n", stats.cas_deletes),
    );

    let mut namespace_entries = String::new();
    let mut namespace_bytes = String::new();
    let mut namespace_budgets = String::new();
    for namespace in &stats.namespaces {
        let label = metrics_label_escape(&namespace.namespace);
        namespace_entries.push_str(&format!(
            "nanocached_node_namespace_entries{{namespace=\"{label}\"}} {}\n",
            namespace.entries
        ));
        namespace_bytes.push_str(&format!(
            "nanocached_node_namespace_used_bytes{{namespace=\"{label}\"}} {}\n",
            namespace.used_bytes
        ));
        if let Some(budget) = namespace.budget_bytes {
            namespace_budgets.push_str(&format!(
                "nanocached_node_namespace_budget_bytes{{namespace=\"{label}\"}} {budget}\n",
            ));
        }
    }
    if !stats.namespaces.is_empty() {
        metric(
            "nanocached_node_namespace_entries",
            "gauge",
            "Live entries per namespace (empty label = the default namespace).",
            namespace_entries,
        );
        metric(
            "nanocached_node_namespace_used_bytes",
            "gauge",
            "Bytes per namespace.",
            namespace_bytes,
        );
    }
    if !namespace_budgets.is_empty() {
        metric(
            "nanocached_node_namespace_budget_bytes",
            "gauge",
            "The --namespace-budget cap, for namespaces that have one \
             (issue #127).",
            namespace_budgets,
        );
    }

    out
}

/// `c`/`F` (issue #106): applied to this node's own store unconditionally
/// — a clear isn't key-addressed, so there is no wrong-node check; the
/// client fans it out to every member and each drops its own sub-map —
/// then replayed on the joiner of an in-flight handoff, if any, so a
/// clear racing a migration can't resurrect entries there. See
/// `route_clear` for which path the replay takes.
async fn handle_clear(
    stream: &mut ServerStream,
    request_tx: &mpsc::Sender<CacheRequest>,
    config: &ConnectionConfig,
    scope: ClearScope,
    tag: Option<u32>,
) -> io::Result<()> {
    let command = match &scope {
        ClearScope::Namespace(namespace) => Command::Clear {
            namespace: namespace.clone(),
        },
        ClearScope::All => Command::ClearAll,
    };
    let response = execute_command(request_tx, command).await?;
    write_response(stream, &encode_response(&response, tag)).await?;

    if let Some(node_context) = &config.node_context
        && let ClearRoute::Forward(target) = route_clear(node_context, &scope)
    {
        spawn_forward(
            config,
            node_context.clone(),
            target,
            OwnedForwardedWrite::Clear(scope),
        );
    }

    Ok(())
}

/// Where a clear's replay to an in-flight handoff's joiner goes.
enum ClearRoute {
    /// No handoff is forwarding: nothing to replay.
    None,
    /// Queued on the slot for the transfer loop to replay in order — see
    /// `ActiveMigration::pending_clears`.
    Queued,
    /// This node's own transfer is done; forward on the shared
    /// connection like a concurrent `S`/`D`.
    Forward(ForwardTarget),
}

fn route_clear(node_context: &NodeContext, scope: &ClearScope) -> ClearRoute {
    let mut slot = node_context
        .active_migration
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());

    // Same lazy expiry as `migration_target_for`.
    if slot.as_ref().is_some_and(ActiveMigration::expired) {
        *slot = None;
    }

    let Some(active) = slot.as_mut() else {
        return ClearRoute::None;
    };

    if active.completed_at.is_none() {
        active.pending_clears.push(scope.clone());
        return ClearRoute::Queued;
    }

    if !active.forwarding_open() {
        return ClearRoute::None;
    }

    ClearRoute::Forward(ForwardTarget {
        addr: active.joining_addr.clone(),
        connection: Arc::clone(&active.forward_connection),
        token: active.joining_token.clone(),
    })
}

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
            Ok((
                Command::Auth {
                    secret,
                    tagging,
                    // Issue #125: accepted so the `A ... T R` probe
                    // succeeds against a node without a fallback round
                    // trip, but unused — the node has no transient
                    // per-request failure to report and never emits `R`
                    // (the proxy is the emitter today).
                    retry_capable: _,
                },
                _,
            )) => {
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
                    joining_token,
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

                // Issue #124: a decommissioning node must not take on a
                // new join handoff — its own drain-out is moving the
                // very entries a join would want it to send. Rejecting
                // lets discovery's retry land once this node has left.
                if node_context
                    .leaving
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner())
                    .is_some()
                {
                    write_response(&mut stream, &Response::MigrationRejected.encode()).await?;
                    continue;
                }

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
                    joining_token.clone(),
                    Arc::clone(&after_ring),
                    replication,
                    &joined,
                    &node_context.known_ring,
                ) {
                    MigrationOutcome::New { guard, restore } => {
                        // Issue #62: before the key snapshot below, so
                        // the restored copies are listed — and re-sent
                        // if this joiner owns them.
                        for key in &restore {
                            unmark_migrated(&node_context.request_tx, key).await;
                        }
                        guard
                    }
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
                        joining_token,
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
                if let Some(restore) = abandon_migration(&node_context, &joining_name) {
                    eprintln!(
                        "WARN join of {joining_name} abandoned by discovery after this node's \
                         handoff completed; restoring {} dead copies",
                        restore.len()
                    );
                    for key in &restore {
                        unmark_migrated(&node_context.request_tx, key).await;
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
            // Issue #128 measurement prototype: per-key wrong-node
            // filtering happens here, before the actor ever sees the
            // frame — same check `Get`'s arm makes per key, just batched.
            // Keys this node owns go to the actor as one `Command::MultiGet`
            // (one round trip for all of them); keys it doesn't are
            // answered `MultiEntry::WrongNode` without ever reaching the
            // actor. Results are spliced back into the original request
            // order before replying — the client never sees the owned/
            // not-owned split.
            Ok((Command::MultiGet { namespace, keys }, tag)) => {
                let mut entries: Vec<Option<MultiEntry>> = vec![None; keys.len()];
                let mut owned_positions = Vec::with_capacity(keys.len());
                let mut owned_keys = Vec::with_capacity(keys.len());

                for (position, name) in keys.into_iter().enumerate() {
                    let wrong = config.node_context.as_ref().is_some_and(|node_context| {
                        wrong_node(node_context, &Key::new(namespace.clone(), name.clone()))
                    });

                    if wrong {
                        entries[position] = Some(MultiEntry::WrongNode);
                    } else {
                        owned_positions.push(position);
                        owned_keys.push(name);
                    }
                }

                if !owned_keys.is_empty() {
                    let response = execute_command(
                        &request_tx,
                        Command::MultiGet {
                            namespace,
                            keys: owned_keys,
                        },
                    )
                    .await?;

                    let Response::Multi(results) = response else {
                        unreachable!("Command::MultiGet always answers with Response::Multi");
                    };

                    for (position, result) in owned_positions.into_iter().zip(results) {
                        entries[position] = Some(result);
                    }
                }

                let entries = entries
                    .into_iter()
                    .map(|entry| entry.expect("every position filled by one of the two branches"))
                    .collect();
                write_response(
                    &mut stream,
                    &encode_response(&Response::Multi(entries), tag),
                )
                .await?;

                continue;
            }
            // Issue #150: same per-key wrong-node filtering as
            // `MultiGet`'s arm above — keys this node owns go to the
            // actor as one `Command::MultiSet`; keys it doesn't are
            // answered `MultiAckEntry::WrongNode` without ever reaching
            // the actor.
            Ok((
                Command::MultiSet {
                    namespace,
                    keys,
                    values,
                    ttl,
                },
                tag,
            )) => {
                let mut entries: Vec<Option<MultiAckEntry>> = vec![None; keys.len()];
                let mut owned_positions = Vec::with_capacity(keys.len());
                let mut owned_keys = Vec::with_capacity(keys.len());
                let mut owned_values = Vec::with_capacity(keys.len());

                for (position, (name, value)) in keys.into_iter().zip(values).enumerate() {
                    let wrong = config.node_context.as_ref().is_some_and(|node_context| {
                        wrong_node(node_context, &Key::new(namespace.clone(), name.clone()))
                    });

                    if wrong {
                        entries[position] = Some(MultiAckEntry::WrongNode);
                    } else {
                        owned_positions.push(position);
                        owned_keys.push(name);
                        owned_values.push(value);
                    }
                }

                if !owned_keys.is_empty() {
                    // Kept for the forwarding loop below — `owned_keys`/
                    // `owned_values` are moved into the `Command::MultiSet`
                    // call, and `Bytes::clone` is a cheap refcount bump.
                    // Issue #233: skip the clone entirely when there's no
                    // `node_context` to forward through — the loop below
                    // never runs in that case.
                    let forward = config
                        .node_context
                        .is_some()
                        .then(|| (owned_keys.clone(), owned_values.clone()));

                    let response = execute_command(
                        &request_tx,
                        Command::MultiSet {
                            namespace: namespace.clone(),
                            keys: owned_keys,
                            values: owned_values,
                            ttl,
                        },
                    )
                    .await?;

                    let Response::MultiAck(results) = response else {
                        unreachable!("Command::MultiSet always answers with Response::MultiAck");
                    };

                    // Issue #176: `Set` (below) forwards a write on a key
                    // caught mid-handoff (staged join) or mid-drain
                    // (decommission) so it isn't lost once `known_ring`
                    // flips — `MultiSet` never did, so a bulk write during
                    // either window silently vanished. Mirror it here, per
                    // owned key, once the local write is confirmed stored
                    // (`execute` only ever answers `Stored` for an owned
                    // key, but check anyway rather than assume it).
                    if let Some(node_context) = &config.node_context {
                        let (forward_names, forward_values) =
                            forward.expect("node_context.is_some() implies forward is Some");
                        for ((name, value), result) in forward_names
                            .into_iter()
                            .zip(forward_values)
                            .zip(results.iter())
                        {
                            if !matches!(result, MultiAckEntry::Stored) {
                                continue;
                            }
                            let key = Key::new(namespace.clone(), name);

                            // Staged node join — see `migration_target_for`.
                            if let Some(target) = migration_target_for(node_context, &key) {
                                spawn_forward(
                                    &config,
                                    node_context.clone(),
                                    target,
                                    OwnedForwardedWrite::Set {
                                        key: key.clone(),
                                        value: value.clone(),
                                        ttl,
                                    },
                                );
                            }

                            // Decommission drain — see `leave_target_for`.
                            if let Some(target) = leave_target_for(node_context, &key) {
                                spawn_forward(
                                    &config,
                                    node_context.clone(),
                                    target,
                                    OwnedForwardedWrite::HandoffSet { key, value, ttl },
                                );
                            }
                        }
                    }

                    for (position, result) in owned_positions.into_iter().zip(results) {
                        entries[position] = Some(result);
                    }
                }

                let entries = entries
                    .into_iter()
                    .map(|entry| entry.expect("every position filled by one of the two branches"))
                    .collect();
                write_response(
                    &mut stream,
                    &encode_response(&Response::MultiAck(entries), tag),
                )
                .await?;

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
                    // Handed to `run`'s own loop via `forward_tx`
                    // (mirroring the `M` handler above, which uses
                    // `migration_tx`), not awaited inline — see
                    // `forward_with_retries`'s own doc comment for why.
                    spawn_forward(
                        &config,
                        node_context.clone(),
                        target,
                        OwnedForwardedWrite::Set {
                            key: key.clone(),
                            value: value.clone(),
                            ttl,
                        },
                    );
                }

                // Issue #124: mirror for a decommission in flight — the
                // key's post-leave entrant must see this write too. As a
                // `U`, not a plain `S`: until the post-leave roster
                // publishes, the entrant doesn't own the key yet and
                // answers `S` with `W`.
                if let Some(node_context) = &config.node_context
                    && let Some(target) = leave_target_for(node_context, &key)
                {
                    spawn_forward(
                        &config,
                        node_context.clone(),
                        target,
                        OwnedForwardedWrite::HandoffSet {
                            key: key.clone(),
                            value: value.clone(),
                            ttl,
                        },
                    );
                }

                continue;
            }
            Ok((
                Command::HandoffSet {
                    key,
                    value,
                    ttl,
                    if_absent,
                    token,
                },
                tag,
            )) => {
                let Some(node_context) = config.node_context.clone() else {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "received U but this node isn't configured with a discovery server",
                    ));
                };

                // Issue #295: `U` skips the wrong-node check below by
                // design (see this arm's own doc comment), so without
                // this it's the only thing standing between "any
                // shared-secret client" and "write any key here,
                // regardless of ring ownership" — the same gap `M`/`X`
                // close with their own `token` check (see
                // `Command::HandoffSet::token`'s doc comment). Same
                // rejection shape as `M`'s.
                if !constant_time_eq(token.as_bytes(), node_context.token.as_bytes()) {
                    eprintln!(
                        "WARN rejected U from {address}: membership token mismatch \
                         (sender is not a legitimate handoff source for this node)"
                    );
                    write_response(
                        &mut stream,
                        &encode_response(&Response::MigrationRejected, tag),
                    )
                    .await?;
                    return Err(io::Error::new(
                        io::ErrorKind::PermissionDenied,
                        "U carried the wrong membership token",
                    ));
                }

                // Issue #124: a decommissioning peer handing this node an
                // entry it is about to own — stored without the
                // wrong-node check (ownership becomes true when the
                // post-leave roster publishes, deliberately after this).
                // Issue #266: also how a survivor re-replicates a key to
                // the owner an eviction promoted, in which case
                // `if_absent` is set and a key already present here wins.
                let response = execute_command(
                    &request_tx,
                    Command::HandoffSet {
                        key: key.clone(),
                        value: value.clone(),
                        ttl,
                        if_absent,
                        token,
                    },
                )
                .await?;
                write_response(&mut stream, &encode_response(&response, tag)).await?;

                // If this node is itself mid-join-handoff for the key,
                // propagate like any other write — but only for an
                // ordinary (unconditional) handoff. A put-if-absent one
                // that lost the race (the key was already present here)
                // changed nothing to propagate; forwarding `value`
                // regardless would ship a possibly-stale value onward as
                // an unconditional overwrite. Re-replication already
                // sends directly to every owner that needs the entry, so
                // skipping this relay loses nothing.
                if !if_absent
                    && let Some(node_context) = &config.node_context
                    && let Some(target) = migration_target_for(node_context, &key)
                {
                    spawn_forward(
                        &config,
                        node_context.clone(),
                        target,
                        OwnedForwardedWrite::Set { key, value, ttl },
                    );
                }

                continue;
            }
            Ok((Command::HandoffDelete { key, token }, tag)) => {
                let Some(node_context) = config.node_context.clone() else {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "received u but this node isn't configured with a discovery server",
                    ));
                };

                // Issue #295: same authorization gap and fix as `U` —
                // see that arm's own comment.
                if !constant_time_eq(token.as_bytes(), node_context.token.as_bytes()) {
                    eprintln!(
                        "WARN rejected u from {address}: membership token mismatch \
                         (sender is not a legitimate handoff source for this node)"
                    );
                    write_response(
                        &mut stream,
                        &encode_response(&Response::MigrationRejected, tag),
                    )
                    .await?;
                    return Err(io::Error::new(
                        io::ErrorKind::PermissionDenied,
                        "u carried the wrong membership token",
                    ));
                }

                // Issue #124: a decommissioning peer forwarding a client's
                // delete for a key this node is about to own — applied
                // without the wrong-node check, same reasoning as `U`.
                let response = execute_command(
                    &request_tx,
                    Command::HandoffDelete {
                        key: key.clone(),
                        token,
                    },
                )
                .await?;
                write_response(&mut stream, &encode_response(&response, tag)).await?;

                // If this node is itself mid-join-handoff for the key,
                // propagate like any other delete.
                if let Some(node_context) = &config.node_context
                    && let Some(target) = migration_target_for(node_context, &key)
                {
                    spawn_forward(
                        &config,
                        node_context.clone(),
                        target,
                        OwnedForwardedWrite::Delete { key },
                    );
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
                    spawn_forward(
                        &config,
                        node_context.clone(),
                        target,
                        OwnedForwardedWrite::Delete { key: key.clone() },
                    );
                }

                // Issue #124: see the `S` arm's decommission mirror —
                // a `u`, not a plain `D`, for the same wrong-node reason.
                if let Some(node_context) = &config.node_context
                    && let Some(target) = leave_target_for(node_context, &key)
                {
                    spawn_forward(
                        &config,
                        node_context.clone(),
                        target,
                        OwnedForwardedWrite::HandoffDelete { key: key.clone() },
                    );
                }

                continue;
            }
            Ok((Command::Incr { key, delta }, tag)) => {
                if let Some(node_context) = &config.node_context
                    && wrong_node(node_context, &key)
                {
                    write_response(&mut stream, &encode_response(&Response::WrongNode, tag))
                        .await?;
                    continue;
                }

                let response = execute_command(
                    &request_tx,
                    Command::Incr {
                        key: key.clone(),
                        delta,
                    },
                )
                .await?;
                write_response(&mut stream, &encode_response(&response, tag)).await?;

                // Issue #129: only a `Response::Incremented` — i.e. INCR
                // actually wrote a new value — needs forwarding;
                // `NotFound`/`NotNumeric` changed nothing. Forwarded as
                // the resulting *absolute* value via a plain `Set`/
                // `HandoffSet`, never as `Incr` itself: replaying the
                // increment on a joining/entrant node that already has
                // this key from an earlier transfer would double-apply
                // it (see `Command::Incr`'s doc comment). The remaining
                // TTL rides along so the receiving node's copy doesn't
                // come back TTL-less.
                if let Response::Incremented(ref value, ttl) = response {
                    if let Some(node_context) = &config.node_context
                        && let Some(target) = migration_target_for(node_context, &key)
                    {
                        spawn_forward(
                            &config,
                            node_context.clone(),
                            target,
                            OwnedForwardedWrite::Set {
                                key: key.clone(),
                                value: value.clone(),
                                ttl,
                            },
                        );
                    }

                    if let Some(node_context) = &config.node_context
                        && let Some(target) = leave_target_for(node_context, &key)
                    {
                        spawn_forward(
                            &config,
                            node_context.clone(),
                            target,
                            OwnedForwardedWrite::HandoffSet {
                                key: key.clone(),
                                value: value.clone(),
                                ttl,
                            },
                        );
                    }
                }

                continue;
            }
            Ok((
                Command::CasSet {
                    key,
                    condition,
                    value,
                    ttl,
                },
                tag,
            )) => {
                if let Some(node_context) = &config.node_context
                    && wrong_node(node_context, &key)
                {
                    write_response(&mut stream, &encode_response(&Response::WrongNode, tag))
                        .await?;
                    continue;
                }

                let response = execute_command(
                    &request_tx,
                    Command::CasSet {
                        key: key.clone(),
                        condition,
                        value: value.clone(),
                        ttl,
                    },
                )
                .await?;
                write_response(&mut stream, &encode_response(&response, tag)).await?;

                // Issue #141: only `Response::Stored` — i.e. the condition
                // held and CAS actually wrote — needs forwarding; a
                // `NotFound` (mismatch) changed nothing. Forwarded as an
                // ordinary `Set`/`HandoffSet` carrying the literal new
                // value, never as `k` itself: a joining/entrant node
                // re-evaluating the same condition against its own
                // (possibly different) copy could reach a different
                // outcome than the primary just did (see `Cache::cas_set`'s
                // doc comment).
                if matches!(response, Response::Stored) {
                    if let Some(node_context) = &config.node_context
                        && let Some(target) = migration_target_for(node_context, &key)
                    {
                        spawn_forward(
                            &config,
                            node_context.clone(),
                            target,
                            OwnedForwardedWrite::Set {
                                key: key.clone(),
                                value: value.clone(),
                                ttl,
                            },
                        );
                    }

                    if let Some(node_context) = &config.node_context
                        && let Some(target) = leave_target_for(node_context, &key)
                    {
                        spawn_forward(
                            &config,
                            node_context.clone(),
                            target,
                            OwnedForwardedWrite::HandoffSet { key, value, ttl },
                        );
                    }
                }

                continue;
            }
            Ok((
                Command::CasDelete {
                    key,
                    expected_digest,
                },
                tag,
            )) => {
                if let Some(node_context) = &config.node_context
                    && wrong_node(node_context, &key)
                {
                    write_response(&mut stream, &encode_response(&Response::WrongNode, tag))
                        .await?;
                    continue;
                }

                let response = execute_command(
                    &request_tx,
                    Command::CasDelete {
                        key: key.clone(),
                        expected_digest,
                    },
                )
                .await?;
                write_response(&mut stream, &encode_response(&response, tag)).await?;

                // Same "forward the literal result, never `x` itself"
                // rule as `CasSet`.
                if matches!(response, Response::Deleted) {
                    if let Some(node_context) = &config.node_context
                        && let Some(target) = migration_target_for(node_context, &key)
                    {
                        spawn_forward(
                            &config,
                            node_context.clone(),
                            target,
                            OwnedForwardedWrite::Delete { key: key.clone() },
                        );
                    }

                    if let Some(node_context) = &config.node_context
                        && let Some(target) = leave_target_for(node_context, &key)
                    {
                        spawn_forward(
                            &config,
                            node_context.clone(),
                            target,
                            OwnedForwardedWrite::HandoffDelete { key: key.clone() },
                        );
                    }
                }

                continue;
            }
            Ok((Command::Clear { namespace }, tag)) => {
                let scope = ClearScope::Namespace(namespace);
                handle_clear(&mut stream, &request_tx, &config, scope, tag).await?;

                continue;
            }
            Ok((Command::ClearAll, tag)) => {
                handle_clear(&mut stream, &request_tx, &config, ClearScope::All, tag).await?;

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

async fn run_cache(
    mut request_rx: mpsc::Receiver<CacheRequest>,
    max_memory_bytes: usize,
    namespace_budgets: Vec<(Bytes, usize)>,
) {
    let mut cache = Cache::with_budgets(max_memory_bytes, namespace_budgets);

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
    node_context: NodeContext,
    mut rereplication_rx: mpsc::Receiver<RereplicationTask>,
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
            node_context.clone(),
            role,
            shutdown_rx.clone(),
        ));
    }
    // This function's own copy must go, or its later drain of
    // `rereplication_rx` would deadlock waiting for a channel it is
    // itself still holding a sender to (`NodeContext::rereplication_tx`)
    // — every real producer (each `register_with_discovery` task above,
    // and any spawned re-replication task) holds its own clone instead,
    // and `node_context` isn't needed again in this function.
    drop(node_context);

    // Issue #266: drains any re-replication task a ring change
    // triggered — either here, via `register_with_discovery`'s own
    // `adopt_membership` detecting an eviction, or from `run_migration`
    // (a completely different task, spawned off `run`'s own
    // `migration_tx`) via its shared `NodeContext::rereplication_tx` —
    // into the same `JoinSet` the registration connections themselves
    // run in, so shutdown (via `heartbeat_task.await` in `run`) waits
    // for it exactly as it already waits for those. `biased` so a task
    // that arrived just as the last registration connection exited is
    // still picked up before the `else` branch (both disabled) ends the
    // loop; the channel closes once every clone of
    // `NodeContext::rereplication_tx` this process holds has dropped —
    // see `run`'s own `drop(rereplication_tx)`.
    loop {
        tokio::select! {
            biased;

            Some(task) = rereplication_rx.recv() => {
                tasks.spawn(task);
            }

            result = tasks.join_next(), if !tasks.is_empty() => {
                if let Some(Err(error)) = result {
                    eprintln!("WARN heartbeat task failed: {error}");
                }
            }

            else => break,
        }
    }
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
    node_context: NodeContext,
    mut role: DiscoveryRole,
    mut shutdown_rx: watch::Receiver<bool>,
) {
    let join = join_message(&node_context.name, port, &node_context.token);
    let announce = announce_message(&node_context.name, port, &node_context.token);

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
            Ok(stream) => {
                // Buffered for the heartbeat ack's line-oriented roster
                // (issue #61, `read_heartbeat_ack`); writes pass straight
                // through.
                let mut stream = tokio::io::BufReader::new(stream);
                let authenticated = match &auth_secret {
                    Some(secret) => {
                        let auth = auth_message(secret);
                        // Bounded so a discovery server that accepts TCP but
                        // never drains can't wedge this task forever (and,
                        // via `heartbeat_task.await`, hang shutdown).
                        let exchange = timeout(OUTBOUND_IO_TIMEOUT, async {
                            stream.write_all(&auth).await?;
                            let mut ack = [0u8; 3];
                            stream.read_exact(&mut ack).await?;
                            io::Result::Ok(ack)
                        })
                        .await;
                        // Only an explicit `Ed` is a rejected secret. A
                        // dropped connection or garbage instead (issue #68)
                        // is most often a node speaking plaintext to a
                        // TLS discovery server — `--tls-ca` missing — and
                        // saying "rejected the auth secret" for that sent
                        // operators to the wrong knob.
                        match exchange {
                            Ok(Ok(ack)) if &ack == b"Od\n" => true,
                            Ok(Ok(ack)) if &ack == b"Ed\n" => {
                                eprintln!(
                                    "WARN discovery server at {discovery_addr} rejected the auth \
                                     secret"
                                );
                                false
                            }
                            Ok(Ok(ack)) => {
                                eprintln!(
                                    "WARN discovery server at {discovery_addr} answered the auth \
                                     handshake with {:?} instead of Od/Ed — if it requires TLS, \
                                     this node needs --tls-ca",
                                    String::from_utf8_lossy(&ack)
                                );
                                false
                            }
                            Ok(Err(error)) => {
                                eprintln!(
                                    "WARN auth handshake with discovery server at {discovery_addr} \
                                     failed: {error} — if it requires TLS, this node needs --tls-ca"
                                );
                                false
                            }
                            Err(_) => {
                                eprintln!(
                                    "WARN auth handshake with discovery server at {discovery_addr} \
                                     timed out"
                                );
                                false
                            }
                        }
                    }
                    None => true,
                };

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
                            let replication = node_context
                                .known_ring
                                .lock()
                                .unwrap_or_else(|poisoned| poisoned.into_inner())
                                .as_ref()
                                .map(|membership| membership.replication);
                            let heartbeat = heartbeat_message(
                                &node_context.name,
                                replication,
                                &node_context.token,
                            );

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
                            let read_ack = tokio::select! {
                                _ = shutdown_rx.changed() => return,
                                result = timeout(OUTBOUND_IO_TIMEOUT, read_heartbeat_ack(&mut stream)) => result,
                            };

                            let roster = match read_ack {
                                Ok(Ok(roster)) => roster,
                                _ => break,
                            };

                            // Issue #61: only the primary's view is
                            // adopted — replicas never reconcile with each
                            // other (discovery HA), and flip-flopping
                            // between two replicas' views would be worse
                            // than either.
                            if let Some((members, replication)) = roster
                                && matches!(role, DiscoveryRole::Primary(_))
                            {
                                let change = adopt_membership(
                                    &node_context.known_ring,
                                    &node_context.active_migration,
                                    &discovery_addr,
                                    members,
                                    replication,
                                );

                                // Issue #266: a ring change that dropped a
                                // member — an eviction, or a leave this
                                // node did not itself hand off for —
                                // leaves the cluster under-replicated
                                // until someone re-replicates the keys
                                // the change promoted a new owner into.
                                // Handed to `send_heartbeats`'s own
                                // `JoinSet` (this task has none of its
                                // own) rather than run inline: it can
                                // scan every key this node holds, which
                                // must not stall this connection's own
                                // heartbeat ticks.
                                if let Some(change) = change
                                    && change.dropped_a_member()
                                {
                                    let before_ring = change
                                        .before
                                        .expect("dropped_a_member() implies before is Some")
                                        .ring
                                        .clone();
                                    let task: RereplicationTask =
                                        Box::pin(spawn_or_supersede_rereplication(
                                            node_context.clone(),
                                            discovery_addr.clone(),
                                            before_ring,
                                            Arc::clone(&change.after),
                                            shutdown_rx.clone(),
                                        ));
                                    if node_context.rereplication_tx.send(task).await.is_err() {
                                        eprintln!(
                                            "WARN re-replication: could not queue the task for a \
                                             ring change (shutting down); a later ring change \
                                             will retrigger it if the cluster is still \
                                             under-replicated"
                                        );
                                    }
                                }
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

/// Upper bounds on what a heartbeat ack's roster (issue #61) may claim,
/// so a misbehaving discovery server can't make this node allocate
/// without bound: mirror `nanocached-discovery`'s own `MAX_REGISTRY_SIZE`
/// and `MAX_REQUEST_SIZE` (the two binaries share no modules by design).
const MAX_ROSTER_ENTRIES: usize = 1 << 16;
const MAX_ROSTER_FIELD_LEN: usize = 4096;
/// Cap on a single heartbeat-ack *line* — the header `A <count> <repl>\n`
/// and each entry's `<name-len> <addr-len>\n` length prefix (issue #92).
/// Both are only ever a keyword and/or two decimal integers, so 64 bytes
/// is generous (a full `A 65536 65536\n` is 14). The read itself is bounded
/// by this, so a hostile/misconfigured discovery that streams a line
/// without ever sending `\n` errors out instead of growing this node's
/// heartbeat task memory until it OOMs. The name/addr *bytes* aren't read
/// this way — they come via a `read_exact` already sized by the
/// `MAX_ROSTER_FIELD_LEN`-checked lengths below.
const MAX_ROSTER_LINE_LEN: usize = 64;

/// Reads one `\n`-terminated line into `line` (cleared first), but errors
/// with `InvalidData` if it would exceed `limit` bytes before the newline —
/// the bounded counterpart of `read_until`, which grows without cap (issue
/// #92). On EOF before a newline, returns what was read (no trailing `\n`);
/// the caller treats a missing terminator as malformed.
async fn read_line_capped<S: AsyncBufReadExt + Unpin>(
    stream: &mut S,
    limit: usize,
    line: &mut Vec<u8>,
) -> io::Result<()> {
    line.clear();
    loop {
        let (consumed, done, too_long) = {
            let available = stream.fill_buf().await?;
            if available.is_empty() {
                (0, true, false) // EOF before newline
            } else if let Some(pos) = available.iter().position(|&byte| byte == b'\n') {
                let take = pos + 1; // include the newline
                if line.len() + take > limit {
                    (0, true, true)
                } else {
                    line.extend_from_slice(&available[..take]);
                    (take, true, false)
                }
            } else if line.len() + available.len() > limit {
                (0, true, true)
            } else {
                let take = available.len();
                line.extend_from_slice(available);
                (take, false, false)
            }
        };
        stream.consume(consumed);
        if too_long {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "heartbeat ack line exceeded its size cap",
            ));
        }
        if done {
            return Ok(());
        }
    }
}

/// Reads one heartbeat ack. A bare `A\n` means "no membership update"
/// (the discovery server is in its startup grace, or its replication
/// factor is disputed — see the `H` entry in `nanocached-discovery`'s
/// module docs); `A <count> <replication>\n` is followed by `count`
/// `<name-len> <addr-len>\n<name><addr>\n` entries — the current `Joined`
/// roster — returned as `Some((names, replication))`. Anything else is an
/// error, on which the caller redials. Addresses are parsed but dropped:
/// this node only needs names to build a ring.
async fn read_heartbeat_ack<S: AsyncBufReadExt + Unpin>(
    stream: &mut S,
) -> io::Result<Option<(Vec<String>, usize)>> {
    let malformed = || io::Error::new(io::ErrorKind::InvalidData, "malformed heartbeat ack");

    let mut line = Vec::new();
    read_line_capped(stream, MAX_ROSTER_LINE_LEN, &mut line).await?;
    if line == b"A\n" {
        return Ok(None);
    }
    let header = line
        .strip_prefix(b"A ")
        .and_then(|rest| rest.strip_suffix(b"\n"))
        .and_then(|rest| std::str::from_utf8(rest).ok())
        .ok_or_else(malformed)?;
    let mut fields = header.split(' ');
    let count: usize = fields
        .next()
        .and_then(|raw| raw.parse().ok())
        .filter(|count| *count <= MAX_ROSTER_ENTRIES)
        .ok_or_else(malformed)?;
    let replication: usize = fields
        .next()
        .and_then(|raw| raw.parse().ok())
        .filter(|replication| *replication >= 1)
        .ok_or_else(malformed)?;
    if fields.next().is_some() {
        return Err(malformed());
    }

    let mut names = Vec::with_capacity(count.min(1024));
    for _ in 0..count {
        read_line_capped(stream, MAX_ROSTER_LINE_LEN, &mut line).await?;
        let entry = line
            .strip_suffix(b"\n")
            .and_then(|rest| std::str::from_utf8(rest).ok())
            .ok_or_else(malformed)?;
        let mut lengths = entry.split(' ');
        let name_len: usize = lengths
            .next()
            .and_then(|raw| raw.parse().ok())
            .filter(|len| *len <= MAX_ROSTER_FIELD_LEN)
            .ok_or_else(malformed)?;
        let addr_len: usize = lengths
            .next()
            .and_then(|raw| raw.parse().ok())
            .filter(|len| *len <= MAX_ROSTER_FIELD_LEN)
            .ok_or_else(malformed)?;
        if lengths.next().is_some() {
            return Err(malformed());
        }
        // `<name><addr>\n`
        let mut body = vec![0u8; name_len + addr_len + 1];
        stream.read_exact(&mut body).await?;
        if body[name_len + addr_len] != b'\n' {
            return Err(malformed());
        }
        let name = std::str::from_utf8(&body[..name_len]).map_err(|_| malformed())?;
        names.push(name.to_string());
    }

    Ok(Some((names, replication)))
}

/// Issue #61: replaces this node's membership belief with the roster its
/// primary discovery server just reported, if it differs. Until this, a
/// node's `known_ring` changed only when it finished a join handoff (`M`),
/// so after a liveness eviction every survivor kept the dead node in its
/// ring: keys that had re-homed to a survivor were answered `W` there
/// (R=1: every client error, permanently, until the next join), and with
/// R>1 writes quietly ran one copy short.
///
/// Skipped while a handoff this node is running is in flight or still
/// forwarding writes (`migration_target_for`'s window) AND the roster
/// doesn't list that handoff's joiner yet: discovery's roster is `Joined`
/// nodes only, so until every source has reported `C` it lacks a joiner
/// this node's `after_ring` already includes, and reverting to the
/// pre-join ring for that gap would only flap. Once the roster names the
/// joiner the join is over from discovery's view and the roster is
/// authoritative again — the forwarding window outlives the join by
/// `forwarding_grace` (a minute or more), and an eviction landing inside
/// it must not be ignored for that long. An abandoned join (`X`) clears
/// the slot; and if that `X` was lost, the window is bounded regardless.
///
/// Returns the ring change this call actually applied, if any — `None`
/// while a join is still pending or the roster matches what was already
/// believed. Issue #266: the caller (`register_with_discovery`) uses this
/// to detect a ring change that dropped a member — an eviction, or a
/// leave this node did not itself hand off for — and spawn a
/// re-replication task; `before` is `None` on the very first membership
/// this node ever adopts, which by definition drops nothing.
fn adopt_membership(
    known_ring: &KnownRing,
    active_migration: &Arc<Mutex<Option<ActiveMigration>>>,
    discovery_addr: &str,
    mut members: Vec<String>,
    replication: usize,
) -> Option<RingChange> {
    let join_still_pending = {
        let mut slot = active_migration
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let (pending, joiner_evicted) = match slot.as_mut() {
            Some(active) => {
                let joiner_listed = members.contains(&active.joining_name);
                // Issue #62: the roster naming the joiner is discovery's
                // word that the join completed — this handoff's dead
                // copies may now be swept (`run_sweep`).
                if joiner_listed && active.completed_at.is_some() && !active.confirmed {
                    active.confirmed = true;
                    println!(
                        "INFO join of {} confirmed by discovery at {discovery_addr}; {} dead \
                         copies released to the sweep",
                        active.joining_name,
                        active.marked_keys.len()
                    );
                }
                // A joiner discovery already confirmed and later dropped
                // from its roster was *evicted*, not "not yet joined": the
                // forwarding window that outlives the join must not hide
                // that eviction (the doc comment above), so only an
                // unconfirmed join keeps the roster at bay.
                (
                    active.forwarding_open() && !joiner_listed && !active.confirmed,
                    active.confirmed && !joiner_listed,
                )
            }
            None => (false, false),
        };
        // Issue #267: the forwarding window is for a joiner that is still
        // there to receive the writes. Once discovery has evicted it,
        // every forward would only dial a dead address — one a new
        // container may already have been given — and burn
        // KEY_TRANSFER_ATTEMPTS x FORWARD_TIMEOUT each, so close the
        // window now instead of letting it lapse. The slot's marks were
        // released to the sweep at confirmation, so taking it is exactly
        // what the grace expiring would have done.
        if joiner_evicted && let Some(taken) = slot.take() {
            println!(
                "INFO joiner {} evicted by discovery at {discovery_addr}; closing its \
                 forwarding window early",
                taken.joining_name
            );
        }
        pending
    };
    if join_still_pending {
        return None;
    }

    members.sort_unstable();
    members.dedup();

    let mut guard = known_ring
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    let unchanged = guard.as_ref().is_some_and(|current| {
        current.replication == replication && {
            let mut known: Vec<&String> = current.ring.nodes().iter().collect();
            known.sort_unstable();
            known.len() == members.len() && known.iter().zip(&members).all(|(a, b)| *a == b)
        }
    });
    if unchanged {
        return None;
    }

    println!(
        "INFO membership updated from discovery at {discovery_addr}: {} member(s), \
         replication factor {replication}",
        members.len()
    );
    let before = guard.as_ref().cloned();
    let after = Arc::new(Membership {
        ring: Arc::new(HashRing::new(members)),
        replication,
    });
    *guard = Some(Arc::clone(&after));
    Some(RingChange { before, after })
}

/// What `adopt_membership` actually applied — see its own doc comment.
struct RingChange {
    before: Option<Arc<Membership>>,
    after: Arc<Membership>,
}

impl RingChange {
    /// Issue #266: true if `after` is missing at least one member
    /// `before` had — an eviction, or a leave this node did not itself
    /// hand off for (an ordinary decommission already transferred every
    /// affected key before it left, so re-triggering for it too is
    /// harmless — the put-if-absent handoff (`U … A`) makes a redundant
    /// send a no-op — just not free; simplicity over precision). `false`
    /// when `before` is `None` (this node's very first membership) or
    /// when the change only ever added members.
    fn dropped_a_member(&self) -> bool {
        self.before
            .as_ref()
            .is_some_and(|before| ring_dropped_a_member(&before.ring, &self.after.ring))
    }
}

/// Issue #266: true if `before`'s member set contains a name `after`
/// doesn't — the "was a member evicted (or otherwise dropped) between
/// these two rings" test both re-replication trigger sites use:
/// `RingChange::dropped_a_member` (an eviction-driven ring change,
/// `before` = the primary's previous heartbeat-ack roster) and
/// `run_migration`'s own join-flip trigger (`before` = this node's last
/// `known_ring` belief, which the flip below is about to replace —
/// see that function's own doc comment for why that belief can still
/// list a member the `M` itself never saw).
fn ring_dropped_a_member(before: &HashRing, after: &HashRing) -> bool {
    before
        .nodes()
        .iter()
        .any(|node| !after.nodes().contains(node))
}

/// Waits for `duration`, or returns `true` early if shutdown is signaled.
async fn wait_or_shutdown(duration: Duration, shutdown_rx: &mut watch::Receiver<bool>) -> bool {
    tokio::select! {
        _ = sleep(duration) => false,
        _ = shutdown_rx.changed() => true,
    }
}

/// The `S`/`s` frame that transfers or forwards `key` to a joining node.
/// A key in the default namespace goes out as the legacy `S` frame —
/// byte-identical to what pre-namespace nodes sent — so a rolling
/// upgrade's mixed-version handoff keeps working for legacy traffic;
/// only a namespaced key needs the `s` form (issue #105), which every
/// node must understand before namespaces are put to use.
fn set_message(key: &Key, value: &[u8], ttl: Option<Duration>) -> Vec<u8> {
    let mut header = if key.is_namespaced() {
        format!(
            "s {} {} {}",
            key.namespace.len(),
            key.name.len(),
            value.len()
        )
    } else {
        format!("S {} {}", key.name.len(), value.len())
    };

    if let Some(ttl) = ttl {
        header.push_str(&format!(" {}", ttl.as_secs()));
    }

    header.push('\n');

    let mut message = header.into_bytes();
    message.extend_from_slice(&key.namespace);
    message.extend_from_slice(&key.name);
    message.extend_from_slice(value);
    message
}

/// Staged node join: propagates a client's `D` for a key an in-progress handoff
/// is moving to the joining node too (see `forward_delete_to_joining_node`).
/// Same legacy-vs-namespaced frame choice as `set_message`.
fn delete_message(key: &Key) -> Vec<u8> {
    let mut message = if key.is_namespaced() {
        format!("d {} {}\n", key.namespace.len(), key.name.len()).into_bytes()
    } else {
        format!("D {}\n", key.name.len()).into_bytes()
    };
    message.extend_from_slice(&key.namespace);
    message.extend_from_slice(&key.name);
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
    /// Issue #295: the joining node's own membership token, carried by
    /// the `M` that started this handoff (`Command::Migrate::
    /// joining_token`) — what this node presents on any `U`/`u` it needs
    /// to send the joiner (a concurrent client write forwarded via
    /// `migration_target_for`, or issue #266's own re-replication loop in
    /// `run_migration`), so the joiner can tell it apart from an ordinary
    /// shared-secret client forging a handoff frame (see
    /// `Command::HandoffSet::token`).
    joining_token: String,
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
    /// Issue #62: the dead copies this handoff marked (`mark_migrated`),
    /// kept here after completion so they can be rolled back if the join
    /// is abandoned after this node's own share was done — an `X` on a
    /// completed slot, or a later `M` whose roster shows the joiner never
    /// made it. Until `confirmed`, `run_sweep` leaves marked entries
    /// alone (`Command::Sweep { marked: false }`).
    marked_keys: Vec<Key>,
    /// Issue #62: discovery has completed this join — the joiner showed
    /// up in the primary's heartbeat-ack roster (`adopt_membership`) or
    /// in the `joined` roster of a subsequent `M`. Only then are this
    /// handoff's `marked_keys` safe to sweep, and only then does the slot
    /// expire on its own once `forwarding_grace` has passed; an
    /// unconfirmed completed slot stops forwarding after the grace but
    /// stays put, holding its marks back from the sweep, until something
    /// decides the join one way or the other.
    confirmed: bool,
    /// Issue #93: the `known_ring` value this handoff replaced when it
    /// flipped to the post-join topology on completion (`run_migration`).
    /// If the join is later abandoned (`X`) after this node's own share
    /// completed but before discovery confirmed it cluster-wide, the
    /// post-join ring never became real — the abandon path restores this
    /// snapshot so `wrong_node` stops answering `W` for the (now live
    /// again) restored keys immediately, instead of waiting for the next
    /// heartbeat's `adopt_membership` to correct `known_ring`. `None`
    /// until `completed()` stamps it.
    pre_completion_ring: Option<Arc<Membership>>,
    /// Issue #106: clears (`c`/`F`) that arrived while this node's own
    /// transfer was still running, waiting for `run_migration` to replay
    /// them on its transfer stream. A clear can't be forwarded the way a
    /// concurrent `S`/`D` is (on the shared forwarding connection) while
    /// keys are still being sent on the transfer connection: the two
    /// streams have no ordering between them, so a key peeked before the
    /// clear could land on the joiner *after* the forwarded clear and
    /// resurrect there. Queued here instead and drained by the transfer
    /// loop onto its own stream, where the order is real. Only filled
    /// while `completed_at` is `None` (both under this slot's lock), so
    /// the final drain after `completed()` is stamped sees everything;
    /// from then on clears forward like any other write.
    pending_clears: Vec<ClearScope>,
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

impl ActiveMigration {
    /// Still moving keys, or completed but within `forwarding_grace` —
    /// the window during which concurrent client writes are forwarded
    /// to the joiner (`migration_target_for`).
    fn forwarding_open(&self) -> bool {
        self.completed_at
            .is_none_or(|completed_at| completed_at.elapsed() < self.forwarding_grace)
    }

    /// Nothing left for this slot to do: the join is confirmed (its
    /// marks can be swept without it) and the forwarding window has
    /// closed. Cleared lazily by whoever notices.
    fn expired(&self) -> bool {
        self.confirmed && !self.forwarding_open()
    }
}

/// Issue #266: a `run_rereplication` in flight — the re-replication
/// counterpart of `ActiveMigration`, held in `NodeContext::active_rereplication`.
/// Wrapped in an `Arc` (rather than living directly in the slot, as
/// `ActiveMigration` does) because `spawn_or_supersede_rereplication`
/// needs to hand a superseded run's own handle to the code asking it to
/// stop *after* it has already replaced the slot's contents with the new
/// run — there is no single lock scope in which both "read the old
/// value" and "write the new one" can happen while also holding a
/// reference to the old value to poll afterward.
struct ActiveRereplication {
    /// Set by a later ring change's task to ask this run to stop early —
    /// checked in `run_rereplication`'s per-key loop, same idiom as
    /// `ActiveMigration::abort_requested`.
    abort_requested: AtomicBool,
    /// Set once `run_rereplication` returns (whether it finished,
    /// aborted, or gave up on the roster fetch) — `Instant`-free, unlike
    /// `ActiveMigration::completed_at`: nothing here needs to measure a
    /// forwarding grace, only to know the run is over.
    done: AtomicBool,
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
    New {
        guard: MigrationGuard,
        /// Issue #62: dead copies left marked by a previous, completed
        /// handoff that this `M`'s roster reveals was abandoned (its
        /// joiner isn't `Joined`); the caller must `unmark_migrated`
        /// each before listing keys, so they're live again — and re-sent
        /// if the new joiner owns them.
        restore: Vec<Key>,
    },
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
            MigrationOutcome::New { guard, .. } => guard,
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
    ///
    /// Issue #218: an abandoned previous handoff (`!previous_confirmed`
    /// below) also reverts `known_ring` to its `pre_completion_ring`, the
    /// same way the explicit `X` path (`abandon_migration`) does — guarded
    /// by the same `Arc::ptr_eq` check so a newer membership update isn't
    /// clobbered. Without it, `wrong_node` would keep routing to the
    /// phantom joiner until the next heartbeat's `adopt_membership`.
    // Returns `MigrationOutcome`, not `Self`, on purpose: reserving the
    // slot can produce an idempotent re-ack or a rejection instead of a
    // guard — see `MigrationOutcome`.
    #[allow(clippy::new_ret_no_self)]
    #[allow(clippy::too_many_arguments)]
    fn new(
        slot: Arc<Mutex<Option<ActiveMigration>>>,
        joining_name: String,
        joining_addr: String,
        joining_token: String,
        after_ring: Arc<HashRing>,
        replication: usize,
        joined: &[(String, String)],
        known_ring: &KnownRing,
    ) -> MigrationOutcome {
        let mut guard = slot.lock().unwrap_or_else(|poisoned| poisoned.into_inner());

        if guard.as_ref().is_some_and(ActiveMigration::expired) {
            *guard = None;
        }

        let mut restore = Vec::new();
        let mut abandoned_known_ring: Option<(Arc<HashRing>, Option<Arc<Membership>>)> = None;
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

            if existing.completed_at.is_none() {
                let conflicting_joining_name = existing.joining_name.clone();
                // Dropped before logging (unlike the lock this replaced,
                // which held it across the `eprintln!`) since `slot` is
                // also locked by `migration_target_for` on every GET/SET
                // — a backpressured stderr shouldn't stall the hot path.
                drop(guard);
                eprintln!(
                    "WARN ignoring M for {joining_name}: a migration to \
                     {conflicting_joining_name} is already active"
                );
                return MigrationOutcome::Conflict;
            }

            // Issue #62: a completed handoff (forwarding or not) doesn't
            // block the next join. Discovery serializes joins, so a new
            // `M` for a different joiner means the previous join has been
            // decided — and `joined` says which way: the previous joiner
            // listed there completed (its dead copies here are really
            // dead), otherwise it was abandoned and those copies are
            // live again. Rejecting here instead is what made discovery
            // abandon back-to-back joins for a whole forwarding window,
            // and sweeping those marks regardless is what lost the keys.
            let previous_confirmed = existing.confirmed
                || joined
                    .iter()
                    .any(|(name, _)| *name == existing.joining_name);
            if !previous_confirmed {
                restore = existing.marked_keys.clone();
                abandoned_known_ring = Some((
                    Arc::clone(&existing.after_ring),
                    existing.pre_completion_ring.clone(),
                ));
                eprintln!(
                    "WARN previous handoff to {} was abandoned (not in the roster M for \
                     {joining_name} carries); restoring {} dead copies",
                    existing.joining_name,
                    restore.len()
                );
            }
        }

        let abort_requested = Arc::new(AtomicBool::new(false));

        *guard = Some(ActiveMigration {
            joining_name,
            joining_addr,
            joining_token,
            after_ring,
            replication,
            completed_at: None,
            forwarding_grace: Duration::ZERO,
            acked_entries: None,
            abort_requested: Arc::clone(&abort_requested),
            marked_keys: Vec::new(),
            confirmed: false,
            pre_completion_ring: None,
            pending_clears: Vec::new(),
            forward_connection: Arc::new(AsyncMutex::new(None)),
        });
        drop(guard);

        // Locked only after the slot lock above is dropped — never both at
        // once (matches `abandon_migration`'s ordering).
        if let Some((after_ring, pre_completion_ring)) = abandoned_known_ring {
            let mut known_ring_guard = known_ring
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            if known_ring_guard
                .as_ref()
                .is_some_and(|current| Arc::ptr_eq(&current.ring, &after_ring))
            {
                *known_ring_guard = pre_completion_ring;
            }
        }

        MigrationOutcome::New {
            guard: Self {
                slot,
                abort_requested,
                completed: false,
            },
            restore,
        }
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
    fn completed(
        mut self,
        entries_sent: usize,
        marked_keys: Vec<Key>,
        pre_completion_ring: Option<Arc<Membership>>,
    ) {
        if let Some(active) = self
            .slot
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .as_mut()
        {
            active.completed_at = Some(Instant::now());
            active.forwarding_grace = forwarding_grace(entries_sent);
            active.marked_keys = marked_keys;
            active.pre_completion_ring = pre_completion_ring;
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
    // Issue #328: `joined` is a snapshot of a live discovery roster, with
    // no guarantee it's free of duplicate names — `adopt_membership`
    // (`known_ring`'s own update path) applies this identical
    // `sort_unstable` + `dedup` to its membership list before building a
    // `HashRing` from it; this path built one straight from `joined`
    // without it. A duplicate here silently breaks `HashRing::is_owner`
    // and `owners` agreeing with each other (see their doc comments),
    // which is exactly what this function's before/after rings decide
    // handoff roles from. `HashRing::new` now also dedupes defensively,
    // but doing it here too keeps `before_members`/`after_members`
    // themselves accurate for anything that inspects them directly.
    before_members.sort_unstable();
    before_members.dedup();

    let mut after_members = before_members.clone();
    after_members.push(joining_name.to_string());
    after_members.sort_unstable();
    after_members.dedup();

    (HashRing::new(before_members), HashRing::new(after_members))
}

/// Size-derived migration timeout: counts how many of `keys` this node will actually
/// send to the joining node, mirroring the predicate `run_migration`
/// computes for real (issue #266: every old owner that holds an
/// affected key sends it — not just a single designated "primary" — so
/// this is "this node was one of the key's old owners", not "this node
/// was the highest-ranked one"). Purely to size discovery's migration
/// timeout — not a transfer plan.
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
    keys: &[Key],
    before_ring: &HashRing,
    after_ring: &HashRing,
    self_name: &str,
    joining_name: &str,
    replication: usize,
) -> usize {
    keys.iter()
        .filter(|key| {
            after_ring.is_owner(key, joining_name, replication)
                && before_ring.is_owner(key, self_name, replication)
        })
        .count()
}

/// Staged node join (generalized by client-side replication): triggered by an incoming `M`.
/// Computes, using the same rendezvous-hash algorithm clients use
/// (`HashRing` here is `src/hash_ring.rs`'s copy, see TLS support), how each
/// of this node's own entries' top-R owner set changes when the joining
/// node is added. Adding exactly one node can only insert it into a key's
/// ranking, never reorder the existing nodes relative to each other, so
/// per affected key exactly one role can apply to the node displaced from
/// rank R to R+1 (if any — there is at most one): its now-dead copy is
/// marked for the post-handoff sweep. This node may hold that role, or
/// not, independently of whether it sends.
///
/// Issue #266: **every** pre-join owner of an affected key that actually
/// holds it sends its copy to the joiner — not just a single designated
/// "old primary". That used to be safe to optimize away (client writes
/// fan out to every owner, so all owners were assumed equally
/// up to date) until re-replication-after-eviction made it possible for
/// an old owner to be a *ring-computed* owner without actually holding
/// live data yet (a promotion whose re-replication hasn't caught up).
/// Electing only the primary to send then risked a silent no-op — the
/// primary's `peek_entry` comes back empty, nothing is transferred, and
/// the *other* old owner (who does hold the real copy) gets marked
/// displaced and swept anyway, destroying the only copy. Every owner
/// sending instead means the joiner gets a copy as long as *any* old
/// owner still has one. Sent as a put-if-absent `U … A`
/// (`ForwardedWrite::HandoffSet`'s `if_absent`), not a plain `SET`, since
/// more than one node may now send the same key: idempotent, so a
/// redundant send from a second holder — or a concurrent client write
/// racing the same joiner on the shared forwarding connection
/// (`migration_target_for`, still an unconditional `SET` there since a
/// live write is always the freshest) — never clobbers whichever arrives
/// first; every one is still acked `S\n`. A displaced holder marks its
/// copy dead only *after* its own send here actually succeeds — not
/// merely because ring math says it's displaced — so a holder that
/// failed to send (or never had the data to begin with) never has
/// something reclaimed out from under a copy the joiner never received.
/// There's no need to compare more than a pre-join ring.
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
    // Issue #295: the joiner's own membership token (`Command::Migrate::
    // joining_token`) — presented on every `U`/`u` this node sends the
    // joiner below, so it can tell this handoff apart from a forged
    // shared-secret frame (see `Command::HandoffSet::token`).
    joining_token: String,
    replication: usize,
    before_ring: HashRing,
    after_ring: Arc<HashRing>,
    migration_guard: MigrationGuard,
    keys: Option<Vec<Key>>,
) {
    println!("INFO migration started: handoff to {joining_name} at {joining_addr}");

    let keys = match keys {
        Some(keys) => keys,
        None => {
            eprintln!("WARN migration to {joining_name} aborted: cache task is unavailable");
            return;
        }
    };

    // Issue #266: a re-replication from an earlier ring change may still
    // be delivering a copy this migration is about to mark dead (see
    // `wait_for_rereplication_to_clear`'s own doc comment) — wait for it
    // to clear before this handoff starts moving/marking anything.
    wait_for_rereplication_to_clear(&node_context).await;

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

        // Issue #266: every old owner that holds this key sends it — see
        // this function's own doc comment for why electing just one
        // ("the old primary") is no longer safe. A key this node was
        // never an old owner of is neither this node's to send nor to
        // mark: skip it outright.
        if !before_ring.is_owner(&key, self_name, replication) {
            continue;
        }
        // The (at most one) node the joiner displaced from rank R: its
        // copy is dead once the join completes — marked for the sweep
        // below, but only once this node's own send actually succeeds.
        let displaced = !after_ring.is_owner(&key, self_name, replication);

        // Re-checked live rather than trusting `entries()`'s snapshot: a
        // concurrent client write racing this key's turn (see
        // `handle_connection`'s own forwarding of `S`/`D` for a key this
        // migration is moving) must win over whatever was true when the
        // snapshot was taken, or its update would ship stale to the
        // joining node. If the key is gone by now (deleted, expired, or
        // already forwarded-and-since-removed), there's nothing to send —
        // `handle_connection`'s own delete-forwarding path (or nothing
        // ever existing to send in the first place) already covers it.
        // Issue #106: a clear that arrived since the last key went out
        // is replayed on this same stream *before* the next key, so the
        // joiner applies them in the order this node did. (A clear that
        // races this key's peek is caught by the next iteration's drain,
        // which then lands after the stale key and wipes it.)
        if !drain_pending_clears(&node_context, &joining_addr, &mut stream).await {
            for key in marked_this_run {
                unmark_migrated(&node_context.request_tx, &key).await;
            }

            return;
        }

        let Some((_, value, ttl)) = peek_entry(&node_context.request_tx, &key).await else {
            continue;
        };

        let sent = transfer_with_retries(
            &node_context,
            &joining_addr,
            &mut stream,
            ForwardedWrite::HandoffSet {
                key: &key,
                value: &value,
                ttl,
                if_absent: true,
                token: &joining_token,
            },
        )
        .await;

        if !sent {
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
    // for every key — see `NodeContext::known_ring`. The value it replaces
    // is snapshotted into the slot (issue #93) so the abandon path can put
    // it back if this join never becomes real cluster-wide.
    let pre_completion_ring = {
        let mut guard = node_context
            .known_ring
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        guard.replace(Arc::new(Membership {
            // Cloned, not moved: this function's own join-flip
            // re-replication trigger (issue #266, below) needs
            // `after_ring` again once the migration is done.
            ring: Arc::clone(&after_ring),
            replication,
        }))
    };

    println!(
        "INFO migration completed: {joining_name} (sent {sent_count} keys, marked {} dead \
         copies, kept until discovery confirms the join; forwarding writes for {}s)",
        marked_this_run.len(),
        forwarding_grace(sent_count).as_secs()
    );

    // Keep the write-forwarding window open past this node's own share
    // (issue #3) — see `MigrationGuard::completed`. Stamped BEFORE `C`
    // goes out (issue #62): discovery may start the next join the moment
    // this `C` lands and have its `M` here before this task gets past the
    // ack read — a slot still reading as in-flight would reject it.
    // Issue #106: whatever clears queued up after the last key's drain
    // go out before the slot flips to "completed" ...
    if !drain_pending_clears(&node_context, &joining_addr, &mut stream).await {
        for key in marked_this_run {
            unmark_migrated(&node_context.request_tx, &key).await;
        }

        return;
    }

    // Cloned, not moved: this function's own join-flip re-replication
    // trigger (issue #266, below) needs `pre_completion_ring` again too.
    migration_guard.completed(sent_count, marked_this_run, pre_completion_ring.clone());

    // ... and one more drain after the stamp catches a clear that slipped
    // in between: `route_clear` queues only while `completed_at` is
    // `None`, under the same lock `completed()` stamps it, so anything
    // not in this drain was forwarded on the shared connection instead.
    // Too late to abandon the join here (it is already completed on this
    // node); a permanent failure is logged like a failed forward.
    if !drain_pending_clears(&node_context, &joining_addr, &mut stream).await {
        eprintln!(
            "WARN migration to {joining_addr} completed but a clear that raced its completion \
             could not be replayed on the joining node"
        );
    }

    if let Err(error) = report_complete(&node_context, &joining_name).await {
        eprintln!(
            "WARN migration to {joining_addr} finished but reporting completion to {} failed: {error}",
            node_context.discovery_addr
        );
    }

    // Issue #266: the flip above may have silently absorbed an eviction
    // this node's own `known_ring` hadn't caught up to yet — a
    // restart+rejoin can outrace this node's next heartbeat ack, so
    // `before_ring` (computed from the `M` itself, which only ever
    // reflects discovery's *current*, already-post-eviction roster)
    // never saw it either. Left alone, that eviction's promotion would
    // never re-replicate: `known_ring` jumps straight from this node's
    // stale pre-eviction belief to the post-join ring, so the next
    // heartbeat ack matches it exactly and `adopt_membership` reports no
    // change. Compare this node's own *last* belief (`pre_completion_ring`,
    // which may still list the dead member) against the post-join ring
    // instead, and trigger the same re-replication an eviction would
    // have if it dropped one. Deliberately after `report_complete`
    // above: this must never delay the `C` report, and by this point the
    // migration is unambiguously done.
    //
    // Issue #295: `run_rereplication` needs a membership token per target
    // it may send `U`/`u` to (see `Command::HandoffSet::token`), and the
    // `M` that started this handoff only ever carries the *joiner's* own
    // token (`joining_token`) — a target that turns out to be one of the
    // OTHER `joined` members is a real, expected outcome here (this
    // trigger fires because THIS node's own belief was stale, so the
    // promoted owner for a given key is just as likely to be an existing
    // member as the joiner). So, unlike the old "reuse what `M` already
    // carried" shortcut, this now pays for the same `fetch_roster_once`
    // (issue #295: self-authenticated, tokens included) fetch
    // `spawn_or_supersede_rereplication` does — a failure here just skips
    // this trigger (logged), same as that function's own failure
    // handling: the next ring-changing event retries.
    if let Some(previous_membership) = pre_completion_ring {
        let previous_ring = Arc::clone(&previous_membership.ring);
        if ring_dropped_a_member(&previous_ring, &after_ring) {
            let (addresses, tokens): (HashMap<String, String>, HashMap<String, String>) =
                match timeout(
                    OUTBOUND_IO_TIMEOUT,
                    fetch_roster_once(&node_context, &node_context.discovery_addr),
                )
                .await
                {
                    Ok(Ok((members, _replication))) => members
                        .into_iter()
                        .map(|(name, addr, token)| ((name.clone(), addr), (name, token)))
                        .unzip(),
                    Ok(Err(error)) => {
                        eprintln!(
                            "WARN re-replication: fetching the roster with addresses for the \
                             join-flip trigger failed: {error} — giving up; a later ring \
                             change will retrigger it if the cluster is still \
                             under-replicated"
                        );
                        return;
                    }
                    Err(_) => {
                        eprintln!(
                            "WARN re-replication: fetching the roster with addresses for the \
                             join-flip trigger timed out — giving up; a later ring change will \
                             retrigger it if the cluster is still under-replicated"
                        );
                        return;
                    }
                };
            let task: RereplicationTask = Box::pin(run_superseding_rereplication(
                node_context.clone(),
                previous_ring,
                Arc::clone(&after_ring),
                replication,
                addresses,
                tokens,
                node_context.shutdown_rx.clone(),
            ));
            if node_context.rereplication_tx.send(task).await.is_err() {
                eprintln!(
                    "WARN re-replication: could not queue the join-flip task (shutting down); \
                     a later ring change will retrigger it if the cluster is still \
                     under-replicated"
                );
            }
        }
    }
}

/// `run_migration`'s per-item retry loop, shared by key transfers and
/// replayed clears (issue #106): up to `KEY_TRANSFER_ATTEMPTS` attempts,
/// reconnecting `stream` after a failure since its state is then
/// unknown (e.g. a partial write) — the next attempt must not risk a
/// desynced stream. Returns whether the item was delivered; on `false`
/// the caller abandons the join for discovery's migration-timeout to
/// reap, after rolling back its marks.
async fn transfer_with_retries(
    node_context: &NodeContext,
    joining_addr: &str,
    stream: &mut Option<ClientStream>,
    write: ForwardedWrite<'_>,
) -> bool {
    let what = match &write {
        ForwardedWrite::Set { .. }
        | ForwardedWrite::Delete { .. }
        | ForwardedWrite::HandoffSet { .. }
        | ForwardedWrite::HandoffDelete { .. } => "a key",
        ForwardedWrite::Clear(_) => "a clear",
    };

    for attempt in 1..=KEY_TRANSFER_ATTEMPTS {
        if stream.is_none() {
            match connect_and_authenticate(node_context, joining_addr, AuthPeer::Node).await {
                Ok(connected) => *stream = Some(connected),
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

        let result = match &write {
            // `send_set` carries its own `OUTBOUND_IO_TIMEOUT`.
            ForwardedWrite::Set { key, value, ttl } => {
                send_set(active_stream, key, value, *ttl).await
            }
            ForwardedWrite::Delete { key } => timeout(
                OUTBOUND_IO_TIMEOUT,
                ForwardedWrite::Delete { key }.send(active_stream),
            )
            .await
            .unwrap_or_else(|_| {
                Err(io::Error::new(
                    io::ErrorKind::TimedOut,
                    "outbound delete timed out",
                ))
            }),
            ForwardedWrite::Clear(scope) => timeout(
                OUTBOUND_IO_TIMEOUT,
                ForwardedWrite::Clear(scope).send(active_stream),
            )
            .await
            .unwrap_or_else(|_| {
                Err(io::Error::new(
                    io::ErrorKind::TimedOut,
                    "outbound clear timed out",
                ))
            }),
            // Issue #266: `run_migration`'s own bulk transfer carries
            // this too now (`if_absent: true`), alongside the
            // decommission's leave-forwards (`if_absent: false`, which go
            // through `forward_on_shared_connection` instead of this
            // function — this arm exists for exhaustiveness on that
            // side).
            ForwardedWrite::HandoffSet {
                key,
                value,
                ttl,
                if_absent,
                token,
            } => timeout(
                OUTBOUND_IO_TIMEOUT,
                ForwardedWrite::HandoffSet {
                    key,
                    value,
                    ttl: *ttl,
                    if_absent: *if_absent,
                    token,
                }
                .send(active_stream),
            )
            .await
            .unwrap_or_else(|_| {
                Err(io::Error::new(
                    io::ErrorKind::TimedOut,
                    "outbound handoff set timed out",
                ))
            }),
            ForwardedWrite::HandoffDelete { key, token } => timeout(
                OUTBOUND_IO_TIMEOUT,
                ForwardedWrite::HandoffDelete { key, token }.send(active_stream),
            )
            .await
            .unwrap_or_else(|_| {
                Err(io::Error::new(
                    io::ErrorKind::TimedOut,
                    "outbound handoff delete timed out",
                ))
            }),
        };

        match result {
            Ok(()) => return true,
            Err(error) => {
                eprintln!(
                    "WARN migration to {joining_addr} failed to transfer {what} \
                     (attempt {attempt}/{KEY_TRANSFER_ATTEMPTS}): {error}"
                );
                *stream = None;
            }
        }
    }

    eprintln!(
        "WARN migration to {joining_addr} permanently failed to transfer {what} after \
         {KEY_TRANSFER_ATTEMPTS} attempts; abandoning the join for discovery's \
         migration-timeout to reap"
    );

    false
}

/// Issue #106: takes every clear queued on the slot since the last call
/// (`ActiveMigration::pending_clears`) and replays it on the transfer
/// stream, in arrival order. `false` if one could not be delivered.
async fn drain_pending_clears(
    node_context: &NodeContext,
    joining_addr: &str,
    stream: &mut Option<ClientStream>,
) -> bool {
    let pending = {
        let mut slot = node_context
            .active_migration
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        match slot.as_mut() {
            Some(active) => std::mem::take(&mut active.pending_clears),
            None => Vec::new(),
        }
    };

    for scope in &pending {
        if !transfer_with_retries(
            node_context,
            joining_addr,
            stream,
            ForwardedWrite::Clear(scope),
        )
        .await
        {
            return false;
        }
    }

    true
}

/// Issue #124: the whole decommission, run on SIGTERM (see `run`'s
/// signal arm). Self-contained by design: removing one node from an HRW
/// ranking can only promote the previous rank-R+1 node into each key's
/// top-R, so the leaver alone can compute every key's single new owner
/// and hand the entry over — the surviving owners already hold theirs.
/// Sequence:
///
/// 1. Ask any in-flight join handoff to abort (its marks roll back;
///    discovery retries the join after this node is gone) and wait
///    briefly for the slot to clear.
/// 2. Fetch the roster (`L`) — names AND addresses; `known_ring` holds
///    only names — and install `LeaveState`, which (a) starts
///    forwarding concurrent writes to each key's entrant and (b) makes
///    the `M` handler reject new joins.
/// 3. Transfer: for every held key this node owns, re-peek the live
///    value and send it to the key's entrant via `U` (the handoff
///    store: the receiver isn't the owner *yet*, so a plain `S` would
///    be answered `W`).
/// 4. Tell every discovery replica this node is leaving (`V`): the
///    post-leave roster publishes, clients' `W`-refresh routes them to
///    the entrants — which now hold the data.
/// 5. A short forwarding grace (bounded by what's left of the drain
///    budget), then return; the caller runs the ordinary shutdown.
///
/// The whole run is budgeted by `--drain-timeout`: if the budget runs
/// out mid-transfer this logs what was left behind and still sends `V`
/// (the process is exiting either way; clean membership beats a
/// liveness-timeout ghost), degrading to today's crash semantics for
/// the untransferred remainder.
/// Issue #233: how `run_decommission`'s transfer loop disposes of one key
/// — extracted so the deadline-vs-ownership ordering has a unit test that
/// doesn't need a full network harness. A key this node never owned is
/// never this node's handoff to miss, so it must be filtered out *before*
/// the deadline check, not after — otherwise a drain that overruns its
/// budget inflates `left_behind` with keys that were always someone
/// else's responsibility.
#[derive(Debug, PartialEq, Eq)]
enum DecommissionKeyOutcome {
    NotOwned,
    DeadlinePassed,
    Owned,
}

fn classify_decommission_key(
    key: &Key,
    before_ring: &HashRing,
    self_name: &str,
    replication: usize,
    deadline_passed: bool,
) -> DecommissionKeyOutcome {
    if !before_ring.is_owner(key, self_name, replication) {
        return DecommissionKeyOutcome::NotOwned;
    }
    if deadline_passed {
        return DecommissionKeyOutcome::DeadlinePassed;
    }
    DecommissionKeyOutcome::Owned
}

async fn run_decommission(
    node_context: NodeContext,
    discovery_addrs: Vec<String>,
    drain_budget: Duration,
) {
    let deadline = Instant::now() + drain_budget;

    // 1. No new joins (the flag isn't set yet, so flip abort first) and
    // wind down the active one.
    if let Some(active) = node_context
        .active_migration
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .as_ref()
    {
        active.abort_requested.store(true, Ordering::SeqCst);
    }
    for _ in 0..50 {
        let busy = node_context
            .active_migration
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .as_ref()
            .is_some_and(|active| active.completed_at.is_none());
        if !busy || Instant::now() >= deadline {
            break;
        }
        sleep(Duration::from_millis(100)).await;
    }

    // 2. Roster with addresses.
    let roster = match fetch_roster_for_leave(&node_context, &discovery_addrs).await {
        Ok(roster) => roster,
        Err(error) => {
            eprintln!(
                "WARN decommission: fetching the roster failed ({error}); leaving without a \
                 handoff"
            );
            send_leave(&node_context, &discovery_addrs).await;
            return;
        }
    };
    let (members, replication) = roster;
    let self_name = node_context.name.clone();
    if !members.iter().any(|(name, _, _)| *name == self_name) {
        // Not a member (already expired, or never joined): nothing to
        // hand off.
        send_leave(&node_context, &discovery_addrs).await;
        return;
    }
    let survivors: Vec<(String, String, String)> = members
        .iter()
        .filter(|(name, _, _)| *name != self_name)
        .cloned()
        .collect();
    if survivors.is_empty() {
        println!("INFO decommission: last member — nothing to hand off");
        send_leave(&node_context, &discovery_addrs).await;
        return;
    }

    let before_ring = Arc::new(HashRing::new(
        members.iter().map(|(name, _, _)| name.clone()).collect(),
    ));
    let after_ring = Arc::new(HashRing::new(
        survivors.iter().map(|(name, _, _)| name.clone()).collect(),
    ));
    let addresses: HashMap<String, String> = members
        .iter()
        .map(|(name, addr, _)| (name.clone(), addr.clone()))
        .collect();
    // Issue #295: name -> membership token, one per entrant `leave_target_for`
    // may need to authorize a `U`/`u` to — see `Command::HandoffSet::token`.
    let tokens: HashMap<String, String> = members
        .iter()
        .map(|(name, _, token)| (name.clone(), token.clone()))
        .collect();

    *node_context
        .leaving
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner()) = Some(LeaveState {
        before_ring: Arc::clone(&before_ring),
        after_ring: Arc::clone(&after_ring),
        replication,
        addresses: addresses.clone(),
        tokens: tokens.clone(),
        connections: Mutex::new(HashMap::new()),
    });

    // 3. Transfer.
    let Some(keys) = list_keys(&node_context.request_tx).await else {
        eprintln!("WARN decommission: cache task unavailable; leaving without a handoff");
        send_leave(&node_context, &discovery_addrs).await;
        return;
    };
    let mut streams: HashMap<String, ClientStream> = HashMap::new();
    let mut sent = 0usize;
    let mut left_behind = 0usize;

    for key in keys {
        match classify_decommission_key(
            &key,
            &before_ring,
            &self_name,
            replication,
            Instant::now() >= deadline,
        ) {
            DecommissionKeyOutcome::NotOwned => continue,
            DecommissionKeyOutcome::DeadlinePassed => {
                left_behind += 1;
                continue;
            }
            DecommissionKeyOutcome::Owned => {}
        }
        let Some(entrant) = after_ring
            .owners(&key, replication)
            .into_iter()
            .find(|owner| !before_ring.is_owner(&key, owner, replication))
            .map(str::to_string)
        else {
            continue;
        };
        let Some(addr) = addresses.get(&entrant) else {
            continue;
        };
        let Some(entrant_token) = tokens.get(&entrant) else {
            continue;
        };

        // Live re-peek, same reasoning as the join transfer: a client
        // write racing this key's turn must win.
        let Some((_, value, ttl)) = peek_entry(&node_context.request_tx, &key).await else {
            continue;
        };

        let mut delivered = false;
        for _ in 0..KEY_TRANSFER_ATTEMPTS {
            if !streams.contains_key(addr) {
                match connect_and_authenticate(&node_context, addr, AuthPeer::Node).await {
                    Ok(stream) => {
                        streams.insert(addr.clone(), stream);
                    }
                    Err(error) => {
                        eprintln!("WARN decommission: connecting to {addr} failed: {error}");
                        continue;
                    }
                }
            }
            let Some(stream) = streams.get_mut(addr) else {
                continue;
            };
            match send_handoff_set(stream, &key, &value, ttl, false, entrant_token).await {
                Ok(()) => {
                    delivered = true;
                    break;
                }
                Err(error) => {
                    eprintln!("WARN decommission: transfer to {addr} failed: {error}");
                    streams.remove(addr);
                }
            }
        }
        if delivered {
            sent += 1;
        } else {
            left_behind += 1;
        }
    }

    if left_behind > 0 {
        eprintln!(
            "WARN decommission: {left_behind} entr(ies) could not be handed off within the \
             drain budget — the surviving replicas (R={replication}) are their only copies now"
        );
    }
    println!("INFO decommission: handed off {sent} entr(ies)");

    // 4. Leave.
    send_leave(&node_context, &discovery_addrs).await;

    // 5. Grace: keep forwarding concurrent writes while clients refresh
    // onto the post-leave roster.
    let grace = forwarding_grace(sent).min(deadline.saturating_duration_since(Instant::now()));
    println!(
        "INFO decommission: forwarding window open for {}s",
        grace.as_secs()
    );
    sleep(grace).await;
}

/// `V <name-len> <token-len>` to every discovery replica — membership
/// removal is immediate on each replica that hears it (they don't
/// gossip); one refusing/unreachable replica only means that replica
/// serves this node until its liveness timeout.
async fn send_leave(node_context: &NodeContext, discovery_addrs: &[String]) {
    for addr in discovery_addrs {
        let result = timeout(OUTBOUND_IO_TIMEOUT, async {
            let mut stream =
                connect_and_authenticate(node_context, addr, AuthPeer::Discovery).await?;
            let mut frame = format!(
                "V {} {}\n",
                node_context.name.len(),
                node_context.token.len()
            )
            .into_bytes();
            frame.extend_from_slice(node_context.name.as_bytes());
            frame.extend_from_slice(node_context.token.as_bytes());
            stream.write_all(&frame).await?;
            let mut ack = [0u8; 2];
            stream.read_exact(&mut ack).await?;
            if &ack != b"R\n" {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "discovery did not acknowledge the leave",
                ));
            }
            io::Result::Ok(())
        })
        .await
        .unwrap_or_else(|_| Err(io::Error::new(io::ErrorKind::TimedOut, "leave timed out")));

        if let Err(error) = result {
            eprintln!("WARN decommission: leave notification to {addr} failed: {error}");
        }
    }
}

/// The `U` frame (issue #124) — `set_message`'s shape with the handoff
/// letter and the namespace length always present. `if_absent` (issue
/// #266) appends the trailing `A` token that asks the receiver for
/// put-if-absent semantics instead of an unconditional overwrite — set by
/// re-replication after an eviction, never by an ordinary decommission
/// handoff (`send_handoff_set`'s callers pass `false`).
///
/// Issue #295: `token` is the receiving node's own membership token (see
/// `Command::HandoffSet::token`) — carried as `<token-len>` in the header
/// and leads the body (before `namespace`/`key`/`value`), same ordering
/// as `X`'s `<token><joining_name>`, so the receiver can verify it before
/// touching anything else.
fn handoff_message(
    key: &Key,
    value: &[u8],
    ttl: Option<Duration>,
    if_absent: bool,
    token: &str,
) -> Vec<u8> {
    let mut header = format!(
        "U {} {} {} {}",
        key.namespace.len(),
        key.name.len(),
        value.len(),
        token.len()
    );
    if let Some(ttl) = ttl {
        header.push_str(&format!(" {}", ttl.as_secs()));
    }
    if if_absent {
        header.push_str(" A");
    }
    header.push('\n');
    let mut message = header.into_bytes();
    message.extend_from_slice(token.as_bytes());
    message.extend_from_slice(&key.namespace);
    message.extend_from_slice(&key.name);
    message.extend_from_slice(value);
    message
}

/// The `u` frame (issue #124) — `delete_message`'s namespaced shape with
/// the handoff letter, mirroring `handoff_message` for `U`, including its
/// issue #295 `token` field and body ordering.
fn handoff_delete_message(key: &Key, token: &str) -> Vec<u8> {
    let mut message = format!(
        "u {} {} {}\n",
        key.namespace.len(),
        key.name.len(),
        token.len()
    )
    .into_bytes();
    message.extend_from_slice(token.as_bytes());
    message.extend_from_slice(&key.namespace);
    message.extend_from_slice(&key.name);
    message
}

async fn send_handoff_set(
    stream: &mut ClientStream,
    key: &Key,
    value: &[u8],
    ttl: Option<Duration>,
    if_absent: bool,
    token: &str,
) -> io::Result<()> {
    timeout(OUTBOUND_IO_TIMEOUT, async {
        stream
            .write_all(&handoff_message(key, value, ttl, if_absent, token))
            .await?;
        let mut ack = [0u8; 2];
        stream.read_exact(&mut ack).await?;
        if &ack != b"S\n" {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "peer did not acknowledge the handed-off entry",
            ));
        }
        io::Result::Ok(())
    })
    .await
    .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "handoff transfer timed out"))?
}

/// Issue #124: a roster-with-addresses fetch for the decommission —
/// `known_ring` holds names only, and the drain needs addresses to dial
/// entrants. Issue #295: also needs every survivor's own membership
/// token, to authorize the `U`/`u` frames the drain sends them (see
/// `Command::HandoffSet::token`) — `fetch_roster_once` carries both.
/// Bounded parse mirroring the SDKs'/harness's.
async fn fetch_roster_for_leave(
    node_context: &NodeContext,
    discovery_addrs: &[String],
) -> io::Result<(Vec<(String, String, String)>, usize)> {
    let mut last_error = io::Error::other("no discovery replicas configured");

    for addr in discovery_addrs {
        match timeout(OUTBOUND_IO_TIMEOUT, fetch_roster_once(node_context, addr)).await {
            Ok(Ok(roster)) => return Ok(roster),
            Ok(Err(error)) => last_error = error,
            Err(_) => {
                last_error = io::Error::new(
                    io::ErrorKind::TimedOut,
                    format!("T fetch from {addr} timed out"),
                )
            }
        }
    }

    Err(last_error)
}

/// Issue #295: `T <name-len> <token-len>\n<name><token>` — a
/// self-authenticated roster fetch, unlike the public, client-facing `L`
/// (which deliberately never carries tokens — see `NodeInfo::token`'s
/// doc comment in `nanocached-discovery`). This node presents its own
/// name+token (the same credential `H`/`C` already use) to prove to
/// discovery it's a genuinely registered node, not merely a
/// shared-secret-holding client; only then does discovery hand back every
/// registered node's address *and* token. The two callers
/// (`fetch_roster_for_leave`'s decommission drain, and
/// `spawn_or_supersede_rereplication`'s eviction-triggered re-replication)
/// both need those tokens to authorize the `U`/`u` frames they may send —
/// see `Command::HandoffSet::token`.
async fn fetch_roster_once(
    node_context: &NodeContext,
    addr: &str,
) -> io::Result<(Vec<(String, String, String)>, usize)> {
    const MAX_ROSTER_ENTRIES: usize = 4096;
    const MAX_NAME_ADDR_OR_TOKEN_LENGTH: usize = 1024;

    let stream = connect_and_authenticate(node_context, addr, AuthPeer::Discovery).await?;
    // Buffered so the length-bounded line reads below (`read_line_capped`,
    // issue #217 — this used to grow a `Vec<u8>` without a cap while
    // scanning for `\n`, the same class of bug #92 fixed for
    // `read_heartbeat_ack`) and the fixed-size body `read_exact`s share one
    // buffer: any bytes `read_line_capped` already buffered past a line's
    // `\n` are still there for the next read, exactly as `read_heartbeat_ack`
    // relies on.
    let mut stream = tokio::io::BufReader::new(stream);
    let mut request = format!(
        "T {} {}\n",
        node_context.name.len(),
        node_context.token.len()
    )
    .into_bytes();
    request.extend_from_slice(node_context.name.as_bytes());
    request.extend_from_slice(node_context.token.as_bytes());
    stream.write_all(&request).await?;

    let bad = || io::Error::new(io::ErrorKind::InvalidData, "bad T response");

    let mut line = Vec::new();
    read_line_capped(&mut stream, MAX_ROSTER_LINE_LEN, &mut line).await?;
    if line == b"B\n" {
        return Err(io::Error::new(
            io::ErrorKind::WouldBlock,
            "discovery is in its startup grace",
        ));
    }
    let header = line
        .strip_suffix(b"\n")
        .map(|rest| String::from_utf8_lossy(rest).into_owned())
        .ok_or_else(bad)?;
    let mut parts = header.strip_prefix("N ").ok_or_else(bad)?.split(' ');
    let count: usize = parts
        .next()
        .and_then(|part| part.parse().ok())
        .ok_or_else(bad)?;
    let replication: usize = parts
        .next()
        .and_then(|part| part.parse().ok())
        .ok_or_else(bad)?;
    if count > MAX_ROSTER_ENTRIES || replication == 0 {
        return Err(bad());
    }

    let mut members = Vec::with_capacity(count);
    for _ in 0..count {
        read_line_capped(&mut stream, MAX_ROSTER_LINE_LEN, &mut line).await?;
        let entry_header = line
            .strip_suffix(b"\n")
            .map(|rest| String::from_utf8_lossy(rest).into_owned())
            .ok_or_else(bad)?;
        let mut parts = entry_header.split(' ');
        let name_length: usize = parts
            .next()
            .and_then(|part| part.parse().ok())
            .ok_or_else(bad)?;
        let addr_length: usize = parts
            .next()
            .and_then(|part| part.parse().ok())
            .ok_or_else(bad)?;
        let token_length: usize = parts
            .next()
            .and_then(|part| part.parse().ok())
            .ok_or_else(bad)?;
        if name_length > MAX_NAME_ADDR_OR_TOKEN_LENGTH
            || addr_length > MAX_NAME_ADDR_OR_TOKEN_LENGTH
            || token_length > MAX_NAME_ADDR_OR_TOKEN_LENGTH
        {
            return Err(bad());
        }
        // Body + trailing newline.
        let mut body = vec![0u8; name_length + addr_length + token_length + 1];
        stream.read_exact(&mut body).await?;
        members.push((
            String::from_utf8_lossy(&body[..name_length]).into_owned(),
            String::from_utf8_lossy(&body[name_length..name_length + addr_length]).into_owned(),
            String::from_utf8_lossy(
                &body[name_length + addr_length..name_length + addr_length + token_length],
            )
            .into_owned(),
        ));
    }

    Ok((members, replication))
}

/// Issue #266: how long `run_migration` waits for an in-flight
/// re-replication to clear before it starts marking dead copies, and how
/// long a re-replication waits for an in-flight join migration to
/// complete before it starts sending — see both functions' doc comments.
/// Generous relative to a single handoff's own transfer time (this is a
/// last-resort bound, not the expected wait) since giving up early
/// defeats the point of waiting at all; either side proceeding anyway
/// after this elapses just means one more, slightly-late ring-change
/// cycle to reconcile, not lost data.
const RING_CHANGE_HANDOFF_WAIT: Duration = Duration::from_secs(60);

/// Issue #266: bounded wait for `node_context.active_rereplication` to
/// clear, so `run_migration` doesn't mark a copy dead (releasing it to
/// the sweep) that a re-replication in flight has not yet delivered to
/// the ring's newly promoted owner — the two would otherwise race to
/// leave that key on only the joiner, exactly the under-replication this
/// issue exists to close. Not shutdown-aware, unlike `run_rereplication`
/// itself: this runs inside an already-bounded handoff (`run_migration`),
/// not a background task of its own, so it has nothing better to do
/// while shutdown is pending than keep waiting out its own bound.
async fn wait_for_rereplication_to_clear(node_context: &NodeContext) {
    let deadline = Instant::now() + RING_CHANGE_HANDOFF_WAIT;
    loop {
        let busy = node_context
            .active_rereplication
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .is_some();
        if !busy {
            return;
        }
        if Instant::now() >= deadline {
            eprintln!(
                "WARN migration: a re-replication was still in flight after {}s; proceeding \
                 anyway",
                RING_CHANGE_HANDOFF_WAIT.as_secs()
            );
            return;
        }
        sleep(Duration::from_millis(100)).await;
    }
}

/// Issue #266: the mirror wait, on the re-replication side — a join
/// migration in flight is about to change `known_ring` again (and,
/// symmetrically, may itself want to mark this node's copy dead once it
/// completes), so a re-replication computed against the ring as it stood
/// a moment ago waits for the migration to finish rather than racing it
/// with a stale before/after pair.
///
/// "In flight" means still transferring — `completed_at.is_none()`, the
/// same check `run_decommission`'s own abort-wait loop uses — not merely
/// "the slot is occupied": a completed handoff's slot stays `Some` for
/// its whole `forwarding_grace` afterglow (up to ~100s for a large
/// transfer, comfortably longer than this wait's own bound), and that
/// window has nothing left to race — `known_ring` already flipped to
/// `after_ring` the moment the transfer finished. Treating the slot's
/// mere presence as "busy" made this wait time out on essentially every
/// call shortly after any join, for no reason.
async fn wait_for_migration_to_clear(node_context: &NodeContext) {
    let deadline = Instant::now() + RING_CHANGE_HANDOFF_WAIT;
    loop {
        let busy = node_context
            .active_migration
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .as_ref()
            .is_some_and(|active| active.completed_at.is_none());
        if !busy {
            return;
        }
        if Instant::now() >= deadline {
            eprintln!(
                "WARN re-replication: a join migration was still in flight after {}s; \
                 proceeding with the ring as adopted",
                RING_CHANGE_HANDOFF_WAIT.as_secs()
            );
            return;
        }
        sleep(Duration::from_millis(100)).await;
    }
}

/// Issue #266: which addresses (empty if none) this node must
/// re-replicate `key` to after a ring change dropped at least one
/// member — pure so the sender-election + target computation has a fast
/// unit test that needs no network harness, mirroring
/// `classify_decommission_key` for the decommission's own transfer loop.
///
/// Sender election: of `key`'s owners under `before`, only the
/// highest-ranked one that is still a member of `after` sends — the
/// evicted node's own rank (if it held one) simply drops out of
/// consideration, so the next-ranked survivor takes over the "first old
/// owner sends" rule `run_migration` uses for a join. `self_name` must
/// itself be an owner under `before`, or this returns empty without even
/// looking for a sender (nothing to elect if this node wasn't an owner
/// to begin with).
///
/// Targets: `after`'s owners minus `before`'s — the ranks this ring
/// change newly promoted into `key`'s top-R. A survivor that was already
/// an owner stays one (removing one node from an HRW ranking can only
/// ever promote others, never demote a survivor — see the ranking's own
/// properties), so this never needs to ask "did I just lose ownership":
/// unlike a join, a re-replication never marks anything dead.
fn rereplication_targets(
    before: &HashRing,
    after: &HashRing,
    key: &Key,
    replication: usize,
    self_name: &str,
) -> Vec<String> {
    if !before.is_owner(key, self_name, replication) {
        return Vec::new();
    }

    let old_owners = before.owners(key, replication);
    let after_nodes = after.nodes();

    let mut sender = None;
    for owner in old_owners.iter().copied() {
        if after_nodes.iter().any(|node| node.as_str() == owner) {
            sender = Some(owner);
            break;
        }
    }
    if sender != Some(self_name) {
        return Vec::new();
    }

    after
        .owners(key, replication)
        .into_iter()
        .filter(|owner| !old_owners.contains(owner))
        .map(str::to_string)
        .collect()
}

/// Issue #275: installs `state` into `slot` and hands back whatever
/// occupied it a moment ago, both under one lock acquisition — the fix
/// for the check-then-act race `run_superseding_rereplication` used to
/// have, where reading the previous occupant and writing the new one
/// were two separate lock acquisitions with an `.await` in between.
/// Two triggers landing in that window could both read the same
/// `previous`, both wait on its `done`, and then both install
/// themselves — the loser's run would keep going untracked, with
/// nothing left pointing at it to abort.
///
/// Doing the swap atomically instead makes every trigger claim the slot
/// the instant it runs, with no window in which two callers can observe
/// the same occupant: whichever of two racing callers reaches the lock
/// first becomes the other's `previous`, so the two chain — the second
/// waits on the first's `done`, not on some third run's — rather than
/// racing. That preserves the at-most-one-running invariant for any
/// number of concurrent triggers, since each new state can only ever be
/// superseded by the one state that actually swapped it out.
fn take_over_rereplication_slot(
    slot: &Arc<Mutex<Option<Arc<ActiveRereplication>>>>,
    state: &Arc<ActiveRereplication>,
) -> Option<Arc<ActiveRereplication>> {
    let mut slot = slot.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
    slot.replace(Arc::clone(state))
}

/// Issue #266: shared core for both re-replication trigger sites —
/// supersedes (aborts, then waits briefly for) any re-replication
/// already in flight for an earlier ring change (at most one runs at a
/// time per node, so two runs can never interleave their sends),
/// installs a fresh `ActiveRereplication` slot, runs `run_rereplication`
/// against the given ring change and address map, then clears the slot
/// (if a later ring change hasn't already claimed it). Takes everything
/// by value so it can be boxed as a `RereplicationTask` and hand off
/// through `NodeContext::rereplication_tx` — see that field's doc
/// comment on why every producer goes through the same channel into
/// `send_heartbeats`'s `JoinSet` rather than spawning directly.
///
/// Two callers: `spawn_or_supersede_rereplication` (an eviction-driven
/// ring change — fetches the address map itself via `L`, and waits out
/// any join migration in progress first) and `run_migration`'s own
/// join-flip trigger (which already has the addresses from the `M` it
/// just handled, plus the joiner's own, and runs only after its own
/// transfer has completed — so neither of those two steps applies there).
///
/// Issue #275: the install (`take_over_rereplication_slot`) happens
/// *before* the wait on the previous occupant's `done`, not after — see
/// that function's doc comment for why this ordering is what keeps
/// concurrent triggers from racing to install their state. The
/// completion guard below (`Arc::ptr_eq` against `slot`'s current
/// occupant) still does the right thing under this scheme: by the time
/// this run finishes, a later trigger may already have swapped its own
/// state in, in which case `slot` no longer points at `state` and this
/// is correctly a no-op — clearing would otherwise erase a successor's
/// still-active state out from under it.
async fn run_superseding_rereplication(
    node_context: NodeContext,
    before_ring: Arc<HashRing>,
    after_ring: Arc<HashRing>,
    replication: usize,
    addresses: HashMap<String, String>,
    // Issue #295: name -> membership token, one per `addresses` entry
    // this run genuinely has a token for — see `Command::HandoffSet::
    // token` and `run_rereplication`'s own doc comment.
    tokens: HashMap<String, String>,
    mut shutdown_rx: watch::Receiver<bool>,
) {
    let state = Arc::new(ActiveRereplication {
        abort_requested: AtomicBool::new(false),
        done: AtomicBool::new(false),
    });
    let previous = take_over_rereplication_slot(&node_context.active_rereplication, &state);
    if let Some(previous) = previous {
        previous.abort_requested.store(true, Ordering::SeqCst);
        let deadline = Instant::now() + RING_CHANGE_HANDOFF_WAIT;
        while !previous.done.load(Ordering::SeqCst) && Instant::now() < deadline {
            sleep(Duration::from_millis(100)).await;
        }
    }

    run_rereplication(
        &node_context,
        &before_ring,
        &after_ring,
        replication,
        &addresses,
        &tokens,
        &state.abort_requested,
        &mut shutdown_rx,
    )
    .await;

    state.done.store(true, Ordering::SeqCst);
    let mut slot = node_context
        .active_rereplication
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    if slot
        .as_ref()
        .is_some_and(|current| Arc::ptr_eq(current, &state))
    {
        *slot = None;
    }
}

/// Issue #266: the task `register_with_discovery` hands to
/// `send_heartbeats`'s `JoinSet` when `adopt_membership` reports a ring
/// change that dropped a member. Fetches the roster with addresses (the
/// heartbeat ack carries names only) and waits out any join migration in
/// progress (`wait_for_migration_to_clear`) before handing off to
/// `run_superseding_rereplication` for the rest.
async fn spawn_or_supersede_rereplication(
    node_context: NodeContext,
    discovery_addr: String,
    before_ring: Arc<HashRing>,
    after: Arc<Membership>,
    shutdown_rx: watch::Receiver<bool>,
) {
    let (addresses, tokens): (HashMap<String, String>, HashMap<String, String>) = match timeout(
        OUTBOUND_IO_TIMEOUT,
        fetch_roster_once(&node_context, &discovery_addr),
    )
    .await
    {
        Ok(Ok((members, _replication))) => members
            .into_iter()
            .map(|(name, addr, token)| ((name.clone(), addr), (name, token)))
            .unzip(),
        Ok(Err(error)) => {
            eprintln!(
                "WARN re-replication: fetching the roster with addresses from {discovery_addr} \
                 failed: {error} — giving up; a later ring change will retrigger it if the \
                 cluster is still under-replicated"
            );
            return;
        }
        Err(_) => {
            eprintln!(
                "WARN re-replication: fetching the roster with addresses from {discovery_addr} \
                 timed out — giving up; a later ring change will retrigger it if the cluster is \
                 still under-replicated"
            );
            return;
        }
    };

    wait_for_migration_to_clear(&node_context).await;

    let after_ring = Arc::clone(&after.ring);
    run_superseding_rereplication(
        node_context,
        before_ring,
        after_ring,
        after.replication,
        addresses,
        tokens,
        shutdown_rx,
    )
    .await;
}

/// Issue #266: after a ring change dropped a member, streams every key
/// this node is the elected sender for (`rereplication_targets`) to the
/// owner(s) the change newly promoted — mirrors `run_decommission`'s
/// handoff loop, with one shared, reused connection per target address,
/// sent as a put-if-absent `U … A` (`send_handoff_set`'s `if_absent`) so
/// this can never regress a newer client write that raced it. Returns
/// once every key has been considered, `abort_requested` is set (a
/// superseding ring change), or shutdown lands.
///
/// Issue #295: a target this node has no membership token for (missing
/// from `tokens`) is skipped exactly like a target with no address —
/// counted in `skipped`, not sent to. Both callers (`spawn_or_supersede_
/// rereplication`'s eviction trigger and `run_migration`'s own join-flip
/// trigger) fetch a token for every target up front (`fetch_roster_once`,
/// issue #295), so this is a defensive fallback for a target that
/// dropped out of the registry between that fetch and this send, not the
/// expected common case.
#[allow(clippy::too_many_arguments)]
async fn run_rereplication(
    node_context: &NodeContext,
    before_ring: &HashRing,
    after_ring: &HashRing,
    replication: usize,
    addresses: &HashMap<String, String>,
    tokens: &HashMap<String, String>,
    abort_requested: &AtomicBool,
    shutdown_rx: &mut watch::Receiver<bool>,
) {
    let Some(keys) = list_keys(&node_context.request_tx).await else {
        eprintln!("WARN re-replication: cache task is unavailable");
        return;
    };

    let self_name = node_context.name.as_str();
    let mut streams: HashMap<String, ClientStream> = HashMap::new();
    let mut sent = 0usize;
    let mut skipped = 0usize;
    let mut owners_reached: HashSet<String> = HashSet::new();

    for key in keys {
        if abort_requested.load(Ordering::SeqCst) || *shutdown_rx.borrow() {
            break;
        }

        let targets = rereplication_targets(before_ring, after_ring, &key, replication, self_name);
        if targets.is_empty() {
            continue;
        }

        // Live re-peek, same reasoning as the join transfer and the
        // decommission's own: a concurrent client write must win over
        // whatever this snapshot held.
        let Some((_, value, ttl)) = peek_entry(&node_context.request_tx, &key).await else {
            continue;
        };

        for target in targets {
            let Some(addr) = addresses.get(&target) else {
                skipped += 1;
                continue;
            };
            let Some(token) = tokens.get(&target) else {
                skipped += 1;
                continue;
            };

            let mut delivered = false;
            for attempt in 1..=KEY_TRANSFER_ATTEMPTS {
                if abort_requested.load(Ordering::SeqCst) || *shutdown_rx.borrow() {
                    break;
                }
                if !streams.contains_key(addr) {
                    match connect_and_authenticate(node_context, addr, AuthPeer::Node).await {
                        Ok(stream) => {
                            streams.insert(addr.clone(), stream);
                        }
                        Err(error) => {
                            eprintln!(
                                "WARN re-replication: connecting to {addr} failed (attempt \
                                 {attempt}/{KEY_TRANSFER_ATTEMPTS}): {error}"
                            );
                            continue;
                        }
                    }
                }
                let Some(stream) = streams.get_mut(addr) else {
                    continue;
                };
                match send_handoff_set(stream, &key, &value, ttl, true, token).await {
                    Ok(()) => {
                        delivered = true;
                        break;
                    }
                    Err(error) => {
                        eprintln!(
                            "WARN re-replication: transfer to {addr} failed (attempt \
                             {attempt}/{KEY_TRANSFER_ATTEMPTS}): {error}"
                        );
                        streams.remove(addr);
                    }
                }
            }

            if delivered {
                sent += 1;
                owners_reached.insert(target);
            } else {
                skipped += 1;
            }
        }
    }

    println!(
        "INFO re-replication after membership change: sent {sent} entries to {} owner(s) ({} \
         skipped)",
        owners_reached.len(),
        skipped
    );
}

async fn list_keys(request_tx: &mpsc::Sender<CacheRequest>) -> Option<Vec<Key>> {
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
    key: &Key,
) -> Option<(Key, Bytes, Option<Duration>)> {
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

async fn mark_migrated(request_tx: &mpsc::Sender<CacheRequest>, key: &Key) {
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

async fn unmark_migrated(request_tx: &mpsc::Sender<CacheRequest>, key: &Key) {
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

/// Handles an incoming `X` (discovery abandoning a join) against this
/// node's migration slot. Always requests abort on a matching in-flight
/// handoff (whether or not it has completed), so a still-running transfer
/// stops. A safe no-op if there's no active migration, or it's for a
/// different `joining_name` — a cancel that arrived after this handoff
/// already finished and cleared its slot, or for some other join.
///
/// For a handoff that had already completed this node's own share (so it
/// only lingered to forward writes, issue #3, and hold its dead copies'
/// marks, issue #62), the abandon ends both: the slot is taken, and its
/// `known_ring` flip to the post-join topology is reverted (issue #93) —
/// but only if `known_ring` still holds *this* handoff's post-join ring,
/// so a newer membership update (e.g. a heartbeat that adopted a fresh
/// roster once the grace lapsed) is never clobbered. Returns the dead
/// copies to restore (their async `unmark_migrated` is left to the
/// caller); `None` when there was nothing completed to abandon.
fn abandon_migration(node_context: &NodeContext, joining_name: &str) -> Option<Vec<Key>> {
    let taken = {
        let mut slot = node_context
            .active_migration
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        match slot.as_ref() {
            Some(active) if active.joining_name == joining_name => {
                active.abort_requested.store(true, Ordering::SeqCst);
                if active.completed_at.is_some() {
                    slot.take()
                } else {
                    // Still transferring: `run_migration` will notice the
                    // abort and roll back its own marks; it hasn't flipped
                    // `known_ring` yet, so there is nothing to revert.
                    None
                }
            }
            _ => None,
        }
    }?;

    {
        let mut guard = node_context
            .known_ring
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if guard
            .as_ref()
            .is_some_and(|current| Arc::ptr_eq(&current.ring, &taken.after_ring))
        {
            *guard = taken.pre_completion_ring;
        }
    }

    Some(taken.marked_keys)
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
            // (issue #3) must not stall TTL sweeping for a minute. Its
            // *marks*, though (issue #62), are held back until discovery
            // has confirmed the join they belong to — see
            // `ActiveMigration::confirmed`.
            let (migration_active, marks_held) = {
                let slot = active_migration
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner());
                match slot.as_ref() {
                    Some(active) => (active.completed_at.is_none(), !active.confirmed),
                    None => (false, false),
                }
            };

            if migration_active {
                break;
            }

            tokio::select! {
                result = sweep(&request_tx, !marks_held) => match result {
                    Some(removed) if removed >= SWEEP_BUDGET => continue,
                    _ => break,
                },
                _ = shutdown_rx.changed() => return,
            }
        }
    }
}

async fn sweep(request_tx: &mpsc::Sender<CacheRequest>, marked: bool) -> Option<usize> {
    let (response_tx, response_rx) = oneshot::channel();

    request_tx
        .send(CacheRequest {
            command: Command::Sweep { marked },
            response_tx,
        })
        .await
        .ok()?;

    match response_rx.await.ok()? {
        Response::Swept(removed) => Some(removed),
        _ => None,
    }
}

/// Which kind of peer an outbound connection is for. A node acks the `A`
/// handshake with `On`, a discovery server with `Od` (each names itself so
/// a client can tell them apart from the first reply) — so an outbound
/// connection has to know who it is dialling to accept the right ack.
/// Before this distinction the decommission path's roster fetch and leave
/// notification dialled discovery expecting a node's `On`, and with
/// `NANOCACHED_AUTH_SECRET` set every SIGTERM-driven scale-in gave up on
/// the handoff ("joining node rejected the auth secret"); the entries were
/// lost with the task and the leave arrived as a liveness eviction.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum AuthPeer {
    Node,
    Discovery,
}

impl AuthPeer {
    fn auth_ack(self) -> &'static [u8; 3] {
        match self {
            AuthPeer::Node => b"On\n",
            AuthPeer::Discovery => b"Od\n",
        }
    }

    fn describe(self) -> &'static str {
        match self {
            AuthPeer::Node => "joining node",
            AuthPeer::Discovery => "discovery server",
        }
    }
}

/// Connects to `addr` and, if `node_context.auth_secret` is set, performs
/// the auth handshake it expects before accepting any other command —
/// shared by every place that opens an outbound connection to another
/// node (`run_migration`'s own persistent connection, the one-shot
/// `set_on_joining_node`/`delete_on_joining_node` calls used to forward a
/// racing client write mid-migration, the decommission handoff) and to a
/// discovery server (the decommission's roster fetch and leave). `peer`
/// selects the ack to expect.
async fn connect_and_authenticate(
    node_context: &NodeContext,
    addr: &str,
    peer: AuthPeer,
) -> io::Result<ClientStream> {
    let mut stream = connect_client_stream(addr, node_context.tls_connector.as_ref()).await?;

    if let Some(secret) = &node_context.auth_secret {
        timeout(OUTBOUND_IO_TIMEOUT, async {
            stream.write_all(&auth_message(secret)).await?;

            let mut ack = [0u8; 3];
            stream.read_exact(&mut ack).await?;

            if &ack != peer.auth_ack() {
                return Err(io::Error::new(
                    io::ErrorKind::PermissionDenied,
                    format!("{} rejected the auth secret", peer.describe()),
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
    key: &Key,
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
    /// Issue #295: the target's own membership token — carried on any
    /// `U`/`u` this forward sends (see `Command::HandoffSet::token`).
    /// Unused for a plain `Set`/`Delete` forward (`migration_target_for`'s
    /// join-side target also serves those), but always populated:
    /// `migration_target_for` sources it from `ActiveMigration::
    /// joining_token`, `leave_target_for` from `LeaveState::tokens`.
    token: String,
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
    key: &Key,
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
        key: &'a Key,
        value: &'a [u8],
        ttl: Option<Duration>,
    },
    Delete {
        key: &'a Key,
    },
    /// Issue #124: the decommission's forwarded write — a `U` frame
    /// rather than a plain `S`, because until discovery publishes the
    /// post-leave roster the receiving entrant doesn't own the key yet
    /// and would answer a plain `S` with `W`. Also (issue #266) how
    /// `run_migration`'s own bulk transfer sends a key to the joiner:
    /// `if_absent` is `true` there (every old owner that holds the key
    /// sends it, so more than one send for the same key is now possible
    /// and must not clobber whichever arrives first — see
    /// `run_migration`'s doc comment) and always `false` for the
    /// decommission's unconditional handoff.
    HandoffSet {
        key: &'a Key,
        value: &'a [u8],
        ttl: Option<Duration>,
        if_absent: bool,
        /// Issue #295: the receiving node's own membership token — see
        /// `Command::HandoffSet::token`.
        token: &'a str,
    },
    /// Issue #124: the decommission's forwarded delete (`u`), same
    /// wrong-node reasoning as `HandoffSet`.
    HandoffDelete {
        key: &'a Key,
        /// Issue #295: see `HandoffSet::token` above.
        token: &'a str,
    },
    Clear(&'a ClearScope),
}

/// What a `c`/`F` clears (issue #106): one namespace, or everything.
#[derive(Clone, Debug, PartialEq, Eq)]
enum ClearScope {
    Namespace(Bytes),
    All,
}

impl ClearScope {
    /// The `c`/`F` frame that replays this clear on the joining node.
    fn message(&self) -> Vec<u8> {
        match self {
            ClearScope::Namespace(namespace) => {
                let mut message = format!("c {}\n", namespace.len()).into_bytes();
                message.extend_from_slice(namespace);
                message
            }
            ClearScope::All => b"F\n".to_vec(),
        }
    }
}

impl ForwardedWrite<'_> {
    fn timed_out_message(&self) -> &'static str {
        match self {
            ForwardedWrite::Set { .. } | ForwardedWrite::HandoffSet { .. } => {
                "forwarding the write to the joining node timed out"
            }
            ForwardedWrite::Delete { .. } | ForwardedWrite::HandoffDelete { .. } => {
                "forwarding the delete to the joining node timed out"
            }
            ForwardedWrite::Clear(_) => "forwarding the clear to the joining node timed out",
        }
    }

    async fn send(self, stream: &mut ClientStream) -> io::Result<()> {
        match self {
            ForwardedWrite::Set { key, value, ttl } => send_set(stream, key, value, ttl).await,
            ForwardedWrite::HandoffSet {
                key,
                value,
                ttl,
                if_absent,
                token,
            } => {
                stream
                    .write_all(&handoff_message(key, value, ttl, if_absent, token))
                    .await?;

                let mut ack = [0u8; 2];
                stream.read_exact(&mut ack).await?;

                if &ack != b"S\n" {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "peer did not acknowledge the handed-off entry",
                    ));
                }
                Ok(())
            }
            ForwardedWrite::HandoffDelete { key, token } => {
                stream
                    .write_all(&handoff_delete_message(key, token))
                    .await?;

                let mut ack = [0u8; 2];
                stream.read_exact(&mut ack).await?;

                // `D` (present there too) or `N` (this delete raced ahead
                // of the drain's own transfer of the key) both mean the
                // entrant won't serve a stale copy.
                if &ack != b"D\n" && &ack != b"N\n" {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "peer did not acknowledge the forwarded delete",
                    ));
                }
                Ok(())
            }
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
            ForwardedWrite::Clear(scope) => {
                stream.write_all(&scope.message()).await?;

                let mut ack = [0u8; 2];
                stream.read_exact(&mut ack).await?;

                if &ack != b"C\n" {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "joining node did not acknowledge the forwarded clear",
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
        key: Key,
        value: Bytes,
        ttl: Option<Duration>,
    },
    Delete {
        key: Key,
    },
    /// Issue #124: a decommission's leave-forward — sent as `U`/`u`
    /// frames (see `ForwardedWrite::HandoffSet`/`HandoffDelete` for why
    /// plain `S`/`D` won't do).
    HandoffSet {
        key: Key,
        value: Bytes,
        ttl: Option<Duration>,
    },
    HandoffDelete {
        key: Key,
    },
    Clear(ClearScope),
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
            OwnedForwardedWrite::HandoffSet { key, value, ttl } => {
                forward_on_shared_connection(
                    node_context,
                    target,
                    ForwardedWrite::HandoffSet {
                        key,
                        value,
                        ttl: *ttl,
                        // A concurrent client write must win unconditionally —
                        // see `ForwardedWrite::HandoffSet`'s own doc comment.
                        if_absent: false,
                        // Issue #295: `target`'s own token, not this
                        // node's — see `Command::HandoffSet::token`.
                        token: &target.token,
                    },
                )
                .await
            }
            OwnedForwardedWrite::HandoffDelete { key } => {
                forward_on_shared_connection(
                    node_context,
                    target,
                    ForwardedWrite::HandoffDelete {
                        key,
                        token: &target.token,
                    },
                )
                .await
            }
            OwnedForwardedWrite::Clear(scope) => {
                forward_on_shared_connection(node_context, target, ForwardedWrite::Clear(scope))
                    .await
            }
        }
    }

    /// Names the write kind for the WARN logs `forward_with_retries`
    /// emits — mirrors the `SET`/`DELETE` wire command letters informally,
    /// not the actual `S`/`D` protocol bytes.
    fn kind(&self) -> &'static str {
        match self {
            OwnedForwardedWrite::Set { .. } | OwnedForwardedWrite::HandoffSet { .. } => "SET",
            OwnedForwardedWrite::Delete { .. } | OwnedForwardedWrite::HandoffDelete { .. } => {
                "DELETE"
            }
            OwnedForwardedWrite::Clear(_) => "CLEAR",
        }
    }

    /// Names what this write was for, for `spawn_forward`'s WARN when it
    /// must actually drop a forward past `MAX_PENDING_FORWARD_WAITERS` —
    /// enough for an operator to tell which entry (or namespace) may now
    /// be stale on the joiner/entrant.
    fn describe(&self) -> String {
        match self {
            OwnedForwardedWrite::Set { key, .. } | OwnedForwardedWrite::HandoffSet { key, .. } => {
                format!("{} {key:?}", self.kind())
            }
            OwnedForwardedWrite::Delete { key } | OwnedForwardedWrite::HandoffDelete { key } => {
                format!("{} {key:?}", self.kind())
            }
            OwnedForwardedWrite::Clear(ClearScope::Namespace(namespace)) => {
                format!("CLEAR namespace {namespace:?}")
            }
            OwnedForwardedWrite::Clear(ClearScope::All) => "CLEAR (all namespaces)".to_string(),
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
            *connection =
                Some(connect_and_authenticate(node_context, &target.addr, AuthPeer::Node).await?);
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
/// Spawned via `spawn_forward`/`forward_tx` (mirroring how the `M` handler
/// hands `run_migration` itself to `run`'s own loop via `migration_tx` —
/// see `ConnectionConfig::forward_tx` for why this gets its own channel
/// rather than sharing that one, issue #219) rather than awaited inline in
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
/// to clients.
///
/// Only until discovery has confirmed the join, though (issue #66):
/// from then on `L` does list the joiner, so a `W` is exactly what a
/// still-stale client needs — its refresh-and-retry lands on the joiner
/// — whereas serving locally would hand out this node's dead copy (which
/// the sweep is about to reclaim, and which writes sent straight to the
/// joiner no longer update): every read that reached this node in the
/// rest of the forwarding window missed (`N`) once the sweep had run.
/// Write forwarding keeps going for the whole window regardless.
fn wrong_node(node_context: &NodeContext, key: &Key) -> bool {
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

    displaced && !serving_locally_for_unconfirmed_join(node_context, key)
}

/// `wrong_node`'s exception: a handoff this node ran is still forwarding
/// `key` to a joiner discovery hasn't confirmed yet (see
/// `ActiveMigration::confirmed`), so this node is the only owner a
/// client's `L` can name and must keep serving the key itself.
fn serving_locally_for_unconfirmed_join(node_context: &NodeContext, key: &Key) -> bool {
    let mut slot = node_context
        .active_migration
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    // Same lazy expiry as `migration_target_for`.
    if slot.as_ref().is_some_and(ActiveMigration::expired) {
        *slot = None;
    }
    slot.as_ref().is_some_and(|active| {
        !active.confirmed
            && active.forwarding_open()
            && active
                .after_ring
                .is_owner(key, &active.joining_name, active.replication)
    })
}

/// If a handoff is currently in flight and `key` is one it's moving (per
/// its `after_ring`), returns where to forward it — for `handle_connection`
/// to also propagate a client's `S`/`D` for that key there, so the
/// joining node doesn't end up serving a stale value once promoted (see
/// the staged-join handoff design).
fn migration_target_for(node_context: &NodeContext, key: &Key) -> Option<ForwardTarget> {
    let mut slot = node_context
        .active_migration
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());

    // Issue #3: a completed handoff keeps forwarding until the grace
    // passes (discovery publishes the joiner — or abandons the join —
    // well within it). Expired entries are cleared lazily here.
    if slot.as_ref().is_some_and(ActiveMigration::expired) {
        *slot = None;
    }

    slot.as_ref()
        // Issue #62: an unconfirmed completed slot outlives its grace (it
        // is still holding its marks back from the sweep), but forwarding
        // ends with the grace regardless.
        .filter(|active| active.forwarding_open())
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
            token: active.joining_token.clone(),
        })
}

/// Issue #124: if a decommission is in flight and this node owned
/// `key`, returns the forward target for the node that newly enters the
/// key's top-R once this node is gone — a concurrent client write must
/// reach it, or the copy handed over by the drain-out transfer goes
/// stale the moment discovery publishes the post-leave roster (the
/// exact mirror of `migration_target_for`'s join-side reasoning).
fn leave_target_for(node_context: &NodeContext, key: &Key) -> Option<ForwardTarget> {
    let leaving = node_context
        .leaving
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    let leave = leaving.as_ref()?;
    let entrant = leave.entrant_for(key, &node_context.name)?;
    let addr = leave.addresses.get(&entrant)?.clone();
    let token = leave.tokens.get(&entrant)?.clone();

    let connection = {
        let mut connections = leave
            .connections
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        Arc::clone(
            connections
                .entry(addr.clone())
                .or_insert_with(|| Arc::new(AsyncMutex::new(None))),
        )
    };

    Some(ForwardTarget {
        addr,
        connection,
        token,
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
    key: &Key,
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

    fn key(name: &[u8]) -> Key {
        Key::unnamespaced(Bytes::copy_from_slice(name))
    }
    use bytes::Bytes;

    /// A stand-in peer address for `handle_connection` in tests — only the
    /// `M`/`X` token-mismatch WARN logs read it, never the control flow.
    fn test_client_addr() -> SocketAddr {
        "127.0.0.1:9000".parse().unwrap()
    }

    #[tokio::test(flavor = "current_thread")]
    async fn run_cache_stores_and_retrieves_a_value() {
        let (request_tx, request_rx) = mpsc::channel(1);

        let cache_task = tokio::spawn(run_cache(request_rx, MAX_CACHE_MEMORY_BYTES, Vec::new()));

        let set_response = send_command(
            &request_tx,
            Command::Set {
                key: key(b"name"),
                value: Bytes::from_static(b"Alice"),
                ttl: None,
            },
        )
        .await;

        assert_eq!(set_response, Response::Stored);

        let get_response = send_command(&request_tx, Command::Get { key: key(b"name") }).await;

        assert_eq!(get_response, Response::Value(Bytes::from_static(b"Alice")));

        drop(request_tx);
        cache_task.await.unwrap();
    }

    #[tokio::test(flavor = "current_thread")]
    async fn handle_connection_processes_multiple_commands() {
        let (mut client, server) = tcp_pair().await;

        let (request_tx, request_rx) = mpsc::channel(1);
        let (_shutdown_tx, shutdown_rx) = watch::channel(false);

        let cache_task = tokio::spawn(run_cache(request_rx, MAX_CACHE_MEMORY_BYTES, Vec::new()));
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
                forward_tx: mpsc::channel(1).0,
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
    async fn handle_connection_keeps_namespaces_apart() {
        // Issue #105: the same key name in two namespaces and in the
        // default namespace are three independent entries, and `g 0` is
        // the default namespace.
        let (mut client, server) = tcp_pair().await;

        let (request_tx, request_rx) = mpsc::channel(1);
        let (_shutdown_tx, shutdown_rx) = watch::channel(false);

        let cache_task = tokio::spawn(run_cache(request_rx, MAX_CACHE_MEMORY_BYTES, Vec::new()));
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
                forward_tx: mpsc::channel(1).0,
            },
            shutdown_rx,
        ));

        client
            .write_all(
                concat!(
                    "S 4 5\nnameAlice",
                    "s 5 4 3\nusersnameBob",
                    "s 6 4 5\nordersnameCarol",
                    "g 5 4\nusersname",
                    "g 6 4\nordersname",
                    "g 0 4\nname",
                    "G 4\nname",
                    "d 5 4\nusersname",
                    "g 5 4\nusersname",
                    "G 4\nname",
                )
                .as_bytes(),
            )
            .await
            .unwrap();

        client.shutdown().await.unwrap();

        let expected = b"S\nS\nS\nV 3\nBobV 5\nCarolV 5\nAliceV 5\nAliceD\nN\nV 5\nAlice";
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
                forward_tx: mpsc::channel(1).0,
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

        let error = run(
            &address,
            None,
            None,
            None,
            MAX_CACHE_MEMORY_BYTES,
            None,
            Duration::from_secs(25),
            ConnectionLimits::default(),
            Vec::new(),
        )
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
                forward_tx: mpsc::channel(1).0,
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
        // just under `IDLE_TIMEOUT` apart could hold a `DEFAULT_MAX_CONNECTIONS`
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
                forward_tx: mpsc::channel(1).0,
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
            leaving: Arc::new(Mutex::new(None)),
            active_rereplication: Arc::new(Mutex::new(None)),
            rereplication_tx: mpsc::channel(1).0,
            shutdown_rx: watch::channel(false).1,
        };

        let target = ForwardTarget {
            addr: joining_addr,
            connection: Arc::new(AsyncMutex::new(None)),
            token: "tok-target".to_string(),
        };

        let forward_task = tokio::spawn(async move {
            set_on_joining_node(&node_context, &target, &key(b"name"), b"Alice", None).await
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
            leaving: Arc::new(Mutex::new(None)),
            active_rereplication: Arc::new(Mutex::new(None)),
            rereplication_tx: mpsc::channel(1).0,
            shutdown_rx: watch::channel(false).1,
        };

        // Exactly what `migration_target_for` would hand every concurrent
        // client write racing the same handoff: the same `addr` and the
        // same shared `connection`.
        let target = ForwardTarget {
            addr: joining_addr,
            connection: Arc::new(AsyncMutex::new(None)),
            token: "tok-target".to_string(),
        };

        set_on_joining_node(&node_context, &target, &key(b"name"), b"Alice", None)
            .await
            .unwrap();
        set_on_joining_node(&node_context, &target, &key(b"age"), b"30", None)
            .await
            .unwrap();
        delete_on_joining_node(&node_context, &target, &key(b"name"))
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
            leaving: Arc::new(Mutex::new(None)),
            active_rereplication: Arc::new(Mutex::new(None)),
            rereplication_tx: mpsc::channel(1).0,
            shutdown_rx: watch::channel(false).1,
        });
        let target = Arc::new(ForwardTarget {
            addr: joining_addr,
            connection: Arc::new(AsyncMutex::new(None)),
            token: "tok-target".to_string(),
        });

        let first_forward = tokio::spawn({
            let node_context = Arc::clone(&node_context);
            let target = Arc::clone(&target);
            async move {
                set_on_joining_node(&node_context, &target, &key(b"name"), b"Alice", None).await
            }
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

        set_on_joining_node(&node_context, &target, &key(b"age"), b"30", None)
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
            leaving: Arc::new(Mutex::new(None)),
            active_rereplication: Arc::new(Mutex::new(None)),
            rereplication_tx: mpsc::channel(1).0,
            shutdown_rx: watch::channel(false).1,
        };

        let target = ForwardTarget {
            addr: joining_addr,
            connection: Arc::new(AsyncMutex::new(None)),
            token: "tok-target".to_string(),
        };

        // If this never retried, the joining task would hang forever
        // waiting for its second `accept` and this test would time out.
        forward_with_retries(
            node_context,
            target,
            OwnedForwardedWrite::Set {
                key: key(b"name"),
                value: Bytes::from_static(b"Alice"),
                ttl: None,
            },
        )
        .await;

        joining_task.await.unwrap();
    }

    #[tokio::test(flavor = "current_thread")]
    async fn a_full_forward_channel_does_not_block_a_write_on_another_connection() {
        // Regression (issue #219): every per-write forward
        // (`forward_with_retries`) used to share `migration_tx` with the
        // one singleton bulk-migration task, sized for "one migration in
        // flight" (capacity 4). Each forward can occupy a slot for up to
        // `KEY_TRANSFER_ATTEMPTS` x `FORWARD_TIMEOUT` when its peer is
        // slow or unresponsive, so with the shared channel's slots all
        // held by stalled forwards, a *different* client connection's own
        // write to a migrating key would block on `migration_tx
        // .send().await` — even though that connection's write had
        // already been stored and acked locally, and its `S`/`D` response
        // already written. `spawn_forward` now hands per-write forwards
        // to a dedicated `forward_tx` via `try_send`, never `.await`, so
        // the connection that triggered a forward is never the one
        // blocked on this channel — see the follow-up below for what a
        // full channel does instead (spawn a background waiter rather
        // than drop the forward outright).
        //
        // This fills a tiny `forward_tx` with real `forward_with_retries`
        // calls to a peer that accepts but never responds — genuinely
        // stalled forwards, not just inert placeholders — with nothing
        // draining the channel (standing in for `run`'s own consumer
        // being unavailable, e.g. mid-shutdown, or simply outrun by a
        // burst of concurrent forwards). A second, independent connection
        // then writes to the same migrating key; before this fix that
        // write's own response would never arrive because
        // `handle_connection` would still be blocked trying to enqueue
        // its forward. See
        // `a_forward_queued_behind_a_full_channel_still_reaches_the_peer_once_drained`
        // for proof that this connection's forward isn't lost either —
        // only this test's own response is under scrutiny here.
        let stalled_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let stalled_addr = stalled_listener.local_addr().unwrap().to_string();
        let stalled_task = tokio::spawn(async move {
            // Accept every dial a stalled forward makes and hold the
            // connection open without ever writing an ack — the peer
            // "never responds", so each forward stays stuck retrying
            // until `KEY_TRANSFER_ATTEMPTS` x `FORWARD_TIMEOUT` elapses.
            let mut held = Vec::new();
            while let Ok((connection, _)) = stalled_listener.accept().await {
                held.push(connection);
            }
        });

        // Two members, R=2: the joiner is in every key's top-R regardless
        // of hash, so a `Set` for any key forwards — same setup as
        // `migrate_command_multi_set_forwards_owned_keys_to_the_joining_node`.
        let after_ring = Arc::new(HashRing::new(vec![
            "ready-node".to_string(),
            "joiner-0".to_string(),
        ]));
        let node_context = NodeContext {
            name: "ready-node".to_string(),
            token: "tk-ready-node".to_string(),
            discovery_addr: "127.0.0.1:0".to_string(),
            active_migration: Arc::new(Mutex::new(Some(ActiveMigration {
                joining_name: "joiner-0".to_string(),
                joining_addr: stalled_addr.clone(),
                joining_token: "tok-joiner-0".to_string(),
                after_ring,
                replication: 2,
                completed_at: None,
                forwarding_grace: Duration::from_secs(60),
                acked_entries: None,
                abort_requested: Arc::new(AtomicBool::new(false)),
                marked_keys: Vec::new(),
                confirmed: false,
                pre_completion_ring: None,
                pending_clears: Vec::new(),
                forward_connection: Arc::new(AsyncMutex::new(None)),
            }))),
            known_ring: Arc::new(Mutex::new(None)),
            auth_secret: None,
            tls_connector: None,
            request_tx: mpsc::channel(1).0,
            leaving: Arc::new(Mutex::new(None)),
            active_rereplication: Arc::new(Mutex::new(None)),
            rereplication_tx: mpsc::channel(1).0,
            shutdown_rx: watch::channel(false).1,
        };

        // A dedicated forward channel, deliberately tiny (capacity 2) and
        // with nothing draining it in this test — the worst case
        // `spawn_forward` must tolerate.
        let (forward_tx, forward_rx) = mpsc::channel::<MigrationTask>(2);
        for name in [b"already-stalled-1".as_slice(), b"already-stalled-2"] {
            forward_tx
                .try_send(Box::pin(forward_with_retries(
                    node_context.clone(),
                    ForwardTarget {
                        addr: stalled_addr.clone(),
                        connection: Arc::new(AsyncMutex::new(None)),
                        token: "tok-target".to_string(),
                    },
                    OwnedForwardedWrite::Set {
                        key: key(name),
                        value: Bytes::from_static(b"x"),
                        ttl: None,
                    },
                )))
                .expect("forward_tx has capacity 2 and is otherwise empty");
        }

        let (request_tx, request_rx) = mpsc::channel(4);
        let cache_task = tokio::spawn(run_cache(request_rx, MAX_CACHE_MEMORY_BYTES, Vec::new()));
        let (mut client, server) = tcp_pair().await;
        let (_shutdown_tx, shutdown_rx) = watch::channel(false);

        let connection_task = tokio::spawn(handle_connection(
            ServerStream::Plain(server),
            test_client_addr(),
            request_tx.clone(),
            ConnectionConfig {
                idle_timeout: IDLE_TIMEOUT,
                auth_secret: None,
                tls_acceptor: None,
                node_context: Some(node_context),
                migration_tx: mpsc::channel(1).0,
                forward_tx,
            },
            shutdown_rx,
        ));

        // This `Set` is itself for a key mid-handoff, so it also tries to
        // enqueue a forward — into the already-full channel above.
        client.write_all(b"S 4 5\nnameAlice").await.unwrap();

        let expected = b"S\n";
        let mut response = vec![0u8; expected.len()];
        tokio::time::timeout(Duration::from_secs(2), client.read_exact(&mut response))
            .await
            .expect(
                "a full forward_tx must not block this connection's own response — \
                 spawn_forward sends with try_send, never .await",
            )
            .unwrap();
        assert_eq!(response, expected);

        client.shutdown().await.unwrap();
        connection_task.await.unwrap().unwrap();
        drop(request_tx);
        cache_task.await.unwrap();
        stalled_task.abort();

        // This connection's own `Set` forward found `forward_tx` already
        // full too, so `spawn_forward` spawned a background waiter for it
        // (see `spawn_forward` and `PENDING_FORWARD_WAITERS`) rather than
        // simply dropping it. Nothing in this test ever drains
        // `forward_rx`, so that waiter is still blocked on its own
        // `send(...).await`; dropping the receiver now closes the
        // channel, which resolves that blocked send with an error and
        // lets the waiter task finish (decrementing
        // `PENDING_FORWARD_WAITERS`) instead of leaking for the rest of
        // this test binary's process.
        drop(forward_rx);
        for _ in 0..8 {
            tokio::task::yield_now().await;
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn a_forward_queued_behind_a_full_channel_still_reaches_the_peer_once_drained() {
        // Issue #219 follow-up: a full `forward_tx` must not simply drop
        // the new forward — that would lose the write on the joiner/
        // entrant, the same class of silent data loss issue #176 fixed
        // for `MultiSet`. `spawn_forward` instead spawns a detached
        // "waiter" that blocks on the channel's own `send(...).await`
        // until a slot frees up. This proves the write is eventually
        // *delivered* once something (here, standing in for `run`'s own
        // loop) actually drains the channel — the companion test
        // `a_full_forward_channel_does_not_block_a_write_on_another_connection`
        // only proves the caller itself never blocks; it doesn't prove
        // the forward survives.
        let joining_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let joining_addr = joining_listener.local_addr().unwrap().to_string();
        let joining_task = tokio::spawn(async move {
            let (mut connection, _) = joining_listener.accept().await.unwrap();
            let mut buffer = [0u8; 256];
            let bytes_read = connection.read(&mut buffer).await.unwrap();
            assert!(bytes_read > 0);
            connection.write_all(b"S\n").await.unwrap();
            buffer[..bytes_read].to_vec()
        });

        // Two members, R=2: the joiner is in every key's top-R regardless
        // of hash, so a `Set` for any key forwards.
        let after_ring = Arc::new(HashRing::new(vec![
            "ready-node".to_string(),
            "joiner-0".to_string(),
        ]));
        let node_context = NodeContext {
            name: "ready-node".to_string(),
            token: "tk-ready-node".to_string(),
            discovery_addr: "127.0.0.1:0".to_string(),
            active_migration: Arc::new(Mutex::new(Some(ActiveMigration {
                joining_name: "joiner-0".to_string(),
                joining_addr: joining_addr.clone(),
                joining_token: "tok-joiner-0".to_string(),
                after_ring,
                replication: 2,
                completed_at: None,
                forwarding_grace: Duration::from_secs(60),
                acked_entries: None,
                abort_requested: Arc::new(AtomicBool::new(false)),
                marked_keys: Vec::new(),
                confirmed: false,
                pre_completion_ring: None,
                pending_clears: Vec::new(),
                forward_connection: Arc::new(AsyncMutex::new(None)),
            }))),
            known_ring: Arc::new(Mutex::new(None)),
            auth_secret: None,
            tls_connector: None,
            request_tx: mpsc::channel(1).0,
            leaving: Arc::new(Mutex::new(None)),
            active_rereplication: Arc::new(Mutex::new(None)),
            rereplication_tx: mpsc::channel(1).0,
            shutdown_rx: watch::channel(false).1,
        };

        // Capacity 1, occupied by an inert filler that's never polled —
        // it just holds the one slot until this test explicitly drains
        // it below, standing in for `run`'s own consumer being busy or
        // temporarily unavailable.
        let (forward_tx, mut forward_rx) = mpsc::channel::<MigrationTask>(1);
        forward_tx
            .try_send(Box::pin(std::future::pending()))
            .expect("capacity 1, channel starts empty");

        let (request_tx, request_rx) = mpsc::channel(4);
        let cache_task = tokio::spawn(run_cache(request_rx, MAX_CACHE_MEMORY_BYTES, Vec::new()));
        let (mut client, server) = tcp_pair().await;
        let (_shutdown_tx, shutdown_rx) = watch::channel(false);

        let connection_task = tokio::spawn(handle_connection(
            ServerStream::Plain(server),
            test_client_addr(),
            request_tx.clone(),
            ConnectionConfig {
                idle_timeout: IDLE_TIMEOUT,
                auth_secret: None,
                tls_acceptor: None,
                node_context: Some(node_context),
                migration_tx: mpsc::channel(1).0,
                forward_tx,
            },
            shutdown_rx,
        ));

        // This `Set` finds `forward_tx` already full, so `spawn_forward`
        // spawns a waiter for it rather than dropping it.
        client.write_all(b"S 4 5\nnameAlice").await.unwrap();
        let expected = b"S\n";
        let mut response = vec![0u8; expected.len()];
        client.read_exact(&mut response).await.unwrap();
        assert_eq!(response, expected);

        client.shutdown().await.unwrap();
        connection_task.await.unwrap().unwrap();
        drop(request_tx);
        cache_task.await.unwrap();

        // Give the waiter a chance to actually reach `send(...).await`
        // and register itself against the still-full channel.
        for _ in 0..8 {
            tokio::task::yield_now().await;
        }

        // Drain the one slot (standing in for `run`'s own loop) — the
        // inert filler comes out first (FIFO), freeing the slot the
        // waiter is blocked on.
        let filler = forward_rx.recv().await.unwrap();
        drop(filler);

        // The waiter's blocked `send` can now complete; drain again to
        // get the real forward and run it to actually deliver it to the
        // peer.
        let forward = tokio::time::timeout(Duration::from_secs(2), forward_rx.recv())
            .await
            .expect("the waiter must eventually enqueue the forward once a slot frees up")
            .expect("forward_tx's sender is still alive — the waiter task still holds a clone");
        forward.await;

        let received = joining_task.await.unwrap();
        assert_eq!(
            received,
            set_message(&key(b"name"), b"Alice", None),
            "a forward queued behind a full channel must still reach the peer once drained, \
             not be dropped"
        );
    }

    /// Issue #124 helper: one plain HTTP GET, returning (status line,
    /// body).
    async fn http_get(addr: &str, path: &str) -> (String, String) {
        let mut stream = TcpStream::connect(addr).await.unwrap();
        stream
            .write_all(format!("GET {path} HTTP/1.1\r\nHost: x\r\n\r\n").as_bytes())
            .await
            .unwrap();
        let mut response = Vec::new();
        stream.read_to_end(&mut response).await.unwrap();
        let response = String::from_utf8(response).unwrap();
        let (head, body) = response.split_once("\r\n\r\n").unwrap();
        (head.lines().next().unwrap().to_string(), body.to_string())
    }

    #[tokio::test(flavor = "current_thread")]
    async fn metrics_endpoint_reports_cache_state_and_counters() {
        let (request_tx, request_rx) = mpsc::channel(16);
        let cache_task = tokio::spawn(run_cache(
            request_rx,
            MAX_CACHE_MEMORY_BYTES,
            vec![(Bytes::from_static(b"users"), 4096)],
        ));
        let (_shutdown_tx, shutdown_rx) = watch::channel(false);

        send_command(
            &request_tx,
            Command::Set {
                key: key(b"k"),
                value: Bytes::from_static(b"v"),
                ttl: None,
            },
        )
        .await;
        send_command(
            &request_tx,
            Command::Set {
                key: Key::new(Bytes::from_static(b"users"), Bytes::from_static(b"a")),
                value: Bytes::from_static(b"x"),
                ttl: None,
            },
        )
        .await;
        send_command(&request_tx, Command::Get { key: key(b"k") }).await;
        send_command(
            &request_tx,
            Command::Get {
                key: key(b"missing"),
            },
        )
        .await;
        send_command(
            &request_tx,
            Command::Set {
                key: key(b"counter"),
                value: Bytes::from_static(b"1"),
                ttl: None,
            },
        )
        .await;
        send_command(
            &request_tx,
            Command::Incr {
                key: key(b"counter"),
                delta: 1,
            },
        )
        .await;
        send_command(
            &request_tx,
            Command::CasSet {
                key: key(b"flag"),
                condition: crate::cache::CasCondition::Absent,
                value: Bytes::from_static(b"on"),
                ttl: None,
            },
        )
        .await;
        send_command(
            &request_tx,
            Command::CasDelete {
                key: key(b"flag"),
                expected_digest: crate::cache::content_digest(b"on"),
            },
        )
        .await;

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        let metrics_task = tokio::spawn(run_metrics_server(
            listener,
            request_tx.clone(),
            Arc::new(Semaphore::new(DEFAULT_MAX_CONNECTIONS)),
            DEFAULT_MAX_CONNECTIONS,
            Arc::new(Mutex::new(None)),
            false,
            shutdown_rx,
        ));

        let (status, body) = http_get(&addr, "/metrics").await;
        assert_eq!(status, "HTTP/1.1 200 OK");
        assert!(body.contains("nanocached_node_entries 3\n"), "{body}");
        assert!(
            body.contains("nanocached_node_namespace_budget_bytes{namespace=\"users\"} 4096\n"),
            "{body}"
        );
        assert!(body.contains("nanocached_node_hits_total 1\n"), "{body}");
        assert!(body.contains("nanocached_node_misses_total 1\n"), "{body}");
        assert!(body.contains("nanocached_node_sets_total 3\n"), "{body}");
        assert!(body.contains("nanocached_node_incrs_total 1\n"), "{body}");
        assert!(
            body.contains("nanocached_node_cas_sets_total 1\n"),
            "{body}"
        );
        assert!(
            body.contains("nanocached_node_cas_deletes_total 1\n"),
            "{body}"
        );
        assert!(
            body.contains("nanocached_node_namespace_entries{namespace=\"users\"} 1\n"),
            "{body}"
        );
        assert!(
            body.contains("nanocached_node_namespace_entries{namespace=\"\"} 2\n"),
            "{body}"
        );
        assert!(
            body.contains("nanocached_node_memory_used_bytes "),
            "{body}"
        );
        assert!(body.contains("nanocached_node_connections 0\n"), "{body}");

        let (status, _) = http_get(&addr, "/healthz").await;
        assert_eq!(status, "HTTP/1.1 200 OK");
        let (status, _) = http_get(&addr, "/nope").await;
        assert_eq!(status, "HTTP/1.1 404 Not Found");

        metrics_task.abort();
        drop(request_tx);
        cache_task.await.unwrap();
    }

    #[tokio::test(flavor = "current_thread")]
    async fn readyz_gates_on_membership_for_cluster_nodes() {
        let (request_tx, _request_rx) = mpsc::channel(16);
        let (_shutdown_tx, shutdown_rx) = watch::channel(false);
        let known_ring: KnownRing = Arc::new(Mutex::new(None));

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        let metrics_task = tokio::spawn(run_metrics_server(
            listener,
            request_tx,
            Arc::new(Semaphore::new(DEFAULT_MAX_CONNECTIONS)),
            DEFAULT_MAX_CONNECTIONS,
            Arc::clone(&known_ring),
            true,
            shutdown_rx,
        ));

        // Cluster node, no membership yet: not ready.
        let (status, _) = http_get(&addr, "/readyz").await;
        assert_eq!(status, "HTTP/1.1 503 Service Unavailable");

        // Membership adopted: ready.
        *known_ring.lock().unwrap() = Some(Arc::new(Membership {
            ring: Arc::new(HashRing::new(vec!["self".to_string()])),
            replication: 1,
        }));
        let (status, _) = http_get(&addr, "/readyz").await;
        assert_eq!(status, "HTTP/1.1 200 OK");

        metrics_task.abort();
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
            DEFAULT_MAX_CONNECTIONS_PER_IP,
            ConnectionConfig {
                idle_timeout: IDLE_TIMEOUT,
                auth_secret: None,
                tls_acceptor: None,
                node_context: None,
                migration_tx: mpsc::channel(1).0,
                forward_tx: mpsc::channel(1).0,
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
            DEFAULT_MAX_CONNECTIONS_PER_IP,
            ConnectionConfig {
                idle_timeout: IDLE_TIMEOUT,
                auth_secret: None,
                tls_acceptor: None,
                node_context: None,
                migration_tx: mpsc::channel(1).0,
                forward_tx: mpsc::channel(1).0,
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
        // Regression: `DEFAULT_MAX_CONNECTIONS` alone lets a single source IP hold
        // every one of the global permits by itself, starving every other
        // client, without the global semaphore ever reporting anything
        // unusual short of the very last permit. `DEFAULT_MAX_CONNECTIONS_PER_IP`
        // must reject a source once it individually reaches its own cap,
        // independent of how much global headroom remains.
        let connection_limit = Arc::new(Semaphore::new(10));
        let per_ip_connections: PerIpConnections = Arc::new(Mutex::new(HashMap::new()));
        let (request_tx, _request_rx) = mpsc::channel(1);

        let ip = std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST);
        // Stands in for `DEFAULT_MAX_CONNECTIONS_PER_IP - 1` other already-live
        // connections from this IP, without actually dispatching that
        // many for the test.
        per_ip_connections
            .lock()
            .unwrap()
            .insert(ip, DEFAULT_MAX_CONNECTIONS_PER_IP - 1);

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
            DEFAULT_MAX_CONNECTIONS_PER_IP,
            ConnectionConfig {
                idle_timeout: IDLE_TIMEOUT,
                auth_secret: None,
                tls_acceptor: None,
                node_context: None,
                migration_tx: mpsc::channel(1).0,
                forward_tx: mpsc::channel(1).0,
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
            Some(DEFAULT_MAX_CONNECTIONS_PER_IP)
        );

        let (mut second_client, second_server) = tcp_pair().await;
        let second_address = SocketAddr::new(ip, 9001);

        dispatch_connection(
            second_server,
            second_address,
            request_tx.clone(),
            Arc::clone(&connection_limit),
            Arc::clone(&per_ip_connections),
            DEFAULT_MAX_CONNECTIONS_PER_IP,
            ConnectionConfig {
                idle_timeout: IDLE_TIMEOUT,
                auth_secret: None,
                tls_acceptor: None,
                node_context: None,
                migration_tx: mpsc::channel(1).0,
                forward_tx: mpsc::channel(1).0,
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
            Some(DEFAULT_MAX_CONNECTIONS_PER_IP),
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
        for _ in 0..DEFAULT_MAX_CONNECTIONS_PER_IP {
            guards.push(
                try_acquire_per_ip(&counts, ip, DEFAULT_MAX_CONNECTIONS_PER_IP)
                    .expect("under the per-IP cap"),
            );
        }

        assert!(
            try_acquire_per_ip(&counts, ip, DEFAULT_MAX_CONNECTIONS_PER_IP).is_none(),
            "the per-IP cap must reject a connection once DEFAULT_MAX_CONNECTIONS_PER_IP is reached"
        );

        // A different source IP has its own, independent budget.
        let other_ip = std::net::IpAddr::V4(std::net::Ipv4Addr::new(10, 0, 0, 1));
        assert!(try_acquire_per_ip(&counts, other_ip, DEFAULT_MAX_CONNECTIONS_PER_IP).is_some());

        // Dropping one guard frees its slot for the same IP again.
        guards.pop();
        assert!(try_acquire_per_ip(&counts, ip, DEFAULT_MAX_CONNECTIONS_PER_IP).is_some());
    }

    #[test]
    fn a_lowered_per_ip_cap_is_honored() {
        // Issue #126: the cap is a runtime value now — a small deployment
        // running with --max-connections-per-ip 2 must reject the third
        // connection from one source, not the 257th.
        let counts: PerIpConnections = Arc::new(Mutex::new(HashMap::new()));
        let ip = std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST);

        let _first = try_acquire_per_ip(&counts, ip, 2).expect("first fits");
        let _second = try_acquire_per_ip(&counts, ip, 2).expect("second fits");
        assert!(try_acquire_per_ip(&counts, ip, 2).is_none());
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
                forward_tx: mpsc::channel(1).0,
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
                forward_tx: mpsc::channel(1).0,
            },
            shutdown_rx,
        ));

        client.write_all(b"G 4\nname").await.unwrap();

        let request = request_rx.recv().await.unwrap();

        assert_eq!(request.command, Command::Get { key: key(b"name") },);

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
    async fn handle_connection_dispatches_incr_and_replies_with_the_new_value() {
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
                forward_tx: mpsc::channel(1).0,
            },
            shutdown_rx,
        ));

        client.write_all(b"i 0 3 5\nfoo").await.unwrap();

        let request = request_rx.recv().await.unwrap();
        assert_eq!(
            request.command,
            Command::Incr {
                key: key(b"foo"),
                delta: 5,
            },
        );
        request
            .response_tx
            .send(Response::Incremented(Bytes::from_static(b"15"), None))
            .unwrap();

        let expected = b"I 2\n15";
        let mut response = vec![0_u8; expected.len()];
        client.read_exact(&mut response).await.unwrap();
        assert_eq!(response, expected);

        shutdown_tx.send_replace(true);
        connection_task.await.unwrap().unwrap();
    }

    #[tokio::test(flavor = "current_thread")]
    async fn handle_connection_dispatches_cas_set_and_replies_with_stored() {
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
                forward_tx: mpsc::channel(1).0,
            },
            shutdown_rx,
        ));

        client.write_all(b"k 0 3 5 A\nfooAlice").await.unwrap();

        let request = request_rx.recv().await.unwrap();
        assert_eq!(
            request.command,
            Command::CasSet {
                key: key(b"foo"),
                condition: crate::cache::CasCondition::Absent,
                value: Bytes::from_static(b"Alice"),
                ttl: None,
            },
        );
        request.response_tx.send(Response::Stored).unwrap();

        let expected = b"S\n";
        let mut response = vec![0_u8; expected.len()];
        client.read_exact(&mut response).await.unwrap();
        assert_eq!(response, expected);

        shutdown_tx.send_replace(true);
        connection_task.await.unwrap().unwrap();
    }

    #[tokio::test(flavor = "current_thread")]
    async fn handle_connection_dispatches_cas_delete_and_replies_with_deleted() {
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
                forward_tx: mpsc::channel(1).0,
            },
            shutdown_rx,
        ));

        client
            .write_all(b"x 0 3 3bc51062973c458d5a6f2d8d64a02324\nfoo")
            .await
            .unwrap();

        let request = request_rx.recv().await.unwrap();
        assert_eq!(
            request.command,
            Command::CasDelete {
                key: key(b"foo"),
                expected_digest: [
                    0x3b, 0xc5, 0x10, 0x62, 0x97, 0x3c, 0x45, 0x8d, 0x5a, 0x6f, 0x2d, 0x8d, 0x64,
                    0xa0, 0x23, 0x24,
                ],
            },
        );
        request.response_tx.send(Response::Deleted).unwrap();

        let expected = b"D\n";
        let mut response = vec![0_u8; expected.len()];
        client.read_exact(&mut response).await.unwrap();
        assert_eq!(response, expected);

        shutdown_tx.send_replace(true);
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
                forward_tx: mpsc::channel(1).0,
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
                forward_tx: mpsc::channel(1).0,
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

        let cache_task = tokio::spawn(run_cache(request_rx, MAX_CACHE_MEMORY_BYTES, Vec::new()));
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
                forward_tx: mpsc::channel(1).0,
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

        let cache_task = tokio::spawn(run_cache(request_rx, MAX_CACHE_MEMORY_BYTES, Vec::new()));
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
                forward_tx: mpsc::channel(1).0,
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
                forward_tx: mpsc::channel(1).0,
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
                forward_tx: mpsc::channel(1).0,
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

        let cache_task = tokio::spawn(run_cache(request_rx, MAX_CACHE_MEMORY_BYTES, Vec::new()));
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
                forward_tx: mpsc::channel(1).0,
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
    async fn handle_connection_clears_one_namespace_or_everything() {
        // Issue #106: `c` drops one namespace (and only it); `F` drops all.
        let (mut client, server) = tcp_pair().await;

        let (request_tx, request_rx) = mpsc::channel(1);
        let (_shutdown_tx, shutdown_rx) = watch::channel(false);

        let cache_task = tokio::spawn(run_cache(request_rx, MAX_CACHE_MEMORY_BYTES, Vec::new()));
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
                forward_tx: mpsc::channel(1).0,
            },
            shutdown_rx,
        ));

        client
            .write_all(
                concat!(
                    "S 4 5\nnameAlice",
                    "s 5 4 3\nusersnameBob",
                    "s 6 4 5\nordersnameCarol",
                    "c 5\nusers",
                    "g 5 4\nusersname",
                    "g 6 4\nordersname",
                    "G 4\nname",
                    "c 0\n",
                    "G 4\nname",
                    "F\n",
                    "g 6 4\nordersname",
                    "c 7\nmissing",
                )
                .as_bytes(),
            )
            .await
            .unwrap();

        client.shutdown().await.unwrap();

        let expected = b"S\nS\nS\nC\nN\nV 5\nCarolV 5\nAliceC\nN\nC\nN\nC\n";
        let mut response = vec![0_u8; expected.len()];

        client.read_exact(&mut response).await.unwrap();

        assert_eq!(response, expected);

        connection_task.await.unwrap().unwrap();

        drop(request_tx);
        cache_task.await.unwrap();
    }

    #[tokio::test(flavor = "current_thread")]
    async fn clear_echoes_the_tag_in_tagged_mode() {
        let (mut client, server) = tcp_pair().await;

        let (request_tx, request_rx) = mpsc::channel(1);
        let (_shutdown_tx, shutdown_rx) = watch::channel(false);

        let cache_task = tokio::spawn(run_cache(request_rx, MAX_CACHE_MEMORY_BYTES, Vec::new()));
        let connection_task = tokio::spawn(handle_connection(
            ServerStream::Plain(server),
            test_client_addr(),
            request_tx.clone(),
            ConnectionConfig {
                idle_timeout: IDLE_TIMEOUT,
                auth_secret: Some(Bytes::from_static(b"secret")),
                tls_acceptor: None,
                node_context: None,
                migration_tx: mpsc::channel(1).0,
                forward_tx: mpsc::channel(1).0,
            },
            shutdown_rx,
        ));

        client
            .write_all(b"A 6 T\nsecretc 5 7\nusersF 8\n")
            .await
            .unwrap();
        client.shutdown().await.unwrap();

        let expected = b"OnT\nC 7\nC 8\n";
        let mut response = vec![0_u8; expected.len()];
        client.read_exact(&mut response).await.unwrap();
        assert_eq!(response, expected);

        connection_task.await.unwrap().unwrap();
        drop(request_tx);
        cache_task.await.unwrap();
    }

    #[test]
    fn clear_scope_messages_are_the_c_and_f_frames() {
        assert_eq!(
            ClearScope::Namespace(Bytes::from_static(b"users")).message(),
            b"c 5\nusers".to_vec()
        );
        assert_eq!(ClearScope::All.message(), b"F\n".to_vec());
    }

    #[tokio::test(flavor = "current_thread")]
    async fn route_clear_queues_during_transfer_then_forwards_then_stops() {
        // Issue #106: the three phases of a handoff slot.
        let after_ring = Arc::new(HashRing::new(vec![
            "ready-node".to_string(),
            "joiner-0".to_string(),
        ]));
        let node_context = NodeContext {
            name: "ready-node".to_string(),
            token: "tk-ready-node".to_string(),
            discovery_addr: "127.0.0.1:0".to_string(),
            active_migration: Arc::new(Mutex::new(Some(ActiveMigration {
                joining_name: "joiner-0".to_string(),
                joining_addr: "127.0.0.1:9".to_string(),
                joining_token: "tok-joiner-0".to_string(),
                after_ring,
                replication: 2,
                completed_at: None,
                forwarding_grace: Duration::ZERO,
                acked_entries: Some(0),
                abort_requested: Arc::new(AtomicBool::new(false)),
                marked_keys: Vec::new(),
                confirmed: false,
                pre_completion_ring: None,
                pending_clears: Vec::new(),
                forward_connection: Arc::new(AsyncMutex::new(None)),
            }))),
            known_ring: Arc::new(Mutex::new(None)),
            auth_secret: None,
            tls_connector: None,
            request_tx: mpsc::channel(1).0,
            leaving: Arc::new(Mutex::new(None)),
            active_rereplication: Arc::new(Mutex::new(None)),
            rereplication_tx: mpsc::channel(1).0,
            shutdown_rx: watch::channel(false).1,
        };
        let scope = ClearScope::Namespace(Bytes::from_static(b"users"));

        // Transfer running: queued on the slot, in order.
        assert!(matches!(
            route_clear(&node_context, &scope),
            ClearRoute::Queued
        ));
        assert!(matches!(
            route_clear(&node_context, &ClearScope::All),
            ClearRoute::Queued
        ));
        {
            let slot = node_context.active_migration.lock().unwrap();
            assert_eq!(
                slot.as_ref().unwrap().pending_clears,
                vec![scope.clone(), ClearScope::All]
            );
        }

        // Completed, forwarding window open: forwarded to the joiner.
        {
            let mut slot = node_context.active_migration.lock().unwrap();
            let active = slot.as_mut().unwrap();
            active.completed_at = Some(Instant::now());
            active.forwarding_grace = Duration::from_secs(60);
        }
        match route_clear(&node_context, &scope) {
            ClearRoute::Forward(target) => assert_eq!(target.addr, "127.0.0.1:9"),
            _ => panic!("a completed handoff must forward clears"),
        }

        // Window closed: nothing to replay.
        {
            let mut slot = node_context.active_migration.lock().unwrap();
            slot.as_mut().unwrap().forwarding_grace = Duration::ZERO;
        }
        assert!(matches!(
            route_clear(&node_context, &scope),
            ClearRoute::None
        ));

        // No handoff at all.
        *node_context.active_migration.lock().unwrap() = None;
        assert!(matches!(
            route_clear(&node_context, &scope),
            ClearRoute::None
        ));
    }

    type RecordedFrames = Arc<std::sync::Mutex<Vec<Vec<u8>>>>;

    /// A fake joining node that records every frame it receives (as
    /// separate frames, split by the protocol's own length fields) and
    /// acks each with the reply its command expects.
    fn spawn_recording_joiner(
        listener: TcpListener,
    ) -> (RecordedFrames, tokio::task::JoinHandle<()>) {
        let frames: RecordedFrames = Arc::new(std::sync::Mutex::new(Vec::new()));
        let recorded = Arc::clone(&frames);
        let task = tokio::spawn(async move {
            let (mut connection, _) = listener.accept().await.unwrap();
            let mut buffer = BytesMut::new();
            loop {
                let Some(header_end) = buffer.iter().position(|byte| *byte == b'\n') else {
                    let mut chunk = [0u8; 1024];
                    let bytes_read = connection.read(&mut chunk).await.unwrap();
                    if bytes_read == 0 {
                        return;
                    }
                    buffer.extend_from_slice(&chunk[..bytes_read]);
                    continue;
                };
                let header = String::from_utf8(buffer[..header_end].to_vec()).unwrap();
                // Issue #266: `U`'s optional trailing `A` (put-if-absent)
                // isn't a length field — skip it rather than fail to
                // parse it as one.
                let fields: Vec<usize> = header
                    .split(' ')
                    .skip(1)
                    .filter(|field| *field != "A")
                    .map(|field| field.parse().unwrap())
                    .collect();
                let (body_length, ack): (usize, &[u8]) = match header.as_bytes()[0] {
                    b'S' => (fields[0] + fields[1], b"S\n"),
                    b's' => (fields[0] + fields[1] + fields[2], b"S\n"),
                    // Issue #266: `run_migration`'s own bulk transfer,
                    // sent as a put-if-absent handoff. Issue #295: the
                    // body now leads with `<token-len>` bytes of token
                    // too (`fields[3]`).
                    b'U' => (fields[0] + fields[1] + fields[2] + fields[3], b"S\n"),
                    b'c' => (fields[0], b"C\n"),
                    b'F' => (0, b"C\n"),
                    other => panic!("unexpected frame {other}"),
                };
                let frame_end = header_end + 1 + body_length;
                if buffer.len() < frame_end {
                    let mut chunk = [0u8; 1024];
                    let bytes_read = connection.read(&mut chunk).await.unwrap();
                    assert!(bytes_read > 0, "frame cut short");
                    buffer.extend_from_slice(&chunk[..bytes_read]);
                    continue;
                }
                recorded
                    .lock()
                    .unwrap()
                    .push(buffer.split_to(frame_end).to_vec());
                connection.write_all(ack).await.unwrap();
            }
        });
        (frames, task)
    }

    #[tokio::test(flavor = "current_thread")]
    async fn a_clear_queued_during_the_transfer_is_replayed_on_the_joiner_in_order() {
        // Issue #106: a `c` that arrived while this node was still moving
        // keys is replayed on the transfer stream before the next key,
        // so a key this node no longer has can't survive on the joiner —
        // and keys sent after it arrive after it.
        let (request_tx, request_rx) = mpsc::channel(1);
        let cache_task = tokio::spawn(run_cache(request_rx, MAX_CACHE_MEMORY_BYTES, Vec::new()));

        // Every key in one namespace, so the joiner cracks some top-2 and
        // this single-member "cluster" (R=2 over 1 node) sends them all.
        let users = Bytes::from_static(b"users");
        for index in 0..20u8 {
            send_command(
                &request_tx,
                Command::Set {
                    key: Key::new(users.clone(), Bytes::from(format!("k{index}"))),
                    value: Bytes::from_static(b"v"),
                    ttl: None,
                },
            )
            .await;
        }

        let joining_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let joining_addr = joining_listener.local_addr().unwrap().to_string();
        let (frames, joining_task) = spawn_recording_joiner(joining_listener);

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
            leaving: Arc::new(Mutex::new(None)),
            active_rereplication: Arc::new(Mutex::new(None)),
            rereplication_tx: mpsc::channel(1).0,
            shutdown_rx: watch::channel(false).1,
        };

        let joined = vec![("ready-node".to_string(), "127.0.0.1:1".to_string())];
        let (before_ring, after_ring) = migration_rings(&node_context, "joiner-0", &joined);
        let after_ring = Arc::new(after_ring);
        let migration_guard = MigrationGuard::new(
            Arc::clone(&node_context.active_migration),
            "joiner-0".to_string(),
            joining_addr.clone(),
            "tok-joiner-0".to_string(),
            Arc::clone(&after_ring),
            2,
            &joined,
            &node_context.known_ring,
        )
        .unwrap_new();

        let keys = list_keys(&request_tx).await;
        let expected_keys = keys.as_ref().unwrap().len();

        // The clear lands while the slot is reserved but before the
        // transfer loop has sent anything: exactly what a client `c`
        // racing the `M` produces.
        assert!(matches!(
            route_clear(
                &node_context,
                &ClearScope::Namespace(Bytes::from_static(b"users"))
            ),
            ClearRoute::Queued
        ));

        run_migration(
            node_context.clone(),
            "joiner-0".to_string(),
            joining_addr.clone(),
            "tok-joiner-0".to_string(),
            2,
            before_ring,
            after_ring,
            migration_guard,
            keys,
        )
        .await;

        let frames = frames.lock().unwrap().clone();
        assert_eq!(frames[0], b"c 5\nusers".to_vec(), "the clear goes first");
        assert_eq!(frames.len(), 1 + expected_keys);
        // Issue #266: `run_migration`'s own bulk transfer sends a `U`
        // (put-if-absent), not a lowercase `s`.
        assert!(frames[1..].iter().all(|frame| frame.starts_with(b"U 5 ")));

        // Once completed, a clear forwards like a concurrent write.
        assert!(matches!(
            route_clear(&node_context, &ClearScope::All),
            ClearRoute::Forward(_)
        ));
        assert!(
            node_context
                .active_migration
                .lock()
                .unwrap()
                .as_ref()
                .unwrap()
                .pending_clears
                .is_empty()
        );

        discovery_task.await.unwrap();
        joining_task.abort();
        drop(node_context);
        drop(request_tx);
        cache_task.await.unwrap();
    }

    #[tokio::test(flavor = "current_thread")]
    async fn handoff_set_bypasses_the_wrong_node_check() {
        // Issue #124: a decommissioning peer's `U` must store even though
        // this node doesn't own the key yet; a plain `S` for the same
        // key answers `W`.
        let (mut client, server) = tcp_pair().await;
        let (request_tx, request_rx) = mpsc::channel(4);
        let (_shutdown_tx, shutdown_rx) = watch::channel(false);
        let cache_task = tokio::spawn(run_cache(request_rx, MAX_CACHE_MEMORY_BYTES, Vec::new()));

        // A membership view where some other node owns everything.
        let node_context = NodeContext {
            name: "self".to_string(),
            token: "tk-self".to_string(),
            discovery_addr: "127.0.0.1:0".to_string(),
            active_migration: Arc::new(Mutex::new(None)),
            known_ring: Arc::new(Mutex::new(Some(Arc::new(Membership {
                ring: Arc::new(HashRing::new(vec!["other".to_string()])),
                replication: 1,
            })))),
            auth_secret: None,
            tls_connector: None,
            request_tx: request_tx.clone(),
            leaving: Arc::new(Mutex::new(None)),
            active_rereplication: Arc::new(Mutex::new(None)),
            rereplication_tx: mpsc::channel(1).0,
            shutdown_rx: watch::channel(false).1,
        };

        let connection_task = tokio::spawn(handle_connection(
            ServerStream::Plain(server),
            test_client_addr(),
            request_tx.clone(),
            ConnectionConfig {
                idle_timeout: IDLE_TIMEOUT,
                auth_secret: None,
                tls_acceptor: None,
                node_context: Some(node_context),
                migration_tx: mpsc::channel(1).0,
                forward_tx: mpsc::channel(1).0,
            },
            shutdown_rx,
        ));

        // Issue #295: `U`'s token must match this node's own
        // (`node_context.token`, "tk-self") to pass the new
        // authorization check before the wrong-node bypass is even
        // reached.
        client
            .write_all(b"S 4 5\nnameAliceU 0 4 5 7\ntk-selfnameAliceG 4\nname")
            .await
            .unwrap();
        client.shutdown().await.unwrap();

        // S → W (not owner), U → S (stored anyway), G → W (reads still
        // follow the routing discipline).
        let expected = b"W\nS\nW\n";
        let mut response = vec![0u8; expected.len()];
        client.read_exact(&mut response).await.unwrap();
        assert_eq!(response, expected);

        connection_task.await.unwrap().unwrap();
        drop(request_tx);
        cache_task.await.unwrap();
    }

    #[tokio::test(flavor = "current_thread")]
    async fn handoff_delete_bypasses_the_wrong_node_check() {
        // Issue #124: a decommissioning peer's forwarded delete (`u`)
        // must apply even though this node doesn't own the key yet; a
        // plain `D` for the same key answers `W`.
        let (mut client, server) = tcp_pair().await;
        let (request_tx, request_rx) = mpsc::channel(4);
        let (_shutdown_tx, shutdown_rx) = watch::channel(false);
        let cache_task = tokio::spawn(run_cache(request_rx, MAX_CACHE_MEMORY_BYTES, Vec::new()));

        let node_context = NodeContext {
            name: "self".to_string(),
            token: "tk-self".to_string(),
            discovery_addr: "127.0.0.1:0".to_string(),
            active_migration: Arc::new(Mutex::new(None)),
            known_ring: Arc::new(Mutex::new(Some(Arc::new(Membership {
                ring: Arc::new(HashRing::new(vec!["other".to_string()])),
                replication: 1,
            })))),
            auth_secret: None,
            tls_connector: None,
            request_tx: request_tx.clone(),
            leaving: Arc::new(Mutex::new(None)),
            active_rereplication: Arc::new(Mutex::new(None)),
            rereplication_tx: mpsc::channel(1).0,
            shutdown_rx: watch::channel(false).1,
        };

        let connection_task = tokio::spawn(handle_connection(
            ServerStream::Plain(server),
            test_client_addr(),
            request_tx.clone(),
            ConnectionConfig {
                idle_timeout: IDLE_TIMEOUT,
                auth_secret: None,
                tls_acceptor: None,
                node_context: Some(node_context),
                migration_tx: mpsc::channel(1).0,
                forward_tx: mpsc::channel(1).0,
            },
            shutdown_rx,
        ));

        // Store via U first so there is something to delete, then:
        // D → W (not owner), u → D (deleted anyway), u again → N (gone).
        // Issue #295: both `U` and `u` must carry this node's own token
        // ("tk-self") to pass authorization.
        client
            .write_all(
                b"U 0 4 5 7\ntk-selfnameAliceD 4\nnameu 0 4 7\ntk-selfnameu 0 4 7\ntk-selfname",
            )
            .await
            .unwrap();
        client.shutdown().await.unwrap();

        let expected = b"S\nW\nD\nN\n";
        let mut response = vec![0u8; expected.len()];
        client.read_exact(&mut response).await.unwrap();
        assert_eq!(response, expected);

        connection_task.await.unwrap().unwrap();
        drop(request_tx);
        cache_task.await.unwrap();
    }

    #[tokio::test(flavor = "current_thread")]
    async fn handoff_set_with_the_wrong_token_is_rejected_and_stores_nothing() {
        // Issue #295: mirrors `migrate_command_with_the_wrong_token_is_
        // rejected_and_transfers_nothing` for `U`. `U`/`u` skip the
        // wrong-node check by design (see their own doc comments), so
        // without a membership-token check any shared-secret client could
        // forge one to write a key here regardless of ring ownership. A
        // wrong token must be rejected outright: nothing stored, and —
        // since this node is itself mid-join-handoff for the key, which
        // would otherwise also forward it to the joiner — nothing
        // forwarded either.
        let (request_tx, request_rx) = mpsc::channel(4);
        let cache_task = tokio::spawn(run_cache(request_rx, MAX_CACHE_MEMORY_BYTES, Vec::new()));

        // A fake joining node: nothing must ever connect to it — a
        // rejected `U` must never reach `migration_target_for`'s forward.
        let joiner_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let joiner_addr = joiner_listener.local_addr().unwrap().to_string();
        let joiner_connected = Arc::new(AtomicBool::new(false));
        let joiner_flag = Arc::clone(&joiner_connected);
        let joiner_task = tokio::spawn(async move {
            if joiner_listener.accept().await.is_ok() {
                joiner_flag.store(true, Ordering::SeqCst);
            }
        });

        // R=2 over {test-node, joiner-0} — every key is owned by both, so
        // `migration_target_for` would forward regardless of which key is
        // used, if a rejected `U` ever reached that far.
        let mut active_migration = test_active_migration(Some(Instant::now()));
        active_migration.joining_addr = joiner_addr;
        let node_context = NodeContext {
            name: "test-node".to_string(),
            token: "tk-self".to_string(),
            discovery_addr: "127.0.0.1:0".to_string(),
            active_migration: Arc::new(Mutex::new(Some(active_migration))),
            known_ring: Arc::new(Mutex::new(None)),
            auth_secret: None,
            tls_connector: None,
            request_tx: request_tx.clone(),
            leaving: Arc::new(Mutex::new(None)),
            active_rereplication: Arc::new(Mutex::new(None)),
            rereplication_tx: mpsc::channel(1).0,
            shutdown_rx: watch::channel(false).1,
        };

        let (mut client, server) = tcp_pair().await;
        let (_shutdown_tx, shutdown_rx) = watch::channel(false);
        let (forward_tx, mut forward_rx) = mpsc::channel::<MigrationTask>(4);
        let forward_relay = tokio::spawn(async move {
            while let Some(task) = forward_rx.recv().await {
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
                node_context: Some(node_context),
                migration_tx: mpsc::channel(1).0,
                forward_tx,
            },
            shutdown_rx,
        ));

        // Wrong token ("tk-not-mine" instead of "tk-self").
        client
            .write_all(b"U 0 4 5 11\ntk-not-minenameAlice")
            .await
            .unwrap();
        client.shutdown().await.unwrap();

        let mut response = Vec::new();
        client.read_to_end(&mut response).await.unwrap();
        assert_eq!(response, b"R\n");

        // The connection closes on rejection — same as `M`'s.
        assert!(connection_task.await.unwrap().is_err());

        drop(forward_relay);
        // Give any (wrongly) spawned forward a chance to dial out.
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(
            !joiner_connected.load(Ordering::SeqCst),
            "a rejected U must never forward to the joiner"
        );
        joiner_task.abort();

        assert_eq!(
            send_command(&request_tx, Command::Get { key: key(b"name") }).await,
            Response::NotFound
        );

        drop(request_tx);
        cache_task.await.unwrap();
    }

    #[tokio::test(flavor = "current_thread")]
    async fn handoff_delete_with_the_wrong_token_is_rejected_and_deletes_nothing() {
        // Issue #295: same proof as `handoff_set_with_the_wrong_token_is_
        // rejected_and_stores_nothing`, for `u`.
        let (request_tx, request_rx) = mpsc::channel(4);
        let cache_task = tokio::spawn(run_cache(request_rx, MAX_CACHE_MEMORY_BYTES, Vec::new()));
        send_command(
            &request_tx,
            Command::Set {
                key: key(b"name"),
                value: Bytes::from_static(b"Alice"),
                ttl: None,
            },
        )
        .await;

        let node_context = NodeContext {
            name: "self".to_string(),
            token: "tk-self".to_string(),
            discovery_addr: "127.0.0.1:0".to_string(),
            active_migration: Arc::new(Mutex::new(None)),
            known_ring: Arc::new(Mutex::new(None)),
            auth_secret: None,
            tls_connector: None,
            request_tx: request_tx.clone(),
            leaving: Arc::new(Mutex::new(None)),
            active_rereplication: Arc::new(Mutex::new(None)),
            rereplication_tx: mpsc::channel(1).0,
            shutdown_rx: watch::channel(false).1,
        };

        let (mut client, server) = tcp_pair().await;
        let (_shutdown_tx, shutdown_rx) = watch::channel(false);
        let connection_task = tokio::spawn(handle_connection(
            ServerStream::Plain(server),
            test_client_addr(),
            request_tx.clone(),
            ConnectionConfig {
                idle_timeout: IDLE_TIMEOUT,
                auth_secret: None,
                tls_acceptor: None,
                node_context: Some(node_context),
                migration_tx: mpsc::channel(1).0,
                forward_tx: mpsc::channel(1).0,
            },
            shutdown_rx,
        ));

        // Wrong token ("tk-not-mine" instead of "tk-self").
        client
            .write_all(b"u 0 4 11\ntk-not-minename")
            .await
            .unwrap();
        client.shutdown().await.unwrap();

        let mut response = Vec::new();
        client.read_to_end(&mut response).await.unwrap();
        assert_eq!(response, b"R\n");
        assert!(connection_task.await.unwrap().is_err());

        assert_eq!(
            send_command(&request_tx, Command::Get { key: key(b"name") }).await,
            Response::Value(Bytes::from_static(b"Alice")),
            "the key must survive a rejected u"
        );

        drop(request_tx);
        cache_task.await.unwrap();
    }

    #[tokio::test(flavor = "current_thread")]
    async fn migrate_command_multi_set_forwards_owned_keys_to_the_joining_node() {
        // Issue #176: `MultiSet` (`o`) used to answer `MultiAck` without
        // ever consulting `migration_target_for` — unlike `Set`, a bulk
        // write for a key mid-handoff never reached the joining node, so
        // the value was lost the moment `known_ring` flipped. This drives
        // an `o` for two keys through `handle_connection` with a handoff
        // in flight and checks both land on the joiner as forwarded `S`
        // frames.
        let (request_tx, request_rx) = mpsc::channel(4);
        let cache_task = tokio::spawn(run_cache(request_rx, MAX_CACHE_MEMORY_BYTES, Vec::new()));

        let joining_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let joining_addr = joining_listener.local_addr().unwrap().to_string();
        let joining_task = tokio::spawn(async move {
            let (mut connection, _) = joining_listener.accept().await.unwrap();
            let mut received = Vec::new();
            // Two forwarded SETs, sequential on the one shared connection
            // (mirrors `forwarded_writes_to_a_joining_node_reuse_one_connection`).
            for _ in 0..2 {
                let mut buffer = [0u8; 256];
                let bytes_read = connection.read(&mut buffer).await.unwrap();
                assert!(bytes_read > 0);
                received.extend_from_slice(&buffer[..bytes_read]);
                connection.write_all(b"S\n").await.unwrap();
            }
            received
        });

        // Two members, R=2: the joiner is in every key's top-R
        // regardless of hash, so both keys must forward.
        let after_ring = Arc::new(HashRing::new(vec![
            "ready-node".to_string(),
            "joiner-0".to_string(),
        ]));

        let node_context = NodeContext {
            name: "ready-node".to_string(),
            token: "tk-ready-node".to_string(),
            discovery_addr: "127.0.0.1:0".to_string(),
            // The transfer is still running (completed_at: None) — same
            // as `migration_target_for`'s "still moving keys" case, the
            // simplest state that keeps forwarding open.
            active_migration: Arc::new(Mutex::new(Some(ActiveMigration {
                joining_name: "joiner-0".to_string(),
                joining_addr: joining_addr.clone(),
                joining_token: "tok-joiner-0".to_string(),
                after_ring,
                replication: 2,
                completed_at: None,
                forwarding_grace: Duration::from_secs(60),
                acked_entries: None,
                abort_requested: Arc::new(AtomicBool::new(false)),
                marked_keys: Vec::new(),
                confirmed: false,
                pre_completion_ring: None,
                pending_clears: Vec::new(),
                forward_connection: Arc::new(AsyncMutex::new(None)),
            }))),
            known_ring: Arc::new(Mutex::new(None)),
            auth_secret: None,
            tls_connector: None,
            request_tx: request_tx.clone(),
            leaving: Arc::new(Mutex::new(None)),
            active_rereplication: Arc::new(Mutex::new(None)),
            rereplication_tx: mpsc::channel(1).0,
            shutdown_rx: watch::channel(false).1,
        };

        let (mut client, server) = tcp_pair().await;
        let (_shutdown_tx, shutdown_rx) = watch::channel(false);

        // Stands in for `run`'s own loop draining `forward_tx` — see
        // `ConnectionConfig::forward_tx` (issue #219: per-write forwards,
        // which is what `MultiSet` triggers here, no longer share
        // `migration_tx` with the bulk-migration task; compare
        // `migrate_command_transfers_matching_keys_and_reports_completion`,
        // which relays `migration_tx` instead).
        let (forward_tx, mut forward_rx) = mpsc::channel::<MigrationTask>(4);
        let forward_relay = tokio::spawn(async move {
            while let Some(task) = forward_rx.recv().await {
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
                node_context: Some(node_context),
                migration_tx: mpsc::channel(1).0,
                forward_tx,
            },
            shutdown_rx,
        ));

        client
            .write_all(b"o 0 2 5 2 5 2\nkey-av1key-bv2")
            .await
            .unwrap();
        client.shutdown().await.unwrap();

        let expected = b"O 2 S S\n";
        let mut response = vec![0u8; expected.len()];
        client.read_exact(&mut response).await.unwrap();
        assert_eq!(response, expected);

        connection_task.await.unwrap().unwrap();
        drop(request_tx);
        cache_task.await.unwrap();
        drop(forward_relay);

        let received = joining_task.await.unwrap();
        assert_eq!(
            received,
            b"S 5 2\nkey-av1S 5 2\nkey-bv2".to_vec(),
            "both owned keys from the MultiSet must be forwarded to the joining node"
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn decommission_multi_set_forwards_owned_keys_to_the_entrant() {
        // Issue #176: the decommission-drain mirror of the test above —
        // `leave_target_for` must see a `MultiSet`'s owned keys too, or a
        // bulk write racing a drain-out is lost once the post-leave
        // roster publishes.
        let (request_tx, request_rx) = mpsc::channel(4);
        let cache_task = tokio::spawn(run_cache(request_rx, MAX_CACHE_MEMORY_BYTES, Vec::new()));

        let peer_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let peer_addr = peer_listener.local_addr().unwrap().to_string();
        let (frames, peer_task) = spawn_recording_peer(peer_listener);

        // R=1 over {leaver, peer}: whichever keys the leaver owns before
        // leaving move to the peer once leaver drops out — the only
        // member left.
        let before_ring = Arc::new(HashRing::new(vec![
            "leaver".to_string(),
            "peer".to_string(),
        ]));
        let after_ring = Arc::new(HashRing::new(vec!["peer".to_string()]));

        // Two keys the "leaver" owns pre-leave, found the same way
        // `a_decommission_hands_off_owned_keys_and_leaves` samples them —
        // consistent hashing means not every name qualifies.
        let mut owned_keys = Vec::new();
        let mut index = 0u32;
        while owned_keys.len() < 2 {
            let name = format!("key-{index}");
            if before_ring.is_owner(&key(name.as_bytes()), "leaver", 1) {
                owned_keys.push(name);
            }
            index += 1;
            assert!(index < 1000, "expected at least two owned keys in range");
        }

        let mut addresses = HashMap::new();
        addresses.insert("peer".to_string(), peer_addr);
        let mut tokens = HashMap::new();
        tokens.insert("peer".to_string(), "tok-peer".to_string());

        let node_context = NodeContext {
            name: "leaver".to_string(),
            token: "tk-leaver".to_string(),
            discovery_addr: "127.0.0.1:0".to_string(),
            active_migration: Arc::new(Mutex::new(None)),
            known_ring: Arc::new(Mutex::new(None)),
            auth_secret: None,
            tls_connector: None,
            request_tx: request_tx.clone(),
            leaving: Arc::new(Mutex::new(Some(LeaveState {
                before_ring,
                after_ring,
                replication: 1,
                addresses,
                tokens,
                connections: Mutex::new(HashMap::new()),
            }))),
            active_rereplication: Arc::new(Mutex::new(None)),
            rereplication_tx: mpsc::channel(1).0,
            shutdown_rx: watch::channel(false).1,
        };

        let (mut client, server) = tcp_pair().await;
        let (_shutdown_tx, shutdown_rx) = watch::channel(false);

        // Stands in for `run`'s own loop draining `forward_tx` — see
        // issue #219 (`ConnectionConfig::forward_tx`); per-write forwards
        // like this decommission-drain `MultiSet` mirror no longer share
        // `migration_tx` with the bulk-migration task.
        let (forward_tx, mut forward_rx) = mpsc::channel::<MigrationTask>(4);
        let forward_relay = tokio::spawn(async move {
            while let Some(task) = forward_rx.recv().await {
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
                node_context: Some(node_context),
                migration_tx: mpsc::channel(1).0,
                forward_tx,
            },
            shutdown_rx,
        ));

        let name_a = &owned_keys[0];
        let name_b = &owned_keys[1];
        let request = format!(
            "o 0 2 {} 2 {} 2\n{name_a}v1{name_b}v2",
            name_a.len(),
            name_b.len()
        );
        client.write_all(request.as_bytes()).await.unwrap();
        client.shutdown().await.unwrap();

        let expected = b"O 2 S S\n";
        let mut response = vec![0u8; expected.len()];
        client.read_exact(&mut response).await.unwrap();
        assert_eq!(response, expected);

        connection_task.await.unwrap().unwrap();
        drop(request_tx);
        cache_task.await.unwrap();
        drop(forward_relay);
        peer_task.abort();

        let frames = frames.lock().unwrap().clone();
        assert_eq!(
            frames.len(),
            2,
            "both owned keys must forward to the entrant"
        );
        // Issue #295: the entrant's own token ("tok-peer") now leads the
        // body.
        let expected_a = format!("U 0 {} 2 8\ntok-peer{name_a}v1", name_a.len()).into_bytes();
        let expected_b = format!("U 0 {} 2 8\ntok-peer{name_b}v2", name_b.len()).into_bytes();
        assert!(frames.contains(&expected_a));
        assert!(frames.contains(&expected_b));
    }

    #[test]
    fn entrant_for_promotes_exactly_the_new_owner() {
        // Issue #124: removing self from the ranking promotes exactly
        // the pre-leave rank-R+1 node, for owned keys only.
        let names = vec![
            "node-a".to_string(),
            "node-b".to_string(),
            "node-c".to_string(),
        ];
        let before = Arc::new(HashRing::new(names.clone()));
        let after = Arc::new(HashRing::new(
            names
                .iter()
                .filter(|name| *name != "node-a")
                .cloned()
                .collect(),
        ));
        let leave = LeaveState {
            before_ring: Arc::clone(&before),
            after_ring: Arc::clone(&after),
            replication: 2,
            addresses: HashMap::new(),
            tokens: HashMap::new(),
            connections: Mutex::new(HashMap::new()),
        };

        let mut promoted = 0;
        for index in 0..200 {
            let key = key(format!("key-{index}").as_bytes());
            let owned = before.is_owner(&key, "node-a", 2);
            match leave.entrant_for(&key, "node-a") {
                Some(entrant) => {
                    promoted += 1;
                    assert!(owned, "entrant only exists for owned keys");
                    // The entrant is a new owner and wasn't one before.
                    assert!(after.is_owner(&key, &entrant, 2));
                    assert!(!before.is_owner(&key, &entrant, 2));
                }
                None => assert!(!owned, "every owned key must have an entrant here"),
            }
        }
        assert!(promoted > 0, "the sample must exercise owned keys");
    }

    #[test]
    fn rereplication_targets_are_new_owners_only_for_the_elected_sender() {
        // Issue #266: a 4-member ring loses "node-b" (an eviction, or a
        // leave this node didn't hand off for). For every key, checked
        // from "node-a"'s point of view against an independent reference
        // computation of the same election rule.
        let names = vec![
            "node-a".to_string(),
            "node-b".to_string(),
            "node-c".to_string(),
            "node-d".to_string(),
        ];
        let before = HashRing::new(names.clone());
        let after = HashRing::new(
            names
                .iter()
                .filter(|name| *name != "node-b")
                .cloned()
                .collect(),
        );
        let replication = 2;

        let mut nonempty = 0;
        for index in 0..500 {
            let key = key(format!("key-{index}").as_bytes());
            let old_owners = before.owners(&key, replication);
            let owned_before = old_owners.contains(&"node-a");

            // Reference: the highest-ranked old owner that survived.
            let elected_sender = old_owners
                .iter()
                .find(|owner| after.nodes().iter().any(|node| node == *owner))
                .copied();

            let targets = rereplication_targets(&before, &after, &key, replication, "node-a");

            if !owned_before || elected_sender != Some("node-a") {
                assert!(
                    targets.is_empty(),
                    "key-{index}: expected no targets (owned={owned_before}, \
                     sender={elected_sender:?}), got {targets:?}"
                );
                continue;
            }

            let new_owners = after.owners(&key, replication);
            let expected: Vec<String> = new_owners
                .iter()
                .filter(|owner| !old_owners.contains(owner))
                .map(|owner| owner.to_string())
                .collect();
            assert_eq!(targets, expected, "key-{index}");
            for target in &targets {
                assert!(after.is_owner(&key, target, replication));
                assert!(!before.is_owner(&key, target, replication));
                nonempty += 1;
            }
        }
        assert!(nonempty > 0, "the sample must exercise at least one send");
    }

    #[test]
    fn rereplication_targets_promotes_the_next_ranked_survivor_when_the_top_owner_is_evicted() {
        // Issue #266 regression guard: sender election must skip the
        // evicted node's own rank and elect the next-ranked *surviving*
        // old owner — not just `old_owners.first()` (the rule a join
        // handoff uses, where the joiner never displaces the sender).
        // Finds a key whose pre-eviction top owner is exactly the node
        // being evicted and "node-a" is next, then checks "node-a" is
        // still elected sender despite not being rank 1 beforehand.
        let names = vec![
            "node-a".to_string(),
            "node-b".to_string(),
            "node-c".to_string(),
        ];
        let before = HashRing::new(names.clone());
        let after = HashRing::new(
            names
                .iter()
                .filter(|name| *name != "node-b")
                .cloned()
                .collect(),
        );
        let replication = 2;

        let mut found = false;
        for index in 0..500 {
            let key = key(format!("key-{index}").as_bytes());
            let old_owners = before.owners(&key, replication);
            if old_owners.first() != Some(&"node-b") || !old_owners.contains(&"node-a") {
                continue;
            }
            found = true;

            let targets = rereplication_targets(&before, &after, &key, replication, "node-a");
            assert!(
                !targets.is_empty(),
                "key-{index}: node-a should have been elected sender after node-b (rank 1) \
                 was evicted"
            );
            break;
        }
        assert!(
            found,
            "the sample must contain a key with node-b ranked above node-a"
        );
    }

    #[test]
    fn migration_rings_deduplicates_a_repeated_name_in_the_joined_roster() {
        // Regression (issue #328): `joined` is a snapshot of a live
        // discovery roster, with no guarantee it's free of duplicate
        // names — unlike `adopt_membership`'s membership list, which is
        // `sort_unstable` + `dedup`ed before becoming a `HashRing`. A
        // duplicate silently broke `HashRing::is_owner`/`owners`
        // agreement (see their doc comments), which is exactly what this
        // function's before/after rings are used to decide handoff roles
        // from.
        let node_context = NodeContext {
            name: "self".to_string(),
            token: "tok-self".to_string(),
            discovery_addr: "127.0.0.1:1".to_string(),
            active_migration: Arc::new(Mutex::new(None)),
            known_ring: Arc::new(Mutex::new(None)),
            auth_secret: None,
            tls_connector: None,
            request_tx: mpsc::channel(1).0,
            leaving: Arc::new(Mutex::new(None)),
            active_rereplication: Arc::new(Mutex::new(None)),
            rereplication_tx: mpsc::channel(1).0,
            shutdown_rx: watch::channel(false).1,
        };

        let joined = vec![
            ("node-b".to_string(), "127.0.0.1:2".to_string()),
            ("node-b".to_string(), "127.0.0.1:2".to_string()),
            ("self".to_string(), "127.0.0.1:1".to_string()),
        ];
        let (before_ring, after_ring) = migration_rings(&node_context, "joiner", &joined);

        let mut before_nodes = before_ring.nodes().to_vec();
        before_nodes.sort_unstable();
        assert_eq!(
            before_nodes,
            vec!["node-b".to_string(), "self".to_string()],
            "the repeated \"node-b\" entry must collapse to one member"
        );

        let mut after_nodes = after_ring.nodes().to_vec();
        after_nodes.sort_unstable();
        assert_eq!(
            after_nodes,
            vec![
                "joiner".to_string(),
                "node-b".to_string(),
                "self".to_string()
            ]
        );

        // With the duplicate collapsed, `owners`/`is_owner` must agree
        // for every member across every replication factor.
        let k = key(b"some-key");
        for ring in [&before_ring, &after_ring] {
            for replicas in 0..=ring.nodes().len() {
                let owners = ring.owners(&k, replicas);
                for name in ring.nodes() {
                    assert_eq!(
                        owners.contains(&name.as_str()),
                        ring.is_owner(&k, name, replicas),
                        "name={name} replicas={replicas} owners={owners:?}"
                    );
                }
            }
        }
    }

    #[test]
    fn ring_dropped_a_member_detects_a_dead_member_absent_from_after() {
        // Issue #266: `run_migration`'s own join-flip trigger compares
        // this node's *last* belief (which may still list a member
        // discovery already evicted — a restart+rejoin outracing this
        // node's next heartbeat ack) against the post-join ring, not the
        // `M`'s own before_ring (which never had a chance to see that
        // eviction at all — it's built from discovery's *current*,
        // already-post-eviction roster).
        let before = HashRing::new(vec![
            "dead".to_string(),
            "node-x".to_string(),
            "node-y".to_string(),
        ]);

        // A pure join (no drop): the joiner is a strict superset.
        let after_pure_join = HashRing::new(vec![
            "dead".to_string(),
            "node-x".to_string(),
            "node-y".to_string(),
            "joiner".to_string(),
        ]);
        assert!(!ring_dropped_a_member(&before, &after_pure_join));

        // The eviction was folded into the same flip as the join: "dead"
        // is gone, "joiner" is new.
        let after_join_and_eviction = HashRing::new(vec![
            "node-x".to_string(),
            "node-y".to_string(),
            "joiner".to_string(),
        ]);
        assert!(ring_dropped_a_member(&before, &after_join_and_eviction));

        // Identical membership: no change either way.
        assert!(!ring_dropped_a_member(&before, &before));
    }

    #[test]
    fn a_classify_decommission_key_filters_ownership_before_deadline() {
        // Issue #233: a key this node never owned must classify as
        // `NotOwned` even once the deadline has passed — not
        // `DeadlinePassed` — otherwise `left_behind` counts keys that
        // were never this node's handoff to miss.
        let before = HashRing::new(vec!["leaver".to_string(), "peer".to_string()]);
        let owned_key = (0..200u32)
            .map(|index| key(format!("key-{index}").as_bytes()))
            .find(|key| before.is_owner(key, "leaver", 1))
            .expect("the sample must contain an owned key");
        let unowned_key = (0..200u32)
            .map(|index| key(format!("key-{index}").as_bytes()))
            .find(|key| !before.is_owner(key, "leaver", 1))
            .expect("the sample must contain an unowned key");

        // Not owned: `NotOwned` regardless of the deadline.
        assert_eq!(
            classify_decommission_key(&unowned_key, &before, "leaver", 1, false),
            DecommissionKeyOutcome::NotOwned
        );
        assert_eq!(
            classify_decommission_key(&unowned_key, &before, "leaver", 1, true),
            DecommissionKeyOutcome::NotOwned
        );

        // Owned: `Owned` before the deadline, `DeadlinePassed` after.
        assert_eq!(
            classify_decommission_key(&owned_key, &before, "leaver", 1, false),
            DecommissionKeyOutcome::Owned
        );
        assert_eq!(
            classify_decommission_key(&owned_key, &before, "leaver", 1, true),
            DecommissionKeyOutcome::DeadlinePassed
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn a_decommission_hands_off_owned_keys_and_leaves() {
        // Issue #124 end to end at the unit level: seed keys, run the
        // decommission against a mock peer + mock discovery, and check
        // every owned key arrives at the peer as a `U` and the leave
        // (`V`) reaches discovery.
        let (request_tx, request_rx) = mpsc::channel(16);
        let cache_task = tokio::spawn(run_cache(request_rx, MAX_CACHE_MEMORY_BYTES, Vec::new()));

        for index in 0..20u8 {
            send_command(
                &request_tx,
                Command::Set {
                    key: key(format!("key-{index}").as_bytes()),
                    value: Bytes::from_static(b"v"),
                    ttl: None,
                },
            )
            .await;
        }

        // The peer records every frame (reusing the recording joiner —
        // `U` is in its grammar below).
        let peer_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let peer_addr = peer_listener.local_addr().unwrap().to_string();
        let (frames, peer_task) = spawn_recording_peer(peer_listener);

        // Mock discovery: serves L (self + peer), records V.
        let discovery_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let discovery_addr = discovery_listener.local_addr().unwrap().to_string();
        let (left, discovery_task) =
            spawn_mock_discovery(discovery_listener, peer_addr.clone(), None);

        let node_context = NodeContext {
            name: "leaver".to_string(),
            token: "tk-leaver".to_string(),
            discovery_addr: discovery_addr.clone(),
            active_migration: Arc::new(Mutex::new(None)),
            known_ring: Arc::new(Mutex::new(None)),
            auth_secret: None,
            tls_connector: None,
            request_tx: request_tx.clone(),
            leaving: Arc::new(Mutex::new(None)),
            active_rereplication: Arc::new(Mutex::new(None)),
            rereplication_tx: mpsc::channel(1).0,
            shutdown_rx: watch::channel(false).1,
        };

        // R=1 over {leaver, peer}: every key the leaver owns must move
        // to the peer — the strongest (no-replica) case.
        let before = HashRing::new(vec!["leaver".to_string(), "peer".to_string()]);
        let expected: usize = (0..20u8)
            .filter(|index| before.is_owner(&key(format!("key-{index}").as_bytes()), "leaver", 1))
            .count();
        assert!(expected > 0, "the sample must give the leaver some keys");

        run_decommission(
            node_context.clone(),
            vec![discovery_addr.clone()],
            Duration::from_secs(5),
        )
        .await;

        // Every owned key arrived as a `U` frame...
        let frames = frames.lock().unwrap().clone();
        assert_eq!(frames.len(), expected, "frames: {frames:?}");
        assert!(frames.iter().all(|frame| frame.starts_with(b"U 0 ")));
        // ...the leave reached discovery...
        assert_eq!(*left.lock().unwrap(), vec!["leaver".to_string()]);
        // ...and the leave state is installed for write forwarding.
        assert!(node_context.leaving.lock().unwrap().is_some());

        discovery_task.abort();
        peer_task.abort();
        drop(node_context);
        drop(request_tx);
        cache_task.await.unwrap();
    }

    /// Issue #124 helper: a fake discovery server that serves `L` (the
    /// leaver plus `peer_addr`, R=1), records the name in every `V`, and —
    /// when `secret` is set — insists on the `A` handshake first, acking
    /// it the way a discovery server does (`Od`, not a node's `On`).
    fn spawn_mock_discovery(
        discovery_listener: TcpListener,
        peer_addr: String,
        secret: Option<&'static [u8]>,
    ) -> (
        Arc<std::sync::Mutex<Vec<String>>>,
        tokio::task::JoinHandle<()>,
    ) {
        let left: Arc<std::sync::Mutex<Vec<String>>> = Arc::new(std::sync::Mutex::new(Vec::new()));
        let left_record = Arc::clone(&left);
        let peer_addr_for_l = peer_addr;
        let discovery_task = tokio::spawn(async move {
            loop {
                let Ok((mut stream, _)) = discovery_listener.accept().await else {
                    return;
                };
                let left_record = Arc::clone(&left_record);
                let peer_addr = peer_addr_for_l.clone();
                tokio::spawn(async move {
                    let mut buf = Vec::new();
                    let mut chunk = [0u8; 1024];
                    let mut authenticated = secret.is_none();
                    loop {
                        let Ok(bytes_read) = stream.read(&mut chunk).await else {
                            return;
                        };
                        if bytes_read == 0 {
                            return;
                        }
                        buf.extend_from_slice(&chunk[..bytes_read]);
                        let Some(position) = buf.iter().position(|byte| *byte == b'\n') else {
                            continue;
                        };
                        let line: Vec<u8> = buf.drain(..=position).collect();
                        let line = String::from_utf8_lossy(&line[..line.len() - 1]).into_owned();
                        if let Some(length) = line.strip_prefix("A ") {
                            let need: usize = length.parse().unwrap();
                            while buf.len() < need {
                                let Ok(bytes_read) = stream.read(&mut chunk).await else {
                                    return;
                                };
                                if bytes_read == 0 {
                                    return;
                                }
                                buf.extend_from_slice(&chunk[..bytes_read]);
                            }
                            let presented: Vec<u8> = buf.drain(..need).collect();
                            if Some(presented.as_slice()) == secret {
                                authenticated = true;
                                let _ = stream.write_all(b"Od\n").await;
                            } else {
                                let _ = stream.write_all(b"Ed\n").await;
                                return;
                            }
                            continue;
                        }
                        assert!(authenticated, "command before auth: {line:?}");
                        if line == "L" {
                            let entries = [("leaver", "127.0.0.1:1"), ("peer", peer_addr.as_str())];
                            let mut response = format!("N {} 1\n", entries.len()).into_bytes();
                            for (name, addr) in entries {
                                response.extend_from_slice(
                                    format!("{} {}\n{name}{addr}\n", name.len(), addr.len())
                                        .as_bytes(),
                                );
                            }
                            let _ = stream.write_all(&response).await;
                        } else if let Some(rest) = line.strip_prefix("T ") {
                            // Issue #295: `fetch_roster_once`'s
                            // self-authenticated roster+token fetch — this
                            // fake doesn't bother validating the presented
                            // name/token (nothing to check it against
                            // here), just consumes the body and answers
                            // with the roster, tokens included.
                            let lengths: Vec<usize> = rest
                                .split(' ')
                                .map(|field| field.parse().unwrap())
                                .collect();
                            let need = lengths[0] + lengths[1];
                            while buf.len() < need {
                                let Ok(bytes_read) = stream.read(&mut chunk).await else {
                                    return;
                                };
                                if bytes_read == 0 {
                                    return;
                                }
                                buf.extend_from_slice(&chunk[..bytes_read]);
                            }
                            let _: Vec<u8> = buf.drain(..need).collect();
                            let entries = [
                                ("leaver", "127.0.0.1:1", "tok-leaver"),
                                ("peer", peer_addr.as_str(), "tok-peer"),
                            ];
                            let mut response = format!("N {} 1\n", entries.len()).into_bytes();
                            for (name, addr, token) in entries {
                                response.extend_from_slice(
                                    format!(
                                        "{} {} {}\n{name}{addr}{token}\n",
                                        name.len(),
                                        addr.len(),
                                        token.len()
                                    )
                                    .as_bytes(),
                                );
                            }
                            let _ = stream.write_all(&response).await;
                        } else if line.starts_with("V ") {
                            let lengths: Vec<usize> = line
                                .split(' ')
                                .skip(1)
                                .map(|field| field.parse().unwrap())
                                .collect();
                            let need = lengths[0] + lengths[1];
                            while buf.len() < need {
                                let Ok(bytes_read) = stream.read(&mut chunk).await else {
                                    return;
                                };
                                if bytes_read == 0 {
                                    return;
                                }
                                buf.extend_from_slice(&chunk[..bytes_read]);
                            }
                            let body: Vec<u8> = buf.drain(..need).collect();
                            left_record
                                .lock()
                                .unwrap()
                                .push(String::from_utf8_lossy(&body[..lengths[0]]).into_owned());
                            let _ = stream.write_all(b"R\n").await;
                        }
                    }
                });
            }
        });
        (left, discovery_task)
    }

    #[tokio::test(flavor = "current_thread")]
    async fn a_decommission_authenticates_to_discovery_with_discoverys_own_ack() {
        // Found on ECS (issue #167 verification): with
        // NANOCACHED_AUTH_SECRET set on both binaries the decommission's
        // roster fetch and leave notification dialled discovery expecting
        // a node's `On` ack, got `Od`, and gave up on the handoff — every
        // scale-in lost the leaver's entries. Same choreography as the
        // test above, with both peers demanding the handshake.
        let (request_tx, request_rx) = mpsc::channel(16);
        let cache_task = tokio::spawn(run_cache(request_rx, MAX_CACHE_MEMORY_BYTES, Vec::new()));
        for index in 0..20u8 {
            send_command(
                &request_tx,
                Command::Set {
                    key: key(format!("key-{index}").as_bytes()),
                    value: Bytes::from_static(b"v"),
                    ttl: None,
                },
            )
            .await;
        }

        let peer_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let peer_addr = peer_listener.local_addr().unwrap().to_string();
        let (frames, peer_task) = spawn_recording_peer(peer_listener);

        let discovery_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let discovery_addr = discovery_listener.local_addr().unwrap().to_string();
        let (left, discovery_task) = spawn_mock_discovery(
            discovery_listener,
            peer_addr.clone(),
            Some(b"shared-secret"),
        );

        let node_context = NodeContext {
            name: "leaver".to_string(),
            token: "tk-leaver".to_string(),
            discovery_addr: discovery_addr.clone(),
            active_migration: Arc::new(Mutex::new(None)),
            known_ring: Arc::new(Mutex::new(None)),
            auth_secret: Some(Bytes::from_static(b"shared-secret")),
            tls_connector: None,
            request_tx: request_tx.clone(),
            leaving: Arc::new(Mutex::new(None)),
            active_rereplication: Arc::new(Mutex::new(None)),
            rereplication_tx: mpsc::channel(1).0,
            shutdown_rx: watch::channel(false).1,
        };

        let before = HashRing::new(vec!["leaver".to_string(), "peer".to_string()]);
        let expected: usize = (0..20u8)
            .filter(|index| before.is_owner(&key(format!("key-{index}").as_bytes()), "leaver", 1))
            .count();
        assert!(expected > 0, "the sample must give the leaver some keys");

        run_decommission(
            node_context.clone(),
            vec![discovery_addr.clone()],
            Duration::from_secs(5),
        )
        .await;

        let frames = frames.lock().unwrap().clone();
        assert_eq!(frames.len(), expected, "frames: {frames:?}");
        assert!(frames.iter().all(|frame| frame.starts_with(b"U 0 ")));
        assert_eq!(*left.lock().unwrap(), vec!["leaver".to_string()]);
        assert!(node_context.leaving.lock().unwrap().is_some());

        discovery_task.abort();
        peer_task.abort();
        drop(node_context);
        drop(request_tx);
        cache_task.await.unwrap();
    }

    /// Issue #124 helper: a fake surviving peer recording every `U`
    /// frame it receives and acking `S`.
    fn spawn_recording_peer(
        listener: TcpListener,
    ) -> (RecordedFrames, tokio::task::JoinHandle<()>) {
        let frames: RecordedFrames = Arc::new(std::sync::Mutex::new(Vec::new()));
        let recorded = Arc::clone(&frames);
        let task = tokio::spawn(async move {
            loop {
                let Ok((mut connection, _)) = listener.accept().await else {
                    return;
                };
                let recorded = Arc::clone(&recorded);
                tokio::spawn(async move {
                    let mut buf = BytesMut::new();
                    loop {
                        let Some(header_end) = buf.iter().position(|byte| *byte == b'\n') else {
                            let mut chunk = [0u8; 1024];
                            let Ok(bytes_read) = connection.read(&mut chunk).await else {
                                return;
                            };
                            if bytes_read == 0 {
                                return;
                            }
                            buf.extend_from_slice(&chunk[..bytes_read]);
                            continue;
                        };
                        let header = String::from_utf8(buf[..header_end].to_vec()).unwrap();
                        let fields: Vec<usize> = header
                            .split(' ')
                            .skip(1)
                            .map(|field| field.parse().unwrap())
                            .collect();
                        if header.starts_with("A ") {
                            // Auth handshake: consume the secret, ack as a
                            // node does. Not recorded — the tests count
                            // handoff frames.
                            let frame_end = header_end + 1 + fields[0];
                            while buf.len() < frame_end {
                                let mut chunk = [0u8; 1024];
                                let Ok(bytes_read) = connection.read(&mut chunk).await else {
                                    return;
                                };
                                if bytes_read == 0 {
                                    return;
                                }
                                buf.extend_from_slice(&chunk[..bytes_read]);
                            }
                            let _ = buf.split_to(frame_end);
                            let _ = connection.write_all(b"On\n").await;
                            continue;
                        }
                        assert!(header.starts_with("U "), "unexpected frame {header:?}");
                        // Issue #295: `fields[3]` is `<token-len>`.
                        let body_length = fields[0] + fields[1] + fields[2] + fields[3];
                        let frame_end = header_end + 1 + body_length;
                        while buf.len() < frame_end {
                            let mut chunk = [0u8; 1024];
                            let Ok(bytes_read) = connection.read(&mut chunk).await else {
                                return;
                            };
                            if bytes_read == 0 {
                                return;
                            }
                            buf.extend_from_slice(&chunk[..bytes_read]);
                        }
                        recorded
                            .lock()
                            .unwrap()
                            .push(buf.split_to(frame_end).to_vec());
                        let _ = connection.write_all(b"S\n").await;
                    }
                });
            }
        });
        (frames, task)
    }

    #[tokio::test(flavor = "current_thread")]
    async fn migrate_with_replication_marks_displaced_copies_and_keeps_the_senders() {
        let (request_tx, request_rx) = mpsc::channel(1);
        let cache_task = tokio::spawn(run_cache(request_rx, MAX_CACHE_MEMORY_BYTES, Vec::new()));

        // Chosen (ready-node / other-node / joiner-0, R=2) so both
        // Client-side replication roles land on this node at once:
        //   "key-0": pre-join top-2 = [ready-node, other-node]; joiner-0
        //            enters and displaces other-node — ready-node STAYS
        //            an owner, so it sends the key and keeps its own
        //            copy unmarked.
        //   "key-3": pre-join top-2 = [other-node, ready-node]; joiner-0
        //            enters and displaces ready-node. Issue #266:
        //            ready-node is still an old owner that holds the
        //            key, so it sends it too (every holding old owner
        //            sends now, not just the pre-join primary) — and,
        //            since its own send succeeds, marks its now-dead
        //            copy for the post-handoff sweep.
        send_command(
            &request_tx,
            Command::Set {
                key: key(b"key-0"),
                value: Bytes::from_static(b"primary-copy"),
                ttl: None,
            },
        )
        .await;
        send_command(
            &request_tx,
            Command::Set {
                key: key(b"key-3"),
                value: Bytes::from_static(b"replica-copy"),
                ttl: None,
            },
        )
        .await;

        // Fake joining node: must receive both keys, on the one shared
        // connection (issue #266: ready-node is an old owner of both now).
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
            leaving: Arc::new(Mutex::new(None)),
            active_rereplication: Arc::new(Mutex::new(None)),
            rereplication_tx: mpsc::channel(1).0,
            shutdown_rx: watch::channel(false).1,
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
            "tok-joiner-0".to_string(),
            Arc::clone(&after_ring),
            2,
            &joined,
            &node_context.known_ring,
        )
        .unwrap_new();

        let keys = list_keys(&request_tx).await;

        run_migration(
            node_context.clone(),
            "joiner-0".to_string(),
            joining_addr.clone(),
            "tok-joiner-0".to_string(),
            2,
            before_ring,
            after_ring,
            migration_guard,
            keys,
        )
        .await;

        // The joiner got both keys, each as a put-if-absent handoff
        // (issue #266) — in whatever order `list_keys` happened to
        // return them, so checked as independent substrings rather than
        // one fixed concatenation.
        let expected_key0 =
            handoff_message(&key(b"key-0"), b"primary-copy", None, true, "tok-joiner-0");
        let expected_key3 =
            handoff_message(&key(b"key-3"), b"replica-copy", None, true, "tok-joiner-0");
        let received = joining_received.lock().unwrap().clone();
        assert!(
            received
                .windows(expected_key0.len())
                .any(|window| window == expected_key0.as_slice()),
            "expected the joining node to receive \"key-0\", got {received:?}"
        );
        assert!(
            received
                .windows(expected_key3.len())
                .any(|window| window == expected_key3.as_slice()),
            "expected the joining node to receive \"key-3\", got {received:?}"
        );

        // The displaced copy — and only it — is reclaimed by the sweep.
        assert_eq!(
            send_command(&request_tx, Command::Sweep { marked: true }).await,
            Response::Swept(1)
        );
        match send_command(&request_tx, Command::PeekEntry { key: key(b"key-0") }).await {
            Response::Entries(entries) => {
                assert_eq!(entries.len(), 1, "the sender must keep its copy")
            }
            other => panic!("unexpected response: {other:?}"),
        }
        match send_command(&request_tx, Command::PeekEntry { key: key(b"key-3") }).await {
            Response::Entries(entries) => {
                assert!(entries.is_empty(), "the displaced copy must be swept")
            }
            other => panic!("unexpected response: {other:?}"),
        }

        // The flipped membership says the displaced key is no longer this
        // node's — but it keeps being served (and forwarded) for as long
        // as the handoff's forwarding window is open, since discovery
        // hasn't published the joiner yet; the kept key is served as ever.
        assert!(!wrong_node(&node_context, &key(b"key-3")));
        assert!(!wrong_node(&node_context, &key(b"key-0")));

        // Join confirmed by discovery, window still open (issue #66): the
        // displaced key is now rejected — `L` lists the joiner, so a
        // stale client's refresh-and-retry lands there instead of on
        // this node's dead copy — while the kept one is still served and
        // write forwarding (`migration_target_for`) continues.
        node_context
            .active_migration
            .lock()
            .unwrap()
            .as_mut()
            .unwrap()
            .confirmed = true;
        assert!(wrong_node(&node_context, &key(b"key-3")));
        assert!(!wrong_node(&node_context, &key(b"key-0")));
        assert!(migration_target_for(&node_context, &key(b"key-3")).is_some());

        // Issue #3: this node's own share being done must NOT close the
        // write-forwarding window — discovery hasn't published the joiner
        // yet (other ready nodes may still be transferring), so a
        // concurrent client write for a key in the joiner's top-R still
        // needs forwarding.
        assert_eq!(
            migration_target_for(&node_context, &key(b"key-0")).map(|target| target.addr),
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

        // Window closed and the join confirmed (issue #62: an unconfirmed
        // slot outlives its grace to hold its marks): now the displaced
        // key is rejected (and the lingering slot is cleared lazily), the
        // kept one still served.
        {
            let mut slot = node_context.active_migration.lock().unwrap();
            let active = slot.as_mut().unwrap();
            active.completed_at = Some(Instant::now() - active.forwarding_grace);
            active.confirmed = true;
        }
        assert!(wrong_node(&node_context, &key(b"key-3")));
        assert!(!wrong_node(&node_context, &key(b"key-0")));
        assert!(node_context.active_migration.lock().unwrap().is_none());

        joining_task.await.unwrap();
        discovery_task.await.unwrap();
        drop(node_context);
        drop(request_tx);
        cache_task.await.unwrap();
    }

    #[tokio::test(flavor = "current_thread")]
    async fn an_m_that_flips_a_known_ring_still_listing_an_evicted_member_triggers_rereplication() {
        // Issue #266 end to end: this node ("node-x")'s own `known_ring`
        // still lists "dead-node" (its next heartbeat ack hasn't caught
        // up to the eviction yet — a restart+rejoin can outrace it), but
        // the incoming `M`'s own roster (`joined`) is discovery's
        // *current* view and already excludes it. For a key whose old
        // owners (under the stale belief) were (dead-node, node-x) and
        // whose new owners (under the post-join ring, excluding the
        // joiner) are (node-x, node-y), node-x must re-replicate to
        // node-y — the join's own before_ring/after_ring alone would
        // never reveal that gap.
        let (request_tx, request_rx) = mpsc::channel(16);
        let cache_task = tokio::spawn(run_cache(request_rx, MAX_CACHE_MEMORY_BYTES, Vec::new()));

        const KEY_COUNT: u32 = 60;
        for index in 0..KEY_COUNT {
            send_command(
                &request_tx,
                Command::Set {
                    key: key(format!("key-{index}").as_bytes()),
                    value: Bytes::from_static(b"v"),
                    ttl: None,
                },
            )
            .await;
        }

        let replication = 2;
        let stale_belief = HashRing::new(vec![
            "dead-node".to_string(),
            "node-x".to_string(),
            "node-y".to_string(),
        ]);
        let after_join = HashRing::new(vec![
            "node-x".to_string(),
            "node-y".to_string(),
            "joiner-w".to_string(),
        ]);
        // Filtered to targets containing "node-y" specifically, not just
        // "non-empty": for some keys the promoted owner is "joiner-w"
        // itself rather than "node-y" (both are new members of
        // `after_join`, and either can win a given key's promoted rank)
        // — a real, valid outcome this test doesn't otherwise exercise
        // (its fake joiner only expects run_migration's own bulk-transfer
        // connection, see `spawn_recording_joiner`), so only the
        // node-y-bound subset belongs in this assertion.
        let expected_keys: Vec<Key> = (0..KEY_COUNT)
            .map(|index| key(format!("key-{index}").as_bytes()))
            .filter(|key| {
                rereplication_targets(&stale_belief, &after_join, key, replication, "node-x")
                    .contains(&"node-y".to_string())
            })
            .collect();
        assert!(
            !expected_keys.is_empty(),
            "the sample must contain at least one key node-x re-replicates to node-y"
        );
        assert!(
            expected_keys.len() < KEY_COUNT as usize,
            "the sample must also contain at least one key node-x does not re-replicate"
        );

        // node-y: records every `U … A` it receives.
        let y_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let y_addr = y_listener.local_addr().unwrap().to_string();
        let (y_frames, y_task) = spawn_rereplication_recording_peer(y_listener);

        // joiner-w: the `M`'s own transfer target — content not checked
        // here (that's `migrate_with_replication_marks_displaced_copies_
        // and_keeps_the_senders`'s job), just drained so the transfer
        // doesn't stall.
        let joining_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let joining_addr = joining_listener.local_addr().unwrap().to_string();
        let (_joiner_frames, joining_task) = spawn_recording_joiner(joining_listener);

        // Fake discovery: expects the `C` completion report, then (issue
        // #295) the join-flip trigger's own `T` roster+token fetch — a
        // fresh connection, answered with node-y's (and joiner-w's)
        // address and a token, so `run_rereplication` can authorize its
        // `U … A` to node-y.
        let discovery_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let discovery_addr = discovery_listener.local_addr().unwrap().to_string();
        let y_addr_for_discovery = y_addr.clone();
        let joining_addr_for_discovery = joining_addr.clone();
        let discovery_task = tokio::spawn(async move {
            let (mut connection, _) = discovery_listener.accept().await.unwrap();
            let mut buffer = [0u8; 256];
            let _ = connection.read(&mut buffer).await.unwrap();
            connection.write_all(b"A\n").await.unwrap();
            drop(connection);

            let (mut connection, _) = discovery_listener.accept().await.unwrap();
            let mut buffer = [0u8; 256];
            let _ = connection.read(&mut buffer).await.unwrap();
            let entries = [
                ("node-y", y_addr_for_discovery.as_str(), "tok-node-y"),
                (
                    "joiner-w",
                    joining_addr_for_discovery.as_str(),
                    "tok-joiner-w",
                ),
            ];
            let mut response = format!("N {} 2\n", entries.len()).into_bytes();
            for (name, addr, token) in entries {
                response.extend_from_slice(
                    format!(
                        "{} {} {}\n{name}{addr}{token}\n",
                        name.len(),
                        addr.len(),
                        token.len()
                    )
                    .as_bytes(),
                );
            }
            connection.write_all(&response).await.unwrap();
        });

        let (rereplication_tx, mut rereplication_rx) = mpsc::channel::<RereplicationTask>(4);
        let node_context = NodeContext {
            name: "node-x".to_string(),
            token: "tk-node-x".to_string(),
            discovery_addr,
            active_migration: Arc::new(Mutex::new(None)),
            // This node's own (stale) belief — still lists "dead-node".
            known_ring: Arc::new(Mutex::new(Some(Arc::new(Membership {
                ring: Arc::new(stale_belief),
                replication,
            })))),
            auth_secret: None,
            tls_connector: None,
            request_tx: request_tx.clone(),
            leaving: Arc::new(Mutex::new(None)),
            active_rereplication: Arc::new(Mutex::new(None)),
            rereplication_tx,
            shutdown_rx: watch::channel(false).1,
        };

        // The `M`'s own roster: discovery's *current* view, which never
        // included "dead-node" in the first place.
        let joined = vec![
            ("node-x".to_string(), "127.0.0.1:1".to_string()),
            ("node-y".to_string(), y_addr.clone()),
        ];
        let (before_ring, after_ring) = migration_rings(&node_context, "joiner-w", &joined);
        let after_ring = Arc::new(after_ring);
        assert_eq!(
            after_ring.nodes().len(),
            3,
            "sanity: node-x, node-y, joiner-w"
        );

        let migration_guard = MigrationGuard::new(
            Arc::clone(&node_context.active_migration),
            "joiner-w".to_string(),
            joining_addr.clone(),
            "tok-joiner-w".to_string(),
            Arc::clone(&after_ring),
            replication,
            &joined,
            &node_context.known_ring,
        )
        .unwrap_new();

        let keys = list_keys(&request_tx).await;

        run_migration(
            node_context.clone(),
            "joiner-w".to_string(),
            joining_addr,
            "tok-joiner-w".to_string(),
            replication,
            before_ring,
            after_ring,
            migration_guard,
            keys,
        )
        .await;

        // The join-flip trigger queued a re-replication task (issue
        // #266) — run it inline rather than through a full
        // `send_heartbeats`/`JoinSet`, exactly as `send_heartbeats`
        // itself would.
        let task = tokio::time::timeout(Duration::from_secs(5), rereplication_rx.recv())
            .await
            .expect("the join-flip trigger must queue a re-replication task")
            .expect(
                "the sender is still alive — node_context.clone() inside run_migration holds it",
            );
        task.await;

        let frames = y_frames.lock().unwrap().clone();
        let mut sent_keys: Vec<Key> = frames
            .iter()
            .map(|frame| {
                // Issue #295: body now leads with `<token-len>` bytes of
                // token before `<ns><key><value>`.
                let header_end = frame.iter().position(|byte| *byte == b'\n').unwrap();
                let header = String::from_utf8(frame[..header_end].to_vec()).unwrap();
                let mut fields = header.split(' ').skip(1);
                let ns_len: usize = fields.next().unwrap().parse().unwrap();
                let key_len: usize = fields.next().unwrap().parse().unwrap();
                let _val_len: usize = fields.next().unwrap().parse().unwrap();
                let token_len: usize = fields.next().unwrap().parse().unwrap();
                let ns_start = header_end + 1 + token_len;
                Key::new(
                    Bytes::copy_from_slice(&frame[ns_start..ns_start + ns_len]),
                    Bytes::copy_from_slice(&frame[ns_start + ns_len..ns_start + ns_len + key_len]),
                )
            })
            .collect();
        sent_keys.sort_by(|a, b| a.name.cmp(&b.name));
        let mut expected_sorted = expected_keys.clone();
        expected_sorted.sort_by(|a, b| a.name.cmp(&b.name));
        assert_eq!(
            sent_keys, expected_sorted,
            "node-y should receive exactly the keys whose old owners were (dead-node, node-x) \
             and whose post-join owners are (node-x, node-y)"
        );

        joining_task.abort();
        y_task.abort();
        discovery_task.abort();
        drop(node_context);
        drop(request_tx);
        cache_task.await.unwrap();
    }

    // ── issue #93: abandon reverts known_ring ──────────────────────────

    /// A `NodeContext` (named "ready-node") whose `known_ring` currently
    /// holds the post-join ring a completed handoff flipped it to, with a
    /// matching completed-but-unconfirmed slot carrying the pre-join
    /// snapshot. `key-3`'s pre-join top-2 is [other-node, ready-node]
    /// (this node owns it); joiner-0 enters and displaces ready-node.
    fn completed_unconfirmed_context(
        marked: &[&str],
    ) -> (NodeContext, Arc<HashRing>, Arc<Membership>) {
        let before_ring = Arc::new(HashRing::new(vec![
            "ready-node".to_string(),
            "other-node".to_string(),
        ]));
        let after_ring = Arc::new(HashRing::new(vec![
            "ready-node".to_string(),
            "other-node".to_string(),
            "joiner-0".to_string(),
        ]));
        let pre_completion_ring = Arc::new(Membership {
            ring: Arc::clone(&before_ring),
            replication: 2,
        });
        let (request_tx, request_rx) = mpsc::channel(1);
        // Keep the receiver alive for the context's lifetime — wrong_node /
        // abandon_migration never touch it, but a dropped rx would make any
        // stray send fail.
        Box::leak(Box::new(request_rx));

        let node_context = NodeContext {
            name: "ready-node".to_string(),
            token: "tk-ready-node".to_string(),
            discovery_addr: "127.0.0.1:1".to_string(),
            active_migration: Arc::new(Mutex::new(Some(ActiveMigration {
                joining_name: "joiner-0".to_string(),
                joining_addr: "127.0.0.1:9".to_string(),
                joining_token: "tok-joiner-0".to_string(),
                after_ring: Arc::clone(&after_ring),
                replication: 2,
                completed_at: Some(Instant::now()),
                forwarding_grace: forwarding_grace(0),
                acked_entries: Some(0),
                abort_requested: Arc::new(AtomicBool::new(false)),
                marked_keys: marked
                    .iter()
                    .map(|key| Key::from(Bytes::copy_from_slice(key.as_bytes())))
                    .collect(),
                confirmed: false,
                pre_completion_ring: Some(Arc::clone(&pre_completion_ring)),
                pending_clears: Vec::new(),
                forward_connection: Arc::new(AsyncMutex::new(None)),
            }))),
            known_ring: Arc::new(Mutex::new(Some(Arc::new(Membership {
                ring: Arc::clone(&after_ring),
                replication: 2,
            })))),
            auth_secret: None,
            tls_connector: None,
            request_tx,
            leaving: Arc::new(Mutex::new(None)),
            active_rereplication: Arc::new(Mutex::new(None)),
            rereplication_tx: mpsc::channel(1).0,
            shutdown_rx: watch::channel(false).1,
        };
        (node_context, after_ring, pre_completion_ring)
    }

    #[test]
    fn abandoning_a_completed_join_reverts_known_ring() {
        let (node_context, _after_ring, pre_completion_ring) =
            completed_unconfirmed_context(&["key-3"]);

        // While forwarding (unconfirmed), the displaced key is still served
        // locally — the join isn't visible to clients yet.
        assert!(!wrong_node(&node_context, &key(b"key-3")));

        let restored = abandon_migration(&node_context, "joiner-0")
            .expect("a completed handoff must hand back its dead copies to restore");
        assert_eq!(restored, vec![key(b"key-3")]);

        // The slot is gone and known_ring is back to the pre-join snapshot.
        assert!(node_context.active_migration.lock().unwrap().is_none());
        assert!(Arc::ptr_eq(
            node_context.known_ring.lock().unwrap().as_ref().unwrap(),
            &pre_completion_ring,
        ));

        // The bug (issue #93): without the revert, known_ring would still be
        // the abandoned post-join ring and the slot would be gone, so
        // wrong_node would answer W for this node's own live key until the
        // next heartbeat. With the revert it's served immediately.
        assert!(!wrong_node(&node_context, &key(b"key-3")));
        assert!(!wrong_node(&node_context, &key(b"key-0")));
    }

    #[test]
    fn abandon_does_not_clobber_a_newer_known_ring() {
        // If a later membership update already replaced known_ring (e.g. the
        // grace lapsed and a heartbeat adopted a fresh roster) it must win —
        // reverting to the stale pre-join snapshot would undo it.
        let (node_context, _after_ring, _pre) = completed_unconfirmed_context(&["key-3"]);
        let newer = Arc::new(Membership {
            ring: Arc::new(HashRing::new(vec![
                "ready-node".to_string(),
                "other-node".to_string(),
                "joiner-0".to_string(),
                "joiner-1".to_string(),
            ])),
            replication: 2,
        });
        *node_context.known_ring.lock().unwrap() = Some(Arc::clone(&newer));

        let restored = abandon_migration(&node_context, "joiner-0").unwrap();
        assert_eq!(restored, vec![key(b"key-3")]);
        assert!(
            Arc::ptr_eq(
                node_context.known_ring.lock().unwrap().as_ref().unwrap(),
                &newer,
            ),
            "a newer known_ring must not be reverted to the pre-join snapshot"
        );
    }

    #[test]
    fn abandoning_an_in_flight_join_only_requests_abort() {
        // The transfer is still running (not completed): the abandon just
        // asks it to stop — run_migration rolls back its own marks and never
        // flipped known_ring, so there's nothing to hand back or revert.
        let (node_context, after_ring, _pre) = completed_unconfirmed_context(&["key-3"]);
        {
            let mut slot = node_context.active_migration.lock().unwrap();
            slot.as_mut().unwrap().completed_at = None;
        }

        assert!(abandon_migration(&node_context, "joiner-0").is_none());
        // The slot survives (run_migration's guard still owns it) with abort
        // requested, and known_ring is left exactly as it was.
        let slot = node_context.active_migration.lock().unwrap();
        let active = slot.as_ref().expect("an in-flight slot must not be taken");
        assert!(active.abort_requested.load(Ordering::SeqCst));
        assert!(Arc::ptr_eq(
            &node_context
                .known_ring
                .lock()
                .unwrap()
                .as_ref()
                .unwrap()
                .ring,
            &after_ring,
        ));
    }

    #[test]
    fn migration_guard_new_reverts_known_ring_for_an_implicitly_abandoned_handoff() {
        // Issue #218: same scenario as `abandoning_a_completed_join_reverts_known_ring`,
        // but discovered *implicitly* — a new `M` for a different joiner (J2)
        // arrives whose roster lacks the previous joiner (J1), instead of an
        // explicit `X`. `MigrationGuard::new` must revert `known_ring` to the
        // pre-completion snapshot exactly like `abandon_migration` does,
        // otherwise `wrong_node` keeps routing to the phantom J1 until the
        // next heartbeat's `adopt_membership`.
        let (node_context, after_ring, pre_completion_ring) =
            completed_unconfirmed_context(&["key-3"]);

        // `M` for joiner-1: the roster lists only ready-node, so joiner-0
        // (the previous, completed-but-unconfirmed joiner) is missing —
        // that handoff was abandoned.
        let joined = vec![("ready-node".to_string(), "127.0.0.1:1".to_string())];
        let outcome = MigrationGuard::new(
            Arc::clone(&node_context.active_migration),
            "joiner-1".to_string(),
            "127.0.0.1:10".to_string(),
            "tok-joiner-1".to_string(),
            Arc::new(HashRing::new(vec![
                "ready-node".to_string(),
                "other-node".to_string(),
                "joiner-1".to_string(),
            ])),
            2,
            &joined,
            &node_context.known_ring,
        );

        let _guard = match outcome {
            MigrationOutcome::New { restore, guard } => {
                assert_eq!(restore, vec![key(b"key-3")]);
                guard
            }
            _ => panic!("expected a new guard"),
        };

        // known_ring is back to the pre-join snapshot, not still pointing at
        // joiner-0's abandoned post-join ring.
        assert!(Arc::ptr_eq(
            node_context.known_ring.lock().unwrap().as_ref().unwrap(),
            &pre_completion_ring,
        ));
        assert!(!Arc::ptr_eq(
            &node_context
                .known_ring
                .lock()
                .unwrap()
                .as_ref()
                .unwrap()
                .ring,
            &after_ring,
        ));
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
            set_message(&key(b"name"), b"Alice", None),
            b"S 4 5\nnameAlice".to_vec()
        );
    }

    #[test]
    fn set_message_with_a_ttl_rounds_down_to_whole_seconds() {
        assert_eq!(
            set_message(&key(b"name"), b"Alice", Some(Duration::from_millis(4900))),
            b"S 4 5 4\nnameAlice".to_vec()
        );
    }

    #[test]
    fn set_message_for_a_namespaced_key_uses_the_lowercase_frame() {
        // Issue #105: namespace length leads, namespace bytes lead the body.
        let namespaced = Key::new(Bytes::from_static(b"users"), Bytes::from_static(b"name"));
        assert_eq!(
            set_message(&namespaced, b"Alice", Some(Duration::from_secs(4))),
            b"s 5 4 5 4\nusersnameAlice".to_vec()
        );
        assert_eq!(delete_message(&namespaced), b"d 5 4\nusersname".to_vec());
        // The default namespace keeps the legacy frames byte-for-byte, so a
        // mixed-version handoff still works for un-namespaced traffic.
        assert_eq!(delete_message(&key(b"name")), b"D 4\nname".to_vec());
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
            leaving: Arc::new(Mutex::new(None)),
            active_rereplication: Arc::new(Mutex::new(None)),
            rereplication_tx: mpsc::channel(1).0,
            shutdown_rx: watch::channel(false).1,
        };

        *node_context.active_migration.lock().unwrap() = Some(ActiveMigration {
            joining_name: "joiner-0".to_string(),
            joining_addr: "127.0.0.1:9".to_string(),
            joining_token: "tok-joiner-0".to_string(),
            after_ring: Arc::new(HashRing::new(vec![
                "ready-node".to_string(),
                "joiner-0".to_string(),
            ])),
            replication: 2,
            completed_at: Some(Instant::now() - forwarding_grace(0) - Duration::from_secs(1)),
            forwarding_grace: forwarding_grace(0),
            acked_entries: Some(0),
            abort_requested: Arc::new(AtomicBool::new(false)),
            marked_keys: Vec::new(),
            confirmed: true,
            pre_completion_ring: None,
            pending_clears: Vec::new(),
            forward_connection: Arc::new(AsyncMutex::new(None)),
        });

        assert!(migration_target_for(&node_context, &key(b"key-0")).is_none());
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
            joining_token: "tok-joiner-0".to_string(),
            after_ring: Arc::new(HashRing::new(vec![
                "ready-node".to_string(),
                "joiner-0".to_string(),
            ])),
            replication: 2,
            completed_at: Some(Instant::now() - forwarding_grace(0) - Duration::from_secs(1)),
            forwarding_grace: forwarding_grace(0),
            acked_entries: Some(0),
            abort_requested: Arc::new(AtomicBool::new(false)),
            marked_keys: Vec::new(),
            confirmed: true,
            pre_completion_ring: None,
            pending_clears: Vec::new(),
            forward_connection: Arc::new(AsyncMutex::new(None)),
        })));

        let after_ring = Arc::new(HashRing::new(vec![
            "ready-node".to_string(),
            "joiner-1".to_string(),
        ]));
        let known_ring: KnownRing = Arc::new(Mutex::new(None));
        let outcome = MigrationGuard::new(
            Arc::clone(&slot),
            "joiner-1".to_string(),
            "127.0.0.1:10".to_string(),
            "tok-joiner-1".to_string(),
            after_ring,
            2,
            &[],
            &known_ring,
        );

        assert!(
            matches!(outcome, MigrationOutcome::New { .. }),
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
            joining_token: "tok-joiner-0".to_string(),
            after_ring: Arc::new(HashRing::new(vec![
                "ready-node".to_string(),
                "joiner-0".to_string(),
            ])),
            replication: 2,
            completed_at: None,
            forwarding_grace: Duration::ZERO,
            acked_entries: Some(0),
            abort_requested: Arc::new(AtomicBool::new(false)),
            marked_keys: Vec::new(),
            confirmed: false,
            pre_completion_ring: None,
            pending_clears: Vec::new(),
            forward_connection: Arc::new(AsyncMutex::new(None)),
        })));

        let after_ring = Arc::new(HashRing::new(vec![
            "ready-node".to_string(),
            "joiner-1".to_string(),
        ]));
        let known_ring: KnownRing = Arc::new(Mutex::new(None));
        let outcome = MigrationGuard::new(
            Arc::clone(&slot),
            "joiner-1".to_string(),
            "127.0.0.1:10".to_string(),
            "tok-joiner-1".to_string(),
            after_ring,
            2,
            &[],
            &known_ring,
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
        let cache_task = tokio::spawn(run_cache(request_rx, MAX_CACHE_MEMORY_BYTES, Vec::new()));

        send_command(
            &request_tx,
            Command::Set {
                key: key(b"name"),
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
                    leaving: Arc::new(Mutex::new(None)),
                    active_rereplication: Arc::new(Mutex::new(None)),
                    rereplication_tx: mpsc::channel(1).0,
                    shutdown_rx: watch::channel(false).1,
                }),
                migration_tx,
                forward_tx: mpsc::channel(1).0,
            },
            shutdown_rx.clone(),
        ));

        // No other Joined nodes: the after-join ring has only the joining
        // node in it, so every key (including "name") routes to it.
        // Chosen so HRW ranks it above "ready-node" for both test keys
        // ("name", "age") — the transfer set must be non-empty.
        let joining_name = "joiner-107";
        let token = "tk-ready-node";
        let joining_token = "tok-joiner-107";
        let mut migrate_message = format!(
            "M {} {} {} 0 1 {}\n",
            joining_name.len(),
            joining_addr.len(),
            joining_token.len(),
            token.len()
        )
        .into_bytes();
        migrate_message.extend_from_slice(token.as_bytes());
        migrate_message.extend_from_slice(joining_name.as_bytes());
        migrate_message.extend_from_slice(joining_addr.as_bytes());
        migrate_message.extend_from_slice(joining_token.as_bytes());

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

        // Issue #266: run_migration's own bulk transfer now sends a
        // put-if-absent `U … A`, not a plain `S` — see its doc comment.
        let expected_set = handoff_message(&key(b"name"), b"Alice", None, true, joining_token);
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
        let cache_task = tokio::spawn(run_cache(request_rx, MAX_CACHE_MEMORY_BYTES, Vec::new()));

        send_command(
            &request_tx,
            Command::Set {
                key: key(b"name"),
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
                    leaving: Arc::new(Mutex::new(None)),
                    active_rereplication: Arc::new(Mutex::new(None)),
                    rereplication_tx: mpsc::channel(1).0,
                    shutdown_rx: watch::channel(false).1,
                }),
                migration_tx,
                forward_tx: mpsc::channel(1).0,
            },
            shutdown_rx.clone(),
        ));

        let joining_name = "joiner-107";
        let wrong_token = "tk-not-mine";
        let joining_token = "tok-joiner-107";
        let mut migrate_message = format!(
            "M {} {} {} 0 1 {}\n",
            joining_name.len(),
            attacker_addr.len(),
            joining_token.len(),
            wrong_token.len()
        )
        .into_bytes();
        migrate_message.extend_from_slice(wrong_token.as_bytes());
        migrate_message.extend_from_slice(joining_name.as_bytes());
        migrate_message.extend_from_slice(attacker_addr.as_bytes());
        migrate_message.extend_from_slice(joining_token.as_bytes());
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
        let cache_task = tokio::spawn(run_cache(request_rx, MAX_CACHE_MEMORY_BYTES, Vec::new()));

        send_command(
            &request_tx,
            Command::Set {
                key: key(b"name"),
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
                    leaving: Arc::new(Mutex::new(None)),
                    active_rereplication: Arc::new(Mutex::new(None)),
                    rereplication_tx: mpsc::channel(1).0,
                    shutdown_rx: watch::channel(false).1,
                }),
                migration_tx,
                forward_tx: mpsc::channel(1).0,
            },
            shutdown_rx.clone(),
        ));

        let joining_name = "joiner-107";
        let token = "tk-ready-node";
        let joining_token = "tok-joiner-107";
        let mut migrate_message = format!(
            "M {} {} {} 0 1 {}\n",
            joining_name.len(),
            joining_addr.len(),
            joining_token.len(),
            token.len()
        )
        .into_bytes();
        migrate_message.extend_from_slice(token.as_bytes());
        migrate_message.extend_from_slice(joining_name.as_bytes());
        migrate_message.extend_from_slice(joining_addr.as_bytes());
        migrate_message.extend_from_slice(joining_token.as_bytes());

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
        // Issue #266: run_migration's own bulk transfer now sends a
        // put-if-absent `U … A`, not a plain `S` — see its doc comment.
        let expected_set = handoff_message(&key(b"name"), b"Alice", None, true, joining_token);
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
    async fn migrate_command_accepts_a_different_joining_node_once_the_handoff_completed() {
        // Issue #62: a completed handoff (still within its forwarding
        // window) must not block the next join — discovery serializes
        // joins, so a second `M` means the first join was decided. (A
        // genuinely in-flight conflict is still rejected — see
        // `migration_guard_rejects_a_still_active_conflicting_migration`.)
        let (request_tx, request_rx) = mpsc::channel(1);
        let cache_task = tokio::spawn(run_cache(request_rx, MAX_CACHE_MEMORY_BYTES, Vec::new()));

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
                    leaving: Arc::new(Mutex::new(None)),
                    active_rereplication: Arc::new(Mutex::new(None)),
                    rereplication_tx: mpsc::channel(1).0,
                    shutdown_rx: watch::channel(false).1,
                }),
                migration_tx,
                forward_tx: mpsc::channel(1).0,
            },
            shutdown_rx.clone(),
        ));

        let first_joining_addr = "127.0.0.1:1";
        let token = "tk-ready-node";
        let joining_token = "tok-joiner-a";
        let mut first_migrate_message = format!(
            "M {} {} {} 0 1 {}\n",
            "joiner-a".len(),
            first_joining_addr.len(),
            joining_token.len(),
            token.len()
        )
        .into_bytes();
        first_migrate_message.extend_from_slice(token.as_bytes());
        first_migrate_message.extend_from_slice(b"joiner-a");
        first_migrate_message.extend_from_slice(first_joining_addr.as_bytes());
        first_migrate_message.extend_from_slice(joining_token.as_bytes());

        client.write_all(&first_migrate_message).await.unwrap();
        let mut first_ack = [0u8; 4];
        client.read_exact(&mut first_ack).await.unwrap();
        assert_eq!(&first_ack, b"A 0\n");

        let second_joining_addr = "127.0.0.1:2";
        let second_joining_token = "tok-joiner-b";
        let mut second_migrate_message = format!(
            "M {} {} {} 0 1 {}\n",
            "joiner-b".len(),
            second_joining_addr.len(),
            second_joining_token.len(),
            token.len()
        )
        .into_bytes();
        second_migrate_message.extend_from_slice(token.as_bytes());
        second_migrate_message.extend_from_slice(b"joiner-b");
        second_migrate_message.extend_from_slice(second_joining_addr.as_bytes());
        second_migrate_message.extend_from_slice(second_joining_token.as_bytes());

        // Only once the first handoff has reported `C` (its slot is
        // stamped completed before that report goes out).
        discovery_task.await.unwrap();

        client.write_all(&second_migrate_message).await.unwrap();
        let mut second_ack = [0u8; 4];
        client.read_exact(&mut second_ack).await.unwrap();
        assert_eq!(
            &second_ack, b"A 0\n",
            "an M for the next joining node must be accepted once the previous handoff completed"
        );

        client.shutdown().await.unwrap();
        let _ = connection_task.await;
        migration_relay.await.unwrap();
        drop(request_tx);
        cache_task.await.unwrap();
    }

    fn completed_forwarding_slot(marked: &[&str]) -> ActiveMigration {
        ActiveMigration {
            joining_name: "joiner-0".to_string(),
            joining_addr: "127.0.0.1:9".to_string(),
            joining_token: "tok-joiner-0".to_string(),
            after_ring: Arc::new(HashRing::new(vec![
                "ready-node".to_string(),
                "joiner-0".to_string(),
            ])),
            replication: 2,
            completed_at: Some(Instant::now()),
            forwarding_grace: forwarding_grace(0),
            acked_entries: Some(0),
            abort_requested: Arc::new(AtomicBool::new(false)),
            marked_keys: marked
                .iter()
                .map(|key| Key::from(Bytes::copy_from_slice(key.as_bytes())))
                .collect(),
            confirmed: false,
            pre_completion_ring: None,
            pending_clears: Vec::new(),
            forward_connection: Arc::new(AsyncMutex::new(None)),
        }
    }

    fn new_guard_for_joiner_1(
        slot: &Arc<Mutex<Option<ActiveMigration>>>,
        joined: &[(String, String)],
        known_ring: &KnownRing,
    ) -> MigrationOutcome {
        MigrationGuard::new(
            Arc::clone(slot),
            "joiner-1".to_string(),
            "127.0.0.1:10".to_string(),
            "tok-joiner-1".to_string(),
            Arc::new(HashRing::new(vec![
                "ready-node".to_string(),
                "joiner-1".to_string(),
            ])),
            2,
            joined,
            known_ring,
        )
    }

    #[test]
    fn migration_guard_accepts_the_next_join_while_a_completed_handoff_is_still_forwarding() {
        // Issue #62: back-to-back joins. The previous joiner is in the new
        // `M`'s roster, so its dead copies stay marked (nothing to
        // restore) and the slot moves on to the new joiner.
        let slot = Arc::new(Mutex::new(Some(completed_forwarding_slot(&["dead"]))));
        let joined = vec![
            ("ready-node".to_string(), "127.0.0.1:1".to_string()),
            ("joiner-0".to_string(), "127.0.0.1:9".to_string()),
        ];
        let known_ring: KnownRing = Arc::new(Mutex::new(None));

        // Kept alive: dropping an uncompleted guard clears the slot.
        let _guard = match new_guard_for_joiner_1(&slot, &joined, &known_ring) {
            MigrationOutcome::New { restore, guard } => {
                assert!(restore.is_empty());
                guard
            }
            _ => panic!("expected a new guard"),
        };
        assert_eq!(
            slot.lock().unwrap().as_ref().unwrap().joining_name,
            "joiner-1"
        );
    }

    #[test]
    fn migration_guard_restores_dead_copies_of_a_handoff_whose_joiner_never_joined() {
        // Issue #62: the new `M`'s roster lacks the previous joiner — that
        // join was abandoned (and this node missed the `X`), so the keys
        // it marked dead are handed back to be unmarked.
        let slot = Arc::new(Mutex::new(Some(completed_forwarding_slot(&[
            "dead-a", "dead-b",
        ]))));
        let joined = vec![("ready-node".to_string(), "127.0.0.1:1".to_string())];
        let known_ring: KnownRing = Arc::new(Mutex::new(None));

        let _guard = match new_guard_for_joiner_1(&slot, &joined, &known_ring) {
            MigrationOutcome::New { restore, guard } => {
                assert_eq!(restore, vec![key(b"dead-a"), key(b"dead-b")]);
                guard
            }
            _ => panic!("expected a new guard"),
        };
        assert_eq!(
            slot.lock().unwrap().as_ref().unwrap().joining_name,
            "joiner-1"
        );
    }

    #[test]
    fn an_unconfirmed_completed_slot_outlives_its_grace_but_stops_forwarding() {
        // Issue #62: past the grace, forwarding is over, but the slot (and
        // so its marks) stays until the join is decided.
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
            leaving: Arc::new(Mutex::new(None)),
            active_rereplication: Arc::new(Mutex::new(None)),
            rereplication_tx: mpsc::channel(1).0,
            shutdown_rx: watch::channel(false).1,
        };
        let mut stale = completed_forwarding_slot(&["dead"]);
        stale.completed_at = Some(Instant::now() - forwarding_grace(0) - Duration::from_secs(1));
        *node_context.active_migration.lock().unwrap() = Some(stale);

        assert!(migration_target_for(&node_context, &key(b"key-0")).is_none());
        assert!(
            node_context.active_migration.lock().unwrap().is_some(),
            "an unconfirmed slot must keep holding its marks"
        );

        // Once confirmed, the same stale slot is cleared lazily as before.
        node_context
            .active_migration
            .lock()
            .unwrap()
            .as_mut()
            .unwrap()
            .confirmed = true;
        assert!(migration_target_for(&node_context, &key(b"key-0")).is_none());
        assert!(node_context.active_migration.lock().unwrap().is_none());
    }

    #[test]
    fn adopt_membership_confirms_a_completed_handoff_once_the_roster_lists_its_joiner() {
        let known_ring: KnownRing = Arc::new(Mutex::new(None));
        let slot: Arc<Mutex<Option<ActiveMigration>>> =
            Arc::new(Mutex::new(Some(completed_forwarding_slot(&["dead"]))));

        // Roster without the joiner: neither adopted nor confirmed.
        adopt_membership(&known_ring, &slot, "d", vec!["ready-node".to_string()], 2);
        assert!(!slot.lock().unwrap().as_ref().unwrap().confirmed);
        assert!(known_ring.lock().unwrap().is_none());

        // Roster with the joiner: both.
        adopt_membership(
            &known_ring,
            &slot,
            "d",
            vec!["joiner-0".to_string(), "ready-node".to_string()],
            2,
        );
        assert!(slot.lock().unwrap().as_ref().unwrap().confirmed);
        assert!(known_ring.lock().unwrap().is_some());
    }

    #[tokio::test(flavor = "current_thread")]
    async fn a_cancel_after_completion_restores_the_dead_copies() {
        // Issue #62: this node finished its share (the key is marked dead)
        // and reported `C`; discovery then abandons the join (another
        // source couldn't take the `M`) and sends `X`. The copy must come
        // back — and must not have been swept in the meantime.
        let (request_tx, request_rx) = mpsc::channel(1);
        let cache_task = tokio::spawn(run_cache(request_rx, MAX_CACHE_MEMORY_BYTES, Vec::new()));

        send_command(
            &request_tx,
            Command::Set {
                key: key(b"name"),
                value: Bytes::from_static(b"Alice"),
                ttl: None,
            },
        )
        .await;

        // A fake joining node that acks the one SET.
        let joining_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let joining_addr = joining_listener.local_addr().unwrap().to_string();
        let joining_task = tokio::spawn(async move {
            let (mut connection, _) = joining_listener.accept().await.unwrap();
            let mut buffer = [0u8; 256];
            let _ = connection.read(&mut buffer).await.unwrap();
            connection.write_all(b"S\n").await.unwrap();
        });

        // A fake discovery server that acks the `C`, and signals it.
        let discovery_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let discovery_addr = discovery_listener.local_addr().unwrap().to_string();
        let (completed_tx, completed_rx) = oneshot::channel();
        let discovery_task = tokio::spawn(async move {
            let (mut connection, _) = discovery_listener.accept().await.unwrap();
            let mut buffer = [0u8; 256];
            let _ = connection.read(&mut buffer).await.unwrap();
            connection.write_all(b"A\n").await.unwrap();
            completed_tx.send(()).unwrap();
        });

        let (mut client, server) = tcp_pair().await;
        let (_shutdown_tx, shutdown_rx) = watch::channel(false);
        let (migration_tx, mut migration_rx) = mpsc::channel::<MigrationTask>(1);
        let migration_relay = tokio::spawn(async move {
            while let Some(task) = migration_rx.recv().await {
                task.await;
            }
        });
        let active_migration: Arc<Mutex<Option<ActiveMigration>>> = Arc::new(Mutex::new(None));

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
                    active_migration: Arc::clone(&active_migration),
                    known_ring: Arc::new(Mutex::new(None)),
                    auth_secret: None,
                    tls_connector: None,
                    request_tx: request_tx.clone(),
                    leaving: Arc::new(Mutex::new(None)),
                    active_rereplication: Arc::new(Mutex::new(None)),
                    rereplication_tx: mpsc::channel(1).0,
                    shutdown_rx: watch::channel(false).1,
                }),
                migration_tx,
                forward_tx: mpsc::channel(1).0,
            },
            shutdown_rx.clone(),
        ));

        // R=1, so the sender is displaced: "name" is marked dead once sent.
        let joining_name = "joiner-107";
        let token = "tk-ready-node";
        let joining_token = "tok-joiner-107";
        let mut migrate_message = format!(
            "M {} {} {} 0 1 {}\n",
            joining_name.len(),
            joining_addr.len(),
            joining_token.len(),
            token.len()
        )
        .into_bytes();
        migrate_message.extend_from_slice(token.as_bytes());
        migrate_message.extend_from_slice(joining_name.as_bytes());
        migrate_message.extend_from_slice(joining_addr.as_bytes());
        migrate_message.extend_from_slice(joining_token.as_bytes());
        client.write_all(&migrate_message).await.unwrap();
        let mut ack = [0u8; 4];
        client.read_exact(&mut ack).await.unwrap();
        assert_eq!(&ack, b"A 1\n");

        completed_rx.await.unwrap();
        joining_task.await.unwrap();
        discovery_task.await.unwrap();
        // `completed()` runs right after `C` is acked; wait for the stamp.
        for _ in 0..200 {
            if active_migration
                .lock()
                .unwrap()
                .as_ref()
                .is_some_and(|active| active.completed_at.is_some())
            {
                break;
            }
            sleep(Duration::from_millis(5)).await;
        }
        {
            let slot = active_migration.lock().unwrap();
            let active = slot.as_ref().expect("the completed slot must linger");
            assert!(active.completed_at.is_some());
            assert!(!active.confirmed);
            assert_eq!(active.marked_keys, vec![key(b"name")]);
        }
        // What `run_sweep` does while the join is undecided: TTL only.
        assert_eq!(
            send_command(&request_tx, Command::Sweep { marked: false }).await,
            Response::Swept(0)
        );

        let mut cancel_message = format!("X {} {}\n", joining_name.len(), token.len()).into_bytes();
        cancel_message.extend_from_slice(token.as_bytes());
        cancel_message.extend_from_slice(joining_name.as_bytes());
        client.write_all(&cancel_message).await.unwrap();
        let mut cancel_ack = [0u8; 2];
        client.read_exact(&mut cancel_ack).await.unwrap();
        assert_eq!(&cancel_ack, b"A\n");

        assert!(active_migration.lock().unwrap().is_none());
        // The mark is gone: a full sweep reclaims nothing and the key is
        // still served.
        assert_eq!(
            send_command(&request_tx, Command::Sweep { marked: true }).await,
            Response::Swept(0)
        );
        assert_eq!(
            send_command(&request_tx, Command::Get { key: key(b"name") }).await,
            Response::Value(Bytes::from_static(b"Alice"))
        );

        client.shutdown().await.unwrap();
        let _ = connection_task.await;
        migration_relay.await.unwrap();
        drop(request_tx);
        cache_task.await.unwrap();
    }

    #[tokio::test(flavor = "current_thread")]
    async fn migrate_command_cancelled_mid_transfer_rolls_back_marks_and_skips_completion() {
        let (request_tx, request_rx) = mpsc::channel(1);
        let cache_task = tokio::spawn(run_cache(request_rx, MAX_CACHE_MEMORY_BYTES, Vec::new()));

        send_command(
            &request_tx,
            Command::Set {
                key: key(b"name"),
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
                    leaving: Arc::new(Mutex::new(None)),
                    active_rereplication: Arc::new(Mutex::new(None)),
                    rereplication_tx: mpsc::channel(1).0,
                    shutdown_rx: watch::channel(false).1,
                }),
                migration_tx,
                forward_tx: mpsc::channel(1).0,
            },
            shutdown_rx.clone(),
        ));

        // Chosen so HRW ranks it above "ready-node" for both test keys
        // ("name", "age") — the transfer set must be non-empty.
        let joining_name = "joiner-107";
        let token = "tk-ready-node";
        let joining_token = "tok-joiner-107";
        let mut migrate_message = format!(
            "M {} {} {} 0 1 {}\n",
            joining_name.len(),
            joining_addr.len(),
            joining_token.len(),
            token.len()
        )
        .into_bytes();
        migrate_message.extend_from_slice(token.as_bytes());
        migrate_message.extend_from_slice(joining_name.as_bytes());
        migrate_message.extend_from_slice(joining_addr.as_bytes());
        migrate_message.extend_from_slice(joining_token.as_bytes());

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
            send_command(&request_tx, Command::Get { key: key(b"name") }).await,
            Response::Value(Bytes::from_static(b"Alice"))
        );
        assert_eq!(
            send_command(&request_tx, Command::Sweep { marked: true }).await,
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
        let cache_task = tokio::spawn(run_cache(request_rx, MAX_CACHE_MEMORY_BYTES, Vec::new()));

        send_command(
            &request_tx,
            Command::Set {
                key: key(b"name"),
                value: Bytes::from_static(b"Alice"),
                ttl: None,
            },
        )
        .await;
        send_command(
            &request_tx,
            Command::Set {
                key: key(b"age"),
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
                    leaving: Arc::new(Mutex::new(None)),
                    active_rereplication: Arc::new(Mutex::new(None)),
                    rereplication_tx: mpsc::channel(1).0,
                    shutdown_rx: watch::channel(false).1,
                }),
                migration_tx,
                forward_tx: mpsc::channel(1).0,
            },
            shutdown_rx.clone(),
        ));

        // Chosen so HRW ranks it above "ready-node" for both test keys
        // ("name", "age") — the transfer set must be non-empty.
        let joining_name = "joiner-107";
        let token = "tk-ready-node";
        let joining_token = "tok-joiner-107";
        let mut migrate_message = format!(
            "M {} {} {} 0 1 {}\n",
            joining_name.len(),
            joining_addr.len(),
            joining_token.len(),
            token.len()
        )
        .into_bytes();
        migrate_message.extend_from_slice(token.as_bytes());
        migrate_message.extend_from_slice(joining_name.as_bytes());
        migrate_message.extend_from_slice(joining_addr.as_bytes());
        migrate_message.extend_from_slice(joining_token.as_bytes());

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

        // Issue #266: run_migration's own bulk transfer now sends a
        // put-if-absent `U … A`, not a plain `S` — see its doc comment.
        let expected_name = handoff_message(&key(b"name"), b"Alice", None, true, joining_token);
        let expected_age = handoff_message(&key(b"age"), b"30", None, true, joining_token);
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
        let cache_task = tokio::spawn(run_cache(request_rx, MAX_CACHE_MEMORY_BYTES, Vec::new()));

        send_command(
            &request_tx,
            Command::Set {
                key: key(b"name"),
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
                    leaving: Arc::new(Mutex::new(None)),
                    active_rereplication: Arc::new(Mutex::new(None)),
                    rereplication_tx: mpsc::channel(1).0,
                    shutdown_rx: watch::channel(false).1,
                }),
                migration_tx,
                forward_tx: mpsc::channel(1).0,
            },
            shutdown_rx.clone(),
        ));

        // Chosen so HRW ranks it above "ready-node" for both test keys
        // ("name", "age") — the transfer set must be non-empty.
        let joining_name = "joiner-107";
        let token = "tk-ready-node";
        let joining_token = "tok-joiner-107";
        let mut migrate_message = format!(
            "M {} {} {} 0 1 {}\n",
            joining_name.len(),
            joining_addr.len(),
            joining_token.len(),
            token.len()
        )
        .into_bytes();
        migrate_message.extend_from_slice(token.as_bytes());
        migrate_message.extend_from_slice(joining_name.as_bytes());
        migrate_message.extend_from_slice(joining_addr.as_bytes());
        migrate_message.extend_from_slice(joining_token.as_bytes());

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
            send_command(&request_tx, Command::Get { key: key(b"name") }).await,
            Response::Value(Bytes::from_static(b"Alice"))
        );
        assert_eq!(
            send_command(&request_tx, Command::Sweep { marked: true }).await,
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
        // Explicit `let` + `drop`, not `mpsc::channel(4).1` inline: a
        // discarded temporary in the argument list of a directly
        // `.await`ed call lives until the `.await` completes (Rust's
        // temporary-lifetime-extension rule for `f(..).await;`), so the
        // sender half would stay alive for this whole call — keeping
        // `rereplication_rx` open and `send_heartbeats` waiting on it
        // forever, well past when `tasks` empties. Every other
        // `send_heartbeats` test sidesteps this by wrapping the call in
        // `tokio::spawn(..)`, which is never itself `.await`ed.
        let (rereplication_tx, rereplication_rx) = mpsc::channel::<RereplicationTask>(4);
        drop(rereplication_tx);

        send_heartbeats(
            HeartbeatConfig {
                discovery_addrs: vec!["127.0.0.1:1".to_string()],
                port: 8356,
                interval: Duration::from_secs(60),
                auth_secret: None,
                tls_connector: None,
            },
            test_node_context(
                "test-node",
                "tk-test-node",
                Arc::new(Mutex::new(None)),
                Arc::new(Mutex::new(None)),
            ),
            rereplication_rx,
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
            test_node_context(
                "test-node",
                "tk-test-node",
                Arc::new(Mutex::new(None)),
                Arc::new(Mutex::new(None)),
            ),
            mpsc::channel::<RereplicationTask>(4).1,
            shutdown_rx,
        ));

        sleep(Duration::from_millis(150)).await;

        shutdown_tx.send_replace(true);
        heartbeat_task.await.unwrap();
        fake_discovery.abort();

        assert_join_then_heartbeats(&received.lock().unwrap(), 8356);
    }

    fn test_active_migration(completed_at: Option<Instant>) -> ActiveMigration {
        ActiveMigration {
            joining_name: "joiner-0".to_string(),
            joining_addr: "127.0.0.1:9".to_string(),
            joining_token: "tok-joiner-0".to_string(),
            after_ring: Arc::new(HashRing::new(vec![
                "test-node".to_string(),
                "joiner-0".to_string(),
            ])),
            replication: 2,
            completed_at,
            forwarding_grace: forwarding_grace(0),
            acked_entries: Some(0),
            abort_requested: Arc::new(AtomicBool::new(false)),
            marked_keys: Vec::new(),
            confirmed: false,
            pre_completion_ring: None,
            pending_clears: Vec::new(),
            forward_connection: Arc::new(AsyncMutex::new(None)),
        }
    }

    /// Issue #266: a minimal `NodeContext` for the `send_heartbeats`/
    /// `register_with_discovery` tests below, none of which exercise a
    /// ring change that drops a member (so `request_tx` — never actually
    /// read by anything — is safe to leave dangling: no re-replication
    /// task ever gets far enough to use it).
    fn test_node_context(
        name: &str,
        token: &str,
        known_ring: KnownRing,
        active_migration: Arc<Mutex<Option<ActiveMigration>>>,
    ) -> NodeContext {
        NodeContext {
            name: name.to_string(),
            token: token.to_string(),
            discovery_addr: "127.0.0.1:0".to_string(),
            active_migration,
            known_ring,
            auth_secret: None,
            tls_connector: None,
            request_tx: mpsc::channel(1).0,
            leaving: Arc::new(Mutex::new(None)),
            active_rereplication: Arc::new(Mutex::new(None)),
            rereplication_tx: mpsc::channel(1).0,
            shutdown_rx: watch::channel(false).1,
        }
    }

    fn member_names(known_ring: &KnownRing) -> Option<(Vec<String>, usize)> {
        known_ring.lock().unwrap().as_ref().map(|membership| {
            let mut names = membership.ring.nodes().to_vec();
            names.sort_unstable();
            (names, membership.replication)
        })
    }

    #[tokio::test(flavor = "current_thread")]
    async fn read_heartbeat_ack_parses_a_bare_ack_and_a_roster() {
        // Issue #61 wire shapes: bare `A\n` = no update; `A <n> <r>\n`
        // + `L`-shaped entries = the current roster.
        let bare: &[u8] = b"A\n";
        let mut stream = tokio::io::BufReader::new(bare);
        assert!(read_heartbeat_ack(&mut stream).await.unwrap().is_none());

        let roster: &[u8] = b"A 2 3\n6 14\nnode-a127.0.0.1:9001\n6 14\nnode-b127.0.0.1:9002\nA\n";
        let mut stream = tokio::io::BufReader::new(roster);
        assert_eq!(
            read_heartbeat_ack(&mut stream).await.unwrap(),
            Some((vec!["node-a".to_string(), "node-b".to_string()], 3))
        );
        // Consumed exactly the roster: the next ack is still readable.
        assert!(read_heartbeat_ack(&mut stream).await.unwrap().is_none());
    }

    #[tokio::test(flavor = "current_thread")]
    async fn read_heartbeat_ack_rejects_malformed_acks() {
        for malformed in [
            &b"B\n"[..],
            b"A 1\n",
            b"A 1 0\n6 14\nnode-a127.0.0.1:9001\n",
            b"A 1 2\n6 14\nnode-a127.0.0.1:9001X",
            b"A 1 2\n6 14 7\nnode-a127.0.0.1:9001\n",
            b"A 99999999 2\n",
            b"A 1 2\n99999 14\n",
        ] {
            let mut stream = tokio::io::BufReader::new(malformed);
            assert!(
                read_heartbeat_ack(&mut stream).await.is_err(),
                "{malformed:?} should be rejected"
            );
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn read_heartbeat_ack_caps_an_unterminated_line() {
        // Issue #92: a hostile/misconfigured/MITM'd discovery that streams a
        // line without ever sending `\n` must error on the size cap, not
        // grow this node's heartbeat-task buffer until it OOMs. The size-cap
        // error (vs the generic "malformed" one an unbounded read reaches
        // only after buffering the whole flood) is the proof the read itself
        // was bounded — 10 MiB here stands in for an unbounded stream.
        let flood = vec![b'x'; 10 * 1024 * 1024];

        // The header line.
        let mut stream = tokio::io::BufReader::new(&flood[..]);
        let error = read_heartbeat_ack(&mut stream).await.unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
        assert!(
            error.to_string().contains("size cap"),
            "header: got {error}"
        );

        // An entry's length-prefix line, after a well-formed header.
        let mut ack = b"A 1 2\n".to_vec();
        ack.extend_from_slice(&vec![b'x'; 10 * 1024 * 1024]);
        let mut stream = tokio::io::BufReader::new(&ack[..]);
        let error = read_heartbeat_ack(&mut stream).await.unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
        assert!(error.to_string().contains("size cap"), "entry: got {error}");
    }

    #[tokio::test(flavor = "current_thread")]
    async fn fetch_roster_once_caps_an_unterminated_line() {
        // Issue #217: the same class of bug #92 fixed for
        // `read_heartbeat_ack`, but for the roster fetch `run_decommission`
        // uses (`L`). A hostile/misconfigured discovery that streams a line
        // without ever sending `\n` must error on the size cap, not grow
        // this node's buffer until it OOMs. The size-cap error (vs the
        // generic "bad L response" one an unbounded read reaches only
        // after buffering the whole flood) is the proof the read itself
        // was bounded — 10 MiB here stands in for an unbounded stream.
        async fn fetch_from(response: Vec<u8>) -> io::Error {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let discovery_addr = listener.local_addr().unwrap().to_string();

            let fake_discovery = tokio::spawn(async move {
                let (mut connection, _) = listener.accept().await.unwrap();
                let mut request = [0u8; 2];
                let _ = connection.read_exact(&mut request).await; // "L\n"
                let _ = connection.write_all(&response).await;
            });

            let node_context = NodeContext {
                name: "test-node".to_string(),
                token: "tk-test-node".to_string(),
                discovery_addr: discovery_addr.clone(),
                active_migration: Arc::new(Mutex::new(None)),
                known_ring: Arc::new(Mutex::new(None)),
                auth_secret: None,
                tls_connector: None,
                request_tx: mpsc::channel(1).0,
                leaving: Arc::new(Mutex::new(None)),
                active_rereplication: Arc::new(Mutex::new(None)),
                rereplication_tx: mpsc::channel(1).0,
                shutdown_rx: watch::channel(false).1,
            };

            let error = fetch_roster_once(&node_context, &discovery_addr)
                .await
                .unwrap_err();
            fake_discovery.abort();
            error
        }

        // The header line.
        let error = fetch_from(vec![b'x'; 10 * 1024 * 1024]).await;
        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
        assert!(
            error.to_string().contains("size cap"),
            "header: got {error}"
        );

        // An entry's length-prefix line, after a well-formed header.
        let mut response = b"N 1 2\n".to_vec();
        response.extend_from_slice(&vec![b'x'; 10 * 1024 * 1024]);
        let error = fetch_from(response).await;
        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
        assert!(error.to_string().contains("size cap"), "entry: got {error}");
    }

    #[test]
    fn adopt_membership_sets_and_replaces_the_belief_only_when_it_changes() {
        let known_ring: KnownRing = Arc::new(Mutex::new(None));
        let slot: Arc<Mutex<Option<ActiveMigration>>> = Arc::new(Mutex::new(None));

        // First roster: from no belief to one.
        adopt_membership(
            &known_ring,
            &slot,
            "d",
            vec!["test-node".to_string(), "node-b".to_string()],
            2,
        );
        assert_eq!(
            member_names(&known_ring),
            Some((vec!["node-b".to_string(), "test-node".to_string()], 2))
        );
        let first = Arc::clone(known_ring.lock().unwrap().as_ref().unwrap());

        // Same members in another order: no churn (same `Arc`).
        adopt_membership(
            &known_ring,
            &slot,
            "d",
            vec!["node-b".to_string(), "test-node".to_string()],
            2,
        );
        assert!(Arc::ptr_eq(
            &first,
            known_ring.lock().unwrap().as_ref().unwrap()
        ));

        // node-b evicted: the belief shrinks — the #61 fix proper.
        adopt_membership(&known_ring, &slot, "d", vec!["test-node".to_string()], 2);
        assert_eq!(
            member_names(&known_ring),
            Some((vec!["test-node".to_string()], 2))
        );

        // A different replication factor alone is a change too.
        adopt_membership(&known_ring, &slot, "d", vec!["test-node".to_string()], 3);
        assert_eq!(
            member_names(&known_ring),
            Some((vec!["test-node".to_string()], 3))
        );
    }

    #[test]
    fn adopt_membership_waits_only_while_the_join_is_still_pending() {
        let known_ring: KnownRing = Arc::new(Mutex::new(None));
        let slot: Arc<Mutex<Option<ActiveMigration>>> = Arc::new(Mutex::new(None));

        // In flight (not completed): skipped.
        *slot.lock().unwrap() = Some(test_active_migration(None));
        adopt_membership(&known_ring, &slot, "d", vec!["test-node".to_string()], 2);
        assert!(known_ring.lock().unwrap().is_none());

        // Completed but still forwarding writes, roster without the
        // joiner (the join itself hasn't completed): skipped.
        *slot.lock().unwrap() = Some(test_active_migration(Some(Instant::now())));
        adopt_membership(&known_ring, &slot, "d", vec!["test-node".to_string()], 2);
        assert!(known_ring.lock().unwrap().is_none());

        // Still forwarding, but the roster now lists the joiner (the join
        // completed) and is missing a member (evicted): applied — the
        // forwarding window must not delay an eviction (issue #61).
        adopt_membership(
            &known_ring,
            &slot,
            "d",
            vec!["joiner-0".to_string(), "test-node".to_string()],
            2,
        );
        assert_eq!(
            member_names(&known_ring),
            Some((vec!["joiner-0".to_string(), "test-node".to_string()], 2))
        );
        *known_ring.lock().unwrap() = None;

        // The joiner was confirmed by an earlier roster and has now been
        // evicted (roster without it) while this node is still inside
        // the forwarding window: applied — otherwise every survivor keeps
        // the dead joiner in its ring (answering `W` for its keys and
        // forwarding writes to its address, which a later container may
        // reuse) for the whole grace, up to minutes.
        *known_ring.lock().unwrap() = None;
        let mut confirmed = test_active_migration(Some(Instant::now()));
        confirmed.confirmed = true;
        *slot.lock().unwrap() = Some(confirmed);
        adopt_membership(&known_ring, &slot, "d", vec!["test-node".to_string()], 2);
        assert_eq!(
            member_names(&known_ring),
            Some((vec!["test-node".to_string()], 2))
        );
        // Issue #267: and the forwarding window closes with it — nothing
        // is left to forward to an evicted joiner.
        assert!(slot.lock().unwrap().is_none());
        *known_ring.lock().unwrap() = None;

        // Forwarding grace elapsed (a stale slot no request has lazily
        // cleared yet): applied.
        *slot.lock().unwrap() = Some(test_active_migration(Some(
            Instant::now() - forwarding_grace(0) - Duration::from_secs(1),
        )));
        adopt_membership(&known_ring, &slot, "d", vec!["test-node".to_string()], 2);
        assert_eq!(
            member_names(&known_ring),
            Some((vec!["test-node".to_string()], 2))
        );
    }

    /// A fake discovery server that answers the registration with `R\n`
    /// and every heartbeat with `ack`, returning everything it received.
    fn fake_discovery_acking_with(
        listener: TcpListener,
        ack: &'static [u8],
    ) -> Arc<std::sync::Mutex<Vec<Vec<u8>>>> {
        let received: Arc<std::sync::Mutex<Vec<Vec<u8>>>> =
            Arc::new(std::sync::Mutex::new(Vec::new()));
        let sink = Arc::clone(&received);
        tokio::spawn(async move {
            let (mut connection, _) = listener.accept().await.unwrap();
            let mut buffer = [0u8; 128];
            let mut first = true;
            loop {
                match connection.read(&mut buffer).await {
                    Ok(0) | Err(_) => return,
                    Ok(bytes_read) => {
                        sink.lock().unwrap().push(buffer[..bytes_read].to_vec());
                        let reply: &[u8] = if first { b"R\n" } else { ack };
                        first = false;
                        if connection.write_all(reply).await.is_err() {
                            return;
                        }
                    }
                }
            }
        });
        received
    }

    #[tokio::test(flavor = "current_thread")]
    async fn a_heartbeat_ack_roster_from_the_primary_updates_known_ring() {
        // Issue #61 end to end on the node side: the primary's ack names
        // two members, so this node — which never handed anything off and
        // so had no belief — adopts them and reports R on its next H.
        let primary = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let primary_addr = primary.local_addr().unwrap().to_string();
        let received = fake_discovery_acking_with(
            primary,
            b"A 2 3\n9 14\ntest-node127.0.0.1:8356\n6 14\nnode-b127.0.0.1:9002\n",
        );

        let known_ring: KnownRing = Arc::new(Mutex::new(None));
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let heartbeat_task = tokio::spawn(send_heartbeats(
            HeartbeatConfig {
                discovery_addrs: vec![primary_addr],
                port: 8356,
                interval: Duration::from_millis(20),
                auth_secret: None,
                tls_connector: None,
            },
            test_node_context(
                "test-node",
                "tk-test-node",
                Arc::clone(&known_ring),
                Arc::new(Mutex::new(None)),
            ),
            mpsc::channel::<RereplicationTask>(4).1,
            shutdown_rx,
        ));
        sleep(Duration::from_millis(150)).await;
        shutdown_tx.send_replace(true);
        heartbeat_task.await.unwrap();

        assert_eq!(
            member_names(&known_ring),
            Some((vec!["node-b".to_string(), "test-node".to_string()], 3))
        );
        let received = received.lock().unwrap();
        assert!(
            received
                .iter()
                .any(|message| message.starts_with(b"H 9 3 12\n")),
            "expected a heartbeat reporting the adopted replication factor, got {received:?}"
        );
    }

    /// Issue #266: a fake discovery server serving both protocols
    /// `spawn_or_supersede_rereplication` needs on the same listener — the
    /// persistent `J`-then-`H` heartbeat connection (first ack names all
    /// three members; every ack after that drops `evicted_name`,
    /// simulating an eviction) and a one-shot `L` roster-with-addresses
    /// fetch on a fresh connection.
    fn spawn_heartbeat_and_roster_discovery(
        listener: TcpListener,
        self_name: &'static str,
        survivor_name: &'static str,
        survivor_addr: String,
        evicted_name: &'static str,
        replication: usize,
    ) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            loop {
                let Ok((mut connection, _)) = listener.accept().await else {
                    return;
                };
                let survivor_addr = survivor_addr.clone();
                tokio::spawn(async move {
                    let mut buf = Vec::new();
                    let mut chunk = [0u8; 1024];

                    async fn read_line(
                        connection: &mut TcpStream,
                        buf: &mut Vec<u8>,
                        chunk: &mut [u8],
                    ) -> Option<String> {
                        loop {
                            if let Some(pos) = buf.iter().position(|byte| *byte == b'\n') {
                                let line = String::from_utf8(buf[..pos].to_vec()).ok()?;
                                buf.drain(..=pos);
                                return Some(line);
                            }
                            let bytes_read = connection.read(chunk).await.ok()?;
                            if bytes_read == 0 {
                                return None;
                            }
                            buf.extend_from_slice(&chunk[..bytes_read]);
                        }
                    }

                    async fn read_body(
                        connection: &mut TcpStream,
                        buf: &mut Vec<u8>,
                        chunk: &mut [u8],
                        len: usize,
                    ) -> Option<()> {
                        while buf.len() < len {
                            let bytes_read = connection.read(chunk).await.ok()?;
                            if bytes_read == 0 {
                                return None;
                            }
                            buf.extend_from_slice(&chunk[..bytes_read]);
                        }
                        buf.drain(..len);
                        Some(())
                    }

                    // Issue #295: `T`'s roster-with-tokens response shape
                    // — `<name-len> <addr-len> <token-len>\n<name><addr>
                    // <token>\n` per entry, unlike the token-free `L`.
                    fn roster_response(
                        members: &[(&str, &str, &str)],
                        replication: usize,
                    ) -> Vec<u8> {
                        let mut response =
                            format!("N {} {replication}\n", members.len()).into_bytes();
                        for (name, addr, token) in members {
                            response.extend_from_slice(
                                format!("{} {} {}\n", name.len(), addr.len(), token.len())
                                    .as_bytes(),
                            );
                            response.extend_from_slice(name.as_bytes());
                            response.extend_from_slice(addr.as_bytes());
                            response.extend_from_slice(token.as_bytes());
                            response.push(b'\n');
                        }
                        response
                    }

                    fn ack_response(members: &[(&str, &str)], replication: usize) -> Vec<u8> {
                        let mut ack = format!("A {} {replication}\n", members.len()).into_bytes();
                        for (name, addr) in members {
                            ack.extend_from_slice(
                                format!("{} {}\n", name.len(), addr.len()).as_bytes(),
                            );
                            ack.extend_from_slice(name.as_bytes());
                            ack.extend_from_slice(addr.as_bytes());
                            ack.push(b'\n');
                        }
                        ack
                    }

                    let Some(header) = read_line(&mut connection, &mut buf, &mut chunk).await
                    else {
                        return;
                    };

                    if let Some(rest) = header.strip_prefix("T ") {
                        // Issue #295: `T <name-len> <token-len>\n<name>
                        // <token>` — the self-authenticated roster+token
                        // fetch `fetch_roster_once` sends in place of `L`.
                        // This fake doesn't bother validating the
                        // presented identity (there's nothing to check it
                        // against here), just consumes the body and
                        // answers with the roster.
                        let mut fields = rest.split(' ');
                        let name_len: usize = fields.next().unwrap().parse().unwrap();
                        let token_len: usize = fields.next().unwrap().parse().unwrap();
                        if read_body(&mut connection, &mut buf, &mut chunk, name_len + token_len)
                            .await
                            .is_none()
                        {
                            return;
                        }
                        let members = [
                            (self_name, "127.0.0.1:1", "tok-self"),
                            (survivor_name, survivor_addr.as_str(), "tok-survivor"),
                        ];
                        let _ = connection
                            .write_all(&roster_response(&members, replication))
                            .await;
                        return;
                    }

                    // "J <name-len> <port> <token-len>"
                    let mut fields = header.split(' ');
                    assert_eq!(fields.next(), Some("J"), "expected a J registration");
                    let name_len: usize = fields.next().unwrap().parse().unwrap();
                    let _port: usize = fields.next().unwrap().parse().unwrap();
                    let token_len: usize = fields.next().unwrap().parse().unwrap();
                    if read_body(&mut connection, &mut buf, &mut chunk, name_len + token_len)
                        .await
                        .is_none()
                    {
                        return;
                    }
                    if connection.write_all(b"R\n").await.is_err() {
                        return;
                    }

                    let mut heartbeats = 0usize;
                    loop {
                        let Some(header) = read_line(&mut connection, &mut buf, &mut chunk).await
                        else {
                            return;
                        };
                        let mut fields = header.split(' ');
                        assert_eq!(fields.next(), Some("H"), "expected an H heartbeat");
                        let name_len: usize = fields.next().unwrap().parse().unwrap();
                        let _replication_belief: usize = fields.next().unwrap().parse().unwrap();
                        let token_len: usize = fields.next().unwrap().parse().unwrap();
                        if read_body(&mut connection, &mut buf, &mut chunk, name_len + token_len)
                            .await
                            .is_none()
                        {
                            return;
                        }

                        heartbeats += 1;
                        let ack = if heartbeats == 1 {
                            // The full, pre-eviction roster.
                            ack_response(
                                &[
                                    (self_name, "127.0.0.1:1"),
                                    (survivor_name, "127.0.0.1:1"),
                                    (evicted_name, "127.0.0.1:1"),
                                ],
                                replication,
                            )
                        } else {
                            // Issue #266's trigger: `evicted_name` is gone.
                            ack_response(
                                &[(self_name, "127.0.0.1:1"), (survivor_name, "127.0.0.1:1")],
                                replication,
                            )
                        };
                        if connection.write_all(&ack).await.is_err() {
                            return;
                        }
                    }
                });
            }
        })
    }

    /// Issue #266: records every `U` frame a re-replication sends,
    /// asserting each one carries the trailing `A` (put-if-absent) token
    /// and acking `S\n` like a real receiver would for an absent key.
    fn spawn_rereplication_recording_peer(
        listener: TcpListener,
    ) -> (RecordedFrames, tokio::task::JoinHandle<()>) {
        let frames: RecordedFrames = Arc::new(std::sync::Mutex::new(Vec::new()));
        let recorded = Arc::clone(&frames);
        let task = tokio::spawn(async move {
            loop {
                let Ok((mut connection, _)) = listener.accept().await else {
                    return;
                };
                let recorded = Arc::clone(&recorded);
                tokio::spawn(async move {
                    let mut buf = BytesMut::new();
                    loop {
                        let Some(header_end) = buf.iter().position(|byte| *byte == b'\n') else {
                            let mut chunk = [0u8; 1024];
                            let Ok(bytes_read) = connection.read(&mut chunk).await else {
                                return;
                            };
                            if bytes_read == 0 {
                                return;
                            }
                            buf.extend_from_slice(&chunk[..bytes_read]);
                            continue;
                        };
                        let header = String::from_utf8(buf[..header_end].to_vec()).unwrap();
                        assert!(header.starts_with("U "), "unexpected frame {header:?}");
                        let mut fields = header.split(' ').skip(1);
                        let ns_len: usize = fields.next().unwrap().parse().unwrap();
                        let key_len: usize = fields.next().unwrap().parse().unwrap();
                        let val_len: usize = fields.next().unwrap().parse().unwrap();
                        // Issue #295: `<token-len>` — the body now leads
                        // with that many bytes of token too.
                        let token_len: usize = fields.next().unwrap().parse().unwrap();
                        let has_absent = header.ends_with(" A");
                        let body_len = ns_len + key_len + val_len + token_len;
                        let frame_end = header_end + 1 + body_len;
                        while buf.len() < frame_end {
                            let mut chunk = [0u8; 1024];
                            let Ok(bytes_read) = connection.read(&mut chunk).await else {
                                return;
                            };
                            if bytes_read == 0 {
                                return;
                            }
                            buf.extend_from_slice(&chunk[..bytes_read]);
                        }
                        let frame = buf.split_to(frame_end).to_vec();
                        assert!(
                            has_absent,
                            "expected the put-if-absent marker on a re-replication send: \
                             {frame:?}"
                        );
                        recorded.lock().unwrap().push(frame);
                        let _ = connection.write_all(b"S\n").await;
                    }
                });
            }
        });
        (frames, task)
    }

    #[tokio::test(flavor = "current_thread")]
    async fn a_dropped_roster_member_triggers_rereplication_to_exactly_the_right_keys() {
        // Issue #266 end to end: a 3-member ring (self "n1", survivor
        // "n2", evicted "n3", R=2) whose heartbeat ack drops "n3" on its
        // second tick. Every key for which "n1" is the elected sender
        // (see `rereplication_targets`) must arrive at "n2" as `U … A`;
        // every other seeded key must not.
        let (request_tx, request_rx) = mpsc::channel(16);
        let cache_task = tokio::spawn(run_cache(request_rx, MAX_CACHE_MEMORY_BYTES, Vec::new()));

        const KEY_COUNT: u32 = 60;
        for index in 0..KEY_COUNT {
            send_command(
                &request_tx,
                Command::Set {
                    key: key(format!("key-{index}").as_bytes()),
                    value: Bytes::from_static(b"v"),
                    ttl: None,
                },
            )
            .await;
        }

        let replication = 2;
        let before_ring = HashRing::new(vec!["n1".to_string(), "n2".to_string(), "n3".to_string()]);
        let after_ring = HashRing::new(vec!["n1".to_string(), "n2".to_string()]);
        let expected_keys: Vec<Key> = (0..KEY_COUNT)
            .map(|index| key(format!("key-{index}").as_bytes()))
            .filter(|key| {
                !rereplication_targets(&before_ring, &after_ring, key, replication, "n1").is_empty()
            })
            .collect();
        assert!(
            !expected_keys.is_empty(),
            "the sample must contain at least one key n1 re-replicates"
        );
        assert!(
            expected_keys.len() < KEY_COUNT as usize,
            "the sample must also contain at least one key n1 does not re-replicate"
        );

        let peer_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let peer_addr = peer_listener.local_addr().unwrap().to_string();
        let (frames, peer_task) = spawn_rereplication_recording_peer(peer_listener);

        let discovery_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let discovery_addr = discovery_listener.local_addr().unwrap().to_string();
        let discovery_task = spawn_heartbeat_and_roster_discovery(
            discovery_listener,
            "n1",
            "n2",
            peer_addr,
            "n3",
            replication,
        );

        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        // Issue #266: must be the receiving half of the exact sender
        // `node_context.rereplication_tx` uses below — a mismatched pair
        // (each with its own, disconnected channel) would make every
        // `register_with_discovery` trigger fail silently with "channel
        // closed", never reaching `send_heartbeats`'s `JoinSet` at all.
        let (rereplication_tx, rereplication_rx) = mpsc::channel::<RereplicationTask>(4);

        let node_context = NodeContext {
            name: "n1".to_string(),
            token: "tk-n1".to_string(),
            discovery_addr: discovery_addr.clone(),
            active_migration: Arc::new(Mutex::new(None)),
            known_ring: Arc::new(Mutex::new(None)),
            auth_secret: None,
            tls_connector: None,
            request_tx: request_tx.clone(),
            leaving: Arc::new(Mutex::new(None)),
            active_rereplication: Arc::new(Mutex::new(None)),
            rereplication_tx,
            shutdown_rx: shutdown_rx.clone(),
        };

        // Moved, not cloned: a lingering clone in this test's own scope
        // would keep `rereplication_tx` alive past every task that
        // actually uses it, so `send_heartbeats`'s own drain of
        // `rereplication_rx` would never see the channel close and
        // `heartbeat_task.await` below would hang forever (`run`'s own
        // shutdown avoids this the same way — see its `drop(node_context)`
        // ahead of `heartbeat_task.await`).
        let heartbeat_task = tokio::spawn(send_heartbeats(
            HeartbeatConfig {
                discovery_addrs: vec![discovery_addr],
                port: 8356,
                interval: Duration::from_millis(20),
                auth_secret: None,
                tls_connector: None,
            },
            node_context,
            rereplication_rx,
            shutdown_rx,
        ));

        // Poll for the expected frame count instead of a fixed sleep —
        // the eviction lands on the second heartbeat tick (~20-40ms in),
        // and the re-replication itself still has to list, peek, and
        // send every matching key.
        let mut observed = 0;
        for _ in 0..500 {
            observed = frames.lock().unwrap().len();
            if observed >= expected_keys.len() {
                break;
            }
            sleep(Duration::from_millis(20)).await;
        }

        shutdown_tx.send_replace(true);
        heartbeat_task.await.unwrap();
        discovery_task.abort();
        peer_task.abort();

        let frames = frames.lock().unwrap().clone();
        assert_eq!(
            frames.len(),
            expected_keys.len(),
            "expected exactly the keys n1 elects itself sender for, got {observed} of {} \
             polled, frames: {frames:?}",
            expected_keys.len()
        );

        let mut sent_keys: Vec<Key> = frames
            .iter()
            .map(|frame| {
                // Issue #295: `U <ns-len> <key-len> <val-len> <token-len>
                // [ttl] A\n<token><ns><key><value>` — the body now leads
                // with `token`.
                let header_end = frame.iter().position(|byte| *byte == b'\n').unwrap();
                let header = String::from_utf8(frame[..header_end].to_vec()).unwrap();
                let mut fields = header.split(' ').skip(1);
                let ns_len: usize = fields.next().unwrap().parse().unwrap();
                let key_len: usize = fields.next().unwrap().parse().unwrap();
                let _val_len: usize = fields.next().unwrap().parse().unwrap();
                let token_len: usize = fields.next().unwrap().parse().unwrap();
                let ns_start = header_end + 1 + token_len;
                Key::new(
                    Bytes::copy_from_slice(&frame[ns_start..ns_start + ns_len]),
                    Bytes::copy_from_slice(&frame[ns_start + ns_len..ns_start + ns_len + key_len]),
                )
            })
            .collect();
        sent_keys.sort_by(|a, b| a.name.cmp(&b.name));
        let mut expected_sorted = expected_keys.clone();
        expected_sorted.sort_by(|a, b| a.name.cmp(&b.name));
        assert_eq!(sent_keys, expected_sorted);

        drop(request_tx);
        cache_task.await.unwrap();
    }

    /// Issue #275 test helper: a fake cache task that answers every
    /// `ListEntries` request with an empty key list, except the first
    /// one, which it holds open (notifying `first_list_seen` that it
    /// has arrived, then blocking on `release_first_list`) so a test
    /// can deterministically observe a `run_rereplication` mid-flight —
    /// wedged inside its very first `.await` — without any timing-based
    /// sleep.
    async fn fake_cache_that_pauses_the_first_list(
        mut request_rx: mpsc::Receiver<CacheRequest>,
        first_list_seen: Arc<tokio::sync::Notify>,
        release_first_list: Arc<tokio::sync::Notify>,
    ) {
        let mut first = true;
        while let Some(request) = request_rx.recv().await {
            if matches!(request.command, Command::ListEntries) && first {
                first = false;
                first_list_seen.notify_one();
                release_first_list.notified().await;
            }
            let _ = request.response_tx.send(Response::Keys(Vec::new()));
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn a_superseding_trigger_chains_behind_the_run_it_displaces_instead_of_racing_it() {
        // Issue #275 regression, end to end. The old
        // `run_superseding_rereplication` read `active_rereplication`
        // and installed its own state in two separate lock
        // acquisitions with an `.await` in between — two triggers
        // landing in that window could both read the same `previous`
        // and race to install, leaving the loser's run executing with
        // nothing left pointing at it to abort.
        //
        // This drives the fixed function through exactly that
        // scenario: the first run is deliberately wedged inside its
        // own `list_keys` call (which can only happen *after* it has
        // installed its state — the install is synchronous and
        // precedes every `.await` in the function), then a second
        // trigger runs concurrently. It must find the first run's own
        // state as `previous` (proven by observing it signal that
        // state's `abort_requested`), install its own state in the
        // same slot, and only then wait — never race the first trigger
        // to install.
        let (request_tx, request_rx) = mpsc::channel(4);
        let first_list_seen = Arc::new(tokio::sync::Notify::new());
        let release_first_list = Arc::new(tokio::sync::Notify::new());
        let cache_task = tokio::spawn(fake_cache_that_pauses_the_first_list(
            request_rx,
            Arc::clone(&first_list_seen),
            Arc::clone(&release_first_list),
        ));

        let known_ring: KnownRing = Arc::new(Mutex::new(None));
        let mut node_context = test_node_context(
            "n1",
            "tk-n1",
            Arc::clone(&known_ring),
            Arc::new(Mutex::new(None)),
        );
        node_context.request_tx = request_tx;

        let ring_a = Arc::new(HashRing::new(vec!["n1".to_string(), "n2".to_string()]));
        let ring_b = Arc::new(HashRing::new(vec![
            "n1".to_string(),
            "n2".to_string(),
            "n3".to_string(),
        ]));

        let first = tokio::spawn(run_superseding_rereplication(
            node_context.clone(),
            Arc::clone(&ring_a),
            Arc::clone(&ring_b),
            2,
            HashMap::new(),
            HashMap::new(),
            node_context.shutdown_rx.clone(),
        ));

        // The first run can only reach here after installing its own
        // state in the slot.
        first_list_seen.notified().await;
        let first_state = node_context
            .active_rereplication
            .lock()
            .unwrap()
            .clone()
            .expect("the first run should have installed its state by now");

        let second = tokio::spawn(run_superseding_rereplication(
            node_context.clone(),
            Arc::clone(&ring_a),
            Arc::clone(&ring_b),
            2,
            HashMap::new(),
            HashMap::new(),
            node_context.shutdown_rx.clone(),
        ));

        // Let the second trigger run up to (and into) its own wait —
        // on this single-threaded runtime, yielding gives it the
        // chance to install its state and signal the first run's
        // `abort_requested` before we inspect either.
        for _ in 0..8 {
            tokio::task::yield_now().await;
        }

        assert!(
            first_state.abort_requested.load(Ordering::SeqCst),
            "the second trigger must have found the first run's own state as its `previous` \
             and signalled it to abort — not raced past it to install its own state \
             independently"
        );
        let current = node_context.active_rereplication.lock().unwrap().clone();
        assert!(
            current.is_some() && !Arc::ptr_eq(current.as_ref().unwrap(), &first_state),
            "the second trigger must have already taken over the slot with its own state"
        );

        release_first_list.notify_one();
        first.await.unwrap();
        second.await.unwrap();

        assert!(
            first_state.done.load(Ordering::SeqCst),
            "the superseded first run must still complete and mark itself done"
        );
        assert!(
            node_context.active_rereplication.lock().unwrap().is_none(),
            "the slot must be cleared once the last (second, winning) run finishes"
        );

        drop(node_context);
        cache_task.await.unwrap();
    }

    #[test]
    fn take_over_rereplication_slot_never_hands_the_same_previous_to_two_callers() {
        // Issue #275 regression, at the primitive itself: the bug was
        // that reading the slot's previous occupant and writing the
        // new one were two separate lock acquisitions, so two callers
        // racing in that window could both read the same `previous`.
        // Fire many real, concurrently-running OS threads at
        // `take_over_rereplication_slot` and confirm no two of them
        // are ever handed the same `previous` to chain behind — each
        // caller's `previous` must be either genuinely absent (at most
        // once, for whichever caller's swap happens first) or a
        // distinct predecessor no other caller also saw.
        let slot: Arc<Mutex<Option<Arc<ActiveRereplication>>>> = Arc::new(Mutex::new(None));

        const CALLERS: usize = 32;
        let states: Vec<Arc<ActiveRereplication>> = (0..CALLERS)
            .map(|_| {
                Arc::new(ActiveRereplication {
                    abort_requested: AtomicBool::new(false),
                    done: AtomicBool::new(false),
                })
            })
            .collect();

        let barrier = Arc::new(std::sync::Barrier::new(CALLERS));
        let handles: Vec<_> = states
            .iter()
            .cloned()
            .map(|state| {
                let slot = Arc::clone(&slot);
                let barrier = Arc::clone(&barrier);
                std::thread::spawn(move || {
                    barrier.wait();
                    take_over_rereplication_slot(&slot, &state)
                })
            })
            .collect();

        let previous: Vec<Option<Arc<ActiveRereplication>>> = handles
            .into_iter()
            .map(|handle| handle.join().unwrap())
            .collect();

        let none_count = previous.iter().filter(|entry| entry.is_none()).count();
        assert_eq!(
            none_count, 1,
            "exactly one caller should see an empty slot; got {none_count} — two callers raced \
             to read the same (missing) previous"
        );

        for (i, a) in previous.iter().enumerate() {
            let Some(a) = a else { continue };
            for (j, b) in previous.iter().enumerate() {
                if i == j {
                    continue;
                }
                let Some(b) = b else { continue };
                assert!(
                    !Arc::ptr_eq(a, b),
                    "two callers were both handed the same previous state — the exact issue \
                     #275 race: both would abort/wait on it and then race to install their own \
                     state"
                );
            }
        }

        for previous in previous.iter().flatten() {
            assert!(
                states.iter().any(|state| Arc::ptr_eq(state, previous)),
                "a caller was handed a previous state that was never installed by this test"
            );
        }

        let never_previous = states
            .iter()
            .filter(|state| {
                !previous
                    .iter()
                    .any(|entry| entry.as_ref().is_some_and(|prev| Arc::ptr_eq(prev, state)))
            })
            .count();
        assert_eq!(
            never_previous, 1,
            "exactly one installed state should remain unclaimed as anyone's previous — the \
             winner still occupying the slot"
        );
        let remaining = slot.lock().unwrap();
        assert!(
            remaining
                .as_ref()
                .is_some_and(|current| states.iter().any(|state| Arc::ptr_eq(state, current))),
            "the slot should still hold one of this test's states"
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn a_heartbeat_ack_roster_from_a_standby_is_ignored() {
        // Discovery HA: replicas never reconcile, so only the primary's
        // view is adopted — a standby reporting a roster changes nothing.
        let primary = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let primary_addr = primary.local_addr().unwrap().to_string();
        let _primary_received = fake_discovery_acking_with(primary, b"A\n");
        let standby = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let standby_addr = standby.local_addr().unwrap().to_string();
        let standby_received =
            fake_discovery_acking_with(standby, b"A 1 3\n9 14\ntest-node127.0.0.1:8356\n");

        let known_ring: KnownRing = Arc::new(Mutex::new(None));
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let heartbeat_task = tokio::spawn(send_heartbeats(
            HeartbeatConfig {
                discovery_addrs: vec![primary_addr, standby_addr],
                port: 8356,
                interval: Duration::from_millis(20),
                auth_secret: None,
                tls_connector: None,
            },
            test_node_context(
                "test-node",
                "tk-test-node",
                Arc::clone(&known_ring),
                Arc::new(Mutex::new(None)),
            ),
            mpsc::channel::<RereplicationTask>(4).1,
            shutdown_rx,
        ));
        sleep(Duration::from_millis(150)).await;
        shutdown_tx.send_replace(true);
        heartbeat_task.await.unwrap();

        assert!(
            standby_received.lock().unwrap().len() > 1,
            "the standby should have seen heartbeats"
        );
        assert!(known_ring.lock().unwrap().is_none());
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
            test_node_context(
                "test-node",
                "tk-test-node",
                Arc::clone(&known_ring),
                Arc::new(Mutex::new(None)),
            ),
            mpsc::channel::<RereplicationTask>(4).1,
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
            test_node_context(
                "test-node",
                "tk-test-node",
                Arc::new(Mutex::new(None)),
                Arc::new(Mutex::new(None)),
            ),
            mpsc::channel::<RereplicationTask>(4).1,
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
            test_node_context(
                "test-node",
                "tk-test-node",
                Arc::new(Mutex::new(None)),
                Arc::new(Mutex::new(None)),
            ),
            mpsc::channel::<RereplicationTask>(4).1,
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
            test_node_context(
                "test-node",
                "tk-test-node",
                Arc::new(Mutex::new(None)),
                Arc::new(Mutex::new(None)),
            ),
            mpsc::channel::<RereplicationTask>(4).1,
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
            test_node_context(
                "test-node",
                "tk-test-node",
                Arc::new(Mutex::new(None)),
                Arc::new(Mutex::new(None)),
            ),
            mpsc::channel::<RereplicationTask>(4).1,
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
        let cache_task = tokio::spawn(run_cache(request_rx, MAX_CACHE_MEMORY_BYTES, Vec::new()));
        let connection_limit = Arc::new(Semaphore::new(1));
        let per_ip_connections: PerIpConnections = Arc::new(Mutex::new(HashMap::new()));
        let (_shutdown_tx, shutdown_rx) = watch::channel(false);

        let config = ConnectionConfig {
            idle_timeout: IDLE_TIMEOUT,
            auth_secret: None,
            tls_acceptor: Some(acceptor),
            node_context: None,
            migration_tx: mpsc::channel(1).0,
            forward_tx: mpsc::channel(1).0,
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
                DEFAULT_MAX_CONNECTIONS_PER_IP,
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
            test_node_context(
                "test-node",
                "tk-test-node",
                Arc::new(Mutex::new(None)),
                Arc::new(Mutex::new(None)),
            ),
            mpsc::channel::<RereplicationTask>(4).1,
            shutdown_rx,
        ));

        sleep(Duration::from_millis(150)).await;

        shutdown_tx.send_replace(true);
        heartbeat_task.await.unwrap();
        fake_discovery.abort();

        assert_join_then_heartbeats(&received.lock().unwrap(), 8356);
    }
}
