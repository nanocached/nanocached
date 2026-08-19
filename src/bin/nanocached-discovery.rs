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
//!   H <name-length> <r>\n<name>   Heartbeat, identified by name (a random
//!                             per-process identity, ADR-0009 — not the
//!                             node's address, which carries no identity
//!                             meaning and was already established by
//!                             `J` on this connection). Only valid for a
//!                             node already `Joined` (see below) —
//!                             refreshes its liveness. `r` is the
//!                             replication factor this node currently
//!                             believes (issue #30) — learned from a `M`
//!                             this node has sent as an ADR-0011 handoff
//!                             source, or `0` if it hasn't sent one yet
//!                             and so has no belief to report. When `r` is
//!                             nonzero and disagrees with this replica's
//!                             own `--replication-factor`, the mismatch is
//!                             logged loudly (not rejected — see ADR-0010,
//!                             replicas never reconcile configuration with
//!                             each other). Response: `A\n`.
//!
//!   L\n                       List currently `Joined` nodes. Response:
//!                             `N <count>\n` followed by `count` lines,
//!                             each `<name-length> <addr-length>\n
//!                             <name><addr>` — `name` (ADR-0009) is what
//!                             hash-ring computations use; `addr` is only
//!                             for opening a connection.
//!
//!   J <name-length> <addr-length>\n<name><addr>   Ask to join (ADR-0008),
//!                             declaring the node's own name (ADR-0009)
//!                             and advertised address. Sent once; the
//!                             connection is then held open (no idle
//!                             timeout applies) rather than closed or
//!                             reused for anything else, since this is
//!                             the node's only channel for learning about
//!                             a state change it didn't itself cause.
//!                             When the node is promoted to `Joined`,
//!                             discovery pushes `R\n` on this same
//!                             connection, which then becomes that node's
//!                             ordinary heartbeat connection (`H` from
//!                             here on).
//!
//!   P <name-length> <addr-length>\n<name><addr>   Announce (ADR-0010): an
//!                             already-promoted node (re-)declaring "I am a
//!                             `Joined` member at this address" — after a
//!                             heartbeat connection broke, after this
//!                             process restarted with an empty registry, or
//!                             to a standby replica it never `J`ed with.
//!                             Upserts the node straight to `Joined` with no
//!                             ADR-0008 handoff. Response: `R\n`, after
//!                             which the connection carries `H` heartbeats,
//!                             exactly like a `J` connection after
//!                             promotion. Rejected for a name currently
//!                             mid-join (`Waiting`/`Joining`).
//!
//!   C <name-length> <joining-length>\n<name><joining>   Sent by an
//!                             already-`Joined` node to report it has
//!                             finished handing its share of the keyspace
//!                             off to `joining` (the joining node's name).
//!                             Naming the join it is for keeps a stale
//!                             report from an abandoned handoff from being
//!                             credited to whatever join is pending next.
//!                             Ignored unless `joining` matches the
//!                             in-progress join. Response: `A\n`.
//!
//!   A <secret-length>\n<secret>   Authenticate. Response: `Od\n` on success,
//!                             `Ed\n` (then the connection closes) if a
//!                             secret is configured and this doesn't match
//!                             it. If no secret is configured (the
//!                             `NANOCACHED_AUTH_SECRET` environment
//!                             variable is unset or empty), this is a
//!                             no-op that always succeeds. If a secret is
//!                             configured, every other command is rejected
//!                             with `Ed\n` until a matching `A` has been
//!                             sent on the connection. The `d` distinguishes
//!                             this response from nanocached-node's own
//!                             `On\n`/`En\n`, letting a client tell the two
//!                             apart from the response to the same `A`
//!                             request without knowing in advance which it
//!                             dialed.
//!
//! If the connection limit has been reached, the server responds with
//! `B\n` and closes the connection instead of accepting the command. `L`
//! is answered the same way (`B\n`, connection closed) during the startup
//! grace period (ADR-0010, one liveness-timeout long): after a restart the
//! registry re-fills from `P` announces within about one heartbeat
//! interval, and until the grace has passed a fresh client must not build
//! a ring from the partial list. All other commands work during the grace
//! — recovery itself depends on them.
//!
//! A node moves through three states (ADR-0008): `Waiting` (registered via
//! `J`, but either another join is already in progress or its handoff
//! hasn't started), `Joining` (actively receiving its handoff from every
//! `Joined` node), and `Joined` (handoff complete, included in `L`
//! responses, now heartbeating normally via `H`). Only `Joined` nodes are
//! visible to clients; `Waiting`/`Joining` nodes are excluded from `L`
//! exactly like a node that was never registered. Only one node moves
//! through `Waiting` -> `Joining` at a time. A `Waiting`/`Joining` node has
//! no heartbeat to time out — its liveness is tied to the one connection
//! it opened with `J`, which discovery holds open the whole time; the
//! registry entry is dropped if that connection dies before promotion. A
//! `Joined` node that stops sending heartbeats is dropped once
//! `--liveness-timeout` has elapsed since its last heartbeat; no explicit
//! "leave" message is required, so this covers both graceful shutdown and
//! crashes. Because the registry is rebuilt from `P` announces (ADR-0010)
//! within about one heartbeat interval, this process can be restarted at
//! any time and self-heals with no data movement, modulo any join in
//! progress at the time (not yet designed — see `doc/adr/0008-*.md`); it
//! can also run as several independent replicas that each converge on the
//! same registry by listening to the same nodes.

use bytes::{Bytes, BytesMut};
use rustc_hash::FxHashMap;
use rustls::pki_types::{CertificateDer, PrivateKeyDer, ServerName};
use rustls::{ClientConfig, RootCertStore, ServerConfig};
use std::collections::HashSet;
use std::io;
use std::io::BufReader;
use std::net::SocketAddr;
use std::pin::Pin;
use std::process::ExitCode;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use std::time::Duration;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, ReadBuf};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{Notify, Semaphore, watch};
use tokio::task::JoinSet;
use tokio::time::{Instant, interval, timeout};
use tokio_rustls::{TlsAcceptor, TlsConnector};

const READ_CHUNK_SIZE: usize = 256;
const MAX_REQUEST_SIZE: usize = 4096;
const MAX_CONNECTIONS: usize = 1024;
const DEFAULT_LIVENESS_TIMEOUT: Duration = Duration::from_secs(15);
/// ADR-0008 pattern-3 guard: a ready node can be alive (heartbeating
/// normally) yet never report `C` for a handoff it's mid-`Migrate` for —
/// no TCP-level signal distinguishes "legitimately still working" from
/// "stuck" (a lost ack, a bug), so this is a plain timeout. Past it,
/// `abandon_current_join` scraps the join and sends every ready node an
/// `X` to roll back, exactly as it does when a node's connection dies
/// outright (patterns 1/2). Size-derived rather than flat
/// (doc/adr/0017-*.md): a large, legitimate join shouldn't get reaped
/// just for being large, so the bound scales with the largest entry
/// count any ready node reported acknowledging its `M`
/// (`PendingJoin::max_entries`). Both constants are hardcoded, not
/// configurable — `--migration-timeout` no longer exists. See
/// `migration_timeout_for`.
const MIGRATION_TIMEOUT_BASE: Duration = Duration::from_secs(60);
const MIGRATION_TIMEOUT_PER_ENTRY: Duration = Duration::from_millis(5);

/// Saturates rather than overflows for a pathologically large count.
fn migration_timeout_for(max_entries: usize) -> Duration {
    MIGRATION_TIMEOUT_BASE
        + MIGRATION_TIMEOUT_PER_ENTRY.saturating_mul(max_entries.min(u32::MAX as usize) as u32)
}
const IDLE_TIMEOUT: Duration = Duration::from_secs(60);
const SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(10);
const TLS_HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(10);
/// Bounds every outbound dial and ack read this process makes toward a
/// node (`M`/`X`, issue #6): without it, one node that accepts TCP but
/// never answers freezes the single sweep task — and with it all
/// liveness eviction — and can hang shutdown indefinitely.
const OUTBOUND_IO_TIMEOUT: Duration = Duration::from_secs(10);
/// ADR-0008 known gap (issue #20): a ready node that never received `M` —
/// a transient connect/write/ack failure, not a rejection — never hands
/// off and never sweeps, stalling the join until `migration_timeout_for`
/// reaps it. Retrying the send absorbs exactly that class of hiccup,
/// mirroring `run_migration`'s own `KEY_TRANSFER_ATTEMPTS` on the node
/// side (same fixed-attempt-count, fresh-connection-each-time shape).
const MIGRATE_SEND_ATTEMPTS: u32 = 3;
const AUTH_SECRET_ENV_VAR: &str = "NANOCACHED_AUTH_SECRET";

/// A registered node's place in the ADR-0008 join lifecycle: `Waiting`
/// (registered, asked to join, but another join is already in progress)
/// -> `Joining` (actively receiving its handoff) -> `Joined` (handoff
/// complete, included in `L` responses, now heartbeating normally). There
/// is no separate "start up already joining" state — every node begins at
/// `Waiting` when it first sends `J`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum NodeState {
    Waiting,
    Joining,
    Joined,
}

struct NodeInfo {
    /// How to open a connection to this node (a client's `G`/`S`/`D`, or,
    /// per [[0008]], a ready node's handoff). Carries no identity meaning
    /// — see ADR-0009 — the registry's key (this node's random per-process
    /// name) is what hashing and lookups use.
    address: String,
    state: NodeState,
    /// Only meaningful (and only refreshed) once `state` is `Joined` —
    /// `Waiting`/`Joining` nodes hold one long-lived connection open
    /// instead of heartbeating, and are dropped when that connection dies
    /// rather than by `sweep_expired`'s liveness check.
    last_heartbeat: Instant,
    /// Fired (via `notify_one`, not `notify_waiters` — this must survive
    /// being sent before the connection task starts waiting on it, e.g.
    /// a bootstrap join that's promoted synchronously) when this node
    /// should move to `Joined`; the connection task holding this node's
    /// `J` connection open waits on it instead of polling.
    promoted: Arc<Notify>,
}

impl NodeInfo {
    fn new(address: String, state: NodeState) -> Self {
        Self {
            address,
            state,
            last_heartbeat: Instant::now(),
            promoted: Arc::new(Notify::new()),
        }
    }
}

/// Keyed by name (ADR-0009's random per-process node identity), not
/// address — see `NodeInfo::address`.
type Registry = Arc<Mutex<FxHashMap<String, NodeInfo>>>;

/// Tracks the single in-progress join (ADR-0008: only one node moves
/// through `Waiting` -> `Joining` at a time). `completed` accumulates the
/// names (ADR-0009) of ready nodes that have reported finishing their
/// handoff via `C`; once it covers all of `expected`, the joining node is
/// promoted. `started_at` backs the timeout that catches a ready node
/// that's alive but never reports in (see `abandon_current_join`,
/// `migration_timeout_for`), sized from `max_entries` — the largest
/// entry count any ready node has reported acknowledging its `M` so far
/// (doc/adr/0017-*.md), updated as each of `try_begin_next_join`'s
/// parallel sends resolves. Starts at 0 (the bound is just
/// `MIGRATION_TIMEOUT_BASE` until the first ack arrives).
struct PendingJoin {
    joining_name: String,
    expected: HashSet<String>,
    completed: HashSet<String>,
    started_at: Instant,
    max_entries: usize,
}

type CurrentJoin = Arc<Mutex<Option<PendingJoin>>>;

/// The node registry and ADR-0008 join-orchestration state, bundled since
/// every connection needs both and they're always threaded through
/// together (keeps `dispatch_connection`'s argument count down).
#[derive(Clone)]
struct ClusterState {
    registry: Registry,
    current_join: CurrentJoin,
}

/// Wraps either a plain TCP connection or one wrapped in TLS behind a
/// single type, so the rest of the connection-handling code doesn't need to
/// know which is in play. Generic over the plain (`P`) and TLS (`T`) stream
/// types since this process both accepts connections (`ServerStream`, TLS
/// terminated by `TlsAcceptor`) and, since ADR-0008 added `M`/`X`, also
/// opens its own outbound ones to nodes (`ClientStream`, TLS via
/// `TlsConnector`) — the two use different `tokio_rustls` stream types.
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
/// A connection this process opened outbound (to a node, sending `M`/`X`),
/// plaintext or TLS-secured.
type ClientStream = MaybeTls<TcpStream, tokio_rustls::client::TlsStream<TcpStream>>;

/// Loads a certificate chain and private key from PEM files and builds a
/// `TlsAcceptor` for terminating incoming TLS connections.
fn load_tls_acceptor(cert_path: &str, key_path: &str) -> io::Result<TlsAcceptor> {
    let certs = load_cert_chain(cert_path)?;
    let key = load_private_key(key_path)?;

    let config = ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(certs, key)
        .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;

    Ok(TlsAcceptor::from(Arc::new(config)))
}

/// Loads CA certificates from a PEM file and builds a `TlsConnector` that
/// trusts only those CAs (not the system trust store), for this process's
/// own outbound connections to a node's TLS-secured port (sending `M`/`X`).
fn load_tls_connector(ca_path: &str) -> io::Result<TlsConnector> {
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

/// Connects to `addr` (a node, to send `M`/`X`), upgrading to TLS first if
/// `tls_connector` is set. There is no plaintext fallback: if TLS is
/// configured and the handshake fails, the connection attempt fails too —
/// mirrors `nanocached-node`'s own `connect_client_stream`.
async fn connect_client_stream(
    addr: &str,
    tls_connector: Option<&TlsConnector>,
) -> io::Result<ClientStream> {
    let stream = timeout(OUTBOUND_IO_TIMEOUT, TcpStream::connect(addr))
        .await
        .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "connect timed out"))??;
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

/// Per-connection settings that don't change once `run` starts, grouped so
/// `dispatch_connection`/`handle_connection` take one value instead of two.
#[derive(Clone)]
struct ConnectionConfig {
    idle_timeout: Duration,
    /// ADR-0010 startup grace: until this instant, `L` answers `B\n`
    /// instead of a node list — after a restart the registry re-fills from
    /// announces within about one heartbeat interval, and serving the
    /// partial list to a bootstrapping client would hand it a wrong ring.
    list_ready_at: Instant,
    /// ADR-0011: the replication factor this process distributes (see
    /// `Args::replication_factor`).
    replication: usize,
    auth_secret: Option<Bytes>,
    /// When set, every accepted connection must complete a TLS handshake
    /// before speaking the protocol; there is no plaintext fallback.
    tls_acceptor: Option<TlsAcceptor>,
    /// When set, this process's own outbound connections to a node
    /// (sending `M`/`X`) upgrade to TLS; see `connect_client_stream`.
    tls_connector: Option<TlsConnector>,
}

/// Reads the shared auth secret from the environment rather than a CLI
/// flag, since CLI arguments are visible to anyone who can list processes
/// (e.g. `ps`) on the host. An unset or empty value means auth is not
/// required.
fn read_auth_secret() -> Option<Bytes> {
    std::env::var(AUTH_SECRET_ENV_VAR)
        .ok()
        .filter(|secret| !secret.is_empty())
        .map(Bytes::from)
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

struct Args {
    host: String,
    port: u16,
    liveness_timeout: Duration,
    /// ADR-0010: how long after startup `L` keeps answering `B\n` while
    /// the registry re-fills from announces. `None` (the default) means
    /// "same as the liveness timeout".
    /// ADR-0011: the cluster's replication factor R — how many nodes hold
    /// each key. This process is R's single source of truth: clients learn
    /// it from `L`, nodes from `M`.
    replication_factor: usize,
    tls_cert: Option<String>,
    tls_key: Option<String>,
    tls_ca: Option<String>,
}

impl Default for Args {
    fn default() -> Self {
        Self {
            host: "127.0.0.1".to_string(),
            port: 8357,
            liveness_timeout: DEFAULT_LIVENESS_TIMEOUT,
            replication_factor: 2,
            tls_cert: None,
            tls_key: None,
            tls_ca: None,
        }
    }
}

/// Distinguishes `-h`/`--help` (a normal request, printed to stdout with
/// exit code 0) from an actual parsing error (printed to stderr with exit
/// code 1) — both previously took the same `Err` path, so `--help` looked
/// like a failure to shell scripts and CLI conventions alike.
enum ArgsError {
    Help(String),
    Invalid(String),
}

impl From<String> for ArgsError {
    fn from(message: String) -> Self {
        ArgsError::Invalid(message)
    }
}

fn parse_args() -> Result<Args, ArgsError> {
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
            "--replication-factor" => {
                let raw = value()?;
                let factor: usize = raw
                    .parse()
                    .map_err(|_| format!("invalid value for --replication-factor: {raw}"))?;
                if factor == 0 {
                    return Err(ArgsError::Invalid(
                        "--replication-factor must be at least 1".to_string(),
                    ));
                }
                args.replication_factor = factor;
            }
            "--tls-cert" => args.tls_cert = Some(value()?),
            "--tls-key" => args.tls_key = Some(value()?),
            "--tls-ca" => args.tls_ca = Some(value()?),
            "-h" | "--help" => return Err(ArgsError::Help(usage())),
            other => {
                return Err(ArgsError::Invalid(format!(
                    "unknown flag: {other}\n\n{}",
                    usage()
                )));
            }
        }
    }

    if args.tls_cert.is_some() != args.tls_key.is_some() {
        return Err(ArgsError::Invalid(
            "--tls-cert and --tls-key must be set together".to_string(),
        ));
    }

    Ok(args)
}

fn usage() -> String {
    "\
Usage: nanocached-discovery [options]

  --host <addr>                 bind address (default 127.0.0.1)
  --port <port>                 bind port (default 8357)
  --liveness-timeout <secs>     drop a node after this many seconds without
                                 a heartbeat (default 15)
  --replication-factor <n>      how many nodes hold each key (ADR-0011);
                                 distributed to clients via L and to nodes
                                 via M (default 2, min 1)
  --tls-cert <path>             PEM certificate chain; requires TLS on
                                 every accepted connection (no plaintext
                                 fallback)
  --tls-key <path>              PEM private key matching --tls-cert
  --tls-ca <path>               PEM CA certificate(s) to trust when this
                                 process connects out to a TLS-secured node
                                 to send M/X"
        .to_string()
}

#[derive(Debug, PartialEq, Eq)]
enum DiscoveryCommand {
    Auth(Bytes),
    /// A refresh from an already-`Joined` node, identified by its name
    /// (ADR-0009) — its address was already established by `Join` on this
    /// same connection. `replication` (issue #30) is the replication
    /// factor this node currently believes, or `None` if it doesn't know
    /// yet (the wire's `0` sentinel — see the module docs).
    Heartbeat {
        name: String,
        replication: Option<usize>,
    },
    List,
    /// ADR-0008: a node asking to join, identified by its name (ADR-0009)
    /// and the port it serves on — the reachable address is composed from
    /// this connection's own source IP plus that port (ADR-0012). Sent
    /// once, on a connection the node then holds open to receive the
    /// `R\n` promotion push.
    Join {
        name: String,
        port: u16,
    },
    /// ADR-0008: a ready node reporting it has finished handing off its
    /// share of a join, identified by its own name (ADR-0009) and the
    /// joining node's name — so a stale report for an abandoned join can
    /// never be credited to the current one (issue #5).
    Complete {
        name: String,
        joining_name: String,
    },
    /// ADR-0010: an already-promoted node (re-)declaring membership, with
    /// the same name/port shape as `Join` — upserted straight to
    /// `Joined`, no handoff orchestration.
    Announce {
        name: String,
        port: u16,
    },
}

#[derive(Debug, PartialEq, Eq)]
enum ParseError {
    InvalidCommand,
    InvalidLength,
    EmptyField,
    EmptySecret,
    InvalidUtf8,
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
        b"A" => {
            let secret_length = parts.next().ok_or(ParseError::InvalidLength)?;

            if parts.next().is_some() {
                return Err(ParseError::InvalidLength);
            }

            let secret_length = parse_length(secret_length)?;

            if secret_length == 0 {
                return Err(ParseError::EmptySecret);
            }

            let secret_start = header_end + 1;
            let secret_end = secret_start
                .checked_add(secret_length)
                .ok_or(ParseError::InvalidLength)?;

            if input.len() < secret_end {
                return Err(ParseError::Incomplete);
            }

            let frame = input.split_to(secret_end).freeze();
            let secret = frame.slice(secret_start..secret_end);

            Ok(DiscoveryCommand::Auth(secret))
        }

        b"L" => {
            if parts.next().is_some() {
                return Err(ParseError::InvalidLength);
            }

            let _ = input.split_to(header_end + 1);
            Ok(DiscoveryCommand::List)
        }

        b"H" => {
            let name_length = parts.next().ok_or(ParseError::InvalidLength)?;
            let replication = parts.next().ok_or(ParseError::InvalidLength)?;

            if parts.next().is_some() {
                return Err(ParseError::InvalidLength);
            }

            let name_length = parse_length(name_length)?;
            // `0` is the wire sentinel for "no belief yet" (issue #30) —
            // not a real replication factor (nodes validate >= 1 the same
            // way this replica's own `--replication-factor` does).
            let replication = match parse_length(replication)? {
                0 => None,
                r => Some(r),
            };
            let name = parse_string_field(input, header_end, name_length)?;

            Ok(DiscoveryCommand::Heartbeat { name, replication })
        }

        b"C" => {
            let name_length = parts.next().ok_or(ParseError::InvalidLength)?;
            let joining_length = parts.next().ok_or(ParseError::InvalidLength)?;

            if parts.next().is_some() {
                return Err(ParseError::InvalidLength);
            }

            let name_length = parse_length(name_length)?;
            let joining_length = parse_length(joining_length)?;
            let (name, joining_name) =
                parse_two_string_fields(input, header_end, name_length, joining_length)?;

            Ok(DiscoveryCommand::Complete { name, joining_name })
        }

        b"J" | b"P" => {
            let name_length = parts.next().ok_or(ParseError::InvalidLength)?;
            let port = parts.next().ok_or(ParseError::InvalidLength)?;

            if parts.next().is_some() {
                return Err(ParseError::InvalidLength);
            }

            let name_length = parse_length(name_length)?;
            // ADR-0012: the node declares only the port it serves on; the
            // reachable address is composed with this connection's source
            // IP. Port 0 can never be served on, so reject it.
            let port: u16 = std::str::from_utf8(port)
                .ok()
                .and_then(|raw| raw.parse().ok())
                .filter(|port| *port != 0)
                .ok_or(ParseError::InvalidLength)?;
            // Same owned-fn-pointer dance as `H`/`C` above: resolve the
            // variant while `command` is still alive, so `input` can be
            // reborrowed mutably below.
            let make: fn(String, u16) -> DiscoveryCommand = match command {
                b"J" => |name, port| DiscoveryCommand::Join { name, port },
                _ => |name, port| DiscoveryCommand::Announce { name, port },
            };
            let name = parse_string_field(input, header_end, name_length)?;

            Ok(make(name, port))
        }

        _ => Err(ParseError::InvalidCommand),
    }
}

/// Parses a single length-prefixed field (a name or an address, both
/// plain UTF-8 strings) starting right after the header's `\n`.
fn parse_string_field(
    input: &mut BytesMut,
    header_end: usize,
    length: usize,
) -> Result<String, ParseError> {
    if length == 0 {
        return Err(ParseError::EmptyField);
    }

    let start = header_end + 1;
    let end = start.checked_add(length).ok_or(ParseError::InvalidLength)?;

    if input.len() < end {
        return Err(ParseError::Incomplete);
    }

    let frame = input.split_to(end);
    String::from_utf8(frame[start..end].to_vec()).map_err(|_| ParseError::InvalidUtf8)
}

/// Parses two consecutive length-prefixed fields (`J`'s name then
/// address), checking both are fully buffered before consuming any of
/// `input`, so `parse`'s "untouched on `Incomplete`" contract holds even
/// though this reads across two fields in one call.
fn parse_two_string_fields(
    input: &mut BytesMut,
    header_end: usize,
    first_length: usize,
    second_length: usize,
) -> Result<(String, String), ParseError> {
    if first_length == 0 || second_length == 0 {
        return Err(ParseError::EmptyField);
    }

    let first_start = header_end + 1;
    let first_end = first_start
        .checked_add(first_length)
        .ok_or(ParseError::InvalidLength)?;
    let second_end = first_end
        .checked_add(second_length)
        .ok_or(ParseError::InvalidLength)?;

    if input.len() < second_end {
        return Err(ParseError::Incomplete);
    }

    let frame = input.split_to(second_end);
    let first = String::from_utf8(frame[first_start..first_end].to_vec())
        .map_err(|_| ParseError::InvalidUtf8)?;
    let second = String::from_utf8(frame[first_end..second_end].to_vec())
        .map_err(|_| ParseError::InvalidUtf8)?;

    Ok((first, second))
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

fn lock(registry: &Registry) -> std::sync::MutexGuard<'_, FxHashMap<String, NodeInfo>> {
    registry
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

fn lock_current_join(current_join: &CurrentJoin) -> std::sync::MutexGuard<'_, Option<PendingJoin>> {
    current_join
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

/// If no join is currently in progress, picks one `Waiting` node (if any)
/// and starts it toward `Joined`: promoted straight to `Joined` if there
/// are no `Joined` nodes yet to hand data off from (the bootstrap case —
/// nothing to receive), otherwise moved to `Joining` with a `PendingJoin`
/// tracking every currently-`Joined` node (by name, ADR-0009) as one it
/// must receive a `C` from before promotion, and sent an `M` (concurrently,
/// one connection per ready node) telling it to start its handoff.
async fn try_begin_next_join(
    registry: &Registry,
    current_join: &CurrentJoin,
    auth_secret: &Option<Bytes>,
    tls_connector: &Option<TlsConnector>,
    replication: usize,
) {
    // Scoped to a block, not just an explicit `drop()`, so `join_guard`
    // (a std::sync::MutexGuard, not Send) is unambiguously out of scope
    // before the awaits below — required for this (and everything that
    // calls it) to remain a Send future, which `tokio::spawn` needs.
    let (name, joining_addr, joined) = {
        let mut join_guard = lock_current_join(current_join);

        if join_guard.is_some() {
            return;
        }

        let next_waiting = lock(registry)
            .iter()
            .find(|(_, info)| info.state == NodeState::Waiting)
            .map(|(name, info)| (name.clone(), info.address.clone()));

        let Some((name, joining_addr)) = next_waiting else {
            return;
        };

        let joined: Vec<(String, String)> = lock(registry)
            .iter()
            .filter(|(_, info)| info.state == NodeState::Joined)
            .map(|(name, info)| (name.clone(), info.address.clone()))
            .collect();

        if let Some(info) = lock(registry).get_mut(&name) {
            info.state = NodeState::Joining;
        }

        if joined.is_empty() {
            drop(join_guard);
            promote_to_joined(registry, &name);
            return;
        }

        let expected: HashSet<String> = joined.iter().map(|(name, _)| name.clone()).collect();

        *join_guard = Some(PendingJoin {
            joining_name: name.clone(),
            expected,
            completed: HashSet::new(),
            started_at: Instant::now(),
            max_entries: 0,
        });

        (name, joining_addr, joined)
    };

    println!(
        "INFO join started: {name} (handoff from {} members)",
        joined.len()
    );

    let mut sends = JoinSet::new();

    for (ready_name, ready_addr) in joined.iter().cloned() {
        let auth_secret = auth_secret.clone();
        let tls_connector = tls_connector.clone();
        let joining_name = name.clone();
        let joining_addr = joining_addr.clone();
        let joined_roster = joined.clone();

        sends.spawn(async move {
            let result = send_migrate_with_retry(
                &ready_name,
                &ready_addr,
                &auth_secret,
                &tls_connector,
                &joining_name,
                &joining_addr,
                &joined_roster,
                replication,
                OUTBOUND_IO_TIMEOUT,
            )
            .await;

            (ready_name, result)
        });
    }

    while let Some(outcome) = sends.join_next().await {
        match outcome {
            Ok((ready_name, Err(error))) => {
                // Every individual attempt (see `send_migrate_with_retry`)
                // was already logged; this is the final, permanent
                // failure. ADR-0008 still doesn't define recovery beyond
                // this point (issue #20's second gap) — the join stalls
                // until discovery's own size-derived migration timeout
                // (doc/adr/0017-*.md) reaps it.
                eprintln!(
                    "WARN permanently failed to send M to {ready_name} after \
                     {MIGRATE_SEND_ATTEMPTS} attempts: {error}"
                );
            }
            Ok((_, Ok(entries))) => {
                // doc/adr/0017-*.md: sizes the migration timeout by the
                // largest handoff any ready node reported — a report for
                // a since-abandoned/replaced join (this one's slot
                // already moved on to a different `joining_name`) must
                // not be credited here, mirroring `handle_complete`'s own
                // guard.
                let mut join_guard = lock_current_join(current_join);
                if let Some(pending) = join_guard.as_mut()
                    && pending.joining_name == name
                {
                    pending.max_entries = pending.max_entries.max(entries);
                }
            }
            Err(error) => eprintln!("WARN a task sending M panicked: {error}"),
        }
    }
}

/// Bounded retry for `M`'s delivery (issue #20 / ADR-0008 Consequences):
/// tries up to `MIGRATE_SEND_ATTEMPTS` times, logging each failed attempt,
/// with a fresh connection every time (`send_migrate` owns its own
/// connect) since a failed write/read leaves the previous connection's
/// state unknown. This only bounds the *send* — the handoff itself,
/// reported back asynchronously via `C`, stays a separate, unretried
/// concern (see `send_migrate`'s own docs).
#[allow(clippy::too_many_arguments)]
async fn send_migrate_with_retry(
    ready_name: &str,
    address: &str,
    auth_secret: &Option<Bytes>,
    tls_connector: &Option<TlsConnector>,
    joining_name: &str,
    joining_addr: &str,
    joined: &[(String, String)],
    replication: usize,
    io_timeout: Duration,
) -> io::Result<usize> {
    let mut last_error = None;

    for attempt in 1..=MIGRATE_SEND_ATTEMPTS {
        match send_migrate(
            address,
            auth_secret,
            tls_connector,
            joining_name,
            joining_addr,
            joined,
            replication,
            io_timeout,
        )
        .await
        {
            Ok(entries) => return Ok(entries),
            Err(error) => {
                eprintln!(
                    "WARN failed to send M to {ready_name} (attempt \
                     {attempt}/{MIGRATE_SEND_ATTEMPTS}): {error}"
                );
                last_error = Some(error);
            }
        }
    }

    Err(last_error.expect("the loop above runs at least once"))
}

/// Connects to `address` (a `Joined` node) as a client, sends `M` with the
/// joining node's identity and the full `joined` roster (ADR-0009 names +
/// addresses), and waits for the `A <entries>\n` acknowledgment —
/// confirmation that `M` was received and parsed, not that the handoff it
/// kicks off (which happens asynchronously on the node's side) has
/// finished; that's reported separately, node-to-discovery, via `C`.
/// Returns the entry count the ack reports, purely for sizing this join's
/// migration timeout (doc/adr/0017-*.md) — not otherwise used here.
#[allow(clippy::too_many_arguments)]
async fn send_migrate(
    address: &str,
    auth_secret: &Option<Bytes>,
    tls_connector: &Option<TlsConnector>,
    joining_name: &str,
    joining_addr: &str,
    joined: &[(String, String)],
    replication: usize,
    io_timeout: Duration,
) -> io::Result<usize> {
    let mut stream = connect_client_stream(address, tls_connector.as_ref()).await?;

    if let Some(secret) = auth_secret {
        let mut auth = format!("A {}\n", secret.len()).into_bytes();
        auth.extend_from_slice(secret);
        stream.write_all(&auth).await?;

        let mut ack = [0u8; 3];
        read_exact_timed(&mut stream, &mut ack, io_timeout).await?;

        if &ack != b"On\n" {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "node rejected the auth secret",
            ));
        }
    }

    let mut message = format!(
        "M {} {} {} {}\n",
        joining_name.len(),
        joining_addr.len(),
        joined.len(),
        replication
    )
    .into_bytes();
    message.extend_from_slice(joining_name.as_bytes());
    message.extend_from_slice(joining_addr.as_bytes());

    for (name, addr) in joined {
        message.extend_from_slice(format!("{} {}\n", name.len(), addr.len()).as_bytes());
        message.extend_from_slice(name.as_bytes());
        message.extend_from_slice(addr.as_bytes());
    }

    stream.write_all(&message).await?;

    let line = read_line_timed(&mut stream, io_timeout).await?;
    let entries = line
        .strip_prefix("A ")
        .and_then(|rest| rest.parse::<usize>().ok())
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "node did not acknowledge M"))?;

    Ok(entries)
}

/// `read_exact` bounded by `io_timeout` — see `OUTBOUND_IO_TIMEOUT`.
async fn read_exact_timed(
    stream: &mut ClientStream,
    buf: &mut [u8],
    io_timeout: Duration,
) -> io::Result<()> {
    timeout(io_timeout, stream.read_exact(buf))
        .await
        .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "ack read timed out"))??;
    Ok(())
}

/// Reads up to (and consuming) the next `\n`, bounded overall by
/// `io_timeout` — used for `M`'s `A <entries>\n` ack, whose length
/// varies with the reported entry count (unlike every other ack on this
/// connection, which is a fixed number of bytes). Bails out past
/// `MAX_ACK_LINE_LENGTH` rather than growing `line` without bound on a
/// desynced or malicious peer.
const MAX_ACK_LINE_LENGTH: usize = 64;

async fn read_line_timed(stream: &mut ClientStream, io_timeout: Duration) -> io::Result<String> {
    let mut line = Vec::new();
    let mut byte = [0u8; 1];

    loop {
        read_exact_timed(stream, &mut byte, io_timeout).await?;
        if byte[0] == b'\n' {
            break;
        }
        if line.len() >= MAX_ACK_LINE_LENGTH {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "ack line too long",
            ));
        }
        line.push(byte[0]);
    }

    String::from_utf8(line)
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "ack was not valid utf-8"))
}

/// Connects to `address` (a ready node) as a client and sends `X`, telling
/// it to abandon whatever handoff it has in flight for `joining_name` —
/// used by `abandon_current_join` to return every ready node to its
/// pre-migration state once a join is scrapped. Best-effort: the caller
/// moves on regardless of the outcome here (see its doc comment) — a node
/// that never receives this either wasn't actually working on this
/// handoff (safe no-op on its end) or will find out its `C` report goes
/// nowhere once `current_join` has already moved on.
async fn send_cancel(
    address: &str,
    auth_secret: &Option<Bytes>,
    tls_connector: &Option<TlsConnector>,
    joining_name: &str,
    io_timeout: Duration,
) -> io::Result<()> {
    let mut stream = connect_client_stream(address, tls_connector.as_ref()).await?;

    if let Some(secret) = auth_secret {
        let mut auth = format!("A {}\n", secret.len()).into_bytes();
        auth.extend_from_slice(secret);
        stream.write_all(&auth).await?;

        let mut ack = [0u8; 3];
        read_exact_timed(&mut stream, &mut ack, io_timeout).await?;

        if &ack != b"On\n" {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "node rejected the auth secret",
            ));
        }
    }

    let mut message = format!("X {}\n", joining_name.len()).into_bytes();
    message.extend_from_slice(joining_name.as_bytes());

    stream.write_all(&message).await?;

    let mut ack = [0u8; 2];
    read_exact_timed(&mut stream, &mut ack, io_timeout).await?;

    if &ack != b"A\n" {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "node did not acknowledge X",
        ));
    }

    Ok(())
}

/// Moves `name` to `Joined`, making it visible in future `L` responses,
/// and wakes its held-open connection so it can push the `R\n` promotion
/// notice.
fn promote_to_joined(registry: &Registry, name: &str) {
    let (promoted, members) = {
        let mut guard = lock(registry);
        let promoted = guard.get_mut(name).map(|info| {
            info.state = NodeState::Joined;
            info.last_heartbeat = Instant::now();
            Arc::clone(&info.promoted)
        });
        let members = guard
            .values()
            .filter(|info| info.state == NodeState::Joined)
            .count();
        (promoted, members)
    };

    if let Some(promoted) = promoted {
        println!("INFO join promoted: {name} (members now {members})");
        // Wake every currently-parked `J` connection (a duplicate `J`
        // under the same name shares this Notify — issue #7) AND store a
        // permit for a waiter that hasn't parked yet (the bootstrap case,
        // promoted synchronously before `wait_for_promotion` runs).
        promoted.notify_waiters();
        promoted.notify_one();
    }
}

/// Registers `name` as `Waiting` with `address` (a no-op if it's already
/// registered — this must not downgrade a node already past `Waiting`)
/// and attempts to start it toward `Joined` immediately. Returns the
/// `Notify` its connection should hold open and wait on for promotion.
#[allow(clippy::too_many_arguments)]
async fn start_join(
    registry: &Registry,
    current_join: &CurrentJoin,
    auth_secret: &Option<Bytes>,
    tls_connector: &Option<TlsConnector>,
    replication: usize,
    name: &str,
    address: String,
) -> Arc<Notify> {
    let promoted = {
        let mut guard = lock(registry);
        if !guard.contains_key(name) {
            println!("INFO node registered: {name} at {address} (waiting to join)");
        }
        let info = guard
            .entry(name.to_string())
            .or_insert_with(|| NodeInfo::new(address, NodeState::Waiting));
        Arc::clone(&info.promoted)
    };

    try_begin_next_join(
        registry,
        current_join,
        auth_secret,
        tls_connector,
        replication,
    )
    .await;

    promoted
}

/// Records a ready node's completion report for the in-progress join. If
/// this was the last of `expected` to report in, promotes the joining
/// node and lets the next `Waiting` node (if any) start.
async fn handle_complete(
    registry: &Registry,
    current_join: &CurrentJoin,
    auth_secret: &Option<Bytes>,
    tls_connector: &Option<TlsConnector>,
    replication: usize,
    reporting_name: &str,
    for_joining_name: &str,
) {
    let joining_name = {
        let mut join_guard = lock_current_join(current_join);

        let Some(pending) = join_guard.as_mut() else {
            return;
        };

        // Issue #5: a report for an earlier, since-abandoned join must
        // not be credited to this one — the reporter has sent this join's
        // target nothing.
        if pending.joining_name != for_joining_name {
            return;
        }

        if !pending.expected.contains(reporting_name) {
            return;
        }

        pending.completed.insert(reporting_name.to_string());
        println!(
            "INFO handoff completed: {reporting_name} -> {for_joining_name} ({}/{})",
            pending.completed.len(),
            pending.expected.len()
        );

        if pending.completed.len() < pending.expected.len() {
            return;
        }

        let joining_name = pending.joining_name.clone();
        *join_guard = None;
        joining_name
    };

    promote_to_joined(registry, &joining_name);
    try_begin_next_join(
        registry,
        current_join,
        auth_secret,
        tls_connector,
        replication,
    )
    .await;
}

/// Called whenever any node's connection to discovery ends (cleanly or
/// not), by name. A Waiting/Joining node has no liveness signal besides
/// this one connection (see `NodeInfo::promoted`'s doc comment), so its
/// registry entry is removed outright; a `Joined` node keeps relying on
/// `sweep_expired`'s timeout instead, since an ordinary connection
/// hiccup — reconnecting for the next heartbeat, say — shouldn't evict it
/// (this fires once per connection, not once per heartbeat).
///
/// If `name` turns out to be the current join's own Waiting/Joining
/// node — it died mid-handoff, its only liveness signal gone — the whole
/// join is abandoned via `abandon_current_join`. A ready member's
/// heartbeat connection dying does *not* abandon the join on its own
/// (issue #10): `C` (handoff complete) is reported over its own
/// short-lived connection (`report_complete`), never the heartbeat one,
/// so a heartbeat hiccup says nothing about that ready member's actual
/// handoff progress — a live node reconnects within one heartbeat
/// interval regardless. A ready member that's truly gone (crashed, or
/// genuinely stuck) is instead caught by `sweep_expired`'s size-derived
/// `migration_timeout_for` — the same size-aware grace
/// doc/adr/0017-*.md introduced so a large, legitimate join doesn't get
/// reaped just for being large; abandoning here too would bypass that
/// grace entirely and reintroduce the flat-timeout failure mode the
/// size-derived design replaced.
async fn on_node_connection_ended(
    registry: &Registry,
    current_join: &CurrentJoin,
    auth_secret: &Option<Bytes>,
    tls_connector: &Option<TlsConnector>,
    replication: usize,
    name: &str,
) {
    let removed = {
        let mut guard = lock(registry);
        let was_waiting_or_joining = guard
            .get(name)
            .is_some_and(|info| info.state != NodeState::Joined);

        if was_waiting_or_joining {
            guard.remove(name)
        } else {
            None
        }
    };

    if let Some(info) = removed {
        // Issue #9: a duplicate `J` under the same name (sharing this
        // `NodeInfo`'s `Notify`, see `start_join`) may have a second
        // connection still parked in `wait_for_promotion` on this same
        // `Notify`. Removing the entry without waking it stranded that
        // connection forever — nothing else would ever tell it the entry
        // is gone. Wake it exactly like `abandon_current_join` already
        // does for its own removal, so `wait_for_promotion`'s re-check
        // observes `None` and errors the connection closed (the node's
        // heartbeat loop redials and re-`J`s).
        info.promoted.notify_waiters();
        info.promoted.notify_one();
    }

    // Only the current join's own Waiting/Joining node dying abandons it
    // here — see this function's doc comment for why a ready member's
    // connection dying deliberately does not.
    let is_current_joining_node = lock_current_join(current_join)
        .as_ref()
        .is_some_and(|pending| pending.joining_name == name);

    if is_current_joining_node {
        abandon_current_join(
            registry,
            current_join,
            auth_secret,
            tls_connector,
            replication,
            "joining node disconnected",
        )
        .await;
    }
}

/// Scraps the current join (if any), returning every node it touched to
/// its pre-migration state: clears `current_join`, removes the joining
/// node from the registry (it was never in `L` to begin with, so this is
/// nothing more than dropping bookkeeping), and tells every still-
/// registered ready node in `expected` to roll back via `X` (see
/// `send_cancel`) — a ready node that already finished, or never gets the
/// message, is unaffected. Then lets the next `Waiting` node (if any)
/// start. A no-op if no join is in progress.
async fn abandon_current_join(
    registry: &Registry,
    current_join: &CurrentJoin,
    auth_secret: &Option<Bytes>,
    tls_connector: &Option<TlsConnector>,
    replication: usize,
    reason: &str,
) {
    let Some(pending) = lock_current_join(current_join).take() else {
        return;
    };

    eprintln!(
        "WARN join abandoned: {} (reason={reason})",
        pending.joining_name
    );

    // Issue #4: the joining node's connection is parked in
    // `wait_for_promotion` with the idle timeout deliberately disabled —
    // removing its entry without waking it would strand that connection
    // (and the node behind it, which waits on `R` forever instead of
    // re-joining). Wake it; the re-check in `wait_for_promotion` sees the
    // entry is gone and errors the connection closed, so the node's
    // heartbeat loop redials and re-`J`s.
    let stranded = lock(registry).remove(&pending.joining_name);
    if let Some(info) = stranded {
        info.promoted.notify_waiters();
        info.promoted.notify_one();
    }

    let ready_addrs: Vec<(String, String)> = {
        let guard = lock(registry);
        pending
            .expected
            .iter()
            .filter_map(|name| {
                guard
                    .get(name)
                    .map(|info| (name.clone(), info.address.clone()))
            })
            .collect()
    };

    let mut sends = JoinSet::new();

    for (ready_name, ready_addr) in ready_addrs {
        let auth_secret = auth_secret.clone();
        let tls_connector = tls_connector.clone();
        let joining_name = pending.joining_name.clone();

        sends.spawn(async move {
            let result = send_cancel(
                &ready_addr,
                &auth_secret,
                &tls_connector,
                &joining_name,
                OUTBOUND_IO_TIMEOUT,
            )
            .await;
            (ready_name, result)
        });
    }

    while let Some(outcome) = sends.join_next().await {
        match outcome {
            Ok((ready_name, Err(error))) => {
                eprintln!("WARN failed to send X (cancel) to {ready_name}: {error}");
            }
            Ok((_, Ok(()))) => {}
            Err(error) => eprintln!("WARN a task sending X panicked: {error}"),
        }
    }

    try_begin_next_join(
        registry,
        current_join,
        auth_secret,
        tls_connector,
        replication,
    )
    .await;
}

/// Holds a Waiting/Joining node's connection open after it sends `J`,
/// since it has no other way to learn it's been promoted (see
/// `NodeInfo::promoted`). Waits for either the promotion notification or
/// the connection dying; any bytes the node sends in the meantime are a
/// protocol error — a well-behaved node sends nothing more until
/// promoted. Deliberately does not apply the ordinary idle timeout: a
/// node may legitimately wait here far longer than `IDLE_TIMEOUT` while
/// another node's join is in progress.
async fn wait_for_promotion(
    stream: &mut ServerStream,
    registry: &Registry,
    name: &str,
    promoted: Arc<Notify>,
    mut shutdown_rx: watch::Receiver<bool>,
) -> io::Result<()> {
    let mut idle_byte = [0u8; 1];

    loop {
        tokio::select! {
            _ = promoted.notified() => {
                // A wake means the join resolved — but in which direction
                // is the registry's call: promotion (issue #7 wakes every
                // duplicate waiter, all of which must answer `R`) or an
                // abandoned join that removed the entry (issue #4). A
                // still-Waiting state would be a spurious wake; keep
                // waiting.
                let state = lock(registry).get(name).map(|info| info.state);
                match state {
                    Some(NodeState::Joined) => {
                        stream.write_all(b"R\n").await?;
                        return Ok(());
                    }
                    None => {
                        return Err(io::Error::other(
                            "join abandoned while waiting for promotion",
                        ));
                    }
                    Some(_) => continue,
                }
            }
            _ = shutdown_rx.changed() => {
                return Err(io::Error::other("shutting down"));
            }
            result = stream.read(&mut idle_byte) => {
                let bytes_read = result?;

                if bytes_read == 0 {
                    return Err(io::Error::new(
                        io::ErrorKind::UnexpectedEof,
                        "connection closed while waiting to join",
                    ));
                }

                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "unexpected data while waiting to join",
                ));
            }
        }
    }
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
async fn run(
    address: &str,
    liveness_timeout: Duration,
    startup_grace: Duration,
    replication: usize,
    auth_secret: Option<Bytes>,
    tls_acceptor: Option<TlsAcceptor>,
    tls_connector: Option<TlsConnector>,
) -> io::Result<()> {
    let listener = TcpListener::bind(address).await?;
    let cluster_state = ClusterState {
        registry: Arc::new(Mutex::new(FxHashMap::default())),
        current_join: Arc::new(Mutex::new(None)),
    };
    let connection_limit = Arc::new(Semaphore::new(MAX_CONNECTIONS));
    let mut connection_tasks = JoinSet::new();
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let connection_config = ConnectionConfig {
        idle_timeout: IDLE_TIMEOUT,
        list_ready_at: Instant::now() + startup_grace,
        replication,
        auth_secret: auth_secret.clone(),
        tls_acceptor,
        tls_connector: tls_connector.clone(),
    };

    println!(
        "INFO startup grace: refusing list queries for {}s",
        startup_grace.as_secs()
    );

    let sweep_task = tokio::spawn(sweep_expired(
        Arc::clone(&cluster_state.registry),
        Arc::clone(&cluster_state.current_join),
        auth_secret,
        tls_connector,
        replication,
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
                println!("INFO shutdown signal received");
                shutdown_tx.send_replace(true);
                break;
            }

            result = connection_tasks.join_next(), if !connection_tasks.is_empty() => {
                if let Some(Err(error)) = result {
                    eprintln!("WARN connection task failed: {error}");
                }
            }

            result = listener.accept() => {
                let (stream, address) = result?;

                dispatch_connection(
                    stream,
                    address,
                    cluster_state.clone(),
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

    // Bounded like everything else on the shutdown path (issue #6): a
    // sweep pass wedged on an unresponsive node must not hold up process
    // exit.
    if timeout(SHUTDOWN_TIMEOUT, sweep_task).await.is_err() {
        eprintln!("WARN sweep task did not finish before the shutdown timeout");
    }

    Ok(())
}

async fn dispatch_connection(
    stream: TcpStream,
    address: SocketAddr,
    cluster_state: ClusterState,
    connection_limit: Arc<Semaphore>,
    config: ConnectionConfig,
    shutdown_rx: watch::Receiver<bool>,
    connection_tasks: &mut JoinSet<()>,
) -> bool {
    let _ = stream.set_nodelay(true);

    let mut stream: ServerStream = match &config.tls_acceptor {
        Some(acceptor) => match timeout(TLS_HANDSHAKE_TIMEOUT, acceptor.accept(stream)).await {
            Ok(Ok(tls_stream)) => MaybeTls::Tls(Box::new(tls_stream)),
            Ok(Err(error)) => {
                eprintln!("WARN TLS handshake with {address} failed: {error}");
                return false;
            }
            Err(_) => {
                eprintln!("WARN TLS handshake with {address} timed out");
                return false;
            }
        },
        None => MaybeTls::Plain(stream),
    };

    let permit = match connection_limit.try_acquire_owned() {
        Ok(permit) => permit,
        Err(_) => {
            if let Err(error) = stream.write_all(b"B\n").await {
                eprintln!("WARN failed to send busy response to {address}: {error}");
            }

            return false;
        }
    };

    connection_tasks.spawn(async move {
        let _connection_permit = permit;
        // Written once this connection identifies itself via `J` (a plain
        // client connection never does, and stays `None`) — read
        // afterward, regardless of how `handle_connection` exits, so
        // `on_node_connection_ended` runs uniformly instead of needing a
        // cleanup call at every one of its internal return points.
        let connection_name: Arc<std::sync::Mutex<Option<String>>> =
            Arc::new(std::sync::Mutex::new(None));

        let result = handle_connection(
            stream,
            address.ip(),
            cluster_state.registry.clone(),
            cluster_state.current_join.clone(),
            config.clone(),
            shutdown_rx,
            Arc::clone(&connection_name),
        )
        .await;

        if let Err(error) = &result {
            eprintln!("WARN connection error from {address}: {error}");
        }

        let name = connection_name
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone();

        if let Some(name) = name {
            on_node_connection_ended(
                &cluster_state.registry,
                &cluster_state.current_join,
                &config.auth_secret,
                &config.tls_connector,
                config.replication,
                &name,
            )
            .await;
        }
    });

    true
}

#[allow(clippy::too_many_arguments)]
async fn sweep_expired(
    registry: Registry,
    current_join: CurrentJoin,
    auth_secret: Option<Bytes>,
    tls_connector: Option<TlsConnector>,
    replication: usize,
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
                // Waiting/Joining nodes hold one long-lived connection
                // open instead of heartbeating (see NodeInfo::promoted);
                // their liveness is tied to that connection, not to this
                // sweep, so only Joined nodes are subject to it.
                let mut evicted = Vec::new();
                lock(&registry).retain(|name, info| {
                    let keep = info.state != NodeState::Joined
                        || now.duration_since(info.last_heartbeat) < liveness_timeout;
                    if !keep {
                        evicted.push(name.clone());
                    }
                    keep
                });
                for name in evicted {
                    eprintln!(
                        "WARN node evicted: {name} (no heartbeat within {}s)",
                        liveness_timeout.as_secs()
                    );
                }

                // ADR-0008 pattern 3: a ready node alive but never
                // reporting `C` (see `migration_timeout_for`).
                let timed_out = lock_current_join(&current_join)
                    .as_ref()
                    .is_some_and(|pending| {
                        pending.started_at.elapsed() >= migration_timeout_for(pending.max_entries)
                    });

                if timed_out {
                    abandon_current_join(&registry, &current_join, &auth_secret, &tls_connector, replication, "migration timeout").await;
                }
            }
            _ = shutdown_rx.changed() => return,
        }
    }
}

async fn handle_connection(
    mut stream: ServerStream,
    // The connection's source IP: combined with the port a `J`/`P`
    // declares, it IS the node's address (ADR-0012).
    peer_ip: std::net::IpAddr,
    registry: Registry,
    current_join: CurrentJoin,
    config: ConnectionConfig,
    mut shutdown_rx: watch::Receiver<bool>,
    connection_name: Arc<std::sync::Mutex<Option<String>>>,
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
            Ok(DiscoveryCommand::Auth(secret)) => {
                let accepted = match &config.auth_secret {
                    Some(expected) => constant_time_eq(&secret, expected),
                    None => true,
                };

                if accepted {
                    authenticated = true;
                    stream.write_all(b"Od\n").await?;
                    continue;
                }

                stream.write_all(b"Ed\n").await?;
                return Err(io::Error::new(
                    io::ErrorKind::PermissionDenied,
                    "invalid auth secret",
                ));
            }
            Ok(_) if !authenticated => {
                stream.write_all(b"Ed\n").await?;
                return Err(io::Error::new(
                    io::ErrorKind::PermissionDenied,
                    "command sent before authenticating",
                ));
            }
            Ok(DiscoveryCommand::Heartbeat { name, replication }) => {
                let refreshed = {
                    let mut guard = lock(&registry);
                    match guard.get_mut(&name) {
                        Some(info) if info.state == NodeState::Joined => {
                            info.last_heartbeat = Instant::now();
                            true
                        }
                        _ => false,
                    }
                };

                if !refreshed {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "heartbeat from a node that has not joined",
                    ));
                }

                // Issue #30: the node's own belief (learned from a `M` it
                // sent as an ADR-0011 handoff source) may disagree with
                // this replica's configured value — every discovery
                // replica takes its own `--replication-factor` flag and
                // nothing else validates they agree (ADR-0010 deliberately
                // keeps replicas from talking to each other). Loud,
                // repeated logging is the whole mechanism: the mismatch
                // isn't rejected, since a stale replica can't tell which
                // side is actually wrong.
                if let Some(reported) = replication
                    && reported != config.replication
                {
                    eprintln!(
                        "WARN node {name} reports replication factor {reported}, but this \
                         discovery replica is configured with --replication-factor \
                         {} — every replica must agree (see doc/adr/0010-*.md); a client \
                         or node that learned R from a different replica may be running \
                         with a different fan-out/failover width right now",
                        config.replication
                    );
                }

                stream.write_all(b"A\n").await?;
                continue;
            }
            Ok(DiscoveryCommand::List) => {
                if Instant::now() < config.list_ready_at {
                    // ADR-0010 startup grace — see `ConnectionConfig::
                    // list_ready_at`. Same `B\n`-then-close shape as the
                    // connection-limit rejection: "can't serve you right
                    // now, retry", which the SDK maps to trying its next
                    // discovery seed.
                    stream.write_all(b"B\n").await?;
                    return Ok(());
                }

                let nodes: Vec<(String, String)> = lock(&registry)
                    .iter()
                    .filter(|(_, info)| info.state == NodeState::Joined)
                    .map(|(name, info)| (name.clone(), info.address.clone()))
                    .collect();
                let mut response = format!("N {} {}\n", nodes.len(), config.replication);
                for (name, addr) in &nodes {
                    response.push_str(&format!("{} {}\n{name}{addr}\n", name.len(), addr.len()));
                }
                stream.write_all(response.as_bytes()).await?;
                continue;
            }
            Ok(DiscoveryCommand::Join { name, port }) => {
                let addr = format!("{peer_ip}:{port}");
                *connection_name
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner()) = Some(name.clone());

                let promoted = start_join(
                    &registry,
                    &current_join,
                    &config.auth_secret,
                    &config.tls_connector,
                    config.replication,
                    &name,
                    addr,
                )
                .await;

                wait_for_promotion(&mut stream, &registry, &name, promoted, shutdown_rx.clone())
                    .await?;

                continue;
            }
            Ok(DiscoveryCommand::Announce { name, port }) => {
                let addr = format!("{peer_ip}:{port}");
                // Same bookkeeping as `J`: this connection now belongs to
                // `name`, so its death runs `on_node_connection_ended`.
                *connection_name
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner()) = Some(name.clone());

                let accepted = {
                    let mut guard = lock(&registry);
                    match guard.get_mut(&name) {
                        // A name mid-join announcing would corrupt the
                        // ADR-0008 join bookkeeping, and no correct node
                        // does it (announces only happen after promotion).
                        Some(info) if info.state != NodeState::Joined => false,
                        Some(info) => {
                            info.address = addr;
                            info.last_heartbeat = Instant::now();
                            true
                        }
                        None => {
                            println!("INFO node announced: {name} at {addr} (re-registered)");
                            guard.insert(name.clone(), NodeInfo::new(addr, NodeState::Joined));
                            true
                        }
                    }
                };

                if !accepted {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "announce for a node that is mid-join",
                    ));
                }

                stream.write_all(b"R\n").await?;
                continue;
            }
            Ok(DiscoveryCommand::Complete { name, joining_name }) => {
                handle_complete(
                    &registry,
                    &current_join,
                    &config.auth_secret,
                    &config.tls_connector,
                    config.replication,
                    &name,
                    &joining_name,
                )
                .await;
                stream.write_all(b"A\n").await?;
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
    rustls::crypto::ring::default_provider()
        .install_default()
        .expect("no other rustls crypto provider is installed this early in the process");

    let args = match parse_args() {
        Ok(args) => args,
        Err(ArgsError::Help(message)) => {
            println!("{message}");
            return ExitCode::SUCCESS;
        }
        Err(ArgsError::Invalid(message)) => {
            eprintln!("{message}");
            return ExitCode::FAILURE;
        }
    };

    let tls_acceptor = match (&args.tls_cert, &args.tls_key) {
        (Some(cert), Some(key)) => match load_tls_acceptor(cert, key) {
            Ok(acceptor) => Some(acceptor),
            Err(err) => {
                eprintln!("discovery: {err}");
                return ExitCode::FAILURE;
            }
        },
        _ => None,
    };

    let tls_connector = match &args.tls_ca {
        Some(ca) => match load_tls_connector(ca) {
            Ok(connector) => Some(connector),
            Err(err) => {
                eprintln!("discovery: {err}");
                return ExitCode::FAILURE;
            }
        },
        None => None,
    };

    let address = format!("{}:{}", args.host, args.port);
    if let Err(err) = run(
        &address,
        args.liveness_timeout,
        // The startup grace (ADR-0010) is the liveness window by
        // definition: it exists so every live member has had time to
        // re-announce before L is served, and that time IS the liveness
        // timeout. Not separately configurable.
        args.liveness_timeout,
        args.replication_factor,
        read_auth_secret(),
        tls_acceptor,
        tls_connector,
    )
    .await
    {
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
        let mut input = BytesMut::from(&b"H 9 2"[..]);
        assert_eq!(parse(&mut input), Err(ParseError::Incomplete));
    }

    #[test]
    fn parse_reports_incomplete_while_the_field_body_is_still_arriving() {
        let mut input = BytesMut::from(&b"H 9 2\n1.2.3"[..]);
        assert_eq!(parse(&mut input), Err(ParseError::Incomplete));
    }

    #[test]
    fn parse_reads_a_heartbeat_and_consumes_only_that_frame() {
        let mut input = BytesMut::from(&b"H 9 2\nsome-nameL\n"[..]);
        let command = parse(&mut input).unwrap();
        assert_eq!(
            command,
            DiscoveryCommand::Heartbeat {
                name: "some-name".to_string(),
                replication: Some(2),
            }
        );
        assert_eq!(&input[..], b"L\n");
    }

    #[test]
    fn parse_reads_a_heartbeat_with_zero_replication_as_unknown() {
        // `0` is the wire sentinel for "this node has no belief yet"
        // (issue #30) — never a real replication factor.
        let mut input = BytesMut::from(&b"H 9 0\nsome-name"[..]);
        let command = parse(&mut input).unwrap();
        assert_eq!(
            command,
            DiscoveryCommand::Heartbeat {
                name: "some-name".to_string(),
                replication: None,
            }
        );
    }

    #[test]
    fn parse_rejects_a_heartbeat_missing_the_replication_field() {
        let mut input = BytesMut::from(&b"H 9\nsome-name"[..]);
        assert_eq!(parse(&mut input), Err(ParseError::InvalidLength));
    }

    #[test]
    fn parse_rejects_a_heartbeat_with_trailing_arguments() {
        let mut input = BytesMut::from(&b"H 9 2 extra\nsome-name"[..]);
        assert_eq!(parse(&mut input), Err(ParseError::InvalidLength));
    }

    #[test]
    fn parse_reads_a_join_command_and_consumes_only_that_frame() {
        let mut input = BytesMut::from(&b"J 9 8356\nsome-nameL\n"[..]);
        let command = parse(&mut input).unwrap();
        assert_eq!(
            command,
            DiscoveryCommand::Join {
                name: "some-name".to_string(),
                port: 8356,
            }
        );
        assert_eq!(&input[..], b"L\n");
    }

    #[test]
    fn parse_reports_incomplete_while_a_joins_second_field_is_still_arriving() {
        let mut input = BytesMut::from(&b"J 9 8356\nsome-na"[..]);
        assert_eq!(parse(&mut input), Err(ParseError::Incomplete));
    }

    #[test]
    fn parse_leaves_input_untouched_when_a_joins_second_field_is_incomplete() {
        let original = b"J 9 8356\nsome-na".to_vec();
        let mut input = BytesMut::from(&original[..]);
        assert_eq!(parse(&mut input), Err(ParseError::Incomplete));
        assert_eq!(&input[..], &original[..]);
    }

    #[test]
    fn parse_reads_a_complete_command() {
        let mut input = BytesMut::from(&b"C 9 6\nsome-namejoinerL\n"[..]);
        let command = parse(&mut input).unwrap();
        assert_eq!(
            command,
            DiscoveryCommand::Complete {
                name: "some-name".to_string(),
                joining_name: "joiner".to_string(),
            }
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
    fn parse_rejects_an_empty_field() {
        let mut input = BytesMut::from(&b"H 0 2\n"[..]);
        assert_eq!(parse(&mut input), Err(ParseError::EmptyField));
    }

    #[test]
    fn parse_rejects_port_zero_in_join() {
        // Port 0 can never be served on — ADR-0012 derives the address
        // from source IP + this port, so a zero here is protocol garbage.
        let mut input = BytesMut::from(&b"J 9 0\nsome-name"[..]);
        assert_eq!(parse(&mut input), Err(ParseError::InvalidLength));
    }

    #[test]
    fn parse_rejects_a_non_numeric_length() {
        let mut input = BytesMut::from(&b"H x 2\n"[..]);
        assert_eq!(parse(&mut input), Err(ParseError::InvalidLength));
    }

    #[test]
    fn parse_rejects_invalid_utf8_fields() {
        let mut input = BytesMut::from(&b"H 2 2\n\xff\xfe"[..]);
        assert_eq!(parse(&mut input), Err(ParseError::InvalidUtf8));
    }

    #[test]
    fn parse_rejects_an_unknown_command() {
        let mut input = BytesMut::from(&b"X\n"[..]);
        assert_eq!(parse(&mut input), Err(ParseError::InvalidCommand));
    }

    #[test]
    fn parse_reads_an_auth_command() {
        let mut input = BytesMut::from(&b"A 6\nsecretL\n"[..]);
        assert_eq!(
            parse(&mut input),
            Ok(DiscoveryCommand::Auth(Bytes::from_static(b"secret")))
        );
        assert_eq!(&input[..], b"L\n");
    }

    #[test]
    fn parse_rejects_an_empty_secret() {
        let mut input = BytesMut::from(&b"A 0\n"[..]);
        assert_eq!(parse(&mut input), Err(ParseError::EmptySecret));
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

    #[tokio::test(flavor = "current_thread")]
    async fn join_then_heartbeat_then_list_reports_the_registered_node() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let registry: Registry = Arc::new(Mutex::new(FxHashMap::default()));
        let current_join: CurrentJoin = Arc::new(Mutex::new(None));
        let (_shutdown_tx, shutdown_rx) = watch::channel(false);

        let server_registry = Arc::clone(&registry);
        let server_current_join = Arc::clone(&current_join);
        tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let _ = handle_connection(
                MaybeTls::Plain(stream),
                std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST),
                server_registry,
                server_current_join,
                ConnectionConfig {
                    idle_timeout: Duration::from_secs(5),
                    list_ready_at: Instant::now(),
                    replication: 2,
                    auth_secret: None,
                    tls_acceptor: None,
                    tls_connector: None,
                },
                shutdown_rx,
                Arc::new(std::sync::Mutex::new(None)),
            )
            .await;
        });

        let mut client = TcpStream::connect(address).await.unwrap();
        // With no Joined nodes yet, this is the bootstrap case: the join
        // is accepted with nothing to hand off, so promotion is immediate.
        client
            .write_all(b"J 6 8356\nnode-aH 6 0\nnode-aL\n")
            .await
            .unwrap();

        // The three responses are written in separate calls but the client
        // may observe them coalesced into a single read, so accumulate
        // until the expected byte count has arrived instead of assuming
        // read boundaries.
        let expected = b"R\nA\nN 1 2\n6 14\nnode-a127.0.0.1:8356\n";
        let mut received = Vec::new();
        let mut chunk = [0u8; 64];

        while received.len() < expected.len() {
            let bytes_read = client.read(&mut chunk).await.unwrap();
            assert!(bytes_read > 0, "connection closed before response arrived");
            received.extend_from_slice(&chunk[..bytes_read]);
        }

        assert_eq!(received, expected);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn a_heartbeat_reporting_a_mismatched_replication_factor_is_still_acked() {
        // Issue #30: a mismatch is logged, not rejected — this replica has
        // no way to tell which side is actually misconfigured (ADR-0010
        // keeps replicas from reconciling with each other), so the node
        // must stay `Joined` and keep heartbeating normally.
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let registry: Registry = Arc::new(Mutex::new(FxHashMap::default()));
        let current_join: CurrentJoin = Arc::new(Mutex::new(None));
        let (_shutdown_tx, shutdown_rx) = watch::channel(false);

        let server_registry = Arc::clone(&registry);
        let server_current_join = Arc::clone(&current_join);
        tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let _ = handle_connection(
                MaybeTls::Plain(stream),
                std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST),
                server_registry,
                server_current_join,
                ConnectionConfig {
                    idle_timeout: Duration::from_secs(5),
                    list_ready_at: Instant::now(),
                    replication: 2,
                    auth_secret: None,
                    tls_acceptor: None,
                    tls_connector: None,
                },
                shutdown_rx,
                Arc::new(std::sync::Mutex::new(None)),
            )
            .await;
        });

        let mut client = TcpStream::connect(address).await.unwrap();
        // This replica is configured with R=2 (above); the node here
        // reports R=1 on its heartbeat — a disagreement.
        client
            .write_all(b"J 6 8356\nnode-aH 6 1\nnode-aL\n")
            .await
            .unwrap();

        let expected = b"R\nA\nN 1 2\n6 14\nnode-a127.0.0.1:8356\n";
        let mut received = Vec::new();
        let mut chunk = [0u8; 64];

        while received.len() < expected.len() {
            let bytes_read = client.read(&mut chunk).await.unwrap();
            assert!(bytes_read > 0, "connection closed before response arrived");
            received.extend_from_slice(&chunk[..bytes_read]);
        }

        assert_eq!(received, expected);
    }

    #[test]
    fn parse_reads_an_announce_command_and_consumes_only_that_frame() {
        let mut input = BytesMut::from(&b"P 9 8356\nsome-nameL\n"[..]);
        let command = parse(&mut input).unwrap();
        assert_eq!(
            command,
            DiscoveryCommand::Announce {
                name: "some-name".to_string(),
                port: 8356,
            }
        );
        assert_eq!(&input[..], b"L\n");
    }

    #[tokio::test(flavor = "current_thread")]
    async fn announce_upserts_a_joined_node_without_any_join_orchestration() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let registry: Registry = Arc::new(Mutex::new(FxHashMap::default()));
        let current_join: CurrentJoin = Arc::new(Mutex::new(None));
        let (_shutdown_tx, shutdown_rx) = watch::channel(false);

        let server_registry = Arc::clone(&registry);
        let server_current_join = Arc::clone(&current_join);
        tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let _ = handle_connection(
                MaybeTls::Plain(stream),
                std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST),
                server_registry,
                server_current_join,
                ConnectionConfig {
                    idle_timeout: Duration::from_secs(5),
                    list_ready_at: Instant::now(),
                    replication: 2,
                    auth_secret: None,
                    tls_acceptor: None,
                    tls_connector: None,
                },
                shutdown_rx,
                Arc::new(std::sync::Mutex::new(None)),
            )
            .await;
        });

        let mut client = TcpStream::connect(address).await.unwrap();
        // ADR-0010: an announce lands straight in `Joined` — promoted (R),
        // heartbeating (A), and visible in L — with no ADR-0008 join
        // machinery involved.
        client
            .write_all(b"P 6 8356\nnode-aH 6 0\nnode-aL\n")
            .await
            .unwrap();

        let expected = b"R\nA\nN 1 2\n6 14\nnode-a127.0.0.1:8356\n";
        let mut received = Vec::new();
        let mut chunk = [0u8; 64];

        while received.len() < expected.len() {
            let bytes_read = client.read(&mut chunk).await.unwrap();
            assert!(bytes_read > 0, "connection closed before response arrived");
            received.extend_from_slice(&chunk[..bytes_read]);
        }

        assert_eq!(received, expected);
        assert!(
            lock_current_join(&current_join).is_none(),
            "an announce must not start join orchestration"
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn announce_updates_the_address_of_an_already_joined_node() {
        let registry: Registry = Arc::new(Mutex::new(FxHashMap::default()));
        let current_join: CurrentJoin = Arc::new(Mutex::new(None));
        let (_shutdown_tx, shutdown_rx) = watch::channel(false);

        lock(&registry).insert(
            "node-a".to_string(),
            NodeInfo::new("127.0.0.1:1111".to_string(), NodeState::Joined),
        );

        let (mut client, server) = tcp_pair().await;
        tokio::spawn(handle_connection(
            MaybeTls::Plain(server),
            std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST),
            Arc::clone(&registry),
            Arc::clone(&current_join),
            ConnectionConfig {
                idle_timeout: Duration::from_secs(5),
                list_ready_at: Instant::now(),
                replication: 2,
                auth_secret: None,
                tls_acceptor: None,
                tls_connector: None,
            },
            shutdown_rx,
            Arc::new(std::sync::Mutex::new(None)),
        ));

        client.write_all(b"P 6 2222\nnode-a").await.unwrap();
        let mut response = [0u8; 2];
        client.read_exact(&mut response).await.unwrap();
        assert_eq!(&response, b"R\n");

        assert_eq!(
            lock(&registry).get("node-a").unwrap().address,
            "127.0.0.1:2222"
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn announce_for_a_name_mid_join_is_rejected() {
        let registry: Registry = Arc::new(Mutex::new(FxHashMap::default()));
        let current_join: CurrentJoin = Arc::new(Mutex::new(None));
        let (_shutdown_tx, shutdown_rx) = watch::channel(false);

        lock(&registry).insert(
            "node-a".to_string(),
            NodeInfo::new("127.0.0.1:1111".to_string(), NodeState::Waiting),
        );

        let (mut client, server) = tcp_pair().await;
        tokio::spawn(handle_connection(
            MaybeTls::Plain(server),
            std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST),
            Arc::clone(&registry),
            Arc::clone(&current_join),
            ConnectionConfig {
                idle_timeout: Duration::from_secs(5),
                list_ready_at: Instant::now(),
                replication: 2,
                auth_secret: None,
                tls_acceptor: None,
                tls_connector: None,
            },
            shutdown_rx,
            Arc::new(std::sync::Mutex::new(None)),
        ));

        client.write_all(b"P 6 2222\nnode-a").await.unwrap();

        // The connection is closed with no `R` — the announce was refused.
        let mut buffer = [0u8; 2];
        let bytes_read = client.read(&mut buffer).await.unwrap();
        assert_eq!(
            bytes_read, 0,
            "expected the connection to close, got {buffer:?}"
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn list_answers_busy_during_the_startup_grace_while_announce_still_works() {
        let registry: Registry = Arc::new(Mutex::new(FxHashMap::default()));
        let current_join: CurrentJoin = Arc::new(Mutex::new(None));
        let (_shutdown_tx, shutdown_rx) = watch::channel(false);

        let config = || ConnectionConfig {
            idle_timeout: Duration::from_secs(5),
            // Still inside the ADR-0010 grace for the whole test.
            list_ready_at: Instant::now() + Duration::from_secs(60),
            replication: 2,
            auth_secret: None,
            tls_acceptor: None,
            tls_connector: None,
        };

        // An announce during the grace must work — recovery depends on it.
        let (mut announcing, server) = tcp_pair().await;
        tokio::spawn(handle_connection(
            MaybeTls::Plain(server),
            std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST),
            Arc::clone(&registry),
            Arc::clone(&current_join),
            config(),
            shutdown_rx.clone(),
            Arc::new(std::sync::Mutex::new(None)),
        ));
        announcing.write_all(b"P 6 8356\nnode-a").await.unwrap();
        let mut response = [0u8; 2];
        announcing.read_exact(&mut response).await.unwrap();
        assert_eq!(&response, b"R\n");

        // A list during the grace gets the busy byte and a closed
        // connection, never the (possibly partial) node list.
        let (mut listing, server) = tcp_pair().await;
        tokio::spawn(handle_connection(
            MaybeTls::Plain(server),
            std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST),
            Arc::clone(&registry),
            Arc::clone(&current_join),
            config(),
            shutdown_rx.clone(),
            Arc::new(std::sync::Mutex::new(None)),
        ));
        listing.write_all(b"L\n").await.unwrap();

        let mut received = Vec::new();
        let mut chunk = [0u8; 16];
        loop {
            let bytes_read = listing.read(&mut chunk).await.unwrap();
            if bytes_read == 0 {
                break;
            }
            received.extend_from_slice(&chunk[..bytes_read]);
        }
        assert_eq!(received, b"B\n");
    }

    /// Boots a registry with node-a Joined (bootstrap join) and node-b
    /// parked in `wait_for_promotion` (Joining, expecting node-a's C).
    /// Returns the client sockets plus the shared state.
    async fn registry_with_a_joined_and_b_waiting(
        shutdown_rx: watch::Receiver<bool>,
    ) -> (TcpStream, TcpStream, Registry, CurrentJoin) {
        let registry: Registry = Arc::new(Mutex::new(FxHashMap::default()));
        let current_join: CurrentJoin = Arc::new(Mutex::new(None));

        let config = || ConnectionConfig {
            idle_timeout: IDLE_TIMEOUT,
            list_ready_at: Instant::now(),
            replication: 2,
            auth_secret: None,
            tls_acceptor: None,
            tls_connector: None,
        };

        let (mut node_a, server_a) = tcp_pair().await;
        tokio::spawn(handle_connection(
            MaybeTls::Plain(server_a),
            std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST),
            Arc::clone(&registry),
            Arc::clone(&current_join),
            config(),
            shutdown_rx.clone(),
            Arc::new(std::sync::Mutex::new(None)),
        ));
        node_a.write_all(b"J 6 9001\nnode-a").await.unwrap();
        let mut promoted = [0u8; 2];
        node_a.read_exact(&mut promoted).await.unwrap();
        assert_eq!(&promoted, b"R\n");

        let (mut node_b, server_b) = tcp_pair().await;
        tokio::spawn(handle_connection(
            MaybeTls::Plain(server_b),
            std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST),
            Arc::clone(&registry),
            Arc::clone(&current_join),
            config(),
            shutdown_rx.clone(),
            Arc::new(std::sync::Mutex::new(None)),
        ));
        node_b.write_all(b"J 6 9002\nnode-b").await.unwrap();

        for _ in 0..1000 {
            if lock_current_join(&current_join).is_some() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
        assert!(lock_current_join(&current_join).is_some());

        (node_a, node_b, registry, current_join)
    }

    #[tokio::test(flavor = "current_thread")]
    async fn an_abandoned_join_wakes_and_closes_the_waiting_connection() {
        // Regression for issue #4: abandoning a join must not strand the
        // joining node's held-open connection — it must observe the
        // rejection (connection closed) so the node redials and re-joins.
        let (_node_a, mut node_b, registry, current_join) = {
            let (_shutdown_tx, shutdown_rx) = watch::channel(false);
            // Keep the sender alive for the test's duration by leaking it
            // into the tuple below via _node_a's lifetime; the tasks only
            // need the receiver clones they already hold.
            let tuple = registry_with_a_joined_and_b_waiting(shutdown_rx).await;
            std::mem::forget(_shutdown_tx);
            tuple
        };

        abandon_current_join(&registry, &current_join, &None, &None, 2, "test").await;

        // node-b's connection must observe the abandonment promptly.
        let mut byte = [0u8; 1];
        let read = tokio::time::timeout(Duration::from_secs(5), node_b.read(&mut byte))
            .await
            .expect("the waiting connection was stranded by the abandoned join");
        assert_eq!(
            read.unwrap(),
            0,
            "expected the connection to close, not data"
        );
        assert!(!lock(&registry).contains_key("node-b"));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn duplicate_j_connections_under_one_name_both_receive_the_promotion() {
        // Regression for issue #7: a second live `J` under the same name
        // (a redial racing a half-open old connection) shares the entry's
        // Notify; promotion must wake both, not just one.
        let (_shutdown_tx, shutdown_rx) = watch::channel(false);
        let (mut node_a, mut node_b_first, registry, current_join) =
            registry_with_a_joined_and_b_waiting(shutdown_rx.clone()).await;

        let config = ConnectionConfig {
            idle_timeout: IDLE_TIMEOUT,
            list_ready_at: Instant::now(),
            replication: 2,
            auth_secret: None,
            tls_acceptor: None,
            tls_connector: None,
        };
        let (mut node_b_second, server) = tcp_pair().await;
        tokio::spawn(handle_connection(
            MaybeTls::Plain(server),
            std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST),
            Arc::clone(&registry),
            Arc::clone(&current_join),
            config,
            shutdown_rx,
            Arc::new(std::sync::Mutex::new(None)),
        ));
        node_b_second.write_all(b"J 6 9002\nnode-b").await.unwrap();
        tokio::time::sleep(Duration::from_millis(20)).await; // let it park

        node_a.write_all(b"C 6 6\nnode-anode-b").await.unwrap();
        let mut ack = [0u8; 2];
        node_a.read_exact(&mut ack).await.unwrap();

        for stream in [&mut node_b_first, &mut node_b_second] {
            let mut response = [0u8; 2];
            tokio::time::timeout(Duration::from_secs(5), stream.read_exact(&mut response))
                .await
                .expect("a duplicate J connection was never woken by the promotion")
                .unwrap();
            assert_eq!(&response, b"R\n");
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn on_node_connection_ended_wakes_a_surviving_duplicate_j_connection() {
        // Regression for issue #9: `on_node_connection_ended`'s registry
        // removal for a Waiting/Joining node used to skip the `Notify`
        // that `abandon_current_join`'s own removal already fires. A
        // duplicate `J` under the same name (issue #7) shares that
        // `Notify` across two parked connections; without waking it, the
        // connection that wasn't the one reported ended would hang in
        // `wait_for_promotion` forever.
        let (_shutdown_tx, shutdown_rx) = watch::channel(false);
        let registry: Registry = Arc::new(Mutex::new(FxHashMap::default()));
        // A different join already in progress, so node-b's own `J`
        // leaves it parked at `Waiting` rather than immediately becoming
        // the current join's own joining node (which `abandon_current_join`
        // already handles correctly, see the sibling tests above).
        lock(&registry).insert(
            "node-a".to_string(),
            NodeInfo::new("127.0.0.1:1".to_string(), NodeState::Joined),
        );
        let current_join: CurrentJoin = Arc::new(Mutex::new(Some(PendingJoin {
            joining_name: "node-x".to_string(),
            expected: ["node-a".to_string()].into_iter().collect(),
            completed: HashSet::new(),
            started_at: Instant::now(),
            max_entries: 0,
        })));

        let config = || ConnectionConfig {
            idle_timeout: IDLE_TIMEOUT,
            list_ready_at: Instant::now(),
            replication: 2,
            auth_secret: None,
            tls_acceptor: None,
            tls_connector: None,
        };

        let (mut node_b_first, server_first) = tcp_pair().await;
        tokio::spawn(handle_connection(
            MaybeTls::Plain(server_first),
            std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST),
            Arc::clone(&registry),
            Arc::clone(&current_join),
            config(),
            shutdown_rx.clone(),
            Arc::new(std::sync::Mutex::new(None)),
        ));
        node_b_first.write_all(b"J 6 9002\nnode-b").await.unwrap();
        tokio::time::sleep(Duration::from_millis(20)).await; // let it park

        let (mut node_b_second, server_second) = tcp_pair().await;
        tokio::spawn(handle_connection(
            MaybeTls::Plain(server_second),
            std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST),
            Arc::clone(&registry),
            Arc::clone(&current_join),
            config(),
            shutdown_rx.clone(),
            Arc::new(std::sync::Mutex::new(None)),
        ));
        node_b_second.write_all(b"J 6 9002\nnode-b").await.unwrap();
        tokio::time::sleep(Duration::from_millis(20)).await; // let it park too

        assert!(lock(&registry).contains_key("node-b"));

        // Simulate the *first* connection dying, as `run`'s connection-task
        // wrapper would report it.
        on_node_connection_ended(&registry, &current_join, &None, &None, 2, "node-b").await;

        // The still-open second connection must observe the registry
        // entry is gone and close, not hang forever.
        let mut byte = [0u8; 1];
        let read = tokio::time::timeout(Duration::from_secs(5), node_b_second.read(&mut byte))
            .await
            .expect("the surviving duplicate connection was stranded");
        assert_eq!(
            read.unwrap(),
            0,
            "expected the connection to close, not data"
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn a_complete_for_a_different_join_is_ignored() {
        // Regression for issue #5: a stale C naming an earlier, abandoned
        // join must not be credited to the current one.
        let (_shutdown_tx, shutdown_rx) = watch::channel(false);
        let (mut node_a, mut node_b, registry, _current_join) =
            registry_with_a_joined_and_b_waiting(shutdown_rx).await;

        // Stale: names a join for "node-x", not the pending one for node-b.
        node_a.write_all(b"C 6 6\nnode-anode-x").await.unwrap();
        let mut ack = [0u8; 2];
        node_a.read_exact(&mut ack).await.unwrap();
        assert_eq!(&ack, b"A\n");

        tokio::time::sleep(Duration::from_millis(20)).await;
        assert_ne!(
            lock(&registry).get("node-b").map(|info| info.state),
            Some(NodeState::Joined),
            "a stale C was credited to the wrong join"
        );

        // The genuine report still promotes.
        node_a.write_all(b"C 6 6\nnode-anode-b").await.unwrap();
        node_a.read_exact(&mut ack).await.unwrap();
        let mut promoted = [0u8; 2];
        tokio::time::timeout(Duration::from_secs(5), node_b.read_exact(&mut promoted))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(&promoted, b"R\n");
    }

    #[tokio::test(flavor = "current_thread")]
    async fn send_cancel_times_out_against_a_silent_node() {
        // Regression for issue #6: an accepted-but-silent node must not
        // block the caller (the sweep task) indefinitely.
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap().to_string();
        let silent = tokio::spawn(async move {
            let (_stream, _) = listener.accept().await.unwrap();
            // Hold the connection open without ever answering.
            tokio::time::sleep(Duration::from_secs(60)).await;
        });

        let started = Instant::now();
        let result =
            send_cancel(&address, &None, &None, "node-b", Duration::from_millis(100)).await;
        silent.abort();

        let error = result.expect_err("expected the silent node to time the ack read out");
        assert_eq!(error.kind(), io::ErrorKind::TimedOut);
        assert!(started.elapsed() < Duration::from_secs(5));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn a_second_node_joining_waits_for_a_completion_report_before_being_promoted() {
        let registry: Registry = Arc::new(Mutex::new(FxHashMap::default()));
        let current_join: CurrentJoin = Arc::new(Mutex::new(None));
        let (_shutdown_tx, shutdown_rx) = watch::channel(false);

        let config = || ConnectionConfig {
            idle_timeout: IDLE_TIMEOUT,
            list_ready_at: Instant::now(),
            replication: 2,
            auth_secret: None,
            tls_acceptor: None,
            tls_connector: None,
        };

        // Node A joins first. There are no Joined nodes yet, so it's the
        // bootstrap case: promoted immediately, with nothing to receive.
        let (mut node_a, server_a) = tcp_pair().await;
        tokio::spawn(handle_connection(
            MaybeTls::Plain(server_a),
            std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST),
            Arc::clone(&registry),
            Arc::clone(&current_join),
            config(),
            shutdown_rx.clone(),
            Arc::new(std::sync::Mutex::new(None)),
        ));
        node_a.write_all(b"J 6 9001\nnode-a").await.unwrap();
        let mut node_a_response = [0u8; 2];
        node_a.read_exact(&mut node_a_response).await.unwrap();
        assert_eq!(&node_a_response, b"R\n");

        // Node B joins next. A is now Joined, so B moves to Joining and
        // must wait for A's completion report — it must not be promoted
        // yet, and so must not appear in L.
        let (mut node_b, server_b) = tcp_pair().await;
        tokio::spawn(handle_connection(
            MaybeTls::Plain(server_b),
            std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST),
            Arc::clone(&registry),
            Arc::clone(&current_join),
            config(),
            shutdown_rx.clone(),
            Arc::new(std::sync::Mutex::new(None)),
        ));
        node_b.write_all(b"J 6 9002\nnode-b").await.unwrap();

        // The write completing only means the OS accepted the bytes, not
        // that the spawned connection task has read and processed them
        // yet; poll briefly instead of assuming one yield is enough.
        for _ in 0..1000 {
            if lock(&registry).contains_key("node-b") {
                break;
            }
            tokio::time::sleep(Duration::from_millis(1)).await;
        }

        assert_eq!(
            lock(&registry).get("node-b").unwrap().state,
            NodeState::Joining
        );

        let (mut lister, server_l) = tcp_pair().await;
        tokio::spawn(handle_connection(
            MaybeTls::Plain(server_l),
            std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST),
            Arc::clone(&registry),
            Arc::clone(&current_join),
            config(),
            shutdown_rx.clone(),
            Arc::new(std::sync::Mutex::new(None)),
        ));
        lister.write_all(b"L\n").await.unwrap();
        let expected_list = b"N 1 2\n6 14\nnode-a127.0.0.1:9001\n";
        let mut list_response = vec![0u8; expected_list.len()];
        lister.read_exact(&mut list_response).await.unwrap();
        assert_eq!(list_response, expected_list);

        // A reports it has finished handing its share off to B. B should
        // now be promoted and receive its own R\n on the connection it's
        // been holding open since it sent J.
        node_a.write_all(b"C 6 6\nnode-anode-b").await.unwrap();
        let mut ack = [0u8; 2];
        node_a.read_exact(&mut ack).await.unwrap();
        assert_eq!(&ack, b"A\n");

        let mut node_b_response = [0u8; 2];
        node_b.read_exact(&mut node_b_response).await.unwrap();
        assert_eq!(&node_b_response, b"R\n");
        assert_eq!(
            lock(&registry).get("node-b").unwrap().state,
            NodeState::Joined
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn a_ready_node_receives_m_when_a_second_node_joins() {
        let registry: Registry = Arc::new(Mutex::new(FxHashMap::default()));
        let current_join: CurrentJoin = Arc::new(Mutex::new(None));
        let (_shutdown_tx, shutdown_rx) = watch::channel(false);

        let config = || ConnectionConfig {
            idle_timeout: IDLE_TIMEOUT,
            list_ready_at: Instant::now(),
            replication: 2,
            auth_secret: None,
            tls_acceptor: None,
            tls_connector: None,
        };

        // A fake ready node: a real listener that expects M and acks it,
        // standing in for node A's registered address.
        let ready_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let ready_port = ready_listener.local_addr().unwrap().port();
        let ready_addr = ready_listener.local_addr().unwrap().to_string();
        let received: Arc<std::sync::Mutex<Vec<u8>>> = Arc::new(std::sync::Mutex::new(Vec::new()));
        let received_task = Arc::clone(&received);
        let ready_task = tokio::spawn(async move {
            let (mut connection, _) = ready_listener.accept().await.unwrap();
            let mut buffer = [0u8; 256];
            let bytes_read = connection.read(&mut buffer).await.unwrap();
            received_task
                .lock()
                .unwrap()
                .extend_from_slice(&buffer[..bytes_read]);
            connection.write_all(b"A 0\n").await.unwrap();
        });

        // Node A joins first (bootstrap: no ready nodes yet), registered
        // at the fake ready node's address.
        let (mut node_a, server_a) = tcp_pair().await;
        tokio::spawn(handle_connection(
            MaybeTls::Plain(server_a),
            std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST),
            Arc::clone(&registry),
            Arc::clone(&current_join),
            config(),
            shutdown_rx.clone(),
            Arc::new(std::sync::Mutex::new(None)),
        ));
        node_a
            .write_all(format!("J 6 {ready_port}\nnode-a").as_bytes())
            .await
            .unwrap();
        let mut node_a_response = [0u8; 2];
        node_a.read_exact(&mut node_a_response).await.unwrap();
        assert_eq!(&node_a_response, b"R\n");

        // Node B joins next — A is Joined and ready, so discovery should
        // send it M.
        let (mut node_b, server_b) = tcp_pair().await;
        tokio::spawn(handle_connection(
            MaybeTls::Plain(server_b),
            std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST),
            Arc::clone(&registry),
            Arc::clone(&current_join),
            config(),
            shutdown_rx.clone(),
            Arc::new(std::sync::Mutex::new(None)),
        ));
        node_b.write_all(b"J 6 9002\nnode-b").await.unwrap();

        for _ in 0..1000 {
            if !received.lock().unwrap().is_empty() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(1)).await;
        }

        ready_task.await.unwrap();

        let message = String::from_utf8(received.lock().unwrap().clone()).unwrap();
        let header_end = message.find('\n').unwrap();
        let mut header = message[..header_end].split(' ');
        assert_eq!(header.next(), Some("M"));

        let joining_name_length: usize = header.next().unwrap().parse().unwrap();
        let joining_addr_length: usize = header.next().unwrap().parse().unwrap();
        let joined_count: usize = header.next().unwrap().parse().unwrap();
        let replication: usize = header.next().unwrap().parse().unwrap();
        assert_eq!(header.next(), None);
        assert_eq!(joined_count, 1);
        assert_eq!(replication, 2);

        let body = &message[header_end + 1..];
        assert_eq!(&body[..joining_name_length], "node-b");
        assert_eq!(
            &body[joining_name_length..joining_name_length + joining_addr_length],
            "127.0.0.1:9002"
        );

        let roster = &body[joining_name_length + joining_addr_length..];
        assert!(
            roster.contains("node-a"),
            "roster should list node-a: {roster:?}"
        );
        assert!(
            roster.contains(&ready_addr),
            "roster should list node-a's address: {roster:?}"
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn handle_connection_rejects_commands_sent_before_authenticating() {
        let (mut client, server) = tcp_pair().await;
        let registry: Registry = Arc::new(Mutex::new(FxHashMap::default()));
        let current_join: CurrentJoin = Arc::new(Mutex::new(None));
        let (_shutdown_tx, shutdown_rx) = watch::channel(false);

        let connection_task = tokio::spawn(handle_connection(
            MaybeTls::Plain(server),
            std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST),
            registry,
            current_join,
            ConnectionConfig {
                idle_timeout: IDLE_TIMEOUT,
                list_ready_at: Instant::now(),
                replication: 2,
                auth_secret: Some(Bytes::from_static(b"correct-secret")),
                tls_acceptor: None,
                tls_connector: None,
            },
            shutdown_rx,
            Arc::new(std::sync::Mutex::new(None)),
        ));

        client.write_all(b"L\n").await.unwrap();

        let mut response = [0u8; 3];
        client.read_exact(&mut response).await.unwrap();
        assert_eq!(&response, b"Ed\n");

        let error = connection_task.await.unwrap().unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::PermissionDenied);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn handle_connection_rejects_an_incorrect_auth_secret() {
        let (mut client, server) = tcp_pair().await;
        let registry: Registry = Arc::new(Mutex::new(FxHashMap::default()));
        let current_join: CurrentJoin = Arc::new(Mutex::new(None));
        let (_shutdown_tx, shutdown_rx) = watch::channel(false);

        let connection_task = tokio::spawn(handle_connection(
            MaybeTls::Plain(server),
            std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST),
            registry,
            current_join,
            ConnectionConfig {
                idle_timeout: IDLE_TIMEOUT,
                list_ready_at: Instant::now(),
                replication: 2,
                auth_secret: Some(Bytes::from_static(b"correct-secret")),
                tls_acceptor: None,
                tls_connector: None,
            },
            shutdown_rx,
            Arc::new(std::sync::Mutex::new(None)),
        ));

        client.write_all(b"A 11\nwrong-value").await.unwrap();

        let mut response = [0u8; 3];
        client.read_exact(&mut response).await.unwrap();
        assert_eq!(&response, b"Ed\n");

        let error = connection_task.await.unwrap().unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::PermissionDenied);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn handle_connection_accepts_commands_after_correct_auth() {
        let (mut client, server) = tcp_pair().await;
        let registry: Registry = Arc::new(Mutex::new(FxHashMap::default()));
        let current_join: CurrentJoin = Arc::new(Mutex::new(None));
        let (_shutdown_tx, shutdown_rx) = watch::channel(false);

        let connection_task = tokio::spawn(handle_connection(
            MaybeTls::Plain(server),
            std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST),
            registry,
            current_join,
            ConnectionConfig {
                idle_timeout: IDLE_TIMEOUT,
                list_ready_at: Instant::now(),
                replication: 2,
                auth_secret: Some(Bytes::from_static(b"correct-secret")),
                tls_acceptor: None,
                tls_connector: None,
            },
            shutdown_rx,
            Arc::new(std::sync::Mutex::new(None)),
        ));

        client.write_all(b"A 14\ncorrect-secretL\n").await.unwrap();
        client.shutdown().await.unwrap();

        let expected = b"Od\nN 0 2\n";
        let mut received = Vec::new();
        let mut chunk = [0u8; 64];

        while received.len() < expected.len() {
            let bytes_read = client.read(&mut chunk).await.unwrap();
            assert!(bytes_read > 0, "connection closed before response arrived");
            received.extend_from_slice(&chunk[..bytes_read]);
        }
        assert_eq!(received, expected);

        connection_task.await.unwrap().unwrap();
    }

    #[tokio::test(flavor = "current_thread")]
    async fn handle_connection_treats_auth_as_a_no_op_when_no_secret_is_configured() {
        let (mut client, server) = tcp_pair().await;
        let registry: Registry = Arc::new(Mutex::new(FxHashMap::default()));
        let current_join: CurrentJoin = Arc::new(Mutex::new(None));
        let (_shutdown_tx, shutdown_rx) = watch::channel(false);

        let connection_task = tokio::spawn(handle_connection(
            MaybeTls::Plain(server),
            std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST),
            registry,
            current_join,
            ConnectionConfig {
                idle_timeout: IDLE_TIMEOUT,
                list_ready_at: Instant::now(),
                replication: 2,
                auth_secret: None,
                tls_acceptor: None,
                tls_connector: None,
            },
            shutdown_rx,
            Arc::new(std::sync::Mutex::new(None)),
        ));

        client.write_all(b"A 8\nanything").await.unwrap();
        client.shutdown().await.unwrap();

        let expected = b"Od\n";
        let mut response = vec![0_u8; expected.len()];
        client.read_exact(&mut response).await.unwrap();
        assert_eq!(response, expected);

        connection_task.await.unwrap().unwrap();
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
        let cluster_state = ClusterState {
            registry: Arc::new(Mutex::new(FxHashMap::default())),
            current_join: Arc::new(Mutex::new(None)),
        };
        let (_shutdown_tx, shutdown_rx) = watch::channel(false);
        let mut connection_tasks = JoinSet::new();

        let (_first_client, first_server) = tcp_pair().await;
        let first_address = first_server.peer_addr().unwrap();

        let first_connection = dispatch_connection(
            first_server,
            first_address,
            cluster_state.clone(),
            Arc::clone(&connection_limit),
            ConnectionConfig {
                idle_timeout: IDLE_TIMEOUT,
                list_ready_at: Instant::now(),
                replication: 2,
                auth_secret: None,
                tls_acceptor: None,
                tls_connector: None,
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
            cluster_state,
            connection_limit,
            ConnectionConfig {
                idle_timeout: IDLE_TIMEOUT,
                list_ready_at: Instant::now(),
                replication: 2,
                auth_secret: None,
                tls_acceptor: None,
                tls_connector: None,
            },
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
        lock(&registry).insert(
            "some-name".to_string(),
            NodeInfo::new("127.0.0.1:8356".to_string(), NodeState::Joined),
        );

        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let current_join: CurrentJoin = Arc::new(Mutex::new(None));
        let sweep_task = tokio::spawn(sweep_expired(
            Arc::clone(&registry),
            current_join,
            None,
            None,
            2,
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

    #[tokio::test(flavor = "current_thread")]
    async fn dispatch_connection_serves_commands_over_tls() {
        let _ = rustls::crypto::ring::default_provider().install_default();

        let rcgen::CertifiedKey { cert, signing_key } =
            rcgen::generate_simple_self_signed(vec!["localhost".to_string()]).unwrap();
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

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();

        let cluster_state = ClusterState {
            registry: Arc::new(Mutex::new(FxHashMap::default())),
            current_join: Arc::new(Mutex::new(None)),
        };
        let connection_limit = Arc::new(Semaphore::new(1));
        let (_shutdown_tx, shutdown_rx) = watch::channel(false);

        let config = ConnectionConfig {
            idle_timeout: IDLE_TIMEOUT,
            list_ready_at: Instant::now(),
            replication: 2,
            auth_secret: None,
            tls_acceptor: Some(acceptor),
            tls_connector: None,
        };

        let server_task = tokio::spawn(async move {
            let (stream, peer_addr) = listener.accept().await.unwrap();
            let mut connection_tasks = JoinSet::new();

            dispatch_connection(
                stream,
                peer_addr,
                cluster_state,
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

        tls.write_all(b"L\n").await.unwrap();

        let expected = b"N 0 2\n";
        let mut response = vec![0_u8; expected.len()];
        tls.read_exact(&mut response).await.unwrap();
        assert_eq!(response, expected);

        server_task.await.unwrap();
    }

    /// Builds a self-signed cert/key pair plus a matching `TlsAcceptor`
    /// (for a fake node standing in as the connection's server side) and
    /// `TlsConnector` (for discovery's own outbound connect), mirroring
    /// `dispatch_connection_serves_commands_over_tls`'s setup.
    fn self_signed_tls_pair() -> (TlsAcceptor, TlsConnector) {
        let rcgen::CertifiedKey { cert, signing_key } =
            rcgen::generate_simple_self_signed(vec!["localhost".to_string()]).unwrap();
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
    async fn send_migrate_delivers_m_to_a_tls_secured_node() {
        let _ = rustls::crypto::ring::default_provider().install_default();
        let (acceptor, connector) = self_signed_tls_pair();

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        // The self-signed cert is issued for "localhost", not an IP, so
        // connect via that name for SNI/hostname verification to pass.
        let address = format!("localhost:{}", listener.local_addr().unwrap().port());

        let received: Arc<std::sync::Mutex<Vec<u8>>> = Arc::new(std::sync::Mutex::new(Vec::new()));
        let received_task = Arc::clone(&received);
        let node_task = tokio::spawn(async move {
            let (tcp, _) = listener.accept().await.unwrap();
            let mut tls = acceptor.accept(tcp).await.unwrap();
            let mut buffer = [0u8; 256];
            let bytes_read = tls.read(&mut buffer).await.unwrap();
            received_task
                .lock()
                .unwrap()
                .extend_from_slice(&buffer[..bytes_read]);
            tls.write_all(b"A 0\n").await.unwrap();
        });

        // A plaintext-only `send_migrate` call would never complete a TLS
        // handshake with this fake node and this would hang/error instead.
        send_migrate(
            &address,
            &None,
            &Some(connector),
            "joining-node",
            "127.0.0.1:9",
            &[],
            2,
            OUTBOUND_IO_TIMEOUT,
        )
        .await
        .unwrap();

        node_task.await.unwrap();

        let mut expected = b"M 12 11 0 2\n".to_vec();
        expected.extend_from_slice(b"joining-node");
        expected.extend_from_slice(b"127.0.0.1:9");
        assert_eq!(*received.lock().unwrap(), expected);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn send_migrate_with_retry_recovers_from_a_transient_failure() {
        // Issue #20: the first attempt is a transient failure (the "node"
        // accepts the connection then drops it without acking); the
        // second succeeds. The whole call must still return `Ok`.
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap().to_string();

        let node_task = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            drop(stream);

            let (mut stream, _) = listener.accept().await.unwrap();
            let mut buffer = [0u8; 256];
            let _ = stream.read(&mut buffer).await.unwrap();
            stream.write_all(b"A 0\n").await.unwrap();
        });

        let result = send_migrate_with_retry(
            "ready-node",
            &address,
            &None,
            &None,
            "joining-node",
            "127.0.0.1:9",
            &[],
            2,
            OUTBOUND_IO_TIMEOUT,
        )
        .await;

        assert!(result.is_ok(), "expected recovery on retry, got {result:?}");
        node_task.await.unwrap();
    }

    #[tokio::test(flavor = "current_thread")]
    async fn send_migrate_with_retry_gives_up_after_exhausting_every_attempt() {
        // Every attempt fails the same way (connection accepted then
        // dropped without acking) — the call must give up after exactly
        // `MIGRATE_SEND_ATTEMPTS` tries, not retry forever.
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap().to_string();

        let attempts = Arc::new(std::sync::atomic::AtomicU32::new(0));
        let server_attempts = Arc::clone(&attempts);
        let node_task = tokio::spawn(async move {
            loop {
                let Ok((stream, _)) = listener.accept().await else {
                    return;
                };
                server_attempts.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                drop(stream);
            }
        });

        let result = send_migrate_with_retry(
            "ready-node",
            &address,
            &None,
            &None,
            "joining-node",
            "127.0.0.1:9",
            &[],
            2,
            OUTBOUND_IO_TIMEOUT,
        )
        .await;

        assert!(result.is_err());
        assert_eq!(
            attempts.load(std::sync::atomic::Ordering::SeqCst),
            MIGRATE_SEND_ATTEMPTS
        );

        node_task.abort();
    }

    #[tokio::test(flavor = "current_thread")]
    async fn send_cancel_delivers_x_to_a_tls_secured_node() {
        let _ = rustls::crypto::ring::default_provider().install_default();
        let (acceptor, connector) = self_signed_tls_pair();

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        // The self-signed cert is issued for "localhost", not an IP, so
        // connect via that name for SNI/hostname verification to pass.
        let address = format!("localhost:{}", listener.local_addr().unwrap().port());

        let received: Arc<std::sync::Mutex<Vec<u8>>> = Arc::new(std::sync::Mutex::new(Vec::new()));
        let received_task = Arc::clone(&received);
        let node_task = tokio::spawn(async move {
            let (tcp, _) = listener.accept().await.unwrap();
            let mut tls = acceptor.accept(tcp).await.unwrap();
            let mut buffer = [0u8; 256];
            let bytes_read = tls.read(&mut buffer).await.unwrap();
            received_task
                .lock()
                .unwrap()
                .extend_from_slice(&buffer[..bytes_read]);
            tls.write_all(b"A\n").await.unwrap();
        });

        send_cancel(
            &address,
            &None,
            &Some(connector),
            "joining-node",
            OUTBOUND_IO_TIMEOUT,
        )
        .await
        .unwrap();

        node_task.await.unwrap();

        let mut expected = b"X 12\n".to_vec();
        expected.extend_from_slice(b"joining-node");
        assert_eq!(*received.lock().unwrap(), expected);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn a_ready_nodes_connection_dying_mid_join_does_not_abandon_the_join() {
        let registry: Registry = Arc::new(Mutex::new(FxHashMap::default()));

        lock(&registry).insert(
            "node-a".to_string(),
            NodeInfo::new("127.0.0.1:1".to_string(), NodeState::Joined),
        );
        lock(&registry).insert(
            "node-c".to_string(),
            NodeInfo::new("127.0.0.1:2".to_string(), NodeState::Joining),
        );

        let current_join: CurrentJoin = Arc::new(Mutex::new(Some(PendingJoin {
            joining_name: "node-c".to_string(),
            expected: ["node-a".to_string()].into_iter().collect(),
            completed: HashSet::new(),
            started_at: Instant::now(),
            max_entries: 0,
        })));

        // Node A's *heartbeat* connection to discovery dies (a transient
        // hiccup) while it's a ready member of an in-progress join.
        // Unlike the joining node's own connection (see the sibling test
        // below), this must not abandon the join: `C` (handoff complete)
        // is reported over its own short-lived connection
        // (`report_complete`), never the heartbeat one, so this event
        // says nothing about node A's actual handoff progress — only
        // `sweep_expired`'s size-derived `migration_timeout_for` should
        // ever reap a ready node that's truly gone (issue #10).
        on_node_connection_ended(&registry, &current_join, &None, &None, 2, "node-a").await;

        assert!(
            lock_current_join(&current_join).is_some(),
            "a ready node's heartbeat connection dying must not abandon the join"
        );
        assert!(
            lock(&registry).contains_key("node-c"),
            "the joining node must still be waiting for promotion"
        );
        // Node A is a `Joined` node, so this event leaves its own
        // registry entry alone too — only `sweep_expired`'s liveness
        // timeout evicts it.
        assert!(lock(&registry).contains_key("node-a"));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn a_joining_nodes_connection_dying_abandons_its_own_join_and_cancels_ready_nodes() {
        let registry: Registry = Arc::new(Mutex::new(FxHashMap::default()));

        let ready_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let ready_addr = ready_listener.local_addr().unwrap().to_string();
        let received: Arc<std::sync::Mutex<Vec<u8>>> = Arc::new(std::sync::Mutex::new(Vec::new()));
        let received_task = Arc::clone(&received);
        let ready_task = tokio::spawn(async move {
            let (mut connection, _) = ready_listener.accept().await.unwrap();
            let mut buffer = [0u8; 256];
            let bytes_read = connection.read(&mut buffer).await.unwrap();
            received_task
                .lock()
                .unwrap()
                .extend_from_slice(&buffer[..bytes_read]);
            connection.write_all(b"A\n").await.unwrap();
        });

        lock(&registry).insert(
            "node-a".to_string(),
            NodeInfo::new(ready_addr, NodeState::Joined),
        );
        lock(&registry).insert(
            "node-b".to_string(),
            NodeInfo::new("127.0.0.1:2".to_string(), NodeState::Joining),
        );

        let current_join: CurrentJoin = Arc::new(Mutex::new(Some(PendingJoin {
            joining_name: "node-b".to_string(),
            expected: ["node-a".to_string()].into_iter().collect(),
            completed: HashSet::new(),
            started_at: Instant::now(),
            max_entries: 0,
        })));

        // Node B (the joining node itself) disconnects before being
        // promoted (ADR-0008 pattern: the joining node dies mid-handoff).
        on_node_connection_ended(&registry, &current_join, &None, &None, 2, "node-b").await;

        ready_task.await.unwrap();

        assert!(lock_current_join(&current_join).is_none());
        assert!(!lock(&registry).contains_key("node-b"));

        let mut expected_cancel = b"X 6\n".to_vec();
        expected_cancel.extend_from_slice(b"node-b");
        assert_eq!(*received.lock().unwrap(), expected_cancel);
    }

    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn a_join_that_never_completes_is_abandoned_after_the_migration_timeout() {
        let registry: Registry = Arc::new(Mutex::new(FxHashMap::default()));

        lock(&registry).insert(
            "node-a".to_string(),
            NodeInfo::new("127.0.0.1:1".to_string(), NodeState::Joined),
        );
        lock(&registry).insert(
            "node-b".to_string(),
            NodeInfo::new("127.0.0.1:2".to_string(), NodeState::Joining),
        );

        let current_join: CurrentJoin = Arc::new(Mutex::new(Some(PendingJoin {
            joining_name: "node-b".to_string(),
            expected: ["node-a".to_string()].into_iter().collect(),
            completed: HashSet::new(),
            started_at: Instant::now(),
            max_entries: 0,
        })));

        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let sweep_task = tokio::spawn(sweep_expired(
            Arc::clone(&registry),
            Arc::clone(&current_join),
            None,
            None,
            2,
            Duration::from_secs(60),
            shutdown_rx,
        ));

        tokio::task::yield_now().await;
        tokio::time::advance(MIGRATION_TIMEOUT_BASE + Duration::from_secs(1)).await;
        tokio::task::yield_now().await;

        assert!(lock_current_join(&current_join).is_none());
        assert!(!lock(&registry).contains_key("node-b"));

        shutdown_tx.send_replace(true);
        sweep_task.await.unwrap();
    }

    // doc/adr/0017-*.md: the whole point of the size-derived timeout — a
    // join moving a lot of data must not be abandoned just for taking
    // longer than the old flat default would have allowed.
    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn a_join_with_many_entries_is_not_abandoned_at_the_base_timeout() {
        let registry: Registry = Arc::new(Mutex::new(FxHashMap::default()));

        lock(&registry).insert(
            "node-a".to_string(),
            NodeInfo::new("127.0.0.1:1".to_string(), NodeState::Joined),
        );
        lock(&registry).insert(
            "node-b".to_string(),
            NodeInfo::new("127.0.0.1:2".to_string(), NodeState::Joining),
        );

        let current_join: CurrentJoin = Arc::new(Mutex::new(Some(PendingJoin {
            joining_name: "node-b".to_string(),
            expected: ["node-a".to_string()].into_iter().collect(),
            completed: HashSet::new(),
            started_at: Instant::now(),
            max_entries: 100_000,
        })));

        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let sweep_task = tokio::spawn(sweep_expired(
            Arc::clone(&registry),
            Arc::clone(&current_join),
            None,
            None,
            2,
            Duration::from_secs(60),
            shutdown_rx,
        ));

        tokio::task::yield_now().await;
        // Past the old flat default, but nowhere near
        // migration_timeout_for(100_000) — still in progress.
        tokio::time::advance(MIGRATION_TIMEOUT_BASE + Duration::from_secs(1)).await;
        tokio::task::yield_now().await;

        assert!(lock_current_join(&current_join).is_some());
        assert!(lock(&registry).contains_key("node-b"));

        shutdown_tx.send_replace(true);
        sweep_task.await.unwrap();
    }
}
