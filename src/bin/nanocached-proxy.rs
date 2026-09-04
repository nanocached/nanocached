//! nanocached-proxy — connection fan-in and routing (issue #109).
//!
//! # Why a proxy
//!
//! The SDKs do proxyless client-side sharding: one lazy, persistent,
//! pipelined connection per node per client process. That gives every
//! client process N connections (N = cluster size), and — the binding
//! ceiling — gives every node one connection from every client process:
//! with a node's `MAX_CONNECTIONS = 1024` (256 per source IP), the
//! *fleet* caps out at ~1024 client processes regardless of data volume.
//! The standard remedy (the mcrouter/twemproxy model) is a proxy tier
//! that terminates many client connections and fans them into few
//! backend connections.
//!
//! # Design: routing and multiplexing together
//!
//! A proxy without backend multiplexing merely moves the FD problem one
//! hop and adds latency, so the two halves were designed together
//! (#109) and are now both implemented — routing in #109, backend
//! multiplexing in #110 — building on exactly the protocol primitives
//! that already exist:
//!
//! - **Routing (this binary).** The proxy is a discovery client: it
//!   fetches `L` (member roster + replication factor R) at startup and
//!   re-fetches periodically and on every `W`, and routes each key by
//!   the same HRW ring every SDK computes (`fmix64(fnv1a(name) XOR
//!   key_hash)`, with #105's namespaced `key_hash`). Reads go to the
//!   key's primary, falling to the next owner if the primary is
//!   unreachable; writes and deletes fan out to all R owners with the
//!   primary's ack answering the client (replica failures are logged
//!   and swallowed, the SDKs' own stance); a `W` from a node means the
//!   proxy's roster is stale — refresh once and retry, never forwarded
//!   to the client. `c`/`F` (#106) fan out to every member; any failure
//!   forces one refresh-and-retry before the operation fails. To a
//!   client the proxy therefore looks like a single node that owns every
//!   key: it answers `A` with the node identity (`On`/`OnT`), so every
//!   existing SDK in single-address mode — and any bare protocol client
//!   — works against it unchanged. Cluster-internal frames (`M`/`X`)
//!   are rejected: the proxy is not a member. One documented gap in
//!   "looks like a single node" (issue #463): the *initial* backend
//!   enqueue of every request happens in the client's request order,
//!   but a retry (a `W` refresh-and-retry, a replica fallback) is
//!   re-enqueued later and so runs outside that order. A client that
//!   pipelines `G k` immediately followed by `S k` on one connection —
//!   without waiting for the `G`'s reply — can therefore, when the `G`
//!   hits a stale-view `W` during a ring change, receive the value its
//!   own `S` just wrote. This is not a linearizability violation (the
//!   `S` was on the wire before the `G` was answered) and is exactly
//!   what an SDK's own `W` retry or a retry-capable client re-issuing
//!   after `R` would observe in cluster-direct mode; closing it would
//!   mean serialising same-key requests per client connection (each
//!   waiting for the previous same-key reply, `c`/`F` waiting for all
//!   of them), which was judged not worth the pipelining cost for a
//!   pattern — read-then-overwrite of one key without awaiting the
//!   read — that no SDK or adapter emits. Clients that need the read
//!   to strictly precede the write must await the `G` reply first.
//!
//! - **Multiplexing (#110, implemented here).** Response tags +
//!   pipelining are the multiplexing primitive: one shared per-node
//!   backend connection (`SharedBackends`) carries interleaved requests
//!   from every client connection, each stamped with a proxy-chosen
//!   sequence tag, and the tag on each reply routes it back to its
//!   waiting client — no new framing. The proxy holds one tagged
//!   connection per (node × proxy), and the node-side connection count
//!   collapses from fleet size to proxy count. Each backend connection
//!   is a writer/reader pair: the writer pumps queued frames without
//!   waiting for replies (genuine pipelining — `run_backend`), the
//!   reader resolves replies FIFO by tag. Scheduling is FIFO across
//!   clients with three bounded stages (`CLIENT_IN_FLIGHT` per client
//!   connection, `BACKEND_QUEUE_DEPTH` admission per backend,
//!   `MAX_BACKEND_IN_FLIGHT` written-but-unanswered per backend) —
//!   fairness by arrival order plus per-client allowances, and
//!   backpressure instead of unbounded queues. A poisoned backend
//!   connection (desynced stream, half-dead peer, per-reply progress
//!   timeout) is dropped whole: every request in flight on it errors,
//!   the tag match having bounded the blast radius to exactly those
//!   requests, and the next request redials — with the drivers'
//!   retry/fallback paths (`retry_get_on`, `refan_write`,
//!   `finish_clear`) absorbing the common node-side idle close so a
//!   long-lived shared connection's death is invisible to clients.
//!   Thin-client mode falls out of the client-facing contract above
//!   (one proxy address, no ring view, no discovery client) and shipped
//!   with #109/#122 (`via_proxy`).
//!
//! # Pipelining and response order
//!
//! Clients pipeline; responses must come back in request order even when
//! consecutive requests route to different nodes. The connection handler
//! therefore splits into a reader (parses frames, dispatches each to a
//! driver task, pushes the driver's completion into a FIFO) and a writer
//! (awaits each completion in FIFO order and writes it back), so a slow
//! node stalls delivery but never reorders it, and requests to other
//! nodes still make progress in the meantime.
//!
//! # Deployment, TLS, auth, limits
//!
//! - Deployment: proxies are stateless and horizontally scalable. Each
//!   announces itself to discovery (`Y`, issue #122) on its refresh
//!   cadence, so clients can fetch the proxy roster from discovery (`Q`)
//!   instead of being handed a proxy address out of band — a DNS/LB/VIP
//!   in front of the proxies keeps working too. SDKs in discovery mode
//!   keep working cluster-direct; the proxy is opt-in per client.
//! - Auth: the shared secret (env `NANOCACHED_AUTH_SECRET`, same as node
//!   and discovery) is required of clients exactly as a node requires it,
//!   and presented by the proxy on every backend and discovery
//!   connection. One trust domain, pass-through by re-authentication.
//! - TLS: `--tls-cert`/`--tls-key` terminate TLS for clients (same
//!   flags as node); `--tls-ca` makes every backend/discovery connection
//!   TLS with that CA (same flag the node uses toward discovery). The
//!   two are independent, so plaintext-in/TLS-out and the reverse both
//!   work.
//! - Limits: `--max-connections` (default 1024) client connections,
//!   over-limit connections answered `B\n` and closed, mirroring the
//!   node. No per-IP cap: a proxy typically fronts a NAT'd fleet where
//!   source IPs are meaningless; put the proxy itself behind one if its
//!   exposure needs it. Requests are bounded by the node's own 1 MiB
//!   request cap, enforced here too so a hostile client can't balloon
//!   proxy memory.
//! - Failure surface (issue #125): for a client that declared the
//!   retryable-error capability (`A ... R`), an upstream failure that
//!   survives one refresh-and-retry answers that request `R` (+tag) and
//!   the connection stays open — the client retries the request itself,
//!   bounded, against a backend the proxy has already dropped for
//!   redial. A legacy client (no `R` in its `A`) keeps the old
//!   contract: `E\n` and close, which its existing `E` handling
//!   understands; single-address SDK clients reconnect and retry
//!   exactly as they do against a restarting node.
//!
//! Self-contained by repo policy: binaries share no modules (see
//! `verify-staged-join`'s module docs), so the HRW ring, the `L` client
//! and the frame grammar are independent re-implementations of the same
//! wire contracts, pinned by the same cross-implementation vectors.

use bytes::{Bytes, BytesMut};
use std::collections::HashMap;
use std::future::Future;
use std::io;
use std::process::ExitCode;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{Semaphore, mpsc, oneshot, watch};
use tokio::time::{sleep, timeout};

/// Mirrors the node's own request-size bound so the proxy never buffers
/// more per request than a node would accept.
const MAX_REQUEST_SIZE: usize = 1_048_576;

/// Client connections accepted at once (`--max-connections`); the
/// default mirrors the node's `MAX_CONNECTIONS`.
const DEFAULT_MAX_CONNECTIONS: usize = 1024;

/// Issue #233: metrics/health/ready connections accepted at once — a
/// small, fixed cap dedicated to the metrics listener, independent of
/// `--max-connections` (that semaphore is only read here for the
/// `nanocached_proxy_client_connections` gauge, it never governs this
/// listener). A scrape storm or a stuck orchestrator probe on this port
/// shouldn't be able to spawn an unbounded number of tasks.
const METRICS_MAX_CONNECTIONS: usize = 16;

/// Client connections idle longer than this are closed — the node's own
/// idle policy, mirrored so a proxy hop doesn't change lifecycle
/// expectations (SDK keep-alives flow through and reset it).
const IDLE_TIMEOUT: Duration = Duration::from_secs(60);

/// Issue #420: bounds each write to a client socket, the same way
/// `IDLE_TIMEOUT` bounds a client's reads. Without this, a client that
/// stops reading its socket leaves `handle_client`'s writer task parked
/// forever inside `write_all` — and since `IDLE_TIMEOUT` firing on the
/// reader side only leads to `drop(fifo_tx); writer.await`, that wait
/// itself would then block forever on the stuck writer, so
/// `handle_client` never returns and its `max_connections` permit is
/// never released. Kept equal to `IDLE_TIMEOUT` in production — both are
/// "how unresponsive can this client be" bounds on the same connection.
#[cfg(not(test))]
const CLIENT_WRITE_TIMEOUT: Duration = IDLE_TIMEOUT;
/// Issue #420: shrunk under test like `UPSTREAM_IO_TIMEOUT`, so a test
/// that stalls a client's reads to exercise this timeout doesn't have to
/// pay out `IDLE_TIMEOUT`'s full 60 real seconds.
#[cfg(test)]
const CLIENT_WRITE_TIMEOUT: Duration = Duration::from_millis(600);

/// Tenth-pass audit (2026-09-02): bounds the TLS handshake performed on
/// an accepted connection, once TLS is configured (`--tls-cert`/
/// `--tls-key`). Without it, a peer that completes the TCP handshake and
/// then never sends a ClientHello holds its `max_connections` permit
/// (acquired before the handshake, same as the node) forever, and enough
/// such connections starve every legitimate client with indefinite `B`
/// busy replies. Mirrors the node's own `TLS_HANDSHAKE_TIMEOUT`.
#[cfg(not(test))]
const TLS_HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(10);
/// Tenth-pass audit (2026-09-02): shrunk under test like
/// `CLIENT_WRITE_TIMEOUT`, so a test that stalls a client's ClientHello
/// to exercise this timeout doesn't have to pay out 10 real seconds.
#[cfg(test)]
const TLS_HANDSHAKE_TIMEOUT: Duration = Duration::from_millis(600);

/// Every backend/discovery I/O interaction is bounded by this, so one
/// hung upstream can't pin a driver task forever.
#[cfg(not(test))]
const UPSTREAM_IO_TIMEOUT: Duration = Duration::from_secs(5);
/// Issue #177: shrunk under test so the black-hole/dial-backoff tests
/// (which pay this out in real wall-clock time — `tokio::time::pause`'s
/// auto-advance isn't reliable once a real, permanently-unresponsive
/// socket is in the mix) run in milliseconds instead of seconds.
/// Comfortably above every mock node's own injected `get_delay` (the
/// largest in this suite is 200ms), so existing delay-based tests are
/// unaffected.
#[cfg(test)]
const UPSTREAM_IO_TIMEOUT: Duration = Duration::from_millis(600);

/// How often the roster is re-fetched in the background. `W` answers
/// force an immediate refresh regardless, so this only bounds how stale
/// the view can get while nothing is being rerouted.
const REFRESH_INTERVAL: Duration = Duration::from_secs(5);

/// Issue #110: how many requests may sit written-but-unanswered on one
/// shared backend connection. The writer stalls (backpressuring every
/// client queued behind it) once this many replies are outstanding, so
/// one slow node bounds memory instead of growing an unbounded reply
/// ledger.
const MAX_BACKEND_IN_FLIGHT: usize = 256;

/// Issue #110: how many requests may queue for a backend connection's
/// writer before enqueueing clients start waiting. Queueing is FIFO
/// across client connections — that, plus this bound and
/// `CLIENT_IN_FLIGHT`, is the fairness story: no client can occupy more
/// of a shared backend than its own in-flight allowance, and admission
/// is strictly arrival-ordered.
const BACKEND_QUEUE_DEPTH: usize = 256;

/// Issue #177: how long `SharedBackends::enqueue` remembers a failed
/// dial to one address before it will try dialing that address again.
/// Within this window a fresh `enqueue`/`call` fails immediately with
/// the address's last-known-bad status instead of re-dialing and paying
/// another full `UPSTREAM_IO_TIMEOUT` — a black-holed node's repeat
/// traffic should cost one timeout, not one per request. Cleared the
/// instant a dial to that address succeeds.
const DIAL_BACKOFF: Duration = Duration::from_secs(1);

/// How many responses one client connection may have outstanding (the
/// reader stops parsing new requests once this many are undelivered) —
/// both the per-client in-flight cap and the per-client share bound on
/// any shared backend.
const CLIENT_IN_FLIGHT: usize = 256;

/// Bounds on the `L` response, mirroring `verify-staged-join`'s: a
/// corrupt header must not drive allocations or blocking reads.
const MAX_ROSTER_ENTRIES: usize = 4096;
const MAX_NAME_OR_ADDR_LENGTH: usize = 1024;

/// This process's own identity toward discovery (issue #122): a random
/// per-process name (like a node's) and the token that pins it — see the
/// discovery side's `ProxyInfo::token`.
struct ProxyIdentity {
    name: String,
    token: String,
}

impl ProxyIdentity {
    fn generate() -> Self {
        Self {
            name: uuid::Uuid::new_v4().to_string(),
            token: format!("tk-{}", uuid::Uuid::new_v4()),
        }
    }
}

const AUTH_SECRET_ENV_VAR: &str = "NANOCACHED_AUTH_SECRET";
/// The name the proxy read before issue #165 — node and discovery always
/// used `NANOCACHED_AUTH_SECRET`, so an operator following the docs got a
/// proxy with auth silently off. Honoured for one release as a fallback,
/// with a warning, so an existing deployment keeps working while it
/// renames the variable.
const LEGACY_AUTH_SECRET_ENV_VAR: &str = "NANOCACHED_SECRET";

fn read_auth_secret() -> Option<Bytes> {
    read_auth_secret_from(|name| std::env::var(name).ok())
}

fn read_auth_secret_from(var: impl Fn(&str) -> Option<String>) -> Option<Bytes> {
    let non_empty = |value: String| (!value.is_empty()).then_some(value);
    if let Some(secret) = var(AUTH_SECRET_ENV_VAR).and_then(non_empty) {
        return Some(Bytes::from(secret));
    }
    let legacy = var(LEGACY_AUTH_SECRET_ENV_VAR).and_then(non_empty)?;
    eprintln!(
        "WARN {LEGACY_AUTH_SECRET_ENV_VAR} is deprecated and will stop being read in a future \
         release; set {AUTH_SECRET_ENV_VAR} instead (the name node and discovery use)"
    );
    Some(Bytes::from(legacy))
}

// ─── HRW ring (independent re-implementation, see module docs) ───────

fn fnv1a(bytes: &[u8]) -> u64 {
    fnv1a_continue(0xcbf29ce484222325, bytes)
}

fn fnv1a_continue(mut hash: u64, bytes: &[u8]) -> u64 {
    for &byte in bytes {
        hash ^= byte as u64;
        hash = hash.wrapping_mul(0x100000001b3);
    }
    hash
}

fn fmix64(mut hash: u64) -> u64 {
    hash ^= hash >> 33;
    hash = hash.wrapping_mul(0xff51afd7ed558ccd);
    hash ^= hash >> 33;
    hash = hash.wrapping_mul(0xc4ceb9fe1a85ec53);
    hash ^= hash >> 33;
    hash
}

/// The canonical key-side hash (issue #105): the legacy form for the
/// default namespace, `fnv1a(be32(len(ns)) || ns || key)` otherwise.
fn key_hash(namespace: &[u8], key: &[u8]) -> u64 {
    if namespace.is_empty() {
        return fnv1a(key);
    }
    let length = u32::try_from(namespace.len())
        .expect("a namespace is bounded by the request-size limit")
        .to_be_bytes();
    let hash = fnv1a(&length);
    let hash = fnv1a_continue(hash, namespace);
    fnv1a_continue(hash, key)
}

/// One fetched roster: the cluster members (name, addr) and discovery's
/// replication factor, plus the members' precomputed name hashes.
struct RingView {
    nodes: Vec<(String, String)>,
    node_hashes: Vec<u64>,
    replication: usize,
}

impl RingView {
    fn new(nodes: Vec<(String, String)>, replication: usize) -> Self {
        let node_hashes = nodes
            .iter()
            .map(|(name, _)| fnv1a(name.as_bytes()))
            .collect();
        Self {
            nodes,
            node_hashes,
            replication,
        }
    }

    /// The key's owner *addresses*, primary first — top-R by the same
    /// total order every implementation uses (descending score, ties to
    /// the lexicographically smaller name).
    fn owners(&self, namespace: &[u8], key: &[u8]) -> Vec<String> {
        let key_hash = key_hash(namespace, key);
        let mut scored: Vec<(u64, &(String, String))> = self
            .node_hashes
            .iter()
            .zip(&self.nodes)
            .map(|(node_hash, node)| (fmix64(node_hash ^ key_hash), node))
            .collect();
        scored.sort_unstable_by(|a, b| b.0.cmp(&a.0).then_with(|| a.1.0.cmp(&b.1.0)));
        scored.truncate(self.replication.min(scored.len()));
        scored
            .into_iter()
            .map(|(_, (_, addr))| addr.clone())
            .collect()
    }

    /// Every member address — `c`/`F`'s fan-out set.
    fn all_addresses(&self) -> Vec<String> {
        self.nodes.iter().map(|(_, addr)| addr.clone()).collect()
    }
}

// ─── args ────────────────────────────────────────────────────────────

struct Args {
    host: String,
    port: u16,
    discovery: Vec<String>,
    max_connections: usize,
    tls_cert: Option<String>,
    tls_key: Option<String>,
    tls_ca: Option<String>,
    /// Issue #124: port for /metrics + /healthz + /readyz on `host`;
    /// `None` = no operations endpoint.
    metrics_port: Option<u16>,
    /// Issue #124: how long a drain (SIGTERM/SIGINT) waits for open
    /// client connections to finish their in-flight requests before the
    /// process exits anyway. Must fit inside the orchestrator's kill
    /// grace (ECS `stopTimeout`, k8s `terminationGracePeriodSeconds`).
    drain_timeout: Duration,
}

impl Default for Args {
    fn default() -> Self {
        Self {
            host: "127.0.0.1".to_string(),
            port: 8358,
            discovery: Vec::new(),
            max_connections: DEFAULT_MAX_CONNECTIONS,
            tls_cert: None,
            tls_key: None,
            tls_ca: None,
            metrics_port: None,
            drain_timeout: Duration::from_secs(25),
        }
    }
}

enum ArgsError {
    Help(String),
    Invalid(String),
}

impl From<String> for ArgsError {
    fn from(message: String) -> Self {
        ArgsError::Invalid(message)
    }
}

fn usage() -> String {
    "usage: nanocached-proxy --discovery <host:port>[,<host:port>...] \
     [--host <host>] [--port <port>] [--max-connections <n>] \
     [--tls-cert <pem> --tls-key <pem>] [--tls-ca <pem>] [--metrics-port <port>]\n\
     [--drain-timeout <secs>]\n\
     The shared auth secret is read from NANOCACHED_AUTH_SECRET."
        .to_string()
}

fn parse_args_from(mut raw: impl Iterator<Item = String>) -> Result<Args, ArgsError> {
    let mut args = Args::default();

    while let Some(flag) = raw.next() {
        let mut value = || raw.next().ok_or_else(|| format!("{flag} requires a value"));

        match flag.as_str() {
            "--host" => args.host = value()?,
            "--port" => {
                args.port = value()?
                    .parse()
                    .map_err(|_| "--port must be a number between 0 and 65535".to_string())?;
            }
            "--discovery" => {
                args.discovery = value()?
                    .split(',')
                    .map(|address| address.trim().to_string())
                    .filter(|address| !address.is_empty())
                    .collect();
            }
            "--max-connections" => {
                let parsed: usize = value()?
                    .parse()
                    .map_err(|_| "--max-connections must be a number".to_string())?;
                if parsed == 0 {
                    return Err("--max-connections must be at least 1".to_string().into());
                }
                args.max_connections = parsed;
            }
            "--tls-cert" => args.tls_cert = Some(value()?),
            "--tls-key" => args.tls_key = Some(value()?),
            "--tls-ca" => args.tls_ca = Some(value()?),
            "--metrics-port" => {
                args.metrics_port = Some(value()?.parse().map_err(|_| {
                    "--metrics-port must be a number between 0 and 65535".to_string()
                })?);
            }
            "--drain-timeout" => {
                let secs: u64 = value()?
                    .parse()
                    .map_err(|_| "--drain-timeout must be a number of seconds".to_string())?;
                args.drain_timeout = Duration::from_secs(secs);
            }
            "-h" | "--help" => return Err(ArgsError::Help(usage())),
            unknown => return Err(format!("unknown flag: {unknown}\n{}", usage()).into()),
        }
    }

    if args.discovery.is_empty() {
        return Err(
            "--discovery is required (the proxy routes by the cluster's roster)"
                .to_string()
                .into(),
        );
    }
    if args.tls_cert.is_some() != args.tls_key.is_some() {
        return Err("--tls-cert and --tls-key must be set together"
            .to_string()
            .into());
    }

    Ok(args)
}

// ─── TLS plumbing (mirrors src/server.rs's, see the module docs on
//     independent re-implementation) ─────────────────────────────────

use rustls::pki_types::{CertificateDer, PrivateKeyDer, ServerName};
use rustls::{ClientConfig, RootCertStore, ServerConfig};
use std::io::BufReader;
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio_rustls::{TlsAcceptor, TlsConnector};

enum MaybeTls<P, T> {
    Plain(P),
    Tls(Box<T>),
}

type ServerStream = MaybeTls<TcpStream, tokio_rustls::server::TlsStream<TcpStream>>;
type UpstreamStream = MaybeTls<TcpStream, tokio_rustls::client::TlsStream<TcpStream>>;

impl<P: AsyncRead + Unpin, T: AsyncRead + Unpin> AsyncRead for MaybeTls<P, T> {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
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

fn load_tls_acceptor(cert_path: &str, key_path: &str) -> io::Result<TlsAcceptor> {
    let certs = load_cert_chain(cert_path)?;
    let key = load_private_key(key_path)?;
    let config = ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(certs, key)
        .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
    Ok(TlsAcceptor::from(Arc::new(config)))
}

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

/// Everything the per-connection tasks need, shared once.
struct ProxyContext {
    secret: Option<Bytes>,
    tls_connector: Option<TlsConnector>,
    /// The latest fetched roster; `None` until the first successful
    /// fetch (connections arriving before that answer `B` — the same
    /// "not ready yet, retry" clients already handle from discovery).
    ring: watch::Receiver<Option<Arc<RingView>>>,
    /// Nudges the refresher for an immediate re-fetch (a `W` was seen or
    /// a clear fan-out failed) instead of waiting out the interval.
    refresh_now: mpsc::Sender<()>,
    /// Issue #110: the proxy-wide shared backend connections — one
    /// tagged, pipelined connection per node, multiplexing every client
    /// connection's traffic. This is what collapses the node-side
    /// connection count from "client connections × nodes" to "one per
    /// node per proxy". `Arc`-wrapped so the roster refresher (issue
    /// #220: pruning stale addresses on each fresh `RingView`) can hold
    /// the same instance independently of this context.
    backends: Arc<SharedBackends>,
    /// Issue #124: flips to `true` when a drain begins (SIGTERM/SIGINT).
    /// Connection readers stop taking new requests (writers still
    /// deliver what is in flight), the accept loop stops, `/readyz`
    /// answers 503, and the refresher deregisters from discovery.
    drain: watch::Receiver<bool>,
    /// Issue #124: requests dispatched (any op), for the metrics
    /// endpoint's rate signal.
    requests_total: std::sync::atomic::AtomicU64,
    /// Issue #124: requests that ended in the fatal `E` path (upstream
    /// failure that survived the retry) — the error-rate signal.
    upstream_failures_total: std::sync::atomic::AtomicU64,
}

// ─── shared line/frame helpers ───────────────────────────────────────

async fn read_line<S: AsyncRead + Unpin>(stream: &mut S, buf: &mut BytesMut) -> io::Result<String> {
    loop {
        if let Some(position) = buf.iter().position(|byte| *byte == b'\n') {
            let line = buf.split_to(position + 1);
            return String::from_utf8(line[..position].to_vec())
                .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "non-UTF-8 header"));
        }
        if buf.len() > MAX_REQUEST_SIZE {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "header exceeds the request-size limit",
            ));
        }
        let mut chunk = [0u8; 4096];
        let bytes_read = stream.read(&mut chunk).await?;
        if bytes_read == 0 {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "connection closed mid-frame",
            ));
        }
        buf.extend_from_slice(&chunk[..bytes_read]);
    }
}

async fn read_exact_into<S: AsyncRead + Unpin>(
    stream: &mut S,
    buf: &mut BytesMut,
    length: usize,
) -> io::Result<()> {
    while buf.len() < length {
        let mut chunk = [0u8; 4096];
        let bytes_read = stream.read(&mut chunk).await?;
        if bytes_read == 0 {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "connection closed mid-frame",
            ));
        }
        buf.extend_from_slice(&chunk[..bytes_read]);
    }
    Ok(())
}

// ─── discovery client ────────────────────────────────────────────────

/// Fetches `L` from the fastest answering discovery replica. A `B` (the
/// replica's startup grace) or connect failure falls back to another
/// replica; `Err` only once every replica has failed.
///
/// Issue #409: every replica is raced concurrently (`race_ok`) rather
/// than tried one at a time — this was the one multi-address fan-out
/// left sequential by the #177 pass. A `for` loop here meant a
/// black-holed first replica cost a full `UPSTREAM_IO_TIMEOUT` on every
/// refresh cycle (`run_refresher`'s `REFRESH_INTERVAL` tick and every
/// `force_refresh`) before the next replica was even dialed, directly
/// delaying `/readyz` and `force_refresh` convergence behind it.
async fn fetch_roster(
    context_discovery: &[String],
    secret: &Option<Bytes>,
    tls_connector: &Option<TlsConnector>,
) -> io::Result<Arc<RingView>> {
    if context_discovery.is_empty() {
        return Err(io::Error::other("no discovery replicas configured"));
    }

    let futs: Vec<RaceFuture<'_, Arc<RingView>>> = context_discovery
        .iter()
        .map(|addr| {
            let fut = async move {
                match timeout(
                    UPSTREAM_IO_TIMEOUT,
                    fetch_roster_from(addr, secret, tls_connector),
                )
                .await
                {
                    Ok(result) => result,
                    Err(_) => Err(io::Error::new(
                        io::ErrorKind::TimedOut,
                        format!("L fetch from {addr} timed out"),
                    )),
                }
            };
            Box::pin(fut) as RaceFuture<'_, Arc<RingView>>
        })
        .collect();

    race_ok(futs).await
}

async fn fetch_roster_from(
    addr: &str,
    secret: &Option<Bytes>,
    tls_connector: &Option<TlsConnector>,
) -> io::Result<Arc<RingView>> {
    let mut stream = connect_upstream(addr, tls_connector).await?;
    let mut buf = BytesMut::new();

    if let Some(secret) = secret {
        let mut auth = format!("A {}\n", secret.len()).into_bytes();
        auth.extend_from_slice(secret);
        stream.write_all(&auth).await?;
        let ack = read_line(&mut stream, &mut buf).await?;
        if !ack.starts_with("Od") && !ack.starts_with("On") {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                format!("discovery at {addr} rejected authentication: {ack:?}"),
            ));
        }
    }

    stream.write_all(b"L\n").await?;
    let header = read_line(&mut stream, &mut buf).await?;

    if header == "B" {
        return Err(io::Error::new(
            io::ErrorKind::WouldBlock,
            format!("discovery at {addr} is in its startup grace"),
        ));
    }

    let bad_header = || io::Error::new(io::ErrorKind::InvalidData, "bad L header");
    let mut parts = header.strip_prefix("N ").ok_or_else(bad_header)?.split(' ');
    let count: usize = parts
        .next()
        .and_then(|c| c.parse().ok())
        .ok_or_else(bad_header)?;
    let replication: usize = parts
        .next()
        .and_then(|r| r.parse().ok())
        .ok_or_else(bad_header)?;

    if count > MAX_ROSTER_ENTRIES || replication == 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "L header out of bounds",
        ));
    }

    let mut nodes = Vec::with_capacity(count);
    for _ in 0..count {
        let entry_header = read_line(&mut stream, &mut buf).await?;
        let mut parts = entry_header.split(' ');
        let bad_entry = || io::Error::new(io::ErrorKind::InvalidData, "bad L entry header");
        let name_length: usize = parts
            .next()
            .and_then(|p| p.parse().ok())
            .ok_or_else(bad_entry)?;
        let addr_length: usize = parts
            .next()
            .and_then(|p| p.parse().ok())
            .ok_or_else(bad_entry)?;
        if name_length > MAX_NAME_OR_ADDR_LENGTH || addr_length > MAX_NAME_OR_ADDR_LENGTH {
            return Err(bad_entry());
        }
        // +1: discovery writes a trailing '\n' after each entry body.
        read_exact_into(&mut stream, &mut buf, name_length + addr_length + 1).await?;
        let entry = buf.split_to(name_length + addr_length + 1);
        let name = String::from_utf8_lossy(&entry[..name_length]).into_owned();
        let node_addr =
            String::from_utf8_lossy(&entry[name_length..name_length + addr_length]).into_owned();
        nodes.push((name, node_addr));
    }

    Ok(Arc::new(RingView::new(nodes, replication)))
}

async fn connect_upstream(
    addr: &str,
    tls_connector: &Option<TlsConnector>,
) -> io::Result<UpstreamStream> {
    let stream = TcpStream::connect(addr).await?;
    let _ = stream.set_nodelay(true);
    match tls_connector {
        None => Ok(MaybeTls::Plain(stream)),
        Some(connector) => {
            let server_name = server_name_from_addr(addr)?;
            let tls = connector.connect(server_name, stream).await?;
            Ok(MaybeTls::Tls(Box::new(tls)))
        }
    }
}

/// One `Y` announce to one discovery replica (issue #122): the declared
/// port composes with this connection's source IP on the discovery side,
/// exactly like a node's `J`/`P` (same NAT caveat).
async fn announce_to(
    addr: &str,
    secret: &Option<Bytes>,
    tls_connector: &Option<TlsConnector>,
    identity: &ProxyIdentity,
    port: u16,
) -> io::Result<()> {
    timeout(UPSTREAM_IO_TIMEOUT, async {
        let mut stream = connect_upstream(addr, tls_connector).await?;
        let mut buf = BytesMut::new();

        if let Some(secret) = secret {
            let mut auth = format!("A {}\n", secret.len()).into_bytes();
            auth.extend_from_slice(secret);
            stream.write_all(&auth).await?;
            let ack = read_line(&mut stream, &mut buf).await?;
            if !ack.starts_with("Od") && !ack.starts_with("On") {
                return Err(io::Error::new(
                    io::ErrorKind::PermissionDenied,
                    format!("discovery at {addr} rejected authentication: {ack:?}"),
                ));
            }
        }

        let mut frame = format!(
            "Y {} {port} {}\n",
            identity.name.len(),
            identity.token.len()
        )
        .into_bytes();
        frame.extend_from_slice(identity.name.as_bytes());
        frame.extend_from_slice(identity.token.as_bytes());
        stream.write_all(&frame).await?;

        let ack = read_line(&mut stream, &mut buf).await?;
        if ack != "R" {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("discovery at {addr} rejected the proxy announce: {ack:?}"),
            ));
        }
        Ok(())
    })
    .await
    .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "proxy announce timed out"))?
}

/// The background roster refresher: one fetch at startup, then again
/// every `REFRESH_INTERVAL` or whenever something sends on `refresh_rx`
/// (a `W`, a failed fan-out). Failures keep the last good view.
/// `run_refresher`'s inputs, bundled (clippy's argument-count lint).
struct RefresherConfig {
    discovery: Vec<String>,
    secret: Option<Bytes>,
    tls_connector: Option<TlsConnector>,
    announce: Option<(ProxyIdentity, u16)>,
    /// Issue #124: when this flips, the refresher deregisters (`Z`) from
    /// every replica and exits — running the deregistration in the same
    /// task that sends announces is what guarantees no `Y` follows the
    /// `Z` and quietly re-registers a draining proxy.
    drain: watch::Receiver<bool>,
}

async fn run_refresher(
    config: RefresherConfig,
    ring_tx: watch::Sender<Option<Arc<RingView>>>,
    mut refresh_rx: mpsc::Receiver<()>,
    backends: Arc<SharedBackends>,
) {
    let RefresherConfig {
        discovery,
        secret,
        tls_connector,
        announce,
        mut drain,
    } = config;

    loop {
        if *drain.borrow() {
            break;
        }

        match fetch_roster(&discovery, &secret, &tls_connector).await {
            Ok(ring) => {
                // Issue #220: prune `slots`/`dial_failures` against the
                // fresh view right here, where the new roster is known —
                // an address that dropped off the ring stops accumulating
                // map entries from this point on.
                backends.prune(&ring);
                let _ = ring_tx.send(Some(ring));
            }
            Err(error) => {
                eprintln!("WARN roster refresh failed: {error}");
            }
        }

        // Issue #122: (re-)announce this proxy on the same cadence, to
        // every replica — each keeps its own proxy map (they don't
        // gossip), and re-announcing is what keeps the entry alive past
        // the liveness timeout. Failures only warn: an unreachable
        // replica can't take the proxy down, it just won't list it.
        // Checked against the drain flag before sending, so a drain
        // already in progress at the start of a cycle can't re-register
        // this proxy. Fanned out concurrently (like every other
        // multi-replica path here) rather than serially, so one slow or
        // unreachable replica can't stretch a cycle to the sum of all
        // replicas' round-trips.
        if let Some((identity, port)) = &announce
            && !*drain.borrow()
        {
            // Bind shared references once so each `async move` future
            // captures a Copy of the reference, not the owned value.
            let (secret_ref, tls_ref, port_val) = (&secret, &tls_connector, *port);
            let futs: Vec<Pin<Box<dyn Future<Output = ()> + Send>>> = discovery
                .iter()
                .map(|addr| {
                    Box::pin(async move {
                        if let Err(error) =
                            announce_to(addr, secret_ref, tls_ref, identity, port_val).await
                        {
                            eprintln!("WARN proxy announce to {addr} failed: {error}");
                        }
                    }) as Pin<Box<dyn Future<Output = ()> + Send>>
                })
                .collect();
            join_all(futs).await;
        }

        tokio::select! {
            _ = sleep(REFRESH_INTERVAL) => {}
            () = drained(&mut drain) => {}
            received = refresh_rx.recv() => {
                if received.is_none() {
                    return;
                }
                // Coalesce a burst of W-triggered nudges into one fetch.
                while refresh_rx.try_recv().is_ok() {}
            }
        }
    }

    // Issue #124: leave `Q` immediately — a stopped proxy must not
    // linger there until the liveness timeout, where new clients would
    // keep dialing it. Fanned out concurrently so the shutdown's single
    // `UPSTREAM_IO_TIMEOUT` budget (see `run`) covers all replicas'
    // deregistrations in parallel rather than needing their sum, which
    // would let a slow replica eat the budget before the others' `Z`
    // frames go out.
    if let Some((identity, _)) = &announce {
        let (secret_ref, tls_ref) = (&secret, &tls_connector);
        let futs: Vec<Pin<Box<dyn Future<Output = ()> + Send>>> = discovery
            .iter()
            .map(|addr| {
                Box::pin(async move {
                    if let Err(error) = deregister_from(addr, secret_ref, tls_ref, identity).await {
                        eprintln!("WARN proxy deregister to {addr} failed: {error}");
                    }
                }) as Pin<Box<dyn Future<Output = ()> + Send>>
            })
            .collect();
        join_all(futs).await;
    }
}

/// One `Z` deregistration to one discovery replica (issue #124).
async fn deregister_from(
    addr: &str,
    secret: &Option<Bytes>,
    tls_connector: &Option<TlsConnector>,
    identity: &ProxyIdentity,
) -> io::Result<()> {
    timeout(UPSTREAM_IO_TIMEOUT, async {
        let mut stream = connect_upstream(addr, tls_connector).await?;
        let mut buf = BytesMut::new();

        if let Some(secret) = secret {
            let mut auth = format!("A {}\n", secret.len()).into_bytes();
            auth.extend_from_slice(secret);
            stream.write_all(&auth).await?;
            let ack = read_line(&mut stream, &mut buf).await?;
            if !ack.starts_with("Od") && !ack.starts_with("On") {
                return Err(io::Error::new(
                    io::ErrorKind::PermissionDenied,
                    format!("discovery at {addr} rejected authentication: {ack:?}"),
                ));
            }
        }

        let mut frame =
            format!("Z {} {}\n", identity.name.len(), identity.token.len()).into_bytes();
        frame.extend_from_slice(identity.name.as_bytes());
        frame.extend_from_slice(identity.token.as_bytes());
        stream.write_all(&frame).await?;

        let ack = read_line(&mut stream, &mut buf).await?;
        if ack != "R" {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("discovery at {addr} rejected the proxy deregister: {ack:?}"),
            ));
        }
        Ok(())
    })
    .await
    .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "proxy deregister timed out"))?
}

// ─── client frames ───────────────────────────────────────────────────

/// One parsed client request. The proxy fully parses (rather than
/// splicing bytes through) because routing needs the namespace and key,
/// and because backend frames are re-tagged with proxy-chosen tags.
#[derive(Debug, PartialEq)]
enum Request {
    Get {
        namespace: Bytes,
        key: Bytes,
    },
    Set {
        namespace: Bytes,
        key: Bytes,
        value: Bytes,
        ttl: Option<u64>,
    },
    Delete {
        namespace: Bytes,
        key: Bytes,
    },
    /// Issue #129: always namespaced on the wire (an empty `namespace`
    /// addresses the default one) — `INCR` has no pre-namespace legacy
    /// form, unlike `Get`/`Set`/`Delete`.
    Incr {
        namespace: Bytes,
        key: Bytes,
        delta: i64,
    },
    /// Issue #141: `k` (compare-and-set). Always namespaced, same
    /// reasoning as `Incr` — this op has no pre-namespace legacy form.
    CasSet {
        namespace: Bytes,
        key: Bytes,
        condition: CasCondition,
        value: Bytes,
        ttl: Option<u64>,
    },
    /// Issue #141: `x` (compare-and-delete). `condition` is always a
    /// digest here — the node rejects `A`/`P` for `x` as a fatal parse
    /// error (an absent- or present-only conditioned delete is already
    /// the plain, unconditional `d`), and this parser holds `x` to the
    /// same rule.
    CasDelete {
        namespace: Bytes,
        key: Bytes,
        expected_digest: [u8; 16],
    },
    Clear {
        namespace: Bytes,
    },
    ClearAll,
    /// Issues #128/#150: `m` (batched get). Always namespaced, same
    /// reasoning as `Incr`/CAS — no pre-namespace legacy form. See
    /// `dispatch_request`'s arm for the owner-grouping fan-out.
    MultiGet {
        namespace: Bytes,
        keys: Vec<Bytes>,
    },
    /// Issue #150: `o` (batched set). Always namespaced. One shared
    /// `ttl` for the whole batch, not per-key — see `Command::MultiSet`'s
    /// (`src/command.rs`) doc comment for why. See `dispatch_request`'s
    /// arm for the by-owner-address fan-out (unlike `MultiGet`'s
    /// by-primary grouping, since a write must reach every owner).
    MultiSet {
        namespace: Bytes,
        keys: Vec<Bytes>,
        values: Vec<Bytes>,
        ttl: Option<u64>,
    },
}

/// Issue #141: `k`'s (and `x`'s) `<cond>` field, decoded. Independent
/// reimplementation of the node's `crate::cache::CasCondition` — this
/// binary shares no modules with the node (see `parse_delta_field`'s doc
/// comment for the established policy).
#[derive(Debug, Clone, Copy, PartialEq)]
enum CasCondition {
    Absent,
    Present,
    Digest([u8; 16]),
}

#[derive(Debug, PartialEq)]
enum ParseOutcome {
    /// A full request (plus the client's tag in tagged mode) was
    /// consumed from the buffer.
    Ready(Request, Option<u32>),
    /// More bytes are needed; the buffer is untouched.
    Incomplete,
    /// `A <len> [T] [R]` — handled by the caller (auth state lives
    /// there). `retry_capable` (issue #125): the client understands the
    /// retryable-error status `R`.
    Auth {
        secret: Bytes,
        tagging: bool,
        retry_capable: bool,
    },
}

fn parse_length_field(field: &str) -> io::Result<usize> {
    if field.is_empty() || !field.bytes().all(|byte| byte.is_ascii_digit()) {
        return Err(invalid("bad length field"));
    }
    field
        .parse()
        .map_err(|_| invalid("length field out of range"))
}

fn invalid(message: &str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message.to_string())
}

/// Issue #129: `INCR`'s canonical decimal-ASCII `i64` grammar for the
/// wire `<delta>` field — mirrors the node's own `parse_decimal_i64`
/// (`src/cache.rs`) rather than sharing it: this binary re-implements
/// the node's parsing independently throughout (see `RingView`'s own
/// hash functions below for the established precedent — no modules are
/// shared between the node and proxy binaries), pinned against the same
/// grammar by `incr_delta_matches_the_node_grammar`.
fn parse_delta_field(field: &str) -> io::Result<i64> {
    let (negative, digits) = match field.strip_prefix('-') {
        Some(rest) => (true, rest),
        None => (false, field),
    };
    if digits.is_empty() || !digits.bytes().all(|byte| byte.is_ascii_digit()) {
        return Err(invalid("bad delta field"));
    }
    if digits.len() > 1 && digits.starts_with('0') {
        return Err(invalid("bad delta field"));
    }
    let magnitude: u64 = digits
        .parse()
        .map_err(|_| invalid("delta field out of range"))?;
    if negative {
        // i64::MIN's magnitude has no positive i64 representation, so it
        // can't go through `i64::try_from` and negate like every other
        // value — same special case `parse_decimal_i64` handles.
        if magnitude == i64::MIN.unsigned_abs() {
            Ok(i64::MIN)
        } else {
            i64::try_from(magnitude)
                .map(|value| -value)
                .map_err(|_| invalid("delta field out of range"))
        }
    } else {
        i64::try_from(magnitude).map_err(|_| invalid("delta field out of range"))
    }
}

/// Issue #141: `k`'s (and `x`'s) `<cond>` field — a fixed-shape bare
/// token, not a length field: exactly `A`, exactly `P` (only when
/// `allow_absent_present`, which `x`'s parse arm sets false — see
/// `Request::CasDelete`'s doc comment), or exactly 32 lowercase hex
/// digits. Independent reimplementation of the node's
/// `command::parse_cas_condition`, same "no shared modules" policy as
/// `parse_delta_field`.
fn parse_cas_condition_field(field: &str, allow_absent_present: bool) -> io::Result<CasCondition> {
    if allow_absent_present && field == "A" {
        return Ok(CasCondition::Absent);
    }
    if allow_absent_present && field == "P" {
        return Ok(CasCondition::Present);
    }
    let bytes = field.as_bytes();
    if bytes.len() == 32 {
        let mut digest = [0u8; 16];
        for (byte, chunk) in digest.iter_mut().zip(bytes.chunks_exact(2)) {
            *byte = (hex_nibble_field(chunk[0])? << 4) | hex_nibble_field(chunk[1])?;
        }
        return Ok(CasCondition::Digest(digest));
    }
    Err(invalid("bad cas condition field"))
}

fn hex_nibble_field(byte: u8) -> io::Result<u8> {
    match byte {
        b'0'..=b'9' => Ok(byte - b'0'),
        b'a'..=b'f' => Ok(byte - b'a' + 10),
        _ => Err(invalid("bad cas condition field")),
    }
}

/// Per-connection state for `parse_request`'s header-`\n` scan, carried
/// across calls the same way the node's `MigrateProgress` carries its
/// resumable scans (`src/command.rs`). Without this, a header trickling
/// in one small `read()` at a time made `handle_client`'s parse loop
/// re-scan the whole buffered prefix for `\n` on every call — quadratic
/// in the header length, and reachable pre-auth. `header_end`, once
/// found, is reused outright; otherwise `header_scanned_to` records how
/// far the scan already confirmed there is no `\n`, so the next call
/// only examines newly arrived bytes.
///
/// Valid only while `input`'s front is unchanged: `parse_request` resets
/// this on every outcome other than `Incomplete` (a full parse consumes
/// the front via `split_to`; an error tears the connection down), never
/// on `Incomplete` alone, and the caller must not otherwise mutate
/// `input`'s front while reusing the same state.
#[derive(Debug, Default)]
struct RequestScanState {
    header_end: Option<usize>,
    header_scanned_to: usize,
}

/// Finds the header-terminating `\n`, resuming `scan`'s prior progress
/// instead of re-scanning bytes already confirmed `\n`-free.
fn find_header_end(input: &[u8], scan: &mut RequestScanState) -> Option<usize> {
    if let Some(end) = scan.header_end
        && input.len() > end
    {
        return Some(end);
    }
    // Shouldn't happen (see the invariant above), but fall through to a
    // fresh scan rather than trust a now out-of-range `header_end`.
    let start = scan.header_scanned_to.min(input.len());
    #[cfg(test)]
    LF_SCAN_BYTES.with(|scanned| scanned.set(scanned.get() + (input.len() - start)));
    match input[start..].iter().position(|byte| *byte == b'\n') {
        Some(relative) => {
            let end = start + relative;
            scan.header_end = Some(end);
            Some(end)
        }
        None => {
            scan.header_scanned_to = input.len();
            None
        }
    }
}

// Total bytes ever handed to `find_header_end`'s `\n` scan on the current
// thread — a proxy for "did the header scan go quadratic" that a unit
// test can check without relying on wall-clock timing (flaky under CI
// load). Reset with `reset_lf_scan_counter` at the start of a test.
#[cfg(test)]
thread_local! {
    static LF_SCAN_BYTES: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

#[cfg(test)]
fn reset_lf_scan_counter() {
    LF_SCAN_BYTES.with(|scanned| scanned.set(0));
}

#[cfg(test)]
fn lf_scan_bytes() -> usize {
    LF_SCAN_BYTES.with(|scanned| scanned.get())
}

/// Parses one frame from the front of `input` (untouched on
/// `Incomplete`), mirroring the node's own grammar for the client-facing
/// commands. `M`/`X` and anything unknown error — the proxy is not a
/// cluster member (see the module docs). `scan` carries the header `\n`
/// scan's progress across calls — see `RequestScanState`.
fn parse_request(
    input: &mut BytesMut,
    tagged: bool,
    scan: &mut RequestScanState,
) -> io::Result<ParseOutcome> {
    let result = parse_request_body(input, tagged, scan);
    if !matches!(result, Ok(ParseOutcome::Incomplete)) {
        *scan = RequestScanState::default();
    }
    result
}

fn parse_request_body(
    input: &mut BytesMut,
    tagged: bool,
    scan: &mut RequestScanState,
) -> io::Result<ParseOutcome> {
    let Some(header_end) = find_header_end(input, scan) else {
        if input.len() > MAX_REQUEST_SIZE {
            return Err(invalid("header exceeds the request-size limit"));
        }
        return Ok(ParseOutcome::Incomplete);
    };

    let header =
        String::from_utf8(input[..header_end].to_vec()).map_err(|_| invalid("non-UTF-8 header"))?;
    let mut parts = header.split(' ');
    let command = parts.next().ok_or_else(|| invalid("empty header"))?;
    let fields: Vec<&str> = parts.collect();

    // In tagged mode every command's last header field is the client's
    // tag (`A` excepted — it *establishes* the mode).
    let split_tag = |fields: &[&str]| -> io::Result<(Vec<usize>, Option<u32>)> {
        if !tagged {
            let lengths = fields
                .iter()
                .map(|field| parse_length_field(field))
                .collect::<io::Result<Vec<_>>>()?;
            return Ok((lengths, None));
        }
        let (tag_field, rest) = fields.split_last().ok_or_else(|| invalid("missing tag"))?;
        let tag = u32::try_from(parse_length_field(tag_field)?)
            .map_err(|_| invalid("tag out of range"))?;
        let lengths = rest
            .iter()
            .map(|field| parse_length_field(field))
            .collect::<io::Result<Vec<_>>>()?;
        Ok((lengths, Some(tag)))
    };

    // Consumes header + `body_length` bytes once fully buffered.
    macro_rules! body {
        ($body_length:expr) => {{
            let body_length: usize = $body_length;
            let frame_end = header_end
                .checked_add(1)
                .and_then(|start| start.checked_add(body_length))
                .ok_or_else(|| invalid("frame length overflow"))?;
            if frame_end > MAX_REQUEST_SIZE {
                return Err(invalid("request exceeds the request-size limit"));
            }
            if input.len() < frame_end {
                return Ok(ParseOutcome::Incomplete);
            }
            input.split_to(frame_end).freeze().slice(header_end + 1..)
        }};
    }

    match command {
        "A" => {
            // `A <len> [T] [R]`; never carries a tag. The trailing `R`
            // (issue #125) is the client declaring it understands the
            // retryable-error status — the gate for answering `R`
            // instead of the fatal `E`-and-close below.
            let (length_field, tagging, retry_capable) = match fields.as_slice() {
                [length] => (*length, false, false),
                [length, "T"] => (*length, true, false),
                [length, "R"] => (*length, false, true),
                [length, "T", "R"] => (*length, true, true),
                _ => return Err(invalid("bad A header")),
            };
            let secret_length = parse_length_field(length_field)?;
            if secret_length == 0 {
                return Err(invalid("empty secret"));
            }
            let body = body!(secret_length);
            Ok(ParseOutcome::Auth {
                secret: body,
                tagging,
                retry_capable,
            })
        }

        "G" | "D" | "g" | "d" => {
            let (lengths, tag) = split_tag(&fields)?;
            let namespaced = command == "g" || command == "d";
            let (namespace_length, key_length) = match (namespaced, lengths.as_slice()) {
                (false, [key_length]) => (0, *key_length),
                (true, [namespace_length, key_length]) => (*namespace_length, *key_length),
                _ => return Err(invalid("bad get/delete header")),
            };
            if key_length == 0 {
                return Err(invalid("empty key"));
            }
            let body = body!(
                namespace_length
                    .checked_add(key_length)
                    .ok_or_else(|| invalid("frame length overflow"))?
            );
            let namespace = body.slice(..namespace_length);
            let key = body.slice(namespace_length..);
            let request = if command == "G" || command == "g" {
                Request::Get { namespace, key }
            } else {
                Request::Delete { namespace, key }
            };
            Ok(ParseOutcome::Ready(request, tag))
        }

        // Issue #128 measurement prototype: `m <ns-len> <n>
        // <key-len-1>...<key-len-n> [tag]` — every field up to the
        // optional tag is a plain length, so `split_tag` (which parses
        // every field as one) handles the whole header in one call,
        // unlike `i`/`k`'s hand-peeled non-length fields.
        "m" => {
            let (lengths, tag) = split_tag(&fields)?;
            let [namespace_length, count, key_lengths @ ..] = lengths.as_slice() else {
                return Err(invalid("bad multi-get header"));
            };
            if *count == 0 || key_lengths.len() != *count {
                return Err(invalid("bad multi-get header"));
            }
            if key_lengths.contains(&0) {
                return Err(invalid("empty key in multi-get"));
            }
            let body_length = key_lengths
                .iter()
                .try_fold(*namespace_length, |sum, &length| sum.checked_add(length))
                .ok_or_else(|| invalid("frame length overflow"))?;
            let body = body!(body_length);
            let namespace = body.slice(..*namespace_length);
            let mut cursor = *namespace_length;
            let mut keys = Vec::with_capacity(key_lengths.len());
            for &length in key_lengths {
                keys.push(body.slice(cursor..cursor + length));
                cursor += length;
            }
            Ok(ParseOutcome::Ready(
                Request::MultiGet { namespace, keys },
                tag,
            ))
        }

        // Issue #150: `o <ns-len> <n> <key-len-1> <value-len-1>...
        // <key-len-n> <value-len-n> [ttl] [tag]` — `split_tag` still
        // handles the whole header in one call (ttl is a plain length
        // too), but unlike `m`'s flat list, the field count after
        // `<n>` is ambiguous between "no ttl" (`2n` fields) and "ttl"
        // (`2n + 1`) until `n` itself resolves it.
        "o" => {
            let (lengths, tag) = split_tag(&fields)?;
            let [namespace_length, count, rest @ ..] = lengths.as_slice() else {
                return Err(invalid("bad multi-set header"));
            };
            let count = *count;
            if count == 0 {
                return Err(invalid("bad multi-set header"));
            }
            let pairs_len = count
                .checked_mul(2)
                .ok_or_else(|| invalid("bad multi-set header"))?;
            let (pairs, ttl) = match rest.len().checked_sub(pairs_len) {
                Some(0) => (rest, None),
                Some(1) => (&rest[..pairs_len], Some(rest[pairs_len] as u64)),
                _ => return Err(invalid("bad multi-set header")),
            };

            let mut lengths_pairs = Vec::with_capacity(count);
            for pair in pairs.chunks_exact(2) {
                let (key_length, value_length) = (pair[0], pair[1]);
                if key_length == 0 {
                    return Err(invalid("empty key in multi-set"));
                }
                lengths_pairs.push((key_length, value_length));
            }

            let body_length = lengths_pairs
                .iter()
                .try_fold(*namespace_length, |sum, &(key_length, value_length)| {
                    sum.checked_add(key_length)
                        .and_then(|sum| sum.checked_add(value_length))
                })
                .ok_or_else(|| invalid("frame length overflow"))?;
            let body = body!(body_length);
            let namespace = body.slice(..*namespace_length);
            let mut cursor = *namespace_length;
            let mut keys = Vec::with_capacity(count);
            let mut values = Vec::with_capacity(count);
            for (key_length, value_length) in lengths_pairs {
                keys.push(body.slice(cursor..cursor + key_length));
                cursor += key_length;
                values.push(body.slice(cursor..cursor + value_length));
                cursor += value_length;
            }

            Ok(ParseOutcome::Ready(
                Request::MultiSet {
                    namespace,
                    keys,
                    values,
                    ttl,
                },
                tag,
            ))
        }

        "S" | "s" => {
            let (lengths, tag) = split_tag(&fields)?;
            let namespaced = command == "s";
            let (namespace_length, key_length, value_length, ttl) =
                match (namespaced, lengths.as_slice()) {
                    (false, [key_length, value_length]) => (0, *key_length, *value_length, None),
                    (false, [key_length, value_length, ttl]) => {
                        (0, *key_length, *value_length, Some(*ttl as u64))
                    }
                    (true, [namespace_length, key_length, value_length]) => {
                        (*namespace_length, *key_length, *value_length, None)
                    }
                    (true, [namespace_length, key_length, value_length, ttl]) => (
                        *namespace_length,
                        *key_length,
                        *value_length,
                        Some(*ttl as u64),
                    ),
                    _ => return Err(invalid("bad set header")),
                };
            if key_length == 0 {
                return Err(invalid("empty key"));
            }
            let body_length = namespace_length
                .checked_add(key_length)
                .and_then(|length| length.checked_add(value_length))
                .ok_or_else(|| invalid("frame length overflow"))?;
            let body = body!(body_length);
            let namespace = body.slice(..namespace_length);
            let key = body.slice(namespace_length..namespace_length + key_length);
            let value = body.slice(namespace_length + key_length..);
            Ok(ParseOutcome::Ready(
                Request::Set {
                    namespace,
                    key,
                    value,
                    ttl,
                },
                tag,
            ))
        }

        // Issue #129: `i <ns-len> <key-len> <delta> [tag]` — `<delta>` is
        // signed decimal, so it can't go through `split_tag`'s
        // unsigned-only length parser; peeled off by hand first, mirroring
        // `split_tag`'s own tag-then-lengths logic for the two fields that
        // remain.
        "i" => {
            let (rest, tag) = if tagged {
                let (tag_field, rest) =
                    fields.split_last().ok_or_else(|| invalid("missing tag"))?;
                let tag = u32::try_from(parse_length_field(tag_field)?)
                    .map_err(|_| invalid("tag out of range"))?;
                (rest, Some(tag))
            } else {
                (fields.as_slice(), None)
            };
            let [namespace_length, key_length, delta] = rest else {
                return Err(invalid("bad incr header"));
            };
            let namespace_length = parse_length_field(namespace_length)?;
            let key_length = parse_length_field(key_length)?;
            if key_length == 0 {
                return Err(invalid("empty key"));
            }
            let delta = parse_delta_field(delta)?;
            let body = body!(
                namespace_length
                    .checked_add(key_length)
                    .ok_or_else(|| invalid("frame length overflow"))?
            );
            let namespace = body.slice(..namespace_length);
            let key = body.slice(namespace_length..);
            Ok(ParseOutcome::Ready(
                Request::Incr {
                    namespace,
                    key,
                    delta,
                },
                tag,
            ))
        }

        // Issue #141: `k <ns-len> <key-len> <val-len> <cond> [ttl] [tag]`
        // — `<cond>` is a bare token, so it's peeled off by hand like
        // `i`'s `<delta>`, with `s`'s own optional trailing `[ttl] [tag]`
        // layered on top of the fields that remain.
        "k" => {
            let (rest, tag) = if tagged {
                let (tag_field, rest) =
                    fields.split_last().ok_or_else(|| invalid("missing tag"))?;
                let tag = u32::try_from(parse_length_field(tag_field)?)
                    .map_err(|_| invalid("tag out of range"))?;
                (rest, Some(tag))
            } else {
                (fields.as_slice(), None)
            };
            let (namespace_length, key_length, value_length, cond, ttl) = match rest {
                [namespace_length, key_length, value_length, cond] => {
                    (namespace_length, key_length, value_length, cond, None)
                }
                [namespace_length, key_length, value_length, cond, ttl] => {
                    (namespace_length, key_length, value_length, cond, Some(*ttl))
                }
                _ => return Err(invalid("bad cas-set header")),
            };
            let namespace_length = parse_length_field(namespace_length)?;
            let key_length = parse_length_field(key_length)?;
            let value_length = parse_length_field(value_length)?;
            if key_length == 0 {
                return Err(invalid("empty key"));
            }
            let condition = parse_cas_condition_field(cond, true)?;
            let ttl = ttl
                .map(parse_length_field)
                .transpose()?
                .map(|ttl| ttl as u64);

            let body_length = namespace_length
                .checked_add(key_length)
                .and_then(|length| length.checked_add(value_length))
                .ok_or_else(|| invalid("frame length overflow"))?;
            let body = body!(body_length);
            let namespace = body.slice(..namespace_length);
            let key = body.slice(namespace_length..namespace_length + key_length);
            let value = body.slice(namespace_length + key_length..);
            Ok(ParseOutcome::Ready(
                Request::CasSet {
                    namespace,
                    key,
                    condition,
                    value,
                    ttl,
                },
                tag,
            ))
        }

        // Issue #141: `x <ns-len> <key-len> <cond> [tag]` — `<cond>` must
        // be a digest (`allow_absent_present: false` — see
        // `Request::CasDelete`'s doc comment).
        "x" => {
            let (rest, tag) = if tagged {
                let (tag_field, rest) =
                    fields.split_last().ok_or_else(|| invalid("missing tag"))?;
                let tag = u32::try_from(parse_length_field(tag_field)?)
                    .map_err(|_| invalid("tag out of range"))?;
                (rest, Some(tag))
            } else {
                (fields.as_slice(), None)
            };
            let [namespace_length, key_length, cond] = rest else {
                return Err(invalid("bad cas-delete header"));
            };
            let namespace_length = parse_length_field(namespace_length)?;
            let key_length = parse_length_field(key_length)?;
            if key_length == 0 {
                return Err(invalid("empty key"));
            }
            let expected_digest = match parse_cas_condition_field(cond, false)? {
                CasCondition::Digest(digest) => digest,
                CasCondition::Absent | CasCondition::Present => {
                    unreachable!("parse_cas_condition_field(cond, false) never returns those")
                }
            };
            let body = body!(
                namespace_length
                    .checked_add(key_length)
                    .ok_or_else(|| invalid("frame length overflow"))?
            );
            let namespace = body.slice(..namespace_length);
            let key = body.slice(namespace_length..);
            Ok(ParseOutcome::Ready(
                Request::CasDelete {
                    namespace,
                    key,
                    expected_digest,
                },
                tag,
            ))
        }

        "c" => {
            let (lengths, tag) = split_tag(&fields)?;
            let [namespace_length] = lengths.as_slice() else {
                return Err(invalid("bad clear header"));
            };
            let body = body!(*namespace_length);
            Ok(ParseOutcome::Ready(Request::Clear { namespace: body }, tag))
        }

        "F" => {
            let (lengths, tag) = split_tag(&fields)?;
            if !lengths.is_empty() {
                return Err(invalid("bad flush header"));
            }
            let _ = body!(0);
            Ok(ParseOutcome::Ready(Request::ClearAll, tag))
        }

        _ => Err(invalid("unknown or cluster-internal command")),
    }
}

// ─── backend connections ─────────────────────────────────────────────

/// What a driver expects back for one forwarded request, so the backend
/// reader knows how to frame the reply.
#[derive(Clone, Copy, PartialEq, Debug)]
enum Expect {
    /// `V <len> <tag>` + body, or `N`/`W`/`E`.
    Value,
    /// `S`, or `W`/`E`.
    Stored,
    /// `D`/`N`, or `W`/`E`.
    Deleted,
    /// `C`, or `E`.
    Cleared,
    /// Issue #129: `I <len> [ttl] <tag>` + body, or `N`/`T`/`W`/`E`.
    Incremented,
    /// Issue #141: `k`'s reply — `S` (condition held) or `N` (mismatch),
    /// or `W`/`E`. `x`'s reply reuses `Expect::Deleted` unchanged (`D`/`N`
    /// is already exactly that shape).
    CasSet,
    /// Issues #128/#150: `M <n> <r-1>...<r-n> <tag>` + concatenated hit
    /// values, or `E`. Parsed specially in `read_reply` (a variable
    /// roster, unlike every other reply's fixed field count), never
    /// reaches the generic `(marker, fields)` match there.
    Multi,
    /// Issue #150: `O <n> <r-1>...<r-n> <tag>`, no body, or `E`. Parsed
    /// specially in `read_reply` alongside `Expect::Multi`, same reason.
    MultiAck,
}

/// One key's outcome inside a backend's `M` reply (issues #128/#150) —
/// independent reimplementation of the node's `crate::response::MultiEntry`
/// (this binary shares no modules with the node, see `CasCondition`'s
/// doc comment for the established policy).
#[derive(Debug, Clone, PartialEq)]
enum ProxyMultiEntry {
    Value(Bytes),
    Miss,
    WrongNode,
}

/// One key's outcome inside a backend's `O` reply (issue #150) —
/// independent reimplementation of the node's
/// `crate::response::MultiAckEntry`.
#[derive(Debug, Clone, Copy, PartialEq)]
enum ProxyAckEntry {
    Stored,
    WrongNode,
}

/// One reply from a node, already tag-verified.
#[derive(Debug, PartialEq)]
enum NodeReply {
    Value(Bytes),
    /// Issue #129: `INCR`'s new value plus its entry's remaining TTL in
    /// whole seconds, if it had one — carried through so a successful
    /// primary INCR's *result* can be fanned out to replicas as a plain
    /// `Set` without silently dropping the TTL (see `finish_incr`).
    Incremented(Bytes, Option<u64>),
    /// Issue #129: the key exists but its value isn't `INCR`'s counter
    /// grammar, or `delta` would overflow — the wire's `T` status.
    NotNumeric,
    NotFound,
    Stored,
    Deleted,
    Cleared,
    WrongNode,
    /// Issues #128/#150: `M`'s per-key roster, already decoded — see
    /// `Expect::Multi`.
    Multi(Vec<ProxyMultiEntry>),
    /// Issue #150: `O`'s per-key roster, already decoded — see
    /// `Expect::MultiAck`.
    MultiAck(Vec<ProxyAckEntry>),
    /// `E`, or a reply that doesn't fit `Expect` — the connection is
    /// dropped by the reader either way.
    Error,
}

struct BackendRequest {
    /// Issue #177: `Bytes`, not `Vec<u8>` — a write/clear fanned out to
    /// several owners shares this one buffer across every replica leg's
    /// `BackendRequest` (a cheap refcount bump), instead of each leg
    /// carrying its own deep copy of the payload.
    frame: Bytes,
    expect: Expect,
    reply: oneshot::Sender<io::Result<NodeReply>>,
}

/// Hands out a unique id per dialed backend connection so `run_backend`'s
/// teardown, and `enqueue`'s poisoned-handle recheck, can tell "the
/// connection I am holding" from "a replacement another task redialed into
/// this slot" — without keeping a `Sender` clone alive for the comparison
/// (that would pin the queue open and defeat the drop-ends-the-task
/// invariant `BackendHandle` documents; see `run_backend`).
static NEXT_BACKEND_ID: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

/// A live, authenticated, tagged connection to one node, owned by a
/// task: requests are written in arrival order and replies matched
/// FIFO by tag. Dropping every sender ends the task and the connection —
/// so nothing outside the slot map (and any in-flight `enqueue`) may hold
/// one, or a pruned backend's task and socket would leak.
#[derive(Clone)]
struct BackendHandle {
    id: u64,
    sender: mpsc::Sender<BackendRequest>,
}

impl BackendHandle {
    async fn connect(
        addr: &str,
        secret: &Option<Bytes>,
        tls_connector: &Option<TlsConnector>,
        slot: Arc<tokio::sync::Mutex<Option<BackendHandle>>>,
        dialed: Arc<std::sync::atomic::AtomicUsize>,
    ) -> io::Result<Self> {
        let mut stream = timeout(UPSTREAM_IO_TIMEOUT, connect_upstream(addr, tls_connector))
            .await
            .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "backend connect timed out"))??;

        // Authenticate and negotiate tagged mode. With no secret
        // configured a node accepts any non-empty secret (the probe
        // convention the SDKs use), so `T` can always be negotiated.
        let probe = Bytes::from_static(b"proxy");
        let secret_bytes = secret.as_ref().unwrap_or(&probe);
        let mut auth = format!("A {} T\n", secret_bytes.len()).into_bytes();
        auth.extend_from_slice(secret_bytes);

        let mut buf = BytesMut::new();
        timeout(UPSTREAM_IO_TIMEOUT, async {
            stream.write_all(&auth).await?;
            let ack = read_line(&mut stream, &mut buf).await?;
            if ack != "OnT" {
                return Err(io::Error::new(
                    io::ErrorKind::PermissionDenied,
                    format!("backend at {addr} rejected the tagged handshake: {ack:?}"),
                ));
            }
            Ok(())
        })
        .await
        .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "backend handshake timed out"))??;

        let (sender, receiver) = mpsc::channel(BACKEND_QUEUE_DEPTH);
        let id = NEXT_BACKEND_ID.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        // `run_backend` gets a *weak* handle to the slot: it must be able to
        // clear the slot at teardown, but holding a strong `Arc` would keep
        // the `Option<BackendHandle>` — and so this connection's own
        // `Sender` — alive even after `prune` drops the map's entry, which
        // would stop `receiver.recv()` from ever returning `None` and leak
        // the task and socket for a retired address (pass-7 audit).
        tokio::spawn(run_backend(
            stream,
            buf,
            receiver,
            addr.to_string(),
            Arc::downgrade(&slot),
            dialed,
            id,
        ));
        Ok(Self { id, sender })
    }
}

/// The shared backend connection's writer half (issue #110): assigns
/// each queued request the next sequence tag, reserves its slot in the
/// reader's pending FIFO (blocking — the in-flight cap,
/// `MAX_BACKEND_IN_FLIGHT`), then writes the frame. Requests from every
/// client connection interleave here in strict queue order; replies are
/// matched by the reader half. Unlike #109's serial
/// write-then-await-reply loop, writing never waits for replies — the
/// connection is genuinely pipelined, so N clients sharing it cost
/// queueing, not N round-trips.
///
/// Poisoning: any reader-side failure (I/O error, tag mismatch,
/// malformed or ill-fitting reply, per-reply timeout) kills the reader,
/// which fails this writer's next pending-slot reservation; the writer
/// exits, dropping the request queue, and every queued/in-flight
/// request's oneshot resolves as an error. The tag verification is what
/// bounds the blast radius to exactly the requests on this connection
/// (see the module docs); the next `enqueue` redials.
async fn run_backend(
    stream: UpstreamStream,
    buf: BytesMut,
    mut receiver: mpsc::Receiver<BackendRequest>,
    addr: String,
    slot: std::sync::Weak<tokio::sync::Mutex<Option<BackendHandle>>>,
    dialed: Arc<std::sync::atomic::AtomicUsize>,
    own_id: u64,
) {
    let (mut read_half, mut write_half) = tokio::io::split(stream);
    let (pending_tx, mut pending_rx) =
        mpsc::channel::<(u32, Expect, oneshot::Sender<io::Result<NodeReply>>)>(
            MAX_BACKEND_IN_FLIGHT,
        );

    // issue #192: lets the reader wake the writer the instant it
    // poisons, even with no further request queued to trip the
    // `pending_tx.send` failure below — otherwise, with no more traffic
    // to this node, the writer (and so this whole task, and the
    // `dialed`-gauge decrement at the bottom) could sit parked on
    // `receiver.recv()` forever after a poison, well past the point the
    // connection is actually dead. `notify_one` before any waiter
    // subscribes still delivers: the permit is stored and consumed by
    // the writer's first `notified().await`.
    let poisoned = Arc::new(tokio::sync::Notify::new());
    let reader_poisoned = Arc::clone(&poisoned);

    // The reader half: resolves pending replies in FIFO order. The
    // per-reply timeout is a *progress* bound (each reply must arrive
    // within it once the reader starts waiting), not an end-to-end
    // per-request deadline — under pipelining a deep queue's total wait
    // is the sum of its predecessors', which is exactly the
    // backpressure `MAX_BACKEND_IN_FLIGHT` exists to bound.
    let reader = tokio::spawn(async move {
        let mut buf = buf;
        while let Some((tag, expect, reply)) = pending_rx.recv().await {
            let result = timeout(
                UPSTREAM_IO_TIMEOUT,
                read_reply(&mut read_half, &mut buf, tag, expect),
            )
            .await
            .unwrap_or_else(|_| {
                Err(io::Error::new(
                    io::ErrorKind::TimedOut,
                    "backend reply timed out",
                ))
            });

            let poisoned = result.is_err();
            let _ = reply.send(result);
            if poisoned {
                reader_poisoned.notify_one();
                return;
            }
        }
    });

    let mut next_tag: u32 = 0;
    // Distinguishes the two exit reasons for the log below: a broken/
    // timed-out stream (poison) versus every sender being dropped —
    // which now includes `SharedBackends::prune` retiring this address,
    // the case that previously leaked because `run_backend` used to hold
    // its own `Sender` clone and so `receiver.recv()` never returned
    // `None`. Holding no sender here is what lets a pruned, idle backend
    // wind down promptly instead.
    let mut poisoned_exit = false;

    loop {
        let request = tokio::select! {
            request = receiver.recv() => match request {
                Some(request) => request,
                None => break,
            },
            () = poisoned.notified() => {
                poisoned_exit = true;
                break;
            }
        };

        let tag = next_tag;
        next_tag = next_tag.wrapping_add(1);

        let (header, body) = substitute_tag(request.frame, tag);

        // Reserve the reply slot before writing: if the reader is gone
        // (poisoned), this fails and the request errors without touching
        // a desynced stream.
        if pending_tx
            .send((tag, request.expect, request.reply))
            .await
            .is_err()
        {
            poisoned_exit = true;
            break;
        }

        // Issue #335: two writes instead of one, so `body` — up to
        // `MAX_VALUE_SIZE` and unmodified by tag substitution — can go
        // straight from `substitute_tag`'s zero-copy slice to the socket
        // instead of through a freshly copied combined buffer first (see
        // `substitute_tag`'s doc comment). Both target the same
        // exclusively-owned stream in the same order the single write
        // used to, so this is not observable on the wire.
        //
        // Issue #420: bounded by `UPSTREAM_IO_TIMEOUT`, like every other
        // I/O site against this connection. A node that stops draining
        // its receive buffer while keeping the connection open would
        // otherwise park this write forever — the reader's own per-reply
        // timeout can't help here: `poisoned.notified()` above is only
        // checked between requests, not while a write is in flight, so
        // nothing would ever bring this task back around to notice it.
        // A write timeout is treated exactly like a write error: poison
        // and stop, so the slot clears and the next `enqueue` redials.
        let write_result = timeout(UPSTREAM_IO_TIMEOUT, async {
            write_half.write_all(&header).await?;
            write_half.write_all(&body).await
        })
        .await;
        if !matches!(write_result, Ok(Ok(()))) {
            // The reader will observe the broken stream (or time out)
            // and poison; nothing more to write here.
            poisoned_exit = true;
            break;
        }
    }

    // Queue closed (handle dropped or poisoned): let the reader drain
    // what is still pending, then stop.
    drop(pending_tx);
    let _ = reader.await;

    // issue #192: name which backend went away — previously this task
    // exited silently and the operator only learned from a downstream
    // "backend connection is gone" error with no address attached. A
    // clean drain (every sender dropped, e.g. prune retired the address)
    // is expected teardown, not a fault, so it is logged differently.
    if poisoned_exit {
        eprintln!("WARN backend connection to {addr} poisoned; will redial on next request");
    } else {
        eprintln!("INFO backend connection to {addr} closed; no callers remain");
    }

    // issue #192: eagerly clear this connection's slot and decrement the
    // `dialed` gauge right when the task actually exits, rather than
    // leaving it counted as live until some later caller's send against
    // it fails and `SharedBackends::enqueue` lazily notices.
    //
    // Exactly one `-1` must pair with this connection's dial `+1`, split
    // across three teardown shapes (the slot lock serializes them against
    // `enqueue`'s own recheck, so each sees a consistent slot):
    //   * slot still ours (`id == own_id`): we clear it and decrement.
    //   * slot holds a different id, or is already `None`: `enqueue`'s
    //     failed-send recheck already found this connection dead, cleared
    //     it and decremented (possibly redialing a replacement) — we must
    //     not decrement again.
    //   * slot gone entirely (`prune` dropped the map's only strong ref,
    //     which is what closed our queue): nobody else will account for
    //     this dial, so we decrement here.
    match slot.upgrade() {
        Some(slot) => {
            let mut guard = slot.lock().await;
            if guard.as_ref().is_some_and(|current| current.id == own_id) {
                *guard = None;
                dialed.fetch_sub(1, std::sync::atomic::Ordering::Relaxed);
            }
        }
        None => {
            dialed.fetch_sub(1, std::sync::atomic::Ordering::Relaxed);
        }
    }
}

/// The `{tag}` placeholder the framers leave in the header, replaced
/// with the connection's own sequence tag at send time (the tag must be
/// chosen by the connection task — requests are queued before their
/// send order, and with it their tag, is known).
const TAG_PLACEHOLDER: &str = "{tag}";

/// Splits `frame` into its header — with `{tag}` substituted for `tag` —
/// and its body, for the caller to write as two separate pieces instead
/// of one concatenated buffer.
///
/// Issue #335: this used to `extend_from_slice` the body onto the
/// rebuilt header and return the result as a single `Bytes`, which
/// recopies the *entire* body (up to `MAX_VALUE_SIZE`, 1MiB) on every
/// request just to attach a handful of substituted header bytes ahead
/// of it — on top of the copy that already assembled `frame` itself.
/// The body never changes here, so it is returned as a slice of `frame`
/// instead: `Bytes::slice` is a cheap refcount bump over the same
/// backing allocation, not a copy, so it costs the same whether the
/// body is empty or at the size limit.
fn substitute_tag(frame: Bytes, tag: u32) -> (Bytes, Bytes) {
    let header_end = frame
        .iter()
        .position(|byte| *byte == b'\n')
        .expect("framers always emit a complete header");
    let header =
        String::from_utf8(frame[..header_end].to_vec()).expect("framers always emit ASCII headers");
    let header = header.replace(TAG_PLACEHOLDER, &tag.to_string());
    let mut header = header.into_bytes();
    header.push(b'\n');
    let body = frame.slice(header_end + 1..);
    (header.into(), body)
}

async fn read_reply<S: AsyncRead + Unpin>(
    stream: &mut S,
    buf: &mut BytesMut,
    tag: u32,
    expect: Expect,
) -> io::Result<NodeReply> {
    let line = read_line(stream, buf).await?;
    let mut parts = line.split(' ');
    let marker = parts.next().unwrap_or_default();

    // `B` is unsolicited and untagged; everything else must echo the tag
    // as its last field.
    if marker == "B" {
        return Err(io::Error::other("backend answered busy"));
    }

    let fields: Vec<&str> = parts.collect();

    // Issue #128 measurement prototype: `M`'s roster is variable-length
    // (`n` per-key tokens, not `V`/`I`'s fixed field count), so it's
    // decoded here rather than joining the generic `(marker, fields)`
    // match below.
    if marker == "M" {
        let (tag_field, rest) = fields
            .split_last()
            .ok_or_else(|| invalid("malformed M reply"))?;
        if *tag_field != tag.to_string() {
            return Err(invalid(&format!(
                "backend reply tag mismatch: expected {tag}, got {tag_field}"
            )));
        }

        let (count_field, roster) = rest
            .split_first()
            .ok_or_else(|| invalid("malformed M reply"))?;
        let count = parse_length_field(count_field)?;
        if roster.len() != count {
            return Err(invalid("M roster length does not match its own count"));
        }

        enum RosterToken {
            Value(usize),
            Miss,
            WrongNode,
        }

        let mut total_value_bytes: usize = 0;
        let tokens = roster
            .iter()
            .map(|token| match *token {
                "-" => Ok(RosterToken::Miss),
                "W" => Ok(RosterToken::WrongNode),
                length => {
                    let length = parse_length_field(length)?;
                    total_value_bytes = total_value_bytes
                        .checked_add(length)
                        .ok_or_else(|| invalid("M roster value length overflow"))?;
                    Ok(RosterToken::Value(length))
                }
            })
            .collect::<io::Result<Vec<_>>>()?;

        if total_value_bytes > MAX_REQUEST_SIZE {
            return Err(invalid("M reply values exceed the request-size limit"));
        }

        read_exact_into(stream, buf, total_value_bytes).await?;
        let mut body = buf.split_to(total_value_bytes).freeze();

        let entries = tokens
            .into_iter()
            .map(|token| match token {
                RosterToken::Value(length) => ProxyMultiEntry::Value(body.split_to(length)),
                RosterToken::Miss => ProxyMultiEntry::Miss,
                RosterToken::WrongNode => ProxyMultiEntry::WrongNode,
            })
            .collect();

        let reply = NodeReply::Multi(entries);
        if !matches!(expect, Expect::Multi) {
            return Err(invalid("backend reply does not fit the request"));
        }
        return Ok(reply);
    }

    // Issue #150: `O`'s roster is variable-length like `M`'s, but has no
    // body — a set has nothing to echo back.
    if marker == "O" {
        let (tag_field, rest) = fields
            .split_last()
            .ok_or_else(|| invalid("malformed O reply"))?;
        if *tag_field != tag.to_string() {
            return Err(invalid(&format!(
                "backend reply tag mismatch: expected {tag}, got {tag_field}"
            )));
        }

        let (count_field, roster) = rest
            .split_first()
            .ok_or_else(|| invalid("malformed O reply"))?;
        let count = parse_length_field(count_field)?;
        if roster.len() != count {
            return Err(invalid("O roster length does not match its own count"));
        }

        let entries = roster
            .iter()
            .map(|token| match *token {
                "S" => Ok(ProxyAckEntry::Stored),
                "W" => Ok(ProxyAckEntry::WrongNode),
                _ => Err(invalid("malformed O roster token")),
            })
            .collect::<io::Result<Vec<_>>>()?;

        let reply = NodeReply::MultiAck(entries);
        if !matches!(expect, Expect::MultiAck) {
            return Err(invalid("backend reply does not fit the request"));
        }
        return Ok(reply);
    }

    // Issue #129: `I`'s header carries an extra optional `<ttl>` field
    // ahead of the tag — same "present-but-unlabeled trailing field"
    // shape `S`'s own optional TTL has, but backend connections are
    // *always* tagged (`BackendHandle::connect` negotiates `T`), so there
    // is no untagged case to disambiguate against here: `[length,
    // tag_field]` is untimed, `[length, ttl_field, tag_field]` carries a
    // TTL.
    let (echoed, value_length, ttl) = match (marker, fields.as_slice()) {
        ("V", [length, tag_field]) => (*tag_field, Some(parse_length_field(length)?), None),
        ("I", [length, tag_field]) => (*tag_field, Some(parse_length_field(length)?), None),
        ("I", [length, ttl_field, tag_field]) => (
            *tag_field,
            Some(parse_length_field(length)?),
            Some(parse_length_field(ttl_field)? as u64),
        ),
        ("S" | "D" | "N" | "T" | "W" | "C" | "E", [tag_field]) => (*tag_field, None, None),
        _ => return Err(invalid(&format!("malformed backend reply: {line:?}"))),
    };
    if echoed != tag.to_string() {
        return Err(invalid(&format!(
            "backend reply tag mismatch: expected {tag}, got {echoed}"
        )));
    }

    let reply = match (marker, value_length) {
        ("V", Some(length)) => {
            if length > MAX_REQUEST_SIZE {
                return Err(invalid("backend value exceeds the request-size limit"));
            }
            read_exact_into(stream, buf, length).await?;
            NodeReply::Value(buf.split_to(length).freeze())
        }
        ("I", Some(length)) => {
            if length > MAX_REQUEST_SIZE {
                return Err(invalid("backend value exceeds the request-size limit"));
            }
            read_exact_into(stream, buf, length).await?;
            NodeReply::Incremented(buf.split_to(length).freeze(), ttl)
        }
        ("N", _) => NodeReply::NotFound,
        ("T", _) => NodeReply::NotNumeric,
        ("S", _) => NodeReply::Stored,
        ("D", _) => NodeReply::Deleted,
        ("C", _) => NodeReply::Cleared,
        ("W", _) => NodeReply::WrongNode,
        ("E", _) => NodeReply::Error,
        _ => unreachable!("matched above"),
    };

    // A reply shape `expect` rules out means a desynced stream, not a
    // negotiable answer.
    let shape_ok = matches!(
        (&reply, expect),
        (
            NodeReply::Value(_) | NodeReply::NotFound | NodeReply::WrongNode | NodeReply::Error,
            Expect::Value
        ) | (
            NodeReply::Stored | NodeReply::WrongNode | NodeReply::Error,
            Expect::Stored
        ) | (
            NodeReply::Deleted | NodeReply::NotFound | NodeReply::WrongNode | NodeReply::Error,
            Expect::Deleted
        ) | (NodeReply::Cleared | NodeReply::Error, Expect::Cleared)
            | (
                NodeReply::Incremented(..)
                    | NodeReply::NotFound
                    | NodeReply::NotNumeric
                    | NodeReply::WrongNode
                    | NodeReply::Error,
                Expect::Incremented
            )
            | (
                NodeReply::Stored | NodeReply::NotFound | NodeReply::WrongNode | NodeReply::Error,
                Expect::CasSet
            )
            | (NodeReply::Error, Expect::Multi)
            | (NodeReply::Error, Expect::MultiAck)
    );
    if !shape_ok {
        return Err(invalid("backend reply does not fit the request"));
    }

    Ok(reply)
}

// ─── backend frame builders (proxy → node, always tagged) ────────────

fn frame_get(namespace: &[u8], key: &[u8]) -> Bytes {
    let mut frame = if namespace.is_empty() {
        format!("G {} {TAG_PLACEHOLDER}\n", key.len()).into_bytes()
    } else {
        format!("g {} {} {TAG_PLACEHOLDER}\n", namespace.len(), key.len()).into_bytes()
    };
    frame.extend_from_slice(namespace);
    frame.extend_from_slice(key);
    frame.into()
}

/// Issue #128 measurement prototype: one sub-frame per owner, carrying
/// only that owner's slice of the original request's keys — see
/// `dispatch_request`'s `Request::MultiGet` arm.
fn frame_multi_get(namespace: &[u8], keys: &[Bytes]) -> Bytes {
    let key_lengths: String = keys.iter().map(|key| format!(" {}", key.len())).collect();
    let mut frame = format!(
        "m {} {}{key_lengths} {TAG_PLACEHOLDER}\n",
        namespace.len(),
        keys.len()
    )
    .into_bytes();
    frame.extend_from_slice(namespace);
    for key in keys {
        frame.extend_from_slice(key);
    }
    frame.into()
}

/// Issue #150: one sub-frame per involved owner *address* — see
/// `dispatch_request`'s `Request::MultiSet` arm for why grouping is by
/// address rather than by rank (a batch's keys can put the same node in
/// different roles). One shared `ttl` for the whole frame.
fn frame_multi_set(namespace: &[u8], keys: &[Bytes], values: &[Bytes], ttl: Option<u64>) -> Bytes {
    let mut lengths = String::new();
    for (key, value) in keys.iter().zip(values) {
        lengths.push_str(&format!(" {} {}", key.len(), value.len()));
    }
    let ttl_field = ttl.map(|ttl| format!(" {ttl}")).unwrap_or_default();
    let mut frame = format!(
        "o {} {}{lengths}{ttl_field} {TAG_PLACEHOLDER}\n",
        namespace.len(),
        keys.len()
    )
    .into_bytes();
    frame.extend_from_slice(namespace);
    for (key, value) in keys.iter().zip(values) {
        frame.extend_from_slice(key);
        frame.extend_from_slice(value);
    }
    frame.into()
}

fn frame_set(namespace: &[u8], key: &[u8], value: &[u8], ttl: Option<u64>) -> Bytes {
    let ttl_field = ttl.map(|ttl| format!(" {ttl}")).unwrap_or_default();
    let mut frame = if namespace.is_empty() {
        format!(
            "S {} {}{ttl_field} {TAG_PLACEHOLDER}\n",
            key.len(),
            value.len()
        )
        .into_bytes()
    } else {
        format!(
            "s {} {} {}{ttl_field} {TAG_PLACEHOLDER}\n",
            namespace.len(),
            key.len(),
            value.len()
        )
        .into_bytes()
    };
    frame.extend_from_slice(namespace);
    frame.extend_from_slice(key);
    frame.extend_from_slice(value);
    frame.into()
}

fn frame_delete(namespace: &[u8], key: &[u8]) -> Bytes {
    let mut frame = if namespace.is_empty() {
        format!("D {} {TAG_PLACEHOLDER}\n", key.len()).into_bytes()
    } else {
        format!("d {} {} {TAG_PLACEHOLDER}\n", namespace.len(), key.len()).into_bytes()
    };
    frame.extend_from_slice(namespace);
    frame.extend_from_slice(key);
    frame.into()
}

/// Issue #129: always the lowercase, namespaced `i` — `INCR` has no
/// uppercase legacy form (see `Request::Incr`'s doc comment).
fn frame_incr(namespace: &[u8], key: &[u8], delta: i64) -> Bytes {
    let mut frame = format!(
        "i {} {} {delta} {TAG_PLACEHOLDER}\n",
        namespace.len(),
        key.len()
    )
    .into_bytes();
    frame.extend_from_slice(namespace);
    frame.extend_from_slice(key);
    frame.into()
}

/// Issue #141: `<cond>`'s wire form for the outgoing `k`/`x` frame — the
/// exact inverse of `parse_cas_condition_field`.
fn cas_condition_field(condition: CasCondition) -> String {
    match condition {
        CasCondition::Absent => "A".to_string(),
        CasCondition::Present => "P".to_string(),
        CasCondition::Digest(digest) => digest.iter().map(|byte| format!("{byte:02x}")).collect(),
    }
}

/// Issue #141: always the lowercase, namespaced `k` — like `INCR`, this
/// op has no uppercase legacy form.
fn frame_cas_set(
    namespace: &[u8],
    key: &[u8],
    condition: CasCondition,
    value: &[u8],
    ttl: Option<u64>,
) -> Bytes {
    let cond = cas_condition_field(condition);
    let ttl_field = ttl.map(|ttl| format!(" {ttl}")).unwrap_or_default();
    let mut frame = format!(
        "k {} {} {} {cond}{ttl_field} {TAG_PLACEHOLDER}\n",
        namespace.len(),
        key.len(),
        value.len()
    )
    .into_bytes();
    frame.extend_from_slice(namespace);
    frame.extend_from_slice(key);
    frame.extend_from_slice(value);
    frame.into()
}

/// Issue #141: always the lowercase, namespaced `x`.
fn frame_cas_delete(namespace: &[u8], key: &[u8], expected_digest: [u8; 16]) -> Bytes {
    let cond = cas_condition_field(CasCondition::Digest(expected_digest));
    let mut frame = format!(
        "x {} {} {cond} {TAG_PLACEHOLDER}\n",
        namespace.len(),
        key.len()
    )
    .into_bytes();
    frame.extend_from_slice(namespace);
    frame.extend_from_slice(key);
    frame.into()
}

fn frame_clear(namespace: &[u8]) -> Bytes {
    let mut frame = format!("c {} {TAG_PLACEHOLDER}\n", namespace.len()).into_bytes();
    frame.extend_from_slice(namespace);
    frame.into()
}

fn frame_clear_all() -> Bytes {
    format!("F {TAG_PLACEHOLDER}\n").into_bytes().into()
}

// ─── request drivers ─────────────────────────────────────────────────

/// Issue #272: marks an `io::Error` returned by `SharedBackends::enqueue`
/// as one where the frame provably never reached the backend's socket —
/// a dial failure, the dial backoff fast-fail, or the request never got
/// pulled off `run_backend`'s queue (and so never reached
/// `write_half.write_all`) before the connection poisoned. Wraps the
/// original error rather than replacing it, so logging still shows the
/// real cause; `request_not_sent` is how a caller tests for the marker.
/// Mirrors `sdk/go`'s `errRequestNotSent` (issue #225) — the same
/// "definitely not sent" vs. "possibly sent" split, applied here instead
/// of at the SDK layer.
#[derive(Debug)]
struct RequestNotSent(io::Error);

impl std::fmt::Display for RequestNotSent {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(formatter)
    }
}

impl std::error::Error for RequestNotSent {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.0)
    }
}

/// Wraps `error` with the `RequestNotSent` marker (see its doc comment).
fn not_sent(error: io::Error) -> io::Error {
    io::Error::other(RequestNotSent(error))
}

/// Whether `error` (from `SharedBackends::enqueue`) was wrapped by
/// `not_sent` — i.e. the frame provably never reached the backend's
/// socket, so a non-idempotent caller (INCR — issue #272) may safely
/// retry it. Any other error means the frame may have reached the wire
/// and must not be replayed.
fn request_not_sent(error: &io::Error) -> bool {
    error
        .get_ref()
        .is_some_and(|inner| inner.downcast_ref::<RequestNotSent>().is_some())
}

/// Issue #110: the proxy-wide backend pool — one shared, tagged,
/// pipelined connection per node, multiplexing every client
/// connection's traffic. (#109 kept one per client connection per node,
/// which merely moved the fleet's connection count one hop; sharing is
/// what collapses it to "proxy count × nodes".)
///
/// Dialing is per-address: the outer map lock is only ever held to
/// fetch/insert an address's slot, and the dial itself happens under
/// that slot's own async mutex — so one node being slow to accept
/// never blocks traffic to the others, and concurrent first-users of
/// the same node coalesce onto a single dial instead of racing.
struct SharedBackends {
    slots: std::sync::Mutex<HashMap<String, Arc<tokio::sync::Mutex<Option<BackendHandle>>>>>,
    /// Issue #124: live backend connections, for the metrics gauge —
    /// incremented on a successful dial, decremented when the
    /// connection's own task exits (issue #192: eagerly, not merely
    /// lazily on some later caller's failed send). `Arc`-wrapped so a
    /// clone can be moved into `run_backend`, which does that decrement
    /// itself.
    dialed: Arc<std::sync::atomic::AtomicUsize>,
    /// Issue #177: when an address's last dial attempt failed, and how
    /// long ago — `enqueue` fails fast against `DIAL_BACKOFF` instead of
    /// re-dialing. Cleared on the next successful dial to that address.
    dial_failures: std::sync::Mutex<HashMap<String, std::time::Instant>>,
}

impl SharedBackends {
    fn new() -> Self {
        Self {
            slots: std::sync::Mutex::new(HashMap::new()),
            dialed: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
            dial_failures: std::sync::Mutex::new(HashMap::new()),
        }
    }

    fn slot(&self, addr: &str) -> Arc<tokio::sync::Mutex<Option<BackendHandle>>> {
        let mut slots = self
            .slots
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        Arc::clone(slots.entry(addr.to_string()).or_default())
    }

    /// Issue #177: whether `addr`'s most recent dial attempt failed
    /// within the last `DIAL_BACKOFF`.
    fn dial_recently_failed(&self, addr: &str) -> bool {
        let failures = self
            .dial_failures
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        failures
            .get(addr)
            .is_some_and(|at| at.elapsed() < DIAL_BACKOFF)
    }

    fn note_dial_failure(&self, addr: &str) {
        let mut failures = self
            .dial_failures
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        failures.insert(addr.to_string(), std::time::Instant::now());
    }

    fn note_dial_success(&self, addr: &str) {
        let mut failures = self
            .dial_failures
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        failures.remove(addr);
    }

    /// Issue #220: drops `slots`/`dial_failures` entries for addresses no
    /// longer present in `ring` — otherwise a long-running proxy in an
    /// autoscaling deployment (ECS/EKS) accumulates one entry per address
    /// the cluster has ever used, growing without bound.
    ///
    /// Safe against a connection in flight on a pruned address: `slot()`
    /// callers (and `run_backend`'s own teardown) hold their own `Arc`
    /// clone of that address's slot, independent of the map entry, so
    /// dropping the entry here neither drops nor disturbs a live
    /// connection — it just stops being reachable through this map. If
    /// the address later rejoins the ring, the next `slot()` call for it
    /// creates a fresh, empty entry and redials, exactly as it would for
    /// an address seen for the first time. `dialed` accounting is
    /// unaffected: it is tracked by `run_backend`'s own teardown (see
    /// issue #192) against the specific handle it owns, not by map
    /// membership.
    fn prune(&self, ring: &RingView) {
        let live = ring.all_addresses();
        {
            let mut slots = self
                .slots
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            slots.retain(|addr, _| live.contains(addr));
        }
        {
            let mut failures = self
                .dial_failures
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            failures.retain(|addr, _| live.contains(addr));
        }
    }

    /// Enqueues one tag-checked request on `addr`'s shared connection
    /// (dialing it if needed) and returns the pending reply.
    ///
    /// This is the ordering-critical half of a request (see
    /// `handle_client`'s dispatch note): one client's requests are
    /// enqueued sequentially by its reader, so they enter this queue —
    /// and the wire — in that client's request order; different
    /// clients' requests interleave in arrival order, which is exactly
    /// a node's own accept-order semantics. A dial failure resolves the
    /// pending reply immediately with the error.
    async fn enqueue(
        &self,
        context: &ProxyContext,
        addr: &str,
        frame: Bytes,
        expect: Expect,
    ) -> PendingReply {
        // Issue #177: a fast-fail check before even touching the
        // per-address slot lock — a black-holed address that just
        // failed shouldn't make every concurrent caller queue up behind
        // the lock only to hit the same backoff once they get it.
        if self.dial_recently_failed(addr) {
            return PendingReply::failed(not_sent(dial_backoff_error(addr)));
        }

        let slot = self.slot(addr);

        // Two passes: a cached handle whose task has exited (the node's
        // idle timeout closed the connection, or an earlier request
        // poisoned it) is detected by the failed queue send, dropped —
        // only if it is still the *same* handle, another client may
        // have redialed already — and replaced by a fresh dial.
        for _ in 0..2 {
            let handle = {
                let mut guard = slot.lock().await;
                match guard.as_ref() {
                    Some(handle) => handle.clone(),
                    None => {
                        // Recheck under the lock: another task may have
                        // just recorded a failure (or a success) for
                        // this address while we were waiting for it —
                        // this is what bounds concurrent first-dialers
                        // of a dead address to one real dial, the rest
                        // fail fast on the recheck instead of each
                        // paying their own `UPSTREAM_IO_TIMEOUT`.
                        if self.dial_recently_failed(addr) {
                            return PendingReply::failed(not_sent(dial_backoff_error(addr)));
                        }
                        match BackendHandle::connect(
                            addr,
                            &context.secret,
                            &context.tls_connector,
                            Arc::clone(&slot),
                            Arc::clone(&self.dialed),
                        )
                        .await
                        {
                            Ok(handle) => {
                                *guard = Some(handle.clone());
                                self.dialed
                                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                                self.note_dial_success(addr);
                                handle
                            }
                            Err(error) => {
                                self.note_dial_failure(addr);
                                return PendingReply::failed(not_sent(error));
                            }
                        }
                    }
                }
            };

            let (reply_tx, reply_rx) = oneshot::channel();
            if handle
                .sender
                .send(BackendRequest {
                    frame: frame.clone(),
                    expect,
                    reply: reply_tx,
                })
                .await
                .is_ok()
            {
                return Box::pin(async move {
                    reply_rx.await.map_err(|_| {
                        not_sent(io::Error::other("backend connection dropped mid-request"))
                    })?
                });
            }

            let mut guard = slot.lock().await;
            if guard
                .as_ref()
                .is_some_and(|current| current.id == handle.id)
            {
                *guard = None;
                self.dialed
                    .fetch_sub(1, std::sync::atomic::Ordering::Relaxed);
            }
        }

        PendingReply::failed(not_sent(io::Error::other("backend connection is gone")))
    }

    /// `enqueue` + await, with one transparent redial when the shared
    /// handle turned out dead — for the retry/fallback paths that run
    /// *after* initial dispatch, where ordering no longer applies.
    ///
    /// Issue #272: this retries unconditionally on ANY failure of the
    /// first attempt, including one where the frame may already have
    /// reached the wire — safe only for an idempotent frame (`Set`/
    /// `Delete`, or the plain `Set` a successful `INCR`/`k` result is
    /// fanned out to replicas as — see `fan_out_write_result`). A
    /// non-idempotent frame (`INCR`) must use `call_non_idempotent`
    /// below instead.
    async fn call(
        &self,
        context: &ProxyContext,
        addr: &str,
        frame: Bytes,
        expect: Expect,
    ) -> io::Result<NodeReply> {
        let first = self
            .enqueue(context, addr, frame.clone(), expect)
            .await
            .await;
        match first {
            Ok(reply) => Ok(reply),
            Err(_) => self.enqueue(context, addr, frame, expect).await.await,
        }
    }

    /// `call`'s counterpart for a non-idempotent frame (`INCR` — issue
    /// #272, mirrors `sdk/go`'s `applyNonIdempotent` fix for #225):
    /// retries via redial exactly like `call` (covering the same
    /// idle-closed-connection case its doc comment describes), but ONLY
    /// when `enqueue`'s failure is marked `request_not_sent` — the frame
    /// provably never reached the backend's socket, so replaying it is
    /// safe. Once a frame may have reached the wire, the failure is
    /// returned to the caller as-is: the primary may already have
    /// applied the delta, so replaying here would risk double-applying
    /// it.
    async fn call_non_idempotent(
        &self,
        context: &ProxyContext,
        addr: &str,
        frame: Bytes,
        expect: Expect,
    ) -> io::Result<NodeReply> {
        let first = self
            .enqueue(context, addr, frame.clone(), expect)
            .await
            .await;
        match first {
            Ok(reply) => Ok(reply),
            Err(error) if request_not_sent(&error) => {
                self.enqueue(context, addr, frame, expect).await.await
            }
            Err(error) => Err(error),
        }
    }
}

/// Issue #177: the error `enqueue` fails fast with while `addr` is
/// within its dial backoff window — a black-holed node's second and
/// later requests in the same short window shouldn't each pay another
/// `UPSTREAM_IO_TIMEOUT` finding that out for themselves.
fn dial_backoff_error(addr: &str) -> io::Error {
    io::Error::new(
        io::ErrorKind::TimedOut,
        format!("backend at {addr} is in dial backoff after a recent failure"),
    )
}

/// A reply still in flight on a backend connection.
type PendingReply = Pin<Box<dyn std::future::Future<Output = io::Result<NodeReply>> + Send>>;

trait PendingReplyExt {
    fn failed(error: io::Error) -> PendingReply;
}

impl PendingReplyExt for PendingReply {
    fn failed(error: io::Error) -> PendingReply {
        Box::pin(async move { Err(error) })
    }
}

/// Issue #177: runs every future in `futs` concurrently on the current
/// task and returns their outputs in the same order as `futs` — a tiny
/// local `join_all`, since the crate carries no `futures` dependency.
/// Every fan-out site below (a write/clear/multi-set batch's per-owner
/// `enqueue`/`call`) used to run these one at a time in a `for` loop, so
/// one address that was slow to dial (or is being black-holed) delayed
/// issuing the request to every other owner behind it in the loop —
/// `tokio::spawn` isn't an option here since these futures borrow
/// `context`/`addr`/... with a lifetime shorter than `'static`, so this
/// polls them all in place instead of spawning them onto the runtime.
async fn join_all<'a, T>(futs: Vec<Pin<Box<dyn Future<Output = T> + Send + 'a>>>) -> Vec<T> {
    let len = futs.len();
    let mut slots: Vec<Option<Pin<Box<dyn Future<Output = T> + Send + 'a>>>> =
        futs.into_iter().map(Some).collect();
    let mut outputs: Vec<Option<T>> = (0..len).map(|_| None).collect();
    let mut remaining = len;
    std::future::poll_fn(move |cx| {
        for i in 0..len {
            if let Some(fut) = slots[i].as_mut()
                && let Poll::Ready(value) = fut.as_mut().poll(cx)
            {
                outputs[i] = Some(value);
                slots[i] = None;
                remaining -= 1;
            }
        }
        if remaining == 0 {
            Poll::Ready(
                outputs
                    .iter_mut()
                    .map(|value| value.take().unwrap())
                    .collect(),
            )
        } else {
            Poll::Pending
        }
    })
    .await
}

/// One future in a `race_ok` fan-out.
type RaceFuture<'a, T> = Pin<Box<dyn Future<Output = io::Result<T>> + Send + 'a>>;

/// Issue #409: like `join_all`, but returns the moment any future
/// resolves `Ok`, leaving the rest unpolled from then on — a race rather
/// than a join. Used for a fan-out where only the fastest success
/// matters (`fetch_roster`'s discovery replicas), as opposed to
/// `join_all`'s fan-outs where every reply is needed. If every future
/// resolves `Err` (or `futs` is empty), returns the last error seen in
/// polling order.
async fn race_ok<'a, T>(futs: Vec<RaceFuture<'a, T>>) -> io::Result<T> {
    let mut slots: Vec<Option<RaceFuture<'a, T>>> = futs.into_iter().map(Some).collect();
    let mut last_error: Option<io::Error> = None;
    let mut remaining = slots.len();
    std::future::poll_fn(move |cx| {
        for slot in slots.iter_mut() {
            if let Some(fut) = slot.as_mut()
                && let Poll::Ready(value) = fut.as_mut().poll(cx)
            {
                *slot = None;
                remaining -= 1;
                match value {
                    Ok(value) => return Poll::Ready(Ok(value)),
                    Err(error) => last_error = Some(error),
                }
            }
        }
        if remaining == 0 {
            Poll::Ready(Err(last_error
                .take()
                .unwrap_or_else(|| io::Error::other("no futures to race"))))
        } else {
            Poll::Pending
        }
    })
    .await
}

/// The current ring, or `None` before the first successful fetch.
fn current_ring(context: &ProxyContext) -> Option<Arc<RingView>> {
    context.ring.borrow().clone()
}

/// Nudges the refresher and waits for the view to change (or a short
/// deadline) — a `W` means the current view is stale, so retrying on the
/// same view would just get the same `W`.
async fn force_refresh(context: &ProxyContext) {
    let mut ring = context.ring.clone();
    ring.mark_unchanged();
    let _ = context.refresh_now.send(()).await;
    let _ = timeout(UPSTREAM_IO_TIMEOUT, ring.changed()).await;
}

/// What a driver hands back for the writer to send: the full client-form
/// response bytes.
fn respond(marker: &str, tag: Option<u32>) -> Vec<u8> {
    match tag {
        Some(tag) => format!("{marker} {tag}\n").into_bytes(),
        None => format!("{marker}\n").into_bytes(),
    }
}

fn respond_value(value: &[u8], tag: Option<u32>) -> Vec<u8> {
    let header = match tag {
        Some(tag) => format!("V {} {tag}\n", value.len()),
        None => format!("V {}\n", value.len()),
    };
    let mut framed = header.into_bytes();
    framed.extend_from_slice(value);
    framed
}

/// Issue #128 measurement prototype: `M <n> <r-1>...<r-n> [tag]\n<hit
/// values, concatenated>` — the client-facing reassembly of every group's
/// `finish_multi_get` result, mirroring the node's own `Response::Multi`
/// wire form (`src/response.rs`) byte-for-byte.
fn respond_multi(entries: &[ProxyMultiEntry], tag: Option<u32>) -> Vec<u8> {
    let mut header = format!("M {}", entries.len());
    let mut values_len = 0;

    for entry in entries {
        match entry {
            ProxyMultiEntry::Value(value) => {
                header.push(' ');
                header.push_str(&value.len().to_string());
                values_len += value.len();
            }
            ProxyMultiEntry::Miss => header.push_str(" -"),
            ProxyMultiEntry::WrongNode => header.push_str(" W"),
        }
    }
    if let Some(tag) = tag {
        header.push(' ');
        header.push_str(&tag.to_string());
    }
    header.push('\n');

    let mut framed = Vec::with_capacity(header.len() + values_len);
    framed.extend_from_slice(header.as_bytes());
    for entry in entries {
        if let ProxyMultiEntry::Value(value) = entry {
            framed.extend_from_slice(value);
        }
    }
    framed
}

/// Issue #150: `O <n> <r-1>...<r-n> [tag]\n` — the client-facing
/// reassembly of every group's `finish_multi_set` result, mirroring the
/// node's own `Response::MultiAck` wire form (`src/response.rs`)
/// byte-for-byte. No body, unlike `respond_multi`'s hit values.
fn respond_multi_ack(entries: &[ProxyAckEntry], tag: Option<u32>) -> Vec<u8> {
    let mut header = format!("O {}", entries.len());

    for entry in entries {
        match entry {
            ProxyAckEntry::Stored => header.push_str(" S"),
            ProxyAckEntry::WrongNode => header.push_str(" W"),
        }
    }
    if let Some(tag) = tag {
        header.push(' ');
        header.push_str(&tag.to_string());
    }
    header.push('\n');

    header.into_bytes()
}

/// Issue #129: `INCR`'s success reply — `I <len> [ttl] [tag]\n<value>`,
/// mirroring `Response::Incremented`'s own wire form on the node side
/// (`src/response.rs`).
fn respond_incremented(value: &[u8], ttl: Option<u64>, tag: Option<u32>) -> Vec<u8> {
    let ttl_field = ttl.map(|ttl| format!(" {ttl}")).unwrap_or_default();
    let header = match tag {
        Some(tag) => format!("I {}{ttl_field} {tag}\n", value.len()),
        None => format!("I {}{ttl_field}\n", value.len()),
    };
    let mut framed = header.into_bytes();
    framed.extend_from_slice(value);
    framed
}

/// Errors that end the client connection: encoded as `E` (+tag), the
/// writer closes after sending. See the module docs' failure-surface
/// note for why upstream failure is fatal to the connection.
struct Fatal;

type DriverResult = Result<Vec<u8>, Fatal>;

/// Issue #125: on a connection whose client declared the retryable-error
/// capability (`A ... R`), an upstream failure that would otherwise be
/// the fatal `E`-and-close becomes a per-request `R` (+tag) and the
/// connection stays open — "this request failed transiently, retry it
/// shortly" is the honest answer, and the poisoned backend has already
/// been dropped for redial by the time this reply is written, so the
/// retry has a fresh connection to land on. Counted as an upstream
/// failure either way (the writer counts the fatal path; this counts
/// the softened one).
/// Issue #125: the per-request "no roster / no owners yet" reply. For a
/// retry-capable client this is a tagged `R` — it is answering one
/// specific request, and the bare untagged `B` desyncs a tagged
/// connection's response pairing. Legacy clients keep the exact
/// pre-#125 bytes (bare `B`), warts and all, rather than being handed
/// a frame shape they never learned.
fn transient_reply(retry_capable: bool, tag: Option<u32>) -> Vec<u8> {
    if retry_capable {
        respond("R", tag)
    } else {
        respond("B", None)
    }
}

fn soften(
    result: DriverResult,
    retry_capable: bool,
    tag: Option<u32>,
    context: &ProxyContext,
) -> DriverResult {
    match result {
        Err(Fatal) if retry_capable => {
            context
                .upstream_failures_total
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            Ok(respond("R", tag))
        }
        other => other,
    }
}

/// Dispatches one parsed request: performs the *initial* backend
/// enqueues inline — in the reader's request order, which is what
/// preserves a pipelined connection's dependency chains (same key →
/// same backend queue; a clear fans out to every backend queue before
/// any later request reaches any of them) — then spawns the remainder
/// (awaiting replies, `W` refresh-and-retry, replica fallback) and
/// returns the receiver the writer will await in FIFO order. Retry
/// paths re-enqueue *after* initial dispatch and so run outside the
/// ordering guarantee; they only ever fire when a node already refused
/// or dropped the ordered attempt. The client-visible consequence (a
/// pipelined same-key `G` retried after `W` can observe the client's
/// own later `S`) is a documented limitation — see the module docs and
/// issue #463 for why it is left as is.
async fn dispatch_request(
    context: Arc<ProxyContext>,
    request: Request,
    tag: Option<u32>,
    retry_capable: bool,
) -> oneshot::Receiver<DriverResult> {
    context
        .requests_total
        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);

    let (result_tx, result_rx) = oneshot::channel();

    let Some(ring) = current_ring(&context) else {
        // No roster yet: `R` for a retry-capable client (tagged — this
        // answers a specific request), the legacy bare `B` otherwise.
        let _ = result_tx.send(Ok(transient_reply(retry_capable, tag)));
        return result_rx;
    };

    match request {
        Request::Get { namespace, key } => {
            let owners = ring.owners(&namespace, &key);
            let Some(primary) = owners.first() else {
                let _ = result_tx.send(Ok(transient_reply(retry_capable, tag)));
                return result_rx;
            };
            let pending = context
                .backends
                .enqueue(
                    &context,
                    primary,
                    frame_get(&namespace, &key),
                    Expect::Value,
                )
                .await;
            tokio::spawn(async move {
                let result = finish_get(&context, &namespace, &key, owners, pending, tag).await;
                let _ = result_tx.send(soften(result, retry_capable, tag, &context));
            });
        }
        Request::Set {
            namespace,
            key,
            value,
            ttl,
        } => {
            let pending =
                enqueue_write(&context, &ring, &namespace, &key, Some((&value, ttl))).await;
            tokio::spawn(async move {
                let write = Some((value, ttl));
                let result = finish_write(
                    &context,
                    &namespace,
                    &key,
                    write,
                    pending,
                    tag,
                    retry_capable,
                )
                .await;
                let _ = result_tx.send(soften(result, retry_capable, tag, &context));
            });
        }
        Request::Delete { namespace, key } => {
            let pending = enqueue_write(&context, &ring, &namespace, &key, None).await;
            tokio::spawn(async move {
                let result = finish_write(
                    &context,
                    &namespace,
                    &key,
                    None,
                    pending,
                    tag,
                    retry_capable,
                )
                .await;
                let _ = result_tx.send(soften(result, retry_capable, tag, &context));
            });
        }
        Request::Incr {
            namespace,
            key,
            delta,
        } => {
            let owners = ring.owners(&namespace, &key);
            let Some(primary) = owners.first() else {
                let _ = result_tx.send(Ok(transient_reply(retry_capable, tag)));
                return result_rx;
            };
            let pending = context
                .backends
                .enqueue(
                    &context,
                    primary,
                    frame_incr(&namespace, &key, delta),
                    Expect::Incremented,
                )
                .await;
            tokio::spawn(async move {
                let result = finish_incr(
                    &context,
                    (&namespace, &key),
                    delta,
                    owners,
                    pending,
                    tag,
                    retry_capable,
                )
                .await;
                let _ = result_tx.send(soften(result, retry_capable, tag, &context));
            });
        }
        Request::CasSet {
            namespace,
            key,
            condition,
            value,
            ttl,
        } => {
            let owners = ring.owners(&namespace, &key);
            let Some(primary) = owners.first() else {
                let _ = result_tx.send(Ok(transient_reply(retry_capable, tag)));
                return result_rx;
            };
            let pending = context
                .backends
                .enqueue(
                    &context,
                    primary,
                    frame_cas_set(&namespace, &key, condition, &value, ttl),
                    Expect::CasSet,
                )
                .await;
            tokio::spawn(async move {
                let result = finish_cas_set(
                    &context,
                    (&namespace, &key),
                    (condition, &value, ttl),
                    owners,
                    pending,
                    tag,
                    retry_capable,
                )
                .await;
                let _ = result_tx.send(soften(result, retry_capable, tag, &context));
            });
        }
        Request::CasDelete {
            namespace,
            key,
            expected_digest,
        } => {
            let owners = ring.owners(&namespace, &key);
            let Some(primary) = owners.first() else {
                let _ = result_tx.send(Ok(transient_reply(retry_capable, tag)));
                return result_rx;
            };
            let pending = context
                .backends
                .enqueue(
                    &context,
                    primary,
                    frame_cas_delete(&namespace, &key, expected_digest),
                    Expect::Deleted,
                )
                .await;
            tokio::spawn(async move {
                let result = finish_cas_delete(
                    &context,
                    (&namespace, &key),
                    expected_digest,
                    owners,
                    pending,
                    tag,
                    retry_capable,
                )
                .await;
                let _ = result_tx.send(soften(result, retry_capable, tag, &context));
            });
        }
        // Issue #150: group keys by primary owner — this is the one
        // dispatch shape that splits a single client frame across
        // multiple backend sub-frames (every other op resolves to
        // exactly one key's worth of backend traffic). The first pass is
        // primary only, matching `Request::Get`'s own primary-first
        // first attempt in `finish_get`. A failed or `W` sub-batch gets
        // one bounded refresh-and-retry in
        // `finish_multi_get`/`retry_multi_get`; issue #221 made that
        // retry fall through the remaining owners (replicas) exactly
        // like `retry_get_on` does for single-key `Get`, instead of
        // giving up after the fresh primary alone.
        Request::MultiGet { namespace, keys } => {
            let mut groups: Vec<(String, Vec<usize>, Vec<Bytes>)> = Vec::new();
            let mut missing = Vec::new();

            for (position, key) in keys.iter().enumerate() {
                let mut owners = ring.owners(&namespace, key).into_iter();
                let Some(primary) = owners.next() else {
                    missing.push(position);
                    continue;
                };

                if let Some(group) = groups.iter_mut().find(|(owner, ..)| *owner == primary) {
                    group.1.push(position);
                    group.2.push(key.clone());
                } else {
                    groups.push((primary, vec![position], vec![key.clone()]));
                }
            }

            if groups.is_empty() {
                let _ = result_tx.send(Ok(transient_reply(retry_capable, tag)));
                return result_rx;
            }

            // Issue #177: every group's `enqueue` (including whatever
            // dial it takes) runs concurrently — a slow-to-dial owner
            // must not delay issuing the request to the others.
            let futs: Vec<Pin<Box<dyn Future<Output = PendingReply> + Send + '_>>> = groups
                .iter()
                .map(|(owner, _, group_keys)| {
                    let fut = context.backends.enqueue(
                        &context,
                        owner,
                        frame_multi_get(&namespace, group_keys),
                        Expect::Multi,
                    );
                    Box::pin(fut) as Pin<Box<dyn Future<Output = PendingReply> + Send + '_>>
                })
                .collect();
            let pending = join_all(futs).await;
            let positions: Vec<Vec<usize>> = groups
                .into_iter()
                .map(|(_, positions, _)| positions)
                .collect();

            tokio::spawn(async move {
                let result = finish_multi_get(
                    &context, &namespace, &keys, missing, positions, pending, tag,
                )
                .await;
                let _ = result_tx.send(soften(result, retry_capable, tag, &context));
            });
        }
        // Issue #150: unlike `MultiGet`'s by-primary grouping, a write
        // must reach every owner, so this groups by owner *address* —
        // a batch's keys can put the same node in different roles
        // (primary for one key, replica for another), and grouping by
        // address collapses that into one sub-frame per node either
        // way. Each leg remembers whether its owner is that key's
        // primary: only the primary's outcome decides the key's
        // client-facing status; replica outcomes are logged and
        // swallowed, exactly like `finish_write`'s existing stance.
        Request::MultiSet {
            namespace,
            keys,
            values,
            ttl,
        } => {
            let mut groups: Vec<(String, Vec<(usize, bool)>)> = Vec::new();
            let mut missing = Vec::new();

            for (position, key) in keys.iter().enumerate() {
                let owners = ring.owners(&namespace, key);
                if owners.is_empty() {
                    missing.push(position);
                    continue;
                }
                for (rank, owner) in owners.iter().enumerate() {
                    let leg = (position, rank == 0);
                    if let Some(group) = groups.iter_mut().find(|(addr, _)| addr == owner) {
                        group.1.push(leg);
                    } else {
                        groups.push((owner.clone(), vec![leg]));
                    }
                }
            }

            if groups.is_empty() {
                let _ = result_tx.send(Ok(transient_reply(retry_capable, tag)));
                return result_rx;
            }

            // Issue #177: same concurrent fan-out as `MultiGet` above —
            // one slow-to-dial owner must not delay the other groups.
            let futs: Vec<Pin<Box<dyn Future<Output = PendingReply> + Send + '_>>> = groups
                .iter()
                .map(|(owner, legs)| {
                    let group_keys: Vec<Bytes> = legs
                        .iter()
                        .map(|(position, _)| keys[*position].clone())
                        .collect();
                    let group_values: Vec<Bytes> = legs
                        .iter()
                        .map(|(position, _)| values[*position].clone())
                        .collect();
                    let fut = context.backends.enqueue(
                        &context,
                        owner,
                        frame_multi_set(&namespace, &group_keys, &group_values, ttl),
                        Expect::MultiAck,
                    );
                    Box::pin(fut) as Pin<Box<dyn Future<Output = PendingReply> + Send + '_>>
                })
                .collect();
            let pending = join_all(futs).await;
            let groups_legs: Vec<Vec<(usize, bool)>> =
                groups.into_iter().map(|(_, legs)| legs).collect();

            tokio::spawn(async move {
                let result = finish_multi_set(
                    &context,
                    MultiSetBatch {
                        namespace: &namespace,
                        keys: &keys,
                        values: &values,
                        ttl,
                    },
                    missing,
                    groups_legs,
                    pending,
                    tag,
                )
                .await;
                let _ = result_tx.send(soften(result, retry_capable, tag, &context));
            });
        }
        Request::Clear { namespace } => {
            let pending = enqueue_clear(&context, &ring, Some(&namespace)).await;
            tokio::spawn(async move {
                let result = finish_clear(&context, Some(namespace), pending, tag).await;
                let _ = result_tx.send(soften(result, retry_capable, tag, &context));
            });
        }
        Request::ClearAll => {
            let pending = enqueue_clear(&context, &ring, None).await;
            tokio::spawn(async move {
                let result = finish_clear(&context, None, pending, tag).await;
                let _ = result_tx.send(soften(result, retry_capable, tag, &context));
            });
        }
    }

    result_rx
}

/// A read's completion: the primary's ordered reply first; a `W` forces
/// one refresh-and-reroute, an unreachable owner falls through to the
/// next (the key's replicas hold live copies — the SDKs' dead-primary
/// stance).
async fn finish_get(
    context: &ProxyContext,
    namespace: &[u8],
    key: &[u8],
    owners: Vec<String>,
    pending: PendingReply,
    tag: Option<u32>,
) -> DriverResult {
    match pending.await {
        Ok(NodeReply::Value(value)) => return Ok(respond_value(&value, tag)),
        Ok(NodeReply::NotFound) => return Ok(respond("N", tag)),
        Ok(NodeReply::WrongNode) => {
            force_refresh(context).await;
            let Some(ring) = current_ring(context) else {
                return Err(Fatal);
            };
            return retry_get_on(context, namespace, key, ring.owners(namespace, key), tag).await;
        }
        Ok(_) | Err(_) => {}
    }

    // The ordered primary attempt failed outright: try the remaining
    // owners *first* (issue #177) — a black-holed primary shouldn't
    // cost another full `UPSTREAM_IO_TIMEOUT` before a live replica
    // even gets a chance. The primary is still retried last rather than
    // dropped, in case this was merely an idle-closed shared connection
    // recovering via `call`'s own transparent redial (issue #110 — a
    // long-lived shared connection makes that the common case, not the
    // rare one); by the time it's retried it also falls within
    // `enqueue`'s dial backoff window, so a genuinely dead primary fails
    // that last attempt fast instead of costing a second full timeout.
    let mut retry_owners = owners;
    if !retry_owners.is_empty() {
        let failed_primary = retry_owners.remove(0);
        retry_owners.push(failed_primary);
    }
    retry_get_on(context, namespace, key, retry_owners, tag).await
}

async fn retry_get_on(
    context: &ProxyContext,
    namespace: &[u8],
    key: &[u8],
    owners: Vec<String>,
    tag: Option<u32>,
) -> DriverResult {
    for addr in &owners {
        match context
            .backends
            .call(context, addr, frame_get(namespace, key), Expect::Value)
            .await
        {
            Ok(NodeReply::Value(value)) => return Ok(respond_value(&value, tag)),
            Ok(NodeReply::NotFound) => return Ok(respond("N", tag)),
            Ok(_) | Err(_) => continue,
        }
    }
    Err(Fatal)
}

/// Issue #150: awaits every owner group's `M` sub-reply and splices the
/// per-key results back into the client's original request order.
/// `missing` (no roster entry at all — only possible together with an
/// empty ring, which the dispatch arm already answers `transient_reply`
/// for before this is ever called) is answered `WrongNode` immediately.
/// A group whose reply is malformed, transport-failed, or carries a
/// per-key `WrongNode` gets exactly one bounded refresh-and-retry via
/// `retry_multi_get` — never a second one, and never a whole-frame
/// failure: partial-result semantics mean a key that's still wrong after
/// the retry degrades to its own `WrongNode` entry, same as every other
/// key's independent outcome.
async fn finish_multi_get(
    context: &ProxyContext,
    namespace: &[u8],
    keys: &[Bytes],
    missing: Vec<usize>,
    positions: Vec<Vec<usize>>,
    pending: Vec<PendingReply>,
    tag: Option<u32>,
) -> DriverResult {
    let mut entries: Vec<Option<ProxyMultiEntry>> = vec![None; keys.len()];
    let mut retry_positions = Vec::new();

    for position in missing {
        entries[position] = Some(ProxyMultiEntry::WrongNode);
    }

    for (group_positions, reply) in positions.into_iter().zip(pending) {
        match reply.await {
            Ok(NodeReply::Multi(results)) if results.len() == group_positions.len() => {
                for (position, entry) in group_positions.into_iter().zip(results) {
                    if matches!(entry, ProxyMultiEntry::WrongNode) {
                        retry_positions.push(position);
                    } else {
                        entries[position] = Some(entry);
                    }
                }
            }
            _ => retry_positions.extend(group_positions),
        }
    }

    if !retry_positions.is_empty() {
        retry_multi_get(context, namespace, keys, &retry_positions, &mut entries).await;
    }

    let entries: Vec<ProxyMultiEntry> = entries
        .into_iter()
        .map(|entry| entry.unwrap_or(ProxyMultiEntry::WrongNode))
        .collect();
    Ok(respond_multi(&entries, tag))
}

/// Issue #150/#221: the one bounded refresh-and-retry pass for keys the
/// first pass left inconclusive — mirrors `finish_get`'s "a single
/// refresh-and-reroute, not a loop," but, like `retry_get_on`, that one
/// reroute can still fall through every remaining owner for a key, not
/// just its fresh primary. Retried keys are first regrouped by their
/// *fresh* top owner (which can differ per key even though they shared a
/// primary before the refresh — a stale ring can be wrong about more
/// than one key's placement at once). A key whose fresh-primary attempt
/// transport-fails, or comes back `W` (that owner's own view says it
/// isn't responsible — "unknown here," not a miss, same reading
/// `retry_get_on` gives a non-final `WrongNode`), moves on to the next
/// owner in its ranking; a real `Value`/`Miss` is trusted immediately and
/// never retried further, matching `retry_get_on`'s trust of a replica's
/// `NotFound`. Each rank (primary, first replica, second replica, ...)
/// gets exactly one dispatched attempt per still-unresolved key, bounded
/// by that key's own owner count — the replication factor — so this
/// can't loop indefinitely. Fills every position in `retry_positions` —
/// with a real result or a final `WrongNode` once every owner is
/// exhausted — so the caller never sees a gap.
async fn retry_multi_get(
    context: &ProxyContext,
    namespace: &[u8],
    keys: &[Bytes],
    retry_positions: &[usize],
    entries: &mut [Option<ProxyMultiEntry>],
) {
    force_refresh(context).await;

    let Some(ring) = current_ring(context) else {
        for &position in retry_positions {
            entries[position] = Some(ProxyMultiEntry::WrongNode);
        }
        return;
    };

    let mut owners_by_position: HashMap<usize, Vec<String>> = HashMap::new();
    let mut unresolved: Vec<usize> = Vec::new();
    for &position in retry_positions {
        let key = &keys[position];
        let owners = ring.owners(namespace, key);
        if owners.is_empty() {
            entries[position] = Some(ProxyMultiEntry::WrongNode);
            continue;
        }
        owners_by_position.insert(position, owners);
        unresolved.push(position);
    }

    let mut rank = 0;
    while !unresolved.is_empty() {
        let mut groups: Vec<(String, Vec<usize>, Vec<Bytes>)> = Vec::new();
        let mut still_unresolved = Vec::new();

        for position in unresolved {
            let owner = match owners_by_position[&position].get(rank) {
                Some(owner) => owner.clone(),
                None => {
                    // Every owner for this key has now been tried once.
                    entries[position] = Some(ProxyMultiEntry::WrongNode);
                    continue;
                }
            };
            let key = keys[position].clone();
            if let Some(group) = groups.iter_mut().find(|(addr, ..)| *addr == owner) {
                group.1.push(position);
                group.2.push(key);
            } else {
                groups.push((owner, vec![position], vec![key]));
            }
        }

        if groups.is_empty() {
            break;
        }

        // Issue #177: fan this rank's regrouped retry out to every owner
        // concurrently, same reasoning as the first pass in
        // `dispatch_request`.
        let futs: Vec<Pin<Box<dyn Future<Output = io::Result<NodeReply>> + Send + '_>>> = groups
            .iter()
            .map(|(owner, _, group_keys)| {
                let fut = context.backends.call(
                    context,
                    owner,
                    frame_multi_get(namespace, group_keys),
                    Expect::Multi,
                );
                Box::pin(fut) as Pin<Box<dyn Future<Output = io::Result<NodeReply>> + Send + '_>>
            })
            .collect();
        let replies = join_all(futs).await;

        for ((_, group_positions, _), reply) in groups.into_iter().zip(replies) {
            match reply {
                Ok(NodeReply::Multi(results)) if results.len() == group_positions.len() => {
                    for (position, entry) in group_positions.into_iter().zip(results) {
                        if matches!(entry, ProxyMultiEntry::WrongNode) {
                            still_unresolved.push(position);
                        } else {
                            entries[position] = Some(entry);
                        }
                    }
                }
                _ => still_unresolved.extend(group_positions),
            }
        }

        unresolved = still_unresolved;
        rank += 1;
    }
}

/// Issue #150: the batch a `finish_multi_set`/`retry_multi_set` pass
/// works against — bundled purely to stay under clippy's argument-count
/// lint; `keys`/`values` are parallel, `ttl` is the one shared value for
/// the whole batch (see `Command::MultiSet`'s doc comment).
struct MultiSetBatch<'a> {
    namespace: &'a [u8],
    keys: &'a [Bytes],
    values: &'a [Bytes],
    ttl: Option<u64>,
}

/// Issue #150: awaits every owner group's `O` sub-reply and reduces it to
/// each key's client-facing status. Only a *primary* leg's outcome
/// matters for that key; a replica leg's `WrongNode` or transport
/// failure is logged and swallowed, exactly like `finish_write`'s
/// existing stance (the primary already has — or, after the retry
/// below, will have — the authoritative copy). A primary `WrongNode` or
/// a whole group that failed/came back malformed marks its primary legs
/// for the same one-bounded-retry pass `finish_multi_get` uses.
async fn finish_multi_set(
    context: &ProxyContext,
    batch: MultiSetBatch<'_>,
    missing: Vec<usize>,
    groups_legs: Vec<Vec<(usize, bool)>>,
    pending: Vec<PendingReply>,
    tag: Option<u32>,
) -> DriverResult {
    let MultiSetBatch {
        namespace,
        keys,
        values,
        ttl,
    } = batch;
    let mut entries: Vec<Option<ProxyAckEntry>> = vec![None; keys.len()];
    let mut retry_positions = Vec::new();

    for position in missing {
        entries[position] = Some(ProxyAckEntry::WrongNode);
    }

    for (legs, reply) in groups_legs.into_iter().zip(pending) {
        match reply.await {
            Ok(NodeReply::MultiAck(results)) if results.len() == legs.len() => {
                for ((position, is_primary), result) in legs.into_iter().zip(results) {
                    if !is_primary {
                        if matches!(result, ProxyAckEntry::WrongNode) {
                            eprintln!("WARN replica multi-set leg wrong-node for one key");
                        }
                        continue;
                    }
                    match result {
                        ProxyAckEntry::Stored => entries[position] = Some(ProxyAckEntry::Stored),
                        ProxyAckEntry::WrongNode => retry_positions.push(position),
                    }
                }
            }
            _ => {
                for (position, is_primary) in legs {
                    if is_primary {
                        retry_positions.push(position);
                    } else {
                        eprintln!("WARN replica multi-set leg failed");
                    }
                }
            }
        }
    }

    if !retry_positions.is_empty() {
        retry_multi_set(
            context,
            MultiSetBatch {
                namespace,
                keys,
                values,
                ttl,
            },
            &retry_positions,
            &mut entries,
        )
        .await;
    }

    let entries: Vec<ProxyAckEntry> = entries
        .into_iter()
        .map(|entry| entry.unwrap_or(ProxyAckEntry::WrongNode))
        .collect();
    Ok(respond_multi_ack(&entries, tag))
}

/// Issue #150: the one bounded refresh-and-retry pass for multi-set,
/// mirroring `refan_write`'s "re-send to every owner including
/// replicas" — a retried key's whole write is re-fanned from the fresh
/// ring, not just its primary, since the replicas that already got the
/// first pass's write are harmless to write again (the op is
/// idempotent, same reasoning `refan_write` relies on for single-key
/// writes).
async fn retry_multi_set(
    context: &ProxyContext,
    batch: MultiSetBatch<'_>,
    retry_positions: &[usize],
    entries: &mut [Option<ProxyAckEntry>],
) {
    let MultiSetBatch {
        namespace,
        keys,
        values,
        ttl,
    } = batch;
    force_refresh(context).await;

    let Some(ring) = current_ring(context) else {
        for &position in retry_positions {
            entries[position] = Some(ProxyAckEntry::WrongNode);
        }
        return;
    };

    let mut groups: Vec<(String, Vec<(usize, bool)>)> = Vec::new();
    for &position in retry_positions {
        let key = &keys[position];
        let owners = ring.owners(namespace, key);
        if owners.is_empty() {
            entries[position] = Some(ProxyAckEntry::WrongNode);
            continue;
        }
        for (rank, owner) in owners.iter().enumerate() {
            let leg = (position, rank == 0);
            if let Some(group) = groups.iter_mut().find(|(addr, _)| addr == owner) {
                group.1.push(leg);
            } else {
                groups.push((owner.clone(), vec![leg]));
            }
        }
    }

    // Issue #177: same concurrent fan-out as `retry_multi_get` above.
    let futs: Vec<Pin<Box<dyn Future<Output = io::Result<NodeReply>> + Send + '_>>> = groups
        .iter()
        .map(|(owner, legs)| {
            let group_keys: Vec<Bytes> = legs
                .iter()
                .map(|(position, _)| keys[*position].clone())
                .collect();
            let group_values: Vec<Bytes> = legs
                .iter()
                .map(|(position, _)| values[*position].clone())
                .collect();
            let fut = context.backends.call(
                context,
                owner,
                frame_multi_set(namespace, &group_keys, &group_values, ttl),
                Expect::MultiAck,
            );
            Box::pin(fut) as Pin<Box<dyn Future<Output = io::Result<NodeReply>> + Send + '_>>
        })
        .collect();
    let replies = join_all(futs).await;

    for ((_, legs), reply) in groups.into_iter().zip(replies) {
        match reply {
            Ok(NodeReply::MultiAck(results)) if results.len() == legs.len() => {
                for ((position, is_primary), result) in legs.into_iter().zip(results) {
                    if is_primary {
                        entries[position] = Some(result);
                    } else if matches!(result, ProxyAckEntry::WrongNode) {
                        eprintln!("WARN replica multi-set leg wrong-node for one key (retry)");
                    }
                }
            }
            _ => {
                for (position, is_primary) in legs {
                    if is_primary {
                        entries[position] = Some(ProxyAckEntry::WrongNode);
                    } else {
                        eprintln!("WARN replica multi-set leg failed (retry)");
                    }
                }
            }
        }
    }
}

/// Enqueues a write/delete on every owner concurrently (issue #177: one
/// owner slow to dial must not delay the others); returns the pending
/// replies in owner order, primary first.
async fn enqueue_write(
    context: &ProxyContext,
    ring: &RingView,
    namespace: &[u8],
    key: &[u8],
    write: Option<(&Bytes, Option<u64>)>,
) -> Vec<PendingReply> {
    let owners = ring.owners(namespace, key);
    let (frame, expect) = write_frame(namespace, key, write);
    let futs: Vec<Pin<Box<dyn Future<Output = PendingReply> + Send + '_>>> = owners
        .iter()
        .map(|addr| {
            let fut = context
                .backends
                .enqueue(context, addr, frame.clone(), expect);
            Box::pin(fut) as Pin<Box<dyn Future<Output = PendingReply> + Send + '_>>
        })
        .collect();
    join_all(futs).await
}

fn write_frame(
    namespace: &[u8],
    key: &[u8],
    write: Option<(&Bytes, Option<u64>)>,
) -> (Bytes, Expect) {
    match write {
        Some((value, ttl)) => (frame_set(namespace, key, value, ttl), Expect::Stored),
        None => (frame_delete(namespace, key), Expect::Deleted),
    }
}

/// A write/delete's completion: the primary's ack answers the client,
/// replica failures are logged and swallowed (the SDKs' replica-leg
/// stance) — but awaited before answering, so a client that reads its
/// own write through a replica isn't racing the leg. A primary `W`
/// forces one refresh-and-refan.
async fn finish_write(
    context: &ProxyContext,
    namespace: &[u8],
    key: &[u8],
    write: Option<(Bytes, Option<u64>)>,
    pending: Vec<PendingReply>,
    tag: Option<u32>,
    retry_capable: bool,
) -> DriverResult {
    let write_ref = write.as_ref().map(|(value, ttl)| (value, *ttl));
    let mut replies = Vec::with_capacity(pending.len());
    for reply in pending {
        replies.push(reply.await);
    }
    let Some(primary_reply) = replies.first() else {
        // Empty roster at enqueue time — same transient class as the
        // dispatch-time check above.
        return Ok(transient_reply(retry_capable, tag));
    };
    for replica_reply in replies.iter().skip(1) {
        if let Err(error) = replica_reply {
            eprintln!("WARN replica write failed: {error}");
        }
    }

    match primary_reply {
        Ok(NodeReply::Stored) => Ok(respond("S", tag)),
        Ok(NodeReply::Deleted) => Ok(respond("D", tag)),
        Ok(NodeReply::NotFound) => Ok(respond("N", tag)),
        Ok(NodeReply::WrongNode) => {
            // Stale roster: refresh, then re-fan on the new owner set.
            force_refresh(context).await;
            refan_write(context, namespace, key, write_ref, tag, retry_capable).await
        }
        // A transport failure on the ordered attempt: the shared
        // connection may simply have been idle-closed by the node
        // (issue #110 — long-lived shared connections make that the
        // common case). Re-fan once via `call`, whose transparent
        // redial recovers it; a second failure is real.
        Err(_) => refan_write(context, namespace, key, write_ref, tag, retry_capable).await,
        Ok(_) => Err(Fatal),
    }
}

/// One whole write fan-out over the *current* ring via `call` (redials
/// dead shared connections) — `finish_write`'s retry path for both a
/// primary `W` and a transport failure.
async fn refan_write(
    context: &ProxyContext,
    namespace: &[u8],
    key: &[u8],
    write: Option<(&Bytes, Option<u64>)>,
    tag: Option<u32>,
    retry_capable: bool,
) -> DriverResult {
    let Some(ring) = current_ring(context) else {
        return Err(Fatal);
    };
    let owners = ring.owners(namespace, key);
    let (frame, expect) = write_frame(namespace, key, write);
    let Some((primary, replicas)) = owners.split_first() else {
        return Ok(transient_reply(retry_capable, tag));
    };
    // Issue #177: every replica leg's `call` runs concurrently — a
    // black-holed replica must not delay the others, or delay reaching
    // the primary below.
    let futs: Vec<Pin<Box<dyn Future<Output = io::Result<NodeReply>> + Send + '_>>> = replicas
        .iter()
        .map(|addr| {
            let fut = context.backends.call(context, addr, frame.clone(), expect);
            Box::pin(fut) as Pin<Box<dyn Future<Output = io::Result<NodeReply>> + Send + '_>>
        })
        .collect();
    for (addr, result) in replicas.iter().zip(join_all(futs).await) {
        if let Err(error) = result {
            eprintln!("WARN replica write to {addr} failed: {error}");
        }
    }
    match context.backends.call(context, primary, frame, expect).await {
        Ok(NodeReply::Stored) => Ok(respond("S", tag)),
        Ok(NodeReply::Deleted) => Ok(respond("D", tag)),
        Ok(NodeReply::NotFound) => Ok(respond("N", tag)),
        _ => Err(Fatal),
    }
}

/// `Incr`'s completion: the primary's ordered `i` reply drives the
/// result. Unlike `finish_write`, replicas are never sent the increment
/// itself up front — only once the primary's result is known is that
/// *result* fanned out to the remaining owners as a plain `Set`,
/// carrying the TTL the primary's `I` reply itself carried
/// (`fan_out_write_result`). Replaying `i` on a replica instead would let
/// it drift from the primary (e.g. after an eviction or a replica leg
/// that missed an earlier write) — the same reasoning
/// `src/server.rs`'s `Incr` connection handler documents for the
/// node-local migration/decommission case. A primary `W` or a transport
/// failure that provably never reached the wire re-runs the whole thing
/// (primary INCR + replica fan-out) on the refreshed ring (`refan_incr`),
/// same as a write's own `W`/failure handling — but unlike a write,
/// INCR is not idempotent: once the frame may have reached the primary,
/// replaying it risks double-applying the delta (issue #272, mirrors
/// #225's SDK-side fix), so that case is surfaced to the client instead
/// of retried.
async fn finish_incr(
    context: &ProxyContext,
    // `(namespace, key)`, grouped into one parameter purely to stay under
    // clippy's argument-count lint — always used together.
    address: (&[u8], &[u8]),
    delta: i64,
    owners: Vec<String>,
    pending: PendingReply,
    tag: Option<u32>,
    retry_capable: bool,
) -> DriverResult {
    let (namespace, key) = address;
    match pending.await {
        Ok(NodeReply::Incremented(value, ttl)) => {
            fan_out_write_result(context, namespace, key, &value, ttl, &owners[1..]).await;
            Ok(respond_incremented(&value, ttl, tag))
        }
        Ok(NodeReply::NotFound) => Ok(respond("N", tag)),
        Ok(NodeReply::NotNumeric) => Ok(respond("T", tag)),
        Ok(NodeReply::WrongNode) => {
            force_refresh(context).await;
            refan_incr(context, address, delta, tag, retry_capable).await
        }
        // Issue #272: only safe to retry when the frame provably never
        // reached the backend socket (`request_not_sent`) — the "shared
        // connection may simply have been idle-closed" case `finish_write`
        // relies on. Once the frame may have reached the wire, retrying
        // would risk a double-apply, so this surfaces `Fatal` instead
        // (softened to a per-request `R` for a retry-capable client —
        // the client's own choice whether to re-issue the INCR).
        Err(error) if request_not_sent(&error) => {
            refan_incr(context, address, delta, tag, retry_capable).await
        }
        Err(_) => Err(Fatal),
        Ok(_) => Err(Fatal),
    }
}

/// Fans a successful primary write's *result* out to `replicas` as a
/// plain `Set` — shared by `INCR` (never replaying `i` itself, see
/// `finish_incr`'s doc comment) and `k`/compare-and-set (never replaying
/// `k` itself, see `finish_cas_set`'s doc comment); both compute or
/// accept a value on the primary that a replica must not be left to
/// (re)derive on its own. Failures are logged and swallowed, the same
/// stance `finish_write`'s replica legs take: an under-replicated entry
/// is recovered by the next node-list refresh, never fails the client's
/// already-successful write.
async fn fan_out_write_result(
    context: &ProxyContext,
    namespace: &[u8],
    key: &[u8],
    value: &[u8],
    ttl: Option<u64>,
    replicas: &[String],
) {
    // Issue #177: concurrent, same reasoning as `refan_write`'s replica
    // loop.
    let frame = frame_set(namespace, key, value, ttl);
    let futs: Vec<Pin<Box<dyn Future<Output = io::Result<NodeReply>> + Send + '_>>> = replicas
        .iter()
        .map(|addr| {
            let fut = context
                .backends
                .call(context, addr, frame.clone(), Expect::Stored);
            Box::pin(fut) as Pin<Box<dyn Future<Output = io::Result<NodeReply>> + Send + '_>>
        })
        .collect();
    for (addr, result) in replicas.iter().zip(join_all(futs).await) {
        if let Err(error) = result {
            eprintln!("WARN replica incr-result write to {addr} failed: {error}");
        }
    }
}

/// `finish_incr`'s retry path for both a primary `W` and a transport
/// failure provably never sent: re-fetches the current ring and runs the
/// whole INCR (primary leg, then the replica fan-out) again via
/// `call_non_idempotent`, whose transparent redial recovers a dead
/// shared connection WITHOUT replaying the `i` frame once it may have
/// reached the wire (issue #272) — unlike `refan_write`'s `call`, which
/// is safe to retry unconditionally only because Set/Delete are
/// idempotent.
async fn refan_incr(
    context: &ProxyContext,
    address: (&[u8], &[u8]),
    delta: i64,
    tag: Option<u32>,
    retry_capable: bool,
) -> DriverResult {
    let (namespace, key) = address;
    let Some(ring) = current_ring(context) else {
        return Err(Fatal);
    };
    let owners = ring.owners(namespace, key);
    let Some((primary, replicas)) = owners.split_first() else {
        return Ok(transient_reply(retry_capable, tag));
    };
    match context
        .backends
        .call_non_idempotent(
            context,
            primary,
            frame_incr(namespace, key, delta),
            Expect::Incremented,
        )
        .await
    {
        Ok(NodeReply::Incremented(value, ttl)) => {
            fan_out_write_result(context, namespace, key, &value, ttl, replicas).await;
            Ok(respond_incremented(&value, ttl, tag))
        }
        Ok(NodeReply::NotFound) => Ok(respond("N", tag)),
        Ok(NodeReply::NotNumeric) => Ok(respond("T", tag)),
        _ => Err(Fatal),
    }
}

/// `k`'s completion: the primary's ordered reply drives the result.
/// Unlike `finish_write`, replicas are never sent the compare-and-set
/// itself up front — a replica evaluating the same condition against its
/// own (possibly different) copy could reach a different outcome than
/// the primary just did. Only once the primary's condition holds is the
/// resulting *value* fanned out to the remaining owners as a plain `Set`
/// (`fan_out_write_result`, shared with `INCR` — see its own doc
/// comment). A primary `W` re-runs the whole thing on the refreshed ring
/// (`refan_cas_set`), same as `finish_incr`. A transport failure only
/// does the same when the frame provably never reached the wire — like
/// `INCR`, `k` is not idempotent (its condition can flip between
/// attempts), so once the frame may have reached the primary, replaying
/// it risks a double-apply and is surfaced to the client instead (issue
/// #293, mirrors #272's fix for `INCR`) — see the `Err` arm below.
async fn finish_cas_set(
    context: &ProxyContext,
    // `(namespace, key)`, grouped to stay under clippy's argument-count
    // lint — always used together.
    address: (&[u8], &[u8]),
    write: (CasCondition, &[u8], Option<u64>),
    owners: Vec<String>,
    pending: PendingReply,
    tag: Option<u32>,
    retry_capable: bool,
) -> DriverResult {
    let (namespace, key) = address;
    let (_, value, ttl) = write;
    match pending.await {
        Ok(NodeReply::Stored) => {
            fan_out_write_result(context, namespace, key, value, ttl, &owners[1..]).await;
            Ok(respond("S", tag))
        }
        Ok(NodeReply::NotFound) => Ok(respond("N", tag)),
        Ok(NodeReply::WrongNode) => {
            force_refresh(context).await;
            refan_cas_set(context, address, write, tag, retry_capable).await
        }
        // Issue #293: only safe to retry when the frame provably never
        // reached the backend socket (`request_not_sent`) — same guard
        // `finish_incr` applies for #272. Once the frame may have
        // reached the wire, retrying risks a double-apply (e.g.
        // re-satisfying an "absent" condition against a value the first
        // attempt already wrote), so this surfaces `Fatal` instead
        // (softened to a per-request `R` for a retry-capable client —
        // the client's own choice whether to re-issue the CAS).
        Err(error) if request_not_sent(&error) => {
            refan_cas_set(context, address, write, tag, retry_capable).await
        }
        Err(_) => Err(Fatal),
        Ok(_) => Err(Fatal),
    }
}

/// `finish_cas_set`'s retry path for both a primary `W` and a
/// request-not-sent transport failure: re-fetches the current ring and
/// runs the whole compare-and-set (primary leg, then the replica
/// fan-out) again via `call_non_idempotent`.
///
/// Issue #322: this used to call the unconditionally-retrying `call` —
/// safe for `Set`/`Delete`, but `k` is exactly as non-idempotent here as
/// it is on `finish_cas_set`'s own first attempt (see that function's
/// doc comment and #293). Once this refanned `k` may have reached the
/// primary's socket, replaying it on a redial risks re-evaluating (and
/// possibly re-satisfying, or falsely failing) the condition against a
/// value the first attempt already changed — the same double-apply
/// `call_non_idempotent` exists to rule out for `refan_incr` (#272).
async fn refan_cas_set(
    context: &ProxyContext,
    address: (&[u8], &[u8]),
    write: (CasCondition, &[u8], Option<u64>),
    tag: Option<u32>,
    retry_capable: bool,
) -> DriverResult {
    let (namespace, key) = address;
    let (condition, value, ttl) = write;
    let Some(ring) = current_ring(context) else {
        return Err(Fatal);
    };
    let owners = ring.owners(namespace, key);
    let Some((primary, replicas)) = owners.split_first() else {
        return Ok(transient_reply(retry_capable, tag));
    };
    match context
        .backends
        .call_non_idempotent(
            context,
            primary,
            frame_cas_set(namespace, key, condition, value, ttl),
            Expect::CasSet,
        )
        .await
    {
        Ok(NodeReply::Stored) => {
            fan_out_write_result(context, namespace, key, value, ttl, replicas).await;
            Ok(respond("S", tag))
        }
        Ok(NodeReply::NotFound) => Ok(respond("N", tag)),
        _ => Err(Fatal),
    }
}

/// `x`'s completion — same shape as `finish_cas_set`, but the successful
/// result is a deletion: fanned out to replicas as a plain `Delete`,
/// never `x` itself, for the same reason `k`'s result is fanned out as a
/// plain `Set`. Retry-on-transport-failure carries the same #293 guard
/// as `finish_cas_set` — see its doc comment and the `Err` arm below.
async fn finish_cas_delete(
    context: &ProxyContext,
    address: (&[u8], &[u8]),
    expected_digest: [u8; 16],
    owners: Vec<String>,
    pending: PendingReply,
    tag: Option<u32>,
    retry_capable: bool,
) -> DriverResult {
    let (namespace, key) = address;
    match pending.await {
        Ok(NodeReply::Deleted) => {
            fan_out_delete_result(context, namespace, key, &owners[1..]).await;
            Ok(respond("D", tag))
        }
        Ok(NodeReply::NotFound) => Ok(respond("N", tag)),
        Ok(NodeReply::WrongNode) => {
            force_refresh(context).await;
            refan_cas_delete(context, address, expected_digest, tag, retry_capable).await
        }
        // Issue #293: same `request_not_sent` guard as `finish_cas_set`
        // — once the `x` frame may have reached the wire, retrying risks
        // double-applying the delete (e.g. re-satisfying a digest
        // condition against a value a concurrent write has since put
        // back), so this surfaces `Fatal` instead.
        Err(error) if request_not_sent(&error) => {
            refan_cas_delete(context, address, expected_digest, tag, retry_capable).await
        }
        Err(_) => Err(Fatal),
        Ok(_) => Err(Fatal),
    }
}

/// Fans a successful compare-and-delete's *result* out to `replicas` as a
/// plain `Delete` — never `x` itself, same "primary decides, forward the
/// literal result" rule `fan_out_write_result` documents for `k`/`INCR`.
async fn fan_out_delete_result(
    context: &ProxyContext,
    namespace: &[u8],
    key: &[u8],
    replicas: &[String],
) {
    // Issue #177: concurrent, same reasoning as `fan_out_write_result`.
    let frame = frame_delete(namespace, key);
    let futs: Vec<Pin<Box<dyn Future<Output = io::Result<NodeReply>> + Send + '_>>> = replicas
        .iter()
        .map(|addr| {
            let fut = context
                .backends
                .call(context, addr, frame.clone(), Expect::Deleted);
            Box::pin(fut) as Pin<Box<dyn Future<Output = io::Result<NodeReply>> + Send + '_>>
        })
        .collect();
    for (addr, result) in replicas.iter().zip(join_all(futs).await) {
        if let Err(error) = result {
            eprintln!("WARN replica cas-delete-result write to {addr} failed: {error}");
        }
    }
}

/// `finish_cas_delete`'s retry path for both a primary `W` and a
/// request-not-sent transport failure: re-runs the whole compare-and-delete
/// again via `call_non_idempotent`, same #322 reasoning as
/// `refan_cas_set` — `x` is no more idempotent than `k` once it may have
/// reached the primary's socket.
async fn refan_cas_delete(
    context: &ProxyContext,
    address: (&[u8], &[u8]),
    expected_digest: [u8; 16],
    tag: Option<u32>,
    retry_capable: bool,
) -> DriverResult {
    let (namespace, key) = address;
    let Some(ring) = current_ring(context) else {
        return Err(Fatal);
    };
    let owners = ring.owners(namespace, key);
    let Some((primary, replicas)) = owners.split_first() else {
        return Ok(transient_reply(retry_capable, tag));
    };
    match context
        .backends
        .call_non_idempotent(
            context,
            primary,
            frame_cas_delete(namespace, key, expected_digest),
            Expect::Deleted,
        )
        .await
    {
        Ok(NodeReply::Deleted) => {
            fan_out_delete_result(context, namespace, key, replicas).await;
            Ok(respond("D", tag))
        }
        Ok(NodeReply::NotFound) => Ok(respond("N", tag)),
        _ => Err(Fatal),
    }
}

/// Enqueues `c`/`F` on every member in one ordered pass.
async fn enqueue_clear(
    context: &ProxyContext,
    ring: &RingView,
    namespace: Option<&Bytes>,
) -> Vec<(String, PendingReply)> {
    let frame = match namespace {
        Some(namespace) => frame_clear(namespace),
        None => frame_clear_all(),
    };
    let addrs = ring.all_addresses();
    // Issue #177: fan out to every member concurrently — a single slow
    // or black-holed member must not delay `enqueue` to the rest of the
    // cluster on a `Clear`/`FlushAll`.
    let futs: Vec<Pin<Box<dyn Future<Output = PendingReply> + Send + '_>>> = addrs
        .iter()
        .map(|addr| {
            let fut = context
                .backends
                .enqueue(context, addr, frame.clone(), Expect::Cleared);
            Box::pin(fut) as Pin<Box<dyn Future<Output = PendingReply> + Send + '_>>
        })
        .collect();
    let replies = join_all(futs).await;
    addrs.into_iter().zip(replies).collect()
}

/// `c`/`F`'s completion: every member must ack. Any failure forces one
/// refresh-and-retry of the whole fan-out (the SDKs' own clear
/// semantics); a second failure is fatal — never a silent partial clear.
async fn finish_clear(
    context: &ProxyContext,
    namespace: Option<Bytes>,
    pending: Vec<(String, PendingReply)>,
    tag: Option<u32>,
) -> DriverResult {
    let mut all_ok = true;
    for (_, reply) in pending {
        all_ok &= matches!(reply.await, Ok(NodeReply::Cleared));
    }
    if all_ok {
        return Ok(respond("C", tag));
    }

    force_refresh(context).await;
    let Some(ring) = current_ring(context) else {
        return Err(Fatal);
    };
    let frame = match &namespace {
        Some(namespace) => frame_clear(namespace),
        None => frame_clear_all(),
    };
    // Issue #177: concurrent, same reasoning as `enqueue_clear`.
    let addrs = ring.all_addresses();
    let futs: Vec<Pin<Box<dyn Future<Output = io::Result<NodeReply>> + Send + '_>>> = addrs
        .iter()
        .map(|addr| {
            let fut = context
                .backends
                .call(context, addr, frame.clone(), Expect::Cleared);
            Box::pin(fut) as Pin<Box<dyn Future<Output = io::Result<NodeReply>> + Send + '_>>
        })
        .collect();
    let all_ok = join_all(futs)
        .await
        .into_iter()
        .all(|result| matches!(result, Ok(NodeReply::Cleared)));
    if all_ok {
        Ok(respond("C", tag))
    } else {
        Err(Fatal)
    }
}

// ─── the client connection ───────────────────────────────────────────

/// Constant-time secret comparison, mirroring the node's.
fn secrets_match(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    let mut diff: u8 = 0;
    for (x, y) in a.iter().zip(b.iter()) {
        diff |= x ^ y;
    }
    diff == 0
}

/// One accepted client connection: authenticates, then splits into this
/// reader (parsing frames and dispatching drivers) and a writer task
/// that delivers each driver's response in FIFO order — see the module
/// docs on pipelining. Any parse error or fatal driver result answers
/// `E` and closes, the node's own stance.
async fn handle_client(stream: ServerStream, context: Arc<ProxyContext>) -> io::Result<()> {
    let (mut read_half, mut write_half) = tokio::io::split(stream);

    // The FIFO between reader and writer: each entry resolves to the
    // bytes to send. Bounded, so a client that pipelines without reading
    // responses back-pressures the reader instead of growing a queue.
    let (fifo_tx, mut fifo_rx) = mpsc::channel::<oneshot::Receiver<DriverResult>>(CLIENT_IN_FLIGHT);

    let writer_context = Arc::clone(&context);
    let writer = tokio::spawn(async move {
        while let Some(pending) = fifo_rx.recv().await {
            match pending.await {
                Ok(Ok(response)) => {
                    // Issue #420: bounded like every other write in this
                    // module. A client that stops reading its socket
                    // would otherwise park this `write_all` forever —
                    // and `handle_client`'s teardown (`drop(fifo_tx);
                    // writer.await`) would then hang right along with
                    // it, leaking this connection's `max_connections`
                    // permit for good. A timeout is treated exactly like
                    // a write error: stop delivering and let the caller
                    // tear the connection down.
                    match timeout(CLIENT_WRITE_TIMEOUT, write_half.write_all(&response)).await {
                        Ok(Ok(())) => {}
                        _ => return write_half,
                    }
                }
                Ok(Err(Fatal)) | Err(_) => {
                    writer_context
                        .upstream_failures_total
                        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    let _ = timeout(CLIENT_WRITE_TIMEOUT, write_half.write_all(b"E\n")).await;
                    return write_half;
                }
            }
        }
        write_half
    });

    let mut buf = BytesMut::new();
    let mut authenticated = context.secret.is_none();
    // Whether an `A` handshake has already been processed on this
    // connection. Distinct from `authenticated` (true from the start in
    // no-secret mode), so the first `A` still negotiates tagging while a
    // second one is rejected rather than allowed to flip `tagged`
    // mid-stream and desync every following frame.
    let mut auth_negotiated = false;
    let mut tagged = false;
    // Issue #125: set when the client's `A` carried the `R` token.
    // Stable before any request is dispatched (auth precedes requests),
    // so it can be passed to `dispatch_request` by value.
    let mut retry_capable = false;
    let mut drain = context.drain.clone();
    let mut scan = RequestScanState::default();

    let result: io::Result<()> = 'connection: loop {
        // Parse everything already buffered before reading more.
        loop {
            match parse_request(&mut buf, tagged, &mut scan) {
                Ok(ParseOutcome::Incomplete) => break,
                Ok(ParseOutcome::Auth {
                    secret,
                    tagging,
                    retry_capable: retryable,
                }) => {
                    if auth_negotiated {
                        // A repeat `A` after the handshake is a protocol
                        // violation — answer `E` and close (the node's
                        // stance on any protocol error) rather than let it
                        // re-negotiate tagging on a live connection.
                        let (response_tx, response_rx) = oneshot::channel();
                        let _ = response_tx.send(Err(Fatal));
                        let _ = fifo_tx.send(response_rx).await;
                        break 'connection Ok(());
                    }
                    let accepted = match &context.secret {
                        Some(required) => secrets_match(&secret, required),
                        // No secret configured: any non-empty secret is
                        // accepted, same as the node.
                        None => true,
                    };
                    let (response_tx, response_rx) = oneshot::channel();
                    let reply: &[u8] = if accepted {
                        authenticated = true;
                        auth_negotiated = true;
                        tagged = tagging;
                        retry_capable = retryable;
                        if tagging { b"OnT\n" } else { b"On\n" }
                    } else {
                        b"En\n"
                    };
                    let _ = response_tx.send(Ok(reply.to_vec()));
                    if fifo_tx.send(response_rx).await.is_err() {
                        break 'connection Ok(());
                    }
                    if !accepted {
                        break 'connection Ok(());
                    }
                }
                Ok(ParseOutcome::Ready(request, tag)) => {
                    if !authenticated {
                        let (response_tx, response_rx) = oneshot::channel();
                        let _ = response_tx.send(Err(Fatal));
                        let _ = fifo_tx.send(response_rx).await;
                        break 'connection Ok(());
                    }
                    // Dispatch inline (awaiting the ordered backend
                    // enqueues) before parsing the next request — see
                    // `dispatch_request` on why order matters here.
                    let response_rx =
                        dispatch_request(Arc::clone(&context), request, tag, retry_capable).await;
                    if fifo_tx.send(response_rx).await.is_err() {
                        break 'connection Ok(());
                    }
                }
                Err(_) => {
                    let (response_tx, response_rx) = oneshot::channel();
                    let _ = response_tx.send(Err(Fatal));
                    let _ = fifo_tx.send(response_rx).await;
                    break 'connection Ok(());
                }
            }
        }

        // Issue #124: a drain stops the intake — everything already
        // dispatched still flows back through the writer's FIFO below.
        if *drain.borrow() {
            break 'connection Ok(());
        }

        let mut chunk = [0u8; 4096];
        let read = tokio::select! {
            read = timeout(IDLE_TIMEOUT, read_half.read(&mut chunk)) => read,
            () = drained(&mut drain) => break 'connection Ok(()),
        };
        match read {
            Err(_) | Ok(Ok(0)) => break Ok(()),
            Ok(Ok(bytes_read)) => buf.extend_from_slice(&chunk[..bytes_read]),
            Ok(Err(error)) => break Err(error),
        }
    };

    // Closing the FIFO lets the writer drain what's pending, then stop.
    drop(fifo_tx);
    let _ = writer.await;
    result
}

// ─── main ────────────────────────────────────────────────────────────

fn main() -> ExitCode {
    let args = match parse_args_from(std::env::args().skip(1)) {
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

    rustls::crypto::ring::default_provider()
        .install_default()
        .expect("no other rustls crypto provider is installed this early in the process");

    let tls_acceptor = match (&args.tls_cert, &args.tls_key) {
        (Some(cert), Some(key)) => match load_tls_acceptor(cert, key) {
            Ok(acceptor) => Some(acceptor),
            Err(error) => {
                eprintln!("failed to load TLS certificate/key: {error}");
                return ExitCode::FAILURE;
            }
        },
        _ => None,
    };
    let tls_connector = match &args.tls_ca {
        Some(ca) => match load_tls_connector(ca) {
            Ok(connector) => Some(connector),
            Err(error) => {
                eprintln!("failed to load TLS CA: {error}");
                return ExitCode::FAILURE;
            }
        },
        None => None,
    };

    let runtime = match tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
    {
        Ok(runtime) => runtime,
        Err(error) => {
            eprintln!("failed to start the async runtime: {error}");
            return ExitCode::FAILURE;
        }
    };

    match runtime.block_on(run(args, tls_acceptor, tls_connector)) {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("fatal: {error}");
            ExitCode::FAILURE
        }
    }
}

async fn run(
    args: Args,
    tls_acceptor: Option<TlsAcceptor>,
    tls_connector: Option<TlsConnector>,
) -> io::Result<()> {
    let secret = read_auth_secret();

    let (ring_tx, ring_rx) = watch::channel(None);
    let (refresh_tx, refresh_rx) = mpsc::channel(16);
    let (drain_tx, drain_rx) = watch::channel(false);
    let identity = ProxyIdentity::generate();
    println!("INFO proxy identity: {}", identity.name);
    let backends = Arc::new(SharedBackends::new());
    let refresher = tokio::spawn(run_refresher(
        RefresherConfig {
            discovery: args.discovery.clone(),
            secret: secret.clone(),
            tls_connector: tls_connector.clone(),
            announce: Some((identity, args.port)),
            drain: drain_rx.clone(),
        },
        ring_tx,
        refresh_rx,
        Arc::clone(&backends),
    ));

    // Issue #124: SIGTERM/SIGINT begin the drain — the orchestrator's
    // ordinary stop signal is the graceful path.
    tokio::spawn(async move {
        let _ = shutdown_signal().await;
        println!("INFO drain: stop signal received — deregistering and finishing in-flight work");
        let _ = drain_tx.send(true);
    });

    let context = Arc::new(ProxyContext {
        secret,
        tls_connector,
        ring: ring_rx,
        refresh_now: refresh_tx,
        drain: drain_rx,
        backends,
        requests_total: std::sync::atomic::AtomicU64::new(0),
        upstream_failures_total: std::sync::atomic::AtomicU64::new(0),
    });

    let listener = TcpListener::bind((args.host.as_str(), args.port)).await?;
    let local = listener.local_addr()?;
    println!(
        "INFO nanocached-proxy listening on {local} (discovery: {})",
        args.discovery.join(",")
    );

    let permits = Arc::new(Semaphore::new(args.max_connections));

    // Issue #124: the operations sidecar — /metrics + /healthz + /readyz
    // on its own listener, mirroring the node's (see that binary's
    // `run_metrics_server` docs; independent re-implementation per the
    // no-shared-modules policy).
    if let Some(port) = args.metrics_port {
        let metrics_listener = TcpListener::bind((args.host.as_str(), port)).await?;
        println!("INFO metrics endpoint listening on {}:{port}", args.host);
        let metrics_permits = Arc::new(Semaphore::new(METRICS_MAX_CONNECTIONS));
        tokio::spawn(run_metrics_server(
            metrics_listener,
            Arc::clone(&context),
            Arc::clone(&permits),
            args.max_connections,
            metrics_permits,
        ));
    }

    serve(listener, context, tls_acceptor, permits, args.drain_timeout).await?;

    // The refresher exits after sending the deregistration; give it its
    // moment so `Z` reliably reaches discovery before the process ends.
    let _ = timeout(UPSTREAM_IO_TIMEOUT, refresher).await;
    println!("INFO drain complete");
    Ok(())
}

/// Resolves when the drain flag is (or becomes) `true`. A dropped
/// sender means "no drain will ever be signalled" (tests, or shutdown
/// paths that never arm it) — park forever rather than resolving, so a
/// `select!` arm built on this can never misread sender-drop as a
/// drain.
async fn drained(rx: &mut watch::Receiver<bool>) {
    loop {
        if *rx.borrow() {
            return;
        }
        if rx.changed().await.is_err() {
            std::future::pending::<()>().await;
        }
    }
}

/// SIGTERM or ctrl-c — mirrors the node's shutdown_signal.
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

/// Whether `error` (from a failed `listener.accept()`) looks like the
/// process (EMFILE) or the whole system (ENFILE) being out of file
/// descriptors — the two accept() failures where retrying immediately
/// would spin the accept loop hot instead of recovering (see
/// `ACCEPT_ERROR_BACKOFF`). EMFILE/ENFILE share the same numeric errno on
/// every Unix this project targets (Linux, macOS/BSD), so this hardcodes
/// them rather than pulling in a `libc` dependency for two integers.
/// Mirrors the node's and discovery's own copy of this check (no
/// shared-modules policy).
#[cfg(unix)]
fn is_fd_exhaustion_error(error: &io::Error) -> bool {
    const EMFILE: i32 = 24;
    const ENFILE: i32 = 23;
    matches!(error.raw_os_error(), Some(EMFILE) | Some(ENFILE))
}

#[cfg(not(unix))]
fn is_fd_exhaustion_error(_error: &io::Error) -> bool {
    false
}

/// Backoff after an accept() failure recognized by
/// `is_fd_exhaustion_error` — same value as the node's and discovery's.
const ACCEPT_ERROR_BACKOFF: Duration = Duration::from_millis(100);

/// Issue #124: minimal, dependency-free HTTP responder for Prometheus
/// text-format metrics and orchestrator probes. `/readyz` answers `503`
/// until the first roster fetch has landed — a proxy with no ring view
/// would answer clients `B`, so keep it out of rotation until then.
/// Unauthenticated by design (operational telemetry; keep the port
/// internal). Issue #233: `metrics_permits` bounds this listener's own
/// concurrent connections (`METRICS_MAX_CONNECTIONS`) — separate from
/// `permits`/`max_connections`, which this only reads for the client
/// connection gauge.
async fn run_metrics_server(
    listener: TcpListener,
    context: Arc<ProxyContext>,
    permits: Arc<Semaphore>,
    max_connections: usize,
    metrics_permits: Arc<Semaphore>,
) {
    loop {
        let stream = match listener.accept().await {
            Ok((stream, _)) => stream,
            Err(error) => {
                // Issue #184: an unadorned `continue` here would
                // busy-loop this task hot under EMFILE/ENFILE instead of
                // backing off, making recovery harder right when file
                // descriptors are already scarce.
                if is_fd_exhaustion_error(&error) {
                    sleep(ACCEPT_ERROR_BACKOFF).await;
                }
                continue;
            }
        };

        // Issue #233: this listener has no client-listener-style busy
        // reply — cap it with its own small semaphore instead, dropping
        // the connection outright once it's exhausted rather than
        // spawning an unbounded number of handler tasks.
        let Ok(metrics_permit) = Arc::clone(&metrics_permits).try_acquire_owned() else {
            drop(stream);
            continue;
        };

        let context = Arc::clone(&context);
        let permits = Arc::clone(&permits);
        tokio::spawn(async move {
            let _metrics_permit = metrics_permit;
            let _ = timeout(
                Duration::from_secs(5),
                serve_metrics_connection(stream, context, permits, max_connections),
            )
            .await;
        });
    }
}

async fn serve_metrics_connection(
    mut stream: TcpStream,
    context: Arc<ProxyContext>,
    permits: Arc<Semaphore>,
    max_connections: usize,
) -> io::Result<()> {
    let path = read_http_request_path(&mut stream).await?;

    let (status, body): (&str, String) = match path.as_str() {
        "/metrics" => {
            let client_connections = max_connections.saturating_sub(permits.available_permits());
            let backend_connections = context
                .backends
                .dialed
                .load(std::sync::atomic::Ordering::Relaxed);
            let requests = context
                .requests_total
                .load(std::sync::atomic::Ordering::Relaxed);
            let failures = context
                .upstream_failures_total
                .load(std::sync::atomic::Ordering::Relaxed);
            let body = format!(
                "# HELP nanocached_proxy_client_connections Client connections currently held.\n\
                 # TYPE nanocached_proxy_client_connections gauge\n\
                 nanocached_proxy_client_connections {client_connections}\n\
                 # HELP nanocached_proxy_client_connections_max The --max-connections bound.\n\
                 # TYPE nanocached_proxy_client_connections_max gauge\n\
                 nanocached_proxy_client_connections_max {max_connections}\n\
                 # HELP nanocached_proxy_backend_connections Live shared connections to nodes.\n\
                 # TYPE nanocached_proxy_backend_connections gauge\n\
                 nanocached_proxy_backend_connections {backend_connections}\n\
                 # HELP nanocached_proxy_requests_total Requests dispatched (all ops).\n\
                 # TYPE nanocached_proxy_requests_total counter\n\
                 nanocached_proxy_requests_total {requests}\n\
                 # HELP nanocached_proxy_upstream_failures_total Requests that failed upstream after retries (answered E).\n\
                 # TYPE nanocached_proxy_upstream_failures_total counter\n\
                 nanocached_proxy_upstream_failures_total {failures}\n"
            );
            ("200 OK", body)
        }
        "/healthz" => ("200 OK", "ok\n".to_string()),
        "/readyz" => {
            if *context.drain.borrow() {
                ("503 Service Unavailable", "draining\n".to_string())
            } else if context.ring.borrow().is_some() {
                ("200 OK", "ok\n".to_string())
            } else {
                ("503 Service Unavailable", "no roster yet\n".to_string())
            }
        }
        _ => ("404 Not Found", "not found\n".to_string()),
    };

    write_http_response(&mut stream, status, &body).await
}

/// Bounded read of one HTTP request head; GET path or error. Mirrors
/// the node's copy.
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

/// The accept loop, factored from `run` so tests can drive it against a
/// listener they bound themselves.
async fn serve(
    listener: TcpListener,
    context: Arc<ProxyContext>,
    tls_acceptor: Option<TlsAcceptor>,
    permits: Arc<Semaphore>,
    drain_timeout: Duration,
) -> io::Result<()> {
    let mut connections = tokio::task::JoinSet::new();
    let mut drain = context.drain.clone();

    loop {
        let accepted = tokio::select! {
            accepted = listener.accept() => accepted,
            // Issue #124: drain — stop accepting; the listener drops at
            // the end of this function, so new dials are refused and a
            // bootstrapping client moves on to another proxy.
            () = drained(&mut drain) => break,
        };
        let (stream, peer) = match accepted {
            Ok(accepted) => accepted,
            Err(error) => {
                // Issue #271: this used to be `return Err(error)`, tearing
                // down the whole proxy process on any accept() failure —
                // most of which (ECONNABORTED: the peer reset before the
                // handshake completed; EMFILE/ENFILE: transient resource
                // pressure) are recoverable and say nothing about this
                // listener's own health. Log and keep serving instead;
                // only a backoff (fd exhaustion specifically) changes the
                // loop's pace, never its continuation — matches the
                // metrics accept loop and discovery's accept loop.
                eprintln!("WARN accept failed: {error}");
                if is_fd_exhaustion_error(&error) {
                    sleep(ACCEPT_ERROR_BACKOFF).await;
                }
                continue;
            }
        };
        let _ = stream.set_nodelay(true);

        let Ok(permit) = Arc::clone(&permits).try_acquire_owned() else {
            // Over the connection budget: answer busy and move on, the
            // node's own stance (see `reject_over_limit`).
            let tls_configured = tls_acceptor.is_some();
            tokio::spawn(async move {
                let mut stream = stream;
                // Tenth-pass audit (2026-09-02): a TLS-configured proxy has
                // no plaintext channel to answer on before the handshake —
                // the peer is expecting a ServerHello, so a plaintext `B\n`
                // is meaningless (wrong-protocol) rather than merely
                // unread. Mirrors the node's own `reject_over_limit`, which
                // skips the busy write entirely once TLS is on and just
                // closes.
                if tls_configured {
                    return;
                }
                // Tenth-pass audit (2026-09-02): bounded like every other
                // client write — a peer with a zero receive window that
                // never reads this reply must not leak the spawned task
                // and socket forever.
                let _ = timeout(CLIENT_WRITE_TIMEOUT, stream.write_all(b"B\n")).await;
            });
            continue;
        };

        let context = Arc::clone(&context);
        let acceptor = tls_acceptor.clone();
        connections.spawn(async move {
            let _permit = permit;
            let stream: ServerStream = match acceptor {
                None => MaybeTls::Plain(stream),
                // Tenth-pass audit (2026-09-02): bounded by
                // `TLS_HANDSHAKE_TIMEOUT` — without this, a peer that opens
                // the TCP connection and never sends a ClientHello holds
                // `_permit` (already acquired, above) forever, and enough
                // such connections exhaust `max_connections` and starve
                // legitimate clients. Mirrors the node's own accept path.
                Some(acceptor) => {
                    match timeout(TLS_HANDSHAKE_TIMEOUT, acceptor.accept(stream)).await {
                        Ok(Ok(tls)) => MaybeTls::Tls(Box::new(tls)),
                        Ok(Err(error)) => {
                            eprintln!("WARN TLS handshake with {peer} failed: {error}");
                            return;
                        }
                        Err(_) => {
                            eprintln!("WARN TLS handshake with {peer} timed out");
                            return;
                        }
                    }
                }
            };
            if let Err(error) = handle_client(stream, context).await {
                eprintln!("WARN connection error from {peer}: {error}");
            }
        });

        // Reap finished connection tasks as we go so the set doesn't
        // grow with connection *history*.
        while connections.try_join_next().is_some() {}
    }

    // Issue #124: connection readers observe the same drain flag and
    // stop taking requests; their writers deliver everything already in
    // flight. Give them the drain window, then cut whatever remains —
    // the orchestrator's SIGKILL would arrive anyway.
    drop(listener);
    let deadline = tokio::time::Instant::now() + drain_timeout;
    while !connections.is_empty() {
        if tokio::time::timeout_at(deadline, connections.join_next())
            .await
            .is_err()
        {
            eprintln!(
                "WARN drain window elapsed with {} connection(s) still open; closing them",
                connections.len()
            );
            connections.abort_all();
            break;
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap as StdHashMap;
    use std::sync::Mutex as StdMutex;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

    #[cfg(unix)]
    #[test]
    fn fd_exhaustion_is_detected_for_emfile_and_enfile() {
        // Issue #184: the metrics accept loop used to `continue` on any
        // accept() error with no backoff, busy-looping under EMFILE/
        // ENFILE — matches the node's and discovery's own copy of this
        // check (`is_fd_exhaustion`/`is_fd_exhaustion_error`).
        assert!(is_fd_exhaustion_error(&io::Error::from_raw_os_error(24))); // EMFILE
        assert!(is_fd_exhaustion_error(&io::Error::from_raw_os_error(23))); // ENFILE
    }

    #[test]
    fn fd_exhaustion_is_not_reported_for_other_accept_errors() {
        // ECONNABORTED is a recoverable per-connection failure, not one
        // that means the process is out of descriptors — it shouldn't
        // trigger the backoff.
        assert!(!is_fd_exhaustion_error(&io::Error::from(
            io::ErrorKind::ConnectionAborted
        )));
    }

    #[test]
    fn auth_secret_comes_from_nanocached_auth_secret() {
        // Issue #165: the proxy used to read NANOCACHED_SECRET while node
        // and discovery read NANOCACHED_AUTH_SECRET.
        let env = |name: &str| match name {
            "NANOCACHED_AUTH_SECRET" => Some("shared".to_string()),
            _ => None,
        };
        assert_eq!(
            read_auth_secret_from(env),
            Some(Bytes::from_static(b"shared"))
        );
    }

    #[test]
    fn auth_secret_falls_back_to_the_legacy_name() {
        let env = |name: &str| match name {
            "NANOCACHED_SECRET" => Some("legacy".to_string()),
            _ => None,
        };
        assert_eq!(
            read_auth_secret_from(env),
            Some(Bytes::from_static(b"legacy"))
        );
    }

    #[test]
    fn auth_secret_prefers_the_new_name_and_ignores_empty_values() {
        let both = |name: &str| match name {
            "NANOCACHED_AUTH_SECRET" => Some("new".to_string()),
            "NANOCACHED_SECRET" => Some("legacy".to_string()),
            _ => None,
        };
        assert_eq!(
            read_auth_secret_from(both),
            Some(Bytes::from_static(b"new"))
        );
        let empty_new = |name: &str| match name {
            "NANOCACHED_AUTH_SECRET" => Some(String::new()),
            "NANOCACHED_SECRET" => Some("legacy".to_string()),
            _ => None,
        };
        assert_eq!(
            read_auth_secret_from(empty_new),
            Some(Bytes::from_static(b"legacy"))
        );
        assert_eq!(read_auth_secret_from(|_| Some(String::new())), None);
        assert_eq!(read_auth_secret_from(|_| None), None);
    }

    // ── request framing ──────────────────────────────────────────────

    #[test]
    fn substitute_tag_replaces_the_placeholder_and_leaves_the_body_untouched() {
        let frame: Bytes = Bytes::from_static(b"G 4 {tag}\nname");
        let (header, body) = substitute_tag(frame, 7);
        assert_eq!(&header[..], b"G 4 7\n");
        assert_eq!(&body[..], b"name");
    }

    #[test]
    fn substitute_tag_returns_the_body_as_a_zero_copy_slice_of_the_frame() {
        // Issue #335: `substitute_tag` used to `extend_from_slice` the
        // body into a freshly allocated buffer alongside the rebuilt
        // header — a full recopy of up to `MAX_VALUE_SIZE` on every
        // request, on top of the copy that already assembled the frame.
        // Proves the fix: the returned body is a `Bytes::slice` over the
        // very same backing allocation as the input frame, not a copy —
        // same underlying pointer, not merely equal bytes.
        let frame: Bytes = Bytes::from(b"S 3 5 {tag}\nkeyvalue".to_vec());
        let body_offset = frame.iter().position(|byte| *byte == b'\n').unwrap() + 1;
        let original_body_ptr = frame[body_offset..].as_ptr();

        let (_, body) = substitute_tag(frame, 3);

        assert_eq!(
            body.as_ptr(),
            original_body_ptr,
            "the body must be sliced from the original frame, not recopied"
        );
        assert_eq!(&body[..], b"keyvalue");
    }

    #[test]
    fn parse_request_trickled_multi_field_header_matches_one_shot_parse() {
        // A multi-field header (key-len, value-len, ttl, tag) trickled in
        // one byte at a time, then a body trickled the same way —
        // regression coverage for caching the header's `\n` scan
        // (`RequestScanState`): before that, every call re-scanned the
        // whole buffered prefix for `\n`.
        let frame = b"S 4 5 10 9\nnameAlice";
        let mut expected_input = BytesMut::from(&frame[..]);
        let expected =
            parse_request(&mut expected_input, true, &mut RequestScanState::default()).unwrap();

        let mut input = BytesMut::new();
        let mut scan = RequestScanState::default();
        let mut parsed = None;

        for (index, byte) in frame.iter().enumerate() {
            input.extend_from_slice(&[*byte]);
            match parse_request(&mut input, true, &mut scan) {
                Ok(ParseOutcome::Incomplete) => {}
                Ok(result) => {
                    parsed = Some(result);
                    break;
                }
                other => panic!("unexpected {other:?} at byte {index}"),
            }
        }

        assert_eq!(parsed, Some(expected));
        assert!(input.is_empty());
    }

    #[test]
    fn parse_request_header_scan_never_rescans_already_buffered_bytes() {
        // Same trickle as above, but asserting the *cost* rather than
        // just the outcome: the header `\n` scan's total bytes-examined
        // count (a `#[cfg(test)]`-only instrument, since wall-clock
        // timing assertions are flaky under CI load) must grow by
        // exactly one per newly buffered header byte and then stop
        // growing once the header is found — never by the whole buffered
        // prefix on every call, which is what made this quadratic in the
        // header length.
        reset_lf_scan_counter();

        let frame = b"S 4 5 10 9\nnameAlice";
        let header_len = 11; // `"S 4 5 10 9\n"`, `\n` included

        let mut input = BytesMut::new();
        let mut scan = RequestScanState::default();
        let mut parsed = None;

        for (index, byte) in frame.iter().enumerate() {
            input.extend_from_slice(&[*byte]);
            match parse_request(&mut input, true, &mut scan) {
                Ok(ParseOutcome::Incomplete) => {
                    assert_eq!(
                        lf_scan_bytes(),
                        (index + 1).min(header_len),
                        "at byte {index}: the header scan should examine only \
                         newly buffered bytes, and none at all once the \
                         header's `\\n` is already cached"
                    );
                }
                Ok(result) => {
                    parsed = Some(result);
                    break;
                }
                other => panic!("unexpected {other:?} at byte {index}"),
            }
        }

        assert!(parsed.is_some());
        assert_eq!(
            lf_scan_bytes(),
            header_len,
            "the body's bytes should never reach the header scan"
        );
    }

    // ── arg parsing ──────────────────────────────────────────────────

    fn args(list: &[&str]) -> Result<Args, ArgsError> {
        parse_args_from(list.iter().map(|s| s.to_string()))
    }

    #[test]
    fn parses_the_full_flag_set() {
        let parsed = args(&[
            "--host",
            "0.0.0.0",
            "--port",
            "9999",
            "--discovery",
            "10.0.0.1:8357, 10.0.0.2:8357",
            "--max-connections",
            "9",
        ])
        .ok()
        .unwrap();
        assert_eq!(parsed.host, "0.0.0.0");
        assert_eq!(parsed.port, 9999);
        assert_eq!(
            parsed.discovery,
            vec!["10.0.0.1:8357".to_string(), "10.0.0.2:8357".to_string()]
        );
        assert_eq!(parsed.max_connections, 9);
    }

    #[test]
    fn requires_discovery_and_paired_tls_flags() {
        assert!(matches!(args(&[]), Err(ArgsError::Invalid(_))));
        assert!(matches!(
            args(&["--discovery", "d:1", "--tls-cert", "x.pem"]),
            Err(ArgsError::Invalid(_))
        ));
        assert!(matches!(
            args(&["--discovery", "d:1", "--max-connections", "0"]),
            Err(ArgsError::Invalid(_))
        ));
    }

    // ── ring: the cross-implementation vectors ───────────────────────

    #[test]
    fn matches_the_cross_implementation_score_vectors() {
        let ring = RingView::new(
            ["node-a", "node-b", "node-c"]
                .iter()
                .map(|name| (name.to_string(), format!("addr-{name}")))
                .collect(),
            3,
        );
        let names = |owners: Vec<String>| -> Vec<String> {
            owners
                .into_iter()
                .map(|addr| addr.trim_start_matches("addr-").to_string())
                .collect()
        };

        assert_eq!(
            names(ring.owners(b"", b"alpha")),
            vec!["node-c", "node-b", "node-a"]
        );
        assert_eq!(
            names(ring.owners(b"", b"beta")),
            vec!["node-a", "node-c", "node-b"]
        );
        assert_eq!(key_hash(b"users", b"alpha"), 0xfd4ab55027c21df6);
        assert_eq!(key_hash(b"users", b""), 0xa9e9bbca44bb502e);
        assert_eq!(key_hash(b"\xff\x00", b"beta"), 0x8f7c097eccb8e792);
        assert_eq!(
            names(ring.owners(b"users", b"alpha")),
            vec!["node-a", "node-c", "node-b"]
        );
    }

    // ── mock cluster ─────────────────────────────────────────────────

    type Store = Arc<StdMutex<StdHashMap<(Vec<u8>, Vec<u8>), Vec<u8>>>>;

    /// A mock node speaking the tagged-mode slice of the protocol the
    /// proxy produces, with knobs for W and slowness.
    struct MockNode {
        addr: String,
        store: Store,
        cleared: Arc<AtomicUsize>,
        flushed: Arc<AtomicUsize>,
        wrong_node_once: Arc<AtomicBool>,
        /// Issue #110: drop the connection instead of answering the next
        /// request — simulates a node-side close (idle timeout, crash)
        /// on the shared connection.
        close_once: Arc<AtomicBool>,
        /// Issue #272: applies the next `i` (INCR) request to the store,
        /// then drops the connection instead of sending the `I` reply —
        /// simulates the delta having landed at the primary while the
        /// reply itself was lost (as opposed to `close_once`, which drops
        /// before ever touching the store). Proves a retry after this
        /// must not resend `i` and double-apply the delta.
        close_after_incr_apply_once: Arc<AtomicBool>,
        /// Issue #293: same idea as `close_after_incr_apply_once`, but
        /// for the next `k` (CAS set) request — applies it to the store,
        /// then drops the connection instead of sending the `S`/`N`
        /// reply. Proves a retry after this must not resend `k` and
        /// re-evaluate (and possibly re-satisfy) the condition.
        close_after_cas_set_apply_once: Arc<AtomicBool>,
        /// Issue #293: same idea, for the next `x` (CAS delete) request.
        close_after_cas_delete_apply_once: Arc<AtomicBool>,
        get_delay: Arc<StdMutex<Duration>>,
        auth_count: Arc<AtomicUsize>,
        /// Issue #110: set when a request was already buffered before
        /// the previous one was answered — only a genuinely pipelined
        /// sender (not #109's serial write-then-await loop) produces it.
        saw_pipelined: Arc<AtomicBool>,
        /// Issue #129: how many `i` frames this node actually received —
        /// proves a replica never gets the increment replayed on it (see
        /// `an_incr_results_fan_out_never_replays_the_increment_on_a_replica`),
        /// only the primary does.
        incrs: Arc<AtomicUsize>,
        /// Issue #141: how many `k`/`x` frames this node actually
        /// received — same "only the primary does" proof as `incrs`, for
        /// compare-and-set (see
        /// `a_cas_results_fan_out_never_replays_the_operation_on_a_replica`).
        cas_sets: Arc<AtomicUsize>,
        cas_deletes: Arc<AtomicUsize>,
    }

    impl MockNode {
        async fn start() -> MockNode {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let addr = listener.local_addr().unwrap().to_string();
            let node = MockNode {
                addr,
                store: Arc::new(StdMutex::new(StdHashMap::new())),
                cleared: Arc::new(AtomicUsize::new(0)),
                flushed: Arc::new(AtomicUsize::new(0)),
                wrong_node_once: Arc::new(AtomicBool::new(false)),
                close_once: Arc::new(AtomicBool::new(false)),
                close_after_incr_apply_once: Arc::new(AtomicBool::new(false)),
                close_after_cas_set_apply_once: Arc::new(AtomicBool::new(false)),
                close_after_cas_delete_apply_once: Arc::new(AtomicBool::new(false)),
                get_delay: Arc::new(StdMutex::new(Duration::ZERO)),
                auth_count: Arc::new(AtomicUsize::new(0)),
                saw_pipelined: Arc::new(AtomicBool::new(false)),
                incrs: Arc::new(AtomicUsize::new(0)),
                cas_sets: Arc::new(AtomicUsize::new(0)),
                cas_deletes: Arc::new(AtomicUsize::new(0)),
            };
            let store = Arc::clone(&node.store);
            let cleared = Arc::clone(&node.cleared);
            let flushed = Arc::clone(&node.flushed);
            let wrong_once = Arc::clone(&node.wrong_node_once);
            let close_once = Arc::clone(&node.close_once);
            let close_after_incr_apply_once = Arc::clone(&node.close_after_incr_apply_once);
            let close_after_cas_set_apply_once = Arc::clone(&node.close_after_cas_set_apply_once);
            let close_after_cas_delete_apply_once =
                Arc::clone(&node.close_after_cas_delete_apply_once);
            let delay = Arc::clone(&node.get_delay);
            let auth_count = Arc::clone(&node.auth_count);
            let saw_pipelined = Arc::clone(&node.saw_pipelined);
            let incrs = Arc::clone(&node.incrs);
            let cas_sets = Arc::clone(&node.cas_sets);
            let cas_deletes = Arc::clone(&node.cas_deletes);
            tokio::spawn(async move {
                loop {
                    let Ok((stream, _)) = listener.accept().await else {
                        return;
                    };
                    tokio::spawn(serve_mock_node(
                        stream,
                        MockNodeState {
                            store: Arc::clone(&store),
                            cleared: Arc::clone(&cleared),
                            flushed: Arc::clone(&flushed),
                            wrong_once: Arc::clone(&wrong_once),
                            close_once: Arc::clone(&close_once),
                            close_after_incr_apply_once: Arc::clone(&close_after_incr_apply_once),
                            close_after_cas_set_apply_once: Arc::clone(
                                &close_after_cas_set_apply_once,
                            ),
                            close_after_cas_delete_apply_once: Arc::clone(
                                &close_after_cas_delete_apply_once,
                            ),
                            delay: Arc::clone(&delay),
                            auth_count: Arc::clone(&auth_count),
                            saw_pipelined: Arc::clone(&saw_pipelined),
                            incrs: Arc::clone(&incrs),
                            cas_sets: Arc::clone(&cas_sets),
                            cas_deletes: Arc::clone(&cas_deletes),
                        },
                    ));
                }
            });
            node
        }

        fn incrs(&self) -> usize {
            self.incrs.load(Ordering::SeqCst)
        }

        fn cas_sets(&self) -> usize {
            self.cas_sets.load(Ordering::SeqCst)
        }

        fn cas_deletes(&self) -> usize {
            self.cas_deletes.load(Ordering::SeqCst)
        }

        fn entry(&self, namespace: &[u8], key: &[u8]) -> Option<Vec<u8>> {
            self.store
                .lock()
                .unwrap()
                .get(&(namespace.to_vec(), key.to_vec()))
                .cloned()
        }

        fn len(&self) -> usize {
            self.store.lock().unwrap().len()
        }
    }

    struct MockNodeState {
        store: Store,
        cleared: Arc<AtomicUsize>,
        flushed: Arc<AtomicUsize>,
        wrong_once: Arc<AtomicBool>,
        close_once: Arc<AtomicBool>,
        close_after_incr_apply_once: Arc<AtomicBool>,
        close_after_cas_set_apply_once: Arc<AtomicBool>,
        close_after_cas_delete_apply_once: Arc<AtomicBool>,
        delay: Arc<StdMutex<Duration>>,
        auth_count: Arc<AtomicUsize>,
        saw_pipelined: Arc<AtomicBool>,
        incrs: Arc<AtomicUsize>,
        cas_sets: Arc<AtomicUsize>,
        cas_deletes: Arc<AtomicUsize>,
    }

    /// Issue #141: same content-digest algorithm as the real node's
    /// `crate::cache::content_digest` — SHA-256 truncated to the first 16
    /// bytes, lowercase hex. An independent reimplementation for this
    /// mock, same "no shared modules" policy as the rest of this binary.
    fn mock_content_digest(value: &[u8]) -> String {
        use sha2::{Digest, Sha256};
        let hash = Sha256::digest(value);
        hash[..16]
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect()
    }

    async fn serve_mock_node(mut stream: TcpStream, state: MockNodeState) {
        let MockNodeState {
            store,
            cleared,
            flushed,
            wrong_once,
            close_once,
            close_after_incr_apply_once,
            close_after_cas_set_apply_once,
            close_after_cas_delete_apply_once,
            delay,
            auth_count,
            saw_pipelined,
            incrs,
            cas_sets,
            cas_deletes,
        } = state;
        let mut buf = BytesMut::new();
        let result: io::Result<()> = async {
            loop {
                let line = read_line(&mut stream, &mut buf).await?;
                let mut parts = line.split(' ');
                let command = parts.next().unwrap_or_default().to_string();
                if command != "A" && close_once.swap(false, Ordering::SeqCst) {
                    // Simulated node-side drop: no reply, connection gone.
                    return Ok(());
                }
                let fields: Vec<String> = parts.map(str::to_string).collect();
                // The proxy always negotiates tagged mode: every
                // non-A request's last field is the tag.
                let tag = |fields: &[String]| fields.last().cloned().unwrap_or_default();

                match command.as_str() {
                    "A" => {
                        let length: usize = fields[0].parse().unwrap();
                        read_exact_into(&mut stream, &mut buf, length).await?;
                        let _ = buf.split_to(length);
                        auth_count.fetch_add(1, Ordering::SeqCst);
                        stream.write_all(b"OnT\n").await?;
                    }
                    "G" | "g" => {
                        let (namespace, key) = if command == "g" {
                            let ns_length: usize = fields[0].parse().unwrap();
                            let key_length: usize = fields[1].parse().unwrap();
                            read_exact_into(&mut stream, &mut buf, ns_length + key_length).await?;
                            let body = buf.split_to(ns_length + key_length);
                            (body[..ns_length].to_vec(), body[ns_length..].to_vec())
                        } else {
                            let key_length: usize = fields[0].parse().unwrap();
                            read_exact_into(&mut stream, &mut buf, key_length).await?;
                            (Vec::new(), buf.split_to(key_length).to_vec())
                        };
                        let pause = *delay.lock().unwrap();
                        if !pause.is_zero() {
                            sleep(pause).await;
                        }
                        // Issue #110: bytes already buffered while this
                        // request was still unanswered = the sender
                        // pipelines rather than awaiting each reply.
                        if !buf.is_empty() {
                            saw_pipelined.store(true, Ordering::SeqCst);
                        }
                        if wrong_once.swap(false, Ordering::SeqCst) {
                            stream
                                .write_all(format!("W {}\n", tag(&fields)).as_bytes())
                                .await?;
                            continue;
                        }
                        let stored = store.lock().unwrap().get(&(namespace, key)).cloned();
                        match stored {
                            Some(value) => {
                                stream
                                    .write_all(
                                        format!("V {} {}\n", value.len(), tag(&fields)).as_bytes(),
                                    )
                                    .await?;
                                stream.write_all(&value).await?;
                            }
                            None => {
                                stream
                                    .write_all(format!("N {}\n", tag(&fields)).as_bytes())
                                    .await?;
                            }
                        }
                    }
                    // Issue #128 measurement prototype: mirrors the real
                    // node's `m`/`M` — see `Command::MultiGet` (src/
                    // command.rs) for the frame grammar this reimplements.
                    "m" => {
                        let ns_length: usize = fields[0].parse().unwrap();
                        let count: usize = fields[1].parse().unwrap();
                        let key_lengths: Vec<usize> = fields[2..2 + count]
                            .iter()
                            .map(|field| field.parse().unwrap())
                            .collect();
                        let total: usize = ns_length + key_lengths.iter().sum::<usize>();
                        read_exact_into(&mut stream, &mut buf, total).await?;
                        let body = buf.split_to(total);
                        let namespace = body[..ns_length].to_vec();
                        let mut cursor = ns_length;
                        let keys: Vec<Vec<u8>> = key_lengths
                            .iter()
                            .map(|&length| {
                                let key = body[cursor..cursor + length].to_vec();
                                cursor += length;
                                key
                            })
                            .collect();

                        let values: Vec<Option<Vec<u8>>> = {
                            let store = store.lock().unwrap();
                            keys.iter()
                                .map(|key| store.get(&(namespace.clone(), key.clone())).cloned())
                                .collect()
                        };

                        // Issue #150: simulates the real node's own
                        // per-key wrong-node check catching a stale
                        // slice of an otherwise-valid roster — the
                        // proxy's `retry_multi_get` path only ever sees
                        // this shape (a `W` token inside a well-formed
                        // `M` roster), never a whole-frame `W`.
                        let first_wrong = wrong_once.swap(false, Ordering::SeqCst);

                        let mut header = format!("M {count}");
                        for (index, value) in values.iter().enumerate() {
                            if first_wrong && index == 0 {
                                header.push_str(" W");
                                continue;
                            }
                            match value {
                                Some(value) => header.push_str(&format!(" {}", value.len())),
                                None => header.push_str(" -"),
                            }
                        }
                        header.push_str(&format!(" {}\n", tag(&fields)));
                        stream.write_all(header.as_bytes()).await?;
                        for (index, value) in values.into_iter().enumerate() {
                            if first_wrong && index == 0 {
                                continue;
                            }
                            if let Some(value) = value {
                                stream.write_all(&value).await?;
                            }
                        }
                    }
                    // Issue #150: mirrors the real node's own `o`/`O` —
                    // see `Command::MultiSet` (src/command.rs). No TTL
                    // fidelity, same as this mock's `S`/`s` arm.
                    "o" => {
                        let ns_length: usize = fields[0].parse().unwrap();
                        let count: usize = fields[1].parse().unwrap();
                        let mut pairs = Vec::with_capacity(count);
                        for pair in 0..count {
                            let key_length: usize = fields[2 + pair * 2].parse().unwrap();
                            let value_length: usize = fields[3 + pair * 2].parse().unwrap();
                            pairs.push((key_length, value_length));
                        }
                        let total: usize =
                            ns_length + pairs.iter().map(|(k, v)| k + v).sum::<usize>();
                        read_exact_into(&mut stream, &mut buf, total).await?;
                        let body = buf.split_to(total);
                        let namespace = body[..ns_length].to_vec();
                        let mut cursor = ns_length;
                        let mut entries = Vec::with_capacity(count);
                        for (key_length, value_length) in pairs {
                            let key = body[cursor..cursor + key_length].to_vec();
                            cursor += key_length;
                            let value = body[cursor..cursor + value_length].to_vec();
                            cursor += value_length;
                            entries.push((key, value));
                        }

                        // Same per-key wrong-node simulation as the "m"
                        // arm above.
                        let first_wrong = wrong_once.swap(false, Ordering::SeqCst);

                        let mut header = format!("O {count}");
                        for (index, (key, value)) in entries.into_iter().enumerate() {
                            if first_wrong && index == 0 {
                                header.push_str(" W");
                                continue;
                            }
                            store
                                .lock()
                                .unwrap()
                                .insert((namespace.clone(), key), value);
                            header.push_str(" S");
                        }
                        header.push_str(&format!(" {}\n", tag(&fields)));
                        stream.write_all(header.as_bytes()).await?;
                    }
                    "S" | "s" => {
                        let offset = if command == "s" { 1 } else { 0 };
                        let ns_length: usize = if command == "s" {
                            fields[0].parse().unwrap()
                        } else {
                            0
                        };
                        let key_length: usize = fields[offset].parse().unwrap();
                        let value_length: usize = fields[offset + 1].parse().unwrap();
                        let total = ns_length + key_length + value_length;
                        read_exact_into(&mut stream, &mut buf, total).await?;
                        let body = buf.split_to(total);
                        let namespace = body[..ns_length].to_vec();
                        let key = body[ns_length..ns_length + key_length].to_vec();
                        let value = body[ns_length + key_length..].to_vec();
                        if wrong_once.swap(false, Ordering::SeqCst) {
                            stream
                                .write_all(format!("W {}\n", tag(&fields)).as_bytes())
                                .await?;
                            continue;
                        }
                        store.lock().unwrap().insert((namespace, key), value);
                        stream
                            .write_all(format!("S {}\n", tag(&fields)).as_bytes())
                            .await?;
                    }
                    // Issue #129: no TTL fidelity, same as this mock's
                    // `S`/`s` arm above (which already ignores any TTL
                    // field) — the mock store has no TTL concept at all.
                    "i" => {
                        let ns_length: usize = fields[0].parse().unwrap();
                        let key_length: usize = fields[1].parse().unwrap();
                        let delta: i64 = fields[2].parse().unwrap();
                        read_exact_into(&mut stream, &mut buf, ns_length + key_length).await?;
                        let body = buf.split_to(ns_length + key_length);
                        let namespace = body[..ns_length].to_vec();
                        let key = body[ns_length..].to_vec();
                        if wrong_once.swap(false, Ordering::SeqCst) {
                            stream
                                .write_all(format!("W {}\n", tag(&fields)).as_bytes())
                                .await?;
                            continue;
                        }
                        incrs.fetch_add(1, Ordering::SeqCst);
                        let current = store
                            .lock()
                            .unwrap()
                            .get(&(namespace.clone(), key.clone()))
                            .cloned();
                        match current {
                            None => {
                                stream
                                    .write_all(format!("N {}\n", tag(&fields)).as_bytes())
                                    .await?;
                            }
                            Some(current) => {
                                let parsed = std::str::from_utf8(&current)
                                    .ok()
                                    .and_then(|text| text.parse::<i64>().ok());
                                match parsed {
                                    None => {
                                        stream
                                            .write_all(format!("T {}\n", tag(&fields)).as_bytes())
                                            .await?;
                                    }
                                    Some(current_value) => {
                                        let new_bytes =
                                            (current_value + delta).to_string().into_bytes();
                                        store
                                            .lock()
                                            .unwrap()
                                            .insert((namespace, key), new_bytes.clone());
                                        // Issue #272: the delta is already
                                        // applied above — closing here
                                        // instead of replying simulates the
                                        // reply being lost after the node
                                        // executed the increment, proving a
                                        // retry must not resend `i`.
                                        if close_after_incr_apply_once.swap(false, Ordering::SeqCst)
                                        {
                                            return Ok(());
                                        }
                                        stream
                                            .write_all(
                                                format!("I {} {}\n", new_bytes.len(), tag(&fields))
                                                    .as_bytes(),
                                            )
                                            .await?;
                                        stream.write_all(&new_bytes).await?;
                                    }
                                }
                            }
                        }
                    }
                    // Issue #141: no TTL fidelity, same as `S`/`s`/`i`
                    // above. `<cond>` is always `fields[3]` regardless of
                    // whether the optional `<ttl>` field follows it — the
                    // proxy always tags, so `fields.last()` is the tag
                    // either way.
                    "k" => {
                        let ns_length: usize = fields[0].parse().unwrap();
                        let key_length: usize = fields[1].parse().unwrap();
                        let value_length: usize = fields[2].parse().unwrap();
                        let cond = fields[3].clone();
                        read_exact_into(
                            &mut stream,
                            &mut buf,
                            ns_length + key_length + value_length,
                        )
                        .await?;
                        let body = buf.split_to(ns_length + key_length + value_length);
                        let namespace = body[..ns_length].to_vec();
                        let key = body[ns_length..ns_length + key_length].to_vec();
                        let value = body[ns_length + key_length..].to_vec();
                        if wrong_once.swap(false, Ordering::SeqCst) {
                            stream
                                .write_all(format!("W {}\n", tag(&fields)).as_bytes())
                                .await?;
                            continue;
                        }
                        cas_sets.fetch_add(1, Ordering::SeqCst);
                        let current = store
                            .lock()
                            .unwrap()
                            .get(&(namespace.clone(), key.clone()))
                            .cloned();
                        let condition_holds = match cond.as_str() {
                            "A" => current.is_none(),
                            "P" => current.is_some(),
                            digest => current
                                .as_deref()
                                .is_some_and(|value| mock_content_digest(value) == digest),
                        };
                        if condition_holds {
                            store.lock().unwrap().insert((namespace, key), value);
                            // Issue #293: the store is already updated
                            // above — closing here instead of replying
                            // simulates the reply being lost after the
                            // node executed the CAS, proving a retry
                            // must not resend `k`.
                            if close_after_cas_set_apply_once.swap(false, Ordering::SeqCst) {
                                return Ok(());
                            }
                            stream
                                .write_all(format!("S {}\n", tag(&fields)).as_bytes())
                                .await?;
                        } else {
                            stream
                                .write_all(format!("N {}\n", tag(&fields)).as_bytes())
                                .await?;
                        }
                    }
                    "x" => {
                        let ns_length: usize = fields[0].parse().unwrap();
                        let key_length: usize = fields[1].parse().unwrap();
                        let cond = fields[2].clone();
                        read_exact_into(&mut stream, &mut buf, ns_length + key_length).await?;
                        let body = buf.split_to(ns_length + key_length);
                        let namespace = body[..ns_length].to_vec();
                        let key = body[ns_length..].to_vec();
                        if wrong_once.swap(false, Ordering::SeqCst) {
                            stream
                                .write_all(format!("W {}\n", tag(&fields)).as_bytes())
                                .await?;
                            continue;
                        }
                        cas_deletes.fetch_add(1, Ordering::SeqCst);
                        let current = store
                            .lock()
                            .unwrap()
                            .get(&(namespace.clone(), key.clone()))
                            .cloned();
                        let condition_holds = current
                            .as_deref()
                            .is_some_and(|value| mock_content_digest(value) == cond);
                        if condition_holds {
                            store.lock().unwrap().remove(&(namespace, key));
                            // Issue #293: same "applied, then reply
                            // lost" simulation as the `k` arm above, for
                            // `x`.
                            if close_after_cas_delete_apply_once.swap(false, Ordering::SeqCst) {
                                return Ok(());
                            }
                            stream
                                .write_all(format!("D {}\n", tag(&fields)).as_bytes())
                                .await?;
                        } else {
                            stream
                                .write_all(format!("N {}\n", tag(&fields)).as_bytes())
                                .await?;
                        }
                    }
                    "D" | "d" => {
                        let (namespace, key) = if command == "d" {
                            let ns_length: usize = fields[0].parse().unwrap();
                            let key_length: usize = fields[1].parse().unwrap();
                            read_exact_into(&mut stream, &mut buf, ns_length + key_length).await?;
                            let body = buf.split_to(ns_length + key_length);
                            (body[..ns_length].to_vec(), body[ns_length..].to_vec())
                        } else {
                            let key_length: usize = fields[0].parse().unwrap();
                            read_exact_into(&mut stream, &mut buf, key_length).await?;
                            (Vec::new(), buf.split_to(key_length).to_vec())
                        };
                        let existed = store.lock().unwrap().remove(&(namespace, key)).is_some();
                        let marker = if existed { "D" } else { "N" };
                        stream
                            .write_all(format!("{marker} {}\n", tag(&fields)).as_bytes())
                            .await?;
                    }
                    "c" => {
                        let ns_length: usize = fields[0].parse().unwrap();
                        read_exact_into(&mut stream, &mut buf, ns_length).await?;
                        let namespace = buf.split_to(ns_length).to_vec();
                        store
                            .lock()
                            .unwrap()
                            .retain(|(entry_ns, _), _| *entry_ns != namespace);
                        cleared.fetch_add(1, Ordering::SeqCst);
                        stream
                            .write_all(format!("C {}\n", tag(&fields)).as_bytes())
                            .await?;
                    }
                    "F" => {
                        store.lock().unwrap().clear();
                        flushed.fetch_add(1, Ordering::SeqCst);
                        stream
                            .write_all(format!("C {}\n", tag(&fields)).as_bytes())
                            .await?;
                    }
                    other => panic!("mock node got unexpected command {other:?}"),
                }
            }
        }
        .await;
        let _ = result;
    }

    /// A mock discovery answering `L` with a fixed roster, accepting
    /// `Y` announces, and recording `Z` deregistrations (issue #124).
    async fn start_mock_discovery_recording(
        roster: Vec<(String, String)>,
        replication: usize,
    ) -> (String, Arc<StdMutex<Vec<String>>>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        let deregistered: Arc<StdMutex<Vec<String>>> = Arc::new(StdMutex::new(Vec::new()));
        let record = Arc::clone(&deregistered);
        tokio::spawn(async move {
            loop {
                let Ok((mut stream, _)) = listener.accept().await else {
                    return;
                };
                let roster = roster.clone();
                let record = Arc::clone(&record);
                tokio::spawn(async move {
                    let mut buf = BytesMut::new();
                    loop {
                        let Ok(line) = read_line(&mut stream, &mut buf).await else {
                            return;
                        };
                        let mut parts = line.split(' ');
                        let command = parts.next().unwrap_or_default().to_string();
                        let lengths: Vec<usize> = parts
                            .map(|field| field.parse().unwrap_or_default())
                            .collect();
                        match command.as_str() {
                            "A" => {
                                if read_exact_into(&mut stream, &mut buf, lengths[0])
                                    .await
                                    .is_err()
                                {
                                    return;
                                }
                                let _ = buf.split_to(lengths[0]);
                                let _ = stream.write_all(b"Od\n").await;
                            }
                            "L" => {
                                let mut response =
                                    format!("N {} {replication}\n", roster.len()).into_bytes();
                                for (name, addr) in &roster {
                                    response.extend_from_slice(
                                        format!("{} {}\n{name}{addr}\n", name.len(), addr.len())
                                            .as_bytes(),
                                    );
                                }
                                if stream.write_all(&response).await.is_err() {
                                    return;
                                }
                            }
                            // `Y <name-len> <port> <token-len>`: consume
                            // name+token, ack.
                            "Y" => {
                                let body = lengths[0] + lengths[2];
                                if read_exact_into(&mut stream, &mut buf, body).await.is_err() {
                                    return;
                                }
                                let _ = buf.split_to(body);
                                let _ = stream.write_all(b"R\n").await;
                            }
                            // `Z <name-len> <token-len>`: record the name.
                            "Z" => {
                                let body = lengths[0] + lengths[1];
                                if read_exact_into(&mut stream, &mut buf, body).await.is_err() {
                                    return;
                                }
                                let body = buf.split_to(body);
                                record.lock().unwrap().push(
                                    String::from_utf8_lossy(&body[..lengths[0]]).into_owned(),
                                );
                                let _ = stream.write_all(b"R\n").await;
                            }
                            other => panic!("mock discovery got {other:?}"),
                        }
                    }
                });
            }
        });
        (addr, deregistered)
    }

    async fn start_mock_discovery(roster: Vec<(String, String)>, replication: usize) -> String {
        start_mock_discovery_recording(roster, replication).await.0
    }

    /// Boots a proxy over the given roster; returns its address and the
    /// drain trigger (issue #124 tests).
    async fn start_proxy_with_drain(
        discovery_addr: &str,
        secret: Option<&str>,
        max_connections: usize,
        announce: Option<ProxyIdentity>,
    ) -> (String, watch::Sender<bool>, Arc<ProxyContext>) {
        let (ring_tx, ring_rx) = watch::channel(None);
        let (refresh_tx, refresh_rx) = mpsc::channel(16);
        let (drain_tx, drain_rx) = watch::channel(false);
        let secret = secret.map(|secret| Bytes::from(secret.to_string()));
        let backends = Arc::new(SharedBackends::new());
        tokio::spawn(run_refresher(
            RefresherConfig {
                discovery: vec![discovery_addr.to_string()],
                secret: secret.clone(),
                tls_connector: None,
                announce: announce.map(|identity| (identity, 0)),
                drain: drain_rx.clone(),
            },
            ring_tx,
            refresh_rx,
            Arc::clone(&backends),
        ));
        let context = Arc::new(ProxyContext {
            secret,
            tls_connector: None,
            ring: ring_rx.clone(),
            refresh_now: refresh_tx,
            drain: drain_rx,
            backends,
            requests_total: std::sync::atomic::AtomicU64::new(0),
            upstream_failures_total: std::sync::atomic::AtomicU64::new(0),
        });
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        tokio::spawn(serve(
            listener,
            Arc::clone(&context),
            None,
            Arc::new(Semaphore::new(max_connections)),
            Duration::from_secs(5),
        ));

        // Wait until the first roster fetch landed, so tests don't race
        // the refresher and see `B`.
        let mut ring = ring_rx;
        while ring.borrow().is_none() {
            ring.changed().await.unwrap();
        }
        (addr, drain_tx, context)
    }

    async fn start_proxy(
        discovery_addr: &str,
        secret: Option<&str>,
        max_connections: usize,
    ) -> String {
        start_proxy_with_drain(discovery_addr, secret, max_connections, None)
            .await
            .0
    }

    /// Installs rustls's default crypto provider if nothing else has yet;
    /// safe to call from multiple tests since a second, redundant install
    /// is just ignored rather than treated as an error. Mirrors the
    /// node's own `ensure_crypto_provider` test helper.
    fn ensure_crypto_provider() {
        let _ = rustls::crypto::ring::default_provider().install_default();
    }

    /// A self-signed cert/key pair valid for both "localhost" and
    /// "127.0.0.1", plus a matching acceptor/connector pair that trusts
    /// only that cert, for exercising the proxy's TLS accept path in
    /// tests without touching the filesystem. Mirrors the node's own
    /// `self_signed_tls` test helper.
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

    /// Tenth-pass audit (2026-09-02): like `start_proxy_with_drain`, but
    /// with a TLS acceptor installed on the client listener, for
    /// exercising `TLS_HANDSHAKE_TIMEOUT` and the busy-reply TLS-skip
    /// behavior.
    async fn start_tls_proxy(
        discovery_addr: &str,
        max_connections: usize,
        tls_acceptor: TlsAcceptor,
    ) -> String {
        let (ring_tx, ring_rx) = watch::channel(None);
        let (refresh_tx, refresh_rx) = mpsc::channel(16);
        let (_drain_tx, drain_rx) = watch::channel(false);
        let backends = Arc::new(SharedBackends::new());
        tokio::spawn(run_refresher(
            RefresherConfig {
                discovery: vec![discovery_addr.to_string()],
                secret: None,
                tls_connector: None,
                announce: None,
                drain: drain_rx.clone(),
            },
            ring_tx,
            refresh_rx,
            Arc::clone(&backends),
        ));
        let context = Arc::new(ProxyContext {
            secret: None,
            tls_connector: None,
            ring: ring_rx.clone(),
            refresh_now: refresh_tx,
            drain: drain_rx,
            backends,
            requests_total: std::sync::atomic::AtomicU64::new(0),
            upstream_failures_total: std::sync::atomic::AtomicU64::new(0),
        });
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        tokio::spawn(serve(
            listener,
            Arc::clone(&context),
            Some(tls_acceptor),
            Arc::new(Semaphore::new(max_connections)),
            Duration::from_secs(5),
        ));

        // Wait until the first roster fetch landed, so tests don't race
        // the refresher and see `B`.
        let mut ring = ring_rx;
        while ring.borrow().is_none() {
            ring.changed().await.unwrap();
        }
        addr
    }

    /// A two-node cluster behind a proxy; returns (nodes, proxy addr).
    async fn cluster(replication: usize) -> (Vec<MockNode>, String) {
        let node_a = MockNode::start().await;
        let node_b = MockNode::start().await;
        let roster = vec![
            ("node-a".to_string(), node_a.addr.clone()),
            ("node-b".to_string(), node_b.addr.clone()),
        ];
        let discovery = start_mock_discovery(roster, replication).await;
        let proxy = start_proxy(&discovery, None, 64).await;
        (vec![node_a, node_b], proxy)
    }

    async fn connect_and_auth(proxy: &str) -> (TcpStream, BytesMut) {
        let mut stream = TcpStream::connect(proxy).await.unwrap();
        stream.write_all(b"A 1\nx").await.unwrap();
        let mut buf = BytesMut::new();
        let ack = read_line(&mut stream, &mut buf).await.unwrap();
        assert_eq!(ack, "On");
        (stream, buf)
    }

    /// `connect_and_auth`, but negotiating tagged + retry-capable mode
    /// (issue #125/#272) — needed to observe a per-request `R` (rather
    /// than a connection-closing `E`) on a transport failure.
    async fn connect_and_auth_tagged(proxy: &str) -> (TcpStream, BytesMut) {
        let mut stream = TcpStream::connect(proxy).await.unwrap();
        stream.write_all(b"A 1 T R\nx").await.unwrap();
        let mut buf = BytesMut::new();
        let ack = read_line(&mut stream, &mut buf).await.unwrap();
        assert_eq!(ack, "OnT");
        (stream, buf)
    }

    /// Issue #177: a node that accepts a connection and then answers
    /// nothing at all — never even completing the auth handshake. Unlike
    /// a dropped listener (`dead_node_cluster`, connection *refused* —
    /// fails instantly), this is a genuine black hole: the dial only
    /// fails once `BackendHandle::connect`'s own `UPSTREAM_IO_TIMEOUT`
    /// elapses.
    async fn serve_black_hole(stream: TcpStream) {
        let _stream = stream;
        std::future::pending::<()>().await;
    }

    async fn start_black_hole_node() -> String {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        tokio::spawn(async move {
            loop {
                let Ok((stream, _)) = listener.accept().await else {
                    return;
                };
                tokio::spawn(serve_black_hole(stream));
            }
        });
        addr
    }

    /// Issue #420: a node that completes the tagged auth handshake
    /// normally — so `BackendHandle::connect` succeeds and `run_backend`
    /// reaches its established write loop — then reads nothing else ever
    /// again. Unlike `serve_black_hole` (which never answers anything,
    /// including the handshake, so the *dial* itself is what times out),
    /// this exercises a write that stalls on an already-live connection:
    /// once a big enough request is queued, `write_half.write_all` in
    /// `run_backend`'s writer blocks on the unread socket exactly as a
    /// node that stops draining its receive buffer would.
    async fn serve_silent_after_handshake(mut stream: TcpStream) {
        let mut buf = BytesMut::new();
        let Ok(line) = read_line(&mut stream, &mut buf).await else {
            return;
        };
        let mut parts = line.split(' ');
        if parts.next() != Some("A") {
            return;
        }
        let Some(length) = parts.next().and_then(|field| field.parse::<usize>().ok()) else {
            return;
        };
        if read_exact_into(&mut stream, &mut buf, length)
            .await
            .is_err()
        {
            return;
        }
        if stream.write_all(b"OnT\n").await.is_err() {
            return;
        }
        // Never read (or write) again.
        let _stream = stream;
        std::future::pending::<()>().await;
    }

    async fn start_silent_after_handshake_node() -> String {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        tokio::spawn(async move {
            loop {
                let Ok((stream, _)) = listener.accept().await else {
                    return;
                };
                tokio::spawn(serve_silent_after_handshake(stream));
            }
        });
        addr
    }

    /// A `ProxyContext` with no live discovery/refresher behind it — for
    /// tests that drive `SharedBackends`/the request drivers directly
    /// (`enqueue_write`, `finish_get`, ...) rather than through a full
    /// proxy listener.
    fn bare_context() -> ProxyContext {
        let (_ring_tx, ring_rx) = watch::channel(None);
        let (refresh_tx, _refresh_rx) = mpsc::channel(1);
        let (_drain_tx, drain_rx) = watch::channel(false);
        ProxyContext {
            secret: None,
            tls_connector: None,
            ring: ring_rx,
            refresh_now: refresh_tx,
            backends: Arc::new(SharedBackends::new()),
            drain: drain_rx,
            requests_total: std::sync::atomic::AtomicU64::new(0),
            upstream_failures_total: std::sync::atomic::AtomicU64::new(0),
        }
    }

    // ── end-to-end ───────────────────────────────────────────────────

    /// Issue #125: a roster whose only node is a dead address — every
    /// upstream attempt fails, which is the transient-failure shape the
    /// retryable status exists for.
    async fn dead_node_cluster() -> String {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let dead_addr = listener.local_addr().unwrap().to_string();
        drop(listener);
        let discovery = start_mock_discovery(vec![("node-dead".to_string(), dead_addr)], 1).await;
        start_proxy(&discovery, None, 64).await
    }

    #[tokio::test(flavor = "current_thread")]
    async fn a_retry_capable_client_gets_r_and_keeps_its_connection() {
        let proxy = dead_node_cluster().await;

        let mut stream = TcpStream::connect(&proxy).await.unwrap();
        stream.write_all(b"A 1 R\nx").await.unwrap();
        let mut buf = BytesMut::new();
        assert_eq!(read_line(&mut stream, &mut buf).await.unwrap(), "On");

        stream.write_all(b"G 4\nname").await.unwrap();
        assert_eq!(read_line(&mut stream, &mut buf).await.unwrap(), "R");

        // The connection survived the failure: a second request gets its
        // own answer instead of a closed socket.
        stream.write_all(b"G 4\nname").await.unwrap();
        assert_eq!(read_line(&mut stream, &mut buf).await.unwrap(), "R");
    }

    #[tokio::test(flavor = "current_thread")]
    async fn a_tagged_retry_capable_client_gets_a_tagged_r() {
        let proxy = dead_node_cluster().await;

        let mut stream = TcpStream::connect(&proxy).await.unwrap();
        stream.write_all(b"A 1 T R\nx").await.unwrap();
        let mut buf = BytesMut::new();
        assert_eq!(read_line(&mut stream, &mut buf).await.unwrap(), "OnT");

        stream.write_all(b"G 4 7\nname").await.unwrap();
        assert_eq!(read_line(&mut stream, &mut buf).await.unwrap(), "R 7");
    }

    #[tokio::test(flavor = "current_thread")]
    async fn an_empty_roster_answers_r_tagged_to_a_capable_client_and_bare_b_to_legacy() {
        // Issue #125: the per-request no-roster reply must not desync a
        // tagged connection — `R <tag>` for capable clients; legacy
        // clients keep the pre-#125 bare `B`.
        let discovery = start_mock_discovery(Vec::new(), 1).await;
        let proxy = start_proxy(&discovery, None, 64).await;

        let mut stream = TcpStream::connect(&proxy).await.unwrap();
        stream.write_all(b"A 1 T R\nx").await.unwrap();
        let mut buf = BytesMut::new();
        assert_eq!(read_line(&mut stream, &mut buf).await.unwrap(), "OnT");
        stream.write_all(b"G 4 9\nname").await.unwrap();
        assert_eq!(read_line(&mut stream, &mut buf).await.unwrap(), "R 9");

        let mut legacy = TcpStream::connect(&proxy).await.unwrap();
        legacy.write_all(b"A 1\nx").await.unwrap();
        let mut legacy_buf = BytesMut::new();
        assert_eq!(read_line(&mut legacy, &mut legacy_buf).await.unwrap(), "On");
        legacy.write_all(b"G 4\nname").await.unwrap();
        assert_eq!(read_line(&mut legacy, &mut legacy_buf).await.unwrap(), "B");
    }

    #[tokio::test(flavor = "current_thread")]
    async fn a_legacy_client_still_gets_the_fatal_e_and_close() {
        // Back-compat (issue #125): no `R` in the client's `A` means the
        // old contract — `E` then close.
        let proxy = dead_node_cluster().await;

        let mut stream = TcpStream::connect(&proxy).await.unwrap();
        stream.write_all(b"A 1\nx").await.unwrap();
        let mut buf = BytesMut::new();
        assert_eq!(read_line(&mut stream, &mut buf).await.unwrap(), "On");

        stream.write_all(b"G 4\nname").await.unwrap();
        assert_eq!(read_line(&mut stream, &mut buf).await.unwrap(), "E");
        // The writer is gone after a fatal `E`: a further request gets
        // no reply (either silence until the idle close, or an
        // immediate EOF) — bounded here rather than waiting out the
        // 60s idle timeout for the actual FIN.
        stream.write_all(b"G 4\nname").await.unwrap();
        let more =
            tokio::time::timeout(Duration::from_millis(500), read_line(&mut stream, &mut buf))
                .await;
        assert!(
            match more {
                Err(_elapsed) => true,
                Ok(Err(_eof)) => true,
                Ok(Ok(line)) => panic!("got a reply after the fatal E: {line}"),
            },
            "no further replies after the fatal E"
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn a_second_auth_after_the_handshake_is_rejected_and_closes() {
        // Regression (pass-7 audit): a repeat `A` on an already-negotiated
        // connection used to silently overwrite `tagged`/`retry_capable`,
        // desyncing every following frame. It must now be a protocol error
        // (`E`, then close), like any other.
        let (_nodes, proxy) = cluster(1).await;
        let (mut stream, mut buf) = connect_and_auth(&proxy).await;

        // A second, differently-shaped `A` (tagged this time).
        stream.write_all(b"A 1 T\nx").await.unwrap();
        assert_eq!(read_line(&mut stream, &mut buf).await.unwrap(), "E");

        // The connection is closed: the next read sees EOF.
        let n = stream.read_buf(&mut buf).await.unwrap();
        assert_eq!(n, 0, "the proxy must close the connection after the E");
    }

    #[tokio::test(flavor = "current_thread")]
    async fn round_trips_and_isolates_namespaces_through_the_proxy() {
        let (nodes, proxy) = cluster(2).await;
        let (mut stream, mut buf) = connect_and_auth(&proxy).await;

        stream
            .write_all(
                b"S 4 5\nnameAlices 5 4 3\nusersnameBobG 4\nnameg 5 4\nusersnameg 6 4\nordersname",
            )
            .await
            .unwrap();

        assert_eq!(read_line(&mut stream, &mut buf).await.unwrap(), "S");
        assert_eq!(read_line(&mut stream, &mut buf).await.unwrap(), "S");
        assert_eq!(read_line(&mut stream, &mut buf).await.unwrap(), "V 5");
        read_exact_into(&mut stream, &mut buf, 5).await.unwrap();
        assert_eq!(&buf.split_to(5)[..], b"Alice");
        assert_eq!(read_line(&mut stream, &mut buf).await.unwrap(), "V 3");
        read_exact_into(&mut stream, &mut buf, 3).await.unwrap();
        assert_eq!(&buf.split_to(3)[..], b"Bob");
        assert_eq!(read_line(&mut stream, &mut buf).await.unwrap(), "N");

        // With R=2 over two nodes, both hold both entries.
        for node in &nodes {
            assert_eq!(node.entry(b"", b"name"), Some(b"Alice".to_vec()));
            assert_eq!(node.entry(b"users", b"name"), Some(b"Bob".to_vec()));
        }

        stream.write_all(b"D 4\nnameG 4\nname").await.unwrap();
        assert_eq!(read_line(&mut stream, &mut buf).await.unwrap(), "D");
        assert_eq!(read_line(&mut stream, &mut buf).await.unwrap(), "N");
    }

    // Issue #128 measurement prototype: multi-get through the proxy.

    #[tokio::test(flavor = "current_thread")]
    async fn multi_get_returns_hits_and_misses_in_request_order() {
        let (_nodes, proxy) = cluster(2).await;
        let (mut stream, mut buf) = connect_and_auth(&proxy).await;

        stream.write_all(b"S 1 1\na1S 1 1\nb2").await.unwrap();
        assert_eq!(read_line(&mut stream, &mut buf).await.unwrap(), "S");
        assert_eq!(read_line(&mut stream, &mut buf).await.unwrap(), "S");

        stream.write_all(b"m 0 3 1 1 1\nabc").await.unwrap();
        assert_eq!(read_line(&mut stream, &mut buf).await.unwrap(), "M 3 1 1 -");
        read_exact_into(&mut stream, &mut buf, 2).await.unwrap();
        assert_eq!(&buf.split_to(2)[..], b"12");
    }

    #[tokio::test(flavor = "current_thread")]
    async fn multi_get_splits_across_owners_and_preserves_request_order() {
        // R=1: a batch spanning both nodes must fan out to each owner's
        // own sub-frame and splice the results back in request order,
        // regardless of which owner answers first.
        let (nodes, proxy) = cluster(1).await;
        let ring = RingView::new(
            vec![
                ("node-a".to_string(), nodes[0].addr.clone()),
                ("node-b".to_string(), nodes[1].addr.clone()),
            ],
            1,
        );

        let mut on_a = None;
        let mut on_b = None;
        for index in 0..32u8 {
            let key = format!("key-{index}");
            let owner = ring.owners(b"", key.as_bytes())[0].clone();
            if owner == nodes[0].addr && on_a.is_none() {
                on_a = Some(key);
            } else if owner == nodes[1].addr && on_b.is_none() {
                on_b = Some(key);
            }
        }
        let (key_a, key_b) = (
            on_a.expect("no key hashed to node a"),
            on_b.expect("no key hashed to node b"),
        );

        let (mut stream, mut buf) = connect_and_auth(&proxy).await;
        for key in [&key_a, &key_b] {
            let frame = format!("S {} 1\n{key}v", key.len());
            stream.write_all(frame.as_bytes()).await.unwrap();
            assert_eq!(read_line(&mut stream, &mut buf).await.unwrap(), "S");
        }

        // Request in a→b order once and b→a order once: the reassembly
        // must always match the request's own order, not arrival order.
        for (first, second) in [(&key_a, &key_b), (&key_b, &key_a)] {
            let frame = format!("m 0 2 {} {}\n{first}{second}", first.len(), second.len());
            stream.write_all(frame.as_bytes()).await.unwrap();
            assert_eq!(read_line(&mut stream, &mut buf).await.unwrap(), "M 2 1 1");
            read_exact_into(&mut stream, &mut buf, 2).await.unwrap();
            assert_eq!(&buf.split_to(2)[..], b"vv");
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn multi_get_retries_a_per_key_wrong_node_after_a_refresh() {
        // Issue #150: the node's own per-key check catches a stale
        // slice of the roster (`wrong_once` makes the mock answer the
        // batch's first key `W` once, exactly like a real node would) —
        // the retry lands on the same node (the ring didn't actually
        // change) and this time it answers for real, so the client
        // still sees every key correctly.
        let (nodes, proxy) = cluster(2).await;
        let (mut stream, mut buf) = connect_and_auth(&proxy).await;

        stream.write_all(b"S 1 1\na1S 1 1\nb2").await.unwrap();
        assert_eq!(read_line(&mut stream, &mut buf).await.unwrap(), "S");
        assert_eq!(read_line(&mut stream, &mut buf).await.unwrap(), "S");

        nodes[0].wrong_node_once.store(true, Ordering::SeqCst);
        nodes[1].wrong_node_once.store(true, Ordering::SeqCst);

        stream.write_all(b"m 0 2 1 1\nab").await.unwrap();
        assert_eq!(read_line(&mut stream, &mut buf).await.unwrap(), "M 2 1 1");
        read_exact_into(&mut stream, &mut buf, 2).await.unwrap();
        assert_eq!(&buf.split_to(2)[..], b"12");
    }

    #[tokio::test(flavor = "current_thread")]
    async fn multi_get_degrades_only_the_unreachable_keys_to_wrong_node() {
        // Issue #150: partial-result semantics — a batch spanning a
        // live owner and a permanently unreachable one must still
        // answer the live owner's keys correctly, and only degrade the
        // unreachable owner's keys to `WrongNode` (never fail the whole
        // frame, and never let one bad group cost the others their
        // answer).
        let live = MockNode::start().await;
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let dead_addr = listener.local_addr().unwrap().to_string();
        drop(listener);

        let discovery = start_mock_discovery(
            vec![
                ("node-live".to_string(), live.addr.clone()),
                ("node-dead".to_string(), dead_addr.clone()),
            ],
            1,
        )
        .await;
        let proxy = start_proxy(&discovery, None, 64).await;

        let ring = RingView::new(
            vec![
                ("node-live".to_string(), live.addr.clone()),
                ("node-dead".to_string(), dead_addr.clone()),
            ],
            1,
        );
        let mut on_live = None;
        let mut on_dead = None;
        for index in 0..32u8 {
            let key = format!("key-{index}");
            let owner = ring.owners(b"", key.as_bytes())[0].clone();
            if owner == live.addr && on_live.is_none() {
                on_live = Some(key);
            } else if owner == dead_addr && on_dead.is_none() {
                on_dead = Some(key);
            }
        }
        let (key_live, key_dead) = (
            on_live.expect("no key hashed to the live node"),
            on_dead.expect("no key hashed to the dead node"),
        );

        let (mut stream, mut buf) = connect_and_auth(&proxy).await;
        let frame = format!("S {} 1\n{key_live}v", key_live.len());
        stream.write_all(frame.as_bytes()).await.unwrap();
        assert_eq!(read_line(&mut stream, &mut buf).await.unwrap(), "S");

        let frame = format!(
            "m 0 2 {} {}\n{key_live}{key_dead}",
            key_live.len(),
            key_dead.len()
        );
        stream.write_all(frame.as_bytes()).await.unwrap();
        assert_eq!(read_line(&mut stream, &mut buf).await.unwrap(), "M 2 1 W");
        read_exact_into(&mut stream, &mut buf, 1).await.unwrap();
        assert_eq!(&buf.split_to(1)[..], b"v");
    }

    // Issue #221: multi-get replica fallback — `retry_multi_get` used to
    // give up as soon as the (freshly refreshed) primary failed, instead
    // of falling through to a replica the way `retry_get_on` does for
    // single-key `Get`.

    async fn dead_primary_live_replica_cluster() -> (MockNode, String, String, String) {
        let live = MockNode::start().await;
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let dead_addr = listener.local_addr().unwrap().to_string();
        drop(listener);

        let discovery = start_mock_discovery(
            vec![
                ("node-live".to_string(), live.addr.clone()),
                ("node-dead".to_string(), dead_addr.clone()),
            ],
            2,
        )
        .await;
        let proxy = start_proxy(&discovery, None, 64).await;

        let ring = RingView::new(
            vec![
                ("node-live".to_string(), live.addr.clone()),
                ("node-dead".to_string(), dead_addr.clone()),
            ],
            2,
        );
        let mut key = None;
        for index in 0..32u8 {
            let candidate = format!("key-{index}");
            if ring.owners(b"", candidate.as_bytes())[0] == dead_addr {
                key = Some(candidate);
                break;
            }
        }
        let key = key.expect("no key primaried by the dead node");

        (live, dead_addr, proxy, key)
    }

    #[tokio::test(flavor = "current_thread")]
    async fn multi_get_falls_back_to_a_replica_when_the_primary_is_down() {
        // R=2, primary unreachable: a live replica holding the value
        // must still answer the multi-get, same as a single `Get` of the
        // same key already does via `retry_get_on`.
        let (live, _dead_addr, proxy, key) = dead_primary_live_replica_cluster().await;

        // The replica already holds the value (e.g. replicated before
        // the primary died) — inserted directly, since a `Set` routed
        // through the dead primary would itself fail via the proxy.
        live.store
            .lock()
            .unwrap()
            .insert((Vec::new(), key.as_bytes().to_vec()), b"v".to_vec());

        let (mut stream, mut buf) = connect_and_auth(&proxy).await;
        let frame = format!("m 0 1 {}\n{key}", key.len());
        stream.write_all(frame.as_bytes()).await.unwrap();
        assert_eq!(read_line(&mut stream, &mut buf).await.unwrap(), "M 1 1");
        read_exact_into(&mut stream, &mut buf, 1).await.unwrap();
        assert_eq!(&buf.split_to(1)[..], b"v");
    }

    #[tokio::test(flavor = "current_thread")]
    async fn multi_get_replica_miss_matches_single_get_after_a_down_primary() {
        // Same dead-primary/live-replica shape, but neither node holds
        // the key: the replica's `NotFound` must be trusted as a real
        // miss (`-`), not degraded to `WrongNode` — the same answer a
        // single `Get` of the same key gives in this situation.
        let (_live, _dead_addr, proxy, key) = dead_primary_live_replica_cluster().await;

        let (mut stream, mut buf) = connect_and_auth(&proxy).await;

        let frame = format!("m 0 1 {}\n{key}", key.len());
        stream.write_all(frame.as_bytes()).await.unwrap();
        assert_eq!(read_line(&mut stream, &mut buf).await.unwrap(), "M 1 -");

        let frame = format!("G {}\n{key}", key.len());
        stream.write_all(frame.as_bytes()).await.unwrap();
        assert_eq!(read_line(&mut stream, &mut buf).await.unwrap(), "N");
    }

    // Issue #150: multi-set through the proxy.

    #[tokio::test(flavor = "current_thread")]
    async fn multi_set_stores_every_key_and_replicates() {
        let (nodes, proxy) = cluster(2).await;
        let (mut stream, mut buf) = connect_and_auth(&proxy).await;

        stream.write_all(b"o 0 2 1 1 1 1\nabcd").await.unwrap();
        assert_eq!(read_line(&mut stream, &mut buf).await.unwrap(), "O 2 S S");

        for node in &nodes {
            assert_eq!(node.entry(b"", b"a"), Some(b"b".to_vec()));
            assert_eq!(node.entry(b"", b"c"), Some(b"d".to_vec()));
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn multi_set_groups_by_address_when_roles_differ_across_keys() {
        // R=2 with 2 nodes: every key's owners are both nodes, but
        // which one is *primary* can differ per key — enough to
        // exercise "group by address, not by rank" (the same node's
        // sub-frame can carry one key it's primary for and another it's
        // only a replica for) without needing a third node.
        let (nodes, proxy) = cluster(2).await;
        let ring = RingView::new(
            vec![
                ("node-a".to_string(), nodes[0].addr.clone()),
                ("node-b".to_string(), nodes[1].addr.clone()),
            ],
            2,
        );

        let mut primary_a = None;
        let mut primary_b = None;
        for index in 0..32u8 {
            let key = format!("key-{index}");
            let owner = ring.owners(b"", key.as_bytes())[0].clone();
            if owner == nodes[0].addr && primary_a.is_none() {
                primary_a = Some(key);
            } else if owner == nodes[1].addr && primary_b.is_none() {
                primary_b = Some(key);
            }
        }
        let (key_a, key_b) = (
            primary_a.expect("no key primaried by node a"),
            primary_b.expect("no key primaried by node b"),
        );

        let (mut stream, mut buf) = connect_and_auth(&proxy).await;
        let frame = format!(
            "o 0 2 {} 1 {} 1\n{key_a}1{key_b}2",
            key_a.len(),
            key_b.len()
        );
        stream.write_all(frame.as_bytes()).await.unwrap();
        assert_eq!(read_line(&mut stream, &mut buf).await.unwrap(), "O 2 S S");

        for node in &nodes {
            assert_eq!(node.entry(b"", key_a.as_bytes()), Some(b"1".to_vec()));
            assert_eq!(node.entry(b"", key_b.as_bytes()), Some(b"2".to_vec()));
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn multi_set_retries_a_per_key_wrong_node_after_a_refresh() {
        let (nodes, proxy) = cluster(2).await;
        let (mut stream, mut buf) = connect_and_auth(&proxy).await;

        nodes[0].wrong_node_once.store(true, Ordering::SeqCst);
        nodes[1].wrong_node_once.store(true, Ordering::SeqCst);

        stream.write_all(b"o 0 2 1 1 1 1\nabcd").await.unwrap();
        assert_eq!(read_line(&mut stream, &mut buf).await.unwrap(), "O 2 S S");
    }

    #[tokio::test(flavor = "current_thread")]
    async fn routes_by_the_same_ring_as_the_cluster() {
        // R=1: each key lands on exactly the node HRW names.
        let (nodes, proxy) = cluster(1).await;
        let (mut stream, mut buf) = connect_and_auth(&proxy).await;

        let ring = RingView::new(
            vec![
                ("node-a".to_string(), nodes[0].addr.clone()),
                ("node-b".to_string(), nodes[1].addr.clone()),
            ],
            1,
        );

        for index in 0..16u8 {
            let key = format!("key-{index}");
            let frame = format!("S {} 1\n{key}v", key.len());
            stream.write_all(frame.as_bytes()).await.unwrap();
            assert_eq!(read_line(&mut stream, &mut buf).await.unwrap(), "S");

            let owner = &ring.owners(b"", key.as_bytes())[0];
            let (on_owner, on_other) = if *owner == nodes[0].addr {
                (&nodes[0], &nodes[1])
            } else {
                (&nodes[1], &nodes[0])
            };
            assert!(
                on_owner.entry(b"", key.as_bytes()).is_some(),
                "{key} misplaced"
            );
            assert!(
                on_other.entry(b"", key.as_bytes()).is_none(),
                "{key} duplicated"
            );
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn pipelined_responses_come_back_in_request_order() {
        // R=1 and a slow first-request owner: the second request (owned
        // by the fast node) completes upstream first, but the client
        // must still see the responses in request order.
        let (nodes, proxy) = cluster(1).await;
        let ring = RingView::new(
            vec![
                ("node-a".to_string(), nodes[0].addr.clone()),
                ("node-b".to_string(), nodes[1].addr.clone()),
            ],
            1,
        );
        // Two keys owned by different nodes.
        let key_on = |node_index: usize| -> String {
            for index in 0..64u32 {
                let key = format!("k{index}");
                if ring.owners(b"", key.as_bytes())[0] == nodes[node_index].addr {
                    return key;
                }
            }
            panic!("no key found for node {node_index}");
        };
        let slow_key = key_on(0);
        let fast_key = key_on(1);

        let (mut stream, mut buf) = connect_and_auth(&proxy).await;
        for key in [&slow_key, &fast_key] {
            let frame = format!("S {} 2\n{key}vv", key.len());
            stream.write_all(frame.as_bytes()).await.unwrap();
            assert_eq!(read_line(&mut stream, &mut buf).await.unwrap(), "S");
        }

        *nodes[0].get_delay.lock().unwrap() = Duration::from_millis(150);

        let start = std::time::Instant::now();
        let frames = format!(
            "G {} 1\n{slow_key}G {} 2\n{fast_key}",
            slow_key.len(),
            fast_key.len()
        );
        // Tags: untagged client — order alone matters.
        let frames = frames.replace(" 1\n", &format!(" {}\n", slow_key.len()));
        let _ = frames;
        let mut request = format!("G {}\n{slow_key}", slow_key.len()).into_bytes();
        request.extend_from_slice(format!("G {}\n{fast_key}", fast_key.len()).as_bytes());
        stream.write_all(&request).await.unwrap();

        assert_eq!(read_line(&mut stream, &mut buf).await.unwrap(), "V 2");
        read_exact_into(&mut stream, &mut buf, 2).await.unwrap();
        let _ = buf.split_to(2);
        let first_arrived = start.elapsed();
        assert_eq!(read_line(&mut stream, &mut buf).await.unwrap(), "V 2");
        read_exact_into(&mut stream, &mut buf, 2).await.unwrap();
        let _ = buf.split_to(2);

        // The slow response arrived first (order held) and took at least
        // the injected delay — proving the fast one waited rather than
        // overtaking.
        assert!(first_arrived >= Duration::from_millis(140));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn a_wrong_node_answer_is_retried_after_a_refresh_not_forwarded() {
        let (nodes, proxy) = cluster(2).await;
        let (mut stream, mut buf) = connect_and_auth(&proxy).await;

        stream.write_all(b"S 1 1\nkv").await.unwrap();
        assert_eq!(read_line(&mut stream, &mut buf).await.unwrap(), "S");

        nodes[0].wrong_node_once.store(true, Ordering::SeqCst);
        nodes[1].wrong_node_once.store(true, Ordering::SeqCst);
        stream.write_all(b"G 1\nk").await.unwrap();
        assert_eq!(read_line(&mut stream, &mut buf).await.unwrap(), "V 1");
        read_exact_into(&mut stream, &mut buf, 1).await.unwrap();
        assert_eq!(&buf.split_to(1)[..], b"v");
    }

    #[tokio::test(flavor = "current_thread")]
    async fn clear_fans_out_to_every_member() {
        let (nodes, proxy) = cluster(2).await;
        let (mut stream, mut buf) = connect_and_auth(&proxy).await;

        stream
            .write_all(b"s 5 1 1\nuserskvS 1 1\nkvc 5\nusersg 5 1\nuserskG 1\nkF\nG 1\nk")
            .await
            .unwrap();
        assert_eq!(read_line(&mut stream, &mut buf).await.unwrap(), "S");
        assert_eq!(read_line(&mut stream, &mut buf).await.unwrap(), "S");
        assert_eq!(read_line(&mut stream, &mut buf).await.unwrap(), "C");
        assert_eq!(read_line(&mut stream, &mut buf).await.unwrap(), "N");
        assert_eq!(read_line(&mut stream, &mut buf).await.unwrap(), "V 1");
        read_exact_into(&mut stream, &mut buf, 1).await.unwrap();
        let _ = buf.split_to(1);
        assert_eq!(read_line(&mut stream, &mut buf).await.unwrap(), "C");
        assert_eq!(read_line(&mut stream, &mut buf).await.unwrap(), "N");

        for node in &nodes {
            assert_eq!(node.cleared.load(Ordering::SeqCst), 1);
            assert_eq!(node.flushed.load(Ordering::SeqCst), 1);
            assert_eq!(node.len(), 0);
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn tagged_clients_get_their_tags_echoed() {
        let (_nodes, proxy) = cluster(2).await;
        let mut stream = TcpStream::connect(&proxy).await.unwrap();
        let mut buf = BytesMut::new();

        stream.write_all(b"A 1 T\nx").await.unwrap();
        assert_eq!(read_line(&mut stream, &mut buf).await.unwrap(), "OnT");

        stream
            .write_all(b"S 1 1 7\nkvG 1 8\nkD 1 9\nkc 5 10\nusers")
            .await
            .unwrap();
        assert_eq!(read_line(&mut stream, &mut buf).await.unwrap(), "S 7");
        assert_eq!(read_line(&mut stream, &mut buf).await.unwrap(), "V 1 8");
        read_exact_into(&mut stream, &mut buf, 1).await.unwrap();
        let _ = buf.split_to(1);
        assert_eq!(read_line(&mut stream, &mut buf).await.unwrap(), "D 9");
        assert_eq!(read_line(&mut stream, &mut buf).await.unwrap(), "C 10");
    }

    #[tokio::test(flavor = "current_thread")]
    async fn auth_is_required_when_a_secret_is_configured() {
        let node = MockNode::start().await;
        let roster = vec![("node-a".to_string(), node.addr.clone())];
        let discovery = start_mock_discovery(roster, 1).await;
        let proxy = start_proxy(&discovery, Some("s3cret"), 64).await;

        // Wrong secret: En, closed.
        let mut stream = TcpStream::connect(&proxy).await.unwrap();
        let mut buf = BytesMut::new();
        stream.write_all(b"A 5\nwrong").await.unwrap();
        assert_eq!(read_line(&mut stream, &mut buf).await.unwrap(), "En");

        // No auth at all: E, closed.
        let mut stream = TcpStream::connect(&proxy).await.unwrap();
        let mut buf = BytesMut::new();
        stream.write_all(b"G 1\nk").await.unwrap();
        assert_eq!(read_line(&mut stream, &mut buf).await.unwrap(), "E");

        // Right secret: works end to end.
        let mut stream = TcpStream::connect(&proxy).await.unwrap();
        let mut buf = BytesMut::new();
        stream
            .write_all(b"A 6\ns3cretS 1 1\nkvG 1\nk")
            .await
            .unwrap();
        assert_eq!(read_line(&mut stream, &mut buf).await.unwrap(), "On");
        assert_eq!(read_line(&mut stream, &mut buf).await.unwrap(), "S");
        assert_eq!(read_line(&mut stream, &mut buf).await.unwrap(), "V 1");
    }

    #[tokio::test(flavor = "current_thread")]
    async fn cluster_internal_commands_are_rejected() {
        let (_nodes, proxy) = cluster(2).await;
        let (mut stream, mut buf) = connect_and_auth(&proxy).await;

        stream.write_all(b"M 1 1 0 2 1\nxyz").await.unwrap();
        assert_eq!(read_line(&mut stream, &mut buf).await.unwrap(), "E");
    }

    #[tokio::test(flavor = "current_thread")]
    async fn connections_over_the_budget_are_answered_busy() {
        let node = MockNode::start().await;
        let roster = vec![("node-a".to_string(), node.addr.clone())];
        let discovery = start_mock_discovery(roster, 1).await;
        let proxy = start_proxy(&discovery, None, 1).await;

        let (_held, _buf) = connect_and_auth(&proxy).await;

        let mut second = TcpStream::connect(&proxy).await.unwrap();
        let mut buf = BytesMut::new();
        assert_eq!(read_line(&mut second, &mut buf).await.unwrap(), "B");
    }

    #[tokio::test(flavor = "current_thread")]
    async fn client_connections_share_one_backend_connection_per_node() {
        // Issue #110: the point of the multiplexing — N clients, still
        // exactly one proxy→node connection per node.
        let (nodes, proxy) = cluster(2).await;

        let (mut first, mut first_buf) = connect_and_auth(&proxy).await;
        let (mut second, mut second_buf) = connect_and_auth(&proxy).await;
        let (mut third, mut third_buf) = connect_and_auth(&proxy).await;

        for (stream, buf) in [
            (&mut first, &mut first_buf),
            (&mut second, &mut second_buf),
            (&mut third, &mut third_buf),
        ] {
            stream.write_all(b"S 1 1\nkv").await.unwrap();
            assert_eq!(read_line(stream, buf).await.unwrap(), "S");
            stream.write_all(b"G 1\nk").await.unwrap();
            assert_eq!(read_line(stream, buf).await.unwrap(), "V 1");
            read_exact_into(stream, buf, 1).await.unwrap();
            let _ = buf.split_to(1);
        }

        for node in &nodes {
            assert_eq!(
                node.auth_count.load(Ordering::SeqCst),
                1,
                "three clients must share one backend connection"
            );
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn the_shared_backend_connection_is_genuinely_pipelined() {
        // Issue #110: with #109's serial write-then-await-reply loop the
        // node never sees request N+1 before it answered N; the mock
        // flags bytes that arrive while a delayed request is still
        // unanswered.
        let node = MockNode::start().await;
        let roster = vec![("node-a".to_string(), node.addr.clone())];
        let discovery = start_mock_discovery(roster, 1).await;
        let proxy = start_proxy(&discovery, None, 64).await;
        let (mut stream, mut buf) = connect_and_auth(&proxy).await;

        stream.write_all(b"S 1 1\nkv").await.unwrap();
        assert_eq!(read_line(&mut stream, &mut buf).await.unwrap(), "S");

        *node.get_delay.lock().unwrap() = Duration::from_millis(100);
        stream.write_all(b"G 1\nkG 1\nkG 1\nk").await.unwrap();
        for _ in 0..3 {
            assert_eq!(read_line(&mut stream, &mut buf).await.unwrap(), "V 1");
            read_exact_into(&mut stream, &mut buf, 1).await.unwrap();
            let _ = buf.split_to(1);
        }

        assert!(
            node.saw_pipelined.load(Ordering::SeqCst),
            "later requests must reach the node while an earlier one is still unanswered"
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn a_dropped_shared_connection_is_redialed_transparently() {
        // Issue #110: a node closing the shared connection (idle
        // timeout, restart) fails the in-flight request's ordered
        // attempt; the retry path redials and the client still gets its
        // answer — for reads AND writes.
        let node = MockNode::start().await;
        let roster = vec![("node-a".to_string(), node.addr.clone())];
        let discovery = start_mock_discovery(roster, 1).await;
        let proxy = start_proxy(&discovery, None, 64).await;
        let (mut stream, mut buf) = connect_and_auth(&proxy).await;

        stream.write_all(b"S 1 1\nkv").await.unwrap();
        assert_eq!(read_line(&mut stream, &mut buf).await.unwrap(), "S");

        node.close_once.store(true, Ordering::SeqCst);
        stream.write_all(b"G 1\nk").await.unwrap();
        assert_eq!(read_line(&mut stream, &mut buf).await.unwrap(), "V 1");
        read_exact_into(&mut stream, &mut buf, 1).await.unwrap();
        assert_eq!(&buf.split_to(1)[..], b"v");

        node.close_once.store(true, Ordering::SeqCst);
        stream.write_all(b"S 1 2\nkv2").await.unwrap();
        assert_eq!(read_line(&mut stream, &mut buf).await.unwrap(), "S");

        assert_eq!(
            node.auth_count.load(Ordering::SeqCst),
            3,
            "each drop must cost exactly one redial"
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn a_poisoned_backend_task_eagerly_decrements_the_dialed_gauge() {
        // issue #192: previously the `dialed` gauge stayed counted for a
        // dead connection until some *later* caller's send against it
        // failed and `SharedBackends::enqueue` lazily noticed. Driving
        // `enqueue` directly (bypassing `call`'s automatic redial, which
        // would itself generate the "further traffic" that used to be
        // required) isolates that: the gauge must reach 0 on its own,
        // with nothing more ever sent to this node.
        let node = MockNode::start().await;
        let context = Arc::new(ProxyContext {
            secret: None,
            tls_connector: None,
            ring: watch::channel(None).1,
            refresh_now: mpsc::channel(4).0,
            drain: watch::channel(false).1,
            backends: Arc::new(SharedBackends::new()),
            requests_total: std::sync::atomic::AtomicU64::new(0),
            upstream_failures_total: std::sync::atomic::AtomicU64::new(0),
        });

        let reply = context
            .backends
            .enqueue(
                &context,
                &node.addr,
                frame_set(b"", b"k", b"v", None),
                Expect::Stored,
            )
            .await;
        assert!(matches!(reply.await, Ok(NodeReply::Stored)));
        assert_eq!(context.backends.dialed.load(Ordering::Relaxed), 1);

        // The node drops the connection without replying to this next
        // request; nothing further is ever sent to it after that.
        node.close_once.store(true, Ordering::SeqCst);
        let reply = context
            .backends
            .enqueue(&context, &node.addr, frame_get(b"", b"k"), Expect::Value)
            .await;
        assert!(
            reply.await.is_err(),
            "the dropped connection must fail this request"
        );

        // Bounded poll: the backend task's own cleanup runs asynchronously
        // once it observes the closed connection, not synchronously with
        // the failed reply above.
        tokio::time::timeout(Duration::from_secs(5), async {
            while context.backends.dialed.load(Ordering::Relaxed) != 0 {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("dialed gauge must reach 0 without any further traffic to the node");
    }

    #[tokio::test(flavor = "current_thread")]
    async fn pruning_drops_a_stale_addresss_slot_and_dial_failure_entries() {
        // Issue #220: `slots`/`dial_failures` must not accumulate one
        // entry per address the cluster has ever used — the roster
        // refresher prunes both maps against each fresh `RingView`.
        let node = MockNode::start().await;
        let context = Arc::new(ProxyContext {
            secret: None,
            tls_connector: None,
            ring: watch::channel(None).1,
            refresh_now: mpsc::channel(4).0,
            drain: watch::channel(false).1,
            backends: Arc::new(SharedBackends::new()),
            requests_total: std::sync::atomic::AtomicU64::new(0),
            upstream_failures_total: std::sync::atomic::AtomicU64::new(0),
        });

        // Dial address A and hold the slot the map hands back — this
        // stands in for a caller that grabbed the slot just before a
        // prune runs, per `slot()`'s own "an in-flight connection holds
        // its own Arc clone" contract.
        let slot_before_prune = context.backends.slot(&node.addr);
        let reply = context
            .backends
            .enqueue(
                &context,
                &node.addr,
                frame_set(b"", b"k", b"v", None),
                Expect::Stored,
            )
            .await;
        assert!(matches!(reply.await, Ok(NodeReply::Stored)));
        assert_eq!(context.backends.dialed.load(Ordering::Relaxed), 1);

        // A synthetic dial-failure record for the same address (as if an
        // earlier dial attempt to it had failed) — prune must clear this
        // map too, not just `slots`.
        context.backends.note_dial_failure(&node.addr);
        {
            let slots = context.backends.slots.lock().unwrap();
            assert!(slots.contains_key(&node.addr));
            let failures = context.backends.dial_failures.lock().unwrap();
            assert!(failures.contains_key(&node.addr));
        }

        // Refresh to a ring that no longer includes A.
        let ring = RingView::new(vec![("other".to_string(), "127.0.0.1:1".to_string())], 1);
        context.backends.prune(&ring);

        {
            let slots = context.backends.slots.lock().unwrap();
            assert!(
                !slots.contains_key(&node.addr),
                "prune must drop the stale address's slot entry"
            );
            let failures = context.backends.dial_failures.lock().unwrap();
            assert!(
                !failures.contains_key(&node.addr),
                "prune must drop the stale address's dial-failure entry"
            );
        }

        // Pruning the map must not disturb the live connection itself —
        // the `dialed` gauge is untouched, and the connection captured
        // beforehand still serves requests normally.
        assert_eq!(context.backends.dialed.load(Ordering::Relaxed), 1);
        let handle = slot_before_prune
            .lock()
            .await
            .clone()
            .expect("the pre-prune slot still holds its connection");
        let (reply_tx, reply_rx) = oneshot::channel();
        handle
            .sender
            .send(BackendRequest {
                frame: frame_get(b"", b"k"),
                expect: Expect::Value,
                reply: reply_tx,
            })
            .await
            .unwrap();
        assert!(matches!(
            reply_rx.await.unwrap(),
            Ok(NodeReply::Value(ref value)) if value.as_ref() == b"v"
        ));

        // And when that pruned-but-still-referenced connection eventually
        // dies, its own teardown still finds and decrements the *same*
        // `dialed` gauge — accounting stays correct even though the map
        // no longer has an entry to route through.
        node.close_once.store(true, Ordering::SeqCst);
        let (reply_tx, reply_rx) = oneshot::channel();
        handle
            .sender
            .send(BackendRequest {
                frame: frame_get(b"", b"k"),
                expect: Expect::Value,
                reply: reply_tx,
            })
            .await
            .unwrap();
        assert!(
            reply_rx.await.unwrap().is_err(),
            "the dropped connection must fail this request"
        );
        tokio::time::timeout(Duration::from_secs(5), async {
            while context.backends.dialed.load(Ordering::Relaxed) != 0 {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("dialed gauge must reach 0 even for a connection pruned from the map");
    }

    #[tokio::test(flavor = "current_thread")]
    async fn pruning_an_unreferenced_backend_tears_down_its_task_and_socket() {
        // Regression (pass-7 audit): when autoscaling retires a node,
        // `prune` drops its slot entry, but the backend's task and TCP
        // socket must actually go away. They previously leaked: the task
        // held its own `Sender` clone, so `receiver.recv()` never
        // returned `None` after the slot's handle was dropped, and — with
        // no further traffic to trip the reader into poisoning — the two
        // tokio tasks and the fd lived for the whole proxy's lifetime.
        // The node here stays fully alive, so a dropped-sender teardown is
        // the ONLY thing that can wind the task down.
        let node = MockNode::start().await;
        let context = Arc::new(ProxyContext {
            secret: None,
            tls_connector: None,
            ring: watch::channel(None).1,
            refresh_now: mpsc::channel(4).0,
            drain: watch::channel(false).1,
            backends: Arc::new(SharedBackends::new()),
            requests_total: std::sync::atomic::AtomicU64::new(0),
            upstream_failures_total: std::sync::atomic::AtomicU64::new(0),
        });

        // Dial via a normal request and let it complete, so no caller
        // holds a handle clone afterward — the slot map is the sole owner.
        let reply = context
            .backends
            .enqueue(
                &context,
                &node.addr,
                frame_set(b"", b"k", b"v", None),
                Expect::Stored,
            )
            .await;
        assert!(matches!(reply.await, Ok(NodeReply::Stored)));
        assert_eq!(context.backends.dialed.load(Ordering::Relaxed), 1);

        // Retire the address. `prune` drops the slot's handle — the last
        // sender — with the node still alive and no traffic in flight.
        let ring = RingView::new(vec![("other".to_string(), "127.0.0.1:1".to_string())], 1);
        context.backends.prune(&ring);

        tokio::time::timeout(Duration::from_secs(5), async {
            while context.backends.dialed.load(Ordering::Relaxed) != 0 {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("a pruned backend's task must wind down on its own, not leak");
    }

    #[tokio::test(flavor = "current_thread")]
    async fn a_deep_pipelined_burst_completes_under_the_in_flight_caps() {
        // Issue #110: 300 outstanding requests exceed every cap in the
        // chain (CLIENT_IN_FLIGHT, BACKEND_QUEUE_DEPTH,
        // MAX_BACKEND_IN_FLIGHT); backpressure must slow them down, not
        // deadlock or drop them.
        let (_nodes, proxy) = cluster(2).await;
        let (mut stream, mut buf) = connect_and_auth(&proxy).await;

        let mut request = Vec::new();
        for index in 0..300u32 {
            let key = format!("burst-{index}");
            request.extend_from_slice(format!("S {} 1\n{key}x", key.len()).as_bytes());
        }

        let (mut read_half, mut write_half) = stream.split();
        let writer = async {
            write_half.write_all(&request).await.unwrap();
        };
        let reader = async {
            for _ in 0..300 {
                assert_eq!(read_line(&mut read_half, &mut buf).await.unwrap(), "S");
            }
        };
        tokio::join!(writer, reader);
    }

    /// Issue #124 helper: one plain HTTP GET → (status line, body).
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
    async fn metrics_endpoint_reports_proxy_gauges_and_counters() {
        // A live proxy with real traffic, plus its metrics listener.
        let (_nodes, proxy) = cluster(2).await;
        let (mut stream, mut buf) = connect_and_auth(&proxy).await;
        stream.write_all(b"S 1 1\nkvG 1\nk").await.unwrap();
        assert_eq!(read_line(&mut stream, &mut buf).await.unwrap(), "S");
        assert_eq!(read_line(&mut stream, &mut buf).await.unwrap(), "V 1");
        read_exact_into(&mut stream, &mut buf, 1).await.unwrap();
        let _ = buf.split_to(1);

        // The metrics server shares the live context; reach it through a
        // second context handle is not possible from here, so boot one
        // against the same shape instead: exercised end-to-end below via
        // the standalone constructor used by start_proxy is private —
        // simplest is a dedicated context.
        let (ring_tx, ring_rx) = watch::channel(None);
        let (refresh_tx, _refresh_rx) = mpsc::channel(4);
        let (_drain_tx, drain_rx) = watch::channel(false);
        let context = Arc::new(ProxyContext {
            secret: None,
            tls_connector: None,
            ring: ring_rx,
            refresh_now: refresh_tx,
            drain: drain_rx,
            backends: Arc::new(SharedBackends::new()),
            requests_total: std::sync::atomic::AtomicU64::new(7),
            upstream_failures_total: std::sync::atomic::AtomicU64::new(2),
        });
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        let permits = Arc::new(Semaphore::new(8));
        let held = Arc::clone(&permits).try_acquire_owned().unwrap();
        tokio::spawn(run_metrics_server(
            listener,
            Arc::clone(&context),
            Arc::clone(&permits),
            8,
            Arc::new(Semaphore::new(METRICS_MAX_CONNECTIONS)),
        ));

        let (status, body) = http_get(&addr, "/metrics").await;
        assert_eq!(status, "HTTP/1.1 200 OK");
        assert!(
            body.contains("nanocached_proxy_client_connections 1\n"),
            "{body}"
        );
        assert!(
            body.contains("nanocached_proxy_client_connections_max 8\n"),
            "{body}"
        );
        assert!(
            body.contains("nanocached_proxy_requests_total 7\n"),
            "{body}"
        );
        assert!(
            body.contains("nanocached_proxy_upstream_failures_total 2\n"),
            "{body}"
        );
        drop(held);

        // Readiness follows the roster.
        let (status, _) = http_get(&addr, "/readyz").await;
        assert_eq!(status, "HTTP/1.1 503 Service Unavailable");
        ring_tx
            .send(Some(Arc::new(RingView::new(
                vec![("a".to_string(), "127.0.0.1:1".to_string())],
                1,
            ))))
            .unwrap();
        let (status, _) = http_get(&addr, "/readyz").await;
        assert_eq!(status, "HTTP/1.1 200 OK");
        let (status, _) = http_get(&addr, "/healthz").await;
        assert_eq!(status, "HTTP/1.1 200 OK");
    }

    #[tokio::test(flavor = "current_thread")]
    async fn metrics_listener_caps_its_own_concurrent_connections() {
        // Issue #233: the metrics listener has no client-listener-style
        // busy reply, so it needs its own connection cap — a scrape
        // storm shouldn't be able to spawn an unbounded number of
        // handler tasks. With the cap exhausted, a new connection must
        // be dropped rather than answered.
        let (ring_tx, ring_rx) = watch::channel(None);
        let (refresh_tx, _refresh_rx) = mpsc::channel(4);
        let (_drain_tx, drain_rx) = watch::channel(false);
        let context = Arc::new(ProxyContext {
            secret: None,
            tls_connector: None,
            ring: ring_rx,
            refresh_now: refresh_tx,
            drain: drain_rx,
            backends: Arc::new(SharedBackends::new()),
            requests_total: std::sync::atomic::AtomicU64::new(0),
            upstream_failures_total: std::sync::atomic::AtomicU64::new(0),
        });
        drop(ring_tx);
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        let metrics_permits = Arc::new(Semaphore::new(1));
        // Hold the listener's only permit, as if one scrape were already
        // in flight.
        let held = Arc::clone(&metrics_permits).try_acquire_owned().unwrap();
        tokio::spawn(run_metrics_server(
            listener,
            Arc::clone(&context),
            Arc::new(Semaphore::new(8)),
            8,
            metrics_permits,
        ));

        let mut stream = TcpStream::connect(&addr).await.unwrap();
        stream
            .write_all(b"GET /healthz HTTP/1.1\r\nHost: x\r\n\r\n")
            .await
            .unwrap();
        let mut response = Vec::new();
        // Some platforms (macOS observed) answer a close of a socket that
        // still has this test's unread request bytes sitting in its
        // kernel buffer with a TCP RST instead of a clean FIN — either
        // way, nothing was ever answered.
        match stream.read_to_end(&mut response).await {
            Ok(read) => assert_eq!(read, 0, "got a response: {response:?}"),
            Err(error) => assert!(
                matches!(
                    error.kind(),
                    std::io::ErrorKind::ConnectionReset | std::io::ErrorKind::BrokenPipe
                ),
                "unexpected read error over the metrics cap: {error}"
            ),
        }
        assert!(
            response.is_empty(),
            "over the metrics cap, the connection must be dropped with no response, got {response:?}"
        );

        drop(held);
        // Once the permit is free again, a scrape gets answered normally.
        let (status, _) = http_get(&addr, "/healthz").await;
        assert_eq!(status, "HTTP/1.1 200 OK");
    }

    #[tokio::test(flavor = "current_thread")]
    async fn a_drain_deregisters_delivers_in_flight_replies_and_stops_accepting() {
        // Issue #124: the whole graceful scale-in story in one pass.
        let node = MockNode::start().await;
        let roster = vec![("node-a".to_string(), node.addr.clone())];
        let (discovery, deregistered) = start_mock_discovery_recording(roster, 1).await;
        let identity = ProxyIdentity {
            name: "proxy-under-test".to_string(),
            token: "tk-drain".to_string(),
        };
        let (proxy, drain_tx, context) =
            start_proxy_with_drain(&discovery, None, 64, Some(identity)).await;

        let (mut stream, mut buf) = connect_and_auth(&proxy).await;
        stream.write_all(b"S 1 1\nkv").await.unwrap();
        assert_eq!(read_line(&mut stream, &mut buf).await.unwrap(), "S");

        // An in-flight request straddling the drain: 200ms of node
        // delay, drain signalled while it is outstanding.
        *node.get_delay.lock().unwrap() = Duration::from_millis(200);
        stream.write_all(b"G 1\nk").await.unwrap();
        tokio::time::sleep(Duration::from_millis(50)).await;
        drain_tx.send(true).unwrap();

        // The reply still arrives — drains finish in-flight work.
        assert_eq!(read_line(&mut stream, &mut buf).await.unwrap(), "V 1");
        read_exact_into(&mut stream, &mut buf, 1).await.unwrap();
        assert_eq!(&buf.split_to(1)[..], b"v");

        // ... and the connection then closes cleanly (no E, just EOF).
        let closed = read_line(&mut stream, &mut buf).await;
        assert!(closed.is_err(), "the drained connection must close");

        // The deregistration reached discovery (the refresher sends it).
        let mut seen = false;
        for _ in 0..50 {
            if deregistered.lock().unwrap().as_slice() == ["proxy-under-test".to_string()] {
                seen = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert!(seen, "the drain must deregister the proxy from discovery");

        // New connections are refused: the accept loop is gone.
        tokio::time::sleep(Duration::from_millis(50)).await;
        let refused = TcpStream::connect(&proxy).await;
        assert!(
            refused.is_err() || refused.unwrap().read_u8().await.is_err(),
            "a draining proxy must not take new connections"
        );

        // And readiness reports it.
        assert!(*context.drain.borrow());
    }

    #[tokio::test(flavor = "current_thread")]
    async fn readyz_answers_draining_during_a_drain() {
        let (ring_tx, ring_rx) = watch::channel(None);
        let (refresh_tx, _refresh_rx) = mpsc::channel(4);
        let (drain_tx, drain_rx) = watch::channel(false);
        let context = Arc::new(ProxyContext {
            secret: None,
            tls_connector: None,
            ring: ring_rx,
            refresh_now: refresh_tx,
            drain: drain_rx,
            backends: Arc::new(SharedBackends::new()),
            requests_total: std::sync::atomic::AtomicU64::new(0),
            upstream_failures_total: std::sync::atomic::AtomicU64::new(0),
        });
        ring_tx
            .send(Some(Arc::new(RingView::new(
                vec![("a".to_string(), "127.0.0.1:1".to_string())],
                1,
            ))))
            .unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        tokio::spawn(run_metrics_server(
            listener,
            Arc::clone(&context),
            Arc::new(Semaphore::new(4)),
            4,
            Arc::new(Semaphore::new(METRICS_MAX_CONNECTIONS)),
        ));

        let (status, _) = http_get(&addr, "/readyz").await;
        assert_eq!(status, "HTTP/1.1 200 OK");
        drain_tx.send(true).unwrap();
        let (status, body) = http_get(&addr, "/readyz").await;
        assert_eq!(status, "HTTP/1.1 503 Service Unavailable");
        assert!(body.contains("draining"), "{body}");
    }

    #[tokio::test(flavor = "current_thread")]
    async fn a_write_reaches_all_replicas_before_the_ack() {
        let (nodes, proxy) = cluster(2).await;
        let (mut stream, mut buf) = connect_and_auth(&proxy).await;

        stream.write_all(b"S 1 1\nkv").await.unwrap();
        assert_eq!(read_line(&mut stream, &mut buf).await.unwrap(), "S");

        // Both owners hold the value the moment the ack is read.
        assert_eq!(nodes[0].entry(b"", b"k"), Some(b"v".to_vec()));
        assert_eq!(nodes[1].entry(b"", b"k"), Some(b"v".to_vec()));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn incr_through_the_proxy_returns_the_new_value() {
        // Issue #129: `i` has no uppercase legacy form, so even an
        // unnamespaced key carries an explicit `<ns-len>` of 0.
        let (_nodes, proxy) = cluster(1).await;
        let (mut stream, mut buf) = connect_and_auth(&proxy).await;

        stream.write_all(b"S 1 2\nk10").await.unwrap();
        assert_eq!(read_line(&mut stream, &mut buf).await.unwrap(), "S");

        stream.write_all(b"i 0 1 5\nk").await.unwrap();
        assert_eq!(read_line(&mut stream, &mut buf).await.unwrap(), "I 2");
        read_exact_into(&mut stream, &mut buf, 2).await.unwrap();
        assert_eq!(&buf.split_to(2)[..], b"15");

        // A missing key answers N, same status G/D would.
        stream.write_all(b"i 0 7 1\nmissing").await.unwrap();
        assert_eq!(read_line(&mut stream, &mut buf).await.unwrap(), "N");
    }

    #[tokio::test(flavor = "current_thread")]
    async fn an_incr_results_fan_out_never_replays_the_increment_on_a_replica() {
        // With R=2 over two nodes, a naive fan-out that resent the `i`
        // frame itself would still land on the right *value* here (both
        // owners start from the same seed) — the real proof is that the
        // replica's `incrs` counter stays 0: only the primary ever
        // receives an `i` frame, the replica only ever sees the result as
        // a plain `Set` (see `finish_incr`/`fan_out_write_result`).
        let (nodes, proxy) = cluster(2).await;
        let (mut stream, mut buf) = connect_and_auth(&proxy).await;

        let ring = RingView::new(
            vec![
                ("node-a".to_string(), nodes[0].addr.clone()),
                ("node-b".to_string(), nodes[1].addr.clone()),
            ],
            2,
        );
        let primary_addr = ring.owners(b"", b"counter")[0].clone();
        let (primary, replica) = if primary_addr == nodes[0].addr {
            (&nodes[0], &nodes[1])
        } else {
            (&nodes[1], &nodes[0])
        };

        stream.write_all(b"S 7 2\ncounter10").await.unwrap();
        assert_eq!(read_line(&mut stream, &mut buf).await.unwrap(), "S");

        stream.write_all(b"i 0 7 5\ncounter").await.unwrap();
        assert_eq!(read_line(&mut stream, &mut buf).await.unwrap(), "I 2");
        read_exact_into(&mut stream, &mut buf, 2).await.unwrap();
        assert_eq!(&buf.split_to(2)[..], b"15");

        assert_eq!(primary.incrs(), 1);
        assert_eq!(replica.incrs(), 0);
        assert_eq!(primary.entry(b"", b"counter"), Some(b"15".to_vec()));
        assert_eq!(replica.entry(b"", b"counter"), Some(b"15".to_vec()));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn incr_never_replays_once_the_primary_may_have_applied_it() {
        // Issue #272: the primary applies the delta but the reply is
        // lost (`close_after_incr_apply_once` — the connection drops
        // after the store update, before the `I` reply is written). A
        // naive retry would resend `i` and double-apply the delta; the
        // fix is to surface an error to the client instead. Proven two
        // ways: the node's `incrs` counter stops at 1 (no replayed `i`
        // frame), and the stored value is `10 + 5`, never `10 + 5 + 5`.
        let (nodes, proxy) = cluster(1).await;
        let node = &nodes[0];
        let (mut stream, mut buf) = connect_and_auth_tagged(&proxy).await;

        stream.write_all(b"S 7 2 1\ncounter10").await.unwrap();
        assert_eq!(read_line(&mut stream, &mut buf).await.unwrap(), "S 1");

        node.close_after_incr_apply_once
            .store(true, Ordering::SeqCst);
        stream.write_all(b"i 0 7 5 2\ncounter").await.unwrap();
        // A tagged, retry-capable client gets the per-request `R` — a
        // successful `I` here (or a bare connection close) would mean
        // the fix regressed: either the double-apply happened silently,
        // or the caller lost the ability to tell "may have been
        // applied" from "definitely wasn't".
        assert_eq!(read_line(&mut stream, &mut buf).await.unwrap(), "R 2");

        assert_eq!(
            node.incrs(),
            1,
            "the lost-reply attempt must not be replayed onto the primary"
        );
        assert_eq!(
            node.entry(b"", b"counter"),
            Some(b"15".to_vec()),
            "the delta must be applied exactly once"
        );

        // The connection survived (retry-capable): a fresh INCR still
        // works and continues from the single applied delta.
        stream.write_all(b"i 0 7 5 3\ncounter").await.unwrap();
        assert_eq!(read_line(&mut stream, &mut buf).await.unwrap(), "I 2 3");
        read_exact_into(&mut stream, &mut buf, 2).await.unwrap();
        assert_eq!(&buf.split_to(2)[..], b"20");
    }

    #[tokio::test(flavor = "current_thread")]
    async fn incr_on_a_non_numeric_value_answers_t() {
        let (_nodes, proxy) = cluster(1).await;
        let (mut stream, mut buf) = connect_and_auth(&proxy).await;

        stream.write_all(b"S 4 5\nnameAlice").await.unwrap();
        assert_eq!(read_line(&mut stream, &mut buf).await.unwrap(), "S");

        stream.write_all(b"i 0 4 1\nname").await.unwrap();
        assert_eq!(read_line(&mut stream, &mut buf).await.unwrap(), "T");
    }

    #[tokio::test(flavor = "current_thread")]
    async fn cas_set_through_the_proxy_succeeds_on_absent_and_fails_on_present() {
        // R=2 over the two-node roster so *both* nodes always own the
        // key, regardless of which one the ring's hash picks as primary
        // for "k" — same reasoning as `a_write_reaches_all_replicas_before_the_ack`.
        let (nodes, proxy) = cluster(2).await;
        let (mut stream, mut buf) = connect_and_auth(&proxy).await;

        // Absent-conditioned set succeeds on a missing key.
        stream.write_all(b"k 0 1 5 A\nkAlice").await.unwrap();
        assert_eq!(read_line(&mut stream, &mut buf).await.unwrap(), "S");

        // The same condition now fails — the key exists.
        stream.write_all(b"k 0 1 3 A\nkBob").await.unwrap();
        assert_eq!(read_line(&mut stream, &mut buf).await.unwrap(), "N");
        assert_eq!(nodes[0].entry(b"", b"k"), Some(b"Alice".to_vec()));

        // Present-conditioned set succeeds now that the key exists.
        stream.write_all(b"k 0 1 3 P\nkBob").await.unwrap();
        assert_eq!(read_line(&mut stream, &mut buf).await.unwrap(), "S");
        assert_eq!(nodes[0].entry(b"", b"k"), Some(b"Bob".to_vec()));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn cas_set_with_a_digest_condition_replaces_only_on_an_exact_match() {
        let (nodes, proxy) = cluster(2).await;
        let (mut stream, mut buf) = connect_and_auth(&proxy).await;

        stream.write_all(b"S 4 5\nnameAlice").await.unwrap();
        assert_eq!(read_line(&mut stream, &mut buf).await.unwrap(), "S");

        let stale = mock_content_digest(b"someone-else");
        stream
            .write_all(format!("k 0 4 3 {stale}\nnameBob").as_bytes())
            .await
            .unwrap();
        assert_eq!(read_line(&mut stream, &mut buf).await.unwrap(), "N");
        assert_eq!(nodes[0].entry(b"", b"name"), Some(b"Alice".to_vec()));

        let current = mock_content_digest(b"Alice");
        stream
            .write_all(format!("k 0 4 3 {current}\nnameBob").as_bytes())
            .await
            .unwrap();
        assert_eq!(read_line(&mut stream, &mut buf).await.unwrap(), "S");
        assert_eq!(nodes[0].entry(b"", b"name"), Some(b"Bob".to_vec()));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn cas_delete_through_the_proxy_removes_only_on_a_matching_digest() {
        let (nodes, proxy) = cluster(2).await;
        let (mut stream, mut buf) = connect_and_auth(&proxy).await;

        stream.write_all(b"S 4 5\nnameAlice").await.unwrap();
        assert_eq!(read_line(&mut stream, &mut buf).await.unwrap(), "S");

        let stale = mock_content_digest(b"someone-else");
        stream
            .write_all(format!("x 0 4 {stale}\nname").as_bytes())
            .await
            .unwrap();
        assert_eq!(read_line(&mut stream, &mut buf).await.unwrap(), "N");
        assert_eq!(nodes[0].entry(b"", b"name"), Some(b"Alice".to_vec()));

        let current = mock_content_digest(b"Alice");
        stream
            .write_all(format!("x 0 4 {current}\nname").as_bytes())
            .await
            .unwrap();
        assert_eq!(read_line(&mut stream, &mut buf).await.unwrap(), "D");
        assert_eq!(nodes[0].entry(b"", b"name"), None);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn a_cas_results_fan_out_never_replays_the_operation_on_a_replica() {
        // Same proof as `an_incr_results_fan_out_never_replays_the_increment_on_a_replica`,
        // for `k`/`x`: a naive fan-out that resent the operation itself
        // would still land on the right value here (both owners start
        // from the same seed) — the real proof is that the replica's
        // `cas_sets`/`cas_deletes` counters stay 0.
        let (nodes, proxy) = cluster(2).await;
        let (mut stream, mut buf) = connect_and_auth(&proxy).await;

        let ring = RingView::new(
            vec![
                ("node-a".to_string(), nodes[0].addr.clone()),
                ("node-b".to_string(), nodes[1].addr.clone()),
            ],
            2,
        );
        let primary_addr = ring.owners(b"", b"name")[0].clone();
        let (primary, replica) = if primary_addr == nodes[0].addr {
            (&nodes[0], &nodes[1])
        } else {
            (&nodes[1], &nodes[0])
        };

        stream.write_all(b"S 4 5\nnameAlice").await.unwrap();
        assert_eq!(read_line(&mut stream, &mut buf).await.unwrap(), "S");

        let alice_digest = mock_content_digest(b"Alice");
        stream
            .write_all(format!("k 0 4 3 {alice_digest}\nnameBob").as_bytes())
            .await
            .unwrap();
        assert_eq!(read_line(&mut stream, &mut buf).await.unwrap(), "S");

        assert_eq!(primary.cas_sets(), 1);
        assert_eq!(replica.cas_sets(), 0);
        assert_eq!(primary.entry(b"", b"name"), Some(b"Bob".to_vec()));
        assert_eq!(replica.entry(b"", b"name"), Some(b"Bob".to_vec()));

        let bob_digest = mock_content_digest(b"Bob");
        stream
            .write_all(format!("x 0 4 {bob_digest}\nname").as_bytes())
            .await
            .unwrap();
        assert_eq!(read_line(&mut stream, &mut buf).await.unwrap(), "D");

        assert_eq!(primary.cas_deletes(), 1);
        assert_eq!(replica.cas_deletes(), 0);
        assert_eq!(primary.entry(b"", b"name"), None);
        assert_eq!(replica.entry(b"", b"name"), None);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn cas_set_never_replays_once_the_primary_may_have_applied_it() {
        // Issue #293: same proof as
        // `incr_never_replays_once_the_primary_may_have_applied_it` (#272),
        // for `k`. The primary applies the CAS but the reply is lost
        // (`close_after_cas_set_apply_once` — the connection drops after
        // the store update, before the `S` reply is written). A naive
        // retry would resend `k` and could double-apply the operation
        // against a condition that has since changed; the fix surfaces
        // an error to the client instead. Proven two ways: the node's
        // `cas_sets` counter stops at 1 (no replayed `k` frame), and the
        // stored value reflects exactly one application.
        // R=1: only one of the two nodes owns "name" — determine which
        // (same reasoning as `a_cas_results_fan_out_never_replays_the_operation_on_a_replica`)
        // so the close-after-apply knob is armed on whichever one the
        // ring actually routes the CAS to.
        let (nodes, proxy) = cluster(1).await;
        let ring = RingView::new(
            vec![
                ("node-a".to_string(), nodes[0].addr.clone()),
                ("node-b".to_string(), nodes[1].addr.clone()),
            ],
            1,
        );
        let primary_addr = ring.owners(b"", b"name")[0].clone();
        let node = if primary_addr == nodes[0].addr {
            &nodes[0]
        } else {
            &nodes[1]
        };
        let (mut stream, mut buf) = connect_and_auth_tagged(&proxy).await;

        stream.write_all(b"S 4 5 1\nnameAlice").await.unwrap();
        assert_eq!(read_line(&mut stream, &mut buf).await.unwrap(), "S 1");

        let alice_digest = mock_content_digest(b"Alice");
        node.close_after_cas_set_apply_once
            .store(true, Ordering::SeqCst);
        stream
            .write_all(format!("k 0 4 3 {alice_digest} 2\nnameBob").as_bytes())
            .await
            .unwrap();
        // A tagged, retry-capable client gets the per-request `R` — a
        // successful `S` here (or a bare connection close) would mean
        // the fix regressed: either the double-apply happened silently,
        // or the caller lost the ability to tell "may have been
        // applied" from "definitely wasn't".
        assert_eq!(read_line(&mut stream, &mut buf).await.unwrap(), "R 2");

        assert_eq!(
            node.cas_sets(),
            1,
            "the lost-reply attempt must not be replayed onto the primary"
        );
        assert_eq!(
            node.entry(b"", b"name"),
            Some(b"Bob".to_vec()),
            "the CAS must be applied exactly once"
        );

        // The connection survived (retry-capable): a fresh CAS still
        // works and observes the single applied write.
        let bob_digest = mock_content_digest(b"Bob");
        stream
            .write_all(format!("k 0 4 5 {bob_digest} 3\nnameCarol").as_bytes())
            .await
            .unwrap();
        assert_eq!(read_line(&mut stream, &mut buf).await.unwrap(), "S 3");
        assert_eq!(node.entry(b"", b"name"), Some(b"Carol".to_vec()));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn cas_delete_never_replays_once_the_primary_may_have_applied_it() {
        // Same proof as `cas_set_never_replays_once_the_primary_may_have_applied_it`,
        // for `x`.
        let (nodes, proxy) = cluster(1).await;
        let ring = RingView::new(
            vec![
                ("node-a".to_string(), nodes[0].addr.clone()),
                ("node-b".to_string(), nodes[1].addr.clone()),
            ],
            1,
        );
        let primary_addr = ring.owners(b"", b"name")[0].clone();
        let node = if primary_addr == nodes[0].addr {
            &nodes[0]
        } else {
            &nodes[1]
        };
        let (mut stream, mut buf) = connect_and_auth_tagged(&proxy).await;

        stream.write_all(b"S 4 5 1\nnameAlice").await.unwrap();
        assert_eq!(read_line(&mut stream, &mut buf).await.unwrap(), "S 1");

        let alice_digest = mock_content_digest(b"Alice");
        node.close_after_cas_delete_apply_once
            .store(true, Ordering::SeqCst);
        stream
            .write_all(format!("x 0 4 {alice_digest} 2\nname").as_bytes())
            .await
            .unwrap();
        assert_eq!(read_line(&mut stream, &mut buf).await.unwrap(), "R 2");

        assert_eq!(
            node.cas_deletes(),
            1,
            "the lost-reply attempt must not be replayed onto the primary"
        );
        assert_eq!(
            node.entry(b"", b"name"),
            None,
            "the delete must be applied exactly once"
        );

        // The connection survived (retry-capable): a fresh write/delete
        // cycle still works.
        stream.write_all(b"S 4 3 3\nnameBob").await.unwrap();
        assert_eq!(read_line(&mut stream, &mut buf).await.unwrap(), "S 3");
        let bob_digest = mock_content_digest(b"Bob");
        stream
            .write_all(format!("x 0 4 {bob_digest} 4\nname").as_bytes())
            .await
            .unwrap();
        assert_eq!(read_line(&mut stream, &mut buf).await.unwrap(), "D 4");
        assert_eq!(node.entry(b"", b"name"), None);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn cas_set_retries_in_full_when_the_request_was_never_sent() {
        // Issue #293: the flip side of
        // `cas_set_never_replays_once_the_primary_may_have_applied_it`
        // above — when `pending` fails with the `request_not_sent`
        // marker (the frame provably never reached the backend's
        // socket, e.g. `enqueue`'s own dial-backoff fast-fail),
        // `finish_cas_set` must still retry the whole CAS via
        // `refan_cas_set`, exactly as `finish_incr` does for #272.
        // Driving `finish_cas_set` directly with a synthetic
        // not-sent failure isolates that guard deterministically —
        // proven by the retry reaching the real node and landing the
        // write.
        let node = MockNode::start().await;
        let ring = Arc::new(RingView::new(
            vec![("node-a".to_string(), node.addr.clone())],
            1,
        ));
        let context = ProxyContext {
            secret: None,
            tls_connector: None,
            ring: watch::channel(Some(ring)).1,
            refresh_now: mpsc::channel(4).0,
            drain: watch::channel(false).1,
            backends: Arc::new(SharedBackends::new()),
            requests_total: std::sync::atomic::AtomicU64::new(0),
            upstream_failures_total: std::sync::atomic::AtomicU64::new(0),
        };

        let pending: PendingReply = PendingReply::failed(not_sent(io::Error::other(
            "simulated: never reached the wire",
        )));
        let outcome = finish_cas_set(
            &context,
            (b"", b"name"),
            (CasCondition::Absent, b"Alice", None),
            vec![node.addr.clone()],
            pending,
            None,
            false,
        )
        .await;

        match outcome {
            Ok(bytes) => assert_eq!(bytes, respond("S", None)),
            Err(_) => {
                panic!("a request_not_sent failure must still retry the CAS, not surface Fatal")
            }
        }
        assert_eq!(node.entry(b"", b"name"), Some(b"Alice".to_vec()));
        assert_eq!(
            node.cas_sets(),
            1,
            "exactly one `k` frame must reach the node — the retry, not a replay"
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn cas_delete_retries_in_full_when_the_request_was_never_sent() {
        // Same proof as `cas_set_retries_in_full_when_the_request_was_never_sent`,
        // for `x`/`finish_cas_delete`.
        let node = MockNode::start().await;
        let ring = Arc::new(RingView::new(
            vec![("node-a".to_string(), node.addr.clone())],
            1,
        ));
        let context = ProxyContext {
            secret: None,
            tls_connector: None,
            ring: watch::channel(Some(ring)).1,
            refresh_now: mpsc::channel(4).0,
            drain: watch::channel(false).1,
            backends: Arc::new(SharedBackends::new()),
            requests_total: std::sync::atomic::AtomicU64::new(0),
            upstream_failures_total: std::sync::atomic::AtomicU64::new(0),
        };

        // Seed the node directly so the CAS delete's digest condition
        // holds once the retry reaches it.
        node.store
            .lock()
            .unwrap()
            .insert((b"".to_vec(), b"name".to_vec()), b"Alice".to_vec());
        let expected_digest = {
            use sha2::{Digest, Sha256};
            let hash = Sha256::digest(b"Alice");
            let mut digest = [0u8; 16];
            digest.copy_from_slice(&hash[..16]);
            digest
        };

        let pending: PendingReply = PendingReply::failed(not_sent(io::Error::other(
            "simulated: never reached the wire",
        )));
        let outcome = finish_cas_delete(
            &context,
            (b"", b"name"),
            expected_digest,
            vec![node.addr.clone()],
            pending,
            None,
            false,
        )
        .await;

        match outcome {
            Ok(bytes) => assert_eq!(bytes, respond("D", None)),
            Err(_) => panic!(
                "a request_not_sent failure must still retry the CAS delete, not surface Fatal"
            ),
        }
        assert_eq!(node.entry(b"", b"name"), None);
        assert_eq!(
            node.cas_deletes(),
            1,
            "exactly one `x` frame must reach the node — the retry, not a replay"
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn refan_cas_set_never_replays_once_the_primary_may_have_applied_it() {
        // Issue #322: `refan_cas_set` used to retry its own primary leg
        // via the unconditionally-retrying `call` — safe for
        // `Set`/`Delete`, but `k` is exactly as non-idempotent here as it
        // is on `finish_cas_set`'s first attempt (#293). Arms
        // `close_after_cas_set_apply_once` on the node `refan_cas_set`
        // itself dispatches to, proving the fix (`call_non_idempotent`)
        // stops retrying a `k` that may have reached the primary instead
        // of resending it and re-evaluating (and possibly misreporting)
        // the condition against a value the first attempt already wrote.
        let node = MockNode::start().await;
        let ring = Arc::new(RingView::new(
            vec![("node-a".to_string(), node.addr.clone())],
            1,
        ));
        let context = ProxyContext {
            secret: None,
            tls_connector: None,
            ring: watch::channel(Some(ring)).1,
            refresh_now: mpsc::channel(4).0,
            drain: watch::channel(false).1,
            backends: Arc::new(SharedBackends::new()),
            requests_total: std::sync::atomic::AtomicU64::new(0),
            upstream_failures_total: std::sync::atomic::AtomicU64::new(0),
        };

        node.close_after_cas_set_apply_once
            .store(true, Ordering::SeqCst);
        let outcome = refan_cas_set(
            &context,
            (b"", b"name"),
            (CasCondition::Absent, b"Alice", None),
            None,
            false,
        )
        .await;

        assert!(
            outcome.is_err(),
            "a lost reply after the refanned `k` may have applied must surface Fatal, not the answer to a replayed frame"
        );
        assert_eq!(
            node.cas_sets(),
            1,
            "the lost-reply refan attempt must not be replayed onto the primary"
        );
        assert_eq!(
            node.entry(b"", b"name"),
            Some(b"Alice".to_vec()),
            "the CAS must be applied exactly once"
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn refan_cas_delete_never_replays_once_the_primary_may_have_applied_it() {
        // Same proof as
        // `refan_cas_set_never_replays_once_the_primary_may_have_applied_it`,
        // for `refan_cas_delete`/`x` (issue #322).
        let node = MockNode::start().await;
        let ring = Arc::new(RingView::new(
            vec![("node-a".to_string(), node.addr.clone())],
            1,
        ));
        let context = ProxyContext {
            secret: None,
            tls_connector: None,
            ring: watch::channel(Some(ring)).1,
            refresh_now: mpsc::channel(4).0,
            drain: watch::channel(false).1,
            backends: Arc::new(SharedBackends::new()),
            requests_total: std::sync::atomic::AtomicU64::new(0),
            upstream_failures_total: std::sync::atomic::AtomicU64::new(0),
        };

        node.store
            .lock()
            .unwrap()
            .insert((b"".to_vec(), b"name".to_vec()), b"Alice".to_vec());
        let expected_digest = {
            use sha2::{Digest, Sha256};
            let hash = Sha256::digest(b"Alice");
            let mut digest = [0u8; 16];
            digest.copy_from_slice(&hash[..16]);
            digest
        };

        node.close_after_cas_delete_apply_once
            .store(true, Ordering::SeqCst);
        let outcome =
            refan_cas_delete(&context, (b"", b"name"), expected_digest, None, false).await;

        assert!(
            outcome.is_err(),
            "a lost reply after the refanned `x` may have applied must surface Fatal, not the answer to a replayed frame"
        );
        assert_eq!(
            node.cas_deletes(),
            1,
            "the lost-reply refan attempt must not be replayed onto the primary"
        );
        assert_eq!(
            node.entry(b"", b"name"),
            None,
            "the delete must be applied exactly once"
        );
    }

    // ── issue #177: concurrent fan-out, dial backoff, failed-primary skip ──

    #[tokio::test(flavor = "current_thread")]
    async fn two_black_holed_owners_do_not_serialize_the_write_fan_out() {
        // Before issue #177's fix, `enqueue_write` dialed each owner one
        // at a time (`for addr in &owners { backends.enqueue(..).await
        // }`), so two black-holed owners in the same write cost two
        // full `UPSTREAM_IO_TIMEOUT`s back to back. Concurrent fan-out
        // bounds the whole batch to about one.
        let healthy = MockNode::start().await;
        let black_hole_a = start_black_hole_node().await;
        let black_hole_b = start_black_hole_node().await;
        let ring = RingView::new(
            vec![
                ("healthy".to_string(), healthy.addr.clone()),
                ("black-a".to_string(), black_hole_a),
                ("black-b".to_string(), black_hole_b),
            ],
            3,
        );
        let context = bare_context();
        let value = Bytes::from_static(b"value");

        let start = std::time::Instant::now();
        let pending = enqueue_write(&context, &ring, b"", b"key", Some((&value, None))).await;
        let elapsed = start.elapsed();

        assert_eq!(pending.len(), 3);
        // At least one timeout really elapsed (rules out a false pass
        // from something returning instantly)...
        assert!(elapsed >= UPSTREAM_IO_TIMEOUT, "elapsed {elapsed:?}");
        // ...but nowhere near two, which is what the sequential bug
        // would have cost with two black-holed owners.
        assert!(
            elapsed < UPSTREAM_IO_TIMEOUT + UPSTREAM_IO_TIMEOUT / 2,
            "elapsed {elapsed:?} looks serialized, not concurrent"
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn a_second_enqueue_to_a_recently_failed_address_fails_fast() {
        // Issue #177: `enqueue` used to re-dial inline with no memory of
        // a prior failure, so a black-holed address cost every caller
        // its own full `UPSTREAM_IO_TIMEOUT`. Within `DIAL_BACKOFF` of
        // the first failure, a second attempt should fail immediately.
        let black_hole = start_black_hole_node().await;
        let context = bare_context();
        let frame = frame_get(b"", b"key");

        let start = std::time::Instant::now();
        let first = context
            .backends
            .enqueue(&context, &black_hole, frame.clone(), Expect::Value)
            .await
            .await;
        let first_elapsed = start.elapsed();
        assert!(first.is_err());
        assert!(
            first_elapsed >= UPSTREAM_IO_TIMEOUT,
            "the first dial should pay the full timeout: {first_elapsed:?}"
        );

        let start = std::time::Instant::now();
        let second = context
            .backends
            .enqueue(&context, &black_hole, frame, Expect::Value)
            .await
            .await;
        let second_elapsed = start.elapsed();
        assert!(second.is_err());
        assert!(
            second_elapsed < DIAL_BACKOFF,
            "the second attempt should fail fast from the backoff window: {second_elapsed:?}"
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn a_get_with_a_dead_primary_falls_over_to_the_replica_within_one_timeout() {
        // Issue #177: `retry_get_on` used to retry the primary that just
        // failed *before* trying any replica, so a black-holed primary
        // could cost up to three timeouts before a live replica ever
        // answered. It should now cost about one.
        let replica = MockNode::start().await;
        replica
            .store
            .lock()
            .unwrap()
            .insert((Vec::new(), b"key".to_vec()), b"value".to_vec());
        let dead_primary = start_black_hole_node().await;
        let context = bare_context();
        let owners = vec![dead_primary.clone(), replica.addr.clone()];

        let start = std::time::Instant::now();
        let pending = context
            .backends
            .enqueue(
                &context,
                &dead_primary,
                frame_get(b"", b"key"),
                Expect::Value,
            )
            .await;
        let result = finish_get(&context, b"", b"key", owners, pending, None).await;
        let elapsed = start.elapsed();

        match result {
            Ok(reply) => assert_eq!(reply, respond_value(b"value", None)),
            Err(Fatal) => panic!("expected the replica's value, got a fatal error"),
        }
        assert!(
            elapsed < UPSTREAM_IO_TIMEOUT + UPSTREAM_IO_TIMEOUT / 2,
            "elapsed {elapsed:?} suggests the dead primary was retried before the replica"
        );
    }

    // ── issue #420: bounded writes to backends and clients ──────────────

    #[tokio::test(flavor = "current_thread")]
    async fn a_stalled_backend_write_is_bounded_and_frees_the_dialed_gauge() {
        // Before the fix, `write_half.write_all` in `run_backend`'s
        // writer had no timeout, unlike every other I/O site against this
        // connection. A node that completes the handshake and then just
        // never reads again would park this write forever:
        // `poisoned.notified()` is only checked between requests, not
        // while a write is in flight, so nothing would ever bring the
        // writer back around to notice the reader poisoning on its own
        // per-reply timeout. The task, its socket, and the `dialed` gauge
        // would leak permanently, and the slot would keep pointing at the
        // dead handle.
        let silent = start_silent_after_handshake_node().await;
        let context = bare_context();
        // Comfortably larger than any plausible default OS socket buffer
        // pair on an unread loopback connection, so the write reliably
        // blocks instead of racing a platform-specific buffer size.
        let big_value = vec![b'x'; 5_000_000];

        // Deliberately not awaited: `run_backend`'s writer stalls trying
        // to deliver this to `silent`, and the point of this test is what
        // happens to the connection's own bookkeeping while that reply
        // never arrives — not the reply itself.
        let _pending = context
            .backends
            .enqueue(
                &context,
                &silent,
                frame_set(b"", b"key", &big_value, None),
                Expect::Stored,
            )
            .await;

        assert_eq!(
            context.backends.dialed.load(Ordering::SeqCst),
            1,
            "the dial should have succeeded before the write ever stalls"
        );

        // Without the fix this spins until the outer timeout below fires
        // and fails the test: the writer task never reaches its
        // teardown, so `dialed` never drops back to 0.
        timeout(UPSTREAM_IO_TIMEOUT * 10, async {
            while context.backends.dialed.load(Ordering::SeqCst) != 0 {
                sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("the stalled writer must time out and release its dialed-gauge slot");
    }

    #[tokio::test(flavor = "current_thread")]
    async fn a_client_that_stops_reading_does_not_block_the_writer_forever() {
        // Before the fix, `write_half.write_all` in `handle_client`'s
        // writer had no timeout either. A client that stops reading its
        // socket would park that write forever — and `handle_client`'s
        // own teardown (`drop(fifo_tx); writer.await`) would then hang
        // right along with it, so `handle_client` never returns and its
        // `max_connections` permit (acquired in `serve`) is never
        // released. With `max_connections` set to 1, one such client
        // should permanently starve every later connection without the
        // fix.
        let node = MockNode::start().await;
        // As large as a value reply is allowed to be (`read_reply` in
        // this same module rejects a `V` length over `MAX_REQUEST_SIZE`
        // as a protocol violation).
        let big_value = vec![b'x'; MAX_REQUEST_SIZE - 1024];
        node.store
            .lock()
            .unwrap()
            .insert((Vec::new(), b"key".to_vec()), big_value);
        let roster = vec![("node".to_string(), node.addr.clone())];
        let discovery = start_mock_discovery(roster, 1).await;
        let proxy = start_proxy(&discovery, None, 1).await;

        // Occupies the sole connection permit, then pipelines
        // `REQUEST_COUNT` requests for the huge value and never reads
        // any response. A *single* ~1MiB reply is not reliably enough to
        // block the writer on every platform: Linux loopback sockets
        // autotune their buffers to several MiB regardless of this
        // client's shrunk receive buffer below, so one reply can be
        // handed entirely to the kernel and "complete" without the
        // writer ever truly blocking (this is exactly what happened in
        // CI — see the "verify on Linux before pushing" lesson).
        // Pipelining enough of these (well under `CLIENT_IN_FLIGHT`)
        // queues tens of MiB of unread reply data, comfortably beyond
        // any realistic combination of sender/receiver buffer sizes on
        // any platform, so some write in the sequence must eventually
        // block for real. The shrunk receive buffer is kept too, since
        // it only helps make the block happen sooner.
        const REQUEST_COUNT: usize = 64;
        let socket = tokio::net::TcpSocket::new_v4().unwrap();
        socket.set_recv_buffer_size(4096).unwrap();
        let mut stalled = socket.connect(proxy.parse().unwrap()).await.unwrap();
        let mut buf = BytesMut::new();
        stalled.write_all(b"A 1\nx").await.unwrap();
        assert_eq!(read_line(&mut stalled, &mut buf).await.unwrap(), "On");
        for _ in 0..REQUEST_COUNT {
            stalled.write_all(b"G 3\nkey").await.unwrap();
        }
        // Half-closing the write side afterward (a TCP FIN, not a full
        // close) lets `handle_client`'s *reader* observe EOF, once it
        // has read every pipelined request above, and reach its
        // `drop(fifo_tx); writer.await` teardown quickly instead of
        // idling for the full, real (not shrunk-under-test)
        // `IDLE_TIMEOUT` — the read direction, and so the still-unread
        // receive buffer the writer is stalled on, is untouched by this.
        stalled.shutdown().await.unwrap();

        // While the writer is stalled, the proxy is over its connection
        // budget: a fresh connection is answered `B` and closed
        // immediately — the existing, unrelated `max_connections`
        // behavior, not itself proof of the fix.
        let mut busy = TcpStream::connect(&proxy).await.unwrap();
        let mut busy_buf = BytesMut::new();
        assert_eq!(read_line(&mut busy, &mut busy_buf).await.unwrap(), "B");

        // The actual proof: once the stalled writer's bounded timeout
        // fires, `handle_client` tears the connection down and releases
        // its permit, letting a fresh connection through. Without the
        // fix this loop never succeeds and the outer timeout fails the
        // test instead of hanging the suite forever.
        let start = std::time::Instant::now();
        timeout(UPSTREAM_IO_TIMEOUT * 10, async {
            loop {
                let mut probe = TcpStream::connect(&proxy).await.unwrap();
                let mut probe_buf = BytesMut::new();
                probe.write_all(b"A 1\nx").await.unwrap();
                match read_line(&mut probe, &mut probe_buf).await.as_deref() {
                    Ok("On") => return,
                    _ => sleep(Duration::from_millis(10)).await,
                }
            }
        })
        .await
        .expect("the stalled client's permit must be released once its writer times out");
        // Confirms the permit was released *because* the write timeout
        // actually fired, not because the write happened to finish
        // instantly (which would make this test pass for the wrong
        // reason even without the fix).
        assert!(
            start.elapsed() >= CLIENT_WRITE_TIMEOUT,
            "elapsed {:?} is suspiciously fast for a write that was supposed to stall",
            start.elapsed()
        );
    }

    // ── Tenth-pass audit (2026-09-02): bound the TLS handshake and the
    // over-budget busy reply ────────────────────────────────────────────

    #[tokio::test(flavor = "current_thread")]
    async fn stalled_tls_handshake_releases_the_connection_permit() {
        // Before the fix, `acceptor.accept` in `serve`'s spawned
        // per-connection task had no timeout. A peer that completes the
        // TCP handshake and then never sends a ClientHello held its
        // `max_connections` permit (acquired before the handshake, same
        // as the node) forever — starving every later client. With
        // `max_connections` set to 1, one such peer should permanently
        // block every later connection without the fix.
        let (acceptor, connector) = self_signed_tls();
        let discovery = start_mock_discovery(Vec::new(), 1).await;
        let proxy = start_tls_proxy(&discovery, 1, acceptor).await;

        // Occupies the sole connection permit with a raw TCP connection
        // that never sends a ClientHello.
        let start = std::time::Instant::now();
        let stalled = TcpStream::connect(&proxy).await.unwrap();

        // The actual proof: once the stalled handshake's bounded timeout
        // fires, `serve` drops the task and its permit, letting a
        // well-behaved TLS client complete a handshake of its own.
        // Without the fix this loop never succeeds and the outer timeout
        // fails the test instead of hanging the suite forever.
        timeout(TLS_HANDSHAKE_TIMEOUT * 10, async {
            loop {
                let Ok(tcp) = TcpStream::connect(&proxy).await else {
                    sleep(Duration::from_millis(10)).await;
                    continue;
                };
                let server_name = ServerName::try_from("localhost").unwrap();
                if connector.clone().connect(server_name, tcp).await.is_ok() {
                    return;
                }
                sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("the stalled peer's permit must be released once the TLS handshake times out");
        // Confirms the permit was released *because* the handshake
        // timeout actually fired, not because the second dial happened
        // to win a race (which would make this test pass for the wrong
        // reason even without the fix).
        assert!(
            start.elapsed() >= TLS_HANDSHAKE_TIMEOUT,
            "elapsed {:?} is suspiciously fast for a handshake that was supposed to stall",
            start.elapsed()
        );
        drop(stalled);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn busy_reply_is_skipped_entirely_when_tls_is_configured() {
        // The node's own `reject_over_limit` never writes a plaintext
        // busy reply once TLS is configured — the peer is expecting a
        // TLS ServerHello, so a plaintext `B\n` is meaningless. The proxy
        // must mirror that: over budget, with TLS on, the connection is
        // just dropped rather than answered in plaintext.
        let (acceptor, _connector) = self_signed_tls();
        let discovery = start_mock_discovery(Vec::new(), 1).await;
        // A permit is held for the whole test, so every dial below is
        // over budget.
        let proxy = start_tls_proxy(&discovery, 1, acceptor).await;
        let _held = TcpStream::connect(&proxy).await.unwrap();

        let mut over_budget = TcpStream::connect(&proxy).await.unwrap();
        let mut buf = [0_u8; 8];
        // No plaintext bytes are ever sent — the connection is closed
        // (EOF) instead of carrying a `B\n` reply.
        let read = timeout(CLIENT_WRITE_TIMEOUT * 2, over_budget.read(&mut buf))
            .await
            .expect("the over-budget connection must close promptly, not hang")
            .unwrap();
        assert_eq!(
            read, 0,
            "expected EOF (no busy reply) once TLS is configured, got {} byte(s)",
            read
        );
    }

    // ── issue #409(b): `fetch_roster` races its replicas ────────────────

    #[tokio::test(flavor = "current_thread")]
    async fn fetch_roster_races_replicas_instead_of_trying_them_in_order() {
        // Before the fix, `fetch_roster` tried discovery replicas one at
        // a time in a `for` loop — the one multi-address fan-out left
        // sequential by the earlier #177 pass (every other fan-out in
        // this module already runs concurrently). A black-holed first
        // replica cost a full `UPSTREAM_IO_TIMEOUT` on every refresh
        // cycle, and delayed `/readyz`/`force_refresh` convergence,
        // before the next (live) replica was even dialed.
        let black_hole = start_black_hole_node().await;
        let live =
            start_mock_discovery(vec![("node".to_string(), "127.0.0.1:1".to_string())], 1).await;

        let start = std::time::Instant::now();
        let result = fetch_roster(&[black_hole, live], &None, &None).await;
        let elapsed = start.elapsed();

        let ring = result.expect("the live replica's roster should still answer");
        assert_eq!(ring.all_addresses(), vec!["127.0.0.1:1".to_string()]);
        // Raced concurrently, the live replica answers almost
        // immediately regardless of where the black hole sits in the
        // list; sequential trial would have cost a full
        // `UPSTREAM_IO_TIMEOUT` first since the black hole is listed
        // *before* the live replica.
        assert!(
            elapsed < UPSTREAM_IO_TIMEOUT / 2,
            "elapsed {elapsed:?} suggests the black-holed replica was tried before the live one, not raced"
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn fetch_roster_falls_back_when_every_replica_but_one_fails() {
        // The race still needs a correct fallback story: if the fastest
        // replicas all fail (refuse, or answer `B` for their startup
        // grace), the one that eventually succeeds must still be
        // returned rather than the race giving up early.
        let refused = "127.0.0.1:1".to_string(); // connection refused: fails fast
        let live =
            start_mock_discovery(vec![("node".to_string(), "127.0.0.1:2".to_string())], 1).await;

        let ring = fetch_roster(&[refused, live], &None, &None)
            .await
            .expect("the live replica's roster should still answer");
        assert_eq!(ring.all_addresses(), vec!["127.0.0.1:2".to_string()]);
    }
}
