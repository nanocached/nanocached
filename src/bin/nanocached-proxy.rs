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
//!   are rejected: the proxy is not a member.
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
//! - Auth: the shared secret (env `NANOCACHED_SECRET`, same as node and
//!   discovery) is required of clients exactly as a node requires it,
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

/// Client connections idle longer than this are closed — the node's own
/// idle policy, mirrored so a proxy hop doesn't change lifecycle
/// expectations (SDK keep-alives flow through and reset it).
const IDLE_TIMEOUT: Duration = Duration::from_secs(60);

/// Every backend/discovery I/O interaction is bounded by this, so one
/// hung upstream can't pin a driver task forever.
const UPSTREAM_IO_TIMEOUT: Duration = Duration::from_secs(5);

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

fn read_auth_secret() -> Option<Bytes> {
    std::env::var("NANOCACHED_SECRET")
        .ok()
        .filter(|secret| !secret.is_empty())
        .map(Bytes::from)
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
     The shared auth secret is read from NANOCACHED_SECRET."
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
    /// node per proxy".
    backends: SharedBackends,
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

/// Fetches `L` from the first answering discovery replica. A `B` (the
/// replica's startup grace) or connect failure tries the next replica.
async fn fetch_roster(
    context_discovery: &[String],
    secret: &Option<Bytes>,
    tls_connector: &Option<TlsConnector>,
) -> io::Result<Arc<RingView>> {
    let mut last_error = io::Error::other("no discovery replicas configured");

    for addr in context_discovery {
        match timeout(
            UPSTREAM_IO_TIMEOUT,
            fetch_roster_from(addr, secret, tls_connector),
        )
        .await
        {
            Ok(Ok(ring)) => return Ok(ring),
            Ok(Err(error)) => last_error = error,
            Err(_) => {
                last_error = io::Error::new(
                    io::ErrorKind::TimedOut,
                    format!("L fetch from {addr} timed out"),
                )
            }
        }
    }

    Err(last_error)
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
        // Re-checked against the drain flag right before sending, so a
        // drain that began mid-cycle can't re-register this proxy.
        if let Some((identity, port)) = &announce {
            for addr in &discovery {
                if *drain.borrow() {
                    break;
                }
                if let Err(error) =
                    announce_to(addr, &secret, &tls_connector, identity, *port).await
                {
                    eprintln!("WARN proxy announce to {addr} failed: {error}");
                }
            }
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
    // keep dialing it.
    if let Some((identity, _)) = &announce {
        for addr in &discovery {
            if let Err(error) = deregister_from(addr, &secret, &tls_connector, identity).await {
                eprintln!("WARN proxy deregister to {addr} failed: {error}");
            }
        }
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
    /// Issue #128 measurement prototype: `m` (batched get). Always
    /// namespaced, same reasoning as `Incr`/CAS — no pre-namespace legacy
    /// form. See `dispatch_request`'s arm for the owner-grouping fan-out.
    MultiGet {
        namespace: Bytes,
        keys: Vec<Bytes>,
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

/// Parses one frame from the front of `input` (untouched on
/// `Incomplete`), mirroring the node's own grammar for the client-facing
/// commands. `M`/`X` and anything unknown error — the proxy is not a
/// cluster member (see the module docs).
fn parse_request(input: &mut BytesMut, tagged: bool) -> io::Result<ParseOutcome> {
    let Some(header_end) = input.iter().position(|byte| *byte == b'\n') else {
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
            Ok(ParseOutcome::Ready(Request::MultiGet { namespace, keys }, tag))
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
    /// Issue #128 measurement prototype: `M <n> <r-1>...<r-n> <tag>` +
    /// concatenated hit values, or `E`. Parsed specially in `read_reply`
    /// (a variable roster, unlike every other reply's fixed field count),
    /// never reaches the generic `(marker, fields)` match there.
    Multi,
}

/// One key's outcome inside a backend's `M` reply (issue #128 measurement
/// prototype) — independent reimplementation of the node's
/// `crate::response::MultiEntry` (this binary shares no modules with the
/// node, see `CasCondition`'s doc comment for the established policy).
#[derive(Debug, Clone, PartialEq)]
enum ProxyMultiEntry {
    Value(Bytes),
    Miss,
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
    /// Issue #128 measurement prototype: `M`'s per-key roster, already
    /// decoded — see `Expect::Multi`.
    Multi(Vec<ProxyMultiEntry>),
    /// `E`, or a reply that doesn't fit `Expect` — the connection is
    /// dropped by the reader either way.
    Error,
}

struct BackendRequest {
    frame: Vec<u8>,
    expect: Expect,
    reply: oneshot::Sender<io::Result<NodeReply>>,
}

/// A live, authenticated, tagged connection to one node, owned by a
/// task: requests are written in arrival order and replies matched
/// FIFO by tag. Dropping the sender ends the task and the connection.
#[derive(Clone)]
struct BackendHandle {
    sender: mpsc::Sender<BackendRequest>,
}

impl BackendHandle {
    async fn connect(
        addr: &str,
        secret: &Option<Bytes>,
        tls_connector: &Option<TlsConnector>,
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
        tokio::spawn(run_backend(stream, buf, receiver, addr.to_string()));
        Ok(Self { sender })
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
    _addr: String,
) {
    let (mut read_half, mut write_half) = tokio::io::split(stream);
    let (pending_tx, mut pending_rx) =
        mpsc::channel::<(u32, Expect, oneshot::Sender<io::Result<NodeReply>>)>(
            MAX_BACKEND_IN_FLIGHT,
        );

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
                return;
            }
        }
    });

    let mut next_tag: u32 = 0;

    while let Some(request) = receiver.recv().await {
        let tag = next_tag;
        next_tag = next_tag.wrapping_add(1);

        let frame = substitute_tag(request.frame, tag);

        // Reserve the reply slot before writing: if the reader is gone
        // (poisoned), this fails and the request errors without touching
        // a desynced stream.
        if pending_tx
            .send((tag, request.expect, request.reply))
            .await
            .is_err()
        {
            break;
        }

        if write_half.write_all(&frame).await.is_err() {
            // The reader will observe the broken stream (or time out)
            // and poison; nothing more to write here.
            break;
        }
    }

    // Queue closed (handle dropped or poisoned): let the reader drain
    // what is still pending, then stop.
    drop(pending_tx);
    let _ = reader.await;
}

/// The `{tag}` placeholder the framers leave in the header, replaced
/// with the connection's own sequence tag at send time (the tag must be
/// chosen by the connection task — requests are queued before their
/// send order, and with it their tag, is known).
const TAG_PLACEHOLDER: &str = "{tag}";

fn substitute_tag(frame: Vec<u8>, tag: u32) -> Vec<u8> {
    let header_end = frame
        .iter()
        .position(|byte| *byte == b'\n')
        .expect("framers always emit a complete header");
    let header =
        String::from_utf8(frame[..header_end].to_vec()).expect("framers always emit ASCII headers");
    let header = header.replace(TAG_PLACEHOLDER, &tag.to_string());
    let mut framed = header.into_bytes();
    framed.push(b'\n');
    framed.extend_from_slice(&frame[header_end + 1..]);
    framed
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
        let (tag_field, rest) = fields.split_last().ok_or_else(|| invalid("malformed M reply"))?;
        if *tag_field != tag.to_string() {
            return Err(invalid(&format!(
                "backend reply tag mismatch: expected {tag}, got {tag_field}"
            )));
        }

        let (count_field, roster) = rest.split_first().ok_or_else(|| invalid("malformed M reply"))?;
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
    );
    if !shape_ok {
        return Err(invalid("backend reply does not fit the request"));
    }

    Ok(reply)
}

// ─── backend frame builders (proxy → node, always tagged) ────────────

fn frame_get(namespace: &[u8], key: &[u8]) -> Vec<u8> {
    let mut frame = if namespace.is_empty() {
        format!("G {} {TAG_PLACEHOLDER}\n", key.len()).into_bytes()
    } else {
        format!("g {} {} {TAG_PLACEHOLDER}\n", namespace.len(), key.len()).into_bytes()
    };
    frame.extend_from_slice(namespace);
    frame.extend_from_slice(key);
    frame
}

/// Issue #128 measurement prototype: one sub-frame per owner, carrying
/// only that owner's slice of the original request's keys — see
/// `dispatch_request`'s `Request::MultiGet` arm.
fn frame_multi_get(namespace: &[u8], keys: &[Bytes]) -> Vec<u8> {
    let key_lengths: String = keys
        .iter()
        .map(|key| format!(" {}", key.len()))
        .collect();
    let mut frame =
        format!("m {} {}{key_lengths} {TAG_PLACEHOLDER}\n", namespace.len(), keys.len())
            .into_bytes();
    frame.extend_from_slice(namespace);
    for key in keys {
        frame.extend_from_slice(key);
    }
    frame
}

fn frame_set(namespace: &[u8], key: &[u8], value: &[u8], ttl: Option<u64>) -> Vec<u8> {
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
    frame
}

fn frame_delete(namespace: &[u8], key: &[u8]) -> Vec<u8> {
    let mut frame = if namespace.is_empty() {
        format!("D {} {TAG_PLACEHOLDER}\n", key.len()).into_bytes()
    } else {
        format!("d {} {} {TAG_PLACEHOLDER}\n", namespace.len(), key.len()).into_bytes()
    };
    frame.extend_from_slice(namespace);
    frame.extend_from_slice(key);
    frame
}

/// Issue #129: always the lowercase, namespaced `i` — `INCR` has no
/// uppercase legacy form (see `Request::Incr`'s doc comment).
fn frame_incr(namespace: &[u8], key: &[u8], delta: i64) -> Vec<u8> {
    let mut frame = format!(
        "i {} {} {delta} {TAG_PLACEHOLDER}\n",
        namespace.len(),
        key.len()
    )
    .into_bytes();
    frame.extend_from_slice(namespace);
    frame.extend_from_slice(key);
    frame
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
) -> Vec<u8> {
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
    frame
}

/// Issue #141: always the lowercase, namespaced `x`.
fn frame_cas_delete(namespace: &[u8], key: &[u8], expected_digest: [u8; 16]) -> Vec<u8> {
    let cond = cas_condition_field(CasCondition::Digest(expected_digest));
    let mut frame = format!(
        "x {} {} {cond} {TAG_PLACEHOLDER}\n",
        namespace.len(),
        key.len()
    )
    .into_bytes();
    frame.extend_from_slice(namespace);
    frame.extend_from_slice(key);
    frame
}

fn frame_clear(namespace: &[u8]) -> Vec<u8> {
    let mut frame = format!("c {} {TAG_PLACEHOLDER}\n", namespace.len()).into_bytes();
    frame.extend_from_slice(namespace);
    frame
}

fn frame_clear_all() -> Vec<u8> {
    format!("F {TAG_PLACEHOLDER}\n").into_bytes()
}

// ─── request drivers ─────────────────────────────────────────────────

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
    /// incremented on a successful dial, decremented when a dead handle
    /// is dropped from its slot.
    dialed: std::sync::atomic::AtomicUsize,
}

impl SharedBackends {
    fn new() -> Self {
        Self {
            slots: std::sync::Mutex::new(HashMap::new()),
            dialed: std::sync::atomic::AtomicUsize::new(0),
        }
    }

    fn slot(&self, addr: &str) -> Arc<tokio::sync::Mutex<Option<BackendHandle>>> {
        let mut slots = self
            .slots
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        Arc::clone(slots.entry(addr.to_string()).or_default())
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
        frame: Vec<u8>,
        expect: Expect,
    ) -> PendingReply {
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
                        match BackendHandle::connect(addr, &context.secret, &context.tls_connector)
                            .await
                        {
                            Ok(handle) => {
                                *guard = Some(handle.clone());
                                self.dialed
                                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                                handle
                            }
                            Err(error) => return PendingReply::failed(error),
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
                    reply_rx
                        .await
                        .map_err(|_| io::Error::other("backend connection dropped mid-request"))?
                });
            }

            let mut guard = slot.lock().await;
            if guard
                .as_ref()
                .is_some_and(|current| current.sender.same_channel(&handle.sender))
            {
                *guard = None;
                self.dialed
                    .fetch_sub(1, std::sync::atomic::Ordering::Relaxed);
            }
        }

        PendingReply::failed(io::Error::other("backend connection is gone"))
    }

    /// `enqueue` + await, with one transparent redial when the shared
    /// handle turned out dead — for the retry/fallback paths that run
    /// *after* initial dispatch, where ordering no longer applies.
    async fn call(
        &self,
        context: &ProxyContext,
        addr: &str,
        frame: Vec<u8>,
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
/// or dropped the ordered attempt.
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
        // exactly one key's worth of backend traffic). Primary only, no
        // replica reads — matches `Request::Get`'s own primary-first
        // reads. A failed or `W` sub-batch gets one bounded
        // refresh-and-retry in `finish_multi_get`/`retry_multi_get`,
        // mirroring `finish_get`/`retry_get_on`'s existing shape.
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

            let mut pending = Vec::with_capacity(groups.len());
            for (owner, _, group_keys) in &groups {
                pending.push(
                    context
                        .backends
                        .enqueue(
                            &context,
                            owner,
                            frame_multi_get(&namespace, group_keys),
                            Expect::Multi,
                        )
                        .await,
                );
            }
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

    // The ordered primary attempt failed outright: fall through the
    // remaining owners.
    // The full owner list, primary included: the shared connection may
    // simply have been idle-closed by the node, and `call`'s transparent
    // redial recovers that without failing the client (issue #110 — a
    // long-lived shared connection makes this the common case, not the
    // rare one).
    retry_get_on(context, namespace, key, owners, tag).await
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

/// Issue #150: the one bounded refresh-and-retry pass for keys the first
/// pass left inconclusive — mirrors `finish_get`'s "a single
/// refresh-and-reroute, not a loop." Retried keys are regrouped by their
/// *fresh* top owner (which can differ per key even though they shared a
/// primary before the refresh — a stale ring can be wrong about more
/// than one key's placement at once) and dispatched via `.call`, same
/// transparent-redial reasoning as `retry_get_on`/`refan_write`. Fills
/// every position in `retry_positions` — with a real result or a final
/// `WrongNode` — so the caller never sees a gap.
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

    let mut groups: Vec<(String, Vec<usize>, Vec<Bytes>)> = Vec::new();
    for &position in retry_positions {
        let key = &keys[position];
        let mut owners = ring.owners(namespace, key).into_iter();
        let Some(primary) = owners.next() else {
            entries[position] = Some(ProxyMultiEntry::WrongNode);
            continue;
        };

        if let Some(group) = groups.iter_mut().find(|(owner, ..)| *owner == primary) {
            group.1.push(position);
            group.2.push(key.clone());
        } else {
            groups.push((primary, vec![position], vec![key.clone()]));
        }
    }

    for (owner, group_positions, group_keys) in groups {
        let reply = context
            .backends
            .call(context, &owner, frame_multi_get(namespace, &group_keys), Expect::Multi)
            .await;

        match reply {
            Ok(NodeReply::Multi(results)) if results.len() == group_positions.len() => {
                for (position, entry) in group_positions.into_iter().zip(results) {
                    entries[position] = Some(entry);
                }
            }
            _ => {
                for position in group_positions {
                    entries[position] = Some(ProxyMultiEntry::WrongNode);
                }
            }
        }
    }
}

/// Enqueues a write/delete on every owner, primary first, in one ordered
/// pass; returns the pending replies (primary first).
async fn enqueue_write(
    context: &ProxyContext,
    ring: &RingView,
    namespace: &[u8],
    key: &[u8],
    write: Option<(&Bytes, Option<u64>)>,
) -> Vec<PendingReply> {
    let owners = ring.owners(namespace, key);
    let (frame, expect) = write_frame(namespace, key, write);
    let mut pending = Vec::with_capacity(owners.len());
    for addr in &owners {
        pending.push(
            context
                .backends
                .enqueue(context, addr, frame.clone(), expect)
                .await,
        );
    }
    pending
}

fn write_frame(
    namespace: &[u8],
    key: &[u8],
    write: Option<(&Bytes, Option<u64>)>,
) -> (Vec<u8>, Expect) {
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
    for addr in replicas {
        if let Err(error) = context
            .backends
            .call(context, addr, frame.clone(), expect)
            .await
        {
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
/// node-local migration/decommission case. A primary `W` or transport
/// failure re-runs the whole thing (primary INCR + replica fan-out) on
/// the refreshed ring (`refan_incr`), same as a write's own `W`/failure
/// handling.
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
        // Same "the shared connection may simply have been idle-closed"
        // reasoning as `finish_write`'s own transport-failure arm.
        Err(_) => refan_incr(context, address, delta, tag, retry_capable).await,
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
    for addr in replicas {
        if let Err(error) = context
            .backends
            .call(
                context,
                addr,
                frame_set(namespace, key, value, ttl),
                Expect::Stored,
            )
            .await
        {
            eprintln!("WARN replica incr-result write to {addr} failed: {error}");
        }
    }
}

/// `finish_incr`'s retry path for both a primary `W` and a transport
/// failure: re-fetches the current ring and runs the whole INCR (primary
/// leg, then the replica fan-out) again via `call`, whose transparent
/// redial recovers a dead shared connection.
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
        .call(
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
/// comment). A primary `W` or transport failure re-runs the whole thing
/// on the refreshed ring (`refan_cas_set`), same as `finish_incr`.
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
        Err(_) => refan_cas_set(context, address, write, tag, retry_capable).await,
        Ok(_) => Err(Fatal),
    }
}

/// `finish_cas_set`'s retry path for both a primary `W` and a transport
/// failure: re-fetches the current ring and runs the whole compare-and-set
/// (primary leg, then the replica fan-out) again via `call`.
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
        .call(
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
/// plain `Set`.
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
        Err(_) => refan_cas_delete(context, address, expected_digest, tag, retry_capable).await,
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
    for addr in replicas {
        if let Err(error) = context
            .backends
            .call(context, addr, frame_delete(namespace, key), Expect::Deleted)
            .await
        {
            eprintln!("WARN replica cas-delete-result write to {addr} failed: {error}");
        }
    }
}

/// `finish_cas_delete`'s retry path for both a primary `W` and a
/// transport failure.
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
        .call(
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
    let mut pending = Vec::new();
    for addr in ring.all_addresses() {
        let reply = context
            .backends
            .enqueue(context, &addr, frame.clone(), Expect::Cleared)
            .await;
        pending.push((addr, reply));
    }
    pending
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
    let mut all_ok = true;
    for addr in ring.all_addresses() {
        all_ok &= matches!(
            context
                .backends
                .call(context, &addr, frame.clone(), Expect::Cleared)
                .await,
            Ok(NodeReply::Cleared)
        );
    }
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
                    if write_half.write_all(&response).await.is_err() {
                        return write_half;
                    }
                }
                Ok(Err(Fatal)) | Err(_) => {
                    writer_context
                        .upstream_failures_total
                        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    let _ = write_half.write_all(b"E\n").await;
                    return write_half;
                }
            }
        }
        write_half
    });

    let mut buf = BytesMut::new();
    let mut authenticated = context.secret.is_none();
    let mut tagged = false;
    // Issue #125: set when the client's `A` carried the `R` token.
    // Stable before any request is dispatched (auth precedes requests),
    // so it can be passed to `dispatch_request` by value.
    let mut retry_capable = false;
    let mut drain = context.drain.clone();

    let result: io::Result<()> = 'connection: loop {
        // Parse everything already buffered before reading more.
        loop {
            match parse_request(&mut buf, tagged) {
                Ok(ParseOutcome::Incomplete) => break,
                Ok(ParseOutcome::Auth {
                    secret,
                    tagging,
                    retry_capable: retryable,
                }) => {
                    let accepted = match &context.secret {
                        Some(required) => secrets_match(&secret, required),
                        // No secret configured: any non-empty secret is
                        // accepted, same as the node.
                        None => true,
                    };
                    let (response_tx, response_rx) = oneshot::channel();
                    let reply: &[u8] = if accepted {
                        authenticated = true;
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
        backends: SharedBackends::new(),
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
        tokio::spawn(run_metrics_server(
            metrics_listener,
            Arc::clone(&context),
            Arc::clone(&permits),
            args.max_connections,
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

/// Issue #124: minimal, dependency-free HTTP responder for Prometheus
/// text-format metrics and orchestrator probes. `/readyz` answers `503`
/// until the first roster fetch has landed — a proxy with no ring view
/// would answer clients `B`, so keep it out of rotation until then.
/// Unauthenticated by design (operational telemetry; keep the port
/// internal).
async fn run_metrics_server(
    listener: TcpListener,
    context: Arc<ProxyContext>,
    permits: Arc<Semaphore>,
    max_connections: usize,
) {
    loop {
        let Ok((stream, _)) = listener.accept().await else {
            continue;
        };
        let context = Arc::clone(&context);
        let permits = Arc::clone(&permits);
        tokio::spawn(async move {
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
            Err(error) => return Err(error),
        };
        let _ = stream.set_nodelay(true);

        let Ok(permit) = Arc::clone(&permits).try_acquire_owned() else {
            // Over the connection budget: answer busy and move on, the
            // node's own stance.
            tokio::spawn(async move {
                let mut stream = stream;
                let _ = stream.write_all(b"B\n").await;
            });
            continue;
        };

        let context = Arc::clone(&context);
        let acceptor = tls_acceptor.clone();
        connections.spawn(async move {
            let _permit = permit;
            let stream: ServerStream = match acceptor {
                None => MaybeTls::Plain(stream),
                Some(acceptor) => match acceptor.accept(stream).await {
                    Ok(tls) => MaybeTls::Tls(Box::new(tls)),
                    Err(error) => {
                        eprintln!("WARN TLS handshake with {peer} failed: {error}");
                        return;
                    }
                },
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
        ));
        let context = Arc::new(ProxyContext {
            secret,
            tls_connector: None,
            ring: ring_rx.clone(),
            refresh_now: refresh_tx,
            drain: drain_rx,
            backends: SharedBackends::new(),
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

        stream
            .write_all(b"S 1 1\na1S 1 1\nb2")
            .await
            .unwrap();
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
        let (key_a, key_b) = (on_a.expect("no key hashed to node a"), on_b.expect("no key hashed to node b"));

        let (mut stream, mut buf) = connect_and_auth(&proxy).await;
        for key in [&key_a, &key_b] {
            let frame = format!("S {} 1\n{key}v", key.len());
            stream.write_all(frame.as_bytes()).await.unwrap();
            assert_eq!(read_line(&mut stream, &mut buf).await.unwrap(), "S");
        }

        // Request in a→b order once and b→a order once: the reassembly
        // must always match the request's own order, not arrival order.
        for (first, second) in [(&key_a, &key_b), (&key_b, &key_a)] {
            let frame = format!(
                "m 0 2 {} {}\n{first}{second}",
                first.len(),
                second.len()
            );
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
            backends: SharedBackends::new(),
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
            backends: SharedBackends::new(),
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
}
