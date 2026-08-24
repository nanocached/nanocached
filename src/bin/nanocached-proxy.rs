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
//! - Failure surface: an upstream failure that survives one
//!   refresh-and-retry answers the client `E\n` and closes, the
//!   protocol's existing (fatal) error status — the protocol has no
//!   non-fatal server-error reply, and inventing one here would break
//!   every existing client's `E` handling. Single-address SDK clients
//!   reconnect and retry exactly as they do against a restarting node.
//!   (A retryable-error status is a protocol-level follow-up, noted in
//!   #110's scheduling work.)
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
     [--tls-cert <pem> --tls-key <pem>] [--tls-ca <pem>]\n\
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
async fn run_refresher(
    discovery: Vec<String>,
    secret: Option<Bytes>,
    tls_connector: Option<TlsConnector>,
    announce: Option<(ProxyIdentity, u16)>,
    ring_tx: watch::Sender<Option<Arc<RingView>>>,
    mut refresh_rx: mpsc::Receiver<()>,
) {
    loop {
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
        if let Some((identity, port)) = &announce {
            for addr in &discovery {
                if let Err(error) =
                    announce_to(addr, &secret, &tls_connector, identity, *port).await
                {
                    eprintln!("WARN proxy announce to {addr} failed: {error}");
                }
            }
        }

        tokio::select! {
            _ = sleep(REFRESH_INTERVAL) => {}
            received = refresh_rx.recv() => {
                if received.is_none() {
                    return;
                }
                // Coalesce a burst of W-triggered nudges into one fetch.
                while refresh_rx.try_recv().is_ok() {}
            }
        }
    }
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
    Clear {
        namespace: Bytes,
    },
    ClearAll,
}

#[derive(Debug, PartialEq)]
enum ParseOutcome {
    /// A full request (plus the client's tag in tagged mode) was
    /// consumed from the buffer.
    Ready(Request, Option<u32>),
    /// More bytes are needed; the buffer is untouched.
    Incomplete,
    /// `A <len> [T]` — handled by the caller (auth state lives there).
    Auth { secret: Bytes, tagging: bool },
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
            // `A <len>` or `A <len> T`; never carries a tag.
            let (length_field, tagging) = match fields.as_slice() {
                [length] => (*length, false),
                [length, "T"] => (*length, true),
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
}

/// One reply from a node, already tag-verified.
#[derive(Debug, PartialEq)]
enum NodeReply {
    Value(Bytes),
    NotFound,
    Stored,
    Deleted,
    Cleared,
    WrongNode,
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
    let (echoed, value_length) = match (marker, fields.as_slice()) {
        ("V", [length, tag_field]) => (*tag_field, Some(parse_length_field(length)?)),
        ("S" | "D" | "N" | "W" | "C" | "E", [tag_field]) => (*tag_field, None),
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
        ("N", _) => NodeReply::NotFound,
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
}

impl SharedBackends {
    fn new() -> Self {
        Self {
            slots: std::sync::Mutex::new(HashMap::new()),
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

/// Errors that end the client connection: encoded as `E` (+tag), the
/// writer closes after sending. See the module docs' failure-surface
/// note for why upstream failure is fatal to the connection.
struct Fatal;

type DriverResult = Result<Vec<u8>, Fatal>;

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
) -> oneshot::Receiver<DriverResult> {
    let (result_tx, result_rx) = oneshot::channel();

    let Some(ring) = current_ring(&context) else {
        // No roster yet: `B`, the "not ready, retry" clients already
        // understand — untagged like the node's own unsolicited busy.
        let _ = result_tx.send(Ok(respond("B", None)));
        return result_rx;
    };

    match request {
        Request::Get { namespace, key } => {
            let owners = ring.owners(&namespace, &key);
            let Some(primary) = owners.first() else {
                let _ = result_tx.send(Ok(respond("B", None)));
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
                let _ = result_tx.send(result);
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
                let result = finish_write(&context, &namespace, &key, write, pending, tag).await;
                let _ = result_tx.send(result);
            });
        }
        Request::Delete { namespace, key } => {
            let pending = enqueue_write(&context, &ring, &namespace, &key, None).await;
            tokio::spawn(async move {
                let result = finish_write(&context, &namespace, &key, None, pending, tag).await;
                let _ = result_tx.send(result);
            });
        }
        Request::Clear { namespace } => {
            let pending = enqueue_clear(&context, &ring, Some(&namespace)).await;
            tokio::spawn(async move {
                let result = finish_clear(&context, Some(namespace), pending, tag).await;
                let _ = result_tx.send(result);
            });
        }
        Request::ClearAll => {
            let pending = enqueue_clear(&context, &ring, None).await;
            tokio::spawn(async move {
                let result = finish_clear(&context, None, pending, tag).await;
                let _ = result_tx.send(result);
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
) -> DriverResult {
    let write_ref = write.as_ref().map(|(value, ttl)| (value, *ttl));
    let mut replies = Vec::with_capacity(pending.len());
    for reply in pending {
        replies.push(reply.await);
    }
    let Some(primary_reply) = replies.first() else {
        return Ok(respond("B", None));
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
            refan_write(context, namespace, key, write_ref, tag).await
        }
        // A transport failure on the ordered attempt: the shared
        // connection may simply have been idle-closed by the node
        // (issue #110 — long-lived shared connections make that the
        // common case). Re-fan once via `call`, whose transparent
        // redial recovers it; a second failure is real.
        Err(_) => refan_write(context, namespace, key, write_ref, tag).await,
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
) -> DriverResult {
    let Some(ring) = current_ring(context) else {
        return Err(Fatal);
    };
    let owners = ring.owners(namespace, key);
    let (frame, expect) = write_frame(namespace, key, write);
    let Some((primary, replicas)) = owners.split_first() else {
        return Ok(respond("B", None));
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

    let writer = tokio::spawn(async move {
        while let Some(pending) = fifo_rx.recv().await {
            match pending.await {
                Ok(Ok(response)) => {
                    if write_half.write_all(&response).await.is_err() {
                        return write_half;
                    }
                }
                Ok(Err(Fatal)) | Err(_) => {
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

    let result: io::Result<()> = 'connection: loop {
        // Parse everything already buffered before reading more.
        loop {
            match parse_request(&mut buf, tagged) {
                Ok(ParseOutcome::Incomplete) => break,
                Ok(ParseOutcome::Auth { secret, tagging }) => {
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
                    let response_rx = dispatch_request(Arc::clone(&context), request, tag).await;
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

        let mut chunk = [0u8; 4096];
        let read = timeout(IDLE_TIMEOUT, read_half.read(&mut chunk)).await;
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
    let identity = ProxyIdentity::generate();
    println!("INFO proxy identity: {}", identity.name);
    tokio::spawn(run_refresher(
        args.discovery.clone(),
        secret.clone(),
        tls_connector.clone(),
        Some((identity, args.port)),
        ring_tx,
        refresh_rx,
    ));

    let context = Arc::new(ProxyContext {
        secret,
        tls_connector,
        ring: ring_rx,
        refresh_now: refresh_tx,
        backends: SharedBackends::new(),
    });

    let listener = TcpListener::bind((args.host.as_str(), args.port)).await?;
    let local = listener.local_addr()?;
    println!(
        "INFO nanocached-proxy listening on {local} (discovery: {})",
        args.discovery.join(",")
    );

    serve(listener, context, tls_acceptor, args.max_connections).await
}

/// The accept loop, factored from `run` so tests can drive it against a
/// listener they bound themselves.
async fn serve(
    listener: TcpListener,
    context: Arc<ProxyContext>,
    tls_acceptor: Option<TlsAcceptor>,
    max_connections: usize,
) -> io::Result<()> {
    let permits = Arc::new(Semaphore::new(max_connections));

    loop {
        let (stream, peer) = listener.accept().await?;

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
        tokio::spawn(async move {
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
    }
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
            };
            let store = Arc::clone(&node.store);
            let cleared = Arc::clone(&node.cleared);
            let flushed = Arc::clone(&node.flushed);
            let wrong_once = Arc::clone(&node.wrong_node_once);
            let close_once = Arc::clone(&node.close_once);
            let delay = Arc::clone(&node.get_delay);
            let auth_count = Arc::clone(&node.auth_count);
            let saw_pipelined = Arc::clone(&node.saw_pipelined);
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
                        },
                    ));
                }
            });
            node
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

    /// A mock discovery answering `L` with a fixed roster.
    async fn start_mock_discovery(roster: Vec<(String, String)>, replication: usize) -> String {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        tokio::spawn(async move {
            loop {
                let Ok((mut stream, _)) = listener.accept().await else {
                    return;
                };
                let roster = roster.clone();
                tokio::spawn(async move {
                    let mut buf = BytesMut::new();
                    loop {
                        let Ok(line) = read_line(&mut stream, &mut buf).await else {
                            return;
                        };
                        if line.starts_with("A ") {
                            let length: usize = line.split(' ').nth(1).unwrap().parse().unwrap();
                            if read_exact_into(&mut stream, &mut buf, length)
                                .await
                                .is_err()
                            {
                                return;
                            }
                            let _ = buf.split_to(length);
                            let _ = stream.write_all(b"Od\n").await;
                            continue;
                        }
                        assert_eq!(line, "L");
                        let mut response =
                            format!("N {} {replication}\n", roster.len()).into_bytes();
                        for (name, addr) in &roster {
                            response.extend_from_slice(
                                format!("{} {}\n{name}{addr}\n", name.len(), addr.len()).as_bytes(),
                            );
                        }
                        if stream.write_all(&response).await.is_err() {
                            return;
                        }
                    }
                });
            }
        });
        addr
    }

    /// Boots a proxy over the given roster; returns its address.
    async fn start_proxy(
        discovery_addr: &str,
        secret: Option<&str>,
        max_connections: usize,
    ) -> String {
        let (ring_tx, ring_rx) = watch::channel(None);
        let (refresh_tx, refresh_rx) = mpsc::channel(16);
        let secret = secret.map(|secret| Bytes::from(secret.to_string()));
        tokio::spawn(run_refresher(
            vec![discovery_addr.to_string()],
            secret.clone(),
            None,
            None,
            ring_tx,
            refresh_rx,
        ));
        let context = Arc::new(ProxyContext {
            secret,
            tls_connector: None,
            ring: ring_rx.clone(),
            refresh_now: refresh_tx,
            backends: SharedBackends::new(),
        });
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        tokio::spawn(serve(listener, context, None, max_connections));

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

    // ── end-to-end ───────────────────────────────────────────────────

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
}
