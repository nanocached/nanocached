//! Async, multi-threaded load client for the nanocached TCP protocol.
//!
//! An earlier Python load-test tool was GIL-bound and could not drive
//! enough concurrent I/O to saturate the server; this binary exists to find
//! nanocached's actual ceiling. It is a development tool, not part of the
//! product, so it is intentionally not reachable through `ncd`.
//!
//! There is deliberately no pipelining option. nanocached's target workload is
//! one lookup per client request (e.g. a web request checking a session or
//! a cached record), which cannot be pipelined because the caller doesn't
//! know the next key before seeing the current result. A pipeline flag
//! invites optimizing for a throughput number the real workload doesn't
//! produce.
//!
//! With `--discovery <addr>`, the node list is fetched once from a
//! discovery server (see `src/bin/nanocached-discovery.rs`) instead of
//! using a single `--host`/`--port`, and keys are routed across those
//! nodes by consistent hashing (see ADR 0002), so this binary can also
//! measure how throughput scales as nodes are added.

use bytes::{Buf, BufMut, BytesMut};
use rustls::pki_types::{CertificateDer, ServerName};
use rustls::{ClientConfig, RootCertStore};
use std::collections::HashMap;
use std::io::BufReader;
use std::pin::Pin;
use std::process::ExitCode;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Instant;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, ReadBuf};
use tokio::net::TcpStream;
use tokio::sync::Barrier;
use tokio::task::JoinSet;
use tokio_rustls::TlsConnector;

/// Wraps either a plain TCP connection or one wrapped in TLS behind a
/// single type, so the rest of this file doesn't need to know which is in
/// play once a connection is established.
enum MaybeTls {
    Plain(TcpStream),
    Tls(Box<tokio_rustls::client::TlsStream<TcpStream>>),
}

impl AsyncRead for MaybeTls {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        match self.get_mut() {
            MaybeTls::Plain(stream) => Pin::new(stream).poll_read(cx, buf),
            MaybeTls::Tls(stream) => Pin::new(stream.as_mut()).poll_read(cx, buf),
        }
    }
}

impl AsyncWrite for MaybeTls {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        match self.get_mut() {
            MaybeTls::Plain(stream) => Pin::new(stream).poll_write(cx, buf),
            MaybeTls::Tls(stream) => Pin::new(stream.as_mut()).poll_write(cx, buf),
        }
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        match self.get_mut() {
            MaybeTls::Plain(stream) => Pin::new(stream).poll_flush(cx),
            MaybeTls::Tls(stream) => Pin::new(stream.as_mut()).poll_flush(cx),
        }
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        match self.get_mut() {
            MaybeTls::Plain(stream) => Pin::new(stream).poll_shutdown(cx),
            MaybeTls::Tls(stream) => Pin::new(stream.as_mut()).poll_shutdown(cx),
        }
    }
}

/// Loads CA certificates from a PEM file and builds a `TlsConnector` that
/// trusts only those CAs (not the system trust store), matching
/// nanocached-node/nanocached-discovery's own `--tls-ca` semantics.
fn load_tls_connector(ca_path: &str) -> std::io::Result<TlsConnector> {
    let file = std::fs::File::open(ca_path)?;
    let certs: Vec<CertificateDer<'static>> =
        rustls_pemfile::certs(&mut BufReader::new(file)).collect::<std::io::Result<_>>()?;

    let mut roots = RootCertStore::empty();
    for cert in certs {
        roots
            .add(cert)
            .map_err(|error| invalid_data(&error.to_string()))?;
    }

    let config = ClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth();

    Ok(TlsConnector::from(Arc::new(config)))
}

/// Connects to `addr`, upgrading to TLS first if `tls` is set. There is no
/// plaintext fallback: if TLS is configured and the handshake fails, the
/// connection attempt fails too.
async fn connect(addr: &str, tls: Option<&TlsConnector>) -> std::io::Result<MaybeTls> {
    let stream = TcpStream::connect(addr).await?;
    let _ = stream.set_nodelay(true);

    match tls {
        Some(connector) => {
            let host = addr.rsplit_once(':').map_or(addr, |(host, _)| host);
            let server_name = ServerName::try_from(host.to_string()).map_err(|error| {
                invalid_data(&format!("invalid TLS server name {host:?}: {error}"))
            })?;
            let tls_stream = connector.connect(server_name, stream).await?;
            Ok(MaybeTls::Tls(Box::new(tls_stream)))
        }
        None => Ok(MaybeTls::Plain(stream)),
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Workload {
    Get,
    Set,
    Mixed,
}

struct Args {
    host: String,
    port: u16,
    discovery: Option<String>,
    auth_secret: Option<String>,
    tls_ca: Option<String>,
    requests: u64,
    connections: u64,
    workload: Workload,
    get_ratio: f64,
    keys: u64,
    value_size: usize,
    ttl: Option<u64>,
    seed: u64,
}

impl Default for Args {
    fn default() -> Self {
        Self {
            host: "127.0.0.1".to_string(),
            port: 8356,
            discovery: None,
            auth_secret: None,
            tls_ca: None,
            requests: 100_000,
            connections: 16,
            workload: Workload::Mixed,
            get_ratio: 0.8,
            keys: 10_000,
            value_size: 128,
            ttl: None,
            seed: 1,
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
            "--port" => args.port = parse_value(&value()?, "--port")?,
            "--discovery" => args.discovery = Some(value()?),
            "--auth-secret" => args.auth_secret = Some(value()?),
            "--tls-ca" => args.tls_ca = Some(value()?),
            "-n" | "--requests" => args.requests = parse_value(&value()?, "--requests")?,
            "-c" | "--connections" => args.connections = parse_value(&value()?, "--connections")?,
            "--workload" => {
                args.workload = match value()?.as_str() {
                    "get" => Workload::Get,
                    "set" => Workload::Set,
                    "mixed" => Workload::Mixed,
                    other => return Err(format!("unknown workload: {other}")),
                }
            }
            "--get-ratio" => args.get_ratio = parse_value(&value()?, "--get-ratio")?,
            "--keys" => args.keys = parse_value(&value()?, "--keys")?,
            "--value-size" => args.value_size = parse_value(&value()?, "--value-size")?,
            "--ttl" => args.ttl = Some(parse_value(&value()?, "--ttl")?),
            "--seed" => args.seed = parse_value(&value()?, "--seed")?,
            "-h" | "--help" => return Err(usage()),
            other => return Err(format!("unknown flag: {other}\n\n{}", usage())),
        }
    }

    if args.requests == 0 || args.connections == 0 || args.keys == 0 {
        return Err("requests, connections, and keys must be positive".to_string());
    }
    if !(0.0..=1.0).contains(&args.get_ratio) {
        return Err("get-ratio must be between 0 and 1".to_string());
    }

    Ok(args)
}

fn parse_value<T: std::str::FromStr>(raw: &str, flag: &str) -> Result<T, String> {
    raw.parse()
        .map_err(|_| format!("invalid value for {flag}: {raw}"))
}

fn usage() -> String {
    "\
Usage: bench [options]

  --host <addr>          server host (default 127.0.0.1)
  --port <port>          server port (default 8356)
  --discovery <addr>     fetch the node list from a discovery server at
                          <addr> instead of using --host/--port, and route
                          keys across those nodes by consistent hashing
  --auth-secret <secret> authenticate to the discovery server and every
                          node with this secret before sending other
                          commands (matches NANOCACHED_AUTH_SECRET)
  --tls-ca <path>        PEM CA certificate(s) to trust; connects to the
                          discovery server and every node over TLS instead
                          of plaintext (matches --tls-cert/--tls-key on
                          nanocached-node/nanocached-discovery)
  -n, --requests <n>     total requests (default 100000)
  -c, --connections <n>  concurrent connections (default 16)
  --workload <kind>      get | set | mixed (default mixed)
  --get-ratio <f>        GET fraction for mixed workload (default 0.8)
  --keys <n>             distinct key count (default 10000)
  --value-size <n>       SET value size in bytes (default 128)
  --ttl <seconds>        TTL for SET requests (default: none)
  --seed <n>             RNG seed (default 1)"
        .to_string()
}

/// SplitMix64: small, dependency-free, deterministic given a seed.
struct Rng(u64);

impl Rng {
    fn new(seed: u64) -> Self {
        Self(seed)
    }

    fn next_u64(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E3779B97F4A7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58476D1CE4E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D049BB133111EB);
        z ^ (z >> 31)
    }

    fn below(&mut self, bound: u64) -> u64 {
        self.next_u64() % bound
    }

    fn unit(&mut self) -> f64 {
        (self.next_u64() >> 11) as f64 * (1.0 / (1u64 << 53) as f64)
    }
}

fn fnv1a(bytes: &[u8]) -> u64 {
    let mut hash: u64 = 0xcbf29ce484222325;
    for &byte in bytes {
        hash ^= byte as u64;
        hash = hash.wrapping_mul(0x100000001b3);
    }
    hash
}

/// MurmurHash3's 64-bit finalizer — see `src/hash_ring.rs`, whose
/// computation this is a byte-for-byte copy of (ADR 0011).
fn fmix64(mut hash: u64) -> u64 {
    hash ^= hash >> 33;
    hash = hash.wrapping_mul(0xff51afd7ed558ccd);
    hash ^= hash >> 33;
    hash = hash.wrapping_mul(0xc4ceb9fe1a85ec53);
    hash ^= hash >> 33;
    hash
}

/// Rendezvous hashing over a fixed node list (ADR 0011, replacing ADR
/// 0002's ring), built once at startup from either `--host`/`--port` or a
/// discovery server's node list. Each entry pairs a node's ADR-0009 name
/// (what routing hashes) with its address (what this binary dials).
/// Routing a key never needs to change once built, since this binary
/// doesn't react to nodes joining or leaving mid-run. bench always talks
/// to a key's primary; replicas are the SDK's concern.
struct HashRing {
    /// (name, address) pairs.
    nodes: Vec<(String, String)>,
    node_hashes: Vec<u64>,
}

impl HashRing {
    fn new(nodes: Vec<(String, String)>) -> Self {
        let node_hashes = nodes
            .iter()
            .map(|(name, _)| fnv1a(name.as_bytes()))
            .collect();
        Self { nodes, node_hashes }
    }

    /// The key's primary, by name.
    fn route(&self, key: &[u8]) -> &str {
        let key_hash = fnv1a(key);
        let (_, name) = self
            .node_hashes
            .iter()
            .zip(&self.nodes)
            .map(|(node_hash, (name, _))| (fmix64(node_hash ^ key_hash), name.as_str()))
            .max_by(|a, b| a.0.cmp(&b.0).then_with(|| b.1.cmp(a.1)))
            .expect("the ring is never built empty");
        name
    }
}

/// Sends `A <len>\n<secret>` and confirms the server accepted it. Both
/// nanocached-node and nanocached-discovery speak this same handshake, but
/// reply with a different second byte (`On\n`/`En\n` for a node, `Od\n`/
/// `Ed\n` for discovery) so a client can tell which kind of server answered
/// without knowing in advance which it dialed. bench doesn't care which
/// kind it's talking to here — it already knows from its own arguments —
/// so it only checks the leading `O`/`E`.
async fn authenticate(stream: &mut MaybeTls, secret: &str) -> std::io::Result<()> {
    let mut request = BytesMut::new();
    request.put_slice(b"A ");
    request.put_slice(secret.len().to_string().as_bytes());
    request.put_u8(b'\n');
    request.put_slice(secret.as_bytes());
    stream.write_all(&request).await?;

    let mut response = [0_u8; 3];
    stream.read_exact(&mut response).await?;
    if response[0] != b'O' {
        return Err(invalid_data("authentication failed"));
    }

    Ok(())
}

/// Fetches the current node list from a discovery server (`L\n` -> `N
/// <count> <r>\n` followed by `count` `<name-length> <addr-length>\n
/// <name><addr>\n` entries — ADR 0009/0011).
async fn fetch_nodes(
    discovery_addr: &str,
    auth_secret: Option<&str>,
    tls: Option<&TlsConnector>,
) -> std::io::Result<Vec<(String, String)>> {
    let mut stream = connect(discovery_addr, tls).await?;

    if let Some(secret) = auth_secret {
        authenticate(&mut stream, secret).await?;
    }

    stream.write_all(b"L\n").await?;

    let mut recv_buf = BytesMut::new();
    loop {
        if let Some(nodes) = parse_node_list(&recv_buf) {
            return Ok(nodes);
        }

        let mut chunk = [0_u8; 4096];
        let read = stream.read(&mut chunk).await?;
        if read == 0 {
            return Err(invalid_data("discovery server closed the connection"));
        }
        recv_buf.extend_from_slice(&chunk[..read]);
    }
}

fn parse_node_list(buf: &[u8]) -> Option<Vec<(String, String)>> {
    if buf.first() != Some(&b'N') {
        return None;
    }

    let header_end = buf.iter().position(|&byte| byte == b'\n')?;
    let mut header = std::str::from_utf8(&buf[2..header_end]).ok()?.split(' ');
    let count: usize = header.next()?.parse().ok()?;
    let _replication: usize = header.next()?.parse().ok()?;

    let mut offset = header_end + 1;
    let mut nodes = Vec::with_capacity(count);

    for _ in 0..count {
        let entry_header_end = offset + buf[offset..].iter().position(|&byte| byte == b'\n')?;
        let mut lengths = std::str::from_utf8(&buf[offset..entry_header_end])
            .ok()?
            .split(' ');
        let name_length: usize = lengths.next()?.parse().ok()?;
        let addr_length: usize = lengths.next()?.parse().ok()?;

        let name_start = entry_header_end + 1;
        let addr_start = name_start + name_length;
        let addr_end = addr_start + addr_length;
        // +1 for the trailing '\n' after each entry's body.
        if buf.len() < addr_end + 1 {
            return None;
        }

        nodes.push((
            std::str::from_utf8(&buf[name_start..addr_start])
                .ok()?
                .to_string(),
            std::str::from_utf8(&buf[addr_start..addr_end])
                .ok()?
                .to_string(),
        ));
        offset = addr_end + 1;
    }

    Some(nodes)
}

fn write_get_request(key: &[u8], out: &mut BytesMut) {
    out.put_slice(b"G ");
    out.put_slice(key.len().to_string().as_bytes());
    out.put_u8(b'\n');
    out.put_slice(key);
}

fn write_set_request(key: &[u8], value: &[u8], ttl: Option<u64>, out: &mut BytesMut) {
    out.put_slice(b"S ");
    out.put_slice(key.len().to_string().as_bytes());
    out.put_u8(b' ');
    out.put_slice(value.len().to_string().as_bytes());
    if let Some(ttl) = ttl {
        out.put_u8(b' ');
        out.put_slice(ttl.to_string().as_bytes());
    }
    out.put_u8(b'\n');
    out.put_slice(key);
    out.put_slice(value);
}

/// Reads one response frame from `stream`, buffering through `recv_buf`.
/// Returns the response's leading byte and its total encoded length.
async fn read_response(
    stream: &mut MaybeTls,
    recv_buf: &mut BytesMut,
) -> std::io::Result<(u8, usize)> {
    loop {
        if let Some(line_end) = recv_buf.iter().position(|&byte| byte == b'\n') {
            let kind = recv_buf[0];
            let header_len = line_end + 1;

            let total = if kind == b'V' {
                let length_str = std::str::from_utf8(&recv_buf[2..line_end])
                    .map_err(|_| invalid_data("non-utf8 value length"))?;
                let value_len: usize = length_str
                    .parse()
                    .map_err(|_| invalid_data("invalid value length"))?;
                header_len + value_len
            } else {
                header_len
            };

            if recv_buf.len() >= total {
                recv_buf.advance(total);
                return Ok((kind, total));
            }
        }

        let mut chunk = [0_u8; 64 * 1024];
        let read = stream.read(&mut chunk).await?;
        if read == 0 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "server closed the connection",
            ));
        }
        recv_buf.extend_from_slice(&chunk[..read]);
    }
}

fn invalid_data(message: &str) -> std::io::Error {
    std::io::Error::new(std::io::ErrorKind::InvalidData, message.to_string())
}

#[derive(Default)]
struct WorkerResult {
    completed: u64,
    latencies_ns: Vec<u64>,
    bytes_sent: u64,
    bytes_received: u64,
    error: Option<String>,
}

async fn worker(
    id: u64,
    args: Arc<Args>,
    ring: Arc<HashRing>,
    barrier: Arc<Barrier>,
    tls: Option<TlsConnector>,
) -> WorkerResult {
    let mut result = WorkerResult::default();

    let mut connections: HashMap<String, MaybeTls> = HashMap::with_capacity(ring.nodes.len());
    for (name, addr) in &ring.nodes {
        let mut stream = match connect(addr.as_str(), tls.as_ref()).await {
            Ok(stream) => stream,
            Err(error) => {
                result.error = Some(format!("connect to {addr}: {error}"));
                barrier.wait().await;
                return result;
            }
        };

        if let Some(secret) = &args.auth_secret
            && let Err(error) = authenticate(&mut stream, secret).await
        {
            result.error = Some(format!("auth to {addr}: {error}"));
            barrier.wait().await;
            return result;
        }

        // Keyed by ADR-0009 name — what `route` returns.
        connections.insert(name.clone(), stream);
    }

    barrier.wait().await;

    let mut rng = Rng::new(args.seed.wrapping_add(id));
    let mut recv_buf = BytesMut::new();
    let mut send_buf = BytesMut::new();
    let value = vec![(id % 256) as u8; args.value_size];

    let mut remaining = args.requests / args.connections;
    if id < args.requests % args.connections {
        remaining += 1;
    }

    while remaining > 0 {
        let key = format!("nanocached:{}", rng.below(args.keys));
        let is_get = match args.workload {
            Workload::Get => true,
            Workload::Set => false,
            Workload::Mixed => rng.unit() < args.get_ratio,
        };

        send_buf.clear();
        if is_get {
            write_get_request(key.as_bytes(), &mut send_buf);
        } else {
            write_set_request(key.as_bytes(), &value, args.ttl, &mut send_buf);
        }

        let node = ring.route(key.as_bytes());
        let stream = connections
            .get_mut(node)
            .expect("ring only routes to a node this worker connected to");

        let started = Instant::now();

        if let Err(error) = stream.write_all(&send_buf).await {
            result.error = Some(format!("write to {node}: {error}"));
            break;
        }
        result.bytes_sent += send_buf.len() as u64;

        match read_response(stream, &mut recv_buf).await {
            Ok((kind, len)) => {
                result.bytes_received += len as u64;
                let valid = if is_get {
                    kind == b'V' || kind == b'N'
                } else {
                    kind == b'S'
                };
                if !valid {
                    result.error = Some(format!("unexpected response byte from {node}: {kind:#x}"));
                    break;
                }
            }
            Err(error) => {
                result.error = Some(format!("read from {node}: {error}"));
                break;
            }
        }

        result
            .latencies_ns
            .push(started.elapsed().as_nanos() as u64);

        result.completed += 1;
        remaining -= 1;
    }

    result
}

fn percentile(sorted_ns: &[u64], fraction: f64) -> f64 {
    if sorted_ns.is_empty() {
        return 0.0;
    }
    let index = (((sorted_ns.len() - 1) as f64) * fraction) as usize;
    sorted_ns[index.min(sorted_ns.len() - 1)] as f64 / 1_000_000.0
}

fn with_thousands(value: u64) -> String {
    let digits = value.to_string();
    let mut grouped = String::with_capacity(digits.len() + digits.len() / 3);

    for (index, ch) in digits.chars().enumerate() {
        if index > 0 && (digits.len() - index).is_multiple_of(3) {
            grouped.push(',');
        }
        grouped.push(ch);
    }

    grouped
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> ExitCode {
    rustls::crypto::ring::default_provider()
        .install_default()
        .expect("no other rustls crypto provider is installed this early in the process");

    let args = match parse_args() {
        Ok(args) => args,
        Err(message) => {
            eprintln!("{message}");
            return ExitCode::FAILURE;
        }
    };

    let tls = match &args.tls_ca {
        Some(ca) => match load_tls_connector(ca) {
            Ok(connector) => Some(connector),
            Err(error) => {
                eprintln!("failed to load --tls-ca {ca}: {error}");
                return ExitCode::FAILURE;
            }
        },
        None => None,
    };

    let nodes = if let Some(discovery_addr) = &args.discovery {
        match fetch_nodes(discovery_addr, args.auth_secret.as_deref(), tls.as_ref()).await {
            Ok(nodes) if !nodes.is_empty() => nodes,
            Ok(_) => {
                eprintln!("no live nodes registered with discovery server at {discovery_addr}");
                return ExitCode::FAILURE;
            }
            Err(error) => {
                eprintln!("failed to fetch node list from {discovery_addr}: {error}");
                return ExitCode::FAILURE;
            }
        }
    } else {
        // Standalone: no discovery, no ADR-0009 name — the address doubles
        // as the (only) ring identity.
        let addr = format!("{}:{}", args.host, args.port);
        vec![(addr.clone(), addr)]
    };

    println!(
        "nodes ({}): {}",
        nodes.len(),
        nodes
            .iter()
            .map(|(_, addr)| addr.as_str())
            .collect::<Vec<_>>()
            .join(", ")
    );

    let ring = Arc::new(HashRing::new(nodes));
    let args = Arc::new(args);

    let barrier = Arc::new(Barrier::new(args.connections as usize + 1));
    let mut workers = JoinSet::new();

    for id in 0..args.connections {
        workers.spawn(worker(
            id,
            Arc::clone(&args),
            Arc::clone(&ring),
            Arc::clone(&barrier),
            tls.clone(),
        ));
    }

    barrier.wait().await;
    let started = Instant::now();

    let mut results = Vec::with_capacity(args.connections as usize);
    while let Some(result) = workers.join_next().await {
        results.push(result.expect("worker task panicked"));
    }

    let elapsed = started.elapsed();

    let completed: u64 = results.iter().map(|r| r.completed).sum();
    let bytes_sent: u64 = results.iter().map(|r| r.bytes_sent).sum();
    let bytes_received: u64 = results.iter().map(|r| r.bytes_received).sum();
    let errors: Vec<&str> = results.iter().filter_map(|r| r.error.as_deref()).collect();

    let mut latencies_ns: Vec<u64> = results
        .iter()
        .flat_map(|r| r.latencies_ns.iter().copied())
        .collect();
    latencies_ns.sort_unstable();

    let throughput = completed as f64 / elapsed.as_secs_f64();
    let avg_ms = if latencies_ns.is_empty() {
        None
    } else {
        Some(latencies_ns.iter().sum::<u64>() as f64 / latencies_ns.len() as f64 / 1_000_000.0)
    };

    println!(
        "completed:   {}/{}",
        with_thousands(completed),
        with_thousands(args.requests)
    );
    println!("duration:    {:.3} s", elapsed.as_secs_f64());
    println!(
        "throughput:  {} requests/s",
        with_thousands(throughput.round() as u64)
    );
    match avg_ms {
        Some(avg_ms) => println!("latency avg: {avg_ms:.3} ms"),
        None => println!("latency avg: n/a"),
    }
    println!("latency p50: {:.3} ms", percentile(&latencies_ns, 0.50));
    println!("latency p95: {:.3} ms", percentile(&latencies_ns, 0.95));
    println!("latency p99: {:.3} ms", percentile(&latencies_ns, 0.99));
    println!(
        "network:     {} B sent, {} B received",
        with_thousands(bytes_sent),
        with_thousands(bytes_received)
    );
    println!("errors:      {}", errors.len());
    for error in errors.iter().take(10) {
        println!("  - {error}");
    }

    if !errors.is_empty() || completed != args.requests {
        ExitCode::FAILURE
    } else {
        ExitCode::SUCCESS
    }
}
