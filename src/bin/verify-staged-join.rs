//! Retained staged node join verification/demonstration harness — a correctness
//! check, not a load-testing tool. Spawns real
//! `nanocached-discovery`/`nanocached-node` processes as subprocesses and
//! drives them exactly like a real client would (raw TCP, the same wire
//! protocol any SDK speaks), to empirically check staged-join behavior
//! rather than reason about it from the design notes alone:
//!
//! - Does a newly joined node actually receive the keys it should, and
//!   does it show up in `L` only once that handoff is done?
//! - How long does a handoff take?
//! - How much does live GET/SET throughput against an *existing* node
//!   dip while that node is busy migrating data to a new one, and does
//!   it recover afterward?
//! - When two nodes ask to join close together, does the second one
//!   really wait (serialize) behind the first, rather than both being
//!   promoted together?
//!
//! Client-side replication (client-side replication via rendezvous hashing) means this
//! harness cannot assume single-owner semantics: with the server's
//! default `--replication-factor 2`, a node rejects `G`/`S` for any key
//! outside its own top-R with `W` (see `wrong_node` in `src/server.rs`).
//! So this harness carries its own byte-for-byte port of the HRW ring
//! (`Ring`, below — the same algorithm `src/hash_ring.rs` and the SDKs
//! each implement independently) and routes every seed write,
//! workload op, and post-join check to a key's actual owners instead of
//! to a fixed node.
//!
//! Scenarios: `1-to-2`, `2-to-3`, `1-to-3-waiting` (two nodes join at
//! once; the second must wait behind the first). Pass `--scenario <name>`
//! to run one, or omit it to run all three in sequence.
//!
//! This binary has no dependency on the node/discovery implementations
//! (the binaries share no modules by design — see size-derived migration timeout and
//! `nanocached-discovery.rs`'s module docs): it only spawns the sibling
//! binaries as subprocesses and
//! speaks the wire protocol to them, with its own minimal copy of just
//! the pieces it needs (`A`/`G`/`S` and discovery's `L`). No TLS/auth
//! support yet — this is for local verification (see staged node join's Context);
//! add it if/when AWS verification needs it.

use bytes::BytesMut;
use std::collections::{HashMap, HashSet};
use std::env;
use std::io;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::Mutex;
use std::time::{Duration, Instant};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
#[cfg(test)]
use tokio::net::TcpListener;
use tokio::net::TcpStream;
use tokio::process::{Child, Command};
use tokio::sync::watch;
use tokio::time::{sleep, timeout};

const CONNECT_RETRY_INTERVAL: Duration = Duration::from_millis(50);
const CONNECT_TIMEOUT: Duration = Duration::from_secs(5);
const JOIN_TIMEOUT: Duration = Duration::from_secs(60);
const POLL_INTERVAL: Duration = Duration::from_millis(50);
const BUCKET_WIDTH: Duration = Duration::from_millis(250);
/// Upper bound on a `G`/`V` reply's declared value length
/// (`get`/`get_value`). The real server's own inbound request cap is
/// 1 MiB (`MAX_REQUEST_SIZE`, `src/server.rs`) and this harness never
/// writes a larger value itself, so a claimed length beyond double that
/// is definitely a corrupt or malformed frame, never a legitimate reply —
/// trusting it outright would hand `read_exact_into` a length that could
/// block waiting for however many bytes a bogus header claims, or drive
/// an unbounded allocation. Mirrors the SDKs' own `MAX_VALUE_LENGTH`
/// (e.g. `sdk/rust/src/connection.rs`).
const MAX_VALUE_LENGTH: usize = 2 * 1024 * 1024;
/// Upper bound on a discovery `L` entry's declared name/addr length
/// (`fetch_joined`). Node identity decoupled from address names and `ip:port` addresses are both, in
/// practice, well under a hundred bytes, and discovery's own inbound
/// request cap is 4 KiB (`MAX_REQUEST_SIZE`,
/// `src/bin/nanocached-discovery.rs`) — nothing legitimate can exceed
/// that either. Same rationale as `MAX_VALUE_LENGTH`.
const MAX_NAME_OR_ADDR_LENGTH: usize = 4096;
/// Upper bound on a discovery `L` response's declared entry count
/// (`fetch_joined`), so a corrupt/malformed header can't drive
/// `Vec::with_capacity(count)` into an oversized allocation before a
/// single entry has even been read. Far above any cluster size this
/// harness's own scenarios ever create.
const MAX_ROSTER_ENTRIES: usize = 1 << 16;

/// A node's name paired with its dialable address.
type Roster = Vec<(String, String)>;

struct Args {
    scenario: Option<String>,
    keys: usize,
    value_size: usize,
    concurrency: usize,
    base_port: u16,
}

impl Default for Args {
    fn default() -> Self {
        Self {
            scenario: None,
            keys: 500,
            value_size: 64,
            concurrency: 8,
            base_port: 19000,
        }
    }
}

fn parse_args() -> Result<Args, String> {
    let mut args = Args::default();
    let mut raw = env::args().skip(1);

    while let Some(flag) = raw.next() {
        let mut value = || raw.next().ok_or_else(|| format!("{flag} requires a value"));

        match flag.as_str() {
            "--scenario" => args.scenario = Some(value()?),
            "--keys" => {
                args.keys = value()?
                    .parse()
                    .map_err(|_| "invalid value for --keys".to_string())?
            }
            "--value-size" => {
                args.value_size = value()?
                    .parse()
                    .map_err(|_| "invalid value for --value-size".to_string())?
            }
            "--concurrency" => {
                args.concurrency = value()?
                    .parse()
                    .map_err(|_| "invalid value for --concurrency".to_string())?
            }
            "--base-port" => {
                args.base_port = value()?
                    .parse()
                    .map_err(|_| "invalid value for --base-port".to_string())?
            }
            "-h" | "--help" => return Err(usage()),
            other => return Err(format!("unknown flag: {other}\n\n{}", usage())),
        }
    }

    Ok(args)
}

fn usage() -> String {
    "\
Usage: verify-staged-join [options]

  --scenario <name>   1-to-2 | 2-to-3 | 1-to-3-waiting (default: run all three)
  --keys <n>          keys seeded before each scenario (default 500)
  --value-size <n>    bytes per value (default 64)
  --concurrency <n>   concurrent workload connections (default 8)
  --base-port <port>  first port used; each scenario claims a fresh block
                       above it (default 19000)"
        .to_string()
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> std::process::ExitCode {
    let args = match parse_args() {
        Ok(args) => args,
        Err(message) => {
            eprintln!("{message}");
            return std::process::ExitCode::FAILURE;
        }
    };

    let discovery_bin = match sibling_binary("nanocached-discovery") {
        Ok(path) => path,
        Err(message) => {
            eprintln!("{message}");
            return std::process::ExitCode::FAILURE;
        }
    };
    let node_bin = match sibling_binary("nanocached-node") {
        Ok(path) => path,
        Err(message) => {
            eprintln!("{message}");
            return std::process::ExitCode::FAILURE;
        }
    };

    let log_dir = log_dir();
    if let Err(error) = std::fs::create_dir_all(&log_dir) {
        eprintln!("cannot create log directory {}: {error}", log_dir.display());
        return std::process::ExitCode::FAILURE;
    }
    println!("node/discovery logs: {}", log_dir.display());

    let scenarios: Vec<&str> = match args.scenario.as_deref() {
        Some(name) => vec![name],
        None => vec!["1-to-2", "2-to-3", "1-to-3-waiting"],
    };

    let mut base_port = args.base_port;
    let mut failed = false;

    for name in scenarios {
        println!("\n=== scenario: {name} ===");

        let result = match name {
            "1-to-2" => {
                run_simple_join(&discovery_bin, &node_bin, &log_dir, &args, base_port, 1).await
            }
            "2-to-3" => {
                run_simple_join(&discovery_bin, &node_bin, &log_dir, &args, base_port, 2).await
            }
            "1-to-3-waiting" => {
                run_waiting_join(&discovery_bin, &node_bin, &log_dir, &args, base_port).await
            }
            other => {
                eprintln!("unknown scenario: {other}\n\n{}", usage());
                return std::process::ExitCode::FAILURE;
            }
        };

        if let Err(error) = result {
            eprintln!("scenario {name} failed: {error}");
            failed = true;
        }

        // Each scenario gets a fresh port block so a slow-to-release
        // socket from the previous one can't collide with the next.
        base_port += 16;
    }

    if failed {
        std::process::ExitCode::FAILURE
    } else {
        std::process::ExitCode::SUCCESS
    }
}

fn sibling_binary(name: &str) -> Result<PathBuf, String> {
    let current_exe = env::current_exe()
        .map_err(|error| format!("cannot locate verify-staged-join's own path: {error}"))?;
    let dir = current_exe
        .parent()
        .ok_or_else(|| "cannot determine verify-staged-join's install directory".to_string())?;

    Ok(dir.join(name))
}

/// A spawned subprocess, killed when dropped (`kill_on_drop`) so a
/// scenario failure or early return never leaves orphaned discovery/node
/// processes behind.
struct Process {
    #[allow(dead_code)]
    child: Child,
    addr: String,
}

/// Where each spawned process's stdout/stderr goes — not discarded, since
/// diagnosing a scenario failure (an unresponsive node, a rejected
/// command) means reading these after the fact. Printed once at startup.
fn log_dir() -> PathBuf {
    std::env::temp_dir().join(format!("verify-staged-join-{}", std::process::id()))
}

fn log_file(log_dir: &Path, name: &str) -> io::Result<Stdio> {
    Ok(Stdio::from(std::fs::File::create(log_dir.join(name))?))
}

fn spawn_discovery(binary: &Path, log_dir: &Path, port: u16) -> io::Result<Process> {
    let child = Command::new(binary)
        .arg("--port")
        .arg(port.to_string())
        // This harness's module doc comment is explicit: "No TLS/auth
        // support yet" — it never sends `A`. But both binaries read
        // `NANOCACHED_AUTH_SECRET` independently of any CLI flag (see
        // README's Authentication section), so if this process happened
        // to inherit that variable from whatever shell/CI environment
        // launched it, the spawned discovery server would require auth
        // this harness can't provide, and every scenario would fail
        // opaquely (a rejected `J`/`P`/`L`, not an obvious "auth secret
        // leaked in") instead of cleanly.
        .env_remove("NANOCACHED_AUTH_SECRET")
        .kill_on_drop(true)
        .stdout(log_file(log_dir, &format!("discovery-{port}.log"))?)
        .stderr(log_file(log_dir, &format!("discovery-{port}.err.log"))?)
        .spawn()?;

    Ok(Process {
        child,
        addr: format!("127.0.0.1:{port}"),
    })
}

fn spawn_node(
    binary: &Path,
    log_dir: &Path,
    port: u16,
    discovery_addr: &str,
) -> io::Result<Process> {
    let child = Command::new(binary)
        .arg("--port")
        .arg(port.to_string())
        .arg("--discovery")
        .arg(discovery_addr)
        // See `spawn_discovery`'s identical `env_remove`: this harness
        // never sends `A`, so an inherited secret would make the node
        // require auth it can't authenticate for either the harness's own
        // `G`/`S` connections or the node's own heartbeats to discovery.
        .env_remove("NANOCACHED_AUTH_SECRET")
        .kill_on_drop(true)
        .stdout(log_file(log_dir, &format!("node-{port}.log"))?)
        .stderr(log_file(log_dir, &format!("node-{port}.err.log"))?)
        .spawn()?;

    Ok(Process {
        child,
        addr: format!("127.0.0.1:{port}"),
    })
}

async fn wait_until_connectable(addr: &str) -> io::Result<()> {
    timeout(CONNECT_TIMEOUT, async {
        loop {
            if TcpStream::connect(addr).await.is_ok() {
                return;
            }
            sleep(CONNECT_RETRY_INTERVAL).await;
        }
    })
    .await
    .map_err(|_| {
        io::Error::new(
            io::ErrorKind::TimedOut,
            format!("{addr} never became connectable"),
        )
    })
}

async fn read_line(stream: &mut TcpStream, buf: &mut BytesMut) -> io::Result<String> {
    loop {
        if let Some(pos) = buf.iter().position(|byte| *byte == b'\n') {
            let line = buf.split_to(pos + 1);
            return Ok(String::from_utf8_lossy(&line[..line.len() - 1]).into_owned());
        }

        let mut chunk = [0u8; 4096];
        let bytes_read = stream.read(&mut chunk).await?;

        if bytes_read == 0 {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "connection closed while reading a line",
            ));
        }

        buf.extend_from_slice(&chunk[..bytes_read]);
    }
}

async fn read_exact_into(
    stream: &mut TcpStream,
    buf: &mut BytesMut,
    need: usize,
) -> io::Result<()> {
    while buf.len() < need {
        let mut chunk = [0u8; 4096];
        let bytes_read = stream.read(&mut chunk).await?;

        if bytes_read == 0 {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "connection closed while reading a body",
            ));
        }

        buf.extend_from_slice(&chunk[..bytes_read]);
    }

    Ok(())
}

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

/// The canonical key-side hash (issue #105), matching `src/hash_ring.rs`:
/// `fnv1a(key)` for the default (empty) namespace, and
/// `fnv1a(be32(len(ns)) || ns || key)` otherwise.
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

/// The namespace half of this harness's keys: every other seeded key
/// lives in a namespace (issue #105), so the handoff is checked for both
/// wire forms and both HRW forms, not just the legacy one.
const VERIFY_NAMESPACE: &[u8] = b"verify";

/// The (namespace, key) pair for seeded key `index`.
fn verify_key(index: usize) -> (&'static [u8], String) {
    let namespace: &'static [u8] = if index.is_multiple_of(2) {
        b""
    } else {
        VERIFY_NAMESPACE
    };
    (namespace, format!("verify-key-{index}"))
}

/// MurmurHash3's 64-bit finalizer, matching `src/hash_ring.rs` exactly.
fn fmix64(mut hash: u64) -> u64 {
    hash ^= hash >> 33;
    hash = hash.wrapping_mul(0xff51afd7ed558ccd);
    hash ^= hash >> 33;
    hash = hash.wrapping_mul(0xc4ceb9fe1a85ec53);
    hash ^= hash >> 33;
    hash
}

/// A byte-for-byte port of `src/hash_ring.rs`'s HRW ring, independently
/// implemented the way any other client-side replication participant (node, SDK)
/// must — see this file's module docs. Only what this harness needs:
/// computing a key's top-R owners, in score order, from a roster snapshot.
struct Ring<'a> {
    nodes: &'a [(String, String)],
    node_hashes: Vec<u64>,
}

impl<'a> Ring<'a> {
    fn new(nodes: &'a [(String, String)]) -> Self {
        let node_hashes = nodes
            .iter()
            .map(|(name, _)| fnv1a(name.as_bytes()))
            .collect();
        Self { nodes, node_hashes }
    }

    /// The key's owners: the `replicas` highest-scoring nodes, primary
    /// first. Fewer than `replicas` when the roster is smaller.
    fn owners(&self, namespace: &[u8], key: &[u8], replicas: usize) -> Roster {
        let key_hash = key_hash(namespace, key);

        let mut scored: Vec<(u64, &(String, String))> = self
            .node_hashes
            .iter()
            .zip(self.nodes)
            .map(|(node_hash, node)| (fmix64(node_hash ^ key_hash), node))
            .collect();

        // Descending by score; ties toward the lexicographically smaller
        // name — same total order every implementation agrees on.
        scored.sort_unstable_by(|a, b| b.0.cmp(&a.0).then_with(|| a.1.0.cmp(&b.1.0)));
        scored.truncate(replicas);

        scored.into_iter().map(|(_, node)| node.clone()).collect()
    }
}

/// Fetches the current `Joined` node list and replication factor from
/// discovery, in the node identity decoupled from address `<name-length> <addr-length>\n<name><addr>`
/// shape.
async fn fetch_joined(discovery_addr: &str) -> io::Result<(Roster, usize)> {
    let mut stream = TcpStream::connect(discovery_addr).await?;
    stream.write_all(b"L\n").await?;

    let mut buf = BytesMut::new();
    let header = read_line(&mut stream, &mut buf).await?;
    // `N <count> <r>\n` since client-side replication: the replication factor rides
    // along so every client (this harness included) can route by top-R
    // instead of assuming single ownership.
    let mut header_parts = header
        .strip_prefix("N ")
        .ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("bad L header: {header:?}"),
            )
        })?
        .split(' ');
    let count: usize = header_parts
        .next()
        .and_then(|count| count.parse().ok())
        .ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("bad L header: {header:?}"),
            )
        })?;
    let replication: usize = header_parts
        .next()
        .and_then(|r| r.parse().ok())
        .ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("bad L header: {header:?}"),
            )
        })?;

    // A corrupt/malformed header must not drive `Vec::with_capacity`
    // into an oversized allocation before a single entry is even read —
    // see `MAX_ROSTER_ENTRIES`.
    if count > MAX_ROSTER_ENTRIES {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "L header declares {count} entries, above MAX_ROSTER_ENTRIES ({MAX_ROSTER_ENTRIES})"
            ),
        ));
    }

    let mut nodes = Vec::with_capacity(count);

    for _ in 0..count {
        let entry_header = read_line(&mut stream, &mut buf).await?;
        let mut parts = entry_header.split(' ');
        let name_length: usize = parts
            .next()
            .and_then(|part| part.parse().ok())
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "bad L entry header"))?;
        let addr_length: usize = parts
            .next()
            .and_then(|part| part.parse().ok())
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "bad L entry header"))?;

        // See `MAX_NAME_OR_ADDR_LENGTH`: a claimed length this large is
        // corrupt, not a legitimate name/addr — reject before it drives
        // `read_exact_into` into blocking on however many bytes a bogus
        // header claims.
        if name_length > MAX_NAME_OR_ADDR_LENGTH || addr_length > MAX_NAME_OR_ADDR_LENGTH {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "L entry header declares name_length={name_length} addr_length={addr_length}, \
                     above MAX_NAME_OR_ADDR_LENGTH ({MAX_NAME_OR_ADDR_LENGTH})"
                ),
            ));
        }

        // +1 for the trailing '\n' discovery writes after each entry's
        // <name><addr> body (see nanocached-discovery.rs's `L` handler).
        read_exact_into(&mut stream, &mut buf, name_length + addr_length + 1).await?;
        let entry = buf.split_to(name_length + addr_length + 1);
        let name = String::from_utf8_lossy(&entry[..name_length]).into_owned();
        let addr =
            String::from_utf8_lossy(&entry[name_length..name_length + addr_length]).into_owned();

        nodes.push((name, addr));
    }

    Ok((nodes, replication))
}

async fn set(
    stream: &mut TcpStream,
    buf: &mut BytesMut,
    namespace: &[u8],
    key: &[u8],
    value: &[u8],
) -> io::Result<bool> {
    let mut message = set_header(namespace, key, value.len());
    message.extend_from_slice(namespace);
    message.extend_from_slice(key);
    message.extend_from_slice(value);
    stream.write_all(&message).await?;

    let line = read_line(stream, buf).await?;
    Ok(line == "S")
}

/// The `S` (default namespace) or `s` (namespaced, issue #105) header.
fn set_header(namespace: &[u8], key: &[u8], value_length: usize) -> Vec<u8> {
    if namespace.is_empty() {
        format!("S {} {}\n", key.len(), value_length).into_bytes()
    } else {
        format!("s {} {} {}\n", namespace.len(), key.len(), value_length).into_bytes()
    }
}

/// The `G`/`g` frame for `key`.
fn get_message(namespace: &[u8], key: &[u8]) -> Vec<u8> {
    let mut message = if namespace.is_empty() {
        format!("G {}\n", key.len()).into_bytes()
    } else {
        format!("g {} {}\n", namespace.len(), key.len()).into_bytes()
    };
    message.extend_from_slice(namespace);
    message.extend_from_slice(key);
    message
}

/// A `G` response: a hit, a miss, or `W` — client-side replication's "your topology view
/// is stale" signal, which the workload treats differently from either
/// (see `workload_get`) rather than lumping it in with a malformed reply.
enum GetReply {
    Hit,
    Miss,
    WrongNode,
}

/// Parses the `<length>` out of a `V <length>` response line, rejecting
/// anything above `MAX_VALUE_LENGTH` before it ever reaches
/// `read_exact_into` — see that constant's doc comment. Shared by `get`
/// and `get_value`, which otherwise duplicated this parsing exactly.
fn parse_value_length(line: &str) -> io::Result<usize> {
    let length: usize = line
        .strip_prefix("V ")
        .and_then(|rest| rest.parse().ok())
        .ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("bad G response: {line:?}"),
            )
        })?;

    if length > MAX_VALUE_LENGTH {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "G response declares length={length}, above MAX_VALUE_LENGTH ({MAX_VALUE_LENGTH})"
            ),
        ));
    }

    Ok(length)
}

async fn get(
    stream: &mut TcpStream,
    buf: &mut BytesMut,
    namespace: &[u8],
    key: &[u8],
) -> io::Result<GetReply> {
    stream.write_all(&get_message(namespace, key)).await?;

    let line = read_line(stream, buf).await?;

    match line.as_str() {
        "N" => Ok(GetReply::Miss),
        "W" => Ok(GetReply::WrongNode),
        _ => {
            let length = parse_value_length(&line)?;

            read_exact_into(stream, buf, length).await?;
            let _ = buf.split_to(length);
            Ok(GetReply::Hit)
        }
    }
}

/// Like `get`, but distinguishes hit from miss instead of collapsing both
/// into "the operation succeeded" — the workload only needs the latter,
/// but `verify_handoff` needs to know whether a key it expects a node to
/// hold is actually there.
async fn get_value(
    stream: &mut TcpStream,
    buf: &mut BytesMut,
    namespace: &[u8],
    key: &[u8],
) -> io::Result<bool> {
    stream.write_all(&get_message(namespace, key)).await?;

    let line = read_line(stream, buf).await?;

    if line == "N" {
        return Ok(false);
    }

    let length = parse_value_length(&line)?;

    read_exact_into(stream, buf, length).await?;
    let _ = buf.split_to(length);
    Ok(true)
}

/// Reuses a per-owner connection across calls, dialing lazily the first
/// time a given address is addressed.
async fn get_or_connect<'a>(
    conns: &'a mut HashMap<String, (TcpStream, BytesMut)>,
    addr: &str,
) -> io::Result<&'a mut (TcpStream, BytesMut)> {
    if !conns.contains_key(addr) {
        let stream = TcpStream::connect(addr).await?;
        conns.insert(addr.to_string(), (stream, BytesMut::new()));
    }

    Ok(conns.get_mut(addr).unwrap())
}

/// A small, dependency-free PRNG (xorshift64*) — this workload just needs
/// unpredictable-enough key/node selection, not cryptographic quality.
struct Rng(u64);

impl Rng {
    fn new(seed: u64) -> Self {
        Self(seed | 1)
    }

    fn next_u64(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x >> 12;
        x ^= x << 25;
        x ^= x >> 27;
        self.0 = x;
        x.wrapping_mul(0x2545F4914F6CDD1D)
    }

    fn below(&mut self, bound: usize) -> usize {
        (self.next_u64() as usize) % bound
    }
}

#[derive(Default)]
struct Stats {
    /// (elapsed since test start, succeeded)
    log: Mutex<Vec<(Duration, bool)>>,
}

impl Stats {
    fn record(&self, elapsed: Duration, ok: bool) {
        self.log.lock().unwrap().push((elapsed, ok));
    }

    /// Buckets the log into `BUCKET_WIDTH`-wide windows and prints
    /// ops/sec + error rate per window, tagging each with which phase of
    /// the test it falls in.
    fn report(&self, join_started_at: Duration, join_promotions: &[Duration]) {
        let log = self.log.lock().unwrap();

        if log.is_empty() {
            println!("  (no workload operations recorded)");
            return;
        }

        let total_duration = log.iter().map(|(elapsed, _)| *elapsed).max().unwrap();
        let bucket_count =
            (total_duration.as_secs_f64() / BUCKET_WIDTH.as_secs_f64()).ceil() as usize + 1;
        let mut ops = vec![0u32; bucket_count];
        let mut errors = vec![0u32; bucket_count];

        for (elapsed, ok) in log.iter() {
            let bucket = (elapsed.as_secs_f64() / BUCKET_WIDTH.as_secs_f64()) as usize;
            ops[bucket] += 1;
            if !ok {
                errors[bucket] += 1;
            }
        }

        let join_finished_at = join_promotions
            .iter()
            .copied()
            .max()
            .unwrap_or(join_started_at);

        println!(
            "  {:>8}  {:>10}  {:>8}  {:>6}",
            "t (ms)", "phase", "ops/sec", "errors"
        );

        let mut before = Vec::new();
        let mut during = Vec::new();
        let mut after = Vec::new();

        for (index, count) in ops.iter().enumerate() {
            let bucket_start = BUCKET_WIDTH * index as u32;
            let phase = if bucket_start < join_started_at {
                before.push(*count);
                "before"
            } else if bucket_start <= join_finished_at {
                during.push(*count);
                "during"
            } else {
                after.push(*count);
                "after"
            };

            let ops_per_sec = *count as f64 / BUCKET_WIDTH.as_secs_f64();
            println!(
                "  {:>8}  {:>10}  {:>8.0}  {:>6}",
                bucket_start.as_millis(),
                phase,
                ops_per_sec,
                errors[index]
            );
        }

        let avg = |bucket: &[u32]| -> f64 {
            if bucket.is_empty() {
                0.0
            } else {
                bucket.iter().sum::<u32>() as f64 / bucket.len() as f64 / BUCKET_WIDTH.as_secs_f64()
            }
        };

        println!(
            "  avg ops/sec — before: {:.0}, during: {:.0}, after: {:.0}",
            avg(&before),
            avg(&during),
            avg(&after)
        );
    }
}

/// One GET, following the SDK's fallback rule: ask the primary; only on a
/// connection-level failure fall through to the next owner in rank order.
/// A `W` from the primary is neither of those — client-side replication defines it as
/// "your topology view is stale", so it's handled by refreshing the
/// roster from discovery and retrying exactly once (`refresh_and_retry`),
/// not by treating it as a failed operation.
async fn workload_get(
    discovery_addr: &str,
    conns: &mut HashMap<String, (TcpStream, BytesMut)>,
    owners: &[(String, String)],
    namespace: &[u8],
    key: &[u8],
) -> (bool, Option<io::Error>) {
    let mut last_error = None;

    for (_, addr) in owners {
        match get_or_connect(conns, addr).await {
            Ok((stream, buf)) => match get(stream, buf, namespace, key).await {
                Ok(GetReply::Hit | GetReply::Miss) => return (true, None),
                Ok(GetReply::WrongNode) => {
                    return refresh_and_retry_get(discovery_addr, conns, namespace, key).await;
                }
                Err(error) => {
                    conns.remove(addr);
                    last_error = Some(error);
                }
            },
            Err(error) => last_error = Some(error),
        }
    }

    (false, last_error)
}

/// Client-side replication's documented recovery for a `W`: "topology stale → refresh
/// and retry once". Fetches the current roster/replication straight from
/// discovery (bypassing the periodic poller, which may not have caught up
/// yet) and retries against the freshly computed primary — exactly once,
/// not by falling through further owners.
async fn refresh_and_retry_get(
    discovery_addr: &str,
    conns: &mut HashMap<String, (TcpStream, BytesMut)>,
    namespace: &[u8],
    key: &[u8],
) -> (bool, Option<io::Error>) {
    let (roster, replication) = match fetch_joined(discovery_addr).await {
        Ok(update) => update,
        Err(error) => return (false, Some(error)),
    };

    let owners = Ring::new(&roster).owners(namespace, key, replication);
    let Some((_, addr)) = owners.first() else {
        return (
            false,
            Some(io::Error::other("no owners for key after refresh")),
        );
    };

    match get_or_connect(conns, addr).await {
        Ok((stream, buf)) => match get(stream, buf, namespace, key).await {
            Ok(GetReply::Hit | GetReply::Miss) => (true, None),
            Ok(GetReply::WrongNode) => (
                false,
                Some(io::Error::other("still WrongNode after refresh-and-retry")),
            ),
            Err(error) => {
                conns.remove(addr);
                (false, Some(error))
            }
        },
        Err(error) => (false, Some(error)),
    }
}

/// One SET, following the SDK's fan-out rule: write to every owner
/// (sequentially here, not truly in parallel — this is a verification
/// harness, not a throughput benchmark); the primary's result is the
/// operation's result, and a replica failure is swallowed.
async fn workload_set(
    conns: &mut HashMap<String, (TcpStream, BytesMut)>,
    owners: &[(String, String)],
    namespace: &[u8],
    key: &[u8],
    value: &[u8],
) -> (bool, Option<io::Error>) {
    let mut primary_ok = false;
    let mut primary_error = None;

    for (index, (_, addr)) in owners.iter().enumerate() {
        let result = match get_or_connect(conns, addr).await {
            Ok((stream, buf)) => set(stream, buf, namespace, key, value).await,
            Err(error) => Err(error),
        };

        match result {
            Ok(ok) if index == 0 => primary_ok = ok,
            Err(error) => {
                conns.remove(addr);
                if index == 0 {
                    primary_error = Some(error);
                }
            }
            _ => {}
        }
    }

    (primary_ok, primary_error)
}

/// Per-worker fixed configuration, bundled so `run_worker` stays under
/// clippy's argument-count lint.
#[derive(Clone)]
struct WorkerContext {
    discovery_addr: String,
    keys: usize,
    value_size: usize,
}

/// One workload worker: owns its own connection pool (one socket per
/// owner it has talked to so far) and, per operation, recomputes the
/// key's owners from the latest roster snapshot it has seen — the roster
/// changes mid-test as a node joins, so a worker started before a join
/// must route differently after one.
async fn run_worker(
    worker_id: usize,
    ctx: WorkerContext,
    stats: std::sync::Arc<Stats>,
    test_start: Instant,
    roster_rx: watch::Receiver<(Roster, usize)>,
    stop: watch::Receiver<bool>,
) {
    let mut rng = Rng::new(0x9E3779B97F4A7C15 ^ worker_id as u64);
    let mut conns: HashMap<String, (TcpStream, BytesMut)> = HashMap::new();
    let value = vec![b'x'; ctx.value_size];

    loop {
        if *stop.borrow() {
            return;
        }

        let (roster, replication) = roster_rx.borrow().clone();
        if roster.is_empty() {
            sleep(Duration::from_millis(10)).await;
            continue;
        }

        let ring = Ring::new(&roster);
        let (namespace, key) = verify_key(rng.below(ctx.keys));
        let is_get = rng.below(10) < 8;
        let owners = ring.owners(namespace, key.as_bytes(), replication);

        let (ok, error) = if is_get {
            workload_get(
                &ctx.discovery_addr,
                &mut conns,
                &owners,
                namespace,
                key.as_bytes(),
            )
            .await
        } else {
            workload_set(&mut conns, &owners, namespace, key.as_bytes(), &value).await
        };

        let elapsed = test_start.elapsed();

        if let Some(error) = error {
            eprintln!("worker {worker_id} error against {}: {error}", owners[0].1);
            // A connect failure (the target node hasn't started listening
            // yet, or is briefly unreachable around a join) would
            // otherwise retry in a tight spin loop — pure wasted CPU, and
            // a flood of one error line per failed attempt. A few
            // milliseconds is enough to break the spin without
            // meaningfully slowing recovery once the target is reachable
            // again.
            sleep(Duration::from_millis(5)).await;
        }

        stats.record(elapsed, ok);
    }
}

/// The subset of `Args` a workload run needs, bundled so `run_workload`
/// stays under clippy's argument-count lint.
#[derive(Clone, Copy)]
struct WorkloadConfig {
    keys: usize,
    value_size: usize,
    concurrency: usize,
}

/// Runs `config.concurrency` workload workers plus a background poller
/// that keeps their view of the roster (and replication factor) current,
/// so a worker started before a join routes correctly to the joining
/// node once it's actually in the cluster.
async fn run_workload(
    discovery_addr: String,
    config: WorkloadConfig,
    stats: std::sync::Arc<Stats>,
    test_start: Instant,
    initial_roster: (Roster, usize),
    mut stop: watch::Receiver<bool>,
) {
    let (roster_tx, roster_rx) = watch::channel(initial_roster);

    let mut poller_stop = stop.clone();
    let poller_addr = discovery_addr.clone();
    let poller = tokio::spawn(async move {
        loop {
            tokio::select! {
                _ = sleep(POLL_INTERVAL) => {
                    if let Ok(update) = fetch_joined(&poller_addr).await {
                        let _ = roster_tx.send(update);
                    }
                }
                _ = poller_stop.changed() => return,
            }
        }
    });

    let ctx = WorkerContext {
        discovery_addr: discovery_addr.clone(),
        keys: config.keys,
        value_size: config.value_size,
    };
    let mut workers = Vec::new();

    for worker_id in 0..config.concurrency {
        let stats = std::sync::Arc::clone(&stats);
        workers.push(tokio::spawn(run_worker(
            worker_id,
            ctx.clone(),
            stats,
            test_start,
            roster_rx.clone(),
            stop.clone(),
        )));
    }

    let _ = stop.changed().await;

    for worker in workers {
        let _ = worker.await;
    }
    let _ = poller.await;
}

/// Seeds `keys` sequential keys, writing each to every one of its
/// Client-side replication top-R owners (as the SDK's fan-out would) rather than to a
/// single fixed node.
async fn seed_keys(
    roster: &Roster,
    replication: usize,
    keys: usize,
    value_size: usize,
) -> io::Result<()> {
    let ring = Ring::new(roster);
    let mut conns: HashMap<String, (TcpStream, BytesMut)> = HashMap::new();
    let value = vec![b'x'; value_size];

    for index in 0..keys {
        let (namespace, key) = verify_key(index);

        for (_, addr) in ring.owners(namespace, key.as_bytes(), replication) {
            let (stream, buf) = get_or_connect(&mut conns, &addr).await?;
            if !set(stream, buf, namespace, key.as_bytes(), &value).await? {
                return Err(io::Error::other(format!(
                    "seed SET for {key} to {addr} was not acknowledged"
                )));
            }
        }
    }

    Ok(())
}

/// Confirms the node that just appeared in `L` actually holds a copy of
/// every seeded key its post-join top-R membership says it should — the
/// direct, wire-observable half of "does a newly joined node actually
/// receive the keys it should" (see the module docs). The other half —
/// that a *displaced* copy elsewhere gets swept — isn't observable this
/// way: as soon as a node applies the same `M`, its own top-R check
/// (`wrong_node` in `src/server.rs`) already answers `W` for a displaced
/// key regardless of whether the sweep has physically reclaimed it yet,
/// so that half stays unit-tested only.
async fn verify_handoff(
    roster: &Roster,
    replication: usize,
    new_node_name: &str,
    keys: usize,
) -> io::Result<()> {
    let new_node_addr = roster
        .iter()
        .find(|(name, _)| name == new_node_name)
        .map(|(_, addr)| addr.clone())
        .ok_or_else(|| {
            io::Error::other(format!(
                "{new_node_name} missing from its own post-join roster"
            ))
        })?;

    let ring = Ring::new(roster);
    let mut stream = TcpStream::connect(&new_node_addr).await?;
    let mut buf = BytesMut::new();
    let mut expected = 0usize;
    let mut missing = Vec::new();

    for index in 0..keys {
        let (namespace, key) = verify_key(index);

        if !ring
            .owners(namespace, key.as_bytes(), replication)
            .iter()
            .any(|(name, _)| name == new_node_name)
        {
            continue;
        }

        expected += 1;
        if !get_value(&mut stream, &mut buf, namespace, key.as_bytes()).await? {
            missing.push(key);
        }
    }

    if missing.is_empty() {
        println!("  handoff check: {new_node_name} holds all {expected} of its owned keys");
        Ok(())
    } else {
        Err(io::Error::other(format!(
            "{new_node_name} is missing {}/{expected} keys it should own after joining \
             (first few: {:?})",
            missing.len(),
            &missing[..missing.len().min(5)]
        )))
    }
}

/// Polls discovery's `L` until a node whose name isn't in `already_known`
/// appears, returning the roster and replication factor at that moment,
/// the new node's name, and how long the wait took. Used to measure one
/// join's handoff duration without needing to know the new node's random
/// Node identity decoupled from address name in advance.
async fn wait_for_new_joined_node(
    discovery_addr: &str,
    already_known: &HashSet<String>,
    started_at: Instant,
) -> io::Result<(Roster, usize, String, Duration)> {
    timeout(JOIN_TIMEOUT, async {
        loop {
            if let Ok((roster, replication)) = fetch_joined(discovery_addr).await
                && let Some(name) = roster
                    .iter()
                    .find(|(name, _)| !already_known.contains(name))
                    .map(|(name, _)| name.clone())
            {
                return (roster, replication, name, started_at.elapsed());
            }

            sleep(POLL_INTERVAL).await;
        }
    })
    .await
    .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "no new node appeared in L in time"))
}

async fn run_simple_join(
    discovery_bin: &Path,
    node_bin: &Path,
    log_dir: &Path,
    args: &Args,
    base_port: u16,
    initial_nodes: usize,
) -> io::Result<()> {
    let discovery_port = base_port;
    let discovery = spawn_discovery(discovery_bin, log_dir, discovery_port)?;
    wait_until_connectable(&discovery.addr).await?;

    let mut nodes = Vec::new();
    for offset in 0..initial_nodes {
        let node = spawn_node(
            node_bin,
            log_dir,
            base_port + 1 + offset as u16,
            &discovery.addr,
        )?;
        wait_until_connectable(&node.addr).await?;
        nodes.push(node);
    }

    let (roster_before, replication) = wait_for_all_joined(&discovery.addr, initial_nodes).await?;
    let known_before: HashSet<String> =
        roster_before.iter().map(|(name, _)| name.clone()).collect();

    seed_keys(&roster_before, replication, args.keys, args.value_size).await?;
    println!(
        "  seeded {} keys across {} node(s) (R={replication})",
        args.keys,
        roster_before.len()
    );

    let stats = std::sync::Arc::new(Stats::default());
    let (stop_tx, stop_rx) = watch::channel(false);
    let test_start = Instant::now();

    let workload = tokio::spawn(run_workload(
        discovery.addr.clone(),
        WorkloadConfig {
            keys: args.keys,
            value_size: args.value_size,
            concurrency: args.concurrency,
        },
        std::sync::Arc::clone(&stats),
        test_start,
        (roster_before, replication),
        stop_rx,
    ));

    // A brief "before" baseline before the new node starts joining.
    sleep(Duration::from_millis(500)).await;

    let join_started_at = test_start.elapsed();
    let joining_node = spawn_node(
        node_bin,
        log_dir,
        base_port + 1 + initial_nodes as u16,
        &discovery.addr,
    )?;
    wait_until_connectable(&joining_node.addr).await?;

    let (roster_after, replication_after, new_name, join_duration) =
        wait_for_new_joined_node(&discovery.addr, &known_before, Instant::now()).await?;
    println!("  node {new_name} joined in {join_duration:?}");

    verify_handoff(&roster_after, replication_after, &new_name, args.keys).await?;

    // A brief "after" window to see recovery.
    sleep(Duration::from_millis(1000)).await;

    let _ = stop_tx.send(true);
    let _ = workload.await;

    stats.report(join_started_at, &[join_started_at + join_duration]);

    Ok(())
}

async fn run_waiting_join(
    discovery_bin: &Path,
    node_bin: &Path,
    log_dir: &Path,
    args: &Args,
    base_port: u16,
) -> io::Result<()> {
    let discovery_port = base_port;
    let discovery = spawn_discovery(discovery_bin, log_dir, discovery_port)?;
    wait_until_connectable(&discovery.addr).await?;

    let first = spawn_node(node_bin, log_dir, base_port + 1, &discovery.addr)?;
    wait_until_connectable(&first.addr).await?;
    let (roster_before, replication) = wait_for_all_joined(&discovery.addr, 1).await?;
    let known_before: HashSet<String> =
        roster_before.iter().map(|(name, _)| name.clone()).collect();

    seed_keys(&roster_before, replication, args.keys, args.value_size).await?;
    println!("  seeded {} keys on {}", args.keys, first.addr);

    let stats = std::sync::Arc::new(Stats::default());
    let (stop_tx, stop_rx) = watch::channel(false);
    let test_start = Instant::now();

    let workload = tokio::spawn(run_workload(
        discovery.addr.clone(),
        WorkloadConfig {
            keys: args.keys,
            value_size: args.value_size,
            concurrency: args.concurrency,
        },
        std::sync::Arc::clone(&stats),
        test_start,
        (roster_before, replication),
        stop_rx,
    ));

    sleep(Duration::from_millis(500)).await;

    let join_started_at = test_start.elapsed();

    // Two nodes ask to join at nearly the same time; only one may be
    // `Joining` at once (staged node join), so the second should be visibly
    // delayed behind the first.
    let second = spawn_node(node_bin, log_dir, base_port + 2, &discovery.addr)?;
    let third = spawn_node(node_bin, log_dir, base_port + 3, &discovery.addr)?;
    wait_until_connectable(&second.addr).await?;
    wait_until_connectable(&third.addr).await?;

    let poll_start = Instant::now();
    let (roster_after_first, replication_after_first, first_new, first_duration) =
        wait_for_new_joined_node(&discovery.addr, &known_before, poll_start).await?;
    verify_handoff(
        &roster_after_first,
        replication_after_first,
        &first_new,
        args.keys,
    )
    .await?;

    let mut known_after_first = known_before.clone();
    known_after_first.insert(first_new.clone());

    let (roster_after_second, replication_after_second, second_new, second_duration) =
        wait_for_new_joined_node(&discovery.addr, &known_after_first, poll_start).await?;
    verify_handoff(
        &roster_after_second,
        replication_after_second,
        &second_new,
        args.keys,
    )
    .await?;

    println!("  first new node ({first_new}) joined in {first_duration:?}");
    println!("  second new node ({second_new}) joined in {second_duration:?}");
    println!(
        "  serialization gap: {:?} (should be well above 0 — a non-trivial join takes real \
         time, so the second node must visibly wait behind the first rather than being \
         promoted alongside it)",
        second_duration.saturating_sub(first_duration)
    );

    sleep(Duration::from_millis(1000)).await;

    let _ = stop_tx.send(true);
    let _ = workload.await;

    stats.report(
        join_started_at,
        &[
            join_started_at + first_duration,
            join_started_at + second_duration,
        ],
    );

    Ok(())
}

async fn wait_for_all_joined(discovery_addr: &str, expected: usize) -> io::Result<(Roster, usize)> {
    timeout(JOIN_TIMEOUT, async {
        loop {
            if let Ok((roster, replication)) = fetch_joined(discovery_addr).await
                && roster.len() >= expected
            {
                return (roster, replication);
            }

            sleep(POLL_INTERVAL).await;
        }
    })
    .await
    .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "nodes never all appeared in L"))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Pinned outputs of the full score pipeline
    /// (`fmix64(fnv1a(name) ^ fnv1a(key))`) — `src/hash_ring.rs` and the
    /// TypeScript SDK assert these same exact values, so this harness's
    /// independent port must agree byte-for-byte or one of these tests
    /// fails.
    #[test]
    fn matches_the_cross_implementation_score_vectors() {
        assert_eq!(fmix64(0), 0);
        assert_eq!(fmix64(1), 0xb456bcfc34c2cb2c);
        assert_eq!(fmix64(0xcbf29ce484222325), 0xefd01f60ba992926);

        let nodes: Roster = ["node-a", "node-b", "node-c"]
            .into_iter()
            .map(|name| (name.to_string(), String::new()))
            .collect();
        let ring = Ring::new(&nodes);
        let names =
            |roster: Roster| -> Vec<String> { roster.into_iter().map(|(name, _)| name).collect() };

        assert_eq!(
            names(ring.owners(b"", b"alpha", 3)),
            vec!["node-c", "node-b", "node-a"]
        );
        assert_eq!(
            names(ring.owners(b"", b"beta", 3)),
            vec!["node-a", "node-c", "node-b"]
        );
        assert_eq!(
            names(ring.owners(b"", b"", 3)),
            vec!["node-a", "node-b", "node-c"]
        );

        // Namespaced form (issue #105) — the same vectors `src/hash_ring.rs`
        // and every SDK pin.
        assert_eq!(key_hash(b"users", b"alpha"), 0xfd4ab55027c21df6);
        assert_eq!(key_hash(b"users", b""), 0xa9e9bbca44bb502e);
        assert_eq!(key_hash(b"\xff\x00", b"beta"), 0x8f7c097eccb8e792);
        assert_eq!(
            names(ring.owners(b"users", b"alpha", 3)),
            vec!["node-a", "node-c", "node-b"]
        );
        assert_eq!(
            names(ring.owners(b"users", b"", 3)),
            vec!["node-b", "node-c", "node-a"]
        );
        assert_eq!(
            names(ring.owners(b"\xff\x00", b"beta", 3)),
            vec!["node-b", "node-a", "node-c"]
        );
    }

    #[test]
    fn namespaced_frames_lead_with_the_namespace_length() {
        assert_eq!(get_message(b"", b"name"), b"G 4\nname".to_vec());
        assert_eq!(get_message(b"users", b"name"), b"g 5 4\nusersname".to_vec());
        assert_eq!(set_header(b"", b"name", 5), b"S 4 5\n".to_vec());
        assert_eq!(set_header(b"users", b"name", 5), b"s 5 4 5\n".to_vec());
    }

    #[test]
    fn seeded_keys_alternate_between_the_default_and_a_namespace() {
        assert_eq!(verify_key(0), (&b""[..], "verify-key-0".to_string()));
        assert_eq!(
            verify_key(1),
            (VERIFY_NAMESPACE, "verify-key-1".to_string())
        );
    }

    #[test]
    fn owners_are_capped_by_roster_size() {
        let nodes: Roster = ["a", "b", "c"]
            .into_iter()
            .map(|name| (name.to_string(), String::new()))
            .collect();
        let ring = Ring::new(&nodes);
        assert_eq!(ring.owners(b"", b"some-key", 10).len(), 3);
    }

    #[test]
    fn parse_value_length_accepts_an_ordinary_length() {
        assert_eq!(parse_value_length("V 5").unwrap(), 5);
        assert_eq!(parse_value_length("V 0").unwrap(), 0);
    }

    #[test]
    fn parse_value_length_accepts_exactly_max_value_length() {
        assert_eq!(
            parse_value_length(&format!("V {MAX_VALUE_LENGTH}")).unwrap(),
            MAX_VALUE_LENGTH
        );
    }

    #[test]
    fn parse_value_length_rejects_a_length_above_max_value_length() {
        // Regression: a corrupt or malicious `V <length>` header used to
        // be trusted outright, so a claimed length far beyond anything
        // the real server would ever send could drive `read_exact_into`
        // into blocking on however many bytes the bogus header claims
        // (or, if it were used to size an allocation, an oversized one).
        let error = parse_value_length(&format!("V {}", MAX_VALUE_LENGTH + 1))
            .expect_err("expected an error");
        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
    }

    #[test]
    fn parse_value_length_rejects_a_malformed_line() {
        assert!(parse_value_length("garbage").is_err());
        assert!(parse_value_length("V not-a-number").is_err());
    }

    /// Writes a discovery `L` response's header line — `N <count> <r>` —
    /// straight to `stream`, for a test acting as a fake discovery server.
    async fn write_list_header(stream: &mut TcpStream, count: usize, replication: usize) {
        stream
            .write_all(format!("N {count} {replication}\n").as_bytes())
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn fetch_joined_rejects_a_declared_entry_count_above_the_bound() {
        // Regression: a corrupt/malicious `N <count> <r>` header used to
        // be trusted outright, so a huge declared `count` could drive
        // `Vec::with_capacity(count)` into an oversized allocation before
        // a single entry had even been read — see `MAX_ROSTER_ENTRIES`.
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();

        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut request = [0u8; 2];
            stream.read_exact(&mut request).await.unwrap();
            assert_eq!(&request, b"L\n");

            // Never sends a single entry — the bound check must reject
            // based on the header alone.
            write_list_header(&mut stream, MAX_ROSTER_ENTRIES + 1, 2).await;
        });

        let error = fetch_joined(&addr).await.expect_err("expected an error");
        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
        server.await.unwrap();
    }

    #[tokio::test]
    async fn fetch_joined_rejects_a_declared_entry_name_length_above_the_bound() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();

        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut request = [0u8; 2];
            stream.read_exact(&mut request).await.unwrap();
            assert_eq!(&request, b"L\n");

            write_list_header(&mut stream, 1, 2).await;
            // Declares a name_length far beyond anything a real node identity decoupled from address
            // name is, and never sends a body that large — the bound
            // check must reject based on the entry header alone.
            stream
                .write_all(format!("{} 3\n", MAX_NAME_OR_ADDR_LENGTH + 1).as_bytes())
                .await
                .unwrap();
        });

        let error = fetch_joined(&addr).await.expect_err("expected an error");
        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
        server.await.unwrap();
    }

    #[tokio::test]
    async fn fetch_joined_accepts_a_well_formed_response() {
        // Control test: the new bound checks must not reject an
        // ordinary, legitimate response.
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();

        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut request = [0u8; 2];
            stream.read_exact(&mut request).await.unwrap();
            assert_eq!(&request, b"L\n");

            write_list_header(&mut stream, 1, 2).await;
            stream
                .write_all(b"6 14\nnode-a127.0.0.1:8356\n")
                .await
                .unwrap();
        });

        let (roster, replication) = fetch_joined(&addr).await.unwrap();
        assert_eq!(replication, 2);
        assert_eq!(
            roster,
            vec![("node-a".to_string(), "127.0.0.1:8356".to_string())]
        );
        server.await.unwrap();
    }
}
