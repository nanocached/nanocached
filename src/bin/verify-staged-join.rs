//! Retained ADR-0008 verification/demonstration harness — not a load
//! testing tool (see `src/bin/bench.rs` for that, which this binary is
//! deliberately kept separate from per the user's request). Spawns real
//! `nanocached-discovery`/`nanocached-node` processes as subprocesses and
//! drives them exactly like a real client would (raw TCP, the same wire
//! protocol any SDK speaks), to empirically check staged-join behavior
//! rather than reason about it from the ADR text alone:
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
//! Scenarios: `1-to-2`, `2-to-3`, `1-to-3-waiting` (two nodes join at
//! once; the second must wait behind the first). Pass `--scenario <name>`
//! to run one, or omit it to run all three in sequence.
//!
//! This binary has no dependency on the node/discovery implementations
//! (see `src/bin/bench.rs` for the same independence rule and rationale,
//! per ADR-0006): it only spawns the sibling binaries as subprocesses and
//! speaks the wire protocol to them, with its own minimal copy of just
//! the pieces it needs (`A`/`G`/`S` and discovery's `L`). No TLS/auth
//! support yet — this is for local verification (see ADR-0008's Context);
//! add it if/when AWS verification needs it.

use bytes::BytesMut;
use std::collections::HashSet;
use std::env;
use std::io;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::Mutex;
use std::time::{Duration, Instant};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::process::{Child, Command};
use tokio::time::{sleep, timeout};

const CONNECT_RETRY_INTERVAL: Duration = Duration::from_millis(50);
const CONNECT_TIMEOUT: Duration = Duration::from_secs(5);
const JOIN_TIMEOUT: Duration = Duration::from_secs(60);
const POLL_INTERVAL: Duration = Duration::from_millis(50);
const BUCKET_WIDTH: Duration = Duration::from_millis(250);

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

/// Fetches the current `Joined` node list from discovery, in the
/// ADR-0009 `<name-length> <addr-length>\n<name><addr>` shape.
async fn fetch_joined(discovery_addr: &str) -> io::Result<Vec<(String, String)>> {
    let mut stream = TcpStream::connect(discovery_addr).await?;
    stream.write_all(b"L\n").await?;

    let mut buf = BytesMut::new();
    let header = read_line(&mut stream, &mut buf).await?;
    // `N <count> <r>\n` since ADR-0011 (the replication factor rides
    // along for clients; this harness doesn't need it).
    let count: usize = header
        .strip_prefix("N ")
        .and_then(|rest| rest.split(' ').next())
        .and_then(|count| count.parse().ok())
        .ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("bad L header: {header:?}"),
            )
        })?;

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

        // +1 for the trailing '\n' discovery writes after each entry's
        // <name><addr> body (see nanocached-discovery.rs's `L` handler).
        read_exact_into(&mut stream, &mut buf, name_length + addr_length + 1).await?;
        let entry = buf.split_to(name_length + addr_length + 1);
        let name = String::from_utf8_lossy(&entry[..name_length]).into_owned();
        let addr =
            String::from_utf8_lossy(&entry[name_length..name_length + addr_length]).into_owned();

        nodes.push((name, addr));
    }

    Ok(nodes)
}

async fn set(
    stream: &mut TcpStream,
    buf: &mut BytesMut,
    key: &[u8],
    value: &[u8],
) -> io::Result<bool> {
    let mut message = format!("S {} {}\n", key.len(), value.len()).into_bytes();
    message.extend_from_slice(key);
    message.extend_from_slice(value);
    stream.write_all(&message).await?;

    let line = read_line(stream, buf).await?;
    Ok(line == "S")
}

async fn get(stream: &mut TcpStream, buf: &mut BytesMut, key: &[u8]) -> io::Result<bool> {
    let mut message = format!("G {}\n", key.len()).into_bytes();
    message.extend_from_slice(key);
    stream.write_all(&message).await?;

    let line = read_line(stream, buf).await?;

    if line == "N" {
        return Ok(true);
    }

    let length: usize = line
        .strip_prefix("V ")
        .and_then(|rest| rest.parse().ok())
        .ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("bad G response: {line:?}"),
            )
        })?;

    read_exact_into(stream, buf, length).await?;
    let _ = buf.split_to(length);
    Ok(true)
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

/// Runs `concurrency` workers, each holding one persistent connection to
/// a randomly chosen node from `targets` (fixed for the worker's
/// lifetime — this deliberately measures whether an *existing*
/// connection to an *existing* node sees degraded service while that
/// node is busy migrating data elsewhere, not whether new connections
/// get routed around it), issuing GET (mostly) / SET as fast as
/// possible against `--keys` keys until `stop` fires.
async fn run_workload(
    targets: Vec<String>,
    keys: usize,
    value_size: usize,
    concurrency: usize,
    stats: std::sync::Arc<Stats>,
    test_start: Instant,
    mut stop: tokio::sync::watch::Receiver<bool>,
) {
    let mut workers = Vec::new();

    for worker_id in 0..concurrency {
        let targets = targets.clone();
        let stats = std::sync::Arc::clone(&stats);
        let stop = stop.clone();

        workers.push(tokio::spawn(async move {
            let mut rng = Rng::new(0x9E3779B97F4A7C15 ^ worker_id as u64);
            let target = &targets[rng.below(targets.len())];

            let mut stream = match TcpStream::connect(target).await {
                Ok(stream) => stream,
                Err(_) => return,
            };
            let mut buf = BytesMut::new();
            let value = vec![b'x'; value_size];

            loop {
                if *stop.borrow() {
                    return;
                }

                let key = format!("verify-key-{}", rng.below(keys));
                let is_get = rng.below(10) < 8;

                let result = if is_get {
                    get(&mut stream, &mut buf, key.as_bytes()).await
                } else {
                    set(&mut stream, &mut buf, key.as_bytes(), &value).await
                };

                let elapsed = test_start.elapsed();

                match result {
                    Ok(ok) => stats.record(elapsed, ok),
                    Err(error) => {
                        eprintln!("worker {worker_id} error against {target}: {error}");
                        stats.record(elapsed, false);
                        // Reconnect to the same target and keep going —
                        // a transient error here is itself part of what
                        // this harness wants to observe, not a reason to
                        // give up on the worker.
                        stream = match TcpStream::connect(target).await {
                            Ok(stream) => stream,
                            Err(_) => return,
                        };
                    }
                }
            }
        }));
    }

    let _ = stop.changed().await;

    for worker in workers {
        let _ = worker.await;
    }
}

async fn seed_keys(target: &str, keys: usize, value_size: usize) -> io::Result<()> {
    let mut stream = TcpStream::connect(target).await?;
    let mut buf = BytesMut::new();
    let value = vec![b'x'; value_size];

    for index in 0..keys {
        let key = format!("verify-key-{index}");
        if !set(&mut stream, &mut buf, key.as_bytes(), &value).await? {
            return Err(io::Error::other("seed SET was not acknowledged"));
        }
    }

    Ok(())
}

/// Polls discovery's `L` until a node whose name isn't in `already_known`
/// appears, returning how long that took and that node's name. Used to
/// measure one join's handoff duration without needing to know the new
/// node's random ADR-0009 name in advance.
async fn wait_for_new_joined_node(
    discovery_addr: &str,
    already_known: &HashSet<String>,
    started_at: Instant,
) -> io::Result<(String, Duration)> {
    timeout(JOIN_TIMEOUT, async {
        loop {
            if let Ok(nodes) = fetch_joined(discovery_addr).await
                && let Some((name, _)) =
                    nodes.iter().find(|(name, _)| !already_known.contains(name))
            {
                return (name.clone(), started_at.elapsed());
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

    let known_before: HashSet<String> = wait_for_all_joined(&discovery.addr, initial_nodes).await?;

    seed_keys(&nodes[0].addr, args.keys, args.value_size).await?;
    println!("  seeded {} keys on {}", args.keys, nodes[0].addr);

    let targets: Vec<String> = nodes.iter().map(|node| node.addr.clone()).collect();
    let stats = std::sync::Arc::new(Stats::default());
    let (stop_tx, stop_rx) = tokio::sync::watch::channel(false);
    let test_start = Instant::now();

    let workload = tokio::spawn(run_workload(
        targets,
        args.keys,
        args.value_size,
        args.concurrency,
        std::sync::Arc::clone(&stats),
        test_start,
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

    let (new_name, join_duration) =
        wait_for_new_joined_node(&discovery.addr, &known_before, Instant::now()).await?;
    println!("  node {new_name} joined in {join_duration:?}");

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
    let known_before = wait_for_all_joined(&discovery.addr, 1).await?;

    seed_keys(&first.addr, args.keys, args.value_size).await?;
    println!("  seeded {} keys on {}", args.keys, first.addr);

    let targets = vec![first.addr.clone()];
    let stats = std::sync::Arc::new(Stats::default());
    let (stop_tx, stop_rx) = tokio::sync::watch::channel(false);
    let test_start = Instant::now();

    let workload = tokio::spawn(run_workload(
        targets,
        args.keys,
        args.value_size,
        args.concurrency,
        std::sync::Arc::clone(&stats),
        test_start,
        stop_rx,
    ));

    sleep(Duration::from_millis(500)).await;

    let join_started_at = test_start.elapsed();

    // Two nodes ask to join at nearly the same time; only one may be
    // `Joining` at once (ADR-0008), so the second should be visibly
    // delayed behind the first.
    let second = spawn_node(node_bin, log_dir, base_port + 2, &discovery.addr)?;
    let third = spawn_node(node_bin, log_dir, base_port + 3, &discovery.addr)?;
    wait_until_connectable(&second.addr).await?;
    wait_until_connectable(&third.addr).await?;

    let poll_start = Instant::now();
    let (first_new, first_duration) =
        wait_for_new_joined_node(&discovery.addr, &known_before, poll_start).await?;
    let mut known_after_first = known_before.clone();
    known_after_first.insert(first_new.clone());

    let (second_new, second_duration) =
        wait_for_new_joined_node(&discovery.addr, &known_after_first, poll_start).await?;

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

async fn wait_for_all_joined(discovery_addr: &str, expected: usize) -> io::Result<HashSet<String>> {
    timeout(JOIN_TIMEOUT, async {
        loop {
            if let Ok(nodes) = fetch_joined(discovery_addr).await
                && nodes.len() >= expected
            {
                return nodes.into_iter().map(|(name, _)| name).collect();
            }

            sleep(POLL_INTERVAL).await;
        }
    })
    .await
    .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "nodes never all appeared in L"))
}
