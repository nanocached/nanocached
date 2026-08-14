//! Async, multi-threaded load client for the Kvelo TCP protocol.
//!
//! `tools/kvelo_bench.py` is GIL-bound and cannot drive enough concurrent
//! I/O to saturate the server; this binary exists to find kvelo's actual
//! ceiling.
//!
//! There is deliberately no pipelining option. kvelo's target workload is
//! one lookup per client request (e.g. a web request checking a session or
//! a cached record), which cannot be pipelined because the caller doesn't
//! know the next key before seeing the current result. A pipeline flag
//! invites optimizing for a throughput number the real workload doesn't
//! produce.

use bytes::{Buf, BufMut, BytesMut};
use std::process::ExitCode;
use std::sync::Arc;
use std::time::Instant;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::Barrier;
use tokio::task::JoinSet;

#[derive(Clone, Copy, PartialEq, Eq)]
enum Workload {
    Get,
    Set,
    Mixed,
}

struct Args {
    host: String,
    port: u16,
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
    stream: &mut TcpStream,
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

async fn worker(id: u64, args: Arc<Args>, barrier: Arc<Barrier>) -> WorkerResult {
    let mut result = WorkerResult::default();

    let mut stream = match TcpStream::connect((args.host.as_str(), args.port)).await {
        Ok(stream) => stream,
        Err(error) => {
            result.error = Some(format!("connect: {error}"));
            barrier.wait().await;
            return result;
        }
    };
    let _ = stream.set_nodelay(true);

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
        let key = format!("kvelo:{}", rng.below(args.keys));
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

        let started = Instant::now();

        if let Err(error) = stream.write_all(&send_buf).await {
            result.error = Some(format!("write: {error}"));
            break;
        }
        result.bytes_sent += send_buf.len() as u64;

        match read_response(&mut stream, &mut recv_buf).await {
            Ok((kind, len)) => {
                result.bytes_received += len as u64;
                let valid = if is_get {
                    kind == b'V' || kind == b'N'
                } else {
                    kind == b'S'
                };
                if !valid {
                    result.error = Some(format!("unexpected response byte: {kind:#x}"));
                    break;
                }
            }
            Err(error) => {
                result.error = Some(format!("read: {error}"));
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
    let args = match parse_args() {
        Ok(args) => args,
        Err(message) => {
            eprintln!("{message}");
            return ExitCode::FAILURE;
        }
    };
    let args = Arc::new(args);

    let barrier = Arc::new(Barrier::new(args.connections as usize + 1));
    let mut workers = JoinSet::new();

    for id in 0..args.connections {
        workers.spawn(worker(id, Arc::clone(&args), Arc::clone(&barrier)));
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
