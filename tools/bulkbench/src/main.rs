// Issue #128/#150: throwaway benchmark comparing a client-side loop of
// single-key frames against a batched multi-op frame (`m`/multi-get,
// `o`/multi-set), direct-to-node and via the proxy. Not a shipped tool —
// its only job is producing the numbers #128's/#150's decisions rest on.
// Speaks the wire directly (no SDK): the loop arm needs no ownership
// routing at all when pointed at a single node, and the proxy's own
// owner-grouping is exactly the fan-out path this issue's "does bulk pay
// through the proxy" question is about, so nothing here needs to
// re-derive HRW placement — see the plan's "why loopback is a sufficient
// screen" section for why a single node (zero routing) and the proxy
// (realistic routing) are the two cells worth measuring, without a
// third "raw multi-node" cell in between.
use std::env;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex as StdMutex};
use std::time::{Duration, Instant};
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpStream;
use tokio::task::JoinError;
use tokio::time::timeout;

/// Mirrors sdk/rust/src/connection.rs's MAX_VALUE_LENGTH — a declared
/// per-value length beyond this is corrupt or hostile, never a
/// legitimate value from this benchmark's own value-size argument.
const MAX_VALUE_LEN: usize = 2 * 1024 * 1024;

/// Mirrors sdk/rust/src/connection.rs's MAX_MULTI_GET_RESPONSE_BYTES —
/// bounds an `M` reply's hit bodies summed across the whole reply.
const MAX_MULTI_RESPONSE_BYTES: usize = 64 * 1024 * 1024;

/// Mirrors src/server.rs's MAX_REQUEST_SIZE — a multi-set (`o`) frame
/// this benchmark builds must fit under this or the server will reject
/// it, which we'd rather report clearly than hit as a write/read panic.
const MAX_REQUEST_SIZE: usize = 1024 * 1024;

/// Bounds every connect/read/write this benchmark makes over its raw
/// socket. Without this, a peer that accepts a connection and then goes
/// silent (crashed-but-open, deadlocked, or a bug on the other end) would
/// hang a worker task forever instead of failing fast with a clear error.
/// Mirrors the `IO_TIMEOUT` pattern in `src/bin/verify-staged-join.rs`
/// (issue #329).
const IO_TIMEOUT: Duration = Duration::from_secs(10);

/// Runs a single network operation (`connect`/`read_line`/`read_exact`/
/// `write_all`) bounded by `IO_TIMEOUT`, panicking with a clear message on
/// either a timeout or an I/O error rather than hanging or propagating a
/// bare `io::Error` up through `.expect()`. `what` names the operation for
/// the panic message.
async fn timed<F, T>(future: F, what: &str) -> T
where
    F: std::future::Future<Output = std::io::Result<T>>,
{
    match timeout(IO_TIMEOUT, future).await {
        Ok(result) => result.unwrap_or_else(|e| panic!("{what}: {e}")),
        Err(_) => panic!("{what} timed out after {IO_TIMEOUT:?}"),
    }
}

fn usage() -> ! {
    eprintln!(
        "usage:\n  \
         bulkbench preload <addr> <keyspace> <value-size>\n  \
         bulkbench run <addr> <get|set> <loop|bulk> <keyspace> <batch> <concurrency> \
         <duration-secs> <value-size>\n\n\
         Optional env: BULKBENCH_SECRET (sent as an untagged `A` before either subcommand)."
    );
    std::process::exit(2);
}

fn key_bytes(index: u64) -> Vec<u8> {
    format!("bk:{index}").into_bytes()
}

/// xorshift64* — good enough for picking benchmark keys, and avoids
/// pulling in the `rand` crate for a throwaway tool. Deterministic given
/// the same seed, which is what makes a re-run's numbers comparable
/// (see the plan's verification step 4).
struct Lcg(u64);

impl Lcg {
    fn new(seed: u64) -> Self {
        Self(seed | 1)
    }

    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }
}

async fn authenticate(stream: &mut TcpStream) {
    let Ok(secret) = env::var("BULKBENCH_SECRET") else {
        return;
    };
    let mut frame = format!("A {}\n", secret.len()).into_bytes();
    frame.extend_from_slice(secret.as_bytes());
    timed(stream.write_all(&frame), "write auth frame").await;

    let mut reader = BufReader::new(&mut *stream);
    let mut line = String::new();
    timed(reader.read_line(&mut line), "read auth reply").await;
    assert!(line.starts_with("On"), "auth rejected: {}", line.trim_end());
}

fn encode_get(key: &[u8]) -> Vec<u8> {
    let mut frame = format!("G {}\n", key.len()).into_bytes();
    frame.extend_from_slice(key);
    frame
}

fn encode_set(key: &[u8], value: &[u8]) -> Vec<u8> {
    let mut frame = format!("S {} {}\n", key.len(), value.len()).into_bytes();
    frame.extend_from_slice(key);
    frame.extend_from_slice(value);
    frame
}

fn encode_multi_get(keys: &[Vec<u8>]) -> Vec<u8> {
    let mut header = format!("m 0 {}", keys.len());
    for key in keys {
        header.push_str(&format!(" {}", key.len()));
    }
    header.push('\n');
    let mut frame = header.into_bytes();
    for key in keys {
        frame.extend_from_slice(key);
    }
    frame
}

fn encode_multi_set(keys: &[Vec<u8>], value: &[u8]) -> Vec<u8> {
    let mut lengths = String::new();
    for key in keys {
        lengths.push_str(&format!(" {} {}", key.len(), value.len()));
    }
    let header = format!("o 0 {}{lengths}\n", keys.len());
    let mut frame = header.into_bytes();
    for key in keys {
        frame.extend_from_slice(key);
        frame.extend_from_slice(value);
    }
    frame
}

/// Reads one `S` reply (bare `S\n`) and asserts it succeeded.
async fn read_set_reply<R: AsyncBufReadExt + Unpin>(reader: &mut R) {
    let mut line = String::new();
    timed(reader.read_line(&mut line), "read reply header").await;
    assert_eq!(line.trim_end(), "S", "unexpected reply to S: {line:?}");
}

/// Reads one `o` reply (`O <n> <r-1>...<r-n>\n`, no body) and returns how
/// many of the `n` entries were `S` (stored) — used only to sanity-check
/// the batch against what was requested.
async fn read_multi_ack_reply<R: AsyncBufReadExt + Unpin>(reader: &mut R) -> (usize, usize) {
    let mut line = String::new();
    timed(reader.read_line(&mut line), "read reply header").await;
    let line = line.trim_end();
    let mut fields = line.split(' ');
    assert_eq!(fields.next(), Some("O"), "unexpected reply to o: {line:?}");
    let count: usize = fields
        .next()
        .expect("missing O count field")
        .parse()
        .expect("bad O count field");

    let stored = (0..count)
        .map(|_| fields.next().expect("missing O roster token"))
        .filter(|&token| token == "S")
        .count();

    (count, stored)
}

/// Reads one `G` reply (`V <len>\n<value>` or `N\n`) and discards the
/// value bytes — the benchmark only needs the round trip's timing and a
/// byte count, not the payload itself.
async fn read_get_reply<R: AsyncBufReadExt + AsyncReadExt + Unpin>(reader: &mut R) -> bool {
    let mut line = String::new();
    timed(reader.read_line(&mut line), "read reply header").await;
    let line = line.trim_end();

    if let Some(length) = line.strip_prefix("V ") {
        let length: usize = length.parse().expect("bad value length");
        assert!(
            length <= MAX_VALUE_LEN,
            "V length {length} exceeds sanity cap {MAX_VALUE_LEN}"
        );
        let mut value = vec![0u8; length];
        timed(reader.read_exact(&mut value), "read value").await;
        true
    } else if line == "N" {
        false
    } else {
        panic!("unexpected reply to G: {line:?}");
    }
}

/// Reads one `m` reply (`M <n> <r-1>...<r-n>\n<hit values>`), discards
/// every value, and returns how many of the `n` entries were hits — used
/// only to sanity-check the batch against what was requested.
async fn read_multi_reply<R: AsyncBufReadExt + AsyncReadExt + Unpin>(
    reader: &mut R,
) -> (usize, usize) {
    let mut line = String::new();
    timed(reader.read_line(&mut line), "read reply header").await;
    let line = line.trim_end();
    let mut fields = line.split(' ');
    assert_eq!(fields.next(), Some("M"), "unexpected reply to m: {line:?}");
    let count: usize = fields
        .next()
        .expect("missing M count field")
        .parse()
        .expect("bad M count field");

    let mut total_bytes = 0usize;
    let mut hits = 0usize;
    for _ in 0..count {
        match fields.next().expect("missing M roster token") {
            "-" | "W" => {}
            length => {
                let length: usize = length.parse().expect("bad M roster length");
                assert!(
                    length <= MAX_VALUE_LEN,
                    "M entry length {length} exceeds sanity cap {MAX_VALUE_LEN}"
                );
                total_bytes += length;
                hits += 1;
            }
        }
    }
    assert!(
        total_bytes <= MAX_MULTI_RESPONSE_BYTES,
        "M reply total {total_bytes} exceeds sanity cap {MAX_MULTI_RESPONSE_BYTES}"
    );

    let mut body = vec![0u8; total_bytes];
    timed(reader.read_exact(&mut body), "read M values").await;
    (count, hits)
}

fn percentile(sorted_ms: &[f64], p: f64) -> f64 {
    if sorted_ms.is_empty() {
        return 0.0;
    }
    let index = ((sorted_ms.len() - 1) as f64 * p).round() as usize;
    sorted_ms[index]
}

async fn preload(args: &[String]) {
    let [addr, keyspace, value_size] = args else {
        usage()
    };
    let keyspace: u64 = keyspace.parse().expect("bad keyspace");
    let value_size: usize = value_size.parse().expect("bad value-size");
    let value = vec![b'x'; value_size];

    let mut stream = timed(TcpStream::connect(addr), "connect").await;
    authenticate(&mut stream).await;
    let mut reader = BufReader::new(stream);

    for index in 0..keyspace {
        let frame = encode_set(&key_bytes(index), &value);
        timed(reader.get_mut().write_all(&frame), "write set").await;
        let mut line = String::new();
        timed(reader.read_line(&mut line), "read set reply").await;
        assert_eq!(line.trim_end(), "S", "set failed for key {index}");
    }

    println!("{{\"preloaded\":{keyspace},\"value_size\":{value_size}}}");
}

async fn run(args: &[String]) {
    let [addr, op, arm, keyspace, batch, concurrency, duration, value_size] = args else {
        usage()
    };
    let keyspace: u64 = keyspace.parse().expect("bad keyspace");
    let batch: usize = batch.parse().expect("bad batch");
    let concurrency: usize = concurrency.parse().expect("bad concurrency");
    let duration = Duration::from_secs_f64(duration.parse().expect("bad duration"));
    let value_size: usize = value_size.parse().expect("bad value-size");
    let bulk = match arm.as_str() {
        "loop" => false,
        "bulk" => true,
        _ => usage(),
    };
    let set = match op.as_str() {
        "get" => false,
        "set" => true,
        _ => usage(),
    };

    if bulk && set {
        // Worst-case key length this run can generate, since key_bytes
        // is `format!("bk:{index}")` — longer indices produce longer keys.
        let key_len = key_bytes(keyspace.saturating_sub(1)).len();
        let header_estimate =
            format!("o 0 {batch}").len() + batch * format!(" {key_len} {value_size}").len() + 1;
        let body_estimate = batch * (key_len + value_size);
        let estimated_frame_size = header_estimate + body_estimate;
        if estimated_frame_size > MAX_REQUEST_SIZE {
            eprintln!(
                "bulkbench: batch={batch} x value-size={value_size} (plus keyspace={keyspace}'s \
                 key-length overhead) would build an estimated {estimated_frame_size}-byte \
                 multi-set frame, exceeding the server's {MAX_REQUEST_SIZE}-byte request limit; \
                 reduce --batch or --value-size."
            );
            std::process::exit(1);
        }
    }

    let value = vec![b'x'; value_size];

    let deadline = Instant::now() + duration;
    let batches = Arc::new(AtomicU64::new(0));
    let keys_done = Arc::new(AtomicU64::new(0));
    let hits_done = Arc::new(AtomicU64::new(0));
    let latencies_ms = Arc::new(StdMutex::new(Vec::<f64>::new()));

    let mut workers = Vec::with_capacity(concurrency);
    for worker in 0..concurrency {
        let addr = addr.clone();
        let value = value.clone();
        let batches = Arc::clone(&batches);
        let keys_done = Arc::clone(&keys_done);
        let hits_done = Arc::clone(&hits_done);
        let latencies_ms = Arc::clone(&latencies_ms);

        workers.push(tokio::spawn(async move {
            let mut stream = timed(TcpStream::connect(&addr), "connect").await;
            authenticate(&mut stream).await;
            let mut reader = BufReader::new(stream);
            let mut rng = Lcg::new(0x9E37_79B9_7F4A_7C15 ^ (worker as u64 + 1));
            let mut local_latencies = Vec::new();
            let mut local_hits = 0u64;

            while Instant::now() < deadline {
                let keys: Vec<Vec<u8>> = (0..batch)
                    .map(|_| key_bytes(rng.next() % keyspace))
                    .collect();

                let start = Instant::now();
                match (set, bulk) {
                    (false, true) => {
                        let frame = encode_multi_get(&keys);
                        timed(reader.get_mut().write_all(&frame), "write m").await;
                        let (count, hits) = read_multi_reply(&mut reader).await;
                        assert_eq!(count, keys.len(), "M roster size mismatch");
                        local_hits += hits as u64;
                    }
                    (false, false) => {
                        let mut frame = Vec::new();
                        for key in &keys {
                            frame.extend_from_slice(&encode_get(key));
                        }
                        timed(reader.get_mut().write_all(&frame), "write g").await;
                        for _ in &keys {
                            if read_get_reply(&mut reader).await {
                                local_hits += 1;
                            }
                        }
                    }
                    (true, true) => {
                        let frame = encode_multi_set(&keys, &value);
                        timed(reader.get_mut().write_all(&frame), "write o").await;
                        let (count, stored) = read_multi_ack_reply(&mut reader).await;
                        assert_eq!(count, keys.len(), "O roster size mismatch");
                        local_hits += stored as u64;
                    }
                    (true, false) => {
                        let mut frame = Vec::new();
                        for key in &keys {
                            frame.extend_from_slice(&encode_set(key, &value));
                        }
                        timed(reader.get_mut().write_all(&frame), "write s").await;
                        for _ in &keys {
                            read_set_reply(&mut reader).await;
                        }
                        local_hits += batch as u64;
                    }
                }
                local_latencies.push(start.elapsed().as_secs_f64() * 1000.0);

                batches.fetch_add(1, Ordering::Relaxed);
                keys_done.fetch_add(batch as u64, Ordering::Relaxed);
            }

            hits_done.fetch_add(local_hits, Ordering::Relaxed);
            latencies_ms.lock().unwrap().extend(local_latencies);
        }));
    }

    // A panicking worker (e.g. an `.expect()` on an unexpected reply) used
    // to abort the whole run via `.expect()` here, discarding every other
    // worker's results along with it. Report it and keep aggregating what
    // the other workers produced instead.
    let mut failed_workers: Vec<(usize, JoinError)> = Vec::new();
    for (index, worker) in workers.into_iter().enumerate() {
        if let Err(error) = worker.await {
            eprintln!("bulkbench: worker {index} failed: {error}");
            failed_workers.push((index, error));
        }
    }
    if !failed_workers.is_empty() {
        eprintln!(
            "bulkbench: {}/{concurrency} worker(s) failed; aggregating the remaining results",
            failed_workers.len()
        );
    }

    let mut latencies_ms = Arc::try_unwrap(latencies_ms)
        .expect("all workers finished")
        .into_inner()
        .expect("mutex not poisoned");
    latencies_ms.sort_by(|a, b| a.partial_cmp(b).expect("no NaN latencies"));

    let total_keys = keys_done.load(Ordering::Relaxed);
    let total_hits = hits_done.load(Ordering::Relaxed);
    println!(
        "{{\"op\":\"{op}\",\"arm\":\"{arm}\",\"batch\":{batch},\"concurrency\":{concurrency},\
         \"batches\":{},\"keys\":{total_keys},\"keys_per_sec\":{:.0},\
         \"hit_rate\":{:.4},\
         \"batch_p50_ms\":{:.3},\"batch_p99_ms\":{:.3}}}",
        batches.load(Ordering::Relaxed),
        total_keys as f64 / duration.as_secs_f64(),
        total_hits as f64 / total_keys.max(1) as f64,
        percentile(&latencies_ms, 0.50),
        percentile(&latencies_ms, 0.99),
    );
}

#[tokio::main]
async fn main() {
    let args: Vec<String> = env::args().collect();
    match args.get(1).map(String::as_str) {
        Some("preload") => preload(&args[2..]).await,
        Some("run") => run(&args[2..]).await,
        _ => usage(),
    }
}
