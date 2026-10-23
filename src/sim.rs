//! A discovery and several nodes in one process, under a paused clock.
//!
//! The cluster bugs that need a particular ordering of independent events
//! (a join's `M` against the previous join's forwarding window, a write
//! landing between a ring change and the next heartbeat) are reproduced
//! here by choosing when each event happens, instead of by running the
//! Docker membership matrix and hoping it hits the window. A scenario
//! takes well under a second.
//!
//! What it is: the real `server::run` and the real discovery `run`, over
//! loopback TCP, with the clock moved only by `run_for`. What it is not:
//! an SDK (the "client" is raw `S`/`G` commands), TLS, or a replacement
//! for the matrix.
//!
//! Time is manual on purpose. Under `start_paused` alone the clock jumps
//! whenever the runtime is idle, which includes every wait for a loopback
//! read, so the point where a node adopts a new ring moves from run to run.
//! `spin` keeps the runtime busy so the clock stands still, and `settle`
//! lets in-flight I/O land before the next step. `SETTLE_YIELDS` is an
//! empirical bound, not a proof of quiescence: if a scenario ever flakes,
//! the fix is an in-memory transport, not a bigger number.
//!
//! Nodes are named by a random uuid, so which keys a node owns differs from
//! run to run; assertions are therefore stated over "the keys this node
//! owns in this ring", never over fixed key names.

#[path = "bin/nanocached-discovery.rs"]
#[allow(dead_code, unused_imports)]
mod discovery;

use std::time::Duration;

use bytes::Bytes;
use nanocached::infra::read_line_bounded;
use tokio::io::{AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpStream;
use tokio::task::JoinHandle;

use crate::server::{self, ConnectionLimits, HeartbeatConfig};

/// A task that is always ready keeps the runtime from ever being idle, so
/// the paused clock never auto-advances while real loopback I/O is in
/// flight; time then moves only through `run_for`.
fn spin() -> JoinHandle<()> {
    tokio::spawn(async {
        loop {
            tokio::task::yield_now().await;
        }
    })
}

const SETTLE_YIELDS: usize = 300;

async fn settle() {
    for _ in 0..SETTLE_YIELDS {
        tokio::task::yield_now().await;
    }
}

async fn run_for(total: Duration) {
    let step = Duration::from_millis(250);
    let mut done = Duration::ZERO;
    while done < total {
        tokio::time::advance(step).await;
        settle().await;
        done += step;
    }
}

fn free_port() -> u16 {
    let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    l.local_addr().unwrap().port()
}

fn start_discovery(port: u16, liveness: Duration) -> JoinHandle<()> {
    tokio::spawn(async move {
        let address = format!("127.0.0.1:{port}");
        let r = discovery::run(&address, liveness, liveness, 2, None, None, None, None).await;
        eprintln!("discovery ended: {r:?}");
    })
}

fn start_node(port: u16, discovery_port: u16) -> JoinHandle<()> {
    tokio::spawn(async move {
        let address = format!("127.0.0.1:{port}");
        let heartbeat = HeartbeatConfig {
            discovery_addrs: vec![format!("127.0.0.1:{discovery_port}")],
            port,
            interval: Duration::from_secs(5),
            auth_secret: None,
            tls_connector: None,
        };
        let r = server::run(
            &address,
            Some(heartbeat),
            None,
            None,
            64 << 20,
            None,
            Duration::from_secs(25),
            ConnectionLimits::default(),
            Vec::new(),
        )
        .await;
        eprintln!("node {port} ended: {r:?}");
    })
}

/// One line, without its newline. Bounded like every other reader here
/// (`clippy.toml` rejects `read_line`); the deadline is far away because
/// the test clock only moves through `run_for`.
async fn line_of<R: tokio::io::AsyncBufRead + Unpin>(reader: &mut R) -> String {
    let mut line = Vec::new();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(3600);
    read_line_bounded(reader, 1 << 16, deadline, &mut line)
        .await
        .unwrap();
    String::from_utf8_lossy(&line).trim_end().to_string()
}

async fn roster(discovery_port: u16) -> Vec<(String, String)> {
    let mut s = TcpStream::connect(("127.0.0.1", discovery_port))
        .await
        .unwrap();
    s.write_all(b"L\n").await.unwrap();
    let mut r = BufReader::new(s);
    let header = line_of(&mut r).await;
    let count: usize = header.split_whitespace().nth(1).unwrap().parse().unwrap();
    let mut out = Vec::new();
    for _ in 0..count {
        let h = line_of(&mut r).await;
        let mut it = h.split_whitespace().map(|v| v.parse::<usize>().unwrap());
        let (nl, al) = (it.next().unwrap(), it.next().unwrap());
        let mut buf = vec![0u8; nl + al + 1];
        r.read_exact(&mut buf).await.unwrap();
        out.push((
            String::from_utf8_lossy(&buf[..nl]).into_owned(),
            String::from_utf8_lossy(&buf[nl..nl + al]).into_owned(),
        ));
    }
    out
}

async fn raw_line(port: u16, request: &[u8]) -> (String, Vec<u8>) {
    let mut s = TcpStream::connect(("127.0.0.1", port)).await.unwrap();
    s.write_all(request).await.unwrap();
    let mut r = BufReader::new(s);
    let line = line_of(&mut r).await;
    let mut body = Vec::new();
    if let Some(n) = line
        .strip_prefix("V ")
        .and_then(|n| n.trim().parse::<usize>().ok())
    {
        body = vec![0u8; n];
        r.read_exact(&mut body).await.unwrap();
    }
    (line, body)
}

async fn raw_set(port: u16, key: &str, value: &str) -> String {
    let req = format!("S {} {}\n{}{}", key.len(), value.len(), key, value);
    raw_line(port, req.as_bytes()).await.0
}

async fn raw_get(port: u16, key: &str) -> (String, Vec<u8>) {
    let req = format!("G {}\n{}", key.len(), key);
    raw_line(port, req.as_bytes()).await
}

/// What one run of the #564 scenario saw.
#[derive(Debug)]
struct Outcome {
    accepted: usize,
    b_should_have: usize,
    b_lacks: usize,
}

/// #564: a second join's M must not revoke the first confirmed joiner's
/// forwarding. A client that does not know B writes to A alone, `write_at`
/// after C starts (inside C's forwarding window).
async fn scenario_564(write_at: Duration) -> Outcome {
    use crate::hash_ring::HashRing;
    use crate::key::Key;

    let dport = free_port();
    let (a, b, c) = (free_port(), free_port(), free_port());
    let d = start_discovery(dport, Duration::from_secs(15));
    settle().await;
    let na = start_node(a, dport);
    run_for(Duration::from_secs(20)).await;
    let nb = start_node(b, dport);
    run_for(Duration::from_secs(25)).await;
    let roster_ab = roster(dport).await;
    assert_eq!(roster_ab.len(), 2, "A and B are members: {roster_ab:?}");
    let name_of = |roster: &[(String, String)], port: u16| {
        roster
            .iter()
            .find(|(_, addr)| addr.ends_with(&format!(":{port}")))
            .map(|(n, _)| n.clone())
            .unwrap()
    };
    let (name_a, name_b) = (name_of(&roster_ab, a), name_of(&roster_ab, b));

    let nc = start_node(c, dport);
    run_for(write_at).await;
    let mut accepted_keys = Vec::new();
    for i in 0..60 {
        let key = format!("k{i}");
        if raw_set(a, &key, "v").await == "S" {
            accepted_keys.push(key);
        }
    }
    run_for(Duration::from_secs(2)).await;
    let roster_abc = roster(dport).await;
    assert_eq!(
        roster_abc.len(),
        3,
        "A, B and C are members: {roster_abc:?}"
    );
    let c_name = name_of(&roster_abc, c);
    let ring3 = HashRing::new(vec![name_a, name_b.clone(), c_name]);
    let mut out = Outcome {
        accepted: accepted_keys.len(),
        b_should_have: 0,
        b_lacks: 0,
    };
    for key in &accepted_keys {
        let owners = ring3.owners(&Key::unnamespaced(Bytes::from(key.clone())), 2);
        if owners.contains(&name_b.as_str()) {
            out.b_should_have += 1;
            if !raw_get(b, key).await.0.starts_with('V') {
                out.b_lacks += 1;
            }
        }
    }
    for h in [d, na, nb, nc] {
        h.abort();
    }
    out
}

#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn a_second_join_does_not_cut_off_the_first_joiners_forwarding() {
    let _spin = spin();
    let out = scenario_564(Duration::from_secs(1)).await;
    eprintln!("{out:?}");
    assert!(
        out.accepted > 0 && out.b_should_have > 0,
        "nothing to check: {out:?}"
    );
    assert_eq!(out.b_lacks, 0, "B never received writes it owns: {out:?}");
}

/// Sweep of one event against another: when, relative to C starting, the
/// stale client writes. Prints one line per offset and the rate.
#[tokio::test(flavor = "current_thread", start_paused = true)]
#[ignore]
async fn sweep_564_write_offset() {
    let _spin = spin();
    let started = std::time::Instant::now();
    let mut runs = 0;
    for ms in (0..=8000).step_by(500) {
        let one = std::time::Instant::now();
        let out = scenario_564(Duration::from_millis(ms)).await;
        runs += 1;
        eprintln!("write_at +{ms:>4} ms: {out:?}  ({:?})", one.elapsed());
        assert_eq!(
            out.b_lacks, 0,
            "writes B owns never reached it at +{ms} ms: {out:?}"
        );
    }
    eprintln!("{runs} runs in {:?}", started.elapsed());
}
