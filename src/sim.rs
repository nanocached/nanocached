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
//! A node is given its name and a shutdown request by the test
//! (`server::RunHooks`), so which keys it owns is the same on every run and
//! one node of several can be asked to leave. Assertions are still stated
//! over "the keys this node owns in this ring", not over fixed key names.

#[path = "bin/nanocached-discovery.rs"]
#[allow(dead_code, unused_imports)]
mod discovery;

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use bytes::Bytes;
use nanocached::infra::read_line_bounded;
use tokio::io::{AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Notify;
use tokio::task::JoinHandle;

use crate::server::{self, ConnectionLimits, HeartbeatConfig, RunHooks};

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

/// A loopback port that stays bound from the moment a test picks it until the
/// node or discovery that will serve it takes the listener over. Picking a
/// port by binding `:0` and dropping the listener leaves a window in which
/// another test process, or an outgoing connection's local port, can be
/// handed the same number, and the later bind fails with `AddrInUse`.
struct Port {
    number: u16,
    listener: Option<std::net::TcpListener>,
}

impl Port {
    fn reserve() -> Port {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let number = listener.local_addr().unwrap().port();
        Port {
            number,
            listener: Some(listener),
        }
    }

    /// The bound listener, for a tokio server; call it inside the runtime.
    /// Only once per port.
    fn take_listener(&mut self) -> TcpListener {
        let listener = self.listener.take().expect("port already handed over");
        listener.set_nonblocking(true).unwrap();
        TcpListener::from_std(listener).unwrap()
    }
}

/// The point of `Port`: from `reserve` until the server takes the listener,
/// no one else can bind the number, which a dropped `:0` listener does not
/// guarantee.
#[test]
fn a_reserved_port_cannot_be_bound_by_anyone_else() {
    let port = Port::reserve();
    let err = std::net::TcpListener::bind(("127.0.0.1", port.number)).unwrap_err();
    assert_eq!(err.kind(), std::io::ErrorKind::AddrInUse);
}

fn start_discovery(port: &mut Port, liveness: Duration) -> JoinHandle<()> {
    let listener = port.take_listener();
    tokio::spawn(async move {
        let r = discovery::run_on_listener(listener, liveness, liveness, 2, None, None, None, None)
            .await;
        eprintln!("discovery ended: {r:?}");
    })
}

/// A node running in this process: abort it to kill it, `leave` to ask it to
/// shut down the way SIGTERM would (a decommission, then exit).
struct Node {
    handle: JoinHandle<()>,
    leave: Arc<Notify>,
}

impl Node {
    fn abort(&self) {
        self.handle.abort();
    }
}

fn start_node(port: &mut Port, discovery_port: u16, name: Option<&str>) -> Node {
    let listener = port.take_listener();
    let port = port.number;
    let leave = Arc::new(Notify::new());
    let signal = Arc::clone(&leave);
    let name = name.map(str::to_string);
    let handle = tokio::spawn(async move {
        let address = format!("127.0.0.1:{port}");
        let heartbeat = HeartbeatConfig {
            discovery_addrs: vec![format!("127.0.0.1:{discovery_port}")],
            port,
            interval: Duration::from_secs(5),
            auth_secret: None,
            tls_connector: None,
        };
        // The first request is the one a test makes; any later one (a
        // second signal while draining) never comes.
        let mut first = true;
        let hooks = RunHooks {
            shutdown: Box::new(move || {
                if std::mem::take(&mut first) {
                    let signal = Arc::clone(&signal);
                    Box::pin(async move {
                        signal.notified().await;
                        Ok(())
                    })
                } else {
                    Box::pin(std::future::pending())
                }
            }),
            node_name: name,
            listener: Some(listener),
        };
        let r = server::run_with_hooks(
            &address,
            Some(heartbeat),
            None,
            None,
            64 << 20,
            None,
            Duration::from_secs(25),
            ConnectionLimits::default(),
            Vec::new(),
            hooks,
        )
        .await;
        eprintln!("node {port} ended: {r:?}");
    });
    Node { handle, leave }
}

/// How long a helper below may wait for its peer, in real time.
const REAL_TIME_LIMIT: Duration = Duration::from_secs(60);

/// Panics if `fut` has not finished within `limit` of real time. The test
/// clock is paused and only moves through `run_for`, so a deadline on it
/// never fires while a peer sits silent and `spin()` keeps the runtime
/// busy: the test would hang at full CPU instead of failing. The watchdog
/// is a plain thread, which the paused clock cannot hold back, and it ends
/// as soon as `fut` does.
async fn within_real_time<T>(
    limit: Duration,
    what: &str,
    fut: impl std::future::Future<Output = T>,
) -> T {
    use std::sync::atomic::AtomicBool;

    let (expired, expiry) = tokio::sync::oneshot::channel::<()>();
    let finished = Arc::new(AtomicBool::new(false));
    let watchdog = {
        let finished = Arc::clone(&finished);
        std::thread::spawn(move || {
            let end = std::time::Instant::now() + limit;
            while !finished.load(Ordering::SeqCst) {
                let left = end.saturating_duration_since(std::time::Instant::now());
                if left.is_zero() {
                    let _ = expired.send(());
                    return;
                }
                std::thread::park_timeout(left);
            }
        })
    };
    let out = tokio::select! {
        out = fut => Some(out),
        _ = expiry => None,
    };
    finished.store(true, Ordering::SeqCst);
    watchdog.thread().unpark();
    out.unwrap_or_else(|| panic!("{what}: no answer within {limit:?} of real time"))
}

/// One line, without its newline. Bounded like every other reader here
/// (`clippy.toml` rejects `read_line`); the deadline is far away because
/// the test clock only moves through `run_for` — see `within_real_time`
/// for the bound that does hold.
async fn line_of<R: tokio::io::AsyncBufRead + Unpin>(reader: &mut R) -> String {
    let mut line = Vec::new();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(3600);
    read_line_bounded(reader, 1 << 16, deadline, &mut line)
        .await
        .unwrap();
    String::from_utf8_lossy(&line).trim_end().to_string()
}

async fn roster(discovery_port: u16) -> Vec<(String, String)> {
    within_real_time(REAL_TIME_LIMIT, "roster", roster_unbounded(discovery_port)).await
}

async fn roster_unbounded(discovery_port: u16) -> Vec<(String, String)> {
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
    within_real_time(
        REAL_TIME_LIMIT,
        "raw_line",
        raw_line_unbounded(port, request),
    )
    .await
}

async fn raw_line_unbounded(port: u16, request: &[u8]) -> (String, Vec<u8>) {
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

    let mut dp = Port::reserve();
    let (mut pa, mut pb, mut pc) = (Port::reserve(), Port::reserve(), Port::reserve());
    let (dport, a, b, c) = (dp.number, pa.number, pb.number, pc.number);
    let d = start_discovery(&mut dp, Duration::from_secs(15));
    settle().await;
    let na = start_node(&mut pa, dport, Some("node-a"));
    run_for(Duration::from_secs(20)).await;
    let nb = start_node(&mut pb, dport, Some("node-b"));
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

    let nc = start_node(&mut pc, dport, Some("node-c"));
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
    d.abort();
    for node in [na, nb, nc] {
        node.abort();
    }
    out
}

/// A peer that accepts and then says nothing used to hang the test at full
/// CPU: the 3600 s deadline of `line_of` is on the paused clock, which only
/// `run_for` moves.
#[tokio::test(flavor = "current_thread", start_paused = true)]
#[should_panic(expected = "no answer within")]
async fn a_silent_peer_fails_a_helper_in_real_time_instead_of_hanging() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let _silent_peer = tokio::spawn(async move {
        let _held = listener.accept().await;
        std::future::pending::<()>().await;
    });
    let _spin = spin();
    within_real_time(
        Duration::from_secs(1),
        "raw_line",
        raw_line_unbounded(port, b"G 1\nk"),
    )
    .await;
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

/// What one run of the touch scenario saw.
#[derive(Debug)]
struct TouchOutcome {
    keys: usize,
    b_held_before: usize,
    a_held_after: usize,
    b_held_after: usize,
}

/// A touch that reaches only the old owner while its forwarding window for a
/// confirmed joiner is open must reach the joiner too: its copy came from the
/// handoff with the long TTL the key was written with, and nothing else
/// tells it the TTL was shortened. A stale client (one that does not know
/// B) shortens every key's TTL on A alone; after that TTL, no copy is left.
async fn scenario_touch_during_a_join(touch_at: Duration) -> TouchOutcome {
    let mut dp = Port::reserve();
    let (mut pa, mut pb) = (Port::reserve(), Port::reserve());
    let (dport, a, b) = (dp.number, pa.number, pb.number);
    let d = start_discovery(&mut dp, Duration::from_secs(15));
    settle().await;
    let na = start_node(&mut pa, dport, Some("node-a"));
    run_for(Duration::from_secs(20)).await;
    let keys: Vec<String> = (0..20).map(|i| format!("t{i}")).collect();
    for key in &keys {
        let req = format!("S {} 1 1000\n{key}v", key.len());
        assert_eq!(raw_line(a, req.as_bytes()).await.0, "S");
    }
    let nb = start_node(&mut pb, dport, Some("node-b"));
    run_for(Duration::from_secs(25)).await;
    assert_eq!(roster(dport).await.len(), 2, "A and B are members");
    run_for(touch_at).await;

    let mut out = TouchOutcome {
        keys: keys.len(),
        b_held_before: 0,
        a_held_after: 0,
        b_held_after: 0,
    };
    for key in &keys {
        if raw_get(b, key).await.0.starts_with('V') {
            out.b_held_before += 1;
        }
        let req = format!("t 0 {} 10\n{key}", key.len());
        assert_eq!(raw_line(a, req.as_bytes()).await.0, "S", "{key}");
    }
    run_for(Duration::from_secs(12)).await;
    for key in &keys {
        if raw_get(a, key).await.0.starts_with('V') {
            out.a_held_after += 1;
        }
        if raw_get(b, key).await.0.starts_with('V') {
            out.b_held_after += 1;
        }
    }
    d.abort();
    na.abort();
    nb.abort();
    out
}

#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn a_touch_on_the_old_owner_reaches_the_joiner_inside_the_forwarding_window() {
    let _spin = spin();
    let out = scenario_touch_during_a_join(Duration::from_secs(1)).await;
    eprintln!("{out:?}");
    assert_eq!(
        out.b_held_before, out.keys,
        "nothing to check, B did not hold the keys: {out:?}"
    );
    assert_eq!(
        out.a_held_after, 0,
        "the touch did not shorten A's TTL: {out:?}"
    );
    assert_eq!(
        out.b_held_after, 0,
        "B kept the long TTL it was handed: {out:?}"
    );
}

/// #571: a joiner that is promoted and then leaves before its senders'
/// next heartbeat ack is never in a roster they see, so its handoff slot
/// stays unconfirmed. The sender must still learn that it is gone and stop
/// forwarding to its address. Returns how many connections reached B's old
/// address from the senders afterwards: a socket listens where B was.
async fn scenario_571(gap: Duration) -> usize {
    let mut dp = Port::reserve();
    let (mut pa, mut pb) = (Port::reserve(), Port::reserve());
    let (dport, a, b) = (dp.number, pa.number, pb.number);
    let d = start_discovery(&mut dp, Duration::from_secs(15));
    settle().await;
    let na = start_node(&mut pa, dport, Some("node-a"));
    run_for(Duration::from_secs(20)).await;
    let nb = start_node(&mut pb, dport, Some("node-b"));
    // Promoted once discovery lists it.
    let mut waited = Duration::ZERO;
    while roster(dport).await.len() < 2 {
        assert!(waited < Duration::from_secs(30), "B was never promoted");
        run_for(Duration::from_millis(250)).await;
        waited += Duration::from_millis(250);
    }
    run_for(gap).await;
    nb.leave.notify_one();
    let mut waited = Duration::ZERO;
    while !nb.handle.is_finished() {
        assert!(waited < Duration::from_secs(90), "B never finished leaving");
        run_for(Duration::from_millis(250)).await;
        waited += Duration::from_millis(250);
    }

    // Whoever still dials B's address now reaches this listener.
    let attempts = Arc::new(AtomicUsize::new(0));
    let listener = TcpListener::bind(("127.0.0.1", b)).await.unwrap();
    let counter = Arc::clone(&attempts);
    let watcher = tokio::spawn(async move {
        let mut held = Vec::new();
        while let Ok((conn, _)) = listener.accept().await {
            counter.fetch_add(1, Ordering::SeqCst);
            held.push(conn);
        }
    });
    // Writes to A for as long as a forgotten forwarding window would last.
    for second in 0..75 {
        for i in 0..10 {
            raw_set(a, &format!("w{second}-{i}"), "v").await;
        }
        run_for(Duration::from_secs(1)).await;
    }
    watcher.abort();
    d.abort();
    na.abort();
    attempts.load(Ordering::SeqCst)
}

#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn a_joiner_that_left_within_a_heartbeat_is_no_longer_forwarded_to() {
    let _spin = spin();
    let attempts = scenario_571(Duration::from_secs(1)).await;
    assert_eq!(attempts, 0, "the sender kept dialling the departed joiner");
}

/// The gap between B's promotion and its leave, against the 5 s heartbeat.
#[tokio::test(flavor = "current_thread", start_paused = true)]
#[ignore]
async fn sweep_571_leave_gap() {
    let _spin = spin();
    let mut dialled = Vec::new();
    for ms in [0, 1000, 2000, 3000, 4000, 5000, 6000, 8000] {
        let one = std::time::Instant::now();
        let attempts = scenario_571(Duration::from_millis(ms)).await;
        eprintln!(
            "leave +{ms:>4} ms after promotion: {attempts} dials ({:?})",
            one.elapsed()
        );
        if attempts > 0 {
            dialled.push(ms);
        }
    }
    assert!(
        dialled.is_empty(),
        "dials to the departed joiner after a gap of {dialled:?} ms"
    );
}
