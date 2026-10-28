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

use crate::server::{self, ConnectionLimits, HeartbeatConfig, RunHooks, SimGates};

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
    /// Hold points inside the node — see `server::SimGates`.
    gates: Arc<SimGates>,
    /// When it was started: its heartbeats fall every 5 s from here.
    started: tokio::time::Instant,
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
    let gates = Arc::new(SimGates::default());
    let node_gates = Arc::clone(&gates);
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
            gates: node_gates,
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
    Node {
        handle,
        leave,
        gates,
        started: tokio::time::Instant::now(),
    }
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

/// One connection per node, reused across requests. A connection per
/// request (`raw_line`) ran the scenarios below out of ephemeral ports: each
/// makes a few thousand requests, and several run at once, so the sockets
/// in TIME_WAIT outnumbered the ports and a connect failed. The node closes
/// a connection that sat idle while the clock was moved, so a request that
/// finds its connection gone dials once more.
struct Client {
    conns: std::collections::HashMap<u16, BufReader<TcpStream>>,
}

impl Client {
    fn new() -> Client {
        Client {
            conns: std::collections::HashMap::new(),
        }
    }

    async fn request(&mut self, port: u16, req: &[u8]) -> (String, Vec<u8>) {
        for _ in 0..2 {
            let conn = match self.conns.entry(port) {
                std::collections::hash_map::Entry::Occupied(conn) => conn.into_mut(),
                std::collections::hash_map::Entry::Vacant(slot) => {
                    let stream = within_real_time(
                        REAL_TIME_LIMIT,
                        "connect",
                        TcpStream::connect(("127.0.0.1", port)),
                    )
                    .await
                    .unwrap_or_else(|e| panic!("connect to {port}: {e}"));
                    slot.insert(BufReader::new(stream))
                }
            };
            match within_real_time(REAL_TIME_LIMIT, "request", Self::exchange(conn, req)).await {
                Ok(reply) => return reply,
                Err(_) => {
                    self.conns.remove(&port);
                }
            }
        }
        panic!("no reply from {port} on a fresh connection either");
    }

    async fn exchange(
        conn: &mut BufReader<TcpStream>,
        req: &[u8],
    ) -> std::io::Result<(String, Vec<u8>)> {
        conn.get_mut().write_all(req).await?;
        let mut line = Vec::new();
        let deadline = tokio::time::Instant::now() + Duration::from_secs(3600);
        read_line_bounded(conn, 1 << 16, deadline, &mut line).await?;
        if line.is_empty() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "closed",
            ));
        }
        let line = String::from_utf8_lossy(&line).trim_end().to_string();
        let mut body = Vec::new();
        if let Some(n) = line
            .strip_prefix("V ")
            .and_then(|n| n.trim().parse::<usize>().ok())
        {
            body = vec![0u8; n];
            conn.read_exact(&mut body).await?;
        }
        Ok((line, body))
    }

    async fn set(&mut self, port: u16, key: &str, value: &str) -> String {
        let req = format!("S {} {}\n{key}{value}", key.len(), value.len());
        self.request(port, req.as_bytes()).await.0
    }

    /// Whether `port` answers the key with a value.
    async fn holds(&mut self, port: u16, key: &str) -> bool {
        let req = format!("G {}\n{key}", key.len());
        self.request(port, req.as_bytes()).await.0.starts_with('V')
    }

    async fn delete(&mut self, port: u16, key: &str) -> String {
        let req = format!("D {}\n{key}", key.len());
        self.request(port, req.as_bytes()).await.0
    }

    /// `t` with a TTL (0 expires the key at once), or without one to
    /// remove the key's TTL.
    async fn touch(&mut self, port: u16, key: &str, ttl: Option<u64>) -> String {
        let req = match ttl {
            Some(ttl) => format!("t 0 {} {ttl}\n{key}", key.len()),
            None => format!("t 0 {}\n{key}", key.len()),
        };
        self.request(port, req.as_bytes()).await.0
    }
}

/// A node's name in a roster, by the port it listens on.
fn name_at(roster: &[(String, String)], port: u16) -> String {
    roster
        .iter()
        .find(|(_, addr)| addr.ends_with(&format!(":{port}")))
        .map(|(name, _)| name.clone())
        .unwrap_or_else(|| panic!("no member listens on {port}: {roster:?}"))
}

/// Runs until `node` has exited, moving the clock a step at a time.
async fn run_until_gone(node: &Node, what: &str) {
    let mut waited = Duration::ZERO;
    while !node.handle.is_finished() {
        assert!(
            waited < Duration::from_secs(90),
            "{what} never finished leaving"
        );
        run_for(Duration::from_millis(250)).await;
        waited += Duration::from_millis(250);
    }
}

/// Runs until discovery lists `members` nodes.
async fn run_until_members(dport: u16, members: usize) {
    let mut waited = Duration::ZERO;
    while roster(dport).await.len() != members {
        assert!(
            waited < Duration::from_secs(30),
            "discovery never listed {members} members"
        );
        run_for(Duration::from_millis(250)).await;
        waited += Duration::from_millis(250);
    }
}

/// What one run of the #970 scenario saw.
#[derive(Debug)]
struct ReclaimOutcome {
    /// Keys a joiner D took from X while A stayed an owner: A's leave gives
    /// them back to X. Half are expired, half deleted.
    keys: usize,
    x_held_before_the_join: usize,
    /// Keys X still answers with a value after A has left: the dead copy
    /// the join displaced, revived by the leave.
    x_revived: usize,
    /// Keys the joiner D still answers with a value: it was told to expire
    /// or delete them, so none should.
    d_kept: usize,
}

/// #970: a key deleted or expired while a former owner still holds the dead
/// copy a join took from it came back when a leave made that node an owner
/// again before the sweep had removed the copy. A, B, C hold keys; D joins
/// and displaces X (B or C) from the keys A and X owned; the client expires
/// or deletes those keys on their owners under the new ring (D and A), and
/// A leaves at once — before the sweep after the join's confirmation — so
/// X owns the keys again. The sweep must let X's dead copies go: D saw the
/// expiry, nothing legitimately brings the keys back.
async fn scenario_970() -> ReclaimOutcome {
    use crate::hash_ring::HashRing;
    use crate::key::Key;

    let mut dp = Port::reserve();
    let (mut pa, mut pb, mut pc, mut pd) = (
        Port::reserve(),
        Port::reserve(),
        Port::reserve(),
        Port::reserve(),
    );
    let (dport, a, b, c, d) = (dp.number, pa.number, pb.number, pc.number, pd.number);
    let disc = start_discovery(&mut dp, Duration::from_secs(15));
    settle().await;
    let mut client = Client::new();
    let na = start_node(&mut pa, dport, Some("node-a"));
    run_for(Duration::from_secs(20)).await;
    let nb = start_node(&mut pb, dport, Some("node-b"));
    run_for(Duration::from_secs(25)).await;
    let nc = start_node(&mut pc, dport, Some("node-c"));
    run_for(Duration::from_secs(25)).await;
    let roster3 = roster(dport).await;
    assert_eq!(roster3.len(), 3, "A, B and C are members: {roster3:?}");
    let (name_a, name_b, name_c) = (
        name_at(&roster3, a),
        name_at(&roster3, b),
        name_at(&roster3, c),
    );
    let port_of = |name: &str| -> u16 {
        [(&name_a, a), (&name_b, b), (&name_c, c)]
            .into_iter()
            .find(|(n, _)| n.as_str() == name)
            .map(|(_, port)| port)
            .unwrap()
    };
    let ring3 = HashRing::new(vec![name_a.clone(), name_b.clone(), name_c.clone()]);
    // Every key to both of its owners, as a replicating client writes.
    let keys: Vec<String> = (0..300).map(|i| format!("r{i}")).collect();
    for key in &keys {
        for owner in ring3.owners(&Key::unnamespaced(Bytes::from(key.clone())), 2) {
            assert_eq!(
                client.set(port_of(owner), key, "v1").await,
                "S",
                "{key} on {owner}"
            );
        }
    }
    // Past the bootstrap joins' forwarding windows (60 s): a write from here
    // on is not forwarded to anyone.
    run_for(Duration::from_secs(70)).await;

    let nd = start_node(&mut pd, dport, Some("node-d"));
    run_until_members(dport, 4).await;
    let roster4 = roster(dport).await;
    let name_d = name_at(&roster4, d);
    let ring4 = HashRing::new(vec![
        name_a.clone(),
        name_b.clone(),
        name_c.clone(),
        name_d.clone(),
    ]);
    let ring_bcd = HashRing::new(vec![name_b.clone(), name_c.clone(), name_d.clone()]);
    // The keys this scenario is about: X (B or C) owned them with A, D took
    // them from X (A stays), and without A they are X's again.
    let mut chosen: Vec<(String, u16)> = Vec::new();
    for key in &keys {
        let k = Key::unnamespaced(Bytes::from(key.clone()));
        let before = ring3.owners(&k, 2);
        let after = ring4.owners(&k, 2);
        let Some(x) = before.iter().find(|n| **n != name_a).copied() else {
            continue;
        };
        if !before.contains(&name_a.as_str())
            || !after.contains(&name_d.as_str())
            || !after.contains(&name_a.as_str())
            || !ring_bcd.owners(&k, 2).contains(&x)
        {
            continue;
        }
        chosen.push((key.clone(), port_of(x)));
    }
    let mut out = ReclaimOutcome {
        keys: chosen.len(),
        x_held_before_the_join: 0,
        x_revived: 0,
        d_kept: 0,
    };
    for (key, x) in &chosen {
        if client.holds(*x, key).await {
            out.x_held_before_the_join += 1;
        }
    }
    // Expire half, delete half, on the owners under the published ring (D
    // and A), then A leaves at once: no clock step in between, as a
    // decommission a second after a join.
    for (i, (key, _)) in chosen.iter().enumerate() {
        for port in [d, a] {
            let reply = if i % 2 == 0 {
                client.touch(port, key, Some(0)).await
            } else {
                client.delete(port, key).await
            };
            assert!(reply == "S" || reply == "D", "{key}: {reply}");
        }
    }
    na.leave.notify_one();
    run_until_gone(&na, "A").await;
    run_until_members(dport, 3).await;
    // Past the next heartbeat (5 s) and the sweep it triggers, and past the
    // 120 s reclaim grace, so whatever X holds now is what it keeps.
    run_for(Duration::from_secs(130)).await;
    for (key, x) in &chosen {
        if client.holds(*x, key).await {
            out.x_revived += 1;
        }
        if client.holds(d, key).await {
            out.d_kept += 1;
        }
    }
    disc.abort();
    for node in [nb, nc, nd] {
        node.abort();
    }
    out
}

/// What one run of the #973 scenario saw.
#[derive(Debug)]
struct WindowOutcome {
    /// Keys A and C owned: C's eviction promotes B (A's window then
    /// mirrors writes to B), D's join displaces B, D's eviction gives
    /// them back to B.
    keys: usize,
    b_held_after_the_eviction: usize,
    /// Keys B does not hold once it owns them again and the TTL a touch
    /// gave them in A's window has passed: the mirrored touch revived B's
    /// dead copy with that TTL, the TTL's removal never reached it, and
    /// the stale copy refused A's re-replication.
    b_missing: usize,
    a_missing: usize,
}

/// #973: the forwarding window of a re-replication pass mirrored a write to
/// the owners the pass had promoted, by the ring it was computed with. A
/// join adopted inside the window took the key from one of them; the
/// mirrored write revived its dead copy, which no later write reached, and
/// an eviction that made it an owner again had it refuse the fresh copy.
/// A, B, C hold keys; C is evicted (A's pass promotes B and opens A's
/// window); D joins inside the window and displaces B; a touch gives the
/// keys a TTL on their owners (A and D); after the window closes the TTL
/// is removed on them; D is evicted; past the TTL, B must hold the keys.
async fn scenario_973() -> WindowOutcome {
    use crate::hash_ring::HashRing;
    use crate::key::Key;

    let mut dp = Port::reserve();
    let (mut pa, mut pb, mut pc, mut pd) = (
        Port::reserve(),
        Port::reserve(),
        Port::reserve(),
        Port::reserve(),
    );
    let (dport, a, b, c, d) = (dp.number, pa.number, pb.number, pc.number, pd.number);
    let disc = start_discovery(&mut dp, Duration::from_secs(15));
    settle().await;
    let mut client = Client::new();
    let na = start_node(&mut pa, dport, Some("node-a"));
    run_for(Duration::from_secs(20)).await;
    let nb = start_node(&mut pb, dport, Some("node-b"));
    run_for(Duration::from_secs(25)).await;
    let nc = start_node(&mut pc, dport, Some("node-c"));
    run_for(Duration::from_secs(25)).await;
    let roster3 = roster(dport).await;
    assert_eq!(roster3.len(), 3, "A, B and C are members: {roster3:?}");
    let (name_a, name_b, name_c) = (
        name_at(&roster3, a),
        name_at(&roster3, b),
        name_at(&roster3, c),
    );
    let port_of = |name: &str| -> u16 {
        [(&name_a, a), (&name_b, b), (&name_c, c)]
            .into_iter()
            .find(|(n, _)| n.as_str() == name)
            .map(|(_, port)| port)
            .unwrap()
    };
    let ring3 = HashRing::new(vec![name_a.clone(), name_b.clone(), name_c.clone()]);
    let keys: Vec<String> = (0..300).map(|i| format!("w{i}")).collect();
    for key in &keys {
        for owner in ring3.owners(&Key::unnamespaced(Bytes::from(key.clone())), 2) {
            assert_eq!(
                client.set(port_of(owner), key, "v1").await,
                "S",
                "{key} on {owner}"
            );
        }
    }
    run_for(Duration::from_secs(70)).await;

    // C dies; its eviction promotes B for the keys A and C owned, by A's
    // re-replication pass, whose forwarding window is open for 60 s.
    nc.abort();
    run_until_members(dport, 2).await;
    let chosen: Vec<String> = keys
        .iter()
        .filter(|key| {
            let owners = ring3.owners(&Key::unnamespaced(Bytes::from((*key).clone())), 2);
            owners.contains(&name_a.as_str()) && owners.contains(&name_c.as_str())
        })
        .cloned()
        .collect();
    let mut out = WindowOutcome {
        keys: 0,
        b_held_after_the_eviction: 0,
        b_missing: 0,
        a_missing: 0,
    };
    // A adopts the two-member ring at its next heartbeat and runs the
    // pass; wait for every promoted copy to land on B.
    let mut waited = Duration::ZERO;
    loop {
        let mut held = 0;
        for key in &chosen {
            if client.holds(b, key).await {
                held += 1;
            }
        }
        out.b_held_after_the_eviction = held;
        if held == chosen.len() || waited >= Duration::from_secs(15) {
            break;
        }
        run_for(Duration::from_millis(250)).await;
        waited += Duration::from_millis(250);
    }

    // D joins inside A's window.
    let nd = start_node(&mut pd, dport, Some("node-d"));
    run_until_members(dport, 3).await;
    let name_d = name_at(&roster(dport).await, d);
    let ring_abd = HashRing::new(vec![name_a.clone(), name_b.clone(), name_d.clone()]);
    // The keys D took from B while A stayed: under the published ring their
    // owners are A and D, and A's window still names B.
    let chosen: Vec<String> = chosen
        .into_iter()
        .filter(|key| {
            let owners = ring_abd.owners(&Key::unnamespaced(Bytes::from(key.clone())), 2);
            owners.contains(&name_a.as_str()) && owners.contains(&name_d.as_str())
        })
        .collect();
    out.keys = chosen.len();
    run_for(Duration::from_secs(5)).await;
    // A client with the published ring gives every key a TTL ...
    for key in &chosen {
        for port in [a, d] {
            assert_eq!(client.touch(port, key, Some(100)).await, "S", "{key}");
        }
    }
    // ... and, once A's window has closed, removes it again.
    run_for(Duration::from_secs(61)).await;
    for key in &chosen {
        for port in [a, d] {
            assert_eq!(client.touch(port, key, None).await, "S", "{key}");
        }
    }
    // D dies: B owns the keys again, and A's pass offers it a fresh copy.
    nd.abort();
    run_until_members(dport, 2).await;
    run_for(Duration::from_secs(40)).await;
    for key in &chosen {
        if !client.holds(b, key).await {
            out.b_missing += 1;
        }
        if !client.holds(a, key).await {
            out.a_missing += 1;
        }
    }
    disc.abort();
    na.abort();
    nb.abort();
    out
}

#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn a_write_in_a_re_replication_window_is_not_mirrored_to_a_node_a_later_join_displaced() {
    let _spin = spin();
    let out = scenario_973().await;
    eprintln!("{out:?}");
    assert!(out.keys > 0, "nothing to check: {out:?}");
    assert!(
        out.b_held_after_the_eviction > 0,
        "nothing to check, B was not promoted for the keys: {out:?}"
    );
    assert_eq!(out.a_missing, 0, "A lost keys it owned throughout: {out:?}");
    assert_eq!(
        out.b_missing, 0,
        "B holds a stale copy a window write revived, or none at all: {out:?}"
    );
}

/// What one run of the #975 (third mechanism) scenario saw.
#[derive(Debug)]
struct LeaverRelayOutcome {
    /// Keys S and E2 owned: E2's eviction promotes L, so S's window names
    /// L; L's leave makes E the owner in its place.
    keys: usize,
    e_held_after_the_drain: usize,
    /// Keys the entrant E still holds after the client expired them on S
    /// while S's window relayed the expiry to the leaving L: L must pass
    /// it on to E, although the value it carries is the one L already had.
    e_kept: usize,
    s_kept: usize,
}

/// #975, third mechanism: an expiry that reached a decommissioning node as a
/// forwarded handoff — a touch with a TTL of 0, relayed through a window —
/// was not passed on to the node's entrant, because a handoff that leaves
/// the value as it was did not count as a change. A, B, C, E2 hold keys;
/// E2 is evicted, promoting L (= C) for the keys S (A or B) and E2 owned and
/// opening S's window toward L; L leaves; once its drain has handed the keys
/// to E (the other of A and B), a client that still routes by the old ring
/// expires the keys on S alone; S's window carries the expiry to L, and L
/// must relay it to E.
async fn scenario_975_leaver_relay() -> LeaverRelayOutcome {
    use crate::hash_ring::HashRing;
    use crate::key::Key;

    let mut dp = Port::reserve();
    let (mut pa, mut pb, mut pc, mut pe) = (
        Port::reserve(),
        Port::reserve(),
        Port::reserve(),
        Port::reserve(),
    );
    let (dport, a, b, c, e2) = (dp.number, pa.number, pb.number, pc.number, pe.number);
    let disc = start_discovery(&mut dp, Duration::from_secs(15));
    settle().await;
    let mut client = Client::new();
    let na = start_node(&mut pa, dport, Some("node-a"));
    // A's and B's heartbeats fall at the same phase of the 5 s interval
    // (B starts a whole number of intervals later): the moment the leave
    // below is asked for is chosen against it.
    let heartbeat_phase = tokio::time::Instant::now();
    run_for(Duration::from_secs(20)).await;
    let nb = start_node(&mut pb, dport, Some("node-b"));
    run_for(Duration::from_secs(25)).await;
    let nc = start_node(&mut pc, dport, Some("node-c"));
    run_for(Duration::from_secs(25)).await;
    let ne = start_node(&mut pe, dport, Some("node-e"));
    run_for(Duration::from_secs(25)).await;
    let roster4 = roster(dport).await;
    assert_eq!(roster4.len(), 4, "A, B, C and E2 are members: {roster4:?}");
    let (name_a, name_b, name_c, name_e) = (
        name_at(&roster4, a),
        name_at(&roster4, b),
        name_at(&roster4, c),
        name_at(&roster4, e2),
    );
    let port_of = |name: &str| -> u16 {
        [(&name_a, a), (&name_b, b), (&name_c, c), (&name_e, e2)]
            .into_iter()
            .find(|(n, _)| n.as_str() == name)
            .map(|(_, port)| port)
            .unwrap()
    };
    let ring4 = HashRing::new(vec![
        name_a.clone(),
        name_b.clone(),
        name_c.clone(),
        name_e.clone(),
    ]);
    let ring_abc = HashRing::new(vec![name_a.clone(), name_b.clone(), name_c.clone()]);
    let keys: Vec<String> = (0..400).map(|i| format!("l{i}")).collect();
    for key in &keys {
        for owner in ring4.owners(&Key::unnamespaced(Bytes::from(key.clone())), 2) {
            assert_eq!(
                client.set(port_of(owner), key, "v1").await,
                "S",
                "{key} on {owner}"
            );
        }
    }
    run_for(Duration::from_secs(70)).await;

    // (key, S, E): S and E2 owned the key; without E2 it is S's and C's,
    // so S's pass promotes C and S's window names C; without C it is S's
    // and E's.
    let chosen: Vec<(String, u16, u16)> = keys
        .iter()
        .filter_map(|key| {
            let k = Key::unnamespaced(Bytes::from(key.clone()));
            let before = ring4.owners(&k, 2);
            let after = ring_abc.owners(&k, 2);
            let s = *before.iter().find(|n| **n == name_a || **n == name_b)?;
            (before.contains(&name_e.as_str())
                && after.contains(&s)
                && after.contains(&name_c.as_str()))
            .then(|| {
                let e = if s == name_a { b } else { a };
                (key.clone(), port_of(s), e)
            })
        })
        .collect();
    let mut out = LeaverRelayOutcome {
        keys: chosen.len(),
        e_held_after_the_drain: 0,
        e_kept: 0,
        s_kept: 0,
    };

    // E2 dies; the survivors' passes promote C and open their windows.
    ne.abort();
    run_until_members(dport, 3).await;
    run_for(Duration::from_secs(3)).await;
    // C leaves just after A's and B's heartbeats: its drain hands the keys
    // to their entrants and it tells discovery within the next step, and
    // the expiry below lands before either survivor's next heartbeat can
    // bring it the roster without C — S's window still names C, and C is
    // inside its forwarding grace. (A survivor that had adopted that
    // roster would mirror the expiry to E itself, through the window of
    // its own pass for the leave, and the relay would not be exercised.)
    while (tokio::time::Instant::now() - heartbeat_phase).as_millis() % 5000 != 1000 {
        run_for(Duration::from_millis(250)).await;
    }
    nc.leave.notify_one();
    let mut waited = Duration::ZERO;
    loop {
        let mut held = 0;
        for (key, _, e) in &chosen {
            if client.holds(*e, key).await {
                held += 1;
            }
        }
        out.e_held_after_the_drain = held;
        if held == chosen.len() || waited >= Duration::from_secs(2) {
            break;
        }
        run_for(Duration::from_millis(250)).await;
        waited += Duration::from_millis(250);
    }
    assert!(
        waited < Duration::from_secs(2),
        "the drain did not reach E before the survivors' next heartbeat"
    );
    // A client that still routes by the ring with E2 expires the keys on S;
    // S's window carries the expiry to C.
    for (key, s, _) in &chosen {
        assert_eq!(client.touch(*s, key, Some(0)).await, "S", "{key}");
    }
    run_until_gone(&nc, "C").await;
    run_until_members(dport, 2).await;
    run_for(Duration::from_secs(10)).await;
    for (key, s, e) in &chosen {
        if client.holds(*e, key).await {
            out.e_kept += 1;
        }
        if client.holds(*s, key).await {
            out.s_kept += 1;
        }
    }
    disc.abort();
    na.abort();
    nb.abort();
    out
}

#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn an_expiry_relayed_to_a_leaving_node_goes_on_to_its_entrant() {
    let _spin = spin();
    let out = scenario_975_leaver_relay().await;
    eprintln!("{out:?}");
    assert!(out.keys > 0, "nothing to check: {out:?}");
    assert_eq!(
        out.e_held_after_the_drain, out.keys,
        "nothing to check, the drain did not reach E: {out:?}"
    );
    assert_eq!(out.s_kept, 0, "the expiry did not take on S: {out:?}");
    assert_eq!(
        out.e_kept, 0,
        "the entrant kept keys the leaver was told had expired: {out:?}"
    );
}

/// A, B, C with every key on both of its owners, settled past the
/// bootstrap joins' forwarding windows. Returns the ports, the names A, B,
/// C registered under, and the node handles (C's included, for the
/// scenarios that kill it).
struct Trio {
    dport: u16,
    disc: JoinHandle<()>,
    a: u16,
    b: u16,
    names: [String; 3],
    nodes: [Node; 3],
    client: Client,
}

async fn trio(dp: &mut Port, pa: &mut Port, pb: &mut Port, pc: &mut Port, keys: &[String]) -> Trio {
    use crate::hash_ring::HashRing;
    use crate::key::Key;

    let (dport, a, b, c) = (dp.number, pa.number, pb.number, pc.number);
    let disc = start_discovery(dp, Duration::from_secs(15));
    settle().await;
    let mut client = Client::new();
    let na = start_node(pa, dport, Some("node-a"));
    run_for(Duration::from_secs(20)).await;
    let nb = start_node(pb, dport, Some("node-b"));
    run_for(Duration::from_secs(25)).await;
    let nc = start_node(pc, dport, Some("node-c"));
    run_for(Duration::from_secs(25)).await;
    let roster3 = roster(dport).await;
    assert_eq!(roster3.len(), 3, "A, B and C are members: {roster3:?}");
    let names = [
        name_at(&roster3, a),
        name_at(&roster3, b),
        name_at(&roster3, c),
    ];
    let ring3 = HashRing::new(names.to_vec());
    for key in keys {
        for owner in ring3.owners(&Key::unnamespaced(Bytes::from(key.clone())), 2) {
            let port = [a, b, c][names.iter().position(|n| n == owner).unwrap()];
            assert_eq!(client.set(port, key, "v1").await, "S", "{key} on {owner}");
        }
    }
    run_for(Duration::from_secs(70)).await;
    Trio {
        dport,
        disc,
        a,
        b,
        names,
        nodes: [na, nb, nc],
        client,
    }
}

/// What one run of the #977 scenario saw.
#[derive(Debug)]
struct RelayOutcome {
    /// Keys A and C owned, that D takes from B once C is gone: A keeps
    /// them, B gets a copy from A's pass while handing off to D and
    /// relays it to D.
    keys: usize,
    /// Keys D holds once promoted, after the client expired them on A and
    /// A forwarded the expiry to D: B's relay, released afterwards,
    /// brought them back.
    d_revived: usize,
    a_kept: usize,
}

/// #977: a copy a re-replication pass delivered to a node whose handoff to
/// the joiner was in flight is relayed to the joiner from that node's
/// forward queue, as it arrived; a client's expiry forwarded to the joiner
/// meanwhile is stored there as an entry that has just expired, and the
/// relay, landing after it, counts the key as absent and revives it.
///
/// C dies while A's pass to B (promoted by the eviction) is held; D joins
/// and B's transfer to D is held, so the pass copy lands on B with its
/// handoff in flight; B's forward queue is held, so the relay waits;
/// the client expires the keys on A alone, and A forwards the expiry to
/// D; then B's relay is let through.
async fn scenario_977() -> RelayOutcome {
    use crate::hash_ring::HashRing;
    use crate::key::Key;

    let mut dp = Port::reserve();
    let (mut pa, mut pb, mut pc, mut pd) = (
        Port::reserve(),
        Port::reserve(),
        Port::reserve(),
        Port::reserve(),
    );
    let keys: Vec<String> = (0..300).map(|i| format!("y{i}")).collect();
    let Trio {
        dport,
        disc,
        a,
        b: _,
        names,
        nodes: [na, nb, nc],
        mut client,
    } = trio(&mut dp, &mut pa, &mut pb, &mut pc, &keys).await;
    let [name_a, name_b, name_c] = names;
    let ring3 = HashRing::new(vec![name_a.clone(), name_b.clone(), name_c.clone()]);

    // C dies. A's pass promotes B for the keys A and C owned, but is held
    // before its first send.
    na.gates.pass.hold();
    nc.abort();
    run_until_members(dport, 2).await;
    run_for(Duration::from_secs(6)).await;

    // D joins; B's transfer to D is held, so B's handoff stays in flight.
    nb.gates.transfer.hold();
    nb.gates.forward.hold();
    let nd = start_node(&mut pd, dport, Some("node-d"));
    run_for(Duration::from_secs(2)).await;
    let roster = roster(dport).await;
    assert_eq!(
        roster.len(),
        2,
        "D is not promoted while B's transfer is held: {roster:?}"
    );
    let name_d = {
        // Not in the roster yet: registered under the name it was started
        // with, which discovery reports once promoted; compute the ring
        // from the name it will have.
        "node-d".to_string()
    };
    let ring_abd = HashRing::new(vec![name_a.clone(), name_b.clone(), name_d.clone()]);
    let chosen: Vec<String> = keys
        .iter()
        .filter(|key| {
            let k = Key::unnamespaced(Bytes::from((*key).clone()));
            let before = ring3.owners(&k, 2);
            let after = ring_abd.owners(&k, 2);
            before.contains(&name_a.as_str())
                && before.contains(&name_c.as_str())
                && after.contains(&name_a.as_str())
                && after.contains(&name_d.as_str())
        })
        .cloned()
        .collect();
    let mut out = RelayOutcome {
        keys: chosen.len(),
        d_revived: 0,
        a_kept: 0,
    };

    // A's pass delivers the keys to B, which relays them to D — the relay
    // waits in B's held forward queue.
    na.gates.pass.release();
    run_for(Duration::from_secs(1)).await;
    // The client expires the keys on A alone; A forwards the expiry to D.
    for key in &chosen {
        assert_eq!(client.touch(a, key, Some(0)).await, "S", "{key}");
    }
    run_for(Duration::from_secs(1)).await;
    // B's relay lands on D after the expiry.
    nb.gates.forward.release();
    run_for(Duration::from_secs(1)).await;
    nb.gates.transfer.release();
    run_until_members(dport, 3).await;
    run_for(Duration::from_secs(10)).await;
    let d = pd.number;
    for key in &chosen {
        if client.holds(d, key).await {
            out.d_revived += 1;
        }
        if client.holds(a, key).await {
            out.a_kept += 1;
        }
    }
    disc.abort();
    for node in [na, nb, nd] {
        node.abort();
    }
    out
}

#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn a_relayed_copy_landing_after_a_forwarded_expiry_does_not_revive_the_key_on_the_joiner() {
    let _spin = spin();
    let out = scenario_977().await;
    eprintln!("{out:?}");
    assert!(out.keys > 0, "nothing to check: {out:?}");
    assert_eq!(out.a_kept, 0, "the expiry did not take on A: {out:?}");
    assert_eq!(
        out.d_revived, 0,
        "the relay revived keys the client had expired on the joiner: {out:?}"
    );
}

/// What one run of the #975 (first mechanism) scenario saw.
#[derive(Debug)]
struct LateCopyOutcome {
    /// Keys A and C owned, that D takes from B once C is gone.
    keys: usize,
    /// Keys B serves once D is gone and B owns them again: the pass copy
    /// that landed on B after its handoff snapshot was never marked, so
    /// it outlived the join, and the client's expiry on the owners never
    /// reached B.
    b_revived: usize,
    a_kept: usize,
}

/// #975, first mechanism: a re-replication copy that lands on a node after
/// its own join handoff took its snapshot — for a key the joiner owns and
/// this node does not — was stored without the dead-copy mark, so the
/// sweep never removed it, no write reached it, and it came back when the
/// node owned the key again.
async fn scenario_975_late_copy() -> LateCopyOutcome {
    use crate::hash_ring::HashRing;
    use crate::key::Key;

    let mut dp = Port::reserve();
    let (mut pa, mut pb, mut pc, mut pd) = (
        Port::reserve(),
        Port::reserve(),
        Port::reserve(),
        Port::reserve(),
    );
    let keys: Vec<String> = (0..300).map(|i| format!("p{i}")).collect();
    let Trio {
        dport,
        disc,
        a,
        b,
        names,
        nodes: [na, nb, nc],
        mut client,
    } = trio(&mut dp, &mut pa, &mut pb, &mut pc, &keys).await;
    let [name_a, name_b, name_c] = names;
    let ring3 = HashRing::new(vec![name_a.clone(), name_b.clone(), name_c.clone()]);

    na.gates.pass.hold();
    nc.abort();
    run_until_members(dport, 2).await;
    run_for(Duration::from_secs(6)).await;
    nb.gates.transfer.hold();
    let nd = start_node(&mut pd, dport, Some("node-d"));
    run_for(Duration::from_secs(2)).await;
    assert_eq!(
        roster(dport).await.len(),
        2,
        "D is not promoted while B's transfer is held"
    );
    let ring_abd = HashRing::new(vec![name_a.clone(), name_b.clone(), "node-d".to_string()]);
    let chosen: Vec<String> = keys
        .iter()
        .filter(|key| {
            let k = Key::unnamespaced(Bytes::from((*key).clone()));
            let before = ring3.owners(&k, 2);
            let after = ring_abd.owners(&k, 2);
            before.contains(&name_a.as_str())
                && before.contains(&name_c.as_str())
                && after.contains(&name_a.as_str())
                && !after.contains(&name_b.as_str())
        })
        .cloned()
        .collect();
    let mut out = LateCopyOutcome {
        keys: chosen.len(),
        b_revived: 0,
        a_kept: 0,
    };
    // The pass copies land on B after its snapshot, while its handoff is
    // in flight.
    na.gates.pass.release();
    run_for(Duration::from_secs(1)).await;
    nb.gates.transfer.release();
    run_until_members(dport, 3).await;
    // Past the join's confirmation on B and the sweep that follows it.
    run_for(Duration::from_secs(12)).await;
    let d = pd.number;
    // Both owners, as a replicating client does: the second leg may find
    // the key gone already, the first owner having forwarded the expiry
    // to the joiner ahead of it.
    for key in &chosen {
        for port in [a, d] {
            let reply = client.touch(port, key, Some(0)).await;
            assert!(reply == "S" || reply == "N", "{key} on {port}: {reply}");
        }
    }
    run_for(Duration::from_secs(1)).await;
    // D dies: B owns the keys again. A has nothing to re-replicate.
    nd.abort();
    run_until_members(dport, 2).await;
    run_for(Duration::from_secs(10)).await;
    for key in &chosen {
        if client.holds(b, key).await {
            out.b_revived += 1;
        }
        if client.holds(a, key).await {
            out.a_kept += 1;
        }
    }
    disc.abort();
    na.abort();
    nb.abort();
    out
}

#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn a_pass_copy_landing_after_the_handoff_snapshot_is_marked_like_the_keys_the_handoff_sent() {
    let _spin = spin();
    let out = scenario_975_late_copy().await;
    eprintln!("{out:?}");
    assert!(out.keys > 0, "nothing to check: {out:?}");
    assert_eq!(out.a_kept, 0, "the expiry did not take on A: {out:?}");
    assert_eq!(
        out.b_revived, 0,
        "B kept a copy of keys the join took from it and the client expired: {out:?}"
    );
}

/// What one run of the #975 (second mechanism) scenario saw.
#[derive(Debug)]
struct FlipPassOutcome {
    /// Keys A and B owned that D takes from B.
    keys: usize,
    /// Keys D holds after the client expired them on its owners (A and D):
    /// B's join-flip pass, released afterwards, sent D the dead copy B's
    /// handoff had marked.
    d_revived: usize,
    a_kept: usize,
}

/// #975, second mechanism: the re-replication pass a node runs when a join
/// flips its ring — a join whose `M` reached it before its heartbeat had
/// brought it the eviction the join follows, so the flip absorbs the
/// eviction and the pass re-replicates for it — sent the joiner the dead
/// copies its handoff had just marked, put-if-absent; one landing after
/// the client had expired the key on the joiner brought it back.
///
/// C dies; D registers the moment discovery has evicted C, just after A's
/// and B's heartbeats, so their `M` arrives while they still believe in C;
/// B's pass is held until the client has expired the keys on A and D.
async fn scenario_975_flip_pass() -> FlipPassOutcome {
    use crate::hash_ring::HashRing;
    use crate::key::Key;

    let mut dp = Port::reserve();
    let (mut pa, mut pb, mut pc, mut pd) = (
        Port::reserve(),
        Port::reserve(),
        Port::reserve(),
        Port::reserve(),
    );
    let keys: Vec<String> = (0..300).map(|i| format!("f{i}")).collect();
    let Trio {
        dport,
        disc,
        a,
        b: _,
        names,
        nodes: [na, nb, nc],
        mut client,
    } = trio(&mut dp, &mut pa, &mut pb, &mut pc, &keys).await;
    let [name_a, name_b, name_c] = names;
    let ring3 = HashRing::new(vec![name_a.clone(), name_b.clone(), name_c.clone()]);
    let ring_abd = HashRing::new(vec![name_a.clone(), name_b.clone(), "node-d".to_string()]);
    let chosen: Vec<String> = keys
        .iter()
        .filter(|key| {
            let k = Key::unnamespaced(Bytes::from((*key).clone()));
            let before = ring3.owners(&k, 2);
            let after = ring_abd.owners(&k, 2);
            before.contains(&name_a.as_str())
                && before.contains(&name_b.as_str())
                && after.contains(&name_a.as_str())
                && after.contains(&"node-d")
        })
        .cloned()
        .collect();
    let mut out = FlipPassOutcome {
        keys: chosen.len(),
        d_revived: 0,
        a_kept: 0,
    };

    // A's and B's heartbeats share a phase of the 5 s interval (A started
    // at `trio`'s first node, B a whole number of intervals later). C is
    // killed just after one, so discovery evicts it — 15 s of liveness
    // plus its sweep — early in an interval, and D, started the moment
    // the eviction shows, has its `M` on A and B before their next
    // heartbeat brings them the roster without C.
    nb.gates.pass.hold();
    let heartbeat_phase = na.started;
    while (tokio::time::Instant::now() - heartbeat_phase).as_millis() % 5000 != 500 {
        run_for(Duration::from_millis(250)).await;
    }
    nc.abort();
    run_until_members(dport, 2).await;
    let since_heartbeat = (tokio::time::Instant::now() - heartbeat_phase).as_millis() % 5000;
    assert!(
        since_heartbeat < 3000,
        "the eviction showed {since_heartbeat} ms after a heartbeat, too close to the next"
    );
    let nd = start_node(&mut pd, dport, Some("node-d"));
    run_until_members(dport, 3).await;
    let d = pd.number;
    // Before B's slot is confirmed (its next heartbeat): the client expires
    // the keys on their owners, then B's pass is let go. Both owners, as a
    // replicating client does: the second leg may find the key gone
    // already, the first owner having forwarded the expiry to the joiner
    // ahead of it.
    for key in &chosen {
        for port in [a, d] {
            let reply = client.touch(port, key, Some(0)).await;
            assert!(reply == "S" || reply == "N", "{key} on {port}: {reply}");
        }
    }
    nb.gates.pass.release();
    run_for(Duration::from_secs(11)).await;
    for key in &chosen {
        if client.holds(d, key).await {
            out.d_revived += 1;
        }
        if client.holds(a, key).await {
            out.a_kept += 1;
        }
    }
    disc.abort();
    for node in [na, nb, nd] {
        node.abort();
    }
    out
}

#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn a_join_flip_pass_does_not_send_the_joiner_the_dead_copies_its_handoff_marked() {
    let _spin = spin();
    let out = scenario_975_flip_pass().await;
    eprintln!("{out:?}");
    assert!(out.keys > 0, "nothing to check: {out:?}");
    assert_eq!(out.a_kept, 0, "the expiry did not take on A: {out:?}");
    assert_eq!(
        out.d_revived, 0,
        "the join-flip pass revived keys the client had expired on the joiner: {out:?}"
    );
}

#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn a_leave_right_after_a_join_does_not_revive_the_keys_the_join_took_and_the_client_removed()
{
    let _spin = spin();
    let out = scenario_970().await;
    eprintln!("{out:?}");
    assert!(out.keys > 0, "nothing to check: {out:?}");
    assert_eq!(
        out.x_held_before_the_join, out.keys,
        "nothing to check, X did not hold the keys: {out:?}"
    );
    assert_eq!(
        out.d_kept, 0,
        "the joiner kept keys the client removed: {out:?}"
    );
    assert_eq!(
        out.x_revived, 0,
        "the leave revived dead copies of keys the client removed: {out:?}"
    );
}

/// What one run of the tombstone-outliving-its-copy scenario saw.
#[derive(Debug)]
struct TombstoneOutcome {
    /// Keys A and C owned, that D takes from B once C is gone.
    keys: usize,
    b_held_after_the_recreation: usize,
    /// Keys B does not hold once D is gone and A's re-replication pass has
    /// offered it the fresh copy back.
    b_missing: usize,
    a_missing: usize,
}

/// A key deleted and created again within the tombstone's two minutes lost
/// a copy when its owner was displaced and then came back: the delete A's
/// window relayed to B left a tombstone there; the new value, which the
/// window relayed too and B stored as a handoff write, was under it; a
/// join displaced B and the sweep removed its dead copy, but not the
/// tombstone; the joiner's eviction made B an owner again, and A's
/// put-if-absent re-replication was refused by that tombstone, which
/// records a delete older than the copy being refused.
///
/// A, B and C hold keys; C is evicted, A's pass promotes B and opens A's
/// window toward it; inside the window the client deletes the keys on A
/// and creates them again; D joins and takes them from B; once the sweep
/// has removed B's dead copies, D dies, inside the tombstone's two
/// minutes, and B owns the keys again.
async fn scenario_tombstone_swept() -> TombstoneOutcome {
    use crate::hash_ring::HashRing;
    use crate::key::Key;

    let mut dp = Port::reserve();
    let (mut pa, mut pb, mut pc, mut pd) = (
        Port::reserve(),
        Port::reserve(),
        Port::reserve(),
        Port::reserve(),
    );
    let (dport, a, b, c, d) = (dp.number, pa.number, pb.number, pc.number, pd.number);
    let disc = start_discovery(&mut dp, Duration::from_secs(15));
    settle().await;
    let mut client = Client::new();
    let na = start_node(&mut pa, dport, Some("node-a"));
    run_for(Duration::from_secs(20)).await;
    let nb = start_node(&mut pb, dport, Some("node-b"));
    run_for(Duration::from_secs(25)).await;
    let nc = start_node(&mut pc, dport, Some("node-c"));
    run_for(Duration::from_secs(25)).await;
    let roster3 = roster(dport).await;
    assert_eq!(roster3.len(), 3, "A, B and C are members: {roster3:?}");
    let (name_a, name_b, name_c) = (
        name_at(&roster3, a),
        name_at(&roster3, b),
        name_at(&roster3, c),
    );
    let port_of = |name: &str| -> u16 {
        [(&name_a, a), (&name_b, b), (&name_c, c)]
            .into_iter()
            .find(|(n, _)| n.as_str() == name)
            .map(|(_, port)| port)
            .unwrap()
    };
    let ring3 = HashRing::new(vec![name_a.clone(), name_b.clone(), name_c.clone()]);
    let keys: Vec<String> = (0..300).map(|i| format!("z{i}")).collect();
    for key in &keys {
        for owner in ring3.owners(&Key::unnamespaced(Bytes::from(key.clone())), 2) {
            assert_eq!(
                client.set(port_of(owner), key, "v1").await,
                "S",
                "{key} on {owner}"
            );
        }
    }
    run_for(Duration::from_secs(70)).await;

    // C dies; A's pass promotes B for the keys A and C owned and opens A's
    // window toward B for 60 s.
    nc.abort();
    run_until_members(dport, 2).await;
    let chosen: Vec<String> = keys
        .iter()
        .filter(|key| {
            let owners = ring3.owners(&Key::unnamespaced(Bytes::from((*key).clone())), 2);
            owners.contains(&name_a.as_str()) && owners.contains(&name_c.as_str())
        })
        .cloned()
        .collect();
    let mut waited = Duration::ZERO;
    loop {
        let mut held = 0;
        for key in &chosen {
            if client.holds(b, key).await {
                held += 1;
            }
        }
        if held == chosen.len() || waited >= Duration::from_secs(15) {
            assert_eq!(held, chosen.len(), "B was not promoted for the keys");
            break;
        }
        run_for(Duration::from_millis(250)).await;
        waited += Duration::from_millis(250);
    }

    // Inside the window: delete on A, which relays the delete to B (a
    // tombstone there), then create the keys again.
    for key in &chosen {
        let reply = client.delete(a, key).await;
        assert!(reply == "D" || reply == "N", "{key}: {reply}");
    }
    run_for(Duration::from_secs(2)).await;
    for key in &chosen {
        assert_eq!(client.set(a, key, "v2").await, "S", "{key}");
    }
    run_for(Duration::from_secs(1)).await;
    let mut b_held = 0;
    for key in &chosen {
        if client.holds(b, key).await {
            b_held += 1;
        }
    }

    // D joins and takes the keys from B while A stays; the sweep after the
    // join's confirmation removes B's dead copies. D then dies and B owns
    // the keys again.
    let nd = start_node(&mut pd, dport, Some("node-d"));
    run_until_members(dport, 3).await;
    let name_d = name_at(&roster(dport).await, d);
    let ring_abd = HashRing::new(vec![name_a.clone(), name_b.clone(), name_d.clone()]);
    let chosen: Vec<String> = chosen
        .into_iter()
        .filter(|key| {
            let owners = ring_abd.owners(&Key::unnamespaced(Bytes::from(key.clone())), 2);
            owners.contains(&name_a.as_str()) && owners.contains(&name_d.as_str())
        })
        .collect();
    run_for(Duration::from_secs(20)).await;
    nd.abort();
    run_until_members(dport, 2).await;
    run_for(Duration::from_secs(30)).await;
    let mut out = TombstoneOutcome {
        keys: chosen.len(),
        b_held_after_the_recreation: b_held,
        b_missing: 0,
        a_missing: 0,
    };
    for key in &chosen {
        if !client.holds(b, key).await {
            out.b_missing += 1;
        }
        if !client.holds(a, key).await {
            out.a_missing += 1;
        }
    }
    disc.abort();
    na.abort();
    nb.abort();
    out
}

#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn a_delete_tombstone_does_not_outlive_the_dead_copy_that_a_later_write_put_under_it() {
    let _spin = spin();
    let out = scenario_tombstone_swept().await;
    eprintln!("{out:?}");
    assert!(out.keys > 0, "nothing to check: {out:?}");
    assert!(
        out.b_held_after_the_recreation > 0,
        "nothing to check, the window did not carry the new value to B: {out:?}"
    );
    assert_eq!(out.a_missing, 0, "A lost keys it owned throughout: {out:?}");
    assert_eq!(
        out.b_missing, 0,
        "B refused its owner's copy of a key created again after the delete: {out:?}"
    );
}
