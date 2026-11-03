//! Discovery's log lines about its peers are rate-limited (issue #1285): a
//! flood of short connections that each end in an error, or each get a
//! request refused, produces a bounded number of lines plus a summary that
//! accounts for the rest, instead of one blocking `eprintln!` per
//! connection on the runtime thread. Checked on the real binary, since the
//! limit lives in the connection path, not in a function a unit test can
//! call.

use std::io::{BufRead, BufReader, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::process::{Child, Command, Stdio};
use std::sync::{Arc, Mutex, MutexGuard};
use std::thread;
use std::time::{Duration, Instant};

/// Connections in the burst. Far more than one second's budget of lines,
/// few enough that the burst itself takes well under a second on loopback.
const BURST: usize = 300;
/// The limiter's budget per second and kind of line.
const BUDGET: usize = 50;

/// The tests of this file run one at a time. Each picks a free port by
/// binding and releasing it, and several open thousands of connections that
/// use up the ephemeral ports: run side by side, one test's discovery could
/// end up on a port another's flood was using, or two could pick the same.
fn serial() -> MutexGuard<'static, ()> {
    static LOCK: Mutex<()> = Mutex::new(());
    LOCK.lock().unwrap_or_else(|poisoned| poisoned.into_inner())
}

struct Discovery {
    child: Child,
    stderr: Arc<Mutex<String>>,
    stdout: Arc<Mutex<String>>,
}

impl Drop for Discovery {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

fn free_port() -> u16 {
    TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

fn start_discovery(port: u16) -> Discovery {
    start_discovery_with(port, &[])
}

fn start_discovery_with(port: u16, extra: &[&str]) -> Discovery {
    let mut child = Command::new(env!("CARGO_BIN_EXE_nanocached-discovery"))
        .args(["--host", "127.0.0.1", "--port", &port.to_string()])
        .args(extra)
        .env_remove("NANOCACHED_AUTH_SECRET")
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("the discovery binary starts");
    let stderr = collect(child.stderr.take().expect("stderr is piped"));
    let stdout = collect(child.stdout.take().expect("stdout is piped"));
    Discovery {
        child,
        stderr,
        stdout,
    }
}

/// Everything `pipe` yields, line by line, as it arrives.
fn collect(pipe: impl Read + Send + 'static) -> Arc<Mutex<String>> {
    let text = Arc::new(Mutex::new(String::new()));
    let sink = Arc::clone(&text);
    thread::spawn(move || {
        for line in BufReader::new(pipe).lines().map_while(Result::ok) {
            let mut sink = sink.lock().unwrap();
            sink.push_str(&line);
            sink.push('\n');
        }
    });
    text
}

fn wait_until_listening(port: u16) {
    let deadline = Instant::now() + Duration::from_secs(10);
    while TcpStream::connect(("127.0.0.1", port)).is_err() {
        assert!(
            Instant::now() < deadline,
            "discovery did not start listening"
        );
        thread::sleep(Duration::from_millis(20));
    }
}

/// Sends `request` on a fresh connection and waits for discovery to close
/// it, so that connections are handled one at a time, never piling up past
/// the per-IP cap (256, under `BURST`), which would answer the excess `B`
/// without an error line.
fn send_and_close(port: u16, request: &[u8]) {
    let mut stream = TcpStream::connect(("127.0.0.1", port)).expect("a connection was refused");
    stream
        .set_read_timeout(Some(Duration::from_secs(5)))
        .unwrap();
    stream.write_all(request).unwrap();
    let mut sink = [0u8; 64];
    while stream.read(&mut sink).is_ok_and(|read| read > 0) {}
}

/// Lines of `log` that start with `individual`, and the count the summary
/// lines (`WARN <n> more <what> (not logged individually)`) report.
fn lines_and_summarised(log: &str, individual: &str, what: &str) -> (usize, usize) {
    let individually = log
        .lines()
        .filter(|line| line.starts_with(individual))
        .count();
    let summarised = log
        .lines()
        .filter_map(|line| {
            line.strip_prefix("WARN ")?
                .strip_suffix(&format!(" more {what} (not logged individually)"))?
                .parse::<usize>()
                .ok()
        })
        .sum();
    (individually, summarised)
}

/// Floods `request` `BURST` times, waits out the window, sends one more so
/// that its line carries the summary of everything dropped, and returns the
/// log.
fn flood(request: &[u8]) -> String {
    let port = free_port();
    let discovery = start_discovery(port);
    wait_until_listening(port);
    // Let the readiness probe's own connection be logged (or not) before
    // the burst starts.
    thread::sleep(Duration::from_millis(1_200));
    let before = discovery.stderr.lock().unwrap().clone();
    assert!(before.is_empty(), "nothing is logged yet; log:\n{before}");

    for _ in 0..BURST {
        send_and_close(port, request);
    }
    thread::sleep(Duration::from_millis(1_200));
    send_and_close(port, request);
    thread::sleep(Duration::from_millis(500));

    discovery.stderr.lock().unwrap().clone()
}

#[test]
fn a_flood_of_failing_connections_is_logged_in_a_bounded_number_of_lines_that_still_add_up() {
    let _serial = serial();
    let log = flood(b"X\n");

    let (individually, summarised) =
        lines_and_summarised(&log, "WARN connection error from ", "connection log lines");
    assert!(
        summarised > 0,
        "the burst must exceed the budget and be summarised; log:\n{log}"
    );
    // Two one-second windows at most for a burst this short, plus the flush line.
    assert!(
        individually <= 2 * BUDGET + 1,
        "{individually} lines logged individually; log:\n{log}"
    );
    assert_eq!(
        individually + summarised,
        BURST + 1,
        "every failed connection is either logged or counted; log:\n{log}"
    );
}

#[test]
fn a_flood_of_rejected_requests_is_logged_in_a_bounded_number_of_lines_that_still_add_up() {
    let _serial = serial();
    // `T` from a name nobody registered: one rejection line and one
    // connection-error line each.
    let log = flood(b"T 1 1\nab");

    let (rejected, rejections) = lines_and_summarised(&log, "WARN rejected T from ", "rejections");
    assert!(
        rejections > 0,
        "the burst must exceed the budget and be summarised; log:\n{log}"
    );
    assert!(
        rejected <= 2 * BUDGET + 1,
        "{rejected} rejections logged individually; log:\n{log}"
    );
    assert_eq!(
        rejected + rejections,
        BURST + 1,
        "every rejection is either logged or counted; log:\n{log}"
    );

    let (errors, errors_summarised) =
        lines_and_summarised(&log, "WARN connection error from ", "connection log lines");
    assert!(
        errors <= 2 * BUDGET + 1,
        "{errors} connection errors logged individually; log:\n{log}"
    );
    assert_eq!(
        errors + errors_summarised,
        BURST + 1,
        "every failed connection is either logged or counted; log:\n{log}"
    );
}

// ---- the paths a peer reaches without being a member (issue #1319) ----
//
// Each is driven `UNAUTHENTICATED` times by a peer that has nothing but the
// address, and the lines it costs must stay within the limiter's budget
// (two windows' worth, plus the line that flushes the summary) with every
// request accounted for in a line or the summary. Before they went through
// the limiter each request wrote its own line: 3,000 wrong-token announces
// wrote 3,000 lines, 20,000 pipelined invalid `C`s wrote 20,000.

/// Requests per flood. Far over the budget, few enough to finish in well
/// under a second on loopback.
const UNAUTHENTICATED: usize = 400;

/// Registers the node `a` (token `b`), the only name the paths below need
/// to exist, with a `P`; returns once discovery has admitted it.
fn register_node_a(port: u16, node_port: u16) {
    let mut stream = TcpStream::connect(("127.0.0.1", port)).unwrap();
    stream
        .set_read_timeout(Some(Duration::from_secs(5)))
        .unwrap();
    stream
        .write_all(format!("P 1 {node_port} 1\nab").as_bytes())
        .unwrap();
    let mut reply = [0u8; 2];
    stream.read_exact(&mut reply).unwrap();
    assert_eq!(&reply, b"R\n");
}

/// Reads and discards everything `stream` receives, until it ends.
fn drain_in_background(mut stream: TcpStream) {
    thread::spawn(move || {
        let mut sink = [0u8; 4096];
        while stream.read(&mut sink).is_ok_and(|read| read > 0) {}
    });
}

/// Every request is a line or part of a summary, and the lines fit the
/// budget of the windows the flood spanned.
fn assert_bounded(log: &str, individual: &str, what: &str, requests: usize) {
    let (individually, summarised) = lines_and_summarised(log, individual, what);
    assert!(
        summarised > 0,
        "the flood must exceed the budget and be summarised; log:\n{log}"
    );
    assert!(
        individually <= 2 * BUDGET + 1,
        "{individually} lines of \"{individual}\" logged individually; log:\n{log}"
    );
    assert_eq!(
        individually + summarised,
        requests,
        "every request is either logged or counted; log:\n{log}"
    );
}

#[test]
fn a_flood_of_announces_with_the_wrong_token_is_logged_in_a_bounded_number_of_lines() {
    let _serial = serial();
    let port = free_port();
    let discovery = start_discovery(port);
    wait_until_listening(port);
    register_node_a(port, 9);
    thread::sleep(Duration::from_millis(1_200));

    for _ in 0..UNAUTHENTICATED {
        send_and_close(port, b"P 1 9 1\nax");
    }
    thread::sleep(Duration::from_millis(1_200));
    send_and_close(port, b"P 1 9 1\nax");
    thread::sleep(Duration::from_millis(500));

    let log = discovery.stderr.lock().unwrap().clone();
    assert_bounded(
        &log,
        "WARN rejected announce for ",
        "rejections",
        UNAUTHENTICATED + 1,
    );
}

#[test]
fn a_flood_of_heartbeats_with_a_wrong_replication_factor_is_logged_in_a_bounded_number_of_lines() {
    let _serial = serial();
    let port = free_port();
    let discovery = start_discovery(port);
    wait_until_listening(port);
    register_node_a(port, 9);
    thread::sleep(Duration::from_millis(1_200));

    // One connection, alternating two factors that both differ from the
    // replica's own (2): each frame is a mismatch to report.
    let stream = TcpStream::connect(("127.0.0.1", port)).unwrap();
    drain_in_background(stream.try_clone().unwrap());
    let mut writer = stream;
    let mut burst = Vec::new();
    for index in 0..UNAUTHENTICATED {
        burst.extend_from_slice(if index % 2 == 0 {
            b"H 1 3 1\nab"
        } else {
            b"H 1 4 1\nab"
        });
    }
    writer.write_all(&burst).unwrap();
    thread::sleep(Duration::from_millis(1_200));
    writer.write_all(b"H 1 3 1\nab").unwrap();
    thread::sleep(Duration::from_millis(500));

    let log = discovery.stderr.lock().unwrap().clone();
    assert_bounded(
        &log,
        "WARN node a reports replication factor ",
        "rejections",
        UNAUTHENTICATED + 1,
    );
}

#[test]
fn a_flood_of_invalid_handoff_reports_on_one_connection_is_logged_in_a_bounded_number_of_lines() {
    let _serial = serial();
    // The reports are only looked at while a join is pending, and one is
    // once a node is `Joined`: `J` from anyone starts it. The startup
    // grace is one liveness timeout; `a` heartbeats through it.
    let node = TcpListener::bind(("127.0.0.1", 0)).unwrap();
    let node_port = node.local_addr().unwrap().port();
    // Takes the `M` discovery sends `a` and never answers, so the join
    // stays pending for the whole test.
    thread::spawn(move || {
        let mut held = Vec::new();
        while let Ok((stream, _)) = node.accept() {
            held.push(stream);
        }
    });

    let port = free_port();
    let discovery = start_discovery_with(port, &["--liveness-timeout", "6"]);
    wait_until_listening(port);
    register_node_a(port, node_port);
    let heartbeats = TcpStream::connect(("127.0.0.1", port)).unwrap();
    drain_in_background(heartbeats.try_clone().unwrap());
    let keep_alive = thread::spawn(move || {
        let mut heartbeats = heartbeats;
        for _ in 0..16 {
            if heartbeats.write_all(b"H 1 2 1\nab").is_err() {
                break;
            }
            thread::sleep(Duration::from_secs(1));
        }
    });

    let mut joiner = TcpStream::connect(("127.0.0.1", port)).unwrap();
    joiner.write_all(b"J 1 9 1\nxy").unwrap();
    drain_in_background(joiner.try_clone().unwrap());
    // Past the grace, so the join has started.
    thread::sleep(Duration::from_millis(8_000));

    // A report is `C <name-len> <joining-len> <token-len>` with the reporter,
    // the joining node and the token as the body: `z` is not a ready member
    // of the join of `x`; `a` is, but with the wrong token.
    for (flood, request) in [
        ("not a ready member", &b"C 1 1 1\nzxb"[..]),
        ("token", &b"C 1 1 1\naxw"[..]),
    ] {
        let stream = TcpStream::connect(("127.0.0.1", port)).unwrap();
        drain_in_background(stream.try_clone().unwrap());
        let mut writer = stream;
        let before = discovery.stderr.lock().unwrap().clone();
        let mut burst = Vec::new();
        for _ in 0..UNAUTHENTICATED {
            burst.extend_from_slice(request);
        }
        writer.write_all(&burst).unwrap();
        thread::sleep(Duration::from_millis(1_200));
        writer.write_all(request).unwrap();
        thread::sleep(Duration::from_millis(500));

        let log = discovery.stderr.lock().unwrap().clone();
        let log = log[before.len()..].to_string();
        let (individually, summarised) = lines_and_summarised(
            &log,
            "WARN ignored handoff-complete report from ",
            "rejections",
        );
        assert!(
            summarised > 0 && individually <= 2 * BUDGET + 1,
            "{flood}: {individually} lines, {summarised} summarised; log:\n{log}"
        );
        assert_eq!(
            individually + summarised,
            UNAUTHENTICATED + 1,
            "{flood}: every report is either logged or counted; log:\n{log}"
        );
    }
    let _ = keep_alive.join();
}

#[test]
fn a_flood_of_connections_over_the_per_ip_limit_that_reset_is_logged_in_a_bounded_number_of_lines()
{
    let _serial = serial();
    let port = free_port();
    let discovery = start_discovery(port);
    wait_until_listening(port);
    // Fills the per-IP cap (256); every connection after it is answered
    // `B` and closed.
    let held: Vec<TcpStream> = (0..256)
        .map(|_| TcpStream::connect(("127.0.0.1", port)).unwrap())
        .collect();
    thread::sleep(Duration::from_millis(500));

    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    // Connections that reset as they close, as a client that went away does.
    let reset_connections = |count: usize| {
        runtime.block_on(async {
            for index in 0..count {
                // A little pacing, as a client population has: back to back
                // the kernel drops some of the resets before the accept.
                if index % 50 == 0 {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
                let socket = tokio::net::TcpSocket::new_v4().unwrap();
                #[allow(deprecated)]
                socket.set_linger(Some(Duration::ZERO)).unwrap();
                let Ok(stream) = socket.connect(([127, 0, 0, 1], port).into()).await else {
                    continue;
                };
                drop(stream);
            }
        })
    };
    let started = Instant::now();
    reset_connections(3_000);
    // A few more once the window is over, so that a line carries the summary.
    thread::sleep(Duration::from_millis(1_200));
    reset_connections(20);
    let windows = started.elapsed().as_secs() as usize + 2;
    thread::sleep(Duration::from_millis(500));
    drop(held);

    let log = discovery.stderr.lock().unwrap().clone();
    // Everything the resets cost goes through the connection log (the
    // busy-response failures, and on some platforms the keepalive failure of
    // a connection already reset), so its lines, and the ones it counted,
    // are what the flood caused.
    let (_, summarised) = lines_and_summarised(&log, "", "connection log lines");
    let lines = log
        .lines()
        .filter(|line| !line.contains(" more connection log lines "))
        .count();
    assert!(
        lines + summarised > 2 * BUDGET + 1,
        "the flood did not exceed what the limiter admits: {lines} lines, {summarised} counted; log:\n{log}"
    );
    assert!(
        lines <= windows * BUDGET + 1,
        "{lines} lines logged in {windows} windows (budget {BUDGET} a second); log:\n{log}"
    );
}

#[test]
fn a_flood_of_joins_that_leave_at_once_is_logged_in_a_bounded_number_of_lines() {
    let _serial = serial();
    let port = free_port();
    let discovery = start_discovery(port);
    wait_until_listening(port);
    thread::sleep(Duration::from_millis(1_200));

    // Connect, `J` under a name nobody holds, leave: each registers a
    // `Waiting` entry, which the disconnect removes before the next one.
    let join_and_leave = |index: usize| {
        let mut stream = TcpStream::connect(("127.0.0.1", port)).expect("a connection was refused");
        stream
            .write_all(format!("J 6 9 1\nj{index:05}k").as_bytes())
            .unwrap();
        thread::sleep(Duration::from_millis(2));
    };
    for index in 0..UNAUTHENTICATED {
        join_and_leave(index);
    }
    thread::sleep(Duration::from_millis(1_200));
    join_and_leave(UNAUTHENTICATED);
    thread::sleep(Duration::from_millis(500));

    let log = discovery.stdout.lock().unwrap().clone();
    let registered = log
        .lines()
        .filter(|line| line.starts_with("INFO node registered: "))
        .count();
    let counted: usize = log
        .lines()
        .filter_map(|line| {
            line.strip_prefix("INFO ")?
                .strip_suffix(" more node registrations (not logged individually)")?
                .parse::<usize>()
                .ok()
        })
        .sum();
    assert!(
        registered + counted > 2 * BUDGET + 1,
        "the flood did not exceed the budget: {registered} lines, {counted} counted; log:\n{log}"
    );
    assert!(
        registered <= 2 * BUDGET + 1,
        "{registered} registrations logged individually; log:\n{log}"
    );
}

/// Sends `request` on a fresh connection and returns what discovery answers
/// before it closes (or the read times out).
fn request_and_read(port: u16, request: &[u8]) -> Vec<u8> {
    let mut stream = TcpStream::connect(("127.0.0.1", port)).expect("a connection was refused");
    stream
        .set_read_timeout(Some(Duration::from_secs(5)))
        .unwrap();
    stream.write_all(request).unwrap();
    let mut reply = Vec::new();
    let mut chunk = [0u8; 64];
    // `R\n` leaves the connection open for heartbeats, `G\n` closes it:
    // stop after the first line either way.
    while !reply.ends_with(b"\n") {
        match stream.read(&mut chunk) {
            Ok(read) if read > 0 => reply.extend_from_slice(&chunk[..read]),
            _ => break,
        }
    }
    reply
}

#[test]
fn a_flood_of_announces_from_an_evicted_node_is_logged_in_a_bounded_number_of_lines() {
    let _serial = serial();
    let port = free_port();
    let discovery = start_discovery(port);
    wait_until_listening(port);

    // `c` announced after `a`, then left: it is on record as departed while
    // `a` has been `Joined` since before that, so an announce from `c` that
    // says it can start over (`G`) is answered `G` and registers nothing.
    assert_eq!(request_and_read(port, b"P 1 9 1\nab"), b"R\n");
    thread::sleep(Duration::from_millis(100));
    assert_eq!(request_and_read(port, b"P 1 9 1\ncd"), b"R\n");
    assert_eq!(request_and_read(port, b"V 1 1\ncd"), b"R\n");
    thread::sleep(Duration::from_millis(1_200));

    let announce = b"P 1 9 1 G\ncd";
    for _ in 0..UNAUTHENTICATED {
        assert_eq!(request_and_read(port, announce), b"G\n");
    }
    thread::sleep(Duration::from_millis(1_200));
    assert_eq!(request_and_read(port, announce), b"G\n");
    thread::sleep(Duration::from_millis(500));

    let log = discovery.stdout.lock().unwrap().clone();
    let individually = log
        .lines()
        .filter(|line| line.starts_with("INFO node announced after its eviction: "))
        .count();
    let counted: usize = log
        .lines()
        .filter_map(|line| {
            line.strip_prefix("INFO ")?
                .strip_suffix(" more node announces (not logged individually)")?
                .parse::<usize>()
                .ok()
        })
        .sum();
    assert!(
        counted > 0 && individually <= 2 * BUDGET + 1,
        "{individually} lines logged individually, {counted} counted; log:\n{log}"
    );
    assert_eq!(
        individually + counted,
        UNAUTHENTICATED + 1,
        "every announce is either logged or counted; log:\n{log}"
    );
}
