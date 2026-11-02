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
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

/// Connections in the burst. Far more than one second's budget of lines,
/// few enough that the burst itself takes well under a second on loopback.
const BURST: usize = 300;
/// The limiter's budget per second and kind of line.
const BUDGET: usize = 50;

struct Discovery {
    child: Child,
    stderr: Arc<Mutex<String>>,
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
    let mut child = Command::new(env!("CARGO_BIN_EXE_nanocached-discovery"))
        .args(["--host", "127.0.0.1", "--port", &port.to_string()])
        .env_remove("NANOCACHED_AUTH_SECRET")
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .spawn()
        .expect("the discovery binary starts");
    let stderr = Arc::new(Mutex::new(String::new()));
    let sink = Arc::clone(&stderr);
    let pipe = child.stderr.take().expect("stderr is piped");
    thread::spawn(move || {
        for line in BufReader::new(pipe).lines().map_while(Result::ok) {
            let mut sink = sink.lock().unwrap();
            sink.push_str(&line);
            sink.push('\n');
        }
    });
    Discovery { child, stderr }
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
