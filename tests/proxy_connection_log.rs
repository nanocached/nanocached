//! The proxy's connection-level log lines are rate-limited: a flood of
//! short connections that each fail the TLS handshake, or each end in a
//! connection error, produces a bounded number of lines plus a summary
//! that accounts for the rest, instead of one blocking `eprintln!` per
//! connection on a runtime worker. Checked on the real binary, since the
//! limit lives in the accept path, not in a function a unit test can call.

use std::io::{BufRead, BufReader};
use std::net::TcpListener;
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

/// Connections in the burst. Far more than one second's budget of lines,
/// few enough that the burst itself takes well under a second on loopback.
const BURST: usize = 300;
/// The limiter's budget per second.
const BUDGET: usize = 50;

struct Proxy {
    child: Child,
    stderr: Arc<Mutex<String>>,
}

impl Drop for Proxy {
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

/// A certificate and key for the TLS proxy, written under the target
/// directory (the proxy takes PEM files, not bytes).
fn write_tls_files(name: &str) -> (PathBuf, PathBuf) {
    let rcgen::CertifiedKey { cert, signing_key } =
        rcgen::generate_simple_self_signed(vec!["localhost".to_string()]).unwrap();
    let dir = PathBuf::from(env!("CARGO_TARGET_TMPDIR"));
    let cert_path = dir.join(format!("{name}-cert.pem"));
    let key_path = dir.join(format!("{name}-key.pem"));
    std::fs::write(&cert_path, cert.pem()).unwrap();
    std::fs::write(&key_path, signing_key.serialize_pem()).unwrap();
    (cert_path, key_path)
}

/// Starts the proxy on `port`; nothing has to answer at the discovery
/// address it is given.
fn start_proxy(port: u16, tls: Option<(PathBuf, PathBuf)>) -> Proxy {
    let mut command = Command::new(env!("CARGO_BIN_EXE_nanocached-proxy"));
    command.args(["--discovery", "127.0.0.1:1"]).args([
        "--host",
        "127.0.0.1",
        "--port",
        &port.to_string(),
    ]);
    if let Some((cert, key)) = tls {
        command
            .arg("--tls-cert")
            .arg(cert)
            .arg("--tls-key")
            .arg(key);
    }
    let mut child = command
        .env_remove("NANOCACHED_AUTH_SECRET")
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .spawn()
        .expect("the proxy binary starts");
    let stderr = Arc::new(Mutex::new(String::new()));
    let sink = Arc::clone(&stderr);
    let pipe = child.stderr.take().expect("stderr is piped");
    std::thread::spawn(move || {
        for line in BufReader::new(pipe).lines().map_while(Result::ok) {
            let mut sink = sink.lock().unwrap();
            sink.push_str(&line);
            sink.push('\n');
        }
    });
    Proxy { child, stderr }
}

async fn wait_until_listening(port: u16) {
    let deadline = Instant::now() + Duration::from_secs(10);
    while TcpStream::connect(("127.0.0.1", port)).await.is_err() {
        assert!(
            Instant::now() < deadline,
            "the proxy did not start listening"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

/// Sends `bytes` on a fresh connection and waits for the proxy to close
/// it, so that connections are handled one at a time and never pile up
/// past a connection cap.
async fn send_and_wait_for_close(port: u16, bytes: &[u8]) {
    let mut stream = TcpStream::connect(("127.0.0.1", port))
        .await
        .expect("a connection was refused");
    stream.write_all(bytes).await.unwrap();
    let mut sink = [0u8; 64];
    let _ = tokio::time::timeout(Duration::from_secs(5), async {
        while stream.read(&mut sink).await.is_ok_and(|read| read > 0) {}
    })
    .await;
}

/// Sends `bytes` and resets the connection (a close with a zero linger
/// sends RST, not FIN).
async fn send_and_reset(port: u16, bytes: &[u8]) {
    let mut stream = TcpStream::connect(("127.0.0.1", port))
        .await
        .expect("a connection was refused");
    stream.write_all(bytes).await.unwrap();
    stream.set_zero_linger().unwrap();
    drop(stream);
}

/// Lines of `log` that start with `individual`, and the count the summary
/// lines (`WARN <n> more connection log lines (not logged individually)`)
/// report.
fn lines_and_summarised(log: &str, individual: &str) -> (usize, usize) {
    let individually = log
        .lines()
        .filter(|line| line.starts_with(individual))
        .count();
    let summarised = log
        .lines()
        .filter_map(|line| {
            line.strip_prefix("WARN ")?
                .strip_suffix(" more connection log lines (not logged individually)")?
                .parse::<usize>()
                .ok()
        })
        .sum();
    (individually, summarised)
}

/// Floods the proxy with `BURST` connections, waits out the window, sends
/// one more so that its line carries the summary of everything dropped,
/// and returns the log.
async fn flood<F: AsyncFn(u16)>(tls: bool, one_connection: F) -> String {
    let port = free_port();
    let files = tls.then(|| write_tls_files(&format!("proxy-log-{port}")));
    let proxy = start_proxy(port, files);
    wait_until_listening(port).await;
    // Let the readiness probe's own connection be logged (or not) before
    // the burst starts, and its window pass.
    tokio::time::sleep(Duration::from_millis(1_200)).await;
    let before = proxy.stderr.lock().unwrap().clone();

    for _ in 0..BURST {
        one_connection(port).await;
    }
    tokio::time::sleep(Duration::from_millis(1_200)).await;
    one_connection(port).await;
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Only what the burst wrote: the readiness probe is itself a
    // connection the proxy may log a line about.
    proxy.stderr.lock().unwrap()[before.len()..].to_string()
}

/// A flood of connections that each fail with `individual` is logged in at
/// most two windows' budget of lines (a burst this short fits in two), one
/// summary line accounts for the others, and nothing is lost: every
/// failed connection is either logged or counted.
fn assert_bounded_and_accounted(log: &str, individual: &str) {
    let (individually, summarised) = lines_and_summarised(log, individual);
    assert!(
        summarised > 0,
        "the burst must exceed the budget and be summarised; log:\n{log}"
    );
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

#[tokio::test]
async fn a_flood_of_failed_tls_handshakes_is_logged_in_a_bounded_number_of_lines_that_still_add_up()
{
    // A plaintext peer on the TLS port: its bytes are not a ClientHello.
    let log = flood(true, async |port| {
        send_and_wait_for_close(port, b"GET / HTTP/1.0\r\n\r\n").await;
    })
    .await;
    assert_bounded_and_accounted(&log, "WARN TLS handshake with ");
}

#[tokio::test]
async fn a_flood_of_reset_connections_is_logged_in_a_bounded_number_of_lines_that_still_add_up() {
    // Three bytes of a request, then RST: the read fails mid-frame.
    let log = flood(false, async |port| send_and_reset(port, b"S 1").await).await;
    assert_bounded_and_accounted(&log, "WARN connection error from ");
}
