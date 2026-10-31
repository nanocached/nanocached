//! The node's per-connection log lines are rate-limited (issue #1028): a
//! burst of connections produces a bounded number of `accepted connection`
//! lines plus a summary that accounts for the rest, instead of one line per
//! connection written synchronously on the runtime thread. Checked on the
//! real binary, since the limit lives in the accept path, not in a function
//! a unit test can call.

use std::io::{BufRead, BufReader};
use std::net::{TcpListener, TcpStream};
use std::process::{Child, Command, Stdio};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

/// Connections in the burst. Far more than one second's budget of lines,
/// few enough that the burst itself takes well under a second on loopback.
const BURST: usize = 300;

struct Node {
    child: Child,
    stdout: Arc<Mutex<String>>,
}

impl Drop for Node {
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

fn start_node(port: u16) -> Node {
    let mut child = Command::new(env!("CARGO_BIN_EXE_nanocached-node"))
        .args(["--host", "127.0.0.1", "--port", &port.to_string()])
        .env_remove("NANOCACHED_AUTH_SECRET")
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .expect("the node binary starts");
    let stdout = Arc::new(Mutex::new(String::new()));
    let sink = Arc::clone(&stdout);
    let pipe = child.stdout.take().expect("stdout is piped");
    thread::spawn(move || {
        for line in BufReader::new(pipe).lines().map_while(Result::ok) {
            let mut sink = sink.lock().unwrap();
            sink.push_str(&line);
            sink.push('\n');
        }
    });
    Node { child, stdout }
}

/// Connects and closes at once; the node logs the accept and reads EOF.
fn connect_once(port: u16) -> bool {
    TcpStream::connect(("127.0.0.1", port)).is_ok()
}

#[test]
fn a_burst_of_connections_is_logged_in_a_bounded_number_of_lines_that_still_add_up() {
    let port = free_port();
    let node = start_node(port);

    // The first accepted connection doubles as the readiness check.
    let deadline = Instant::now() + Duration::from_secs(10);
    while !connect_once(port) {
        assert!(
            Instant::now() < deadline,
            "the node did not start listening"
        );
        thread::sleep(Duration::from_millis(20));
    }
    for _ in 1..BURST {
        assert!(connect_once(port), "a connection in the burst was refused");
    }

    // A quiet second, then one more connection: the line it is admitted on
    // carries the summary of everything the burst's window dropped.
    thread::sleep(Duration::from_millis(1_200));
    assert!(connect_once(port));
    thread::sleep(Duration::from_millis(500));

    let log = node.stdout.lock().unwrap().clone();
    let individually = log.matches("INFO accepted connection from ").count();
    let summarised: usize = log
        .lines()
        .filter_map(|line| {
            line.strip_prefix("INFO ")?
                .strip_suffix(" more connections accepted (not logged individually)")?
                .parse::<usize>()
                .ok()
        })
        .sum();

    assert!(
        summarised > 0,
        "the burst must exceed the budget and be summarised; log:\n{log}"
    );
    // Two one-second windows at most for a burst this short, plus the flush line.
    assert!(
        individually <= 2 * 50 + 1,
        "{individually} lines logged individually; log:\n{log}"
    );
    assert_eq!(
        individually + summarised,
        BURST + 1,
        "every accepted connection is either logged or counted; log:\n{log}"
    );
}
