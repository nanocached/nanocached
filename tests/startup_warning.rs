//! The startup warning for a server that is reachable from off the machine
//! and has no auth secret, checked on the real binaries: the shared check is
//! unit-tested in `infra`, this guards that each binary actually calls it
//! after binding, and that it never stops a server from starting.

use std::io::{BufRead, BufReader};
use std::process::{Child, Command, Stdio};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

const WARNING: &str = "no auth secret";

/// How long a silent case runs before "no warning" counts as an answer:
/// the warning is printed right after the bind, so this is far longer than
/// it needs.
const SETTLE: Duration = Duration::from_secs(2);

struct Server {
    child: Child,
    stderr: Arc<Mutex<String>>,
}

impl Server {
    fn start(binary: &str, extra: &[&str], host: &str, secret: Option<&str>) -> Server {
        let mut command = Command::new(binary);
        command
            .args(extra)
            .args(["--host", host, "--port", "0"])
            .env_remove("NANOCACHED_AUTH_SECRET")
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::piped());
        if let Some(secret) = secret {
            command.env("NANOCACHED_AUTH_SECRET", secret);
        }
        let mut child = command.spawn().expect("the binary starts");

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
        Server { child, stderr }
    }

    fn warnings(&self) -> usize {
        self.stderr.lock().unwrap().matches(WARNING).count()
    }

    /// Waits up to `within` for the first warning.
    fn wait_for_warning(&self, within: Duration) -> bool {
        let deadline = Instant::now() + within;
        while Instant::now() < deadline {
            if self.warnings() > 0 {
                return true;
            }
            thread::sleep(Duration::from_millis(20));
        }
        false
    }

    fn still_running(&mut self) -> bool {
        self.child
            .try_wait()
            .expect("the child can be polled")
            .is_none()
    }
}

impl Drop for Server {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

/// `(name, path, the arguments it needs besides --host/--port)`. The proxy
/// needs a discovery address to start; nothing has to answer there.
fn servers() -> [(&'static str, &'static str, &'static [&'static str]); 3] {
    [
        ("node", env!("CARGO_BIN_EXE_nanocached-node"), &[]),
        ("discovery", env!("CARGO_BIN_EXE_nanocached-discovery"), &[]),
        (
            "proxy",
            env!("CARGO_BIN_EXE_nanocached-proxy"),
            &["--discovery", "127.0.0.1:1"],
        ),
    ]
}

fn assert_silent(host: &str, secret: Option<&str>) {
    for (name, binary, extra) in servers() {
        let mut server = Server::start(binary, extra, host, secret);
        thread::sleep(SETTLE);
        assert_eq!(
            server.warnings(),
            0,
            "{name} on {host}: {}",
            server.stderr.lock().unwrap()
        );
        assert!(server.still_running(), "{name} on {host} must be running");
    }
}

#[test]
fn listening_on_all_interfaces_without_a_secret_warns_once_and_still_starts() {
    for (name, binary, extra) in servers() {
        let mut server = Server::start(binary, extra, "0.0.0.0", None);
        assert!(
            server.wait_for_warning(Duration::from_secs(10)),
            "{name} printed no warning: {}",
            server.stderr.lock().unwrap()
        );
        thread::sleep(Duration::from_millis(500));
        assert_eq!(server.warnings(), 1, "{name}: exactly one warning");
        assert!(server.still_running(), "{name} must keep running");
    }
}

#[test]
fn a_loopback_listener_without_a_secret_is_silent() {
    assert_silent("127.0.0.1", None);
}

#[test]
fn a_hostname_that_resolves_to_loopback_is_silent() {
    assert_silent("localhost", None);
}

#[test]
fn a_secret_silences_the_warning_on_all_interfaces() {
    assert_silent("0.0.0.0", Some("a-secret"));
}
