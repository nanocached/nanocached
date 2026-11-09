//! A node built with the `trace` feature must keep serving when nothing
//! reads its stderr. TRACE lines used to be written with `eprintln!` on the
//! request path, so once the pipe to the log collector was full the write
//! blocked and with it every connection of the node. Only meaningful with
//! `--features trace` (`cargo test --all-features`).
#![cfg(all(feature = "trace", unix))]

use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::process::{Command, Stdio};
use std::thread;
use std::time::{Duration, Instant};

/// More TRACE lines than a pipe (16 to 64 KiB) and the writer's queue hold.
const TRACED_SETS: usize = 12_000;

fn set(stream: &mut TcpStream, key: &str) -> std::io::Result<()> {
    stream.write_all(format!("S {} 1\n{key}v", key.len()).as_bytes())?;
    let mut reply = [0u8; 2];
    stream.read_exact(&mut reply)?;
    assert_eq!(&reply, b"S\n");
    Ok(())
}

#[test]
fn a_node_whose_stderr_nobody_reads_keeps_serving_and_reports_what_it_dropped() {
    let port = TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port();
    let mut child = Command::new(env!("CARGO_BIN_EXE_nanocached-node"))
        .args(["--host", "127.0.0.1", "--port", &port.to_string()])
        .args(["--drain-timeout", "0"])
        .env_remove("NANOCACHED_AUTH_SECRET")
        .env("NANOCACHED_TRACE_PREFIX", "t")
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .spawn()
        .expect("the node binary starts");
    // Taken out now, read only at the end: until then the pipe fills up.
    let mut stderr = child.stderr.take().unwrap();

    let connect = || {
        let deadline = Instant::now() + Duration::from_secs(20);
        loop {
            match TcpStream::connect(("127.0.0.1", port)) {
                Ok(stream) => {
                    stream
                        .set_read_timeout(Some(Duration::from_secs(5)))
                        .unwrap();
                    return stream;
                }
                Err(error) => {
                    assert!(
                        Instant::now() < deadline,
                        "the node never listened: {error}"
                    );
                    thread::sleep(Duration::from_millis(50));
                }
            }
        }
    };
    let mut traced = connect();
    let mut other = connect();

    for index in 0..TRACED_SETS {
        if let Err(error) = set(&mut traced, &format!("t{index}")) {
            let _ = child.kill();
            panic!("a traced request stalled after {index} sets while stderr was unread: {error}");
        }
    }
    // A request that traces nothing is served too.
    if let Err(error) = set(&mut other, "untraced") {
        let _ = child.kill();
        panic!("an untraced request stalled while stderr was unread: {error}");
    }

    // Reading starts now; the node, told to stop, flushes and reports.
    let reader = thread::spawn(move || {
        let mut text = Vec::new();
        let _ = stderr.read_to_end(&mut text);
        String::from_utf8_lossy(&text).into_owned()
    });
    assert!(
        Command::new("kill")
            .args(["-TERM", &child.id().to_string()])
            .status()
            .unwrap()
            .success()
    );
    let deadline = Instant::now() + Duration::from_secs(20);
    let status = loop {
        if let Some(status) = child.try_wait().unwrap() {
            break status;
        }
        if Instant::now() >= deadline {
            let _ = child.kill();
            panic!("the node did not stop within 20 s of SIGTERM");
        }
        thread::sleep(Duration::from_millis(50));
    };
    assert!(status.success(), "{status}");
    let text = reader.join().unwrap();
    assert!(
        text.contains("TRACE set_request key=t"),
        "no TRACE line reached stderr"
    );
    assert!(
        text.lines()
            .any(|line| line.starts_with("TRACE dropped lines=")),
        "the lines that did not fit were not reported"
    );
}
