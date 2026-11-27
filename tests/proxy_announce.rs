//! What the proxy announces to discovery (`Y`), checked on the real binary
//! against a stand-in discovery server that records every announce: the
//! port it declares is the one its listener bound, and a proxy whose
//! startup fails never announces at all.

use std::io::{BufRead, BufReader, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::process::{Child, Command, Stdio};
use std::sync::mpsc;
use std::thread;
use std::time::{Duration, Instant};

struct Proxy {
    child: Child,
}

impl Drop for Proxy {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

/// A stand-in discovery server: an `L` gets its connection closed (the
/// proxy logs a failed roster fetch and carries on), a `Y` is answered `R`
/// and its declared port sent on the returned channel.
fn start_discovery() -> (String, mpsc::Receiver<u16>) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    let (announced_tx, announced_rx) = mpsc::channel();
    thread::spawn(move || {
        for stream in listener.incoming() {
            let Ok(stream) = stream else { return };
            let announced_tx = announced_tx.clone();
            thread::spawn(move || serve_discovery(stream, &announced_tx));
        }
    });
    (addr, announced_rx)
}

fn serve_discovery(stream: TcpStream, announced: &mpsc::Sender<u16>) {
    let _ = stream.set_read_timeout(Some(Duration::from_secs(5)));
    let mut writer = stream.try_clone().unwrap();
    let mut reader = BufReader::new(stream);
    // The header, read a byte at a time up to a bound.
    let mut header = Vec::new();
    let mut byte = [0u8; 1];
    while header.len() < 256 {
        if reader.read_exact(&mut byte).is_err() {
            return;
        }
        if byte[0] == b'\n' {
            break;
        }
        header.push(byte[0]);
    }
    let line = String::from_utf8_lossy(&header);
    // `Y <name-len> <port> <token-len>\n<name><token>`
    let fields: Vec<&str> = line.trim_end().split(' ').collect();
    if fields.first() != Some(&"Y") || fields.len() != 4 {
        return;
    }
    let (Ok(name_len), Ok(port), Ok(token_len)) = (
        fields[1].parse::<usize>(),
        fields[2].parse::<u16>(),
        fields[3].parse::<usize>(),
    ) else {
        return;
    };
    let mut body = vec![0u8; name_len + token_len];
    if reader.read_exact(&mut body).is_err() {
        return;
    }
    let _ = announced.send(port);
    let _ = writer.write_all(b"R\n");
}

fn start_proxy(discovery: &str, port: u16, extra: &[&str]) -> (Proxy, mpsc::Receiver<String>) {
    let mut child = Command::new(env!("CARGO_BIN_EXE_nanocached-proxy"))
        .args(["--discovery", discovery, "--host", "127.0.0.1", "--port"])
        .arg(port.to_string())
        .args(extra)
        .env_remove("NANOCACHED_AUTH_SECRET")
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .expect("the proxy binary starts");
    let stdout = child.stdout.take().unwrap();
    let (lines_tx, lines_rx) = mpsc::channel();
    thread::spawn(move || {
        for line in BufReader::new(stdout).lines() {
            let Ok(line) = line else { return };
            if lines_tx.send(line).is_err() {
                return;
            }
        }
    });
    (Proxy { child }, lines_rx)
}

/// The port of the proxy's `listening on <addr>` line.
fn listening_port(lines: &mpsc::Receiver<String>) -> u16 {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let left = deadline.saturating_duration_since(Instant::now());
        let line = lines
            .recv_timeout(left)
            .expect("the proxy did not log its listening address");
        if let Some((_, rest)) = line.split_once("nanocached-proxy listening on ") {
            let addr = rest.split(' ').next().unwrap();
            return addr.rsplit_once(':').unwrap().1.parse().unwrap();
        }
    }
}

#[test]
fn a_proxy_started_with_port_0_announces_the_port_it_bound() {
    let (discovery, announced) = start_discovery();
    let (_proxy, lines) = start_proxy(&discovery, 0, &[]);

    let bound = listening_port(&lines);
    assert_ne!(bound, 0);
    let declared = announced
        .recv_timeout(Duration::from_secs(10))
        .expect("the proxy never announced itself");
    assert_eq!(
        declared, bound,
        "the announce must carry the port the listener bound, not --port"
    );
}

#[test]
fn a_proxy_whose_listener_cannot_bind_never_announces() {
    for metrics in [false, true] {
        let (discovery, announced) = start_discovery();
        let taken = TcpListener::bind("127.0.0.1:0").unwrap();
        let taken_port = taken.local_addr().unwrap().port().to_string();
        let (port, extra): (u16, Vec<&str>) = if metrics {
            (0, vec!["--metrics-port", &taken_port])
        } else {
            (taken.local_addr().unwrap().port(), vec![])
        };
        let (mut proxy, _lines) = start_proxy(&discovery, port, &extra);

        let deadline = Instant::now() + Duration::from_secs(10);
        let status = loop {
            if let Some(status) = proxy.child.try_wait().unwrap() {
                break status;
            }
            assert!(Instant::now() < deadline, "the proxy did not exit");
            thread::sleep(Duration::from_millis(20));
        };
        assert!(!status.success(), "a failed bind must fail the start");
        assert_eq!(
            announced.recv_timeout(Duration::from_millis(500)),
            Err(mpsc::RecvTimeoutError::Timeout),
            "a proxy that never listened announced itself (metrics bind: {metrics})"
        );
        drop(taken);
    }
}
