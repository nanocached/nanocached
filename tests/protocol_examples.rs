//! The examples in `docs/protocol.html` are run against the real node, so
//! the page cannot drift from what the server answers.
//!
//! Two kinds of block are checked:
//!
//! * `<pre class="session">`: one connection to a fresh node. A line starting
//!   with `> ` is sent, a line starting with `< ` is the reply that must
//!   follow, and `= closed` says the server then closes the connection.
//!   Consecutive lines of one direction are joined. The payload after the
//!   marker is the bytes as written on the wire, with `\n`, `\0`, `\\` and
//!   `\xNN` escapes (no newline is implied at the end of a line); a line
//!   starting with `# ` is a comment. An
//!   optional `data-secret="..."` starts the node with that auth secret.
//! * `<pre class="shell">`: `$ printf '<escaped bytes>' | nc <host> <port>`
//!   followed by the literal output, as a reader would run it by hand.
//!
//! The `L`/`Q` example is not run here: it needs a discovery server past its
//! startup grace, and that is longer than a unit-style test should wait.

use std::io::{ErrorKind, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::process::{Child, Command, Stdio};
use std::thread;
use std::time::{Duration, Instant};

const PAGE: &str = include_str!("../docs/protocol.html");

/// Fewer than this and the markers in the page have drifted from the
/// parser, which would otherwise pass by checking nothing.
const MIN_SESSIONS: usize = 12;

struct Node {
    child: Child,
    port: u16,
}

impl Drop for Node {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

fn start_node(secret: Option<&str>) -> Node {
    let port = TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port();
    let mut command = Command::new(env!("CARGO_BIN_EXE_nanocached-node"));
    command
        .args(["--host", "127.0.0.1", "--port", &port.to_string()])
        .env_remove("NANOCACHED_AUTH_SECRET")
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null());
    if let Some(secret) = secret {
        command.env("NANOCACHED_AUTH_SECRET", secret);
    }
    let child = command.spawn().expect("the node binary starts");
    let node = Node { child, port };
    let deadline = Instant::now() + Duration::from_secs(10);
    while TcpStream::connect(("127.0.0.1", port)).is_err() {
        assert!(
            Instant::now() < deadline,
            "the node did not start listening"
        );
        thread::sleep(Duration::from_millis(20));
    }
    node
}

fn connect(node: &Node) -> TcpStream {
    let stream = TcpStream::connect(("127.0.0.1", node.port)).expect("connects");
    stream
        .set_read_timeout(Some(Duration::from_secs(5)))
        .unwrap();
    stream
}

/// Bytes from the page's escaped form.
fn unescape(text: &str, context: &str) -> Vec<u8> {
    let mut out = Vec::new();
    let mut bytes = text.bytes();
    while let Some(byte) = bytes.next() {
        if byte != b'\\' {
            out.push(byte);
            continue;
        }
        match bytes.next() {
            Some(b'n') => out.push(b'\n'),
            Some(b'0') => out.push(0),
            Some(b'\\') => out.push(b'\\'),
            Some(b'x') => {
                let hex: Vec<u8> = bytes.by_ref().take(2).collect();
                let hex = std::str::from_utf8(&hex).unwrap_or("");
                out.push(
                    u8::from_str_radix(hex, 16)
                        .unwrap_or_else(|_| panic!("{context}: bad \\x escape in {text:?}")),
                );
            }
            other => panic!("{context}: unknown escape {other:?} in {text:?}"),
        }
    }
    out
}

fn unescape_html(text: &str) -> String {
    text.replace("&lt;", "<")
        .replace("&gt;", ">")
        .replace("&quot;", "\"")
        .replace("&amp;", "&")
}

struct Block {
    attributes: String,
    body: String,
    line: usize,
}

/// The `<pre class="{class}" ...><code>...</code></pre>` blocks of the page.
fn blocks(class: &str) -> Vec<Block> {
    let open = format!("<pre class=\"{class}\"");
    let mut found = Vec::new();
    let mut from = 0;
    while let Some(start) = PAGE[from..].find(&open) {
        let start = from + start;
        let tag_end = start + PAGE[start..].find('>').expect("a complete tag");
        let attributes = PAGE[start + open.len()..tag_end].to_string();
        let end = tag_end + PAGE[tag_end..].find("</pre>").expect("a closing tag");
        let inner = &PAGE[tag_end + 1..end];
        let body = inner
            .strip_prefix("<code>")
            .and_then(|inner| inner.strip_suffix("</code>"))
            .unwrap_or_else(|| {
                panic!(
                    "line {}: a block is <pre><code>…</code></pre>",
                    line_of(start)
                )
            });
        found.push(Block {
            attributes,
            body: unescape_html(body),
            line: line_of(start),
        });
        from = end;
    }
    found
}

fn line_of(offset: usize) -> usize {
    PAGE[..offset].matches('\n').count() + 1
}

fn attribute<'a>(attributes: &'a str, name: &str) -> Option<&'a str> {
    let marker = format!(" {name}=\"");
    let start = attributes.find(&marker)? + marker.len();
    let end = start + attributes[start..].find('"')?;
    Some(&attributes[start..end])
}

/// Reads exactly `expected.len()` bytes and compares them.
fn expect_reply(stream: &mut TcpStream, expected: &[u8], context: &str) {
    let mut got = vec![0u8; expected.len()];
    let mut filled = 0;
    while filled < got.len() {
        match stream.read(&mut got[filled..]) {
            Ok(0) => break,
            Ok(read) => filled += read,
            Err(error) => panic!(
                "{context}: no reply (read {:?} of {:?}): {error}",
                String::from_utf8_lossy(&got[..filled]),
                String::from_utf8_lossy(expected)
            ),
        }
    }
    assert_eq!(
        String::from_utf8_lossy(&got[..filled]),
        String::from_utf8_lossy(expected),
        "{context}: the reply differs from the page"
    );
}

/// After the last reply nothing more may arrive (`closed` false), or the
/// server must close the connection (`closed` true).
fn expect_quiet_or_closed(stream: &mut TcpStream, closed: bool, context: &str) {
    stream
        .set_read_timeout(Some(Duration::from_millis(300)))
        .unwrap();
    let mut extra = [0u8; 64];
    match stream.read(&mut extra) {
        Ok(0) if closed => {}
        Ok(0) => panic!("{context}: the server closed the connection; the page does not say so"),
        Ok(read) => panic!(
            "{context}: unexpected extra bytes {:?}",
            String::from_utf8_lossy(&extra[..read])
        ),
        Err(error) if matches!(error.kind(), ErrorKind::WouldBlock | ErrorKind::TimedOut) => {
            assert!(
                !closed,
                "{context}: the page says the connection is closed, the server kept it open"
            );
        }
        Err(error) if error.kind() == ErrorKind::ConnectionReset && closed => {}
        Err(error) => panic!("{context}: {error}"),
    }
}

fn run_session(block: &Block) {
    let context = format!("session at docs/protocol.html:{}", block.line);
    let secret = attribute(&block.attributes, "data-secret");
    let node = start_node(secret);
    let mut stream = connect(&node);

    let mut sending: Vec<u8> = Vec::new();
    let mut expecting: Vec<u8> = Vec::new();
    let mut closed = false;
    let mut exchanges = 0;

    let flush = |stream: &mut TcpStream,
                 sending: &mut Vec<u8>,
                 expecting: &mut Vec<u8>,
                 exchanges: &mut usize| {
        if !sending.is_empty() {
            stream.write_all(sending).expect("the request is written");
            sending.clear();
        }
        if !expecting.is_empty() {
            expect_reply(stream, expecting, &context);
            expecting.clear();
            *exchanges += 1;
        }
    };

    for line in block.body.lines() {
        if let Some(payload) = line.strip_prefix("> ") {
            // A new request after a reply: settle the previous exchange.
            if !expecting.is_empty() {
                flush(&mut stream, &mut sending, &mut expecting, &mut exchanges);
            }
            sending.extend(unescape(payload, &context));
        } else if let Some(payload) = line.strip_prefix("< ") {
            expecting.extend(unescape(payload, &context));
        } else if line.starts_with("# ") {
            // a comment for the reader
        } else if line == "= closed" {
            closed = true;
        } else if !line.trim().is_empty() {
            panic!("{context}: a line starts with neither `> `, `< ` nor `= closed`: {line:?}");
        }
    }
    flush(&mut stream, &mut sending, &mut expecting, &mut exchanges);
    assert!(exchanges > 0, "{context}: the session checks no reply");
    expect_quiet_or_closed(&mut stream, closed, &context);
}

fn run_shell(block: &Block) {
    let context = format!("shell example at docs/protocol.html:{}", block.line);
    let mut lines = block.body.lines();
    let command = lines.next().expect("a command line");
    let format = command
        .strip_prefix("$ printf '")
        .and_then(|rest| rest.split_once("' | nc "))
        .unwrap_or_else(|| panic!("{context}: expected `$ printf '…' | nc host port`"))
        .0;
    let request = unescape(format, &context);
    let expected = lines.collect::<Vec<_>>().join("\n");

    let node = start_node(None);
    let mut stream = connect(&node);
    stream.write_all(&request).expect("the request is written");
    expect_reply(&mut stream, expected.as_bytes(), &context);
    expect_quiet_or_closed(&mut stream, false, &context);
}

#[test]
fn the_sessions_in_the_protocol_page_are_what_a_node_answers() {
    let sessions = blocks("session");
    assert!(
        sessions.len() >= MIN_SESSIONS,
        "found {} session blocks, expected at least {MIN_SESSIONS}",
        sessions.len()
    );
    for block in &sessions {
        run_session(block);
    }
}

#[test]
fn the_shell_example_in_the_protocol_page_is_what_a_node_answers() {
    let examples = blocks("shell");
    assert!(!examples.is_empty(), "the page has no shell example");
    for block in &examples {
        run_shell(block);
    }
}
