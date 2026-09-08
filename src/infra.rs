//! Connection- and process-level glue shared by `nanocached-node`,
//! `nanocached-proxy` and `nanocached-discovery`: TLS setup, the metrics
//! HTTP responder, shutdown-signal handling, the accept-loop backoff, and
//! per-source-IP connection limiting. See the crate root docs for what
//! this deliberately excludes (the HRW ring, wire-protocol parsing,
//! `verify-staged-join`).
//!
//! Every item here used to be an independently maintained copy per binary
//! (three or four, depending on the item); consolidating them removes the
//! risk of the copies quietly drifting apart the way `is_fd_exhaustion`'s
//! non-Unix behavior once had (`nanocached-node`'s copy always backed off
//! on a non-Unix accept error, the others never did — inert today since
//! CI and every deployment target are Unix, but exactly the kind of
//! divergence duplicated infra invites over time). This module now backs
//! every caller with the more conservative always-back-off behavior.

use rustls::pki_types::{CertificateDer, PrivateKeyDer, ServerName};
use rustls::{ClientConfig, RootCertStore, ServerConfig};
use std::collections::HashMap;
use std::io;
use std::io::BufReader;
use std::net::{IpAddr, SocketAddr};
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use std::time::Duration;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, ReadBuf};
use tokio::net::TcpStream;
use tokio::time::timeout;
use tokio_rustls::{TlsAcceptor, TlsConnector};

// ─── TLS-or-plaintext streams ──────────────────────────────────────────

/// Wraps either a plain TCP connection or one wrapped in TLS behind a
/// single type, so the rest of a connection handler doesn't need to know
/// which is in play. `P` is always `TcpStream`; `T` is the TLS-wrapped
/// stream type, which differs between the accept side
/// (`tokio_rustls::server::TlsStream`, for connections a process accepts)
/// and the connect side (`tokio_rustls::client::TlsStream`, for
/// connections a process opens outbound).
pub enum MaybeTls<P, T> {
    Plain(P),
    Tls(Box<T>),
}

impl<P: AsyncRead + Unpin, T: AsyncRead + Unpin> AsyncRead for MaybeTls<P, T> {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        match self.get_mut() {
            MaybeTls::Plain(stream) => Pin::new(stream).poll_read(cx, buf),
            MaybeTls::Tls(stream) => Pin::new(stream.as_mut()).poll_read(cx, buf),
        }
    }
}

impl<P: AsyncWrite + Unpin, T: AsyncWrite + Unpin> AsyncWrite for MaybeTls<P, T> {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        match self.get_mut() {
            MaybeTls::Plain(stream) => Pin::new(stream).poll_write(cx, buf),
            MaybeTls::Tls(stream) => Pin::new(stream.as_mut()).poll_write(cx, buf),
        }
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match self.get_mut() {
            MaybeTls::Plain(stream) => Pin::new(stream).poll_flush(cx),
            MaybeTls::Tls(stream) => Pin::new(stream.as_mut()).poll_flush(cx),
        }
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match self.get_mut() {
            MaybeTls::Plain(stream) => Pin::new(stream).poll_shutdown(cx),
            MaybeTls::Tls(stream) => Pin::new(stream.as_mut()).poll_shutdown(cx),
        }
    }
}

/// Loads a certificate chain and private key from PEM files and builds a
/// `TlsAcceptor` for terminating incoming TLS connections.
pub fn load_tls_acceptor(cert_path: &str, key_path: &str) -> io::Result<TlsAcceptor> {
    let certs = load_cert_chain(cert_path)?;
    let key = load_private_key(key_path)?;

    let config = ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(certs, key)
        .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;

    Ok(TlsAcceptor::from(Arc::new(config)))
}

/// Loads CA certificates from a PEM file and builds a `TlsConnector` that
/// trusts only those CAs (not the system trust store), for a process's own
/// outbound connections to another nanocached process's TLS-secured port.
pub fn load_tls_connector(ca_path: &str) -> io::Result<TlsConnector> {
    let certs = load_cert_chain(ca_path)?;
    let mut roots = RootCertStore::empty();

    for cert in certs {
        roots
            .add(cert)
            .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error.to_string()))?;
    }

    let config = ClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth();

    Ok(TlsConnector::from(Arc::new(config)))
}

fn load_cert_chain(path: &str) -> io::Result<Vec<CertificateDer<'static>>> {
    let file = std::fs::File::open(path)?;
    rustls_pemfile::certs(&mut BufReader::new(file)).collect()
}

fn load_private_key(path: &str) -> io::Result<PrivateKeyDer<'static>> {
    let file = std::fs::File::open(path)?;
    rustls_pemfile::private_key(&mut BufReader::new(file))?.ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            format!("no private key found in {path}"),
        )
    })
}

/// Parses the host portion of a `host:port` address into a TLS server name
/// for certificate verification, accepting either a DNS name or IP address.
/// A bracketed IPv6 host (`[::1]:8356`, required so the port's `:` is
/// unambiguous) has its brackets stripped before conversion — left in,
/// `ServerName::try_from` rejects the string both as an IP (brackets
/// aren't part of the address) and as a DNS name (`[`/`]` aren't valid
/// there either), so TLS to an IPv6 address would otherwise always fail.
pub fn server_name_from_addr(addr: &str) -> io::Result<ServerName<'static>> {
    let host = addr.rsplit_once(':').map_or(addr, |(host, _)| host);
    let host = host
        .strip_prefix('[')
        .and_then(|host| host.strip_suffix(']'))
        .unwrap_or(host);

    ServerName::try_from(host.to_string()).map_err(|error| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("invalid TLS server name {host:?}: {error}"),
        )
    })
}

/// Connects to `addr`, upgrading to TLS first if `tls_connector` is set.
/// There is no plaintext fallback: if TLS is configured and the handshake
/// fails, the connection attempt fails too. `connect_timeout` bounds the
/// TCP connect; `tls_handshake_timeout` bounds the handshake — the caller's
/// own constants (each binary tunes these independently, and shrinks them
/// under test).
///
/// Not used by `nanocached-proxy`'s own outbound dials: those bound the
/// whole connect-plus-handshake-plus-first-exchange sequence with one
/// outer `timeout()` at the call site instead of per-leg timeouts here, a
/// deliberately different composition (a peer slow on the TCP connect
/// leaves proportionally less time for the rest of the exchange, rather
/// than each leg getting a fresh budget) — so proxy keeps its own
/// `connect_upstream`.
pub async fn connect_client_stream(
    addr: &str,
    tls_connector: Option<&TlsConnector>,
    connect_timeout: Duration,
    tls_handshake_timeout: Duration,
) -> io::Result<MaybeTls<TcpStream, tokio_rustls::client::TlsStream<TcpStream>>> {
    let stream = timeout(connect_timeout, TcpStream::connect(addr))
        .await
        .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "connect timed out"))??;
    let _ = stream.set_nodelay(true);

    match tls_connector {
        Some(connector) => {
            let server_name = server_name_from_addr(addr)?;
            let tls_stream = timeout(
                tls_handshake_timeout,
                connector.connect(server_name, stream),
            )
            .await
            .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "TLS handshake timed out"))??;

            Ok(MaybeTls::Tls(Box::new(tls_stream)))
        }
        None => Ok(MaybeTls::Plain(stream)),
    }
}

// ─── shutdown and accept-loop backoff ──────────────────────────────────

/// SIGTERM or ctrl-c.
pub async fn shutdown_signal() -> io::Result<()> {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{SignalKind, signal};

        let mut terminate = signal(SignalKind::terminate())?;

        tokio::select! {
            result = tokio::signal::ctrl_c() => result,
            _ = terminate.recv() => Ok(()),
        }
    }

    #[cfg(not(unix))]
    {
        tokio::signal::ctrl_c().await
    }
}

/// Whether `error` (from a failed `listener.accept()`) looks like the
/// process (EMFILE) or the whole system (ENFILE) being out of file
/// descriptors — the two `accept` failures worth backing off from rather
/// than retrying immediately. Unix-only: there's no portable stable
/// `ErrorKind` for either, and this project's Docker images and dev
/// platforms (Linux, macOS) share these errno values.
pub fn is_fd_exhaustion(error: &io::Error) -> bool {
    #[cfg(unix)]
    {
        matches!(error.raw_os_error(), Some(23) | Some(24)) // ENFILE, EMFILE
    }

    #[cfg(not(unix))]
    {
        let _ = error;
        false
    }
}

/// Whether an accept loop should pause briefly (`ACCEPT_ERROR_BACKOFF`)
/// after `error` rather than immediately retrying `accept`. On Unix this
/// is exactly `is_fd_exhaustion`: any other accept failure there
/// (`ECONNABORTED` and friends) is typically a one-off, per-connection
/// condition safe to retry right away. On non-Unix targets — no portable
/// stable `ErrorKind` for the underlying errno — every accept error backs
/// off instead: more conservative than the Unix check (an occasional
/// one-off failure there now also pays the pause), but a bounded 100ms
/// delay per failed accept is a small cost to avoid an unbounded
/// busy-loop on a sustained one.
pub fn should_backoff_after_accept_error(error: &io::Error) -> bool {
    is_fd_exhaustion(error) || cfg!(not(unix))
}

/// Backoff after an accept() failure recognized by
/// `should_backoff_after_accept_error`.
pub const ACCEPT_ERROR_BACKOFF: Duration = Duration::from_millis(100);

// ─── metrics HTTP responder ─────────────────────────────────────────────

/// Metrics/health/ready connections a listener accepts at once — a small,
/// fixed cap dedicated to that listener, independent of any client
/// connection limit (which is only ever read for a connection-count
/// gauge, never governs this listener). A scrape storm or a stuck
/// orchestrator probe on this port shouldn't be able to spawn an
/// unbounded number of tasks. Fixed rather than a CLI flag: this port
/// only ever sees a handful of legitimate scrapers (Prometheus, a load
/// balancer's health check), never client traffic, so there's no
/// per-deployment tuning need to expose.
pub const METRICS_MAX_CONNECTIONS: usize = 16;

/// Bounded read of one HTTP request head; the GET path, or an error.
/// Anything that isn't a small, well-formed GET is an error — every
/// caller's metrics listener is a scrape endpoint, not a web server.
pub async fn read_http_request_path(stream: &mut TcpStream) -> io::Result<String> {
    let mut head = Vec::new();
    let mut chunk = [0u8; 1024];
    while !head.windows(4).any(|window| window == b"\r\n\r\n") {
        if head.len() > 8192 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "oversized http request head",
            ));
        }
        let bytes_read = stream.read(&mut chunk).await?;
        if bytes_read == 0 {
            break;
        }
        head.extend_from_slice(&chunk[..bytes_read]);
    }

    let head = String::from_utf8_lossy(&head);
    let request_line = head.lines().next().unwrap_or_default();
    let mut parts = request_line.split(' ');
    match (parts.next(), parts.next()) {
        (Some("GET"), Some(path)) => Ok(path.to_string()),
        _ => Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "not a GET request",
        )),
    }
}

pub async fn write_http_response(
    stream: &mut TcpStream,
    status: &str,
    body: &str,
) -> io::Result<()> {
    let response = format!(
        "HTTP/1.1 {status}\r\nContent-Type: text/plain; version=0.0.4; charset=utf-8\r\n\
         Content-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    );
    stream.write_all(response.as_bytes()).await?;
    stream.shutdown().await
}

// ─── constant-time comparison ───────────────────────────────────────────

/// Compares two byte strings without leaking, via timing, how many leading
/// bytes matched. Length differs openly (no secret ever has a length worth
/// hiding), but once lengths match, every byte is compared regardless of
/// earlier mismatches.
pub fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }

    let mut diff: u8 = 0;
    for (x, y) in a.iter().zip(b.iter()) {
        diff |= x ^ y;
    }

    diff == 0
}

// ─── per-source-IP connection limiting ──────────────────────────────────

/// Live connection counts per source IP, shared between an accept loop and
/// the `PerIpConnectionGuard`s it hands out.
pub type PerIpConnections = Arc<Mutex<HashMap<IpAddr, usize>>>;

/// Held by one accepted connection for as long as it counts against its
/// source IP's cap; decrements (and, once it reaches zero, removes) that
/// IP's entry on drop.
pub struct PerIpConnectionGuard {
    counts: PerIpConnections,
    ip: IpAddr,
}

impl Drop for PerIpConnectionGuard {
    fn drop(&mut self) {
        let mut counts = self
            .counts
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());

        if let Some(count) = counts.get_mut(&self.ip) {
            *count -= 1;
            if *count == 0 {
                // Don't let a long-lived process accumulate one entry per
                // distinct IP that has ever connected, most of which will
                // never connect again.
                counts.remove(&self.ip);
            }
        }
    }
}

/// Reserves one of `cap` slots for `ip`, or `None` if it's already at the
/// cap.
pub fn try_acquire_per_ip(
    counts: &PerIpConnections,
    ip: IpAddr,
    cap: usize,
) -> Option<PerIpConnectionGuard> {
    let mut guard = counts
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());

    let count = guard.entry(ip).or_insert(0);
    if *count >= cap {
        return None;
    }
    *count += 1;
    drop(guard);

    Some(PerIpConnectionGuard {
        counts: Arc::clone(counts),
        ip,
    })
}

/// Best-effort busy reply on `stream` before the caller drops it — for an
/// over-limit rejection (a global connection cap, or, per source IP,
/// `try_acquire_per_ip`'s cap). `busy_reply` is the wire bytes for that
/// (every caller's own encoding of a "busy" response — `b"B\n"` on every
/// binary today, but this stays agnostic of any one binary's response
/// type). A TLS-configured listener has no plaintext channel to answer on
/// before the handshake completes (no plaintext fallback once TLS is set)
/// — it just closes. A plaintext listener can still reply on the raw
/// stream. Bounded by `tls_handshake_timeout` (the caller's own constant,
/// reused rather than a new one: a peer that never reads this reply must
/// not leak the task by leaving the write pending indefinitely — the same
/// reasoning as the handshake itself).
pub async fn reject_over_limit(
    mut stream: TcpStream,
    address: SocketAddr,
    tls_acceptor: &Option<TlsAcceptor>,
    tls_handshake_timeout: Duration,
    busy_reply: &[u8],
) {
    if tls_acceptor.is_none() {
        match timeout(tls_handshake_timeout, stream.write_all(busy_reply)).await {
            Ok(Ok(())) => {}
            Ok(Err(error)) => {
                eprintln!("WARN failed to send busy response to {address}: {error}");
            }
            Err(_) => {
                eprintln!("WARN sending busy response to {address} timed out");
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn constant_time_eq_matches_identical_byte_strings() {
        assert!(constant_time_eq(b"same-secret", b"same-secret"));
    }

    #[test]
    fn constant_time_eq_rejects_different_content_of_the_same_length() {
        assert!(!constant_time_eq(b"secret-one", b"secret-two"));
    }

    #[test]
    fn constant_time_eq_rejects_different_lengths() {
        assert!(!constant_time_eq(b"short", b"a much longer value"));
    }

    #[test]
    fn fd_exhaustion_is_detected_for_emfile_and_enfile() {
        assert!(is_fd_exhaustion(&io::Error::from_raw_os_error(24))); // EMFILE
        assert!(is_fd_exhaustion(&io::Error::from_raw_os_error(23))); // ENFILE
    }

    #[test]
    fn fd_exhaustion_is_not_reported_for_other_accept_errors() {
        assert!(!is_fd_exhaustion(&io::Error::from(
            io::ErrorKind::ConnectionAborted
        )));
    }

    #[test]
    fn server_name_from_addr_strips_brackets_from_a_bracketed_ipv6_host() {
        let name = server_name_from_addr("[::1]:8356").unwrap();
        assert_eq!(name, ServerName::try_from("::1").unwrap());
    }

    #[test]
    fn server_name_from_addr_still_handles_a_plain_ipv4_host() {
        let name = server_name_from_addr("127.0.0.1:8356").unwrap();
        assert_eq!(name, ServerName::try_from("127.0.0.1").unwrap());
    }

    #[test]
    fn server_name_from_addr_still_handles_a_dns_name() {
        let name = server_name_from_addr("node-a.example.com:8356").unwrap();
        assert_eq!(name, ServerName::try_from("node-a.example.com").unwrap());
    }

    #[test]
    fn try_acquire_per_ip_admits_up_to_the_cap_then_rejects() {
        let counts: PerIpConnections = Arc::new(Mutex::new(HashMap::new()));
        let ip: IpAddr = "127.0.0.1".parse().unwrap();

        let _first = try_acquire_per_ip(&counts, ip, 2).expect("first fits");
        let _second = try_acquire_per_ip(&counts, ip, 2).expect("second fits");
        assert!(try_acquire_per_ip(&counts, ip, 2).is_none());
    }

    #[test]
    fn per_ip_connection_guard_releases_its_slot_on_drop() {
        let counts: PerIpConnections = Arc::new(Mutex::new(HashMap::new()));
        let ip: IpAddr = "127.0.0.1".parse().unwrap();

        let guard = try_acquire_per_ip(&counts, ip, 1).expect("fits");
        assert!(try_acquire_per_ip(&counts, ip, 1).is_none());

        drop(guard);
        assert!(try_acquire_per_ip(&counts, ip, 1).is_some());
    }
}
