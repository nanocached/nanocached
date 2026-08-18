//! Connect-and-identify: dials `host:port`, authenticates, and figures
//! out from the server's own `A` response whether it reached a cache node
//! (`On`) or a discovery server (`Od`) — the caller never says which it
//! expects (doc/adr/0007-*.md). A node's stream is handed back live; a
//! discovery connection is used once for `L` and dropped, returning the
//! name/address list and the cluster's replication factor R
//! (doc/adr/0009, 0010, 0011).

use std::pin::Pin;
use std::task::{Context, Poll};

use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, BufReader, ReadBuf};
use tokio::net::TcpStream;

use crate::connection::read_line;
use crate::error::{Error, Result};

// A server with no secret accepts any non-empty secret; one that
// requires a real secret correctly rejects this placeholder.
const NO_SECRET_PLACEHOLDER: &[u8] = &[0];

/// A node's hash-ring identity (a random per-process UUID) and its
/// network address — two different things since doc/adr/0009-*.md.
#[derive(Debug, Clone)]
pub struct DiscoveredNode {
    pub name: String,
    pub address: String,
}

/// Plain TCP or TLS, behind one type (mirrors the server's own MaybeTls).
pub(crate) enum Stream {
    Plain(TcpStream),
    #[cfg(feature = "tls")]
    Tls(Box<tokio_rustls::client::TlsStream<TcpStream>>),
}

impl AsyncRead for Stream {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        match self.get_mut() {
            Stream::Plain(stream) => Pin::new(stream).poll_read(cx, buf),
            #[cfg(feature = "tls")]
            Stream::Tls(stream) => Pin::new(stream.as_mut()).poll_read(cx, buf),
        }
    }
}

impl AsyncWrite for Stream {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        match self.get_mut() {
            Stream::Plain(stream) => Pin::new(stream).poll_write(cx, buf),
            #[cfg(feature = "tls")]
            Stream::Tls(stream) => Pin::new(stream.as_mut()).poll_write(cx, buf),
        }
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        match self.get_mut() {
            Stream::Plain(stream) => Pin::new(stream).poll_flush(cx),
            #[cfg(feature = "tls")]
            Stream::Tls(stream) => Pin::new(stream.as_mut()).poll_flush(cx),
        }
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        match self.get_mut() {
            Stream::Plain(stream) => Pin::new(stream).poll_shutdown(cx),
            #[cfg(feature = "tls")]
            Stream::Tls(stream) => Pin::new(stream.as_mut()).poll_shutdown(cx),
        }
    }
}

/// TLS configuration, resolved once at `connect()` time from
/// `Options::tls`/`Options::ca` and reused for every dial (initial
/// connect, lazy reconnects, node-list refreshes).
#[cfg(feature = "tls")]
pub(crate) type TlsConfig = std::sync::Arc<tokio_rustls::rustls::ClientConfig>;
#[cfg(not(feature = "tls"))]
pub(crate) type TlsConfig = std::convert::Infallible;

/// Resolves `Options::tls`/`Options::ca` into a `TlsConfig` to reuse for
/// every dial this client makes. `ca` is meaningful only when `tls` is
/// true — a `ca` set with `tls(false)` is silently ignored, matching
/// every other nanocached SDK.
#[cfg(feature = "tls")]
pub(crate) fn resolve_tls(tls: bool, ca: Option<&std::path::Path>) -> Result<Option<TlsConfig>> {
    if !tls {
        return Ok(None);
    }
    build_tls_config(ca).map(Some)
}

#[cfg(not(feature = "tls"))]
pub(crate) fn resolve_tls(tls: bool, _ca: Option<&std::path::Path>) -> Result<Option<TlsConfig>> {
    if tls {
        return Err(Error::InvalidArgument(
            "nanocached: tls(true) requires the `tls` feature".to_string(),
        ));
    }
    Ok(None)
}

/// Builds a rustls `ClientConfig`: a `ca` PEM file's certificate(s) as
/// the sole trusted roots (replacing the default store, today's
/// semantics), or the platform's native trust store when `ca` is absent
/// (mirrors src/server.rs's `load_tls_connector`, minus the private-CA
/// requirement).
#[cfg(feature = "tls")]
fn build_tls_config(ca: Option<&std::path::Path>) -> Result<TlsConfig> {
    use tokio_rustls::rustls::{ClientConfig, RootCertStore};

    let mut roots = RootCertStore::empty();
    match ca {
        Some(path) => {
            let file = std::fs::File::open(path).map_err(|error| {
                Error::InvalidArgument(format!(
                    "nanocached: could not read CA file {}: {error}",
                    path.display()
                ))
            })?;
            let certs: std::result::Result<Vec<_>, _> =
                rustls_pemfile::certs(&mut std::io::BufReader::new(file)).collect();
            let certs = certs.map_err(|error| {
                Error::InvalidArgument(format!(
                    "nanocached: could not parse CA file {}: {error}",
                    path.display()
                ))
            })?;
            if certs.is_empty() {
                return Err(Error::InvalidArgument(format!(
                    "nanocached: no certificates found in CA file {}",
                    path.display()
                )));
            }
            for cert in certs {
                roots.add(cert).map_err(|error| {
                    Error::InvalidArgument(format!(
                        "nanocached: invalid certificate in CA file {}: {error}",
                        path.display()
                    ))
                })?;
            }
        }
        None => {
            // Best-effort, like every other nanocached SDK's default-store
            // path: a platform cert store entry that fails to parse is
            // skipped rather than failing the whole connect.
            for cert in rustls_native_certs::load_native_certs().certs {
                let _ = roots.add(cert);
            }
        }
    }

    let config = ClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth();
    Ok(std::sync::Arc::new(config))
}

pub(crate) enum Identified {
    Node(Stream),
    Cluster {
        nodes: Vec<DiscoveredNode>,
        replication: usize,
    },
}

/// Default bound on dial + handshake, matching the Go and Java SDKs.
/// Without it, a node whose IP has been reclaimed (a stopped container, a
/// dead cloud instance) blackholes the TCP connect and a caller hangs for
/// the kernel's own timeout — minutes — instead of failing over.
pub(crate) const CONNECT_DEADLINE: std::time::Duration = std::time::Duration::from_secs(10);

pub(crate) async fn connect_and_identify(
    host: &str,
    port: u16,
    auth_secret: Option<&[u8]>,
    tls: Option<&TlsConfig>,
    deadline: std::time::Duration,
) -> Result<Identified> {
    match tokio::time::timeout(
        deadline,
        do_connect_and_identify(host, port, auth_secret, tls),
    )
    .await
    {
        Ok(result) => result,
        Err(_) => Err(Error::ConnectionLost(format!(
            "nanocached: connecting to {host}:{port} timed out after {deadline:?}"
        ))),
    }
}

async fn do_connect_and_identify(
    host: &str,
    port: u16,
    auth_secret: Option<&[u8]>,
    tls: Option<&TlsConfig>,
) -> Result<Identified> {
    let stream = open(host, port, tls).await?;
    let mut stream = BufReader::new(stream);

    let secret = auth_secret.unwrap_or(NO_SECRET_PLACEHOLDER);
    let mut auth = format!("A {}\n", secret.len()).into_bytes();
    auth.extend_from_slice(secret);
    stream.get_mut().write_all(&auth).await?;

    let mut ack = [0u8; 3];
    stream.read_exact(&mut ack).await?;
    if ack[2] != b'\n' || !matches!(ack[0], b'O' | b'E') || !matches!(ack[1], b'n' | b'd') {
        return Err(Error::Protocol(
            "nanocached: unexpected response to A".to_string(),
        ));
    }
    if ack[0] == b'E' {
        return Err(if auth_secret.is_none() {
            Error::Protocol(format!(
                "nanocached: {host}:{port} requires authentication, but no auth_secret was given"
            ))
        } else {
            Error::Protocol("nanocached: authentication failed".to_string())
        });
    }

    if ack[1] == b'n' {
        return Ok(Identified::Node(stream.into_inner()));
    }

    // A discovery server: one-shot `L`, then this connection is done.
    stream.get_mut().write_all(b"L\n").await?;
    read_node_list(&mut stream).await
}

async fn open(host: &str, port: u16, tls: Option<&TlsConfig>) -> Result<Stream> {
    let tcp = TcpStream::connect((host, port)).await?;
    tcp.set_nodelay(true).ok();

    match tls {
        None => Ok(Stream::Plain(tcp)),
        #[cfg(feature = "tls")]
        Some(config) => {
            let server_name =
                rustls_pki_types::ServerName::try_from(host.to_string()).map_err(|_| {
                    Error::InvalidArgument(format!("nanocached: invalid TLS host: {host}"))
                })?;
            let connector = tokio_rustls::TlsConnector::from(config.clone());
            let stream = connector.connect(server_name, tcp).await?;
            Ok(Stream::Tls(Box::new(stream)))
        }
        #[cfg(not(feature = "tls"))]
        Some(_) => unreachable!("TlsConfig is uninhabited without the tls feature"),
    }
}

async fn read_node_list(stream: &mut BufReader<Stream>) -> Result<Identified> {
    let header = read_line_checked(stream).await?;

    if header.starts_with('B') {
        return Err(Error::DiscoveryBusy);
    }
    let Some(rest) = header.strip_prefix("N ") else {
        return Err(Error::Protocol(format!(
            "nanocached: unexpected response from discovery server: {header}"
        )));
    };

    // `N <count> <r>\n` (ADR-0011) — the replication factor rides along.
    let mut fields = rest.split(' ');
    let (count, replication) = match (fields.next(), fields.next(), fields.next()) {
        (Some(count), Some(replication), None) => (
            count.parse::<usize>().map_err(bad_header)?,
            replication.parse::<usize>().map_err(bad_header)?,
        ),
        _ => return Err(bad_header(())),
    };
    if replication < 1 {
        return Err(Error::Protocol(
            "nanocached: invalid replication factor in discovery response".to_string(),
        ));
    }

    let mut nodes = Vec::with_capacity(count.min(1024));
    for _ in 0..count {
        let entry = read_line_checked(stream).await?;
        let mut lengths = entry.split(' ');
        let (name_length, addr_length) = match (lengths.next(), lengths.next(), lengths.next()) {
            (Some(name), Some(addr), None) => (
                name.parse::<usize>().map_err(bad_header)?,
                addr.parse::<usize>().map_err(bad_header)?,
            ),
            _ => return Err(bad_header(())),
        };

        let mut body = vec![0u8; name_length + addr_length + 1]; // +1: trailing '\n'
        stream.read_exact(&mut body).await?;
        if body.last() != Some(&b'\n') {
            return Err(Error::Protocol(
                "nanocached: malformed node entry in discovery response".to_string(),
            ));
        }
        let name = String::from_utf8(body[..name_length].to_vec()).map_err(|_| bad_header(()))?;
        let address = String::from_utf8(body[name_length..name_length + addr_length].to_vec())
            .map_err(|_| bad_header(()))?;
        nodes.push(DiscoveredNode { name, address });
    }

    Ok(Identified::Cluster { nodes, replication })
}

fn bad_header<T>(_: T) -> Error {
    Error::Protocol("nanocached: invalid node-list frame in discovery response".to_string())
}

async fn read_line_checked(stream: &mut BufReader<Stream>) -> Result<String> {
    read_line(stream).await
}

pub(crate) fn split_host_port(address: &str) -> Result<(String, u16)> {
    let Some((host, port)) = address.rsplit_once(':') else {
        return Err(Error::Protocol(format!(
            "nanocached: invalid node address from discovery server: {address}"
        )));
    };
    let port: u16 = port.parse().map_err(|_| {
        Error::Protocol(format!(
            "nanocached: invalid node address from discovery server: {address}"
        ))
    })?;
    Ok((host.to_string(), port))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn connecting_to_a_silent_server_fails_within_the_deadline() {
        // A server that accepts the TCP connection but never answers the
        // handshake (a blackholed address behaves the same way) must fail
        // the connect within the deadline instead of hanging.
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let holder = tokio::spawn(async move {
            let mut sockets = Vec::new();
            loop {
                let Ok((socket, _)) = listener.accept().await else {
                    return;
                };
                sockets.push(socket);
            }
        });

        let started = std::time::Instant::now();
        let result = connect_and_identify(
            "127.0.0.1",
            port,
            None,
            None,
            std::time::Duration::from_millis(100),
        )
        .await;

        assert!(
            matches!(result, Err(Error::ConnectionLost(_))),
            "expected a connection-lost error"
        );
        assert!(
            started.elapsed() < std::time::Duration::from_secs(5),
            "connect_and_identify took {:?}",
            started.elapsed()
        );
        holder.abort();
    }

    #[cfg(feature = "tls")]
    mod tls_config {
        use super::*;
        use std::path::Path;

        // A throwaway self-signed CA cert (openssl req -x509 ..., 10-year
        // validity) — only its shape as a parseable PEM matters here, not
        // its trust chain.
        const VALID_CA_PEM: &str = "-----BEGIN CERTIFICATE-----\n\
MIIDGzCCAgOgAwIBAgIUX0/ng0j0ArO5ai+E6DgpHNW3YTEwDQYJKoZIhvcNAQEL\n\
BQAwHTEbMBkGA1UEAwwSbmFub2NhY2hlZC10ZXN0LWNhMB4XDTI2MDgxODExNDgx\n\
M1oXDTM2MDgxNTExNDgxM1owHTEbMBkGA1UEAwwSbmFub2NhY2hlZC10ZXN0LWNh\n\
MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEAy/Tq6ODLh7BqdocDSMnS\n\
JksOlQxDzKuuTfNRBqqKfUWy0s4qzdoB5xKHYXn5/kZchjdRn5gVAH+sdU0R3H4C\n\
GDF2j8D+uz9Fwhhxfi0wkkuXEUPXXYg9ijBS/vbtXMrhXxmOJawqAVaCVTXfrl3q\n\
D3S0sLNMGUOxiJ2YWmEfYC6793SUFO5dtLfq6reeus9BpRzdR6pOnF0FFEB+da9D\n\
lwrP5klSQT2syDX6b4eGMDZ4EV9zN7qRddVk3u2ZewyvxJcJoeoPFBpzR4UgaHJp\n\
opHdLsUoYUzgO4ERR1vx+XVFrFUU0wz4BmJa3In1j/MwDE/oEm4Oqz8snAxoTgUS\n\
gwIDAQABo1MwUTAdBgNVHQ4EFgQUO+aV67u+OtyFvjsDE0sZwzeLjUMwHwYDVR0j\n\
BBgwFoAUO+aV67u+OtyFvjsDE0sZwzeLjUMwDwYDVR0TAQH/BAUwAwEB/zANBgkq\n\
hkiG9w0BAQsFAAOCAQEADX3fPsL6E7o5+Q58FhN0yoHgGHv+DY/DERrsk8g4VVSH\n\
GfzWp94+a/0C6h7i0BMDQObI2as88oBABPv2wC9vd2Xrfd7lO2uwI4SDtHEfEH6w\n\
qDyoPLENs480WNUOQbt/C4V3IJ+yCpYAD9VDi2xYKBMRKs4fHajPRwO+OVO0o9Om\n\
JMSzHNNqXFVYW+L8hErch9Zv+yThLnjDyoI7CJe9/iv/YsVnw+dgGWJHIkQOhH7U\n\
MHU16fgEz9h08NOh9MJYpE+kz1LpQ56m8+9U1t5rLI/z1rDoDgSONupQ0A2mJkzJ\n\
ValXM/4meyTDFmbKUiHWzNkElZZ8lEhjxHccD4X23w==\n\
-----END CERTIFICATE-----\n";

        fn write_pem(dir: &std::path::Path, name: &str, contents: &str) -> std::path::PathBuf {
            let path = dir.join(name);
            std::fs::write(&path, contents).unwrap();
            path
        }

        #[test]
        fn tls_false_silently_ignores_ca() {
            // Even a nonexistent CA file must not error when tls is off.
            let result = resolve_tls(false, Some(Path::new("/no/such/ca.pem")));
            assert!(matches!(result, Ok(None)));
        }

        #[test]
        fn tls_true_without_ca_resolves_to_the_default_trust_store() {
            assert!(resolve_tls(true, None).unwrap().is_some());
        }

        #[test]
        fn tls_true_with_a_valid_ca_file_replaces_the_default_store() {
            let dir = std::env::temp_dir();
            let path = write_pem(&dir, "nanocached-test-valid-ca.pem", VALID_CA_PEM);
            let result = resolve_tls(true, Some(&path));
            std::fs::remove_file(&path).ok();
            assert!(result.unwrap().is_some());
        }

        #[test]
        fn tls_true_with_an_unreadable_ca_file_is_a_connect_time_error() {
            let result = resolve_tls(true, Some(Path::new("/no/such/ca.pem")));
            assert!(
                matches!(result, Err(Error::InvalidArgument(_))),
                "expected InvalidArgument, got {result:?}"
            );
        }

        #[test]
        fn tls_true_with_a_ca_file_containing_no_certificates_is_a_connect_time_error() {
            let dir = std::env::temp_dir();
            let path = write_pem(&dir, "nanocached-test-empty-ca.pem", "not a certificate\n");
            let result = resolve_tls(true, Some(&path));
            std::fs::remove_file(&path).ok();
            assert!(
                matches!(result, Err(Error::InvalidArgument(_))),
                "expected InvalidArgument, got {result:?}"
            );
        }
    }

    #[cfg(not(feature = "tls"))]
    #[test]
    fn tls_true_without_the_feature_is_a_connect_time_error() {
        let result = resolve_tls(true, None);
        assert!(
            matches!(result, Err(Error::InvalidArgument(_))),
            "expected InvalidArgument, got {result:?}"
        );
    }

    #[cfg(not(feature = "tls"))]
    #[test]
    fn tls_false_is_fine_without_the_feature() {
        assert!(matches!(resolve_tls(false, None), Ok(None)));
    }
}
