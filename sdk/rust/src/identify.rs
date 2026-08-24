//! Connect-and-identify: dials `host:port`, authenticates, and figures
//! out from the server's own `A` response whether it reached a cache node
//! (`On`) or a discovery server (`Od`) — the caller never says which it
//! expects (the server type in the auth response). A node's stream is handed back live; a
//! discovery connection is used once for `L` (the node roster) or `Q`
//! (the proxy roster — SDK proxy mode, issue #122; see
//! `Options::via_proxy` in client.rs) and dropped, returning the
//! name/address list — and, for `L` only, the cluster's replication
//! factor R (node identity, discovery HA, replication). Which of the two
//! a discovery connection is asked for is the caller's choice
//! ([`DiscoveryQuery`]); a node identifies itself the same way regardless.

use std::pin::Pin;
use std::task::{Context, Poll};

use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, BufReader, ReadBuf};
use tokio::net::TcpStream;

use crate::connection::read_line;
use crate::error::{Error, Result};

// A server with no secret accepts any non-empty secret; one that
// requires a real secret correctly rejects this placeholder.
const NO_SECRET_PLACEHOLDER: &[u8] = &[0];

/// Bound a discovery `N` response before allocation, mirroring
/// `MAX_VALUE_LENGTH` on the `V` path: a malicious or MITM'd discovery
/// server must not be able to make the client pre-allocate (and
/// `handle_alloc_error`-abort on) arbitrary memory from an unverified
/// length prefix. Shared verbatim by the proxy roster (`Q`, issue #122)
/// — its entries have exactly the same shape as `L`'s, so the same caps
/// apply.
const MAX_NODE_COUNT: usize = 1 << 16;
const MAX_NODE_FIELD_LENGTH: usize = 64 * 1024;

/// Per-field caps alone still leave the aggregate response size
/// unbounded in practice (`MAX_NODE_COUNT * 2 * MAX_NODE_FIELD_LENGTH` is
/// ~8.5GB) — this caps the total, bounding a malicious discovery
/// server's memory pressure on the client while comfortably fitting a
/// full 65536-node registry of ordinary name/address lengths. Shared
/// with the proxy roster (`Q`, issue #122) for the same reason as the
/// two caps above.
const MAX_NODE_LIST_RESPONSE_BYTES: usize = 16 * 1024 * 1024;

/// A node's hash-ring identity (a random per-process UUID) and its
/// network address — two different things since node identity decoupled from address.
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
///
/// `build_tls_config`'s file read (`std::fs::File::open`) and PEM parsing
/// are blocking calls; since `connect()` (this function's only caller) is
/// itself async, running them inline would block whatever tokio worker
/// thread happens to poll it — a real stall under a multi-thread runtime
/// with a slow or network-mounted CA file. `spawn_blocking` moves that
/// work onto tokio's blocking thread pool instead (issue #47 audit item
/// R3); the join error case only fires if that task panics, which
/// `build_tls_config` never does on its own.
#[cfg(feature = "tls")]
pub(crate) async fn resolve_tls(
    tls: bool,
    ca: Option<&std::path::Path>,
) -> Result<Option<TlsConfig>> {
    if !tls {
        return Ok(None);
    }
    let ca = ca.map(std::path::Path::to_path_buf);
    let config = tokio::task::spawn_blocking(move || build_tls_config(ca.as_deref()))
        .await
        .map_err(|error| {
            Error::Protocol(format!("nanocached: TLS setup task panicked: {error}"))
        })??;
    Ok(Some(config))
}

#[cfg(not(feature = "tls"))]
pub(crate) async fn resolve_tls(
    tls: bool,
    _ca: Option<&std::path::Path>,
) -> Result<Option<TlsConfig>> {
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
    Node {
        stream: Stream,
        /// Echoed response tags: the node accepted the extended `A ... T`, so this
        /// socket's `G`/`S`/`D` traffic must carry tags and its responses
        /// echo them; `false` means an older node answered the plain-`A`
        /// fallback (see `connect_and_identify`).
        tagged: bool,
    },
    Cluster {
        nodes: Vec<DiscoveredNode>,
        replication: usize,
    },
    /// A discovery server's answer to `Q` (SDK proxy mode, issue #122):
    /// every proxy currently announced to it, name and address exactly
    /// like a `DiscoveredNode`'s (a proxy has no separate identity concept
    /// of its own worth modeling) — only ever returned when the caller
    /// asked with [`DiscoveryQuery::Proxies`]. Unlike `Cluster`, carries
    /// no replication factor: the wire response has no such field (a
    /// proxy client fans nothing out itself).
    Proxies { proxies: Vec<DiscoveredNode> },
}

/// Which roster a discovery connection is asked for once it identifies as
/// `Od` — `L` (the node roster, normal cluster routing) or `Q` (the proxy
/// roster, SDK proxy mode / issue #122, see `Options::via_proxy` in
/// client.rs). Meaningless when the peer turns out to be a cache node
/// (`On`): a node has no roster to fetch either way, so every call site
/// that only ever expects to reach nodes (e.g. dialing an individual
/// node's own address) still passes one, but it's never consulted.
#[derive(Clone, Copy)]
pub(crate) enum DiscoveryQuery {
    Nodes,
    Proxies,
}

/// Default bound on dial + handshake, matching the Go and Java SDKs.
/// Without it, a node whose IP has been reclaimed (a stopped container, a
/// dead cloud instance) blackholes the TCP connect and a caller hangs for
/// the kernel's own timeout — minutes — instead of failing over.
pub(crate) const CONNECT_DEADLINE: std::time::Duration = std::time::Duration::from_secs(5);

pub(crate) async fn connect_and_identify(
    host: &str,
    port: u16,
    auth_secret: Option<&[u8]>,
    tls: Option<&TlsConfig>,
    deadline: std::time::Duration,
    query: DiscoveryQuery,
) -> Result<Identified> {
    match run_identify_attempt(host, port, auth_secret, tls, deadline, true, query).await {
        Ok(identified) => Ok(identified),
        Err(AuthFailure::LegacyServer) => {
            // Echoed response tags transparent fallback: a pre-0019 server treats the
            // extended `A ... T` as a parse error and closes without
            // replying — redial once with the plain form and run the
            // connection untagged (the pre-0019 behavior, desync window
            // included).
            run_identify_attempt(host, port, auth_secret, tls, deadline, false, query)
                .await
                .map_err(AuthFailure::into_error)
        }
        Err(failure) => Err(failure.into_error()),
    }
}

async fn run_identify_attempt(
    host: &str,
    port: u16,
    auth_secret: Option<&[u8]>,
    tls: Option<&TlsConfig>,
    deadline: std::time::Duration,
    request_tags: bool,
    query: DiscoveryQuery,
) -> std::result::Result<Identified, AuthFailure> {
    match tokio::time::timeout(
        deadline,
        do_connect_and_identify(host, port, auth_secret, tls, request_tags, query),
    )
    .await
    {
        Ok(result) => result,
        Err(_) => Err(AuthFailure::Other(Error::ConnectionLost(format!(
            "nanocached: connecting to {host}:{port} timed out after {deadline:?}"
        )))),
    }
}

/// Distinguishes a pre-0019 server slamming the door on the extended `A`
/// (connection closed/reset/broken before any reply) from every other
/// identify failure — only the former is worth retrying with the plain
/// form; a timeout is not one, since the server kept the connection open,
/// it just didn't answer (mirrors the TypeScript SDK's
/// `isLegacyServerSignal`).
enum AuthFailure {
    LegacyServer,
    Other(Error),
}

impl AuthFailure {
    fn into_error(self) -> Error {
        match self {
            AuthFailure::LegacyServer => Error::ConnectionLost(
                "nanocached: connection closed before the expected response arrived".to_string(),
            ),
            AuthFailure::Other(error) => error,
        }
    }
}

/// Classifies an I/O error from writing/reading the `A` exchange: a clean
/// EOF, reset, or broken pipe before any reply is the legacy-server
/// signature above; anything else (DNS, refused, etc.) is just an
/// ordinary connect-time failure.
fn classify_auth_io_error(error: std::io::Error) -> AuthFailure {
    if matches!(
        error.kind(),
        std::io::ErrorKind::UnexpectedEof
            | std::io::ErrorKind::ConnectionReset
            | std::io::ErrorKind::BrokenPipe
    ) {
        AuthFailure::LegacyServer
    } else {
        AuthFailure::Other(error.into())
    }
}

async fn do_connect_and_identify(
    host: &str,
    port: u16,
    auth_secret: Option<&[u8]>,
    tls: Option<&TlsConfig>,
    request_tags: bool,
    query: DiscoveryQuery,
) -> std::result::Result<Identified, AuthFailure> {
    let stream = open(host, port, tls).await.map_err(AuthFailure::Other)?;
    let mut stream = BufReader::new(stream);

    let secret = auth_secret.unwrap_or(NO_SECRET_PLACEHOLDER);
    // Echoed response tags: always send the extended form — a `T` after the secret
    // length asks the server to echo tags on this connection's G/S/D
    // traffic. A pre-0019 server can't parse this and slams the door
    // (see `classify_auth_io_error`/`connect_and_identify`'s fallback).
    let mut auth = format!(
        "A {}{}\n",
        secret.len(),
        if request_tags { " T" } else { "" }
    )
    .into_bytes();
    auth.extend_from_slice(secret);
    stream
        .get_mut()
        .write_all(&auth)
        .await
        .map_err(classify_auth_io_error)?;

    // Read the fixed 3-byte ack first: `On\n`/`En\n`/`Od\n`/`Ed\n` — the
    // pre-0019 shape. A `T` as the third byte instead of `\n` means the
    // server understood the extended `A` and is about to echo tags on
    // this connection; read one more byte to confirm its `\n` terminator
    // (`OnT\n`/`EnT\n`/`OdT\n`/`EdT\n`).
    let mut ack = [0u8; 3];
    stream
        .read_exact(&mut ack)
        .await
        .map_err(classify_auth_io_error)?;

    let tagged = if ack[2] == b'\n' {
        false
    } else if ack[2] == b'T' {
        let mut terminator = [0u8; 1];
        stream
            .read_exact(&mut terminator)
            .await
            .map_err(classify_auth_io_error)?;
        if terminator[0] != b'\n' {
            return Err(AuthFailure::Other(Error::Protocol(
                "nanocached: unexpected response to A".to_string(),
            )));
        }
        true
    } else {
        return Err(AuthFailure::Other(Error::Protocol(
            "nanocached: unexpected response to A".to_string(),
        )));
    };

    if !matches!(ack[0], b'O' | b'E') || !matches!(ack[1], b'n' | b'd') {
        return Err(AuthFailure::Other(Error::Protocol(
            "nanocached: unexpected response to A".to_string(),
        )));
    }
    if ack[0] == b'E' {
        // A well-formed rejection of the secret itself — not a protocol
        // violation — so it maps to `Error::Authentication`, never
        // `Error::Protocol` (a genuine "the server sent something the
        // wire protocol doesn't allow" case; see the two `ack[0]`/`ack[1]`
        // shape checks above) or `Error::ConnectionLost` (which implies a
        // redial might help; retrying with the same secret never will).
        return Err(AuthFailure::Other(if auth_secret.is_none() {
            Error::Authentication(format!(
                "nanocached: {host}:{port} requires authentication, but no auth_secret was given"
            ))
        } else {
            Error::Authentication("nanocached: authentication failed".to_string())
        }));
    }

    if ack[1] == b'n' {
        return Ok(Identified::Node {
            stream: stream.into_inner(),
            tagged,
        });
    }

    // A discovery server: one-shot `L` or `Q` (per `query`), then this
    // connection is done. Tags have no meaning on a discovery connection
    // (a single request and done), but the extended ack still had to be
    // parsed above.
    match query {
        DiscoveryQuery::Nodes => {
            stream
                .get_mut()
                .write_all(b"L\n")
                .await
                .map_err(|error| AuthFailure::Other(error.into()))?;
            read_node_list(&mut stream)
                .await
                .map_err(AuthFailure::Other)
        }
        DiscoveryQuery::Proxies => {
            stream
                .get_mut()
                .write_all(b"Q\n")
                .await
                .map_err(|error| AuthFailure::Other(error.into()))?;
            read_proxy_list(&mut stream)
                .await
                .map_err(AuthFailure::Other)
        }
    }
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

    // `N <count> <r>\n` (client-side replication) — the replication factor rides along.
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
    if count > MAX_NODE_COUNT {
        return Err(bad_header(()));
    }

    let mut nodes = Vec::with_capacity(count.min(1024));
    let mut total = 0usize;
    for _ in 0..count {
        let entry = read_line_checked(stream).await?;
        total += entry.len();
        let mut lengths = entry.split(' ');
        let (name_length, addr_length) = match (lengths.next(), lengths.next(), lengths.next()) {
            (Some(name), Some(addr), None) => (
                name.parse::<usize>().map_err(bad_header)?,
                addr.parse::<usize>().map_err(bad_header)?,
            ),
            _ => return Err(bad_header(())),
        };
        if name_length > MAX_NODE_FIELD_LENGTH || addr_length > MAX_NODE_FIELD_LENGTH {
            return Err(bad_header(()));
        }

        let body_length = name_length + addr_length + 1; // +1: trailing '\n'
        total += body_length;
        if total > MAX_NODE_LIST_RESPONSE_BYTES {
            return Err(Error::Protocol(format!(
                "nanocached: discovery node-list response exceeds {MAX_NODE_LIST_RESPONSE_BYTES} bytes"
            )));
        }

        let mut body = vec![0u8; body_length];
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

/// `Q`'s response (SDK proxy mode, issue #122): `N <count>\n` — unlike
/// `L`'s header, no replication field, since a proxy client fans nothing
/// out itself — then, per proxy, exactly `L`'s own entry shape
/// (`<name-len> <addr-len>\n<name><addr>\n`). Bounded identically to
/// `read_node_list` (`MAX_NODE_COUNT`/`MAX_NODE_FIELD_LENGTH`/
/// `MAX_NODE_LIST_RESPONSE_BYTES`, shared with it) — see those constants'
/// doc comments.
async fn read_proxy_list(stream: &mut BufReader<Stream>) -> Result<Identified> {
    let header = read_line_checked(stream).await?;

    if header.starts_with('B') {
        return Err(Error::DiscoveryBusy);
    }
    let Some(count) = header.strip_prefix("N ") else {
        return Err(Error::Protocol(format!(
            "nanocached: unexpected response from discovery server: {header}"
        )));
    };
    let count: usize = count.parse().map_err(bad_header)?;
    if count > MAX_NODE_COUNT {
        return Err(bad_header(()));
    }

    let mut proxies = Vec::with_capacity(count.min(1024));
    let mut total = 0usize;
    for _ in 0..count {
        let entry = read_line_checked(stream).await?;
        total += entry.len();
        let mut lengths = entry.split(' ');
        let (name_length, addr_length) = match (lengths.next(), lengths.next(), lengths.next()) {
            (Some(name), Some(addr), None) => (
                name.parse::<usize>().map_err(bad_header)?,
                addr.parse::<usize>().map_err(bad_header)?,
            ),
            _ => return Err(bad_header(())),
        };
        if name_length > MAX_NODE_FIELD_LENGTH || addr_length > MAX_NODE_FIELD_LENGTH {
            return Err(bad_header(()));
        }

        let body_length = name_length + addr_length + 1; // +1: trailing '\n'
        total += body_length;
        if total > MAX_NODE_LIST_RESPONSE_BYTES {
            return Err(Error::Protocol(format!(
                "nanocached: discovery proxy-list response exceeds {MAX_NODE_LIST_RESPONSE_BYTES} bytes"
            )));
        }

        let mut body = vec![0u8; body_length];
        stream.read_exact(&mut body).await?;
        if body.last() != Some(&b'\n') {
            return Err(Error::Protocol(
                "nanocached: malformed proxy entry in discovery response".to_string(),
            ));
        }
        let name = String::from_utf8(body[..name_length].to_vec()).map_err(|_| bad_header(()))?;
        let address = String::from_utf8(body[name_length..name_length + addr_length].to_vec())
            .map_err(|_| bad_header(()))?;
        proxies.push(DiscoveredNode { name, address });
    }

    Ok(Identified::Proxies { proxies })
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
            DiscoveryQuery::Nodes,
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

    /// A minimal server that always rejects the `A` handshake with `En\n`
    /// (or `EnT\n` if the client asked for tags) — standing in for both
    /// "the server requires a secret and none was given" and "the
    /// configured secret is wrong": from the wire's point of view these
    /// are the same well-formed rejection, and `run_identify_attempt`
    /// only tells them apart by whether `auth_secret` was `None`.
    async fn spawn_auth_rejecting_server() -> u16 {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        tokio::spawn(async move {
            let Ok((mut socket, _)) = listener.accept().await else {
                return;
            };
            let mut stream = BufReader::new(&mut socket);
            let Ok(header) = read_line(&mut stream).await else {
                return;
            };
            let parts: Vec<&str> = header.split(' ').collect();
            let Ok(secret_len) = parts[1].parse::<usize>() else {
                return;
            };
            let mut secret = vec![0u8; secret_len];
            if stream.read_exact(&mut secret).await.is_err() {
                return;
            }
            let reply: &[u8] = if parts.get(2) == Some(&"T") {
                b"EnT\n"
            } else {
                b"En\n"
            };
            let _ = socket.write_all(reply).await;
        });
        port
    }

    #[tokio::test]
    async fn a_missing_required_secret_is_error_authentication_not_protocol() {
        // Fix 3: the server rejecting a handshake that carried no secret
        // at all (this SDK's `auth_secret` unset) must map to
        // `Error::Authentication`, never `Error::Protocol` — it's a
        // well-formed, non-transient rejection, not a wire violation.
        let port = spawn_auth_rejecting_server().await;
        let result = connect_and_identify(
            "127.0.0.1",
            port,
            None,
            None,
            CONNECT_DEADLINE,
            DiscoveryQuery::Nodes,
        )
        .await;
        match result {
            Err(Error::Authentication(message)) => {
                assert!(message.contains("requires authentication"), "{message:?}");
            }
            Ok(_) => panic!("connect_and_identify succeeded, want Err(Error::Authentication(_))"),
            Err(other) => panic!("{other}, want Error::Authentication"),
        }
    }

    #[tokio::test]
    async fn a_wrong_secret_is_error_authentication_not_protocol() {
        let port = spawn_auth_rejecting_server().await;
        let result = connect_and_identify(
            "127.0.0.1",
            port,
            Some(b"wrong"),
            None,
            CONNECT_DEADLINE,
            DiscoveryQuery::Nodes,
        )
        .await;
        match result {
            Err(Error::Authentication(message)) => {
                assert!(message.contains("authentication failed"), "{message:?}");
            }
            Ok(_) => panic!("connect_and_identify succeeded, want Err(Error::Authentication(_))"),
            Err(other) => panic!("{other}, want Error::Authentication"),
        }
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

        // resolve_tls now runs its blocking file I/O on spawn_blocking
        // (issue #47 audit item R3), which needs a tokio runtime even for
        // the tls(false) short-circuit path — hence #[tokio::test] rather
        // than #[test] throughout this module. Error types/messages are
        // unchanged, so the assertions below are otherwise identical to
        // before that change.
        #[tokio::test]
        async fn tls_false_silently_ignores_ca() {
            // Even a nonexistent CA file must not error when tls is off.
            let result = resolve_tls(false, Some(Path::new("/no/such/ca.pem"))).await;
            assert!(matches!(result, Ok(None)));
        }

        #[tokio::test]
        async fn tls_true_without_ca_resolves_to_the_default_trust_store() {
            assert!(resolve_tls(true, None).await.unwrap().is_some());
        }

        #[tokio::test]
        async fn tls_true_with_a_valid_ca_file_replaces_the_default_store() {
            let dir = std::env::temp_dir();
            let path = write_pem(&dir, "nanocached-test-valid-ca.pem", VALID_CA_PEM);
            let result = resolve_tls(true, Some(&path)).await;
            std::fs::remove_file(&path).ok();
            assert!(result.unwrap().is_some());
        }

        #[tokio::test]
        async fn tls_true_with_an_unreadable_ca_file_is_a_connect_time_error() {
            let result = resolve_tls(true, Some(Path::new("/no/such/ca.pem"))).await;
            assert!(
                matches!(result, Err(Error::InvalidArgument(_))),
                "expected InvalidArgument, got {result:?}"
            );
        }

        #[tokio::test]
        async fn tls_true_with_a_ca_file_containing_no_certificates_is_a_connect_time_error() {
            let dir = std::env::temp_dir();
            let path = write_pem(&dir, "nanocached-test-empty-ca.pem", "not a certificate\n");
            let result = resolve_tls(true, Some(&path)).await;
            std::fs::remove_file(&path).ok();
            assert!(
                matches!(result, Err(Error::InvalidArgument(_))),
                "expected InvalidArgument, got {result:?}"
            );
        }
    }

    #[cfg(not(feature = "tls"))]
    #[tokio::test]
    async fn tls_true_without_the_feature_is_a_connect_time_error() {
        let result = resolve_tls(true, None).await;
        assert!(
            matches!(result, Err(Error::InvalidArgument(_))),
            "expected InvalidArgument, got {result:?}"
        );
    }

    #[cfg(not(feature = "tls"))]
    #[tokio::test]
    async fn tls_false_is_fine_without_the_feature() {
        assert!(matches!(resolve_tls(false, None).await, Ok(None)));
    }
}
