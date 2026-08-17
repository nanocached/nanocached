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

/// TLS configuration: absent (plaintext) or a rustls client config the
/// caller built (system roots, a private CA — their choice).
#[cfg(feature = "tls")]
pub type TlsConfig = std::sync::Arc<tokio_rustls::rustls::ClientConfig>;
#[cfg(not(feature = "tls"))]
pub type TlsConfig = std::convert::Infallible;

pub(crate) enum Identified {
    Node(Stream),
    Cluster {
        nodes: Vec<DiscoveredNode>,
        replication: usize,
    },
}

pub(crate) async fn connect_and_identify(
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
