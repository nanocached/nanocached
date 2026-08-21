using System.Net.Security;
using System.Net.Sockets;
using System.Text;

namespace Nanocached;

/// <summary>
/// A node's hash-ring identity (a random per-process UUID) and its network
/// address (<c>host:port</c>) — two different things since
/// Node identity decoupled from address: <see cref="Name"/> is what routing hashes;
/// <see cref="Address"/> is only for opening a connection.
/// </summary>
public sealed record DiscoveredNode(string Name, string Address);

/// <summary>
/// Connect-and-identify: dials <c>host:port</c>, authenticates, and
/// figures out from the server's own <c>A</c> response whether it reached
/// a cache node (<c>On</c>) or a discovery server (<c>Od</c>) — the caller
/// never says which it expects (the server type in the auth response). A node's stream is
/// handed back live; a discovery connection is used once for <c>L</c> and
/// disposed, returning the name/address list and the cluster's replication
/// factor R (node identity, discovery HA, replication).
/// </summary>
internal static class Identify
{
    // A server with no secret accepts any non-empty secret; one that
    // requires a real secret correctly rejects this placeholder.
    private static readonly byte[] NoSecretPlaceholder = { 0 };

    // Bound on dial + handshake, matching the Go and Java SDKs. Without
    // it, a node whose IP has been reclaimed (a stopped container, a dead
    // cloud instance) blackholes the TCP connect and a caller hangs for
    // the kernel's own timeout — minutes — instead of failing over.
    // Internal and mutable only so tests can shorten it.
    internal static TimeSpan ConnectDeadline = TimeSpan.FromSeconds(5);

    internal abstract record Result;

    // `Tagged` (echoed response tags): the node accepted the extended `A ... T`, so
    // this stream's `G`/`S`/`D` traffic must carry tags and its responses
    // echo them; false means an older node answered the plain-`A` fallback.
    internal sealed record NodeTarget(Stream Stream, bool Tagged) : Result;

    internal sealed record ClusterTarget(IReadOnlyList<DiscoveredNode> Nodes, int Replication) : Result;

    /// <summary>
    /// Always dials with the extended <c>A ... T</c> first (echoed response tags),
    /// so a 0019+ server tags this connection from the start. Only when the
    /// extended form itself signals a pre-0019 server — the connection
    /// closed/EOF/reset before any reply arrived, never a timeout or a
    /// malformed-but-present reply — does this transparently redial once
    /// with the plain <c>A</c> form and run the connection untagged, the
    /// same as before echoed response tags existed.
    /// </summary>
    internal static async Task<Result> ConnectAndIdentifyAsync(
        string host, int port, byte[]? authSecret, SslClientAuthenticationOptions? tls)
    {
        try
        {
            return await ConnectAndIdentifyWithDeadlineAsync(host, port, authSecret, tls, requestTags: true)
                .ConfigureAwait(false);
        }
        catch (Exception error) when (IsLegacyServerSignal(error))
        {
            return await ConnectAndIdentifyWithDeadlineAsync(host, port, authSecret, tls, requestTags: false)
                .ConfigureAwait(false);
        }
    }

    /// <summary>Whether an identify failure looks like a pre-0019 server
    /// slamming the door on the extended <c>A</c> (close/reset before any
    /// reply) — the only failures worth retrying with the plain form. A
    /// timeout is not one: the server kept the connection open, it just
    /// didn't answer (and is handled by its own catch in
    /// <see cref="ConnectAndIdentifyWithDeadlineAsync"/>, never reaching
    /// here). A malformed-but-present reply is not one either — that's a
    /// real protocol error, not a legacy server's silence.</summary>
    private static bool IsLegacyServerSignal(Exception error) =>
        error is EndOfStreamException
        || IsConnectionResetOrAborted(error as SocketException)
        || IsConnectionResetOrAborted((error as IOException)?.InnerException as SocketException);

    private static bool IsConnectionResetOrAborted(SocketException? error) =>
        error is not null
        && error.SocketErrorCode is SocketError.ConnectionReset or SocketError.ConnectionAborted;

    private static async Task<Result> ConnectAndIdentifyWithDeadlineAsync(
        string host, int port, byte[]? authSecret, SslClientAuthenticationOptions? tls, bool requestTags)
    {
        using var deadline = new CancellationTokenSource(ConnectDeadline);
        try
        {
            return await ConnectAndIdentifyAsync(host, port, authSecret, tls, requestTags, deadline.Token)
                .ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (deadline.IsCancellationRequested)
        {
            throw new ConnectionLostException(
                $"nanocached: connecting to {host}:{port} timed out after {ConnectDeadline.TotalSeconds}s");
        }
    }

    private static async Task<Result> ConnectAndIdentifyAsync(
        string host, int port, byte[]? authSecret, SslClientAuthenticationOptions? tls, bool requestTags,
        CancellationToken cancel)
    {
        Stream stream = await OpenAsync(host, port, tls, cancel).ConfigureAwait(false);
        try
        {
            byte[] secret = authSecret ?? NoSecretPlaceholder;
            byte[] header = Encoding.ASCII.GetBytes($"A {secret.Length}{(requestTags ? " T" : "")}\n");
            await stream.WriteAsync(header, cancel).ConfigureAwait(false);
            await stream.WriteAsync(secret, cancel).ConfigureAwait(false);
            await stream.FlushAsync(cancel).ConfigureAwait(false);

            // Read the reply's first 3 bytes: a pre-0019 shape ends the
            // reply right there (`On\n`/`En\n`/`Od\n`/`Ed\n`, byte[2] ==
            // '\n', untagged); echoed response tags stretches it to 4 bytes
            // with a `T` before the final '\n' when the server echoes the
            // tag capability our extended `A` asked for.
            var ack = new byte[3];
            await stream.ReadExactlyAsync(ack, cancel).ConfigureAwait(false);
            bool shapedPrefix = ack[0] is (byte)'O' or (byte)'E' && ack[1] is (byte)'n' or (byte)'d';
            if (!shapedPrefix || (ack[2] != (byte)'\n' && ack[2] != (byte)'T'))
            {
                throw new NanocachedException("nanocached: unexpected response to A");
            }

            bool tagged;
            if (ack[2] == (byte)'\n')
            {
                tagged = false;
            }
            else
            {
                var trailer = new byte[1];
                await stream.ReadExactlyAsync(trailer, cancel).ConfigureAwait(false);
                if (trailer[0] != (byte)'\n')
                {
                    throw new NanocachedException("nanocached: unexpected response to A");
                }
                tagged = true;
            }

            if (ack[0] == (byte)'E')
            {
                throw authSecret is null
                    ? new AuthenticationFailedException(
                        $"nanocached: {host}:{port} requires authentication, but no AuthSecret was given")
                    : new AuthenticationFailedException("nanocached: authentication failed");
            }

            if (ack[1] == (byte)'n')
            {
                return new NodeTarget(stream, tagged);
            }

            // A discovery server: one-shot L, then this connection is done.
            // Tags carry no meaning for discovery (echoed response tags) — L is a
            // single request/response with nothing to desync — but the
            // `OdT\n` ack still had to be parsed above since the client
            // sends the extended `A` before knowing which kind of server
            // answered.
            await stream.WriteAsync("L\n"u8.ToArray(), cancel).ConfigureAwait(false);
            await stream.FlushAsync(cancel).ConfigureAwait(false);
            ClusterTarget cluster = await ReadNodeListAsync(stream, cancel).ConfigureAwait(false);
            stream.Dispose();
            return cluster;
        }
        catch
        {
            stream.Dispose();
            throw;
        }
    }

    /// <summary>Every read this SDK does past this point — this method's
    /// own identify-handshake reads (the <c>A</c> ack, a discovery
    /// server's node list) and, once a node's stream lives on as a
    /// <see cref="Connection"/>, every header byte <see cref="Connection"/>
    /// reads too — goes one byte at a time via <c>ReadExactlyAsync</c> on
    /// a 1-byte buffer (audit finding: unbuffered per-byte reads).
    /// Wrapping the stream in a <see cref="ReadBufferedStream"/> here, the
    /// single place a connection's stream is created, turns what would
    /// otherwise be a syscall per byte into one read filling the buffer,
    /// which then serves many subsequent 1-byte reads — see that type's
    /// own doc comment for why it, and not the BCL's own
    /// <see cref="BufferedStream"/>, is used: this stream is duplex (one
    /// concurrent reader, one concurrent writer — <see cref="Connection"/>'s
    /// dedicated reader loop runs for the connection's whole lifetime
    /// while requests are written concurrently), which
    /// <see cref="BufferedStream"/>'s shared read/write buffer state does
    /// not support safely. Safe under TLS: <see cref="SslStream"/> already
    /// only ever exposes decrypted application data through Read/Write, so
    /// buffering on top of it behaves exactly like buffering on top of a
    /// plain <see cref="NetworkStream"/>.</summary>
    private static async Task<Stream> OpenAsync(
        string host, int port, SslClientAuthenticationOptions? tls, CancellationToken cancel)
    {
        var socket = new Socket(SocketType.Stream, ProtocolType.Tcp) { NoDelay = true };
        NetworkStream? network = null;
        SslStream? ssl = null;
        try
        {
            await socket.ConnectAsync(host, port, cancel).ConfigureAwait(false);
            network = new NetworkStream(socket, ownsSocket: true);
            if (tls is null)
            {
                return new ReadBufferedStream(network);
            }

            var options = tls;
            if (options.TargetHost is null)
            {
                options = Clone(options);
                options.TargetHost = host;
            }
            ssl = new SslStream(network);
            await ssl.AuthenticateAsClientAsync(options, cancel).ConfigureAwait(false);
            return new ReadBufferedStream(ssl);
        }
        catch
        {
            // Dispose only the outermost wrapper that actually got built —
            // SslStream.Dispose() cascades into its NetworkStream (it
            // doesn't own leaveInnerStreamOpen), which cascades into the
            // socket (ownsSocket: true), so this never double-disposes.
            // Without this, a failed handshake left the SslStream and
            // NetworkStream around forever: only socket.Dispose() ran, so
            // repeated failed handshakes accumulated undisposed streams.
            // Mirrors Java's Identify.open, whose autoClose SSLSocket ties
            // the same cleanup to closing the underlying plain socket.
            if (ssl is not null) ssl.Dispose();
            else if (network is not null) network.Dispose();
            else socket.Dispose();
            throw;
        }
    }

    // Copies every settable field the caller could have populated, not
    // just the handful this SDK happens to exercise today — otherwise an
    // Options value that sets, say, ClientCertificateContext or
    // CipherSuitesPolicy would silently lose it here, since this clone
    // only exists to patch in TargetHost per-connect.
    private static SslClientAuthenticationOptions Clone(SslClientAuthenticationOptions options) =>
        new()
        {
            TargetHost = options.TargetHost,
            ClientCertificates = options.ClientCertificates,
            ClientCertificateContext = options.ClientCertificateContext,
            RemoteCertificateValidationCallback = options.RemoteCertificateValidationCallback,
            LocalCertificateSelectionCallback = options.LocalCertificateSelectionCallback,
            CertificateRevocationCheckMode = options.CertificateRevocationCheckMode,
            EnabledSslProtocols = options.EnabledSslProtocols,
            ApplicationProtocols = options.ApplicationProtocols,
            AllowRenegotiation = options.AllowRenegotiation,
            CipherSuitesPolicy = options.CipherSuitesPolicy,
            EncryptionPolicy = options.EncryptionPolicy,
            CertificateChainPolicy = options.CertificateChainPolicy,
        };

    // Bounds a discovery `N <count> <r>` response before allocation,
    // mirroring MaxValueLength on the `V` path: a malicious or MITM'd
    // discovery server must not be able to make the client pre-allocate
    // arbitrary memory from an unverified length prefix (`N 2000000000 3`
    // would otherwise ask for a multi-GB list). 65536 mirrors the cap the
    // Go and Rust SDKs already enforce.
    private const int MaxNodeCount = 1 << 16;

    // A within-cap node count can still carry an absurd per-entry
    // name/address length, so this bounds the whole response's on-wire
    // size too: comfortably fits a full MaxNodeCount-node registry of
    // ordinary lengths while still bounding a malicious discovery
    // server's memory pressure on this client. The same constant is
    // being added to all six SDKs.
    private const long MaxNodeListResponseBytes = 16 * 1024 * 1024;

    // The `N <count> <r>` header and each per-node `<nameLen> <addrLen>`
    // line are always a handful of bytes in the real protocol — this
    // bounds ReadLineAsync itself, before MaxNodeCount/
    // MaxNodeListResponseBytes ever get a chance to run: without it, a
    // malicious or MITM'd discovery server that streams bytes with no
    // '\n' would grow the line buffer without bound.
    private const int MaxHeaderLineLength = 1024;

    private static async Task<ClusterTarget> ReadNodeListAsync(Stream stream, CancellationToken cancel)
    {
        string header = await ReadLineAsync(stream, cancel).ConfigureAwait(false);

        if (header.StartsWith('B'))
        {
            throw new DiscoveryBusyException();
        }
        if (!header.StartsWith("N ", StringComparison.Ordinal))
        {
            throw new NanocachedException(
                $"nanocached: unexpected response from discovery server: {header}");
        }

        // `N <count> <r>\n` (client-side replication) — the replication factor rides along.
        string[] fields = header[2..].Split(' ');
        if (fields.Length != 2
            || !int.TryParse(fields[0], out int count)
            || !int.TryParse(fields[1], out int replication))
        {
            throw new NanocachedException("nanocached: invalid node-list header in discovery response");
        }
        if (count < 0 || count > MaxNodeCount)
        {
            throw new NanocachedException("nanocached: invalid node count in discovery response");
        }
        if (replication < 1)
        {
            throw new NanocachedException("nanocached: invalid replication factor in discovery response");
        }

        // count is now bounded by MaxNodeCount above, so sizing the list's
        // capacity from it is safe — never trust an unvalidated wire value
        // for a pre-allocation, validated or not.
        var nodes = new List<DiscoveredNode>(count);
        long totalBytes = 0;
        for (int i = 0; i < count; i++)
        {
            string[] lengths = (await ReadLineAsync(stream, cancel).ConfigureAwait(false)).Split(' ');
            if (lengths.Length != 2
                || !int.TryParse(lengths[0], out int nameLength)
                || !int.TryParse(lengths[1], out int addrLength)
                || nameLength < 0
                || addrLength < 0)
            {
                throw new NanocachedException("nanocached: invalid node entry header in discovery response");
            }

            // Checked before allocating: a single absurd entry (or many
            // ordinary ones) must not be able to exhaust memory even
            // though count itself is within MaxNodeCount.
            totalBytes += (long)nameLength + addrLength + 1;
            if (totalBytes > MaxNodeListResponseBytes)
            {
                throw new NanocachedException(
                    $"nanocached: discovery node-list response exceeds {MaxNodeListResponseBytes} bytes");
            }

            var body = new byte[nameLength + addrLength + 1]; // +1: trailing '\n'
            await stream.ReadExactlyAsync(body, cancel).ConfigureAwait(false);
            if (body[^1] != (byte)'\n')
            {
                throw new NanocachedException("nanocached: malformed node entry in discovery response");
            }
            nodes.Add(new DiscoveredNode(
                Encoding.UTF8.GetString(body, 0, nameLength),
                Encoding.UTF8.GetString(body, nameLength, addrLength)));
        }

        return new ClusterTarget(nodes, replication);
    }

    private static async Task<string> ReadLineAsync(Stream stream, CancellationToken cancel)
    {
        var line = new StringBuilder();
        var single = new byte[1];
        while (true)
        {
            await stream.ReadExactlyAsync(single, cancel).ConfigureAwait(false);
            if (single[0] == (byte)'\n') return line.ToString();
            if (line.Length >= MaxHeaderLineLength)
            {
                throw new NanocachedException("nanocached: header line too long in discovery response");
            }
            line.Append((char)single[0]);
        }
    }

    internal static (string Host, int Port) SplitHostPort(string address)
    {
        // The range check matters: an out-of-range port would otherwise
        // surface later as a raw ArgumentOutOfRangeException from the
        // socket layer instead of a NanocachedException.
        int separator = address.LastIndexOf(':');
        if (separator == -1
            || !int.TryParse(address[(separator + 1)..], out int port)
            || port is < 0 or > 65535)
        {
            throw new NanocachedException(
                $"nanocached: invalid node address from discovery server: {address}");
        }
        return (address[..separator], port);
    }
}
