using System.Net.Security;
using System.Net.Sockets;
using System.Text;

namespace Nanocached;

/// <summary>
/// A node's hash-ring identity (a random per-process UUID) and its network
/// address (<c>host:port</c>) — two different things since
/// doc/adr/0009-*.md: <see cref="Name"/> is what routing hashes;
/// <see cref="Address"/> is only for opening a connection.
/// </summary>
public sealed record DiscoveredNode(string Name, string Address);

/// <summary>
/// Connect-and-identify: dials <c>host:port</c>, authenticates, and
/// figures out from the server's own <c>A</c> response whether it reached
/// a cache node (<c>On</c>) or a discovery server (<c>Od</c>) — the caller
/// never says which it expects (doc/adr/0007-*.md). A node's stream is
/// handed back live; a discovery connection is used once for <c>L</c> and
/// disposed, returning the name/address list and the cluster's replication
/// factor R (doc/adr/0009, 0010, 0011).
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
    internal static TimeSpan ConnectDeadline = TimeSpan.FromSeconds(10);

    internal abstract record Result;

    internal sealed record NodeTarget(Stream Stream) : Result;

    internal sealed record ClusterTarget(IReadOnlyList<DiscoveredNode> Nodes, int Replication) : Result;

    internal static async Task<Result> ConnectAndIdentifyAsync(
        string host, int port, byte[]? authSecret, SslClientAuthenticationOptions? tls)
    {
        using var deadline = new CancellationTokenSource(ConnectDeadline);
        try
        {
            return await ConnectAndIdentifyAsync(host, port, authSecret, tls, deadline.Token)
                .ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (deadline.IsCancellationRequested)
        {
            throw new ConnectionLostException(
                $"nanocached: connecting to {host}:{port} timed out after {ConnectDeadline.TotalSeconds}s");
        }
    }

    private static async Task<Result> ConnectAndIdentifyAsync(
        string host, int port, byte[]? authSecret, SslClientAuthenticationOptions? tls,
        CancellationToken cancel)
    {
        Stream stream = await OpenAsync(host, port, tls, cancel).ConfigureAwait(false);
        try
        {
            byte[] secret = authSecret ?? NoSecretPlaceholder;
            byte[] header = Encoding.ASCII.GetBytes($"A {secret.Length}\n");
            await stream.WriteAsync(header, cancel).ConfigureAwait(false);
            await stream.WriteAsync(secret, cancel).ConfigureAwait(false);
            await stream.FlushAsync(cancel).ConfigureAwait(false);

            var ack = new byte[3];
            await stream.ReadExactlyAsync(ack, cancel).ConfigureAwait(false);
            bool shaped = ack[2] == (byte)'\n'
                && ack[0] is (byte)'O' or (byte)'E'
                && ack[1] is (byte)'n' or (byte)'d';
            if (!shaped)
            {
                throw new NanocachedException("nanocached: unexpected response to A");
            }

            if (ack[0] == (byte)'E')
            {
                throw authSecret is null
                    ? new NanocachedException(
                        $"nanocached: {host}:{port} requires authentication, but no AuthSecret was given")
                    : new NanocachedException("nanocached: authentication failed");
            }

            if (ack[1] == (byte)'n')
            {
                return new NodeTarget(stream);
            }

            // A discovery server: one-shot L, then this connection is done.
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

    private static async Task<Stream> OpenAsync(
        string host, int port, SslClientAuthenticationOptions? tls, CancellationToken cancel)
    {
        var socket = new Socket(SocketType.Stream, ProtocolType.Tcp) { NoDelay = true };
        try
        {
            await socket.ConnectAsync(host, port, cancel).ConfigureAwait(false);
            var network = new NetworkStream(socket, ownsSocket: true);
            if (tls is null)
            {
                return network;
            }

            var options = tls;
            if (options.TargetHost is null)
            {
                options = Clone(options);
                options.TargetHost = host;
            }
            var ssl = new SslStream(network);
            await ssl.AuthenticateAsClientAsync(options, cancel).ConfigureAwait(false);
            return ssl;
        }
        catch
        {
            socket.Dispose();
            throw;
        }
    }

    private static SslClientAuthenticationOptions Clone(SslClientAuthenticationOptions options) =>
        new()
        {
            TargetHost = options.TargetHost,
            ClientCertificates = options.ClientCertificates,
            RemoteCertificateValidationCallback = options.RemoteCertificateValidationCallback,
            CertificateRevocationCheckMode = options.CertificateRevocationCheckMode,
            EnabledSslProtocols = options.EnabledSslProtocols,
        };

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

        // `N <count> <r>\n` (ADR-0011) — the replication factor rides along.
        string[] fields = header[2..].Split(' ');
        if (fields.Length != 2
            || !int.TryParse(fields[0], out int count)
            || !int.TryParse(fields[1], out int replication))
        {
            throw new NanocachedException("nanocached: invalid node-list header in discovery response");
        }
        if (replication < 1)
        {
            throw new NanocachedException("nanocached: invalid replication factor in discovery response");
        }

        var nodes = new List<DiscoveredNode>(count);
        for (int i = 0; i < count; i++)
        {
            string[] lengths = (await ReadLineAsync(stream, cancel).ConfigureAwait(false)).Split(' ');
            if (lengths.Length != 2
                || !int.TryParse(lengths[0], out int nameLength)
                || !int.TryParse(lengths[1], out int addrLength))
            {
                throw new NanocachedException("nanocached: invalid node entry header in discovery response");
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
            line.Append((char)single[0]);
        }
    }

    internal static (string Host, int Port) SplitHostPort(string address)
    {
        int separator = address.LastIndexOf(':');
        if (separator == -1 || !int.TryParse(address[(separator + 1)..], out int port))
        {
            throw new NanocachedException(
                $"nanocached: invalid node address from discovery server: {address}");
        }
        return (address[..separator], port);
    }
}
