using System.Collections.Concurrent;
using System.Net;
using System.Net.Sockets;
using System.Text;

namespace Nanocached.Tests;

/// <summary>
/// In-process stand-ins for nanocached-node and nanocached-discovery,
/// speaking just enough of the wire protocol for client tests to run over
/// real TCP without the Rust binaries. Mirrors the other SDKs' mocks.
/// </summary>
public sealed class MockNode : IDisposable
{
    public ConcurrentDictionary<string, byte[]> Store { get; } = new();
    public int ConnectionCount => _connectionCount;
    public int GetCount => _getCount;

    /// <summary>The TTL (whole seconds; 0 if omitted on the wire) from
    /// the most recent S request this server received.</summary>
    public long LastSetTtl => _lastSetTtl;

    private readonly TcpListener _listener;
    private readonly ConcurrentDictionary<TcpClient, bool> _clients = new();
    private readonly byte[]? _requiredSecret;
    /// <summary>Speak ADR-0019: acknowledge `A ... T` with `OnT\n` and echo
    /// tags on that connection's replies. Off by default so the bulk of
    /// the suite keeps exercising the legacy untagged path.</summary>
    private readonly bool _supportTags;
    /// <summary>Behave like a pre-ADR-0019 server: an extended `A ... T`
    /// is a parse error — close the connection without replying.</summary>
    private readonly bool _closeOnExtendedAuth;
    private int _connectionCount;
    private int _getCount;
    private int _wrongNodeReplies;
    private int _wrongTagReplies;
    private int _swallowedGets;
    private int _malformedValueReplies;
    private int _storedToGetReplies;
    private int _wrongNodeOnSetReplies;
    private volatile int _setDelayMillis;
    private long _lastSetTtl;

    public MockNode(string? requiredSecret = null, bool supportTags = false, bool closeOnExtendedAuth = false)
    {
        _requiredSecret = requiredSecret is null ? null : Encoding.UTF8.GetBytes(requiredSecret);
        _supportTags = supportTags;
        _closeOnExtendedAuth = closeOnExtendedAuth;
        _listener = new TcpListener(IPAddress.Loopback, 0);
        _listener.Start();
        _ = AcceptLoopAsync();
    }

    public int Port => ((IPEndPoint)_listener.LocalEndpoint).Port;

    public string Address => $"127.0.0.1:{Port}";

    public void AnswerWrongNodeOnce() => Interlocked.Increment(ref _wrongNodeReplies);

    /// <summary>Queue a one-off reply for the next G request on a tagged
    /// connection that echoes the WRONG tag (the request's tag + 1) — the
    /// desync a pre-ADR-0019 stream misalignment would produce.</summary>
    public void AnswerWrongTagOnce() => Interlocked.Increment(ref _wrongTagReplies);

    /// <summary>Swallow the next G request entirely (no reply) — the
    /// off-by-one stream desync where every later response answers the
    /// previous request.</summary>
    public void SwallowGetOnce() => Interlocked.Increment(ref _swallowedGets);

    /// <summary>Queue a one-off garbage V header for the next G request.</summary>
    public void AnswerMalformedValueOnce() => Interlocked.Increment(ref _malformedValueReplies);

    /// <summary>Reply <c>S</c> to the next G — a well-formed frame of the
    /// wrong kind, as a desynced (off-by-one) stream would produce.</summary>
    public void AnswerStoredToGetOnce() => Interlocked.Increment(ref _storedToGetReplies);

    /// <summary>Reply <c>W</c> to the next S specifically (not G/D) — for
    /// tests that need a node to keep answering GET normally while a
    /// later SET against it (e.g. a read-repair write) fails.</summary>
    public void AnswerWrongNodeOnSetOnce() => Interlocked.Increment(ref _wrongNodeOnSetReplies);

    /// <summary>Holds every future S reply for <paramref name="millis"/>
    /// first — for tests proving a caller isn't blocked on a slow replica
    /// leg (doc/adr/0014-*.md).</summary>
    public void DelaySets(int millis) => _setDelayMillis = millis;

    /// <summary>Server-side FIN on every open connection, like the idle timeout.</summary>
    public void DropConnections()
    {
        foreach (TcpClient client in _clients.Keys) client.Close();
    }

    public void Dispose()
    {
        DropConnections();
        _listener.Stop();
    }

    internal static string KeyOf(byte[] key) => Convert.ToBase64String(key);

    private async Task AcceptLoopAsync()
    {
        try
        {
            while (true)
            {
                TcpClient client = await _listener.AcceptTcpClientAsync();
                Interlocked.Increment(ref _connectionCount);
                _clients[client] = true;
                _ = ServeAsync(client);
            }
        }
        catch (Exception)
        {
            // Listener stopped — normal shutdown.
        }
    }

    private async Task ServeAsync(TcpClient client)
    {
        try
        {
            NetworkStream stream = client.GetStream();
            // ADR-0019: set when this connection's `A ... T` was
            // acknowledged — its requests then carry a trailing tag the
            // replies must echo.
            bool tagged = false;
            while (true)
            {
                string[] parts = (await Wire.ReadLineAsync(stream)).Split(' ');
                // On a tagged connection every request's last header field
                // is its tag, echoed back as each reply's own last field.
                string tag = tagged ? $" {parts[^1]}" : "";

                switch (parts[0])
                {
                    case "A":
                    {
                        if (parts.Length > 2 && _closeOnExtendedAuth)
                        {
                            client.Close();
                            return;
                        }

                        byte[] secret = await Wire.ReadExactlyAsync(stream, int.Parse(parts[1]));
                        bool accepted = _requiredSecret is null
                            ? secret.Length > 0
                            : secret.AsSpan().SequenceEqual(_requiredSecret);
                        tagged = accepted && _supportTags && parts.Length > 2 && parts[2] == "T";
                        await Wire.WriteAsync(stream, accepted ? (tagged ? "OnT\n" : "On\n") : "En\n");
                        if (!accepted) return;
                        break;
                    }
                    case "G":
                    {
                        byte[] key = await Wire.ReadExactlyAsync(stream, int.Parse(parts[1]));
                        Interlocked.Increment(ref _getCount);
                        if (TakeOne(ref _swallowedGets))
                        {
                            break;
                        }
                        if (tagged && TakeOne(ref _wrongTagReplies))
                        {
                            await Wire.WriteAsync(stream, $"N {int.Parse(parts[^1]) + 1}\n");
                            break;
                        }
                        if (TakeMalformedValue())
                        {
                            await Wire.WriteAsync(stream, "V x\n");
                            break;
                        }
                        if (TakeOne(ref _storedToGetReplies))
                        {
                            await Wire.WriteAsync(stream, $"S{tag}\n");
                            break;
                        }
                        if (TakeWrongNode())
                        {
                            await Wire.WriteAsync(stream, $"W{tag}\n");
                        }
                        else if (Store.TryGetValue(KeyOf(key), out byte[]? value))
                        {
                            await Wire.WriteAsync(stream, $"V {value.Length}{tag}\n");
                            await stream.WriteAsync(value);
                        }
                        else
                        {
                            await Wire.WriteAsync(stream, $"N{tag}\n");
                        }
                        break;
                    }
                    case "S":
                    {
                        byte[] key = await Wire.ReadExactlyAsync(stream, int.Parse(parts[1]));
                        byte[] value = await Wire.ReadExactlyAsync(stream, int.Parse(parts[2]));
                        // The TTL, when present, is the field after the
                        // two lengths (omitted on the wire means "no
                        // expiry", i.e. 0); on a tagged connection the tag
                        // sits after it as the last field.
                        int ttlFieldCount = parts.Length - (tagged ? 4 : 3);
                        _lastSetTtl = ttlFieldCount > 0 ? long.Parse(parts[3]) : 0;
                        if (_setDelayMillis > 0)
                        {
                            await Task.Delay(_setDelayMillis);
                        }
                        if (TakeOne(ref _wrongNodeOnSetReplies) || TakeWrongNode())
                        {
                            await Wire.WriteAsync(stream, $"W{tag}\n");
                        }
                        else
                        {
                            Store[KeyOf(key)] = value;
                            await Wire.WriteAsync(stream, $"S{tag}\n");
                        }
                        break;
                    }
                    case "D":
                    {
                        byte[] key = await Wire.ReadExactlyAsync(stream, int.Parse(parts[1]));
                        if (TakeWrongNode())
                        {
                            await Wire.WriteAsync(stream, $"W{tag}\n");
                        }
                        else
                        {
                            await Wire.WriteAsync(stream, Store.TryRemove(KeyOf(key), out _) ? $"D{tag}\n" : $"N{tag}\n");
                        }
                        break;
                    }
                    default:
                        return;
                }
            }
        }
        catch (Exception)
        {
            // Connection closed — normal end of a mock session.
        }
        finally
        {
            _clients.TryRemove(client, out _);
            client.Close();
        }
    }

    private static bool TakeOne(ref int counter)
    {
        while (true)
        {
            int pending = counter;
            if (pending == 0) return false;
            if (Interlocked.CompareExchange(ref counter, pending - 1, pending) == pending)
            {
                return true;
            }
        }
    }

    private bool TakeMalformedValue()
    {
        while (true)
        {
            int pending = _malformedValueReplies;
            if (pending == 0) return false;
            if (Interlocked.CompareExchange(ref _malformedValueReplies, pending - 1, pending) == pending)
            {
                return true;
            }
        }
    }

    private bool TakeWrongNode()
    {
        while (true)
        {
            int pending = _wrongNodeReplies;
            if (pending == 0) return false;
            if (Interlocked.CompareExchange(ref _wrongNodeReplies, pending - 1, pending) == pending)
            {
                return true;
            }
        }
    }
}

public sealed class MockDiscovery : IDisposable
{
    public volatile bool WarmingUp;

    private readonly TcpListener _listener;
    private readonly int _replication;
    private volatile IReadOnlyList<(string Name, string Address)> _nodes;

    public MockDiscovery(IReadOnlyList<(string Name, string Address)> nodes, int replication = 1)
    {
        _nodes = nodes;
        _replication = replication;
        _listener = new TcpListener(IPAddress.Loopback, 0);
        _listener.Start();
        _ = AcceptLoopAsync();
    }

    public int Port => ((IPEndPoint)_listener.LocalEndpoint).Port;

    public void SetNodes(IReadOnlyList<(string Name, string Address)> nodes) => _nodes = nodes;

    /// <summary>When set, an L request gets this exact text instead of
    /// the normally generated frame — for tests that need to claim
    /// things about the node list a real registry couldn't (an
    /// over-the-cap count, a malformed header, an entry whose declared
    /// length would blow the aggregate response cap) without actually
    /// having to hold that much node data in memory.</summary>
    public string? RawListResponse { get; set; }

    public void Dispose() => _listener.Stop();

    private async Task AcceptLoopAsync()
    {
        try
        {
            while (true)
            {
                TcpClient client = await _listener.AcceptTcpClientAsync();
                _ = ServeAsync(client);
            }
        }
        catch (Exception)
        {
            // Listener stopped — normal shutdown.
        }
    }

    private async Task ServeAsync(TcpClient client)
    {
        try
        {
            NetworkStream stream = client.GetStream();
            while (true)
            {
                string[] parts = (await Wire.ReadLineAsync(stream)).Split(' ');
                if (parts[0] == "A")
                {
                    await Wire.ReadExactlyAsync(stream, int.Parse(parts[1]));
                    await Wire.WriteAsync(stream, "Od\n");
                }
                else if (parts[0] == "L")
                {
                    if (WarmingUp)
                    {
                        await Wire.WriteAsync(stream, "B\n");
                        return;
                    }
                    if (RawListResponse is not null)
                    {
                        await Wire.WriteAsync(stream, RawListResponse);
                        return;
                    }
                    IReadOnlyList<(string Name, string Address)> snapshot = _nodes;
                    var frame = new StringBuilder($"N {snapshot.Count} {_replication}\n");
                    foreach (var (name, address) in snapshot)
                    {
                        frame.Append($"{name.Length} {address.Length}\n{name}{address}\n");
                    }
                    await Wire.WriteAsync(stream, frame.ToString());
                }
                else
                {
                    return;
                }
            }
        }
        catch (Exception)
        {
            // Connection closed — normal end of a mock session.
        }
        finally
        {
            client.Close();
        }
    }
}

internal static class Wire
{
    internal static async Task<string> ReadLineAsync(Stream stream)
    {
        var line = new StringBuilder();
        var single = new byte[1];
        while (true)
        {
            await stream.ReadExactlyAsync(single);
            if (single[0] == (byte)'\n') return line.ToString();
            line.Append((char)single[0]);
        }
    }

    internal static async Task<byte[]> ReadExactlyAsync(Stream stream, int length)
    {
        var data = new byte[length];
        await stream.ReadExactlyAsync(data);
        return data;
    }

    internal static Task WriteAsync(Stream stream, string text) =>
        stream.WriteAsync(Encoding.ASCII.GetBytes(text)).AsTask();

    internal static int UnusedPort()
    {
        var listener = new TcpListener(IPAddress.Loopback, 0);
        listener.Start();
        int port = ((IPEndPoint)listener.LocalEndpoint).Port;
        listener.Stop();
        return port;
    }
}
