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

    private readonly TcpListener _listener;
    private readonly ConcurrentDictionary<TcpClient, bool> _clients = new();
    private readonly byte[]? _requiredSecret;
    private int _connectionCount;
    private int _getCount;
    private int _wrongNodeReplies;
    private int _malformedValueReplies;
    private int _storedToGetReplies;

    public MockNode(string? requiredSecret = null)
    {
        _requiredSecret = requiredSecret is null ? null : Encoding.UTF8.GetBytes(requiredSecret);
        _listener = new TcpListener(IPAddress.Loopback, 0);
        _listener.Start();
        _ = AcceptLoopAsync();
    }

    public int Port => ((IPEndPoint)_listener.LocalEndpoint).Port;

    public string Address => $"127.0.0.1:{Port}";

    public void AnswerWrongNodeOnce() => Interlocked.Increment(ref _wrongNodeReplies);

    /// <summary>Queue a one-off garbage V header for the next G request.</summary>
    public void AnswerMalformedValueOnce() => Interlocked.Increment(ref _malformedValueReplies);

    /// <summary>Reply <c>S</c> to the next G — a well-formed frame of the
    /// wrong kind, as a desynced (off-by-one) stream would produce.</summary>
    public void AnswerStoredToGetOnce() => Interlocked.Increment(ref _storedToGetReplies);

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
            while (true)
            {
                string[] parts = (await Wire.ReadLineAsync(stream)).Split(' ');
                switch (parts[0])
                {
                    case "A":
                    {
                        byte[] secret = await Wire.ReadExactlyAsync(stream, int.Parse(parts[1]));
                        bool accepted = _requiredSecret is null
                            ? secret.Length > 0
                            : secret.AsSpan().SequenceEqual(_requiredSecret);
                        await Wire.WriteAsync(stream, accepted ? "On\n" : "En\n");
                        if (!accepted) return;
                        break;
                    }
                    case "G":
                    {
                        byte[] key = await Wire.ReadExactlyAsync(stream, int.Parse(parts[1]));
                        Interlocked.Increment(ref _getCount);
                        if (TakeMalformedValue())
                        {
                            await Wire.WriteAsync(stream, "V x\n");
                            break;
                        }
                        if (TakeOne(ref _storedToGetReplies))
                        {
                            await Wire.WriteAsync(stream, "S\n");
                            break;
                        }
                        if (TakeWrongNode())
                        {
                            await Wire.WriteAsync(stream, "W\n");
                        }
                        else if (Store.TryGetValue(KeyOf(key), out byte[]? value))
                        {
                            await Wire.WriteAsync(stream, $"V {value.Length}\n");
                            await stream.WriteAsync(value);
                        }
                        else
                        {
                            await Wire.WriteAsync(stream, "N\n");
                        }
                        break;
                    }
                    case "S":
                    {
                        byte[] key = await Wire.ReadExactlyAsync(stream, int.Parse(parts[1]));
                        byte[] value = await Wire.ReadExactlyAsync(stream, int.Parse(parts[2]));
                        if (TakeWrongNode())
                        {
                            await Wire.WriteAsync(stream, "W\n");
                        }
                        else
                        {
                            Store[KeyOf(key)] = value;
                            await Wire.WriteAsync(stream, "S\n");
                        }
                        break;
                    }
                    case "D":
                    {
                        byte[] key = await Wire.ReadExactlyAsync(stream, int.Parse(parts[1]));
                        if (TakeWrongNode())
                        {
                            await Wire.WriteAsync(stream, "W\n");
                        }
                        else
                        {
                            await Wire.WriteAsync(stream, Store.TryRemove(KeyOf(key), out _) ? "D\n" : "N\n");
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
