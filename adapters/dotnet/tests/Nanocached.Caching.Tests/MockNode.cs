using System.Collections.Concurrent;
using System.Net;
using System.Net.Sockets;
using System.Text;

namespace Nanocached.Caching.Tests;

/// <summary>
/// A minimal in-process nanocached node speaking just the slice of the
/// wire protocol the .NET caching adapter's traffic produces: the
/// <c>A ... T</c> handshake, the namespaced <c>g</c>/<c>s</c>/<c>d</c>/<c>c</c>
/// commands, their legacy default-namespace forms (the SDK's keep-alive
/// probe), and <c>F</c>. Mirrors the structure of
/// adapters/spring/src/test/java/org/nanocached/spring/MockNode.java (a
/// trimmed re-implementation for the same reason that one is: the SDK's own
/// test doubles are internal to the SDK) — not a real store, no TTL expiry,
/// no LRU. Tests only assert what reaches the wire and what comes back.
/// </summary>
internal sealed class MockNode : IDisposable
{
    internal sealed record Entry(byte[] Value, long TtlSeconds);

    private readonly TcpListener _listener;
    private readonly CancellationTokenSource _cts = new();
    private readonly Task _acceptLoop;

    // namespace -> key (both Base64-of-raw-bytes, for value equality) -> entry.
    private readonly ConcurrentDictionary<string, ConcurrentDictionary<string, Entry>> _stores = new();
    private int _clearCount;
    private int _flushCount;
    private int _setCount;

    internal MockNode()
    {
        _listener = new TcpListener(IPAddress.Loopback, 0);
        _listener.Start();
        _acceptLoop = Task.Run(AcceptLoopAsync);
    }

    internal int Port => ((IPEndPoint)_listener.LocalEndpoint).Port;

    /// <summary>How many <c>c</c> (clear-one-namespace) requests this node
    /// has received. Unused by IDistributedCache itself (the SPI has no
    /// Clear), kept for parity with the wire commands this mock
    /// understands.</summary>
    internal int ClearCount => Volatile.Read(ref _clearCount);
    internal int FlushCount => Volatile.Read(ref _flushCount);

    /// <summary>How many <c>s</c>/<c>S</c> (set) requests this node has
    /// received — lets a test prove a re-set actually reached the wire
    /// (e.g. sliding expiration's renew-on-Get), not merely that the
    /// stored value still looks the same.</summary>
    internal int SetCount => Volatile.Read(ref _setCount);

    /// <summary>The entry currently stored for <paramref name="key"/> in
    /// namespace <paramref name="ns"/> (<c>""</c> for the default/legacy
    /// namespace) — what tests assert against to see exactly what reached
    /// the wire, TTL included.</summary>
    internal Entry? EntryFor(string ns, byte[] key) =>
        _stores.TryGetValue(ns, out var store) && store.TryGetValue(EncodeKey(key), out Entry? entry)
            ? entry
            : null;

    private static string EncodeKey(byte[] bytes) => Convert.ToBase64String(bytes);

    private ConcurrentDictionary<string, Entry> Store(string ns) =>
        _stores.GetOrAdd(ns, static _ => new ConcurrentDictionary<string, Entry>());

    public void Dispose()
    {
        _cts.Cancel();
        _listener.Stop();
        _cts.Dispose();
    }

    private async Task AcceptLoopAsync()
    {
        while (!_cts.IsCancellationRequested)
        {
            TcpClient client;
            try
            {
                client = await _listener.AcceptTcpClientAsync(_cts.Token).ConfigureAwait(false);
            }
            catch (Exception) when (_cts.IsCancellationRequested)
            {
                return;
            }
            _ = Task.Run(() => ServeAsync(client));
        }
    }

    private async Task ServeAsync(TcpClient client)
    {
        using TcpClient owned = client;
        NetworkStream stream = owned.GetStream();
        bool tagged = false;
        try
        {
            while (true)
            {
                string[] parts = (await ReadLineAsync(stream).ConfigureAwait(false)).Split(' ');
                string tagSuffix = tagged ? " " + parts[^1] : "";
                switch (parts[0])
                {
                    case "A":
                    {
                        byte[] secret = await ReadExactlyAsync(stream, int.Parse(parts[1])).ConfigureAwait(false);
                        bool accepted = secret.Length > 0;
                        tagged = accepted && parts.Length > 2 && parts[2] == "T";
                        await ReplyAsync(stream, accepted ? (tagged ? "OnT\n" : "On\n") : "En\n")
                            .ConfigureAwait(false);
                        if (!accepted) return;
                        break;
                    }
                    case "G":
                        await GetAsync(stream, "", int.Parse(parts[1]), tagSuffix).ConfigureAwait(false);
                        break;
                    case "g":
                    {
                        byte[] ns = await ReadExactlyAsync(stream, int.Parse(parts[1])).ConfigureAwait(false);
                        await GetAsync(stream, Ns(ns), int.Parse(parts[2]), tagSuffix).ConfigureAwait(false);
                        break;
                    }
                    case "S":
                        await SetAsync(stream, "", parts, 1, tagged, tagSuffix).ConfigureAwait(false);
                        break;
                    case "s":
                    {
                        // Body order is namespace-first, but the length
                        // headers put the namespace length first too, so
                        // read it before delegating for the key/value.
                        byte[] ns = await ReadExactlyAsync(stream, int.Parse(parts[1])).ConfigureAwait(false);
                        await SetAsync(stream, Ns(ns), parts, 2, tagged, tagSuffix).ConfigureAwait(false);
                        break;
                    }
                    case "D":
                        await DeleteAsync(stream, "", int.Parse(parts[1]), tagSuffix).ConfigureAwait(false);
                        break;
                    case "d":
                    {
                        byte[] ns = await ReadExactlyAsync(stream, int.Parse(parts[1])).ConfigureAwait(false);
                        await DeleteAsync(stream, Ns(ns), int.Parse(parts[2]), tagSuffix).ConfigureAwait(false);
                        break;
                    }
                    case "c":
                    {
                        byte[] ns = await ReadExactlyAsync(stream, int.Parse(parts[1])).ConfigureAwait(false);
                        _stores.TryRemove(Ns(ns), out _);
                        Interlocked.Increment(ref _clearCount);
                        await ReplyAsync(stream, "C" + tagSuffix + "\n").ConfigureAwait(false);
                        break;
                    }
                    case "F":
                        _stores.Clear();
                        Interlocked.Increment(ref _flushCount);
                        await ReplyAsync(stream, "C" + tagSuffix + "\n").ConfigureAwait(false);
                        break;
                    default:
                        throw new IOException($"unexpected command {parts[0]}");
                }
            }
        }
        catch (Exception)
        {
            // connection closed by the client (or test teardown)
        }
    }

    private async Task GetAsync(NetworkStream stream, string ns, int keyLength, string tagSuffix)
    {
        byte[] key = await ReadExactlyAsync(stream, keyLength).ConfigureAwait(false);
        Entry? entry = EntryFor(ns, key);
        if (entry is null)
        {
            await ReplyAsync(stream, "N" + tagSuffix + "\n").ConfigureAwait(false);
            return;
        }
        byte[] header = Encoding.ASCII.GetBytes($"V {entry.Value.Length}{tagSuffix}\n");
        await stream.WriteAsync(header).ConfigureAwait(false);
        await stream.WriteAsync(entry.Value).ConfigureAwait(false);
        await stream.FlushAsync().ConfigureAwait(false);
    }

    private async Task SetAsync(
        NetworkStream stream, string ns, string[] parts, int firstLengthIndex, bool tagged, string tagSuffix)
    {
        int keyLength = int.Parse(parts[firstLengthIndex]);
        int valueLength = int.Parse(parts[firstLengthIndex + 1]);
        // Remaining numeric fields: [ttl] in untagged mode, [ttl] tag in
        // tagged mode — the tag is always last.
        int remaining = parts.Length - (firstLengthIndex + 2) - (tagged ? 1 : 0);
        long ttlSeconds = remaining > 0 ? long.Parse(parts[firstLengthIndex + 2]) : 0;
        byte[] key = await ReadExactlyAsync(stream, keyLength).ConfigureAwait(false);
        byte[] value = await ReadExactlyAsync(stream, valueLength).ConfigureAwait(false);
        Store(ns)[EncodeKey(key)] = new Entry(value, ttlSeconds);
        Interlocked.Increment(ref _setCount);
        await ReplyAsync(stream, "S" + tagSuffix + "\n").ConfigureAwait(false);
    }

    private async Task DeleteAsync(NetworkStream stream, string ns, int keyLength, string tagSuffix)
    {
        byte[] key = await ReadExactlyAsync(stream, keyLength).ConfigureAwait(false);
        bool existed = Store(ns).TryRemove(EncodeKey(key), out _);
        await ReplyAsync(stream, (existed ? "D" : "N") + tagSuffix + "\n").ConfigureAwait(false);
    }

    private static string Ns(byte[] namespaceBytes) => Encoding.UTF8.GetString(namespaceBytes);

    private static async Task ReplyAsync(NetworkStream stream, string line)
    {
        await stream.WriteAsync(Encoding.ASCII.GetBytes(line)).ConfigureAwait(false);
        await stream.FlushAsync().ConfigureAwait(false);
    }

    private static async Task<byte[]> ReadExactlyAsync(NetworkStream stream, int length)
    {
        byte[] buffer = new byte[length];
        int offset = 0;
        while (offset < length)
        {
            int read = await stream.ReadAsync(buffer.AsMemory(offset, length - offset)).ConfigureAwait(false);
            if (read == 0) throw new IOException("connection closed");
            offset += read;
        }
        return buffer;
    }

    private static async Task<string> ReadLineAsync(NetworkStream stream)
    {
        var line = new StringBuilder();
        var one = new byte[1];
        while (true)
        {
            int read = await stream.ReadAsync(one.AsMemory(0, 1)).ConfigureAwait(false);
            if (read == 0) throw new IOException("connection closed");
            if (one[0] == (byte)'\n') return line.ToString();
            line.Append((char)one[0]);
        }
    }
}
