using System.Collections.Concurrent;
using System.Diagnostics;
using System.Globalization;
using System.Text;

namespace Nanocached;

/// <summary>
/// One already-identified connection to a single nanocached-node, speaking
/// the cache protocol (<c>G</c>/<c>S</c>/<c>D</c>, their namespaced
/// counterparts <c>g</c>/<c>s</c>/<c>d</c> — issue #105 — the
/// namespace-clear/flush-everything commands <c>c</c>/<c>F</c> — issue
/// #106 — the always-namespaced counter op <c>i</c> — issue #129 — and
/// the always-namespaced compare-and-set ops <c>k</c>/<c>x</c> — issue
/// #141 — the <c>A</c> identify exchange happens in <see cref="Identify"/>
/// before a Connection exists).
/// Requests are pipelined onto the socket and matched to responses in send
/// order (request pipelining): a dedicated read loop, started in the
/// constructor, consumes responses and dispatches each to the oldest
/// still-pending request, since nanocached-node itself only ever answers in
/// the order it received requests. <see cref="Stream"/> supports one
/// concurrent reader and one concurrent writer safely, so the read loop
/// never contends with writers. Enqueuing the pending slot and writing the
/// frame happen under one semaphore, so concurrent callers' queue order
/// always matches the order their frames actually hit the wire.
///
/// <para>echoed response tags: when <see cref="_tagged"/> (negotiated during
/// identify), every request carries a per-connection tag as its header's
/// last field, and the server echoes it in the response. The tag is
/// claimed inside the same enqueue+write critical section as the pending
/// slot itself, so tag order can never skew from queue/wire order, and the
/// read loop verifies the echoed tag against the oldest pending slot's
/// expected tag <em>before</em> that slot is handed its response — closing
/// the desync window an off-by-one stream corruption (e.g. a swallowed
/// response) would otherwise leave open (the caller-side kind check in
/// <see cref="Mismatch"/> only ever notices that after the fact).</para>
///
/// <para>issue #125 — retryable-error status <c>R</c>: any data command
/// (G/S/D/g/s/d/c/F) may be answered <c>R</c> instead of its usual
/// response — the request specifically failed transiently (e.g. a proxy's
/// upstream node was briefly unreachable); the connection itself is fine.
/// <see cref="RequestAsync"/> retries the SAME request transparently, up
/// to twice more on this SAME connection, before ever surfacing anything
/// to the caller — see its own doc comment. <c>R</c> is never a reason to
/// close or redial: it is not a connection error, not a <c>W</c>, not an
/// <c>E</c>.</para>
/// </summary>
internal sealed class Connection
{
    // The server's own request cap is 1 MiB; this constant doubles that
    // as headroom, so a claimed length beyond it is definitely a corrupt
    // or malicious frame, never just a legitimately large value.
    private const int MaxValueLength = 2 * 1024 * 1024;

    // issue #207 (follow-up to #179, fixed for Java in PR #201): each M
    // entry's own declared length is already bounded above by
    // MaxValueLength, but nothing bounded the SUM of entry sizes across an
    // entire multi-get reply — a node answering a 400-key request with 400
    // x 2 MiB hits would still force ~800 MiB of allocation from a single
    // reply. Checked in the M decode loop before each entry's body is
    // read, so an oversized running total is caught before the
    // allocation/read happens, not after. Same 64 MiB figure as
    // Compression's own decompression cap (issue #41), not derived from
    // batch-size x per-value-cap: a wire reply legitimately needing more
    // than that in one round trip is unreasonable — callers should batch
    // smaller. Mutable only so tests can shrink it without moving tens of
    // MB over a loopback socket, mirroring RequestTimeout.
    internal static long MaxMultiGetResponseBytes = 64L * 1024 * 1024;

    // Header/tag lines (the marker line ahead of a V's body, or the whole
    // line for S/D/N/W) are usually just a handful of bytes, but an `M`
    // reply's header carries one length token per requested key on this
    // same single line — a full MaxBatchKeys (400, NanocachedClient.cs)
    // multi-get where every key hits packs 400 tokens, each at worst a
    // decimal byte length up to MaxValueLength's own digit count plus its
    // separating space (len("2097152")+1 = 8 bytes): 400*8 = 3200 bytes,
    // which the old 1024 cap tripped on a perfectly valid reply, poisoning
    // the connection with ConnectionLostException (issue #273). 4096
    // leaves comfortable headroom above that 3200-byte worst case while
    // still bounding a malicious or buggy node that streams bytes with no
    // '\n' from growing ReadLineAsync's StringBuilder without bound,
    // gated only by RequestTimeout rather than failing fast. Matches the
    // Go/Rust/TypeScript/Java SDKs' equivalent constant (Go's
    // maxHeaderLineLength, connection.go:54, derives it the same way).
    private const int MaxHeaderLineLength = 4096;

    /// <summary>Bounds how long the connection may go without progress
    /// while requests are outstanding (issue #42) — each response must
    /// arrive within this window of the previous one (or of its own send,
    /// when the queue was empty): without it, a half-open server that
    /// accepts the TCP connection but never writes back — or stops
    /// mid-stream — would hang Get/Set/Delete forever. No
    /// <see cref="CancellationToken"/> ever reaches the <see cref="Stream"/>
    /// calls (see <see cref="RequestAsync"/>'s doc comment), so the
    /// watchdog instead disposes the stream, which unblocks the read loop
    /// with an IO error the existing poison path already handles.
    /// Generous versus the server's own 10s outbound timeouts, and the
    /// same 30s the Go and Rust SDKs use. Mutable only so tests can
    /// shorten it, mirroring <c>KeepAliveInterval</c>.</summary>
    internal static TimeSpan RequestTimeout = TimeSpan.FromSeconds(30);

    private readonly Stream _stream;
    private readonly SemaphoreSlim _writeGate = new(1, 1);
    /// <summary>echoed response tags: negotiated during identify — when true, every
    /// request carries a tag the server echoes, and the read loop verifies
    /// the echo against the oldest pending slot before resolving it.</summary>
    private readonly bool _tagged;
    /// <summary>issue #125 — retryable-error status <c>R</c>: fired once
    /// per <c>R</c> response this connection sees, whether it was
    /// transparently retried away or ultimately surfaced as
    /// <see cref="RetryableException"/> — the client's hook for
    /// <c>Stats().TransientRetries</c>.</summary>
    private readonly Action? _onTransientRetry;
    // A u32 wrapping counter (echoed response tags), claimed only inside the
    // _writeGate critical section — never touched concurrently, so no
    // Interlocked ceremony is needed here the way _closedFlag needs one.
    private uint _nextTag;
    private readonly ConcurrentQueue<(TaskCompletionSource<(byte Marker, byte[]? Value, long TtlSeconds, List<MultiEntry>? Entries)> Tcs, uint? Tag)> _pending = new();
    private readonly Stopwatch _sinceLastUse = Stopwatch.StartNew();
    private readonly Action? _onClosed;
    // 0 = open, 1 = closed. An int (not a bool) so Close() can gate on it
    // with Interlocked.Exchange: the previous plain "if (_closed) return;
    // _closed = true;" was a non-atomic check-then-set, so two concurrent
    // Close() calls could both pass the check and both run the body,
    // double-firing _onClosed and corrupting the open-target counter it
    // decrements. Exchange makes "am I the first caller to close this"
    // a single atomic step — exactly the guarantee Java's poison() gets
    // from `synchronized`.
    private int _closedFlag;

    // The reason CloseWithReason was closing for, published before the
    // stream is disposed. The read loop wakes from that dispose with a
    // bare ObjectDisposedException and races CloseWithReason's own drain
    // for the oldest pending entry — whoever wins, the caller must see
    // the *reason* (e.g. the issue-#42 request timeout), not the disposed
    // stream's noise.
    private volatile Exception? _closeReason;

    // The progress-based request deadline (issue #42): armed when the
    // outstanding count goes 0→1, re-armed by the read loop each time a
    // response is dispatched with more still outstanding, cleared once
    // nothing is. _deadlineTicks (Environment.TickCount64-based) is the
    // authority a possibly-stale timer callback re-checks against — a
    // Timer.Change can't recall a callback already in flight. 0 = unarmed.
    private readonly Timer _watchdog;
    private long _deadlineTicks;
    private int _outstanding;
    // Serializes arm/clear decisions against each other: without it, the
    // read loop's "count hit 0, clear" could land *after* a concurrent
    // writer's "count hit 1, arm" and disarm the deadline with a request
    // still outstanding. Each decision re-reads _outstanding under this
    // gate, so the last one to run always matches the current count.
    private readonly object _deadlineGate = new();

    /// <summary><paramref name="tagged"/> (echoed response tags): whether identify
    /// negotiated tagged mode for this connection. <paramref name="onClosed"/>,
    /// when given, fires exactly once — the first time this connection
    /// actually closes — no matter how many call sites call
    /// <see cref="Close"/> on it. Lets the client hook every place it
    /// closes or discards a connection (issue #12's forgotten-close
    /// tracking) without each call site worrying about double-counting.</summary>
    internal Connection(Stream stream, bool tagged = false, Action? onClosed = null, Action? onTransientRetry = null)
    {
        _stream = stream;
        _tagged = tagged;
        _onClosed = onClosed;
        _onTransientRetry = onTransientRetry;
        _watchdog = new Timer(OnRequestTimeout, null, Timeout.Infinite, Timeout.Infinite);
        _ = ReadLoopAsync();
    }

    private void ArmDeadline()
    {
        lock (_deadlineGate)
        {
            if (Volatile.Read(ref _outstanding) == 0) return; // cleared concurrently
            TimeSpan timeout = RequestTimeout;
            Volatile.Write(ref _deadlineTicks, Environment.TickCount64 + (long)timeout.TotalMilliseconds);
            try
            {
                _watchdog.Change(timeout, Timeout.InfiniteTimeSpan);
            }
            catch (ObjectDisposedException)
            {
                // CloseWithReason disposed the timer concurrently — the
                // connection is already poisoned, nothing left to bound.
            }
        }
    }

    private void ClearDeadline()
    {
        lock (_deadlineGate)
        {
            if (Volatile.Read(ref _outstanding) != 0) return; // re-armed concurrently
            Volatile.Write(ref _deadlineTicks, 0);
            try
            {
                _watchdog.Change(Timeout.Infinite, Timeout.Infinite);
            }
            catch (ObjectDisposedException)
            {
                // Same benign race as in ArmDeadline.
            }
        }
    }

    private void OnRequestTimeout(object? _)
    {
        // A stale callback — the deadline was cleared or pushed forward
        // after this fired — does nothing; the re-armed timer covers the
        // new deadline.
        long deadline = Volatile.Read(ref _deadlineTicks);
        if (deadline == 0 || Environment.TickCount64 < deadline) return;
        // Poison with the timeout as the drain reason: CloseWithReason
        // disposes the stream, which is what actually unblocks the read
        // loop (and any in-flight write) with an IO error, and rejects the
        // stalled request plus everything pipelined behind it.
        CloseWithReason(new ConnectionLostException(
            $"nanocached: no response from server within {(long)RequestTimeout.TotalMilliseconds}ms "
            + "(request timed out)"));
    }

    internal bool IsClosed => Volatile.Read(ref _closedFlag) != 0;

    internal TimeSpan Idle => _sinceLastUse.Elapsed;

    /// <summary>Idempotent — safe to call concurrently from more than one
    /// caller. Rejects every request still pending with a
    /// connection-closed error — the read loop's own exit path (a failed
    /// read) also routes through here, so this is the single place
    /// draining ever happens.</summary>
    internal void Close() => CloseWithReason(new ConnectionLostException("nanocached: connection closed"));

    /// <summary>Same idempotency as <see cref="Close"/>, but drains every
    /// still-pending request with <paramref name="reason"/> instead of the
    /// generic closed error — used by the echoed response tags tag check below so that
    /// every request behind a desynced one is rejected with a message that
    /// actually says so, not a generic "connection closed".</summary>
    private void CloseWithReason(Exception reason)
    {
        if (Interlocked.Exchange(ref _closedFlag, 1) != 0) return;
        _closeReason = reason;
        Volatile.Write(ref _deadlineTicks, 0);
        _watchdog.Dispose();
        _stream.Dispose();
        _onClosed?.Invoke();
        while (_pending.TryDequeue(out var pending))
        {
            pending.Tcs.TrySetException(reason);
        }
    }

    // issue #105 — first-class namespaces. Never sent on the wire as an
    // explicit zero-length namespace: IsNamespaced treats this the same as
    // "no namespace at all" everywhere below, so a legacy (unnamespaced)
    // call still produces the exact G/S/D frame it always has — see the
    // class doc comment's protocol note and docs/protocol.html's
    // namespaces section ("<namespace-length> of 0 addresses the default
    // namespace ... the same one every G/S/D addresses").
    private static readonly byte[] EmptyNamespace = Array.Empty<byte>();

    private static bool IsNamespaced(byte[] namespaceBytes) => namespaceBytes.Length > 0;

    internal Task<byte[]?> GetAsync(byte[] key) => GetAsync(EmptyNamespace, key);

    /// <summary>issue #105: as the single-argument overload, but scoped to
    /// <paramref name="namespaceBytes"/> — an empty namespace sends the
    /// legacy <c>G</c> frame byte-for-byte; a non-empty one sends the
    /// namespaced <c>g</c> frame, whose header gains a leading
    /// <c>&lt;namespace-length&gt;</c> field and whose body leads with the
    /// namespace bytes ahead of the key.</summary>
    internal async Task<byte[]?> GetAsync(byte[] namespaceBytes, byte[] key)
    {
        var (marker, value, _, _) = await RequestAsync(tag =>
            Frame(GetHeader(namespaceBytes, key.Length, tag), namespaceBytes, key, null))
            .ConfigureAwait(false);
        return marker switch
        {
            (byte)'V' => value,
            (byte)'N' => null,
            (byte)'W' => throw new WrongNodeException(),
            _ => throw Mismatch(marker),
        };
    }

    /// <summary><paramref name="ttlSeconds"/> of 0 means no expiry, mapped
    /// on the wire exactly as the old absent-TTL header was.</summary>
    internal Task SetAsync(byte[] key, byte[] value, long ttlSeconds) =>
        SetAsync(EmptyNamespace, key, value, ttlSeconds);

    /// <summary>issue #105: as the unnamespaced overload, but scoped to
    /// <paramref name="namespaceBytes"/> — see <see cref="GetAsync(byte[], byte[])"/>'s
    /// doc comment for the legacy-vs-namespaced frame rule.</summary>
    internal async Task SetAsync(byte[] namespaceBytes, byte[] key, byte[] value, long ttlSeconds)
    {
        var (marker, _, _, _) = await RequestAsync(tag =>
            Frame(SetHeader(namespaceBytes, key.Length, value.Length, ttlSeconds, tag), namespaceBytes, key, value))
            .ConfigureAwait(false);
        if (marker == (byte)'W') throw new WrongNodeException();
        if (marker != (byte)'S') throw Mismatch(marker);
    }

    internal Task<bool> DeleteAsync(byte[] key) => DeleteAsync(EmptyNamespace, key);

    /// <summary>issue #105: as the unnamespaced overload, but scoped to
    /// <paramref name="namespaceBytes"/> — see <see cref="GetAsync(byte[], byte[])"/>'s
    /// doc comment for the legacy-vs-namespaced frame rule.</summary>
    internal async Task<bool> DeleteAsync(byte[] namespaceBytes, byte[] key)
    {
        var (marker, _, _, _) = await RequestAsync(tag =>
            Frame(DeleteHeader(namespaceBytes, key.Length, tag), namespaceBytes, key, null))
            .ConfigureAwait(false);
        return marker switch
        {
            (byte)'D' => true,
            (byte)'N' => false,
            (byte)'W' => throw new WrongNodeException(),
            _ => throw Mismatch(marker),
        };
    }

    /// <summary>issue #106: drops every entry in one namespace on this
    /// node — <paramref name="namespaceBytes"/> empty clears the default
    /// namespace (<c>c 0\n</c>, not rejected). Unlike
    /// <see cref="GetAsync(byte[], byte[])"/>/Set/Delete, there is no
    /// legacy uppercase counterpart to fall back to for the empty
    /// namespace: <c>c</c>/<c>F</c> are new in #106 and always sent
    /// lowercase, whatever the namespace. Never answered <c>W</c> (not
    /// key-addressed, docs/protocol.html's "c / F" section) — the
    /// fan-out-and-refresh-once-and-retry logic for a node that fails
    /// this lives in <see cref="NanocachedClient"/>, not here; this
    /// method only speaks for the one node this connection is dialed
    /// to.</summary>
    internal async Task ClearAsync(byte[] namespaceBytes)
    {
        var (marker, _, _, _) = await RequestAsync(tag =>
            Frame(ClearHeader(namespaceBytes, tag), namespaceBytes, EmptyNamespace, null))
            .ConfigureAwait(false);
        if (marker != (byte)'C') throw Mismatch(marker);
    }

    /// <summary>issue #106: drops every namespace on this node, the
    /// default one included (<c>F\n</c>). See <see cref="ClearAsync(byte[])"/>'s
    /// doc comment for the shared <c>C</c>-ack/no-<c>W</c> rules.</summary>
    internal async Task ClearAllAsync()
    {
        var (marker, _, _, _) = await RequestAsync(tag =>
            Encoding.ASCII.GetBytes($"F{TagField(tag)}\n"))
            .ConfigureAwait(false);
        if (marker != (byte)'C') throw Mismatch(marker);
    }

    /// <summary>issue #129 — the always-namespaced counter op: increments
    /// (a negative <paramref name="delta"/> decrements — there is no
    /// separate decrement opcode) the counter stored at (<paramref
    /// name="namespaceBytes"/>, <paramref name="key"/>) and returns its new
    /// value plus, when the entry carries a TTL, its remaining seconds —
    /// or <c>null</c> when the key is missing or expired, the same
    /// not-found convention <see cref="GetAsync(byte[], byte[])"/> uses.
    /// Throws <see cref="NotNumericException"/> when the stored value isn't
    /// INCR's counter grammar or applying the delta would overflow a
    /// signed 64-bit integer (<c>T</c>). Unlike <c>G</c>/<c>S</c>/<c>D</c>,
    /// <c>i</c> has no legacy uppercase form — every request always carries
    /// an explicit (possibly zero) namespace length.
    ///
    /// <para>Cluster replication (issue #129's design note): this method
    /// only ever talks to the ONE node this connection is dialed to — it
    /// has no notion of owners or replicas. It is <see cref="NanocachedClient"/>'s
    /// job to call this against the primary owner only, and — on success —
    /// forward the literal resulting value to the replicas via this same
    /// connection's <see cref="SetAsync(byte[], byte[], byte[], long)"/>,
    /// never by sending <c>i</c> to a replica (which could let a replica's
    /// counter drift from the primary's).</para></summary>
    internal async Task<(long Value, long TtlSeconds)?> IncrAsync(byte[] namespaceBytes, byte[] key, long delta)
    {
        var (marker, value, ttlSeconds, _) = await RequestAsync(tag =>
            Frame(IncrHeader(namespaceBytes, key.Length, delta, tag), namespaceBytes, key, null))
            .ConfigureAwait(false);
        switch (marker)
        {
            case (byte)'I':
                if (!TryParseWireCounter(Encoding.ASCII.GetString(value!), out long newValue))
                {
                    throw new ConnectionLostException("nanocached: invalid incr value in response");
                }
                return (newValue, ttlSeconds);
            case (byte)'N':
                return null;
            case (byte)'T':
                throw new NotNumericException();
            case (byte)'W':
                throw new WrongNodeException();
            default:
                throw Mismatch(marker);
        }
    }

    /// <summary>issue #105: <c>g &lt;ns-len&gt; &lt;key-len&gt;[ &lt;tag&gt;]\n</c>
    /// when namespaced, or the untouched legacy <c>G &lt;key-len&gt;[ &lt;tag&gt;]\n</c>
    /// otherwise.</summary>
    private static string GetHeader(byte[] namespaceBytes, int keyLength, uint? tag) =>
        IsNamespaced(namespaceBytes)
            ? $"g {namespaceBytes.Length} {keyLength}{TagField(tag)}\n"
            : $"G {keyLength}{TagField(tag)}\n";

    /// <summary>issue #105: <c>s &lt;ns-len&gt; &lt;key-len&gt; &lt;val-len&gt; [&lt;ttl&gt;][ &lt;tag&gt;]\n</c>
    /// when namespaced, or the untouched legacy <c>S</c> form otherwise —
    /// see <see cref="SetAsync(byte[], byte[], long)"/>'s doc comment for
    /// the TTL-omission rule, unchanged by namespacing.</summary>
    private static string SetHeader(byte[] namespaceBytes, int keyLength, int valueLength, long ttlSeconds, uint? tag)
    {
        string ttlField = ttlSeconds == 0 ? "" : $" {ttlSeconds}";
        return IsNamespaced(namespaceBytes)
            ? $"s {namespaceBytes.Length} {keyLength} {valueLength}{ttlField}{TagField(tag)}\n"
            : $"S {keyLength} {valueLength}{ttlField}{TagField(tag)}\n";
    }

    /// <summary>issue #105: <c>d &lt;ns-len&gt; &lt;key-len&gt;[ &lt;tag&gt;]\n</c>
    /// when namespaced, or the untouched legacy <c>D &lt;key-len&gt;[ &lt;tag&gt;]\n</c>
    /// otherwise.</summary>
    private static string DeleteHeader(byte[] namespaceBytes, int keyLength, uint? tag) =>
        IsNamespaced(namespaceBytes)
            ? $"d {namespaceBytes.Length} {keyLength}{TagField(tag)}\n"
            : $"D {keyLength}{TagField(tag)}\n";

    /// <summary>issue #106: <c>c &lt;ns-len&gt;[ &lt;tag&gt;]\n</c> — always
    /// lowercase, even for the empty (default) namespace, since <c>c</c>
    /// has no legacy uppercase form to fall back to (it postdates
    /// namespaces).</summary>
    private static string ClearHeader(byte[] namespaceBytes, uint? tag) =>
        $"c {namespaceBytes.Length}{TagField(tag)}\n";

    /// <summary>issue #129: <c>i &lt;ns-len&gt; &lt;key-len&gt; &lt;delta&gt;[ &lt;tag&gt;]\n</c> —
    /// always lowercase and always namespaced (0 addresses the default
    /// namespace), since <c>i</c> postdates namespaces and has no legacy
    /// uppercase form. <paramref name="delta"/> is emitted via
    /// <see cref="CultureInfo.InvariantCulture"/> so its wire form is
    /// always the canonical signed-decimal grammar the server expects
    /// (optional leading <c>-</c>, no leading zeros, no <c>+</c>), never a
    /// locale-dependent one.</summary>
    private static string IncrHeader(byte[] namespaceBytes, int keyLength, long delta, uint? tag) =>
        $"i {namespaceBytes.Length} {keyLength} {delta.ToString(CultureInfo.InvariantCulture)}{TagField(tag)}\n";

    /// <summary>issue #141: <c>k &lt;ns-len&gt; &lt;key-len&gt; &lt;val-len&gt; &lt;cond&gt; [&lt;ttl-seconds&gt;][ &lt;tag&gt;]\n</c> —
    /// always lowercase and always namespaced, like <see cref="IncrHeader"/>.
    /// <paramref name="cond"/> is emitted as a bare token, never
    /// length-prefixed — its own shape (<c>A</c>, <c>P</c>, or a
    /// 32-character hex digest) identifies it.</summary>
    private static string CasHeader(byte[] namespaceBytes, int keyLength, int valueLength, string cond, long ttlSeconds, uint? tag)
    {
        string ttlField = ttlSeconds == 0 ? "" : $" {ttlSeconds}";
        return $"k {namespaceBytes.Length} {keyLength} {valueLength} {cond}{ttlField}{TagField(tag)}\n";
    }

    /// <summary>issue #141: <c>x &lt;ns-len&gt; &lt;key-len&gt; &lt;cond&gt; [&lt;tag&gt;]\n</c> —
    /// <paramref name="digest"/> here is always a content digest, never
    /// <c>A</c>/<c>P</c> (see <see cref="RemoveIfMatchesAsync"/>'s doc
    /// comment).</summary>
    private static string RemoveIfMatchesHeader(byte[] namespaceBytes, int keyLength, string digest, uint? tag) =>
        $"x {namespaceBytes.Length} {keyLength} {digest}{TagField(tag)}\n";

    /// <summary>issue #141 — compare-and-set: stores <paramref name="value"/>
    /// at (<paramref name="namespaceBytes"/>, <paramref name="key"/>) only
    /// if <paramref name="cond"/> holds against the key's current stored
    /// bytes — <c>"A"</c> (absent), <c>"P"</c> (present, any value), or a
    /// 32-character lowercase hex digest (exact content match). Returns
    /// <c>true</c> on success (<c>S</c>), <c>false</c> on a condition
    /// mismatch (<c>N</c> — a normal outcome, not an exception, the same
    /// idiom <see cref="DeleteAsync(byte[], byte[])"/> uses for "nothing to
    /// act on"). <paramref name="ttlSeconds"/> means exactly what it means
    /// for <see cref="SetAsync(byte[], byte[], byte[], long)"/> (0 = no
    /// expiry). Always namespaced, like <see cref="IncrAsync"/> — no legacy
    /// uppercase form.
    ///
    /// <para>Not a distributed lock: LRU eviction can still reclaim the key
    /// exactly as it would after a plain <c>Set</c>.</para>
    ///
    /// <para>Cluster replication (same rule as <see cref="IncrAsync"/>):
    /// this method only ever talks to the ONE node this connection is
    /// dialed to. It is <see cref="NanocachedClient"/>'s job to call this
    /// against the primary owner only, and — on success — forward the
    /// literal resulting value to the replicas via this same connection's
    /// <see cref="SetAsync(byte[], byte[], byte[], long)"/>, never by
    /// sending <c>k</c> to a replica (which could let a replica evaluate
    /// <paramref name="cond"/> against its own possibly-different copy and
    /// reach a different outcome than the primary just did).</para></summary>
    internal async Task<bool> CasAsync(byte[] namespaceBytes, byte[] key, byte[] value, string cond, long ttlSeconds)
    {
        var (marker, _, _, _) = await RequestAsync(tag =>
            Frame(CasHeader(namespaceBytes, key.Length, value.Length, cond, ttlSeconds, tag), namespaceBytes, key, value))
            .ConfigureAwait(false);
        return marker switch
        {
            (byte)'S' => true,
            (byte)'N' => false,
            (byte)'W' => throw new WrongNodeException(),
            _ => throw Mismatch(marker),
        };
    }

    /// <summary>issue #141 — compare-and-set: removes the key at
    /// (<paramref name="namespaceBytes"/>, <paramref name="key"/>) only if
    /// <paramref name="digest"/> — always a content digest here; an
    /// absent/present-only conditioned delete is already the plain
    /// unconditional <see cref="DeleteAsync(byte[], byte[])"/> — matches its
    /// current stored bytes. Returns <c>true</c> on success (<c>D</c>),
    /// <c>false</c> on a mismatch or a missing key (<c>N</c>). See
    /// <see cref="CasAsync"/>'s doc comment for the shared not-a-lock
    /// caveat and cluster-replication rule (a successful removal is
    /// forwarded to replicas as an ordinary
    /// <see cref="DeleteAsync(byte[], byte[])"/>, never by replaying
    /// <c>x</c>).</summary>
    internal async Task<bool> RemoveIfMatchesAsync(byte[] namespaceBytes, byte[] key, string digest)
    {
        var (marker, _, _, _) = await RequestAsync(tag =>
            Frame(RemoveIfMatchesHeader(namespaceBytes, key.Length, digest, tag), namespaceBytes, key, null))
            .ConfigureAwait(false);
        return marker switch
        {
            (byte)'D' => true,
            (byte)'N' => false,
            (byte)'W' => throw new WrongNodeException(),
            _ => throw Mismatch(marker),
        };
    }

    // issue #151 — batched get/set: `m`/`o` — same always-namespaced shape
    // as INCR/CAS (an explicit <namespace-length>, 0 = default namespace;
    // no legacy pre-namespace form — batching postdates namespaces).
    // Restricted to one namespace per frame (docs/protocol.html#multi):
    // rendezvous hashing routes on (namespace, key), so a frame mixing
    // namespaces couldn't route as a single unit anyway. Routing (grouping
    // keys by owner, wrong-node retry, replica fan-out for MultiSetAsync)
    // is NanocachedClient's job, same split as every other op here.

    /// <summary>One key's outcome inside an <c>M</c> (multi-get) or
    /// <c>O</c> (multi-set) response (issue #151,
    /// docs/protocol.html#multi) — a batch never fails as a whole, so
    /// each key's result is independent of every other key's. Reused for
    /// both response kinds rather than two near-identical types:
    /// <list type="bullet">
    /// <item><c>M</c>: <see cref="Ok"/> true is a hit (<see cref="Value"/>
    /// holds the bytes, possibly empty); <see cref="WrongNode"/> is a
    /// per-key <c>W</c>; neither set is a clean miss (<c>-</c>).</item>
    /// <item><c>O</c>: <see cref="Ok"/> true is <c>S</c> (stored);
    /// <see cref="WrongNode"/> is <c>W</c>; <see cref="Value"/> is always
    /// <c>null</c> — a set has nothing to echo back.</item>
    /// </list></summary>
    internal readonly struct MultiEntry
    {
        public byte[]? Value { get; }
        public bool Ok { get; }
        public bool WrongNode { get; }

        private MultiEntry(byte[]? value, bool ok, bool wrongNode)
        {
            Value = value;
            Ok = ok;
            WrongNode = wrongNode;
        }

        internal static MultiEntry Hit(byte[] value) => new(value, true, false);
        internal static MultiEntry Miss() => new(null, false, false);
        internal static MultiEntry Wrong() => new(null, false, true);
        internal static MultiEntry Stored() => new(null, true, false);
    }

    /// <summary>Sends <c>m</c> — one round trip for every key in
    /// <paramref name="keys"/> (docs/protocol.html#multi).
    /// <c>entries[i]</c> answers <c>keys[i]</c>, in request order. A
    /// reply whose roster length doesn't match <c>keys.Count</c> is
    /// treated as a desynced connection, same stance as
    /// <see cref="Mismatch"/> — a malformed reply can't be trusted
    /// key-for-key.</summary>
    internal async Task<List<MultiEntry>> MultiGetAsync(byte[] namespaceBytes, IReadOnlyList<byte[]> keys)
    {
        var (marker, _, _, entries) = await RequestAsync(tag =>
            MultiGetFrame(namespaceBytes, keys, tag)).ConfigureAwait(false);
        if (marker != (byte)'M') throw Mismatch(marker);
        if (entries!.Count != keys.Count) throw DesyncedEntryCount("multi-get", entries.Count, keys.Count);
        return entries;
    }

    /// <summary>Builds an <c>m</c> request frame: <c>m &lt;ns-len&gt;
    /// &lt;n&gt; &lt;key-len-1&gt; ... &lt;key-len-n&gt;[ &lt;tag&gt;]\n&lt;ns&gt;&lt;key-1&gt;...&lt;key-n&gt;</c>
    /// (docs/protocol.html#multi).</summary>
    private static byte[] MultiGetFrame(byte[] namespaceBytes, IReadOnlyList<byte[]> keys, uint? tag)
    {
        var header = new StringBuilder("m ").Append(namespaceBytes.Length).Append(' ').Append(keys.Count);
        foreach (byte[] key in keys)
        {
            header.Append(' ').Append(key.Length);
        }
        header.Append(TagField(tag)).Append('\n');
        byte[] headerBytes = Encoding.ASCII.GetBytes(header.ToString());

        int bodyLength = namespaceBytes.Length;
        foreach (byte[] key in keys) bodyLength += key.Length;
        var frame = new byte[headerBytes.Length + bodyLength];
        int offset = 0;
        headerBytes.CopyTo(frame, offset);
        offset += headerBytes.Length;
        namespaceBytes.CopyTo(frame, offset);
        offset += namespaceBytes.Length;
        foreach (byte[] key in keys)
        {
            key.CopyTo(frame, offset);
            offset += key.Length;
        }
        return frame;
    }

    /// <summary>Sends <c>o</c> — stores every key/value pair in one round
    /// trip, one shared <paramref name="ttlSeconds"/> (0 means no expiry)
    /// for the whole batch rather than per key
    /// (docs/protocol.html#multi). <c>entries[i]</c> answers
    /// <c>keys[i]</c>/<c>values[i]</c>, in request order; see
    /// <see cref="MultiGetAsync"/> for the same "only a desynced roster
    /// is an error" stance.</summary>
    internal async Task<List<MultiEntry>> MultiSetAsync(
        byte[] namespaceBytes, IReadOnlyList<byte[]> keys, IReadOnlyList<byte[]> values, long ttlSeconds)
    {
        var (marker, _, _, entries) = await RequestAsync(tag =>
            MultiSetFrame(namespaceBytes, keys, values, ttlSeconds, tag)).ConfigureAwait(false);
        if (marker != (byte)'O') throw Mismatch(marker);
        if (entries!.Count != keys.Count) throw DesyncedEntryCount("multi-set", entries.Count, keys.Count);
        return entries;
    }

    /// <summary>Builds an <c>o</c> request frame: <c>o &lt;ns-len&gt;
    /// &lt;n&gt; &lt;key-len-1&gt; &lt;value-len-1&gt; ... &lt;key-len-n&gt;
    /// &lt;value-len-n&gt; [&lt;ttl&gt;][ &lt;tag&gt;]\n&lt;ns&gt;&lt;key-1&gt;&lt;value-1&gt;...&lt;key-n&gt;&lt;value-n&gt;</c>
    /// (docs/protocol.html#multi). The optional TTL sits ahead of the
    /// tag, same convention <see cref="CasHeader"/>'s own
    /// <c>[ttl-seconds]</c> uses.</summary>
    private static byte[] MultiSetFrame(
        byte[] namespaceBytes, IReadOnlyList<byte[]> keys, IReadOnlyList<byte[]> values, long ttlSeconds, uint? tag)
    {
        var header = new StringBuilder("o ").Append(namespaceBytes.Length).Append(' ').Append(keys.Count);
        for (int i = 0; i < keys.Count; i++)
        {
            header.Append(' ').Append(keys[i].Length).Append(' ').Append(values[i].Length);
        }
        if (ttlSeconds != 0)
        {
            header.Append(' ').Append(ttlSeconds);
        }
        header.Append(TagField(tag)).Append('\n');
        byte[] headerBytes = Encoding.ASCII.GetBytes(header.ToString());

        int bodyLength = namespaceBytes.Length;
        for (int i = 0; i < keys.Count; i++) bodyLength += keys[i].Length + values[i].Length;
        var frame = new byte[headerBytes.Length + bodyLength];
        int offset = 0;
        headerBytes.CopyTo(frame, offset);
        offset += headerBytes.Length;
        namespaceBytes.CopyTo(frame, offset);
        offset += namespaceBytes.Length;
        for (int i = 0; i < keys.Count; i++)
        {
            keys[i].CopyTo(frame, offset);
            offset += keys[i].Length;
            values[i].CopyTo(frame, offset);
            offset += values[i].Length;
        }
        return frame;
    }

    /// <summary>An <c>M</c>/<c>O</c> response whose result-roster length
    /// doesn't match the request's key count: the streams are just as
    /// desynced as a kind mismatch (<see cref="Mismatch"/>), so this
    /// poisons the connection the same way.</summary>
    private ConnectionLostException DesyncedEntryCount(string op, int got, int want)
    {
        Close();
        return new ConnectionLostException(
            $"nanocached: {op} response roster length {got} does not match request key count {want} "
            + "(connection desynced)");
    }

    /// <summary>issue #105: <paramref name="namespaceBytes"/> leads the
    /// body, ahead of the key (and, for a Set, the value) — empty for
    /// every legacy (unnamespaced) call, so
    /// <c>namespaceBytes.CopyTo(...)</c> below is then a no-op and the
    /// frame is exactly what it always was.</summary>
    private static byte[] Frame(string header, byte[] namespaceBytes, byte[] key, byte[]? value)
    {
        byte[] headerBytes = Encoding.ASCII.GetBytes(header);
        var frame = new byte[headerBytes.Length + namespaceBytes.Length + key.Length + (value?.Length ?? 0)];
        int offset = 0;
        headerBytes.CopyTo(frame, offset);
        offset += headerBytes.Length;
        namespaceBytes.CopyTo(frame, offset);
        offset += namespaceBytes.Length;
        key.CopyTo(frame, offset);
        offset += key.Length;
        value?.CopyTo(frame, offset);
        return frame;
    }

    /// <summary>echoed response tags: on a tagged connection every request header's
    /// last field is the client's tag; an untagged connection's wire bytes
    /// are unchanged from before this field existed.</summary>
    private static string TagField(uint? tag) => tag is null ? "" : $" {tag}";

    /// <summary>
    /// A well-formed response of the wrong kind (a <c>S</c> answering a G)
    /// means the request/response streams are misaligned — every later
    /// response would answer the wrong request, silently returning other
    /// keys' data. Poison the connection, and classify as connection-lost
    /// so the client's retry layer redials and retries once. Requests
    /// still pending behind this one may already have been resolved with
    /// misaligned data by the time this runs — an inherent limitation of
    /// matching-by-order pipelining shared with the TypeScript SDK's
    /// Connection (request pipelining), not something this SDK introduces.
    /// This is the second line of defense: on a tagged connection, the
    /// read loop's own tag check (echoed response tags) normally catches a desync like
    /// this before any response is ever handed to a caller.
    /// </summary>
    private ConnectionLostException Mismatch(byte marker)
    {
        Close();
        return new ConnectionLostException(
            $"nanocached: response '{(char)marker}' does not match the request (connection desynced)");
    }

    /// <summary>issue #125 — retryable-error status <c>R</c>: how many
    /// times a single logical request is attempted in total before a
    /// still-<c>R</c> answer surfaces as <see cref="RetryableException"/> —
    /// 1 initial attempt plus up to 2 retries. Fixed by the protocol's
    /// spec, not configurable.</summary>
    private const int MaxRequestAttempts = 3;

    /// <summary>issue #125: the delay before each retry —
    /// <c>RetryDelays[0]</c> before the 2nd attempt, <c>RetryDelays[1]</c>
    /// before the 3rd.</summary>
    private static readonly TimeSpan[] RetryDelays =
    {
        TimeSpan.FromMilliseconds(50),
        TimeSpan.FromMilliseconds(100),
    };

    /// <summary>issue #125 — retryable-error status <c>R</c>: wraps
    /// <see cref="SendOnceAsync"/> with the protocol's bounded transient
    /// retry. An <c>R</c> answer means THIS request specifically failed
    /// transiently and the connection is fine — retry the same request on
    /// the same connection, up to <see cref="MaxRequestAttempts"/> attempts
    /// total, sleeping <see cref="RetryDelays"/> between attempts. Every
    /// <c>R</c> seen is reported via <see cref="_onTransientRetry"/>
    /// (<c>Stats().TransientRetries</c>) whether or not it's the one that
    /// ultimately fails the call. A still-<c>R</c> answer on the final
    /// attempt raises <see cref="RetryableException"/> — this connection is
    /// never closed or redialed for that (<c>R</c> is not a connection
    /// error, not a <c>W</c>, not an <c>E</c>) and stays usable for the
    /// next operation. <paramref name="buildFrame"/> runs again for each
    /// attempt, so a tagged connection claims a fresh tag per attempt —
    /// see <see cref="SendOnceAsync"/>'s own doc comment for what it does
    /// with it.</summary>
    private async Task<(byte Marker, byte[]? Value, long TtlSeconds, List<MultiEntry>? Entries)> RequestAsync(Func<uint?, byte[]> buildFrame)
    {
        for (int attempt = 1; ; attempt++)
        {
            (byte Marker, byte[]? Value, long TtlSeconds, List<MultiEntry>? Entries) response = await SendOnceAsync(buildFrame).ConfigureAwait(false);
            if (response.Marker != (byte)'R')
            {
                return response;
            }

            _onTransientRetry?.Invoke();
            if (attempt >= MaxRequestAttempts)
            {
                throw new RetryableException(
                    $"nanocached: request answered R on all {MaxRequestAttempts} attempts (transient failure); "
                    + "the connection is still usable");
            }
            await Task.Delay(RetryDelays[attempt - 1]).ConfigureAwait(false);
        }
    }

    /// <summary>Enqueues a pending slot and writes the built frame under
    /// one semaphore — see the class doc comment — then awaits its own
    /// <see cref="TaskCompletionSource{TResult}"/>, not the stream.
    /// Nothing here needs to guard against the caller abandoning this
    /// await (e.g. racing it with a timeout): unlike some other SDKs'
    /// ports of this design, this method never receives a
    /// <see cref="CancellationToken"/> to pass into the underlying
    /// <see cref="Stream"/> calls, so the write, once started, always
    /// runs to completion regardless of what the caller does
    /// afterward — and completing an abandoned
    /// <see cref="TaskCompletionSource{TResult}"/> that nothing is
    /// awaiting anymore is harmless. The await below is still bounded:
    /// <see cref="RequestTimeout"/>'s watchdog poisons the connection
    /// when a response stops making progress (issue #42). One send-and-await
    /// of ONE attempt — <see cref="RequestAsync"/>, the only caller, is
    /// what turns a run of these into the bounded <c>R</c> retry.</summary>
    /// <param name="buildFrame">Builds the wire frame from this request's
    /// claimed tag (<c>null</c> on an untagged connection). Called inside
    /// the write-gate critical section, after the tag is claimed but
    /// before anything is enqueued — an encoder that rejects its input
    /// must fail with nothing queued, or the next response would resolve
    /// an orphaned waiter and desync the stream (echoed response tags).</param>
    private async Task<(byte Marker, byte[]? Value, long TtlSeconds, List<MultiEntry>? Entries)> SendOnceAsync(Func<uint?, byte[]> buildFrame)
    {
        if (IsClosed)
        {
            // issue #225: known dead before this call ever tried to send
            // anything — nothing was written, so replaying this exact
            // request (e.g. Incr/CAS) after a redial can never double-apply
            // it.
            throw new ConnectionLostException("nanocached: connection is closed", requestNotSent: true);
        }

        var tcs = new TaskCompletionSource<(byte Marker, byte[]? Value, long TtlSeconds, List<MultiEntry>? Entries)>(
            TaskCreationOptions.RunContinuationsAsynchronously);

        await _writeGate.WaitAsync().ConfigureAwait(false);
        try
        {
            if (IsClosed)
            {
                // issue #225: same reasoning as the pre-gate check above —
                // this call still hasn't written anything.
                throw new ConnectionLostException("nanocached: connection is closed", requestNotSent: true);
            }
            _sinceLastUse.Restart();
            // Echoed response tags: the tag is claimed in the same critical section
            // that enqueues the waiter and writes the frame (request pipelining's
            // enqueue+write atomicity), so tag order can never skew from
            // queue/wire order.
            uint? tag = _tagged ? ClaimTag() : null;
            byte[] frame = buildFrame(tag);
            _pending.Enqueue((tcs, tag));
            // Armed only on the 0→1 transition: arming on *every* request
            // would let a continuous stream of new requests push the
            // deadline forever ahead of a server that has stopped
            // answering — exactly the half-open hang the timeout exists
            // to catch (issue #42).
            if (Interlocked.Increment(ref _outstanding) == 1)
            {
                ArmDeadline();
            }
            await _stream.WriteAsync(frame).ConfigureAwait(false);
            await _stream.FlushAsync().ConfigureAwait(false);
        }
        catch (Exception error) when (error is IOException or ObjectDisposedException)
        {
            // The stream state after a failed write is unknown — poison
            // the connection so the client redials lazily.
            //
            // issue #225: WriteAsync/FlushAsync failing outright (rather
            // than completing) is the idle-FIN signature — the peer had
            // already closed the connection, so the OS rejects the write
            // before any of this frame reaches it (the same distinction
            // Java's poison()/Go's applyReconnecting comment call out).
            // requestNotSent: true tells NanocachedClient's non-idempotent
            // retry guard (Incr/CAS/RemoveIfMatches) this specific attempt
            // is safe to replay after a redial. A genuine partial write
            // (some bytes reached the peer, then the socket died) can't be
            // told apart from this from here — .NET's Stream.WriteAsync
            // itself loops until every byte is either written or an error
            // is raised, so in practice a mid-frame partial write and a
            // clean pre-write rejection both surface as this same
            // exception; we still treat it as not-sent, matching this
            // SDK's own "if the write path cannot distinguish reliably,
            // treat WriteAsync completing as possibly applied" rule — this
            // catch only runs when WriteAsync/FlushAsync did NOT complete.
            Close();
            throw new ConnectionLostException(
                $"nanocached: connection failed: {error.Message}", error, requestNotSent: true);
        }
        finally
        {
            _writeGate.Release();
        }

        return await tcs.Task.ConfigureAwait(false);
    }

    /// <summary>Only ever called from inside the <c>_writeGate</c>
    /// critical section, so no interlocking is needed despite concurrent
    /// callers of <see cref="RequestAsync"/>.</summary>
    private uint ClaimTag()
    {
        uint tag = _nextTag;
        unchecked { _nextTag++; } // wraps at u32, matching the wire's width
        return tag;
    }

    /// <summary>This connection's only reader, for its whole lifetime —
    /// nothing else may read from <see cref="_stream"/>. Consumes
    /// responses off the wire and dispatches each to the oldest pending
    /// request (FIFO — request pipelining), until a read fails.</summary>
    private async Task ReadLoopAsync()
    {
        while (true)
        {
            (byte Marker, byte[]? Value, long TtlSeconds, uint? Tag, List<MultiEntry>? Entries) response;
            try
            {
                response = await ReadResponseAsync().ConfigureAwait(false);
            }
            catch (Exception error) when (error is IOException or ObjectDisposedException or EndOfStreamException
                or ConnectionLostException)
            {
                // error belongs to whichever request has been waiting
                // longest — this loop only ever reads one response at a
                // time, in order, so a failure here is always about the
                // oldest pending request specifically, not the
                // connection in general; Close() drains everyone else
                // with a generic "connection closed" instead, since
                // their responses were never received at all. When the
                // read failed because CloseWithReason disposed the stream
                // under us, its recorded reason is the root cause and the
                // disposed-stream error is just its echo (issue #42's CI
                // race: this dequeue can beat CloseWithReason's drain to
                // the stalled request).
                // Whatever the cause, the waiter must see a
                // ConnectionLostException: the README promises every
                // failure extends NanocachedException, and the client's
                // retry layer only redials on ConnectionLostException — a
                // raw EndOfStreamException (server FIN landing while a
                // request was in flight) used to escape both.
                if (_pending.TryDequeue(out var failed))
                {
                    Exception cause = _closeReason ?? error;
                    failed.Tcs.TrySetException(cause is ConnectionLostException
                        ? cause
                        : new ConnectionLostException($"nanocached: connection failed: {cause.Message}", cause));
                }
                Close();
                return;
            }

            // One atomic dequeue, not an IsEmpty check followed by a
            // TryDequeue: the two are separate operations on a lock-free
            // queue, and a request enqueued between them by a concurrent
            // RequestAsync would be dequeued here yet judged "unsolicited"
            // by the stale emptiness read — and, popped from the queue,
            // never completed by Close()'s drain either (a caller awaiting
            // forever). Every other SDK does this under the write lock;
            // here the single TryDequeue is the atomic step.
            bool dispatched = _pending.TryDequeue(out var pending);

            // Progress-based deadline (see RequestAsync): a dispatched
            // response is progress, so the next-oldest request gets a
            // fresh window; with nothing left waiting, clear it so an
            // otherwise-idle connection is never closed by it.
            if (dispatched)
            {
                if (Interlocked.Decrement(ref _outstanding) == 0) ClearDeadline();
                else ArmDeadline();
            }

            // An unsolicited "busy" response means the server hit its
            // connection limit right after accept and is about to close
            // the connection; it isn't an answer to anything we sent
            // (mirrors the TypeScript SDK's Connection.onData). Busy is
            // always untagged (echoed response tags) — it is never a reply to a
            // specific request.
            if (response.Marker == (byte)'B')
            {
                if (!dispatched)
                {
                    Close();
                    return;
                }

                // A busy marker while a request was waiting answers
                // nothing — the streams are misaligned just as surely as
                // a wrong tag. The dequeued request must be failed here
                // (the drain can no longer reach it), and everything
                // behind it poisoned the same way.
                var busyDesync = new ConnectionLostException(
                    "nanocached: unexpected busy response while a request was pending (connection desynced)");
                CloseWithReason(busyDesync);
                pending.Tcs.TrySetException(busyDesync);
                return;
            }

            if (!dispatched)
            {
                // Unsolicited and not the known busy case — desync.
                Close();
                return;
            }

            // Echoed response tags: on a tagged connection, verify the echoed tag
            // against the request this response is about to answer —
            // *before* it can reach any caller. A mismatch means the
            // streams are misaligned; unlike the caller-side kind check
            // (Mismatch()), catching it here stops the misdelivery
            // instead of merely noticing it later.
            if (_tagged && response.Tag != pending.Tag)
            {
                var error = new ConnectionLostException(
                    $"nanocached: response tag {response.Tag} does not answer request tag {pending.Tag} "
                    + "(connection desynced)");
                // pending has already been dequeued, so CloseWithReason's
                // own drain won't reach it — poison the rest of the
                // queue with the same desynced reason, then reject this
                // one directly.
                CloseWithReason(error);
                pending.Tcs.TrySetException(error);
                return;
            }

            pending.Tcs.TrySetResult((response.Marker, response.Value, response.TtlSeconds, response.Entries));
        }
    }

    private async Task<(byte Marker, byte[]? Value, long TtlSeconds, uint? Tag, List<MultiEntry>? Entries)> ReadResponseAsync()
    {
        byte marker = await ReadByteAsync().ConfigureAwait(false);
        switch (marker)
        {
            case (byte)'V':
            {
                // Untagged: `V <len>`. Tagged: `V <len> <tag>` (echoed response tags).
                string[] fields = (await ReadLineAsync().ConfigureAwait(false)).Split(' ');
                if (fields.Length != (_tagged ? 2 : 1))
                {
                    throw new ConnectionLostException("nanocached: invalid value header in response");
                }

                // A non-numeric, negative, or absurd length (the server
                // caps requests at 1 MiB) is protocol garbage: the
                // connection is desynced mid-frame and must be poisoned,
                // and the error must be connection-classified so the
                // redial/retry layer handles it (issue #8).
                if (!int.TryParse(fields[0], NumberStyles.None, CultureInfo.InvariantCulture, out int length) || length < 0 || length > MaxValueLength)
                {
                    throw new ConnectionLostException("nanocached: invalid value length in response");
                }
                uint? tag = _tagged ? ParseTag(fields[1]) : null;

                var value = new byte[length];
                await _stream.ReadExactlyAsync(value).ConfigureAwait(false);
                return (marker, value, 0, tag, null);
            }
            case (byte)'I':
            {
                // issue #129: untagged `I <len>` or `I <len> <ttl>`;
                // tagged `I <len> <tag>` (no ttl) or `I <len> <ttl> <tag>`
                // — disambiguated purely by whether this connection is
                // tagged, exactly like S's own optional trailing ttl field
                // on the request side: on an untagged connection 0 fields
                // past <len> means no ttl, 1 means ttl present; on a
                // tagged connection 1 field past <len> means "just the
                // tag", 2 means "ttl then tag".
                string[] fields = (await ReadLineAsync().ConfigureAwait(false)).Split(' ');
                int bareFields = _tagged ? 2 : 1;
                int ttlFields = bareFields + 1;
                if (fields.Length != bareFields && fields.Length != ttlFields)
                {
                    throw new ConnectionLostException("nanocached: invalid incr header in response");
                }

                if (!int.TryParse(fields[0], NumberStyles.None, CultureInfo.InvariantCulture, out int length) || length < 0 || length > MaxValueLength)
                {
                    throw new ConnectionLostException("nanocached: invalid value length in response");
                }

                long ttlSeconds = 0;
                if (fields.Length == ttlFields)
                {
                    if (!long.TryParse(fields[1], NumberStyles.None, CultureInfo.InvariantCulture, out ttlSeconds) || ttlSeconds < 0)
                    {
                        throw new ConnectionLostException("nanocached: invalid ttl in incr response");
                    }
                }
                uint? tag = _tagged ? ParseTag(fields[^1]) : null;

                var value = new byte[length];
                await _stream.ReadExactlyAsync(value).ConfigureAwait(false);
                return (marker, value, ttlSeconds, tag, null);
            }
            case (byte)'S':
            case (byte)'D':
            case (byte)'N':
            case (byte)'W':
            case (byte)'C': // issue #106: same fixed shape as S/D/N/W.
            case (byte)'R': // issue #125: same fixed shape — retryable-error status; RequestAsync retries this transparently before it can ever reach a caller.
            case (byte)'T': // issue #129: same fixed shape — INCR's stored value isn't its counter grammar, or the delta would overflow.
            {
                if (!_tagged)
                {
                    await ExpectLfAsync().ConfigureAwait(false); // the trailing '\n'
                    return (marker, null, 0, null, null);
                }

                // Tagged: `<marker> <tag>\n` — a byte other than the
                // space separator here means the server answered with the
                // untagged 2-byte form on a connection it agreed to tag,
                // i.e. the response is missing its tag entirely: the
                // streams are desynced exactly as much as an echoed wrong
                // tag would mean.
                byte next = await ReadByteAsync().ConfigureAwait(false);
                if (next != (byte)' ')
                {
                    throw new ConnectionLostException(
                        "nanocached: response is missing its tag (connection desynced)");
                }
                // The single mandatory delimiter space was already
                // consumed and validated above — nothing left for
                // ReadLineAsync to strip, so any further leading
                // whitespace here is itself an attack and must reach
                // ParseTag's NumberStyles.None check intact (issue #462).
                uint tag = ParseTag(await ReadLineAsync(stripLeadingSpace: false).ConfigureAwait(false));
                return (marker, null, 0, tag, null);
            }
            case (byte)'B':
                // Busy is unsolicited and always bare (echoed response tags) — never
                // tagged, even on a tagged connection.
                await ExpectLfAsync().ConfigureAwait(false); // the trailing '\n'
                return (marker, null, 0, null, null);
            // issue #151 — batched get/set (docs/protocol.html#multi):
            // `M <n> <result-1> ... <result-n>[ <tag>]\n<hit values,
            // concatenated in request order>`. Each result token is "-"
            // (miss), "W" (wrong node), or a decimal byte length (a hit —
            // that many trailing body bytes belong to this key, read
            // here, inline, in token order, since only hit tokens consume
            // body bytes).
            case (byte)'M':
            {
                string[] fields = (await ReadLineAsync().ConfigureAwait(false)).Split(' ');
                if (fields.Length < 1 || !int.TryParse(fields[0], NumberStyles.None, CultureInfo.InvariantCulture, out int count) || count < 0)
                {
                    throw new ConnectionLostException("nanocached: invalid multi-get header in response");
                }
                int wantFields = 1 + count + (_tagged ? 1 : 0);
                if (fields.Length != wantFields)
                {
                    throw new ConnectionLostException("nanocached: invalid multi-get header in response");
                }
                var entries = new List<MultiEntry>(count);
                // issue #207: running total of every hit's declared length
                // seen so far this reply — bounds the reply as a whole,
                // not just each individual entry (MaxValueLength, checked
                // just below, only bounds one entry at a time).
                long totalBytes = 0;
                for (int i = 0; i < count; i++)
                {
                    string token = fields[1 + i];
                    if (token == "-")
                    {
                        entries.Add(MultiEntry.Miss());
                    }
                    else if (token == "W")
                    {
                        entries.Add(MultiEntry.Wrong());
                    }
                    else
                    {
                        if (!int.TryParse(token, NumberStyles.None, CultureInfo.InvariantCulture, out int length) || length < 0 || length > MaxValueLength)
                        {
                            throw new ConnectionLostException(
                                "nanocached: invalid multi-get result length in response");
                        }
                        // issue #207: checked BEFORE allocating/reading
                        // this entry's body — a claim that would push the
                        // cumulative total over the bound must poison the
                        // connection before the over-large read happens,
                        // not after.
                        totalBytes += length;
                        if (totalBytes > MaxMultiGetResponseBytes)
                        {
                            throw new ConnectionLostException(
                                $"nanocached: multi-get response exceeds {MaxMultiGetResponseBytes} bytes "
                                + "(connection desynced)");
                        }
                        var hit = new byte[length];
                        await _stream.ReadExactlyAsync(hit).ConfigureAwait(false);
                        entries.Add(MultiEntry.Hit(hit));
                    }
                }
                uint? tag = _tagged ? ParseTag(fields[1 + count]) : null;
                return (marker, null, 0, tag, entries);
            }
            // issue #151: `O <n> <result-1> ... <result-n>[ <tag>]\n` — no
            // body, unlike M's hit values (a set has nothing to echo
            // back). Each token is "S" (stored) or "W" (wrong node).
            //
            // issue #207: unlike M, no cumulative-bytes bound is needed
            // here — an O reply carries no bodies at all, just one
            // fixed-width token per key on this single header line, so its
            // decode cost is already O(count), and count is already
            // bounded by MaxHeaderLineLength capping the line this whole
            // roster lives on.
            case (byte)'O':
            {
                string[] fields = (await ReadLineAsync().ConfigureAwait(false)).Split(' ');
                if (fields.Length < 1 || !int.TryParse(fields[0], NumberStyles.None, CultureInfo.InvariantCulture, out int count) || count < 0)
                {
                    throw new ConnectionLostException("nanocached: invalid multi-set header in response");
                }
                int wantFields = 1 + count + (_tagged ? 1 : 0);
                if (fields.Length != wantFields)
                {
                    throw new ConnectionLostException("nanocached: invalid multi-set header in response");
                }
                var entries = new List<MultiEntry>(count);
                for (int i = 0; i < count; i++)
                {
                    string token = fields[1 + i];
                    if (token == "S")
                    {
                        entries.Add(MultiEntry.Stored());
                    }
                    else if (token == "W")
                    {
                        entries.Add(MultiEntry.Wrong());
                    }
                    else
                    {
                        throw new ConnectionLostException("nanocached: invalid multi-set result token in response");
                    }
                }
                uint? tag = _tagged ? ParseTag(fields[1 + count]) : null;
                return (marker, null, 0, tag, entries);
            }
            default:
                // A garbage marker means the stream is desynced; poison
                // and classify as connection-level (issue #8) so the
                // retry layer redials instead of failing the op outright.
                throw new ConnectionLostException(
                    $"nanocached: unexpected response from server: {(char)marker}");
        }
    }

    /// <summary>The node's counter body
    /// is an optional <c>-</c> followed by digits — never a <c>+</c>, never
    /// whitespace — and every other wire integer is digits only, so all of
    /// them are parsed with <see cref="NumberStyles.None"/> and the
    /// invariant culture (matching the encode side); this is the one
    /// place a sign is allowed, and only the minus.</summary>
    private static bool TryParseWireCounter(string body, out long value)
    {
        value = 0;
        if (body.Length == 0 || body[0] == '+')
        {
            return false;
        }
        return long.TryParse(body, NumberStyles.AllowLeadingSign, CultureInfo.InvariantCulture, out value);
    }

    private static uint ParseTag(string field)
    {
        if (!uint.TryParse(field, NumberStyles.None, CultureInfo.InvariantCulture, out uint tag))
        {
            throw new ConnectionLostException("nanocached: invalid response tag");
        }
        return tag;
    }

    // Reused across every call instead of allocating a fresh byte[1] each
    // time (audit finding): safe because ReadLoopAsync is this connection's
    // only reader, for its whole lifetime (see the class doc comment) — no
    // other call can be mid-ReadByteAsync concurrently.
    private readonly byte[] _readByteBuffer = new byte[1];

    private async Task<byte> ReadByteAsync()
    {
        await _stream.ReadExactlyAsync(_readByteBuffer).ConfigureAwait(false);
        return _readByteBuffer[0];
    }

    /// <summary>Consumes one byte and verifies it is '\n' — used for the
    /// untagged fixed-shape responses (S/D/N/W/B), which are exactly two
    /// bytes on the wire. A byte other than '\n' here means the streams
    /// are desynced (e.g. a server that unexpectedly tagged a response on
    /// an untagged connection) and every later response would be
    /// misaligned too, so this must poison the connection rather than
    /// silently discard the extra byte. Mirrors the Java SDK's
    /// expectLf() (Connection.java:492-498). Throwing here relies on the
    /// same path every other ReadResponseAsync failure already uses:
    /// ReadLoopAsync's catch classifies this as a ConnectionLostException
    /// and poisons the connection via Close().</summary>
    private async Task ExpectLfAsync()
    {
        byte value = await ReadByteAsync().ConfigureAwait(false);
        if (value != (byte)'\n')
        {
            throw new ConnectionLostException(
                "nanocached: unexpected byte after response marker (connection desynced)");
        }
    }

    /// <summary>Reads up to (and consuming) the next '\n'. Every header
    /// line the wire sends carries exactly one mandatory space right
    /// after the marker byte — the field delimiter, not part of any
    /// field's value — so <paramref name="stripLeadingSpace"/> (true by
    /// default) strips exactly that one leading space, never more.
    /// Issue #462: a blanket <c>.Trim()</c> here used to strip ANY amount
    /// of leading/trailing whitespace from the whole line, which silently
    /// absorbed a malicious server's extra padding around the first or
    /// last field on the line — the one place NumberStyles.None's
    /// leading/trailing-whitespace rejection could never see it, since
    /// the whitespace was gone before Split(' ') ever ran. Now only the
    /// single mandatory delimiter is ever removed; any other stray
    /// whitespace stays in the string and reaches Split/TryParse, which
    /// then reject it exactly like any other non-digit byte would.
    /// Callers that have already consumed and validated that single
    /// delimiter space themselves (the bare-marker tagged-response path)
    /// pass <paramref name="stripLeadingSpace"/> as false, since there is
    /// no delimiter left in the line for this method to strip.</summary>
    private async Task<string> ReadLineAsync(bool stripLeadingSpace = true)
    {
        var line = new StringBuilder();
        while (true)
        {
            byte b = await ReadByteAsync().ConfigureAwait(false);
            if (b == (byte)'\n')
            {
                string result = line.ToString();
                return stripLeadingSpace && result.StartsWith(' ') ? result[1..] : result;
            }
            if (line.Length >= MaxHeaderLineLength)
            {
                throw new ConnectionLostException("nanocached: response header line too long");
            }
            line.Append((char)b);
        }
    }
}
