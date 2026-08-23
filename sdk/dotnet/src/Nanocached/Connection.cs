using System.Collections.Concurrent;
using System.Diagnostics;
using System.Text;

namespace Nanocached;

/// <summary>
/// One already-identified connection to a single nanocached-node, speaking
/// the cache protocol (<c>G</c>/<c>S</c>/<c>D</c>, their namespaced
/// counterparts <c>g</c>/<c>s</c>/<c>d</c> — issue #105 — and the
/// namespace-clear/flush-everything commands <c>c</c>/<c>F</c> — issue
/// #106 — the <c>A</c> identify exchange happens in <see cref="Identify"/>
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
/// </summary>
internal sealed class Connection
{
    // The server's own request cap is 1 MiB; this constant doubles that
    // as headroom, so a claimed length beyond it is definitely a corrupt
    // or malicious frame, never just a legitimately large value.
    private const int MaxValueLength = 2 * 1024 * 1024;

    // Header/tag lines (the marker line ahead of a V's body, or the whole
    // line for S/D/N/W) are always a handful of bytes in the real
    // protocol. Without a cap, a malicious or buggy node that streams
    // bytes with no '\n' would grow ReadLineAsync's StringBuilder without
    // bound, gated only by RequestTimeout rather than failing fast.
    private const int MaxHeaderLineLength = 1024;

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
    // A u32 wrapping counter (echoed response tags), claimed only inside the
    // _writeGate critical section — never touched concurrently, so no
    // Interlocked ceremony is needed here the way _closedFlag needs one.
    private uint _nextTag;
    private readonly ConcurrentQueue<(TaskCompletionSource<(byte Marker, byte[]? Value)> Tcs, uint? Tag)> _pending = new();
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
    internal Connection(Stream stream, bool tagged = false, Action? onClosed = null)
    {
        _stream = stream;
        _tagged = tagged;
        _onClosed = onClosed;
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
        var (marker, value) = await RequestAsync(tag =>
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
        var (marker, _) = await RequestAsync(tag =>
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
        var (marker, _) = await RequestAsync(tag =>
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
        var (marker, _) = await RequestAsync(tag =>
            Frame(ClearHeader(namespaceBytes, tag), namespaceBytes, EmptyNamespace, null))
            .ConfigureAwait(false);
        if (marker != (byte)'C') throw Mismatch(marker);
    }

    /// <summary>issue #106: drops every namespace on this node, the
    /// default one included (<c>F\n</c>). See <see cref="ClearAsync(byte[])"/>'s
    /// doc comment for the shared <c>C</c>-ack/no-<c>W</c> rules.</summary>
    internal async Task ClearAllAsync()
    {
        var (marker, _) = await RequestAsync(tag =>
            Encoding.ASCII.GetBytes($"F{TagField(tag)}\n"))
            .ConfigureAwait(false);
        if (marker != (byte)'C') throw Mismatch(marker);
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
    /// when a response stops making progress (issue #42).</summary>
    /// <param name="buildFrame">Builds the wire frame from this request's
    /// claimed tag (<c>null</c> on an untagged connection). Called inside
    /// the write-gate critical section, after the tag is claimed but
    /// before anything is enqueued — an encoder that rejects its input
    /// must fail with nothing queued, or the next response would resolve
    /// an orphaned waiter and desync the stream (echoed response tags).</param>
    private async Task<(byte Marker, byte[]? Value)> RequestAsync(Func<uint?, byte[]> buildFrame)
    {
        if (IsClosed)
        {
            throw new ConnectionLostException("nanocached: connection is closed");
        }

        var tcs = new TaskCompletionSource<(byte Marker, byte[]? Value)>(
            TaskCreationOptions.RunContinuationsAsynchronously);

        await _writeGate.WaitAsync().ConfigureAwait(false);
        try
        {
            if (IsClosed)
            {
                throw new ConnectionLostException("nanocached: connection is closed");
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
            Close();
            throw new ConnectionLostException($"nanocached: connection failed: {error.Message}", error);
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
            (byte Marker, byte[]? Value, uint? Tag) response;
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

            pending.Tcs.TrySetResult((response.Marker, response.Value));
        }
    }

    private async Task<(byte Marker, byte[]? Value, uint? Tag)> ReadResponseAsync()
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
                if (!int.TryParse(fields[0], out int length) || length < 0 || length > MaxValueLength)
                {
                    throw new ConnectionLostException("nanocached: invalid value length in response");
                }
                uint? tag = _tagged ? ParseTag(fields[1]) : null;

                var value = new byte[length];
                await _stream.ReadExactlyAsync(value).ConfigureAwait(false);
                return (marker, value, tag);
            }
            case (byte)'S':
            case (byte)'D':
            case (byte)'N':
            case (byte)'W':
            case (byte)'C': // issue #106: same fixed shape as S/D/N/W.
            {
                if (!_tagged)
                {
                    await ExpectLfAsync().ConfigureAwait(false); // the trailing '\n'
                    return (marker, null, null);
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
                uint tag = ParseTag(await ReadLineAsync().ConfigureAwait(false));
                return (marker, null, tag);
            }
            case (byte)'B':
                // Busy is unsolicited and always bare (echoed response tags) — never
                // tagged, even on a tagged connection.
                await ExpectLfAsync().ConfigureAwait(false); // the trailing '\n'
                return (marker, null, null);
            default:
                // A garbage marker means the stream is desynced; poison
                // and classify as connection-level (issue #8) so the
                // retry layer redials instead of failing the op outright.
                throw new ConnectionLostException(
                    $"nanocached: unexpected response from server: {(char)marker}");
        }
    }

    private static uint ParseTag(string field)
    {
        if (!uint.TryParse(field, out uint tag))
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

    /// <summary>Reads up to (and consuming) the next '\n'.</summary>
    private async Task<string> ReadLineAsync()
    {
        var line = new StringBuilder();
        while (true)
        {
            byte b = await ReadByteAsync().ConfigureAwait(false);
            if (b == (byte)'\n') return line.ToString().Trim();
            if (line.Length >= MaxHeaderLineLength)
            {
                throw new ConnectionLostException("nanocached: response header line too long");
            }
            line.Append((char)b);
        }
    }
}
