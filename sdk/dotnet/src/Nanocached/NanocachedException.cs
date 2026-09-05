namespace Nanocached;

/// <summary>Base class for every error this SDK raises on its own behalf.</summary>
public class NanocachedException : Exception
{
    public NanocachedException(string message) : base(message) { }

    public NanocachedException(string message, Exception inner) : base(message, inner) { }
}

/// <summary>Raised by get/set/delete after Close(); Close() itself is idempotent.</summary>
public sealed class AlreadyClosedException : NanocachedException
{
    public AlreadyClosedException() : base("nanocached: this client is closed") { }
}

/// <summary>
/// A node answered <c>W</c> (staged node join): per its own view of cluster
/// membership it doesn't hold this key — the caller's routing table is
/// stale. The client catches this internally to refresh the node list and
/// retry once; it only escapes when that retry also fails, or in
/// single-node mode where there is no discovery to refresh from.
///
/// <para>Not <c>sealed</c> (unlike this SDK's other leaf exceptions) so
/// <see cref="PartialWrongNodeException{T}"/> — issue #151's batched-get
/// partial-failure carrier — can subclass it while still satisfying every
/// existing <c>catch (WrongNodeException)</c>.</para>
/// </summary>
public class WrongNodeException : NanocachedException
{
    public WrongNodeException() : base("nanocached: this node does not hold the requested key") { }
}

/// <summary>
/// issue #151 — raised by <c>NanocachedClient.GetManyAsync</c>/
/// <c>NanocachedClient.GetManyBytesAsync</c> when some keys are
/// still wrong-node after the one bounded refresh-and-retry every batch
/// gets (the per-key analogue of <c>GetAsync</c>'s own <c>W</c>
/// refresh-and-retry). A <see cref="WrongNodeException"/> subclass, so
/// existing <c>catch (WrongNodeException)</c> handling keeps working
/// unchanged; <see cref="PartialValues"/> holds every key that DID
/// resolve — a batch never fails as a whole (docs/protocol.html#multi),
/// so a handful of stale placements shouldn't force discarding an
/// otherwise successful batch. <c>NanocachedClient.SetManyAsync</c>/
/// <c>NanocachedClient.SetManyBytesAsync</c> have nothing to
/// return on success, so they just throw a plain
/// <see cref="WrongNodeException"/> on the same condition.
/// </summary>
public sealed class PartialWrongNodeException<T> : WrongNodeException
{
    public T PartialValues { get; }

    public PartialWrongNodeException(T partialValues)
    {
        PartialValues = partialValues;
    }
}

/// <summary>
/// The server rejected the <c>A</c> handshake's secret — either no
/// <c>AuthSecret</c> was configured for a server that requires one, or
/// the configured secret is wrong. Never transient: retrying with the
/// same configuration cannot succeed.
/// </summary>
public sealed class AuthenticationFailedException : NanocachedException
{
    public AuthenticationFailedException(string message) : base(message) { }
}

/// <summary>
/// A discovery server answered <c>L</c> with <c>B</c> — it is inside its
/// startup grace (discovery HA), re-learning membership after a restart. Try
/// another address, or retry shortly.
/// </summary>
public sealed class DiscoveryBusyException : NanocachedException
{
    public DiscoveryBusyException()
        : base("nanocached: the discovery server is busy: warming up after a restart, or its replication factor disagrees with the cluster's") { }
}

/// <summary>A connection-level failure; the client redials lazily on the next use.
///
/// <para>Not <c>sealed</c> (unlike this SDK's other leaf exceptions) so
/// <see cref="PartialConnectionLostException{T}"/> — issue #411's
/// chunked-batch partial-failure carrier — can subclass it while still
/// satisfying every existing <c>catch (ConnectionLostException)</c>, the
/// same reason <see cref="WrongNodeException"/> is deliberately left
/// unsealed for <see cref="PartialWrongNodeException{T}"/>.</para>
/// </summary>
public class ConnectionLostException : NanocachedException
{
    /// <summary>
    /// issue #225 / #484 — internal only: true when this SDK can prove no
    /// complete request frame reached the peer — the connection was
    /// already closed before the frame was written (Connection's
    /// <c>IsClosed</c> pre-write checks), or <c>WriteAsync</c> itself failed
    /// while the connection was still this SDK's own (a failed write leaves
    /// at most a truncated frame, which the server never executes). False
    /// (the default, used by every other throw site — a lost reply after a
    /// fully-written frame, a <c>FlushAsync</c> failure after the write
    /// completed, a request timeout, a stream desync, or a write that
    /// failed because a concurrent <c>Close</c> disposed the stream under
    /// it) means the request may already have reached the peer and possibly
    /// been applied. <see cref="NanocachedClient"/>
    /// uses this to decide whether replaying a non-idempotent request
    /// (Incr/CAS/RemoveIfMatches) after a redial is safe: true is exactly
    /// as safe as retrying an idempotent Get/Set/Delete; false is not — see
    /// NanocachedClient's ApplyReconnectingNotIdempotentAsync.
    /// </summary>
    internal bool RequestNotSent { get; }

    public ConnectionLostException(string message) : base(message) { }

    public ConnectionLostException(string message, Exception inner) : base(message, inner) { }

    internal ConnectionLostException(string message, bool requestNotSent) : base(message)
    {
        RequestNotSent = requestNotSent;
    }

    internal ConnectionLostException(string message, Exception inner, bool requestNotSent) : base(message, inner)
    {
        RequestNotSent = requestNotSent;
    }
}

/// <summary>
/// issue #411 — raised by <c>NanocachedClient.GetManyBytesAsync</c>/
/// <c>NanocachedClient.SetManyBytesAsync</c> (and their decoded-string
/// siblings) when a connection failure interrupts a chunked multi-get/
/// multi-set batch (batch chunking, issue #222: <c>MultiGetChunkedAsync</c>/
/// <c>MultiSetChunkedAsync</c> split an over-<c>MaxBatchKeys</c> or
/// over-<c>MaxRequestBytes</c> batch into more than one <c>m</c>/<c>o</c>
/// wire sub-frame) after at least one earlier sub-frame already completed —
/// and this SDK's own built-in reconnect-and-retry
/// (<c>ApplyReconnectingAsync</c>) already tried and failed on the
/// sub-frame that lost the connection. A <see cref="ConnectionLostException"/>
/// subclass — chaining the original failure as <see cref="Exception.InnerException"/> —
/// so existing <c>catch (ConnectionLostException)</c> handling keeps working
/// unchanged, mirroring how <see cref="PartialWrongNodeException{T}"/>
/// subclasses <see cref="WrongNodeException"/> for the analogous wrong-node
/// case. <see cref="PartialValues"/> holds whatever the batch DID confirm
/// before the failing sub-frame: for get, every key that resolved
/// (<c>T</c> = <c>Dictionary&lt;string, byte[]&gt;</c>, exactly like
/// <see cref="PartialWrongNodeException{T}"/>'s own <c>T</c>); for set,
/// since there's nothing to "return" on success, every key confirmed
/// stored instead (<c>T</c> = <c>HashSet&lt;string&gt;</c>).
///
/// <para>If the very FIRST sub-frame is the one that fails, there is no
/// partial data yet, so a plain (bare) <see cref="ConnectionLostException"/>
/// propagates instead — this type is only thrown once a second or later
/// sub-frame fails.</para>
/// </summary>
public sealed class PartialConnectionLostException<T> : ConnectionLostException
{
    public T PartialValues { get; }

    public PartialConnectionLostException(T partialValues, Exception inner)
        : base(
            "nanocached: connection lost partway through a chunked batch; " +
            "PartialValues holds what the batch confirmed before the failing chunk",
            inner)
    {
        PartialValues = partialValues;
    }
}

/// <summary>
/// Raised by GetAsync/GetBytesAsync when a value with <c>Compress</c>
/// enabled can't be interpreted — almost always a <c>Compress</c>
/// mismatch between clients sharing this key (value compression's
/// compatibility caveat: every client touching a given keyspace must
/// agree on <c>Compress</c>), not a transient failure.
/// </summary>
public sealed class DecompressionException : NanocachedException
{
    public DecompressionException(string message) : base(message) { }

    public DecompressionException(string message, Exception inner) : base(message, inner) { }
}

/// <summary>
/// issue #125 — retryable-error status <c>R</c>: a single request answered
/// <c>R</c> (this request failed transiently — e.g. the proxy's upstream
/// node was briefly unreachable — but the connection itself is fine) on
/// every one of its bounded attempts (an initial attempt plus up to 2
/// transparent retries on the same connection, 50ms then 100ms apart).
/// Unlike every other <see cref="NanocachedException"/> raised by a bad
/// response, this one never closes or redials the connection — <c>R</c>
/// is not a connection error, not a <c>W</c>, not an <c>E</c> — the
/// connection remains usable for the caller's next operation. Every
/// <c>R</c> received, whether it led to this exception or was transparently
/// retried away, is counted in <see cref="NanocachedClient.ClientStats.TransientRetries"/>.
/// </summary>
public sealed class RetryableException : NanocachedException
{
    public RetryableException(string message) : base(message) { }
}

/// <summary>
/// issue #129 — INCR/DECR: the target key exists but its stored value
/// isn't INCR's counter grammar (a decimal ASCII signed 64-bit integer),
/// or applying the delta would overflow one (<c>T</c>). Never transient:
/// retrying the same INCR against the same stored value cannot succeed
/// until something else (a Set, a Delete, or a differently-shaped Incr)
/// changes it first.
/// </summary>
public sealed class NotNumericException : NanocachedException
{
    public NotNumericException()
        : base("nanocached: the stored value is not an integer INCR can operate on") { }
}

/// <summary>
/// issue #321 — INCR/DECR is structurally incompatible with <c>Compress</c>:
/// a successful increment forwards the primary's literal ASCII result to
/// replicas as an unmarked <c>Set</c>, while <c>Compress</c> unconditionally
/// runs decompression on every subsequent <c>Get</c>, so reading that key
/// back always fails (and incrementing a value <c>Compress</c> itself wrote
/// fails <see cref="NotNumericException"/> instead). The server never
/// interprets values, so this can't be fixed with a wire-level marker byte
/// alone — raised immediately, before any I/O, rather than let either
/// failure mode surface later. Disable <c>Compress</c> on the client used
/// for counters, or use a separate client for keys that need compression.
/// </summary>
public sealed class CompressionIncompatibleException : NanocachedException
{
    public CompressionIncompatibleException()
        : base("nanocached: incr/decr is incompatible with value compression (disable Compress or use a separate client for counters)") { }
}
