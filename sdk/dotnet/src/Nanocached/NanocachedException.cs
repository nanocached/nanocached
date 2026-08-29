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

/// <summary>A connection-level failure; the client redials lazily on the next use.</summary>
public sealed class ConnectionLostException : NanocachedException
{
    public ConnectionLostException(string message) : base(message) { }

    public ConnectionLostException(string message, Exception inner) : base(message, inner) { }
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
