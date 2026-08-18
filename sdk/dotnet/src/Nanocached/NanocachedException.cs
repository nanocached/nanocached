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
/// A node answered <c>W</c> (ADR-0008): per its own view of cluster
/// membership it doesn't hold this key — the caller's routing table is
/// stale. The client catches this internally to refresh the node list and
/// retry once; it only escapes when that retry also fails, or in
/// single-node mode where there is no discovery to refresh from.
/// </summary>
public sealed class WrongNodeException : NanocachedException
{
    public WrongNodeException() : base("nanocached: this node does not hold the requested key") { }
}

/// <summary>
/// A discovery server answered <c>L</c> with <c>B</c> — it is inside its
/// startup grace (ADR-0010), re-learning membership after a restart. Try
/// another address, or retry shortly.
/// </summary>
public sealed class DiscoveryBusyException : NanocachedException
{
    public DiscoveryBusyException()
        : base("nanocached: the discovery server is warming up after a restart") { }
}

/// <summary>A connection-level failure; the client redials lazily on the next use.</summary>
public sealed class ConnectionLostException : NanocachedException
{
    public ConnectionLostException(string message) : base(message) { }

    public ConnectionLostException(string message, Exception inner) : base(message, inner) { }
}
