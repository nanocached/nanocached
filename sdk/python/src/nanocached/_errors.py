"""Public exception types. All extend ``NanocachedError`` so callers can
catch the SDK's failures with one clause when they don't care which."""

from __future__ import annotations


class NanocachedError(Exception):
    """Base class for every error this SDK raises on its own behalf."""


class AlreadyClosedError(NanocachedError):
    """Raised by get/set/delete after close(). close() itself is idempotent."""

    def __init__(self) -> None:
        super().__init__("nanocached: this client is closed")


class WrongNodeError(NanocachedError):
    """A node answered ``W`` (staged node join): per its own view of cluster
    membership it doesn't hold this key — the caller's routing table is
    stale. The client catches this internally to refresh the node list and
    retry once; it only escapes when that retry also fails, or in
    single-node mode where there is no discovery to refresh from."""

    def __init__(self) -> None:
        super().__init__("nanocached: this node does not hold the requested key")


class PartialWrongNodeError(WrongNodeError):
    """Raised by get_many/get_many_bytes (issues #128/#150/#151) when some
    keys are still wrong-node after the one bounded refresh-and-retry
    every batch gets (see NanocachedClient's own multi-get docstring). A
    subclass of WrongNodeError, so existing ``except WrongNodeError:``
    handling keeps working unchanged; ``partial_values`` holds every key
    that DID resolve, keyed by the original object the caller passed, so
    a caller who wants a mostly-successful batch's data instead of
    discarding it can still get at it. set_many has nothing partial
    worth attaching on this condition and raises a plain WrongNodeError
    instead."""

    def __init__(self, partial_values: dict) -> None:
        super().__init__()
        self.partial_values = partial_values


class AuthenticationError(NanocachedError):
    """The server rejected the ``A`` handshake's secret — either no
    ``auth_secret`` was configured for a server that requires one, or the
    configured secret is wrong. Never transient: retrying with the same
    configuration cannot succeed."""


class DiscoveryBusyError(NanocachedError):
    """A discovery server answered ``L`` with ``B`` — it is inside its
    startup grace (discovery HA), re-learning membership after a restart. Try
    another address, or retry shortly."""

    def __init__(self) -> None:
        super().__init__("nanocached: the discovery server is busy: warming up after a restart, or its replication factor disagrees with the cluster's")


class NotNumericError(NanocachedError):
    """Raised by incr/decr (issue #129) when the stored value isn't an
    integer INCR can operate on (it wasn't written by a previous
    incr/decr and isn't itself decimal-integer text), or applying
    ``delta`` would overflow a signed 64-bit integer — the wire's ``T``
    response. The key is left untouched; unlike a dead replica, this is
    never swallowed or retried."""

    def __init__(self) -> None:
        super().__init__(
            "nanocached: the stored value is not numeric (or the increment would overflow)"
        )


class CompressionIncompatibleError(NanocachedError):
    """Raised by incr/decr (issue #321) on a client constructed with
    ``compress=True``, before any I/O. The protocol has no marker byte
    on the wire result of an increment, so a compress-enabled client
    forwarding that result to replicas as an ordinary set() would write
    an unmarked value get() then unconditionally tries to decompress
    (and incr on a value a compress-enabled client already wrote fails
    server-side with NotNumericError, since it's DEFLATE bytes, not
    decimal text) — there is no way for the SDK to make this work, so
    it raises instead of quietly corrupting the keyspace. Disable
    compress, or use a separate, compress-disabled client for counters."""

    def __init__(self) -> None:
        super().__init__(
            "nanocached: incr/decr is incompatible with value compression "
            "(disable compress, or use a separate client for counters)"
        )


class ConnectionLostError(NanocachedError, ConnectionError):
    """A request's connection failed *after* its frame was already fully
    written to the socket (issue #225) — write()/drain() returned
    normally, so the server may have received and applied it before the
    reply was lost (a dropped/crashed connection, the server closing
    mid-response, a malformed or misaligned reply, ...). Distinct from
    the ordinary (plain ``ConnectionError``) case where the request was
    never sent at all — the connection was already dead (an idle FIN),
    the dial itself failed, or the write/drain call raised outright —
    which is always safe to redial and resend.

    get/set/delete/clear/get_many/set_many treat this exactly like any
    other connection failure and transparently retry after a redial:
    they're idempotent (or, for clear, already a no-op when repeated),
    so resending is always safe either way. incr/decr and the CAS
    operations (replace()/replace_if_present()/put_if_absent()/
    delete_if_matches()) are not: replaying an already-applied ``i``
    would double the increment, and replaying an already-applied ``k``/
    ``x`` would misreport a just-succeeded CAS as a mismatch. Their own
    retry wrapper (NanocachedClient._with_wrong_node_retry,
    replay_safe=False) therefore lets this subclass escape instead of
    retrying — see each of those methods' own docstring for the
    resulting at-least-once caveat."""

    def __init__(self, message: str) -> None:
        super().__init__(message)


class PartialConnectionLostError(NanocachedError, ConnectionError):
    """Raised by get_many/get_many_bytes (issue #411) when a batch spans
    more than one ``m`` sub-frame (batch chunking, issue #222) and a
    later chunk's connection failure escapes without being retried — in
    single-node/proxy mode there is no ring to refresh against and retry
    from (mirroring WrongNodeError's own single-mode stance), so this is
    only ever raised there — after at least one earlier chunk already
    succeeded over the same connection. Mirrors PartialWrongNodeError's
    shape for this related but distinct failure mode: ``partial_values``
    holds every key that DID resolve from the chunk(s) that succeeded
    before the failure, keyed by the original object the caller passed,
    so a mostly-successful batch's data isn't discarded behind one late
    chunk's connection error.

    Same multiple-inheritance shape as ConnectionLostError above
    (NanocachedError, ConnectionError) rather than subclassing it: the
    exception that actually escapes a failing chunk is not always a
    ConnectionLostError specifically (issue #225's narrower "the request
    may have already reached the server" case) — it may just as well be
    a plain builtin ConnectionError/OSError (e.g. the chunk's connection
    was already dead, or the redial for a later chunk itself failed) —
    raised with ``from`` the original error either way, so it's always
    reachable via ``__cause__``.

    Only raised when a chunk *after* the first has already succeeded; a
    failure on the very first chunk has no partial data to attach and
    still raises that original exception unwrapped, exactly as before
    this fix. get_many()'s own PartialConnectionLostError carries str
    values (UTF-8 decoded); get_many_bytes() carries the raw bytes."""

    def __init__(self, partial_values: dict, message: str) -> None:
        super().__init__(message)
        self.partial_values = partial_values


class PartialSetConnectionLostError(NanocachedError, ConnectionError):
    """set_many's analog of PartialConnectionLostError above (issue
    #411): unlike PartialWrongNodeError's write-side sibling — a plain
    WrongNodeError, since a successful set() has no value to report —
    THIS condition does have something meaningful to attach: which keys
    were already confirmed stored (by a chunk whose ``o`` sub-frame
    completed) before a later chunk's connection failure escaped,
    single-node/proxy mode only, exactly like PartialConnectionLostError
    (there is no ring to refresh against and retry from). ``partial_keys``
    holds every key the batch DID store, as the original object the
    caller passed — not a dict, since a successful set carries no value
    worth returning, only the fact that it landed. Same rule as its read
    sibling: a failure on the very first chunk has nothing to attach and
    still raises the original exception unwrapped; the original error is
    always reachable via ``__cause__``."""

    def __init__(self, partial_keys: set, message: str) -> None:
        super().__init__(message)
        self.partial_keys = partial_keys


class RetryableError(NanocachedError):
    """A single request was answered ``R`` (issue #125) three times
    running — the connection's bounded transient-retry budget (2 retries,
    3 attempts total, sleeping 50ms then 100ms between attempts) was
    exhausted without a non-``R`` answer. ``R`` means the request itself
    failed transiently (e.g. nanocached-proxy's upstream node was briefly
    unreachable across every retry) while the connection stayed healthy
    the whole time — this is never a connection error, a ``W``, or an
    ``E``, so it never triggers this SDK's reconnect/node-list-refresh
    machinery. The connection remains open and usable; the caller decides
    whether and when to retry the call itself."""

    def __init__(self) -> None:
        super().__init__(
            "nanocached: the server answered R three times in a row for this request "
            "(transient-retry budget exhausted); the connection is still usable"
        )
