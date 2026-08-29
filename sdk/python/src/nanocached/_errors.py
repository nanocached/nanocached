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
