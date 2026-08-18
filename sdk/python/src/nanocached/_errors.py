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
    """A node answered ``W`` (ADR-0008): per its own view of cluster
    membership it doesn't hold this key — the caller's routing table is
    stale. The client catches this internally to refresh the node list and
    retry once; it only escapes when that retry also fails, or in
    single-node mode where there is no discovery to refresh from."""

    def __init__(self) -> None:
        super().__init__("nanocached: this node does not hold the requested key")


class DiscoveryBusyError(NanocachedError):
    """A discovery server answered ``L`` with ``B`` — it is inside its
    startup grace (ADR-0010), re-learning membership after a restart. Try
    another address, or retry shortly."""

    def __init__(self) -> None:
        super().__init__("nanocached: the discovery server is warming up after a restart")
