"""nanocached — asyncio client SDK for the nanocached distributed cache.

See https://github.com/nanocached/nanocached for the server and protocol.
"""

from ._compression import DecompressionError
from ._digest import content_digest
from ._errors import (
    AlreadyClosedError,
    AuthenticationError,
    CompressionIncompatibleError,
    ConnectionLostError,
    DiscoveryBusyError,
    NanocachedError,
    NotNumericError,
    PartialConnectionLostError,
    PartialSetConnectionLostError,
    PartialWrongNodeError,
    RetryableError,
    WrongNodeError,
)
from ._hashring import HashRing
from ._identify import DiscoveredNode
from .client import ClientStats, NanocachedClient, Namespace

__all__ = [
    "AlreadyClosedError",
    "AuthenticationError",
    "ClientStats",
    "CompressionIncompatibleError",
    "ConnectionLostError",
    "DecompressionError",
    "DiscoveredNode",
    "DiscoveryBusyError",
    "HashRing",
    "Namespace",
    "NanocachedClient",
    "NanocachedError",
    "NotNumericError",
    "PartialConnectionLostError",
    "PartialSetConnectionLostError",
    "PartialWrongNodeError",
    "RetryableError",
    "WrongNodeError",
    "content_digest",
]

# Must match sdk/python/pyproject.toml's `version` — tools/release-all.sh
# verifies both before tagging a release.
__version__ = "0.3.0"
