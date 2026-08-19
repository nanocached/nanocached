"""nanocached — asyncio client SDK for the nanocached distributed cache.

See https://github.com/nanocached/nanocached for the server and protocol.
"""

from ._compression import DecompressionError
from ._errors import (
    AlreadyClosedError,
    DiscoveryBusyError,
    NanocachedError,
    WrongNodeError,
)
from ._hashring import HashRing
from ._identify import DiscoveredNode
from .client import NanocachedClient

__all__ = [
    "AlreadyClosedError",
    "DecompressionError",
    "DiscoveredNode",
    "DiscoveryBusyError",
    "HashRing",
    "NanocachedClient",
    "NanocachedError",
    "WrongNodeError",
]

__version__ = "1.0.0"
