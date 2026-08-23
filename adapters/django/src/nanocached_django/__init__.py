"""Django cache backend for nanocached (issue #108).

See https://github.com/nanocached/nanocached for the server and protocol,
and this package's README for setup — adding the dependency alone changes
nothing; ``CACHES["default"]["BACKEND"]`` has to name ``NanocachedCache``.
"""

from .backend import NanocachedCache

__all__ = ["NanocachedCache"]

# Independent of the SDK core's own version (nanocached/__init__.py) —
# this adapter is versioned and released on its own, like the Spring
# adapter is on top of the Java SDK (issue #108 spec, "Policy note").
__version__ = "0.1.0"
