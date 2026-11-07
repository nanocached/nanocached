"""Django cache backend for nanocached (issue #108).

See https://github.com/nanocached/nanocached for the server and protocol,
and this package's README for setup — adding the dependency alone changes
nothing; ``CACHES["default"]["BACKEND"]`` has to name ``NanocachedCache``.
"""

from .backend import NanocachedCache

__all__ = ["NanocachedCache"]
