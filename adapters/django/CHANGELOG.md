# Changelog

All notable changes to nanocached-django are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/); versions follow the
`adapter/django/vX.Y.Z` tags. Every release ships the server, the six SDKs and
the seven framework adapters at one version, whether or not a component
changed. What 1.0.0 will promise is described on the
[compatibility page](https://nanocached.org/compatibility.html).

## [Unreleased]

## [0.5.0] - 2026-10-26

### Changed

- `touch()` is the wire's own touch instead of a read followed by a full
  re-write: the TTL is replaced on the node in place, no value travels
  either way, and a write that lands at the same time is no longer
  overwritten with the pre-touch value. Django's `timeout=None` removes
  the TTL; `0` or a negative timeout still deletes the entry. Needs a
  server release that includes the `t` command and a Python SDK release
  that includes `touch`; against an older server `touch()` raises
  `ServerTooOldError`.

## [0.4.8] - 2026-10-21

### Fixed

- `LOCATION = "[::1]:8357"` connects: the brackets around an IPv6
  literal are stripped before the address reaches the SDK, which used to
  try to resolve `[::1]` as a host name and fail on the first cache call
  (#729).
- An `int` outside the signed 64-bit range (for example `10**4300`) is
  pickled like any other value instead of raising `ValueError` from the
  decimal fast path; `incr`/`decr` on such a key raise `NotNumericError`
  as for any non-counter value (#679).
- README: `add()` has been atomic (put-if-absent) since the wire gained
  compare-and-set; only `touch()` is get-then-set. The Python SDK
  requirement now matches `pyproject.toml`, the minimum server version
  is named, and new sections describe an unreachable node and the
  request size limit (#672, #736).

## [0.4.7] - 2026-10-19

No changes. Version aligned with the rest of the project: every release
ships the server, the six SDKs and the seven framework adapters at one
version, whether or not a component changed.

## [0.4.6] - 2026-10-17

No changes. Version aligned with the rest of the project: every release
ships the server, the six SDKs and the seven framework adapters at one
version, whether or not a component changed.

## [0.4.5] - 2026-10-15

### Fixed

- Every short-lived thread or ASGI request context used to build its own
  `NanocachedCache` with a private loop thread, client and sockets that
  were never released (`close()` is a no-op by default and the keepalive
  pings stop the server's idle timeout from reclaiming them), eventually
  exhausting the node connection limit. Instances with the same
  connection options now share one loop thread and client, closed at
  interpreter exit. `shutdown()` now applies to all instances sharing it;
  `CLOSE_ON_REQUEST` instances keep a private one.

## [0.4.4] - 2026-09-09

First release: a Django cache backend on the Python SDK.
