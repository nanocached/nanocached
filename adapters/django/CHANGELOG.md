# Changelog

All notable changes to nanocached-django are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/); versions follow the
`adapter/django/vX.Y.Z` tags. Every release ships the server, the six SDKs and
the seven framework adapters at one version, whether or not a component
changed. What 1.0.0 will promise is described on the
[compatibility page](https://nanocached.org/compatibility.html).

## [Unreleased]

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
