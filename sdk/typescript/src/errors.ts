/** Common base class for every error this SDK itself raises (issue #44):
 * `AlreadyClosedError`, `WrongNodeError`, `ConnectionLostError`,
 * `DecompressionError`, `DiscoveryBusyError`, and every protocol/auth
 * failure that has no more specific class — so callers can catch "an
 * expected nanocached failure" as one family (`instanceof
 * NanocachedError`), the way the other five SDKs' base
 * type/enum/sentinels already allow. Node system errors from the socket
 * itself (ECONNREFUSED, ECONNRESET, ...) still surface as-is — they are
 * the platform's, not this SDK's — mirroring the Python SDK, whose
 * connection failures are builtin `ConnectionError`/`OSError` outside
 * its own base too. */
export class NanocachedError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "NanocachedError";
  }
}
