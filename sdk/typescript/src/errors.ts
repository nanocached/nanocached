/** Common base class for every error this SDK itself raises (issue #44):
 * `AlreadyClosedError`, `WrongNodeError`, `ConnectionLostError`,
 * `DecompressionError`, `DiscoveryBusyError`, and every protocol/auth
 * failure that has no more specific class — so callers can catch "an
 * expected nanocached failure" as one family (`instanceof
 * NanocachedError`), the way the other five SDKs' base
 * type/enum/sentinels already allow. A Node system error from the socket
 * itself (ECONNREFUSED, ENOTFOUND, ECONNRESET, ...) is wrapped in a
 * `ConnectionLostError` that carries it as `cause`, for a failed dial
 * (`socket.ts`) and for a socket that dies under an established
 * connection (`connection.ts`) alike. */
export class NanocachedError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "NanocachedError";
  }
}

/** The node answered `E` and is closing the connection (issue #825): a
 * command it does not know, or a request it rejected. Internal — it rides
 * as the `cause` of the `ConnectionLostError` a sent request fails with,
 * and `touch`/`getAndTouch` turn it into `ServerTooOldError`. */
export class ServerRejectedError extends NanocachedError {
  constructor() {
    super("nanocached: unexpected response from server: E");
    this.name = "ServerRejectedError";
  }
}
