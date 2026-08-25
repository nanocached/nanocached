package nanocached

import "errors"

// Sentinel errors, matched with errors.Is.
var (
	// ErrClosed is returned by Get/Set/Delete after Close(); Close()
	// itself is idempotent.
	ErrClosed = errors.New("nanocached: this client is closed")

	// ErrWrongNode: a node answered `W` (staged node join) — per its own view of
	// cluster membership it doesn't hold this key, so the caller's
	// routing table is stale. The client catches this internally to
	// refresh the node list and retry once; it only escapes when that
	// retry also fails, or in single-node mode where there is no
	// discovery to refresh from.
	ErrWrongNode = errors.New("nanocached: this node does not hold the requested key")

	// ErrDiscoveryBusy: a discovery server answered `L` with `B` — it is
	// inside its startup grace (discovery HA), re-learning membership after a
	// restart. Try another address, or retry shortly.
	ErrDiscoveryBusy = errors.New("nanocached: the discovery server is busy: warming up after a restart, or its replication factor disagrees with the cluster's")

	// ErrConnectionLost wraps connection-level failures; the client
	// redials lazily on the next use, and in cluster mode retries once
	// through a node-list refresh.
	ErrConnectionLost = errors.New("nanocached: connection lost")

	// ErrAuthenticationFailed: the server rejected the A handshake's
	// secret — either no AuthSecret was configured for a server that
	// requires one, or the configured secret is wrong. Never transient:
	// retrying with the same configuration cannot succeed (issue #47).
	ErrAuthenticationFailed = errors.New("nanocached: authentication failed")

	// ErrDecompression is returned by Get/GetBytes when a value with
	// Config.Compress enabled can't be interpreted — almost always a
	// Compress mismatch between clients sharing this key
	// (value compression's compatibility caveat: every client touching a
	// given keyspace must agree on Compress), not a transient failure.
	ErrDecompression = errors.New("nanocached: failed to decompress a value")

	// ErrProtocol wraps a malformed or unexpected response frame — the
	// server sent bytes that don't parse as the wire protocol at all
	// (a garbage value header/length, an unparsable tag, an unknown
	// marker byte, an over-long header line), as opposed to a genuine
	// I/O failure (EOF, reset, timeout) that just happens to also end
	// the connection. Both still poison the connection the same way —
	// this only changes the error TYPE a caller sees, not the "this
	// connection is dead" handling: a corrupt frame is arguably not
	// transient in the way a dropped socket is, so callers that want to
	// tell the two apart can (issue #47 audit item G4; Rust's
	// Error::Protocol and TypeScript's bare NanocachedError vs.
	// ConnectionLostError draw the same line).
	ErrProtocol = errors.New("nanocached: protocol violation")

	// ErrInvalidArgument is returned by client-side argument validation
	// that runs before any network I/O: an empty or oversized key, a
	// key+value pair too large for the server's own request cap, or an
	// invalid TTL. Never transient — retrying with the same arguments
	// cannot succeed.
	ErrInvalidArgument = errors.New("nanocached: invalid argument")

	// ErrRetryable is returned when a single request was answered `R`
	// (issue #125) on every attempt of the bounded retry budget — the
	// server (today, only nanocached-proxy) reported the request itself
	// failed transiently (e.g. its upstream node was briefly unreachable)
	// and asked for a retry, but that still didn't succeed within 3
	// attempts. Unlike ErrConnectionLost, the connection itself is fine
	// and stays open and usable — this is a per-request outcome, not a
	// connection-level failure, so a later call on the same connection
	// may still succeed. Possible on any connection: the SDK always
	// probes for the `R` capability during identify (see identify.go),
	// regardless of what the caller configured.
	ErrRetryable = errors.New("nanocached: request failed transiently and retries were exhausted")
)

func connectionLost(context string, cause error) error {
	if cause != nil {
		return errors.Join(ErrConnectionLost, errors.New(context+": "+cause.Error()))
	}
	return errors.Join(ErrConnectionLost, errors.New(context))
}

func decompressionFailed(context string) error {
	return errors.Join(ErrDecompression, errors.New(context))
}

func protocolError(context string) error {
	return errors.Join(ErrProtocol, errors.New(context))
}

func invalidArgument(context string) error {
	return errors.Join(ErrInvalidArgument, errors.New(context))
}

func retryableFailed(context string) error {
	return errors.Join(ErrRetryable, errors.New(context))
}
