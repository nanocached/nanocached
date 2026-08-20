package nanocached

import "errors"

// Sentinel errors, matched with errors.Is.
var (
	// ErrClosed is returned by Get/Set/Delete after Close(); Close()
	// itself is idempotent.
	ErrClosed = errors.New("nanocached: this client is closed")

	// ErrWrongNode: a node answered `W` (ADR-0008) — per its own view of
	// cluster membership it doesn't hold this key, so the caller's
	// routing table is stale. The client catches this internally to
	// refresh the node list and retry once; it only escapes when that
	// retry also fails, or in single-node mode where there is no
	// discovery to refresh from.
	ErrWrongNode = errors.New("nanocached: this node does not hold the requested key")

	// ErrDiscoveryBusy: a discovery server answered `L` with `B` — it is
	// inside its startup grace (ADR-0010), re-learning membership after a
	// restart. Try another address, or retry shortly.
	ErrDiscoveryBusy = errors.New("nanocached: the discovery server is warming up after a restart")

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
	// (doc/adr/0013-*.md's compatibility caveat: every client touching a
	// given keyspace must agree on Compress), not a transient failure.
	ErrDecompression = errors.New("nanocached: failed to decompress a value")
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
