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
)

func connectionLost(context string, cause error) error {
	if cause != nil {
		return errors.Join(ErrConnectionLost, errors.New(context+": "+cause.Error()))
	}
	return errors.Join(ErrConnectionLost, errors.New(context))
}
