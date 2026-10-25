package nanocached

import (
	"errors"
	"fmt"
)

// RemoveTTL, passed as ttlSeconds to Touch/GetAndTouch (issue #825),
// removes the entry's TTL so it never expires. It is a named constant
// rather than a bare -1 because the parameter has no default: 0 is a real
// value there (expire the entry at once), the opposite of Set, where 0
// means no expiry.
const RemoveTTL int64 = -1

// Touch replaces the TTL of an existing entry in place, without rewriting
// its value (issue #825), and reports whether the key existed: false for a
// missing or expired key, which a touch never creates. ttlSeconds is
// counted from the moment the node receives the request — RemoveTTL
// removes the TTL, 0 expires the entry at once, and any other negative
// value is rejected before any I/O (ErrInvalidArgument).
//
// Unlike Incr and the compare-and-set family a touch is idempotent, so a
// request whose reply was lost is retried like Get/Set/Delete are — with
// one exception (issue #901): a 0 TTL expires the entry, so a resend after
// the node already applied it would answer false for a key that existed.
// That call is retried only when its request was never sent, and a reply
// lost after the send returns ErrConnectionLost, as Incr does. In a cluster
// it goes to every owner, like a Set, and the primary's answer is returned —
// except that a 0 TTL goes to the primary alone first and to the other
// owners only after the primary answered (issue #934): a replica that had
// expired its copy before the primary failed or answered `W` would leave the
// reroute, which makes it the primary, nothing to find. Those replica legs
// therefore run after the primary answered, outside the pipelined-order
// guarantee, the same class as the retry paths.
// Returns ErrServerTooOld against a node that predates the command.
func (c *Client) Touch(key string, ttlSeconds int64) (existed bool, err error) {
	return c.touchNS(nil, key, ttlSeconds)
}

// GetAndTouch reads a value and replaces its TTL in one step (issue #825);
// ok is false on a miss, which creates nothing. See Touch for ttlSeconds
// (RemoveTTL removes the TTL, 0 expires the entry at once).
//
// A read that writes: it goes to the key's primary like a write (one
// refresh and retry on a wrong-node answer or a connection failure; no
// hedging, and no fallback to a replica on a miss) and the other owners are
// sent the plain touch, so a replica is never the only owner touched. It
// counts as a read on the primary, which makes the entry the most recently
// used.
//
// With ttlSeconds == 0 a reply lost after the request was sent returns
// ErrConnectionLost instead of being resent (issue #901): the node may have
// expired the entry and consumed its value, and a resend would report a
// miss. The plain touch to the other owners then follows the primary's
// answer instead of racing it (issue #934), so a reroute after a failed or
// stale-ring primary still reads the value from the replica.
func (c *Client) GetAndTouch(key string, ttlSeconds int64) (value string, ok bool, err error) {
	raw, ok, err := c.GetAndTouchBytes(key, ttlSeconds)
	if err != nil || !ok {
		return "", ok, err
	}
	return string(raw), true, nil
}

// GetAndTouchBytes is GetAndTouch's raw companion. Transparently
// decompresses when Config.Compress is enabled.
func (c *Client) GetAndTouchBytes(key string, ttlSeconds int64) (value []byte, ok bool, err error) {
	return c.getAndTouchBytesNS(nil, key, ttlSeconds)
}

// validateTouchTTL is the argument check Touch/GetAndTouch share: RemoveTTL
// or a non-negative number of seconds.
func validateTouchTTL(ttlSeconds int64) error {
	if ttlSeconds < 0 && ttlSeconds != RemoveTTL {
		return invalidArgument(fmt.Sprintf(
			"nanocached: ttlSeconds must not be negative (pass RemoveTTL to remove the TTL), got %d", ttlSeconds))
	}
	return nil
}

// touchRejected turns the node's `En` into ErrServerTooOld (issue #825),
// leaving every other error as it was.
func touchRejected(err error) error {
	if err != nil && errors.Is(err, errServerRejected) {
		return errors.Join(ErrServerTooOld, err)
	}
	return err
}

// touchRetryPolicy picks how Touch/GetAndTouch resend (issue #901). A touch
// is idempotent, so the default is withClusterRetry/applyReconnecting like
// Get/Set/Delete — except ttlSeconds == 0 (not RemoveTTL, which is -1),
// which expires the entry: a resend after the node applied it would find
// the key gone, so that call uses the Incr family's not-sent boundary
// instead (withClusterRetryNonIdempotent, applyExpiring).
func (c *Client) touchRetryPolicy(ttlSeconds int64) (
	retry func(func() error) error,
	applyPrimary func(slot string, op func(*connection) error) error,
) {
	if ttlSeconds == 0 {
		return c.withClusterRetryNonIdempotent, c.applyExpiring
	}
	return c.withClusterRetry, c.applyReconnecting
}

// touchNS is Touch scoped to namespace — see getBytesNS's doc comment; the
// same internal (namespace, key) entry point a *Namespace handle's Touch
// forwards to.
func (c *Client) touchNS(namespace []byte, key string, ttlSeconds int64) (existed bool, err error) {
	if err := validateKey(namespace, key); err != nil {
		return false, err
	}
	if err := validateTouchTTL(ttlSeconds); err != nil {
		return false, err
	}
	if err := c.beforeOperation(); err != nil {
		return false, err
	}
	keyBytes := []byte(key)
	retry, applyPrimary := c.touchRetryPolicy(ttlSeconds)
	err = retry(func() error {
		return c.writeWith(namespace, keyBytes, func(conn *connection, primary bool) error {
			e, opErr := conn.touchNS(namespace, keyBytes, ttlSeconds)
			// Only the primary's answer decides, and only the primary leg
			// may write `existed` — see deleteNS.
			if primary {
				existed = e
			}
			return opErr
		}, applyPrimary, ttlSeconds == 0)
	})
	return existed, touchRejected(err)
}

// getAndTouchBytesNS is GetAndTouchBytes scoped to namespace.
func (c *Client) getAndTouchBytesNS(namespace []byte, key string, ttlSeconds int64) (value []byte, ok bool, err error) {
	if err := validateKey(namespace, key); err != nil {
		return nil, false, err
	}
	if err := validateTouchTTL(ttlSeconds); err != nil {
		return nil, false, err
	}
	if err := c.beforeOperation(); err != nil {
		return nil, false, err
	}
	keyBytes := []byte(key)
	retry, applyPrimary := c.touchRetryPolicy(ttlSeconds)
	err = retry(func() error {
		value, ok = nil, false
		return c.writeWith(namespace, keyBytes, func(conn *connection, primary bool) error {
			if !primary {
				// A replica is sent the plain touch: it has no value to
				// return, and `G` there would only move the value over the
				// wire for nothing.
				_, opErr := conn.touchNS(namespace, keyBytes, ttlSeconds)
				return opErr
			}
			v, found, opErr := conn.getAndTouchNS(namespace, keyBytes, ttlSeconds)
			value, ok = v, found
			return opErr
		}, applyPrimary, ttlSeconds == 0)
	})
	if err != nil {
		return nil, false, touchRejected(err)
	}
	if !ok || !c.compress {
		return value, ok, nil
	}
	value, err = decompressValue(value, c.inflate)
	return value, ok, err
}

// Touch replaces the TTL of an existing entry within this namespace. See
// Client.Touch.
func (n *Namespace) Touch(key string, ttlSeconds int64) (existed bool, err error) {
	return n.client.touchNS(n.namespace, key, ttlSeconds)
}

// GetAndTouch reads a value within this namespace and replaces its TTL. See
// Client.GetAndTouch.
func (n *Namespace) GetAndTouch(key string, ttlSeconds int64) (value string, ok bool, err error) {
	raw, ok, err := n.client.getAndTouchBytesNS(n.namespace, key, ttlSeconds)
	if err != nil || !ok {
		return "", ok, err
	}
	return string(raw), true, nil
}

// GetAndTouchBytes is GetAndTouch's raw companion within this namespace.
// See Client.GetAndTouchBytes.
func (n *Namespace) GetAndTouchBytes(key string, ttlSeconds int64) (value []byte, ok bool, err error) {
	return n.client.getAndTouchBytesNS(n.namespace, key, ttlSeconds)
}
