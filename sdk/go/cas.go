package nanocached

// Compare-and-set (issue #141): PutIfAbsent/ReplaceIfPresent/Replace
// (the `k` frame) and DeleteIfMatches (the `x` frame) — see
// docs/protocol.html#cas. Follows INCR's own pattern (issue #129)
// exactly: always namespaced, no legacy uppercase form, and — in a
// cluster — only the key's primary owner evaluates the condition, with
// the literal result fanned out to the remaining owners as an ordinary
// Set/Delete (see cas and deleteIfMatches below).
//
// This is content-based CAS, not a distributed lock: LRU eviction
// reclaims a key exactly as it would after a plain Set, CAS or not. A key
// used as a lock (PutIfAbsent to acquire, a TTL to eventually release)
// that gets evicted under memory pressure leaves a second caller's
// PutIfAbsent free to succeed while the first caller still believes it
// holds the lock — a silent double-acquisition CAS cannot detect.
// PutIfAbsent/ReplaceIfPresent/Replace/DeleteIfMatches are atomic against
// concurrent requests on the node that currently owns the key, the same
// guarantee Incr/Decr make and no stronger.

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
)

// casCondAbsent and casCondPresent are the `k` frame's two bare-token
// <cond> forms (docs/protocol.html#cas) — everything else is a
// 32-character lowercase hex digest (CasToken.Hex).
const (
	casCondAbsent  = "A"
	casCondPresent = "P"
)

// CasToken is an opaque compare-and-set token (issue #141): the first 16
// bytes (128 bits) of the SHA-256 digest of a key's exact stored wire
// bytes, obtained from GetWithToken and consumed by Replace/
// DeleteIfMatches to condition a write on that exact content. The zero
// CasToken is never a valid token for any real key — it's only ever
// produced as the placeholder second return value alongside ok == false
// or a non-nil error.
type CasToken struct {
	digest [16]byte
}

// Digest returns the token's raw 16-byte digest.
func (t CasToken) Digest() [16]byte { return t.digest }

// Hex returns the 32-character lowercase hex encoding this token places
// on the wire as a k/x <cond> field — the same encoding ContentDigest's
// caller would produce by hand.
func (t CasToken) Hex() string { return hex.EncodeToString(t.digest[:]) }

// TokenFromDigest wraps a raw 16-byte digest as a CasToken — for a caller
// that computed one via ContentDigest from a value it already holds,
// rather than one taken from a real prior GetWithToken. See Replace's own
// doc comment for why that reconstruction path is only as safe as
// memcached's own value-based CAS: it must reproduce the exact bytes the
// server stores (compression marker byte included, when this client has
// Config.Compress enabled), or it will simply never match and every
// Replace call will report a mismatch.
func TokenFromDigest(digest [16]byte) CasToken { return CasToken{digest: digest} }

// ContentDigest computes the CAS content digest (issue #141) of raw wire
// bytes: SHA-256 truncated to its first 16 bytes (128 bits). Computed
// identically by the server and every nanocached SDK — a fixed
// cross-language test vector pins the agreement (see
// TestContentDigestMatchesTheCrossLanguagePinnedVector).
//
// The input must be the exact bytes the server stores for the key — the
// same bytes a GetBytes/GetWithToken response body carries at the wire
// level, marker byte included when Config.Compress is enabled (the
// server never decompresses, so it can only ever hash what it actually
// holds). GetWithToken already computes this correctly from the raw wire
// bytes; ContentDigest is exported so a caller that already holds a value
// by some other means (e.g. it just wrote it) can compute the same digest
// without a round-trip GET — see Replace's own doc comment for that
// reconstruction path's caveat.
func ContentDigest(value []byte) [16]byte {
	sum := sha256.Sum256(value)
	var out [16]byte
	copy(out[:], sum[:16])
	return out
}

// GetWithToken returns key's raw value together with a CasToken usable
// with Replace/DeleteIfMatches to condition a later write on this exact
// content; ok is false when the key is missing, the same shape GetBytes
// uses. Transparently decompresses the returned value when Config.Compress
// is enabled, exactly like GetBytes — but the token itself is always
// computed from the raw wire bytes (before decompression), since that is
// what the server itself hashes (the server never decompresses; value
// compression). Computing it from the decompressed value instead would
// never match the server's own digest, silently breaking every CAS call
// made against it.
func (c *Client) GetWithToken(key string) (value []byte, token CasToken, ok bool, err error) {
	return c.getBytesWithTokenNS(nil, key)
}

// getBytesWithTokenNS is GetWithToken scoped to namespace — the internal
// (namespace, key) entry point a *Namespace handle's own GetWithToken
// forwards to, mirroring getBytesNS.
func (c *Client) getBytesWithTokenNS(namespace []byte, key string) (value []byte, token CasToken, ok bool, err error) {
	raw, ok, err := c.getRawNS(namespace, key)
	if err != nil || !ok {
		return nil, CasToken{}, ok, err
	}
	token = CasToken{digest: ContentDigest(raw)}
	if !c.compress {
		return raw, token, true, nil
	}
	value, err = decompressValue(raw)
	return value, token, true, err
}

// PutIfAbsent stores value under key only if the key is currently absent
// (including lazily expired) — the `k` frame's `A` condition
// (docs/protocol.html#cas), i.e. memcached's add. Returns true if it
// stored the value, false if the key already existed — a mismatch is a
// plain boolean outcome, never an error, the same idiom Delete's existed
// return uses. ttlSeconds is a whole number of seconds; 0 means no
// expiry, negative is rejected.
//
// See the package-level compare-and-set doc comment above for the "not a
// distributed lock" eviction caveat, and DeleteIfMatches' own doc comment
// for the at-least-once caveat every `k`/`x`-backed method here shares
// (issue #225): a connection failure after the request may have reached
// the primary is never silently retried, since PutIfAbsent/
// ReplaceIfPresent/Replace/DeleteIfMatches are not idempotent.
func (c *Client) PutIfAbsent(key string, value []byte, ttlSeconds int64) (bool, error) {
	return c.putIfAbsentNS(nil, key, value, ttlSeconds)
}

// putIfAbsentNS is PutIfAbsent scoped to namespace — the internal
// (namespace, key) entry point a *Namespace handle's own PutIfAbsent
// forwards to.
func (c *Client) putIfAbsentNS(namespace []byte, key string, value []byte, ttlSeconds int64) (bool, error) {
	return c.casNS(namespace, key, value, casCondAbsent, ttlSeconds)
}

// ReplaceIfPresent stores value under key only if the key currently holds
// any (unexpired) value, whatever it is — the `k` frame's `P` condition
// (docs/protocol.html#cas), the two-argument replace(key, value). Returns
// true if it stored the value, false if the key was absent — never an
// error on its own. ttlSeconds is a whole number of seconds; 0 means no
// expiry, negative is rejected.
func (c *Client) ReplaceIfPresent(key string, value []byte, ttlSeconds int64) (bool, error) {
	return c.replaceIfPresentNS(nil, key, value, ttlSeconds)
}

// replaceIfPresentNS is ReplaceIfPresent scoped to namespace — the
// internal (namespace, key) entry point a *Namespace handle's own
// ReplaceIfPresent forwards to.
func (c *Client) replaceIfPresentNS(namespace []byte, key string, value []byte, ttlSeconds int64) (bool, error) {
	return c.casNS(namespace, key, value, casCondPresent, ttlSeconds)
}

// Replace stores newValue under key only if the key currently holds an
// unexpired value whose content digest equals token exactly — the `k`
// frame's digest condition (docs/protocol.html#cas), the three-argument
// replace(key, old, new). Returns true if it stored newValue, false on a
// digest mismatch (including a missing key) — never an error on its own.
// ttlSeconds is a whole number of seconds; 0 means no expiry, negative is
// rejected.
//
// token is ordinarily one GetWithToken returned for this exact key, which
// makes this call correct by construction: the digest was computed from
// the server's own bytes. A token built instead via
// TokenFromDigest(ContentDigest(v)) from a value v the caller already
// holds is exactly as sensitive to encoding as memcached's own
// value-based CAS: it's only correct if v's serialization (and, with
// Config.Compress enabled, its marker byte and compression) reproduces
// byte-identical output to what the server actually stores — true within
// one client sharing one serializer/compressor, not guaranteed across
// languages or configurations. The read-then-write-back path
// (GetWithToken -> Replace) has no such caveat.
func (c *Client) Replace(key string, token CasToken, newValue []byte, ttlSeconds int64) (bool, error) {
	return c.replaceNS(nil, key, token, newValue, ttlSeconds)
}

// replaceNS is Replace scoped to namespace — the internal (namespace,
// key) entry point a *Namespace handle's own Replace forwards to.
func (c *Client) replaceNS(namespace []byte, key string, token CasToken, newValue []byte, ttlSeconds int64) (bool, error) {
	return c.casNS(namespace, key, newValue, token.Hex(), ttlSeconds)
}

// casNS validates and drives every `k`-backed operation
// (PutIfAbsent/ReplaceIfPresent/Replace) scoped to namespace: argument
// validation and value compression mirror setBytesNS exactly, then cas
// (below) drives the actual routing.
func (c *Client) casNS(namespace []byte, key string, value []byte, cond string, ttlSeconds int64) (bool, error) {
	if err := validateKeyAndValue(namespace, key, len(value)); err != nil {
		return false, err
	}
	if ttlSeconds < 0 {
		return false, invalidArgument(fmt.Sprintf("nanocached: ttlSeconds must not be negative, got %d", ttlSeconds))
	}
	if err := c.beforeOperation(); err != nil {
		return false, err
	}
	wireTTL := int64(-1) // no expiry
	if ttlSeconds > 0 {
		wireTTL = ttlSeconds
	}
	outgoing := value
	if c.compress {
		outgoing = compressValue(value, c.compressionThreshold)
	}
	keyBytes := []byte(key)
	var stored bool
	err := c.withClusterRetryNonIdempotent(func() error {
		s, casErr := c.cas(namespace, keyBytes, outgoing, cond, wireTTL)
		stored = s
		return casErr
	})
	return stored, err
}

// cas drives PutIfAbsent/ReplaceIfPresent/Replace's routing (issue #141)
// — deliberately NOT write()'s "run the same op against every owner"
// pattern, exactly like incr's own routing (see incr's doc comment):
// only the key's primary owner evaluates <cond>, since a replica
// evaluating the same condition against its own possibly-different copy
// could reach a different outcome than the primary just did. Once the
// primary succeeds, its literal result — the outgoing value and wireTTL,
// already compressed by casNS if applicable — is fanned out to the
// remaining owners as an ordinary Set (fanReplicas — the same
// best-effort, fire-and-forget-aware machinery write()/incr() use for
// their own replica legs), never by replaying `k` itself. A mismatch
// (`N`) leaves every replica untouched — nothing was written anywhere.
func (c *Client) cas(namespace, key []byte, value []byte, cond string, wireTTL int64) (bool, error) {
	var stored bool
	primaryOp := func(conn *connection) error {
		s, casErr := conn.casNS(namespace, key, value, cond, wireTTL)
		stored = s
		return casErr
	}

	c.mu.Lock()
	single := c.ring == nil
	c.mu.Unlock()

	var primaryErr error
	if single {
		primaryErr = c.applyNonIdempotent("", primaryOp)
	} else {
		names := c.ownerNames(namespace, key)
		if len(names) == 0 {
			return false, connectionLost("no owner is reachable for this key", nil)
		}
		primaryErr = c.applyNonIdempotent(names[0], primaryOp)
		if primaryErr == nil && stored {
			wg := c.fanReplicas(names[1:], func(conn *connection) error {
				return conn.setNS(namespace, key, value, wireTTL)
			})
			wg.Wait()
		}
	}
	if primaryErr != nil {
		return false, primaryErr
	}
	return stored, nil
}

// DeleteIfMatches removes key only if its current content digest equals
// token exactly — the `x` frame (docs/protocol.html#cas), the
// two-argument remove(key, old). Returns true if it deleted the key,
// false on a digest mismatch (including an already-missing key) — never
// an error on its own, the same idiom Delete's existed return uses.
//
// DeleteIfMatches — and PutIfAbsent/ReplaceIfPresent/Replace above — are
// at-least-once, not exactly-once (issue #225): `k`/`x`, unlike Get/Set/
// Delete, are not idempotent — a k/x that already succeeded, replayed,
// would compare against the value/absence it itself just wrote and come
// back reporting a mismatch even though the original call did succeed.
// So a connection failure is only retried transparently (via a redial)
// when the request provably never reached the server — e.g. a
// connection that went idle and was closed by the server before this
// call reused it. Once the request may have reached the primary, a
// connection failure is returned as ErrConnectionLost instead of being
// silently retried; the caller cannot tell from that alone whether the
// operation was applied, and must decide whether to retry (risking a
// false mismatch on the next call, or a double-delete) or treat the
// outcome as unknown.
func (c *Client) DeleteIfMatches(key string, token CasToken) (bool, error) {
	return c.deleteIfMatchesNS(nil, key, token)
}

// deleteIfMatchesNS is DeleteIfMatches scoped to namespace — the internal
// (namespace, key) entry point a *Namespace handle's own DeleteIfMatches
// forwards to.
func (c *Client) deleteIfMatchesNS(namespace []byte, key string, token CasToken) (bool, error) {
	if err := validateKey(namespace, key); err != nil {
		return false, err
	}
	if err := c.beforeOperation(); err != nil {
		return false, err
	}
	keyBytes := []byte(key)
	digest := token.Hex()
	var deleted bool
	err := c.withClusterRetryNonIdempotent(func() error {
		d, delErr := c.deleteIfMatches(namespace, keyBytes, digest)
		deleted = d
		return delErr
	})
	return deleted, err
}

// deleteIfMatches drives DeleteIfMatches's routing (issue #141) — the
// same primary-only-then-fan-the-result pattern as cas/incr: only the
// primary owner evaluates the digest; on success the remaining owners
// receive an ordinary Delete, never a replayed `x` (a replica's own copy
// could differ, and Delete is unconditional so there is no result to get
// wrong by replaying it — but replaying `x` itself would still risk a
// spurious mismatch against a replica that is momentarily behind).
func (c *Client) deleteIfMatches(namespace, key []byte, digest string) (bool, error) {
	var deleted bool
	primaryOp := func(conn *connection) error {
		d, err := conn.deleteIfMatchesNS(namespace, key, digest)
		deleted = d
		return err
	}

	c.mu.Lock()
	single := c.ring == nil
	c.mu.Unlock()

	var primaryErr error
	if single {
		primaryErr = c.applyNonIdempotent("", primaryOp)
	} else {
		names := c.ownerNames(namespace, key)
		if len(names) == 0 {
			return false, connectionLost("no owner is reachable for this key", nil)
		}
		primaryErr = c.applyNonIdempotent(names[0], primaryOp)
		if primaryErr == nil && deleted {
			wg := c.fanReplicas(names[1:], func(conn *connection) error {
				_, delErr := conn.deleteNS(namespace, key)
				return delErr
			})
			wg.Wait()
		}
	}
	if primaryErr != nil {
		return false, primaryErr
	}
	return deleted, nil
}
