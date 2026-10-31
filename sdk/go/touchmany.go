package nanocached

import (
	"errors"
	"fmt"
	"sort"
	"sync"
)

// Batched touch (issue #871): TouchMany/GetManyAndTouch are
// Touch/GetAndTouch for N keys in one `y` sub-frame per involved node
// (docs/protocol.html "y — batched touch"), chunked like GetMany/SetMany. A
// touch is idempotent and not primary-only, so every owner of a key is sent
// it, as for SetMany, and only the primary's answer decides the key's
// outcome. `G` belongs to the whole frame, so a node that is primary for
// some keys of a GetManyAndTouch batch and a replica for others gets two
// frames: the G form for the first, the plain form for the second
// (SetMany's "exactly one sub-frame per node" does not hold). Keys are
// deduplicated like GetMany's. With ttlSeconds == 0 the passes send the
// primary legs only and the other owners get one plain fan-out afterwards
// (issue #934; touchReplicasAfterPrimary).

// TouchMany replaces the TTL of every existing key in keys in place, one
// round trip per involved node instead of one per key (issue #871). keys
// must be non-empty. The returned map has an entry for each key whose
// primary answered: true for a key that existed, false for a missing or
// expired one, which a touch never creates. ttlSeconds is one TTL for the
// whole batch and means what it means for Touch: RemoveTTL removes the TTL,
// 0 expires the keys at once, any other negative value is rejected before
// any I/O (ErrInvalidArgument).
//
// Like Touch it is idempotent, so a request whose reply was lost is
// retried — except with ttlSeconds == 0, where a primary whose connection
// fails after the frame was written is not resent (issue #901): the call
// returns that ErrConnectionLost, with every key that did resolve in the
// map. In a cluster it goes to every owner of a key, the primary's answer
// being the key's — with ttlSeconds == 0 the primaries are sent the batch
// first, and the other owners only after they answered (issue #934, see
// touchReplicasAfterPrimary). A replica's failure or `W` is counted in
// Stats().ReplicaWriteFailures and never fails the batch. Larger batches
// are transparently split into more than one `y` sub-frame per node (see
// maxBatchKeys).
//
// As for GetMany, the map always holds what resolved, whether or not err is
// nil: if some keys' primaries are still wrong-node after one bounded
// refresh-and-retry, the error is ErrWrongNode, and a key whose primary
// could not be reached on both passes returns the last connection error
// (ErrConnectionLost) — never ErrWrongNode, since no `W` was ever received.
// Every key that did resolve was touched, and since a touch is idempotent
// the whole batch may simply be sent again. Returns ErrServerTooOldBatch
// against a node that predates the command.
func (c *Client) TouchMany(keys []string, ttlSeconds int64) (map[string]bool, error) {
	return c.touchManyNS(nil, keys, ttlSeconds)
}

// GetManyAndTouch reads the values of keys and replaces their TTL in one
// step, one round trip per involved node (issue #871). keys must be
// non-empty. A miss is absent from the returned map, creates nothing and
// is not an error. See TouchMany for ttlSeconds (RemoveTTL removes the TTL,
// 0 expires the keys at once).
//
// A read that writes, as GetAndTouch is: each key goes to its primary in
// the `G` form, with no hedging and no fallback to a replica on a miss, and
// its other owners are sent the plain touch, so a replica is never the only
// owner touched. It counts as a read on the primary, which makes the
// entries the most recently used. A value the node had no room for in its
// reply (16 MiB per frame) is left out and left untouched, so it reads as a
// miss here.
//
// Failures follow GetMany: the map holds every key that did resolve,
// paired with ErrWrongNode for keys still wrong-node after one bounded
// refresh-and-retry, or with the last connection error for keys whose
// primary was unreachable on both passes. Unlike GetMany a key is not tried
// on its next owner when its primary is down — a replica's answer would
// leave the primary untouched. With ttlSeconds == 0 a primary whose
// connection fails after the frame was written is not resent (issue #901):
// it may have expired and consumed its values already, so those keys return
// ErrConnectionLost at once instead of reading as misses. Returns
// ErrServerTooOldBatch against a node that predates the command.
func (c *Client) GetManyAndTouch(keys []string, ttlSeconds int64) (map[string]string, error) {
	raw, err := c.GetManyAndTouchBytes(keys, ttlSeconds)
	if raw == nil {
		return nil, err
	}
	values := make(map[string]string, len(raw))
	for key, value := range raw {
		values[key] = string(value)
	}
	return values, err
}

// GetManyAndTouchBytes is GetManyAndTouch's raw companion. Transparently
// decompresses when Config.Compress is enabled.
func (c *Client) GetManyAndTouchBytes(keys []string, ttlSeconds int64) (map[string][]byte, error) {
	return c.getManyAndTouchNS(nil, keys, ttlSeconds)
}

// TouchMany touches every key within this namespace. See Client.TouchMany.
func (n *Namespace) TouchMany(keys []string, ttlSeconds int64) (map[string]bool, error) {
	return n.client.touchManyNS(n.namespace, keys, ttlSeconds)
}

// GetManyAndTouch reads and touches every key within this namespace. See
// Client.GetManyAndTouch.
func (n *Namespace) GetManyAndTouch(keys []string, ttlSeconds int64) (map[string]string, error) {
	raw, err := n.GetManyAndTouchBytes(keys, ttlSeconds)
	if raw == nil {
		return nil, err
	}
	values := make(map[string]string, len(raw))
	for key, value := range raw {
		values[key] = string(value)
	}
	return values, err
}

// GetManyAndTouchBytes is GetManyAndTouch's raw companion within this
// namespace. See Client.GetManyAndTouchBytes.
func (n *Namespace) GetManyAndTouchBytes(keys []string, ttlSeconds int64) (map[string][]byte, error) {
	return n.client.getManyAndTouchNS(n.namespace, keys, ttlSeconds)
}

// touchManyNS is TouchMany scoped to namespace — see getBytesNS's doc
// comment; the internal entry point a *Namespace handle's TouchMany
// forwards to.
func (c *Client) touchManyNS(namespace []byte, keys []string, ttlSeconds int64) (map[string]bool, error) {
	keys, results, err := c.multiTouchNS(namespace, keys, ttlSeconds, false, "TouchMany")
	if keys == nil {
		return nil, err
	}
	touched := make(map[string]bool, len(results))
	for idx, entry := range results {
		touched[keys[idx]] = entry.ok
	}
	return touched, err
}

// getManyAndTouchNS is GetManyAndTouchBytes scoped to namespace.
func (c *Client) getManyAndTouchNS(namespace []byte, keys []string, ttlSeconds int64) (map[string][]byte, error) {
	keys, results, err := c.multiTouchNS(namespace, keys, ttlSeconds, true, "GetManyAndTouch/GetManyAndTouchBytes")
	if keys == nil {
		return nil, err
	}
	indices := make([]int, 0, len(results))
	for idx := range results {
		indices = append(indices, idx)
	}
	sort.Ints(indices)
	values := make(map[string][]byte, len(indices))
	// Cumulative decompressed bytes across this whole response — see
	// maxMultiGetDecompressedBytes. The budget only applies when
	// compression is enabled, as in getManyNS.
	var decompressed int64
	for _, idx := range indices {
		entry := results[idx]
		if !entry.ok {
			continue
		}
		value, decErr := c.maybeDecompress(entry.value)
		if decErr != nil {
			return values, decErr
		}
		if c.compress {
			// Charge the current entry before checking so the entry that
			// actually crosses the cap is caught (and excluded).
			decompressed += int64(len(value))
			if decompressed > maxMultiGetDecompressedBytes {
				return values, decompressionFailed(
					"cumulative decompressed size of this GetManyAndTouch response exceeds the " +
						"maximum — possible decompression bomb across the batch")
			}
		}
		values[keys[idx]] = value
	}
	return values, err
}

// touchManyRejected turns the node's `En` into ErrServerTooOldBatch (issue
// #871), leaving every other error as it was.
func touchManyRejected(err error) error {
	if err != nil && errors.Is(err, errServerRejected) {
		return errors.Join(ErrServerTooOldBatch, err)
	}
	return err
}

// multiTouchNS is the shared body of TouchMany (get false) and
// GetManyAndTouch (get true). It returns the deduplicated keys (nil when
// validation failed before any I/O) and, by key index, the primary's answer
// for every key that resolved — hits for a get, and for a plain touch ok
// says whether the key existed.
func (c *Client) multiTouchNS(namespace []byte, keys []string, ttlSeconds int64, get bool, method string) ([]string, map[int]multiEntry, error) {
	if len(keys) == 0 {
		return nil, nil, invalidArgument("nanocached: " + method + " requires at least one key")
	}
	if err := validateTouchTTL(ttlSeconds); err != nil {
		return nil, nil, err
	}
	// A repeated key is touched once, as GetMany reads it once.
	keys = dedupeKeys(keys)
	keyBytes := make([][]byte, len(keys))
	for i, key := range keys {
		if err := validateKey(namespace, key); err != nil {
			return nil, nil, err
		}
		keyBytes[i] = []byte(key)
	}
	if err := c.beforeOperation(); err != nil {
		return nil, nil, err
	}

	c.mu.Lock()
	single := c.ring == nil
	c.mu.Unlock()

	if single {
		// Single/proxy mode: no ring to refresh against, so a `W` or a
		// failure propagates immediately, as in getManyNS.
		entries, resolved, err := c.multiTouchChunked("", namespace, keyBytes, ttlSeconds, get, false)
		results := make(map[int]multiEntry, len(keys))
		wrongNode := false
		for i := 0; i < resolved; i++ {
			if entries[i].wrongNode {
				wrongNode = true
				continue
			}
			results[i] = entries[i]
		}
		if err != nil {
			return keys, results, touchManyRejected(err)
		}
		if wrongNode {
			return keys, results, ErrWrongNode
		}
		return keys, results, nil
	}

	state := &multiTouchState{
		results: make(map[int]multiEntry, len(keys)), wrongNode: map[int]bool{}, lost: map[int]bool{},
		answeredBy: map[int]string{},
	}
	retry := c.multiTouchPass(namespace, keyBytes, ttlSeconds, get, nil, state)
	if len(retry) > 0 {
		// One bounded refresh-and-retry over everything a primary left
		// unresolved: a `W` is stale routing the refresh fixes, a primary
		// that could not be reached gets a fresh view of who the owners
		// are. A touch is idempotent, so the keys go to every owner of the
		// fresh ring again.
		c.maybeRefresh(true)
		retry = c.multiTouchPass(namespace, keyBytes, ttlSeconds, get, retry, state)
	}
	if ttlSeconds == 0 {
		c.touchReplicasAfterPrimary(namespace, keyBytes, state)
	}
	if state.rejected {
		return keys, state.results, errors.Join(ErrServerTooOldBatch, state.lastErr)
	}
	if len(retry) == 0 && len(state.lost) == 0 {
		return keys, state.results, nil
	}
	if len(state.lost) == 0 {
		for _, idx := range retry {
			if state.wrongNode[idx] {
				return keys, state.results, ErrWrongNode
			}
		}
	}
	// No primary answered `W` for these keys: each one's primary was
	// unreachable on both passes, or (ttlSeconds == 0) lost its connection
	// after the frame was written and was not resent. That is a connection
	// loss and is reported as one, as getManyNS does. A lost key's own
	// failure is reported, never a later not-sent one (a dial that failed
	// in the retry pass): only it says the request may have been applied.
	cause := state.lastErr
	if len(state.lost) > 0 && state.lostErr != nil {
		cause = state.lostErr
	}
	if cause == nil {
		cause = connectionLost("no owner is reachable for these keys", nil)
	}
	return keys, state.results, fmt.Errorf("nanocached: %s could not reach the primary of %d of %d keys: %w", method, len(retry)+len(state.lost), len(keys), cause)
}

// multiTouchChunked is multiGetChunked's twin for `y`: one or more
// sub-frames against slot for keys — already grouped to one owner (or the
// single/proxy target) by the caller — chunked like `m` (maxBatchKeys,
// maxRequestBytes; issue #222). get selects the `G` form. Always returns
// len(keys) entries; only the first resolved of them (the index the failing
// chunk started at, or len(keys) on success) are real answers — the rest
// were never reached, which no entry value can say, since a miss is a real
// answer too.
func (c *Client) multiTouchChunked(slot string, namespace []byte, keys [][]byte, ttlSeconds int64, get bool, replicaOnly bool) (entries []multiEntry, resolved int, err error) {
	entries = make([]multiEntry, len(keys))
	// A touch that expires the keys is resent only if never sent (issue
	// #901). A frame none of whose keys has this slot as primary (replicaOnly)
	// decides nothing and keeps the plain resend.
	apply := c.applyReconnecting
	if ttlSeconds == 0 && !replicaOnly {
		apply = c.applyExpiring
	}
	budget := maxRequestBytes - len(namespace) - multiFrameHeaderSlack
	for start := 0; start < len(keys); {
		end := start
		total := 0
		for end < len(keys) && end-start < maxBatchKeys {
			cost := multiGetEntryCost(keys[end])
			if end > start && total+cost > budget {
				break
			}
			total += cost
			end++
		}
		var chunkEntries []multiEntry
		err := apply(slot, func(conn *connection) error {
			var opErr error
			if get {
				chunkEntries, opErr = conn.multiGetAndTouchNS(namespace, keys[start:end], ttlSeconds)
			} else {
				chunkEntries, opErr = conn.multiTouchNS(namespace, keys[start:end], ttlSeconds)
			}
			return opErr
		})
		if err != nil {
			return entries, start, err
		}
		copy(entries[start:end], chunkEntries)
		start = end
	}
	return entries, len(keys), nil
}

// multiTouchState is what one TouchMany/GetManyAndTouch call learns across
// its two multiTouchPass passes, guarded by mu: results maps a key index to
// its primary's answer; wrongNode holds the indices whose primary leg's
// latest outcome was a `W` rather than a connection failure; lastErr is the
// most recent primary-leg connection failure and rejected whether any
// primary leg was answered `En` (a node that predates `y`); lost is below.
type multiTouchState struct {
	mu        sync.Mutex
	results   map[int]multiEntry
	wrongNode map[int]bool
	lastErr   error
	rejected  bool
	// lost holds the indices a ttlSeconds == 0 call must not resend (issue
	// #901): their primary leg failed after the frame was written, so the
	// node may have expired them already. lostErr is that failure, kept
	// apart from lastErr so a later not-sent failure in the retry pass
	// cannot replace it.
	lost    map[int]bool
	lostErr error
	// answeredBy names the node whose primary answer is in results[idx], so
	// the deferred replica fan-out of a ttlSeconds == 0 call (issue #934)
	// does not touch it a second time.
	answeredBy map[int]string
}

// multiTouchPass runs one pass of the cluster routing of TouchMany and
// GetManyAndTouch (with ttlSeconds == 0, to the primary legs only — the rest
// follows in touchReplicasAfterPrimary, issue #934): every key still needing
// resolution (every key when
// retryIndices is nil, or just what the previous pass left unresolved) is
// sent to every owner, in one leg per (owner, form): with get a key's
// primary gets it in the `G` form and every other owner in the plain one,
// without it every owner gets the plain form, so a node holding some keys
// as primary and others as replica gets one frame in the second case and
// two in the first. Only the primary's answer is recorded in state.results;
// a replica's failure or `W` is counted in Stats().ReplicaWriteFailures, as
// in runMultiSetLeg. A leg that holds no key as primary is eligible for
// Config.FireAndForgetReplicas, exactly like a single-key replica write.
//
// Returns the indices whose primary answered `W` or could not be reached (a
// key with no owners at all counts as `W`: a refresh may populate the
// ring). state learns which of the two it was, and whether a primary was a
// node that predates `y`.
func (c *Client) multiTouchPass(
	namespace []byte, keyBytes [][]byte, ttlSeconds int64, get bool,
	retryIndices []int, state *multiTouchState,
) []int {
	indices := retryIndices
	if indices == nil {
		indices = make([]int, len(keyBytes))
		for i := range keyBytes {
			indices[i] = i
		}
	}

	type legID struct {
		name string
		get  bool
	}
	type touchLeg struct {
		name      string
		get       bool
		indices   []int
		isPrimary []bool
	}
	legs := make(map[legID]*touchLeg)
	var retry []int
	for _, idx := range indices {
		names := c.ownerNames(namespace, keyBytes[idx])
		if len(names) == 0 {
			state.wrongNode[idx] = true
			retry = append(retry, idx)
			continue
		}
		for rank, name := range names {
			if ttlSeconds == 0 && rank > 0 {
				// An expiring touch goes to the primary alone first; the
				// other owners are sent theirs after it answered (issue
				// #934, see touchReplicasAfterPrimary).
				break
			}
			form := get && rank == 0
			id := legID{name, form}
			leg := legs[id]
			if leg == nil {
				leg = &touchLeg{name: name, get: form}
				legs[id] = leg
			}
			leg.indices = append(leg.indices, idx)
			leg.isPrimary = append(leg.isPrimary, rank == 0)
		}
	}

	runLeg := func(leg *touchLeg, retry *[]int) {
		groupKeys := make([][]byte, len(leg.indices))
		for i, idx := range leg.indices {
			groupKeys[i] = keyBytes[idx]
		}
		anyPrimary := false
		for _, isPrimary := range leg.isPrimary {
			anyPrimary = anyPrimary || isPrimary
		}
		entries, resolved, err := c.multiTouchChunked(leg.name, namespace, groupKeys, ttlSeconds, leg.get, !anyPrimary)

		state.mu.Lock()
		defer state.mu.Unlock()
		// A touch that expires (ttlSeconds == 0) is not safe to resend once
		// its frame was written (issue #901): the node may have applied it,
		// and a resend would report the keys as missing — and, with `G`,
		// lose the values it consumed. A failure marked errRequestNotSent
		// (a dead connection, a dial that failed) stays retryable, and an
		// `En` keeps its own handling.
		lostIsFinal := false
		if err != nil && anyPrimary {
			state.lastErr = err
			if errors.Is(err, errServerRejected) {
				state.rejected = true
			} else {
				lostIsFinal = ttlSeconds == 0 &&
					(errors.Is(err, ErrConnectionLost) || errors.Is(err, ErrProtocol)) &&
					!errors.Is(err, errRequestNotSent)
				if lostIsFinal {
					state.lostErr = err
				}
			}
		}
		for i, idx := range leg.indices {
			answered := i < resolved && !entries[i].wrongNode
			switch {
			case !leg.isPrimary[i]:
				if !answered {
					c.stats.replicaWriteFailures.Add(1)
				}
			case answered:
				state.results[idx] = entries[i]
				state.answeredBy[idx] = leg.name
			case i >= resolved && errors.Is(err, errServerRejected):
				// A node that predates `y` refuses it every time: another
				// pass would only dial it again.
				delete(state.wrongNode, idx)
			default:
				if i < resolved {
					state.wrongNode[idx] = true
				} else {
					delete(state.wrongNode, idx)
					if lostIsFinal {
						state.lost[idx] = true
						continue
					}
				}
				*retry = append(*retry, idx)
			}
		}
	}

	var wg sync.WaitGroup
	var legRetry []int
	for _, leg := range legs {
		pureReplica := true
		for _, isPrimary := range leg.isPrimary {
			if isPrimary {
				pureReplica = false
				break
			}
		}

		if pureReplica && c.detachReplicaLeg(func() {
			// A pure-replica leg holds no primary key, so it appends
			// nothing to the retry list.
			var none []int
			runLeg(leg, &none)
		}) {
			continue
		}

		wg.Add(1)
		go func(leg *touchLeg) {
			defer wg.Done()
			runLeg(leg, &legRetry)
		}(leg)
	}
	wg.Wait()
	return append(retry, legRetry...)
}

// detachReplicaLeg runs fn in the background when FireAndForgetReplicas is
// on and a slot is free, reporting whether it did; the caller runs it itself
// otherwise. Background legs are tracked on backgroundReplicaWG so Close()
// still drains them (the same Add-under-c.mu-with-a-closed-recheck ordering
// multiSetPass and fanReplicas use, which guarantees every Add
// happens-before Close()'s Wait), and capped by backgroundReplicaSem.
func (c *Client) detachReplicaLeg(fn func()) bool {
	if !c.fireAndForgetReplicas {
		return false
	}
	select {
	case c.backgroundReplicaSem <- struct{}{}:
		c.mu.Lock()
		if !c.closed {
			c.backgroundReplicaWG.Add(1)
			c.mu.Unlock()
			go func() {
				defer c.backgroundReplicaWG.Done()
				defer func() { <-c.backgroundReplicaSem }()
				fn()
			}()
			return true
		}
		c.mu.Unlock()
		<-c.backgroundReplicaSem
	default:
	}
	return false
}

// touchReplicasAfterPrimary is the second half of an expiring batch touch
// (ttlSeconds == 0, issue #934): both passes of multiTouchPass sent only the
// PRIMARY legs, so no replica has expired a copy a reroute would still need
// to return. Now, in one plain fan-out grouped by replica owner on the ring
// in force, the other owners are sent the touch for every key whose primary
// answered — `S`/`V` and also `N`, since a primary that missed may still
// have a replica copy, and the caller's intent is to expire it everywhere.
// A key whose primary answered `W`, was lost or was rejected gets no replica
// leg. A replica's failure or `W` is counted in
// Stats().ReplicaWriteFailures and never fails the batch, and a leg is
// eligible for Config.FireAndForgetReplicas as an ordinary replica leg is.
// These legs run after the primary answered, outside the pipelined-order
// guarantee, the same class as the retry paths.
func (c *Client) touchReplicasAfterPrimary(namespace []byte, keyBytes [][]byte, state *multiTouchState) {
	groups := make(map[string][][]byte)
	for idx := range state.results {
		for _, name := range c.ownersExcept(namespace, keyBytes[idx], state.answeredBy[idx]) {
			groups[name] = append(groups[name], keyBytes[idx])
		}
	}
	var wg sync.WaitGroup
	for name, groupKeys := range groups {
		run := func() {
			entries, resolved, _ := c.multiTouchChunked(name, namespace, groupKeys, 0, false, true)
			for i := range groupKeys {
				if i >= resolved || entries[i].wrongNode {
					c.stats.replicaWriteFailures.Add(1)
				}
			}
		}
		if c.detachReplicaLeg(run) {
			continue
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			run()
		}()
	}
	wg.Wait()
}
