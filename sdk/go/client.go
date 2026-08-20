// Package nanocached is the Go client SDK for nanocached, a tiny
// distributed cache with client-side replication.
//
// A Config's Addresses may name either a single nanocached-node or
// discovery server(s) fronting a cluster — Connect finds out from the
// server's own handshake response (doc/adr/0007-*.md), so calling code
// is identical either way.
//
// Cluster mode implements ADR-0011 client-side replication: writes fan
// out to each key's top-R owners (the primary's result decides; a dead
// replica never fails a write), reads ask the primary and fall over to
// the next owner only when the holder is unreachable. Dead connections
// are redialed lazily on use (with one transparent retry — a socket only
// learns of a peer FIN on I/O, and every operation is idempotent), and an
// opt-in keep-alive can hold connections open across the server's 60s
// idle timeout.
//
// The Client is safe for concurrent use. Requests are pipelined per
// connection (doc/adr/0016-*.md): concurrent callers on the same
// connection each pay only their own network latency, not everyone
// else's ahead of them.
package nanocached

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"net"
	"os"
	"strconv"
	"sync"
	"time"
)

// nodeListStaleAfter is how long the node list may go without a re-fetch
// from discovery before Get/Set/Delete refreshes it first (checked lazily
// on use).
const nodeListStaleAfter = 30 * time.Second

// keepaliveKey: the server rejects empty keys, so the keep-alive G needs
// one byte; a single NUL stays out of any real key space.
var keepaliveKey = []byte{0}

// Address is a "host:port" connect target: a single nanocached-node, or
// one discovery replica (ADR-0010) fronting a cluster.
type Address struct {
	Host string
	Port int
}

func (a Address) String() string {
	return net.JoinHostPort(a.Host, strconv.Itoa(a.Port))
}

// Config configures Connect.
type Config struct {
	// Addresses lists connect targets, tried in order — one entry for a
	// single node, or every discovery replica (ADR-0010) for a cluster.
	Addresses []Address
	// AuthSecret matches NANOCACHED_AUTH_SECRET on the server; empty
	// means no authentication configured.
	AuthSecret string
	// TLS connects every socket over TLS when true (plaintext otherwise).
	TLS bool
	// CA names a PEM file of trusted root certificate(s), replacing the
	// platform/system trust store. Only meaningful when TLS is true; a
	// set CA is silently ignored when TLS is false.
	CA string
	// Compress transparently DEFLATE-compresses values at or above
	// CompressionThreshold on Set/SetBytes and decompresses them on
	// Get/GetBytes (doc/adr/0013-*.md). Off by default. Every client that
	// reads or writes a given set of keys must agree on Compress — it is
	// a per-keyspace format decision, not a per-client preference.
	Compress bool
	// CompressionThreshold is the byte length at or above which Compress
	// actually compresses a value; below it (or when compressing doesn't
	// shrink the value) the value is stored as-is behind the marker
	// byte, to avoid bloating small values with compression overhead.
	// Zero means DefaultCompressionThreshold. Only meaningful when
	// Compress is true.
	CompressionThreshold int
	// FireAndForgetReplicas lets Set/SetBytes/Delete return as soon as
	// the primary owner acks, letting replica legs finish in the
	// background instead of waiting for them too (doc/adr/0014-*.md).
	// Off by default. Unlike Compress, this is a pure latency/durability
	// trade for this client's own writes — it carries no wire format and
	// needs no agreement with other clients.
	FireAndForgetReplicas bool
	// ReadRepair probes the remaining owners on a clean primary miss and
	// repairs the gap in the background if one still holds the value
	// (doc/adr/0015-*.md). Off by default. Costs extra reads only on the
	// misses it actually applies to.
	ReadRepair bool
}

// DefaultCompressionThreshold is the CompressionThreshold used when
// Config.CompressionThreshold is left at zero.
const DefaultCompressionThreshold = 256

// maxInFlightBackgroundReplicaWrites bounds how many replica writes a
// single client may have running in the background at once when
// FireAndForgetReplicas is enabled (doc/adr/0014-*.md) — once the cap is
// reached, further replica legs fall back to running synchronously, the
// same as with the option off. A variable only so tests can shrink it.
var maxInFlightBackgroundReplicaWrites = 32

// keepAliveInterval is the always-on keep-alive cadence (issue #27):
// half the server's 60s idle timeout, so it never severs a healthy
// client. A variable only so tests can shorten it.
var keepAliveInterval = 30 * time.Second

// readRepairTTL is the TTL a read-repair write applies to the primary
// (doc/adr/0015-*.md). G's response carries no TTL, so the key's
// original expiry is unrecoverable; repairing with TTL 0 would make an
// expiring key immortal, permanently resurrecting data the primary had
// correctly let expire. 60s bounds the overshoot instead — a key
// repaired past its true expiry simply gets re-repaired (or genuinely
// found missing) on a later miss.
const readRepairTTL = 60

type member struct {
	address    string
	connection *connection
}

// Client is a nanocached client handle.
type Client struct {
	mu          sync.Mutex // guards single/members/ring/replication/lastFetch
	refreshMu   sync.Mutex
	redialMu    sync.Mutex
	redialGates map[string]*sync.Mutex

	addresses  []Address
	authSecret []byte
	tlsConfig  *tls.Config

	compress             bool
	compressionThreshold int

	fireAndForgetReplicas bool
	// backgroundReplicaSem bounds in-flight background replica writes;
	// backgroundReplicaWG lets Close() drain them before tearing down
	// connections (doc/adr/0014-*.md).
	backgroundReplicaSem chan struct{}
	backgroundReplicaWG  sync.WaitGroup

	readRepair bool

	// targetKey is the address this client's connect() ultimately settled
	// on — a node's own address in single mode, the winning discovery
	// server's address in cluster mode. Every socket this client ever
	// opens (initial connect, lazy reconnect, newly discovered members)
	// is tracked in openTargets under this one key, mirroring the
	// TypeScript SDK's `this.url`.
	targetKey string

	closed        bool
	stopKeepalive chan struct{}

	single        *connection // single-node mode
	singleAddress string
	members       map[string]*member // cluster mode
	ring          *HashRing
	replication   int
	lastFetch     time.Time
}

// ── open-connection tracking (forgotten-close detection) ────────────
//
// A process-global count of open SDK sockets per target address, purely
// a programming-error guard: it catches "connect() called again for the
// same address before the previous client's close() was ever called"
// without affecting behavior — connecting again still works, this only
// warns. Mirrors sdk/typescript/src/client.ts's openTargets.
var (
	openTargetsMu sync.Mutex
	openTargets   = map[string]int{}
)

func trackOpenTarget(key string) {
	openTargetsMu.Lock()
	openTargets[key]++
	openTargetsMu.Unlock()
}

func untrackOpenTarget(key string) {
	openTargetsMu.Lock()
	if openTargets[key] <= 1 {
		delete(openTargets, key)
	} else {
		openTargets[key]--
	}
	openTargetsMu.Unlock()
}

func openTargetCount(key string) int {
	openTargetsMu.Lock()
	defer openTargetsMu.Unlock()
	return openTargets[key]
}

// trackedConnection wraps netConn, counting it against this client's
// targetKey until it closes (whichever of the several close() call
// sites — Close(), refresh reconciliation, dead-connection replacement —
// eventually fires).
func (c *Client) trackedConnection(netConn net.Conn) *connection {
	key := c.targetKey
	trackOpenTarget(key)
	return newConnection(netConn, func() { untrackOpenTarget(key) })
}

// Connect dials the first working address and returns a ready client.
func Connect(config Config) (*Client, error) {
	if len(config.Addresses) == 0 {
		return nil, fmt.Errorf("nanocached: connect() needs a non-empty addresses list")
	}

	tlsConfig, err := buildTLSConfig(config)
	if err != nil {
		return nil, err
	}

	compressionThreshold := config.CompressionThreshold
	if compressionThreshold == 0 {
		compressionThreshold = DefaultCompressionThreshold
	}

	client := &Client{
		redialGates:           map[string]*sync.Mutex{},
		addresses:             append([]Address(nil), config.Addresses...),
		tlsConfig:             tlsConfig,
		members:               map[string]*member{},
		replication:           1,
		lastFetch:             time.Now(),
		stopKeepalive:         make(chan struct{}),
		compress:              config.Compress,
		compressionThreshold:  compressionThreshold,
		fireAndForgetReplicas: config.FireAndForgetReplicas,
		backgroundReplicaSem:  make(chan struct{}, maxInFlightBackgroundReplicaWrites),
		readRepair:            config.ReadRepair,
	}
	if config.AuthSecret != "" {
		client.authSecret = []byte(config.AuthSecret)
	}

	// Walk the addresses until one yields a working target; an address
	// that is unreachable, warming up (B, ADR-0010), or knows no live
	// nodes is skipped — the next replica may do better.
	var lastError error
	for _, addr := range client.addresses {
		key := addr.String()

		// Only meaningful for a single explicit target: with an
		// addresses list, another client instance legitimately holding
		// connections to the same address makes this heuristic a false
		// positive (issue #12).
		if len(client.addresses) == 1 && openTargetCount(key) > 0 {
			fmt.Fprintf(os.Stderr,
				"nanocached: connect() called for %s while a previous connection to it is "+
					"still open — was close() forgotten?\n", key)
		}

		result, err := connectAndIdentify(key, client.authSecret, client.tlsConfig)
		if err != nil {
			lastError = err
			continue
		}

		client.targetKey = key

		if result.conn != nil {
			if len(client.addresses) > 1 {
				fmt.Fprintf(os.Stderr,
					"nanocached: %s is a cache node, so this client is pinned to that single "+
						"server — the %d remaining address(es) will not be used. Point addresses "+
						"at discovery servers for cluster routing and failover.\n",
					key, len(client.addresses)-1)
			}
			client.single = client.trackedConnection(result.conn)
			client.singleAddress = key
			client.startKeepalive(keepAliveInterval)
			return client, nil
		}

		if len(result.nodes) == 0 {
			lastError = fmt.Errorf(
				"nanocached: no live nodes registered with the discovery server at %s", key)
			continue
		}

		if err := client.openCluster(result); err != nil {
			client.teardown()
			return nil, err
		}
		client.startKeepalive(keepAliveInterval)
		return client, nil
	}

	if lastError == nil {
		lastError = fmt.Errorf("nanocached: could not connect to any address")
	}
	return nil, lastError
}

// buildTLSConfig turns Config.TLS/CA into a *tls.Config, or nil for
// plaintext. CA is meaningful only when TLS is true — when TLS is false
// it is silently ignored, matching every other SDK's semantics.
func buildTLSConfig(config Config) (*tls.Config, error) {
	if !config.TLS {
		return nil, nil
	}
	if config.CA == "" {
		return &tls.Config{}, nil // system/platform trust store
	}
	pemBytes, err := os.ReadFile(config.CA)
	if err != nil {
		return nil, fmt.Errorf("nanocached: could not read CA file %s: %w", config.CA, err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(pemBytes) {
		return nil, fmt.Errorf("nanocached: no valid certificates found in CA file %s", config.CA)
	}
	return &tls.Config{RootCAs: pool}, nil
}

func (c *Client) openCluster(result *identified) error {
	names := make([]string, 0, len(result.nodes))
	for _, node := range result.nodes {
		conn, err := c.openNodeConnection(node.Address)
		if err != nil {
			return err
		}
		c.members[node.Name] = &member{address: node.Address, connection: conn}
		names = append(names, node.Name)
	}
	c.ring = NewHashRing(names)
	c.replication = result.replication
	return nil
}

// ── 公開 API ──────────────────────────────────────────────────────

// Replication reports how many nodes hold each key (ADR-0011) — 1
// against a single node.
func (c *Client) Replication() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.ring == nil {
		return 1
	}
	return c.replication
}

// IsClosed reports whether Close has been called.
func (c *Client) IsClosed() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.closed
}

// Get returns the key's value as a string; ok is false when the key is
// missing. string(bytes) is a lossless conversion in Go, so unlike some
// other nanocached SDKs there is no decode-failure error path — use
// GetBytes for the raw bytes if the value isn't meant to be text.
func (c *Client) Get(key string) (value string, ok bool, err error) {
	raw, ok, err := c.GetBytes(key)
	if err != nil || !ok {
		return "", ok, err
	}
	return string(raw), true, nil
}

// GetBytes returns the key's raw value; ok is false when the key is
// missing. Transparently decompresses when Config.Compress is enabled
// (doc/adr/0013-*.md). With Config.ReadRepair, a clean miss probes the
// remaining owners before being accepted as final, repairing the gap in
// the background if one still holds the value (doc/adr/0015-*.md).
func (c *Client) GetBytes(key string) (value []byte, ok bool, err error) {
	if err := c.beforeOperation(); err != nil {
		return nil, false, err
	}
	keyBytes := []byte(key)
	err = c.withClusterRetry(func() error {
		return c.read(keyBytes, func(conn *connection) error {
			var opErr error
			value, ok, opErr = conn.get(keyBytes)
			return opErr
		})
	})
	if err == nil && !ok && c.readRepair {
		value, ok = c.tryReadRepair(keyBytes)
	}
	if err != nil || !ok || !c.compress {
		return value, ok, err
	}
	value, err = decompressValue(value)
	return value, ok, err
}

// tryReadRepair probes every owner of key, in rank order, for a value the
// normal read path already reported missing. The first owner that has it
// wins: its value is returned, and a best-effort, fully detached write
// repairs the primary in the background (doc/adr/0015-*.md) with
// readRepairTTL — no bounding, no close() draining, since losing this
// write costs nothing beyond staying in the window this feature narrows
// for one more read. Every failure along the way (connection lost,
// WrongNode, another miss) is swallowed; nothing here may turn an
// already-accepted miss into an error.
func (c *Client) tryReadRepair(key []byte) (value []byte, ok bool) {
	names := c.ownerNames(key)
	for _, name := range names {
		v, found, err := c.get(name, key)
		if err != nil || !found {
			continue
		}
		if len(names) > 0 {
			primary := names[0]
			go func() {
				_ = c.applyReconnecting(primary, func(conn *connection) error {
					return conn.set(key, v, readRepairTTL)
				})
			}()
		}
		return v, true
	}
	return nil, false
}

func (c *Client) get(slot string, key []byte) (value []byte, ok bool, err error) {
	err = c.applyReconnecting(slot, func(conn *connection) error {
		var opErr error
		value, ok, opErr = conn.get(key)
		return opErr
	})
	return value, ok, err
}

// Set stores the string value under the key. ttlSeconds is a whole
// number of seconds; 0 means no expiry, negative is rejected.
func (c *Client) Set(key, value string, ttlSeconds int64) error {
	return c.SetBytes(key, []byte(value), ttlSeconds)
}

// SetBytes stores the raw value under the key. ttlSeconds is a whole
// number of seconds; 0 means no expiry, negative is rejected.
// Transparently compresses values at or above Config.CompressionThreshold
// when Config.Compress is enabled (doc/adr/0013-*.md).
func (c *Client) SetBytes(key string, value []byte, ttlSeconds int64) error {
	if ttlSeconds < 0 {
		return fmt.Errorf("nanocached: ttlSeconds must not be negative, got %d", ttlSeconds)
	}
	if err := c.beforeOperation(); err != nil {
		return err
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
	return c.withClusterRetry(func() error {
		return c.write(keyBytes, func(conn *connection, _ bool) error {
			return conn.set(keyBytes, outgoing, wireTTL)
		})
	})
}

// Delete removes the key, reporting whether it existed before this call.
func (c *Client) Delete(key string) (existed bool, err error) {
	if err := c.beforeOperation(); err != nil {
		return false, err
	}
	keyBytes := []byte(key)
	err = c.withClusterRetry(func() error {
		return c.write(keyBytes, func(conn *connection, primary bool) error {
			e, opErr := conn.delete(keyBytes)
			// Only the primary's answer decides (ADR-0011) — and only the
			// primary leg may touch `existed`: the replica legs run on
			// other goroutines, so writing it there would both race and
			// let a replica's answer overwrite the primary's.
			if primary {
				existed = e
			}
			return opErr
		})
	})
	return existed, err
}

// Close is idempotent; later Get/Set/Delete return ErrClosed. Calling
// Close a second time is harmless but warns to stderr — it usually
// signals the caller lost track of this client's lifecycle.
func (c *Client) Close() {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		fmt.Fprintln(os.Stderr, "nanocached: close() called again on an already-closed client")
		return
	}
	c.closed = true
	close(c.stopKeepalive)
	c.mu.Unlock()
	// doc/adr/0014-*.md: give background replica writes (if any) a chance
	// to finish before their connections are torn out from under them.
	// Bounded by maxInFlightBackgroundReplicaWrites, so this is a short
	// wait in practice.
	c.backgroundReplicaWG.Wait()
	c.teardown()
}

func (c *Client) teardown() {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.single != nil {
		c.single.close()
	}
	for _, m := range c.members {
		m.connection.close()
	}
}

// ── ルーティングと複製 ────────────────────────────────────────────

func (c *Client) beforeOperation() error {
	if c.IsClosed() {
		return ErrClosed
	}
	c.maybeRefresh(false)
	return nil
}

// withClusterRetry runs the operation; on a W answer (stale routing) or a
// connection-level failure that exhausted the current ranking (e.g. the
// key's primary died), it forces a node-list refresh and retries the
// whole operation once against the fresh ranking. The retry window for a
// dead node is therefore bounded by discovery's liveness timeout. A
// second failure after a fresh refresh propagates.
func (c *Client) withClusterRetry(operation func() error) error {
	err := operation()
	if err == nil || (!errors.Is(err, ErrWrongNode) && !errors.Is(err, ErrConnectionLost)) {
		return err
	}
	c.mu.Lock()
	clustered := c.ring != nil
	c.mu.Unlock()
	if !clustered {
		return err
	}
	c.maybeRefresh(true)
	return operation()
}

func (c *Client) ownerNames(key []byte) []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.ring == nil {
		return nil
	}
	return c.ring.Owners(key, c.replication)
}

// applyReconnecting runs op against the slot's connection, retrying once
// on a connection-level failure: a socket only learns of a peer FIN (e.g.
// the server's 60s idle timeout) on I/O, so lazy reconnect-on-use means
// the failed request poisons the connection, the redial replaces it, and
// the operation runs again. Safe because Get/Set/Delete are idempotent.
// slot is "" in single mode.
func (c *Client) applyReconnecting(slot string, op func(*connection) error) error {
	conn, err := c.slotConnection(slot)
	if err != nil {
		return err
	}
	if err := op(conn); err != nil {
		if !errors.Is(err, ErrConnectionLost) {
			return err
		}
		conn, redialErr := c.slotConnection(slot)
		if redialErr != nil {
			return redialErr
		}
		return op(conn)
	}
	return nil
}

// read drives the owner walk and error policy; the op closure delivers
// its result through variables captured by the caller.
func (c *Client) read(key []byte, op func(*connection) error) error {
	c.mu.Lock()
	single := c.ring == nil
	c.mu.Unlock()
	if single {
		return c.applyReconnecting("", op)
	}

	// Owners in rank order; fall through only on connection-level
	// failure — a replica hedges against a dead holder, not a miss.
	var lastError error
	for _, name := range c.ownerNames(key) {
		err := c.applyReconnecting(name, op)
		if err == nil {
			return nil
		}
		if errors.Is(err, ErrWrongNode) {
			return err
		}
		lastError = err
	}
	if lastError == nil {
		lastError = connectionLost("no owner is reachable for this key", nil)
	}
	return lastError
}

// write runs op against every owner of the key; op's second argument
// reports whether this leg is the primary, whose outcome alone decides
// the operation's result — replica legs run on their own goroutines, so
// an op that captures outer variables must only write them when primary.
func (c *Client) write(key []byte, op func(conn *connection, primary bool) error) error {
	c.mu.Lock()
	single := c.ring == nil
	c.mu.Unlock()
	primaryOp := func(conn *connection) error { return op(conn, true) }
	if single {
		return c.applyReconnecting("", primaryOp)
	}

	names := c.ownerNames(key)
	if len(names) == 0 {
		return connectionLost("no owner is reachable for this key", nil)
	}

	// Fan out to the replicas concurrently with the primary write. The
	// primary's outcome decides; replica failures are swallowed by design
	// (ADR-0011) — a dead or disagreeing replica leaves the key
	// under-replicated until the next node-list refresh, never fails the
	// write.
	replicaWrite := func(replica string) {
		_ = c.applyReconnecting(replica, func(conn *connection) error { return op(conn, false) })
	}

	var replicas sync.WaitGroup
	for _, name := range names[1:] {
		// doc/adr/0014-*.md: with FireAndForgetReplicas, try to run this
		// leg in the background instead of waiting for it — but only up
		// to maxInFlightBackgroundReplicaWrites; past that cap, fall back
		// to the synchronous path below exactly as with the option off.
		if c.fireAndForgetReplicas {
			select {
			case c.backgroundReplicaSem <- struct{}{}:
				// Register the background leg under c.mu, rechecking
				// c.closed: Close() sets c.closed under the same lock and
				// only then calls backgroundReplicaWG.Wait(), so this
				// ordering guarantees every Add happens-before that Wait.
				// Without it, a Set racing Close can call Add(1) just as
				// Wait() observes the counter at zero — which Go panics on
				// ("Add called concurrently with Wait"), crashing the
				// process. If Close already won, fall back to the
				// synchronous path so the write still completes.
				c.mu.Lock()
				if !c.closed {
					c.backgroundReplicaWG.Add(1)
					c.mu.Unlock()
					go func(replica string) {
						defer c.backgroundReplicaWG.Done()
						defer func() { <-c.backgroundReplicaSem }()
						replicaWrite(replica)
					}(name)
					continue
				}
				c.mu.Unlock()
				<-c.backgroundReplicaSem
			default:
			}
		}

		replicas.Add(1)
		go func(replica string) {
			defer replicas.Done()
			replicaWrite(replica)
		}(name)
	}

	err := c.applyReconnecting(names[0], primaryOp)
	replicas.Wait()
	return err
}

// ── 遅延再接続 ────────────────────────────────────────────────────

func (c *Client) slotConnection(slot string) (*connection, error) {
	address, current, err := c.snapshotSlot(slot)
	if err != nil {
		return nil, err
	}
	if !current.isClosed() {
		return current, nil
	}

	// Concurrent requests finding the same dead connection share one
	// dial: the first caller redials, the rest wait then reuse.
	c.redialMu.Lock()
	gate, ok := c.redialGates[slot]
	if !ok {
		gate = &sync.Mutex{}
		c.redialGates[slot] = gate
	}
	c.redialMu.Unlock()

	gate.Lock()
	defer gate.Unlock()

	address, current, err = c.snapshotSlot(slot)
	if err != nil {
		return nil, err
	}
	if !current.isClosed() {
		return current, nil
	}

	fresh, err := c.openNodeConnection(address)
	if err != nil {
		return nil, err
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		// Close() ran while we were dialing (issue #10): installing this
		// connection now would leak it past teardown.
		fresh.close()
		return nil, ErrClosed
	}
	if slot == "" {
		c.single = fresh
		return fresh, nil
	}
	if m, ok := c.members[slot]; ok {
		m.connection = fresh
		return fresh, nil
	}
	fresh.close()
	return nil, connectionLost(slot+" left the cluster while reconnecting", nil)
}

func (c *Client) snapshotSlot(slot string) (string, *connection, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if slot == "" {
		return c.singleAddress, c.single, nil
	}
	m, ok := c.members[slot]
	if !ok {
		return "", nil, connectionLost(slot+" has no open connection", nil)
	}
	return m.address, m.connection, nil
}

func (c *Client) openNodeConnection(address string) (*connection, error) {
	result, err := connectAndIdentify(address, c.authSecret, c.tlsConfig)
	if err != nil {
		return nil, err
	}
	if result.conn == nil {
		return nil, fmt.Errorf("nanocached: %s no longer identifies as a cache node", address)
	}
	if c.IsClosed() {
		_ = result.conn.Close()
		return nil, ErrClosed
	}
	return c.trackedConnection(result.conn), nil
}

// ── ノードリスト更新 ──────────────────────────────────────────────

func (c *Client) maybeRefresh(force bool) {
	c.mu.Lock()
	skip := c.ring == nil || (!force && time.Since(c.lastFetch) < nodeListStaleAfter)
	c.mu.Unlock()
	if skip {
		return
	}

	c.refreshMu.Lock()
	defer c.refreshMu.Unlock()
	c.mu.Lock()
	skip = !force && time.Since(c.lastFetch) < nodeListStaleAfter
	c.mu.Unlock()
	if skip {
		return
	}
	c.refreshNodeList()
}

func (c *Client) refreshNodeList() {
	nodes, replication, ok := c.fetchNodeList()

	c.mu.Lock()
	defer c.mu.Unlock()
	c.lastFetch = time.Now()
	if !ok || c.ring == nil {
		return
	}

	fresh := make(map[string]*member, len(nodes))
	names := make([]string, 0, len(nodes))
	for _, node := range nodes {
		names = append(names, node.Name)
		if existing, present := c.members[node.Name]; present {
			existing.address = node.Address
			fresh[node.Name] = existing
			delete(c.members, node.Name)
			continue
		}
		// Newly listed nodes are dialed lazily on first use
		// (slotConnection), keeping this refresh free of network I/O
		// under the lock.
		fresh[node.Name] = &member{address: node.Address, connection: deadConnection()}
	}
	// Nodes no longer listed: close their connections.
	for _, dropped := range c.members {
		dropped.connection.close()
	}

	c.members = fresh
	c.ring = NewHashRing(names)
	c.replication = replication

	// Node names are per-process UUIDs; departed nodes' redial gates
	// would otherwise accumulate forever (issue #12).
	c.redialMu.Lock()
	for slot := range c.redialGates {
		if slot == "" {
			continue
		}
		if _, live := fresh[slot]; !live {
			delete(c.redialGates, slot)
		}
	}
	c.redialMu.Unlock()
}

// fetchNodeList walks every configured address (ADR-0010); ok=false
// means keep the last-known list.
func (c *Client) fetchNodeList() ([]DiscoveredNode, int, bool) {
	for _, addr := range c.addresses {
		result, err := connectAndIdentify(addr.String(), c.authSecret, c.tlsConfig)
		if err != nil {
			continue
		}
		if result.conn != nil {
			_ = result.conn.Close()
			continue
		}
		if len(result.nodes) == 0 {
			continue
		}
		return result.nodes, result.replication, true
	}
	return nil, 0, false
}

// ── keep-alive ────────────────────────────────────────────────────

func (c *Client) startKeepalive(interval time.Duration) {
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-c.stopKeepalive:
				return
			case <-ticker.C:
			}

			c.mu.Lock()
			connections := make([]*connection, 0, len(c.members)+1)
			if c.single != nil {
				connections = append(connections, c.single)
			}
			for _, m := range c.members {
				connections = append(connections, m.connection)
			}
			c.mu.Unlock()

			for _, conn := range connections {
				if conn.isClosed() || conn.idle() < interval {
					continue // dead ones stay lazy; busy ones don't need a ping
				}
				// Any parseable reply proves liveness — N, or W from a
				// non-owner — and resets the server's idle timer.
				_, _, _ = conn.get(keepaliveKey)
			}
		}
	}()
}
