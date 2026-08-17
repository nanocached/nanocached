// Package nanocached is the Go client SDK for nanocached, a tiny
// distributed cache with client-side replication.
//
// A Config's Seeds may name either a single nanocached-node or discovery
// server(s) fronting a cluster — Connect finds out from the server's own
// handshake response (doc/adr/0007-*.md), so calling code is identical
// either way.
//
// Cluster mode implements ADR-0011 client-side replication: writes fan
// out to each key's top-R owners (the primary's result decides; a dead
// replica never fails a write), reads ask the primary and fall over to
// the next owner only when the holder is unreachable. Dead connections
// are redialed lazily on use (with one transparent retry — a socket only
// learns of a peer FIN on I/O, and every operation is idempotent), and an
// opt-in keep-alive can hold connections open across the server's 30s
// idle timeout.
//
// The Client is safe for concurrent use. Requests are serialized per
// connection; concurrent callers queue.
package nanocached

import (
	"crypto/tls"
	"errors"
	"fmt"
	"os"
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

// Config configures Connect.
type Config struct {
	// Seeds lists "host:port" targets, tried in order — one entry for a
	// single node, or every discovery replica (ADR-0010) for a cluster.
	Seeds []string
	// AuthSecret matches NANOCACHED_AUTH_SECRET on the server; empty
	// means no authentication configured.
	AuthSecret string
	// TLS, when non-nil, connects every socket over TLS with this config
	// (system roots by default; set RootCAs for a private CA).
	TLS *tls.Config
}

// keepAliveInterval is the always-on keep-alive cadence (issue #27):
// half the server's 30s idle timeout, so it never severs a healthy
// client. A variable only so tests can shorten it.
var keepAliveInterval = 15 * time.Second

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

	seeds      []string
	authSecret []byte
	tlsConfig  *tls.Config

	closed        bool
	stopKeepalive chan struct{}

	single        *connection // single-node mode
	singleAddress string
	members       map[string]*member // cluster mode
	ring          *HashRing
	replication   int
	lastFetch     time.Time
}

// Connect dials the first working seed and returns a ready client.
func Connect(config Config) (*Client, error) {
	if len(config.Seeds) == 0 {
		return nil, fmt.Errorf("nanocached: Connect needs at least one seed")
	}

	client := &Client{
		redialGates:   map[string]*sync.Mutex{},
		seeds:         append([]string(nil), config.Seeds...),
		tlsConfig:     config.TLS,
		members:       map[string]*member{},
		replication:   1,
		lastFetch:     time.Now(),
		stopKeepalive: make(chan struct{}),
	}
	if config.AuthSecret != "" {
		client.authSecret = []byte(config.AuthSecret)
	}

	// Walk the seeds until one yields a working target; a seed that is
	// unreachable, warming up (B, ADR-0010), or knows no live nodes is
	// skipped — the next replica may do better.
	var lastError error
	for _, seed := range client.seeds {
		result, err := connectAndIdentify(seed, client.authSecret, client.tlsConfig)
		if err != nil {
			lastError = err
			continue
		}

		if result.conn != nil {
			if len(client.seeds) > 1 {
				fmt.Fprintf(os.Stderr,
					"nanocached: %s is a cache node, so this client is pinned to that single "+
						"server — the remaining seed(s) will not be used. Point seeds at "+
						"discovery servers for cluster routing and failover.\n", seed)
			}
			client.single = newConnection(result.conn)
			client.singleAddress = seed
			client.startKeepalive(keepAliveInterval)
			return client, nil
		}

		if len(result.nodes) == 0 {
			lastError = fmt.Errorf(
				"nanocached: no live nodes registered with the discovery server at %s", seed)
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
		lastError = fmt.Errorf("nanocached: could not connect to any seed")
	}
	return nil, lastError
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

// Get returns the key's value; ok is false when the key is missing.
func (c *Client) Get(key string) (value []byte, ok bool, err error) {
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
	return value, ok, err
}

// Set stores the value under the key. A zero ttl means no expiry.
func (c *Client) Set(key string, value []byte, ttl time.Duration) error {
	if ttl < 0 {
		return fmt.Errorf("nanocached: ttl must not be negative, got %v", ttl)
	}
	if err := c.beforeOperation(); err != nil {
		return err
	}
	ttlSeconds := int64(-1) // no expiry
	if ttl > 0 {
		// Round sub-second TTLs UP (issue #9): truncation turned e.g.
		// 300ms into an explicit 0-second TTL — near-immediate expiry —
		// silently changing the caller's intent. TTL granularity on the
		// wire is whole seconds.
		ttlSeconds = int64((ttl + time.Second - 1) / time.Second)
	}
	keyBytes := []byte(key)
	return c.withClusterRetry(func() error {
		return c.write(keyBytes, func(conn *connection) error {
			return conn.set(keyBytes, value, ttlSeconds)
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
		return c.write(keyBytes, func(conn *connection) error {
			var opErr error
			existed, opErr = conn.delete(keyBytes)
			return opErr
		})
	})
	return existed, err
}

// Close is idempotent; later Get/Set/Delete return ErrClosed.
func (c *Client) Close() {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return
	}
	c.closed = true
	close(c.stopKeepalive)
	c.mu.Unlock()
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
// the server's 30s idle timeout) on I/O, so lazy reconnect-on-use means
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

func (c *Client) write(key []byte, op func(*connection) error) error {
	c.mu.Lock()
	single := c.ring == nil
	c.mu.Unlock()
	if single {
		return c.applyReconnecting("", op)
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
	var replicas sync.WaitGroup
	for _, name := range names[1:] {
		replicas.Add(1)
		go func(replica string) {
			defer replicas.Done()
			_ = c.applyReconnecting(replica, op)
		}(name)
	}

	err := c.applyReconnecting(names[0], op)
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
	return newConnection(result.conn), nil
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

// fetchNodeList walks every seed (ADR-0010); ok=false means keep the
// last-known list.
func (c *Client) fetchNodeList() ([]DiscoveredNode, int, bool) {
	for _, seed := range c.seeds {
		result, err := connectAndIdentify(seed, c.authSecret, c.tlsConfig)
		if err != nil {
			fmt.Fprintf(os.Stderr, "nanocached: could not refresh the node list from %s: %v\n", seed, err)
			continue
		}
		if result.conn != nil {
			_ = result.conn.Close()
			fmt.Fprintf(os.Stderr, "nanocached: %s no longer identifies as a discovery server\n", seed)
			continue
		}
		if len(result.nodes) == 0 {
			fmt.Fprintf(os.Stderr, "nanocached: discovery at %s returned no live nodes, skipping\n", seed)
			continue
		}
		return result.nodes, result.replication, true
	}
	fmt.Fprintln(os.Stderr, "nanocached: no discovery seed could provide a node list, keeping the last-known list")
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
