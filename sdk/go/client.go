// Package nanocached is the Go client SDK for nanocached, a tiny
// distributed cache with client-side replication.
//
// A Config's Addresses may name either a single nanocached-node or
// discovery server(s) fronting a cluster — Connect finds out from the
// server's own handshake response (the server type in the auth response), so calling code
// is identical either way.
//
// Cluster mode implements client-side replication client-side replication: writes fan
// out to each key's top-R owners (the primary's result decides; a dead
// replica never fails a write), reads ask the primary and fall over to
// the next owner only when the holder is unreachable. Dead connections
// are redialed lazily on use (with one transparent retry — a socket only
// learns of a peer FIN on I/O, and every operation is idempotent), and an
// opt-in keep-alive can hold connections open across the server's 60s
// idle timeout.
//
// The Client is safe for concurrent use. Requests are pipelined per
// connection (request pipelining): concurrent callers on the same
// connection each pay only their own network latency, not everyone
// else's ahead of them.
//
// Client.Namespace(ns) returns a lightweight *Namespace handle scoping
// Get/Set/Delete to ns — the same key name in different namespaces (or
// in a namespace versus no namespace at all) names independent entries
// (issue #105: first-class namespaces). The namespace-less methods on
// Client remain the default and are unaffected; namespace("") is
// equivalent to using Client directly.
//
// Config.ViaProxy (issue #122) connects through a nanocached-proxy
// fronting the cluster instead of joining the ring directly: Addresses
// still names discovery server(s), but Connect fetches the registered
// proxy roster and picks one at random rather than fetching the node
// roster and building a ring, and the client then runs in its ordinary
// single-connection mode for its whole lifetime — see Config.ViaProxy's
// own doc comment for the full connect/reconnect/caveats story.
package nanocached

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"math/rand/v2"
	"net"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// nodeListStaleAfter is how long the node list may go without a re-fetch
// from discovery before Get/Set/Delete refreshes it first (checked lazily
// on use).
const nodeListStaleAfter = 30 * time.Second

// keepaliveKey is reserved by the SDKs precisely so a real application
// key can never collide with it: a leading 0x00 already keeps it out of
// any UTF-8 key space, and "nanocached-keepalive" makes an accidental
// binary-key collision vanishingly unlikely too. Collision would matter
// because a G does refresh the server-side LRU recency of whatever key
// it names — colliding with a real key would silently keep that key
// artificially "hot" on every keep-alive tick.
var keepaliveKey = []byte("\x00nanocached-keepalive")

// Address is a "host:port" connect target: a single nanocached-node, or
// one discovery replica (discovery HA) fronting a cluster.
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
	// single node, or every discovery replica (discovery HA) for a cluster.
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
	// Get/GetBytes (value compression). Off by default. Every client that
	// reads or writes a given set of keys must agree on Compress — it is
	// a per-keyspace format decision, not a per-client preference.
	Compress bool
	// CompressionThreshold is the byte length at or above which Compress
	// actually compresses a value; below it (or when compressing doesn't
	// shrink the value) the value is stored as-is behind the marker
	// byte, to avoid bloating small values with compression overhead.
	// Zero means DefaultCompressionThreshold. Only meaningful when
	// Compress is true. Negative is rejected by Connect.
	CompressionThreshold int
	// FireAndForgetReplicas lets Set/SetBytes/Delete return as soon as
	// the primary owner acks, letting replica legs finish in the
	// background instead of waiting for them too (fire-and-forget replica writes).
	// Off by default. Unlike Compress, this is a pure latency/durability
	// trade for this client's own writes — it carries no wire format and
	// needs no agreement with other clients.
	FireAndForgetReplicas bool
	// ReadRepair probes the remaining owners on a clean primary miss and
	// repairs the gap in the background if one still holds the value
	// (read repair). Off by default. Costs extra reads only on the
	// misses it actually applies to.
	ReadRepair bool
	// ReadHedgeAfter routes a Get/GetBytes around a slow — not dead —
	// owner (hedged reads): if the primary hasn't answered within this
	// long, the same read is also sent to the next owner (and so on, one
	// more owner per interval, until every owner is in flight); the first
	// answer decides, except that a miss is only final coming from the
	// primary — a replica's miss is provisional (it may simply lack the
	// copy), so hedging can never turn a hit into a miss. Zero (the
	// default) disables hedging: the read then bounds on whichever owner
	// it happens to touch, exactly as before this option existed. Only
	// meaningful once a ring is known and the key has at least two owners
	// (Replication() >= 2); a single-node client, or a key with only one
	// owner, is unaffected either way. Negative is rejected by Connect.
	// Losing legs are never cancelled — cancelling mid-write would poison
	// a shared connection (see connection.request) — they run to
	// completion detached, tracked so Close() waits for them exactly like
	// a fire-and-forget replica write.
	ReadHedgeAfter time.Duration
	// ReconnectCooldown is how long, after a reconnect dial to an address
	// fails, that address is treated as still down — a request routed to
	// it during this window fails immediately with the original dial
	// error instead of paying another full 5-second connect deadline
	// redialing an address that just proved unreachable. Zero means
	// DefaultReconnectCooldown (1s) — the zero value of Config can't
	// distinguish "not specified" from "explicitly zero", so zero has to
	// mean "default". Keep it well under nodeListStaleAfter (30s) so a
	// node that genuinely recovers isn't shut out for long. A negative
	// value disables the cooldown entirely — every request that finds a
	// dead connection pays its own full dial attempt. This mirrors the
	// Rust SDK's Options::reconnect_cooldown (where Duration::ZERO also
	// means "default") and its Options::disable_reconnect_cooldown()
	// (the equivalent of a negative value here).
	ReconnectCooldown time.Duration
	// ViaProxy connects through a nanocached-proxy fronting the cluster
	// instead of joining the ring directly (issue #122). Only meaningful
	// when Addresses names discovery server(s): Connect fetches the
	// registered proxy roster (`Q`, discovery's ListProxies command)
	// instead of the node roster (`L`), and connects to one proxy chosen
	// at random — spreading a fleet of clients across the proxy fleet —
	// failing over through the rest, still in random order, if the first
	// choice can't be reached. Pointing ViaProxy at a plain node address
	// (the SDK's identify handshake tells the two apart) fails Connect
	// with a clear error, since there is no roster to fetch from a node.
	// A proxy answers the identify handshake exactly like a single node
	// that owns every key, so from then on the client runs in its
	// ordinary single-connection mode: no ring, no per-node connections,
	// and — since a single connection has no replicas to hedge to — a
	// configured ReadHedgeAfter is inert (every other option — Compress,
	// FireAndForgetReplicas, ReadRepair, namespaces, clear/clear-all,
	// keep-alive — is unaffected). Losing the proxy connection first
	// retries the same proxy (it may simply have restarted); only if
	// that also fails does the client re-fetch the roster and pick
	// another at random. Off by default.
	ViaProxy bool
}

// String implements fmt.Stringer, redacting AuthSecret so a Config never
// leaks its shared secret through logging, error messages, or %v/%s
// formatting (issue #47 audit item G3) — a Config is otherwise a
// tempting thing to log wholesale for debugging.
func (c Config) String() string {
	secret := "unset"
	if c.AuthSecret != "" {
		secret = "REDACTED"
	}
	return fmt.Sprintf(
		"Config{Addresses:%v AuthSecret:%s TLS:%v CA:%q Compress:%v CompressionThreshold:%d "+
			"FireAndForgetReplicas:%v ReadRepair:%v ReadHedgeAfter:%v ReconnectCooldown:%v ViaProxy:%v}",
		c.Addresses, secret, c.TLS, c.CA, c.Compress, c.CompressionThreshold,
		c.FireAndForgetReplicas, c.ReadRepair, c.ReadHedgeAfter, c.ReconnectCooldown, c.ViaProxy)
}

// GoString implements fmt.GoStringer so %#v also redacts AuthSecret —
// without this, %#v bypasses String() entirely and would print the
// secret in plain text.
func (c Config) GoString() string {
	return c.String()
}

// DefaultCompressionThreshold is the CompressionThreshold used when
// Config.CompressionThreshold is left at zero.
const DefaultCompressionThreshold = 256

// DefaultReconnectCooldown is the ReconnectCooldown used when
// Config.ReconnectCooldown is left at zero.
const DefaultReconnectCooldown = time.Second

// maxRequestBytes bounds key+value size before any network I/O. The
// server's own request cap (src/server.rs's MAX_REQUEST_SIZE) is 1 MiB
// for the *entire* frame — header line plus key plus value; a request
// over that limit is rejected by simply closing the connection without a
// response, which poisons whatever else is pipelined behind it on that
// same connection. This reserves 256 bytes of headroom for the header
// itself (marker byte, decimal lengths, an optional TTL, echoed response tags's tag
// field, spaces, the trailing newline — always comfortably under this
// even for the largest fields), so a key+value that clears maxRequestBytes
// is guaranteed to fit under the server's own cap (issue #47 audit item
// G1).
const maxRequestBytes = 1024*1024 - 256

// validateKey rejects an empty key, or one that alone already exceeds
// maxRequestBytes, before any network I/O: the server has no way to
// answer either shape except by closing the connection outright, silently
// poisoning every other request already pipelined on that connection.
// The size bound matters here specifically because GetBytes and Delete
// call validateKey directly (they have no value to combine it with, unlike
// validateKeyAndValue) — without it, an oversized key on GET/DELETE would
// sail past client-side validation and only be caught by the server
// slamming the connection shut (issue #47 audit item G1 follow-up; matches
// protocol.ts's checkKey and client.py's _check_key, which both fold this
// bound into the key-only validator rather than leaving it to the
// key+value check alone). Matches the style of the ttlSeconds < 0 check in
// SetBytes.
func validateKey(key string) error {
	if len(key) == 0 {
		return invalidArgument("nanocached: key must not be empty")
	}
	if len(key) > maxRequestBytes {
		return invalidArgument(fmt.Sprintf(
			"nanocached: key exceeds maxRequestBytes (%d bytes), got %d bytes",
			maxRequestBytes, len(key)))
	}
	return nil
}

// validateKeyAndValue is validateKey plus a maxRequestBytes bound on
// len(key)+valueLen — anything past it can never fit the server's own
// request cap, so failing fast here is strictly better than sending a
// frame the server can only reject by silently closing the connection.
func validateKeyAndValue(key string, valueLen int) error {
	if err := validateKey(key); err != nil {
		return err
	}
	if len(key)+valueLen > maxRequestBytes {
		return invalidArgument(fmt.Sprintf(
			"nanocached: key (%d bytes) + value (%d bytes) exceeds the %d-byte request limit",
			len(key), valueLen, maxRequestBytes))
	}
	return nil
}

// maxInFlightBackgroundReplicaWrites bounds how many replica writes a
// single client may have running in the background at once when
// FireAndForgetReplicas is enabled (fire-and-forget replica writes) — once the cap is
// reached, further replica legs fall back to running synchronously, the
// same as with the option off. A variable only so tests can shrink it.
var maxInFlightBackgroundReplicaWrites = 32

// keepAliveInterval is the always-on keep-alive cadence (issue #27):
// half the server's 60s idle timeout, so it never severs a healthy
// client. A variable only so tests can shorten it.
var keepAliveInterval = 30 * time.Second

// readRepairTTL is the TTL a read-repair write applies to the primary
// (read repair). G's response carries no TTL, so the key's
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

// redialCooldown records a failed dial's outcome for one address (see
// Client.reconnectCooldown): the deadline the address stays "down" until,
// and the error to hand back verbatim to every caller that hits the
// cooldown window.
type redialCooldown struct {
	until time.Time
	err   error
}

// Stats holds counters for failures this SDK deliberately swallows
// (client-side replication / fire-and-forget replica writes / read repair) — observability for silently degrading
// replication or a stuck node-list refresh that would otherwise have no
// visible symptom until reads start missing more often than expected.
// Every field is monotonic and never reset.
type Stats struct {
	ReplicaWriteFailures uint64
	ReadRepairFailures   uint64
	RefreshFailures      uint64
	// TransientRetries counts every retryable-error status `R` this
	// client has received (issue #125) — one nanocached-proxy (today, the
	// only server that sends it) reported a single request failed
	// transiently and asked for a retry on the same connection. Counted
	// whether or not the retry that followed it succeeded, so this can
	// run ahead of any visible error: a request that ultimately succeeded
	// after one `R` still bumps this once.
	TransientRetries uint64
}

// clientStats holds the live, atomically-updated counters Stats()
// snapshots; kept separate from the exported Stats so the atomic types
// stay an implementation detail.
type clientStats struct {
	replicaWriteFailures atomic.Uint64
	readRepairFailures   atomic.Uint64
	refreshFailures      atomic.Uint64
	transientRetries     atomic.Uint64
}

// Client is a nanocached client handle.
type Client struct {
	mu          sync.Mutex // guards single/members/ring/replication/lastFetch
	refreshMu   sync.Mutex
	redialMu    sync.Mutex
	redialGates map[string]*sync.Mutex

	// reconnectCooldown and redialCooldowns implement the per-address
	// reconnect cooldown (see Config.ReconnectCooldown): the address of
	// the most recently failed dial, and how long it stays "down" before
	// another dial to it is attempted. Keyed by address, not slot — a
	// cluster refresh can reassign a slot (node name) to a different
	// address, but the address itself is what's actually unreachable.
	// <= 0 disables the cooldown.
	reconnectCooldown time.Duration
	redialCooldownMu  sync.Mutex
	redialCooldowns   map[string]redialCooldown

	stats clientStats

	addresses  []Address
	authSecret []byte
	tlsConfig  *tls.Config

	compress             bool
	compressionThreshold int

	fireAndForgetReplicas bool
	// backgroundReplicaSem bounds in-flight background replica writes;
	// backgroundReplicaWG lets Close() drain them before tearing down
	// connections (fire-and-forget replica writes).
	backgroundReplicaSem chan struct{}
	backgroundReplicaWG  sync.WaitGroup

	readRepair bool

	// readHedgeAfter is Config.ReadHedgeAfter (hedged reads); <= 0 disables
	// hedging. hedgedReadsWG lets Close() drain hedging's losing legs
	// before tearing down connections, exactly like backgroundReplicaWG
	// does for fire-and-forget replica writes — unlike that pool, hedge
	// legs aren't capped by a semaphore, since at most len(names)-1 of
	// them can ever be in flight per read.
	readHedgeAfter time.Duration
	hedgedReadsWG  sync.WaitGroup

	// targetKey is the address this client's connect() ultimately settled
	// on — a node's own address in single mode, the winning discovery
	// server's address in cluster mode (and in proxy mode, issue #122:
	// the discovery seed the proxy was fetched from, not the proxy's own
	// address, which can change across a proxy failover). Every socket
	// this client ever opens (initial connect, lazy reconnect, newly
	// discovered members) is tracked in openTargets under this one key,
	// mirroring the TypeScript SDK's `this.url`.
	targetKey string

	// viaProxy is Config.ViaProxy (issue #122): true means this client's
	// single connection (see single/singleAddress below — proxy mode
	// never populates ring/members, exactly like plain single-node mode)
	// is to a nanocached-proxy discovered via discovery's `Q`, and a lost
	// connection's reconnect should fail over to another proxy — chosen
	// by re-fetching `Q` — rather than only ever retrying singleAddress.
	// See connectViaProxy and dialSlot.
	viaProxy bool

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
func (c *Client) trackedConnection(netConn net.Conn, tagged bool) *connection {
	key := c.targetKey
	trackOpenTarget(key)
	return newConnection(netConn, func() { untrackOpenTarget(key) }, tagged, &c.stats.transientRetries)
}

// Connect dials the first working address and returns a ready client.
func Connect(config Config) (*Client, error) {
	if len(config.Addresses) == 0 {
		return nil, fmt.Errorf("nanocached: connect() needs a non-empty addresses list")
	}
	if config.CompressionThreshold < 0 {
		return nil, fmt.Errorf(
			"nanocached: CompressionThreshold must not be negative, got %d", config.CompressionThreshold)
	}
	if config.ReadHedgeAfter < 0 {
		return nil, fmt.Errorf(
			"nanocached: ReadHedgeAfter must not be negative, got %v", config.ReadHedgeAfter)
	}
	// ReadHedgeAfter isn't rejected alongside ViaProxy: it's merely inert
	// there (see Config.ViaProxy's doc), not a misconfiguration — a
	// caller that toggles ViaProxy per environment shouldn't also have to
	// conditionally unset ReadHedgeAfter.

	tlsConfig, err := buildTLSConfig(config)
	if err != nil {
		return nil, err
	}

	compressionThreshold := config.CompressionThreshold
	if compressionThreshold == 0 {
		compressionThreshold = DefaultCompressionThreshold
	}

	reconnectCooldown := config.ReconnectCooldown
	if reconnectCooldown == 0 {
		reconnectCooldown = DefaultReconnectCooldown
	}

	client := &Client{
		redialGates:           map[string]*sync.Mutex{},
		reconnectCooldown:     reconnectCooldown,
		redialCooldowns:       map[string]redialCooldown{},
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
		readHedgeAfter:        config.ReadHedgeAfter,
		viaProxy:              config.ViaProxy,
	}
	if config.AuthSecret != "" {
		client.authSecret = []byte(config.AuthSecret)
	}

	if config.ViaProxy {
		// Issue #122: an entirely separate connect flow — a `Q` roster
		// fetch and a random pick among proxies, rather than `L` and
		// ring construction — so it doesn't tangle with the node/cluster
		// loop below. Ends in the same single-connection state
		// Connect's plain single-node path does (client.single set,
		// client.ring left nil), so every other method (Get/Set/Delete,
		// keep-alive, Close) needs no proxy-mode awareness at all.
		if err := client.connectViaProxy(); err != nil {
			client.teardown()
			return nil, err
		}
		client.startKeepalive(keepAliveInterval)
		return client, nil
	}

	// Walk the addresses until one yields a working target; an address
	// that is unreachable, warming up (B, discovery HA), or knows no live
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
			client.single = client.trackedConnection(result.conn, result.tagged)
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

// clusterDialOutcome is one node's identify result from openCluster's
// concurrent dial round: exactly one of err/result.conn is set on
// success, matching connectAndIdentify's own contract.
type clusterDialOutcome struct {
	result *identified
	err    error
}

// openCluster dials every node discovery listed, concurrently — the
// per-dial timeout is connectDeadline either way, so doing this
// concurrently instead of one at a time is purely a latency win once more
// than one node is unreachable. A node that can't be reached (issue #67:
// typically one that just died and discovery hasn't evicted yet — its
// liveness window is seconds long, and every key is still served by
// another owner when replication > 1) is installed as a member with no
// live connection — the same deadConnection placeholder a freshly
// discovered node gets in refreshNodeList — and its reconnect cooldown
// armed, exactly the state a member is in after dying mid-life: requests
// for its keys fail over per request instead of the whole Connect
// failing, and the next request after the cooldown redials it. Only a
// cluster with *no* reachable node fails Connect, with the last dial
// error. A listed address that identifies as something other than a node
// (a discovery misconfiguration, not a transient failure) remains a hard,
// non-tolerated error, checked before anything is installed so it can't
// leak a socket successfully opened for a different node in the same
// dial round.
func (c *Client) openCluster(result *identified) error {
	nodes := result.nodes
	outcomes := make([]clusterDialOutcome, len(nodes))
	var wg sync.WaitGroup
	wg.Add(len(nodes))
	for i, node := range nodes {
		go func(i int, address string) {
			defer wg.Done()
			ident, err := connectAndIdentify(address, c.authSecret, c.tlsConfig)
			outcomes[i] = clusterDialOutcome{result: ident, err: err}
		}(i, node.Address)
	}
	wg.Wait()

	for i, outcome := range outcomes {
		if outcome.err == nil && outcome.result.conn == nil {
			for _, other := range outcomes {
				if other.err == nil && other.result.conn != nil {
					_ = other.result.conn.Close()
				}
			}
			return fmt.Errorf(
				"nanocached: discovery server returned a non-node address: %s", nodes[i].Address)
		}
	}

	names := make([]string, 0, len(nodes))
	reachable := 0
	var lastError error
	for i, node := range nodes {
		names = append(names, node.Name)
		outcome := outcomes[i]

		if outcome.err != nil {
			c.members[node.Name] = &member{address: node.Address, connection: deadConnection()}
			if c.reconnectCooldown > 0 {
				c.redialCooldownMu.Lock()
				c.redialCooldowns[node.Address] = redialCooldown{
					until: time.Now().Add(c.reconnectCooldown), err: outcome.err,
				}
				c.redialCooldownMu.Unlock()
			}
			lastError = outcome.err
			continue
		}

		c.members[node.Name] = &member{
			address:    node.Address,
			connection: c.trackedConnection(outcome.result.conn, outcome.result.tagged),
		}
		reachable++
	}

	if reachable == 0 {
		if lastError == nil {
			lastError = fmt.Errorf("nanocached: could not connect to any address")
		}
		return lastError
	}

	c.ring = NewHashRing(names)
	c.replication = result.replication
	return nil
}

// ── プロキシモード (issue #122) ──────────────────────────────────────

// connectViaProxy implements Config.ViaProxy's connect flow: reach a
// discovery seed (the same seed-iteration and B/warming-up handling
// Connect's node/cluster path uses), fetch the registered nanocached-proxy
// roster (`Q`), and connect to one of them, chosen at random — spreading
// a fleet of clients across the proxy fleet instead of every client
// piling onto whichever proxy happens to be listed first. A discovery
// seed that identifies a listed address as a plain node (not itself —
// the seed is already known to be discovery by the time result.nodes is
// populated) is a hard misconfiguration and fails Connect immediately
// rather than falling through to the next seed, the same treatment
// openCluster gives a non-node address in an `L` reply.
func (c *Client) connectViaProxy() error {
	var lastError error
	for _, seed := range c.addresses {
		key := seed.String()
		result, err := connectAndIdentifyProxies(key, c.authSecret, c.tlsConfig)
		if err != nil {
			lastError = err
			continue
		}

		if result.conn != nil {
			// ViaProxy needs a discovery address, not a node's — unlike
			// the plain single-node path (which happily pins to a lone
			// node address), this is exactly the misconfiguration
			// Config.ViaProxy's doc calls out, so fail fast instead of
			// silently connecting to the wrong thing.
			_ = result.conn.Close()
			return fmt.Errorf(
				"nanocached: ViaProxy requires a discovery server address, but %s identifies as "+
					"a cache node", key)
		}

		if len(result.nodes) == 0 {
			lastError = fmt.Errorf(
				"nanocached: no proxies registered with the discovery server at %s", key)
			continue
		}

		// targetKey is the discovery seed, not whichever proxy is chosen
		// below — see its own doc comment: it must stay stable across a
		// later proxy failover for the forgotten-close tracker to mean
		// anything.
		c.targetKey = key
		conn, address, err := c.dialRandomProxy(result.nodes)
		if err != nil {
			return err
		}
		c.single = conn
		c.singleAddress = address
		return nil
	}

	if lastError == nil {
		lastError = fmt.Errorf("nanocached: could not connect to any address")
	}
	return lastError
}

// dialRandomProxy connects to one of proxies, tried in a random order
// (math/rand/v2's top-level functions are auto-seeded as of Go 1.22, so
// no explicit seeding is needed) so that repeated Connect calls across a
// client fleet spread themselves over every registered proxy rather than
// converging on one. A dial failure fails over to the next candidate in
// that same random order, exactly as the initial connect's own
// seed-iteration does for discovery addresses; every candidate
// unreachable (or, oddly, no longer identifying as a node/proxy) returns
// the last such failure.
func (c *Client) dialRandomProxy(proxies []discoveredNode) (conn *connection, address string, err error) {
	var lastError error
	for _, i := range rand.Perm(len(proxies)) {
		candidate := proxies[i].Address
		result, dialErr := connectAndIdentify(candidate, c.authSecret, c.tlsConfig)
		if dialErr != nil {
			lastError = dialErr
			continue
		}
		if result.conn == nil {
			// discovery's own ProxyAnnounce bookkeeping said this was a
			// proxy; if it no longer identifies as one, treat it like any
			// other unreachable candidate rather than erroring out
			// early — another registered proxy may still be reachable.
			lastError = fmt.Errorf("nanocached: %s no longer identifies as a proxy", candidate)
			continue
		}
		return c.trackedConnection(result.conn, result.tagged), candidate, nil
	}
	if lastError == nil {
		lastError = fmt.Errorf("nanocached: no proxies registered with discovery")
	}
	return nil, "", lastError
}

// fetchProxyList walks every configured discovery address (discovery HA)
// for the registered-proxy roster (`Q`), mirroring fetchNodeList's own
// address-walk exactly — ok=false just means none could be reached (or
// identified as discovery, or listed any proxies) right now.
func (c *Client) fetchProxyList() ([]discoveredNode, bool) {
	for _, addr := range c.addresses {
		result, err := connectAndIdentifyProxies(addr.String(), c.authSecret, c.tlsConfig)
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
		return result.nodes, true
	}
	return nil, false
}

// ── 公開 API ──────────────────────────────────────────────────────

// Replication reports how many nodes hold each key (client-side replication) — 1
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

// Stats returns a snapshot of counters for failures this SDK swallows by
// design (client-side replication / fire-and-forget replica writes / read repair) — lets operators detect silently
// degrading replication or a stuck node-list refresh.
func (c *Client) Stats() Stats {
	return Stats{
		ReplicaWriteFailures: c.stats.replicaWriteFailures.Load(),
		ReadRepairFailures:   c.stats.readRepairFailures.Load(),
		RefreshFailures:      c.stats.refreshFailures.Load(),
		TransientRetries:     c.stats.transientRetries.Load(),
	}
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
// (value compression). With Config.ReadRepair, a clean miss probes the
// remaining owners before being accepted as final, repairing the gap in
// the background if one still holds the value (read repair).
func (c *Client) GetBytes(key string) (value []byte, ok bool, err error) {
	return c.getBytesNS(nil, key)
}

// getBytesNS is GetBytes scoped to namespace (issue #105: first-class
// namespaces) — the internal (namespace, key) entry point every
// namespace-aware operation funnels through, including a *Namespace
// handle's own GetBytes, so routing, replication, hedging, tagging,
// compression and read repair all stay in exactly one place regardless
// of which namespace a caller is in. A nil/empty namespace is the
// default namespace, byte-identical on the wire to a pre-#105 client.
func (c *Client) getBytesNS(namespace []byte, key string) (value []byte, ok bool, err error) {
	if err := validateKey(key); err != nil {
		return nil, false, err
	}
	if err := c.beforeOperation(); err != nil {
		return nil, false, err
	}
	keyBytes := []byte(key)
	err = c.withClusterRetry(func() error {
		v, o, readErr := c.read(namespace, keyBytes, func(conn *connection) ([]byte, bool, error) {
			return conn.getNS(namespace, keyBytes)
		})
		value, ok = v, o
		return readErr
	})
	if err == nil && !ok && c.readRepair {
		value, ok = c.tryReadRepair(namespace, keyBytes)
	}
	if err != nil || !ok || !c.compress {
		return value, ok, err
	}
	value, err = decompressValue(value)
	return value, ok, err
}

// tryReadRepair probes the remaining owners of key — every owner but the
// primary, which the normal read path already probed and got a clean miss
// from — in rank order, for a value. The first one that has it wins: its
// value is returned, and a best-effort write repairs the primary in the
// background (read repair) with readRepairTTL. The background write
// is bounded and drained exactly like a fire-and-forget replica write: it
// takes a backgroundReplicaSem slot and is tracked on backgroundReplicaWG
// so Close() waits for it, and no more than
// maxInFlightBackgroundReplicaWrites run at once. Past the cap the repair
// for this miss is simply skipped — it's opportunistic, so a later miss
// repairs the key instead, and it must never add latency or unbounded
// goroutine growth to the read it rides on. Every failure along the way
// (connection lost, WrongNode, another miss) is swallowed; nothing here may
// turn an already-accepted miss into an error. A failed repair write is
// counted in Stats().ReadRepairFailures.
func (c *Client) tryReadRepair(namespace, key []byte) (value []byte, ok bool) {
	names := c.ownerNames(namespace, key)
	if len(names) == 0 {
		return nil, false
	}
	for _, name := range names[1:] {
		v, found, err := c.get(name, namespace, key)
		if err != nil || !found {
			continue
		}
		if len(names) > 0 {
			primary := names[0]
			repair := func() {
				if err := c.applyReconnecting(primary, func(conn *connection) error {
					return conn.setNS(namespace, key, v, readRepairTTL)
				}); err != nil {
					c.stats.readRepairFailures.Add(1)
				}
			}
			select {
			case c.backgroundReplicaSem <- struct{}{}:
				// Register under c.mu, rechecking c.closed — the same
				// ordering the replica path uses to guarantee every Add
				// happens-before Close()'s Wait (and to avoid the
				// "Add called concurrently with Wait" panic). If Close
				// already won, release the slot and skip: unlike a replica
				// write there's no synchronous fallback, since a missed
				// repair is harmless and this must not delay teardown.
				c.mu.Lock()
				if !c.closed {
					c.backgroundReplicaWG.Add(1)
					c.mu.Unlock()
					go func() {
						defer c.backgroundReplicaWG.Done()
						defer func() { <-c.backgroundReplicaSem }()
						repair()
					}()
				} else {
					c.mu.Unlock()
					<-c.backgroundReplicaSem
				}
			default:
			}
		}
		return v, true
	}
	return nil, false
}

func (c *Client) get(slot string, namespace, key []byte) (value []byte, ok bool, err error) {
	err = c.applyReconnecting(slot, func(conn *connection) error {
		var opErr error
		value, ok, opErr = conn.getNS(namespace, key)
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
// when Config.Compress is enabled (value compression).
func (c *Client) SetBytes(key string, value []byte, ttlSeconds int64) error {
	return c.setBytesNS(nil, key, value, ttlSeconds)
}

// setBytesNS is SetBytes scoped to namespace — see getBytesNS's doc
// comment; the same internal (namespace, key) entry point a *Namespace
// handle's SetBytes forwards to.
func (c *Client) setBytesNS(namespace []byte, key string, value []byte, ttlSeconds int64) error {
	if err := validateKeyAndValue(key, len(value)); err != nil {
		return err
	}
	if ttlSeconds < 0 {
		return invalidArgument(fmt.Sprintf("nanocached: ttlSeconds must not be negative, got %d", ttlSeconds))
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
		return c.write(namespace, keyBytes, func(conn *connection, _ bool) error {
			return conn.setNS(namespace, keyBytes, outgoing, wireTTL)
		})
	})
}

// Delete removes the key, reporting whether it existed before this call.
func (c *Client) Delete(key string) (existed bool, err error) {
	return c.deleteNS(nil, key)
}

// deleteNS is Delete scoped to namespace — see getBytesNS's doc comment;
// the same internal (namespace, key) entry point a *Namespace handle's
// Delete forwards to.
func (c *Client) deleteNS(namespace []byte, key string) (existed bool, err error) {
	if err := validateKey(key); err != nil {
		return false, err
	}
	if err := c.beforeOperation(); err != nil {
		return false, err
	}
	keyBytes := []byte(key)
	err = c.withClusterRetry(func() error {
		return c.write(namespace, keyBytes, func(conn *connection, primary bool) error {
			e, opErr := conn.deleteNS(namespace, keyBytes)
			// Only the primary's answer decides (client-side replication) — and only the
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

// clearNS drops every entry in namespace across every node — the same
// internal entry point a *Namespace handle's Clear forwards to (issue
// #106). A nil/empty namespace clears the default namespace; it is never
// rejected, matching getBytesNS/setBytesNS/deleteNS's own namespace("")
// rule.
func (c *Client) clearNS(namespace []byte) error {
	if err := c.beforeOperation(); err != nil {
		return err
	}
	return c.clearFanout(func(conn *connection) error {
		return conn.clear(namespace)
	})
}

// ClearAll drops every namespace across every node, the default one
// included (issue #106's `F`). Unlike Get/Set/Delete this touches the
// whole keyspace at once, so there is no Namespace-scoped equivalent
// beyond Namespace.Clear (one namespace) — see clearFanout for the
// fan-out and failure semantics both share.
func (c *Client) ClearAll() error {
	if err := c.beforeOperation(); err != nil {
		return err
	}
	return c.clearFanout(func(conn *connection) error {
		return conn.clearAll()
	})
}

// clearFanout runs op (a `c`/`F` request already bound to its namespace
// or lack thereof) against every node the client currently knows about.
// Unlike Get/Set/Delete a clear isn't key-addressed — there's no HRW
// owner ranking to pick a primary/replica split from, no `W` ever
// answers, and a namespace's keys are spread over every member node by
// rendezvous hashing — so it fans out to the whole membership rather
// than a per-key owner list (docs/protocol.html's "c / F"). In single
// mode there is only ever the one node to send it to.
//
// Success requires every node to ack `C`. On any failure (connection
// error, no/invalid ack, timeout) the node list is refreshed once — the
// same refresh path W / a dead primary uses elsewhere — and every node
// of the *refreshed* list (which may differ: a node can have joined or
// left) is retried once more. Because clear is idempotent, replaying it
// against nodes that already succeeded in the first round is harmless. A
// second round of failures raises ErrConnectionLost naming the
// still-failing node(s) — this must never silently succeed on a partial
// clear, so unlike replica writes there is no swallowing here.
func (c *Client) clearFanout(op func(conn *connection) error) error {
	c.mu.Lock()
	single := c.ring == nil
	c.mu.Unlock()
	if single {
		return c.applyReconnecting("", op)
	}

	if failed := c.clearRound(op); len(failed) > 0 {
		c.maybeRefresh(true)
		if failed := c.clearRound(op); len(failed) > 0 {
			sort.Strings(failed)
			return connectionLost(
				fmt.Sprintf("clear failed on node(s): %s", strings.Join(failed, ", ")), nil)
		}
	}
	return nil
}

// clearRound sends op to every currently known member concurrently (each
// leg gets applyReconnecting's own one-shot redial-and-retry, exactly
// like a replica write leg), returning the names of the ones that still
// failed after that — nil on full success.
func (c *Client) clearRound(op func(conn *connection) error) []string {
	c.mu.Lock()
	names := make([]string, 0, len(c.members))
	for name := range c.members {
		names = append(names, name)
	}
	c.mu.Unlock()

	var mu sync.Mutex
	var failed []string
	var wg sync.WaitGroup
	wg.Add(len(names))
	for _, name := range names {
		go func(name string) {
			defer wg.Done()
			if err := c.applyReconnecting(name, op); err != nil {
				mu.Lock()
				failed = append(failed, name)
				mu.Unlock()
			}
		}(name)
	}
	wg.Wait()
	return failed
}

// ── namespaces (issue #105) ──────────────────────────────────────────

// Namespace is a lightweight handle scoping Get/GetBytes/Set/SetBytes/
// Delete/Clear to one namespace: the same key name in two different
// namespaces — or in a namespace versus the default, unnamespaced
// keyspace — names two independent cache entries (docs/protocol.html's
// "g / s / d — namespaced get, set, delete" and "c / F — clear a
// namespace, flush everything"). Obtained from Client.Namespace.
//
// A Namespace does no networking of its own and holds no connections: it
// is cheap to create, shares the Client's connections, and every method
// simply forwards to the Client's own internal (namespace, key) entry
// points (getBytesNS/setBytesNS/deleteNS/clearNS) — routing (HRW over
// (ns,key), see hashring.go), replication fan-out, hedged reads, W
// refresh-and-retry, response tags, and value compression all apply
// exactly as they do to the Client's own namespace-less methods. It
// becomes invalid — every method returns ErrClosed — the moment the
// underlying Client is closed; a Namespace has no Close of its own to
// call. Safe for concurrent use, like Client itself.
type Namespace struct {
	client    *Client
	namespace []byte
	name      string
}

// Namespace returns a handle scoping every operation to ns (issue #105).
// ns is UTF-8 encoded. ns == "" returns a handle equivalent to the
// Client itself — legacy G/S/D frames, the same key placement as before
// namespaces existed — so namespace("") is never rejected; it exists so
// callers that generically pick a namespace at runtime don't need a
// special case for "no namespace".
func (c *Client) Namespace(ns string) *Namespace {
	return &Namespace{client: c, namespace: []byte(ns), name: ns}
}

// Name returns the namespace this handle scopes operations to (empty
// for the default namespace) — surfaced for the framework adapters
// layered on top of namespaces (issues #107/#108), which need to know
// which namespace a handle they were given addresses.
func (n *Namespace) Name() string { return n.name }

// Get returns key's value, within this namespace, as a string; ok is
// false when the key is missing. See Client.Get.
func (n *Namespace) Get(key string) (value string, ok bool, err error) {
	raw, ok, err := n.GetBytes(key)
	if err != nil || !ok {
		return "", ok, err
	}
	return string(raw), true, nil
}

// GetBytes returns key's raw value within this namespace; ok is false
// when the key is missing. See Client.GetBytes.
func (n *Namespace) GetBytes(key string) (value []byte, ok bool, err error) {
	return n.client.getBytesNS(n.namespace, key)
}

// Set stores the string value under key within this namespace. See
// Client.Set.
func (n *Namespace) Set(key, value string, ttlSeconds int64) error {
	return n.SetBytes(key, []byte(value), ttlSeconds)
}

// SetBytes stores the raw value under key within this namespace. See
// Client.SetBytes.
func (n *Namespace) SetBytes(key string, value []byte, ttlSeconds int64) error {
	return n.client.setBytesNS(n.namespace, key, value, ttlSeconds)
}

// Delete removes key within this namespace, reporting whether it existed
// before this call. See Client.Delete.
func (n *Namespace) Delete(key string) (existed bool, err error) {
	return n.client.deleteNS(n.namespace, key)
}

// Clear drops every entry in this namespace across every node (issue
// #106) — see Client.ClearAll to flush every namespace at once, default
// included. namespace("")'s Clear clears the default namespace; it is
// never rejected.
func (n *Namespace) Clear() error {
	return n.client.clearNS(n.namespace)
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
	// Fire-and-forget replica writes: give background replica writes (if any) a chance
	// to finish before their connections are torn out from under them.
	// Bounded by maxInFlightBackgroundReplicaWrites, so this is a short
	// wait in practice.
	c.backgroundReplicaWG.Wait()
	// Hedged reads: same drain contract for hedging's losing legs (issue
	// #64) — left running to completion rather than cancelled (see
	// readHedged), so Close() waits for them too before tearing down
	// connections out from under them.
	c.hedgedReadsWG.Wait()
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

// ownerNames returns (namespace, key)'s owners in rank order (issue
// #105: HRW routing takes the namespace into account — see
// HashRing.OwnersNS). A nil/empty namespace routes exactly as before
// namespaces existed.
func (c *Client) ownerNames(namespace, key []byte) []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.ring == nil {
		return nil
	}
	return c.ring.OwnersNS(namespace, key, c.replication)
}

// applyReconnecting runs op against the slot's connection, retrying once
// on a connection-level failure: a socket only learns of a peer FIN (e.g.
// the server's 60s idle timeout) on I/O, so lazy reconnect-on-use means
// the failed request poisons the connection, the redial replaces it, and
// the operation runs again. Safe because Get/Set/Delete are idempotent.
// slot is "" in single mode.
//
// A malformed/unexpected response frame (ErrProtocol) poisons the
// connection exactly the same way a genuine I/O failure does (see
// connection.poison and readLoop) — only the error TYPE surfaced to a
// caller differs between the two, not the "this connection is dead,
// discard it" mechanics — so it gets the same retry-via-redial treatment
// here as ErrConnectionLost.
func (c *Client) applyReconnecting(slot string, op func(*connection) error) error {
	conn, err := c.slotConnection(slot)
	if err != nil {
		return err
	}
	if err := op(conn); err != nil {
		if !errors.Is(err, ErrConnectionLost) && !errors.Is(err, ErrProtocol) {
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

// read drives the owner walk and error policy; op returns its outcome
// directly (rather than writing into variables the caller captured) so
// that readHedged below can run several legs concurrently without racing
// on shared state.
func (c *Client) read(namespace, key []byte, op func(*connection) ([]byte, bool, error)) (value []byte, ok bool, err error) {
	c.mu.Lock()
	single := c.ring == nil
	c.mu.Unlock()
	if single {
		return c.readFromOwner("", op)
	}

	names := c.ownerNames(namespace, key)
	if c.readHedgeAfter > 0 && len(names) > 1 {
		return c.readHedged(names, op)
	}

	// Owners in rank order; fall through only on connection-level
	// failure — a replica hedges against a dead holder, not a miss.
	var lastError error
	for _, name := range names {
		v, o, err := c.readFromOwner(name, op)
		if err == nil {
			return v, o, nil
		}
		if errors.Is(err, ErrWrongNode) {
			return nil, false, err
		}
		lastError = err
	}
	if lastError == nil {
		lastError = connectionLost("no owner is reachable for this key", nil)
	}
	return nil, false, lastError
}

// readFromOwner runs op against one owner slot via applyReconnecting
// (reconnecting once on a connection-level failure), returning its
// outcome directly. Used both by read()'s sequential walk and by each
// concurrent leg of readHedged, so every caller gets its own private
// value/ok/err rather than writing into state shared across goroutines.
func (c *Client) readFromOwner(slot string, op func(*connection) ([]byte, bool, error)) (value []byte, ok bool, err error) {
	err = c.applyReconnecting(slot, func(conn *connection) error {
		var opErr error
		value, ok, opErr = op(conn)
		return opErr
	})
	return value, ok, err
}

// readHedged implements hedged reads (issue #64): one slow — not dead —
// owner otherwise bounds every read that touches it at its full RTT,
// since the sequential walk in read() only moves on to the next owner
// when the current one *fails*. Here the read starts at the primary
// (names[0]); if it hasn't answered within readHedgeAfter the same read
// is also sent to the next owner (and so on, one more owner per
// interval, until every owner is in flight). The first answer decides:
//
//   - a hit (ok==true) from any owner is final;
//   - the primary's answer (names[0]) is final even when it's a miss —
//     a replica's miss is merely provisional (it may simply lack the
//     copy) and does not finalize the read on its own;
//   - a connection-level failure (or any error other than ErrWrongNode)
//     hedges onward immediately (no wait for the interval) and is
//     remembered as the last error;
//   - ErrWrongNode propagates exactly as read()'s sequential path does.
//
// If every owner answers with a miss or fails, but at least one
// non-primary owner's answer was a clean miss, the read is accepted as a
// miss overall — mirroring the Python SDK exactly (there is no positive
// evidence the key exists on any owner that actually answered). Only when
// no owner ever produced even a provisional miss does the last error
// propagate.
//
// Losing legs are never cancelled — cancelling mid-write would poison a
// shared connection (see connection.request) — they run to completion
// detached, their outcome discarded, tracked on hedgedReadsWG so Close()
// waits for them exactly like a fire-and-forget replica write.
func (c *Client) readHedged(names []string, op func(*connection) ([]byte, bool, error)) (value []byte, ok bool, err error) {
	type legResult struct {
		index int
		value []byte
		ok    bool
		err   error
	}

	results := make(chan legResult, len(names))
	// start registers a leg on hedgedReadsWG and launches it, reporting
	// whether it actually started. c.closed is rechecked under c.mu
	// immediately before the Add — Close() flips closed under the same
	// lock before it ever calls hedgedReadsWG.Wait(), so this guarantees
	// every Add happens-before that Wait (mirrors the identical guard on
	// backgroundReplicaWG in write() and tryReadRepair()); without it, a
	// leg starting exactly as Close() observes the counter at zero could
	// race sync.WaitGroup's Add/Wait and panic. There is no synchronous
	// fallback here — a leg that loses this race is simply never started.
	start := func(index int) bool {
		c.mu.Lock()
		if c.closed {
			c.mu.Unlock()
			return false
		}
		c.hedgedReadsWG.Add(1)
		c.mu.Unlock()
		go func() {
			defer c.hedgedReadsWG.Done()
			v, o, e := c.readFromOwner(names[index], op)
			results <- legResult{index: index, value: v, ok: o, err: e}
		}()
		return true
	}

	if !start(0) {
		return nil, false, ErrClosed
	}
	pending := 1
	nextIndex := 1
	var lastError error
	replicaMissed := false

	// tryStartNext starts the next owner, if any remain and the client
	// isn't closing; a start refused because the client is closing is
	// treated as having run out of owners, same as reaching len(names).
	tryStartNext := func() {
		if nextIndex >= len(names) {
			return
		}
		if start(nextIndex) {
			pending++
			nextIndex++
			return
		}
		nextIndex = len(names)
	}

	for pending > 0 {
		var timer *time.Timer
		var timeout <-chan time.Time
		if nextIndex < len(names) {
			timer = time.NewTimer(c.readHedgeAfter)
			timeout = timer.C
		}

		select {
		case res := <-results:
			if timer != nil {
				timer.Stop()
			}
			pending--
			switch {
			case res.err != nil:
				if errors.Is(res.err, ErrWrongNode) {
					// Remaining legs, if any, are left running: already
					// registered on hedgedReadsWG, they finish and drain
					// via Close() like any other detached leg.
					return nil, false, res.err
				}
				lastError = res.err
			case res.ok || res.index == 0:
				return res.value, res.ok, nil
			default:
				// A non-primary clean miss: provisional only.
				replicaMissed = true
			}
			if pending == 0 {
				tryStartNext()
			}
		case <-timeout:
			// The hedge interval elapsed with no answer: one more owner,
			// without waiting for the legs already in flight.
			tryStartNext()
		}
	}

	if replicaMissed {
		return nil, false, nil
	}
	if lastError == nil {
		lastError = connectionLost("no owner is reachable for this key", nil)
	}
	return nil, false, lastError
}

// write runs op against every owner of the key; op's second argument
// reports whether this leg is the primary, whose outcome alone decides
// the operation's result — replica legs run on their own goroutines, so
// an op that captures outer variables must only write them when primary.
func (c *Client) write(namespace, key []byte, op func(conn *connection, primary bool) error) error {
	c.mu.Lock()
	single := c.ring == nil
	c.mu.Unlock()
	primaryOp := func(conn *connection) error { return op(conn, true) }
	if single {
		return c.applyReconnecting("", primaryOp)
	}

	names := c.ownerNames(namespace, key)
	if len(names) == 0 {
		return connectionLost("no owner is reachable for this key", nil)
	}

	// Fan out to the replicas concurrently with the primary write. The
	// primary's outcome decides; replica failures are swallowed by design
	// (client-side replication) — a dead or disagreeing replica leaves the key
	// under-replicated until the next node-list refresh, never fails the
	// write. Counted in Stats().ReplicaWriteFailures so operators can spot
	// silently degrading replication.
	replicaWrite := func(replica string) {
		if err := c.applyReconnecting(replica, func(conn *connection) error { return op(conn, false) }); err != nil {
			c.stats.replicaWriteFailures.Add(1)
		}
	}

	var replicas sync.WaitGroup
	for _, name := range names[1:] {
		// Fire-and-forget replica writes: with FireAndForgetReplicas, try to run this
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

	// Per-address reconnect cooldown (see Client.redialCooldowns' own doc
	// comment): an address whose dial just failed stays "down" for
	// reconnectCooldown, so a burst of requests routed to it — or one
	// request every keep-alive tick — fails immediately with the same
	// error the dial itself produced, instead of each paying another full
	// connectDeadline in turn.
	c.redialCooldownMu.Lock()
	cooldown, onCooldown := c.redialCooldowns[address]
	c.redialCooldownMu.Unlock()
	if onCooldown && time.Now().Before(cooldown.until) {
		return nil, cooldown.err
	}

	fresh, dialedAddress, err := c.dialSlot(slot, address)
	if err != nil {
		if c.reconnectCooldown > 0 {
			c.redialCooldownMu.Lock()
			c.redialCooldowns[address] = redialCooldown{until: time.Now().Add(c.reconnectCooldown), err: err}
			c.redialCooldownMu.Unlock()
		}
		return nil, err
	}
	c.redialCooldownMu.Lock()
	delete(c.redialCooldowns, address)
	c.redialCooldownMu.Unlock()

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
		// In proxy mode dialedAddress may differ from address — a
		// failover to another proxy (see dialSlot) — so this must be
		// re-recorded, not just address, or the next redial would target
		// the proxy that was just abandoned.
		c.singleAddress = dialedAddress
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

// dialSlot dials slot's current address, exactly as a plain redial always
// has — except in proxy mode's single slot (slot == ""), where a failed
// redial to the same proxy falls over to a freshly re-fetched, randomly
// chosen other one instead of simply propagating the dial error (issue
// #122's reconnect-on-loss: "first retry the same proxy ... if that
// fails, re-fetch Q from discovery and pick another"). Cluster-mode slots
// need no such fallback — the per-key owner walk in read()/write()
// already fails over to a different member when one is dead; proxy mode
// has no such second member to fall over to on its own, hence this.
// Returns the address actually dialed, which the caller installs as the
// slot's new address — in proxy mode's failover case that can differ
// from address.
func (c *Client) dialSlot(slot, address string) (conn *connection, dialedAddress string, err error) {
	conn, err = c.openNodeConnection(address)
	if err == nil {
		return conn, address, nil
	}
	if slot != "" || !c.viaProxy {
		return nil, "", err
	}
	return c.reconnectAnotherProxy(address)
}

// reconnectAnotherProxy re-fetches the proxy roster from discovery and
// connects to one at random (dialRandomProxy), preferring a proxy other
// than deadAddress — the one that just failed to redial — when the fresh
// roster offers a choice; deadAddress may simply have been dropped from
// discovery's roster too, in which case every candidate is already
// "other". Falls back to deadAddress's own dial error when discovery
// can't be reached (or reports no proxies) either.
func (c *Client) reconnectAnotherProxy(deadAddress string) (*connection, string, error) {
	proxies, ok := c.fetchProxyList()
	if !ok || len(proxies) == 0 {
		return nil, "", connectionLost("proxy "+deadAddress+" is unreachable", nil)
	}

	candidates := proxies
	if len(proxies) > 1 {
		other := make([]discoveredNode, 0, len(proxies)-1)
		for _, p := range proxies {
			if p.Address != deadAddress {
				other = append(other, p)
			}
		}
		if len(other) > 0 {
			candidates = other
		}
	}
	return c.dialRandomProxy(candidates)
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
	return c.trackedConnection(result.conn, result.tagged), nil
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
	if !ok {
		// Every configured address was unreachable, still warming up, no
		// longer a discovery server, or knows no live nodes: keep the
		// last-known list rather than erroring Get/Set/Delete over what
		// may be a transient hiccup. Silent by design — counted in
		// Stats().RefreshFailures instead of a log line on every stale
		// check.
		c.stats.refreshFailures.Add(1)
		return
	}
	if c.ring == nil {
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

	// Same rationale for the per-address reconnect cooldowns: an address
	// whose owning node has left the cluster would otherwise leave its
	// cooldown entry behind forever in a churny deployment where nodes get
	// a fresh IP:port on every restart (issue #96).
	liveAddresses := make(map[string]struct{}, len(nodes))
	for _, node := range nodes {
		liveAddresses[node.Address] = struct{}{}
	}
	c.redialCooldownMu.Lock()
	for address := range c.redialCooldowns {
		if _, live := liveAddresses[address]; !live {
			delete(c.redialCooldowns, address)
		}
	}
	c.redialCooldownMu.Unlock()
}

// fetchNodeList walks every configured address (discovery HA); ok=false
// means keep the last-known list.
func (c *Client) fetchNodeList() ([]discoveredNode, int, bool) {
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
