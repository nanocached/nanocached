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
	"math"
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

// maxRequestBytes bounds namespace+key+value size before any network
// I/O. The server's own request cap (src/server.rs's MAX_REQUEST_SIZE)
// is 1 MiB for the *entire* frame — header line plus namespace plus key
// plus value; a request over that limit is rejected by simply closing
// the connection without a response, which poisons whatever else is
// pipelined behind it on that same connection. This reserves 256 bytes
// of headroom for the header itself (marker byte, decimal lengths, an
// optional TTL, echoed response tags's tag field, spaces, the trailing
// newline — always comfortably under this even for the largest fields),
// so a namespace+key+value that clears maxRequestBytes is guaranteed to
// fit under the server's own cap (issue #47 audit item G1; issue #105
// namespaces; issue #228 folded namespace length into this bound
// alongside key and value, matching client.py's _check_key/
// _check_key_and_value, client.rs's validate_key/validate_key_and_value,
// and NanocachedClient.java's validateKey/validateKeyAndValue). Also the
// per-sub-frame byte budget multiGetChunked/multiSetChunked enforce
// (issue #222): validateKey/validateKeyAndValue only bound one
// namespace+key(+value) at a time, never the sum across a whole `m`/`o`
// batch, so a batch of individually valid pairs could otherwise add up
// to a multi-megabyte sub-frame that the server can only reject by
// closing the connection outright (see maxBatchKeys).
const maxRequestBytes = 1024*1024 - 256

// maxBatchKeys bounds how many keys GetMany/GetManyBytes/SetMany/
// SetManyBytes send in one `m`/`o` sub-frame before splitting into more
// than one (batch chunking, issues #128/#150/#151) — purely a
// client-side concern, invisible to callers: a call with more keys than
// this simply becomes more than one sub-frame per owner, transparently
// reassembled. Derived from connection.go's maxHeaderLineLength (4 KiB):
// an `M` response's roster carries one token per key, at worst a decimal
// byte length up to maxValueLength's own digit count plus its
// separating space (len("2097152")+1 = 8 bytes). 400*8 = 3200 bytes
// leaves comfortable headroom under 4 KiB even with a trailing tag
// field, well before this constant would ever need revisiting. A chunk
// can still end before reaching this key count: multiGetChunked/
// multiSetChunked also cut a sub-frame short as soon as the next entry
// would push its cumulative wire size past maxRequestBytes (issue #222)
// — large keys/values hit that bound long before 400 keys would.
const maxBatchKeys = 400

// validateKey rejects an empty key, or a namespace+key pair that alone
// already exceeds maxRequestBytes, before any network I/O: the server
// has no way to answer either shape except by closing the connection
// outright, silently poisoning every other request already pipelined on
// that connection. The size bound matters here specifically because
// getRawNS/deleteNS/incrNS/deleteIfMatchesNS and getManyNS's per-key loop
// call validateKey directly (they have no value to combine it with,
// unlike validateKeyAndValue) — without it, an oversized namespace+key on
// GET/DELETE/INCR/DECR/DeleteIfMatches/GetMany would sail past
// client-side validation and only be caught by the server slamming the
// connection shut (issue #47 audit item G1 follow-up; issue #228 folded
// the namespace in, matching client.py's _check_key, client.rs's
// validate_key, and NanocachedClient.java's validateKey, which all fold
// namespace into the key-only validator rather than leaving it to the
// key+value check alone). A nil/empty namespace costs nothing here,
// keeping this byte-identical to the pre-namespace check for
// namespace-less callers. Matches the style of the ttlSeconds < 0 check
// in SetBytes.
func validateKey(namespace []byte, key string) error {
	if len(key) == 0 {
		return invalidArgument("nanocached: key must not be empty")
	}
	total := len(namespace) + len(key)
	if total > maxRequestBytes {
		if len(namespace) == 0 {
			// Keeps the pre-namespace message unchanged for the common,
			// namespace-less case.
			return invalidArgument(fmt.Sprintf(
				"nanocached: key exceeds maxRequestBytes (%d bytes), got %d bytes",
				maxRequestBytes, len(key)))
		}
		return invalidArgument(fmt.Sprintf(
			"nanocached: namespace (%d bytes) + key (%d bytes) exceeds maxRequestBytes (%d bytes)",
			len(namespace), len(key), maxRequestBytes))
	}
	return nil
}

// validateKeyAndValue is validateKey plus a maxRequestBytes bound on
// len(namespace)+len(key)+valueLen — anything past it can never fit the
// server's own request cap, so failing fast here is strictly better than
// sending a frame the server can only reject by silently closing the
// connection.
func validateKeyAndValue(namespace []byte, key string, valueLen int) error {
	if err := validateKey(namespace, key); err != nil {
		return err
	}
	total := len(namespace) + len(key) + valueLen
	if total > maxRequestBytes {
		if len(namespace) == 0 {
			return invalidArgument(fmt.Sprintf(
				"nanocached: key (%d bytes) + value (%d bytes) exceeds the %d-byte request limit",
				len(key), valueLen, maxRequestBytes))
		}
		return invalidArgument(fmt.Sprintf(
			"nanocached: namespace (%d bytes) + key (%d bytes) + value (%d bytes) exceeds the %d-byte request limit",
			len(namespace), len(key), valueLen, maxRequestBytes))
	}
	return nil
}

// validateNamespaceForClear is clearNS's own bound (issue #106): a clear
// frame carries no key or value, only the namespace, so unlike
// validateKey/validateKeyAndValue there is nothing to sum it against —
// the namespace alone just needs to fit under the server's own request
// cap, same rationale as those two (issue #47 audit item G1 follow-up;
// issue #228; matches client.py's _check_namespace, client.rs's
// validate_namespace_for_clear, and NanocachedClient.java's
// validateNamespace). A nil/empty namespace always passes, matching
// clearNS's own "clears the default namespace, never rejected" rule.
func validateNamespaceForClear(namespace []byte) error {
	if len(namespace) > maxRequestBytes {
		return invalidArgument(fmt.Sprintf(
			"nanocached: namespace exceeds maxRequestBytes (%d bytes), got %d bytes",
			maxRequestBytes, len(namespace)))
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
	// keepaliveWG lets Close() wait for the keepalive goroutine itself to
	// exit (issue #192) — otherwise it could still be mid-ping against a
	// connection Close()'s teardown() is about to close out from under
	// it, racing the very thing every other background-work WaitGroup
	// here (backgroundReplicaWG, hedgedReadsWG) exists to prevent.
	keepaliveWG sync.WaitGroup

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
	raw, ok, err := c.getRawNS(namespace, key)
	if err != nil || !ok || !c.compress {
		return raw, ok, err
	}
	value, err = decompressValue(raw)
	return value, ok, err
}

// getRawNS fetches (namespace, key)'s raw wire bytes — exactly what
// getBytesNS itself used to do before this helper existed, still applying
// the same cluster retry and read repair policy, but stopping short of
// this SDK's own client-side decompression. Shared by getBytesNS and
// getBytesWithTokenNS/GetWithToken (issue #141: compare-and-set), whose
// CasToken must be computed from these exact bytes — the ones the server
// itself hashes, since the server never decompresses (value compression)
// — never from the decompressed value getBytesNS goes on to return.
func (c *Client) getRawNS(namespace []byte, key string) (raw []byte, ok bool, err error) {
	if err := validateKey(namespace, key); err != nil {
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
		raw, ok = v, o
		return readErr
	})
	if err == nil && !ok && c.readRepair {
		raw, ok = c.tryReadRepair(namespace, keyBytes)
	}
	return raw, ok, err
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
	if err := validateKeyAndValue(namespace, key, len(value)); err != nil {
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
	if err := validateKey(namespace, key); err != nil {
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

// ── バッチ操作 (issues #128/#150/#151) ───────────────────────────────

// GetMany returns every requested key's value as a string in one round
// trip per owner (batched get) instead of one round trip per key — see
// GetManyBytes for the raw-bytes form this wraps, including its
// wrong-node and partial-result contract, which applies here unchanged.
func (c *Client) GetMany(keys []string) (map[string]string, error) {
	raw, err := c.GetManyBytes(keys)
	if raw == nil {
		return nil, err
	}
	values := make(map[string]string, len(raw))
	for key, value := range raw {
		values[key] = string(value)
	}
	return values, err
}

// GetManyBytes returns every requested key's raw value in one round
// trip per owner (batched get, docs/protocol.html#multi) — a missing
// key is simply absent from the returned map, never an error, the same
// "a miss is not an error" contract GetBytes itself has. keys must be
// non-empty.
//
// A batch never fails as a whole: if some keys are still wrong-node
// after one bounded refresh-and-retry (the same policy GetBytes' own
// withClusterRetry applies, generalized to a per-key roster instead of
// an all-or-nothing retry — see multiGetPass), the returned map holds
// every key that DID resolve, paired with ErrWrongNode, rather than
// discarding a mostly-successful batch over a handful of stale
// placements. In single-node/proxy mode a `W` propagates immediately,
// exactly as GetBytes' own single-mode behavior does — there is no ring
// to refresh against.
//
// Larger batches are transparently split into more than one `m`
// sub-frame per owner (batch chunking, see maxBatchKeys) — callers
// never need to think about this.
func (c *Client) GetManyBytes(keys []string) (map[string][]byte, error) {
	return c.getManyNS(nil, keys)
}

// getManyNS is GetManyBytes scoped to namespace — the internal entry
// point a *Namespace handle's GetMany/GetManyBytes forward to, mirroring
// getBytesNS.
func (c *Client) getManyNS(namespace []byte, keys []string) (map[string][]byte, error) {
	if len(keys) == 0 {
		return nil, invalidArgument("nanocached: GetMany/GetManyBytes requires at least one key")
	}
	keyBytes := make([][]byte, len(keys))
	for i, key := range keys {
		if err := validateKey(namespace, key); err != nil {
			return nil, err
		}
		keyBytes[i] = []byte(key)
	}
	if err := c.beforeOperation(); err != nil {
		return nil, err
	}

	c.mu.Lock()
	single := c.ring == nil
	c.mu.Unlock()

	values := make(map[string][]byte, len(keys))

	if single {
		entries, chunkErr := c.multiGetChunked("", namespace, keyBytes)
		for i, entry := range entries {
			if !entry.ok {
				continue
			}
			value, decErr := c.maybeDecompress(entry.value)
			if decErr != nil {
				return values, decErr
			}
			values[keys[i]] = value
		}
		if chunkErr != nil {
			return values, chunkErr
		}
		if multiAnyWrongNode(entries) {
			return values, ErrWrongNode
		}
		return values, nil
	}

	retry, err := c.multiGetPass(namespace, keys, keyBytes, values, nil)
	if err != nil {
		return values, err
	}
	if len(retry) == 0 {
		return values, nil
	}
	c.maybeRefresh(true)
	retry, err = c.multiGetPass(namespace, keys, keyBytes, values, retry)
	if err != nil {
		return values, err
	}
	if len(retry) > 0 {
		return values, ErrWrongNode
	}
	return values, nil
}

// multiFrameHeaderSlack conservatively bounds the `m`/`o` header bytes
// that aren't already counted per entry by multiGetEntryCost/
// multiSetEntryCost below: the marker+space, decimal namespace-length
// and entry-count fields (at most 3 digits each — maxBatchKeys caps the
// count at 400), an optional TTL (up to 11 bytes: a space plus a
// negative int64's worst case), the optional tag field (a space plus up
// to 10 digits for a uint32), and the trailing newline. 64 bytes leaves
// comfortable headroom over that worst case (issue #222).
const multiFrameHeaderSlack = 64

// decimalDigits returns how many bytes strconv.AppendInt would write
// for a non-negative n, without actually building the string — used to
// size a chunk's header honestly (batch chunking's cumulative-bytes
// bound, issue #222) to match appendMultiGetFrame/appendMultiSetFrame's
// own decimal length fields.
func decimalDigits(n int) int {
	digits := 1
	for n >= 10 {
		n /= 10
		digits++
	}
	return digits
}

// multiGetEntryCost is the wire bytes one more key adds to an `m` frame
// per appendMultiGetFrame: a separating space, that key's decimal
// length field, and the key bytes themselves.
func multiGetEntryCost(key []byte) int {
	return 1 + decimalDigits(len(key)) + len(key)
}

// multiSetEntryCost is multiGetEntryCost's write-side twin, matching
// appendMultiSetFrame: two separating spaces, the key's and value's
// decimal length fields, and the key and value bytes themselves.
func multiSetEntryCost(key, value []byte) int {
	return 2 + decimalDigits(len(key)) + decimalDigits(len(value)) + len(key) + len(value)
}

// multiGetChunked issues one or more `m` sub-frames against slot for
// keys — already grouped to one owner (or the single/proxy target) by
// the caller — splitting into sub-frames bounded by both maxBatchKeys
// and maxRequestBytes (batch chunking, issue #222): a chunk stops
// growing as soon as either the next key would push its count past
// maxBatchKeys or its cumulative wire size (namespace, once, plus each
// included key's multiGetEntryCost, plus multiFrameHeaderSlack) past
// maxRequestBytes, so no reply header risks exceeding
// maxHeaderLineLength and no request frame risks the server's own
// MAX_REQUEST_SIZE. A chunk's first key is always included regardless
// of the byte budget — validateKey already guarantees any single
// namespace+key fits under maxRequestBytes on its own. Always returns
// len(keys) entries: a chunk that fails outright (a connection-level
// failure, not a per-key `W`) leaves its keys' entries at the zero
// value (a clean miss) and that chunk's error is returned alongside —
// the caller treats that gap as "still needs a retry", exactly like a
// per-key WrongNode.
func (c *Client) multiGetChunked(slot string, namespace []byte, keys [][]byte) ([]multiEntry, error) {
	entries := make([]multiEntry, len(keys))
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
		err := c.applyReconnecting(slot, func(conn *connection) error {
			var opErr error
			chunkEntries, opErr = conn.multiGetNS(namespace, keys[start:end])
			return opErr
		})
		if err != nil {
			return entries, err
		}
		copy(entries[start:end], chunkEntries)
		start = end
	}
	return entries, nil
}

// multiGetPass runs one pass of GetManyBytes' cluster routing: group
// the given indices (every key, when retryIndices is nil — the initial
// pass — or just the keys a previous pass left unresolved) by their
// current primary owner (matching plain Get's own primary-first
// stance), dispatch one (possibly chunked) `m` exchange per owner
// concurrently, splice hits into values, and return the indices still
// unresolved: a per-key `W`, or a whole owner group whose call failed
// outright (indistinguishable from a possibly-idle-closed connection,
// same stance applyReconnecting's own callers take elsewhere). Called
// once for the initial pass and once more, if needed, after a single
// force refresh — see getManyNS. The returned error is a client-side
// decompression failure only (Config.Compress mismatch) — never a
// routing outcome, so it aborts the batch immediately rather than
// feeding into the retry pass.
func (c *Client) multiGetPass(
	namespace []byte, keys []string, keyBytes [][]byte,
	values map[string][]byte, retryIndices []int,
) ([]int, error) {
	indices := retryIndices
	if indices == nil {
		indices = make([]int, len(keys))
		for i := range keys {
			indices[i] = i
		}
	}

	groups := make(map[string][]int)
	var retry []int
	for _, idx := range indices {
		owners := c.ownerNames(namespace, keyBytes[idx])
		if len(owners) == 0 {
			retry = append(retry, idx)
			continue
		}
		groups[owners[0]] = append(groups[owners[0]], idx)
	}

	var mu sync.Mutex
	var spliceErr error
	var wg sync.WaitGroup
	wg.Add(len(groups))
	for owner, groupIndices := range groups {
		go func(owner string, groupIndices []int) {
			defer wg.Done()
			groupKeys := make([][]byte, len(groupIndices))
			for i, idx := range groupIndices {
				groupKeys[i] = keyBytes[idx]
			}
			entries, err := c.multiGetChunked(owner, namespace, groupKeys)

			mu.Lock()
			defer mu.Unlock()
			if err != nil {
				retry = append(retry, groupIndices...)
				return
			}
			for i, idx := range groupIndices {
				entry := entries[i]
				switch {
				case entry.wrongNode:
					retry = append(retry, idx)
				case entry.ok:
					value, decErr := c.maybeDecompress(entry.value)
					if decErr != nil {
						if spliceErr == nil {
							spliceErr = decErr
						}
						continue
					}
					values[keys[idx]] = value
				}
			}
		}(owner, groupIndices)
	}
	wg.Wait()
	return retry, spliceErr
}

// maybeDecompress is GetBytes' own decompression step (see getBytesNS),
// generalized so GetManyBytes' per-entry splicing can share it: a no-op
// when Config.Compress is off.
func (c *Client) maybeDecompress(value []byte) ([]byte, error) {
	if !c.compress {
		return value, nil
	}
	return decompressValue(value)
}

// multiAnyWrongNode reports whether any entry carries a per-key `W` —
// shared by GetManyBytes' and SetManyBytes' single-mode paths, which
// (like GetBytes/SetBytes) have no ring to refresh against, so a
// wrong-node answer propagates immediately rather than feeding a retry
// pass.
func multiAnyWrongNode(entries []multiEntry) bool {
	for _, entry := range entries {
		if entry.wrongNode {
			return true
		}
	}
	return false
}

// SetMany stores every value in values in one round trip per involved
// node (batched set) instead of one round trip per key — see
// SetManyBytes for the raw-bytes form this wraps, including its
// wrong-node and replication contract, which applies here unchanged.
// ttlSeconds is shared by the whole batch, not per key (one real caller
// of a batched set — Django's set_many, cache-manager's mset — already
// passes one TTL per call).
func (c *Client) SetMany(values map[string]string, ttlSeconds int64) error {
	raw := make(map[string][]byte, len(values))
	for key, value := range values {
		raw[key] = []byte(value)
	}
	return c.SetManyBytes(raw, ttlSeconds)
}

// SetManyBytes stores every raw value in values in one round trip per
// involved node (batched set, docs/protocol.html#multi). ttlSeconds is
// a whole number of seconds shared by the whole batch; 0 means no
// expiry, negative is rejected. values must be non-empty. Transparently
// compresses values at or above Config.CompressionThreshold when
// Config.Compress is enabled, exactly like SetBytes.
//
// Within one batch, the same node can be a key's primary and another
// key's replica at once — it receives exactly one `o` sub-frame either
// way, and only its answer for the keys it is primary for decides that
// key's outcome; a replica-held key's failure or `W` is
// logged-and-swallowed into Stats().ReplicaWriteFailures, exactly like
// SetBytes' own replica legs (write/fanReplicas). A batch never fails
// as a whole: if some keys' primaries are still wrong-node after one
// bounded refresh-and-retry, SetManyBytes returns ErrWrongNode — every
// other key in the batch was still stored. In single-node/proxy mode a
// `W` propagates immediately, exactly as SetBytes' own single-mode
// behavior does.
//
// Larger batches are transparently split into more than one `o`
// sub-frame per node (batch chunking, see maxBatchKeys).
func (c *Client) SetManyBytes(values map[string][]byte, ttlSeconds int64) error {
	return c.setManyNS(nil, values, ttlSeconds)
}

// setManyNS is SetManyBytes scoped to namespace — the internal entry
// point a *Namespace handle's SetMany/SetManyBytes forward to, mirroring
// setBytesNS.
func (c *Client) setManyNS(namespace []byte, values map[string][]byte, ttlSeconds int64) error {
	if len(values) == 0 {
		return invalidArgument("nanocached: SetMany/SetManyBytes requires at least one key")
	}
	if ttlSeconds < 0 {
		return invalidArgument(fmt.Sprintf("nanocached: ttlSeconds must not be negative, got %d", ttlSeconds))
	}

	keys := make([]string, 0, len(values))
	keyBytes := make([][]byte, 0, len(values))
	valueBytes := make([][]byte, 0, len(values))
	for key, value := range values {
		if err := validateKeyAndValue(namespace, key, len(value)); err != nil {
			return err
		}
		keys = append(keys, key)
		keyBytes = append(keyBytes, []byte(key))
		outgoing := value
		if c.compress {
			outgoing = compressValue(value, c.compressionThreshold)
		}
		valueBytes = append(valueBytes, outgoing)
	}
	if err := c.beforeOperation(); err != nil {
		return err
	}

	wireTTL := int64(-1) // no expiry
	if ttlSeconds > 0 {
		wireTTL = ttlSeconds
	}

	c.mu.Lock()
	single := c.ring == nil
	c.mu.Unlock()

	if single {
		entries, err := c.multiSetChunked("", namespace, keyBytes, valueBytes, wireTTL)
		if err != nil {
			return err
		}
		if multiAnyWrongNode(entries) {
			return ErrWrongNode
		}
		return nil
	}

	retry := c.multiSetPass(namespace, keys, keyBytes, valueBytes, wireTTL, nil)
	if len(retry) == 0 {
		return nil
	}
	c.maybeRefresh(true)
	retry = c.multiSetPass(namespace, keys, keyBytes, valueBytes, wireTTL, retry)
	if len(retry) > 0 {
		return ErrWrongNode
	}
	return nil
}

// multiSetChunked is multiGetChunked's write-side twin: one or more `o`
// sub-frames against slot for keys/values (already grouped to one
// owner, or the single/proxy target), split into sub-frames bounded by
// both maxBatchKeys and maxRequestBytes the same way (batch chunking,
// issue #222), using multiSetEntryCost in place of multiGetEntryCost so
// the cumulative byte tracking also counts each value. Always returns
// len(keys) entries; a chunk that fails outright leaves its keys'
// entries at the zero value and that chunk's error is returned
// alongside.
func (c *Client) multiSetChunked(slot string, namespace []byte, keys, values [][]byte, ttlSeconds int64) ([]multiEntry, error) {
	entries := make([]multiEntry, len(keys))
	budget := maxRequestBytes - len(namespace) - multiFrameHeaderSlack
	for start := 0; start < len(keys); {
		end := start
		total := 0
		for end < len(keys) && end-start < maxBatchKeys {
			cost := multiSetEntryCost(keys[end], values[end])
			if end > start && total+cost > budget {
				break
			}
			total += cost
			end++
		}
		var chunkEntries []multiEntry
		err := c.applyReconnecting(slot, func(conn *connection) error {
			var opErr error
			chunkEntries, opErr = conn.multiSetNS(namespace, keys[start:end], values[start:end], ttlSeconds)
			return opErr
		})
		if err != nil {
			return entries, err
		}
		copy(entries[start:end], chunkEntries)
		start = end
	}
	return entries, nil
}

// multiSetPass runs one pass of SetManyBytes' cluster routing: for
// every key still needing resolution (every key, when retryIndices is
// nil, or just what a previous pass left unresolved), build one
// sub-batch per **owner name across every rank** — not just primaries,
// unlike multiGetPass — because within one batch the same node can be
// primary for one key and a replica for another (see SetManyBytes' own
// doc comment); each owner therefore gets exactly one `o` sub-frame
// covering every key it holds in any role. Only a leg's *primary* keys
// can end up in the returned retry list; a leg's replica-held keys are
// logged-and-swallowed into Stats().ReplicaWriteFailures instead,
// mirroring fanReplicas' stance for single-key Set. A leg that is a
// pure replica for every key it holds is eligible for
// Config.FireAndForgetReplicas, exactly like a single-key replica
// write — see runMultiSetLeg.
func (c *Client) multiSetPass(
	namespace []byte, keys []string, keyBytes, valueBytes [][]byte, ttlSeconds int64,
	retryIndices []int,
) []int {
	indices := retryIndices
	if indices == nil {
		indices = make([]int, len(keys))
		for i := range keys {
			indices[i] = i
		}
	}

	type ownerBatch struct {
		indices   []int
		isPrimary []bool
	}
	owners := make(map[string]*ownerBatch)
	var retry []int
	for _, idx := range indices {
		names := c.ownerNames(namespace, keyBytes[idx])
		if len(names) == 0 {
			retry = append(retry, idx)
			continue
		}
		for rank, name := range names {
			batch := owners[name]
			if batch == nil {
				batch = &ownerBatch{}
				owners[name] = batch
			}
			batch.indices = append(batch.indices, idx)
			batch.isPrimary = append(batch.isPrimary, rank == 0)
		}
	}

	var mu sync.Mutex
	var wg sync.WaitGroup
	for name, batch := range owners {
		pureReplica := true
		for _, isPrimary := range batch.isPrimary {
			if isPrimary {
				pureReplica = false
				break
			}
		}

		if c.fireAndForgetReplicas && pureReplica {
			select {
			case c.backgroundReplicaSem <- struct{}{}:
				// Same Add-under-c.mu-with-a-closed-recheck ordering
				// fanReplicas itself uses, guaranteeing every Add
				// happens-before Close()'s Wait.
				c.mu.Lock()
				if !c.closed {
					c.backgroundReplicaWG.Add(1)
					c.mu.Unlock()
					go func(name string, batch *ownerBatch) {
						defer c.backgroundReplicaWG.Done()
						defer func() { <-c.backgroundReplicaSem }()
						c.runMultiSetLeg(namespace, name, batch.indices, batch.isPrimary,
							keyBytes, valueBytes, ttlSeconds, &mu, nil)
					}(name, batch)
					continue
				}
				c.mu.Unlock()
				<-c.backgroundReplicaSem
			default:
			}
		}

		wg.Add(1)
		go func(name string, batch *ownerBatch) {
			defer wg.Done()
			c.runMultiSetLeg(namespace, name, batch.indices, batch.isPrimary,
				keyBytes, valueBytes, ttlSeconds, &mu, &retry)
		}(name, batch)
	}
	wg.Wait()
	return retry
}

// runMultiSetLeg dispatches one owner's `o` sub-batch and applies its
// result under mu: only primary-held keys can end up appended to
// *retry (retry is nil for a detached fire-and-forget replica leg,
// which by construction — see multiSetPass's pureReplica check — holds
// no primary key at all, so there is nothing for it to retry). Every
// replica-held key's failure or `W` is counted in
// Stats().ReplicaWriteFailures instead of affecting the caller's
// result, mirroring fanReplicas' own stance for single-key Set. A
// connection-level failure for the whole leg is treated the same way,
// key by key, since the SAME sub-frame can carry both primary- and
// replica-held keys and a transport failure doesn't distinguish between
// them.
func (c *Client) runMultiSetLeg(
	namespace []byte, name string, indices []int, isPrimary []bool,
	keyBytes, valueBytes [][]byte, ttlSeconds int64,
	mu *sync.Mutex, retry *[]int,
) {
	groupKeys := make([][]byte, len(indices))
	groupValues := make([][]byte, len(indices))
	for i, idx := range indices {
		groupKeys[i] = keyBytes[idx]
		groupValues[i] = valueBytes[idx]
	}
	entries, err := c.multiSetChunked(name, namespace, groupKeys, groupValues, ttlSeconds)

	mu.Lock()
	defer mu.Unlock()
	if err != nil {
		for i, idx := range indices {
			if isPrimary[i] {
				if retry != nil {
					*retry = append(*retry, idx)
				}
			} else {
				c.stats.replicaWriteFailures.Add(1)
			}
		}
		return
	}
	for i, idx := range indices {
		if !isPrimary[i] {
			if entries[i].wrongNode {
				c.stats.replicaWriteFailures.Add(1)
			}
			continue
		}
		if entries[i].wrongNode && retry != nil {
			*retry = append(*retry, idx)
		}
	}
}

// Incr atomically adds delta to key's stored counter value and returns
// the result; ok is false when the key is missing or expired (the same
// "not found" shape GetBytes returns). delta may be negative — Decr is
// just Incr with delta negated, there is no separate wire opcode. Returns
// ErrNotNumeric (matched with errors.Is) when the stored value isn't a
// signed decimal int64, or applying delta would overflow one.
//
// INCR is exactly as volatile as Set: LRU eviction and TTL expiry reclaim
// an incremented value like any other entry, so this is a fit for rate
// limiting or approximate counters, not durable counts (billing,
// inventory).
//
// In a cluster, only the key's primary owner actually runs the increment;
// replicas receive the primary's literal resulting value (as an ordinary
// Set) rather than replaying the increment themselves — see incr's own
// doc comment for why.
//
// Incr/Decr are at-least-once, not exactly-once (issue #225): unlike
// Get/Set/Delete, INCR is not idempotent, so it is never resent once its
// request may have reached the primary — a connection failure at that
// point returns ErrConnectionLost instead of silently retrying, since
// resending could double-apply delta. The request is only retried
// (transparently, via a redial) when it provably never left this
// process — e.g. a connection that went idle and was closed by the
// server before this call reused it. A caller that gets
// ErrConnectionLost from Incr/Decr cannot tell whether the increment was
// applied; the caller must decide whether to retry (accepting a possible
// double-apply) or treat the outcome as unknown.
func (c *Client) Incr(key string, delta int64) (value int64, ok bool, err error) {
	return c.incrNS(nil, key, delta)
}

// Decr is Incr with delta negated — a thin convenience wrapper; it sends
// exactly the same `i` wire opcode as Incr, never a separate one. Returns
// ErrInvalidArgument (issue #182) for delta == math.MinInt64, which has
// no valid int64 negation — see negateDecrDelta.
func (c *Client) Decr(key string, delta int64) (value int64, ok bool, err error) {
	negated, err := negateDecrDelta(delta)
	if err != nil {
		return 0, false, err
	}
	return c.incrNS(nil, key, negated)
}

// negateDecrDelta negates delta for Decr (shared by *Client and
// *Namespace), rejecting math.MinInt64 (issue #182): two's complement has
// no positive int64 large enough to represent |math.MinInt64|
// (math.MaxInt64 is one short), so negating it silently wraps back to
// math.MinInt64 itself — turning a decrement into the largest possible
// increment instead of failing loudly. Caught here, before any I/O,
// mirroring the Java and Rust SDKs' own rejection of this value.
func negateDecrDelta(delta int64) (int64, error) {
	if delta == math.MinInt64 {
		return 0, invalidArgument("nanocached: decr delta must not be math.MinInt64, which has no valid int64 negation")
	}
	return -delta, nil
}

// incrNS is Incr scoped to namespace — the internal (namespace, key)
// entry point a *Namespace handle's Incr/Decr forward to, mirroring
// getBytesNS/setBytesNS/deleteNS.
func (c *Client) incrNS(namespace []byte, key string, delta int64) (value int64, ok bool, err error) {
	if err := validateKey(namespace, key); err != nil {
		return 0, false, err
	}
	if err := c.beforeOperation(); err != nil {
		return 0, false, err
	}
	keyBytes := []byte(key)
	err = c.withClusterRetryNonIdempotent(func() error {
		v, o, incrErr := c.incr(namespace, keyBytes, delta)
		value, ok = v, o
		return incrErr
	})
	return value, ok, err
}

// incr drives Incr/Decr's routing (issue #129) — deliberately NOT
// write()'s "run the same op against every owner" pattern. INCR runs
// against the primary owner ONLY; only once that succeeds is the
// primary's literal resulting value (+ TTL) fanned out to the remaining
// owners as an ordinary Set (fanReplicas — the same best-effort,
// fire-and-forget-aware machinery write() itself uses for its replica
// legs). Replaying `i` on a replica instead would let it drift from the
// primary — e.g. an earlier replica-leg write silently dropped (client-side
// replication swallows replica failures by design), or the replica
// separately evicting and resetting the key — whereas forwarding the
// absolute result keeps every replica byte-identical to the primary
// (server.rs's own migration/decommission-handoff logic follows the same
// rule). A miss (`N`) or a not-numeric/overflow answer (`T`) is returned
// as-is without touching any replica — nothing was written.
func (c *Client) incr(namespace, key []byte, delta int64) (value int64, ok bool, err error) {
	var raw []byte
	var ttlSeconds int64
	primaryOp := func(conn *connection) error {
		r, t, o, incrErr := conn.incrNS(namespace, key, delta)
		raw, ttlSeconds, ok = r, t, o
		return incrErr
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
			return 0, false, connectionLost("no owner is reachable for this key", nil)
		}
		primaryErr = c.applyNonIdempotent(names[0], primaryOp)
		if primaryErr == nil && ok {
			wg := c.fanReplicas(names[1:], func(conn *connection) error {
				return conn.setNS(namespace, key, raw, ttlSeconds)
			})
			wg.Wait()
		}
	}
	if primaryErr != nil || !ok {
		return 0, ok, primaryErr
	}

	value, err = parseIncrValue(raw)
	return value, ok, err
}

// parseIncrValue parses an `I` response's <value> body — decimal ASCII
// int64, the same grammar as the request's own <delta> (appendIncrFrame)
// — into the value Incr/Decr return. A parse failure here means the
// server's own response violated its own wire contract; wrapped as
// ErrProtocol exactly like any other malformed frame.
func parseIncrValue(raw []byte) (int64, error) {
	value, err := strconv.ParseInt(string(raw), 10, 64)
	if err != nil {
		return 0, protocolError(fmt.Sprintf("invalid incr value in response: %q", raw))
	}
	return value, nil
}

// clearNS drops every entry in namespace across every node — the same
// internal entry point a *Namespace handle's Clear forwards to (issue
// #106). A nil/empty namespace clears the default namespace; it is never
// rejected, matching getBytesNS/setBytesNS/deleteNS's own namespace("")
// rule. A namespace so large it alone would exceed maxRequestBytes is
// rejected client-side (issue #228) — a clear frame carries no key, so
// unlike validateKey/validateKeyAndValue there is nothing else to bound
// it against.
func (c *Client) clearNS(namespace []byte) error {
	if err := validateNamespaceForClear(namespace); err != nil {
		return err
	}
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

// GetWithToken returns key's raw value together with a CasToken, within
// this namespace. See Client.GetWithToken.
func (n *Namespace) GetWithToken(key string) (value []byte, token CasToken, ok bool, err error) {
	return n.client.getBytesWithTokenNS(n.namespace, key)
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

// GetMany returns every requested key's value, within this namespace,
// as a string. See Client.GetMany.
func (n *Namespace) GetMany(keys []string) (map[string]string, error) {
	raw, err := n.GetManyBytes(keys)
	if raw == nil {
		return nil, err
	}
	values := make(map[string]string, len(raw))
	for key, value := range raw {
		values[key] = string(value)
	}
	return values, err
}

// GetManyBytes returns every requested key's raw value within this
// namespace. See Client.GetManyBytes.
func (n *Namespace) GetManyBytes(keys []string) (map[string][]byte, error) {
	return n.client.getManyNS(n.namespace, keys)
}

// SetMany stores every value under its key within this namespace. See
// Client.SetMany.
func (n *Namespace) SetMany(values map[string]string, ttlSeconds int64) error {
	raw := make(map[string][]byte, len(values))
	for key, value := range values {
		raw[key] = []byte(value)
	}
	return n.SetManyBytes(raw, ttlSeconds)
}

// SetManyBytes stores every raw value under its key within this
// namespace. See Client.SetManyBytes.
func (n *Namespace) SetManyBytes(values map[string][]byte, ttlSeconds int64) error {
	return n.client.setManyNS(n.namespace, values, ttlSeconds)
}

// Incr atomically adds delta to key's stored counter value within this
// namespace and returns the result; ok is false when the key is missing.
// See Client.Incr.
func (n *Namespace) Incr(key string, delta int64) (value int64, ok bool, err error) {
	return n.client.incrNS(n.namespace, key, delta)
}

// Decr is Incr with delta negated, within this namespace. See Client.Decr.
func (n *Namespace) Decr(key string, delta int64) (value int64, ok bool, err error) {
	negated, err := negateDecrDelta(delta)
	if err != nil {
		return 0, false, err
	}
	return n.client.incrNS(n.namespace, key, negated)
}

// Clear drops every entry in this namespace across every node (issue
// #106) — see Client.ClearAll to flush every namespace at once, default
// included. namespace("")'s Clear clears the default namespace; it is
// never rejected.
func (n *Namespace) Clear() error {
	return n.client.clearNS(n.namespace)
}

// PutIfAbsent stores value under key within this namespace only if the
// key is currently absent. See Client.PutIfAbsent.
func (n *Namespace) PutIfAbsent(key string, value []byte, ttlSeconds int64) (bool, error) {
	return n.client.putIfAbsentNS(n.namespace, key, value, ttlSeconds)
}

// ReplaceIfPresent stores value under key within this namespace only if
// the key currently holds any value. See Client.ReplaceIfPresent.
func (n *Namespace) ReplaceIfPresent(key string, value []byte, ttlSeconds int64) (bool, error) {
	return n.client.replaceIfPresentNS(n.namespace, key, value, ttlSeconds)
}

// Replace stores newValue under key within this namespace only if the
// key's current content digest matches token. See Client.Replace.
func (n *Namespace) Replace(key string, token CasToken, newValue []byte, ttlSeconds int64) (bool, error) {
	return n.client.replaceNS(n.namespace, key, token, newValue, ttlSeconds)
}

// DeleteIfMatches removes key within this namespace only if its current
// content digest matches token. See Client.DeleteIfMatches.
func (n *Namespace) DeleteIfMatches(key string, token CasToken) (bool, error) {
	return n.client.deleteIfMatchesNS(n.namespace, key, token)
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
	// issue #192: wait for the keepalive goroutine to actually exit (it
	// observes c.stopKeepalive above and returns), not just for it to be
	// signalled — otherwise teardown() below could close a connection
	// while a ping against it is still in flight.
	c.keepaliveWG.Wait()
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
//
// Only for an idempotent operation (Get/Set/Delete/Clear/batched
// get-set) — replaying the whole operation, `i`/`k`/`x` included, is not
// safe; see withClusterRetryNonIdempotent below (issue #225).
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

// withClusterRetryNonIdempotent is withClusterRetry's counterpart for
// Incr/Decr, PutIfAbsent/ReplaceIfPresent/Replace, and DeleteIfMatches
// (issue #225). A stale-routing failure (ErrWrongNode) is retried exactly
// like withClusterRetry's own — the primary flatly rejected the request
// without acting on it, so nothing was applied and replaying after a
// refresh is safe. A connection-level failure (ErrConnectionLost) only
// retries the whole operation — which re-picks the primary from a
// refreshed ranking and would resend `i`/`k`/`x` — when it is also
// marked errRequestNotSent: applyNonIdempotent already gave the request
// one safe redial-and-retry at the connection layer, so an
// ErrConnectionLost still carrying that marker here means even the
// retried attempt never reached the wire (e.g. the redial itself
// failed), and trying again after a full node-list refresh remains safe.
// Once the marker is gone — the request may have reached and been
// executed by the primary — retrying here would risk exactly the same
// double-apply this whole issue is about, so it is surfaced as-is.
func (c *Client) withClusterRetryNonIdempotent(operation func() error) error {
	err := operation()
	if err == nil {
		return err
	}
	retryable := errors.Is(err, ErrWrongNode) ||
		(errors.Is(err, ErrConnectionLost) && errors.Is(err, errRequestNotSent))
	if !retryable {
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
// the operation runs again. Safe because Get/Set/Delete (and Clear/
// ClearAll, and the multi-get/multi-set chunks) are idempotent — op may
// genuinely run twice against the server. slot is "" in single mode.
//
// Do NOT use this for a non-idempotent op (Incr/Decr, PutIfAbsent/
// ReplaceIfPresent/Replace, DeleteIfMatches) — see applyNonIdempotent
// below (issue #225), which retries only when the request provably never
// reached the server.
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

// applyNonIdempotent is applyReconnecting's counterpart for Incr/Decr,
// PutIfAbsent/ReplaceIfPresent/Replace, and DeleteIfMatches (issue #225):
// their `i`/`k`/`x` frames are not safe to resend once the server may
// already have executed them — a resent increment would double-apply its
// delta, and a resent CAS that had already succeeded would come back as
// a mismatch. It retries via redial exactly like applyReconnecting
// (covering the same idle-FIN case its doc comment describes), but ONLY
// when connection.attemptRequest classified the failure as
// errRequestNotSent (see errors.go) — the request's bytes never left
// this process, so the server never had a chance to run it. Once a
// request's frame has actually been written, any later failure
// (ErrConnectionLost or ErrProtocol) is returned to the caller as-is:
// the operation may already be applied at the primary, so this
// deliberately does not retry — see Incr/Replace/DeleteIfMatches' own
// doc comments for the resulting at-least-once caveat.
func (c *Client) applyNonIdempotent(slot string, op func(*connection) error) error {
	conn, err := c.slotConnection(slot)
	if err != nil {
		return err
	}
	if err := op(conn); err != nil {
		if !errors.Is(err, errRequestNotSent) {
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

	// Fan out to the replicas concurrently with the primary write — see
	// fanReplicas for the failure/fire-and-forget semantics both this and
	// Incr's post-success result fan-out (issue #129) share.
	wg := c.fanReplicas(names[1:], func(conn *connection) error { return op(conn, false) })
	err := c.applyReconnecting(names[0], primaryOp)
	wg.Wait()
	return err
}

// fanReplicas runs op against every name in replicas (a key's non-primary
// owners) concurrently, without waiting for them — the returned
// *sync.WaitGroup lets the caller decide when (or whether) to wait. A
// failing leg is swallowed by design (client-side replication) — a dead or
// disagreeing replica leaves the key under-replicated until the next
// node-list refresh, never fails the caller's operation — and counted in
// Stats().ReplicaWriteFailures so operators can spot silently degrading
// replication.
//
// With FireAndForgetReplicas, up to maxInFlightBackgroundReplicaWrites legs
// run detached in the background instead — tracked on backgroundReplicaWG
// so Close() still drains them, but never added to the returned WaitGroup,
// so a caller that waits on it returns as soon as the legs it *is* waiting
// for are done (fire-and-forget replica writes). Past that cap, a leg
// falls back to the synchronous path below, exactly as with the option
// off.
//
// Shared by write() (issue #105's replicated Set/Delete, which runs op
// concurrently with the primary leg) and incr()'s post-success result
// fan-out (issue #129's Incr/Decr, which calls this only after the primary
// already succeeded — see incr's own doc comment for why it fans out the
// literal result via Set instead of replaying `i`).
func (c *Client) fanReplicas(replicas []string, op func(conn *connection) error) *sync.WaitGroup {
	replicaWrite := func(replica string) {
		if err := c.applyReconnecting(replica, op); err != nil {
			c.stats.replicaWriteFailures.Add(1)
		}
	}

	var wg sync.WaitGroup
	for _, name := range replicas {
		if c.fireAndForgetReplicas {
			select {
			case c.backgroundReplicaSem <- struct{}{}:
				// Register the background leg under c.mu, rechecking
				// c.closed: Close() sets c.closed under the same lock and
				// only then calls backgroundReplicaWG.Wait(), so this
				// ordering guarantees every Add happens-before that Wait.
				// Without it, a leg racing Close can call Add(1) just as
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

		wg.Add(1)
		go func(replica string) {
			defer wg.Done()
			replicaWrite(replica)
		}(name)
	}
	return &wg
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
	c.keepaliveWG.Add(1)
	go func() {
		defer c.keepaliveWG.Done()
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

			pingIdleConnections(connections, interval)
		}
	}()
}

// pingIdleConnections sends a keepalive probe to every idle connection in
// connections, in parallel — one goroutine per connection, all joined
// before this returns (issue #192). Previously this ran sequentially, so
// a slow or hung node delayed the ping reaching every other member until
// its own request finished or timed out; bounded parallelism here means
// one slow connection no longer holds up the rest. Extracted from
// startKeepalive's loop body so it can be driven directly, with a
// controlled connection order, in tests.
func pingIdleConnections(connections []*connection, interval time.Duration) {
	var pings sync.WaitGroup
	for _, conn := range connections {
		if conn.isClosed() || conn.idle() < interval {
			continue // dead ones stay lazy; busy ones don't need a ping
		}
		pings.Add(1)
		go func(conn *connection) {
			defer pings.Done()
			// Any parseable reply proves liveness — N, or W from a
			// non-owner — and resets the server's idle timer.
			_, _, _ = conn.get(keepaliveKey)
		}(conn)
	}
	pings.Wait()
}
