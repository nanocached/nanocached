//! Standalone cluster-membership registry for nanocached cache nodes.
//!
//! This binary has no dependency on the cache server's own protocol code —
//! `nanocached-node` and `nanocached-discovery` independently implement
//! their own, unrelated wire protocols (size-derived migration timeout),
//! so nothing protocol-shaped is shared. It does share `nanocached::infra`
//! with the other binaries: connection- and process-level glue (TLS setup,
//! the metrics HTTP responder, shutdown handling, accept-loop backoff,
//! per-IP connection limiting) with nothing to do with either protocol —
//! see that crate's own module docs for the line this policy draws. Run it
//! via `ncd discovery start`, or directly as `nanocached-discovery`.
//!
//! Protocol (ASCII header line, terminated by `\n`; a command may repeat
//! on the same connection):
//!
//!   H <name-length> <r> <token-length> [<joiner-length>]\n<name><token>[<joiner>]
//!                             Heartbeat,
//!                             identified by name (a random per-process
//!                             identity, node identity decoupled from address — not the node's
//!                             address, which carries no identity
//!                             meaning and was already established by
//!                             `J` on this connection). Only valid for a
//!                             node already `Joined` (see below) —
//!                             refreshes its liveness. `token` is the
//!                             node's per-process membership token
//!                             (issue #34, see `NodeInfo::token`) and
//!                             must match the one its registration
//!                             established. `r` is the
//!                             replication factor this node currently
//!                             believes (issue #30) — learned from a `M`
//!                             this node has sent as an client-side replication handoff
//!                             source, or `0` if it hasn't sent one yet
//!                             and so has no belief to report. When `r` is
//!                             nonzero and disagrees with this replica's
//!                             own `--replication-factor`, the mismatch is
//!                             logged loudly and recorded (not rejected —
//!                             see discovery HA, replicas never reconcile
//!                             membership with each other); unlike
//!                             membership, replication factor is static
//!                             config, so a recorded mismatch — once it
//!                             becomes a strict majority of voting nodes,
//!                             not merely one — makes this replica refuse
//!                             `L` until it clears — see below. Response:
//!                             `A <count> <replication>\n` followed by
//!                             `count` entries in exactly `L`'s entry
//!                             format (below) — the roster of currently-
//!                             `Joined` nodes as this replica sees it, so
//!                             the node can keep its own membership view
//!                             (which it otherwise only learns from `M`,
//!                             i.e. on joins) current across liveness
//!                             evictions too (issue #61: survivors kept
//!                             routing keys to an evicted node, answering
//!                             `W` for them until the next join). The
//!                             roster is withheld — a bare `A\n`, "no
//!                             update" — exactly when `L` would answer
//!                             `B\n`: during the startup grace (the
//!                             registry is still re-filling and a partial
//!                             roster would make nodes reject keys they do
//!                             own) and while a strict majority of voting
//!                             nodes dispute this replica's replication
//!                             factor. The optional trailing
//!                             `<joiner-length>` / `<joiner>` (issue #571)
//!                             names a node whose handoff the sender
//!                             completed but which it has not yet seen in
//!                             any roster; the ack (withheld or not) is
//!                             then followed by exactly one more line —
//!                             `K\n` (still registered, or not known to be
//!                             gone) or `G\n` (on record as having left,
//!                             been evicted or abandoned, and not
//!                             registered since) — so a joiner that was
//!                             promoted and gone again between two of the
//!                             sender's heartbeats does not keep its
//!                             forwarding window open for the whole grace.
//!                             A reader that sent the field must consume
//!                             that line.
//!
//!   L\n                       List currently `Joined` nodes. Response:
//!                             `N <count> <replication>\n` (the replication
//!                             factor is this replica's own, issue #30)
//!                             followed by `count` entries, each
//!                             `<name-length> <addr-length>\n<name><addr>\n`
//!                             — note the trailing newline after every
//!                             entry. `name` (node identity decoupled from address) is what hash-ring
//!                             computations use; `addr` is only for opening
//!                             a connection. Refused with
//!                             `B\n` (connection then closed) if a STRICT
//!                             MAJORITY of currently-`Joined` nodes whose
//!                             last heartbeat reported a replication-
//!                             factor belief disagree with this replica's
//!                             own (issue #30, amended: not merely "any",
//!                             which would let one misconfigured or
//!                             behind-the-times node deny `L` to the whole
//!                             cluster by itself) — a tie does not refuse.
//!                             This replica's `config.replication`, which
//!                             this response embeds, is then a value known
//!                             to disagree with what most of the cluster
//!                             learned elsewhere.
//!
//!   T <name-length> <token-length>\n<name><token>   Issue #295: a
//!                             self-authenticated roster fetch for an
//!                             already-registered node (`name`/`token`
//!                             are checked against the registry the same
//!                             way `H`'s are — a mismatch closes the
//!                             connection with no response). Response:
//!                             `N <count> <replication>\n` followed by
//!                             `count` entries, each `<name-length>
//!                             <addr-length> <token-length>\n<name><addr>
//!                             <token>\n`, for EVERY registered node
//!                             regardless of state (`Waiting`/`Joining`/
//!                             `Joined`) — unlike `L`, deliberately
//!                             including each node's own token, since the
//!                             caller has already proven itself a
//!                             registered node, not merely a
//!                             shared-secret-holding client (see
//!                             `NodeInfo::token`'s doc comment). Used by a
//!                             decommissioning node and by
//!                             eviction-triggered re-replication to
//!                             authorize the `U`/`u` frames they send —
//!                             see `src/server.rs`'s `Command::HandoffSet::
//!                             token`.
//!
//!   J <name-length> <port> <token-length>\n<name><token>   Ask to join
//!                             (staged node join), declaring the node's own name
//!                             (node identity decoupled from address), the port it serves on (the
//!                             reachable address is composed from this
//!                             connection's source IP, addresses derived from the registration connection), and its
//!                             membership token (issue #34) — the
//!                             credential every later `P`/`H`/`C` naming
//!                             this node must present. Sent once; the
//!                             connection is then held open (no idle
//!                             timeout applies) rather than closed or
//!                             reused for anything else, since this is
//!                             the node's only channel for learning about
//!                             a state change it didn't itself cause.
//!                             When the node is promoted to `Joined`,
//!                             discovery pushes `R\n` on this same
//!                             connection, which then becomes that node's
//!                             ordinary heartbeat connection (`H` from
//!                             here on).
//!
//!   P <name-length> <port> <token-length> [G]\n<name><token>   Announce
//!                             (discovery HA): an already-promoted node
//!                             (re-)declaring "I am a `Joined` member at
//!                             this address" (composed like `J`'s) —
//!                             after a heartbeat connection broke, after
//!                             this process restarted with an empty
//!                             registry, or to a standby replica it never
//!                             `J`ed with. Upserts the node straight to
//!                             `Joined` with no staged node join handoff.
//!                             Response: `R\n`, after which the
//!                             connection carries `H` heartbeats, exactly
//!                             like a `J` connection after promotion.
//!                             Rejected for a name currently mid-join
//!                             (`Waiting`/`Joining`), and — issue #34 —
//!                             for a registered name whose stored token
//!                             doesn't match `token`, so knowing another
//!                             node's (public, `L`-listed) name is not
//!                             enough to redirect its traffic.
//!                             A `G` after the token lengths
//!                             (`P <name-length> <port> <token-length> G`)
//!                             says the node can start over: if `name` is
//!                             on record as departed (evicted for missed
//!                             heartbeats, left, or its join abandoned
//!                             within `DEPARTED_RETENTION`) and not
//!                             registered, discovery answers `G\n` and
//!                             closes without registering it. The cluster
//!                             moved on without that node — re-replicated
//!                             its keys, took writes for them — so what it
//!                             still holds is not to be served; the node
//!                             clears its cache and sends `J`. A discovery
//!                             that has just restarted has no record, so a
//!                             node that kept heartbeating through the
//!                             restart still re-registers at once. Without
//!                             the `G` (a node of an earlier version, or an
//!                             announce to a standby replica) the announce
//!                             is accepted as before.
//!
//!   C <name-length> <joining-length> <token-length> [<generation>]\n
//!   <name><joining><token>   Sent by an already-`Joined` node to report
//!                             it has finished handing its share of the
//!                             keyspace off to `joining` (the joining
//!                             node's name). Naming the join it is for
//!                             keeps a stale report from an abandoned
//!                             handoff from being credited to whatever
//!                             join is pending next. Ignored unless
//!                             `joining` matches the in-progress join and
//!                             `token` matches the reporting node's
//!                             registered one (issue #34). Response: `A\n`.
//!
//!                             `generation` (issue #421(c), tenth-pass
//!                             audit 2026-09-02) is a trailing plain (not
//!                             length-prefixed) field, the join
//!                             generation the reporting node's own `M`
//!                             carried, echoed back so a report for a
//!                             since-abandoned-and-superseded join that
//!                             reused the same `joining` name isn't
//!                             wrongly credited to the fresh one — see
//!                             `handle_complete`. Omitted by a node whose
//!                             `M` itself carried none (this discovery
//!                             process, or a discovery HA peer it was
//!                             registered with at the time, predates this
//!                             fix) — accepted as a legacy report, name+
//!                             token only, same as before this fix. The
//!                             generation protection is only complete
//!                             once every discovery replica AND every
//!                             node in the cluster has upgraded past this
//!                             fix; during a rolling upgrade a legacy `C`
//!                             is still possible and is not rejected for
//!                             lacking `generation`.
//!
//!   V <name-length> <token-length>\n<name><token>   Leave: the node
//!                             removes itself from membership, in any state
//!                             (a `Joined` member that has finished handing
//!                             its keys off, or a `Waiting`/`Joining` node
//!                             aborting its own join, which ends the join),
//!                             so the next `L`, heartbeat ack and `T` serve
//!                             the roster without it at once instead of
//!                             after the liveness timeout. Response: `R\n`
//!                             (also for a name that is not registered, so
//!                             a repeat is harmless). A `token` that does
//!                             not match the registered one closes the
//!                             connection with no response. Served during
//!                             the startup grace.
//!
//!   Y <name-length> <port> <token-length>\n<name><token>   Proxy
//!                             announce: a `nanocached-proxy` registering,
//!                             or refreshing, itself in the proxy roster
//!                             that `Q` lists (the address is composed from
//!                             this connection's source IP and `port`, like
//!                             `J`'s). The proxy repeats it to stay listed;
//!                             one not heard from for `--liveness-timeout`
//!                             is dropped. Response: `R\n`, and the
//!                             connection stays open for the next command. A
//!                             `token` that differs from the registered
//!                             one for `name` (a name hijack), or a full
//!                             roster (`MAX_PROXY_ENTRIES`), closes the
//!                             connection with no response. Proxies are not
//!                             cache nodes: they never appear in `L`, hold
//!                             no keys and take part in no join. Served
//!                             during the startup grace.
//!
//!   Z <name-length> <token-length>\n<name><token>   Proxy deregister:
//!                             a draining proxy leaves the proxy roster at
//!                             once instead of lingering until the liveness
//!                             timeout. Response: `R\n` (also for a name
//!                             that is not listed). A `token` that does not
//!                             match closes the connection with no
//!                             response. Served during the startup grace.
//!
//!   Q\n                       List the proxies announced with `Y`.
//!                             Response: `N <count>\n` followed by `count`
//!                             entries, each `<name-length> <addr-length>\n
//!                             <name><addr>\n`. Answered `B\n` (connection
//!                             closed) during the startup grace, like `L`:
//!                             an empty list there would read as "no
//!                             proxies exist".
//!
//!   A <secret-length>\n<secret>   Authenticate. Response: `Od\n` on success,
//!                             `Ed\n` (then the connection closes) if a
//!                             secret is configured and this doesn't match
//!                             it. If no secret is configured (the
//!                             `NANOCACHED_AUTH_SECRET` environment
//!                             variable is unset or empty), this is a
//!                             no-op that always succeeds. If a secret is
//!                             configured, every other command is rejected
//!                             with `Ed\n` until a matching `A` has been
//!                             sent on the connection. A request that does
//!                             not parse (an unknown command, a bad length,
//!                             a stray token) is answered `Ed\n` as well,
//!                             and the connection closes — after reading
//!                             what the client is still sending, for at
//!                             most a second, so the reply is not lost to
//!                             a reset (see `drain_rejected_request`). The
//!                             `d` distinguishes
//!                             this response from nanocached-node's own
//!                             `On\n`/`En\n`, letting a client tell the two
//!                             apart from the response to the same `A`
//!                             request without knowing in advance which it
//!                             dialed.
//!
//! If the connection limit has been reached, the server responds with
//! `B\n` and closes the connection instead of accepting the command. `L`
//! is answered the same way (`B\n`, connection closed) during the startup
//! grace period (discovery HA, one liveness-timeout long): after a restart the
//! registry re-fills from `P` announces within about one heartbeat
//! interval, and until the grace has passed a fresh client must not build
//! a ring from the partial list. `T` and `Q` are answered the same way
//! (`B\n`, connection closed), and `H` is acknowledged with a bare `A\n`
//! (no roster, see above). The commands recovery depends on work during
//! the grace: `P`, `Y`, `V`, `Z`, `C`, `A` and `J` are served as usual,
//! except that a `J` is only registered (`Waiting`): its join does not
//! start until the grace has passed, so that it is handed off against the
//! re-filled registry rather than a partial one.
//!
//! A node moves through three states (staged node join): `Waiting` (registered via
//! `J`, but either another join is already in progress or its handoff
//! hasn't started), `Joining` (actively receiving its handoff from every
//! `Joined` node), and `Joined` (handoff complete, included in `L`
//! responses, now heartbeating normally via `H`). Only `Joined` nodes are
//! visible to clients; `Waiting`/`Joining` nodes are excluded from `L`
//! exactly like a node that was never registered. Only one node moves
//! through `Waiting` -> `Joining` at a time. A `Waiting`/`Joining` node has
//! no heartbeat to time out — its liveness is tied to the one connection
//! it opened with `J`, which discovery holds open the whole time; the
//! registry entry is dropped if that connection dies before promotion. A
//! `Waiting` node is additionally dropped (and its connection closed) if it
//! goes unpromoted past `waiting_timeout_for`, one source address may only
//! hold `MAX_WAITING_PER_SOURCE_IP` such registrations at once, and no more
//! than `MAX_WAITING_TOTAL` may be outstanding cluster-wide — otherwise,
//! with authentication unset, a `J` under a fresh unverifiable name always
//! succeeds, so nothing would stop one source (or many sources acting in
//! concert) from parking `MAX_CONNECTIONS` fake registrations forever. A
//! `Joined` node that stops
//! sending heartbeats is dropped once
//! `--liveness-timeout` has elapsed since its last heartbeat; no explicit
//! "leave" message is required, so this covers both graceful shutdown and
//! crashes. Because the registry is rebuilt from `P` announces (discovery HA)
//! within about one heartbeat interval, this process can be restarted at
//! any time and self-heals with no data movement, modulo any join in
//! progress at the time (not yet designed — see staged node join); it
//! can also run as several independent replicas that each converge on the
//! same registry by listening to the same nodes.

use bytes::{Bytes, BytesMut};
use nanocached::infra;
use nanocached::infra::{
    ACCEPT_ERROR_BACKOFF, LogLimiter, METRICS_MAX_CONNECTIONS, MaybeTls, PerIpConnections,
    constant_time_eq, load_tls_acceptor, load_tls_connector, per_ip_key, read_http_request_path,
    reject_over_limit, should_backoff_after_accept_error, shutdown_signal, try_acquire_per_ip,
    write_http_response,
};
use rustc_hash::{FxHashMap, FxHashSet};
use std::collections::HashMap;
use std::collections::HashSet;
use std::collections::VecDeque;
use std::io;
use std::net::{IpAddr, SocketAddr};
use std::process::ExitCode;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{Notify, Semaphore, watch};
use tokio::task::JoinSet;
use tokio::time::{Instant, interval, timeout, timeout_at};
use tokio_rustls::{TlsAcceptor, TlsConnector};

const READ_CHUNK_SIZE: usize = 256;
const MAX_REQUEST_SIZE: usize = 4096;
const MAX_CONNECTIONS: usize = 1024;
/// Issue #358: metrics/health/ready connections accepted at once on
/// `--metrics-port` — a small, fixed cap dedicated to that listener,
/// Coarse cap on how many live connections a single source IP may hold at
/// once, layered under the global `MAX_CONNECTIONS` semaphore (mirrors
/// `src/server.rs`'s own `MAX_CONNECTIONS_PER_IP`: no per-source-IP
/// limit on *ordinary* connections meant a single misbehaving or
/// compromised peer could otherwise claim the entire `MAX_CONNECTIONS`
/// budget by itself — heartbeats, `L`, everything — starving every other
/// client and node, without the global semaphore ever reporting anything
/// unusual). Deliberately coarse, not a tight per-client budget: a
/// pooled application host — many worker processes or threads sharing
/// one egress IP, or a fleet behind one NAT — can legitimately hold a
/// large number of concurrent connections to discovery, and this guard
/// exists only to stop one source from monopolising the whole process,
/// not to bound ordinary legitimate concurrency.
///
/// Distinct from `MAX_WAITING_PER_SOURCE_IP`: that one bounds a much
/// narrower thing — concurrent `Waiting`/`Joining` *join registrations*
/// from one source, a slice of this process's application state — while
/// this bounds raw TCP connection count regardless of what a connection
/// ever does with itself (heartbeats, `L`, an idle client that never
/// sends anything). A source could hold up to `MAX_WAITING_PER_SOURCE_IP`
/// pending joins and still be well under this cap from its other,
/// non-joining connections. See `try_acquire_per_ip`.
const MAX_CONNECTIONS_PER_IP: usize = 256;
/// Upper bound on distinct registered node names. `J` (Join) holds one
/// connection open per node, so it's already bounded by `MAX_CONNECTIONS`,
/// but `P` (Announce) does not hold its connection — a single connection
/// can insert unlimited distinct `Joined` entries as fast as it sends
/// ~20-byte messages, growing the registry (and every `L` response built
/// from it) without bound until liveness sweep catches up. This caps that.
/// Far above any realistic cluster size, so legitimate nodes never hit it.
///
/// This alone doesn't bound `M`'s wire size, though — see
/// `NODE_MAX_REQUEST_SIZE` and `start_join`'s own size check, which is
/// what actually keeps a large registry from producing an `M` a node
/// will refuse to even parse.
const MAX_REGISTRY_SIZE: usize = 1 << 16;

/// Issue #299: caps how many outbound connect attempts a fan-out over the
/// registry (`try_begin_next_join` sending `M` to every `Joined` node at
/// once on join start; `abandon_current_join` sending `X` to every ready
/// member of `PendingJoin::expected` at once on abandon) may have in
/// flight at the same time. In a no-auth deployment, `P` registers into
/// `Joined` gated only by the announce limiter (`ANNOUNCE_INSERT_BURST`
/// new names per source at once, then one per `ANNOUNCE_INSERT_COOLDOWN`),
/// up to `MAX_REGISTRY_SIZE` (65536) entries — nothing stops an
/// attacker rotating source IPs from inflating the registry that far.
/// Without a bound here, the next legitimate `J`/`V` fans a `JoinSet` out
/// to every one of those entries concurrently, opening that many
/// simultaneous outbound connects well before `MAX_REGISTRY_SIZE` itself
/// is the limiting factor — exhausting file descriptors (a typical ulimit
/// is 1024) long before a real cluster would ever approach that size. This
/// only throttles how many sends are actually dialing at once (via a
/// `Semaphore` each spawned task acquires before dialing) — every node is
/// still contacted and every result is still collected exactly as before;
/// see "bound everything that waits". Comfortably above any real
/// migration/abandon fan-out width (a legitimate cluster's node count),
/// so normal operation never notices it.
const MAX_FANOUT_CONCURRENCY: usize = 64;

/// Issue #1100: how many nodes `P` may admit, and send the join's `M` to,
/// while one join is pending (see `hand_off_from_late_member`). Each is an
/// outbound dial of up to `MIGRATE_SEND_ATTEMPTS` x `OUTBOUND_IO_TIMEOUT`
/// and a join the pending slot has to wait for, and a `P` costs its sender
/// a few dozen bytes — so without a bound one source rotating names during
/// a join (60 s or more) grows that work as far as `MAX_REGISTRY_SIZE`.
/// Further `P`s for new names are refused until the join ends; a node that
/// healed from a partition retries its announce and is admitted by the
/// next join's own roster or after this one. The same width as
/// `MAX_FANOUT_CONCURRENCY`: far above the handful of nodes a partition
/// heal brings back in practice.
const MAX_LATE_MEMBERS_PER_JOIN: usize = 64;

/// Issue #122: bound on registered proxies — a fleet has orders of
/// magnitude fewer proxies than clients, so this is generous, and it
/// keeps a secret-holder from growing the proxy map without bound.
const MAX_PROXY_ENTRIES: usize = 1024;
/// `nanocached-node`'s own inbound request-size cap (`MAX_REQUEST_SIZE`,
/// `src/server.rs`) — the two binaries share no modules by design (see
/// this file's own module doc comment), so this is a separate constant
/// kept in sync by convention, the same way the migration-timeout pair
/// in size-derived migration timeout already is. An `M` this process sends
/// (`send_migrate`) is, from the receiving node's point of view, just
/// another request subject to that cap; `M`'s payload is dominated by
/// the `joined` roster (one entry per currently-`Joined` node), so with
/// `MAX_REGISTRY_SIZE` (65536) worth of nodes it can exceed this cap
/// long before a legitimate join's own data volume ever could — at
/// which point the node rejects the connection outright (issue: `M`
/// too large for `MAX_REQUEST_SIZE`) and the join silently stalls until
/// discovery's own size-derived migration timeout (size-derived migration timeout)
/// reaps it, with no clearer signal than that. `start_join` checks
/// against this up front instead, so an admission that would produce an
/// oversized `M` is rejected immediately, with a clear log line, rather
/// than left to time out. See `migrate_message_len`.
const NODE_MAX_REQUEST_SIZE: usize = 1024 * 1024;
/// Refill interval of the per-source token bucket that paces *new*
/// registry insertions via `P`: one more insertion is allowed per source
/// (an IPv6 source is its /64, see `per_ip_key`) every this often, on top
/// of the `ANNOUNCE_INSERT_BURST` it may spend at once
/// (issue: `MAX_REGISTRY_SIZE`'s own doc comment above — a bare `P` holds
/// no connection open, so nothing but that hard ceiling used to bound how
/// fast a single source could approach it; connect -> `A` -> `P` ->
/// disconnect, repeated with a fresh name each time, could otherwise fill
/// the registry in well under a second). Only checked for a name this
/// replica doesn't already know — a legitimate node re-announcing itself
/// after a broken heartbeat connection or a restart always refreshes its
/// own existing entry (see the `P` handler), never touching this limiter,
/// so normal reconnect traffic is unaffected regardless of how often it
/// happens. A rejection just delays that source's next attempt; the
/// node's heartbeat loop redials and retries, so this is a rate limit,
/// not a permanent block. See `announce_insert_allowed`.
const ANNOUNCE_INSERT_COOLDOWN: Duration = Duration::from_secs(2);
/// How many *new* registrations one source may make at once before the
/// `ANNOUNCE_INSERT_COOLDOWN` pacing applies. Issue #1148: with a plain
/// one-per-cooldown gap, a fleet that shares a source key — IPv4 nodes
/// behind one NAT address, or every node of one AWS VPC IPv6 subnet, which
/// is a /64 — re-registered one node per 2 s after a discovery restart or a
/// partition, so the startup grace (one liveness timeout) ended with a
/// partial registry and the first join ran against it, the very thing the
/// grace exists to prevent. A burst lets a whole fleet re-register at once
/// while a sustained stream of forged names from one source is still paced
/// to one per cooldown; `MAX_REGISTRY_SIZE` stays the hard ceiling. Sized
/// like `MAX_LATE_MEMBERS_PER_JOIN`: the most members one join is built
/// to absorb.
const ANNOUNCE_INSERT_BURST: u32 = 64;
/// Bounds `AnnounceLimiter`'s own memory: without this, an attacker who
/// cycles through source IPs (trivial to spoof/rotate, unlike holding
/// open `MAX_CONNECTIONS` real connections) could grow the limiter map
/// itself without bound, defeating the very thing it exists to bound.
/// Far above any realistic number of distinct legitimate source
/// addresses in one cluster's network.
const MAX_ANNOUNCE_LIMITER_ENTRIES: usize = 4096;
const DEFAULT_LIVENESS_TIMEOUT: Duration = Duration::from_secs(15);
/// How often a node heartbeats, in seconds. Mirrors
/// `DEFAULT_HEARTBEAT_INTERVAL_SECS` in `src/main.rs` (the node binary fixes
/// the interval; discovery does not link `server.rs`, so the value is
/// repeated here and `node_heartbeat_interval_matches_the_node_binary`
/// fails if the two drift apart).
const NODE_HEARTBEAT_INTERVAL_SECS: u64 = 5;
/// A `Joined` node heard from again after this long without a heartbeat or
/// an announce was out of contact: at least one heartbeat did not arrive.
/// Below two intervals so that a single lost heartbeat (a gap of two
/// intervals) counts, above one and a half so that one heartbeat that is
/// late by a few seconds does not. `note_contact` records such a gap in the
/// node's `NodeInfo::contact_breaks`, which is what `announce_needs_rejoin`
/// reads.
const CONTACT_BREAK: Duration = Duration::from_millis(NODE_HEARTBEAT_INTERVAL_SECS * 1800);
/// Most contact breaks kept per node. They are also dropped once older than
/// `DEPARTED_RETENTION`, the longest a departure can be asked about, so the
/// cap only matters for a node whose heartbeat connection drops more than
/// once every few seconds for an hour; its oldest breaks go first.
const MAX_CONTACT_BREAKS: usize = 1024;
/// Heartbeats that fit in the smallest accepted liveness window: one that
/// is due, one that may be late or lost, and one so that the stall guard
/// (`sweep_was_stalled` treats a sweep gap over half the window as a pause
/// of discovery itself) still spans a whole interval.
const MIN_LIVENESS_HEARTBEATS: u64 = 3;
/// Smallest `--liveness-timeout` accepted, in seconds. Anything at or below
/// one heartbeat interval evicts live members between two heartbeats and
/// the membership flaps for good; below three, a pause of discovery shorter
/// than the stall guard (half the window) or one lost heartbeat is enough to
/// evict healthy nodes, which then come back to a `G` answer and lose their
/// caches. `0` also made the sweep tick every millisecond, evict every node
/// on its first tick and set the startup grace to nothing.
const MIN_LIVENESS_TIMEOUT_SECS: u64 = NODE_HEARTBEAT_INTERVAL_SECS * MIN_LIVENESS_HEARTBEATS;
/// Largest `--liveness-timeout` accepted, in seconds (7 days, the node's
/// own `--drain-timeout` cap). The value is added to `Instant::now()` for
/// the startup grace and the sweep deadlines, and an unbounded `u64` there
/// panics on overflow instead of failing as a usage error.
const MAX_LIVENESS_TIMEOUT_SECS: u64 = 7 * 24 * 60 * 60;
/// Staged node join pattern-3 guard: a ready node can be alive (heartbeating
/// normally) yet never report `C` for a handoff it's mid-`Migrate` for —
/// no TCP-level signal distinguishes "legitimately still working" from
/// "stuck" (a lost ack, a bug), so this is a plain timeout. Past it,
/// `abandon_current_join` scraps the join and sends every ready node an
/// `X` to roll back, exactly as it does when a node's connection dies
/// outright (patterns 1/2). Size-derived rather than flat
/// (size-derived migration timeout): a large, legitimate join shouldn't get reaped
/// just for being large, so the bound scales with the largest entry
/// count any ready node reported acknowledging its `M`
/// (`PendingJoin::max_entries`). Both constants are hardcoded, not
/// configurable — `--migration-timeout` no longer exists. See
/// `migration_timeout_for`.
const MIGRATION_TIMEOUT_BASE: Duration = Duration::from_secs(60);
const MIGRATION_TIMEOUT_PER_ENTRY: Duration = Duration::from_millis(5);
/// Ceiling on the size-derived timeout. `max_entries` comes from a ready
/// node's `A <entries>` ack, which is untrusted: a malicious or buggy
/// node can claim `u32::MAX` and then never send `C`, inflating the
/// abandon deadline to ~248 days. Since only one join runs cluster-wide
/// at a time (`try_begin_next_join`), that would stall every future join
/// for the life of the process. Clamp so the reaper always fires within a
/// bounded window — well above any legitimate handoff (1M entries ≈ 83min
/// uncapped), so honest large joins are unaffected.
const MIGRATION_TIMEOUT_MAX: Duration = Duration::from_secs(2 * 60 * 60);

/// Saturates rather than overflows for a pathologically large count, then
/// clamps to `MIGRATION_TIMEOUT_MAX` so an untrusted ack can't push the
/// deadline arbitrarily far out.
fn migration_timeout_for(max_entries: usize) -> Duration {
    let scaled = MIGRATION_TIMEOUT_BASE
        + MIGRATION_TIMEOUT_PER_ENTRY.saturating_mul(max_entries.min(u32::MAX as usize) as u32);
    scaled.min(MIGRATION_TIMEOUT_MAX)
}
/// Flat slack added on top of `waiting_timeout_for`'s queue-position-scaled
/// bound, absorbing ordinary scheduling/network jitter around the worst
/// case rather than being the primary bound itself.
const WAITING_TIMEOUT_MARGIN: Duration = Duration::from_secs(60);
/// Small per-source-IP cap on concurrent `Waiting`/`Joining` registrations
/// (issue: with auth unset, an unauthenticated attacker can `J` under
/// distinct fake names — `wait_for_promotion` holds each such connection
/// open indefinitely and `sweep_expired` never swept `Waiting`/`Joining`
/// nodes — up to `MAX_CONNECTIONS`, permanently exhausting connection
/// slots). Small enough that no legitimate deployment plausibly starts
/// this many nodes from one address at once, but well above the 1 a
/// single legitimate node ever needs.
const MAX_WAITING_PER_SOURCE_IP: usize = 4;
/// Global cap on concurrent `Waiting`/`Joining` registrations, across every
/// source address (issue: join-queue starvation — `MAX_WAITING_PER_SOURCE_IP`
/// alone only bounds how many one source can hold, not how deep the queue
/// can get in total; many distinct sources, each within their own per-IP
/// cap, could still queue enough nodes behind each other that a late
/// arrival's `waiting_timeout_for` bound — scaled by queue position —
/// stretches implausibly long before `sweep_expired` gives up on it. Only
/// a genuinely new name counts against this, exactly like
/// `MAX_WAITING_PER_SOURCE_IP` — a duplicate `J` reusing an existing entry
/// must not double-count. Far above any realistic number of nodes
/// legitimately joining a cluster at once, but small enough that even the
/// deepest permitted queue keeps `waiting_timeout_for`'s worst case (see
/// `MAX_WAITING_TIMEOUT_POSITIONS`, which bounds it independently of this
/// constant) in a sane range. See `start_join`.
const MAX_WAITING_TOTAL: usize = 32;
/// Bounds how long a `Waiting` node's `J` connection is held open before
/// discovery gives up on it and closes it (issue: see
/// `MAX_WAITING_PER_SOURCE_IP` — the cap alone still lets a handful of
/// connections per attacker IP, times many IPs, sit parked forever with
/// no other node able to use those `MAX_CONNECTIONS` slots; this reclaims
/// them once no join is plausibly still coming for them).
///
/// Only one join runs cluster-wide at a time (staged node join), and each one is
/// itself bounded by `MIGRATION_TIMEOUT_MAX` before `abandon_current_join`
/// reaps it — so a node that arrived behind `queue_position - 1` other
/// `Waiting`/`Joining` nodes can legitimately need up to that many multiples
/// of `MIGRATION_TIMEOUT_MAX` before its own turn ever comes up. Scaling
/// the bound by `queue_position` — captured once, at registration, in
/// `NodeInfo::queue_position` — rather than using a flat bound means a
/// deep but genuine queue is never cut short; scaling instead by whatever
/// the queue depth happens to be *at sweep time* would be wrong in the
/// other direction (it shrinks as earlier nodes are promoted/reaped,
/// which would then retroactively make a node's own elapsed wait look
/// like a timeout it never actually earned).
fn waiting_timeout_for(queue_position: usize) -> Duration {
    MIGRATION_TIMEOUT_MAX
        .saturating_mul(queue_position.min(MAX_WAITING_TIMEOUT_POSITIONS) as u32)
        .saturating_add(WAITING_TIMEOUT_MARGIN)
}
/// Caps the queue-position multiplier `waiting_timeout_for` applies
/// (issue: join-queue starvation — `MAX_WAITING_TOTAL` bounds the queue's
/// concurrent *size*, but nothing previously bounded how long a single
/// node queued deep within that allowance might have to wait for its own
/// turn; the multiplier scaled with `queue_position` unconditionally, so
/// the bound grew without limit as the queue got deeper). Below
/// `MAX_WAITING_TOTAL` on purpose: even the deepest permitted queue then
/// has a fixed worst case —
/// `MAX_WAITING_TIMEOUT_POSITIONS * MIGRATION_TIMEOUT_MAX +
/// WAITING_TIMEOUT_MARGIN` (about 16 hours at the current constants),
/// rather than `MAX_WAITING_TOTAL * MIGRATION_TIMEOUT_MAX` (about 64
/// hours) — a range an operator would consider the join simply failed
/// long before `sweep_expired` ever gave up on it. A node queued behind
/// more than `MAX_WAITING_TIMEOUT_POSITIONS - 1` others is still served
/// in order — this only clamps how long it's willing to keep waiting
/// before giving up and letting its own `J` connection be reclaimed.
const MAX_WAITING_TIMEOUT_POSITIONS: usize = 8;
const IDLE_TIMEOUT: Duration = Duration::from_secs(60);
/// Bounds how long a connection may hold one of `MAX_CONNECTIONS` open
/// without ever successfully parsing a single complete command (issue:
/// slowloris via `MAX_CONNECTIONS` exhaustion). `IDLE_TIMEOUT` alone
/// doesn't bound this: a connection that trickles in one byte just under
/// `IDLE_TIMEOUT` apart resets the per-read timeout forever without ever
/// finishing a frame. This is a fixed deadline measured from when the
/// connection started (not reset per read, unlike `IDLE_TIMEOUT`), so no
/// amount of slow byte-at-a-time trickling extends it — see
/// `handle_connection`'s `unidentified_deadline`. Set equal to
/// `IDLE_TIMEOUT`: a well-behaved peer sends its first command in one
/// write, well under either bound, so this only ever fires for a
/// connection already behaving like an attack. Once any command is
/// successfully parsed, this bound stops applying — `IDLE_TIMEOUT` alone
/// governs from then on, exactly as before this fix.
const UNIDENTIFIED_CONNECTION_TIMEOUT: Duration = IDLE_TIMEOUT;
/// Bounds a response write (mirrors `src/server.rs`'s own `WRITE_TIMEOUT`
/// and `write_response`) — see `write_response` below. Shorter than
/// `IDLE_TIMEOUT`: that one tolerates a normal gap between a client's
/// requests, but a peer that has simply stopped draining its receive
/// buffer is a distinct failure that shouldn't get to hold a
/// `MAX_CONNECTIONS` permit for as long as an idle-but-otherwise-fine
/// connection is allowed to sit.
const WRITE_TIMEOUT: Duration = Duration::from_secs(10);
const SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(10);
const TLS_HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(10);
/// Bounds every outbound dial and ack read this process makes toward a
/// node (`M`/`X`, issue #6): without it, one node that accepts TCP but
/// never answers freezes the single sweep task — and with it all
/// liveness eviction — and can hang shutdown indefinitely.
const OUTBOUND_IO_TIMEOUT: Duration = Duration::from_secs(10);
/// Staged node join known gap (issue #20): a ready node that never received `M` —
/// a transient connect/write/ack failure, not a rejection — never hands
/// off and never sweeps, stalling the join until `migration_timeout_for`
/// reaps it. Retrying the send absorbs exactly that class of hiccup,
/// mirroring `run_migration`'s own `KEY_TRANSFER_ATTEMPTS` on the node
/// side (same fixed-attempt-count, fresh-connection-each-time shape).
const MIGRATE_SEND_ATTEMPTS: u32 = 3;
const AUTH_SECRET_ENV_VAR: &str = "NANOCACHED_AUTH_SECRET";
/// Upper bound on a `name`/`joining_name` field's length, enforced at
/// parse time (`parse_string_fields`).
/// Both `nanocached-node` and `verify-staged-join` only ever generate a
/// v4 UUID (`Uuid::new_v4().to_string()`, 36 bytes — see `src/server.rs`,
/// Node identity decoupled from address) for a name, so 128 is far more headroom than any legitimate
/// value ever needs, while still bounding how much of `MAX_REQUEST_SIZE`
/// — and, more to the point, how much of every registry entry
/// (`NodeInfo`, keyed by name) and every `L`/`M` response listing it —
/// one field can consume.
const MAX_NAME_LENGTH: usize = 128;
/// Upper bound on a `token` field's length, same rationale and same
/// headroom over the 36-byte v4 UUID `nanocached-node` actually generates
/// (`NodeInfo::token`, issue #34) as `MAX_NAME_LENGTH`.
const MAX_TOKEN_LENGTH: usize = 128;

/// A registered node's place in the staged node join join lifecycle: `Waiting`
/// (registered, asked to join, but another join is already in progress)
/// -> `Joining` (actively receiving its handoff) -> `Joined` (handoff
/// complete, included in `L` responses, now heartbeating normally). There
/// is no separate "start up already joining" state — every node begins at
/// `Waiting` when it first sends `J`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum NodeState {
    Waiting,
    Joining,
    Joined,
}

struct NodeInfo {
    /// How to open a connection to this node (a client's `G`/`S`/`D`, or,
    /// per [[0008]], a ready node's handoff). Carries no identity meaning
    /// — see node identity decoupled from address — the registry's key (this node's random per-process
    /// name) is what hashing and lookups use.
    address: String,
    state: NodeState,
    /// Only meaningful (and only refreshed) once `state` is `Joined` —
    /// `Waiting`/`Joining` nodes hold one long-lived connection open
    /// instead of heartbeating, and are dropped when that connection dies
    /// rather than by `sweep_expired`'s liveness check.
    last_heartbeat: Instant,
    /// Fired (via `notify_one`, not `notify_waiters` — this must survive
    /// being sent before the connection task starts waiting on it, e.g.
    /// a bootstrap join that's promoted synchronously) when this node
    /// should move to `Joined`; the connection task holding this node's
    /// `J` connection open waits on it instead of polling.
    promoted: Arc<Notify>,
    /// The replication factor this node last reported believing, via
    /// `H`'s `r` field (issue #30) — `None` until its first heartbeat
    /// that reports one (a node that hasn't sent an client-side replication handoff `M`
    /// yet has no belief to report, and so does not vote — see below).
    /// Unlike membership, replication factor is static operator config:
    /// the `L` handler refuses to serve `config.replication` only once a
    /// STRICT MAJORITY of `Joined` nodes with a non-`None` value here
    /// disagree with it (issue #30 amendment — a lone dissenter must not
    /// be able to deny `L` to the whole cluster by itself). Overwritten
    /// on every heartbeat, so a past mismatch is cleared the moment the
    /// node reports a matching value again; removed along with the rest
    /// of a departed node's entry.
    reported_replication: Option<usize>,
    /// The node's per-process membership token (issue #34): a random
    /// value generated alongside its name (node identity decoupled from address) and presented on
    /// every `J`/`P`/`H`/`C` naming it. Established by whichever
    /// registration this replica saw first for the name (`J`, or `P` for
    /// a name this replica didn't know — a standby, or an amnesiac
    /// restart; discovery HA replicas never talk to each other, so
    /// first-use is the only place trust can start) and required to
    /// match on everything after, so knowing a node's public name —
    /// `L` lists them — is not enough to re-point its address or spoof
    /// its liveness/handoff reports. `L` deliberately carries no tokens
    /// at all. `M` is a narrower exception (issue #295/build_migrate_message):
    /// it carries the RECIPIENT's own token (which that node already
    /// holds) and, as a deliberate, scoped grant, the JOINING node's own
    /// token — never any other node's. Otherwise never sent back out, or
    /// any node/client could impersonate any other. Compared via
    /// `constant_time_eq`.
    token: String,
    /// When this entry was created. Only meaningful (like `last_heartbeat`
    /// above, but for the opposite states) while `state` is `Waiting` —
    /// left untouched by a duplicate `J` reusing the same entry
    /// (`start_join`), so a retried join doesn't reset its own bound.
    /// Backs `waiting_timeout_for`.
    waiting_since: Instant,
    /// How many other `Waiting`/`Joining` nodes this one counted itself
    /// behind (itself included) when it registered — captured once, not
    /// recomputed as the queue drains; see `waiting_timeout_for` for why.
    /// Only meaningful while `state` is `Waiting`.
    queue_position: usize,
    /// The id (`next_connection_id`) of the connection currently recorded
    /// as owning this registration — i.e. whichever `J` most recently
    /// (re-)established or reused it. Only meaningful while `state` is
    /// `Waiting`/`Joining`, same as `waiting_since`/`queue_position` above
    /// (issue #3/#9: `on_node_connection_ended`, previously keyed only by
    /// name, couldn't tell a duplicate `J`'s now-superseded original
    /// connection dying — harmless, since the newer connection is still
    /// live and this node's join must proceed normally — from this node's
    /// only connection dying, which must remove the entry / abandon a join
    /// it owns. `start_join` overwrites this on every accepted `J` for the
    /// name, including a duplicate reusing an existing entry, so it always
    /// names the most recent connection). Defaults to `0`, which
    /// `next_connection_id` never hands out (it starts at 1) — the
    /// sentinel for an entry no live connection currently owns, e.g. one
    /// `P` (Announce) created directly, or a test constructs directly.
    owner_connection_id: u64,
    /// When this entry became a member: set at creation and again by
    /// `promote_to_joined`. A `Joined` node never leaves that state without
    /// leaving the registry. `announce_needs_rejoin` compares this with the
    /// moment another node was evicted, to tell a node that was a member
    /// then from one that registered after (issue #1311); whether the node
    /// was in contact across that moment is `contact_breaks`.
    joined_at: Instant,
    /// The gaps of `CONTACT_BREAK` or more between two consecutive contacts
    /// of this `Joined` node, as `(previous_last_heartbeat, heard_again_at)`,
    /// oldest first: at least one heartbeat did not arrive in between and the
    /// node was out of contact. A node cut off across another node's
    /// eviction saw no roster without it and took nothing over, so it does
    /// not count, in `announce_needs_rejoin`, as one the cluster moved on
    /// with (issue #1479); one whose gap began after the eviction was already
    /// served that roster, and a later gap does not undo it (issue #1523).
    /// Recorded by `note_contact` only (the stall guard moves
    /// `last_heartbeat` without touching it), pruned to `DEPARTED_RETENTION`
    /// and capped at `MAX_CONTACT_BREAKS`.
    contact_breaks: Vec<(Instant, Instant)>,
    /// When this node itself was last heard from: set at creation and at
    /// promotion, and by `note_contact` on every heartbeat or announce.
    /// `last_heartbeat` is the liveness clock and the stall guard moves it
    /// for every node after a pause of discovery, which says nothing about
    /// the node; this one the guard never touches. `announce_needs_rejoin`
    /// reads it to tell a member that has been silent since before an
    /// eviction (and may still be cut off, with no break recorded until it
    /// is heard from again) from one that is in contact (issue #1532).
    last_contact: Instant,
    /// Issue #1663: set on a `Waiting` entry that registered from the
    /// address of `Joined` members that were still being heard from. See
    /// `AddressHold`. `None` for every other entry.
    address_hold: Option<AddressHold>,
}

/// The collision a new name's `J` left undecided: it came from the address
/// of `Joined` members that had been heard from less than `CONTACT_BREAK`
/// ago, which is either a node restarted under a new name (its old name is
/// about to go silent) or another process on a live node's address (its old
/// name keeps heartbeating). `settle_address_holds` decides, from the
/// sweep: an old name heard from after `since` is alive and the hold is
/// dropped (the join proceeds as it did before #1616); one silent for
/// `CONTACT_BREAK` is replaced and the hold dropped; one that left the
/// registry meanwhile is dropped from it. While a hold is set,
/// `try_begin_next_join` does not start this name's join. The hold ends no
/// later than `CONTACT_BREAK` after the old name's last heartbeat, which
/// was at most `since`.
struct AddressHold {
    since: Instant,
    old_names: Vec<String>,
}

impl NodeInfo {
    fn new(address: String, state: NodeState, token: String) -> Self {
        Self::with_queue_position(address, state, token, 1)
    }

    /// Like `new`, but records `queue_position` for `waiting_timeout_for`
    /// — used by `start_join` when registering a genuinely new `Waiting`
    /// entry, so its wait-timeout bound reflects how many other
    /// `Waiting`/`Joining` nodes were ahead of it at the moment it joined
    /// the queue.
    fn with_queue_position(
        address: String,
        state: NodeState,
        token: String,
        queue_position: usize,
    ) -> Self {
        Self {
            address,
            state,
            last_heartbeat: Instant::now(),
            promoted: Arc::new(Notify::new()),
            reported_replication: None,
            token,
            waiting_since: Instant::now(),
            queue_position,
            owner_connection_id: 0,
            joined_at: Instant::now(),
            contact_breaks: Vec::new(),
            last_contact: Instant::now(),
            address_hold: None,
        }
    }
}

/// Source of unique per-accepted-connection ids (issue #3/#9), consumed by
/// `NodeInfo::owner_connection_id` to tell which of possibly several
/// connections registered under the same name (a duplicate `J`, issue #7)
/// is the one currently live — see `on_node_connection_ended`. A single
/// global counter rather than something threaded through
/// `ConnectionConfig`/`ClusterState`: there's no state to share besides
/// monotonically increasing values, exactly one of which every accepted
/// connection needs for the life of the process. Starts at 1, not 0: `0`
/// is `NodeInfo::owner_connection_id`'s default/no-owner sentinel, so it
/// must never be handed out to a real connection.
static NEXT_CONNECTION_ID: AtomicU64 = AtomicU64::new(1);

fn next_connection_id() -> u64 {
    NEXT_CONNECTION_ID.fetch_add(1, Ordering::Relaxed)
}

/// Issue #421(c): identifies one `PendingJoin` incarnation, so a stale
/// result from an abandoned join can't be credited to a fresh
/// `PendingJoin` that happens to share the same `joining_name` (a node
/// whose join was abandoned and which immediately re-registers reuses its
/// own name). Mirrors `NEXT_CONNECTION_ID`: one global monotonic counter
/// rather than anything threaded through `CurrentJoin`, since every new
/// `PendingJoin` needs exactly one value, unique for the life of the
/// process.
static NEXT_JOIN_GENERATION: std::sync::OnceLock<AtomicU64> = std::sync::OnceLock::new();

/// Tenth-pass follow-up (2026-09-04): seeded from the wall clock (unix
/// microseconds, `| 1` so it is never 0) on first use rather than starting
/// at 1. A generation only proves anything against a stale `C` if it is
/// never reissued, and a process-local counter restarting at 1 would
/// reissue every generation a previous incarnation of this discovery
/// handed out — a `C` sent for a join that incarnation started could then
/// match a fresh `PendingJoin` of the same name by number as well as by
/// name. Still only monotonic within one process; HA replicas (which never
/// talk to each other) each seed independently, and the wall clock makes
/// two replicas' ranges overlap only if they start within the same
/// microsecond and run the same number of joins.
fn next_join_generation() -> u64 {
    NEXT_JOIN_GENERATION
        .get_or_init(|| {
            let micros = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|since_epoch| u64::try_from(since_epoch.as_micros()).unwrap_or(u64::MAX))
                .unwrap_or(1);
            AtomicU64::new(micros | 1)
        })
        .fetch_add(1, Ordering::Relaxed)
}

/// The node registry (`nodes`, keyed by name — node identity decoupled
/// from address's random per-process node identity, not address; see
/// `NodeInfo::address`) plus a `generation` counter (issue #95) bumped
/// on every change that would alter the heartbeat-ack roster — a node
/// joining/leaving, an address change, or a `reported_replication`
/// update. `build_roster_response` re-serializes the whole `Joined` roster,
/// and with #61 every `H` from every node carries it; caching the
/// serialized ack and rebuilding it only when `generation` moves turns
/// what was O(nodes²) CPU per liveness cycle (a full scan + re-serialize
/// per heartbeat) into O(nodes) work per actual membership change. Bump
/// via `bump_roster` while holding `nodes`, so the counter and map stay
/// consistent for the rebuild path.
/// One registered proxy (issue #122): `nanocached-proxy` announces via
/// `Y` and re-announces on its roster-refresh cadence; clients fetch the
/// set with `Q`. Entirely separate from membership — proxies are not in
/// any ring, never join, and never affect `L`/`H`.
struct ProxyInfo {
    /// Composed like a node's: announce connection's source IP + the
    /// declared port (addresses derived from the registration connection).
    address: String,
    /// Pins the name (issue #34's rationale): a re-announce with a
    /// different token is rejected, so another holder of the shared
    /// secret can't hijack a proxy's name and siphon its clients.
    token: String,
    last_seen: Instant,
}

struct RegistryState {
    nodes: Mutex<FxHashMap<String, NodeInfo>>,
    /// Issue #122 — keyed by proxy name, swept by `sweep_expired` on the
    /// same liveness timeout as heartbeats.
    proxies: Mutex<FxHashMap<String, ProxyInfo>>,
    /// Moves on every change to what `Q` lists (`bump_proxies`); what
    /// `proxy_list_cache` is keyed by. Separate from `generation`, which
    /// tracks node membership: a proxy announce must not rebuild the
    /// heartbeat ack, nor a join the proxy list.
    proxy_generation: AtomicU64,
    /// The `Q` response, rendered once per `proxy_generation` and shared
    /// (`Arc`) by every connection writing it. `Q` used to build its
    /// reply per request — every entry cloned into a `Vec`, then the
    /// whole text — and hold both copies for as long as the write was
    /// blocked, so a few hundred clients that asked and never read held
    /// a full registry's reply each (150 KB and the clones, about 0.4 MB
    /// a connection) and a 256 MiB discovery was killed by 700 of them.
    /// A blocked writer now holds one reference to one shared buffer.
    proxy_list_cache: Mutex<Option<ProxyListCache>>,
    /// Issue #124: joins completed (nodes promoted to `Joined`) and
    /// joins abandoned, for the metrics endpoint.
    joins_total: AtomicU64,
    joins_abandoned_total: AtomicU64,
    generation: AtomicU64,
    /// The heartbeat-ack roster, cached and rebuilt only when `generation`
    /// moves (issue #95). Co-located with the map and counter it derives
    /// from, so it travels wherever the registry does — no plumbing
    /// through `ConnectionConfig`/`handle_connection`.
    heartbeat_ack: Mutex<Option<CachedResponse>>,
    /// The `L` response, cached the same way and keyed off the same
    /// `generation` (issue #298): `L` renders exactly the same
    /// `roster_snapshot` (the `Joined` set, addresses, and
    /// `reported_replication` votes) that `cached_heartbeat_ack` already
    /// invalidates on every relevant mutation, so no separate bump is
    /// needed here — see `cached_list_response`.
    list_cache: Mutex<Option<CachedResponse>>,
    /// The `T` (NodeRoster) response, cached the same way (issue #356):
    /// `T` renders every entry's name/address/token regardless of state,
    /// a strictly broader view than `L`/`H`'s `Joined`-only roster, so
    /// `generation` is bumped on *every* mutation of that view (see
    /// `bump_roster`) — the Waiting/Joining-only mutations that never
    /// mattered to the other two caches included.
    roster_cache: Mutex<Option<CachedResponse>>,
    /// Issue #571: names that left (`V`), were evicted (`sweep_expired`)
    /// or whose join was abandoned, with when. What a heartbeat's
    /// pending-joiner question (`DiscoveryCommand::Heartbeat::
    /// pending_joiner`) is answered from: a name is reported gone only
    /// on this positive record, never on mere absence from `nodes` —
    /// which is empty after a restart until nodes re-announce, and on a
    /// standby never holds a Waiting joiner. A name registering again
    /// (`J` or `P`) is taken out, so an abandoned joiner retrying under
    /// its own name is not reported gone. Pruned by age
    /// (`DEPARTED_RETENTION`) on every insert and capped
    /// (`MAX_DEPARTED`).
    departed: Mutex<Departed>,
}

/// How long a departure stays answerable — comfortably past the longest
/// forwarding grace a sender can hold a slot open for, after which the
/// question is moot (issue #571).
const DEPARTED_RETENTION: Duration = Duration::from_secs(3600);
/// Upper bound on remembered departures; the oldest goes first.
const MAX_DEPARTED: usize = 4096;

/// `RegistryState::departed`'s storage (issue #1201). `at` answers lookups;
/// `order` holds the same records oldest first so that both the age prune
/// and the over-cap eviction work from the front in amortised O(1) — they
/// used to scan the whole map per call, 3.3 s for the 65,536 evictions of a
/// mass expiry, all of it on the single-threaded runtime. An `order` entry
/// is current only while `at` still holds that exact instant for the name;
/// a name forgotten or recorded again leaves a stale entry behind, skipped
/// when it reaches the front and swept out wholesale once `order` outgrows
/// twice the cap (so a repeating name cannot grow it without bound).
/// Recording happens under the lock with `Instant::now()`, so `order` is
/// sorted by time.
#[derive(Default)]
struct Departed {
    at: FxHashMap<String, Instant>,
    order: VecDeque<(Instant, String)>,
    /// The names in `at` whose eviction was judged by a sweep that had not
    /// run for a long while (`sweep_was_stalled`): discovery itself was
    /// not processing heartbeats, so the silence says nothing about the
    /// node. Such a node may re-register at once (`earns_rejoin`).
    unreliable: FxHashSet<String>,
}

impl Departed {
    fn is_current(&self, at: Instant, name: &str) -> bool {
        self.at.get(name) == Some(&at)
    }

    fn record(&mut self, name: &str, now: Instant) {
        // Age prune: only the front can be expired, since `order` is in
        // time order.
        while let Some((at, front)) = self.order.front() {
            if now.duration_since(*at) < DEPARTED_RETENTION {
                break;
            }
            if self.is_current(*at, front) {
                self.at.remove(front);
                self.unreliable.remove(front);
            }
            self.order.pop_front();
        }
        // Over the cap and not merely refreshing a known name: the oldest
        // current record goes (stale entries in front of it are dropped on
        // the way).
        if self.at.len() >= MAX_DEPARTED && !self.at.contains_key(name) {
            while let Some((at, front)) = self.order.pop_front() {
                if self.is_current(at, &front) {
                    self.at.remove(&front);
                    self.unreliable.remove(&front);
                    break;
                }
            }
        }
        self.unreliable.remove(name);
        self.at.insert(name.to_string(), now);
        self.order.push_back((now, name.to_string()));
        if self.order.len() > 2 * MAX_DEPARTED {
            let at = &self.at;
            self.order.retain(|(when, n)| at.get(n) == Some(when));
        }
    }

    /// `record` for an eviction decided by a stalled sweep.
    fn record_unreliable(&mut self, name: &str, now: Instant) {
        self.record(name, now);
        self.unreliable.insert(name.to_string());
    }

    fn forget(&mut self, name: &str) {
        self.at.remove(name);
        self.unreliable.remove(name);
    }

    /// Whether a node announcing itself under `name` has to start over: it
    /// departed within `DEPARTED_RETENTION`, and not on the word of a
    /// sweep that was itself not running.
    fn earns_rejoin(&self, name: &str) -> bool {
        !self.unreliable.contains(name)
            && self
                .at
                .get(name)
                .is_some_and(|at| at.elapsed() < DEPARTED_RETENTION)
    }

    fn departed_at(&self, name: &str) -> Option<Instant> {
        self.at.get(name).copied()
    }
}

impl Default for RegistryState {
    fn default() -> Self {
        RegistryState {
            nodes: Mutex::new(FxHashMap::default()),
            proxies: Mutex::new(FxHashMap::default()),
            proxy_generation: AtomicU64::new(0),
            proxy_list_cache: Mutex::new(None),
            joins_total: AtomicU64::new(0),
            joins_abandoned_total: AtomicU64::new(0),
            generation: AtomicU64::new(0),
            heartbeat_ack: Mutex::new(None),
            list_cache: Mutex::new(None),
            roster_cache: Mutex::new(None),
            departed: Mutex::new(Departed::default()),
        }
    }
}

type Registry = Arc<RegistryState>;

/// Records a change that affects the heartbeat-ack roster so the cached
/// serialization (`cached_heartbeat_ack`) is rebuilt on the next `H`.
/// Called at every registry mutation that could change the `Joined` set,
/// a node's address, or a `reported_replication` vote — generously (an
/// unnecessary bump only costs one extra rebuild; a *missing* one would
/// serve a stale roster, the #61 bug). `Relaxed` is enough: the value is
/// only ever compared for equality against a previously-read snapshot,
/// and each bump happens under the `nodes` lock the rebuild also takes.
///
/// Issue #356: `cached_node_roster` (`T`) also keys off this counter, and
/// its view is broader — every entry's name/address/token regardless of
/// state — so mutations invisible to `L`/`H` bump too: a `J` registering
/// or re-addressing a `Waiting` node, a `Waiting` eviction in
/// `sweep_expired`, an abandoned join's `Joining` removal, and
/// `on_node_connection_ended`'s owned-entry removal. Those bumps
/// invalidate the `L`/`H` caches for nothing, but that's the documented
/// "generous" tradeoff above; membership churn stays far rarer than the
/// requests the caches serve.
fn bump_roster(registry: &Registry) {
    registry.generation.fetch_add(1, Ordering::Relaxed);
}

/// Marks the proxy registry changed, so the next `Q` re-renders
/// `proxy_list_cache` (see that field). Called on every insert, address
/// change and removal of a `ProxyInfo` — a re-announce that changes
/// nothing `Q` lists (only `last_seen`) does not call it.
fn bump_proxies(registry: &Registry) {
    registry.proxy_generation.fetch_add(1, Ordering::Relaxed);
}

/// `RegistryState::proxy_list_cache`'s entry: the rendered `Q` reply and
/// the `proxy_generation` it was built at.
struct ProxyListCache {
    generation: u64,
    response: Arc<[u8]>,
}

/// The `Q` reply: the shared buffer for the current `proxy_generation`,
/// rendered now if the registry changed since the last render. Same
/// double-check shape as `cached_by_generation`, over the proxy map and
/// its own generation.
fn cached_proxy_list(registry: &Registry) -> Arc<[u8]> {
    let generation = registry.proxy_generation.load(Ordering::Relaxed);
    {
        let cached = registry
            .proxy_list_cache
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if let Some(current) = cached.as_ref()
            && current.generation == generation
        {
            return Arc::clone(&current.response);
        }
    }

    let (built_generation, response) = {
        let proxies = registry
            .proxies
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let generation = registry.proxy_generation.load(Ordering::Relaxed);
        (generation, render_proxy_list(&proxies))
    };
    let response: Arc<[u8]> = Arc::from(response.into_bytes());

    let mut cached = registry
        .proxy_list_cache
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    if cached
        .as_ref()
        .is_none_or(|current| current.generation <= built_generation)
    {
        *cached = Some(ProxyListCache {
            generation: built_generation,
            response: Arc::clone(&response),
        });
    }
    response
}

/// `Q`'s wire form: `N <count>\n` then `<name-len> <addr-len>\n<name><addr>\n`
/// per proxy.
fn render_proxy_list(proxies: &FxHashMap<String, ProxyInfo>) -> String {
    let mut response = format!("N {}\n", proxies.len());
    for (name, info) in proxies {
        let addr = &info.address;
        response.push_str(&format!("{} {}\n{name}{addr}\n", name.len(), addr.len()));
    }
    response
}

/// One cached response serialization and the `generation` it was built at
/// — the heartbeat ack (issue #95), the `L` response (#298) and the `T`
/// response (#356) each hold one in `RegistryState`, rebuilt by
/// `cached_by_generation` only when `generation` moves. Whatever
/// membership-gated decision the response folds in (the `H`/`L` refuse
/// cases) is baked into `response`; the `list_ready_at` startup grace is
/// time-gated, so it stays outside the cache and is handled by the caller.
/// `Arc<[u8]>` so a hit hands out the shared buffer without recopying it
/// per request.
struct CachedResponse {
    generation: u64,
    replication: usize,
    response: Arc<[u8]>,
}

/// Tracks the single in-progress join (staged node join: only one node moves
/// through `Waiting` -> `Joining` at a time). `expected` snapshots, at
/// join start, every ready node this join is waiting on and the token
/// (issue #34) it presented at that moment — name -> token, not a plain
/// `HashSet<String>`. This is a security fix, not a convenience: names
/// are public via `L`, and an unknown name is trust-on-first-use for
/// `J`/`P` (per-node membership tokens), so if `handle_complete` instead checked a
/// reporter's token against whatever the *live* registry entry holds, an
/// attacker could wait for a ready node to be evicted (see
/// `sweep_expired`'s own mid-join-eviction handling below, and the
/// liveness/waiting-timeout paths generally), re-register its now-free
/// name under a token of the attacker's choosing, and send a `C` crediting
/// a handoff that member never performed — forging join completion.
/// Checking against this snapshot instead means only the token that was
/// actually registered when the join began can ever complete it.
/// `completed` accumulates the names (node identity decoupled from address) of ready nodes that have
/// reported finishing their handoff via `C`; once it covers all of
/// `expected`, the joining node is promoted. `started_at` backs the
/// timeout that catches a ready node that's alive but never reports in
/// (see `abandon_current_join`, `migration_timeout_for`), sized from
/// `max_entries` — the largest entry count any ready node has reported
/// acknowledging its `M` so far (size-derived migration timeout), updated as each of
/// `try_begin_next_join`'s parallel sends resolves. Starts at 0 (the
/// bound is just `MIGRATION_TIMEOUT_BASE` until the first ack arrives).
/// `generation` (issue #421(c), `next_join_generation`) identifies this
/// specific incarnation: `joining_name` alone isn't enough, since an
/// abandoned join's node can immediately re-register and start a fresh
/// `PendingJoin` under the very same name, and a late-arriving M-send
/// result from the abandoned incarnation must not be credited to the new
/// one (see the check in `try_begin_next_join`'s completion loop).
struct PendingJoin {
    joining_name: String,
    expected: HashMap<String, String>,
    completed: HashSet<String>,
    started_at: Instant,
    max_entries: usize,
    generation: u64,
    /// The ready roster (name, address) this join's `M` carried, so a node
    /// admitted by `P` while the join is pending can be sent the same `M`
    /// (issue #1031, see `hand_off_from_late_member`). Shared with the
    /// join's own fan-out and with every late member's task instead of
    /// copied per recipient.
    roster: Arc<Vec<(String, String)>>,
    /// The `M` sends to nodes admitted by `P` after the join started.
    late: LateHandoffs,
}

/// Issue #1100: the `M` sends `hand_off_from_late_member` makes for one
/// join. They draw on the same dial limit as the join's own fan-out
/// (`fanout_limit`, `MAX_FANOUT_CONCURRENCY`) and are owned by the join:
/// dropping the `PendingJoin` (promotion, abandon, eviction) drops the
/// `JoinSet`, which aborts every send still running, so none outlives the
/// join it was for. The number of sends is capped at
/// `MAX_LATE_MEMBERS_PER_JOIN`.
struct LateHandoffs {
    fanout_limit: Arc<Semaphore>,
    sends: JoinSet<()>,
    /// Issue #1870: set by `handle_complete` just before it clears the
    /// slot and promotes the joiner, so a retried `M` send of this join
    /// (`send_migrate_with_retry`) can tell "this join completed" apart
    /// from "this join was abandoned or superseded" — both leave
    /// `current_join` without this joiner and generation. Each
    /// `PendingJoin` gets its own, so a re-join of the same name (a new
    /// generation) never sees an earlier join's flag, nor it this one's.
    promoted: Arc<AtomicBool>,
}

impl Default for LateHandoffs {
    fn default() -> Self {
        Self {
            fanout_limit: Arc::new(Semaphore::new(MAX_FANOUT_CONCURRENCY)),
            sends: JoinSet::new(),
            promoted: Arc::new(AtomicBool::new(false)),
        }
    }
}

/// What `send_migrate_with_retry` finds when it checks, before every
/// attempt, on the join its `M` is for (issue #1030, #1870).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum JoinStatus {
    /// Still the pending join: send (or retry) the `M`.
    Current,
    /// Completed: every ready member reported `C` and the joiner was
    /// promoted. Stop without an `X` — the handoff it would cancel has
    /// already finished.
    Promoted,
    /// Abandoned or superseded: stop, and cancel with `X` an `M` that
    /// may have landed.
    Ended,
}

/// The `send_migrate_with_retry` check for the join identified by
/// `joining_name` and `generation`, whose `LateHandoffs::promoted` is
/// `promoted` (issue #1870).
fn join_status(
    current_join: &CurrentJoin,
    joining_name: &str,
    generation: u64,
    promoted: &AtomicBool,
) -> JoinStatus {
    let is_current = lock_current_join(current_join)
        .as_ref()
        .is_some_and(|pending| {
            pending.joining_name == joining_name && pending.generation == generation
        });
    if is_current {
        JoinStatus::Current
    } else if promoted.load(Ordering::Acquire) {
        JoinStatus::Promoted
    } else {
        JoinStatus::Ended
    }
}

impl PendingJoin {
    fn identity(&self) -> JoinIdentity {
        JoinIdentity {
            joining_name: self.joining_name.clone(),
            generation: self.generation,
        }
    }
}

/// Which incarnation of a join a decision was made about (issue #1030):
/// its joining node's name and `PendingJoin::generation`. An abandon
/// decided for one join must not take a different join that came to
/// occupy the slot in the meantime, and a retried `M` must not go out
/// for a join that has since been abandoned.
#[derive(Clone, Debug, PartialEq, Eq)]
struct JoinIdentity {
    joining_name: String,
    generation: u64,
}

type CurrentJoin = Arc<Mutex<Option<PendingJoin>>>;

/// The node registry and staged node join join-orchestration state, bundled since
/// every connection needs both and they're always threaded through
/// together (keeps `dispatch_connection`'s argument count down).
#[derive(Clone)]
struct ClusterState {
    registry: Registry,
    current_join: CurrentJoin,
}

/// A connection this process accepted, plaintext or TLS-terminated.
type ServerStream = infra::MaybeTls<TcpStream, tokio_rustls::server::TlsStream<TcpStream>>;
/// A connection this process opened outbound (to a node, sending `M`/`X`),
/// plaintext or TLS-secured.
type ClientStream = infra::MaybeTls<TcpStream, tokio_rustls::client::TlsStream<TcpStream>>;

/// Connects to `addr` (a node, to send `M`/`X`), upgrading to TLS first if
/// `tls_connector` is set. There is no plaintext fallback: if TLS is
/// configured and the handshake fails, the connection attempt fails too.
async fn connect_client_stream(
    addr: &str,
    tls_connector: Option<&TlsConnector>,
) -> io::Result<ClientStream> {
    infra::connect_client_stream(
        addr,
        tls_connector,
        OUTBOUND_IO_TIMEOUT,
        TLS_HANDSHAKE_TIMEOUT,
    )
    .await
}

/// Per-connection settings that don't change once `run` starts, grouped so
/// `dispatch_connection`/`handle_connection` take one value instead of two.
#[derive(Clone)]
struct ConnectionConfig {
    idle_timeout: Duration,
    /// Discovery HA startup grace: until this instant, `L` answers `B\n`
    /// instead of a node list — after a restart the registry re-fills from
    /// announces within about one heartbeat interval, and serving the
    /// partial list to a bootstrapping client would hand it a wrong ring.
    list_ready_at: Instant,
    /// Client-side replication: the replication factor this process distributes (see
    /// `Args::replication_factor`).
    replication: usize,
    auth_secret: Option<Bytes>,
    /// When set, every accepted connection must complete a TLS handshake
    /// before speaking the protocol; there is no plaintext fallback.
    tls_acceptor: Option<TlsAcceptor>,
    /// When set, this process's own outbound connections to a node
    /// (sending `M`/`X`) upgrade to TLS; see `connect_client_stream`.
    tls_connector: Option<TlsConnector>,
    /// Guards new (not refreshing) registry insertions via `P` and `Y` — see
    /// `AnnounceLimiter`.
    announce_limiter: AnnounceLimiter,
}

/// Reads the shared auth secret from the environment rather than a CLI
/// flag, since CLI arguments are visible to anyone who can list processes
/// (e.g. `ps`) on the host. An unset or empty value means auth is not
/// required; a value that is not valid UTF-8 is a startup error
/// (`infra::read_env_secret`).
fn read_auth_secret() -> Result<Option<Bytes>, String> {
    infra::read_env_secret(AUTH_SECRET_ENV_VAR).map(|secret| secret.map(Bytes::from))
}

/// One source's token bucket in the announce limiter: `tokens` new
/// registrations may still be spent now; one token comes back per
/// `ANNOUNCE_INSERT_COOLDOWN` since `refilled_at`, up to
/// `ANNOUNCE_INSERT_BURST`. `last_seen` orders eviction when the limiter
/// is full.
struct AnnounceBucket {
    tokens: u32,
    refilled_at: Instant,
    last_seen: Instant,
}

/// Which registry a new registration goes into. Issue #1798: node (`P`)
/// and proxy (`Y`) registrations from one source used to spend one shared
/// bucket, so on a host (or NAT address, or IPv6 /64) that runs both, the
/// proxies re-registering after a discovery restart could use up the burst
/// the nodes need inside the startup grace, and the reverse. Each kind has
/// its own bucket now.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
enum AnnounceKind {
    Node,
    Proxy,
}

/// Per-source, per-kind token buckets guarding new registry insertions via
/// `P` and `Y` (see `ANNOUNCE_INSERT_BURST` and `ANNOUNCE_INSERT_COOLDOWN`,
/// and `AnnounceKind`). Bounded itself
/// (`MAX_ANNOUNCE_LIMITER_ENTRIES`, least recently seen entry evicted to
/// make room) so an attacker cycling through source addresses can't grow
/// the limiter's own memory without bound — the same shape of problem this
/// exists to solve for the registry, one level down.
type AnnounceLimiter = Arc<Mutex<FxHashMap<(std::net::IpAddr, AnnounceKind), AnnounceBucket>>>;

/// Whether a *new* registration of `kind` from `peer_ip` may be inserted
/// right now, spending one of that kind's tokens if so. A source (an IPv6 source is its
/// /64, see `per_ip_key`) starts with `ANNOUNCE_INSERT_BURST` tokens and
/// earns one back per `ANNOUNCE_INSERT_COOLDOWN`; with none left it is
/// refused until the next refill. Only meant to be called for a genuinely
/// new name (see the `P` handler) — a refresh of an existing entry must not
/// consume or be gated by this limiter at all.
fn announce_insert_allowed(
    limiter: &AnnounceLimiter,
    peer_ip: std::net::IpAddr,
    kind: AnnounceKind,
) -> bool {
    // Keyed like the join waiting cap and the connection cap: an IPv6
    // source counts as its /64, so rotating addresses inside one prefix
    // doesn't earn a fresh bucket each time.
    let source = (per_ip_key(peer_ip), kind);
    let now = Instant::now();
    let mut guard = limiter
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());

    if let Some(bucket) = guard.get_mut(&source) {
        // Whole refills only, so the remainder keeps accruing toward the
        // next one instead of being dropped on every call.
        let elapsed = now.saturating_duration_since(bucket.refilled_at);
        let refills = u32::try_from(elapsed.as_nanos() / ANNOUNCE_INSERT_COOLDOWN.as_nanos())
            .unwrap_or(u32::MAX);
        if refills > 0 {
            bucket.tokens = bucket
                .tokens
                .saturating_add(refills)
                .min(ANNOUNCE_INSERT_BURST);
            bucket.refilled_at = if bucket.tokens == ANNOUNCE_INSERT_BURST {
                now
            } else {
                bucket.refilled_at + ANNOUNCE_INSERT_COOLDOWN * refills
            };
        }
        bucket.last_seen = now;
        if bucket.tokens == 0 {
            return false;
        }
        bucket.tokens -= 1;
        return true;
    }

    if guard.len() >= MAX_ANNOUNCE_LIMITER_ENTRIES {
        // Evict the single least recently seen entry to make room. A
        // linear scan, but this only runs when the (small, bounded)
        // limiter is already full and a genuinely new source shows up —
        // not on the common path of a source that's already tracked.
        if let Some(oldest) = guard
            .iter()
            .min_by_key(|&(_, bucket)| bucket.last_seen)
            .map(|(source, _)| *source)
        {
            guard.remove(&oldest);
        }
    }

    guard.insert(
        source,
        AnnounceBucket {
            tokens: ANNOUNCE_INSERT_BURST - 1,
            refilled_at: now,
            last_seen: now,
        },
    );
    true
}

/// Applies an announce (`P`) for `name`/`addr`/`token` against an
/// already-registered entry, or reports `None` if `name` isn't currently
/// registered — the caller decides separately how to admit it as new. A
/// read-modify-write against this one entry, so the caller must hold
/// `registry`'s lock across the call (see the `Announce` handler's own
/// comment for why this is split out: admitting a genuinely new name also
/// needs `announce_insert_allowed`'s rate-limit decision, which must run
/// with the registry lock *not* held).
/// `Ok(true)` means the entry's address changed (the caller must bump the
/// roster generation, issue #95); `Ok(false)` a same-address refresh.
fn apply_announce_to_existing(
    guard: &mut FxHashMap<String, NodeInfo>,
    name: &str,
    addr: &str,
    token: &str,
    peer_ip: std::net::IpAddr,
) -> Option<Result<bool, &'static str>> {
    match guard.get_mut(name) {
        // Issue #34: an announce for a registered name is only the node
        // itself re-declaring if it can present the token its
        // registration established — checked before the mid-join check
        // below, and before the caller lets this connection claim `name`,
        // so a stranger's announce can neither re-point the node's
        // address nor, by getting itself rejected, have its teardown run
        // `on_node_connection_ended` against the real node's entry (which
        // would let anyone abort an in-progress join by announcing its
        // name).
        Some(info) if !constant_time_eq(info.token.as_bytes(), token.as_bytes()) => {
            log_rejection(format_args!(
                "WARN rejected announce for {name} from {peer_ip}: wrong token — either \
                 an impersonation attempt or a node reusing another's name"
            ));
            Some(Err(
                "announce with a token that does not match the registered one",
            ))
        }
        // A name mid-join announcing would corrupt the staged node join join
        // bookkeeping, and no correct node does it (announces only
        // happen after promotion).
        Some(info) if info.state != NodeState::Joined => {
            Some(Err("announce for a node that is mid-join"))
        }
        Some(info) => {
            let address_changed = info.address != addr;
            info.address = addr.to_string();
            note_contact(info, name, Instant::now());
            Some(Ok(address_changed))
        }
        None => None,
    }
}

/// A heartbeat or announce from `Joined` node `name` at `now`. Refreshes
/// `last_heartbeat` and `last_contact`. When the previous one is `CONTACT_BREAK` or more ago, at
/// least one heartbeat did not arrive and the node was out of contact in
/// between: the gap is recorded in `contact_breaks` (issues #1479, #1523).
/// `announce_needs_rejoin` then does not count the node as one the cluster
/// moved on with for a node evicted inside that gap. The stall guard
/// (`sweep_expired`) moves `last_heartbeat` without going through here,
/// since a pause of discovery is not a break on the node's side, and leaves
/// `last_contact` alone, since the node did not speak.
fn note_contact(info: &mut NodeInfo, name: &str, now: Instant) {
    let previous = info.last_heartbeat;
    let gap = now.duration_since(previous);
    if info.state == NodeState::Joined && gap >= CONTACT_BREAK {
        info.contact_breaks
            .retain(|(_, end)| now.duration_since(*end) < DEPARTED_RETENTION);
        if info.contact_breaks.len() >= MAX_CONTACT_BREAKS {
            info.contact_breaks.remove(0);
        }
        info.contact_breaks.push((previous, now));
        println!(
            "INFO node heard from again after {}s without a heartbeat: {name}",
            gap.as_secs()
        );
    }
    info.last_heartbeat = now;
    info.last_contact = now;
}

/// Whether `info` was out of contact across `at`: one of its contact breaks
/// began before it and ended at or after it, so the node saw no roster
/// without whatever departed then.
fn contact_broken_across(info: &NodeInfo, at: Instant) -> bool {
    info.contact_breaks
        .iter()
        .any(|(start, end)| *start < at && at <= *end)
}

/// Whether `info` has not been heard from since `departed_at` and is out of
/// contact now: its last contact is before the departure and `CONTACT_BREAK`
/// or more ago. The heartbeat that ends such a silence records a break that
/// spans the departure (`note_contact`), so the node never took part in the
/// cluster going on without the departed one, and until that heartbeat
/// arrives it must not count as one that did. A member that has spoken since
/// the departure, or whose silence is shorter than the break, is in contact
/// and is served the roster without the departed node at its next
/// heartbeat. Reads `last_contact`, not `last_heartbeat`: the stall guard
/// moves the latter for nodes that said nothing.
fn cut_off_since(info: &NodeInfo, departed_at: Instant, now: Instant) -> bool {
    info.last_contact < departed_at && now.duration_since(info.last_contact) >= CONTACT_BREAK
}

struct Args {
    host: String,
    port: u16,
    liveness_timeout: Duration,
    /// Discovery HA: how long after startup `L` keeps answering `B\n` while
    /// the registry re-fills from announces. `None` (the default) means
    /// "same as the liveness timeout".
    /// Client-side replication: the cluster's replication factor R — how many nodes hold
    /// each key. This process is R's single source of truth: clients learn
    /// it from `L`, nodes from `M`.
    replication_factor: usize,
    tls_cert: Option<String>,
    tls_key: Option<String>,
    tls_ca: Option<String>,
    /// Issue #124: port for /metrics + /healthz + /readyz on `host`;
    /// `None` = no operations endpoint.
    metrics_port: Option<u16>,
}

impl Default for Args {
    fn default() -> Self {
        Self {
            host: "127.0.0.1".to_string(),
            port: 8357,
            liveness_timeout: DEFAULT_LIVENESS_TIMEOUT,
            replication_factor: 2,
            tls_cert: None,
            tls_key: None,
            tls_ca: None,
            metrics_port: None,
        }
    }
}

/// Distinguishes `-h`/`--help` (a normal request, printed to stdout with
/// exit code 0) from an actual parsing error (printed to stderr with exit
/// code 1) — both previously took the same `Err` path, so `--help` looked
/// like a failure to shell scripts and CLI conventions alike.
#[derive(Debug)]
enum ArgsError {
    Help(String),
    Invalid(String),
}

impl From<String> for ArgsError {
    fn from(message: String) -> Self {
        ArgsError::Invalid(message)
    }
}

fn parse_args() -> Result<Args, ArgsError> {
    parse_args_from(std::env::args().skip(1))
}

fn parse_args_from(raw: impl Iterator<Item = String>) -> Result<Args, ArgsError> {
    let mut args = Args::default();
    let mut raw = raw;

    while let Some(flag) = raw.next() {
        let mut value = || raw.next().ok_or_else(|| format!("{flag} requires a value"));

        match flag.as_str() {
            "--host" => args.host = value()?,
            "--port" => {
                let raw_port = value()?;
                args.port = raw_port
                    .parse()
                    .map_err(|_| format!("invalid value for --port: {raw_port}"))?;
            }
            "--liveness-timeout" => {
                let raw_secs = value()?;
                let secs: u64 = raw_secs
                    .parse()
                    .map_err(|_| format!("invalid value for --liveness-timeout: {raw_secs}"))?;
                if !(MIN_LIVENESS_TIMEOUT_SECS..=MAX_LIVENESS_TIMEOUT_SECS).contains(&secs) {
                    return Err(ArgsError::Invalid(format!(
                        "--liveness-timeout must be between {MIN_LIVENESS_TIMEOUT_SECS} and \
                         {MAX_LIVENESS_TIMEOUT_SECS} seconds (a node heartbeats every \
                         {NODE_HEARTBEAT_INTERVAL_SECS} s and the window must hold at least \
                         {MIN_LIVENESS_HEARTBEATS} beats; {secs} would evict live nodes on a \
                         late heartbeat or overflow the clock)"
                    )));
                }
                args.liveness_timeout = Duration::from_secs(secs);
            }
            "--replication-factor" => {
                let raw = value()?;
                let factor: usize = raw
                    .parse()
                    .map_err(|_| format!("invalid value for --replication-factor: {raw}"))?;
                if factor == 0 {
                    return Err(ArgsError::Invalid(
                        "--replication-factor must be at least 1".to_string(),
                    ));
                }
                args.replication_factor = factor;
            }
            "--tls-cert" => args.tls_cert = Some(value()?),
            "--tls-key" => args.tls_key = Some(value()?),
            "--tls-ca" => args.tls_ca = Some(value()?),
            "--metrics-port" => {
                args.metrics_port = Some(value()?.parse().map_err(|_| {
                    "--metrics-port must be a number between 0 and 65535".to_string()
                })?);
            }
            "-h" | "--help" => return Err(ArgsError::Help(usage())),
            other => {
                return Err(ArgsError::Invalid(format!(
                    "unknown flag: {other}\n\n{}",
                    usage()
                )));
            }
        }
    }

    if args.tls_cert.is_some() != args.tls_key.is_some() {
        return Err(ArgsError::Invalid(
            "--tls-cert and --tls-key must be set together".to_string(),
        ));
    }

    Ok(args)
}

fn usage() -> String {
    "\
Usage: nanocached-discovery [options]

  --host <addr>                 bind address (default 127.0.0.1)
  --port <port>                 bind port (default 8357)
  --liveness-timeout <secs>     drop a node after this many seconds without
                                 a heartbeat (default 15; at least 15, three
                                 node heartbeats; at most 604800)
  --replication-factor <n>      how many nodes hold each key (client-side replication);
                                 distributed to clients via L and to nodes
                                 via M (default 2, min 1)
  --tls-cert <path>             PEM certificate chain; requires TLS on
                                 every accepted connection (no plaintext
                                 fallback)
  --tls-key <path>              PEM private key matching --tls-cert
  --metrics-port <port>         serve GET /metrics (Prometheus text format),
                                 /healthz and /readyz on this port at --host;
                                 omitted = no operations endpoint. Keep it
                                 internal: the endpoint is unauthenticated
  --tls-ca <path>               PEM CA certificate(s) to trust when this
                                 process connects out to a TLS-secured node
                                 to send M/X"
        .to_string()
}

#[derive(Debug, PartialEq, Eq)]
enum DiscoveryCommand {
    /// `tagging` (echoed response tags): the client sent `A <len> T\n`, asking for
    /// echoed response tags. Discovery never tags anything (its post-auth
    /// traffic is the one-shot `L`), but must accept and echo the flag,
    /// because a client doesn't know which kind of server it dialed until
    /// `A`'s reply.
    Auth {
        secret: Bytes,
        tagging: bool,
    },
    /// A refresh from an already-`Joined` node, identified by its name
    /// (node identity decoupled from address) — its address was already established by `Join` on this
    /// same connection. `replication` (issue #30) is the replication
    /// factor this node currently believes, or `None` if it doesn't know
    /// yet (the wire's `0` sentinel — see the module docs).
    Heartbeat {
        name: String,
        replication: Option<usize>,
        token: String,
        /// Issue #571: the joiner of a handoff this node completed but
        /// has not seen in any roster yet, if there is one — an optional
        /// trailing `<joiner-len>` header field and a trailing body
        /// field. The ack is then followed by one more line, `K\n` or
        /// `G\n` (`pending_joiner_verdict`): a joiner that was promoted
        /// and left again between two of this node's heartbeats is never
        /// listed in a roster it sees, and without this answer it would
        /// keep forwarding to it for the whole grace.
        pending_joiner: Option<String>,
    },
    List,
    /// Issue #122: a client asking for the registered proxies — `L`'s
    /// shape without the replication field.
    ListProxies,
    /// Issue #124: a decommissioning node leaving the cluster — sent
    /// after its drain-out handoff is done. Removed from the registry
    /// (and so from `L` and the heartbeat-ack roster) immediately;
    /// token-checked like every node-identifying command (#34);
    /// idempotent for an unknown name (a drain retry, or a replica that
    /// already expired it).
    NodeLeave {
        name: String,
        token: String,
    },
    /// Issue #124: a draining proxy deregistering itself — removed from
    /// `Q` immediately instead of lingering until the liveness timeout.
    /// Token must match the registration's (same hijack rationale as
    /// `ProxyAnnounce`); deregistering an unknown name is an idempotent
    /// no-op (a retry after a partial drain must not error).
    ProxyDeregister {
        name: String,
        token: String,
    },
    /// Issue #122: a `nanocached-proxy` (re-)announcing itself, same
    /// name/port/token frame as `Join`/`Announce`; the address is
    /// composed the same way. Refreshes `ProxyInfo::last_seen`.
    ProxyAnnounce {
        name: String,
        port: u16,
        token: String,
    },
    /// Staged node join: a node asking to join, identified by its name (node identity decoupled from address)
    /// and the port it serves on — the reachable address is composed from
    /// this connection's own source IP plus that port (addresses derived from the registration connection). `token`
    /// (issue #34) establishes the credential every later command naming
    /// this node must present — see `NodeInfo::token`. Sent once, on a
    /// connection the node then holds open to receive the `R\n`
    /// promotion push.
    Join {
        name: String,
        port: u16,
        token: String,
    },
    /// Staged node join: a ready node reporting it has finished handing off its
    /// share of a join, identified by its own name (node identity decoupled from address) and the
    /// joining node's name — so a stale report for an abandoned join can
    /// never be credited to the current one (issue #5). `token` must
    /// match the reporting node's registered one (issue #34).
    ///
    /// `generation` (tenth-pass audit, 2026-09-02, issue #421(c)/#34) is
    /// the sending node's own copy of the join generation its `M` (this
    /// report is for) carried — see `Command::Migrate::generation` on the
    /// node side. Closes the gap issue #5's `joining_name` check alone
    /// leaves: a joining node whose join is abandoned (see
    /// `abandon_current_join`) can immediately re-register under that
    /// very same name and have `try_begin_next_join` start a FRESH
    /// `PendingJoin` (a new generation) against the same ready roster —
    /// at which point a still-in-flight `C` for the abandoned incarnation
    /// names the right `joining_name` and presents a valid token (issue
    /// #5/#34 both still hold) but is really reporting a handoff to a
    /// process that, if the joiner crashed and restarted under the same
    /// name, no longer exists. `handle_complete` rejects it by comparing
    /// this against the in-progress `PendingJoin::generation`.
    ///
    /// `None` when the reporting node's own `M` carried no generation —
    /// an old discovery server (predating this fix) sent it, so the node
    /// has nothing to echo and falls back to the legacy three-field `C`.
    /// See `handle_complete`'s doc comment for what protection a legacy
    /// report still gets.
    Complete {
        name: String,
        joining_name: String,
        token: String,
        generation: Option<u64>,
    },
    /// Discovery HA: an already-promoted node (re-)declaring membership, with
    /// the same name/port/token shape as `Join` — upserted straight to
    /// `Joined`, no handoff orchestration. `token` must match a
    /// registered name's stored one (issue #34).
    Announce {
        name: String,
        port: u16,
        token: String,
        /// The trailing `G`: the sender is a node that clears its cache and
        /// joins again when told it was evicted (`G\n`, see the module
        /// docs' `P`). An announce without it is always accepted.
        rejoin_capable: bool,
    },
    /// Issue #295: a registered node fetching the roster *with* every
    /// member's own membership token — `name`/`token` self-identify the
    /// requester (checked the same way `Heartbeat`'s are, token-checked
    /// like every node-identifying command), proving it's a genuinely
    /// registered node and not merely a shared-secret-holding client,
    /// before discovery hands back anything it wouldn't put in the
    /// public `L` (see `NodeInfo::token`'s doc comment). Used by a
    /// decommissioning node to authorize its drain-out `U`/`u` frames,
    /// and by eviction-triggered re-replication — see
    /// `src/server.rs`'s `fetch_roster_once`.
    NodeRoster {
        name: String,
        token: String,
    },
}

#[derive(Debug, PartialEq, Eq)]
enum ParseError {
    InvalidCommand,
    InvalidLength,
    EmptyField,
    EmptySecret,
    InvalidUtf8,
    ControlCharacter,
    Incomplete,
}

/// Rejects control characters (`< 0x20` or `0x7F`) in a parsed name/token
/// field — those fields are logged verbatim (`node registered`, `node
/// left the cluster`, etc., issue #192), so a `\n` or other control byte
/// smuggled through would let a malicious peer forge extra log lines.
/// Checked once here rather than escaped at every print site.
fn contains_control_character(value: &str) -> bool {
    // `char::is_control` covers the Unicode Cc category, which already
    // includes both `< 0x20` and `0x7F` (plus the C1 range) — a superset
    // of what issue #192 asks to reject.
    value.chars().any(char::is_control)
}

/// Parses one request from the front of `input`, removing the consumed
/// bytes via `BytesMut::split_to`. On `Incomplete`, `input` is left
/// untouched.
fn parse(input: &mut BytesMut) -> Result<DiscoveryCommand, ParseError> {
    let header_end = find_lf(&input[..]).ok_or(ParseError::Incomplete)?;
    let header = &input[..header_end];

    let mut parts = header.split(|byte| *byte == b' ');
    let command = parts.next().ok_or(ParseError::InvalidCommand)?;

    match command {
        b"A" => {
            let secret_length = parts.next().ok_or(ParseError::InvalidLength)?;

            // Echoed response tags: an optional literal `T` requests tagged mode.
            // A trailing `R` (issue #125, retryable-error capability) is
            // accepted and ignored: discovery never emits `R`, but
            // rejecting the token would force every new SDK's `A ... T R`
            // probe into a reconnect-and-fallback round trip here.
            let tagging = match parts.next() {
                None => false,
                Some(b"T") => match parts.next() {
                    None => true,
                    Some(b"R") => true,
                    Some(_) => return Err(ParseError::InvalidCommand),
                },
                Some(b"R") => false,
                Some(_) => return Err(ParseError::InvalidCommand),
            };

            if parts.next().is_some() {
                return Err(ParseError::InvalidLength);
            }

            let secret_length = parse_length(secret_length)?;

            if secret_length == 0 {
                return Err(ParseError::EmptySecret);
            }

            let secret_start = header_end + 1;
            let secret_end = secret_start
                .checked_add(secret_length)
                .ok_or(ParseError::InvalidLength)?;

            if input.len() < secret_end {
                return Err(ParseError::Incomplete);
            }

            let frame = input.split_to(secret_end).freeze();
            let secret = frame.slice(secret_start..secret_end);

            Ok(DiscoveryCommand::Auth { secret, tagging })
        }

        b"L" => {
            if parts.next().is_some() {
                return Err(ParseError::InvalidLength);
            }

            let _ = input.split_to(header_end + 1);
            Ok(DiscoveryCommand::List)
        }

        b"V" => {
            let name_length = parts.next().ok_or(ParseError::InvalidLength)?;
            let token_length = parts.next().ok_or(ParseError::InvalidLength)?;

            if parts.next().is_some() {
                return Err(ParseError::InvalidLength);
            }

            let name_length = parse_length(name_length)?;
            let token_length = parse_length(token_length)?;
            let [name, token] = parse_string_fields(
                input,
                header_end,
                [
                    (name_length, MAX_NAME_LENGTH),
                    (token_length, MAX_TOKEN_LENGTH),
                ],
            )?;

            Ok(DiscoveryCommand::NodeLeave { name, token })
        }

        b"Z" => {
            let name_length = parts.next().ok_or(ParseError::InvalidLength)?;
            let token_length = parts.next().ok_or(ParseError::InvalidLength)?;

            if parts.next().is_some() {
                return Err(ParseError::InvalidLength);
            }

            let name_length = parse_length(name_length)?;
            let token_length = parse_length(token_length)?;
            let [name, token] = parse_string_fields(
                input,
                header_end,
                [
                    (name_length, MAX_NAME_LENGTH),
                    (token_length, MAX_TOKEN_LENGTH),
                ],
            )?;

            Ok(DiscoveryCommand::ProxyDeregister { name, token })
        }

        b"Q" => {
            if parts.next().is_some() {
                return Err(ParseError::InvalidLength);
            }

            let _ = input.split_to(header_end + 1);
            Ok(DiscoveryCommand::ListProxies)
        }

        b"T" => {
            let name_length = parts.next().ok_or(ParseError::InvalidLength)?;
            let token_length = parts.next().ok_or(ParseError::InvalidLength)?;

            if parts.next().is_some() {
                return Err(ParseError::InvalidLength);
            }

            let name_length = parse_length(name_length)?;
            let token_length = parse_length(token_length)?;
            let [name, token] = parse_string_fields(
                input,
                header_end,
                [
                    (name_length, MAX_NAME_LENGTH),
                    (token_length, MAX_TOKEN_LENGTH),
                ],
            )?;

            Ok(DiscoveryCommand::NodeRoster { name, token })
        }

        b"H" => {
            let name_length = parts.next().ok_or(ParseError::InvalidLength)?;
            let replication = parts.next().ok_or(ParseError::InvalidLength)?;
            let token_length = parts.next().ok_or(ParseError::InvalidLength)?;
            // Issue #571: optional trailing field, same backward-compat
            // idiom as `C`'s generation.
            let joiner_length = parts.next();

            if parts.next().is_some() {
                return Err(ParseError::InvalidLength);
            }

            let name_length = parse_length(name_length)?;
            // `0` is the wire sentinel for "no belief yet" (issue #30) —
            // not a real replication factor (nodes validate >= 1 the same
            // way this replica's own `--replication-factor` does).
            let replication = match parse_length(replication)? {
                0 => None,
                r => Some(r),
            };
            let token_length = parse_length(token_length)?;
            let (name, token, pending_joiner) = match joiner_length {
                Some(joiner_length) => {
                    let joiner_length = parse_length(joiner_length)?;
                    let [name, token, joiner] = parse_string_fields(
                        input,
                        header_end,
                        [
                            (name_length, MAX_NAME_LENGTH),
                            (token_length, MAX_TOKEN_LENGTH),
                            (joiner_length, MAX_NAME_LENGTH),
                        ],
                    )?;
                    (name, token, Some(joiner))
                }
                None => {
                    let [name, token] = parse_string_fields(
                        input,
                        header_end,
                        [
                            (name_length, MAX_NAME_LENGTH),
                            (token_length, MAX_TOKEN_LENGTH),
                        ],
                    )?;
                    (name, token, None)
                }
            };

            Ok(DiscoveryCommand::Heartbeat {
                name,
                replication,
                token,
                pending_joiner,
            })
        }

        b"C" => {
            let name_length = parts.next().ok_or(ParseError::InvalidLength)?;
            let joining_length = parts.next().ok_or(ParseError::InvalidLength)?;
            let token_length = parts.next().ok_or(ParseError::InvalidLength)?;
            // Tenth-pass audit (2026-09-02), issue #421(c): a trailing,
            // optional generation field — same backward-compat idiom as
            // `M`'s own trailing generation (see `DiscoveryCommand::
            // Complete::generation`'s doc comment). Absent for a `C` from
            // a node whose own `M` never carried one.
            let generation_field = parts.next();

            if parts.next().is_some() {
                return Err(ParseError::InvalidLength);
            }

            let name_length = parse_length(name_length)?;
            let joining_length = parse_length(joining_length)?;
            let token_length = parse_length(token_length)?;
            let generation = generation_field
                .map(parse_length)
                .transpose()?
                .map(|generation| generation as u64);
            let [name, joining_name, token] = parse_string_fields(
                input,
                header_end,
                [
                    (name_length, MAX_NAME_LENGTH),
                    (joining_length, MAX_NAME_LENGTH),
                    (token_length, MAX_TOKEN_LENGTH),
                ],
            )?;

            Ok(DiscoveryCommand::Complete {
                name,
                joining_name,
                token,
                generation,
            })
        }

        b"J" | b"P" | b"Y" => {
            let name_length = parts.next().ok_or(ParseError::InvalidLength)?;
            let port = parts.next().ok_or(ParseError::InvalidLength)?;
            let token_length = parts.next().ok_or(ParseError::InvalidLength)?;

            // `P` alone takes one more field, the `G` of a node that can
            // start over (`DiscoveryCommand::Announce::rejoin_capable`).
            let rejoin_capable = match parts.next() {
                None => false,
                Some(b"G") if command == b"P" => true,
                Some(_) => return Err(ParseError::InvalidLength),
            };
            if parts.next().is_some() {
                return Err(ParseError::InvalidLength);
            }

            let name_length = parse_length(name_length)?;
            // Addresses derived from the registration connection: the node declares only the port it serves on; the
            // reachable address is composed with this connection's source
            // IP. Port 0 can never be served on, so reject it.
            let port: u16 = std::str::from_utf8(port)
                .ok()
                .and_then(|raw| raw.parse().ok())
                .filter(|port| *port != 0)
                .ok_or(ParseError::InvalidLength)?;
            let token_length = parse_length(token_length)?;
            // Same owned-fn-pointer dance as `H`/`C` above: resolve the
            // variant while `command` is still alive, so `input` can be
            // reborrowed mutably below.
            let make: fn(String, u16, String, bool) -> DiscoveryCommand = match command {
                b"J" => |name, port, token, _| DiscoveryCommand::Join { name, port, token },
                b"Y" => {
                    |name, port, token, _| DiscoveryCommand::ProxyAnnounce { name, port, token }
                }
                _ => |name, port, token, rejoin_capable| DiscoveryCommand::Announce {
                    name,
                    port,
                    token,
                    rejoin_capable,
                },
            };
            let [name, token] = parse_string_fields(
                input,
                header_end,
                [
                    (name_length, MAX_NAME_LENGTH),
                    (token_length, MAX_TOKEN_LENGTH),
                ],
            )?;

            Ok(make(name, port, token, rejoin_capable))
        }

        _ => Err(ParseError::InvalidCommand),
    }
}

/// Parses `N` consecutive length-prefixed string fields (`H`/`J`/`P`'s
/// name then token, `C`'s name, joining name, then token — issue #34),
/// each given as `(length, max)`, checking all are fully buffered before
/// consuming any of `input`, so `parse`'s "untouched on `Incomplete`"
/// contract holds even though this reads across several fields in one
/// call. `max` is `MAX_NAME_LENGTH`/`MAX_TOKEN_LENGTH` — see those
/// constants' own doc comments for why.
fn parse_string_fields<const N: usize>(
    input: &mut BytesMut,
    header_end: usize,
    fields: [(usize, usize); N],
) -> Result<[String; N], ParseError> {
    if fields.iter().any(|(length, _)| *length == 0) {
        return Err(ParseError::EmptyField);
    }
    if fields.iter().any(|(length, max)| length > max) {
        return Err(ParseError::InvalidLength);
    }

    let mut ends = [0usize; N];
    let mut cursor = header_end + 1;
    for (end, (length, _)) in ends.iter_mut().zip(fields) {
        cursor = cursor
            .checked_add(length)
            .ok_or(ParseError::InvalidLength)?;
        *end = cursor;
    }

    if input.len() < cursor {
        return Err(ParseError::Incomplete);
    }

    let frame = input.split_to(cursor);
    let mut start = header_end + 1;
    let mut decoded: [String; N] = std::array::from_fn(|_| String::new());
    for (field, end) in decoded.iter_mut().zip(ends) {
        *field =
            String::from_utf8(frame[start..end].to_vec()).map_err(|_| ParseError::InvalidUtf8)?;
        start = end;
    }

    // issue #192: these fields end up in server logs verbatim (name via
    // `node registered`/`node left the cluster`/handoff completion, token
    // never logged today but held to the same bar) — reject control
    // characters here rather than escaping at every print site.
    if decoded
        .iter()
        .any(|field| contains_control_character(field))
    {
        return Err(ParseError::ControlCharacter);
    }

    Ok(decoded)
}

fn find_lf(input: &[u8]) -> Option<usize> {
    input.iter().position(|byte| *byte == b'\n')
}

fn parse_length(input: &[u8]) -> Result<usize, ParseError> {
    if input.is_empty() {
        return Err(ParseError::InvalidLength);
    }

    input.iter().try_fold(0usize, |length, byte| {
        if !byte.is_ascii_digit() {
            return Err(ParseError::InvalidLength);
        }

        length
            .checked_mul(10)
            .and_then(|length| length.checked_add((byte - b'0') as usize))
            .ok_or(ParseError::InvalidLength)
    })
}

/// Issue #124: minimal, dependency-free HTTP responder for Prometheus
/// text-format metrics and orchestrator probes — see the node's
/// `run_metrics_server` for the shared design notes. Unauthenticated;
/// keep the port internal. Issue #358: `metrics_permits` bounds this
/// listener's own concurrent connections (`METRICS_MAX_CONNECTIONS`) —
/// separate from `MAX_CONNECTIONS`, which this never touches.
async fn run_metrics_server(
    listener: TcpListener,
    registry: Registry,
    list_ready_at: Instant,
    metrics_permits: Arc<Semaphore>,
) {
    // Tracked in a `JoinSet` and reaped as it goes (same shape as the node's
    // `run_metrics_server`), instead of bare detached `tokio::spawn`s whose
    // panic in `serve_metrics_connection` would go unobserved. Bounded by
    // `metrics_permits` regardless, so this never grows past that many
    // outstanding at once; when the process shuts down the runtime drops
    // this task and the set aborts whatever is left.
    let mut connection_tasks: JoinSet<()> = JoinSet::new();

    loop {
        let accepted = tokio::select! {
            accepted = listener.accept() => accepted,

            result = connection_tasks.join_next(), if !connection_tasks.is_empty() => {
                if let Some(Err(error)) = result {
                    eprintln!("WARN metrics connection task failed: {error}");
                }
                continue;
            }
        };
        let stream = match accepted {
            Ok((stream, _)) => stream,
            Err(error) => {
                // Issue #184: mirrors `run`'s own accept loop above — an
                // unadorned `continue` here would busy-loop this task hot
                // under EMFILE/ENFILE instead of backing off, making
                // recovery harder right when file descriptors are already
                // scarce.
                if should_backoff_after_accept_error(&error) {
                    tokio::time::sleep(ACCEPT_ERROR_BACKOFF).await;
                }
                continue;
            }
        };

        // Issue #358: this listener has no client-listener-style busy
        // reply — cap it with its own small semaphore instead, dropping
        // the connection outright once it's exhausted rather than
        // spawning an unbounded number of handler tasks.
        let Ok(metrics_permit) = Arc::clone(&metrics_permits).try_acquire_owned() else {
            drop(stream);
            continue;
        };

        let registry = Arc::clone(&registry);
        connection_tasks.spawn(async move {
            let _metrics_permit = metrics_permit;
            let _ = tokio::time::timeout(
                Duration::from_secs(5),
                serve_metrics_connection(stream, registry, list_ready_at),
            )
            .await;
        });
    }
}

async fn serve_metrics_connection(
    mut stream: TcpStream,
    registry: Registry,
    list_ready_at: Instant,
) -> io::Result<()> {
    let path = read_http_request_path(&mut stream).await?;

    let (status, body): (&str, String) = match path.as_str() {
        "/metrics" => {
            let (joined, waiting, joining) = {
                let guard = lock(&registry);
                let mut joined = 0usize;
                let mut waiting = 0usize;
                let mut joining = 0usize;
                for info in guard.values() {
                    match info.state {
                        NodeState::Joined => joined += 1,
                        NodeState::Waiting => waiting += 1,
                        NodeState::Joining => joining += 1,
                    }
                }
                (joined, waiting, joining)
            };
            let proxies = registry
                .proxies
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .len();
            let joins = registry.joins_total.load(Ordering::Relaxed);
            let abandoned = registry.joins_abandoned_total.load(Ordering::Relaxed);
            let body = format!(
                "# HELP nanocached_discovery_members Joined cluster members.\n\
                 # TYPE nanocached_discovery_members gauge\n\
                 nanocached_discovery_members {joined}\n\
                 # HELP nanocached_discovery_waiting_nodes Nodes waiting to join.\n\
                 # TYPE nanocached_discovery_waiting_nodes gauge\n\
                 nanocached_discovery_waiting_nodes {waiting}\n\
                 # HELP nanocached_discovery_joining_nodes Nodes mid staged join.\n\
                 # TYPE nanocached_discovery_joining_nodes gauge\n\
                 nanocached_discovery_joining_nodes {joining}\n\
                 # HELP nanocached_discovery_proxies Registered proxies.\n\
                 # TYPE nanocached_discovery_proxies gauge\n\
                 nanocached_discovery_proxies {proxies}\n\
                 # HELP nanocached_discovery_joins_total Joins promoted to membership.\n\
                 # TYPE nanocached_discovery_joins_total counter\n\
                 nanocached_discovery_joins_total {joins}\n\
                 # HELP nanocached_discovery_joins_abandoned_total Joins abandoned.\n\
                 # TYPE nanocached_discovery_joins_abandoned_total counter\n\
                 nanocached_discovery_joins_abandoned_total {abandoned}\n"
            );
            ("200 OK", body)
        }
        "/healthz" => ("200 OK", "ok\n".to_string()),
        "/readyz" => {
            if Instant::now() >= list_ready_at {
                ("200 OK", "ok\n".to_string())
            } else {
                ("503 Service Unavailable", "startup grace\n".to_string())
            }
        }
        _ => ("404 Not Found", "not found\n".to_string()),
    };

    write_http_response(&mut stream, status, &body).await
}

/// Issue #571: remembers that `name` left the cluster, was evicted, or
/// had its join abandoned — see `RegistryState::departed`.
fn record_departed(registry: &Registry, name: &str) {
    registry
        .departed
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .record(name, Instant::now());
}

/// `record_departed` for a node evicted by a sweep that had not run for
/// a long while (`sweep_was_stalled`): the node is on record as gone for
/// the pending-joiner question, but is not sent back through a join when it
/// announces itself again.
fn record_departed_by_stalled_sweep(registry: &Registry, name: &str) {
    registry
        .departed
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .record_unreliable(name, Instant::now());
}

/// Whether a sweep tick at `now` comes so long after the previous one,
/// `last`, that discovery itself was not running in between: stopped,
/// suspended, or starved of CPU. The sweep ticks at most a second apart, so
/// a gap of half the liveness timeout is a stall, and every node's last
/// heartbeat is as old as the stall — the nodes did nothing wrong.
fn sweep_was_stalled(last: Instant, now: Instant, liveness_timeout: Duration) -> bool {
    now.duration_since(last) > liveness_timeout / 2
}

/// Issue #571: `name` registered (again) — it is no longer gone.
fn forget_departed(registry: &Registry, name: &str) {
    registry
        .departed
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .forget(name);
}

/// Issue #571: the answer to a heartbeat's pending-joiner question —
/// `G\n` (gone) only when `joiner` is on record as departed and has not
/// registered since, `K\n` (known, or simply not known to be gone)
/// otherwise. Takes the two locks one after the other, never nested.
fn pending_joiner_verdict(registry: &Registry, joiner: &str) -> &'static [u8] {
    let registered = lock(registry).contains_key(joiner);
    let departed = registry
        .departed
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .departed_at(joiner)
        .is_some_and(|at| at.elapsed() < DEPARTED_RETENTION);
    if departed && !registered {
        b"G\n"
    } else {
        b"K\n"
    }
}

/// Whether a node announcing itself (`P`) under `name` has to start over:
/// `name` is on record as departed within `DEPARTED_RETENTION`, is not
/// registered, and the cluster went on without it. The last needs a `Joined`
/// node that was already a member when `name` was evicted and was in contact
/// across it (issue #1311, `joined_at`, `contact_breaks`): only such a node
/// was served a roster without `name`, took over its keys and was written
/// to. When every node was evicted together, as when the path between
/// discovery and all of them was cut, nobody was left to move on, and
/// sending each back through a join would empty every cache for nothing;
/// each registers as it announces. The same holds for the nodes evicted
/// first when that path heals before the last of them is evicted: the ones
/// still listed were cut off just the same, a contact break spans the
/// eviction, so they do not make the evicted ones start over (issue #1479).
/// A break that began after the eviction does not count: that node had
/// already been served the roster without `name` (issue #1523). A break is
/// only recorded when the node is heard from again, so that this does not
/// depend on which of them reaches discovery first when the path heals, a
/// member not heard from since the eviction and silent for `CONTACT_BREAK`
/// or more already counts as cut off (`cut_off_since`, issue #1532). Takes
/// the two locks one after the other, never nested.
fn announce_needs_rejoin(registry: &Registry, name: &str) -> bool {
    announce_needs_rejoin_at(registry, name, Instant::now())
}

/// `announce_needs_rejoin` at `now`.
fn announce_needs_rejoin_at(registry: &Registry, name: &str, now: Instant) -> bool {
    if lock(registry).contains_key(name) {
        return false;
    }
    let departed_at = {
        let departed = registry
            .departed
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if !departed.earns_rejoin(name) {
            return false;
        }
        departed.departed_at(name)
    };
    let Some(departed_at) = departed_at else {
        return false;
    };
    lock(registry).values().any(|info| {
        info.state == NodeState::Joined
            && info.joined_at < departed_at
            && !contact_broken_across(info, departed_at)
            && !cut_off_since(info, departed_at, now)
    })
}

fn lock(registry: &Registry) -> std::sync::MutexGuard<'_, FxHashMap<String, NodeInfo>> {
    registry
        .nodes
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

fn lock_current_join(current_join: &CurrentJoin) -> std::sync::MutexGuard<'_, Option<PendingJoin>> {
    current_join
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

/// If no join is currently in progress, picks one `Waiting` node (if any)
/// and starts it toward `Joined`: promoted straight to `Joined` if there
/// are no `Joined` nodes yet to hand data off from (the bootstrap case —
/// nothing to receive), otherwise moved to `Joining` with a `PendingJoin`
/// tracking every currently-`Joined` node (by name, node identity decoupled from address) as one it
/// must receive a `C` from before promotion, and sent an `M` (concurrently,
/// one connection per ready node) telling it to start its handoff.
async fn try_begin_next_join(
    registry: &Registry,
    current_join: &CurrentJoin,
    auth_secret: &Option<Bytes>,
    tls_connector: &Option<TlsConnector>,
    replication: usize,
    joins_ready_at: Instant,
) {
    // Scoped to a block, not just an explicit `drop()`, so `join_guard`
    // (a std::sync::MutexGuard, not Send) is unambiguously out of scope
    // before the awaits below — required for this (and everything that
    // calls it) to remain a Send future, which `tokio::spawn` needs.
    // Issue #63: not during the startup grace (discovery HA). Right after
    // a restart the registry is still re-filling from `P` announces, and
    // a join orchestrated against that partial roster hands off from
    // only the members that have re-announced so far — every key the
    // joiner now owns that lives on a later-announcing member is
    // stranded there (1,706/20,000 observed at R=2; with an empty
    // registry the joiner is simply promoted with no handoff at all).
    // `L` is refused for exactly this reason; a `J` is accepted but held
    // in `Waiting` until the grace has passed, and `sweep_expired` calls
    // back here to start it then.
    if Instant::now() < joins_ready_at {
        return;
    }

    // A loop, not a single pass (issue #113): the bootstrap branch below
    // promotes a node with no handoff and therefore no `C` to chain the
    // next join off of — so when several nodes are parked in `Waiting`
    // (all registered during the startup grace, see issue #63, which
    // kicks this exactly once after the grace), a single pass promoted
    // the first and left every other one waiting forever. Go around
    // again after each bootstrap promotion; the next candidate then
    // finds a `Joined` member and starts a real staged join, after which
    // `C`/abandon chain the rest as usual.
    let (
        name,
        joining_addr,
        joining_token,
        joined,
        ready_tokens,
        generation,
        fanout_limit,
        promoted,
    ) = loop {
        let mut join_guard = lock_current_join(current_join);

        if join_guard.is_some() {
            return;
        }

        let (name, joining_addr, joining_token, joined, ready_tokens) = {
            let mut reg = lock(registry);

            // Strictly in arrival order: `waiting_timeout_for`'s bound
            // (and `queue_position`) assume a node is served once the
            // ones ahead of it are, which a plain `FxHashMap` walk does
            // not give — its (unseeded, deterministic) iteration order
            // would let whoever picks the right names keep cutting in.
            let next_waiting = reg
                .iter()
                .filter(|(_, info)| info.state == NodeState::Waiting)
                .min_by(|(name_a, a), (name_b, b)| {
                    a.waiting_since
                        .cmp(&b.waiting_since)
                        .then_with(|| name_a.cmp(name_b))
                })
                .map(|(name, info)| {
                    (
                        name.clone(),
                        info.address.clone(),
                        info.token.clone(),
                        info.address_hold.is_some(),
                    )
                });

            let Some((name, joining_addr, joining_token, held)) = next_waiting else {
                return;
            };
            // Issue #1663: the head of the queue is held until its address
            // collision is decided (`settle_address_holds`, from the sweep,
            // which also starts the queue again). The names behind it are
            // not let past it, so arrival order (`waiting_timeout_for`
            // assumes it) is kept; the wait is bounded by `CONTACT_BREAK`.
            if held {
                return;
            }

            let joined: Vec<(String, String)> = reg
                .iter()
                .filter(|(_, info)| info.state == NodeState::Joined)
                .map(|(name, info)| (name.clone(), info.address.clone()))
                .collect();

            // Each ready node's own token, so `M` can be sent with the
            // token that node will verify (issue #34) — the roster above
            // stays name+address only, since the wire `M` never carries
            // anyone else's token.
            let ready_tokens: HashMap<String, String> = reg
                .iter()
                .filter(|(_, info)| info.state == NodeState::Joined)
                .map(|(name, info)| (name.clone(), info.token.clone()))
                .collect();

            // Flip the state within the same lock acquisition used to find
            // and snapshot it, so a concurrent disconnect (which removes
            // the registry entry under this same lock, see
            // `on_node_connection_ended`) can't sneak in between "found"
            // and "marked Joining" and leave a ghost `PendingJoin` naming
            // a node nobody will ever hear from again — see the
            // `try_begin_next_join` liveness bug fix.
            match reg.get_mut(&name) {
                Some(info) => info.state = NodeState::Joining,
                None => return,
            }

            (name, joining_addr, joining_token, joined, ready_tokens)
        };

        if joined.is_empty() {
            drop(join_guard);
            promote_to_joined(registry, &name);
            continue;
        }

        // Issue #34 forged-completion fix (see `PendingJoin::expected`'s
        // own doc comment): snapshot each ready node's token as it stood
        // at join start. `ready_tokens` was already captured in this same
        // lock scope, keyed by the same names as `joined`, so this is
        // just reusing it rather than a second registry pass.
        let expected = ready_tokens.clone();
        let generation = next_join_generation();
        // One roster, shared with the join's late members (issue #1100)
        // and with every send of the fan-out below.
        let joined = Arc::new(joined);
        let late = LateHandoffs::default();
        let fanout_limit = Arc::clone(&late.fanout_limit);
        let promoted = Arc::clone(&late.promoted);

        *join_guard = Some(PendingJoin {
            joining_name: name.clone(),
            expected,
            completed: HashSet::new(),
            started_at: Instant::now(),
            max_entries: 0,
            roster: Arc::clone(&joined),
            late,
            generation,
        });

        break (
            name,
            joining_addr,
            joining_token,
            joined,
            ready_tokens,
            generation,
            fanout_limit,
            promoted,
        );
    };

    println!(
        "INFO join started: {name} (handoff from {} members)",
        joined.len()
    );

    let mut sends = JoinSet::new();
    // Issue #299: bounds how many of these `M` sends are dialing out at
    // once — see `MAX_FANOUT_CONCURRENCY`'s own doc comment. Every ready
    // node is still spawned and still contacted; a permit just gates when
    // each spawned task's own dial actually starts. Issue #1100: the
    // limit is the join's own, shared with its late members' sends.
    // Issue #409(a): share the invariant roster via one `Arc` instead of
    // deep-cloning the whole `Vec` on every iteration below. The roster is
    // identical for every ready node in this fanout, so a per-iteration
    // `joined.clone()` was an O(N^2) allocation running synchronously on
    // the current_thread runtime — for a large `joined` roster that stalls
    // heartbeats/accepts long enough to risk spurious liveness evictions.
    // Cloning the `Arc` instead is O(1) per iteration.
    let joined_roster = joined;

    for (ready_name, ready_addr) in joined_roster.iter().cloned() {
        let auth_secret = auth_secret.clone();
        let tls_connector = tls_connector.clone();
        let joining_name = name.clone();
        let joining_addr = joining_addr.clone();
        let joining_token = joining_token.clone();
        let joined_roster = Arc::clone(&joined_roster);
        // Every `Joined` node in `joined` was captured with its token in the
        // same lock scope, so this lookup is always present.
        let ready_token = ready_tokens.get(&ready_name).cloned().unwrap_or_default();
        let fanout_limit = Arc::clone(&fanout_limit);
        let current_join = Arc::clone(current_join);
        let promoted = Arc::clone(&promoted);

        sends.spawn(async move {
            let _permit = fanout_limit
                .acquire_owned()
                .await
                .expect("fanout_limit semaphore is never closed");
            let result = send_migrate_with_retry(
                &ready_token,
                &ready_name,
                &ready_addr,
                &auth_secret,
                &tls_connector,
                &joining_name,
                &joining_addr,
                &joining_token,
                &joined_roster,
                replication,
                generation,
                OUTBOUND_IO_TIMEOUT,
                || join_status(&current_join, &joining_name, generation, &promoted),
            )
            .await;

            (ready_name, result)
        });
    }

    while let Some(outcome) = sends.join_next().await {
        match outcome {
            Ok((ready_name, Err(error))) => {
                // Every individual attempt (see `send_migrate_with_retry`)
                // was already logged; this is the final, permanent
                // failure. Staged node join still doesn't define recovery beyond
                // this point (issue #20's second gap) — the join stalls
                // until discovery's own size-derived migration timeout
                // (size-derived migration timeout) reaps it.
                eprintln!(
                    "WARN permanently failed to send M to {ready_name} after \
                     {MIGRATE_SEND_ATTEMPTS} attempts: {error}"
                );
            }
            Ok((_, Ok(Some(entries)))) => {
                let mut join_guard = lock_current_join(current_join);
                credit_migrate_send_result(&mut join_guard, &name, generation, entries);
            }
            // Issue #1870: the join completed before a retry went out —
            // nothing failed.
            Ok((_, Ok(None))) => {}
            Err(error) => eprintln!("WARN a task sending M panicked: {error}"),
        }
    }
}

/// Applies one ready node's successful `M`-send result (its reported entry
/// count) to `pending.max_entries`, sizing the size-derived migration
/// timeout — but only if `pending` is still the exact `PendingJoin`
/// incarnation this result was sent for. `joining_name` alone isn't
/// enough: a report for a since-abandoned/replaced join (this slot has
/// already moved on to a different `joining_name`) must not be credited
/// here, mirroring `handle_complete`'s own guard against a stale-name
/// report. Issue #421(c) closes the narrower gap that check alone leaves:
/// if the node named `joining_name` was abandoned and immediately
/// re-registered under that very same name, a late-arriving result from
/// the abandoned (stale) incarnation must not be credited to the fresh
/// `PendingJoin` that now occupies the slot — so `generation` (captured by
/// the caller when its own incarnation started, from
/// `PendingJoin::generation`/`next_join_generation`) is checked too, not
/// just the name.
///
/// Correction (tenth-pass audit, 2026-09-02, issue #34/#421(c)): the PR
/// that introduced this function's own `generation` check (and the
/// commit that merged it) described the whole of issue #421 as closed by
/// this alone. That was wrong: `handle_complete` — the `C` handler that
/// actually promotes the joining node, not just this internal
/// send-result bookkeeping — had (and until this fix, still has no)
/// equivalent guard, checking only `joining_name` and the reporting
/// node's token (issue #34), never `PendingJoin::generation`. That gap
/// was independently re-discovered and is closed by `handle_complete`'s
/// own generation check, added alongside this comment's correction — see
/// its doc comment.
fn credit_migrate_send_result(
    join_guard: &mut Option<PendingJoin>,
    joining_name: &str,
    generation: u64,
    entries: usize,
) {
    if let Some(pending) = join_guard.as_mut()
        && pending.joining_name == joining_name
        && pending.generation == generation
    {
        pending.max_entries = pending.max_entries.max(entries);
    }
}

/// Bounded retry for `M`'s delivery (issue #20 / staged-join handoff design):
/// tries up to `MIGRATE_SEND_ATTEMPTS` times, logging each failed attempt,
/// with a fresh connection every time (`send_migrate` owns its own
/// connect) since a failed write/read leaves the previous connection's
/// state unknown. This only bounds the *send* — the handoff itself,
/// reported back asynchronously via `C`, stays a separate, unretried
/// concern (see `send_migrate`'s own docs). `status` says whether the
/// join this `M` is for is still the pending one; checked before every
/// attempt (issue #1030, see the loop). Returns `Ok(None)` if the join
/// completed before an attempt went out (issue #1870).
#[allow(clippy::too_many_arguments)]
async fn send_migrate_with_retry(
    token: &str,
    ready_name: &str,
    address: &str,
    auth_secret: &Option<Bytes>,
    tls_connector: &Option<TlsConnector>,
    joining_name: &str,
    joining_addr: &str,
    joining_token: &str,
    joined: &[(String, String)],
    replication: usize,
    generation: u64,
    io_timeout: Duration,
    status: impl Fn() -> JoinStatus,
) -> io::Result<Option<usize>> {
    let mut last_error = None;

    for attempt in 1..=MIGRATE_SEND_ATTEMPTS {
        // Issue #1030: an abandon (`abandon_current_join`, which sends `X`
        // to every ready member) can land between two attempts. A retry
        // after it would reach the node after its `X` — a no-op there,
        // since nothing was pending yet — and start a handoff to a joiner
        // discovery has already dropped; if that joiner re-registers, the
        // occupied slot then rejects the new join's `M`. So the send stops
        // here, and if an earlier attempt may have landed (its ack lost in
        // transit), an `X` of this send's own follows it.
        // Issue #1870: but a join that is no longer pending may also have
        // *completed* — this node's `C` (and every other member's) came
        // in while an attempt's ack was lost or still timing out, and the
        // joiner was promoted. An `X` then would make the node abandon a
        // finished handoff (end its forwarding window, revert its ring,
        // restore the handed-off copies) under a joiner discovery lists
        // as a member, so the send just stops.
        let join_status = status();
        if join_status == JoinStatus::Promoted {
            return Ok(None);
        }
        if join_status == JoinStatus::Ended {
            if attempt > 1
                && let Err(error) = send_cancel(
                    token,
                    address,
                    auth_secret,
                    tls_connector,
                    joining_name,
                    io_timeout,
                )
                .await
            {
                eprintln!(
                    "WARN failed to send X (cancel) to {ready_name} for the abandoned join of \
                     {joining_name}: {error}"
                );
            }
            return Err(io::Error::other(format!(
                "join of {joining_name} (generation {generation}) abandoned before attempt \
                 {attempt}/{MIGRATE_SEND_ATTEMPTS}"
            )));
        }
        match send_migrate(
            token,
            address,
            auth_secret,
            tls_connector,
            joining_name,
            joining_addr,
            joining_token,
            joined,
            replication,
            generation,
            io_timeout,
        )
        .await
        {
            Ok(entries) => return Ok(Some(entries)),
            Err(error) => {
                eprintln!(
                    "WARN failed to send M to {ready_name} (attempt \
                     {attempt}/{MIGRATE_SEND_ATTEMPTS}): {error}"
                );
                last_error = Some(error);
            }
        }
    }

    Err(last_error.expect("the loop above runs at least once"))
}

/// Builds the exact bytes `send_migrate` puts on the wire for `M`:
/// `M {joining_name_len} {joining_addr_len} {joining_token_len} {joined.len()}
/// {replication} {token_len} {generation}\n` followed by
/// `token` + `joining_name` + `joining_addr` + `joining_token`, then one
/// `{name_len} {addr_len}\n{name}{addr}` per entry in `joined` — exactly
/// the header/body layout `src/command.rs`'s `MigrateHeader`/`parse_migrate`
/// decode on the receiving node. Factored out of `send_migrate` so
/// `start_join`'s `NODE_MAX_REQUEST_SIZE` check can compute this message's
/// real length (`.len()` on the result) using the actual wire format,
/// rather than a hand-maintained estimate that could silently drift out
/// of sync with it.
///
/// Issue #441: until this fix, this builder predated issue #295's
/// `joining_token` field and never emitted it (nor the header slot for
/// it) at all — every `M` this process ever sent a real
/// `nanocached-node` was rejected with `ParseError::InvalidLength` and
/// the join stalled and was eventually reaped, so no staged join could
/// ever complete past the bootstrap (handoff-free) node. Confirmed live
/// against a real two-node cluster while investigating issue #421(c)
/// (this function is exactly where that fix's own `generation` field
/// also has to go). Fixed in the same change since both touch this exact
/// wire frame and neither can be meaningfully tested without the other.
fn build_migrate_message(
    token: &str,
    joining_name: &str,
    joining_addr: &str,
    joining_token: &str,
    joined: &[(String, String)],
    replication: usize,
    // Tenth-pass audit (2026-09-02), issue #421(c): this join's
    // `PendingJoin::generation`, echoed back unchanged on the
    // recipient's own `C` report — see `Command::Migrate::generation`'s
    // doc comment and `handle_complete`'s generation check. Always sent
    // by a discovery process new enough to run this code; there is no
    // "omit it" path on the send side (unlike the receive side's
    // backward-compat handling — see `Command::Migrate::generation`'s
    // doc comment for the rolling-upgrade rationale, which is entirely
    // about an old *node* or an old *discovery peer*'s M, never about
    // this process withholding it).
    generation: u64,
) -> Vec<u8> {
    // `token` is the *recipient* ready node's own membership token, echoed
    // so the node can prove this `M` came from a discovery server it
    // registered with (issue #34) — see `Command::Migrate::token` on the
    // node side. `joining_token` (issue #295) is the *joining* node's own
    // token — see `Command::Migrate::joining_token`'s doc comment for why
    // handing it to this one recipient is a deliberate, narrow exception
    // to per-node membership tokens never being sent back out. Body
    // layout: `<token><joining_name><joining_addr><joining_token><entries>`.
    let mut message = format!(
        "M {} {} {} {} {} {} {}\n",
        joining_name.len(),
        joining_addr.len(),
        joining_token.len(),
        joined.len(),
        replication,
        token.len(),
        generation,
    )
    .into_bytes();
    message.extend_from_slice(token.as_bytes());
    message.extend_from_slice(joining_name.as_bytes());
    message.extend_from_slice(joining_addr.as_bytes());
    message.extend_from_slice(joining_token.as_bytes());

    for (name, addr) in joined {
        message.extend_from_slice(format!("{} {}\n", name.len(), addr.len()).as_bytes());
        message.extend_from_slice(name.as_bytes());
        message.extend_from_slice(addr.as_bytes());
    }

    message
}

/// Connects to `address` (a `Joined` node) as a client, sends `M` with the
/// joining node's identity and the full `joined` roster (node identity decoupled from address names +
/// addresses), and waits for the `A <entries>\n` acknowledgment —
/// confirmation that `M` was received and parsed, not that the handoff it
/// kicks off (which happens asynchronously on the node's side) has
/// finished; that's reported separately, node-to-discovery, via `C`.
/// Returns the entry count the ack reports, purely for sizing this join's
/// migration timeout (size-derived migration timeout) — not otherwise used here.
#[allow(clippy::too_many_arguments)]
async fn send_migrate(
    token: &str,
    address: &str,
    auth_secret: &Option<Bytes>,
    tls_connector: &Option<TlsConnector>,
    joining_name: &str,
    joining_addr: &str,
    joining_token: &str,
    joined: &[(String, String)],
    replication: usize,
    generation: u64,
    io_timeout: Duration,
) -> io::Result<usize> {
    let mut stream = connect_client_stream(address, tls_connector.as_ref()).await?;

    if let Some(secret) = auth_secret {
        let mut auth = format!("A {}\n", secret.len()).into_bytes();
        auth.extend_from_slice(secret);
        infra::write_all_within(&mut stream, &auth, io_timeout).await?;

        let mut ack = [0u8; 3];
        read_exact_timed(&mut stream, &mut ack, io_timeout).await?;

        if &ack != b"On\n" {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "node rejected the auth secret",
            ));
        }
    }

    let message = build_migrate_message(
        token,
        joining_name,
        joining_addr,
        joining_token,
        joined,
        replication,
        generation,
    );

    infra::write_all_within(&mut stream, &message, io_timeout).await?;

    let line = read_ack_line(&mut stream, io_timeout).await?;
    let entries = line
        .strip_prefix("A ")
        .and_then(|rest| rest.parse::<usize>().ok())
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "node did not acknowledge M"))?;

    Ok(entries)
}

/// `read_exact` bounded by `io_timeout` — see `OUTBOUND_IO_TIMEOUT`.
async fn read_exact_timed(
    stream: &mut ClientStream,
    buf: &mut [u8],
    io_timeout: Duration,
) -> io::Result<()> {
    timeout(io_timeout, stream.read_exact(buf))
        .await
        .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "ack read timed out"))??;
    Ok(())
}

/// The longest line `read_ack_line` accepts, newline included. `M`'s ack is
/// `A <entries>\n`, a few bytes; anything near this is a desynced or
/// malicious peer.
const MAX_ACK_LINE_LENGTH: usize = 64;

/// Reads `M`'s `A <entries>\n` ack — the one ack on this connection whose
/// length varies with the reported entry count (every other is a fixed
/// number of bytes), so the line is read through the shared bounded reader:
/// at most `MAX_ACK_LINE_LENGTH` bytes, and one `io_timeout` for the whole
/// line, not one per read (a peer dripping the line a byte at a time must
/// not keep `sweep_expired`'s one in-flight join fan-out pinned). Nothing is
/// read after this line, so what the reader buffered past it is dropped.
async fn read_ack_line(stream: &mut ClientStream, io_timeout: Duration) -> io::Result<String> {
    let mut carry = BytesMut::new();
    let mut line = Vec::new();
    infra::read_line_bounded(
        &mut infra::CarryReader::new(stream, &mut carry),
        MAX_ACK_LINE_LENGTH,
        Instant::now() + io_timeout,
        &mut line,
    )
    .await?;
    line.pop(); // the newline
    String::from_utf8(line)
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "ack was not valid utf-8"))
}

/// Connects to `address` (a ready node) as a client and sends `X`, telling
/// it to abandon whatever handoff it has in flight for `joining_name` —
/// used by `abandon_current_join` to return every ready node to its
/// pre-migration state once a join is scrapped. Best-effort: the caller
/// moves on regardless of the outcome here (see its doc comment) — a node
/// that never receives this either wasn't actually working on this
/// handoff (safe no-op on its end) or will find out its `C` report goes
/// nowhere once `current_join` has already moved on.
async fn send_cancel(
    token: &str,
    address: &str,
    auth_secret: &Option<Bytes>,
    tls_connector: &Option<TlsConnector>,
    joining_name: &str,
    io_timeout: Duration,
) -> io::Result<()> {
    let mut stream = connect_client_stream(address, tls_connector.as_ref()).await?;

    if let Some(secret) = auth_secret {
        let mut auth = format!("A {}\n", secret.len()).into_bytes();
        auth.extend_from_slice(secret);
        infra::write_all_within(&mut stream, &auth, io_timeout).await?;

        let mut ack = [0u8; 3];
        read_exact_timed(&mut stream, &mut ack, io_timeout).await?;

        if &ack != b"On\n" {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "node rejected the auth secret",
            ));
        }
    }

    // `<token><joining_name>` — the recipient node's own token first, so it
    // can prove this `X` came from discovery (see `send_migrate`/issue #34).
    let mut message = format!("X {} {}\n", joining_name.len(), token.len()).into_bytes();
    message.extend_from_slice(token.as_bytes());
    message.extend_from_slice(joining_name.as_bytes());

    infra::write_all_within(&mut stream, &message, io_timeout).await?;

    let mut ack = [0u8; 2];
    read_exact_timed(&mut stream, &mut ack, io_timeout).await?;

    if &ack != b"A\n" {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "node did not acknowledge X",
        ));
    }

    Ok(())
}

/// Moves `name` to `Joined`, making it visible in future `L` responses,
/// and wakes its held-open connection so it can push the `R\n` promotion
/// notice.
fn promote_to_joined(registry: &Registry, name: &str) {
    let (promoted, members) = {
        let mut guard = lock(registry);
        let promoted = guard.get_mut(name).map(|info| {
            info.state = NodeState::Joined;
            info.last_heartbeat = Instant::now();
            info.joined_at = info.last_heartbeat;
            info.last_contact = info.last_heartbeat;
            Arc::clone(&info.promoted)
        });
        if promoted.is_some() {
            // A new `Joined` node changes the heartbeat-ack roster (issue
            // #95). Issue #279: bumped here, still under `guard`, matching
            // `bump_roster`'s documented invariant — not after the guard
            // below drops.
            bump_roster(registry);
        }
        let members = guard
            .values()
            .filter(|info| info.state == NodeState::Joined)
            .count();
        (promoted, members)
    };

    if let Some(promoted) = promoted {
        registry.joins_total.fetch_add(1, Ordering::Relaxed);
        println!("INFO join promoted: {name} (members now {members})");
        // Wake every currently-parked `J` connection (a duplicate `J`
        // under the same name shares this Notify — issue #7) AND store a
        // permit for a waiter that hasn't parked yet (the bootstrap case,
        // promoted synchronously before `wait_for_promotion` runs).
        promoted.notify_waiters();
        promoted.notify_one();
    }
}

/// Why `start_join` refused a registration.
#[derive(Debug)]
enum JoinRejection {
    /// A `J` for an already-`Joined` name is spurious (a correct node
    /// re-registers with `P`, never `J`) and must be rejected rather
    /// than parked: its `Notify` was already consumed by the original
    /// promotion, so `wait_for_promotion` would block on it forever,
    /// holding a `MAX_CONNECTIONS` permit until the process exits.
    /// Repeated, that exhausts the connection semaphore.
    AlreadyJoined,
    /// Admitting this node would make the `M` this join eventually sends
    /// exceed `NODE_MAX_REQUEST_SIZE` — the node on the other end would
    /// just reject it outright, stalling the join until discovery's own
    /// migration timeout reaps it. Rejected here instead, immediately
    /// and with a clear reason, rather than left to time out.
    MigrateMessageTooLarge { message_len: usize },
    /// A `J` for a name already registered (`Waiting`/`Joining`) under a
    /// different token (issue #34): not the same node re-sending its
    /// join, so it must not share that entry's `Notify` (or anything
    /// else) — rejected outright.
    TokenMismatch,
    /// This source address already has `MAX_WAITING_PER_SOURCE_IP`
    /// `Waiting`/`Joining` registrations outstanding (issue: unauthenticated
    /// `J` connection exhaustion — see `MAX_WAITING_PER_SOURCE_IP`).
    TooManyWaitingFromSource,
    /// `MAX_WAITING_TOTAL` `Waiting`/`Joining` registrations are already
    /// outstanding cluster-wide (issue: join-queue starvation — see
    /// `MAX_WAITING_TOTAL`).
    TooManyWaitingTotal,
}

/// The source a registered `address` (`{peer_ip}:{port}`) counts under for
/// `MAX_WAITING_PER_SOURCE_IP`: the same key the per-IP connection caps use
/// (`infra::per_ip_key`, an IPv6 address's /64 prefix), so one IPv6 /64
/// can't hold a distinct allowance per address and walk around the cap.
/// `rsplit_once` finds the *last* `:`, which is the port separator even for
/// an IPv6 address that itself contains colons. An address that is not an
/// IP literal is compared as the text it is.
fn waiting_source_key(address: &str) -> String {
    let host = address.rsplit_once(':').map_or(address, |(host, _)| host);
    match host.trim_matches(['[', ']']).parse::<IpAddr>() {
        Ok(ip) => per_ip_key(ip).to_string(),
        Err(_) => host.to_string(),
    }
}

/// Registers `name` as `Waiting` with `address` (a no-op if it's already
/// registered — this must not downgrade a node already past `Waiting`)
/// and attempts to start it toward `Joined` immediately. Returns the
/// `Notify` its connection should hold open and wait on for promotion, or
/// a `JoinRejection` if it didn't register this node at all. `connection_id`
/// (issue #3/#9, `next_connection_id`) is recorded as the registration's
/// current owner regardless of whether this call creates a fresh entry or
/// reuses an existing one (a duplicate `J`) — see
/// `NodeInfo::owner_connection_id`.
#[allow(clippy::too_many_arguments)]
async fn start_join(
    registry: &Registry,
    current_join: &CurrentJoin,
    auth_secret: &Option<Bytes>,
    tls_connector: &Option<TlsConnector>,
    replication: usize,
    joins_ready_at: Instant,
    name: &str,
    address: String,
    token: String,
    connection_id: u64,
) -> Result<Arc<Notify>, JoinRejection> {
    let (promoted, replaced) = {
        let mut guard = lock(registry);
        if guard
            .get(name)
            .is_some_and(|info| info.state == NodeState::Joined)
        {
            return Err(JoinRejection::AlreadyJoined);
        }

        // Issue #34: a duplicate `J` (issue #7) is only genuinely the
        // same node retrying if it presents the same token; anything
        // else must not be allowed to park on the existing entry's
        // `Notify` and receive its promotion push.
        if guard
            .get(name)
            .is_some_and(|info| !constant_time_eq(info.token.as_bytes(), token.as_bytes()))
        {
            return Err(JoinRejection::TokenMismatch);
        }

        // Issue: unauthenticated `J` connection exhaustion — with auth
        // unset, a `J` under a name this replica has never seen always
        // registers (there is no proof of identity to fail), so nothing
        // upstream of here stops one source from doing that up to
        // `MAX_CONNECTIONS` times with distinct fake names. Cap concurrent
        // `Waiting`/`Joining` registrations per source address instead.
        // Only a genuinely new name counts against this — a duplicate `J`
        // for a name already registered here (the token check above just
        // confirmed it's the same node) reuses its existing entry rather
        // than adding one, so it must not double-count against its own
        // cap. `address` is `{peer_ip}:{port}` (addresses derived from the registration connection), so the source
        // is recovered by trimming the port back off; `rsplit_once` finds
        // the *last* `:`, which lands on the port separator even for an
        // IPv6 address that itself contains colons.
        if !guard.contains_key(name) {
            // Issue: join-queue starvation — checked before the per-source
            // cap below so a source still under its own `
            // MAX_WAITING_PER_SOURCE_IP` allowance can't keep registering
            // once the cluster-wide queue is already as deep as
            // `MAX_WAITING_TOTAL` permits; see that constant's own doc
            // comment. Only a genuinely new name counts, same rationale as
            // the per-source cap just below.
            let waiting_total = guard
                .values()
                .filter(|info| info.state != NodeState::Joined)
                .count();
            if waiting_total >= MAX_WAITING_TOTAL {
                return Err(JoinRejection::TooManyWaitingTotal);
            }

            let source = waiting_source_key(&address);
            let waiting_from_source = guard
                .values()
                .filter(|info| {
                    info.state != NodeState::Joined && waiting_source_key(&info.address) == source
                })
                .count();
            if waiting_from_source >= MAX_WAITING_PER_SOURCE_IP {
                return Err(JoinRejection::TooManyWaitingFromSource);
            }
        }

        // Issue (M-message size vs. `nanocached-node`'s request cap):
        // checked against the CURRENT joined roster — a conservative,
        // good-enough estimate for a registration-time check, even
        // though other `Waiting` nodes ahead of this one in the queue
        // could grow the roster further by the time this join's own `M`
        // actually goes out (only ever making the real message bigger,
        // never smaller, so this can under- but never over-admit). See
        // `NODE_MAX_REQUEST_SIZE` and `build_migrate_message`.
        //
        // Issue #421(b): like the two waiting-queue caps above, only a
        // genuinely new name counts here. A duplicate `J` for a name
        // already registered (the token check above already confirmed
        // it's the same node) is a supported reconnect (issue #7/#9)
        // that just reuses the existing `Waiting` entry rather than
        // registering afresh — running this check against it risks
        // wrongly rejecting a legitimate reconnect with
        // `JoinRejection::MigrateMessageTooLarge` if the joined roster
        // has grown since the node's original registration, leaving the
        // stale entry (pointing at a dead `owner_connection_id`) parked
        // until `sweep_expired`'s `waiting_timeout_for` eventually reaps
        // it.
        if !guard.contains_key(name) {
            let joined_now: Vec<(String, String)> = guard
                .iter()
                .filter(|(_, info)| info.state == NodeState::Joined)
                .map(|(joined_name, info)| (joined_name.clone(), info.address.clone()))
                .collect();
            // The `M` recipients' tokens are all per-process UUIDs of the
            // same length as this joining node's own `token`, so it
            // stands in here for an accurate size estimate without
            // looking each recipient up. It also stands in for the real
            // `joining_token` field here exactly, not merely as a
            // same-length proxy: this join's actual `joining_token`,
            // once it starts, IS this very node's own registered token
            // (issue #295/#441). `generation` isn't allocated until the
            // join actually starts (`next_join_generation`), so
            // `u64::MAX` — the longest any `u64` can ever print — is
            // used as a conservative upper bound on its digit width,
            // preserving this check's "can under- but never over-admit"
            // guarantee (see the comment above).
            let message_len = build_migrate_message(
                &token,
                name,
                &address,
                &token,
                &joined_now,
                replication,
                u64::MAX,
            )
            .len();
            if message_len > NODE_MAX_REQUEST_SIZE {
                return Err(JoinRejection::MigrateMessageTooLarge { message_len });
            }
        }

        let newly_registered = !guard.contains_key(name);
        // Issue #1616: a new name registering from the address of a
        // `Joined` member is that node restarted (a process on the same
        // ip:port cannot be the old one), and its old name is as gone as if
        // it had missed its heartbeats: it is dropped now rather than a
        // liveness timeout later. Left in, the join's `M` to the old name
        // went to the new process, which refused it, and the join waited
        // for the eviction, was abandoned for the evicted member and ran
        // the handoff again.
        //
        // Issue #1663: only when the old name is plausibly gone. The
        // address is the connection's source IP plus the port the `J`
        // declares, which proves nothing about the old name: another
        // process on the same host advertising the same port would
        // otherwise remove a live member, whose next `P` is then answered
        // `G` (the departure is on record) and which drops its cache to
        // join again. "Gone" is `CONTACT_BREAK` without a heartbeat or
        // announce on the liveness clock (`last_heartbeat`, which the stall
        // guard moves forward after a pause of discovery, so a stall does
        // not make every member look gone): the age at which a missed
        // heartbeat is already recorded as a contact break. The registry
        // has no reliable owner connection for a `Joined` member
        // (`owner_connection_id` is only kept for `Waiting`/`Joining`
        // entries), so a closed connection is not a usable signal here.
        //
        // A member at that address that was heard from more recently is
        // undecided, not kept: a node that crashed and was restarted within
        // a few seconds looks exactly like that, and leaving its old name
        // in sent the join's `M` to the new process (#1616 again). The new
        // name is held instead (`AddressHold`): its join does not begin,
        // and `settle_address_holds`, from the sweep, replaces the old name
        // once it has been silent for `CONTACT_BREAK` or lets the join go
        // ahead as it would have without #1616 if the old name is heard
        // from again.
        let now = Instant::now();
        let (replaced, held): (Vec<String>, Vec<String>) = if newly_registered {
            let (mut gone, mut undecided) = (Vec::new(), Vec::new());
            for (old, info) in guard.iter() {
                if info.state == NodeState::Joined && info.address == address {
                    if now.duration_since(info.last_heartbeat) >= CONTACT_BREAK {
                        gone.push(old.clone());
                    } else {
                        undecided.push(old.clone());
                    }
                }
            }
            (gone, undecided)
        } else {
            (Vec::new(), Vec::new())
        };
        for old in &replaced {
            guard.remove(old);
            record_departed(registry, old);
            eprintln!(
                "WARN node replaced: {old} (the new node {name} registered at its address \
                 {address})"
            );
        }
        if !held.is_empty() {
            eprintln!(
                "WARN {name} registered at the address of {}, which was heard from within the \
                 last {}s: its join waits for that to be decided",
                held.join(", "),
                CONTACT_BREAK.as_secs()
            );
        }
        if newly_registered {
            log_registration(format_args!(
                "INFO node registered: {name} at {address} (waiting to join)"
            ));
        }
        // Captured once, at registration, for `waiting_timeout_for` — see
        // `NodeInfo::queue_position`'s doc comment for why this must not
        // be recomputed later as the queue drains.
        let queue_position = guard
            .values()
            .filter(|info| info.state != NodeState::Joined)
            .count()
            + 1;
        forget_departed(registry, name);
        let info = guard.entry(name.to_string()).or_insert_with(|| {
            NodeInfo::with_queue_position(
                address.clone(),
                NodeState::Waiting,
                token,
                queue_position,
            )
        });
        // A duplicate `J` from a node still waiting its turn carries its
        // current address (addresses derived from the registration connection: derived from this very connection),
        // which may differ from the first registration's — same as
        // `apply_announce_to_existing` does for `P`. Not once `Joining`:
        // the in-flight handoff was dispatched against the recorded one.
        let address_changed = info.state == NodeState::Waiting && info.address != address;
        if info.state == NodeState::Waiting {
            info.address = address;
        }
        // Issue #356: a `Waiting` entry appearing (or moving address) is
        // invisible to `L`/`H` but not to `T`'s every-state roster —
        // bumped here, still under `guard`, per `bump_roster`'s invariant.
        if newly_registered || address_changed || !replaced.is_empty() {
            bump_roster(registry);
        }
        // Issue #3/#9: unconditionally overwritten on every accepted `J`
        // for this name — including a duplicate reusing an existing
        // Waiting/Joining entry — so this always names the most recently
        // accepted connection as the owner, never a stale one. See
        // `NodeInfo::owner_connection_id`.
        info.owner_connection_id = connection_id;
        if !held.is_empty() {
            info.address_hold = Some(AddressHold {
                since: now,
                old_names: held,
            });
        }
        (Arc::clone(&info.promoted), replaced)
    };

    drop_replaced_from_current_join(registry, current_join, &replaced);

    try_begin_next_join(
        registry,
        current_join,
        auth_secret,
        tls_connector,
        replication,
        joins_ready_at,
    )
    .await;

    Ok(promoted)
}

/// The members `replaced` are no longer part of a join in flight: one that
/// has not reported `C` never will. `handle_complete` no longer takes a
/// report in their name, and the join is finished if it was the last one it
/// waited for.
fn drop_replaced_from_current_join(
    registry: &Registry,
    current_join: &CurrentJoin,
    replaced: &[String],
) {
    if replaced.is_empty() {
        return;
    }
    let finished = {
        let mut join_guard = lock_current_join(current_join);
        match join_guard.as_mut() {
            Some(pending) => {
                for old in replaced {
                    if !pending.completed.contains(old) {
                        pending.expected.remove(old);
                    }
                }
                if pending.completed.len() >= pending.expected.len() {
                    join_guard.take().map(|pending| pending.joining_name)
                } else {
                    None
                }
            }
            None => None,
        }
    };
    if let Some(joining_name) = finished {
        promote_to_joined(registry, &joining_name);
    }
}

/// What `settle_address_holds` found.
#[derive(Default)]
struct HoldOutcome {
    /// Old names removed because they had been silent for `CONTACT_BREAK`.
    replaced: Vec<String>,
    /// At least one hold ended (any way): the queue may be able to move.
    decided: bool,
    /// Some `Waiting` entry is still held.
    holds_remain: bool,
}

/// Decides the address collisions `start_join` left undecided (issue
/// #1663), for every held `Waiting` entry. Each old name is, in turn:
/// - gone from the registry (left, evicted, replaced by another): nothing
///   left to decide;
/// - heard from after the `J` (`last_contact`, which only the node itself
///   moves): alive, so the address is a duplicate and the hold is dropped,
///   leaving the join to go ahead as it did before the replacement existed;
/// - silent for `CONTACT_BREAK` on the liveness clock (`last_heartbeat`):
///   replaced, with the same effects as in `start_join` (removed, recorded
///   as departed, `T`'s roster bumped);
/// - otherwise still undecided.
///
/// Called from the sweep, after its stall guard has run: that guard is what
/// moves `last_heartbeat` forward after a pause of discovery, so deciding
/// anywhere else could read the age of a stall as silence of the node.
/// The caller removes `replaced` from the join in flight
/// (`drop_replaced_from_current_join`).
fn settle_address_holds(registry: &Registry, now: Instant) -> HoldOutcome {
    let mut outcome = HoldOutcome::default();
    let mut guard = lock(registry);
    let held: Vec<String> = guard
        .iter()
        .filter(|(_, info)| info.address_hold.is_some())
        .map(|(name, _)| name.clone())
        .collect();
    for name in held {
        let Some(hold) = guard
            .get_mut(&name)
            .and_then(|info| info.address_hold.take())
        else {
            continue;
        };
        let mut undecided = Vec::new();
        let mut to_replace = Vec::new();
        for old in hold.old_names {
            match guard.get(&old) {
                Some(old_info) if old_info.state == NodeState::Joined => {
                    if old_info.last_contact > hold.since {
                        println!(
                            "INFO {old} was heard from after {name} registered at its address: \
                             it is alive, not replaced"
                        );
                    } else if now.duration_since(old_info.last_heartbeat) >= CONTACT_BREAK {
                        to_replace.push(old);
                    } else {
                        undecided.push(old);
                    }
                }
                _ => {}
            }
        }
        for old in &to_replace {
            guard.remove(old);
            record_departed(registry, old);
            eprintln!(
                "WARN node replaced: {old} (the new node {name} registered at its address and \
                 {old} was then silent for {}s)",
                CONTACT_BREAK.as_secs()
            );
        }
        if !to_replace.is_empty() {
            bump_roster(registry);
        }
        outcome.replaced.extend(to_replace);
        if undecided.is_empty() {
            outcome.decided = true;
        } else {
            outcome.holds_remain = true;
            if let Some(info) = guard.get_mut(&name) {
                info.address_hold = Some(AddressHold {
                    since: hold.since,
                    old_names: undecided,
                });
            }
        }
    }
    outcome
}

/// Issue #1031: a node admitted as `Joined` by `P` while a join is
/// pending — one heartbeat-evicted before the join started (a partition
/// longer than the liveness window) that healed and re-announced — is not
/// in the join's `expected` roster, which was snapshotted at join start,
/// so it received no `M`: its copies of the keys the joiner now owns were
/// never handed off, and with replication 1 those keys missed on the new
/// owner. It is now added to `expected` and sent the join's own `M` (the
/// same roster and generation the other ready nodes received; the node
/// adds itself to the before-ring, as every recipient does), so the join
/// waits for its `C` like the others'. The send runs on its own task:
/// `P` is the heartbeat path and its `R` must not wait for a dial.
///
/// Issue #1100: that task is bounded like the join's own fan-out. It takes
/// a permit of the join's dial limit (`MAX_FANOUT_CONCURRENCY`) before it
/// dials, it shares the join's roster instead of copying it, and it is
/// owned by the `PendingJoin` (aborted when the join ends). At most
/// `MAX_LATE_MEMBERS_PER_JOIN` nodes are handed off per join; the `P`
/// handler refuses further new names while the join is full (see
/// `late_handoffs_exhausted`), so the limit reached here is a backstop
/// and a node it turns away is left out of `expected` — admitted, but
/// without an `M`, as before #1031.
#[allow(clippy::too_many_arguments)]
fn hand_off_from_late_member(
    registry: &Registry,
    current_join: &CurrentJoin,
    auth_secret: &Option<Bytes>,
    tls_connector: &Option<TlsConnector>,
    replication: usize,
    name: &str,
    addr: &str,
    token: &str,
) {
    let mut join_guard = lock_current_join(current_join);
    let Some(pending) = join_guard.as_mut() else {
        return;
    };
    if pending.joining_name == name || pending.expected.contains_key(name) {
        return;
    }
    if pending.late.sends.len() >= MAX_LATE_MEMBERS_PER_JOIN {
        eprintln!(
            "WARN {name} admitted by P while the join of {} is pending, but \
             {MAX_LATE_MEMBERS_PER_JOIN} late members were already handed off: not sending it M",
            pending.joining_name
        );
        return;
    }
    // Registry lock under the join lock: the order `try_begin_next_join`
    // takes them in.
    let Some((joining_addr, joining_token)) = lock(registry)
        .get(&pending.joining_name)
        .map(|info| (info.address.clone(), info.token.clone()))
    else {
        return;
    };
    pending.expected.insert(name.to_string(), token.to_string());
    let joining_name = pending.joining_name.clone();
    let generation = pending.generation;
    let roster = Arc::clone(&pending.roster);
    let fanout_limit = Arc::clone(&pending.late.fanout_limit);
    let promoted = Arc::clone(&pending.late.promoted);
    println!(
        "INFO join in progress: {joining_name} — sending M to {name}, admitted by P after the \
         join started"
    );

    let registry = Arc::clone(registry);
    let current_join = Arc::clone(current_join);
    let auth_secret = auth_secret.clone();
    let tls_connector = tls_connector.clone();
    let (name, addr, token) = (name.to_string(), addr.to_string(), token.to_string());
    pending.late.sends.spawn(async move {
        let _permit = fanout_limit
            .acquire_owned()
            .await
            .expect("fanout_limit semaphore is never closed");
        let result = send_migrate_with_retry(
            &token,
            &name,
            &addr,
            &auth_secret,
            &tls_connector,
            &joining_name,
            &joining_addr,
            &joining_token,
            &roster,
            replication,
            generation,
            OUTBOUND_IO_TIMEOUT,
            || join_status(&current_join, &joining_name, generation, &promoted),
        )
        .await;
        match result {
            Ok(Some(entries)) => {
                let mut join_guard = lock_current_join(&current_join);
                credit_migrate_send_result(&mut join_guard, &joining_name, generation, entries);
            }
            // Issue #1870: the join completed before a retry went out.
            Ok(None) => {}
            Err(error) => eprintln!(
                "WARN permanently failed to send M to {name} (admitted by P mid-join) after \
                 {MIGRATE_SEND_ATTEMPTS} attempts: {error}"
            ),
        }
        // Keep the registry alive for the duration like the other fan-outs.
        drop(registry);
    });
}

/// Whether the pending join (if any) has already handed off to
/// `MAX_LATE_MEMBERS_PER_JOIN` nodes admitted by `P`, so one more new name
/// must wait for the join to end (issue #1100). Checked by the `P` handler
/// before it admits a new name and, on the single-threaded runtime, with no
/// `.await` between that check and `hand_off_from_late_member`.
fn late_handoffs_exhausted(current_join: &CurrentJoin) -> bool {
    lock_current_join(current_join)
        .as_ref()
        .is_some_and(|pending| pending.late.sends.len() >= MAX_LATE_MEMBERS_PER_JOIN)
}

/// Records a ready node's completion report for the in-progress join. If
/// this was the last of `expected` to report in, promotes the joining
/// node and returns `true`; the caller then acks the report and starts
/// the next `Waiting` node (if any) with `try_begin_next_join`. Issue
/// #1029: that fan-out used to run in here, before the ack, so a ready
/// node that was slow to answer its `M` (up to three attempts of 10 s
/// each) held the `A` past the reporter's own 10 s wait for it; the
/// reporter then logged a false "reporting completion failed" WARN and
/// its join-flip re-replication waited for the retry.
///
/// Tenth-pass audit (2026-09-02), issue #421(c)/#34: `generation`, if the
/// reporter's own `M` carried one, must match the in-progress
/// `PendingJoin::generation` — checked in addition to (not instead of)
/// the existing `joining_name`/token checks below. Closes a gap those two
/// alone leave open: `abandon_current_join` can strand this exact
/// `joining_name` and, moments later, the SAME name re-registers (the
/// stranded node's control connection dropped, or it crashed and
/// restarted) and `try_begin_next_join` starts a brand-new `PendingJoin`
/// — a new generation — against the very same ready roster `{A, B}` this
/// abandoned join also used. A ready member's `C` for the OLD generation,
/// still in flight when this happens, then names the right
/// `joining_name` and presents a valid, still-registered token (issue #5
/// and #34 both still hold) — so without this check it gets credited to
/// the new incarnation, and once every ready member has reported (in any
/// mix of old- and new-generation `C`s), the joining node is promoted
/// having never actually received the new incarnation's own handoff from
/// whichever ready member's report was stale. See `credit_migrate_send_result`'s
/// doc comment for the closely related (and, until this fix, the only)
/// existing generation guard, and its own correction of the historical
/// claim that this gap was already closed.
///
/// A `None` `generation` (a legacy `C` — see `DiscoveryCommand::Complete::
/// generation`'s doc comment) skips this check entirely and is accepted
/// exactly as before this fix, name+token only. This means the
/// protection this fix adds is only complete once every node in the
/// cluster is running a version that both sends `generation` on `M`'s
/// `C` and understands the field on receipt — during a rolling upgrade a
/// legacy report from a not-yet-upgraded node (or one whose in-flight
/// handoff started under a not-yet-upgraded discovery peer) is still
/// possible and is not, by itself, evidence of a stale report.
#[allow(clippy::too_many_arguments)]
async fn handle_complete(
    registry: &Registry,
    current_join: &CurrentJoin,
    reporting_name: &str,
    for_joining_name: &str,
    token: &str,
    generation: Option<u64>,
) -> bool {
    let joining_name = {
        let mut join_guard = lock_current_join(current_join);

        let Some(pending) = join_guard.as_mut() else {
            return false;
        };

        // Issue #5: a report for an earlier, since-abandoned join must
        // not be credited to this one — the reporter has sent this join's
        // target nothing.
        if pending.joining_name != for_joining_name {
            return false;
        }

        // Issue #421(c): a report for the right `joining_name` but a
        // stale (superseded) join generation — see this function's own
        // doc comment for the abandon-then-immediately-re-register
        // scenario this closes. Only checked when the reporter's own `M`
        // carried a generation to echo; a legacy, generation-free report
        // (`None`) falls through to the existing name/token checks
        // unchanged.
        if let Some(reported_generation) = generation
            && pending.generation != reported_generation
        {
            log_rejection(format_args!(
                "WARN ignored handoff-complete report from {reporting_name} for \
                 {for_joining_name}: generation {reported_generation} does not match the \
                 in-progress join's generation {} — a stale report from a since-abandoned, \
                 superseded incarnation of this join",
                pending.generation
            ));
            return false;
        }

        // Issue #34 forged-completion fix: verify `token` against the
        // token `PendingJoin::expected` snapshotted for this name at join
        // start, NOT whatever the *live* registry entry for
        // `reporting_name` currently holds. Checking the live entry would
        // let an attacker wait for `sweep_expired` (or a connection drop)
        // to evict the real ready node, re-register its now-free name
        // under a token of the attacker's own choosing (names are public
        // via `L`; an unknown name is trust-on-first-use, per-node membership tokens), and
        // then send a `C` crediting a handoff that member never
        // performed. A reporter not in `expected` at all — never a ready
        // member of this join, or already evicted from it by the
        // mid-join-eviction handling in `sweep_expired` below — is
        // rejected the same way `contains_key` already would have.
        let Some(expected_token) = pending.expected.get(reporting_name) else {
            log_rejection(format_args!(
                "WARN ignored handoff-complete report from {reporting_name} for \
                 {for_joining_name}: not a ready member of this join"
            ));
            return false;
        };
        if !constant_time_eq(expected_token.as_bytes(), token.as_bytes()) {
            log_rejection(format_args!(
                "WARN ignored handoff-complete report from {reporting_name} for \
                 {for_joining_name}: presented token does not match the one recorded \
                 when this join started — either a forged report or a stale one from a \
                 node that re-registered under this name since"
            ));
            return false;
        }

        pending.completed.insert(reporting_name.to_string());
        println!(
            "INFO handoff completed: {reporting_name} -> {for_joining_name} ({}/{})",
            pending.completed.len(),
            pending.expected.len()
        );

        if pending.completed.len() < pending.expected.len() {
            return false;
        }

        let joining_name = pending.joining_name.clone();
        // Issue #1870: marked before the slot clears, so a retried `M`
        // send of this join that finds it gone knows it completed and
        // sends no `X` (see `send_migrate_with_retry`).
        pending.late.promoted.store(true, Ordering::Release);
        *join_guard = None;
        joining_name
    };

    promote_to_joined(registry, &joining_name);
    true
}

/// Called whenever any node's connection to discovery ends (cleanly or
/// not), by name and by that connection's own id (`next_connection_id`,
/// issue #3/#9). A Waiting/Joining node has no liveness signal besides
/// this one connection (see `NodeInfo::promoted`'s doc comment), so its
/// registry entry is removed outright; a `Joined` node keeps relying on
/// `sweep_expired`'s timeout instead, since an ordinary connection
/// hiccup — reconnecting for the next heartbeat, say — shouldn't evict it
/// (this fires once per connection, not once per heartbeat).
///
/// This is a no-op — no removal, no `abandon_current_join` — unless
/// `connection_id` matches `NodeInfo::owner_connection_id`, i.e. this was
/// the connection currently recorded as owning `name`'s registration
/// (issue #3/#9: a node re-dials with a duplicate `J` — a supported
/// scenario, issue #7/#9 — and `start_join` hands ownership to the new
/// connection immediately; when the OLDER, now-superseded connection
/// later notices it's dead and reports in here, that must not disturb a
/// registration — or an in-progress join — the newer, still-live
/// connection now owns. Keyed only by name, this used to abandon a
/// perfectly healthy in-progress join whenever that stale connection
/// finally got around to closing).
///
/// If `name` turns out to be the current join's own Waiting/Joining
/// node — it died mid-handoff, its only liveness signal gone — the whole
/// join is abandoned via `abandon_current_join`. A ready member's
/// heartbeat connection dying does *not* abandon the join on its own
/// (issue #10): `C` (handoff complete) is reported over its own
/// short-lived connection (`report_complete`), never the heartbeat one,
/// so a heartbeat hiccup says nothing about that ready member's actual
/// handoff progress — a live node reconnects within one heartbeat
/// interval regardless. A ready member that's truly gone (crashed, or
/// genuinely stuck) is instead caught by `sweep_expired`'s size-derived
/// `migration_timeout_for` — the same size-aware grace
/// Size-derived migration timeout introduced so a large, legitimate join doesn't get
/// reaped just for being large; abandoning here too would bypass that
/// grace entirely and reintroduce the flat-timeout failure mode the
/// size-derived design replaced.
#[allow(clippy::too_many_arguments)]
async fn on_node_connection_ended(
    registry: &Registry,
    current_join: &CurrentJoin,
    auth_secret: &Option<Bytes>,
    tls_connector: &Option<TlsConnector>,
    replication: usize,
    joins_ready_at: Instant,
    name: &str,
    connection_id: u64,
) {
    let (removed, owns_entry) = {
        let mut guard = lock(registry);
        let owns_entry = guard.get(name).is_some_and(|info| {
            info.state != NodeState::Joined && info.owner_connection_id == connection_id
        });

        let removed = if owns_entry { guard.remove(name) } else { None };
        // Issue #356: same as `abandon_current_join`'s removal — a
        // Waiting/Joining entry leaving the registry only matters to `T`,
        // whose cache keys off this generation.
        if removed.is_some() {
            bump_roster(registry);
        }
        (removed, owns_entry)
    };

    if let Some(info) = removed {
        // Issue #9: a duplicate `J` under the same name (sharing this
        // `NodeInfo`'s `Notify`, see `start_join`) may have a second
        // connection still parked in `wait_for_promotion` on this same
        // `Notify`. Removing the entry without waking it stranded that
        // connection forever — nothing else would ever tell it the entry
        // is gone. Wake it exactly like `abandon_current_join` already
        // does for its own removal, so `wait_for_promotion`'s re-check
        // observes `None` and errors the connection closed (the node's
        // heartbeat loop redials and re-`J`s).
        info.promoted.notify_waiters();
        info.promoted.notify_one();
    }

    // Only the current join's own Waiting/Joining node dying — and only
    // when this ending connection was actually its recorded owner (see
    // this function's own doc comment, issue #3/#9) — abandons it here.
    // A ready member's connection dying deliberately never does, for the
    // separate reason explained above.
    let join_of_this_node: Option<JoinIdentity> = if owns_entry {
        lock_current_join(current_join)
            .as_ref()
            .filter(|pending| pending.joining_name == name)
            .map(PendingJoin::identity)
    } else {
        None
    };

    if let Some(join) = join_of_this_node {
        abandon_current_join(
            registry,
            current_join,
            auth_secret,
            tls_connector,
            replication,
            joins_ready_at,
            &join,
            "joining node disconnected",
        )
        .await;
    }
}

/// Scraps the current join — if it is still `join`, the one the caller
/// decided to abandon (issue #1030) — returning every node it touched to
/// its pre-migration state: clears `current_join`, removes the joining
/// node from the registry (it was never in `L` to begin with, so this is
/// nothing more than dropping bookkeeping), and tells every still-
/// registered ready node in `expected` to roll back via `X` (see
/// `send_cancel`) — a ready node that already finished, or never gets the
/// message, is unaffected. Then lets the next `Waiting` node (if any)
/// start. A no-op if no join is in progress.
#[allow(clippy::too_many_arguments)]
async fn abandon_current_join(
    registry: &Registry,
    current_join: &CurrentJoin,
    auth_secret: &Option<Bytes>,
    tls_connector: &Option<TlsConnector>,
    replication: usize,
    joins_ready_at: Instant,
    join: &JoinIdentity,
    reason: &str,
) {
    // Issue #1030: only the join this abandon was decided for. The sweep
    // decides on one tick and runs the abandon on a spawned task; a `C`
    // or a disconnect in between can have completed or replaced that join,
    // and the join now in the slot has done nothing to deserve an `X`.
    let pending = {
        let mut guard = lock_current_join(current_join);
        match guard.as_ref() {
            Some(pending) if pending.identity() == *join => guard.take(),
            _ => None,
        }
    };
    let Some(mut pending) = pending else {
        println!(
            "INFO abandon of {} (generation {}) skipped: no longer the pending join \
             (reason={reason})",
            join.joining_name, join.generation
        );
        return;
    };
    // Issue #1100: the `M` sends to this join's late members stop here,
    // before the `X`s below, rather than at some later attempt.
    pending.late.sends.abort_all();
    registry
        .joins_abandoned_total
        .fetch_add(1, Ordering::Relaxed);

    eprintln!(
        "WARN join abandoned: {} (reason={reason})",
        pending.joining_name
    );

    // Issue #4: the joining node's connection is parked in
    // `wait_for_promotion` with the idle timeout deliberately disabled —
    // removing its entry without waking it would strand that connection
    // (and the node behind it, which waits on `R` forever instead of
    // re-joining). Wake it; the re-check in `wait_for_promotion` sees the
    // entry is gone and errors the connection closed, so the node's
    // heartbeat loop redials and re-`J`s.
    let stranded = {
        let mut guard = lock(registry);
        let stranded = guard.remove(&pending.joining_name);
        record_departed(registry, &pending.joining_name);
        // Issue #356: a `Joining` entry leaving the registry is invisible
        // to `L`/`H` but not to `T`'s every-state roster — bumped under
        // `guard`, per `bump_roster`'s invariant.
        if stranded.is_some() {
            bump_roster(registry);
        }
        stranded
    };
    if let Some(info) = stranded {
        info.promoted.notify_waiters();
        info.promoted.notify_one();
    }

    let ready_addrs: Vec<(String, String, String)> = {
        let guard = lock(registry);
        pending
            .expected
            .keys()
            .filter_map(|name| {
                guard
                    .get(name)
                    .map(|info| (name.clone(), info.address.clone(), info.token.clone()))
            })
            .collect()
    };

    let mut sends = JoinSet::new();
    // Issue #299: bounds how many of these `X` sends are dialing out at
    // once — see `MAX_FANOUT_CONCURRENCY`'s own doc comment. Every ready
    // member is still spawned and still contacted; a permit just gates
    // when each spawned task's own dial actually starts.
    let fanout_limit = Arc::new(Semaphore::new(MAX_FANOUT_CONCURRENCY));

    for (ready_name, ready_addr, ready_token) in ready_addrs {
        let auth_secret = auth_secret.clone();
        let tls_connector = tls_connector.clone();
        let joining_name = pending.joining_name.clone();
        let fanout_limit = Arc::clone(&fanout_limit);

        sends.spawn(async move {
            let _permit = fanout_limit
                .acquire_owned()
                .await
                .expect("fanout_limit semaphore is never closed");
            let result = send_cancel(
                &ready_token,
                &ready_addr,
                &auth_secret,
                &tls_connector,
                &joining_name,
                OUTBOUND_IO_TIMEOUT,
            )
            .await;
            (ready_name, result)
        });
    }

    while let Some(outcome) = sends.join_next().await {
        match outcome {
            Ok((ready_name, Err(error))) => {
                eprintln!("WARN failed to send X (cancel) to {ready_name}: {error}");
            }
            Ok((_, Ok(()))) => {}
            Err(error) => eprintln!("WARN a task sending X panicked: {error}"),
        }
    }

    try_begin_next_join(
        registry,
        current_join,
        auth_secret,
        tls_connector,
        replication,
        joins_ready_at,
    )
    .await;
}

/// Holds a Waiting/Joining node's connection open after it sends `J`,
/// since it has no other way to learn it's been promoted (see
/// `NodeInfo::promoted`). Waits for either the promotion notification or
/// the connection dying; any byte newly read off `stream` while waiting is
/// a protocol error — a well-behaved node sends nothing more until
/// promoted. Issue #297 (doc fix only): this only covers bytes this
/// function itself reads. Bytes the node sent in the very same read as its
/// `J` — already sitting, unconsumed, in the outer loop's `received`
/// buffer before `wait_for_promotion` is ever called — aren't examined
/// here at all; they're left in `received` and only looked at again once
/// promotion resumes the outer loop, where they're parsed as the start of
/// the node's next command like any other bytes would be. So a same-read
/// surplus isn't an immediate error — it's silently deferred, not rejected,
/// until after promotion. Deliberately does not apply the ordinary idle
/// timeout: a node may legitimately wait here far longer than
/// `IDLE_TIMEOUT` while another node's join is in progress. It isn't
/// unbounded, though — `sweep_expired` separately reaps a `Waiting` entry
/// (and wakes this wait, the same way an abandoned join already does) once
/// `waiting_timeout_for` has elapsed, so this can still return an error
/// with no bytes ever having arrived on `stream`.
async fn wait_for_promotion(
    stream: &mut ServerStream,
    registry: &Registry,
    name: &str,
    promoted: Arc<Notify>,
    mut shutdown_rx: watch::Receiver<bool>,
) -> io::Result<()> {
    let mut idle_byte = [0u8; 1];

    loop {
        tokio::select! {
            _ = promoted.notified() => {
                // A wake means the join resolved — but in which direction
                // is the registry's call: promotion (issue #7 wakes every
                // duplicate waiter, all of which must answer `R`) or an
                // abandoned join that removed the entry (issue #4). A
                // still-Waiting state would be a spurious wake; keep
                // waiting.
                let state = lock(registry).get(name).map(|info| info.state);
                match state {
                    Some(NodeState::Joined) => {
                        write_response(stream, b"R\n").await?;
                        return Ok(());
                    }
                    None => {
                        return Err(io::Error::other(
                            "join abandoned while waiting for promotion",
                        ));
                    }
                    Some(_) => continue,
                }
            }
            _ = shutdown_rx.changed() => {
                return Err(io::Error::other("shutting down"));
            }
            result = stream.read(&mut idle_byte) => {
                let bytes_read = result?;

                if bytes_read == 0 {
                    return Err(io::Error::new(
                        io::ErrorKind::UnexpectedEof,
                        "connection closed while waiting to join",
                    ));
                }

                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "unexpected data while waiting to join",
                ));
            }
        }
    }
}

#[allow(clippy::too_many_arguments)]
pub(crate) async fn run(
    address: &str,
    liveness_timeout: Duration,
    startup_grace: Duration,
    replication: usize,
    auth_secret: Option<Bytes>,
    tls_acceptor: Option<TlsAcceptor>,
    tls_connector: Option<TlsConnector>,
    metrics_address: Option<String>,
) -> io::Result<()> {
    let listener = TcpListener::bind(address).await?;
    run_on_listener(
        listener,
        liveness_timeout,
        startup_grace,
        replication,
        auth_secret,
        tls_acceptor,
        tls_connector,
        metrics_address,
    )
    .await
}

/// `run` on a listener the caller already holds, so a test that picked the
/// port ahead of time never gives it up before discovery binds it (`sim.rs`).
#[allow(clippy::too_many_arguments)]
pub(crate) async fn run_on_listener(
    listener: TcpListener,
    liveness_timeout: Duration,
    startup_grace: Duration,
    replication: usize,
    auth_secret: Option<Bytes>,
    tls_acceptor: Option<TlsAcceptor>,
    tls_connector: Option<TlsConnector>,
    metrics_address: Option<String>,
) -> io::Result<()> {
    nanocached::infra::warn_if_exposed_without_secret(
        "discovery",
        listener.local_addr()?,
        auth_secret.is_some(),
    );
    let cluster_state = ClusterState {
        registry: Arc::new(RegistryState::default()),
        current_join: Arc::new(Mutex::new(None)),
    };
    let connection_limit = Arc::new(Semaphore::new(MAX_CONNECTIONS));
    let per_ip_connections: PerIpConnections = Arc::new(Mutex::new(HashMap::new()));
    let mut connection_tasks = JoinSet::new();
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    // Issue: `P` (Announce) holds no connection open, so nothing else
    // bounds how fast a single source can grow the registry toward
    // `MAX_REGISTRY_SIZE` — see `AnnounceLimiter`. One instance, shared
    // by every connection via `ConnectionConfig`, for the life of the
    // process.
    let announce_limiter: AnnounceLimiter = Arc::new(Mutex::new(FxHashMap::default()));
    // Discovery HA startup grace: `L` is refused until then, and — issue
    // #63 — so is starting a join, see `try_begin_next_join`.
    let list_ready_at = Instant::now() + startup_grace;
    let connection_config = ConnectionConfig {
        idle_timeout: IDLE_TIMEOUT,
        list_ready_at,
        replication,
        auth_secret: auth_secret.clone(),
        tls_acceptor,
        tls_connector: tls_connector.clone(),
        announce_limiter: announce_limiter.clone(),
    };

    println!(
        "INFO startup grace: refusing list queries for {}s",
        startup_grace.as_secs()
    );

    // Issue #124: the operations sidecar — /metrics + /healthz + /readyz.
    // The HTTP framing and the listener's own connection cap are
    // `nanocached::infra` (shared with the node and proxy); what each
    // endpoint reports is this binary's own. /readyz answers 503 during
    // the startup grace, exactly the window where `L`/`Q` answer `B`.
    if let Some(metrics_address) = &metrics_address {
        let metrics_listener = TcpListener::bind(metrics_address.as_str()).await?;
        println!("INFO metrics endpoint listening on {metrics_address}");
        tokio::spawn(run_metrics_server(
            metrics_listener,
            Arc::clone(&cluster_state.registry),
            list_ready_at,
            Arc::new(Semaphore::new(METRICS_MAX_CONNECTIONS)),
        ));
    }

    // Issue #421(a): the sweep task is watched and respawned when it exits
    // (a panic anywhere in its own body would otherwise silently stop
    // membership maintenance while the process kept serving). The watching
    // and the back-off between respawns live in `supervise_sweep`.
    let sweep_task = tokio::spawn(supervise_sweep(shutdown_rx.clone(), {
        let registry = Arc::clone(&cluster_state.registry);
        let current_join = Arc::clone(&cluster_state.current_join);
        let shutdown_rx = shutdown_rx.clone();
        move || {
            sweep_expired(
                Arc::clone(&registry),
                Arc::clone(&current_join),
                auth_secret.clone(),
                tls_connector.clone(),
                replication,
                list_ready_at,
                liveness_timeout,
                shutdown_rx.clone(),
            )
        }
    }));

    let shutdown = shutdown_signal();
    tokio::pin!(shutdown);

    loop {
        tokio::select! {
            biased;

            result = &mut shutdown => {
                result?;
                println!("INFO shutdown signal received");
                shutdown_tx.send_replace(true);
                break;
            }

            result = connection_tasks.join_next(), if !connection_tasks.is_empty() => {
                if let Some(Err(error)) = result {
                    eprintln!("WARN connection task failed: {error}");
                }
            }

            result = listener.accept() => {
                // Issue: this used to be `result?`, tearing down the
                // whole discovery process on any accept() failure — most
                // of which (ECONNABORTED: the peer reset before the
                // handshake completed; EMFILE/ENFILE/ENOBUFS: transient
                // resource pressure) are recoverable and say nothing
                // about this listener's own health. Log and keep serving
                // instead; only a backoff (fd exhaustion specifically)
                // changes the loop's pace, never its continuation.
                let (stream, address) = match result {
                    Ok(pair) => pair,
                    Err(error) => {
                        log_connection(format_args!("WARN accept failed: {error}"));
                        if should_backoff_after_accept_error(&error) {
                            tokio::time::sleep(ACCEPT_ERROR_BACKOFF).await;
                        }
                        continue;
                    }
                };

                dispatch_connection(
                    stream,
                    address,
                    cluster_state.clone(),
                    Arc::clone(&connection_limit),
                    Arc::clone(&per_ip_connections),
                    connection_config.clone(),
                    shutdown_rx.clone(),
                    &mut connection_tasks,
                );
            }
        }
    }

    let connections_finished = timeout(SHUTDOWN_TIMEOUT, async {
        while let Some(result) = connection_tasks.join_next().await {
            if let Err(error) = result {
                eprintln!("WARN connection task failed: {error}");
            }
        }
    })
    .await;

    if connections_finished.is_err() {
        eprintln!("WARN shutdown timeout reached");
        connection_tasks.abort_all();

        while connection_tasks.join_next().await.is_some() {}
    }

    // Bounded like everything else on the shutdown path (issue #6): a
    // sweep pass wedged on an unresponsive node must not hold up process
    // exit.
    if timeout(SHUTDOWN_TIMEOUT, sweep_task).await.is_err() {
        eprintln!("WARN sweep task did not finish before the shutdown timeout");
    }

    Ok(())
}

/// First pause before the sweep task is respawned after it exited.
const SWEEP_RESPAWN_BACKOFF_BASE: Duration = Duration::from_secs(1);
/// The pause doubles on each consecutive failure up to this.
const SWEEP_RESPAWN_BACKOFF_MAX: Duration = Duration::from_secs(30);
/// A sweep task that stayed up this long counts as healthy again: the next
/// failure starts the back-off over instead of continuing it.
const SWEEP_RESPAWN_HEALTHY_AFTER: Duration = Duration::from_secs(60);
/// The respawn notices are few by construction (the back-off bounds them),
/// but they go through a limiter like every other line the runtime thread
/// writes, so a log sink that stalls cannot stall the supervisor either.
static SWEEP_RESPAWN_LOG: LogLimiter = LogLimiter::new(5, Duration::from_secs(60));

/// How long to wait before respawning a sweep task that keeps exiting.
#[derive(Default)]
struct SweepRespawnBackoff {
    consecutive_failures: u32,
}

impl SweepRespawnBackoff {
    /// The pause before the next respawn, given how long the task that just
    /// exited had been running: `BASE`, doubling per consecutive failure up
    /// to `MAX`, starting over after a run of `HEALTHY_AFTER` or more.
    fn next_delay(&mut self, ran_for: Duration) -> Duration {
        if ran_for >= SWEEP_RESPAWN_HEALTHY_AFTER {
            self.consecutive_failures = 0;
        }
        let delay = SWEEP_RESPAWN_BACKOFF_BASE
            .saturating_mul(1u32 << self.consecutive_failures.min(16))
            .min(SWEEP_RESPAWN_BACKOFF_MAX);
        self.consecutive_failures = self.consecutive_failures.saturating_add(1);
        delay
    }
}

/// Runs the sweep (`make` builds one run of it) until shutdown, starting it
/// again when it exits. Until shutdown it has no reason to: a panic is the
/// only way out, and a sweep that panics on every pass would otherwise be
/// respawned in a hot loop (its interval's first tick is immediate), each
/// pass writing a line. The respawns back off exponentially, and the lines
/// go through `SWEEP_RESPAWN_LOG`.
async fn supervise_sweep<F, Fut>(mut shutdown_rx: watch::Receiver<bool>, mut make: F)
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = ()> + Send + 'static,
{
    let mut backoff = SweepRespawnBackoff::default();
    loop {
        let started = Instant::now();
        let outcome = tokio::spawn(make()).await;
        // `sweep_expired` returns normally only for the shutdown signal.
        if *shutdown_rx.borrow() {
            return;
        }
        let delay = backoff.next_delay(started.elapsed());
        match outcome {
            Ok(()) => log_limited(
                &SWEEP_RESPAWN_LOG,
                "sweep task respawns",
                format_args!(
                    "WARN sweep task exited unexpectedly; respawning in {}s",
                    delay.as_secs()
                ),
            ),
            Err(error) => log_limited(
                &SWEEP_RESPAWN_LOG,
                "sweep task respawns",
                format_args!(
                    "WARN sweep task panicked: {error}; respawning in {}s",
                    delay.as_secs()
                ),
            ),
        }
        tokio::select! {
            _ = tokio::time::sleep(delay) => {}
            _ = shutdown_rx.wait_for(|stop| *stop) => return,
        }
    }
}

/// Issue #1285: the log lines a peer can cause at will (a connection that
/// ends in an error, a TLS handshake that fails, a rejected `T`, join,
/// announce, leave or deregister) are each written at most this many times
/// a second, as the node does for its own connection lines. One short
/// connection costs one such line, so without the bound a flood within the
/// per-IP caps wrote about 5,000 lines a second; and `eprintln!` is a
/// blocking write on this current-thread runtime, so a log sink that
/// stalled (a full pipe, a slow journald) stalled accepts and heartbeats
/// with it. Lines past the budget are counted and reported in one summary
/// line ahead of the next one written. Connections and rejections have
/// separate budgets so a flood of one never hides the other.
const PEER_LOG_LINES_PER_SECOND: u32 = 50;
static CONNECTION_LOG: LogLimiter =
    LogLimiter::new(PEER_LOG_LINES_PER_SECOND, Duration::from_secs(1));
static REJECTION_LOG: LogLimiter =
    LogLimiter::new(PEER_LOG_LINES_PER_SECOND, Duration::from_secs(1));
/// Issue #1319: the `INFO node registered` line a `J` writes is also one
/// per connection for a peer that connects, sends `J` and leaves in a loop
/// (1,057 lines in 5.9 s, with ten names), so it is bounded too. On its own
/// budget so that a flood of registrations never hides a refusal, and the
/// other way round; and on standard output, where the line has always been.
static REGISTRATION_LOG: LogLimiter =
    LogLimiter::new(PEER_LOG_LINES_PER_SECOND, Duration::from_secs(1));
/// The same for the two `INFO node announced` lines a `P` writes (one when
/// it re-registers a node, one when it is answered `G` for a node evicted
/// earlier): a peer that loops `P` wrote one per connection. Its own budget,
/// on standard output like the registration line.
static ANNOUNCE_LOG: LogLimiter =
    LogLimiter::new(PEER_LOG_LINES_PER_SECOND, Duration::from_secs(1));

/// One line about a connection (accept and TLS failures, a connection that
/// ended in an error), through `CONNECTION_LOG`.
fn log_connection(line: std::fmt::Arguments<'_>) {
    log_limited(&CONNECTION_LOG, "connection log lines", line);
}

/// One line about a request this replica refused, through `REJECTION_LOG`.
fn log_rejection(line: std::fmt::Arguments<'_>) {
    log_limited(&REJECTION_LOG, "rejections", line);
}

/// One `INFO node registered` line, through `REGISTRATION_LOG`.
fn log_registration(line: std::fmt::Arguments<'_>) {
    log_info_limited(&REGISTRATION_LOG, "node registrations", line);
}

/// One `INFO node announced ...` line (a `P` that re-registered a node, or
/// one answered `G` for a node evicted earlier), through `ANNOUNCE_LOG`.
fn log_announce(line: std::fmt::Arguments<'_>) {
    log_info_limited(&ANNOUNCE_LOG, "node announces", line);
}

/// Issue #1716: the `INFO proxy deregistered` line a `Z` writes, on its
/// own budget so that a loop of `Y`/`Z` never hides another line.
static PROXY_DEREGISTER_LOG: LogLimiter =
    LogLimiter::new(PEER_LOG_LINES_PER_SECOND, Duration::from_secs(1));

fn log_proxy_deregistered(line: std::fmt::Arguments<'_>) {
    log_info_limited(&PROXY_DEREGISTER_LOG, "proxy deregistrations", line);
}

/// The standard-output counterpart of `log_limited`.
fn log_info_limited(limiter: &LogLimiter, what: &str, line: std::fmt::Arguments<'_>) {
    let Some(suppressed) = limiter.admit() else {
        return;
    };
    if suppressed > 0 {
        println!("INFO {suppressed} more {what} (not logged individually)");
    }
    println!("{line}");
}

fn log_limited(limiter: &LogLimiter, what: &str, line: std::fmt::Arguments<'_>) {
    let Some(suppressed) = limiter.admit() else {
        return;
    };
    if suppressed > 0 {
        eprintln!("WARN {suppressed} more {what} (not logged individually)");
    }
    eprintln!("{line}");
}

#[allow(clippy::too_many_arguments)]
fn dispatch_connection(
    stream: TcpStream,
    address: SocketAddr,
    cluster_state: ClusterState,
    connection_limit: Arc<Semaphore>,
    per_ip_connections: PerIpConnections,
    config: ConnectionConfig,
    shutdown_rx: watch::Receiver<bool>,
    connection_tasks: &mut JoinSet<()>,
) {
    // Every request/response is small; without this, the kernel may delay
    // small writes waiting to coalesce with more data (Nagle's algorithm).
    let _ = stream.set_nodelay(true);

    // A joining node's liveness is this connection: `wait_for_promotion`
    // reads it until promotion, and the sweep never evicts a `Waiting` or
    // `Joining` entry. A joiner whose host dies without a FIN or RST
    // (power loss, hypervisor failure, a partition) would otherwise stay
    // registered, and if it is the oldest `Waiting` entry its join would
    // start, fail silently on every ready node and hold the whole join
    // queue until `migration_timeout_for` (60 s and up to 2 h). TCP
    // keepalive makes the kernel fail the read after about 25 s of
    // silence instead; see `infra::TCP_KEEPALIVE_IDLE`.
    if let Err(error) = infra::enable_tcp_keepalive(&stream) {
        log_connection(format_args!(
            "WARN enabling TCP keepalive for {address} failed: {error}"
        ));
    }

    // CRITICAL — everything below (the connection-limit check, the TLS
    // handshake, and the over-limit "Busy" reply) runs inside the spawned
    // task, never inline here in `run`'s accept loop. This used to await
    // the TLS handshake right here, before `connection_tasks.spawn`: with
    // `#[tokio::main(flavor = "current_thread")]` there is only ever one
    // OS thread driving every future, so a client that stalled its
    // ClientHello blocked `run`'s `select!` — freezing new-connection
    // accepts, shutdown detection, and connection-task reaping — for up
    // to `TLS_HANDSHAKE_TIMEOUT` (10s). Mirrors `src/server.rs`'s own
    // `dispatch_connection`; see its comments for more detail.
    connection_tasks.spawn(async move {
        // Checked before the (potentially expensive) TLS handshake below,
        // not after: gating on the connection limit only once a handshake
        // has already been paid for defeats its purpose as a resource-
        // exhaustion guard under overload, since an unbounded number of
        // handshakes could otherwise run concurrently while every one of
        // them is ultimately rejected anyway. Acquired *before* the
        // handshake so a peer can't spend handshake CPU/fds past
        // `MAX_CONNECTIONS` just by dialing and stalling — only a
        // permit-holding connection ever performs one.
        let permit = match connection_limit.try_acquire_owned() {
            Ok(permit) => permit,
            Err(_) => {
                reject_over_limit(
                    stream,
                    address,
                    &config.tls_acceptor,
                    TLS_HANDSHAKE_TIMEOUT,
                    b"B\n",
                    log_connection,
                )
                .await;
                return;
            }
        };

        // No per-source-IP connection limit: without this, a single
        // source could hold `MAX_CONNECTIONS` connections all by itself
        // and starve every other client/node, even though the global
        // semaphore above isn't literally exhausted until the very last
        // one. Reserved before the TLS handshake for the same reason as
        // the global permit — see `MAX_CONNECTIONS_PER_IP`. Distinct
        // from `MAX_WAITING_PER_SOURCE_IP` (see that constant), which
        // this does not replace.
        let per_ip_permit =
            match try_acquire_per_ip(&per_ip_connections, address.ip(), MAX_CONNECTIONS_PER_IP) {
                Some(permit) => permit,
                None => {
                    // Issue #1664: the global permit goes back before the
                    // busy reply, which on a plaintext listener is followed
                    // by up to `BUSY_REPLY_DRAIN_WINDOW` of reading from a
                    // peer that may not close. Held through that, a source
                    // at its per-IP cap could keep global slots by churning
                    // rejected connections it leaves open. The global-limit
                    // rejection above holds no permit either. Not reordered
                    // (per-IP first) because then a connection the global
                    // limit would turn away would take a per-IP slot first.
                    drop(permit);
                    reject_over_limit(
                        stream,
                        address,
                        &config.tls_acceptor,
                        TLS_HANDSHAKE_TIMEOUT,
                        b"B\n",
                        log_connection,
                    )
                    .await;
                    return;
                }
            };

        let stream: ServerStream = match &config.tls_acceptor {
            Some(acceptor) => match timeout(TLS_HANDSHAKE_TIMEOUT, acceptor.accept(stream)).await {
                Ok(Ok(tls_stream)) => MaybeTls::Tls(Box::new(tls_stream)),
                Ok(Err(error)) => {
                    log_connection(format_args!(
                        "WARN TLS handshake with {address} failed: {error}"
                    ));
                    return;
                }
                Err(_) => {
                    log_connection(format_args!("WARN TLS handshake with {address} timed out"));
                    return;
                }
            },
            None => MaybeTls::Plain(stream),
        };

        let _connection_permit = permit;
        let _per_ip_permit = per_ip_permit;

        // Written once this connection identifies itself via `J`/`P` (a
        // plain client connection never does, and stays `None`) — read
        // afterward, regardless of how `handle_connection` exits, so
        // `on_node_connection_ended` runs uniformly instead of needing a
        // cleanup call at every one of its internal return points. See
        // `handle_connection`'s own doc comment on this parameter for why
        // it carries a connection id alongside the name.
        let connection_name: Arc<std::sync::Mutex<Option<(String, u64)>>> =
            Arc::new(std::sync::Mutex::new(None));

        let result = handle_connection(
            stream,
            address.ip(),
            cluster_state.registry.clone(),
            cluster_state.current_join.clone(),
            config.clone(),
            shutdown_rx,
            Arc::clone(&connection_name),
        )
        .await;

        if let Err(error) = &result {
            // Issue #68: a peer closing without a TLS `close_notify` (how
            // every SDK and node ends a connection) is an error to rustls
            // but not to us — INFO, not a WARN per disconnect.
            if error.to_string().contains("close_notify") {
                log_connection(format_args!(
                    "INFO connection from {address} closed without TLS close_notify"
                ));
            } else {
                log_connection(format_args!(
                    "WARN connection error from {address}: {error}"
                ));
            }
        }

        let identity = connection_name
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone();

        if let Some((name, connection_id)) = identity {
            on_node_connection_ended(
                &cluster_state.registry,
                &cluster_state.current_join,
                &config.auth_secret,
                &config.tls_connector,
                config.replication,
                config.list_ready_at,
                &name,
                connection_id,
            )
            .await;
        }
    });
}

#[allow(clippy::too_many_arguments)]
/// One consistent read of the registry for `L` and the `H` ack: the
/// replication-factor vote tally (issue #30) and the served roster, taken
/// under a single lock acquisition so a concurrent heartbeat/leave can't
/// slip between "no majority disagrees on replication" and the roster
/// then served.
struct RosterSnapshot {
    agreeing: usize,
    dissenting: Vec<String>,
    /// Every currently-`Joined` node as `(name, address)`.
    nodes: Vec<(String, String)>,
}

impl RosterSnapshot {
    /// Issue #30 (amended): a STRICT majority of voting nodes disputing
    /// this replica's own `--replication-factor` is what makes `L` answer
    /// `B\n` and the `H` ack withhold its roster. A tie does not refuse.
    ///
    /// Issue #279: while discovery-HA replicas disagree on
    /// `--replication-factor` this way, `refuse()` is true and every `H`
    /// this replica answers carries no roster at all (`cached_heartbeat_ack`
    /// below folds this into a bare `A\n`) — so for as long as the
    /// disagreement persists, nodes heartbeating against this replica learn
    /// of NO membership change via heartbeat, eviction included. Since
    /// eviction-triggered re-replication (#264/#266/#267) depends on that
    /// channel to propagate promptly, a live `--replication-factor`
    /// disagreement silently widens the window a stale owner is
    /// under-replicated for on this replica — worth knowing when
    /// diagnosing a re-replication delay, not just a routing one.
    fn refuse(&self) -> bool {
        self.dissenting.len() > self.agreeing
    }
}

// Issue #298: no non-test caller needs the unlocked convenience form any
// more — `cached_heartbeat_ack` and `cached_list_response` both already
// hold the registry lock when they build a snapshot, so they call
// `roster_snapshot_locked` directly. Kept for tests, which don't.
#[cfg(test)]
fn roster_snapshot(registry: &Registry, replication: usize) -> RosterSnapshot {
    roster_snapshot_locked(&lock(registry), replication)
}

/// The scan half of `roster_snapshot`, taking an already-held registry
/// guard — lets `cached_heartbeat_ack` read the generation and build the
/// snapshot under one lock acquisition (issue #95).
fn roster_snapshot_locked(
    guard: &FxHashMap<String, NodeInfo>,
    replication: usize,
) -> RosterSnapshot {
    let mut agreeing = 0usize;
    let mut dissenting: Vec<String> = Vec::new();
    let mut nodes: Vec<(String, String)> = Vec::new();
    for (name, info) in guard.iter() {
        if info.state != NodeState::Joined {
            continue;
        }
        match info.reported_replication {
            Some(r) if r == replication => agreeing += 1,
            Some(_) => dissenting.push(name.clone()),
            None => {}
        }
        nodes.push((name.clone(), info.address.clone()));
    }
    RosterSnapshot {
        agreeing,
        dissenting,
        nodes,
    }
}

/// The `Joined` roster as the `H` ack (issue #61, `tag` `A`) and the `L`
/// response (`tag` `N`) both carry it: `<tag> <count> <replication>\n`
/// then one `<name-len> <addr-len>\n<name><addr>\n` per node — one entry
/// layout, so a node parses the ack the way an SDK parses `L`.
fn build_roster_response(tag: char, nodes: &[(String, String)], replication: usize) -> Vec<u8> {
    let mut response = format!("{tag} {} {}\n", nodes.len(), replication).into_bytes();
    for (name, addr) in nodes {
        response.extend_from_slice(format!("{} {}\n", name.len(), addr.len()).as_bytes());
        response.extend_from_slice(name.as_bytes());
        response.extend_from_slice(addr.as_bytes());
        response.push(b'\n');
    }
    response
}

/// One response derived from the registry, cached in `cache` and rebuilt
/// only when the registry's `generation` has moved since the last build.
/// On the common path this returns the shared cached buffer after one
/// atomic load and an equality check — no registry lock, no
/// re-serialization. `snapshot` runs under the registry lock and captures
/// whatever the response needs; `render` serializes it outside the lock.
///
/// Whether the response was built at exactly the latest generation doesn't
/// matter for correctness — the roster is a convergence aid the next
/// heartbeat/refresh picks up — so the fast-path generation read need not
/// be taken under the registry lock. The rebuild reads it under the lock,
/// so it matches the state the snapshot serializes (a concurrent bump
/// lands either fully before or fully after that critical section), and
/// only ever advances the cache: a concurrent rebuild that already stored
/// a newer generation for this same replication is never overwritten with
/// an older one.
fn cached_by_generation<S>(
    registry: &Registry,
    cache: &Mutex<Option<CachedResponse>>,
    replication: usize,
    snapshot: impl FnOnce(&FxHashMap<String, NodeInfo>) -> S,
    render: impl FnOnce(S) -> Arc<[u8]>,
) -> Arc<[u8]> {
    let generation = registry.generation.load(Ordering::Relaxed);
    {
        let cached = cache
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if let Some(current) = cached.as_ref()
            && current.generation == generation
            && current.replication == replication
        {
            return Arc::clone(&current.response);
        }
    }

    let (built_generation, snapshot) = {
        let guard = lock(registry);
        let generation = registry.generation.load(Ordering::Relaxed);
        (generation, snapshot(&guard))
    };
    let response = render(snapshot);

    let mut cached = cache
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    if cached.as_ref().is_none_or(|current| {
        current.replication != replication || current.generation <= built_generation
    }) {
        *cached = Some(CachedResponse {
            generation: built_generation,
            replication,
            response: Arc::clone(&response),
        });
    }
    response
}

/// The heartbeat-ack roster, cached and rebuilt only when the registry's
/// `generation` has moved since the last build (issue #95). With #61
/// every `H` from every `Joined` node carries the full roster; rebuilding
/// it per heartbeat is O(nodes) scan + serialize under the registry lock,
/// O(nodes²) per liveness cycle. Membership changes far less often than
/// nodes heartbeat, so `cached_by_generation`'s fast path serves nearly
/// every `H`.
///
/// The withheld-roster cases (`refuse`, a bare `A\n`) are baked into the
/// cached bytes; the startup-grace `A\n` is handled by the caller before
/// this is reached (it's time- not membership-gated, so it must not be
/// cached). Issue #279: the `refuse()` branch — a bare `A\n`, no roster —
/// is exactly the case `RosterSnapshot::refuse`'s doc comment warns about:
/// for as long as this replica's `--replication-factor` disagreement
/// persists, every `H` answered here tells the heartbeating node nothing
/// about membership, eviction included.
fn cached_heartbeat_ack(registry: &Registry, replication: usize) -> Arc<[u8]> {
    cached_by_generation(
        registry,
        &registry.heartbeat_ack,
        replication,
        |guard| roster_snapshot_locked(guard, replication),
        |snapshot| {
            if snapshot.refuse() {
                Arc::from(b"A\n".as_slice())
            } else {
                Arc::from(build_roster_response('A', &snapshot.nodes, replication).as_slice())
            }
        },
    )
}

/// Issue #1717: the dissent WARN gets its own budget. Every `L` rebuild
/// writes one while a dissenter is present, and generation bumps
/// interleaved with `L` make a rebuild per request.
///
/// Issue #1826: "served despite dissent" and "refusing L" each have a
/// budget of their own. The cache is rebuilt only on a generation bump, so
/// the refusal is written once, at the rebuild where `L` starts answering
/// `B`; with one shared budget a "served" line just before it swallowed
/// that line and `L` answered `B` with nothing in the log saying why.
/// Both stay bounded (one line per window each).
struct DissentLog {
    served: LogLimiter,
    refusing: LogLimiter,
}

impl DissentLog {
    const fn new() -> Self {
        Self {
            served: LogLimiter::new(1, Duration::from_secs(10)),
            refusing: LogLimiter::new(1, Duration::from_secs(10)),
        }
    }
}

static DISSENT_LOG: DissentLog = DissentLog::new();

/// How many dissenting names the WARN lists; the rest are counted. With
/// many fake nodes the full list was one line of megabytes (65k names).
const DISSENT_NAMES_LISTED: usize = 5;

fn dissenter_names(dissenting: &[String]) -> String {
    let listed = dissenting
        .iter()
        .take(DISSENT_NAMES_LISTED)
        .map(String::as_str)
        .collect::<Vec<_>>()
        .join(", ");
    match dissenting.len().checked_sub(DISSENT_NAMES_LISTED) {
        Some(rest) if rest > 0 => format!("{listed} and {rest} more"),
        _ => listed,
    }
}

/// Writes the replication-factor dissent WARN for `snapshot` if the
/// limiter admits one; returns whether it did. Nothing is formatted when
/// the line is suppressed.
fn log_dissent(log: &DissentLog, snapshot: &RosterSnapshot, replication: usize) -> bool {
    if snapshot.dissenting.is_empty() {
        return false;
    }
    let limiter = if snapshot.refuse() {
        &log.refusing
    } else {
        &log.served
    };
    let Some(suppressed) = limiter.admit() else {
        return false;
    };
    if suppressed > 0 {
        eprintln!("WARN {suppressed} more replication dissent warnings (not logged individually)");
    }
    let dissenting = snapshot.dissenting.len();
    let voting = dissenting + snapshot.agreeing;
    let names = dissenter_names(&snapshot.dissenting);
    if snapshot.refuse() {
        eprintln!(
            "WARN refusing L: {dissenting} of {voting} voting Joined nodes report a \
             replication factor different from this replica's own \
             --replication-factor {replication} (a strict majority) — dissenting: {names} — \
             discovery replicas have drifted out of alignment; the operator \
             must align --replication-factor across every replica (see \
             Discovery HA)"
        );
    } else {
        eprintln!(
            "WARN L served despite {dissenting} of {voting} voting Joined nodes reporting a \
             replication factor different from this replica's own \
             --replication-factor {replication} — not yet a strict majority, so still \
             served, but worth investigating: {names}"
        );
    }
    true
}

/// The `L` response, cached and rebuilt only when the registry's
/// `generation` has moved since the last build (issue #298) —
/// `cached_heartbeat_ack`'s sibling for `L`. Before this, every `L` re-ran
/// the vote tally and re-serialized the roster (`roster_snapshot`) per
/// request — O(registry) work with none of #61's excuse (every `H`
/// fanning the roster out on a timer); here it was simply paid again on
/// every single `L` call. `L` renders exactly the same `roster_snapshot`
/// state `cached_heartbeat_ack` already invalidates on — the `Joined`
/// set, addresses, and `reported_replication` votes — so this reuses
/// `RegistryState::generation` rather than needing its own bump call
/// sites. `Q` (`ListProxies`) is unaffected and needs none either: it
/// renders the entirely separate `proxies` map, never `roster_snapshot`
/// (see `ProxyInfo`'s doc comment — "never affect `L`/`H`"), so a proxy
/// `Y` announce correctly leaves this cache alone.
///
/// The refuse case (issue #30's amended majority check, a bare `B\n`) is
/// folded into the cached bytes, same as `cached_heartbeat_ack` folds its
/// own withheld-roster case into `ack`; the caller tells the two apart by
/// the leading byte (`B` vs `N`) rather than this returning a separate
/// flag. The dissenting-vote WARN logging the un-cached handler used to
/// do on every request now fires only on an actual rebuild — a cache hit
/// skips it exactly as it skips re-tallying the vote in the first place.
fn cached_list_response(registry: &Registry, replication: usize) -> Arc<[u8]> {
    cached_by_generation(
        registry,
        &registry.list_cache,
        replication,
        |guard| roster_snapshot_locked(guard, replication),
        |snapshot| {
            // Issue #30 (amended, HIGH-severity follow-up): see the module
            // docs' `L` entry and `RosterSnapshot::refuse`'s own doc
            // comment for why replication-factor disagreement gets a
            // strict-majority vote rather than refusing on any dissent.
            // Logged here, on rebuild only, rather than on every request
            // as the un-cached handler used to.
            log_dissent(&DISSENT_LOG, &snapshot, replication);

            if snapshot.refuse() {
                Arc::from(b"B\n".as_slice())
            } else {
                Arc::from(build_roster_response('N', &snapshot.nodes, replication).as_slice())
            }
        },
    )
}

/// The `T` (NodeRoster) response: `N <count> <replication>\n` then one
/// `<name-len> <addr-len> <token-len>\n<name><addr><token>\n` per
/// registered node — `build_roster_response`'s sibling for `T`'s
/// token-carrying, every-state listing (see the `NodeRoster` handler for
/// why `Waiting`/`Joining` entries belong in it).
fn build_node_roster_response(entries: &[(String, String, String)], replication: usize) -> Vec<u8> {
    let mut response = format!("N {} {}\n", entries.len(), replication).into_bytes();
    for (name, addr, token) in entries {
        response.extend_from_slice(
            format!("{} {} {}\n", name.len(), addr.len(), token.len()).as_bytes(),
        );
        response.extend_from_slice(name.as_bytes());
        response.extend_from_slice(addr.as_bytes());
        response.extend_from_slice(token.as_bytes());
        response.push(b'\n');
    }
    response
}

/// The `T` response, cached and rebuilt only when the registry's
/// `generation` has moved since the last build (issue #356) —
/// `cached_list_response`'s sibling for `T`. Before this, every `T`
/// scanned and re-serialized the whole registry (up to
/// `MAX_REGISTRY_SIZE` entries) under the registry lock per request; the
/// call is internal, authenticated, and comparatively rare, but an
/// already-authenticated node tight-looping `T` was still an
/// O(registry)-per-request load source with none of that excuse. `T`'s
/// view is broader than `L`/`H`'s (every state, tokens included), which
/// is why `bump_roster` also fires on the Waiting/Joining-only mutations
/// — see its doc comment for the list.
///
/// No refuse case to fold in (no replication vote, per the handler's own
/// comment); requester authentication and the `list_ready_at` startup
/// grace are the caller's, exactly as they are for `L`/`H`.
fn cached_node_roster(registry: &Registry, replication: usize) -> Arc<[u8]> {
    cached_by_generation(
        registry,
        &registry.roster_cache,
        replication,
        |guard| -> Vec<(String, String, String)> {
            guard
                .iter()
                .map(|(name, info)| (name.clone(), info.address.clone(), info.token.clone()))
                .collect()
        },
        |entries| Arc::from(build_node_roster_response(&entries, replication).as_slice()),
    )
}

/// What `sweep_expired` hands off to a background task instead of
/// awaiting inline — the two join operations that fan out to other nodes.
enum JoinFanout {
    /// `abandon_current_join` for `join`: an `X` to every ready member of
    /// the join, then the next join's `M`s.
    Abandon {
        reason: &'static str,
        join: JoinIdentity,
    },
    /// `try_begin_next_join`: the post-grace kick-off.
    BeginNext,
}

/// `sweep_expired`'s inputs for `abandon_current_join`/
/// `try_begin_next_join`, cloneable into a spawned task.
struct JoinFanoutContext {
    registry: Registry,
    current_join: CurrentJoin,
    auth_secret: Option<Bytes>,
    tls_connector: Option<TlsConnector>,
    replication: usize,
    joins_ready_at: Instant,
}

impl JoinFanoutContext {
    /// Starts `action` on `fanouts`. The caller guarantees `fanouts` is
    /// empty (see `sweep_expired`): at most one runs at a time, so they
    /// stay as serialized as they were when the sweep awaited them inline.
    fn spawn(&self, fanouts: &mut JoinSet<()>, action: JoinFanout) {
        let registry = Arc::clone(&self.registry);
        let current_join = Arc::clone(&self.current_join);
        let auth_secret = self.auth_secret.clone();
        let tls_connector = self.tls_connector.clone();
        let (replication, joins_ready_at) = (self.replication, self.joins_ready_at);
        fanouts.spawn(async move {
            match action {
                JoinFanout::Abandon { reason, join } => {
                    abandon_current_join(
                        &registry,
                        &current_join,
                        &auth_secret,
                        &tls_connector,
                        replication,
                        joins_ready_at,
                        &join,
                        reason,
                    )
                    .await;
                }
                JoinFanout::BeginNext => {
                    try_begin_next_join(
                        &registry,
                        &current_join,
                        &auth_secret,
                        &tls_connector,
                        replication,
                        joins_ready_at,
                    )
                    .await;
                }
            }
        });
    }
}

#[allow(clippy::too_many_arguments)]
async fn sweep_expired(
    registry: Registry,
    current_join: CurrentJoin,
    auth_secret: Option<Bytes>,
    tls_connector: Option<TlsConnector>,
    replication: usize,
    joins_ready_at: Instant,
    liveness_timeout: Duration,
    mut shutdown_rx: watch::Receiver<bool>,
) {
    let sweep_interval = (liveness_timeout / 4)
        .min(Duration::from_secs(1))
        .max(Duration::from_millis(1));
    let mut ticker = interval(sweep_interval);
    let mut grace_joins_kicked = false;
    let mut last_tick = Instant::now();
    // Issue #1663: whether a `Waiting` entry was held at the previous tick,
    // and whether a hold ended since the queue was last started.
    let mut had_holds = false;
    let mut begin_after_hold = false;

    // The join fan-outs (`abandon_current_join`'s `X`s, `try_begin_next_join`'s
    // `M`s with their retries) used to be awaited right here, which parked
    // this — the sole task doing liveness eviction, proxy reaping and the
    // migration-timeout reaper — for up to ~40 s (X) or ~120 s (M, three
    // attempts) per batch of `MAX_FANOUT_CONCURRENCY` unresponsive nodes:
    // exactly when nodes are failing, nothing was being evicted or reaped.
    // They now run on this `JoinSet` instead, at most one at a time (a new
    // one starts only once `fanouts` is empty), so sweep-initiated fan-outs
    // stay serialized with each other exactly as before and cannot pile up;
    // only the rest of the tick no longer waits on them. A state-based
    // trigger skipped because one is still running simply fires again on
    // the next tick; the one edge-based trigger (a member evicted this tick)
    // is remembered in `evicted_members` until it can be acted on.
    let fanout_context = JoinFanoutContext {
        registry: Arc::clone(&registry),
        current_join: Arc::clone(&current_join),
        auth_secret: auth_secret.clone(),
        tls_connector: tls_connector.clone(),
        replication,
        joins_ready_at,
    };
    let mut fanouts: JoinSet<()> = JoinSet::new();
    let mut evicted_members: HashSet<String> = HashSet::new();

    loop {
        tokio::select! {
            _ = ticker.tick() => {
                let now = Instant::now();
                let stalled = sweep_was_stalled(last_tick, now, liveness_timeout);
                let since_last_tick = now.duration_since(last_tick);
                last_tick = now;
                // Reap finished fan-outs: frees the slot, and surfaces a
                // panic the way the old inline call's would have (loudly).
                while let Some(finished) = fanouts.try_join_next() {
                    if let Err(error) = finished {
                        eprintln!("WARN a join fan-out task failed: {error}");
                    }
                }
                // Joined nodes are reaped on missed heartbeats. Joining
                // nodes hold one long-lived connection open instead of
                // heartbeating (see NodeInfo::promoted) and are bounded
                // separately, below, by the migration timeout. Waiting
                // nodes hold a connection open too, but — issue:
                // unauthenticated `J` connection exhaustion — that alone
                // doesn't bound how long one can sit unpromoted if it's
                // never actually going to get its turn (a fake
                // registration with nothing behind it), so they're also
                // reaped here once `waiting_timeout_for` has elapsed.
                // Issue #122: proxies that stopped re-announcing.
                //
                // Issue #1312: after a stall every `last_heartbeat` and
                // `last_seen` is as old as the stall, so judging them now
                // would evict (and forget) every healthy node and proxy
                // before any of them could be read. A stalled tick instead
                // gives each a fresh window: one that really is gone is
                // dropped a liveness timeout later.
                let mut proxies_dropped = false;
                registry
                    .proxies
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner())
                    .retain(|name, info| {
                        if stalled {
                            info.last_seen = now;
                        }
                        let keep = now.duration_since(info.last_seen) < liveness_timeout;
                        if !keep {
                            println!("INFO proxy dropped after missed announces: {name}");
                            proxies_dropped = true;
                        }
                        keep
                    });
                if proxies_dropped {
                    bump_proxies(&registry);
                }

                let mut heartbeat_evicted = Vec::new();
                let mut waiting_evicted = Vec::new();
                {
                    let mut guard = lock(&registry);
                    guard.retain(|name, info| {
                        if stalled && info.state == NodeState::Joined {
                            info.last_heartbeat = now;
                        }
                        let keep = match info.state {
                            NodeState::Joined => {
                                now.duration_since(info.last_heartbeat) < liveness_timeout
                            }
                            NodeState::Waiting => {
                                now.duration_since(info.waiting_since)
                                    < waiting_timeout_for(info.queue_position)
                            }
                            NodeState::Joining => true,
                        };
                        if !keep {
                            match info.state {
                                NodeState::Joined => heartbeat_evicted.push(name.clone()),
                                NodeState::Waiting => {
                                    waiting_evicted
                                        .push((name.clone(), Arc::clone(&info.promoted)));
                                }
                                NodeState::Joining => unreachable!("Joining always kept above"),
                            }
                        }
                        keep
                    });
                    // Evicting a `Joined` node changes the heartbeat-ack
                    // roster (issue #95); a `Waiting` eviction changes only
                    // `T`'s every-state roster, which keys off the same
                    // generation (issue #356). Issue #279: bumped here,
                    // still under `guard`, rather than after it drops.
                    if !heartbeat_evicted.is_empty() || !waiting_evicted.is_empty() {
                        bump_roster(&registry);
                    }
                }
                if stalled {
                    eprintln!(
                        "WARN discovery was not running for {}s before this sweep: every \
                         node and proxy gets a fresh {}s liveness window instead of being \
                         judged by heartbeats it could not read",
                        since_last_tick.as_secs(),
                        liveness_timeout.as_secs()
                    );
                }
                // Nothing is evicted on a stalled tick, so `stalled` holds
                // below only as a second guard: should a node be evicted
                // here all the same, it is not sent back through a join.
                for name in &heartbeat_evicted {
                    eprintln!(
                        "WARN node evicted: {name} (no heartbeat within {}s)",
                        liveness_timeout.as_secs()
                    );
                    if stalled {
                        eprintln!(
                            "WARN discovery was not running for {}s before this sweep: {name} \
                             may be healthy and is not sent back through a join if it announces \
                             itself again",
                            since_last_tick.as_secs()
                        );
                        record_departed_by_stalled_sweep(&registry, name);
                    } else {
                        record_departed(&registry, name);
                    }
                }

                // Issue #34 forged-completion fix, other half (see
                // `PendingJoin::expected`'s doc comment): a `Joined` node
                // evicted here for missing heartbeats may be one of the
                // current join's own ready members, still expected but not
                // yet `completed`. Left alone, the join would either hang
                // until `migration_timeout_for` eventually reaps it, or —
                // worse — sit there long enough for an attacker to
                // re-register the now-free name (names are public via `L`;
                // an unknown name is trust-on-first-use, per-node membership tokens) and
                // forge a `C` for a handoff that member never performed.
                // `handle_complete`'s token-snapshot check already closes
                // that specific forgery, but abandoning immediately is
                // still strictly better than waiting on the timeout for a
                // member that is provably gone. A no-op if none of the
                // evicted names are part of the current join.
                //
                // Edge-triggered (the names exist only in this tick's
                // `heartbeat_evicted`), so they're kept in `evicted_members`
                // until the abandon is actually started — a fan-out still
                // running from an earlier tick can occupy the slot — and
                // dropped once the current join no longer involves any of
                // them (the abandon in flight already took it, or it never
                // did).
                evicted_members.extend(heartbeat_evicted.iter().cloned());
                let join_with_evicted_member: Option<JoinIdentity> =
                    if evicted_members.is_empty() {
                        None
                    } else {
                        lock_current_join(&current_join)
                            .as_ref()
                            .filter(|pending| {
                                evicted_members.iter().any(|name| {
                                    pending.expected.contains_key(name)
                                        && !pending.completed.contains(name)
                                })
                            })
                            .map(PendingJoin::identity)
                    };
                match join_with_evicted_member {
                    None => evicted_members.clear(),
                    Some(join) if fanouts.is_empty() => {
                        evicted_members.clear();
                        fanout_context.spawn(
                            &mut fanouts,
                            JoinFanout::Abandon {
                                reason: "ready member evicted mid-join",
                                join,
                            },
                        );
                    }
                    Some(_) => {}
                }

                // Issue #1663: decide the address collisions `start_join`
                // left open. After the stall guard above, so that a pause
                // of discovery is not read as the old name's silence.
                let holds = settle_address_holds(&registry, now);
                drop_replaced_from_current_join(&registry, &current_join, &holds.replaced);
                // A hold that ended, or whose held entry went away (its
                // connection died, the waiting timeout), may have been all
                // that kept the queue from starting.
                begin_after_hold |= holds.decided || (had_holds && !holds.holds_remain);
                had_holds = holds.holds_remain;

                for (name, promoted) in waiting_evicted {
                    record_departed(&registry, &name);
                    eprintln!(
                        "WARN node evicted: {name} (never promoted; waiting-to-join timeout \
                         elapsed — see MAX_WAITING_PER_SOURCE_IP/waiting_timeout_for)"
                    );
                    // Same wake-up as `abandon_current_join`/
                    // `on_node_connection_ended` (issue #4/#9): a
                    // connection may be parked in `wait_for_promotion` on
                    // this same `Notify`, and removing the entry without
                    // waking it would strand that connection (and the
                    // `MAX_CONNECTIONS` permit it holds) forever.
                    promoted.notify_waiters();
                    promoted.notify_one();
                }

                // Issue #1617: the pause that gave every node a fresh
                // liveness window above gave the join the same: the nodes
                // could not report `C` to a discovery that was not
                // running, so its migration timeout does not count the
                // time. Only the time discovery was running is spent.
                if stalled && let Some(pending) = lock_current_join(&current_join).as_mut() {
                    pending.started_at = (pending.started_at + since_last_tick).min(now);
                }

                // Staged node join pattern 3: a ready node alive but never
                // reporting `C` (see `migration_timeout_for`).
                let timed_out: Option<JoinIdentity> = lock_current_join(&current_join)
                    .as_ref()
                    .filter(|pending| {
                        pending.started_at.elapsed() >= migration_timeout_for(pending.max_entries)
                    })
                    .map(PendingJoin::identity);

                // State-based: still true on the next tick if a fan-out in
                // flight kept this one from starting now.
                if let Some(join) = timed_out
                    && fanouts.is_empty()
                {
                    fanout_context.spawn(
                        &mut fanouts,
                        JoinFanout::Abandon {
                            reason: "migration timeout",
                            join,
                        },
                    );
                }

                // Issue #63: a `J` accepted during the startup grace was
                // parked in `Waiting` (see `try_begin_next_join`); nothing
                // event-driven starts it once the grace ends, so the
                // first tick past the grace does — once; from then on
                // joins are started by `J`/`C`/abandon exactly as before.
                // (No join can have started during the grace, so there's
                // nothing for this one call to collide with.)
                if !grace_joins_kicked && fanouts.is_empty() && Instant::now() >= joins_ready_at {
                    grace_joins_kicked = true;
                    fanout_context.spawn(&mut fanouts, JoinFanout::BeginNext);
                }

                // State-based like the kick above: still pending on the next
                // tick if a fan-out in flight kept it from starting now.
                if begin_after_hold && fanouts.is_empty() {
                    begin_after_hold = false;
                    fanout_context.spawn(&mut fanouts, JoinFanout::BeginNext);
                }
            }
            _ = shutdown_rx.changed() => {
                // A fan-out in flight used to finish before the sweep could
                // observe the shutdown (it was awaited inline); dropping
                // `fanouts` here would instead abort it mid-`X`/`M`. Give it
                // the same bound `run` gives the sweep task as a whole.
                let _ = timeout(SHUTDOWN_TIMEOUT, async {
                    while fanouts.join_next().await.is_some() {}
                })
                .await;
                return;
            }
        }
    }
}

/// Bounds every response write to a client-or-node-facing `stream` in
/// `handle_connection`/`wait_for_promotion` (mirrors `src/server.rs`'s own
/// `write_response`): the read side already has `IDLE_TIMEOUT`/
/// `UNIDENTIFIED_CONNECTION_TIMEOUT`, but an unbounded `write_all` let a
/// peer that stops reading (without closing the TCP connection — e.g. a
/// full receive buffer) hold this connection's `MAX_CONNECTIONS` permit
/// forever. Uses `WRITE_TIMEOUT` rather than reusing `IDLE_TIMEOUT`: the
/// two are different failure modes (a normal gap between requests vs. a
/// peer that isn't draining its receive buffer at all), and reusing the
/// 60s read timeout would let a stuck write hold a permit far longer than
/// necessary. Distinct from the outbound writes of `send_migrate` and
/// `send_cancel`, which bound this process's dials to a node (`M`/`X`)
/// under `OUTBOUND_IO_TIMEOUT` —
/// that one is about this process acting as a client of another node;
/// this one is about this process, as a server, replying to whoever
/// dialed it.
async fn write_response(stream: &mut ServerStream, data: &[u8]) -> io::Result<()> {
    infra::write_all_within(stream, data, WRITE_TIMEOUT).await
}

/// How long `drain_rejected_request` keeps reading after the `Ed\n` for
/// the client to close first. The node's and the proxy's
/// `REJECTED_REQUEST_DRAIN_WINDOW`.
const REJECTED_REQUEST_DRAIN_WINDOW: Duration = Duration::from_secs(1);

/// After the `Ed\n` to a request that does not parse: half-close the write
/// side (the client sees the reply and EOF at once), then read and discard
/// what is still arriving — until the client closes, for at most
/// `REJECTED_REQUEST_DRAIN_WINDOW` and a request's worth of bytes — before
/// the socket is dropped. A request is rejected from its header (a `J`
/// whose name length is over the limit, say) while its body may still be
/// on the way, and closing a socket with unread bytes in its receive
/// buffer sends a reset, which can discard the `Ed\n` on the client's side
/// before it is read. The node and the proxy do the same for their `E`.
async fn drain_rejected_request(stream: &mut ServerStream) {
    // The half-close is inside the window too: over TLS it first flushes
    // the pending records, which blocks for as long as a peer that has
    // stopped reading keeps the send buffer full.
    let drain = async {
        let _ = stream.shutdown().await;
        let mut sink = [0_u8; 1024];
        let mut discarded = 0;
        while discarded < MAX_REQUEST_SIZE {
            match stream.read(&mut sink).await {
                Ok(0) | Err(_) => break,
                Ok(n) => discarded += n,
            }
        }
    };
    let _ = timeout(REJECTED_REQUEST_DRAIN_WINDOW, drain).await;
}

async fn handle_connection(
    mut stream: ServerStream,
    // The connection's source IP: combined with the port a `J`/`P`
    // declares, it IS the node's address (addresses derived from the registration connection).
    peer_ip: std::net::IpAddr,
    registry: Registry,
    current_join: CurrentJoin,
    config: ConnectionConfig,
    mut shutdown_rx: watch::Receiver<bool>,
    // Written once this connection identifies itself via `J`/`P` (a plain
    // client connection never does, and stays `None`) — read afterward,
    // regardless of how this function exits, so the caller's
    // `on_node_connection_ended` cleanup runs uniformly instead of needing
    // a call at every one of this function's internal return points.
    // Carries the connection's own id (`next_connection_id`, issue #3/#9)
    // alongside the name so that cleanup can tell this connection apart
    // from a duplicate `J` that has since superseded it — see
    // `NodeInfo::owner_connection_id`.
    connection_name: Arc<std::sync::Mutex<Option<(String, u64)>>>,
) -> io::Result<()> {
    // Issue #3/#9: this connection's own identity, established once here
    // regardless of whether it ever sends a `J` — only actually consulted
    // if it does (see the `Join` arm below and `connection_name` above).
    let connection_id = next_connection_id();
    let mut received = BytesMut::new();
    // No secret configured means auth isn't required, so every connection
    // starts already authenticated.
    let mut authenticated = config.auth_secret.is_none();
    // Issue (slowloris via MAX_CONNECTIONS exhaustion): whether this
    // connection has ever successfully parsed a single complete command.
    // `IDLE_TIMEOUT` alone resets on every partial read, so a connection
    // that trickles in one byte at a time — never completing a frame —
    // can otherwise hold a `MAX_CONNECTIONS` slot open indefinitely.
    // `unidentified_deadline` below bounds that; once `identified` flips
    // true it no longer applies, and `IDLE_TIMEOUT` governs exactly as
    // before this fix.
    let mut identified = false;
    let unidentified_deadline = Instant::now() + UNIDENTIFIED_CONNECTION_TIMEOUT;

    // Slowloris resistance (mirrors `src/server.rs`'s own `deadline`):
    // anchored to accept-time here, then re-anchored to
    // `now + config.idle_timeout` below every time `parse` completes a
    // full command — never on a bare read. An earlier version of this
    // function instead recomputed `now + config.idle_timeout` on *every*
    // read once `identified` was true, so a client that sent one command
    // and then trickled in a single byte just under `idle_timeout` apart
    // could hold a `MAX_CONNECTIONS` permit open forever without ever
    // completing another request. While `!identified`, the effective
    // bound is `deadline.min(unidentified_deadline)` — see the read loop
    // below — so the tighter pre-first-command bound from
    // `UNIDENTIFIED_CONNECTION_TIMEOUT` still applies.
    let mut deadline = Instant::now() + config.idle_timeout;
    // Set when a command was just parsed (and so handled): the handler may
    // have awaited far longer than `idle_timeout` — a `J` parked in
    // `wait_for_promotion` for the whole join queue, a `C`/`V` running an
    // `M`/`X` fan-out — so the budget for the *next* command is re-anchored
    // to the moment the handler finished, just before waiting for it. See
    // the read below.
    let mut handled_a_command = false;

    loop {
        let parsed = parse(&mut received);

        // Only a fully parsed command extends the deadline — an
        // `Incomplete` result (more bytes needed) leaves it untouched, so
        // a client that trickles bytes in without ever finishing a
        // command can't renew its own budget one byte at a time. See
        // `deadline`'s own doc comment above.
        if parsed.is_ok() {
            identified = true;
            deadline = Instant::now() + config.idle_timeout;
            handled_a_command = true;
        }
        match parsed {
            Ok(DiscoveryCommand::Auth { secret, tagging }) => {
                let accepted = match &config.auth_secret {
                    Some(expected) => constant_time_eq(&secret, expected),
                    None => true,
                };

                // Echoed response tags: echo the tag capability only to a client that
                // asked — a plain `A` keeps the exact three-byte reply
                // older SDKs hard-read.
                let (ok, err): (&[u8], &[u8]) = if tagging {
                    (b"OdT\n", b"EdT\n")
                } else {
                    (b"Od\n", b"Ed\n")
                };

                if accepted {
                    authenticated = true;
                    write_response(&mut stream, ok).await?;
                    continue;
                }

                write_response(&mut stream, err).await?;
                return Err(io::Error::new(
                    io::ErrorKind::PermissionDenied,
                    "invalid auth secret",
                ));
            }
            Ok(_) if !authenticated => {
                write_response(&mut stream, b"Ed\n").await?;
                return Err(io::Error::new(
                    io::ErrorKind::PermissionDenied,
                    "command sent before authenticating",
                ));
            }
            Ok(DiscoveryCommand::Heartbeat {
                name,
                replication,
                token,
                pending_joiner,
            }) => {
                // Checked before anything is refreshed (issue #34): a
                // heartbeat presenting the wrong token must not keep an
                // entry alive — otherwise anyone who learned a name from
                // `L` could pin a dead node's (or, after a takeover
                // attempt, a hijacked address's) entry past
                // `sweep_expired` forever.
                let refreshed = {
                    let mut guard = lock(&registry);
                    match guard.get_mut(&name) {
                        Some(info)
                            if info.state == NodeState::Joined
                                && constant_time_eq(info.token.as_bytes(), token.as_bytes()) =>
                        {
                            note_contact(info, &name, Instant::now());
                            // Stored (`Some` or `None`) every heartbeat —
                            // see `NodeInfo::reported_replication` and the
                            // `L` handler, which reads this back. Only an
                            // actual *change* bumps the roster generation
                            // (issue #95): last_heartbeat moves every tick
                            // but doesn't affect the ack roster, and the
                            // vote is the same value almost every time, so
                            // bumping unconditionally would defeat the
                            // heartbeat-ack cache.
                            if info.reported_replication != replication {
                                info.reported_replication = replication;
                                bump_roster(&registry);
                            }
                            true
                        }
                        _ => false,
                    }
                };

                if !refreshed {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "heartbeat from a node that has not joined (or presented the wrong token)",
                    ));
                }

                // Issue #30: the node's own belief (learned from a `M` it
                // sent as an client-side replication handoff source) may disagree with
                // this replica's configured value — every discovery
                // replica takes its own `--replication-factor` flag and
                // nothing else validates they agree (discovery HA deliberately
                // keeps replicas from talking to each other). Membership
                // itself is soft state that's expected to converge as
                // nodes join, leave, and heartbeat, so discovery HA's "never
                // reconcile, just log" applies there — but replication
                // factor is static operator config, not membership, and a
                // persistent disagreement means this replica's own
                // `--replication-factor` (which `L` embeds) is a value at
                // least one node in the cluster has already learned is
                // wrong. Recorded above (`reported_replication`) so the
                // `L` handler can refuse to serve that value instead of
                // handing it out anyway; still logged loudly here too,
                // since even a since-resolved disagreement is worth the
                // operator's attention.
                if let Some(reported) = replication
                    && reported != config.replication
                {
                    log_rejection(format_args!(
                        "WARN node {name} reports replication factor {reported}, but this \
                         discovery replica is configured with --replication-factor \
                         {} — every replica must agree, since replicas never reconcile \
                         with each other; a client \
                         or node that learned R from a different replica may be running \
                         with a different fan-out/failover width right now",
                        config.replication
                    ));
                }

                // Issue #61: carry the current `Joined` roster so the node
                // can refresh its own membership view on every heartbeat,
                // not only on the `M` of a join. Withheld (bare `A\n`)
                // under the same two conditions `L` refuses with `B\n` —
                // see the module docs' `H` entry and `roster_snapshot`.
                if Instant::now() < config.list_ready_at {
                    // Startup grace is time-gated, not membership-gated, so
                    // it is handled here rather than folded into the cache.
                    write_response(&mut stream, b"A\n").await?;
                } else {
                    let ack = cached_heartbeat_ack(&registry, config.replication);
                    write_response(&mut stream, &ack).await?;
                }
                // Issue #571: answered whenever asked, grace or not, so
                // the node's read count is deterministic.
                if let Some(joiner) = pending_joiner.as_deref() {
                    write_response(&mut stream, pending_joiner_verdict(&registry, joiner)).await?;
                }
                continue;
            }
            Ok(DiscoveryCommand::List) => {
                if Instant::now() < config.list_ready_at {
                    // Discovery HA startup grace — see `ConnectionConfig::
                    // list_ready_at`. Same `B\n`-then-close shape as the
                    // connection-limit rejection: "can't serve you right
                    // now, retry", which the SDK maps to trying its next
                    // discovery seed.
                    write_response(&mut stream, b"B\n").await?;
                    return Ok(());
                }

                // Issue #30 (amended, HIGH-severity follow-up): refuse to
                // serve `config.replication` (the R this response embeds)
                // when a strict MAJORITY of currently-`Joined` nodes that
                // have reported a belief disagree with it — not merely
                // "any" — see `NodeInfo::reported_replication` and the
                // heartbeat handler's comment above for why replication
                // factor gets this treatment and membership doesn't. A
                // single dissenting node (a straggler that hasn't sent its
                // own `M` yet, or one genuinely misconfigured) must not be
                // able to DoS `L` for the whole cluster by itself — voting
                // is the difference: this replica's own
                // `--replication-factor` is only worth doubting once more
                // reporting nodes have learned a different value than have
                // confirmed this one, at which point it's this replica,
                // not the dissenters, that's most likely the one that's
                // wrong. A node that hasn't sent an client-side replication handoff `M`
                // yet (`reported_replication` is `None`) doesn't vote
                // either way. Same `B\n`-then-close shape as the
                // startup-grace refusal just above: "can't serve you right
                // now, retry" is exactly as appropriate here, since a
                // client bootstrapping now would otherwise compute a
                // replica set most of the cluster already disagrees with.
                // The vote tally and the served roster are read under one
                // lock acquisition so they're a consistent snapshot: a
                // concurrent heartbeat/leave can't slip between "no
                // majority disagrees on replication" and the roster we
                // then serve, which would otherwise let a `B\n`-worthy
                // state be served as a valid `N` list (or vice versa) for
                // one request.
                //
                // Issue #298: this used to re-run the vote tally and
                // re-serialize the roster on every single `L`; both now
                // come from `cached_list_response`, rebuilt only when
                // membership actually changes — see its own doc comment.
                // Its cached bytes fold in the refuse decision, so the
                // leading byte (`B` vs `N`) is all that's left to check
                // here to preserve this handler's `B\n`-then-close shape.
                let response = cached_list_response(&registry, config.replication);
                write_response(&mut stream, &response).await?;
                if response.first() == Some(&b'B') {
                    return Ok(());
                }
                continue;
            }
            Ok(DiscoveryCommand::NodeRoster { name, token }) => {
                // Issue #295: self-authenticate the requester first,
                // exactly like `Heartbeat` — a node presenting a name/
                // token this registry doesn't recognize as matching gets
                // nothing back, silently (no response written, connection
                // closed by the `Err` below): unlike `NodeLeave`/`Complete`
                // (idempotent no-ops for a stale/unknown name), a
                // mismatch here specifically means "not a genuinely
                // registered node", so there is nothing safe to answer.
                //
                // The `state == Joined` guard matches `Heartbeat`'s check
                // (see its handler): `start_join` stores the joiner's own
                // self-chosen token unverified, so a `Waiting`/`Joining`
                // entry proves nothing about identity. Without this guard,
                // anyone who can reach discovery could send `J <name>
                // <port> <token>` to park a `Waiting` entry under a token
                // they picked, then replay it here to read back EVERY
                // member's address and token — the exact "hands them only
                // to proven members" invariant this handler documents. A
                // genuinely joined node is the only proof of membership.
                let authenticated = {
                    let reg = lock(&registry);
                    reg.get(&name).is_some_and(|info| {
                        info.state == NodeState::Joined
                            && constant_time_eq(info.token.as_bytes(), token.as_bytes())
                    })
                };
                if !authenticated {
                    log_rejection(format_args!(
                        "WARN rejected T from {peer_ip}: {name} is not a registered node (or \
                         presented the wrong token)"
                    ));
                    return Err(io::Error::new(
                        io::ErrorKind::PermissionDenied,
                        "T carried an unrecognized name or the wrong membership token",
                    ));
                }

                if Instant::now() < config.list_ready_at {
                    // Same startup-grace withholding as `L`/`H` — a
                    // partial registry mid-restart must not be served as
                    // if it were complete.
                    write_response(&mut stream, b"B\n").await?;
                    return Ok(());
                }

                // Issue #295: unlike `L` (client-facing, `Joined`-only,
                // token-free — see `NodeInfo::token`'s doc comment), this
                // lists EVERY registered node regardless of state
                // (`Waiting`/`Joining`/`Joined`) with its address *and*
                // token — a `Joining` node (e.g. the joiner an in-flight
                // `M` names) is a legitimate target for the caller's
                // `U`/`u` frames just as much as an already-`Joined` one.
                // No replication-factor voting/refusal here (unlike `L`):
                // this is an internal, already-authenticated, comparatively
                // rare call (decommission, eviction re-replication), not
                // the hot client-routing path that protection guards.
                //
                // Issue #356: this used to scan and re-serialize the whole
                // registry per request; the roster now comes from
                // `cached_node_roster`, rebuilt only when the registry
                // actually changes — see its own doc comment.
                let response = cached_node_roster(&registry, config.replication);
                write_response(&mut stream, &response).await?;
                continue;
            }
            Ok(DiscoveryCommand::NodeLeave { name, token }) => {
                // Issue #124: the node has finished handing off its
                // entries; take it out of membership now — every later
                // heartbeat ack and `L` serves the post-leave roster.
                //
                // Issue #297: this removes whatever state the entry is in —
                // a Waiting/Joining node can send `V` too (e.g. to abort its
                // own pending join), not only an already-`Joined` one (see
                // `DiscoveryCommand::NodeLeave`'s doc comment). Every other
                // removal path (`on_node_connection_ended`,
                // `abandon_current_join`, the waiting-eviction in
                // `sweep_expired`) wakes `info.promoted` before dropping the
                // entry so a connection parked in `wait_for_promotion` (no
                // idle timeout while it waits) isn't stranded holding its
                // `MAX_CONNECTIONS` permit forever; this path must too.
                let outcome = {
                    let mut reg = lock(&registry);
                    match reg.get(&name) {
                        Some(info) if constant_time_eq(info.token.as_bytes(), token.as_bytes()) => {
                            let removed = reg.remove(&name).expect("just matched above");
                            removed.promoted.notify_waiters();
                            removed.promoted.notify_one();
                            // Issue #279: bumped here, still under `reg`,
                            // rather than after it drops below.
                            bump_roster(&registry);
                            record_departed(&registry, &name);
                            Ok(Some(removed.state))
                        }
                        Some(_) => Err(()),
                        None => Ok(None),
                    }
                };

                match outcome {
                    Ok(removed_state) => {
                        if removed_state.is_some() {
                            println!("INFO node left the cluster: {name}");
                        }

                        // Issue #297: mirrors `sweep_expired`'s
                        // `ready_member_evicted_mid_join` check (issue #34
                        // forged-completion fix) for the same condition
                        // reached a different way — a ready member of the
                        // in-flight join leaving via an explicit,
                        // authenticated `V` rather than a liveness eviction.
                        // Left unabandoned, the join would stall until
                        // `migration_timeout_for` (up to 2h) reaps it
                        // instead of moving on immediately.
                        //
                        // Issue #323: the *joining* node itself (state
                        // Joining or Waiting) can also send an explicit
                        // `V` to abort its own pending join — this must
                        // abandon `current_join` the same way its
                        // connection dying does in
                        // `on_node_connection_ended` (which checks
                        // `pending.joining_name == name` regardless of
                        // state). Before this fix only a `Joined` removal
                        // was ever checked here, so a self-`V` from the
                        // joining node left `current_join` pointing at an
                        // entry that no longer exists — stalling every
                        // later join behind it until
                        // `migration_timeout_for` eventually reaps the
                        // ghost.
                        let abandon: Option<(JoinIdentity, &str)> = match removed_state {
                            Some(NodeState::Joined) => lock_current_join(&current_join)
                                .as_ref()
                                .filter(|pending| {
                                    pending.expected.contains_key(&name)
                                        && !pending.completed.contains(&name)
                                })
                                .map(|pending| (pending.identity(), "ready member left mid-join")),
                            Some(NodeState::Joining) | Some(NodeState::Waiting) => {
                                lock_current_join(&current_join)
                                    .as_ref()
                                    .filter(|pending| pending.joining_name == name)
                                    .map(|pending| {
                                        (pending.identity(), "joining node left mid-join")
                                    })
                            }
                            None => None,
                        };
                        // Issue #1662: ack first, then abandon — the same
                        // order as `Complete` (issue #1029). The removal
                        // above is what the leaving node waits for; the
                        // abandon's `X` to every remaining ready member
                        // and the next join's `M` fan-out can take
                        // `OUTBOUND_IO_TIMEOUT` each against a member that
                        // accepts and never answers, and the node's
                        // `send_leave` waits only about that long for `R`
                        // (it would log `leave timed out` for a leave
                        // discovery had already applied, and delay its own
                        // exit). The abandon runs whether or not the ack
                        // was delivered: the node is out of the registry
                        // either way.
                        let acked = write_response(&mut stream, b"R\n").await;
                        if let Some((join, reason)) = abandon {
                            abandon_current_join(
                                &registry,
                                &current_join,
                                &config.auth_secret,
                                &config.tls_connector,
                                config.replication,
                                config.list_ready_at,
                                &join,
                                reason,
                            )
                            .await;
                        }
                        acked?;
                        continue;
                    }
                    Err(()) => {
                        log_rejection(format_args!(
                            "WARN rejected node leave for {name} from {peer_ip}: token mismatch"
                        ));
                        return Err(io::Error::new(
                            io::ErrorKind::InvalidData,
                            "node leave rejected",
                        ));
                    }
                }
            }
            Ok(DiscoveryCommand::ProxyDeregister { name, token }) => {
                // Issue #124: accepted any time (grace included — a
                // draining proxy must be able to leave whenever).
                let outcome = {
                    let mut proxies = registry
                        .proxies
                        .lock()
                        .unwrap_or_else(|poisoned| poisoned.into_inner());
                    match proxies.get(&name) {
                        Some(existing)
                            if constant_time_eq(existing.token.as_bytes(), token.as_bytes()) =>
                        {
                            proxies.remove(&name);
                            bump_proxies(&registry);
                            Ok(true)
                        }
                        Some(_) => Err(()),
                        // Unknown name: idempotent no-op (already gone,
                        // or expired by the sweep mid-drain).
                        None => Ok(false),
                    }
                };

                match outcome {
                    Ok(removed) => {
                        if removed {
                            // Issue #1716: one line per `Z` for a name the
                            // peer registered a moment before, so it is
                            // bounded like the node register/announce lines.
                            log_proxy_deregistered(format_args!("INFO proxy deregistered: {name}"));
                        }
                        write_response(&mut stream, b"R\n").await?;
                        continue;
                    }
                    Err(()) => {
                        log_rejection(format_args!(
                            "WARN rejected proxy deregister for {name} from {peer_ip}: \
                             token mismatch"
                        ));
                        return Err(io::Error::new(
                            io::ErrorKind::InvalidData,
                            "proxy deregister rejected",
                        ));
                    }
                }
            }
            Ok(DiscoveryCommand::ProxyAnnounce { name, port, token }) => {
                // Issue #122. Accepted during the startup grace too, like
                // `P` — that is exactly when a restarted replica needs
                // re-announces to refill this map.
                let addr = format!("{peer_ip}:{port}");
                let mut limited = false;
                let accepted = {
                    let mut proxies = registry
                        .proxies
                        .lock()
                        .unwrap_or_else(|poisoned| poisoned.into_inner());
                    let at_capacity = proxies.len() >= MAX_PROXY_ENTRIES;
                    match proxies.get_mut(&name) {
                        Some(existing)
                            if constant_time_eq(existing.token.as_bytes(), token.as_bytes()) =>
                        {
                            if existing.address != addr {
                                existing.address = addr;
                                bump_proxies(&registry);
                            }
                            existing.last_seen = Instant::now();
                            true
                        }
                        // A different token is a name hijack, not a
                        // refresh — see `ProxyInfo::token`.
                        Some(_) => false,
                        None if at_capacity => false,
                        // Issue #1716: a new name is gated like a new `P`
                        // (only here, so a refresh above is never gated,
                        // and a name refused for capacity spends no
                        // token). One connection can loop `Y` under fresh
                        // names, so without this it fills the 1024-entry
                        // map in milliseconds. The limiter has its own
                        // mutex and never takes this one.
                        None if !announce_insert_allowed(
                            &config.announce_limiter,
                            peer_ip,
                            AnnounceKind::Proxy,
                        ) =>
                        {
                            limited = true;
                            false
                        }
                        None => {
                            proxies.insert(
                                name.clone(),
                                ProxyInfo {
                                    address: addr,
                                    token,
                                    last_seen: Instant::now(),
                                },
                            );
                            bump_proxies(&registry);
                            true
                        }
                    }
                };

                if accepted {
                    write_response(&mut stream, b"R\n").await?;
                    continue;
                }
                if limited {
                    log_rejection(format_args!(
                        "WARN rejected proxy announce for {name} from {peer_ip}: more than \
                         {ANNOUNCE_INSERT_BURST} new registrations from this source at once; \
                         one more is allowed every {}s (see ANNOUNCE_INSERT_BURST / \
                         ANNOUNCE_INSERT_COOLDOWN)",
                        ANNOUNCE_INSERT_COOLDOWN.as_secs()
                    ));
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "too many new proxy announces from this source",
                    ));
                }
                log_rejection(format_args!(
                    "WARN rejected proxy announce for {name} from {peer_ip}: token mismatch \
                     or proxy registry full"
                ));
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "proxy announce rejected",
                ));
            }
            Ok(DiscoveryCommand::ListProxies) => {
                if Instant::now() < config.list_ready_at {
                    // Same startup-grace refusal as `L`: a freshly
                    // restarted replica hasn't heard re-announces yet, so
                    // an empty answer would read as "no proxies exist".
                    write_response(&mut stream, b"B\n").await?;
                    return Ok(());
                }

                // One shared buffer per registry state, so a blocked
                // write holds a reference, not a copy — see
                // `RegistryState::proxy_list_cache`.
                let response = cached_proxy_list(&registry);
                write_response(&mut stream, &response).await?;
                continue;
            }
            Ok(DiscoveryCommand::Join { name, port, token }) => {
                let addr = format!("{peer_ip}:{port}");

                let promoted = start_join(
                    &registry,
                    &current_join,
                    &config.auth_secret,
                    &config.tls_connector,
                    config.replication,
                    config.list_ready_at,
                    &name,
                    addr,
                    token,
                    connection_id,
                )
                .await;

                // `connection_name` is left unset on either rejection, so
                // this connection's teardown can't run cleanup against a
                // real node registered under that name (there isn't
                // one, in both cases below).
                let promoted = match promoted {
                    Ok(promoted) => promoted,
                    // A spurious `J` for an already-`Joined` name: reject
                    // rather than park forever — see `JoinRejection`.
                    Err(JoinRejection::AlreadyJoined) => {
                        return Err(io::Error::new(
                            io::ErrorKind::InvalidData,
                            "join for a node that is already joined",
                        ));
                    }
                    Err(JoinRejection::TokenMismatch) => {
                        log_rejection(format_args!(
                            "WARN rejected join for {name} from {peer_ip}: wrong token for \
                             an already-registered name — either an impersonation attempt \
                             or a node reusing another's name"
                        ));
                        return Err(io::Error::new(
                            io::ErrorKind::InvalidData,
                            "join with a token that does not match the registered one",
                        ));
                    }
                    Err(JoinRejection::TooManyWaitingFromSource) => {
                        log_rejection(format_args!(
                            "WARN rejected join for {name} from {peer_ip}: already has \
                             {MAX_WAITING_PER_SOURCE_IP} Waiting/Joining registrations \
                             outstanding from this source"
                        ));
                        return Err(io::Error::new(
                            io::ErrorKind::InvalidData,
                            "too many pending joins already registered from this source",
                        ));
                    }
                    Err(JoinRejection::MigrateMessageTooLarge { message_len }) => {
                        log_rejection(format_args!(
                            "WARN rejected join for {name}: the M message this join would \
                             require ({message_len} bytes) exceeds nanocached-node's own \
                             request-size cap ({NODE_MAX_REQUEST_SIZE} bytes) — the registry \
                             is too large to admit another node without nodes rejecting the \
                             handoff outright; see NODE_MAX_REQUEST_SIZE"
                        ));
                        return Err(io::Error::new(
                            io::ErrorKind::InvalidData,
                            "registry too large to admit a new node: the resulting M message \
                             would exceed the node-side request cap",
                        ));
                    }
                    Err(JoinRejection::TooManyWaitingTotal) => {
                        log_rejection(format_args!(
                            "WARN rejected join for {name} from {peer_ip}: \
                             {MAX_WAITING_TOTAL} Waiting/Joining registrations are already \
                             outstanding cluster-wide"
                        ));
                        return Err(io::Error::new(
                            io::ErrorKind::InvalidData,
                            "too many pending joins already registered cluster-wide",
                        ));
                    }
                };

                // Only now that the join is staged does this connection own
                // `name`, so its death runs `on_node_connection_ended`.
                *connection_name
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner()) =
                    Some((name.clone(), connection_id));

                wait_for_promotion(&mut stream, &registry, &name, promoted, shutdown_rx.clone())
                    .await?;

                continue;
            }
            Ok(DiscoveryCommand::Announce {
                name,
                port,
                token,
                rejoin_capable,
            }) => {
                let addr = format!("{peer_ip}:{port}");

                // A node that was evicted and now announces itself again
                // holds a cache the cluster has moved on from: its keys
                // were re-replicated to other owners and took writes
                // there. Put straight back in the ring it would serve its
                // old values and, once the survivors' copies stop being
                // owned (the reclaim pass), delete the newer ones. So it
                // is not registered: it is told to clear its cache and
                // join through `J`, whose handoff fills it from the
                // owners. Only on a positive record of the departure
                // (`record_departed`) — a restarted discovery has none
                // and re-registers its nodes at once — and only for a node
                // that said it can (`rejoin_capable`): an older node would
                // read the reply as a failure and ask again forever.
                if rejoin_capable && announce_needs_rejoin(&registry, &name) {
                    log_announce(format_args!(
                        "INFO node announced after its eviction: {name} must clear its cache and \
                         join again"
                    ));
                    write_response(&mut stream, b"G\n").await?;
                    return Ok(());
                }

                // Issue: `announce_insert_allowed` (only reached for a
                // genuinely new name, below) is a *different* mutex
                // (`AnnounceLimiter`) than the registry's, and when its
                // own map is full it does a bounded-but-non-trivial
                // linear eviction scan (`MAX_ANNOUNCE_LIMITER_ENTRIES`).
                // Running that scan while still holding the registry
                // lock would block every other connection's registry
                // access (heartbeats, `L`, other joins) for no reason,
                // since the limiter's own state needs no protection from
                // the registry lock. So: an existing-name outcome (token
                // check / mid-join rejection / refresh) is a read-modify-
                // write on that one entry and stays atomic under a single
                // lock acquisition; only a genuinely new name drops the
                // lock to consult the limiter, then re-acquires to
                // insert — re-validated against fresh state, since
                // another connection could have changed either while the
                // lock was released.
                let rejection = 'decide: {
                    let existing = {
                        let mut guard = lock(&registry);
                        let outcome =
                            apply_announce_to_existing(&mut guard, &name, &addr, &token, peer_ip);
                        // Existing entry refreshed — bump only if its
                        // address actually moved (issue #95). Issue #279:
                        // bumped here, still under `guard`, rather than
                        // after it drops below.
                        if let Some(Ok(true)) = outcome {
                            bump_roster(&registry);
                        }
                        outcome
                    };
                    match existing {
                        Some(Ok(_address_changed)) => {
                            break 'decide Ok(false);
                        }
                        Some(Err(reason)) => break 'decide Err(reason),
                        None => {}
                    }

                    // Short-circuits on `at_capacity` so a registry
                    // that's already full doesn't also spend a limiter
                    // slot on a source it's about to reject anyway.
                    if lock(&registry).len() >= MAX_REGISTRY_SIZE {
                        break 'decide Err("registry is full");
                    }

                    // Issue #1100: a join that has already handed off to
                    // `MAX_LATE_MEMBERS_PER_JOIN` late members takes no
                    // more new names until it ends. Before the limiter
                    // call so a refused `P` doesn't spend a cooldown slot.
                    if late_handoffs_exhausted(&current_join) {
                        break 'decide Err("too many nodes announced while a join is pending; \
                                           retry shortly");
                    }

                    // Issue: `P` holds no connection open the way `J`
                    // does, so nothing else bounds how fast one source
                    // can grow the registry toward `MAX_REGISTRY_SIZE`
                    // via connect -> `A` -> `P` -> disconnect, repeated
                    // under a fresh name each time. Only reached for a
                    // genuinely new name (checked above) — a refresh of
                    // an existing entry never touches the limiter, so a
                    // legitimate node's own reconnect/re-announce cadence
                    // is unaffected no matter how frequent. Deliberately
                    // called with the registry lock NOT held (see this
                    // arm's own comment above).
                    if !announce_insert_allowed(
                        &config.announce_limiter,
                        peer_ip,
                        AnnounceKind::Node,
                    ) {
                        log_rejection(format_args!(
                            "WARN rejected announce for {name} from {peer_ip}: more than \
                             {ANNOUNCE_INSERT_BURST} new registrations from this source at \
                             once; one more is allowed every {}s (see \
                             ANNOUNCE_INSERT_BURST / ANNOUNCE_INSERT_COOLDOWN)",
                            ANNOUNCE_INSERT_COOLDOWN.as_secs()
                        ));
                        break 'decide Err("too many new announces from this source; retry \
                                            shortly");
                    }

                    // Re-validate: another connection may have registered
                    // this exact name, or filled the registry, while the
                    // lock above was released for the limiter call.
                    let mut guard = lock(&registry);
                    match apply_announce_to_existing(&mut guard, &name, &addr, &token, peer_ip) {
                        Some(Ok(address_changed)) => {
                            // A refreshed existing entry — bump only if its
                            // address actually moved (issue #95).
                            if address_changed {
                                bump_roster(&registry);
                            }
                            break 'decide Ok(false);
                        }
                        Some(Err(reason)) => break 'decide Err(reason),
                        None => {}
                    }
                    if guard.len() >= MAX_REGISTRY_SIZE {
                        break 'decide Err("registry is full");
                    }

                    log_announce(format_args!(
                        "INFO node announced: {name} at {addr} (re-registered)"
                    ));
                    forget_departed(&registry, &name);
                    guard.insert(
                        name.clone(),
                        NodeInfo::new(addr.clone(), NodeState::Joined, token.clone()),
                    );
                    // A newly (re-)admitted `Joined` node changes the roster.
                    bump_roster(&registry);
                    Ok(true)
                };

                let newly_admitted = match rejection {
                    Ok(newly_admitted) => newly_admitted,
                    Err(reason) => {
                        return Err(io::Error::new(io::ErrorKind::InvalidData, reason));
                    }
                };
                if newly_admitted {
                    hand_off_from_late_member(
                        &registry,
                        &current_join,
                        &config.auth_secret,
                        &config.tls_connector,
                        config.replication,
                        &name,
                        &addr,
                        &token,
                    );
                }

                // Same bookkeeping as `J`, and — like `J` — only now that
                // the announce has actually been accepted does this
                // connection own `name`, so its death runs
                // `on_node_connection_ended` (see the rejection arms
                // above for why not any earlier). The connection id
                // recorded here is never actually consulted — a `P`-
                // registered node is always `Joined`, and
                // `on_node_connection_ended` never removes a `Joined`
                // entry regardless of ownership — but is included for
                // uniformity with the `J` path.
                *connection_name
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner()) =
                    Some((name.clone(), connection_id));

                write_response(&mut stream, b"R\n").await?;
                continue;
            }
            Ok(DiscoveryCommand::Complete {
                name,
                joining_name,
                token,
                generation,
            }) => {
                let completed = handle_complete(
                    &registry,
                    &current_join,
                    &name,
                    &joining_name,
                    &token,
                    generation,
                )
                .await;
                // Ack first, then start the next waiting node (issue
                // #1029): the `M` fan-out below can take up to three
                // 10 s attempts against a slow ready node, and the
                // reporter waits only about 10 s for this ack. The
                // fan-out runs whether or not the ack was delivered — a
                // reporter that gave up on it has still handed off.
                let acked = write_response(&mut stream, b"A\n").await;
                if completed {
                    try_begin_next_join(
                        &registry,
                        &current_join,
                        &config.auth_secret,
                        &config.tls_connector,
                        config.replication,
                        config.list_ready_at,
                    )
                    .await;
                }
                acked?;
                continue;
            }
            Err(ParseError::Incomplete) => {}
            Err(error) => {
                // `Ed\n` before the close, as for a failed `A` and for a
                // command sent before authenticating: the protocol page
                // lists an unknown command or a malformed request as `Ed`,
                // and a client that gets nothing but EOF cannot tell this
                // from a dropped connection. Best effort — the peer may be
                // gone already, and the parse error is what gets logged.
                // Then what the client is still sending is read before
                // the close, so a reset cannot discard the reply.
                let _ = write_response(&mut stream, b"Ed\n").await;
                drain_rejected_request(&mut stream).await;
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("{error:?}"),
                ));
            }
        }

        // Issue #6: checked here — only once `parse` has drained every
        // complete command already buffered (an `Incomplete` result means
        // there isn't one) — rather than at the top of the loop, so a
        // shutdown signal that arrives mid-pipeline doesn't silently drop
        // a second/third request that arrived in the same read as the
        // first and needs no further I/O to answer. Mirrors
        // `nanocached-node`'s own fix in `src/server.rs`.
        if *shutdown_rx.borrow() {
            return Ok(());
        }

        received.reserve(READ_CHUNK_SIZE);

        // The deadline above was set when the last command was parsed, but
        // its handler may have run past it: a node that sat in the join
        // queue for over `idle_timeout` is promoted, answered `R`, and then
        // would have been closed here as idle before its first `H` could
        // possibly arrive (it self-heals by redialing and sending `P`,
        // costing a reconnect and a WARN). The idle clock for the next
        // command starts when this one's handler is done. Once, per
        // handled command — never on a bare read, so trickling bytes of the
        // *next* command still can't push the deadline out.
        if handled_a_command {
            deadline = Instant::now() + config.idle_timeout;
            handled_a_command = false;
        }

        // Issue (slowloris): bound this read by whichever of the ordinary
        // per-read idle timeout and the connection's total unidentified
        // lifetime is tighter. Both `deadline` and `unidentified_deadline`
        // are fixed points in time (neither resets on a bare read, unlike
        // an earlier version of `deadline`), so no amount of trickling
        // small reads in just under either extends the bound — see
        // `deadline`'s own doc comment above.
        let read_deadline = if identified {
            deadline
        } else {
            deadline.min(unidentified_deadline)
        };

        let bytes_read = tokio::select! {
            _ = shutdown_rx.changed() => return Ok(()),

            result = timeout_at(read_deadline, stream.read_buf(&mut received)) => {
                result.map_err(|_| {
                    if !identified && Instant::now() >= unidentified_deadline {
                        io::Error::new(
                            io::ErrorKind::TimedOut,
                            "connection did not complete a single command within the \
                             unidentified-connection timeout",
                        )
                    } else {
                        io::Error::new(io::ErrorKind::TimedOut, "connection idle timeout")
                    }
                })??
            }
        };

        if bytes_read == 0 {
            if received.is_empty() {
                return Ok(());
            }

            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "connection closed before request was complete",
            ));
        }

        if received.len() > MAX_REQUEST_SIZE {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "request is too large",
            ));
        }
    }
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> ExitCode {
    rustls::crypto::ring::default_provider()
        .install_default()
        .expect("no other rustls crypto provider is installed this early in the process");

    let args = match parse_args() {
        Ok(args) => args,
        Err(ArgsError::Help(message)) => {
            println!("{message}");
            return ExitCode::SUCCESS;
        }
        Err(ArgsError::Invalid(message)) => {
            eprintln!("{message}");
            return ExitCode::FAILURE;
        }
    };

    let tls_acceptor = match (&args.tls_cert, &args.tls_key) {
        (Some(cert), Some(key)) => match load_tls_acceptor(cert, key) {
            Ok(acceptor) => Some(acceptor),
            Err(err) => {
                eprintln!("discovery: {err}");
                return ExitCode::FAILURE;
            }
        },
        _ => None,
    };

    let tls_connector = match &args.tls_ca {
        Some(ca) => match load_tls_connector(ca) {
            Ok(connector) => Some(connector),
            Err(err) => {
                eprintln!("discovery: {err}");
                return ExitCode::FAILURE;
            }
        },
        None => None,
    };

    // Inbound and outbound TLS are configured independently (this server
    // is dialled by nodes/clients *and* dials nodes for `M`/`X`, which
    // carry the recipient's membership token). Half-configuring it is
    // easy to do and otherwise silent.
    match (&tls_acceptor, &tls_connector) {
        (Some(_), None) => eprintln!(
            "discovery: WARN TLS is enabled for inbound connections only (no --tls-ca): \
             outbound M/X to nodes — and the membership tokens they carry — go out in plaintext"
        ),
        (None, Some(_)) => eprintln!(
            "discovery: WARN TLS is enabled for outbound connections only (no --tls-cert/--tls-key): \
             inbound J/P/H/L — and the tokens and secrets they carry — arrive in plaintext"
        ),
        _ => {}
    }

    let address = format!("{}:{}", args.host, args.port);
    let auth_secret = match read_auth_secret() {
        Ok(secret) => secret,
        Err(message) => {
            eprintln!("discovery: {message}");
            return ExitCode::FAILURE;
        }
    };
    if let Err(err) = run(
        &address,
        args.liveness_timeout,
        // The startup grace (discovery HA) is the liveness window by
        // definition: it exists so every live member has had time to
        // re-announce before L is served, and that time IS the liveness
        // timeout. Not separately configurable.
        args.liveness_timeout,
        args.replication_factor,
        auth_secret,
        tls_acceptor,
        tls_connector,
        args.metrics_port
            .map(|port| format!("{}:{port}", args.host)),
    )
    .await
    {
        eprintln!("discovery: {err}");
        return ExitCode::FAILURE;
    }

    ExitCode::SUCCESS
}

#[cfg(test)]
mod tests {
    use super::*;
    use nanocached::infra::server_name_from_addr;
    use rustls::pki_types::{PrivateKeyDer, ServerName};
    use rustls::{ClientConfig, RootCertStore, ServerConfig};

    #[test]
    fn parse_reports_incomplete_before_the_header_is_fully_buffered() {
        let mut input = BytesMut::from(&b"H 9 2"[..]);
        assert_eq!(parse(&mut input), Err(ParseError::Incomplete));
    }

    #[test]
    fn parse_reports_incomplete_while_the_field_body_is_still_arriving() {
        let mut input = BytesMut::from(&b"H 9 2 5\n1.2.3"[..]);
        assert_eq!(parse(&mut input), Err(ParseError::Incomplete));
    }

    #[test]
    fn parse_reads_a_heartbeat_and_consumes_only_that_frame() {
        let mut input = BytesMut::from(&b"H 9 2 5\nsome-nametok-aL\n"[..]);
        let command = parse(&mut input).unwrap();
        assert_eq!(
            command,
            DiscoveryCommand::Heartbeat {
                name: "some-name".to_string(),
                replication: Some(2),
                token: "tok-a".to_string(),
                pending_joiner: None,
            }
        );
        assert_eq!(&input[..], b"L\n");
    }

    #[test]
    fn parse_reads_a_heartbeat_with_zero_replication_as_unknown() {
        // `0` is the wire sentinel for "this node has no belief yet"
        // (issue #30) — never a real replication factor.
        let mut input = BytesMut::from(&b"H 9 0 5\nsome-nametok-a"[..]);
        let command = parse(&mut input).unwrap();
        assert_eq!(
            command,
            DiscoveryCommand::Heartbeat {
                name: "some-name".to_string(),
                replication: None,
                token: "tok-a".to_string(),
                pending_joiner: None,
            }
        );
    }

    #[test]
    fn parse_rejects_a_heartbeat_missing_the_replication_field() {
        let mut input = BytesMut::from(&b"H 9\nsome-name"[..]);
        assert_eq!(parse(&mut input), Err(ParseError::InvalidLength));
    }

    #[test]
    fn parse_rejects_a_heartbeat_missing_the_token_field() {
        // The pre-issue-#34 frame shape: no token length in the header.
        let mut input = BytesMut::from(&b"H 9 2\nsome-name"[..]);
        assert_eq!(parse(&mut input), Err(ParseError::InvalidLength));
    }

    #[test]
    fn parse_rejects_a_heartbeat_with_an_empty_token() {
        let mut input = BytesMut::from(&b"H 9 2 0\nsome-name"[..]);
        assert_eq!(parse(&mut input), Err(ParseError::EmptyField));
    }

    #[test]
    fn parse_rejects_a_heartbeat_whose_name_exceeds_max_name_length() {
        // Both a node's name and token are v4 UUIDs (36 bytes) in
        // practice (node identity decoupled from address, issue #34) — MAX_NAME_LENGTH/
        // MAX_TOKEN_LENGTH bound the field at parse time regardless, so
        // an oversized declared length can't bloat a registry entry or
        // every `L`/`M` response that lists it.
        let long_name = "n".repeat(MAX_NAME_LENGTH + 1);
        let header = format!("H {} 0 5\n", long_name.len());
        let mut input = BytesMut::from(format!("{header}{long_name}tok-a").as_bytes());
        assert_eq!(parse(&mut input), Err(ParseError::InvalidLength));
    }

    #[test]
    fn parse_rejects_a_heartbeat_whose_token_exceeds_max_token_length() {
        let long_token = "t".repeat(MAX_TOKEN_LENGTH + 1);
        let header = format!("H 9 0 {}\n", long_token.len());
        let mut input = BytesMut::from(format!("{header}some-name{long_token}").as_bytes());
        assert_eq!(parse(&mut input), Err(ParseError::InvalidLength));
    }

    #[test]
    fn parse_accepts_a_heartbeat_with_fields_at_exactly_the_length_bounds() {
        // Off-by-one check for the bounds above: exactly at the limit
        // must still parse.
        let name = "n".repeat(MAX_NAME_LENGTH);
        let token = "t".repeat(MAX_TOKEN_LENGTH);
        let header = format!("H {} 0 {}\n", name.len(), token.len());
        let mut input = BytesMut::from(format!("{header}{name}{token}").as_bytes());
        assert_eq!(
            parse(&mut input),
            Ok(DiscoveryCommand::Heartbeat {
                name,
                replication: None,
                token,
                pending_joiner: None,
            })
        );
    }

    #[test]
    fn parse_rejects_a_complete_whose_joining_name_exceeds_max_name_length() {
        let long_joining_name = "j".repeat(MAX_NAME_LENGTH + 1);
        let header = format!("C 9 {} 5\n", long_joining_name.len());
        let mut input =
            BytesMut::from(format!("{header}some-name{long_joining_name}tok-a").as_bytes());
        assert_eq!(parse(&mut input), Err(ParseError::InvalidLength));
    }

    #[test]
    fn parse_rejects_a_heartbeat_with_trailing_arguments() {
        let mut input = BytesMut::from(&b"H 9 2 5 8 extra\nsome-nametok-ajoiner-0"[..]);
        assert_eq!(parse(&mut input), Err(ParseError::InvalidLength));
    }

    #[test]
    fn parse_reads_a_heartbeat_with_a_pending_joiner() {
        // Issue #571: the optional fourth header field and trailing body
        // field name the joiner this node is waiting to see in a roster.
        let mut input = BytesMut::from(&b"H 9 2 5 8\nsome-nametok-ajoiner-0L\n"[..]);
        assert_eq!(
            parse(&mut input).unwrap(),
            DiscoveryCommand::Heartbeat {
                name: "some-name".to_string(),
                replication: Some(2),
                token: "tok-a".to_string(),
                pending_joiner: Some("joiner-0".to_string()),
            }
        );
        assert_eq!(&input[..], b"L\n");
    }

    #[test]
    fn a_pending_joiner_is_gone_only_while_recorded_departed_and_not_registered_again() {
        let registry: Registry = Arc::new(RegistryState::default());
        // Never heard of: not known to be gone (a restarted replica's
        // empty registry must not abandon live joins).
        assert_eq!(pending_joiner_verdict(&registry, "joiner-0"), b"K\n");
        record_departed(&registry, "joiner-0");
        assert_eq!(pending_joiner_verdict(&registry, "joiner-0"), b"G\n");
        // Registered again under the same name (an abandoned join retrying).
        lock(&registry).insert(
            "joiner-0".to_string(),
            NodeInfo::new(
                "127.0.0.1:1".to_string(),
                NodeState::Waiting,
                "tok".to_string(),
            ),
        );
        assert_eq!(pending_joiner_verdict(&registry, "joiner-0"), b"K\n");
        forget_departed(&registry, "joiner-0");
        lock(&registry).remove("joiner-0");
        assert_eq!(pending_joiner_verdict(&registry, "joiner-0"), b"K\n");
    }

    #[test]
    fn recording_many_departures_back_to_back_stays_cheap() {
        // Issue #1201: every call used to scan the whole departed map
        // (age prune, then an oldest search once full), so recording the
        // 65,536 evictions of a mass expiry in one sweep tick took 3.3 s on
        // a release build, with the runtime unable to serve anything
        // meanwhile. Amortised O(1) per call finishes in milliseconds;
        // the bound leaves a wide margin for a slow CI debug build.
        let registry: Registry = Arc::new(RegistryState::default());
        let started = Instant::now();
        for index in 0..MAX_REGISTRY_SIZE {
            record_departed(&registry, &format!("node-{index}"));
        }
        let elapsed = started.elapsed();
        assert!(
            elapsed < Duration::from_secs(2),
            "recording {MAX_REGISTRY_SIZE} departures took {elapsed:?}"
        );
        // The cap still holds, and it is the newest names that are kept.
        assert_eq!(pending_joiner_verdict(&registry, "node-0"), b"K\n");
        let newest = format!("node-{}", MAX_REGISTRY_SIZE - 1);
        assert_eq!(pending_joiner_verdict(&registry, &newest), b"G\n");
    }

    #[test]
    fn departed_keeps_the_newest_names_and_drops_expired_and_forgotten_ones() {
        let base = Instant::now();
        let at = |millis: u64| base + Duration::from_millis(millis);
        let mut departed = Departed::default();

        // Over the cap, the oldest record goes first; a name recorded
        // again counts as new, so it outlives names recorded after its
        // first record.
        for index in 0..MAX_DEPARTED {
            departed.record(&format!("n{index}"), at(index as u64));
        }
        departed.record("n0", at(MAX_DEPARTED as u64));
        departed.record("extra", at(MAX_DEPARTED as u64 + 1));
        assert!(
            departed.departed_at("n0").is_some(),
            "refreshed, not oldest"
        );
        assert!(
            departed.departed_at("n1").is_none(),
            "oldest current record"
        );
        assert!(departed.departed_at("n2").is_some());
        assert!(departed.departed_at("extra").is_some());
        assert_eq!(departed.at.len(), MAX_DEPARTED);

        // A forgotten name frees its place without leaving a record, and
        // its stale queue entry is skipped rather than evicting a live one.
        departed.forget("n2");
        departed.record("again", at(MAX_DEPARTED as u64 + 2));
        assert!(departed.departed_at("n3").is_some());
        assert!(departed.departed_at("n2").is_none());
        assert_eq!(departed.at.len(), MAX_DEPARTED);

        // Everything older than the retention is gone on the next record.
        let later = at(MAX_DEPARTED as u64 + 2) + DEPARTED_RETENTION - Duration::from_millis(1);
        departed.record("fresh", later);
        assert_eq!(
            departed.at.len(),
            2,
            "only `again` (just inside) and `fresh`"
        );
        assert!(departed.departed_at("again").is_some());
        assert!(departed.departed_at("fresh").is_some());
    }

    #[test]
    fn a_departure_judged_by_a_stalled_sweep_does_not_earn_a_rejoin() {
        let mut departed = Departed::default();
        let now = Instant::now();
        departed.record("evicted", now);
        departed.record_unreliable("stalled", now);
        assert!(departed.earns_rejoin("evicted"));
        assert!(!departed.earns_rejoin("stalled"));
        assert!(!departed.earns_rejoin("never-seen"));
        // Still on record as gone (the pending-joiner question).
        assert!(departed.departed_at("stalled").is_some());
        // Recorded again, this time by a sweep that was running: it earns it.
        departed.record("stalled", now);
        assert!(departed.earns_rejoin("stalled"));
        // Forgotten (it registered again) and gone from both.
        departed.record_unreliable("again", now);
        departed.forget("again");
        assert!(departed.departed_at("again").is_none() && departed.unreliable.is_empty());
    }

    #[test]
    fn the_unreliable_set_follows_the_departed_records_out() {
        let mut departed = Departed::default();
        let base = Instant::now();
        departed.record_unreliable("first", base);
        for index in 0..MAX_DEPARTED {
            departed.record(
                &format!("n{index}"),
                base + Duration::from_nanos(index as u64 + 1),
            );
        }
        // "first" was the oldest and went out when the cap was reached.
        assert!(departed.departed_at("first").is_none());
        assert!(departed.unreliable.is_empty());
    }

    #[test]
    fn a_sweep_is_stalled_when_it_comes_much_later_than_the_ticks_a_second_apart() {
        let last = Instant::now();
        let liveness = Duration::from_secs(15);
        assert!(!sweep_was_stalled(
            last,
            last + Duration::from_secs(1),
            liveness
        ));
        assert!(!sweep_was_stalled(
            last,
            last + Duration::from_millis(7500),
            liveness
        ));
        assert!(sweep_was_stalled(
            last,
            last + Duration::from_millis(7501),
            liveness
        ));
        assert!(sweep_was_stalled(
            last,
            last + Duration::from_secs(25),
            liveness
        ));
    }

    #[test]
    fn departed_queue_stays_bounded_when_one_name_is_recorded_repeatedly() {
        let base = Instant::now();
        let mut departed = Departed::default();
        for tick in 0..(10 * MAX_DEPARTED) {
            departed.record("flapping", base + Duration::from_nanos(tick as u64));
            assert!(departed.order.len() <= 2 * MAX_DEPARTED + 1);
        }
        assert_eq!(departed.at.len(), 1);
    }

    #[test]
    fn parse_reads_a_join_command_and_consumes_only_that_frame() {
        let mut input = BytesMut::from(&b"J 9 8356 5\nsome-nametok-aL\n"[..]);
        let command = parse(&mut input).unwrap();
        assert_eq!(
            command,
            DiscoveryCommand::Join {
                name: "some-name".to_string(),
                port: 8356,
                token: "tok-a".to_string(),
            }
        );
        assert_eq!(&input[..], b"L\n");
    }

    #[test]
    fn parse_reports_incomplete_while_a_joins_second_field_is_still_arriving() {
        let mut input = BytesMut::from(&b"J 9 8356 5\nsome-na"[..]);
        assert_eq!(parse(&mut input), Err(ParseError::Incomplete));
    }

    #[test]
    fn parse_leaves_input_untouched_when_a_joins_second_field_is_incomplete() {
        let original = b"J 9 8356 5\nsome-nametok".to_vec();
        let mut input = BytesMut::from(&original[..]);
        assert_eq!(parse(&mut input), Err(ParseError::Incomplete));
        assert_eq!(&input[..], &original[..]);
    }

    #[test]
    fn parse_reads_a_complete_command() {
        let mut input = BytesMut::from(&b"C 9 6 5\nsome-namejoinertok-aL\n"[..]);
        let command = parse(&mut input).unwrap();
        assert_eq!(
            command,
            DiscoveryCommand::Complete {
                name: "some-name".to_string(),
                joining_name: "joiner".to_string(),
                token: "tok-a".to_string(),
                generation: None,
            }
        );
        assert_eq!(&input[..], b"L\n");
    }

    #[test]
    fn parse_reads_a_complete_command_with_a_trailing_generation_field() {
        // Tenth-pass audit (2026-09-02), issue #421(c): a node new
        // enough to have received a generation on its own `M` echoes it
        // back as a fourth, plain (not length-prefixed) `C` field.
        let mut input = BytesMut::from(&b"C 9 6 5 3\nsome-namejoinertok-aL\n"[..]);
        let command = parse(&mut input).unwrap();
        assert_eq!(
            command,
            DiscoveryCommand::Complete {
                name: "some-name".to_string(),
                joining_name: "joiner".to_string(),
                token: "tok-a".to_string(),
                generation: Some(3),
            }
        );
        assert_eq!(&input[..], b"L\n");
    }

    #[test]
    fn parse_reads_a_list_command() {
        let mut input = BytesMut::from(&b"L\n"[..]);
        assert_eq!(parse(&mut input), Ok(DiscoveryCommand::List));
        assert!(input.is_empty());
    }

    #[test]
    fn parse_rejects_list_with_trailing_arguments() {
        let mut input = BytesMut::from(&b"L extra\n"[..]);
        assert_eq!(parse(&mut input), Err(ParseError::InvalidLength));
    }

    #[test]
    fn parse_rejects_an_empty_field() {
        let mut input = BytesMut::from(&b"H 0 2 5\n"[..]);
        assert_eq!(parse(&mut input), Err(ParseError::EmptyField));
    }

    #[test]
    fn parse_rejects_port_zero_in_join() {
        // Port 0 can never be served on — addresses derived from the registration connection derives the address
        // from source IP + this port, so a zero here is protocol garbage.
        let mut input = BytesMut::from(&b"J 9 0 5\nsome-nametok-a"[..]);
        assert_eq!(parse(&mut input), Err(ParseError::InvalidLength));
    }

    #[test]
    fn parse_rejects_a_join_name_containing_a_control_character() {
        // issue #192: a `\n` smuggled through a name would forge extra
        // log lines at the "node registered"/"node announced" print sites.
        let mut input = BytesMut::from(&b"J 9 8356 5\nsome\nnametok-a"[..]);
        assert_eq!(parse(&mut input), Err(ParseError::ControlCharacter));
    }

    #[test]
    fn parse_rejects_a_heartbeat_token_containing_a_control_character() {
        // issue #192: the token field is bound to the same rejection as
        // the name field, even though it isn't logged today.
        let mut input = BytesMut::from(&b"H 9 2 5\nsome-nameto\x7f-a"[..]);
        assert_eq!(parse(&mut input), Err(ParseError::ControlCharacter));
    }

    #[test]
    fn parse_rejects_a_complete_joining_name_containing_a_control_character() {
        // issue #192: `joining_name` reaches the "handoff completed" log
        // line too, so the `C` command gets the same rejection.
        let mut input = BytesMut::from(&b"C 9 6 5\nsome-namejoin\x01rtok-a"[..]);
        assert_eq!(parse(&mut input), Err(ParseError::ControlCharacter));
    }

    #[test]
    fn contains_control_character_accepts_ordinary_printable_names() {
        assert!(!contains_control_character("some-name-v4-uuid"));
    }

    #[test]
    fn parse_rejects_a_non_numeric_length() {
        let mut input = BytesMut::from(&b"H x 2 5\n"[..]);
        assert_eq!(parse(&mut input), Err(ParseError::InvalidLength));
    }

    #[test]
    fn parse_rejects_invalid_utf8_fields() {
        let mut input = BytesMut::from(&b"H 2 2 2\n\xff\xfetk"[..]);
        assert_eq!(parse(&mut input), Err(ParseError::InvalidUtf8));
    }

    #[test]
    fn parse_rejects_an_unknown_command() {
        let mut input = BytesMut::from(&b"X\n"[..]);
        assert_eq!(parse(&mut input), Err(ParseError::InvalidCommand));
    }

    #[test]
    fn parse_reads_an_auth_command() {
        let mut input = BytesMut::from(&b"A 6\nsecretL\n"[..]);
        assert_eq!(
            parse(&mut input),
            Ok(DiscoveryCommand::Auth {
                secret: Bytes::from_static(b"secret"),
                tagging: false,
            })
        );
        assert_eq!(&input[..], b"L\n");
    }

    #[test]
    fn parse_rejects_an_empty_secret() {
        let mut input = BytesMut::from(&b"A 0\n"[..]);
        assert_eq!(parse(&mut input), Err(ParseError::EmptySecret));
    }

    #[test]
    fn constant_time_eq_matches_identical_byte_strings() {
        assert!(constant_time_eq(b"same-secret", b"same-secret"));
    }

    #[test]
    fn constant_time_eq_rejects_different_content_of_the_same_length() {
        assert!(!constant_time_eq(b"secret-one", b"secret-two"));
    }

    #[test]
    fn constant_time_eq_rejects_different_lengths() {
        assert!(!constant_time_eq(b"short", b"a much longer value"));
    }

    #[test]
    fn waiting_timeout_for_scales_by_queue_position_and_never_shrinks_below_one_join() {
        // Only one join runs cluster-wide at a time, and each is itself
        // bounded by MIGRATION_TIMEOUT_MAX — so a node queued behind N-1
        // others can legitimately need close to N times that long. A
        // node with no one ahead of it (position 1) still gets a full
        // MIGRATION_TIMEOUT_MAX, not zero.
        assert_eq!(
            waiting_timeout_for(1),
            MIGRATION_TIMEOUT_MAX + WAITING_TIMEOUT_MARGIN
        );
        assert_eq!(
            waiting_timeout_for(3),
            MIGRATION_TIMEOUT_MAX * 3 + WAITING_TIMEOUT_MARGIN
        );
        assert!(waiting_timeout_for(1) < waiting_timeout_for(2));
    }

    #[test]
    fn waiting_timeout_for_saturates_at_max_waiting_timeout_positions() {
        // Issue: join-queue starvation — without a cap on the multiplier,
        // a deep queue's tail could wait an implausibly long multiple of
        // `MIGRATION_TIMEOUT_MAX`. Positions at and beyond
        // `MAX_WAITING_TIMEOUT_POSITIONS` must all get the same, capped
        // bound rather than continuing to scale.
        assert_eq!(
            waiting_timeout_for(MAX_WAITING_TIMEOUT_POSITIONS),
            MIGRATION_TIMEOUT_MAX * MAX_WAITING_TIMEOUT_POSITIONS as u32 + WAITING_TIMEOUT_MARGIN
        );
        assert_eq!(
            waiting_timeout_for(MAX_WAITING_TIMEOUT_POSITIONS + 1),
            waiting_timeout_for(MAX_WAITING_TIMEOUT_POSITIONS),
            "a queue position past the cap must not keep scaling the bound"
        );
        assert_eq!(
            waiting_timeout_for(MAX_WAITING_TOTAL),
            waiting_timeout_for(MAX_WAITING_TIMEOUT_POSITIONS),
            "even the deepest position MAX_WAITING_TOTAL permits stays at the cap"
        );
    }

    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn announce_insert_allowed_blocks_within_the_cooldown_then_allows_after() {
        let limiter: AnnounceLimiter = Arc::new(Mutex::new(FxHashMap::default()));
        let ip = std::net::IpAddr::V4(std::net::Ipv4Addr::new(10, 0, 0, 1));
        let other_ip = std::net::IpAddr::V4(std::net::Ipv4Addr::new(10, 0, 0, 2));

        // A source may spend its whole burst at once...
        for _ in 0..ANNOUNCE_INSERT_BURST {
            assert!(announce_insert_allowed(&limiter, ip, AnnounceKind::Node));
        }
        // ...the next attempt within the cooldown is refused...
        assert!(!announce_insert_allowed(&limiter, ip, AnnounceKind::Node));
        // ...but a different source is entirely unaffected by it.
        assert!(announce_insert_allowed(
            &limiter,
            other_ip,
            AnnounceKind::Node
        ));

        tokio::time::advance(ANNOUNCE_INSERT_COOLDOWN + Duration::from_millis(1)).await;

        // Once the cooldown has elapsed, the original source has earned
        // exactly one more.
        assert!(announce_insert_allowed(&limiter, ip, AnnounceKind::Node));
        assert!(!announce_insert_allowed(&limiter, ip, AnnounceKind::Node));

        // Refills accrue while the source is quiet, up to the burst and
        // no further.
        tokio::time::advance(ANNOUNCE_INSERT_COOLDOWN * (ANNOUNCE_INSERT_BURST + 5)).await;
        for _ in 0..ANNOUNCE_INSERT_BURST {
            assert!(announce_insert_allowed(&limiter, ip, AnnounceKind::Node));
        }
        assert!(!announce_insert_allowed(&limiter, ip, AnnounceKind::Node));
    }

    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn node_and_proxy_registrations_from_one_source_have_separate_bursts() {
        // Issue #1798: `P` and `Y` spent one shared bucket per source, so
        // proxies re-registering after a discovery restart could use up
        // the burst the nodes on the same host (or NAT address, or /64)
        // need inside the startup grace, and the reverse.
        let limiter: AnnounceLimiter = Arc::new(Mutex::new(FxHashMap::default()));
        let ip = std::net::IpAddr::V4(std::net::Ipv4Addr::new(10, 0, 0, 1));

        for _ in 0..ANNOUNCE_INSERT_BURST {
            assert!(announce_insert_allowed(&limiter, ip, AnnounceKind::Proxy));
        }
        assert!(!announce_insert_allowed(&limiter, ip, AnnounceKind::Proxy));
        for _ in 0..ANNOUNCE_INSERT_BURST {
            assert!(
                announce_insert_allowed(&limiter, ip, AnnounceKind::Node),
                "a spent proxy burst must not limit node registrations"
            );
        }
        assert!(!announce_insert_allowed(&limiter, ip, AnnounceKind::Node));

        tokio::time::advance(ANNOUNCE_INSERT_COOLDOWN).await;
        assert!(announce_insert_allowed(&limiter, ip, AnnounceKind::Proxy));
        assert!(announce_insert_allowed(&limiter, ip, AnnounceKind::Node));
    }

    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn a_fleet_sharing_one_source_key_re_registers_at_once() {
        // Regression (issue #1148): a plain one-per-cooldown gap let a
        // fleet that shares a source key — IPv4 nodes behind one NAT
        // address, or every node of one IPv6 /64 subnet — re-register
        // only one node per 2 s after a discovery restart, so the startup
        // grace ended with a partial registry. Up to the burst they must
        // all get in at once; beyond it the pacing still applies.
        let limiter: AnnounceLimiter = Arc::new(Mutex::new(FxHashMap::default()));
        for host in 1..=ANNOUNCE_INSERT_BURST {
            let addr: std::net::IpAddr = format!("2001:db8:1:2::{host:x}").parse().unwrap();
            assert!(
                announce_insert_allowed(&limiter, addr, AnnounceKind::Node),
                "node {host} of a /64 fleet must register in the same burst"
            );
        }
        let one_more: std::net::IpAddr = "2001:db8:1:2:ffff::1".parse().unwrap();
        assert!(
            !announce_insert_allowed(&limiter, one_more, AnnounceKind::Node),
            "the /64 is one source: past the burst it is paced"
        );
        tokio::time::advance(ANNOUNCE_INSERT_COOLDOWN).await;
        assert!(announce_insert_allowed(
            &limiter,
            one_more,
            AnnounceKind::Node
        ));
        assert_eq!(
            limiter
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .len(),
            1,
            "the whole /64 is one limiter entry"
        );
    }

    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn announce_insert_allowed_counts_an_ipv6_64_as_one_source() {
        // Regression: the cooldown was keyed by the raw peer address, so a
        // sender in one IPv6 /64 rotated its source address to walk around
        // the 2 s cooldown and fill the registry with forged entries. It
        // is keyed by `per_ip_key` now, like the waiting cap and the
        // connection cap.
        let limiter: AnnounceLimiter = Arc::new(Mutex::new(FxHashMap::default()));
        let first: std::net::IpAddr = "2001:db8:1:2::1".parse().unwrap();
        let same_64: std::net::IpAddr = "2001:db8:1:2:ffff::9".parse().unwrap();
        let other_64: std::net::IpAddr = "2001:db8:1:3::1".parse().unwrap();

        for _ in 0..ANNOUNCE_INSERT_BURST {
            assert!(announce_insert_allowed(&limiter, first, AnnounceKind::Node));
        }
        assert!(
            !announce_insert_allowed(&limiter, same_64, AnnounceKind::Node),
            "a second address in the same /64 is the same source"
        );
        assert!(
            announce_insert_allowed(&limiter, other_64, AnnounceKind::Node),
            "a different /64 is unaffected"
        );
        assert_eq!(
            limiter
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .len(),
            2,
            "the limiter holds one entry per /64"
        );
    }

    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn announce_insert_allowed_evicts_the_oldest_entry_once_the_limiter_is_full() {
        // Regression: an attacker who cycles through source addresses
        // could otherwise grow `AnnounceLimiter` itself without bound —
        // defeating the very thing it exists to bound. It must stay a
        // fixed-size, oldest-evicted map instead.
        let limiter: AnnounceLimiter = Arc::new(Mutex::new(FxHashMap::default()));

        let first_ip = std::net::IpAddr::V4(std::net::Ipv4Addr::from(1u32));
        assert!(announce_insert_allowed(
            &limiter,
            first_ip,
            AnnounceKind::Node
        ));

        // Fill the limiter to capacity with distinct addresses, spacing
        // each one out in (virtual) time so every entry has a distinct,
        // well-ordered timestamp.
        for i in 2..=MAX_ANNOUNCE_LIMITER_ENTRIES as u32 {
            tokio::time::advance(Duration::from_millis(1)).await;
            assert!(announce_insert_allowed(
                &limiter,
                std::net::IpAddr::V4(std::net::Ipv4Addr::from(i)),
                AnnounceKind::Node
            ));
        }
        assert_eq!(
            limiter
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .len(),
            MAX_ANNOUNCE_LIMITER_ENTRIES
        );

        tokio::time::advance(Duration::from_millis(1)).await;
        let overflow_ip = std::net::IpAddr::V4(std::net::Ipv4Addr::from(
            MAX_ANNOUNCE_LIMITER_ENTRIES as u32 + 1,
        ));
        assert!(announce_insert_allowed(
            &limiter,
            overflow_ip,
            AnnounceKind::Node
        ));

        let guard = limiter
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        assert_eq!(guard.len(), MAX_ANNOUNCE_LIMITER_ENTRIES);
        assert!(
            !guard.contains_key(&(first_ip, AnnounceKind::Node)),
            "the oldest entry should have been evicted to make room"
        );
        assert!(guard.contains_key(&(overflow_ip, AnnounceKind::Node)));
    }

    #[test]
    fn server_name_from_addr_strips_brackets_from_a_bracketed_ipv6_host() {
        let name = server_name_from_addr("[::1]:8356").unwrap();

        assert_eq!(name, ServerName::try_from("::1").unwrap());
    }

    #[test]
    fn server_name_from_addr_handles_a_full_bracketed_ipv6_host() {
        let name = server_name_from_addr("[2001:db8::1]:8356").unwrap();

        assert_eq!(name, ServerName::try_from("2001:db8::1").unwrap());
    }

    #[test]
    fn server_name_from_addr_still_handles_a_plain_ipv4_host() {
        let name = server_name_from_addr("127.0.0.1:8356").unwrap();

        assert_eq!(name, ServerName::try_from("127.0.0.1").unwrap());
    }

    #[test]
    fn server_name_from_addr_still_handles_a_dns_name() {
        let name = server_name_from_addr("node-a.example.com:8356").unwrap();

        assert_eq!(name, ServerName::try_from("node-a.example.com").unwrap());
    }

    #[tokio::test(flavor = "current_thread")]
    async fn join_then_heartbeat_then_list_reports_the_registered_node() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let registry: Registry = Arc::new(RegistryState::default());
        let current_join: CurrentJoin = Arc::new(Mutex::new(None));
        let (_shutdown_tx, shutdown_rx) = watch::channel(false);

        let server_registry = Arc::clone(&registry);
        let server_current_join = Arc::clone(&current_join);
        tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let _ = handle_connection(
                MaybeTls::Plain(stream),
                std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST),
                server_registry,
                server_current_join,
                ConnectionConfig {
                    idle_timeout: Duration::from_secs(5),
                    list_ready_at: Instant::now(),
                    replication: 2,
                    auth_secret: None,
                    tls_acceptor: None,
                    tls_connector: None,
                    announce_limiter: Arc::new(Mutex::new(FxHashMap::default())),
                },
                shutdown_rx,
                Arc::new(std::sync::Mutex::new(None)),
            )
            .await;
        });

        let mut client = TcpStream::connect(address).await.unwrap();
        // With no Joined nodes yet, this is the bootstrap case: the join
        // is accepted with nothing to hand off, so promotion is immediate.
        client
            .write_all(b"J 6 8356 9\nnode-atk-node-aH 6 0 9\nnode-atk-node-aL\n")
            .await
            .unwrap();

        // The three responses are written in separate calls but the client
        // may observe them coalesced into a single read, so accumulate
        // until the expected byte count has arrived instead of assuming
        // read boundaries.
        let expected = b"R\nA 1 2\n6 14\nnode-a127.0.0.1:8356\nN 1 2\n6 14\nnode-a127.0.0.1:8356\n";
        let mut received = Vec::new();
        let mut chunk = [0u8; 64];

        while received.len() < expected.len() {
            let bytes_read = client.read(&mut chunk).await.unwrap();
            assert!(bytes_read > 0, "connection closed before response arrived");
            received.extend_from_slice(&chunk[..bytes_read]);
        }

        assert_eq!(received, expected);
    }

    /// Drives one `handle_connection` over a real socket, writes `frame`,
    /// and returns everything the server sent back before it closed. Shared
    /// by the `T`-authorization regression tests below so each seeds only
    /// its registry and asserts on the bytes.
    async fn roundtrip_one_frame(registry: Registry, frame: &[u8]) -> Vec<u8> {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let current_join: CurrentJoin = Arc::new(Mutex::new(None));
        let (_shutdown_tx, shutdown_rx) = watch::channel(false);

        let server_registry = Arc::clone(&registry);
        tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let _ = handle_connection(
                MaybeTls::Plain(stream),
                std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST),
                server_registry,
                current_join,
                ConnectionConfig {
                    idle_timeout: Duration::from_secs(5),
                    list_ready_at: Instant::now(),
                    replication: 2,
                    auth_secret: None,
                    tls_acceptor: None,
                    tls_connector: None,
                    announce_limiter: Arc::new(Mutex::new(FxHashMap::default())),
                },
                shutdown_rx,
                Arc::new(std::sync::Mutex::new(None)),
            )
            .await;
        });

        let mut client = TcpStream::connect(address).await.unwrap();
        client.write_all(frame).await.unwrap();
        let mut received = Vec::new();
        client.read_to_end(&mut received).await.unwrap();
        received
    }

    #[tokio::test(flavor = "current_thread")]
    async fn t_from_a_joined_node_returns_the_roster() {
        // The happy path this authorization guard must keep working: a
        // genuinely `Joined` node presenting its own token gets the
        // token-carrying every-state roster back (issue #295).
        let registry: Registry = Arc::new(RegistryState::default());
        {
            let mut guard = lock(&registry);
            guard.insert(
                "node-j".to_string(),
                joined_node_reporting("127.0.0.1:9001", "tk-j", Some(2)),
            );
        }

        // `T <name_len> <token_len>\n<name><token>` — "node-j"/"tk-j".
        let received = roundtrip_one_frame(registry, b"T 6 4\nnode-jtk-j").await;

        assert!(
            received.starts_with(b"N "),
            "a Joined node's T must be answered with the roster, got {received:?}"
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn t_from_a_waiting_node_is_rejected_without_leaking_the_roster() {
        // Regression (pass-7 audit): `start_join` stores the joiner's own
        // self-chosen token unverified, so a `Waiting` entry proves nothing
        // about identity. `T` must apply the same `state == Joined` guard as
        // `Heartbeat` — otherwise anyone reachable by discovery could park a
        // `Waiting` entry under a token they picked and replay it here to
        // read back every member's address and token. The requester matches
        // a real registry entry by name AND token, and is still rejected: it
        // is the `Waiting` state, not a bad token, that closes the door.
        let registry: Registry = Arc::new(RegistryState::default());
        {
            let mut guard = lock(&registry);
            // A real Joined member whose token must NOT leak.
            guard.insert(
                "node-j".to_string(),
                joined_node_reporting("127.0.0.1:9001", "tk-j", Some(2)),
            );
            // The attacker's self-registered Waiting entry.
            guard.insert(
                "node-w".to_string(),
                NodeInfo::new(
                    "127.0.0.1:9002".to_string(),
                    NodeState::Waiting,
                    "tk-w".to_string(),
                ),
            );
        }

        // `T` presenting the Waiting entry's own (matching) name/token.
        let received = roundtrip_one_frame(registry, b"T 6 4\nnode-wtk-w").await;

        assert!(
            received.is_empty(),
            "a Waiting node's T must be rejected with no response written, got {received:?}"
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn a_heartbeat_reporting_a_mismatched_replication_factor_is_still_acked() {
        // Issue #30: a mismatch is logged and recorded, not rejected —
        // this replica has no way to tell which side is actually
        // misconfigured (discovery HA keeps replicas from reconciling
        // membership with each other), so the node must stay `Joined`
        // and keep heartbeating normally. (Recording the mismatch does
        // make this replica start refusing `L` — see
        // `a_mismatched_heartbeat_replication_factor_makes_l_refuse`.)
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let registry: Registry = Arc::new(RegistryState::default());
        let current_join: CurrentJoin = Arc::new(Mutex::new(None));
        let (_shutdown_tx, shutdown_rx) = watch::channel(false);

        let server_registry = Arc::clone(&registry);
        let server_current_join = Arc::clone(&current_join);
        tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let _ = handle_connection(
                MaybeTls::Plain(stream),
                std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST),
                server_registry,
                server_current_join,
                ConnectionConfig {
                    idle_timeout: Duration::from_secs(5),
                    list_ready_at: Instant::now(),
                    replication: 2,
                    auth_secret: None,
                    tls_acceptor: None,
                    tls_connector: None,
                    announce_limiter: Arc::new(Mutex::new(FxHashMap::default())),
                },
                shutdown_rx,
                Arc::new(std::sync::Mutex::new(None)),
            )
            .await;
        });

        let mut client = TcpStream::connect(address).await.unwrap();
        // This replica is configured with R=2 (above); the node here
        // reports R=1 on its heartbeat — a disagreement.
        client
            .write_all(b"J 6 8356 9\nnode-atk-node-aH 6 1 9\nnode-atk-node-a")
            .await
            .unwrap();

        let expected = b"R\nA\n";
        let mut received = Vec::new();
        let mut chunk = [0u8; 64];

        while received.len() < expected.len() {
            let bytes_read = client.read(&mut chunk).await.unwrap();
            assert!(bytes_read > 0, "connection closed before response arrived");
            received.extend_from_slice(&chunk[..bytes_read]);
        }

        assert_eq!(received, expected);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn a_mismatched_heartbeat_replication_factor_makes_l_refuse() {
        // Issue #30: replication factor is static operator config, not
        // membership — a persistent disagreement means this replica's own
        // `--replication-factor` (which `L` embeds) is a value the node
        // itself has learned elsewhere is wrong, so `L` must refuse
        // rather than hand it out.
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let registry: Registry = Arc::new(RegistryState::default());
        let current_join: CurrentJoin = Arc::new(Mutex::new(None));
        let (_shutdown_tx, shutdown_rx) = watch::channel(false);

        let server_registry = Arc::clone(&registry);
        let server_current_join = Arc::clone(&current_join);
        tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let _ = handle_connection(
                MaybeTls::Plain(stream),
                std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST),
                server_registry,
                server_current_join,
                ConnectionConfig {
                    idle_timeout: Duration::from_secs(5),
                    list_ready_at: Instant::now(),
                    replication: 2,
                    auth_secret: None,
                    tls_acceptor: None,
                    tls_connector: None,
                    announce_limiter: Arc::new(Mutex::new(FxHashMap::default())),
                },
                shutdown_rx,
                Arc::new(std::sync::Mutex::new(None)),
            )
            .await;
        });

        let mut client = TcpStream::connect(address).await.unwrap();
        // This replica is configured with R=2 (above); the node here
        // reports R=1 on its heartbeat, then asks for `L`.
        client
            .write_all(b"J 6 8356 9\nnode-atk-node-aH 6 1 9\nnode-atk-node-aL\n")
            .await
            .unwrap();

        let expected = b"R\nA\nB\n";
        let mut received = Vec::new();
        let mut chunk = [0u8; 64];

        while received.len() < expected.len() {
            let bytes_read = client.read(&mut chunk).await.unwrap();
            assert!(bytes_read > 0, "connection closed before response arrived");
            received.extend_from_slice(&chunk[..bytes_read]);
        }

        assert_eq!(received, expected);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn a_later_matching_heartbeat_clears_the_mismatch_and_l_serves_again() {
        // Issue #30: the recorded mismatch is overwritten on every
        // heartbeat, so a node that later reports the matching value
        // clears it naturally — `L` must go back to serving normally
        // rather than staying refused forever. Two separate connections
        // (an `L` refusal closes the connection it arrived on, same as
        // the startup-grace refusal — see
        // `list_answers_busy_during_the_startup_grace_while_announce_still_works`),
        // sharing one registry.
        let registry: Registry = Arc::new(RegistryState::default());
        let current_join: CurrentJoin = Arc::new(Mutex::new(None));
        let (_shutdown_tx, shutdown_rx) = watch::channel(false);

        let config = || ConnectionConfig {
            idle_timeout: Duration::from_secs(5),
            list_ready_at: Instant::now(),
            replication: 2,
            auth_secret: None,
            tls_acceptor: None,
            tls_connector: None,
            announce_limiter: Arc::new(Mutex::new(FxHashMap::default())),
        };

        // First connection: joins node-a, then a mismatched (R=1)
        // heartbeat, then an `L` that must be refused and close.
        let (mut first, server) = tcp_pair().await;
        tokio::spawn(handle_connection(
            MaybeTls::Plain(server),
            std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST),
            Arc::clone(&registry),
            Arc::clone(&current_join),
            config(),
            shutdown_rx.clone(),
            Arc::new(std::sync::Mutex::new(None)),
        ));
        first
            .write_all(b"J 6 8356 9\nnode-atk-node-aH 6 1 9\nnode-atk-node-aL\n")
            .await
            .unwrap();

        let expected_first = b"R\nA\nB\n";
        let mut received_first = Vec::new();
        let mut chunk = [0u8; 64];
        while received_first.len() < expected_first.len() {
            let bytes_read = first.read(&mut chunk).await.unwrap();
            assert!(bytes_read > 0, "connection closed before response arrived");
            received_first.extend_from_slice(&chunk[..bytes_read]);
        }
        assert_eq!(received_first, expected_first);

        // Second connection: a matching (R=2) heartbeat for the same
        // node-a, then an `L` that must serve normally again.
        let (mut second, server) = tcp_pair().await;
        tokio::spawn(handle_connection(
            MaybeTls::Plain(server),
            std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST),
            Arc::clone(&registry),
            Arc::clone(&current_join),
            config(),
            shutdown_rx.clone(),
            Arc::new(std::sync::Mutex::new(None)),
        ));
        second
            .write_all(b"H 6 2 9\nnode-atk-node-aL\n")
            .await
            .unwrap();

        let expected_second =
            b"A 1 2\n6 14\nnode-a127.0.0.1:8356\nN 1 2\n6 14\nnode-a127.0.0.1:8356\n";
        let mut received_second = Vec::new();
        while received_second.len() < expected_second.len() {
            let bytes_read = second.read(&mut chunk).await.unwrap();
            assert!(bytes_read > 0, "connection closed before response arrived");
            received_second.extend_from_slice(&chunk[..bytes_read]);
        }
        assert_eq!(received_second, expected_second);
    }

    /// A `Joined` `NodeInfo` with a given `reported_replication`, for the
    /// majority-rule `L` tests below — built by direct field assignment
    /// (bypassing a real heartbeat) since only the recorded belief, not
    /// how it got there, matters to `L`'s vote tally.
    fn joined_node_reporting(addr: &str, token: &str, reported: Option<usize>) -> NodeInfo {
        let mut info = NodeInfo::new(addr.to_string(), NodeState::Joined, token.to_string());
        info.reported_replication = reported;
        info
    }

    #[test]
    fn cached_heartbeat_ack_reuses_the_buffer_until_the_generation_moves() {
        // Issue #95: rebuilding the roster ack on every heartbeat is the
        // O(nodes²)-per-cycle cost. The cache must hand back the very same
        // buffer while membership is unchanged, and rebuild only once the
        // generation has moved.
        let registry: Registry = Arc::new(RegistryState::default());
        {
            let mut guard = lock(&registry);
            guard.insert(
                "node-a".to_string(),
                joined_node_reporting("127.0.0.1:9001", "tk-a", Some(2)),
            );
            guard.insert(
                "node-b".to_string(),
                joined_node_reporting("127.0.0.1:9002", "tk-b", Some(2)),
            );
        }

        let first = cached_heartbeat_ack(&registry, 2);
        let second = cached_heartbeat_ack(&registry, 2);
        assert!(
            Arc::ptr_eq(&first, &second),
            "unchanged membership must reuse the cached buffer, not rebuild it"
        );
        // The cached bytes match a fresh serialization of the roster.
        assert_eq!(
            &*first,
            build_roster_response('A', &roster_snapshot(&registry, 2).nodes, 2).as_slice()
        );
        assert!(first.starts_with(b"A 2 2\n"));

        // A membership change (a new Joined node) plus its generation bump
        // forces a rebuild: a different buffer reflecting the new roster.
        {
            let mut guard = lock(&registry);
            guard.insert(
                "node-c".to_string(),
                joined_node_reporting("127.0.0.1:9003", "tk-c", Some(2)),
            );
        }
        bump_roster(&registry);
        let third = cached_heartbeat_ack(&registry, 2);
        assert!(
            !Arc::ptr_eq(&second, &third),
            "a membership change must invalidate the cache"
        );
        assert!(third.starts_with(b"A 3 2\n"));
    }

    #[test]
    fn cached_heartbeat_ack_folds_in_the_refuse_decision() {
        // The withheld-roster case (a strict dissenting majority → bare
        // `A\n`, issue #30) is baked into the cached bytes, so a cache hit
        // never has to recompute it.
        let registry: Registry = Arc::new(RegistryState::default());
        {
            let mut guard = lock(&registry);
            guard.insert(
                "agree".to_string(),
                joined_node_reporting("127.0.0.1:9001", "tk", Some(2)),
            );
            guard.insert(
                "dissent-0".to_string(),
                joined_node_reporting("127.0.0.1:9002", "tk", Some(1)),
            );
            guard.insert(
                "dissent-1".to_string(),
                joined_node_reporting("127.0.0.1:9003", "tk", Some(1)),
            );
        }
        assert_eq!(&*cached_heartbeat_ack(&registry, 2), b"A\n");
    }

    #[test]
    fn cached_list_response_matches_a_fresh_render_and_reuses_the_buffer_until_the_generation_moves()
     {
        // Issue #298: `cached_list_response` is `cached_heartbeat_ack`'s
        // sibling for `L` — mirrors that test exactly. The cache must hand
        // back the very same buffer while membership is unchanged, the
        // cached bytes must match a fresh `roster_snapshot` render, and a
        // membership change plus its generation bump must force a rebuild.
        let registry: Registry = Arc::new(RegistryState::default());
        {
            let mut guard = lock(&registry);
            guard.insert(
                "node-a".to_string(),
                joined_node_reporting("127.0.0.1:9001", "tk-a", Some(2)),
            );
            guard.insert(
                "node-b".to_string(),
                joined_node_reporting("127.0.0.1:9002", "tk-b", Some(2)),
            );
        }

        let first = cached_list_response(&registry, 2);
        let second = cached_list_response(&registry, 2);
        assert!(
            Arc::ptr_eq(&first, &second),
            "unchanged membership must reuse the cached buffer, not rebuild it"
        );
        assert_eq!(
            &*first,
            build_roster_response('N', &roster_snapshot(&registry, 2).nodes, 2).as_slice()
        );
        assert!(first.starts_with(b"N 2 2\n"));

        // A membership change (a new Joined node) plus its generation bump
        // forces a rebuild: a different buffer reflecting the new roster.
        {
            let mut guard = lock(&registry);
            guard.insert(
                "node-c".to_string(),
                joined_node_reporting("127.0.0.1:9003", "tk-c", Some(2)),
            );
        }
        bump_roster(&registry);
        let third = cached_list_response(&registry, 2);
        assert!(
            !Arc::ptr_eq(&second, &third),
            "a membership change must invalidate the cache"
        );
        assert!(third.starts_with(b"N 3 2\n"));
    }

    #[test]
    fn cached_proxy_list_reuses_the_buffer_until_the_registry_changes() {
        // `Q`'s sibling of the `L` cache test: the same buffer is handed
        // out while the proxy registry is unchanged, its bytes match a
        // fresh render, and an insert, an address change or a removal
        // (each followed by `bump_proxies`, as the handlers and the
        // sweep do) forces a rebuild.
        let registry: Registry = Arc::new(RegistryState::default());
        let insert = |name: &str, addr: &str| {
            registry.proxies.lock().unwrap().insert(
                name.to_string(),
                ProxyInfo {
                    address: addr.to_string(),
                    token: format!("tk-{name}"),
                    last_seen: Instant::now(),
                },
            );
            bump_proxies(&registry);
        };
        insert("proxy-a", "10.0.0.1:8358");
        insert("proxy-b", "10.0.0.2:8358");

        let first = cached_proxy_list(&registry);
        let second = cached_proxy_list(&registry);
        assert!(
            Arc::ptr_eq(&first, &second),
            "an unchanged registry must reuse the buffer"
        );
        assert_eq!(
            &*first,
            render_proxy_list(&registry.proxies.lock().unwrap()).as_bytes()
        );
        assert!(first.starts_with(b"N 2\n"));
        let text = String::from_utf8_lossy(&first).into_owned();
        assert!(text.contains("7 13\nproxy-a10.0.0.1:8358\n"), "{text}");

        insert("proxy-c", "10.0.0.3:8358");
        let third = cached_proxy_list(&registry);
        assert!(
            !Arc::ptr_eq(&second, &third),
            "an insert must invalidate the cache"
        );
        assert!(third.starts_with(b"N 3\n"));

        registry.proxies.lock().unwrap().remove("proxy-a");
        bump_proxies(&registry);
        let fourth = cached_proxy_list(&registry);
        assert!(
            !Arc::ptr_eq(&third, &fourth),
            "a removal must invalidate the cache"
        );
        assert!(fourth.starts_with(b"N 2\n"));
        assert!(!String::from_utf8_lossy(&fourth).contains("proxy-a"));
    }

    #[test]
    fn cached_list_response_folds_in_the_refuse_decision() {
        // The withheld-roster case (a strict dissenting majority → bare
        // `B\n`, issue #30) is baked into the cached bytes, so a cache hit
        // never has to recompute it — `cached_heartbeat_ack`'s equivalent
        // test, for `L`'s own refusal byte.
        let registry: Registry = Arc::new(RegistryState::default());
        {
            let mut guard = lock(&registry);
            guard.insert(
                "agree".to_string(),
                joined_node_reporting("127.0.0.1:9001", "tk", Some(2)),
            );
            guard.insert(
                "dissent-0".to_string(),
                joined_node_reporting("127.0.0.1:9002", "tk", Some(1)),
            );
            guard.insert(
                "dissent-1".to_string(),
                joined_node_reporting("127.0.0.1:9003", "tk", Some(1)),
            );
        }
        assert_eq!(&*cached_list_response(&registry, 2), b"B\n");
    }

    #[test]
    fn cached_node_roster_matches_a_fresh_render_and_reuses_the_buffer_until_the_generation_moves()
    {
        // Issue #356: `cached_node_roster` is `cached_list_response`'s
        // sibling for `T` — mirrors that test. Seeded with one node per
        // state to also lock in `T`'s every-state, token-carrying view
        // (issue #295): all three must appear, unlike `L`/`H`.
        let registry: Registry = Arc::new(RegistryState::default());
        {
            let mut guard = lock(&registry);
            guard.insert(
                "node-a".to_string(),
                joined_node_reporting("127.0.0.1:9001", "tk-a", Some(2)),
            );
            guard.insert(
                "node-b".to_string(),
                NodeInfo::new(
                    "127.0.0.1:9002".to_string(),
                    NodeState::Joining,
                    "tk-b".to_string(),
                ),
            );
            guard.insert(
                "node-c".to_string(),
                NodeInfo::with_queue_position(
                    "127.0.0.1:9003".to_string(),
                    NodeState::Waiting,
                    "tk-c".to_string(),
                    1,
                ),
            );
        }

        let first = cached_node_roster(&registry, 2);
        let second = cached_node_roster(&registry, 2);
        assert!(
            Arc::ptr_eq(&first, &second),
            "an unchanged registry must reuse the cached buffer, not rebuild it"
        );
        let fresh: Vec<(String, String, String)> = {
            let guard = lock(&registry);
            guard
                .iter()
                .map(|(name, info)| (name.clone(), info.address.clone(), info.token.clone()))
                .collect()
        };
        assert_eq!(&*first, build_node_roster_response(&fresh, 2).as_slice());
        assert!(
            first.starts_with(b"N 3 2\n"),
            "T must list every state, not just Joined"
        );

        // A registry change plus its generation bump forces a rebuild.
        {
            let mut guard = lock(&registry);
            guard.remove("node-c");
        }
        bump_roster(&registry);
        let third = cached_node_roster(&registry, 2);
        assert!(
            !Arc::ptr_eq(&second, &third),
            "a registry change must invalidate the cache"
        );
        assert!(third.starts_with(b"N 2 2\n"));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn t_cache_is_invalidated_by_a_waiting_registration() {
        // Issue #356: a `J` registering a `Waiting` node is invisible to
        // `L`/`H` but must invalidate the `T` cache — driven through the
        // real `start_join` rather than a manual `bump_roster` call, so
        // this fails if that handler ever stops bumping.
        let registry: Registry = Arc::new(RegistryState::default());
        // A join already in progress keeps the registration below in
        // `Waiting` instead of auto-promoting it via the empty-registry
        // bootstrap shortcut.
        let current_join: CurrentJoin = Arc::new(Mutex::new(Some(PendingJoin {
            joining_name: "unrelated-joiner".to_string(),
            expected: HashMap::new(),
            completed: HashSet::new(),
            started_at: Instant::now(),
            max_entries: 0,
            roster: Arc::new(Vec::new()),
            late: LateHandoffs::default(),
            generation: 1,
        })));

        assert_eq!(&*cached_node_roster(&registry, 2), b"N 0 2\n");

        start_join(
            &registry,
            &current_join,
            &None,
            &None,
            2,
            Instant::now(),
            "waiter",
            "127.0.0.1:9001".to_string(),
            "tk-waiter".to_string(),
            1,
        )
        .await
        .expect("the registration must be admitted");

        assert_eq!(
            &*cached_node_roster(&registry, 2),
            b"N 1 2\n6 14 9\nwaiter127.0.0.1:9001tk-waiter\n",
            "the Waiting registration must invalidate the cache"
        );
    }

    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn t_cache_is_invalidated_by_a_waiting_eviction() {
        // Issue #356's other half: `sweep_expired` reaping a `Waiting`
        // node never bumped the generation (it's not in the `L`/`H`
        // roster) — but it is in `T`'s, so the cache primed before the
        // eviction must not survive it.
        let registry: Registry = Arc::new(RegistryState::default());
        lock(&registry).insert(
            "waiter".to_string(),
            NodeInfo::with_queue_position(
                "10.0.0.1:9000".to_string(),
                NodeState::Waiting,
                "tk-waiter".to_string(),
                1,
            ),
        );
        assert!(cached_node_roster(&registry, 2).starts_with(b"N 1 2\n"));

        let current_join: CurrentJoin = Arc::new(Mutex::new(None));
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let sweep_task = tokio::spawn(sweep_expired(
            Arc::clone(&registry),
            current_join,
            None,
            None,
            2,
            // Far in the future: keeps the post-grace kick (issue #63)
            // from promoting this lone Waiting entry first.
            Instant::now() + Duration::from_secs(365 * 86_400),
            Duration::from_secs(60),
            shutdown_rx,
        ));

        tokio::task::yield_now().await;
        tokio::time::advance(waiting_timeout_for(1) + Duration::from_secs(1)).await;
        tokio::task::yield_now().await;
        assert!(!lock(&registry).contains_key("waiter"));

        assert_eq!(
            &*cached_node_roster(&registry, 2),
            b"N 0 2\n",
            "the Waiting eviction must invalidate the cache"
        );

        shutdown_tx.send_replace(true);
        sweep_task.await.unwrap();
    }

    /// Moves the paused clock by `total` in steps of `step`, letting the
    /// sweep run after each, so no sweep tick sees a gap that looks like a
    /// stall.
    async fn advance_in_steps(total: Duration, step: Duration) {
        let mut moved = Duration::ZERO;
        while moved < total {
            tokio::time::advance(step).await;
            tokio::task::yield_now().await;
            moved += step;
        }
    }

    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn the_first_sweep_after_a_stall_gives_every_node_and_proxy_a_fresh_window() {
        // Issue #1312: discovery frozen (SIGSTOP, a paused VM) for several
        // liveness windows finds every `last_heartbeat` that old when it
        // resumes. Evicting on that sweep emptied the roster `L` answers
        // and closed every heartbeat connection ("not joined") until the
        // nodes announced themselves again; they were never silent.
        let liveness = Duration::from_secs(15);
        let registry: Registry = Arc::new(RegistryState::default());
        for name in ["node-a", "node-b"] {
            lock(&registry).insert(
                name.to_string(),
                NodeInfo::new(
                    "10.0.0.1:9000".to_string(),
                    NodeState::Joined,
                    format!("tk-{name}"),
                ),
            );
        }
        registry.proxies.lock().unwrap().insert(
            "proxy-a".to_string(),
            ProxyInfo {
                address: "10.0.0.2:9100".to_string(),
                token: "tk-proxy".to_string(),
                last_seen: Instant::now(),
            },
        );
        let current_join: CurrentJoin = Arc::new(Mutex::new(None));
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let sweep_task = tokio::spawn(sweep_expired(
            Arc::clone(&registry),
            current_join,
            None,
            None,
            2,
            Instant::now() + Duration::from_secs(365 * 86_400),
            liveness,
            shutdown_rx,
        ));
        tokio::task::yield_now().await;

        // One step across several windows: the next sweep tick is the
        // first in 60 s.
        tokio::time::advance(4 * liveness).await;
        for _ in 0..5 {
            tokio::task::yield_now().await;
        }
        let members = |registry: &Registry| {
            let mut names: Vec<String> = lock(registry).keys().cloned().collect();
            names.sort();
            names
        };
        assert_eq!(
            members(&registry),
            ["node-a", "node-b"],
            "a stalled sweep evicted nodes that had done nothing wrong"
        );
        assert!(registry.proxies.lock().unwrap().contains_key("proxy-a"));
        assert!(
            registry.departed.lock().unwrap().at.is_empty(),
            "nothing is on record as departed"
        );

        // Nodes that really are silent are still dropped, a window later.
        advance_in_steps(Duration::from_secs(20), Duration::from_secs(1)).await;
        assert!(members(&registry).is_empty());
        assert!(registry.proxies.lock().unwrap().is_empty());
        assert_eq!(registry.departed.lock().unwrap().at.len(), 2);
        // Judged by a sweep that was running: they earn the rejoin.
        assert!(registry.departed.lock().unwrap().unreliable.is_empty());

        shutdown_tx.send_replace(true);
        sweep_task.await.unwrap();
    }

    #[tokio::test(flavor = "current_thread")]
    async fn l_cache_is_invalidated_by_a_join_promotion_an_announce_and_a_leave() {
        // Issue #298: exercises the three real mutation paths that must
        // each invalidate the `L` cache — a staged join's promotion
        // (`promote_to_joined`, the join-completion call site), `P`
        // (announce, which lands straight in `Joined` with no staged-join
        // machinery — see `DiscoveryCommand::Announce`), and `V` (leave) —
        // driven through the real connection handler rather than a manual
        // `bump_roster` call, so this fails if any of those handlers ever
        // stops bumping the generation this cache relies on.
        let registry: Registry = Arc::new(RegistryState::default());
        let current_join: CurrentJoin = Arc::new(Mutex::new(None));
        let (_shutdown_tx, shutdown_rx) = watch::channel(false);

        let config = || ConnectionConfig {
            idle_timeout: IDLE_TIMEOUT,
            list_ready_at: Instant::now(),
            replication: 2,
            auth_secret: None,
            tls_acceptor: None,
            tls_connector: None,
            announce_limiter: Arc::new(Mutex::new(FxHashMap::default())),
        };

        assert_eq!(&*cached_list_response(&registry, 2), b"N 0 2\n");

        // Join: a node promoted straight via `promote_to_joined`, the same
        // call `try_begin_next_join` makes once a staged join hands off to
        // nobody (an empty-registry join, exactly this case) or completes.
        lock(&registry).insert(
            "node-a".to_string(),
            NodeInfo::new(
                "127.0.0.1:9001".to_string(),
                NodeState::Joining,
                "tk-node-a".to_string(),
            ),
        );
        promote_to_joined(&registry, "node-a");
        assert_eq!(
            &*cached_list_response(&registry, 2),
            b"N 1 2\n6 14\nnode-a127.0.0.1:9001\n",
            "the join promotion must invalidate the cache"
        );

        // Announce: `P` for a brand-new name, over a real connection.
        let (mut announcer, server) = tcp_pair().await;
        tokio::spawn(handle_connection(
            MaybeTls::Plain(server),
            std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST),
            Arc::clone(&registry),
            Arc::clone(&current_join),
            config(),
            shutdown_rx.clone(),
            Arc::new(std::sync::Mutex::new(None)),
        ));
        announcer
            .write_all(b"P 6 9002 9\nnode-btk-node-b")
            .await
            .unwrap();
        let mut ack = [0u8; 2];
        announcer.read_exact(&mut ack).await.unwrap();
        assert_eq!(&ack, b"R\n");
        assert!(
            cached_list_response(&registry, 2).starts_with(b"N 2 2\n"),
            "the announce must invalidate the cache (order of the two nodes is unspecified)"
        );

        // Leave: `V` for the join-promoted node above.
        let (mut leaver, server) = tcp_pair().await;
        tokio::spawn(handle_connection(
            MaybeTls::Plain(server),
            std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST),
            Arc::clone(&registry),
            Arc::clone(&current_join),
            config(),
            shutdown_rx.clone(),
            Arc::new(std::sync::Mutex::new(None)),
        ));
        leaver.write_all(b"V 6 9\nnode-atk-node-a").await.unwrap();
        let mut ack = [0u8; 2];
        leaver.read_exact(&mut ack).await.unwrap();
        assert_eq!(&ack, b"R\n");
        assert_eq!(
            &*cached_list_response(&registry, 2),
            b"N 1 2\n6 14\nnode-b127.0.0.1:9002\n",
            "the leave must invalidate the cache"
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn l_is_served_despite_a_single_dissenter_among_a_majority() {
        // HIGH-severity amendment to issue #30: one Joined node reporting
        // a mismatched replication factor must not be able to deny `L` to
        // the whole cluster by itself. Three nodes agree with this
        // replica's R=2; one dissents (reports R=1) — not a strict
        // majority, so `L` is still served.
        let registry: Registry = Arc::new(RegistryState::default());
        {
            let mut guard = lock(&registry);
            for i in 0..3 {
                guard.insert(
                    format!("agree-{i}"),
                    joined_node_reporting(&format!("127.0.0.1:{}", 9000 + i), "tk-agree", Some(2)),
                );
            }
            guard.insert(
                "dissent-0".to_string(),
                joined_node_reporting("127.0.0.1:9100", "tk-dissent-0", Some(1)),
            );
        }
        let current_join: CurrentJoin = Arc::new(Mutex::new(None));
        let (_shutdown_tx, shutdown_rx) = watch::channel(false);

        let (mut client, server) = tcp_pair().await;
        tokio::spawn(handle_connection(
            MaybeTls::Plain(server),
            std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST),
            registry,
            current_join,
            ConnectionConfig {
                idle_timeout: Duration::from_secs(5),
                list_ready_at: Instant::now(),
                replication: 2,
                auth_secret: None,
                tls_acceptor: None,
                tls_connector: None,
                announce_limiter: Arc::new(Mutex::new(FxHashMap::default())),
            },
            shutdown_rx,
            Arc::new(std::sync::Mutex::new(None)),
        ));

        client.write_all(b"L\n").await.unwrap();
        let expected = b"N 4 2\n";
        let mut response = vec![0u8; expected.len()];
        client.read_exact(&mut response).await.unwrap();
        assert_eq!(response, expected);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn l_refuses_when_dissenters_form_a_strict_majority() {
        // Companion to the test above: two dissenting nodes (R=1) against
        // only one agreeing node (R=2) IS a strict majority, so `L` must
        // refuse exactly as the pre-amendment "any dissenter" rule did.
        let registry: Registry = Arc::new(RegistryState::default());
        {
            let mut guard = lock(&registry);
            guard.insert(
                "agree-0".to_string(),
                joined_node_reporting("127.0.0.1:9000", "tk-agree-0", Some(2)),
            );
            for i in 0..2 {
                guard.insert(
                    format!("dissent-{i}"),
                    joined_node_reporting(
                        &format!("127.0.0.1:{}", 9100 + i),
                        "tk-dissent",
                        Some(1),
                    ),
                );
            }
        }
        let current_join: CurrentJoin = Arc::new(Mutex::new(None));
        let (_shutdown_tx, shutdown_rx) = watch::channel(false);

        let (mut client, server) = tcp_pair().await;
        tokio::spawn(handle_connection(
            MaybeTls::Plain(server),
            std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST),
            registry,
            current_join,
            ConnectionConfig {
                idle_timeout: Duration::from_secs(5),
                list_ready_at: Instant::now(),
                replication: 2,
                auth_secret: None,
                tls_acceptor: None,
                tls_connector: None,
                announce_limiter: Arc::new(Mutex::new(FxHashMap::default())),
            },
            shutdown_rx,
            Arc::new(std::sync::Mutex::new(None)),
        ));

        client.write_all(b"L\n").await.unwrap();
        let expected = b"B\n";
        let mut response = vec![0u8; expected.len()];
        client.read_exact(&mut response).await.unwrap();
        assert_eq!(response, expected);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn l_is_served_on_a_tie_between_dissenters_and_agreeing_nodes() {
        // A tie (one dissenter, one agreeing node) is NOT a strict
        // majority, so `L` is still served — "more dissenters than
        // agreers" is required, not "at least as many".
        let registry: Registry = Arc::new(RegistryState::default());
        {
            let mut guard = lock(&registry);
            guard.insert(
                "agree-0".to_string(),
                joined_node_reporting("127.0.0.1:9000", "tk-agree-0", Some(2)),
            );
            guard.insert(
                "dissent-0".to_string(),
                joined_node_reporting("127.0.0.1:9100", "tk-dissent-0", Some(1)),
            );
        }
        let current_join: CurrentJoin = Arc::new(Mutex::new(None));
        let (_shutdown_tx, shutdown_rx) = watch::channel(false);

        let (mut client, server) = tcp_pair().await;
        tokio::spawn(handle_connection(
            MaybeTls::Plain(server),
            std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST),
            registry,
            current_join,
            ConnectionConfig {
                idle_timeout: Duration::from_secs(5),
                list_ready_at: Instant::now(),
                replication: 2,
                auth_secret: None,
                tls_acceptor: None,
                tls_connector: None,
                announce_limiter: Arc::new(Mutex::new(FxHashMap::default())),
            },
            shutdown_rx,
            Arc::new(std::sync::Mutex::new(None)),
        ));

        client.write_all(b"L\n").await.unwrap();
        let expected = b"N 2 2\n";
        let mut response = vec![0u8; expected.len()];
        client.read_exact(&mut response).await.unwrap();
        assert_eq!(response, expected);
    }

    #[test]
    fn parse_reads_an_announce_command_and_consumes_only_that_frame() {
        let mut input = BytesMut::from(&b"P 9 8356 12\nsome-nametk-some-nameL\n"[..]);
        let command = parse(&mut input).unwrap();
        assert_eq!(
            command,
            DiscoveryCommand::Announce {
                name: "some-name".to_string(),
                port: 8356,
                token: "tk-some-name".to_string(),
                rejoin_capable: false,
            }
        );
        assert_eq!(&input[..], b"L\n");
    }

    #[test]
    fn parse_reads_the_g_of_a_node_that_can_start_over_on_an_announce_only() {
        let mut input = BytesMut::from(&b"P 9 8356 12 G\nsome-nametk-some-nameL\n"[..]);
        assert_eq!(
            parse(&mut input).unwrap(),
            DiscoveryCommand::Announce {
                name: "some-name".to_string(),
                port: 8356,
                token: "tk-some-name".to_string(),
                rejoin_capable: true,
            }
        );
        assert_eq!(&input[..], b"L\n");
        for frame in [
            &b"J 9 8356 12 G\nsome-nametk-some-name"[..],
            b"Y 9 8356 12 G\nsome-nametk-some-name",
            b"P 9 8356 12 X\nsome-nametk-some-name",
            b"P 9 8356 12 G G\nsome-nametk-some-name",
        ] {
            let mut input = BytesMut::from(frame);
            assert!(
                parse(&mut input).is_err(),
                "{:?}",
                String::from_utf8_lossy(frame)
            );
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn announce_upserts_a_joined_node_without_any_join_orchestration() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let registry: Registry = Arc::new(RegistryState::default());
        let current_join: CurrentJoin = Arc::new(Mutex::new(None));
        let (_shutdown_tx, shutdown_rx) = watch::channel(false);

        let server_registry = Arc::clone(&registry);
        let server_current_join = Arc::clone(&current_join);
        tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let _ = handle_connection(
                MaybeTls::Plain(stream),
                std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST),
                server_registry,
                server_current_join,
                ConnectionConfig {
                    idle_timeout: Duration::from_secs(5),
                    list_ready_at: Instant::now(),
                    replication: 2,
                    auth_secret: None,
                    tls_acceptor: None,
                    tls_connector: None,
                    announce_limiter: Arc::new(Mutex::new(FxHashMap::default())),
                },
                shutdown_rx,
                Arc::new(std::sync::Mutex::new(None)),
            )
            .await;
        });

        let mut client = TcpStream::connect(address).await.unwrap();
        // Discovery HA: an announce lands straight in `Joined` — promoted (R),
        // heartbeating (A), and visible in L — with no staged node join join
        // machinery involved.
        client
            .write_all(b"P 6 8356 9\nnode-atk-node-aH 6 0 9\nnode-atk-node-aL\n")
            .await
            .unwrap();

        let expected = b"R\nA 1 2\n6 14\nnode-a127.0.0.1:8356\nN 1 2\n6 14\nnode-a127.0.0.1:8356\n";
        let mut received = Vec::new();
        let mut chunk = [0u8; 64];

        while received.len() < expected.len() {
            let bytes_read = client.read(&mut chunk).await.unwrap();
            assert!(bytes_read > 0, "connection closed before response arrived");
            received.extend_from_slice(&chunk[..bytes_read]);
        }

        assert_eq!(received, expected);
        assert!(
            lock_current_join(&current_join).is_none(),
            "an announce must not start join orchestration"
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn announce_updates_the_address_of_an_already_joined_node() {
        let registry: Registry = Arc::new(RegistryState::default());
        let current_join: CurrentJoin = Arc::new(Mutex::new(None));
        let (_shutdown_tx, shutdown_rx) = watch::channel(false);

        lock(&registry).insert(
            "node-a".to_string(),
            NodeInfo::new(
                "127.0.0.1:1111".to_string(),
                NodeState::Joined,
                "tk-node-a".to_string(),
            ),
        );

        let (mut client, server) = tcp_pair().await;
        tokio::spawn(handle_connection(
            MaybeTls::Plain(server),
            std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST),
            Arc::clone(&registry),
            Arc::clone(&current_join),
            ConnectionConfig {
                idle_timeout: Duration::from_secs(5),
                list_ready_at: Instant::now(),
                replication: 2,
                auth_secret: None,
                tls_acceptor: None,
                tls_connector: None,
                announce_limiter: Arc::new(Mutex::new(FxHashMap::default())),
            },
            shutdown_rx,
            Arc::new(std::sync::Mutex::new(None)),
        ));

        client
            .write_all(b"P 6 2222 9\nnode-atk-node-a")
            .await
            .unwrap();
        let mut response = [0u8; 2];
        client.read_exact(&mut response).await.unwrap();
        assert_eq!(&response, b"R\n");

        assert_eq!(
            lock(&registry).get("node-a").unwrap().address,
            "127.0.0.1:2222"
        );
    }

    /// What `announce` (a whole `P` frame) is answered on a fresh
    /// connection to a registry in which `prepare` has run, and whether
    /// the name is registered afterwards.
    async fn announce_reply(frame: &[u8], prepare: impl FnOnce(&Registry)) -> (Vec<u8>, bool) {
        let registry: Registry = Arc::new(RegistryState::default());
        prepare(&registry);
        let current_join: CurrentJoin = Arc::new(Mutex::new(None));
        let (_shutdown_tx, shutdown_rx) = watch::channel(false);
        let (mut client, server) = tcp_pair().await;
        tokio::spawn(handle_connection(
            MaybeTls::Plain(server),
            std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST),
            Arc::clone(&registry),
            current_join,
            ConnectionConfig {
                idle_timeout: Duration::from_secs(5),
                list_ready_at: Instant::now(),
                replication: 2,
                auth_secret: None,
                tls_acceptor: None,
                tls_connector: None,
                announce_limiter: Arc::new(Mutex::new(FxHashMap::default())),
            },
            shutdown_rx,
            Arc::new(std::sync::Mutex::new(None)),
        ));
        client.write_all(frame).await.unwrap();
        let mut reply = [0u8; 2];
        client.read_exact(&mut reply).await.unwrap();
        let registered = lock(&registry).contains_key("node-a");
        (reply.to_vec(), registered)
    }

    /// How long before the eviction a survivor became `Joined`.
    const JOINED_BEFORE: Option<Duration> = Some(Duration::from_secs(60));

    /// Registers `name` in `state` as a node that has been in that state
    /// since `joined_before` ago (`None`: just now).
    fn insert_survivor(
        registry: &Registry,
        name: &str,
        state: NodeState,
        joined_before: Option<Duration>,
    ) {
        let mut info = NodeInfo::new("127.0.0.1:3333".to_string(), state, format!("tk-{name}"));
        if let Some(before) = joined_before {
            info.joined_at = Instant::now() - before;
        }
        lock(registry).insert(name.to_string(), info);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn a_node_evicted_together_with_every_other_node_is_registered_when_it_announces() {
        // #1311: nobody was left to move on without it, so there is
        // nothing its cache is behind. Told to start over, every node of a
        // cluster that was cut off from discovery as a whole would empty
        // its cache.
        let frame = b"P 6 2222 9 G\nnode-atk-node-a";
        let (reply, registered) = announce_reply(frame, |registry| {
            record_departed(registry, "node-a");
        })
        .await;
        assert_eq!(reply, b"R\n", "alone in the registry");
        assert!(registered);

        // Another node registered after this one was evicted (it announced
        // first when the path healed): it has not been through the
        // eviction, so it moved nothing on.
        let (reply, registered) = announce_reply(frame, |registry| {
            record_departed(registry, "node-a");
            insert_survivor(registry, "node-b", NodeState::Joined, None);
        })
        .await;
        assert_eq!(
            reply, b"R\n",
            "the only other member joined after the eviction"
        );
        assert!(registered);

        // A node that is still `Waiting` or `Joining` holds nothing the
        // evicted one is behind either.
        for state in [NodeState::Waiting, NodeState::Joining] {
            let (reply, registered) = announce_reply(frame, |registry| {
                insert_survivor(registry, "node-b", state, JOINED_BEFORE);
                record_departed(registry, "node-a");
            })
            .await;
            assert_eq!(reply, b"R\n", "{state:?}");
            assert!(registered);
        }

        // One node that has been `Joined` throughout is enough.
        let (reply, registered) = announce_reply(frame, |registry| {
            insert_survivor(registry, "node-b", NodeState::Joined, None);
            insert_survivor(registry, "node-c", NodeState::Joined, JOINED_BEFORE);
            record_departed(registry, "node-a");
        })
        .await;
        assert_eq!(reply, b"G\n");
        assert!(!registered);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn a_node_evicted_while_the_survivors_were_cut_off_too_is_registered_when_it_announces() {
        // #1479: the path to the whole fleet was cut, but this node's last
        // heartbeat was the oldest, so it was evicted while the others were
        // still listed; the path healed before theirs ran out. The survivor
        // heartbeats again after a gap of at least one missed heartbeat
        // that spans the eviction: it was out of contact too, saw no roster
        // without the evicted node and took nothing over.
        let frame = b"P 6 2222 9 G\nnode-atk-node-a";
        let (reply, registered) = announce_reply(frame, |registry| {
            insert_survivor(registry, "node-b", NodeState::Joined, JOINED_BEFORE);
            record_departed(registry, "node-a");
            let mut guard = lock(registry);
            let survivor = guard.get_mut("node-b").unwrap();
            survivor.last_heartbeat = Instant::now() - CONTACT_BREAK;
            note_contact(survivor, "node-b", Instant::now());
        })
        .await;
        assert_eq!(
            reply, b"R\n",
            "the survivor was cut off across the eviction"
        );
        assert!(registered);

        // A survivor whose heartbeat was merely late — under the gap one
        // lost heartbeat leaves — was in contact throughout: the evicted
        // node is behind it and starts over.
        let (reply, registered) = announce_reply(frame, |registry| {
            insert_survivor(registry, "node-b", NodeState::Joined, JOINED_BEFORE);
            record_departed(registry, "node-a");
            let mut guard = lock(registry);
            let survivor = guard.get_mut("node-b").unwrap();
            survivor.last_heartbeat = Instant::now() - (CONTACT_BREAK - Duration::from_secs(1));
            note_contact(survivor, "node-b", Instant::now());
        })
        .await;
        assert_eq!(reply, b"G\n", "a late heartbeat is not a break in contact");
        assert!(!registered);
    }

    /// A departure recorded at `at`, as `record_departed` would at that moment.
    fn depart_at(registry: &Registry, name: &str, at: Instant) {
        registry.departed.lock().unwrap().record(name, at);
    }

    /// A heartbeat of `name` at `at`.
    fn heartbeat_at(registry: &Registry, name: &str, at: Instant) {
        let mut guard = lock(registry);
        note_contact(guard.get_mut(name).unwrap(), name, at);
    }

    /// Registers `name` as a member `Joined` for ten minutes whose last
    /// heartbeat was at `last_heartbeat`.
    fn insert_member(registry: &Registry, name: &str, now: Instant, last_heartbeat: Instant) {
        let mut info = NodeInfo::new(
            "127.0.0.1:3333".to_string(),
            NodeState::Joined,
            format!("tk-{name}"),
        );
        info.joined_at = now - Duration::from_secs(600);
        info.last_heartbeat = last_heartbeat;
        info.last_contact = last_heartbeat;
        lock(registry).insert(name.to_string(), info);
    }

    #[test]
    fn note_contact_records_a_break_only_after_a_gap_of_a_missed_heartbeat() {
        let now = Instant::now();
        let mut info = NodeInfo::new(
            "127.0.0.1:3333".to_string(),
            NodeState::Joined,
            "tk".to_string(),
        );
        let joined_at = now - Duration::from_secs(600);
        info.joined_at = joined_at;

        // On time, and late by less than the break: no break.
        for gap in [
            Duration::from_secs(NODE_HEARTBEAT_INTERVAL_SECS),
            CONTACT_BREAK - Duration::from_millis(1),
        ] {
            info.last_heartbeat = now - gap;
            note_contact(&mut info, "node", now);
            assert!(info.contact_breaks.is_empty(), "gap {gap:?}");
            assert_eq!(info.last_heartbeat, now);
        }

        // One lost heartbeat (two intervals), or exactly the break: recorded
        // as (previous heartbeat, now). The membership start is not touched.
        for gap in [
            CONTACT_BREAK,
            Duration::from_secs(2 * NODE_HEARTBEAT_INTERVAL_SECS),
        ] {
            info.contact_breaks.clear();
            info.last_heartbeat = now - gap;
            note_contact(&mut info, "node", now);
            assert_eq!(info.contact_breaks, vec![(now - gap, now)], "gap {gap:?}");
            assert_eq!(info.last_heartbeat, now);
            assert_eq!(info.joined_at, joined_at);
        }

        // Not for a node that is not `Joined`.
        let mut waiting = NodeInfo::new(
            "127.0.0.1:3333".to_string(),
            NodeState::Waiting,
            "tk".to_string(),
        );
        waiting.last_heartbeat = now - Duration::from_secs(60);
        note_contact(&mut waiting, "node", now);
        assert!(waiting.contact_breaks.is_empty());
        assert_eq!(waiting.last_heartbeat, now);
    }

    #[test]
    fn contact_breaks_are_pruned_to_the_departure_retention_and_capped() {
        // Instants ahead of now: a plain `now - hour` would not exist on a
        // host that has been up for less than that.
        let start = Instant::now();
        let mut info = NodeInfo::new(
            "127.0.0.1:3333".to_string(),
            NodeState::Joined,
            "tk".to_string(),
        );

        // Breaks that ended more than the retention ago are dropped by the
        // next one; younger ones stay.
        let short = Duration::from_secs(1);
        info.contact_breaks = vec![
            (start, start + short),
            (
                start + DEPARTED_RETENTION,
                start + DEPARTED_RETENTION + short,
            ),
        ];
        let heard_again = start + short + DEPARTED_RETENTION + CONTACT_BREAK;
        info.last_heartbeat = heard_again - CONTACT_BREAK;
        note_contact(&mut info, "node", heard_again);
        assert_eq!(
            info.contact_breaks,
            vec![
                (
                    start + DEPARTED_RETENTION,
                    start + DEPARTED_RETENTION + short
                ),
                (heard_again - CONTACT_BREAK, heard_again),
            ]
        );

        // The cap (unreachable through heartbeats alone: an hour holds at
        // most 400 breaks of 9 s): the oldest goes first.
        info.contact_breaks = (0..MAX_CONTACT_BREAKS as u32)
            .map(|i| (start + short * (2 * i), start + short * (2 * i + 1)))
            .collect();
        let first = info.contact_breaks[1];
        let last = start + Duration::from_secs(2100);
        info.last_heartbeat = last;
        note_contact(&mut info, "node", last + CONTACT_BREAK);
        assert_eq!(info.contact_breaks.len(), MAX_CONTACT_BREAKS);
        assert_eq!(info.contact_breaks[0], first);
        assert_eq!(
            *info.contact_breaks.last().unwrap(),
            (last, last + CONTACT_BREAK)
        );
    }

    #[test]
    fn a_survivor_that_loses_a_heartbeat_after_the_eviction_still_makes_the_evicted_node_start_over()
     {
        // node-b was served the roster without node-a (its heartbeat after
        // the eviction) and took node-a's keys over. One heartbeat
        // connection then drops: its next heartbeat comes 10 s later. That
        // gap starts after the eviction and must not make node-b look as if
        // it never moved on.
        let registry: Registry = Arc::new(RegistryState::default());
        let now = Instant::now();
        let evicted_at = now - Duration::from_secs(30);
        insert_member(
            &registry,
            "node-b",
            now,
            evicted_at - Duration::from_secs(1),
        );
        depart_at(&registry, "node-a", evicted_at);

        heartbeat_at(&registry, "node-b", evicted_at + Duration::from_secs(1));
        assert!(
            announce_needs_rejoin(&registry, "node-a"),
            "node-b moved on at its first heartbeat after the eviction"
        );
        heartbeat_at(&registry, "node-b", evicted_at + Duration::from_secs(11));
        assert!(
            announce_needs_rejoin(&registry, "node-a"),
            "node-b took node-a's keys over before its gap; node-a must still start over"
        );
    }

    #[test]
    fn a_break_that_spans_another_departure_does_not_undo_having_moved_on() {
        // node-b moved on past node-a's eviction at T+2. At T+20 node-x
        // leaves; node-b's heartbeat connection drops and its next
        // heartbeat, at T+28, ends a gap that spans node-x's departure but
        // not node-a's. node-a announcing at T+30 still starts over.
        let registry: Registry = Arc::new(RegistryState::default());
        let now = Instant::now();
        let t = now - Duration::from_secs(40);
        insert_member(&registry, "node-b", now, t - Duration::from_secs(1));
        depart_at(&registry, "node-a", t);
        heartbeat_at(&registry, "node-b", t + Duration::from_secs(2));
        assert!(announce_needs_rejoin(&registry, "node-a"));
        depart_at(&registry, "node-x", t + Duration::from_secs(20));
        heartbeat_at(&registry, "node-b", t + Duration::from_secs(28));
        assert!(
            announce_needs_rejoin(&registry, "node-a"),
            "a break spanning node-x's departure must not hide node-a's eviction"
        );
        // node-x, which did depart inside that break, is registered: the
        // survivor was cut off across its departure.
        assert!(!announce_needs_rejoin(&registry, "node-x"));
    }

    #[test]
    fn a_survivor_cut_off_across_the_eviction_does_not_make_the_evicted_node_start_over() {
        // node-c was out of contact from before node-d's eviction until
        // after it.
        let registry: Registry = Arc::new(RegistryState::default());
        let now = Instant::now();
        let evicted_d = now - Duration::from_secs(5);
        insert_member(&registry, "node-c", now, evicted_d - Duration::from_secs(5));
        depart_at(&registry, "node-d", evicted_d);
        heartbeat_at(&registry, "node-c", now);
        assert!(!announce_needs_rejoin(&registry, "node-d"));

        // A break that ended exactly at the eviction still spans it; one
        // that began exactly there does not.
        let registry: Registry = Arc::new(RegistryState::default());
        insert_member(&registry, "node-c", now, evicted_d - CONTACT_BREAK);
        depart_at(&registry, "node-d", evicted_d);
        heartbeat_at(&registry, "node-c", evicted_d);
        assert!(!announce_needs_rejoin(&registry, "node-d"));
        let registry: Registry = Arc::new(RegistryState::default());
        insert_member(&registry, "node-c", now, evicted_d);
        depart_at(&registry, "node-d", evicted_d);
        heartbeat_at(&registry, "node-c", evicted_d + CONTACT_BREAK);
        assert!(announce_needs_rejoin(&registry, "node-d"));
    }

    #[test]
    fn one_survivor_that_stayed_in_contact_is_enough_to_make_the_evicted_node_start_over() {
        let registry: Registry = Arc::new(RegistryState::default());
        let now = Instant::now();
        let evicted = now - Duration::from_secs(5);
        insert_member(&registry, "node-b", now, evicted - Duration::from_secs(5));
        depart_at(&registry, "node-a", evicted);
        heartbeat_at(&registry, "node-b", now);
        assert!(
            !announce_needs_rejoin(&registry, "node-a"),
            "node-b was cut off across the eviction"
        );
        insert_member(&registry, "node-c", now, evicted - Duration::from_secs(3));
        heartbeat_at(&registry, "node-c", evicted + Duration::from_secs(1));
        assert!(announce_needs_rejoin(&registry, "node-a"));
    }

    #[test]
    fn a_survivor_silent_since_before_the_eviction_and_out_of_contact_does_not_veto() {
        // The path to the whole fleet was cut. node-a was evicted 8 s ago;
        // node-b, still listed, has said nothing for 12 s, so it was heard
        // last before the eviction and has been cut off for more than the
        // break. Its next heartbeat will record a break that spans the
        // eviction; until it arrives it must not make node-a, whose
        // announce got here first, start over.
        let now = Instant::now();
        let registry: Registry = Arc::new(RegistryState::default());
        insert_member(&registry, "node-b", now, now - Duration::from_secs(12));
        depart_at(&registry, "node-a", now - Duration::from_secs(8));
        assert!(
            !announce_needs_rejoin_at(&registry, "node-a", now),
            "node-b was not served a roster without node-a and is out of contact"
        );

        // The verdict does not depend on who reaches discovery first: after
        // node-b's heartbeat the answer is the same.
        heartbeat_at(&registry, "node-b", now);
        assert!(
            !announce_needs_rejoin_at(&registry, "node-a", now),
            "node-b's heartbeat after the announce must not change the verdict"
        );
    }

    #[test]
    fn a_survivor_silent_for_less_than_the_break_since_before_the_eviction_still_vetoes() {
        // Last contact 3 s before the eviction, eviction just now: node-b is
        // in contact and will be served the roster without node-a at its
        // next heartbeat.
        let now = Instant::now();
        let registry: Registry = Arc::new(RegistryState::default());
        insert_member(&registry, "node-b", now, now - Duration::from_secs(3));
        depart_at(&registry, "node-a", now);
        assert!(announce_needs_rejoin_at(&registry, "node-a", now));

        // Up to the break: silent for CONTACT_BREAK - 1 ms still counts, a
        // silence of CONTACT_BREAK is the one whose next heartbeat would
        // record a break and no longer counts.
        for (silent_for, vetoes) in [
            (CONTACT_BREAK - Duration::from_millis(1), true),
            (CONTACT_BREAK, false),
            (CONTACT_BREAK + Duration::from_millis(1), false),
        ] {
            let registry: Registry = Arc::new(RegistryState::default());
            insert_member(&registry, "node-b", now, now - silent_for);
            depart_at(&registry, "node-a", now - Duration::from_millis(500));
            assert_eq!(
                announce_needs_rejoin_at(&registry, "node-a", now),
                vetoes,
                "silent for {silent_for:?}"
            );
        }
    }

    #[test]
    fn a_survivor_that_spoke_after_the_eviction_vetoes_however_long_ago() {
        // node-b heartbeated 20 s ago, 10 s after the eviction: it was
        // served the roster without node-a and took its keys over. Its
        // silence since then (it is about to be evicted itself) does not
        // undo that.
        let now = Instant::now();
        let registry: Registry = Arc::new(RegistryState::default());
        insert_member(&registry, "node-b", now, now - Duration::from_secs(20));
        depart_at(&registry, "node-a", now - Duration::from_secs(30));
        assert!(announce_needs_rejoin_at(&registry, "node-a", now));

        // One survivor out of contact does not outvote another that is in it.
        insert_member(&registry, "node-c", now, now - Duration::from_secs(40));
        assert!(announce_needs_rejoin_at(&registry, "node-a", now));
        lock(&registry).remove("node-b");
        assert!(
            !announce_needs_rejoin_at(&registry, "node-a", now),
            "node-c alone is out of contact"
        );
    }

    #[test]
    fn the_stall_guards_refresh_does_not_count_as_a_survivors_contact() {
        // The sweep after a pause of discovery gives every `Joined` node a
        // fresh liveness window by moving `last_heartbeat` to now. node-b
        // said nothing: its last contact is 12 s ago, before the eviction.
        let now = Instant::now();
        let registry: Registry = Arc::new(RegistryState::default());
        insert_member(&registry, "node-b", now, now - Duration::from_secs(12));
        lock(&registry).get_mut("node-b").unwrap().last_heartbeat = now;
        depart_at(&registry, "node-a", now - Duration::from_secs(8));
        assert!(
            !announce_needs_rejoin_at(&registry, "node-a", now),
            "the guard's refresh made a silent survivor look in contact"
        );
        // And it records no break: the heartbeat that follows is on time.
        heartbeat_at(&registry, "node-b", now + Duration::from_secs(5));
        assert!(lock(&registry)["node-b"].contact_breaks.is_empty());
    }

    #[tokio::test(flavor = "current_thread")]
    async fn a_node_evicted_while_the_survivors_were_silent_is_registered_when_it_announces_first()
    {
        // On the wire: the evicted node's announce reaches discovery before
        // any survivor's heartbeat does.
        let frame = b"P 6 2222 9 G\nnode-atk-node-a";
        let (reply, registered) = announce_reply(frame, |registry| {
            let now = Instant::now();
            insert_member(registry, "node-b", now, now - Duration::from_secs(12));
            depart_at(registry, "node-a", now - Duration::from_secs(8));
        })
        .await;
        assert_eq!(reply, b"R\n", "the only survivor is out of contact");
        assert!(registered);

        // The same survivor in contact: node-a starts over.
        let (reply, registered) = announce_reply(frame, |registry| {
            let now = Instant::now();
            insert_member(registry, "node-b", now, now - Duration::from_secs(3));
            depart_at(registry, "node-a", now);
        })
        .await;
        assert_eq!(reply, b"G\n");
        assert!(!registered);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn a_node_announcing_after_its_eviction_is_told_to_start_over_instead_of_registered() {
        // The node was evicted: the survivors took over its keys and
        // accepted writes for them. Registered again with the cache it
        // held, it would serve the older values, and the reclaim pass on
        // the survivors would then delete the newer ones.
        let evicted = |registry: &Registry| {
            insert_survivor(registry, "node-b", NodeState::Joined, JOINED_BEFORE);
            record_departed(registry, "node-a");
        };
        let (reply, registered) = announce_reply(b"P 6 2222 9 G\nnode-atk-node-a", evicted).await;
        assert_eq!(reply, b"G\n");
        assert!(!registered, "it must join through J, not be admitted by P");

        // An announce without the `G` (a node of an earlier version, or one
        // to a standby replica) is admitted as before.
        let (reply, registered) = announce_reply(b"P 6 2222 9\nnode-atk-node-a", evicted).await;
        assert_eq!(reply, b"R\n");
        assert!(registered);

        // No record of a departure (a discovery that has just restarted
        // and met its running nodes again): admitted at once.
        let (reply, registered) = announce_reply(b"P 6 2222 9 G\nnode-atk-node-a", |_| {}).await;
        assert_eq!(reply, b"R\n");
        assert!(registered);

        // Registered since (it joined again): its announces are refreshes.
        let (reply, registered) = announce_reply(b"P 6 2222 9 G\nnode-atk-node-a", |registry| {
            record_departed(registry, "node-a");
            lock(registry).insert(
                "node-a".to_string(),
                NodeInfo::new(
                    "127.0.0.1:2222".to_string(),
                    NodeState::Joined,
                    "tk-node-a".to_string(),
                ),
            );
        })
        .await;
        assert_eq!(reply, b"R\n");
        assert!(registered);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn announce_with_the_wrong_token_cannot_repoint_a_joined_nodes_address() {
        // Issue #34, the takeover scenario itself: knowing a node's name
        // (public — `L` lists it) must not be enough to redirect its
        // traffic; the announce must also present the token the name was
        // registered with.
        let registry: Registry = Arc::new(RegistryState::default());
        let current_join: CurrentJoin = Arc::new(Mutex::new(None));
        let (_shutdown_tx, shutdown_rx) = watch::channel(false);

        lock(&registry).insert(
            "node-a".to_string(),
            NodeInfo::new(
                "127.0.0.1:1111".to_string(),
                NodeState::Joined,
                "tk-node-a".to_string(),
            ),
        );

        let connection_name = Arc::new(std::sync::Mutex::new(None));
        let (mut attacker, server) = tcp_pair().await;
        let connection_task = tokio::spawn(handle_connection(
            MaybeTls::Plain(server),
            std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST),
            Arc::clone(&registry),
            Arc::clone(&current_join),
            ConnectionConfig {
                idle_timeout: Duration::from_secs(5),
                list_ready_at: Instant::now(),
                replication: 2,
                auth_secret: None,
                tls_acceptor: None,
                tls_connector: None,
                announce_limiter: Arc::new(Mutex::new(FxHashMap::default())),
            },
            shutdown_rx,
            Arc::clone(&connection_name),
        ));

        attacker
            .write_all(b"P 6 2222 8\nnode-aevil-tok")
            .await
            .unwrap();

        // Rejected: the connection errors out and closes without `R\n`.
        let mut response = [0u8; 2];
        assert!(attacker.read_exact(&mut response).await.is_err());
        assert!(connection_task.await.unwrap().is_err());

        // The real node's registration is untouched...
        assert_eq!(
            lock(&registry).get("node-a").unwrap().address,
            "127.0.0.1:1111"
        );
        // ...and the rejected connection never claimed the name, so its
        // teardown can't run `on_node_connection_ended` against the real
        // node's entry.
        assert!(
            connection_name
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .is_none(),
            "a rejected announce must not claim the name for its connection's teardown"
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn a_rejected_announce_cannot_abort_an_in_progress_join() {
        // Issue #34's companion DoS: announcing a mid-join name used to
        // set `connection_name` before rejecting, so the rejected
        // connection's teardown removed the real (Waiting/Joining) entry
        // and abandoned the join. The name must only be claimed once the
        // announce is accepted.
        let (_shutdown_tx, shutdown_rx) = watch::channel(false);
        let (_node_a, _node_b, registry, current_join) =
            registry_with_a_joined_and_b_waiting(shutdown_rx.clone()).await;

        let connection_name = Arc::new(std::sync::Mutex::new(None));
        let (mut attacker, server) = tcp_pair().await;
        let connection_task = tokio::spawn(handle_connection(
            MaybeTls::Plain(server),
            std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST),
            Arc::clone(&registry),
            Arc::clone(&current_join),
            ConnectionConfig {
                idle_timeout: IDLE_TIMEOUT,
                list_ready_at: Instant::now(),
                replication: 2,
                auth_secret: None,
                tls_acceptor: None,
                tls_connector: None,
                announce_limiter: Arc::new(Mutex::new(FxHashMap::default())),
            },
            shutdown_rx,
            Arc::clone(&connection_name),
        ));

        attacker
            .write_all(b"P 6 2222 8\nnode-bevil-tok")
            .await
            .unwrap();
        assert!(connection_task.await.unwrap().is_err());

        assert!(
            connection_name
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .is_none(),
            "a rejected announce must not claim the mid-join name"
        );
        assert!(
            lock(&registry).contains_key("node-b"),
            "the joining node's entry must survive the rejected announce"
        );
        assert!(
            lock_current_join(&current_join).is_some(),
            "the in-progress join must survive the rejected announce"
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn a_heartbeat_with_the_wrong_token_is_rejected() {
        // Issue #34: a heartbeat must not refresh (or overwrite the
        // reported replication belief of) an entry it can't present the
        // token for — otherwise anyone could keep a dead node's entry
        // alive past `sweep_expired`.
        let registry: Registry = Arc::new(RegistryState::default());
        let current_join: CurrentJoin = Arc::new(Mutex::new(None));
        let (_shutdown_tx, shutdown_rx) = watch::channel(false);

        lock(&registry).insert(
            "node-a".to_string(),
            NodeInfo::new(
                "127.0.0.1:1111".to_string(),
                NodeState::Joined,
                "tk-node-a".to_string(),
            ),
        );

        let (mut attacker, server) = tcp_pair().await;
        let connection_task = tokio::spawn(handle_connection(
            MaybeTls::Plain(server),
            std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST),
            Arc::clone(&registry),
            Arc::clone(&current_join),
            ConnectionConfig {
                idle_timeout: Duration::from_secs(5),
                list_ready_at: Instant::now(),
                replication: 2,
                auth_secret: None,
                tls_acceptor: None,
                tls_connector: None,
                announce_limiter: Arc::new(Mutex::new(FxHashMap::default())),
            },
            shutdown_rx,
            Arc::new(std::sync::Mutex::new(None)),
        ));

        attacker
            .write_all(b"H 6 3 8\nnode-aevil-tok")
            .await
            .unwrap();

        let mut ack = [0u8; 2];
        assert!(attacker.read_exact(&mut ack).await.is_err());
        assert!(connection_task.await.unwrap().is_err());
        assert_eq!(
            lock(&registry).get("node-a").unwrap().reported_replication,
            None,
            "a wrong-token heartbeat must not record a replication belief"
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn an_announce_for_an_unknown_name_establishes_its_token() {
        // Issue #34: a name this replica doesn't know (a standby replica,
        // or an amnesiac restart) is trusted on first use — its announce
        // both registers it and binds the presented token, which
        // everything after must match.
        let registry: Registry = Arc::new(RegistryState::default());
        let current_join: CurrentJoin = Arc::new(Mutex::new(None));
        let (_shutdown_tx, shutdown_rx) = watch::channel(false);

        let config = || ConnectionConfig {
            idle_timeout: Duration::from_secs(5),
            list_ready_at: Instant::now(),
            replication: 2,
            auth_secret: None,
            tls_acceptor: None,
            tls_connector: None,
            announce_limiter: Arc::new(Mutex::new(FxHashMap::default())),
        };

        let (mut node, server) = tcp_pair().await;
        tokio::spawn(handle_connection(
            MaybeTls::Plain(server),
            std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST),
            Arc::clone(&registry),
            Arc::clone(&current_join),
            config(),
            shutdown_rx.clone(),
            Arc::new(std::sync::Mutex::new(None)),
        ));

        // First use: announce registers the name and binds tk-node-a; the
        // same connection's heartbeat (same token) is accepted.
        node.write_all(b"P 6 8356 9\nnode-atk-node-aH 6 0 9\nnode-atk-node-a")
            .await
            .unwrap();
        let expected = b"R\nA 1 2\n6 14\nnode-a127.0.0.1:8356\n";
        let mut responses = vec![0u8; expected.len()];
        node.read_exact(&mut responses).await.unwrap();
        assert_eq!(responses, expected);

        // A later connection presenting a different token is rejected.
        let (mut attacker, server) = tcp_pair().await;
        let connection_task = tokio::spawn(handle_connection(
            MaybeTls::Plain(server),
            std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST),
            Arc::clone(&registry),
            Arc::clone(&current_join),
            config(),
            shutdown_rx.clone(),
            Arc::new(std::sync::Mutex::new(None)),
        ));
        attacker
            .write_all(b"H 6 0 8\nnode-aevil-tok")
            .await
            .unwrap();
        let mut ack = [0u8; 2];
        assert!(attacker.read_exact(&mut ack).await.is_err());
        assert!(connection_task.await.unwrap().is_err());
    }

    #[tokio::test(flavor = "current_thread")]
    async fn start_join_rejects_a_registration_that_would_make_m_exceed_the_node_request_cap() {
        // Regression: with a large enough `joined` roster, the `M`
        // `try_begin_next_join` would eventually send for this
        // registration exceeds `nanocached-node`'s own
        // `MAX_REQUEST_SIZE` (1 MiB, src/server.rs) — the node would
        // just reject the connection outright and the join would stall
        // until discovery's own migration timeout reaps it. Uses
        // deliberately long names/addresses so a modest node count
        // (rather than tens of thousands, as a real cluster hitting this
        // would have) is enough to cross the cap in a fast unit test.
        let registry: Registry = Arc::new(RegistryState::default());
        let current_join: CurrentJoin = Arc::new(Mutex::new(None));

        let long_name_prefix = "x".repeat(10_000);
        let long_addr = format!("127.0.0.1:1{}", "y".repeat(10_000));
        {
            let mut guard = lock(&registry);
            for i in 0..60 {
                guard.insert(
                    format!("{long_name_prefix}-{i}"),
                    NodeInfo::new(long_addr.clone(), NodeState::Joined, "tk".to_string()),
                );
            }
        }

        let result = start_join(
            &registry,
            &current_join,
            &None,
            &None,
            2,
            Instant::now(),
            "joining-node",
            "127.0.0.1:9999".to_string(),
            "tk-joining-node".to_string(),
            1,
        )
        .await;

        let Err(JoinRejection::MigrateMessageTooLarge { message_len }) = result else {
            panic!("expected a MigrateMessageTooLarge rejection");
        };
        assert!(message_len > NODE_MAX_REQUEST_SIZE);

        // Rejected outright: the node must not have been registered.
        assert!(!lock(&registry).contains_key("joining-node"));
    }

    /// Issue #1616: a node restarted on the address its old name still holds
    /// as a `Joined` member. The old name used to stay in the roster until
    /// its heartbeats timed out, and the join's `M` to it reached the new
    /// process, which refused it.
    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn a_node_restarted_on_a_members_address_replaces_the_old_name_at_once() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap().to_string();
        let accepted = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let counter = Arc::clone(&accepted);
        tokio::spawn(async move {
            while let Ok((stream, _)) = listener.accept().await {
                counter.fetch_add(1, Ordering::SeqCst);
                drop(stream);
            }
        });
        let registry: Registry = Arc::new(RegistryState::default());
        lock(&registry).insert(
            "node-old".to_string(),
            NodeInfo::new(address.clone(), NodeState::Joined, "tk-old".to_string()),
        );
        let current_join: CurrentJoin = Arc::new(Mutex::new(None));
        // Silent for a missed heartbeat (issue #1663): a member that is still
        // heartbeating is not replaced.
        tokio::time::advance(CONTACT_BREAK).await;

        start_join(
            &registry,
            &current_join,
            &None,
            &None,
            2,
            Instant::now(),
            "node-new",
            address.clone(),
            "tk-new".to_string(),
            1,
        )
        .await
        .unwrap_or_else(|_| panic!("the restarted node is accepted"));

        assert!(
            !lock(&registry).contains_key("node-old"),
            "the old name is still a member"
        );
        assert_eq!(
            accepted.load(Ordering::SeqCst),
            0,
            "a handoff was asked of the address that is the joining node itself"
        );
        // With nobody left to hand off from, the node is promoted at once.
        assert_eq!(
            lock(&registry).get("node-new").map(|info| info.state),
            Some(NodeState::Joined)
        );
        assert!(
            registry
                .departed
                .lock()
                .unwrap()
                .departed_at("node-old")
                .is_some()
        );
        assert!(lock_current_join(&current_join).is_none());
    }

    /// Issue #1616: a join in flight no longer waits for the replaced name's
    /// `C`; when that was the last one, the join is finished.
    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn a_replaced_member_is_dropped_from_the_join_in_flight() {
        let registry: Registry = Arc::new(RegistryState::default());
        lock(&registry).insert(
            "node-old".to_string(),
            NodeInfo::new(
                "127.0.0.1:1".to_string(),
                NodeState::Joined,
                "tk-old".to_string(),
            ),
        );
        lock(&registry).insert(
            "node-b".to_string(),
            NodeInfo::new(
                "127.0.0.1:2".to_string(),
                NodeState::Joining,
                "tk-b".to_string(),
            ),
        );
        let current_join: CurrentJoin = Arc::new(Mutex::new(Some(PendingJoin {
            joining_name: "node-b".to_string(),
            expected: [("node-old".to_string(), "tk-old".to_string())]
                .into_iter()
                .collect(),
            completed: HashSet::new(),
            started_at: Instant::now(),
            max_entries: 0,
            roster: Arc::new(Vec::new()),
            late: LateHandoffs::default(),
            generation: 1,
        })));
        // Silent for a missed heartbeat (issue #1663): a member that is still
        // heartbeating is not replaced.
        tokio::time::advance(CONTACT_BREAK).await;

        start_join(
            &registry,
            &current_join,
            &None,
            &None,
            2,
            Instant::now(),
            "node-new",
            "127.0.0.1:1".to_string(),
            "tk-new".to_string(),
            2,
        )
        .await
        .unwrap_or_else(|_| panic!("the restarted node is accepted"));

        assert!(!lock(&registry).contains_key("node-old"));
        assert_eq!(
            lock(&registry).get("node-b").map(|info| info.state),
            Some(NodeState::Joined),
            "the join kept waiting for a handoff report that cannot come"
        );
        // A report in the replaced name is refused from here on.
        assert!(
            !handle_complete(
                &registry,
                &current_join,
                "node-old",
                "node-b",
                "tk-old",
                None
            )
            .await
        );
    }

    /// A stand-in ready node for `M`: accepts each connection, reads the
    /// frame and answers `A 0\n` (a handoff that finished). Returns its
    /// address and a count of the connections it has accepted.
    async fn migrate_acking_node() -> (String, Arc<AtomicU64>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap().to_string();
        let accepted = Arc::new(AtomicU64::new(0));
        let counter = Arc::clone(&accepted);
        tokio::spawn(async move {
            loop {
                let Ok((mut stream, _)) = listener.accept().await else {
                    return;
                };
                counter.fetch_add(1, Ordering::SeqCst);
                tokio::spawn(async move {
                    let mut chunk = [0u8; 4096];
                    let _ = stream.read(&mut chunk).await;
                    let _ = stream.write_all(b"A 0\n").await;
                });
            }
        });
        (address, accepted)
    }

    /// Issue #1663 deferred replacement: `node-old` and `node-b` are
    /// `Joined`, both last heard from now, with a sweep running (liveness
    /// 60 s, so only the hold's own rule can remove anyone within these
    /// tests). `old_accepted` counts the connections made to `node-old`'s
    /// address, which is where a restarted process would be listening.
    struct HoldFixture {
        registry: Registry,
        current_join: CurrentJoin,
        old_address: String,
        old_accepted: Arc<AtomicU64>,
        b_accepted: Arc<AtomicU64>,
        shutdown_tx: watch::Sender<bool>,
        sweep_task: tokio::task::JoinHandle<()>,
    }

    impl HoldFixture {
        async fn new() -> Self {
            let (old_address, old_accepted) = migrate_acking_node().await;
            let (b_address, b_accepted) = migrate_acking_node().await;
            let registry: Registry = Arc::new(RegistryState::default());
            for (name, address) in [("node-old", &old_address), ("node-b", &b_address)] {
                lock(&registry).insert(
                    name.to_string(),
                    NodeInfo::new(address.clone(), NodeState::Joined, format!("tk-{name}")),
                );
            }
            let current_join: CurrentJoin = Arc::new(Mutex::new(None));
            let (shutdown_tx, shutdown_rx) = watch::channel(false);
            let sweep_task = tokio::spawn(sweep_expired(
                Arc::clone(&registry),
                Arc::clone(&current_join),
                None,
                None,
                2,
                Instant::now(),
                Duration::from_secs(60),
                shutdown_rx,
            ));
            tokio::task::yield_now().await;
            Self {
                registry,
                current_join,
                old_address,
                old_accepted,
                b_accepted,
                shutdown_tx,
                sweep_task,
            }
        }

        /// A `J` for `name` from `address`.
        async fn join(&self, name: &str, address: &str, connection_id: u64) {
            start_join(
                &self.registry,
                &self.current_join,
                &None,
                &None,
                2,
                Instant::now(),
                name,
                address.to_string(),
                format!("tk-{name}"),
                connection_id,
            )
            .await
            .unwrap_or_else(|_| panic!("{name} is accepted"));
        }

        fn state_of(&self, name: &str) -> Option<NodeState> {
            lock(&self.registry).get(name).map(|info| info.state)
        }

        fn joining_name(&self) -> Option<String> {
            lock_current_join(&self.current_join)
                .as_ref()
                .map(|pending| pending.joining_name.clone())
        }

        async fn finish(self) {
            self.shutdown_tx.send_replace(true);
            let _ = self.sweep_task.await;
        }
    }

    /// Issue #1663: a restart whose `J` arrives while the old name was
    /// heard from only 2 s ago is undecided: nothing is sent to the old
    /// name's address, and the old name stays (and so does the join of the
    /// new one) until it has been silent for `CONTACT_BREAK`. Then it is
    /// replaced as in the immediate case, and the join runs once, against
    /// the members that are left. A name queued behind the held one waits
    /// its turn and starts when the first join completes; the hold costs it
    /// at most the hold.
    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn a_restart_is_replaced_once_the_old_name_is_silent_and_the_handoff_runs_once() {
        let fx = HoldFixture::new().await;
        advance_in_steps(Duration::from_secs(2), Duration::from_millis(100)).await;
        fx.join("node-new", &fx.old_address, 2).await;
        fx.join("node-z", "127.0.0.1:5555", 3).await;

        // Held: 2 s after the last heartbeat, 5 s before CONTACT_BREAK.
        advance_in_steps(Duration::from_secs(6), Duration::from_millis(100)).await;
        assert!(
            fx.state_of("node-old").is_some(),
            "replaced while undecided"
        );
        assert_eq!(fx.state_of("node-new"), Some(NodeState::Waiting));
        assert_eq!(fx.state_of("node-z"), Some(NodeState::Waiting));
        assert!(fx.joining_name().is_none(), "a join began during the hold");
        assert_eq!(fx.old_accepted.load(Ordering::SeqCst), 0);
        assert_eq!(fx.b_accepted.load(Ordering::SeqCst), 0);

        // Silent for CONTACT_BREAK (9 s) at 7 s after the J: replaced, and
        // the join of node-new starts without the old name.
        advance_in_steps(Duration::from_secs(2), Duration::from_millis(100)).await;
        wait_until("node-new's join", Duration::from_secs(5), || {
            fx.joining_name().as_deref() == Some("node-new")
        })
        .await;
        assert!(fx.state_of("node-old").is_none());
        assert!(
            fx.registry
                .departed
                .lock()
                .unwrap()
                .departed_at("node-old")
                .is_some()
        );
        let generation = {
            let guard = lock_current_join(&fx.current_join);
            let pending = guard.as_ref().unwrap();
            let mut expected: Vec<_> = pending.expected.keys().cloned().collect();
            expected.sort();
            assert_eq!(
                expected,
                ["node-b"],
                "the join still waits for the old name"
            );
            pending.generation
        };
        wait_until("the M to node-b", Duration::from_secs(5), || {
            fx.b_accepted.load(Ordering::SeqCst) >= 1
        })
        .await;
        assert_eq!(
            fx.old_accepted.load(Ordering::SeqCst),
            0,
            "the old name's address was sent an M"
        );
        // The name behind it has not been let past it.
        assert_eq!(fx.state_of("node-z"), Some(NodeState::Waiting));

        // The handoff ran once; node-z follows when it is done.
        assert!(
            handle_complete(
                &fx.registry,
                &fx.current_join,
                "node-b",
                "node-new",
                "tk-node-b",
                Some(generation)
            )
            .await
        );
        try_begin_next_join(
            &fx.registry,
            &fx.current_join,
            &None,
            &None,
            2,
            Instant::now(),
        )
        .await;
        assert_eq!(fx.state_of("node-new"), Some(NodeState::Joined));
        assert_eq!(fx.joining_name().as_deref(), Some("node-z"));
        assert_eq!(
            fx.b_accepted.load(Ordering::SeqCst),
            2,
            "one M each for node-new and node-z"
        );
        // The address is a member again, under the new name: node-z's `M`
        // goes to it, the only connection made there.
        assert_eq!(fx.old_accepted.load(Ordering::SeqCst), 1);
        fx.finish().await;
    }

    /// Issue #1663: the old name is heard from again after the new name
    /// registered, so the address is a duplicate, not a restart. The hold
    /// is released at the next sweep, the old name is never removed (not
    /// even when it later falls silent for `CONTACT_BREAK`, the decision
    /// having been made) and the join of the new name begins as it did
    /// before the replacement existed: the `M` goes to the old name too.
    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn a_live_duplicate_is_never_removed_and_the_new_name_joins_as_before() {
        let fx = HoldFixture::new().await;
        advance_in_steps(Duration::from_secs(2), Duration::from_millis(100)).await;
        fx.join("node-new", &fx.old_address, 2).await;
        advance_in_steps(Duration::from_secs(3), Duration::from_millis(100)).await;
        assert!(fx.joining_name().is_none());
        assert_eq!(fx.old_accepted.load(Ordering::SeqCst), 0);

        // The old name heartbeats.
        heartbeat_at(&fx.registry, "node-old", Instant::now());
        advance_in_steps(Duration::from_secs(2), Duration::from_millis(100)).await;
        wait_until("node-new's join", Duration::from_secs(5), || {
            fx.joining_name().as_deref() == Some("node-new")
        })
        .await;
        {
            let guard = lock_current_join(&fx.current_join);
            let mut expected: Vec<_> = guard.as_ref().unwrap().expected.keys().cloned().collect();
            expected.sort();
            assert_eq!(expected, ["node-b", "node-old"]);
        }
        wait_until("the M to the old name", Duration::from_secs(5), || {
            fx.old_accepted.load(Ordering::SeqCst) >= 1
        })
        .await;

        // Silent again for longer than CONTACT_BREAK: still not replaced
        // (the liveness timeout is 60 s).
        advance_in_steps(Duration::from_secs(12), Duration::from_millis(100)).await;
        assert!(fx.state_of("node-old").is_some());
        assert!(
            fx.registry
                .departed
                .lock()
                .unwrap()
                .departed_at("node-old")
                .is_none()
        );
        fx.finish().await;
    }

    /// Issue #1663: a pause of discovery itself must not make the old name
    /// look silent. The first sweep after it gives every member a fresh
    /// window (issue #1312), and the hold's `CONTACT_BREAK` runs from there.
    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn a_stalled_sweep_does_not_end_the_hold() {
        let fx = HoldFixture::new().await;
        advance_in_steps(Duration::from_secs(2), Duration::from_millis(100)).await;
        fx.join("node-new", &fx.old_address, 2).await;

        // One jump: discovery was not running for 40 s.
        tokio::time::advance(Duration::from_secs(40)).await;
        advance_in_steps(Duration::from_secs(3), Duration::from_millis(100)).await;
        assert!(
            fx.state_of("node-old").is_some(),
            "replaced on a stalled tick"
        );
        assert!(fx.joining_name().is_none());
        assert_eq!(fx.old_accepted.load(Ordering::SeqCst), 0);

        // Still undecided 8 s into the fresh window, decided after 9.
        advance_in_steps(Duration::from_secs(5), Duration::from_millis(100)).await;
        assert!(fx.state_of("node-old").is_some());
        advance_in_steps(Duration::from_secs(3), Duration::from_millis(100)).await;
        wait_until("node-new's join", Duration::from_secs(5), || {
            fx.joining_name().as_deref() == Some("node-new")
        })
        .await;
        assert!(fx.state_of("node-old").is_none());
        fx.finish().await;
    }

    /// Issue #1663: the hold never outlives its reason. The old name
    /// leaving (or being evicted) before the decision ends it, and the
    /// queue it was holding up is started.
    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn the_hold_ends_when_the_old_name_leaves_the_registry() {
        let fx = HoldFixture::new().await;
        advance_in_steps(Duration::from_secs(2), Duration::from_millis(100)).await;
        fx.join("node-new", &fx.old_address, 2).await;
        advance_in_steps(Duration::from_secs(1), Duration::from_millis(100)).await;
        assert!(fx.joining_name().is_none());

        lock(&fx.registry).remove("node-old");
        record_departed(&fx.registry, "node-old");
        advance_in_steps(Duration::from_secs(2), Duration::from_millis(100)).await;
        wait_until("node-new's join", Duration::from_secs(5), || {
            fx.joining_name().as_deref() == Some("node-new")
        })
        .await;
        fx.finish().await;
    }

    /// Issue #1663: the held name's own connection dying (its entry is
    /// removed) must not leave the queue behind it parked.
    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn the_queue_starts_when_the_held_name_itself_goes_away() {
        let fx = HoldFixture::new().await;
        advance_in_steps(Duration::from_secs(2), Duration::from_millis(100)).await;
        fx.join("node-new", &fx.old_address, 2).await;
        fx.join("node-z", "127.0.0.1:5555", 3).await;
        advance_in_steps(Duration::from_secs(1), Duration::from_millis(100)).await;
        assert!(fx.joining_name().is_none());

        on_node_connection_ended(
            &fx.registry,
            &fx.current_join,
            &None,
            &None,
            2,
            Instant::now(),
            "node-new",
            2,
        )
        .await;
        advance_in_steps(Duration::from_secs(2), Duration::from_millis(100)).await;
        wait_until("node-z's join", Duration::from_secs(5), || {
            fx.joining_name().as_deref() == Some("node-z")
        })
        .await;
        assert!(fx.state_of("node-old").is_some());
        fx.finish().await;
    }

    /// Issue #1663: a `J` from the address of a member that is still
    /// heartbeating is not that member restarted (a second process on the
    /// same host that advertises the same port, say). The live member
    /// stays, the new name waits like any other, and the live member is not
    /// told to start over: its next `P` is answered `R`, not `G` (which
    /// makes a node drop its cache and join again).
    #[tokio::test(flavor = "current_thread")]
    async fn a_j_from_the_address_of_a_member_that_is_still_heartbeating_does_not_remove_it() {
        let registry: Registry = Arc::new(RegistryState::default());
        let now = Instant::now();
        insert_member(&registry, "node-old", now, now - Duration::from_secs(2));
        insert_member(&registry, "node-b", now, now - Duration::from_secs(1));
        lock(&registry).get_mut("node-b").unwrap().address = "127.0.0.1:4444".to_string();
        // A join already in progress keeps the new name `Waiting`, so no `M`
        // goes out in this test.
        let current_join: CurrentJoin = Arc::new(Mutex::new(Some(PendingJoin {
            joining_name: "unrelated-joiner".to_string(),
            expected: [("node-b".to_string(), "tk-node-b".to_string())]
                .into_iter()
                .collect(),
            completed: HashSet::new(),
            started_at: Instant::now(),
            max_entries: 0,
            roster: Arc::new(Vec::new()),
            late: LateHandoffs::default(),
            generation: 1,
        })));

        start_join(
            &registry,
            &current_join,
            &None,
            &None,
            2,
            Instant::now(),
            "node-new",
            "127.0.0.1:3333".to_string(),
            "tk-new".to_string(),
            2,
        )
        .await
        .unwrap_or_else(|_| panic!("the new name is accepted"));

        // The `P` of the member the `J` would have removed.
        let (_shutdown_tx, shutdown_rx) = watch::channel(false);
        let (mut node_old, server) = tcp_pair().await;
        tokio::spawn(handle_connection(
            MaybeTls::Plain(server),
            std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST),
            Arc::clone(&registry),
            Arc::clone(&current_join),
            ConnectionConfig {
                idle_timeout: IDLE_TIMEOUT,
                list_ready_at: Instant::now(),
                replication: 2,
                auth_secret: None,
                tls_acceptor: None,
                tls_connector: None,
                announce_limiter: Arc::new(Mutex::new(FxHashMap::default())),
            },
            shutdown_rx,
            Arc::new(std::sync::Mutex::new(None)),
        ));
        node_old
            .write_all(b"P 8 3333 11 G\nnode-oldtk-node-old")
            .await
            .unwrap();
        let mut reply = [0u8; 2];
        node_old.read_exact(&mut reply).await.unwrap();

        assert!(
            lock(&registry).contains_key("node-old"),
            "a member that heartbeated 2 s ago was removed by a J from its address"
        );
        assert_eq!(
            lock(&registry).get("node-new").map(|info| info.state),
            Some(NodeState::Waiting),
            "the new name waits its turn like any other"
        );
        assert!(
            registry
                .departed
                .lock()
                .unwrap()
                .departed_at("node-old")
                .is_none(),
            "the member must not be on record as departed"
        );
        assert_eq!(
            &reply, b"R\n",
            "the live member was told to clear its cache and join again"
        );
    }

    /// Issue #1663: the old name counts as gone once it has been silent for
    /// `CONTACT_BREAK` (the point at which a missed heartbeat is recorded as
    /// a break), not before.
    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn a_member_is_replaced_once_it_has_been_silent_for_a_contact_break() {
        let registry: Registry = Arc::new(RegistryState::default());
        lock(&registry).insert(
            "node-old".to_string(),
            NodeInfo::new(
                "127.0.0.1:1".to_string(),
                NodeState::Joined,
                "tk-old".to_string(),
            ),
        );
        let current_join: CurrentJoin = Arc::new(Mutex::new(Some(PendingJoin {
            joining_name: "unrelated-joiner".to_string(),
            expected: [("node-old".to_string(), "tk-old".to_string())]
                .into_iter()
                .collect(),
            completed: HashSet::new(),
            started_at: Instant::now(),
            max_entries: 0,
            roster: Arc::new(Vec::new()),
            late: LateHandoffs::default(),
            generation: 1,
        })));
        let join = |name: &'static str, connection_id: u64| {
            let registry = Arc::clone(&registry);
            let current_join = Arc::clone(&current_join);
            async move {
                start_join(
                    &registry,
                    &current_join,
                    &None,
                    &None,
                    2,
                    Instant::now(),
                    name,
                    "127.0.0.1:1".to_string(),
                    format!("tk-{name}"),
                    connection_id,
                )
                .await
                .unwrap_or_else(|_| panic!("{name} is accepted"));
            }
        };

        tokio::time::advance(CONTACT_BREAK - Duration::from_millis(1)).await;
        join("node-new-1", 2).await;
        assert!(
            lock(&registry).contains_key("node-old"),
            "replaced one millisecond before a missed heartbeat would be recorded"
        );

        tokio::time::advance(Duration::from_millis(1)).await;
        join("node-new-2", 3).await;
        assert!(
            !lock(&registry).contains_key("node-old"),
            "a member silent for CONTACT_BREAK was not replaced"
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn start_join_does_not_size_reject_a_duplicate_j_for_an_already_waiting_node() {
        // Regression for issue #421(b): the `NODE_MAX_REQUEST_SIZE` check
        // must only apply to a genuinely new registration, like the
        // waiting-queue caps just above it in `start_join` already do (via
        // `!guard.contains_key(name)`). A duplicate `J` for a name already
        // registered here is a supported reconnect (issue #7/#9) that just
        // reuses the existing `Waiting` entry — it must not be rejected
        // just because the joined roster has grown past the cap since the
        // node's original registration.
        let registry: Registry = Arc::new(RegistryState::default());
        // A join already in progress (for some unrelated node) keeps every
        // registration below in `Waiting` instead of auto-promoting it via
        // the empty-registry bootstrap shortcut — same trick the waiting-cap
        // tests below use.
        let current_join: CurrentJoin = Arc::new(Mutex::new(Some(PendingJoin {
            joining_name: "unrelated-joiner".to_string(),
            expected: HashMap::new(),
            completed: HashSet::new(),
            started_at: Instant::now(),
            max_entries: 0,
            roster: Arc::new(Vec::new()),
            late: LateHandoffs::default(),
            generation: 1,
        })));

        // First registration: the joined roster is empty, so this is well
        // under NODE_MAX_REQUEST_SIZE and registers as `Waiting`.
        let first = start_join(
            &registry,
            &current_join,
            &None,
            &None,
            2,
            Instant::now(),
            "joining-node",
            "127.0.0.1:9999".to_string(),
            "tk-joining-node".to_string(),
            1,
        )
        .await;
        assert!(first.is_ok());
        assert_eq!(
            lock(&registry).get("joining-node").map(|info| info.state),
            Some(NodeState::Waiting)
        );

        // The joined roster grows enough, after that registration, that a
        // fresh registration's `M` would now exceed NODE_MAX_REQUEST_SIZE
        // (same construction as the rejection test above).
        let long_name_prefix = "x".repeat(10_000);
        let long_addr = format!("127.0.0.1:1{}", "y".repeat(10_000));
        {
            let mut guard = lock(&registry);
            for i in 0..60 {
                guard.insert(
                    format!("{long_name_prefix}-{i}"),
                    NodeInfo::new(long_addr.clone(), NodeState::Joined, "tk".to_string()),
                );
            }
        }

        // A duplicate `J` for the same already-registered name, presenting
        // the same token, must still succeed — not be rejected with
        // `MigrateMessageTooLarge` — and must not disturb its existing
        // `Waiting` entry.
        let second = start_join(
            &registry,
            &current_join,
            &None,
            &None,
            2,
            Instant::now(),
            "joining-node",
            "127.0.0.1:9999".to_string(),
            "tk-joining-node".to_string(),
            2,
        )
        .await;
        assert!(
            second.is_ok(),
            "duplicate J for an already-registered name was wrongly size-rejected: {second:?}"
        );
        assert_eq!(
            lock(&registry).get("joining-node").map(|info| info.state),
            Some(NodeState::Waiting)
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn start_join_rejects_a_new_registration_once_the_source_hits_the_waiting_cap() {
        // Regression: with auth unset, an unauthenticated attacker can
        // `J` under distinct fake names from one source without limit —
        // each held open forever by `wait_for_promotion` — up to
        // `MAX_CONNECTIONS`. `MAX_WAITING_PER_SOURCE_IP` caps how many
        // concurrent Waiting/Joining registrations one source may hold.
        let registry: Registry = Arc::new(RegistryState::default());
        // A join already in progress cluster-wide (for some unrelated
        // node) means every `start_join` call below only registers as
        // Waiting — it never takes the bootstrap "no Joined nodes yet"
        // shortcut that would otherwise auto-promote the very first
        // registration and make it stop counting against the cap.
        let current_join: CurrentJoin = Arc::new(Mutex::new(Some(PendingJoin {
            joining_name: "unrelated-joiner".to_string(),
            expected: HashMap::new(),
            completed: HashSet::new(),
            started_at: Instant::now(),
            max_entries: 0,
            roster: Arc::new(Vec::new()),
            late: LateHandoffs::default(),
            generation: 1,
        })));

        for i in 0..MAX_WAITING_PER_SOURCE_IP {
            let result = start_join(
                &registry,
                &current_join,
                &None,
                &None,
                2,
                Instant::now(),
                &format!("attacker-{i}"),
                "10.0.0.1:9000".to_string(),
                format!("tk-attacker-{i}"),
                (i + 1) as u64,
            )
            .await;
            assert!(result.is_ok(), "registration {i} should have been admitted");
        }

        let rejected = start_join(
            &registry,
            &current_join,
            &None,
            &None,
            2,
            Instant::now(),
            "attacker-overflow",
            "10.0.0.1:9000".to_string(),
            "tk-attacker-overflow".to_string(),
            100,
        )
        .await;
        let Err(JoinRejection::TooManyWaitingFromSource) = rejected else {
            panic!("expected a TooManyWaitingFromSource rejection");
        };
        assert!(!lock(&registry).contains_key("attacker-overflow"));

        // A different source address is unaffected by this one's cap.
        let other_source = start_join(
            &registry,
            &current_join,
            &None,
            &None,
            2,
            Instant::now(),
            "legit-node",
            "10.0.0.2:9000".to_string(),
            "tk-legit-node".to_string(),
            101,
        )
        .await;
        assert!(other_source.is_ok());

        // A duplicate `J` (issue #7) for a name the capped source already
        // holds, presenting the same token, reuses that entry rather than
        // adding a new one, so it must not be blocked by its own cap. Uses
        // a distinct connection id from the original registration's (`1`)
        // — a real duplicate `J` always arrives on a new connection — to
        // also exercise `start_join` overwriting `NodeInfo::
        // owner_connection_id` on a reused entry (issue #3/#9).
        let retry = start_join(
            &registry,
            &current_join,
            &None,
            &None,
            2,
            Instant::now(),
            "attacker-0",
            "10.0.0.1:9000".to_string(),
            "tk-attacker-0".to_string(),
            102,
        )
        .await;
        assert!(retry.is_ok());
        assert_eq!(
            lock(&registry)
                .get("attacker-0")
                .unwrap()
                .owner_connection_id,
            102,
            "a duplicate J must take over ownership of the reused entry"
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn the_waiting_cap_counts_one_ipv6_64_as_one_source() {
        // `MAX_WAITING_PER_SOURCE_IP` used to compare the registered address
        // text, so every address of one IPv6 /64 (2^64 of them) held a
        // separate allowance. It counts under `infra::per_ip_key` now,
        // like the per-IP connection caps.
        let registry: Registry = Arc::new(RegistryState::default());
        let current_join: CurrentJoin = Arc::new(Mutex::new(Some(PendingJoin {
            joining_name: "unrelated-joiner".to_string(),
            expected: HashMap::new(),
            completed: HashSet::new(),
            started_at: Instant::now(),
            max_entries: 0,
            roster: Arc::new(Vec::new()),
            late: LateHandoffs::default(),
            generation: 1,
        })));

        for i in 0..MAX_WAITING_PER_SOURCE_IP {
            let result = start_join(
                &registry,
                &current_join,
                &None,
                &None,
                2,
                Instant::now(),
                &format!("v6-{i}"),
                format!("2001:db8:0:1::{:x}:9000", i + 1),
                format!("tk-v6-{i}"),
                (i + 1) as u64,
            )
            .await;
            assert!(result.is_ok(), "registration {i} should have been admitted");
        }

        // Another address of the same /64 is the same source.
        let rejected = start_join(
            &registry,
            &current_join,
            &None,
            &None,
            2,
            Instant::now(),
            "v6-overflow",
            "2001:db8:0:1:ffff:ffff:ffff:ffff:9000".to_string(),
            "tk-v6-overflow".to_string(),
            100,
        )
        .await;
        let Err(JoinRejection::TooManyWaitingFromSource) = rejected else {
            panic!("expected a TooManyWaitingFromSource rejection");
        };

        // A different /64 is a different source.
        let other = start_join(
            &registry,
            &current_join,
            &None,
            &None,
            2,
            Instant::now(),
            "v6-other",
            "2001:db8:0:2::1:9000".to_string(),
            "tk-v6-other".to_string(),
            101,
        )
        .await;
        assert!(other.is_ok());
    }

    #[test]
    fn waiting_source_key_groups_a_64_and_leaves_ipv4_and_text_alone() {
        assert_eq!(waiting_source_key("10.0.0.1:9000"), "10.0.0.1");
        assert_eq!(
            waiting_source_key("2001:db8:0:1::5:9000"),
            waiting_source_key("2001:db8:0:1:aaaa::9:1234")
        );
        assert_ne!(
            waiting_source_key("2001:db8:0:1::5:9000"),
            waiting_source_key("2001:db8:0:2::5:9000")
        );
        assert_eq!(
            waiting_source_key("[2001:db8::1]:9000"),
            waiting_source_key("2001:db8::2:9000")
        );
        assert_eq!(waiting_source_key("::ffff:10.0.0.1:9000"), "10.0.0.1");
        assert_eq!(
            waiting_source_key("node-a.internal:9000"),
            "node-a.internal"
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn start_join_rejects_a_new_registration_once_the_global_waiting_cap_is_reached() {
        // Issue: join-queue starvation — `MAX_WAITING_PER_SOURCE_IP` only
        // bounds one source's own share of the queue; many distinct
        // sources, each within their own allowance, could still queue the
        // registry past any sane depth. `MAX_WAITING_TOTAL` caps the
        // queue's total size regardless of how many distinct sources
        // contribute to it. Uses distinct source IPs, each staying under
        // `MAX_WAITING_PER_SOURCE_IP`, so only the global cap is at play.
        let registry: Registry = Arc::new(RegistryState::default());
        let current_join: CurrentJoin = Arc::new(Mutex::new(Some(PendingJoin {
            joining_name: "unrelated-joiner".to_string(),
            expected: HashMap::new(),
            completed: HashSet::new(),
            started_at: Instant::now(),
            max_entries: 0,
            roster: Arc::new(Vec::new()),
            late: LateHandoffs::default(),
            generation: 1,
        })));

        let mut connection_id = 0u64;
        let sources = MAX_WAITING_TOTAL.div_ceil(MAX_WAITING_PER_SOURCE_IP);
        let mut admitted = 0;
        'sources: for source in 0..sources {
            for slot in 0..MAX_WAITING_PER_SOURCE_IP {
                if admitted >= MAX_WAITING_TOTAL {
                    break 'sources;
                }
                connection_id += 1;
                let result = start_join(
                    &registry,
                    &current_join,
                    &None,
                    &None,
                    2,
                    Instant::now(),
                    &format!("node-{source}-{slot}"),
                    format!("10.0.{source}.1:9000"),
                    format!("tk-node-{source}-{slot}"),
                    connection_id,
                )
                .await;
                assert!(
                    result.is_ok(),
                    "registration {source}-{slot} should be admitted"
                );
                admitted += 1;
            }
        }
        assert_eq!(admitted, MAX_WAITING_TOTAL);

        // One more, from a source that has never registered before (so
        // only the global cap, not the per-source one, could be at play).
        connection_id += 1;
        let rejected = start_join(
            &registry,
            &current_join,
            &None,
            &None,
            2,
            Instant::now(),
            "overflow-node",
            "10.99.0.1:9000".to_string(),
            "tk-overflow-node".to_string(),
            connection_id,
        )
        .await;
        let Err(JoinRejection::TooManyWaitingTotal) = rejected else {
            panic!("expected a TooManyWaitingTotal rejection, got {rejected:?}");
        };
        assert!(!lock(&registry).contains_key("overflow-node"));

        // A duplicate J for an already-registered name still succeeds —
        // reusing an entry must not be blocked by the cap the same way a
        // genuinely new registration is.
        connection_id += 1;
        let retry = start_join(
            &registry,
            &current_join,
            &None,
            &None,
            2,
            Instant::now(),
            "node-0-0",
            "10.0.0.1:9000".to_string(),
            "tk-node-0-0".to_string(),
            connection_id,
        )
        .await;
        assert!(retry.is_ok());
    }

    #[tokio::test(flavor = "current_thread")]
    async fn announce_for_a_name_mid_join_is_rejected() {
        let registry: Registry = Arc::new(RegistryState::default());
        let current_join: CurrentJoin = Arc::new(Mutex::new(None));
        let (_shutdown_tx, shutdown_rx) = watch::channel(false);

        lock(&registry).insert(
            "node-a".to_string(),
            NodeInfo::new(
                "127.0.0.1:1111".to_string(),
                NodeState::Waiting,
                "tk-node-a".to_string(),
            ),
        );

        let (mut client, server) = tcp_pair().await;
        tokio::spawn(handle_connection(
            MaybeTls::Plain(server),
            std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST),
            Arc::clone(&registry),
            Arc::clone(&current_join),
            ConnectionConfig {
                idle_timeout: Duration::from_secs(5),
                list_ready_at: Instant::now(),
                replication: 2,
                auth_secret: None,
                tls_acceptor: None,
                tls_connector: None,
                announce_limiter: Arc::new(Mutex::new(FxHashMap::default())),
            },
            shutdown_rx,
            Arc::new(std::sync::Mutex::new(None)),
        ));

        client
            .write_all(b"P 6 2222 9\nnode-atk-node-a")
            .await
            .unwrap();

        // The connection is closed with no `R` — the announce was refused.
        let mut buffer = [0u8; 2];
        let bytes_read = client.read(&mut buffer).await.unwrap();
        assert_eq!(
            bytes_read, 0,
            "expected the connection to close, got {buffer:?}"
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn list_answers_busy_during_the_startup_grace_while_announce_still_works() {
        let registry: Registry = Arc::new(RegistryState::default());
        let current_join: CurrentJoin = Arc::new(Mutex::new(None));
        let (_shutdown_tx, shutdown_rx) = watch::channel(false);

        let config = || ConnectionConfig {
            idle_timeout: Duration::from_secs(5),
            // Still inside the discovery HA grace for the whole test.
            list_ready_at: Instant::now() + Duration::from_secs(60),
            replication: 2,
            auth_secret: None,
            tls_acceptor: None,
            tls_connector: None,
            announce_limiter: Arc::new(Mutex::new(FxHashMap::default())),
        };

        // An announce during the grace must work — recovery depends on it.
        let (mut announcing, server) = tcp_pair().await;
        tokio::spawn(handle_connection(
            MaybeTls::Plain(server),
            std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST),
            Arc::clone(&registry),
            Arc::clone(&current_join),
            config(),
            shutdown_rx.clone(),
            Arc::new(std::sync::Mutex::new(None)),
        ));
        announcing
            .write_all(b"P 6 8356 9\nnode-atk-node-a")
            .await
            .unwrap();
        let mut response = [0u8; 2];
        announcing.read_exact(&mut response).await.unwrap();
        assert_eq!(&response, b"R\n");

        // A list during the grace gets the busy byte and a closed
        // connection, never the (possibly partial) node list.
        let (mut listing, server) = tcp_pair().await;
        tokio::spawn(handle_connection(
            MaybeTls::Plain(server),
            std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST),
            Arc::clone(&registry),
            Arc::clone(&current_join),
            config(),
            shutdown_rx.clone(),
            Arc::new(std::sync::Mutex::new(None)),
        ));
        listing.write_all(b"L\n").await.unwrap();

        let mut received = Vec::new();
        let mut chunk = [0u8; 16];
        loop {
            let bytes_read = listing.read(&mut chunk).await.unwrap();
            if bytes_read == 0 {
                break;
            }
            received.extend_from_slice(&chunk[..bytes_read]);
        }
        assert_eq!(received, b"B\n");
    }

    /// Boots a registry with node-a Joined (bootstrap join) and node-b
    /// parked in `wait_for_promotion` (Joining, expecting node-a's C).
    /// Returns the client sockets plus the shared state.
    /// Issue #63: spawns a discovery-side connection for `stream` whose
    /// startup grace ends at `list_ready_at`.
    fn spawn_grace_connection(
        server: TcpStream,
        registry: &Registry,
        current_join: &CurrentJoin,
        list_ready_at: Instant,
        shutdown_rx: watch::Receiver<bool>,
    ) {
        tokio::spawn(handle_connection(
            MaybeTls::Plain(server),
            std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST),
            Arc::clone(registry),
            Arc::clone(current_join),
            ConnectionConfig {
                idle_timeout: IDLE_TIMEOUT,
                list_ready_at,
                replication: 2,
                auth_secret: None,
                tls_acceptor: None,
                tls_connector: None,
                announce_limiter: Arc::new(Mutex::new(FxHashMap::default())),
            },
            shutdown_rx,
            Arc::new(std::sync::Mutex::new(None)),
        ));
    }

    /// Issue #124 helper: one plain HTTP GET → (status line, body).
    async fn http_get(addr: &str, path: &str) -> (String, String) {
        let mut stream = TcpStream::connect(addr).await.unwrap();
        stream
            .write_all(format!("GET {path} HTTP/1.1\r\nHost: x\r\n\r\n").as_bytes())
            .await
            .unwrap();
        let mut response = Vec::new();
        stream.read_to_end(&mut response).await.unwrap();
        let response = String::from_utf8(response).unwrap();
        let (head, body) = response.split_once("\r\n\r\n").unwrap();
        (head.lines().next().unwrap().to_string(), body.to_string())
    }

    #[tokio::test(flavor = "current_thread")]
    async fn metrics_endpoint_reports_registry_gauges_and_join_counters() {
        let registry: Registry = Arc::new(RegistryState::default());
        let current_join: CurrentJoin = Arc::new(Mutex::new(None));
        let (_shutdown_tx, shutdown_rx) = watch::channel(false);
        let ready = Instant::now();

        // One member (P upserts to Joined → also counts as a promoted
        // join), one waiting J, one proxy.
        let (mut member, server) = tcp_pair().await;
        spawn_grace_connection(server, &registry, &current_join, ready, shutdown_rx.clone());
        member
            .write_all(b"P 6 9001 9\nnode-atk-node-a")
            .await
            .unwrap();
        let mut ack = [0u8; 2];
        member.read_exact(&mut ack).await.unwrap();
        let (mut waiter, server) = tcp_pair().await;
        spawn_grace_connection(
            server,
            &registry,
            &current_join,
            Instant::now() + Duration::from_secs(60),
            shutdown_rx.clone(),
        );
        waiter
            .write_all(b"J 6 9002 9\nnode-btk-node-b")
            .await
            .unwrap();
        announce_proxy(
            &registry,
            &current_join,
            ready,
            shutdown_rx.clone(),
            "proxy-a",
            8358,
            "tk-a",
        )
        .await;
        // Give the parked J a beat to register as Waiting.
        tokio::time::sleep(Duration::from_millis(50)).await;

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        tokio::spawn(run_metrics_server(
            listener,
            Arc::clone(&registry),
            ready,
            Arc::new(Semaphore::new(METRICS_MAX_CONNECTIONS)),
        ));

        let (status, body) = http_get(&addr, "/metrics").await;
        assert_eq!(status, "HTTP/1.1 200 OK");
        assert!(body.contains("nanocached_discovery_members 1\n"), "{body}");
        assert!(
            body.contains("nanocached_discovery_waiting_nodes 1\n"),
            "{body}"
        );
        assert!(body.contains("nanocached_discovery_proxies 1\n"), "{body}");

        let (status, _) = http_get(&addr, "/healthz").await;
        assert_eq!(status, "HTTP/1.1 200 OK");
        let (status, _) = http_get(&addr, "/readyz").await;
        assert_eq!(status, "HTTP/1.1 200 OK");
    }

    #[tokio::test(flavor = "current_thread")]
    async fn readyz_refuses_during_the_startup_grace() {
        let registry: Registry = Arc::new(RegistryState::default());
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        tokio::spawn(run_metrics_server(
            listener,
            Arc::clone(&registry),
            Instant::now() + Duration::from_secs(60),
            Arc::new(Semaphore::new(METRICS_MAX_CONNECTIONS)),
        ));

        let (status, _) = http_get(&addr, "/readyz").await;
        assert_eq!(status, "HTTP/1.1 503 Service Unavailable");
        // Liveness is unconditional.
        let (status, _) = http_get(&addr, "/healthz").await;
        assert_eq!(status, "HTTP/1.1 200 OK");
    }

    #[tokio::test(flavor = "current_thread")]
    async fn metrics_listener_caps_its_own_concurrent_connections() {
        // Regression (issue #358): before this fix, `run_metrics_server`'s
        // accept loop spawned one unbounded task per connection — nothing
        // comparable to `MAX_CONNECTIONS` on the registration port capped
        // how many it would hold open at once, so a scrape storm (or a
        // peer that just opens connections and holds them) could grow the
        // task/fd count without limit. Mirrors the node's and
        // `nanocached-proxy`'s identical regression tests for issues
        // #327/#233.
        let registry: Registry = Arc::new(RegistryState::default());
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        let metrics_permits = Arc::new(Semaphore::new(1));
        // Hold the listener's only permit, as if one scrape were already
        // in flight.
        let held = Arc::clone(&metrics_permits).try_acquire_owned().unwrap();
        tokio::spawn(run_metrics_server(
            listener,
            Arc::clone(&registry),
            Instant::now(),
            metrics_permits,
        ));

        let mut stream = TcpStream::connect(&addr).await.unwrap();
        stream
            .write_all(b"GET /healthz HTTP/1.1\r\nHost: x\r\n\r\n")
            .await
            .unwrap();
        let mut response = Vec::new();
        // Some platforms (macOS observed) answer a close of a socket that
        // still has this test's unread request bytes sitting in its
        // kernel buffer with a TCP RST instead of a clean FIN — either
        // way, nothing was ever answered.
        match stream.read_to_end(&mut response).await {
            Ok(read) => assert_eq!(read, 0, "got a response: {response:?}"),
            Err(error) => assert!(
                matches!(
                    error.kind(),
                    std::io::ErrorKind::ConnectionReset | std::io::ErrorKind::BrokenPipe
                ),
                "unexpected read error over the metrics cap: {error}"
            ),
        }
        assert!(
            response.is_empty(),
            "over the metrics cap, the connection must be dropped with no response, got {response:?}"
        );

        drop(held);
        // Once the permit is free again, a scrape gets answered normally.
        let (status, _) = http_get(&addr, "/healthz").await;
        assert_eq!(status, "HTTP/1.1 200 OK");
    }

    #[test]
    fn parses_proxy_announce_and_list_proxies() {
        // Issue #122.
        let mut input = BytesMut::from(&b"Y 7 8358 8\nproxy-atk-proxy"[..]);
        assert_eq!(
            parse(&mut input),
            Ok(DiscoveryCommand::ProxyAnnounce {
                name: "proxy-a".to_string(),
                port: 8358,
                token: "tk-proxy".to_string(),
            })
        );

        let mut input = BytesMut::from(&b"Q\n"[..]);
        assert_eq!(parse(&mut input), Ok(DiscoveryCommand::ListProxies));

        let mut input = BytesMut::from(&b"Q extra\n"[..]);
        assert!(parse(&mut input).is_err());
    }

    /// Issue #122 helper: one `Y` announce over a fresh connection;
    /// returns the reply line ("R" on success).
    async fn announce_proxy(
        registry: &Registry,
        current_join: &CurrentJoin,
        list_ready_at: Instant,
        shutdown_rx: watch::Receiver<bool>,
        name: &str,
        port: u16,
        token: &str,
    ) -> String {
        let (mut client, server) = tcp_pair().await;
        spawn_grace_connection(server, registry, current_join, list_ready_at, shutdown_rx);
        let frame = format!("Y {} {port} {}\n{name}{token}", name.len(), token.len());
        client.write_all(frame.as_bytes()).await.unwrap();
        let mut reply = [0u8; 2];
        match client.read_exact(&mut reply).await {
            Ok(_) => String::from_utf8_lossy(&reply[..1]).into_owned(),
            Err(_) => "closed".to_string(),
        }
    }

    /// Issue #122 helper: one `Q` over a fresh connection; returns the
    /// raw response text.
    async fn query_proxies(
        registry: &Registry,
        current_join: &CurrentJoin,
        list_ready_at: Instant,
        shutdown_rx: watch::Receiver<bool>,
    ) -> String {
        let (mut client, server) = tcp_pair().await;
        spawn_grace_connection(server, registry, current_join, list_ready_at, shutdown_rx);
        client.write_all(b"Q\n").await.unwrap();
        let mut response = Vec::new();
        let mut chunk = [0u8; 1024];
        loop {
            match timeout(Duration::from_millis(300), client.read(&mut chunk)).await {
                Ok(Ok(0)) | Err(_) => break,
                Ok(Ok(bytes_read)) => response.extend_from_slice(&chunk[..bytes_read]),
                Ok(Err(_)) => break,
            }
        }
        String::from_utf8_lossy(&response).into_owned()
    }

    #[tokio::test(flavor = "current_thread")]
    async fn announced_proxies_are_served_by_q() {
        let registry: Registry = Arc::new(RegistryState::default());
        let current_join: CurrentJoin = Arc::new(Mutex::new(None));
        let (_shutdown_tx, shutdown_rx) = watch::channel(false);
        let ready = Instant::now();

        assert_eq!(
            announce_proxy(
                &registry,
                &current_join,
                ready,
                shutdown_rx.clone(),
                "proxy-a",
                8358,
                "tk-a"
            )
            .await,
            "R"
        );
        assert_eq!(
            announce_proxy(
                &registry,
                &current_join,
                ready,
                shutdown_rx.clone(),
                "proxy-b",
                9358,
                "tk-b"
            )
            .await,
            "R"
        );

        let response = query_proxies(&registry, &current_join, ready, shutdown_rx.clone()).await;
        assert!(response.starts_with("N 2\n"), "got {response:?}");
        assert!(
            response.contains("proxy-a127.0.0.1:8358"),
            "got {response:?}"
        );
        assert!(
            response.contains("proxy-b127.0.0.1:9358"),
            "got {response:?}"
        );

        // A re-announce with the right token moves the address.
        assert_eq!(
            announce_proxy(
                &registry,
                &current_join,
                ready,
                shutdown_rx.clone(),
                "proxy-a",
                8360,
                "tk-a"
            )
            .await,
            "R"
        );
        let response = query_proxies(&registry, &current_join, ready, shutdown_rx.clone()).await;
        assert!(
            response.contains("proxy-a127.0.0.1:8360"),
            "got {response:?}"
        );
        assert!(response.starts_with("N 2\n"), "got {response:?}");
    }

    #[tokio::test(flavor = "current_thread")]
    async fn a_proxy_announce_with_the_wrong_token_is_rejected() {
        // Issue #122: the token pins the name (issue #34's rationale).
        let registry: Registry = Arc::new(RegistryState::default());
        let current_join: CurrentJoin = Arc::new(Mutex::new(None));
        let (_shutdown_tx, shutdown_rx) = watch::channel(false);
        let ready = Instant::now();

        announce_proxy(
            &registry,
            &current_join,
            ready,
            shutdown_rx.clone(),
            "proxy-a",
            8358,
            "tk-a",
        )
        .await;
        let reply = announce_proxy(
            &registry,
            &current_join,
            ready,
            shutdown_rx.clone(),
            "proxy-a",
            9999,
            "tk-evil",
        )
        .await;
        assert_ne!(reply, "R");

        // The original registration is untouched.
        let response = query_proxies(&registry, &current_join, ready, shutdown_rx.clone()).await;
        assert!(
            response.contains("proxy-a127.0.0.1:8358"),
            "got {response:?}"
        );
    }

    #[test]
    fn the_dissent_warn_is_rate_limited_and_lists_a_bounded_number_of_names() {
        // Issue #1717: one line per `L` rebuild, naming every dissenter.
        let dissenting: Vec<String> = (0..65_000).map(|i| format!("fake-node-{i}")).collect();
        let names = dissenter_names(&dissenting);
        assert!(names.starts_with("fake-node-0, fake-node-1"), "{names}");
        assert!(names.ends_with("and 64995 more"), "{names}");
        assert!(names.len() < 200, "{} bytes", names.len());
        let few: Vec<String> = vec!["a".to_string(), "b".to_string()];
        assert_eq!(dissenter_names(&few), "a, b");

        let snapshot = RosterSnapshot {
            nodes: Vec::new(),
            dissenting,
            agreeing: 1,
        };
        let log = DissentLog {
            served: LogLimiter::new(1, Duration::from_secs(3600)),
            refusing: LogLimiter::new(1, Duration::from_secs(3600)),
        };
        // 65,000 dissenters against 1 agreeing is a strict majority: the
        // refusal line, bounded to one per window.
        let written = (0..1000)
            .filter(|_| log_dissent(&log, &snapshot, 2))
            .count();
        assert_eq!(written, 1);
    }

    #[test]
    fn a_refusal_right_after_a_served_dissent_warn_is_still_logged() {
        // Issue #1826: the two WARNs shared one 1-line budget, so a
        // "served despite dissent" line followed within the window by the
        // rebuild that starts refusing `L` left no line saying why `L`
        // answers `B`.
        let log = DissentLog {
            served: LogLimiter::new(1, Duration::from_secs(3600)),
            refusing: LogLimiter::new(1, Duration::from_secs(3600)),
        };
        let served = RosterSnapshot {
            nodes: Vec::new(),
            dissenting: vec!["a".to_string()],
            agreeing: 2,
        };
        let refusing = RosterSnapshot {
            nodes: Vec::new(),
            dissenting: vec!["a".to_string(), "b".to_string(), "c".to_string()],
            agreeing: 2,
        };
        assert!(!served.refuse());
        assert!(refusing.refuse());
        assert!(log_dissent(&log, &served, 2));
        assert!(log_dissent(&log, &refusing, 2), "the refusal was dropped");
        // Each kind is still bounded to one line per window.
        let written = (0..1000)
            .filter(|i| {
                let snapshot = if i % 2 == 0 { &served } else { &refusing };
                log_dissent(&log, snapshot, 2)
            })
            .count();
        assert_eq!(written, 0);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn proxy_announce_rate_limits_new_names_from_one_source_but_not_refreshes() {
        // Issue #1716: `Y` under a fresh name each time, on one connection,
        // used to fill `MAX_PROXY_ENTRIES` in milliseconds and lock real
        // proxies out. New names are now gated per source like `P`'s; a
        // refresh of a known name never is.
        let registry: Registry = Arc::new(RegistryState::default());
        let current_join: CurrentJoin = Arc::new(Mutex::new(None));
        let (_shutdown_tx, shutdown_rx) = watch::channel(false);
        let (mut client, server) = tcp_pair().await;
        tokio::spawn(handle_connection(
            MaybeTls::Plain(server),
            std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST),
            Arc::clone(&registry),
            Arc::clone(&current_join),
            ConnectionConfig {
                idle_timeout: IDLE_TIMEOUT,
                list_ready_at: Instant::now(),
                replication: 2,
                auth_secret: None,
                tls_acceptor: None,
                tls_connector: None,
                announce_limiter: Arc::new(Mutex::new(FxHashMap::default())),
            },
            shutdown_rx,
            Arc::new(std::sync::Mutex::new(None)),
        ));

        async fn announce(client: &mut TcpStream, name: &str) -> bool {
            let frame = format!("Y {} 8358 2\n{name}tk", name.len());
            client.write_all(frame.as_bytes()).await.unwrap();
            let mut reply = [0u8; 2];
            match client.read_exact(&mut reply).await {
                Ok(_) => {
                    assert_eq!(&reply, b"R\n");
                    true
                }
                Err(_) => false,
            }
        }

        for index in 0..ANNOUNCE_INSERT_BURST {
            assert!(announce(&mut client, &format!("fake-{index}")).await);
        }
        // A refresh of a registered name is never gated.
        assert!(announce(&mut client, "fake-0").await);
        // The next new name is refused and the connection closed.
        assert!(!announce(&mut client, "fake-over").await);
        let proxies = registry.proxies.lock().unwrap();
        assert_eq!(proxies.len(), ANNOUNCE_INSERT_BURST as usize);
        assert!(!proxies.contains_key("fake-over"));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn a_deregistered_proxy_leaves_q_immediately() {
        // Issue #124: a draining proxy must not linger in Q until the
        // liveness timeout.
        let registry: Registry = Arc::new(RegistryState::default());
        let current_join: CurrentJoin = Arc::new(Mutex::new(None));
        let (_shutdown_tx, shutdown_rx) = watch::channel(false);
        let ready = Instant::now();

        announce_proxy(
            &registry,
            &current_join,
            ready,
            shutdown_rx.clone(),
            "proxy-a",
            8358,
            "tk-a",
        )
        .await;
        announce_proxy(
            &registry,
            &current_join,
            ready,
            shutdown_rx.clone(),
            "proxy-b",
            8359,
            "tk-b",
        )
        .await;

        // Wrong token: rejected, entry intact.
        let (mut client, server) = tcp_pair().await;
        spawn_grace_connection(server, &registry, &current_join, ready, shutdown_rx.clone());
        client.write_all(b"Z 7 7\nproxy-atk-evil").await.unwrap();
        let mut reply = [0u8; 2];
        assert!(client.read_exact(&mut reply).await.is_err() || &reply != b"R\n");
        let listed = query_proxies(&registry, &current_join, ready, shutdown_rx.clone()).await;
        assert!(listed.contains("proxy-a"), "got {listed:?}");

        // Right token: gone at once.
        let (mut client, server) = tcp_pair().await;
        spawn_grace_connection(server, &registry, &current_join, ready, shutdown_rx.clone());
        client.write_all(b"Z 7 4\nproxy-atk-a").await.unwrap();
        let mut reply = [0u8; 2];
        client.read_exact(&mut reply).await.unwrap();
        assert_eq!(&reply, b"R\n");

        let listed = query_proxies(&registry, &current_join, ready, shutdown_rx.clone()).await;
        assert!(listed.starts_with("N 1\n"), "got {listed:?}");
        assert!(!listed.contains("proxy-a"), "got {listed:?}");
        assert!(listed.contains("proxy-b"), "got {listed:?}");

        // Deregistering again (unknown now): idempotent R.
        let (mut client, server) = tcp_pair().await;
        spawn_grace_connection(server, &registry, &current_join, ready, shutdown_rx.clone());
        client.write_all(b"Z 7 4\nproxy-atk-a").await.unwrap();
        let mut reply = [0u8; 2];
        client.read_exact(&mut reply).await.unwrap();
        assert_eq!(&reply, b"R\n");
    }

    #[tokio::test(flavor = "current_thread")]
    async fn q_refuses_during_the_grace_while_y_is_accepted() {
        let registry: Registry = Arc::new(RegistryState::default());
        let current_join: CurrentJoin = Arc::new(Mutex::new(None));
        let (_shutdown_tx, shutdown_rx) = watch::channel(false);
        let grace_end = Instant::now() + Duration::from_millis(250);

        assert_eq!(
            announce_proxy(
                &registry,
                &current_join,
                grace_end,
                shutdown_rx.clone(),
                "proxy-a",
                8358,
                "tk-a"
            )
            .await,
            "R",
            "a Y during the grace must be accepted — it is how the map refills"
        );
        let response =
            query_proxies(&registry, &current_join, grace_end, shutdown_rx.clone()).await;
        assert!(response.starts_with("B\n"), "got {response:?}");

        tokio::time::sleep(Duration::from_millis(300)).await;
        let response =
            query_proxies(&registry, &current_join, grace_end, shutdown_rx.clone()).await;
        assert!(response.starts_with("N 1\n"), "got {response:?}");
    }

    #[tokio::test(flavor = "current_thread")]
    async fn proxies_never_appear_in_l_and_nodes_never_in_q() {
        let registry: Registry = Arc::new(RegistryState::default());
        let current_join: CurrentJoin = Arc::new(Mutex::new(None));
        let (_shutdown_tx, shutdown_rx) = watch::channel(false);
        let ready = Instant::now();

        // One member (P upserts straight to Joined) and one proxy.
        let (mut member, server) = tcp_pair().await;
        spawn_grace_connection(server, &registry, &current_join, ready, shutdown_rx.clone());
        member
            .write_all(b"P 6 9001 9\nnode-atk-node-a")
            .await
            .unwrap();
        let mut ack = [0u8; 2];
        member.read_exact(&mut ack).await.unwrap();
        announce_proxy(
            &registry,
            &current_join,
            ready,
            shutdown_rx.clone(),
            "proxy-a",
            8358,
            "tk-a",
        )
        .await;

        let (mut client, server) = tcp_pair().await;
        spawn_grace_connection(server, &registry, &current_join, ready, shutdown_rx.clone());
        client.write_all(b"L\n").await.unwrap();
        let mut response = Vec::new();
        let mut chunk = [0u8; 1024];
        loop {
            match timeout(Duration::from_millis(300), client.read(&mut chunk)).await {
                Ok(Ok(0)) | Err(_) => break,
                Ok(Ok(bytes_read)) => response.extend_from_slice(&chunk[..bytes_read]),
                Ok(Err(_)) => break,
            }
        }
        let listed = String::from_utf8_lossy(&response).into_owned();
        assert!(listed.contains("node-a"), "got {listed:?}");
        assert!(!listed.contains("proxy-a"), "got {listed:?}");

        let proxies = query_proxies(&registry, &current_join, ready, shutdown_rx.clone()).await;
        assert!(proxies.contains("proxy-a"), "got {proxies:?}");
        assert!(!proxies.contains("node-a"), "got {proxies:?}");
    }

    #[tokio::test(flavor = "current_thread")]
    async fn the_sweep_drops_proxies_that_stopped_announcing() {
        let registry: Registry = Arc::new(RegistryState::default());
        let current_join: CurrentJoin = Arc::new(Mutex::new(None));
        let (_shutdown_tx, shutdown_rx) = watch::channel(false);
        let ready = Instant::now();
        let liveness = Duration::from_millis(200);

        let _sweep_task = tokio::spawn(sweep_expired(
            Arc::clone(&registry),
            Arc::clone(&current_join),
            None,
            None,
            2,
            ready,
            liveness,
            shutdown_rx.clone(),
        ));

        announce_proxy(
            &registry,
            &current_join,
            ready,
            shutdown_rx.clone(),
            "proxy-old",
            8358,
            "tk-old",
        )
        .await;
        announce_proxy(
            &registry,
            &current_join,
            ready,
            shutdown_rx.clone(),
            "proxy-live",
            8359,
            "tk-live",
        )
        .await;

        // Keep proxy-live fresh past proxy-old's expiry.
        for _ in 0..4 {
            tokio::time::sleep(Duration::from_millis(80)).await;
            announce_proxy(
                &registry,
                &current_join,
                ready,
                shutdown_rx.clone(),
                "proxy-live",
                8359,
                "tk-live",
            )
            .await;
        }

        let response = query_proxies(&registry, &current_join, ready, shutdown_rx.clone()).await;
        assert!(response.starts_with("N 1\n"), "got {response:?}");
        assert!(response.contains("proxy-live"), "got {response:?}");
        assert!(!response.contains("proxy-old"), "got {response:?}");
    }

    #[tokio::test(flavor = "current_thread")]
    async fn a_join_during_the_startup_grace_waits_for_the_members_to_re_announce() {
        // Issue #63: after a restart, a `J` must not be orchestrated
        // against the partial registry. Here the registry is empty when
        // the `J` arrives — before this fix the joiner was promoted on
        // the spot with no handoff at all. node-a re-announces during
        // the grace; once the grace ends the join starts, and it starts
        // from node-a.
        let registry: Registry = Arc::new(RegistryState::default());
        let current_join: CurrentJoin = Arc::new(Mutex::new(None));
        let (_shutdown_tx, shutdown_rx) = watch::channel(false);
        let grace = Duration::from_millis(400);
        let list_ready_at = Instant::now() + grace;

        let _sweep_task = tokio::spawn(sweep_expired(
            Arc::clone(&registry),
            Arc::clone(&current_join),
            None,
            None,
            2,
            list_ready_at,
            Duration::from_secs(1),
            shutdown_rx.clone(),
        ));

        let (mut joiner, server) = tcp_pair().await;
        spawn_grace_connection(
            server,
            &registry,
            &current_join,
            list_ready_at,
            shutdown_rx.clone(),
        );
        joiner
            .write_all(b"J 6 9002 9\nnode-btk-node-b")
            .await
            .unwrap();

        // Held: no promotion, no join, while the grace is running.
        let mut promoted = [0u8; 2];
        assert!(
            timeout(Duration::from_millis(150), joiner.read_exact(&mut promoted))
                .await
                .is_err(),
            "a J during the grace must not be promoted"
        );
        assert!(lock_current_join(&current_join).is_none());
        assert_eq!(lock(&registry)["node-b"].state, NodeState::Waiting);

        // A member re-announcing during the grace, as after a restart.
        let (mut member, server) = tcp_pair().await;
        spawn_grace_connection(
            server,
            &registry,
            &current_join,
            list_ready_at,
            shutdown_rx.clone(),
        );
        member
            .write_all(b"P 6 9001 9\nnode-atk-node-a")
            .await
            .unwrap();
        let mut announced = [0u8; 2];
        member.read_exact(&mut announced).await.unwrap();
        assert_eq!(&announced, b"R\n");

        // Grace over: the sweep ticker starts the join — from node-a.
        let mut started = false;
        for _ in 0..100 {
            if let Some(pending) = lock_current_join(&current_join).as_ref() {
                assert_eq!(pending.joining_name, "node-b");
                assert!(
                    pending.expected.contains_key("node-a"),
                    "the handoff must come from the member that re-announced"
                );
                started = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert!(started, "the join must start once the grace has ended");
        assert!(
            Instant::now() >= list_ready_at,
            "the join must not have started before the grace ended"
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn every_node_parked_during_the_grace_is_served_after_it() {
        // Issue #113: a fresh cluster whose nodes all register during the
        // startup grace. The post-grace kick (issue #63) runs
        // `try_begin_next_join` once; its bootstrap branch promoted the
        // first waiting node with no handoff — and no `C` to chain the
        // next join off of — so the second stayed `Waiting` forever.
        let registry: Registry = Arc::new(RegistryState::default());
        let current_join: CurrentJoin = Arc::new(Mutex::new(None));
        let (_shutdown_tx, shutdown_rx) = watch::channel(false);
        let grace = Duration::from_millis(400);
        let list_ready_at = Instant::now() + grace;

        let _sweep_task = tokio::spawn(sweep_expired(
            Arc::clone(&registry),
            Arc::clone(&current_join),
            None,
            None,
            2,
            list_ready_at,
            Duration::from_secs(1),
            shutdown_rx.clone(),
        ));

        let mut joiners = Vec::new();
        for frame in [
            &b"J 6 9002 9\nnode-btk-node-b"[..],
            &b"J 6 9003 9\nnode-ctk-node-c"[..],
        ] {
            let (mut joiner, server) = tcp_pair().await;
            spawn_grace_connection(
                server,
                &registry,
                &current_join,
                list_ready_at,
                shutdown_rx.clone(),
            );
            joiner.write_all(frame).await.unwrap();
            joiners.push(joiner);
        }

        // Both held while the grace runs.
        tokio::time::sleep(Duration::from_millis(150)).await;
        {
            let reg = lock(&registry);
            assert_eq!(reg["node-b"].state, NodeState::Waiting);
            assert_eq!(reg["node-c"].state, NodeState::Waiting);
        }

        // Grace over: one node is promoted by bootstrap (nobody to hand
        // off from), and the other must be started as a real staged join
        // against it — not left in `Waiting`.
        // Snapshot under the locks, decide outside them (clippy's
        // `await_holding_lock`).
        let observe = || {
            let reg = lock(&registry);
            let joined: Vec<String> = reg
                .iter()
                .filter(|(_, info)| info.state == NodeState::Joined)
                .map(|(name, _)| name.clone())
                .collect();
            let joining = reg.iter().any(|(_, info)| info.state == NodeState::Joining);
            drop(reg);
            let hands_off_from_joined = lock_current_join(&current_join).as_ref().map(|pending| {
                joined
                    .iter()
                    .all(|name| pending.expected.contains_key(name))
            });
            (joined.len(), joining, hands_off_from_joined)
        };

        let mut served = false;
        for _ in 0..200 {
            if let (1, true, Some(from_joined)) = observe() {
                assert!(
                    from_joined,
                    "the second join must hand off from the bootstrapped member"
                );
                served = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert!(
            served,
            "the second node parked during the grace must be started after it"
        );
    }

    /// Issue #61 helper: one `Joined` node (`node-a`, announced with `P`,
    /// which upserts straight to `Joined` — a `J` would be held back
    /// during the grace, issue #63) on a connection that then carries
    /// `H`.
    async fn joined_node_a(list_ready_at: Instant) -> (TcpStream, Registry) {
        let registry: Registry = Arc::new(RegistryState::default());
        let current_join: CurrentJoin = Arc::new(Mutex::new(None));
        let (_shutdown_tx, shutdown_rx) = watch::channel(false);
        let (mut node_a, server_a) = tcp_pair().await;
        tokio::spawn(handle_connection(
            MaybeTls::Plain(server_a),
            std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST),
            Arc::clone(&registry),
            current_join,
            ConnectionConfig {
                idle_timeout: IDLE_TIMEOUT,
                list_ready_at,
                replication: 2,
                auth_secret: None,
                tls_acceptor: None,
                tls_connector: None,
                announce_limiter: Arc::new(Mutex::new(FxHashMap::default())),
            },
            shutdown_rx,
            Arc::new(std::sync::Mutex::new(None)),
        ));
        node_a
            .write_all(b"P 6 9001 9\nnode-atk-node-a")
            .await
            .unwrap();
        let mut promoted = [0u8; 2];
        node_a.read_exact(&mut promoted).await.unwrap();
        assert_eq!(&promoted, b"R\n");
        // Keep the shutdown sender alive for the connection's lifetime.
        std::mem::forget(_shutdown_tx);
        (node_a, registry)
    }

    async fn read_exactly(stream: &mut TcpStream, len: usize) -> Vec<u8> {
        let mut buf = vec![0u8; len];
        stream.read_exact(&mut buf).await.unwrap();
        buf
    }

    #[tokio::test(flavor = "current_thread")]
    async fn a_heartbeat_ack_carries_the_joined_roster() {
        // Issue #61: the ack is `A <count> <r>\n` plus `L`-shaped entries.
        let (mut node_a, _registry) = joined_node_a(Instant::now()).await;
        node_a.write_all(b"H 6 2 9\nnode-atk-node-a").await.unwrap();
        let expected = b"A 1 2\n6 14\nnode-a127.0.0.1:9001\n";
        let ack = read_exactly(&mut node_a, expected.len()).await;
        assert_eq!(ack, expected);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn a_heartbeat_ack_withholds_the_roster_during_the_startup_grace() {
        // A partial, still-re-filling registry must not be pushed to nodes
        // (they'd reject keys owned by members that haven't re-announced).
        let (mut node_a, _registry) =
            joined_node_a(Instant::now() + Duration::from_secs(3600)).await;
        node_a.write_all(b"H 6 2 9\nnode-atk-node-a").await.unwrap();
        let ack = read_exactly(&mut node_a, 2).await;
        assert_eq!(ack, b"A\n");
        // Nothing else follows: a second heartbeat's ack is the very next
        // thing on the wire.
        node_a.write_all(b"H 6 2 9\nnode-atk-node-a").await.unwrap();
        let ack = read_exactly(&mut node_a, 2).await;
        assert_eq!(ack, b"A\n");
    }

    #[tokio::test(flavor = "current_thread")]
    async fn a_heartbeat_ack_withholds_the_roster_when_a_majority_disputes_replication() {
        // Issue #30 applies to the `H` ack exactly as to `L`: the only
        // voter reports r=3 against this replica's 2, so the roster (and
        // the disputed r it would embed) is withheld.
        let (mut node_a, _registry) = joined_node_a(Instant::now()).await;
        node_a.write_all(b"H 6 3 9\nnode-atk-node-a").await.unwrap();
        let ack = read_exactly(&mut node_a, 2).await;
        assert_eq!(ack, b"A\n");
        // Back in agreement: the roster is served again.
        node_a.write_all(b"H 6 2 9\nnode-atk-node-a").await.unwrap();
        let expected = b"A 1 2\n6 14\nnode-a127.0.0.1:9001\n";
        let ack = read_exactly(&mut node_a, expected.len()).await;
        assert_eq!(ack, expected);
    }

    async fn registry_with_a_joined_and_b_waiting(
        shutdown_rx: watch::Receiver<bool>,
    ) -> (TcpStream, TcpStream, Registry, CurrentJoin) {
        registry_with_a_joined_and_b_waiting_idle(shutdown_rx, IDLE_TIMEOUT).await
    }

    async fn registry_with_a_joined_and_b_waiting_idle(
        shutdown_rx: watch::Receiver<bool>,
        idle_timeout: Duration,
    ) -> (TcpStream, TcpStream, Registry, CurrentJoin) {
        let registry: Registry = Arc::new(RegistryState::default());
        let current_join: CurrentJoin = Arc::new(Mutex::new(None));

        let config = || ConnectionConfig {
            idle_timeout,
            list_ready_at: Instant::now(),
            replication: 2,
            auth_secret: None,
            tls_acceptor: None,
            tls_connector: None,
            announce_limiter: Arc::new(Mutex::new(FxHashMap::default())),
        };

        let (mut node_a, server_a) = tcp_pair().await;
        tokio::spawn(handle_connection(
            MaybeTls::Plain(server_a),
            std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST),
            Arc::clone(&registry),
            Arc::clone(&current_join),
            config(),
            shutdown_rx.clone(),
            Arc::new(std::sync::Mutex::new(None)),
        ));
        node_a
            .write_all(b"J 6 9001 9\nnode-atk-node-a")
            .await
            .unwrap();
        let mut promoted = [0u8; 2];
        node_a.read_exact(&mut promoted).await.unwrap();
        assert_eq!(&promoted, b"R\n");

        let (mut node_b, server_b) = tcp_pair().await;
        tokio::spawn(handle_connection(
            MaybeTls::Plain(server_b),
            std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST),
            Arc::clone(&registry),
            Arc::clone(&current_join),
            config(),
            shutdown_rx.clone(),
            Arc::new(std::sync::Mutex::new(None)),
        ));
        node_b
            .write_all(b"J 6 9002 9\nnode-btk-node-b")
            .await
            .unwrap();

        for _ in 0..1000 {
            if lock_current_join(&current_join).is_some() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
        assert!(lock_current_join(&current_join).is_some());

        (node_a, node_b, registry, current_join)
    }

    #[tokio::test(flavor = "current_thread")]
    async fn an_abandoned_join_wakes_and_closes_the_waiting_connection() {
        // Regression for issue #4: abandoning a join must not strand the
        // joining node's held-open connection — it must observe the
        // rejection (connection closed) so the node redials and re-joins.
        let (_node_a, mut node_b, registry, current_join) = {
            let (_shutdown_tx, shutdown_rx) = watch::channel(false);
            // Keep the sender alive for the test's duration by leaking it
            // into the tuple below via _node_a's lifetime; the tasks only
            // need the receiver clones they already hold.
            let tuple = registry_with_a_joined_and_b_waiting(shutdown_rx).await;
            std::mem::forget(_shutdown_tx);
            tuple
        };

        let join = lock_current_join(&current_join)
            .as_ref()
            .unwrap()
            .identity();
        abandon_current_join(
            &registry,
            &current_join,
            &None,
            &None,
            2,
            Instant::now(),
            &join,
            "test",
        )
        .await;

        // node-b's connection must observe the abandonment promptly.
        let mut byte = [0u8; 1];
        let read = tokio::time::timeout(Duration::from_secs(5), node_b.read(&mut byte))
            .await
            .expect("the waiting connection was stranded by the abandoned join");
        assert_eq!(
            read.unwrap(),
            0,
            "expected the connection to close, not data"
        );
        assert!(!lock(&registry).contains_key("node-b"));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn a_node_promoted_after_waiting_longer_than_idle_timeout_keeps_its_connection() {
        // The idle deadline used to be anchored when `J` was parsed, so a
        // node that waited longer than the idle timeout in the join queue
        // was promoted, answered `R`, and then closed as idle before its
        // first `H` could arrive. The clock for the next command now starts
        // when the `J` handler (the whole wait) is done.
        //
        // Real time with a short idle timeout, not a paused clock: with
        // real sockets a paused runtime auto-advances the clock whenever
        // every task is parked on I/O, which fired the server's fresh idle
        // timer while the `H` was still in flight (about one run in three).
        let idle_timeout = Duration::from_millis(400);
        let (_shutdown_tx, shutdown_rx) = watch::channel(false);
        let (_node_a, mut node_b, registry, _current_join) =
            registry_with_a_joined_and_b_waiting_idle(shutdown_rx, idle_timeout).await;

        // Outwait the idle timeout while parked in `wait_for_promotion`
        // (which has none), then promote node-b the way a completed
        // handoff does.
        tokio::time::sleep(idle_timeout + Duration::from_millis(200)).await;
        {
            let mut guard = lock(&registry);
            let info = guard.get_mut("node-b").expect("node-b is registered");
            info.state = NodeState::Joined;
            info.promoted.notify_one();
        }
        assert_eq!(read_exactly(&mut node_b, 2).await, b"R\n");

        // The first heartbeat, sent promptly after `R`, must be answered
        // on the same connection rather than find it closed.
        node_b.write_all(b"H 6 2 9\nnode-btk-node-b").await.unwrap();
        let mut ack = [0u8; 2];
        tokio::time::timeout(Duration::from_secs(5), node_b.read_exact(&mut ack))
            .await
            .expect("no heartbeat ack arrived on the same connection")
            .expect("the promoted node's connection was closed as idle");
        assert_eq!(&ack, b"A ");
    }

    #[tokio::test(flavor = "current_thread")]
    async fn an_explicit_v_wakes_and_closes_a_parked_waiting_connection() {
        // Regression for issue #297 (a): every other removal path
        // (`on_node_connection_ended`, `abandon_current_join`, the
        // waiting-eviction in `sweep_expired`) wakes a removed entry's
        // `promoted` `Notify` before dropping it, so a connection parked in
        // `wait_for_promotion` (idle timeout deliberately disabled while it
        // waits) is woken rather than stranded holding its
        // `MAX_CONNECTIONS` permit forever. An explicit `V` removing a
        // Waiting/Joining entry must do the same.
        let (_shutdown_tx, shutdown_rx) = watch::channel(false);
        let (_node_a, mut node_b, registry, current_join) = {
            let tuple = registry_with_a_joined_and_b_waiting(shutdown_rx.clone()).await;
            std::mem::forget(_shutdown_tx);
            tuple
        };
        assert_eq!(
            lock(&registry).get("node-b").map(|info| info.state),
            Some(NodeState::Joining),
            "node-b must still be parked in wait_for_promotion, not yet promoted"
        );

        let (mut caller, server) = tcp_pair().await;
        tokio::spawn(handle_connection(
            MaybeTls::Plain(server),
            std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST),
            Arc::clone(&registry),
            Arc::clone(&current_join),
            ConnectionConfig {
                idle_timeout: IDLE_TIMEOUT,
                list_ready_at: Instant::now(),
                replication: 2,
                auth_secret: None,
                tls_acceptor: None,
                tls_connector: None,
                announce_limiter: Arc::new(Mutex::new(FxHashMap::default())),
            },
            shutdown_rx,
            Arc::new(std::sync::Mutex::new(None)),
        ));

        caller.write_all(b"V 6 9\nnode-btk-node-b").await.unwrap();
        let mut ack = [0u8; 2];
        caller.read_exact(&mut ack).await.unwrap();
        assert_eq!(&ack, b"R\n");

        // node-b's parked connection must observe the removal promptly —
        // an error/close, matching every other removal path's behavior —
        // instead of hanging forever.
        let mut byte = [0u8; 1];
        let read = tokio::time::timeout(Duration::from_secs(5), node_b.read(&mut byte))
            .await
            .expect("the waiting connection was stranded by the V removal");
        assert_eq!(
            read.unwrap(),
            0,
            "expected the connection to close, not data"
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn duplicate_j_connections_under_one_name_both_receive_the_promotion() {
        // Regression for issue #7: a second live `J` under the same name
        // (a redial racing a half-open old connection) shares the entry's
        // Notify; promotion must wake both, not just one.
        let (_shutdown_tx, shutdown_rx) = watch::channel(false);
        let (mut node_a, mut node_b_first, registry, current_join) =
            registry_with_a_joined_and_b_waiting(shutdown_rx.clone()).await;

        let config = ConnectionConfig {
            idle_timeout: IDLE_TIMEOUT,
            list_ready_at: Instant::now(),
            replication: 2,
            auth_secret: None,
            tls_acceptor: None,
            tls_connector: None,
            announce_limiter: Arc::new(Mutex::new(FxHashMap::default())),
        };
        let (mut node_b_second, server) = tcp_pair().await;
        tokio::spawn(handle_connection(
            MaybeTls::Plain(server),
            std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST),
            Arc::clone(&registry),
            Arc::clone(&current_join),
            config,
            shutdown_rx,
            Arc::new(std::sync::Mutex::new(None)),
        ));
        node_b_second
            .write_all(b"J 6 9002 9\nnode-btk-node-b")
            .await
            .unwrap();
        tokio::time::sleep(Duration::from_millis(20)).await; // let it park

        node_a
            .write_all(b"C 6 6 9\nnode-anode-btk-node-a")
            .await
            .unwrap();
        let mut ack = [0u8; 2];
        node_a.read_exact(&mut ack).await.unwrap();

        for stream in [&mut node_b_first, &mut node_b_second] {
            let mut response = [0u8; 2];
            tokio::time::timeout(Duration::from_secs(5), stream.read_exact(&mut response))
                .await
                .expect("a duplicate J connection was never woken by the promotion")
                .unwrap();
            assert_eq!(&response, b"R\n");
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn on_node_connection_ended_wakes_a_surviving_duplicate_j_connection() {
        // Regression for issue #9: `on_node_connection_ended`'s registry
        // removal for a Waiting/Joining node used to skip the `Notify`
        // that `abandon_current_join`'s own removal already fires. A
        // duplicate `J` under the same name (issue #7) shares that
        // `Notify` across two parked connections; without waking it, the
        // connection that wasn't the one reported ended would hang in
        // `wait_for_promotion` forever.
        //
        // Also covers issue #3/#9's amendment: `start_join` hands
        // ownership of the shared entry to whichever connection most
        // recently `J`ed (see `NodeInfo::owner_connection_id`), so the
        // *first* connection ending — now a stale, superseded owner — must
        // be a no-op, and only the second (current owner) connection
        // ending actually removes the entry and wakes the (now sole)
        // surviving parked connection, the first.
        let (_shutdown_tx, shutdown_rx) = watch::channel(false);
        let registry: Registry = Arc::new(RegistryState::default());
        // A different join already in progress, so node-b's own `J`
        // leaves it parked at `Waiting` rather than immediately becoming
        // the current join's own joining node (which `abandon_current_join`
        // already handles correctly, see the sibling tests above).
        lock(&registry).insert(
            "node-a".to_string(),
            NodeInfo::new(
                "127.0.0.1:1".to_string(),
                NodeState::Joined,
                "tk-node-a".to_string(),
            ),
        );
        let current_join: CurrentJoin = Arc::new(Mutex::new(Some(PendingJoin {
            joining_name: "node-x".to_string(),
            expected: [("node-a".to_string(), "tk-node-a".to_string())]
                .into_iter()
                .collect(),
            completed: HashSet::new(),
            started_at: Instant::now(),
            max_entries: 0,
            roster: Arc::new(Vec::new()),
            late: LateHandoffs::default(),
            generation: 1,
        })));

        let config = || ConnectionConfig {
            idle_timeout: IDLE_TIMEOUT,
            list_ready_at: Instant::now(),
            replication: 2,
            auth_secret: None,
            tls_acceptor: None,
            tls_connector: None,
            announce_limiter: Arc::new(Mutex::new(FxHashMap::default())),
        };

        let (mut node_b_first, server_first) = tcp_pair().await;
        tokio::spawn(handle_connection(
            MaybeTls::Plain(server_first),
            std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST),
            Arc::clone(&registry),
            Arc::clone(&current_join),
            config(),
            shutdown_rx.clone(),
            Arc::new(std::sync::Mutex::new(None)),
        ));
        node_b_first
            .write_all(b"J 6 9002 9\nnode-btk-node-b")
            .await
            .unwrap();
        tokio::time::sleep(Duration::from_millis(20)).await; // let it park

        // The first connection's own id, captured before the duplicate `J`
        // below overwrites it — used below to simulate that connection
        // (now stale) ending.
        let stale_id = lock(&registry).get("node-b").unwrap().owner_connection_id;

        let (mut node_b_second, server_second) = tcp_pair().await;
        tokio::spawn(handle_connection(
            MaybeTls::Plain(server_second),
            std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST),
            Arc::clone(&registry),
            Arc::clone(&current_join),
            config(),
            shutdown_rx.clone(),
            Arc::new(std::sync::Mutex::new(None)),
        ));
        node_b_second
            .write_all(b"J 6 9002 9\nnode-btk-node-b")
            .await
            .unwrap();
        tokio::time::sleep(Duration::from_millis(20)).await; // let it park too

        assert!(lock(&registry).contains_key("node-b"));
        let owning_id = lock(&registry).get("node-b").unwrap().owner_connection_id;
        assert_ne!(
            stale_id, owning_id,
            "the duplicate J must have taken over ownership of the entry"
        );

        // Issue #3/#9: the stale, superseded first connection reporting in
        // — as `run`'s connection-task wrapper eventually would, once it
        // notices that half-open connection is actually dead — must be a
        // no-op. The entry, and the still-live second connection, survive.
        on_node_connection_ended(
            &registry,
            &current_join,
            &None,
            &None,
            2,
            Instant::now(),
            "node-b",
            stale_id,
        )
        .await;
        assert!(
            lock(&registry).contains_key("node-b"),
            "a stale, superseded connection's own id must not remove the entry"
        );
        let mut probe = [0u8; 1];
        assert!(
            tokio::time::timeout(Duration::from_millis(50), node_b_second.read(&mut probe))
                .await
                .is_err(),
            "the surviving connection must not have been woken by the stale no-op"
        );

        // Now the connection actually recorded as owning the entry (the
        // second) ends for real: this must remove the entry and wake every
        // parked connection sharing its `Notify` — including the first,
        // which is still open and parked even though it's no longer the
        // owner (issue #9's original regression).
        on_node_connection_ended(
            &registry,
            &current_join,
            &None,
            &None,
            2,
            Instant::now(),
            "node-b",
            owning_id,
        )
        .await;

        let mut byte = [0u8; 1];
        let read = tokio::time::timeout(Duration::from_secs(5), node_b_first.read(&mut byte))
            .await
            .expect("the surviving duplicate connection was stranded");
        assert_eq!(
            read.unwrap(),
            0,
            "expected the connection to close, not data"
        );
        assert!(!lock(&registry).contains_key("node-b"));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn a_complete_for_a_different_join_is_ignored() {
        // Regression for issue #5: a stale C naming an earlier, abandoned
        // join must not be credited to the current one.
        let (_shutdown_tx, shutdown_rx) = watch::channel(false);
        let (mut node_a, mut node_b, registry, _current_join) =
            registry_with_a_joined_and_b_waiting(shutdown_rx).await;

        // Stale: names a join for "node-x", not the pending one for node-b.
        node_a
            .write_all(b"C 6 6 9\nnode-anode-xtk-node-a")
            .await
            .unwrap();
        let mut ack = [0u8; 2];
        node_a.read_exact(&mut ack).await.unwrap();
        assert_eq!(&ack, b"A\n");

        tokio::time::sleep(Duration::from_millis(20)).await;
        assert_ne!(
            lock(&registry).get("node-b").map(|info| info.state),
            Some(NodeState::Joined),
            "a stale C was credited to the wrong join"
        );

        // The genuine report still promotes.
        node_a
            .write_all(b"C 6 6 9\nnode-anode-btk-node-a")
            .await
            .unwrap();
        node_a.read_exact(&mut ack).await.unwrap();
        let mut promoted = [0u8; 2];
        tokio::time::timeout(Duration::from_secs(5), node_b.read_exact(&mut promoted))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(&promoted, b"R\n");
    }

    #[tokio::test(flavor = "current_thread")]
    async fn a_complete_with_the_wrong_token_is_not_credited() {
        // Issue #34: a forged C would promote the joining node before it
        // actually holds the reporter's share of the keyspace, so a
        // report that can't present the reporter's registered token is
        // ignored (same shape as issue #5's stale-report handling).
        let (_shutdown_tx, shutdown_rx) = watch::channel(false);
        let (mut node_a, mut node_b, registry, _current_join) =
            registry_with_a_joined_and_b_waiting(shutdown_rx).await;

        node_a
            .write_all(b"C 6 6 8\nnode-anode-bevil-tok")
            .await
            .unwrap();
        let mut ack = [0u8; 2];
        node_a.read_exact(&mut ack).await.unwrap();
        assert_eq!(&ack, b"A\n");

        tokio::time::sleep(Duration::from_millis(20)).await;
        assert_ne!(
            lock(&registry).get("node-b").map(|info| info.state),
            Some(NodeState::Joined),
            "a wrong-token C was credited to the join"
        );

        // The genuine report still promotes.
        node_a
            .write_all(b"C 6 6 9\nnode-anode-btk-node-a")
            .await
            .unwrap();
        node_a.read_exact(&mut ack).await.unwrap();
        let mut promoted = [0u8; 2];
        tokio::time::timeout(Duration::from_secs(5), node_b.read_exact(&mut promoted))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(&promoted, b"R\n");
    }

    #[test]
    fn credit_migrate_send_result_ignores_a_stale_generation_under_the_same_name() {
        // Regression for issue #421(c): a stale M-send result from an
        // abandoned PendingJoin must not inflate max_entries for a fresh
        // PendingJoin that re-registered under the very same
        // `joining_name` — `joining_name` alone isn't a safe enough guard,
        // since a node's own name is exactly what's reused across
        // incarnations.
        let mut join_guard = Some(PendingJoin {
            joining_name: "node-x".to_string(),
            expected: HashMap::new(),
            completed: HashSet::new(),
            started_at: Instant::now(),
            max_entries: 5,
            roster: Arc::new(Vec::new()),
            late: LateHandoffs::default(),
            generation: 2,
        });

        // A result from generation 1 (the abandoned incarnation) must not
        // touch generation 2's max_entries, even though the name matches.
        credit_migrate_send_result(&mut join_guard, "node-x", 1, 999);
        assert_eq!(join_guard.as_ref().unwrap().max_entries, 5);

        // A result naming a different join entirely is still ignored too
        // (issue #5's existing guard, unaffected by this fix).
        credit_migrate_send_result(&mut join_guard, "node-y", 2, 999);
        assert_eq!(join_guard.as_ref().unwrap().max_entries, 5);

        // A result for the current incarnation (matching name AND
        // generation) is credited, same as before this fix.
        credit_migrate_send_result(&mut join_guard, "node-x", 2, 42);
        assert_eq!(join_guard.as_ref().unwrap().max_entries, 42);

        // Crediting again only ever raises the max (size-derived migration
        // timeout is sized by the largest report seen).
        credit_migrate_send_result(&mut join_guard, "node-x", 2, 10);
        assert_eq!(join_guard.as_ref().unwrap().max_entries, 42);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn a_forged_complete_after_a_ready_members_eviction_and_reregistration_is_rejected() {
        // Issue #34 forged-completion fix (see `PendingJoin::expected`'s
        // doc comment): node names are public via `L`, and `P` for an
        // unknown name is trust-on-first-use (per-node membership tokens) — so once a ready
        // member's registry entry is gone for any reason, an attacker can
        // re-register its name under a token of their own choosing. Before
        // this fix, `handle_complete` checked a `C`'s token against
        // whatever the *live* registry entry for that name currently held,
        // so this would forge a credited handoff the real node-a never
        // performed. The registry entry is removed directly here (not via
        // `sweep_expired`) so this test isolates the token-snapshot check
        // itself, independent of the sibling fix that abandons a join when
        // `sweep_expired` evicts one of its own ready members.
        let (_shutdown_tx, shutdown_rx) = watch::channel(false);
        let (_node_a, mut node_b, registry, current_join) =
            registry_with_a_joined_and_b_waiting(shutdown_rx.clone()).await;

        // node-a's entry disappears (crash, out-of-band eviction, ...)
        // while the join is still pending on it.
        assert!(lock(&registry).remove("node-a").is_some());

        // An attacker re-registers the now-free name with a token of its
        // own choosing — trust-on-first-use accepts it, exactly as it
        // would for a legitimate standby learning the name for the first
        // time (per-node membership tokens).
        let config = ConnectionConfig {
            idle_timeout: IDLE_TIMEOUT,
            list_ready_at: Instant::now(),
            replication: 2,
            auth_secret: None,
            tls_acceptor: None,
            tls_connector: None,
            announce_limiter: Arc::new(Mutex::new(FxHashMap::default())),
        };
        let (mut attacker, server) = tcp_pair().await;
        tokio::spawn(handle_connection(
            MaybeTls::Plain(server),
            std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST),
            Arc::clone(&registry),
            Arc::clone(&current_join),
            config,
            shutdown_rx,
            Arc::new(std::sync::Mutex::new(None)),
        ));
        attacker
            .write_all(b"P 6 9001 10\nnode-aevil-token")
            .await
            .unwrap();
        let mut announce_ack = [0u8; 2];
        attacker.read_exact(&mut announce_ack).await.unwrap();
        assert_eq!(&announce_ack, b"R\n");
        assert_eq!(
            lock(&registry).get("node-a").map(|info| info.token.clone()),
            Some("evil-token".to_string()),
            "the attacker's re-registration must have won the name under its own token"
        );

        // The forged completion report, presenting the attacker's own
        // (now genuinely "registered") token — this is exactly the
        // report a check against the *live* registry entry would accept.
        attacker
            .write_all(b"C 6 6 10\nnode-anode-bevil-token")
            .await
            .unwrap();
        let mut complete_ack = [0u8; 2];
        attacker.read_exact(&mut complete_ack).await.unwrap();
        assert_eq!(&complete_ack, b"A\n");

        tokio::time::sleep(Duration::from_millis(20)).await;
        assert_ne!(
            lock(&registry).get("node-b").map(|info| info.state),
            Some(NodeState::Joined),
            "a forged C from a re-registered name was credited to a handoff it never \
             performed"
        );
        // node-b's connection must still be parked, not promoted.
        let mut byte = [0u8; 1];
        let woken = tokio::time::timeout(Duration::from_millis(50), node_b.read(&mut byte)).await;
        assert!(
            woken.is_err(),
            "node-b must not have been promoted by the forged report"
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn a_stale_generation_complete_after_abandon_and_reregister_is_rejected() {
        // Tenth-pass audit (2026-09-02), issue #421(c)/#34: the scenario
        // `handle_complete`'s own doc comment describes. `joining_name`
        // and the reporting node's token (issues #5 and #34) are both
        // necessary but not sufficient to identify which incarnation of a
        // join a `C` belongs to: a joining node's own name is exactly
        // what gets reused when its join is abandoned
        // (`abandon_current_join`, e.g. its control connection dropped,
        // or it crashed and restarted) and it immediately re-registers.
        // `try_begin_next_join` then starts a brand-new `PendingJoin` — a
        // new generation — against the very same ready roster the
        // abandoned join used, and a ready member's `C` still in flight
        // for the OLD incarnation names the right `joining_name` and
        // presents a genuinely valid token, but must not be credited to
        // the NEW one.
        let (_shutdown_tx, shutdown_rx) = watch::channel(false);
        let (mut node_a, _node_b, registry, current_join) =
            registry_with_a_joined_and_b_waiting(shutdown_rx.clone()).await;

        // The abandoned incarnation's own generation, captured while it's
        // still the pending one.
        let stale_generation = lock_current_join(&current_join)
            .as_ref()
            .unwrap()
            .generation;

        let join = lock_current_join(&current_join)
            .as_ref()
            .unwrap()
            .identity();
        abandon_current_join(
            &registry,
            &current_join,
            &None,
            &None,
            2,
            Instant::now(),
            &join,
            "test: superseded by an immediate re-registration",
        )
        .await;
        assert!(
            lock_current_join(&current_join).is_none(),
            "abandon must have cleared the slot"
        );

        // node-b immediately re-registers under the very same name — on
        // a NEW connection, standing in for a crash-and-restart (the
        // original `node_b` connection from the helper is left
        // unread/unwritten from here on; abandon already stranded its
        // registry entry).
        let config = ConnectionConfig {
            idle_timeout: IDLE_TIMEOUT,
            list_ready_at: Instant::now(),
            replication: 2,
            auth_secret: None,
            tls_acceptor: None,
            tls_connector: None,
            announce_limiter: Arc::new(Mutex::new(FxHashMap::default())),
        };
        let (mut node_b, server_b) = tcp_pair().await;
        tokio::spawn(handle_connection(
            MaybeTls::Plain(server_b),
            std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST),
            Arc::clone(&registry),
            Arc::clone(&current_join),
            config,
            shutdown_rx.clone(),
            Arc::new(std::sync::Mutex::new(None)),
        ));
        node_b
            .write_all(b"J 6 9002 9\nnode-btk-node-b")
            .await
            .unwrap();

        for _ in 0..1000 {
            if lock_current_join(&current_join).is_some() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
        let fresh_generation = lock_current_join(&current_join)
            .as_ref()
            .unwrap()
            .generation;
        assert_ne!(
            fresh_generation, stale_generation,
            "the re-registration must have started a fresh PendingJoin incarnation"
        );

        // node-a's still-in-flight `C` for the ABANDONED incarnation
        // arrives late: right `joining_name`, a genuinely valid token —
        // only the generation is stale.
        node_a
            .write_all(format!("C 6 6 9 {stale_generation}\nnode-anode-btk-node-a").as_bytes())
            .await
            .unwrap();
        let mut ack = [0u8; 2];
        node_a.read_exact(&mut ack).await.unwrap();
        assert_eq!(
            &ack, b"A\n",
            "a rejected C is still acked (issue #5's own precedent)"
        );

        tokio::time::sleep(Duration::from_millis(20)).await;
        assert_ne!(
            lock(&registry).get("node-b").map(|info| info.state),
            Some(NodeState::Joined),
            "a stale-generation C was credited to the superseding join"
        );
        assert!(
            lock_current_join(&current_join).is_some(),
            "the fresh incarnation must still be pending, waiting on node-a's real report"
        );
        let mut byte = [0u8; 1];
        let woken = tokio::time::timeout(Duration::from_millis(50), node_b.read(&mut byte)).await;
        assert!(
            woken.is_err(),
            "node-b must not have been promoted by the stale-generation report"
        );

        // The genuine report, echoing the CURRENT generation, still
        // promotes.
        node_a
            .write_all(format!("C 6 6 9 {fresh_generation}\nnode-anode-btk-node-a").as_bytes())
            .await
            .unwrap();
        node_a.read_exact(&mut ack).await.unwrap();
        let mut promoted = [0u8; 2];
        tokio::time::timeout(Duration::from_secs(5), node_b.read_exact(&mut promoted))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(&promoted, b"R\n");
    }

    #[tokio::test(flavor = "current_thread")]
    async fn a_legacy_complete_without_a_generation_field_is_still_credited() {
        // Tenth-pass audit (2026-09-02), issue #421(c): rolling-upgrade
        // compatibility — a `C` from a node whose own `M` never carried a
        // generation (an old, not-yet-upgraded discovery peer sent it, or
        // the node itself predates this fix) omits the field entirely
        // (`DiscoveryCommand::Complete::generation` parses as `None`).
        // `handle_complete` must accept it exactly as it did before this
        // fix — name+token only — even though the in-progress
        // `PendingJoin` this discovery process itself started very much
        // has a real, nonzero generation. See `handle_complete`'s own
        // doc comment for why this is the deliberate compatibility
        // tradeoff: the generation protection only fully holds once
        // every node and discovery replica has upgraded.
        let (_shutdown_tx, shutdown_rx) = watch::channel(false);
        let (mut node_a, mut node_b, registry, current_join) =
            registry_with_a_joined_and_b_waiting(shutdown_rx).await;

        assert!(
            lock_current_join(&current_join)
                .as_ref()
                .unwrap()
                .generation
                > 0,
            "sanity: this discovery process itself tracks a real generation for the join"
        );

        // The legacy, generation-free three-field form.
        node_a
            .write_all(b"C 6 6 9\nnode-anode-btk-node-a")
            .await
            .unwrap();
        let mut ack = [0u8; 2];
        node_a.read_exact(&mut ack).await.unwrap();
        assert_eq!(&ack, b"A\n");

        let mut promoted = [0u8; 2];
        tokio::time::timeout(Duration::from_secs(5), node_b.read_exact(&mut promoted))
            .await
            .expect("a legacy C must still be credited during a rolling upgrade")
            .unwrap();
        assert_eq!(&promoted, b"R\n");
        assert_eq!(
            lock(&registry).get("node-b").map(|info| info.state),
            Some(NodeState::Joined)
        );
    }

    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn sweep_expired_abandons_the_join_when_an_expected_ready_member_is_evicted() {
        // Issue #34 forged-completion fix, other half (see
        // `PendingJoin::expected`'s doc comment and the sibling test
        // above): `sweep_expired`'s ordinary liveness-eviction path must
        // not leave a join dangling on a ready member it just removed —
        // otherwise the join either hangs until `migration_timeout_for`,
        // or leaves a window for the forged-report attack the sibling
        // test exercises directly.
        let registry: Registry = Arc::new(RegistryState::default());
        lock(&registry).insert(
            "node-a".to_string(),
            NodeInfo::new(
                "127.0.0.1:1".to_string(),
                NodeState::Joined,
                "tk-node-a".to_string(),
            ),
        );
        lock(&registry).insert(
            "node-b".to_string(),
            NodeInfo::new(
                "127.0.0.1:2".to_string(),
                NodeState::Joining,
                "tk-node-b".to_string(),
            ),
        );

        let current_join: CurrentJoin = Arc::new(Mutex::new(Some(PendingJoin {
            joining_name: "node-b".to_string(),
            expected: [("node-a".to_string(), "tk-node-a".to_string())]
                .into_iter()
                .collect(),
            completed: HashSet::new(),
            started_at: Instant::now(),
            max_entries: 0,
            roster: Arc::new(Vec::new()),
            late: LateHandoffs::default(),
            generation: 1,
        })));

        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let sweep_task = tokio::spawn(sweep_expired(
            Arc::clone(&registry),
            Arc::clone(&current_join),
            None,
            None,
            2,
            Instant::now(),
            Duration::from_secs(1),
            shutdown_rx,
        ));

        tokio::task::yield_now().await;
        // In steps well under half the 1 s liveness timeout: one jump of 2 s
        // would be a stalled sweep, which evicts nothing (issue #1312).
        advance_in_steps(Duration::from_secs(2), Duration::from_millis(100)).await;

        // node-a's own liveness eviction (well under the migration
        // timeout) must have abandoned the join immediately, not left it
        // to `migration_timeout_for` to eventually reap.
        assert!(
            lock_current_join(&current_join).is_none(),
            "the join must be abandoned once one of its expected ready members is evicted"
        );
        assert!(!lock(&registry).contains_key("node-a"));
        assert!(
            !lock(&registry).contains_key("node-b"),
            "abandon_current_join must also strand the joining node's own entry"
        );

        shutdown_tx.send_replace(true);
        sweep_task.await.unwrap();
    }

    #[tokio::test(flavor = "current_thread")]
    async fn an_explicit_v_from_an_expected_ready_member_abandons_the_in_flight_join() {
        // Regression for issue #297 (b): the sibling test above covers
        // `sweep_expired`'s liveness-eviction path abandoning a join whose
        // expected ready member just got evicted (issue #34
        // forged-completion fix). An explicit, authenticated `V` from that
        // same ready member reaches the identical condition a different
        // way and must abandon the join just as promptly — left alone, it
        // would otherwise stall until `migration_timeout_for` (up to 2h)
        // eventually reaps it.
        let registry: Registry = Arc::new(RegistryState::default());
        lock(&registry).insert(
            "node-a".to_string(),
            NodeInfo::new(
                "127.0.0.1:1".to_string(),
                NodeState::Joined,
                "tk-node-a".to_string(),
            ),
        );
        lock(&registry).insert(
            "node-b".to_string(),
            NodeInfo::new(
                "127.0.0.1:2".to_string(),
                NodeState::Joining,
                "tk-node-b".to_string(),
            ),
        );

        let current_join: CurrentJoin = Arc::new(Mutex::new(Some(PendingJoin {
            joining_name: "node-b".to_string(),
            expected: [("node-a".to_string(), "tk-node-a".to_string())]
                .into_iter()
                .collect(),
            completed: HashSet::new(),
            started_at: Instant::now(),
            max_entries: 0,
            roster: Arc::new(Vec::new()),
            late: LateHandoffs::default(),
            generation: 1,
        })));

        let (_shutdown_tx, shutdown_rx) = watch::channel(false);
        let (mut node_a, server) = tcp_pair().await;
        tokio::spawn(handle_connection(
            MaybeTls::Plain(server),
            std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST),
            Arc::clone(&registry),
            Arc::clone(&current_join),
            ConnectionConfig {
                idle_timeout: IDLE_TIMEOUT,
                list_ready_at: Instant::now(),
                replication: 2,
                auth_secret: None,
                tls_acceptor: None,
                tls_connector: None,
                announce_limiter: Arc::new(Mutex::new(FxHashMap::default())),
            },
            shutdown_rx,
            Arc::new(std::sync::Mutex::new(None)),
        ));

        node_a.write_all(b"V 6 9\nnode-atk-node-a").await.unwrap();
        let mut ack = [0u8; 2];
        node_a.read_exact(&mut ack).await.unwrap();
        assert_eq!(&ack, b"R\n");

        // `R` no longer waits for the abandon (issue #1662): wait for it.
        wait_until("the abandon", Duration::from_secs(3), || {
            lock_current_join(&current_join).is_none()
        })
        .await;
        assert!(
            lock_current_join(&current_join).is_none(),
            "the join must be abandoned once an expected ready member explicitly leaves"
        );
        assert!(!lock(&registry).contains_key("node-a"));
        assert!(
            !lock(&registry).contains_key("node-b"),
            "abandon_current_join must also strand the joining node's own entry"
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn an_explicit_v_is_acked_before_the_abandon_fan_out_finishes() {
        // Issue #1662: `V` removes the node first and used to answer `R`
        // only after `abandon_current_join`, whose `X` to every remaining
        // ready member can take `OUTBOUND_IO_TIMEOUT` each. Here node-b
        // accepts the `X` and never answers it for far longer than this
        // test runs: node-a's `R` must arrive anyway, and the abandon must
        // still happen (join taken, `X` delivered to node-b).
        let (node_b_addr, accepted, _acked) = slow_cancel_node(Duration::from_secs(30)).await;
        let registry: Registry = Arc::new(RegistryState::default());
        for (name, address, state) in [
            ("node-a", "127.0.0.1:1", NodeState::Joined),
            ("node-b", node_b_addr.as_str(), NodeState::Joined),
            ("node-c", "127.0.0.1:3", NodeState::Joining),
        ] {
            lock(&registry).insert(
                name.to_string(),
                NodeInfo::new(address.to_string(), state, format!("tk-{name}")),
            );
        }
        let current_join: CurrentJoin = Arc::new(Mutex::new(Some(PendingJoin {
            joining_name: "node-c".to_string(),
            expected: [
                ("node-a".to_string(), "tk-node-a".to_string()),
                ("node-b".to_string(), "tk-node-b".to_string()),
            ]
            .into_iter()
            .collect(),
            completed: HashSet::new(),
            started_at: Instant::now(),
            max_entries: 0,
            roster: Arc::new(Vec::new()),
            late: LateHandoffs::default(),
            generation: 1,
        })));

        let (_shutdown_tx, shutdown_rx) = watch::channel(false);
        let (mut node_a, server) = tcp_pair().await;
        let connection = tokio::spawn(handle_connection(
            MaybeTls::Plain(server),
            std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST),
            Arc::clone(&registry),
            Arc::clone(&current_join),
            ConnectionConfig {
                idle_timeout: IDLE_TIMEOUT,
                list_ready_at: Instant::now(),
                replication: 2,
                auth_secret: None,
                tls_acceptor: None,
                tls_connector: None,
                announce_limiter: Arc::new(Mutex::new(FxHashMap::default())),
            },
            shutdown_rx,
            Arc::new(std::sync::Mutex::new(None)),
        ));

        let sent_at = Instant::now();
        node_a.write_all(b"V 6 9\nnode-atk-node-a").await.unwrap();
        let mut ack = [0u8; 2];
        tokio::time::timeout(Duration::from_secs(3), node_a.read_exact(&mut ack))
            .await
            .expect("R must not wait for the abandon's X to node-b (hangs 30 s)")
            .unwrap();
        assert_eq!(&ack, b"R\n");
        assert!(
            sent_at.elapsed() < OUTBOUND_IO_TIMEOUT / 2,
            "R arrived after {:?}",
            sent_at.elapsed()
        );

        // The abandon still happens: node-b is sent the `X`, the join and
        // the joining node's entry are gone.
        wait_until(
            "the abandon's X to reach node-b",
            Duration::from_secs(3),
            || accepted.load(Ordering::SeqCst) == 1,
        )
        .await;
        assert!(lock_current_join(&current_join).is_none());
        assert!(!lock(&registry).contains_key("node-a"));
        assert!(
            !lock(&registry).contains_key("node-c"),
            "the abandon must also strand the joining node's own entry"
        );

        // The stuck `X` would hold the connection task for 10 s.
        connection.abort();
        let _ = connection.await;
    }

    #[tokio::test(flavor = "current_thread")]
    async fn an_explicit_v_from_the_joining_node_itself_abandons_its_own_join() {
        // Regression for issue #323: before this fix, `NodeLeave`
        // (`V`) only abandoned `current_join` when the *removed* node
        // was an already-`Joined` ready member (issue #297's fix,
        // sibling test above). A Joining node sending `V` for itself —
        // e.g. to abort its own pending join — removed its registry
        // entry but left `current_join` pointing at that now-gone name,
        // stalling every later join behind it until
        // `migration_timeout_for` (up to 2h) eventually reaped the
        // ghost. `on_node_connection_ended` already treats this the
        // same regardless of state (`pending.joining_name == name`);
        // this handler must match that.
        //
        // A second node, node-c, is left `Waiting` so this also proves
        // `try_begin_next_join` picks it up once the abandoned join
        // clears `current_join` — its state flips to `Joining`
        // synchronously, before `abandon_current_join`'s awaited `M`
        // fanout to node-a even resolves.
        let registry: Registry = Arc::new(RegistryState::default());
        lock(&registry).insert(
            "node-a".to_string(),
            NodeInfo::new(
                "127.0.0.1:1".to_string(),
                NodeState::Joined,
                "tk-node-a".to_string(),
            ),
        );
        lock(&registry).insert(
            "node-b".to_string(),
            NodeInfo::new(
                "127.0.0.1:2".to_string(),
                NodeState::Joining,
                "tk-node-b".to_string(),
            ),
        );
        lock(&registry).insert(
            "node-c".to_string(),
            NodeInfo::new(
                "127.0.0.1:3".to_string(),
                NodeState::Waiting,
                "tk-node-c".to_string(),
            ),
        );

        let current_join: CurrentJoin = Arc::new(Mutex::new(Some(PendingJoin {
            joining_name: "node-b".to_string(),
            expected: [("node-a".to_string(), "tk-node-a".to_string())]
                .into_iter()
                .collect(),
            completed: HashSet::new(),
            started_at: Instant::now(),
            max_entries: 0,
            roster: Arc::new(Vec::new()),
            late: LateHandoffs::default(),
            generation: 1,
        })));

        let (_shutdown_tx, shutdown_rx) = watch::channel(false);
        let (mut node_b, server) = tcp_pair().await;
        tokio::spawn(handle_connection(
            MaybeTls::Plain(server),
            std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST),
            Arc::clone(&registry),
            Arc::clone(&current_join),
            ConnectionConfig {
                idle_timeout: IDLE_TIMEOUT,
                list_ready_at: Instant::now(),
                replication: 2,
                auth_secret: None,
                tls_acceptor: None,
                tls_connector: None,
                announce_limiter: Arc::new(Mutex::new(FxHashMap::default())),
            },
            shutdown_rx,
            Arc::new(std::sync::Mutex::new(None)),
        ));

        node_b.write_all(b"V 6 9\nnode-btk-node-b").await.unwrap();
        let mut ack = [0u8; 2];
        node_b.read_exact(&mut ack).await.unwrap();
        assert_eq!(&ack, b"R\n");

        // `R` no longer waits for the abandon (issue #1662): wait for it.
        wait_until("node-c's join to start", Duration::from_secs(3), || {
            lock_current_join(&current_join)
                .as_ref()
                .is_some_and(|pending| pending.joining_name == "node-c")
        })
        .await;
        assert!(!lock(&registry).contains_key("node-b"));
        assert_eq!(
            lock_current_join(&current_join)
                .as_ref()
                .map(|pending| pending.joining_name.clone()),
            Some("node-c".to_string()),
            "abandoning node-b's own join must let try_begin_next_join start node-c's"
        );
        assert_eq!(
            lock(&registry).get("node-c").map(|info| info.state),
            Some(NodeState::Joining),
            "node-c must have been picked up as the next join once node-b's was abandoned"
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn an_explicit_v_from_a_waiting_node_matching_the_current_join_abandons_it() {
        // Regression for issue #323, the `Waiting` half of the fix above:
        // the matching logic must key off `current_join.joining_name`,
        // not the removed node's own state — `on_node_connection_ended`
        // already does this unconditionally. This registry doesn't
        // naturally reach a `Waiting` node being `current_join`'s
        // `joining_name` (that transition flips the state to `Joining`
        // atomically under the same lock, see `try_begin_next_join`),
        // but the handler must not silently rely on that invariant
        // holding forever — it checks name, not state, exactly like
        // `on_node_connection_ended` does.
        let registry: Registry = Arc::new(RegistryState::default());
        lock(&registry).insert(
            "node-b".to_string(),
            NodeInfo::new(
                "127.0.0.1:2".to_string(),
                NodeState::Waiting,
                "tk-node-b".to_string(),
            ),
        );

        let current_join: CurrentJoin = Arc::new(Mutex::new(Some(PendingJoin {
            joining_name: "node-b".to_string(),
            expected: HashMap::new(),
            completed: HashSet::new(),
            started_at: Instant::now(),
            max_entries: 0,
            roster: Arc::new(Vec::new()),
            late: LateHandoffs::default(),
            generation: 1,
        })));

        let (_shutdown_tx, shutdown_rx) = watch::channel(false);
        let (mut node_b, server) = tcp_pair().await;
        tokio::spawn(handle_connection(
            MaybeTls::Plain(server),
            std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST),
            Arc::clone(&registry),
            Arc::clone(&current_join),
            ConnectionConfig {
                idle_timeout: IDLE_TIMEOUT,
                list_ready_at: Instant::now(),
                replication: 2,
                auth_secret: None,
                tls_acceptor: None,
                tls_connector: None,
                announce_limiter: Arc::new(Mutex::new(FxHashMap::default())),
            },
            shutdown_rx,
            Arc::new(std::sync::Mutex::new(None)),
        ));

        node_b.write_all(b"V 6 9\nnode-btk-node-b").await.unwrap();
        let mut ack = [0u8; 2];
        node_b.read_exact(&mut ack).await.unwrap();
        assert_eq!(&ack, b"R\n");

        // `R` no longer waits for the abandon (issue #1662): wait for it.
        wait_until("the abandon", Duration::from_secs(3), || {
            lock_current_join(&current_join).is_none()
        })
        .await;
        assert!(
            lock_current_join(&current_join).is_none(),
            "the join must be abandoned once its joining node (still Waiting) explicitly leaves"
        );
        assert!(!lock(&registry).contains_key("node-b"));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn a_join_with_the_wrong_token_for_a_registered_name_is_rejected() {
        // Issue #34: only the same node retrying (same token) may share a
        // Waiting entry's promotion `Notify` — see
        // `JoinRejection::TokenMismatch`.
        let (_shutdown_tx, shutdown_rx) = watch::channel(false);
        let (_node_a, _node_b, registry, current_join) =
            registry_with_a_joined_and_b_waiting(shutdown_rx.clone()).await;

        let (mut attacker, server) = tcp_pair().await;
        let connection_task = tokio::spawn(handle_connection(
            MaybeTls::Plain(server),
            std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST),
            Arc::clone(&registry),
            Arc::clone(&current_join),
            ConnectionConfig {
                idle_timeout: IDLE_TIMEOUT,
                list_ready_at: Instant::now(),
                replication: 2,
                auth_secret: None,
                tls_acceptor: None,
                tls_connector: None,
                announce_limiter: Arc::new(Mutex::new(FxHashMap::default())),
            },
            shutdown_rx,
            Arc::new(std::sync::Mutex::new(None)),
        ));

        attacker
            .write_all(b"J 6 9002 8\nnode-bevil-tok")
            .await
            .unwrap();
        assert!(connection_task.await.unwrap().is_err());
        assert!(
            lock(&registry).contains_key("node-b"),
            "the real node's entry must survive the rejected join"
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn send_cancel_times_out_against_a_silent_node() {
        // Regression for issue #6: an accepted-but-silent node must not
        // block the caller (the sweep task) indefinitely.
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap().to_string();
        let silent = tokio::spawn(async move {
            let (_stream, _) = listener.accept().await.unwrap();
            // Hold the connection open without ever answering.
            tokio::time::sleep(Duration::from_secs(60)).await;
        });

        let started = Instant::now();
        let result = send_cancel(
            "ready-token",
            &address,
            &None,
            &None,
            "node-b",
            Duration::from_millis(100),
        )
        .await;
        silent.abort();

        let error = result.expect_err("expected the silent node to time the ack read out");
        assert_eq!(error.kind(), io::ErrorKind::TimedOut);
        assert!(started.elapsed() < Duration::from_secs(5));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn abandon_current_join_fans_out_to_more_ready_members_than_the_concurrency_cap() {
        // Regression for issue #299: MAX_FANOUT_CONCURRENCY bounds how many
        // of `abandon_current_join`'s `X` (cancel) sends may be dialing
        // out at once, but every ready member must still be contacted and
        // the join must still finish being abandoned — not stall, and not
        // silently drop members once there are more of them than the
        // concurrency cap allows in flight at once.
        let member_count = MAX_FANOUT_CONCURRENCY + 20;

        // A port nothing is listening on: every dial fails fast
        // (connection refused) instead of hanging, so this stays fast even
        // spread over more than one batch of MAX_FANOUT_CONCURRENCY.
        let probe = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let dead_addr = probe.local_addr().unwrap().to_string();
        drop(probe);

        let registry: Registry = Arc::new(RegistryState::default());
        let mut expected = HashMap::new();
        {
            let mut guard = lock(&registry);
            for i in 0..member_count {
                let name = format!("ready-{i}");
                let token = format!("tk-{i}");
                guard.insert(
                    name.clone(),
                    NodeInfo::new(dead_addr.clone(), NodeState::Joined, token.clone()),
                );
                expected.insert(name, token);
            }
            guard.insert(
                "joining-node".to_string(),
                NodeInfo::new(
                    "127.0.0.1:1".to_string(),
                    NodeState::Joining,
                    "tk-joining".to_string(),
                ),
            );
        }

        let current_join: CurrentJoin = Arc::new(Mutex::new(Some(PendingJoin {
            joining_name: "joining-node".to_string(),
            expected,
            completed: HashSet::new(),
            started_at: Instant::now(),
            max_entries: 0,
            roster: Arc::new(Vec::new()),
            late: LateHandoffs::default(),
            generation: 1,
        })));

        let join = lock_current_join(&current_join)
            .as_ref()
            .unwrap()
            .identity();
        tokio::time::timeout(
            Duration::from_secs(30),
            abandon_current_join(
                &registry,
                &current_join,
                &None,
                &None,
                2,
                Instant::now(),
                &join,
                "test",
            ),
        )
        .await
        .expect(
            "abandon_current_join must still finish with more ready members than \
             MAX_FANOUT_CONCURRENCY",
        );

        assert!(
            lock_current_join(&current_join).is_none(),
            "the join must be fully abandoned regardless of fan-out batching"
        );
        assert!(
            !lock(&registry).contains_key("joining-node"),
            "abandon_current_join must still strand the joining node's own entry"
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn a_second_node_joining_waits_for_a_completion_report_before_being_promoted() {
        let registry: Registry = Arc::new(RegistryState::default());
        let current_join: CurrentJoin = Arc::new(Mutex::new(None));
        let (_shutdown_tx, shutdown_rx) = watch::channel(false);

        let config = || ConnectionConfig {
            idle_timeout: IDLE_TIMEOUT,
            list_ready_at: Instant::now(),
            replication: 2,
            auth_secret: None,
            tls_acceptor: None,
            tls_connector: None,
            announce_limiter: Arc::new(Mutex::new(FxHashMap::default())),
        };

        // Node A joins first. There are no Joined nodes yet, so it's the
        // bootstrap case: promoted immediately, with nothing to receive.
        let (mut node_a, server_a) = tcp_pair().await;
        tokio::spawn(handle_connection(
            MaybeTls::Plain(server_a),
            std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST),
            Arc::clone(&registry),
            Arc::clone(&current_join),
            config(),
            shutdown_rx.clone(),
            Arc::new(std::sync::Mutex::new(None)),
        ));
        node_a
            .write_all(b"J 6 9001 9\nnode-atk-node-a")
            .await
            .unwrap();
        let mut node_a_response = [0u8; 2];
        node_a.read_exact(&mut node_a_response).await.unwrap();
        assert_eq!(&node_a_response, b"R\n");

        // Node B joins next. A is now Joined, so B moves to Joining and
        // must wait for A's completion report — it must not be promoted
        // yet, and so must not appear in L.
        let (mut node_b, server_b) = tcp_pair().await;
        tokio::spawn(handle_connection(
            MaybeTls::Plain(server_b),
            std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST),
            Arc::clone(&registry),
            Arc::clone(&current_join),
            config(),
            shutdown_rx.clone(),
            Arc::new(std::sync::Mutex::new(None)),
        ));
        node_b
            .write_all(b"J 6 9002 9\nnode-btk-node-b")
            .await
            .unwrap();

        // The write completing only means the OS accepted the bytes, not
        // that the spawned connection task has read and processed them
        // yet; poll briefly instead of assuming one yield is enough.
        for _ in 0..1000 {
            if lock(&registry).contains_key("node-b") {
                break;
            }
            tokio::time::sleep(Duration::from_millis(1)).await;
        }

        assert_eq!(
            lock(&registry).get("node-b").unwrap().state,
            NodeState::Joining
        );

        let (mut lister, server_l) = tcp_pair().await;
        tokio::spawn(handle_connection(
            MaybeTls::Plain(server_l),
            std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST),
            Arc::clone(&registry),
            Arc::clone(&current_join),
            config(),
            shutdown_rx.clone(),
            Arc::new(std::sync::Mutex::new(None)),
        ));
        lister.write_all(b"L\n").await.unwrap();
        let expected_list = b"N 1 2\n6 14\nnode-a127.0.0.1:9001\n";
        let mut list_response = vec![0u8; expected_list.len()];
        lister.read_exact(&mut list_response).await.unwrap();
        assert_eq!(list_response, expected_list);

        // A reports it has finished handing its share off to B. B should
        // now be promoted and receive its own R\n on the connection it's
        // been holding open since it sent J.
        node_a
            .write_all(b"C 6 6 9\nnode-anode-btk-node-a")
            .await
            .unwrap();
        let mut ack = [0u8; 2];
        node_a.read_exact(&mut ack).await.unwrap();
        assert_eq!(&ack, b"A\n");

        let mut node_b_response = [0u8; 2];
        node_b.read_exact(&mut node_b_response).await.unwrap();
        assert_eq!(&node_b_response, b"R\n");
        assert_eq!(
            lock(&registry).get("node-b").unwrap().state,
            NodeState::Joined
        );
    }

    /// Issue #1029: the `A` for the last `C` goes out before the next
    /// waiting node's `M` fan-out, not after it. A ready node that never
    /// answers its `M` used to hold the ack for the fan-out's attempts
    /// (three of 10 s), past the reporter's own 10 s wait for it.
    #[tokio::test(flavor = "current_thread")]
    async fn the_completion_ack_is_written_before_the_next_joins_fan_out() {
        // Node A is ready, at the address of a listener that accepts the
        // next join's `M` and never answers it.
        let blackhole = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let blackhole_addr = blackhole.local_addr().unwrap().to_string();
        tokio::spawn(async move {
            let mut held = Vec::new();
            loop {
                let (connection, _) = blackhole.accept().await.unwrap();
                held.push(connection);
            }
        });

        let registry: Registry = Arc::new(RegistryState::default());
        lock(&registry).insert(
            "node-a".to_string(),
            NodeInfo::new(blackhole_addr, NodeState::Joined, "tk-node-a".to_string()),
        );
        lock(&registry).insert(
            "node-b".to_string(),
            NodeInfo::new(
                "127.0.0.1:2".to_string(),
                NodeState::Joining,
                "tk-node-b".to_string(),
            ),
        );
        lock(&registry).insert(
            "node-c".to_string(),
            NodeInfo::new(
                "127.0.0.1:3".to_string(),
                NodeState::Waiting,
                "tk-node-c".to_string(),
            ),
        );
        let current_join: CurrentJoin = Arc::new(Mutex::new(Some(PendingJoin {
            joining_name: "node-b".to_string(),
            expected: [("node-a".to_string(), "tk-node-a".to_string())]
                .into_iter()
                .collect(),
            completed: HashSet::new(),
            started_at: Instant::now(),
            max_entries: 0,
            roster: Arc::new(Vec::new()),
            late: LateHandoffs::default(),
            generation: 1,
        })));

        let (_shutdown_tx, shutdown_rx) = watch::channel(false);
        let (mut node_a, server_a) = tcp_pair().await;
        tokio::spawn(handle_connection(
            MaybeTls::Plain(server_a),
            std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST),
            Arc::clone(&registry),
            Arc::clone(&current_join),
            ConnectionConfig {
                idle_timeout: IDLE_TIMEOUT,
                list_ready_at: Instant::now(),
                replication: 2,
                auth_secret: None,
                tls_acceptor: None,
                tls_connector: None,
                announce_limiter: Arc::new(Mutex::new(FxHashMap::default())),
            },
            shutdown_rx,
            Arc::new(std::sync::Mutex::new(None)),
        ));

        // A's report completes B's join; C is next and its `M` to A hangs
        // for the fan-out's three 10 s attempts. Real time, not the paused
        // clock: the ack travels through a real socket, and the paused
        // clock jumps to the fan-out's timers while it is in flight.
        node_a
            .write_all(b"C 6 6 9\nnode-anode-btk-node-a")
            .await
            .unwrap();
        let mut ack = [0u8; 2];
        timeout(Duration::from_secs(3), node_a.read_exact(&mut ack))
            .await
            .expect("the ack must not wait for the next join's M fan-out")
            .unwrap();
        assert_eq!(&ack, b"A\n");
        assert_eq!(
            lock(&registry).get("node-b").unwrap().state,
            NodeState::Joined
        );

        // The next join still starts; only the ack no longer waits for it.
        for _ in 0..1000 {
            if lock(&registry).get("node-c").map(|info| info.state) == Some(NodeState::Joining) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
        assert_eq!(
            lock(&registry).get("node-c").map(|info| info.state),
            Some(NodeState::Joining)
        );
    }

    /// Issue #1031: a node admitted by `P` while a join is pending was not
    /// in the join's roster and received no `M`, so its copies of the
    /// joiner's keys were never handed off. It is now expected to report
    /// like the others and sent the join's own `M`.
    #[tokio::test(flavor = "current_thread")]
    async fn a_node_announced_during_a_pending_join_is_sent_m_and_expected_to_report() {
        // The late node: a listener that records the `M` it receives and
        // acks it.
        let late_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let late_port = late_listener.local_addr().unwrap().port();
        let received: Arc<std::sync::Mutex<Vec<u8>>> = Arc::new(std::sync::Mutex::new(Vec::new()));
        let received_task = Arc::clone(&received);
        tokio::spawn(async move {
            let (mut connection, _) = late_listener.accept().await.unwrap();
            let mut buffer = [0u8; 1024];
            let bytes_read = connection.read(&mut buffer).await.unwrap();
            received_task
                .lock()
                .unwrap()
                .extend_from_slice(&buffer[..bytes_read]);
            connection.write_all(b"A 3\n").await.unwrap();
        });

        let registry: Registry = Arc::new(RegistryState::default());
        lock(&registry).insert(
            "node-a".to_string(),
            NodeInfo::new(
                "127.0.0.1:1".to_string(),
                NodeState::Joined,
                "tk-node-a".to_string(),
            ),
        );
        lock(&registry).insert(
            "node-b".to_string(),
            NodeInfo::new(
                "127.0.0.1:2".to_string(),
                NodeState::Joining,
                "tk-node-b".to_string(),
            ),
        );
        let current_join: CurrentJoin = Arc::new(Mutex::new(Some(PendingJoin {
            joining_name: "node-b".to_string(),
            expected: [("node-a".to_string(), "tk-node-a".to_string())]
                .into_iter()
                .collect(),
            completed: HashSet::new(),
            started_at: Instant::now(),
            max_entries: 0,
            generation: 4,
            roster: Arc::new(vec![("node-a".to_string(), "127.0.0.1:1".to_string())]),
            late: LateHandoffs::default(),
        })));

        let (_shutdown_tx, shutdown_rx) = watch::channel(false);
        let config = || ConnectionConfig {
            idle_timeout: IDLE_TIMEOUT,
            list_ready_at: Instant::now(),
            replication: 2,
            auth_secret: None,
            tls_acceptor: None,
            tls_connector: None,
            announce_limiter: Arc::new(Mutex::new(FxHashMap::default())),
        };

        // node-c, unknown to the registry, announces while B's join waits
        // for A's report.
        let (mut node_c, server_c) = tcp_pair().await;
        tokio::spawn(handle_connection(
            MaybeTls::Plain(server_c),
            std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST),
            Arc::clone(&registry),
            Arc::clone(&current_join),
            config(),
            shutdown_rx.clone(),
            Arc::new(std::sync::Mutex::new(None)),
        ));
        node_c
            .write_all(format!("P 6 {late_port} 9\nnode-ctk-node-c").as_bytes())
            .await
            .unwrap();
        let mut reply = [0u8; 2];
        node_c.read_exact(&mut reply).await.unwrap();
        assert_eq!(&reply, b"R\n", "the announce is accepted as before");

        for _ in 0..1000 {
            if !received.lock().unwrap().is_empty() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
        let frame = received.lock().unwrap().clone();
        let text = String::from_utf8_lossy(&frame);
        assert!(text.starts_with("M "), "node-c received {text:?}");
        assert!(text.contains("node-b"), "the M names the joiner: {text:?}");
        assert!(
            text.contains("tk-node-c"),
            "the M carries node-c's own token: {text:?}"
        );
        {
            let guard = lock_current_join(&current_join);
            let pending = guard.as_ref().expect("the join is still pending");
            assert_eq!(
                pending.expected.get("node-c").map(String::as_str),
                Some("tk-node-c"),
                "node-c is expected to report"
            );
            assert_eq!(pending.max_entries, 3, "its M ack sized the timeout");
        }

        // node-c's report alone does not complete the join; A's does.
        node_c
            .write_all(b"C 6 6 9\nnode-cnode-btk-node-c")
            .await
            .unwrap();
        node_c.read_exact(&mut reply).await.unwrap();
        assert_eq!(&reply, b"A\n");
        assert_eq!(
            lock(&registry).get("node-b").unwrap().state,
            NodeState::Joining
        );

        let (mut node_a, server_a) = tcp_pair().await;
        tokio::spawn(handle_connection(
            MaybeTls::Plain(server_a),
            std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST),
            Arc::clone(&registry),
            Arc::clone(&current_join),
            config(),
            shutdown_rx.clone(),
            Arc::new(std::sync::Mutex::new(None)),
        ));
        node_a
            .write_all(b"C 6 6 9\nnode-anode-btk-node-a")
            .await
            .unwrap();
        node_a.read_exact(&mut reply).await.unwrap();
        assert_eq!(&reply, b"A\n");
        assert_eq!(
            lock(&registry).get("node-b").unwrap().state,
            NodeState::Joined
        );
        assert!(lock_current_join(&current_join).is_none());
    }

    /// A registry with a ready `node-a` and a joining `node-b`, and the
    /// join of `node-b` pending on `node-a`'s report.
    fn registry_with_a_pending_join() -> (Registry, CurrentJoin) {
        let registry: Registry = Arc::new(RegistryState::default());
        lock(&registry).insert(
            "node-a".to_string(),
            NodeInfo::new(
                "127.0.0.1:1".to_string(),
                NodeState::Joined,
                "tk-node-a".to_string(),
            ),
        );
        lock(&registry).insert(
            "node-b".to_string(),
            NodeInfo::new(
                "127.0.0.1:2".to_string(),
                NodeState::Joining,
                "tk-node-b".to_string(),
            ),
        );
        let current_join: CurrentJoin = Arc::new(Mutex::new(Some(PendingJoin {
            joining_name: "node-b".to_string(),
            expected: [("node-a".to_string(), "tk-node-a".to_string())]
                .into_iter()
                .collect(),
            completed: HashSet::new(),
            started_at: Instant::now(),
            max_entries: 0,
            generation: 4,
            roster: Arc::new(vec![("node-a".to_string(), "127.0.0.1:1".to_string())]),
            late: LateHandoffs::default(),
        })));
        (registry, current_join)
    }

    fn hand_off_to(registry: &Registry, current_join: &CurrentJoin, name: &str, addr: &str) {
        hand_off_from_late_member(
            registry,
            current_join,
            &None,
            &None,
            2,
            name,
            addr,
            &format!("tk-{name}"),
        );
    }

    /// Issue #1100: the `M` send to a node admitted mid-join dials only
    /// once the join's dial limit (`MAX_FANOUT_CONCURRENCY`, shared with
    /// the join's own fan-out) has a free permit, like every other send.
    #[tokio::test(flavor = "current_thread")]
    async fn a_late_members_m_send_waits_for_the_joins_dial_limit() {
        let late_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let late_addr = late_listener.local_addr().unwrap().to_string();
        let (registry, current_join) = registry_with_a_pending_join();
        let limit = Arc::clone(
            &lock_current_join(&current_join)
                .as_ref()
                .unwrap()
                .late
                .fanout_limit,
        );

        // Every permit of the join's dial limit is in use.
        let held = limit
            .try_acquire_many(MAX_FANOUT_CONCURRENCY as u32)
            .unwrap();
        hand_off_to(&registry, &current_join, "node-c", &late_addr);
        assert!(
            tokio::time::timeout(Duration::from_millis(300), late_listener.accept())
                .await
                .is_err(),
            "the late member must not be dialed while the dial limit is exhausted"
        );

        // Once a permit frees up, the send goes ahead.
        drop(held);
        tokio::time::timeout(Duration::from_secs(5), late_listener.accept())
            .await
            .expect("the late member is dialed once a permit is free")
            .unwrap();
    }

    /// Issue #1100: the late members' sends belong to the join. When it is
    /// abandoned they are aborted, not left to run out their attempts.
    #[tokio::test(flavor = "current_thread")]
    async fn abandoning_a_join_aborts_its_late_members_m_sends() {
        let (registry, current_join) = registry_with_a_pending_join();
        let limit = Arc::clone(
            &lock_current_join(&current_join)
                .as_ref()
                .unwrap()
                .late
                .fanout_limit,
        );
        // Borrowed permits: they hold no clone of the semaphore's `Arc`,
        // so the count below is the tasks' alone.
        let held = limit
            .try_acquire_many(MAX_FANOUT_CONCURRENCY as u32)
            .unwrap();
        for i in 0..3 {
            hand_off_to(
                &registry,
                &current_join,
                &format!("late-{i}"),
                "127.0.0.1:1",
            );
        }
        tokio::task::yield_now().await;
        assert_eq!(
            Arc::strong_count(&limit),
            1 + 1 + 3,
            "the join, this test and the three queued sends hold the limit"
        );

        let join = lock_current_join(&current_join)
            .as_ref()
            .unwrap()
            .identity();
        abandon_current_join(
            &registry,
            &current_join,
            &None,
            &None,
            2,
            Instant::now(),
            &join,
            "test",
        )
        .await;
        tokio::task::yield_now().await;

        assert!(lock_current_join(&current_join).is_none());
        assert_eq!(
            Arc::strong_count(&limit),
            1,
            "no late send is left running after the abandon"
        );
        drop(held);
    }

    /// Issue #1100: a join hands off to at most `MAX_LATE_MEMBERS_PER_JOIN`
    /// late members; the `P` handler refuses further new names until it
    /// ends, rather than admitting nodes that would get no `M`.
    #[tokio::test(flavor = "current_thread")]
    async fn a_join_takes_at_most_a_bounded_number_of_late_members() {
        let (registry, current_join) = registry_with_a_pending_join();
        let limit = Arc::clone(
            &lock_current_join(&current_join)
                .as_ref()
                .unwrap()
                .late
                .fanout_limit,
        );
        // Keep every send queued behind the dial limit.
        let held = limit
            .try_acquire_many(MAX_FANOUT_CONCURRENCY as u32)
            .unwrap();

        assert!(!late_handoffs_exhausted(&current_join));
        for i in 0..MAX_LATE_MEMBERS_PER_JOIN + 5 {
            hand_off_to(
                &registry,
                &current_join,
                &format!("late-{i}"),
                "127.0.0.1:1",
            );
        }
        assert!(late_handoffs_exhausted(&current_join));
        {
            let guard = lock_current_join(&current_join);
            let pending = guard.as_ref().unwrap();
            assert_eq!(pending.late.sends.len(), MAX_LATE_MEMBERS_PER_JOIN);
            assert_eq!(
                pending.expected.len(),
                1 + MAX_LATE_MEMBERS_PER_JOIN,
                "nodes past the bound are not added to the report set"
            );
        }

        // A `P` for one more new name is refused and registers nothing.
        let (_shutdown_tx, shutdown_rx) = watch::channel(false);
        let (mut node_x, server_x) = tcp_pair().await;
        tokio::spawn(handle_connection(
            MaybeTls::Plain(server_x),
            std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST),
            Arc::clone(&registry),
            Arc::clone(&current_join),
            ConnectionConfig {
                idle_timeout: IDLE_TIMEOUT,
                list_ready_at: Instant::now(),
                replication: 2,
                auth_secret: None,
                tls_acceptor: None,
                tls_connector: None,
                announce_limiter: Arc::new(Mutex::new(FxHashMap::default())),
            },
            shutdown_rx,
            Arc::new(std::sync::Mutex::new(None)),
        ));
        node_x
            .write_all(b"P 6 4000 9\nnode-xtk-node-x")
            .await
            .unwrap();
        let mut reply = [0u8; 16];
        let read = tokio::time::timeout(Duration::from_secs(5), node_x.read(&mut reply))
            .await
            .expect("the refused announce is answered by closing the connection");
        assert!(
            !matches!(read, Ok(n) if reply[..n].starts_with(b"R")),
            "the announce must not be accepted"
        );
        assert!(!lock(&registry).contains_key("node-x"));
        drop(held);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn a_ready_node_receives_m_when_a_second_node_joins() {
        let registry: Registry = Arc::new(RegistryState::default());
        let current_join: CurrentJoin = Arc::new(Mutex::new(None));
        let (_shutdown_tx, shutdown_rx) = watch::channel(false);

        let config = || ConnectionConfig {
            idle_timeout: IDLE_TIMEOUT,
            list_ready_at: Instant::now(),
            replication: 2,
            auth_secret: None,
            tls_acceptor: None,
            tls_connector: None,
            announce_limiter: Arc::new(Mutex::new(FxHashMap::default())),
        };

        // A fake ready node: a real listener that expects M and acks it,
        // standing in for node A's registered address.
        let ready_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let ready_port = ready_listener.local_addr().unwrap().port();
        let ready_addr = ready_listener.local_addr().unwrap().to_string();
        let received: Arc<std::sync::Mutex<Vec<u8>>> = Arc::new(std::sync::Mutex::new(Vec::new()));
        let received_task = Arc::clone(&received);
        let ready_task = tokio::spawn(async move {
            let (mut connection, _) = ready_listener.accept().await.unwrap();
            let mut buffer = [0u8; 256];
            let bytes_read = connection.read(&mut buffer).await.unwrap();
            received_task
                .lock()
                .unwrap()
                .extend_from_slice(&buffer[..bytes_read]);
            connection.write_all(b"A 0\n").await.unwrap();
        });

        // Node A joins first (bootstrap: no ready nodes yet), registered
        // at the fake ready node's address.
        let (mut node_a, server_a) = tcp_pair().await;
        tokio::spawn(handle_connection(
            MaybeTls::Plain(server_a),
            std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST),
            Arc::clone(&registry),
            Arc::clone(&current_join),
            config(),
            shutdown_rx.clone(),
            Arc::new(std::sync::Mutex::new(None)),
        ));
        node_a
            .write_all(format!("J 6 {ready_port} 9\nnode-atk-node-a").as_bytes())
            .await
            .unwrap();
        let mut node_a_response = [0u8; 2];
        node_a.read_exact(&mut node_a_response).await.unwrap();
        assert_eq!(&node_a_response, b"R\n");

        // Node B joins next — A is Joined and ready, so discovery should
        // send it M.
        let (mut node_b, server_b) = tcp_pair().await;
        tokio::spawn(handle_connection(
            MaybeTls::Plain(server_b),
            std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST),
            Arc::clone(&registry),
            Arc::clone(&current_join),
            config(),
            shutdown_rx.clone(),
            Arc::new(std::sync::Mutex::new(None)),
        ));
        node_b
            .write_all(b"J 6 9002 9\nnode-btk-node-b")
            .await
            .unwrap();

        for _ in 0..1000 {
            if !received.lock().unwrap().is_empty() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(1)).await;
        }

        ready_task.await.unwrap();

        let message = String::from_utf8(received.lock().unwrap().clone()).unwrap();
        let header_end = message.find('\n').unwrap();
        let mut header = message[..header_end].split(' ');
        assert_eq!(header.next(), Some("M"));

        // Header order per `build_migrate_message`/`src/command.rs`'s
        // `MigrateHeader`: joining_name_len, joining_addr_len,
        // joining_token_len, joined_count, replication, token_len,
        // [generation]. Issue #441: this test used to check only the
        // pre-#295 five-field header, which never caught that
        // `build_migrate_message` had silently drifted out of sync with
        // the real wire format `src/command.rs` parses (the M this
        // process actually sent to a real node was rejected outright).
        let joining_name_length: usize = header.next().unwrap().parse().unwrap();
        let joining_addr_length: usize = header.next().unwrap().parse().unwrap();
        let joining_token_length: usize = header.next().unwrap().parse().unwrap();
        let joined_count: usize = header.next().unwrap().parse().unwrap();
        let replication: usize = header.next().unwrap().parse().unwrap();
        let token_length: usize = header.next().unwrap().parse().unwrap();
        // Tenth-pass audit (2026-09-02), issue #421(c): the trailing
        // join generation, always sent by this (upgraded) discovery
        // process — see `build_migrate_message`'s doc comment.
        let generation: u64 = header.next().unwrap().parse().unwrap();
        assert_eq!(header.next(), None);
        assert_eq!(joined_count, 1);
        assert_eq!(replication, 2);
        // `NEXT_JOIN_GENERATION` is one process-wide counter shared by
        // every test in this binary (they run concurrently), so this
        // join's generation is merely SOME value `next_join_generation`
        // handed out — never 0 (`NEXT_JOIN_GENERATION` is seeded odd
        // and only increments).
        assert!(generation > 0);

        // Body: `<token><joining_name><joining_addr><joining_token><roster>`
        // — `token` is node-a's own membership token, echoed so it can
        // authenticate the M; `joining_token` (issue #295) is node-b's
        // own token, granting node-a a scoped trust relationship with it
        // for the handoff.
        let body = &message[header_end + 1..];
        assert_eq!(&body[..token_length], "tk-node-a");
        let after_token = &body[token_length..];
        assert_eq!(&after_token[..joining_name_length], "node-b");
        assert_eq!(
            &after_token[joining_name_length..joining_name_length + joining_addr_length],
            "127.0.0.1:9002"
        );
        let after_addr = &after_token[joining_name_length + joining_addr_length..];
        assert_eq!(&after_addr[..joining_token_length], "tk-node-b");

        let roster = &after_addr[joining_token_length..];
        assert!(
            roster.contains("node-a"),
            "roster should list node-a: {roster:?}"
        );
        assert!(
            roster.contains(&ready_addr),
            "roster should list node-a's address: {roster:?}"
        );
    }

    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn handle_connection_is_closed_after_the_unidentified_connection_timeout() {
        // Issue (slowloris via MAX_CONNECTIONS exhaustion): a connection
        // that never completes a single valid command must not be able
        // to hold its MAX_CONNECTIONS slot open for anywhere near
        // `idle_timeout` — `idle_timeout` is set well above
        // UNIDENTIFIED_CONNECTION_TIMEOUT here so this test isolates the
        // new, tighter bound specifically, not the ordinary idle timeout
        // it already had.
        let (client, server) = tcp_pair().await;
        let registry: Registry = Arc::new(RegistryState::default());
        let current_join: CurrentJoin = Arc::new(Mutex::new(None));
        let (_shutdown_tx, shutdown_rx) = watch::channel(false);

        let connection_task = tokio::spawn(handle_connection(
            MaybeTls::Plain(server),
            std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST),
            registry,
            current_join,
            ConnectionConfig {
                idle_timeout: UNIDENTIFIED_CONNECTION_TIMEOUT * 3,
                list_ready_at: Instant::now(),
                replication: 2,
                auth_secret: None,
                tls_acceptor: None,
                tls_connector: None,
                announce_limiter: Arc::new(Mutex::new(FxHashMap::default())),
            },
            shutdown_rx,
            Arc::new(std::sync::Mutex::new(None)),
        ));

        // Never sends anything — never completes a command, so
        // `identified` never flips true.
        tokio::task::yield_now().await;
        tokio::time::advance(UNIDENTIFIED_CONNECTION_TIMEOUT + Duration::from_secs(1)).await;
        tokio::task::yield_now().await;

        let result = tokio::time::timeout(Duration::from_secs(5), connection_task)
            .await
            .expect(
                "the connection must be closed once the unidentified-connection timeout \
                 elapses, not held open for the full (much larger) idle_timeout",
            )
            .unwrap();

        assert!(
            result.is_err(),
            "a connection that never completes a command must be closed"
        );
        // Kept alive for the whole test so the socket isn't dropped/reset
        // before the server side observes the timeout on its own.
        drop(client);
    }

    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn handle_connection_times_out_on_trickled_bytes_after_identification() {
        // Slowloris regression: before this fix, once `identified` flipped
        // true, every read recomputed the idle deadline as
        // `now + config.idle_timeout` regardless of whether it completed a
        // command — so a client that sent one valid command (`L\n`, no
        // auth required here) and then trickled in a single byte just
        // under `idle_timeout` apart could hold a `MAX_CONNECTIONS` permit
        // open forever without ever completing another request. The
        // deadline must instead stay anchored to the last *completed*
        // parse, exactly like the pre-identification case already covered
        // by `handle_connection_is_closed_after_the_unidentified_connection_timeout`.
        let (mut client, server) = tcp_pair().await;
        let registry: Registry = Arc::new(RegistryState::default());
        let current_join: CurrentJoin = Arc::new(Mutex::new(None));
        let (_shutdown_tx, shutdown_rx) = watch::channel(false);

        let connection_task = tokio::spawn(handle_connection(
            MaybeTls::Plain(server),
            std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST),
            registry,
            current_join,
            ConnectionConfig {
                idle_timeout: IDLE_TIMEOUT,
                list_ready_at: Instant::now(),
                replication: 2,
                auth_secret: None,
                tls_acceptor: None,
                tls_connector: None,
                announce_limiter: Arc::new(Mutex::new(FxHashMap::default())),
            },
            shutdown_rx,
            Arc::new(std::sync::Mutex::new(None)),
        ));

        tokio::task::yield_now().await;

        // Complete one command (`L\n`) — `identified` flips true and
        // `deadline` is anchored to this moment (accept + IDLE_TIMEOUT,
        // effectively unchanged here since barely any virtual time has
        // elapsed since accept). Driven forward with explicit
        // `yield_now`s rather than by awaiting the response directly: on
        // a paused clock, a real loopback read on the client side doesn't
        // reliably re-poll the server's spawned task on its own, so
        // without this the response bytes would only ever arrive once
        // some later `tokio::time::advance` call happened to drive the
        // runtime forward.
        client.write_all(b"L\n").await.unwrap();
        for _ in 0..3 {
            tokio::task::yield_now().await;
        }
        let expected = b"N 0 2\n";
        let mut response = vec![0_u8; expected.len()];
        client.read_exact(&mut response).await.unwrap();
        assert_eq!(response, expected);

        // Trickle in a single byte of an otherwise-incomplete second `L`
        // command just under the deadline the completed command above
        // set. A read-resetting deadline would treat this as grounds for
        // another full IDLE_TIMEOUT.
        tokio::time::advance(IDLE_TIMEOUT - Duration::from_secs(1)).await;
        client.write_all(b"L").await.unwrap();
        tokio::task::yield_now().await;

        // Past the deadline the completed `L` set, but nowhere near what
        // a read-resetting deadline would have allowed (another ~59s).
        tokio::time::advance(Duration::from_secs(2)).await;

        // Wrapped in a bounded wait, not a bare `.await`: under Tokio's
        // paused-clock auto-advance, a bare `connection_task.await` would
        // fast-forward to *whatever* timer the task is actually waiting
        // on and still report `TimedOut` even if that timer were the
        // read-resetting deadline's much later ~119s mark, silently
        // failing to catch the regression this test exists for. Bounding
        // the wait to a few seconds past the deadline the completed `L`
        // set forces a real distinction: only the fixed, non-resetting
        // deadline resolves within it.
        let result = tokio::time::timeout(Duration::from_secs(5), connection_task)
            .await
            .expect(
                "the connection must close at roughly IDLE_TIMEOUT after the last completed \
                 command, not be held open by the trickled, never-completing second `L`",
            )
            .unwrap();

        assert_eq!(result.unwrap_err().kind(), io::ErrorKind::TimedOut);
    }

    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn handle_connection_bounds_a_stalled_response_write_by_write_timeout() {
        // Issue #4-equivalent: an unbounded `stream.write_all` let a
        // client that stops draining its receive buffer (without closing
        // the connection — e.g. a full receive window) hold this
        // connection's `MAX_CONNECTIONS` permit open forever. Every
        // response write now routes through `write_response`, which
        // bounds it by `WRITE_TIMEOUT` (mirrors `src/server.rs`'s own
        // `write_response`/`WRITE_TIMEOUT`).
        //
        // A `L` reply large enough to exceed both this process's own
        // kernel send buffer and the client's receive window is required
        // to actually exercise a blocked write here — a handful of bytes
        // would just sit in the kernel's send buffer and complete
        // instantly, proving nothing. So the registry is pre-populated
        // directly (bypassing `P`/`J`, which are rate- and size-limited
        // for reasons unrelated to this test) with far more `Joined`
        // entries than any real socket buffer could hold at once.
        let registry: Registry = Arc::new(RegistryState::default());
        {
            let mut guard = lock(&registry);
            // Sized for Linux, not just macOS: a Linux loopback socket's
            // send buffer plus the peer's autotuned receive buffer can
            // absorb several MiB before a write blocks, so a few MiB of
            // `N` roster (50k short names) completed instantly there and
            // the connection simply sat in its idle wait instead of
            // timing out the write. ~120k entries with `MAX_NAME_LENGTH`
            // names puts the reply near 20 MiB, past any default buffer.
            for i in 0..120_000 {
                let name = format!("node-{i:06}-{}", "x".repeat(MAX_NAME_LENGTH - 12));
                let addr = format!(
                    "10.{}.{}.{}:9000",
                    (i / 65536) % 256,
                    (i / 256) % 256,
                    i % 256
                );
                guard.insert(
                    name.clone(),
                    NodeInfo::new(addr, NodeState::Joined, format!("tk-{name}")),
                );
            }
        }
        let current_join: CurrentJoin = Arc::new(Mutex::new(None));
        let (mut client, server) = tcp_pair().await;
        let (_shutdown_tx, shutdown_rx) = watch::channel(false);

        let connection_task = tokio::spawn(handle_connection(
            MaybeTls::Plain(server),
            std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST),
            registry,
            current_join,
            ConnectionConfig {
                idle_timeout: IDLE_TIMEOUT,
                list_ready_at: Instant::now(),
                replication: 2,
                auth_secret: None,
                tls_acceptor: None,
                tls_connector: None,
                announce_limiter: Arc::new(Mutex::new(FxHashMap::default())),
            },
            shutdown_rx,
            Arc::new(std::sync::Mutex::new(None)),
        ));

        tokio::task::yield_now().await;

        // The client never reads anything from here on — driven forward
        // with explicit `yield_now`s (see the trickled-bytes test above
        // for why a bare `.await` on the client side doesn't reliably
        // re-poll the server's spawned task under a paused clock) so the
        // (huge) `N ...` response actually starts writing and blocks once
        // the real OS-level send buffer and the client's receive window
        // both fill.
        client.write_all(b"L\n").await.unwrap();
        for _ in 0..10 {
            tokio::task::yield_now().await;
        }

        // Past `WRITE_TIMEOUT`; the client still hasn't read a single
        // byte, so the write can only have resolved via the timeout, not
        // by draining.
        tokio::time::advance(WRITE_TIMEOUT + Duration::from_secs(1)).await;

        // Wrapped in a bounded wait, not a bare `.await` — see the
        // trickled-bytes test above for why an unbounded wait wouldn't
        // reliably catch a regression here either.
        let result = tokio::time::timeout(Duration::from_secs(5), connection_task)
            .await
            .expect(
                "the connection must close at roughly WRITE_TIMEOUT after the stalled write, \
                 not be held open indefinitely by a client that never reads",
            )
            .unwrap();

        assert_eq!(result.unwrap_err().kind(), io::ErrorKind::TimedOut);

        // Kept alive for the whole test so the socket isn't dropped/reset
        // before the server side observes the timeout on its own.
        drop(client);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn handle_connection_rejects_commands_sent_before_authenticating() {
        let (mut client, server) = tcp_pair().await;
        let registry: Registry = Arc::new(RegistryState::default());
        let current_join: CurrentJoin = Arc::new(Mutex::new(None));
        let (_shutdown_tx, shutdown_rx) = watch::channel(false);

        let connection_task = tokio::spawn(handle_connection(
            MaybeTls::Plain(server),
            std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST),
            registry,
            current_join,
            ConnectionConfig {
                idle_timeout: IDLE_TIMEOUT,
                list_ready_at: Instant::now(),
                replication: 2,
                auth_secret: Some(Bytes::from_static(b"correct-secret")),
                tls_acceptor: None,
                tls_connector: None,
                announce_limiter: Arc::new(Mutex::new(FxHashMap::default())),
            },
            shutdown_rx,
            Arc::new(std::sync::Mutex::new(None)),
        ));

        client.write_all(b"L\n").await.unwrap();

        let mut response = [0u8; 3];
        client.read_exact(&mut response).await.unwrap();
        assert_eq!(&response, b"Ed\n");

        let error = connection_task.await.unwrap().unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::PermissionDenied);
    }

    /// A request that does not parse is answered `Ed\n` and the connection
    /// closed, before and after authenticating, whatever the fault: an
    /// unknown command, an empty secret, a length that is not a number, a
    /// stray token. The protocol page lists these as `Ed`; they used to
    /// close without a byte.
    #[tokio::test(flavor = "current_thread")]
    async fn handle_connection_answers_ed_to_a_request_that_does_not_parse() {
        for frame in [
            &b"~ 1\nx"[..],
            b"A 0\n",
            b"A abc\nxyz",
            b"Q extra\n",
            b"L x\n",
        ] {
            for secret in [None, Some(&b"correct-secret"[..])] {
                let (mut client, server) = tcp_pair().await;
                let registry: Registry = Arc::new(RegistryState::default());
                let current_join: CurrentJoin = Arc::new(Mutex::new(None));
                let (_shutdown_tx, shutdown_rx) = watch::channel(false);
                let connection_task = tokio::spawn(handle_connection(
                    MaybeTls::Plain(server),
                    std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST),
                    registry,
                    current_join,
                    ConnectionConfig {
                        idle_timeout: IDLE_TIMEOUT,
                        list_ready_at: Instant::now(),
                        replication: 2,
                        auth_secret: secret.map(Bytes::copy_from_slice),
                        tls_acceptor: None,
                        tls_connector: None,
                        announce_limiter: Arc::new(Mutex::new(FxHashMap::default())),
                    },
                    shutdown_rx,
                    Arc::new(std::sync::Mutex::new(None)),
                ));
                if secret.is_some() {
                    client.write_all(b"A 14\ncorrect-secret").await.unwrap();
                    let mut ok = [0u8; 3];
                    client.read_exact(&mut ok).await.unwrap();
                    assert_eq!(&ok, b"Od\n");
                }
                client.write_all(frame).await.unwrap();
                let mut response = Vec::new();
                client.read_to_end(&mut response).await.unwrap();
                assert_eq!(
                    response,
                    b"Ed\n",
                    "frame {:?}, secret {:?}",
                    String::from_utf8_lossy(frame),
                    secret.is_some()
                );
                // The server half-closed and is reading until the client
                // closes (or a second passes): close first.
                drop(client);
                let error = connection_task.await.unwrap().unwrap_err();
                assert_eq!(error.kind(), io::ErrorKind::InvalidData);
            }
        }
    }

    /// A request rejected from its header while its body is still being
    /// written: the client must still be able to read the `Ed\n`. Closing
    /// a socket with unread bytes in its receive buffer sends a reset,
    /// which can discard the reply on the client's side, so the server
    /// reads what is still arriving (a bounded amount, for a bounded time)
    /// before it drops the socket, as the node and the proxy do.
    #[tokio::test(flavor = "current_thread")]
    async fn a_client_still_writing_a_rejected_request_reads_the_ed() {
        let (mut client, server) = tcp_pair().await;
        let registry: Registry = Arc::new(RegistryState::default());
        let current_join: CurrentJoin = Arc::new(Mutex::new(None));
        let (_shutdown_tx, shutdown_rx) = watch::channel(false);
        let connection_task = tokio::spawn(handle_connection(
            MaybeTls::Plain(server),
            std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST),
            registry,
            current_join,
            ConnectionConfig {
                idle_timeout: IDLE_TIMEOUT,
                list_ready_at: Instant::now(),
                replication: 2,
                auth_secret: None,
                tls_acceptor: None,
                tls_connector: None,
                announce_limiter: Arc::new(Mutex::new(FxHashMap::default())),
            },
            shutdown_rx,
            Arc::new(std::sync::Mutex::new(None)),
        ));

        // A name length over the limit is refused from the header alone;
        // the body keeps coming after that. A write error here (the server
        // gone) is what the old behaviour looked like; the reply is what
        // is checked.
        client.write_all(b"J 999 1 1\n").await.unwrap();
        for _ in 0..4 {
            if client.write_all(&[b'x'; 256]).await.is_err() {
                break;
            }
        }
        let mut response = Vec::new();
        client.read_to_end(&mut response).await.unwrap();
        assert_eq!(response, b"Ed\n");
        drop(client);
        let error = connection_task.await.unwrap().unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn handle_connection_rejects_an_incorrect_auth_secret() {
        let (mut client, server) = tcp_pair().await;
        let registry: Registry = Arc::new(RegistryState::default());
        let current_join: CurrentJoin = Arc::new(Mutex::new(None));
        let (_shutdown_tx, shutdown_rx) = watch::channel(false);

        let connection_task = tokio::spawn(handle_connection(
            MaybeTls::Plain(server),
            std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST),
            registry,
            current_join,
            ConnectionConfig {
                idle_timeout: IDLE_TIMEOUT,
                list_ready_at: Instant::now(),
                replication: 2,
                auth_secret: Some(Bytes::from_static(b"correct-secret")),
                tls_acceptor: None,
                tls_connector: None,
                announce_limiter: Arc::new(Mutex::new(FxHashMap::default())),
            },
            shutdown_rx,
            Arc::new(std::sync::Mutex::new(None)),
        ));

        client.write_all(b"A 11\nwrong-value").await.unwrap();

        let mut response = [0u8; 3];
        client.read_exact(&mut response).await.unwrap();
        assert_eq!(&response, b"Ed\n");

        let error = connection_task.await.unwrap().unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::PermissionDenied);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn handle_connection_accepts_commands_after_correct_auth() {
        let (mut client, server) = tcp_pair().await;
        let registry: Registry = Arc::new(RegistryState::default());
        let current_join: CurrentJoin = Arc::new(Mutex::new(None));
        let (_shutdown_tx, shutdown_rx) = watch::channel(false);

        let connection_task = tokio::spawn(handle_connection(
            MaybeTls::Plain(server),
            std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST),
            registry,
            current_join,
            ConnectionConfig {
                idle_timeout: IDLE_TIMEOUT,
                list_ready_at: Instant::now(),
                replication: 2,
                auth_secret: Some(Bytes::from_static(b"correct-secret")),
                tls_acceptor: None,
                tls_connector: None,
                announce_limiter: Arc::new(Mutex::new(FxHashMap::default())),
            },
            shutdown_rx,
            Arc::new(std::sync::Mutex::new(None)),
        ));

        client.write_all(b"A 14\ncorrect-secretL\n").await.unwrap();
        client.shutdown().await.unwrap();

        let expected = b"Od\nN 0 2\n";
        let mut received = Vec::new();
        let mut chunk = [0u8; 64];

        while received.len() < expected.len() {
            let bytes_read = client.read(&mut chunk).await.unwrap();
            assert!(bytes_read > 0, "connection closed before response arrived");
            received.extend_from_slice(&chunk[..bytes_read]);
        }
        assert_eq!(received, expected);

        connection_task.await.unwrap().unwrap();
    }

    #[tokio::test(flavor = "current_thread")]
    async fn handle_connection_treats_auth_as_a_no_op_when_no_secret_is_configured() {
        let (mut client, server) = tcp_pair().await;
        let registry: Registry = Arc::new(RegistryState::default());
        let current_join: CurrentJoin = Arc::new(Mutex::new(None));
        let (_shutdown_tx, shutdown_rx) = watch::channel(false);

        let connection_task = tokio::spawn(handle_connection(
            MaybeTls::Plain(server),
            std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST),
            registry,
            current_join,
            ConnectionConfig {
                idle_timeout: IDLE_TIMEOUT,
                list_ready_at: Instant::now(),
                replication: 2,
                auth_secret: None,
                tls_acceptor: None,
                tls_connector: None,
                announce_limiter: Arc::new(Mutex::new(FxHashMap::default())),
            },
            shutdown_rx,
            Arc::new(std::sync::Mutex::new(None)),
        ));

        client.write_all(b"A 8\nanything").await.unwrap();
        client.shutdown().await.unwrap();

        let expected = b"Od\n";
        let mut response = vec![0_u8; expected.len()];
        client.read_exact(&mut response).await.unwrap();
        assert_eq!(response, expected);

        connection_task.await.unwrap().unwrap();
    }

    #[tokio::test(flavor = "current_thread")]
    async fn handle_connection_echoes_the_tag_capability_in_the_auth_reply() {
        let (mut client, server) = tcp_pair().await;
        let registry: Registry = Arc::new(RegistryState::default());
        let current_join: CurrentJoin = Arc::new(Mutex::new(None));
        let (_shutdown_tx, shutdown_rx) = watch::channel(false);

        let connection_task = tokio::spawn(handle_connection(
            MaybeTls::Plain(server),
            std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST),
            registry,
            current_join,
            ConnectionConfig {
                idle_timeout: IDLE_TIMEOUT,
                list_ready_at: Instant::now(),
                replication: 2,
                auth_secret: None,
                tls_acceptor: None,
                tls_connector: None,
                announce_limiter: Arc::new(Mutex::new(FxHashMap::default())),
            },
            shutdown_rx,
            Arc::new(std::sync::Mutex::new(None)),
        ));

        // Echoed response tags: discovery never tags anything, but a client doesn't
        // know which kind of server it dialed until `A`'s reply — so the
        // capability must still be accepted and echoed here.
        client.write_all(b"A 8 T\nanything").await.unwrap();
        client.shutdown().await.unwrap();

        let expected = b"OdT\n";
        let mut response = vec![0_u8; expected.len()];
        client.read_exact(&mut response).await.unwrap();
        assert_eq!(response, expected);

        connection_task.await.unwrap().unwrap();
    }

    async fn tcp_pair() -> (TcpStream, TcpStream) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();

        let connect = TcpStream::connect(address);
        let accept = listener.accept();

        let (client, server) = tokio::join!(connect, accept);

        let client = client.unwrap();
        let (server, _) = server.unwrap();

        (client, server)
    }

    /// A joiner's liveness is its `J` connection, so every accepted socket
    /// must have TCP keepalive on, with the idle time and interval from
    /// `infra`; otherwise a joiner whose host dies without a FIN or RST
    /// stays registered. A peer vanishing without a FIN cannot be
    /// simulated in-process, so this checks the socket options instead.
    #[tokio::test(flavor = "current_thread")]
    async fn dispatch_connection_enables_tcp_keepalive_on_accepted_sockets() {
        let (_client, server) = tcp_pair().await;
        let address = server.peer_addr().unwrap();
        // Dispatch consumes the stream; keep a duplicate handle to the same
        // socket to read the options back.
        let observer = server.into_std().unwrap();
        let stream = TcpStream::from_std(observer.try_clone().unwrap()).unwrap();

        let connection_limit = Arc::new(Semaphore::new(1));
        let per_ip_connections: PerIpConnections = Arc::new(Mutex::new(HashMap::new()));
        let cluster_state = ClusterState {
            registry: Arc::new(RegistryState::default()),
            current_join: Arc::new(Mutex::new(None)),
        };
        let (_shutdown_tx, shutdown_rx) = watch::channel(false);
        let mut connection_tasks = JoinSet::new();

        dispatch_connection(
            stream,
            address,
            cluster_state,
            connection_limit,
            per_ip_connections,
            ConnectionConfig {
                idle_timeout: IDLE_TIMEOUT,
                list_ready_at: Instant::now(),
                replication: 2,
                auth_secret: None,
                tls_acceptor: None,
                tls_connector: None,
                announce_limiter: Arc::new(Mutex::new(FxHashMap::default())),
            },
            shutdown_rx,
            &mut connection_tasks,
        );

        let socket = socket2::SockRef::from(&observer);
        assert!(socket.keepalive().unwrap());
        assert_eq!(
            socket.tcp_keepalive_time().unwrap(),
            infra::TCP_KEEPALIVE_IDLE
        );
        assert_eq!(
            socket.tcp_keepalive_interval().unwrap(),
            infra::TCP_KEEPALIVE_INTERVAL
        );
        #[cfg(any(target_os = "linux", target_os = "macos"))]
        assert_eq!(
            socket.tcp_keepalive_retries().unwrap(),
            infra::TCP_KEEPALIVE_RETRIES
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn rejects_connection_when_connection_limit_is_reached() {
        let connection_limit = Arc::new(Semaphore::new(1));
        let per_ip_connections: PerIpConnections = Arc::new(Mutex::new(HashMap::new()));
        let cluster_state = ClusterState {
            registry: Arc::new(RegistryState::default()),
            current_join: Arc::new(Mutex::new(None)),
        };
        let (_shutdown_tx, shutdown_rx) = watch::channel(false);
        let mut connection_tasks = JoinSet::new();

        let (_first_client, first_server) = tcp_pair().await;
        let first_address = first_server.peer_addr().unwrap();

        dispatch_connection(
            first_server,
            first_address,
            cluster_state.clone(),
            Arc::clone(&connection_limit),
            Arc::clone(&per_ip_connections),
            ConnectionConfig {
                idle_timeout: IDLE_TIMEOUT,
                list_ready_at: Instant::now(),
                replication: 2,
                auth_secret: None,
                tls_acceptor: None,
                tls_connector: None,
                announce_limiter: Arc::new(Mutex::new(FxHashMap::default())),
            },
            shutdown_rx.clone(),
            &mut connection_tasks,
        );

        // CRITICAL fix: the connection-limit check now runs inside the
        // spawned task, not inline here — let it run far enough to take
        // the sole permit and settle into its read loop.
        tokio::task::yield_now().await;
        assert_eq!(connection_limit.available_permits(), 0);

        let (mut second_client, second_server) = tcp_pair().await;
        let second_address = second_server.peer_addr().unwrap();

        dispatch_connection(
            second_server,
            second_address,
            cluster_state,
            connection_limit,
            per_ip_connections,
            ConnectionConfig {
                idle_timeout: IDLE_TIMEOUT,
                list_ready_at: Instant::now(),
                replication: 2,
                auth_secret: None,
                tls_acceptor: None,
                tls_connector: None,
                announce_limiter: Arc::new(Mutex::new(FxHashMap::default())),
            },
            shutdown_rx,
            &mut connection_tasks,
        );

        // Reading to EOF drives the over-limit task to completion: it
        // replies "Busy" and closes without ever acquiring a permit.
        let mut response = Vec::new();
        second_client.read_to_end(&mut response).await.unwrap();
        assert_eq!(response, b"B\n");
    }

    #[tokio::test(flavor = "current_thread")]
    async fn rejects_connection_when_the_per_ip_connection_limit_is_reached() {
        // Regression: `MAX_CONNECTIONS` alone lets a single source IP
        // hold every one of the global permits by itself, starving every
        // other client and node, without the global semaphore ever
        // reporting anything unusual short of the very last permit.
        // `MAX_CONNECTIONS_PER_IP` must reject a source once it
        // individually reaches its own cap, independent of how much
        // global headroom remains. Mirrors `src/server.rs`'s own
        // `rejects_connection_when_the_per_ip_connection_limit_is_reached`.
        let connection_limit = Arc::new(Semaphore::new(10));
        let per_ip_connections: PerIpConnections = Arc::new(Mutex::new(HashMap::new()));
        let cluster_state = ClusterState {
            registry: Arc::new(RegistryState::default()),
            current_join: Arc::new(Mutex::new(None)),
        };

        let ip = std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST);
        // Stands in for `MAX_CONNECTIONS_PER_IP - 1` other already-live
        // connections from this IP, without actually dispatching that
        // many for the test.
        per_ip_connections
            .lock()
            .unwrap()
            .insert(ip, MAX_CONNECTIONS_PER_IP - 1);

        let (_first_client, first_server) = tcp_pair().await;
        let first_address = SocketAddr::new(ip, 9000);

        let mut connection_tasks = JoinSet::new();
        let (_shutdown_tx, shutdown_rx) = watch::channel(false);

        dispatch_connection(
            first_server,
            first_address,
            cluster_state.clone(),
            Arc::clone(&connection_limit),
            Arc::clone(&per_ip_connections),
            ConnectionConfig {
                idle_timeout: IDLE_TIMEOUT,
                list_ready_at: Instant::now(),
                replication: 2,
                auth_secret: None,
                tls_acceptor: None,
                tls_connector: None,
                announce_limiter: Arc::new(Mutex::new(FxHashMap::default())),
            },
            shutdown_rx.clone(),
            &mut connection_tasks,
        );

        // Let the task run far enough to reserve its per-IP slot and
        // settle into its read loop — this is the connection that fills
        // the cap exactly.
        tokio::task::yield_now().await;
        assert_eq!(
            per_ip_connections.lock().unwrap().get(&ip).copied(),
            Some(MAX_CONNECTIONS_PER_IP)
        );

        let (mut second_client, second_server) = tcp_pair().await;
        let second_address = SocketAddr::new(ip, 9001);

        dispatch_connection(
            second_server,
            second_address,
            cluster_state,
            connection_limit,
            Arc::clone(&per_ip_connections),
            ConnectionConfig {
                idle_timeout: IDLE_TIMEOUT,
                list_ready_at: Instant::now(),
                replication: 2,
                auth_secret: None,
                tls_acceptor: None,
                tls_connector: None,
                announce_limiter: Arc::new(Mutex::new(FxHashMap::default())),
            },
            shutdown_rx,
            &mut connection_tasks,
        );

        // Reading to EOF drives the over-limit task to completion: it
        // replies "Busy" and closes without reserving a per-IP slot.
        let mut response = Vec::new();
        second_client.read_to_end(&mut response).await.unwrap();

        assert_eq!(response, b"B\n");
        assert_eq!(
            per_ip_connections.lock().unwrap().get(&ip).copied(),
            Some(MAX_CONNECTIONS_PER_IP),
            "the rejected connection must not have reserved a slot"
        );

        connection_tasks.abort_all();

        while connection_tasks.join_next().await.is_some() {}
    }

    #[tokio::test(flavor = "current_thread")]
    async fn a_connection_rejected_by_the_per_ip_cap_does_not_hold_a_global_slot() {
        // Issue #1664: the per-IP rejection used to keep the global permit
        // while `reject_over_limit` read for up to `BUSY_REPLY_DRAIN_WINDOW`
        // (200 ms) from a peer that does not close. A source at its per-IP
        // cap could then hold global slots by churning rejected connections
        // it keeps open: the very thing the per-IP cap exists to prevent.
        // Global limit 4, one live connection that fills the per-IP cap,
        // two rejected ones kept open (they have read their `B`, so each is
        // inside its drain window): only the live one may hold a slot.
        let connection_limit = Arc::new(Semaphore::new(4));
        let per_ip_connections: PerIpConnections = Arc::new(Mutex::new(HashMap::new()));
        let cluster_state = ClusterState {
            registry: Arc::new(RegistryState::default()),
            current_join: Arc::new(Mutex::new(None)),
        };
        let ip = std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST);
        // Stands in for `MAX_CONNECTIONS_PER_IP - 1` other live connections
        // from this IP; the live connection below fills the cap exactly.
        per_ip_connections
            .lock()
            .unwrap()
            .insert(ip, MAX_CONNECTIONS_PER_IP - 1);
        let mut connection_tasks = JoinSet::new();
        let (_shutdown_tx, shutdown_rx) = watch::channel(false);
        let config = || ConnectionConfig {
            idle_timeout: IDLE_TIMEOUT,
            list_ready_at: Instant::now(),
            replication: 2,
            auth_secret: None,
            tls_acceptor: None,
            tls_connector: None,
            announce_limiter: Arc::new(Mutex::new(FxHashMap::default())),
        };

        let (_live_client, live_server) = tcp_pair().await;
        dispatch_connection(
            live_server,
            SocketAddr::new(ip, 9000),
            cluster_state.clone(),
            Arc::clone(&connection_limit),
            Arc::clone(&per_ip_connections),
            config(),
            shutdown_rx.clone(),
            &mut connection_tasks,
        );
        tokio::task::yield_now().await;
        assert_eq!(connection_limit.available_permits(), 3);

        let mut rejected_clients = Vec::new();
        for port in [9001, 9002] {
            let (mut client, server) = tcp_pair().await;
            dispatch_connection(
                server,
                SocketAddr::new(ip, port),
                cluster_state.clone(),
                Arc::clone(&connection_limit),
                Arc::clone(&per_ip_connections),
                config(),
                shutdown_rx.clone(),
                &mut connection_tasks,
            );
            let mut busy = [0u8; 2];
            client.read_exact(&mut busy).await.unwrap();
            assert_eq!(&busy, b"B\n");
            // Kept open: the server's drain waits for this peer to close.
            rejected_clients.push(client);
        }

        assert_eq!(
            connection_limit.available_permits(),
            3,
            "a rejected connection is holding a global slot while its busy reply drains"
        );

        connection_tasks.abort_all();
        while connection_tasks.join_next().await.is_some() {}
    }

    #[test]
    fn try_acquire_per_ip_denies_once_the_cap_is_reached_and_frees_the_slot_on_drop() {
        // Mirrors `src/server.rs`'s own
        // `try_acquire_per_ip_denies_once_the_cap_is_reached_and_frees_the_slot_on_drop`.
        let counts: PerIpConnections = Arc::new(Mutex::new(HashMap::new()));
        let ip = std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST);

        let mut guards = Vec::new();
        for _ in 0..MAX_CONNECTIONS_PER_IP {
            guards.push(
                try_acquire_per_ip(&counts, ip, MAX_CONNECTIONS_PER_IP)
                    .expect("under the per-IP cap"),
            );
        }

        assert!(
            try_acquire_per_ip(&counts, ip, MAX_CONNECTIONS_PER_IP).is_none(),
            "the per-IP cap must reject a connection once MAX_CONNECTIONS_PER_IP is reached"
        );

        // A different source IP has its own, independent budget.
        let other_ip = std::net::IpAddr::V4(std::net::Ipv4Addr::new(10, 0, 0, 1));
        assert!(try_acquire_per_ip(&counts, other_ip, MAX_CONNECTIONS_PER_IP).is_some());

        // Dropping one guard frees its slot for the same IP again.
        guards.pop();
        assert!(try_acquire_per_ip(&counts, ip, MAX_CONNECTIONS_PER_IP).is_some());
    }

    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn sweep_expired_drops_nodes_past_the_liveness_timeout() {
        let registry: Registry = Arc::new(RegistryState::default());
        lock(&registry).insert(
            "some-name".to_string(),
            NodeInfo::new(
                "127.0.0.1:8356".to_string(),
                NodeState::Joined,
                "tk-some-name".to_string(),
            ),
        );

        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let current_join: CurrentJoin = Arc::new(Mutex::new(None));
        let sweep_task = tokio::spawn(sweep_expired(
            Arc::clone(&registry),
            current_join,
            None,
            None,
            2,
            Instant::now(),
            Duration::from_secs(1),
            shutdown_rx,
        ));

        tokio::task::yield_now().await;
        // In steps well under half the 1 s liveness timeout: one jump of 2 s
        // would be a stalled sweep, which evicts nothing (issue #1312).
        advance_in_steps(Duration::from_secs(2), Duration::from_millis(100)).await;

        assert!(lock(&registry).is_empty());

        shutdown_tx.send_replace(true);
        sweep_task.await.unwrap();
    }

    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn sweep_expired_drops_a_waiting_node_past_its_bounded_wait_and_wakes_it() {
        // Regression: with auth unset, an attacker's `J` under a fake
        // name used to sit `Waiting` forever — `wait_for_promotion`
        // applies no idle timeout, and `sweep_expired` excluded
        // Waiting/Joining nodes from its liveness check entirely — one of
        // `MAX_CONNECTIONS` slots held open with nothing ever reclaiming
        // it. A `Waiting` node must eventually be reaped even if no join
        // ever reaches it, and — like an abandoned join (issue #4) — the
        // connection parked on its `Notify` must be woken, not stranded.
        let registry: Registry = Arc::new(RegistryState::default());
        let info = NodeInfo::with_queue_position(
            "10.0.0.1:9000".to_string(),
            NodeState::Waiting,
            "tk-attacker".to_string(),
            1,
        );
        let promoted = Arc::clone(&info.promoted);
        lock(&registry).insert("attacker".to_string(), info);

        let current_join: CurrentJoin = Arc::new(Mutex::new(None));
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let sweep_task = tokio::spawn(sweep_expired(
            Arc::clone(&registry),
            current_join,
            None,
            None,
            2,
            // Far in the future: this test is about the bounded wait, not
            // join orchestration — the post-grace kick (issue #63) would
            // otherwise promote this lone Waiting entry.
            Instant::now() + Duration::from_secs(365 * 86_400),
            Duration::from_secs(60),
            shutdown_rx,
        ));

        // Parked on the same `Notify` a real `wait_for_promotion` call
        // would be — `notify_one`'s stored-permit semantics mean this
        // resolves whenever the wake happens, regardless of exactly when
        // this task itself gets polled relative to it.
        let woken = tokio::spawn(async move {
            promoted.notified().await;
        });

        tokio::task::yield_now().await;
        // Comfortably below the bound at queue position 1 — must still
        // be waiting.
        tokio::time::advance(waiting_timeout_for(1) - Duration::from_secs(1)).await;
        tokio::task::yield_now().await;
        assert!(
            lock(&registry).contains_key("attacker"),
            "evicted before its bounded wait elapsed"
        );

        tokio::time::advance(Duration::from_secs(2)).await;
        tokio::task::yield_now().await;

        assert!(!lock(&registry).contains_key("attacker"));
        tokio::time::timeout(Duration::from_secs(5), woken)
            .await
            .expect("the Notify was never fired — the connection would be stranded")
            .unwrap();

        shutdown_tx.send_replace(true);
        sweep_task.await.unwrap();
    }

    #[tokio::test(flavor = "current_thread")]
    async fn announce_rate_limits_new_registrations_from_one_source_but_not_refreshes() {
        // Regression: `P` holds no connection open, so connect -> `A` ->
        // `P` -> disconnect, repeated under a fresh name each time, used
        // to be able to fill the registry toward `MAX_REGISTRY_SIZE` as
        // fast as new connections could be opened. A shared, per-source
        // cooldown must reject a second *new* name from the same source
        // shortly after the first, while never gating a refresh of an
        // already-known name, and never affecting a different source.
        let registry: Registry = Arc::new(RegistryState::default());
        let current_join: CurrentJoin = Arc::new(Mutex::new(None));
        let (_shutdown_tx, shutdown_rx) = watch::channel(false);
        let announce_limiter: AnnounceLimiter = Arc::new(Mutex::new(FxHashMap::default()));
        let peer_ip = std::net::IpAddr::V4(std::net::Ipv4Addr::new(10, 0, 0, 5));
        let other_peer_ip = std::net::IpAddr::V4(std::net::Ipv4Addr::new(10, 0, 0, 6));

        let config = || ConnectionConfig {
            idle_timeout: IDLE_TIMEOUT,
            list_ready_at: Instant::now(),
            replication: 2,
            auth_secret: None,
            tls_acceptor: None,
            tls_connector: None,
            announce_limiter: Arc::clone(&announce_limiter),
        };

        // First: a new name from this source is admitted.
        let (mut first, server) = tcp_pair().await;
        tokio::spawn(handle_connection(
            MaybeTls::Plain(server),
            peer_ip,
            Arc::clone(&registry),
            Arc::clone(&current_join),
            config(),
            shutdown_rx.clone(),
            Arc::new(std::sync::Mutex::new(None)),
        ));
        first
            .write_all(b"P 6 8356 9\nnode-atk-node-a")
            .await
            .unwrap();
        let mut response = [0u8; 2];
        first.read_exact(&mut response).await.unwrap();
        assert_eq!(&response, b"R\n");

        // The source may spend the rest of its burst (issue #1148: a fleet
        // sharing one source must get in at once); spend it here directly.
        for _ in 1..ANNOUNCE_INSERT_BURST {
            assert!(announce_insert_allowed(
                &announce_limiter,
                peer_ip,
                AnnounceKind::Node
            ));
        }

        // Immediately after: one more new name from the same source is
        // refused — the connection closes with no `R`.
        let (mut second, server) = tcp_pair().await;
        tokio::spawn(handle_connection(
            MaybeTls::Plain(server),
            peer_ip,
            Arc::clone(&registry),
            Arc::clone(&current_join),
            config(),
            shutdown_rx.clone(),
            Arc::new(std::sync::Mutex::new(None)),
        ));
        second
            .write_all(b"P 6 8357 9\nnode-btk-node-b")
            .await
            .unwrap();
        let mut buffer = [0u8; 2];
        let bytes_read = second.read(&mut buffer).await.unwrap();
        assert_eq!(
            bytes_read, 0,
            "expected the connection to close, got {buffer:?}"
        );
        assert!(!lock(&registry).contains_key("node-b"));

        // But re-announcing the SAME, already-registered name from the
        // same source is never gated by the limiter, no matter how soon.
        let (mut refresh, server) = tcp_pair().await;
        tokio::spawn(handle_connection(
            MaybeTls::Plain(server),
            peer_ip,
            Arc::clone(&registry),
            Arc::clone(&current_join),
            config(),
            shutdown_rx.clone(),
            Arc::new(std::sync::Mutex::new(None)),
        ));
        refresh
            .write_all(b"P 6 8358 9\nnode-atk-node-a")
            .await
            .unwrap();
        let mut response = [0u8; 2];
        refresh.read_exact(&mut response).await.unwrap();
        assert_eq!(&response, b"R\n");

        // A different source is entirely unaffected by this source's
        // cooldown.
        let (mut other_source, server) = tcp_pair().await;
        tokio::spawn(handle_connection(
            MaybeTls::Plain(server),
            other_peer_ip,
            Arc::clone(&registry),
            Arc::clone(&current_join),
            config(),
            shutdown_rx,
            Arc::new(std::sync::Mutex::new(None)),
        ));
        other_source
            .write_all(b"P 6 8359 9\nnode-ctk-node-c")
            .await
            .unwrap();
        let mut response = [0u8; 2];
        other_source.read_exact(&mut response).await.unwrap();
        assert_eq!(&response, b"R\n");
    }

    #[tokio::test(flavor = "current_thread")]
    async fn dispatch_connection_serves_commands_over_tls() {
        let _ = rustls::crypto::ring::default_provider().install_default();

        let rcgen::CertifiedKey { cert, signing_key } =
            rcgen::generate_simple_self_signed(vec!["localhost".to_string()]).unwrap();
        let cert_der = cert.der().clone();
        let key_der = PrivateKeyDer::Pkcs8(signing_key.serialize_der().into());

        let server_config = ServerConfig::builder()
            .with_no_client_auth()
            .with_single_cert(vec![cert_der.clone()], key_der)
            .unwrap();
        let acceptor = TlsAcceptor::from(Arc::new(server_config));

        let mut roots = RootCertStore::empty();
        roots.add(cert_der).unwrap();
        let client_config = ClientConfig::builder()
            .with_root_certificates(roots)
            .with_no_client_auth();
        let connector = TlsConnector::from(Arc::new(client_config));

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();

        let cluster_state = ClusterState {
            registry: Arc::new(RegistryState::default()),
            current_join: Arc::new(Mutex::new(None)),
        };
        let connection_limit = Arc::new(Semaphore::new(1));
        let per_ip_connections: PerIpConnections = Arc::new(Mutex::new(HashMap::new()));
        let (_shutdown_tx, shutdown_rx) = watch::channel(false);

        let config = ConnectionConfig {
            idle_timeout: IDLE_TIMEOUT,
            list_ready_at: Instant::now(),
            replication: 2,
            auth_secret: None,
            tls_acceptor: Some(acceptor),
            tls_connector: None,
            announce_limiter: Arc::new(Mutex::new(FxHashMap::default())),
        };

        let server_task = tokio::spawn(async move {
            let (stream, peer_addr) = listener.accept().await.unwrap();
            let mut connection_tasks = JoinSet::new();

            dispatch_connection(
                stream,
                peer_addr,
                cluster_state,
                connection_limit,
                per_ip_connections,
                config,
                shutdown_rx,
                &mut connection_tasks,
            );

            while connection_tasks.join_next().await.is_some() {}
        });

        let tcp = TcpStream::connect(address).await.unwrap();
        let server_name = ServerName::try_from("localhost").unwrap();
        let mut tls = connector.connect(server_name, tcp).await.unwrap();

        tls.write_all(b"L\n").await.unwrap();

        let expected = b"N 0 2\n";
        let mut response = vec![0_u8; expected.len()];
        tls.read_exact(&mut response).await.unwrap();
        assert_eq!(response, expected);

        // Without this, the connection sits open and the server task only
        // finishes after IDLE_TIMEOUT (60s) — see the equivalent fix on
        // the node's own `dispatch_connection_serves_commands_over_tls`,
        // `src/server.rs`.
        tls.shutdown().await.unwrap();

        server_task.await.unwrap();
    }

    /// Builds a self-signed cert/key pair plus a matching `TlsAcceptor`
    /// (for a fake node standing in as the connection's server side) and
    /// `TlsConnector` (for discovery's own outbound connect), mirroring
    /// `dispatch_connection_serves_commands_over_tls`'s setup.
    fn self_signed_tls_pair() -> (TlsAcceptor, TlsConnector) {
        let rcgen::CertifiedKey { cert, signing_key } =
            rcgen::generate_simple_self_signed(vec!["localhost".to_string()]).unwrap();
        let cert_der = cert.der().clone();
        let key_der = PrivateKeyDer::Pkcs8(signing_key.serialize_der().into());

        let server_config = ServerConfig::builder()
            .with_no_client_auth()
            .with_single_cert(vec![cert_der.clone()], key_der)
            .unwrap();
        let acceptor = TlsAcceptor::from(Arc::new(server_config));

        let mut roots = RootCertStore::empty();
        roots.add(cert_der).unwrap();
        let client_config = ClientConfig::builder()
            .with_root_certificates(roots)
            .with_no_client_auth();
        let connector = TlsConnector::from(Arc::new(client_config));

        (acceptor, connector)
    }

    #[tokio::test(flavor = "current_thread")]
    async fn a_stalled_tls_handshake_does_not_block_a_second_connection() {
        // CRITICAL regression: `dispatch_connection` used to run the
        // connection-limit check and the TLS handshake inline, awaited
        // before ever spawning the connection's task. With
        // `#[tokio::main(flavor = "current_thread")]` (this process's
        // actual runtime), that meant a client that stalled its
        // ClientHello blocked `run`'s entire `select!` — accepts,
        // shutdown detection, task reaping — for up to
        // `TLS_HANDSHAKE_TIMEOUT`, since nothing else could run until that
        // `.await` resolved. Both connections below are dispatched
        // back-to-back with nothing awaited in between (exactly like two
        // consecutive iterations of `run`'s accept loop would); if the fix
        // regressed, the connection-limit-and-handshake work would again
        // run inline in the second `dispatch_connection` call, and it
        // wouldn't even start until the first connection's stalled
        // handshake timed out 10s later, blowing the 2s bound below.
        let _ = rustls::crypto::ring::default_provider().install_default();
        let (acceptor, connector) = self_signed_tls_pair();

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();

        let cluster_state = ClusterState {
            registry: Arc::new(RegistryState::default()),
            current_join: Arc::new(Mutex::new(None)),
        };
        let connection_limit = Arc::new(Semaphore::new(2));
        let per_ip_connections: PerIpConnections = Arc::new(Mutex::new(HashMap::new()));
        let (_shutdown_tx, shutdown_rx) = watch::channel(false);

        let config = ConnectionConfig {
            idle_timeout: IDLE_TIMEOUT,
            list_ready_at: Instant::now(),
            replication: 2,
            auth_secret: None,
            tls_acceptor: Some(acceptor),
            tls_connector: None,
            announce_limiter: Arc::new(Mutex::new(FxHashMap::default())),
        };

        let mut connection_tasks = JoinSet::new();

        // Connection A: connects but never sends a ClientHello (or
        // anything else) — its TLS handshake stalls until
        // `TLS_HANDSHAKE_TIMEOUT`.
        let _stalled_client = TcpStream::connect(address).await.unwrap();
        let (stalled_stream, stalled_addr) = listener.accept().await.unwrap();
        dispatch_connection(
            stalled_stream,
            stalled_addr,
            cluster_state.clone(),
            Arc::clone(&connection_limit),
            Arc::clone(&per_ip_connections),
            config.clone(),
            shutdown_rx.clone(),
            &mut connection_tasks,
        );

        // Connection B: dispatched immediately after, with nothing
        // awaited in between — must still be served promptly.
        let outcome = tokio::time::timeout(Duration::from_secs(2), async {
            let served_client = TcpStream::connect(address).await.unwrap();
            let (served_stream, served_addr) = listener.accept().await.unwrap();
            dispatch_connection(
                served_stream,
                served_addr,
                cluster_state,
                connection_limit,
                per_ip_connections,
                config,
                shutdown_rx,
                &mut connection_tasks,
            );

            let server_name = ServerName::try_from("localhost").unwrap();
            let mut tls = connector.connect(server_name, served_client).await.unwrap();
            tls.write_all(b"L\n").await.unwrap();

            let expected = b"N 0 2\n";
            let mut response = vec![0_u8; expected.len()];
            tls.read_exact(&mut response).await.unwrap();
            assert_eq!(response, expected);
        })
        .await;

        outcome
            .expect("the second connection was never served — a stalled TLS handshake blocked it");

        connection_tasks.abort_all();
        while connection_tasks.join_next().await.is_some() {}
    }

    #[tokio::test(flavor = "current_thread")]
    async fn send_migrate_delivers_m_to_a_tls_secured_node() {
        let _ = rustls::crypto::ring::default_provider().install_default();
        let (acceptor, connector) = self_signed_tls_pair();

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        // The self-signed cert is issued for "localhost", not an IP, so
        // connect via that name for SNI/hostname verification to pass.
        let address = format!("localhost:{}", listener.local_addr().unwrap().port());

        let received: Arc<std::sync::Mutex<Vec<u8>>> = Arc::new(std::sync::Mutex::new(Vec::new()));
        let received_task = Arc::clone(&received);
        let node_task = tokio::spawn(async move {
            let (tcp, _) = listener.accept().await.unwrap();
            let mut tls = acceptor.accept(tcp).await.unwrap();
            let mut buffer = [0u8; 256];
            let bytes_read = tls.read(&mut buffer).await.unwrap();
            received_task
                .lock()
                .unwrap()
                .extend_from_slice(&buffer[..bytes_read]);
            tls.write_all(b"A 0\n").await.unwrap();
        });

        // A plaintext-only `send_migrate` call would never complete a TLS
        // handshake with this fake node and this would hang/error instead.
        send_migrate(
            "ready-token",
            &address,
            &None,
            &Some(connector),
            "joining-node",
            "127.0.0.1:9",
            "joiner-token",
            &[],
            2,
            9,
            OUTBOUND_IO_TIMEOUT,
        )
        .await
        .unwrap();

        node_task.await.unwrap();

        let mut expected = b"M 12 11 12 0 2 11 9\n".to_vec();
        expected.extend_from_slice(b"ready-token");
        expected.extend_from_slice(b"joining-node");
        expected.extend_from_slice(b"127.0.0.1:9");
        expected.extend_from_slice(b"joiner-token");
        assert_eq!(*received.lock().unwrap(), expected);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn read_ack_line_stops_at_the_line_cap_instead_of_buffering_a_flood() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap().to_string();
        let node_task = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            // No newline, ever: far more than `MAX_ACK_LINE_LENGTH`.
            let _ = stream.write_all(&vec![b'x'; 1024 * 1024]).await;
        });

        let mut stream = connect_client_stream(&address, None).await.unwrap();
        let error = read_ack_line(&mut stream, Duration::from_secs(5))
            .await
            .expect_err("an unterminated flood must hit the cap");
        node_task.abort();

        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn read_ack_line_fails_within_the_overall_deadline_against_a_trickling_peer() {
        // Regression for issue #216: the ack line read used to call
        // read_exact_timed once per byte, re-arming a fresh io_timeout
        // window on every single-byte read instead of racing the whole
        // line against one deadline. A peer that drips the ack slower than
        // the per-byte timeout but faster than the overall timeout would
        // never trip that per-byte timeout at all, making the doc
        // comment's "bounded overall" claim false. Here the peer sends the
        // 4-byte "A 0\n" ack 150ms apart (600ms total) against a 200ms
        // io_timeout: the fix must fail out around 200ms, not succeed
        // around 600ms.
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap().to_string();

        let node_task = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            for byte in b"A 0\n" {
                stream.write_all(&[*byte]).await.unwrap();
                tokio::time::sleep(Duration::from_millis(150)).await;
            }
        });

        let io_timeout = Duration::from_millis(200);
        let mut stream = connect_client_stream(&address, None).await.unwrap();

        let started = Instant::now();
        let result = read_ack_line(&mut stream, io_timeout).await;
        let elapsed = started.elapsed();

        node_task.abort();

        let error = result.expect_err("expected the trickling peer to time out overall");
        assert_eq!(error.kind(), io::ErrorKind::TimedOut);
        assert!(
            elapsed >= Duration::from_millis(100),
            "failed suspiciously fast ({elapsed:?}) — is this actually exercising the trickle?"
        );
        assert!(
            elapsed < Duration::from_millis(400),
            "expected the read to fail close to io_timeout (~200ms), took {elapsed:?} instead — \
             the per-byte re-arm bug would let this keep succeeding past 600ms"
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn send_migrate_with_retry_recovers_from_a_transient_failure() {
        // Issue #20: the first attempt is a transient failure (the "node"
        // accepts the connection then drops it without acking); the
        // second succeeds. The whole call must still return `Ok`.
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap().to_string();

        let node_task = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            drop(stream);

            let (mut stream, _) = listener.accept().await.unwrap();
            let mut buffer = [0u8; 256];
            let _ = stream.read(&mut buffer).await.unwrap();
            stream.write_all(b"A 0\n").await.unwrap();
        });

        let result = send_migrate_with_retry(
            "ready-token",
            "ready-node",
            &address,
            &None,
            &None,
            "joining-node",
            "127.0.0.1:9",
            "joiner-token",
            &[],
            2,
            9,
            OUTBOUND_IO_TIMEOUT,
            || JoinStatus::Current,
        )
        .await;

        assert!(
            matches!(result, Ok(Some(_))),
            "expected recovery on retry, got {result:?}"
        );
        node_task.await.unwrap();
    }

    #[tokio::test(flavor = "current_thread")]
    async fn send_migrate_with_retry_gives_up_after_exhausting_every_attempt() {
        // Every attempt fails the same way (connection accepted then
        // dropped without acking) — the call must give up after exactly
        // `MIGRATE_SEND_ATTEMPTS` tries, not retry forever.
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap().to_string();

        let attempts = Arc::new(std::sync::atomic::AtomicU32::new(0));
        let server_attempts = Arc::clone(&attempts);
        let node_task = tokio::spawn(async move {
            loop {
                let Ok((stream, _)) = listener.accept().await else {
                    return;
                };
                server_attempts.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                drop(stream);
            }
        });

        let result = send_migrate_with_retry(
            "ready-token",
            "ready-node",
            &address,
            &None,
            &None,
            "joining-node",
            "127.0.0.1:9",
            "joiner-token",
            &[],
            2,
            9,
            OUTBOUND_IO_TIMEOUT,
            || JoinStatus::Current,
        )
        .await;

        assert!(result.is_err());
        assert_eq!(
            attempts.load(std::sync::atomic::Ordering::SeqCst),
            MIGRATE_SEND_ATTEMPTS
        );

        node_task.abort();
    }

    /// Issue #1030: the join is abandoned between two `M` attempts. The
    /// retry must not go out — it would reach the node after the
    /// abandon's `X` and start a handoff to a dropped joiner — and since
    /// the first attempt may have landed with its ack lost, an `X` of this
    /// send's own follows instead.
    #[tokio::test(flavor = "current_thread")]
    async fn send_migrate_with_retry_stops_after_an_abandon_and_cancels_instead_of_retrying() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap().to_string();

        let received: Arc<std::sync::Mutex<Vec<Vec<u8>>>> =
            Arc::new(std::sync::Mutex::new(Vec::new()));
        let received_task = Arc::clone(&received);
        let node_task = tokio::spawn(async move {
            // Attempt 1: accepted, then dropped without an ack.
            let (stream, _) = listener.accept().await.unwrap();
            drop(stream);
            // Whatever comes next is recorded and acked.
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut buffer = [0u8; 256];
            let bytes_read = stream.read(&mut buffer).await.unwrap();
            received_task
                .lock()
                .unwrap()
                .push(buffer[..bytes_read].to_vec());
            stream.write_all(b"A\n").await.unwrap();
        });

        // Current for the first attempt, abandoned by the time of the retry.
        let checks = std::sync::atomic::AtomicU32::new(0);
        let result = send_migrate_with_retry(
            "ready-token",
            "ready-node",
            &address,
            &None,
            &None,
            "joining-node",
            "127.0.0.1:9",
            "joiner-token",
            &[],
            2,
            9,
            OUTBOUND_IO_TIMEOUT,
            || {
                if checks.fetch_add(1, std::sync::atomic::Ordering::SeqCst) == 0 {
                    JoinStatus::Current
                } else {
                    JoinStatus::Ended
                }
            },
        )
        .await;

        let error = result.expect_err("an abandoned join's M is not retried");
        assert!(
            error.to_string().contains("abandoned before attempt 2"),
            "{error}"
        );
        node_task.await.unwrap();
        let frames = received.lock().unwrap();
        assert_eq!(frames.len(), 1);
        assert!(
            frames[0].starts_with(b"X "),
            "the second frame must be an X (cancel), got {:?}",
            String::from_utf8_lossy(&frames[0])
        );
    }

    /// Issue #1870: attempt 1's `M` lands but its ack never comes back;
    /// meanwhile the ready member finishes its handoff and sends `C`, and
    /// the joiner is promoted. The retry must neither resend `M` nor send
    /// an `X` — that would make the member abandon a handoff that already
    /// completed — and the send is not a failure.
    #[tokio::test(flavor = "current_thread")]
    async fn send_migrate_with_retry_sends_no_cancel_after_the_join_completed() {
        let registry: Registry = Arc::new(RegistryState::default());
        lock(&registry).insert(
            "node-a".to_string(),
            NodeInfo::new(
                "127.0.0.1:1".to_string(),
                NodeState::Joined,
                "tk-node-a".to_string(),
            ),
        );
        lock(&registry).insert(
            "node-b".to_string(),
            NodeInfo::new(
                "127.0.0.1:2".to_string(),
                NodeState::Joining,
                "tk-node-b".to_string(),
            ),
        );
        let late = LateHandoffs::default();
        let promoted = Arc::clone(&late.promoted);
        let current_join: CurrentJoin = Arc::new(Mutex::new(Some(PendingJoin {
            joining_name: "node-b".to_string(),
            expected: [("node-a".to_string(), "tk-node-a".to_string())]
                .into_iter()
                .collect(),
            completed: HashSet::new(),
            started_at: Instant::now(),
            max_entries: 0,
            generation: 9,
            roster: Arc::new(Vec::new()),
            late,
        })));

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap().to_string();
        let node_registry = Arc::clone(&registry);
        let node_join = Arc::clone(&current_join);
        let node_task = tokio::spawn(async move {
            // node-a: reads the `M`, withholds the ack, and reports `C`.
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut buffer = [0u8; 256];
            let _ = stream.read(&mut buffer).await.unwrap();
            assert!(
                handle_complete(
                    &node_registry,
                    &node_join,
                    "node-a",
                    "node-b",
                    "tk-node-a",
                    Some(9),
                )
                .await
            );
            // Hold the connection until the attempt times out, then
            // record anything discovery sends after it.
            let _ = stream.read(&mut buffer).await;
            let mut frames = Vec::new();
            while let Ok(Ok((mut stream, _))) =
                timeout(Duration::from_millis(500), listener.accept()).await
            {
                let bytes_read = stream.read(&mut buffer).await.unwrap_or(0);
                frames.push(buffer[..bytes_read].to_vec());
                let _ = stream.write_all(b"A\n").await;
            }
            frames
        });

        let result = send_migrate_with_retry(
            "tk-node-a",
            "node-a",
            &address,
            &None,
            &None,
            "node-b",
            "127.0.0.1:2",
            "tk-node-b",
            &[],
            2,
            9,
            Duration::from_millis(200),
            || join_status(&current_join, "node-b", 9, &promoted),
        )
        .await;

        assert_eq!(lock(&registry)["node-b"].state, NodeState::Joined);
        assert!(
            matches!(result, Ok(None)),
            "a completed join's send is not a failure, got {result:?}"
        );
        let frames = node_task.await.unwrap();
        assert!(
            frames.is_empty(),
            "nothing may follow the promotion, got {:?}",
            frames
                .iter()
                .map(|frame| String::from_utf8_lossy(frame).into_owned())
                .collect::<Vec<_>>()
        );
    }

    /// Issue #1030: the sweep decides to abandon a join on one tick and
    /// runs the abandon on a spawned task; if a different join occupies
    /// the slot by then, the abandon must leave it alone.
    #[tokio::test(flavor = "current_thread")]
    async fn abandon_current_join_leaves_a_join_it_was_not_decided_for_alone() {
        let registry: Registry = Arc::new(RegistryState::default());
        lock(&registry).insert(
            "node-a".to_string(),
            NodeInfo::new(
                "127.0.0.1:1".to_string(),
                NodeState::Joined,
                "tk-node-a".to_string(),
            ),
        );
        lock(&registry).insert(
            "node-b".to_string(),
            NodeInfo::new(
                "127.0.0.1:2".to_string(),
                NodeState::Joining,
                "tk-node-b".to_string(),
            ),
        );
        let current_join: CurrentJoin = Arc::new(Mutex::new(Some(PendingJoin {
            joining_name: "node-b".to_string(),
            expected: [("node-a".to_string(), "tk-node-a".to_string())]
                .into_iter()
                .collect(),
            completed: HashSet::new(),
            started_at: Instant::now(),
            max_entries: 0,
            roster: Arc::new(Vec::new()),
            late: LateHandoffs::default(),
            generation: 7,
        })));

        // Decided for an earlier incarnation of node-b's join (or, the same
        // way, for a different joiner): a no-op.
        let stale = JoinIdentity {
            joining_name: "node-b".to_string(),
            generation: 6,
        };
        abandon_current_join(
            &registry,
            &current_join,
            &None,
            &None,
            2,
            Instant::now(),
            &stale,
            "test: stale decision",
        )
        .await;
        assert_eq!(
            lock_current_join(&current_join)
                .as_ref()
                .map(|pending| pending.generation),
            Some(7),
            "the join in the slot is not the one the abandon was decided for"
        );
        assert!(lock(&registry).contains_key("node-b"));
        assert_eq!(
            registry
                .joins_abandoned_total
                .load(std::sync::atomic::Ordering::Relaxed),
            0
        );

        // Decided for the join actually in the slot: abandoned as before.
        let current = lock_current_join(&current_join)
            .as_ref()
            .unwrap()
            .identity();
        abandon_current_join(
            &registry,
            &current_join,
            &None,
            &None,
            2,
            Instant::now(),
            &current,
            "test: current decision",
        )
        .await;
        assert!(lock_current_join(&current_join).is_none());
        assert!(!lock(&registry).contains_key("node-b"));
        assert_eq!(
            registry
                .joins_abandoned_total
                .load(std::sync::atomic::Ordering::Relaxed),
            1
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn send_cancel_delivers_x_to_a_tls_secured_node() {
        let _ = rustls::crypto::ring::default_provider().install_default();
        let (acceptor, connector) = self_signed_tls_pair();

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        // The self-signed cert is issued for "localhost", not an IP, so
        // connect via that name for SNI/hostname verification to pass.
        let address = format!("localhost:{}", listener.local_addr().unwrap().port());

        let received: Arc<std::sync::Mutex<Vec<u8>>> = Arc::new(std::sync::Mutex::new(Vec::new()));
        let received_task = Arc::clone(&received);
        let node_task = tokio::spawn(async move {
            let (tcp, _) = listener.accept().await.unwrap();
            let mut tls = acceptor.accept(tcp).await.unwrap();
            let mut buffer = [0u8; 256];
            let bytes_read = tls.read(&mut buffer).await.unwrap();
            received_task
                .lock()
                .unwrap()
                .extend_from_slice(&buffer[..bytes_read]);
            tls.write_all(b"A\n").await.unwrap();
        });

        send_cancel(
            "ready-token",
            &address,
            &None,
            &Some(connector),
            "joining-node",
            OUTBOUND_IO_TIMEOUT,
        )
        .await
        .unwrap();

        node_task.await.unwrap();

        let mut expected = b"X 12 11\n".to_vec();
        expected.extend_from_slice(b"ready-token");
        expected.extend_from_slice(b"joining-node");
        assert_eq!(*received.lock().unwrap(), expected);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn a_ready_nodes_connection_dying_mid_join_does_not_abandon_the_join() {
        let registry: Registry = Arc::new(RegistryState::default());

        lock(&registry).insert(
            "node-a".to_string(),
            NodeInfo::new(
                "127.0.0.1:1".to_string(),
                NodeState::Joined,
                "tk-node-a".to_string(),
            ),
        );
        lock(&registry).insert(
            "node-c".to_string(),
            NodeInfo::new(
                "127.0.0.1:2".to_string(),
                NodeState::Joining,
                "tk-node-c".to_string(),
            ),
        );

        let current_join: CurrentJoin = Arc::new(Mutex::new(Some(PendingJoin {
            joining_name: "node-c".to_string(),
            expected: [("node-a".to_string(), "tk-node-a".to_string())]
                .into_iter()
                .collect(),
            completed: HashSet::new(),
            started_at: Instant::now(),
            max_entries: 0,
            roster: Arc::new(Vec::new()),
            late: LateHandoffs::default(),
            generation: 1,
        })));

        // Node A's *heartbeat* connection to discovery dies (a transient
        // hiccup) while it's a ready member of an in-progress join.
        // Unlike the joining node's own connection (see the sibling test
        // below), this must not abandon the join: `C` (handoff complete)
        // is reported over its own short-lived connection
        // (`report_complete`), never the heartbeat one, so this event
        // says nothing about node A's actual handoff progress — only
        // `sweep_expired`'s size-derived `migration_timeout_for` should
        // ever reap a ready node that's truly gone (issue #10). `0` is the
        // matching connection id for a `NodeInfo` built directly by
        // `NodeInfo::new` rather than through `start_join` (see
        // `NodeInfo::owner_connection_id`'s doc comment).
        on_node_connection_ended(
            &registry,
            &current_join,
            &None,
            &None,
            2,
            Instant::now(),
            "node-a",
            0,
        )
        .await;

        assert!(
            lock_current_join(&current_join).is_some(),
            "a ready node's heartbeat connection dying must not abandon the join"
        );
        assert!(
            lock(&registry).contains_key("node-c"),
            "the joining node must still be waiting for promotion"
        );
        // Node A is a `Joined` node, so this event leaves its own
        // registry entry alone too — only `sweep_expired`'s liveness
        // timeout evicts it.
        assert!(lock(&registry).contains_key("node-a"));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn a_joining_nodes_connection_dying_abandons_its_own_join_and_cancels_ready_nodes() {
        let registry: Registry = Arc::new(RegistryState::default());

        let ready_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let ready_addr = ready_listener.local_addr().unwrap().to_string();
        let received: Arc<std::sync::Mutex<Vec<u8>>> = Arc::new(std::sync::Mutex::new(Vec::new()));
        let received_task = Arc::clone(&received);
        let ready_task = tokio::spawn(async move {
            let (mut connection, _) = ready_listener.accept().await.unwrap();
            let mut buffer = [0u8; 256];
            let bytes_read = connection.read(&mut buffer).await.unwrap();
            received_task
                .lock()
                .unwrap()
                .extend_from_slice(&buffer[..bytes_read]);
            connection.write_all(b"A\n").await.unwrap();
        });

        lock(&registry).insert(
            "node-a".to_string(),
            NodeInfo::new(ready_addr, NodeState::Joined, "tk-node-a".to_string()),
        );
        lock(&registry).insert(
            "node-b".to_string(),
            NodeInfo::new(
                "127.0.0.1:2".to_string(),
                NodeState::Joining,
                "tk-node-b".to_string(),
            ),
        );

        let current_join: CurrentJoin = Arc::new(Mutex::new(Some(PendingJoin {
            joining_name: "node-b".to_string(),
            expected: [("node-a".to_string(), "tk-node-a".to_string())]
                .into_iter()
                .collect(),
            completed: HashSet::new(),
            started_at: Instant::now(),
            max_entries: 0,
            roster: Arc::new(Vec::new()),
            late: LateHandoffs::default(),
            generation: 1,
        })));

        // Node B (the joining node itself) disconnects before being
        // promoted (staged node join pattern: the joining node dies mid-handoff).
        // `0` matches the connection id `NodeInfo::new` defaults to for an
        // entry built directly rather than through `start_join`.
        on_node_connection_ended(
            &registry,
            &current_join,
            &None,
            &None,
            2,
            Instant::now(),
            "node-b",
            0,
        )
        .await;

        ready_task.await.unwrap();

        assert!(lock_current_join(&current_join).is_none());
        assert!(!lock(&registry).contains_key("node-b"));

        let mut expected_cancel = b"X 6 9\n".to_vec();
        expected_cancel.extend_from_slice(b"tk-node-a");
        expected_cancel.extend_from_slice(b"node-b");
        assert_eq!(*received.lock().unwrap(), expected_cancel);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn a_stale_duplicate_connection_dying_during_joining_does_not_abandon_the_join() {
        // Regression for issue #3/#9 (MEDIUM): the joining node re-dials
        // with a duplicate `J` (a supported scenario, issue #7/#9) while
        // already `Joining` — its handoff in progress — and the newer
        // connection takes over ownership of the registration (see
        // `NodeInfo::owner_connection_id`). When the OLDER, now-
        // superseded connection then finally notices it's dead and its
        // teardown reports in, that must NOT abandon the still-healthy,
        // in-progress join: it isn't the connection currently recorded as
        // owning the entry.
        let registry: Registry = Arc::new(RegistryState::default());

        let ready_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let ready_addr = ready_listener.local_addr().unwrap().to_string();
        let received: Arc<std::sync::Mutex<Vec<u8>>> = Arc::new(std::sync::Mutex::new(Vec::new()));
        let received_task = Arc::clone(&received);
        let ready_task = tokio::spawn(async move {
            let (mut connection, _) = ready_listener.accept().await.unwrap();
            let mut buffer = [0u8; 256];
            let bytes_read = connection.read(&mut buffer).await.unwrap();
            received_task
                .lock()
                .unwrap()
                .extend_from_slice(&buffer[..bytes_read]);
            connection.write_all(b"A\n").await.unwrap();
        });

        lock(&registry).insert(
            "node-a".to_string(),
            NodeInfo::new(ready_addr, NodeState::Joined, "tk-node-a".to_string()),
        );
        // node-b is Joining, currently owned by connection id 2 — as if
        // it originally registered on connection 1 and then re-dialed
        // with a duplicate J on connection 2, which `start_join` records
        // as the new owner.
        let mut node_b = NodeInfo::new(
            "127.0.0.1:2".to_string(),
            NodeState::Joining,
            "tk-node-b".to_string(),
        );
        node_b.owner_connection_id = 2;
        lock(&registry).insert("node-b".to_string(), node_b);

        let current_join: CurrentJoin = Arc::new(Mutex::new(Some(PendingJoin {
            joining_name: "node-b".to_string(),
            expected: [("node-a".to_string(), "tk-node-a".to_string())]
                .into_iter()
                .collect(),
            completed: HashSet::new(),
            started_at: Instant::now(),
            max_entries: 0,
            roster: Arc::new(Vec::new()),
            late: LateHandoffs::default(),
            generation: 1,
        })));

        // The OLD connection (id 1, no longer the recorded owner) reports
        // its own end — this must be a complete no-op.
        on_node_connection_ended(
            &registry,
            &current_join,
            &None,
            &None,
            2,
            Instant::now(),
            "node-b",
            1,
        )
        .await;

        assert!(
            lock_current_join(&current_join).is_some(),
            "a stale, superseded connection dying must not abandon the in-progress join"
        );
        assert!(
            lock(&registry).contains_key("node-b"),
            "the joining node's registry entry must remain"
        );
        assert_eq!(
            lock(&registry).get("node-b").unwrap().owner_connection_id,
            2,
            "ownership must still be the newer connection's, untouched by the no-op"
        );

        // The CURRENT owner (connection id 2) later ending for real still
        // abandons the join normally — the fix only changes behavior for
        // a non-owning connection.
        on_node_connection_ended(
            &registry,
            &current_join,
            &None,
            &None,
            2,
            Instant::now(),
            "node-b",
            2,
        )
        .await;
        ready_task.await.unwrap();
        assert!(lock_current_join(&current_join).is_none());
        assert!(!lock(&registry).contains_key("node-b"));

        let mut expected_cancel = b"X 6 9\n".to_vec();
        expected_cancel.extend_from_slice(b"tk-node-a");
        expected_cancel.extend_from_slice(b"node-b");
        assert_eq!(*received.lock().unwrap(), expected_cancel);
    }

    /// A stand-in ready node for `send_cancel`: accepts each connection,
    /// reads the `X` frame, waits `delay`, then acks. Returns its address,
    /// a count of the connections it has accepted, and a count of the acks
    /// it has sent.
    async fn slow_cancel_node(delay: Duration) -> (String, Arc<AtomicU64>, Arc<AtomicU64>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap().to_string();
        let accepted = Arc::new(AtomicU64::new(0));
        let acked = Arc::new(AtomicU64::new(0));
        let (accept_counter, ack_counter) = (Arc::clone(&accepted), Arc::clone(&acked));
        tokio::spawn(async move {
            loop {
                let Ok((mut stream, _)) = listener.accept().await else {
                    return;
                };
                accept_counter.fetch_add(1, Ordering::SeqCst);
                let ack_counter = Arc::clone(&ack_counter);
                tokio::spawn(async move {
                    let mut chunk = [0u8; 256];
                    let _ = stream.read(&mut chunk).await;
                    tokio::time::sleep(delay).await;
                    if stream.write_all(b"A\n").await.is_ok() {
                        ack_counter.fetch_add(1, Ordering::SeqCst);
                    }
                });
            }
        });
        (address, accepted, acked)
    }

    /// Keeps `name`'s heartbeat fresh, standing in for a live node.
    fn keep_heartbeating(registry: &Registry, name: &'static str) -> tokio::task::JoinHandle<()> {
        let registry = Arc::clone(registry);
        tokio::spawn(async move {
            loop {
                if let Some(info) = lock(&registry).get_mut(name) {
                    info.last_heartbeat = Instant::now();
                    info.last_contact = info.last_heartbeat;
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        })
    }

    async fn wait_until(what: &str, limit: Duration, mut condition: impl FnMut() -> bool) {
        let deadline = Instant::now() + limit;
        while !condition() {
            assert!(Instant::now() < deadline, "timed out waiting for {what}");
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    fn timed_out_join_of_node_b_awaiting_node_a() -> PendingJoin {
        PendingJoin {
            joining_name: "node-b".to_string(),
            expected: [("node-a".to_string(), "tk-node-a".to_string())]
                .into_iter()
                .collect(),
            completed: HashSet::new(),
            // Already past `MIGRATION_TIMEOUT_BASE`: the first sweep tick
            // abandons it.
            started_at: Instant::now()
                .checked_sub(MIGRATION_TIMEOUT_BASE + Duration::from_secs(1))
                .expect("the clock has run long enough to backdate"),
            max_entries: 0,
            roster: Arc::new(Vec::new()),
            late: LateHandoffs::default(),
            generation: 1,
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn a_stuck_cancel_fan_out_does_not_stop_the_sweep_from_evicting_dead_nodes() {
        // The sweep used to await `abandon_current_join` inline: an `X` to a
        // ready member that accepts and never answers parked it for
        // `OUTBOUND_IO_TIMEOUT` (per batch), so liveness eviction paused
        // exactly while nodes were failing. Here node-a's `X` hangs for far
        // longer than this test runs, and node-c — which stops
        // heartbeating — must still be evicted in the meantime.
        let (node_a_addr, accepted, _acked) = slow_cancel_node(Duration::from_secs(30)).await;
        let registry: Registry = Arc::new(RegistryState::default());
        for (name, address, state) in [
            ("node-a", node_a_addr.as_str(), NodeState::Joined),
            ("node-b", "127.0.0.1:2", NodeState::Joining),
            ("node-c", "127.0.0.1:3", NodeState::Joined),
        ] {
            lock(&registry).insert(
                name.to_string(),
                NodeInfo::new(address.to_string(), state, format!("tk-{name}")),
            );
        }
        let _node_a_alive = keep_heartbeating(&registry, "node-a");
        let current_join: CurrentJoin =
            Arc::new(Mutex::new(Some(timed_out_join_of_node_b_awaiting_node_a())));

        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let started = Instant::now();
        let sweep_task = tokio::spawn(sweep_expired(
            Arc::clone(&registry),
            Arc::clone(&current_join),
            None,
            None,
            2,
            Instant::now(),
            Duration::from_millis(600),
            shutdown_rx,
        ));

        // The abandon is under way: join taken, `X` sent and hanging.
        wait_until(
            "the abandon's X to reach node-a",
            Duration::from_secs(3),
            || accepted.load(Ordering::SeqCst) == 1,
        )
        .await;
        assert!(lock_current_join(&current_join).is_none());

        // ...and node-c is evicted anyway, well before that `X` could
        // time out (10 s) — the sweep was not parked behind it.
        wait_until("node-c's eviction", Duration::from_secs(4), || {
            !lock(&registry).contains_key("node-c")
        })
        .await;
        assert!(
            started.elapsed() < OUTBOUND_IO_TIMEOUT / 2,
            "evicted only after {:?}",
            started.elapsed()
        );
        assert!(lock(&registry).contains_key("node-a"));

        // The stuck `X` would hold a graceful shutdown for up to
        // `SHUTDOWN_TIMEOUT` (see the test below), so don't wait for it.
        drop(shutdown_tx);
        sweep_task.abort();
        let _ = sweep_task.await;
    }

    #[tokio::test(flavor = "current_thread")]
    async fn a_mid_join_eviction_seen_while_a_fan_out_runs_is_acted_on_once_it_finishes() {
        // At most one sweep-initiated fan-out runs at a time, so an
        // abandon the sweep wants to start while another is still in flight
        // must wait for it — but it must not be lost: a member evicted
        // mid-join is an edge, seen on one tick only.
        //
        // J1 (timed out) is abandoned first; its `X` to node-a takes 1.5 s.
        // Meanwhile J2 is installed with node-c as a ready member, and
        // node-c stops heartbeating. node-c's eviction (~0.6 s) happens
        // during J1's fan-out, so J2's abandon is deferred (still current
        // at ~1 s), then performed after the fan-out is done.
        let (node_a_addr, accepted, _acked) = slow_cancel_node(Duration::from_millis(1500)).await;
        let registry: Registry = Arc::new(RegistryState::default());
        for (name, address, state) in [
            ("node-a", node_a_addr.as_str(), NodeState::Joined),
            ("node-b", "127.0.0.1:2", NodeState::Joining),
            ("node-c", "127.0.0.1:3", NodeState::Joined),
        ] {
            lock(&registry).insert(
                name.to_string(),
                NodeInfo::new(address.to_string(), state, format!("tk-{name}")),
            );
        }
        let _node_a_alive = keep_heartbeating(&registry, "node-a");
        let current_join: CurrentJoin =
            Arc::new(Mutex::new(Some(timed_out_join_of_node_b_awaiting_node_a())));

        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let sweep_task = tokio::spawn(sweep_expired(
            Arc::clone(&registry),
            Arc::clone(&current_join),
            None,
            None,
            2,
            Instant::now(),
            Duration::from_millis(600),
            shutdown_rx,
        ));

        wait_until("J1's X to reach node-a", Duration::from_secs(3), || {
            accepted.load(Ordering::SeqCst) == 1 && lock_current_join(&current_join).is_none()
        })
        .await;
        lock(&registry).insert(
            "node-d".to_string(),
            NodeInfo::new(
                "127.0.0.1:4".to_string(),
                NodeState::Joining,
                "tk-node-d".to_string(),
            ),
        );
        *lock_current_join(&current_join) = Some(PendingJoin {
            joining_name: "node-d".to_string(),
            expected: [("node-c".to_string(), "tk-node-c".to_string())]
                .into_iter()
                .collect(),
            completed: HashSet::new(),
            started_at: Instant::now(),
            max_entries: 0,
            roster: Arc::new(Vec::new()),
            late: LateHandoffs::default(),
            generation: 2,
        });

        // node-c is evicted while J1's X is still outstanding...
        wait_until("node-c's eviction", Duration::from_secs(4), || {
            !lock(&registry).contains_key("node-c")
        })
        .await;
        // ...and J2's abandon is held back behind that fan-out.
        assert!(
            lock_current_join(&current_join).is_some(),
            "J2 was abandoned while the previous fan-out was still running"
        );

        // Once J1's fan-out is done, J2 is abandoned after all.
        wait_until("J2's abandon", Duration::from_secs(5), || {
            lock_current_join(&current_join).is_none()
        })
        .await;
        assert!(!lock(&registry).contains_key("node-d"));

        shutdown_tx.send_replace(true);
        sweep_task.await.unwrap();
    }

    #[tokio::test(flavor = "current_thread")]
    async fn shutdown_lets_an_in_flight_fan_out_finish_instead_of_aborting_it() {
        // Awaited inline, a fan-out used to finish before the sweep could
        // even observe the shutdown. On a `JoinSet`, dropping it on return
        // would abort the `X` mid-exchange; the sweep drains it first.
        let (node_a_addr, accepted, acked) = slow_cancel_node(Duration::from_millis(800)).await;
        let registry: Registry = Arc::new(RegistryState::default());
        for (name, address, state) in [
            ("node-a", node_a_addr.as_str(), NodeState::Joined),
            ("node-b", "127.0.0.1:2", NodeState::Joining),
        ] {
            lock(&registry).insert(
                name.to_string(),
                NodeInfo::new(address.to_string(), state, format!("tk-{name}")),
            );
        }
        let _node_a_alive = keep_heartbeating(&registry, "node-a");
        let current_join: CurrentJoin =
            Arc::new(Mutex::new(Some(timed_out_join_of_node_b_awaiting_node_a())));

        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let sweep_task = tokio::spawn(sweep_expired(
            Arc::clone(&registry),
            current_join,
            None,
            None,
            2,
            Instant::now(),
            Duration::from_secs(60),
            shutdown_rx,
        ));

        wait_until(
            "the abandon's X to reach node-a",
            Duration::from_secs(3),
            || accepted.load(Ordering::SeqCst) == 1,
        )
        .await;
        assert_eq!(acked.load(Ordering::SeqCst), 0, "the X is still in flight");

        shutdown_tx.send_replace(true);
        tokio::time::timeout(Duration::from_secs(5), sweep_task)
            .await
            .expect("the sweep did not stop after draining its fan-out")
            .unwrap();
        assert_eq!(
            acked.load(Ordering::SeqCst),
            1,
            "the in-flight X was cut off by the shutdown"
        );
    }

    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn a_join_that_never_completes_is_abandoned_after_the_migration_timeout() {
        let registry: Registry = Arc::new(RegistryState::default());

        lock(&registry).insert(
            "node-a".to_string(),
            NodeInfo::new(
                "127.0.0.1:1".to_string(),
                NodeState::Joined,
                "tk-node-a".to_string(),
            ),
        );
        lock(&registry).insert(
            "node-b".to_string(),
            NodeInfo::new(
                "127.0.0.1:2".to_string(),
                NodeState::Joining,
                "tk-node-b".to_string(),
            ),
        );

        let current_join: CurrentJoin = Arc::new(Mutex::new(Some(PendingJoin {
            joining_name: "node-b".to_string(),
            expected: [("node-a".to_string(), "tk-node-a".to_string())]
                .into_iter()
                .collect(),
            completed: HashSet::new(),
            started_at: Instant::now(),
            max_entries: 0,
            roster: Arc::new(Vec::new()),
            late: LateHandoffs::default(),
            generation: 1,
        })));

        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let sweep_task = tokio::spawn(sweep_expired(
            Arc::clone(&registry),
            Arc::clone(&current_join),
            None,
            None,
            2,
            Instant::now(),
            Duration::from_secs(60),
            shutdown_rx,
        ));

        tokio::task::yield_now().await;
        // In steps: one jump across the timeout looks like a pause of
        // discovery itself, which the timeout does not count (issue #1617).
        advance_in_steps(
            MIGRATION_TIMEOUT_BASE + Duration::from_secs(1),
            Duration::from_secs(1),
        )
        .await;

        assert!(lock_current_join(&current_join).is_none());
        assert!(!lock(&registry).contains_key("node-b"));

        shutdown_tx.send_replace(true);
        sweep_task.await.unwrap();
    }

    /// Issue #1617: the stall guard gives every node a fresh liveness window
    /// after discovery itself did not run, but the migration timeout runs on
    /// `started_at`, which the pause counts against too. A join that had
    /// made progress was abandoned on the first tick after a pause longer
    /// than the timeout, though no node had been given the time to answer.
    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn a_pause_of_discovery_does_not_count_against_the_migration_timeout() {
        let registry: Registry = Arc::new(RegistryState::default());
        lock(&registry).insert(
            "node-a".to_string(),
            NodeInfo::new(
                "127.0.0.1:1".to_string(),
                NodeState::Joined,
                "tk-node-a".to_string(),
            ),
        );
        lock(&registry).insert(
            "node-b".to_string(),
            NodeInfo::new(
                "127.0.0.1:2".to_string(),
                NodeState::Joining,
                "tk-node-b".to_string(),
            ),
        );
        let current_join: CurrentJoin = Arc::new(Mutex::new(Some(PendingJoin {
            joining_name: "node-b".to_string(),
            expected: [("node-a".to_string(), "tk-node-a".to_string())]
                .into_iter()
                .collect(),
            completed: HashSet::new(),
            started_at: Instant::now(),
            max_entries: 0,
            roster: Arc::new(Vec::new()),
            late: LateHandoffs::default(),
            generation: 1,
        })));
        let liveness = Duration::from_secs(60);
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let sweep_task = tokio::spawn(sweep_expired(
            Arc::clone(&registry),
            Arc::clone(&current_join),
            None,
            None,
            2,
            Instant::now(),
            liveness,
            shutdown_rx,
        ));
        tokio::task::yield_now().await;

        // The join runs for 10 s with the sweep ticking.
        advance_in_steps(Duration::from_secs(10), Duration::from_secs(1)).await;
        assert!(lock_current_join(&current_join).is_some());

        // Discovery stops for longer than the migration timeout and the
        // liveness window; the node's heartbeat gets a fresh window.
        tokio::time::advance(MIGRATION_TIMEOUT_BASE + Duration::from_secs(10)).await;
        for _ in 0..5 {
            tokio::task::yield_now().await;
        }
        assert!(
            lock_current_join(&current_join).is_some(),
            "the pause was counted against the migration timeout"
        );
        assert!(lock(&registry).contains_key("node-b"));

        // The timeout still runs: it is reached on the time discovery was
        // running for.
        advance_in_steps(
            MIGRATION_TIMEOUT_BASE - Duration::from_secs(5),
            Duration::from_secs(1),
        )
        .await;
        assert!(lock_current_join(&current_join).is_none());
        assert!(!lock(&registry).contains_key("node-b"));

        shutdown_tx.send_replace(true);
        sweep_task.await.unwrap();
    }

    // Size-derived migration timeout: the whole point of the size-derived timeout — a
    // join moving a lot of data must not be abandoned just for taking
    // longer than the old flat default would have allowed.
    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn a_join_with_many_entries_is_not_abandoned_at_the_base_timeout() {
        let registry: Registry = Arc::new(RegistryState::default());

        lock(&registry).insert(
            "node-a".to_string(),
            NodeInfo::new(
                "127.0.0.1:1".to_string(),
                NodeState::Joined,
                "tk-node-a".to_string(),
            ),
        );
        lock(&registry).insert(
            "node-b".to_string(),
            NodeInfo::new(
                "127.0.0.1:2".to_string(),
                NodeState::Joining,
                "tk-node-b".to_string(),
            ),
        );

        let current_join: CurrentJoin = Arc::new(Mutex::new(Some(PendingJoin {
            joining_name: "node-b".to_string(),
            expected: [("node-a".to_string(), "tk-node-a".to_string())]
                .into_iter()
                .collect(),
            completed: HashSet::new(),
            started_at: Instant::now(),
            max_entries: 100_000,
            roster: Arc::new(Vec::new()),
            late: LateHandoffs::default(),
            generation: 1,
        })));

        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let sweep_task = tokio::spawn(sweep_expired(
            Arc::clone(&registry),
            Arc::clone(&current_join),
            None,
            None,
            2,
            Instant::now(),
            // Well above the advance below, so node-a's own liveness
            // eviction (and the mid-join-eviction abandon it would now
            // trigger, issue #34) doesn't confound what this test is
            // actually about: the size-derived *migration* timeout, not
            // the liveness one.
            Duration::from_secs(600),
            shutdown_rx,
        ));

        tokio::task::yield_now().await;
        // Past the old flat default, but nowhere near
        // migration_timeout_for(100_000) — still in progress.
        tokio::time::advance(MIGRATION_TIMEOUT_BASE + Duration::from_secs(1)).await;
        tokio::task::yield_now().await;

        assert!(lock_current_join(&current_join).is_some());
        assert!(lock(&registry).contains_key("node-b"));

        shutdown_tx.send_replace(true);
        sweep_task.await.unwrap();
    }

    #[test]
    fn sweep_respawn_backoff_doubles_to_a_cap_and_starts_over_after_a_healthy_run() {
        let mut backoff = SweepRespawnBackoff::default();
        let quick = Duration::ZERO;
        let delays: Vec<u64> = (0..8)
            .map(|_| backoff.next_delay(quick).as_secs())
            .collect();
        assert_eq!(delays, [1, 2, 4, 8, 16, 30, 30, 30]);
        // A run that stayed up long enough forgives the failures before it.
        assert_eq!(
            backoff.next_delay(SWEEP_RESPAWN_HEALTHY_AFTER),
            SWEEP_RESPAWN_BACKOFF_BASE
        );
        assert_eq!(backoff.next_delay(quick), 2 * SWEEP_RESPAWN_BACKOFF_BASE);
        // A run just under that does not.
        assert_eq!(
            backoff.next_delay(SWEEP_RESPAWN_HEALTHY_AFTER - Duration::from_millis(1)),
            4 * SWEEP_RESPAWN_BACKOFF_BASE
        );
        // Many consecutive failures neither overflow nor exceed the cap.
        for _ in 0..1_000 {
            assert!(backoff.next_delay(quick) <= SWEEP_RESPAWN_BACKOFF_MAX);
        }
        assert_eq!(backoff.next_delay(quick), SWEEP_RESPAWN_BACKOFF_MAX);
    }

    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn a_sweep_that_panics_every_time_is_not_respawned_faster_than_the_back_off() {
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let runs = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let supervisor = tokio::spawn(supervise_sweep(shutdown_rx, {
            let runs = Arc::clone(&runs);
            move || {
                let runs = Arc::clone(&runs);
                async move {
                    runs.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                    panic!("sweep panics on every pass");
                }
            }
        }));
        let runs_so_far = || runs.load(std::sync::atomic::Ordering::SeqCst);
        let advance = |seconds: u64| async move {
            // In steps, letting the spawned tasks run between them.
            for _ in 0..seconds * 10 {
                tokio::time::advance(Duration::from_millis(100)).await;
                for _ in 0..8 {
                    tokio::task::yield_now().await;
                }
            }
        };

        // Without a back-off this is thousands of runs a second.
        for _ in 0..8 {
            tokio::task::yield_now().await;
        }
        assert_eq!(runs_so_far(), 1, "the first run starts at once");
        // Runs start at 0 s, then 1 s, 3 s, 7 s and 15 s after the first.
        advance(10).await;
        assert_eq!(runs_so_far(), 4, "after 10 s");
        advance(6).await;
        assert_eq!(runs_so_far(), 5, "after 16 s");
        // At the cap, one run every 30 s.
        advance(30).await;
        assert_eq!(runs_so_far(), 6, "after 46 s");

        // Shutdown ends the supervisor at once, from inside a back-off pause.
        shutdown_tx.send_replace(true);
        timeout(Duration::from_secs(1), supervisor)
            .await
            .expect("the supervisor stops at shutdown")
            .unwrap();
        assert_eq!(runs_so_far(), 6, "no run starts after shutdown");
    }

    #[test]
    fn liveness_timeout_is_bounded_below_and_above() {
        let parse = |secs: &str| {
            parse_args_from(["--liveness-timeout".to_string(), secs.to_string()].into_iter())
        };
        // Below three node heartbeats (15 s): `6` and `14` used to be accepted
        // and let one lost heartbeat, or a pause of discovery under the stall
        // guard, evict live nodes.
        for rejected in ["0", "5", "6", "10", "14", "604801", "18446744073709551615"] {
            match parse(rejected) {
                Err(ArgsError::Invalid(message)) => assert!(
                    message.starts_with("--liveness-timeout must be between 15 and 604800 seconds"),
                    "{rejected}: {message}"
                ),
                Ok(_) => panic!("{rejected} must be refused"),
                Err(ArgsError::Help(_)) => {
                    panic!("{rejected} must be refused, not answered with help")
                }
            }
        }
        assert_eq!(
            parse("15").unwrap().liveness_timeout,
            Duration::from_secs(15)
        );
        assert_eq!(
            parse("604800").unwrap().liveness_timeout,
            Duration::from_secs(604_800)
        );
        assert_eq!(
            parse_args_from(std::iter::empty())
                .unwrap()
                .liveness_timeout,
            DEFAULT_LIVENESS_TIMEOUT
        );
    }

    #[test]
    fn node_heartbeat_interval_matches_the_node_binary() {
        let node_main = include_str!("../main.rs");
        let declared =
            format!("const DEFAULT_HEARTBEAT_INTERVAL_SECS: u64 = {NODE_HEARTBEAT_INTERVAL_SECS};");
        assert!(
            node_main.contains(&declared),
            "src/main.rs no longer declares `{declared}`; update NODE_HEARTBEAT_INTERVAL_SECS"
        );
        // The accepted minimum is derived from it, and the default is not
        // below it.
        assert_eq!(
            MIN_LIVENESS_TIMEOUT_SECS,
            MIN_LIVENESS_HEARTBEATS * NODE_HEARTBEAT_INTERVAL_SECS
        );
        assert!(DEFAULT_LIVENESS_TIMEOUT.as_secs() >= MIN_LIVENESS_TIMEOUT_SECS);
    }
}
