//! Standalone cluster-membership registry for nanocached cache nodes.
//!
//! This binary has no dependency on the cache server's own modules —
//! `nanocached-node` and `nanocached-discovery` share no modules by
//! design (size-derived migration timeout); its protocol is unrelated to nanocached's
//! cache protocol, so nothing is shared. Run it via `ncd discovery start`,
//! or directly as `nanocached-discovery`.
//!
//! Protocol (ASCII header line, terminated by `\n`; a command may repeat
//! on the same connection):
//!
//!   H <name-length> <r> <token-length>\n<name><token>   Heartbeat,
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
//!                             factor.
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
//!   P <name-length> <port> <token-length>\n<name><token>   Announce
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
//!
//!   C <name-length> <joining-length> <token-length>\n<name><joining><token>
//!                             Sent by an already-`Joined` node to report
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
//!   A <secret-length>\n<secret>   Authenticate. Response: `Od\n` on success,
//!                             `Ed\n` (then the connection closes) if a
//!                             secret is configured and this doesn't match
//!                             it. If no secret is configured (the
//!                             `NANOCACHED_AUTH_SECRET` environment
//!                             variable is unset or empty), this is a
//!                             no-op that always succeeds. If a secret is
//!                             configured, every other command is rejected
//!                             with `Ed\n` until a matching `A` has been
//!                             sent on the connection. The `d` distinguishes
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
//! a ring from the partial list. All other commands work during the grace
//! — recovery itself depends on them.
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
use rustc_hash::FxHashMap;
use rustls::pki_types::{CertificateDer, PrivateKeyDer, ServerName};
use rustls::{ClientConfig, RootCertStore, ServerConfig};
use std::collections::HashMap;
use std::collections::HashSet;
use std::io;
use std::io::BufReader;
use std::net::SocketAddr;
use std::pin::Pin;
use std::process::ExitCode;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use std::time::Duration;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, ReadBuf};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{Notify, Semaphore, watch};
use tokio::task::JoinSet;
use tokio::time::{Instant, interval, timeout, timeout_at};
use tokio_rustls::{TlsAcceptor, TlsConnector};

const READ_CHUNK_SIZE: usize = 256;
const MAX_REQUEST_SIZE: usize = 4096;
const MAX_CONNECTIONS: usize = 1024;
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
/// Per-source-IP cooldown between *new* registry insertions via `P`
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
/// Bounds `AnnounceLimiter`'s own memory: without this, an attacker who
/// cycles through source IPs (trivial to spoof/rotate, unlike holding
/// open `MAX_CONNECTIONS` real connections) could grow the limiter map
/// itself without bound, defeating the very thing it exists to bound.
/// Far above any realistic number of distinct legitimate source
/// addresses in one cluster's network.
const MAX_ANNOUNCE_LIMITER_ENTRIES: usize = 4096;
const DEFAULT_LIVENESS_TIMEOUT: Duration = Duration::from_secs(15);
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
/// Backoff applied after an `accept()` failure that looks like file-
/// descriptor exhaustion (EMFILE/ENFILE, see `is_fd_exhaustion_error`) —
/// issue: `listener.accept()`'s error used to be propagated with `?`,
/// killing the whole process on what is, for this and every other
/// recoverable accept() error (ECONNABORTED, ENOBUFS, ...), a transient
/// condition. Retrying immediately under fd exhaustion would just spin
/// the accept loop hot instead of giving descriptors a chance to free up;
/// short enough not to meaningfully delay recovery once they do.
const ACCEPT_ERROR_BACKOFF: Duration = Duration::from_millis(100);
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
/// parse time (`parse_two_string_fields`/`parse_three_string_fields`).
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
    /// its liveness/handoff reports. Never sent back out: `L` and `M`
    /// deliberately carry no tokens, or any node/client could
    /// impersonate any other. Compared via `constant_time_eq`.
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

/// The node registry (`nodes`, keyed by name — node identity decoupled
/// from address's random per-process node identity, not address; see
/// `NodeInfo::address`) plus a `generation` counter (issue #95) bumped
/// on every change that would alter the heartbeat-ack roster — a node
/// joining/leaving, an address change, or a `reported_replication`
/// update. `build_heartbeat_ack` re-serializes the whole `Joined` roster,
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
    /// Issue #124: joins completed (nodes promoted to `Joined`) and
    /// joins abandoned, for the metrics endpoint.
    joins_total: AtomicU64,
    joins_abandoned_total: AtomicU64,
    generation: AtomicU64,
    /// The heartbeat-ack roster, cached and rebuilt only when `generation`
    /// moves (issue #95). Co-located with the map and counter it derives
    /// from, so it travels wherever the registry does — no plumbing
    /// through `ConnectionConfig`/`handle_connection`.
    heartbeat_ack: Mutex<Option<CachedAck>>,
    /// The `L` response, cached the same way and keyed off the same
    /// `generation` (issue #298): `L` renders exactly the same
    /// `roster_snapshot` (the `Joined` set, addresses, and
    /// `reported_replication` votes) that `cached_heartbeat_ack` already
    /// invalidates on every relevant mutation, so no separate bump is
    /// needed here — see `cached_list_response`.
    list_cache: Mutex<Option<CachedList>>,
}

impl Default for RegistryState {
    fn default() -> Self {
        RegistryState {
            nodes: Mutex::new(FxHashMap::default()),
            proxies: Mutex::new(FxHashMap::default()),
            joins_total: AtomicU64::new(0),
            joins_abandoned_total: AtomicU64::new(0),
            generation: AtomicU64::new(0),
            heartbeat_ack: Mutex::new(None),
            list_cache: Mutex::new(None),
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
fn bump_roster(registry: &Registry) {
    registry.generation.fetch_add(1, Ordering::Relaxed);
}

/// The cached heartbeat-ack serialization and the `generation` it was
/// built at (issue #95). Held in `RegistryState::heartbeat_ack`, one per
/// discovery process; the `refuse` decision is folded into `ack` (the
/// `list_ready_at` startup grace is not — it's time-gated and handled by
/// the caller), so a cache hit needs only a generation comparison.
/// `Arc<[u8]>` so a hit hands out the shared buffer without recopying it
/// per heartbeat.
struct CachedAck {
    generation: u64,
    replication: usize,
    ack: Arc<[u8]>,
}

/// The cached `L` response and the `generation` it was built at (issue
/// #298) — `CachedAck`'s sibling for `RegistryState::list_cache`. `L`
/// renders `roster_snapshot`'s `Joined` set/addresses and the
/// `reported_replication` vote tally, exactly what `generation` already
/// tracks for `CachedAck`, so the two share one invalidation signal. The
/// refuse decision (`B\n`) is folded into `response`, same as `CachedAck`
/// folds its withheld-roster case into `ack`; the `list_ready_at` startup
/// grace is time-, not membership-gated, so it stays outside the cache and
/// is handled by the caller.
struct CachedList {
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
struct PendingJoin {
    joining_name: String,
    expected: HashMap<String, String>,
    completed: HashSet<String>,
    started_at: Instant,
    max_entries: usize,
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

/// Wraps either a plain TCP connection or one wrapped in TLS behind a
/// single type, so the rest of the connection-handling code doesn't need to
/// know which is in play. Generic over the plain (`P`) and TLS (`T`) stream
/// types since this process both accepts connections (`ServerStream`, TLS
/// terminated by `TlsAcceptor`) and, since staged node join added `M`/`X`, also
/// opens its own outbound ones to nodes (`ClientStream`, TLS via
/// `TlsConnector`) — the two use different `tokio_rustls` stream types.
enum MaybeTls<P, T> {
    Plain(P),
    Tls(Box<T>),
}

impl<P: AsyncRead + Unpin, T: AsyncRead + Unpin> AsyncRead for MaybeTls<P, T> {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        match self.get_mut() {
            MaybeTls::Plain(stream) => Pin::new(stream).poll_read(cx, buf),
            MaybeTls::Tls(stream) => Pin::new(stream.as_mut()).poll_read(cx, buf),
        }
    }
}

impl<P: AsyncWrite + Unpin, T: AsyncWrite + Unpin> AsyncWrite for MaybeTls<P, T> {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        match self.get_mut() {
            MaybeTls::Plain(stream) => Pin::new(stream).poll_write(cx, buf),
            MaybeTls::Tls(stream) => Pin::new(stream.as_mut()).poll_write(cx, buf),
        }
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match self.get_mut() {
            MaybeTls::Plain(stream) => Pin::new(stream).poll_flush(cx),
            MaybeTls::Tls(stream) => Pin::new(stream.as_mut()).poll_flush(cx),
        }
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match self.get_mut() {
            MaybeTls::Plain(stream) => Pin::new(stream).poll_shutdown(cx),
            MaybeTls::Tls(stream) => Pin::new(stream.as_mut()).poll_shutdown(cx),
        }
    }
}

/// A connection this process accepted, plaintext or TLS-terminated.
type ServerStream = MaybeTls<TcpStream, tokio_rustls::server::TlsStream<TcpStream>>;
/// A connection this process opened outbound (to a node, sending `M`/`X`),
/// plaintext or TLS-secured.
type ClientStream = MaybeTls<TcpStream, tokio_rustls::client::TlsStream<TcpStream>>;

/// Loads a certificate chain and private key from PEM files and builds a
/// `TlsAcceptor` for terminating incoming TLS connections.
fn load_tls_acceptor(cert_path: &str, key_path: &str) -> io::Result<TlsAcceptor> {
    let certs = load_cert_chain(cert_path)?;
    let key = load_private_key(key_path)?;

    let config = ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(certs, key)
        .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;

    Ok(TlsAcceptor::from(Arc::new(config)))
}

/// Loads CA certificates from a PEM file and builds a `TlsConnector` that
/// trusts only those CAs (not the system trust store), for this process's
/// own outbound connections to a node's TLS-secured port (sending `M`/`X`).
fn load_tls_connector(ca_path: &str) -> io::Result<TlsConnector> {
    let certs = load_cert_chain(ca_path)?;
    let mut roots = RootCertStore::empty();

    for cert in certs {
        roots
            .add(cert)
            .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error.to_string()))?;
    }

    let config = ClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth();

    Ok(TlsConnector::from(Arc::new(config)))
}

fn load_cert_chain(path: &str) -> io::Result<Vec<CertificateDer<'static>>> {
    let file = std::fs::File::open(path)?;
    rustls_pemfile::certs(&mut BufReader::new(file)).collect()
}

fn load_private_key(path: &str) -> io::Result<PrivateKeyDer<'static>> {
    let file = std::fs::File::open(path)?;
    rustls_pemfile::private_key(&mut BufReader::new(file))?.ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            format!("no private key found in {path}"),
        )
    })
}

/// Parses the host portion of a `host:port` address into a TLS server name
/// for certificate verification, accepting either a DNS name or IP address.
/// A bracketed IPv6 host (`[::1]:8356`, required so the port's `:` is
/// unambiguous) has its brackets stripped before conversion — left in,
/// `ServerName::try_from` rejects the string both as an IP (brackets
/// aren't part of the address) and as a DNS name (`[`/`]` aren't valid
/// there either), so TLS to an IPv6 address would otherwise always fail.
/// Mirrors `nanocached-node`'s own copy in `src/server.rs`.
fn server_name_from_addr(addr: &str) -> io::Result<ServerName<'static>> {
    let host = addr.rsplit_once(':').map_or(addr, |(host, _)| host);
    let host = host
        .strip_prefix('[')
        .and_then(|host| host.strip_suffix(']'))
        .unwrap_or(host);

    ServerName::try_from(host.to_string()).map_err(|error| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("invalid TLS server name {host:?}: {error}"),
        )
    })
}

/// Connects to `addr` (a node, to send `M`/`X`), upgrading to TLS first if
/// `tls_connector` is set. There is no plaintext fallback: if TLS is
/// configured and the handshake fails, the connection attempt fails too —
/// mirrors `nanocached-node`'s own `connect_client_stream`.
async fn connect_client_stream(
    addr: &str,
    tls_connector: Option<&TlsConnector>,
) -> io::Result<ClientStream> {
    let stream = timeout(OUTBOUND_IO_TIMEOUT, TcpStream::connect(addr))
        .await
        .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "connect timed out"))??;
    let _ = stream.set_nodelay(true);

    match tls_connector {
        Some(connector) => {
            let server_name = server_name_from_addr(addr)?;
            let tls_stream = timeout(
                TLS_HANDSHAKE_TIMEOUT,
                connector.connect(server_name, stream),
            )
            .await
            .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "TLS handshake timed out"))??;

            Ok(ClientStream::Tls(Box::new(tls_stream)))
        }
        None => Ok(ClientStream::Plain(stream)),
    }
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
    /// Guards new (not refreshing) registry insertions via `P` — see
    /// `AnnounceLimiter`.
    announce_limiter: AnnounceLimiter,
}

/// Reads the shared auth secret from the environment rather than a CLI
/// flag, since CLI arguments are visible to anyone who can list processes
/// (e.g. `ps`) on the host. An unset or empty value means auth is not
/// required.
fn read_auth_secret() -> Option<Bytes> {
    std::env::var(AUTH_SECRET_ENV_VAR)
        .ok()
        .filter(|secret| !secret.is_empty())
        .map(Bytes::from)
}

/// Compares two byte strings without leaking, via timing, how many leading
/// bytes matched. Length differs openly (no secret ever has a length worth
/// hiding), but once lengths match, every byte is compared regardless of
/// earlier mismatches.
fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }

    let mut diff: u8 = 0;
    for (x, y) in a.iter().zip(b.iter()) {
        diff |= x ^ y;
    }

    diff == 0
}

/// Per-source-IP cooldown state guarding new registry insertions via `P`
/// (see `ANNOUNCE_INSERT_COOLDOWN`). Bounded itself
/// (`MAX_ANNOUNCE_LIMITER_ENTRIES`, oldest entry evicted to make room) so
/// an attacker cycling through source addresses can't grow the limiter's
/// own memory without bound — the same shape of problem this exists to
/// solve for the registry, one level down.
type AnnounceLimiter = Arc<Mutex<FxHashMap<std::net::IpAddr, Instant>>>;

/// Returns whether a new registry insertion from `peer_ip` is allowed
/// right now, recording `peer_ip` against the current time if so.
/// `peer_ip` is allowed unless it has a recorded insertion within
/// `ANNOUNCE_INSERT_COOLDOWN`. Only meant to be called for a genuinely new
/// name (see the `P` handler) — a refresh of an existing entry must not
/// consume or be gated by this limiter at all.
fn announce_insert_allowed(limiter: &AnnounceLimiter, peer_ip: std::net::IpAddr) -> bool {
    let now = Instant::now();
    let mut guard = limiter
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());

    if let Some(last) = guard.get(&peer_ip)
        && now.duration_since(*last) < ANNOUNCE_INSERT_COOLDOWN
    {
        return false;
    }

    if guard.len() >= MAX_ANNOUNCE_LIMITER_ENTRIES && !guard.contains_key(&peer_ip) {
        // Evict the single oldest entry to make room. A linear scan, but
        // this only runs when the (small, bounded) limiter is already
        // full and a genuinely new source shows up — not on the common
        // path of a source that's already tracked.
        if let Some(oldest) = guard
            .iter()
            .min_by_key(|&(_, recorded_at)| *recorded_at)
            .map(|(ip, _)| *ip)
        {
            guard.remove(&oldest);
        }
    }

    guard.insert(peer_ip, now);
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
            eprintln!(
                "WARN rejected announce for {name} from {peer_ip}: wrong token — either \
                 an impersonation attempt (issue #34) or a node reusing another's name"
            );
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
            info.last_heartbeat = Instant::now();
            Some(Ok(address_changed))
        }
        None => None,
    }
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
    let mut args = Args::default();
    let mut raw = std::env::args().skip(1);

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
                                 a heartbeat (default 15)
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
    Complete {
        name: String,
        joining_name: String,
        token: String,
    },
    /// Discovery HA: an already-promoted node (re-)declaring membership, with
    /// the same name/port/token shape as `Join` — upserted straight to
    /// `Joined`, no handoff orchestration. `token` must match a
    /// registered name's stored one (issue #34).
    Announce {
        name: String,
        port: u16,
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
            let (name, token) =
                parse_two_string_fields(input, header_end, name_length, token_length)?;

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
            let (name, token) =
                parse_two_string_fields(input, header_end, name_length, token_length)?;

            Ok(DiscoveryCommand::ProxyDeregister { name, token })
        }

        b"Q" => {
            if parts.next().is_some() {
                return Err(ParseError::InvalidLength);
            }

            let _ = input.split_to(header_end + 1);
            Ok(DiscoveryCommand::ListProxies)
        }

        b"H" => {
            let name_length = parts.next().ok_or(ParseError::InvalidLength)?;
            let replication = parts.next().ok_or(ParseError::InvalidLength)?;
            let token_length = parts.next().ok_or(ParseError::InvalidLength)?;

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
            let (name, token) =
                parse_two_string_fields(input, header_end, name_length, token_length)?;

            Ok(DiscoveryCommand::Heartbeat {
                name,
                replication,
                token,
            })
        }

        b"C" => {
            let name_length = parts.next().ok_or(ParseError::InvalidLength)?;
            let joining_length = parts.next().ok_or(ParseError::InvalidLength)?;
            let token_length = parts.next().ok_or(ParseError::InvalidLength)?;

            if parts.next().is_some() {
                return Err(ParseError::InvalidLength);
            }

            let name_length = parse_length(name_length)?;
            let joining_length = parse_length(joining_length)?;
            let token_length = parse_length(token_length)?;
            let (name, joining_name, token) = parse_three_string_fields(
                input,
                header_end,
                name_length,
                joining_length,
                token_length,
            )?;

            Ok(DiscoveryCommand::Complete {
                name,
                joining_name,
                token,
            })
        }

        b"J" | b"P" | b"Y" => {
            let name_length = parts.next().ok_or(ParseError::InvalidLength)?;
            let port = parts.next().ok_or(ParseError::InvalidLength)?;
            let token_length = parts.next().ok_or(ParseError::InvalidLength)?;

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
            let make: fn(String, u16, String) -> DiscoveryCommand = match command {
                b"J" => |name, port, token| DiscoveryCommand::Join { name, port, token },
                b"Y" => |name, port, token| DiscoveryCommand::ProxyAnnounce { name, port, token },
                _ => |name, port, token| DiscoveryCommand::Announce { name, port, token },
            };
            let (name, token) =
                parse_two_string_fields(input, header_end, name_length, token_length)?;

            Ok(make(name, port, token))
        }

        _ => Err(ParseError::InvalidCommand),
    }
}

/// Parses two consecutive length-prefixed fields (`H`/`J`/`P`'s name then
/// token — issue #34), checking both are fully buffered before consuming
/// any of `input`, so `parse`'s "untouched on `Incomplete`" contract
/// holds even though this reads across two fields in one call. Bounds
/// each field to `MAX_NAME_LENGTH`/`MAX_TOKEN_LENGTH` — see those
/// constants' own doc comments for why.
fn parse_two_string_fields(
    input: &mut BytesMut,
    header_end: usize,
    first_length: usize,
    second_length: usize,
) -> Result<(String, String), ParseError> {
    if first_length == 0 || second_length == 0 {
        return Err(ParseError::EmptyField);
    }
    if first_length > MAX_NAME_LENGTH || second_length > MAX_TOKEN_LENGTH {
        return Err(ParseError::InvalidLength);
    }

    let first_start = header_end + 1;
    let first_end = first_start
        .checked_add(first_length)
        .ok_or(ParseError::InvalidLength)?;
    let second_end = first_end
        .checked_add(second_length)
        .ok_or(ParseError::InvalidLength)?;

    if input.len() < second_end {
        return Err(ParseError::Incomplete);
    }

    let frame = input.split_to(second_end);
    let first = String::from_utf8(frame[first_start..first_end].to_vec())
        .map_err(|_| ParseError::InvalidUtf8)?;
    let second = String::from_utf8(frame[first_end..second_end].to_vec())
        .map_err(|_| ParseError::InvalidUtf8)?;

    // issue #192: both fields end up in server logs verbatim (name via
    // `node registered`/`node left the cluster`/etc., token never
    // logged today but held to the same bar) — reject control
    // characters here rather than escaping at every print site.
    if contains_control_character(&first) || contains_control_character(&second) {
        return Err(ParseError::ControlCharacter);
    }

    Ok((first, second))
}

/// Parses three consecutive length-prefixed fields (`C`'s name, joining
/// name, then token — issue #34), with the same "untouched on
/// `Incomplete`" contract as `parse_two_string_fields`. Bounds each field
/// to `MAX_NAME_LENGTH`/`MAX_TOKEN_LENGTH`, same as that function.
fn parse_three_string_fields(
    input: &mut BytesMut,
    header_end: usize,
    first_length: usize,
    second_length: usize,
    third_length: usize,
) -> Result<(String, String, String), ParseError> {
    if first_length == 0 || second_length == 0 || third_length == 0 {
        return Err(ParseError::EmptyField);
    }
    if first_length > MAX_NAME_LENGTH
        || second_length > MAX_NAME_LENGTH
        || third_length > MAX_TOKEN_LENGTH
    {
        return Err(ParseError::InvalidLength);
    }

    let first_start = header_end + 1;
    let first_end = first_start
        .checked_add(first_length)
        .ok_or(ParseError::InvalidLength)?;
    let second_end = first_end
        .checked_add(second_length)
        .ok_or(ParseError::InvalidLength)?;
    let third_end = second_end
        .checked_add(third_length)
        .ok_or(ParseError::InvalidLength)?;

    if input.len() < third_end {
        return Err(ParseError::Incomplete);
    }

    let frame = input.split_to(third_end);
    let first = String::from_utf8(frame[first_start..first_end].to_vec())
        .map_err(|_| ParseError::InvalidUtf8)?;
    let second = String::from_utf8(frame[first_end..second_end].to_vec())
        .map_err(|_| ParseError::InvalidUtf8)?;
    let third = String::from_utf8(frame[second_end..third_end].to_vec())
        .map_err(|_| ParseError::InvalidUtf8)?;

    // issue #192: `name`/`joining_name` are logged verbatim on handoff
    // completion — same rejection as `parse_two_string_fields`.
    if contains_control_character(&first)
        || contains_control_character(&second)
        || contains_control_character(&third)
    {
        return Err(ParseError::ControlCharacter);
    }

    Ok((first, second, third))
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
/// keep the port internal.
async fn run_metrics_server(listener: TcpListener, registry: Registry, list_ready_at: Instant) {
    loop {
        let stream = match listener.accept().await {
            Ok((stream, _)) => stream,
            Err(error) => {
                // Issue #184: mirrors `run`'s own accept loop above — an
                // unadorned `continue` here would busy-loop this task hot
                // under EMFILE/ENFILE instead of backing off, making
                // recovery harder right when file descriptors are already
                // scarce.
                if is_fd_exhaustion_error(&error) {
                    tokio::time::sleep(ACCEPT_ERROR_BACKOFF).await;
                }
                continue;
            }
        };
        let registry = Arc::clone(&registry);
        tokio::spawn(async move {
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

/// Bounded read of one HTTP request head; GET path or error. Mirrors the
/// node's copy.
async fn read_http_request_path(stream: &mut TcpStream) -> io::Result<String> {
    let mut head = Vec::new();
    let mut chunk = [0u8; 1024];
    while !head.windows(4).any(|window| window == b"\r\n\r\n") {
        if head.len() > 8192 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "oversized http request head",
            ));
        }
        let bytes_read = stream.read(&mut chunk).await?;
        if bytes_read == 0 {
            break;
        }
        head.extend_from_slice(&chunk[..bytes_read]);
    }

    let head = String::from_utf8_lossy(&head);
    let request_line = head.lines().next().unwrap_or_default();
    let mut parts = request_line.split(' ');
    match (parts.next(), parts.next()) {
        (Some("GET"), Some(path)) => Ok(path.to_string()),
        _ => Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "not a GET request",
        )),
    }
}

async fn write_http_response(stream: &mut TcpStream, status: &str, body: &str) -> io::Result<()> {
    let response = format!(
        "HTTP/1.1 {status}\r\nContent-Type: text/plain; version=0.0.4; charset=utf-8\r\n\
         Content-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    );
    stream.write_all(response.as_bytes()).await?;
    stream.shutdown().await
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
    let (name, joining_addr, joined, ready_tokens) = loop {
        let mut join_guard = lock_current_join(current_join);

        if join_guard.is_some() {
            return;
        }

        let (name, joining_addr, joined, ready_tokens) = {
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
                .map(|(name, info)| (name.clone(), info.address.clone()));

            let Some((name, joining_addr)) = next_waiting else {
                return;
            };

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

            (name, joining_addr, joined, ready_tokens)
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

        *join_guard = Some(PendingJoin {
            joining_name: name.clone(),
            expected,
            completed: HashSet::new(),
            started_at: Instant::now(),
            max_entries: 0,
        });

        break (name, joining_addr, joined, ready_tokens);
    };

    println!(
        "INFO join started: {name} (handoff from {} members)",
        joined.len()
    );

    let mut sends = JoinSet::new();

    for (ready_name, ready_addr) in joined.iter().cloned() {
        let auth_secret = auth_secret.clone();
        let tls_connector = tls_connector.clone();
        let joining_name = name.clone();
        let joining_addr = joining_addr.clone();
        let joined_roster = joined.clone();
        // Every `Joined` node in `joined` was captured with its token in the
        // same lock scope, so this lookup is always present.
        let ready_token = ready_tokens.get(&ready_name).cloned().unwrap_or_default();

        sends.spawn(async move {
            let result = send_migrate_with_retry(
                &ready_token,
                &ready_name,
                &ready_addr,
                &auth_secret,
                &tls_connector,
                &joining_name,
                &joining_addr,
                &joined_roster,
                replication,
                OUTBOUND_IO_TIMEOUT,
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
            Ok((_, Ok(entries))) => {
                // Size-derived migration timeout: sizes the migration timeout by the
                // largest handoff any ready node reported — a report for
                // a since-abandoned/replaced join (this one's slot
                // already moved on to a different `joining_name`) must
                // not be credited here, mirroring `handle_complete`'s own
                // guard.
                let mut join_guard = lock_current_join(current_join);
                if let Some(pending) = join_guard.as_mut()
                    && pending.joining_name == name
                {
                    pending.max_entries = pending.max_entries.max(entries);
                }
            }
            Err(error) => eprintln!("WARN a task sending M panicked: {error}"),
        }
    }
}

/// Bounded retry for `M`'s delivery (issue #20 / staged-join handoff design):
/// tries up to `MIGRATE_SEND_ATTEMPTS` times, logging each failed attempt,
/// with a fresh connection every time (`send_migrate` owns its own
/// connect) since a failed write/read leaves the previous connection's
/// state unknown. This only bounds the *send* — the handoff itself,
/// reported back asynchronously via `C`, stays a separate, unretried
/// concern (see `send_migrate`'s own docs).
#[allow(clippy::too_many_arguments)]
async fn send_migrate_with_retry(
    token: &str,
    ready_name: &str,
    address: &str,
    auth_secret: &Option<Bytes>,
    tls_connector: &Option<TlsConnector>,
    joining_name: &str,
    joining_addr: &str,
    joined: &[(String, String)],
    replication: usize,
    io_timeout: Duration,
) -> io::Result<usize> {
    let mut last_error = None;

    for attempt in 1..=MIGRATE_SEND_ATTEMPTS {
        match send_migrate(
            token,
            address,
            auth_secret,
            tls_connector,
            joining_name,
            joining_addr,
            joined,
            replication,
            io_timeout,
        )
        .await
        {
            Ok(entries) => return Ok(entries),
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
/// `M {joining_name_len} {joining_addr_len} {joined.len()} {replication}\n`
/// followed by `joining_name` + `joining_addr`, then one
/// `{name_len} {addr_len}\n{name}{addr}` per entry in `joined`. Factored
/// out of `send_migrate` so `start_join`'s `NODE_MAX_REQUEST_SIZE` check
/// can compute this message's real length (`.len()` on the result) using
/// the actual wire format, rather than a hand-maintained estimate that
/// could silently drift out of sync with it.
fn build_migrate_message(
    token: &str,
    joining_name: &str,
    joining_addr: &str,
    joined: &[(String, String)],
    replication: usize,
) -> Vec<u8> {
    // `token` is the *recipient* ready node's own membership token, echoed
    // so the node can prove this `M` came from a discovery server it
    // registered with (issue #34) — see `Command::Migrate::token` on the
    // node side. Body layout: `<token><joining_name><joining_addr><entries>`.
    let mut message = format!(
        "M {} {} {} {} {}\n",
        joining_name.len(),
        joining_addr.len(),
        joined.len(),
        replication,
        token.len()
    )
    .into_bytes();
    message.extend_from_slice(token.as_bytes());
    message.extend_from_slice(joining_name.as_bytes());
    message.extend_from_slice(joining_addr.as_bytes());

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
    joined: &[(String, String)],
    replication: usize,
    io_timeout: Duration,
) -> io::Result<usize> {
    let mut stream = connect_client_stream(address, tls_connector.as_ref()).await?;

    if let Some(secret) = auth_secret {
        let mut auth = format!("A {}\n", secret.len()).into_bytes();
        auth.extend_from_slice(secret);
        write_all_timed(&mut stream, &auth, io_timeout).await?;

        let mut ack = [0u8; 3];
        read_exact_timed(&mut stream, &mut ack, io_timeout).await?;

        if &ack != b"On\n" {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "node rejected the auth secret",
            ));
        }
    }

    let message = build_migrate_message(token, joining_name, joining_addr, joined, replication);

    write_all_timed(&mut stream, &message, io_timeout).await?;

    let line = read_line_timed(&mut stream, io_timeout).await?;
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

/// `write_all` bounded by `io_timeout`. Without this bound a ready node
/// that accepts the connection but stops draining its receive buffer (a
/// crashed-but-open, blackholed, or malicious peer) would make this write
/// block forever — and because `abandon_current_join`/`try_begin_next_join`
/// await these sends, that would freeze `sweep_expired`, the sole task doing
/// liveness eviction and migration-timeout reaping. Mirrors the read-side
/// bound so the whole `M`/`X` exchange is time-bounded, exactly like
/// `server.rs`'s `OUTBOUND_IO_TIMEOUT` machinery.
async fn write_all_timed(
    stream: &mut ClientStream,
    buf: &[u8],
    io_timeout: Duration,
) -> io::Result<()> {
    timeout(io_timeout, stream.write_all(buf))
        .await
        .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "write timed out"))??;
    Ok(())
}

/// Reads up to (and consuming) the next `\n`, bounded overall by
/// `io_timeout` — used for `M`'s `A <entries>\n` ack, whose length
/// varies with the reported entry count (unlike every other ack on this
/// connection, which is a fixed number of bytes). Bails out past
/// `MAX_ACK_LINE_LENGTH` rather than growing `line` without bound on a
/// desynced or malicious peer.
///
/// The deadline is computed once, before the loop, and every per-byte read
/// is raced against that same `Instant` via `timeout_at` — not against a
/// fresh `io_timeout` window each time. A peer that drips the ack one byte
/// at a time, each byte arriving just under `io_timeout` apart, would
/// otherwise never trip a per-read `timeout()`, turning a bounded read into
/// an effectively unbounded one; because `send_migrate`/`send_cancel` are
/// awaited by `sweep_expired` — the sole task doing liveness eviction and
/// migration-timeout reaping — that would freeze it for as long as the
/// trickle continues.
const MAX_ACK_LINE_LENGTH: usize = 64;

async fn read_line_timed(stream: &mut ClientStream, io_timeout: Duration) -> io::Result<String> {
    let deadline = Instant::now() + io_timeout;
    let mut line = Vec::new();
    let mut byte = [0u8; 1];

    loop {
        timeout_at(deadline, stream.read_exact(&mut byte))
            .await
            .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "ack read timed out"))??;
        if byte[0] == b'\n' {
            break;
        }
        if line.len() >= MAX_ACK_LINE_LENGTH {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "ack line too long",
            ));
        }
        line.push(byte[0]);
    }

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
        write_all_timed(&mut stream, &auth, io_timeout).await?;

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

    write_all_timed(&mut stream, &message, io_timeout).await?;

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
    let promoted = {
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

            let source_ip = address
                .rsplit_once(':')
                .map_or(address.as_str(), |(ip, _)| ip);
            let waiting_from_source = guard
                .values()
                .filter(|info| {
                    info.state != NodeState::Joined
                        && info.address.rsplit_once(':').map(|(ip, _)| ip) == Some(source_ip)
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
        let joined_now: Vec<(String, String)> = guard
            .iter()
            .filter(|(_, info)| info.state == NodeState::Joined)
            .map(|(joined_name, info)| (joined_name.clone(), info.address.clone()))
            .collect();
        // The `M` recipients' tokens are all per-process UUIDs of the same
        // length as this joining node's own `token`, so it stands in here
        // for an accurate size estimate without looking each recipient up.
        let message_len =
            build_migrate_message(&token, name, &address, &joined_now, replication).len();
        if message_len > NODE_MAX_REQUEST_SIZE {
            return Err(JoinRejection::MigrateMessageTooLarge { message_len });
        }

        if !guard.contains_key(name) {
            println!("INFO node registered: {name} at {address} (waiting to join)");
        }
        // Captured once, at registration, for `waiting_timeout_for` — see
        // `NodeInfo::queue_position`'s doc comment for why this must not
        // be recomputed later as the queue drains.
        let queue_position = guard
            .values()
            .filter(|info| info.state != NodeState::Joined)
            .count()
            + 1;
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
        if info.state == NodeState::Waiting {
            info.address = address;
        }
        // Issue #3/#9: unconditionally overwritten on every accepted `J`
        // for this name — including a duplicate reusing an existing
        // Waiting/Joining entry — so this always names the most recently
        // accepted connection as the owner, never a stale one. See
        // `NodeInfo::owner_connection_id`.
        info.owner_connection_id = connection_id;
        Arc::clone(&info.promoted)
    };

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

/// Records a ready node's completion report for the in-progress join. If
/// this was the last of `expected` to report in, promotes the joining
/// node and lets the next `Waiting` node (if any) start.
#[allow(clippy::too_many_arguments)]
async fn handle_complete(
    registry: &Registry,
    current_join: &CurrentJoin,
    auth_secret: &Option<Bytes>,
    tls_connector: &Option<TlsConnector>,
    replication: usize,
    joins_ready_at: Instant,
    reporting_name: &str,
    for_joining_name: &str,
    token: &str,
) {
    let joining_name = {
        let mut join_guard = lock_current_join(current_join);

        let Some(pending) = join_guard.as_mut() else {
            return;
        };

        // Issue #5: a report for an earlier, since-abandoned join must
        // not be credited to this one — the reporter has sent this join's
        // target nothing.
        if pending.joining_name != for_joining_name {
            return;
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
            eprintln!(
                "WARN ignored handoff-complete report from {reporting_name} for \
                 {for_joining_name}: not a ready member of this join (issue #34)"
            );
            return;
        };
        if !constant_time_eq(expected_token.as_bytes(), token.as_bytes()) {
            eprintln!(
                "WARN ignored handoff-complete report from {reporting_name} for \
                 {for_joining_name}: presented token does not match the one recorded \
                 when this join started — either a forged report or a stale one from a \
                 node that re-registered under this name since (issue #34)"
            );
            return;
        }

        pending.completed.insert(reporting_name.to_string());
        println!(
            "INFO handoff completed: {reporting_name} -> {for_joining_name} ({}/{})",
            pending.completed.len(),
            pending.expected.len()
        );

        if pending.completed.len() < pending.expected.len() {
            return;
        }

        let joining_name = pending.joining_name.clone();
        *join_guard = None;
        joining_name
    };

    promote_to_joined(registry, &joining_name);
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
    let is_current_joining_node = owns_entry
        && lock_current_join(current_join)
            .as_ref()
            .is_some_and(|pending| pending.joining_name == name);

    if is_current_joining_node {
        abandon_current_join(
            registry,
            current_join,
            auth_secret,
            tls_connector,
            replication,
            joins_ready_at,
            "joining node disconnected",
        )
        .await;
    }
}

/// Scraps the current join (if any), returning every node it touched to
/// its pre-migration state: clears `current_join`, removes the joining
/// node from the registry (it was never in `L` to begin with, so this is
/// nothing more than dropping bookkeeping), and tells every still-
/// registered ready node in `expected` to roll back via `X` (see
/// `send_cancel`) — a ready node that already finished, or never gets the
/// message, is unaffected. Then lets the next `Waiting` node (if any)
/// start. A no-op if no join is in progress.
async fn abandon_current_join(
    registry: &Registry,
    current_join: &CurrentJoin,
    auth_secret: &Option<Bytes>,
    tls_connector: &Option<TlsConnector>,
    replication: usize,
    joins_ready_at: Instant,
    reason: &str,
) {
    let Some(pending) = lock_current_join(current_join).take() else {
        return;
    };
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
    let stranded = lock(registry).remove(&pending.joining_name);
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

    for (ready_name, ready_addr, ready_token) in ready_addrs {
        let auth_secret = auth_secret.clone();
        let tls_connector = tls_connector.clone();
        let joining_name = pending.joining_name.clone();

        sends.spawn(async move {
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

async fn shutdown_signal() -> io::Result<()> {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{SignalKind, signal};

        let mut terminate = signal(SignalKind::terminate())?;

        tokio::select! {
            result = tokio::signal::ctrl_c() => result,
            _ = terminate.recv() => Ok(()),
        }
    }

    #[cfg(not(unix))]
    {
        tokio::signal::ctrl_c().await
    }
}

/// Whether `error` (from a failed `listener.accept()`) looks like the
/// process (EMFILE) or the whole system (ENFILE) being out of file
/// descriptors — the two accept() failures where retrying immediately
/// would spin the accept loop hot instead of recovering (see
/// `ACCEPT_ERROR_BACKOFF`). EMFILE/ENFILE share the same numeric errno on
/// every Unix this project targets (Linux, macOS/BSD), so this hardcodes
/// them rather than pulling in a `libc` dependency for two integers. Any
/// other accept() error (ECONNABORTED, ENOBUFS, ...) is still logged and
/// retried immediately by the caller — just without the backoff, since
/// those aren't a resource-pressure condition an immediate retry would
/// make worse.
#[cfg(unix)]
fn is_fd_exhaustion_error(error: &io::Error) -> bool {
    const EMFILE: i32 = 24;
    const ENFILE: i32 = 23;
    matches!(error.raw_os_error(), Some(EMFILE) | Some(ENFILE))
}

#[cfg(not(unix))]
fn is_fd_exhaustion_error(_error: &io::Error) -> bool {
    false
}

#[allow(clippy::too_many_arguments)]
async fn run(
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

    // Issue #124: the operations sidecar — /metrics + /healthz +
    // /readyz, mirroring the node's (independent re-implementation per
    // the no-shared-modules policy). /readyz answers 503 during the
    // startup grace, exactly the window where `L`/`Q` answer `B`.
    if let Some(metrics_address) = &metrics_address {
        let metrics_listener = TcpListener::bind(metrics_address.as_str()).await?;
        println!("INFO metrics endpoint listening on {metrics_address}");
        tokio::spawn(run_metrics_server(
            metrics_listener,
            Arc::clone(&cluster_state.registry),
            list_ready_at,
        ));
    }

    let sweep_task = tokio::spawn(sweep_expired(
        Arc::clone(&cluster_state.registry),
        Arc::clone(&cluster_state.current_join),
        auth_secret,
        tls_connector,
        replication,
        list_ready_at,
        liveness_timeout,
        shutdown_rx.clone(),
    ));

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
                        eprintln!("WARN accept failed: {error}");
                        if is_fd_exhaustion_error(&error) {
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

/// Live connection counts per source IP, backing `MAX_CONNECTIONS_PER_IP`
/// (see that constant). Mirrors `src/server.rs`'s own `PerIpConnections`:
/// a plain `Mutex<HashMap<..>>` rather than anything fancier, since every
/// access here is a brief increment/decrement with no I/O under the lock,
/// and every accepted connection already pays for a `Semaphore`
/// acquisition on the shared `connection_limit`, so this adds no
/// bottleneck relative to that existing one.
type PerIpConnections = Arc<Mutex<HashMap<std::net::IpAddr, usize>>>;

/// Releases one `MAX_CONNECTIONS_PER_IP` slot on drop — the per-IP
/// counterpart to the `Semaphore` permit `dispatch_connection` already
/// holds for `MAX_CONNECTIONS` (`_connection_permit`, which frees itself
/// the same way). Mirrors `src/server.rs`'s own `PerIpConnectionGuard`.
struct PerIpConnectionGuard {
    counts: PerIpConnections,
    ip: std::net::IpAddr,
}

impl Drop for PerIpConnectionGuard {
    fn drop(&mut self) {
        let mut counts = self
            .counts
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());

        if let Some(count) = counts.get_mut(&self.ip) {
            *count -= 1;
            if *count == 0 {
                // Don't let a long-lived process accumulate one entry
                // per distinct IP that has ever connected, most of which
                // will never connect again.
                counts.remove(&self.ip);
            }
        }
    }
}

/// Reserves one of `MAX_CONNECTIONS_PER_IP` slots for `ip`, or `None` if
/// it's already at the cap — see `MAX_CONNECTIONS_PER_IP`. Mirrors
/// `src/server.rs`'s own `try_acquire_per_ip`.
fn try_acquire_per_ip(
    counts: &PerIpConnections,
    ip: std::net::IpAddr,
) -> Option<PerIpConnectionGuard> {
    let mut guard = counts
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());

    let count = guard.entry(ip).or_insert(0);
    if *count >= MAX_CONNECTIONS_PER_IP {
        return None;
    }
    *count += 1;
    drop(guard);

    Some(PerIpConnectionGuard {
        counts: Arc::clone(counts),
        ip,
    })
}

/// Best-effort "Busy" reply on `stream` before the caller drops it —
/// shared by every over-limit rejection in `dispatch_connection`
/// (`MAX_CONNECTIONS` and, per source IP, `MAX_CONNECTIONS_PER_IP`).
/// Mirrors `src/server.rs`'s own `reject_over_limit`. A TLS-configured
/// server has no plaintext channel to answer on before the handshake
/// completes (TLS support: no plaintext fallback once TLS is set) — it just
/// closes. A plaintext server can still reply on the raw stream. Bounded
/// by `TLS_HANDSHAKE_TIMEOUT` (reused rather than a new constant: a peer
/// that never reads this reply must not leak the task by leaving the
/// write pending indefinitely — the same reasoning as the handshake
/// itself).
async fn reject_over_limit(
    mut stream: TcpStream,
    address: SocketAddr,
    tls_acceptor: &Option<TlsAcceptor>,
) {
    if tls_acceptor.is_none() {
        match timeout(TLS_HANDSHAKE_TIMEOUT, stream.write_all(b"B\n")).await {
            Ok(Ok(())) => {}
            Ok(Err(error)) => {
                eprintln!("WARN failed to send busy response to {address}: {error}");
            }
            Err(_) => {
                eprintln!("WARN sending busy response to {address} timed out");
            }
        }
    }
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
                reject_over_limit(stream, address, &config.tls_acceptor).await;
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
        let per_ip_permit = match try_acquire_per_ip(&per_ip_connections, address.ip()) {
            Some(permit) => permit,
            None => {
                reject_over_limit(stream, address, &config.tls_acceptor).await;
                return;
            }
        };

        let stream: ServerStream = match &config.tls_acceptor {
            Some(acceptor) => match timeout(TLS_HANDSHAKE_TIMEOUT, acceptor.accept(stream)).await {
                Ok(Ok(tls_stream)) => MaybeTls::Tls(Box::new(tls_stream)),
                Ok(Err(error)) => {
                    eprintln!("WARN TLS handshake with {address} failed: {error}");
                    return;
                }
                Err(_) => {
                    eprintln!("WARN TLS handshake with {address} timed out");
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
                println!("INFO connection from {address} closed without TLS close_notify");
            } else {
                eprintln!("WARN connection error from {address}: {error}");
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

/// The `H` ack carrying a roster (issue #61): `A <count> <replication>\n`
/// then one `<name-len> <addr-len>\n<name><addr>\n` per `Joined` node —
/// the same entry layout `L` uses, so a node parses it the way an SDK
/// parses `L`.
fn build_heartbeat_ack(nodes: &[(String, String)], replication: usize) -> Vec<u8> {
    let mut ack = format!("A {} {}\n", nodes.len(), replication).into_bytes();
    for (name, addr) in nodes {
        ack.extend_from_slice(format!("{} {}\n", name.len(), addr.len()).as_bytes());
        ack.extend_from_slice(name.as_bytes());
        ack.extend_from_slice(addr.as_bytes());
        ack.push(b'\n');
    }
    ack
}

/// The heartbeat-ack roster, cached and rebuilt only when the registry's
/// `generation` has moved since the last build (issue #95). With #61
/// every `H` from every `Joined` node carries the full roster; rebuilding
/// it per heartbeat is O(nodes) scan + serialize under the registry lock,
/// O(nodes²) per liveness cycle. Membership changes far less often than
/// nodes heartbeat, so on the common path this returns the shared cached
/// buffer after one atomic load and an equality check — no registry lock,
/// no re-serialization.
///
/// The withheld-roster cases (`refuse`, a bare `A\n`) are baked into the
/// cached bytes; the startup-grace `A\n` is handled by the caller before
/// this is reached (it's time- not membership-gated, so it must not be
/// cached). Whether the ack was built at exactly the latest generation
/// doesn't matter for correctness — the roster is a convergence aid the
/// next heartbeat refreshes — so the fast-path generation read need not
/// be taken under the registry lock.
fn cached_heartbeat_ack(registry: &Registry, replication: usize) -> Arc<[u8]> {
    let generation = registry.generation.load(Ordering::Relaxed);
    {
        let cached = registry
            .heartbeat_ack
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if let Some(current) = cached.as_ref()
            && current.generation == generation
            && current.replication == replication
        {
            return Arc::clone(&current.ack);
        }
    }

    // Rebuild: read the generation under the registry lock so it matches
    // the roster this snapshot serializes (a concurrent bump lands either
    // fully before or fully after this critical section).
    let snapshot;
    let built_generation;
    {
        let guard = lock(registry);
        built_generation = registry.generation.load(Ordering::Relaxed);
        snapshot = roster_snapshot_locked(&guard, replication);
    }
    // Issue #279: the `refuse()` branch — a bare `A\n`, no roster — is
    // exactly the case `RosterSnapshot::refuse`'s doc comment warns about:
    // for as long as this replica's `--replication-factor` disagreement
    // persists, every `H` answered here tells the heartbeating node
    // nothing about membership, eviction included.
    let ack: Arc<[u8]> = if snapshot.refuse() {
        Arc::from(b"A\n".as_slice())
    } else {
        Arc::from(build_heartbeat_ack(&snapshot.nodes, replication).as_slice())
    };

    let mut cached = registry
        .heartbeat_ack
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    // Only advance the cache — a concurrent rebuild that already stored a
    // newer generation for this same replication must not be overwritten
    // with this older one.
    if cached.as_ref().is_none_or(|current| {
        current.replication != replication || current.generation <= built_generation
    }) {
        *cached = Some(CachedAck {
            generation: built_generation,
            replication,
            ack: Arc::clone(&ack),
        });
    }
    ack
}

/// The `L` response: `N <count> <replication>\n` then one `<name-len>
/// <addr-len>\n<name><addr>\n` per `Joined` node — `build_heartbeat_ack`'s
/// sibling for `L`'s own `N` tag (see its doc comment; the two share the
/// same per-entry layout, so an SDK parses either the same way).
fn build_list_response(nodes: &[(String, String)], replication: usize) -> Vec<u8> {
    let mut response = format!("N {} {}\n", nodes.len(), replication).into_bytes();
    for (name, addr) in nodes {
        response.extend_from_slice(format!("{} {}\n", name.len(), addr.len()).as_bytes());
        response.extend_from_slice(name.as_bytes());
        response.extend_from_slice(addr.as_bytes());
        response.push(b'\n');
    }
    response
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
    let generation = registry.generation.load(Ordering::Relaxed);
    {
        let cached = registry
            .list_cache
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if let Some(current) = cached.as_ref()
            && current.generation == generation
            && current.replication == replication
        {
            return Arc::clone(&current.response);
        }
    }

    // Rebuild: read the generation under the registry lock so it matches
    // the roster this snapshot serializes (a concurrent bump lands either
    // fully before or fully after this critical section) — mirrors
    // `cached_heartbeat_ack`.
    let snapshot;
    let built_generation;
    {
        let guard = lock(registry);
        built_generation = registry.generation.load(Ordering::Relaxed);
        snapshot = roster_snapshot_locked(&guard, replication);
    }

    // Issue #30 (amended, HIGH-severity follow-up): see the module docs'
    // `L` entry and `RosterSnapshot::refuse`'s own doc comment for why
    // replication-factor disagreement gets a strict-majority vote rather
    // than refusing on any dissent. Logged here, on rebuild only, rather
    // than on every request as the un-cached handler used to.
    if !snapshot.dissenting.is_empty() {
        if snapshot.refuse() {
            eprintln!(
                "WARN refusing L: {} of {} voting Joined nodes report a \
                 replication factor different from this replica's own \
                 --replication-factor {} (a strict majority) — dissenting: {} — \
                 discovery replicas have drifted out of alignment; the operator \
                 must align --replication-factor across every replica (see \
                 Discovery HA)",
                snapshot.dissenting.len(),
                snapshot.dissenting.len() + snapshot.agreeing,
                replication,
                snapshot.dissenting.join(", ")
            );
        } else {
            // Logged on rebuild rather than rate-limited or deduplicated
            // against a remembered dissenter set: simpler, and with the
            // cache in place a persistent single dissenter now logs only
            // as often as membership actually changes, not per `L` call.
            eprintln!(
                "WARN L served despite {} of {} voting Joined nodes reporting a \
                 replication factor different from this replica's own \
                 --replication-factor {} — not yet a strict majority, so still \
                 served, but worth investigating: {}",
                snapshot.dissenting.len(),
                snapshot.dissenting.len() + snapshot.agreeing,
                replication,
                snapshot.dissenting.join(", ")
            );
        }
    }

    let response: Arc<[u8]> = if snapshot.refuse() {
        Arc::from(b"B\n".as_slice())
    } else {
        Arc::from(build_list_response(&snapshot.nodes, replication).as_slice())
    };

    let mut cached = registry
        .list_cache
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    // Only advance the cache — a concurrent rebuild that already stored a
    // newer generation for this same replication must not be overwritten
    // with this older one.
    if cached.as_ref().is_none_or(|current| {
        current.replication != replication || current.generation <= built_generation
    }) {
        *cached = Some(CachedList {
            generation: built_generation,
            replication,
            response: Arc::clone(&response),
        });
    }
    response
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

    loop {
        tokio::select! {
            _ = ticker.tick() => {
                let now = Instant::now();
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
                registry
                    .proxies
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner())
                    .retain(|name, info| {
                        let keep = now.duration_since(info.last_seen) < liveness_timeout;
                        if !keep {
                            println!("INFO proxy dropped after missed announces: {name}");
                        }
                        keep
                    });

                let mut heartbeat_evicted = Vec::new();
                let mut waiting_evicted = Vec::new();
                {
                    let mut guard = lock(&registry);
                    guard.retain(|name, info| {
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
                    // roster (issue #95); Waiting evictions don't (they're
                    // never in it). Issue #279: bumped here, still under
                    // `guard`, rather than after it drops.
                    if !heartbeat_evicted.is_empty() {
                        bump_roster(&registry);
                    }
                }
                for name in &heartbeat_evicted {
                    eprintln!(
                        "WARN node evicted: {name} (no heartbeat within {}s)",
                        liveness_timeout.as_secs()
                    );
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
                let ready_member_evicted_mid_join = lock_current_join(&current_join)
                    .as_ref()
                    .is_some_and(|pending| {
                        heartbeat_evicted.iter().any(|name| {
                            pending.expected.contains_key(name)
                                && !pending.completed.contains(name)
                        })
                    });
                if ready_member_evicted_mid_join {
                    abandon_current_join(
                        &registry,
                        &current_join,
                        &auth_secret,
                        &tls_connector,
                        replication,
                        joins_ready_at,
                        "ready member evicted mid-join",
                    )
                    .await;
                }

                for (name, promoted) in waiting_evicted {
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

                // Staged node join pattern 3: a ready node alive but never
                // reporting `C` (see `migration_timeout_for`).
                let timed_out = lock_current_join(&current_join)
                    .as_ref()
                    .is_some_and(|pending| {
                        pending.started_at.elapsed() >= migration_timeout_for(pending.max_entries)
                    });

                if timed_out {
                    abandon_current_join(&registry, &current_join, &auth_secret, &tls_connector, replication, joins_ready_at, "migration timeout").await;
                }

                // Issue #63: a `J` accepted during the startup grace was
                // parked in `Waiting` (see `try_begin_next_join`); nothing
                // event-driven starts it once the grace ends, so the
                // first tick past the grace does — once; from then on
                // joins are started by `J`/`C`/abandon exactly as before.
                // (No join can have started during the grace, so there's
                // nothing for this one call to collide with.)
                if !grace_joins_kicked && Instant::now() >= joins_ready_at {
                    grace_joins_kicked = true;
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
            _ = shutdown_rx.changed() => return,
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
/// necessary. Distinct from `write_all_timed`, which bounds this node's
/// own *outbound* dials to a node (`M`/`X`) under `OUTBOUND_IO_TIMEOUT` —
/// that one is about this process acting as a client of another node;
/// this one is about this process, as a server, replying to whoever
/// dialed it.
async fn write_response(stream: &mut ServerStream, data: &[u8]) -> io::Result<()> {
    timeout(WRITE_TIMEOUT, stream.write_all(data))
        .await
        .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "write timed out"))?
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
                            info.last_heartbeat = Instant::now();
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
                    eprintln!(
                        "WARN node {name} reports replication factor {reported}, but this \
                         discovery replica is configured with --replication-factor \
                         {} — every replica must agree (see discovery HA); a client \
                         or node that learned R from a different replica may be running \
                         with a different fan-out/failover width right now",
                        config.replication
                    );
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
                        if removed_state == Some(NodeState::Joined) {
                            let ready_member_left_mid_join = lock_current_join(&current_join)
                                .as_ref()
                                .is_some_and(|pending| {
                                    pending.expected.contains_key(&name)
                                        && !pending.completed.contains(&name)
                                });
                            if ready_member_left_mid_join {
                                abandon_current_join(
                                    &registry,
                                    &current_join,
                                    &config.auth_secret,
                                    &config.tls_connector,
                                    config.replication,
                                    config.list_ready_at,
                                    "ready member left mid-join",
                                )
                                .await;
                            }
                        }

                        write_response(&mut stream, b"R\n").await?;
                        continue;
                    }
                    Err(()) => {
                        eprintln!(
                            "WARN rejected node leave for {name} from {peer_ip}: token mismatch"
                        );
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
                            println!("INFO proxy deregistered: {name}");
                        }
                        write_response(&mut stream, b"R\n").await?;
                        continue;
                    }
                    Err(()) => {
                        eprintln!(
                            "WARN rejected proxy deregister for {name} from {peer_ip}: \
                             token mismatch"
                        );
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
                            existing.address = addr;
                            existing.last_seen = Instant::now();
                            true
                        }
                        // A different token is a name hijack, not a
                        // refresh — see `ProxyInfo::token`.
                        Some(_) => false,
                        None if at_capacity => false,
                        None => {
                            proxies.insert(
                                name.clone(),
                                ProxyInfo {
                                    address: addr,
                                    token,
                                    last_seen: Instant::now(),
                                },
                            );
                            true
                        }
                    }
                };

                if accepted {
                    write_response(&mut stream, b"R\n").await?;
                    continue;
                }
                eprintln!(
                    "WARN rejected proxy announce for {name} from {peer_ip}: token mismatch \
                     or proxy registry full"
                );
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

                let entries: Vec<(String, String)> = {
                    let proxies = registry
                        .proxies
                        .lock()
                        .unwrap_or_else(|poisoned| poisoned.into_inner());
                    proxies
                        .iter()
                        .map(|(name, info)| (name.clone(), info.address.clone()))
                        .collect()
                };
                let mut response = format!("N {}\n", entries.len());
                for (name, addr) in &entries {
                    response.push_str(&format!("{} {}\n{name}{addr}\n", name.len(), addr.len()));
                }
                write_response(&mut stream, response.as_bytes()).await?;
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
                        eprintln!(
                            "WARN rejected join for {name} from {peer_ip}: wrong token for \
                             an already-registered name — either an impersonation attempt \
                             (issue #34) or a node reusing another's name"
                        );
                        return Err(io::Error::new(
                            io::ErrorKind::InvalidData,
                            "join with a token that does not match the registered one",
                        ));
                    }
                    Err(JoinRejection::TooManyWaitingFromSource) => {
                        eprintln!(
                            "WARN rejected join for {name} from {peer_ip}: already has \
                             {MAX_WAITING_PER_SOURCE_IP} Waiting/Joining registrations \
                             outstanding from this source"
                        );
                        return Err(io::Error::new(
                            io::ErrorKind::InvalidData,
                            "too many pending joins already registered from this source",
                        ));
                    }
                    Err(JoinRejection::MigrateMessageTooLarge { message_len }) => {
                        eprintln!(
                            "WARN rejected join for {name}: the M message this join would \
                             require ({message_len} bytes) exceeds nanocached-node's own \
                             request-size cap ({NODE_MAX_REQUEST_SIZE} bytes) — the registry \
                             is too large to admit another node without nodes rejecting the \
                             handoff outright; see NODE_MAX_REQUEST_SIZE"
                        );
                        return Err(io::Error::new(
                            io::ErrorKind::InvalidData,
                            "registry too large to admit a new node: the resulting M message \
                             would exceed the node-side request cap",
                        ));
                    }
                    Err(JoinRejection::TooManyWaitingTotal) => {
                        eprintln!(
                            "WARN rejected join for {name} from {peer_ip}: \
                             {MAX_WAITING_TOTAL} Waiting/Joining registrations are already \
                             outstanding cluster-wide"
                        );
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
            Ok(DiscoveryCommand::Announce { name, port, token }) => {
                let addr = format!("{peer_ip}:{port}");

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
                            break 'decide None;
                        }
                        Some(Err(reason)) => break 'decide Some(reason),
                        None => {}
                    }

                    // Short-circuits on `at_capacity` so a registry
                    // that's already full doesn't also spend a limiter
                    // slot on a source it's about to reject anyway.
                    if lock(&registry).len() >= MAX_REGISTRY_SIZE {
                        break 'decide Some("registry is full");
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
                    if !announce_insert_allowed(&config.announce_limiter, peer_ip) {
                        eprintln!(
                            "WARN rejected announce for {name} from {peer_ip}: too many new \
                             registrations from this source within {}s (see \
                             ANNOUNCE_INSERT_COOLDOWN)",
                            ANNOUNCE_INSERT_COOLDOWN.as_secs()
                        );
                        break 'decide Some(
                            "too many new announces from this source; retry \
                                            shortly",
                        );
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
                            break 'decide None;
                        }
                        Some(Err(reason)) => break 'decide Some(reason),
                        None => {}
                    }
                    if guard.len() >= MAX_REGISTRY_SIZE {
                        break 'decide Some("registry is full");
                    }

                    println!("INFO node announced: {name} at {addr} (re-registered)");
                    guard.insert(name.clone(), NodeInfo::new(addr, NodeState::Joined, token));
                    // A newly (re-)admitted `Joined` node changes the roster.
                    bump_roster(&registry);
                    None
                };

                if let Some(reason) = rejection {
                    return Err(io::Error::new(io::ErrorKind::InvalidData, reason));
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
            }) => {
                handle_complete(
                    &registry,
                    &current_join,
                    &config.auth_secret,
                    &config.tls_connector,
                    config.replication,
                    config.list_ready_at,
                    &name,
                    &joining_name,
                    &token,
                )
                .await;
                write_response(&mut stream, b"A\n").await?;
                continue;
            }
            Err(ParseError::Incomplete) => {}
            Err(error) => {
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
    if let Err(err) = run(
        &address,
        args.liveness_timeout,
        // The startup grace (discovery HA) is the liveness window by
        // definition: it exists so every live member has had time to
        // re-announce before L is served, and that time IS the liveness
        // timeout. Not separately configurable.
        args.liveness_timeout,
        args.replication_factor,
        read_auth_secret(),
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
        let mut input = BytesMut::from(&b"H 9 2 5 extra\nsome-nametok-a"[..]);
        assert_eq!(parse(&mut input), Err(ParseError::InvalidLength));
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

        assert!(announce_insert_allowed(&limiter, ip));
        // A second attempt from the same source within the cooldown is
        // refused...
        assert!(!announce_insert_allowed(&limiter, ip));
        // ...but a different source is entirely unaffected by it.
        assert!(announce_insert_allowed(&limiter, other_ip));

        tokio::time::advance(ANNOUNCE_INSERT_COOLDOWN + Duration::from_millis(1)).await;

        // Once the cooldown has elapsed, the original source is allowed
        // again.
        assert!(announce_insert_allowed(&limiter, ip));
    }

    #[tokio::test(flavor = "current_thread", start_paused = true)]
    async fn announce_insert_allowed_evicts_the_oldest_entry_once_the_limiter_is_full() {
        // Regression: an attacker who cycles through source addresses
        // could otherwise grow `AnnounceLimiter` itself without bound —
        // defeating the very thing it exists to bound. It must stay a
        // fixed-size, oldest-evicted map instead.
        let limiter: AnnounceLimiter = Arc::new(Mutex::new(FxHashMap::default()));

        let first_ip = std::net::IpAddr::V4(std::net::Ipv4Addr::from(1u32));
        assert!(announce_insert_allowed(&limiter, first_ip));

        // Fill the limiter to capacity with distinct addresses, spacing
        // each one out in (virtual) time so every entry has a distinct,
        // well-ordered timestamp.
        for i in 2..=MAX_ANNOUNCE_LIMITER_ENTRIES as u32 {
            tokio::time::advance(Duration::from_millis(1)).await;
            assert!(announce_insert_allowed(
                &limiter,
                std::net::IpAddr::V4(std::net::Ipv4Addr::from(i))
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
        assert!(announce_insert_allowed(&limiter, overflow_ip));

        let guard = limiter
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        assert_eq!(guard.len(), MAX_ANNOUNCE_LIMITER_ENTRIES);
        assert!(
            !guard.contains_key(&first_ip),
            "the oldest entry should have been evicted to make room"
        );
        assert!(guard.contains_key(&overflow_ip));
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
            build_heartbeat_ack(&roster_snapshot(&registry, 2).nodes, 2).as_slice()
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
            build_list_response(&roster_snapshot(&registry, 2).nodes, 2).as_slice()
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
            }
        );
        assert_eq!(&input[..], b"L\n");
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
        tokio::spawn(run_metrics_server(listener, Arc::clone(&registry), ready));

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
        ));

        let (status, _) = http_get(&addr, "/readyz").await;
        assert_eq!(status, "HTTP/1.1 503 Service Unavailable");
        // Liveness is unconditional.
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
        let registry: Registry = Arc::new(RegistryState::default());
        let current_join: CurrentJoin = Arc::new(Mutex::new(None));

        let config = || ConnectionConfig {
            idle_timeout: IDLE_TIMEOUT,
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

        abandon_current_join(
            &registry,
            &current_join,
            &None,
            &None,
            2,
            Instant::now(),
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
        tokio::time::advance(Duration::from_secs(2)).await;
        tokio::task::yield_now().await;

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

        let joining_name_length: usize = header.next().unwrap().parse().unwrap();
        let joining_addr_length: usize = header.next().unwrap().parse().unwrap();
        let joined_count: usize = header.next().unwrap().parse().unwrap();
        let replication: usize = header.next().unwrap().parse().unwrap();
        let token_length: usize = header.next().unwrap().parse().unwrap();
        assert_eq!(header.next(), None);
        assert_eq!(joined_count, 1);
        assert_eq!(replication, 2);

        // Body: `<token><joining_name><joining_addr><roster>` — the token is
        // node-a's own membership token, echoed so it can authenticate the M.
        let body = &message[header_end + 1..];
        assert_eq!(&body[..token_length], "tk-node-a");
        let after_token = &body[token_length..];
        assert_eq!(&after_token[..joining_name_length], "node-b");
        assert_eq!(
            &after_token[joining_name_length..joining_name_length + joining_addr_length],
            "127.0.0.1:9002"
        );

        let roster = &after_token[joining_name_length + joining_addr_length..];
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

    #[test]
    fn try_acquire_per_ip_denies_once_the_cap_is_reached_and_frees_the_slot_on_drop() {
        // Mirrors `src/server.rs`'s own
        // `try_acquire_per_ip_denies_once_the_cap_is_reached_and_frees_the_slot_on_drop`.
        let counts: PerIpConnections = Arc::new(Mutex::new(HashMap::new()));
        let ip = std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST);

        let mut guards = Vec::new();
        for _ in 0..MAX_CONNECTIONS_PER_IP {
            guards.push(try_acquire_per_ip(&counts, ip).expect("under the per-IP cap"));
        }

        assert!(
            try_acquire_per_ip(&counts, ip).is_none(),
            "the per-IP cap must reject a connection once MAX_CONNECTIONS_PER_IP is reached"
        );

        // A different source IP has its own, independent budget.
        let other_ip = std::net::IpAddr::V4(std::net::Ipv4Addr::new(10, 0, 0, 1));
        assert!(try_acquire_per_ip(&counts, other_ip).is_some());

        // Dropping one guard frees its slot for the same IP again.
        guards.pop();
        assert!(try_acquire_per_ip(&counts, ip).is_some());
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
        tokio::time::advance(Duration::from_secs(2)).await;
        tokio::task::yield_now().await;

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

        // Immediately after: a second, different new name from the same
        // source is refused — the connection closes with no `R`.
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
            &[],
            2,
            OUTBOUND_IO_TIMEOUT,
        )
        .await
        .unwrap();

        node_task.await.unwrap();

        let mut expected = b"M 12 11 0 2 11\n".to_vec();
        expected.extend_from_slice(b"ready-token");
        expected.extend_from_slice(b"joining-node");
        expected.extend_from_slice(b"127.0.0.1:9");
        assert_eq!(*received.lock().unwrap(), expected);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn read_line_timed_fails_within_the_overall_deadline_against_a_trickling_peer() {
        // Regression for issue #216: read_line_timed used to call
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
        let result = read_line_timed(&mut stream, io_timeout).await;
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
            &[],
            2,
            OUTBOUND_IO_TIMEOUT,
        )
        .await;

        assert!(result.is_ok(), "expected recovery on retry, got {result:?}");
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
            &[],
            2,
            OUTBOUND_IO_TIMEOUT,
        )
        .await;

        assert!(result.is_err());
        assert_eq!(
            attempts.load(std::sync::atomic::Ordering::SeqCst),
            MIGRATE_SEND_ATTEMPTS
        );

        node_task.abort();
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
        tokio::time::advance(MIGRATION_TIMEOUT_BASE + Duration::from_secs(1)).await;
        tokio::task::yield_now().await;

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
}
