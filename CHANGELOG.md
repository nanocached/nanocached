# Changelog

All notable changes to the nanocached server (the `nanocached-node`,
`nanocached-proxy` and `nanocached-discovery` binaries and their container
images) are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/); versions follow
the `server/vX.Y.Z` tags. SDK and adapter changes are in their own
changelogs.

From 0.4.4 on, every release ships the server, the six SDKs and the seven
framework adapters at one version, whether or not a component changed.

## [Unreleased]

### Added

- `nanocached-node`: the `t` command refreshes or removes an entry's TTL in
  place, without rewriting its value, and with `G` also returns the value
  (`V <len> [<ttl>]`) and counts as a read. `t <ns-len> <key-len> [ttl] [G]`:
  the TTL counts from receipt, `0` expires the entry at once, and omitting
  it removes the TTL; a missing or expired key answers `N` and is never
  created. Unlike `i`/`k`/`x` it is not primary-only: it is idempotent, so
  every owner of the key accepts it. During a join or a decommission it is
  forwarded to the joiner or the entrant as the value with its new TTL, like
  a set. A node older than this release answers `En` and closes the
  connection, so upgrade every node before a client sends it. No SDK,
  adapter or the proxy uses it yet.
- `nanocached-node`, `nanocached-discovery` and `nanocached-proxy` print a
  one-line `WARN` at startup when they listen on an address that is not
  loopback and `NANOCACHED_AUTH_SECRET` is unset or empty: anyone who can
  reach the port can then read and write every entry. The check uses the
  address the listener actually bound, so `--host localhost` stays silent
  and a specific private address warns like `0.0.0.0`. Startup is not
  refused (a container has to listen on `0.0.0.0`); set a secret, bind
  loopback, or restrict the port at the network layer.

## [0.4.8] - 2026-10-21

### Fixed

- `nanocached-node` forwards a clear it receives while re-replicating
  after an eviction to every other member of the new ring, behind the
  writes it has already mirrored to the owners the change promoted. The
  clear used to go through a join's and a decommission's window only,
  so a write this node had acknowledged and mirrored to a promoted
  owner could land there after the client's own clear and leave the
  cleared key on that one owner.
- `nanocached-node` asks discovery for the list clients route by (`L`)
  when it refreshes the published roster before refusing a key, no
  longer for the node roster (`T`), which lists every registered node,
  a waiting or joining one included: under a ring with such a node in
  it the node owned fewer keys than under the published list, so the
  refresh refused, for the rest of a heartbeat, the keys it exists to
  serve. A refresh also no longer replaces a roster the heartbeat loop
  noted while the ask was in flight, which is the newer view.
- `nanocached-node` no longer hangs a client connection on a forward
  queue that has no drainer. A write forwarded to a joiner or an
  entrant waits for room when that target's queue is full, until the
  queue's drainer makes some; a queue whose drainer could not be
  started (the drainer channel full with its waiters at their cap) or
  died kept its writes and had no drainer, so the next forward to that
  target that did not fit waited forever, and with it the client
  request behind it. Such a queue now admits the write and the forward
  starts the drainer.
- `nanocached-node` no longer sends a clear or a handed-off write around
  in circles between two nodes. A clear forwarded to another node was
  byte-identical to a client's, so a node decommissioning while its
  join source still forwarded to it mirrored the clear back, the source
  forwarded it on again, and every write either took in between was
  wiped until a window closed; and a handoff (`U`/`u`) applied here was
  relayed to every join target, including a confirmed one its sender
  had already reached, so a leaver's drained value came back to it
  through its entrant and overwrote the newer value it held. Forwarded
  clears now travel as their own frame (`f`, carrying the receiver's
  membership token) and go on to a decommission's entrants only when
  they removed something here, and a node relays a handoff write or
  delete only to a joiner discovery has not confirmed yet (a client
  write relayed to that joiner by a node two joins back is now not
  relayed on; it reaches the joiner on the next write). During a
  rolling upgrade, a node still on an earlier version answers `f` with
  an error, so a clear forwarded to it while the versions are mixed is
  not applied there.
- `nanocached-node` forwards a clear it receives while decommissioning to
  every surviving node, behind the writes it has already queued for
  them. The clear used to go only to a join's targets, so a write this
  node had acknowledged and queued for its entrant could land there
  after the client's own clear and leave the cleared key on that one
  owner (#688).
- `nanocached-node` keeps a key deleted when a relayed handoff delete
  (`u`) and a put-if-absent handoff of the same key (`U … A`) cross: the
  delete leaves a two-minute tombstone the put-if-absent is refused
  against. A join transfer and a re-replication read each key as they
  go and send it on their own connection, so a copy read before a
  client's delete could land after the relayed delete and bring the key
  back on the joiner — seen with two joins a few seconds apart, where
  several old owners send the same key (#690).
- `nanocached-node` mirrors the client writes it accepts during a
  re-replication, and for 60 s after it, to the owner the eviction
  promoted — as unconditional `U`/`u` handoffs, the way a decommission
  already mirrors them to its entrant. A client keeps its node list for
  up to 30 s after an eviction (every SDK), so its writes reached the
  surviving old owners only, and the promoted owner kept the copy the
  pass had handed it: an acknowledged write, delete or increment was
  missing on that one owner until the key was written again (#689).
- `nanocached-node`'s sweep no longer reclaims a marked copy of a key
  the node owns again. A join marks the keys it moves as dead copies for
  the sweep; when that joiner is evicted the keys come back to the node,
  but the marks stayed, so a put-if-absent re-replication arriving while
  a later join's transfer held the sweep paused was refused as
  "present", and the sweep then deleted the copy it had refused the
  write for. A marked copy of a key the adopted ring gives this node is
  now unmarked and kept (#682).

### Changed

- `nanocached-node` and `nanocached-proxy` reject a `--max-connections`
  or `--max-in-flight-bytes` above what the budget can count
  (`tokio`'s semaphore limit) at argument parsing, instead of panicking
  after "listening" was printed.
- `nanocached-node`'s smallest accepted `--namespace-budget` is ten
  entry overheads and so rises from 1,000 to 1,800 bytes with the
  overhead change below; a budget in that range is now refused at
  startup.
- `nanocached-node` charges each entry 180 bytes and each non-default
  namespace 750 bytes of overhead against `--max-memory` (was 100 and
  256), the figures measured on the container image. A node filled with
  tiny entries used 1.6× its budget in resident memory, one filled with
  tiny namespaces 2.3×, so the documented sizing (a 512 MiB cache in a
  1 GiB task) did not hold for them. Capacity in entries at a given
  `--max-memory` drops accordingly: about 1.4M small entries at the
  default 256 MiB instead of 2–3M (#697).
- `nanocached-proxy` answers the fatal `E` (and closes) instead of the
  retryable `R` when its connection to a node fails after an `i`, `k` or
  `x` may have reached the node. `R` is defined as "not applied, re-send",
  and every SDK re-sent on it, so a counter could move twice and a
  compare-and-set could report failure for a condition it had applied.
  Idempotent writes and requests that provably never left the proxy keep
  getting `R` (#656).
- `nanocached-node` answers `W` to `i`, `k` and `x` on a key it owns but is
  not the primary for. Any owner used to run them, so after a join a
  client with a stale ring and one with the fresh ring could both win the
  same `put_if_absent` or CAS, or both receive the same counter value,
  for as long as the stale client kept its ring; concurrent increments in
  the seconds after a join lost updates the same way. Plain writes are
  unchanged (every owner accepts them); the SDKs and the proxy already
  refresh and retry on `W` (#718, #655).

### Added

- `nanocached-proxy` logs a replica leg of `S`/`D` (and of the result
  write after `i`/`k`/`x`) that a node answered `W` or `E`, naming the
  node, and counts every such leg in a new
  `nanocached_proxy_replica_refusals_total` metric. Before, only a leg
  that failed at the transport level or a multi-set leg left a line;
  a refused leg changed nothing the client could see and nothing the
  operator could see either (#719).

### Fixed

- A node decommissioned inside its own join's forwarding window no
  longer overwrites newer acknowledged writes on the surviving owners.
  Its drain reached an entrant, which applied each key and relayed it to
  its join targets — the leaver itself, still inside that window — and
  the leaver relayed the echo back to the entrant, so the drained value
  circled between the two until the entrant's heartbeat revoked the
  forward, overwriting on each pass whatever a client had written to the
  entrant in between: sets, deletes, counters, compare-and-sets and
  multi-sets acknowledged in those seconds came back as the older value.
  A leaving node now relays a handoff (`U`/`u`) to its entrant only when
  it changed the leaver's own store; an echo of its drain, or a delete
  of a key it does not hold, stops there (#680, #717). The conformance
  harness gains a `joinfresh` scenario (a joiner decommissioned 6 s
  after it is listed) for it.
- `nanocached-node` answers `En\n` before closing the connection on a
  request it cannot parse (an unknown command, a malformed length or TTL,
  an empty key) or one over the request-size limit. It used to close
  with no reply, while `docs/protocol.html` promised an `E` and the proxy
  sent one, so a hand-written client could not tell a rejected request
  from a dropped connection; for an over-size request the node now also
  waits briefly for the client to close first, so the reply is not lost
  to a reset (#654, #733).
- `nanocached-node` and `nanocached-discovery` deliver their `B` (busy)
  reply to a client refused at the connection limit. The reply was
  written and the socket dropped at once, with the client's own `A`
  frame still unread in it, and closing on unread bytes is a reset: the
  client read "connection reset by peer" and never the `B`, on Linux
  and macOS alike. The over-limit path now half-closes after the reply
  and reads the client's bytes away, briefly, so the close is a FIN that
  follows the `B` (#663).
- `nanocached-node` frees the entries a `c`/`F` (clear) removed on a
  thread of their own instead of on the cache thread. Dropping the map
  freed every entry inline, so a clear of 3,000,000 small entries held
  every other request on the node for 0.9 s; now the cache thread does
  one map removal and returns, and the memory is returned over the
  moments that follow (a map under 4,096 entries is still freed inline,
  in well under a millisecond). The protocol page and the six SDK and
  adapter READMEs that called this an O(1) drop with memory reclaimed
  immediately now say what happens (#702, #732).
- `nanocached-node` bounds what it queues for a node it is forwarding
  writes to (a joiner during a join, an entrant during a decommission)
  at 64 MiB per target, counting each queued write's key and value, a
  per-entry overhead and each namespace allocation once. The queue had
  no bound and a queued write pinned the whole client frame it came
  from, so a joiner that stopped answering let a few hundred tiny writes
  under a 1 MB namespace take a 256 MiB node down. A client write that
  does not fit is held, before its ack, until the queue has room; to
  make that possible every forward is now queued before the client's
  reply is written rather than after (#694).
- Text a user sees no longer carries issue numbers or references to
- `nanocached-node` asks discovery for the current member list before
  answering `W` for a key its own ring does not grant (at most once a
  second, each ask bounded at a second), and serves the key when the
  published list makes it an owner. A node's ring followed discovery
  only on its next heartbeat — up to 5 s after a leave or an eviction
  was published, and not at all while a join was pending on the node —
  so a client that already routed by the new list had its write to the
  new owner refused; the SDKs drop a refused replica leg, and the owner
  kept the older copy it had been re-replicated for the key's lifetime
  (#681; the kill-window stale owners of #689 are the same refusal). or references to
  documentation sections that do not exist: the `/metrics` HELP lines
  for `connections_max`, `incrs_total`, `cas_sets_total`,
  `cas_deletes_total` and `namespace_budget_bytes`, five discovery
  `WARN` lines, the node's `--help` text and its `--max-memory` error
  now state the reason in words (#669, #737).

## [0.4.7] - 2026-10-19

### Fixed

- `nanocached-node`: the `# HELP` line of `nanocached_node_sets_total`
  said a staged join's migration transfer and a decommissioning node's
  drain-out were counted there. Both travel as `U` handoff frames since
  the join bulk transfer moved to them (#266), and handoff writes bypass
  the client counters (#394), so the counter has counted client writes
  only; the description now says so. Found while verifying the metrics
  guide (#169): a joiner's `entries` rose to the migrated count while its
  `sets_total` stayed at 0.
- `nanocached-proxy`: warns at start-up when TLS is configured in one
  direction only (`--tls-cert`/`--tls-key` without `--tls-ca`, or the
  reverse), as the node and discovery binaries already did; a proxy with no
  `--tls-ca` against a TLS-only cluster looked fine to its clients and never
  obtained a roster, and its own log never said why. A roster-refresh
  failure that looks like a plaintext request to a TLS-only peer (the
  connection closed before the first reply) now says so and names the flag.

## [0.4.6] - 2026-10-17

### Fixed

- `nanocached-node`: the ownership reclaim pass (#582) no longer takes a
  snapshot of every key every 30 s. Each pass started with a full key walk
  on the cache task (about 25 ms per million keys, during which every
  request waits) that held 64 bytes per key outside `--max-memory` (61 MiB
  at 1M keys, 183 MiB at 3M — roughly the default limit's worth of small
  entries) and then re-checked ownership of every key on the same task
  (about 200 ms per million), in steady state with nothing to remove. The
  pass now remembers what its last complete run judged against — the
  published-membership history, how many of those memberships the grace
  window still covers, and the cache's count of handoff writes — and skips
  the snapshot while none of the three has moved, which outside a
  membership change is always. An abandon that reverts a handoff's ring
  also re-arms it, since the keys admitted under that unpublished ring are
  what the pass exists to find. `docs/architecture.html`'s "Known limits"
  now lists the reclaim pass among the walkers and the snapshot's
  transient size (#594).
- `nanocached-node`: a `G` verdict from discovery (#571, "the joiner you
  asked about has left") no longer takes the remnant an `X` for the same
  join left behind (#573). The remnant keeps the abandoned slot's
  `forward_revoked` flag so the forwards already queued against that slot
  stay revoked, and that flag is also the identity the heartbeat carried
  — so when the `X` landed before the heartbeat's ack, the `G` matched the
  remnant, "abandoned" it (nothing to restore, the remnant is rebuilt with
  its lingering windows intact) and scheduled a re-replication over a ring
  nobody left, which superseded whichever real re-replication was in
  flight and left its unsent keys for the next ring change. Needs another
  join inside the previous joiner's forwarding grace and the `X` beating
  the ack; found by review, reproduced in a unit test, not in Docker
  (#595).
- `nanocached-node`: a handoff write (`U`/`u`) for a key this node already
  owns under the membership it has adopted no longer pauses the ownership
  reclaim pass (#582) for 120 s. The pause exists for a key received ahead
  of its ring — a leaver's handoff before the post-leave roster publishes,
  a re-replication before the roster that made this node a target — which
  no ownership check the pass holds can see; it fired for every handoff
  write, so each re-replication `u` a survivor received after an eviction
  or leave, and a leaver's forwards for keys the receiver already held as
  a replica, postponed its reclaim of leftovers from an earlier join by the
  drain or the churn plus 120 s. The connection handler now tells the cache
  whether the key was owned when the write landed; only a key that was not
  pauses the pass (and counts toward the #594 mark). Delay, not loss, from
  reading the code (#601).

### Security

- Updated `rustls` to 0.23.46 in the node, proxy and discovery binaries
  (RUSTSEC-2026-0346: `Debug` impls for client session values exposed TLS
  secrets; RUSTSEC-2026-0347: buffered application data was sent to the
  outer server after an ECH rejection; RUSTSEC-2026-0348: Brotli
  certificate decompression could allocate up to 1 GiB before
  authentication; RUSTSEC-2026-0349: an unauthenticated ServerHello could
  downgrade later connections to classical key exchange). The crate's
  lower bound is raised to 0.23.46 as well (#593).

## [0.4.5] - 2026-10-15

### Fixed

- `nanocached-node`: abandoning a handoff no longer ends the forwarding
  windows of the joiners before it. Since #564 a superseded handoff keeps
  forwarding from the new slot's `lingering` list, but the three ways a slot
  is dropped — an `X` (or a `G`, #571) for its joiner, that joiner's
  eviction read from a heartbeat roster, and a transfer that aborts or
  fails — took the whole slot with it, lingering entries included, so a
  client that still wrote to the old owners stopped reaching an earlier
  joiner inside its grace and that joiner fell behind. All three now leave
  the still-open lingering forwards behind in a closed remnant of the slot
  (own window shut, confirmed, no marks) that clears once their windows
  end. The remnant is never taken for the handoff it was cut from (a
  retried `M` starts a new transfer rather than being acked as a
  duplicate), and the eviction check still revokes a departed joiner's
  queued forwards whatever the state of its window but only closes a
  window that is actually open, so the remnant is not re-evicted on every
  ack. Found by a review of
  #569–#572; not reproduced in Docker (it needs a second join to be
  abandoned, or its joiner to leave unseen or be evicted, inside the first
  joiner's grace).
- `nanocached-node` / `nanocached-discovery`: a sender no longer keeps
  forwarding to a joiner that left within one heartbeat interval of its
  promotion (#571). The heartbeat ack lists the members discovery knows, so
  a joiner that was promoted and decommissioned between two of a sender's
  heartbeats never appeared in a roster that sender saw; its handoff slot
  stayed completed but unconfirmed, which the node read as "not promoted
  yet", and it forwarded every concurrent write for those keys to the
  departed node until the forwarding grace ran out (2,515 refused forwards
  over 65 s in a Docker reproduction) and held the slot's dead-copy marks
  until the next join. No data was lost: the marks were held back from the
  sweep, the leaver's drain handoff had re-sent the keys, and the next `M`
  restored the rest. The heartbeat (`H`) now carries a trailing
  `<joiner-len>` field and the joiner's name while the node has such a
  slot; discovery answers the ack with one more line, `K` (still
  registered, or not known to be gone) or `G` (on record as having left,
  been evicted or abandoned, and not registered since), and on `G` the
  node abandons the slot as it does for an `X` — only that very slot, not
  one a same-name retry installed meanwhile — and re-replicates the keys
  the departed joiner owned from its post-join ring, since the reverted
  membership view hides that drop from the usual trigger. Discovery reports a name
  gone only from that record — never from mere absence, which is what a
  restarted replica's empty registry looks like — and forgets it when the
  name registers again. **Upgrade discovery before the nodes**: an older
  discovery rejects the extra heartbeat field, and a node in that state
  would be evicted for missing heartbeats.
- `nanocached-node`: a node answers `W` to every client key request until
  discovery has promoted it. Before, a request that reached a node still in
  its join handoff was served from whatever the migration had delivered so
  far, so a key that was live elsewhere read as a miss (`N`). No client's
  node list names such a node, but a client can hold a connection to it
  under another name: an SDK's connect dials every listed member and waits
  for all of them, so after a kill the dial to the dead member's address
  stays pending until its deadline, and when the replacement process starts
  listening on the same address within that time (a same-host restart) the
  dial completes, labelled with the evicted name. Every read on that
  connection missed until promotion — a few hundred in the half second
  before it in a Docker reproduction (kill, eviction, immediate restart),
  none afterwards. The `W` makes the client refresh its list, which no
  longer names the evicted node, and retry on a surviving owner. Concurrent
  writes a sender forwards to the joiner during the handoff now go as
  `U`/`u` frames, which the check does not apply to, as a lingering
  joiner's already did.
- `nanocached-node`: a join no longer ends the forwarding window of the join
  before it. A node holds one handoff slot, and a new `M` used to replace
  it and revoke its forwards even though the earlier joiner was confirmed
  and its forwarding grace (100+ seconds) was still open. A client whose
  node list had not caught up kept writing to the old owners, which from
  then on forwarded only to the newest joiner, so an earlier joiner that
  owned the key stopped receiving the writes and stayed behind (later a
  stale read once the other copies left). The superseded handoff now moves
  to a `lingering` list on the new slot, keeps its connection and its
  forwards, and receives a write for every key it owns under the current
  ring (and every `CLEAR`) until its grace ends. It is dropped and its
  forwards are revoked, as before, when its join was abandoned, when the
  same name joins again at a new generation, and when discovery evicts or
  removes it (issues #267 and #474: its address may belong to another node
  by then). Observed in a join-then-decommission-then-kill chaos run: the
  restarted node that had joined just before the next join was behind its
  peers on 4–285 keys right before the last kill (median 139) and is now
  behind on 0–47 (median 6.5); the end-to-end count of stale reads in that
  run did not change measurably because other causes dominate it (#563,
  #565). Issue #564.
- `nanocached-node`: a write forwarded to an earlier joiner that is still
  inside its forwarding window (the `lingering` list above) is now sent as
  an unconditional handoff frame (`U`/`u`, the frames a decommission's
  leave-forward already uses) instead of a plain `S`/`D`. That joiner is a
  confirmed member with a ring view of its own, which follows its heartbeat
  acks and can lag the ring the sender forwards by. Seen in the same chaos
  run after a kill, its eviction and a restart whose `M` landed one second
  later, before the survivors had adopted the eviction: the sender picked
  the earlier joiner as a target by the new ring, the joiner still ranked
  the evicted node above itself for those keys and answered `W`, and the
  three retries all fell inside the same second, so 408 writes were dropped
  as "permanently failed to forward" (five runs of a shorter reproduction:
  123–256 per run, none after the change). The slot's own joiner is not
  affected: it has no ring of its own until discovery promotes it and only
  gains ownership until the next `M` moves it into `lingering`. The WARN
  for a refused forward now says what the peer answered. A node receiving
  a `U`/`u` while it is itself decommissioning now mirrors that write to
  the decommission's entrant the way it already did for a plain `S`/`D`;
  before, a handoff frame landing after the drain snapshot (from a
  lingering joiner's old owner, a chained drain-out, or a re-replication)
  stayed on the departing node and left with it.
- `nanocached-node`: with an auth secret configured, a connection that has
  not authenticated yet is now held to what an unauthenticated peer can
  legitimately do. The node used to parse and buffer a whole request (up to
  1 MiB, and for an `M` a 32-byte span per roster entry) before checking
  `authenticated`, so each of up to 1024 unauthenticated connections could
  pin that much. Now only an `A` frame is accepted first: anything else is
  answered `En` and closed on its first byte without being buffered, an `A`
  frame (header plus secret) is capped at 4096 bytes while incomplete
  (matching discovery's pre-identification cap; a longer secret field no
  longer makes the node keep reading). Commands pipelined behind a complete
  `A` frame are unaffected, and a node with no secret behaves exactly as
  before. Because an `A` frame must now fit in 4096 bytes, the node refuses
  to start with a `NANOCACHED_AUTH_SECRET` longer than about 4080 bytes
  (discovery's identical cap already ruled such a secret out in a cluster).
- `nanocached-node`: a multi-key `m`/`o` frame no longer costs (keys x
  namespace length) on the node's single thread. A namespace can be about
  1 MiB, and the ownership checks (FNV over the whole namespace) and the
  cache lookups (SipHash over it) ran once per key, so one frame of 1-byte keys
  under a ~500 KB namespace hashed tens of gigabytes. The namespace's
  share of the key hash and its cache entry are now resolved once per
  frame, and the migration, decommission and re-replication loops hash
  each distinct namespace once. Placement is unchanged (the hash is
  byte-identical) and there is no new limit on namespace length.
- `nanocached-node`: a namespace's name is now charged to `--max-memory`
  (its length plus a fixed 256-byte overhead, released when the namespace
  empties or is cleared; the default namespace is not charged). Before, a
  stream of `s`/`o` writes naming fresh large namespaces with 1-byte keys
  and values was accounted about 100 bytes apiece while the process held
  every name, so the memory bound never evicted. Per-namespace
  `--namespace-budget` accounting and the `/metrics` per-namespace rows are
  unchanged; the namespace rows now sum to `used_bytes` minus these name
  charges.
- `nanocached-node`: client writes to keys being handed to a joining node (or
  to the entrants of a decommission) are no longer dropped when the target
  is slow. Each write was its own task behind a bounded channel, so while
  the target was busy (for example a replacement node receiving its
  handoff) the backlog passed about 4350 and the rest were dropped with a
  warning: under chaos load, 10,000 to 35,000 per node, leaving the joiner
  with older values than the cluster had acknowledged. Writes now wait in a
  per-target queue and a later write to a key replaces one still waiting
  for it, so the queue is bounded by the number of distinct keys, not by
  the number of writes, and nothing is dropped for want of room. A clear
  and a put-if-absent relay keep their place in the order. One drainer per
  target sends in order, so a large `o` costs one queue insert per key.
- `nanocached-node`: a `U` or `u` carrying the wrong membership token on a
  connection that negotiated tagged mode no longer panics the connection
  task. The rejection is now answered `R <tag>` (the tagged form of the
  retryable-error status) before the connection is closed; handoff peers
  connect untagged and still get the bare `R`.
- `nanocached-node`: `--drain-timeout` is now limited to 604800 seconds
  (7 days) and rejected above that at startup. Any larger value (the flag
  took an unbounded `u64`) overflowed the `Instant` the drain deadline is
  computed from, so the first SIGTERM panicked instead of shutting down.
- `nanocached-node`: forwarding a client write to a joining node (or a
  decommission entrant) no longer `Debug`-formats the whole key on every
  forward. A key can be about 1 MiB, so this was a per-write allocation
  and format on the node's single thread.
- `nanocached-node`: shutdown no longer waits out an unresponsive peer
  during an in-flight re-replication. The dial/auth and send legs are now
  interrupted by the shutdown signal (before, only the gaps between
  attempts noticed it, so a stalled leg held the exit for up to its 10 s
  bound), and `run` waits at most 5 s for the heartbeat task (which carries
  re-replication) before aborting it.
- `nanocached-node` and `nanocached-proxy`: an `m`/`o` frame whose body arrives
  a few bytes at a time no longer has its (O(keys)) header re-parsed on every
  read. This ran before authentication, so a large header followed by a
  trickled body could keep the node's single thread busy.
- `nanocached-node`: reporting a finished migration to discovery (`C`) is now
  bounded by one 10 s budget for the whole call. Before, the auth leg and the
  `C`/`A` exchange each had their own 10 s, so a slow discovery could hold the
  report for up to 20 s.
- `nanocached-node` and `nanocached-proxy`: an `M` (multi-get) reply is now
  bounded to 16 MiB of values. A request that named one key many times could
  make the node allocate a reply of (key count x value size), over a gigabyte
  from a ~1 MiB request; hits past the budget are now answered as misses. The
  proxy used to reject any backend `M` reply over 1 MiB of values as a
  protocol error, dropping the shared backend connection and failing every
  other client's in-flight requests on it; it now accepts up to the node's
  16 MiB and holds its own reassembled reply to the same figure.
- `nanocached-proxy` and `nanocached-discovery`: a panic while serving a
  metrics scrape is now logged (`WARN metrics connection task failed`)
  instead of going unobserved, like the node's metrics endpoint.
- `nanocached-proxy`: routing the keys of a large `m`/`o` frame no longer
  scores, sorts and copies the whole roster once per key (O(nodes log nodes)
  per key, hundreds of thousands of times for a ~1 MiB frame, on a tokio
  worker). Each key now keeps only its top owners in one pass, and keys are
  grouped by node without cloning an address per key. Key placement is
  unchanged.
- `nanocached-proxy`: a burst of `W` replies (a node mid-handoff answers `W`
  for every key it no longer owns) no longer drives one roster fetch and
  `Y` announce to every discovery replica per reply, back to back. Forced
  refreshes are now spaced at least 1 s apart and the nudges in between are
  coalesced into one fetch, and a fetched roster identical to the current
  one is no longer republished to the proxy's connections.
- `nanocached-proxy`: after a client was answered the fatal `E` (or a write to
  it failed or timed out), its connection now closes at once. Before, the
  reader kept waiting for up to the 60 s idle timeout, holding the socket,
  a connection permit and a per-IP slot; and a frame the client sent in that
  window was still run against the backends before its reply failed to
  send, so a `S` or `i` could be applied after the client had been told the
  connection was closed.
- `nanocached-discovery`: a connection's idle clock for its next command now
  starts when the previous command's handler finishes, not when it was
  parsed. A joining node that waited in the queue for more than 60 s was
  promoted and answered `R`, then closed as idle before its first heartbeat
  could arrive (it recovered by redialing and re-announcing, at the cost of a
  reconnect and a `WARN`); the same applied after a slow `C` or `V` handler.
- `nanocached-discovery`: the periodic sweep (liveness eviction, proxy
  reaping, the migration-timeout reaper) no longer pauses while an abandoned
  join's `X` fan-out or the next join's `M` fan-out is running. Those used
  to be awaited inline, so a batch of unresponsive nodes froze the sweep for
  up to ~40 s (`X`) or ~120 s (`M`, three attempts) per batch of 64, exactly
  when nodes were failing. They now run in the background, one at a time
  (as serialized with each other as before), and an eviction noticed while
  one is running is acted on as soon as it finishes.
- `nanocached-node`: once the cache is at `--max-memory`, each write no longer
  scans every namespace to find the one holding the oldest entry. A lazily
  kept min-heap of each namespace's oldest entry replaces the scan, so the
  cost per eviction no longer grows with the number of namespaces (with
  200,000 namespaces, an evicting write went from about 7.6 ms to about
  14 us). Which entry is evicted is unchanged.

### Security

- Updated `rustls` to 0.23.45 in the node, proxy and discovery binaries
  (RUSTSEC-2026-0285: TLS 1.3 handshake messages were accepted across
  encryption-level boundaries).
- `nanocached-proxy`: with an auth secret configured, a connection that has
  not yet authenticated is now held to the same bounds discovery applies. It
  may buffer at most 4096 bytes (only an `A` frame is acceptable before the
  secret is checked; a larger declared frame is refused from its header) and
  must authenticate within a fixed 60 s of being accepted. Before, it could
  declare an `A` of up to 1 MiB and trickle it in a byte per 59 s, holding
  1-2 MiB indefinitely per connection (about 1 GiB across the default 1024
  connections) because the 60 s idle timeout restarts on every read. A
  proxy with no secret is unchanged.
- `nanocached-node`, `nanocached-proxy` and `nanocached-discovery`: the
  per-source-IP connection cap now counts an IPv6 peer by its /64 prefix
  instead of its full address, and an IPv4-mapped IPv6 peer
  (`::ffff:a.b.c.d`) as the IPv4 address. Before, one /64 offered 2^64
  distinct sources, each with a full cap of its own, so a single host could
  hold every connection slot. IPv4 behavior is unchanged. Clients that share
  a /64 (hosts on one subnet, or pods on one node of an IPv6 Kubernetes
  cluster) now share one cap, so raise `--max-connections-per-ip` for them
  as for any other shared source.
- `nanocached-discovery`: the cap on concurrent `Waiting`/`Joining`
  registrations per source (`MAX_WAITING_PER_SOURCE_IP`) now counts an IPv6
  source by its /64 prefix, like the per-source-IP connection caps. Before,
  it compared the registered address text, so every address of one /64 held
  its own allowance and one host could fill the global waiting queue.

## [0.4.4] - 2026-09-09

No changes. Version aligned with the rest of the project (the first release
that also publishes the seven framework adapters).

## [0.4.3] - 2026-09-08

Server-only release.

### Added

- `nanocached-proxy --max-connections-per-ip <n>` (default 256, the same
  default the node has always had) caps client connections per source IP.
  Previously only the global `--max-connections` applied, so one client IP
  holding idle connections could exhaust the whole budget and make every
  other client see `B` while the cluster was healthy. Over the cap the proxy
  answers `B` and closes, like the node. The value must not exceed
  `--max-connections`; an unset cap follows a lowered `--max-connections`
  down (issue #520).

## [0.4.2] - 2026-09-07

### Fixed

- `nanocached-proxy` keeps its shared backend connections alive: after 30 s
  without writes it sends the SDKs' reserved keep-alive `G` probe, so the
  node's 60 s idle timeout no longer closes the connection underneath an idle
  proxy. Before, the first request after a quiet minute was written to a dead
  socket, and an `INCR`/`CAS` among them was answered with an error because a
  written frame cannot be retried. A connection lost with only the probe on it
  is now logged at INFO instead of WARN (issue #514).

## [0.4.1] - 2026-09-06

### Fixed

- `nanocached-proxy` no longer replays pipelined `INCR`/`CAS`/`CAD` requests
  that sat behind a failed reply on a shared backend connection. Those frames
  had already been written and may have been applied, but were classified as
  "not sent" and so retried after the redial, which could apply a counter
  increment twice. They now fail as "may have been applied" (issue #497).
- `nanocached-node` shutdown no longer waits up to 60 s (twice back to back)
  for a re-replication wait to time out: the waits now stop as soon as
  shutdown is requested, so a SIGTERM during a join migration or a superseded
  re-replication stays within the orchestrator's stop grace (issue #498).
- `nanocached-node` shutdown drain now also runs forwards still queued behind
  the last connection task. Before, a write already acknowledged to a client
  could be dropped unlogged without ever reaching the joining or leaving node
  (issue #502).

## [0.4.0] - 2026-09-05

First server release since 0.3.0: new `nanocached-proxy` binary and image,
namespaces, new commands, autoscaling support (metrics, health endpoints,
graceful scale-in), and the fixes from audit passes 3 to 11. The
cluster-internal frames changed (see the **Breaking** entries below):
upgrade every node and every discovery replica in the same rollout, and
upgrade nodes before clients start using namespaces.

### Added

- `nanocached-proxy` (issues #109, #110): a new binary and a third container
  image (`nanocached-proxy`) that terminates client connections and routes on
  their behalf. To a client it looks like one node that owns every key, so an
  unmodified SDK in single-address mode works against it. Reads go to the
  primary with replica fallback, writes and deletes fan out to all R owners
  with the primary's answer returned, a stale-view `W` is refreshed and
  retried, and `c`/`F` fan out to every member. All client traffic for a node
  shares one pipelined backend connection, so the node-side connection count
  is the proxy count rather than the client count. Flags: `--host`, `--port`,
  `--discovery`, `--tls-cert`/`--tls-key`/`--tls-ca`, `--max-connections`,
  `--max-connections-per-ip`, `--drain-timeout`, `--metrics-port`. The
  shared secret comes from `NANOCACHED_AUTH_SECRET` (`NANOCACHED_SECRET` is
  accepted as a deprecated fallback with a startup warning; the new name wins
  when both are set).
- Discovery serves the proxy roster (issue #122): `Y` (proxy announce), `Z`
  (proxy deregister) and `Q` (proxy list, answered `B` during the startup
  grace). Proxy entries live apart from node membership and expire after
  `--liveness-timeout` without a re-announce.
- Namespaces (issue #105): lowercase `g`/`s`/`d` commands carry a
  length-prefixed namespace ahead of the key; the uppercase `G`/`S`/`D`
  address the default namespace unchanged. Routing hashes
  `fnv1a(be32(len(ns)) || ns || key)` for a non-empty namespace and the
  legacy `fnv1a(key)` for the default one, so existing key placement does not
  move. The cache keeps one LRU sub-map per namespace. A pre-namespace node
  answers `E` and closes on a namespaced frame.
- `c <namespace-length>` clears one namespace and `F` flushes every namespace,
  both answered `C` (issue #106). On a node this is an O(1) sub-map drop.
  During a join handoff, clears are queued and replayed on the transfer stream
  so they cannot be overtaken by stale keys.
- `i` atomic counter (issue #129): answered `I <value-length> [<ttl>]`, `N`
  for a missing key, or the new `T` status when the stored value is not a
  plain decimal `i64` or the delta would overflow. In a cluster only the
  primary evaluates it and its literal result is forwarded to replicas as an
  ordinary set.
- `k`/`x` compare-and-set and compare-and-delete (issue #141): the condition
  is `A` (absent), `P` (present) or a 32-hex-digit digest, the first 16 bytes
  of the SHA-256 of the stored bytes. Primary-only evaluation with
  result-forwarding replication, like `i`. LRU eviction can reclaim a
  CAS-protected key, so this is not a distributed lock.
- `m`/`M` batched get and `o`/`O` batched set (issue #150), with one shared
  TTL per `o` batch. The proxy groups keys by owner and does one bounded
  refresh-and-retry pass for stale views, with per-key `W` for keys that still
  fail.
- Retryable-error status `R` (issue #125): `A` accepts an optional trailing
  `R` capability token (`A <len> [T] [R]`). On a connection that declared it,
  `nanocached-proxy` answers a transient upstream failure with `R` and keeps
  the connection open instead of `E` and close; legacy connections still get
  `E`. Node and discovery accept the token.
- Operations endpoints (issue #124): `--metrics-port <port>` on node, proxy
  and discovery serves `/metrics` (Prometheus text format), `/healthz` and
  `/readyz` on a separate listener, off by default and unauthenticated, so
  keep it internal. Node `/readyz` is 503 until a clustered node has adopted a
  membership view; proxy `/readyz` is 503 without a roster and while
  draining; discovery `/readyz` is 503 during the startup grace. The listeners
  accept at most 16 concurrent connections.
- Graceful scale-in (issue #124). The node, on SIGTERM, stops heartbeating,
  hands every key it owns to the node the ring promotes for it (new `U`
  handoff-store command), leaves membership with the new `V` discovery
  command, keeps forwarding writes for a short window, then exits. A second
  signal overrides the drain. The proxy, on SIGTERM, deregisters via `Z`,
  stops accepting, finishes in-flight requests and exits. `--drain-timeout
  <secs>` (default 25) bounds both and must fit the orchestrator's stop grace.
- Node flags `--max-connections <n>` (default 1024) and
  `--max-connections-per-ip <n>` (default 256) replace the compile-time
  limits (issue #126). Both must be at least 1, an explicit per-IP cap may not
  exceed the total, and an unset per-IP cap follows a lowered total down.
  The new gauge `nanocached_node_connections_max` reports the bound.
- Node flag `--namespace-budget <ns>=<bytes>` (repeatable; an empty name is
  the default namespace) caps one namespace's share of memory (issue #127):
  over its cap, a namespace evicts its own oldest entries. It is a cap, not
  a reservation, and `--max-memory` stays authoritative. Values below 1000
  bytes and duplicate namespaces are rejected (issue #355). Reported by the
  gauge `nanocached_node_namespace_budget_bytes`.
- A surviving node re-replicates keys after an eviction promotes a new owner
  (idempotent put-if-absent via `U ... A`), so a cluster no longer runs
  under-replicated until a second failure loses data (issues #266, #267).
- Discovery `T` command (node roster including membership tokens), usable only
  by a registered, `Joined` node that identifies itself with its own token.
  Decommission and eviction-triggered re-replication use it instead of `L`.

### Changed

- **Breaking: `U`/`u` handoff frames now carry the receiving node's
  membership token, and `M` carries the joining node's token** (issue #295).
  Before, any holder of the shared secret could forge a handoff frame and
  write or delete a key regardless of ownership. Frames without the token are
  rejected, so a node and a discovery server from different versions cannot
  complete a staged join or a handoff. Upgrade all nodes and all discovery
  replicas together.
- **Breaking: a clustered node now hands off its entries and leaves
  membership on SIGTERM**, within `--drain-timeout` (default 25 s). Before,
  it exited immediately. Make sure the orchestrator's stop grace (ECS
  `stopTimeout`, Kubernetes `terminationGracePeriodSeconds`) exceeds the
  budget, or pass `--drain-timeout 0` for the old behavior.
- `M` and `C` carry a join generation as an optional trailing field, so
  discovery rejects a handoff-complete report from a superseded join of the
  same node name (issue #421). A legacy peer's frames still parse, but the
  protection is complete only once every node and discovery replica has
  upgraded.
- Sets applied by handoff (`U`) no longer count toward the client-visible
  `sets_total` and `cas_sets_total` metrics; scale events do not inflate them
  (issue #394). Staged-join transfers still arrive as ordinary `S` frames and
  are counted (the metric's help text says so).

### Fixed

- Discovery serves every node parked during the startup grace once it ends,
  not just the first (issue #113).
- A node reverts its ring view when a completed join is later abandoned,
  including implicit abandonment by the next `M` (issues #93, #218), so it no
  longer answers `W` for keys of a joiner that never took them over.
- Writes during a handoff or decommission are no longer lost or reordered:
  batched `o` writes are forwarded to the joining or leaving node (issue #176;
  a later fix covers a join starting while the write was in progress), a write dropped when the forward channel was full is queued instead
  (bounded at 4096 waiters), forwards to one key are applied in order, and
  forwards queued behind a permanently failed target make one attempt until it
  answers.
- Forwards are dropped once their handoff is revoked (eviction of the joiner,
  abandoned join, superseded `M`), so a backlog no longer lands on a process
  that was handed the dead node's recycled IP (issue #474). The forwarding
  window also closes as soon as discovery evicts the joiner.
- Node: `M` retries for a crash-restarted joiner under the same name are told
  apart by join generation instead of being answered as duplicates, which had
  stalled the join until its timeout.
- Decommission now authenticates to discovery with discovery's own ack (`Od`);
  with `NANOCACHED_AUTH_SECRET` set, scale-in handoffs used to be abandoned
  and the leaver's entries lost (found in the ECS verification).
- Decommission, re-replication and abandon waits stop at shutdown, and a
  second SIGTERM during a drain exits promptly and still sends `V`.
  The migration wait is capped at min(drain budget / 4, 5 s), and transfer
  retries back off 100 ms times the attempt number.
- Re-replication after consecutive evictions no longer stalls: every
  surviving old owner may send, not only the top-ranked survivor (issue #405).
  A re-replication put-if-absent that arrives mid-handoff is relayed to the
  joining owner (issue #266).
- Node heartbeat-ack read is bounded, so a hostile discovery cannot exhaust
  memory (issue #92). `fetch_roster` and discovery line reads are bounded by
  one overall deadline rather than per byte (issues #216, #217).
- Discovery: handoff-complete reports from a superseded join generation are
  rejected (issue #421), and generations are seeded from the clock so a
  restarted discovery cannot reissue old ones.
- Discovery: an explicit `V` from a joining or waiting node now abandons the
  in-flight join and wakes a parked promotion wait, instead of stalling later
  joins and leaking a connection slot until the migration timeout (issues #297,
  #323).
- Discovery performance and bounds: the `L`, `H` and `T` responses are cached
  per roster change (issues #95, #298, #356), the registry fan-out is bounded
  (issue #299), the roster is no longer deep-cloned per ready node (issue
  #409), and duplicate `J` reconnects are not rejected by the request size
  check (issue #421).
- Proxy correctness: `INCR`, `CAS` and `CAD` are replayed only when the
  request provably never reached the wire (issues #272, #293, #322).
  Multi-get retry falls back to replicas, not only the fresh primary (issue
  #221). The proxy no longer exits on a transient accept error (issue #271),
  prunes backends for addresses that left the roster (issue #220, which
  otherwise grew without bound under autoscaling), and sets `TCP_NODELAY` on
  client and backend connections (about 40 ms stalls on pipelined bursts over
  a real network).
- Proxy DoS bounds: backend and client writes, the client TLS handshake and
  the busy reply have timeouts, so a client that stops reading can no longer
  hold a connection slot forever; discovery replicas are queried concurrently.
- Node and proxy: the header-terminating newline scan is cached per
  connection, so a header trickled in byte by byte no longer costs O(n^2)
  CPU, reachable before authentication.
- Accept loops on the node, discovery and proxy metrics listeners back off on
  fd exhaustion (issue #184), and all three bound concurrent metrics
  connections (issues #327, #358).
- Constant-time comparison for CAS digests (issue #336) and proxy tokens
  (issue #183).
- `HashRing` deduplicates repeated node names, so `owners` and `is_owner` can
  no longer disagree during a migration (issue #328).
- A new namespace's map key is copied, so it can no longer pin a whole
  pipelined receive buffer without being charged to memory (issue #406).
- Migration marks are accounted to the owning namespace, so the per-namespace
  `/metrics` rows sum to the global figure and `--namespace-budget` accounts
  them (found in the seventh-pass audit).

## [0.3.0] - 2026-08-22

Aligned release of the server and all six SDKs. It fixes the cluster
data-loss and availability bugs found in the 0.2.0 end-to-end run (issues
#61 to #63, #66).

### Changed

- **Breaking: the discovery heartbeat acknowledgment now carries the current
  member roster** (`A <count> <r>` followed by `L`-shaped entries; a bare `A`
  during the startup grace or a replication-factor dispute) instead of a bare
  `A`. Nodes adopt their primary replica's roster whenever it differs from
  their current view. **Upgrade nodes before discovery servers:** a new node
  accepts either form, an old node redials on every heartbeat when handed the
  new one (issue #61).
- Failed auth handshakes with discovery log their cause, and a peer closing
  without TLS `close_notify` logs at INFO instead of one WARN per
  disconnect (issue #68).

### Fixed

- After an eviction, surviving nodes kept routing the dead node's keys to it:
  `W` for every such key at R=1, and writes one copy short at R>1. Each
  node now refreshes its ring view from the heartbeat ack (issue #61).
- Back-to-back or concurrent joins no longer lose keys. A source that
  finished its handoff accepts the next `M` while forwarding (the `M`'s
  `joined` roster says whether the previous join completed or was abandoned),
  and dead copies are kept until discovery confirms the join (issue #62).
- Discovery holds joins until its startup grace has ended; a `J` accepted
  during the grace parks in `Waiting` and starts once, so a restarted
  discovery no longer hands off from only the members that had re-announced
  (issue #63).
- Reads during a join: a displaced key is served locally only while the
  handoff is still forwarding and unconfirmed; once discovery confirms the
  join the old owner answers `W`, so a stale client lands on the joiner
  instead of missing (issue #66).

## [0.2.0] - 2026-08-22

First aligned release of the server and all six SDKs at one version. 0.1.x
were pipeline-validation releases; as a pre-1.0 line, breaking changes ship
without a deprecation cycle and are listed here.

### Added

- Baseline feature set: `G`/`S`/`D` with optional TTL, shared-secret
  authentication (`A`), optional TLS on every connection, pipelining, echoed
  response tags (tagged mode) so a pipeline cannot desynchronize, idle
  timeouts, and request-size and connection limits (256 connections per
  source IP).
- Memory-bounded cache with LRU eviction, `--max-memory <bytes>` (default
  256 MiB, minimum 1 MiB).
- Cluster mode through `nanocached-discovery`: nodes register with
  `--discovery`, keys are placed by rendezvous hashing and replicated to the
  top R nodes (`--replication-factor`, default 2), joins migrate keys in a
  staged handoff, and discovery runs as soft-state replicas (HA). Membership
  commands between nodes and discovery are authenticated with a per-node
  token.
- Container images for `nanocached-node` and `nanocached-discovery`
  (separate Dockerfile targets).

[Unreleased]: https://github.com/nanocached/nanocached/compare/server/v0.4.5...HEAD
[0.4.8]: https://github.com/nanocached/nanocached/compare/server/v0.4.7...server/v0.4.8
[0.4.7]: https://github.com/nanocached/nanocached/compare/server/v0.4.6...server/v0.4.7
[0.4.6]: https://github.com/nanocached/nanocached/compare/server/v0.4.5...server/v0.4.6
[0.4.5]: https://github.com/nanocached/nanocached/compare/server/v0.4.4...server/v0.4.5
[0.4.4]: https://github.com/nanocached/nanocached/compare/server/v0.4.3...server/v0.4.4
[0.4.3]: https://github.com/nanocached/nanocached/compare/server/v0.4.2...server/v0.4.3
[0.4.2]: https://github.com/nanocached/nanocached/compare/server/v0.4.1...server/v0.4.2
[0.4.1]: https://github.com/nanocached/nanocached/compare/server/v0.4.0...server/v0.4.1
[0.4.0]: https://github.com/nanocached/nanocached/compare/server/v0.3.0...server/v0.4.0
[0.3.0]: https://github.com/nanocached/nanocached/compare/server/v0.2.0...server/v0.3.0
[0.2.0]: https://github.com/nanocached/nanocached/releases/tag/server/v0.2.0
