use crate::key::Key;
use bytes::Bytes;
use lru::LruCache;
use nanocached::infra::constant_time_eq;
use rustc_hash::FxHashMap;
use std::collections::hash_map::RandomState;
use std::collections::{HashMap, HashSet, VecDeque};
use std::time::{Duration, Instant};

/// Caps how many entries a single `sweep` call removes. Scanning for
/// expired/marked entries is cheap even over a large cache (~4ms/1M
/// entries, measured), but each removal itself is not (~500ns/entry,
/// measured — `LruCache::pop` unlinks from both a hash map and a linked
/// list) — sweeping hundreds of thousands of entries in one call has been
/// measured to take 100ms+, which would stall every other command queued
/// behind it on the single-threaded cache actor for that long. Chunking
/// removals lets client commands interleave between `sweep` calls instead.
pub(crate) const SWEEP_BUDGET: usize = 2_000;

/// Added to `used_bytes` per stored entry, on top of its key+value bytes,
/// to approximate the `HashMap` bucket and intrusive LRU list node
/// `LruCache` allocates for it — invisible to plain key+value accounting,
/// but real RSS a small-value workload pays for every entry. A rough,
/// documented estimate rather than a measured constant (issue #19); if a
/// closer figure is measured later, this is the only place to change it.
pub(crate) const ENTRY_OVERHEAD_BYTES: usize = 100;

struct Entry {
    value: Bytes,
    expires_at: Option<Instant>,
    /// `Cache::clock` at this entry's last `set`/`get` — what orders the
    /// per-namespace LRU tails against each other for eviction (see
    /// `evict_one`).
    last_used: u64,
}

/// One namespace's entries (issue #105). Keyed with the std default
/// `RandomState` (SipHash, seeded randomly per process) rather than a
/// fast fixed hasher like FxHash: cache keys are fully attacker-controlled,
/// and a non-randomized hash lets a client precompute colliding keys
/// offline and degrade every lookup to O(n) — a hash-flooding
/// CPU-exhaustion DoS. This is the same reason std's HashMap defaults to
/// SipHash.
type Entries = LruCache<Bytes, Entry, RandomState>;

/// Charged to `Cache::used_bytes` once per live *non-default* namespace,
/// on top of the namespace name's own length: the slot, the index entry
/// and the `LruCache`'s own allocations, invisible to per-entry
/// accounting. Without it a stream of `s`/`o` frames naming fresh large
/// namespaces (a name may be ~1 MiB) with a 1-byte key and value was
/// accounted ~102 bytes apiece while RSS grew by the whole name, so
/// `--max-memory` never evicted. A rough, documented estimate like
/// `ENTRY_OVERHEAD_BYTES`. The default (empty) namespace is a single
/// process-lifetime sub-map nobody can multiply, so it is not charged.
pub(crate) const NAMESPACE_OVERHEAD_BYTES: usize = 256;

/// What a live namespace named `name` costs `Cache::used_bytes`.
fn name_charge(name: &[u8]) -> usize {
    if name.is_empty() {
        0
    } else {
        name.len() + NAMESPACE_OVERHEAD_BYTES
    }
}

/// One namespace's sub-map plus its own byte accounting, so `clear`
/// (issue #106) can drop the whole thing and credit `Cache::used_bytes`
/// in O(1) without walking the entries.
struct Namespace {
    /// The namespace's own allocation (never a slice of a request frame,
    /// issue #406). Shared with the `Cache::namespaces` index key by a
    /// refcount bump, so the name is stored — and charged — once.
    name: Bytes,
    entries: Entries,
    /// This namespace's share of `Cache::used_bytes`: key + value +
    /// `ENTRY_OVERHEAD_BYTES` per entry, plus the duplicate key bytes of
    /// any of this namespace's entries currently `migrated`-marked
    /// (`mark_migrated`/`clear_migrated_mark` credit/debit this alongside
    /// the global `Cache::used_bytes`, so the per-namespace rows stay
    /// consistent with the total mid-migration — see `mark_migrated`'s
    /// own doc comment). Excludes the name charge (`name_charge`), so
    /// `--namespace-budget` semantics are unchanged.
    used_bytes: usize,
    /// Issue #127: this namespace's `--namespace-budget`, resolved once at
    /// creation (budgets never change after construction) so the
    /// per-write budget check needs no lookup keyed by the name.
    budget: Option<usize>,
    /// This namespace's entries handed off during a staged join, by key
    /// name, awaiting `sweep`. Kept per namespace rather than in one
    /// global `HashSet<Key>` so no mark lookup ever hashes the (up to
    /// ~1 MiB) namespace name.
    migrated: HashSet<Bytes, RandomState>,
}

impl Namespace {
    fn new(name: Bytes, budget: Option<usize>) -> Self {
        Self {
            name,
            entries: LruCache::unbounded_with_hasher(RandomState::new()),
            used_bytes: 0,
            budget,
            migrated: HashSet::with_hasher(RandomState::new()),
        }
    }
}

/// Issue #124: `Cache::stats`'s snapshot.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CacheStats {
    pub used_bytes: usize,
    pub max_memory_bytes: usize,
    pub entries: usize,
    pub hits: u64,
    pub misses: u64,
    pub sets: u64,
    pub deletes: u64,
    pub evictions: u64,
    pub expirations: u64,
    /// Issue #129: successful `INCR` operations (a stored value that
    /// wasn't INCR's decimal-ASCII grammar, or an overflowing `delta`,
    /// isn't counted here — see `Cache::incr`).
    pub incrs: u64,
    /// Issue #141: successful `k` (compare-and-set) writes. A mismatched
    /// condition isn't counted here — see `Cache::cas_set`.
    pub cas_sets: u64,
    /// Issue #141: successful `x` (compare-and-delete) removals. A
    /// mismatched or absent key isn't counted here — see
    /// `Cache::cas_delete`.
    pub cas_deletes: u64,
    /// Largest first; one row per live namespace.
    pub namespaces: Vec<NamespaceStats>,
}

/// Issue #124: one namespace's share, as `Cache::stats` reports it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NamespaceStats {
    pub namespace: Bytes,
    pub entries: usize,
    pub used_bytes: usize,
    /// Issue #127: this namespace's `--namespace-budget`, if one is set.
    pub budget_bytes: Option<usize>,
}

pub struct Cache {
    /// Index from namespace name to its slot in `slots`. One sub-map per
    /// namespace — the default namespace lives under the empty key (issue
    /// #105). A sub-map exists exactly while it holds at least one entry,
    /// so the per-eviction scan over namespaces (`evict_one`) is bounded
    /// by the number of *live* namespaces, and `CLEAR <ns>` (issue #106)
    /// is a single O(1) sub-map drop. Same `RandomState` reasoning as
    /// `Entries`: namespace names are attacker-controlled too.
    ///
    /// Namespaces are addressed by slot index once resolved, because a
    /// name can be ~1 MiB and every lookup *by name* hashes and compares
    /// all of it: a multi-key frame (`m`/`o`) resolves its namespace once
    /// (`find_namespace`) and then works by index.
    namespaces: HashMap<Bytes, u64, RandomState>,
    /// Namespace storage, addressed by the ids in `namespaces`. Ids are
    /// handed out from `next_slot_id` and never reused, so an id held
    /// across a namespace's removal can only miss, never alias another
    /// namespace. Ids are sequential, not attacker-chosen, so a fast
    /// non-randomized hasher is safe here.
    slots: FxHashMap<u64, Namespace>,
    next_slot_id: u64,
    /// The slot `find_namespace` resolved last. A lookup first checks
    /// whether the incoming name *is* that slot's own allocation (same
    /// pointer and length — only keys cloned out of this cache, such as
    /// `keys()`'s, can be), which makes the loops that walk those keys
    /// (migration, sweep) hash nothing.
    last_slot: std::cell::Cell<u64>,
    /// Total `migrated` marks across namespaces.
    marked: usize,
    entry_count: usize,
    used_bytes: usize,
    max_memory_bytes: usize,
    /// Ticks once per `set`/`get`; stamps `Entry::last_used`.
    clock: u64,
    /// Issue #127: per-namespace memory budgets (`--namespace-budget`).
    /// A budget is a *cap*, not a reservation: a namespace over its
    /// budget evicts from itself (its own LRU order) before the write
    /// returns, so a churny namespace can't grow past its cap and evict
    /// everyone else — but the global bound stays authoritative, and the
    /// global LRU (`evict_one`) still picks the overall-oldest entry
    /// regardless of budgets. Protecting a small, precious namespace is
    /// therefore done by capping the big churny ones, not by reserving
    /// for the small one. Keyed independently of `namespaces` — a budget
    /// outlives its (empty-and-dropped) sub-map.
    budgets: HashMap<Bytes, usize, RandomState>,
    /// Issue #124: operation counters, snapshotted by `stats()` for the
    /// metrics endpoint. Plain fields, not atomics — the cache actor is
    /// single-threaded. Issue #394: `U` handoff writes bypass these (see
    /// `handoff_set`); `sets` still includes staged-join migration and
    /// any other internal write delivered as an ordinary `S` frame,
    /// which this node cannot tell apart from a client's — the exported
    /// HELP text says so.
    hits: u64,
    misses: u64,
    sets: u64,
    deletes: u64,
    /// Entries removed by the memory bound (`evict_one`).
    evictions: u64,
    /// Entries removed because their TTL had passed — lazily on access
    /// or proactively by the sweep. Migration-mark reclaims are internal
    /// bookkeeping and deliberately not counted here.
    expirations: u64,
    /// Issue #129: successful `INCR` operations — see `CacheStats::incrs`.
    incrs: u64,
    /// Issue #141: successful `k` writes — see `CacheStats::cas_sets`.
    cas_sets: u64,
    /// Issue #141: successful `x` removals — see `CacheStats::cas_deletes`.
    cas_deletes: u64,
    /// Expired/marked keys queued for removal by `sweep`, in
    /// `SWEEP_BUDGET`-sized bites. Refilled (by scanning the namespaces for
    /// expired keys and collecting their `migrated` marks) only once this drains empty,
    /// so an in-progress sweep pass isn't rescanned from scratch every
    /// call.
    pending_removal: VecDeque<Key>,
    /// Test-only: how many namespace lookups went by name (hash + compare
    /// of the whole name) — what a multi-key frame must do once, not per
    /// key.
    #[cfg(test)]
    name_lookups: std::cell::Cell<usize>,
}

impl Entry {
    fn is_expired_at(&self, now: Instant) -> bool {
        self.expires_at.is_some_and(|expires_at| expires_at <= now)
    }
}

/// `Cache::incr`'s outcome. `Value` carries the entry's remaining TTL
/// alongside the new value — never put on the wire (see
/// `Response::Incremented`), but needed by `src/server.rs`'s `Incr`
/// connection handler to forward the correct TTL to a migration/
/// decommission target.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IncrResult {
    Value(i64, Option<Duration>),
    /// The key doesn't exist — never created by `incr` (unlike some
    /// memcached-alikes' optional "start from N"), and deliberately
    /// indistinguishable from a key eviction or TTL reclaimed: both are
    /// "no counter here right now" to the caller.
    NotFound,
    /// The key exists, but its stored value isn't INCR's canonical
    /// decimal-ASCII `i64` (see `parse_decimal_i64`), or applying `delta`
    /// would overflow `i64`. Distinct from `NotFound` so a caller (e.g.
    /// the Django adapter) can tell "no such key" from "wrong type" apart.
    NotNumeric,
}

/// Issue #141: `k`'s (and `x`'s) `<cond>` field, already decoded by
/// `command.rs`'s parser — `cache.rs` never sees the wire token, only the
/// decoded condition.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CasCondition {
    /// Wire `A`: succeeds only if the key is absent — including lazily
    /// expired, indistinguishable from never having been set (same
    /// "eviction and never-existed look the same" rule `IncrResult::NotFound`
    /// follows).
    Absent,
    /// Wire `P`: succeeds only if the key currently holds any
    /// (unexpired) value, regardless of what it is.
    Present,
    /// Wire: a 32-hex-digit digest. Succeeds only if the key holds an
    /// unexpired value whose `content_digest` equals this exactly.
    Digest([u8; 16]),
}

/// `Cache::cas_set`'s outcome.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CasResult {
    /// The condition held; the new value is now stored.
    Stored,
    /// The condition did not hold; nothing changed.
    Mismatch,
}

/// Issue #141: CAS's content digest — SHA-256 of the exact bytes a key's
/// entry stores (the same bytes a `V`/`I` response body would carry: for a
/// compression-enabled client, that includes its marker byte, since the
/// server never decompresses), truncated to the first 16 bytes (128 bits).
/// Computed identically here and by every SDK — a fixed cross-language
/// test vector pins the agreement (see `docs/protocol.html#cas`); a
/// mismatch here would silently break CAS between languages sharing a
/// keyspace.
pub(crate) fn content_digest(value: &[u8]) -> [u8; 16] {
    use sha2::{Digest, Sha256};
    let hash = Sha256::digest(value);
    let mut digest = [0u8; 16];
    digest.copy_from_slice(&hash[..16]);
    digest
}

/// Issue #129: INCR's canonical decimal-ASCII integer grammar — shared
/// between the wire `<delta>` field (`command.rs`'s `i` parse arm) and a
/// stored counter value (`Cache::incr_at`, above), so the form INCR
/// itself writes back is always a form it accepts back: an optional
/// leading `-`, then ASCII digits with no leading zero (other than a
/// lone `0`). No `+`, no internal sign, no whitespace — an incremented
/// value's canonical form is unambiguous across all six SDKs.
pub(crate) fn parse_decimal_i64(bytes: &[u8]) -> Option<i64> {
    let text = std::str::from_utf8(bytes).ok()?;
    let (negative, digits) = match text.strip_prefix('-') {
        Some(rest) => (true, rest),
        None => (false, text),
    };

    if digits.is_empty() || !digits.bytes().all(|byte| byte.is_ascii_digit()) {
        return None;
    }
    if digits.len() > 1 && digits.starts_with('0') {
        return None;
    }

    let magnitude: u64 = digits.parse().ok()?;
    if negative {
        // i64::MIN's magnitude (9223372036854775808) has no positive i64
        // representation, so it can't go through `i64::try_from` and
        // negate like every other value — handled as the one special
        // case instead of losing it.
        if magnitude == i64::MIN.unsigned_abs() {
            Some(i64::MIN)
        } else {
            i64::try_from(magnitude).ok().map(|value| -value)
        }
    } else {
        i64::try_from(magnitude).ok()
    }
}

impl Cache {
    /// Test-only convenience since issue #127 made budgets part of
    /// construction — production goes through `with_budgets` (an empty
    /// flag list is just an empty budget list).
    #[cfg(test)]
    pub fn new(max_memory_bytes: usize) -> Self {
        Self::with_budgets(max_memory_bytes, Vec::new())
    }

    /// Issue #127: `new` plus per-namespace budgets — see
    /// `Cache::budgets`. Duplicate names keep the last value
    /// (`parse_args` already rejects duplicates; this is just the map's
    /// natural behavior).
    pub fn with_budgets(max_memory_bytes: usize, budgets: Vec<(Bytes, usize)>) -> Self {
        Self {
            namespaces: HashMap::with_hasher(RandomState::new()),
            slots: FxHashMap::default(),
            next_slot_id: 0,
            last_slot: std::cell::Cell::new(0),
            marked: 0,
            entry_count: 0,
            used_bytes: 0,
            max_memory_bytes,
            budgets: budgets.into_iter().collect(),
            clock: 0,
            hits: 0,
            misses: 0,
            sets: 0,
            deletes: 0,
            evictions: 0,
            expirations: 0,
            incrs: 0,
            cas_sets: 0,
            cas_deletes: 0,
            pending_removal: VecDeque::new(),
            #[cfg(test)]
            name_lookups: std::cell::Cell::new(0),
        }
    }

    pub fn set(&mut self, key: Key, value: Bytes) {
        self.sets += 1;
        self.insert(key, value, None);
    }

    pub fn set_with_ttl(&mut self, key: Key, value: Bytes, ttl: Duration) {
        // The TTL comes straight off the wire with no upper bound, so a huge
        // value (up to `u64::MAX` seconds) would overflow `Instant + Duration`
        // and panic — taking down the single cache actor and, with it, every
        // client's cache operations. A TTL too far out to represent is treated
        // as "never expires" (no expiry), which is the closest honest meaning.
        let expires_at = Instant::now().checked_add(ttl);
        self.sets += 1;
        self.insert(key, value, expires_at);
    }

    pub fn get(&mut self, key: &Key) -> Option<Bytes> {
        self.get_at(key, Instant::now())
    }

    /// Issue #129: `INCR` — reads the stored value as INCR's canonical
    /// decimal-ASCII `i64` (`parse_decimal_i64`), adds `delta`, and writes
    /// the result back through `insert` (the same accounting-safe
    /// overwrite path `set`/`set_with_ttl` use), preserving the entry's
    /// existing TTL rather than resetting it — an `S`/`s` always replaces
    /// the TTL (or clears it); `INCR` never does.
    ///
    /// Not atomic across a cluster (see `src/server.rs`'s `Incr` connection
    /// handler and the module docs on client-side replication for how a
    /// cluster stays consistent) — only against every other command on
    /// this node's single-threaded cache actor, which is where its
    /// atomicity comes from for free: no per-key lock, no CAS loop.
    ///
    /// Deliberately does *not* create a missing key (memcached's own
    /// `incr` doesn't either) — a key absent because it was never set and
    /// a key absent because eviction or TTL reclaimed it must look the
    /// same to the caller, so `IncrResult::NotFound` covers both.
    pub fn incr(&mut self, key: &Key, delta: i64) -> IncrResult {
        self.incr_at(key, delta, Instant::now())
    }

    fn incr_at(&mut self, key: &Key, delta: i64, now: Instant) -> IncrResult {
        let Some(entry) = self.peek(key) else {
            return IncrResult::NotFound;
        };

        if entry.is_expired_at(now) {
            self.remove_entry(key);
            self.expirations += 1;
            return IncrResult::NotFound;
        }

        let Some(current) = parse_decimal_i64(&entry.value) else {
            return IncrResult::NotNumeric;
        };

        let Some(new_value) = current.checked_add(delta) else {
            return IncrResult::NotNumeric;
        };

        // Extracted before the mutable borrow below; `entry` isn't touched
        // again after this point.
        let expires_at = entry.expires_at;
        let remaining_ttl = expires_at.map(|expires_at| expires_at.saturating_duration_since(now));

        self.incrs += 1;
        self.insert(key.clone(), Bytes::from(new_value.to_string()), expires_at);

        IncrResult::Value(new_value, remaining_ttl)
    }

    /// Issue #141: `k` — compare-and-set. Evaluates `condition` against
    /// the key's current (lazily-expired-first) value and, only on a
    /// match, overwrites it through `insert` (the same accounting-safe
    /// path `set`/`set_with_ttl`/`incr_at` share). `ttl` follows `Set`'s
    /// own convention exactly (`None` = no expiry) — unlike `incr_at`,
    /// the new value is supplied whole by the caller, so there is no old
    /// TTL to preserve.
    ///
    /// Not atomic across a cluster on its own — see `src/server.rs`'s
    /// `CasSet` connection handler for how a cluster stays consistent
    /// (the same "primary decides, forward the literal result" rule
    /// `Cache::incr`'s doc comment describes), and `docs/protocol.html#cas`
    /// for why LRU eviction still means CAS is not a distributed lock.
    pub fn cas_set(
        &mut self,
        key: &Key,
        condition: CasCondition,
        value: Bytes,
        ttl: Option<Duration>,
    ) -> CasResult {
        self.cas_set_at(key, condition, value, ttl, Instant::now())
    }

    /// Issue #394: `U` handoff writes (re-replication after an eviction,
    /// decommission drain, join relay) are cluster management, not client
    /// traffic — they used to funnel through `set`/`cas_set` and inflate
    /// the client-visible `sets`/`cas_sets` counters by the size of
    /// whatever internal transfer was in flight, contradicting the
    /// metrics HELP text and tripping rate-based alerting during routine
    /// scaling. These variants insert without touching any operation
    /// counter. (Staged-join migration transfer arrives as ordinary `S`
    /// frames from the source node and stays indistinguishable from a
    /// client write here — the `sets_total` HELP text documents that
    /// remainder instead.)
    pub fn handoff_set(&mut self, key: Key, value: Bytes, ttl: Option<Duration>) {
        // Same TTL-overflow stance as `set_with_ttl`: too far out to
        // represent is "never expires", not a panic.
        let expires_at = ttl.and_then(|ttl| Instant::now().checked_add(ttl));
        self.insert(key, value, expires_at);
    }

    /// The `if_absent` companion to [`Self::handoff_set`] (issue #266's
    /// put-if-absent re-replication), mirroring `cas_set_at` with
    /// `CasCondition::Absent` minus the `cas_sets` bump — no client sent
    /// a `k` frame for this write.
    pub fn handoff_set_if_absent(
        &mut self,
        key: &Key,
        value: Bytes,
        ttl: Option<Duration>,
    ) -> CasResult {
        let now = Instant::now();
        if !self.condition_holds_at(key, CasCondition::Absent, now) {
            return CasResult::Mismatch;
        }
        let expires_at = ttl.and_then(|ttl| now.checked_add(ttl));
        self.insert(key.clone(), value, expires_at);
        CasResult::Stored
    }

    fn cas_set_at(
        &mut self,
        key: &Key,
        condition: CasCondition,
        value: Bytes,
        ttl: Option<Duration>,
        now: Instant,
    ) -> CasResult {
        if !self.condition_holds_at(key, condition, now) {
            return CasResult::Mismatch;
        }

        // Same overflow handling as `set_with_ttl`: a TTL too far out to
        // represent as an `Instant` is treated as "never expires" rather
        // than panicking the single cache actor.
        let expires_at = ttl.and_then(|ttl| now.checked_add(ttl));
        self.cas_sets += 1;
        self.insert(key.clone(), value, expires_at);
        CasResult::Stored
    }

    /// Issue #141: `x` — compare-and-delete. `command.rs`'s parser only
    /// ever produces `CasCondition::Digest` for `x` (`A`/`P` are rejected
    /// as a fatal parse error there — deleting on "absent" or "any
    /// present value" is already the plain, unconditional `d`), but this
    /// takes the decoded digest directly rather than re-asserting that
    /// here.
    pub fn cas_delete(&mut self, key: &Key, expected: [u8; 16]) -> bool {
        self.cas_delete_at(key, expected, Instant::now())
    }

    fn cas_delete_at(&mut self, key: &Key, expected: [u8; 16], now: Instant) -> bool {
        if !self.condition_holds_at(key, CasCondition::Digest(expected), now) {
            return false;
        }
        self.cas_deletes += 1;
        self.remove_entry(key);
        true
    }

    /// Shared by `cas_set_at`/`cas_delete_at`: lazily expires the entry
    /// first (an expired entry is "absent" to every condition, same as
    /// `get_at`/`incr_at`), then checks `condition` against whatever is
    /// left.
    fn condition_holds_at(&mut self, key: &Key, condition: CasCondition, now: Instant) -> bool {
        if self.peek(key).is_some_and(|entry| entry.is_expired_at(now)) {
            self.remove_entry(key);
            self.expirations += 1;
        }

        match (condition, self.peek(key)) {
            (CasCondition::Absent, None) => true,
            (CasCondition::Absent, Some(_)) => false,
            (CasCondition::Present, Some(_)) => true,
            (CasCondition::Present, None) => false,
            (CasCondition::Digest(expected), Some(entry)) => {
                // Issue #336: was a plain `==`. A CAS digest is derived
                // from the value the caller must already be able to read
                // (a `G` on the same key, or its own prior write) rather
                // than a secret this comparison alone protects, but a
                // byte-at-a-time `==` still leaks, via timing, how many
                // leading bytes of a guessed digest matched — the same
                // reasoning `constant_time_eq` already exists for on
                // every secret/token comparison in `server.rs`.
                constant_time_eq(&content_digest(&entry.value), &expected)
            }
            (CasCondition::Digest(_), None) => false,
        }
    }

    pub fn delete(&mut self, key: &Key) -> bool {
        self.delete_at(key, Instant::now())
    }

    /// Total entries across every namespace.
    #[cfg(test)]
    pub fn len(&self) -> usize {
        self.entry_count
    }

    fn tick(&mut self) -> u64 {
        self.clock += 1;
        self.clock
    }

    /// The slot of `name`'s sub-map, if it is live. A lookup by name hashes
    /// and compares the whole name (up to ~1 MiB), so it is done once per
    /// frame by the multi-key paths and never per key; the check against
    /// `last_slot` keeps loops over keys cloned out of this cache
    /// (`keys()`) from paying it either.
    fn find_namespace(&self, name: &Bytes) -> Option<u64> {
        let hint = self.last_slot.get();
        if self.slots.get(&hint).is_some_and(|namespace| {
            // Same allocation and length means the same bytes.
            namespace.name.as_ptr() == name.as_ptr() && namespace.name.len() == name.len()
        }) {
            return Some(hint);
        }

        #[cfg(test)]
        self.name_lookups.set(self.name_lookups.get() + 1);
        let id = self.namespaces.get(&name[..]).copied()?;
        self.last_slot.set(id);
        Some(id)
    }

    /// `find_namespace`, creating the (empty) sub-map if there is none yet
    /// and charging its name to `used_bytes` (see `NAMESPACE_OVERHEAD_BYTES`).
    fn ensure_namespace(&mut self, name: &Bytes) -> u64 {
        if let Some(id) = self.find_namespace(name) {
            return id;
        }

        // Issue #406: `name` is sliced zero-copy out of the request frame by
        // the parser (like the key name and value), so using it as-is for a
        // *fresh* namespace would pin the whole pipelined receive-buffer
        // chunk alive for as long as the namespace exists — uncharged to
        // `used_bytes`, same class of bug the value/key re-copies in
        // `insert_into` avoid. An already-known namespace is looked up
        // first so the common case doesn't pay for a copy it doesn't need.
        let name = Bytes::copy_from_slice(name);
        let budget = self.budgets.get(&name).copied();
        let id = self.next_slot_id;
        self.next_slot_id += 1;

        self.used_bytes += name_charge(&name);
        self.namespaces.insert(name.clone(), id);
        self.slots.insert(id, Namespace::new(name, budget));
        self.last_slot.set(id);
        id
    }

    /// Removes the (empty) sub-map in slot `id` and releases its name
    /// charge.
    fn drop_namespace(&mut self, id: u64) {
        let namespace = self
            .slots
            .remove(&id)
            .expect("the namespace being dropped is live");
        self.namespaces.remove(&namespace.name[..]);
        self.used_bytes -= name_charge(&namespace.name);
    }

    /// The shared accounting-safe overwrite path for every write: `set`/
    /// `set_with_ttl` (which bump `self.sets`) and `incr_at` (which bumps
    /// `self.incrs` instead — see its own doc comment for why counting an
    /// INCR as a `sets_total` GET/SET-shaped write would be misleading).
    fn insert(&mut self, key: Key, value: Bytes, expires_at: Option<Instant>) {
        let id = self.ensure_namespace(&key.namespace);
        self.insert_into(id, key.name, value, expires_at);
    }

    /// `insert` into an already-resolved namespace (`ensure_namespace`):
    /// what a multi-key frame calls per key so the namespace name is
    /// resolved once for the whole frame. Eviction can never remove
    /// namespace `id` itself here — it always holds the entry just written,
    /// which is its most-recently-used and so never the one evicted.
    fn insert_into(&mut self, id: u64, name: Bytes, value: Bytes, expires_at: Option<Instant>) {
        // Entries stored long-term must not keep a shared receive-buffer
        // chunk (which may span an entire pipelined batch) alive just to
        // retain a few bytes of it, so re-copy into right-sized allocations
        // here, where the invariant is actually enforced for every caller.
        let value = Bytes::copy_from_slice(&value);

        let value_len = value.len();
        let last_used = self.tick();
        let entry = Entry {
            value,
            expires_at,
            last_used,
        };

        let namespace = self
            .slots
            .get_mut(&id)
            .expect("the namespace was resolved just before this write");

        // A fresh write is not the value a handoff transferred: a stale
        // `migrated` mark left over from an earlier value must not condemn
        // this one to the next sweep (it would silently delete it).
        Self::release_mark(namespace, &name, &mut self.used_bytes, &mut self.marked);

        // An overwrite keeps the stored key (`LruCache::put` would too, and
        // discard the copy), so only copy the key for a genuinely new
        // entry. `get_mut` promotes to most-recently-used like `put`.
        if let Some(existing) = namespace.entries.get_mut(&name[..]) {
            let replaced = std::mem::replace(existing, entry);
            let delta = value_len as isize - replaced.value.len() as isize;
            namespace.used_bytes = namespace.used_bytes.wrapping_add_signed(delta);
            self.used_bytes = self.used_bytes.wrapping_add_signed(delta);
        } else {
            let name = Bytes::copy_from_slice(&name);
            let entry_bytes = name.len() + value_len + ENTRY_OVERHEAD_BYTES;
            namespace.entries.put(name, entry);
            namespace.used_bytes += entry_bytes;
            self.entry_count += 1;
            self.used_bytes += entry_bytes;
        }

        // Issue #127: a budgeted namespace pays for its own growth first
        // — evicted from its *own* LRU order — so the global loop below
        // never has to make an innocent namespace pay for this one's
        // churn. Runs on overwrites too (a grown value can breach the
        // budget just like a new entry).
        self.enforce_namespace_budget(id);

        // Evict least-recently-used entries until the cache fits its memory
        // budget, but never evict the entry just inserted above: it is
        // always the most-recently-used one, so `evict_one` would only
        // reach it once nothing else is left.
        while self.used_bytes > self.max_memory_bytes && self.entry_count > 1 {
            self.evict_one();
        }
    }

    /// Batched `set`/`set_with_ttl` for one namespace (a multi-key `o`
    /// frame): the namespace's name is hashed once for the whole batch
    /// instead of once per key, which for a ~1 MiB name made one frame of
    /// tiny keys cost gigabytes of hashing on the single cache actor. Each
    /// item is otherwise exactly one `set`/`set_with_ttl` (same counters,
    /// accounting and eviction).
    pub fn set_many(
        &mut self,
        namespace: &Bytes,
        items: impl IntoIterator<Item = (Bytes, Bytes)>,
        ttl: Option<Duration>,
    ) {
        // Same TTL-overflow stance as `set_with_ttl`.
        let expires_at = ttl.and_then(|ttl| Instant::now().checked_add(ttl));
        let mut id = None;

        for (name, value) in items {
            self.sets += 1;
            // Re-validated per key, as a stale id would otherwise panic;
            // a slot id is never reused, so this is one integer lookup.
            let live = match id {
                Some(id) if self.slots.contains_key(&id) => id,
                _ => self.ensure_namespace(namespace),
            };
            id = Some(live);
            self.insert_into(live, name, value, expires_at);
        }
    }

    /// Batched `get` for one namespace (a multi-key `m` frame), resolving
    /// the namespace once — see `set_many`. Replies in `names` order.
    /// Once the values returned so far reach `max_value_bytes`, the
    /// remaining names are answered `None` without being looked up (no
    /// recency or hit/miss effect), and a hit that would push the total
    /// past it is answered `None` too (but is still counted as a hit).
    pub fn get_many(
        &mut self,
        namespace: &Bytes,
        names: &[Bytes],
        max_value_bytes: usize,
    ) -> Vec<Option<Bytes>> {
        let now = Instant::now();
        let id = self.find_namespace(namespace);
        let mut value_bytes: usize = 0;
        let mut results = Vec::with_capacity(names.len());

        for name in names {
            if value_bytes >= max_value_bytes {
                results.push(None);
                continue;
            }

            let last_used = self.tick();
            let value = self.get_in(id, name, now, last_used);
            match value {
                Some(value) if value_bytes + value.len() <= max_value_bytes => {
                    value_bytes += value.len();
                    results.push(Some(value));
                }
                Some(_) | None => results.push(None),
            }
        }

        results
    }

    /// Issue #127: evicts this namespace's least-recently-used entries
    /// until it fits its `--namespace-budget`, if it has one. Mirrors the
    /// global loop's one-entry floor: the entry just inserted is its
    /// namespace's most-recently-used, so it survives even when it alone
    /// exceeds the budget (exactly how a single oversized entry is
    /// allowed to exceed `--max-memory`).
    fn enforce_namespace_budget(&mut self, id: u64) {
        loop {
            let Some(namespace) = self.slots.get(&id) else {
                return;
            };
            let Some(budget) = namespace.budget else {
                return;
            };
            if namespace.used_bytes <= budget || namespace.entries.len() <= 1 {
                return;
            }
            self.evict_one_from(id);
        }
    }

    /// Removes the least-recently-used entry across *all* namespaces: each
    /// sub-map keeps its own recency order, so the global LRU victim is
    /// the oldest of the sub-maps' tails by `Entry::last_used`. O(number
    /// of live namespaces) per eviction — a handful for the framework
    /// named-cache workloads namespaces exist for (issue #105), and only
    /// ever paid while over the memory bound. Each live namespace now
    /// costs its name plus `NAMESPACE_OVERHEAD_BYTES` against that bound,
    /// so the count is itself bounded by `--max-memory`.
    fn evict_one(&mut self) {
        let victim = self
            .slots
            .iter()
            .filter_map(|(id, namespace)| {
                namespace
                    .entries
                    .peek_lru()
                    .map(|(_, entry)| (entry.last_used, *id))
            })
            .min_by_key(|(last_used, _)| *last_used)
            .map(|(_, id)| id)
            .expect("entry_count > 1 guarantees an entry to evict");

        self.evict_one_from(victim);
    }

    /// Removes namespace `id`'s least-recently-used entry — the shared
    /// tail of the global `evict_one` (which picks the victim namespace
    /// first) and the per-namespace budget loop (issue #127, where the
    /// victim namespace is the one over its budget).
    fn evict_one_from(&mut self, id: u64) {
        self.evictions += 1;
        let namespace = self
            .slots
            .get_mut(&id)
            .expect("the victim namespace exists");
        let (evicted_name, evicted_entry) = namespace
            .entries
            .pop_lru()
            .expect("the victim namespace is non-empty");
        let entry_bytes = evicted_name.len() + evicted_entry.value.len() + ENTRY_OVERHEAD_BYTES;
        namespace.used_bytes -= entry_bytes;
        self.entry_count -= 1;
        self.used_bytes -= entry_bytes;
        // The marked value is gone; a future entry under this key is a
        // different value and must not inherit the mark.
        Self::release_mark(
            namespace,
            &evicted_name,
            &mut self.used_bytes,
            &mut self.marked,
        );
        if namespace.entries.is_empty() {
            self.drop_namespace(id);
        }
    }

    fn get_at(&mut self, key: &Key, now: Instant) -> Option<Bytes> {
        let last_used = self.tick();
        let id = self.find_namespace(&key.namespace);
        self.get_in(id, &key.name, now, last_used)
    }

    /// `get_at` against an already-resolved namespace (`None`: no such
    /// namespace, so a miss). `last_used` is the tick the caller took for
    /// this lookup.
    fn get_in(
        &mut self,
        id: Option<u64>,
        name: &[u8],
        now: Instant,
        last_used: u64,
    ) -> Option<Bytes> {
        let Some(entry) = id
            .and_then(|id| self.slots.get_mut(&id))
            .and_then(|namespace| namespace.entries.get_mut(name))
        else {
            self.misses += 1;
            return None;
        };

        if entry.is_expired_at(now) {
            self.remove_entry_in(id.expect("an entry was found in it"), name);
            self.expirations += 1;
            self.misses += 1;
            return None;
        }

        entry.last_used = last_used;
        self.hits += 1;
        Some(entry.value.clone())
    }

    fn delete_at(&mut self, key: &Key, now: Instant) -> bool {
        let expired = self.peek(key).is_some_and(|entry| entry.is_expired_at(now));

        if expired {
            self.remove_entry(key);
            return false;
        }

        self.remove_entry(key).is_some()
    }

    /// `LruCache::peek` through the namespace: no recency change.
    fn peek(&self, key: &Key) -> Option<&Entry> {
        let id = self.find_namespace(&key.namespace)?;
        self.slots.get(&id)?.entries.peek(&key.name[..])
    }

    #[cfg(test)]
    fn contains(&self, key: &Key) -> bool {
        self.peek(key).is_some()
    }

    fn remove_entry(&mut self, key: &Key) -> Option<Entry> {
        let id = self.find_namespace(&key.namespace)?;
        self.remove_entry_in(id, &key.name)
    }

    fn remove_entry_in(&mut self, id: u64, name: &[u8]) -> Option<Entry> {
        let namespace = self.slots.get_mut(&id)?;
        let entry = namespace.entries.pop(name)?;
        let entry_bytes = name.len() + entry.value.len() + ENTRY_OVERHEAD_BYTES;
        namespace.used_bytes -= entry_bytes;
        self.entry_count -= 1;
        self.used_bytes -= entry_bytes;
        // The mark referred to this entry's value; whatever is stored
        // under the key later is a different value.
        Self::release_mark(namespace, name, &mut self.used_bytes, &mut self.marked);
        if namespace.entries.is_empty() {
            self.drop_namespace(id);
        }
        Some(entry)
    }

    /// Removes any mark for `name` from `namespace`, crediting `used_bytes`
    /// (and the namespace's own share) back for the duplicate key copy
    /// `mark_migrated` stored there — a no-op, memory accounting included,
    /// if `name` wasn't marked.
    fn release_mark(
        namespace: &mut Namespace,
        name: &[u8],
        used_bytes: &mut usize,
        marked: &mut usize,
    ) {
        if namespace.migrated.is_empty() {
            return;
        }
        if namespace.migrated.remove(name) {
            let mark_bytes = namespace.name.len() + name.len();
            namespace.used_bytes -= mark_bytes;
            *used_bytes -= mark_bytes;
            *marked -= 1;
        }
    }

    /// Whether `key` is currently `migrated`-marked.
    fn is_marked(&self, key: &Key) -> bool {
        self.find_namespace(&key.namespace)
            .and_then(|id| self.slots.get(&id))
            .is_some_and(|namespace| namespace.migrated.contains(&key.name[..]))
    }

    /// Removes any mark for `key` — see `release_mark`.
    fn clear_migrated_mark(&mut self, key: &Key) {
        if let Some(namespace) = self
            .find_namespace(&key.namespace)
            .and_then(|id| self.slots.get_mut(&id))
        {
            Self::release_mark(namespace, &key.name, &mut self.used_bytes, &mut self.marked);
        }
    }

    /// A point-in-time snapshot of every non-expired key, across every
    /// namespace. For staged node join's
    /// migration task: both of its consumers only ever need the key, never
    /// a value or TTL captured here. `entries_to_send_count` (in
    /// `src/server.rs`) filters purely on key membership in the before/
    /// after hash rings, and `run_migration` re-peeks each key's *live*
    /// value and TTL right before sending it (`peek_entry`, below) rather
    /// than trusting anything captured in an earlier snapshot — a
    /// concurrent client write between this snapshot and that key's turn
    /// must win, so a stale value/TTL captured here would only ever be
    /// thrown away unused. Uses `LruCache::iter`, not `get`, so listing
    /// keys doesn't itself perturb recency.
    ///
    /// Used to clone every key *and* value *and* compute each one's
    /// remaining TTL in this same synchronous walk — real work (issue
    /// #19's audit) for data neither consumer above ever looked at once
    /// `peek_entry` re-checks it live anyway. Now clones only the key
    /// (cloning `Bytes` is cheap — a refcount bump, not a copy — but the
    /// `Vec` itself and its iteration are still O(entries)), and this
    /// still runs synchronously on the single cache actor task
    /// (`run_cache` in `src/server.rs`), so calling this still blocks
    /// every other request the actor handles for as long as it takes to
    /// walk the whole cache.
    ///
    /// Tenth-pass audit (2026-09-02): this is the same O(n) full-cache
    /// scan (with `Bytes` refcount clones, not copies) `sweep`'s own
    /// `pending_removal` refill already does when its queue runs dry
    /// (`self.pending_removal.extend(self.namespaces.iter().flat_map(...))`,
    /// below) — an accepted pattern here, not unique to this method:
    /// scanning is measured cheap (~4ms/1M entries) relative to mutating,
    /// which is why `sweep` only chunks its *removals*, never this walk.
    /// A pagination attempt was tried and reverted (see PR #447's
    /// history) — it moved the transfer over the request channel into
    /// bounded pages, but `Command::ListEntries`'s first page still paid
    /// this exact synchronous scan, so nothing was actually bounded, and
    /// the reverted version *also* held the whole scanned snapshot as new
    /// actor-side state (a `listings` map with its own eviction/restart
    /// semantics) for no corresponding benefit. Bounding the scan itself
    /// would need a data structure that can answer "the next N keys after
    /// this cursor" without walking everything preceding it — an ordered
    /// per-namespace index, not present today — which is out of scope
    /// here; left as a documented limitation (issue #449, closed as
    /// such). Measured in release builds at roughly 25 ms per million
    /// live keys (200k: 5 ms, 1M: 25 ms, 2M: 52 ms), so at the default
    /// `--max-memory` of 256 MiB — on the order of 2–3M small entries —
    /// one ring change costs a single stall of a few tens of
    /// milliseconds, the same order as `sweep`'s own refill. See
    /// `docs/architecture.html` ("Known limits").
    pub fn keys(&self) -> Vec<Key> {
        self.keys_at(Instant::now())
    }

    fn keys_at(&self, now: Instant) -> Vec<Key> {
        self.slots
            .values()
            .flat_map(|sub_map| {
                sub_map
                    .entries
                    .iter()
                    .filter(move |(_, entry)| !entry.is_expired_at(now))
                    .map(move |(name, _)| Key::new(sub_map.name.clone(), name.clone()))
            })
            .collect()
    }

    /// The current value and remaining TTL for one key, same shape as one
    /// `entries()` row, without disturbing recency (`LruCache::peek`, not
    /// `get`). For staged node join's migration task, to re-check a key's *live*
    /// value right before sending it, instead of trusting whatever
    /// `entries()`'s snapshot captured at the start of the handoff — a
    /// concurrent client write between the snapshot and this key's turn
    /// would otherwise ship a stale value to the joining node. `None` if
    /// the key isn't present or has expired.
    pub fn peek_entry(&self, key: &Key) -> Option<(Key, Bytes, Option<Duration>)> {
        self.peek_entry_at(key, Instant::now())
    }

    fn peek_entry_at(&self, key: &Key, now: Instant) -> Option<(Key, Bytes, Option<Duration>)> {
        let entry = self.peek(key)?;

        if entry.is_expired_at(now) {
            return None;
        }

        let remaining_ttl = entry
            .expires_at
            .map(|expires_at| expires_at.saturating_duration_since(now));

        Some((key.clone(), entry.value.clone(), remaining_ttl))
    }

    /// Marks `key` as handed off during an staged node join migration this node
    /// was the source for. A no-op if the key is already marked or no
    /// longer present; `sweep` reclaims marked entries later. The
    /// namespace's `migrated` set holds its own copy of the key name (see
    /// its field docs), and a freshly marked key is charged an extra
    /// namespace+name length — the audit behind issue #19 flagged this
    /// duplicate as otherwise invisible to the memory limit.
    pub fn mark_migrated(&mut self, key: &Key) {
        let Some(namespace) = self
            .find_namespace(&key.namespace)
            .and_then(|id| self.slots.get_mut(&id))
        else {
            return;
        };

        if namespace.entries.contains(&key.name[..]) && namespace.migrated.insert(key.name.clone())
        {
            let mark_bytes = namespace.name.len() + key.name.len();
            self.used_bytes += mark_bytes;
            self.marked += 1;
            // Credit the owning namespace too, so per-namespace accounting
            // (`stats`'s `/metrics` rows and `--namespace-budget`) stays
            // consistent with the global total mid-migration instead of the
            // namespace rows summing to less than `used_bytes`.
            namespace.used_bytes += mark_bytes;
        }
    }

    /// Reverses `mark_migrated`: this node is keeping `key` after all (its
    /// migration was cancelled), so it must not be swept. A no-op if `key`
    /// wasn't marked. Does not touch `pending_removal` — a key already
    /// queued there by an earlier `sweep` refill finishes being removed
    /// regardless (see `sweep_at`), since cancellation only runs while
    /// `migration_in_progress` keeps `sweep` paused, so nothing this node
    /// marks can have reached `pending_removal` yet.
    pub fn unmark_migrated(&mut self, key: &Key) {
        self.clear_migrated_mark(key);
    }

    /// Issue #124: one consistent snapshot of the counters, accounting,
    /// and per-namespace breakdown, for the metrics endpoint. O(#live
    /// namespaces) — never a walk over entries.
    pub fn stats(&self) -> CacheStats {
        let mut namespaces: Vec<NamespaceStats> = self
            .slots
            .values()
            .map(|sub_map| NamespaceStats {
                namespace: sub_map.name.clone(),
                entries: sub_map.entries.len(),
                used_bytes: sub_map.used_bytes,
                budget_bytes: sub_map.budget,
            })
            .collect();
        // Deterministic output order (HashMap iteration isn't), largest
        // first — the read a capacity dashboard wants.
        namespaces.sort_by(|a, b| {
            b.used_bytes
                .cmp(&a.used_bytes)
                .then_with(|| a.namespace.cmp(&b.namespace))
        });

        CacheStats {
            used_bytes: self.used_bytes,
            max_memory_bytes: self.max_memory_bytes,
            entries: self.entry_count,
            hits: self.hits,
            misses: self.misses,
            sets: self.sets,
            deletes: self.deletes,
            evictions: self.evictions,
            expirations: self.expirations,
            incrs: self.incrs,
            cas_sets: self.cas_sets,
            cas_deletes: self.cas_deletes,
            namespaces,
        }
    }

    /// `CLEAR <ns>` (issue #106): drops the whole namespace in O(1) — its
    /// sub-map is one allocation to free, its byte share one subtraction —
    /// rather than scanning and unlinking entries one by one (which would
    /// stall every other command on the single-threaded cache actor for
    /// the whole walk, the Redis `KEYS` foot-gun). Returns how many
    /// entries went. The namespace's `migrated` marks go with it (they
    /// live in the sub-map) — the values they referred to no longer exist,
    /// and a mark must never outlive its value (a later write under the
    /// same key would otherwise inherit it and be swept, see `insert`).
    pub fn clear(&mut self, namespace: &[u8]) -> usize {
        let Some(id) = self.namespaces.remove(namespace) else {
            return 0;
        };
        let dropped = self
            .slots
            .remove(&id)
            .expect("an indexed namespace has a slot");

        let removed = dropped.entries.len();
        self.entry_count -= removed;
        // `dropped.used_bytes` already includes this namespace's `migrated`
        // mark bytes (`mark_migrated` credits them to the sub-map), so this
        // one subtraction covers entries and marks alike — crediting
        // `used_bytes` per mark here would double-subtract bytes this line
        // already reclaimed. The name's own charge is released alongside.
        self.used_bytes -= dropped.used_bytes + name_charge(&dropped.name);
        self.marked -= dropped.migrated.len();

        removed
    }

    /// Whole-store flush (issue #106): every namespace, the default one
    /// included. Same O(#namespaces) shape as `clear`.
    pub fn clear_all(&mut self) -> usize {
        let removed = self.entry_count;
        self.namespaces.clear();
        self.slots.clear();
        self.entry_count = 0;
        self.used_bytes = 0;
        self.marked = 0;
        // `used_bytes` is already 0; the marks' duplicate bytes and the
        // namespace name charges went with everything else.
        removed
    }

    /// Staged node join's active-deletion facility: reclaims entries marked by
    /// `mark_migrated`, and — since `get_at`/`delete_at` only expire a
    /// TTL'd entry lazily, on access — also proactively removes anything
    /// past its TTL, so an unread expired key doesn't sit in memory
    /// indefinitely. Removes at most `SWEEP_BUDGET` entries per call (the
    /// caller should call again if the backlog isn't drained yet — see
    /// `pending_removal`), so one call can't stall every other cache
    /// command behind it for as long as a full pass over a large cache
    /// would take. Returns how many entries were actually removed this
    /// call (a marked or expired key may already be gone, e.g. deleted by
    /// a client in the meantime) — `< SWEEP_BUDGET` means the backlog is
    /// now fully drained.
    pub fn sweep(&mut self) -> usize {
        self.sweep_at(Instant::now(), true)
    }

    /// `sweep` restricted to TTL expiry: marked entries are left alone.
    /// Issue #62: a source's dead copies must survive until discovery has
    /// actually completed the join they were handed off for — an
    /// abandoned join rolls the marks back instead — so the periodic
    /// sweep runs in this mode while that's still undecided.
    pub fn sweep_expired(&mut self) -> usize {
        self.sweep_at(Instant::now(), false)
    }

    fn sweep_at(&mut self, now: Instant, include_marked: bool) -> usize {
        if self.pending_removal.is_empty() {
            self.pending_removal
                .extend(self.slots.values().flat_map(|sub_map| {
                    sub_map
                        .entries
                        .iter()
                        .filter(move |(_, entry)| entry.is_expired_at(now))
                        .map(move |(name, _)| Key::new(sub_map.name.clone(), name.clone()))
                }));
            // Marks stay in `migrated` until the moment of removal (not
            // drained here): the queue is only a snapshot of candidates,
            // and a key rewritten after this point clears its mark, which
            // the removability re-check below must still observe.
            if include_marked {
                self.pending_removal
                    .extend(self.slots.values().flat_map(|sub_map| {
                        sub_map
                            .migrated
                            .iter()
                            .map(move |name| Key::new(sub_map.name.clone(), name.clone()))
                    }));
            }
        }

        let mut removed = 0;

        for _ in 0..SWEEP_BUDGET {
            let Some(key) = self.pending_removal.pop_front() else {
                break;
            };

            // Re-check at removal time: the snapshot above may be stale —
            // the key may have been rewritten (mark cleared, or no longer
            // expired) since it was queued, and a fresh value must never
            // be swept on the strength of an old candidate entry.
            let marked = include_marked && self.is_marked(&key);
            let expired = self
                .peek(&key)
                .is_some_and(|entry| entry.is_expired_at(now));

            if (marked || expired) && self.remove_entry(&key).is_some() {
                removed += 1;
                // A marked reclaim is migration bookkeeping, not a
                // client-visible expiry — see the counter's field docs.
                if expired {
                    self.expirations += 1;
                }
            }
        }

        removed
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn key(name: &[u8]) -> Key {
        Key::unnamespaced(Bytes::copy_from_slice(name))
    }

    fn namespaced(namespace: &[u8], name: &[u8]) -> Key {
        Key::new(
            Bytes::copy_from_slice(namespace),
            Bytes::copy_from_slice(name),
        )
    }

    const UNBOUNDED: usize = usize::MAX;

    #[test]
    fn gets_a_previously_set_value() {
        let mut cache = Cache::new(UNBOUNDED);

        cache.set(key(b"name"), Bytes::from_static(b"Alice"));

        assert_eq!(cache.get(&key(b"name")), Some(Bytes::from_static(b"Alice")));
    }

    #[test]
    fn get_returns_none_for_missing_key() {
        let mut cache = Cache::new(UNBOUNDED);

        assert_eq!(cache.get(&key(b"missing")), None);
    }

    #[test]
    fn delete_returns_true_for_existing_key() {
        let mut cache = Cache::new(UNBOUNDED);

        cache.set(key(b"name"), Bytes::from_static(b"Alice"));

        assert!(cache.delete(&key(b"name")));
    }

    #[test]
    fn delete_returns_false_for_missing_key() {
        let mut cache = Cache::new(UNBOUNDED);

        assert!(!cache.delete(&key(b"name")));
    }

    #[test]
    fn set_overwrites_existing_value() {
        let mut cache = Cache::new(UNBOUNDED);

        cache.set(key(b"name"), Bytes::from_static(b"Alice"));
        cache.set(key(b"name"), Bytes::from_static(b"Bob"));

        assert_eq!(cache.get(&key(b"name")), Some(Bytes::from_static(b"Bob")));
    }

    #[test]
    fn deleted_value_can_no_longer_be_retrieved() {
        let mut cache = Cache::new(UNBOUNDED);

        cache.set(key(b"name"), Bytes::from_static(b"Alice"));
        cache.delete(&key(b"name"));

        assert_eq!(cache.get(&key(b"name")), None);
    }

    #[test]
    fn gets_a_previously_set_value_with_ttl() {
        let mut cache = Cache::new(UNBOUNDED);

        cache.set_with_ttl(
            key(b"name"),
            Bytes::from_static(b"Alice"),
            Duration::from_secs(5),
        );

        assert_eq!(cache.get(&key(b"name")), Some(Bytes::from_static(b"Alice")));
    }

    #[test]
    fn incr_on_a_missing_key_returns_not_found() {
        let mut cache = Cache::new(UNBOUNDED);

        assert_eq!(cache.incr(&key(b"counter"), 1), IncrResult::NotFound);
    }

    #[test]
    fn incr_adds_delta_to_the_stored_value() {
        let mut cache = Cache::new(UNBOUNDED);
        cache.set(key(b"counter"), Bytes::from_static(b"10"));

        assert_eq!(cache.incr(&key(b"counter"), 5), IncrResult::Value(15, None));
        assert_eq!(cache.get(&key(b"counter")), Some(Bytes::from_static(b"15")));
    }

    #[test]
    fn incr_accepts_a_negative_delta() {
        let mut cache = Cache::new(UNBOUNDED);
        cache.set(key(b"counter"), Bytes::from_static(b"10"));

        assert_eq!(cache.incr(&key(b"counter"), -3), IncrResult::Value(7, None));
    }

    #[test]
    fn incr_on_a_non_numeric_value_returns_not_numeric() {
        let mut cache = Cache::new(UNBOUNDED);
        cache.set(key(b"name"), Bytes::from_static(b"Alice"));

        assert_eq!(cache.incr(&key(b"name"), 1), IncrResult::NotNumeric);
        // The value is untouched — a failed INCR never writes.
        assert_eq!(cache.get(&key(b"name")), Some(Bytes::from_static(b"Alice")));
    }

    #[test]
    fn incr_rejects_a_leading_plus_sign() {
        let mut cache = Cache::new(UNBOUNDED);
        cache.set(key(b"counter"), Bytes::from_static(b"+10"));

        assert_eq!(cache.incr(&key(b"counter"), 1), IncrResult::NotNumeric);
    }

    #[test]
    fn incr_rejects_a_leading_zero() {
        let mut cache = Cache::new(UNBOUNDED);
        cache.set(key(b"counter"), Bytes::from_static(b"010"));

        assert_eq!(cache.incr(&key(b"counter"), 1), IncrResult::NotNumeric);
    }

    #[test]
    fn incr_that_would_overflow_i64_returns_not_numeric() {
        let mut cache = Cache::new(UNBOUNDED);
        cache.set(key(b"counter"), Bytes::from(i64::MAX.to_string()));

        assert_eq!(cache.incr(&key(b"counter"), 1), IncrResult::NotNumeric);
    }

    #[test]
    fn incr_on_an_expired_key_returns_not_found_and_removes_it() {
        let mut cache = Cache::new(UNBOUNDED);
        cache.set_with_ttl(key(b"counter"), Bytes::from_static(b"10"), Duration::ZERO);

        std::thread::sleep(Duration::from_millis(5));

        assert_eq!(cache.incr(&key(b"counter"), 1), IncrResult::NotFound);
        assert_eq!(cache.len(), 0);
    }

    #[test]
    fn incr_preserves_the_entrys_ttl() {
        let mut cache = Cache::new(UNBOUNDED);
        cache.set_with_ttl(
            key(b"counter"),
            Bytes::from_static(b"10"),
            Duration::from_secs(60),
        );

        let IncrResult::Value(11, Some(remaining)) = cache.incr(&key(b"counter"), 1) else {
            panic!("expected a value with a remaining TTL");
        };
        assert!(remaining <= Duration::from_secs(60) && remaining > Duration::from_secs(55));
    }

    #[test]
    fn incr_does_not_reset_a_missing_ttl_to_one() {
        let mut cache = Cache::new(UNBOUNDED);
        cache.set(key(b"counter"), Bytes::from_static(b"10"));

        assert_eq!(cache.incr(&key(b"counter"), 1), IncrResult::Value(11, None));
    }

    #[test]
    fn incr_is_scoped_to_its_namespace() {
        let mut cache = Cache::new(UNBOUNDED);
        cache.set(namespaced(b"ns", b"counter"), Bytes::from_static(b"10"));

        assert_eq!(
            cache.incr(&namespaced(b"ns", b"counter"), 1),
            IncrResult::Value(11, None)
        );
        assert_eq!(cache.incr(&key(b"counter"), 1), IncrResult::NotFound);
    }

    #[test]
    fn cas_set_with_absent_condition_succeeds_on_a_missing_key() {
        let mut cache = Cache::new(UNBOUNDED);

        assert_eq!(
            cache.cas_set(
                &key(b"name"),
                CasCondition::Absent,
                Bytes::from_static(b"Alice"),
                None
            ),
            CasResult::Stored
        );
        assert_eq!(cache.get(&key(b"name")), Some(Bytes::from_static(b"Alice")));
    }

    #[test]
    fn cas_set_with_absent_condition_fails_when_the_key_exists() {
        let mut cache = Cache::new(UNBOUNDED);
        cache.set(key(b"name"), Bytes::from_static(b"Alice"));

        assert_eq!(
            cache.cas_set(
                &key(b"name"),
                CasCondition::Absent,
                Bytes::from_static(b"Bob"),
                None
            ),
            CasResult::Mismatch
        );
        // A failed CAS never writes.
        assert_eq!(cache.get(&key(b"name")), Some(Bytes::from_static(b"Alice")));
    }

    #[test]
    fn cas_set_with_present_condition_succeeds_when_the_key_exists() {
        let mut cache = Cache::new(UNBOUNDED);
        cache.set(key(b"name"), Bytes::from_static(b"Alice"));

        assert_eq!(
            cache.cas_set(
                &key(b"name"),
                CasCondition::Present,
                Bytes::from_static(b"Bob"),
                None
            ),
            CasResult::Stored
        );
        assert_eq!(cache.get(&key(b"name")), Some(Bytes::from_static(b"Bob")));
    }

    #[test]
    fn cas_set_with_present_condition_fails_on_a_missing_key() {
        let mut cache = Cache::new(UNBOUNDED);

        assert_eq!(
            cache.cas_set(
                &key(b"name"),
                CasCondition::Present,
                Bytes::from_static(b"Bob"),
                None
            ),
            CasResult::Mismatch
        );
        assert_eq!(cache.get(&key(b"name")), None);
    }

    #[test]
    fn cas_set_with_a_matching_digest_succeeds_and_replaces_the_value() {
        let mut cache = Cache::new(UNBOUNDED);
        cache.set(key(b"name"), Bytes::from_static(b"Alice"));
        let digest = content_digest(b"Alice");

        assert_eq!(
            cache.cas_set(
                &key(b"name"),
                CasCondition::Digest(digest),
                Bytes::from_static(b"Bob"),
                None
            ),
            CasResult::Stored
        );
        assert_eq!(cache.get(&key(b"name")), Some(Bytes::from_static(b"Bob")));
    }

    #[test]
    fn cas_set_with_a_stale_digest_fails_and_leaves_the_value_untouched() {
        let mut cache = Cache::new(UNBOUNDED);
        cache.set(key(b"name"), Bytes::from_static(b"Alice"));
        let stale_digest = content_digest(b"someone-else");

        assert_eq!(
            cache.cas_set(
                &key(b"name"),
                CasCondition::Digest(stale_digest),
                Bytes::from_static(b"Bob"),
                None
            ),
            CasResult::Mismatch
        );
        assert_eq!(cache.get(&key(b"name")), Some(Bytes::from_static(b"Alice")));
    }

    #[test]
    fn cas_set_with_a_digest_condition_fails_on_a_missing_key() {
        let mut cache = Cache::new(UNBOUNDED);

        assert_eq!(
            cache.cas_set(
                &key(b"name"),
                CasCondition::Digest(content_digest(b"Alice")),
                Bytes::from_static(b"Bob"),
                None
            ),
            CasResult::Mismatch
        );
    }

    #[test]
    fn cas_set_on_an_expired_key_treats_it_as_absent() {
        let mut cache = Cache::new(UNBOUNDED);
        cache.set_with_ttl(key(b"name"), Bytes::from_static(b"Alice"), Duration::ZERO);
        std::thread::sleep(Duration::from_millis(5));

        assert_eq!(
            cache.cas_set(
                &key(b"name"),
                CasCondition::Absent,
                Bytes::from_static(b"Bob"),
                None
            ),
            CasResult::Stored
        );
        assert_eq!(cache.get(&key(b"name")), Some(Bytes::from_static(b"Bob")));
    }

    #[test]
    fn cas_set_applies_the_given_ttl() {
        let mut cache = Cache::new(UNBOUNDED);

        assert_eq!(
            cache.cas_set(
                &key(b"name"),
                CasCondition::Absent,
                Bytes::from_static(b"Alice"),
                Some(Duration::from_secs(60))
            ),
            CasResult::Stored
        );
        let (_, _, remaining) = cache.peek_entry(&key(b"name")).expect("entry must exist");
        let remaining = remaining.expect("a TTL was given");
        assert!(remaining <= Duration::from_secs(60) && remaining > Duration::from_secs(55));
    }

    #[test]
    fn cas_set_with_no_ttl_means_no_expiry() {
        let mut cache = Cache::new(UNBOUNDED);

        cache.cas_set(
            &key(b"name"),
            CasCondition::Absent,
            Bytes::from_static(b"Alice"),
            None,
        );

        let (_, _, remaining) = cache.peek_entry(&key(b"name")).expect("entry must exist");
        assert_eq!(remaining, None);
    }

    #[test]
    fn cas_set_is_scoped_to_its_namespace() {
        let mut cache = Cache::new(UNBOUNDED);
        cache.set(namespaced(b"ns", b"name"), Bytes::from_static(b"Alice"));

        // The default-namespace key is absent, so `Absent` succeeds there
        // even though the same name exists under "ns".
        assert_eq!(
            cache.cas_set(
                &key(b"name"),
                CasCondition::Absent,
                Bytes::from_static(b"Bob"),
                None
            ),
            CasResult::Stored
        );
        assert_eq!(
            cache.get(&namespaced(b"ns", b"name")),
            Some(Bytes::from_static(b"Alice"))
        );
    }

    #[test]
    fn cas_delete_with_a_matching_digest_removes_the_key() {
        let mut cache = Cache::new(UNBOUNDED);
        cache.set(key(b"name"), Bytes::from_static(b"Alice"));

        assert!(cache.cas_delete(&key(b"name"), content_digest(b"Alice")));
        assert_eq!(cache.get(&key(b"name")), None);
    }

    #[test]
    fn cas_delete_with_a_stale_digest_fails_and_leaves_the_key() {
        let mut cache = Cache::new(UNBOUNDED);
        cache.set(key(b"name"), Bytes::from_static(b"Alice"));

        assert!(!cache.cas_delete(&key(b"name"), content_digest(b"someone-else")));
        assert_eq!(cache.get(&key(b"name")), Some(Bytes::from_static(b"Alice")));
    }

    #[test]
    fn cas_delete_on_a_missing_key_fails() {
        let mut cache = Cache::new(UNBOUNDED);

        assert!(!cache.cas_delete(&key(b"name"), content_digest(b"Alice")));
    }

    #[test]
    fn cas_set_digest_condition_is_sensitive_to_a_single_trailing_byte() {
        // Regression (issue #336): guards the `constant_time_eq` swap in
        // `condition_holds_at` against a broken refactor (e.g. comparing
        // the wrong slice, or stopping at the first mismatch and missing
        // a later differing byte) — a digest differing in only its last
        // byte must still be rejected.
        let mut cache = Cache::new(UNBOUNDED);
        cache.set(key(b"name"), Bytes::from_static(b"Alice"));

        let mut almost_right = content_digest(b"Alice");
        *almost_right.last_mut().unwrap() ^= 0xff;

        assert_eq!(
            cache.cas_set(
                &key(b"name"),
                CasCondition::Digest(almost_right),
                Bytes::from_static(b"Bob"),
                None
            ),
            CasResult::Mismatch
        );
        assert_eq!(cache.get(&key(b"name")), Some(Bytes::from_static(b"Alice")));
    }

    #[test]
    fn content_digest_is_deterministic_and_sensitive_to_every_byte() {
        assert_eq!(content_digest(b"Alice"), content_digest(b"Alice"));
        assert_ne!(content_digest(b"Alice"), content_digest(b"alice"));
    }

    /// Issue #141: the cross-language pinned test vector — the same input
    /// and expected 32-hex-digit digest is duplicated into every SDK's
    /// test suite (same pattern as the DEFLATE and HRW FNV-1a vectors). A
    /// mismatch anywhere means CAS has silently stopped agreeing across
    /// languages.
    #[test]
    fn content_digest_matches_the_pinned_cross_language_vector() {
        let digest = content_digest(b"nanocached-cas-vector");
        let hex: String = digest.iter().map(|byte| format!("{byte:02x}")).collect();
        assert_eq!(hex, "36287141940ca57acbd7695ccdde9d43");
    }

    #[test]
    fn parse_decimal_i64_accepts_zero_and_negative_zero_shaped_input() {
        assert_eq!(parse_decimal_i64(b"0"), Some(0));
    }

    #[test]
    fn parse_decimal_i64_rejects_empty_input() {
        assert_eq!(parse_decimal_i64(b""), None);
        assert_eq!(parse_decimal_i64(b"-"), None);
    }

    #[test]
    fn parse_decimal_i64_rejects_non_digit_bytes() {
        assert_eq!(parse_decimal_i64(b"12a"), None);
        assert_eq!(parse_decimal_i64(b"1.5"), None);
        assert_eq!(parse_decimal_i64(b" 1"), None);
    }

    #[test]
    fn parse_decimal_i64_handles_i64_min_specially() {
        assert_eq!(parse_decimal_i64(b"-9223372036854775808"), Some(i64::MIN));
        assert_eq!(parse_decimal_i64(b"9223372036854775807"), Some(i64::MAX));
        // One past either boundary doesn't fit.
        assert_eq!(parse_decimal_i64(b"-9223372036854775809"), None);
        assert_eq!(parse_decimal_i64(b"9223372036854775808"), None);
    }

    #[test]
    fn set_with_an_overflowing_ttl_never_expires_instead_of_panicking() {
        // A TTL near `u64::MAX` seconds overflows `Instant + Duration`, which
        // panics — taking down the whole cache actor. Such a value is stored
        // with no expiry instead: it must not panic, and the entry stays
        // retrievable arbitrarily far into the future.
        let mut cache = Cache::new(UNBOUNDED);

        cache.set_with_ttl(
            key(b"name"),
            Bytes::from_static(b"Alice"),
            Duration::from_secs(u64::MAX),
        );

        let far_future = Instant::now() + Duration::from_secs(1_000_000_000);

        assert_eq!(
            cache.get_at(&key(b"name"), far_future),
            Some(Bytes::from_static(b"Alice"))
        );
    }

    #[test]
    fn delete_returns_true_before_expiration() {
        let mut cache = Cache::new(UNBOUNDED);

        cache.set_with_ttl(
            key(b"name"),
            Bytes::from_static(b"Alice"),
            Duration::from_secs(5),
        );

        assert!(cache.delete(&key(b"name")));
    }

    #[test]
    fn get_returns_value_before_expiration() {
        let mut cache = Cache::new(UNBOUNDED);

        cache.set_with_ttl(
            key(b"name"),
            Bytes::from_static(b"Alice"),
            Duration::from_secs(5),
        );

        let future = Instant::now() + Duration::from_secs(4);

        assert_eq!(
            cache.get_at(&key(b"name"), future),
            Some(Bytes::from_static(b"Alice"))
        );
    }

    #[test]
    fn get_returns_none_after_expiration() {
        let mut cache = Cache::new(UNBOUNDED);

        cache.set_with_ttl(
            key(b"name"),
            Bytes::from_static(b"Alice"),
            Duration::from_secs(5),
        );

        let future = Instant::now() + Duration::from_secs(6);

        assert_eq!(cache.get_at(&key(b"name"), future), None);
    }

    #[test]
    fn get_removes_expired_entry() {
        let mut cache = Cache::new(UNBOUNDED);

        cache.set_with_ttl(
            key(b"name"),
            Bytes::from_static(b"Alice"),
            Duration::from_secs(5),
        );

        let future = Instant::now() + Duration::from_secs(6);

        cache.get_at(&key(b"name"), future);

        assert!(!cache.contains(&key(b"name")));
    }

    #[test]
    fn delete_returns_false_after_expiration() {
        let mut cache = Cache::new(UNBOUNDED);

        cache.set_with_ttl(
            key(b"name"),
            Bytes::from_static(b"Alice"),
            Duration::from_secs(5),
        );

        let future = Instant::now() + Duration::from_secs(6);

        assert!(!cache.delete_at(&key(b"name"), future));
    }

    #[test]
    fn evicts_least_recently_used_entry_when_over_memory_limit() {
        // Each entry costs 2 (key) + 4 (value) + ENTRY_OVERHEAD_BYTES;
        // room for exactly two.
        let mut cache = Cache::new(2 * (2 + 4 + ENTRY_OVERHEAD_BYTES));

        cache.set(key(b"k1"), Bytes::from_static(b"vvvv"));
        cache.set(key(b"k2"), Bytes::from_static(b"vvvv"));
        cache.set(key(b"k3"), Bytes::from_static(b"vvvv"));

        assert_eq!(cache.get(&key(b"k1")), None);
        assert_eq!(cache.get(&key(b"k2")), Some(Bytes::from_static(b"vvvv")));
        assert_eq!(cache.get(&key(b"k3")), Some(Bytes::from_static(b"vvvv")));
    }

    #[test]
    fn get_protects_an_entry_from_eviction_by_marking_it_recently_used() {
        let mut cache = Cache::new(2 * (2 + 4 + ENTRY_OVERHEAD_BYTES));

        cache.set(key(b"k1"), Bytes::from_static(b"vvvv"));
        cache.set(key(b"k2"), Bytes::from_static(b"vvvv"));

        cache.get(&key(b"k1"));

        cache.set(key(b"k3"), Bytes::from_static(b"vvvv"));

        assert_eq!(cache.get(&key(b"k1")), Some(Bytes::from_static(b"vvvv")));
        assert_eq!(cache.get(&key(b"k2")), None);
        assert_eq!(cache.get(&key(b"k3")), Some(Bytes::from_static(b"vvvv")));
    }

    #[test]
    fn overwriting_a_key_does_not_double_count_memory_usage() {
        let mut cache = Cache::new(2 * (2 + 4 + ENTRY_OVERHEAD_BYTES));

        cache.set(key(b"k1"), Bytes::from_static(b"vvvv"));
        cache.set(key(b"k1"), Bytes::from_static(b"vvvv"));
        cache.set(key(b"k2"), Bytes::from_static(b"vvvv"));

        assert_eq!(cache.get(&key(b"k1")), Some(Bytes::from_static(b"vvvv")));
        assert_eq!(cache.get(&key(b"k2")), Some(Bytes::from_static(b"vvvv")));
    }

    #[test]
    fn overwrite_accounts_for_a_shrinking_value_precisely() {
        // Post-eviction, "a" (shrunk to 1+1=2 data bytes) and "c" (1+5=6)
        // must fit (8 data bytes + 2 entries' overhead); "a"+"b"+"c"
        // together (12 data bytes + 3 entries' overhead) must not.
        let mut cache = Cache::new(2 * ENTRY_OVERHEAD_BYTES + 10);

        cache.set(key(b"a"), Bytes::from_static(b"XXX")); // size 4, used 4 + overhead
        cache.set(key(b"b"), Bytes::from_static(b"XXX")); // size 4, used 8 + 2*overhead
        cache.set(key(b"a"), Bytes::from_static(b"Z")); // shrinks to size 2, used 6 + 2*overhead
        cache.set(key(b"c"), Bytes::from_static(b"WWWWW")); // size 6, used 12 + 3*overhead: evicts LRU "b"

        assert_eq!(cache.get(&key(b"b")), None);
        assert_eq!(cache.get(&key(b"a")), Some(Bytes::from_static(b"Z")));
        assert_eq!(cache.get(&key(b"c")), Some(Bytes::from_static(b"WWWWW")));
    }

    #[test]
    fn eviction_loop_accounts_for_freed_bytes_precisely() {
        let mut cache = Cache::new(7 + 2 * ENTRY_OVERHEAD_BYTES);

        cache.set(key(b"a"), Bytes::from_static(b"X")); // size 2, used 2
        cache.set(key(b"b"), Bytes::from_static(b"X")); // size 2, used 4
        cache.set(key(b"c"), Bytes::from_static(b"X")); // size 2, used 6
        cache.set(key(b"d"), Bytes::from_static(b"WWW")); // size 4, used 10 > 7: evicts "a" then "b"

        assert_eq!(cache.get(&key(b"a")), None);
        assert_eq!(cache.get(&key(b"b")), None);
        assert_eq!(cache.get(&key(b"c")), Some(Bytes::from_static(b"X")));
        assert_eq!(cache.get(&key(b"d")), Some(Bytes::from_static(b"WWW")));
    }

    #[test]
    fn delete_frees_the_deleted_entrys_exact_byte_count() {
        let mut cache = Cache::new(10 + 2 * ENTRY_OVERHEAD_BYTES);

        cache.set(key(b"a"), Bytes::from_static(b"XXX")); // size 4
        cache.set(key(b"b"), Bytes::from_static(b"XXX")); // size 4, used 8
        cache.delete(&key(b"a")); // used 4
        cache.set(key(b"c"), Bytes::from_static(b"XXX")); // size 4, used 8, fits

        assert_eq!(cache.get(&key(b"b")), Some(Bytes::from_static(b"XXX")));
        assert_eq!(cache.get(&key(b"c")), Some(Bytes::from_static(b"XXX")));
    }

    #[test]
    fn delete_does_not_under_report_freed_bytes() {
        let mut cache = Cache::new(6 + 2 * ENTRY_OVERHEAD_BYTES);

        cache.set(key(b"a"), Bytes::from_static(b"XXX")); // size 4
        cache.set(key(b"b"), Bytes::from_static(b"X")); // size 2, used 6
        cache.delete(&key(b"a")); // used 2
        cache.set(key(b"c"), Bytes::from_static(b"WWWW")); // size 5, used 7 > 6: evicts "b"

        assert_eq!(cache.get(&key(b"b")), None);
        assert_eq!(cache.get(&key(b"c")), Some(Bytes::from_static(b"WWWW")));
    }

    #[test]
    fn per_entry_overhead_counts_toward_the_memory_limit_even_for_tiny_values() {
        // Two 2-byte entries (1-byte key + 1-byte value each) total 4 raw
        // data bytes — well under a 4-byte budget's raw accounting, but
        // each also costs ENTRY_OVERHEAD_BYTES of invisible bookkeeping,
        // so the second insert must evict the first.
        let mut cache = Cache::new(4);

        cache.set(key(b"a"), Bytes::from_static(b"1"));
        cache.set(key(b"b"), Bytes::from_static(b"2"));

        assert_eq!(cache.get(&key(b"a")), None);
        assert_eq!(cache.get(&key(b"b")), Some(Bytes::from_static(b"2")));
    }

    #[test]
    fn a_single_entry_larger_than_the_limit_is_kept_and_not_evicted() {
        let mut cache = Cache::new(4);

        cache.set(key(b"k1"), Bytes::from_static(b"vvvv"));

        assert_eq!(cache.get(&key(b"k1")), Some(Bytes::from_static(b"vvvv")));
    }

    #[test]
    fn delete_frees_memory_for_subsequent_inserts() {
        let mut cache = Cache::new(6);

        cache.set(key(b"k1"), Bytes::from_static(b"vvvv"));
        cache.delete(&key(b"k1"));
        cache.set(key(b"k2"), Bytes::from_static(b"vvvv"));

        assert_eq!(cache.get(&key(b"k1")), None);
        assert_eq!(cache.get(&key(b"k2")), Some(Bytes::from_static(b"vvvv")));
    }

    #[test]
    fn a_fresh_namespaces_map_key_is_a_copy_not_a_slice_of_the_callers_buffer() {
        // Regression (issue #406): the parser slices `key.namespace`
        // zero-copy out of the request frame (like `key.name`/`value`,
        // which `insert` already re-copies for exactly this reason — see
        // its own comment). Using it as-is for a brand-new namespace's
        // `HashMap` key would pin whatever buffer it was sliced from —
        // potentially an entire pipelined read chunk — alive for as long
        // as the namespace exists, uncharged to `used_bytes`.
        let mut cache = Cache::new(UNBOUNDED);

        // A namespace name sliced out of a much larger shared buffer, the
        // same shape a zero-copy parse would produce.
        let mut backing = vec![0u8; 4096];
        backing[100..106].copy_from_slice(b"tenant");
        let backing = Bytes::from(backing);
        let namespace = backing.slice(100..106);
        assert_eq!(&namespace[..], b"tenant");

        cache.set(
            Key::new(namespace.clone(), Bytes::copy_from_slice(b"k")),
            Bytes::from_static(b"v"),
        );

        let stored_namespace = cache
            .namespaces
            .keys()
            .find(|stored| stored.as_ref() == b"tenant")
            .expect("the namespace was just inserted");

        assert_ne!(
            stored_namespace.as_ptr(),
            namespace.as_ptr(),
            "the namespace map key must be its own allocation, not a zero-copy slice of the \
             caller's buffer"
        );
    }

    #[test]
    fn namespaces_keep_the_same_key_name_apart() {
        let mut cache = Cache::new(UNBOUNDED);

        cache.set(key(b"name"), Bytes::from_static(b"default"));
        cache.set(namespaced(b"users", b"name"), Bytes::from_static(b"users"));
        cache.set(
            namespaced(b"orders", b"name"),
            Bytes::from_static(b"orders"),
        );

        assert_eq!(cache.len(), 3);
        assert_eq!(
            cache.get(&key(b"name")),
            Some(Bytes::from_static(b"default"))
        );
        assert_eq!(
            cache.get(&namespaced(b"users", b"name")),
            Some(Bytes::from_static(b"users"))
        );
        assert_eq!(
            cache.get(&namespaced(b"orders", b"name")),
            Some(Bytes::from_static(b"orders"))
        );

        assert!(cache.delete(&namespaced(b"users", b"name")));
        assert_eq!(cache.get(&namespaced(b"users", b"name")), None);
        assert_eq!(
            cache.get(&key(b"name")),
            Some(Bytes::from_static(b"default"))
        );
        assert_eq!(cache.len(), 2);
    }

    #[test]
    fn an_emptied_namespace_releases_its_sub_map() {
        // The per-eviction scan is over *live* namespaces, so a namespace
        // must not linger once its last entry is gone.
        let mut cache = Cache::new(UNBOUNDED);

        cache.set(namespaced(b"users", b"a"), Bytes::from_static(b"1"));
        cache.set(namespaced(b"users", b"b"), Bytes::from_static(b"2"));
        assert_eq!(cache.namespaces.len(), 1);

        cache.delete(&namespaced(b"users", b"a"));
        assert_eq!(cache.namespaces.len(), 1);
        cache.delete(&namespaced(b"users", b"b"));
        assert_eq!(cache.namespaces.len(), 0);
    }

    /// One entry's cost with a 2-byte key and 4-byte value — the shape
    /// every budget test below uses.
    const SMALL_ENTRY: usize = 2 + 4 + ENTRY_OVERHEAD_BYTES;

    #[test]
    fn a_namespace_over_its_budget_evicts_from_itself_oldest_first() {
        // Issue #127: the churny namespace pays for its own growth — the
        // other namespace's older entries survive untouched.
        let mut cache = Cache::with_budgets(
            UNBOUNDED,
            vec![(Bytes::from_static(b"hot"), 2 * SMALL_ENTRY)],
        );

        cache.set(namespaced(b"cold", b"c1"), Bytes::from_static(b"vvvv"));
        cache.set(namespaced(b"hot", b"h1"), Bytes::from_static(b"vvvv"));
        cache.set(namespaced(b"hot", b"h2"), Bytes::from_static(b"vvvv"));
        // The third hot entry breaches the 2-entry budget: h1 (hot's own
        // LRU victim) goes; cold — strictly older — stays.
        cache.set(namespaced(b"hot", b"h3"), Bytes::from_static(b"vvvv"));

        assert_eq!(cache.get(&namespaced(b"hot", b"h1")), None);
        assert_eq!(
            cache.get(&namespaced(b"hot", b"h2")),
            Some(Bytes::from_static(b"vvvv"))
        );
        assert_eq!(
            cache.get(&namespaced(b"hot", b"h3")),
            Some(Bytes::from_static(b"vvvv"))
        );
        assert_eq!(
            cache.get(&namespaced(b"cold", b"c1")),
            Some(Bytes::from_static(b"vvvv"))
        );
        assert_eq!(cache.evictions, 1);
    }

    #[test]
    fn an_overwrite_that_grows_past_the_budget_evicts_within_the_namespace() {
        let mut cache = Cache::with_budgets(
            UNBOUNDED,
            vec![(Bytes::from_static(b"hot"), 2 * SMALL_ENTRY)],
        );

        cache.set(namespaced(b"hot", b"h1"), Bytes::from_static(b"vvvv"));
        cache.set(namespaced(b"hot", b"h2"), Bytes::from_static(b"vvvv"));
        // Growing h2 by more than one entry's worth of bytes breaches the
        // budget without adding an entry — h1 must go.
        let grown = Bytes::from(vec![b'x'; 4 + SMALL_ENTRY]);
        cache.set(namespaced(b"hot", b"h2"), grown.clone());

        assert_eq!(cache.get(&namespaced(b"hot", b"h1")), None);
        assert_eq!(cache.get(&namespaced(b"hot", b"h2")), Some(grown));
    }

    #[test]
    fn a_single_entry_may_exceed_its_namespace_budget() {
        // Mirrors the global bound's floor: the entry just inserted is
        // never its own eviction victim.
        let mut cache =
            Cache::with_budgets(UNBOUNDED, vec![(Bytes::from_static(b"hot"), SMALL_ENTRY)]);

        let oversized = Bytes::from(vec![b'x'; 3 * SMALL_ENTRY]);
        cache.set(namespaced(b"hot", b"h1"), oversized.clone());

        assert_eq!(cache.get(&namespaced(b"hot", b"h1")), Some(oversized));
        assert_eq!(cache.evictions, 0);
    }

    #[test]
    fn a_budget_does_not_shield_a_namespace_from_the_global_lru() {
        // Issue #127: the global bound stays authoritative — a budgeted
        // namespace's oldest entry is still the global victim when the
        // whole cache is over --max-memory.
        let mut cache = Cache::with_budgets(
            2 * SMALL_ENTRY + name_charge(b"hot") + name_charge(b"cold"),
            vec![(Bytes::from_static(b"hot"), 10 * SMALL_ENTRY)],
        );

        cache.set(namespaced(b"hot", b"h1"), Bytes::from_static(b"vvvv"));
        cache.set(namespaced(b"cold", b"c1"), Bytes::from_static(b"vvvv"));
        cache.set(namespaced(b"cold", b"c2"), Bytes::from_static(b"vvvv"));

        // h1 was globally oldest; its namespace's generous budget didn't
        // protect it.
        assert_eq!(cache.get(&namespaced(b"hot", b"h1")), None);
        assert_eq!(cache.len(), 2);
    }

    #[test]
    fn stats_report_the_namespace_budget() {
        let mut cache = Cache::with_budgets(UNBOUNDED, vec![(Bytes::from_static(b"hot"), 4096)]);
        cache.set(namespaced(b"hot", b"h1"), Bytes::from_static(b"vvvv"));
        cache.set(namespaced(b"cold", b"c1"), Bytes::from_static(b"vvvv"));

        let stats = cache.stats();
        let budget = |name: &[u8]| {
            stats
                .namespaces
                .iter()
                .find(|row| row.namespace == name)
                .unwrap()
                .budget_bytes
        };
        assert_eq!(budget(b"hot"), Some(4096));
        assert_eq!(budget(b"cold"), None);
    }

    #[test]
    fn eviction_is_least_recently_used_across_namespaces() {
        // Each entry: 2-byte key + 4-byte value + overhead = 106; room for
        // exactly three, plus the three non-default namespaces' name
        // charges.
        let mut cache =
            Cache::new(3 * 106 + name_charge(b"x") + name_charge(b"y") + name_charge(b"z"));

        cache.set(namespaced(b"x", b"k1"), Bytes::from_static(b"vvvv"));
        cache.set(namespaced(b"y", b"k2"), Bytes::from_static(b"vvvv"));
        cache.set(key(b"k3"), Bytes::from_static(b"vvvv"));

        // Touch `x/k1`: the global LRU is now `y/k2`, even though `x/k1`
        // is the tail of its own namespace's list.
        cache.get(&namespaced(b"x", b"k1"));

        cache.set(namespaced(b"z", b"k4"), Bytes::from_static(b"vvvv"));

        assert_eq!(cache.get(&namespaced(b"y", b"k2")), None);
        assert_eq!(
            cache.get(&namespaced(b"x", b"k1")),
            Some(Bytes::from_static(b"vvvv"))
        );
        assert_eq!(cache.get(&key(b"k3")), Some(Bytes::from_static(b"vvvv")));
        assert_eq!(
            cache.get(&namespaced(b"z", b"k4")),
            Some(Bytes::from_static(b"vvvv"))
        );
        assert_eq!(cache.len(), 3);
    }

    #[test]
    fn eviction_follows_recency_within_a_namespace_too() {
        let mut cache = Cache::new(3 * 106 + name_charge(b"x") + name_charge(b"y"));

        cache.set(namespaced(b"x", b"k1"), Bytes::from_static(b"vvvv"));
        cache.set(namespaced(b"x", b"k2"), Bytes::from_static(b"vvvv"));
        cache.set(namespaced(b"x", b"k3"), Bytes::from_static(b"vvvv"));
        cache.get(&namespaced(b"x", b"k1"));

        cache.set(namespaced(b"y", b"k4"), Bytes::from_static(b"vvvv"));

        assert_eq!(cache.get(&namespaced(b"x", b"k2")), None);
        assert!(cache.get(&namespaced(b"x", b"k1")).is_some());
        assert!(cache.get(&namespaced(b"x", b"k3")).is_some());
    }

    #[test]
    fn keys_and_sweep_span_every_namespace() {
        let mut cache = Cache::new(UNBOUNDED);

        cache.set(key(b"a"), Bytes::from_static(b"1"));
        cache.set_with_ttl(
            namespaced(b"users", b"b"),
            Bytes::from_static(b"2"),
            Duration::from_secs(5),
        );
        cache.set(namespaced(b"orders", b"c"), Bytes::from_static(b"3"));
        cache.mark_migrated(&namespaced(b"orders", b"c"));

        let mut keys = cache.keys();
        keys.sort_by(|a, b| a.namespace.cmp(&b.namespace).then(a.name.cmp(&b.name)));
        assert_eq!(
            keys,
            vec![
                key(b"a"),
                namespaced(b"orders", b"c"),
                namespaced(b"users", b"b"),
            ]
        );

        let later = Instant::now() + Duration::from_secs(6);
        assert_eq!(cache.sweep_at(later, true), 2);
        assert_eq!(cache.keys_at(later), vec![key(b"a")]);
    }

    #[test]
    fn a_mark_is_scoped_to_its_namespace() {
        let mut cache = Cache::new(UNBOUNDED);

        cache.set(key(b"k"), Bytes::from_static(b"default"));
        cache.set(namespaced(b"users", b"k"), Bytes::from_static(b"users"));
        cache.mark_migrated(&namespaced(b"users", b"k"));

        assert_eq!(cache.sweep(), 1);
        assert_eq!(cache.get(&key(b"k")), Some(Bytes::from_static(b"default")));
        assert_eq!(cache.get(&namespaced(b"users", b"k")), None);
    }

    #[test]
    fn clear_drops_one_namespace_and_its_bytes() {
        let mut cache = Cache::new(UNBOUNDED);

        cache.set(key(b"a"), Bytes::from_static(b"1"));
        cache.set(namespaced(b"users", b"a"), Bytes::from_static(b"22"));
        cache.set(namespaced(b"users", b"b"), Bytes::from_static(b"333"));
        let before = cache.used_bytes;

        assert_eq!(cache.clear(b"users"), 2);

        assert_eq!(cache.len(), 1);
        assert_eq!(cache.namespaces.len(), 1);
        assert_eq!(
            cache.used_bytes,
            before
                - (1 + 2 + ENTRY_OVERHEAD_BYTES)
                - (1 + 3 + ENTRY_OVERHEAD_BYTES)
                - name_charge(b"users")
        );
        assert_eq!(cache.get(&namespaced(b"users", b"a")), None);
        assert_eq!(cache.get(&key(b"a")), Some(Bytes::from_static(b"1")));

        // Clearing again, or a namespace that never existed, is a no-op.
        assert_eq!(cache.clear(b"users"), 0);
        assert_eq!(cache.clear(b"nope"), 0);
    }

    #[test]
    fn clear_of_the_empty_namespace_is_the_default_one() {
        let mut cache = Cache::new(UNBOUNDED);
        cache.set(key(b"a"), Bytes::from_static(b"1"));
        cache.set(namespaced(b"users", b"a"), Bytes::from_static(b"2"));

        assert_eq!(cache.clear(b""), 1);
        assert_eq!(cache.get(&key(b"a")), None);
        assert_eq!(
            cache.get(&namespaced(b"users", b"a")),
            Some(Bytes::from_static(b"2"))
        );
    }

    #[test]
    fn clear_all_empties_the_store_and_its_accounting() {
        let mut cache = Cache::new(UNBOUNDED);
        cache.set(key(b"a"), Bytes::from_static(b"1"));
        cache.set(namespaced(b"users", b"a"), Bytes::from_static(b"2"));
        cache.mark_migrated(&key(b"a"));

        assert_eq!(cache.clear_all(), 2);

        assert_eq!(cache.len(), 0);
        assert_eq!(cache.used_bytes, 0);
        assert!(cache.namespaces.is_empty());
        assert_eq!(cache.marked, 0);
        assert_eq!(cache.get(&key(b"a")), None);
    }

    #[test]
    fn clear_drops_the_namespaces_marks_so_a_later_write_is_not_swept() {
        let mut cache = Cache::new(UNBOUNDED);
        cache.set(namespaced(b"users", b"a"), Bytes::from_static(b"old"));
        cache.set(key(b"a"), Bytes::from_static(b"kept"));
        cache.mark_migrated(&namespaced(b"users", b"a"));
        cache.mark_migrated(&key(b"a"));
        let marked_bytes = cache.used_bytes;

        cache.clear(b"users");
        // The mark's duplicate bytes were credited back along with the
        // entry's own.
        assert_eq!(
            cache.used_bytes,
            marked_bytes - (5 + 1) - (1 + 3 + ENTRY_OVERHEAD_BYTES) - name_charge(b"users")
        );

        cache.set(namespaced(b"users", b"a"), Bytes::from_static(b"new"));
        // Only the default namespace's mark survives the sweep.
        assert_eq!(cache.sweep(), 1);
        assert_eq!(
            cache.get(&namespaced(b"users", b"a")),
            Some(Bytes::from_static(b"new"))
        );
        assert_eq!(cache.get(&key(b"a")), None);
    }

    #[test]
    fn clear_keeps_eviction_accounting_honest() {
        // Per-namespace byte shares must track overwrites too, or a clear
        // after a value shrank/grew would leave `used_bytes` skewed and
        // the memory bound either over- or under-enforced.
        let mut cache = Cache::new(UNBOUNDED);
        cache.set(namespaced(b"x", b"k"), Bytes::from_static(b"short"));
        cache.set(
            namespaced(b"x", b"k"),
            Bytes::from_static(b"much-longer-value"),
        );
        cache.set(namespaced(b"y", b"k"), Bytes::from_static(b"vvvv"));
        let y_bytes = 1 + 4 + ENTRY_OVERHEAD_BYTES + name_charge(b"y");

        cache.clear(b"x");
        assert_eq!(cache.used_bytes, y_bytes);
        cache.clear(b"y");
        assert_eq!(cache.used_bytes, 0);
    }

    #[test]
    fn keys_includes_every_stored_key() {
        let mut cache = Cache::new(UNBOUNDED);

        cache.set(key(b"a"), Bytes::from_static(b"1"));
        cache.set(key(b"b"), Bytes::from_static(b"2"));

        let mut keys = cache.keys();
        keys.sort_by(|a, b| a.name.cmp(&b.name));

        assert_eq!(keys, vec![key(b"a"), key(b"b")]);
    }

    #[test]
    fn keys_excludes_expired_keys() {
        let mut cache = Cache::new(UNBOUNDED);

        cache.set_with_ttl(
            key(b"name"),
            Bytes::from_static(b"Alice"),
            Duration::from_secs(5),
        );

        let future = Instant::now() + Duration::from_secs(6);

        assert_eq!(cache.keys_at(future), Vec::<Key>::new());
    }

    #[test]
    fn keys_does_not_disturb_lru_order() {
        let mut cache = Cache::new(7 + 2 * ENTRY_OVERHEAD_BYTES);

        cache.set(key(b"a"), Bytes::from_static(b"XX")); // used 3
        cache.set(key(b"b"), Bytes::from_static(b"XX")); // used 6

        // If listing keys touched recency the same way `get` does, "a"
        // would become most-recently-used here and survive the eviction
        // below instead of "b".
        let _ = cache.keys();

        cache.set(key(b"c"), Bytes::from_static(b"XXX")); // evicts "a" (still LRU)

        assert_eq!(cache.get(&key(b"a")), None);
        assert_eq!(cache.get(&key(b"b")), Some(Bytes::from_static(b"XX")));
    }

    #[test]
    fn peek_entry_returns_the_current_value_and_remaining_ttl() {
        let mut cache = Cache::new(UNBOUNDED);

        cache.set_with_ttl(
            key(b"name"),
            Bytes::from_static(b"Alice"),
            Duration::from_secs(5),
        );

        let (peeked, value, ttl) = cache.peek_entry(&key(b"name")).unwrap();

        assert_eq!(peeked, key(b"name"));
        assert_eq!(value, Bytes::from_static(b"Alice"));
        assert!(ttl.unwrap() <= Duration::from_secs(5));
    }

    #[test]
    fn peek_entry_is_none_for_a_missing_key() {
        let cache = Cache::new(UNBOUNDED);

        assert_eq!(cache.peek_entry(&key(b"missing")), None);
    }

    #[test]
    fn peek_entry_is_none_for_an_expired_key() {
        let mut cache = Cache::new(UNBOUNDED);

        cache.set_with_ttl(
            key(b"name"),
            Bytes::from_static(b"Alice"),
            Duration::from_secs(5),
        );

        let future = Instant::now() + Duration::from_secs(6);

        assert_eq!(cache.peek_entry_at(&key(b"name"), future), None);
    }

    #[test]
    fn peek_entry_does_not_disturb_lru_order() {
        let mut cache = Cache::new(7 + 2 * ENTRY_OVERHEAD_BYTES);

        cache.set(key(b"a"), Bytes::from_static(b"XX"));
        cache.set(key(b"b"), Bytes::from_static(b"XX"));

        let _ = cache.peek_entry(&key(b"a"));

        cache.set(key(b"c"), Bytes::from_static(b"XXX")); // evicts "a" (still LRU)

        assert_eq!(cache.get(&key(b"a")), None);
        assert_eq!(cache.get(&key(b"b")), Some(Bytes::from_static(b"XX")));
    }

    #[test]
    fn mark_migrated_is_a_no_op_for_a_missing_key() {
        let mut cache = Cache::new(UNBOUNDED);

        cache.mark_migrated(&key(b"missing"));

        assert_eq!(cache.sweep(), 0);
    }

    #[test]
    fn mark_migrated_does_not_remove_or_change_the_entry() {
        let mut cache = Cache::new(UNBOUNDED);

        cache.set(key(b"name"), Bytes::from_static(b"Alice"));
        cache.mark_migrated(&key(b"name"));

        assert_eq!(cache.get(&key(b"name")), Some(Bytes::from_static(b"Alice")));
    }

    #[test]
    fn mark_migrated_counts_the_marked_keys_duplicate_bytes_toward_the_limit() {
        // "a" (2 data bytes) then "b" (2 data bytes) together cost exactly
        // 2*(2+ENTRY_OVERHEAD_BYTES) — this budget leaves no slack.
        // Marking "a" first adds one more byte (its key, duplicated into
        // `migrated`) that a boundary this tight cannot absorb, so
        // inserting "b" must evict "a". Without the mark, both would fit.
        let mut cache = Cache::new(2 * (2 + ENTRY_OVERHEAD_BYTES));

        cache.set(key(b"a"), Bytes::from_static(b"1"));
        cache.mark_migrated(&key(b"a"));
        cache.set(key(b"b"), Bytes::from_static(b"2"));

        assert_eq!(cache.get(&key(b"a")), None);
        assert_eq!(cache.get(&key(b"b")), Some(Bytes::from_static(b"2")));
    }

    #[test]
    fn unmark_migrated_credits_back_the_marked_keys_duplicate_bytes() {
        // Same tight budget as above, but the mark is reversed before "b"
        // is inserted — both must now fit.
        let mut cache = Cache::new(2 * (2 + ENTRY_OVERHEAD_BYTES));

        cache.set(key(b"a"), Bytes::from_static(b"1"));
        cache.mark_migrated(&key(b"a"));
        cache.unmark_migrated(&key(b"a"));
        cache.set(key(b"b"), Bytes::from_static(b"2"));

        assert_eq!(cache.get(&key(b"a")), Some(Bytes::from_static(b"1")));
        assert_eq!(cache.get(&key(b"b")), Some(Bytes::from_static(b"2")));
    }

    #[test]
    fn per_namespace_used_bytes_stay_consistent_with_the_global_total_across_marks() {
        // Regression (pass-7 audit): `mark_migrated` used to credit the
        // duplicate mark bytes to the global `used_bytes` only, so mid-
        // migration the per-namespace rows summed to LESS than the global
        // total (an observable /metrics mismatch, and a slightly loose
        // `--namespace-budget`). The two must agree through every mark
        // lifecycle: mark, unmark, overwrite, eviction, and CLEAR.
        let consistent = |cache: &Cache| {
            let stats = cache.stats();
            let per_ns: usize = stats
                .namespaces
                .iter()
                .map(|n| n.used_bytes + name_charge(&n.namespace))
                .sum();
            assert_eq!(
                per_ns, stats.used_bytes,
                "namespace rows (plus their name charges) must sum to the global used_bytes"
            );
        };

        let mut cache = Cache::new(UNBOUNDED);
        cache.set(namespaced(b"users", b"a"), Bytes::from_static(b"1"));
        cache.set(namespaced(b"users", b"b"), Bytes::from_static(b"2"));
        cache.set(namespaced(b"orders", b"c"), Bytes::from_static(b"3"));
        consistent(&cache);

        cache.mark_migrated(&namespaced(b"users", b"a"));
        cache.mark_migrated(&namespaced(b"orders", b"c"));
        consistent(&cache);

        // Unmark one, overwrite another (overwrite clears its mark).
        cache.unmark_migrated(&namespaced(b"users", b"a"));
        cache.set(namespaced(b"orders", b"c"), Bytes::from_static(b"33"));
        consistent(&cache);

        // Re-mark, then CLEAR the whole namespace out from under the mark.
        cache.mark_migrated(&namespaced(b"orders", b"c"));
        consistent(&cache);
        cache.clear(b"orders");
        consistent(&cache);
    }

    #[test]
    fn evicting_a_marked_entry_keeps_namespace_and_global_bytes_consistent() {
        // The eviction path drops the sub-map (when it empties) before
        // clearing the mark, so the per-namespace credit must not be
        // double-subtracted. A budget tight enough to force an eviction of
        // a marked, still-present entry exercises exactly that ordering.
        let mut cache = Cache::new(2 * (2 + ENTRY_OVERHEAD_BYTES) + name_charge(b"ns"));
        cache.set(namespaced(b"ns", b"a"), Bytes::from_static(b"1"));
        cache.mark_migrated(&namespaced(b"ns", b"a"));
        // Inserting past the budget evicts the marked "a".
        cache.set(namespaced(b"ns", b"b"), Bytes::from_static(b"2"));

        let stats = cache.stats();
        let per_ns: usize = stats.namespaces.iter().map(|n| n.used_bytes).sum();
        assert_eq!(per_ns + name_charge(b"ns"), stats.used_bytes);
        assert_eq!(cache.get(&namespaced(b"ns", b"a")), None);
    }

    #[test]
    fn marking_an_already_marked_key_does_not_double_count_its_bytes() {
        let mut cache = Cache::new(2 * (2 + ENTRY_OVERHEAD_BYTES));

        cache.set(key(b"a"), Bytes::from_static(b"1"));
        cache.mark_migrated(&key(b"a"));
        cache.mark_migrated(&key(b"a")); // already marked — must not charge twice
        cache.unmark_migrated(&key(b"a")); // one unmark fully reverses one mark

        cache.set(key(b"b"), Bytes::from_static(b"2"));

        assert_eq!(cache.get(&key(b"a")), Some(Bytes::from_static(b"1")));
        assert_eq!(cache.get(&key(b"b")), Some(Bytes::from_static(b"2")));
    }

    #[test]
    fn sweep_expired_leaves_marked_entries_alone() {
        // Issue #62: until the join is confirmed, a dead copy must not be
        // reclaimed — only TTL expiry is swept in this mode.
        let mut cache = Cache::new(UNBOUNDED);
        cache.set(key(b"dead"), Bytes::from_static(b"copy"));
        cache.mark_migrated(&key(b"dead"));
        cache.set_with_ttl(
            key(b"ttl"),
            Bytes::from_static(b"x"),
            Duration::from_secs(1),
        );
        let later = Instant::now() + Duration::from_secs(2);

        assert_eq!(cache.sweep_at(later, false), 1);
        assert_eq!(cache.get(&key(b"dead")), Some(Bytes::from_static(b"copy")));
        assert!(cache.get_at(&key(b"ttl"), later).is_none());

        // The mark is still in force once marks are swept again.
        assert_eq!(cache.sweep_at(later, true), 1);
        assert!(cache.get(&key(b"dead")).is_none());
    }

    #[test]
    fn sweep_does_not_remove_a_value_rewritten_after_its_mark() {
        // Regression for issue #2: a mark refers to the value that was
        // handed off, not to the key forever — deleting the marked value
        // and writing a fresh one must not condemn the fresh one.
        let mut cache = Cache::new(UNBOUNDED);

        cache.set(key(b"name"), Bytes::from_static(b"Alice"));
        cache.mark_migrated(&key(b"name"));
        cache.delete(&key(b"name"));
        cache.set(key(b"name"), Bytes::from_static(b"Bob"));

        assert_eq!(cache.sweep(), 0);
        assert_eq!(cache.get(&key(b"name")), Some(Bytes::from_static(b"Bob")));
    }

    #[test]
    fn overwriting_a_marked_key_clears_the_mark() {
        let mut cache = Cache::new(UNBOUNDED);

        cache.set(key(b"name"), Bytes::from_static(b"Alice"));
        cache.mark_migrated(&key(b"name"));
        cache.set(key(b"name"), Bytes::from_static(b"Bob"));

        assert_eq!(cache.sweep(), 0);
        assert_eq!(cache.get(&key(b"name")), Some(Bytes::from_static(b"Bob")));
    }

    #[test]
    fn eviction_clears_the_mark_for_the_evicted_key() {
        // Room for roughly one small entry at a time, so the second set
        // evicts the first.
        let mut cache = Cache::new(16);

        cache.set(key(b"aaaa"), Bytes::from_static(b"11111111"));
        cache.mark_migrated(&key(b"aaaa"));
        cache.set(key(b"bbbb"), Bytes::from_static(b"22222222")); // evicts "aaaa"
        cache.set(key(b"aaaa"), Bytes::from_static(b"33333333")); // fresh value

        cache.sweep();
        assert_eq!(
            cache.get(&key(b"aaaa")),
            Some(Bytes::from_static(b"33333333"))
        );
    }

    #[test]
    fn a_key_rewritten_while_queued_for_removal_survives_the_sweep() {
        // Regression for the same staleness through `pending_removal`: with
        // more expired keys than one sweep's budget, a key can sit queued
        // across sweeps; rewriting it fresh in that window must not let the
        // stale queue entry delete the new value.
        let mut cache = Cache::new(UNBOUNDED);
        let now = Instant::now();

        for i in 0..(SWEEP_BUDGET + 1) {
            cache.set_with_ttl(
                Key::from(Bytes::from(format!("key-{i}"))),
                Bytes::from_static(b"old"),
                Duration::from_secs(1),
            );
        }

        let later = now + Duration::from_secs(60);
        assert_eq!(cache.sweep_at(later, true), SWEEP_BUDGET);

        // Exactly one expired key remains, and it is still queued. Rewrite
        // it with a fresh, unexpiring value before the next sweep round.
        let leftover = cache
            .keys_at(Instant::now())
            .into_iter()
            .next()
            .expect("one expired entry should remain after the budgeted sweep");
        cache.set(leftover.clone(), Bytes::from_static(b"fresh"));

        cache.sweep_at(later, true);
        assert_eq!(
            cache.get_at(&leftover, later),
            Some(Bytes::from_static(b"fresh"))
        );
    }

    #[test]
    fn unmark_migrated_keeps_sweep_from_removing_the_entry() {
        let mut cache = Cache::new(UNBOUNDED);

        cache.set(key(b"name"), Bytes::from_static(b"Alice"));
        cache.mark_migrated(&key(b"name"));
        cache.unmark_migrated(&key(b"name"));

        assert_eq!(cache.sweep(), 0);
        assert_eq!(cache.get(&key(b"name")), Some(Bytes::from_static(b"Alice")));
    }

    #[test]
    fn unmark_migrated_is_a_no_op_for_a_key_that_was_never_marked() {
        let mut cache = Cache::new(UNBOUNDED);

        cache.set(key(b"name"), Bytes::from_static(b"Alice"));
        cache.unmark_migrated(&key(b"name"));

        assert_eq!(cache.get(&key(b"name")), Some(Bytes::from_static(b"Alice")));
    }

    #[test]
    fn sweep_removes_a_marked_entry_and_reports_it_removed() {
        let mut cache = Cache::new(UNBOUNDED);

        cache.set(key(b"name"), Bytes::from_static(b"Alice"));
        cache.mark_migrated(&key(b"name"));

        assert_eq!(cache.sweep(), 1);
        assert_eq!(cache.get(&key(b"name")), None);
    }

    #[test]
    fn sweep_does_not_touch_an_unmarked_entry() {
        let mut cache = Cache::new(UNBOUNDED);

        cache.set(key(b"name"), Bytes::from_static(b"Alice"));

        assert_eq!(cache.sweep(), 0);
        assert_eq!(cache.get(&key(b"name")), Some(Bytes::from_static(b"Alice")));
    }

    #[test]
    fn sweep_frees_the_memory_a_marked_entry_used() {
        let mut cache = Cache::new(UNBOUNDED);

        cache.set(key(b"name"), Bytes::from_static(b"Alice"));
        cache.mark_migrated(&key(b"name"));
        cache.sweep();

        cache.set(key(b"other"), Bytes::from_static(b"Bob"));

        assert_eq!(cache.get(&key(b"other")), Some(Bytes::from_static(b"Bob")));
    }

    #[test]
    fn sweep_proactively_removes_an_expired_entry_without_being_read_first() {
        let mut cache = Cache::new(UNBOUNDED);

        cache.set_with_ttl(
            key(b"name"),
            Bytes::from_static(b"Alice"),
            Duration::from_secs(5),
        );

        let removed = cache.sweep_at(Instant::now() + Duration::from_secs(6), true);

        assert_eq!(removed, 1);
    }

    #[test]
    fn sweep_does_not_remove_an_entry_that_has_not_expired() {
        let mut cache = Cache::new(UNBOUNDED);

        cache.set_with_ttl(
            key(b"name"),
            Bytes::from_static(b"Alice"),
            Duration::from_secs(5),
        );

        assert_eq!(cache.sweep(), 0);
        assert_eq!(cache.get(&key(b"name")), Some(Bytes::from_static(b"Alice")));
    }

    #[test]
    fn sweep_only_counts_each_entry_once_when_both_marked_and_expired() {
        let mut cache = Cache::new(UNBOUNDED);

        cache.set_with_ttl(
            key(b"name"),
            Bytes::from_static(b"Alice"),
            Duration::from_secs(5),
        );
        cache.mark_migrated(&key(b"name"));

        let removed = cache.sweep_at(Instant::now() + Duration::from_secs(6), true);

        assert_eq!(removed, 1);
    }

    #[test]
    fn sweep_removes_at_most_sweep_budget_entries_per_call() {
        let mut cache = Cache::new(UNBOUNDED);

        for i in 0..(SWEEP_BUDGET + 500) {
            let key = key(format!("key-{i}").as_bytes());
            cache.set(key.clone(), Bytes::from_static(b"x"));
            cache.mark_migrated(&key);
        }

        assert_eq!(cache.sweep(), SWEEP_BUDGET);
        assert_eq!(cache.sweep(), 500);
        assert_eq!(cache.sweep(), 0);
    }

    #[test]
    #[ignore]
    fn perf_one_sweep_chunk_against_a_large_removal_backlog() {
        let mut cache = Cache::new(UNBOUNDED);

        for i in 0..1_000_000u32 {
            let key = key(format!("key-{i}").as_bytes());
            let value = Bytes::copy_from_slice(format!("value-{i}").as_bytes());
            cache.set(key, value);
        }

        for i in 0..250_000u32 {
            cache.mark_migrated(&key(format!("key-{i}").as_bytes()));
        }

        // First call also pays the one-time refill scan (see
        // `pending_removal`); this is the worst-case single blocking call.
        let start = std::time::Instant::now();
        let removed = cache.sweep();
        let first_call = start.elapsed();

        let start = std::time::Instant::now();
        let mut total_removed = removed;
        while total_removed < 250_000 {
            total_removed += cache.sweep();
        }
        let total_elapsed = start.elapsed();

        eprintln!(
            "first sweep() call (refill + one {SWEEP_BUDGET}-entry chunk, {removed} removed) \
             took {first_call:?}; draining the remaining {} marked entries took {total_elapsed:?} more",
            250_000 - removed
        );
    }

    #[test]
    #[ignore]
    fn perf_a_ttl_only_sweep_scan_with_nothing_marked() {
        let mut cache = Cache::new(UNBOUNDED);

        for i in 0..1_000_000u32 {
            let key = key(format!("key-{i}").as_bytes());
            let value = Bytes::copy_from_slice(format!("value-{i}").as_bytes());
            cache.set_with_ttl(key, value, Duration::from_secs(3600));
        }

        let start = std::time::Instant::now();
        let removed = cache.sweep();
        let elapsed = start.elapsed();

        eprintln!("scanned 1_000_000 non-expired TTL'd entries, removed {removed}, in {elapsed:?}");
    }

    // Issue: namespace-length cost. A namespace name may be ~1 MiB, so
    // anything that resolves it per key makes one frame of tiny keys cost
    // (keys x name length) bytes of hashing on the single cache actor.

    fn big_namespace(len: usize) -> Bytes {
        Bytes::from(vec![b'n'; len])
    }

    fn names(count: usize) -> Vec<Bytes> {
        (0..count)
            .map(|i| Bytes::from(format!("k{i}").into_bytes()))
            .collect()
    }

    #[test]
    fn set_many_resolves_the_namespace_once_however_many_keys() {
        let mut cache = Cache::new(UNBOUNDED);
        let namespace = big_namespace(64 * 1024);
        let names = names(2_000);

        cache.set_many(
            &namespace,
            names
                .iter()
                .cloned()
                .map(|name| (name, Bytes::from_static(b"v"))),
            None,
        );

        // One lookup to find (and then create) the namespace — not one per
        // key, which would be 2_000.
        assert_eq!(cache.name_lookups.get(), 1);
        assert_eq!(cache.len(), 2_000);
    }

    #[test]
    fn get_many_resolves_the_namespace_once_however_many_keys() {
        let mut cache = Cache::new(UNBOUNDED);
        let namespace = big_namespace(64 * 1024);
        let names = names(2_000);
        cache.set_many(
            &namespace,
            names
                .iter()
                .cloned()
                .map(|name| (name, Bytes::from_static(b"v"))),
            None,
        );
        let before = cache.name_lookups.get();

        let results = cache.get_many(&namespace, &names, UNBOUNDED);

        assert_eq!(cache.name_lookups.get() - before, 1);
        assert!(
            results
                .iter()
                .all(|value| value.as_deref() == Some(&b"v"[..]))
        );
    }

    #[test]
    fn set_many_resolves_once_even_while_evicting_and_budget_trimming() {
        // The evicting steady state is where a re-resolve-after-eviction
        // design would pay per key: every write here evicts, and the
        // namespace is budgeted too.
        let namespace = big_namespace(4 * 1024);
        let mut cache = Cache::with_budgets(
            10 * (3 + 1 + ENTRY_OVERHEAD_BYTES) + name_charge(&namespace),
            vec![(namespace.clone(), 5 * (3 + 1 + ENTRY_OVERHEAD_BYTES))],
        );
        let names = names(500);

        cache.set_many(
            &namespace,
            names
                .iter()
                .cloned()
                .map(|name| (name, Bytes::from_static(b"v"))),
            None,
        );

        assert_eq!(cache.name_lookups.get(), 1);
        assert!(cache.evictions >= 400);
        assert!(cache.len() <= 5);
    }

    #[test]
    fn keys_walks_do_not_hash_the_namespace_per_key() {
        // Migration, decommission and sweep loops work over `keys()`,
        // whose namespaces are clones of the stored allocation; peeking
        // and marking them must not hash the name each time.
        let mut cache = Cache::new(UNBOUNDED);
        let namespace = big_namespace(64 * 1024);
        cache.set_many(
            &namespace,
            names(1_000)
                .into_iter()
                .map(|name| (name, Bytes::from_static(b"v"))),
            None,
        );
        let before = cache.name_lookups.get();

        for key in cache.keys() {
            assert!(cache.peek_entry(&key).is_some());
            cache.mark_migrated(&key);
        }

        assert_eq!(cache.name_lookups.get(), before);
        assert_eq!(cache.marked, 1_000);
    }

    #[test]
    fn sweep_does_not_hash_the_namespace_per_key() {
        let mut cache = Cache::new(UNBOUNDED);
        let namespace = big_namespace(64 * 1024);
        cache.set_many(
            &namespace,
            names(1_000)
                .into_iter()
                .map(|name| (name, Bytes::from_static(b"v"))),
            None,
        );
        for key in cache.keys() {
            cache.mark_migrated(&key);
        }
        let before = cache.name_lookups.get();

        let removed = cache.sweep();

        assert_eq!(removed, 1_000);
        assert_eq!(cache.name_lookups.get(), before);
        assert_eq!(cache.used_bytes, 0);
    }

    #[test]
    fn batched_writes_and_reads_match_the_per_key_path() {
        // Differential: same results, counters and accounting as the same
        // operations one key at a time, across a mark, an overwrite, a
        // TTL, a tight memory bound and a budget.
        let namespace = Bytes::from_static(b"users");
        let build = || {
            Cache::with_budgets(
                6 * (3 + 2 + ENTRY_OVERHEAD_BYTES) + name_charge(&namespace),
                vec![(namespace.clone(), 4 * (3 + 2 + ENTRY_OVERHEAD_BYTES))],
            )
        };
        let mut batched = build();
        let mut single = build();
        let items: Vec<(Bytes, Bytes)> = (0..12)
            .map(|i| {
                (
                    Bytes::from(format!("k{}", i % 9).into_bytes()),
                    Bytes::from(format!("v{}", i % 10).into_bytes()),
                )
            })
            .collect();

        for cache in [&mut batched, &mut single] {
            cache.set(
                Key::new(namespace.clone(), Bytes::from_static(b"k0")),
                Bytes::from_static(b"zz"),
            );
            cache.mark_migrated(&Key::new(namespace.clone(), Bytes::from_static(b"k0")));
        }
        batched.set_many(
            &namespace,
            items.iter().cloned(),
            Some(Duration::from_secs(60)),
        );
        for (name, value) in &items {
            single.set_with_ttl(
                Key::new(namespace.clone(), name.clone()),
                value.clone(),
                Duration::from_secs(60),
            );
        }

        let probe: Vec<Bytes> = (0..10)
            .map(|i| Bytes::from(format!("k{i}").into_bytes()))
            .collect();
        let batched_reads = batched.get_many(&namespace, &probe, UNBOUNDED);
        let single_reads: Vec<Option<Bytes>> = probe
            .iter()
            .map(|name| single.get(&Key::new(namespace.clone(), name.clone())))
            .collect();

        assert_eq!(batched_reads, single_reads);
        assert_eq!(batched.used_bytes, single.used_bytes);
        assert_eq!(batched.entry_count, single.entry_count);
        assert_eq!(batched.marked, single.marked);
        let (a, b) = (batched.stats(), single.stats());
        assert_eq!(
            (a.hits, a.misses, a.sets, a.evictions),
            (b.hits, b.misses, b.sets, b.evictions)
        );
        assert_eq!(a.namespaces, b.namespaces);
    }

    #[test]
    fn get_many_caps_the_values_it_returns_and_skips_lookups_past_the_cap() {
        let mut cache = Cache::new(UNBOUNDED);
        let namespace = Bytes::from_static(b"ns");
        cache.set_many(
            &namespace,
            names(4)
                .into_iter()
                .map(|name| (name, Bytes::from_static(b"vvvv"))),
            None,
        );
        let hits_before = cache.hits;

        // Room for two 4-byte values: the third is skipped unlooked-up.
        let results = cache.get_many(&namespace, &names(4), 8);

        assert_eq!(results.iter().filter(|value| value.is_some()).count(), 2);
        assert_eq!(cache.hits - hits_before, 2);
    }

    #[test]
    fn get_many_on_a_missing_namespace_is_all_misses() {
        let mut cache = Cache::new(UNBOUNDED);

        let results = cache.get_many(&Bytes::from_static(b"nope"), &names(3), UNBOUNDED);

        assert_eq!(results, vec![None, None, None]);
        assert_eq!(cache.misses, 3);
    }

    #[test]
    fn get_many_survives_its_namespace_emptying_on_lazy_expiry() {
        // The first key's lazy expiry removes the namespace's only entry,
        // dropping the sub-map mid-batch; later names must simply miss.
        let mut cache = Cache::new(UNBOUNDED);
        let namespace = Bytes::from_static(b"ns");
        cache.set_with_ttl(
            Key::new(namespace.clone(), Bytes::from_static(b"k0")),
            Bytes::from_static(b"v"),
            Duration::from_millis(1),
        );
        std::thread::sleep(Duration::from_millis(10));

        let results = cache.get_many(&namespace, &names(3), UNBOUNDED);

        assert_eq!(results, vec![None, None, None]);
        assert!(cache.namespaces.is_empty());
        assert_eq!(cache.used_bytes, 0);
    }

    #[test]
    fn a_namespace_name_is_charged_to_used_bytes_and_released_with_it() {
        let mut cache = Cache::new(UNBOUNDED);
        let namespace = big_namespace(10_000);
        let name = Bytes::from_static(b"k");

        cache.set(
            Key::new(namespace.clone(), name.clone()),
            Bytes::from_static(b"v"),
        );
        let entry = 1 + 1 + ENTRY_OVERHEAD_BYTES;
        assert_eq!(cache.used_bytes, entry + 10_000 + NAMESPACE_OVERHEAD_BYTES);

        // A second entry in the same namespace is not charged the name
        // again.
        cache.set(
            Key::new(namespace.clone(), Bytes::from_static(b"j")),
            Bytes::from_static(b"v"),
        );
        assert_eq!(
            cache.used_bytes,
            2 * entry + 10_000 + NAMESPACE_OVERHEAD_BYTES
        );

        // The per-namespace row (and so `--namespace-budget`) stays
        // entries only.
        assert_eq!(cache.stats().namespaces[0].used_bytes, 2 * entry);

        cache.delete(&Key::new(namespace.clone(), name));
        cache.delete(&Key::new(namespace, Bytes::from_static(b"j")));
        assert_eq!(cache.used_bytes, 0);
    }

    #[test]
    fn the_default_namespace_is_not_charged_a_name() {
        let mut cache = Cache::new(UNBOUNDED);

        cache.set(key(b"k"), Bytes::from_static(b"v"));

        assert_eq!(cache.used_bytes, 1 + 1 + ENTRY_OVERHEAD_BYTES);
    }

    #[test]
    fn namespace_charge_is_released_by_clear_clear_all_and_eviction() {
        let namespace = big_namespace(1_000);
        let one = |cache: &mut Cache, ns: &Bytes| {
            cache.set(
                Key::new(ns.clone(), Bytes::from_static(b"k")),
                Bytes::from_static(b"v"),
            );
        };

        let mut cache = Cache::new(UNBOUNDED);
        one(&mut cache, &namespace);
        assert_eq!(cache.clear(&namespace), 1);
        assert_eq!(cache.used_bytes, 0);

        one(&mut cache, &namespace);
        one(&mut cache, &Bytes::from_static(b"other"));
        assert_eq!(cache.clear_all(), 2);
        assert_eq!(cache.used_bytes, 0);

        // Eviction: the big namespace's one entry is the global LRU victim.
        let mut cache = Cache::new(500);
        one(&mut cache, &namespace);
        one(&mut cache, &Bytes::from_static(b"other"));
        assert_eq!(cache.evictions, 1);
        assert_eq!(cache.namespaces.len(), 1);
        assert_eq!(
            cache.used_bytes,
            1 + 1 + ENTRY_OVERHEAD_BYTES + name_charge(b"other")
        );
    }

    #[test]
    fn a_stream_of_fresh_large_namespaces_with_tiny_entries_is_bounded_by_max_memory() {
        // Before the name was charged, each of these cost ~102 bytes of
        // accounting while the process held the whole 20 KB name, so
        // `--max-memory` never evicted.
        let max_memory = 200_000;
        let mut cache = Cache::new(max_memory);

        for i in 0..100u32 {
            let mut namespace = vec![b'n'; 20_000];
            namespace[..4].copy_from_slice(&i.to_be_bytes());
            cache.set(
                Key::new(Bytes::from(namespace), Bytes::from_static(b"k")),
                Bytes::from_static(b"v"),
            );
        }

        assert!(cache.evictions > 0);
        assert!(cache.namespaces.len() < 15);
        let held: usize = cache.namespaces.keys().map(Bytes::len).sum();
        assert!(held <= max_memory, "{held} bytes of namespace names kept");
        assert!(cache.used_bytes <= max_memory + 20_000 + NAMESPACE_OVERHEAD_BYTES);
    }
}
