use crate::key::Key;
use bytes::Bytes;
use lru::LruCache;
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
const ENTRY_OVERHEAD_BYTES: usize = 100;

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

/// One namespace's sub-map plus its own byte accounting, so `clear`
/// (issue #106) can drop the whole thing and credit `Cache::used_bytes`
/// in O(1) without walking the entries.
struct Namespace {
    entries: Entries,
    /// This namespace's share of `Cache::used_bytes`: key + value +
    /// `ENTRY_OVERHEAD_BYTES` per entry (the `migrated` marks' duplicate
    /// bytes are accounted globally, not here).
    used_bytes: usize,
}

impl Namespace {
    fn new() -> Self {
        Self {
            entries: LruCache::unbounded_with_hasher(RandomState::new()),
            used_bytes: 0,
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
    /// One sub-map per namespace — the default namespace lives under the
    /// empty key (issue #105). A sub-map exists exactly while it holds at
    /// least one entry, so the per-eviction scan over namespaces
    /// (`evict_one`) is bounded by the number of *live* namespaces, and
    /// `CLEAR <ns>` (issue #106) is a single O(1) sub-map drop. Same
    /// `RandomState` reasoning as `Entries`: namespace names are
    /// attacker-controlled too.
    namespaces: HashMap<Bytes, Namespace, RandomState>,
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
    /// Issue #124: client-visible operation counters, snapshotted by
    /// `stats()` for the metrics endpoint. Plain fields, not atomics —
    /// the cache actor is single-threaded.
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
    /// Keys handed off to another node during an staged node join migration this
    /// node was the source for, awaiting `sweep`'s next pass.
    migrated: HashSet<Key>,
    /// Expired/marked keys queued for removal by `sweep`, in
    /// `SWEEP_BUDGET`-sized bites. Refilled (by scanning `namespaces` for
    /// expired keys and draining `migrated`) only once this drains empty,
    /// so an in-progress sweep pass isn't rescanned from scratch every
    /// call.
    pending_removal: VecDeque<Key>,
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
            migrated: HashSet::new(),
            pending_removal: VecDeque::new(),
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
                content_digest(&entry.value) == expected
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

    /// The shared accounting-safe overwrite path for every write: `set`/
    /// `set_with_ttl` (which bump `self.sets`) and `incr_at` (which bumps
    /// `self.incrs` instead — see its own doc comment for why counting an
    /// INCR as a `sets_total` GET/SET-shaped write would be misleading).
    fn insert(&mut self, key: Key, value: Bytes, expires_at: Option<Instant>) {
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

        // A fresh write is not the value a handoff transferred: a stale
        // `migrated` mark left over from an earlier value must not condemn
        // this one to the next sweep (it would silently delete it).
        self.clear_migrated_mark(&key);

        let namespace = self
            .namespaces
            .entry(key.namespace.clone())
            .or_insert_with(Namespace::new);

        // An overwrite keeps the stored key (`LruCache::put` would too, and
        // discard the copy), so only copy the key for a genuinely new
        // entry. `get_mut` promotes to most-recently-used like `put`.
        if let Some(existing) = namespace.entries.get_mut(&key.name[..]) {
            let replaced = std::mem::replace(existing, entry);
            let delta = value_len as isize - replaced.value.len() as isize;
            namespace.used_bytes = namespace.used_bytes.wrapping_add_signed(delta);
            self.used_bytes = self.used_bytes.wrapping_add_signed(delta);
        } else {
            let name = Bytes::copy_from_slice(&key.name);
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
        self.enforce_namespace_budget(&key.namespace);

        // Evict least-recently-used entries until the cache fits its memory
        // budget, but never evict the entry just inserted above: it is
        // always the most-recently-used one, so `evict_one` would only
        // reach it once nothing else is left.
        while self.used_bytes > self.max_memory_bytes && self.entry_count > 1 {
            self.evict_one();
        }
    }

    /// Issue #127: evicts this namespace's least-recently-used entries
    /// until it fits its `--namespace-budget`, if it has one. Mirrors the
    /// global loop's one-entry floor: the entry just inserted is its
    /// namespace's most-recently-used, so it survives even when it alone
    /// exceeds the budget (exactly how a single oversized entry is
    /// allowed to exceed `--max-memory`).
    fn enforce_namespace_budget(&mut self, namespace_name: &Bytes) {
        let Some(&budget) = self.budgets.get(namespace_name) else {
            return;
        };

        loop {
            let Some(namespace) = self.namespaces.get(namespace_name) else {
                return;
            };
            if namespace.used_bytes <= budget || namespace.entries.len() <= 1 {
                return;
            }
            self.evict_one_from(namespace_name.clone());
        }
    }

    /// Removes the least-recently-used entry across *all* namespaces: each
    /// sub-map keeps its own recency order, so the global LRU victim is
    /// the oldest of the sub-maps' tails by `Entry::last_used`. O(number
    /// of live namespaces) per eviction — a handful for the framework
    /// named-cache workloads namespaces exist for (issue #105), and only
    /// ever paid while over the memory bound.
    fn evict_one(&mut self) {
        let victim_namespace = self
            .namespaces
            .iter()
            .filter_map(|(name, namespace)| {
                namespace
                    .entries
                    .peek_lru()
                    .map(|(_, entry)| (entry.last_used, name))
            })
            .min_by_key(|(last_used, _)| *last_used)
            .map(|(_, namespace)| namespace.clone())
            .expect("entry_count > 1 guarantees an entry to evict");

        self.evict_one_from(victim_namespace);
    }

    /// Removes `namespace_name`'s least-recently-used entry — the shared
    /// tail of the global `evict_one` (which picks the victim namespace
    /// first) and the per-namespace budget loop (issue #127, where the
    /// victim namespace is the one over its budget).
    fn evict_one_from(&mut self, namespace_name: Bytes) {
        self.evictions += 1;
        let namespace = self
            .namespaces
            .get_mut(&namespace_name)
            .expect("the victim namespace exists");
        let (evicted_name, evicted_entry) = namespace
            .entries
            .pop_lru()
            .expect("the victim namespace is non-empty");
        let entry_bytes = evicted_name.len() + evicted_entry.value.len() + ENTRY_OVERHEAD_BYTES;
        namespace.used_bytes -= entry_bytes;
        if namespace.entries.is_empty() {
            self.namespaces.remove(&namespace_name);
        }

        self.entry_count -= 1;
        self.used_bytes -= entry_bytes;
        // The marked value is gone; a future entry under this key is a
        // different value and must not inherit the mark.
        self.clear_migrated_mark(&Key::new(namespace_name, evicted_name));
    }

    fn get_at(&mut self, key: &Key, now: Instant) -> Option<Bytes> {
        let last_used = self.tick();
        let Some(entry) = self
            .namespaces
            .get_mut(&key.namespace)
            .and_then(|namespace| namespace.entries.get_mut(&key.name[..]))
        else {
            self.misses += 1;
            return None;
        };

        if entry.is_expired_at(now) {
            self.remove_entry(key);
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
        self.namespaces
            .get(&key.namespace)?
            .entries
            .peek(&key.name[..])
    }

    fn contains(&self, key: &Key) -> bool {
        self.peek(key).is_some()
    }

    fn remove_entry(&mut self, key: &Key) -> Option<Entry> {
        let namespace = self.namespaces.get_mut(&key.namespace)?;
        let entry = namespace.entries.pop(&key.name[..])?;
        let entry_bytes = key.name.len() + entry.value.len() + ENTRY_OVERHEAD_BYTES;
        namespace.used_bytes -= entry_bytes;
        if namespace.entries.is_empty() {
            self.namespaces.remove(&key.namespace);
        }
        self.entry_count -= 1;
        self.used_bytes -= entry_bytes;
        // The mark referred to this entry's value; whatever is stored
        // under the key later is a different value.
        self.clear_migrated_mark(key);
        Some(entry)
    }

    /// Removes any mark for `key` from `migrated`, crediting `used_bytes`
    /// back for the duplicate key copy `mark_migrated` stored there — a
    /// no-op, memory accounting included, if `key` wasn't marked.
    fn clear_migrated_mark(&mut self, key: &Key) {
        if self.migrated.remove(key) {
            self.used_bytes -= key.namespace.len() + key.name.len();
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
    /// walk the whole cache — the walk itself isn't chunked/budgeted (that
    /// would need the migration protocol to support resuming a partial
    /// listing across multiple round trips instead of one; left as a
    /// larger follow-up), only the per-entry value/TTL work it used to
    /// also do is gone. `src/server.rs`'s `handle_connection` (the `M`
    /// handler) takes exactly one such snapshot per migration and reuses
    /// it for both `entries_to_send_count` and `run_migration`, rather
    /// than calling this twice.
    pub fn keys(&self) -> Vec<Key> {
        self.keys_at(Instant::now())
    }

    fn keys_at(&self, now: Instant) -> Vec<Key> {
        self.namespaces
            .iter()
            .flat_map(|(namespace, sub_map)| {
                sub_map
                    .entries
                    .iter()
                    .filter(move |(_, entry)| !entry.is_expired_at(now))
                    .map(move |(name, _)| Key::new(namespace.clone(), name.clone()))
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
    /// longer present; `sweep` reclaims marked entries later. `migrated`
    /// holds its own copy of the key bytes (see its field docs), so a
    /// freshly marked key costs `used_bytes` an extra namespace+name
    /// length — the audit behind issue #19 flagged this duplicate as
    /// otherwise invisible to the memory limit.
    pub fn mark_migrated(&mut self, key: &Key) {
        if self.contains(key) && self.migrated.insert(key.clone()) {
            self.used_bytes += key.namespace.len() + key.name.len();
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
            .namespaces
            .iter()
            .map(|(name, sub_map)| NamespaceStats {
                namespace: name.clone(),
                entries: sub_map.entries.len(),
                used_bytes: sub_map.used_bytes,
                budget_bytes: self.budgets.get(name).copied(),
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
    /// entries went. The namespace's `migrated` marks go with it — the
    /// values they referred to no longer exist, and a mark must never
    /// outlive its value (a later write under the same key would
    /// otherwise inherit it and be swept, see `insert`). That retain is
    /// O(marks), and marks only exist while a handoff is in flight.
    pub fn clear(&mut self, namespace: &[u8]) -> usize {
        let Some(dropped) = self.namespaces.remove(namespace) else {
            return 0;
        };

        let removed = dropped.entries.len();
        self.entry_count -= removed;
        self.used_bytes -= dropped.used_bytes;
        self.retain_marks(|key| &key.namespace[..] != namespace);

        removed
    }

    /// Whole-store flush (issue #106): every namespace, the default one
    /// included. Same O(#namespaces) shape as `clear`.
    pub fn clear_all(&mut self) -> usize {
        let removed = self.entry_count;
        self.namespaces.clear();
        self.entry_count = 0;
        self.used_bytes = 0;
        self.migrated.clear();
        // `used_bytes` is already 0; the marks' duplicate bytes went with
        // everything else.
        removed
    }

    /// Drops every `migrated` mark `keep` rejects, crediting `used_bytes`
    /// for each — `clear_migrated_mark` over a predicate.
    fn retain_marks(&mut self, keep: impl Fn(&Key) -> bool) {
        let mut credited = 0usize;
        self.migrated.retain(|key| {
            let kept = keep(key);
            if !kept {
                credited += key.namespace.len() + key.name.len();
            }
            kept
        });
        self.used_bytes -= credited;
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
                .extend(self.namespaces.iter().flat_map(|(namespace, sub_map)| {
                    sub_map
                        .entries
                        .iter()
                        .filter(move |(_, entry)| entry.is_expired_at(now))
                        .map(move |(name, _)| Key::new(namespace.clone(), name.clone()))
                }));
            // Marks stay in `migrated` until the moment of removal (not
            // drained here): the queue is only a snapshot of candidates,
            // and a key rewritten after this point clears its mark, which
            // the removability re-check below must still observe.
            if include_marked {
                self.pending_removal.extend(self.migrated.iter().cloned());
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
            let marked = include_marked && self.migrated.contains(&key);
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
            2 * SMALL_ENTRY,
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
        // exactly three.
        let mut cache = Cache::new(3 * 106);

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
        let mut cache = Cache::new(3 * 106);

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
            before - (1 + 2 + ENTRY_OVERHEAD_BYTES) - (1 + 3 + ENTRY_OVERHEAD_BYTES)
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
        assert!(cache.migrated.is_empty());
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
            marked_bytes - (5 + 1) - (1 + 3 + ENTRY_OVERHEAD_BYTES)
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
        let y_bytes = 1 + 4 + ENTRY_OVERHEAD_BYTES;

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
}
