using System.Buffers.Binary;
using System.Text;
using Microsoft.Extensions.Caching.Distributed;

namespace Nanocached.Caching;

/// <summary>
/// <see cref="IDistributedCache"/> on top of one nanocached namespace
/// (<see cref="NanocachedNamespace"/>) — issue #108. One instance binds to
/// exactly one namespace: two instances with different namespaces are
/// fully isolated, even over the same keys, since namespaces enter cluster
/// routing (docs/protocol.html's "g / s / d" section) — mirrors the Spring
/// adapter's "named cache ⇄ namespace" rule (adapters/spring/README.md),
/// just without a manager, since the SPI here has exactly one cache per
/// registration rather than Spring's named-cache lookup. The wire has no
/// namespace-scoped Clear counterpart on this SPI (<c>IDistributedCache</c>
/// itself has no Clear method), so none is exposed here.
///
/// <para>The SDK is async-only; <see cref="Get"/>/<see cref="Set"/>/
/// <see cref="Refresh"/>/<see cref="Remove"/> block on their async
/// counterparts (<c>.GetAwaiter().GetResult()</c>) rather than duplicating
/// any networking.</para>
///
/// <para><b>Sliding expiration.</b> nanocached's TTL is a one-shot
/// countdown with no server-side notion of "renew on read" — a key's TTL
/// never changes just because it was read. This adapter emulates a
/// sliding window client-side: every value is wrapped in a small envelope
/// (see the private <c>Envelope</c> type) recording the configured sliding
/// window and/or absolute expiry alongside the payload, so a later
/// <see cref="Get"/>/<see cref="Refresh"/> can recompute the remaining TTL
/// and re-write the entry with it. The renewal is a conditional
/// compare-and-set against the exact bytes the read returned (issue #391):
/// a plain re-set would be a read-modify-write that silently clobbers a
/// concurrent <see cref="Set"/> landing between the read and the renewal —
/// a lost update with nothing signalling it. A renewal whose condition no
/// longer matches is simply skipped: whoever won wrote a fresher entry
/// (with its own TTL) than the one this renewal was about to restore. An
/// entry with no sliding window is never rewritten on <see cref="Get"/> —
/// its TTL (if any) is fixed regardless of access, so the extra round trip
/// would buy nothing.</para>
/// </summary>
public class NanocachedDistributedCache : IDistributedCache
{
    // Envelope layout (see the class doc comment): 1 version byte, a
    // 4-byte big-endian sliding-seconds field (0 = no sliding), an 8-byte
    // big-endian absolute-expiry Unix-seconds field (0 = no absolute
    // expiry), then the caller's payload untouched. Fixed-width and
    // versioned so a future format change can recognize — and reject
    // rather than misread — an entry written by an older adapter version.
    private const byte EnvelopeVersion = 0x01;
    private const int EnvelopeHeaderLength = 1 + 4 + 8;

    private readonly NanocachedNamespace _namespace;

    /// <summary>The nanocached namespace this cache instance is bound to
    /// (issue #108's shared spec: one adapter instance, one namespace).
    /// The SPI itself has no notion of this — exposed for an application
    /// that also talks to the same namespace directly through
    /// <see cref="NanocachedClient.Namespace(string)"/>.</summary>
    public string Namespace { get; }

    /// <summary>Binds to <paramref name="namespace"/> on
    /// <paramref name="client"/> — borrowed: this class never closes it,
    /// the caller that connected it does.</summary>
    public NanocachedDistributedCache(
        NanocachedClient client, string @namespace = NanocachedCacheOptions.DefaultNamespace)
    {
        ArgumentNullException.ThrowIfNull(client);
        ArgumentNullException.ThrowIfNull(@namespace);
        Namespace = @namespace;
        _namespace = client.Namespace(@namespace);
    }

    /// <summary>As the <c>(client, namespace)</c> constructor, taking the
    /// namespace from <paramref name="options"/> — used directly, or via
    /// <see cref="ServiceCollectionExtensions"/>' DI registration.</summary>
    public NanocachedDistributedCache(NanocachedClient client, NanocachedCacheOptions options)
        : this(client, (options ?? throw new ArgumentNullException(nameof(options))).Namespace)
    {
    }

    public byte[]? Get(string key) => GetAsync(key).GetAwaiter().GetResult();

    public async Task<byte[]?> GetAsync(string key, CancellationToken token = default)
    {
        ArgumentNullException.ThrowIfNull(key);
        token.ThrowIfCancellationRequested();

        byte[] keyBytes = Encoding.UTF8.GetBytes(key);
        (byte[] Value, string Token)? read = await _namespace.GetBytesWithTokenAsync(keyBytes).ConfigureAwait(false);
        if (read is not { } hit) return null;

        Envelope envelope = Envelope.Parse(hit.Value);
        if (envelope.SlidingSeconds == 0) return envelope.Payload; // fixed TTL (or none) — nothing to renew

        DateTimeOffset now = DateTimeOffset.UtcNow;
        if (envelope.IsPastAbsoluteExpiry(now))
        {
            // Issue #233: an absolute expiry already in the past (clock
            // skew, or this node just hasn't reclaimed it yet) must not
            // be floored to a 1-second TTL and resurrected by the
            // sliding renewal below — treat it as expired instead, the
            // same miss this call would answer once eviction actually
            // catches up. Conditional on the token (issue #391) so a
            // concurrent Set that just replaced the entry isn't deleted
            // along with the expired bytes this call actually read.
            await _namespace.DeleteIfMatchesAsync(keyBytes, hit.Token).ConfigureAwait(false);
            return null;
        }

        // Sliding expiration: re-write with the recomputed TTL before
        // returning — awaited, never fire-and-forget (shared spec), so a
        // caller that awaits this call can rely on the renewal having
        // actually reached the wire. Conditional on the read's token
        // (issue #391): an unconditional SetAsync here was a
        // read-modify-write that could clobber a concurrent Set landing
        // between the read above and this write. A false return means
        // exactly that — someone else just wrote a fresher entry — so the
        // renewal is skipped, not retried.
        long wireTtl = envelope.WireTtlSeconds(now);
        await _namespace.ReplaceAsync(keyBytes, hit.Token, envelope.ToBytes(), wireTtl).ConfigureAwait(false);
        return envelope.Payload;
    }

    public void Set(string key, byte[] value, DistributedCacheEntryOptions options) =>
        SetAsync(key, value, options).GetAwaiter().GetResult();

    public async Task SetAsync(
        string key, byte[] value, DistributedCacheEntryOptions options, CancellationToken token = default)
    {
        ArgumentNullException.ThrowIfNull(key);
        ArgumentNullException.ThrowIfNull(value);
        ArgumentNullException.ThrowIfNull(options);
        token.ThrowIfCancellationRequested();

        DateTimeOffset now = DateTimeOffset.UtcNow;
        Envelope envelope = Envelope.FromOptions(options, value, now);
        long wireTtl = envelope.WireTtlSeconds(now);
        await _namespace.SetAsync(Encoding.UTF8.GetBytes(key), envelope.ToBytes(), wireTtl).ConfigureAwait(false);
    }

    public void Refresh(string key) => RefreshAsync(key).GetAwaiter().GetResult();

    public async Task RefreshAsync(string key, CancellationToken token = default)
    {
        ArgumentNullException.ThrowIfNull(key);
        token.ThrowIfCancellationRequested();

        byte[] keyBytes = Encoding.UTF8.GetBytes(key);
        (byte[] Value, string Token)? read = await _namespace.GetBytesWithTokenAsync(keyBytes).ConfigureAwait(false);
        // A missing key is a no-op, per IDistributedCache's contract —
        // not a miss error.
        if (read is not { } hit) return;

        Envelope envelope = Envelope.Parse(hit.Value);
        // No sliding window: nothing a refresh would change (an absolute
        // expiry doesn't move; no expiry needs no TTL rewrite either) — so
        // skip the round trip rather than re-set identical content.
        if (envelope.SlidingSeconds == 0) return;

        DateTimeOffset now = DateTimeOffset.UtcNow;
        if (envelope.IsPastAbsoluteExpiry(now))
        {
            // Issue #233: see GetAsync's identical check — an already-past
            // absolute expiry must not be floored to a 1-second TTL and
            // resurrected by this refresh. Token-conditional for the same
            // reason as there (issue #391).
            await _namespace.DeleteIfMatchesAsync(keyBytes, hit.Token).ConfigureAwait(false);
            return;
        }

        // Token-conditional renewal — see GetAsync (issue #391): a lost
        // condition means a concurrent writer just refreshed the entry,
        // so there is nothing left for this renewal to do.
        long wireTtl = envelope.WireTtlSeconds(now);
        await _namespace.ReplaceAsync(keyBytes, hit.Token, envelope.ToBytes(), wireTtl).ConfigureAwait(false);
    }

    public void Remove(string key) => RemoveAsync(key).GetAwaiter().GetResult();

    public async Task RemoveAsync(string key, CancellationToken token = default)
    {
        ArgumentNullException.ThrowIfNull(key);
        token.ThrowIfCancellationRequested();
        await _namespace.DeleteAsync(Encoding.UTF8.GetBytes(key)).ConfigureAwait(false);
    }

    /// <summary>
    /// The wire envelope every value is stored under (see the class doc
    /// comment) — carries just enough of a <see cref="DistributedCacheEntryOptions"/>
    /// to recompute the TTL on a later Get/Refresh, since nanocached's own
    /// TTL is a one-shot countdown with no notion of "sliding".
    /// </summary>
    private readonly struct Envelope
    {
        internal readonly long SlidingSeconds; // 0 = no sliding window
        internal readonly long AbsoluteUnixSeconds; // 0 = no absolute expiry
        internal readonly byte[] Payload;

        // Issue #418: the sub-second remainder that AbsoluteUnixSeconds'
        // whole-second wire format necessarily discards (see FromOptions).
        // Populated only by FromOptions, for the immediate SetAsync write
        // that just computed it — never persisted, never present on an
        // envelope round-tripped through Parse (a later Get/Refresh
        // renewal), so those keep computing WireTtlSeconds purely from the
        // (floored) AbsoluteUnixSeconds, same as IsPastAbsoluteExpiry
        // already does (needed for issue #233's prompt "already past"
        // detection — rounding that up would delay it by up to 1s).
        private readonly DateTimeOffset? _preciseAbsoluteExpiry;

        private Envelope(
            long slidingSeconds, long absoluteUnixSeconds, byte[] payload,
            DateTimeOffset? preciseAbsoluteExpiry = null)
        {
            SlidingSeconds = slidingSeconds;
            AbsoluteUnixSeconds = absoluteUnixSeconds;
            Payload = payload;
            _preciseAbsoluteExpiry = preciseAbsoluteExpiry;
        }

        /// <summary>
        /// Resolves a <see cref="DistributedCacheEntryOptions"/> into the
        /// envelope's two TTL fields, mirroring
        /// <c>Microsoft.Extensions.Caching.Memory.MemoryDistributedCache</c>'s
        /// own validation: a past (or exactly-now) absolute expiration —
        /// whether given directly or computed from
        /// <see cref="DistributedCacheEntryOptions.AbsoluteExpirationRelativeToNow"/> —
        /// throws <see cref="ArgumentOutOfRangeException"/> rather than
        /// silently writing an already-dead entry, and a non-positive
        /// sliding/relative value throws the same way. When both
        /// <see cref="DistributedCacheEntryOptions.AbsoluteExpiration"/> and
        /// <see cref="DistributedCacheEntryOptions.AbsoluteExpirationRelativeToNow"/>
        /// are set, the earlier of the two wins — a decision beyond the
        /// spec, made so neither field can silently widen a window the
        /// other one narrowed.
        /// </summary>
        internal static Envelope FromOptions(DistributedCacheEntryOptions options, byte[] payload, DateTimeOffset now)
        {
            DateTimeOffset? absolute = options.AbsoluteExpiration;

            if (options.AbsoluteExpirationRelativeToNow is { } relative)
            {
                if (relative <= TimeSpan.Zero)
                {
                    throw new ArgumentOutOfRangeException(
                        nameof(DistributedCacheEntryOptions.AbsoluteExpirationRelativeToNow), relative,
                        "The relative expiration value must be positive.");
                }
                DateTimeOffset fromRelative = now + relative;
                absolute = absolute is { } explicitAbsolute ? Min(explicitAbsolute, fromRelative) : fromRelative;
            }

            if (absolute is { } absoluteValue && absoluteValue <= now)
            {
                throw new ArgumentOutOfRangeException(
                    nameof(DistributedCacheEntryOptions.AbsoluteExpiration), absoluteValue,
                    "The absolute expiration value must be in the future.");
            }

            long slidingSeconds = 0;
            if (options.SlidingExpiration is { } sliding)
            {
                if (sliding <= TimeSpan.Zero)
                {
                    throw new ArgumentOutOfRangeException(
                        nameof(DistributedCacheEntryOptions.SlidingExpiration), sliding,
                        "The sliding expiration value must be positive.");
                }
                slidingSeconds = CeilSeconds(sliding);
            }

            // AbsoluteUnixSeconds floors to whole seconds — required so
            // IsPastAbsoluteExpiry (issue #233) flags an already-passed
            // absolute expiry as soon as real time reaches it, rather than
            // rounding that detection up to a full second late. That same
            // floored field feeds WireTtlSeconds for a later Get/Refresh
            // renewal too, where the precise original instant below is no
            // longer available (only these wire bytes survive a round
            // trip) — a renewal's TTL is at most ~1s short of the caller's
            // original request, the same slop CeilSeconds already accepts
            // elsewhere on the wire.
            long absoluteUnixSeconds = absolute is { } a ? a.ToUnixTimeSeconds() : 0;
            // Issue #418: for *this* write, though, the precise instant is
            // still in hand — hand it to WireTtlSeconds below so it can
            // ceil the true remaining duration once, instead of ceiling an
            // already-floored (up to ~1s short) reconstruction of it, which
            // could turn e.g. a 5.9s-from-now expiration into a 5s wire TTL
            // instead of the intended 6 — expiring up to ~1s early.
            return new Envelope(slidingSeconds, absoluteUnixSeconds, payload, absolute);
        }

        private static DateTimeOffset Min(DateTimeOffset a, DateTimeOffset b) => a < b ? a : b;

        internal static Envelope Parse(byte[] raw)
        {
            if (raw.Length < EnvelopeHeaderLength || raw[0] != EnvelopeVersion)
            {
                throw new InvalidOperationException(
                    "nanocached.caching: stored value is not a recognized cache envelope — was this key "
                    + "written by something other than NanocachedDistributedCache?");
            }
            long sliding = BinaryPrimitives.ReadUInt32BigEndian(raw.AsSpan(1, 4));
            long absolute = BinaryPrimitives.ReadInt64BigEndian(raw.AsSpan(5, 8));
            byte[] payload = raw[EnvelopeHeaderLength..];
            return new Envelope(sliding, absolute, payload);
        }

        internal byte[] ToBytes()
        {
            var bytes = new byte[EnvelopeHeaderLength + Payload.Length];
            bytes[0] = EnvelopeVersion;
            // SlidingSeconds is a `long` (CeilSeconds(TimeSpan) — and
            // TimeSpan.SlidingExpiration can be nearly 922 billion seconds),
            // but the envelope's own wire field is only 4 bytes wide. An
            // unchecked `(uint)SlidingSeconds` cast silently wraps for any
            // value past ~136 years (issue #304), corrupting what a later
            // Get/Refresh renewal reads back into an arbitrary — often much
            // shorter, sometimes exactly 0 ("no expiry") — sliding window.
            // Clamp to uint.MaxValue instead: SlidingSeconds is never
            // negative (0 = no sliding window, or CeilSeconds' result,
            // which floors at 1), so only the upper bound needs guarding.
            // This only bounds what a *future* renewal recomputes from —
            // the immediate SetAsync call below already used the caller's
            // real, uncapped SlidingSeconds for this write's own wire TTL.
            uint slidingSecondsWire = SlidingSeconds > uint.MaxValue ? uint.MaxValue : (uint)SlidingSeconds;
            BinaryPrimitives.WriteUInt32BigEndian(bytes.AsSpan(1, 4), slidingSecondsWire);
            BinaryPrimitives.WriteInt64BigEndian(bytes.AsSpan(5, 8), AbsoluteUnixSeconds);
            Payload.CopyTo(bytes.AsSpan(EnvelopeHeaderLength));
            return bytes;
        }

        /// <summary>
        /// Whether this envelope's absolute expiry has already passed as
        /// of <paramref name="now"/> — <c>false</c> when there is no
        /// absolute expiry at all. Callers renewing a sliding window
        /// (<see cref="NanocachedDistributedCache.GetAsync"/>/<see cref="NanocachedDistributedCache.RefreshAsync"/>)
        /// must check this <em>before</em> calling <see cref="WireTtlSeconds"/>
        /// (issue #233): that method's own floor exists for a sub-second-but-still-future
        /// remainder, not for "already past", and would otherwise turn an
        /// expired entry into a fresh 1-second TTL instead of the miss it
        /// should be.
        /// </summary>
        internal bool IsPastAbsoluteExpiry(DateTimeOffset now) =>
            AbsoluteUnixSeconds > 0 && DateTimeOffset.FromUnixTimeSeconds(AbsoluteUnixSeconds) <= now;

        /// <summary>
        /// The wire TTL (whole seconds) for this envelope's next write:
        /// the sliding window when only that is set, the remaining time to
        /// the absolute expiry when only that is set, or the smaller of
        /// the two when both are — matching a real sliding+absolute cache
        /// entry, which expires at whichever limit is hit first. 0 (no
        /// TTL — nanocached's "lives until evicted/removed") when neither
        /// is set. Callers with a sliding window must rule out
        /// <see cref="IsPastAbsoluteExpiry"/> first — see its own doc
        /// comment.
        ///
        /// <para>Issue #418: when this envelope still carries the precise
        /// (not-yet-floored) absolute instant — i.e. this call is the
        /// immediate write right after <see cref="FromOptions"/>, not a
        /// later renewal reconstructed via <see cref="Parse"/> — the
        /// remaining duration is ceiled once from that precise instant
        /// rather than from <see cref="AbsoluteUnixSeconds"/>' whole-second
        /// floor, so a request like "5.9s from now" yields a 6-second wire
        /// TTL, not 5.</para>
        /// </summary>
        internal long WireTtlSeconds(DateTimeOffset now)
        {
            long? ttl = SlidingSeconds > 0 ? SlidingSeconds : null;
            if (_preciseAbsoluteExpiry is { } precise)
            {
                long remaining = CeilSeconds(precise - now);
                ttl = ttl is { } sliding ? Math.Min(sliding, remaining) : remaining;
            }
            else if (AbsoluteUnixSeconds > 0)
            {
                long remaining = CeilSeconds(DateTimeOffset.FromUnixTimeSeconds(AbsoluteUnixSeconds) - now);
                ttl = ttl is { } sliding ? Math.Min(sliding, remaining) : remaining;
            }
            return ttl ?? 0;
        }

        /// <summary>Whole seconds, rounded up — the wire's TTL unit
        /// (shared spec: positive sub-second values round UP to 1 second,
        /// never down to "eternal"). Doubles as the floor for an
        /// already-past-expiry remainder computed by
        /// <see cref="WireTtlSeconds"/> under clock skew: 1 second, never
        /// 0 (which would mean "no expiry" on the wire) and never
        /// negative.</summary>
        private static long CeilSeconds(TimeSpan span)
        {
            long whole = (long)Math.Ceiling(span.TotalSeconds);
            return whole < 1 ? 1 : whole;
        }
    }
}
