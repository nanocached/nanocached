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
/// and re-set the entry with it. An entry with no sliding window is never
/// rewritten on <see cref="Get"/> — its TTL (if any) is fixed regardless of
/// access, so the extra round trip would buy nothing.</para>
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
        byte[]? raw = await _namespace.GetBytesAsync(keyBytes).ConfigureAwait(false);
        if (raw is null) return null;

        Envelope envelope = Envelope.Parse(raw);
        if (envelope.SlidingSeconds == 0) return envelope.Payload; // fixed TTL (or none) — nothing to renew

        // Sliding expiration: re-set with the recomputed TTL before
        // returning — awaited, never fire-and-forget (shared spec), so a
        // caller that awaits this call can rely on the renewal having
        // actually reached the wire.
        long wireTtl = envelope.WireTtlSeconds(DateTimeOffset.UtcNow);
        await _namespace.SetAsync(keyBytes, envelope.ToBytes(), wireTtl).ConfigureAwait(false);
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
        byte[]? raw = await _namespace.GetBytesAsync(keyBytes).ConfigureAwait(false);
        // A missing key is a no-op, per IDistributedCache's contract —
        // not a miss error.
        if (raw is null) return;

        Envelope envelope = Envelope.Parse(raw);
        // No sliding window: nothing a refresh would change (an absolute
        // expiry doesn't move; no expiry needs no TTL rewrite either) — so
        // skip the round trip rather than re-set identical content.
        if (envelope.SlidingSeconds == 0) return;

        long wireTtl = envelope.WireTtlSeconds(DateTimeOffset.UtcNow);
        await _namespace.SetAsync(keyBytes, envelope.ToBytes(), wireTtl).ConfigureAwait(false);
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

        private Envelope(long slidingSeconds, long absoluteUnixSeconds, byte[] payload)
        {
            SlidingSeconds = slidingSeconds;
            AbsoluteUnixSeconds = absoluteUnixSeconds;
            Payload = payload;
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

            long absoluteUnixSeconds = absolute is { } a ? a.ToUnixTimeSeconds() : 0;
            return new Envelope(slidingSeconds, absoluteUnixSeconds, payload);
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
            BinaryPrimitives.WriteUInt32BigEndian(bytes.AsSpan(1, 4), (uint)SlidingSeconds);
            BinaryPrimitives.WriteInt64BigEndian(bytes.AsSpan(5, 8), AbsoluteUnixSeconds);
            Payload.CopyTo(bytes.AsSpan(EnvelopeHeaderLength));
            return bytes;
        }

        /// <summary>
        /// The wire TTL (whole seconds) for this envelope's next write:
        /// the sliding window when only that is set, the remaining time to
        /// the absolute expiry when only that is set, or the smaller of
        /// the two when both are — matching a real sliding+absolute cache
        /// entry, which expires at whichever limit is hit first. 0 (no
        /// TTL — nanocached's "lives until evicted/removed") when neither
        /// is set.
        /// </summary>
        internal long WireTtlSeconds(DateTimeOffset now)
        {
            long? ttl = SlidingSeconds > 0 ? SlidingSeconds : null;
            if (AbsoluteUnixSeconds > 0)
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
