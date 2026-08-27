package org.nanocached.jcache;

import java.util.concurrent.atomic.AtomicLong;
import javax.cache.management.CacheStatisticsMXBean;

/**
 * A bare-bones {@link CacheStatisticsMXBean} (issue #118): hit/miss/put/
 * removal counts only. Timings ({@code getAverageGetTime} and friends)
 * and evictions aren't tracked — the wire protocol doesn't report
 * server-side LRU evictions to the client, and per-call timing isn't
 * otherwise part of this adapter's scope.
 */
final class NanocachedCacheStatistics implements CacheStatisticsMXBean {

    private final AtomicLong hits = new AtomicLong();
    private final AtomicLong misses = new AtomicLong();
    private final AtomicLong puts = new AtomicLong();
    private final AtomicLong removals = new AtomicLong();

    void recordHit() {
        hits.incrementAndGet();
    }

    void recordMiss() {
        misses.incrementAndGet();
    }

    void recordPut() {
        puts.incrementAndGet();
    }

    void recordRemove() {
        removals.incrementAndGet();
    }

    @Override
    public void clear() {
        hits.set(0);
        misses.set(0);
        puts.set(0);
        removals.set(0);
    }

    @Override
    public long getCacheHits() {
        return hits.get();
    }

    @Override
    public float getCacheHitPercentage() {
        long hitCount = hits.get();
        long total = hitCount + misses.get();
        return total == 0 ? 0f : (100f * hitCount / total);
    }

    @Override
    public long getCacheMisses() {
        return misses.get();
    }

    @Override
    public float getCacheMissPercentage() {
        long missCount = misses.get();
        long total = hits.get() + missCount;
        return total == 0 ? 0f : (100f * missCount / total);
    }

    @Override
    public long getCacheGets() {
        return hits.get() + misses.get();
    }

    @Override
    public long getCachePuts() {
        return puts.get();
    }

    @Override
    public long getCacheRemovals() {
        return removals.get();
    }

    @Override
    public long getCacheEvictions() {
        return 0;
    }

    @Override
    public float getAverageGetTime() {
        return 0f;
    }

    @Override
    public float getAveragePutTime() {
        return 0f;
    }

    @Override
    public float getAverageRemoveTime() {
        return 0f;
    }
}
