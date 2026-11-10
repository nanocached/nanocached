package org.nanocached.jcache;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.management.ManagementFactory;
import java.net.URI;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import javax.cache.Cache;
import javax.cache.CacheManager;
import javax.cache.configuration.MutableConfiguration;
import javax.cache.management.CacheStatisticsMXBean;
import javax.cache.processor.EntryProcessor;
import javax.cache.spi.CachingProvider;
import javax.management.JMX;
import javax.management.MBeanServer;
import javax.management.ObjectName;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * The statistics each {@link Cache} operation records, as an application
 * reads them: through the {@link CacheStatisticsMXBean} the cache registers
 * with the platform MBean server. The expected counts are the ones JSR-107
 * 1.1.1's "Statistics Effects of Cache Operations" table lists, with the
 * ambiguous cells (an operation that is both a hit and a miss "depending
 * on the mapping") resolved the way the reference implementation
 * ({@code RICache}) and the TCK's {@code CacheMBStatisticsBeanTest} do.
 */
class NanocachedCacheStatisticsTest {

    private MockNode node;
    private CachingProvider provider;
    private CacheManager manager;
    private Cache<String, String> cache;
    private CacheStatisticsMXBean statistics;

    @BeforeEach
    void start() throws Exception {
        node = new MockNode();
        provider = new NanocachedCachingProvider();
        Properties properties = new Properties();
        properties.setProperty("nanocached.addresses", node.address());
        manager = provider.getCacheManager(URI.create("test:statistics"), null, properties);
        MutableConfiguration<String, String> config = new MutableConfiguration<>();
        config.setTypes(String.class, String.class);
        config.setStatisticsEnabled(true);
        cache = manager.createCache("stats", config);
        MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
        ObjectName objectName = new ObjectName("javax.cache:type=CacheStatistics,CacheManager="
                + ObjectName.quote(manager.getURI().toString()) + ",Cache=" + ObjectName.quote("stats"));
        statistics = JMX.newMXBeanProxy(mbs, objectName, CacheStatisticsMXBean.class);
    }

    @AfterEach
    void stop() throws Exception {
        provider.close();
        node.close();
    }

    private void assertCounts(long hits, long misses, long puts, long removals) {
        assertEquals(
                java.util.List.of(hits, misses, puts, removals),
                java.util.List.of(
                        statistics.getCacheHits(),
                        statistics.getCacheMisses(),
                        statistics.getCachePuts(),
                        statistics.getCacheRemovals()),
                "[hits, misses, puts, removals]");
    }

    // ── get / getAll / containsKey / put / putAll ───────────────────

    @Test
    void getCountsAHitOrAMiss() {
        cache.get("a");
        assertCounts(0, 1, 0, 0);
        cache.put("a", "1");
        cache.get("a");
        assertCounts(1, 1, 1, 0);
    }

    @Test
    void getAllCountsOneHitOrMissPerKey() {
        cache.putAll(Map.of("a", "1", "b", "2"));
        assertCounts(0, 0, 2, 0);
        cache.getAll(Set.of("a", "b", "c"));
        assertCounts(2, 1, 2, 0);
    }

    @Test
    void containsKeyCountsNothing() {
        cache.put("a", "1");
        assertTrue(cache.containsKey("a"));
        assertFalse(cache.containsKey("b"));
        assertCounts(0, 0, 1, 0);
    }

    @Test
    void putCountsAPutWhetherItCreatesOrUpdatesAndNothingElse() {
        cache.put("a", "1");
        cache.put("a", "2");
        assertCounts(0, 0, 2, 0);
    }

    // ── getAndPut / getAndReplace / getAndRemove ────────────────────

    @Test
    void getAndPutCountsAMissAndAPutForAnAbsentKey() {
        assertNull(cache.getAndPut("a", "1"));
        assertCounts(0, 1, 1, 0);
    }

    @Test
    void getAndPutCountsAHitAndAPutForAPresentKey() {
        cache.put("a", "1");
        assertEquals("1", cache.getAndPut("a", "2"));
        assertCounts(1, 0, 2, 0);
    }

    @Test
    void getAndReplaceCountsAMissAndNoPutForAnAbsentKey() {
        assertNull(cache.getAndReplace("a", "1"));
        assertCounts(0, 1, 0, 0);
    }

    @Test
    void getAndReplaceCountsAHitAndAPutForAPresentKey() {
        cache.put("a", "1");
        assertEquals("1", cache.getAndReplace("a", "2"));
        assertCounts(1, 0, 2, 0);
    }

    @Test
    void getAndRemoveCountsAMissAndNoRemovalForAnAbsentKey() {
        assertNull(cache.getAndRemove("a"));
        assertCounts(0, 1, 0, 0);
    }

    @Test
    void getAndRemoveCountsAHitAndARemovalForAPresentKey() {
        cache.put("a", "1");
        assertEquals("1", cache.getAndRemove("a"));
        assertCounts(1, 0, 1, 1);
    }

    // ── putIfAbsent / replace / remove ──────────────────────────────

    @Test
    void putIfAbsentCountsAMissAndAPutWhenItStores() {
        assertTrue(cache.putIfAbsent("a", "1"));
        assertCounts(0, 1, 1, 0);
    }

    @Test
    void putIfAbsentCountsAHitAndNoPutWhenTheKeyIsPresent() {
        cache.put("a", "1");
        assertFalse(cache.putIfAbsent("a", "2"));
        assertCounts(1, 0, 1, 0);
    }

    @Test
    void twoArgReplaceCountsAMissAndNoPutForAnAbsentKey() {
        assertFalse(cache.replace("a", "1"));
        assertCounts(0, 1, 0, 0);
    }

    @Test
    void twoArgReplaceCountsAHitAndAPutForAPresentKey() {
        cache.put("a", "1");
        assertTrue(cache.replace("a", "2"));
        assertCounts(1, 0, 2, 0);
    }

    @Test
    void threeArgReplaceCountsAMissForAnAbsentKey() {
        assertFalse(cache.replace("a", "1", "2"));
        assertCounts(0, 1, 0, 0);
    }

    @Test
    void threeArgReplaceCountsAHitAndAPutWhenTheOldValueMatches() {
        cache.put("a", "1");
        assertTrue(cache.replace("a", "1", "2"));
        assertCounts(1, 0, 2, 0);
    }

    @Test
    void threeArgReplaceCountsAHitAndNoPutWhenTheKeyIsPresentWithAnotherValue() {
        cache.put("a", "1");
        assertFalse(cache.replace("a", "other", "2"));
        assertCounts(1, 0, 1, 0);
    }

    @Test
    void removeCountsARemovalOnlyWhenItRemoves() {
        cache.put("a", "1");
        assertTrue(cache.remove("a"));
        assertFalse(cache.remove("a"));
        assertCounts(0, 0, 1, 1);
    }

    @Test
    void twoArgRemoveCountsAMissForAnAbsentKey() {
        assertFalse(cache.remove("a", "1"));
        assertCounts(0, 1, 0, 0);
    }

    @Test
    void twoArgRemoveCountsAHitAndNoRemovalWhenTheKeyIsPresentWithAnotherValue() {
        cache.put("a", "1");
        assertFalse(cache.remove("a", "other"));
        assertCounts(1, 0, 1, 0);
    }

    @Test
    void twoArgRemoveCountsAHitAndARemovalWhenTheValueMatches() {
        cache.put("a", "1");
        assertTrue(cache.remove("a", "1"));
        assertCounts(1, 0, 1, 1);
    }

    // ── removeAll / clear / invoke ──────────────────────────────────

    @Test
    void removeAllOfKeysCountsOneRemovalPerEntryRemoved() {
        cache.putAll(Map.of("a", "1", "b", "2"));
        cache.removeAll(Set.of("a", "b", "absent"));
        assertCounts(0, 0, 2, 2);
    }

    @Test
    void removeAllAndClearCountNothing() {
        // The wire protocol clears a namespace in one frame without
        // reporting how many entries it held, so the "one removal per
        // entry" the specification asks of removeAll() cannot be counted;
        // the README says so.
        cache.putAll(Map.of("a", "1", "b", "2"));
        cache.removeAll();
        cache.put("c", "3");
        cache.clear();
        assertCounts(0, 0, 3, 0);
    }

    @Test
    void invokeIsNotSupportedSoItCountsNothing() {
        cache.put("a", "1");
        EntryProcessor<String, String, String> read = (entry, arguments) -> entry.getValue();
        assertThrows(UnsupportedOperationException.class, () -> cache.invoke("a", read));
        assertCounts(0, 0, 1, 0);
    }

    @Test
    void theStatisticsMBeanClearOperationResetsTheCounters() {
        cache.put("a", "1");
        cache.getAndPut("a", "2");
        cache.getAndRemove("a");
        statistics.clear();
        assertCounts(0, 0, 0, 0);
    }
}
