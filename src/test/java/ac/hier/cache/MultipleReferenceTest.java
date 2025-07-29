
package ac.hier.cache;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.redisson.api.RedissonClient;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;

class MultipleReferenceTest {
    
    private RedissonClient redissonClient;
    private KryoHierarchicalCacheService cacheService;

    @BeforeEach
    void setUp() {
        redissonClient = RedissonClientFactory.createDefault();
        cacheService = new KryoHierarchicalCacheService(redissonClient, "test_multi_ref_cache", 300);
        cacheService.clearAll();
    }

    @AfterEach
    void tearDown() {
        if (redissonClient != null) {
            cacheService.clearAll();
            redissonClient.shutdown();
        }
    }

    @Test
    void testMultipleSearchPatterns() {
        String testKey = "multi-pattern-key";
        String testValue = "multi-pattern-data";
        
        List<List<SearchParameter>> multiplePatterns = Arrays.asList(
            Arrays.asList(
                new SearchParameter("region", "US", 0),
                new SearchParameter("category", "electronics", 1)
            ),
            Arrays.asList(
                new SearchParameter("brand", "apple", 0),
                new SearchParameter("product", "iphone", 1)
            )
        );

        // Cache with multiple patterns
        String returnedKey = cacheService.put(testKey, multiplePatterns, testValue);
        assertEquals(testKey, returnedKey);

        // Should be retrievable via both patterns
        Optional<String> result1 = cacheService.get(Arrays.asList(
            new SearchParameter("region", "US", 0)
        ), String.class);
        assertTrue(result1.isPresent());
        assertEquals(testValue, result1.get());

        Optional<String> result2 = cacheService.get(Arrays.asList(
            new SearchParameter("brand", "apple", 0)
        ), String.class);
        assertTrue(result2.isPresent());
        assertEquals(testValue, result2.get());
    }

    @Test
    void testGetAllResults() {
        String value1 = "data1";
        String value2 = "data2";
        
        // Cache two items that both match a search pattern
        cacheService.put("key1", Arrays.asList(
            Arrays.asList(new SearchParameter("category", "electronics", 0))
        ), value1);
        
        cacheService.put("key2", Arrays.asList(
            Arrays.asList(new SearchParameter("category", "electronics", 0))
        ), value2);

        // getAll should return both items
        List<String> allResults = cacheService.getAll(Arrays.asList(
            new SearchParameter("category", "electronics", 0)
        ), String.class);
        
        assertEquals(2, allResults.size());
        assertTrue(allResults.contains(value1));
        assertTrue(allResults.contains(value2));
    }

    @Test
    void testFindByPattern() {
        String testKey = "pattern-test-key";
        String testValue = "pattern-test-data";
        
        cacheService.put(testKey, Arrays.asList(
            Arrays.asList(
                new SearchParameter("type", "premium", 0),
                new SearchParameter("brand", "apple", 1)
            )
        ), testValue);

        Map<String, String> results = cacheService.findByPattern(Arrays.asList(
            new SearchParameter("type", "premium", 0)
        ), String.class);
        
        assertEquals(1, results.size());
        assertTrue(results.containsKey(testKey));
        assertEquals(testValue, results.get(testKey));
    }

    @Test
    void testSelectiveInvalidation() {
        String testKey = "selective-test-key";
        String testValue = "selective-test-data";
        
        List<SearchParameter> pattern1 = Arrays.asList(
            new SearchParameter("region", "US", 0),
            new SearchParameter("category", "electronics", 1)
        );
        
        List<SearchParameter> pattern2 = Arrays.asList(
            new SearchParameter("brand", "apple", 0),
            new SearchParameter("product", "iphone", 1)
        );
        
        List<List<SearchParameter>> patterns = Arrays.asList(pattern1, pattern2);
        
        cacheService.put(testKey, patterns, testValue);
        
        // Both patterns should work initially
        assertTrue(cacheService.get(pattern1, String.class).isPresent());
        assertTrue(cacheService.get(pattern2, String.class).isPresent());
        
        // Invalidate only pattern1
        cacheService.invalidate(pattern1);
        
        // Pattern1 should miss, but pattern2 should still work
        assertFalse(cacheService.get(pattern1, String.class).isPresent());
        assertTrue(cacheService.get(pattern2, String.class).isPresent());
        
        // Direct key access should still work
        assertTrue(cacheService.getByKey(testKey, String.class).isPresent());
    }

    @Test
    void testGetOrComputeWithMultiplePatterns() {
        String testKey = "compute-multi-key";
        List<List<SearchParameter>> patterns = Arrays.asList(
            Arrays.asList(new SearchParameter("region", "US", 0)),
            Arrays.asList(new SearchParameter("brand", "test", 0))
        );

        String computedValue = "computed-multi-data";
        boolean[] supplierCalled = {false};

        // First call should compute and cache
        String result1 = cacheService.getOrCompute(testKey, patterns, String.class, () -> {
            supplierCalled[0] = true;
            return computedValue;
        });

        assertTrue(supplierCalled[0]);
        assertEquals(computedValue, result1);

        // Second call should hit cache via any of the patterns
        supplierCalled[0] = false;
        String result2 = cacheService.getOrCompute(testKey, patterns, String.class, () -> {
            supplierCalled[0] = true;
            return "should-not-be-called";
        });

        assertFalse(supplierCalled[0]);
        assertEquals(computedValue, result2);
    }

    @Test
    void testEnhancedStatistics() {
        String value = "stats-test-data";
        
        // Cache same value with multiple patterns to create multiple references
        List<List<SearchParameter>> patterns = Arrays.asList(
            Arrays.asList(new SearchParameter("category", "electronics", 0)),
            Arrays.asList(new SearchParameter("brand", "apple", 0)),
            Arrays.asList(new SearchParameter("region", "US", 0))
        );
        
        cacheService.put("stats-key", patterns, value);
        
        var stats = cacheService.getStats();
        
        // Should have 1 key reference, multiple hierarchical references, 1 data item, 1 ref set
        assertEquals(1, stats.getKeyCount());
        assertTrue(stats.getReferenceCount() >= 3, "Should have at least 3 hierarchical references");
        assertEquals(1, stats.getDataCount());
        assertEquals(1, stats.getRefSetCount());
        assertTrue(stats.getAverageReferencesPerData() >= 3.0, "Should have multiple references per data");
    }

    @Test
    void testInvalidateByKeyWithMultipleReferences() {
        String testKey = "invalidate-key-test";
        String testValue = "invalidate-key-data";
        
        List<List<SearchParameter>> patterns = Arrays.asList(
            Arrays.asList(new SearchParameter("category", "electronics", 0)),
            Arrays.asList(new SearchParameter("brand", "apple", 0))
        );
        
        cacheService.put(testKey, patterns, testValue);
        
        // Verify all access methods work
        assertTrue(cacheService.getByKey(testKey, String.class).isPresent());
        assertTrue(cacheService.get(patterns.get(0), String.class).isPresent());
        assertTrue(cacheService.get(patterns.get(1), String.class).isPresent());
        
        // Invalidate by key should remove all references
        cacheService.invalidateByKey(testKey);
        
        // All access methods should miss
        assertFalse(cacheService.getByKey(testKey, String.class).isPresent());
        assertFalse(cacheService.get(patterns.get(0), String.class).isPresent());
        assertFalse(cacheService.get(patterns.get(1), String.class).isPresent());
    }

    @Test
    void testDataDeduplicationWithMultiplePatterns() {
        String sameValue = "dedup-test-data";
        
        // Cache same value with different keys but overlapping patterns
        List<List<SearchParameter>> patterns1 = Arrays.asList(
            Arrays.asList(new SearchParameter("category", "electronics", 0))
        );
        
        List<List<SearchParameter>> patterns2 = Arrays.asList(
            Arrays.asList(new SearchParameter("category", "electronics", 0)),
            Arrays.asList(new SearchParameter("brand", "apple", 0))
        );
        
        cacheService.put("key1", patterns1, sameValue);
        cacheService.put("key2", patterns2, sameValue);
        
        var stats = cacheService.getStats();
        
        // Should have 2 keys, multiple references, but only 1 actual data item
        assertEquals(2, stats.getKeyCount());
        assertEquals(1, stats.getDataCount());
        assertTrue(stats.getReferenceCount() > 0);
        assertTrue(stats.getCompressionRatio() > 1.0, "Should show compression due to deduplication");
    }
}
