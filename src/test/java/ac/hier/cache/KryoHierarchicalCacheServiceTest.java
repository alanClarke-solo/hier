// src/test/java/ac/hier/cache/KryoHierarchicalCacheServiceTest.java
package ac.hier.cache;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.redisson.api.RedissonClient;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;

class KryoHierarchicalCacheServiceTest {
    
    private RedissonClient redissonClient;
    private KryoHierarchicalCacheService cacheService;

    @BeforeEach
    void setUp() {
        redissonClient = RedissonClientFactory.createDefault();
        cacheService = new KryoHierarchicalCacheService(redissonClient, "test_kryo_cache", 300);
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
    void testStringKeyBasedCaching() {
        String testKey = "test-key-123";
        String testValue = "test-data";
        List<SearchParameter> params = Arrays.asList(
            new SearchParameter("region", "US", 0)
        );

        // Cache with String key
        String returnedKey = cacheService.put(testKey, params, testValue);
        assertEquals(testKey, returnedKey);

        // Retrieve by String key
        Optional<String> result = cacheService.getByKey(testKey, String.class);
        assertTrue(result.isPresent());
        assertEquals(testValue, result.get());
    }

    @Test
    void testKeyGeneration() {
        String testValue = "test-data";
        List<SearchParameter> params = Arrays.asList(
            new SearchParameter("region", "US", 0)
        );

        // Cache without key (should generate one)
        String generatedKey = cacheService.put(null, params, testValue);
        assertNotNull(generatedKey);
        assertTrue(generatedKey.startsWith("key_"));

        // Retrieve by generated key
        Optional<String> result = cacheService.getByKey(generatedKey, String.class);
        assertTrue(result.isPresent());
        assertEquals(testValue, result.get());
    }

    @Test
    void testHierarchicalRetrieval() {
        String testKey = "hierarchical-test-key";
        String testValue = "hierarchical-test-data";
        List<SearchParameter> fullParams = Arrays.asList(
            new SearchParameter("region", "US", 0),
            new SearchParameter("category", "electronics", 1),
            new SearchParameter("brand", "apple", 2)
        );

        cacheService.put(testKey, fullParams, testValue);

        // Test partial parameter retrieval
        List<SearchParameter> partialParams = Arrays.asList(
            new SearchParameter("region", "US", 0),
            new SearchParameter("category", "electronics", 1)
        );

        Optional<String> result = cacheService.get(partialParams, String.class);
        assertTrue(result.isPresent());
        assertEquals(testValue, result.get());
    }

    @Test
    void testGetOrComputeWithStringKey() {
        String testKey = "compute-test-key";
        List<SearchParameter> params = Arrays.asList(
            new SearchParameter("region", "EU", 0)
        );

        String computedValue = "computed-data";
        boolean[] supplierCalled = {false};

        // First call should compute and cache
        String result1 = cacheService.getOrCompute(testKey, params, String.class, () -> {
            supplierCalled[0] = true;
            return computedValue;
        });

        assertTrue(supplierCalled[0]);
        assertEquals(computedValue, result1);

        // Second call should hit cache by String key
        supplierCalled[0] = false;
        String result2 = cacheService.getOrCompute(testKey, params, String.class, () -> {
            supplierCalled[0] = true;
            return "should-not-be-called";
        });

        assertFalse(supplierCalled[0]);
        assertEquals(computedValue, result2);
    }

    @Test
    void testDataDeduplication() {
        String sameValue = "same-data-kryo";
        String key1 = "key1";
        String key2 = "key2";
        
        List<SearchParameter> params1 = Arrays.asList(
            new SearchParameter("region", "US", 0)
        );
        
        List<SearchParameter> params2 = Arrays.asList(
            new SearchParameter("region", "US", 0),
            new SearchParameter("category", "electronics", 1)
        );

        // Cache same value with different keys and parameters
        cacheService.put(key1, params1, sameValue);
        cacheService.put(key2, params2, sameValue);

        // Both should retrieve successfully
        assertTrue(cacheService.getByKey(key1, String.class).isPresent());
        assertTrue(cacheService.getByKey(key2, String.class).isPresent());

        // Check statistics for deduplication
        var stats = cacheService.getStats();
        assertTrue(stats.getTotalReferences() >= 2, "Should have at least 2 references");
        assertTrue(stats.getCompressionRatio() >= 1.0, "Should show compression ratio >= 1.0");
    }

    @Test
    void testInvalidationByStringKey() {
        String testKey = "invalidation-test-key";
        String testValue = "test-data";
        List<SearchParameter> params = Arrays.asList(
            new SearchParameter("region", "US", 0)
        );

        cacheService.put(testKey, params, testValue);
        assertTrue(cacheService.getByKey(testKey, String.class).isPresent());

        // Invalidate by String key
        cacheService.invalidateByKey(testKey);
        assertFalse(cacheService.getByKey(testKey, String.class).isPresent());
    }

    @Test
    void testNullHandling() {
        String testKey = "null-test-key";
        List<SearchParameter> params = Arrays.asList(
            new SearchParameter("region", "US", 0)
        );

        // Should handle null value gracefully
        String result = cacheService.put(testKey, params, null);
        assertEquals(testKey, result);

        // Should return empty optional for null key
        assertFalse(cacheService.getByKey(null, String.class).isPresent());
    }
}
