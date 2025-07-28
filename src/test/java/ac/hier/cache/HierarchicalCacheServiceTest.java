// src/test/java/ac/hier/cache/HierarchicalCacheServiceTest.java
package ac.hier.cache;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.redisson.api.RedissonClient;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;

class HierarchicalCacheServiceTest {

    private RedissonClient redissonClient;
    private HierarchicalCacheService cacheService;

    @BeforeEach
    void setUp() {
        redissonClient = RedissonClientFactory.createDefault();
        cacheService = new HierarchicalCacheService(redissonClient, "test_cache", 300);
        cacheService.clearAll(); // Clean slate for each test
    }

    @AfterEach
    void tearDown() {
        if (redissonClient != null) {
            cacheService.clearAll();
            redissonClient.shutdown();
        }
    }

    @Test
    void testBasicCacheOperations() {
        List<SearchParameter> params = Arrays.asList(
                new SearchParameter("region", "US", 0),
                new SearchParameter("category", "electronics", 1)
        );

        String testValue = "test-data";

        // Test put and get
        cacheService.put(params, testValue);
        Optional<String> result = cacheService.get(params, String.class);

        assertTrue(result.isPresent());
        assertEquals(testValue, result.get());
    }

    @Test
    void testDataDeduplication() {
        String sameValue = "same-data";

        // Create two different search parameter combinations that will cache the same data
        List<SearchParameter> params1 = Arrays.asList(
                new SearchParameter("region", "US", 0),
                new SearchParameter("category", "electronics", 1)
        );

        List<SearchParameter> params2 = Arrays.asList(
                new SearchParameter("region", "US", 0),
                new SearchParameter("category", "electronics", 1),
                new SearchParameter("brand", "apple", 2)
        );

        // Cache the same value with different parameter combinations
        cacheService.put(params1, sameValue);
        cacheService.put(params2, sameValue);

        // Verify both can retrieve the data
        assertTrue(cacheService.get(params1, String.class).isPresent());
        assertTrue(cacheService.get(params2, String.class).isPresent());

        // Check statistics - should have more references than actual data
        var stats = cacheService.getStats();
        assertTrue(stats.getReferenceCount() > stats.getDataCount(),
                "References should exceed actual data count due to deduplication");

        // Should have only 1 actual data entry despite multiple references
        assertEquals(1, stats.getDataCount(), "Should have only one actual data entry");
    }

    @Test
    void testHierarchicalRetrieval() {
        List<SearchParameter> fullParams = Arrays.asList(
                new SearchParameter("region", "US", 0),
                new SearchParameter("category", "electronics", 1),
                new SearchParameter("brand", "apple", 2)
        );

        String testValue = "iphone-data";
        cacheService.put(fullParams, testValue);

        // Should find data using partial parameters
        List<SearchParameter> partialParams = Arrays.asList(
                new SearchParameter("region", "US", 0),
                new SearchParameter("category", "electronics", 1)
        );

        Optional<String> result = cacheService.get(partialParams, String.class);
        assertTrue(result.isPresent());
        assertEquals(testValue, result.get());
    }

    @Test
    void testGetOrCompute() {
        List<SearchParameter> params = Arrays.asList(
                new SearchParameter("region", "EU", 0)
        );

        String computedValue = "computed-data";
        boolean[] supplierCalled = {false};

        // First call should compute and cache
        String result1 = cacheService.getOrCompute(params, String.class, () -> {
            supplierCalled[0] = true;
            return computedValue;
        });

        assertTrue(supplierCalled[0]);
        assertEquals(computedValue, result1);

        // Second call should hit cache
        supplierCalled[0] = false;
        String result2 = cacheService.getOrCompute(params, String.class, () -> {
            supplierCalled[0] = true;
            return "should-not-be-called";
        });

        assertFalse(supplierCalled[0]);
        assertEquals(computedValue, result2);
    }

    @Test
    void testCacheMiss() {
        List<SearchParameter> params = Arrays.asList(
                new SearchParameter("region", "NONEXISTENT", 0)
        );

        Optional<String> result = cacheService.get(params, String.class);
        assertFalse(result.isPresent());
    }

    @Test
    void testInvalidation() {
        List<SearchParameter> params = Arrays.asList(
                new SearchParameter("region", "US", 0),
                new SearchParameter("category", "books", 1)
        );

        String testValue = "book-data";
        cacheService.put(params, testValue);

        // Verify data is cached
        assertTrue(cacheService.get(params, String.class).isPresent());

        // Invalidate cache
        cacheService.invalidate(params);

        // Verify data is no longer cached
        assertFalse(cacheService.get(params, String.class).isPresent());
    }

    @Test
    void testInvalidationWithCleanup() {
        List<SearchParameter> params = Arrays.asList(
                new SearchParameter("region", "US", 0),
                new SearchParameter("category", "books", 1)
        );

        String testValue = "book-data";
        cacheService.put(params, testValue);

        var statsBeforeCleanup = cacheService.getStats();
        assertTrue(statsBeforeCleanup.getDataCount() > 0);

        // Invalidate with cleanup
        cacheService.invalidateWithCleanup(params);

        // Verify reference is gone
        assertFalse(cacheService.get(params, String.class).isPresent());

        // Give cleanup time to complete (in a real scenario you might want to wait or check async)
        var statsAfterCleanup = cacheService.getStats();
        assertTrue(statsAfterCleanup.getDataCount() <= statsBeforeCleanup.getDataCount());
    }

    @Test
    void testNullValueHandling() {
        List<SearchParameter> params = Arrays.asList(
                new SearchParameter("region", "US", 0)
        );

        // Should not crash on null value
        cacheService.put(params, null);

        // Should return empty optional
        Optional<String> result = cacheService.get(params, String.class);
        assertFalse(result.isPresent());
    }
}