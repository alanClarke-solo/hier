package ac.hier.cache;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.redisson.api.RedissonClient;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;

class VariableParameterCountTest {
    
    private RedissonClient redissonClient;
    private KryoHierarchicalCacheService cacheService;

    @BeforeEach
    void setUp() {
        redissonClient = RedissonClientFactory.createDefault();
        cacheService = new KryoHierarchicalCacheService(redissonClient, "test_variable_params", 300);
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
    void testSingleParameterCacheAndRetrieval() {
        String value = "single-param-data";
        
        // Cache with single parameter
        cacheService.put("single-key", Arrays.asList(
            Arrays.asList(new SearchParameter("region", "US", 0))
        ), value);

        // Should find with exact match
        Optional<String> result = cacheService.get(Arrays.asList(
            new SearchParameter("region", "US", 0)
        ), String.class);
        
        assertTrue(result.isPresent());
        assertEquals(value, result.get());
    }

    @Test
    void testMultiParameterCacheWithVariableLengthRetrieval() {
        String value = "multi-param-data";
        
        // Cache with 4 parameters
        List<SearchParameter> fullParams = Arrays.asList(
            new SearchParameter("region", "US", 0),
            new SearchParameter("category", "electronics", 1),
            new SearchParameter("brand", "apple", 2),
            new SearchParameter("model", "iphone", 3)
        );
        
        cacheService.put("multi-key", Arrays.asList(fullParams), value);

        // Should find with 1 parameter
        Optional<String> result1 = cacheService.get(Arrays.asList(
            new SearchParameter("region", "US", 0)
        ), String.class);
        assertTrue(result1.isPresent());
        assertEquals(value, result1.get());

        // Should find with 2 parameters
        Optional<String> result2 = cacheService.get(Arrays.asList(
            new SearchParameter("region", "US", 0),
            new SearchParameter("category", "electronics", 1)
        ), String.class);
        assertTrue(result2.isPresent());
        assertEquals(value, result2.get());

        // Should find with 3 parameters
        Optional<String> result3 = cacheService.get(Arrays.asList(
            new SearchParameter("region", "US", 0),
            new SearchParameter("category", "electronics", 1),
            new SearchParameter("brand", "apple", 2)
        ), String.class);
        assertTrue(result3.isPresent());
        assertEquals(value, result3.get());

        // Should find with all 4 parameters
        Optional<String> result4 = cacheService.get(fullParams, String.class);
        assertTrue(result4.isPresent());
        assertEquals(value, result4.get());
    }

    @Test
    void testDifferentParameterCountsForSameData() {
        String sameValue = "shared-data";
        
        // Cache same data with different parameter structures
        cacheService.put("key1", Arrays.asList(
            Arrays.asList(new SearchParameter("region", "US", 0))  // 1 parameter
        ), sameValue);
        
        cacheService.put("key2", Arrays.asList(
            Arrays.asList(
                new SearchParameter("region", "US", 0),           // 2 parameters
                new SearchParameter("category", "electronics", 1)
            )
        ), sameValue);
        
        cacheService.put("key3", Arrays.asList(
            Arrays.asList(
                new SearchParameter("region", "US", 0),           // 5 parameters
                new SearchParameter("category", "electronics", 1),
                new SearchParameter("brand", "apple", 2),
                new SearchParameter("type", "phone", 3),
                new SearchParameter("model", "15", 4)
            )
        ), sameValue);

        // All should be retrievable with basic region search
        List<String> allResults = cacheService.getAll(Arrays.asList(
            new SearchParameter("region", "US", 0)
        ), String.class);
        
        assertEquals(3, allResults.size()); // Should find all 3 despite different parameter counts
        assertTrue(allResults.stream().allMatch(r -> r.equals(sameValue)));
    }

    @Test
    void testGappedLevels() {
        String value = "gapped-level-data";
        
        // Cache with gaps in levels (0, 2, 4 - skipping 1 and 3)
        List<SearchParameter> gappedParams = Arrays.asList(
            new SearchParameter("region", "US", 0),
            new SearchParameter("brand", "apple", 2),      // Skip level 1
            new SearchParameter("model", "pro", 4)         // Skip level 3
        );
        
        cacheService.put("gapped-key", Arrays.asList(gappedParams), value);

        // Should find with partial parameters respecting gaps
        Optional<String> result1 = cacheService.get(Arrays.asList(
            new SearchParameter("region", "US", 0)
        ), String.class);
        assertTrue(result1.isPresent());

        Optional<String> result2 = cacheService.get(Arrays.asList(
            new SearchParameter("region", "US", 0),
            new SearchParameter("brand", "apple", 2)
        ), String.class);
        assertTrue(result2.isPresent());

        // Should find with all parameters including gaps
        Optional<String> result3 = cacheService.get(gappedParams, String.class);
        assertTrue(result3.isPresent());
        assertEquals(value, result3.get());
    }

    @Test
    void testMixedParameterCountsInMultiplePatterns() {
        String value = "mixed-pattern-data";
        
        // Cache with multiple patterns of different lengths
        List<List<SearchParameter>> mixedPatterns = Arrays.asList(
            // Pattern 1: Single parameter
            Arrays.asList(
                new SearchParameter("type", "premium", 0)
            ),
            // Pattern 2: Two parameters
            Arrays.asList(
                new SearchParameter("region", "US", 0),
                new SearchParameter("category", "electronics", 1)
            ),
            // Pattern 3: Five parameters
            Arrays.asList(
                new SearchParameter("brand", "apple", 0),
                new SearchParameter("product_line", "iphone", 1),
                new SearchParameter("generation", "15", 2),
                new SearchParameter("storage", "256gb", 3),
                new SearchParameter("color", "blue", 4)
            )
        );
        
        cacheService.put("mixed-key", mixedPatterns, value);

        // Should be findable through any pattern length
        assertTrue(cacheService.get(Arrays.asList(
            new SearchParameter("type", "premium", 0)
        ), String.class).isPresent());

        assertTrue(cacheService.get(Arrays.asList(
            new SearchParameter("region", "US", 0)
        ), String.class).isPresent());

        assertTrue(cacheService.get(Arrays.asList(
            new SearchParameter("brand", "apple", 0),
            new SearchParameter("product_line", "iphone", 1)
        ), String.class).isPresent());

        assertTrue(cacheService.get(Arrays.asList(
            new SearchParameter("brand", "apple", 0),
            new SearchParameter("product_line", "iphone", 1),
            new SearchParameter("generation", "15", 2),
            new SearchParameter("storage", "256gb", 3)
        ), String.class).isPresent());
    }

    @Test
    void testParameterCountStatistics() {
        // Cache items with different parameter counts
        cacheService.put("key1", Arrays.asList(
            Arrays.asList(new SearchParameter("a", "1", 0))  // 1 param
        ), "data1");
        
        cacheService.put("key2", Arrays.asList(
            Arrays.asList(
                new SearchParameter("a", "1", 0),             // 3 params
                new SearchParameter("b", "2", 1),
                new SearchParameter("c", "3", 2)
            )
        ), "data2");
        
        cacheService.put("key3", Arrays.asList(
            Arrays.asList(
                new SearchParameter("x", "1", 0),             // 6 params
                new SearchParameter("y", "2", 1),
                new SearchParameter("z", "3", 2),
                new SearchParameter("w", "4", 3),
                new SearchParameter("v", "5", 4),
                new SearchParameter("u", "6", 5)
            )
        ), "data3");

        var stats = cacheService.getStats();
        
        // Should have 3 keys, 3 data items, and multiple hierarchical references
        assertEquals(3, stats.getKeyCount());
        assertEquals(3, stats.getDataCount());
        assertTrue(stats.getReferenceCount() > 3, "Should have more hierarchical references than data items");
        
        // The item with 6 parameters should create more references than the item with 1 parameter
        assertTrue(stats.getReferenceCount() >= 10, "Should have many hierarchical references from varied parameter counts");
    }
}
