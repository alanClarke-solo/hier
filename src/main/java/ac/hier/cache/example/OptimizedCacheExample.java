// src/main/java/ac/hier/cache/example/OptimizedCacheExample.java
package ac.hier.cache.example;

import ac.hier.cache.HierarchicalCacheService;
import ac.hier.cache.RedissonClientFactory;
import ac.hier.cache.SearchParameter;
import org.redisson.api.RedissonClient;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.List;

/**
 * Example demonstrating optimized hierarchical cache with reference-based storage
 */
public class OptimizedCacheExample {
    
    public static void main(String[] args) {
        RedissonClient redissonClient = RedissonClientFactory.createDefault();
        HierarchicalCacheService cacheService = new HierarchicalCacheService(
            redissonClient, "optimized_cache", 3600
        );

        try {
            demonstrateOptimizedCaching(cacheService);
        } finally {
            redissonClient.shutdown();
        }
    }

    private static void demonstrateOptimizedCaching(HierarchicalCacheService cacheService) {
        System.out.println("=== Optimized Hierarchical Cache Demo ===");
        
        // Create the same product that will be cached multiple times
        Product product = new Product("1", "iPhone 15", "electronics", "apple", 
                                    new BigDecimal("999.99"), "US");

        // Cache the same product with different search parameter combinations
        List<List<SearchParameter>> differentSearchCombinations = Arrays.asList(
            // Same product for US region + electronics category
            Arrays.asList(
                new SearchParameter("region", "US", 0),
                new SearchParameter("category", "electronics", 1)
            ),
            // Same product for US region + electronics + apple brand
            Arrays.asList(
                new SearchParameter("region", "US", 0),
                new SearchParameter("category", "electronics", 1),
                new SearchParameter("brand", "apple", 2)
            ),
            // Same product for different search combination but same result
            Arrays.asList(
                new SearchParameter("region", "US", 0),
                new SearchParameter("category", "electronics", 1),
                new SearchParameter("brand", "apple", 2),
                new SearchParameter("price_range", "900-1000", 3)
            )
        );

        // Cache the same product data with different search combinations
        for (int i = 0; i < differentSearchCombinations.size(); i++) {
            List<SearchParameter> params = differentSearchCombinations.get(i);
            cacheService.put(params, product);
            System.out.printf("Cached product with search combination %d: %s%n", i + 1, params);
        }

        // Show cache statistics
        var stats = cacheService.getStats();
        System.out.println("\nCache Statistics:");
        System.out.println(stats);
        System.out.printf("Data deduplication: %d references point to %d actual data entries%n", 
                         stats.getReferenceCount(), stats.getDataCount());

        // Retrieve data using different parameter combinations
        System.out.println("\n=== Retrieval Tests ===");
        
        for (int i = 0; i < differentSearchCombinations.size(); i++) {
            List<SearchParameter> params = differentSearchCombinations.get(i);
            var result = cacheService.get(params, Product.class);
            System.out.printf("Retrieved with combination %d: %s%n", i + 1, 
                             result.map(Product::getName).orElse("NOT FOUND"));
        }

        // Test hierarchical retrieval
        System.out.println("\n=== Hierarchical Retrieval Test ===");
        List<SearchParameter> partialParams = Arrays.asList(
            new SearchParameter("region", "US", 0)
        );
        
        var hierarchicalResult = cacheService.get(partialParams, Product.class);
        System.out.printf("Retrieved with partial params (region only): %s%n", 
                         hierarchicalResult.map(Product::getName).orElse("NOT FOUND"));

        // Demonstrate that different data creates separate storage
        System.out.println("\n=== Different Data Test ===");
        Product differentProduct = new Product("2", "MacBook Pro", "electronics", "apple", 
                                             new BigDecimal("2499.99"), "US");
        
        List<SearchParameter> differentProductParams = Arrays.asList(
            new SearchParameter("region", "US", 0),
            new SearchParameter("category", "electronics", 1),
            new SearchParameter("brand", "apple", 2)
        );
        
        cacheService.put(differentProductParams, differentProduct);
        System.out.println("Cached different product with similar parameters");
        
        var finalStats = cacheService.getStats();
        System.out.println("\nFinal Cache Statistics:");
        System.out.println(finalStats);
        
        // Clean up demonstration
        System.out.println("\n=== Cleanup Test ===");
        cacheService.invalidateWithCleanup(differentSearchCombinations.get(0));
        var afterCleanupStats = cacheService.getStats();
        System.out.println("After cleanup:");
        System.out.println(afterCleanupStats);
    }
}
