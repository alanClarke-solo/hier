// src/main/java/ac/hier/cache/example/ProductSearchExample.java
package ac.hier.cache.example;

import ac.hier.cache.HierarchicalCacheService;
import ac.hier.cache.RedissonClientFactory;
import ac.hier.cache.SearchParameter;
import org.redisson.api.RedissonClient;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.List;

/**
 * Example demonstrating hierarchical cache usage for product search
 */
public class ProductSearchExample {
    
    public static void main(String[] args) {
        // Initialize Redisson client and cache service
        RedissonClient redissonClient = RedissonClientFactory.createDefault();
        HierarchicalCacheService cacheService = new HierarchicalCacheService(
            redissonClient, "product_search", 3600 // 1 hour TTL
        );

        try {
            demonstrateHierarchicalCaching(cacheService);
        } finally {
            redissonClient.shutdown();
        }
    }

    private static void demonstrateHierarchicalCaching(HierarchicalCacheService cacheService) {
        // Create hierarchical search parameters
        // Level 0: Region (highest level)
        // Level 1: Category 
        // Level 2: Brand (most specific)
        List<SearchParameter> searchParams = Arrays.asList(
            new SearchParameter("region", "US", 0),
            new SearchParameter("category", "electronics", 1),
            new SearchParameter("brand", "apple", 2)
        );

        // Simulate getting data (would normally come from database)
        Product product = new Product("1", "iPhone 15", "electronics", "apple", 
                                    new BigDecimal("999.99"), "US");

        System.out.println("=== Hierarchical Cache Demo ===");

        // Cache the product
        cacheService.put(searchParams, product);
        System.out.println("Cached product: " + product);

        // Try to retrieve using exact parameters
        var result1 = cacheService.get(searchParams, Product.class);
        System.out.println("Retrieved with exact params: " + result1.orElse(null));

        // Try to retrieve using partial parameters (should find cached result from higher level)
        List<SearchParameter> partialParams = Arrays.asList(
            new SearchParameter("region", "US", 0),
            new SearchParameter("category", "electronics", 1)
        );
        
        var result2 = cacheService.get(partialParams, Product.class);
        System.out.println("Retrieved with partial params: " + result2.orElse(null));

        // Try to retrieve using only region (should find cached result)
        List<SearchParameter> regionOnlyParams = Arrays.asList(
            new SearchParameter("region", "US", 0)
        );
        
        var result3 = cacheService.get(regionOnlyParams, Product.class);
        System.out.println("Retrieved with region only: " + result3.orElse(null));

        // Demonstrate getOrCompute
        List<SearchParameter> newSearchParams = Arrays.asList(
            new SearchParameter("region", "EU", 0),
            new SearchParameter("category", "books", 1)
        );

        Product computedProduct = cacheService.getOrCompute(
            newSearchParams, 
            Product.class, 
            () -> {
                System.out.println("Computing new product (cache miss)...");
                return new Product("2", "Java Book", "books", "tech-publisher", 
                                 new BigDecimal("49.99"), "EU");
            }
        );
        System.out.println("Computed/cached product: " + computedProduct);

        // Second call should hit cache
        Product cachedProduct = cacheService.getOrCompute(
            newSearchParams, 
            Product.class, 
            () -> {
                System.out.println("This shouldn't be called (cache hit)");
                return null;
            }
        );
        System.out.println("Retrieved from cache: " + cachedProduct);
    }
}
