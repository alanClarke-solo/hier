package ac.hier.cache.example;

import ac.hier.cache.KryoHierarchicalCacheService;
import ac.hier.cache.RedissonClientFactory;
import ac.hier.cache.SearchParameter;
import org.redisson.api.RedissonClient;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.List;

/**
 * Comprehensive example demonstrating handling of search patterns with different parameter counts
 */
public class VariableParameterCountExample {
    
    public static void main(String[] args) {
        RedissonClient redissonClient = RedissonClientFactory.createDefault();
        KryoHierarchicalCacheService cacheService = new KryoHierarchicalCacheService(
            redissonClient, "variable_param_cache", 3600
        );

        try {
            demonstrateVariableParameterCounts(cacheService);
        } finally {
            redissonClient.shutdown();
        }
    }

    private static void demonstrateVariableParameterCounts(KryoHierarchicalCacheService cacheService) {
        System.out.println("=== Variable Parameter Count Demo ===");
        
        // Create products with different levels of specificity
        Product iPhone = new Product("1", "iPhone 15", "electronics", "apple", 
                                   new BigDecimal("999.99"), "US");
        Product macBook = new Product("2", "MacBook Pro", "electronics", "apple", 
                                    new BigDecimal("2499.99"), "US");
        Product book = new Product("3", "Java Programming", "books", "tech-press", 
                                 new BigDecimal("49.99"), "US");

        System.out.println("\n=== Caching with Different Parameter Counts ===");
        
        // Cache iPhone with MANY parameters (6 levels)
        List<List<SearchParameter>> iPhonePatterns = Arrays.asList(
            Arrays.asList(
                new SearchParameter("region", "US", 0),
                new SearchParameter("category", "electronics", 1),
                new SearchParameter("subcategory", "smartphones", 2),
                new SearchParameter("brand", "apple", 3),
                new SearchParameter("product_line", "iphone", 4),
                new SearchParameter("model", "15", 5)
            )
        );
        cacheService.put("iphone-key", iPhonePatterns, iPhone);
        System.out.println("Cached iPhone with 6-level hierarchy");

        // Cache MacBook with MEDIUM parameters (4 levels)
        List<List<SearchParameter>> macBookPatterns = Arrays.asList(
            Arrays.asList(
                new SearchParameter("region", "US", 0),
                new SearchParameter("category", "electronics", 1),
                new SearchParameter("brand", "apple", 3),  // Note: skipped level 2
                new SearchParameter("product_line", "macbook", 4)
            )
        );
        cacheService.put("macbook-key", macBookPatterns, macBook);
        System.out.println("Cached MacBook with 4-level hierarchy (with gap at level 2)");

        // Cache Book with FEW parameters (2 levels)
        List<List<SearchParameter>> bookPatterns = Arrays.asList(
            Arrays.asList(
                new SearchParameter("region", "US", 0),
                new SearchParameter("category", "books", 1)
            )
        );
        cacheService.put("book-key", bookPatterns, book);
        System.out.println("Cached Book with 2-level hierarchy");

        // Show statistics
        var stats = cacheService.getStats();
        System.out.println("\nCache Statistics after variable-count caching:");
        System.out.println(stats);

        System.out.println("\n=== Search with Different Parameter Counts ===");
        
        // Test 1: Single parameter searches
        System.out.println("\n--- Single Parameter Searches ---");
        testSingleParameterSearch(cacheService, "region", "US");
        testSingleParameterSearch(cacheService, "category", "electronics");
        testSingleParameterSearch(cacheService, "category", "books");
        testSingleParameterSearch(cacheService, "brand", "apple");

        // Test 2: Two parameter searches
        System.out.println("\n--- Two Parameter Searches ---");
        testMultiParameterSearch(cacheService, Arrays.asList(
            new SearchParameter("region", "US", 0),
            new SearchParameter("category", "electronics", 1)
        ), "US Electronics");
        
        testMultiParameterSearch(cacheService, Arrays.asList(
            new SearchParameter("region", "US", 0),
            new SearchParameter("category", "books", 1)
        ), "US Books");

        // Test 3: Three parameter searches
        System.out.println("\n--- Three Parameter Searches ---");
        testMultiParameterSearch(cacheService, Arrays.asList(
            new SearchParameter("region", "US", 0),
            new SearchParameter("category", "electronics", 1),
            new SearchParameter("brand", "apple", 3)  // Note: Level 3, not 2
        ), "US Apple Electronics");

        // Test 4: Very specific searches
        System.out.println("\n--- Very Specific Searches ---");
        testMultiParameterSearch(cacheService, Arrays.asList(
            new SearchParameter("region", "US", 0),
            new SearchParameter("category", "electronics", 1),
            new SearchParameter("subcategory", "smartphones", 2),
            new SearchParameter("brand", "apple", 3),
            new SearchParameter("product_line", "iphone", 4)
        ), "Specific iPhone Search");

        // Test 5: Search with gaps in levels
        System.out.println("\n--- Searches with Level Gaps ---");
        testMultiParameterSearch(cacheService, Arrays.asList(
            new SearchParameter("region", "US", 0),
            new SearchParameter("brand", "apple", 3),  // Skip levels 1,2
            new SearchParameter("product_line", "macbook", 4)
        ), "Gap Search (MacBook)");

        // Test 6: getAllResults with different parameter counts
        System.out.println("\n=== Get All Results with Variable Counts ===");
        
        // Get all US products (should find all 3)
        List<Product> usProducts = cacheService.getAll(Arrays.asList(
            new SearchParameter("region", "US", 0)
        ), Product.class);
        System.out.println("All US products (1 param): " + usProducts.size());
        usProducts.forEach(p -> System.out.println("  - " + p.getName()));

        // Get all electronics (should find iPhone and MacBook)
        List<Product> electronics = cacheService.getAll(Arrays.asList(
            new SearchParameter("category", "electronics", 1)
        ), Product.class);
        System.out.println("All electronics (1 param): " + electronics.size());
        electronics.forEach(p -> System.out.println("  - " + p.getName()));

        // Get all Apple products with multiple parameters
        List<Product> appleProducts = cacheService.getAll(Arrays.asList(
            new SearchParameter("region", "US", 0),
            new SearchParameter("brand", "apple", 3)
        ), Product.class);
        System.out.println("Apple products (2 params): " + appleProducts.size());
        appleProducts.forEach(p -> System.out.println("  - " + p.getName()));

        System.out.println("\n=== Cross-Level Pattern Matching ===");
        
        // Add same product with different parameter structures
        Product sameiPhone = new Product("1", "iPhone 15", "electronics", "apple", 
                                       new BigDecimal("999.99"), "US");
        
        // Different structure: Brand-first hierarchy
        List<List<SearchParameter>> brandFirstPattern = Arrays.asList(
            Arrays.asList(
                new SearchParameter("brand", "apple", 0),        // Brand at level 0
                new SearchParameter("region", "US", 1),          // Region at level 1
                new SearchParameter("product_type", "phone", 2)   // Type at level 2
            )
        );
        cacheService.put("iphone-brand-first", brandFirstPattern, sameiPhone);
        System.out.println("Added same iPhone with brand-first hierarchy");

        // Now search with different parameter counts on both structures
        testMultiParameterSearch(cacheService, Arrays.asList(
            new SearchParameter("brand", "apple", 0)
        ), "Brand-first search");

        testMultiParameterSearch(cacheService, Arrays.asList(
            new SearchParameter("brand", "apple", 0),
            new SearchParameter("region", "US", 1)
        ), "Brand+Region search");

        // Final comprehensive statistics
        var finalStats = cacheService.getStats();
        System.out.println("\n=== Final Statistics ===");
        System.out.println(finalStats);
        System.out.println("Successfully demonstrated variable parameter count handling!");
    }

    private static void testSingleParameterSearch(KryoHierarchicalCacheService cacheService, 
                                                String key, String value) {
        List<SearchParameter> params = Arrays.asList(new SearchParameter(key, value, 0));
        var result = cacheService.get(params, Product.class);
        System.out.printf("Search [%s=%s]: %s%n", key, value, 
                         result.map(Product::getName).orElse("NOT FOUND"));
    }

    private static void testMultiParameterSearch(KryoHierarchicalCacheService cacheService, 
                                               List<SearchParameter> params, String description) {
        var result = cacheService.get(params, Product.class);
        System.out.printf("%s: %s%n", description, 
                         result.map(Product::getName).orElse("NOT FOUND"));
    }
}
