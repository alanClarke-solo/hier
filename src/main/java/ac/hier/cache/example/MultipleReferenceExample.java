package ac.hier.cache.example;

import ac.hier.cache.KryoHierarchicalCacheService;
import ac.hier.cache.RedissonClientFactory;
import ac.hier.cache.SearchParameter;
import org.redisson.api.RedissonClient;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * Example demonstrating multiple hierarchical references for complex search patterns
 */
public class MultipleReferenceExample {
    
    public static void main(String[] args) {
        RedissonClient redissonClient = RedissonClientFactory.createDefault();
        KryoHierarchicalCacheService cacheService = new KryoHierarchicalCacheService(
            redissonClient, "multi_ref_cache", 3600
        );

        try {
            demonstrateMultipleReferences(cacheService);
        } finally {
            redissonClient.shutdown();
        }
    }

    private static void demonstrateMultipleReferences(KryoHierarchicalCacheService cacheService) {
        System.out.println("=== Multiple Hierarchical References Demo ===");
        
        // Create a product that should be searchable by multiple patterns
        Product iPhone = new Product("1", "iPhone 15", "electronics", "apple", 
                                   new BigDecimal("999.99"), "US");

        // Define multiple search parameter sets for the same product
        // This product should be findable through different search paths
        List<List<SearchParameter>> multipleSearchPatterns = Arrays.asList(
            // Pattern 1: Geographic + Category search
            Arrays.asList(
                new SearchParameter("region", "US", 0),
                new SearchParameter("category", "electronics", 1),
                new SearchParameter("subcategory", "smartphones", 2)
            ),
            
            // Pattern 2: Brand + Product type search
            Arrays.asList(
                new SearchParameter("brand", "apple", 0),
                new SearchParameter("product_line", "iphone", 1),
                new SearchParameter("generation", "15", 2)
            ),
            
            // Pattern 3: Price range search
            Arrays.asList(
                new SearchParameter("price_range", "900-1200", 0),
                new SearchParameter("currency", "USD", 1),
                new SearchParameter("target_market", "premium", 2)
            )
        );

        String iPhoneKey = "product:iphone:15:primary";
        
        // Cache the product with multiple search patterns
        System.out.println("\n=== Caching with Multiple References ===");
        cacheService.put(iPhoneKey, multipleSearchPatterns, iPhone);
        System.out.println("Cached iPhone with " + multipleSearchPatterns.size() + " different search patterns");
        
        // Show enhanced statistics
        var stats = cacheService.getStats();
        System.out.println("Cache Statistics after multi-reference caching:");
        System.out.println(stats);

        // Test retrieval using different search patterns
        System.out.println("\n=== Multi-Pattern Retrieval Tests ===");
        
        // Test Pattern 1: Geographic search
        List<SearchParameter> geoSearch = Arrays.asList(
            new SearchParameter("region", "US", 0),
            new SearchParameter("category", "electronics", 1)
        );
        var geoResult = cacheService.get(geoSearch, Product.class);
        System.out.println("Geographic search result: " + 
                          geoResult.map(Product::getName).orElse("NOT FOUND"));

        // Test Pattern 2: Brand search
        List<SearchParameter> brandSearch = Arrays.asList(
            new SearchParameter("brand", "apple", 0),
            new SearchParameter("product_line", "iphone", 1)
        );
        var brandResult = cacheService.get(brandSearch, Product.class);
        System.out.println("Brand search result: " + 
                          brandResult.map(Product::getName).orElse("NOT FOUND"));

        // Test Pattern 3: Price search
        List<SearchParameter> priceSearch = Arrays.asList(
            new SearchParameter("price_range", "900-1200", 0)
        );
        var priceResult = cacheService.get(priceSearch, Product.class);
        System.out.println("Price search result: " + 
                          priceResult.map(Product::getName).orElse("NOT FOUND"));

        // Add more products to demonstrate complex searches
        System.out.println("\n=== Adding More Products ===");
        
        Product macBook = new Product("2", "MacBook Pro", "electronics", "apple", 
                                    new BigDecimal("2499.99"), "US");
        
        List<List<SearchParameter>> macBookPatterns = Arrays.asList(
            // Geographic + Category
            Arrays.asList(
                new SearchParameter("region", "US", 0),
                new SearchParameter("category", "electronics", 1),
                new SearchParameter("subcategory", "laptops", 2)
            ),
            
            // Brand + Product type
            Arrays.asList(
                new SearchParameter("brand", "apple", 0),
                new SearchParameter("product_line", "macbook", 1),
                new SearchParameter("model", "pro", 2)
            ),
            
            // Price range (different from iPhone)
            Arrays.asList(
                new SearchParameter("price_range", "2000-3000", 0),
                new SearchParameter("currency", "USD", 1),
                new SearchParameter("target_market", "professional", 2)
            )
        );
        
        String macBookKey = "product:macbook:pro:primary";
        cacheService.put(macBookKey, macBookPatterns, macBook);
        System.out.println("Added MacBook Pro with multiple search patterns");

        // Test getAll functionality
        System.out.println("\n=== Get All Results Tests ===");
        
        // Search for all Apple products
        List<SearchParameter> appleSearch = Arrays.asList(
            new SearchParameter("brand", "apple", 0)
        );
        List<Product> appleProducts = cacheService.getAll(appleSearch, Product.class);
        System.out.println("All Apple products found: " + appleProducts.size());
        appleProducts.forEach(p -> System.out.println("  - " + p.getName()));

        // Search for all US electronics
        List<SearchParameter> usElectronics = Arrays.asList(
            new SearchParameter("region", "US", 0),
            new SearchParameter("category", "electronics", 1)
        );
        List<Product> usElectronicProducts = cacheService.getAll(usElectronics, Product.class);
        System.out.println("All US electronics found: " + usElectronicProducts.size());
        usElectronicProducts.forEach(p -> System.out.println("  - " + p.getName()));

        // Test findByPattern functionality
        System.out.println("\n=== Pattern Search Tests ===");
        
        Map<String, Product> premiumProducts = cacheService.findByPattern(
            Arrays.asList(new SearchParameter("target_market", "premium", 2)), 
            Product.class
        );
        System.out.println("Premium products found: " + premiumProducts.size());
        premiumProducts.forEach((key, product) -> 
            System.out.println("  Key: " + key + " -> " + product.getName()));

        // Test getOrCompute with multiple patterns
        System.out.println("\n=== GetOrCompute with Multiple Patterns ===");
        
        String newProductKey = "product:ipad:air";
        List<List<SearchParameter>> iPadPatterns = Arrays.asList(
            Arrays.asList(
                new SearchParameter("brand", "apple", 0),
                new SearchParameter("product_line", "ipad", 1)
            ),
            Arrays.asList(
                new SearchParameter("category", "electronics", 0),
                new SearchParameter("subcategory", "tablets", 1)
            )
        );
        
        Product iPad = cacheService.getOrCompute(
            newProductKey,
            iPadPatterns,
            Product.class,
            () -> {
                System.out.println("Computing new iPad (cache miss)...");
                return new Product("3", "iPad Air", "electronics", "apple", 
                                 new BigDecimal("599.99"), "US");
            }
        );
        System.out.println("Computed/cached iPad: " + iPad.getName());

        // Second call should hit cache
        Product cachedIPad = cacheService.getOrCompute(
            newProductKey,
            iPadPatterns,
            Product.class,
            () -> {
                System.out.println("This shouldn't be called (cache hit)");
                return null;
            }
        );
        System.out.println("Retrieved iPad from cache: " + cachedIPad.getName());

        // Final statistics
        var finalStats = cacheService.getStats();
        System.out.println("\n=== Final Statistics ===");
        System.out.println(finalStats);
        System.out.println("Multiple references working efficiently: " + 
                          (finalStats.getAverageReferencesPerData() > 1.0 ? "YES" : "NO"));

        // Test selective invalidation
        System.out.println("\n=== Selective Invalidation Test ===");
        
        // Invalidate only the brand-based search for iPhone
        cacheService.invalidate(Arrays.asList(
            new SearchParameter("brand", "apple", 0),
            new SearchParameter("product_line", "iphone", 1)
        ));
        
        // Brand search should miss now
        var brandSearchAfterInvalidation = cacheService.get(brandSearch, Product.class);
        System.out.println("Brand search after selective invalidation: " + 
                          brandSearchAfterInvalidation.map(Product::getName).orElse("NOT FOUND"));
        
        // But geographic search should still work
        var geoSearchAfterInvalidation = cacheService.get(geoSearch, Product.class);
        System.out.println("Geographic search after selective invalidation: " + 
                          geoSearchAfterInvalidation.map(Product::getName).orElse("NOT FOUND"));
        
        // Direct key access should still work
        var keyAccessAfterInvalidation = cacheService.getByKey(iPhoneKey, Product.class);
        System.out.println("Key access after selective invalidation: " + 
                          keyAccessAfterInvalidation.map(Product::getName).orElse("NOT FOUND"));

        var afterInvalidationStats = cacheService.getStats();
        System.out.println("Stats after selective invalidation:");
        System.out.println(afterInvalidationStats);
    }
}
