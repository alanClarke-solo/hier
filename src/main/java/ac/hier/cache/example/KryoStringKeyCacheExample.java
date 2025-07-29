// src/main/java/ac/hier/cache/example/KryoStringKeyCacheExample.java
package ac.hier.cache.example;

import ac.hier.cache.KryoHierarchicalCacheService;
import ac.hier.cache.RedissonClientFactory;
import ac.hier.cache.SearchParameter;
import org.redisson.api.RedissonClient;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.List;

/**
 * Example demonstrating Kryo-based hierarchical cache with String key support
 */
public class KryoStringKeyCacheExample {
    
    public static void main(String[] args) {
        RedissonClient redissonClient = RedissonClientFactory.createDefault();
        KryoHierarchicalCacheService cacheService = new KryoHierarchicalCacheService(
            redissonClient, "kryo_string_cache", 3600
        );

        try {
            demonstrateStringKeyCache(cacheService);
        } finally {
            redissonClient.shutdown();
        }
    }

    private static void demonstrateStringKeyCache(KryoHierarchicalCacheService cacheService) {
        System.out.println("=== Kryo String Key Hierarchical Cache Demo ===");
        
        // Create test products
        Product iPhone = new Product("1", "iPhone 15", "electronics", "apple", 
                                   new BigDecimal("999.99"), "US");
        Product macBook = new Product("2", "MacBook Pro", "electronics", "apple", 
                                    new BigDecimal("2499.99"), "US");

        // Define String keys for direct access
        String iPhoneKey = "product:iphone:15";
        String macBookKey = "product:macbook:pro";

        // Define hierarchical search parameters
        List<SearchParameter> iPhoneParams = Arrays.asList(
            new SearchParameter("region", "US", 0),
            new SearchParameter("category", "electronics", 1),
            new SearchParameter("brand", "apple", 2),
            new SearchParameter("product_type", "phone", 3)
        );

        List<SearchParameter> macBookParams = Arrays.asList(
            new SearchParameter("region", "US", 0),
            new SearchParameter("category", "electronics", 1),
            new SearchParameter("brand", "apple", 2),
            new SearchParameter("product_type", "laptop", 3)
        );

        // Cache products with String keys and hierarchical parameters
        System.out.println("\n=== Caching Products ===");
        cacheService.put(iPhoneKey, iPhoneParams, iPhone);
        System.out.println("Cached iPhone with key: " + iPhoneKey);
        
        cacheService.put(macBookKey, macBookParams, macBook);
        System.out.println("Cached MacBook with key: " + macBookKey);

        // Show cache statistics
        var stats = cacheService.getStats();
        System.out.println("\nCache Statistics after adding products:");
        System.out.println(stats);

        // Test String key-based retrieval (most specific)
        System.out.println("\n=== String Key-based Retrieval ===");
        var iPhoneByKey = cacheService.getByKey(iPhoneKey, Product.class);
        System.out.println("Retrieved iPhone by key: " + 
                          iPhoneByKey.map(Product::getName).orElse("NOT FOUND"));

        var macBookByKey = cacheService.getByKey(macBookKey, Product.class);
        System.out.println("Retrieved MacBook by key: " + 
                          macBookByKey.map(Product::getName).orElse("NOT FOUND"));

        // Test hierarchical retrieval
        System.out.println("\n=== Hierarchical Retrieval Tests ===");
        
        // Exact match
        var exactMatch = cacheService.get(iPhoneParams, Product.class);
        System.out.println("Exact hierarchical match: " + 
                          exactMatch.map(Product::getName).orElse("NOT FOUND"));

        // Partial match (should find iPhone or MacBook)
        List<SearchParameter> partialParams = Arrays.asList(
            new SearchParameter("region", "US", 0),
            new SearchParameter("category", "electronics", 1),
            new SearchParameter("brand", "apple", 2)
        );
        var partialMatch = cacheService.get(partialParams, Product.class);
        System.out.println("Partial hierarchical match (apple electronics): " + 
                          partialMatch.map(Product::getName).orElse("NOT FOUND"));

        // Even more general match
        List<SearchParameter> generalParams = Arrays.asList(
            new SearchParameter("region", "US", 0),
            new SearchParameter("category", "electronics", 1)
        );
        var generalMatch = cacheService.get(generalParams, Product.class);
        System.out.println("General hierarchical match (US electronics): " + 
                          generalMatch.map(Product::getName).orElse("NOT FOUND"));

        // Test getOrCompute with String key
        System.out.println("\n=== GetOrCompute with String Key ===");
        String newProductKey = "product:java:book";
        List<SearchParameter> newProductParams = Arrays.asList(
            new SearchParameter("region", "EU", 0),
            new SearchParameter("category", "books", 1)
        );

        Product computedProduct = cacheService.getOrCompute(
            newProductKey,
            newProductParams,
            Product.class,
            () -> {
                System.out.println("Computing new product (cache miss)...");
                return new Product("3", "Java Programming Book", "books", "tech-publisher", 
                                 new BigDecimal("49.99"), "EU");
            }
        );
        System.out.println("Computed/cached product: " + computedProduct.getName());
        System.out.println("Product key: " + newProductKey);

        // Second call should hit cache by String key
        Product cachedProduct = cacheService.getOrCompute(
            newProductKey,
            newProductParams,
            Product.class,
            () -> {
                System.out.println("This shouldn't be called (cache hit by key)");
                return null;
            }
        );
        System.out.println("Retrieved from cache by key: " + cachedProduct.getName());

        // Test cache without String key (auto-generated)
        System.out.println("\n=== Cache without String Key ===");
        List<SearchParameter> noKeyParams = Arrays.asList(
            new SearchParameter("region", "ASIA", 0),
            new SearchParameter("category", "toys", 1)
        );

        Product toyProduct = new Product("4", "LEGO Set", "toys", "lego", 
                                       new BigDecimal("79.99"), "ASIA");
        
        String generatedKey = cacheService.put(null, noKeyParams, toyProduct); // Key will be generated
        System.out.println("Cached toy product with generated key: " + generatedKey);

        // Retrieve by generated key
        var toyByKey = cacheService.getByKey(generatedKey, Product.class);
        System.out.println("Retrieved toy by generated key: " + 
                          toyByKey.map(Product::getName).orElse("NOT FOUND"));

        // Test data deduplication with same content
        System.out.println("\n=== Data Deduplication Test ===");
        Product duplicateIPhone = new Product("1", "iPhone 15", "electronics", "apple", 
                                            new BigDecimal("999.99"), "US");
        
        String duplicateKey = "product:iphone:15:duplicate";
        List<SearchParameter> duplicateParams = Arrays.asList(
            new SearchParameter("region", "US", 0),
            new SearchParameter("category", "electronics", 1),
            new SearchParameter("brand", "apple", 2),
            new SearchParameter("product_type", "smartphone", 3)
        );
        
        cacheService.put(duplicateKey, duplicateParams, duplicateIPhone);
        System.out.println("Cached duplicate iPhone content with different key/params");

        // Final statistics should show deduplication
        var finalStats = cacheService.getStats();
        System.out.println("\nFinal Cache Statistics:");
        System.out.println(finalStats);
        System.out.println("Deduplication working: " + 
                          (finalStats.getTotalReferences() > finalStats.getDataCount() ? "YES" : "NO"));

        // Demonstrate invalidation
        System.out.println("\n=== Invalidation Test ===");
        cacheService.invalidateByKey(iPhoneKey);
        var afterInvalidation = cacheService.getByKey(iPhoneKey, Product.class);
        System.out.println("iPhone after key invalidation: " + 
                          afterInvalidation.map(Product::getName).orElse("NOT FOUND"));

        var afterInvalidationStats = cacheService.getStats();
        System.out.println("Stats after invalidation:");
        System.out.println(afterInvalidationStats);
    }
}
