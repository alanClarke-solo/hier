package ac.hier.cache;

import org.junit.jupiter.api.*;
import org.redisson.Redisson;
import org.redisson.api.RedissonClient;
import org.redisson.codec.Kryo5Codec;
import org.redisson.config.Config;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.math.BigDecimal;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

@Testcontainers
class KryoHierarchicalCacheServiceIntegrationTest {

    @Container
    static GenericContainer<?> redis = new GenericContainer<>(DockerImageName.parse("redis:7-alpine"))
            .withExposedPorts(6379);

    private RedissonClient redissonClient;
    private KryoHierarchicalCacheService cacheService;

    @BeforeEach
    void setUp() {
        Config config = new Config();
        config.useSingleServer()
                .setAddress("redis://" + redis.getHost() + ":" + redis.getFirstMappedPort());
        config.setCodec(new Kryo5Codec());
        
        redissonClient = Redisson.create(config);
        cacheService = new KryoHierarchicalCacheService(redissonClient, "integration_test", 3600);
        
        // Clear any existing data
        cacheService.clearAll();
    }

    @AfterEach
    void tearDown() {
        if (redissonClient != null) {
            redissonClient.shutdown();
        }
    }

    @Test
    @DisplayName("End-to-end cache operations")
    void endToEndCacheOperations() {
        // Create test data
        TestProduct product = new TestProduct("1", "iPhone 15", "Electronics", "Apple", new BigDecimal("999.99"));
        
        List<SearchParameter> searchParams = Arrays.asList(
            new SearchParameter("region", "US", 0),
            new SearchParameter("category", "Electronics", 1),
            new SearchParameter("brand", "Apple", 2)
        );

        // Test put and get
        String key = cacheService.putWithSingleParameterSet("iphone-15", searchParams, product);
        assertEquals("iphone-15", key);

        // Test retrieval by key
        Optional<TestProduct> byKey = cacheService.getByKey("iphone-15", TestProduct.class);
        assertTrue(byKey.isPresent());
        assertEquals(product, byKey.get());

        // Test hierarchical retrieval
        List<SearchParameter> partialParams = Arrays.asList(
            new SearchParameter("region", "US", 0),
            new SearchParameter("category", "Electronics", 1)
        );
        
        Optional<TestProduct> byHierarchy = cacheService.getByHierarchicalSearch(partialParams, TestProduct.class);
        assertTrue(byHierarchy.isPresent());
        assertEquals(product, byHierarchy.get());

        // Test statistics
        var stats = cacheService.getStats();
        assertTrue(stats.getKeyCount() > 0);
        assertTrue(stats.getDataCount() > 0);

        // Test invalidation
        cacheService.invalidateByKey("iphone-15");
        Optional<TestProduct> afterInvalidation = cacheService.getByKey("iphone-15", TestProduct.class);
        assertFalse(afterInvalidation.isPresent());
    }

    @Test
    @DisplayName("Data deduplication functionality")
    void dataDeduplicationFunctionality() {
        TestProduct product = new TestProduct("1", "MacBook", "Computers", "Apple", new BigDecimal("1299.99"));
        
        // Store same product with different search parameter combinations
        List<List<SearchParameter>> parameterSets = Arrays.asList(
            Arrays.asList(
                new SearchParameter("region", "US", 0),
                new SearchParameter("category", "Computers", 1)
            ),
            Arrays.asList(
                new SearchParameter("region", "US", 0),
                new SearchParameter("category", "Computers", 1),
                new SearchParameter("brand", "Apple", 2)
            ),
            Arrays.asList(
                new SearchParameter("region", "US", 0),
                new SearchParameter("category", "Computers", 1),
                new SearchParameter("brand", "Apple", 2),
                new SearchParameter("price_range", "1000-1500", 3)
            )
        );

        // Store with multiple parameter sets
        for (int i = 0; i < parameterSets.size(); i++) {
            cacheService.putWithSingleParameterSet("macbook-" + i, parameterSets.get(i), product);
        }

        // Verify data deduplication
        var stats = cacheService.getStats();
        assertEquals(1L, stats.getDataCount()); // Should have only 1 actual data entry
        assertTrue(stats.getReferenceCount() > 1); // But multiple references
        assertTrue(stats.getCompressionRatio() > 1.0); // Good compression ratio
    }

    @Test
    @DisplayName("GetOrCompute functionality")
    void getOrComputeFunctionality() {
        List<SearchParameter> searchParams = Arrays.asList(
            new SearchParameter("region", "EU", 0),
            new SearchParameter("category", "Books", 1)
        );

        AtomicInteger computeCount = new AtomicInteger(0);
        
        // First call should compute
        TestProduct result1 = cacheService.getOrComputeWithSingleParameterSet(
            "book-1", searchParams, TestProduct.class, () -> {
                computeCount.incrementAndGet();
                return new TestProduct("book-1", "Java Guide", "Books", "TechPress", new BigDecimal("49.99"));
            }
        );
        
        assertNotNull(result1);
        assertEquals(1, computeCount.get());

        // Second call should hit cache
        TestProduct result2 = cacheService.getOrComputeWithSingleParameterSet(
            "book-1", searchParams, TestProduct.class, () -> {
                computeCount.incrementAndGet();
                return new TestProduct("should-not-be-called", "Should Not", "Be Called", "Never", BigDecimal.ZERO);
            }
        );
        
        assertNotNull(result2);
        assertEquals(result1, result2);
        assertEquals(1, computeCount.get()); // Should not have computed again
    }

    @Test
    @DisplayName("Concurrent access safety")
    void concurrentAccessSafety() throws InterruptedException {
        int numberOfThreads = 10;
        int operationsPerThread = 50;
        ExecutorService executor = Executors.newFixedThreadPool(numberOfThreads);
        CountDownLatch latch = new CountDownLatch(numberOfThreads);
        AtomicInteger successCount = new AtomicInteger(0);

        for (int i = 0; i < numberOfThreads; i++) {
            final int threadId = i;
            executor.submit(() -> {
                try {
                    for (int j = 0; j < operationsPerThread; j++) {
                        String key = "thread-" + threadId + "-item-" + j;
                        TestProduct product = new TestProduct(
                            key, 
                            "Product " + key, 
                            "Category" + (j % 3), 
                            "Brand" + (j % 2), 
                            new BigDecimal(100 + j)
                        );
                        
                        List<SearchParameter> params = Arrays.asList(
                            new SearchParameter("thread", String.valueOf(threadId), 0),
                            new SearchParameter("category", "Category" + (j % 3), 1)
                        );

                        // Put and get
                        cacheService.putWithSingleParameterSet(key, params, product);
                        Optional<TestProduct> retrieved = cacheService.getByKey(key, TestProduct.class);
                        
                        if (retrieved.isPresent() && retrieved.get().equals(product)) {
                            successCount.incrementAndGet();
                        }
                    }
                } finally {
                    latch.countDown();
                }
            });
        }

        latch.await();
        executor.shutdown();

        // Verify all operations succeeded
        assertEquals(numberOfThreads * operationsPerThread, successCount.get());
        
        // Verify cache state
        var stats = cacheService.getStats();
        assertTrue(stats.getKeyCount() > 0);
        assertTrue(stats.getDataCount() > 0);
    }

    @Test
    @DisplayName("TTL functionality")
    void ttlFunctionality() throws InterruptedException {
        TestProduct product = new TestProduct("ttl-test", "TTL Product", "Test", "Test", new BigDecimal("10.00"));
        List<SearchParameter> params = Arrays.asList(
            new SearchParameter("test", "ttl", 0)
        );

        // Store with very short TTL
        cacheService.putWithSingleParameterSet("ttl-key", params, product, 1L); // 1 second TTL
        
        // Should be available immediately
        Optional<TestProduct> immediate = cacheService.getByKey("ttl-key", TestProduct.class);
        assertTrue(immediate.isPresent());
        
        // Wait for expiration
        Thread.sleep(2000);
        
        // Should be expired
        Optional<TestProduct> afterExpiry = cacheService.getByKey("ttl-key", TestProduct.class);
        assertFalse(afterExpiry.isPresent());
    }

    // Test data class
    private static class TestProduct {
        private String id;
        private String name;
        private String category;
        private String brand;
        private BigDecimal price;

        public TestProduct() {}

        public TestProduct(String id, String name, String category, String brand, BigDecimal price) {
            this.id = id;
            this.name = name;
            this.category = category;
            this.brand = brand;
            this.price = price;
        }

        // Getters and setters
        public String getId() { return id; }
        public void setId(String id) { this.id = id; }
        public String getName() { return name; }
        public void setName(String name) { this.name = name; }
        public String getCategory() { return category; }
        public void setCategory(String category) { this.category = category; }
        public String getBrand() { return brand; }
        public void setBrand(String brand) { this.brand = brand; }
        public BigDecimal getPrice() { return price; }
        public void setPrice(BigDecimal price) { this.price = price; }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            TestProduct that = (TestProduct) o;
            return Objects.equals(id, that.id) && 
                   Objects.equals(name, that.name) && 
                   Objects.equals(category, that.category) && 
                   Objects.equals(brand, that.brand) && 
                   Objects.equals(price, that.price);
        }

        @Override
        public int hashCode() {
            return Objects.hash(id, name, category, brand, price);
        }

        @Override
        public String toString() {
            return String.format("TestProduct{id='%s', name='%s', category='%s', brand='%s', price=%s}",
                    id, name, category, brand, price);
        }
    }
}
