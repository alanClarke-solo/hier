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
import java.util.concurrent.*;

import static org.junit.jupiter.api.Assertions.*;

@Testcontainers
@Tag("performance")
class KryoHierarchicalCacheServicePerformanceTest {

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
        cacheService = new KryoHierarchicalCacheService(redissonClient, "perf_test", 3600);
        cacheService.clearAll();
    }

    @AfterEach
    void tearDown() {
        if (redissonClient != null) {
            redissonClient.shutdown();
        }
    }

    @Test
    @DisplayName("Performance test: Bulk operations")
    void performanceBulkOperations() {
        int numItems = 1000;
        List<TestItem> items = new ArrayList<>();
        
        // Generate test data
        for (int i = 0; i < numItems; i++) {
            items.add(new TestItem("item-" + i, "Name " + i, "Category" + (i % 10), new BigDecimal(i)));
        }

        // Measure put operations
        long putStartTime = System.currentTimeMillis();
        for (int i = 0; i < numItems; i++) {
            TestItem item = items.get(i);
            List<SearchParameter> params = Arrays.asList(
                new SearchParameter("category", item.getCategory(), 0),
                new SearchParameter("id", item.getId(), 1)
            );
            cacheService.putWithSingleParameterSet("key-" + i, params, item);
        }
        long putEndTime = System.currentTimeMillis();
        long putDuration = putEndTime - putStartTime;

        System.out.printf("Put %d items in %d ms (%.2f items/sec)%n", 
                         numItems, putDuration, (double) numItems * 1000 / putDuration);

        // Measure get operations
        long getStartTime = System.currentTimeMillis();
        int hitCount = 0;
        for (int i = 0; i < numItems; i++) {
            Optional<TestItem> result = cacheService.getByKey("key-" + i, TestItem.class);
            if (result.isPresent()) {
                hitCount++;
            }
        }
        long getEndTime = System.currentTimeMillis();
        long getDuration = getEndTime - getStartTime;

        System.out.printf("Retrieved %d/%d items in %d ms (%.2f items/sec)%n", 
                         hitCount, numItems, getDuration, (double) numItems * 1000 / getDuration);

        // Verify cache efficiency
        var stats = cacheService.getStats();
        System.out.println("Cache stats: " + stats);
        
        assertTrue(putDuration < 30000); // Should complete within 30 seconds
        assertTrue(getDuration < 10000);  // Should complete within 10 seconds
        assertEquals(numItems, hitCount); // All items should be retrievable
    }

    @Test
    @DisplayName("Performance test: Concurrent access")
    void performanceConcurrentAccess() throws InterruptedException {
        int numberOfThreads = 20;
        int operationsPerThread = 100;
        ExecutorService executor = Executors.newFixedThreadPool(numberOfThreads);
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch endLatch = new CountDownLatch(numberOfThreads);
        
        AtomicInteger totalOperations = new AtomicInteger(0);
        long startTime = System.currentTimeMillis();

        for (int i = 0; i < numberOfThreads; i++) {
            final int threadId = i;
            executor.submit(() -> {
                try {
                    startLatch.await(); // Wait for all threads to be ready
                    
                    for (int j = 0; j < operationsPerThread; j++) {
                        String key = "thread-" + threadId + "-op-" + j;
                        TestItem item = new TestItem(key, "Item " + key, "Cat" + (j % 5), new BigDecimal(j));
                        
                        List<SearchParameter> params = Arrays.asList(
                            new SearchParameter("thread", String.valueOf(threadId), 0),
                            new SearchParameter("operation", String.valueOf(j), 1)
                        );

                        // Put and immediately get
                        cacheService.putWithSingleParameterSet(key, params, item);
                        Optional<TestItem> retrieved = cacheService.getByKey(key, TestItem.class);
                        
                        if (retrieved.isPresent()) {
                            totalOperations.incrementAndGet();
                        }
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    endLatch.countDown();
                }
            });
        }

        startLatch.countDown(); // Start all threads
        endLatch.await(); // Wait for all to complete
        long endTime = System.currentTimeMillis();
        long duration = endTime - startTime;

        executor.shutdown();

        int expectedOperations = numberOfThreads * operationsPerThread;
        double operationsPerSecond = (double) expectedOperations * 1000 / duration;

        System.out.printf("Concurrent test: %d threads, %d ops/thread, %d total ops in %d ms (%.2f ops/sec)%n",
                         numberOfThreads, operationsPerThread, expectedOperations, duration, operationsPerSecond);

        assertEquals(expectedOperations, totalOperations.get());
        assertTrue(duration < 60000); // Should complete within 1 minute
    }

    private static class TestItem {
        private String id;
        private String name;
        private String category;
        private BigDecimal value;

        public TestItem() {}

        public TestItem(String id, String name, String category, BigDecimal value) {
            this.id = id;
            this.name = name;
            this.category = category;
            this.value = value;
        }

        public String getId() { return id; }
        public void setId(String id) { this.id = id; }
        public String getName() { return name; }
        public void setName(String name) { this.name = name; }
        public String getCategory() { return category; }
        public void setCategory(String category) { this.category = category; }
        public BigDecimal getValue() { return value; }
        public void setValue(BigDecimal value) { this.value = value; }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            TestItem testItem = (TestItem) o;
            return Objects.equals(id, testItem.id) && 
                   Objects.equals(name, testItem.name) && 
                   Objects.equals(category, testItem.category) && 
                   Objects.equals(value, testItem.value);
        }

        @Override
        public int hashCode() {
            return Objects.hash(id, name, category, value);
        }
    }
}
