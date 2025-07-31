package ac.hier.cache;

import org.junit.jupiter.api.*;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.redisson.api.*;

import java.util.*;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class KryoHierarchicalCacheServiceTest {

    @Mock
    private RedissonClient redissonClient;
    
    @Mock
    private RBatch batch;
    
    @Mock
    private RBucketAsync<Object> bucketAsync;
    
    @Mock
    private RMapAsync<Object, Object> mapAsync;
    
    @Mock
    private RSetAsync<Object> setAsync;
    
    @Mock
    private RAtomicLongAsync atomicLongAsync;
    
    @Mock
    private RBucket<Object> bucket;
    
    @Mock
    private RMap<String, String> map;
    
    @Mock
    private RSet<String> set;
    
    @Mock
    private RAtomicLong atomicLong;
    
    @Mock
    private RKeys keys;

    private KryoHierarchicalCacheService cacheService;
    private TestObject testObject;
    private List<SearchParameter> searchParameters;

    @BeforeEach
    void setUp() {
        cacheService = new KryoHierarchicalCacheService(redissonClient, "test", 3600);
        
        testObject = new TestObject("test-id", "Test Name", "Test Value");
        
        searchParameters = Arrays.asList(
            new SearchParameter("region", "US", 0),
            new SearchParameter("category", "electronics", 1),
            new SearchParameter("brand", "apple", 2)
        );
        
        // Default mock behaviors
        when(redissonClient.createBatch()).thenReturn(batch);
        when(redissonClient.getKeys()).thenReturn(keys);
    }

    @Nested
    @DisplayName("PUT Operations")
    class PutOperationsTest {

        @Test
        @DisplayName("Should store data with multiple parameter sets successfully")
        void shouldPutWithMultipleParameterSets() {
            // Arrange
            List<List<SearchParameter>> parameterSets = Arrays.asList(
                searchParameters,
                Arrays.asList(new SearchParameter("region", "US", 0))
            );
            
            setupBatchMocks();
            
            // Act
            String result = cacheService.putWithMultipleParameterSets("unique-key", parameterSets, testObject);
            
            // Assert
            assertNotNull(result);
            assertEquals("unique-key", result);
            verify(batch).execute();
            verify(bucketAsync).setAsync(eq(testObject), eq(3900L), eq(TimeUnit.SECONDS));
        }

        @Test
        @DisplayName("Should generate unique key when null provided")
        void shouldGenerateUniqueKeyWhenNullProvided() {
            // Arrange
            setupBatchMocks();
            
            // Act
            String result = cacheService.putWithMultipleParameterSets(null, 
                Collections.singletonList(searchParameters), testObject);
            
            // Assert
            assertNotNull(result);
            assertTrue(result.startsWith("key_"));
            verify(batch).execute();
        }

        @Test
        @DisplayName("Should handle null value gracefully")
        void shouldHandleNullValueGracefully() {
            // Act
            String result = cacheService.putWithMultipleParameterSets("key", 
                Collections.singletonList(searchParameters), null);
            
            // Assert
            assertEquals("key", result);
            verify(batch, never()).execute();
        }

        @Test
        @DisplayName("Should handle empty parameter sets")
        void shouldHandleEmptyParameterSets() {
            // Arrange
            setupBatchMocks();
            
            // Act
            String result = cacheService.putWithMultipleParameterSets("key", 
                Collections.emptyList(), testObject);
            
            // Assert
            assertEquals("key", result);
            verify(batch).execute();
        }

        @Test
        @DisplayName("Should use custom TTL")
        void shouldUseCustomTtl() {
            // Arrange
            long customTtl = 1800L;
            setupBatchMocks();
            
            // Act
            cacheService.putWithMultipleParameterSets("key", 
                Collections.singletonList(searchParameters), testObject, customTtl);
            
            // Assert
            verify(bucketAsync).setAsync(eq(testObject), eq(customTtl + 300), eq(TimeUnit.SECONDS));
        }

        @Test
        @DisplayName("Should handle single parameter set convenience method")
        void shouldHandleSingleParameterSetConvenienceMethod() {
            // Arrange
            setupBatchMocks();
            
            // Act
            String result = cacheService.putWithSingleParameterSet("key", searchParameters, testObject);
            
            // Assert
            assertEquals("key", result);
            verify(batch).execute();
        }

        private void setupBatchMocks() {
            when(batch.getBucket(anyString())).thenReturn(bucketAsync);
            when(batch.getMap(anyString())).thenReturn(mapAsync);
            when(batch.getSet(anyString())).thenReturn(setAsync);
            when(batch.getAtomicLong(anyString())).thenReturn(atomicLongAsync);
            
            when(bucketAsync.setAsync(any(), anyLong(), any(TimeUnit.class)))
                .thenReturn(mock(RFuture.class));
            when(mapAsync.putAsync(anyString(), anyString()))
                .thenReturn(mock(RFuture.class));
            when(mapAsync.expireAsync(anyLong(), any(TimeUnit.class)))
                .thenReturn(mock(RFuture.class));
            when(setAsync.addAllAsync(any(Collection.class)))
                .thenReturn(mock(RFuture.class));
            when(setAsync.expireAsync(anyLong(), any(TimeUnit.class)))
                .thenReturn(mock(RFuture.class));
            when(atomicLongAsync.incrementAndGetAsync())
                .thenReturn(mock(RFuture.class));
            when(atomicLongAsync.expireAsync(anyLong(), any(TimeUnit.class)))
                .thenReturn(mock(RFuture.class));
        }
    }

    @Nested
    @DisplayName("GET Operations")
    class GetOperationsTest {

        @Test
        @DisplayName("Should retrieve data by unique key")
        void shouldRetrieveDataByUniqueKey() {
            // Arrange
            String contentHash = "test-hash";
            when(redissonClient.getMap("test:key:unique-key")).thenReturn(map);
            when(map.get("content_hash")).thenReturn(contentHash);
            when(redissonClient.getBucket("test:data:" + contentHash)).thenReturn(bucket);
            when(bucket.get()).thenReturn(testObject);
            
            // Act
            Optional<TestObject> result = cacheService.getByKey("unique-key", TestObject.class);
            
            // Assert
            assertTrue(result.isPresent());
            assertEquals(testObject, result.get());
        }

        @Test
        @DisplayName("Should return empty for non-existent key")
        void shouldReturnEmptyForNonExistentKey() {
            // Arrange
            when(redissonClient.getMap("test:key:non-existent")).thenReturn(map);
            when(map.get("content_hash")).thenReturn(null);
            
            // Act
            Optional<TestObject> result = cacheService.getByKey("non-existent", TestObject.class);
            
            // Assert
            assertFalse(result.isPresent());
        }

        @Test
        @DisplayName("Should handle null unique key")
        void shouldHandleNullUniqueKey() {
            // Act
            Optional<TestObject> result = cacheService.getByKey(null, TestObject.class);
            
            // Assert
            assertFalse(result.isPresent());
        }

        @Test
        @DisplayName("Should retrieve data by hierarchical search")
        void shouldRetrieveDataByHierarchicalSearch() {
            // Arrange
            String contentHash = "test-hash";
            String refKey = "test:ref:L0:region=US|L1:category=electronics|L2:brand=apple";
            
            when(redissonClient.getMap(refKey)).thenReturn(map);
            when(map.get("content_hash")).thenReturn(contentHash);
            when(redissonClient.getBucket("test:data:" + contentHash)).thenReturn(bucket);
            when(bucket.get()).thenReturn(testObject);
            
            // Act
            Optional<TestObject> result = cacheService.getByHierarchicalSearch(searchParameters, TestObject.class);
            
            // Assert
            assertTrue(result.isPresent());
            assertEquals(testObject, result.get());
        }

        @Test
        @DisplayName("Should return empty for hierarchical search miss")
        void shouldReturnEmptyForHierarchicalSearchMiss() {
            // Arrange
            when(redissonClient.getMap(anyString())).thenReturn(map);
            when(map.get("content_hash")).thenReturn(null);
            
            // Act
            Optional<TestObject> result = cacheService.getByHierarchicalSearch(searchParameters, TestObject.class);
            
            // Assert
            assertFalse(result.isPresent());
        }

        @Test
        @DisplayName("Should handle null or empty search parameters")
        void shouldHandleNullOrEmptySearchParameters() {
            // Act & Assert
            assertFalse(cacheService.getByHierarchicalSearch(null, TestObject.class).isPresent());
            assertFalse(cacheService.getByHierarchicalSearch(Collections.emptyList(), TestObject.class).isPresent());
        }

        @Test
        @DisplayName("Should retrieve all matching data by hierarchical search")
        void shouldRetrieveAllMatchingDataByHierarchicalSearch() {
            // Arrange
            String contentHash1 = "hash1";
            String contentHash2 = "hash2";
            TestObject obj1 = new TestObject("1", "Name1", "Value1");
            TestObject obj2 = new TestObject("2", "Name2", "Value2");
            
            List<String> matchingKeys = Arrays.asList("key1", "key2");
            when(keys.getKeysByPattern(anyString())).thenReturn(matchingKeys);
            
            // Mock first key
            RMap<String, String> map1 = mock(RMap.class);
            when(redissonClient.getMap("key1")).thenReturn(map1);
            when(map1.get("content_hash")).thenReturn(contentHash1);
            
            RBucket<TestObject> bucket1 = mock(RBucket.class);
            when(redissonClient.getBucket("test:data:" + contentHash1)).thenReturn(bucket1);
            when(bucket1.get()).thenReturn(obj1);
            
            // Mock second key
            RMap<String, String> map2 = mock(RMap.class);
            when(redissonClient.getMap("key2")).thenReturn(map2);
            when(map2.get("content_hash")).thenReturn(contentHash2);
            
            RBucket<TestObject> bucket2 = mock(RBucket.class);
            when(redissonClient.getBucket("test:data:" + contentHash2)).thenReturn(bucket2);
            when(bucket2.get()).thenReturn(obj2);
            
            // Act
            List<TestObject> results = cacheService.getAllByHierarchicalSearch(searchParameters, TestObject.class);
            
            // Assert
            assertEquals(2, results.size());
            assertTrue(results.contains(obj1));
            assertTrue(results.contains(obj2));
        }
    }

    @Nested
    @DisplayName("GET OR COMPUTE Operations")
    class GetOrComputeOperationsTest {

        @Test
        @DisplayName("Should return cached value when available")
        void shouldReturnCachedValueWhenAvailable() {
            // Arrange
            String contentHash = "test-hash";
            when(redissonClient.getMap("test:key:unique-key")).thenReturn(map);
            when(map.get("content_hash")).thenReturn(contentHash);
            when(redissonClient.getBucket("test:data:" + contentHash)).thenReturn(bucket);
            when(bucket.get()).thenReturn(testObject);
            
            // Act
            TestObject result = cacheService.getOrComputeWithMultipleParameterSets(
                "unique-key",
                Collections.singletonList(searchParameters),
                TestObject.class,
                () -> new TestObject("computed", "Computed", "Should not be called")
            );
            
            // Assert
            assertEquals(testObject, result);
        }

        @Test
        @DisplayName("Should compute and cache when not available")
        void shouldComputeAndCacheWhenNotAvailable() {
            // Arrange
            TestObject computedObject = new TestObject("computed", "Computed", "Computed Value");
            when(redissonClient.getMap(anyString())).thenReturn(map);
            when(map.get("content_hash")).thenReturn(null);
            
            setupBatchMocks();
            
            // Act
            TestObject result = cacheService.getOrComputeWithMultipleParameterSets(
                "unique-key",
                Collections.singletonList(searchParameters),
                TestObject.class,
                () -> computedObject
            );
            
            // Assert
            assertEquals(computedObject, result);
            verify(batch).execute(); // Verify caching occurred
        }

        @Test
        @DisplayName("Should handle supplier returning null")
        void shouldHandleSupplierReturningNull() {
            // Arrange
            when(redissonClient.getMap(anyString())).thenReturn(map);
            when(map.get("content_hash")).thenReturn(null);
            
            // Act
            TestObject result = cacheService.getOrComputeWithMultipleParameterSets(
                "unique-key",
                Collections.singletonList(searchParameters),
                TestObject.class,
                () -> null
            );
            
            // Assert
            assertNull(result);
            verify(batch, never()).execute(); // Should not cache null
        }

        @Test
        @DisplayName("Should handle single parameter set convenience method")
        void shouldHandleSingleParameterSetGetOrCompute() {
            // Arrange
            when(redissonClient.getMap(anyString())).thenReturn(map);
            when(map.get("content_hash")).thenReturn(null);
            
            TestObject computedObject = new TestObject("computed", "Computed", "Value");
            setupBatchMocks();
            
            // Act
            TestObject result = cacheService.getOrComputeWithSingleParameterSet(
                "key", searchParameters, TestObject.class, () -> computedObject
            );
            
            // Assert
            assertEquals(computedObject, result);
        }

        private void setupBatchMocks() {
            when(redissonClient.createBatch()).thenReturn(batch);
            when(batch.getBucket(anyString())).thenReturn(bucketAsync);
            when(batch.getMap(anyString())).thenReturn(mapAsync);
            when(batch.getSet(anyString())).thenReturn(setAsync);
            when(batch.getAtomicLong(anyString())).thenReturn(atomicLongAsync);
            
            when(bucketAsync.setAsync(any(), anyLong(), any(TimeUnit.class)))
                .thenReturn(mock(RFuture.class));
            when(mapAsync.putAsync(anyString(), anyString()))
                .thenReturn(mock(RFuture.class));
            when(mapAsync.expireAsync(anyLong(), any(TimeUnit.class)))
                .thenReturn(mock(RFuture.class));
            when(setAsync.addAllAsync(any(Collection.class)))
                .thenReturn(mock(RFuture.class));
            when(setAsync.expireAsync(anyLong(), any(TimeUnit.class)))
                .thenReturn(mock(RFuture.class));
            when(atomicLongAsync.incrementAndGetAsync())
                .thenReturn(mock(RFuture.class));
            when(atomicLongAsync.expireAsync(anyLong(), any(TimeUnit.class)))
                .thenReturn(mock(RFuture.class));
        }
    }

    @Nested
    @DisplayName("INVALIDATION Operations")
    class InvalidationOperationsTest {

        @Test
        @DisplayName("Should invalidate by unique key")
        void shouldInvalidateByUniqueKey() {
            // Arrange
            String contentHash = "test-hash";
            when(redissonClient.getMap("test:key:unique-key")).thenReturn(map);
            when(map.get("content_hash")).thenReturn(contentHash);
            when(map.delete()).thenReturn(true);
            
            // Mock reference set cleanup
            when(redissonClient.getSet("test:refset:" + contentHash)).thenReturn(set);
            when(set.isExists()).thenReturn(true);
            when(set.readAll()).thenReturn(Collections.singleton("ref-key"));
            
            RMap<String, String> refMap = mock(RMap.class);
            when(redissonClient.getMap("test:ref:ref-key")).thenReturn(refMap);
            when(refMap.get("unique_key")).thenReturn("unique-key");
            when(refMap.delete()).thenReturn(true);
            
            // Mock reference count
            when(redissonClient.getAtomicLong("test:refcount:" + contentHash)).thenReturn(atomicLong);
            when(atomicLong.decrementAndGet()).thenReturn(0L);
            
            // Mock batch for cleanup
            when(redissonClient.createBatch()).thenReturn(batch);
            when(batch.getBucket(anyString())).thenReturn(bucketAsync);
            when(batch.getAtomicLong(anyString())).thenReturn(atomicLongAsync);
            when(batch.getSet(anyString())).thenReturn(setAsync);
            
            // Act
            cacheService.invalidateByKey("unique-key");
            
            // Assert
            verify(map).delete();
            verify(batch).execute();
        }

        @Test
        @DisplayName("Should handle null key in invalidation")
        void shouldHandleNullKeyInInvalidation() {
            // Act & Assert - should not throw exception
            assertDoesNotThrow(() -> cacheService.invalidateByKey(null));
        }

        @Test
        @DisplayName("Should invalidate by hierarchical search")
        void shouldInvalidateByHierarchicalSearch() {
            // Arrange
            when(redissonClient.getMap(anyString())).thenReturn(map);
            when(map.delete()).thenReturn(true);
            
            // Act
            cacheService.invalidateByHierarchicalSearch(searchParameters);
            
            // Assert
            verify(map, atLeastOnce()).delete();
        }

        @Test
        @DisplayName("Should handle null parameters in hierarchical invalidation")
        void shouldHandleNullParametersInHierarchicalInvalidation() {
            // Act & Assert - should not throw exception
            assertDoesNotThrow(() -> cacheService.invalidateByHierarchicalSearch(null));
            assertDoesNotThrow(() -> cacheService.invalidateByHierarchicalSearch(Collections.emptyList()));
        }

        @Test
        @DisplayName("Should invalidate with cleanup")
        void shouldInvalidateWithCleanup() {
            // Arrange
            String contentHash = "test-hash";
            when(redissonClient.getMap(anyString())).thenReturn(map);
            when(map.get("content_hash")).thenReturn(contentHash);
            when(map.delete()).thenReturn(true);
            
            // Mock reference count
            when(redissonClient.getAtomicLong("test:refcount:" + contentHash)).thenReturn(atomicLong);
            when(atomicLong.decrementAndGet()).thenReturn(0L);
            
            // Mock batch for cleanup
            when(redissonClient.createBatch()).thenReturn(batch);
            when(batch.getBucket(anyString())).thenReturn(bucketAsync);
            when(batch.getAtomicLong(anyString())).thenReturn(atomicLongAsync);
            when(batch.getSet(anyString())).thenReturn(setAsync);
            
            // Act
            cacheService.invalidateWithCleanup(searchParameters);
            
            // Assert
            verify(map, atLeastOnce()).delete();
            verify(batch).execute();
        }
    }

    @Nested
    @DisplayName("UTILITY Operations")
    class UtilityOperationsTest {

        @Test
        @DisplayName("Should clear all cache entries")
        void shouldClearAllCacheEntries() {
            // Act
            cacheService.clearAll();
            
            // Assert
            verify(keys).deleteByPattern("test:*");
        }

        @Test
        @DisplayName("Should get cache statistics")
        void shouldGetCacheStatistics() {
            // Arrange
            when(keys.countByPattern("test:key:*")).thenReturn(10L);
            when(keys.countByPattern("test:ref:*")).thenReturn(25L);
            when(keys.countByPattern("test:data:*")).thenReturn(8L);
            when(keys.countByPattern("test:refset:*")).thenReturn(8L);
            
            // Act
            KryoHierarchicalCacheService.CacheStats stats = cacheService.getStats();
            
            // Assert
            assertEquals(10L, stats.getKeyCount());
            assertEquals(25L, stats.getReferenceCount());
            assertEquals(8L, stats.getDataCount());
            assertEquals(8L, stats.getRefSetCount());
            assertEquals(35L, stats.getTotalReferences()); // keys + hierarchical refs
            assertTrue(stats.getCompressionRatio() > 0);
        }

        @Test
        @DisplayName("Should handle errors in statistics gracefully")
        void shouldHandleErrorsInStatisticsGracefully() {
            // Arrange
            when(keys.countByPattern(anyString())).thenThrow(new RuntimeException("Redis error"));
            
            // Act
            KryoHierarchicalCacheService.CacheStats stats = cacheService.getStats();
            
            // Assert
            assertEquals(0L, stats.getKeyCount());
            assertEquals(0L, stats.getReferenceCount());
            assertEquals(0L, stats.getDataCount());
            assertEquals(0L, stats.getRefSetCount());
        }
    }

    @Nested
    @DisplayName("ERROR HANDLING")
    class ErrorHandlingTest {

        @Test
        @DisplayName("Should handle Redis connection errors gracefully in put")
        void shouldHandleRedisErrorsGracefullyInPut() {
            // Arrange
            when(redissonClient.createBatch()).thenThrow(new RuntimeException("Redis connection error"));
            
            // Act & Assert - should not throw exception
            assertDoesNotThrow(() -> {
                String result = cacheService.putWithMultipleParameterSets(
                    "key", Collections.singletonList(searchParameters), testObject
                );
                assertEquals("key", result); // Should return the key even on error
            });
        }

        @Test
        @DisplayName("Should handle Redis connection errors gracefully in get")
        void shouldHandleRedisErrorsGracefullyInGet() {
            // Arrange
            when(redissonClient.getMap(anyString())).thenThrow(new RuntimeException("Redis connection error"));
            
            // Act
            Optional<TestObject> result = cacheService.getByKey("key", TestObject.class);
            
            // Assert
            assertFalse(result.isPresent());
        }

        @Test
        @DisplayName("Should handle errors in getOrCompute by falling back to supplier")
        void shouldHandleErrorsInGetOrComputeByFallingBackToSupplier() {
            // Arrange
            TestObject fallbackObject = new TestObject("fallback", "Fallback", "Value");
            when(redissonClient.getMap(anyString())).thenThrow(new RuntimeException("Redis error"));
            
            // Act
            TestObject result = cacheService.getOrComputeWithSingleParameterSet(
                "key", searchParameters, TestObject.class, () -> fallbackObject
            );
            
            // Assert
            assertEquals(fallbackObject, result);
        }
    }

    // Test helper class
    private static class TestObject {
        private String id;
        private String name;
        private String value;

        public TestObject() {}

        public TestObject(String id, String name, String value) {
            this.id = id;
            this.name = name;
            this.value = value;
        }

        // Getters and setters
        public String getId() { return id; }
        public void setId(String id) { this.id = id; }
        public String getName() { return name; }
        public void setName(String name) { this.name = name; }
        public String getValue() { return value; }
        public void setValue(String value) { this.value = value; }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            TestObject that = (TestObject) o;
            return Objects.equals(id, that.id) &&
                   Objects.equals(name, that.name) &&
                   Objects.equals(value, that.value);
        }

        @Override
        public int hashCode() {
            return Objects.hash(id, name, value);
        }

        @Override
        public String toString() {
            return String.format("TestObject{id='%s', name='%s', value='%s'}", id, name, value);
        }
    }
}
