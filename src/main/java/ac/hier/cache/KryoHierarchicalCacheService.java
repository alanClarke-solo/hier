package ac.hier.cache;

import org.redisson.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.security.MessageDigest;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * Enhanced Kryo-based hierarchical cache service with multiple reference support
 * Uses Redisson's built-in Kryo5Codec for efficient binary serialization
 * Supports multiple hierarchical references per data item for complex search patterns
 *
 * Features:
 * - Data deduplication using content hashing
 * - Atomic batch operations to prevent race conditions
 * - Comprehensive error handling without throwing runtime exceptions
 * - Reference counting for proper cleanup
 * - Multiple search parameter set support
 */
public class KryoHierarchicalCacheService {
    private static final Logger logger = LoggerFactory.getLogger(KryoHierarchicalCacheService.class);

    private final RedissonClient redissonClient;
    private final String cachePrefix;
    private final String dataPrefix;
    private final String refPrefix;
    private final String keyPrefix;
    private final String refSetPrefix;
    private final String refCountPrefix;
    private final long defaultTtlSeconds;

    public KryoHierarchicalCacheService(RedissonClient redissonClient,
                                        String cachePrefix,
                                        long defaultTtlSeconds) {
        this.redissonClient = redissonClient;
        this.cachePrefix = cachePrefix + ":";
        this.dataPrefix = this.cachePrefix + "data:";
        this.refPrefix = this.cachePrefix + "ref:";
        this.keyPrefix = this.cachePrefix + "key:";
        this.refSetPrefix = this.cachePrefix + "refset:";
        this.refCountPrefix = this.cachePrefix + "refcount:";
        this.defaultTtlSeconds = defaultTtlSeconds;
    }

    // PUT methods - using different method names to avoid erasure conflicts

    /**
     * Stores data with String key and multiple hierarchical search parameter sets
     */
    public <T> String putWithMultipleParameterSets(String uniqueKey, List<List<SearchParameter>> searchParameterSets, T value) {
        return putWithMultipleParameterSets(uniqueKey, searchParameterSets, value, defaultTtlSeconds);
    }

    /**
     * Stores data with String key and multiple hierarchical search parameter sets with custom TTL
     */
    public <T> String putWithMultipleParameterSets(String uniqueKey, List<List<SearchParameter>> searchParameterSets,
                                                   T value, long ttlSeconds) {
        if (value == null) {
            logger.warn("Attempted to cache null value for key: {} and parameter sets: {}", uniqueKey, searchParameterSets);
            return uniqueKey != null ? uniqueKey : generateUniqueKey();
        }

        if (uniqueKey == null) {
            uniqueKey = generateUniqueKey();
        }

        final String finalUniqueKey = uniqueKey;

        try {
            // Generate content hash for deduplication
            String contentHash = generateContentHash(value);

            // Use batch operations for atomicity
            RBatch batch = redissonClient.createBatch();

            // Store or update the actual data
            String dataKey = dataPrefix + contentHash;
            RBucketAsync<T> dataBucket = batch.getBucket(dataKey);
            dataBucket.setAsync(value, ttlSeconds + 300, TimeUnit.SECONDS);

            // Increment reference count
            String refCountKey = refCountPrefix + contentHash;
            batch.getAtomicLong(refCountKey).incrementAndGetAsync();
            batch.getAtomicLong(refCountKey).expireAsync(ttlSeconds + 300, TimeUnit.SECONDS);

            // Create direct key-based access reference
            String keyRefKey = keyPrefix + finalUniqueKey;
            RMapAsync<Object, Object> keyMap = batch.getMap(keyRefKey);
            keyMap.putAsync("content_hash", contentHash);
            keyMap.putAsync("created_at", String.valueOf(System.currentTimeMillis()));
            keyMap.expireAsync(ttlSeconds, TimeUnit.SECONDS);

            // Create hierarchical references for all parameter sets
            Set<String> allReferenceKeys = new HashSet<>();
            if (searchParameterSets != null && !searchParameterSets.isEmpty()) {
                for (List<SearchParameter> searchParameters : searchParameterSets) {
                    if (searchParameters != null && !searchParameters.isEmpty()) {
                        CacheKey cacheKey = new CacheKey(searchParameters);
                        List<CacheKey> hierarchicalKeys = cacheKey.getHierarchicalKeys();

                        for (CacheKey key : hierarchicalKeys) {
                            String refKeyString = key.getKeyString();
                            allReferenceKeys.add(refKeyString);
                            createHierarchicalReferenceInBatch(batch, key, contentHash, finalUniqueKey, ttlSeconds);
                        }
                    }
                }

                // Store the set of all reference keys for this content hash
                if (!allReferenceKeys.isEmpty()) {
                    String refSetKey = refSetPrefix + contentHash;
                    RSetAsync<Object> refSet = batch.getSet(refSetKey);
                    refSet.addAllAsync(allReferenceKeys);
                    refSet.expireAsync(ttlSeconds + 300, TimeUnit.SECONDS);
                }
            }

            // Execute batch atomically
            batch.execute();

            logger.debug("Successfully cached data with unique key: {} and content hash: {}", finalUniqueKey, contentHash);
            return finalUniqueKey;

        } catch (Exception e) {
            logger.error("Error storing to cache for key: {} and parameter sets: {}", finalUniqueKey, searchParameterSets, e);
            return finalUniqueKey; // Don't throw exception, return key for graceful degradation
        }
    }

    /**
     * Convenience method for single parameter set
     */
    public <T> String putWithSingleParameterSet(String uniqueKey, List<SearchParameter> searchParameters, T value) {
        return putWithMultipleParameterSets(uniqueKey,
                searchParameters != null ? Collections.singletonList(searchParameters) : null, value);
    }

    /**
     * Convenience method for single parameter set with custom TTL
     */
    public <T> String putWithSingleParameterSet(String uniqueKey, List<SearchParameter> searchParameters,
                                                T value, long ttlSeconds) {
        return putWithMultipleParameterSets(uniqueKey,
                searchParameters != null ? Collections.singletonList(searchParameters) : null, value, ttlSeconds);
    }

    // GET methods

    /**
     * Retrieves data by unique String key (most specific access)
     */
    public <T> Optional<T> getByKey(String uniqueKey, Class<T> valueType) {
        if (uniqueKey == null) {
            return Optional.empty();
        }

        try {
            String keyRefKey = keyPrefix + uniqueKey;
            RMap<String, String> keyMap = redissonClient.getMap(keyRefKey);
            String contentHash = keyMap.get("content_hash");

            if (contentHash != null) {
                Optional<T> result = getDataByContentHash(contentHash, valueType);
                if (result.isPresent()) {
                    logger.debug("Cache hit for unique key: {}", uniqueKey);
                    return result;
                } else {
                    // Clean up stale key reference
                    cleanupStaleKeyReference(keyRefKey, contentHash);
                }
            }

            logger.debug("Cache miss for unique key: {}", uniqueKey);
            return Optional.empty();

        } catch (Exception e) {
            logger.error("Error retrieving from cache for key: {}", uniqueKey, e);
            return Optional.empty();
        }
    }

    /**
     * Retrieves data using hierarchical search parameters
     * Returns the first match found, searching from most specific to least specific
     */
    public <T> Optional<T> getByHierarchicalSearch(List<SearchParameter> searchParameters, Class<T> valueType) {
        if (searchParameters == null || searchParameters.isEmpty()) {
            return Optional.empty();
        }

        try {
            CacheKey cacheKey = new CacheKey(searchParameters);
            List<CacheKey> hierarchicalKeys = cacheKey.getHierarchicalKeys();

            // Search from most specific to least specific
            for (int i = hierarchicalKeys.size() - 1; i >= 0; i--) {
                CacheKey key = hierarchicalKeys.get(i);
                Optional<T> result = getFromCacheByHierarchicalReference(key, valueType);
                if (result.isPresent()) {
                    logger.debug("Cache hit for hierarchical key: {}", key);
                    return result;
                }
            }

            logger.debug("Cache miss for all hierarchical keys of: {}", cacheKey);
            return Optional.empty();

        } catch (Exception e) {
            logger.error("Error retrieving from cache for hierarchical search: {}", searchParameters, e);
            return Optional.empty();
        }
    }

    /**
     * Retrieves all data matching hierarchical search parameters
     * Returns all items that match at any hierarchical level
     */
    public <T> List<T> getAllByHierarchicalSearch(List<SearchParameter> searchParameters, Class<T> valueType) {
        if (searchParameters == null || searchParameters.isEmpty()) {
            return Collections.emptyList();
        }

        try {
            Set<String> seenContentHashes = new HashSet<>();
            List<T> results = new ArrayList<>();

            CacheKey cacheKey = new CacheKey(searchParameters);
            List<CacheKey> hierarchicalKeys = cacheKey.getHierarchicalKeys();

            // Search all hierarchical levels and collect unique results
            for (CacheKey key : hierarchicalKeys) {
                List<T> levelResults = getAllFromCacheByHierarchicalReference(key, valueType, seenContentHashes);
                results.addAll(levelResults);
            }

            logger.debug("Found {} unique results for hierarchical search: {}", results.size(), cacheKey);
            return results;

        } catch (Exception e) {
            logger.error("Error retrieving all results for hierarchical search: {}", searchParameters, e);
            return Collections.emptyList();
        }
    }

    /**
     * Finds all items that match the given search parameters at any level
     * Returns a map of unique keys to their corresponding data
     */
    public <T> Map<String, T> findByPattern(List<SearchParameter> searchParameters, Class<T> valueType) {
        if (searchParameters == null || searchParameters.isEmpty()) {
            return Collections.emptyMap();
        }

        try {
            Map<String, T> results = new HashMap<>();
            Set<String> seenContentHashes = new HashSet<>();

            CacheKey cacheKey = new CacheKey(searchParameters);
            List<CacheKey> hierarchicalKeys = cacheKey.getHierarchicalKeys();

            for (CacheKey key : hierarchicalKeys) {
                String refKey = refPrefix + key.getKeyString();
                RMap<String, String> refMap = redissonClient.getMap(refKey);

                if (refMap.isExists()) {
                    String contentHash = refMap.get("content_hash");
                    String uniqueKey = refMap.get("unique_key");

                    if (contentHash != null && !seenContentHashes.contains(contentHash)) {
                        Optional<T> data = getDataByContentHash(contentHash, valueType);
                        if (data.isPresent()) {
                            results.put(uniqueKey != null ? uniqueKey : contentHash, data.get());
                            seenContentHashes.add(contentHash);
                        }
                    }
                }
            }

            logger.debug("Pattern search found {} unique results for: {}", results.size(), cacheKey);
            return results;

        } catch (Exception e) {
            logger.error("Error in pattern search for: {}", searchParameters, e);
            return Collections.emptyMap();
        }
    }

    // GET OR COMPUTE methods - using different method names to avoid erasure conflicts

    /**
     * Gets data from cache or computes it with String key support for multiple parameter sets
     */
    public <T> T getOrComputeWithMultipleParameterSets(String uniqueKey, List<List<SearchParameter>> searchParameterSets,
                                                       Class<T> valueType, Supplier<T> dataSupplier) {
        return getOrComputeWithMultipleParameterSets(uniqueKey, searchParameterSets, valueType, dataSupplier, defaultTtlSeconds);
    }

    /**
     * Gets data from cache or computes it with String key support and custom TTL for multiple parameter sets
     */
    public <T> T getOrComputeWithMultipleParameterSets(String uniqueKey, List<List<SearchParameter>> searchParameterSets,
                                                       Class<T> valueType, Supplier<T> dataSupplier, long ttlSeconds) {
        try {
            // Try unique key first if provided
            if (uniqueKey != null) {
                Optional<T> keyResult = getByKey(uniqueKey, valueType);
                if (keyResult.isPresent()) {
                    return keyResult.get();
                }
            }

            // Try hierarchical search on all parameter sets
            if (searchParameterSets != null) {
                for (List<SearchParameter> searchParameters : searchParameterSets) {
                    Optional<T> hierarchicalResult = getByHierarchicalSearch(searchParameters, valueType);
                    if (hierarchicalResult.isPresent()) {
                        return hierarchicalResult.get();
                    }
                }
            }

            // Compute new value
            T computedValue = dataSupplier.get();
            if (computedValue != null) {
                putWithMultipleParameterSets(uniqueKey, searchParameterSets, computedValue, ttlSeconds);
            }

            return computedValue;

        } catch (Exception e) {
            logger.error("Error in getOrCompute for key: {} and parameter sets: {}", uniqueKey, searchParameterSets, e);
            // Fallback to supplier
            return dataSupplier.get();
        }
    }

    /**
     * Convenience method for single parameter set
     */
    public <T> T getOrComputeWithSingleParameterSet(String uniqueKey, List<SearchParameter> searchParameters,
                                                    Class<T> valueType, Supplier<T> dataSupplier) {
        return getOrComputeWithMultipleParameterSets(uniqueKey,
                searchParameters != null ? Collections.singletonList(searchParameters) : null,
                valueType, dataSupplier, defaultTtlSeconds);
    }

    /**
     * Convenience method for single parameter set with custom TTL
     */
    public <T> T getOrComputeWithSingleParameterSet(String uniqueKey, List<SearchParameter> searchParameters,
                                                    Class<T> valueType, Supplier<T> dataSupplier, long ttlSeconds) {
        return getOrComputeWithMultipleParameterSets(uniqueKey,
                searchParameters != null ? Collections.singletonList(searchParameters) : null,
                valueType, dataSupplier, ttlSeconds);
    }

    // INVALIDATION methods

    /**
     * Invalidates cache by unique String key and cleans up all associated references
     */
    public void invalidateByKey(String uniqueKey) {
        if (uniqueKey == null) return;

        try {
            String keyRefKey = keyPrefix + uniqueKey;
            RMap<String, String> keyMap = redissonClient.getMap(keyRefKey);
            String contentHash = keyMap.get("content_hash");

            // Remove key reference
            keyMap.delete();
            logger.debug("Invalidated key reference: {}", uniqueKey);

            // Clean up all hierarchical references for this unique key and decrement reference count
            if (contentHash != null) {
                invalidateAllReferencesForContentHash(contentHash, uniqueKey);
                decrementReferenceCountAndCleanup(contentHash);
            }

        } catch (Exception e) {
            logger.error("Error invalidating key: {}", uniqueKey, e);
        }
    }

    /**
     * Invalidates cache by hierarchical parameters - removes only specific hierarchical references
     */
    public void invalidateByHierarchicalSearch(List<SearchParameter> searchParameters) {
        if (searchParameters == null || searchParameters.isEmpty()) return;

        try {
            CacheKey cacheKey = new CacheKey(searchParameters);
            List<CacheKey> hierarchicalKeys = cacheKey.getHierarchicalKeys();

            for (CacheKey key : hierarchicalKeys) {
                String refKey = refPrefix + key.getKeyString();
                redissonClient.getMap(refKey).delete();
                logger.debug("Invalidated hierarchical reference for key: {}", key);
            }

        } catch (Exception e) {
            logger.error("Error invalidating hierarchical search: {}", searchParameters, e);
        }
    }

    /**
     * Invalidates with cleanup - removes references and cleans unreferenced data
     */
    public void invalidateWithCleanup(List<SearchParameter> searchParameters) {
        if (searchParameters == null || searchParameters.isEmpty()) return;

        try {
            CacheKey cacheKey = new CacheKey(searchParameters);
            List<CacheKey> hierarchicalKeys = cacheKey.getHierarchicalKeys();
            Set<String> contentHashesToCheck = new HashSet<>();

            for (CacheKey key : hierarchicalKeys) {
                String refKey = refPrefix + key.getKeyString();
                RMap<String, String> refMap = redissonClient.getMap(refKey);
                String contentHash = refMap.get("content_hash");

                // Remove reference
                refMap.delete();
                logger.debug("Invalidated hierarchical reference for key: {}", key);

                if (contentHash != null) {
                    contentHashesToCheck.add(contentHash);
                }
            }

            // Clean up data if no other references exist
            for (String contentHash : contentHashesToCheck) {
                decrementReferenceCountAndCleanup(contentHash);
            }

        } catch (Exception e) {
            logger.error("Error invalidating with cleanup: {}", searchParameters, e);
        }
    }

    /**
     * Clears all cache entries
     */
    public void clearAll() {
        try {
            redissonClient.getKeys().deleteByPattern(cachePrefix + "*");
            logger.info("Cleared all cache entries with prefix: {}", cachePrefix);
        } catch (Exception e) {
            logger.error("Error clearing all cache entries", e);
        }
    }

    /**
     * Gets enhanced cache statistics
     */
    public CacheStats getStats() {
        try {
            long keyCount = redissonClient.getKeys().countByPattern(keyPrefix + "*");
            long referenceCount = redissonClient.getKeys().countByPattern(refPrefix + "*");
            long dataCount = redissonClient.getKeys().countByPattern(dataPrefix + "*");
            long refSetCount = redissonClient.getKeys().countByPattern(refSetPrefix + "*");
            return new CacheStats(keyCount, referenceCount, dataCount, refSetCount);
        } catch (Exception e) {
            logger.error("Error getting cache statistics", e);
            return new CacheStats(0, 0, 0, 0);
        }
    }

    // Private helper methods

    private <T> Optional<T> getDataByContentHash(String contentHash, Class<T> valueType) {
        try {
            String dataKey = dataPrefix + contentHash;
            RBucket<T> dataBucket = redissonClient.getBucket(dataKey);
            T data = dataBucket.get();
            return Optional.ofNullable(data);
        } catch (Exception e) {
            logger.error("Error retrieving data for content hash: {}", contentHash, e);
            return Optional.empty();
        }
    }

    private <T> Optional<T> getFromCacheByHierarchicalReference(CacheKey cacheKey, Class<T> valueType) {
        try {
            String refKey = refPrefix + cacheKey.getKeyString();
            RMap<String, String> refMap = redissonClient.getMap(refKey);
            String contentHash = refMap.get("content_hash");

            if (contentHash != null) {
                Optional<T> result = getDataByContentHash(contentHash, valueType);
                if (result.isPresent()) {
                    return result;
                } else {
                    // Clean up stale reference
                    cleanupStaleHierarchicalReference(refKey, contentHash);
                }
            }
        } catch (Exception e) {
            logger.error("Error retrieving from cache for hierarchical key: {}", cacheKey, e);
        }

        return Optional.empty();
    }

    private <T> List<T> getAllFromCacheByHierarchicalReference(CacheKey cacheKey, Class<T> valueType,
                                                               Set<String> seenContentHashes) {
        List<T> results = new ArrayList<>();

        try {
            // Find all references that match this hierarchical pattern
            String pattern = refPrefix + cacheKey.getKeyString() + "*";
            Iterable<String> matchingKeys = redissonClient.getKeys().getKeysByPattern(pattern);

            for (String refKey : matchingKeys) {
                RMap<String, String> refMap = redissonClient.getMap(refKey);
                String contentHash = refMap.get("content_hash");

                if (contentHash != null && !seenContentHashes.contains(contentHash)) {
                    Optional<T> data = getDataByContentHash(contentHash, valueType);
                    if (data.isPresent()) {
                        results.add(data.get());
                        seenContentHashes.add(contentHash);
                    }
                }
            }
        } catch (Exception e) {
            logger.error("Error retrieving all results for hierarchical key: {}", cacheKey, e);
        }

        return results;
    }

    private void createHierarchicalReferenceInBatch(RBatch batch, CacheKey cacheKey, String contentHash,
                                                    String uniqueKey, long ttlSeconds) {
        String refKey = refPrefix + cacheKey.getKeyString();
        RMapAsync<Object, Object> refMap = batch.getMap(refKey);

        // Store all data first
        refMap.putAsync("content_hash", contentHash);
        refMap.putAsync("unique_key", uniqueKey);
        refMap.putAsync("created_at", String.valueOf(System.currentTimeMillis()));

        // Set expiration with error handling
        RFuture<Boolean> expireFuture = refMap.expireAsync(ttlSeconds, TimeUnit.SECONDS);
        expireFuture.whenComplete((success, throwable) -> {
            if (throwable != null) {
                logger.error("Failed to set expiration for reference key: {}", refKey, throwable);
            } else if (!success) {
                logger.warn("Failed to set expiration for reference key: {} - key might not exist", refKey);
            } else {
                logger.debug("Successfully set expiration for reference key: {}", refKey);
            }
        });

    }

    private void invalidateAllReferencesForContentHash(String contentHash, String uniqueKey) {
        try {
            String refSetKey = refSetPrefix + contentHash;
            RSet<String> refSet = redissonClient.getSet(refSetKey);

            if (refSet.isExists()) {
                Set<String> referenceKeys = refSet.readAll();
                List<String> keysToRemoveFromSet = new ArrayList<>();

                for (String refKeyString : referenceKeys) {
                    String refKey = refPrefix + refKeyString;
                    RMap<String, String> refMap = redissonClient.getMap(refKey);
                    String refUniqueKey = refMap.get("unique_key");

                    // Only remove references that belong to this unique key
                    if (uniqueKey.equals(refUniqueKey)) {
                        refMap.delete();
                        keysToRemoveFromSet.add(refKeyString);
                        logger.debug("Removed hierarchical reference: {} for unique key: {}", refKeyString, uniqueKey);
                    }
                }

                // Remove keys from reference set
                if (!keysToRemoveFromSet.isEmpty()) {
                    refSet.removeAll(keysToRemoveFromSet);
                }

                // Clean up reference set if empty
                if (refSet.size() == 0) {
                    refSet.delete();
                }
            }
        } catch (Exception e) {
            logger.error("Error invalidating references for content hash: {} and unique key: {}", contentHash, uniqueKey, e);
        }
    }

    private void cleanupStaleKeyReference(String keyRefKey, String contentHash) {
        try {
            redissonClient.getMap(keyRefKey).delete();
            decrementReferenceCountAndCleanup(contentHash);
            logger.warn("Cleaned up stale key reference: {}", keyRefKey);
        } catch (Exception e) {
            logger.error("Error cleaning up stale key reference: {}", keyRefKey, e);
        }
    }

    private void cleanupStaleHierarchicalReference(String refKey, String contentHash) {
        try {
            redissonClient.getMap(refKey).delete();
            decrementReferenceCountAndCleanup(contentHash);
            logger.warn("Cleaned up stale hierarchical reference: {}", refKey);
        } catch (Exception e) {
            logger.error("Error cleaning up stale hierarchical reference: {}", refKey, e);
        }
    }

    private String generateUniqueKey() {
        return "key_" + System.currentTimeMillis() + "_" + UUID.randomUUID().toString().substring(0, 8);
    }

    private <T> String generateContentHash(T value) {
        try {
            // Create a more robust content hash using object's class and toString
            String content = value.getClass().getName() + ":" + value.toString();
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            byte[] hash = digest.digest(content.getBytes("UTF-8"));
            StringBuilder hexString = new StringBuilder();

            for (byte b : hash) {
                String hex = Integer.toHexString(0xff & b);
                if (hex.length() == 1) {
                    hexString.append('0');
                }
                hexString.append(hex);
            }

            return hexString.toString();
        } catch (Exception e) {
            logger.error("Error generating content hash", e);
            // Fallback to UUID-based hash for uniqueness
            return "fallback_" + UUID.randomUUID().toString().replace("-", "");
        }
    }

    private void decrementReferenceCountAndCleanup(String contentHash) {
        try {
            String refCountKey = refCountPrefix + contentHash;
            long remainingReferences = redissonClient.getAtomicLong(refCountKey).decrementAndGet();

            if (remainingReferences <= 0) {
                // Clean up data, reference count, and reference set
                String dataKey = dataPrefix + contentHash;
                String refSetKey = refSetPrefix + contentHash;

                RBatch batch = redissonClient.createBatch();
                batch.getBucket(dataKey).deleteAsync();
                batch.getAtomicLong(refCountKey).deleteAsync();
                batch.getSet(refSetKey).deleteAsync();
                batch.execute();

                logger.debug("Cleaned up unreferenced data and associated structures: {}", contentHash);
            }
        } catch (Exception e) {
            logger.error("Error during reference count cleanup for: {}", contentHash, e);
        }
    }

    /**
     * Enhanced cache statistics holder
     */
    public static class CacheStats {
        private final long keyCount;
        private final long referenceCount;
        private final long dataCount;
        private final long refSetCount;

        public CacheStats(long keyCount, long referenceCount, long dataCount, long refSetCount) {
            this.keyCount = keyCount;
            this.referenceCount = referenceCount;
            this.dataCount = dataCount;
            this.refSetCount = refSetCount;
        }

        public long getKeyCount() { return keyCount; }
        public long getReferenceCount() { return referenceCount; }
        public long getDataCount() { return dataCount; }
        public long getRefSetCount() { return refSetCount; }

        public long getTotalReferences() { return keyCount + referenceCount; }

        public double getCompressionRatio() {
            return dataCount == 0 ? 0 : (double) getTotalReferences() / dataCount;
        }

        public double getAverageReferencesPerData() {
            return dataCount == 0 ? 0 : (double) referenceCount / dataCount;
        }

        @Override
        public String toString() {
            return String.format("CacheStats{keys=%d, hierarchicalRefs=%d, actualData=%d, refSets=%d, " +
                            "totalRefs=%d, compressionRatio=%.2f, avgRefsPerData=%.2f}",
                    keyCount, referenceCount, dataCount, refSetCount, getTotalReferences(),
                    getCompressionRatio(), getAverageReferencesPerData());
        }
    }
}