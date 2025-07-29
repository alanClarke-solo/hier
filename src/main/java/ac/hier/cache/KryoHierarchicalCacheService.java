package ac.hier.cache;

import org.redisson.api.RBucket;
import org.redisson.api.RMap;
import org.redisson.api.RSet;
import org.redisson.api.RedissonClient;
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
 */
public class KryoHierarchicalCacheService {
    private static final Logger logger = LoggerFactory.getLogger(KryoHierarchicalCacheService.class);

    private final RedissonClient redissonClient;
    private final String cachePrefix;
    private final String dataPrefix;
    private final String refPrefix;
    private final String keyPrefix;
    private final String refSetPrefix;
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
        this.defaultTtlSeconds = defaultTtlSeconds;
    }

    /**
     * Stores data with String key and multiple hierarchical search parameter sets
     * @param uniqueKey Unique string identifier for direct access
     * @param searchParameterSets List of search parameter sets for different hierarchical access patterns
     * @param value Data to cache
     * @return The unique key used for storage
     */
    public <T> String put(String uniqueKey, List<List<SearchParameter>> searchParameterSets, T value) {
        return put(uniqueKey, searchParameterSets, value, defaultTtlSeconds);
    }

    /**
     * Stores data with String key and multiple hierarchical search parameter sets with custom TTL
     */
    public <T> String put(String uniqueKey, List<List<SearchParameter>> searchParameterSets, T value, long ttlSeconds) {
        if (value == null) {
            logger.warn("Attempted to cache null value for key: {} and parameter sets: {}", uniqueKey, searchParameterSets);
            return uniqueKey;
        }

        if (uniqueKey == null) {
            uniqueKey = generateUniqueKey();
        }

        try {
            // Generate content hash for deduplication
            String contentHash = generateContentHash(value);

            // Store the actual data using content hash (deduplication)
            String dataKey = dataPrefix + contentHash;
            RBucket<T> dataBucket = redissonClient.getBucket(dataKey);
            if (!dataBucket.isExists()) {
                dataBucket.set(value, ttlSeconds + 300, TimeUnit.SECONDS); // Extra TTL buffer
                logger.debug("Stored actual data for content hash: {}", contentHash);
            } else {
                // Extend TTL if data already exists
                dataBucket.expire(ttlSeconds + 300, TimeUnit.SECONDS);
                logger.debug("Extended TTL for existing data with content hash: {}", contentHash);
            }

            // Create direct key-based access reference
            String keyRefKey = keyPrefix + uniqueKey;
            RMap<String, String> keyMap = redissonClient.getMap(keyRefKey);
            keyMap.put("content_hash", contentHash);
            keyMap.put("created_at", String.valueOf(System.currentTimeMillis()));
            keyMap.expire(ttlSeconds, TimeUnit.SECONDS);
            logger.debug("Created key reference: {} -> {}", uniqueKey, contentHash);

            // Create hierarchical references for all parameter sets
            if (searchParameterSets != null && !searchParameterSets.isEmpty()) {
                Set<String> allReferenceKeys = new HashSet<>();

                for (List<SearchParameter> searchParameters : searchParameterSets) {
                    if (searchParameters != null && !searchParameters.isEmpty()) {
                        CacheKey cacheKey = new CacheKey(searchParameters);
                        List<CacheKey> hierarchicalKeys = cacheKey.getHierarchicalKeys();

                        for (CacheKey key : hierarchicalKeys) {
                            String refKeyString = key.getKeyString();
                            allReferenceKeys.add(refKeyString);
                            createHierarchicalReference(key, contentHash, uniqueKey, ttlSeconds);
                            logger.debug("Created hierarchical reference for key: {} -> {} (UniqueKey: {})",
                                    key, contentHash, uniqueKey);
                        }
                    }
                }

                // Store the set of all reference keys for this content hash (for cleanup purposes)
                if (!allReferenceKeys.isEmpty()) {
                    String refSetKey = refSetPrefix + contentHash;
                    RSet<String> refSet = redissonClient.getSet(refSetKey);
                    refSet.addAll(allReferenceKeys);
                    refSet.expire(ttlSeconds + 300, TimeUnit.SECONDS);
                    logger.debug("Stored reference set for content hash: {} with {} references",
                            contentHash, allReferenceKeys.size());
                }
            }

            return uniqueKey;

        } catch (Exception e) {
            logger.error("Error storing to cache for key: {} and parameter sets: {}", uniqueKey, searchParameterSets, e);
            throw new RuntimeException("Failed to store cache entry", e);
        }
    }

    /**
     * Convenience method for single parameter set
     */
    public <T> String put(String uniqueKey, List<SearchParameter> searchParameters, T value) {
        return put(uniqueKey, searchParameters != null ? Arrays.asList(searchParameters) : null, value);
    }

    /**
     * Convenience method for single parameter set with custom TTL
     */
    public <T> String put(String uniqueKey, List<SearchParameter> searchParameters, T value, long ttlSeconds) {
        return put(uniqueKey, searchParameters != null ? Arrays.asList(searchParameters) : null, value, ttlSeconds);
    }

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
                    keyMap.delete();
                    logger.warn("Found stale key reference: {}, cleaning up", uniqueKey);
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
     * Retrieves data using hierarchical search parameters with multiple result support
     * Returns the first match found, searching from most specific to least specific
     */
    public <T> Optional<T> get(List<SearchParameter> searchParameters, Class<T> valueType) {
        if (searchParameters == null || searchParameters.isEmpty()) {
            return Optional.empty();
        }

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
    }

    /**
     * Retrieves all data matching hierarchical search parameters
     * Returns all items that match at any hierarchical level
     */
    public <T> List<T> getAll(List<SearchParameter> searchParameters, Class<T> valueType) {
        if (searchParameters == null || searchParameters.isEmpty()) {
            return Collections.emptyList();
        }

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
    }

    /**
     * Finds all items that match the given search parameters at any level
     * Returns a map of cache keys to their corresponding data
     */
    public <T> Map<String, T> findByPattern(List<SearchParameter> searchParameters, Class<T> valueType) {
        if (searchParameters == null || searchParameters.isEmpty()) {
            return Collections.emptyMap();
        }

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
    }

    /**
     * Gets data from cache or computes it with String key support for multiple parameter sets
     */
    public <T> T getOrCompute(String uniqueKey, List<List<SearchParameter>> searchParameterSets,
                              Class<T> valueType, Supplier<T> dataSupplier) {
        return getOrCompute(uniqueKey, searchParameterSets, valueType, dataSupplier, defaultTtlSeconds);
    }

    /**
     * Gets data from cache or computes it with String key support and custom TTL for multiple parameter sets
     */
    public <T> T getOrCompute(String uniqueKey, List<List<SearchParameter>> searchParameterSets,
                              Class<T> valueType, Supplier<T> dataSupplier, long ttlSeconds) {
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
                Optional<T> hierarchicalResult = get(searchParameters, valueType);
                if (hierarchicalResult.isPresent()) {
                    return hierarchicalResult.get();
                }
            }
        }

        // Compute new value
        T computedValue = dataSupplier.get();
        if (computedValue != null) {
            put(uniqueKey, searchParameterSets, computedValue, ttlSeconds);
        }

        return computedValue;
    }

    /**
     * Convenience method for single parameter set
     */
    public <T> T getOrCompute(String uniqueKey, List<SearchParameter> searchParameters,
                              Class<T> valueType, Supplier<T> dataSupplier) {
        return getOrCompute(uniqueKey, searchParameters != null ? Arrays.asList(searchParameters) : null,
                valueType, dataSupplier, defaultTtlSeconds);
    }

    /**
     * Convenience method for single parameter set with custom TTL
     */
    public <T> T getOrCompute(String uniqueKey, List<SearchParameter> searchParameters,
                              Class<T> valueType, Supplier<T> dataSupplier, long ttlSeconds) {
        return getOrCompute(uniqueKey, searchParameters != null ? Arrays.asList(searchParameters) : null,
                valueType, dataSupplier, ttlSeconds);
    }

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

            // Clean up all hierarchical references for this unique key
            if (contentHash != null) {
                invalidateAllReferencesForContentHash(contentHash, uniqueKey);
                cleanupDataIfUnreferenced(contentHash);
            }
        } catch (Exception e) {
            logger.error("Error invalidating key: {}", uniqueKey, e);
        }
    }

    /**
     * Invalidates cache by hierarchical parameters - removes only specific hierarchical references
     */
    public void invalidate(List<SearchParameter> searchParameters) {
        if (searchParameters == null || searchParameters.isEmpty()) return;

        CacheKey cacheKey = new CacheKey(searchParameters);
        List<CacheKey> hierarchicalKeys = cacheKey.getHierarchicalKeys();

        for (CacheKey key : hierarchicalKeys) {
            String refKey = refPrefix + key.getKeyString();
            redissonClient.getMap(refKey).delete();
            logger.debug("Invalidated hierarchical reference for key: {}", key);
        }
    }

    /**
     * Invalidates with cleanup - removes references and cleans unreferenced data
     */
    public void invalidateWithCleanup(List<SearchParameter> searchParameters) {
        if (searchParameters == null || searchParameters.isEmpty()) return;

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
            cleanupDataIfUnreferenced(contentHash);
        }
    }

    /**
     * Clears all cache entries
     */
    public void clearAll() {
        redissonClient.getKeys().deleteByPattern(cachePrefix + "*");
        logger.info("Cleared all cache entries with prefix: {}", cachePrefix);
    }

    /**
     * Gets enhanced cache statistics
     */
    public CacheStats getStats() {
        long keyCount = redissonClient.getKeys().countByPattern(keyPrefix + "*");
        long referenceCount = redissonClient.getKeys().countByPattern(refPrefix + "*");
        long dataCount = redissonClient.getKeys().countByPattern(dataPrefix + "*");
        long refSetCount = redissonClient.getKeys().countByPattern(refSetPrefix + "*");
        return new CacheStats(keyCount, referenceCount, dataCount, refSetCount);
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
        }
        return Optional.empty();
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
                    refMap.delete();
                    logger.warn("Found stale hierarchical reference for key: {}, cleaning up", cacheKey);
                }
            }
        } catch (Exception e) {
            logger.error("Error retrieving from cache for hierarchical key: {}", cacheKey, e);
        }

        return Optional.empty();
    }

    private <T> List<T> getAllFromCacheByHierarchicalReference(CacheKey cacheKey, Class<T> valueType, Set<String> seenContentHashes) {
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

    private void createHierarchicalReference(CacheKey cacheKey, String contentHash, String uniqueKey, long ttlSeconds) {
        String refKey = refPrefix + cacheKey.getKeyString();
        RMap<String, String> refMap = redissonClient.getMap(refKey);
        refMap.put("content_hash", contentHash);
        refMap.put("unique_key", uniqueKey);
        refMap.put("created_at", String.valueOf(System.currentTimeMillis()));
        refMap.expire(ttlSeconds, TimeUnit.SECONDS);
    }

    private void invalidateAllReferencesForContentHash(String contentHash, String uniqueKey) {
        try {
            String refSetKey = refSetPrefix + contentHash;
            RSet<String> refSet = redissonClient.getSet(refSetKey);

            if (refSet.isExists()) {
                Set<String> referenceKeys = refSet.readAll();

                for (String refKeyString : referenceKeys) {
                    String refKey = refPrefix + refKeyString;
                    RMap<String, String> refMap = redissonClient.getMap(refKey);
                    String refUniqueKey = refMap.get("unique_key");

                    // Only remove references that belong to this unique key
                    if (uniqueKey.equals(refUniqueKey)) {
                        refMap.delete();
                        logger.debug("Removed hierarchical reference: {} for unique key: {}", refKeyString, uniqueKey);
                    }
                }

                // Clean up reference set if no more references exist
                if (referenceKeys.isEmpty()) {
                    refSet.delete();
                }
            }
        } catch (Exception e) {
            logger.error("Error invalidating references for content hash: {} and unique key: {}", contentHash, uniqueKey, e);
        }
    }

    private String generateUniqueKey() {
        return "key_" + System.currentTimeMillis() + "_" + Math.random();
    }

    private <T> String generateContentHash(T value) {
        try {
            // Use object's hashCode and class for simple content hash
            String content = value.getClass().getName() + ":" + value.hashCode();
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
            // Fallback to simple hash
            return String.valueOf(value.hashCode());
        }
    }

    private void cleanupDataIfUnreferenced(String contentHash) {
        try {
            // Check key references
            String keyPattern = keyPrefix + "*";
            Iterable<String> keyKeys = redissonClient.getKeys().getKeysByPattern(keyPattern);
            for (String keyKey : keyKeys) {
                RMap<String, String> keyMap = redissonClient.getMap(keyKey);
                String refContentHash = keyMap.get("content_hash");
                if (contentHash.equals(refContentHash)) {
                    return; // Still referenced by a key
                }
            }

            // Check hierarchical references
            String refPattern = refPrefix + "*";
            Iterable<String> refKeys = redissonClient.getKeys().getKeysByPattern(refPattern);
            for (String refKey : refKeys) {
                RMap<String, String> refMap = redissonClient.getMap(refKey);
                String refContentHash = refMap.get("content_hash");
                if (contentHash.equals(refContentHash)) {
                    return; // Still referenced by hierarchical key
                }
            }

            // No references found, clean up data and reference set
            String dataKey = dataPrefix + contentHash;
            redissonClient.getBucket(dataKey).delete();

            String refSetKey = refSetPrefix + contentHash;
            redissonClient.getSet(refSetKey).delete();

            logger.debug("Cleaned up unreferenced data and reference set: {}", contentHash);

        } catch (Exception e) {
            logger.error("Error during cleanup of data: {}", contentHash, e);
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
            return String.format("CacheStats{keys=%d, hierarchicalRefs=%d, actualData=%d, refSets=%d, totalRefs=%d, compressionRatio=%.2f, avgRefsPerData=%.2f}",
                    keyCount, referenceCount, dataCount, refSetCount, getTotalReferences(),
                    getCompressionRatio(), getAverageReferencesPerData());
        }
    }
}