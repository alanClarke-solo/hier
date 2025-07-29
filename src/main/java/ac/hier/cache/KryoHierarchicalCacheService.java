// src/main/java/ac/hier/cache/KryoHierarchicalCacheService.java
package ac.hier.cache;

import org.redisson.api.RBucket;
import org.redisson.api.RMap;
import org.redisson.api.RedissonClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.security.MessageDigest;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

/**
 * Kryo-based hierarchical cache service using String keys for direct access
 * Uses Redisson's built-in Kryo5Codec for efficient binary serialization
 * Supports both direct String key access and hierarchical parameter-based search
 */
public class KryoHierarchicalCacheService {
    private static final Logger logger = LoggerFactory.getLogger(KryoHierarchicalCacheService.class);
    
    private final RedissonClient redissonClient;
    private final String cachePrefix;
    private final String dataPrefix;
    private final String refPrefix;
    private final String keyPrefix;
    private final long defaultTtlSeconds;

    public KryoHierarchicalCacheService(RedissonClient redissonClient, 
                                       String cachePrefix, 
                                       long defaultTtlSeconds) {
        this.redissonClient = redissonClient;
        this.cachePrefix = cachePrefix + ":";
        this.dataPrefix = this.cachePrefix + "data:";
        this.refPrefix = this.cachePrefix + "ref:";
        this.keyPrefix = this.cachePrefix + "key:";
        this.defaultTtlSeconds = defaultTtlSeconds;
    }

    /**
     * Stores data with String key and hierarchical search parameters
     * @param uniqueKey Unique string identifier for direct access
     * @param searchParameters Hierarchical search parameters
     * @param value Data to cache
     * @return The unique key used for storage
     */
    public <T> String put(String uniqueKey, List<SearchParameter> searchParameters, T value) {
        return put(uniqueKey, searchParameters, value, defaultTtlSeconds);
    }

    /**
     * Stores data with String key and hierarchical search parameters with custom TTL
     */
    public <T> String put(String uniqueKey, List<SearchParameter> searchParameters, T value, long ttlSeconds) {
        if (value == null) {
            logger.warn("Attempted to cache null value for key: {} and parameters: {}", uniqueKey, searchParameters);
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

            // Create hierarchical references
            if (searchParameters != null && !searchParameters.isEmpty()) {
                CacheKey cacheKey = new CacheKey(searchParameters);
                List<CacheKey> hierarchicalKeys = cacheKey.getHierarchicalKeys();
                
                for (CacheKey key : hierarchicalKeys) {
                    createHierarchicalReference(key, contentHash, uniqueKey, ttlSeconds);
                    logger.debug("Created hierarchical reference for key: {} -> {} (UniqueKey: {})", 
                               key, contentHash, uniqueKey);
                }
            }

            return uniqueKey;
            
        } catch (Exception e) {
            logger.error("Error storing to cache for key: {} and parameters: {}", uniqueKey, searchParameters, e);
            throw new RuntimeException("Failed to store cache entry", e);
        }
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
     * Retrieves data using hierarchical search parameters
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
     * Gets data from cache or computes it with String key support
     */
    public <T> T getOrCompute(String uniqueKey, List<SearchParameter> searchParameters, 
                            Class<T> valueType, Supplier<T> dataSupplier) {
        return getOrCompute(uniqueKey, searchParameters, valueType, dataSupplier, defaultTtlSeconds);
    }

    /**
     * Gets data from cache or computes it with String key support and custom TTL
     */
    public <T> T getOrCompute(String uniqueKey, List<SearchParameter> searchParameters, 
                            Class<T> valueType, Supplier<T> dataSupplier, long ttlSeconds) {
        // Try unique key first if provided
        if (uniqueKey != null) {
            Optional<T> keyResult = getByKey(uniqueKey, valueType);
            if (keyResult.isPresent()) {
                return keyResult.get();
            }
        }

        // Try hierarchical search
        Optional<T> hierarchicalResult = get(searchParameters, valueType);
        if (hierarchicalResult.isPresent()) {
            return hierarchicalResult.get();
        }

        // Compute new value
        T computedValue = dataSupplier.get();
        if (computedValue != null) {
            put(uniqueKey, searchParameters, computedValue, ttlSeconds);
        }
        
        return computedValue;
    }

    /**
     * Invalidates cache by unique String key
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
            
            // Optionally clean up data if no other references exist
            if (contentHash != null) {
                cleanupDataIfUnreferenced(contentHash);
            }
        } catch (Exception e) {
            logger.error("Error invalidating key: {}", uniqueKey, e);
        }
    }

    /**
     * Invalidates cache by hierarchical parameters
     */
    public void invalidate(List<SearchParameter> searchParameters) {
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
        CacheKey cacheKey = new CacheKey(searchParameters);
        List<CacheKey> hierarchicalKeys = cacheKey.getHierarchicalKeys();
        
        for (CacheKey key : hierarchicalKeys) {
            String refKey = refPrefix + key.getKeyString();
            RMap<String, String> refMap = redissonClient.getMap(refKey);
            String contentHash = refMap.get("content_hash");
            
            // Remove reference
            refMap.delete();
            logger.debug("Invalidated hierarchical reference for key: {}", key);
            
            // Clean up data if no other references exist
            if (contentHash != null) {
                cleanupDataIfUnreferenced(contentHash);
            }
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
     * Gets cache statistics
     */
    public CacheStats getStats() {
        long keyCount = redissonClient.getKeys().countByPattern(keyPrefix + "*");
        long referenceCount = redissonClient.getKeys().countByPattern(refPrefix + "*");
        long dataCount = redissonClient.getKeys().countByPattern(dataPrefix + "*");
        return new CacheStats(keyCount, referenceCount, dataCount);
    }

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

    private void createHierarchicalReference(CacheKey cacheKey, String contentHash, String uniqueKey, long ttlSeconds) {
        String refKey = refPrefix + cacheKey.getKeyString();
        RMap<String, String> refMap = redissonClient.getMap(refKey);
        refMap.put("content_hash", contentHash);
        refMap.put("unique_key", uniqueKey);
        refMap.put("created_at", String.valueOf(System.currentTimeMillis()));
        refMap.expire(ttlSeconds, TimeUnit.SECONDS);
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
                    return; // Still referenced
                }
            }

            // Check hierarchical references
            String refPattern = refPrefix + "*";
            Iterable<String> refKeys = redissonClient.getKeys().getKeysByPattern(refPattern);
            for (String refKey : refKeys) {
                RMap<String, String> refMap = redissonClient.getMap(refKey);
                String refContentHash = refMap.get("content_hash");
                if (contentHash.equals(refContentHash)) {
                    return; // Still referenced
                }
            }

            // No references found, clean up data
            String dataKey = dataPrefix + contentHash;
            redissonClient.getBucket(dataKey).delete();
            logger.debug("Cleaned up unreferenced data: {}", contentHash);
            
        } catch (Exception e) {
            logger.error("Error during cleanup of data: {}", contentHash, e);
        }
    }

    /**
     * Cache statistics holder
     */
    public static class CacheStats {
        private final long keyCount;
        private final long referenceCount;
        private final long dataCount;

        public CacheStats(long keyCount, long referenceCount, long dataCount) {
            this.keyCount = keyCount;
            this.referenceCount = referenceCount;
            this.dataCount = dataCount;
        }

        public long getKeyCount() { return keyCount; }
        public long getReferenceCount() { return referenceCount; }
        public long getDataCount() { return dataCount; }

        public long getTotalReferences() { return keyCount + referenceCount; }

        public double getCompressionRatio() {
            return dataCount == 0 ? 0 : (double) getTotalReferences() / dataCount;
        }

        @Override
        public String toString() {
            return String.format("CacheStats{keys=%d, hierarchicalRefs=%d, actualData=%d, totalRefs=%d, compressionRatio=%.2f}", 
                               keyCount, referenceCount, dataCount, getTotalReferences(), getCompressionRatio());
        }
    }
}
