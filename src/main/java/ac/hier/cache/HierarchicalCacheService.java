// src/main/java/ac/hier/cache/HierarchicalCacheService.java
package ac.hier.cache;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.redisson.api.RMap;
import org.redisson.api.RedissonClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

/**
 * Hierarchical cache service using Redis and Redisson
 * Supports caching with multiple search parameters organized in hierarchical levels
 * Uses a reference-based approach to store actual data only once
 */
public class HierarchicalCacheService {
    private static final Logger logger = LoggerFactory.getLogger(HierarchicalCacheService.class);

    private final RedissonClient redissonClient;
    private final ObjectMapper objectMapper;
    private final String cachePrefix;
    private final String dataPrefix;
    private final String refPrefix;
    private final long defaultTtlSeconds;

    public HierarchicalCacheService(RedissonClient redissonClient,
                                    String cachePrefix,
                                    long defaultTtlSeconds) {
        this.redissonClient = redissonClient;
        this.objectMapper = new ObjectMapper();
        this.cachePrefix = cachePrefix + ":";
        this.dataPrefix = this.cachePrefix + "data:";
        this.refPrefix = this.cachePrefix + "ref:";
        this.defaultTtlSeconds = defaultTtlSeconds;
    }

    /**
     * Retrieves data from cache using hierarchical search parameters
     * Searches from most specific to least specific cache keys
     */
    public <T> Optional<T> get(List<SearchParameter> searchParameters, Class<T> valueType) {
        CacheKey cacheKey = new CacheKey(searchParameters);
        List<CacheKey> hierarchicalKeys = cacheKey.getHierarchicalKeys();

        // Search from most specific to least specific
        for (int i = hierarchicalKeys.size() - 1; i >= 0; i--) {
            CacheKey key = hierarchicalKeys.get(i);
            Optional<T> result = getFromCacheByReference(key, valueType);
            if (result.isPresent()) {
                logger.debug("Cache hit for key: {}", key);
                return result;
            }
        }

        logger.debug("Cache miss for all hierarchical keys of: {}", cacheKey);
        return Optional.empty();
    }

    /**
     * Stores data in cache with hierarchical keys using reference-based approach
     */
    public <T> void put(List<SearchParameter> searchParameters, T value) {
        put(searchParameters, value, defaultTtlSeconds);
    }

    /**
     * Stores data in cache with hierarchical keys and custom TTL using reference-based approach
     */
    public <T> void put(List<SearchParameter> searchParameters, T value, long ttlSeconds) {
        if (value == null) {
            logger.warn("Attempted to cache null value for parameters: {}", searchParameters);
            return;
        }

        try {
            // Serialize the value to get its content hash
            String jsonValue = objectMapper.writeValueAsString(value);
            String contentHash = generateContentHash(jsonValue);
            String dataKey = dataPrefix + contentHash;

            // Store the actual data only once using content hash
            RMap<String, Object> dataMap = redissonClient.getMap(dataKey);
            if (!dataMap.containsKey("data")) {
                dataMap.put("data", jsonValue);
                dataMap.put("created_at", System.currentTimeMillis());
                dataMap.expire(ttlSeconds + 300, TimeUnit.SECONDS); // Give data extra TTL buffer
                logger.debug("Stored actual data for content hash: {}", contentHash);
            } else {
                // Extend TTL if data already exists
                dataMap.expire(ttlSeconds + 300, TimeUnit.SECONDS);
                logger.debug("Extended TTL for existing data with content hash: {}", contentHash);
            }

            // Create references for all hierarchical keys
            CacheKey cacheKey = new CacheKey(searchParameters);
            List<CacheKey> hierarchicalKeys = cacheKey.getHierarchicalKeys();

            for (CacheKey key : hierarchicalKeys) {
                createReference(key, contentHash, ttlSeconds);
                logger.debug("Created reference for key: {} -> {}", key, contentHash);
            }

        } catch (JsonProcessingException e) {
            logger.error("Error serializing value for parameters: {}", searchParameters, e);
        } catch (Exception e) {
            logger.error("Error storing to cache for parameters: {}", searchParameters, e);
        }
    }

    /**
     * Gets data from cache or computes it using the supplier and caches the result
     */
    public <T> T getOrCompute(List<SearchParameter> searchParameters,
                              Class<T> valueType,
                              Supplier<T> dataSupplier) {
        return getOrCompute(searchParameters, valueType, dataSupplier, defaultTtlSeconds);
    }

    /**
     * Gets data from cache or computes it using the supplier and caches the result with custom TTL
     */
    public <T> T getOrCompute(List<SearchParameter> searchParameters,
                              Class<T> valueType,
                              Supplier<T> dataSupplier,
                              long ttlSeconds) {
        Optional<T> cachedValue = get(searchParameters, valueType);
        if (cachedValue.isPresent()) {
            return cachedValue.get();
        }

        T computedValue = dataSupplier.get();
        if (computedValue != null) {
            put(searchParameters, computedValue, ttlSeconds);
        }

        return computedValue;
    }

    /**
     * Invalidates cache entries for the given search parameters
     * Only removes references, not the actual data (which may be referenced by other keys)
     */
    public void invalidate(List<SearchParameter> searchParameters) {
        CacheKey cacheKey = new CacheKey(searchParameters);
        List<CacheKey> hierarchicalKeys = cacheKey.getHierarchicalKeys();

        for (CacheKey key : hierarchicalKeys) {
            String refKey = refPrefix + key.getKeyString();
            redissonClient.getMap(refKey).delete();
            logger.debug("Invalidated reference for key: {}", key);
        }
    }

    /**
     * Invalidates specific reference and cleans up unreferenced data
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
            logger.debug("Invalidated reference for key: {}", key);

            // Check if we should clean up the data (optional - can be done by a separate cleanup job)
            if (contentHash != null) {
                cleanupDataIfUnreferenced(contentHash);
            }
        }
    }

    /**
     * Clears all cache entries with the configured prefix
     */
    public void clearAll() {
        redissonClient.getKeys().deleteByPattern(cachePrefix + "*");
        logger.info("Cleared all cache entries with prefix: {}", cachePrefix);
    }

    /**
     * Gets cached data count statistics
     */
    public CacheStats getStats() {
        long referenceCount = redissonClient.getKeys().countByPattern(refPrefix + "*");
        long dataCount = redissonClient.getKeys().countByPattern(dataPrefix + "*");
        return new CacheStats(referenceCount, dataCount);
    }

    private <T> Optional<T> getFromCacheByReference(CacheKey cacheKey, Class<T> valueType) {
        try {
            String refKey = refPrefix + cacheKey.getKeyString();
            RMap<String, String> refMap = redissonClient.getMap(refKey);
            String contentHash = refMap.get("content_hash");

            if (contentHash != null) {
                // Get actual data using content hash
                String dataKey = dataPrefix + contentHash;
                RMap<String, Object> dataMap = redissonClient.getMap(dataKey);
                String jsonValue = (String) dataMap.get("data");

                if (jsonValue != null) {
                    T value = objectMapper.readValue(jsonValue, valueType);
                    return Optional.of(value);
                } else {
                    // Data was cleaned up but reference still exists - clean up the stale reference
                    refMap.delete();
                    logger.warn("Found stale reference for key: {}, cleaning up", cacheKey);
                }
            }
        } catch (JsonProcessingException e) {
            logger.error("Error deserializing cached value for key: {}", cacheKey, e);
        } catch (Exception e) {
            logger.error("Error retrieving from cache for key: {}", cacheKey, e);
        }

        return Optional.empty();
    }

    private void createReference(CacheKey cacheKey, String contentHash, long ttlSeconds) {
        String refKey = refPrefix + cacheKey.getKeyString();
        RMap<String, String> refMap = redissonClient.getMap(refKey);
        refMap.put("content_hash", contentHash);
        refMap.put("created_at", String.valueOf(System.currentTimeMillis()));
        refMap.expire(ttlSeconds, TimeUnit.SECONDS);
    }

    private String generateContentHash(String content) {
        try {
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
            // Fallback to simpler hash
            return String.valueOf(content.hashCode());
        }
    }

    private void cleanupDataIfUnreferenced(String contentHash) {
        // This is a simple implementation - in production you might want to use a more sophisticated approach
        // like reference counting or a separate cleanup job
        try {
            String pattern = refPrefix + "*";
            Iterable<String> refKeys = redissonClient.getKeys().getKeysByPattern(pattern);

            boolean hasReferences = false;
            for (String refKey : refKeys) {
                RMap<String, String> refMap = redissonClient.getMap(refKey);
                String refContentHash = refMap.get("content_hash");
                if (contentHash.equals(refContentHash)) {
                    hasReferences = true;
                    break;
                }
            }

            if (!hasReferences) {
                String dataKey = dataPrefix + contentHash;
                redissonClient.getMap(dataKey).delete();
                logger.debug("Cleaned up unreferenced data: {}", contentHash);
            }
        } catch (Exception e) {
            logger.error("Error during cleanup of data: {}", contentHash, e);
        }
    }

    /**
     * Cache statistics holder
     */
    public static class CacheStats {
        private final long referenceCount;
        private final long dataCount;

        public CacheStats(long referenceCount, long dataCount) {
            this.referenceCount = referenceCount;
            this.dataCount = dataCount;
        }

        public long getReferenceCount() {
            return referenceCount;
        }

        public long getDataCount() {
            return dataCount;
        }

        public double getCompressionRatio() {
            return dataCount == 0 ? 0 : (double) referenceCount / dataCount;
        }

        @Override
        public String toString() {
            return String.format("CacheStats{references=%d, actualData=%d, compressionRatio=%.2f}",
                    referenceCount, dataCount, getCompressionRatio());
        }
    }
}