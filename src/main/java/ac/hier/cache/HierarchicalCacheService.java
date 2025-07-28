// src/main/java/ac/hier/cache/HierarchicalCacheService.java
package ac.hier.cache;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.type.TypeFactory;
import org.redisson.api.RMap;
import org.redisson.api.RedissonClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

/**
 * Hierarchical cache service using Redis and Redisson
 * Supports caching with multiple search parameters organized in hierarchical levels
 */
public class HierarchicalCacheService {
    private static final Logger logger = LoggerFactory.getLogger(HierarchicalCacheService.class);
    
    private final RedissonClient redissonClient;
    private final ObjectMapper objectMapper;
    private final String cachePrefix;
    private final long defaultTtlSeconds;

    public HierarchicalCacheService(RedissonClient redissonClient, 
                                  String cachePrefix, 
                                  long defaultTtlSeconds) {
        this.redissonClient = redissonClient;
        this.objectMapper = new ObjectMapper();
        this.cachePrefix = cachePrefix + ":";
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
            Optional<T> result = getFromCache(key, valueType);
            if (result.isPresent()) {
                logger.debug("Cache hit for key: {}", key);
                return result;
            }
        }
        
        logger.debug("Cache miss for all hierarchical keys of: {}", cacheKey);
        return Optional.empty();
    }

    /**
     * Stores data in cache with hierarchical keys
     */
    public <T> void put(List<SearchParameter> searchParameters, T value) {
        put(searchParameters, value, defaultTtlSeconds);
    }

    /**
     * Stores data in cache with hierarchical keys and custom TTL
     */
    public <T> void put(List<SearchParameter> searchParameters, T value, long ttlSeconds) {
        CacheKey cacheKey = new CacheKey(searchParameters);
        List<CacheKey> hierarchicalKeys = cacheKey.getHierarchicalKeys();
        
        // Store at all hierarchical levels
        for (CacheKey key : hierarchicalKeys) {
            putToCache(key, value, ttlSeconds);
            logger.debug("Cached data for key: {}", key);
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
     */
    public void invalidate(List<SearchParameter> searchParameters) {
        CacheKey cacheKey = new CacheKey(searchParameters);
        List<CacheKey> hierarchicalKeys = cacheKey.getHierarchicalKeys();
        
        for (CacheKey key : hierarchicalKeys) {
            String redisKey = cachePrefix + key.getKeyString();
            redissonClient.getMap(redisKey).delete();
            logger.debug("Invalidated cache for key: {}", key);
        }
    }

    /**
     * Clears all cache entries with the configured prefix
     */
    public void clearAll() {
        redissonClient.getKeys().deleteByPattern(cachePrefix + "*");
        logger.info("Cleared all cache entries with prefix: {}", cachePrefix);
    }

    private <T> Optional<T> getFromCache(CacheKey cacheKey, Class<T> valueType) {
        try {
            String redisKey = cachePrefix + cacheKey.getKeyString();
            RMap<String, String> map = redissonClient.getMap(redisKey);
            String jsonValue = map.get("data");
            
            if (jsonValue != null) {
                T value = objectMapper.readValue(jsonValue, valueType);
                return Optional.of(value);
            }
        } catch (JsonProcessingException e) {
            logger.error("Error deserializing cached value for key: {}", cacheKey, e);
        } catch (Exception e) {
            logger.error("Error retrieving from cache for key: {}", cacheKey, e);
        }
        
        return Optional.empty();
    }

    private <T> void putToCache(CacheKey cacheKey, T value, long ttlSeconds) {
        try {
            String redisKey = cachePrefix + cacheKey.getKeyString();
            RMap<String, String> map = redissonClient.getMap(redisKey);
            
            String jsonValue = objectMapper.writeValueAsString(value);
            map.put("data", jsonValue);
            map.expire(ttlSeconds, TimeUnit.SECONDS);
            
        } catch (JsonProcessingException e) {
            logger.error("Error serializing value for key: {}", cacheKey, e);
        } catch (Exception e) {
            logger.error("Error storing to cache for key: {}", cacheKey, e);
        }
    }
}
