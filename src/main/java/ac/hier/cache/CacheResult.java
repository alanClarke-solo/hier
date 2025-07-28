// src/main/java/ac/hier/cache/CacheResult.java
package ac.hier.cache;

import java.time.LocalDateTime;
import java.util.Objects;

/**
 * Represents a cached result with metadata
 */
public class CacheResult<T> {
    private final T data;
    private final LocalDateTime timestamp;
    private final CacheKey cacheKey;
    private final long ttlSeconds;

    public CacheResult(T data, CacheKey cacheKey, long ttlSeconds) {
        this.data = data;
        this.cacheKey = Objects.requireNonNull(cacheKey, "Cache key cannot be null");
        this.timestamp = LocalDateTime.now();
        this.ttlSeconds = ttlSeconds;
    }

    public T getData() {
        return data;
    }

    public LocalDateTime getTimestamp() {
        return timestamp;
    }

    public CacheKey getCacheKey() {
        return cacheKey;
    }

    public long getTtlSeconds() {
        return ttlSeconds;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        CacheResult<?> that = (CacheResult<?>) o;
        return ttlSeconds == that.ttlSeconds &&
               Objects.equals(data, that.data) &&
               Objects.equals(timestamp, that.timestamp) &&
               Objects.equals(cacheKey, that.cacheKey);
    }

    @Override
    public int hashCode() {
        return Objects.hash(data, timestamp, cacheKey, ttlSeconds);
    }

    @Override
    public String toString() {
        return String.format("CacheResult{cacheKey=%s, timestamp=%s, ttlSeconds=%d}", 
                           cacheKey, timestamp, ttlSeconds);
    }
}
