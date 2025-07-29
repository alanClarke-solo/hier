// src/main/java/ac/hier/cache/RedissonClientFactory.java
package ac.hier.cache;

import org.redisson.Redisson;
import org.redisson.api.RedissonClient;
import org.redisson.codec.Kryo5Codec;
import org.redisson.config.Config;

/**
 * Factory for creating RedissonClient instances with Kryo5Codec
 */
public class RedissonClientFactory {

    /**
     * Creates a default RedissonClient with Kryo5Codec for binary serialization
     */
    public static RedissonClient createDefault() {
        Config config = new Config();
        config.useSingleServer()
                .setAddress("redis://localhost:6379")
                .setConnectionMinimumIdleSize(10)
                .setConnectionPoolSize(64)
                .setIdleConnectionTimeout(10000)
                .setConnectTimeout(10000)
                .setTimeout(3000)
                .setRetryAttempts(3)
                .setRetryInterval(1500);

        // Use Kryo5Codec for efficient binary serialization
        config.setCodec(new Kryo5Codec());

        return Redisson.create(config);
    }

    /**
     * Creates a RedissonClient with custom configuration and Kryo5Codec
     */
    public static RedissonClient create(String address, String password) {
        Config config = new Config();
        config.useSingleServer()
                .setAddress(address)
                .setPassword(password)
                .setConnectionMinimumIdleSize(10)
                .setConnectionPoolSize(64)
                .setIdleConnectionTimeout(10000)
                .setConnectTimeout(10000)
                .setTimeout(3000)
                .setRetryAttempts(3)
                .setRetryInterval(1500);

        // Use Kryo5Codec for efficient binary serialization
        config.setCodec(new Kryo5Codec());

        return Redisson.create(config);
    }
}