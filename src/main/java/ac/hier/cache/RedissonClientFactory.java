// src/main/java/ac/hier/cache/RedissonClientFactory.java
package ac.hier.cache;

import org.redisson.Redisson;
import org.redisson.api.RedissonClient;
import org.redisson.config.Config;

/**
 * Factory for creating Redisson client instances
 */
public class RedissonClientFactory {
    
    /**
     * Creates a Redisson client with default local Redis configuration
     */
    public static RedissonClient createDefault() {
        Config config = new Config();
        config.useSingleServer().setAddress("redis://localhost:6379");
        return Redisson.create(config);
    }

    /**
     * Creates a Redisson client with custom Redis server configuration
     */
    public static RedissonClient create(String redisAddress, String password) {
        Config config = new Config();
        config.useSingleServer()
              .setAddress(redisAddress)
              .setPassword(password);
        return Redisson.create(config);
    }

    /**
     * Creates a Redisson client with cluster configuration
     */
    public static RedissonClient createCluster(String... nodeAddresses) {
        Config config = new Config();
        config.useClusterServers().addNodeAddress(nodeAddresses);
        return Redisson.create(config);
    }
}
