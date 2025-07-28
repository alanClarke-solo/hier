// src/main/java/ac/hier/cache/CacheKey.java
package ac.hier.cache;

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * Represents a hierarchical cache key built from multiple search parameters
 */
public class CacheKey {
    private final List<SearchParameter> parameters;
    private final String keyString;

    public CacheKey(List<SearchParameter> parameters) {
        this.parameters = Objects.requireNonNull(parameters, "Parameters cannot be null");
        this.keyString = buildKeyString();
    }

    private String buildKeyString() {
        return parameters.stream()
                .sorted((p1, p2) -> {
                    // Sort by level first, then by key, then by value for consistent ordering
                    int levelCompare = Integer.compare(p1.getLevel(), p2.getLevel());
                    if (levelCompare != 0) return levelCompare;
                    
                    int keyCompare = p1.getKey().compareTo(p2.getKey());
                    if (keyCompare != 0) return keyCompare;
                    
                    return p1.getValue().compareTo(p2.getValue());
                })
                .map(p -> String.format("L%d:%s=%s", p.getLevel(), p.getKey(), p.getValue()))
                .collect(Collectors.joining("|"));
    }

    public List<SearchParameter> getParameters() {
        return parameters;
    }

    public String getKeyString() {
        return keyString;
    }

    /**
     * Generates hierarchical cache keys for different levels
     * This allows caching at multiple hierarchy levels
     */
    public List<CacheKey> getHierarchicalKeys() {
        return parameters.stream()
                .collect(Collectors.groupingBy(SearchParameter::getLevel))
                .entrySet()
                .stream()
                .sorted((e1, e2) -> Integer.compare(e1.getKey(), e2.getKey()))
                .map(entry -> {
                    List<SearchParameter> levelParams = parameters.stream()
                            .filter(p -> p.getLevel() <= entry.getKey())
                            .collect(Collectors.toList());
                    return new CacheKey(levelParams);
                })
                .collect(Collectors.toList());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        CacheKey cacheKey = (CacheKey) o;
        return Objects.equals(keyString, cacheKey.keyString);
    }

    @Override
    public int hashCode() {
        return Objects.hash(keyString);
    }

    @Override
    public String toString() {
        return keyString;
    }
}
