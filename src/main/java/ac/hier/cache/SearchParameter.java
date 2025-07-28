// src/main/java/ac/hier/cache/SearchParameter.java
package ac.hier.cache;

import java.util.Objects;

/**
 * Represents a search parameter with a key-value pair and hierarchical level
 */
public class SearchParameter {
    private final String key;
    private final String value;
    private final int level;

    public SearchParameter(String key, String value, int level) {
        this.key = Objects.requireNonNull(key, "Key cannot be null");
        this.value = Objects.requireNonNull(value, "Value cannot be null");
        this.level = level;
    }

    public String getKey() {
        return key;
    }

    public String getValue() {
        return value;
    }

    public int getLevel() {
        return level;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SearchParameter that = (SearchParameter) o;
        return level == that.level &&
               Objects.equals(key, that.key) &&
               Objects.equals(value, that.value);
    }

    @Override
    public int hashCode() {
        return Objects.hash(key, value, level);
    }

    @Override
    public String toString() {
        return String.format("SearchParameter{key='%s', value='%s', level=%d}", key, value, level);
    }
}
