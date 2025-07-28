// src/main/java/ac/hier/cache/example/Product.java
package ac.hier.cache.example;

import java.math.BigDecimal;
import java.util.Objects;

/**
 * Example domain object for demonstrating hierarchical caching
 */
public class Product {
    private String id;
    private String name;
    private String category;
    private String brand;
    private BigDecimal price;
    private String region;

    public Product() {
        // Default constructor for Jackson
    }

    public Product(String id, String name, String category, String brand, BigDecimal price, String region) {
        this.id = id;
        this.name = name;
        this.category = category;
        this.brand = brand;
        this.price = price;
        this.region = region;
    }

    // Getters and setters
    public String getId() { return id; }
    public void setId(String id) { this.id = id; }

    public String getName() { return name; }
    public void setName(String name) { this.name = name; }

    public String getCategory() { return category; }
    public void setCategory(String category) { this.category = category; }

    public String getBrand() { return brand; }
    public void setBrand(String brand) { this.brand = brand; }

    public BigDecimal getPrice() { return price; }
    public void setPrice(BigDecimal price) { this.price = price; }

    public String getRegion() { return region; }
    public void setRegion(String region) { this.region = region; }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Product product = (Product) o;
        return Objects.equals(id, product.id);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id);
    }

    @Override
    public String toString() {
        return String.format("Product{id='%s', name='%s', category='%s', brand='%s', price=%s, region='%s'}", 
                           id, name, category, brand, price, region);
    }
}
