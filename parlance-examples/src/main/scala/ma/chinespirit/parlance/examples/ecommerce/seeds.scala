package ma.chinespirit.parlance.examples.ecommerce

import ma.chinespirit.parlance.*

import java.time.OffsetDateTime

object SeedData:
  def insertAll()(using DbCon[Postgres], AppContext): Unit =
    Category.repo.rawInsertAll(
      Seq(
        CategoryCreator("Electronics", None),
        CategoryCreator("Clothing", None),
        CategoryCreator("Smartphones", Some(1L)),
        CategoryCreator("Laptops", Some(1L))
      )
    )

    Product.repo.rawInsertAll(
      Seq(
        ProductCreator("iPhone 15", "Latest Apple smartphone", 3L, Vector("phone", "apple", "premium"), true),
        ProductCreator("Galaxy S24", "Samsung flagship", 3L, Vector("phone", "samsung", "android"), true),
        ProductCreator("MacBook Pro", "Professional laptop", 4L, Vector("laptop", "apple", "premium"), true),
        ProductCreator("Old Phone", "Discontinued phone", 3L, Vector("phone", "discontinued"), false),
        ProductCreator("AirPods Pro", "Wireless earbuds", 1L, Vector("audio", "apple", "premium"), true)
      )
    )

    ProductVariant.repo.rawInsertAll(
      Seq(
        VariantCreator(1L, "IP15-128-BLK", Some("128GB"), Some("Black"), BigDecimal("999.99"), 50),
        VariantCreator(1L, "IP15-256-WHT", Some("256GB"), Some("White"), BigDecimal("1099.99"), 30),
        VariantCreator(2L, "GS24-128-BLU", Some("128GB"), Some("Blue"), BigDecimal("899.99"), 40),
        VariantCreator(3L, "MBP-14-SLV", Some("14\""), Some("Silver"), BigDecimal("1999.99"), 20),
        VariantCreator(4L, "OLD-64-GRY", Some("64GB"), Some("Gray"), BigDecimal("299.99"), 0)
      )
    )

    Customer.repo.rawInsertAll(
      Seq(
        CustomerCreator("Alice", "Smith", "alice@example.com"),
        CustomerCreator("Bob", "Jones", "bob@example.com"),
        CustomerCreator("Charlie", "Brown", "charlie@example.com")
      )
    )

    Order.repo.rawInsertAll(
      Seq(
        OrderCreator(1L, OrderStatus.Delivered, BigDecimal("999.99"), "123 Main St", "NYC", "US", "123 Main St", "NYC", "US"),
        OrderCreator(1L, OrderStatus.Shipped, BigDecimal("1099.99"), "123 Main St", "NYC", "US", "123 Main St", "NYC", "US"),
        OrderCreator(2L, OrderStatus.Pending, BigDecimal("899.99"), "456 Oak Ave", "LA", "US", "456 Oak Ave", "LA", "US"),
        OrderCreator(3L, OrderStatus.Confirmed, BigDecimal("1999.99"), "789 Pine Rd", "CHI", "US", "789 Pine Rd", "CHI", "US")
      )
    )

    OrderItem.repo.rawInsertAll(
      Seq(
        OrderItemCreator(1L, 1L, 1, BigDecimal("999.99")),
        OrderItemCreator(2L, 2L, 1, BigDecimal("1099.99")),
        OrderItemCreator(3L, 3L, 1, BigDecimal("899.99")),
        OrderItemCreator(4L, 4L, 1, BigDecimal("1999.99"))
      )
    )

    Review.repo.rawInsertAll(
      Seq(
        ReviewCreator(1L, 1L, 5, "Amazing phone", "Best phone I ever owned!", true),
        ReviewCreator(1L, 2L, 4, "Great phone", "Really good but pricey", false),
        ReviewCreator(2L, 1L, 4, "Solid choice", "Good Android phone", true),
        ReviewCreator(3L, 3L, 5, "Perfect laptop", "Incredible machine", true)
      )
    )

    Wishlist.repo.rawInsertAll(
      Seq(
        WishlistCreator(1L, 2L, OffsetDateTime.now()),
        WishlistCreator(1L, 3L, OffsetDateTime.now()),
        WishlistCreator(2L, 1L, OffsetDateTime.now())
      )
    )
  end insertAll
end SeedData
