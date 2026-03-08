package ma.chinespirit.parlance.examples.ecommerce

import com.dimafeng.testcontainers.PostgreSQLContainer
import com.dimafeng.testcontainers.munit.fixtures.TestContainersFixtures
import munit.{AnyFixture, FunSuite, Tag}
import org.postgresql.ds.PGSimpleDataSource
import org.testcontainers.utility.DockerImageName

import ma.chinespirit.parlance.*
import ma.chinespirit.parlance.pg.PgCodec.given
import ma.chinespirit.parlance.pg.enums.given

class ECommerceTests extends FunSuite, TestContainersFixtures:

  given ctx: AppContext = AppContext()

  val accounts = AccountController()
  val catalog = CatalogController()
  val categories = CategoryController()
  val orders = OrderController()
  val wishlists = WishlistController()

  override def munitTestTransforms: List[TestTransform] =
    super.munitTestTransforms :+ new TestTransform(
      "Slow",
      test => test.withTags(test.tags + new Tag("Slow"))
    )

  val pgContainer = ForAllContainerFixture(
    PostgreSQLContainer
      .Def(dockerImageName = DockerImageName.parse("postgres:17.0"))
      .createContainer()
  )

  override def munitFixtures: Seq[AnyFixture[?]] =
    super.munitFixtures :+ pgContainer

  def xa(): Transactor[Postgres] =
    val ds = PGSimpleDataSource()
    val pg = pgContainer()
    ds.setUrl(pg.jdbcUrl)
    ds.setUser(pg.username)
    ds.setPassword(pg.password)
    Transactor(Postgres, ds)

  // ===== Bootstrap =====

  test("1 - migrate and verify schema"):
    assertEquals(Database.migrate(xa()).appliedCount, 5)
    val status = Database.status(xa())
    assertEquals(status.applied.size, 5)
    assertEquals(status.pending.size, 0)
    xa().connect:
      Database.verifySchema().foreach: result =>
        assert(!result.hasErrors, s"Schema mismatch for ${result.entityName}:\n${result.prettyPrint}")

  test("2 - seed data"):
    xa().connect:
      SeedData.insertAll()

  // ===== Account flows =====

  test("3 - register and find by email"):
    xa().connect:
      val customer = accounts.register("Dave", "Wilson", "dave@example.com")
      assert(customer.id > 0L)
      val found = accounts.findByEmail("dave@example.com")
      assertEquals(found.get.firstName, "Dave")

  test("4 - update profile"):
    xa().connect:
      val saved = accounts.updateProfile(1L, "Johnson", "alice-new@example.com")
      assertEquals(saved.lastName, "Johnson")
      assertEquals(saved.email, "alice-new@example.com")
      // restore
      accounts.updateProfile(1L, "Smith", "alice@example.com")

  test("5 - search and lookup customers"):
    xa().connect:
      assert(accounts.searchCustomers("%ohnson%").nonEmpty || accounts.searchCustomers("%mith%").nonEmpty)
      assertEquals(accounts.lookupCustomers(Vector("Alice", "Bob")).size, 2)

  test("6 - deactivate and reactivate account"):
    xa().connect:
      accounts.deactivateAccount(3L)
      assert(accounts.findByEmail("charlie@example.com").isEmpty)
      assert(accounts.deactivatedAccounts().exists(_.id == 3L))
      val reactivated = accounts.reactivateAccount(3L)
      assertEquals(reactivated.firstName, "Charlie")
      assert(accounts.findByEmail("charlie@example.com").isDefined)

  test("7 - permanently delete account"):
    xa().connect:
      accounts.register("Temp", "User", "temp@example.com")
      val temp = accounts.findByEmail("temp@example.com").get
      accounts.deleteAccountPermanently(temp.id)
      assert(!accounts.allAccounts().exists(_.email == "temp@example.com"))

  test("8 - customer dashboard"):
    xa().connect:
      val (customer, customerOrders, wishlist) = accounts.dashboard(1L)
      assertEquals(customer.firstName, "Alice")
      assert(customerOrders.size >= 2)
      assertEquals(wishlist.size, 2)

  test("9 - active customers and customers with orders"):
    xa().connect:
      assertEquals(accounts.activeCustomers().size, 4)
      val withOrders = accounts.customersWithOrders()
      assert(withOrders.nonEmpty && withOrders.size <= 3)

  // ===== Catalog flows =====

  test("10 - browse catalog with keyset pagination"):
    xa().connect:
      val (active, total) = catalog.productCounts()
      assertEquals(active, 4L)
      assertEquals(total, 5L)
      // first page
      val page1 = catalog.browse(pageSize = 2)
      assertEquals(page1.items.map(_.name), Vector("MacBook Pro", "iPhone 15"))
      assert(page1.hasMore)
      // cursor to next page
      val page2 = catalog.browse(pageSize = 2, afterName = page1.nextKey)
      assertEquals(page2.items.map(_.name), Vector("Galaxy S24", "AirPods Pro"))
      assert(!page2.hasMore)

  test("10b - browse catalog with offset pagination"):
    xa().connect:
      val page = catalog.browsePaged(page = 1, pageSize = 2)
      assertEquals(page.items.size, 2)
      assertEquals(page.total, 4L)
      assert(page.hasNext)

  test("11 - product detail page"):
    xa().connect:
      val Some((product, variants, reviews)) = catalog.productDetail(1L): @unchecked
      assertEquals(product.name, "iPhone 15")
      assertEquals(variants.size, 2)
      assertEquals(reviews.size, 2)

  test("12 - product by name with variants"):
    xa().connect:
      val Some((product, variants)) = catalog.productByName("iPhone 15"): @unchecked
      assertEquals(product.name, "iPhone 15")
      assertEquals(variants.size, 2)

  test("13 - products with categories"):
    xa().connect:
      val results = catalog.productsWithCategories()
      assert(results.nonEmpty)
      assert(results.forall(_._2.isDefined))

  test("14 - products with review counts"):
    xa().connect:
      val results = catalog.productsWithReviewCounts()
      val iphoneReviews = results.find(_._1.name == "iPhone 15").get._2
      assertEquals(iphoneReviews, 2L)

  test("15 - search by tag"):
    xa().connect:
      val premium = catalog.searchByTag("premium")
      assert(premium.size >= 2 && premium.forall(_.tags.contains("premium")))

  test("16 - products needing reviews"):
    xa().connect:
      val needsReviews = catalog.productsNeedingReviews()
      assert(needsReviews.nonEmpty)
      assert(needsReviews.exists(_.name == "AirPods Pro"))

  test("17 - top rated reviews"):
    xa().connect:
      val topRated = catalog.topRatedReviews(4)
      assert(topRated.nonEmpty && topRated.forall(_.rating >= 4))

  // ===== Category management =====

  test("18 - create and remove category"):
    xa().connect:
      val category = categories.create("Temporary")
      assert(category.id > 0L)
      categories.remove(category.id)
      assert(!categories.listAll().exists(_.id == category.id))

  test("19 - bulk import and remove categories"):
    xa().connect:
      val afterImport = categories.bulkImport(Seq("Books", "Music", "Movies"))
      val imported = afterImport.filter(c => Set("Books", "Music", "Movies").contains(c.name))
      assertEquals(imported.size, 3)
      categories.bulkRemove(imported.map(_.id))
      assert(!categories.listAll().exists(c => Set("Books", "Music", "Movies").contains(c.name)))

  // ===== Order flows =====

  test("20 - place order with stock reservation"):
    xa().transact:
      val (order, items) = orders.placeOrder(
        customerId = 2L,
        items = Seq((3L, 1, BigDecimal("899.99"))),
        shippingStreet = "456 Oak Ave", shippingCity = "LA", shippingCountry = "US"
      )
      assertEquals(order.status, OrderStatus.Pending)
      assertEquals(items.size, 1)
      // verify stock was decremented
      val variant = ProductVariant.repo.findById(3L).get
      assertEquals(variant.stockQuantity, 39)

  test("21 - order details with customer and items"):
    xa().connect:
      val Some((order, customer, items)) = orders.orderDetails(1L): @unchecked
      assertEquals(customer.firstName, "Alice")
      assert(items.nonEmpty)

  test("22 - order ownership check"):
    xa().connect:
      assert(orders.isOrderOwner(1L, 1L))
      assert(!orders.isOrderOwner(1L, 2L))

  test("23 - restock variant"):
    xa().connect:
      val before = ProductVariant.repo.findById(1L).get.stockQuantity
      val after = orders.restockVariant(1L, 10)
      assertEquals(after.stockQuantity, before + 10)
      // restore
      ProductVariant.repo.query.where(_.id === 1L).decrement(_.stockQuantity, 10)

  test("24 - filter orders by status"):
    xa().connect:
      assertEquals(orders.byStatus(OrderStatus.Delivered).size, 1)
      assertEquals(orders.byStatus(OrderStatus.Shipped).size, 1)

  test("25 - all orders with customers"):
    xa().connect:
      val results = orders.allOrdersWithCustomers()
      assert(results.nonEmpty)
      assert(results.head._2.firstName.nonEmpty)

  test("26 - all orders with items"):
    xa().connect:
      val results = orders.allOrdersWithItems()
      assert(results.nonEmpty)
      assert(results.forall(_._2.nonEmpty))

  // ===== Wishlist =====

  test("27 - add to wishlist idempotent"):
    xa().connect:
      wishlists.addToWishlist(1L, 2L) // duplicate — no-op
      assert(wishlists.forCustomer(1L).size >= 2)

  // ===== Sales reports =====

  test("28 - sales summary"):
    xa().connect:
      val summary = orders.salesSummary()
      assert(summary.count >= 4L)
      assert(summary.revenue.isDefined)
      assert(summary.avgValue.isDefined)
      assert(summary.minOrder.isDefined && summary.maxOrder.isDefined)

  test("29 - top customers by revenue"):
    xa().connect:
      val results = orders.topCustomersByRevenue(BigDecimal("1000"))
      assert(results.nonEmpty)
      assert(results.forall(_.revenue > BigDecimal("1000")))

  test("30 - orders by status breakdown"):
    xa().connect:
      val statusCounts = orders.ordersByStatus().map(r => (r.status, r.count)).toMap
      assertEquals(statusCounts(OrderStatus.Delivered), 1L)
      assertEquals(statusCounts(OrderStatus.Shipped), 1L)

end ECommerceTests
