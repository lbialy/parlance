package ma.chinespirit.parlance.examples.ecommerce

import ma.chinespirit.parlance.*
import ma.chinespirit.parlance.pg.PgCodec.given
import ma.chinespirit.parlance.pg.enums.given
import ma.chinespirit.parlance.migrate.*

import java.time.OffsetDateTime

// ===== App bootstrap =====

object Database:
  val migrations = List(V1_Foundation, V2_Products, V3_Orders, V4_Social, V5_Indexes)

  def migrate(t: Transactor[Postgres]): MigrateResult =
    Migrator(migrations, t, PostgresCompiler).migrate()

  def status(t: Transactor[Postgres]): MigrationStatus =
    Migrator(migrations, t, PostgresCompiler).status()

  def verifySchema()(using DbCon[Postgres]): List[VerifyResult] =
    List(
      verify[Customer],
      verify[Category],
      verify[Product],
      verify[ProductVariant],
      verify[Order],
      verify[OrderItem],
      verify[Review],
      verify[Wishlist]
    )
end Database

// ===== Account management =====

class AccountController(using AppContext):

  def register(firstName: String, lastName: String, email: String)(using DbCon[Postgres]): Customer =
    CustomerCreator(firstName, lastName, email).create()

  def findByEmail(email: String)(using DbCon[Postgres]): Option[Customer] =
    Customer.repo.query.where(_.email === email).first()

  def searchCustomers(lastNamePattern: String)(using DbCon[Postgres]): Vector[Customer] =
    Customer.repo.query.where(_.lastName like lastNamePattern).run()

  def lookupCustomers(names: Vector[String])(using DbCon[Postgres]): Vector[Customer] =
    Customer.repo.query.where(_.firstName in names).run()

  /** Update profile */
  def updateProfile(customerId: Long, newLastName: String, newEmail: String)(using DbCon[Postgres]): Customer =
    val customer = Customer.repo
      .findById(customerId)
      .getOrElse(throw java.util.NoSuchElementException(s"Customer $customerId not found"))
    val updated = customer.copy(lastName = newLastName, email = newEmail)
    updated.save()
    updated.refresh()

  /** Deactivate account (soft delete) */
  def deactivateAccount(customerId: Long)(using DbCon[Postgres]): Unit =
    Customer.repo
      .findById(customerId)
      .getOrElse(throw java.util.NoSuchElementException(s"Customer $customerId not found"))
      .delete()

  /** Reactivate a previously deactivated account */
  def reactivateAccount(customerId: Long)(using DbCon[Postgres]): Customer =
    val deactivated = Customer.repo.withTrashed
      .run()
      .find(_.id == customerId)
      .getOrElse(throw java.util.NoSuchElementException(s"Customer $customerId not found"))
    require(deactivated.trashed, "Account is not deactivated")
    deactivated.restore()
    Customer.repo
      .findById(customerId)
      .getOrElse(throw java.util.NoSuchElementException(s"Customer $customerId not found after restore"))

  /** Permanently delete account — GDPR "right to be forgotten" */
  def deleteAccountPermanently(customerId: Long)(using DbCon[Postgres]): Unit =
    val customer = Customer.repo.withTrashed
      .run()
      .find(_.id == customerId)
      .getOrElse(throw java.util.NoSuchElementException(s"Customer $customerId not found"))
    Customer.repo.forceDelete(customer)

  /** Customer dashboard: profile, orders, wishlisted products */
  def dashboard(customerId: Long)(using DbCon[Postgres]): (Customer, Vector[Order], Vector[Product]) =
    val customer = Customer.repo
      .findById(customerId)
      .getOrElse(throw java.util.NoSuchElementException(s"Customer $customerId not found"))
    val orders = customer.load(Customer.orders)
    val wishlist = customer.load(Customer.wishlistedProducts)
    (customer, orders, wishlist)

  def activeCustomers()(using DbCon[Postgres]): Vector[Customer] =
    Customer.repo.findAll

  def customersWithOrders()(using DbCon[Postgres]): Vector[Customer] =
    Customer.repo.query.whereHas(Customer.orders).run()

  def deactivatedAccounts()(using DbCon[Postgres]): Vector[Customer] =
    Customer.repo.onlyTrashed.run()

  def allAccounts()(using DbCon[Postgres]): Vector[Customer] =
    Customer.repo.withTrashed.run()
end AccountController

// ===== Product catalog =====

class CatalogController(using AppContext):

  /** Browse active products with keyset pagination (cursor-based, efficient for large catalogs) */
  def browse(pageSize: Int, afterName: Option[String] = None)(using DbCon[Postgres]): KeysetPage[Product, String] =
    Product.repo.query
      .keysetPaginate(pageSize)(_.desc(_.name))
      .afterOpt(afterName)
      .run()

  /** Browse active products with offset pagination (simpler, good for admin UIs with page numbers) */
  def browsePaged(page: Int, pageSize: Int)(using DbCon[Postgres]): OffsetPage[Product] =
    Product.repo.query
      .orderBy(_.name, SortOrder.Desc, NullOrder.Last)
      .paginate(page, pageSize)

  /** Active vs total product counts */
  def productCounts()(using DbCon[Postgres]): (Long, Long) =
    val active = Product.repo.query.count()
    val total = Product.repo.queryUnscoped.count()
    (active, total)

  /** Product detail page: product with its variants and reviews */
  def productDetail(productId: Long)(using DbCon[Postgres]): Option[(Product, Vector[ProductVariant], Vector[Review])] =
    Product.repo
      .findById(productId)
      .map: product =>
        val variants = product.load(Product.variants)
        val reviews = product.load(Product.reviews)
        (product, variants, reviews)

  /** Product detail by name — e.g. for SEO-friendly URLs */
  def productByName(name: String)(using DbCon[Postgres]): Option[(Product, Vector[ProductVariant])] =
    Product.repo.query.where(_.name === name).withRelated(Product.variants).run().headOption

  /** Browse products with their categories (for navigation breadcrumbs) */
  def productsWithCategories()(using DbCon[Postgres]): Vector[(Product, Option[Category])] =
    Product.repo.query.leftJoin(Product.category).run()

  /** Products with review counts (for showing star ratings in listings) */
  def productsWithReviewCounts()(using DbCon[Postgres]): Vector[(Product, Long)] =
    Product.repo.query.orderBy(_.name).withCount(Product.reviews).run()

  /** Search products by tag */
  def searchByTag(tag: String)(using DbCon[Postgres]): Vector[Product] =
    sql"SELECT * FROM products WHERE $tag = ANY(tags) AND is_active = TRUE".query[Product].run()

  /** Products that haven't been reviewed yet — prompt customers to review */
  def productsNeedingReviews()(using DbCon[Postgres]): Vector[Product] =
    Product.repo.query.doesntHave(Product.reviews).run()

  /** Top-rated reviews (raw SQL for complex filtering) */
  def topRatedReviews(minRating: Int)(using DbCon[Postgres]): Vector[Review] =
    sql"SELECT * FROM reviews WHERE rating >= $minRating ORDER BY rating DESC"
      .query[Review]
      .run()
end CatalogController

// ===== Category management =====

class CategoryController(using AppContext):

  def create(name: String, parentId: Option[Long] = None)(using DbCon[Postgres]): Category =
    CategoryCreator(name, parentId).create()

  def remove(categoryId: Long)(using DbCon[Postgres]): Unit =
    Category.repo
      .findById(categoryId)
      .getOrElse(throw java.util.NoSuchElementException(s"Category $categoryId not found"))
      .delete()

  /** Bulk import categories and return the full list */
  def bulkImport(names: Seq[String])(using DbCon[Postgres]): Vector[Category] =
    Category.repo.rawInsertAll(names.map(CategoryCreator(_, None)))
    Category.repo.findAll

  /** Bulk remove categories by IDs */
  def bulkRemove(ids: Vector[Long])(using DbCon[Postgres]): Unit =
    Category.repo.deleteAllById(ids)

  def listAll()(using DbCon[Postgres]): Vector[Category] =
    Category.repo.findAll
end CategoryController

// ===== Order management =====

class OrderController(using AppContext):

  /** Place an order: create order + line items, reserve stock with row locks */
  def placeOrder(
      customerId: Long,
      items: Seq[(Long, Int, BigDecimal)],
      shippingStreet: String,
      shippingCity: String,
      shippingCountry: String
  )(using DbTx[Postgres]): (Order, Vector[OrderItem]) =
    val total = items.map((_, qty, price) => price * qty).sum
    val order = OrderCreator(
      customerId,
      OrderStatus.Pending,
      total,
      shippingStreet,
      shippingCity,
      shippingCountry,
      shippingStreet,
      shippingCity,
      shippingCountry
    ).create()

    val orderItems = items.map: (variantId, quantity, unitPrice) =>
      // Lock the variant row to prevent overselling
      val locked = ProductVariant.repo.query
        .where(_.id === variantId)
        .lockForUpdate
        .run()
        .headOption
        .getOrElse(throw java.util.NoSuchElementException(s"Variant $variantId not found"))
      require(locked.stockQuantity >= quantity, s"Insufficient stock for variant $variantId")
      ProductVariant.repo.query.where(_.id === variantId).decrement(_.stockQuantity, quantity)
      OrderItemCreator(order.id, variantId, quantity, unitPrice).create()

    (order, orderItems.toVector)
  end placeOrder

  /** View order with all line items */
  def orderDetails(orderId: Long)(using DbCon[Postgres]): Option[(Order, Customer, Vector[OrderItem])] =
    Order.repo
      .findById(orderId)
      .map: order =>
        val customer = order
          .loadOne(Order.customer)
          .getOrElse(throw java.util.NoSuchElementException(s"Order ${order.id} has no customer"))
        val items = order.load(Order.items)
        (order, customer, items)

  /** Check if a customer owns a given order (authorization) */
  def isOrderOwner(orderId: Long, customerId: Long)(using DbCon[Postgres]): Boolean =
    val order = Order.repo
      .findById(orderId)
      .getOrElse(throw java.util.NoSuchElementException(s"Order $orderId not found"))
    val orderOwner = order
      .loadOne(Order.customer)
      .getOrElse(throw java.util.NoSuchElementException(s"Order $orderId has no customer"))
    val requestingUser = Customer.repo
      .findById(customerId)
      .getOrElse(throw java.util.NoSuchElementException(s"Customer $customerId not found"))
    orderOwner.is(requestingUser)

  /** Restock a variant (e.g. new shipment arrived) */
  def restockVariant(variantId: Long, quantity: Int)(using DbCon[Postgres]): ProductVariant =
    ProductVariant.repo.query.where(_.id === variantId).increment(_.stockQuantity, quantity)
    ProductVariant.repo
      .findById(variantId)
      .getOrElse(throw java.util.NoSuchElementException(s"Variant $variantId not found"))
      .refresh()

  /** Filter orders by status */
  def byStatus(status: OrderStatus)(using DbCon[Postgres]): Vector[Order] =
    Order.repo.query.where(_.status === status).run()

  /** All orders with their customers (admin order list) */
  def allOrdersWithCustomers()(using DbCon[Postgres]): Vector[(Order, Customer)] =
    Order.repo.query.join(Order.customer).run()

  /** All orders with their items (admin detail view) */
  def allOrdersWithItems()(using DbCon[Postgres]): Vector[(Order, Vector[OrderItem])] =
    Order.repo.query.withRelated(Order.items).run()

  /** Sales dashboard: key metrics in one call */
  def salesSummary()(using DbCon[Postgres]) =
    val count = Order.repo.query.count()
    val revenue = Order.repo.query.sum(_.totalAmount)
    val avgValue = Order.repo.query.avg(_.totalAmount)
    val (minOrder, maxOrder) = (Order.repo.query.min(_.totalAmount), Order.repo.query.max(_.totalAmount))
    (count = count, revenue = revenue, avgValue = avgValue, minOrder = minOrder, maxOrder = maxOrder)

  /** Revenue per customer, filtered to high-value customers */
  def topCustomersByRevenue(minRevenue: BigDecimal)(using DbCon[Postgres]) =
    Order.repo.query
      .select(o => (customerId = o.customerId, revenue = Expr.sum(o.totalAmount)))
      .groupBy(_.customerId)
      .having(_.revenue > minRevenue)
      .run()

  /** Order distribution by status */
  def ordersByStatus()(using DbCon[Postgres]) =
    Order.repo.query
      .select(o => (status = o.status, count = Expr.count))
      .groupBy(_.status)
      .run()
end OrderController

// ===== Wishlist =====

class WishlistController(using AppContext):

  /** Add product to wishlist — idempotent (no error on duplicate) */
  def addToWishlist(customerId: Long, productId: Long)(using DbCon[Postgres]): Unit =
    Wishlist.repo.insertOnConflict(
      WishlistCreator(customerId, productId, OffsetDateTime.now()),
      ConflictTarget.Columns(Col[Long]("customer_id", "customer_id"), Col[Long]("product_id", "product_id")),
      ConflictAction.DoNothing
    )

  def forCustomer(customerId: Long)(using DbCon[Postgres]): Vector[Wishlist] =
    Wishlist.repo.query.where(_.customerId === customerId).run()
