package ma.chinespirit.parlance.examples.ecommerce

import ma.chinespirit.parlance.*
import ma.chinespirit.parlance.pg.PgCodec.given
import ma.chinespirit.parlance.pg.enums.given

import java.time.OffsetDateTime

// --- OrderStatus PG enum ---

@SqlName("order_status")
enum OrderStatus:
  case Pending, Confirmed, Shipped, Delivered, Cancelled

// --- Customer ---

@SqlName("customers")
@Table(SqlNameMapper.CamelToSnakeCase)
case class CustomerCreator(
    firstName: String,
    lastName: String,
    email: String
) extends CreatorOf[Customer]
    derives DbCodec

@SqlName("customers")
@Table(SqlNameMapper.CamelToSnakeCase)
case class Customer(
    @Id id: Long,
    firstName: String,
    lastName: String,
    email: String,
    @createdAt createdAt: OffsetDateTime,
    @updatedAt updatedAt: OffsetDateTime,
    @deletedAt deletedAt: Option[OffsetDateTime]
) derives EntityMeta,
      HasCreatedAt,
      HasUpdatedAt,
      HasDeletedAt

object Customer:
  val orders = Relationship.hasMany[Customer, Order](_.id, _.customerId)
  val reviews = Relationship.hasMany[Customer, Review](_.id, _.customerId)
  val wishlistedProducts =
    Relationship.belongsToMany[Customer, Product]("wishlists", "customer_id", "product_id")
  given repo: (Repo[CustomerCreator, Customer, Long] & SoftDeletes[CustomerCreator, Customer, Long]) =
    new Repo[CustomerCreator, Customer, Long] with SoftDeletes[CustomerCreator, Customer, Long]

// --- Category ---

@SqlName("categories")
@Table(SqlNameMapper.CamelToSnakeCase)
case class CategoryCreator(
    name: String,
    parentId: Option[Long]
) extends CreatorOf[Category]
    derives DbCodec

@SqlName("categories")
@Table(SqlNameMapper.CamelToSnakeCase)
case class Category(
    @Id id: Long,
    name: String,
    parentId: Option[Long]
) derives EntityMeta

object Category:
  val products = Relationship.hasMany[Category, Product](_.id, _.categoryId)
  val children = Relationship.hasMany[Category, Category](_.id, _.parentId)
  val parent = Relationship.belongsTo[Category, Category](_.parentId, _.id)
  given repo: Repo[CategoryCreator, Category, Long] = Repo[CategoryCreator, Category, Long]()

// --- Product ---

@SqlName("products")
@Table(SqlNameMapper.CamelToSnakeCase)
case class ProductCreator(
    name: String,
    description: String,
    categoryId: Long,
    tags: Vector[String],
    isActive: Boolean
) extends CreatorOf[Product]
    derives DbCodec

@SqlName("products")
@Table(SqlNameMapper.CamelToSnakeCase)
case class Product(
    @Id id: Long,
    name: String,
    description: String,
    categoryId: Long,
    tags: Vector[String],
    isActive: Boolean
) derives EntityMeta

object Product:
  val category = Relationship.belongsTo[Product, Category](_.categoryId, _.id)
  val variants = Relationship.hasMany[Product, ProductVariant](_.id, _.productId)
  val reviews = Relationship.hasMany[Product, Review](_.id, _.productId)

  private val activeScope: Scope[Product] = new Scope[Product]:
    override def conditions(meta: TableMeta[Product]): Vector[WhereFrag] =
      Vector(WhereFrag(Frag("is_active = TRUE", Seq.empty, FragWriter.empty)))

  given repo: Repo[ProductCreator, Product, Long] = Repo[ProductCreator, Product, Long](injectedScopes = Vector(activeScope))

// --- ProductVariant ---

@SqlName("product_variants")
@Table(SqlNameMapper.CamelToSnakeCase)
case class VariantCreator(
    productId: Long,
    sku: String,
    size: Option[String],
    color: Option[String],
    price: BigDecimal,
    stockQuantity: Int
) extends CreatorOf[ProductVariant]
    derives DbCodec

@SqlName("product_variants")
@Table(SqlNameMapper.CamelToSnakeCase)
case class ProductVariant(
    @Id id: Long,
    productId: Long,
    sku: String,
    size: Option[String],
    color: Option[String],
    price: BigDecimal,
    stockQuantity: Int
) derives EntityMeta

object ProductVariant:
  val product = Relationship.belongsTo[ProductVariant, Product](_.productId, _.id)
  given repo: Repo[VariantCreator, ProductVariant, Long] = Repo[VariantCreator, ProductVariant, Long]()

// --- Order ---

@SqlName("orders")
@Table(SqlNameMapper.CamelToSnakeCase)
case class OrderCreator(
    customerId: Long,
    status: OrderStatus,
    totalAmount: BigDecimal,
    shippingStreet: String,
    shippingCity: String,
    shippingCountry: String,
    billingStreet: String,
    billingCity: String,
    billingCountry: String
) extends CreatorOf[Order]
    derives DbCodec

@SqlName("orders")
@Table(SqlNameMapper.CamelToSnakeCase)
case class Order(
    @Id id: Long,
    customerId: Long,
    status: OrderStatus,
    totalAmount: BigDecimal,
    shippingStreet: String,
    shippingCity: String,
    shippingCountry: String,
    billingStreet: String,
    billingCity: String,
    billingCountry: String,
    @createdAt createdAt: OffsetDateTime
) derives EntityMeta,
      HasCreatedAt

object Order:
  val customer = Relationship.belongsTo[Order, Customer](_.customerId, _.id)
  val items = Relationship.hasMany[Order, OrderItem](_.id, _.orderId)
  inline given repo(using ctx: AppContext): Repo[OrderCreator, Order, Long] = ctx.orderRepo

// --- OrderItem ---

@SqlName("order_items")
@Table(SqlNameMapper.CamelToSnakeCase)
case class OrderItemCreator(
    orderId: Long,
    variantId: Long,
    quantity: Int,
    unitPrice: BigDecimal
) extends CreatorOf[OrderItem]
    derives DbCodec

@SqlName("order_items")
@Table(SqlNameMapper.CamelToSnakeCase)
case class OrderItem(
    @Id id: Long,
    orderId: Long,
    variantId: Long,
    quantity: Int,
    unitPrice: BigDecimal
) derives EntityMeta

object OrderItem:
  val order = Relationship.belongsTo[OrderItem, Order](_.orderId, _.id)
  val variant = Relationship.belongsTo[OrderItem, ProductVariant](_.variantId, _.id)
  given repo: Repo[OrderItemCreator, OrderItem, Long] = Repo[OrderItemCreator, OrderItem, Long]()

// --- Review ---

@SqlName("reviews")
@Table(SqlNameMapper.CamelToSnakeCase)
case class ReviewCreator(
    productId: Long,
    customerId: Long,
    rating: Int,
    title: String,
    body: String,
    isVerified: Boolean
) extends CreatorOf[Review]
    derives DbCodec

@SqlName("reviews")
@Table(SqlNameMapper.CamelToSnakeCase)
case class Review(
    @Id id: Long,
    productId: Long,
    customerId: Long,
    rating: Int,
    title: String,
    body: String,
    isVerified: Boolean,
    @createdAt createdAt: OffsetDateTime
) derives EntityMeta,
      HasCreatedAt

object Review:
  val product = Relationship.belongsTo[Review, Product](_.productId, _.id)
  val customer = Relationship.belongsTo[Review, Customer](_.customerId, _.id)
  given repo: Repo[ReviewCreator, Review, Long] = Repo[ReviewCreator, Review, Long]()

// --- Wishlist ---

@SqlName("wishlists")
@Table(SqlNameMapper.CamelToSnakeCase)
case class WishlistCreator(
    customerId: Long,
    productId: Long,
    addedAt: OffsetDateTime
) extends CreatorOf[Wishlist]
    derives DbCodec

@SqlName("wishlists")
@Table(SqlNameMapper.CamelToSnakeCase)
case class Wishlist(
    @Id id: Long,
    customerId: Long,
    productId: Long,
    addedAt: OffsetDateTime
) derives EntityMeta

object Wishlist:
  given repo: Repo[WishlistCreator, Wishlist, Long] = Repo[WishlistCreator, Wishlist, Long]()
