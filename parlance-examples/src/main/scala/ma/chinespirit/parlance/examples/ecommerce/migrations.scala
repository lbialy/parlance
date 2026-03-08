package ma.chinespirit.parlance.examples.ecommerce

import ma.chinespirit.parlance.migrate.*

object V1_Foundation extends MigrationDef:
  val version = 1L
  val name = "foundation"
  val up = List(
    createEnumType(
      "order_status",
      "Pending",
      "Confirmed",
      "Shipped",
      "Delivered",
      "Cancelled"
    ),
    createTable("customers")(
      (Seq(
        id(),
        column[String]("first_name").varchar(100),
        column[String]("last_name").varchar(100),
        column[String]("email").varchar(255).unique
      ) ++ timestamps() :+ softDelete())*
    ),
    createTable("categories")(
      id(),
      column[String]("name").varchar(100),
      column[Option[Long]]("parent_id").nullable.references("categories", "id", FkAction.SetNull, FkAction.NoAction)
    )
  )
  val down = List(
    dropTable("categories"),
    dropTable("customers"),
    dropEnumType("order_status")
  )

object V2_Products extends MigrationDef:
  val version = 2L
  val name = "products"
  val up = List(
    createTable("products")(
      id(),
      column[String]("name").varchar(200),
      column[String]("description"),
      column[Long]("category_id").references("categories", "id", FkAction.Restrict, FkAction.NoAction),
      ColumnDef[String]("tags", ColumnType.Custom("TEXT[]")),
      column[Boolean]("is_active").defaultExpression("TRUE")
    ),
    createTable("product_variants")(
      id(),
      column[Long]("product_id").references("products", "id", FkAction.Cascade, FkAction.NoAction),
      column[String]("sku").varchar(50).unique,
      column[Option[String]]("size").nullable.varchar(20),
      column[Option[String]]("color").nullable.varchar(30),
      column[BigDecimal]("price").decimal(10, 2),
      column[Int]("stock_quantity").default(0)
    )
  )
  val down = List(
    dropTable("product_variants"),
    dropTable("products")
  )

object V3_Orders extends MigrationDef:
  val version = 3L
  val name = "orders"
  val up = List(
    createTable("orders")(
      id(),
      column[Long]("customer_id").references("customers", "id", FkAction.Restrict, FkAction.NoAction),
      ColumnDef[String]("status", ColumnType.PgEnum("order_status")).defaultExpression("'Pending'"),
      column[BigDecimal]("total_amount").decimal(12, 2),
      column[String]("shipping_street").varchar(255),
      column[String]("shipping_city").varchar(100),
      column[String]("shipping_country").varchar(100),
      column[String]("billing_street").varchar(255),
      column[String]("billing_city").varchar(100),
      column[String]("billing_country").varchar(100),
      column[java.time.Instant]("created_at").defaultExpression("CURRENT_TIMESTAMP")
    ),
    createTable("order_items")(
      id(),
      column[Long]("order_id").references("orders", "id", FkAction.Cascade, FkAction.NoAction),
      column[Long]("variant_id").references("product_variants", "id", FkAction.Restrict, FkAction.NoAction),
      column[Int]("quantity").check("quantity > 0"),
      column[BigDecimal]("unit_price").decimal(10, 2).check("unit_price >= 0")
    )
  )
  val down = List(
    dropTable("order_items"),
    dropTable("orders")
  )

object V4_Social extends MigrationDef:
  val version = 4L
  val name = "social"
  val up = List(
    createTable("reviews")(
      id(),
      column[Long]("product_id").references("products", "id", FkAction.Cascade, FkAction.NoAction),
      column[Long]("customer_id").references("customers", "id", FkAction.Cascade, FkAction.NoAction),
      column[Int]("rating").check("rating >= 1 AND rating <= 5"),
      column[String]("title").varchar(200),
      column[String]("body"),
      column[Boolean]("is_verified").defaultExpression("FALSE"),
      column[java.time.Instant]("created_at").defaultExpression("CURRENT_TIMESTAMP")
    ),
    createTable("wishlists")(
      id(),
      column[Long]("customer_id").references("customers", "id", FkAction.Cascade, FkAction.NoAction),
      column[Long]("product_id").references("products", "id", FkAction.Cascade, FkAction.NoAction),
      column[java.time.Instant]("added_at").defaultExpression("CURRENT_TIMESTAMP")
    ),
    alterTable("wishlists")(
      addUniqueIndex("customer_id", "product_id")
    )
  )
  val down = List(
    dropTable("wishlists"),
    dropTable("reviews")
  )

object V5_Indexes extends MigrationDef:
  val version = 5L
  val name = "indexes"
  val up = List(
    alterTable("orders")(
      addIndex("customer_id")
    ),
    alterTable("order_items")(
      addIndex("order_id"),
      addIndex("variant_id")
    ),
    alterTable("reviews")(
      addIndex("product_id"),
      addIndex("customer_id")
    ),
    alterTable("wishlists")(
      addIndex("customer_id"),
      addIndex("product_id")
    ),
    alterTable("product_variants")(
      addIndex("product_id")
    ),
    alterTable("products")(
      AlterOp.AddIndex(
        columns = List("tags"),
        using = Some(IndexMethod.Gin)
      )
    )
  )
  val down = List.empty
