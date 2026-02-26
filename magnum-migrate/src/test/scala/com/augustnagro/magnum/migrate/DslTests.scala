package com.augustnagro.magnum.migrate

import munit.FunSuite
import com.augustnagro.magnum.DbCodec

class DslTests extends FunSuite:

  // ---- column[T] inference ----

  test("column[Long] infers BigInt"):
    val col = column[Long]("id")
    assertEquals(col.columnType, ColumnType.BigInt)

  test("column[Int] infers Integer"):
    val col = column[Int]("count")
    assertEquals(col.columnType, ColumnType.Integer)

  test("column[Short] infers SmallInt"):
    val col = column[Short]("small")
    assertEquals(col.columnType, ColumnType.SmallInt)

  test("column[String] infers Text"):
    val col = column[String]("name")
    assertEquals(col.columnType, ColumnType.Text)

  test("column[Boolean] infers Boolean"):
    val col = column[Boolean]("active")
    assertEquals(col.columnType, ColumnType.Boolean)

  test("column[Double] infers DoublePrecision"):
    val col = column[Double]("amount")
    assertEquals(col.columnType, ColumnType.DoublePrecision)

  test("column[Float] infers Real"):
    val col = column[Float]("score")
    assertEquals(col.columnType, ColumnType.Real)

  test("column[BigDecimal] infers Numeric"):
    val col = column[BigDecimal]("price")
    assertEquals(col.columnType, ColumnType.Numeric(18, 2))

  test("column[java.time.LocalDate] infers Date"):
    val col = column[java.time.LocalDate]("birth_date")
    assertEquals(col.columnType, ColumnType.Date)

  test("column[java.time.LocalTime] infers Time"):
    val col = column[java.time.LocalTime]("start_time")
    assertEquals(col.columnType, ColumnType.Time())

  test("column[java.time.LocalDateTime] infers Timestamp"):
    val col = column[java.time.LocalDateTime]("created")
    assertEquals(col.columnType, ColumnType.Timestamp())

  test("column[java.time.Instant] infers TimestampTz"):
    val col = column[java.time.Instant]("created_at")
    assertEquals(col.columnType, ColumnType.TimestampTz())

  test("column[java.util.UUID] infers Uuid"):
    val col = column[java.util.UUID]("uuid")
    assertEquals(col.columnType, ColumnType.Uuid)

  // ---- type override methods ----

  test("column.varchar overrides type"):
    val col = column[String]("email").varchar(255)
    assertEquals(col.columnType, ColumnType.Varchar(255))

  test("column.decimal overrides type"):
    val col = column[BigDecimal]("price").decimal(10, 4)
    assertEquals(col.columnType, ColumnType.Numeric(10, 4))

  test("column.customType overrides type"):
    val col = column[String]("data").customType("vector(1536)")
    assertEquals(col.columnType, ColumnType.Custom("vector(1536)"))

  // ---- modifier chaining ----

  test("modifier chaining is immutable"):
    val base = column[String]("name")
    val withNullable = base.nullable
    val withUnique = base.unique
    assert(!base.modifiers.nullable)
    assert(!base.modifiers.unique)
    assert(withNullable.modifiers.nullable)
    assert(!withNullable.modifiers.unique)
    assert(withUnique.modifiers.unique)
    assert(!withUnique.modifiers.nullable)

  test("multiple modifiers chain"):
    val col = column[String]("email").varchar(255).unique.check("email LIKE '%@%'")
    assertEquals(col.columnType, ColumnType.Varchar(255))
    assert(col.modifiers.unique)
    assertEquals(col.modifiers.check, Some("email LIKE '%@%'"))

  // ---- convenience helpers ----

  test("id() produces correct ColumnDef"):
    val col = id()
    assertEquals(col.name, "id")
    assertEquals(col.columnType, ColumnType.BigInt)
    assert(col.modifiers.primaryKey)
    assert(col.modifiers.autoIncrement)

  test("id(name) uses custom name"):
    val col = id("user_id")
    assertEquals(col.name, "user_id")
    assert(col.modifiers.primaryKey)

  test("timestamps() produces two columns"):
    val cols = timestamps()
    assertEquals(cols.size, 2)
    assertEquals(cols(0).name, "created_at")
    assertEquals(cols(1).name, "updated_at")
    assertEquals(cols(0).columnType, ColumnType.TimestampTz())
    assertEquals(cols(1).columnType, ColumnType.TimestampTz())

  test("softDelete() produces nullable column"):
    val col = softDelete()
    assertEquals(col.name, "deleted_at")
    assert(col.modifiers.nullable)

  // ---- DSL builder functions ----

  test("createTable produces Migration.CreateTable"):
    val m = createTable("users")(id(), column[String]("email"))
    assert(m.isInstanceOf[Migration.CreateTable])
    val ct = m.asInstanceOf[Migration.CreateTable]
    assertEquals(ct.name, "users")
    assertEquals(ct.columns.size, 2)

  test("createTable with options"):
    val m = createTable("temp", TableOptions(temporary = true))(column[Int]("x"))
    val ct = m.asInstanceOf[Migration.CreateTable]
    assert(ct.options.temporary)

  test("dropTable produces Migration.DropTable"):
    val m = dropTable("users")
    assertEquals(m, Migration.DropTable("users"))

  test("dropTableIfExists produces Migration.DropTableIfExists"):
    val m = dropTableIfExists("users")
    assertEquals(m, Migration.DropTableIfExists("users"))

  test("renameTable produces Migration.RenameTable"):
    val m = renameTable("old", "new_t")
    assertEquals(m, Migration.RenameTable("old", "new_t"))

  test("alterTable produces Migration.AlterTable"):
    val m = alterTable("users")(dropColumn("bio"), addIndex("email"))
    val at = m.asInstanceOf[Migration.AlterTable]
    assertEquals(at.name, "users")
    assertEquals(at.ops.size, 2)

  test("createEnumType produces Migration.CreateEnumType"):
    val m = createEnumType("role", "admin", "member")
    val ce = m.asInstanceOf[Migration.CreateEnumType]
    assertEquals(ce.name, "role")
    assertEquals(ce.values, List("admin", "member"))

  test("raw produces Migration.Raw"):
    assertEquals(raw("SELECT 1"), Migration.Raw("SELECT 1"))

  // ---- alter op helpers ----

  test("addIndex produces AddIndex"):
    val op = addIndex("email", "name")
    assertEquals(op.columns, List("email", "name"))
    assert(!op.unique)

  test("addUniqueIndex produces unique AddIndex"):
    val op = addUniqueIndex("email")
    assert(op.unique)

  test("addForeignKey produces AddForeignKey"):
    val op = addForeignKey("user_id", "users", "id", FkAction.Cascade)
    assertEquals(op.columns, List("user_id"))
    assertEquals(op.refTable, "users")
    assertEquals(op.onDelete, FkAction.Cascade)

  test("Option[T] column is nullable by default via inference"):
    val col = column[Option[java.time.Instant]]("deleted_at").nullable
    assert(col.modifiers.nullable)
