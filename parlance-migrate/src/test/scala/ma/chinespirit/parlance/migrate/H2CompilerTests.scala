package ma.chinespirit.parlance.migrate

import munit.FunSuite

class H2CompilerTests extends FunSuite:
  import H2Compiler.compile

  // ---- CREATE TABLE ----

  test("CREATE TABLE with basic columns"):
    val m = Migration.CreateTable(
      "users",
      List(
        ColumnDef[Long]("id", ColumnType.BigInt, ColumnModifiers(primaryKey = true, autoIncrement = true)),
        ColumnDef[String]("email", ColumnType.Varchar(255), ColumnModifiers(unique = true)),
        ColumnDef[String]("name", ColumnType.Text),
        ColumnDef[Boolean]("active", ColumnType.Boolean, ColumnModifiers(default = Some(DefaultValue.Literal(true))))
      )
    )
    val sqls = compile(m)
    assertEquals(sqls.size, 1)
    val sql = sqls.head
    assert(sql.contains("id BIGINT AUTO_INCREMENT NOT NULL PRIMARY KEY"), sql)
    assert(sql.contains("email VARCHAR(255) NOT NULL UNIQUE"), sql)
    assert(sql.contains("name VARCHAR NOT NULL"), sql)
    assert(sql.contains("active BOOLEAN NOT NULL DEFAULT TRUE"), sql)

  test("CREATE TABLE with TEMPORARY and IF NOT EXISTS"):
    val m = Migration.CreateTable(
      "temp",
      List(ColumnDef[Int]("x", ColumnType.Integer)),
      TableOptions(temporary = true, ifNotExists = true)
    )
    val sql = compile(m).head
    assert(sql.startsWith("CREATE TEMPORARY TABLE IF NOT EXISTS temp"), sql)

  test("CREATE TABLE ignores UNLOGGED"):
    val m = Migration.CreateTable(
      "data",
      List(ColumnDef[Int]("x", ColumnType.Integer)),
      TableOptions(unlogged = true)
    )
    val sql = compile(m).head
    assert(!sql.contains("UNLOGGED"), sql)

  // ---- TYPE DIFFERENCES ----

  test("H2 type mappings"):
    assertEquals(H2Compiler.compileType(ColumnType.Text), "VARCHAR")
    assertEquals(H2Compiler.compileType(ColumnType.Bytea), "BINARY")
    assertEquals(H2Compiler.compileType(ColumnType.Jsonb), "JSON")
    assertEquals(H2Compiler.compileType(ColumnType.TimestampTz(6)), "TIMESTAMP(6) WITH TIME ZONE")
    assertEquals(H2Compiler.compileType(ColumnType.TimeTz(3)), "TIME(3) WITH TIME ZONE")
    assertEquals(H2Compiler.compileType(ColumnType.PgEnum("status")), "VARCHAR(255)")
    assertEquals(H2Compiler.compileType(ColumnType.Inet), "VARCHAR(45)")
    assertEquals(H2Compiler.compileType(ColumnType.MacAddr), "VARCHAR(17)")
    assertEquals(H2Compiler.compileType(ColumnType.Money), "NUMERIC(19, 2)")
    assertEquals(H2Compiler.compileType(ColumnType.ArrayOf(ColumnType.Integer)), "INTEGER ARRAY")

  test("H2 AUTO_INCREMENT instead of SERIAL"):
    assertEquals(H2Compiler.compileType(ColumnType.BigSerial), "BIGINT AUTO_INCREMENT")
    assertEquals(H2Compiler.compileType(ColumnType.Serial), "INTEGER AUTO_INCREMENT")
    assertEquals(H2Compiler.compileType(ColumnType.SmallSerial), "SMALLINT AUTO_INCREMENT")

  // ---- ENUM OPERATIONS SKIPPED ----

  test("enum operations produce no SQL in H2"):
    assertEquals(compile(Migration.CreateEnumType("role", List("a", "b"))), Nil)
    assertEquals(compile(Migration.DropEnumType("role")), Nil)
    assertEquals(compile(Migration.AddEnumValue("role", "c")), Nil)
    assertEquals(compile(Migration.RenameEnumValue("role", "a", "x")), Nil)

  // ---- EXTENSION OPERATIONS SKIPPED ----

  test("extension operations produce no SQL in H2"):
    assertEquals(compile(Migration.CreateExtension("uuid-ossp")), Nil)
    assertEquals(compile(Migration.DropExtension("uuid-ossp")), Nil)

  // ---- ALTER TABLE ----

  test("ALTER TABLE ADD COLUMN not batched in H2"):
    val m = Migration.AlterTable(
      "users",
      List(
        AlterOp.AddColumn(ColumnDef[String]("bio", ColumnType.Text, ColumnModifiers(nullable = true))),
        AlterOp.AddColumn(ColumnDef[String]("url", ColumnType.Varchar(500), ColumnModifiers(nullable = true)))
      )
    )
    val sqls = compile(m)
    assertEquals(sqls.size, 2)
    assert(sqls(0).contains("ADD COLUMN bio VARCHAR"), sqls(0))
    assert(sqls(1).contains("ADD COLUMN url VARCHAR(500)"), sqls(1))

  test("ALTER TABLE RENAME COLUMN uses H2 syntax"):
    val m = Migration.AlterTable("users", List(AlterOp.RenameColumn("name", "full_name")))
    assertEquals(compile(m), List("ALTER TABLE users ALTER COLUMN name RENAME TO full_name"))

  test("ALTER COLUMN TYPE uses SET DATA TYPE"):
    val m = Migration.AlterTable("users", List(AlterOp.AlterColumnType("age", ColumnType.BigInt)))
    assertEquals(compile(m), List("ALTER TABLE users ALTER COLUMN age SET DATA TYPE BIGINT"))

  test("DROP NOT NULL uses SET NULL"):
    val m = Migration.AlterTable("users", List(AlterOp.DropNotNull("email")))
    assertEquals(compile(m), List("ALTER TABLE users ALTER COLUMN email SET NULL"))

  test("DROP COLUMN DEFAULT uses SET DEFAULT NULL"):
    val m = Migration.AlterTable("users", List(AlterOp.DropColumnDefault("active")))
    assertEquals(compile(m), List("ALTER TABLE users ALTER COLUMN active SET DEFAULT NULL"))

  test("INDEX ignores CONCURRENTLY, USING, and WHERE"):
    val m = Migration.AlterTable(
      "users",
      List(
        AlterOp.AddIndex(
          List("email"),
          concurrently = true,
          using = Some(IndexMethod.Gin),
          where = Some("active = true")
        )
      )
    )
    val sql = compile(m).head
    assertEquals(sql, "CREATE INDEX users_email_idx ON users (email)")

  test("DROP INDEX CONCURRENTLY just drops normally"):
    val m = Migration.AlterTable("users", List(AlterOp.DropIndexConcurrently("users_email_idx")))
    assertEquals(compile(m), List("DROP INDEX users_email_idx"))

  // ---- RAW SQL ----

  test("Raw SQL passthrough"):
    assertEquals(compile(Migration.Raw("INSERT INTO foo VALUES (1)")), List("INSERT INTO foo VALUES (1)"))

  // ---- COMMENTS ----

  test("H2 supports COMMENT ON TABLE"):
    val m = Migration.CreateTable(
      "users",
      List(ColumnDef[Long]("id", ColumnType.BigInt)),
      TableOptions(comment = Some("User table"))
    )
    val sqls = compile(m)
    assertEquals(sqls.size, 2)
    assertEquals(sqls(1), "COMMENT ON TABLE users IS 'User table'")

  test("H2 supports COMMENT ON COLUMN"):
    val m = Migration.AlterTable("users", List(AlterOp.SetColumnComment("email", "Primary email")))
    assertEquals(compile(m), List("COMMENT ON COLUMN users.email IS 'Primary email'"))
end H2CompilerTests
