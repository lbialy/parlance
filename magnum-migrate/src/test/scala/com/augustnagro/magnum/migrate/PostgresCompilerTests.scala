package com.augustnagro.magnum.migrate

import munit.FunSuite

class PostgresCompilerTests extends FunSuite:
  import PostgresCompiler.compile

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
    assert(sql.contains("CREATE TABLE users ("), sql)
    assert(sql.contains("id BIGSERIAL NOT NULL PRIMARY KEY"), sql)
    assert(sql.contains("email VARCHAR(255) NOT NULL UNIQUE"), sql)
    assert(sql.contains("name TEXT NOT NULL"), sql)
    assert(sql.contains("active BOOLEAN NOT NULL DEFAULT TRUE"), sql)

  test("CREATE TABLE with nullable column"):
    val m = Migration.CreateTable(
      "posts",
      List(
        ColumnDef[String]("bio", ColumnType.Text, ColumnModifiers(nullable = true))
      )
    )
    val sql = compile(m).head
    assert(sql.contains("bio TEXT"), sql)
    assert(!sql.contains("NOT NULL"), sql)

  test("CREATE TABLE with table options"):
    val m = Migration.CreateTable(
      "temp_data",
      List(ColumnDef[Int]("x", ColumnType.Integer)),
      TableOptions(temporary = true, ifNotExists = true, unlogged = true, tablespace = Some("fast_ssd"))
    )
    val sql = compile(m).head
    assert(sql.startsWith("CREATE TEMPORARY UNLOGGED TABLE IF NOT EXISTS temp_data"), sql)
    assert(sql.contains("TABLESPACE fast_ssd"), sql)

  test("CREATE TABLE with table comment"):
    val m = Migration.CreateTable(
      "accounts",
      List(ColumnDef[Long]("id", ColumnType.BigInt)),
      TableOptions(comment = Some("User accounts"))
    )
    val sqls = compile(m)
    assertEquals(sqls.size, 2)
    assertEquals(sqls(1), "COMMENT ON TABLE accounts IS 'User accounts'")

  test("CREATE TABLE with column comment"):
    val m = Migration.CreateTable(
      "items",
      List(
        ColumnDef[String]("code", ColumnType.Varchar(10), ColumnModifiers(comment = Some("Item code")))
      )
    )
    val sqls = compile(m)
    assertEquals(sqls.size, 2)
    assertEquals(sqls(1), "COMMENT ON COLUMN items.code IS 'Item code'")

  test("CREATE TABLE with inline references"):
    val m = Migration.CreateTable(
      "posts",
      List(
        ColumnDef[Long](
          "author_id",
          ColumnType.BigInt,
          ColumnModifiers(references = Some(InlineReference("users", "id", FkAction.Cascade, FkAction.NoAction)))
        )
      )
    )
    val sql = compile(m).head
    assert(sql.contains("REFERENCES users(id) ON DELETE CASCADE"), sql)
    assert(!sql.contains("ON UPDATE"), sql)

  test("CREATE TABLE with check constraint"):
    val m = Migration.CreateTable(
      "products",
      List(
        ColumnDef[BigDecimal]("price", ColumnType.Numeric(10, 2), ColumnModifiers(check = Some("price > 0")))
      )
    )
    val sql = compile(m).head
    assert(sql.contains("CHECK (price > 0)"), sql)

  test("CREATE TABLE with collation"):
    val m = Migration.CreateTable(
      "names",
      List(
        ColumnDef[String]("name", ColumnType.Text, ColumnModifiers(collation = Some("en_US")))
      )
    )
    val sql = compile(m).head
    assert(sql.contains("""COLLATE "en_US""""), sql)

  test("CREATE TABLE with generated column"):
    val m = Migration.CreateTable(
      "items",
      List(
        ColumnDef[String]("full_name", ColumnType.Text, ColumnModifiers(generatedAs = Some("first_name || ' ' || last_name")))
      )
    )
    val sql = compile(m).head
    assert(sql.contains("GENERATED ALWAYS AS (first_name || ' ' || last_name) STORED"), sql)

  test("CREATE TABLE with default expression"):
    val m = Migration.CreateTable(
      "events",
      List(
        ColumnDef[java.util.UUID](
          "id",
          ColumnType.Uuid,
          ColumnModifiers(default = Some(DefaultValue.Expression("gen_random_uuid()")), primaryKey = true)
        )
      )
    )
    val sql = compile(m).head
    assert(sql.contains("DEFAULT gen_random_uuid()"), sql)

  // ---- AUTO-INCREMENT ----

  test("auto-increment promotes BigInt to BIGSERIAL"):
    val col = ColumnDef[Long]("id", ColumnType.BigInt, ColumnModifiers(autoIncrement = true))
    val sql = PostgresCompiler.compileColumnDef(col)
    assert(sql.startsWith("id BIGSERIAL"), sql)

  test("auto-increment promotes Integer to SERIAL"):
    val col = ColumnDef[Int]("id", ColumnType.Integer, ColumnModifiers(autoIncrement = true))
    val sql = PostgresCompiler.compileColumnDef(col)
    assert(sql.startsWith("id SERIAL"), sql)

  test("auto-increment promotes SmallInt to SMALLSERIAL"):
    val col = ColumnDef[Short]("id", ColumnType.SmallInt, ColumnModifiers(autoIncrement = true))
    val sql = PostgresCompiler.compileColumnDef(col)
    assert(sql.startsWith("id SMALLSERIAL"), sql)

  // ---- DROP TABLE ----

  test("DROP TABLE"):
    assertEquals(compile(Migration.DropTable("users")), List("DROP TABLE users"))

  test("DROP TABLE IF EXISTS"):
    assertEquals(compile(Migration.DropTableIfExists("users")), List("DROP TABLE IF EXISTS users"))

  // ---- RENAME TABLE ----

  test("RENAME TABLE"):
    assertEquals(compile(Migration.RenameTable("old", "new_table")), List("ALTER TABLE old RENAME TO new_table"))

  // ---- ALTER TABLE ----

  test("ALTER TABLE ADD COLUMN batching"):
    val m = Migration.AlterTable(
      "users",
      List(
        AlterOp.AddColumn(ColumnDef[String]("bio", ColumnType.Text, ColumnModifiers(nullable = true))),
        AlterOp.AddColumn(ColumnDef[String]("url", ColumnType.Varchar(500), ColumnModifiers(nullable = true)))
      )
    )
    val sqls = compile(m)
    assertEquals(sqls.size, 1)
    val sql = sqls.head
    assert(sql.contains("ALTER TABLE users"), sql)
    assert(sql.contains("ADD COLUMN bio TEXT"), sql)
    assert(sql.contains("ADD COLUMN url VARCHAR(500)"), sql)

  test("ALTER TABLE DROP COLUMN"):
    val m = Migration.AlterTable("users", List(AlterOp.DropColumn("bio")))
    assertEquals(compile(m), List("ALTER TABLE users DROP COLUMN bio"))

  test("ALTER TABLE DROP COLUMN IF EXISTS"):
    val m = Migration.AlterTable("users", List(AlterOp.DropColumnIfExists("bio")))
    assertEquals(compile(m), List("ALTER TABLE users DROP COLUMN IF EXISTS bio"))

  test("ALTER TABLE RENAME COLUMN"):
    val m = Migration.AlterTable("users", List(AlterOp.RenameColumn("name", "full_name")))
    assertEquals(compile(m), List("ALTER TABLE users RENAME COLUMN name TO full_name"))

  test("ALTER TABLE ALTER COLUMN TYPE"):
    val m = Migration.AlterTable("users", List(AlterOp.AlterColumnType("age", ColumnType.BigInt)))
    assertEquals(compile(m), List("ALTER TABLE users ALTER COLUMN age TYPE BIGINT"))

  test("ALTER TABLE ALTER COLUMN TYPE with USING"):
    val m = Migration.AlterTable(
      "users",
      List(AlterOp.AlterColumnType("data", ColumnType.Jsonb, Some("data::jsonb")))
    )
    assertEquals(compile(m), List("ALTER TABLE users ALTER COLUMN data TYPE JSONB USING data::jsonb"))

  test("ALTER TABLE SET/DROP DEFAULT"):
    val m1 = Migration.AlterTable("users", List(AlterOp.SetColumnDefault("active", "TRUE")))
    assertEquals(compile(m1), List("ALTER TABLE users ALTER COLUMN active SET DEFAULT TRUE"))
    val m2 = Migration.AlterTable("users", List(AlterOp.DropColumnDefault("active")))
    assertEquals(compile(m2), List("ALTER TABLE users ALTER COLUMN active DROP DEFAULT"))

  test("ALTER TABLE SET/DROP NOT NULL"):
    val m1 = Migration.AlterTable("users", List(AlterOp.SetNotNull("email")))
    assertEquals(compile(m1), List("ALTER TABLE users ALTER COLUMN email SET NOT NULL"))
    val m2 = Migration.AlterTable("users", List(AlterOp.DropNotNull("email")))
    assertEquals(compile(m2), List("ALTER TABLE users ALTER COLUMN email DROP NOT NULL"))

  // ---- INDEX ----

  test("CREATE INDEX with auto-generated name"):
    val m = Migration.AlterTable("users", List(AlterOp.AddIndex(List("email"))))
    val sql = compile(m).head
    assertEquals(sql, "CREATE INDEX users_email_idx ON users (email)")

  test("CREATE UNIQUE INDEX"):
    val m = Migration.AlterTable("users", List(AlterOp.AddIndex(List("email"), unique = true)))
    val sql = compile(m).head
    assertEquals(sql, "CREATE UNIQUE INDEX users_email_unique ON users (email)")

  test("CREATE INDEX with custom name and method"):
    val m = Migration.AlterTable(
      "posts",
      List(AlterOp.AddIndex(List("data"), name = Some("posts_data_gin_idx"), using = Some(IndexMethod.Gin)))
    )
    val sql = compile(m).head
    assertEquals(sql, "CREATE INDEX posts_data_gin_idx ON posts USING gin (data)")

  test("CREATE INDEX with WHERE (partial index)"):
    val m = Migration.AlterTable(
      "users",
      List(AlterOp.AddIndex(List("email"), where = Some("deleted_at IS NULL")))
    )
    val sql = compile(m).head
    assertEquals(sql, "CREATE INDEX users_email_idx ON users (email) WHERE deleted_at IS NULL")

  test("CREATE INDEX CONCURRENTLY"):
    val m = Migration.AlterTable(
      "users",
      List(AlterOp.AddIndex(List("email"), concurrently = true))
    )
    val sql = compile(m).head
    assertEquals(sql, "CREATE INDEX CONCURRENTLY users_email_idx ON users (email)")

  test("DROP INDEX"):
    val m = Migration.AlterTable("users", List(AlterOp.DropIndex("users_email_idx")))
    assertEquals(compile(m), List("DROP INDEX users_email_idx"))

  test("DROP INDEX IF EXISTS"):
    val m = Migration.AlterTable("users", List(AlterOp.DropIndexIfExists("users_email_idx")))
    assertEquals(compile(m), List("DROP INDEX IF EXISTS users_email_idx"))

  test("DROP INDEX CONCURRENTLY"):
    val m = Migration.AlterTable("users", List(AlterOp.DropIndexConcurrently("users_email_idx")))
    assertEquals(compile(m), List("DROP INDEX CONCURRENTLY users_email_idx"))

  test("RENAME INDEX"):
    val m = Migration.AlterTable("users", List(AlterOp.RenameIndex("old_idx", "new_idx")))
    assertEquals(compile(m), List("ALTER INDEX old_idx RENAME TO new_idx"))

  test("multi-column index"):
    val m = Migration.AlterTable(
      "users",
      List(AlterOp.AddIndex(List("first_name", "last_name")))
    )
    val sql = compile(m).head
    assertEquals(sql, "CREATE INDEX users_first_name_last_name_idx ON users (first_name, last_name)")

  // ---- CONSTRAINTS ----

  test("ADD PRIMARY KEY"):
    val m = Migration.AlterTable("users", List(AlterOp.AddPrimaryKey(List("id"))))
    assertEquals(compile(m), List("ALTER TABLE users ADD CONSTRAINT users_pkey PRIMARY KEY (id)"))

  test("ADD UNIQUE CONSTRAINT"):
    val m = Migration.AlterTable("users", List(AlterOp.AddUniqueConstraint(List("email"))))
    assertEquals(compile(m), List("ALTER TABLE users ADD CONSTRAINT users_email_unique UNIQUE (email)"))

  test("ADD CHECK CONSTRAINT"):
    val m = Migration.AlterTable("products", List(AlterOp.AddCheckConstraint("price_positive", "price > 0")))
    assertEquals(compile(m), List("ALTER TABLE products ADD CONSTRAINT price_positive CHECK (price > 0)"))

  test("DROP CONSTRAINT"):
    val m = Migration.AlterTable("users", List(AlterOp.DropConstraint("users_email_unique")))
    assertEquals(compile(m), List("ALTER TABLE users DROP CONSTRAINT users_email_unique"))

  // ---- FOREIGN KEY ----

  test("ADD FOREIGN KEY with auto name"):
    val m = Migration.AlterTable(
      "posts",
      List(AlterOp.AddForeignKey(List("author_id"), "users", List("id"), FkAction.Cascade, FkAction.NoAction))
    )
    val sql = compile(m).head
    assertEquals(
      sql,
      "ALTER TABLE posts ADD CONSTRAINT posts_author_id_fkey FOREIGN KEY (author_id) REFERENCES users(id) ON DELETE CASCADE"
    )

  test("ADD FOREIGN KEY with custom name"):
    val m = Migration.AlterTable(
      "posts",
      List(AlterOp.AddForeignKey(List("author_id"), "users", List("id"), name = Some("fk_posts_users")))
    )
    val sql = compile(m).head
    assert(sql.contains("fk_posts_users"), sql)

  test("DROP FOREIGN KEY"):
    val m = Migration.AlterTable("posts", List(AlterOp.DropForeignKey("posts_author_id_fkey")))
    assertEquals(compile(m), List("ALTER TABLE posts DROP CONSTRAINT posts_author_id_fkey"))

  // ---- COMMENTS in ALTER TABLE ----

  test("SET TABLE COMMENT"):
    val m = Migration.AlterTable("users", List(AlterOp.SetTableComment("User table")))
    assertEquals(compile(m), List("COMMENT ON TABLE users IS 'User table'"))

  test("SET COLUMN COMMENT"):
    val m = Migration.AlterTable("users", List(AlterOp.SetColumnComment("email", "Primary email")))
    assertEquals(compile(m), List("COMMENT ON COLUMN users.email IS 'Primary email'"))

  // ---- ENUM ----

  test("CREATE ENUM TYPE"):
    val m = Migration.CreateEnumType("user_role", List("admin", "member", "guest"))
    assertEquals(compile(m), List("CREATE TYPE user_role AS ENUM ('admin', 'member', 'guest')"))

  test("DROP ENUM TYPE"):
    assertEquals(compile(Migration.DropEnumType("user_role")), List("DROP TYPE user_role"))

  test("ADD ENUM VALUE"):
    assertEquals(
      compile(Migration.AddEnumValue("user_role", "moderator")),
      List("ALTER TYPE user_role ADD VALUE 'moderator'")
    )

  test("ADD ENUM VALUE BEFORE"):
    val m = Migration.AddEnumValue("user_role", "moderator", EnumValuePosition.Before("guest"))
    assertEquals(compile(m), List("ALTER TYPE user_role ADD VALUE 'moderator' BEFORE 'guest'"))

  test("ADD ENUM VALUE AFTER"):
    val m = Migration.AddEnumValue("user_role", "moderator", EnumValuePosition.After("member"))
    assertEquals(compile(m), List("ALTER TYPE user_role ADD VALUE 'moderator' AFTER 'member'"))

  test("RENAME ENUM VALUE"):
    val m = Migration.RenameEnumValue("user_role", "admin", "superadmin")
    assertEquals(compile(m), List("ALTER TYPE user_role RENAME VALUE 'admin' TO 'superadmin'"))

  // ---- EXTENSION ----

  test("CREATE EXTENSION"):
    assertEquals(compile(Migration.CreateExtension("uuid-ossp")), List("CREATE EXTENSION IF NOT EXISTS uuid-ossp"))

  test("DROP EXTENSION"):
    assertEquals(compile(Migration.DropExtension("uuid-ossp")), List("DROP EXTENSION IF EXISTS uuid-ossp"))

  // ---- RAW SQL ----

  test("Raw SQL passthrough"):
    assertEquals(compile(Migration.Raw("INSERT INTO foo VALUES (1)")), List("INSERT INTO foo VALUES (1)"))

  test("Raw parameterized SQL passthrough"):
    assertEquals(
      compile(Migration.RawParameterized("UPDATE foo SET bar = ?", List(42))),
      List("UPDATE foo SET bar = ?")
    )

  // ---- DEFAULT VALUE RENDERING ----

  test("renderLiteral for string escapes quotes"):
    assertEquals(PostgresCompiler.renderLiteral("it's"), "'it''s'")

  test("renderLiteral for boolean"):
    assertEquals(PostgresCompiler.renderLiteral(true), "TRUE")
    assertEquals(PostgresCompiler.renderLiteral(false), "FALSE")

  test("renderLiteral for numbers"):
    assertEquals(PostgresCompiler.renderLiteral(42), "42")
    assertEquals(PostgresCompiler.renderLiteral(3.14), "3.14")
    assertEquals(PostgresCompiler.renderLiteral(100L), "100")

  test("renderLiteral for null"):
    assertEquals(PostgresCompiler.renderLiteral(null), "NULL")

  // ---- COLUMN TYPE COMPILATION ----

  test("compileType covers all variants"):
    assertEquals(PostgresCompiler.compileType(ColumnType.SmallInt), "SMALLINT")
    assertEquals(PostgresCompiler.compileType(ColumnType.Integer), "INTEGER")
    assertEquals(PostgresCompiler.compileType(ColumnType.BigInt), "BIGINT")
    assertEquals(PostgresCompiler.compileType(ColumnType.SmallSerial), "SMALLSERIAL")
    assertEquals(PostgresCompiler.compileType(ColumnType.Serial), "SERIAL")
    assertEquals(PostgresCompiler.compileType(ColumnType.BigSerial), "BIGSERIAL")
    assertEquals(PostgresCompiler.compileType(ColumnType.Numeric(10, 2)), "NUMERIC(10, 2)")
    assertEquals(PostgresCompiler.compileType(ColumnType.DoublePrecision), "DOUBLE PRECISION")
    assertEquals(PostgresCompiler.compileType(ColumnType.Real), "REAL")
    assertEquals(PostgresCompiler.compileType(ColumnType.Boolean), "BOOLEAN")
    assertEquals(PostgresCompiler.compileType(ColumnType.Char(10)), "CHAR(10)")
    assertEquals(PostgresCompiler.compileType(ColumnType.Varchar(255)), "VARCHAR(255)")
    assertEquals(PostgresCompiler.compileType(ColumnType.Text), "TEXT")
    assertEquals(PostgresCompiler.compileType(ColumnType.Bytea), "BYTEA")
    assertEquals(PostgresCompiler.compileType(ColumnType.Date), "DATE")
    assertEquals(PostgresCompiler.compileType(ColumnType.Time(6)), "TIME(6)")
    assertEquals(PostgresCompiler.compileType(ColumnType.TimeTz(3)), "TIMETZ(3)")
    assertEquals(PostgresCompiler.compileType(ColumnType.Timestamp(6)), "TIMESTAMP(6)")
    assertEquals(PostgresCompiler.compileType(ColumnType.TimestampTz(3)), "TIMESTAMPTZ(3)")
    assertEquals(PostgresCompiler.compileType(ColumnType.Interval), "INTERVAL")
    assertEquals(PostgresCompiler.compileType(ColumnType.Json), "JSON")
    assertEquals(PostgresCompiler.compileType(ColumnType.Jsonb), "JSONB")
    assertEquals(PostgresCompiler.compileType(ColumnType.Uuid), "UUID")
    assertEquals(PostgresCompiler.compileType(ColumnType.PgEnum("status")), "status")
    assertEquals(PostgresCompiler.compileType(ColumnType.ArrayOf(ColumnType.Text)), "TEXT[]")
    assertEquals(PostgresCompiler.compileType(ColumnType.Inet), "INET")
    assertEquals(PostgresCompiler.compileType(ColumnType.Cidr), "CIDR")
    assertEquals(PostgresCompiler.compileType(ColumnType.MacAddr), "MACADDR")
    assertEquals(PostgresCompiler.compileType(ColumnType.Money), "MONEY")
    assertEquals(PostgresCompiler.compileType(ColumnType.Custom("vector(1536)")), "vector(1536)")

  // ---- MIXED ALTER TABLE OPS ----

  test("ALTER TABLE with ADD COLUMN + INDEX produces separate statements"):
    val m = Migration.AlterTable(
      "users",
      List(
        AlterOp.AddColumn(ColumnDef[String]("bio", ColumnType.Text, ColumnModifiers(nullable = true))),
        AlterOp.AddIndex(List("email"))
      )
    )
    val sqls = compile(m)
    assertEquals(sqls.size, 2)
    assert(sqls(0).contains("ADD COLUMN bio TEXT"), sqls(0))
    assert(sqls(1).contains("CREATE INDEX users_email_idx"), sqls(1))
end PostgresCompilerTests
