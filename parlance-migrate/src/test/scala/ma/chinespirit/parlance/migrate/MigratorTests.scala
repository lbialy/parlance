package ma.chinespirit.parlance.migrate

import ma.chinespirit.parlance.{H2, Transactor}
import munit.FunSuite
import org.h2.jdbcx.JdbcDataSource

import java.nio.file.Files

/** H2Compiler that detects CONCURRENTLY ops like PostgresCompiler does. */
object H2CompilerWithConcurrentlyDetection extends MigrationCompiler:
  def compile(migration: Migration): List[String] =
    H2Compiler.compile(migration)
  override def requiresNonTx(migration: Migration): Boolean =
    PostgresCompiler.requiresNonTx(migration)

class MigratorTests extends FunSuite:

  lazy val h2DbPath: String =
    Files.createTempDirectory(null).toAbsolutePath.toString

  def freshDs(): JdbcDataSource =
    val ds = JdbcDataSource()
    ds.setURL("jdbc:h2:" + h2DbPath + ";DB_CLOSE_DELAY=-1")
    ds.setUser("sa")
    ds.setPassword("")
    ds

  lazy val xa: Transactor[?] = Transactor(H2, freshDs())

  // -- Test fixtures: 3 MigrationDefs --

  val v1: MigrationDef = new MigrationDef:
    val version = 1L
    val name = "create_users"
    val up = List(
      Migration.CreateTable(
        "users",
        List(
          ColumnDef[Int]("id", ColumnType.Serial).primaryKey,
          ColumnDef[String]("email", ColumnType.Varchar(255)),
          ColumnDef[String]("name", ColumnType.Varchar(255))
        )
      )
    )
    val down = List(Migration.DropTable("users"))

  val v2: MigrationDef = new MigrationDef:
    val version = 2L
    val name = "add_users_name_index"
    val up = List(
      Migration.AlterTable(
        "users",
        List(AlterOp.AddIndex(List("name")))
      )
    )
    val down = List(
      Migration.AlterTable(
        "users",
        List(AlterOp.DropIndex("users_name_idx"))
      )
    )

  val v3: MigrationDef = new MigrationDef:
    val version = 3L
    val name = "create_posts"
    val up = List(
      Migration.CreateTable(
        "posts",
        List(
          ColumnDef[Int]("id", ColumnType.Serial).primaryKey,
          ColumnDef[String]("title", ColumnType.Varchar(255)),
          ColumnDef[String]("body", ColumnType.Text),
          ColumnDef[Int]("user_id", ColumnType.Integer)
            .references("users", "id", FkAction.Cascade, FkAction.NoAction)
        )
      )
    )
    val down = List(Migration.DropTable("posts"))

  // Reset DB between tests
  override def beforeEach(context: BeforeEach): Unit =
    val conn = freshDs().getConnection
    try
      val stmt = conn.createStatement()
      stmt.execute("DROP ALL OBJECTS")
      stmt.close()
    finally conn.close()

  // --- Construction validation ---

  test("rejects duplicate versions"):
    val dup = new MigrationDef:
      val version = 1L
      val name = "duplicate"
      val up = Nil
      val down = Nil
    intercept[MigrationError]:
      Migrator(List(v1, dup), xa, H2Compiler)

  test("rejects unsorted versions"):
    intercept[MigrationError]:
      Migrator(List(v2, v1), xa, H2Compiler)

  test("accepts empty list"):
    val m = Migrator(Nil, xa, H2Compiler)
    val result = m.migrate()
    assertEquals(result.appliedCount, 0)
    assertEquals(result.batch, None)

  // --- migrate() ---

  test("migrate applies all migrations on fresh DB"):
    val m = Migrator(List(v1, v2, v3), xa, H2Compiler)
    val result = m.migrate()
    assertEquals(result.appliedCount, 3)
    assertEquals(result.batch, Some(1))
    assertEquals(result.applied.size, 3)
    assertEquals(
      result.applied.map(_.migration),
      List(
        "1_create_users",
        "2_add_users_name_index",
        "3_create_posts"
      )
    )

  test("migrate is idempotent"):
    val m = Migrator(List(v1, v2, v3), xa, H2Compiler)
    m.migrate()
    val result = m.migrate()
    assertEquals(result.appliedCount, 0)
    assertEquals(result.batch, None)
    assertEquals(result.applied, Nil)

  test("migrate applies only pending migrations"):
    val m1 = Migrator(List(v1), xa, H2Compiler)
    m1.migrate()
    val m2 = Migrator(List(v1, v2, v3), xa, H2Compiler)
    val result = m2.migrate()
    assertEquals(result.appliedCount, 2)
    assertEquals(result.batch, Some(2))
    assertEquals(
      result.applied.map(_.migration),
      List("2_add_users_name_index", "3_create_posts")
    )

  test("migrate with empty list does nothing"):
    val m = Migrator(Nil, xa, H2Compiler)
    val result = m.migrate()
    assertEquals(result.appliedCount, 0)

  test("migrate actually executes DDL"):
    val m = Migrator(List(v1), xa, H2Compiler)
    m.migrate()
    // Verify table exists by querying it
    xa.connect:
      val conn = summon[ma.chinespirit.parlance.DbCon[?]].connection
      val rs = conn.createStatement().executeQuery("SELECT COUNT(*) FROM users")
      rs.next()
      assertEquals(rs.getInt(1), 0)
      rs.close()

  // --- rollback() ---

  test("rollback rolls back latest batch"):
    val m = Migrator(List(v1, v2, v3), xa, H2Compiler)
    m.migrate()
    val result = m.rollback()
    assertEquals(result.rolledBackCount, 3)

  test("rollback does nothing when nothing applied"):
    val m = Migrator(List(v1, v2, v3), xa, H2Compiler)
    val result = m.rollback()
    assertEquals(result.rolledBackCount, 0)
    assertEquals(result.rolledBack, Nil)

  test("rollback only rolls back latest batch"):
    val m1 = Migrator(List(v1), xa, H2Compiler)
    m1.migrate()
    val m2 = Migrator(List(v1, v2, v3), xa, H2Compiler)
    m2.migrate()
    val result = m2.rollback()
    assertEquals(result.rolledBackCount, 2)
    assertEquals(
      result.rolledBack.map(_.migration),
      List("3_create_posts", "2_add_users_name_index")
    )

  // --- rollbackSteps() ---

  test("rollbackSteps rolls back last N migrations"):
    val m1 = Migrator(List(v1), xa, H2Compiler)
    m1.migrate()
    val m2 = Migrator(List(v1, v2, v3), xa, H2Compiler)
    m2.migrate()
    val result = m2.rollbackSteps(2)
    assertEquals(result.rolledBackCount, 2)
    assertEquals(
      result.rolledBack.map(_.migration),
      List("3_create_posts", "2_add_users_name_index")
    )

  test("rollbackSteps across batch boundaries"):
    val m1 = Migrator(List(v1), xa, H2Compiler)
    m1.migrate()
    val m2 = Migrator(List(v1, v2), xa, H2Compiler)
    m2.migrate()
    // v2 is batch 2, v1 is batch 1 — rolling back 2 steps crosses boundary
    val result = m2.rollbackSteps(2)
    assertEquals(result.rolledBackCount, 2)
    assertEquals(
      result.rolledBack.map(_.migration),
      List("2_add_users_name_index", "1_create_users")
    )

  // --- reset() ---

  test("reset rolls back everything"):
    val m = Migrator(List(v1, v2, v3), xa, H2Compiler)
    m.migrate()
    val result = m.reset()
    assertEquals(result.rolledBackCount, 3)
    val st = m.status()
    assertEquals(st.applied, Nil)
    assertEquals(st.pending.size, 3)

  // --- status() ---

  test("status on fresh DB shows all pending"):
    val m = Migrator(List(v1, v2, v3), xa, H2Compiler)
    val st = m.status()
    assertEquals(st.applied, Nil)
    assertEquals(st.pending.size, 3)

  test("status after migrate shows all applied"):
    val m = Migrator(List(v1, v2, v3), xa, H2Compiler)
    m.migrate()
    val st = m.status()
    assertEquals(st.applied.size, 3)
    assertEquals(st.pending, Nil)

  test("status after partial rollback shows correct split"):
    val m = Migrator(List(v1, v2, v3), xa, H2Compiler)
    m.migrate()
    m.rollbackSteps(1)
    val st = m.status()
    assertEquals(st.applied.size, 2)
    assertEquals(st.pending.size, 1)
    assertEquals(st.pending.head.version, 3L)

  // --- pretend() ---

  test("pretend shows SQL without executing"):
    val m = Migrator(List(v1, v2), xa, H2Compiler)
    val results = m.pretend()
    assertEquals(results.size, 2)
    assert(results.head.compiledSql.exists(_.contains("CREATE")))
    // Verify users table does NOT exist by trying to query it
    xa.connect:
      val conn = summon[ma.chinespirit.parlance.DbCon[?]].connection
      val threw =
        try
          conn.createStatement().executeQuery("SELECT COUNT(*) FROM users")
          false
        catch case _: Exception => true
      assert(threw, "Expected users table to not exist after pretend()")

  // --- RawParameterized ---

  test("RawParameterized executes with parameters"):
    val insertMigration = new MigrationDef:
      val version = 4L
      val name = "seed_user"
      val up = List(
        Migration.RawParameterized(
          "INSERT INTO users (email, name) VALUES (?, ?)",
          List("test@example.com", "Test User")
        )
      )
      val down = List(
        Migration.Raw("DELETE FROM users WHERE email = 'test@example.com'")
      )

    val m = Migrator(List(v1, v2, v3, insertMigration), xa, H2Compiler)
    m.migrate()
    xa.connect:
      val conn = summon[ma.chinespirit.parlance.DbCon[?]].connection
      val rs = conn
        .createStatement()
        .executeQuery(
          "SELECT email, name FROM users WHERE email = 'test@example.com'"
        )
      assert(rs.next(), "Expected a row")
      assertEquals(rs.getString("email"), "test@example.com")
      assertEquals(rs.getString("name"), "Test User")
      rs.close()

  // --- CONCURRENTLY / TxMode tests ---

  test(
    "txMode=Transactional with CONCURRENTLY ops throws at resolveTxMode"
  ):
    val concurrentMigration = new MigrationDef:
      val version = 4L
      val name = "concurrent_index"
      val up = List(
        Migration.AlterTable(
          "users",
          List(AlterOp.AddIndex(List("email"), concurrently = true))
        )
      )
      val down = Nil
      override val txMode: TxMode = TxMode.Transactional

    val m = Migrator(
      List(v1, v2, v3, concurrentMigration),
      xa,
      H2CompilerWithConcurrentlyDetection
    )
    val err = intercept[MigrationError]:
      m.migrate()
    assert(err.getMessage.contains("CONCURRENTLY"))

  test(
    "txMode=Transactional with DropIndexConcurrently throws at resolveTxMode"
  ):
    val concurrentMigration = new MigrationDef:
      val version = 4L
      val name = "concurrent_drop"
      val up = List(
        Migration.AlterTable(
          "users",
          List(AlterOp.DropIndexConcurrently("some_idx"))
        )
      )
      val down = Nil
      override val txMode: TxMode = TxMode.Transactional

    val m = Migrator(
      List(v1, v2, v3, concurrentMigration),
      xa,
      H2CompilerWithConcurrentlyDetection
    )
    val err = intercept[MigrationError]:
      m.migrate()
    assert(err.getMessage.contains("CONCURRENTLY"))

  test("txMode=NonTransactional succeeds via migrateWithTxControl"):
    val concurrentMigration = new MigrationDef:
      val version = 4L
      val name = "concurrent_index"
      val up = List(
        Migration.AlterTable(
          "users",
          List(AlterOp.AddIndex(List("email"), concurrently = true))
        )
      )
      val down = Nil
      override val txMode: TxMode = TxMode.NonTransactional

    // H2 ignores CONCURRENTLY, so this should succeed
    val m = Migrator(
      List(v1, v2, v3, concurrentMigration),
      xa,
      H2Compiler
    )
    val result = m.migrateWithTxControl()
    assertEquals(result.appliedCount, 4)

  test("txMode=Auto with CONCURRENTLY resolves to NonTransactional"):
    val concurrentMigration = new MigrationDef:
      val version = 4L
      val name = "concurrent_index"
      val up = List(
        Migration.AlterTable(
          "users",
          List(AlterOp.AddIndex(List("email"), concurrently = true))
        )
      )
      val down = Nil
      // txMode defaults to Auto

    // H2CompilerWithConcurrentlyDetection detects CONCURRENTLY as requiresNonTx
    // Auto + requiresNonTx=true resolves to NonTransactional
    // migrateWithTxControl handles per-migration tx mode
    val m = Migrator(
      List(v1, v2, v3, concurrentMigration),
      xa,
      H2CompilerWithConcurrentlyDetection
    )
    val result = m.migrateWithTxControl()
    assertEquals(result.appliedCount, 4)

end MigratorTests
