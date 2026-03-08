import ma.chinespirit.parlance.*

trait LockingTestsDefs[D <: SupportsForShare]:
  self: QbTestBase[D] =>

  test("lockForUpdate.build produces SQL ending with FOR UPDATE"):
    val t = xa()
    t.transact:
      val frag = QueryBuilder.from[QbUser].lockForUpdate.build
      assert(frag.sqlString.endsWith("FOR UPDATE"))

  test("forShare.build produces SQL ending with FOR SHARE"):
    val t = xa()
    t.transact:
      val frag = QueryBuilder.from[QbUser].forShare.build
      assert(frag.sqlString.endsWith("FOR SHARE"))

  test("lockForUpdate with where + orderBy + limit produces well-formed SQL"):
    val t = xa()
    t.transact:
      val frag = QueryBuilder
        .from[QbUser]
        .where(_.age > 18)
        .orderBy(_.age)
        .limit(2)
        .lockForUpdate
        .build
      val sql = frag.sqlString
      assert(sql.contains("WHERE"), s"Expected WHERE in: $sql")
      assert(sql.contains("ORDER BY"), s"Expected ORDER BY in: $sql")
      assert(sql.contains("LIMIT"), s"Expected LIMIT in: $sql")
      assert(sql.endsWith("FOR UPDATE"), s"Expected FOR UPDATE at end: $sql")

  test("lockForUpdate.run() works inside transact"):
    val t = xa()
    t.transact:
      val results = QueryBuilder.from[QbUser].lockForUpdate.run()
      assertEquals(results.size, 4)

  test("lockForUpdate.first() works inside transact"):
    val t = xa()
    t.transact:
      val result = QueryBuilder
        .from[QbUser]
        .where(_.firstName === Some("Alice"))
        .lockForUpdate
        .first()
      assert(result.isDefined)
      assertEquals(result.get.firstName, Some("Alice"))

  test("lockForUpdate.first() returns None when no rows match"):
    val t = xa()
    t.transact:
      val result = QueryBuilder
        .from[QbUser]
        .where(_.age > 100)
        .lockForUpdate
        .first()
      assertEquals(result, None)

  test("lockForUpdate.firstOrFail() returns entity"):
    val t = xa()
    t.transact:
      val result = QueryBuilder
        .from[QbUser]
        .where(_.firstName === Some("Bob"))
        .lockForUpdate
        .firstOrFail()
      assertEquals(result.firstName, Some("Bob"))

  test("lockForUpdate.firstOrFail() throws on empty"):
    val t = xa()
    t.transact:
      intercept[QueryBuilderException]:
        QueryBuilder
          .from[QbUser]
          .where(_.age > 100)
          .lockForUpdate
          .firstOrFail()

  test("lockForUpdate does not compile with DbCon"):
    val errors = compileErrors("""
      import ma.chinespirit.parlance.*
      def test(using DbCon[?]): Unit =
        QueryBuilder.from[QbUser].lockForUpdate.run()
    """)
    assert(errors.contains("No given instance of type ma.chinespirit.parlance.DbTx[? <: ma.chinespirit.parlance.SupportsRowLocks]"))

  test("forShare does not compile with DbCon"):
    val errors = compileErrors("""
      import ma.chinespirit.parlance.*
      def test(using DbCon[?]): Unit =
        QueryBuilder.from[QbUser].forShare.run()
    """)
    assert(errors.contains("No given instance of type ma.chinespirit.parlance.DbTx[? <: ma.chinespirit.parlance.SupportsForShare]"))

  test("lockForUpdate does not compile with plain DbTx (no SupportsRowLocks)"):
    val errors = compileErrors("""
      import ma.chinespirit.parlance.*
      def test(using DbTx[SQLite]): Unit =
        QueryBuilder.from[QbUser].lockForUpdate.run()
    """)
    assert(errors.nonEmpty)

end LockingTestsDefs

class LockingTests extends QbH2TestBase, LockingTestsDefs[H2]:
  val h2Ddls = Seq("/h2/qb-user.sql")
end LockingTests

class PgLockingTests extends QbPgTestBase, LockingTestsDefs[Postgres]:
  val pgDdls = Seq("/pg/qb-user.sql")
end PgLockingTests
