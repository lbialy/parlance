import ma.chinespirit.parlance.*

@Table(SqlNameMapper.CamelToSnakeCase)
case class QbOrder(@Id id: Long, customer: String, status: String, amount: Int) derives EntityMeta

trait ProjectedQueryTestsDefs:
  self: QbTestBase[?] =>

  test("select subset of columns"):
    val t = xa()
    t.connect:
      val results = QueryBuilder
        .from[QbOrder]
        .select(c => (customer = c.customer, amount = c.amount))
        .run()
      assertEquals(results.length, 7)
      assertEquals(results.head.customer, "Alice")

  test("select with where filter"):
    val t = xa()
    t.connect:
      val results = QueryBuilder
        .from[QbOrder]
        .where(_.status === "paid")
        .select(c => (customer = c.customer, amount = c.amount))
        .run()
      assertEquals(results.length, 5)
      assert(results.forall(r => r.amount >= 100))

  test("select with GROUP BY and COUNT"):
    val t = xa()
    t.connect:
      val results = QueryBuilder
        .from[QbOrder]
        .select(c => (status = c.status, cnt = Expr.count))
        .groupBy(_.status)
        .run()
      assertEquals(results.length, 2)
      val paid = results.find(_.status == "paid").get
      val pending = results.find(_.status == "pending").get
      assertEquals(paid.cnt, 5L)
      assertEquals(pending.cnt, 2L)

  test("select with GROUP BY and SUM"):
    val t = xa()
    t.connect:
      val results = QueryBuilder
        .from[QbOrder]
        .select(c => (customer = c.customer, total = Expr.sum(c.amount)))
        .groupBy(_.customer)
        .orderBy(_.customer)
        .run()
      assertEquals(results.length, 3)
      assertEquals(results(0).customer, "Alice")
      assertEquals(results(0).total, 300)
      assertEquals(results(1).customer, "Bob")
      assertEquals(results(1).total, 200)
      assertEquals(results(2).customer, "Charlie")
      assertEquals(results(2).total, 475)

  test("select with HAVING filter"):
    val t = xa()
    t.connect:
      val results = QueryBuilder
        .from[QbOrder]
        .select(c => (customer = c.customer, total = Expr.sum(c.amount)))
        .groupBy(_.customer)
        .having(_.total > 250)
        .run()
      assertEquals(results.length, 2)
      val names = results.map(_.customer).sorted
      assertEquals(names, Vector("Alice", "Charlie"))

  test("select with ORDER BY on projected column"):
    val t = xa()
    t.connect:
      val results = QueryBuilder
        .from[QbOrder]
        .select(c => (customer = c.customer, total = Expr.sum(c.amount)))
        .groupBy(_.customer)
        .orderBy(_.total, SortOrder.Desc)
        .run()
      assertEquals(results(0).customer, "Charlie")
      assertEquals(results(1).customer, "Alice")
      assertEquals(results(2).customer, "Bob")

  test("select with LIMIT"):
    val t = xa()
    t.connect:
      val results = QueryBuilder
        .from[QbOrder]
        .select(c => (customer = c.customer, amount = c.amount))
        .orderBy(_.amount, SortOrder.Desc)
        .limit(3)
        .run()
      assertEquals(results.length, 3)
      assertEquals(results(0).amount, 300)

  test("select with OFFSET"):
    val t = xa()
    t.connect:
      val results = QueryBuilder
        .from[QbOrder]
        .select(c => (customer = c.customer, amount = c.amount))
        .orderBy(_.amount, SortOrder.Desc)
        .limit(2)
        .offset(1)
        .run()
      assertEquals(results.length, 2)
      assertEquals(results(0).amount, 200)

  test("select DISTINCT"):
    val t = xa()
    t.connect:
      val results = QueryBuilder
        .from[QbOrder]
        .select(c => (customer = c.customer, status = c.status))
        .distinct
        .run()
      assertEquals(results.length, 5)
      val alicePaid = results.filter(r => r.customer == "Alice" && r.status == "paid")
      assertEquals(alicePaid.length, 1)

  test("select build returns correct SQL"):
    val frag = QueryBuilder
      .from[QbOrder]
      .where(_.status === "paid")
      .select(c => (customer = c.customer, total = Expr.sum(c.amount)))
      .groupBy(_.customer)
      .having(_.total > 100)
      .orderBy(_.total, SortOrder.Desc)
      .limit(10)
      .buildWith(databaseType)
    assertEquals(
      frag.sqlString,
      "SELECT customer AS customer, SUM(amount) AS total FROM qb_order WHERE status = ? GROUP BY customer HAVING SUM(amount) > ? ORDER BY SUM(amount) DESC LIMIT 10"
    )

  test("first() returns head option"):
    val t = xa()
    t.connect:
      val result = QueryBuilder
        .from[QbOrder]
        .select(c => (customer = c.customer, amount = c.amount))
        .orderBy(_.amount, SortOrder.Desc)
        .first()
      assert(result.isDefined)
      assertEquals(result.get.amount, 300)

  test("firstOrFail() returns value"):
    val t = xa()
    t.connect:
      val result = QueryBuilder
        .from[QbOrder]
        .where(_.customer === "Alice")
        .select(c => (customer = c.customer, amount = c.amount))
        .orderBy(_.amount)
        .firstOrFail()
      assertEquals(result.customer, "Alice")
      assertEquals(result.amount, 100)

  test("select with multiple GROUP BY columns"):
    val t = xa()
    t.connect:
      val results = QueryBuilder
        .from[QbOrder]
        .select(c => (customer = c.customer, status = c.status, cnt = Expr.count))
        .groupBy(_.customer)
        .groupBy(_.status)
        .orderBy(_.customer)
        .orderBy(_.status)
        .run()
      assert(results.length >= 4, s"expected at least 4 rows, got ${results.length}")
      val alicePaid = results.find(r => r.customer == "Alice" && r.status == "paid").get
      assertEquals(alicePaid.cnt, 2L)

  test("select with MIN and MAX"):
    val t = xa()
    t.connect:
      val results = QueryBuilder
        .from[QbOrder]
        .select(c => (customer = c.customer, minAmt = Expr.min(c.amount), maxAmt = Expr.max(c.amount)))
        .groupBy(_.customer)
        .orderBy(_.customer)
        .run()
      assertEquals(results.length, 3)
      val alice = results(0)
      assertEquals(alice.customer, "Alice")
      assertEquals(alice.minAmt, 100)
      assertEquals(alice.maxAmt, 200)

end ProjectedQueryTestsDefs

class ProjectedQueryTests extends QbH2TestBase, ProjectedQueryTestsDefs:
  val h2Ddls = Seq("/h2/qb-projected.sql")
end ProjectedQueryTests

class PgProjectedQueryTests extends QbPgTestBase, ProjectedQueryTestsDefs:
  val pgDdls = Seq("/pg/qb-projected.sql")
end PgProjectedQueryTests
