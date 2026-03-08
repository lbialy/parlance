import ma.chinespirit.parlance.*

trait QueryBuilderTerminalTestsDefs:
  self: QbTestBase[?] =>

  // val u = Columns.of[QbUser]

  test("first() returns Some when rows exist"):
    val t = xa()
    t.connect:
      val result = QueryBuilder.from[QbUser].orderBy(_.age).first()
      assert(result.isDefined)
      assertEquals(result.get.age, 17)

  test("first() returns None when no rows match"):
    val t = xa()
    t.connect:
      val result = QueryBuilder.from[QbUser].where(_.age > 100).first()
      assertEquals(result, None)

  test("firstOrFail() returns entity"):
    val t = xa()
    t.connect:
      val result = QueryBuilder
        .from[QbUser]
        .where(_.firstName === Some("Alice"))
        .firstOrFail()
      assertEquals(result.firstName, Some("Alice"))

  test("firstOrFail() throws on empty"):
    val t = xa()
    t.connect:
      intercept[QueryBuilderException]:
        QueryBuilder.from[QbUser].where(_.age > 100).firstOrFail()

  test("count() returns total count"):
    val t = xa()
    t.connect:
      val result = QueryBuilder.from[QbUser].count()
      assertEquals(result, 4L)

  test("where(...).count() returns filtered count"):
    val t = xa()
    t.connect:
      val result = QueryBuilder.from[QbUser].where(_.age > 18).count()
      assertEquals(result, 3L)

  test("exists() returns true/false"):
    val t = xa()
    t.connect:
      assert(QueryBuilder.from[QbUser].exists())
      assert(!QueryBuilder.from[QbUser].where(_.age > 100).exists())

end QueryBuilderTerminalTestsDefs

class QueryBuilderTerminalTests extends QbH2TestBase, QueryBuilderTerminalTestsDefs:
  val h2Ddls = Seq("/h2/qb-user.sql")

class PgQueryBuilderTerminalTests extends QbPgTestBase, QueryBuilderTerminalTestsDefs:
  val pgDdls = Seq("/pg/qb-user.sql")
