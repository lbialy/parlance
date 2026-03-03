import com.augustnagro.magnum.*

class QueryBuilderSortPaginateTests extends QbTestBase:

  val h2Ddls = Seq("/h2/qb-user.sql")

  // val u = Columns.of[QbUser]

  test("orderBy age ascending"):
    val t = xa()
    t.connect:
      val results = QueryBuilder.from[QbUser].orderBy(_.age).run()
      assertEquals(results.map(_.age), Vector(17, 22, 25, 30))

  test("orderBy age descending"):
    val t = xa()
    t.connect:
      val results =
        QueryBuilder.from[QbUser].orderBy(_.age, SortOrder.Desc).run()
      assertEquals(results.map(_.age), Vector(30, 25, 22, 17))

  test("limit(2) returns exactly 2 rows"):
    val t = xa()
    t.connect:
      val results = QueryBuilder.from[QbUser].limit(2).run()
      assertEquals(results.length, 2)

  test("offset(1).limit(2) skips 1 row and returns next 2"):
    val t = xa()
    t.connect:
      val results =
        QueryBuilder.from[QbUser].orderBy(_.age).offset(1).limit(2).run()
      assertEquals(results.map(_.age), Vector(22, 25))

  test("where + orderBy + limit combined"):
    val t = xa()
    t.connect:
      val results = QueryBuilder
        .from[QbUser]
        .where(_.age > 18)
        .orderBy(_.age)
        .limit(2)
        .run()
      assertEquals(results.length, 2)
      assertEquals(results.map(_.age), Vector(22, 25))

  test("multiple orderBy calls accumulate"):
    val frag = QueryBuilder
      .from[QbUser]
      .orderBy(_.firstName)
      .orderBy(_.age)
      .buildWith(H2)
    assert(frag.sqlString.contains("ORDER BY first_name ASC, age ASC"))

  test("multiple orderBy calls run against H2"):
    val t = xa()
    t.connect:
      val results = QueryBuilder
        .from[QbUser]
        .orderBy(_.firstName)
        .orderBy(_.age)
        .run()
      assertEquals(
        results.map(_.firstName),
        Vector(None, Some("Alice"), Some("Bob"), Some("Charlie"))
      )

  test("build SQL with ORDER BY, LIMIT, OFFSET"):
    val frag = QueryBuilder
      .from[QbUser]
      .where(_.age > 18)
      .orderBy(_.age, SortOrder.Desc)
      .limit(10)
      .offset(5)
      .buildWith(H2)
    assertEquals(
      frag.sqlString,
      "SELECT id, first_name, age FROM qb_user WHERE age > ? ORDER BY age DESC LIMIT 10 OFFSET 5"
    )

end QueryBuilderSortPaginateTests
