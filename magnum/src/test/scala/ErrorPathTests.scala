import com.augustnagro.magnum.*

class ErrorPathTests extends QbTestBase:

  val h2Ddls = Seq("/h2/qb-user.sql", "/h2/qb-join.sql")

  // --- chunk() validation ---

  test("chunk(0) throws IllegalArgumentException"):
    val t = xa()
    t.connect:
      intercept[IllegalArgumentException]:
        QueryBuilder.from[QbUser].chunk(0)

  test("chunk(-1) throws IllegalArgumentException"):
    val t = xa()
    t.connect:
      intercept[IllegalArgumentException]:
        QueryBuilder.from[QbUser].chunk(-1)

  // --- chunk iterator exhaustion ---

  test("chunk iterator next() after exhaustion throws NoSuchElementException"):
    val t = xa()
    t.connect:
      val iter = QueryBuilder.from[QbUser].chunk(100)
      // consume the single batch
      iter.next()
      assert(!iter.hasNext)
      intercept[NoSuchElementException]:
        iter.next()

  // --- joined query firstOrFail() on empty ---

  val bookAuthor = Relationship.belongsTo[JnBook, JnAuthor](_.authorId, _.id)

  test("joined firstOrFail() throws QueryBuilderException on empty"):
    val t = xa()
    t.connect:
      intercept[QueryBuilderException]:
        QueryBuilder
          .from[JnBook]
          .join(bookAuthor)
          .where(sql"title = ${"NonExistent"}".unsafeAsWhere)
          .firstOrFail()

  // --- firstOrFail() error message includes table name ---

  test("firstOrFail() error message includes table name"):
    val t = xa()
    t.connect:
      val ex = intercept[QueryBuilderException]:
        QueryBuilder.from[QbUser].where(_.age > 100).firstOrFail()
      assert(
        ex.getMessage.contains("qb_user"),
        s"Expected table name in message: ${ex.getMessage}"
      )

  // --- limit(0) is valid (returns empty) ---

  test("limit(0) returns empty result"):
    val t = xa()
    t.connect:
      val results = QueryBuilder.from[QbUser].limit(0).run()
      assertEquals(results.length, 0)

  // --- count on empty filtered result returns 0, not throw ---

  test("count() on empty filtered result returns 0"):
    val t = xa()
    t.connect:
      val result = QueryBuilder.from[QbUser].where(_.age > 100).count()
      assertEquals(result, 0L)

  // --- exists() on empty filtered result returns false, not throw ---

  test("exists() on empty filtered result returns false"):
    val t = xa()
    t.connect:
      val result = QueryBuilder.from[QbUser].where(_.age > 100).exists()
      assertEquals(result, false)

  // --- aggregate on empty filtered result returns None ---

  test("sum on empty filtered result returns None"):
    val t = xa()
    t.connect:
      val result = QueryBuilder.from[QbUser].where(_.age > 100).sum(_.age)
      assertEquals(result, None)

  test("avg on empty filtered result returns None"):
    val t = xa()
    t.connect:
      val result = QueryBuilder.from[QbUser].where(_.age > 100).avg(_.age)
      assertEquals(result, None)

  test("min on empty filtered result returns None"):
    val t = xa()
    t.connect:
      val result = QueryBuilder.from[QbUser].where(_.age > 100).min(_.age)
      assertEquals(result, None)

  test("max on empty filtered result returns None"):
    val t = xa()
    t.connect:
      val result = QueryBuilder.from[QbUser].where(_.age > 100).max(_.age)
      assertEquals(result, None)

end ErrorPathTests
