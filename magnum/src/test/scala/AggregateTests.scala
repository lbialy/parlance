import com.augustnagro.magnum.*

class AggregateTests extends QbTestBase:

  val h2Ddls = Seq("/h2/qb-chunk.sql", "/h2/qb-user.sql")

  // --- SUM ---

  test("sum returns total"):
    val t = xa()
    t.connect:
      val result = QueryBuilder.from[QbItem].sum(_.amount)
      assertEquals(result, Some(50500))

  test("sum with where"):
    val t = xa()
    t.connect:
      val result = QueryBuilder
        .from[QbItem]
        .where(_.amount > 500)
        .sum(_.amount)
      assertEquals(result, Some(37750))

  test("sum on empty returns None"):
    val t = xa()
    t.connect:
      val result = QueryBuilder
        .from[QbItem]
        .where(_.amount > 99999)
        .sum(_.amount)
      assertEquals(result, None)

  // --- AVG ---

  test("avg returns Double"):
    val t = xa()
    t.connect:
      val result = QueryBuilder.from[QbItem].avg(_.amount)
      assert(result.isDefined, "avg should return Some")
      assertEqualsDouble(result.get, 505.0, 0.01)

  test("avg with where"):
    val t = xa()
    t.connect:
      val result = QueryBuilder
        .from[QbItem]
        .where(_.amount > 500)
        .avg(_.amount)
      assert(result.isDefined, "avg with where should return Some")
      assertEqualsDouble(result.get, 755.0, 0.01)

  test("avg on empty returns None"):
    val t = xa()
    t.connect:
      val result = QueryBuilder
        .from[QbItem]
        .where(_.amount > 99999)
        .avg(_.amount)
      assertEquals(result, None)

  // --- MIN ---

  test("min returns minimum"):
    val t = xa()
    t.connect:
      val result = QueryBuilder.from[QbItem].min(_.amount)
      assertEquals(result, Some(10))

  test("min with where"):
    val t = xa()
    t.connect:
      val result = QueryBuilder
        .from[QbItem]
        .where(_.amount > 500)
        .min(_.amount)
      assertEquals(result, Some(510))

  // --- MAX ---

  test("max returns maximum"):
    val t = xa()
    t.connect:
      val result = QueryBuilder.from[QbItem].max(_.amount)
      assertEquals(result, Some(1000))

  test("max with where"):
    val t = xa()
    t.connect:
      val result = QueryBuilder
        .from[QbItem]
        .where(_.amount > 500)
        .max(_.amount)
      assertEquals(result, Some(1000))

  // --- MIN/MAX on empty ---

  test("min/max on empty returns None"):
    val t = xa()
    t.connect:
      val minResult = QueryBuilder
        .from[QbItem]
        .where(_.amount > 99999)
        .min(_.amount)
      val maxResult = QueryBuilder
        .from[QbItem]
        .where(_.amount > 99999)
        .max(_.amount)
      assertEquals(minResult, None)
      assertEquals(maxResult, None)

  // --- COUNT(column) ---

  test("count(column) counts non-null values"):
    val t = xa()
    t.connect:
      val countAll = QueryBuilder.from[QbUser].count()
      val countFirstName = QueryBuilder.from[QbUser].count(_.firstName)
      assertEquals(countAll, 4L)
      assertEquals(countFirstName, 3L)

end AggregateTests
