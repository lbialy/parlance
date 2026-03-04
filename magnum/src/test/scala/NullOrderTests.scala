import com.augustnagro.magnum.*

class NullOrderTests extends QbTestBase:

  val h2Ddls = Seq("/h2/qb-user.sql")

  // --- SQL generation tests ---

  test("orderBy with NullOrder.First generates NULLS FIRST"):
    val t = xa()
    t.connect:
      val frag = QueryBuilder
        .from[QbUser]
        .orderBy(_.firstName, SortOrder.Asc, NullOrder.First)
        .build
      assert(frag.sqlString.contains("ORDER BY first_name ASC NULLS FIRST"), frag.sqlString)

  test("orderBy with NullOrder.Last generates NULLS LAST"):
    val t = xa()
    t.connect:
      val frag = QueryBuilder
        .from[QbUser]
        .orderBy(_.age, SortOrder.Desc, NullOrder.Last)
        .build
      assert(frag.sqlString.contains("ORDER BY age DESC NULLS LAST"), frag.sqlString)

  test("orderBy with NullOrder.Default generates no NULLS suffix"):
    val t = xa()
    t.connect:
      val frag = QueryBuilder
        .from[QbUser]
        .orderBy(_.firstName, SortOrder.Asc, NullOrder.Default)
        .build
      assert(!frag.sqlString.contains("NULLS"), frag.sqlString)

  test("orderBy without explicit NullOrder generates no NULLS suffix"):
    val t = xa()
    t.connect:
      val frag = QueryBuilder
        .from[QbUser]
        .orderBy(_.firstName, SortOrder.Asc)
        .build
      assert(!frag.sqlString.contains("NULLS"), frag.sqlString)

  // --- Behavioral tests against H2 ---

  test("NULLS FIRST puts NULL first_name first"):
    val t = xa()
    t.connect:
      val results = QueryBuilder
        .from[QbUser]
        .orderBy(_.firstName, SortOrder.Asc, NullOrder.First)
        .run()
      assertEquals(results.head.firstName, None)

  test("NULLS LAST puts NULL first_name last"):
    val t = xa()
    t.connect:
      val results = QueryBuilder
        .from[QbUser]
        .orderBy(_.firstName, SortOrder.Asc, NullOrder.Last)
        .run()
      assertEquals(results.last.firstName, None)

  // --- Multiple orderBy entries ---

  test("multiple orderBy entries with mixed NullOrder"):
    val t = xa()
    t.connect:
      val frag = QueryBuilder
        .from[QbUser]
        .orderBy(_.firstName, SortOrder.Asc, NullOrder.First)
        .orderBy(_.age, SortOrder.Desc, NullOrder.Last)
        .build
      assert(frag.sqlString.contains("ORDER BY first_name ASC NULLS FIRST, age DESC NULLS LAST"), frag.sqlString)

end NullOrderTests
