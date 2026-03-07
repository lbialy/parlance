import com.augustnagro.magnum.*

class HasCountTests extends QbTestBase:

  val h2Ddls = Seq("/h2/qb-where-has.sql")


  // --- HasMany unconstrained ---

  test("has(ElAuthor.books)(_ >= 2) returns authors with 2+ books"):
    val t = xa()
    t.connect:
      val results = QueryBuilder
        .from[ElAuthor]
        .has(ElAuthor.books)(_ >= 2)
        .orderBy(_.name)
        .run()
      assertEquals(results.size, 2)
      assertEquals(results.map(_.name), Vector("Asimov", "Tolkien"))

  test("has(ElAuthor.books)(_ >= 1) returns authors with 1+ books"):
    val t = xa()
    t.connect:
      val results = QueryBuilder
        .from[ElAuthor]
        .has(ElAuthor.books)(_ >= 1)
        .orderBy(_.name)
        .run()
      assertEquals(results.size, 3)
      assertEquals(results.map(_.name), Vector("Asimov", "Herbert", "Tolkien"))

  test("has(ElAuthor.books)(_ === 0) returns authors with no books"):
    val t = xa()
    t.connect:
      val results = QueryBuilder
        .from[ElAuthor]
        .has(ElAuthor.books)(_ === 0)
        .run()
      assertEquals(results.size, 1)
      assertEquals(results.head.name, "Rowling")

  test("has(ElAuthor.books)(_ > 1) returns authors with more than 1 book"):
    val t = xa()
    t.connect:
      val results = QueryBuilder
        .from[ElAuthor]
        .has(ElAuthor.books)(_ > 1)
        .orderBy(_.name)
        .run()
      assertEquals(results.size, 2)
      assertEquals(results.map(_.name), Vector("Asimov", "Tolkien"))

  test("has(ElAuthor.books)(_ < 2) returns authors with fewer than 2 books"):
    val t = xa()
    t.connect:
      val results = QueryBuilder
        .from[ElAuthor]
        .has(ElAuthor.books)(_ < 2)
        .orderBy(_.name)
        .run()
      assertEquals(results.size, 2)
      assertEquals(results.map(_.name), Vector("Herbert", "Rowling"))

  // --- BelongsToMany unconstrained ---

  test("has(PvUser.roles)(_ >= 2) returns users with 2+ roles"):
    val t = xa()
    t.connect:
      val results = QueryBuilder
        .from[PvUser]
        .has(PvUser.roles)(_ >= 2)
        .orderBy(_.name)
        .run()
      assertEquals(results.size, 2)
      assertEquals(results.map(_.name), Vector("Alice", "Dave"))

  test("has(PvUser.roles)(_ >= 3) returns users with 3+ roles"):
    val t = xa()
    t.connect:
      val results = QueryBuilder
        .from[PvUser]
        .has(PvUser.roles)(_ >= 3)
        .run()
      assertEquals(results.size, 1)
      assertEquals(results.head.name, "Dave")

  // --- Combined with where ---

  test("where + has combined"):
    val t = xa()
    t.connect:
      val results = QueryBuilder
        .from[ElAuthor]
        .where(_.name === "Tolkien")
        .has(ElAuthor.books)(_ >= 2)
        .run()
      assertEquals(results.size, 1)
      assertEquals(results.head.name, "Tolkien")

  // --- Constrained has ---

  test("has(ElAuthor.books) constrained with LIKE"):
    val t = xa()
    t.connect:
      val results = QueryBuilder
        .from[ElAuthor]
        .has(ElAuthor.books, _.title.like("The%"))(_ >= 1)
        .run()
      assertEquals(results.size, 1)
      assertEquals(results.head.name, "Tolkien")

  // --- SQL verification ---

  test("SQL contains count subquery with operator"):
    val frag = QueryBuilder
      .from[ElAuthor]
      .has(ElAuthor.books)(_ >= 2)
      .buildWith(H2)
    assert(
      frag.sqlString.contains("(SELECT COUNT(*) FROM"),
      s"SQL should contain count subquery: ${frag.sqlString}"
    )
    assert(
      frag.sqlString.contains(") >= ?"),
      s"SQL should contain >= ? operator: ${frag.sqlString}"
    )

end HasCountTests
