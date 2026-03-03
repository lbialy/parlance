import com.augustnagro.magnum.*

class HasCountTests extends QbTestBase:

  val h2Ddls = Seq("/h2/qb-where-has.sql")

  val authorBooks =
    Relationship.hasMany[ElAuthor, ElBook](_.id, _.authorId)

  val userRoles =
    Relationship.belongsToMany[PvUser, PvRole]("pv_user_role", "user_id", "role_id")

  // --- HasMany unconstrained ---

  test("has(authorBooks)(_ >= 2) returns authors with 2+ books"):
    val t = xa()
    t.connect:
      val results = QueryBuilder
        .from[ElAuthor]
        .has(authorBooks)(_ >= 2)
        .orderBy(_.name)
        .run()
      assertEquals(results.size, 2)
      assertEquals(results.map(_.name), Vector("Asimov", "Tolkien"))

  test("has(authorBooks)(_ >= 1) returns authors with 1+ books"):
    val t = xa()
    t.connect:
      val results = QueryBuilder
        .from[ElAuthor]
        .has(authorBooks)(_ >= 1)
        .orderBy(_.name)
        .run()
      assertEquals(results.size, 3)
      assertEquals(results.map(_.name), Vector("Asimov", "Herbert", "Tolkien"))

  test("has(authorBooks)(_ === 0) returns authors with no books"):
    val t = xa()
    t.connect:
      val results = QueryBuilder
        .from[ElAuthor]
        .has(authorBooks)(_ === 0)
        .run()
      assertEquals(results.size, 1)
      assertEquals(results.head.name, "Rowling")

  test("has(authorBooks)(_ > 1) returns authors with more than 1 book"):
    val t = xa()
    t.connect:
      val results = QueryBuilder
        .from[ElAuthor]
        .has(authorBooks)(_ > 1)
        .orderBy(_.name)
        .run()
      assertEquals(results.size, 2)
      assertEquals(results.map(_.name), Vector("Asimov", "Tolkien"))

  test("has(authorBooks)(_ < 2) returns authors with fewer than 2 books"):
    val t = xa()
    t.connect:
      val results = QueryBuilder
        .from[ElAuthor]
        .has(authorBooks)(_ < 2)
        .orderBy(_.name)
        .run()
      assertEquals(results.size, 2)
      assertEquals(results.map(_.name), Vector("Herbert", "Rowling"))

  // --- BelongsToMany unconstrained ---

  test("has(userRoles)(_ >= 2) returns users with 2+ roles"):
    val t = xa()
    t.connect:
      val results = QueryBuilder
        .from[PvUser]
        .has(userRoles)(_ >= 2)
        .orderBy(_.name)
        .run()
      assertEquals(results.size, 2)
      assertEquals(results.map(_.name), Vector("Alice", "Dave"))

  test("has(userRoles)(_ >= 3) returns users with 3+ roles"):
    val t = xa()
    t.connect:
      val results = QueryBuilder
        .from[PvUser]
        .has(userRoles)(_ >= 3)
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
        .has(authorBooks)(_ >= 2)
        .run()
      assertEquals(results.size, 1)
      assertEquals(results.head.name, "Tolkien")

  // --- Constrained has ---

  test("has(authorBooks) constrained with LIKE"):
    val t = xa()
    t.connect:
      val results = QueryBuilder
        .from[ElAuthor]
        .has(authorBooks, _.title.like("The%"))(_ >= 1)
        .run()
      assertEquals(results.size, 1)
      assertEquals(results.head.name, "Tolkien")

  // --- SQL verification ---

  test("SQL contains count subquery with operator"):
    val frag = QueryBuilder
      .from[ElAuthor]
      .has(authorBooks)(_ >= 2)
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
