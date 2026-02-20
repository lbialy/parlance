import com.augustnagro.magnum.*

class WithCountTests extends QbTestBase:

  val h2Ddls = Seq("/h2/qb-where-has.sql")

  val authorBooks =
    Relationship.hasMany[ElAuthor, ElBook](_.id, _.authorId)

  val userRoles =
    Relationship.belongsToMany[PvUser, PvRole]("pv_user_role", "user_id", "role_id")

  // --- HasMany tests ---

  test("withCount returns all authors with book counts"):
    val t = xa()
    t.connect:
      val results =
        QueryBuilder.from[ElAuthor].orderBy(_.name).withCount(authorBooks).run()
      assertEquals(results.size, 4)
      val byName = results.map((a, c) => (a.name, c)).toMap
      assertEquals(byName("Tolkien"), 2L)
      assertEquals(byName("Asimov"), 2L)
      assertEquals(byName("Herbert"), 1L)
      assertEquals(byName("Rowling"), 0L)

  test("withCount with WHERE filter on root"):
    val t = xa()
    t.connect:
      val results = QueryBuilder
        .from[ElAuthor]
        .where(_.name === "Tolkien")
        .withCount(authorBooks)
        .run()
      assertEquals(results.size, 1)
      assertEquals(results.head._1.name, "Tolkien")
      assertEquals(results.head._2, 2L)

  test("withCount with condition on related entity"):
    val t = xa()
    t.connect:
      val results = QueryBuilder
        .from[ElAuthor]
        .orderBy(_.name)
        .withCount(authorBooks)(_.title.like("The%"))
        .run()
      assertEquals(results.size, 4)
      val byName = results.map((a, c) => (a.name, c)).toMap
      assertEquals(byName("Tolkien"), 2L)
      assertEquals(byName("Asimov"), 0L)
      assertEquals(byName("Herbert"), 0L)
      assertEquals(byName("Rowling"), 0L)

  test("withCount zero count case"):
    val t = xa()
    t.connect:
      val results = QueryBuilder
        .from[ElAuthor]
        .where(_.name === "Rowling")
        .withCount(authorBooks)
        .run()
      assertEquals(results.size, 1)
      assertEquals(results.head._2, 0L)

  test("withCount with limit"):
    val t = xa()
    t.connect:
      val results = QueryBuilder
        .from[ElAuthor]
        .orderBy(_.name)
        .limit(2)
        .withCount(authorBooks)
        .run()
      assertEquals(results.size, 2)
      assertEquals(results(0)._1.name, "Asimov")
      assertEquals(results(1)._1.name, "Herbert")

  test("withCount first()"):
    val t = xa()
    t.connect:
      val result = QueryBuilder
        .from[ElAuthor]
        .orderBy(_.name)
        .withCount(authorBooks)
        .first()
      assert(result.isDefined)
      assertEquals(result.get._1.name, "Asimov")
      assertEquals(result.get._2, 2L)

  // --- BelongsToMany tests ---

  test("withCount(btm) returns users with role counts"):
    val t = xa()
    t.connect:
      val results = QueryBuilder
        .from[PvUser]
        .orderBy(_.name)
        .withCount(userRoles)
        .run()
      assertEquals(results.size, 4)
      val byName = results.map((u, c) => (u.name, c)).toMap
      assertEquals(byName("Alice"), 2L)
      assertEquals(byName("Bob"), 1L)
      assertEquals(byName("Charlie"), 0L)
      assertEquals(byName("Dave"), 3L)

  test("withCount(btm) with condition"):
    val t = xa()
    t.connect:
      val results = QueryBuilder
        .from[PvUser]
        .orderBy(_.name)
        .withCount(userRoles)(_.name === "admin")
        .run()
      assertEquals(results.size, 4)
      val byName = results.map((u, c) => (u.name, c)).toMap
      assertEquals(byName("Alice"), 1L)
      assertEquals(byName("Bob"), 0L)
      assertEquals(byName("Charlie"), 0L)
      assertEquals(byName("Dave"), 1L)

  test("withCount(btm) WHERE + condition combined"):
    val t = xa()
    t.connect:
      val results = QueryBuilder
        .from[PvUser]
        .where(_.name === "Dave")
        .withCount(userRoles)(_.name === "admin")
        .run()
      assertEquals(results.size, 1)
      assertEquals(results.head._1.name, "Dave")
      assertEquals(results.head._2, 1L)

  test("withCount(btm) zero count case"):
    val t = xa()
    t.connect:
      val results = QueryBuilder
        .from[PvUser]
        .where(_.name === "Charlie")
        .withCount(userRoles)
        .run()
      assertEquals(results.size, 1)
      assertEquals(results.head._2, 0L)

  // --- SQL verification ---

  test("SQL verification: HasMany contains correlated COUNT subquery"):
    val frag = QueryBuilder
      .from[ElAuthor]
      .withCount(authorBooks)
      .build
    assert(
      frag.sqlString.contains("(SELECT COUNT(*)"),
      s"SQL should contain COUNT subquery: ${frag.sqlString}"
    )
    assert(
      frag.sqlString.contains("el_book.author_id = el_author.id"),
      s"SQL should contain correlation: ${frag.sqlString}"
    )

  test("SQL verification: BelongsToMany contains correlated COUNT subquery"):
    val frag = QueryBuilder
      .from[PvUser]
      .withCount(userRoles)
      .build
    assert(
      frag.sqlString.contains("(SELECT COUNT(*)"),
      s"SQL should contain COUNT subquery: ${frag.sqlString}"
    )
    assert(
      frag.sqlString.contains("pv_user_role.user_id = pv_user.id"),
      s"SQL should contain correlation: ${frag.sqlString}"
    )

  test("SQL verification: BelongsToMany with condition contains JOIN"):
    val frag = QueryBuilder
      .from[PvUser]
      .withCount(userRoles)(_.name === "admin")
      .build
    assert(
      frag.sqlString.contains("JOIN pv_role"),
      s"SQL should contain JOIN to target table: ${frag.sqlString}"
    )

  // --- Edge case ---

  test("withCount on empty result returns empty vector"):
    val t = xa()
    t.connect:
      val results = QueryBuilder
        .from[ElAuthor]
        .where(_.name === "Nobody")
        .withCount(authorBooks)
        .run()
      assertEquals(results, Vector.empty[(ElAuthor, Long)])

end WithCountTests
