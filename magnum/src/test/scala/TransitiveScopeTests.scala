import com.augustnagro.magnum.*

/** Tests that scopes on related entities are transitively applied through whereHas, doesntHave, has, withCount, withRelated, and join.
  *
  * Uses the existing el_author/el_book schema from qb-where-has.sql. Data:
  *   - Tolkien: "The Hobbit", "The Silmarillion"
  *   - Asimov: "Foundation", "I, Robot"
  *   - Herbert: "Dune"
  *   - Rowling: (no books)
  *
  * The "published books" scope filters books whose title starts with "The" or is "Dune" or "Foundation" — we use a simple scope that
  * excludes "I, Robot" (id=4) to test transitivity.
  *
  * Also uses pv_user/pv_role/pv_user_role from the same DDL.
  *   - Alice: admin+editor, Bob: editor, Charlie: no roles, Dave: admin+editor+viewer
  */
class TransitiveScopeTests extends QbTestBase:

  val h2Ddls = Seq("/h2/qb-where-has.sql")

  // --- Relationships ---

  val authorBooks =
    Relationship.hasMany[ElAuthor, ElBook](_.id, _.authorId)

  val userRoles =
    Relationship.belongsToMany[PvUser, PvRole]("pv_user_role", "user_id", "role_id")

  // --- Scoped book repo that excludes book id=4 ("I, Robot") ---

  val excludeIRobotScope = new Scope[ElBook]:
    override def conditions(meta: TableMeta[ElBook]): Vector[WhereFrag] =
      Vector(Frag(s"${meta.tableName}.id <> 4", Seq.empty, FragWriter.empty).unsafeAsWhere)

  given bookScoped: Scoped[ElBook] with
    def scopes: Vector[Scope[ElBook]] = Vector(excludeIRobotScope)

  // --- Scoped role repo that only includes "admin" roles ---

  val adminOnlyScope = new Scope[PvRole]:
    override def conditions(meta: TableMeta[PvRole]): Vector[WhereFrag] =
      Vector(Frag(s"${meta.tableName}.name = 'admin'", Seq.empty, FragWriter.empty).unsafeAsWhere)

  given roleScoped: Scoped[PvRole] with
    def scopes: Vector[Scope[PvRole]] = Vector(adminOnlyScope)

  // --- whereHas: HasMany with transitive scope ---

  test("whereHas applies book scope transitively"):
    val t = xa()
    t.connect:
      // Without scope: Tolkien(2), Asimov(2), Herbert(1) have books → 3 authors
      // With scope excluding "I, Robot": Asimov now has only "Foundation" → still 3 authors
      val results =
        QueryBuilder.from[ElAuthor].whereHas(authorBooks).orderBy(_.name).run()
      assertEquals(results.size, 3)
      assertEquals(results.map(_.name), Vector("Asimov", "Herbert", "Tolkien"))

  test("whereHas with condition + transitive scope"):
    val t = xa()
    t.connect:
      // Looking for authors with books titled "I, Robot" — but scope excludes it
      val results = QueryBuilder
        .from[ElAuthor]
        .whereHas(authorBooks)(_.title === "I, Robot")
        .run()
      assertEquals(results.size, 0)

  test("whereHas with condition that passes scope"):
    val t = xa()
    t.connect:
      // "Foundation" (id=3) is not excluded by scope
      val results = QueryBuilder
        .from[ElAuthor]
        .whereHas(authorBooks)(_.title === "Foundation")
        .run()
      assertEquals(results.size, 1)
      assertEquals(results.head.name, "Asimov")

  // --- doesntHave: HasMany with transitive scope ---

  test("doesntHave applies book scope transitively"):
    val t = xa()
    t.connect:
      // Without scope: only Rowling has no books
      // With scope excluding "I, Robot": Rowling still has no books → 1 author
      // (Asimov still has Foundation, so he's not included)
      val results =
        QueryBuilder.from[ElAuthor].doesntHave(authorBooks).run()
      assertEquals(results.size, 1)
      assertEquals(results.head.name, "Rowling")

  // --- whereHas: BelongsToMany with transitive scope ---

  test("whereHas(btm) applies role scope transitively"):
    val t = xa()
    t.connect:
      // Without scope: Alice(admin+editor), Bob(editor), Dave(admin+editor+viewer) → 3 users
      // With admin-only scope: Alice(admin), Dave(admin) → 2 users
      val results = QueryBuilder
        .from[PvUser]
        .whereHas(userRoles)
        .orderBy(_.name)
        .run()
      assertEquals(results.size, 2)
      assertEquals(results.map(_.name), Vector("Alice", "Dave"))

  test("doesntHave(btm) applies role scope transitively"):
    val t = xa()
    t.connect:
      // With admin-only scope: Bob and Charlie don't have admin role
      val results = QueryBuilder
        .from[PvUser]
        .doesntHave(userRoles)
        .orderBy(_.name)
        .run()
      assertEquals(results.size, 2)
      assertEquals(results.map(_.name), Vector("Bob", "Charlie"))

  // --- withRelated: HasMany with transitive scope ---

  test("withRelated applies book scope transitively"):
    val t = xa()
    t.connect:
      val results = QueryBuilder
        .from[ElAuthor]
        .orderBy(_.name)
        .withRelated(authorBooks)
        .run()
      assertEquals(results.size, 4)
      // Asimov: only "Foundation" (not "I, Robot")
      val asimov = results.find(_._1.name == "Asimov").get
      assertEquals(asimov._2.size, 1)
      assertEquals(asimov._2.head.title, "Foundation")
      // Tolkien: both books (neither excluded)
      val tolkien = results.find(_._1.name == "Tolkien").get
      assertEquals(tolkien._2.size, 2)

  // --- withRelated: BelongsToMany with transitive scope ---

  test("withRelated(btm) applies role scope transitively"):
    val t = xa()
    t.connect:
      val results = QueryBuilder
        .from[PvUser]
        .orderBy(_.name)
        .withRelated(userRoles)
        .run()
      assertEquals(results.size, 4)
      // Alice: only admin (not editor)
      val alice = results.find(_._1.name == "Alice").get
      assertEquals(alice._2.size, 1)
      assertEquals(alice._2.head.name, "admin")
      // Bob: no admin role → empty
      val bob = results.find(_._1.name == "Bob").get
      assertEquals(bob._2.size, 0)
      // Dave: only admin (not editor, viewer)
      val dave = results.find(_._1.name == "Dave").get
      assertEquals(dave._2.size, 1)

  // --- withCount: HasMany with transitive scope ---

  test("withCount applies book scope transitively"):
    val t = xa()
    t.connect:
      val results = QueryBuilder
        .from[ElAuthor]
        .orderBy(_.name)
        .withCount(authorBooks)
        .run()
      assertEquals(results.size, 4)
      val byName = results.map((a, c) => (a.name, c)).toMap
      assertEquals(byName("Tolkien"), 2L)
      assertEquals(byName("Asimov"), 1L) // "I, Robot" excluded
      assertEquals(byName("Herbert"), 1L)
      assertEquals(byName("Rowling"), 0L)

  test("withCount(btm) applies role scope transitively"):
    val t = xa()
    t.connect:
      val results = QueryBuilder
        .from[PvUser]
        .orderBy(_.name)
        .withCount(userRoles)
        .run()
      assertEquals(results.size, 4)
      val byName = results.map((u, c) => (u.name, c)).toMap
      assertEquals(byName("Alice"), 1L) // only admin
      assertEquals(byName("Bob"), 0L) // no admin
      assertEquals(byName("Charlie"), 0L)
      assertEquals(byName("Dave"), 1L) // only admin

  // --- has: HasMany with transitive scope ---

  test("has applies book scope transitively"):
    val t = xa()
    t.connect:
      // Asimov has 1 book with scope (Foundation only)
      // asking for authors with >= 2 books: only Tolkien
      val results = QueryBuilder
        .from[ElAuthor]
        .has(authorBooks)(_ >= 2)
        .orderBy(_.name)
        .run()
      assertEquals(results.size, 1)
      assertEquals(results.head.name, "Tolkien")

  // --- join: with transitive scope ---

  test("join applies book scope transitively"):
    val t = xa()
    t.connect:
      val results = QueryBuilder
        .from[ElAuthor]
        .join(authorBooks)
        .run()
      // Without scope: 5 rows (Tolkien×2, Asimov×2, Herbert×1)
      // With scope: 4 rows (Tolkien×2, Asimov×1, Herbert×1) — "I, Robot" excluded
      assertEquals(results.size, 4)
      val titles = results.map(_._2.title).sorted
      assert(!titles.contains("I, Robot"), s"Should not contain excluded book, got: $titles")
      assert(titles.contains("Foundation"), s"Should contain non-excluded book, got: $titles")

  test("leftJoin applies book scope transitively"):
    val t = xa()
    t.connect:
      val results = QueryBuilder
        .from[ElAuthor]
        .leftJoin(authorBooks)
        .run()
      // 5 rows: Tolkien×2, Asimov×1 (Foundation only), Herbert×1, Rowling×1 (None)
      assertEquals(results.size, 5)
      val asimovRows = results.filter(_._1.name == "Asimov")
      assertEquals(asimovRows.size, 1)
      assertEquals(asimovRows.head._2.get.title, "Foundation")

  // --- SQL verification ---

  test("SQL verification: whereHas with scope includes scope condition in EXISTS"):
    val frag = QueryBuilder
      .from[ElAuthor]
      .whereHas(authorBooks)
      .buildWith(H2)
    assert(
      frag.sqlString.contains("el_book.id <> 4"),
      s"SQL should contain scope condition: ${frag.sqlString}"
    )

  test("SQL verification: withCount with scope includes scope condition"):
    val frag = QueryBuilder
      .from[ElAuthor]
      .withCount(authorBooks)
      .buildWith(H2)
    val sql = frag.sqlString
    assert(
      sql.contains("el_book.id <> 4"),
      s"SQL should contain scope condition: $sql"
    )
end TransitiveScopeTests
