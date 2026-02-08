import com.augustnagro.magnum.*
import munit.{FunSuite, Tag}
import org.h2.jdbcx.JdbcDataSource

import java.nio.file.{Files, Path}
import scala.util.Using

class WhereHasTests extends FunSuite:

  override def munitTestTransforms: List[TestTransform] =
    super.munitTestTransforms :+ new TestTransform(
      "QB",
      test => test.withTags(test.tags + new Tag("QB"))
    )

  lazy val h2DbPath = Files.createTempDirectory(null).toAbsolutePath

  def xa(): Transactor =
    val ds = JdbcDataSource()
    ds.setURL("jdbc:h2:" + h2DbPath)
    ds.setUser("sa")
    ds.setPassword("")
    val ddl = Files.readString(
      Path.of(getClass.getResource("/h2/qb-where-has.sql").toURI)
    )
    Using.Manager: use =>
      val con = use(ds.getConnection)
      val stmt = use(con.createStatement)
      stmt.execute(ddl)

    Transactor(ds)

  val authorBooks =
    Relationship.hasMany[ElAuthor, ElBook](_.id, _.authorId)

  val userRoles =
    Relationship.belongsToMany[PvUser, PvRole]("pv_user_role", "user_id", "role_id")

  // --- Relationship (HasMany) tests ---

  test("whereHas returns only authors with books"):
    val t = xa()
    t.connect:
      val results =
        QueryBuilder.from[ElAuthor].whereHas(authorBooks).orderBy(_.name).run()
      assertEquals(results.size, 3)
      assertEquals(results.map(_.name), Vector("Asimov", "Herbert", "Tolkien"))

  test("doesntHave returns only authors without books"):
    val t = xa()
    t.connect:
      val results =
        QueryBuilder.from[ElAuthor].doesntHave(authorBooks).run()
      assertEquals(results.size, 1)
      assertEquals(results.head.name, "Rowling")

  test("whereHas with condition (exact match)"):
    val t = xa()
    t.connect:
      val results = QueryBuilder
        .from[ElAuthor]
        .whereHas(authorBooks)(_.title === "Dune")
        .run()
      assertEquals(results.size, 1)
      assertEquals(results.head.name, "Herbert")

  test("whereHas with condition (LIKE)"):
    val t = xa()
    t.connect:
      val results = QueryBuilder
        .from[ElAuthor]
        .whereHas(authorBooks)(_.title.like("The%"))
        .run()
      assertEquals(results.size, 1)
      assertEquals(results.head.name, "Tolkien")

  test("whereHas + where combined"):
    val t = xa()
    t.connect:
      val results = QueryBuilder
        .from[ElAuthor]
        .where(_.name === "Tolkien")
        .whereHas(authorBooks)
        .run()
      assertEquals(results.size, 1)
      assertEquals(results.head.name, "Tolkien")

  test("whereHas + orderBy + limit"):
    val t = xa()
    t.connect:
      val results = QueryBuilder
        .from[ElAuthor]
        .whereHas(authorBooks)
        .orderBy(_.name)
        .limit(2)
        .run()
      assertEquals(results.size, 2)
      assertEquals(results(0).name, "Asimov")
      assertEquals(results(1).name, "Herbert")

  test("SQL verification: contains EXISTS subquery"):
    val frag = QueryBuilder
      .from[ElAuthor]
      .whereHas(authorBooks)
      .build
    assert(
      frag.sqlString.contains("EXISTS (SELECT 1 FROM"),
      s"SQL should contain EXISTS subquery: ${frag.sqlString}"
    )
    assert(
      frag.sqlString.contains("el_book.author_id = el_author.id"),
      s"SQL should contain correlation: ${frag.sqlString}"
    )

  // --- BelongsToMany tests ---

  test("whereHas(btm) returns users with any roles"):
    val t = xa()
    t.connect:
      val results = QueryBuilder
        .from[PvUser]
        .whereHas(userRoles)
        .orderBy(_.name)
        .run()
      assertEquals(results.size, 3)
      assertEquals(results.map(_.name), Vector("Alice", "Bob", "Dave"))

  test("doesntHave(btm) returns users without roles"):
    val t = xa()
    t.connect:
      val results = QueryBuilder
        .from[PvUser]
        .doesntHave(userRoles)
        .run()
      assertEquals(results.size, 1)
      assertEquals(results.head.name, "Charlie")

  test("whereHas(btm, condition) on target"):
    val t = xa()
    t.connect:
      val results = QueryBuilder
        .from[PvUser]
        .whereHas(userRoles)(_.name === "admin")
        .orderBy(_.name)
        .run()
      assertEquals(results.size, 2)
      assertEquals(results.map(_.name), Vector("Alice", "Dave"))

  test("doesntHave + where combined"):
    val userRoles = Relationship.belongsToMany[PvUser, PvRole]("pv_user_role", "user_id", "role_id")
    val t = xa()
    t.connect:
      val results = QueryBuilder
        .from[PvUser]
        .doesntHave(userRoles)
        .where(_.name === "Charlie")
        .run()
      assertEquals(results.size, 1)
      assertEquals(results.head.name, "Charlie")

  // --- Edge cases ---

  test("whereHas on empty result"):
    val t = xa()
    t.connect:
      val results = QueryBuilder
        .from[ElAuthor]
        .where(_.name === "Nobody")
        .whereHas(authorBooks)
        .run()
      assertEquals(results, Vector.empty[ElAuthor])

  test("chained whereHas + doesntHave (contradictory) returns empty"):
    val t = xa()
    t.connect:
      val results = QueryBuilder
        .from[ElAuthor]
        .whereHas(authorBooks)
        .doesntHave(authorBooks)
        .run()
      assertEquals(results, Vector.empty[ElAuthor])

end WhereHasTests
