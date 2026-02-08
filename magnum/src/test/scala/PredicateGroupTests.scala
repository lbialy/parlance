import com.augustnagro.magnum.*
import munit.{FunSuite, Tag}
import org.h2.jdbcx.JdbcDataSource

import java.nio.file.{Files, Path}
import scala.util.Using

class PredicateGroupTests extends FunSuite:

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
      Path.of(getClass.getResource("/h2/qb-user.sql").toURI)
    )
    Using.Manager: use =>
      val con = use(ds.getConnection)
      val stmt = use(con.createStatement)
      stmt.execute(ddl)

    Transactor(ds)

  test("orWhere SQL generation"):
    val frag = QueryBuilder
      .from[QbUser]
      .where(_.age > 18)
      .orWhere(_.age < 5)
      .build
    assertEquals(
      frag.sqlString,
      "SELECT id, first_name, age FROM qb_user WHERE (age > ? OR age < ?)"
    )

  test("orWhere returns correct rows"):
    val t = xa()
    t.connect:
      val results = QueryBuilder
        .from[QbUser]
        .where(_.firstName === Some("Alice"))
        .orWhere(_.firstName === Some("Bob"))
        .run()
      assertEquals(results.length, 2)
      val names = results.flatMap(_.firstName).sorted
      assertEquals(names, Vector("Alice", "Bob"))

  test("whereGroup + orWhereGroup SQL generation"):
    val frag = QueryBuilder
      .from[QbUser]
      .whereGroup(g => g.and(_.age > 20).and(_.age < 26))
      .orWhereGroup(g => g.and(_.firstName === Some("Bob")))
      .build
    assertEquals(
      frag.sqlString,
      "SELECT id, first_name, age FROM qb_user WHERE ((age > ? AND age < ?) OR first_name = ?)"
    )

  test("whereGroup + orWhereGroup returns correct rows"):
    val t = xa()
    t.connect:
      val results = QueryBuilder
        .from[QbUser]
        .whereGroup(g => g.and(_.age > 20).and(_.age < 26))
        .orWhereGroup(g => g.and(_.firstName === Some("Bob")))
        .run()
      assertEquals(results.length, 3)
      val ids = results.map(_.id).sorted
      assertEquals(ids, Vector(1L, 2L, 4L))

  test("whereGroup with or() builds OR group"):
    val t = xa()
    t.connect:
      val results = QueryBuilder
        .from[QbUser]
        .where(_.age > 10)
        .whereGroup(g =>
          g.or(_.firstName === Some("Alice"))
            .or(_.firstName === Some("Bob"))
        )
        .run()
      assertEquals(results.length, 2)
      val frag = QueryBuilder
        .from[QbUser]
        .where(_.age > 10)
        .whereGroup(g =>
          g.or(_.firstName === Some("Alice"))
            .or(_.firstName === Some("Bob"))
        )
        .build
      assertEquals(
        frag.sqlString,
        "SELECT id, first_name, age FROM qb_user WHERE (age > ? AND (first_name = ? OR first_name = ?))"
      )

  test("where + where still ANDs (regression)"):
    val t = xa()
    t.connect:
      val results = QueryBuilder
        .from[QbUser]
        .where(_.age > 18)
        .where(_.age < 30)
        .run()
      assertEquals(results.length, 2)
      val ids = results.map(_.id).sorted
      assertEquals(ids, Vector(1L, 4L))

  test("existing terminal ops work with new predicate state"):
    val t = xa()
    t.connect:
      val c = QueryBuilder
        .from[QbUser]
        .where(_.age > 18)
        .orWhere(_.firstName === Some("Charlie"))
        .count()
      assertEquals(c, 4L)

end PredicateGroupTests
