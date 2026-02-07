import com.augustnagro.magnum.*
import munit.{FunSuite, Tag}
import org.h2.jdbcx.JdbcDataSource

import java.nio.file.{Files, Path}
import scala.util.Using

@Table(H2DbType, SqlNameMapper.CamelToSnakeCase)
case class QbUser(@Id id: Long, firstName: Option[String], age: Int)
    derives DbCodec, TableMeta

class QueryBuilderBasicTests extends FunSuite:

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

  val u = Columns.of[QbUser]

  test("from[QbUser].run() returns all rows"):
    val t = xa()
    t.connect:
      val results = QueryBuilder.from[QbUser].run()
      assertEquals(results.length, 4)
      assertEquals(results.map(_.id).sorted, Vector(1L, 2L, 3L, 4L))

  test("where firstName === Some(Alice) returns 1 row"):
    val t = xa()
    t.connect:
      val results = QueryBuilder
        .from[QbUser]
        .where(u.firstName === Some("Alice"))
        .run()
      assertEquals(results.length, 1)
      assertEquals(results.head.id, 1L)
      assertEquals(results.head.firstName, Some("Alice"))

  test("compound where with age > 18 and age < 30 returns 2 rows"):
    val t = xa()
    t.connect:
      val results = QueryBuilder
        .from[QbUser]
        .where(u.age > 18)
        .where(u.age < 30)
        .run()
      assertEquals(results.length, 2)
      val ids = results.map(_.id).sorted
      assertEquals(ids, Vector(1L, 4L))

  test("where firstName.isNull returns 1 row"):
    val t = xa()
    t.connect:
      val results = QueryBuilder
        .from[QbUser]
        .where(u.firstName.isNull)
        .run()
      assertEquals(results.length, 1)
      assertEquals(results.head.id, 4L)
      assertEquals(results.head.firstName, None)

  test("where age.in(List(25, 30)) returns 2 rows"):
    val t = xa()
    t.connect:
      val results = QueryBuilder
        .from[QbUser]
        .where(u.age.in(List(25, 30)))
        .run()
      assertEquals(results.length, 2)
      val ids = results.map(_.id).sorted
      assertEquals(ids, Vector(1L, 2L))

  test("build returns correct SQL"):
    val frag = QueryBuilder
      .from[QbUser]
      .where(u.age > 18)
      .build
    assertEquals(
      frag.sqlString,
      "SELECT id, first_name, age FROM qb_user WHERE age > ?"
    )

  test("empty in() produces no results"):
    val t = xa()
    t.connect:
      val results = QueryBuilder
        .from[QbUser]
        .where(u.age.in(List.empty[Int]))
        .run()
      assertEquals(results.length, 0)

end QueryBuilderBasicTests
