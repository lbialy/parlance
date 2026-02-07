import com.augustnagro.magnum.*
import munit.{FunSuite, Tag}
import org.h2.jdbcx.JdbcDataSource

import java.nio.file.{Files, Path}
import scala.util.Using

class QueryBuilderTerminalTests extends FunSuite:

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

  test("first() returns Some when rows exist"):
    val t = xa()
    t.connect:
      val result = QueryBuilder.from[QbUser].orderBy(u.age).first()
      assert(result.isDefined)
      assertEquals(result.get.age, 17)

  test("first() returns None when no rows match"):
    val t = xa()
    t.connect:
      val result = QueryBuilder.from[QbUser].where(u.age > 100).first()
      assertEquals(result, None)

  test("firstOrFail() returns entity"):
    val t = xa()
    t.connect:
      val result = QueryBuilder
        .from[QbUser]
        .where(u.firstName === Some("Alice"))
        .firstOrFail()
      assertEquals(result.firstName, Some("Alice"))

  test("firstOrFail() throws on empty"):
    val t = xa()
    t.connect:
      intercept[QueryBuilderException]:
        QueryBuilder.from[QbUser].where(u.age > 100).firstOrFail()

  test("count() returns total count"):
    val t = xa()
    t.connect:
      val result = QueryBuilder.from[QbUser].count()
      assertEquals(result, 4L)

  test("where(...).count() returns filtered count"):
    val t = xa()
    t.connect:
      val result = QueryBuilder.from[QbUser].where(u.age > 18).count()
      assertEquals(result, 3L)

  test("exists() returns true/false"):
    val t = xa()
    t.connect:
      assert(QueryBuilder.from[QbUser].exists())
      assert(!QueryBuilder.from[QbUser].where(u.age > 100).exists())

end QueryBuilderTerminalTests
