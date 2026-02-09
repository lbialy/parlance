import com.augustnagro.magnum.*
import munit.{FunSuite, Tag}
import org.h2.jdbcx.JdbcDataSource

import java.nio.file.{Files, Path}
import scala.util.Using

class AggregateTests extends FunSuite:

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
    val chunkDdl = Files.readString(
      Path.of(getClass.getResource("/h2/qb-chunk.sql").toURI)
    )
    val userDdl = Files.readString(
      Path.of(getClass.getResource("/h2/qb-user.sql").toURI)
    )
    Using.Manager: use =>
      val con = use(ds.getConnection)
      val stmt = use(con.createStatement)
      stmt.execute(chunkDdl)
      stmt.execute(userDdl)

    Transactor(ds)

  // --- SUM ---

  test("sum returns total"):
    val t = xa()
    t.connect:
      val result = QueryBuilder.from[QbItem].sum(_.amount)
      assertEquals(result, Some(50500))

  test("sum with where"):
    val t = xa()
    t.connect:
      val result = QueryBuilder.from[QbItem]
        .where(_.amount > 500)
        .sum(_.amount)
      assertEquals(result, Some(37750))

  test("sum on empty returns None"):
    val t = xa()
    t.connect:
      val result = QueryBuilder.from[QbItem]
        .where(_.amount > 99999)
        .sum(_.amount)
      assertEquals(result, None)

  // --- AVG ---

  test("avg returns Double"):
    val t = xa()
    t.connect:
      val result = QueryBuilder.from[QbItem].avg(_.amount)
      assertEquals(result, Some(505.0))

  test("avg with where"):
    val t = xa()
    t.connect:
      val result = QueryBuilder.from[QbItem]
        .where(_.amount > 500)
        .avg(_.amount)
      assertEquals(result, Some(755.0))

  test("avg on empty returns None"):
    val t = xa()
    t.connect:
      val result = QueryBuilder.from[QbItem]
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
      val result = QueryBuilder.from[QbItem]
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
      val result = QueryBuilder.from[QbItem]
        .where(_.amount > 500)
        .max(_.amount)
      assertEquals(result, Some(1000))

  // --- MIN/MAX on empty ---

  test("min/max on empty returns None"):
    val t = xa()
    t.connect:
      val minResult = QueryBuilder.from[QbItem]
        .where(_.amount > 99999)
        .min(_.amount)
      val maxResult = QueryBuilder.from[QbItem]
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
