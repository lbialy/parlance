import com.augustnagro.magnum.*
import munit.{FunSuite, Tag}
import org.h2.jdbcx.JdbcDataSource

import java.nio.file.{Files, Path}
import scala.util.Using

@Table(H2DbType, SqlNameMapper.CamelToSnakeCase)
case class JnAuthor(@Id id: Long, name: String) derives DbCodec, TableMeta

@Table(H2DbType, SqlNameMapper.CamelToSnakeCase)
case class JnBook(@Id id: Long, authorId: Long, title: String)
    derives DbCodec, TableMeta

class JoinedQueryTests extends FunSuite:

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
      Path.of(getClass.getResource("/h2/qb-join.sql").toURI)
    )
    Using.Manager: use =>
      val con = use(ds.getConnection)
      val stmt = use(con.createStatement)
      stmt.execute(ddl)

    Transactor(ds)

  val bookAuthor = Relationship.belongsTo[JnBook, JnAuthor](_.authorId, _.id)

  test("join returns all tuples"):
    val t = xa()
    t.connect:
      val results = QueryBuilder.from[JnBook].join(bookAuthor).run()
      assertEquals(results.size, 5)
      val dune = results.find(_._1.title == "Dune")
      assert(dune.isDefined)
      assertEquals(dune.get._2.name, "Herbert")

  test("join + where filters on unambiguous column"):
    val t = xa()
    t.connect:
      val results = QueryBuilder
        .from[JnBook]
        .join(bookAuthor)
        .where(sql"title = ${"Dune"}")
        .run()
      assertEquals(results.size, 1)
      assertEquals(results.head._1.title, "Dune")
      assertEquals(results.head._2.name, "Herbert")

  test("join + orderBy + limit"):
    val t = xa()
    t.connect:
      val results = QueryBuilder
        .from[JnBook]
        .join(bookAuthor)
        .orderBy(Col("title", "title"), SortOrder.Asc)
        .limit(2)
        .run()
      assertEquals(results.size, 2)
      assertEquals(results(0)._1.title, "Dune")
      assertEquals(results(1)._1.title, "Foundation")

  test("build produces correct SQL"):
    val frag = QueryBuilder
      .from[JnBook]
      .join(bookAuthor)
      .build
    val sql = frag.sqlString
    assert(sql.contains("t0."), s"Expected t0. alias in: $sql")
    assert(sql.contains("t1."), s"Expected t1. alias in: $sql")
    assert(sql.contains("INNER JOIN"), s"Expected INNER JOIN in: $sql")
    assert(
      sql.contains("ON t0.author_id = t1.id"),
      s"Expected ON clause in: $sql"
    )

  test("count with join"):
    val t = xa()
    t.connect:
      val result = QueryBuilder.from[JnBook].join(bookAuthor).count()
      assertEquals(result, 5L)

  test("count with join + where"):
    val t = xa()
    t.connect:
      val result = QueryBuilder
        .from[JnBook]
        .join(bookAuthor)
        .where(sql"name = ${"Tolkien"}")
        .count()
      assertEquals(result, 2L)

  test("exists with join"):
    val t = xa()
    t.connect:
      assert(QueryBuilder.from[JnBook].join(bookAuthor).exists())
      assert(
        !QueryBuilder
          .from[JnBook]
          .join(bookAuthor)
          .where(sql"name = ${"Nobody"}")
          .exists()
      )

  test("firstOrFail with join"):
    val t = xa()
    t.connect:
      val (book, author) = QueryBuilder
        .from[JnBook]
        .join(bookAuthor)
        .where(sql"title = ${"Foundation"}")
        .firstOrFail()
      assertEquals(book.title, "Foundation")
      assertEquals(author.name, "Asimov")

end JoinedQueryTests
