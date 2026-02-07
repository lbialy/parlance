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

  // --- Phase 10: alias-qualified WHERE on joined tables ---

  val b = Columns.of[JnBook]
  val a = Columns.of[JnAuthor]

  test("whereJoined with typed operator"):
    val t = xa()
    t.connect:
      val qb = QueryBuilder.from[JnBook].join(bookAuthor)
      val results = qb.where(qb.joinedCol(a.name) === "Tolkien").run()
      assertEquals(results.size, 2)
      assert(results.forall(_._2.name == "Tolkien"))

  test("whereRoot with typed operator"):
    val t = xa()
    t.connect:
      val qb = QueryBuilder.from[JnBook].join(bookAuthor)
      val results = qb.where(qb.rootCol(b.title) === "Dune").run()
      assertEquals(results.size, 1)
      assertEquals(results.head._1.title, "Dune")
      assertEquals(results.head._2.name, "Herbert")

  test("combined root + joined where"):
    val t = xa()
    t.connect:
      val qb = QueryBuilder.from[JnBook].join(bookAuthor)
      val results = qb
        .where(qb.joinedCol(a.name) === "Tolkien")
        .where(qb.rootCol(b.title) === "The Hobbit")
        .run()
      assertEquals(results.size, 1)
      assertEquals(results.head._1.title, "The Hobbit")

  test("ambiguous column with qualification"):
    val t = xa()
    t.connect:
      val qb = QueryBuilder.from[JnBook].join(bookAuthor)
      val results = qb.where(qb.rootCol(b.id) === 5L).run()
      assertEquals(results.size, 1)
      assertEquals(results.head._1.title, "Dune")
      // also test joinedCol on id
      val results2 = qb.where(qb.joinedCol(a.id) === 1L).run()
      assertEquals(results2.size, 2)
      assert(results2.forall(_._2.name == "Tolkien"))

  test("orderBy with BoundCol"):
    val t = xa()
    t.connect:
      val qb = QueryBuilder.from[JnBook].join(bookAuthor)
      val results = qb.orderBy(qb.joinedCol(a.name)).run()
      // Asimov < Herbert < Tolkien
      assertEquals(results(0)._2.name, "Asimov")
      assertEquals(results(1)._2.name, "Asimov")
      assertEquals(results(2)._2.name, "Herbert")
      assertEquals(results(3)._2.name, "Tolkien")
      assertEquals(results(4)._2.name, "Tolkien")

  test("build SQL shows qualified WHERE columns"):
    val qb = QueryBuilder.from[JnBook].join(bookAuthor)
    val frag = qb
      .where(qb.rootCol(b.title) === "Dune")
      .where(qb.joinedCol(a.name) === "Tolkien")
      .build
    val sql = frag.sqlString
    assert(sql.contains("t0.title = ?"), s"Expected t0.title = ? in: $sql")
    assert(sql.contains("t1.name = ?"), s"Expected t1.name = ? in: $sql")

end JoinedQueryTests
