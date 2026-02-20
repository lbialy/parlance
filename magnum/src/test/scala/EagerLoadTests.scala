import com.augustnagro.magnum.*

@Table(H2DbType, SqlNameMapper.CamelToSnakeCase)
case class ElAuthor(@Id id: Long, name: String) derives DbCodec, TableMeta

@Table(H2DbType, SqlNameMapper.CamelToSnakeCase)
case class ElBook(@Id id: Long, authorId: Long, title: String) derives DbCodec, TableMeta

class EagerLoadTests extends QbTestBase:

  val h2Ddls = Seq("/h2/qb-eager-load.sql")

  val authorBooks =
    Relationship.hasMany[ElAuthor, ElBook](_.id, _.authorId)

  test("basic eager load returns all authors with books"):
    val t = xa()
    t.connect:
      val results =
        QueryBuilder.from[ElAuthor].withRelated(authorBooks).run()
      assertEquals(results.size, 4)
      val tolkien = results.find(_._1.name == "Tolkien").get
      assertEquals(tolkien._2.size, 2)
      val asimov = results.find(_._1.name == "Asimov").get
      assertEquals(asimov._2.size, 2)
      val herbert = results.find(_._1.name == "Herbert").get
      assertEquals(herbert._2.size, 1)
      assertEquals(herbert._2.head.title, "Dune")

  test("author with 0 books gets empty vector"):
    val t = xa()
    t.connect:
      val results =
        QueryBuilder.from[ElAuthor].withRelated(authorBooks).run()
      val rowling = results.find(_._1.name == "Rowling").get
      assertEquals(rowling._2, Vector.empty[ElBook])

  test("author with multiple books gets all books"):
    val t = xa()
    t.connect:
      val results =
        QueryBuilder.from[ElAuthor].withRelated(authorBooks).run()
      val tolkien = results.find(_._1.name == "Tolkien").get
      val titles = tolkien._2.map(_.title).toSet
      assertEquals(titles, Set("The Hobbit", "The Silmarillion"))
      val asimov = results.find(_._1.name == "Asimov").get
      val asimovTitles = asimov._2.map(_.title).toSet
      assertEquals(asimovTitles, Set("Foundation", "I, Robot"))

  test("with WHERE on root query"):
    val t = xa()
    t.connect:
      val results = QueryBuilder
        .from[ElAuthor]
        .where(_.name === "Tolkien")
        .withRelated(authorBooks)
        .run()
      assertEquals(results.size, 1)
      assertEquals(results.head._1.name, "Tolkien")
      assertEquals(results.head._2.size, 2)

  test("empty root result returns empty vector"):
    val t = xa()
    t.connect:
      val results = QueryBuilder
        .from[ElAuthor]
        .where(_.name === "Nobody")
        .withRelated(authorBooks)
        .run()
      assertEquals(results, Vector.empty)

  test("with ORDER BY preserves author ordering"):
    val t = xa()
    t.connect:
      val results = QueryBuilder
        .from[ElAuthor]
        .orderBy(_.name)
        .withRelated(authorBooks)
        .run()
      assertEquals(results.size, 4)
      assertEquals(results(0)._1.name, "Asimov")
      assertEquals(results(1)._1.name, "Herbert")
      assertEquals(results(2)._1.name, "Rowling")
      assertEquals(results(3)._1.name, "Tolkien")

  test("with LIMIT returns only limited authors with correct books"):
    val t = xa()
    t.connect:
      val results = QueryBuilder
        .from[ElAuthor]
        .orderBy(_.name)
        .limit(2)
        .withRelated(authorBooks)
        .run()
      assertEquals(results.size, 2)
      assertEquals(results(0)._1.name, "Asimov")
      assertEquals(results(0)._2.size, 2)
      assertEquals(results(1)._1.name, "Herbert")
      assertEquals(results(1)._2.size, 1)

  test("first() returns single author with books"):
    val t = xa()
    t.connect:
      val result = QueryBuilder
        .from[ElAuthor]
        .where(_.name === "Herbert")
        .withRelated(authorBooks)
        .first()
      assert(result.isDefined)
      val (author, books) = result.get
      assertEquals(author.name, "Herbert")
      assertEquals(books.size, 1)
      assertEquals(books.head.title, "Dune")

  test("SQL verification: root has no JOIN, child uses IN"):
    val rootFrag = QueryBuilder
      .from[ElAuthor]
      .where(_.name === "Tolkien")
      .build
    assert(
      !rootFrag.sqlString.contains("JOIN"),
      s"Root query should not contain JOIN: ${rootFrag.sqlString}"
    )
    assert(
      rootFrag.sqlString.contains("SELECT"),
      s"Root query should be a SELECT: ${rootFrag.sqlString}"
    )

end EagerLoadTests
