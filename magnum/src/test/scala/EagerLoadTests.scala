import com.augustnagro.magnum.*

@Table(SqlNameMapper.CamelToSnakeCase)
case class ElAuthor(@Id id: Long, name: String) derives EntityMeta

@Table(SqlNameMapper.CamelToSnakeCase)
case class ElBook(@Id id: Long, authorId: Long, title: String) derives EntityMeta

@Table(SqlNameMapper.CamelToSnakeCase)
case class ElReview(@Id id: Long, bookId: Long, score: Int, body: String) derives EntityMeta

class EagerLoadTests extends QbTestBase:

  val h2Ddls = Seq("/h2/qb-eager-load.sql")

  val authorBooks =
    Relationship.hasMany[ElAuthor, ElBook](_.id, _.authorId)

  val bookReviews =
    Relationship.hasMany[ElBook, ElReview](_.id, _.bookId)

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
      .buildWith(H2)
    assert(
      !rootFrag.sqlString.contains("JOIN"),
      s"Root query should not contain JOIN: ${rootFrag.sqlString}"
    )
    assert(
      rootFrag.sqlString.contains("SELECT"),
      s"Root query should be a SELECT: ${rootFrag.sqlString}"
    )

  // --- Constrained eager load tests ---

  test("constrained eager load filters children"):
    val t = xa()
    t.connect:
      val results = QueryBuilder
        .from[ElAuthor]
        .where(_.name === "Tolkien")
        .withRelated(authorBooks)(_.title === "The Hobbit")
        .run()
      assertEquals(results.size, 1)
      val (author, books) = results.head
      assertEquals(author.name, "Tolkien")
      assertEquals(books.size, 1)
      assertEquals(books.head.title, "The Hobbit")

  test("constrained eager load with no matching children returns empty"):
    val t = xa()
    t.connect:
      val results = QueryBuilder
        .from[ElAuthor]
        .where(_.name === "Tolkien")
        .withRelated(authorBooks)(_.title === "Nonexistent")
        .run()
      assertEquals(results.size, 1)
      assertEquals(results.head._2, Vector.empty[ElBook])

  test("constrained eager load across all authors"):
    val t = xa()
    t.connect:
      val results = QueryBuilder
        .from[ElAuthor]
        .withRelated(authorBooks)(_.title like "The%")
        .run()
      assertEquals(results.size, 4)
      val tolkien = results.find(_._1.name == "Tolkien").get
      assertEquals(tolkien._2.map(_.title).toSet, Set("The Hobbit", "The Silmarillion"))
      val asimov = results.find(_._1.name == "Asimov").get
      assertEquals(asimov._2, Vector.empty[ElBook])
      val herbert = results.find(_._1.name == "Herbert").get
      assertEquals(herbert._2, Vector.empty[ElBook])

  // --- Multiple eager load tests ---

  test("multiple withRelated: author -> books + books -> reviews"):
    val t = xa()
    t.connect:
      val results = QueryBuilder
        .from[ElBook]
        .where(_.title === "The Hobbit")
        .withRelated(bookReviews)
        .run()
      assertEquals(results.size, 1)
      val (book, reviews) = results.head
      assertEquals(book.title, "The Hobbit")
      assertEquals(reviews.size, 2)

  test("chained withRelated produces correct tuple shape"):
    val t = xa()
    t.connect:
      val authorBooksRel = Relationship.hasMany[ElAuthor, ElBook](_.id, _.authorId)
      val authorBooksAgain = Relationship.hasMany[ElAuthor, ElBook](_.id, _.authorId)
      val results = QueryBuilder
        .from[ElAuthor]
        .where(_.name === "Tolkien")
        .withRelated(authorBooksRel)
        .withRelated(authorBooksAgain)
        .run()
      assertEquals(results.size, 1)
      val row = results.head
      // row is ElAuthor *: Vector[ElBook] *: Vector[ElBook] *: EmptyTuple
      val author: ElAuthor = row.head
      val books1: Vector[ElBook] = row.tail.head
      val books2: Vector[ElBook] = row.tail.tail.head
      assertEquals(author.name, "Tolkien")
      assertEquals(books1.size, 2)
      assertEquals(books2.size, 2)

  test("chained withRelated first() returns correct tuple"):
    val t = xa()
    t.connect:
      val authorBooksRel = Relationship.hasMany[ElAuthor, ElBook](_.id, _.authorId)
      val authorBooksAgain = Relationship.hasMany[ElAuthor, ElBook](_.id, _.authorId)
      val result = QueryBuilder
        .from[ElAuthor]
        .where(_.name === "Herbert")
        .withRelated(authorBooksRel)
        .withRelated(authorBooksAgain)
        .first()
      assert(result.isDefined)
      val row = result.get
      val author: ElAuthor = row.head
      val books1: Vector[ElBook] = row.tail.head
      val books2: Vector[ElBook] = row.tail.tail.head
      assertEquals(author.name, "Herbert")
      assertEquals(books1.size, 1)
      assertEquals(books2.size, 1)

  test("chained withRelated with constrained second load"):
    val t = xa()
    t.connect:
      val authorBooksRel = Relationship.hasMany[ElAuthor, ElBook](_.id, _.authorId)
      val authorBooksFiltered = Relationship.hasMany[ElAuthor, ElBook](_.id, _.authorId)
      val results = QueryBuilder
        .from[ElAuthor]
        .where(_.name === "Tolkien")
        .withRelated(authorBooksRel)
        .withRelated(authorBooksFiltered)(_.title === "The Hobbit")
        .run()
      assertEquals(results.size, 1)
      val row = results.head
      val allBooks: Vector[ElBook] = row.tail.head
      val filteredBooks: Vector[ElBook] = row.tail.tail.head
      assertEquals(allBooks.size, 2)
      assertEquals(filteredBooks.size, 1)
      assertEquals(filteredBooks.head.title, "The Hobbit")

  test("buildQueries includes root + all defs"):
    val authorBooksRel = Relationship.hasMany[ElAuthor, ElBook](_.id, _.authorId)
    val authorBooksAgain = Relationship.hasMany[ElAuthor, ElBook](_.id, _.authorId)
    val eq = QueryBuilder
      .from[ElAuthor]
      .withRelated(authorBooksRel)
      .withRelated(authorBooksAgain)
    val queries = eq.buildQueriesWith(H2)
    // root + 1 child query + 1 child query = 3
    assertEquals(queries.size, 3)

end EagerLoadTests
