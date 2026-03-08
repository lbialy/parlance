import com.augustnagro.magnum.*

trait JoinedSelectTestsDefs:
  self: QbTestBase[?] =>

  test("join select: columns from both tables"):
    val t = xa()
    t.connect:
      val results = QueryBuilder
        .from[JnBook]
        .join(JnBook.author)
        .select(jq =>
          (
            title = jq.of[JnBook].title,
            author = jq.of[JnAuthor].name
          )
        )
        .run()
      assertEquals(results.length, 5)
      val dune = results.find(_.title == "Dune")
      assert(dune.isDefined)
      assertEquals(dune.get.author, "Herbert")

  test("join select + GROUP BY + COUNT"):
    val t = xa()
    t.connect:
      val results = QueryBuilder
        .from[JnBook]
        .join(JnBook.author)
        .select(jq =>
          (
            author = jq.of[JnAuthor].name,
            cnt = Expr.count
          )
        )
        .groupBy(_.author)
        .orderBy(_.author)
        .run()
      assertEquals(results.length, 3)
      assertEquals(results(0).author, "Asimov")
      assertEquals(results(0).cnt, 2L)
      assertEquals(results(1).author, "Herbert")
      assertEquals(results(1).cnt, 1L)
      assertEquals(results(2).author, "Tolkien")
      assertEquals(results(2).cnt, 2L)

  test("join select + HAVING"):
    val t = xa()
    t.connect:
      val results = QueryBuilder
        .from[JnBook]
        .join(JnBook.author)
        .select(jq =>
          (
            author = jq.of[JnAuthor].name,
            cnt = Expr.count
          )
        )
        .groupBy(_.author)
        .having(_.cnt > 1L)
        .run()
      assertEquals(results.length, 2)
      val names = results.map(_.author).sorted
      assertEquals(names, Vector("Asimov", "Tolkien"))

  test("join select + ORDER BY on projected column"):
    val t = xa()
    t.connect:
      val results = QueryBuilder
        .from[JnBook]
        .join(JnBook.author)
        .select(jq =>
          (
            author = jq.of[JnAuthor].name,
            cnt = Expr.count
          )
        )
        .groupBy(_.author)
        .orderBy(_.cnt, SortOrder.Desc)
        .run()
      // Tolkien=2, Asimov=2, Herbert=1
      assertEquals(results.last.cnt, 1L)
      assertEquals(results.last.author, "Herbert")

  test("join select + LIMIT/OFFSET"):
    val t = xa()
    t.connect:
      val results = QueryBuilder
        .from[JnBook]
        .join(JnBook.author)
        .select(jq =>
          (
            title = jq.of[JnBook].title,
            author = jq.of[JnAuthor].name
          )
        )
        .orderBy(_.title)
        .limit(2)
        .offset(1)
        .run()
      assertEquals(results.length, 2)
      // sorted by title: Dune, Foundation, I Robot, The Hobbit, The Silmarillion
      assertEquals(results(0).title, "Foundation")
      assertEquals(results(1).title, "I, Robot")

  test("join select + DISTINCT"):
    val t = xa()
    t.connect:
      val results = QueryBuilder
        .from[JnBook]
        .join(JnBook.author)
        .select(jq =>
          (
            author = jq.of[JnAuthor].name
          )
        )
        .distinct
        .run()
      assertEquals(results.length, 3)

  test("join select + WHERE (frozen from before select)"):
    val t = xa()
    t.connect:
      val jq = QueryBuilder.from[JnBook].join(JnBook.author)
      val results = jq
        .where(jq.of[JnAuthor].name === "Tolkien")
        .select(jq2 =>
          (
            title = jq2.of[JnBook].title
          )
        )
        .orderBy(_.title)
        .run()
      assertEquals(results.length, 2)
      assertEquals(results(0).title, "The Hobbit")
      assertEquals(results(1).title, "The Silmarillion")

  test("join select build SQL verification"):
    val frag = QueryBuilder
      .from[JnBook]
      .join(JnBook.author)
      .select(jq =>
        (
          title = jq.of[JnBook].title,
          author = jq.of[JnAuthor].name
        )
      )
      .buildWith(databaseType)
    val sql = frag.sqlString
    assert(sql.contains("SELECT"), s"Expected SELECT in: $sql")
    assert(sql.contains("t0.title AS title"), s"Expected t0.title AS title in: $sql")
    assert(sql.contains("t1.name AS author"), s"Expected t1.name AS author in: $sql")
    assert(sql.contains("FROM jn_book t0"), s"Expected FROM jn_book t0 in: $sql")
    assert(sql.contains("INNER JOIN jn_author t1"), s"Expected INNER JOIN in: $sql")
    assert(sql.contains("ON t0.author_id = t1.id"), s"Expected ON clause in: $sql")

  test("join select + GROUP BY + HAVING build SQL verification"):
    val frag = QueryBuilder
      .from[JnBook]
      .join(JnBook.author)
      .select(jq =>
        (
          author = jq.of[JnAuthor].name,
          cnt = Expr.count
        )
      )
      .groupBy(_.author)
      .having(_.cnt > 1L)
      .orderBy(_.author)
      .buildWith(databaseType)
    val sql = frag.sqlString
    assert(sql.contains("GROUP BY t1.name"), s"Expected GROUP BY in: $sql")
    assert(sql.contains("HAVING COUNT(*) > ?"), s"Expected HAVING in: $sql")
    assert(sql.contains("ORDER BY t1.name ASC"), s"Expected ORDER BY in: $sql")

end JoinedSelectTestsDefs

class JoinedSelectTests extends QbH2TestBase with JoinedSelectTestsDefs:
  val h2Ddls = Seq("/h2/qb-join.sql")
end JoinedSelectTests

class PgJoinedSelectTests extends QbPgTestBase with JoinedSelectTestsDefs:
  val pgDdls = Seq("/pg/qb-join.sql")
end PgJoinedSelectTests

trait LeftJoinSelectTestsDefs:
  self: QbTestBase[?] =>

  test("left join select: columns from both tables"):
    val t = xa()
    t.connect:
      val results = QueryBuilder
        .from[LjBook]
        .leftJoin(LjBook.author)
        .select(jq =>
          (
            title = jq.of[LjBook].title,
            author = jq.ofLeft[LjAuthor].name
          )
        )
        .orderBy(_.title)
        .run()
      assertEquals(results.length, 3)
      // Anonymous Tales has no author
      val anon = results.find(_.title == "Anonymous Tales")
      assert(anon.isDefined)
      // The Hobbit has Tolkien
      val hobbit = results.find(_.title == "The Hobbit")
      assert(hobbit.isDefined)

  test("left join select build SQL contains LEFT JOIN"):
    val frag = QueryBuilder
      .from[LjBook]
      .leftJoin(LjBook.author)
      .select(jq =>
        (
          title = jq.of[LjBook].title,
          author = jq.ofLeft[LjAuthor].name
        )
      )
      .buildWith(databaseType)
    val sql = frag.sqlString
    assert(sql.contains("LEFT JOIN"), s"Expected LEFT JOIN in: $sql")
    assert(sql.contains("t1.name AS author"), s"Expected t1.name AS author in: $sql")

end LeftJoinSelectTestsDefs

class LeftJoinSelectTests extends QbH2TestBase with LeftJoinSelectTestsDefs:
  val h2Ddls = Seq("/h2/qb-left-join.sql")
end LeftJoinSelectTests

class PgLeftJoinSelectTests extends QbPgTestBase with LeftJoinSelectTestsDefs:
  val pgDdls = Seq("/pg/qb-left-join.sql")
end PgLeftJoinSelectTests

trait MultiJoinSelectTestsDefs:
  self: QbTestBase[?] =>

  test("3-table join select: columns from all 3 tables"):
    val t = xa()
    t.connect:
      val results = QueryBuilder
        .from[MjBook]
        .join(MjBook.author)
        .join(MjAuthor.country)
        .select(jq =>
          (
            title = jq.of[MjBook].title,
            author = jq.of[MjAuthor].name,
            country = jq.of[MjCountry].name
          )
        )
        .orderBy(_.title)
        .run()
      assertEquals(results.length, 5)
      val dune = results.find(_.title == "Dune")
      assert(dune.isDefined)
      assertEquals(dune.get.author, "Herbert")
      assertEquals(dune.get.country, "USA")

  test("3-table join select + GROUP BY"):
    val t = xa()
    t.connect:
      val results = QueryBuilder
        .from[MjBook]
        .join(MjBook.author)
        .join(MjAuthor.country)
        .select(jq =>
          (
            country = jq.of[MjCountry].name,
            cnt = Expr.count
          )
        )
        .groupBy(_.country)
        .orderBy(_.country)
        .run()
      assertEquals(results.length, 2)
      assertEquals(results(0).country, "UK")
      assertEquals(results(0).cnt, 2L)
      assertEquals(results(1).country, "USA")
      assertEquals(results(1).cnt, 3L)

end MultiJoinSelectTestsDefs

class MultiJoinSelectTests extends QbH2TestBase with MultiJoinSelectTestsDefs:
  val h2Ddls = Seq("/h2/qb-multi-join.sql")
end MultiJoinSelectTests

class PgMultiJoinSelectTests extends QbPgTestBase with MultiJoinSelectTestsDefs:
  val pgDdls = Seq("/pg/qb-multi-join.sql")
end PgMultiJoinSelectTests
