import com.augustnagro.magnum.*

@Table(H2DbType, SqlNameMapper.CamelToSnakeCase)
case class LjCountry(@Id id: Long, name: String) derives DbCodec, TableMeta

@Table(H2DbType, SqlNameMapper.CamelToSnakeCase)
case class LjAuthor(@Id id: Long, name: String, countryId: Option[Long]) derives DbCodec, TableMeta

@Table(H2DbType, SqlNameMapper.CamelToSnakeCase)
case class LjBook(@Id id: Long, authorId: Option[Long], title: String) derives DbCodec, TableMeta

class LeftJoinTests extends QbTestBase:

  val h2Ddls = Seq("/h2/qb-left-join.sql")

  val bookAuthor = Relationship.belongsTo[LjBook, LjAuthor](_.authorId, _.id)
  val authorCountry = Relationship.belongsTo[LjAuthor, LjCountry](_.countryId, _.id)

  test("leftJoin returns all books with Option[Author]"):
    val t = xa()
    t.connect:
      val results = QueryBuilder.from[LjBook].leftJoin(bookAuthor).run()
      assertEquals(results.size, 3)
      val hobbit = results.find(_._1.title == "The Hobbit")
      assert(hobbit.isDefined)
      assertEquals(hobbit.get._2, Some(LjAuthor(1, "Tolkien", Some(1L))))
      val anon = results.find(_._1.title == "Anonymous Tales")
      assert(anon.isDefined)
      assertEquals(anon.get._2, None)

  test("leftJoin with WHERE on root table"):
    val t = xa()
    t.connect:
      val results = QueryBuilder
        .from[LjBook]
        .leftJoin(bookAuthor)
        .where(sql"title = ${"Anonymous Tales"}")
        .run()
      assertEquals(results.size, 1)
      assertEquals(results.head._1.title, "Anonymous Tales")
      assertEquals(results.head._2, None)

  test("leftJoin with ofLeft WHERE on joined table"):
    val t = xa()
    t.connect:
      val qb = QueryBuilder.from[LjBook].leftJoin(bookAuthor)
      val results = qb.where(qb.ofLeft[LjAuthor].name === "Tolkien").run()
      assertEquals(results.size, 1)
      assertEquals(results.head._1.title, "The Hobbit")
      assert(results.head._2.isDefined)

  test("leftJoin count includes all root rows"):
    val t = xa()
    t.connect:
      val result = QueryBuilder.from[LjBook].leftJoin(bookAuthor).count()
      assertEquals(result, 3L)

  test("leftJoin first returns correct Option"):
    val t = xa()
    t.connect:
      val qb = QueryBuilder.from[LjBook].leftJoin(bookAuthor)
      val result = qb.where(sql"title = ${"Foundation"}").first()
      assert(result.isDefined)
      assertEquals(result.get._2, Some(LjAuthor(2, "Asimov", Some(2L))))

  test("build SQL contains LEFT JOIN"):
    val frag = QueryBuilder
      .from[LjBook]
      .leftJoin(bookAuthor)
      .build
    val sql = frag.sqlString
    assert(sql.contains("LEFT JOIN"), s"Expected LEFT JOIN in: $sql")
    assert(sql.contains("ON t0.author_id = t1.id"), s"Expected ON clause in: $sql")

  test("mixed join + leftJoin chain"):
    val t = xa()
    t.connect:
      // Book INNER JOIN Author LEFT JOIN Country
      // Tolkien has country UK, Asimov has country USA, Lonely has no country
      // But since inner join on bookAuthor, only matched books come through
      val qb = QueryBuilder.from[LjBook].join(bookAuthor).leftJoin(authorCountry)
      val results = qb.run()
      // Only books with authors (inner join filters Anonymous Tales)
      assertEquals(results.size, 2)
      // Each result is (LjBook, LjAuthor, Option[LjCountry])
      val hobbit = results.find(_._1.title == "The Hobbit")
      assert(hobbit.isDefined)
      assertEquals(hobbit.get._3, Some(LjCountry(1, "UK")))

  test("leftJoin with no matching rows gives all None"):
    val t = xa()
    t.connect:
      // Query books where author doesn't match — all get None
      val qb = QueryBuilder.from[LjBook].leftJoin(bookAuthor)
      val results = qb.where(qb.ofLeft[LjAuthor].name === "Nobody").run()
      assertEquals(results.size, 0)

  test("reverse direction: author leftJoin books"):
    val t = xa()
    t.connect:
      val authorBook = Relationship.belongsTo[LjAuthor, LjBook](_.id, _.authorId)
      val results = QueryBuilder.from[LjAuthor].leftJoin(authorBook).run()
      // Tolkien -> Hobbit, Asimov -> Foundation, Lonely -> None
      assertEquals(results.size, 3)
      val lonely = results.find(_._1.name == "Lonely")
      assert(lonely.isDefined)
      assertEquals(lonely.get._2, None)

end LeftJoinTests
