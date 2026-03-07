import com.augustnagro.magnum.*

@Table(SqlNameMapper.CamelToSnakeCase)
case class MjCountry(@Id id: Long, name: String) derives EntityMeta

@Table(SqlNameMapper.CamelToSnakeCase)
case class MjPublisher(@Id id: Long, name: String) derives EntityMeta

@Table(SqlNameMapper.CamelToSnakeCase)
case class MjAuthor(@Id id: Long, name: String, countryId: Long) derives EntityMeta
object MjAuthor:
  val country = Relationship.belongsTo[MjAuthor, MjCountry](_.countryId, _.id)

@Table(SqlNameMapper.CamelToSnakeCase)
case class MjBook(@Id id: Long, authorId: Long, publisherId: Long, title: String) derives EntityMeta
object MjBook:
  val author = Relationship.belongsTo[MjBook, MjAuthor](_.authorId, _.id)
  val publisher = Relationship.belongsTo[MjBook, MjPublisher](_.publisherId, _.id)

class MultiJoinQueryTests extends QbTestBase:

  val h2Ddls = Seq("/h2/qb-multi-join.sql")

  test("3-table linear join returns all tuples"):
    val t = xa()
    t.connect:
      val results = QueryBuilder.from[MjBook].join(MjBook.author).join(MjAuthor.country).run()
      assertEquals(results.size, 5)
      // each result is (MjBook, MjAuthor, MjCountry)
      val dune = results.find(_._1.title == "Dune")
      assert(dune.isDefined)
      assertEquals(dune.get._2.name, "Herbert")
      assertEquals(dune.get._3.name, "USA")

  test("WHERE on 3rd table"):
    val t = xa()
    t.connect:
      val qb = QueryBuilder.from[MjBook].join(MjBook.author).join(MjAuthor.country)
      val results = qb.where(qb.of[MjCountry].name === "UK").run()
      assertEquals(results.size, 2)
      assert(results.forall(_._2.name == "Tolkien"))
      assert(results.forall(_._3.name == "UK"))

  test("ORDER BY on 3rd table"):
    val t = xa()
    t.connect:
      val qb = QueryBuilder.from[MjBook].join(MjBook.author).join(MjAuthor.country)
      val results = qb.orderBy(qb.of[MjCountry].name).run()
      // UK < USA — Tolkien's books first, then Asimov/Herbert
      assertEquals(results(0)._3.name, "UK")
      assertEquals(results(1)._3.name, "UK")
      assertEquals(results(2)._3.name, "USA")

  test("build SQL for 3-table join"):
    val frag = QueryBuilder.from[MjBook].join(MjBook.author).join(MjAuthor.country).buildWith(H2)
    val sql = frag.sqlString
    assert(sql.contains("t0."), s"Expected t0. alias in: $sql")
    assert(sql.contains("t1."), s"Expected t1. alias in: $sql")
    assert(sql.contains("t2."), s"Expected t2. alias in: $sql")
    assert(sql.contains("INNER JOIN"), s"Expected INNER JOIN in: $sql")
    assert(
      sql.contains("ON t0.author_id = t1.id"),
      s"Expected first ON clause in: $sql"
    )
    assert(
      sql.contains("ON t1.country_id = t2.id"),
      s"Expected second ON clause in: $sql"
    )

  test("star join: book -> author and book -> publisher"):
    val t = xa()
    t.connect:
      val results = QueryBuilder.from[MjBook].join(MjBook.author).join(MjBook.publisher).run()
      assertEquals(results.size, 5)
      // each result is (MjBook, MjAuthor, MjPublisher)
      val dune = results.find(_._1.title == "Dune")
      assert(dune.isDefined)
      assertEquals(dune.get._2.name, "Herbert")
      assertEquals(dune.get._3.name, "Chilton Books")

  test("star join WHERE on both branches"):
    val t = xa()
    t.connect:
      val qb = QueryBuilder.from[MjBook].join(MjBook.author).join(MjBook.publisher)
      val results = qb
        .where(qb.of[MjAuthor].name === "Asimov")
        .where(qb.of[MjPublisher].name === "Gnome Press")
        .run()
      assertEquals(results.size, 2)
      assert(results.forall(_._1.authorId == 2L))

  test("count with 3-table join"):
    val t = xa()
    t.connect:
      val result = QueryBuilder.from[MjBook].join(MjBook.author).join(MjAuthor.country).count()
      assertEquals(result, 5L)

  test("first with 3-table join"):
    val t = xa()
    t.connect:
      val qb = QueryBuilder.from[MjBook].join(MjBook.author).join(MjAuthor.country)
      val result = qb.where(qb.of[MjBook].title === "Dune").first()
      assert(result.isDefined)
      assertEquals(result.get._1.title, "Dune")
      assertEquals(result.get._2.name, "Herbert")
      assertEquals(result.get._3.name, "USA")

end MultiJoinQueryTests
