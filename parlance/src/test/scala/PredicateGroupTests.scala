import ma.chinespirit.parlance.*

trait PredicateGroupTestsDefs:
  self: QbTestBase[?] =>

  test("orWhere SQL generation"):
    val frag = QueryBuilder
      .from[QbUser]
      .where(_.age > 18)
      .orWhere(_.age < 5)
      .buildWith(databaseType)
    assertEquals(
      frag.sqlString,
      "SELECT id, first_name, age FROM qb_user WHERE (age > ? OR age < ?)"
    )

  test("orWhere returns correct rows"):
    val t = xa()
    t.connect:
      val results = QueryBuilder
        .from[QbUser]
        .where(_.firstName === Some("Alice"))
        .orWhere(_.firstName === Some("Bob"))
        .run()
      assertEquals(results.length, 2)
      val names = results.flatMap(_.firstName).sorted
      assertEquals(names, Vector("Alice", "Bob"))

  test("&& combinator SQL generation"):
    val frag = QueryBuilder
      .from[QbUser]
      .where(sq => (sq.age > 20) && (sq.age < 26))
      .orWhere(_.firstName === Some("Bob"))
      .buildWith(databaseType)
    assertEquals(
      frag.sqlString,
      "SELECT id, first_name, age FROM qb_user WHERE ((age > ? AND age < ?) OR first_name = ?)"
    )

  test("&& combinator returns correct rows"):
    val t = xa()
    t.connect:
      val results = QueryBuilder
        .from[QbUser]
        .where(sq => (sq.age > 20) && (sq.age < 26))
        .orWhere(_.firstName === Some("Bob"))
        .run()
      assertEquals(results.length, 3)
      val ids = results.map(_.id).sorted
      assertEquals(ids, Vector(1L, 2L, 4L))

  test("|| combinator builds OR group"):
    val t = xa()
    t.connect:
      val results = QueryBuilder
        .from[QbUser]
        .where(_.age > 10)
        .where(sq => (sq.firstName === Some("Alice")) || (sq.firstName === Some("Bob")))
        .run()
      assertEquals(results.length, 2)
      val frag = QueryBuilder
        .from[QbUser]
        .where(_.age > 10)
        .where(sq => (sq.firstName === Some("Alice")) || (sq.firstName === Some("Bob")))
        .build
      assertEquals(
        frag.sqlString,
        "SELECT id, first_name, age FROM qb_user WHERE (age > ? AND (first_name = ? OR first_name = ?))"
      )

  test("where + where still ANDs (regression)"):
    val t = xa()
    t.connect:
      val results = QueryBuilder
        .from[QbUser]
        .where(_.age > 18)
        .where(_.age < 30)
        .run()
      assertEquals(results.length, 2)
      val ids = results.map(_.id).sorted
      assertEquals(ids, Vector(1L, 4L))

  test("existing terminal ops work with new predicate state"):
    val t = xa()
    t.connect:
      val c = QueryBuilder
        .from[QbUser]
        .where(_.age > 18)
        .orWhere(_.firstName === Some("Charlie"))
        .count()
      assertEquals(c, 4L)

  test("&& with empty WhereFrag returns the non-empty side"):
    val nonEmpty: WhereFrag = Frag("age > ?", Seq(18), FragWriter.empty).unsafeAsWhere
    val result = nonEmpty && WhereFrag.empty
    assertEquals(result.sqlString, "age > ?")
    val result2 = WhereFrag.empty && nonEmpty
    assertEquals(result2.sqlString, "age > ?")

  test("|| with empty WhereFrag returns the non-empty side"):
    val nonEmpty: WhereFrag = Frag("age > ?", Seq(18), FragWriter.empty).unsafeAsWhere
    val result = nonEmpty || WhereFrag.empty
    assertEquals(result.sqlString, "age > ?")
    val result2 = WhereFrag.empty || nonEmpty
    assertEquals(result2.sqlString, "age > ?")

end PredicateGroupTestsDefs

class PredicateGroupTests extends QbH2TestBase, PredicateGroupTestsDefs:
  val h2Ddls = Seq("/h2/qb-user.sql")

class PgPredicateGroupTests extends QbPgTestBase, PredicateGroupTestsDefs:
  val pgDdls = Seq("/pg/qb-user.sql")
