import com.augustnagro.magnum.*

@Table(H2DbType, SqlNameMapper.CamelToSnakeCase)
case class QbUser(@Id id: Long, firstName: Option[String], age: Int) derives DbCodec, TableMeta

class QueryBuilderBasicTests extends QbTestBase:

  val h2Ddls = Seq("/h2/qb-user.sql")

  // val u = Columns.of[QbUser]

  test("from[QbUser].run() returns all rows"):
    val t = xa()
    t.connect:
      val results = QueryBuilder.from[QbUser].run()
      assertEquals(results.length, 4)
      assertEquals(results.map(_.id).sorted, Vector(1L, 2L, 3L, 4L))

  test("where firstName === Some(Alice) returns 1 row"):
    val t = xa()
    t.connect:
      val results = QueryBuilder
        .from[QbUser]
        .where(_.firstName === Some("Alice"))
        .run()
      assertEquals(results.length, 1)
      assertEquals(results.head.id, 1L)
      assertEquals(results.head.firstName, Some("Alice"))

  test("compound where with age > 18 and age < 30 returns 2 rows"):
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

  test("where firstName.isNull returns 1 row"):
    val t = xa()
    t.connect:
      val results = QueryBuilder
        .from[QbUser]
        .where(_.firstName.isNull)
        .run()
      assertEquals(results.length, 1)
      assertEquals(results.head.id, 4L)
      assertEquals(results.head.firstName, None)

  test("where age.in(List(25, 30)) returns 2 rows"):
    val t = xa()
    t.connect:
      val results = QueryBuilder
        .from[QbUser]
        .where(_.age.in(List(25, 30)))
        .run()
      assertEquals(results.length, 2)
      val ids = results.map(_.id).sorted
      assertEquals(ids, Vector(1L, 2L))

  test("build returns correct SQL"):
    val frag = QueryBuilder
      .from[QbUser]
      .where(_.age > 18)
      .build
    assertEquals(
      frag.sqlString,
      "SELECT id, first_name, age FROM qb_user WHERE age > ?"
    )

  test("empty in() produces no results"):
    val t = xa()
    t.connect:
      val results = QueryBuilder
        .from[QbUser]
        .where(_.age.in(List.empty[Int]))
        .run()
      assertEquals(results.length, 0)

  // --- Lambda syntax tests ---

  test("lambda where: _.age > 18"):
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

  test("lambda where: _.firstName === Some(Alice)"):
    val t = xa()
    t.connect:
      val results = QueryBuilder
        .from[QbUser]
        .where(_.firstName === Some("Alice"))
        .run()
      assertEquals(results.length, 1)
      assertEquals(results.head.firstName, Some("Alice"))

  test("lambda orderBy: _.age"):
    val t = xa()
    t.connect:
      val results = QueryBuilder
        .from[QbUser]
        .orderBy(_.age)
        .run()
      val ages = results.map(_.age)
      assertEquals(ages, Vector(17, 22, 25, 30))

  test("lambda where + orderBy + limit combined"):
    val t = xa()
    t.connect:
      val results = QueryBuilder
        .from[QbUser]
        .where(_.age > 18)
        .orderBy(_.age)
        .limit(2)
        .run()
      assertEquals(results.length, 2)
      assertEquals(results(0).age, 22)
      assertEquals(results(1).age, 25)

  test("lambda build SQL matches expected"):
    val frag = QueryBuilder
      .from[QbUser]
      .where(_.age > 18)
      .build
    assertEquals(
      frag.sqlString,
      "SELECT id, first_name, age FROM qb_user WHERE age > ?"
    )

end QueryBuilderBasicTests
