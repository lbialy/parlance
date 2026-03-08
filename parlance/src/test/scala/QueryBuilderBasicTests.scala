import ma.chinespirit.parlance.*

@Table(SqlNameMapper.CamelToSnakeCase)
case class QbUser(@Id id: Long, firstName: Option[String], age: Int) derives EntityMeta

@Table(SqlNameMapper.CamelToSnakeCase)
case class QbProduct(@Id id: Long, name: String, price: Int) derives EntityMeta

trait QueryBuilderBasicTestsDefs:
  self: QbTestBase[? <: SupportsILike] =>

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
      .buildWith(databaseType)
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

  // --- notIn tests ---

  test("where age.notIn(List(25, 30)) returns rows NOT matching"):
    val t = xa()
    t.connect:
      val results = QueryBuilder
        .from[QbUser]
        .where(_.age.notIn(List(25, 30)))
        .run()
      assertEquals(results.length, 2)
      val ids = results.map(_.id).sorted
      assertEquals(ids, Vector(3L, 4L))

  test("empty notIn() returns all rows"):
    val t = xa()
    t.connect:
      val results = QueryBuilder
        .from[QbUser]
        .where(_.age.notIn(List.empty[Int]))
        .run()
      assertEquals(results.length, 4)

  // --- between tests ---

  test("where age.between(20, 26) returns rows in range"):
    val t = xa()
    t.connect:
      val results = QueryBuilder
        .from[QbUser]
        .where(_.age.between(20, 26))
        .run()
      assertEquals(results.length, 2)
      val ids = results.map(_.id).sorted
      assertEquals(ids, Vector(1L, 4L))

  // --- notLike tests ---

  test("where name.notLike(A%) returns rows not matching pattern"):
    val t = xa()
    t.connect:
      val results = QueryBuilder
        .from[QbProduct]
        .where(_.name.notLike("A%"))
        .run()
      assertEquals(results.length, 3)
      val ids = results.map(_.id).sorted
      assertEquals(ids, Vector(2L, 3L, 4L))

  // --- ilike tests ---

  test("where name.ilike(a%) matches case-insensitively"):
    val t = xa()
    t.connect:
      val results = QueryBuilder
        .from[QbProduct]
        .where(_.name.ilike("a%"))
        .run()
      assertEquals(results.length, 2)
      val ids = results.map(_.id).sorted
      assertEquals(ids, Vector(1L, 3L))

  // --- Option[String] column tests (like/notLike/ilike on nullable) ---

  test("like on Option[String] column works"):
    val t = xa()
    t.connect:
      val results = QueryBuilder
        .from[QbUser]
        .where(_.firstName.like("A%"))
        .run()
      assertEquals(results.length, 1)
      assertEquals(results.head.id, 1L)

  test("notLike on Option[String] column works"):
    val t = xa()
    t.connect:
      val results = QueryBuilder
        .from[QbUser]
        .where(_.firstName.notLike("A%"))
        .run()
      assertEquals(results.length, 2)
      val ids = results.map(_.id).sorted
      assertEquals(ids, Vector(2L, 3L))

  test("ilike on Option[String] column works"):
    val t = xa()
    t.connect:
      val results = QueryBuilder
        .from[QbUser]
        .where(_.firstName.ilike("alice"))
        .run()
      assertEquals(results.length, 1)
      assertEquals(results.head.id, 1L)

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
      .buildWith(databaseType)
    assertEquals(
      frag.sqlString,
      "SELECT id, first_name, age FROM qb_user WHERE age > ?"
    )

  // --- distinct tests ---

  test("distinct.build produces SELECT DISTINCT"):
    val frag = QueryBuilder
      .from[QbUser]
      .distinct
      .buildWith(databaseType)
    assertEquals(
      frag.sqlString,
      "SELECT DISTINCT id, first_name, age FROM qb_user"
    )

  test("distinct with where produces correct SQL"):
    val frag = QueryBuilder
      .from[QbUser]
      .distinct
      .where(_.age > 18)
      .buildWith(databaseType)
    assertEquals(
      frag.sqlString,
      "SELECT DISTINCT id, first_name, age FROM qb_user WHERE age > ?"
    )

  test("distinct.run() returns correct rows"):
    val t = xa()
    t.connect:
      val results = QueryBuilder
        .from[QbUser]
        .distinct
        .run()
      assertEquals(results.length, 4)
      assertEquals(results.map(_.id).sorted, Vector(1L, 2L, 3L, 4L))

end QueryBuilderBasicTestsDefs

class QueryBuilderBasicTests extends QbH2TestBase, QueryBuilderBasicTestsDefs:
  val h2Ddls = Seq("/h2/qb-user.sql", "/h2/qb-product.sql")
end QueryBuilderBasicTests

class PgQueryBuilderBasicTests extends QbPgTestBase, QueryBuilderBasicTestsDefs:
  val pgDdls = Seq("/pg/qb-user.sql", "/pg/qb-product.sql")
end PgQueryBuilderBasicTests
