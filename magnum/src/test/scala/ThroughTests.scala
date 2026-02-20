import com.augustnagro.magnum.*

@Table(H2DbType, SqlNameMapper.CamelToSnakeCase)
case class ThCountry(@Id id: Long, name: String) derives DbCodec, TableMeta

@Table(H2DbType, SqlNameMapper.CamelToSnakeCase)
case class ThUser(@Id id: Long, countryId: Long, name: String) derives DbCodec, TableMeta

@Table(H2DbType, SqlNameMapper.CamelToSnakeCase)
case class ThPost(@Id id: Long, userId: Long, title: String) derives DbCodec, TableMeta

@Table(H2DbType, SqlNameMapper.CamelToSnakeCase)
case class ThMechanic(@Id id: Long, name: String) derives DbCodec, TableMeta

@Table(H2DbType, SqlNameMapper.CamelToSnakeCase)
case class ThCar(@Id id: Long, mechanicId: Long, model: String) derives DbCodec, TableMeta

@Table(H2DbType, SqlNameMapper.CamelToSnakeCase)
case class ThOwner(@Id id: Long, carId: Long, name: String) derives DbCodec, TableMeta

class ThroughTests extends QbTestBase:

  val h2Ddls = Seq("/h2/qb-through.sql")

  // Country → User → Post (hasManyThrough)
  val countryPosts =
    Relationship.hasManyThrough[ThCountry, ThUser, ThPost](_.countryId, _.userId)

  // Mechanic → Car → Owner (hasOneThrough)
  val mechanicOwner =
    Relationship.hasOneThrough[ThMechanic, ThCar, ThOwner](_.mechanicId, _.carId)

  // --- hasManyThrough tests ---

  test("hasManyThrough: all countries with posts"):
    val t = xa()
    t.connect:
      val results =
        QueryBuilder.from[ThCountry].withRelated(countryPosts).run()
      assertEquals(results.size, 4)
      val uk = results.find(_._1.name == "UK").get
      assertEquals(uk._2.size, 3)
      val us = results.find(_._1.name == "US").get
      assertEquals(us._2.size, 1)
      val france = results.find(_._1.name == "France").get
      assertEquals(france._2.size, 0)
      val japan = results.find(_._1.name == "Japan").get
      assertEquals(japan._2.size, 0)

  test("hasManyThrough: with WHERE filter on root"):
    val t = xa()
    t.connect:
      val results = QueryBuilder
        .from[ThCountry]
        .where(_.name === "UK")
        .withRelated(countryPosts)
        .run()
      assertEquals(results.size, 1)
      assertEquals(results.head._1.name, "UK")
      assertEquals(results.head._2.size, 3)

  test("hasManyThrough: empty target — France has user but no posts"):
    val t = xa()
    t.connect:
      val results = QueryBuilder
        .from[ThCountry]
        .where(_.name === "France")
        .withRelated(countryPosts)
        .run()
      assertEquals(results.size, 1)
      assertEquals(results.head._2, Vector.empty[ThPost])

  test("hasManyThrough: no intermediate — Japan has no users"):
    val t = xa()
    t.connect:
      val results = QueryBuilder
        .from[ThCountry]
        .where(_.name === "Japan")
        .withRelated(countryPosts)
        .run()
      assertEquals(results.size, 1)
      assertEquals(results.head._2, Vector.empty[ThPost])

  test("hasManyThrough: with limit"):
    val t = xa()
    t.connect:
      val results = QueryBuilder
        .from[ThCountry]
        .orderBy(_.name)
        .limit(2)
        .withRelated(countryPosts)
        .run()
      assertEquals(results.size, 2)
      // Ordered: France, Japan — both have 0 posts
      assertEquals(results(0)._1.name, "France")
      assertEquals(results(0)._2.size, 0)
      assertEquals(results(1)._1.name, "Japan")
      assertEquals(results(1)._2.size, 0)

  test("hasManyThrough: first()"):
    val t = xa()
    t.connect:
      val result = QueryBuilder
        .from[ThCountry]
        .where(_.name === "US")
        .withRelated(countryPosts)
        .first()
      assert(result.isDefined)
      val (country, posts) = result.get
      assertEquals(country.name, "US")
      assertEquals(posts.size, 1)
      assertEquals(posts.head.title, "Charlie says hi")

  // --- hasOneThrough tests ---

  test("hasOneThrough: all mechanics with owners"):
    val t = xa()
    t.connect:
      val results =
        QueryBuilder.from[ThMechanic].withRelated(mechanicOwner).run()
      assertEquals(results.size, 3)
      val mech1 = results.find(_._1.name == "Mech1").get
      assertEquals(mech1._2.size, 1)
      assertEquals(mech1._2.head.name, "OwnerA")
      val mech2 = results.find(_._1.name == "Mech2").get
      assertEquals(mech2._2.size, 0)
      val mech3 = results.find(_._1.name == "Mech3").get
      assertEquals(mech3._2.size, 0)

  test("hasOneThrough: with WHERE filter on root"):
    val t = xa()
    t.connect:
      val results = QueryBuilder
        .from[ThMechanic]
        .where(_.name === "Mech1")
        .withRelated(mechanicOwner)
        .run()
      assertEquals(results.size, 1)
      assertEquals(results.head._1.name, "Mech1")
      assertEquals(results.head._2.size, 1)
      assertEquals(results.head._2.head.name, "OwnerA")

  test("hasOneThrough: no intermediate — Mech3 has no car"):
    val t = xa()
    t.connect:
      val results = QueryBuilder
        .from[ThMechanic]
        .where(_.name === "Mech3")
        .withRelated(mechanicOwner)
        .run()
      assertEquals(results.size, 1)
      assertEquals(results.head._2, Vector.empty[ThOwner])

  test("hasOneThrough: intermediate but no target — Mech2 has car but no owner"):
    val t = xa()
    t.connect:
      val results = QueryBuilder
        .from[ThMechanic]
        .where(_.name === "Mech2")
        .withRelated(mechanicOwner)
        .run()
      assertEquals(results.size, 1)
      assertEquals(results.head._2, Vector.empty[ThOwner])

  test("hasOneThrough: first()"):
    val t = xa()
    t.connect:
      val result = QueryBuilder
        .from[ThMechanic]
        .where(_.name === "Mech1")
        .withRelated(mechanicOwner)
        .first()
      assert(result.isDefined)
      val (mechanic, owners) = result.get
      assertEquals(mechanic.name, "Mech1")
      assertEquals(owners.size, 1)
      assertEquals(owners.head.name, "OwnerA")

  // --- SQL verification ---

  test("SQL verification: hasManyThrough queries use correct structure"):
    val tq = QueryBuilder
      .from[ThCountry]
      .withRelated(countryPosts)
    val queries = tq.buildQueries
    assertEquals(queries.size, 3)
    assert(queries(0).sqlString.contains("SELECT"), "Root should be a SELECT")
    assert(!queries(0).sqlString.contains("JOIN"), "Root should not contain JOIN")
    assert(queries(1).sqlString.contains("country_id"), "Intermediate query should reference sourceFk")
    assert(queries(1).sqlString.contains("th_user"), "Intermediate query should reference intermediate table")
    assert(queries(2).sqlString.contains("th_post"), "Target query should reference target table")
    assert(queries(2).sqlString.contains("user_id"), "Target query should reference targetFk")

  test("SQL verification: hasOneThrough queries use correct structure"):
    val tq = QueryBuilder
      .from[ThMechanic]
      .withRelated(mechanicOwner)
    val queries = tq.buildQueries
    assertEquals(queries.size, 3)
    assert(queries(0).sqlString.contains("SELECT"), "Root should be a SELECT")
    assert(queries(1).sqlString.contains("mechanic_id"), "Intermediate query should reference sourceFk")
    assert(queries(1).sqlString.contains("th_car"), "Intermediate query should reference intermediate table")
    assert(queries(2).sqlString.contains("th_owner"), "Target query should reference target table")
    assert(queries(2).sqlString.contains("car_id"), "Target query should reference targetFk")

  // --- Edge case ---

  test("empty root returns empty vector"):
    val t = xa()
    t.connect:
      val results = QueryBuilder
        .from[ThCountry]
        .where(_.name === "Nobody")
        .withRelated(countryPosts)
        .run()
      assertEquals(results, Vector.empty)

end ThroughTests
