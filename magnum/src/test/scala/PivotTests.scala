import com.augustnagro.magnum.*

@Table(H2DbType, SqlNameMapper.CamelToSnakeCase)
case class PvUser(@Id id: Long, name: String) derives EntityMeta

@Table(H2DbType, SqlNameMapper.CamelToSnakeCase)
case class PvRole(@Id id: Long, name: String) derives EntityMeta

@Table(H2DbType, SqlNameMapper.CamelToSnakeCase)
case class Student(@Id id: Long, name: String) derives EntityMeta

@Table(H2DbType, SqlNameMapper.CamelToSnakeCase)
case class Course(@Id id: Long, title: String) derives EntityMeta

class PivotTests extends QbTestBase:

  val h2Ddls = Seq("/h2/qb-pivot.sql")

  val userRoles =
    Relationship.belongsToMany[PvUser, PvRole]("pv_user_role", "user_id", "role_id")

  test("basic pivot load returns all users with roles"):
    val t = xa()
    t.connect:
      val results =
        QueryBuilder.from[PvUser].withRelated(userRoles).run()
      assertEquals(results.size, 4)
      val alice = results.find(_._1.name == "Alice").get
      assertEquals(alice._2.size, 2)
      val bob = results.find(_._1.name == "Bob").get
      assertEquals(bob._2.size, 1)
      val charlie = results.find(_._1.name == "Charlie").get
      assertEquals(charlie._2.size, 0)
      val dave = results.find(_._1.name == "Dave").get
      assertEquals(dave._2.size, 3)

  test("user with 0 roles gets empty vector"):
    val t = xa()
    t.connect:
      val results =
        QueryBuilder.from[PvUser].withRelated(userRoles).run()
      val charlie = results.find(_._1.name == "Charlie").get
      assertEquals(charlie._2, Vector.empty[PvRole])

  test("user with multiple roles gets all roles"):
    val t = xa()
    t.connect:
      val results =
        QueryBuilder.from[PvUser].withRelated(userRoles).run()
      val dave = results.find(_._1.name == "Dave").get
      val roleNames = dave._2.map(_.name).toSet
      assertEquals(roleNames, Set("admin", "editor", "viewer"))
      val alice = results.find(_._1.name == "Alice").get
      val aliceRoles = alice._2.map(_.name).toSet
      assertEquals(aliceRoles, Set("admin", "editor"))

  test("with WHERE on root query"):
    val t = xa()
    t.connect:
      val results = QueryBuilder
        .from[PvUser]
        .where(_.name === "Alice")
        .withRelated(userRoles)
        .run()
      assertEquals(results.size, 1)
      assertEquals(results.head._1.name, "Alice")
      assertEquals(results.head._2.size, 2)

  test("empty root result returns empty vector"):
    val t = xa()
    t.connect:
      val results = QueryBuilder
        .from[PvUser]
        .where(_.name === "Nobody")
        .withRelated(userRoles)
        .run()
      assertEquals(results, Vector.empty)

  test("with ORDER BY preserves user ordering"):
    val t = xa()
    t.connect:
      val results = QueryBuilder
        .from[PvUser]
        .orderBy(_.name)
        .withRelated(userRoles)
        .run()
      assertEquals(results.size, 4)
      assertEquals(results(0)._1.name, "Alice")
      assertEquals(results(1)._1.name, "Bob")
      assertEquals(results(2)._1.name, "Charlie")
      assertEquals(results(3)._1.name, "Dave")

  test("with LIMIT returns only limited users with correct roles"):
    val t = xa()
    t.connect:
      val results = QueryBuilder
        .from[PvUser]
        .orderBy(_.name)
        .limit(2)
        .withRelated(userRoles)
        .run()
      assertEquals(results.size, 2)
      assertEquals(results(0)._1.name, "Alice")
      assertEquals(results(0)._2.size, 2)
      assertEquals(results(1)._1.name, "Bob")
      assertEquals(results(1)._2.size, 1)

  test("first() returns single user with roles"):
    val t = xa()
    t.connect:
      val result = QueryBuilder
        .from[PvUser]
        .where(_.name === "Dave")
        .withRelated(userRoles)
        .first()
      assert(result.isDefined)
      val (user, roles) = result.get
      assertEquals(user.name, "Dave")
      assertEquals(roles.size, 3)
      assertEquals(roles.map(_.name).toSet, Set("admin", "editor", "viewer"))

  test("SQL verification: root query has no JOIN"):
    val rootFrag = QueryBuilder
      .from[PvUser]
      .where(_.name === "Alice")
      .build
    assert(
      !rootFrag.sqlString.contains("JOIN"),
      s"Root query should not contain JOIN: ${rootFrag.sqlString}"
    )
    assert(
      rootFrag.sqlString.contains("SELECT"),
      s"Root query should be a SELECT: ${rootFrag.sqlString}"
    )

  // --- Convention-based belongsToMany tests ---

  val studentCourses = Relationship.belongsToMany[Student, Course]()

  test("convention: derives correct pivot table and FK names"):
    assertEquals(studentCourses.pivotTable, "student_course")
    assertEquals(studentCourses.sourceFk, "student_id")
    assertEquals(studentCourses.targetFk, "course_id")

  test("convention: basic pivot load works end-to-end"):
    val t = xa()
    t.connect:
      val results =
        QueryBuilder.from[Student].withRelated(studentCourses).run()
      assertEquals(results.size, 2)
      val alice = results.find(_._1.name == "Alice").get
      assertEquals(alice._2.map(_.title).toSet, Set("Math", "Physics"))
      val bob = results.find(_._1.name == "Bob").get
      assertEquals(bob._2.map(_.title).toSet, Set("Physics", "History"))

  test("convention: with WHERE on root query"):
    val t = xa()
    t.connect:
      val results = QueryBuilder
        .from[Student]
        .where(_.name === "Bob")
        .withRelated(studentCourses)
        .run()
      assertEquals(results.size, 1)
      assertEquals(results.head._2.size, 2)

  test("convention: first() works"):
    val t = xa()
    t.connect:
      val result = QueryBuilder
        .from[Student]
        .where(_.name === "Alice")
        .withRelated(studentCourses)
        .first()
      assert(result.isDefined)
      val (student, courses) = result.get
      assertEquals(student.name, "Alice")
      assertEquals(courses.size, 2)

end PivotTests
