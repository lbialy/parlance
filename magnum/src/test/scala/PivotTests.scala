import com.augustnagro.magnum.*
import java.time.LocalDateTime

@Table(SqlNameMapper.CamelToSnakeCase)
case class PvUser(@Id id: Long, name: String) derives EntityMeta

@Table(SqlNameMapper.CamelToSnakeCase)
case class PvRole(@Id id: Long, name: String) derives EntityMeta

@Table(SqlNameMapper.CamelToSnakeCase)
case class Student(@Id id: Long, name: String) derives EntityMeta

@Table(SqlNameMapper.CamelToSnakeCase)
case class Course(@Id id: Long, title: String) derives EntityMeta

@Table(SqlNameMapper.CamelToSnakeCase)
case class PvUserRoleExt(
    @Id userId: Long,
    @Id roleId: Long,
    assignedBy: String,
    assignedAt: LocalDateTime
) derives EntityMeta

case class PvUserRoleExtCreator(
    userId: Long,
    roleId: Long,
    assignedBy: String,
    assignedAt: LocalDateTime
) derives DbCodec

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
      .buildWith(H2)
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

  // === Composite key derivation tests ===

  test("EntityMeta with multiple @Id derives composite primaryKeys"):
    val meta = summon[EntityMeta[PvUserRoleExt]]
    assertEquals(meta.primaryKeys.length, 2)
    assertEquals(meta.primaryKeys(0).scalaName, "userId")
    assertEquals(meta.primaryKeys(1).scalaName, "roleId")
    assert(meta.isCompositeKey)
    // backward compat: primaryKey returns first
    assertEquals(meta.primaryKey.scalaName, "userId")

  test("EntityMeta with single @Id has primaryKeys of length 1"):
    val meta = summon[EntityMeta[PvUser]]
    assertEquals(meta.primaryKeys.length, 1)
    assertEquals(meta.primaryKeys(0).scalaName, "id")
    assert(!meta.isCompositeKey)

  // === Simple FK-only ops (on BelongsToMany) ===

  test("attach inserts pivot rows"):
    val t = xa()
    t.transact:
      val alice = PvUser(1, "Alice")
      val viewer = PvRole(3, "viewer")
      val result = userRoles.attach(alice, viewer)
      assertEquals(result, 1)
    t.connect:
      val results = QueryBuilder
        .from[PvUser]
        .where(_.name === "Alice")
        .withRelated(userRoles)
        .run()
      val alice = results.head
      assertEquals(alice._2.size, 3) // was 2, now 3

  test("detach removes pivot rows"):
    val t = xa()
    t.transact:
      val alice = PvUser(1, "Alice")
      val admin = PvRole(1, "admin")
      val result = userRoles.detach(alice, admin)
      assertEquals(result, 1)
    t.connect:
      val results = QueryBuilder
        .from[PvUser]
        .where(_.name === "Alice")
        .withRelated(userRoles)
        .run()
      val alice = results.head
      assertEquals(alice._2.size, 1) // was 2, now 1
      assertEquals(alice._2.head.name, "editor")

  test("detachAll removes all pivot rows for source"):
    val t = xa()
    t.transact:
      val dave = PvUser(4, "Dave")
      val result = userRoles.detachAll(dave)
      assertEquals(result, 3) // Dave had 3 roles
    t.connect:
      val results = QueryBuilder
        .from[PvUser]
        .where(_.name === "Dave")
        .withRelated(userRoles)
        .run()
      assertEquals(results.head._2.size, 0)

  test("sync attaches new and detaches removed"):
    val t = xa()
    t.transact:
      val alice = PvUser(1, "Alice")
      val admin = PvRole(1, "admin")
      val viewer = PvRole(3, "viewer")
      // Alice has admin+editor, sync to admin+viewer
      val result = userRoles.sync(alice, List(admin, viewer))
      assertEquals(result.attached, 1) // viewer
      assertEquals(result.detached, 1) // editor
      assertEquals(result.unchanged, 1) // admin
    t.connect:
      val results = QueryBuilder
        .from[PvUser]
        .where(_.name === "Alice")
        .withRelated(userRoles)
        .run()
      val roleNames = results.head._2.map(_.name).toSet
      assertEquals(roleNames, Set("admin", "viewer"))

  test("sync idempotent — second call returns all unchanged"):
    val t = xa()
    t.transact:
      val bob = PvUser(2, "Bob")
      val editor = PvRole(2, "editor")
      // Bob has editor, sync to editor (no change)
      val result = userRoles.sync(bob, List(editor))
      assertEquals(result.attached, 0)
      assertEquals(result.detached, 0)
      assertEquals(result.unchanged, 1)

  // === Creator-based ops (on WritablePivotRelation) ===

  val userRolesExt = Relationship
    .belongsToMany[PvUser, PvRole]("pv_user_role_ext", "user_id", "role_id")
    .withPivot[PvUserRoleExt, PvUserRoleExtCreator]

  test("WritablePivotRelation attach inserts row with metadata"):
    val t = xa()
    val now = LocalDateTime.of(2025, 6, 1, 12, 0)
    t.transact:
      // Charlie (id=3) has no roles, attach viewer
      userRolesExt.attach(PvUserRoleExtCreator(3, 3, "test", now))
    t.connect:
      val results = QueryBuilder
        .from[PvUser]
        .where(_.name === "Charlie")
        .withRelatedAndPivot(userRolesExt)
        .run()
      assertEquals(results.size, 1)
      val (user, pairs) = results.head
      assertEquals(user.name, "Charlie")
      assertEquals(pairs.size, 1)
      val (role, pivot) = pairs.head
      assertEquals(role.name, "viewer")
      assertEquals(pivot.assignedBy, "test")
      assertEquals(pivot.assignedAt, now)

  test("WritablePivotRelation attachAll batch inserts"):
    val t = xa()
    val now = LocalDateTime.of(2025, 7, 1, 12, 0)
    t.transact:
      // Charlie has no roles in ext table yet
      userRolesExt.attachAll(
        List(
          PvUserRoleExtCreator(3, 1, "batch", now),
          PvUserRoleExtCreator(3, 2, "batch", now)
        )
      )
    t.connect:
      val results = QueryBuilder
        .from[PvUser]
        .where(_.name === "Charlie")
        .withRelatedAndPivot(userRolesExt)
        .run()
      assertEquals(results.head._2.size, 2)

  test("WritablePivotRelation sync with creator factory"):
    val t = xa()
    val now = LocalDateTime.of(2025, 8, 1, 12, 0)
    t.transact:
      val alice = PvUser(1, "Alice")
      val admin = PvRole(1, "admin")
      val viewer = PvRole(3, "viewer")
      // Alice has admin+editor in ext table, sync to admin+viewer
      val result = userRolesExt.sync(alice, List(admin, viewer), role => PvUserRoleExtCreator(alice.id, role.id, "sync-test", now))
      assertEquals(result.attached, 1) // viewer
      assertEquals(result.detached, 1) // editor
      assertEquals(result.unchanged, 1) // admin

  // === Eager loading with pivot data ===

  test("withRelatedAndPivot loads (target, pivot) pairs"):
    val t = xa()
    t.connect:
      val results = QueryBuilder
        .from[PvUser]
        .orderBy(_.name)
        .withRelatedAndPivot(userRolesExt)
        .run()
      assertEquals(results.size, 4)
      val alice = results.find(_._1.name == "Alice").get
      assertEquals(alice._2.size, 2)
      alice._2.foreach: (role, pivot) =>
        assert(Set("admin", "editor").contains(role.name))
        assert(Set("system", "admin1").contains(pivot.assignedBy))

      // Charlie has no roles
      val charlie = results.find(_._1.name == "Charlie").get
      assertEquals(charlie._2.size, 0)

  test("withRelatedAndPivot with Frag filter on pivot columns"):
    val t = xa()
    t.connect:
      val results = QueryBuilder
        .from[PvUser]
        .withRelatedAndPivot(userRolesExt, Frag("assigned_by = ?", Seq("system"), FragWriter.fromKeys(Vector("system"))))
        .run()
      // Only Alice (admin by system, editor NOT by system -> only admin) and Bob (editor by system)
      val alice = results.find(_._1.name == "Alice").get
      assertEquals(alice._2.size, 1)
      assertEquals(alice._2.head._2.assignedBy, "system")

      val bob = results.find(_._1.name == "Bob").get
      assertEquals(bob._2.size, 1)

      val dave = results.find(_._1.name == "Dave").get
      assertEquals(dave._2.size, 0) // Dave's roles all assigned by admin1

  test("withRelatedAndPivot empty result for user with no roles"):
    val t = xa()
    t.connect:
      val results = QueryBuilder
        .from[PvUser]
        .where(_.name === "Charlie")
        .withRelatedAndPivot(userRolesExt)
        .run()
      assertEquals(results.size, 1)
      assertEquals(results.head._2, Vector.empty)

  test("withRelated on PivotRelation returns just targets (no pivot data)"):
    val t = xa()
    t.connect:
      val results = QueryBuilder
        .from[PvUser]
        .where(_.name === "Alice")
        .withRelated(userRolesExt)
        .run()
      assertEquals(results.size, 1)
      val (user, roles) = results.head
      assertEquals(user.name, "Alice")
      assertEquals(roles.size, 2)
      // roles are PvRole, not (PvRole, PvUserRoleExt) tuples
      assertEquals(roles.map(_.name).toSet, Set("admin", "editor"))

  test("withRelatedAndPivot chained with withRelated"):
    val t = xa()
    t.connect:
      val results = QueryBuilder
        .from[PvUser]
        .withRelatedAndPivot(userRolesExt)
        .withRelated(userRoles)
        .run()
      assertEquals(results.size, 4)
      val alice = results.find(_._1.name == "Alice").get
      // alice._2 = Vector[(PvRole, PvUserRoleExt)] from ext table
      // alice._3 = Vector[PvRole] from simple pivot
      assertEquals(alice._2.size, 2)
      assertEquals(alice._3.size, 2)

  // === PivotRelation detach tests ===

  test("PivotRelation detach works (using ext relation)"):
    val t = xa()
    t.transact:
      val alice = PvUser(1, "Alice")
      val admin = PvRole(1, "admin")
      val result = userRolesExt.detach(alice, admin)
      assertEquals(result, 1)
    t.connect:
      val results = QueryBuilder
        .from[PvUser]
        .where(_.name === "Alice")
        .withRelatedAndPivot(userRolesExt)
        .run()
      assertEquals(results.head._2.size, 1)
      assertEquals(results.head._2.head._1.name, "editor")

  test("PivotRelation detachAll works"):
    val t = xa()
    t.transact:
      val dave = PvUser(4, "Dave")
      val result = userRolesExt.detachAll(dave)
      assertEquals(result, 3)
    t.connect:
      val results = QueryBuilder
        .from[PvUser]
        .where(_.name === "Dave")
        .withRelatedAndPivot(userRolesExt)
        .run()
      assertEquals(results.head._2.size, 0)

end PivotTests
