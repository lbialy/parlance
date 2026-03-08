import com.augustnagro.magnum.*

@Table(SqlNameMapper.CamelToSnakeCase)
case class BgUser(@Id id: Long, name: String) derives EntityMeta
object BgUser:
  val roles =
    Relationship.belongsToMany[BgUser, BgRole]("bg_user_role", "user_id", "role_id")

@Table(SqlNameMapper.CamelToSnakeCase)
case class BgRole(@Id id: Long, name: String) derives EntityMeta

@Table(SqlNameMapper.CamelToSnakeCase)
case class BgCountry(@Id id: Long, name: String) derives EntityMeta
object BgCountry:
  val articles =
    Relationship.hasManyThrough[BgCountry, BgPerson, BgArticle](_.countryId, _.personId)

@Table(SqlNameMapper.CamelToSnakeCase)
case class BgPerson(@Id id: Long, countryId: Long, name: String) derives EntityMeta

@Table(SqlNameMapper.CamelToSnakeCase)
case class BgArticle(@Id id: Long, personId: Long, title: String) derives EntityMeta

trait BugTestsDefs:
  self: QbTestBase[?] =>

  // ===== Bug 1a: first() skips stitching in PivotEagerQuery =====

  test("BUG 1a-pivot: first() result count should match run() for duplicate pivot entries"):
    // Eve is linked to alpha twice in the pivot table (no unique constraint).
    // run() stitches through pivot pairs and includes alpha twice.
    // first() calls .distinct on target keys and returns alpha once.
    // This is a behavioral inconsistency.
    val t = xa()
    t.connect:
      val runResult = QueryBuilder
        .from[BgUser]
        .where(_.id === 1L)
        .withRelated(BgUser.roles)
        .run()
      val firstResult = QueryBuilder
        .from[BgUser]
        .where(_.id === 1L)
        .withRelated(BgUser.roles)
        .first()
      assert(firstResult.isDefined)
      val runTargets = runResult.head._2
      val firstTargets = firstResult.get._2
      assertEquals(
        firstTargets.size,
        runTargets.size,
        s"first() returned ${firstTargets.size} targets but run() returned ${runTargets.size}"
      )

  // ===== Bug 1a: first() skips stitching in ThroughQuery =====

  test("BUG 1a-through: first() target ordering should match run()"):
    // Testland has Person Alice (id=10) with articles 10 and 30,
    // and Person Bob (id=20) with article 20.
    // Intermediate pairs by person PK: [(1,10), (1,20)]
    // run() stitches: pair(1,10)->[A10,A30], pair(1,20)->[A20]
    //   = [Alice first, Alice second, Bob writes]
    // first() fetchTargets: WHERE person_id IN (10,20), returns by article PK:
    //   [A10, A20, A30] = [Alice first, Bob writes, Alice second]
    // The orderings differ — Bob's article is interleaved differently.
    val t = xa()
    t.connect:
      val runResult = QueryBuilder
        .from[BgCountry]
        .where(_.id === 1L)
        .withRelated(BgCountry.articles)
        .run()
      val firstResult = QueryBuilder
        .from[BgCountry]
        .where(_.id === 1L)
        .withRelated(BgCountry.articles)
        .first()
      assert(firstResult.isDefined)
      val runTitles = runResult.head._2.map(_.title)
      val firstTitles = firstResult.get._2.map(_.title)
      assertEquals(
        firstTitles,
        runTitles,
        s"first() ordering $firstTitles differs from run() ordering $runTitles"
      )

  // ===== Bug 1b: indexWhere returns -1 without validation =====

  test("BUG 1b-eager: invalid FK column should produce descriptive error"):
    // Manually construct a HasMany with a non-existent FK column name.
    // Current code: indexWhere returns -1, productElement(-1) throws IndexOutOfBoundsException
    // Expected: descriptive QueryBuilderException indicating which column was not found
    val badRel = HasMany[BgUser, BgRole, Selectable](
      fk = Col[Long]("nonExistent", "non_existent"),
      pk = Col[Long]("id", "id")
    )
    val t = xa()
    t.connect:
      intercept[QueryBuilderException]:
        QueryBuilder.from[BgUser].withRelated(badRel).run()

  test("BUG 1b-pivot: invalid sourcePk column should produce descriptive error"):
    // Manually construct a BelongsToMany with a non-existent sourcePk column name.
    // Current code: indexWhere returns -1, productElement(-1) throws IndexOutOfBoundsException
    // Expected: descriptive QueryBuilderException
    val badPivotRel = BelongsToMany[BgUser, BgRole, Selectable](
      pivotTable = "bg_user_role",
      sourceFk = "user_id",
      targetFk = "role_id",
      sourcePk = Col[Long]("nonExistent", "non_existent"),
      targetPk = Col[Long]("id", "id")
    )
    val t = xa()
    t.connect:
      intercept[QueryBuilderException]:
        QueryBuilder.from[BgUser].withRelated(badPivotRel).run()

  test("BUG 1b-through: invalid sourcePk column should produce descriptive error"):
    // Manually construct a HasManyThrough with a non-existent sourcePk column name.
    // Current code: indexWhere returns -1, productElement(-1) throws IndexOutOfBoundsException
    // Expected: descriptive QueryBuilderException
    val badThroughRel = HasManyThrough[BgCountry, BgArticle, Selectable](
      intermediateTable = "bg_person",
      sourceFk = "country_id",
      intermediatePk = Col[Long]("id", "id"),
      targetFk = Col[Long]("personId", "person_id"),
      sourcePk = Col[Long]("nonExistent", "non_existent")
    )
    val t = xa()
    t.connect:
      intercept[QueryBuilderException]:
        QueryBuilder.from[BgCountry].withRelated(badThroughRel).run()

  // ===== Bug 2b: Columns.selectDynamic uses .get without context =====

  test("BUG 2b: Columns.selectDynamic with invalid name should throw QueryBuilderException"):
    // Columns.selectDynamic does cols.find(_.scalaName == name).get
    // If column name isn't found, throws NoSuchElementException("None.get")
    // with zero indication of which column was missing.
    // Expected: QueryBuilderException with descriptive message.
    val cols = new Columns[BgUser](summon[TableMeta[BgUser]].columns)
    intercept[QueryBuilderException]:
      cols.selectDynamic("nonExistentColumn")

  // ===== Bug 2d: no input validation on limit() / offset() =====

  test("BUG 2d-limit: negative limit should throw validation error"):
    // limit() accepts any Int without validation. Negative values silently
    // produce invalid SQL (LIMIT -5). Compare with chunk() which correctly
    // does require(batchSize > 0, "batchSize must be positive").
    // Expected: QueryBuilderException on negative input.
    intercept[QueryBuilderException]:
      QueryBuilder.from[BgUser].limit(-1)

  test("BUG 2d-offset: negative offset should throw validation error"):
    // offset() accepts any Long without validation. Negative values silently
    // produce invalid SQL (OFFSET -5).
    // Expected: QueryBuilderException on negative input.
    intercept[QueryBuilderException]:
      QueryBuilder.from[BgUser].offset(-1L)

end BugTestsDefs

class BugTests extends QbH2TestBase, BugTestsDefs:
  val h2Ddls = Seq("/h2/qb-bugs.sql")
end BugTests

class PgBugTests extends QbPgTestBase, BugTestsDefs:
  val pgDdls = Seq("/pg/qb-bugs.sql")
end PgBugTests
