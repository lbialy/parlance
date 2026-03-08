import com.augustnagro.magnum.*

import java.time.OffsetDateTime

@Table(SqlNameMapper.CamelToSnakeCase)
case class SdUser(
    @Id id: Long,
    name: String,
    @deletedAt deletedAt: Option[OffsetDateTime]
) derives EntityMeta, HasDeletedAt
object SdUser:
  val repo = new Repo[SdUser, SdUser, Long] with SoftDeletes[SdUser, SdUser, Long]
  val plainRepo = Repo[SdUser, SdUser, Long]()

trait SoftDeleteTestsDefs[D <: SupportsMutations]:
  self: QbTestBase[D] =>

  // --- reads filter soft-deleted rows ---

  test("findAll excludes soft-deleted"):
    val t = xa()
    t.connect:
      val results = SdUser.repo.findAll
      assertEquals(results.length, 2)
      assertEquals(results.map(_.name).sorted, Vector("Alice", "Bob"))

  test("findById returns active entity"):
    val t = xa()
    t.connect:
      val result = SdUser.repo.findById(1L)
      assert(result.isDefined)
      assertEquals(result.get.name, "Alice")

  test("findById returns None for soft-deleted entity"):
    val t = xa()
    t.connect:
      val result = SdUser.repo.findById(3L)
      assert(result.isEmpty)

  test("count excludes soft-deleted"):
    val t = xa()
    t.connect:
      assertEquals(SdUser.repo.count, 2L)

  test("existsById returns true for active"):
    val t = xa()
    t.connect:
      assert(SdUser.repo.existsById(1L))

  test("existsById returns false for soft-deleted"):
    val t = xa()
    t.connect:
      assert(!SdUser.repo.existsById(3L))

  test("findAllById excludes soft-deleted"):
    val t = xa()
    t.connect:
      val results = SdUser.repo.findAllById(Seq(1L, 2L, 3L))
      assertEquals(results.length, 2)
      assertEquals(results.map(_.name).sorted, Vector("Alice", "Bob"))

  test("query.run() applies scope"):
    val t = xa()
    t.connect:
      val results = SdUser.repo.query.run()
      assertEquals(results.length, 2)

  test("find delegates through findById with scope"):
    val t = xa()
    t.connect:
      assert(SdUser.repo.find(1L).isDefined)
      assert(SdUser.repo.find(3L).isEmpty)

  test("findOrFail throws for soft-deleted entity"):
    val t = xa()
    t.connect:
      intercept[QueryBuilderException]:
        SdUser.repo.findOrFail(3L)

  // --- write overrides ---

  test("delete soft-deletes an entity"):
    val t = xa()
    t.transact:
      val alice = SdUser.repo.findById(1L).get
      SdUser.repo.delete(alice)

      // Not visible via scoped reads
      assert(SdUser.repo.findById(1L).isEmpty)

      // Still exists in DB
      val allRows = SdUser.repo.withTrashed.run()
      assert(allRows.exists(_.id == 1L))

      // Restore for other tests
      SdUser.repo.restoreById(1L)

  test("deleteById soft-deletes"):
    val t = xa()
    t.transact:
      SdUser.repo.deleteById(2L)
      assert(SdUser.repo.findById(2L).isEmpty)

      // Restore
      SdUser.repo.restoreById(2L)

  // --- restore ---

  test("restore clears deleted_at"):
    val t = xa()
    t.transact:
      // Carol (id=3) is soft-deleted
      assert(SdUser.repo.findById(3L).isEmpty)
      SdUser.repo.restoreById(3L)
      val carol = SdUser.repo.findById(3L)
      assert(carol.isDefined)
      assertEquals(carol.get.name, "Carol")

      // Re-soft-delete for other tests
      SdUser.repo.deleteById(3L)

  // --- force delete ---

  test("forceDelete hard-deletes"):
    val t = xa()
    t.transact:
      // Dave (id=4) is soft-deleted
      SdUser.repo.forceDeleteById(4L)

      // Gone from withTrashed too
      val all = SdUser.repo.withTrashed.run()
      assert(!all.exists(_.id == 4L))

      // Restore for other tests
      sql"INSERT INTO sd_user VALUES (4, 'Dave', TIMESTAMP WITH TIME ZONE '2025-06-15T12:00:00Z')".update
        .run()

  // --- withTrashed / onlyTrashed ---

  test("withTrashed returns all rows"):
    val t = xa()
    t.connect:
      val results = SdUser.repo.withTrashed.run()
      assertEquals(results.length, 4)

  test("onlyTrashed returns only soft-deleted rows"):
    val t = xa()
    t.connect:
      val results = SdUser.repo.onlyTrashed.run()
      assertEquals(results.length, 2)
      assertEquals(results.map(_.name).sorted, Vector("Carol", "Dave"))

  // --- isTrashed ---

  test("isTrashed returns true for soft-deleted entity"):
    val t = xa()
    t.connect:
      val all = SdUser.repo.withTrashed.run()
      val carol = all.find(_.name == "Carol").get
      assert(SdUser.repo.isTrashed(carol))

  test("isTrashed returns false for active entity"):
    val t = xa()
    t.connect:
      val alice = SdUser.repo.findById(1L).get
      assert(!SdUser.repo.isTrashed(alice))

  // --- truncate still hard-truncates ---

  test("truncate hard-truncates all rows"):
    val t = xa()
    t.transact:
      SdUser.repo.truncate()
      val all = SdUser.repo.withTrashed.run()
      assertEquals(all.length, 0)

      // Restore test data
      sql"INSERT INTO sd_user VALUES (1, 'Alice', NULL)".update.run()
      sql"INSERT INTO sd_user VALUES (2, 'Bob', NULL)".update.run()
      sql"INSERT INTO sd_user VALUES (3, 'Carol', TIMESTAMP WITH TIME ZONE '2025-01-01T00:00:00Z')".update
        .run()
      sql"INSERT INTO sd_user VALUES (4, 'Dave', TIMESTAMP WITH TIME ZONE '2025-06-15T12:00:00Z')".update
        .run()

  // --- plain repo unchanged ---

  test("scope-free repo returns all rows (no overhead)"):
    val t = xa()
    t.connect:
      val results = SdUser.plainRepo.findAll
      assertEquals(results.length, 4)
      assertEquals(SdUser.plainRepo.count, 4L)

  // --- refresh respects scope ---

  test("refresh on soft-deleted entity throws"):
    val t = xa()
    t.transact:
      val alice = SdUser.repo.findById(1L).get
      SdUser.repo.deleteById(1L)

      intercept[QueryBuilderException]:
        SdUser.repo.refresh(alice)

      // Restore
      SdUser.repo.restoreById(1L)

end SoftDeleteTestsDefs

class SoftDeleteTests extends QbH2TestBase with SoftDeleteTestsDefs[H2]:
  val h2Ddls = Seq("/h2/soft-delete.sql")

class PgSoftDeleteTests extends QbPgTestBase with SoftDeleteTestsDefs[Postgres]:
  val pgDdls = Seq("/pg/soft-delete.sql")
