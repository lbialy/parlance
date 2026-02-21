import com.augustnagro.magnum.*

import java.time.OffsetDateTime

@Table(H2DbType, SqlNameMapper.CamelToSnakeCase)
case class SdUser(
    @Id id: Long,
    name: String,
    deletedAt: Option[OffsetDateTime]
) derives DbCodec, TableMeta

class SoftDeleteTests extends QbTestBase:

  val h2Ddls = Seq("/h2/soft-delete.sql")

  val sdRepo = new Repo[SdUser, SdUser, Long] with SoftDeletes[SdUser, SdUser, Long]

  // A plain repo with no SoftDeletes to verify zero overhead
  val plainRepo = Repo[SdUser, SdUser, Long]()

  // --- reads filter soft-deleted rows ---

  test("findAll excludes soft-deleted"):
    val t = xa()
    t.connect:
      val results = sdRepo.findAll
      assertEquals(results.length, 2)
      assertEquals(results.map(_.name).sorted, Vector("Alice", "Bob"))

  test("findById returns active entity"):
    val t = xa()
    t.connect:
      val result = sdRepo.findById(1L)
      assert(result.isDefined)
      assertEquals(result.get.name, "Alice")

  test("findById returns None for soft-deleted entity"):
    val t = xa()
    t.connect:
      val result = sdRepo.findById(3L)
      assert(result.isEmpty)

  test("count excludes soft-deleted"):
    val t = xa()
    t.connect:
      assertEquals(sdRepo.count, 2L)

  test("existsById returns true for active"):
    val t = xa()
    t.connect:
      assert(sdRepo.existsById(1L))

  test("existsById returns false for soft-deleted"):
    val t = xa()
    t.connect:
      assert(!sdRepo.existsById(3L))

  test("findAllById excludes soft-deleted"):
    val t = xa()
    t.connect:
      val results = sdRepo.findAllById(Seq(1L, 2L, 3L))
      assertEquals(results.length, 2)
      assertEquals(results.map(_.name).sorted, Vector("Alice", "Bob"))

  test("query.run() applies scope"):
    val t = xa()
    t.connect:
      val results = sdRepo.query.run()
      assertEquals(results.length, 2)

  test("find delegates through findById with scope"):
    val t = xa()
    t.connect:
      assert(sdRepo.find(1L).isDefined)
      assert(sdRepo.find(3L).isEmpty)

  test("findOrFail throws for soft-deleted entity"):
    val t = xa()
    t.connect:
      intercept[QueryBuilderException]:
        sdRepo.findOrFail(3L)

  // --- write overrides ---

  test("delete soft-deletes an entity"):
    val t = xa()
    t.transact:
      val alice = sdRepo.findById(1L).get
      sdRepo.delete(alice)

      // Not visible via scoped reads
      assert(sdRepo.findById(1L).isEmpty)

      // Still exists in DB
      val allRows = sdRepo.withTrashed.run()
      assert(allRows.exists(_.id == 1L))

      // Restore for other tests
      sdRepo.restoreById(1L)

  test("deleteById soft-deletes"):
    val t = xa()
    t.transact:
      sdRepo.deleteById(2L)
      assert(sdRepo.findById(2L).isEmpty)

      // Restore
      sdRepo.restoreById(2L)

  // --- restore ---

  test("restore clears deleted_at"):
    val t = xa()
    t.transact:
      // Carol (id=3) is soft-deleted
      assert(sdRepo.findById(3L).isEmpty)
      sdRepo.restoreById(3L)
      val carol = sdRepo.findById(3L)
      assert(carol.isDefined)
      assertEquals(carol.get.name, "Carol")

      // Re-soft-delete for other tests
      sdRepo.deleteById(3L)

  // --- force delete ---

  test("forceDelete hard-deletes"):
    val t = xa()
    t.transact:
      // Dave (id=4) is soft-deleted
      sdRepo.forceDeleteById(4L)

      // Gone from withTrashed too
      val all = sdRepo.withTrashed.run()
      assert(!all.exists(_.id == 4L))

      // Restore for other tests
      sql"INSERT INTO sd_user VALUES (4, 'Dave', TIMESTAMP WITH TIME ZONE '2025-06-15T12:00:00Z')".update
        .run()

  // --- withTrashed / onlyTrashed ---

  test("withTrashed returns all rows"):
    val t = xa()
    t.connect:
      val results = sdRepo.withTrashed.run()
      assertEquals(results.length, 4)

  test("onlyTrashed returns only soft-deleted rows"):
    val t = xa()
    t.connect:
      val results = sdRepo.onlyTrashed.run()
      assertEquals(results.length, 2)
      assertEquals(results.map(_.name).sorted, Vector("Carol", "Dave"))

  // --- isTrashed ---

  test("isTrashed returns true for soft-deleted entity"):
    val t = xa()
    t.connect:
      val all = sdRepo.withTrashed.run()
      val carol = all.find(_.name == "Carol").get
      assert(sdRepo.isTrashed(carol))

  test("isTrashed returns false for active entity"):
    val t = xa()
    t.connect:
      val alice = sdRepo.findById(1L).get
      assert(!sdRepo.isTrashed(alice))

  // --- truncate still hard-truncates ---

  test("truncate hard-truncates all rows"):
    val t = xa()
    t.transact:
      sdRepo.truncate()
      val all = sdRepo.withTrashed.run()
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
      val results = plainRepo.findAll
      assertEquals(results.length, 4)
      assertEquals(plainRepo.count, 4L)

  // --- refresh respects scope ---

  test("refresh on soft-deleted entity throws"):
    val t = xa()
    t.transact:
      val alice = sdRepo.findById(1L).get
      sdRepo.deleteById(1L)

      intercept[QueryBuilderException]:
        sdRepo.refresh(alice)

      // Restore
      sdRepo.restoreById(1L)

end SoftDeleteTests
