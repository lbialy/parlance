import ma.chinespirit.parlance.*

import java.time.OffsetDateTime

@Table(SqlNameMapper.CamelToSnakeCase)
case class TsItem(
    @Id id: Long,
    name: String,
    @createdAt createdAt: OffsetDateTime,
    @updatedAt updatedAt: OffsetDateTime,
    @deletedAt deletedAt: Option[OffsetDateTime]
) derives EntityMeta, HasCreatedAt, HasUpdatedAt, HasDeletedAt

case class TsItemCreator(name: String) extends CreatorOf[TsItem] derives DbCodec

object TsItem:
  val tsRepo = new Repo[TsItemCreator, TsItem, Long] with Timestamps[TsItemCreator, TsItem, Long]
  val tsAndSdRepo =
    new Repo[TsItemCreator, TsItem, Long] with Timestamps[TsItemCreator, TsItem, Long] with SoftDeletes[TsItemCreator, TsItem, Long]
  val sdAndTsRepo =
    new Repo[TsItemCreator, TsItem, Long] with SoftDeletes[TsItemCreator, TsItem, Long] with Timestamps[TsItemCreator, TsItem, Long]
  val plainRepo = Repo[TsItemCreator, TsItem, Long]()

trait TimestampsTestsDefs:
  self: QbTestBase[? <: SupportsMutations & SupportsReturning] =>

  // DB defaults are year 2000 — anything after 2020 proves the hook set CURRENT_TIMESTAMP
  val cutoff = OffsetDateTime.parse("2020-01-01T00:00:00Z")

  // --- Timestamps on INSERT ---

  test("Timestamps: create sets both created_at and updated_at to CURRENT_TIMESTAMP"):
    val t = xa()
    t.connect:
      val item = TsItem.tsRepo.create(TsItemCreator("Alpha"))
      assert(item.createdAt.isAfter(cutoff), s"created_at ${item.createdAt} should be recent, not DB default")
      assert(item.updatedAt.isAfter(cutoff), s"updated_at ${item.updatedAt} should be recent, not DB default")

  test("Timestamps: rawInsert sets timestamps to CURRENT_TIMESTAMP"):
    val t = xa()
    t.connect:
      TsItem.tsRepo.rawInsert(TsItemCreator("Beta"))
      val beta = TsItem.plainRepo.findAll.find(_.name == "Beta").get
      assert(beta.createdAt.isAfter(cutoff), s"created_at ${beta.createdAt} should be recent")
      assert(beta.updatedAt.isAfter(cutoff), s"updated_at ${beta.updatedAt} should be recent")

  test("Timestamps: rawInsertAll sets timestamps for all rows"):
    val t = xa()
    t.connect:
      TsItem.tsRepo.rawInsertAll(Seq(TsItemCreator("D1"), TsItemCreator("D2")))
      val items = TsItem.plainRepo.findAll
      for name <- Seq("D1", "D2") do
        val item = items.find(_.name == name).get
        assert(item.createdAt.isAfter(cutoff), s"$name created_at should be recent")
        assert(item.updatedAt.isAfter(cutoff), s"$name updated_at should be recent")

  test("plain repo: insert gets DB default (year 2000), not CURRENT_TIMESTAMP"):
    val t = xa()
    t.connect:
      val item = TsItem.plainRepo.create(TsItemCreator("Plain"))
      assert(item.createdAt.isBefore(cutoff), s"Without Timestamps mixin, created_at should be DB default (2000), got ${item.createdAt}")

  // --- Timestamps on UPDATE ---

  test("Timestamps: update bumps updated_at but not created_at"):
    val t = xa()
    t.transact:
      // Seed a row with known past timestamps via raw SQL
      sql"INSERT INTO ts_item (name, created_at, updated_at) VALUES ('Seeded', TIMESTAMP WITH TIME ZONE '2020-06-15T00:00:00Z', TIMESTAMP WITH TIME ZONE '2020-06-15T00:00:00Z')".update
        .run()
      val seeded = TsItem.tsRepo.findAll.find(_.name == "Seeded").get
      val originalCreatedAt = seeded.createdAt
      assertEquals(originalCreatedAt, OffsetDateTime.parse("2020-06-15T00:00:00Z"))

      TsItem.tsRepo.update(seeded.copy(name = "SeededUpdated"))
      val updated = TsItem.tsRepo.findById(seeded.id).get
      assertEquals(updated.name, "SeededUpdated")
      assertEquals(updated.createdAt, originalCreatedAt, "created_at must not change on update")
      assert(updated.updatedAt.isAfter(cutoff), s"updated_at ${updated.updatedAt} should be bumped to current time")
      assert(updated.updatedAt.isAfter(originalCreatedAt), "updated_at should be after original value")

  // --- Timestamps + SoftDeletes composition ---

  test("Timestamps + SoftDeletes: create sets timestamps via hook"):
    val t = xa()
    t.connect:
      val item = TsItem.tsAndSdRepo.create(TsItemCreator("TsSD"))
      assert(item.createdAt.isAfter(cutoff), "created_at should be set by hook")
      assert(item.updatedAt.isAfter(cutoff), "updated_at should be set by hook")
      assert(item.deletedAt.isEmpty)

  test("Timestamps + SoftDeletes: delete sets deleted_at and bumps updated_at"):
    val t = xa()
    t.transact:
      val item = TsItem.tsAndSdRepo.create(TsItemCreator("ToDelete"))
      val createdAtBefore = item.updatedAt
      TsItem.tsAndSdRepo.deleteById(item.id)
      // Not visible via scoped reads
      assert(TsItem.tsAndSdRepo.findById(item.id).isEmpty)
      // Check raw state
      val raw = TsItem.plainRepo.findById(item.id).get
      assert(raw.deletedAt.isDefined, "deleted_at should be set by SoftDeletes")
      assert(raw.updatedAt.isAfter(cutoff), "updated_at should be bumped by Timestamps onUpdate hook")

  test("SoftDeletes + Timestamps (reversed mixin order): both scopes compose"):
    val t = xa()
    t.transact:
      val item = TsItem.sdAndTsRepo.create(TsItemCreator("Reversed"))
      assert(item.createdAt.isAfter(cutoff), "created_at should be set by hook regardless of mixin order")
      assert(item.updatedAt.isAfter(cutoff), "updated_at should be set by hook regardless of mixin order")
      TsItem.sdAndTsRepo.deleteById(item.id)
      assert(TsItem.sdAndTsRepo.findById(item.id).isEmpty)
      val raw = TsItem.plainRepo.findById(item.id).get
      assert(raw.deletedAt.isDefined, "deleted_at should be set by SoftDeletes")

end TimestampsTestsDefs

class TimestampsTests extends QbH2TestBase, TimestampsTestsDefs:
  val h2Ddls = Seq("/h2/timestamps.sql")

class PgTimestampsTests extends QbPgTestBase, TimestampsTestsDefs:
  val pgDdls = Seq("/pg/timestamps.sql")
