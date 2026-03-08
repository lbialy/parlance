import ma.chinespirit.parlance.*

@Table(SqlNameMapper.CamelToSnakeCase)
case class UpsertItem(@Id id: Long, name: String, amount: Int) derives EntityMeta
object UpsertItem:
  val repo = Repo[UpsertItem, UpsertItem, Long]()

trait UpsertTestsDefs[D <: SupportsMutations]:
  self: QbTestBase[D] =>

  // --- insertOnConflict ---

  test("insertOnConflict DoNothing + AnyConflict silently ignores duplicate"):
    val t = xa()
    t.connect:
      UpsertItem.repo.insertOnConflict(
        UpsertItem(1, "Alpha", 999),
        ConflictTarget.AnyConflict,
        ConflictAction.DoNothing
      )
      val item = UpsertItem.repo.findById(1L).get
      assertEquals(item.amount, 10) // unchanged

  test("insertOnConflict DoNothing + Columns ignores duplicate on specific column"):
    val t = xa()
    t.connect:
      val nameCol = Col[String]("name", "name")
      UpsertItem.repo.insertOnConflict(
        UpsertItem(99, "Alpha", 999),
        ConflictTarget.Columns(nameCol),
        ConflictAction.DoNothing
      )
      // should not insert because name = "Alpha" conflicts
      assertEquals(UpsertItem.repo.count, 2L)

  test("insertOnConflict DoUpdate upserts on conflict"):
    val t = xa()
    t.connect:
      // H2 uses MERGE INTO KEY which updates all columns on conflict
      UpsertItem.repo.insertOnConflict(
        UpsertItem(1, "AlphaUpdated", 999),
        ConflictTarget.Columns(Col[Long]("id", "id")),
        ConflictAction.DoUpdate(sql"amount = ${999}")
      )
      val item = UpsertItem.repo.findById(1L).get
      assertEquals(item.amount, 999)

  test("insertOnConflict inserts when no conflict"):
    val t = xa()
    t.connect:
      UpsertItem.repo.insertOnConflict(
        UpsertItem(3, "Gamma", 30),
        ConflictTarget.AnyConflict,
        ConflictAction.DoNothing
      )
      assertEquals(UpsertItem.repo.count, 3L)
      val item = UpsertItem.repo.findById(3L).get
      assertEquals(item.name, "Gamma")

  // --- insertOnConflictUpdateAll ---

  test("insertOnConflictUpdateAll updates all EC columns on conflict"):
    val t = xa()
    t.connect:
      UpsertItem.repo.insertOnConflictUpdateAll(
        UpsertItem(1, "AlphaUpdated", 999),
        ConflictTarget.Columns(Col[Long]("id", "id"))
      )
      val item = UpsertItem.repo.findById(1L).get
      assertEquals(item.name, "AlphaUpdated")
      assertEquals(item.amount, 999)

  // --- insertAllIgnoring ---

  test("insertAllIgnoring bulk inserts with conflicts, returns actual insert count"):
    val t = xa()
    t.connect:
      val items = Seq(
        UpsertItem(1, "Alpha", 999), // conflict
        UpsertItem(2, "Beta", 999), // conflict
        UpsertItem(3, "Gamma", 30), // new
        UpsertItem(4, "Delta", 40) // new
      )
      val inserted = UpsertItem.repo.insertAllIgnoring(items)
      assertEquals(inserted, 2)
      assertEquals(UpsertItem.repo.count, 4L)
      // originals unchanged
      assertEquals(UpsertItem.repo.findById(1L).get.amount, 10)
      assertEquals(UpsertItem.repo.findById(2L).get.amount, 20)

  // --- save() upsert behavior ---

  test("save() untracked entity, row absent inserts"):
    val t = xa()
    t.connect:
      val newItem = UpsertItem(3, "Gamma", 30)
      UpsertItem.repo.save(newItem)
      val item = UpsertItem.repo.findById(3L).get
      assertEquals(item.name, "Gamma")
      assertEquals(item.amount, 30)

  test("save() untracked entity, row present updates"):
    val t = xa()
    t.connect:
      val modified = UpsertItem(1, "AlphaModified", 999)
      UpsertItem.repo.save(modified)
      val item = UpsertItem.repo.findById(1L).get
      assertEquals(item.name, "AlphaModified")
      assertEquals(item.amount, 999)

  test("save() tracked entity uses partial update"):
    val t = xa()
    t.connect:
      val item = UpsertItem.repo.findById(1L).get
      val modified = item.copy(name = "AlphaChanged")
      UpsertItem.repo.save(modified)
      val result = UpsertItem.repo.findById(1L).get
      assertEquals(result.name, "AlphaChanged")
      assertEquals(result.amount, 10) // unchanged

  // --- firstOrCreate ---

  test("firstOrCreate returns existing entity when found"):
    val t = xa()
    t.transact:
      val result = UpsertItem.repo.firstOrCreate(
        sql"name = ${"Alpha"}".unsafeAsWhere,
        UpsertItem(99, "Alpha", 999)
      )
      assertEquals(result.id, 1L)
      assertEquals(result.amount, 10)
      assertEquals(UpsertItem.repo.count, 2L) // no new row

  test("firstOrCreate inserts and returns new entity when not found"):
    val t = xa()
    t.transact:
      val result = UpsertItem.repo.firstOrCreate(
        sql"name = ${"Gamma"}".unsafeAsWhere,
        UpsertItem(3, "Gamma", 30)
      )
      assertEquals(result.name, "Gamma")
      assertEquals(result.amount, 30)
      assertEquals(UpsertItem.repo.count, 3L)

  // --- updateOrCreate ---

  test("updateOrCreate updates and returns existing entity when found"):
    val t = xa()
    t.transact:
      val result = UpsertItem.repo.updateOrCreate(
        sql"name = ${"Alpha"}".unsafeAsWhere,
        UpsertItem(99, "Alpha", 999),
        existing => existing.copy(amount = existing.amount + 100)
      )
      assertEquals(result.id, 1L)
      assertEquals(result.amount, 110)
      val fromDb = UpsertItem.repo.findById(1L).get
      assertEquals(fromDb.amount, 110)

  test("updateOrCreate inserts and returns new entity when not found"):
    val t = xa()
    t.transact:
      val result = UpsertItem.repo.updateOrCreate(
        sql"name = ${"Gamma"}".unsafeAsWhere,
        UpsertItem(3, "Gamma", 30),
        existing => existing.copy(amount = 999)
      )
      assertEquals(result.name, "Gamma")
      assertEquals(result.amount, 30) // creator value, not updater
      assertEquals(UpsertItem.repo.count, 3L)

end UpsertTestsDefs

class UpsertTests extends QbH2TestBase with UpsertTestsDefs[H2]:
  val h2Ddls = Seq("/h2/upsert.sql")

class PgUpsertTests extends QbPgTestBase with UpsertTestsDefs[Postgres]:
  val pgDdls = Seq("/pg/upsert.sql")
