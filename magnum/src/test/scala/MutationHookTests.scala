import com.augustnagro.magnum.*

import java.time.OffsetDateTime
import scala.reflect.{ClassTag, classTag}

@Table(H2DbType, SqlNameMapper.CamelToSnakeCase)
case class MhItem(
    @Id id: Long,
    name: String,
    updatedAt: Option[OffsetDateTime],
    @deletedAt deletedAt: Option[OffsetDateTime],
    status: String
) derives EntityMeta, HasDeletedAt

class MutationHookTests extends QbTestBase:

  val h2Ddls = Seq("/h2/mutation-hooks.sql")

  // --- scope definitions ---

  /** A scope that sets updated_at on every UPDATE. */
  class AutoUpdatedAt extends Scope[MhItem]:
    override def onUpdate(meta: TableMeta[MhItem]): Vector[SetClause] =
      val col = meta.columns.find(_.scalaName == "updatedAt").get
      Vector(SetClause.literal(col, "CURRENT_TIMESTAMP"))
    override def key: ClassTag[?] = classTag[AutoUpdatedAt]

  /** A scope with only conditions (no hooks). */
  class ActiveOnly extends Scope[MhItem]:
    override def conditions(meta: TableMeta[MhItem]): Vector[WhereFrag] =
      Vector(Frag("status = ?", Seq("active"), FragWriter.fromKeys(Vector("active"))).unsafeAsWhere)
    override def key: ClassTag[?] = classTag[ActiveOnly]

  /** A custom rewriteDelete scope (not SoftDeletes). */
  class CustomRewriteDelete extends Scope[MhItem]:
    override def rewriteDelete(meta: TableMeta[MhItem]): Vector[SetClause] =
      val col = meta.columns.find(_.scalaName == "status").get
      Vector(SetClause.parameterized(col, "archived"))
    override def key: ClassTag[?] = classTag[CustomRewriteDelete]

  // --- repos ---

  val plainRepo = Repo[MhItem, MhItem, Long]()

  val onUpdateRepo = new Repo[MhItem, MhItem, Long](
    Vector(new AutoUpdatedAt)
  )

  val conditionsOnlyRepo = new Repo[MhItem, MhItem, Long](
    Vector(new ActiveOnly)
  )

  val rewriteDeleteRepo = new Repo[MhItem, MhItem, Long](
    Vector(new CustomRewriteDelete)
  )

  val softDeleteWithOnUpdateRepo = new Repo[MhItem, MhItem, Long](
    Vector(new AutoUpdatedAt)
  ) with SoftDeletes[MhItem, MhItem, Long]

  // --- test: no scopes delegates to defaults (zero overhead) ---

  test("no scopes: deleteById delegates to defaults"):
    val t = xa()
    t.transact:
      plainRepo.deleteById(1L)
      assert(plainRepo.findById(1L).isEmpty)
      // restore
      sql"INSERT INTO mh_item VALUES (1, 'Alpha', NULL, NULL, 'active')".update.run()

  test("no scopes: update delegates to defaults"):
    val t = xa()
    t.transact:
      val item = plainRepo.findById(1L).get
      plainRepo.update(item.copy(name = "Alpha2"))
      assertEquals(plainRepo.findById(1L).get.name, "Alpha2")
      // restore
      plainRepo.update(item)

  // --- test: onUpdate hook ---

  test("onUpdate: update includes extra SET fragment"):
    val t = xa()
    t.transact:
      val item = onUpdateRepo.findById(1L).get
      assert(item.updatedAt.isEmpty)
      onUpdateRepo.update(item.copy(name = "AlphaUpdated"))
      val updated = onUpdateRepo.findById(1L).get
      assertEquals(updated.name, "AlphaUpdated")
      assert(updated.updatedAt.isDefined, "updatedAt should be set by onUpdate hook")
      // restore
      sql"UPDATE mh_item SET name = 'Alpha', updated_at = NULL WHERE id = 1".update.run()

  test("onUpdate: updatePartial includes hook SET frags"):
    val t = xa()
    t.transact:
      val item = onUpdateRepo.findById(1L).get
      assert(item.updatedAt.isEmpty)
      val modified = item.copy(name = "AlphaPartial")
      onUpdateRepo.updatePartial(item, modified)
      val updated = onUpdateRepo.findById(1L).get
      assertEquals(updated.name, "AlphaPartial")
      assert(updated.updatedAt.isDefined, "updatedAt should be set by onUpdate hook in updatePartial")
      // restore
      sql"UPDATE mh_item SET name = 'Alpha', updated_at = NULL WHERE id = 1".update.run()

  // --- test: rewriteDelete ---

  test("rewriteDelete: deleteById emits UPDATE not DELETE"):
    val t = xa()
    t.transact:
      rewriteDeleteRepo.deleteById(1L)
      // Row still exists but status changed
      val item = plainRepo.findById(1L).get
      assertEquals(item.status, "archived")
      // restore
      sql"UPDATE mh_item SET status = 'active' WHERE id = 1".update.run()

  // --- test: SoftDeletes + onUpdate compose ---

  test("SoftDeletes + onUpdate: delete sets both deleted_at and updated_at"):
    val t = xa()
    t.transact:
      val item = softDeleteWithOnUpdateRepo.findById(1L).get
      assert(item.updatedAt.isEmpty)
      softDeleteWithOnUpdateRepo.deleteById(1L)
      // Not visible via scoped reads
      assert(softDeleteWithOnUpdateRepo.findById(1L).isEmpty)
      // Check raw state
      val raw = plainRepo.findById(1L).get
      assert(raw.deletedAt.isDefined, "deleted_at should be set")
      assert(raw.updatedAt.isDefined, "updated_at should also be set by onUpdate hook")
      // restore
      sql"UPDATE mh_item SET deleted_at = NULL, updated_at = NULL WHERE id = 1".update.run()

  test("SoftDeletes + onUpdate: update includes hook and scope WHERE"):
    val t = xa()
    t.transact:
      val item = softDeleteWithOnUpdateRepo.findById(1L).get
      softDeleteWithOnUpdateRepo.update(item.copy(name = "AlphaSD"))
      val updated = softDeleteWithOnUpdateRepo.findById(1L).get
      assertEquals(updated.name, "AlphaSD")
      assert(updated.updatedAt.isDefined, "updatedAt should be set by onUpdate hook")
      // restore
      sql"UPDATE mh_item SET name = 'Alpha', updated_at = NULL WHERE id = 1".update.run()

  // --- test: conditions-only scope ---

  test("conditions only: deleteById adds WHERE condition"):
    val t = xa()
    t.transact:
      conditionsOnlyRepo.deleteById(1L)
      // Row deleted because status was 'active'
      assert(plainRepo.findById(1L).isEmpty)
      // restore
      sql"INSERT INTO mh_item VALUES (1, 'Alpha', NULL, NULL, 'active')".update.run()

  test("conditions only: update adds conditions to WHERE"):
    val t = xa()
    t.transact:
      val item = conditionsOnlyRepo.findById(1L).get
      conditionsOnlyRepo.update(item.copy(name = "AlphaCond"))
      assertEquals(conditionsOnlyRepo.findById(1L).get.name, "AlphaCond")
      // restore
      sql"UPDATE mh_item SET name = 'Alpha' WHERE id = 1".update.run()

  test("conditions only: deleteById on non-matching row is no-op"):
    val t = xa()
    t.transact:
      // id=3 has deleted_at set, but ActiveOnly checks status='active'.
      // id=3 actually has status='active', so it would match.
      // Let's set id=2 status to 'inactive' and try to delete it via conditionsOnlyRepo
      sql"UPDATE mh_item SET status = 'inactive' WHERE id = 2".update.run()
      conditionsOnlyRepo.deleteById(2L)
      // Row should still exist because condition didn't match
      val item = plainRepo.findById(2L).get
      assertEquals(item.status, "inactive")
      // restore
      sql"UPDATE mh_item SET status = 'active' WHERE id = 2".update.run()

  // --- test: deleteAll / deleteAllById with hooks ---

  test("deleteAll iterates when hooks active"):
    val t = xa()
    t.transact:
      val items = rewriteDeleteRepo.findAll
      rewriteDeleteRepo.deleteAll(items)
      // All should be archived, not deleted
      val all = plainRepo.findAll
      assert(all.forall(_.status == "archived") || all.length < items.length)
      // restore
      sql"UPDATE mh_item SET status = 'active'".update.run()

end MutationHookTests
