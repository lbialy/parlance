import com.augustnagro.magnum.*

import java.time.OffsetDateTime
import scala.collection.mutable

// --- entities ---

@Table(SqlNameMapper.CamelToSnakeCase)
case class ObsUserCreator(name: String) derives DbCodec

@Table(SqlNameMapper.CamelToSnakeCase)
case class ObsUser(@Id id: Long, name: String) derives EntityMeta

given InsertBuilder[ObsUserCreator, ObsUser] = InsertBuilder.derived

@Table(SqlNameMapper.CamelToSnakeCase)
case class ObsSdUser(
    @Id id: Long,
    name: String,
    @deletedAt deletedAt: Option[OffsetDateTime]
) derives EntityMeta, HasDeletedAt

given InsertBuilder[ObsSdUser, ObsSdUser] = InsertBuilder.derived

// --- recording observer ---

class RecordingObserver extends RepoObserver[ObsUserCreator, ObsUser]:
  val events: mutable.Buffer[String] = mutable.Buffer.empty

  override def creating(entity: ObsUserCreator | ObsUser)(using DbCon[?]): Unit = events += "creating"
  override def created(entity: ObsUser)(using DbCon[?]): Unit = events += "created"
  override def updating(entity: ObsUser)(using DbCon[?]): Unit = events += "updating"
  override def updated(entity: ObsUser)(using DbCon[?]): Unit = events += "updated"
  override def deleting(entity: ObsUser)(using DbCon[?]): Unit = events += "deleting"
  override def deleted(entity: ObsUser)(using DbCon[?]): Unit = events += "deleted"

class RecordingSdObserver extends RepoObserver[ObsSdUser, ObsSdUser]:
  val events: mutable.Buffer[String] = mutable.Buffer.empty

  override def creating(entity: ObsSdUser | ObsSdUser)(using DbCon[?]): Unit = events += "creating"
  override def created(entity: ObsSdUser)(using DbCon[?]): Unit = events += "created"
  override def updating(entity: ObsSdUser)(using DbCon[?]): Unit = events += "updating"
  override def updated(entity: ObsSdUser)(using DbCon[?]): Unit = events += "updated"
  override def deleting(entity: ObsSdUser)(using DbCon[?]): Unit = events += "deleting"
  override def deleted(entity: ObsSdUser)(using DbCon[?]): Unit = events += "deleted"
  override def trashed(entity: ObsSdUser)(using DbCon[?]): Unit = events += "trashed"
  override def forceDeleting(entity: ObsSdUser)(using DbCon[?]): Unit = events += "forceDeleting"
  override def forceDeleted(entity: ObsSdUser)(using DbCon[?]): Unit = events += "forceDeleted"
  override def restoring(entity: ObsSdUser)(using DbCon[?]): Unit = events += "restoring"
  override def restored(entity: ObsSdUser)(using DbCon[?]): Unit = events += "restored"

class RepoObserverTests extends QbTestBase:

  val h2Ddls = Seq("/h2/repo-observer.sql")

  val observer = RecordingObserver()
  val repo = new Repo[ObsUserCreator, ObsUser, Long](
    observers = Vector(observer)
  )

  val plainRepo = Repo[ObsUserCreator, ObsUser, Long]()

  override def beforeEach(context: BeforeEach): Unit =
    super.beforeEach(context)
    observer.events.clear()

  // --- no observers = zero overhead (regression) ---

  test("plain repo works identically without observers"):
    val t = xa()
    t.connect:
      val all = plainRepo.findAll
      assertEquals(all.length, 2)

  // --- create fires: creating, created ---

  test("create fires create hooks"):
    val t = xa()
    t.transact:
      val result = repo.create(ObsUserCreator("Charlie"))
      assertEquals(observer.events.toList, List("creating", "created"))
      assertEquals(result.name, "Charlie")
      // clean up
      repo.deleteById(result.id)

  // --- rawInsert fires no hooks ---

  test("rawInsert fires no hooks"):
    val t = xa()
    t.transact:
      observer.events.clear()
      repo.rawInsert(ObsUserCreator("Temp"))
      assertEquals(observer.events.toList, Nil)
      // clean up
      sql"DELETE FROM obs_user WHERE name = 'Temp'".update.run()

  // --- update fires: updating, updated ---

  test("update fires update hooks"):
    val t = xa()
    t.transact:
      val alice = repo.findById(1L).get
      observer.events.clear()
      repo.update(alice.copy(name = "Alice Updated"))
      assertEquals(observer.events.toList, List("updating", "updated"))
      // restore
      repo.update(ObsUser(1L, "Alice"))

  // --- updatePartial fires: updating, updated ---

  test("updatePartial fires update hooks"):
    val t = xa()
    t.transact:
      val alice = repo.findById(1L).get
      observer.events.clear()
      repo.updatePartial(alice, alice.copy(name = "Alice Partial"))
      assertEquals(observer.events.toList, List("updating", "updated"))
      // restore
      repo.update(ObsUser(1L, "Alice"))

  // --- save tracked entity fires update hooks ---

  test("save tracked entity fires update hooks"):
    val t = xa()
    t.transact:
      val alice = repo.findById(1L).get // tracked
      observer.events.clear()
      repo.save(alice.copy(name = "Alice Saved"))
      assertEquals(observer.events.toList, List("updating", "updated"))
      // restore
      repo.update(ObsUser(1L, "Alice"))

  // --- save untracked entity not in DB fires create hooks ---

  test("save untracked entity fires create hooks"):
    val t = xa()
    t.transact:
      val entity = ObsUser(99L, "Untracked")
      repo.save(entity)
      assertEquals(observer.events.toList, List("creating", "created"))
      // clean up
      sql"DELETE FROM obs_user WHERE id = 99".update.run()

  // --- save untracked entity that exists in DB fires update hooks ---

  test("save untracked entity that exists in DB fires update hooks"):
    val t = xa()
    t.transact:
      // Insert entity first
      sql"INSERT INTO obs_user (id, name) VALUES (50, 'Existing')".update.run()
      observer.events.clear()
      // Save with modified name — entity is not tracked but exists in DB
      val entity = ObsUser(50L, "Modified")
      repo.save(entity)
      assertEquals(observer.events.toList, List("updating", "updated"))
      // Verify the update actually happened
      val fetched = repo.findById(50L).get
      assertEquals(fetched.name, "Modified")
      // clean up
      sql"DELETE FROM obs_user WHERE id = 50".update.run()

  // --- delete(entity) fires deleting, deleted ---

  test("delete(entity) fires delete hooks"):
    val t = xa()
    t.transact:
      val bob = repo.findById(2L).get
      observer.events.clear()
      repo.delete(bob)
      assertEquals(observer.events.toList, List("deleting", "deleted"))
      // restore
      sql"INSERT INTO obs_user (id, name) VALUES (2, 'Bob')".update.run()

  // --- deleteById fires deleting, deleted (fetches entity) ---

  test("deleteById fires delete hooks with entity fetch"):
    val t = xa()
    t.transact:
      observer.events.clear()
      repo.deleteById(2L)
      assertEquals(observer.events.toList, List("deleting", "deleted"))
      // restore
      sql"INSERT INTO obs_user (id, name) VALUES (2, 'Bob')".update.run()

  // --- bulk ops fire nothing ---

  test("rawInsertAll fires no hooks"):
    val t = xa()
    t.transact:
      repo.rawInsertAll(Seq(ObsUserCreator("Bulk1"), ObsUserCreator("Bulk2")))
      assertEquals(observer.events.toList, Nil)
      // clean up
      sql"DELETE FROM obs_user WHERE name LIKE 'Bulk%'".update.run()

  test("deleteAll fires no hooks"):
    val t = xa()
    t.transact:
      val all = repo.findAll
      observer.events.clear()
      repo.deleteAll(all)
      assertEquals(observer.events.toList, Nil)
      // restore
      sql"INSERT INTO obs_user (id, name) VALUES (1, 'Alice')".update.run()
      sql"INSERT INTO obs_user (id, name) VALUES (2, 'Bob')".update.run()

  // --- multiple observers fire in order ---

  test("multiple observers fire in order"):
    val obs1 = RecordingObserver()
    val obs2 = RecordingObserver()
    val multiRepo = new Repo[ObsUserCreator, ObsUser, Long](
      observers = Vector(obs1, obs2)
    )
    val t = xa()
    t.transact:
      multiRepo.create(ObsUserCreator("Multi"))
      assertEquals(obs1.events.toList, List("creating", "created"))
      assertEquals(obs2.events.toList, List("creating", "created"))
      // clean up
      sql"DELETE FROM obs_user WHERE name = 'Multi'".update.run()

  // --- creating exception cancels insert ---

  test("creating exception cancels insert"):
    val failingObserver = new RepoObserver[ObsUserCreator, ObsUser]:
      override def creating(entity: ObsUserCreator | ObsUser)(using DbCon[?]): Unit =
        throw RuntimeException("blocked")

    val failRepo = new Repo[ObsUserCreator, ObsUser, Long](
      observers = Vector(failingObserver)
    )
    val t = xa()
    t.transact:
      intercept[RuntimeException]:
        failRepo.create(ObsUserCreator("Blocked"))
      // no row inserted
      val count = sql"SELECT COUNT(*) FROM obs_user WHERE name = 'Blocked'".query[Long].run().head
      assertEquals(count, 0L)

  // --- DbCon available in hook ---

  test("observer can run SQL inside hook"):
    val sqlObserver = new RepoObserver[ObsUserCreator, ObsUser]:
      override def created(entity: ObsUser)(using DbCon[?]): Unit =
        sql"INSERT INTO obs_audit (event) VALUES ('created')".update.run()

    val sqlRepo = new Repo[ObsUserCreator, ObsUser, Long](
      observers = Vector(sqlObserver)
    )
    val t = xa()
    t.transact:
      sqlRepo.create(ObsUserCreator("Audited"))
      val audits = sql"SELECT event FROM obs_audit".query[String].run()
      assertEquals(audits.head, "created")
      // clean up
      sql"DELETE FROM obs_audit".update.run()
      sql"DELETE FROM obs_user WHERE name = 'Audited'".update.run()

  // --- ec.create() extension ---

  test("ec.create() extension works"):
    given Repo[ObsUserCreator, ObsUser, Long] = repo
    val t = xa()
    t.transact:
      val result = ObsUserCreator("ExtCreated").create()
      assertEquals(result.name, "ExtCreated")
      assertEquals(observer.events.toList, List("creating", "created"))
      // clean up
      sql"DELETE FROM obs_user WHERE name = 'ExtCreated'".update.run()

  // --- SoftDeletes: delete fires deleting, trashed, deleted ---

  test("SoftDeletes: delete fires deleting, trashed, deleted"):
    val sdObs = RecordingSdObserver()
    val sdRepo = new Repo[ObsSdUser, ObsSdUser, Long](
      observers = Vector(sdObs)
    ) with SoftDeletes[ObsSdUser, ObsSdUser, Long]

    val t = xa()
    t.transact:
      val active = sdRepo.findById(1L).get
      sdObs.events.clear()
      sdRepo.delete(active)
      assertEquals(sdObs.events.toList, List("deleting", "trashed", "deleted"))
      // restore
      sdRepo.restoreById(1L)

  // --- SoftDeletes: forceDelete fires forceDelete hooks ---

  test("SoftDeletes: forceDelete fires forceDeleting, deleting, deleted, forceDeleted"):
    val sdObs = RecordingSdObserver()
    val sdRepo = new Repo[ObsSdUser, ObsSdUser, Long](
      observers = Vector(sdObs)
    ) with SoftDeletes[ObsSdUser, ObsSdUser, Long]

    val t = xa()
    t.transact:
      val trashed = sdRepo.withTrashed.run().find(_.name == "Trashed").get
      sdObs.events.clear()
      sdRepo.forceDelete(trashed)
      assertEquals(sdObs.events.toList, List("forceDeleting", "deleting", "deleted", "forceDeleted"))
      // restore
      sql"INSERT INTO obs_sd_user (id, name, deleted_at) VALUES (2, 'Trashed', TIMESTAMP WITH TIME ZONE '2025-01-01T00:00:00Z')".update
        .run()

  // --- SoftDeletes: restore fires restore hooks ---

  test("SoftDeletes: restore fires restoring, updating, updated, restored"):
    val sdObs = RecordingSdObserver()
    val sdRepo = new Repo[ObsSdUser, ObsSdUser, Long](
      observers = Vector(sdObs)
    ) with SoftDeletes[ObsSdUser, ObsSdUser, Long]

    val t = xa()
    t.transact:
      val trashed = sdRepo.withTrashed.run().find(_.name == "Trashed").get
      sdObs.events.clear()
      sdRepo.restore(trashed)
      assertEquals(
        sdObs.events.toList,
        List("restoring", "updating", "updated", "restored")
      )
      // re-trash
      sdRepo.deleteById(2L)

end RepoObserverTests
