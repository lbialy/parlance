package shared

import com.augustnagro.magnum.*
import munit.FunSuite

import java.time.OffsetDateTime
import java.util.UUID

def entityExtensionTests(
    suite: FunSuite,
    dbType: DbType,
    xa: () => Transactor
)(using
    munit.Location,
    DbCodec[UUID],
    DbCodec[Boolean],
    DbCodec[OffsetDateTime]
): Unit =
  import suite.*

  @Table(dbType, SqlNameMapper.CamelToSnakeCase)
  case class Person(
      id: Long,
      firstName: Option[String],
      lastName: String,
      isAdmin: Boolean,
      created: OffsetDateTime,
      socialId: Option[UUID]
  ) derives DbCodec, TableMeta

  given personRepo: Repo[Person, Person, Long] = Repo[Person, Person, Long]()

  // --- Group 1: Core lifecycle ---

  test("entity.save() on tracked entity"):
    assume(dbType != ClickhouseDbType)
    xa().connect:
      val original = personRepo.findById(1L).get
      val modified = original.copy(lastName = "ExtSaved")
      modified.save()
      val fetched = personRepo.findById(1L).get
      assert(fetched.lastName == "ExtSaved")
      assert(fetched.firstName == original.firstName)

  test("entity.delete()"):
    assume(dbType != ClickhouseDbType)
    xa().connect:
      val p = personRepo.findById(1L).get
      p.delete()
      assert(personRepo.findById(1L).isEmpty)

  test("entity.refresh()"):
    assume(dbType != ClickhouseDbType)
    xa().connect:
      val original = personRepo.findById(1L).get
      personRepo.update(original.copy(lastName = "Refreshed"))
      val refreshed = original.refresh()
      assert(refreshed.lastName == "Refreshed")

  // --- Group 2: Identity comparison ---

  test("entity.is() with same PK"):
    xa().connect:
      val p1 = personRepo.findById(1L).get
      val p2 = p1.copy(lastName = "Different")
      assert(p1.is(p2))

  test("entity.isNot() with different PK"):
    xa().connect:
      val p1 = personRepo.findById(1L).get
      val p2 = personRepo.findById(2L).get
      assert(p1.isNot(p2))

  test("entity.is() returns false for different PKs"):
    xa().connect:
      val p1 = personRepo.findById(1L).get
      val p2 = personRepo.findById(2L).get
      assert(!p1.is(p2))

  // --- Group 3: Change tracking ---

  test("entity.isDirty on modified entity"):
    assume(dbType != ClickhouseDbType)
    xa().connect:
      val original = personRepo.findById(1L).get
      val modified = original.copy(lastName = "Dirty")
      assert(modified.isDirty)

  test("entity.isDirty on clean entity"):
    assume(dbType != ClickhouseDbType)
    xa().connect:
      val original = personRepo.findById(1L).get
      assert(!original.isDirty)

  test("entity.isDirty on untracked entity"):
    assume(dbType != ClickhouseDbType)
    val t = xa()
    val person = t.connect:
      personRepo.findById(1L).get
    // New connect block — entity is not tracked
    t.connect:
      assert(!person.isDirty)

  test("entity.isClean"):
    assume(dbType != ClickhouseDbType)
    xa().connect:
      val original = personRepo.findById(1L).get
      assert(original.isClean)
      val modified = original.copy(lastName = "NotClean")
      assert(!modified.isClean)

  test("entity.getOriginal returns original snapshot"):
    assume(dbType != ClickhouseDbType)
    xa().connect:
      val original = personRepo.findById(1L).get
      val modified = original.copy(lastName = "Changed")
      val retrieved = modified.getOriginal
      assert(retrieved.lastName == original.lastName)

  test("entity.getOriginal on untracked returns self"):
    assume(dbType != ClickhouseDbType)
    val t = xa()
    val person = t.connect:
      personRepo.findById(1L).get
    t.connect:
      val orig = person.getOriginal
      assert(orig eq person)

  test("entity.getChanges with field changes"):
    assume(dbType != ClickhouseDbType)
    xa().connect:
      val original = personRepo.findById(1L).get
      val modified = original.copy(
        lastName = "NewLast",
        isAdmin = !original.isAdmin
      )
      val changes = modified.getChanges
      assert(changes.size == 2)
      assert(changes("lastName") == (original.lastName, "NewLast"))
      assert(changes("isAdmin") == (original.isAdmin, !original.isAdmin))

  test("entity.getChanges with no changes"):
    assume(dbType != ClickhouseDbType)
    xa().connect:
      val original = personRepo.findById(1L).get
      val changes = original.getChanges
      assert(changes.isEmpty)

end entityExtensionTests
