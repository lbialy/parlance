package shared

import com.augustnagro.magnum.*
import munit.FunSuite

import java.time.OffsetDateTime
import java.util.UUID

def saveTests[D <: DatabaseType](
    suite: FunSuite,
    xa: () => Transactor[D]
)(using
    munit.Location,
    DbCodec[UUID],
    DbCodec[Boolean],
    DbCodec[OffsetDateTime]
): Unit =
  import suite.*

  @Table(SqlNameMapper.CamelToSnakeCase)
  case class Person(
      id: Long,
      firstName: Option[String],
      lastName: String,
      isAdmin: Boolean,
      created: OffsetDateTime,
      socialId: Option[UUID]
  ) derives EntityMeta

  val personRepo = Repo[Person, Person, Long]()

  test("save tracked entity - single field"):
    assume(xa().databaseType != ClickHouse)
    xa().connect:
      val original = personRepo.findById(1L).get
      val modified = original.copy(lastName = "SavedLastName")
      personRepo.save(modified)
      val fetched = personRepo.findById(1L).get
      assert(fetched.lastName == "SavedLastName")
      assert(fetched.firstName == original.firstName)
      assert(fetched.isAdmin == original.isAdmin)

  test("save tracked entity - multiple fields"):
    assume(xa().databaseType != ClickHouse)
    xa().connect:
      val original = personRepo.findById(1L).get
      val modified =
        original.copy(firstName = Some("SavedFirst"), isAdmin = !original.isAdmin)
      personRepo.save(modified)
      val fetched = personRepo.findById(1L).get
      assert(fetched.firstName == Some("SavedFirst"))
      assert(fetched.isAdmin == !original.isAdmin)
      assert(fetched.lastName == original.lastName)

  test("save no-op when unchanged"):
    assume(xa().databaseType != ClickHouse)
    xa().connect:
      val original = personRepo.findById(1L).get
      personRepo.save(original)
      val fetched = personRepo.findById(1L).get
      assert(fetched == original)

  test("save untracked entity - falls back to full update"):
    assume(xa().databaseType != ClickHouse)
    val t = xa()
    // Get a snapshot of the entity from one connect block
    val person = t.connect:
      personRepo.findById(1L).get
    // In a new connect block, the entity is untracked — save falls back to update
    val modified = person.copy(lastName = "UntrackedSave")
    t.connect:
      personRepo.save(modified)
    t.connect:
      val fetched = personRepo.findById(1L).get
      assert(fetched.lastName == "UntrackedSave")

  test("findAll tracks all entities"):
    assume(xa().databaseType != ClickHouse)
    xa().connect:
      val all = personRepo.findAll
      val target = all.find(_.id == 1L).get
      val modified = target.copy(lastName = "FindAllTracked")
      personRepo.save(modified)
      val fetched = personRepo.findById(1L).get
      assert(fetched.lastName == "FindAllTracked")

  test("findAllById tracks entities"):
    assume(xa().databaseType != ClickHouse)
    assume(xa().databaseType != MySQL)
    assume(xa().databaseType != SQLite)
    xa().connect:
      val people = personRepo.findAllById(Vector(1L, 2L))
      val target = people.find(_.id == 1L).get
      val modified = target.copy(lastName = "FindAllByIdTracked")
      personRepo.save(modified)
      val fetched = personRepo.findById(1L).get
      assert(fetched.lastName == "FindAllByIdTracked")

  test("identity map first-load-wins"):
    assume(xa().databaseType != ClickHouse)
    xa().connect:
      val original = personRepo.findById(1L).get
      // Update directly — bypasses identity map
      personRepo.update(original.copy(lastName = "DirectUpdate"))
      // Re-fetch — identity map should still hold the first-loaded snapshot
      val refetched = personRepo.findById(1L).get
      assert(refetched.lastName == "DirectUpdate")
      // But save should diff against the ORIGINAL first load
      val modified = refetched.copy(firstName = Some("NewFirst"))
      personRepo.save(modified)
      val final_ = personRepo.findById(1L).get
      assert(final_.firstName == Some("NewFirst"))
      // lastName should still be "DirectUpdate" since save diffed against
      // original (which had the old lastName), and we only changed firstName
      assert(final_.lastName == "DirectUpdate")

  test("identity map scoped to connection block"):
    assume(xa().databaseType != ClickHouse)
    val t = xa()
    // Load and modify in first block
    t.connect:
      val original = personRepo.findById(1L).get
      personRepo.update(original.copy(lastName = "ScopedBlock1"))
    // Second block: entity is untracked, so save does a full update
    // (if identity map leaked across blocks, save would diff against
    //  the original from block 1, not do a full update)
    val person = t.connect:
      personRepo.findById(1L).get
    val modified = person.copy(lastName = "ScopedBlock2")
    t.connect:
      personRepo.save(modified)
    t.connect:
      val fetched = personRepo.findById(1L).get
      assert(fetched.lastName == "ScopedBlock2")

  test("transact + save works"):
    assume(xa().databaseType != ClickHouse)
    xa().transact:
      val original = personRepo.findById(1L).get
      val modified = original.copy(lastName = "TransactSave")
      personRepo.save(modified)
      val fetched = personRepo.findById(1L).get
      assert(fetched.lastName == "TransactSave")

end saveTests
