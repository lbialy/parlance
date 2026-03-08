package shared

import ma.chinespirit.parlance.*
import munit.FunSuite

import java.time.OffsetDateTime
import java.util.UUID

def partialUpdateTests[D <: SupportsMutations](
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

  test("updatePartial single field changed"):
    xa().connect:
      val original = personRepo.findById(1L).get
      val current = original.copy(lastName = "UpdatedLastName")
      personRepo.updatePartial(original, current)
      val fetched = personRepo.findById(1L).get
      assert(fetched.lastName == "UpdatedLastName")
      assert(fetched.firstName == original.firstName)
      assert(fetched.isAdmin == original.isAdmin)

  test("updatePartial multiple fields changed"):
    xa().connect:
      val original = personRepo.findById(1L).get
      val current =
        original.copy(firstName = Some("NewFirst"), isAdmin = !original.isAdmin)
      personRepo.updatePartial(original, current)
      val fetched = personRepo.findById(1L).get
      assert(fetched.firstName == Some("NewFirst"))
      assert(fetched.isAdmin == !original.isAdmin)
      assert(fetched.lastName == original.lastName)

  test("updatePartial no-op when nothing changed"):
    xa().connect:
      val original = personRepo.findById(1L).get
      personRepo.updatePartial(original, original)
      val fetched = personRepo.findById(1L).get
      assert(fetched == original)

  test("updatePartial rejects different PKs"):
    xa().connect:
      val p1 = personRepo.findById(1L).get
      val p2 = personRepo.findById(2L).get
      intercept[IllegalArgumentException]:
        personRepo.updatePartial(p1, p2)

  test("updatePartial Option field None to Some"):
    xa().connect:
      val original = personRepo.findById(1L).get
      val newSocialId = UUID.randomUUID()
      val current = original.copy(socialId = Some(newSocialId))
      personRepo.updatePartial(original, current)
      val fetched = personRepo.findById(1L).get
      assert(fetched.socialId == Some(newSocialId))

  test("updatePartial Option field Some to None"):
    xa().connect:
      val original = personRepo.findById(1L).get
      val withSocial = original.copy(socialId = Some(UUID.randomUUID()))
      personRepo.update(withSocial)
      val current = withSocial.copy(socialId = None)
      personRepo.updatePartial(withSocial, current)
      val fetched = personRepo.findById(1L).get
      assert(fetched.socialId == None)

end partialUpdateTests
