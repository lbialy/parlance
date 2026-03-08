import munit.FunSuite

class EffectiveSubsetTests extends FunSuite:

  test("creator must extend CreatorOf[E] when EC != E"):
    val errors = compileErrors("""
      import ma.chinespirit.parlance.*
      @Table(SqlNameMapper.CamelToSnakeCase)
      case class PersonCreator(name: String) derives DbCodec
      @Table(SqlNameMapper.CamelToSnakeCase)
      case class Person(@Id id: Long, name: String) derives EntityMeta
      Repo[PersonCreator, Person, Long]()
    """)
    assert(errors.contains("must extend CreatorOf"), s"Expected CreatorOf error, got: $errors")

  test("Repo macro error if EC not an effective subset of E"):
    val errors = compileErrors("""
      import ma.chinespirit.parlance.*
      @Table(SqlNameMapper.CamelToSnakeCase)
      case class Person(@Id id: Long, last: String) derives EntityMeta, DbCodec
      case class PersonCreator(first: String, last: String) extends CreatorOf[Person] derives DbCodec
      Repo[PersonCreator, Person, Long]()
    """)
    assert(errors.contains("effective subset"), s"Expected effective subset error, got: $errors")

  test("creator with @Id field should not compile"):
    val errors = compileErrors("""
      import ma.chinespirit.parlance.*
      @Table(SqlNameMapper.CamelToSnakeCase)
      case class Entity(@Id id: Long, name: String) derives EntityMeta
      case class BadCreator(id: Long, name: String) extends CreatorOf[Entity] derives DbCodec
      Repo[BadCreator, Entity, Long]()
    """)
    assert(
      errors.contains("should not contain auto-managed fields: id"),
      s"Expected auto-managed field error, got: $errors"
    )

  test("creator with @createdAt field should not compile"):
    val errors = compileErrors("""
      import ma.chinespirit.parlance.*
      import java.time.OffsetDateTime
      @Table(SqlNameMapper.CamelToSnakeCase)
      case class Entity(@Id id: Long, name: String, @createdAt createdAt: OffsetDateTime) derives EntityMeta
      case class BadCreator(name: String, createdAt: OffsetDateTime) extends CreatorOf[Entity] derives DbCodec
      Repo[BadCreator, Entity, Long]()
    """)
    assert(
      errors.contains("should not contain auto-managed fields: createdAt"),
      s"Expected auto-managed field error, got: $errors"
    )

  test("creator with @updatedAt field should not compile"):
    val errors = compileErrors("""
      import ma.chinespirit.parlance.*
      import java.time.OffsetDateTime
      @Table(SqlNameMapper.CamelToSnakeCase)
      case class Entity(@Id id: Long, name: String, @updatedAt updatedAt: OffsetDateTime) derives EntityMeta
      case class BadCreator(name: String, updatedAt: OffsetDateTime) extends CreatorOf[Entity] derives DbCodec
      Repo[BadCreator, Entity, Long]()
    """)
    assert(
      errors.contains("should not contain auto-managed fields: updatedAt"),
      s"Expected auto-managed field error, got: $errors"
    )

  test("creator with @deletedAt field should not compile"):
    val errors = compileErrors("""
      import ma.chinespirit.parlance.*
      import java.time.OffsetDateTime
      @Table(SqlNameMapper.CamelToSnakeCase)
      case class Entity(@Id id: Long, name: String, @deletedAt deletedAt: Option[OffsetDateTime]) derives EntityMeta
      case class BadCreator(name: String, deletedAt: Option[OffsetDateTime]) extends CreatorOf[Entity] derives DbCodec
      Repo[BadCreator, Entity, Long]()
    """)
    assert(
      errors.contains("should not contain auto-managed fields: deletedAt"),
      s"Expected auto-managed field error, got: $errors"
    )

  test("creator with multiple auto-managed fields should not compile"):
    val errors = compileErrors("""
      import ma.chinespirit.parlance.*
      import java.time.OffsetDateTime
      @Table(SqlNameMapper.CamelToSnakeCase)
      case class Entity(
          @Id id: Long,
          name: String,
          @createdAt createdAt: OffsetDateTime,
          @updatedAt updatedAt: OffsetDateTime
      ) derives EntityMeta
      case class BadCreator(id: Long, name: String, createdAt: OffsetDateTime, updatedAt: OffsetDateTime) extends CreatorOf[Entity] derives DbCodec
      Repo[BadCreator, Entity, Long]()
    """)
    assert(
      errors.contains("should not contain auto-managed fields"),
      s"Expected auto-managed field error, got: $errors"
    )
    assert(errors.contains("createdAt"), s"Expected createdAt in error, got: $errors")
    assert(errors.contains("id"), s"Expected id in error, got: $errors")
    assert(errors.contains("updatedAt"), s"Expected updatedAt in error, got: $errors")

  test("EC = E is allowed even with auto-managed fields"):
    val errors = compileErrors("""
      import ma.chinespirit.parlance.*
      import java.time.OffsetDateTime
      @Table(SqlNameMapper.CamelToSnakeCase)
      case class SelfEntity(@Id id: Long, name: String, @createdAt createdAt: OffsetDateTime) derives EntityMeta, DbCodec
      Repo[SelfEntity, SelfEntity, Long]()
    """)
    assert(errors.isEmpty, s"Expected no errors, got: $errors")

  test("creator without auto-managed fields compiles successfully"):
    val errors = compileErrors("""
      import ma.chinespirit.parlance.*
      import java.time.OffsetDateTime
      @Table(SqlNameMapper.CamelToSnakeCase)
      case class Entity(
          @Id id: Long,
          name: String,
          @createdAt createdAt: OffsetDateTime,
          @updatedAt updatedAt: OffsetDateTime
      ) derives EntityMeta
      case class GoodCreator(name: String) extends CreatorOf[Entity] derives DbCodec
      Repo[GoodCreator, Entity, Long]()
    """)
    assert(errors.isEmpty, s"Expected no errors, got: $errors")
