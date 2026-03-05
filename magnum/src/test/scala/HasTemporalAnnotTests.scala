import com.augustnagro.magnum.*

import java.time.{Instant, OffsetDateTime}

class HasTemporalAnnotTests extends munit.FunSuite:

  // --- HasDeletedAt negative tests ---

  test("@deletedAt on non-Option type should not compile"):
    @Table(SqlNameMapper.CamelToSnakeCase)
    case class Bad(@Id id: Long, @deletedAt deletedAt: Instant) derives EntityMeta
    val errors = compileErrors("HasDeletedAt.derived[Bad]")
    assert(
      errors.contains(
        "@deletedAt field 'deletedAt' must be Option[Instant] or Option[OffsetDateTime], but found java.time.Instant"
      ),
      errors
    )

  test("@deletedAt on String should not compile"):
    @Table(SqlNameMapper.CamelToSnakeCase)
    case class Bad(@Id id: Long, @deletedAt deletedAt: String) derives EntityMeta
    val errors = compileErrors("HasDeletedAt.derived[Bad]")
    assert(
      errors.contains(
        "@deletedAt field 'deletedAt' must be Option[Instant] or Option[OffsetDateTime], but found scala.Predef.String"
      ),
      errors
    )

  test("missing @deletedAt annotation should not compile"):
    @Table(SqlNameMapper.CamelToSnakeCase)
    case class Bad(@Id id: Long, deletedAt: Option[OffsetDateTime]) derives EntityMeta
    val errors = compileErrors("HasDeletedAt.derived[Bad]")
    assert(
      errors.contains(
        "Bad has no field annotated with @deletedAt. Add @deletedAt to the timestamp field to derive HasDeletedAt."
      ),
      errors
    )

  // --- HasCreatedAt negative tests ---

  test("@createdAt on String should not compile"):
    @Table(SqlNameMapper.CamelToSnakeCase)
    case class Bad(@Id id: Long, @createdAt createdAt: String) derives EntityMeta
    val errors = compileErrors("HasCreatedAt.derived[Bad]")
    assert(
      errors.contains(
        "@createdAt field 'createdAt' must be Instant or OffsetDateTime, but found scala.Predef.String"
      ),
      errors
    )

  test("@createdAt on Option[Instant] should not compile"):
    @Table(SqlNameMapper.CamelToSnakeCase)
    case class Bad(@Id id: Long, @createdAt createdAt: Option[Instant]) derives EntityMeta
    val errors = compileErrors("HasCreatedAt.derived[Bad]")
    assert(
      errors.contains(
        "@createdAt field 'createdAt' must be Instant or OffsetDateTime, but found scala.Option[java.time.Instant]"
      ),
      errors
    )

  test("missing @createdAt annotation should not compile"):
    @Table(SqlNameMapper.CamelToSnakeCase)
    case class Bad(@Id id: Long, createdAt: Instant) derives EntityMeta
    val errors = compileErrors("HasCreatedAt.derived[Bad]")
    assert(
      errors.contains(
        "Bad has no field annotated with @createdAt. Add @createdAt to the timestamp field to derive HasCreatedAt."
      ),
      errors
    )

  // --- HasUpdatedAt negative tests ---

  test("@updatedAt on Option[Instant] should not compile"):
    @Table(SqlNameMapper.CamelToSnakeCase)
    case class Bad(@Id id: Long, @updatedAt updatedAt: Option[Instant]) derives EntityMeta
    val errors = compileErrors("HasUpdatedAt.derived[Bad]")
    assert(
      errors.contains(
        "@updatedAt field 'updatedAt' must be Instant or OffsetDateTime, but found scala.Option[java.time.Instant]"
      ),
      errors
    )

  test("@updatedAt on String should not compile"):
    @Table(SqlNameMapper.CamelToSnakeCase)
    case class Bad(@Id id: Long, @updatedAt updatedAt: String) derives EntityMeta
    val errors = compileErrors("HasUpdatedAt.derived[Bad]")
    assert(
      errors.contains(
        "@updatedAt field 'updatedAt' must be Instant or OffsetDateTime, but found scala.Predef.String"
      ),
      errors
    )

  test("missing @updatedAt annotation should not compile"):
    @Table(SqlNameMapper.CamelToSnakeCase)
    case class Bad(@Id id: Long, updatedAt: Instant) derives EntityMeta
    val errors = compileErrors("HasUpdatedAt.derived[Bad]")
    assert(
      errors.contains(
        "Bad has no field annotated with @updatedAt. Add @updatedAt to the timestamp field to derive HasUpdatedAt."
      ),
      errors
    )

  // --- Duplicate annotation tests ---

  test("multiple @deletedAt fields should not compile"):
    @Table(SqlNameMapper.CamelToSnakeCase)
    case class Bad(@Id id: Long, @deletedAt a: Option[Instant], @deletedAt b: Option[Instant]) derives EntityMeta
    val errors = compileErrors("HasDeletedAt.derived[Bad]")
    assert(
      errors.contains(
        "Bad has multiple fields annotated with @deletedAt (a, b). Only one @deletedAt field is allowed."
      ),
      errors
    )

  test("multiple @createdAt fields should not compile"):
    @Table(SqlNameMapper.CamelToSnakeCase)
    case class Bad(@Id id: Long, @createdAt a: Instant, @createdAt b: OffsetDateTime) derives EntityMeta
    val errors = compileErrors("HasCreatedAt.derived[Bad]")
    assert(
      errors.contains(
        "Bad has multiple fields annotated with @createdAt (a, b). Only one @createdAt field is allowed."
      ),
      errors
    )

  test("multiple @updatedAt fields should not compile"):
    @Table(SqlNameMapper.CamelToSnakeCase)
    case class Bad(@Id id: Long, @updatedAt a: Instant, @updatedAt b: OffsetDateTime) derives EntityMeta
    val errors = compileErrors("HasUpdatedAt.derived[Bad]")
    assert(
      errors.contains(
        "Bad has multiple fields annotated with @updatedAt (a, b). Only one @updatedAt field is allowed."
      ),
      errors
    )

  // --- Positive: valid derivations compile ---

  test("@deletedAt on Option[OffsetDateTime] compiles"):
    @Table(SqlNameMapper.CamelToSnakeCase)
    case class Good(@Id id: Long, @deletedAt deletedAt: Option[OffsetDateTime]) derives EntityMeta, HasDeletedAt
    val hda = summon[HasDeletedAt[Good]]
    assertEquals(hda.index, 1)
    assertEquals(hda.column.scalaName, "deletedAt")

  test("@deletedAt on Option[Instant] compiles"):
    @Table(SqlNameMapper.CamelToSnakeCase)
    case class Good(@Id id: Long, @deletedAt deletedAt: Option[Instant]) derives EntityMeta, HasDeletedAt
    val hda = summon[HasDeletedAt[Good]]
    assertEquals(hda.index, 1)

  test("@createdAt on Instant compiles"):
    @Table(SqlNameMapper.CamelToSnakeCase)
    case class Good(@Id id: Long, @createdAt createdAt: Instant) derives EntityMeta, HasCreatedAt
    val hca = summon[HasCreatedAt[Good]]
    assertEquals(hca.index, 1)
    assertEquals(hca.column.scalaName, "createdAt")

  test("@updatedAt on OffsetDateTime compiles"):
    @Table(SqlNameMapper.CamelToSnakeCase)
    case class Good(@Id id: Long, @updatedAt updatedAt: OffsetDateTime) derives EntityMeta, HasUpdatedAt
    val hua = summon[HasUpdatedAt[Good]]
    assertEquals(hua.index, 1)
    assertEquals(hua.column.scalaName, "updatedAt")

end HasTemporalAnnotTests
