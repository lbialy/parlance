import com.augustnagro.magnum.*
import munit.{FunSuite, Tag}
import scala.compiletime.testing.typeCheckErrors

class RelationshipTests extends FunSuite:

  override def munitTestTransforms: List[TestTransform] =
    super.munitTestTransforms :+ new TestTransform(
      "QB",
      test => test.withTags(test.tags + new Tag("QB"))
    )

  @Table(SqlNameMapper.CamelToSnakeCase)
  case class RAuthor(@Id id: Long, name: String) derives EntityMeta

  @Table(SqlNameMapper.CamelToSnakeCase)
  case class RBook(@Id id: Long, authorId: Long, title: String) derives EntityMeta

  @Table(SqlNameMapper.CamelToSnakeCase)
  case class RBookDetail(@Id id: Long, bookId: Long, isbn: String) derives EntityMeta

  test("belongsTo compiles and stores correct metadata"):
    val rel = Relationship.belongsTo[RBook, RAuthor](_.authorId, _.id)
    assert(rel.isInstanceOf[BelongsTo[?, ?]])
    assertEquals(rel.fk.scalaName, "authorId")
    assertEquals(rel.fk.sqlName, "author_id")
    assertEquals(rel.pk.scalaName, "id")
    assertEquals(rel.pk.sqlName, "id")

  test("hasOne compiles and stores correct metadata"):
    val rel = Relationship.hasOne[RAuthor, RBookDetail](_.id, _.bookId)
    assert(rel.isInstanceOf[HasOne[?, ?]])
    assertEquals(rel.fk.scalaName, "id")
    assertEquals(rel.fk.sqlName, "id")
    assertEquals(rel.pk.scalaName, "bookId")
    assertEquals(rel.pk.sqlName, "book_id")

  test("non-existent field on source fails at compile time"):
    val errors = typeCheckErrors("""
      import com.augustnagro.magnum.*
      @Table(SqlNameMapper.CamelToSnakeCase)
      case class NBook(@Id id: Long, authorId: Long, title: String) derives EntityMeta
      @Table(SqlNameMapper.CamelToSnakeCase)
      case class NAuthor(@Id id: Long, name: String) derives EntityMeta
      Relationship.belongsTo[NBook, NAuthor](_.nonExistent, _.id)
    """)
    assert(errors.nonEmpty, "expected compile error for non-existent source field")

  test("non-existent field on target fails at compile time"):
    val errors = typeCheckErrors("""
      import com.augustnagro.magnum.*
      @Table(SqlNameMapper.CamelToSnakeCase)
      case class NBook2(@Id id: Long, authorId: Long, title: String) derives EntityMeta
      @Table(SqlNameMapper.CamelToSnakeCase)
      case class NAuthor2(@Id id: Long, name: String) derives EntityMeta
      Relationship.belongsTo[NBook2, NAuthor2](_.authorId, _.nonExistent)
    """)
    assert(errors.nonEmpty, "expected compile error for non-existent target field")

  test("belongsTo is a Relationship"):
    val r: Relationship[RBook, RAuthor] =
      Relationship.belongsTo[RBook, RAuthor](_.authorId, _.id)
    assertEquals(r.fk.scalaName, "authorId")
    assertEquals(r.pk.scalaName, "id")

  test("hasOne is a Relationship"):
    val r: Relationship[RAuthor, RBookDetail] =
      Relationship.hasOne[RAuthor, RBookDetail](_.id, _.bookId)
    assertEquals(r.fk.scalaName, "id")
    assertEquals(r.pk.scalaName, "bookId")

end RelationshipTests
