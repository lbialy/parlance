import com.augustnagro.magnum.*
import munit.{FunSuite, Tag}
import scala.compiletime.testing.typeCheckErrors

class ColumnsTests extends FunSuite:

  override def munitTestTransforms: List[TestTransform] =
    super.munitTestTransforms :+ new TestTransform(
      "QB",
      test => test.withTags(test.tags + new Tag("QB"))
    )

  @Table(H2DbType, SqlNameMapper.CamelToSnakeCase)
  case class User(@Id id: Long, firstName: String, age: Int)
      derives DbCodec, TableMeta

  @Table(H2DbType, SqlNameMapper.CamelToSnakeCase)
  case class WithOptional(@Id id: Long, nickname: Option[String])
      derives DbCodec, TableMeta

  @Table(H2DbType, SqlNameMapper.CamelToSnakeCase)
  case class WithSqlName(
      @Id id: Long,
      @SqlName("custom_col") description: String
  ) derives DbCodec, TableMeta

  val userCols = Columns.of[User]
  val optCols = Columns.of[WithOptional]
  val sqlNameCols = Columns.of[WithSqlName]

  test("Columns.of[User].firstName returns Col[String] with correct names"):
    val col: Col[String] = userCols.firstName
    assertEquals(col.scalaName, "firstName")
    assertEquals(col.sqlName, "first_name")

  test("Columns.of[User].id returns Col[Long]"):
    val col: Col[Long] = userCols.id
    assertEquals(col.scalaName, "id")
    assertEquals(col.sqlName, "id")

  test("Columns.of[User].age returns Col[Int]"):
    val col: Col[Int] = userCols.age
    assertEquals(col.scalaName, "age")
    assertEquals(col.sqlName, "age")

  test("non-existent field is a compile error"):
    val errors = typeCheckErrors("""
      import com.augustnagro.magnum.*
      @Table(H2DbType, SqlNameMapper.CamelToSnakeCase)
      case class NegTest(@Id id: Long, name: String) derives DbCodec, TableMeta
      val cols = Columns.of[NegTest]
      cols.nonExistent
    """)
    assert(errors.nonEmpty, "expected compile error for non-existent field")

  test("@SqlName override produces correct sqlName"):
    val col: Col[String] = sqlNameCols.description
    assertEquals(col.scalaName, "description")
    assertEquals(col.sqlName, "custom_col")

  test("Option[String] field returns Col[Option[String]]"):
    val col: Col[Option[String]] = optCols.nickname
    assertEquals(col.scalaName, "nickname")
    assertEquals(col.sqlName, "nickname")

end ColumnsTests
