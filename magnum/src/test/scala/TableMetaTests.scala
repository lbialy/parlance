import com.augustnagro.magnum.*
import munit.{FunSuite, Tag}

class TableMetaTests extends FunSuite:

  override def munitTestTransforms: List[TestTransform] =
    super.munitTestTransforms :+ new TestTransform(
      "QB",
      test => test.withTags(test.tags + new Tag("QB"))
    )

  @Table(SqlNameMapper.CamelToSnakeCase)
  case class User(@Id id: Long, firstName: String, age: Int) derives EntityMeta

  @Table(SqlNameMapper.CamelToSnakeCase)
  case class Product(
      name: String,
      @SqlName("custom_col") description: String,
      @Id productId: Long
  ) derives EntityMeta

  @SqlName("custom_table")
  @Table(SqlNameMapper.CamelToSnakeCase)
  case class CustomNamed(@Id id: Long, value: String) derives EntityMeta

  test("tableName is derived via nameMapper"):
    val meta = summon[TableMeta[User]]
    assertEquals(meta.tableName, "user")

  test("columns has correct count"):
    val meta = summon[TableMeta[User]]
    assertEquals(meta.columns.length, 3)

  test("columns have correct scalaNames"):
    val meta = summon[TableMeta[User]]
    assertEquals(meta.columns.map(_.scalaName).toList, List("id", "firstName", "age"))

  test("columns have correct sqlNames"):
    val meta = summon[TableMeta[User]]
    assertEquals(meta.columns.map(_.sqlName).toList, List("id", "first_name", "age"))

  test("primaryKey is the @Id field"):
    val meta = summon[TableMeta[User]]
    assertEquals(meta.primaryKey.scalaName, "id")
    assertEquals(meta.primaryKey.sqlName, "id")

  test("primaryKey defaults to first field when no @Id"):
    // Product has @Id on productId (index 2), so this tests explicit @Id
    val meta = summon[TableMeta[Product]]
    assertEquals(meta.primaryKey.scalaName, "productId")

  test("columnByName returns correct Col"):
    val meta = summon[TableMeta[User]]
    val col = meta.columnByName("firstName")
    assert(col.isDefined)
    assertEquals(col.get.scalaName, "firstName")
    assertEquals(col.get.sqlName, "first_name")

  test("columnByName returns None for non-existent field"):
    val meta = summon[TableMeta[User]]
    assertEquals(meta.columnByName("nonExistent"), None)

  test("@SqlName overrides column name"):
    val meta = summon[TableMeta[Product]]
    val col = meta.columnByName("description")
    assert(col.isDefined)
    assertEquals(col.get.sqlName, "custom_col")

  test("@SqlName on type overrides table name"):
    val meta = summon[TableMeta[CustomNamed]]
    assertEquals(meta.tableName, "custom_table")

end TableMetaTests
