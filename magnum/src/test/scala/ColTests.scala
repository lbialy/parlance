import com.augustnagro.magnum.*
import munit.{FunSuite, Tag}

class ColTests extends FunSuite:

  override def munitTestTransforms: List[TestTransform] =
    super.munitTestTransforms :+ new TestTransform(
      "QB",
      test => test.withTags(test.tags + new Tag("QB"))
    )

  test("Col has correct scalaName and sqlName"):
    val col = Col[String]("firstName", "first_name")
    assertEquals(col.scalaName, "firstName")
    assertEquals(col.sqlName, "first_name")

  test("Col queryRepr is sqlName"):
    val col = Col[String]("firstName", "first_name")
    assertEquals(col.queryRepr, "first_name")

  test("BoundCol queryRepr is alias.sqlName"):
    val col = Col[String]("firstName", "first_name")
    val bound = col.bound("u")
    assertEquals(bound.queryRepr, "u.first_name")

  test("BoundCol preserves scalaName and sqlName"):
    val col = Col[String]("firstName", "first_name")
    val bound = col.bound("u")
    assertEquals(bound.scalaName, "firstName")
    assertEquals(bound.sqlName, "first_name")
    assertEquals(bound.alias, "u")

  test("BoundCol constructed directly"):
    val col = Col[Long]("id", "id")
    val bound = BoundCol(col, "p")
    assertEquals(bound.queryRepr, "p.id")

  test("Col.bound with different aliases produces different queryRepr"):
    val col = Col[Int]("age", "age")
    val bound1 = col.bound("u")
    val bound2 = col.bound("p")
    assertEquals(bound1.queryRepr, "u.age")
    assertEquals(bound2.queryRepr, "p.age")

  test("Col toString"):
    val col = Col[String]("firstName", "first_name")
    assertEquals(col.toString, "Col(firstName, first_name)")

  test("BoundCol toString"):
    val col = Col[String]("firstName", "first_name")
    val bound = col.bound("u")
    assertEquals(bound.toString, "BoundCol(firstName, u.first_name)")

  test("Col is SqlLiteral"):
    val col: SqlLiteral = Col[String]("firstName", "first_name")
    assertEquals(col.queryRepr, "first_name")

  test("BoundCol is SqlLiteral"):
    val bound: SqlLiteral = Col[String]("firstName", "first_name").bound("u")
    assertEquals(bound.queryRepr, "u.first_name")

  test("BoundCol renders correctly in sql\"\" fragment"):
    val bound = Col[String]("firstName", "first_name").bound("u")
    val frag = sql"SELECT $bound FROM users u"
    assertEquals(frag.sqlString, "SELECT u.first_name FROM users u")

  test("Col renders correctly in sql\"\" fragment"):
    val col = Col[String]("firstName", "first_name")
    val frag = sql"SELECT $col FROM users"
    assertEquals(frag.sqlString, "SELECT first_name FROM users")

end ColTests
