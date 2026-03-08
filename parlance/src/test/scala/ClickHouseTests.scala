import ma.chinespirit.parlance.*
import com.clickhouse.jdbc.ClickHouseDataSource
import com.dimafeng.testcontainers.ClickHouseContainer
import com.dimafeng.testcontainers.munit.fixtures.TestContainersFixtures
import munit.{AnyFixture, FunSuite, Location, Tag}
import org.testcontainers.utility.DockerImageName
import shared.*

import java.nio.file.{Files, Path}
import java.util.UUID
import scala.util.Using

class ClickHouseTests extends FunSuite, TestContainersFixtures:

  override def munitTestTransforms: List[TestTransform] =
    super.munitTestTransforms :+ new TestTransform(
      "Slow",
      test => test.withTags(test.tags + new Tag("Slow"))
    )

  sharedTests(this, xa)

  test("repo.update disallowed on ClickHouse"):
    val errors = compileErrors("""
      import ma.chinespirit.parlance.*
      @Table()
      case class Item(@Id id: Long, name: String) derives EntityMeta
      val repo = Repo[Item, Item, Long]()
      def test(using DbCon[ClickHouse]): Unit =
        repo.update(Item(1L, "x"))
    """)
    assert(
      errors.contains("SupportsMutations"),
      s"Expected SupportsMutations compile error but got: $errors"
    )

  test("entity.save() disallowed on ClickHouse"):
    val errors = compileErrors("""
      import ma.chinespirit.parlance.*
      @Table()
      case class Item(@Id id: Long, name: String) derives EntityMeta
      given repo: Repo[Item, Item, Long] = Repo[Item, Item, Long]()
      def test(using DbCon[ClickHouse]): Unit =
        Item(1L, "x").save()
    """)
    assert(
      errors.contains("SupportsMutations"),
      s"Expected SupportsMutations compile error but got: $errors"
    )

  test("qb.upsertByPk disallowed on ClickHouse"):
    val errors = compileErrors("""
      import ma.chinespirit.parlance.*
      @Table()
      case class Item(@Id id: Long, name: String) derives EntityMeta
      def test(using DbCon[ClickHouse]): Unit =
        QueryBuilder.from[Item].upsertByPk(Item(1L, "x"))
    """)
    assert(
      errors.contains("SupportsMutations"),
      s"Expected SupportsMutations compile error but got: $errors"
    )

  test("insertReturning disallowed when EC != E on ClickHouse"):
    val errors = compileErrors("""
      import ma.chinespirit.parlance.*
      import java.util.UUID
      @Table()
      case class User(id: UUID, name: String) derives EntityMeta
      case class UserCreator(name: String) extends CreatorOf[User] derives DbCodec
      val repo = Repo[UserCreator, User, UUID]()
      def test(using DbCon[ClickHouse]): Unit =
        repo.create(UserCreator("test"))
    """)
    assert(
      errors.contains("No given instance of type ma.chinespirit.parlance.CanReturn[UserCreator, User, D] was found"),
      s"Expected CanReturn compile error but got: $errors"
    )

  val clickHouseContainer = ForAllContainerFixture(
    ClickHouseContainer
      .Def(dockerImageName = DockerImageName.parse("clickhouse/clickhouse-server:24.3.12.75"))
      .createContainer()
  )

  override def munitFixtures: Seq[AnyFixture[?]] =
    super.munitFixtures :+ clickHouseContainer

  def xa(): Transactor[ClickHouse] =
    val clickHouse = clickHouseContainer()
    val ds = ClickHouseDataSource(clickHouse.jdbcUrl)
    val tableDDLs = Vector(
      "clickhouse/car.sql",
      "clickhouse/no-id.sql",
      "clickhouse/person.sql",
      "clickhouse/big-dec.sql",
      "clickhouse/my-time.sql"
    ).map(p => Files.readString(Path.of(getClass.getResource(p).toURI)))
    Using
      .Manager(use =>
        val con = use(ds.getConnection)
        val stmt = use(con.createStatement)
        for ddl <- tableDDLs do stmt.execute(ddl)
      )
      .get
    Transactor(ClickHouse, ds)
end ClickHouseTests
