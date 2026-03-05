import com.augustnagro.magnum.*
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

  test("insertReturning disallowed when EC != E on ClickHouse"):
    val errors = compileErrors("""
      import com.augustnagro.magnum.*
      import java.util.UUID
      case class UserCreator(name: String) derives DbCodec
      @Table()
      case class User(id: UUID, name: String) derives EntityMeta
      val repo = Repo[UserCreator, User, UUID]()
      def test(using DbCon[ClickHouse.type]): Unit =
        repo.insertReturning(UserCreator("test"))
    """)
    assert(
      errors.contains("No given instance of type com.augustnagro.magnum.CanReturn[UserCreator, User, D] was found"),
      s"Expected CanReturn compile error but got: $errors"
    )

  val clickHouseContainer = ForAllContainerFixture(
    ClickHouseContainer
      .Def(dockerImageName = DockerImageName.parse("clickhouse/clickhouse-server:24.3.12.75"))
      .createContainer()
  )

  override def munitFixtures: Seq[AnyFixture[?]] =
    super.munitFixtures :+ clickHouseContainer

  def xa(): Transactor[ClickHouse.type] =
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
