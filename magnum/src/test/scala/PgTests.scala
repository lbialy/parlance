import com.augustnagro.magnum.*
import com.dimafeng.testcontainers.PostgreSQLContainer
import com.dimafeng.testcontainers.munit.fixtures.TestContainersFixtures
import munit.{AnyFixture, FunSuite, Location, Tag}
import org.postgresql.ds.PGSimpleDataSource
import org.testcontainers.utility.DockerImageName
import shared.*

import java.nio.file.{Files, Path}
import scala.util.Using
import scala.util.Using.Manager

class PgTests extends FunSuite, TestContainersFixtures:

  override def munitTestTransforms: List[TestTransform] =
    super.munitTestTransforms :+ new TestTransform(
      "Slow",
      test => test.withTags(test.tags + new Tag("Slow"))
    )

  sharedTests(this, xa)
  sharedMutationTests(this, xa)
  sharedPartialJoinTests(this, xa)
  sharedReturningMutationTests(this, xa)
  sharedMultiColReturningTests(this, xa)
  entityCreatorReturningTests(this, xa)

  val pgContainer = ForAllContainerFixture(
    PostgreSQLContainer
      .Def(dockerImageName = DockerImageName.parse("postgres:17.0"))
      .createContainer()
  )

  override def munitFixtures: Seq[AnyFixture[?]] =
    super.munitFixtures :+ pgContainer

  def xa(): Transactor[Postgres.type] =
    val ds = PGSimpleDataSource()
    val pg = pgContainer()
    ds.setUrl(pg.jdbcUrl)
    ds.setUser(pg.username)
    ds.setPassword(pg.password)
    val tableDDLs = Vector(
      "/pg/car.sql",
      "/pg/person.sql",
      "/pg/my-user.sql",
      "/pg/no-id.sql",
      "/pg/big-dec.sql",
      "/pg/my-time.sql"
    ).map(p => Files.readString(Path.of(getClass.getResource(p).toURI)))

    Manager(use =>
      val con = use(ds.getConnection)
      val stmt = use(con.createStatement)
      for ddl <- tableDDLs do stmt.execute(ddl)
    ).get
    Transactor(Postgres, ds)
  end xa
end PgTests
