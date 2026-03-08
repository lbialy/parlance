import ma.chinespirit.parlance.{Postgres, Transactor}
import com.dimafeng.testcontainers.PostgreSQLContainer
import com.dimafeng.testcontainers.munit.fixtures.TestContainersFixtures
import munit.{AnyFixture, FunSuite, Tag}
import org.postgresql.ds.PGSimpleDataSource
import org.testcontainers.utility.DockerImageName

import java.nio.file.{Files, Path}
import scala.util.Using.Manager

trait QbPgTestBase extends QbTestBase[Postgres], TestContainersFixtures:

  override def munitTestTransforms: List[TestTransform] =
    super.munitTestTransforms :+ new TestTransform(
      "Slow",
      test => test.withTags(test.tags + new Tag("Slow"))
    )

  def pgDdls: Seq[String]

  def databaseType: Postgres = Postgres

  val pgContainer = ForAllContainerFixture(
    PostgreSQLContainer
      .Def(dockerImageName = DockerImageName.parse("postgres:17.0"))
      .createContainer()
  )

  override def munitFixtures: Seq[AnyFixture[?]] =
    super.munitFixtures :+ pgContainer

  def xa(): Transactor[Postgres] =
    val ds = PGSimpleDataSource()
    val pg = pgContainer()
    ds.setUrl(pg.jdbcUrl)
    ds.setUser(pg.username)
    ds.setPassword(pg.password)
    val tableDDLs = pgDdls.map(p =>
      Files.readString(Path.of(getClass.getResource(p).toURI))
    )
    Manager(use =>
      val con = use(ds.getConnection)
      val stmt = use(con.createStatement)
      for ddl <- tableDDLs do stmt.execute(ddl)
    ).get
    Transactor(Postgres, ds)

end QbPgTestBase
