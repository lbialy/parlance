import com.augustnagro.magnum.{DatabaseType, H2, Transactor}
import munit.{FunSuite, Tag}
import org.h2.jdbcx.JdbcDataSource

import java.nio.file.{Files, Path}
import scala.util.Using

trait QbTestBase[D <: DatabaseType] extends FunSuite:

  override def munitTestTransforms: List[TestTransform] =
    super.munitTestTransforms :+ new TestTransform(
      "QB",
      test => test.withTags(test.tags + new Tag("QB"))
    )

  def xa(): Transactor[D]
  def databaseType: D

end QbTestBase

trait QbH2TestBase extends QbTestBase[H2]:

  lazy val h2DbPath = Files.createTempDirectory(null).toAbsolutePath

  def h2Ddls: Seq[String]

  def databaseType: H2 = H2

  def xa(): Transactor[H2] =
    val ds = JdbcDataSource()
    ds.setURL("jdbc:h2:" + h2DbPath)
    ds.setUser("sa")
    ds.setPassword("")
    Using.Manager: use =>
      val con = use(ds.getConnection)
      val stmt = use(con.createStatement)
      for ddlResource <- h2Ddls do
        val ddl = Files.readString(
          Path.of(getClass.getResource(ddlResource).toURI)
        )
        stmt.execute(ddl)

    Transactor(H2, ds)

end QbH2TestBase
