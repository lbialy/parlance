package ma.chinespirit.parlance

import java.sql.Connection
import javax.sql.DataSource
import scala.util.Using

class Transactor[D <: DatabaseType] private (
    val databaseType: D,
    dataSource: DataSource,
    sqlLogger: SqlLogger = SqlLogger.Default,
    connectionConfig: Connection => Unit = con => ()
):
  def withSqlLogger(sqlLogger: SqlLogger): Transactor[D] =
    new Transactor(databaseType, dataSource, sqlLogger, connectionConfig)

  def withConnectionConfig(connectionConfig: Connection => Unit): Transactor[D] =
    new Transactor(databaseType, dataSource, sqlLogger, connectionConfig)

  def connect[T](f: DbCon[D] ?=> T): T =
    Using.resource(dataSource.getConnection): con =>
      connectionConfig(con)
      f(using DbCon(con, sqlLogger, databaseType))

  def transact[T](f: DbTx[D] ?=> T): T =
    Using.resource(dataSource.getConnection): con =>
      connectionConfig(con)
      con.setAutoCommit(false)
      try
        val res = f(using DbTx(con, sqlLogger, databaseType))
        con.commit()
        res
      catch
        case t =>
          try con.rollback()
          catch { case t2 => t.addSuppressed(t2) }
          throw t
end Transactor

object Transactor:

  def apply[D <: DatabaseType](
      databaseType: D,
      dataSource: DataSource,
      sqlLogger: SqlLogger,
      connectionConfig: Connection => Unit
  ): Transactor[D] =
    new Transactor(databaseType, dataSource, sqlLogger, connectionConfig)

  def apply[D <: DatabaseType](databaseType: D, dataSource: DataSource, sqlLogger: SqlLogger): Transactor[D] =
    new Transactor(databaseType, dataSource, sqlLogger, _ => ())

  def apply[D <: DatabaseType](
      databaseType: D,
      dataSource: DataSource,
      connectionConfig: Connection => Unit
  ): Transactor[D] =
    new Transactor(databaseType, dataSource, SqlLogger.Default, connectionConfig)

  def apply[D <: DatabaseType](databaseType: D, dataSource: DataSource): Transactor[D] =
    new Transactor(databaseType, dataSource, SqlLogger.Default, _ => ())

end Transactor
