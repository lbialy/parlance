package ma.chinespirit.parlance

import java.sql.Connection
import scala.util.Using

/** Represents a transactional [[DbCon]]
  */
class DbTx[D <: DatabaseType] private[parlance] (connection: Connection, sqlLogger: SqlLogger, databaseType: D)
    extends DbCon[D](connection, sqlLogger, databaseType)
