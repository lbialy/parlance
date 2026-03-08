package ma.chinespirit.parlance

import java.sql.Connection

/** Simple wrapper around java.sql.Connection. See `ma.chinespirit.parlance.connect` and `transact`
  */
class DbCon[D <: DatabaseType] private[parlance] (connection: Connection, sqlLogger: SqlLogger, databaseType: D)
    extends DbSession[D](connection, sqlLogger, databaseType)
