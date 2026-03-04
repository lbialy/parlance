package com.augustnagro.magnum

import java.sql.Connection

/** Simple wrapper around java.sql.Connection. See `com.augustnagro.magnum.connect` and `transact`
  */
class DbCon[D <: DatabaseType] private[magnum] (connection: Connection, sqlLogger: SqlLogger, databaseType: D)
    extends DbSession[D](connection, sqlLogger, databaseType)
