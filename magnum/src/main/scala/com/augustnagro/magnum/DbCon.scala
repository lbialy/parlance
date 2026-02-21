package com.augustnagro.magnum

import java.sql.Connection

/** Simple wrapper around java.sql.Connection. See `com.augustnagro.magnum.connect` and `transact`
  */
class DbCon private[magnum] (connection: Connection, sqlLogger: SqlLogger) extends DbSession(connection, sqlLogger)
