package com.augustnagro.magnum

import java.sql.Connection
import scala.util.Using

/** Represents a transactional [[DbCon]]
  */
class DbTx[D <: DatabaseType] private[magnum] (connection: Connection, sqlLogger: SqlLogger, databaseType: D) extends DbCon[D](connection, sqlLogger, databaseType)
