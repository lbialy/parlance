package com.augustnagro.magnum

import java.sql.Connection
import scala.collection.mutable

class DbSession private[magnum] (
    val connection: Connection,
    val sqlLogger: SqlLogger
):
  private[magnum] val identityMap: mutable.HashMap[(String, Any), Any] =
    mutable.HashMap.empty

  /** Track an entity. First-load-wins: re-fetching does NOT overwrite. */
  private[magnum] def trackLoaded(
      tableName: String,
      pkValue: Any,
      entity: Any
  ): Unit =
    identityMap.getOrElseUpdate((tableName, pkValue), entity)

  /** Look up the original snapshot. */
  private[magnum] def getOriginal(
      tableName: String,
      pkValue: Any
  ): Option[Any] =
    identityMap.get((tableName, pkValue))
end DbSession
