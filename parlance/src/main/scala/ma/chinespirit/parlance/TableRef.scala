package ma.chinespirit.parlance

/** Identifies a table in a query context.
  *
  * @param tableName
  *   the SQL table name
  * @param alias
  *   the alias assigned in this query (e.g. "user_0")
  * @param entityType
  *   the Scala entity class name (for debugging/error messages)
  */
case class TableRef(tableName: String, alias: String, entityType: String)
