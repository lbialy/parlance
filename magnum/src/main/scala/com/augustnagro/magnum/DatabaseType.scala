package com.augustnagro.magnum

trait DatabaseType:
  def renderLimitOffset(limit: Option[Int], offset: Option[Long]): String

  /** SQL to truncate a table. SQLite uses DELETE FROM since it has no TRUNCATE. */
  def renderTruncate(tableName: String): String =
    s"TRUNCATE TABLE $tableName"

  /** SQL for MERGE/upsert by primary key. Entity type E columns. */
  def renderUpsertByPk(
      tableName: String,
      allCols: IArray[String],
      allColsQueryRepr: String,
      pkCol: String
  ): String =
    throw UnsupportedOperationException(
      s"upsertByPk is not supported for ${getClass.getSimpleName}"
    )

  /** Whether this database supports INSERT ... RETURNING via getGeneratedKeys. */
  def supportsInsertReturning: Boolean = false

  /** Whether this database supports upsert operations. */
  def supportsUpsert: Boolean = false

  /** Whether this database supports ON CONFLICT / ON DUPLICATE KEY handling. */
  def supportsConflictHandling: Boolean = false

  /** SQL for upsert by PK with extra SET clauses and WHERE conditions from mutation hooks. */
  def renderUpsertByPkWithHooks(
      tableName: String,
      allCols: IArray[String],
      allColsQueryRepr: String,
      pkCol: String,
      extraSetClauses: Vector[SetClause],
      conditions: Vector[WhereFrag]
  ): String =
    throw UnsupportedOperationException(
      s"upsertByPkWithHooks is not supported for ${getClass.getSimpleName}"
    )
end DatabaseType

trait SupportsRowLocks extends DatabaseType
trait SupportsForShare extends SupportsRowLocks
trait SupportsILike extends DatabaseType
trait SupportsArrayTypes extends DatabaseType
trait SupportsReturning extends DatabaseType

object Postgres extends DatabaseType, SupportsForShare, SupportsILike, SupportsReturning, SupportsArrayTypes:
  def renderLimitOffset(limit: Option[Int], offset: Option[Long]): String =
    val limitSql = limit.fold("")(n => s" LIMIT $n")
    val offsetSql = offset.fold("")(n => s" OFFSET $n")
    limitSql + offsetSql

  override def renderUpsertByPk(
      tableName: String,
      allCols: IArray[String],
      allColsQueryRepr: String,
      pkCol: String
  ): String =
    val colsList = allCols.mkString("(", ", ", ")")
    val updateSet = allCols.filter(_ != pkCol).map(c => s"$c = EXCLUDED.$c").mkString(", ")
    s"INSERT INTO $tableName $colsList VALUES ($allColsQueryRepr) ON CONFLICT ($pkCol) DO UPDATE SET $updateSet"

  override def renderUpsertByPkWithHooks(
      tableName: String,
      allCols: IArray[String],
      allColsQueryRepr: String,
      pkCol: String,
      extraSetClauses: Vector[SetClause],
      conditions: Vector[WhereFrag]
  ): String =
    val colsList = allCols.mkString("(", ", ", ")")
    val updateSet = allCols.filter(_ != pkCol).map(c => s"$c = EXCLUDED.$c").mkString(", ")
    val extraSetSql = if extraSetClauses.isEmpty then "" else extraSetClauses.map(_.sqlString).mkString(", ", ", ", "")
    val wherePart = if conditions.isEmpty then "" else s" WHERE ${conditions.map(_.sqlString).mkString(" AND ")}"
    s"INSERT INTO $tableName $colsList VALUES ($allColsQueryRepr) ON CONFLICT ($pkCol) DO UPDATE SET $updateSet$extraSetSql$wherePart"

  override val supportsInsertReturning: Boolean = true
  override val supportsUpsert: Boolean = true
  override val supportsConflictHandling: Boolean = true
end Postgres

object MySQL extends DatabaseType, SupportsRowLocks:
  def renderLimitOffset(limit: Option[Int], offset: Option[Long]): String =
    (limit, offset) match
      case (Some(l), Some(o)) => s" LIMIT $o, $l"
      case (Some(l), None)    => s" LIMIT $l"
      case (None, Some(o))    => s" LIMIT $o, 18446744073709551615"
      case (None, None)       => ""

  override def renderUpsertByPk(
      tableName: String,
      allCols: IArray[String],
      allColsQueryRepr: String,
      pkCol: String
  ): String =
    val colsList = allCols.mkString("(", ", ", ")")
    val updateSet = allCols.filter(_ != pkCol).map(c => s"$c = VALUES($c)").mkString(", ")
    s"INSERT INTO $tableName $colsList VALUES ($allColsQueryRepr) ON DUPLICATE KEY UPDATE $updateSet"

  override def renderUpsertByPkWithHooks(
      tableName: String,
      allCols: IArray[String],
      allColsQueryRepr: String,
      pkCol: String,
      extraSetClauses: Vector[SetClause],
      conditions: Vector[WhereFrag]
  ): String =
    val colsList = allCols.mkString("(", ", ", ")")
    val updateSet = allCols.filter(_ != pkCol).map(c => s"$c = VALUES($c)").mkString(", ")
    val extraSetSql = if extraSetClauses.isEmpty then "" else extraSetClauses.map(_.sqlString).mkString(", ", ", ", "")
    // MySQL does not support WHERE on ON DUPLICATE KEY UPDATE
    s"INSERT INTO $tableName $colsList VALUES ($allColsQueryRepr) ON DUPLICATE KEY UPDATE $updateSet$extraSetSql"

  override val supportsUpsert: Boolean = true
  override val supportsConflictHandling: Boolean = true
end MySQL

object SQLite extends DatabaseType:
  def renderLimitOffset(limit: Option[Int], offset: Option[Long]): String =
    (limit, offset) match
      case (Some(l), Some(o)) => s" LIMIT $l OFFSET $o"
      case (Some(l), None)    => s" LIMIT $l"
      case (None, Some(o))    => s" LIMIT -1 OFFSET $o"
      case (None, None)       => ""

  override def renderTruncate(tableName: String): String =
    s"DELETE FROM $tableName"

  override def renderUpsertByPk(
      tableName: String,
      allCols: IArray[String],
      allColsQueryRepr: String,
      pkCol: String
  ): String =
    val colsList = allCols.mkString("(", ", ", ")")
    val updateSet = allCols.filter(_ != pkCol).map(c => s"$c = EXCLUDED.$c").mkString(", ")
    s"INSERT INTO $tableName $colsList VALUES ($allColsQueryRepr) ON CONFLICT ($pkCol) DO UPDATE SET $updateSet"

  override def renderUpsertByPkWithHooks(
      tableName: String,
      allCols: IArray[String],
      allColsQueryRepr: String,
      pkCol: String,
      extraSetClauses: Vector[SetClause],
      conditions: Vector[WhereFrag]
  ): String =
    val colsList = allCols.mkString("(", ", ", ")")
    val updateSet = allCols.filter(_ != pkCol).map(c => s"$c = EXCLUDED.$c").mkString(", ")
    val extraSetSql = if extraSetClauses.isEmpty then "" else extraSetClauses.map(_.sqlString).mkString(", ", ", ", "")
    val wherePart = if conditions.isEmpty then "" else s" WHERE ${conditions.map(_.sqlString).mkString(" AND ")}"
    s"INSERT INTO $tableName $colsList VALUES ($allColsQueryRepr) ON CONFLICT ($pkCol) DO UPDATE SET $updateSet$extraSetSql$wherePart"

  override val supportsUpsert: Boolean = true
  override val supportsConflictHandling: Boolean = true
end SQLite

object H2 extends DatabaseType, SupportsForShare, SupportsILike, SupportsArrayTypes, SupportsReturning:
  def renderLimitOffset(limit: Option[Int], offset: Option[Long]): String =
    val limitSql = limit.fold("")(n => s" LIMIT $n")
    val offsetSql = offset.fold("")(n => s" OFFSET $n")
    limitSql + offsetSql

  override def renderUpsertByPk(
      tableName: String,
      allCols: IArray[String],
      allColsQueryRepr: String,
      pkCol: String
  ): String =
    val colsList = allCols.mkString("(", ", ", ")")
    s"MERGE INTO $tableName $colsList KEY ($pkCol) VALUES ($allColsQueryRepr)"

  override def renderUpsertByPkWithHooks(
      tableName: String,
      allCols: IArray[String],
      allColsQueryRepr: String,
      pkCol: String,
      extraSetClauses: Vector[SetClause],
      conditions: Vector[WhereFrag]
  ): String =
    if extraSetClauses.isEmpty && conditions.isEmpty then
      // Use MERGE syntax when no hooks/conditions — H2 doesn't support ON CONFLICT
      renderUpsertByPk(tableName, allCols, allColsQueryRepr, pkCol)
    else
      // Fall back to ON CONFLICT syntax for hook-aware upserts (requires H2 2.x)
      val colsList = allCols.mkString("(", ", ", ")")
      val updateSet = allCols.filter(_ != pkCol).map(c => s"$c = EXCLUDED.$c").mkString(", ")
      val extraSetSql = extraSetClauses.map(_.sqlString).mkString(", ", ", ", "")
      val wherePart = if conditions.isEmpty then "" else s" WHERE ${conditions.map(_.sqlString).mkString(" AND ")}"
      s"INSERT INTO $tableName $colsList VALUES ($allColsQueryRepr) ON CONFLICT ($pkCol) DO UPDATE SET $updateSet$extraSetSql$wherePart"

  override val supportsInsertReturning: Boolean = true
  override val supportsUpsert: Boolean = true
  override val supportsConflictHandling: Boolean = true
end H2

object Oracle extends DatabaseType, SupportsRowLocks, SupportsReturning:
  def renderLimitOffset(limit: Option[Int], offset: Option[Long]): String =
    (limit, offset) match
      case (Some(l), Some(o)) =>
        s" OFFSET $o ROWS FETCH NEXT $l ROWS ONLY"
      case (Some(l), None) =>
        s" FETCH FIRST $l ROWS ONLY"
      case (None, Some(o)) =>
        s" OFFSET $o ROWS"
      case (None, None) => ""

  override val supportsInsertReturning: Boolean = true

object ClickHouse extends DatabaseType:
  def renderLimitOffset(limit: Option[Int], offset: Option[Long]): String =
    val limitSql = limit.fold("")(n => s" LIMIT $n")
    val offsetSql = offset.fold("")(n => s" OFFSET $n")
    limitSql + offsetSql
