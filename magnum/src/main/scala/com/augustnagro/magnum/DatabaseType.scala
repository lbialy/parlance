package com.augustnagro.magnum

import java.sql.Types

trait DatabaseType:
  def renderLimitOffset(limit: Option[Int], offset: Option[Long]): String

  /** SQL to truncate a table. SQLite uses DELETE FROM since it has no TRUNCATE. */
  def renderTruncate(tableName: String): String =
    s"TRUNCATE TABLE $tableName"

  /** Whether this database supports INSERT ... RETURNING via getGeneratedKeys. */
  def supportsInsertReturning: Boolean = false
end DatabaseType

trait SupportsMutations extends DatabaseType:
  /** SQL for MERGE/upsert by primary key. Entity type E columns. */
  def renderUpsertByPk(
      tableName: String,
      allCols: IArray[String],
      allColsQueryRepr: String,
      pkCol: String
  ): String

  /** SQL for upsert by PK with extra SET clauses and WHERE conditions from mutation hooks. */
  def renderUpsertByPkWithHooks(
      tableName: String,
      allCols: IArray[String],
      allColsQueryRepr: String,
      pkCol: String,
      extraSetClauses: Vector[SetClause],
      conditions: Vector[WhereFrag]
  ): String

trait SupportsRowLocks extends DatabaseType
trait SupportsForShare extends SupportsRowLocks
trait SupportsILike extends DatabaseType
trait SupportsArrayTypes extends DatabaseType:
  /** Map a JDBC type code to the SQL type name for Connection.createArrayOf.
    * Returns None if the type cannot be used in arrays, in which case
    * findAllById falls back to IN expansion.
    */
  def arrayTypeName(jdbcType: Int): Option[String]
trait SupportsReturning extends DatabaseType
trait SupportsPartialJoins extends DatabaseType
trait SupportsMultiColumnReturningKeys extends SupportsReturning

sealed trait Postgres extends DatabaseType, SupportsMutations, SupportsForShare, SupportsILike, SupportsReturning, SupportsArrayTypes, SupportsPartialJoins, SupportsMultiColumnReturningKeys
object Postgres extends Postgres:
  def renderLimitOffset(limit: Option[Int], offset: Option[Long]): String =
    val limitSql = limit.fold("")(n => s" LIMIT $n")
    val offsetSql = offset.fold("")(n => s" OFFSET $n")
    limitSql + offsetSql

  def renderUpsertByPk(
      tableName: String,
      allCols: IArray[String],
      allColsQueryRepr: String,
      pkCol: String
  ): String =
    val colsList = allCols.mkString("(", ", ", ")")
    val updateSet = allCols.filter(_ != pkCol).map(c => s"$c = EXCLUDED.$c").mkString(", ")
    s"INSERT INTO $tableName $colsList VALUES ($allColsQueryRepr) ON CONFLICT ($pkCol) DO UPDATE SET $updateSet"

  def renderUpsertByPkWithHooks(
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

  def arrayTypeName(jdbcType: Int): Option[String] = jdbcType match
    case Types.BIGINT                    => Some("bigint")
    case Types.INTEGER                   => Some("integer")
    case Types.SMALLINT                  => Some("smallint")
    case Types.TINYINT                   => Some("smallint")
    case Types.VARCHAR                   => Some("varchar")
    case Types.BOOLEAN                   => Some("boolean")
    case Types.DOUBLE                    => Some("float8")
    case Types.REAL                      => Some("float4")
    case Types.NUMERIC                   => Some("numeric")
    case Types.DATE                      => Some("date")
    case Types.TIMESTAMP                 => Some("timestamp")
    case Types.TIMESTAMP_WITH_TIMEZONE   => Some("timestamptz")
    case _                               => None
end Postgres

sealed trait MySQL extends DatabaseType, SupportsMutations, SupportsRowLocks, SupportsPartialJoins
object MySQL extends MySQL:
  def renderLimitOffset(limit: Option[Int], offset: Option[Long]): String =
    (limit, offset) match
      case (Some(l), Some(o)) => s" LIMIT $o, $l"
      case (Some(l), None)    => s" LIMIT $l"
      case (None, Some(o))    => s" LIMIT $o, 18446744073709551615"
      case (None, None)       => ""

  def renderUpsertByPk(
      tableName: String,
      allCols: IArray[String],
      allColsQueryRepr: String,
      pkCol: String
  ): String =
    val colsList = allCols.mkString("(", ", ", ")")
    val updateSet = allCols.filter(_ != pkCol).map(c => s"$c = VALUES($c)").mkString(", ")
    s"INSERT INTO $tableName $colsList VALUES ($allColsQueryRepr) ON DUPLICATE KEY UPDATE $updateSet"

  def renderUpsertByPkWithHooks(
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
end MySQL

sealed trait SQLite extends DatabaseType, SupportsMutations, SupportsPartialJoins
object SQLite extends SQLite:
  def renderLimitOffset(limit: Option[Int], offset: Option[Long]): String =
    (limit, offset) match
      case (Some(l), Some(o)) => s" LIMIT $l OFFSET $o"
      case (Some(l), None)    => s" LIMIT $l"
      case (None, Some(o))    => s" LIMIT -1 OFFSET $o"
      case (None, None)       => ""

  override def renderTruncate(tableName: String): String =
    s"DELETE FROM $tableName"

  def renderUpsertByPk(
      tableName: String,
      allCols: IArray[String],
      allColsQueryRepr: String,
      pkCol: String
  ): String =
    val colsList = allCols.mkString("(", ", ", ")")
    val updateSet = allCols.filter(_ != pkCol).map(c => s"$c = EXCLUDED.$c").mkString(", ")
    s"INSERT INTO $tableName $colsList VALUES ($allColsQueryRepr) ON CONFLICT ($pkCol) DO UPDATE SET $updateSet"

  def renderUpsertByPkWithHooks(
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
end SQLite

sealed trait H2 extends DatabaseType, SupportsMutations, SupportsForShare, SupportsILike, SupportsArrayTypes, SupportsReturning, SupportsPartialJoins, SupportsMultiColumnReturningKeys
object H2 extends H2:
  def renderLimitOffset(limit: Option[Int], offset: Option[Long]): String =
    val limitSql = limit.fold("")(n => s" LIMIT $n")
    val offsetSql = offset.fold("")(n => s" OFFSET $n")
    limitSql + offsetSql

  def renderUpsertByPk(
      tableName: String,
      allCols: IArray[String],
      allColsQueryRepr: String,
      pkCol: String
  ): String =
    val colsList = allCols.mkString("(", ", ", ")")
    s"MERGE INTO $tableName $colsList KEY ($pkCol) VALUES ($allColsQueryRepr)"

  def renderUpsertByPkWithHooks(
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

  def arrayTypeName(jdbcType: Int): Option[String] = jdbcType match
    case Types.BIGINT                    => Some("BIGINT")
    case Types.INTEGER                   => Some("INTEGER")
    case Types.SMALLINT                  => Some("SMALLINT")
    case Types.TINYINT                   => Some("TINYINT")
    case Types.VARCHAR                   => Some("VARCHAR")
    case Types.BOOLEAN                   => Some("BOOLEAN")
    case Types.DOUBLE                    => Some("DOUBLE")
    case Types.REAL                      => Some("REAL")
    case Types.NUMERIC                   => Some("NUMERIC")
    case Types.DATE                      => Some("DATE")
    case Types.TIMESTAMP                 => Some("TIMESTAMP")
    case Types.TIMESTAMP_WITH_TIMEZONE   => Some("TIMESTAMP WITH TIME ZONE")
    case _                               => None
end H2

sealed trait Oracle extends DatabaseType, SupportsMutations, SupportsRowLocks, SupportsReturning, SupportsPartialJoins
object Oracle extends Oracle:
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

  def renderUpsertByPk(
      tableName: String,
      allCols: IArray[String],
      allColsQueryRepr: String,
      pkCol: String
  ): String =
    val colsList = allCols.mkString(", ")
    val valsList = allCols.map(c => s"src.$c").mkString(", ")
    val srcCols = allCols.map(c => s"? AS $c").mkString(", ")
    val updateSet = allCols.filter(_ != pkCol).map(c => s"tgt.$c = src.$c").mkString(", ")
    s"MERGE INTO $tableName tgt USING (SELECT $srcCols FROM DUAL) src ON (tgt.$pkCol = src.$pkCol) " +
      s"WHEN MATCHED THEN UPDATE SET $updateSet " +
      s"WHEN NOT MATCHED THEN INSERT ($colsList) VALUES ($valsList)"

  def renderUpsertByPkWithHooks(
      tableName: String,
      allCols: IArray[String],
      allColsQueryRepr: String,
      pkCol: String,
      extraSetClauses: Vector[SetClause],
      conditions: Vector[WhereFrag]
  ): String =
    val colsList = allCols.mkString(", ")
    val valsList = allCols.map(c => s"src.$c").mkString(", ")
    val srcCols = allCols.map(c => s"? AS $c").mkString(", ")
    val updateSet = allCols.filter(_ != pkCol).map(c => s"tgt.$c = src.$c").mkString(", ")
    val extraSetSql = if extraSetClauses.isEmpty then "" else extraSetClauses.map(c => s"tgt.${c.sqlString}").mkString(", ", ", ", "")
    val wherePart = if conditions.isEmpty then "" else s" WHERE ${conditions.map(_.sqlString).mkString(" AND ")}"
    s"MERGE INTO $tableName tgt USING (SELECT $srcCols FROM DUAL) src ON (tgt.$pkCol = src.$pkCol) " +
      s"WHEN MATCHED THEN UPDATE SET $updateSet$extraSetSql$wherePart " +
      s"WHEN NOT MATCHED THEN INSERT ($colsList) VALUES ($valsList)"

sealed trait ClickHouse extends DatabaseType
object ClickHouse extends ClickHouse:
  def renderLimitOffset(limit: Option[Int], offset: Option[Long]): String =
    val limitSql = limit.fold("")(n => s" LIMIT $n")
    val offsetSql = offset.fold("")(n => s" OFFSET $n")
    limitSql + offsetSql
