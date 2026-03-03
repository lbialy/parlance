package com.augustnagro.magnum

trait DatabaseType:
  def renderLimitOffset(limit: Option[Int], offset: Option[Long]): String

trait SupportsRowLocks extends DatabaseType
trait SupportsForShare extends SupportsRowLocks
trait SupportsILike extends DatabaseType
trait SupportsArrayTypes extends DatabaseType
trait SupportsReturning extends DatabaseType

object Postgres
    extends DatabaseType,
      SupportsForShare,
      SupportsILike,
      SupportsReturning,
      SupportsArrayTypes:
  def renderLimitOffset(limit: Option[Int], offset: Option[Long]): String =
    val limitSql = limit.fold("")(n => s" LIMIT $n")
    val offsetSql = offset.fold("")(n => s" OFFSET $n")
    limitSql + offsetSql

object MySQL extends DatabaseType, SupportsRowLocks:
  def renderLimitOffset(limit: Option[Int], offset: Option[Long]): String =
    (limit, offset) match
      case (Some(l), Some(o)) => s" LIMIT $o, $l"
      case (Some(l), None)    => s" LIMIT $l"
      case (None, Some(o))    => s" LIMIT $o, 18446744073709551615"
      case (None, None)       => ""

object SQLite extends DatabaseType:
  def renderLimitOffset(limit: Option[Int], offset: Option[Long]): String =
    (limit, offset) match
      case (Some(l), Some(o)) => s" LIMIT $l OFFSET $o"
      case (Some(l), None)    => s" LIMIT $l"
      case (None, Some(o))    => s" LIMIT -1 OFFSET $o"
      case (None, None)       => ""

object H2
    extends DatabaseType,
      SupportsForShare,
      SupportsILike,
      SupportsArrayTypes,
      SupportsReturning:
  def renderLimitOffset(limit: Option[Int], offset: Option[Long]): String =
    val limitSql = limit.fold("")(n => s" LIMIT $n")
    val offsetSql = offset.fold("")(n => s" OFFSET $n")
    limitSql + offsetSql

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

object ClickHouse extends DatabaseType:
  def renderLimitOffset(limit: Option[Int], offset: Option[Long]): String =
    val limitSql = limit.fold("")(n => s" LIMIT $n")
    val offsetSql = offset.fold("")(n => s" OFFSET $n")
    limitSql + offsetSql
