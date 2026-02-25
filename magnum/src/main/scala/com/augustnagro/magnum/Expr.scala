package com.augustnagro.magnum

object Expr:
  /** COUNT(*) */
  def count(using c: DbCodec[Long]): SelectExpr[Long] =
    SelectExpr("COUNT(*)", "count", c)

  /** COUNT(col) */
  def count[A](col: ColRef[A])(using c: DbCodec[Long]): SelectExpr[Long] =
    SelectExpr(s"COUNT(${col.queryRepr})", "count", c)

  /** COUNT(DISTINCT col) */
  def countDistinct[A](col: ColRef[A])(using c: DbCodec[Long]): SelectExpr[Long] =
    SelectExpr(s"COUNT(DISTINCT ${col.queryRepr})", "count_distinct", c)

  /** SUM(col) */
  def sum[A](col: ColRef[A])(using c: DbCodec[A]): SelectExpr[A] =
    SelectExpr(s"SUM(${col.queryRepr})", "sum", c)

  /** AVG(col) */
  def avg(col: ColRef[?])(using c: DbCodec[Double]): SelectExpr[Double] =
    SelectExpr(s"AVG(${col.queryRepr})", "avg", c)

  /** MIN(col) */
  def min[A](col: ColRef[A])(using c: DbCodec[A]): SelectExpr[A] =
    SelectExpr(s"MIN(${col.queryRepr})", "min", c)

  /** MAX(col) */
  def max[A](col: ColRef[A])(using c: DbCodec[A]): SelectExpr[A] =
    SelectExpr(s"MAX(${col.queryRepr})", "max", c)

  /** Raw SQL expression with a declared result type. */
  def raw[A](sql: String, alias: String)(using c: DbCodec[A]): SelectExpr[A] =
    SelectExpr(sql, alias, c)
