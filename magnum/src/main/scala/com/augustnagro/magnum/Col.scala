package com.augustnagro.magnum

/** Typed column reference. Unbound (no alias).
  *
  * @tparam A
  *   the Scala type of this column
  * @param scalaName
  *   the Scala field name (e.g. "firstName")
  * @param sqlName
  *   the SQL column name (e.g. "first_name")
  */
class Col[A](val scalaName: String, val sqlName: String) extends SqlLiteral:
  def queryRepr: String = sqlName

  /** Bind this column to a table alias, producing a BoundCol. */
  def bound(alias: String): BoundCol[A] = BoundCol(this, alias)

  override def toString: String = s"Col($scalaName, $sqlName)"
