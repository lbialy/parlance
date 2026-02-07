package com.augustnagro.magnum

/** A column reference bound to a table alias.
  *
  * @tparam A
  *   the Scala type of this column
  * @param col
  *   the unbound column reference
  * @param alias
  *   the table alias (e.g. "u")
  */
class BoundCol[A](val col: Col[A], val alias: String) extends ColRef[A]:
  def queryRepr: String = s"$alias.${col.sqlName}"

  def scalaName: String = col.scalaName
  def sqlName: String = col.sqlName

  override def toString: String = s"BoundCol(${col.scalaName}, $queryRepr)"
