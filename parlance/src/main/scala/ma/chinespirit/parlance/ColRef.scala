package ma.chinespirit.parlance

/** Shared trait for column references, both unbound (Col) and alias-bound (BoundCol). Enables ColumnOps (===, >, <, in, etc.) to work with
  * either.
  *
  * @tparam A
  *   the Scala type of this column
  */
trait ColRef[A] extends SqlLiteral:
  def scalaName: String
  def sqlName: String
  def queryRepr: String
