package ma.chinespirit.parlance

/** ADT for SQL predicates with correct parenthesization. */
enum Predicate:
  case Leaf(frag: WhereFrag)
  case And(children: Vector[Predicate])
  case Or(children: Vector[Predicate])

  /** Render this predicate tree to a WhereFrag with correct parenthesization. */
  def toFrag: WhereFrag = this match
    case Leaf(frag) => frag

    case And(children) =>
      val nonEmpty = children.map(_.toFrag).filter(_.sqlString.nonEmpty)
      nonEmpty.size match
        case 0 => WhereFrag.empty
        case 1 => nonEmpty.head
        case _ => Predicate.joinFrags(nonEmpty, " AND ")

    case Or(children) =>
      val nonEmpty = children.map(_.toFrag).filter(_.sqlString.nonEmpty)
      nonEmpty.size match
        case 0 => WhereFrag.empty
        case 1 => nonEmpty.head
        case _ => Predicate.joinFrags(nonEmpty, " OR ")

  /** Rewrite all SQL fragments, replacing `tableName.` with `alias.` */
  def rewriteAlias(tableName: String, alias: String): Predicate = this match
    case Leaf(frag) =>
      val newSql = frag.sqlString.replace(tableName + ".", alias + ".")
      Leaf(WhereFrag(Frag(newSql, frag.params, frag.writer)))
    case And(children) => And(children.map(_.rewriteAlias(tableName, alias)))
    case Or(children)  => Or(children.map(_.rewriteAlias(tableName, alias)))

end Predicate

object Predicate:
  val empty: Predicate = And(Vector.empty)

  private def joinFrags(frags: Vector[WhereFrag], separator: String): WhereFrag =
    val sb = new StringBuilder("(")
    val allParams = Vector.newBuilder[Any]
    val writers = Vector.newBuilder[FragWriter]

    var first = true
    for f <- frags do
      if !first then sb.append(separator)
      first = false
      sb.append(f.sqlString)
      allParams ++= f.params
      writers += f.writer

    sb.append(")")

    val combinedWriter: FragWriter = (ps, pos) =>
      var currentPos = pos
      for w <- writers.result() do currentPos = w.write(ps, currentPos)
      currentPos

    WhereFrag(Frag(sb.result(), allParams.result(), combinedWriter))
  end joinFrags
end Predicate
