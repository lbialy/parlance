package ma.chinespirit.parlance

object WhereFrag:
  opaque type WhereFrag <: Frag = Frag
  private[parlance] inline def apply(f: Frag): WhereFrag = f
  val empty: WhereFrag = Frag("", Seq.empty, FragWriter.empty)
  extension (f: Frag) def unsafeAsWhere: WhereFrag = f

  private def combine(lhs: WhereFrag, rhs: WhereFrag, op: String): WhereFrag =
    val lEmpty = lhs.sqlString.isEmpty
    val rEmpty = rhs.sqlString.isEmpty
    if lEmpty && rEmpty then empty
    else if lEmpty then rhs
    else if rEmpty then lhs
    else
      val sql = s"(${lhs.sqlString} $op ${rhs.sqlString})"
      val params = lhs.params ++ rhs.params
      val writer: FragWriter = (ps, pos) =>
        val next = lhs.writer.write(ps, pos)
        rhs.writer.write(ps, next)
      WhereFrag(Frag(sql, params, writer))

  extension (lhs: WhereFrag)
    def &&(rhs: WhereFrag): WhereFrag = combine(lhs, rhs, "AND")
    def ||(rhs: WhereFrag): WhereFrag = combine(lhs, rhs, "OR")
end WhereFrag

export WhereFrag.WhereFrag
export WhereFrag.unsafeAsWhere
export WhereFrag.`&&`
export WhereFrag.`||`
