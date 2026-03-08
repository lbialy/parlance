package ma.chinespirit.parlance

class CountExpr(
    subquerySql: String,
    condParams: Seq[Any] = Seq.empty,
    condWriter: FragWriter = FragWriter.empty
):
  def >=(n: Long): WhereFrag = mkFrag(">=", n)
  def >(n: Long): WhereFrag = mkFrag(">", n)
  def <=(n: Long): WhereFrag = mkFrag("<=", n)
  def <(n: Long): WhereFrag = mkFrag("<", n)
  def ===(n: Long): WhereFrag = mkFrag("=", n)
  def !==(n: Long): WhereFrag = mkFrag("<>", n)

  private def mkFrag(operator: String, n: Long): WhereFrag =
    val writer: FragWriter = (ps, pos) =>
      val next = condWriter.write(ps, pos)
      ps.setLong(next, n)
      next + 1
    WhereFrag(Frag(s"($subquerySql) $operator ?", condParams :+ n, writer))

object CountExpr:
  /** Create a CountExpr from a Frag (for scope-aware count queries that carry parameters). */
  def fromFrag(baseFrag: Frag): CountExpr =
    CountExpr(baseFrag.sqlString, baseFrag.params, baseFrag.writer)
