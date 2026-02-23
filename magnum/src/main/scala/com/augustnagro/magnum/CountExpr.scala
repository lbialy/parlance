package com.augustnagro.magnum

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
