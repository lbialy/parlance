package com.augustnagro.magnum

class CountExpr(
    subquerySql: String,
    condParams: Seq[Any] = Seq.empty,
    condWriter: FragWriter = FragWriter.empty
):
  def >=(n: Long): Frag = mkFrag(">=", n)
  def >(n: Long): Frag = mkFrag(">", n)
  def <=(n: Long): Frag = mkFrag("<=", n)
  def <(n: Long): Frag = mkFrag("<", n)
  def ===(n: Long): Frag = mkFrag("=", n)
  def !==(n: Long): Frag = mkFrag("<>", n)

  private def mkFrag(operator: String, n: Long): Frag =
    val writer: FragWriter = (ps, pos) =>
      val next = condWriter.write(ps, pos)
      ps.setLong(next, n)
      next + 1
    Frag(s"($subquerySql) $operator ?", condParams :+ n, writer)
