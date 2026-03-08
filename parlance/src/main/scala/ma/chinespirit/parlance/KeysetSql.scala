package ma.chinespirit.parlance

import java.sql.PreparedStatement

object KeysetSql:

  def buildKeysetFrag(
      entries: Vector[KeysetColumnEntry],
      keyValues: Vector[Any]
  ): WhereFrag =
    val n = entries.size
    val sb = new StringBuilder("(")
    val allParams = Vector.newBuilder[Any]
    val writers = Vector.newBuilder[(DbCodec[Any], Any)]

    for i <- 0 until n do
      if i > 0 then sb.append(" OR ")
      sb.append("(")
      // Equality prefix: columns 0 .. i-1
      for j <- 0 until i do
        sb.append(entries(j).colRef.queryRepr)
        sb.append(" = ?")
        sb.append(" AND ")
        allParams += keyValues(j)
        writers += ((entries(j).codec.asInstanceOf[DbCodec[Any]], keyValues(j)))
      // Comparison on column i
      val entry = entries(i)
      val cmp = if entry.sortOrder == SortOrder.Desc then "<" else ">"
      sb.append(entry.colRef.queryRepr)
      sb.append(" ")
      sb.append(cmp)
      sb.append(" ?")
      allParams += keyValues(i)
      writers += ((entry.codec.asInstanceOf[DbCodec[Any]], keyValues(i)))
      sb.append(")")
    end for

    sb.append(")")

    val collectedWriters = writers.result()
    val writer: FragWriter = (ps, pos) =>
      var currentPos = pos
      for (codec, value) <- collectedWriters do
        codec.writeSingle(value, ps, currentPos)
        currentPos += codec.cols.length
      currentPos

    WhereFrag(Frag(sb.result(), allParams.result(), writer))
  end buildKeysetFrag
end KeysetSql
