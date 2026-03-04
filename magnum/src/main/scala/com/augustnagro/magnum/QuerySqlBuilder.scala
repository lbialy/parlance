package com.augustnagro.magnum

private[magnum] object QuerySqlBuilder:

  def addAnd(existing: Option[Predicate], pred: Predicate): Option[Predicate] =
    Some(existing match
      case None                          => pred
      case Some(Predicate.And(children)) => Predicate.And(children :+ pred)
      case Some(other)                   => Predicate.And(Vector(other, pred)))

  def addOr(existing: Option[Predicate], pred: Predicate): Option[Predicate] =
    Some(existing match
      case None                         => pred
      case Some(Predicate.Or(children)) => Predicate.Or(children :+ pred)
      case Some(other)                  => Predicate.Or(Vector(other, pred)))

  def buildWhere(pred: Option[Predicate]): (String, Seq[Any], FragWriter) =
    pred match
      case None => ("", Seq.empty, FragWriter.empty)
      case Some(p) =>
        val frag = p.toFrag
        if frag.sqlString.isEmpty then ("", Seq.empty, FragWriter.empty)
        else (" WHERE " + frag.sqlString, frag.params, frag.writer)

  def buildOrderBy(
      entries: Vector[(ColRef[?], SortOrder, NullOrder)],
      rawFrags: Vector[OrderByFrag] = Vector.empty
  ): (String, Seq[Any], FragWriter) =
    val structuredParts = entries.map((col, ord, nullOrd) =>
      val base = s"${col.queryRepr} ${ord.queryRepr}"
      if nullOrd.queryRepr.isEmpty then base else s"$base ${nullOrd.queryRepr}"
    )
    val rawParts = rawFrags.filter(_.sqlString.nonEmpty)
    val allParts = structuredParts ++ rawParts.map(_.sqlString)
    if allParts.isEmpty then ("", Seq.empty, FragWriter.empty)
    else
      val sql = " ORDER BY " + allParts.mkString(", ")
      val params = rawParts.flatMap(_.params)
      val writer: FragWriter =
        if rawParts.isEmpty then FragWriter.empty
        else (ps, pos) => rawParts.foldLeft(pos)((p, f) => f.writer.write(ps, p))
      (sql, params, writer)

  def buildLimitOffset(limitOpt: Option[Int], offsetOpt: Option[Long], dt: DatabaseType): String =
    dt.renderLimitOffset(limitOpt, offsetOpt)

  def buildGroupBy(entries: Vector[ColRef[?]]): String =
    if entries.isEmpty then ""
    else " GROUP BY " + entries.map(_.queryRepr).mkString(", ")

  def buildHaving(pred: Option[Predicate]): (String, Seq[Any], FragWriter) =
    pred match
      case None => ("", Seq.empty, FragWriter.empty)
      case Some(p) =>
        val frag = p.toFrag
        if frag.sqlString.isEmpty then ("", Seq.empty, FragWriter.empty)
        else (" HAVING " + frag.sqlString, frag.params, frag.writer)

end QuerySqlBuilder
