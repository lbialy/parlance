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

  def buildOrderBy(entries: Vector[(ColRef[?], SortOrder, NullOrder)]): String =
    if entries.isEmpty then ""
    else
      val parts = entries.map((col, ord, nullOrd) =>
        val base = s"${col.queryRepr} ${ord.queryRepr}"
        if nullOrd.queryRepr.isEmpty then base else s"$base ${nullOrd.queryRepr}"
      )
      " ORDER BY " + parts.mkString(", ")

  def buildLimitOffset(limitOpt: Option[Int], offsetOpt: Option[Long]): String =
    val limitSql = limitOpt.fold("")(n => s" LIMIT $n")
    val offsetSql = offsetOpt.fold("")(n => s" OFFSET $n")
    limitSql + offsetSql

end QuerySqlBuilder
