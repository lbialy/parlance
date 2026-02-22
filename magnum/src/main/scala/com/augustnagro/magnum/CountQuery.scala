package com.augustnagro.magnum

class CountQuery[E] private[magnum] (
    private val meta: TableMeta[E],
    private val rootCodec: DbCodec[E],
    private val countSubquerySql: String,
    private val countCondition: Option[Frag],
    private val rootPredicate: Option[Predicate],
    private val orderEntries: Vector[(ColRef[?], SortOrder, NullOrder)],
    private val limitOpt: Option[Int],
    private val offsetOpt: Option[Long]
):

  def build: Frag =
    val selectCols = meta.columns.map(_.sqlName).mkString(", ")

    val countSql = countCondition match
      case None       => s"($countSubquerySql)"
      case Some(cond) => s"($countSubquerySql AND ${cond.sqlString})"

    val baseSql = s"SELECT $selectCols, $countSql AS cnt FROM ${meta.tableName}"

    val (whereSql, whereParams, whereWriter) = rootPredicate match
      case None => ("", Seq.empty, FragWriter.empty)
      case Some(pred) =>
        val frag = pred.toFrag
        if frag.sqlString.isEmpty then ("", Seq.empty, FragWriter.empty)
        else (" WHERE " + frag.sqlString, frag.params, frag.writer)

    val orderBySql =
      if orderEntries.isEmpty then ""
      else
        val entries = orderEntries.map((col, ord, nullOrd) =>
          val base = s"${col.queryRepr} ${ord.queryRepr}"
          if nullOrd.queryRepr.isEmpty then base else s"$base ${nullOrd.queryRepr}"
        )
        " ORDER BY " + entries.mkString(", ")

    val limitSql = limitOpt.fold("")(n => s" LIMIT $n")
    val offsetSql = offsetOpt.fold("")(n => s" OFFSET $n")

    val fullSql = baseSql + whereSql + orderBySql + limitSql + offsetSql

    // Count condition params appear in SELECT (before WHERE),
    // so combined writer writes count params first, then WHERE params
    val combinedParams = countCondition.map(_.params).getOrElse(Seq.empty) ++ whereParams
    val combinedWriter: FragWriter = (ps, pos) =>
      val afterCount = countCondition match
        case None       => pos
        case Some(cond) => cond.writer.write(ps, pos)
      whereWriter.write(ps, afterCount)

    Frag(fullSql, combinedParams, combinedWriter)
  end build

  def run()(using DbCon): Vector[(E, Long)] =
    given DbCodec[E] = rootCodec
    build.query[(E, Long)].run()

  def first()(using DbCon): Option[(E, Long)] =
    given DbCodec[E] = rootCodec
    val q = new CountQuery(meta, rootCodec, countSubquerySql, countCondition, rootPredicate, orderEntries, Some(1), offsetOpt)
    q.build.query[(E, Long)].run().headOption

  def buildQueries: Vector[Frag] = Vector(build)

  def debugPrintSql(using DbCon): this.type =
    DebugSql.printDebug(buildQueries)
    this

end CountQuery
