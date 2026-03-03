package com.augustnagro.magnum

class CountQuery[E] private[magnum] (
    private val meta: TableMeta[E],
    private val rootCodec: DbCodec[E],
    private val countSubquerySql: String,
    private val countCondition: Option[Frag],
    private val rootPredicate: Option[Predicate],
    private val orderEntries: Vector[(ColRef[?], SortOrder, NullOrder)],
    private val limitOpt: Option[Int],
    private val offsetOpt: Option[Long],
    private val countBaseParams: Seq[Any] = Seq.empty,
    private val countBaseWriter: FragWriter = FragWriter.empty
):

  def build: Frag =
    val selectCols = meta.columns.map(_.sqlName).mkString(", ")

    val countSql = countCondition match
      case None       => s"($countSubquerySql)"
      case Some(cond) => s"($countSubquerySql AND ${cond.sqlString})"

    val baseSql = s"SELECT $selectCols, $countSql AS cnt FROM ${meta.tableName}"

    val (whereSql, whereParams, whereWriter) = QuerySqlBuilder.buildWhere(rootPredicate)
    val (orderBySql, _, _) = QuerySqlBuilder.buildOrderBy(orderEntries)
    val limitOffsetSql = QuerySqlBuilder.buildLimitOffset(limitOpt, offsetOpt)

    val fullSql = baseSql + whereSql + orderBySql + limitOffsetSql

    // Count params appear in SELECT (before WHERE):
    // first base params (scope conditions), then condition params, then WHERE params
    val countCondParams = countCondition.map(_.params).getOrElse(Seq.empty)
    val combinedParams = countBaseParams ++ countCondParams ++ whereParams
    val combinedWriter: FragWriter = (ps, pos) =>
      val afterBase = countBaseWriter.write(ps, pos)
      val afterCount = countCondition match
        case None       => afterBase
        case Some(cond) => cond.writer.write(ps, afterBase)
      whereWriter.write(ps, afterCount)

    Frag(fullSql, combinedParams, combinedWriter)
  end build

  def run()(using DbCon): Vector[(E, Long)] =
    given DbCodec[E] = rootCodec
    build.query[(E, Long)].run()

  def first()(using DbCon): Option[(E, Long)] =
    given DbCodec[E] = rootCodec
    val q = new CountQuery(meta, rootCodec, countSubquerySql, countCondition, rootPredicate, orderEntries, Some(1), offsetOpt, countBaseParams, countBaseWriter)
    q.build.query[(E, Long)].run().headOption

  def buildQueries: Vector[Frag] = Vector(build)

  def debugPrintSql(using DbCon): this.type =
    DebugSql.printDebug(buildQueries)
    this

end CountQuery
