package com.augustnagro.magnum

class LockedQueryBuilder[S <: QBState, E, C <: Selectable] private[magnum] (
    private val meta: TableMeta[E],
    private val codec: DbCodec[E],
    private val cols: C,
    private val rootPredicate: Option[Predicate],
    private val orderEntries: Vector[(ColRef[?], SortOrder, NullOrder)],
    private val limitOpt: Option[Int],
    private val offsetOpt: Option[Long],
    private val distinctFlag: Boolean,
    private val lockMode: LockMode
):

  def build: Frag =
    val selectCols = meta.columns.map(_.sqlName).mkString(", ")
    val keyword = if distinctFlag then "SELECT DISTINCT" else "SELECT"
    val baseSql = s"$keyword $selectCols FROM ${meta.tableName}"
    val (whereSql, params, writer) = QuerySqlBuilder.buildWhere(rootPredicate)
    val orderBySql = QuerySqlBuilder.buildOrderBy(orderEntries)
    val limitOffsetSql = QuerySqlBuilder.buildLimitOffset(limitOpt, offsetOpt)
    Frag(baseSql + whereSql + orderBySql + limitOffsetSql + " " + lockMode.sql, params, writer)

  def run()(using DbTx): Vector[E] =
    build.query[E](using codec).run()

  def first()(using DbTx): Option[E] =
    LockedQueryBuilder(meta, codec, cols, rootPredicate, orderEntries, Some(1), offsetOpt, distinctFlag, lockMode)
      .run().headOption

  def firstOrFail()(using DbTx): E =
    first().getOrElse(
      throw QueryBuilderException(
        s"No ${meta.tableName} found matching query"
      )
    )

end LockedQueryBuilder
