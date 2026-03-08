package ma.chinespirit.parlance

class LockedQueryBuilder[S <: QBState, E, C <: Selectable] private[parlance] (
    private val meta: TableMeta[E],
    private val codec: DbCodec[E],
    private val cols: C,
    private val rootPredicate: Option[Predicate],
    private val orderEntries: Vector[(ColRef[?], SortOrder, NullOrder)],
    private val limitOpt: Option[Int],
    private val offsetOpt: Option[Long],
    private val distinctFlag: Boolean,
    private val lockMode: LockMode,
    private val rawOrderFrags: Vector[OrderByFrag] = Vector.empty
):

  def build(using con: DbTx[? <: SupportsRowLocks]): Frag =
    buildWith(con.databaseType)

  def buildWith(dt: DatabaseType): Frag =
    val selectCols = meta.columns.map(_.sqlName).mkString(", ")
    val keyword = if distinctFlag then "SELECT DISTINCT" else "SELECT"
    val baseSql = s"$keyword $selectCols FROM ${meta.tableName}"
    val (whereSql, whereParams, whereWriter) = QuerySqlBuilder.buildWhere(rootPredicate)
    val (orderBySql, orderParams, orderWriter) = QuerySqlBuilder.buildOrderBy(orderEntries, rawOrderFrags)
    val limitOffsetSql = QuerySqlBuilder.buildLimitOffset(limitOpt, offsetOpt, dt)
    val allParams = whereParams ++ orderParams
    val combinedWriter: FragWriter =
      if orderParams.isEmpty then whereWriter
      else
        (ps, pos) =>
          val next = whereWriter.write(ps, pos)
          orderWriter.write(ps, next)
    Frag(baseSql + whereSql + orderBySql + limitOffsetSql + " " + lockMode.sql, allParams, combinedWriter)

  def run()(using DbTx[? <: SupportsRowLocks]): Vector[E] =
    build.query[E](using codec).run()

  def first()(using DbTx[? <: SupportsRowLocks]): Option[E] =
    LockedQueryBuilder(meta, codec, cols, rootPredicate, orderEntries, Some(1), offsetOpt, distinctFlag, lockMode, rawOrderFrags)
      .run()
      .headOption

  def firstOrFail()(using DbTx[? <: SupportsRowLocks]): E =
    first().getOrElse(
      throw QueryBuilderException(
        s"No ${meta.tableName} found matching query"
      )
    )

end LockedQueryBuilder
