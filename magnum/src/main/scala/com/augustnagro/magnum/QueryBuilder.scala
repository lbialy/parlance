package com.augustnagro.magnum

sealed trait QBState
sealed trait HasRoot extends QBState

class QueryBuilder[S <: QBState, E] private (
    private val meta: TableMeta[E],
    private val codec: DbCodec[E],
    private val predicates: Vector[Predicate],
    private val orderEntries: Vector[(Col[?], SortOrder)],
    private val limitOpt: Option[Int],
    private val offsetOpt: Option[Long]
):

  def where(frag: Frag): QueryBuilder[HasRoot, E] =
    new QueryBuilder(meta, codec, predicates :+ Predicate.Leaf(frag), orderEntries, limitOpt, offsetOpt)

  def orderBy(col: Col[?], order: SortOrder = SortOrder.Asc): QueryBuilder[S, E] =
    new QueryBuilder(meta, codec, predicates, orderEntries :+ (col, order), limitOpt, offsetOpt)

  def limit(n: Int): QueryBuilder[S, E] =
    new QueryBuilder(meta, codec, predicates, orderEntries, Some(n), offsetOpt)

  def offset(n: Long): QueryBuilder[S, E] =
    new QueryBuilder(meta, codec, predicates, orderEntries, limitOpt, Some(n))

  private def buildWhere: (String, Seq[Any], FragWriter) =
    if predicates.isEmpty then ("", Seq.empty, FragWriter.empty)
    else
      val whereFrag = Predicate.And(predicates).toFrag
      (" WHERE " + whereFrag.sqlString, whereFrag.params, whereFrag.writer)

  def build: Frag =
    val selectCols = meta.columns.map(_.sqlName).mkString(", ")
    val baseSql = s"SELECT $selectCols FROM ${meta.tableName}"

    val (whereSql, params, writer) = buildWhere

    val orderBySql =
      if orderEntries.isEmpty then ""
      else
        val entries = orderEntries.map((col, ord) => s"${col.sqlName} ${ord.queryRepr}")
        " ORDER BY " + entries.mkString(", ")

    val limitSql = limitOpt.fold("")(n => s" LIMIT $n")
    val offsetSql = offsetOpt.fold("")(n => s" OFFSET $n")

    Frag(baseSql + whereSql + orderBySql + limitSql + offsetSql, params, writer)

  def run()(using DbCon): Vector[E] =
    build.query[E](using codec).run()

  def first()(using DbCon): Option[E] =
    limit(1).run().headOption

  def firstOrFail()(using DbCon): E =
    first().getOrElse(
      throw QueryBuilderException(
        s"No ${meta.tableName} found matching query"
      )
    )

  def count()(using DbCon): Long =
    val baseSql = s"SELECT COUNT(*) FROM ${meta.tableName}"
    val (whereSql, params, writer) = buildWhere
    Frag(baseSql + whereSql, params, writer)
      .query[Long].run().head

  def exists()(using DbCon): Boolean =
    val innerSql = s"SELECT 1 FROM ${meta.tableName}"
    val (whereSql, params, writer) = buildWhere
    Frag(s"SELECT EXISTS($innerSql$whereSql)", params, writer)
      .query[Boolean].run().head

  def debugPrintSql(): this.type =
    println(s"SQL: ${build.sqlString}")
    println(s"Params: ${build.params.mkString(", ")}")
    this

end QueryBuilder

object QueryBuilder:
  def from[E](using
      meta: TableMeta[E],
      codec: DbCodec[E]
  ): QueryBuilder[HasRoot, E] =
    new QueryBuilder(meta, codec, Vector.empty, Vector.empty, None, None)
