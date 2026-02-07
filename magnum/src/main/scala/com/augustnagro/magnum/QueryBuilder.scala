package com.augustnagro.magnum

sealed trait QBState
sealed trait HasRoot extends QBState

class QueryBuilder[S <: QBState, E] private (
    private val meta: TableMeta[E],
    private val codec: DbCodec[E],
    private val rootPredicate: Option[Predicate],
    private val orderEntries: Vector[(Col[?], SortOrder)],
    private val limitOpt: Option[Int],
    private val offsetOpt: Option[Long]
):

  private def addAnd(pred: Predicate): Option[Predicate] =
    Some(rootPredicate match
      case None                          => pred
      case Some(Predicate.And(children)) => Predicate.And(children :+ pred)
      case Some(other)                   => Predicate.And(Vector(other, pred))
    )

  private def addOr(pred: Predicate): Option[Predicate] =
    Some(rootPredicate match
      case None                         => pred
      case Some(Predicate.Or(children)) => Predicate.Or(children :+ pred)
      case Some(other)                  => Predicate.Or(Vector(other, pred))
    )

  def where(frag: Frag): QueryBuilder[HasRoot, E] =
    new QueryBuilder(meta, codec, addAnd(Predicate.Leaf(frag)), orderEntries, limitOpt, offsetOpt)

  def orWhere(frag: Frag): QueryBuilder[HasRoot, E] =
    new QueryBuilder(meta, codec, addOr(Predicate.Leaf(frag)), orderEntries, limitOpt, offsetOpt)

  def whereGroup(
      f: PredicateGroupBuilder => PredicateGroupBuilder
  ): QueryBuilder[HasRoot, E] =
    new QueryBuilder(meta, codec, addAnd(f(PredicateGroupBuilder.empty).build), orderEntries, limitOpt, offsetOpt)

  def orWhereGroup(
      f: PredicateGroupBuilder => PredicateGroupBuilder
  ): QueryBuilder[HasRoot, E] =
    new QueryBuilder(meta, codec, addOr(f(PredicateGroupBuilder.empty).build), orderEntries, limitOpt, offsetOpt)

  def orderBy(col: Col[?], order: SortOrder = SortOrder.Asc): QueryBuilder[S, E] =
    new QueryBuilder(meta, codec, rootPredicate, orderEntries :+ (col, order), limitOpt, offsetOpt)

  def limit(n: Int): QueryBuilder[S, E] =
    new QueryBuilder(meta, codec, rootPredicate, orderEntries, Some(n), offsetOpt)

  def offset(n: Long): QueryBuilder[S, E] =
    new QueryBuilder(meta, codec, rootPredicate, orderEntries, limitOpt, Some(n))

  private def buildWhere: (String, Seq[Any], FragWriter) =
    rootPredicate match
      case None => ("", Seq.empty, FragWriter.empty)
      case Some(pred) =>
        val frag = pred.toFrag
        if frag.sqlString.isEmpty then ("", Seq.empty, FragWriter.empty)
        else (" WHERE " + frag.sqlString, frag.params, frag.writer)

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

  def join[T](rel: Relationship[E, T])(using
      joinedMeta: TableMeta[T],
      joinedCodec: DbCodec[T]
  ): JoinedQuery[(E, T)] =
    val entry = JoinEntry(
      TableRef(joinedMeta.tableName, "t1", joinedMeta.tableName),
      JoinType.Inner,
      Frag(s"t0.${rel.fk.sqlName} = t1.${rel.pk.sqlName}", Seq.empty, FragWriter.empty)
    )
    new JoinedQuery[(E, T)](
      Vector(meta, joinedMeta),
      Vector(codec, joinedCodec),
      Vector(entry),
      rootPredicate,
      orderEntries.map((c, o) => (c: ColRef[?], o)),
      limitOpt, offsetOpt
    )

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
    new QueryBuilder(meta, codec, None, Vector.empty, None, None)
