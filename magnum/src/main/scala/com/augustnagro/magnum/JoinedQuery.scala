package com.augustnagro.magnum

class JoinedQuery[E, T] private[magnum] (
    private val rootMeta: TableMeta[E],
    private val rootCodec: DbCodec[E],
    private val joinedMeta: TableMeta[T],
    private val joinedCodec: DbCodec[T],
    private val joinType: JoinType,
    private val fkCol: Col[?],
    private val pkCol: Col[?],
    private val rootPredicate: Option[Predicate],
    private val orderEntries: Vector[(Col[?], SortOrder)],
    private val limitOpt: Option[Int],
    private val offsetOpt: Option[Long]
):
  private val rootAlias = "t0"
  private val joinAlias = "t1"

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

  def where(frag: Frag): JoinedQuery[E, T] =
    new JoinedQuery(rootMeta, rootCodec, joinedMeta, joinedCodec, joinType, fkCol, pkCol, addAnd(Predicate.Leaf(frag)), orderEntries, limitOpt, offsetOpt)

  def orWhere(frag: Frag): JoinedQuery[E, T] =
    new JoinedQuery(rootMeta, rootCodec, joinedMeta, joinedCodec, joinType, fkCol, pkCol, addOr(Predicate.Leaf(frag)), orderEntries, limitOpt, offsetOpt)

  def whereGroup(
      f: PredicateGroupBuilder => PredicateGroupBuilder
  ): JoinedQuery[E, T] =
    new JoinedQuery(rootMeta, rootCodec, joinedMeta, joinedCodec, joinType, fkCol, pkCol, addAnd(f(PredicateGroupBuilder.empty).build), orderEntries, limitOpt, offsetOpt)

  def orWhereGroup(
      f: PredicateGroupBuilder => PredicateGroupBuilder
  ): JoinedQuery[E, T] =
    new JoinedQuery(rootMeta, rootCodec, joinedMeta, joinedCodec, joinType, fkCol, pkCol, addOr(f(PredicateGroupBuilder.empty).build), orderEntries, limitOpt, offsetOpt)

  def orderBy(col: Col[?], order: SortOrder = SortOrder.Asc): JoinedQuery[E, T] =
    new JoinedQuery(rootMeta, rootCodec, joinedMeta, joinedCodec, joinType, fkCol, pkCol, rootPredicate, orderEntries :+ (col, order), limitOpt, offsetOpt)

  def limit(n: Int): JoinedQuery[E, T] =
    new JoinedQuery(rootMeta, rootCodec, joinedMeta, joinedCodec, joinType, fkCol, pkCol, rootPredicate, orderEntries, Some(n), offsetOpt)

  def offset(n: Long): JoinedQuery[E, T] =
    new JoinedQuery(rootMeta, rootCodec, joinedMeta, joinedCodec, joinType, fkCol, pkCol, rootPredicate, orderEntries, limitOpt, Some(n))

  private def buildFromJoinWhere: (String, Seq[Any], FragWriter) =
    val joinKeyword = joinType match
      case JoinType.Inner => "INNER JOIN"
      case JoinType.Left  => "LEFT JOIN"
      case JoinType.Right => "RIGHT JOIN"
      case JoinType.Cross => "CROSS JOIN"

    val fromSql =
      s"FROM ${rootMeta.tableName} $rootAlias $joinKeyword ${joinedMeta.tableName} $joinAlias ON $rootAlias.${fkCol.sqlName} = $joinAlias.${pkCol.sqlName}"

    rootPredicate match
      case None => (fromSql, Seq.empty, FragWriter.empty)
      case Some(pred) =>
        val frag = pred.toFrag
        if frag.sqlString.isEmpty then (fromSql, Seq.empty, FragWriter.empty)
        else (fromSql + " WHERE " + frag.sqlString, frag.params, frag.writer)

  def build: Frag =
    val rootCols = rootMeta.columns.map(c => s"$rootAlias.${c.sqlName}").mkString(", ")
    val joinedCols = joinedMeta.columns.map(c => s"$joinAlias.${c.sqlName}").mkString(", ")
    val selectSql = s"SELECT $rootCols, $joinedCols "

    val (fromJoinWhereSql, params, writer) = buildFromJoinWhere

    val orderBySql =
      if orderEntries.isEmpty then ""
      else
        val entries = orderEntries.map((col, ord) => s"${col.sqlName} ${ord.queryRepr}")
        " ORDER BY " + entries.mkString(", ")

    val limitSql = limitOpt.fold("")(n => s" LIMIT $n")
    val offsetSql = offsetOpt.fold("")(n => s" OFFSET $n")

    Frag(selectSql + fromJoinWhereSql + orderBySql + limitSql + offsetSql, params, writer)

  def run()(using DbCon): Vector[(E, T)] =
    given tupleCodec: DbCodec[(E, T)] = DbCodec.Tuple2Codec(using rootCodec, joinedCodec)
    build.query[(E, T)].run()

  def first()(using DbCon): Option[(E, T)] =
    limit(1).run().headOption

  def firstOrFail()(using DbCon): (E, T) =
    first().getOrElse(
      throw QueryBuilderException(
        s"No ${rootMeta.tableName} joined with ${joinedMeta.tableName} found matching query"
      )
    )

  def count()(using DbCon): Long =
    val (fromJoinWhereSql, params, writer) = buildFromJoinWhere
    Frag(s"SELECT COUNT(*) $fromJoinWhereSql", params, writer)
      .query[Long].run().head

  def exists()(using DbCon): Boolean =
    val (fromJoinWhereSql, params, writer) = buildFromJoinWhere
    Frag(s"SELECT EXISTS(SELECT 1 $fromJoinWhereSql)", params, writer)
      .query[Boolean].run().head

end JoinedQuery
