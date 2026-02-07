package com.augustnagro.magnum

import java.sql.{PreparedStatement, ResultSet}

class JoinedQuery[R <: NonEmptyTuple] private[magnum] (
    private val metas: Vector[TableMeta[?]],
    private val codecs: Vector[DbCodec[?]],
    private val joinClauses: Vector[JoinEntry],
    private val predicate: Option[Predicate],
    private val orderEntries: Vector[(ColRef[?], SortOrder)],
    private val limitOpt: Option[Int],
    private val offsetOpt: Option[Long]
):

  def alias(index: Int): String = s"t$index"
  def col[A](index: Int, c: Col[A]): BoundCol[A] = c.bound(alias(index))
  def rootCol[A](c: Col[A]): BoundCol[A] = col(0, c)
  def joinedCol[A](c: Col[A]): BoundCol[A] = col(1, c)

  private def addAnd(pred: Predicate): Option[Predicate] =
    Some(predicate match
      case None                          => pred
      case Some(Predicate.And(children)) => Predicate.And(children :+ pred)
      case Some(other)                   => Predicate.And(Vector(other, pred))
    )

  private def addOr(pred: Predicate): Option[Predicate] =
    Some(predicate match
      case None                         => pred
      case Some(Predicate.Or(children)) => Predicate.Or(children :+ pred)
      case Some(other)                  => Predicate.Or(Vector(other, pred))
    )

  def where(frag: Frag): JoinedQuery[R] =
    new JoinedQuery(metas, codecs, joinClauses, addAnd(Predicate.Leaf(frag)), orderEntries, limitOpt, offsetOpt)

  def orWhere(frag: Frag): JoinedQuery[R] =
    new JoinedQuery(metas, codecs, joinClauses, addOr(Predicate.Leaf(frag)), orderEntries, limitOpt, offsetOpt)

  def whereGroup(
      f: PredicateGroupBuilder => PredicateGroupBuilder
  ): JoinedQuery[R] =
    new JoinedQuery(metas, codecs, joinClauses, addAnd(f(PredicateGroupBuilder.empty).build), orderEntries, limitOpt, offsetOpt)

  def orWhereGroup(
      f: PredicateGroupBuilder => PredicateGroupBuilder
  ): JoinedQuery[R] =
    new JoinedQuery(metas, codecs, joinClauses, addOr(f(PredicateGroupBuilder.empty).build), orderEntries, limitOpt, offsetOpt)

  def orderBy(col: ColRef[?], order: SortOrder = SortOrder.Asc): JoinedQuery[R] =
    new JoinedQuery(metas, codecs, joinClauses, predicate, orderEntries :+ (col, order), limitOpt, offsetOpt)

  def limit(n: Int): JoinedQuery[R] =
    new JoinedQuery(metas, codecs, joinClauses, predicate, orderEntries, Some(n), offsetOpt)

  def offset(n: Long): JoinedQuery[R] =
    new JoinedQuery(metas, codecs, joinClauses, predicate, orderEntries, limitOpt, Some(n))

  def join[S, U](rel: Relationship[S, U])(using
      sMeta: TableMeta[S],
      uMeta: TableMeta[U],
      uCodec: DbCodec[U]
  ): JoinedQuery[Tuple.Append[R, U]] =
    val sourceIdx = metas.indexWhere(_.tableName == sMeta.tableName)
    require(sourceIdx >= 0, s"Table ${sMeta.tableName} not in join chain")
    val newIdx = metas.size
    val entry = JoinEntry(
      TableRef(uMeta.tableName, s"t$newIdx", uMeta.tableName),
      JoinType.Inner,
      Frag(s"t$sourceIdx.${rel.fk.sqlName} = t$newIdx.${rel.pk.sqlName}", Seq.empty, FragWriter.empty)
    )
    new JoinedQuery[Tuple.Append[R, U]](
      metas :+ uMeta, codecs :+ uCodec, joinClauses :+ entry,
      predicate, orderEntries, limitOpt, offsetOpt
    )

  private def resultCodec: DbCodec[R] =
    new DbCodec[R]:
      val cols: IArray[Int] = IArray.from(codecs.flatMap(_.cols.toSeq))
      def queryRepr: String = codecs.map(_.queryRepr).mkString("(", ", ", ")")
      def readSingle(rs: ResultSet, pos: Int): R =
        val arr = new Array[Any](codecs.size)
        var p = pos
        for i <- codecs.indices do
          arr(i) = codecs(i).readSingle(rs, p)
          p += codecs(i).cols.length
        Tuple.fromArray(arr).asInstanceOf[R]
      def readSingleOption(rs: ResultSet, pos: Int): Option[R] =
        val arr = new Array[Any](codecs.size)
        var p = pos
        var allPresent = true
        for i <- codecs.indices do
          codecs(i).readSingleOption(rs, p) match
            case Some(v) => arr(i) = v
            case None    => allPresent = false
          p += codecs(i).cols.length
        if allPresent then Some(Tuple.fromArray(arr).asInstanceOf[R])
        else None
      def writeSingle(entity: R, ps: PreparedStatement, pos: Int): Unit =
        var p = pos
        for i <- codecs.indices do
          codecs(i).asInstanceOf[DbCodec[Any]].writeSingle(entity.productElement(i), ps, p)
          p += codecs(i).cols.length

  private def buildFromJoinWhere: (String, Seq[Any], FragWriter) =
    val fromSql = s"FROM ${metas(0).tableName} t0"

    val joinsSql = joinClauses.map { entry =>
      val kw = entry.joinType match
        case JoinType.Inner => "INNER JOIN"
        case JoinType.Left  => "LEFT JOIN"
        case JoinType.Right => "RIGHT JOIN"
        case JoinType.Cross => "CROSS JOIN"
      s"$kw ${entry.tableRef.tableName} ${entry.tableRef.alias} ON ${entry.onCondition.sqlString}"
    }.mkString(" ", " ", "")

    val fromJoinSql = fromSql + joinsSql

    predicate match
      case None => (fromJoinSql, Seq.empty, FragWriter.empty)
      case Some(pred) =>
        val frag = pred.toFrag
        if frag.sqlString.isEmpty then (fromJoinSql, Seq.empty, FragWriter.empty)
        else (fromJoinSql + " WHERE " + frag.sqlString, frag.params, frag.writer)

  def build: Frag =
    val selectParts = metas.zipWithIndex.map { (meta, idx) =>
      meta.columns.map(c => s"t$idx.${c.sqlName}").mkString(", ")
    }
    val selectSql = s"SELECT ${selectParts.mkString(", ")} "

    val (fromJoinWhereSql, params, writer) = buildFromJoinWhere

    val orderBySql =
      if orderEntries.isEmpty then ""
      else
        val entries = orderEntries.map((col, ord) => s"${col.queryRepr} ${ord.queryRepr}")
        " ORDER BY " + entries.mkString(", ")

    val limitSql = limitOpt.fold("")(n => s" LIMIT $n")
    val offsetSql = offsetOpt.fold("")(n => s" OFFSET $n")

    Frag(selectSql + fromJoinWhereSql + orderBySql + limitSql + offsetSql, params, writer)

  def run()(using DbCon): Vector[R] =
    given codec: DbCodec[R] = resultCodec
    build.query[R].run()

  def first()(using DbCon): Option[R] =
    limit(1).run().headOption

  def firstOrFail()(using DbCon): R =
    first().getOrElse(
      throw QueryBuilderException(
        s"No result found matching joined query"
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
