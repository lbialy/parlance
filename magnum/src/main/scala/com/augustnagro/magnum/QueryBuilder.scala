package com.augustnagro.magnum

import scala.deriving.Mirror
import scala.quoted.*

sealed trait QBState
sealed trait HasRoot extends QBState

class QueryBuilder[S <: QBState, E, C <: Selectable] private[magnum] (
    private val meta: TableMeta[E],
    private val codec: DbCodec[E],
    private val cols: C,
    private val rootPredicate: Option[Predicate],
    private val orderEntries: Vector[(ColRef[?], SortOrder, NullOrder)],
    private val limitOpt: Option[Int],
    private val offsetOpt: Option[Long]
):

  private def addAnd(pred: Predicate): Option[Predicate] =
    Some(rootPredicate match
      case None                          => pred
      case Some(Predicate.And(children)) => Predicate.And(children :+ pred)
      case Some(other)                   => Predicate.And(Vector(other, pred)))

  private def addOr(pred: Predicate): Option[Predicate] =
    Some(rootPredicate match
      case None                         => pred
      case Some(Predicate.Or(children)) => Predicate.Or(children :+ pred)
      case Some(other)                  => Predicate.Or(Vector(other, pred)))

  def where(frag: Frag): QueryBuilder[HasRoot, E, C] =
    new QueryBuilder(meta, codec, cols, addAnd(Predicate.Leaf(frag)), orderEntries, limitOpt, offsetOpt)

  def where(f: C => Frag): QueryBuilder[HasRoot, E, C] =
    new QueryBuilder(meta, codec, cols, addAnd(Predicate.Leaf(f(cols))), orderEntries, limitOpt, offsetOpt)

  def orWhere(frag: Frag): QueryBuilder[HasRoot, E, C] =
    new QueryBuilder(meta, codec, cols, addOr(Predicate.Leaf(frag)), orderEntries, limitOpt, offsetOpt)

  def orWhere(f: C => Frag): QueryBuilder[HasRoot, E, C] =
    new QueryBuilder(meta, codec, cols, addOr(Predicate.Leaf(f(cols))), orderEntries, limitOpt, offsetOpt)

  def whereGroup(
      f: PredicateGroupBuilder[C] => PredicateGroupBuilder[C]
  ): QueryBuilder[HasRoot, E, C] =
    new QueryBuilder(meta, codec, cols, addAnd(f(PredicateGroupBuilder.empty(cols)).build), orderEntries, limitOpt, offsetOpt)

  def orWhereGroup(
      f: PredicateGroupBuilder[C] => PredicateGroupBuilder[C]
  ): QueryBuilder[HasRoot, E, C] =
    new QueryBuilder(meta, codec, cols, addOr(f(PredicateGroupBuilder.empty(cols)).build), orderEntries, limitOpt, offsetOpt)

  def orderBy(f: C => ColRef[?], order: SortOrder = SortOrder.Asc, nullOrder: NullOrder = NullOrder.Default): QueryBuilder[S, E, C] =
    new QueryBuilder(meta, codec, cols, rootPredicate, orderEntries :+ (f(cols), order, nullOrder), limitOpt, offsetOpt)

  def orderBy(f: C => ColRef[?]): QueryBuilder[S, E, C] =
    new QueryBuilder(meta, codec, cols, rootPredicate, orderEntries :+ (f(cols), SortOrder.Asc, NullOrder.Default), limitOpt, offsetOpt)

  def limit(n: Int): QueryBuilder[S, E, C] =
    if n < 0 then throw QueryBuilderException("limit must not be negative")
    new QueryBuilder(meta, codec, cols, rootPredicate, orderEntries, Some(n), offsetOpt)

  def offset(n: Long): QueryBuilder[S, E, C] =
    if n < 0 then throw QueryBuilderException("offset must not be negative")
    new QueryBuilder(meta, codec, cols, rootPredicate, orderEntries, limitOpt, Some(n))

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
        val entries = orderEntries.map((col, ord, nullOrd) =>
          val base = s"${col.queryRepr} ${ord.queryRepr}"
          if nullOrd.queryRepr.isEmpty then base else s"$base ${nullOrd.queryRepr}"
        )
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
    QueryBuilderException.requireNonEmpty(
      Frag(baseSql + whereSql, params, writer).query[Long].run(),
      s"Aggregate query on ${meta.tableName}"
    )

  def exists()(using DbCon): Boolean =
    val innerSql = s"SELECT 1 FROM ${meta.tableName}"
    val (whereSql, params, writer) = buildWhere
    QueryBuilderException.requireNonEmpty(
      Frag(s"SELECT EXISTS($innerSql$whereSql)", params, writer).query[Boolean].run(),
      s"Aggregate query on ${meta.tableName}"
    )

  def sum[A](f: C => ColRef[A])(using DbCodec[A], DbCon): Option[A] =
    runAgg[Option[A]]("SUM", f(cols).queryRepr)

  def avg(f: C => ColRef[?])(using DbCon): Option[Double] =
    runAgg[Option[Double]]("AVG", f(cols).queryRepr)

  def min[A](f: C => ColRef[A])(using DbCodec[A], DbCon): Option[A] =
    runAgg[Option[A]]("MIN", f(cols).queryRepr)

  def max[A](f: C => ColRef[A])(using DbCodec[A], DbCon): Option[A] =
    runAgg[Option[A]]("MAX", f(cols).queryRepr)

  def count(f: C => ColRef[?])(using DbCon): Long =
    runAgg[Long]("COUNT", f(cols).queryRepr)(using DbCodec.LongCodec)

  private def runAgg[R](fn: String, colRepr: String)(using DbCodec[R], DbCon): R =
    val baseSql = s"SELECT $fn($colRepr) FROM ${meta.tableName}"
    val (whereSql, params, writer) = buildWhere
    QueryBuilderException.requireNonEmpty(
      Frag(baseSql + whereSql, params, writer).query[R].run(),
      s"Aggregate query on ${meta.tableName}"
    )

  def withRelated[T](rel: HasMany[E, T, ?])(using
      childMeta: TableMeta[T],
      childCodec: DbCodec[T]
  ): EagerQuery[E, Vector[T] *: EmptyTuple] =
    val d = DirectEagerDef(meta, rel, childMeta, childCodec, None)
    EagerQuery(build, codec, meta, Vector(d))

  def withRelated[T](rel: BelongsToMany[E, T, ?])(using
      targetMeta: TableMeta[T],
      targetCodec: DbCodec[T]
  ): EagerQuery[E, Vector[T] *: EmptyTuple] =
    val d = PivotEagerDef(meta, rel, targetMeta, targetCodec, None)
    EagerQuery(build, codec, meta, Vector(d))

  def withRelated[T](rel: HasManyThrough[E, T, ?])(using
      targetMeta: TableMeta[T],
      targetCodec: DbCodec[T]
  ): EagerQuery[E, Vector[T] *: EmptyTuple] =
    val d = ThroughEagerDef(
      meta,
      rel.intermediateTable,
      rel.sourceFk,
      rel.intermediatePk.sqlName,
      rel.targetFk.scalaName,
      rel.targetFk.sqlName,
      rel.sourcePk.scalaName,
      targetMeta,
      targetCodec,
      None
    )
    EagerQuery(build, codec, meta, Vector(d))

  def withRelated[T](rel: HasOneThrough[E, T, ?])(using
      targetMeta: TableMeta[T],
      targetCodec: DbCodec[T]
  ): EagerQuery[E, Vector[T] *: EmptyTuple] =
    val d = ThroughEagerDef(
      meta,
      rel.intermediateTable,
      rel.sourceFk,
      rel.intermediatePk.sqlName,
      rel.targetFk.scalaName,
      rel.targetFk.sqlName,
      rel.sourcePk.scalaName,
      targetMeta,
      targetCodec,
      None
    )
    EagerQuery(build, codec, meta, Vector(d))

  // --- Constrained withRelated ---

  def withRelated[T, CT <: Selectable](rel: HasMany[E, T, CT])(f: CT => Frag)(using
      childMeta: TableMeta[T],
      childCodec: DbCodec[T]
  ): EagerQuery[E, Vector[T] *: EmptyTuple] =
    val relCols = new Columns[T](childMeta.columns).asInstanceOf[CT]
    val d = DirectEagerDef(meta, rel, childMeta, childCodec, Some(f(relCols)))
    EagerQuery(build, codec, meta, Vector(d))

  def withRelated[T, CT <: Selectable](rel: BelongsToMany[E, T, CT])(f: CT => Frag)(using
      targetMeta: TableMeta[T],
      targetCodec: DbCodec[T]
  ): EagerQuery[E, Vector[T] *: EmptyTuple] =
    val relCols = new Columns[T](targetMeta.columns).asInstanceOf[CT]
    val d = PivotEagerDef(meta, rel, targetMeta, targetCodec, Some(f(relCols)))
    EagerQuery(build, codec, meta, Vector(d))

  def withRelated[T, CT <: Selectable](rel: HasManyThrough[E, T, CT])(f: CT => Frag)(using
      targetMeta: TableMeta[T],
      targetCodec: DbCodec[T]
  ): EagerQuery[E, Vector[T] *: EmptyTuple] =
    val relCols = new Columns[T](targetMeta.columns).asInstanceOf[CT]
    val d = ThroughEagerDef(
      meta,
      rel.intermediateTable,
      rel.sourceFk,
      rel.intermediatePk.sqlName,
      rel.targetFk.scalaName,
      rel.targetFk.sqlName,
      rel.sourcePk.scalaName,
      targetMeta,
      targetCodec,
      Some(f(relCols))
    )
    EagerQuery(build, codec, meta, Vector(d))

  def withRelated[T, CT <: Selectable](rel: HasOneThrough[E, T, CT])(f: CT => Frag)(using
      targetMeta: TableMeta[T],
      targetCodec: DbCodec[T]
  ): EagerQuery[E, Vector[T] *: EmptyTuple] =
    val relCols = new Columns[T](targetMeta.columns).asInstanceOf[CT]
    val d = ThroughEagerDef(
      meta,
      rel.intermediateTable,
      rel.sourceFk,
      rel.intermediatePk.sqlName,
      rel.targetFk.scalaName,
      rel.targetFk.sqlName,
      rel.sourcePk.scalaName,
      targetMeta,
      targetCodec,
      Some(f(relCols))
    )
    EagerQuery(build, codec, meta, Vector(d))

  // --- Composed (via) withRelated ---

  def withRelated[I, T](rel: ComposedRelationship[E, I, T, ?])(using
      intermediateMeta: TableMeta[I],
      intermediateCodec: DbCodec[I],
      targetMeta: TableMeta[T],
      targetCodec: DbCodec[T]
  ): EagerQuery[E, Vector[T] *: EmptyTuple] =
    val d = ComposedEagerDef(meta, rel.inner, intermediateMeta, intermediateCodec, rel.outer, targetMeta, targetCodec, None)
    EagerQuery(build, codec, meta, Vector(d))

  def withRelated[I, T, CT <: Selectable](rel: ComposedRelationship[E, I, T, CT])(f: CT => Frag)(using
      intermediateMeta: TableMeta[I],
      intermediateCodec: DbCodec[I],
      targetMeta: TableMeta[T],
      targetCodec: DbCodec[T]
  ): EagerQuery[E, Vector[T] *: EmptyTuple] =
    val relCols = new Columns[T](targetMeta.columns).asInstanceOf[CT]
    val d = ComposedEagerDef(meta, rel.inner, intermediateMeta, intermediateCodec, rel.outer, targetMeta, targetCodec, Some(f(relCols)))
    EagerQuery(build, codec, meta, Vector(d))

  // --- withCount for HasMany ---

  def withCount[T](rel: HasMany[E, T, ?])(using
      relMeta: TableMeta[T]
  ): CountQuery[E] =
    val subSql = buildRelCountSql(rel, relMeta)
    new CountQuery(meta, codec, subSql, None, rootPredicate, orderEntries, limitOpt, offsetOpt)

  def withCount[T, CT <: Selectable](rel: HasMany[E, T, CT])(f: CT => Frag)(using
      relMeta: TableMeta[T]
  ): CountQuery[E] =
    val subSql = buildRelCountSql(rel, relMeta)
    val cols = new Columns[T](relMeta.columns).asInstanceOf[CT]
    new CountQuery(meta, codec, subSql, Some(f(cols)), rootPredicate, orderEntries, limitOpt, offsetOpt)

  // --- withCount for BelongsToMany ---

  def withCount[T](rel: BelongsToMany[E, T, ?]): CountQuery[E] =
    val subSql = buildPivotCountSql(rel, None)
    new CountQuery(meta, codec, subSql, None, rootPredicate, orderEntries, limitOpt, offsetOpt)

  def withCount[T, CT <: Selectable](rel: BelongsToMany[E, T, CT])(f: CT => Frag)(using
      relMeta: TableMeta[T]
  ): CountQuery[E] =
    val cols = new Columns[T](relMeta.columns).asInstanceOf[CT]
    val subSql = buildPivotCountSql(rel, Some(relMeta))
    new CountQuery(meta, codec, subSql, Some(f(cols)), rootPredicate, orderEntries, limitOpt, offsetOpt)

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
      orderEntries,
      limitOpt,
      offsetOpt
    )

  def leftJoin[T](rel: Relationship[E, T])(using
      joinedMeta: TableMeta[T],
      joinedCodec: DbCodec[T]
  ): JoinedQuery[(E, Option[T])] =
    val optCodec = DbCodec.OptionCodec[T](using joinedCodec)
    val entry = JoinEntry(
      TableRef(joinedMeta.tableName, "t1", joinedMeta.tableName),
      JoinType.Left,
      Frag(s"t0.${rel.fk.sqlName} = t1.${rel.pk.sqlName}", Seq.empty, FragWriter.empty)
    )
    new JoinedQuery[(E, Option[T])](
      Vector(meta, joinedMeta),
      Vector(codec, optCodec),
      Vector(entry),
      rootPredicate,
      orderEntries,
      limitOpt,
      offsetOpt
    )

  // --- whereHas / doesntHave for Relationship ---

  def whereHas[T](rel: Relationship[E, T])(using
      relMeta: TableMeta[T]
  ): QueryBuilder[HasRoot, E, C] =
    where(buildRelExistsFrag(rel, relMeta, None, negate = false))

  def whereHas[T](rel: HasMany[E, T, ?])(using
      relMeta: TableMeta[T]
  ): QueryBuilder[HasRoot, E, C] =
    where(buildRelExistsFrag(rel, relMeta, None, negate = false))

  def whereHas[T, CT <: Selectable](rel: HasMany[E, T, CT])(f: CT => Frag)(using
      relMeta: TableMeta[T]
  ): QueryBuilder[HasRoot, E, C] =
    val cols = new Columns[T](relMeta.columns).asInstanceOf[CT]
    where(buildRelExistsFrag(rel, relMeta, Some(f(cols)), negate = false))

  def doesntHave[T](rel: HasMany[E, T, ?])(using
      relMeta: TableMeta[T]
  ): QueryBuilder[HasRoot, E, C] =
    where(buildRelExistsFrag(rel, relMeta, None, negate = true))

  def doesntHave[T](rel: Relationship[E, T])(using
      relMeta: TableMeta[T]
  ): QueryBuilder[HasRoot, E, C] =
    where(buildRelExistsFrag(rel, relMeta, None, negate = true))

  // --- whereHas / doesntHave for BelongsToMany ---

  def whereHas[T](rel: BelongsToMany[E, T, ?]): QueryBuilder[HasRoot, E, C] =
    where(buildPivotExistsFrag(rel, None, negate = false))

  def whereHas[T, CT <: Selectable](rel: BelongsToMany[E, T, CT])(f: CT => Frag)(using
      relMeta: TableMeta[T]
  ): QueryBuilder[HasRoot, E, C] =
    val cols = new Columns[T](relMeta.columns).asInstanceOf[CT]
    where(buildPivotExistsFrag(rel, Some((f(cols), relMeta)), negate = false))

  def doesntHave[T](rel: BelongsToMany[E, T, ?]): QueryBuilder[HasRoot, E, C] =
    where(buildPivotExistsFrag(rel, None, negate = true))

  // --- has with count threshold ---

  // HasMany — unconstrained
  def has[T](rel: HasMany[E, T, ?])(f: CountExpr => Frag)(using
      relMeta: TableMeta[T]
  ): QueryBuilder[HasRoot, E, C] =
    val countSql = buildRelCountSql(rel, relMeta)
    where(f(CountExpr(countSql)))

  // HasMany — constrained
  def has[T, CT <: Selectable](rel: HasMany[E, T, CT], cond: CT => Frag)(f: CountExpr => Frag)(using
      relMeta: TableMeta[T]
  ): QueryBuilder[HasRoot, E, C] =
    val countSql = buildRelCountSql(rel, relMeta)
    val relCols = new Columns[T](relMeta.columns).asInstanceOf[CT]
    val condFrag = cond(relCols)
    where(f(CountExpr(s"$countSql AND ${condFrag.sqlString}", condFrag.params, condFrag.writer)))

  // BelongsToMany — unconstrained
  def has[T](rel: BelongsToMany[E, T, ?])(f: CountExpr => Frag): QueryBuilder[HasRoot, E, C] =
    val countSql = buildPivotCountSql(rel, None)
    where(f(CountExpr(countSql)))

  // BelongsToMany — constrained
  def has[T, CT <: Selectable](rel: BelongsToMany[E, T, CT], cond: CT => Frag)(f: CountExpr => Frag)(using
      relMeta: TableMeta[T]
  ): QueryBuilder[HasRoot, E, C] =
    val countSql = buildPivotCountSql(rel, Some(relMeta))
    val relCols = new Columns[T](relMeta.columns).asInstanceOf[CT]
    val condFrag = cond(relCols)
    where(f(CountExpr(s"$countSql AND ${condFrag.sqlString}", condFrag.params, condFrag.writer)))

  // --- Private helpers ---

  private def buildExistsFrag(
      fromClause: String,
      correlation: String,
      condition: Option[Frag],
      negate: Boolean
  ): Frag =
    val prefix = if negate then "NOT EXISTS" else "EXISTS"
    condition match
      case None =>
        Frag(s"$prefix (SELECT 1 FROM $fromClause WHERE $correlation)", Seq.empty, FragWriter.empty)
      case Some(cond) =>
        Frag(s"$prefix (SELECT 1 FROM $fromClause WHERE $correlation AND ${cond.sqlString})", cond.params, cond.writer)

  private def buildCountSql(fromClause: String, correlation: String): String =
    s"SELECT COUNT(*) FROM $fromClause WHERE $correlation"

  private def buildRelExistsFrag[T](
      rel: Relationship[E, T],
      relMeta: TableMeta[T],
      condition: Option[Frag],
      negate: Boolean
  ): Frag =
    val correlation = s"${relMeta.tableName}.${rel.pk.sqlName} = ${meta.tableName}.${rel.fk.sqlName}"
    buildExistsFrag(relMeta.tableName, correlation, condition, negate)

  private def buildPivotExistsFrag[T](
      rel: BelongsToMany[E, T, ?],
      conditionWithMeta: Option[(Frag, TableMeta[T])],
      negate: Boolean
  ): Frag =
    val correlation = s"${rel.pivotTable}.${rel.sourceFk} = ${meta.tableName}.${rel.sourcePk.sqlName}"
    val fromClause = conditionWithMeta match
      case None => rel.pivotTable
      case Some((_, tMeta)) =>
        s"${rel.pivotTable} JOIN ${tMeta.tableName} ON ${rel.pivotTable}.${rel.targetFk} = ${tMeta.tableName}.${rel.targetPk.sqlName}"
    buildExistsFrag(fromClause, correlation, conditionWithMeta.map(_._1), negate)

  private def buildRelCountSql[T](
      rel: Relationship[E, T],
      relMeta: TableMeta[T]
  ): String =
    val correlation = s"${relMeta.tableName}.${rel.pk.sqlName} = ${meta.tableName}.${rel.fk.sqlName}"
    buildCountSql(relMeta.tableName, correlation)

  private def buildPivotCountSql[T](
      rel: BelongsToMany[E, T, ?],
      targetMeta: Option[TableMeta[T]]
  ): String =
    val correlation = s"${rel.pivotTable}.${rel.sourceFk} = ${meta.tableName}.${rel.sourcePk.sqlName}"
    val fromClause = targetMeta match
      case None => rel.pivotTable
      case Some(tMeta) =>
        s"${rel.pivotTable} JOIN ${tMeta.tableName} ON ${rel.pivotTable}.${rel.targetFk} = ${tMeta.tableName}.${rel.targetPk.sqlName}"
    buildCountSql(fromClause, correlation)

  def paginate(page: Int, perPage: Int)(using DbCon): OffsetPage[E] =
    if page < 1 then throw QueryBuilderException("page must be >= 1")
    if perPage < 1 then throw QueryBuilderException("perPage must be >= 1")
    val total = count()
    val items = this.limit(perPage).offset((page - 1).toLong * perPage).run()
    OffsetPage(items, total, page, perPage)

  def keysetPaginate[K <: NonEmptyTuple](perPage: Int)(
      f: KeysetColumns[E, C, EmptyTuple] => KeysetColumns[E, C, K]
  ): KeysetPaginator[E, UnwrapSingle[K]] =
    if perPage < 1 then throw QueryBuilderException("perPage must be >= 1")
    val builder = new KeysetColumns[E, C, EmptyTuple](cols, meta, Vector.empty)
    val result = f(builder)
    val entries = result.entries
    val arity = entries.size

    val keyExtractor: E => UnwrapSingle[K] =
      if arity == 1 then
        val idx = entries.head.colIndex
        (e: E) => e.asInstanceOf[Product].productElement(idx).asInstanceOf[UnwrapSingle[K]]
      else
        val indices = entries.map(_.colIndex)
        (e: E) =>
          val prod = e.asInstanceOf[Product]
          val values = indices.map(i => prod.productElement(i).asInstanceOf[AnyRef])
          val tuple = scala.runtime.Tuples.fromIArray(IArray.unsafeFromArray(values.toArray))
          tuple.asInstanceOf[UnwrapSingle[K]]

    val keyToValues: UnwrapSingle[K] => Vector[Any] =
      if arity == 1 then (k: UnwrapSingle[K]) => Vector(k)
      else (k: UnwrapSingle[K]) => k.asInstanceOf[Product].productIterator.toVector

    new KeysetPaginator(meta, codec, rootPredicate, perPage, entries, keyExtractor, keyToValues, None)
  end keysetPaginate

  def chunk(batchSize: Int)(using DbCon): Iterator[Vector[E]] =
    require(batchSize > 0, "batchSize must be positive")
    val startOffset = offsetOpt.getOrElse(0L)
    val maxRows = limitOpt.map(_.toLong).getOrElse(Long.MaxValue)

    new Iterator[Vector[E]]:
      private var currentOffset = startOffset
      private var fetched = 0L
      private var done = false
      private var prefetched: Vector[E] | Null = null

      private def prefetch(): Unit =
        if !done && prefetched == null then
          val remaining = maxRows - fetched
          val fetchSize = math.min(batchSize.toLong, remaining).toInt
          if fetchSize <= 0 then done = true
          else
            val batch = QueryBuilder.this.limit(fetchSize).offset(currentOffset).run()
            if batch.isEmpty then done = true
            else
              prefetched = batch
              fetched += batch.size
              currentOffset += batch.size
              if batch.size < fetchSize then done = true

      override def hasNext: Boolean =
        prefetch()
        !done || prefetched != null

      override def next(): Vector[E] =
        if !hasNext then throw new NoSuchElementException("chunk iterator exhausted")
        val result = prefetched.nn
        prefetched = null
        result
    end new
  end chunk

  def debugPrintSql(using DbCon): this.type =
    val frag = build
    DebugSql.printDebug(Vector(frag))
    this
end QueryBuilder

object QueryBuilder:
  private[magnum] def build0[E, C <: Selectable](
      meta: TableMeta[E],
      codec: DbCodec[E],
      cols: C
  ): QueryBuilder[HasRoot, E, C] =
    new QueryBuilder(meta, codec, cols, None, Vector.empty, None, None)

  transparent inline def from[E](using
      inline meta: TableMeta[E],
      codec: DbCodec[E]
  ): Any = ${ fromImpl[E]('meta, 'codec) }

  private def fromImpl[E: Type](
      meta: Expr[TableMeta[E]],
      codec: Expr[DbCodec[E]]
  )(using Quotes): Expr[Any] =
    import quotes.reflect.*

    Expr.summon[Mirror.ProductOf[E]] match
      case Some('{
            $mirror: Mirror.ProductOf[E] {
              type MirroredElemLabels = eMels
              type MirroredElemTypes = eMets
            }
          }) =>
        val names = elemNames[eMels]()
        val types = elemTypes[eMets]()

        val colsRefinement =
          names.zip(types).foldLeft(TypeRepr.of[Columns[E]]) { case (typeRepr, (name, tpe)) =>
            tpe match
              case '[t] =>
                Refinement(typeRepr, name, TypeRepr.of[Col[t]])
          }

        colsRefinement.asType match
          case '[ct] =>
            '{
              val cols = new Columns[E]($meta.columns).asInstanceOf[ct & Selectable]
              build0[E, ct & Selectable]($meta, $codec, cols)
            }

      case _ =>
        report.errorAndAbort(
          s"A Mirror.ProductOf is required for QueryBuilder.from[${TypeRepr.of[E].show}]"
        )
    end match
  end fromImpl

  transparent inline def fromWithScopes[E](
      scopes: Vector[Scope[E]]
  )(using
      inline meta: TableMeta[E],
      codec: DbCodec[E]
  ): Any = ${ fromWithScopesImpl[E]('meta, 'codec, 'scopes) }

  private def fromWithScopesImpl[E: Type](
      meta: Expr[TableMeta[E]],
      codec: Expr[DbCodec[E]],
      scopes: Expr[Vector[Scope[E]]]
  )(using Quotes): Expr[Any] =
    import quotes.reflect.*

    Expr.summon[Mirror.ProductOf[E]] match
      case Some('{
            $mirror: Mirror.ProductOf[E] {
              type MirroredElemLabels = eMels
              type MirroredElemTypes = eMets
            }
          }) =>
        val names = elemNames[eMels]()
        val types = elemTypes[eMets]()

        val colsRefinement =
          names.zip(types).foldLeft(TypeRepr.of[Columns[E]]) { case (typeRepr, (name, tpe)) =>
            tpe match
              case '[t] =>
                Refinement(typeRepr, name, TypeRepr.of[Col[t]])
          }

        colsRefinement.asType match
          case '[ct] =>
            '{
              val cols = new Columns[E]($meta.columns).asInstanceOf[ct & Selectable]
              val qb = build0[E, ct & Selectable]($meta, $codec, cols)
              $scopes.foldLeft(qb)((q, s) => s.apply(q))
            }

      case _ =>
        report.errorAndAbort(
          s"A Mirror.ProductOf is required for QueryBuilder.fromWithScopes[${TypeRepr.of[E].show}]"
        )
    end match
  end fromWithScopesImpl

end QueryBuilder
