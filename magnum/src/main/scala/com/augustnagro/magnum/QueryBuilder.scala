package com.augustnagro.magnum

import scala.NamedTuple
import scala.deriving.Mirror
import scala.quoted.*
import scala.reflect.TypeTest

sealed trait QBState
sealed trait HasRoot extends QBState

class QueryBuilder[S <: QBState, E, C <: Selectable] private[magnum] (
    private[magnum] val meta: TableMeta[E],
    private val codec: DbCodec[E],
    private[magnum] val cols: C,
    private[magnum] val rootPredicate: Option[Predicate],
    private val orderEntries: Vector[(ColRef[?], SortOrder, NullOrder)],
    private val limitOpt: Option[Int],
    private val offsetOpt: Option[Long],
    private val distinctFlag: Boolean = false
):

  private def addAnd(pred: Predicate): Option[Predicate] =
    QuerySqlBuilder.addAnd(rootPredicate, pred)

  private def addOr(pred: Predicate): Option[Predicate] =
    QuerySqlBuilder.addOr(rootPredicate, pred)

  def where(frag: WhereFrag): QueryBuilder[HasRoot, E, C] =
    new QueryBuilder(meta, codec, cols, addAnd(Predicate.Leaf(frag)), orderEntries, limitOpt, offsetOpt, distinctFlag)

  def where(f: C => WhereFrag): QueryBuilder[HasRoot, E, C] =
    new QueryBuilder(meta, codec, cols, addAnd(Predicate.Leaf(f(cols))), orderEntries, limitOpt, offsetOpt, distinctFlag)

  def orWhere(frag: WhereFrag): QueryBuilder[HasRoot, E, C] =
    new QueryBuilder(meta, codec, cols, addOr(Predicate.Leaf(frag)), orderEntries, limitOpt, offsetOpt, distinctFlag)

  def orWhere(f: C => WhereFrag): QueryBuilder[HasRoot, E, C] =
    new QueryBuilder(meta, codec, cols, addOr(Predicate.Leaf(f(cols))), orderEntries, limitOpt, offsetOpt, distinctFlag)

  def whereGroup(
      f: PredicateGroupBuilder[C] => PredicateGroupBuilder[C]
  ): QueryBuilder[HasRoot, E, C] =
    new QueryBuilder(meta, codec, cols, addAnd(f(PredicateGroupBuilder.empty(cols)).build), orderEntries, limitOpt, offsetOpt, distinctFlag)

  def orWhereGroup(
      f: PredicateGroupBuilder[C] => PredicateGroupBuilder[C]
  ): QueryBuilder[HasRoot, E, C] =
    new QueryBuilder(meta, codec, cols, addOr(f(PredicateGroupBuilder.empty(cols)).build), orderEntries, limitOpt, offsetOpt, distinctFlag)

  def orderBy(f: C => ColRef[?], order: SortOrder = SortOrder.Asc, nullOrder: NullOrder = NullOrder.Default): QueryBuilder[S, E, C] =
    new QueryBuilder(meta, codec, cols, rootPredicate, orderEntries :+ (f(cols), order, nullOrder), limitOpt, offsetOpt, distinctFlag)

  def orderBy(f: C => ColRef[?]): QueryBuilder[S, E, C] =
    new QueryBuilder(meta, codec, cols, rootPredicate, orderEntries :+ (f(cols), SortOrder.Asc, NullOrder.Default), limitOpt, offsetOpt, distinctFlag)

  def limit(n: Int): QueryBuilder[S, E, C] =
    if n < 0 then throw QueryBuilderException("limit must not be negative")
    new QueryBuilder(meta, codec, cols, rootPredicate, orderEntries, Some(n), offsetOpt, distinctFlag)

  def offset(n: Long): QueryBuilder[S, E, C] =
    if n < 0 then throw QueryBuilderException("offset must not be negative")
    new QueryBuilder(meta, codec, cols, rootPredicate, orderEntries, limitOpt, Some(n), distinctFlag)

  def distinct: QueryBuilder[S, E, C] =
    new QueryBuilder(meta, codec, cols, rootPredicate, orderEntries, limitOpt, offsetOpt, true)

  def lockForUpdate: LockedQueryBuilder[S, E, C] =
    LockedQueryBuilder(meta, codec, cols, rootPredicate, orderEntries, limitOpt, offsetOpt, distinctFlag, LockMode.ForUpdate)

  def forShare: LockedQueryBuilder[S, E, C] =
    LockedQueryBuilder(meta, codec, cols, rootPredicate, orderEntries, limitOpt, offsetOpt, distinctFlag, LockMode.ForShare)

  private def buildWhere: (String, Seq[Any], FragWriter) =
    QuerySqlBuilder.buildWhere(rootPredicate)

  def build: Frag =
    val selectCols = meta.columns.map(_.sqlName).mkString(", ")
    val keyword = if distinctFlag then "SELECT DISTINCT" else "SELECT"
    val baseSql = s"$keyword $selectCols FROM ${meta.tableName}"

    val (whereSql, params, writer) = buildWhere
    val orderBySql = QuerySqlBuilder.buildOrderBy(orderEntries)
    val limitOffsetSql = QuerySqlBuilder.buildLimitOffset(limitOpt, offsetOpt)

    Frag(baseSql + whereSql + orderBySql + limitOffsetSql, params, writer)

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

  def select: QueryBuilder.SelectPhase[C] =
    QueryBuilder.SelectPhase(meta.tableName, cols, rootPredicate)

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

  def withRelated[T, CT <: Selectable](rel: HasMany[E, T, CT])(f: CT => WhereFrag)(using
      childMeta: TableMeta[T],
      childCodec: DbCodec[T]
  ): EagerQuery[E, Vector[T] *: EmptyTuple] =
    val relCols = new Columns[T](childMeta.columns).asInstanceOf[CT]
    val d = DirectEagerDef(meta, rel, childMeta, childCodec, Some(f(relCols)))
    EagerQuery(build, codec, meta, Vector(d))

  def withRelated[T, CT <: Selectable](rel: BelongsToMany[E, T, CT])(f: CT => WhereFrag)(using
      targetMeta: TableMeta[T],
      targetCodec: DbCodec[T]
  ): EagerQuery[E, Vector[T] *: EmptyTuple] =
    val relCols = new Columns[T](targetMeta.columns).asInstanceOf[CT]
    val d = PivotEagerDef(meta, rel, targetMeta, targetCodec, Some(f(relCols)))
    EagerQuery(build, codec, meta, Vector(d))

  def withRelated[T, CT <: Selectable](rel: HasManyThrough[E, T, CT])(f: CT => WhereFrag)(using
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

  def withRelated[T, CT <: Selectable](rel: HasOneThrough[E, T, CT])(f: CT => WhereFrag)(using
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

  def withRelated[I, T, CT <: Selectable](rel: ComposedRelationship[E, I, T, CT])(f: CT => WhereFrag)(using
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

  def withCount[T, CT <: Selectable](rel: HasMany[E, T, CT])(f: CT => WhereFrag)(using
      relMeta: TableMeta[T]
  ): CountQuery[E] =
    val subSql = buildRelCountSql(rel, relMeta)
    val cols = new Columns[T](relMeta.columns).asInstanceOf[CT]
    new CountQuery(meta, codec, subSql, Some(f(cols)), rootPredicate, orderEntries, limitOpt, offsetOpt)

  // --- withCount for BelongsToMany ---

  def withCount[T](rel: BelongsToMany[E, T, ?]): CountQuery[E] =
    val subSql = buildPivotCountSql(rel, None)
    new CountQuery(meta, codec, subSql, None, rootPredicate, orderEntries, limitOpt, offsetOpt)

  def withCount[T, CT <: Selectable](rel: BelongsToMany[E, T, CT])(f: CT => WhereFrag)(using
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

  def whereHas[T, CT <: Selectable](rel: HasMany[E, T, CT])(f: CT => WhereFrag)(using
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

  def whereHas[T, CT <: Selectable](rel: BelongsToMany[E, T, CT])(f: CT => WhereFrag)(using
      relMeta: TableMeta[T]
  ): QueryBuilder[HasRoot, E, C] =
    val cols = new Columns[T](relMeta.columns).asInstanceOf[CT]
    where(buildPivotExistsFrag(rel, Some((f(cols), relMeta)), negate = false))

  def doesntHave[T](rel: BelongsToMany[E, T, ?]): QueryBuilder[HasRoot, E, C] =
    where(buildPivotExistsFrag(rel, None, negate = true))

  // --- orWhereHas / orDoesntHave for Relationship ---

  def orWhereHas[T](rel: Relationship[E, T])(using relMeta: TableMeta[T]): QueryBuilder[HasRoot, E, C] =
    orWhere(buildRelExistsFrag(rel, relMeta, None, negate = false))

  def orWhereHas[T](rel: HasMany[E, T, ?])(using relMeta: TableMeta[T]): QueryBuilder[HasRoot, E, C] =
    orWhere(buildRelExistsFrag(rel, relMeta, None, negate = false))

  def orWhereHas[T, CT <: Selectable](rel: HasMany[E, T, CT])(f: CT => WhereFrag)(using relMeta: TableMeta[T]): QueryBuilder[HasRoot, E, C] =
    val cols = new Columns[T](relMeta.columns).asInstanceOf[CT]
    orWhere(buildRelExistsFrag(rel, relMeta, Some(f(cols)), negate = false))

  def orDoesntHave[T](rel: HasMany[E, T, ?])(using relMeta: TableMeta[T]): QueryBuilder[HasRoot, E, C] =
    orWhere(buildRelExistsFrag(rel, relMeta, None, negate = true))

  def orDoesntHave[T](rel: Relationship[E, T])(using relMeta: TableMeta[T]): QueryBuilder[HasRoot, E, C] =
    orWhere(buildRelExistsFrag(rel, relMeta, None, negate = true))

  // --- orWhereHas / orDoesntHave for BelongsToMany ---

  def orWhereHas[T](rel: BelongsToMany[E, T, ?]): QueryBuilder[HasRoot, E, C] =
    orWhere(buildPivotExistsFrag(rel, None, negate = false))

  def orWhereHas[T, CT <: Selectable](rel: BelongsToMany[E, T, CT])(f: CT => WhereFrag)(using relMeta: TableMeta[T]): QueryBuilder[HasRoot, E, C] =
    val cols = new Columns[T](relMeta.columns).asInstanceOf[CT]
    orWhere(buildPivotExistsFrag(rel, Some((f(cols), relMeta)), negate = false))

  def orDoesntHave[T](rel: BelongsToMany[E, T, ?]): QueryBuilder[HasRoot, E, C] =
    orWhere(buildPivotExistsFrag(rel, None, negate = true))

  // --- has with count threshold ---

  // HasMany — unconstrained
  def has[T](rel: HasMany[E, T, ?])(f: CountExpr => WhereFrag)(using
      relMeta: TableMeta[T]
  ): QueryBuilder[HasRoot, E, C] =
    val countSql = buildRelCountSql(rel, relMeta)
    where(f(CountExpr(countSql)))

  // HasMany — constrained
  def has[T, CT <: Selectable](rel: HasMany[E, T, CT], cond: CT => WhereFrag)(f: CountExpr => WhereFrag)(using
      relMeta: TableMeta[T]
  ): QueryBuilder[HasRoot, E, C] =
    val countSql = buildRelCountSql(rel, relMeta)
    val relCols = new Columns[T](relMeta.columns).asInstanceOf[CT]
    val condFrag = cond(relCols)
    where(f(CountExpr(s"$countSql AND ${condFrag.sqlString}", condFrag.params, condFrag.writer)))

  // BelongsToMany — unconstrained
  def has[T](rel: BelongsToMany[E, T, ?])(f: CountExpr => WhereFrag): QueryBuilder[HasRoot, E, C] =
    val countSql = buildPivotCountSql(rel, None)
    where(f(CountExpr(countSql)))

  // BelongsToMany — constrained
  def has[T, CT <: Selectable](rel: BelongsToMany[E, T, CT], cond: CT => WhereFrag)(f: CountExpr => WhereFrag)(using
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
      condition: Option[WhereFrag],
      negate: Boolean
  ): WhereFrag =
    val prefix = if negate then "NOT EXISTS" else "EXISTS"
    condition match
      case None =>
        WhereFrag(Frag(s"$prefix (SELECT 1 FROM $fromClause WHERE $correlation)", Seq.empty, FragWriter.empty))
      case Some(cond) =>
        WhereFrag(Frag(s"$prefix (SELECT 1 FROM $fromClause WHERE $correlation AND ${cond.sqlString})", cond.params, cond.writer))

  private def buildCountSql(fromClause: String, correlation: String): String =
    s"SELECT COUNT(*) FROM $fromClause WHERE $correlation"

  private def buildRelExistsFrag[T](
      rel: Relationship[E, T],
      relMeta: TableMeta[T],
      condition: Option[WhereFrag],
      negate: Boolean
  ): WhereFrag =
    val correlation = s"${relMeta.tableName}.${rel.pk.sqlName} = ${meta.tableName}.${rel.fk.sqlName}"
    buildExistsFrag(relMeta.tableName, correlation, condition, negate)

  private def buildPivotExistsFrag[T](
      rel: BelongsToMany[E, T, ?],
      conditionWithMeta: Option[(WhereFrag, TableMeta[T])],
      negate: Boolean
  ): WhereFrag =
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

  // --- Mutation operations (§5.1, §5.2, §5.3) ---

  def update: QueryBuilder.UpdatePhase[E] =
    QueryBuilder.UpdatePhase(this)

  def delete()(using DbCon): Int =
    val (whereSql, params, writer) = buildWhere
    Frag(s"DELETE FROM ${meta.tableName}$whereSql", params, writer).update.run()

  def updateUnsafe(assignments: Frag)(using DbCon): Int =
    val (whereSql, whereParams, whereWriter) = buildWhere
    val sql = s"UPDATE ${meta.tableName} SET ${assignments.sqlString}$whereSql"
    val allParams = assignments.params ++ whereParams
    val combinedWriter: FragWriter = (ps, pos) =>
      val next = assignments.writer.write(ps, pos)
      whereWriter.write(ps, next)
    Frag(sql, allParams, combinedWriter).update.run()

  def updateUnsafe(f: C => Frag)(using DbCon): Int =
    updateUnsafe(f(cols))

  def increment[A](f: C => ColRef[A], amount: A | None.type = None)(using num: Numeric[A], codec: DbCodec[A], tt: TypeTest[A | None.type, A], con: DbCon): Int =
    val actual: A = amount match
      case None    => num.one
      case a: A    => a
    val col = f(cols)
    val writer: FragWriter = (ps, pos) =>
      codec.writeSingle(actual, ps, pos)
      pos + codec.cols.length
    updateUnsafe(Frag(s"${col.queryRepr} = ${col.queryRepr} + ?", Seq(actual), writer))

  def decrement[A](f: C => ColRef[A], amount: A | None.type = None)(using num: Numeric[A], codec: DbCodec[A], tt: TypeTest[A | None.type, A], con: DbCon): Int =
    val actual: A = amount match
      case None    => num.one
      case a: A    => a
    val col = f(cols)
    val writer: FragWriter = (ps, pos) =>
      codec.writeSingle(actual, ps, pos)
      pos + codec.cols.length
    updateUnsafe(Frag(s"${col.queryRepr} = ${col.queryRepr} - ?", Seq(amount), writer))

  def debugPrintSql(using DbCon): this.type =
    val frag = build
    DebugSql.printDebug(Vector(frag))
    this
end QueryBuilder

object QueryBuilder:
  /** Intermediate object for the select() → ProjectedQuery transition.
    * Created by QueryBuilder.select, holds the frozen WHERE + column proxy.
    * The transparent inline apply method triggers the macro.
    */
  class SelectPhase[C](val tableName: String, val cols: C, val predicate: Option[Predicate]):
    transparent inline def apply[P](inline f: C => P): Any =
      ${ selectImpl[C, P]('this, 'f) }

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

  // --- select() macro implementation ---

  private def selectImpl[C: Type, P: Type](
      phaseExpr: Expr[SelectPhase[C]],
      f: Expr[C => P]
  )(using Quotes): Expr[Any] =
    import quotes.reflect.*

    // 1. Decompose P as NamedTuple[N, V]
    val ntSym = Symbol.requiredModule("scala.NamedTuple").typeMember("NamedTuple")
    val pRepr = TypeRepr.of[P].dealias

    val (namesRepr, valuesRepr) = pRepr match
      case AppliedType(tycon, List(n, v)) if tycon.typeSymbol == ntSym =>
        (n, v)
      case _ =>
        report.errorAndAbort(
          s"select() requires a named tuple return type, got: ${pRepr.show}"
        )

    // 2. Extract field names from the names tuple type
    val fieldNames: List[String] = namesRepr.asType match
      case '[nmes] => selectFieldNames[nmes]()

    // 3. Extract and analyze element types
    val elemInfos: List[(String, Type[?], Boolean)] = valuesRepr.asType match
      case '[vals] => selectAnalyzeElements[vals](fieldNames)

    // 4. Build result named tuple type (unwrapped inner types)
    val resultTypes: List[Type[?]] = elemInfos.map(_._2)
    val resultNtType = selectPackNamedTuple(fieldNames, resultTypes)

    // 5. Build PC named tuple type (all SelectExpr[A])
    val pcTypes: List[Type[?]] = elemInfos.map { case (_, innerTpe, _) =>
      innerTpe match { case '[a] => Type.of[SelectExpr[a]] }
    }
    val pcNtType = selectPackNamedTuple(fieldNames, pcTypes)

    // 6. Generate codec expressions for Col elements (summon DbCodec[A])
    val codecExprs: List[Expr[DbCodec[?]]] = elemInfos.map { case (name, innerTpe, isSE) =>
      innerTpe match
        case '[a] =>
          if isSE then
            '{ null.asInstanceOf[DbCodec[?]] } // placeholder; runtime uses SelectExpr.codec
          else
            Expr.summon[DbCodec[a]] match
              case Some(c) => '{ $c.asInstanceOf[DbCodec[?]] }
              case None =>
                report.errorAndAbort(
                  s"No DbCodec found for type ${TypeRepr.of[a].show} (field '$name')"
                )
    }

    val fieldNameExprs: Expr[Seq[String]] = Expr.ofSeq(fieldNames.map(Expr.apply))
    val codecsSeqExpr: Expr[Seq[DbCodec[?]]] = Expr.ofSeq(codecExprs)
    val nExpr = Expr(elemInfos.length)

    // 7. Generate the final expression
    (resultNtType, pcNtType) match
      case ('[resultT], '[pcT]) =>
        '{
          val phase = $phaseExpr
          val rawTuple = $f(phase.cols).asInstanceOf[Product]
          val names = $fieldNameExprs
          val fallbackCodecs = $codecsSeqExpr
          val n = $nExpr

          val selectExprsArr = new Array[SelectExpr[?]](n)
          var i = 0
          while i < n do
            val elem = rawTuple.productElement(i)
            val alias = names(i)
            elem match
              case se: SelectExpr[?] =>
                selectExprsArr(i) = new SelectExpr(se.queryRepr, alias, se.codec)
              case col: ColRef[?] =>
                selectExprsArr(i) = new SelectExpr(col.queryRepr, alias, fallbackCodecs(i))
            i += 1

          val codecArr = IArray.from(selectExprsArr.map(_.codec))
          val resultCodec = ProjectedQuery.positionalCodec[resultT](codecArr)

          val pcArray = selectExprsArr.map(_.asInstanceOf[Object])
          val pc = Tuple.fromArray(pcArray).asInstanceOf[pcT]

          new ProjectedQuery[resultT, pcT](
            s"FROM ${phase.tableName}",
            Seq.empty,
            FragWriter.empty,
            selectExprsArr.toVector,
            resultCodec,
            pc,
            phase.predicate,
            Vector.empty,
            None,
            Vector.empty,
            None,
            None
          )
        }
      case _ =>
        report.errorAndAbort("select() failed to construct result types. This is a bug in magnum.")
  end selectImpl

  // --- select() macro helper: extract field names from a tuple of string literal types ---
  private[magnum] def selectFieldNames[N: Type](res: List[String] = Nil)(using Quotes): List[String] =
    import quotes.reflect.*
    Type.of[N] match
      case '[n *: ns] =>
        val name = TypeRepr.of[n] match
          case ConstantType(StringConstant(s)) => s
          case other =>
            report.errorAndAbort(s"Expected string literal type in named tuple, got ${other.show}")
        selectFieldNames[ns](name :: res)
      case '[EmptyTuple] => res.reverse

  // --- select() macro helper: analyze element types ---
  // Returns (fieldName, innerType, isSelectExpr)
  private[magnum] def selectAnalyzeElements[V: Type](names: List[String])(using Quotes): List[(String, Type[?], Boolean)] =
    import quotes.reflect.*
    def loop[T: Type](remainingNames: List[String]): List[(String, Type[?], Boolean)] =
      Type.of[T] match
        case '[v *: vs] =>
          val name = remainingNames.head
          val (innerType, isSE) = Type.of[v] match
            case '[SelectExpr[a]] => (Type.of[a], true)
            case '[Col[a]]       => (Type.of[a], false)
            case '[ColRef[a]]    => (Type.of[a], false)
            case _ =>
              report.errorAndAbort(
                s"select() element '$name' must be Col[A] or SelectExpr[A], got ${TypeRepr.of[v].show}"
              )
          (name, innerType, isSE) :: loop[vs](remainingNames.tail)
        case '[EmptyTuple] => Nil
    loop[V](names)

  // --- select() macro helper: build a tuple type from a list of types ---
  private[magnum] def selectListToTupleType(ts: List[Type[?]])(using Quotes): Type[?] =
    ts.foldRight(Type.of[EmptyTuple]: Type[?]) { (t, acc) =>
      (t, acc) match
        case ('[ft], '[type acc <: Tuple; `acc`]) =>
          Type.of[ft *: acc]
        case _ =>
          quotes.reflect.report.errorAndAbort("Failed to build tuple type from select() elements. This is a bug in magnum.")
    }

  // --- select() macro helper: build a names-tuple type from string literals ---
  private[magnum] def selectStringsToTupleType(names: List[String])(using Quotes): Type[?] =
    import quotes.reflect.*
    selectListToTupleType(names.map(n => ConstantType(StringConstant(n)).asType))

  // --- select() macro helper: assemble a NamedTuple type ---
  private[magnum] def selectPackNamedTuple(names: List[String], types: List[Type[?]])(using Quotes): Type[?] =
    import quotes.reflect.*
    val nmesTpe = selectStringsToTupleType(names)
    val valsTpe = selectListToTupleType(types)
    (nmesTpe, valsTpe) match
      case ('[type nmes <: Tuple; `nmes`], '[type tps <: Tuple; `tps`]) =>
        Type.of[NamedTuple.NamedTuple[nmes, tps]]
      case _ =>
        report.errorAndAbort("Failed to construct NamedTuple type for select(). This is a bug in magnum.")

  class UpdatePhase[E] private[magnum] (
      private[magnum] val qb: QueryBuilder[?, E, ?]
  ):
    inline def apply[P](inline assignments: P)(using inline con: DbCon): Int =
      ${ updateImpl[E, P]('this, 'assignments, 'con) }

  // --- update() macro implementation ---

  private def updateImpl[E: Type, P: Type](
      phaseExpr: Expr[UpdatePhase[E]],
      assignments: Expr[P],
      conExpr: Expr[DbCon]
  )(using Quotes): Expr[Int] =
    import quotes.reflect.*

    // 1. Decompose P as NamedTuple[N, V]
    val ntSym = Symbol.requiredModule("scala.NamedTuple").typeMember("NamedTuple")
    val pRepr = TypeRepr.of[P].dealias

    val (namesRepr, valuesRepr) = pRepr match
      case AppliedType(tycon, List(n, v)) if tycon.typeSymbol == ntSym =>
        (n, v)
      case _ =>
        report.errorAndAbort(
          s"update() requires a named tuple, got: ${pRepr.show}"
        )

    // 2. Extract field names from the names tuple type
    val fieldNames: List[String] = namesRepr.asType match
      case '[nmes] => selectFieldNames[nmes]()

    // 3. Extract value types from the values tuple type
    val valueTypes: List[Type[?]] = valuesRepr.asType match
      case '[vals] => elemTypes[vals]()

    // 4. Get entity field names and types from Mirror
    val entityFields: Map[String, TypeRepr] =
      Expr.summon[Mirror.ProductOf[E]] match
        case Some('{
              $mirror: Mirror.ProductOf[E] {
                type MirroredElemLabels = eMels
                type MirroredElemTypes = eMets
              }
            }) =>
          val names = elemNames[eMels]()
          val types = elemTypes[eMets]()
          names.zip(types.map { case '[t] => TypeRepr.of[t] }).toMap
        case _ =>
          report.errorAndAbort(
            s"A Mirror.ProductOf is required for update() on ${TypeRepr.of[E].show}"
          )

    // 5. Validate each field name exists on E and type-check values
    fieldNames.zip(valueTypes).foreach { case (fieldName, valType) =>
      entityFields.get(fieldName) match
        case None =>
          val available = entityFields.keys.toList.sorted.mkString(", ")
          report.errorAndAbort(
            s"Field '$fieldName' does not exist on entity. Available columns: $available"
          )
        case Some(entityFieldType) =>
          valType match
            case '[v] =>
              if !(TypeRepr.of[v] <:< entityFieldType) then
                report.errorAndAbort(
                  s"Type mismatch for field '$fieldName': expected ${entityFieldType.show}, got ${TypeRepr.of[v].show}"
                )
    }

    // 6. Summon DbCodec for each value type
    val codecExprs: List[Expr[DbCodec[?]]] = fieldNames.zip(valueTypes).map { case (name, valType) =>
      valType match
        case '[a] =>
          Expr.summon[DbCodec[a]] match
            case Some(c) => '{ $c.asInstanceOf[DbCodec[?]] }
            case None =>
              report.errorAndAbort(
                s"No DbCodec found for type ${TypeRepr.of[a].show} (field '$name')"
              )
    }

    // 7. Generate runtime code
    val fieldNameExprs: Expr[Seq[String]] = Expr.ofSeq(fieldNames.map(Expr.apply))
    val codecsSeqExpr: Expr[Seq[DbCodec[?]]] = Expr.ofSeq(codecExprs)
    val nExpr = Expr(fieldNames.length)

    '{
      val phase = $phaseExpr
      val qb = phase.qb
      val rawTuple = $assignments.asInstanceOf[Product]
      val fieldNamesRT = $fieldNameExprs
      val codecs = $codecsSeqExpr
      val n = $nExpr

      val sb = new StringBuilder()
      val params = new scala.collection.mutable.ArrayBuffer[Any](n)
      var i = 0
      while i < n do
        if i > 0 then sb.append(", ")
        val col = qb.meta.columnByName(fieldNamesRT(i)).get
        sb.append(col.sqlName)
        sb.append(" = ?")
        params += rawTuple.productElement(i)
        i += 1

      val writer: FragWriter = (ps, pos) =>
        var p = pos
        var j = 0
        while j < n do
          codecs(j).asInstanceOf[DbCodec[Any]].writeSingle(rawTuple.productElement(j), ps, p)
          p += codecs(j).cols.length
          j += 1
        p

      qb.updateUnsafe(Frag(sb.toString, params.toSeq, writer))(using $conExpr)
    }
  end updateImpl

end QueryBuilder
