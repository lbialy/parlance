package ma.chinespirit.parlance

import scala.NamedTuple
import scala.deriving.Mirror
import scala.quoted.*
import scala.reflect.TypeTest
import scala.util.Using

sealed trait QBState
sealed trait HasRoot extends QBState

class QueryBuilder[S <: QBState, E, C <: Selectable, P <: ScopePolicy] private[parlance] (
    private[parlance] val meta: TableMeta[E],
    private val codec: DbCodec[E],
    private[parlance] val cols: C,
    private[parlance] val rootPredicate: Option[Predicate],
    private val orderEntries: Vector[(ColRef[?], SortOrder, NullOrder)],
    private val limitOpt: Option[Int],
    private val offsetOpt: Option[Long],
    private val distinctFlag: Boolean = false,
    private val rawOrderFrags: Vector[OrderByFrag] = Vector.empty
):

  private def addAnd(pred: Predicate): Option[Predicate] =
    QuerySqlBuilder.addAnd(rootPredicate, pred)

  private def addOr(pred: Predicate): Option[Predicate] =
    QuerySqlBuilder.addOr(rootPredicate, pred)

  def where(frag: WhereFrag): QueryBuilder[HasRoot, E, C, P] =
    new QueryBuilder(meta, codec, cols, addAnd(Predicate.Leaf(frag)), orderEntries, limitOpt, offsetOpt, distinctFlag, rawOrderFrags)

  def where(f: SubQuery[E, C, P] => WhereFrag): QueryBuilder[HasRoot, E, C, P] =
    val sq = new SubQuery[E, C, P](meta, cols)
    new QueryBuilder(meta, codec, cols, addAnd(Predicate.Leaf(f(sq))), orderEntries, limitOpt, offsetOpt, distinctFlag, rawOrderFrags)

  def orWhere(frag: WhereFrag): QueryBuilder[HasRoot, E, C, P] =
    new QueryBuilder(meta, codec, cols, addOr(Predicate.Leaf(frag)), orderEntries, limitOpt, offsetOpt, distinctFlag, rawOrderFrags)

  def orWhere(f: SubQuery[E, C, P] => WhereFrag): QueryBuilder[HasRoot, E, C, P] =
    val sq = new SubQuery[E, C, P](meta, cols)
    new QueryBuilder(meta, codec, cols, addOr(Predicate.Leaf(f(sq))), orderEntries, limitOpt, offsetOpt, distinctFlag, rawOrderFrags)

  def orderBy(f: C => ColRef[?], order: SortOrder = SortOrder.Asc, nullOrder: NullOrder = NullOrder.Default): QueryBuilder[S, E, C, P] =
    new QueryBuilder(
      meta,
      codec,
      cols,
      rootPredicate,
      orderEntries :+ (f(cols), order, nullOrder),
      limitOpt,
      offsetOpt,
      distinctFlag,
      rawOrderFrags
    )

  def orderBy(f: C => ColRef[?]): QueryBuilder[S, E, C, P] =
    new QueryBuilder(
      meta,
      codec,
      cols,
      rootPredicate,
      orderEntries :+ (f(cols), SortOrder.Asc, NullOrder.Default),
      limitOpt,
      offsetOpt,
      distinctFlag,
      rawOrderFrags
    )

  def orderBy(frag: OrderByFrag): QueryBuilder[S, E, C, P] =
    new QueryBuilder(meta, codec, cols, rootPredicate, orderEntries, limitOpt, offsetOpt, distinctFlag, rawOrderFrags :+ frag)

  def limit(n: Int): QueryBuilder[S, E, C, P] =
    if n < 0 then throw QueryBuilderException("limit must not be negative")
    new QueryBuilder(meta, codec, cols, rootPredicate, orderEntries, Some(n), offsetOpt, distinctFlag, rawOrderFrags)

  def offset(n: Long): QueryBuilder[S, E, C, P] =
    if n < 0 then throw QueryBuilderException("offset must not be negative")
    new QueryBuilder(meta, codec, cols, rootPredicate, orderEntries, limitOpt, Some(n), distinctFlag, rawOrderFrags)

  def distinct: QueryBuilder[S, E, C, P] =
    new QueryBuilder(meta, codec, cols, rootPredicate, orderEntries, limitOpt, offsetOpt, true, rawOrderFrags)

  def lockForUpdate(using DbTx[? <: SupportsRowLocks]): LockedQueryBuilder[S, E, C] =
    LockedQueryBuilder(meta, codec, cols, rootPredicate, orderEntries, limitOpt, offsetOpt, distinctFlag, LockMode.ForUpdate, rawOrderFrags)

  def forShare(using DbTx[? <: SupportsForShare]): LockedQueryBuilder[S, E, C] =
    LockedQueryBuilder(meta, codec, cols, rootPredicate, orderEntries, limitOpt, offsetOpt, distinctFlag, LockMode.ForShare, rawOrderFrags)

  private def buildWhere: (String, Seq[Any], FragWriter) =
    QuerySqlBuilder.buildWhere(rootPredicate)

  def build(using con: DbCon[?]): Frag =
    buildWith(con.databaseType)

  def buildWith(dt: DatabaseType): Frag =
    val selectCols = meta.columns.map(_.sqlName).mkString(", ")
    val keyword = if distinctFlag then "SELECT DISTINCT" else "SELECT"
    val baseSql = s"$keyword $selectCols FROM ${meta.tableName}"

    val (whereSql, whereParams, whereWriter) = buildWhere
    val (orderBySql, orderParams, orderWriter) = QuerySqlBuilder.buildOrderBy(orderEntries, rawOrderFrags)
    val limitOffsetSql = QuerySqlBuilder.buildLimitOffset(limitOpt, offsetOpt, dt)

    val allParams = whereParams ++ orderParams
    val combinedWriter: FragWriter =
      if orderParams.isEmpty then whereWriter
      else
        (ps, pos) =>
          val next = whereWriter.write(ps, pos)
          orderWriter.write(ps, next)

    Frag(baseSql + whereSql + orderBySql + limitOffsetSql, allParams, combinedWriter)

  def run()(using DbCon[?]): Vector[E] =
    build.query[E](using codec).run()

  def first()(using DbCon[?]): Option[E] =
    limit(1).run().headOption

  def firstOrFail()(using DbCon[?]): E =
    first().getOrElse(
      throw QueryBuilderException(
        s"No ${meta.tableName} found matching query"
      )
    )

  def count()(using DbCon[?]): Long =
    val baseSql = s"SELECT COUNT(*) FROM ${meta.tableName}"
    val (whereSql, params, writer) = buildWhere
    QueryBuilderException.requireNonEmpty(
      Frag(baseSql + whereSql, params, writer).query[Long].run(),
      s"Aggregate query on ${meta.tableName}"
    )

  def exists()(using DbCon[?]): Boolean =
    val innerSql = s"SELECT 1 FROM ${meta.tableName}"
    val (whereSql, params, writer) = buildWhere
    QueryBuilderException.requireNonEmpty(
      Frag(s"SELECT EXISTS($innerSql$whereSql)", params, writer).query[Boolean].run(),
      s"Aggregate query on ${meta.tableName}"
    )

  def sum[A](f: C => ColRef[A])(using DbCodec[A], DbCon[?]): Option[A] =
    runAgg[Option[A]]("SUM", f(cols).queryRepr)

  def avg(f: C => ColRef[?])(using DbCon[?]): Option[Double] =
    runAgg[Option[Double]]("AVG", f(cols).queryRepr)

  def min[A](f: C => ColRef[A])(using DbCodec[A], DbCon[?]): Option[A] =
    runAgg[Option[A]]("MIN", f(cols).queryRepr)

  def max[A](f: C => ColRef[A])(using DbCodec[A], DbCon[?]): Option[A] =
    runAgg[Option[A]]("MAX", f(cols).queryRepr)

  def count(f: C => ColRef[?])(using DbCon[?]): Long =
    runAgg[Long]("COUNT", f(cols).queryRepr)(using DbCodec.LongCodec)

  private def runAgg[R](fn: String, colRepr: String)(using DbCodec[R], DbCon[?]): R =
    val baseSql = s"SELECT $fn($colRepr) FROM ${meta.tableName}"
    val (whereSql, params, writer) = buildWhere
    QueryBuilderException.requireNonEmpty(
      Frag(baseSql + whereSql, params, writer).query[R].run(),
      s"Aggregate query on ${meta.tableName}"
    )

  def select: QueryBuilder.SelectPhase[C] =
    QueryBuilder.SelectPhase(meta.tableName, cols, rootPredicate)

  /** Merge scope conditions with an optional user filter for eager loading. */
  private def mergeEagerFilter[T](userFilter: Option[Frag], targetMeta: TableMeta[T], scopes: Vector[Scope[T]]): Option[Frag] =
    val scopeFrag = ExistsBuilder.scopeConditions(scopes, targetMeta).map(f => f: Frag)
    (userFilter, scopeFrag) match
      case (None, None)    => None
      case (Some(u), None) => Some(u)
      case (None, Some(s)) => Some(s)
      case (Some(u), Some(s)) =>
        val sql = s"${u.sqlString} AND ${s.sqlString}"
        val params = u.params ++ s.params
        val writer: FragWriter = (ps, pos) =>
          val next = u.writer.write(ps, pos)
          s.writer.write(ps, next)
        Some(Frag(sql, params, writer))

  def withRelated[T](rel: HasMany[E, T, ?])(using
      childMeta: TableMeta[T],
      childCodec: DbCodec[T],
      resolve: ResolveScopes[T, P]
  ): EagerQuery[E, Vector[T] *: EmptyTuple, P] =
    val d = DirectEagerDef(meta, rel, childMeta, childCodec, mergeEagerFilter(None, childMeta, resolve.scopes))
    EagerQuery(buildWith, codec, meta, Vector(d))

  def withRelated[T](rel: BelongsToMany[E, T, ?])(using
      targetMeta: TableMeta[T],
      targetCodec: DbCodec[T],
      resolve: ResolveScopes[T, P]
  ): EagerQuery[E, Vector[T] *: EmptyTuple, P] =
    val d = PivotEagerDef(meta, rel, targetMeta, targetCodec, mergeEagerFilter(None, targetMeta, resolve.scopes))
    EagerQuery(buildWith, codec, meta, Vector(d))

  def withRelated[T](rel: HasManyThrough[E, T, ?])(using
      targetMeta: TableMeta[T],
      targetCodec: DbCodec[T],
      resolve: ResolveScopes[T, P]
  ): EagerQuery[E, Vector[T] *: EmptyTuple, P] =
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
      mergeEagerFilter(None, targetMeta, resolve.scopes)
    )
    EagerQuery(buildWith, codec, meta, Vector(d))

  def withRelated[T](rel: HasOneThrough[E, T, ?])(using
      targetMeta: TableMeta[T],
      targetCodec: DbCodec[T],
      resolve: ResolveScopes[T, P]
  ): EagerQuery[E, Vector[T] *: EmptyTuple, P] =
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
      mergeEagerFilter(None, targetMeta, resolve.scopes)
    )
    EagerQuery(buildWith, codec, meta, Vector(d))

  // --- Constrained withRelated ---

  def withRelated[T, CT <: Selectable](rel: HasMany[E, T, CT])(f: SubQuery[T, CT, P] => WhereFrag)(using
      childMeta: TableMeta[T],
      childCodec: DbCodec[T],
      resolve: ResolveScopes[T, P]
  ): EagerQuery[E, Vector[T] *: EmptyTuple, P] =
    val relCols = new Columns[T](childMeta.columns).asInstanceOf[CT]
    val sq = new SubQuery[T, CT, P](childMeta, relCols)
    val d = DirectEagerDef(meta, rel, childMeta, childCodec, mergeEagerFilter(Some(f(sq)), childMeta, resolve.scopes))
    EagerQuery(buildWith, codec, meta, Vector(d))

  def withRelated[T, CT <: Selectable](rel: BelongsToMany[E, T, CT])(f: SubQuery[T, CT, P] => WhereFrag)(using
      targetMeta: TableMeta[T],
      targetCodec: DbCodec[T],
      resolve: ResolveScopes[T, P]
  ): EagerQuery[E, Vector[T] *: EmptyTuple, P] =
    val relCols = new Columns[T](targetMeta.columns).asInstanceOf[CT]
    val sq = new SubQuery[T, CT, P](targetMeta, relCols)
    val d = PivotEagerDef(meta, rel, targetMeta, targetCodec, mergeEagerFilter(Some(f(sq)), targetMeta, resolve.scopes))
    EagerQuery(buildWith, codec, meta, Vector(d))

  def withRelated[T, CT <: Selectable](rel: HasManyThrough[E, T, CT])(f: SubQuery[T, CT, P] => WhereFrag)(using
      targetMeta: TableMeta[T],
      targetCodec: DbCodec[T],
      resolve: ResolveScopes[T, P]
  ): EagerQuery[E, Vector[T] *: EmptyTuple, P] =
    val relCols = new Columns[T](targetMeta.columns).asInstanceOf[CT]
    val sq = new SubQuery[T, CT, P](targetMeta, relCols)
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
      mergeEagerFilter(Some(f(sq)), targetMeta, resolve.scopes)
    )
    EagerQuery(buildWith, codec, meta, Vector(d))
  end withRelated

  def withRelated[T, CT <: Selectable](rel: HasOneThrough[E, T, CT])(f: SubQuery[T, CT, P] => WhereFrag)(using
      targetMeta: TableMeta[T],
      targetCodec: DbCodec[T],
      resolve: ResolveScopes[T, P]
  ): EagerQuery[E, Vector[T] *: EmptyTuple, P] =
    val relCols = new Columns[T](targetMeta.columns).asInstanceOf[CT]
    val sq = new SubQuery[T, CT, P](targetMeta, relCols)
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
      mergeEagerFilter(Some(f(sq)), targetMeta, resolve.scopes)
    )
    EagerQuery(buildWith, codec, meta, Vector(d))
  end withRelated

  // --- withRelated for PivotRelation (delegates to underlying BelongsToMany) ---

  def withRelated[T, Pv, CT <: Selectable, PCT <: Selectable](rel: PivotRelation[E, T, Pv, CT, PCT])(using
      targetMeta: TableMeta[T],
      targetCodec: DbCodec[T],
      resolve: ResolveScopes[T, P]
  ): EagerQuery[E, Vector[T] *: EmptyTuple, P] =
    val d = PivotEagerDef(meta, rel.underlying, targetMeta, targetCodec, mergeEagerFilter(None, targetMeta, resolve.scopes))
    EagerQuery(buildWith, codec, meta, Vector(d))

  // --- withRelatedAndPivot ---

  def withRelatedAndPivot[T, Pv, CT <: Selectable, PCT <: Selectable](
      rel: PivotRelation[E, T, Pv, CT, PCT]
  )(using
      targetMeta: TableMeta[T],
      targetCodec: DbCodec[T],
      pivotMeta: TableMeta[Pv],
      pivotCodec: DbCodec[Pv],
      resolve: ResolveScopes[T, P]
  ): EagerQuery[E, Vector[(T, Pv)] *: EmptyTuple, P] =
    val d = PivotWithDataEagerDef(
      meta,
      rel.underlying,
      targetMeta,
      targetCodec,
      pivotMeta,
      pivotCodec,
      mergeEagerFilter(None, targetMeta, resolve.scopes)
    )
    EagerQuery(buildWith, codec, meta, Vector(d))

  def withRelatedAndPivot[T, Pv, CT <: Selectable, PCT <: Selectable](
      rel: PivotRelation[E, T, Pv, CT, PCT],
      pivotFilter: Frag
  )(using
      targetMeta: TableMeta[T],
      targetCodec: DbCodec[T],
      pivotMeta: TableMeta[Pv],
      pivotCodec: DbCodec[Pv],
      resolve: ResolveScopes[T, P]
  ): EagerQuery[E, Vector[(T, Pv)] *: EmptyTuple, P] =
    val d = PivotWithDataEagerDef(
      meta,
      rel.underlying,
      targetMeta,
      targetCodec,
      pivotMeta,
      pivotCodec,
      mergeEagerFilter(Some(pivotFilter), targetMeta, resolve.scopes)
    )
    EagerQuery(buildWith, codec, meta, Vector(d))
  end withRelatedAndPivot

  // --- Composed (via) withRelated ---

  def withRelated[I, T](rel: ComposedRelationship[E, I, T, ?])(using
      intermediateMeta: TableMeta[I],
      intermediateCodec: DbCodec[I],
      targetMeta: TableMeta[T],
      targetCodec: DbCodec[T],
      intermediateResolve: ResolveScopes[I, P],
      resolve: ResolveScopes[T, P]
  ): EagerQuery[E, Vector[T] *: EmptyTuple, P] =
    val d = ComposedEagerDef(
      meta,
      rel.inner,
      intermediateMeta,
      intermediateCodec,
      rel.outer,
      targetMeta,
      targetCodec,
      mergeEagerFilter(None, intermediateMeta, intermediateResolve.scopes),
      mergeEagerFilter(None, targetMeta, resolve.scopes)
    )
    EagerQuery(buildWith, codec, meta, Vector(d))
  end withRelated

  def withRelated[I, T, CT <: Selectable](rel: ComposedRelationship[E, I, T, CT])(f: SubQuery[T, CT, P] => WhereFrag)(using
      intermediateMeta: TableMeta[I],
      intermediateCodec: DbCodec[I],
      targetMeta: TableMeta[T],
      targetCodec: DbCodec[T],
      intermediateResolve: ResolveScopes[I, P],
      resolve: ResolveScopes[T, P]
  ): EagerQuery[E, Vector[T] *: EmptyTuple, P] =
    val relCols = new Columns[T](targetMeta.columns).asInstanceOf[CT]
    val sq = new SubQuery[T, CT, P](targetMeta, relCols)
    val d = ComposedEagerDef(
      meta,
      rel.inner,
      intermediateMeta,
      intermediateCodec,
      rel.outer,
      targetMeta,
      targetCodec,
      mergeEagerFilter(None, intermediateMeta, intermediateResolve.scopes),
      mergeEagerFilter(Some(f(sq)), targetMeta, resolve.scopes)
    )
    EagerQuery(buildWith, codec, meta, Vector(d))
  end withRelated

  // --- withCount for HasMany ---

  def withCount[T](rel: HasMany[E, T, ?])(using
      relMeta: TableMeta[T],
      resolve: ResolveScopes[T, P]
  ): CountQuery[E] =
    val countFrag = ExistsBuilder.buildRelCountFrag(meta, rel, relMeta, resolve.scopes)
    new CountQuery(
      meta,
      codec,
      countFrag.sqlString,
      None,
      rootPredicate,
      orderEntries,
      limitOpt,
      offsetOpt,
      countFrag.params,
      countFrag.writer
    )

  def withCount[T, CT <: Selectable](rel: HasMany[E, T, CT])(f: SubQuery[T, CT, P] => WhereFrag)(using
      relMeta: TableMeta[T],
      resolve: ResolveScopes[T, P]
  ): CountQuery[E] =
    val countFrag = ExistsBuilder.buildRelCountFrag(meta, rel, relMeta, resolve.scopes)
    val relCols = new Columns[T](relMeta.columns).asInstanceOf[CT]
    val sq = new SubQuery[T, CT, P](relMeta, relCols)
    new CountQuery(
      meta,
      codec,
      countFrag.sqlString,
      Some(f(sq)),
      rootPredicate,
      orderEntries,
      limitOpt,
      offsetOpt,
      countFrag.params,
      countFrag.writer
    )

  // --- withCount for BelongsToMany ---

  def withCount[T](rel: BelongsToMany[E, T, ?])(using
      relMeta: TableMeta[T],
      resolve: ResolveScopes[T, P]
  ): CountQuery[E] =
    val countFrag = ExistsBuilder.buildPivotCountFrag(meta, rel, relMeta, resolve.scopes)
    new CountQuery(
      meta,
      codec,
      countFrag.sqlString,
      None,
      rootPredicate,
      orderEntries,
      limitOpt,
      offsetOpt,
      countFrag.params,
      countFrag.writer
    )

  def withCount[T, CT <: Selectable](rel: BelongsToMany[E, T, CT])(f: SubQuery[T, CT, P] => WhereFrag)(using
      relMeta: TableMeta[T],
      resolve: ResolveScopes[T, P]
  ): CountQuery[E] =
    val countFrag = ExistsBuilder.buildPivotCountFrag(meta, rel, relMeta, resolve.scopes, hasUserCondition = true)
    val relCols = new Columns[T](relMeta.columns).asInstanceOf[CT]
    val sq = new SubQuery[T, CT, P](relMeta, relCols)
    new CountQuery(
      meta,
      codec,
      countFrag.sqlString,
      Some(f(sq)),
      rootPredicate,
      orderEntries,
      limitOpt,
      offsetOpt,
      countFrag.params,
      countFrag.writer
    )

  def join[T](rel: Relationship[E, T])(using
      joinedMeta: TableMeta[T],
      joinedCodec: DbCodec[T],
      resolve: ResolveScopes[T, P]
  ): JoinedQuery[(E, T), P] =
    val baseOn = s"t0.${rel.fk.sqlName} = t1.${rel.pk.sqlName}"
    val onFrag = ExistsBuilder.scopeConditions(resolve.scopes, joinedMeta) match
      case None => Frag(baseOn, Seq.empty, FragWriter.empty)
      case Some(sc) =>
        val scopeSql = sc.sqlString.replace(joinedMeta.tableName + ".", "t1.")
        Frag(s"$baseOn AND $scopeSql", sc.params, sc.writer)
    val entry = JoinEntry(
      TableRef(joinedMeta.tableName, "t1", joinedMeta.tableName),
      JoinType.Inner,
      onFrag
    )
    val rewrittenPredicate = rootPredicate.map(_.rewriteAlias(meta.tableName, "t0"))
    new JoinedQuery[(E, T), P](
      Vector(meta, joinedMeta),
      Vector(codec, joinedCodec),
      Vector(entry),
      rewrittenPredicate,
      orderEntries,
      limitOpt,
      offsetOpt
    )
  end join

  def leftJoin[T](rel: Relationship[E, T])(using
      joinedMeta: TableMeta[T],
      joinedCodec: DbCodec[T],
      resolve: ResolveScopes[T, P]
  ): JoinedQuery[(E, Option[T]), P] =
    val optCodec = DbCodec.OptionCodec[T](using joinedCodec)
    val baseOn = s"t0.${rel.fk.sqlName} = t1.${rel.pk.sqlName}"
    val onFrag = ExistsBuilder.scopeConditions(resolve.scopes, joinedMeta) match
      case None => Frag(baseOn, Seq.empty, FragWriter.empty)
      case Some(sc) =>
        val scopeSql = sc.sqlString.replace(joinedMeta.tableName + ".", "t1.")
        Frag(s"$baseOn AND $scopeSql", sc.params, sc.writer)
    val entry = JoinEntry(
      TableRef(joinedMeta.tableName, "t1", joinedMeta.tableName),
      JoinType.Left,
      onFrag
    )
    val rewrittenPredicate = rootPredicate.map(_.rewriteAlias(meta.tableName, "t0"))
    new JoinedQuery[(E, Option[T]), P](
      Vector(meta, joinedMeta),
      Vector(codec, optCodec),
      Vector(entry),
      rewrittenPredicate,
      orderEntries,
      limitOpt,
      offsetOpt
    )
  end leftJoin

  // --- whereHas / doesntHave for Relationship ---

  def whereHas[T](rel: Relationship[E, T])(using
      relMeta: TableMeta[T],
      resolve: ResolveScopes[T, P]
  ): QueryBuilder[HasRoot, E, C, P] =
    where(buildRelExistsFrag(rel, relMeta, None, negate = false, resolve.scopes))

  def whereHas[T](rel: BelongsTo[E, T])(f: SubQuery[T, Columns[T], P] => WhereFrag)(using
      relMeta: TableMeta[T],
      resolve: ResolveScopes[T, P]
  ): QueryBuilder[HasRoot, E, C, P] =
    val relCols = new Columns[T](relMeta.columns)
    val sq = new SubQuery[T, Columns[T], P](relMeta, relCols)
    where(buildRelExistsFrag(rel, relMeta, Some(f(sq)), negate = false, resolve.scopes))

  def whereHas[T](rel: HasOne[E, T])(f: SubQuery[T, Columns[T], P] => WhereFrag)(using
      relMeta: TableMeta[T],
      resolve: ResolveScopes[T, P]
  ): QueryBuilder[HasRoot, E, C, P] =
    val relCols = new Columns[T](relMeta.columns)
    val sq = new SubQuery[T, Columns[T], P](relMeta, relCols)
    where(buildRelExistsFrag(rel, relMeta, Some(f(sq)), negate = false, resolve.scopes))

  def whereHas[T](rel: HasMany[E, T, ?])(using
      relMeta: TableMeta[T],
      resolve: ResolveScopes[T, P]
  ): QueryBuilder[HasRoot, E, C, P] =
    where(buildRelExistsFrag(rel, relMeta, None, negate = false, resolve.scopes))

  def whereHas[T, CT <: Selectable](rel: HasMany[E, T, CT])(f: SubQuery[T, CT, P] => WhereFrag)(using
      relMeta: TableMeta[T],
      resolve: ResolveScopes[T, P]
  ): QueryBuilder[HasRoot, E, C, P] =
    val relCols = new Columns[T](relMeta.columns).asInstanceOf[CT]
    val sq = new SubQuery[T, CT, P](relMeta, relCols)
    where(buildRelExistsFrag(rel, relMeta, Some(f(sq)), negate = false, resolve.scopes))

  def doesntHave[T](rel: HasMany[E, T, ?])(using
      relMeta: TableMeta[T],
      resolve: ResolveScopes[T, P]
  ): QueryBuilder[HasRoot, E, C, P] =
    where(buildRelExistsFrag(rel, relMeta, None, negate = true, resolve.scopes))

  def doesntHave[T](rel: Relationship[E, T])(using
      relMeta: TableMeta[T],
      resolve: ResolveScopes[T, P]
  ): QueryBuilder[HasRoot, E, C, P] =
    where(buildRelExistsFrag(rel, relMeta, None, negate = true, resolve.scopes))

  // --- whereHas / doesntHave for BelongsToMany ---

  def whereHas[T](rel: BelongsToMany[E, T, ?])(using
      relMeta: TableMeta[T],
      resolve: ResolveScopes[T, P]
  ): QueryBuilder[HasRoot, E, C, P] =
    where(buildPivotExistsFrag(rel, None, negate = false, resolve.scopes, relMeta))

  def whereHas[T, CT <: Selectable](rel: BelongsToMany[E, T, CT])(f: SubQuery[T, CT, P] => WhereFrag)(using
      relMeta: TableMeta[T],
      resolve: ResolveScopes[T, P]
  ): QueryBuilder[HasRoot, E, C, P] =
    val relCols = new Columns[T](relMeta.columns).asInstanceOf[CT]
    val sq = new SubQuery[T, CT, P](relMeta, relCols)
    where(buildPivotExistsFrag(rel, Some((f(sq), relMeta)), negate = false, resolve.scopes, relMeta))

  def doesntHave[T](rel: BelongsToMany[E, T, ?])(using
      relMeta: TableMeta[T],
      resolve: ResolveScopes[T, P]
  ): QueryBuilder[HasRoot, E, C, P] =
    where(buildPivotExistsFrag(rel, None, negate = true, resolve.scopes, relMeta))

  // --- orWhereHas / orDoesntHave for Relationship ---

  def orWhereHas[T](rel: Relationship[E, T])(using relMeta: TableMeta[T], resolve: ResolveScopes[T, P]): QueryBuilder[HasRoot, E, C, P] =
    orWhere(buildRelExistsFrag(rel, relMeta, None, negate = false, resolve.scopes))

  def orWhereHas[T](rel: HasMany[E, T, ?])(using relMeta: TableMeta[T], resolve: ResolveScopes[T, P]): QueryBuilder[HasRoot, E, C, P] =
    orWhere(buildRelExistsFrag(rel, relMeta, None, negate = false, resolve.scopes))

  def orWhereHas[T, CT <: Selectable](rel: HasMany[E, T, CT])(
      f: SubQuery[T, CT, P] => WhereFrag
  )(using relMeta: TableMeta[T], resolve: ResolveScopes[T, P]): QueryBuilder[HasRoot, E, C, P] =
    val relCols = new Columns[T](relMeta.columns).asInstanceOf[CT]
    val sq = new SubQuery[T, CT, P](relMeta, relCols)
    orWhere(buildRelExistsFrag(rel, relMeta, Some(f(sq)), negate = false, resolve.scopes))

  def orDoesntHave[T](rel: HasMany[E, T, ?])(using relMeta: TableMeta[T], resolve: ResolveScopes[T, P]): QueryBuilder[HasRoot, E, C, P] =
    orWhere(buildRelExistsFrag(rel, relMeta, None, negate = true, resolve.scopes))

  def orDoesntHave[T](rel: Relationship[E, T])(using relMeta: TableMeta[T], resolve: ResolveScopes[T, P]): QueryBuilder[HasRoot, E, C, P] =
    orWhere(buildRelExistsFrag(rel, relMeta, None, negate = true, resolve.scopes))

  // --- orWhereHas / orDoesntHave for BelongsToMany ---

  def orWhereHas[T](
      rel: BelongsToMany[E, T, ?]
  )(using relMeta: TableMeta[T], resolve: ResolveScopes[T, P]): QueryBuilder[HasRoot, E, C, P] =
    orWhere(buildPivotExistsFrag(rel, None, negate = false, resolve.scopes, relMeta))

  def orWhereHas[T, CT <: Selectable](rel: BelongsToMany[E, T, CT])(
      f: SubQuery[T, CT, P] => WhereFrag
  )(using relMeta: TableMeta[T], resolve: ResolveScopes[T, P]): QueryBuilder[HasRoot, E, C, P] =
    val relCols = new Columns[T](relMeta.columns).asInstanceOf[CT]
    val sq = new SubQuery[T, CT, P](relMeta, relCols)
    orWhere(buildPivotExistsFrag(rel, Some((f(sq), relMeta)), negate = false, resolve.scopes, relMeta))

  def orDoesntHave[T](
      rel: BelongsToMany[E, T, ?]
  )(using relMeta: TableMeta[T], resolve: ResolveScopes[T, P]): QueryBuilder[HasRoot, E, C, P] =
    orWhere(buildPivotExistsFrag(rel, None, negate = true, resolve.scopes, relMeta))

  // --- has with count threshold ---

  // HasMany — unconstrained
  def has[T](rel: HasMany[E, T, ?])(f: CountExpr => WhereFrag)(using
      relMeta: TableMeta[T],
      resolve: ResolveScopes[T, P]
  ): QueryBuilder[HasRoot, E, C, P] =
    val countFrag = ExistsBuilder.buildRelCountFrag(meta, rel, relMeta, resolve.scopes)
    where(f(CountExpr.fromFrag(countFrag)))

  // HasMany — constrained
  def has[T, CT <: Selectable](rel: HasMany[E, T, CT], cond: SubQuery[T, CT, P] => WhereFrag)(f: CountExpr => WhereFrag)(using
      relMeta: TableMeta[T],
      resolve: ResolveScopes[T, P]
  ): QueryBuilder[HasRoot, E, C, P] =
    val countFrag = ExistsBuilder.buildRelCountFrag(meta, rel, relMeta, resolve.scopes)
    val relCols = new Columns[T](relMeta.columns).asInstanceOf[CT]
    val sq = new SubQuery[T, CT, P](relMeta, relCols)
    val condFrag = cond(sq)
    val combinedSql = s"${countFrag.sqlString} AND ${condFrag.sqlString}"
    val combinedParams = countFrag.params ++ condFrag.params
    val combinedWriter: FragWriter = (ps, pos) =>
      val next = countFrag.writer.write(ps, pos)
      condFrag.writer.write(ps, next)
    where(f(CountExpr(combinedSql, combinedParams, combinedWriter)))

  // BelongsToMany — unconstrained
  def has[T](rel: BelongsToMany[E, T, ?])(f: CountExpr => WhereFrag)(using
      relMeta: TableMeta[T],
      resolve: ResolveScopes[T, P]
  ): QueryBuilder[HasRoot, E, C, P] =
    val countFrag = ExistsBuilder.buildPivotCountFrag(meta, rel, relMeta, resolve.scopes)
    where(f(CountExpr.fromFrag(countFrag)))

  // BelongsToMany — constrained
  def has[T, CT <: Selectable](rel: BelongsToMany[E, T, CT], cond: SubQuery[T, CT, P] => WhereFrag)(f: CountExpr => WhereFrag)(using
      relMeta: TableMeta[T],
      resolve: ResolveScopes[T, P]
  ): QueryBuilder[HasRoot, E, C, P] =
    val countFrag = ExistsBuilder.buildPivotCountFrag(meta, rel, relMeta, resolve.scopes)
    val relCols = new Columns[T](relMeta.columns).asInstanceOf[CT]
    val sq = new SubQuery[T, CT, P](relMeta, relCols)
    val condFrag = cond(sq)
    val combinedSql = s"${countFrag.sqlString} AND ${condFrag.sqlString}"
    val combinedParams = countFrag.params ++ condFrag.params
    val combinedWriter: FragWriter = (ps, pos) =>
      val next = countFrag.writer.write(ps, pos)
      condFrag.writer.write(ps, next)
    where(f(CountExpr(combinedSql, combinedParams, combinedWriter)))

  // --- Private helpers (delegated to ExistsBuilder) ---

  private def buildRelExistsFrag[T](
      rel: Relationship[E, T],
      relMeta: TableMeta[T],
      condition: Option[WhereFrag],
      negate: Boolean,
      scopes: Vector[Scope[T]]
  ): WhereFrag =
    ExistsBuilder.buildRelExistsFrag(meta, rel, relMeta, condition, negate, scopes)

  private def buildPivotExistsFrag[T](
      rel: BelongsToMany[E, T, ?],
      conditionWithMeta: Option[(WhereFrag, TableMeta[T])],
      negate: Boolean,
      scopes: Vector[Scope[T]],
      targetMeta: TableMeta[T]
  ): WhereFrag =
    ExistsBuilder.buildPivotExistsFragScoped(meta, rel, conditionWithMeta, negate, scopes, targetMeta)

  private def buildPivotExistsFrag[T](
      rel: BelongsToMany[E, T, ?],
      conditionWithMeta: Option[(WhereFrag, TableMeta[T])],
      negate: Boolean
  ): WhereFrag =
    ExistsBuilder.buildPivotExistsFrag(meta, rel, conditionWithMeta, negate)

  private def buildRelCountSql[T](
      rel: Relationship[E, T],
      relMeta: TableMeta[T]
  ): String =
    ExistsBuilder.buildRelCountSql(meta, rel, relMeta)

  private def buildPivotCountSql[T](
      rel: BelongsToMany[E, T, ?],
      targetMeta: Option[TableMeta[T]]
  ): String =
    ExistsBuilder.buildPivotCountSql(meta, rel, targetMeta)

  def paginate(page: Int, perPage: Int)(using DbCon[?]): OffsetPage[E] =
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

  def chunk(batchSize: Int)(using DbCon[?]): Iterator[Vector[E]] =
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

  def delete()(using DbCon[? <: SupportsMutations]): Int =
    val (whereSql, params, writer) = buildWhere
    Frag(s"DELETE FROM ${meta.tableName}$whereSql", params, writer).update.run()

  def updateUnsafe(assignments: Frag)(using DbCon[? <: SupportsMutations]): Int =
    val (whereSql, whereParams, whereWriter) = buildWhere
    val sql = s"UPDATE ${meta.tableName} SET ${assignments.sqlString}$whereSql"
    val allParams = assignments.params ++ whereParams
    val combinedWriter: FragWriter = (ps, pos) =>
      val next = assignments.writer.write(ps, pos)
      whereWriter.write(ps, next)
    Frag(sql, allParams, combinedWriter).update.run()

  def updateUnsafe(f: C => Frag)(using DbCon[? <: SupportsMutations]): Int =
    updateUnsafe(f(cols))

  def increment[A](f: C => ColRef[A], amount: A | None.type = None)(using
      num: Numeric[A],
      codec: DbCodec[A],
      tt: TypeTest[A | None.type, A],
      con: DbCon[? <: SupportsMutations]
  ): Int =
    val actual: A = amount match
      case None => num.one
      case a: A => a
    val col = f(cols)
    val writer: FragWriter = (ps, pos) =>
      codec.writeSingle(actual, ps, pos)
      pos + codec.cols.length
    updateUnsafe(Frag(s"${col.queryRepr} = ${col.queryRepr} + ?", Seq(actual), writer))

  def touch()(using hasUpdatedAt: HasUpdatedAt[E], con: DbCon[? <: SupportsMutations]): Int =
    val (whereSql, params, writer) = buildWhere
    Frag(
      s"UPDATE ${meta.tableName} SET ${hasUpdatedAt.column.sqlName} = CURRENT_TIMESTAMP$whereSql",
      params,
      writer
    ).update.run()

  def decrement[A](f: C => ColRef[A], amount: A | None.type = None)(using
      num: Numeric[A],
      codec: DbCodec[A],
      tt: TypeTest[A | None.type, A],
      con: DbCon[? <: SupportsMutations]
  ): Int =
    val actual: A = amount match
      case None => num.one
      case a: A => a
    val col = f(cols)
    val writer: FragWriter = (ps, pos) =>
      codec.writeSingle(actual, ps, pos)
      pos + codec.cols.length
    updateUnsafe(Frag(s"${col.queryRepr} = ${col.queryRepr} - ?", Seq(amount), writer))

  // --- Entity-level mutations (ignore QB WHERE state) ---

  private val _pkIndex: Int = meta.pkIndex
  private def pkIndex: Int = _pkIndex

  def updateEntity(entity: E)(using con: DbCon[? <: SupportsMutations]): Unit =
    val idx = pkIndex
    val elemCodecs = meta.elementCodecs
    val cols = meta.columns
    val idCodec = elemCodecs(idx)
    val updateKeys = cols.indices
      .filter(_ != idx)
      .map(i => cols(i).sqlName + " = " + elemCodecs(i).queryRepr)
      .mkString(", ")
    val sql = s"UPDATE ${meta.tableName} SET $updateKeys WHERE ${cols(idx).sqlName} = ${idCodec.queryRepr}"
    handleQuery(sql, entity):
      Using(con.connection.prepareStatement(sql)): ps =>
        val product = entity.asInstanceOf[Product]
        var pos = 1
        for i <- cols.indices if i != idx do
          val codec = elemCodecs(i).asInstanceOf[DbCodec[Any]]
          codec.writeSingle(product.productElement(i), ps, pos)
          pos += codec.cols.length
        idCodec.asInstanceOf[DbCodec[Any]].writeSingle(product.productElement(idx), ps, pos)
        timed(ps.executeUpdate())
  end updateEntity

  def updateAllEntities(entities: Iterable[E])(using con: DbCon[? <: SupportsMutations]): BatchUpdateResult =
    val idx = pkIndex
    val elemCodecs = meta.elementCodecs
    val cols = meta.columns
    val idCodec = elemCodecs(idx)
    val updateKeys = cols.indices
      .filter(_ != idx)
      .map(i => cols(i).sqlName + " = " + elemCodecs(i).queryRepr)
      .mkString(", ")
    val sql = s"UPDATE ${meta.tableName} SET $updateKeys WHERE ${cols(idx).sqlName} = ${idCodec.queryRepr}"
    handleQuery(sql, entities):
      Using(con.connection.prepareStatement(sql)): ps =>
        for entity <- entities do
          val product = entity.asInstanceOf[Product]
          var pos = 1
          for i <- cols.indices if i != idx do
            val codec = elemCodecs(i).asInstanceOf[DbCodec[Any]]
            codec.writeSingle(product.productElement(i), ps, pos)
            pos += codec.cols.length
          idCodec.asInstanceOf[DbCodec[Any]].writeSingle(product.productElement(idx), ps, pos)
          ps.addBatch()
        timed(batchUpdateResult(ps.executeBatch()))
  end updateAllEntities

  def updatePartial(original: E, current: E)(using con: DbCon[? <: SupportsMutations]): Unit =
    val idx = pkIndex
    val elemCodecs = meta.elementCodecs
    val cols = meta.columns
    val origProduct = original.asInstanceOf[Product]
    val currProduct = current.asInstanceOf[Product]
    val origId = origProduct.productElement(idx)
    val currId = currProduct.productElement(idx)
    require(origId == currId, s"updatePartial requires same id, got $origId != $currId")

    val arity = origProduct.productArity
    val changed = Vector.newBuilder[Int]
    var i = 0
    while i < arity do
      if i != idx && origProduct.productElement(i) != currProduct.productElement(i)
      then changed += i
      i += 1
    val changedIndices = changed.result()
    if changedIndices.isEmpty then return

    val idCodec = elemCodecs(idx)
    val setClauses = changedIndices
      .map(ci => cols(ci).sqlName + " = " + elemCodecs(ci).queryRepr)
      .mkString(", ")
    val sql = s"UPDATE ${meta.tableName} SET $setClauses WHERE ${cols(idx).sqlName} = ${idCodec.queryRepr}"
    handleQuery(sql, current):
      Using(con.connection.prepareStatement(sql)): ps =>
        var pos = 1
        for ci <- changedIndices do
          val codec = elemCodecs(ci).asInstanceOf[DbCodec[Any]]
          codec.writeSingle(currProduct.productElement(ci), ps, pos)
          pos += codec.cols.length
        idCodec.asInstanceOf[DbCodec[Any]].writeSingle(currId, ps, pos)
        timed(ps.executeUpdate())
  end updatePartial

  def deleteEntity(entity: E)(using con: DbCon[? <: SupportsMutations]): Unit =
    val idx = pkIndex
    val idCodec = meta.elementCodecs(idx)
    val idCol = meta.columns(idx)
    val sql = s"DELETE FROM ${meta.tableName} WHERE ${idCol.sqlName} = ${idCodec.queryRepr}"
    val id = entity.asInstanceOf[Product].productElement(idx)
    val writer: FragWriter = (ps, pos) =>
      idCodec.asInstanceOf[DbCodec[Any]].writeSingle(id, ps, pos)
      pos + idCodec.cols.length
    Frag(sql, Seq(id), writer).update.run()

  def deleteAllEntities(entities: Iterable[E])(using con: DbCon[? <: SupportsMutations]): BatchUpdateResult =
    val idx = pkIndex
    val idCodec = meta.elementCodecs(idx).asInstanceOf[DbCodec[Any]]
    val idCol = meta.columns(idx)
    val sql = s"DELETE FROM ${meta.tableName} WHERE ${idCol.sqlName} = ${idCodec.queryRepr}"
    handleQuery(sql, entities):
      Using(con.connection.prepareStatement(sql)): ps =>
        for entity <- entities do
          val id = entity.asInstanceOf[Product].productElement(idx)
          idCodec.writeSingle(id, ps, 1)
          ps.addBatch()
        timed(batchUpdateResult(ps.executeBatch()))

  def deleteAllById[ID](ids: Iterable[ID])(using idCodec: DbCodec[ID], con: DbCon[? <: SupportsMutations]): BatchUpdateResult =
    val idx = pkIndex
    val idCol = meta.columns(idx)
    val sql = s"DELETE FROM ${meta.tableName} WHERE ${idCol.sqlName} = ${idCodec.queryRepr}"
    handleQuery(sql, ids):
      Using(con.connection.prepareStatement(sql)): ps =>
        idCodec.write(ids, ps)
        timed(batchUpdateResult(ps.executeBatch()))

  def truncate()(using con: DbCon[? <: SupportsMutations]): Unit =
    val sql = con.databaseType.renderTruncate(meta.tableName)
    Frag(sql, Vector.empty, FragWriter.empty).update.run()

  def upsertByPk(entity: E)(using con: DbCon[? <: SupportsMutations]): Unit =
    val allCols = meta.columns.map(_.sqlName)
    val sql = con.databaseType.renderUpsertByPk(
      meta.tableName,
      allCols,
      codec.queryRepr,
      meta.primaryKey.sqlName
    )
    handleQuery(sql, entity):
      Using(con.connection.prepareStatement(sql)): ps =>
        codec.writeSingle(entity, ps)
        timed(ps.executeUpdate())

  def debugPrintSql(using DbCon[?]): this.type =
    val frag = build
    DebugSql.printDebug(Vector(frag))
    this
end QueryBuilder

object QueryBuilder:
  /** Intermediate object for the select() → ProjectedQuery transition. Created by QueryBuilder.select, holds the frozen WHERE + column
    * proxy. The transparent inline apply method triggers the macro.
    */
  class SelectPhase[C](val tableName: String, val cols: C, val predicate: Option[Predicate]):
    transparent inline def apply[P](inline f: C => P): Any =
      ${ selectImpl[C, P]('this, 'f) }

  private[parlance] def build0[E, C <: Selectable, P <: ScopePolicy](
      meta: TableMeta[E],
      codec: DbCodec[E],
      cols: C
  ): QueryBuilder[HasRoot, E, C, P] =
    new QueryBuilder(meta, codec, cols, None, Vector.empty, None, None)

  inline def into[EC, E](using
      inline meta: EntityMeta[E],
      inline ecMirror: Mirror.ProductOf[EC],
      ecCodec: DbCodec[EC]
  ): InsertBuilder[EC, E] =
    ${ intoImpl[EC, E]('meta, 'ecMirror, 'ecCodec) }

  transparent inline def from[E](using
      inline meta: TableMeta[E],
      codec: DbCodec[E]
  ): Any = ${ fromImpl[E]('meta, 'codec) }

  private def intoImpl[EC: Type, E: Type](
      metaExpr: Expr[EntityMeta[E]],
      ecMirror: Expr[Mirror.ProductOf[EC]],
      ecCodecExpr: Expr[DbCodec[EC]]
  )(using Quotes): Expr[InsertBuilder[EC, E]] =
    import quotes.reflect.*

    // Get E's @Table annotation for name mapping
    val tableAnnotExpr: Expr[Table] =
      DerivingUtil.tableAnnot[E] match
        case Some(t) => t
        case None =>
          report.errorAndAbort(
            s"${TypeRepr.of[E].show} must have @Table annotation for QueryBuilder.into"
          )

    val nameMapper: Expr[SqlNameMapper] = '{ $tableAnnotExpr.nameMapper }

    // Get EC field names from its mirror
    ecMirror match
      case '{
            $m: Mirror.ProductOf[EC] {
              type MirroredElemLabels = ecMels
            }
          } =>
        val ecFieldNames = elemNames[ecMels]()
        assertECIsSubsetOfE[EC, E]
        assertCreatorExtendsCreatorOf[EC, E]
        assertCreatorOmitsAutoManaged[EC, E]
        val ecFieldNamesSql: List[Expr[String]] = ecFieldNames.map { name =>
          sqlNameAnnot[E](name) match
            case Some(sqlName) => '{ $sqlName.name }
            case None          => '{ $nameMapper.toColumnName(${ Expr(name) }) }
        }

        // Get the id index from E
        val idIndex = idAnnotIndex[E]

        val ecColNamesExpr = Expr.ofSeq(ecFieldNamesSql)

        '{
          new InsertBuilder[EC, E](
            $metaExpr.tableName,
            IArray.from($ecColNamesExpr),
            $ecCodecExpr,
            $metaExpr,
            $metaExpr.columns.map(_.sqlName),
            $metaExpr.elementCodecs,
            $metaExpr.primaryKey.sqlName,
            $idIndex
          )
        }
      case _ =>
        report.errorAndAbort(
          s"A Mirror.ProductOf is required for QueryBuilder.into[${TypeRepr.of[EC].show}, ${TypeRepr.of[E].show}]"
        )
    end match
  end intoImpl

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
              build0[E, ct & Selectable, IgnoreScopes]($meta, $codec, cols)
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
              val qb = build0[E, ct & Selectable, ApplyScopes]($meta, $codec, cols)
              val m = $meta
              val withWheres = $scopes
                .flatMap(_.conditions(m))
                .foldLeft(qb)(_.where(_))
              $scopes
                .flatMap(_.orderings(m))
                .foldLeft(withWheres)(_.orderBy(_))
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
          if isSE then '{ null.asInstanceOf[DbCodec[?]] } // placeholder; runtime uses SelectExpr.codec
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
        report.errorAndAbort("select() failed to construct result types. This is a bug in parlance.")
    end match
  end selectImpl

  // --- select() macro helper: extract field names from a tuple of string literal types ---
  private[parlance] def selectFieldNames[N: Type](res: List[String] = Nil)(using Quotes): List[String] =
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
  private[parlance] def selectAnalyzeElements[V: Type](names: List[String])(using Quotes): List[(String, Type[?], Boolean)] =
    import quotes.reflect.*
    def loop[T: Type](remainingNames: List[String]): List[(String, Type[?], Boolean)] =
      Type.of[T] match
        case '[v *: vs] =>
          val name = remainingNames.head
          val (innerType, isSE) = Type.of[v] match
            case '[SelectExpr[a]] => (Type.of[a], true)
            case '[Col[a]]        => (Type.of[a], false)
            case '[ColRef[a]]     => (Type.of[a], false)
            case _ =>
              report.errorAndAbort(
                s"select() element '$name' must be Col[A] or SelectExpr[A], got ${TypeRepr.of[v].show}"
              )
          (name, innerType, isSE) :: loop[vs](remainingNames.tail)
        case '[EmptyTuple] => Nil
    loop[V](names)

  // --- select() macro helper: build a tuple type from a list of types ---
  private[parlance] def selectListToTupleType(ts: List[Type[?]])(using Quotes): Type[?] =
    ts.foldRight(Type.of[EmptyTuple]: Type[?]) { (t, acc) =>
      (t, acc) match
        case (
              '[ft],
              '[
              type acc <: Tuple; `acc`]
            ) =>
          Type.of[ft *: acc]
        case _ =>
          quotes.reflect.report.errorAndAbort("Failed to build tuple type from select() elements. This is a bug in parlance.")
    }

  // --- select() macro helper: build a names-tuple type from string literals ---
  private[parlance] def selectStringsToTupleType(names: List[String])(using Quotes): Type[?] =
    import quotes.reflect.*
    selectListToTupleType(names.map(n => ConstantType(StringConstant(n)).asType))

  // --- select() macro helper: assemble a NamedTuple type ---
  private[parlance] def selectPackNamedTuple(names: List[String], types: List[Type[?]])(using Quotes): Type[?] =
    import quotes.reflect.*
    val nmesTpe = selectStringsToTupleType(names)
    val valsTpe = selectListToTupleType(types)
    (nmesTpe, valsTpe) match
      case (
            '[
            type nmes <: Tuple; `nmes`],
            '[
            type tps <: Tuple; `tps`]
          ) =>
        Type.of[NamedTuple.NamedTuple[nmes, tps]]
      case _ =>
        report.errorAndAbort("Failed to construct NamedTuple type for select(). This is a bug in parlance.")

  class UpdatePhase[E] private[parlance] (
      private[parlance] val qb: QueryBuilder[?, E, ?, ?]
  ):
    inline def apply[P](inline assignments: P)(using inline con: DbCon[? <: SupportsMutations]): Int =
      ${ updateImpl[E, P]('this, 'assignments, 'con) }

  // --- update() macro implementation ---

  private def updateImpl[E: Type, P: Type](
      phaseExpr: Expr[UpdatePhase[E]],
      assignments: Expr[P],
      conExpr: Expr[DbCon[? <: SupportsMutations]]
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
