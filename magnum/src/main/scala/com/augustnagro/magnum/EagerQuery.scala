package com.augustnagro.magnum

import scala.collection.mutable

class EagerQuery[E, R <: Tuple] private[magnum] (
    private val rootFrag: Frag,
    private val rootCodec: DbCodec[E],
    private val rootMeta: TableMeta[E],
    private val defs: Vector[EagerQueryDef]
):

  import EagerQueryDef.*

  /** Merge scope conditions with an optional user filter. */
  private def mergeFilter[T](userFilter: Option[Frag], targetMeta: TableMeta[T], scoped: Scoped[T]): Option[Frag] =
    val scopeFrag = ExistsBuilder.scopeConditions(scoped.scopes, targetMeta).map(f => f: Frag)
    (userFilter, scopeFrag) match
      case (None, None)       => None
      case (Some(u), None)    => Some(u)
      case (None, Some(s))    => Some(s)
      case (Some(u), Some(s)) =>
        val sql = s"${u.sqlString} AND ${s.sqlString}"
        val params = u.params ++ s.params
        val writer: FragWriter = (ps, pos) =>
          val next = u.writer.write(ps, pos)
          s.writer.write(ps, next)
        Some(Frag(sql, params, writer))

  // --- Unconstrained withRelated ---

  def withRelated[T](rel: HasMany[E, T, ?])(using
      childMeta: TableMeta[T],
      childCodec: DbCodec[T],
      scoped: Scoped[T]
  ): EagerQuery[E, Tuple.Append[R, Vector[T]]] =
    val d = DirectEagerDef(rootMeta, rel, childMeta, childCodec, mergeFilter(None, childMeta, scoped))
    EagerQuery(rootFrag, rootCodec, rootMeta, defs :+ d)

  def withRelated[T](rel: BelongsToMany[E, T, ?])(using
      targetMeta: TableMeta[T],
      targetCodec: DbCodec[T],
      scoped: Scoped[T]
  ): EagerQuery[E, Tuple.Append[R, Vector[T]]] =
    val d = PivotEagerDef(rootMeta, rel, targetMeta, targetCodec, mergeFilter(None, targetMeta, scoped))
    EagerQuery(rootFrag, rootCodec, rootMeta, defs :+ d)

  def withRelated[T](rel: HasManyThrough[E, T, ?])(using
      targetMeta: TableMeta[T],
      targetCodec: DbCodec[T],
      scoped: Scoped[T]
  ): EagerQuery[E, Tuple.Append[R, Vector[T]]] =
    val d = ThroughEagerDef(
      rootMeta,
      rel.intermediateTable,
      rel.sourceFk,
      rel.intermediatePk.sqlName,
      rel.targetFk.scalaName,
      rel.targetFk.sqlName,
      rel.sourcePk.scalaName,
      targetMeta,
      targetCodec,
      mergeFilter(None, targetMeta, scoped)
    )
    EagerQuery(rootFrag, rootCodec, rootMeta, defs :+ d)

  def withRelated[T](rel: HasOneThrough[E, T, ?])(using
      targetMeta: TableMeta[T],
      targetCodec: DbCodec[T],
      scoped: Scoped[T]
  ): EagerQuery[E, Tuple.Append[R, Vector[T]]] =
    val d = ThroughEagerDef(
      rootMeta,
      rel.intermediateTable,
      rel.sourceFk,
      rel.intermediatePk.sqlName,
      rel.targetFk.scalaName,
      rel.targetFk.sqlName,
      rel.sourcePk.scalaName,
      targetMeta,
      targetCodec,
      mergeFilter(None, targetMeta, scoped)
    )
    EagerQuery(rootFrag, rootCodec, rootMeta, defs :+ d)

  // --- Constrained withRelated ---

  def withRelated[T, CT <: Selectable](rel: HasMany[E, T, CT])(f: CT => Frag)(using
      childMeta: TableMeta[T],
      childCodec: DbCodec[T],
      scoped: Scoped[T]
  ): EagerQuery[E, Tuple.Append[R, Vector[T]]] =
    val cols = new Columns[T](childMeta.columns).asInstanceOf[CT]
    val d = DirectEagerDef(rootMeta, rel, childMeta, childCodec, mergeFilter(Some(f(cols)), childMeta, scoped))
    EagerQuery(rootFrag, rootCodec, rootMeta, defs :+ d)

  def withRelated[T, CT <: Selectable](rel: BelongsToMany[E, T, CT])(f: CT => Frag)(using
      targetMeta: TableMeta[T],
      targetCodec: DbCodec[T],
      scoped: Scoped[T]
  ): EagerQuery[E, Tuple.Append[R, Vector[T]]] =
    val cols = new Columns[T](targetMeta.columns).asInstanceOf[CT]
    val d = PivotEagerDef(rootMeta, rel, targetMeta, targetCodec, mergeFilter(Some(f(cols)), targetMeta, scoped))
    EagerQuery(rootFrag, rootCodec, rootMeta, defs :+ d)

  def withRelated[T, CT <: Selectable](rel: HasManyThrough[E, T, CT])(f: CT => Frag)(using
      targetMeta: TableMeta[T],
      targetCodec: DbCodec[T],
      scoped: Scoped[T]
  ): EagerQuery[E, Tuple.Append[R, Vector[T]]] =
    val cols = new Columns[T](targetMeta.columns).asInstanceOf[CT]
    val d = ThroughEagerDef(
      rootMeta,
      rel.intermediateTable,
      rel.sourceFk,
      rel.intermediatePk.sqlName,
      rel.targetFk.scalaName,
      rel.targetFk.sqlName,
      rel.sourcePk.scalaName,
      targetMeta,
      targetCodec,
      mergeFilter(Some(f(cols)), targetMeta, scoped)
    )
    EagerQuery(rootFrag, rootCodec, rootMeta, defs :+ d)

  def withRelated[T, CT <: Selectable](rel: HasOneThrough[E, T, CT])(f: CT => Frag)(using
      targetMeta: TableMeta[T],
      targetCodec: DbCodec[T],
      scoped: Scoped[T]
  ): EagerQuery[E, Tuple.Append[R, Vector[T]]] =
    val cols = new Columns[T](targetMeta.columns).asInstanceOf[CT]
    val d = ThroughEagerDef(
      rootMeta,
      rel.intermediateTable,
      rel.sourceFk,
      rel.intermediatePk.sqlName,
      rel.targetFk.scalaName,
      rel.targetFk.sqlName,
      rel.sourcePk.scalaName,
      targetMeta,
      targetCodec,
      mergeFilter(Some(f(cols)), targetMeta, scoped)
    )
    EagerQuery(rootFrag, rootCodec, rootMeta, defs :+ d)

  // --- withRelated for PivotRelation (delegates to underlying) ---

  def withRelated[T, P, CT <: Selectable, PCT <: Selectable](rel: PivotRelation[E, T, P, CT, PCT])(using
      targetMeta: TableMeta[T],
      targetCodec: DbCodec[T],
      scoped: Scoped[T]
  ): EagerQuery[E, Tuple.Append[R, Vector[T]]] =
    val d = PivotEagerDef(rootMeta, rel.underlying, targetMeta, targetCodec, mergeFilter(None, targetMeta, scoped))
    EagerQuery(rootFrag, rootCodec, rootMeta, defs :+ d)

  // --- withRelatedAndPivot ---

  def withRelatedAndPivot[T, P, CT <: Selectable, PCT <: Selectable](
      rel: PivotRelation[E, T, P, CT, PCT]
  )(using
      targetMeta: TableMeta[T],
      targetCodec: DbCodec[T],
      pivotMeta: TableMeta[P],
      pivotCodec: DbCodec[P],
      scoped: Scoped[T]
  ): EagerQuery[E, Tuple.Append[R, Vector[(T, P)]]] =
    val d = PivotWithDataEagerDef(rootMeta, rel.underlying, targetMeta, targetCodec, pivotMeta, pivotCodec, mergeFilter(None, targetMeta, scoped))
    EagerQuery(rootFrag, rootCodec, rootMeta, defs :+ d)

  def withRelatedAndPivot[T, P, CT <: Selectable, PCT <: Selectable](
      rel: PivotRelation[E, T, P, CT, PCT],
      pivotFilter: Frag
  )(using
      targetMeta: TableMeta[T],
      targetCodec: DbCodec[T],
      pivotMeta: TableMeta[P],
      pivotCodec: DbCodec[P],
      scoped: Scoped[T]
  ): EagerQuery[E, Tuple.Append[R, Vector[(T, P)]]] =
    val d = PivotWithDataEagerDef(rootMeta, rel.underlying, targetMeta, targetCodec, pivotMeta, pivotCodec, mergeFilter(Some(pivotFilter), targetMeta, scoped))
    EagerQuery(rootFrag, rootCodec, rootMeta, defs :+ d)

  // --- Composed (via) withRelated ---

  def withRelated[I, T](rel: ComposedRelationship[E, I, T, ?])(using
      intermediateMeta: TableMeta[I],
      intermediateCodec: DbCodec[I],
      targetMeta: TableMeta[T],
      targetCodec: DbCodec[T],
      intermediateScoped: Scoped[I],
      scoped: Scoped[T]
  ): EagerQuery[E, Tuple.Append[R, Vector[T]]] =
    val d = ComposedEagerDef(rootMeta, rel.inner, intermediateMeta, intermediateCodec, rel.outer, targetMeta, targetCodec, mergeFilter(None, intermediateMeta, intermediateScoped), mergeFilter(None, targetMeta, scoped))
    EagerQuery(rootFrag, rootCodec, rootMeta, defs :+ d)

  def withRelated[I, T, CT <: Selectable](rel: ComposedRelationship[E, I, T, CT])(f: CT => Frag)(using
      intermediateMeta: TableMeta[I],
      intermediateCodec: DbCodec[I],
      targetMeta: TableMeta[T],
      targetCodec: DbCodec[T],
      intermediateScoped: Scoped[I],
      scoped: Scoped[T]
  ): EagerQuery[E, Tuple.Append[R, Vector[T]]] =
    val cols = new Columns[T](targetMeta.columns).asInstanceOf[CT]
    val d = ComposedEagerDef(rootMeta, rel.inner, intermediateMeta, intermediateCodec, rel.outer, targetMeta, targetCodec, mergeFilter(None, intermediateMeta, intermediateScoped), mergeFilter(Some(f(cols)), targetMeta, scoped))
    EagerQuery(rootFrag, rootCodec, rootMeta, defs :+ d)

  // --- Execution ---

  def run()(using DbCon): Vector[E *: R] =
    val parents = rootFrag.query[E](using rootCodec).run()
    if parents.isEmpty then return Vector.empty

    val groupedResults: Vector[mutable.LinkedHashMap[Any, Vector[Any]]] =
      defs.map: d =>
        val pkIdx = resolveColumnIndex(rootMeta, d.parentKeyScalaName)
        val parentKeys = parents.map(p => extractKey(p, rootMeta, pkIdx))
        d.fetchGrouped(parentKeys)

    parents.map: parent =>
      val tail = defs.zipWithIndex.foldRight[Tuple](EmptyTuple): (pair, acc) =>
        val (d, i) = pair
        val pkIdx = resolveColumnIndex(rootMeta, d.parentKeyScalaName)
        val key = extractKey(parent, rootMeta, pkIdx)
        val children = groupedResults(i).getOrElse(key, Vector.empty)
        children *: acc
      (parent *: tail).asInstanceOf[E *: R]

  def first()(using DbCon): Option[E *: R] =
    rootFrag
      .query[E](using rootCodec)
      .run()
      .headOption
      .map: parent =>
        val tail = defs.foldRight[Tuple](EmptyTuple): (d, acc) =>
          val pkIdx = resolveColumnIndex(rootMeta, d.parentKeyScalaName)
          val key = extractKey(parent, rootMeta, pkIdx)
          val grouped = d.fetchGrouped(Vector(key))
          val children = grouped.getOrElse(key, Vector.empty)
          children *: acc
        (parent *: tail).asInstanceOf[E *: R]

  def buildQueries: Vector[Frag] =
    rootFrag +: defs.flatMap(_.representativeQueries)

  def debugPrintSql(using DbCon): this.type =
    DebugSql.printDebug(buildQueries)
    this

end EagerQuery
