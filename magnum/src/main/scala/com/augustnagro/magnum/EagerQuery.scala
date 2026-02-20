package com.augustnagro.magnum

import scala.collection.mutable

class EagerQuery[E, R <: Tuple] private[magnum] (
    private val rootFrag: Frag,
    private val rootCodec: DbCodec[E],
    private val rootMeta: TableMeta[E],
    private val defs: Vector[EagerQueryDef]
):

  import EagerQueryDef.*

  // --- Unconstrained withRelated ---

  def withRelated[T](rel: HasMany[E, T, ?])(using
      childMeta: TableMeta[T],
      childCodec: DbCodec[T]
  ): EagerQuery[E, Tuple.Append[R, Vector[T]]] =
    val d = DirectEagerDef(rootMeta, rel, childMeta, childCodec, None)
    EagerQuery(rootFrag, rootCodec, rootMeta, defs :+ d)

  def withRelated[T](rel: BelongsToMany[E, T, ?])(using
      targetMeta: TableMeta[T],
      targetCodec: DbCodec[T]
  ): EagerQuery[E, Tuple.Append[R, Vector[T]]] =
    val d = PivotEagerDef(rootMeta, rel, targetMeta, targetCodec, None)
    EagerQuery(rootFrag, rootCodec, rootMeta, defs :+ d)

  def withRelated[T](rel: HasManyThrough[E, T, ?])(using
      targetMeta: TableMeta[T],
      targetCodec: DbCodec[T]
  ): EagerQuery[E, Tuple.Append[R, Vector[T]]] =
    val d = ThroughEagerDef(rootMeta, rel.intermediateTable, rel.sourceFk,
      rel.intermediatePk.sqlName, rel.targetFk.scalaName, rel.targetFk.sqlName,
      rel.sourcePk.scalaName, targetMeta, targetCodec, None)
    EagerQuery(rootFrag, rootCodec, rootMeta, defs :+ d)

  def withRelated[T](rel: HasOneThrough[E, T, ?])(using
      targetMeta: TableMeta[T],
      targetCodec: DbCodec[T]
  ): EagerQuery[E, Tuple.Append[R, Vector[T]]] =
    val d = ThroughEagerDef(rootMeta, rel.intermediateTable, rel.sourceFk,
      rel.intermediatePk.sqlName, rel.targetFk.scalaName, rel.targetFk.sqlName,
      rel.sourcePk.scalaName, targetMeta, targetCodec, None)
    EagerQuery(rootFrag, rootCodec, rootMeta, defs :+ d)

  // --- Constrained withRelated ---

  def withRelated[T, CT <: Selectable](rel: HasMany[E, T, CT])(f: CT => Frag)(using
      childMeta: TableMeta[T],
      childCodec: DbCodec[T]
  ): EagerQuery[E, Tuple.Append[R, Vector[T]]] =
    val cols = new Columns[T](childMeta.columns).asInstanceOf[CT]
    val d = DirectEagerDef(rootMeta, rel, childMeta, childCodec, Some(f(cols)))
    EagerQuery(rootFrag, rootCodec, rootMeta, defs :+ d)

  def withRelated[T, CT <: Selectable](rel: BelongsToMany[E, T, CT])(f: CT => Frag)(using
      targetMeta: TableMeta[T],
      targetCodec: DbCodec[T]
  ): EagerQuery[E, Tuple.Append[R, Vector[T]]] =
    val cols = new Columns[T](targetMeta.columns).asInstanceOf[CT]
    val d = PivotEagerDef(rootMeta, rel, targetMeta, targetCodec, Some(f(cols)))
    EagerQuery(rootFrag, rootCodec, rootMeta, defs :+ d)

  def withRelated[T, CT <: Selectable](rel: HasManyThrough[E, T, CT])(f: CT => Frag)(using
      targetMeta: TableMeta[T],
      targetCodec: DbCodec[T]
  ): EagerQuery[E, Tuple.Append[R, Vector[T]]] =
    val cols = new Columns[T](targetMeta.columns).asInstanceOf[CT]
    val d = ThroughEagerDef(rootMeta, rel.intermediateTable, rel.sourceFk,
      rel.intermediatePk.sqlName, rel.targetFk.scalaName, rel.targetFk.sqlName,
      rel.sourcePk.scalaName, targetMeta, targetCodec, Some(f(cols)))
    EagerQuery(rootFrag, rootCodec, rootMeta, defs :+ d)

  def withRelated[T, CT <: Selectable](rel: HasOneThrough[E, T, CT])(f: CT => Frag)(using
      targetMeta: TableMeta[T],
      targetCodec: DbCodec[T]
  ): EagerQuery[E, Tuple.Append[R, Vector[T]]] =
    val cols = new Columns[T](targetMeta.columns).asInstanceOf[CT]
    val d = ThroughEagerDef(rootMeta, rel.intermediateTable, rel.sourceFk,
      rel.intermediatePk.sqlName, rel.targetFk.scalaName, rel.targetFk.sqlName,
      rel.sourcePk.scalaName, targetMeta, targetCodec, Some(f(cols)))
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
    rootFrag.query[E](using rootCodec).run().headOption.map: parent =>
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
