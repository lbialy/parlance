package com.augustnagro.magnum

import scala.collection.mutable

class PivotEagerQuery[E, T] private[magnum] (
    protected val rootFrag: Frag,
    protected val rootCodec: DbCodec[E],
    protected val rootMeta: TableMeta[E],
    private val rel: BelongsToMany[E, T, ?],
    private val targetMeta: TableMeta[T],
    private val targetCodec: DbCodec[T]
) extends EagerQueryBase[E, T]:

  private def sourcePkIndex: Int =
    resolveColumnIndex(rootMeta, rel.sourcePk.scalaName)

  private def targetPkIndex: Int =
    resolveColumnIndex(targetMeta, rel.targetPk.scalaName)

  private def pivotQuerySql(placeholders: String): String =
    s"SELECT ${rel.sourceFk}, ${rel.targetFk} FROM ${rel.pivotTable} WHERE ${rel.sourceFk} IN ($placeholders)"

  private def targetQuerySql(placeholders: String): String =
    val targetCols = targetMeta.columns.map(_.sqlName).mkString(", ")
    s"SELECT $targetCols FROM ${targetMeta.tableName} WHERE ${rel.targetPk.sqlName} IN ($placeholders)"

  def run()(using con: DbCon): Vector[(E, Vector[T])] =
    val parents = fetchParents()
    if parents.isEmpty then return Vector.empty

    val pkIdx = sourcePkIndex
    val parentKeys = extractKeys(parents, pkIdx)
    val pivotPairs = fetchPairsByKeys(pivotQuerySql, parentKeys)
    if pivotPairs.isEmpty then
      return parents.map(p => (p, Vector.empty[T]))

    val uniqueTargetKeys = pivotPairs.map(_._2).distinct
    val targets = fetchByKeys(targetQuerySql, uniqueTargetKeys, targetCodec)

    val tPkIdx = targetPkIndex
    val targetByKey = mutable.LinkedHashMap.empty[Any, T]
    targets.foreach: t =>
      val key = extractKey(t, targetMeta, tPkIdx)
      targetByKey(key) = t

    val parentToTargets = mutable.LinkedHashMap.empty[Any, Vector[T]]
    pivotPairs.foreach: (srcKey, tgtKey) =>
      targetByKey.get(tgtKey).foreach: target =>
        parentToTargets.updateWith(srcKey):
          case Some(existing) => Some(existing :+ target)
          case None           => Some(Vector(target))

    parents.map: parent =>
      val key = extractKey(parent, rootMeta, pkIdx)
      (parent, parentToTargets.getOrElse(key, Vector.empty))

  def first()(using DbCon): Option[(E, Vector[T])] =
    fetchParents().headOption.map: parent =>
      val pkIdx = sourcePkIndex
      val key = extractKey(parent, rootMeta, pkIdx)
      val pivotPairs = fetchPairsByKeys(pivotQuerySql, Vector(key))
      if pivotPairs.isEmpty then (parent, Vector.empty[T])
      else
        val uniqueTargetKeys = pivotPairs.map(_._2).distinct
        val targets = fetchByKeys(targetQuerySql, uniqueTargetKeys, targetCodec)
        val tPkIdx = targetPkIndex
        val targetByKey = mutable.LinkedHashMap.empty[Any, T]
        targets.foreach: t =>
          val k = extractKey(t, targetMeta, tPkIdx)
          targetByKey(k) = t
        val stitched = Vector.newBuilder[T]
        pivotPairs.foreach: (_, tgtKey) =>
          targetByKey.get(tgtKey).foreach(stitched += _)
        (parent, stitched.result())

  def buildQueries: Vector[Frag] =
    Vector(
      rootFrag,
      Frag(pivotQuerySql("?"), Seq.empty, FragWriter.empty),
      Frag(targetQuerySql("?"), Seq.empty, FragWriter.empty)
    )

end PivotEagerQuery
