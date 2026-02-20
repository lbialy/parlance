package com.augustnagro.magnum

import scala.collection.mutable

class ThroughQuery[E, T] private[magnum] (
    protected val rootFrag: Frag,
    protected val rootCodec: DbCodec[E],
    protected val rootMeta: TableMeta[E],
    private val intermediateTable: String,
    private val sourceFk: String,
    private val intermediatePkSqlName: String,
    private val targetFkScalaName: String,
    private val targetFkSqlName: String,
    private val sourcePkScalaName: String,
    private val targetMeta: TableMeta[T],
    private val targetCodec: DbCodec[T]
) extends EagerQueryBase[E, T]:

  private def sourcePkIndex: Int =
    resolveColumnIndex(rootMeta, sourcePkScalaName)

  private def targetFkIndex: Int =
    resolveColumnIndex(targetMeta, targetFkScalaName)

  private def intermediateQuerySql(placeholders: String): String =
    s"SELECT $sourceFk, $intermediatePkSqlName FROM $intermediateTable WHERE $sourceFk IN ($placeholders)"

  private def targetQuerySql(placeholders: String): String =
    val targetCols = targetMeta.columns.map(_.sqlName).mkString(", ")
    s"SELECT $targetCols FROM ${targetMeta.tableName} WHERE $targetFkSqlName IN ($placeholders)"

  def run()(using con: DbCon): Vector[(E, Vector[T])] =
    val parents = fetchParents()
    if parents.isEmpty then return Vector.empty

    val pkIdx = sourcePkIndex
    val parentKeys = extractKeys(parents, pkIdx)
    val intermediatePairs = fetchPairsByKeys(intermediateQuerySql, parentKeys)
    if intermediatePairs.isEmpty then
      return parents.map(p => (p, Vector.empty[T]))

    val uniqueIntermediateKeys = intermediatePairs.map(_._2).distinct
    val targets = fetchByKeys(targetQuerySql, uniqueIntermediateKeys, targetCodec)
    val targetsByIntermediateKey = groupByKey(targets, targetMeta, targetFkIndex)

    val sourceToTargets = mutable.LinkedHashMap.empty[Any, Vector[T]]
    intermediatePairs.foreach: (srcKey, intKey) =>
      targetsByIntermediateKey.getOrElse(intKey, Vector.empty).foreach: target =>
        sourceToTargets.updateWith(srcKey):
          case Some(existing) => Some(existing :+ target)
          case None           => Some(Vector(target))

    parents.map: parent =>
      val key = extractKey(parent, rootMeta, pkIdx)
      (parent, sourceToTargets.getOrElse(key, Vector.empty))

  def first()(using DbCon): Option[(E, Vector[T])] =
    fetchParents().headOption.map: parent =>
      val pkIdx = sourcePkIndex
      val key = extractKey(parent, rootMeta, pkIdx)
      val intermediatePairs = fetchPairsByKeys(intermediateQuerySql, Vector(key))
      if intermediatePairs.isEmpty then (parent, Vector.empty[T])
      else
        val uniqueIntermediateKeys = intermediatePairs.map(_._2).distinct
        val targets = fetchByKeys(targetQuerySql, uniqueIntermediateKeys, targetCodec)
        val targetsByIntermediateKey = groupByKey(targets, targetMeta, targetFkIndex)
        val stitched = Vector.newBuilder[T]
        intermediatePairs.foreach: (_, intKey) =>
          targetsByIntermediateKey.getOrElse(intKey, Vector.empty).foreach(stitched += _)
        (parent, stitched.result())

  def buildQueries: Vector[Frag] =
    Vector(
      rootFrag,
      Frag(intermediateQuerySql("?"), Seq.empty, FragWriter.empty),
      Frag(targetQuerySql("?"), Seq.empty, FragWriter.empty)
    )

end ThroughQuery
