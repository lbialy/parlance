package ma.chinespirit.parlance

import scala.collection.mutable
import scala.util.Using

trait EagerQueryDef:
  def parentKeyScalaName: String
  def fetchGrouped(parentKeys: Vector[Any])(using DbCon[?]): mutable.LinkedHashMap[Any, Vector[Any]]
  def representativeQueries: Vector[Frag]

object EagerQueryDef:

  def resolveColumnIndex(
      meta: TableMeta[?],
      scalaName: String
  ): Int =
    val idx = meta.columns.indexWhere(_.scalaName == scalaName)
    if idx == -1 then
      throw QueryBuilderException(
        s"Column '$scalaName' not found in ${meta.tableName} columns: ${meta.columns.map(_.scalaName).mkString(", ")}"
      )
    idx

  def extractKey(
      entity: Any,
      meta: TableMeta[?],
      idx: Int
  ): Any =
    QueryBuilderException
      .requireProduct(entity, meta.tableName)
      .productElement(idx)

  def fetchByKeys[A](
      buildSql: String => String,
      keys: Vector[Any],
      codec: DbCodec[A],
      filter: Option[Frag]
  )(using DbCon[?]): Vector[A] =
    val placeholders = keys.map(_ => "?").mkString(", ")
    val baseSql = buildSql(placeholders)
    filter match
      case None =>
        Frag(baseSql, keys, FragWriter.fromKeys(keys))
          .query[A](using codec)
          .run()
      case Some(f) =>
        val sql = baseSql + " AND " + f.sqlString
        val allParams = keys ++ f.params
        val writer = combineWriters(FragWriter.fromKeys(keys), f.writer)
        Frag(sql, allParams, writer)
          .query[A](using codec)
          .run()
  end fetchByKeys

  def fetchPairsByKeys(
      buildSql: String => String,
      keys: Vector[Any],
      filter: Option[Frag]
  )(using con: DbCon[?]): Vector[(Any, Any)] =
    val placeholders = keys.map(_ => "?").mkString(", ")
    val baseSql = buildSql(placeholders)
    val frag = filter match
      case None =>
        Frag(baseSql, keys, FragWriter.fromKeys(keys))
      case Some(f) =>
        val sql = baseSql + " AND " + f.sqlString
        val allParams = keys ++ f.params
        val writer = combineWriters(FragWriter.fromKeys(keys), f.writer)
        Frag(sql, allParams, writer)
    val pairs = Vector.newBuilder[(Any, Any)]
    Using.resource(con.connection.prepareStatement(frag.sqlString)): ps =>
      frag.writer.write(ps, 1)
      Using.resource(ps.executeQuery()): rs =>
        while rs.next() do pairs += ((rs.getObject(1), rs.getObject(2)))
    pairs.result()
  end fetchPairsByKeys

  def groupByKey[A](
      entities: Vector[A],
      meta: TableMeta[?],
      keyIdx: Int
  ): mutable.LinkedHashMap[Any, Vector[A]] =
    val grouped = mutable.LinkedHashMap.empty[Any, Vector[A]]
    entities.foreach: entity =>
      val key = extractKey(entity, meta, keyIdx)
      grouped.updateWith(key):
        case Some(existing) => Some(existing :+ entity)
        case None           => Some(Vector(entity))
    grouped

  private def combineWriters(a: FragWriter, b: FragWriter): FragWriter =
    (ps, pos) =>
      val pos2 = a.write(ps, pos)
      b.write(ps, pos2)

end EagerQueryDef

class DirectEagerDef[E, T](
    private val rootMeta: TableMeta[E],
    private val rel: HasMany[E, T, ?],
    private val childMeta: TableMeta[T],
    private val childCodec: DbCodec[T],
    private val filter: Option[Frag]
) extends EagerQueryDef:

  import EagerQueryDef.*

  def parentKeyScalaName: String = rel.fk.scalaName

  private def parentKeyIndex: Int =
    resolveColumnIndex(rootMeta, rel.fk.scalaName)

  private def childFkIndex: Int =
    resolveColumnIndex(childMeta, rel.pk.scalaName)

  private def childQuerySql(placeholders: String): String =
    val childCols = childMeta.columns.map(_.sqlName).mkString(", ")
    s"SELECT $childCols FROM ${childMeta.tableName} WHERE ${rel.pk.sqlName} IN ($placeholders)"

  def fetchGrouped(parentKeys: Vector[Any])(using DbCon[?]): mutable.LinkedHashMap[Any, Vector[Any]] =
    val children = fetchByKeys(childQuerySql, parentKeys, childCodec, filter)
    val grouped = groupByKey(children, childMeta, childFkIndex)
    grouped.asInstanceOf[mutable.LinkedHashMap[Any, Vector[Any]]]

  def representativeQueries: Vector[Frag] =
    Vector(Frag(childQuerySql("?"), Seq.empty, FragWriter.empty))

end DirectEagerDef

class PivotEagerDef[E, T](
    private val rootMeta: TableMeta[E],
    private val rel: BelongsToMany[E, T, ?],
    private val targetMeta: TableMeta[T],
    private val targetCodec: DbCodec[T],
    private val filter: Option[Frag]
) extends EagerQueryDef:

  import EagerQueryDef.*

  def parentKeyScalaName: String = rel.sourcePk.scalaName

  private def sourcePkIndex: Int =
    resolveColumnIndex(rootMeta, rel.sourcePk.scalaName)

  private def targetPkIndex: Int =
    resolveColumnIndex(targetMeta, rel.targetPk.scalaName)

  private def pivotQuerySql(placeholders: String): String =
    s"SELECT ${rel.sourceFk}, ${rel.targetFk} FROM ${rel.pivotTable} WHERE ${rel.sourceFk} IN ($placeholders)"

  private def targetQuerySql(placeholders: String): String =
    val targetCols = targetMeta.columns.map(_.sqlName).mkString(", ")
    s"SELECT $targetCols FROM ${targetMeta.tableName} WHERE ${rel.targetPk.sqlName} IN ($placeholders)"

  def fetchGrouped(parentKeys: Vector[Any])(using DbCon[?]): mutable.LinkedHashMap[Any, Vector[Any]] =
    val pivotPairs = fetchPairsByKeys(pivotQuerySql, parentKeys, None)
    if pivotPairs.isEmpty then return mutable.LinkedHashMap.empty

    val uniqueTargetKeys = pivotPairs.map(_._2).distinct
    val targets = fetchByKeys(targetQuerySql, uniqueTargetKeys, targetCodec, filter)

    val tPkIdx = targetPkIndex
    val targetByKey = mutable.LinkedHashMap.empty[Any, T]
    targets.foreach: t =>
      val key = extractKey(t, targetMeta, tPkIdx)
      targetByKey(key) = t

    val result = mutable.LinkedHashMap.empty[Any, Vector[Any]]
    pivotPairs.foreach: (srcKey, tgtKey) =>
      targetByKey
        .get(tgtKey)
        .foreach: target =>
          result.updateWith(srcKey):
            case Some(existing) => Some(existing :+ target)
            case None           => Some(Vector(target))
    result
  end fetchGrouped

  def representativeQueries: Vector[Frag] =
    Vector(
      Frag(pivotQuerySql("?"), Seq.empty, FragWriter.empty),
      Frag(targetQuerySql("?"), Seq.empty, FragWriter.empty)
    )

end PivotEagerDef

class ThroughEagerDef[E, T](
    private val rootMeta: TableMeta[E],
    private val intermediateTable: String,
    private val sourceFk: String,
    private val intermediatePkSqlName: String,
    private val targetFkScalaName: String,
    private val targetFkSqlName: String,
    private val sourcePkScalaName: String,
    private val targetMeta: TableMeta[T],
    private val targetCodec: DbCodec[T],
    private val filter: Option[Frag]
) extends EagerQueryDef:

  import EagerQueryDef.*

  def parentKeyScalaName: String = sourcePkScalaName

  private def sourcePkIndex: Int =
    resolveColumnIndex(rootMeta, sourcePkScalaName)

  private def targetFkIndex: Int =
    resolveColumnIndex(targetMeta, targetFkScalaName)

  private def intermediateQuerySql(placeholders: String): String =
    s"SELECT $sourceFk, $intermediatePkSqlName FROM $intermediateTable WHERE $sourceFk IN ($placeholders)"

  private def targetQuerySql(placeholders: String): String =
    val targetCols = targetMeta.columns.map(_.sqlName).mkString(", ")
    s"SELECT $targetCols FROM ${targetMeta.tableName} WHERE $targetFkSqlName IN ($placeholders)"

  def fetchGrouped(parentKeys: Vector[Any])(using DbCon[?]): mutable.LinkedHashMap[Any, Vector[Any]] =
    val intermediatePairs = fetchPairsByKeys(intermediateQuerySql, parentKeys, None)
    if intermediatePairs.isEmpty then return mutable.LinkedHashMap.empty

    val uniqueIntermediateKeys = intermediatePairs.map(_._2).distinct
    val targets = fetchByKeys(targetQuerySql, uniqueIntermediateKeys, targetCodec, filter)
    val targetsByIntermediateKey = groupByKey(targets, targetMeta, targetFkIndex)

    val result = mutable.LinkedHashMap.empty[Any, Vector[Any]]
    intermediatePairs.foreach: (srcKey, intKey) =>
      targetsByIntermediateKey
        .getOrElse(intKey, Vector.empty)
        .foreach: target =>
          result.updateWith(srcKey):
            case Some(existing) => Some(existing :+ target)
            case None           => Some(Vector(target))
    result

  def representativeQueries: Vector[Frag] =
    Vector(
      Frag(intermediateQuerySql("?"), Seq.empty, FragWriter.empty),
      Frag(targetQuerySql("?"), Seq.empty, FragWriter.empty)
    )

end ThroughEagerDef

class ComposedEagerDef[Root, Intermediate, Target](
    private val rootMeta: TableMeta[Root],
    private val innerRel: Relationship[Root, Intermediate],
    private val intermediateMeta: TableMeta[Intermediate],
    private val intermediateCodec: DbCodec[Intermediate],
    private val outerRel: HasMany[Intermediate, Target, ?],
    private val targetMeta: TableMeta[Target],
    private val targetCodec: DbCodec[Target],
    private val intermediateFilter: Option[Frag],
    private val filter: Option[Frag]
) extends EagerQueryDef:

  import EagerQueryDef.*

  def parentKeyScalaName: String = innerRel.fk.scalaName

  private def innerPkIndex: Int =
    resolveColumnIndex(intermediateMeta, innerRel.pk.scalaName)

  private def outerFkIndex: Int =
    resolveColumnIndex(intermediateMeta, outerRel.fk.scalaName)

  private def outerPkIndex: Int =
    resolveColumnIndex(targetMeta, outerRel.pk.scalaName)

  private def intermediateQuerySql(placeholders: String): String =
    val cols = intermediateMeta.columns.map(_.sqlName).mkString(", ")
    s"SELECT $cols FROM ${intermediateMeta.tableName} WHERE ${innerRel.pk.sqlName} IN ($placeholders)"

  private def targetQuerySql(placeholders: String): String =
    val cols = targetMeta.columns.map(_.sqlName).mkString(", ")
    s"SELECT $cols FROM ${targetMeta.tableName} WHERE ${outerRel.pk.sqlName} IN ($placeholders)"

  def fetchGrouped(parentKeys: Vector[Any])(using DbCon[?]): mutable.LinkedHashMap[Any, Vector[Any]] =
    val intermediates = fetchByKeys(intermediateQuerySql, parentKeys, intermediateCodec, intermediateFilter)
    if intermediates.isEmpty then return mutable.LinkedHashMap.empty

    val iPkIdx = innerPkIndex
    val intermediatesByInnerPk = mutable.LinkedHashMap.empty[Any, Vector[Intermediate]]
    intermediates.foreach: i =>
      val key = extractKey(i, intermediateMeta, iPkIdx)
      intermediatesByInnerPk.updateWith(key):
        case Some(existing) => Some(existing :+ i)
        case None           => Some(Vector(i))

    val oFkIdx = outerFkIndex
    val outerFkValues = intermediates.map(i => extractKey(i, intermediateMeta, oFkIdx)).distinct
    val targets = fetchByKeys(targetQuerySql, outerFkValues, targetCodec, filter)

    val oPkIdx = outerPkIndex
    val targetsByOuterPk = mutable.LinkedHashMap.empty[Any, Vector[Target]]
    targets.foreach: t =>
      val key = extractKey(t, targetMeta, oPkIdx)
      targetsByOuterPk.updateWith(key):
        case Some(existing) => Some(existing :+ t)
        case None           => Some(Vector(t))

    val result = mutable.LinkedHashMap.empty[Any, Vector[Any]]
    parentKeys.distinct.foreach: rootKey =>
      val myIntermediates = intermediatesByInnerPk.getOrElse(rootKey, Vector.empty)
      val myTargets = myIntermediates.flatMap: i =>
        val outerFkVal = extractKey(i, intermediateMeta, oFkIdx)
        targetsByOuterPk.getOrElse(outerFkVal, Vector.empty)
      if myTargets.nonEmpty then result(rootKey) = myTargets
    result.asInstanceOf[mutable.LinkedHashMap[Any, Vector[Any]]]
  end fetchGrouped

  def representativeQueries: Vector[Frag] =
    Vector(
      Frag(intermediateQuerySql("?"), Seq.empty, FragWriter.empty),
      Frag(targetQuerySql("?"), Seq.empty, FragWriter.empty)
    )

end ComposedEagerDef

class PivotWithDataEagerDef[E, T, P](
    private val rootMeta: TableMeta[E],
    private val rel: BelongsToMany[E, T, ?],
    private val targetMeta: TableMeta[T],
    private val targetCodec: DbCodec[T],
    private val pivotMeta: TableMeta[P],
    private val pivotCodec: DbCodec[P],
    private val pivotFilter: Option[Frag]
) extends EagerQueryDef:

  import EagerQueryDef.*

  def parentKeyScalaName: String = rel.sourcePk.scalaName

  private def sourceFkPivotIndex: Int =
    pivotMeta.columns.indexWhere(_.sqlName == rel.sourceFk)

  private def targetFkPivotIndex: Int =
    pivotMeta.columns.indexWhere(_.sqlName == rel.targetFk)

  private def targetPkIndex: Int =
    resolveColumnIndex(targetMeta, rel.targetPk.scalaName)

  private def pivotQuerySql(placeholders: String): String =
    val pivotCols = pivotMeta.columns.map(_.sqlName).mkString(", ")
    s"SELECT $pivotCols FROM ${pivotMeta.tableName} WHERE ${rel.sourceFk} IN ($placeholders)"

  private def targetQuerySql(placeholders: String): String =
    val targetCols = targetMeta.columns.map(_.sqlName).mkString(", ")
    s"SELECT $targetCols FROM ${targetMeta.tableName} WHERE ${rel.targetPk.sqlName} IN ($placeholders)"

  def fetchGrouped(parentKeys: Vector[Any])(using DbCon[?]): mutable.LinkedHashMap[Any, Vector[Any]] =
    // 1. Fetch all pivot rows for these source keys
    val pivots = fetchByKeys(pivotQuerySql, parentKeys, pivotCodec, pivotFilter)
    if pivots.isEmpty then return mutable.LinkedHashMap.empty

    // 2. Extract source/target FKs from pivot rows
    val srcFkIdx = sourceFkPivotIndex
    val tgtFkIdx = targetFkPivotIndex
    val pivotsBySourceAndTarget = pivots.map: p =>
      val prod = p.asInstanceOf[Product]
      val srcKey = prod.productElement(srcFkIdx)
      val tgtKey = prod.productElement(tgtFkIdx)
      (srcKey, tgtKey, p)

    // 3. Fetch all target entities
    val uniqueTargetKeys = pivotsBySourceAndTarget.map(_._2).distinct
    val targets = fetchByKeys(targetQuerySql, uniqueTargetKeys, targetCodec, None)
    val tPkIdx = targetPkIndex
    val targetByKey = mutable.LinkedHashMap.empty[Any, T]
    targets.foreach: t =>
      val key = extractKey(t, targetMeta, tPkIdx)
      targetByKey(key) = t

    // 4. Build result: sourceKey → Vector[(T, P)] stored as Any tuples
    val result = mutable.LinkedHashMap.empty[Any, Vector[Any]]
    pivotsBySourceAndTarget.foreach: (srcKey, tgtKey, pivot) =>
      targetByKey
        .get(tgtKey)
        .foreach: target =>
          val pair: (T, P) = (target, pivot)
          result.updateWith(srcKey):
            case Some(existing) => Some(existing :+ pair)
            case None           => Some(Vector(pair))
    result
  end fetchGrouped

  def representativeQueries: Vector[Frag] =
    Vector(
      Frag(pivotQuerySql("?"), Seq.empty, FragWriter.empty),
      Frag(targetQuerySql("?"), Seq.empty, FragWriter.empty)
    )

end PivotWithDataEagerDef
