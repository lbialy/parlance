package com.augustnagro.magnum

import java.sql.PreparedStatement
import scala.collection.mutable
import scala.util.Using

class ThroughQuery[E, T] private[magnum] (
    private val rootFrag: Frag,
    private val rootCodec: DbCodec[E],
    private val rootMeta: TableMeta[E],
    private val intermediateTable: String,
    private val sourceFk: String,
    private val intermediatePkSqlName: String,
    private val targetFkScalaName: String,
    private val targetFkSqlName: String,
    private val sourcePkScalaName: String,
    private val targetMeta: TableMeta[T],
    private val targetCodec: DbCodec[T]
):

  private def sourcePkIndex: Int =
    rootMeta.columns.indexWhere(_.scalaName == sourcePkScalaName)

  private def targetFkIndex: Int =
    targetMeta.columns.indexWhere(_.scalaName == targetFkScalaName)

  def run()(using con: DbCon): Vector[(E, Vector[T])] =
    val parents = rootFrag.query[E](using rootCodec).run()
    if parents.isEmpty then return Vector.empty

    val pkIdx = sourcePkIndex
    val parentKeys = parents.map(p =>
      p.asInstanceOf[Product].productElement(pkIdx)
    )

    val intermediatePairs = fetchIntermediatePairs(parentKeys)
    if intermediatePairs.isEmpty then
      return parents.map(p => (p, Vector.empty[T]))

    val uniqueIntermediateKeys = intermediatePairs.map(_._2).distinct
    val targets = fetchTargets(uniqueIntermediateKeys)

    val tFkIdx = targetFkIndex
    val targetsByIntermediateKey = mutable.LinkedHashMap.empty[Any, Vector[T]]
    targets.foreach: t =>
      val fkValue = t.asInstanceOf[Product].productElement(tFkIdx)
      targetsByIntermediateKey.updateWith(fkValue):
        case Some(existing) => Some(existing :+ t)
        case None           => Some(Vector(t))

    val sourceToTargets = mutable.LinkedHashMap.empty[Any, Vector[T]]
    intermediatePairs.foreach: (srcKey, intKey) =>
      targetsByIntermediateKey.getOrElse(intKey, Vector.empty).foreach: target =>
        sourceToTargets.updateWith(srcKey):
          case Some(existing) => Some(existing :+ target)
          case None           => Some(Vector(target))

    parents.map: parent =>
      val key = parent.asInstanceOf[Product].productElement(pkIdx)
      (parent, sourceToTargets.getOrElse(key, Vector.empty))

  def first()(using DbCon): Option[(E, Vector[T])] =
    val parents = rootFrag.query[E](using rootCodec).run()
    parents.headOption.map: parent =>
      val pkIdx = sourcePkIndex
      val key = parent.asInstanceOf[Product].productElement(pkIdx)
      val intermediatePairs = fetchIntermediatePairs(Vector(key))
      if intermediatePairs.isEmpty then (parent, Vector.empty[T])
      else
        val uniqueIntermediateKeys = intermediatePairs.map(_._2).distinct
        val targets = fetchTargets(uniqueIntermediateKeys)
        (parent, targets)

  private def intermediateQuerySql(placeholders: String): String =
    s"SELECT $sourceFk, $intermediatePkSqlName FROM $intermediateTable WHERE $sourceFk IN ($placeholders)"

  private def targetQuerySql(placeholders: String): String =
    val targetCols = targetMeta.columns.map(_.sqlName).mkString(", ")
    s"SELECT $targetCols FROM ${targetMeta.tableName} WHERE $targetFkSqlName IN ($placeholders)"

  private def fetchIntermediatePairs(keys: Vector[Any])(using con: DbCon): Vector[(Any, Any)] =
    val sql = intermediateQuerySql(keys.map(_ => "?").mkString(", "))

    val writer: FragWriter = (ps: PreparedStatement, pos: Int) =>
      var i = pos
      keys.foreach: key =>
        ps.setObject(i, key)
        i += 1
      i

    val frag = Frag(sql, keys, writer)
    val pairs = Vector.newBuilder[(Any, Any)]
    Using.resource(con.connection.prepareStatement(frag.sqlString)): ps =>
      frag.writer.write(ps, 1)
      Using.resource(ps.executeQuery()): rs =>
        while rs.next() do
          pairs += ((rs.getObject(1), rs.getObject(2)))
    pairs.result()

  private def fetchTargets(keys: Vector[Any])(using DbCon): Vector[T] =
    val sql = targetQuerySql(keys.map(_ => "?").mkString(", "))

    val writer: FragWriter = (ps: PreparedStatement, pos: Int) =>
      var i = pos
      keys.foreach: key =>
        ps.setObject(i, key)
        i += 1
      i

    Frag(sql, keys, writer)
      .query[T](using targetCodec)
      .run()

  def buildQueries: Vector[Frag] =
    Vector(
      rootFrag,
      Frag(intermediateQuerySql("?"), Seq.empty, FragWriter.empty),
      Frag(targetQuerySql("?"), Seq.empty, FragWriter.empty)
    )

  def debugPrintSql(using DbCon): this.type =
    DebugSql.printDebug(buildQueries)
    this

end ThroughQuery
