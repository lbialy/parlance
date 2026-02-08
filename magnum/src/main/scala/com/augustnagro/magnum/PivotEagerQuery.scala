package com.augustnagro.magnum

import java.sql.PreparedStatement
import scala.collection.mutable
import scala.util.Using

class PivotEagerQuery[E, T] private[magnum] (
    private val rootFrag: Frag,
    private val rootCodec: DbCodec[E],
    private val rootMeta: TableMeta[E],
    private val rel: BelongsToMany[E, T, ?],
    private val targetMeta: TableMeta[T],
    private val targetCodec: DbCodec[T]
):

  private def sourcePkIndex: Int =
    rootMeta.columns.indexWhere(_.scalaName == rel.sourcePk.scalaName)

  private def targetPkIndex: Int =
    targetMeta.columns.indexWhere(_.scalaName == rel.targetPk.scalaName)

  def run()(using con: DbCon): Vector[(E, Vector[T])] =
    val parents = rootFrag.query[E](using rootCodec).run()
    if parents.isEmpty then return Vector.empty

    val pkIdx = sourcePkIndex
    val parentKeys = parents.map(p =>
      p.asInstanceOf[Product].productElement(pkIdx)
    )

    val pivotPairs = fetchPivotPairs(parentKeys)
    if pivotPairs.isEmpty then
      return parents.map(p => (p, Vector.empty[T]))

    val uniqueTargetKeys = pivotPairs.map(_._2).distinct
    val targets = fetchTargets(uniqueTargetKeys)

    val tPkIdx = targetPkIndex
    val targetByKey = mutable.LinkedHashMap.empty[Any, T]
    targets.foreach: t =>
      val key = t.asInstanceOf[Product].productElement(tPkIdx)
      targetByKey(key) = t

    val parentToTargets = mutable.LinkedHashMap.empty[Any, Vector[T]]
    pivotPairs.foreach: (srcKey, tgtKey) =>
      targetByKey.get(tgtKey).foreach: target =>
        parentToTargets.updateWith(srcKey):
          case Some(existing) => Some(existing :+ target)
          case None           => Some(Vector(target))

    parents.map: parent =>
      val key = parent.asInstanceOf[Product].productElement(pkIdx)
      (parent, parentToTargets.getOrElse(key, Vector.empty))

  def first()(using DbCon): Option[(E, Vector[T])] =
    val parents = rootFrag.query[E](using rootCodec).run()
    parents.headOption.map: parent =>
      val pkIdx = sourcePkIndex
      val key = parent.asInstanceOf[Product].productElement(pkIdx)
      val pivotPairs = fetchPivotPairs(Vector(key))
      if pivotPairs.isEmpty then (parent, Vector.empty[T])
      else
        val uniqueTargetKeys = pivotPairs.map(_._2).distinct
        val targets = fetchTargets(uniqueTargetKeys)
        (parent, targets)

  private def pivotQuerySql(placeholders: String): String =
    s"SELECT ${rel.sourceFk}, ${rel.targetFk} FROM ${rel.pivotTable} WHERE ${rel.sourceFk} IN ($placeholders)"

  private def targetQuerySql(placeholders: String): String =
    val targetCols = targetMeta.columns.map(_.sqlName).mkString(", ")
    s"SELECT $targetCols FROM ${targetMeta.tableName} WHERE ${rel.targetPk.sqlName} IN ($placeholders)"

  private def fetchPivotPairs(keys: Vector[Any])(using con: DbCon): Vector[(Any, Any)] =
    val pivotSql = pivotQuerySql(keys.map(_ => "?").mkString(", "))

    val writer: FragWriter = (ps: PreparedStatement, pos: Int) =>
      var i = pos
      keys.foreach: key =>
        ps.setObject(i, key)
        i += 1
      i

    val frag = Frag(pivotSql, keys, writer)
    val pairs = Vector.newBuilder[(Any, Any)]
    Using.resource(con.connection.prepareStatement(frag.sqlString)): ps =>
      frag.writer.write(ps, 1)
      Using.resource(ps.executeQuery()): rs =>
        while rs.next() do
          pairs += ((rs.getObject(1), rs.getObject(2)))
    pairs.result()

  private def fetchTargets(keys: Vector[Any])(using DbCon): Vector[T] =
    val targetSql = targetQuerySql(keys.map(_ => "?").mkString(", "))

    val writer: FragWriter = (ps: PreparedStatement, pos: Int) =>
      var i = pos
      keys.foreach: key =>
        ps.setObject(i, key)
        i += 1
      i

    Frag(targetSql, keys, writer)
      .query[T](using targetCodec)
      .run()

  def buildQueries: Vector[Frag] =
    Vector(
      rootFrag,
      Frag(pivotQuerySql("?"), Seq.empty, FragWriter.empty),
      Frag(targetQuerySql("?"), Seq.empty, FragWriter.empty)
    )

  def debugPrintSql(using DbCon): this.type =
    DebugSql.printDebug(buildQueries)
    this

end PivotEagerQuery
