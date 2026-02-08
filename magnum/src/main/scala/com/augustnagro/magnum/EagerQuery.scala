package com.augustnagro.magnum

import java.sql.PreparedStatement
import scala.collection.mutable
import scala.util.Using

class EagerQuery[E, T] private[magnum] (
    private val rootFrag: Frag,
    private val rootCodec: DbCodec[E],
    private val rootMeta: TableMeta[E],
    private val rel: HasMany[E, T, ?],
    private val childMeta: TableMeta[T],
    private val childCodec: DbCodec[T]
):

  private def parentKeyIndex: Int =
    rootMeta.columns.indexWhere(_.scalaName == rel.fk.scalaName)

  private def childFkIndex: Int =
    childMeta.columns.indexWhere(_.scalaName == rel.pk.scalaName)

  def run()(using DbCon): Vector[(E, Vector[T])] =
    val parents = rootFrag.query[E](using rootCodec).run()
    if parents.isEmpty then return Vector.empty

    val pkIdx = parentKeyIndex
    val parentKeys = parents.map(p =>
      p.asInstanceOf[Product].productElement(pkIdx)
    )

    val children = fetchChildren(parentKeys)
    val fkIdx = childFkIndex
    val grouped = mutable.LinkedHashMap.empty[Any, Vector[T]]
    children.foreach: child =>
      val fkValue = child.asInstanceOf[Product].productElement(fkIdx)
      grouped.updateWith(fkValue):
        case Some(existing) => Some(existing :+ child)
        case None           => Some(Vector(child))

    parents.map: parent =>
      val key = parent.asInstanceOf[Product].productElement(pkIdx)
      (parent, grouped.getOrElse(key, Vector.empty))

  def first()(using DbCon): Option[(E, Vector[T])] =
    val parents = rootFrag.query[E](using rootCodec).run()
    parents.headOption.map: parent =>
      val pkIdx = parentKeyIndex
      val key = parent.asInstanceOf[Product].productElement(pkIdx)
      val children = fetchChildren(Vector(key))
      (parent, children)

  private def childQuerySql(placeholders: String): String =
    val childCols = childMeta.columns.map(_.sqlName).mkString(", ")
    s"SELECT $childCols FROM ${childMeta.tableName} WHERE ${rel.pk.sqlName} IN ($placeholders)"

  private def fetchChildren(keys: Vector[Any])(using con: DbCon): Vector[T] =
    val childSql = childQuerySql(keys.map(_ => "?").mkString(", "))

    val writer: FragWriter = (ps: PreparedStatement, pos: Int) =>
      var i = pos
      keys.foreach: key =>
        ps.setObject(i, key)
        i += 1
      i

    Frag(childSql, keys, writer)
      .query[T](using childCodec)
      .run()

  def buildQueries: Vector[Frag] =
    val childSql = childQuerySql("?")
    Vector(rootFrag, Frag(childSql, Seq.empty, FragWriter.empty))

  def debugPrintSql(using DbCon): this.type =
    DebugSql.printDebug(buildQueries)
    this

end EagerQuery
