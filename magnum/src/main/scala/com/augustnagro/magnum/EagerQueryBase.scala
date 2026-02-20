package com.augustnagro.magnum

import scala.collection.mutable
import scala.util.Using

private[magnum] trait EagerQueryBase[E, T]:
  protected def rootFrag: Frag
  protected def rootCodec: DbCodec[E]
  protected def rootMeta: TableMeta[E]

  def buildQueries: Vector[Frag]

  def debugPrintSql(using DbCon): this.type =
    DebugSql.printDebug(buildQueries)
    this

  protected def resolveColumnIndex(
      meta: TableMeta[?],
      scalaName: String
  ): Int =
    val idx = meta.columns.indexWhere(_.scalaName == scalaName)
    if idx == -1 then
      throw QueryBuilderException(
        s"Column '$scalaName' not found in ${meta.tableName} columns: ${meta.columns.map(_.scalaName).mkString(", ")}"
      )
    idx

  protected def extractKey(
      entity: Any,
      meta: TableMeta[?],
      idx: Int
  ): Any =
    QueryBuilderException
      .requireProduct(entity, meta.tableName)
      .productElement(idx)

  protected def fetchParents()(using DbCon): Vector[E] =
    rootFrag.query[E](using rootCodec).run()

  protected def extractKeys(
      parents: Vector[E],
      idx: Int
  ): Vector[Any] =
    parents.map(p => extractKey(p, rootMeta, idx))

  protected def fetchByKeys[A](
      buildSql: String => String,
      keys: Vector[Any],
      codec: DbCodec[A]
  )(using DbCon): Vector[A] =
    val sql = buildSql(keys.map(_ => "?").mkString(", "))
    Frag(sql, keys, FragWriter.fromKeys(keys))
      .query[A](using codec)
      .run()

  protected def fetchPairsByKeys(
      buildSql: String => String,
      keys: Vector[Any]
  )(using con: DbCon): Vector[(Any, Any)] =
    val sql = buildSql(keys.map(_ => "?").mkString(", "))
    val frag = Frag(sql, keys, FragWriter.fromKeys(keys))
    val pairs = Vector.newBuilder[(Any, Any)]
    Using.resource(con.connection.prepareStatement(frag.sqlString)): ps =>
      frag.writer.write(ps, 1)
      Using.resource(ps.executeQuery()): rs =>
        while rs.next() do
          pairs += ((rs.getObject(1), rs.getObject(2)))
    pairs.result()

  protected def groupByKey[A](
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

end EagerQueryBase
