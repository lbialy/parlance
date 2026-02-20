package com.augustnagro.magnum

class EagerQuery[E, T] private[magnum] (
    protected val rootFrag: Frag,
    protected val rootCodec: DbCodec[E],
    protected val rootMeta: TableMeta[E],
    private val rel: HasMany[E, T, ?],
    private val childMeta: TableMeta[T],
    private val childCodec: DbCodec[T]
) extends EagerQueryBase[E, T]:

  private def parentKeyIndex: Int =
    resolveColumnIndex(rootMeta, rel.fk.scalaName)

  private def childFkIndex: Int =
    resolveColumnIndex(childMeta, rel.pk.scalaName)

  private def childQuerySql(placeholders: String): String =
    val childCols = childMeta.columns.map(_.sqlName).mkString(", ")
    s"SELECT $childCols FROM ${childMeta.tableName} WHERE ${rel.pk.sqlName} IN ($placeholders)"

  def run()(using DbCon): Vector[(E, Vector[T])] =
    val parents = fetchParents()
    if parents.isEmpty then return Vector.empty

    val pkIdx = parentKeyIndex
    val parentKeys = extractKeys(parents, pkIdx)
    val children = fetchByKeys(childQuerySql, parentKeys, childCodec)
    val grouped = groupByKey(children, childMeta, childFkIndex)

    parents.map: parent =>
      val key = extractKey(parent, rootMeta, pkIdx)
      (parent, grouped.getOrElse(key, Vector.empty))

  def first()(using DbCon): Option[(E, Vector[T])] =
    fetchParents().headOption.map: parent =>
      val pkIdx = parentKeyIndex
      val key = extractKey(parent, rootMeta, pkIdx)
      val children = fetchByKeys(childQuerySql, Vector(key), childCodec)
      (parent, children)

  def buildQueries: Vector[Frag] =
    Vector(rootFrag, Frag(childQuerySql("?"), Seq.empty, FragWriter.empty))

end EagerQuery
