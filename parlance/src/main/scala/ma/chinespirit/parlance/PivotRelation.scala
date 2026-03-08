package ma.chinespirit.parlance

import scala.collection.mutable
import scala.deriving.Mirror
import scala.quoted.*
import scala.util.Using

case class SyncResult(attached: Int, detached: Int, unchanged: Int)

/** Read-only pivot relation — supports withRelatedAndPivot and detach, but NOT attach. Created via `belongsToMany.withPivot[P]`.
  */
class PivotRelation[S, T, P, +CT <: Selectable, +PCT <: Selectable](
    val underlying: BelongsToMany[S, T, CT],
    val pivotMeta: EntityMeta[P]
):
  def detach(source: S, targets: T*)(using
      sm: EntityMeta[S],
      tm: EntityMeta[T],
      con: DbCon[?]
  ): Int =
    BelongsToManyOps.detachImpl(underlying, source, targets, sm, tm, con)

  def detachAll(source: S)(using sm: EntityMeta[S], con: DbCon[?]): Int =
    BelongsToManyOps.detachAllImpl(underlying, source, sm, con)

/** Writable pivot relation — supports attach with typed creator. Created via `belongsToMany.withPivot[P, PC]`.
  */
class WritablePivotRelation[S, T, P, PC, +CT <: Selectable, +PCT <: Selectable](
    underlying: BelongsToMany[S, T, CT],
    pivotMeta: EntityMeta[P],
    val creatorCodec: DbCodec[PC]
) extends PivotRelation[S, T, P, CT, PCT](underlying, pivotMeta):

  def attach(creator: PC)(using con: DbCon[?]): Unit =
    attachAll(List(creator))

  def attachAll(creators: Iterable[PC])(using con: DbCon[?]): Unit =
    if creators.isEmpty then return
    val allCols = pivotMeta.columns.map(_.sqlName).mkString(", ")
    val placeholders = pivotMeta.columns.map(_ => "?").mkString(", ")
    val sql = s"INSERT INTO ${pivotMeta.tableName} ($allCols) VALUES ($placeholders)"
    Using.resource(con.connection.prepareStatement(sql)): ps =>
      creators.foreach: c =>
        creatorCodec.writeSingle(c, ps, 1)
        ps.addBatch()
      ps.executeBatch()

  def sync(source: S, targets: Iterable[T], creator: T => PC)(using
      sm: EntityMeta[S],
      tm: EntityMeta[T],
      con: DbCon[?]
  ): SyncResult =
    val rel = underlying
    val sourcePkIdx = sm.columnIndex(rel.sourcePk.scalaName)
    val targetPkIdx = tm.columnIndex(rel.targetPk.scalaName)
    val sourceKey = source.asInstanceOf[Product].productElement(sourcePkIdx)

    val currentKeys = BelongsToManyOps.fetchTargetKeys(rel, sourceKey, con)
    val currentKeySet = currentKeys.toSet

    val desiredTargets = targets.toVector
    val desiredKeys = desiredTargets.map(t => t.asInstanceOf[Product].productElement(targetPkIdx))
    val desiredKeySet = desiredKeys.toSet

    val toDetach = currentKeySet -- desiredKeySet
    val toAttach = desiredTargets.filter: t =>
      val k = t.asInstanceOf[Product].productElement(targetPkIdx)
      !currentKeySet.contains(k)
    val unchanged = currentKeySet.intersect(desiredKeySet).size

    val detached =
      if toDetach.isEmpty then 0
      else BelongsToManyOps.detachByTargetKeys(rel, sourceKey, toDetach.toVector, con)

    if toAttach.nonEmpty then attachAll(toAttach.map(creator))

    SyncResult(attached = toAttach.size, detached = detached, unchanged = unchanged)
  end sync

end WritablePivotRelation

// === Top-level extension methods (importable via `import ma.chinespirit.parlance.*`) ===

extension [S, T, CT <: Selectable](rel: BelongsToMany[S, T, CT])
  /** Read-only pivot: can eager-load pivot data, detach, but NOT attach. */
  transparent inline def withPivot[P](using inline pm: EntityMeta[P]): Any =
    ${ withPivotImpl[S, T, P, CT]('rel, 'pm) }

  /** Read-write pivot: can attach with typed creator. */
  transparent inline def withPivot[P, PC](using inline pm: EntityMeta[P])(using inline cc: DbCodec[PC]): Any =
    ${ withPivotWithCreatorImpl[S, T, P, PC, CT]('rel, 'pm, 'cc) }

  def attach(source: S, targets: T*)(using
      sm: EntityMeta[S],
      tm: EntityMeta[T],
      con: DbCon[?]
  ): Int =
    BelongsToManyOps.attachImpl(rel, source, targets, sm, tm, con)

  def detach(source: S, targets: T*)(using
      sm: EntityMeta[S],
      tm: EntityMeta[T],
      con: DbCon[?]
  ): Int =
    BelongsToManyOps.detachImpl(rel, source, targets, sm, tm, con)

  def detachAll(source: S)(using sm: EntityMeta[S], con: DbCon[?]): Int =
    BelongsToManyOps.detachAllImpl(rel, source, sm, con)

  def sync(source: S, targets: Iterable[T])(using
      sm: EntityMeta[S],
      tm: EntityMeta[T],
      con: DbCon[?]
  ): SyncResult =
    BelongsToManyOps.syncImpl(rel, source, targets, sm, tm, con)
end extension

// === Macro implementations for withPivot ===

private def withPivotImpl[S: Type, T: Type, P: Type, CT <: Selectable: Type](
    rel: Expr[BelongsToMany[S, T, CT]],
    pm: Expr[EntityMeta[P]]
)(using Quotes): Expr[Any] =
  import quotes.reflect.*
  val pct = Relationship.computeColumnsRefinementFor[P]()
  pct.asType match
    case '[pctType] =>
      '{ new PivotRelation[S, T, P, CT, pctType & Selectable]($rel, $pm) }

private def withPivotWithCreatorImpl[S: Type, T: Type, P: Type, PC: Type, CT <: Selectable: Type](
    rel: Expr[BelongsToMany[S, T, CT]],
    pm: Expr[EntityMeta[P]],
    cc: Expr[DbCodec[PC]]
)(using Quotes): Expr[Any] =
  import quotes.reflect.*
  val pct = Relationship.computeColumnsRefinementFor[P]()
  pct.asType match
    case '[pctType] =>
      '{ new WritablePivotRelation[S, T, P, PC, CT, pctType & Selectable]($rel, $pm, $cc) }

// === Shared pivot operation helpers ===

private[parlance] object BelongsToManyOps:

  def attachImpl[S, T](
      rel: BelongsToMany[S, T, ?],
      source: S,
      targets: Seq[T],
      sm: EntityMeta[S],
      tm: EntityMeta[T],
      con: DbCon[?]
  ): Int =
    if targets.isEmpty then return 0
    val sourcePkIdx = sm.columnIndex(rel.sourcePk.scalaName)
    val targetPkIdx = tm.columnIndex(rel.targetPk.scalaName)
    val sourceKey = source.asInstanceOf[Product].productElement(sourcePkIdx)
    val sql = s"INSERT INTO ${rel.pivotTable} (${rel.sourceFk}, ${rel.targetFk}) VALUES (?, ?)"
    Using.resource(con.connection.prepareStatement(sql)): ps =>
      targets.foreach: t =>
        val targetKey = t.asInstanceOf[Product].productElement(targetPkIdx)
        ps.setObject(1, sourceKey)
        ps.setObject(2, targetKey)
        ps.addBatch()
      ps.executeBatch().sum
  end attachImpl

  def detachImpl[S, T](
      rel: BelongsToMany[S, T, ?],
      source: S,
      targets: Seq[T],
      sm: EntityMeta[S],
      tm: EntityMeta[T],
      con: DbCon[?]
  ): Int =
    if targets.isEmpty then return 0
    val sourcePkIdx = sm.columnIndex(rel.sourcePk.scalaName)
    val targetPkIdx = tm.columnIndex(rel.targetPk.scalaName)
    val sourceKey = source.asInstanceOf[Product].productElement(sourcePkIdx)
    val targetKeys = targets.map(t => t.asInstanceOf[Product].productElement(targetPkIdx))
    detachByTargetKeys(rel, sourceKey, targetKeys.toVector, con)

  def detachByTargetKeys(
      rel: BelongsToMany[?, ?, ?],
      sourceKey: Any,
      targetKeys: Vector[Any],
      con: DbCon[?]
  ): Int =
    if targetKeys.isEmpty then return 0
    val placeholders = targetKeys.map(_ => "?").mkString(", ")
    val sql = s"DELETE FROM ${rel.pivotTable} WHERE ${rel.sourceFk} = ? AND ${rel.targetFk} IN ($placeholders)"
    Using.resource(con.connection.prepareStatement(sql)): ps =>
      ps.setObject(1, sourceKey)
      targetKeys.zipWithIndex.foreach: (k, i) =>
        ps.setObject(i + 2, k)
      ps.executeUpdate()

  def detachAllImpl[S](
      rel: BelongsToMany[S, ?, ?],
      source: S,
      sm: EntityMeta[S],
      con: DbCon[?]
  ): Int =
    val sourcePkIdx = sm.columnIndex(rel.sourcePk.scalaName)
    val sourceKey = source.asInstanceOf[Product].productElement(sourcePkIdx)
    val sql = s"DELETE FROM ${rel.pivotTable} WHERE ${rel.sourceFk} = ?"
    Using.resource(con.connection.prepareStatement(sql)): ps =>
      ps.setObject(1, sourceKey)
      ps.executeUpdate()

  def fetchTargetKeys(
      rel: BelongsToMany[?, ?, ?],
      sourceKey: Any,
      con: DbCon[?]
  ): Vector[Any] =
    val sql = s"SELECT ${rel.targetFk} FROM ${rel.pivotTable} WHERE ${rel.sourceFk} = ?"
    val keys = Vector.newBuilder[Any]
    Using.resource(con.connection.prepareStatement(sql)): ps =>
      ps.setObject(1, sourceKey)
      Using.resource(ps.executeQuery()): rs =>
        while rs.next() do keys += rs.getObject(1)
    keys.result()

  def syncImpl[S, T](
      rel: BelongsToMany[S, T, ?],
      source: S,
      targets: Iterable[T],
      sm: EntityMeta[S],
      tm: EntityMeta[T],
      con: DbCon[?]
  ): SyncResult =
    val sourcePkIdx = sm.columnIndex(rel.sourcePk.scalaName)
    val targetPkIdx = tm.columnIndex(rel.targetPk.scalaName)
    val sourceKey = source.asInstanceOf[Product].productElement(sourcePkIdx)

    val currentKeys = fetchTargetKeys(rel, sourceKey, con)
    val currentKeySet = currentKeys.toSet

    val desiredTargets = targets.toVector
    val desiredKeys = desiredTargets.map(t => t.asInstanceOf[Product].productElement(targetPkIdx))
    val desiredKeySet = desiredKeys.toSet

    val toDetachKeys = currentKeySet -- desiredKeySet
    val toAttachTargets = desiredTargets.filter: t =>
      val k = t.asInstanceOf[Product].productElement(targetPkIdx)
      !currentKeySet.contains(k)
    val unchanged = currentKeySet.intersect(desiredKeySet).size

    val detached =
      if toDetachKeys.isEmpty then 0
      else detachByTargetKeys(rel, sourceKey, toDetachKeys.toVector, con)

    val attached =
      if toAttachTargets.isEmpty then 0
      else
        val sql = s"INSERT INTO ${rel.pivotTable} (${rel.sourceFk}, ${rel.targetFk}) VALUES (?, ?)"
        Using.resource(con.connection.prepareStatement(sql)): ps =>
          toAttachTargets.foreach: t =>
            val targetKey = t.asInstanceOf[Product].productElement(targetPkIdx)
            ps.setObject(1, sourceKey)
            ps.setObject(2, targetKey)
            ps.addBatch()
          ps.executeBatch().sum

    SyncResult(attached = attached, detached = detached, unchanged = unchanged)
  end syncImpl

end BelongsToManyOps
