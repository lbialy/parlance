package ma.chinespirit.parlance

import scala.util.Using

private def extractPkValue(entity: Any, meta: TableMeta[?]): Any =
  entity.asInstanceOf[Product].productElement(meta.pkIndex)

private def queryScalarColumn(sql: String, params: Vector[Any])(using con: DbCon[?]): Vector[Any] =
  Using.resource(con.connection.prepareStatement(sql)): ps =>
    FragWriter.fromKeys(params).write(ps, 1)
    Using.resource(ps.executeQuery()): rs =>
      val builder = Vector.newBuilder[Any]
      while rs.next() do builder += rs.getObject(1)
      builder.result()

private def selectCols(tm: TableMeta[?]): String =
  tm.columns.map(_.sqlName).mkString(", ")

private def queryByColumn[T](value: Any, colSqlName: String, tm: EntityMeta[T])(using DbCon[?]): Vector[T] =
  val sql = s"SELECT ${selectCols(tm)} FROM ${tm.tableName} WHERE $colSqlName = ?"
  Frag(sql, Seq(value), FragWriter.fromKeys(Vector(value)))
    .query[T](using tm)
    .run()

private def queryByColumnIn[T](keys: Vector[Any], colSqlName: String, tm: EntityMeta[T])(using DbCon[?]): Vector[T] =
  if keys.isEmpty then return Vector.empty
  val placeholders = keys.map(_ => "?").mkString(", ")
  val sql = s"SELECT ${selectCols(tm)} FROM ${tm.tableName} WHERE $colSqlName IN ($placeholders)"
  Frag(sql, keys, FragWriter.fromKeys(keys)).query[T](using tm).run()

// --- Group 1a: Mutations ---
extension [EC, E, ID](entity: E)(using repo: Repo[EC, E, ID], con: DbCon[? <: SupportsMutations])
  def save(): Unit = repo.save(entity)
  def delete(): Unit = repo.delete(entity)

// --- Group 1b: Reads ---
extension [EC, E, ID](entity: E)(using repo: Repo[EC, E, ID], con: DbCon[?]) def refresh(): E = repo.refresh(entity)

// --- Group 2: Identity comparison ---
extension [E](entity: E)(using meta: TableMeta[E])
  def is(other: E): Boolean =
    extractPkValue(entity, meta) == extractPkValue(other, meta)
  def isNot(other: E): Boolean =
    extractPkValue(entity, meta) != extractPkValue(other, meta)

// --- Group 3: Change tracking ---
extension [E](entity: E)(using meta: TableMeta[E], con: DbCon[?])
  def isDirty: Boolean =
    con.getOriginal(meta.tableName, extractPkValue(entity, meta)) match
      case Some(original) =>
        val p1 = original.asInstanceOf[Product]
        val p2 = entity.asInstanceOf[Product]
        (0 until p1.productArity).exists(i => p1.productElement(i) != p2.productElement(i))
      case None => false

  def isClean: Boolean = !entity.isDirty

  def getOriginal: E =
    con
      .getOriginal(meta.tableName, extractPkValue(entity, meta))
      .map(_.asInstanceOf[E])
      .getOrElse(entity)

  def getChanges: Map[String, (Any, Any)] =
    con.getOriginal(meta.tableName, extractPkValue(entity, meta)) match
      case Some(original) =>
        val p1 = original.asInstanceOf[Product]
        val p2 = entity.asInstanceOf[Product]
        val cols = meta.columns
        (0 until p1.productArity).flatMap { i =>
          val old = p1.productElement(i)
          val cur = p2.productElement(i)
          if old != cur then Some(cols(i).scalaName -> (old, cur))
          else None
        }.toMap
      case None => Map.empty
end extension

// --- Group 5: Creator extensions ---
extension [EC, E, ID](creator: EC)(using repo: Repo[EC, E, ID])
  def create[D <: DatabaseType]()(using con: DbCon[D], cr: CanReturn[EC, E, D]): E =
    repo.create(creator)

// --- Group 4: SoftDeletes extensions ---
extension [EC, E, ID](entity: E)(using
    repo: Repo[EC, E, ID] & SoftDeletes[EC, E, ID],
    con: DbCon[? <: SupportsMutations]
)
  def forceDelete(): Unit = repo.forceDelete(entity)
  def restore(): Unit = repo.restore(entity)
  def trashed: Boolean = repo.isTrashed(entity)

// --- Group 6: Relationship loading ---
extension [E](entity: E)(using sm: TableMeta[E], con: DbCon[?])

  def load[T](rel: Relationship[E, T])(using tm: EntityMeta[T]): Vector[T] =
    val fkIdx = sm.columnIndex(rel.fk.scalaName)
    val rawValue = entity.asInstanceOf[Product].productElement(fkIdx)
    rawValue match
      case None    => Vector.empty
      case Some(v) => queryByColumn(v, rel.pk.sqlName, tm)
      case v       => queryByColumn(v, rel.pk.sqlName, tm)

  def load[T, CT <: Selectable](rel: BelongsToMany[E, T, CT])(using tm: EntityMeta[T]): Vector[T] =
    val pkIdx = sm.columnIndex(rel.sourcePk.scalaName)
    val pkValue = entity.asInstanceOf[Product].productElement(pkIdx)
    val pivotSql = s"SELECT ${rel.targetFk} FROM ${rel.pivotTable} WHERE ${rel.sourceFk} = ?"
    val targetKeys = queryScalarColumn(pivotSql, Vector(pkValue))
    queryByColumnIn(targetKeys, rel.targetPk.sqlName, tm)

  def load[T, CT <: Selectable](rel: HasManyThrough[E, T, CT])(using tm: EntityMeta[T]): Vector[T] =
    val pkIdx = sm.columnIndex(rel.sourcePk.scalaName)
    val pkValue = entity.asInstanceOf[Product].productElement(pkIdx)
    val intSql = s"SELECT ${rel.intermediatePk.sqlName} FROM ${rel.intermediateTable} WHERE ${rel.sourceFk} = ?"
    val intKeys = queryScalarColumn(intSql, Vector(pkValue))
    queryByColumnIn(intKeys, rel.targetFk.sqlName, tm)

  def load[T, CT <: Selectable](rel: HasOneThrough[E, T, CT])(using tm: EntityMeta[T]): Vector[T] =
    val pkIdx = sm.columnIndex(rel.sourcePk.scalaName)
    val pkValue = entity.asInstanceOf[Product].productElement(pkIdx)
    val intSql = s"SELECT ${rel.intermediatePk.sqlName} FROM ${rel.intermediateTable} WHERE ${rel.sourceFk} = ?"
    val intKeys = queryScalarColumn(intSql, Vector(pkValue))
    queryByColumnIn(intKeys, rel.targetFk.sqlName, tm)

  def load[I, T, CT <: Selectable](rel: ComposedRelationship[E, I, T, CT])(using
      im: EntityMeta[I],
      tm: EntityMeta[T]
  ): Vector[T] =
    val intermediates = load(rel.inner)(using im)
    given TableMeta[I] = im
    intermediates.flatMap(_.load(rel.outer)(using tm))

  def loadOne[T](rel: Relationship[E, T])(using EntityMeta[T]): Option[T] =
    load(rel).headOption

  def loadOne[T, CT <: Selectable](rel: BelongsToMany[E, T, CT])(using EntityMeta[T]): Option[T] =
    load(rel).headOption

  def loadOne[T, CT <: Selectable](rel: HasManyThrough[E, T, CT])(using EntityMeta[T]): Option[T] =
    load(rel).headOption

  def loadOne[T, CT <: Selectable](rel: HasOneThrough[E, T, CT])(using EntityMeta[T]): Option[T] =
    load(rel).headOption

  def loadOne[I, T, CT <: Selectable](rel: ComposedRelationship[E, I, T, CT])(using
      EntityMeta[I],
      EntityMeta[T]
  ): Option[T] =
    load(rel).headOption
end extension
