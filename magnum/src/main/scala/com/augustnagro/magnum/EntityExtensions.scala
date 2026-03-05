package com.augustnagro.magnum

private def extractPkValue(entity: Any, meta: TableMeta[?]): Any =
  entity.asInstanceOf[Product].productElement(meta.pkIndex)

// --- Group 1a: Mutations ---
extension [EC, E, ID](entity: E)(using repo: Repo[EC, E, ID], con: DbCon[? <: SupportsMutations])
  def save(): Unit = repo.save(entity)
  def delete(): Unit = repo.delete(entity)

// --- Group 1b: Reads ---
extension [EC, E, ID](entity: E)(using repo: Repo[EC, E, ID], con: DbCon[?])
  def refresh(): E = repo.refresh(entity)

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

// --- Group 4: SoftDeletes extensions ---
extension [EC, E, ID](entity: E)(using
    repo: Repo[EC, E, ID] & SoftDeletes[EC, E, ID],
    con: DbCon[? <: SupportsMutations]
)
  def forceDelete(): Unit = repo.forceDelete(entity)
  def restore(): Unit = repo.restore(entity)
  def trashed: Boolean = repo.isTrashed(entity)
