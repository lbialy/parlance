package com.augustnagro.magnum

/** Mixin trait for soft-delete behavior on a [[Repo]].
  *
  * Adds a scope that filters rows where the deleted-at column is NULL (i.e. not deleted), and overrides write methods to SET the timestamp
  * instead of issuing a real DELETE.
  *
  * Usage:
  * {{{
  *   val repo = new Repo[SdUser, SdUser, Long] with SoftDeletes[SdUser, SdUser, Long]
  * }}}
  *
  * The entity must have an `Option` -typed deleted-at field (e.g. `deletedAt: Option[OffsetDateTime]`). Override [[deletedAtFieldName]] if
  * the Scala field is not called `deletedAt`.
  */
trait SoftDeletes[EC, E, ID]:
  self: Repo[EC, E, ID] =>

  /** Scala field name for the deleted-at column. Override if not "deletedAt". */
  def deletedAtFieldName: String = "deletedAt"

  // --- internals ---

  private lazy val sdCol: Col[?] =
    self.entityMeta
      .columnByName(deletedAtFieldName)
      .getOrElse(
        throw new IllegalStateException(
          s"SoftDeletes: column '$deletedAtFieldName' not found in ${self.entityMeta.tableName}. " +
            "Override deletedAtFieldName if the field has a different name."
        )
      )

  private lazy val sdColSql: String = sdCol.sqlName
  private lazy val tbl: String = self.entityMeta.tableName
  private lazy val pkSql: String = self.entityMeta.primaryKey.sqlName

  private def extractId(entity: E): ID =
    val pkIdx = self.entityMeta.columns.indexWhere(
      _.scalaName == self.entityMeta.primaryKey.scalaName
    )
    entity.asInstanceOf[Product].productElement(pkIdx).asInstanceOf[ID]

  // --- scope: the only thing needed for reads ---

  private val softDeleteScope: Scope[E] = new Scope[E]:
    def apply[C <: Selectable](
        qb: QueryBuilder[HasRoot, E, C]
    ): QueryBuilder[HasRoot, E, C] =
      qb.where(Frag(s"$sdColSql IS NULL", Seq.empty, FragWriter.empty))

  override def finalScopes: Vector[Scope[E]] =
    self.injectedScopes :+ softDeleteScope

  // --- write overrides: soft-delete instead of hard-delete ---

  override def delete(entity: E)(using DbCon): Unit =
    deleteById(extractId(entity))

  override def deleteById(id: ID)(using DbCon): Unit =
    Frag(
      s"UPDATE $tbl SET $sdColSql = CURRENT_TIMESTAMP WHERE $pkSql = ?",
      Seq(id),
      FragWriter.fromKeys(Vector(id.asInstanceOf[Any]))
    ).update.run()

  override def deleteAll(entities: Iterable[E])(using DbCon): BatchUpdateResult =
    var count = 0L
    entities.foreach { e =>
      delete(e); count += 1
    }
    BatchUpdateResult.Success(count)

  override def deleteAllById(ids: Iterable[ID])(using DbCon): BatchUpdateResult =
    var count = 0L
    ids.foreach { id =>
      deleteById(id); count += 1
    }
    BatchUpdateResult.Success(count)

  // --- new methods: force delete, restore, inspection ---

  /** Hard-delete an entity (real DELETE FROM). */
  def forceDelete(entity: E)(using DbCon): Unit =
    forceDeleteById(extractId(entity))

  /** Hard-delete by id (real DELETE FROM). */
  def forceDeleteById(id: ID)(using DbCon): Unit =
    Frag(
      s"DELETE FROM $tbl WHERE $pkSql = ?",
      Seq(id),
      FragWriter.fromKeys(Vector(id.asInstanceOf[Any]))
    ).update.run()

  /** Restore a soft-deleted entity by clearing the deleted-at column. */
  def restore(entity: E)(using DbCon): Unit =
    restoreById(extractId(entity))

  /** Restore a soft-deleted entity by id. */
  def restoreById(id: ID)(using DbCon): Unit =
    Frag(
      s"UPDATE $tbl SET $sdColSql = NULL WHERE $pkSql = ?",
      Seq(id),
      FragWriter.fromKeys(Vector(id.asInstanceOf[Any]))
    ).update.run()

  /** Check whether an entity is soft-deleted (deletedAt is not None). */
  def isTrashed(entity: E): Boolean =
    val idx = self.entityMeta.columns.indexWhere(_.scalaName == deletedAtFieldName)
    entity.asInstanceOf[Product].productElement(idx) != None

  // --- query helpers ---

  /** Query builder that includes ALL rows (no scopes at all). */
  transparent inline def withTrashed: Any = self.queryUnscoped

  /** Query builder that returns ONLY soft-deleted rows. Uses build0 + manual scope application since transparent inline macros cannot
    * resolve given instances from trait self-types.
    */
  def onlyTrashed: QueryBuilder[HasRoot, E, Columns[E]] =
    val meta = self.entityMeta
    val codec = self.entityCodec
    val cols = new Columns[E](meta.columns)
    val qb = QueryBuilder.build0[E, Columns[E]](meta, codec, cols)
    val scopes = onlyTrashedScopes
    scopes.foldLeft(qb)((q, s) => s.apply(q))

  private def onlyTrashedScopes: Vector[Scope[E]] =
    self.injectedScopes :+ new Scope[E]:
      def apply[C <: Selectable](
          qb: QueryBuilder[HasRoot, E, C]
      ): QueryBuilder[HasRoot, E, C] =
        qb.where(Frag(s"$sdColSql IS NOT NULL", Seq.empty, FragWriter.empty))
end SoftDeletes
