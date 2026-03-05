package com.augustnagro.magnum

import scala.reflect.{ClassTag, classTag}

/** Mixin trait for soft-delete behavior on a [[Repo]].
  *
  * Adds a scope that filters rows where the deleted-at column is NULL (i.e. not deleted), and uses the `rewriteDelete` mutation hook to SET
  * the timestamp instead of issuing a real DELETE.
  *
  * Usage:
  * {{{
  *   @Table(SqlNameMapper.CamelToSnakeCase)
  *   case class SdUser(@Id id: Long, name: String, @deletedAt deletedAt: Option[OffsetDateTime])
  *     derives EntityMeta, HasDeletedAt
  *
  *   val repo = new Repo[SdUser, SdUser, Long] with SoftDeletes[SdUser, SdUser, Long]
  * }}}
  *
  * The entity must have an `Option` -typed field annotated with `@deletedAt` and must derive `HasDeletedAt`.
  */
trait SoftDeletes[EC, E, ID](using hasDeletedAt: HasDeletedAt[E]):
  self: Repo[EC, E, ID] =>

  // --- internals ---

  private lazy val sdColSql: String = hasDeletedAt.column.sqlName
  private lazy val tbl: String = self.entityMeta.tableName
  private lazy val pkSql: String = self.entityMeta.primaryKey.sqlName

  private def extractId(entity: E): ID =
    self.entityMeta.extractPk(entity).asInstanceOf[ID]

  // --- scope with mutation hooks ---

  private val softDeleteScope: Scope[E] = new Scope[E]:
    override def conditions(meta: TableMeta[E]): Vector[WhereFrag] =
      Vector(WhereFrag(Frag(s"$sdColSql IS NULL", Seq.empty, FragWriter.empty)))
    override def rewriteDelete(meta: TableMeta[E]): Vector[SetClause] =
      Vector(SetClause.literal(hasDeletedAt.column, "CURRENT_TIMESTAMP"))
    override def key: ClassTag[?] = classTag[SoftDeletes[?, ?, ?]]

  override def finalScopes: Vector[Scope[E]] =
    self.injectedScopes :+ softDeleteScope

  // --- new methods: force delete, restore, inspection ---

  /** Hard-delete an entity (real DELETE FROM). */
  def forceDelete(entity: E)(using DbCon[? <: SupportsMutations]): Unit =
    forceDeleteById(extractId(entity))

  /** Hard-delete by id (real DELETE FROM). */
  def forceDeleteById(id: ID)(using DbCon[? <: SupportsMutations]): Unit =
    Frag(
      s"DELETE FROM $tbl WHERE $pkSql = ?",
      Seq(id),
      FragWriter.fromKeys(Vector(id.asInstanceOf[Any]))
    ).update.run()

  /** Restore a soft-deleted entity by clearing the deleted-at column. */
  def restore(entity: E)(using DbCon[? <: SupportsMutations]): Unit =
    restoreById(extractId(entity))

  /** Restore a soft-deleted entity by id. */
  def restoreById(id: ID)(using DbCon[? <: SupportsMutations]): Unit =
    Frag(
      s"UPDATE $tbl SET $sdColSql = NULL WHERE $pkSql = ?",
      Seq(id),
      FragWriter.fromKeys(Vector(id.asInstanceOf[Any]))
    ).update.run()

  /** Check whether an entity is soft-deleted (deletedAt is not None). */
  def isTrashed(entity: E): Boolean =
    entity.asInstanceOf[Product].productElement(hasDeletedAt.index) != None

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
    val withWheres = scopes.flatMap(_.conditions(meta)).foldLeft(qb)(_.where(_))
    scopes.flatMap(_.orderings(meta)).foldLeft(withWheres)(_.orderBy(_))

  private def onlyTrashedScopes: Vector[Scope[E]] =
    self.injectedScopes :+ new Scope[E]:
      override def conditions(meta: TableMeta[E]): Vector[WhereFrag] =
        Vector(WhereFrag(Frag(s"$sdColSql IS NOT NULL", Seq.empty, FragWriter.empty)))
end SoftDeletes
