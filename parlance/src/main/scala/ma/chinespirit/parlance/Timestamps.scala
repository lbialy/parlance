package ma.chinespirit.parlance

import scala.reflect.{ClassTag, classTag}

/** Mixin trait for automatic timestamp management on a [[Repo]].
  *
  * Automatically sets `created_at = CURRENT_TIMESTAMP` on INSERT and `updated_at = CURRENT_TIMESTAMP` on both INSERT and UPDATE.
  *
  * Usage:
  * {{{
  *   @Table(SqlNameMapper.CamelToSnakeCase)
  *   case class Post(@Id id: Long, title: String, @createdAt createdAt: OffsetDateTime, @updatedAt updatedAt: OffsetDateTime)
  *     derives EntityMeta, HasCreatedAt, HasUpdatedAt
  *
  *   val repo = new Repo[PostCreator, Post, Long] with Timestamps[PostCreator, Post, Long]
  * }}}
  *
  * The entity must have fields annotated with `@createdAt` and `@updatedAt` and must derive `HasCreatedAt` and `HasUpdatedAt`.
  */
trait Timestamps[EC, E, ID](using
    hasCreatedAt: HasCreatedAt[E],
    hasUpdatedAt: HasUpdatedAt[E]
) extends HasScopes[E]:
  self: Repo[EC, E, ID] =>

  private val timestampScope: Scope[E] = new Scope[E]:
    override def onUpdate(meta: TableMeta[E]): Vector[SetClause] =
      Vector(SetClause.literal(hasUpdatedAt.column, "CURRENT_TIMESTAMP"))
    override def onInsert(meta: TableMeta[E]): Vector[SetClause] =
      Vector(
        SetClause.literal(hasCreatedAt.column, "CURRENT_TIMESTAMP"),
        SetClause.literal(hasUpdatedAt.column, "CURRENT_TIMESTAMP")
      )
    override def key: ClassTag[?] = classTag[Timestamps[?, ?, ?]]

  abstract override def finalScopes: Vector[Scope[E]] =
    super.finalScopes :+ timestampScope
