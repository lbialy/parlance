package com.augustnagro.magnum

import java.sql.ResultSet
import javax.sql.DataSource
import scala.util.{Try, Using}

/** Repository supporting read-only queries. When entity `E` does not have an
  * id, use `Null` for the `Id` type.
  * @tparam E
  *   database entity class
  * @tparam ID
  *   id type of E
  */
open class ImmutableRepo[E, ID](
    val injectedScopes: Vector[Scope[E]] = Vector.empty[Scope[E]]
)(using
    defaults: RepoDefaults[?, E, ID],
    tableMeta: TableMeta[E],
    eCodec: DbCodec[E]
):

  /** Count of all entities */
  def count(using DbCon): Long = defaults.count

  /** Returns true if an E exists with the given id */
  def existsById(id: ID)(using DbCon): Boolean = defaults.existsById(id)

  /** Returns all entity values */
  def findAll(using DbCon): Vector[E] = defaults.findAll

  /** Find all entities matching the specification. See the scaladoc of [[Spec]]
    * for more details
    */
  def findAll(spec: Spec[E])(using DbCon): Vector[E] = defaults.findAll(spec)

  /** Returns Some(entity) if a matching E is found */
  def findById(id: ID)(using DbCon): Option[E] = defaults.findById(id)

  /** Find all entities having ids in the Iterable. If an Id is not found, no
    * error is thrown.
    */
  def findAllById(ids: Iterable[ID])(using DbCon): Vector[E] =
    defaults.findAllById(ids)

  /** All scopes that will be applied to queries created via `query`.
    * Override to add local scopes in subclasses.
    */
  def finalScopes: Vector[Scope[E]] = injectedScopes

  /** Apply all finalScopes to a QueryBuilder. */
  final def applyScopes[C <: Selectable](
      qb: QueryBuilder[HasRoot, E, C]
  ): QueryBuilder[HasRoot, E, C] =
    finalScopes.foldLeft(qb)((q, s) => s.apply(q))

  /** Create a QueryBuilder with all scopes applied. */
  transparent inline def query: Any =
    QueryBuilder.fromWithScopes[E](finalScopes)

  /** Create a QueryBuilder without any scopes applied. */
  transparent inline def queryUnscoped: Any =
    QueryBuilder.from[E]

  /** Alias for findById */
  def find(id: ID)(using DbCon): Option[E] = defaults.findById(id)

  /** Find by id or throw QueryBuilderException */
  def findOrFail(id: ID)(using DbCon): E =
    defaults.findById(id).getOrElse(
      throw QueryBuilderException(s"Entity not found for id: $id")
    )

end ImmutableRepo
