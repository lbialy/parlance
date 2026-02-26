package com.augustnagro.magnum

import java.sql.ResultSet
import javax.sql.DataSource
import scala.reflect.ClassTag
import scala.util.{Try, Using}

/** Repository supporting read-only queries. When entity `E` does not have an id, use `Null` for the `Id` type.
  * @tparam E
  *   database entity class
  * @tparam ID
  *   id type of E
  */
open class ImmutableRepo[E, ID](
    val injectedScopes: Vector[Scope[E]] = Vector.empty[Scope[E]]
)(using
    defaults: RepoDefaults[?, E, ID],
    meta: EntityMeta[E]
):

  /** Expose table metadata for traits with self-type. */
  protected def entityMeta: TableMeta[E] = meta

  /** Expose entity codec for traits with self-type. */
  protected def entityCodec: DbCodec[E] = meta

  /** Index of the primary key column in the entity's product elements. */
  protected val pkIndex: Int =
    meta.columns.indexWhere(_.scalaName == meta.primaryKey.scalaName)

  /** Extract the primary key value from an entity. */
  protected def extractPk(entity: E): Any =
    entity.asInstanceOf[Product].productElement(pkIndex)

  private def track(entity: E)(using con: DbCon): E =
    con.trackLoaded(meta.tableName, extractPk(entity), entity)
    entity

  private def trackAll(entities: Vector[E])(using con: DbCon): Vector[E] =
    entities.foreach(e => con.trackLoaded(meta.tableName, extractPk(e), e))
    entities

  /** Internal scoped query builder using build0 (no structural typing needed). */
  private def scopedQb: QueryBuilder[HasRoot, E, Columns[E]] =
    val cols = new Columns[E](meta.columns)
    val qb = QueryBuilder.build0[E, Columns[E]](meta, meta, cols)
    applyScopes(qb)

  /** Build a WhereFrag for `pk = ?` */
  private def pkEqualsFrag(id: ID): WhereFrag =
    WhereFrag(Frag(
      s"${meta.primaryKey.sqlName} = ?",
      Seq(id),
      FragWriter.fromKeys(Vector(id.asInstanceOf[Any]))
    ))

  /** Build a WhereFrag for `pk IN (?, ?, ...)` */
  private def pkInFrag(ids: Iterable[ID]): WhereFrag =
    val keys = ids.toVector
    val placeholders = keys.map(_ => "?").mkString(", ")
    WhereFrag(Frag(
      s"${meta.primaryKey.sqlName} IN ($placeholders)",
      keys,
      FragWriter.fromKeys(keys.asInstanceOf[Vector[Any]])
    ))

  /** Count of all entities */
  def count(using DbCon): Long =
    if finalScopes.isEmpty then defaults.count
    else scopedQb.count()

  /** Returns true if an E exists with the given id */
  def existsById(id: ID)(using DbCon): Boolean =
    if finalScopes.isEmpty then defaults.existsById(id)
    else scopedQb.where(pkEqualsFrag(id)).exists()

  /** Returns all entity values */
  def findAll(using con: DbCon): Vector[E] =
    val results =
      if finalScopes.isEmpty then defaults.findAll
      else scopedQb.run()
    trackAll(results)

  /** Find all entities matching the specification. See the scaladoc of [[Spec]] for more details
    */
  def findAll(spec: Spec[E])(using con: DbCon): Vector[E] =
    trackAll(defaults.findAll(spec))

  /** Returns Some(entity) if a matching E is found */
  def findById(id: ID)(using con: DbCon): Option[E] =
    val result =
      if finalScopes.isEmpty then defaults.findById(id)
      else scopedQb.where(pkEqualsFrag(id)).first()
    result.map(track)

  /** Find all entities having ids in the Iterable. If an Id is not found, no error is thrown.
    */
  def findAllById(ids: Iterable[ID])(using con: DbCon): Vector[E] =
    val results =
      if finalScopes.isEmpty then defaults.findAllById(ids)
      else if ids.isEmpty then Vector.empty
      else scopedQb.where(pkInFrag(ids)).run()
    trackAll(results)

  /** All scopes that will be applied to queries created via `query`. Override to add local scopes in subclasses.
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

  /** Create a QueryBuilder with a specific scope type removed. Other scopes are preserved. */
  transparent inline def queryWithout[S: ClassTag]: Any =
    QueryBuilder.fromWithScopes[E](
      finalScopes.filterNot(_.key.runtimeClass == implicitly[ClassTag[S]].runtimeClass)
    )

  /** Alias for findById */
  def find(id: ID)(using DbCon): Option[E] = findById(id)

  /** Find by id or throw QueryBuilderException */
  def findOrFail(id: ID)(using DbCon): E =
    findById(id).getOrElse(
      throw QueryBuilderException(s"Entity not found for id: $id")
    )

end ImmutableRepo
