package ma.chinespirit.parlance

import scala.reflect.ClassTag

/** A reusable query scope that can be injected into repositories.
  *
  * Scopes are data producers — they return WHERE conditions and ORDER BY clauses that apply to an entity's queries. This makes scopes
  * inspectable and composable in any context (QueryBuilder, EXISTS subqueries, JOINs, etc.) without requiring a QueryBuilder instance.
  *
  * @tparam E
  *   the entity type this scope applies to
  */
trait Scope[E]:
  /** WHERE conditions this scope contributes. All conditions from all scopes are ANDed together. */
  def conditions(meta: TableMeta[E]): Vector[WhereFrag] = Vector.empty

  /** ORDER BY clauses this scope contributes. Appended after all scope conditions. */
  def orderings(meta: TableMeta[E]): Vector[OrderByFrag] = Vector.empty

  /** Extra SET clauses to include in UPDATE statements (e.g. `updated_at = CURRENT_TIMESTAMP`). */
  def onUpdate(meta: TableMeta[E]): Vector[SetClause] = Vector.empty

  /** Extra SET clauses to include in INSERT statements. */
  def onInsert(meta: TableMeta[E]): Vector[SetClause] = Vector.empty

  /** If non-empty, DELETE is rewritten to UPDATE SET <clauses> (e.g. `deleted_at = CURRENT_TIMESTAMP`). */
  def rewriteDelete(meta: TableMeta[E]): Vector[SetClause] = Vector.empty

  /** Key used to identify this scope for selective removal via `queryWithout[S]`. Defaults to a ClassTag of the runtime class. Override for
    * anonymous classes to use a well-known tag.
    */
  def key: ClassTag[?] = ClassTag(this.getClass)
end Scope
