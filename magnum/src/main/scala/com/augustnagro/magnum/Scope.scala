package com.augustnagro.magnum

/** A reusable query scope that can be injected into repositories.
  *
  * Scopes modify QueryBuilder instances by adding WHERE clauses, ordering, etc. They compose with other scopes via `finalScopes` on repos.
  *
  * Since the column type `C` is erased at the scope level, use `qb.where(sql"...".unsafeAsWhere)`.
  *
  * @tparam E
  *   the entity type this scope applies to
  */
trait Scope[E]:
  def apply[C <: Selectable](qb: QueryBuilder[HasRoot, E, C]): QueryBuilder[HasRoot, E, C]
