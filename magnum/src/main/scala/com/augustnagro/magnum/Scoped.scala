package com.augustnagro.magnum

/** Typeclass providing scopes for entity E. When an `ImmutableRepo[E, _]` is available as a `given`, its `finalScopes` are used
  * automatically in relation methods (whereHas, doesntHave, has, withRelated, join). When no repo is in scope, the default empty instance
  * from the companion object is used.
  */
trait Scoped[E]:
  def scopes: Vector[Scope[E]]

object Scoped:
  given empty[E]: Scoped[E] with
    def scopes: Vector[Scope[E]] = Vector.empty
