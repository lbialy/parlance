package ma.chinespirit.parlance

/** Typeclass providing scopes for entity E. When an `ImmutableRepo[E, _]` is available as a `given`, its `finalScopes` are used
  * automatically in relation methods (whereHas, doesntHave, has, withRelated, join). When no repo is in scope, the default empty instance
  * is used as a fallback.
  */
trait Scoped[E]:
  def scopes: Vector[Scope[E]]

object Scoped:
  /** When a repo is in given scope, use its scopes. More constrained → higher priority. */
  given fromRepo[E, ID](using repo: ImmutableRepo[E, ID]): Scoped[E] with
    def scopes: Vector[Scope[E]] = repo.finalScopes

  /** Fallback when no repo is available. Less constrained → lower priority. */
  given empty[E]: Scoped[E] with
    def scopes: Vector[Scope[E]] = Vector.empty
