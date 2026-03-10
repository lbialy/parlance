package ma.chinespirit.parlance

sealed trait ScopePolicy
sealed trait ApplyScopes extends ScopePolicy
sealed trait IgnoreScopes extends ScopePolicy

trait ResolveScopes[T, P <: ScopePolicy]:
  def scopes: Vector[Scope[T]]

object ResolveScopes:
  given apply[T](using s: Scoped[T]): ResolveScopes[T, ApplyScopes] with
    def scopes: Vector[Scope[T]] = s.scopes

  given ignore[T]: ResolveScopes[T, IgnoreScopes] with
    def scopes: Vector[Scope[T]] = Vector.empty
