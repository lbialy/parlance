package ma.chinespirit.parlance

/** Base trait for scope composition via the stackable trait pattern.
  *
  * `ImmutableRepo` provides the concrete base (`finalScopes = injectedScopes`). Mixin traits like `SoftDeletes` and `Timestamps` extend
  * this and use `abstract override` to chain `super.finalScopes`, allowing any number of scope mixins to compose correctly regardless of
  * linearization order.
  */
trait HasScopes[E]:
  def injectedScopes: Vector[Scope[E]]
  def finalScopes: Vector[Scope[E]] = injectedScopes
