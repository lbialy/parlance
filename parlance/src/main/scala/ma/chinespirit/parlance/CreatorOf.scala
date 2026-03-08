package ma.chinespirit.parlance

/** Marker trait linking a creator type to its entity type. Extending this trait puts the entity's companion object into the creator's
  * implicit search scope, allowing entity extension methods like `creator.create()` to find the repo automatically without explicit
  * imports.
  *
  * {{{
  * case class CategoryCreator(name: String) extends CreatorOf[Category] derives DbCodec
  *
  * // Now this works without importing Category.repo:
  * val category = CategoryCreator("Books").create[Postgres]()
  * }}}
  */
trait CreatorOf[E]
