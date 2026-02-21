package com.augustnagro.magnum

class PredicateGroupBuilder[C] private[magnum] (
    private val cols: C,
    private val predicates: Vector[Predicate],
    private val mode: PredicateGroupBuilder.Mode
):
  def and(frag: Frag): PredicateGroupBuilder[C] =
    new PredicateGroupBuilder(
      cols,
      predicates :+ Predicate.Leaf(frag),
      PredicateGroupBuilder.Mode.And
    )

  def and(f: C => Frag): PredicateGroupBuilder[C] =
    new PredicateGroupBuilder(
      cols,
      predicates :+ Predicate.Leaf(f(cols)),
      PredicateGroupBuilder.Mode.And
    )

  def or(frag: Frag): PredicateGroupBuilder[C] =
    new PredicateGroupBuilder(
      cols,
      predicates :+ Predicate.Leaf(frag),
      PredicateGroupBuilder.Mode.Or
    )

  def or(f: C => Frag): PredicateGroupBuilder[C] =
    new PredicateGroupBuilder(
      cols,
      predicates :+ Predicate.Leaf(f(cols)),
      PredicateGroupBuilder.Mode.Or
    )

  private[magnum] def build: Predicate = mode match
    case PredicateGroupBuilder.Mode.And => Predicate.And(predicates)
    case PredicateGroupBuilder.Mode.Or  => Predicate.Or(predicates)
end PredicateGroupBuilder

object PredicateGroupBuilder:
  private[magnum] enum Mode:
    case And, Or

  private[magnum] def empty[C](cols: C): PredicateGroupBuilder[C] =
    new PredicateGroupBuilder(cols, Vector.empty, Mode.And)
