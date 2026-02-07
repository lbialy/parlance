package com.augustnagro.magnum

class PredicateGroupBuilder private (
    private val predicates: Vector[Predicate],
    private val mode: PredicateGroupBuilder.Mode
):
  def and(frag: Frag): PredicateGroupBuilder =
    new PredicateGroupBuilder(
      predicates :+ Predicate.Leaf(frag),
      PredicateGroupBuilder.Mode.And
    )

  def or(frag: Frag): PredicateGroupBuilder =
    new PredicateGroupBuilder(
      predicates :+ Predicate.Leaf(frag),
      PredicateGroupBuilder.Mode.Or
    )

  private[magnum] def build: Predicate = mode match
    case PredicateGroupBuilder.Mode.And => Predicate.And(predicates)
    case PredicateGroupBuilder.Mode.Or  => Predicate.Or(predicates)

object PredicateGroupBuilder:
  private[magnum] enum Mode:
    case And, Or

  val empty: PredicateGroupBuilder =
    new PredicateGroupBuilder(Vector.empty, Mode.And)
