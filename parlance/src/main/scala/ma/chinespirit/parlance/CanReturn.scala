package ma.chinespirit.parlance

/** Evidence that insertReturning is valid for this EC/E/D combination.
  *
  * When EC =:= E, no generated keys are needed (works on all databases). When EC != E, the database must support generated keys
  * (SupportsReturning).
  *
  * EC and E are invariant to prevent the compiler from unifying via Nothing. D is contravariant so that givens work with erased/existential
  * database types.
  */
sealed trait CanReturn[EC, E, -D <: DatabaseType]

private[parlance] trait LowPriorityCanReturn:
  /** EC != E: only valid when the database supports RETURNING / generated keys. */
  given needsGeneratedKeys[EC, E, D <: SupportsReturning]: CanReturn[EC, E, D] =
    CanReturn.unsafeInstance

object CanReturn extends LowPriorityCanReturn:
  private[parlance] def unsafeInstance[EC, E, D <: DatabaseType]: CanReturn[EC, E, D] =
    _instance.asInstanceOf[CanReturn[EC, E, D]]

  private val _instance: CanReturn[Any, Any, DatabaseType] =
    new CanReturn[Any, Any, DatabaseType] {}

  /** EC =:= E: always valid, no generated keys needed. */
  given sameType[E, D <: DatabaseType]: CanReturn[E, E, D] = unsafeInstance
