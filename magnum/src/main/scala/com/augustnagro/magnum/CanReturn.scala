package com.augustnagro.magnum

/** Evidence that insertReturning is valid for this EC/E/D combination.
  *
  * When EC =:= E, no generated keys are needed (works on all databases).
  * When EC != E, the database must support generated keys (SupportsReturning).
  */
sealed trait CanReturn[-EC, -E, -D <: DatabaseType]

object CanReturn:
  private val instance: CanReturn[Any, Any, DatabaseType] =
    new CanReturn[Any, Any, DatabaseType] {}

  /** EC =:= E: always valid, no generated keys needed. */
  given sameType[E, D <: DatabaseType]: CanReturn[E, E, D] = instance

  /** EC != E: only valid when the database supports RETURNING / generated keys. */
  given needsGeneratedKeys[EC, E, D <: SupportsReturning]: CanReturn[EC, E, D] =
    instance
