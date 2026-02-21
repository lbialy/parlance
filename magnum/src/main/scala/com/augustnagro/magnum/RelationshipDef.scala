package com.augustnagro.magnum

/** ADT for relationship metadata. Describes how two entities are related.
  *
  * Morph variants are deferred to a later phase.
  */
sealed trait RelationshipDef

object RelationshipDef:
  /** Direct FK join for hasOne / belongsTo (to-one relationships). */
  case class DirectJoin(
      fkColumn: Col[?],
      pkColumn: Col[?],
      joinType: JoinType
  ) extends RelationshipDef

  /** Eager-loaded relationship for hasMany (to-many relationships). */
  case class EagerLoad(
      fkColumn: Col[?],
      pkColumn: Col[?]
  ) extends RelationshipDef

  /** Many-to-many via a pivot table. */
  case class PivotJoin(
      pivotTable: String,
      sourceFk: Col[?],
      targetFk: Col[?]
  ) extends RelationshipDef

  /** Through relationship via an intermediate table. */
  case class ThroughJoin(
      intermediate: String,
      sourceFk: Col[?],
      intermediatePk: Col[?],
      intermediateFk: Col[?],
      targetPk: Col[?]
  ) extends RelationshipDef
end RelationshipDef
