package com.augustnagro.magnum.migrate

/** Per-field compile-time type information extracted by the verify macro.
  *
  * @param scalaTypeName
  *   the Scala type name (e.g. "Long", "String", "Array[Byte]")
  * @param isOption
  *   true if the field is Option[T]
  */
case class FieldMeta(scalaTypeName: String, isOption: Boolean)
