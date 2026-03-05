package com.augustnagro.magnum

/** Compile-time table metadata derived from @Table-annotated case classes.
  *
  * Provides typed column list, table name, and primary key for use by the query builder. Derive via `EntityMeta` which combines this with
  * `DbCodec`.
  */
trait TableMeta[E]:
  def tableName: String
  def columns: IArray[Col[?]]
  def primaryKey: Col[?]
  def primaryKeys: IArray[Col[?]]
  def elementCodecs: IArray[DbCodec[?]]
  def isCompositeKey: Boolean = primaryKeys.length > 1
  def columnByName(scalaName: String): Option[Col[?]] =
    columns.find(_.scalaName == scalaName)

  /** Index of the primary key column in `columns`. */
  def pkIndex: Int =
    columns.indexWhere(_.scalaName == primaryKey.scalaName)

  /** Extract the primary key value from an entity instance. */
  def extractPk(entity: E): Any =
    entity.asInstanceOf[Product].productElement(pkIndex)

  /** Index of a column by its Scala name, or -1 if not found. */
  def columnIndex(scalaName: String): Int =
    columns.indexWhere(_.scalaName == scalaName)
