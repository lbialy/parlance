package com.augustnagro.magnum

/** Compile-time table metadata derived from @Table-annotated case classes.
  *
  * Provides typed column list, table name, and primary key for use by the query builder.
  * Derive via `EntityMeta` which combines this with `DbCodec`.
  */
trait TableMeta[E]:
  def tableName: String
  def columns: IArray[Col[?]]
  def primaryKey: Col[?]
  def columnByName(scalaName: String): Option[Col[?]] =
    columns.find(_.scalaName == scalaName)
