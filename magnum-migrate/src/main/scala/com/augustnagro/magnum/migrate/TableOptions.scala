package com.augustnagro.magnum.migrate

case class TableOptions(
    temporary: Boolean = false,
    ifNotExists: Boolean = false,
    comment: Option[String] = None,
    tablespace: Option[String] = None,
    unlogged: Boolean = false
)

object TableOptions:
  val empty: TableOptions = TableOptions()
