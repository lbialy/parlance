package com.augustnagro.magnum.migrate

import com.augustnagro.magnum.DbCodec

case class ColumnDef[T](
    name: String,
    columnType: ColumnType,
    modifiers: ColumnModifiers = ColumnModifiers()
):
  def nullable: ColumnDef[T] =
    copy(modifiers = modifiers.copy(nullable = true))

  def default(value: T)(using codec: DbCodec[T]): ColumnDef[T] =
    copy(modifiers = modifiers.copy(default = Some(DefaultValue.Literal(value))))

  def defaultExpression(sql: String): ColumnDef[T] =
    copy(modifiers = modifiers.copy(default = Some(DefaultValue.Expression(sql))))

  def primaryKey: ColumnDef[T] =
    copy(modifiers = modifiers.copy(primaryKey = true))

  def autoIncrement: ColumnDef[T] =
    copy(modifiers = modifiers.copy(autoIncrement = true))

  def unique: ColumnDef[T] =
    copy(modifiers = modifiers.copy(unique = true))

  def check(expression: String): ColumnDef[T] =
    copy(modifiers = modifiers.copy(check = Some(expression)))

  def comment(text: String): ColumnDef[T] =
    copy(modifiers = modifiers.copy(comment = Some(text)))

  def collation(name: String): ColumnDef[T] =
    copy(modifiers = modifiers.copy(collation = Some(name)))

  def generatedAlwaysAs(expression: String): ColumnDef[T] =
    copy(modifiers = modifiers.copy(generatedAs = Some(expression)))

  def references(table: String, col: String): ColumnDef[T] =
    copy(modifiers = modifiers.copy(references = Some(InlineReference(table, col))))

  def references(
      table: String,
      col: String,
      onDelete: FkAction,
      onUpdate: FkAction
  ): ColumnDef[T] =
    copy(modifiers = modifiers.copy(references = Some(InlineReference(table, col, onDelete, onUpdate))))

  // Type override methods
  def varchar(length: Int): ColumnDef[T] =
    copy(columnType = ColumnType.Varchar(length))
  def char(length: Int): ColumnDef[T] =
    copy(columnType = ColumnType.Char(length))
  def decimal(precision: Int, scale: Int): ColumnDef[T] =
    copy(columnType = ColumnType.Numeric(precision, scale))
  def timestamp(precision: Int): ColumnDef[T] =
    copy(columnType = ColumnType.Timestamp(precision))
  def timestampTz(precision: Int): ColumnDef[T] =
    copy(columnType = ColumnType.TimestampTz(precision))
  def time(precision: Int): ColumnDef[T] =
    copy(columnType = ColumnType.Time(precision))
  def timeTz(precision: Int): ColumnDef[T] =
    copy(columnType = ColumnType.TimeTz(precision))
  def customType(sqlTypeName: String): ColumnDef[T] =
    copy(columnType = ColumnType.Custom(sqlTypeName))
end ColumnDef
