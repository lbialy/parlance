package com.augustnagro.magnum.migrate

import com.augustnagro.magnum.DbCodec
import java.sql.Types

private def inferColumnType(jdbcTypes: IArray[Int]): ColumnType =
  require(
    jdbcTypes.length == 1,
    "Only single-column types supported in migrations"
  )
  jdbcTypes(0) match
    case Types.BOOLEAN                 => ColumnType.Boolean
    case Types.TINYINT | Types.SMALLINT => ColumnType.SmallInt
    case Types.INTEGER                 => ColumnType.Integer
    case Types.BIGINT                  => ColumnType.BigInt
    case Types.REAL                    => ColumnType.Real
    case Types.DOUBLE                  => ColumnType.DoublePrecision
    case Types.NUMERIC | Types.DECIMAL => ColumnType.Numeric(18, 2)
    case Types.VARCHAR | Types.LONGVARCHAR | Types.NVARCHAR |
        Types.CLOB                     => ColumnType.Text
    case Types.CHAR                    => ColumnType.Text
    case Types.BINARY | Types.VARBINARY | Types.LONGVARBINARY |
        Types.BLOB                     => ColumnType.Bytea
    case Types.DATE                    => ColumnType.Date
    case Types.TIME                    => ColumnType.Time()
    case Types.TIMESTAMP               => ColumnType.Timestamp()
    case Types.TIMESTAMP_WITH_TIMEZONE => ColumnType.TimestampTz()
    case Types.OTHER                   => ColumnType.Uuid
    case _                             => ColumnType.Text

// ---- Column builder ----

def column[T](name: String)(using codec: DbCodec[T]): ColumnDef[T] =
  ColumnDef[T](name, inferColumnType(codec.cols))

// ---- Table builders ----

def createTable(name: String)(columns: ColumnDef[?]*): Migration =
  Migration.CreateTable(name, columns.toList)

def createTable(name: String, options: TableOptions)(
    columns: ColumnDef[?]*
): Migration =
  Migration.CreateTable(name, columns.toList, options)

def dropTable(name: String): Migration =
  Migration.DropTable(name)

def dropTableIfExists(name: String): Migration =
  Migration.DropTableIfExists(name)

def renameTable(from: String, to: String): Migration =
  Migration.RenameTable(from, to)

// ---- Alter table ----

def alterTable(name: String)(ops: AlterOp*): Migration =
  Migration.AlterTable(name, ops.toList)

// ---- Alter op helpers ----

def addColumn[T](name: String)(using codec: DbCodec[T]): AlterOp.AddColumn =
  AlterOp.AddColumn(column[T](name))

def dropColumn(name: String): AlterOp.DropColumn =
  AlterOp.DropColumn(name)

def dropColumnIfExists(name: String): AlterOp.DropColumnIfExists =
  AlterOp.DropColumnIfExists(name)

def renameColumn(from: String, to: String): AlterOp.RenameColumn =
  AlterOp.RenameColumn(from, to)

def addIndex(columns: String*): AlterOp.AddIndex =
  AlterOp.AddIndex(columns.toList)

def addUniqueIndex(columns: String*): AlterOp.AddIndex =
  AlterOp.AddIndex(columns.toList, unique = true)

def dropIndex(name: String): AlterOp.DropIndex =
  AlterOp.DropIndex(name)

def addForeignKey(
    column: String,
    refTable: String,
    refColumn: String,
    onDelete: FkAction = FkAction.NoAction,
    onUpdate: FkAction = FkAction.NoAction
): AlterOp.AddForeignKey =
  AlterOp.AddForeignKey(
    List(column),
    refTable,
    List(refColumn),
    onDelete,
    onUpdate
  )

def dropForeignKey(name: String): AlterOp.DropForeignKey =
  AlterOp.DropForeignKey(name)

// ---- Enum types ----

def createEnumType(name: String, values: String*): Migration =
  Migration.CreateEnumType(name, values.toList)

def dropEnumType(name: String): Migration =
  Migration.DropEnumType(name)

def addEnumValue(typeName: String, value: String): Migration =
  Migration.AddEnumValue(typeName, value)

// ---- Extensions ----

def createExtension(name: String): Migration =
  Migration.CreateExtension(name)

// ---- Raw SQL ----

def raw(sql: String): Migration =
  Migration.Raw(sql)

// ---- Convenience helpers ----

def id(): ColumnDef[Long] =
  column[Long]("id").primaryKey.autoIncrement

def id(name: String): ColumnDef[Long] =
  column[Long](name).primaryKey.autoIncrement

def timestamps(): List[ColumnDef[?]] = List(
  column[java.time.Instant]("created_at"),
  column[java.time.Instant]("updated_at")
)

def softDelete(): ColumnDef[Option[java.time.Instant]] =
  column[Option[java.time.Instant]]("deleted_at").nullable
