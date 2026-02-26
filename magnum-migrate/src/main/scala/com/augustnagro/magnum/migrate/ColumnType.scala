package com.augustnagro.magnum.migrate

enum ColumnType:
  // Integer
  case SmallInt, Integer, BigInt
  case SmallSerial, Serial, BigSerial

  // Decimal / Float
  case Numeric(precision: Int, scale: Int)
  case DoublePrecision, Real

  // Boolean
  case Boolean

  // String / Text
  case Char(length: Int)
  case Varchar(length: Int)
  case Text

  // Binary
  case Bytea

  // Date / Time
  case Date
  case Time(precision: Int = 6)
  case TimeTz(precision: Int = 6)
  case Timestamp(precision: Int = 6)
  case TimestampTz(precision: Int = 6)
  case Interval

  // JSON
  case Json, Jsonb

  // UUID
  case Uuid

  // Enum (references a CREATE TYPE ... AS ENUM)
  case PgEnum(typeName: String)

  // Array
  case ArrayOf(elementType: ColumnType)

  // Network
  case Inet, Cidr, MacAddr

  // Money
  case Money

  // Escape hatch
  case Custom(sqlTypeName: String)
