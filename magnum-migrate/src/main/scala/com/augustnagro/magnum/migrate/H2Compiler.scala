package com.augustnagro.magnum.migrate

import java.util.UUID

/** H2-dialect compiler. Pragmatic — supports common cases for testing, not full fidelity with PostgreSQL.
  */
object H2Compiler extends MigrationCompiler:

  def compile(migration: Migration): List[String] =
    migration match
      case ct: Migration.CreateTable          => compileCreateTable(ct)
      case Migration.DropTable(name)          => List(s"DROP TABLE $name")
      case Migration.DropTableIfExists(n)     => List(s"DROP TABLE IF EXISTS $n")
      case Migration.RenameTable(f, t)        => List(s"ALTER TABLE $f RENAME TO $t")
      case at: Migration.AlterTable           => compileAlterTable(at)
      case _: Migration.CreateEnumType        => Nil // H2 doesn't have enum types
      case _: Migration.DropEnumType          => Nil
      case _: Migration.AddEnumValue          => Nil
      case _: Migration.RenameEnumValue       => Nil
      case _: Migration.CreateExtension       => Nil // H2 doesn't have extensions
      case _: Migration.DropExtension         => Nil
      case Migration.Raw(sql)                 => List(sql)
      case Migration.RawParameterized(sql, _) => List(sql)

  def compileType(ct: ColumnType): String = ct match
    case ColumnType.SmallInt        => "SMALLINT"
    case ColumnType.Integer         => "INTEGER"
    case ColumnType.BigInt          => "BIGINT"
    case ColumnType.SmallSerial     => "SMALLINT AUTO_INCREMENT"
    case ColumnType.Serial          => "INTEGER AUTO_INCREMENT"
    case ColumnType.BigSerial       => "BIGINT AUTO_INCREMENT"
    case ColumnType.Numeric(p, s)   => s"NUMERIC($p, $s)"
    case ColumnType.DoublePrecision => "DOUBLE PRECISION"
    case ColumnType.Real            => "REAL"
    case ColumnType.Boolean         => "BOOLEAN"
    case ColumnType.Char(n)         => s"CHAR($n)"
    case ColumnType.Varchar(n)      => s"VARCHAR($n)"
    case ColumnType.Text            => "VARCHAR"
    case ColumnType.Bytea           => "BINARY"
    case ColumnType.Date            => "DATE"
    case ColumnType.Time(p)         => s"TIME($p)"
    case ColumnType.TimeTz(p)       => s"TIME($p) WITH TIME ZONE"
    case ColumnType.Timestamp(p)    => s"TIMESTAMP($p)"
    case ColumnType.TimestampTz(p)  => s"TIMESTAMP($p) WITH TIME ZONE"
    case ColumnType.Interval        => "INTERVAL"
    case ColumnType.Json            => "JSON"
    case ColumnType.Jsonb           => "JSON"
    case ColumnType.Uuid            => "UUID"
    case ColumnType.PgEnum(_)       => "VARCHAR(255)"
    case ColumnType.ArrayOf(el)     => s"${compileType(el)} ARRAY"
    case ColumnType.Inet            => "VARCHAR(45)"
    case ColumnType.Cidr            => "VARCHAR(45)"
    case ColumnType.MacAddr         => "VARCHAR(17)"
    case ColumnType.Money           => "NUMERIC(19, 2)"
    case ColumnType.Custom(sql)     => sql

  def compileColumnDef(col: ColumnDef[?]): String =
    val sb = StringBuilder()
    val mods = col.modifiers
    val colType =
      if mods.autoIncrement then autoIncrementType(col.columnType)
      else col.columnType
    sb.append(col.name)
    sb.append(" ")
    sb.append(compileType(colType))
    if !mods.nullable then sb.append(" NOT NULL")
    mods.default.foreach:
      case DefaultValue.Literal(v)    => sb.append(s" DEFAULT ${renderLiteral(v)}")
      case DefaultValue.Expression(e) => sb.append(s" DEFAULT $e")
    if mods.primaryKey then sb.append(" PRIMARY KEY")
    if mods.unique then sb.append(" UNIQUE")
    mods.check.foreach(expr => sb.append(s" CHECK ($expr)"))
    mods.collation.foreach(c => sb.append(s""" COLLATE "$c""""))
    mods.generatedAs.foreach(expr => sb.append(s" GENERATED ALWAYS AS ($expr) STORED"))
    mods.references.foreach: ref =>
      sb.append(s" REFERENCES ${ref.table}(${ref.column})")
      if ref.onDelete != FkAction.NoAction then sb.append(s" ON DELETE ${compileFkAction(ref.onDelete)}")
      if ref.onUpdate != FkAction.NoAction then sb.append(s" ON UPDATE ${compileFkAction(ref.onUpdate)}")
    sb.toString
  end compileColumnDef

  private def compileCreateTable(ct: Migration.CreateTable): List[String] =
    val sb = StringBuilder()
    sb.append("CREATE")
    if ct.options.temporary then sb.append(" TEMPORARY")
    // H2 doesn't support UNLOGGED, skip
    sb.append(" TABLE")
    if ct.options.ifNotExists then sb.append(" IF NOT EXISTS")
    sb.append(s" ${ct.name} (\n")
    val colDefs = ct.columns.map(c => s"  ${compileColumnDef(c)}")
    sb.append(colDefs.mkString(",\n"))
    sb.append("\n)")
    val stmts = List.newBuilder[String]
    stmts += sb.toString
    // H2 supports COMMENT ON TABLE/COLUMN
    ct.options.comment.foreach: c =>
      stmts += s"COMMENT ON TABLE ${ct.name} IS '${escapeStr(c)}'"
    ct.columns.foreach: col =>
      col.modifiers.comment.foreach: c =>
        stmts += s"COMMENT ON COLUMN ${ct.name}.${col.name} IS '${escapeStr(c)}'"
    stmts.result()
  end compileCreateTable

  private def compileAlterTable(at: Migration.AlterTable): List[String] =
    val stmts = List.newBuilder[String]

    // H2 doesn't support batching ADD COLUMN, emit individually
    at.ops.foreach:
      case AlterOp.AddColumn(col) =>
        stmts += s"ALTER TABLE ${at.name} ADD COLUMN ${compileColumnDef(col)}"
        col.modifiers.comment.foreach: c =>
          stmts += s"COMMENT ON COLUMN ${at.name}.${col.name} IS '${escapeStr(c)}'"
      case op => stmts ++= compileAlterOp(at.name, op)

    stmts.result()

  private def compileAlterOp(
      table: String,
      op: AlterOp
  ): List[String] = op match
    case AlterOp.AddColumn(_) =>
      Nil
    case AlterOp.DropColumn(name) =>
      List(s"ALTER TABLE $table DROP COLUMN $name")
    case AlterOp.DropColumnIfExists(name) =>
      List(s"ALTER TABLE $table DROP COLUMN IF EXISTS $name")
    case AlterOp.RenameColumn(from, to) =>
      List(s"ALTER TABLE $table ALTER COLUMN $from RENAME TO $to")
    case AlterOp.AlterColumnType(name, newType, _) =>
      // H2 doesn't support USING clause
      List(
        s"ALTER TABLE $table ALTER COLUMN $name SET DATA TYPE ${compileType(newType)}"
      )
    case AlterOp.SetColumnDefault(name, expr) =>
      List(s"ALTER TABLE $table ALTER COLUMN $name SET DEFAULT $expr")
    case AlterOp.DropColumnDefault(name) =>
      List(s"ALTER TABLE $table ALTER COLUMN $name SET DEFAULT NULL")
    case AlterOp.SetNotNull(name) =>
      List(s"ALTER TABLE $table ALTER COLUMN $name SET NOT NULL")
    case AlterOp.DropNotNull(name) =>
      List(s"ALTER TABLE $table ALTER COLUMN $name SET NULL")
    case idx: AlterOp.AddIndex =>
      val idxName = idx.name.getOrElse(
        autoName(table, idx.columns, if idx.unique then "unique" else "idx")
      )
      val sb = StringBuilder()
      sb.append("CREATE")
      if idx.unique then sb.append(" UNIQUE")
      sb.append(s" INDEX $idxName ON $table")
      // H2 doesn't support USING clause or CONCURRENTLY
      sb.append(s" (${idx.columns.mkString(", ")})")
      // H2 doesn't support partial indexes (WHERE)
      List(sb.toString)
    case AlterOp.DropIndex(name) =>
      List(s"DROP INDEX $name")
    case AlterOp.DropIndexIfExists(name) =>
      List(s"DROP INDEX IF EXISTS $name")
    case AlterOp.DropIndexConcurrently(name) =>
      // H2 doesn't support CONCURRENTLY, just drop
      List(s"DROP INDEX $name")
    case AlterOp.RenameIndex(from, to) =>
      List(s"ALTER INDEX $from RENAME TO $to")
    case AlterOp.AddPrimaryKey(cols, name) =>
      val cName = name.getOrElse(s"${table}_pkey")
      List(
        s"ALTER TABLE $table ADD CONSTRAINT $cName PRIMARY KEY (${cols.mkString(", ")})"
      )
    case AlterOp.DropConstraint(name) =>
      List(s"ALTER TABLE $table DROP CONSTRAINT $name")
    case AlterOp.AddUniqueConstraint(cols, name) =>
      val cName = name.getOrElse(autoName(table, cols, "unique"))
      List(
        s"ALTER TABLE $table ADD CONSTRAINT $cName UNIQUE (${cols.mkString(", ")})"
      )
    case AlterOp.AddCheckConstraint(name, expr) =>
      List(s"ALTER TABLE $table ADD CONSTRAINT $name CHECK ($expr)")
    case fk: AlterOp.AddForeignKey =>
      val fkName = fk.name.getOrElse(autoName(table, fk.columns, "fkey"))
      val sb = StringBuilder()
      sb.append(
        s"ALTER TABLE $table ADD CONSTRAINT $fkName FOREIGN KEY (${fk.columns.mkString(", ")})"
      )
      sb.append(
        s" REFERENCES ${fk.refTable}(${fk.refColumns.mkString(", ")})"
      )
      if fk.onDelete != FkAction.NoAction then sb.append(s" ON DELETE ${compileFkAction(fk.onDelete)}")
      if fk.onUpdate != FkAction.NoAction then sb.append(s" ON UPDATE ${compileFkAction(fk.onUpdate)}")
      List(sb.toString)
    case AlterOp.DropForeignKey(name) =>
      List(s"ALTER TABLE $table DROP CONSTRAINT $name")
    case AlterOp.SetTableComment(comment) =>
      List(s"COMMENT ON TABLE $table IS '${escapeStr(comment)}'")
    case AlterOp.SetColumnComment(col, comment) =>
      List(s"COMMENT ON COLUMN $table.$col IS '${escapeStr(comment)}'")

  private def compileFkAction(action: FkAction): String = action match
    case FkAction.NoAction   => "NO ACTION"
    case FkAction.Restrict   => "RESTRICT"
    case FkAction.Cascade    => "CASCADE"
    case FkAction.SetNull    => "SET NULL"
    case FkAction.SetDefault => "SET DEFAULT"

  private def autoIncrementType(ct: ColumnType): ColumnType = ct match
    case ColumnType.BigInt   => ColumnType.BigSerial
    case ColumnType.Integer  => ColumnType.Serial
    case ColumnType.SmallInt => ColumnType.SmallSerial
    case other               => other

  private def autoName(
      table: String,
      columns: List[String],
      suffix: String
  ): String =
    s"${table}_${columns.mkString("_")}_$suffix"

  private def renderLiteral(value: Any): String =
    PostgresCompiler.renderLiteral(value)

  private def escapeStr(s: String): String =
    s.replace("'", "''")
end H2Compiler
