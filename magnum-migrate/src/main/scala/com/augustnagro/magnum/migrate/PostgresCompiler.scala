package com.augustnagro.magnum.migrate

import java.util.UUID

object PostgresCompiler extends MigrationCompiler:

  def compile(migration: Migration): List[String] =
    migration match
      case ct: Migration.CreateTable       => compileCreateTable(ct)
      case Migration.DropTable(name)       => List(s"DROP TABLE $name")
      case Migration.DropTableIfExists(n)  => List(s"DROP TABLE IF EXISTS $n")
      case Migration.RenameTable(f, t)     => List(s"ALTER TABLE $f RENAME TO $t")
      case at: Migration.AlterTable        => compileAlterTable(at)
      case ct: Migration.CreateEnumType    => List(compileCreateEnum(ct))
      case Migration.DropEnumType(n)       => List(s"DROP TYPE $n")
      case av: Migration.AddEnumValue      => List(compileAddEnumValue(av))
      case rv: Migration.RenameEnumValue =>
        List(
          s"ALTER TYPE ${rv.typeName} RENAME VALUE '${escapeStr(rv.from)}' TO '${escapeStr(rv.to)}'"
        )
      case Migration.CreateExtension(n) =>
        List(s"CREATE EXTENSION IF NOT EXISTS $n")
      case Migration.DropExtension(n) =>
        List(s"DROP EXTENSION IF EXISTS $n")
      case Migration.Raw(sql)              => List(sql)
      case Migration.RawParameterized(sql, _) => List(sql)

  override def requiresNonTx(migration: Migration): Boolean =
    migration match
      case Migration.AlterTable(_, ops) =>
        ops.exists:
          case AlterOp.AddIndex(_, _, _, _, _, true) => true
          case _: AlterOp.DropIndexConcurrently      => true
          case _                                     => false
      case _ => false

  def compileType(ct: ColumnType): String = ct match
    case ColumnType.SmallInt       => "SMALLINT"
    case ColumnType.Integer        => "INTEGER"
    case ColumnType.BigInt         => "BIGINT"
    case ColumnType.SmallSerial    => "SMALLSERIAL"
    case ColumnType.Serial         => "SERIAL"
    case ColumnType.BigSerial      => "BIGSERIAL"
    case ColumnType.Numeric(p, s)  => s"NUMERIC($p, $s)"
    case ColumnType.DoublePrecision => "DOUBLE PRECISION"
    case ColumnType.Real           => "REAL"
    case ColumnType.Boolean        => "BOOLEAN"
    case ColumnType.Char(n)        => s"CHAR($n)"
    case ColumnType.Varchar(n)     => s"VARCHAR($n)"
    case ColumnType.Text           => "TEXT"
    case ColumnType.Bytea          => "BYTEA"
    case ColumnType.Date           => "DATE"
    case ColumnType.Time(p)        => s"TIME($p)"
    case ColumnType.TimeTz(p)      => s"TIMETZ($p)"
    case ColumnType.Timestamp(p)   => s"TIMESTAMP($p)"
    case ColumnType.TimestampTz(p) => s"TIMESTAMPTZ($p)"
    case ColumnType.Interval       => "INTERVAL"
    case ColumnType.Json           => "JSON"
    case ColumnType.Jsonb          => "JSONB"
    case ColumnType.Uuid           => "UUID"
    case ColumnType.PgEnum(name)   => name
    case ColumnType.ArrayOf(el)    => s"${compileType(el)}[]"
    case ColumnType.Inet           => "INET"
    case ColumnType.Cidr           => "CIDR"
    case ColumnType.MacAddr        => "MACADDR"
    case ColumnType.Money          => "MONEY"
    case ColumnType.Custom(sql)    => sql

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
      case DefaultValue.Literal(v)   => sb.append(s" DEFAULT ${renderLiteral(v)}")
      case DefaultValue.Expression(e) => sb.append(s" DEFAULT $e")
    if mods.primaryKey then sb.append(" PRIMARY KEY")
    if mods.unique then sb.append(" UNIQUE")
    mods.check.foreach(expr => sb.append(s" CHECK ($expr)"))
    mods.collation.foreach(c => sb.append(s""" COLLATE "$c""""))
    mods.generatedAs.foreach(expr =>
      sb.append(s" GENERATED ALWAYS AS ($expr) STORED")
    )
    mods.references.foreach: ref =>
      sb.append(s" REFERENCES ${ref.table}(${ref.column})")
      if ref.onDelete != FkAction.NoAction then
        sb.append(s" ON DELETE ${compileFkAction(ref.onDelete)}")
      if ref.onUpdate != FkAction.NoAction then
        sb.append(s" ON UPDATE ${compileFkAction(ref.onUpdate)}")
    sb.toString

  private def compileCreateTable(ct: Migration.CreateTable): List[String] =
    val sb = StringBuilder()
    sb.append("CREATE")
    if ct.options.temporary then sb.append(" TEMPORARY")
    if ct.options.unlogged then sb.append(" UNLOGGED")
    sb.append(" TABLE")
    if ct.options.ifNotExists then sb.append(" IF NOT EXISTS")
    sb.append(s" ${ct.name} (\n")
    val colDefs = ct.columns.map(c => s"  ${compileColumnDef(c)}")
    sb.append(colDefs.mkString(",\n"))
    sb.append("\n)")
    ct.options.tablespace.foreach(ts => sb.append(s" TABLESPACE $ts"))
    val stmts = List.newBuilder[String]
    stmts += sb.toString
    ct.options.comment.foreach: c =>
      stmts += s"COMMENT ON TABLE ${ct.name} IS '${escapeStr(c)}'"
    ct.columns.foreach: col =>
      col.modifiers.comment.foreach: c =>
        stmts += s"COMMENT ON COLUMN ${ct.name}.${col.name} IS '${escapeStr(c)}'"
    stmts.result()

  private def compileAlterTable(at: Migration.AlterTable): List[String] =
    val stmts = List.newBuilder[String]

    // Batch ADD COLUMN ops into a single ALTER TABLE
    val addCols = at.ops.collect { case AlterOp.AddColumn(col) => col }
    if addCols.nonEmpty then
      val clauses = addCols.map(c => s"  ADD COLUMN ${compileColumnDef(c)}")
      stmts += s"ALTER TABLE ${at.name}\n${clauses.mkString(",\n")}"
      // Column comments from ADD COLUMN
      addCols.foreach: col =>
        col.modifiers.comment.foreach: c =>
          stmts += s"COMMENT ON COLUMN ${at.name}.${col.name} IS '${escapeStr(c)}'"

    // All other ops
    at.ops.foreach:
      case _: AlterOp.AddColumn => // already handled
      case op                   => stmts ++= compileAlterOp(at.name, op)

    stmts.result()

  private def compileAlterOp(
      table: String,
      op: AlterOp
  ): List[String] = op match
    case AlterOp.AddColumn(_) =>
      Nil // handled in batch
    case AlterOp.DropColumn(name) =>
      List(s"ALTER TABLE $table DROP COLUMN $name")
    case AlterOp.DropColumnIfExists(name) =>
      List(s"ALTER TABLE $table DROP COLUMN IF EXISTS $name")
    case AlterOp.RenameColumn(from, to) =>
      List(s"ALTER TABLE $table RENAME COLUMN $from TO $to")
    case AlterOp.AlterColumnType(name, newType, usingExpr) =>
      val base = s"ALTER TABLE $table ALTER COLUMN $name TYPE ${compileType(newType)}"
      List(usingExpr.fold(base)(u => s"$base USING $u"))
    case AlterOp.SetColumnDefault(name, expr) =>
      List(s"ALTER TABLE $table ALTER COLUMN $name SET DEFAULT $expr")
    case AlterOp.DropColumnDefault(name) =>
      List(s"ALTER TABLE $table ALTER COLUMN $name DROP DEFAULT")
    case AlterOp.SetNotNull(name) =>
      List(s"ALTER TABLE $table ALTER COLUMN $name SET NOT NULL")
    case AlterOp.DropNotNull(name) =>
      List(s"ALTER TABLE $table ALTER COLUMN $name DROP NOT NULL")
    case idx: AlterOp.AddIndex =>
      val idxName = idx.name.getOrElse(
        autoName(table, idx.columns, if idx.unique then "unique" else "idx")
      )
      val sb = StringBuilder()
      sb.append("CREATE")
      if idx.unique then sb.append(" UNIQUE")
      sb.append(" INDEX")
      if idx.concurrently then sb.append(" CONCURRENTLY")
      sb.append(s" $idxName ON $table")
      idx.using.foreach(m => sb.append(s" USING ${compileIndexMethod(m)}"))
      sb.append(s" (${idx.columns.mkString(", ")})")
      idx.where.foreach(w => sb.append(s" WHERE $w"))
      List(sb.toString)
    case AlterOp.DropIndex(name) =>
      List(s"DROP INDEX $name")
    case AlterOp.DropIndexIfExists(name) =>
      List(s"DROP INDEX IF EXISTS $name")
    case AlterOp.DropIndexConcurrently(name) =>
      List(s"DROP INDEX CONCURRENTLY $name")
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
      if fk.onDelete != FkAction.NoAction then
        sb.append(s" ON DELETE ${compileFkAction(fk.onDelete)}")
      if fk.onUpdate != FkAction.NoAction then
        sb.append(s" ON UPDATE ${compileFkAction(fk.onUpdate)}")
      List(sb.toString)
    case AlterOp.DropForeignKey(name) =>
      List(s"ALTER TABLE $table DROP CONSTRAINT $name")
    case AlterOp.SetTableComment(comment) =>
      List(s"COMMENT ON TABLE $table IS '${escapeStr(comment)}'")
    case AlterOp.SetColumnComment(col, comment) =>
      List(s"COMMENT ON COLUMN $table.$col IS '${escapeStr(comment)}'")

  private def compileCreateEnum(ct: Migration.CreateEnumType): String =
    val vals = ct.values.map(v => s"'${escapeStr(v)}'").mkString(", ")
    s"CREATE TYPE ${ct.name} AS ENUM ($vals)"

  private def compileAddEnumValue(av: Migration.AddEnumValue): String =
    val base = s"ALTER TYPE ${av.typeName} ADD VALUE '${escapeStr(av.value)}'"
    av.position match
      case EnumValuePosition.End        => base
      case EnumValuePosition.Before(ex) => s"$base BEFORE '${escapeStr(ex)}'"
      case EnumValuePosition.After(ex)  => s"$base AFTER '${escapeStr(ex)}'"

  private def compileFkAction(action: FkAction): String = action match
    case FkAction.NoAction  => "NO ACTION"
    case FkAction.Restrict  => "RESTRICT"
    case FkAction.Cascade   => "CASCADE"
    case FkAction.SetNull   => "SET NULL"
    case FkAction.SetDefault => "SET DEFAULT"

  private def compileIndexMethod(m: IndexMethod): String = m match
    case IndexMethod.BTree  => "btree"
    case IndexMethod.Hash   => "hash"
    case IndexMethod.Gin    => "gin"
    case IndexMethod.Gist   => "gist"
    case IndexMethod.Brin   => "brin"
    case IndexMethod.SpGist => "spgist"

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

  private[migrate] def renderLiteral(value: Any): String = value match
    case s: String     => s"'${escapeStr(s)}'"
    case b: Boolean    => if b then "TRUE" else "FALSE"
    case _: (Int | Long | Short | Byte | Float | Double | BigDecimal) =>
      value.toString
    case u: UUID       => s"'$u'"
    case null          => "NULL"
    case other         => s"'${escapeStr(other.toString)}'"

  private def escapeStr(s: String): String =
    s.replace("'", "''")
