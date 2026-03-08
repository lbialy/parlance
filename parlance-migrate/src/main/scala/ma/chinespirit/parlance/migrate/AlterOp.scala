package ma.chinespirit.parlance.migrate

enum AlterOp:
  // Column operations
  case AddColumn(col: ColumnDef[?])
  case DropColumn(name: String)
  case DropColumnIfExists(name: String)
  case RenameColumn(from: String, to: String)
  case AlterColumnType(
      name: String,
      newType: ColumnType,
      using: Option[String] = None
  )
  case SetColumnDefault(name: String, expression: String)
  case DropColumnDefault(name: String)
  case SetNotNull(name: String)
  case DropNotNull(name: String)

  // Index operations
  case AddIndex(
      columns: List[String],
      unique: Boolean = false,
      name: Option[String] = None,
      using: Option[IndexMethod] = None,
      where: Option[String] = None,
      concurrently: Boolean = false
  )
  case DropIndex(name: String)
  case DropIndexIfExists(name: String)
  case DropIndexConcurrently(name: String)
  case RenameIndex(from: String, to: String)

  // Primary key
  case AddPrimaryKey(columns: List[String], name: Option[String] = None)
  case DropConstraint(name: String)

  // Unique constraint
  case AddUniqueConstraint(
      columns: List[String],
      name: Option[String] = None
  )

  // Check constraint
  case AddCheckConstraint(name: String, expression: String)

  // Foreign key
  case AddForeignKey(
      columns: List[String],
      refTable: String,
      refColumns: List[String],
      onDelete: FkAction = FkAction.NoAction,
      onUpdate: FkAction = FkAction.NoAction,
      name: Option[String] = None
  )
  case DropForeignKey(name: String)

  // Comments (compiled as separate statements)
  case SetTableComment(comment: String)
  case SetColumnComment(column: String, comment: String)
end AlterOp
