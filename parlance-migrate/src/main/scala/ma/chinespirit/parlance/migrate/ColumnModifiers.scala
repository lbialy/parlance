package ma.chinespirit.parlance.migrate

case class ColumnModifiers(
    nullable: Boolean = false,
    default: Option[DefaultValue] = None,
    primaryKey: Boolean = false,
    autoIncrement: Boolean = false,
    unique: Boolean = false,
    check: Option[String] = None,
    comment: Option[String] = None,
    collation: Option[String] = None,
    generatedAs: Option[String] = None,
    references: Option[InlineReference] = None
)

enum DefaultValue:
  case Literal(value: Any)
  case Expression(sql: String)

case class InlineReference(
    table: String,
    column: String,
    onDelete: FkAction = FkAction.NoAction,
    onUpdate: FkAction = FkAction.NoAction
)
