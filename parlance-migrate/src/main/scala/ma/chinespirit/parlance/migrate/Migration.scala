package ma.chinespirit.parlance.migrate

enum Migration:
  case CreateTable(
      name: String,
      columns: List[ColumnDef[?]],
      options: TableOptions = TableOptions.empty
  )
  case DropTable(name: String)
  case DropTableIfExists(name: String)
  case RenameTable(from: String, to: String)
  case AlterTable(name: String, ops: List[AlterOp])
  case CreateEnumType(name: String, values: List[String])
  case DropEnumType(name: String)
  case AddEnumValue(
      typeName: String,
      value: String,
      position: EnumValuePosition = EnumValuePosition.End
  )
  case RenameEnumValue(typeName: String, from: String, to: String)
  case CreateExtension(name: String)
  case DropExtension(name: String)
  case Raw(sql: String)
  case RawParameterized(sql: String, params: List[Any])
end Migration
