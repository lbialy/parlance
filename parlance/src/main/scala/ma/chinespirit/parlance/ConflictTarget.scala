package ma.chinespirit.parlance

enum ConflictTarget:
  case Columns(cols: ColRef[?]*)
  case Constraint(name: String)
  case AnyConflict
