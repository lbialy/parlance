package com.augustnagro.magnum

enum ConflictTarget:
  case Columns(cols: ColRef[?]*)
  case Constraint(name: String)
  case AnyConflict
