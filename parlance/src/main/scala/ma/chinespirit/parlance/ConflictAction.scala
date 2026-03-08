package ma.chinespirit.parlance

enum ConflictAction:
  case DoNothing
  case DoUpdate(assignments: Frag)
