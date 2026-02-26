package com.augustnagro.magnum

enum ConflictAction:
  case DoNothing
  case DoUpdate(assignments: Frag)
