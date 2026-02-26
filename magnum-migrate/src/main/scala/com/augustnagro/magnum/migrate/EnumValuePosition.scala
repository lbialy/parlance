package com.augustnagro.magnum.migrate

enum EnumValuePosition:
  case End
  case Before(existing: String)
  case After(existing: String)
