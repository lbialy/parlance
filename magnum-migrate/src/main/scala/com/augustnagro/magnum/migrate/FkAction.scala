package com.augustnagro.magnum.migrate

enum FkAction:
  case NoAction, Restrict, Cascade, SetNull, SetDefault
