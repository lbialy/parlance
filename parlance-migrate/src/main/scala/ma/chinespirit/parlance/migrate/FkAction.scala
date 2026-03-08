package ma.chinespirit.parlance.migrate

enum FkAction:
  case NoAction, Restrict, Cascade, SetNull, SetDefault
