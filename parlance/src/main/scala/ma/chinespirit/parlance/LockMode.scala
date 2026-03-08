package ma.chinespirit.parlance

enum LockMode(val sql: String):
  case ForUpdate extends LockMode("FOR UPDATE")
  case ForShare extends LockMode("FOR SHARE")
