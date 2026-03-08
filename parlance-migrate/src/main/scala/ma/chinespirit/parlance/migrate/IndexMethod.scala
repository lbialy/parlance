package ma.chinespirit.parlance.migrate

enum IndexMethod:
  case BTree, Hash, Gin, Gist, Brin, SpGist
