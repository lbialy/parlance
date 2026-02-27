package com.augustnagro.magnum.migrate

enum IndexMethod:
  case BTree, Hash, Gin, Gist, Brin, SpGist
