package ma.chinespirit.parlance

private[parlance] type UnwrapSingle[K <: NonEmptyTuple] = K match
  case h *: EmptyTuple => h
  case _               => K
