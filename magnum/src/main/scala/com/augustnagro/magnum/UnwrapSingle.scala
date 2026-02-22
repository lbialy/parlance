package com.augustnagro.magnum

private[magnum] type UnwrapSingle[K <: NonEmptyTuple] = K match
  case h *: EmptyTuple => h
  case _               => K
