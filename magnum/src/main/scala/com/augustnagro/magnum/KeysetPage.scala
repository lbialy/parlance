package com.augustnagro.magnum

case class KeysetPage[E, K](
    items: Vector[E],
    nextKey: Option[K],
    prevKey: Option[K],
    hasMore: Boolean
)
