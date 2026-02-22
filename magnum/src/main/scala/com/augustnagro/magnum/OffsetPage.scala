package com.augustnagro.magnum

case class OffsetPage[E](
    items: Vector[E],
    total: Long,
    page: Int,
    perPage: Int
):
  def totalPages: Int =
    if perPage <= 0 then 0
    else ((total + perPage - 1) / perPage).toInt
  def hasNext: Boolean = page < totalPages
  def hasPrev: Boolean = page > 1
