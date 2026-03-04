package com.augustnagro.magnum

class KeysetColumns[E, C <: Selectable, K <: Tuple] private[magnum] (
    private[magnum] val cols: C,
    private[magnum] val meta: TableMeta[E],
    private[magnum] val entries: Vector[KeysetColumnEntry]
):
  def asc[A](f: C => ColRef[A], nullOrder: NullOrder = NullOrder.Default)(using
      codec: DbCodec[A]
  ): KeysetColumns[E, C, Tuple.Append[K, A]] =
    val colRef = f(cols)
    val colIndex = meta.columns.indexWhere(_.scalaName == colRef.scalaName)
    new KeysetColumns(cols, meta, entries :+ KeysetColumnEntry(colRef, SortOrder.Asc, nullOrder, codec, colIndex))

  def desc[A](f: C => ColRef[A], nullOrder: NullOrder = NullOrder.Default)(using
      codec: DbCodec[A]
  ): KeysetColumns[E, C, Tuple.Append[K, A]] =
    val colRef = f(cols)
    val colIndex = meta.columns.indexWhere(_.scalaName == colRef.scalaName)
    new KeysetColumns(cols, meta, entries :+ KeysetColumnEntry(colRef, SortOrder.Desc, nullOrder, codec, colIndex))
