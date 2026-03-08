package ma.chinespirit.parlance

class KeysetColumns[E, C <: Selectable, K <: Tuple] private[parlance] (
    private[parlance] val cols: C,
    private[parlance] val meta: TableMeta[E],
    private[parlance] val entries: Vector[KeysetColumnEntry]
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
