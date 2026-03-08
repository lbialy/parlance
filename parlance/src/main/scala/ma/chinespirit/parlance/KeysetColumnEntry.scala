package ma.chinespirit.parlance

case class KeysetColumnEntry(
    colRef: ColRef[?],
    sortOrder: SortOrder,
    nullOrder: NullOrder,
    codec: DbCodec[?],
    colIndex: Int
)
