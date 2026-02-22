package com.augustnagro.magnum

case class KeysetColumnEntry(
    colRef: ColRef[?],
    sortOrder: SortOrder,
    nullOrder: NullOrder,
    codec: DbCodec[?],
    colIndex: Int
)
