package ma.chinespirit.parlance

class Seek private[parlance] (
    val column: String,
    val seekDirection: SeekDir,
    val value: Any,
    val columnSort: SortOrder,
    val nullOrder: NullOrder,
    val codec: DbCodec[?]
)
