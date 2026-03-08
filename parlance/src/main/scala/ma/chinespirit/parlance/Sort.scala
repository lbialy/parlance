package ma.chinespirit.parlance

class Sort private[parlance] (
    val column: String,
    val direction: SortOrder,
    val nullOrder: NullOrder
)
