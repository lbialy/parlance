package ma.chinespirit.parlance

/** Represents a single JOIN clause in a query.
  *
  * @param tableRef
  *   the joined table and its alias
  * @param joinType
  *   the type of join (INNER, LEFT, etc.)
  * @param onCondition
  *   the ON condition as a Frag
  */
case class JoinEntry(
    tableRef: TableRef,
    joinType: JoinType,
    onCondition: Frag
)
