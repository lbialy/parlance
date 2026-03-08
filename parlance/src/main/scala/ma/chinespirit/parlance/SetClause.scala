package ma.chinespirit.parlance

/** A structured SET clause carrying a typed column reference + expression.
  *
  * Used by mutation hooks (`onUpdate`, `rewriteDelete`) to describe column assignments without raw SQL string parsing.
  *
  * @param col
  *   the column being set
  * @param expression
  *   the SQL expression (e.g. "CURRENT_TIMESTAMP" or "?")
  * @param params
  *   bind parameters for the expression
  * @param writer
  *   writes params to a PreparedStatement
  */
case class SetClause(col: Col[?], expression: String, params: Seq[Any], writer: FragWriter):
  /** Produces `col_name = expression` for use in SET clauses. */
  def sqlString: String = s"${col.sqlName} = $expression"

object SetClause:
  /** A literal SET clause with no bind parameters (e.g. `col = CURRENT_TIMESTAMP`). */
  def literal(col: Col[?], expression: String): SetClause =
    SetClause(col, expression, Seq.empty, FragWriter.empty)

  /** A parameterized SET clause (e.g. `col = ?`). */
  def parameterized(col: Col[?], value: Any): SetClause =
    SetClause(col, "?", Seq(value), FragWriter.fromKeys(Vector(value)))
