package com.augustnagro.magnum

private[magnum] object ExistsBuilder:

  def buildExistsFrag(
      fromClause: String,
      correlation: String,
      condition: Option[WhereFrag],
      negate: Boolean
  ): WhereFrag =
    val prefix = if negate then "NOT EXISTS" else "EXISTS"
    condition match
      case None =>
        WhereFrag(Frag(s"$prefix (SELECT 1 FROM $fromClause WHERE $correlation)", Seq.empty, FragWriter.empty))
      case Some(cond) =>
        WhereFrag(Frag(s"$prefix (SELECT 1 FROM $fromClause WHERE $correlation AND ${cond.sqlString})", cond.params, cond.writer))

  def buildRelExistsFrag[S, T](
      sourceMeta: TableMeta[S],
      rel: Relationship[S, T],
      relMeta: TableMeta[T],
      condition: Option[WhereFrag],
      negate: Boolean
  ): WhereFrag =
    val correlation = s"${relMeta.tableName}.${rel.pk.sqlName} = ${sourceMeta.tableName}.${rel.fk.sqlName}"
    buildExistsFrag(relMeta.tableName, correlation, condition, negate)

  def buildPivotExistsFrag[S, T](
      sourceMeta: TableMeta[S],
      rel: BelongsToMany[S, T, ?],
      conditionWithMeta: Option[(WhereFrag, TableMeta[T])],
      negate: Boolean
  ): WhereFrag =
    val correlation = s"${rel.pivotTable}.${rel.sourceFk} = ${sourceMeta.tableName}.${rel.sourcePk.sqlName}"
    val fromClause = conditionWithMeta match
      case None => rel.pivotTable
      case Some((_, tMeta)) =>
        s"${rel.pivotTable} JOIN ${tMeta.tableName} ON ${rel.pivotTable}.${rel.targetFk} = ${tMeta.tableName}.${rel.targetPk.sqlName}"
    buildExistsFrag(fromClause, correlation, conditionWithMeta.map(_._1), negate)

  def buildCountSql(fromClause: String, correlation: String): String =
    s"SELECT COUNT(*) FROM $fromClause WHERE $correlation"

  def buildRelCountSql[S, T](
      sourceMeta: TableMeta[S],
      rel: Relationship[S, T],
      relMeta: TableMeta[T]
  ): String =
    val correlation = s"${relMeta.tableName}.${rel.pk.sqlName} = ${sourceMeta.tableName}.${rel.fk.sqlName}"
    buildCountSql(relMeta.tableName, correlation)

  def buildPivotCountSql[S, T](
      sourceMeta: TableMeta[S],
      rel: BelongsToMany[S, T, ?],
      targetMeta: Option[TableMeta[T]]
  ): String =
    val correlation = s"${rel.pivotTable}.${rel.sourceFk} = ${sourceMeta.tableName}.${rel.sourcePk.sqlName}"
    val fromClause = targetMeta match
      case None => rel.pivotTable
      case Some(tMeta) =>
        s"${rel.pivotTable} JOIN ${tMeta.tableName} ON ${rel.pivotTable}.${rel.targetFk} = ${tMeta.tableName}.${rel.targetPk.sqlName}"
    buildCountSql(fromClause, correlation)

end ExistsBuilder
