package com.augustnagro.magnum

private[magnum] object ExistsBuilder:

  /** Extract and AND-combine all WHERE conditions from a set of scopes. */
  def scopeConditions[T](
      scopes: Vector[Scope[T]],
      meta: TableMeta[T]
  ): Option[WhereFrag] =
    val frags = scopes.flatMap(_.conditions(meta))
    if frags.isEmpty then None
    else Some(frags.reduce(_ && _))

  /** Merge a user-supplied condition with scope conditions. */
  private def mergeConditions(
      userCondition: Option[WhereFrag],
      scopeCondition: Option[WhereFrag]
  ): Option[WhereFrag] =
    (userCondition, scopeCondition) match
      case (None, None)       => None
      case (Some(u), None)    => Some(u)
      case (None, Some(s))    => Some(s)
      case (Some(u), Some(s)) => Some(u && s)

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
      negate: Boolean,
      scopes: Vector[Scope[T]] = Vector.empty[Scope[T]]
  ): WhereFrag =
    val correlation = s"${relMeta.tableName}.${rel.pk.sqlName} = ${sourceMeta.tableName}.${rel.fk.sqlName}"
    val merged = mergeConditions(condition, scopeConditions(scopes, relMeta))
    buildExistsFrag(relMeta.tableName, correlation, merged, negate)

  def buildPivotExistsFrag[S, T](
      sourceMeta: TableMeta[S],
      rel: BelongsToMany[S, T, ?],
      conditionWithMeta: Option[(WhereFrag, TableMeta[T])],
      negate: Boolean,
      scopes: Vector[Scope[T]] = Vector.empty[Scope[T]]
  ): WhereFrag =
    val correlation = s"${rel.pivotTable}.${rel.sourceFk} = ${sourceMeta.tableName}.${rel.sourcePk.sqlName}"
    // When we have scopes, we need to JOIN the target table to apply scope conditions
    val targetMeta = conditionWithMeta.map(_._2)
    val scopeCond = targetMeta
      .flatMap(tm => scopeConditions(scopes, tm))
      .orElse:
        // If no conditionWithMeta but scopes exist, we can't apply scope conditions
        // without the target meta — scopes require the table meta to produce conditions.
        // Callers must provide target meta when scopes are non-empty.
        None
    val fromClause = conditionWithMeta match
      case None if scopes.isEmpty => rel.pivotTable
      case None                   =>
        // scopes exist but no user condition — still just pivot table
        // (scope conditions can't be applied without target meta)
        rel.pivotTable
      case Some((_, tMeta)) =>
        s"${rel.pivotTable} JOIN ${tMeta.tableName} ON ${rel.pivotTable}.${rel.targetFk} = ${tMeta.tableName}.${rel.targetPk.sqlName}"
    val merged = mergeConditions(conditionWithMeta.map(_._1), scopeCond)
    buildExistsFrag(fromClause, correlation, merged, negate)
  end buildPivotExistsFrag

  /** Build a scope-aware pivot EXISTS fragment that can apply scopes even without a user condition. */
  def buildPivotExistsFragScoped[S, T](
      sourceMeta: TableMeta[S],
      rel: BelongsToMany[S, T, ?],
      conditionWithMeta: Option[(WhereFrag, TableMeta[T])],
      negate: Boolean,
      scopes: Vector[Scope[T]],
      targetMeta: TableMeta[T]
  ): WhereFrag =
    val correlation = s"${rel.pivotTable}.${rel.sourceFk} = ${sourceMeta.tableName}.${rel.sourcePk.sqlName}"
    val scopeCond = scopeConditions(scopes, targetMeta)
    val needsJoin = conditionWithMeta.isDefined || scopeCond.isDefined
    val fromClause =
      if needsJoin then
        s"${rel.pivotTable} JOIN ${targetMeta.tableName} ON ${rel.pivotTable}.${rel.targetFk} = ${targetMeta.tableName}.${rel.targetPk.sqlName}"
      else rel.pivotTable
    val merged = mergeConditions(conditionWithMeta.map(_._1), scopeCond)
    buildExistsFrag(fromClause, correlation, merged, negate)

  def buildCountSql(fromClause: String, correlation: String): String =
    s"SELECT COUNT(*) FROM $fromClause WHERE $correlation"

  def buildRelCountSql[S, T](
      sourceMeta: TableMeta[S],
      rel: Relationship[S, T],
      relMeta: TableMeta[T]
  ): String =
    val correlation = s"${relMeta.tableName}.${rel.pk.sqlName} = ${sourceMeta.tableName}.${rel.fk.sqlName}"
    buildCountSql(relMeta.tableName, correlation)

  /** Build count SQL with scope conditions merged in, returning a Frag to carry params. */
  def buildRelCountFrag[S, T](
      sourceMeta: TableMeta[S],
      rel: Relationship[S, T],
      relMeta: TableMeta[T],
      scopes: Vector[Scope[T]]
  ): Frag =
    val correlation = s"${relMeta.tableName}.${rel.pk.sqlName} = ${sourceMeta.tableName}.${rel.fk.sqlName}"
    val baseSql = buildCountSql(relMeta.tableName, correlation)
    scopeConditions(scopes, relMeta) match
      case None     => Frag(baseSql, Seq.empty, FragWriter.empty)
      case Some(sc) => Frag(s"$baseSql AND ${sc.sqlString}", sc.params, sc.writer)

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

  /** Build pivot count with scope conditions, returning a Frag. */
  def buildPivotCountFrag[S, T](
      sourceMeta: TableMeta[S],
      rel: BelongsToMany[S, T, ?],
      targetMeta: TableMeta[T],
      scopes: Vector[Scope[T]],
      hasUserCondition: Boolean = false
  ): Frag =
    val correlation = s"${rel.pivotTable}.${rel.sourceFk} = ${sourceMeta.tableName}.${rel.sourcePk.sqlName}"
    val scopeCond = scopeConditions(scopes, targetMeta)
    val needsJoin = scopeCond.isDefined || hasUserCondition
    val fromClause =
      if needsJoin then
        s"${rel.pivotTable} JOIN ${targetMeta.tableName} ON ${rel.pivotTable}.${rel.targetFk} = ${targetMeta.tableName}.${rel.targetPk.sqlName}"
      else rel.pivotTable
    val baseSql = buildCountSql(fromClause, correlation)
    scopeCond match
      case None     => Frag(baseSql, Seq.empty, FragWriter.empty)
      case Some(sc) => Frag(s"$baseSql AND ${sc.sqlString}", sc.params, sc.writer)

end ExistsBuilder
