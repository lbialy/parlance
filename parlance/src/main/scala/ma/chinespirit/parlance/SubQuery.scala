package ma.chinespirit.parlance

class SubQuery[E, C <: Selectable] private[parlance] (
    private[parlance] val meta: TableMeta[E],
    private[parlance] val cols: C
):

  // --- HasMany / Relationship EXISTS ---

  def whereHas[T](rel: Relationship[E, T])(using
      relMeta: TableMeta[T],
      scoped: Scoped[T]
  ): WhereFrag =
    ExistsBuilder.buildRelExistsFrag(meta, rel, relMeta, None, negate = false, scoped.scopes)

  def whereHas[T](rel: BelongsTo[E, T])(f: SubQuery[T, Columns[T]] => WhereFrag)(using
      relMeta: TableMeta[T],
      scoped: Scoped[T]
  ): WhereFrag =
    val relCols = new Columns[T](relMeta.columns)
    val sq = new SubQuery[T, Columns[T]](relMeta, relCols)
    ExistsBuilder.buildRelExistsFrag(meta, rel, relMeta, Some(f(sq)), negate = false, scoped.scopes)

  def whereHas[T](rel: HasOne[E, T])(f: SubQuery[T, Columns[T]] => WhereFrag)(using
      relMeta: TableMeta[T],
      scoped: Scoped[T]
  ): WhereFrag =
    val relCols = new Columns[T](relMeta.columns)
    val sq = new SubQuery[T, Columns[T]](relMeta, relCols)
    ExistsBuilder.buildRelExistsFrag(meta, rel, relMeta, Some(f(sq)), negate = false, scoped.scopes)

  def whereHas[T](rel: HasMany[E, T, ?])(using
      relMeta: TableMeta[T],
      scoped: Scoped[T]
  ): WhereFrag =
    ExistsBuilder.buildRelExistsFrag(meta, rel, relMeta, None, negate = false, scoped.scopes)

  def whereHas[T, CT <: Selectable](rel: HasMany[E, T, CT])(f: SubQuery[T, CT] => WhereFrag)(using
      relMeta: TableMeta[T],
      scoped: Scoped[T]
  ): WhereFrag =
    val relCols = new Columns[T](relMeta.columns).asInstanceOf[CT]
    val sq = new SubQuery[T, CT](relMeta, relCols)
    ExistsBuilder.buildRelExistsFrag(meta, rel, relMeta, Some(f(sq)), negate = false, scoped.scopes)

  def doesntHave[T](rel: HasMany[E, T, ?])(using
      relMeta: TableMeta[T],
      scoped: Scoped[T]
  ): WhereFrag =
    ExistsBuilder.buildRelExistsFrag(meta, rel, relMeta, None, negate = true, scoped.scopes)

  def doesntHave[T](rel: Relationship[E, T])(using
      relMeta: TableMeta[T],
      scoped: Scoped[T]
  ): WhereFrag =
    ExistsBuilder.buildRelExistsFrag(meta, rel, relMeta, None, negate = true, scoped.scopes)

  // --- BelongsToMany EXISTS ---

  def whereHas[T](rel: BelongsToMany[E, T, ?])(using
      relMeta: TableMeta[T],
      scoped: Scoped[T]
  ): WhereFrag =
    ExistsBuilder.buildPivotExistsFragScoped(meta, rel, None, negate = false, scoped.scopes, relMeta)

  def whereHas[T, CT <: Selectable](rel: BelongsToMany[E, T, CT])(f: SubQuery[T, CT] => WhereFrag)(using
      relMeta: TableMeta[T],
      scoped: Scoped[T]
  ): WhereFrag =
    val relCols = new Columns[T](relMeta.columns).asInstanceOf[CT]
    val sq = new SubQuery[T, CT](relMeta, relCols)
    ExistsBuilder.buildPivotExistsFragScoped(meta, rel, Some((f(sq), relMeta)), negate = false, scoped.scopes, relMeta)

  def doesntHave[T](rel: BelongsToMany[E, T, ?])(using
      relMeta: TableMeta[T],
      scoped: Scoped[T]
  ): WhereFrag =
    ExistsBuilder.buildPivotExistsFragScoped(meta, rel, None, negate = true, scoped.scopes, relMeta)

end SubQuery

object SubQuery:
  given [E, C <: Selectable]: Conversion[SubQuery[E, C], C] = _.cols
