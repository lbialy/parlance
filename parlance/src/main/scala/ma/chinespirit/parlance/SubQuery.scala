package ma.chinespirit.parlance

class SubQuery[E, C <: Selectable, P <: ScopePolicy] private[parlance] (
    private[parlance] val meta: TableMeta[E],
    private[parlance] val cols: C
):

  // --- HasMany / Relationship EXISTS ---

  def whereHas[T](rel: Relationship[E, T])(using
      relMeta: TableMeta[T],
      resolve: ResolveScopes[T, P]
  ): WhereFrag =
    ExistsBuilder.buildRelExistsFrag(meta, rel, relMeta, None, negate = false, resolve.scopes)

  def whereHas[T](rel: BelongsTo[E, T])(f: SubQuery[T, Columns[T], P] => WhereFrag)(using
      relMeta: TableMeta[T],
      resolve: ResolveScopes[T, P]
  ): WhereFrag =
    val relCols = new Columns[T](relMeta.columns)
    val sq = new SubQuery[T, Columns[T], P](relMeta, relCols)
    ExistsBuilder.buildRelExistsFrag(meta, rel, relMeta, Some(f(sq)), negate = false, resolve.scopes)

  def whereHas[T](rel: HasOne[E, T])(f: SubQuery[T, Columns[T], P] => WhereFrag)(using
      relMeta: TableMeta[T],
      resolve: ResolveScopes[T, P]
  ): WhereFrag =
    val relCols = new Columns[T](relMeta.columns)
    val sq = new SubQuery[T, Columns[T], P](relMeta, relCols)
    ExistsBuilder.buildRelExistsFrag(meta, rel, relMeta, Some(f(sq)), negate = false, resolve.scopes)

  def whereHas[T](rel: HasMany[E, T, ?])(using
      relMeta: TableMeta[T],
      resolve: ResolveScopes[T, P]
  ): WhereFrag =
    ExistsBuilder.buildRelExistsFrag(meta, rel, relMeta, None, negate = false, resolve.scopes)

  def whereHas[T, CT <: Selectable](rel: HasMany[E, T, CT])(f: SubQuery[T, CT, P] => WhereFrag)(using
      relMeta: TableMeta[T],
      resolve: ResolveScopes[T, P]
  ): WhereFrag =
    val relCols = new Columns[T](relMeta.columns).asInstanceOf[CT]
    val sq = new SubQuery[T, CT, P](relMeta, relCols)
    ExistsBuilder.buildRelExistsFrag(meta, rel, relMeta, Some(f(sq)), negate = false, resolve.scopes)

  def doesntHave[T](rel: HasMany[E, T, ?])(using
      relMeta: TableMeta[T],
      resolve: ResolveScopes[T, P]
  ): WhereFrag =
    ExistsBuilder.buildRelExistsFrag(meta, rel, relMeta, None, negate = true, resolve.scopes)

  def doesntHave[T](rel: Relationship[E, T])(using
      relMeta: TableMeta[T],
      resolve: ResolveScopes[T, P]
  ): WhereFrag =
    ExistsBuilder.buildRelExistsFrag(meta, rel, relMeta, None, negate = true, resolve.scopes)

  // --- BelongsToMany EXISTS ---

  def whereHas[T](rel: BelongsToMany[E, T, ?])(using
      relMeta: TableMeta[T],
      resolve: ResolveScopes[T, P]
  ): WhereFrag =
    ExistsBuilder.buildPivotExistsFragScoped(meta, rel, None, negate = false, resolve.scopes, relMeta)

  def whereHas[T, CT <: Selectable](rel: BelongsToMany[E, T, CT])(f: SubQuery[T, CT, P] => WhereFrag)(using
      relMeta: TableMeta[T],
      resolve: ResolveScopes[T, P]
  ): WhereFrag =
    val relCols = new Columns[T](relMeta.columns).asInstanceOf[CT]
    val sq = new SubQuery[T, CT, P](relMeta, relCols)
    ExistsBuilder.buildPivotExistsFragScoped(meta, rel, Some((f(sq), relMeta)), negate = false, resolve.scopes, relMeta)

  def doesntHave[T](rel: BelongsToMany[E, T, ?])(using
      relMeta: TableMeta[T],
      resolve: ResolveScopes[T, P]
  ): WhereFrag =
    ExistsBuilder.buildPivotExistsFragScoped(meta, rel, None, negate = true, resolve.scopes, relMeta)

end SubQuery

object SubQuery:
  given [E, C <: Selectable, P <: ScopePolicy]: Conversion[SubQuery[E, C, P], C] = _.cols
