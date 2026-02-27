package com.augustnagro.magnum

class SubQuery[E, C <: Selectable] private[magnum] (
    private[magnum] val meta: TableMeta[E],
    private[magnum] val cols: C
):

  // --- HasMany / Relationship EXISTS ---

  def whereHas[T](rel: Relationship[E, T])(using
      relMeta: TableMeta[T]
  ): WhereFrag =
    ExistsBuilder.buildRelExistsFrag(meta, rel, relMeta, None, negate = false)

  def whereHas[T](rel: BelongsTo[E, T])(f: SubQuery[T, Columns[T]] => WhereFrag)(using
      relMeta: TableMeta[T]
  ): WhereFrag =
    val relCols = new Columns[T](relMeta.columns)
    val sq = new SubQuery[T, Columns[T]](relMeta, relCols)
    ExistsBuilder.buildRelExistsFrag(meta, rel, relMeta, Some(f(sq)), negate = false)

  def whereHas[T](rel: HasOne[E, T])(f: SubQuery[T, Columns[T]] => WhereFrag)(using
      relMeta: TableMeta[T]
  ): WhereFrag =
    val relCols = new Columns[T](relMeta.columns)
    val sq = new SubQuery[T, Columns[T]](relMeta, relCols)
    ExistsBuilder.buildRelExistsFrag(meta, rel, relMeta, Some(f(sq)), negate = false)

  def whereHas[T](rel: HasMany[E, T, ?])(using
      relMeta: TableMeta[T]
  ): WhereFrag =
    ExistsBuilder.buildRelExistsFrag(meta, rel, relMeta, None, negate = false)

  def whereHas[T, CT <: Selectable](rel: HasMany[E, T, CT])(f: SubQuery[T, CT] => WhereFrag)(using
      relMeta: TableMeta[T]
  ): WhereFrag =
    val relCols = new Columns[T](relMeta.columns).asInstanceOf[CT]
    val sq = new SubQuery[T, CT](relMeta, relCols)
    ExistsBuilder.buildRelExistsFrag(meta, rel, relMeta, Some(f(sq)), negate = false)

  def doesntHave[T](rel: HasMany[E, T, ?])(using
      relMeta: TableMeta[T]
  ): WhereFrag =
    ExistsBuilder.buildRelExistsFrag(meta, rel, relMeta, None, negate = true)

  def doesntHave[T](rel: Relationship[E, T])(using
      relMeta: TableMeta[T]
  ): WhereFrag =
    ExistsBuilder.buildRelExistsFrag(meta, rel, relMeta, None, negate = true)

  // --- BelongsToMany EXISTS ---

  def whereHas[T](rel: BelongsToMany[E, T, ?]): WhereFrag =
    ExistsBuilder.buildPivotExistsFrag(meta, rel, None, negate = false)

  def whereHas[T, CT <: Selectable](rel: BelongsToMany[E, T, CT])(f: SubQuery[T, CT] => WhereFrag)(using
      relMeta: TableMeta[T]
  ): WhereFrag =
    val relCols = new Columns[T](relMeta.columns).asInstanceOf[CT]
    val sq = new SubQuery[T, CT](relMeta, relCols)
    ExistsBuilder.buildPivotExistsFrag(meta, rel, Some((f(sq), relMeta)), negate = false)

  def doesntHave[T](rel: BelongsToMany[E, T, ?]): WhereFrag =
    ExistsBuilder.buildPivotExistsFrag(meta, rel, None, negate = true)

end SubQuery

object SubQuery:
  given [E, C <: Selectable]: Conversion[SubQuery[E, C], C] = _.cols
