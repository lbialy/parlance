package com.augustnagro.magnum

class KeysetPaginator[E, K] private[magnum] (
    private val meta: TableMeta[E],
    private val entityCodec: DbCodec[E],
    private val rootPredicate: Option[Predicate],
    private val perPage: Int,
    private val entries: Vector[KeysetColumnEntry],
    private val keyExtractor: E => K,
    private val keyToValues: K => Vector[Any],
    private val afterKey: Option[K]
):

  def after(key: K): KeysetPaginator[E, K] =
    new KeysetPaginator(meta, entityCodec, rootPredicate, perPage, entries, keyExtractor, keyToValues, Some(key))

  def afterOpt(key: Option[K]): KeysetPaginator[E, K] =
    new KeysetPaginator(meta, entityCodec, rootPredicate, perPage, entries, keyExtractor, keyToValues, key)

  def run()(using DbCon): KeysetPage[E, K] =
    val selectCols = meta.columns.map(_.sqlName).mkString(", ")
    val baseSql = s"SELECT $selectCols FROM ${meta.tableName}"

    // Build WHERE clause
    val baseWhereFrag = rootPredicate.map(_.toFrag).filter(_.sqlString.nonEmpty)
    val keysetFrag = afterKey.map(k => KeysetSql.buildKeysetFrag(entries, keyToValues(k)))

    val whereFrags = Vector.concat(baseWhereFrag, keysetFrag)
    val (whereSql, whereParams, whereWriter) =
      if whereFrags.isEmpty then ("", Seq.empty[Any], FragWriter.empty)
      else
        val combined = combineFragsWithAnd(whereFrags)
        (" WHERE " + combined.sqlString, combined.params, combined.writer)

    // Build ORDER BY
    val orderBySql =
      val orderEntries = entries.map(e =>
        val base = s"${e.colRef.queryRepr} ${e.sortOrder.queryRepr}"
        if e.nullOrder.queryRepr.isEmpty then base else s"$base ${e.nullOrder.queryRepr}"
      )
      " ORDER BY " + orderEntries.mkString(", ")

    val limitSql = s" LIMIT ${perPage + 1}"

    val fullSql = baseSql + whereSql + orderBySql + limitSql
    val frag = Frag(fullSql, whereParams, whereWriter)
    val rawItems = frag.query[E](using entityCodec).run()

    val hasMore = rawItems.size > perPage
    val items = if hasMore then rawItems.take(perPage) else rawItems
    val nextKey = if hasMore then items.lastOption.map(keyExtractor) else None
    val prevKey = items.headOption.map(keyExtractor)

    KeysetPage(items, nextKey, prevKey, hasMore)
  end run

  private def combineFragsWithAnd(frags: Vector[Frag]): Frag =
    if frags.size == 1 then frags.head
    else
      val sql = frags.map(f => s"(${f.sqlString})").mkString(" AND ")
      val allParams = frags.flatMap(_.params)
      val allWriters = frags.map(_.writer)
      val writer: FragWriter = (ps, pos) =>
        var currentPos = pos
        for w <- allWriters do currentPos = w.write(ps, currentPos)
        currentPos
      Frag(sql, allParams, writer)
