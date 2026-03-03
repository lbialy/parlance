package com.augustnagro.magnum

import java.sql.{PreparedStatement, ResultSet}

class ProjectedQuery[P, PC](
    private val fromClause: String,
    private val fromParams: Seq[Any],
    private val fromWriter: FragWriter,
    private val selectExprs: Vector[SelectExpr[?]],
    private val resultCodec: DbCodec[P],
    private val projCols: PC,
    private val wherePredicate: Option[Predicate],
    private val groupByExprs: Vector[ColRef[?]],
    private val havingPredicate: Option[Predicate],
    private val orderEntries: Vector[(ColRef[?], SortOrder, NullOrder)],
    private val limitOpt: Option[Int],
    private val offsetOpt: Option[Long],
    private val distinctFlag: Boolean = false
):

  def groupBy(f: PC => ColRef[?]): ProjectedQuery[P, PC] =
    new ProjectedQuery(fromClause, fromParams, fromWriter, selectExprs, resultCodec, projCols, wherePredicate, groupByExprs :+ f(projCols), havingPredicate, orderEntries, limitOpt, offsetOpt, distinctFlag)

  def having(f: PC => WhereFrag): ProjectedQuery[P, PC] =
    val pred = Predicate.Leaf(f(projCols))
    val newHaving = QuerySqlBuilder.addAnd(havingPredicate, pred)
    new ProjectedQuery(fromClause, fromParams, fromWriter, selectExprs, resultCodec, projCols, wherePredicate, groupByExprs, newHaving, orderEntries, limitOpt, offsetOpt, distinctFlag)

  def having(frag: WhereFrag): ProjectedQuery[P, PC] =
    val pred = Predicate.Leaf(frag)
    val newHaving = QuerySqlBuilder.addAnd(havingPredicate, pred)
    new ProjectedQuery(fromClause, fromParams, fromWriter, selectExprs, resultCodec, projCols, wherePredicate, groupByExprs, newHaving, orderEntries, limitOpt, offsetOpt, distinctFlag)

  def orHaving(f: PC => WhereFrag): ProjectedQuery[P, PC] =
    val pred = Predicate.Leaf(f(projCols))
    val newHaving = QuerySqlBuilder.addOr(havingPredicate, pred)
    new ProjectedQuery(fromClause, fromParams, fromWriter, selectExprs, resultCodec, projCols, wherePredicate, groupByExprs, newHaving, orderEntries, limitOpt, offsetOpt, distinctFlag)

  def orderBy(f: PC => ColRef[?], order: SortOrder = SortOrder.Asc, nullOrder: NullOrder = NullOrder.Default): ProjectedQuery[P, PC] =
    new ProjectedQuery(fromClause, fromParams, fromWriter, selectExprs, resultCodec, projCols, wherePredicate, groupByExprs, havingPredicate, orderEntries :+ (f(projCols), order, nullOrder), limitOpt, offsetOpt, distinctFlag)

  def orderBy(f: PC => ColRef[?]): ProjectedQuery[P, PC] =
    new ProjectedQuery(fromClause, fromParams, fromWriter, selectExprs, resultCodec, projCols, wherePredicate, groupByExprs, havingPredicate, orderEntries :+ (f(projCols), SortOrder.Asc, NullOrder.Default), limitOpt, offsetOpt, distinctFlag)

  def limit(n: Int): ProjectedQuery[P, PC] =
    if n < 0 then throw QueryBuilderException("limit must not be negative")
    new ProjectedQuery(fromClause, fromParams, fromWriter, selectExprs, resultCodec, projCols, wherePredicate, groupByExprs, havingPredicate, orderEntries, Some(n), offsetOpt, distinctFlag)

  def offset(n: Long): ProjectedQuery[P, PC] =
    if n < 0 then throw QueryBuilderException("offset must not be negative")
    new ProjectedQuery(fromClause, fromParams, fromWriter, selectExprs, resultCodec, projCols, wherePredicate, groupByExprs, havingPredicate, orderEntries, limitOpt, Some(n), distinctFlag)

  def distinct: ProjectedQuery[P, PC] =
    new ProjectedQuery(fromClause, fromParams, fromWriter, selectExprs, resultCodec, projCols, wherePredicate, groupByExprs, havingPredicate, orderEntries, limitOpt, offsetOpt, true)

  def build(using con: DbCon[?]): Frag =
    buildWith(con.databaseType)

  def buildWith(dt: DatabaseType): Frag =
    val selectClause = selectExprs.map(e => s"${e.queryRepr} AS ${e.alias}").mkString(", ")
    val keyword = if distinctFlag then "SELECT DISTINCT" else "SELECT"
    val baseSql = s"$keyword $selectClause $fromClause"

    val (whereSql, whereParams, whereWriter) = QuerySqlBuilder.buildWhere(wherePredicate)
    val groupBySql = QuerySqlBuilder.buildGroupBy(groupByExprs)
    val (havingSql, havingParams, havingWriter) = QuerySqlBuilder.buildHaving(havingPredicate)
    val (orderBySql, orderByParams, orderByWriter) = QuerySqlBuilder.buildOrderBy(orderEntries)
    val limitOffsetSql = QuerySqlBuilder.buildLimitOffset(limitOpt, offsetOpt, dt)

    val allParams = fromParams ++ whereParams ++ havingParams ++ orderByParams
    val combinedWriter: FragWriter = (ps, pos) =>
      val afterFrom = fromWriter.write(ps, pos)
      val afterWhere = whereWriter.write(ps, afterFrom)
      val afterHaving = havingWriter.write(ps, afterWhere)
      orderByWriter.write(ps, afterHaving)

    Frag(baseSql + whereSql + groupBySql + havingSql + orderBySql + limitOffsetSql, allParams, combinedWriter)

  def run()(using DbCon[?]): Vector[P] =
    build.query[P](using resultCodec).run()

  def first()(using DbCon[?]): Option[P] =
    limit(1).run().headOption

  def firstOrFail()(using DbCon[?]): P =
    first().getOrElse(
      throw QueryBuilderException(
        s"No projection found matching query"
      )
    )
end ProjectedQuery

object ProjectedQuery:
  def positionalCodec[P](codecs: IArray[DbCodec[?]]): DbCodec[P] =
    new DbCodec[P]:
      val cols: IArray[Int] = codecs.flatMap(_.cols)
      def queryRepr: String = codecs.map(_.queryRepr).mkString(", ")

      def readSingle(rs: ResultSet, pos: Int): P =
        val n = codecs.length
        val result = new Array[Any](n)
        var i = 0
        var p = pos
        while i < n do
          result(i) = codecs(i).readSingle(rs, p)
          p += codecs(i).cols.length
          i += 1
        Tuple.fromArray(result.asInstanceOf[Array[Object]]).asInstanceOf[P]

      def readSingleOption(rs: ResultSet, pos: Int): Option[P] =
        val n = codecs.length
        val result = new Array[Any](n)
        var i = 0
        var p = pos
        while i < n do
          codecs(i).readSingleOption(rs, p) match
            case Some(v) => result(i) = v
            case None    => return None
          p += codecs(i).cols.length
          i += 1
        Some(Tuple.fromArray(result.asInstanceOf[Array[Object]]).asInstanceOf[P])

      def writeSingle(entity: P, ps: PreparedStatement, pos: Int): Unit =
        val prod = entity.asInstanceOf[Product]
        var i = 0
        var p = pos
        while i < codecs.length do
          codecs(i).asInstanceOf[DbCodec[Any]].writeSingle(prod.productElement(i), ps, p)
          p += codecs(i).cols.length
          i += 1
end ProjectedQuery
