package com.augustnagro.magnum

import java.sql.{PreparedStatement, ResultSet}
import scala.NamedTuple
import scala.deriving.Mirror
import scala.quoted.*

class JoinedQuery[R <: NonEmptyTuple] private[magnum] (
    private[magnum] val metas: Vector[TableMeta[?]],
    private val codecs: Vector[DbCodec[?]],
    private val joinClauses: Vector[JoinEntry],
    private[magnum] val predicate: Option[Predicate],
    private val orderEntries: Vector[(ColRef[?], SortOrder, NullOrder)],
    private val limitOpt: Option[Int],
    private val offsetOpt: Option[Long]
):

  def alias(index: Int): String = s"t$index"
  def col[A](index: Int, c: Col[A]): BoundCol[A] = c.bound(alias(index))
  def rootCol[A](c: Col[A]): BoundCol[A] = col(0, c)
  def joinedCol[A](c: Col[A]): BoundCol[A] = col(1, c)

  private def addAnd(pred: Predicate): Option[Predicate] =
    QuerySqlBuilder.addAnd(predicate, pred)

  private def addOr(pred: Predicate): Option[Predicate] =
    QuerySqlBuilder.addOr(predicate, pred)

  def where(frag: WhereFrag): JoinedQuery[R] =
    new JoinedQuery(metas, codecs, joinClauses, addAnd(Predicate.Leaf(frag)), orderEntries, limitOpt, offsetOpt)

  def orWhere(frag: WhereFrag): JoinedQuery[R] =
    new JoinedQuery(metas, codecs, joinClauses, addOr(Predicate.Leaf(frag)), orderEntries, limitOpt, offsetOpt)

  def orderBy(col: ColRef[?], order: SortOrder = SortOrder.Asc, nullOrder: NullOrder = NullOrder.Default): JoinedQuery[R] =
    new JoinedQuery(metas, codecs, joinClauses, predicate, orderEntries :+ (col, order, nullOrder), limitOpt, offsetOpt)

  def limit(n: Int): JoinedQuery[R] =
    new JoinedQuery(metas, codecs, joinClauses, predicate, orderEntries, Some(n), offsetOpt)

  def offset(n: Long): JoinedQuery[R] =
    new JoinedQuery(metas, codecs, joinClauses, predicate, orderEntries, limitOpt, Some(n))

  def join[S, U](rel: Relationship[S, U])(using
      sMeta: TableMeta[S],
      uMeta: TableMeta[U],
      uCodec: DbCodec[U],
      scoped: Scoped[U]
  ): JoinedQuery[Tuple.Append[R, U]] =
    val sourceIdx = metas.indexWhere(_.tableName == sMeta.tableName)
    require(sourceIdx >= 0, s"Table ${sMeta.tableName} not in join chain")
    val newIdx = metas.size
    val alias = s"t$newIdx"
    val baseOn = s"t$sourceIdx.${rel.fk.sqlName} = $alias.${rel.pk.sqlName}"
    val onFrag = ExistsBuilder.scopeConditions(scoped.scopes, uMeta) match
      case None => Frag(baseOn, Seq.empty, FragWriter.empty)
      case Some(sc) =>
        val scopeSql = sc.sqlString.replace(uMeta.tableName + ".", alias + ".")
        Frag(s"$baseOn AND $scopeSql", sc.params, sc.writer)
    val entry = JoinEntry(
      TableRef(uMeta.tableName, alias, uMeta.tableName),
      JoinType.Inner,
      onFrag
    )
    new JoinedQuery[Tuple.Append[R, U]](
      metas :+ uMeta,
      codecs :+ uCodec,
      joinClauses :+ entry,
      predicate,
      orderEntries,
      limitOpt,
      offsetOpt
    )
  end join

  def leftJoin[S, U](rel: Relationship[S, U])(using
      sMeta: TableMeta[S],
      uMeta: TableMeta[U],
      uCodec: DbCodec[U],
      scoped: Scoped[U]
  ): JoinedQuery[Tuple.Append[R, Option[U]]] =
    val sourceIdx = metas.indexWhere(_.tableName == sMeta.tableName)
    require(sourceIdx >= 0, s"Table ${sMeta.tableName} not in join chain")
    val newIdx = metas.size
    val alias = s"t$newIdx"
    val optCodec = DbCodec.OptionCodec[U](using uCodec)
    val baseOn = s"t$sourceIdx.${rel.fk.sqlName} = $alias.${rel.pk.sqlName}"
    val onFrag = ExistsBuilder.scopeConditions(scoped.scopes, uMeta) match
      case None => Frag(baseOn, Seq.empty, FragWriter.empty)
      case Some(sc) =>
        val scopeSql = sc.sqlString.replace(uMeta.tableName + ".", alias + ".")
        Frag(s"$baseOn AND $scopeSql", sc.params, sc.writer)
    val entry = JoinEntry(
      TableRef(uMeta.tableName, alias, uMeta.tableName),
      JoinType.Left,
      onFrag
    )
    new JoinedQuery[Tuple.Append[R, Option[U]]](
      metas :+ uMeta,
      codecs :+ optCodec,
      joinClauses :+ entry,
      predicate,
      orderEntries,
      limitOpt,
      offsetOpt
    )
  end leftJoin

  private def resultCodec: DbCodec[R] =
    new DbCodec[R]:
      val cols: IArray[Int] = IArray.from(codecs.flatMap(_.cols.toSeq))
      def queryRepr: String = codecs.map(_.queryRepr).mkString("(", ", ", ")")
      def readSingle(rs: ResultSet, pos: Int): R =
        val arr = new Array[Any](codecs.size)
        var p = pos
        for i <- codecs.indices do
          arr(i) = codecs(i).readSingle(rs, p)
          p += codecs(i).cols.length
        Tuple.fromArray(arr).asInstanceOf[R]
      def readSingleOption(rs: ResultSet, pos: Int): Option[R] =
        val arr = new Array[Any](codecs.size)
        var p = pos
        var allPresent = true
        for i <- codecs.indices do
          codecs(i).readSingleOption(rs, p) match
            case Some(v) => arr(i) = v
            case None    => allPresent = false
          p += codecs(i).cols.length
        if allPresent then Some(Tuple.fromArray(arr).asInstanceOf[R])
        else None
      def writeSingle(entity: R, ps: PreparedStatement, pos: Int): Unit =
        var p = pos
        for i <- codecs.indices do
          codecs(i).asInstanceOf[DbCodec[Any]].writeSingle(entity.productElement(i), ps, p)
          p += codecs(i).cols.length

  private def buildJoinsSql: String =
    joinClauses
      .map { entry =>
        val kw = entry.joinType match
          case JoinType.Inner => "INNER JOIN"
          case JoinType.Left  => "LEFT JOIN"
          case JoinType.Right => "RIGHT JOIN"
          case JoinType.Cross => "CROSS JOIN"
        s"$kw ${entry.tableRef.tableName} ${entry.tableRef.alias} ON ${entry.onCondition.sqlString}"
      }
      .mkString(" ", " ", "")

  private[magnum] def buildFromJoin: String =
    s"FROM ${metas(0).tableName} t0" + buildJoinsSql

  private[magnum] def joinOnParams: Seq[Any] =
    joinClauses.flatMap(_.onCondition.params)

  private[magnum] def joinOnWriter: FragWriter =
    joinClauses.foldLeft(FragWriter.empty) { (acc, entry) =>
      (ps, pos) =>
        val afterAcc = acc.write(ps, pos)
        entry.onCondition.writer.write(ps, afterAcc)
    }

  private def buildFromJoinWhere: (String, Seq[Any], FragWriter) =
    val fromJoinSql = buildFromJoin

    val (whereSql, whereParams, whereWriter) = QuerySqlBuilder.buildWhere(predicate)
    val allParams = joinOnParams ++ whereParams
    val combinedWriter: FragWriter = (ps, pos) =>
      val afterJoin = joinOnWriter.write(ps, pos)
      whereWriter.write(ps, afterJoin)
    (fromJoinSql + whereSql, allParams, combinedWriter)
  end buildFromJoinWhere

  def build: Frag =
    val selectParts = metas.zipWithIndex.map { (meta, idx) =>
      meta.columns.map(c => s"t$idx.${c.sqlName}").mkString(", ")
    }
    val selectSql = s"SELECT ${selectParts.mkString(", ")} "

    val (fromJoinWhereSql, params, writer) = buildFromJoinWhere

    val (orderBySql, _, _) = QuerySqlBuilder.buildOrderBy(orderEntries)
    val limitOffsetSql = QuerySqlBuilder.buildLimitOffset(limitOpt, offsetOpt)

    Frag(selectSql + fromJoinWhereSql + orderBySql + limitOffsetSql, params, writer)

  def run()(using DbCon): Vector[R] =
    given codec: DbCodec[R] = resultCodec
    build.query[R].run()

  def first()(using DbCon): Option[R] =
    limit(1).run().headOption

  def firstOrFail()(using DbCon): R =
    first().getOrElse(
      throw QueryBuilderException(
        s"No result found matching joined query"
      )
    )

  def count()(using DbCon): Long =
    val (fromJoinWhereSql, params, writer) = buildFromJoinWhere
    Frag(s"SELECT COUNT(*) $fromJoinWhereSql", params, writer)
      .query[Long]
      .run()
      .head

  def exists()(using DbCon): Boolean =
    val (fromJoinWhereSql, params, writer) = buildFromJoinWhere
    Frag(s"SELECT EXISTS(SELECT 1 $fromJoinWhereSql)", params, writer)
      .query[Boolean]
      .run()
      .head

  def select: JoinedQuery.JoinedSelectPhase[R] =
    JoinedQuery.JoinedSelectPhase(this)

  def debugPrintSql(using DbCon): this.type =
    DebugSql.printDebug(Vector(build))
    this

end JoinedQuery

object JoinedQuery:

  class JoinedSelectPhase[R <: NonEmptyTuple](val jq: JoinedQuery[R]):
    transparent inline def apply[P](inline f: JoinedQuery[R] => P): Any =
      ${ joinedSelectImpl[R, P]('this, 'f) }

  extension [R <: NonEmptyTuple](jq: JoinedQuery[R])
    transparent inline def of[T]: Any = ${ ofImpl[R, T] }
    transparent inline def ofLeft[T]: Any = ${ ofLeftImpl[R, T] }

  private[magnum] def ofImpl[R: Type, T: Type](using Quotes): Expr[Any] =
    import quotes.reflect.*

    val indices = tupleIndicesOf[T](TypeRepr.of[R], 0)
    indices match
      case Nil =>
        report.errorAndAbort(
          s"Type ${TypeRepr.of[T].show} is not in the join tuple ${TypeRepr.of[R].show}"
        )
      case _ :: _ :: _ =>
        report.errorAndAbort(
          s"Type ${TypeRepr.of[T].show} appears multiple times in ${TypeRepr.of[R].show}; use col(index, col) instead"
        )
      case idx :: Nil =>
        val aliasExpr = Expr(s"t$idx")
        buildAliasedColumns[T](aliasExpr)

  private[magnum] def ofLeftImpl[R: Type, T: Type](using Quotes): Expr[Any] =
    import quotes.reflect.*

    val indices = tupleIndicesOfOption[T](TypeRepr.of[R], 0)
    indices match
      case Nil =>
        report.errorAndAbort(
          s"Type Option[${TypeRepr.of[T].show}] is not in the join tuple ${TypeRepr.of[R].show}"
        )
      case _ :: _ :: _ =>
        report.errorAndAbort(
          s"Type Option[${TypeRepr.of[T].show}] appears multiple times in ${TypeRepr.of[R].show}; use col(index, col) instead"
        )
      case idx :: Nil =>
        val aliasExpr = Expr(s"t$idx")
        buildAliasedColumns[T](aliasExpr)

  private def tupleIndicesOfOption[T: Type](using
      Quotes
  )(
      tpe: quotes.reflect.TypeRepr,
      offset: Int
  ): List[Int] =
    import quotes.reflect.*
    tpe.dealias match
      case AppliedType(tycon, args) if tycon.typeSymbol.name == "*:" =>
        val head = args(0)
        val tail = args(1)
        val here = if head =:= TypeRepr.of[Option[T]] then List(offset) else Nil
        here ++ tupleIndicesOfOption[T](tail, offset + 1)
      case AppliedType(tycon, args) =>
        args.zipWithIndex.collect {
          case (arg, idx) if arg =:= TypeRepr.of[Option[T]] => offset + idx
        }
      case _ => Nil

  private def buildAliasedColumns[E: Type](aliasExpr: Expr[String])(using Quotes): Expr[Any] =
    import quotes.reflect.*

    val metaExpr = Expr
      .summon[TableMeta[E]]
      .getOrElse(
        report.errorAndAbort(
          s"No TableMeta found for ${TypeRepr.of[E].show}"
        )
      )

    Expr.summon[Mirror.ProductOf[E]] match
      case Some('{
            $m: Mirror.ProductOf[E] {
              type MirroredElemLabels = mels
              type MirroredElemTypes = mets
            }
          }) =>
        val names = elemNames[mels]()
        val types = elemTypes[mets]()

        val refinement =
          names.zip(types).foldLeft(TypeRepr.of[Columns[E]]) { case (tr, (name, tpe)) =>
            tpe match
              case '[t] => Refinement(tr, name, TypeRepr.of[BoundCol[t]])
          }

        refinement.asType match
          case '[rt] =>
            '{ Columns.aliased[E]($metaExpr, $aliasExpr).asInstanceOf[rt] }

      case _ =>
        report.errorAndAbort(
          s"No Mirror.ProductOf for ${TypeRepr.of[E].show}"
        )
    end match
  end buildAliasedColumns

  private def tupleIndicesOf[T: Type](using
      Quotes
  )(
      tpe: quotes.reflect.TypeRepr,
      offset: Int
  ): List[Int] =
    import quotes.reflect.*
    tpe.dealias match
      case AppliedType(tycon, args) if tycon.typeSymbol.name == "*:" =>
        val head = args(0)
        val tail = args(1)
        val here = if head =:= TypeRepr.of[T] then List(offset) else Nil
        here ++ tupleIndicesOf[T](tail, offset + 1)
      case AppliedType(tycon, args) =>
        // TupleN form
        args.zipWithIndex.collect {
          case (arg, idx) if arg =:= TypeRepr.of[T] => offset + idx
        }
      case _ => Nil

  // --- joinedSelectImpl macro ---

  private def joinedSelectImpl[R <: NonEmptyTuple : Type, P: Type](
      phaseExpr: Expr[JoinedSelectPhase[R]],
      f: Expr[JoinedQuery[R] => P]
  )(using Quotes): Expr[Any] =
    import quotes.reflect.*

    // 1. Decompose P as NamedTuple[N, V]
    val ntSym = Symbol.requiredModule("scala.NamedTuple").typeMember("NamedTuple")
    val pRepr = TypeRepr.of[P].dealias

    val (namesRepr, valuesRepr) = pRepr match
      case AppliedType(tycon, List(n, v)) if tycon.typeSymbol == ntSym =>
        (n, v)
      case _ =>
        report.errorAndAbort(
          s"select() requires a named tuple return type, got: ${pRepr.show}"
        )

    // 2. Extract field names
    val fieldNames: List[String] = namesRepr.asType match
      case '[nmes] => QueryBuilder.selectFieldNames[nmes]()

    // 3. Analyze element types
    val elemInfos: List[(String, Type[?], Boolean)] = valuesRepr.asType match
      case '[vals] => QueryBuilder.selectAnalyzeElements[vals](fieldNames)

    // 4. Build result named tuple type
    val resultTypes: List[Type[?]] = elemInfos.map(_._2)
    val resultNtType = QueryBuilder.selectPackNamedTuple(fieldNames, resultTypes)

    // 5. Build PC named tuple type
    val pcTypes: List[Type[?]] = elemInfos.map { case (_, innerTpe, _) =>
      innerTpe match { case '[a] => Type.of[SelectExpr[a]] }
    }
    val pcNtType = QueryBuilder.selectPackNamedTuple(fieldNames, pcTypes)

    // 6. Generate codec expressions
    val codecExprs: List[Expr[DbCodec[?]]] = elemInfos.map { case (name, innerTpe, isSE) =>
      innerTpe match
        case '[a] =>
          if isSE then
            '{ null.asInstanceOf[DbCodec[?]] }
          else
            Expr.summon[DbCodec[a]] match
              case Some(c) => '{ $c.asInstanceOf[DbCodec[?]] }
              case None =>
                report.errorAndAbort(
                  s"No DbCodec found for type ${TypeRepr.of[a].show} (field '$name')"
                )
    }

    val fieldNameExprs: Expr[Seq[String]] = Expr.ofSeq(fieldNames.map(Expr.apply))
    val codecsSeqExpr: Expr[Seq[DbCodec[?]]] = Expr.ofSeq(codecExprs)
    val nExpr = Expr(elemInfos.length)

    // 7. Generate the final expression
    (resultNtType, pcNtType) match
      case ('[resultT], '[pcT]) =>
        '{
          val phase = $phaseExpr
          val jq = phase.jq
          val rawTuple = $f(jq).asInstanceOf[Product]
          val names = $fieldNameExprs
          val fallbackCodecs = $codecsSeqExpr
          val n = $nExpr

          val selectExprsArr = new Array[SelectExpr[?]](n)
          var i = 0
          while i < n do
            val elem = rawTuple.productElement(i)
            val alias = names(i)
            elem match
              case se: SelectExpr[?] =>
                selectExprsArr(i) = new SelectExpr(se.queryRepr, alias, se.codec)
              case col: ColRef[?] =>
                selectExprsArr(i) = new SelectExpr(col.queryRepr, alias, fallbackCodecs(i))
            i += 1

          val codecArr = IArray.from(selectExprsArr.map(_.codec))
          val resultCodec = ProjectedQuery.positionalCodec[resultT](codecArr)

          val pcArray = selectExprsArr.map(_.asInstanceOf[Object])
          val pc = Tuple.fromArray(pcArray).asInstanceOf[pcT]

          val fromClause = jq.buildFromJoin
          val fromParams = jq.joinOnParams
          val fromWriter = jq.joinOnWriter

          // Merge frozen WHERE predicate from JoinedQuery
          new ProjectedQuery[resultT, pcT](
            fromClause,
            fromParams,
            fromWriter,
            selectExprsArr.toVector,
            resultCodec,
            pc,
            jq.predicate,
            Vector.empty,
            None,
            Vector.empty,
            None,
            None
          )
        }
      case _ =>
        report.errorAndAbort("select() failed to construct result types. This is a bug in magnum.")
  end joinedSelectImpl

end JoinedQuery
