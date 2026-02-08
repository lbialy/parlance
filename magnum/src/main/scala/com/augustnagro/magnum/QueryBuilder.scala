package com.augustnagro.magnum

import scala.deriving.Mirror
import scala.quoted.*

sealed trait QBState
sealed trait HasRoot extends QBState

class QueryBuilder[S <: QBState, E, C <: Selectable] private[magnum] (
    private val meta: TableMeta[E],
    private val codec: DbCodec[E],
    private val cols: C,
    private val rootPredicate: Option[Predicate],
    private val orderEntries: Vector[(ColRef[?], SortOrder)],
    private val limitOpt: Option[Int],
    private val offsetOpt: Option[Long]
):

  private def addAnd(pred: Predicate): Option[Predicate] =
    Some(rootPredicate match
      case None                          => pred
      case Some(Predicate.And(children)) => Predicate.And(children :+ pred)
      case Some(other)                   => Predicate.And(Vector(other, pred)))

  private def addOr(pred: Predicate): Option[Predicate] =
    Some(rootPredicate match
      case None                         => pred
      case Some(Predicate.Or(children)) => Predicate.Or(children :+ pred)
      case Some(other)                  => Predicate.Or(Vector(other, pred)))

  def where(frag: Frag): QueryBuilder[HasRoot, E, C] =
    new QueryBuilder(meta, codec, cols, addAnd(Predicate.Leaf(frag)), orderEntries, limitOpt, offsetOpt)

  def where(f: C => Frag): QueryBuilder[HasRoot, E, C] =
    new QueryBuilder(meta, codec, cols, addAnd(Predicate.Leaf(f(cols))), orderEntries, limitOpt, offsetOpt)

  def orWhere(frag: Frag): QueryBuilder[HasRoot, E, C] =
    new QueryBuilder(meta, codec, cols, addOr(Predicate.Leaf(frag)), orderEntries, limitOpt, offsetOpt)

  def orWhere(f: C => Frag): QueryBuilder[HasRoot, E, C] =
    new QueryBuilder(meta, codec, cols, addOr(Predicate.Leaf(f(cols))), orderEntries, limitOpt, offsetOpt)

  def whereGroup(
      f: PredicateGroupBuilder[C] => PredicateGroupBuilder[C]
  ): QueryBuilder[HasRoot, E, C] =
    new QueryBuilder(meta, codec, cols, addAnd(f(PredicateGroupBuilder.empty(cols)).build), orderEntries, limitOpt, offsetOpt)

  def orWhereGroup(
      f: PredicateGroupBuilder[C] => PredicateGroupBuilder[C]
  ): QueryBuilder[HasRoot, E, C] =
    new QueryBuilder(meta, codec, cols, addOr(f(PredicateGroupBuilder.empty(cols)).build), orderEntries, limitOpt, offsetOpt)

  def orderBy(f: C => ColRef[?], order: SortOrder = SortOrder.Asc): QueryBuilder[S, E, C] =
    new QueryBuilder(meta, codec, cols, rootPredicate, orderEntries :+ (f(cols), order), limitOpt, offsetOpt)

  def orderBy(f: C => ColRef[?]): QueryBuilder[S, E, C] =
    new QueryBuilder(meta, codec, cols, rootPredicate, orderEntries :+ (f(cols), SortOrder.Asc), limitOpt, offsetOpt)

  def limit(n: Int): QueryBuilder[S, E, C] =
    new QueryBuilder(meta, codec, cols, rootPredicate, orderEntries, Some(n), offsetOpt)

  def offset(n: Long): QueryBuilder[S, E, C] =
    new QueryBuilder(meta, codec, cols, rootPredicate, orderEntries, limitOpt, Some(n))

  private def buildWhere: (String, Seq[Any], FragWriter) =
    rootPredicate match
      case None => ("", Seq.empty, FragWriter.empty)
      case Some(pred) =>
        val frag = pred.toFrag
        if frag.sqlString.isEmpty then ("", Seq.empty, FragWriter.empty)
        else (" WHERE " + frag.sqlString, frag.params, frag.writer)

  def build: Frag =
    val selectCols = meta.columns.map(_.sqlName).mkString(", ")
    val baseSql = s"SELECT $selectCols FROM ${meta.tableName}"

    val (whereSql, params, writer) = buildWhere

    val orderBySql =
      if orderEntries.isEmpty then ""
      else
        val entries = orderEntries.map((col, ord) => s"${col.queryRepr} ${ord.queryRepr}")
        " ORDER BY " + entries.mkString(", ")

    val limitSql = limitOpt.fold("")(n => s" LIMIT $n")
    val offsetSql = offsetOpt.fold("")(n => s" OFFSET $n")

    Frag(baseSql + whereSql + orderBySql + limitSql + offsetSql, params, writer)

  def run()(using DbCon): Vector[E] =
    build.query[E](using codec).run()

  def first()(using DbCon): Option[E] =
    limit(1).run().headOption

  def firstOrFail()(using DbCon): E =
    first().getOrElse(
      throw QueryBuilderException(
        s"No ${meta.tableName} found matching query"
      )
    )

  def count()(using DbCon): Long =
    val baseSql = s"SELECT COUNT(*) FROM ${meta.tableName}"
    val (whereSql, params, writer) = buildWhere
    Frag(baseSql + whereSql, params, writer)
      .query[Long]
      .run()
      .head

  def exists()(using DbCon): Boolean =
    val innerSql = s"SELECT 1 FROM ${meta.tableName}"
    val (whereSql, params, writer) = buildWhere
    Frag(s"SELECT EXISTS($innerSql$whereSql)", params, writer)
      .query[Boolean]
      .run()
      .head

  def withRelated[T](rel: HasMany[E, T, ?])(using
      childMeta: TableMeta[T],
      childCodec: DbCodec[T]
  ): EagerQuery[E, T] =
    EagerQuery(build, codec, meta, rel, childMeta, childCodec)

  def withRelated[T](rel: BelongsToMany[E, T, ?])(using
      targetMeta: TableMeta[T],
      targetCodec: DbCodec[T]
  ): PivotEagerQuery[E, T] =
    PivotEagerQuery(build, codec, meta, rel, targetMeta, targetCodec)

  def join[T](rel: Relationship[E, T])(using
      joinedMeta: TableMeta[T],
      joinedCodec: DbCodec[T]
  ): JoinedQuery[(E, T)] =
    val entry = JoinEntry(
      TableRef(joinedMeta.tableName, "t1", joinedMeta.tableName),
      JoinType.Inner,
      Frag(s"t0.${rel.fk.sqlName} = t1.${rel.pk.sqlName}", Seq.empty, FragWriter.empty)
    )
    new JoinedQuery[(E, T)](
      Vector(meta, joinedMeta),
      Vector(codec, joinedCodec),
      Vector(entry),
      rootPredicate,
      orderEntries,
      limitOpt,
      offsetOpt
    )

  // --- whereHas / doesntHave for Relationship ---

  def whereHas[T](rel: Relationship[E, T])(using
      relMeta: TableMeta[T]
  ): QueryBuilder[HasRoot, E, C] =
    where(buildRelExistsFrag(rel, relMeta, None, negate = false))

  def whereHas[T](rel: HasMany[E, T, ?])(using
      relMeta: TableMeta[T]
  ): QueryBuilder[HasRoot, E, C] =
    where(buildRelExistsFrag(rel, relMeta, None, negate = false))

  def whereHas[T, CT <: Selectable](rel: HasMany[E, T, CT])(f: CT => Frag)(using
      relMeta: TableMeta[T]
  ): QueryBuilder[HasRoot, E, C] =
    val cols = new Columns[T](relMeta.columns).asInstanceOf[CT]
    where(buildRelExistsFrag(rel, relMeta, Some(f(cols)), negate = false))

  def doesntHave[T](rel: HasMany[E, T, ?])(using
      relMeta: TableMeta[T]
  ): QueryBuilder[HasRoot, E, C] =
    where(buildRelExistsFrag(rel, relMeta, None, negate = true))

  def doesntHave[T](rel: Relationship[E, T])(using
      relMeta: TableMeta[T]
  ): QueryBuilder[HasRoot, E, C] =
    where(buildRelExistsFrag(rel, relMeta, None, negate = true))

  // --- whereHas / doesntHave for BelongsToMany ---

  def whereHas[T](rel: BelongsToMany[E, T, ?]): QueryBuilder[HasRoot, E, C] =
    where(buildPivotExistsFrag(rel, None, negate = false))

  def whereHas[T, CT <: Selectable](rel: BelongsToMany[E, T, CT])(f: CT => Frag)(using
      relMeta: TableMeta[T]
  ): QueryBuilder[HasRoot, E, C] =
    val cols = new Columns[T](relMeta.columns).asInstanceOf[CT]
    where(buildPivotExistsFrag(rel, Some((f(cols), relMeta)), negate = false))

  def doesntHave[T](rel: BelongsToMany[E, T, ?]): QueryBuilder[HasRoot, E, C] =
    where(buildPivotExistsFrag(rel, None, negate = true))

  // --- Private helpers ---

  private def buildRelExistsFrag[T](
      rel: Relationship[E, T],
      relMeta: TableMeta[T],
      condition: Option[Frag],
      negate: Boolean
  ): Frag =
    val prefix = if negate then "NOT EXISTS" else "EXISTS"
    val correlation =
      s"${relMeta.tableName}.${rel.pk.sqlName} = ${meta.tableName}.${rel.fk.sqlName}"
    condition match
      case None =>
        Frag(
          s"$prefix (SELECT 1 FROM ${relMeta.tableName} WHERE $correlation)",
          Seq.empty,
          FragWriter.empty
        )
      case Some(cond) =>
        Frag(
          s"$prefix (SELECT 1 FROM ${relMeta.tableName} WHERE $correlation AND ${cond.sqlString})",
          cond.params,
          cond.writer
        )
  end buildRelExistsFrag

  private def buildPivotExistsFrag[T](
      rel: BelongsToMany[E, T, ?],
      conditionWithMeta: Option[(Frag, TableMeta[T])],
      negate: Boolean
  ): Frag =
    val prefix = if negate then "NOT EXISTS" else "EXISTS"
    val correlation =
      s"${rel.pivotTable}.${rel.sourceFk} = ${meta.tableName}.${rel.sourcePk.sqlName}"
    conditionWithMeta match
      case None =>
        Frag(
          s"$prefix (SELECT 1 FROM ${rel.pivotTable} WHERE $correlation)",
          Seq.empty,
          FragWriter.empty
        )
      case Some((cond, relMeta)) =>
        val joinClause =
          s"${rel.pivotTable}.${rel.targetFk} = ${relMeta.tableName}.${rel.targetPk.sqlName}"
        Frag(
          s"$prefix (SELECT 1 FROM ${rel.pivotTable} JOIN ${relMeta.tableName} ON $joinClause WHERE $correlation AND ${cond.sqlString})",
          cond.params,
          cond.writer
        )
  end buildPivotExistsFrag

  def debugPrintSql(using DbCon): this.type =
    val frag = build
    DebugSql.printDebug(Vector(frag))
    this
end QueryBuilder

object QueryBuilder:
  private[magnum] def build0[E, C <: Selectable](
      meta: TableMeta[E],
      codec: DbCodec[E],
      cols: C
  ): QueryBuilder[HasRoot, E, C] =
    new QueryBuilder(meta, codec, cols, None, Vector.empty, None, None)

  transparent inline def from[E](using
      inline meta: TableMeta[E],
      codec: DbCodec[E]
  ): Any = ${ fromImpl[E]('meta, 'codec) }

  private def fromImpl[E: Type](
      meta: Expr[TableMeta[E]],
      codec: Expr[DbCodec[E]]
  )(using Quotes): Expr[Any] =
    import quotes.reflect.*

    Expr.summon[Mirror.ProductOf[E]] match
      case Some('{
            $mirror: Mirror.ProductOf[E] {
              type MirroredElemLabels = eMels
              type MirroredElemTypes = eMets
            }
          }) =>
        val elemNames = metaElemNames[eMels]()
        val elemTypes = metaElemTypes[eMets]()

        val colsRefinement =
          elemNames.zip(elemTypes).foldLeft(TypeRepr.of[Columns[E]]) { case (typeRepr, (name, tpe)) =>
            tpe match
              case '[t] =>
                Refinement(typeRepr, name, TypeRepr.of[Col[t]])
          }

        colsRefinement.asType match
          case '[ct] =>
            '{
              val cols = new Columns[E]($meta.columns).asInstanceOf[ct & Selectable]
              build0[E, ct & Selectable]($meta, $codec, cols)
            }

      case _ =>
        report.errorAndAbort(
          s"A Mirror.ProductOf is required for QueryBuilder.from[${TypeRepr.of[E].show}]"
        )
    end match
  end fromImpl

  private def metaElemNames[Mels: Type](res: List[String] = Nil)(using
      Quotes
  ): List[String] =
    import quotes.reflect.*
    Type.of[Mels] match
      case '[mel *: melTail] =>
        val melString = Type.valueOfConstant[mel].get.toString
        metaElemNames[melTail](melString :: res)
      case '[EmptyTuple] =>
        res.reverse

  private def metaElemTypes[Mets: Type](res: List[Type[?]] = Nil)(using
      Quotes
  ): List[Type[?]] =
    import quotes.reflect.*
    Type.of[Mets] match
      case '[met *: metTail] =>
        metaElemTypes[metTail](Type.of[met] :: res)
      case '[EmptyTuple] =>
        res.reverse
end QueryBuilder
