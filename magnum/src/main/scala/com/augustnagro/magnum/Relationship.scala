package com.augustnagro.magnum

import scala.deriving.Mirror
import scala.quoted.*

sealed trait Relationship[Source, Target]:
  def fk: Col[?]
  def pk: Col[?]

case class BelongsTo[S, T](fk: Col[?], pk: Col[?]) extends Relationship[S, T]
case class HasOne[S, T](fk: Col[?], pk: Col[?]) extends Relationship[S, T]
case class HasMany[S, T, +CT <: Selectable](fk: Col[?], pk: Col[?]) extends Relationship[S, T]

case class BelongsToMany[S, T, +CT <: Selectable](
    pivotTable: String,
    sourceFk: String,
    targetFk: String,
    sourcePk: Col[?],
    targetPk: Col[?]
)

case class HasManyThrough[S, T, +CT <: Selectable](
    intermediateTable: String,
    sourceFk: String,
    intermediatePk: Col[?],
    targetFk: Col[?],
    sourcePk: Col[?]
)

case class HasOneThrough[S, T, +CT <: Selectable](
    intermediateTable: String,
    sourceFk: String,
    intermediatePk: Col[?],
    targetFk: Col[?],
    sourcePk: Col[?]
)

case class ComposedRelationship[Root, Intermediate, Target, +CT <: Selectable](
    inner: Relationship[Root, Intermediate],
    outer: HasMany[Intermediate, Target, CT]
)

object Relationship:
  inline def belongsTo[S, T](
      inline fk: S => Any,
      inline pk: T => Any
  )(using TableMeta[S], TableMeta[T]): BelongsTo[S, T] =
    ${ belongsToImpl[S, T]('fk, 'pk) }

  inline def hasOne[S, T](
      inline fk: S => Any,
      inline pk: T => Any
  )(using TableMeta[S], TableMeta[T]): HasOne[S, T] =
    ${ hasOneImpl[S, T]('fk, 'pk) }

  transparent inline def hasMany[S, T](
      inline fk: S => Any,
      inline pk: T => Any
  )(using TableMeta[S], TableMeta[T]): Any =
    ${ hasManyImpl[S, T]('fk, 'pk) }

  transparent inline def belongsToMany[S, T](
      pivotTable: String,
      sourceFkColumn: String,
      targetFkColumn: String
  )(using TableMeta[S], TableMeta[T]): Any =
    ${ belongsToManyImpl[S, T]('pivotTable, 'sourceFkColumn, 'targetFkColumn) }

  transparent inline def belongsToMany[S, T]()(using
      TableMeta[S],
      TableMeta[T]
  ): Any =
    ${ belongsToManyConventionImpl[S, T] }

  transparent inline def hasManyThrough[S, I, T](
      inline intermediateFk: I => Any,
      inline targetFk: T => Any
  )(using TableMeta[S], TableMeta[I], TableMeta[T]): Any =
    ${ hasManyThroughImpl[S, I, T]('intermediateFk, 'targetFk) }

  transparent inline def hasOneThrough[S, I, T](
      inline intermediateFk: I => Any,
      inline targetFk: T => Any
  )(using TableMeta[S], TableMeta[I], TableMeta[T]): Any =
    ${ hasOneThroughImpl[S, I, T]('intermediateFk, 'targetFk) }

  // --- Macro implementations ---

  private def belongsToImpl[S: Type, T: Type](
      fk: Expr[S => Any],
      pk: Expr[T => Any]
  )(using Quotes): Expr[BelongsTo[S, T]] =
    val (fkExpr, pkExpr) = resolveColumns[S, T](fk, pk)
    '{ BelongsTo[S, T]($fkExpr, $pkExpr) }

  private def hasOneImpl[S: Type, T: Type](
      fk: Expr[S => Any],
      pk: Expr[T => Any]
  )(using Quotes): Expr[HasOne[S, T]] =
    val (fkExpr, pkExpr) = resolveColumns[S, T](fk, pk)
    '{ HasOne[S, T]($fkExpr, $pkExpr) }

  private def hasManyImpl[S: Type, T: Type](
      fk: Expr[S => Any],
      pk: Expr[T => Any]
  )(using Quotes): Expr[Any] =
    import quotes.reflect.*
    val (fkExpr, pkExpr) = resolveColumns[S, T](fk, pk)
    val ct = computeColumnsRefinement[T]()
    ct.asType match
      case '[ctType] =>
        '{ HasMany[S, T, ctType & Selectable]($fkExpr, $pkExpr) }

  private def belongsToManyImpl[S: Type, T: Type](
      pivotTable: Expr[String],
      sourceFkColumn: Expr[String],
      targetFkColumn: Expr[String]
  )(using Quotes): Expr[Any] =
    import quotes.reflect.*
    val metaS = Expr
      .summon[TableMeta[S]]
      .getOrElse(
        report.errorAndAbort(s"No TableMeta for ${TypeRepr.of[S].show}")
      )
    val metaT = Expr
      .summon[TableMeta[T]]
      .getOrElse(
        report.errorAndAbort(s"No TableMeta for ${TypeRepr.of[T].show}")
      )
    val ct = computeColumnsRefinement[T]()
    ct.asType match
      case '[ctType] =>
        '{ BelongsToMany[S, T, ctType & Selectable]($pivotTable, $sourceFkColumn, $targetFkColumn, $metaS.primaryKey, $metaT.primaryKey) }
  end belongsToManyImpl

  private def belongsToManyConventionImpl[S: Type, T: Type](using
      Quotes
  ): Expr[Any] =
    import quotes.reflect.*
    val metaS = Expr
      .summon[TableMeta[S]]
      .getOrElse(
        report.errorAndAbort(s"No TableMeta for ${TypeRepr.of[S].show}")
      )
    val metaT = Expr
      .summon[TableMeta[T]]
      .getOrElse(
        report.errorAndAbort(s"No TableMeta for ${TypeRepr.of[T].show}")
      )
    val nameMapperS = DerivingUtil.tableAnnot[S] match
      case Some(t) => '{ $t.nameMapper }
      case None =>
        report.errorAndAbort(
          s"${TypeRepr.of[S].show} must have @Table annotation for convention-based belongsToMany"
        )
    val nameMapperT = DerivingUtil.tableAnnot[T] match
      case Some(t) => '{ $t.nameMapper }
      case None =>
        report.errorAndAbort(
          s"${TypeRepr.of[T].show} must have @Table annotation for convention-based belongsToMany"
        )
    val sName = Expr(TypeRepr.of[S].typeSymbol.name)
    val tName = Expr(TypeRepr.of[T].typeSymbol.name)
    val ct = computeColumnsRefinement[T]()
    ct.asType match
      case '[ctType] =>
        '{
          val sSnake = $nameMapperS.toColumnName($sName)
          val tSnake = $nameMapperT.toColumnName($tName)
          BelongsToMany[S, T, ctType & Selectable](
            sSnake + "_" + tSnake,
            sSnake + "_id",
            tSnake + "_id",
            $metaS.primaryKey,
            $metaT.primaryKey
          )
        }
  end belongsToManyConventionImpl

  private def hasManyThroughImpl[S: Type, I: Type, T: Type](
      intermediateFk: Expr[I => Any],
      targetFk: Expr[T => Any]
  )(using Quotes): Expr[Any] =
    import quotes.reflect.*
    val metaS = Expr
      .summon[TableMeta[S]]
      .getOrElse(
        report.errorAndAbort(s"No TableMeta for ${TypeRepr.of[S].show}")
      )
    val metaI = Expr
      .summon[TableMeta[I]]
      .getOrElse(
        report.errorAndAbort(s"No TableMeta for ${TypeRepr.of[I].show}")
      )
    val metaT = Expr
      .summon[TableMeta[T]]
      .getOrElse(
        report.errorAndAbort(s"No TableMeta for ${TypeRepr.of[T].show}")
      )
    val iFkName = extractFieldName(intermediateFk.asTerm)
    val tFkName = extractFieldName(targetFk.asTerm)
    val iFkNameExpr = Expr(iFkName)
    val tFkNameExpr = Expr(tFkName)
    val ct = computeColumnsRefinement[T]()
    ct.asType match
      case '[ctType] =>
        '{
          val iFkCol = $metaI
            .columnByName($iFkNameExpr)
            .getOrElse(
              throw RuntimeException("Column " + $iFkNameExpr + " not found on intermediate")
            )
          val tFkCol = $metaT
            .columnByName($tFkNameExpr)
            .getOrElse(
              throw RuntimeException("Column " + $tFkNameExpr + " not found on target")
            )
          HasManyThrough[S, T, ctType & Selectable](
            $metaI.tableName,
            iFkCol.sqlName,
            $metaI.primaryKey,
            tFkCol,
            $metaS.primaryKey
          )
        }
    end match
  end hasManyThroughImpl

  private def hasOneThroughImpl[S: Type, I: Type, T: Type](
      intermediateFk: Expr[I => Any],
      targetFk: Expr[T => Any]
  )(using Quotes): Expr[Any] =
    import quotes.reflect.*
    val metaS = Expr
      .summon[TableMeta[S]]
      .getOrElse(
        report.errorAndAbort(s"No TableMeta for ${TypeRepr.of[S].show}")
      )
    val metaI = Expr
      .summon[TableMeta[I]]
      .getOrElse(
        report.errorAndAbort(s"No TableMeta for ${TypeRepr.of[I].show}")
      )
    val metaT = Expr
      .summon[TableMeta[T]]
      .getOrElse(
        report.errorAndAbort(s"No TableMeta for ${TypeRepr.of[T].show}")
      )
    val iFkName = extractFieldName(intermediateFk.asTerm)
    val tFkName = extractFieldName(targetFk.asTerm)
    val iFkNameExpr = Expr(iFkName)
    val tFkNameExpr = Expr(tFkName)
    val ct = computeColumnsRefinement[T]()
    ct.asType match
      case '[ctType] =>
        '{
          val iFkCol = $metaI
            .columnByName($iFkNameExpr)
            .getOrElse(
              throw RuntimeException("Column " + $iFkNameExpr + " not found on intermediate")
            )
          val tFkCol = $metaT
            .columnByName($tFkNameExpr)
            .getOrElse(
              throw RuntimeException("Column " + $tFkNameExpr + " not found on target")
            )
          HasOneThrough[S, T, ctType & Selectable](
            $metaI.tableName,
            iFkCol.sqlName,
            $metaI.primaryKey,
            tFkCol,
            $metaS.primaryKey
          )
        }
    end match
  end hasOneThroughImpl

  private def computeColumnsRefinement[T: Type]()(using Quotes): quotes.reflect.TypeRepr =
    import quotes.reflect.*
    Expr.summon[Mirror.ProductOf[T]] match
      case Some('{
            $mirror: Mirror.ProductOf[T] {
              type MirroredElemLabels = eMels
              type MirroredElemTypes = eMets
            }
          }) =>
        val names = elemNames[eMels]()
        val types = elemTypes[eMets]()
        names.zip(types).foldLeft(TypeRepr.of[Columns[T]]) { case (typeRepr, (name, tpe)) =>
          tpe match
            case '[t] =>
              Refinement(typeRepr, name, TypeRepr.of[Col[t]])
        }
      case _ =>
        report.errorAndAbort(
          s"A Mirror.ProductOf is required for ${TypeRepr.of[T].show}"
        )
  end computeColumnsRefinement

  private def resolveColumns[S: Type, T: Type](
      fk: Expr[S => Any],
      pk: Expr[T => Any]
  )(using Quotes): (Expr[Col[?]], Expr[Col[?]]) =
    import quotes.reflect.*
    val fkName = extractFieldName(fk.asTerm)
    val pkName = extractFieldName(pk.asTerm)
    val metaS = Expr
      .summon[TableMeta[S]]
      .getOrElse(
        report.errorAndAbort(s"No TableMeta for ${TypeRepr.of[S].show}")
      )
    val metaT = Expr
      .summon[TableMeta[T]]
      .getOrElse(
        report.errorAndAbort(s"No TableMeta for ${TypeRepr.of[T].show}")
      )
    val fkNameExpr = Expr(fkName)
    val pkNameExpr = Expr(pkName)
    val fkExpr = '{
      $metaS
        .columnByName($fkNameExpr)
        .getOrElse(
          throw RuntimeException("Column " + $fkNameExpr + " not found")
        )
    }
    val pkExpr = '{
      $metaT
        .columnByName($pkNameExpr)
        .getOrElse(
          throw RuntimeException("Column " + $pkNameExpr + " not found")
        )
    }
    (fkExpr, pkExpr)
  end resolveColumns

  private def extractFieldName(using
      Quotes
  )(
      term: quotes.reflect.Term
  ): String =
    import quotes.reflect.*
    term match
      case Inlined(_, _, inner) => extractFieldName(inner)
      case Block(List(DefDef(_, _, _, Some(body))), _) =>
        extractFieldBody(body)
      case _ =>
        report.errorAndAbort(
          s"Expected a field selector like _.fieldName, got: ${term.show}"
        )

  private def extractFieldBody(using
      Quotes
  )(
      term: quotes.reflect.Term
  ): String =
    import quotes.reflect.*
    term match
      case Select(_, name)      => name
      case Inlined(_, _, inner) => extractFieldBody(inner)
      case _ =>
        report.errorAndAbort(
          s"Expected a field selector like _.fieldName, got: ${term.show}"
        )
  extension [A, B, CT <: Selectable](outer: HasMany[A, B, CT])
    def via[Z](inner: BelongsTo[Z, A]): ComposedRelationship[Z, A, B, CT] =
      ComposedRelationship(inner, outer)
    def via[Z](inner: HasOne[Z, A]): ComposedRelationship[Z, A, B, CT] =
      ComposedRelationship(inner, outer)
    def via[Z, ICT <: Selectable](inner: HasMany[Z, A, ICT]): ComposedRelationship[Z, A, B, CT] =
      ComposedRelationship(inner, outer)

end Relationship
