package com.augustnagro.magnum

import scala.quoted.*

sealed trait Relationship[Source, Target]:
  def fk: Col[?]
  def pk: Col[?]

case class BelongsTo[S, T](fk: Col[?], pk: Col[?]) extends Relationship[S, T]
case class HasOne[S, T](fk: Col[?], pk: Col[?]) extends Relationship[S, T]

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

  private def resolveColumns[S: Type, T: Type](
      fk: Expr[S => Any],
      pk: Expr[T => Any]
  )(using Quotes): (Expr[Col[?]], Expr[Col[?]]) =
    import quotes.reflect.*
    val fkName = extractFieldName(fk.asTerm)
    val pkName = extractFieldName(pk.asTerm)
    val metaS = Expr.summon[TableMeta[S]].getOrElse(
      report.errorAndAbort(s"No TableMeta for ${TypeRepr.of[S].show}")
    )
    val metaT = Expr.summon[TableMeta[T]].getOrElse(
      report.errorAndAbort(s"No TableMeta for ${TypeRepr.of[T].show}")
    )
    val fkNameExpr = Expr(fkName)
    val pkNameExpr = Expr(pkName)
    val fkExpr = '{
      $metaS.columnByName($fkNameExpr).getOrElse(
        throw RuntimeException("Column " + $fkNameExpr + " not found")
      )
    }
    val pkExpr = '{
      $metaT.columnByName($pkNameExpr).getOrElse(
        throw RuntimeException("Column " + $pkNameExpr + " not found")
      )
    }
    (fkExpr, pkExpr)

  private def extractFieldName(using Quotes)(
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

  private def extractFieldBody(using Quotes)(
      term: quotes.reflect.Term
  ): String =
    import quotes.reflect.*
    term match
      case Select(_, name)     => name
      case Inlined(_, _, inner) => extractFieldBody(inner)
      case _ =>
        report.errorAndAbort(
          s"Expected a field selector like _.fieldName, got: ${term.show}"
        )
