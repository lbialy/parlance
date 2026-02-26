package com.augustnagro.magnum

import java.time.{Instant, OffsetDateTime}
import scala.deriving.Mirror
import scala.quoted.*

/** Compile-time evidence that entity E has a field annotated with @createdAt. */
trait HasCreatedAt[E]:
  def column: Col[?]
  def index: Int

object HasCreatedAt:
  inline given derived[E](using Mirror.ProductOf[E]): HasCreatedAt[E] =
    ${ derivedImpl[E] }

  private def derivedImpl[E: Type](using Quotes): Expr[HasCreatedAt[E]] =
    import quotes.reflect.*

    val annotSym = TypeRepr.of[createdAt].typeSymbol
    val params = TypeRepr.of[E].typeSymbol.primaryConstructor.paramSymss.head

    val annotatedIndices = params.zipWithIndex.collect {
      case (sym, i) if sym.hasAnnotation(annotSym) => (sym, i)
    }
    if annotatedIndices.isEmpty then
      report.errorAndAbort(
        s"${TypeRepr.of[E].show} has no field annotated with @createdAt. " +
          "Add @createdAt to the timestamp field to derive HasCreatedAt."
      )
    if annotatedIndices.size > 1 then
      val fieldNames = annotatedIndices.map(_._1.name).mkString(", ")
      report.errorAndAbort(
        s"${TypeRepr.of[E].show} has multiple fields annotated with @createdAt ($fieldNames). " +
          "Only one @createdAt field is allowed."
      )

    val (annotatedParam, annotatedIndex) = annotatedIndices.head
    val fieldName = annotatedParam.name

    // Validate field type: must be Instant or OffsetDateTime
    val fieldType = annotatedParam.tree match
      case vd: ValDef => vd.tpt.tpe
      case _          => report.errorAndAbort(s"Cannot resolve type of field '$fieldName'")

    val isInstant = fieldType =:= TypeRepr.of[Instant]
    val isOffsetDateTime = fieldType =:= TypeRepr.of[OffsetDateTime]
    if !isInstant && !isOffsetDateTime then
      report.errorAndAbort(
        s"@createdAt field '$fieldName' must be Instant or OffsetDateTime, " +
          s"but found ${fieldType.show}"
      )

    // Resolve SQL column name
    val tableExpr: Expr[Table] =
      DerivingUtil.tableAnnot[E] match
        case Some(t) => t
        case None =>
          report.errorAndAbort(
            s"${TypeRepr.of[E].show} must have @Table annotation to derive HasCreatedAt"
          )

    val nameMapper: Expr[SqlNameMapper] = '{ $tableExpr.nameMapper }

    val sqlNameExpr: Expr[String] =
      metaSqlNameAnnot[E](fieldName) match
        case Some(sqlName) => '{ $sqlName.name }
        case None          => '{ $nameMapper.toColumnName(${ Expr(fieldName) }) }

    val fieldNameExpr = Expr(fieldName)
    val indexExpr = Expr(annotatedIndex)

    '{
      new HasCreatedAt[E]:
        val column: Col[Any] = new Col[Any]($fieldNameExpr, $sqlNameExpr)
        val index: Int = $indexExpr
    }
  end derivedImpl

  private def metaSqlNameAnnot[T: Type](elemName: String)(using
      Quotes
  ): Option[Expr[SqlName]] =
    import quotes.reflect.*
    val annot = TypeRepr.of[SqlName].typeSymbol
    TypeRepr
      .of[T]
      .typeSymbol
      .primaryConstructor
      .paramSymss
      .head
      .find(sym => sym.name == elemName && sym.hasAnnotation(annot))
      .flatMap(sym => sym.getAnnotation(annot))
      .map(term => term.asExprOf[SqlName])

end HasCreatedAt
