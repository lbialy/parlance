package com.augustnagro.magnum

import scala.deriving.Mirror
import scala.quoted.*

/** Typed column proxy for entity E. Created via `Columns.of[E]`.
  *
  * The returned value has a structural refinement type mapping each field of E to `Col[FieldType]`. For example:
  *
  * {{{
  *   @Table(SqlNameMapper.CamelToSnakeCase)
  *   case class User(@Id id: Long, firstName: String, age: Int)
  *       derives DbCodec, TableMeta
  *
  *   val cols = Columns.of[User]
  *   val firstName: Col[String] = cols.firstName  // typed!
  *   val id: Col[Long] = cols.id
  * }}}
  */
final class Columns[E](private val cols: IArray[ColRef[?]]) extends Selectable:
  def selectDynamic(name: String): Any =
    cols
      .find(_.scalaName == name)
      .getOrElse(
        throw QueryBuilderException(
          s"Column '$name' not found in columns: ${cols.map(_.scalaName).mkString(", ")}"
        )
      )

object Columns:
  transparent inline def of[E](using TableMeta[E]): Any =
    ${ ofImpl[E] }

  private[magnum] def aliased[E](meta: TableMeta[E], alias: String): Columns[E] =
    val bounded: IArray[ColRef[?]] = IArray.from(
      meta.columns.map(c => c.asInstanceOf[Col[Any]].bound(alias))
    )
    new Columns[E](bounded)

  private def ofImpl[E: Type](using Quotes): Expr[Any] =
    import quotes.reflect.*

    val metaExpr = Expr
      .summon[TableMeta[E]]
      .getOrElse(
        report.errorAndAbort(
          s"No TableMeta instance found for ${TypeRepr.of[E].show}. Does it derive TableMeta?"
        )
      )

    Expr.summon[Mirror.ProductOf[E]] match
      case Some('{
            $mirror: Mirror.ProductOf[E] {
              type MirroredElemLabels = eMels
              type MirroredElemTypes = eMets
            }
          }) =>
        val names = elemNames[eMels]()
        val types = elemTypes[eMets]()

        val refinement =
          names.zip(types).foldLeft(TypeRepr.of[Columns[E]]) { case (typeRepr, (name, tpe)) =>
            tpe match
              case '[t] =>
                Refinement(typeRepr, name, TypeRepr.of[Col[t]])
          }

        refinement.asType match
          case '[tpe] =>
            '{ new Columns[E]($metaExpr.columns).asInstanceOf[tpe] }

      case _ =>
        report.errorAndAbort(
          s"A Mirror.ProductOf is required for Columns.of[${TypeRepr.of[E].show}]"
        )
    end match
  end ofImpl

end Columns
