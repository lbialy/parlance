package com.augustnagro.magnum

import scala.deriving.Mirror
import scala.quoted.*

/** Typed column proxy for entity E. Created via `Columns.of[E]`.
  *
  * The returned value has a structural refinement type mapping each field of E
  * to `Col[FieldType]`. For example:
  *
  * {{{
  *   @Table(H2DbType, SqlNameMapper.CamelToSnakeCase)
  *   case class User(@Id id: Long, firstName: String, age: Int)
  *       derives DbCodec, TableMeta
  *
  *   val cols = Columns.of[User]
  *   val firstName: Col[String] = cols.firstName  // typed!
  *   val id: Col[Long] = cols.id
  * }}}
  */
final class Columns[E](private val cols: IArray[Col[?]])
    extends Selectable:
  def selectDynamic(name: String): Any =
    cols.find(_.scalaName == name).get

object Columns:
  transparent inline def of[E](using TableMeta[E]): Any =
    ${ ofImpl[E] }

  private def ofImpl[E: Type](using Quotes): Expr[Any] =
    import quotes.reflect.*

    val metaExpr = Expr.summon[TableMeta[E]].getOrElse(
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
        val elemNames = columnsElemNames[eMels]()
        val elemTypes = columnsElemTypes[eMets]()

        val refinement =
          elemNames.zip(elemTypes).foldLeft(TypeRepr.of[Columns[E]]) {
            case (typeRepr, (name, tpe)) =>
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

  private def columnsElemNames[Mels: Type](res: List[String] = Nil)(using
      Quotes
  ): List[String] =
    import quotes.reflect.*
    Type.of[Mels] match
      case '[mel *: melTail] =>
        val melString = Type.valueOfConstant[mel].get.toString
        columnsElemNames[melTail](melString :: res)
      case '[EmptyTuple] =>
        res.reverse

  private def columnsElemTypes[Mets: Type](res: List[Type[?]] = Nil)(using
      Quotes
  ): List[Type[?]] =
    import quotes.reflect.*
    Type.of[Mets] match
      case '[met *: metTail] =>
        columnsElemTypes[metTail](Type.of[met] :: res)
      case '[EmptyTuple] =>
        res.reverse

end Columns
