package com.augustnagro.magnum

import scala.deriving.Mirror
import scala.quoted.*

/** Compile-time table metadata derived from @Table-annotated case classes.
  *
  * Provides typed column list, table name, and primary key for use by the query
  * builder.
  */
trait TableMeta[E]:
  def tableName: String
  def columns: IArray[Col[?]]
  def primaryKey: Col[?]
  def columnByName(scalaName: String): Option[Col[?]] =
    columns.find(_.scalaName == scalaName)

object TableMeta:
  inline given derived[E](using Mirror.ProductOf[E]): TableMeta[E] =
    ${ derivedImpl[E] }

  private def derivedImpl[E: Type](using Quotes): Expr[TableMeta[E]] =
    import quotes.reflect.*

    val tableExpr: Expr[Table] =
      DerivingUtil.tableAnnot[E] match
        case Some(t) => t
        case None =>
          report.errorAndAbort(
            s"${TypeRepr.of[E].show} must have @Table annotation to derive TableMeta"
          )

    val nameMapper: Expr[SqlNameMapper] = '{ $tableExpr.nameMapper }

    Expr.summon[Mirror.ProductOf[E]] match
      case Some('{
            $mirror: Mirror.ProductOf[E] {
              type MirroredLabel = eLabel
              type MirroredElemLabels = eMels
            }
          }) =>
        val tableNameScala = Type.valueOfConstant[eLabel].get.toString
        val tableNameScalaExpr = Expr(tableNameScala)
        val tableNameSql: Expr[String] =
          DerivingUtil.sqlTableNameAnnot[E] match
            case Some(sqlName) => '{ $sqlName.name }
            case None => '{ $nameMapper.toTableName($tableNameScalaExpr) }

        val eElemNames: List[String] = elemNames[eMels]()
        val eElemNamesSql: List[Expr[String]] = eElemNames.map(elemName =>
          metaSqlNameAnnot[E](elemName) match
            case Some(sqlName) => '{ $sqlName.name }
            case None =>
              '{ $nameMapper.toColumnName(${ Expr(elemName) }) }
        )

        val idIndex: Int = metaIdAnnotIndex[E]

        val colExprs: List[Expr[Col[?]]] =
          eElemNames.lazyZip(eElemNamesSql).map { (scalaName, sqlNameExpr) =>
            '{
              new Col[Any](
                ${ Expr(scalaName) },
                $sqlNameExpr
              )
            }
          }

        val colsExpr = Expr.ofSeq(colExprs)
        val idIndexExpr = Expr(idIndex)

        '{
          val cols = IArray.from($colsExpr)
          val pk = cols($idIndexExpr)
          new TableMeta[E]:
            def tableName: String = $tableNameSql
            def columns: IArray[Col[?]] = cols
            def primaryKey: Col[?] = pk
        }

      case _ =>
        report.errorAndAbort(
          s"A Mirror.ProductOf is required to derive TableMeta for ${TypeRepr.of[E].show}"
        )
    end match
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

  private def metaIdAnnotIndex[E: Type](using q: Quotes): Int =
    import q.reflect.*
    val idAnnot = TypeRepr.of[Id].typeSymbol
    TypeRepr
      .of[E]
      .typeSymbol
      .primaryConstructor
      .paramSymss
      .head
      .indexWhere(sym => sym.hasAnnotation(idAnnot)) match
      case -1 => 0
      case x  => x

end TableMeta
