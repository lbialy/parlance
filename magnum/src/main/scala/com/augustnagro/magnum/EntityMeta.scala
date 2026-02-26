package com.augustnagro.magnum

import java.sql.{PreparedStatement, ResultSet}
import scala.deriving.Mirror
import scala.quoted.*

/** Combined typeclass for entity types: table metadata + JDBC codec.
  *
  * Derive on @Table-annotated case classes instead of separate DbCodec + TableMeta:
  * {{{
  *   @Table(PostgresDbType, SqlNameMapper.CamelToSnakeCase)
  *   case class User(@Id id: Long, name: String) derives EntityMeta
  * }}}
  */
trait EntityMeta[E] extends TableMeta[E] with DbCodec[E]

object EntityMeta:
  inline given derived[E](using Mirror.ProductOf[E]): EntityMeta[E] =
    ${ derivedImpl[E] }

  private def derivedImpl[E: Type](using Quotes): Expr[EntityMeta[E]] =
    import quotes.reflect.*

    val tableExpr: Expr[Table] =
      DerivingUtil.tableAnnot[E] match
        case Some(t) => t
        case None =>
          report.errorAndAbort(
            s"${TypeRepr.of[E].show} must have @Table annotation to derive EntityMeta"
          )

    val nameMapper: Expr[SqlNameMapper] = '{ $tableExpr.nameMapper }

    Expr.summon[Mirror.ProductOf[E]] match
      case Some('{
            $mirror: Mirror.ProductOf[E] {
              type MirroredLabel = eLabel
              type MirroredElemLabels = eMels
              type MirroredElemTypes = mets
            }
          }) =>
        // --- TableMeta part ---
        val tableNameScala = Type.valueOfConstant[eLabel].get.toString
        val tableNameScalaExpr = Expr(tableNameScala)
        val tableNameSql: Expr[String] =
          DerivingUtil.sqlTableNameAnnot[E] match
            case Some(sqlName) => '{ $sqlName.name }
            case None          => '{ $nameMapper.toTableName($tableNameScalaExpr) }

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

        val metaColsExpr = Expr.ofSeq(colExprs)
        val idIndexExpr = Expr(idIndex)

        // --- DbCodec part (reuse DbCodec macro helpers) ---
        val jdbcColsExpr = DbCodec.buildColsExpr[mets]()
        val queryReprExpr = DbCodec.productQueryRepr[mets]()

        '{
          val metaCols = IArray.from($metaColsExpr)
          val pk = metaCols($idIndexExpr)
          new EntityMeta[E]:
            // TableMeta
            def tableName: String = $tableNameSql
            def columns: IArray[Col[?]] = metaCols
            def primaryKey: Col[?] = pk
            // DbCodec
            val cols: IArray[Int] = $jdbcColsExpr
            def readSingle(rs: ResultSet, pos: Int): E =
              ${
                DbCodec.productReadSingle[E, mets]('{ rs }, mirror, Vector.empty, '{ pos })
              }
            def readSingleOption(rs: ResultSet, pos: Int): Option[E] =
              ${
                DbCodec.productReadOption[E, mets]('{ rs }, mirror, Vector.empty, '{ pos })
              }
            def writeSingle(e: E, ps: PreparedStatement, pos: Int): Unit =
              ${
                DbCodec.productWriteSingle[E, mets]('{ e }, '{ ps }, '{ pos }, '{ 0 })
              }
            val queryRepr: String = $queryReprExpr
        }

      case _ =>
        report.errorAndAbort(
          s"A Mirror.ProductOf is required to derive EntityMeta for ${TypeRepr.of[E].show}"
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

end EntityMeta
