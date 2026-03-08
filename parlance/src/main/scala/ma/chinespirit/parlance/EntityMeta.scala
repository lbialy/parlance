package ma.chinespirit.parlance

import java.sql.{PreparedStatement, ResultSet}
import scala.deriving.Mirror
import scala.quoted.*

/** Combined typeclass for entity types: table metadata + JDBC codec.
  *
  * Derive on @Table-annotated case classes instead of separate DbCodec + TableMeta:
  * {{{
  *   @Table(SqlNameMapper.CamelToSnakeCase)
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

        val idIndices: List[Int] = metaIdAnnotIndices[E]

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
        val idIndicesExpr = Expr.ofSeq(idIndices.map(Expr(_)))

        // --- DbCodec part (reuse DbCodec macro helpers) ---
        val jdbcColsExpr = DbCodec.buildColsExpr[mets]()
        val queryReprExpr = DbCodec.productQueryRepr[mets]()

        // --- element codecs (per-field DbCodec instances) ---
        val elemCodecExprs: List[Expr[DbCodec[?]]] = summonElemCodecs[mets]()
        val elemCodecsSeqExpr = Expr.ofSeq(elemCodecExprs)

        '{
          val metaCols = IArray.from($metaColsExpr)
          val pkIndices = $idIndicesExpr
          val pks = IArray.from(pkIndices.map(metaCols(_)))
          val pk = pks(0)
          val elemCodecsArr = IArray.from($elemCodecsSeqExpr)
          new EntityMeta[E]:
            // TableMeta
            def tableName: String = $tableNameSql
            def columns: IArray[Col[?]] = metaCols
            def primaryKey: Col[?] = pk
            def primaryKeys: IArray[Col[?]] = pks
            def elementCodecs: IArray[DbCodec[?]] = elemCodecsArr
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
          end new
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

  private def metaIdAnnotIndices[E: Type](using q: Quotes): List[Int] =
    import q.reflect.*
    val idAnnot = TypeRepr.of[Id].typeSymbol
    val params = TypeRepr
      .of[E]
      .typeSymbol
      .primaryConstructor
      .paramSymss
      .head
    val indices = params.zipWithIndex.collect:
      case (sym, idx) if sym.hasAnnotation(idAnnot) => idx
    if indices.isEmpty then List(0) else indices

  private def summonElemCodecs[Mets: Type](
      res: List[Expr[DbCodec[?]]] = Nil
  )(using Quotes): List[Expr[DbCodec[?]]] =
    import quotes.reflect.*
    Type.of[Mets] match
      case '[met *: metTail] =>
        val codec = Expr.summon[DbCodec[met]].getOrElse {
          '{ DbCodec.AnyCodec.asInstanceOf[DbCodec[met]] }
        }
        summonElemCodecs[metTail](res :+ '{ $codec.asInstanceOf[DbCodec[?]] })
      case '[EmptyTuple] => res

end EntityMeta
