package com.augustnagro.magnum.migrate

import com.augustnagro.magnum.{DbCon, DatabaseType, EntityMeta}

import java.sql.{Connection, Types}
import scala.deriving.Mirror
import scala.quoted.*

inline def verify[E](using
    con: DbCon[?],
    em: EntityMeta[E]
)(using
    Mirror.ProductOf[E]
): VerifyResult =
  ${ Verify.verifyImpl[E]('con, 'em) }

object Verify:

  def verifyImpl[E: Type](con: Expr[DbCon[?]], em: Expr[EntityMeta[E]])(using
      Quotes
  ): Expr[VerifyResult] =
    import quotes.reflect.*

    val mirror = Expr
      .summon[Mirror.ProductOf[E]]
      .getOrElse(
        report.errorAndAbort(
          s"Cannot summon Mirror.ProductOf for ${TypeRepr.of[E].show}"
        )
      )

    mirror match
      case '{
            $m: Mirror.ProductOf[E] {
              type MirroredLabel = eLabel
              type MirroredElemTypes = mets
            }
          } =>
        val entityName = Expr(Type.valueOfConstant[eLabel].get.toString)
        val fieldMetas = buildFieldMetas[mets]()
        val fieldMetasExpr = Expr.ofSeq(fieldMetas)
        '{
          Verify.check(
            $em,
            $entityName,
            IArray.from($fieldMetasExpr),
            $con
          )
        }
  end verifyImpl

  private def buildFieldMetas[Mets: Type](
      res: Vector[Expr[FieldMeta]] = Vector.empty
  )(using Quotes): Vector[Expr[FieldMeta]] =
    import quotes.reflect.*
    Type.of[Mets] match
      case '[met *: metTail] =>
        val (typeName, isOption) = Type.of[met] match
          case '[Option[inner]] =>
            val name = innerTypeName[inner]
            (name, true)
          case _ =>
            val name = innerTypeName[met]
            (name, false)
        val typeNameExpr = Expr(typeName)
        val isOptionExpr = Expr(isOption)
        val fm = '{ FieldMeta($typeNameExpr, $isOptionExpr) }
        buildFieldMetas[metTail](res :+ fm)
      case '[EmptyTuple] =>
        res

  private def innerTypeName[T: Type](using Quotes): String =
    import quotes.reflect.*
    Type.of[T] match
      case '[Array[Byte]] => "Array[Byte]"
      case _ =>
        val repr = TypeRepr.of[T]
        repr.typeSymbol.name

  // --- Runtime check ---

  private case class DbColumn(
      name: String,
      dataType: String,
      isNullable: String,
      charMaxLen: Option[Long],
      numPrecision: Option[Int],
      numScale: Option[Int]
  )

  def check(
      em: EntityMeta[?],
      entityName: String,
      fieldMetas: IArray[FieldMeta],
      con: DbCon[?]
  ): VerifyResult =
    val conn = con.connection
    val schema = Option(conn.getSchema()).getOrElse("PUBLIC")
    val tableName = em.tableName

    if !tableExists(conn, schema, tableName) then
      return VerifyResult(
        tableName,
        entityName,
        List(VerifyIssue.TableMissing(tableName))
      )

    val dbColumns = loadColumns(conn, schema, tableName)
    val pkColumns = loadPrimaryKey(conn, schema, tableName)

    val issues = List.newBuilder[VerifyIssue]
    val details = List.newBuilder[VerifyColumnDetail]
    val matchedDbCols = scala.collection.mutable.Set[String]()

    val columns = em.columns
    val jdbcCols = em.cols

    // Track position into the flat jdbcCols array
    var jdbcPos = 0
    var fieldIdx = 0
    while fieldIdx < columns.length do
      val col = columns(fieldIdx)
      val fm = fieldMetas(fieldIdx)
      val jdbcType = jdbcCols(jdbcPos)
      jdbcPos += 1

      val dbColOpt =
        dbColumns.find(_.name.equalsIgnoreCase(col.sqlName))

      dbColOpt match
        case None =>
          val issue = VerifyIssue.ColumnMissing(col.sqlName, fm.scalaTypeName)
          issues += issue
          details += VerifyColumnDetail(
            col.sqlName,
            fm.scalaTypeName,
            "\u2014 not found in DB",
            Some(issue)
          )

        case Some(dbCol) =>
          matchedDbCols += dbCol.name.toLowerCase
          val dbInfo = formatDbInfo(dbCol, pkColumns)

          if !isTypeCompatible(jdbcType, dbCol.dataType) then
            val issue = VerifyIssue.TypeMismatch(
              col.sqlName,
              fm.scalaTypeName,
              dbCol.dataType
            )
            issues += issue
            details += VerifyColumnDetail(
              col.sqlName,
              fm.scalaTypeName,
              dbInfo,
              Some(issue)
            )
          else
            val dbIsNullable = dbCol.isNullable.equalsIgnoreCase("YES")
            if fm.isOption != dbIsNullable then
              val issue = VerifyIssue.NullabilityMismatch(
                col.sqlName,
                fm.isOption,
                dbIsNullable
              )
              issues += issue
              details += VerifyColumnDetail(
                col.sqlName,
                fm.scalaTypeName,
                dbInfo,
                Some(issue)
              )
            else
              details += VerifyColumnDetail(
                col.sqlName,
                fm.scalaTypeName,
                dbInfo,
                None
              )
            end if
          end if
      end match
      fieldIdx += 1
    end while

    // Extra columns in DB
    for dbCol <- dbColumns do if !matchedDbCols.contains(dbCol.name.toLowerCase) then issues += VerifyIssue.ExtraColumnInDb(dbCol.name)

    // Primary key check
    val expectedPk = List(em.primaryKey.sqlName)
    val actualPk = pkColumns.map(_.toLowerCase)
    if !expectedPk
        .map(_.toLowerCase)
        .zip(actualPk)
        .forall((e, a) => e == a) || expectedPk.size != actualPk.size
    then issues += VerifyIssue.PrimaryKeyMismatch(expectedPk, pkColumns)

    VerifyResult(tableName, entityName, issues.result(), details.result())
  end check

  private def tableExists(
      conn: Connection,
      schema: String,
      tableName: String
  ): Boolean =
    val sql =
      """SELECT 1 FROM information_schema.tables
        |WHERE LOWER(table_schema) = LOWER(?) AND LOWER(table_name) = LOWER(?)""".stripMargin
    val ps = conn.prepareStatement(sql)
    ps.setString(1, schema)
    ps.setString(2, tableName)
    val rs = ps.executeQuery()
    val exists = rs.next()
    rs.close()
    ps.close()
    exists

  private def loadColumns(
      conn: Connection,
      schema: String,
      tableName: String
  ): List[DbColumn] =
    val sql =
      """SELECT column_name, data_type, is_nullable,
        |       character_maximum_length, numeric_precision, numeric_scale
        |FROM information_schema.columns
        |WHERE LOWER(table_schema) = LOWER(?) AND LOWER(table_name) = LOWER(?)
        |ORDER BY ordinal_position""".stripMargin
    val ps = conn.prepareStatement(sql)
    ps.setString(1, schema)
    ps.setString(2, tableName)
    val rs = ps.executeQuery()
    val buf = List.newBuilder[DbColumn]
    while rs.next() do
      buf += DbColumn(
        name = rs.getString("column_name"),
        dataType = rs.getString("data_type"),
        isNullable = rs.getString("is_nullable"),
        charMaxLen = Option(rs.getLong("character_maximum_length"))
          .filterNot(_ => rs.wasNull()),
        numPrecision = Option(rs.getInt("numeric_precision"))
          .filterNot(_ => rs.wasNull()),
        numScale = Option(rs.getInt("numeric_scale")).filterNot(_ => rs.wasNull())
      )
    rs.close()
    ps.close()
    buf.result()
  end loadColumns

  private def loadPrimaryKey(
      conn: Connection,
      schema: String,
      tableName: String
  ): List[String] =
    val sql =
      """SELECT kcu.column_name
        |FROM information_schema.table_constraints tc
        |JOIN information_schema.key_column_usage kcu
        |  ON tc.constraint_name = kcu.constraint_name
        |  AND tc.table_schema = kcu.table_schema
        |WHERE LOWER(tc.table_schema) = LOWER(?) AND LOWER(tc.table_name) = LOWER(?)
        |  AND tc.constraint_type = 'PRIMARY KEY'
        |ORDER BY kcu.ordinal_position""".stripMargin
    val ps = conn.prepareStatement(sql)
    ps.setString(1, schema)
    ps.setString(2, tableName)
    val rs = ps.executeQuery()
    val buf = List.newBuilder[String]
    while rs.next() do buf += rs.getString("column_name")
    rs.close()
    ps.close()
    buf.result()
  end loadPrimaryKey

  private def formatDbInfo(
      dbCol: DbColumn,
      pkCols: List[String]
  ): String =
    val typeStr = dbCol.charMaxLen match
      case Some(len) => s"${dbCol.dataType.toUpperCase}($len)"
      case None      => dbCol.dataType.toUpperCase
    val isPk = pkCols.exists(_.equalsIgnoreCase(dbCol.name))
    val nullable =
      if dbCol.isNullable.equalsIgnoreCase("YES") then "nullable"
      else "not null"
    val parts = List.newBuilder[String]
    if isPk then parts += "pk"
    parts += nullable
    s"$typeStr (${parts.result().mkString(", ")})"

  private def isTypeCompatible(jdbcType: Int, dbDataType: String): Boolean =
    val dt = dbDataType.toLowerCase.trim
    jdbcType match
      case Types.BOOLEAN | Types.BIT =>
        dt == "boolean"
      case Types.SMALLINT | Types.TINYINT =>
        dt == "smallint" || dt == "tinyint"
      case Types.INTEGER =>
        dt == "integer" || dt == "int" || dt == "int4" || dt == "serial"
      case Types.BIGINT =>
        dt == "bigint" || dt == "int8" || dt == "bigserial"
      case Types.REAL | Types.FLOAT =>
        dt == "real" || dt == "float4"
      case Types.DOUBLE =>
        dt == "double precision" || dt == "double" || dt == "float8"
      case Types.NUMERIC | Types.DECIMAL =>
        dt == "numeric" || dt == "decimal"
      case Types.VARCHAR | Types.LONGVARCHAR | Types.NVARCHAR | Types.CLOB | Types.CHAR | Types.NCHAR | Types.LONGNVARCHAR | Types.NCLOB =>
        dt == "character varying" || dt == "varchar" || dt == "text" ||
        dt == "character" || dt == "char" ||
        dt == "character large object" || dt == "clob" || dt == "nvarchar" ||
        dt == "nchar" || dt == "nclob" || dt == "longvarchar"
      case Types.BINARY | Types.VARBINARY | Types.LONGVARBINARY | Types.BLOB =>
        dt == "binary" || dt == "bytea" || dt == "binary varying" ||
        dt == "binary large object" || dt == "blob" || dt == "varbinary"
      case Types.DATE =>
        dt == "date"
      case Types.TIME =>
        dt == "time" || dt == "time without time zone"
      case Types.TIMESTAMP =>
        dt == "timestamp" || dt == "timestamp without time zone"
      case Types.TIMESTAMP_WITH_TIMEZONE =>
        dt == "timestamp with time zone"
      case Types.OTHER | Types.JAVA_OBJECT =>
        // Permissive for UUIDs, JSON, custom types
        dt == "uuid" || dt == "user-defined" || dt == "other" ||
        dt == "json" || dt == "jsonb"
      case _ =>
        false
    end match
  end isTypeCompatible

end Verify
