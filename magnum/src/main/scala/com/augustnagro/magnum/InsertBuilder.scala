package com.augustnagro.magnum

import java.sql.Statement
import scala.util.{Failure, Success, Using}

class InsertBuilder[EC, E] @scala.annotation.publicInBinary private[magnum] (
    private[magnum] val tableName: String,
    private[magnum] val ecColNames: IArray[String],
    private[magnum] val ecCodec: DbCodec[EC],
    private[magnum] val eCodec: DbCodec[E],
    private[magnum] val eColNames: IArray[String],
    private[magnum] val eElemCodecs: IArray[DbCodec[?]],
    private[magnum] val pkColName: String,
    private[magnum] val idIndex: Int
):

  private val ecInsertKeys: String = ecColNames.mkString("(", ", ", ")")
  private val insertSql: String =
    s"INSERT INTO $tableName $ecInsertKeys VALUES (${ecCodec.queryRepr})"
  private val insertGenKeys: Array[String] = Array.from(eColNames)

  def insert(ec: EC)(using con: DbCon[?]): Unit =
    handleQuery(insertSql, ec):
      Using(con.connection.prepareStatement(insertSql)): ps =>
        ecCodec.writeSingle(ec, ps)
        timed(ps.executeUpdate())

  def insertAll(ecs: Iterable[EC])(using con: DbCon[?]): Unit =
    handleQuery(insertSql, ecs):
      Using(con.connection.prepareStatement(insertSql)): ps =>
        ecCodec.write(ecs, ps)
        timed(batchUpdateResult(ps.executeBatch()))

  def insertReturning[D <: DatabaseType](ec: EC)(using con: DbCon[D], cr: CanReturn[EC, E, D]): E =
    if !con.databaseType.supportsInsertReturning then
      insert(ec)
      ec.asInstanceOf[E]
    else
      handleQuery(insertSql, ec):
        Using.Manager: use =>
          val ps = use(con.connection.prepareStatement(insertSql, insertGenKeys))
          ecCodec.writeSingle(ec, ps)
          timed:
            ps.executeUpdate()
            val rs = use(ps.getGeneratedKeys)
            rs.next()
            eCodec.readSingle(rs)

  def insertAllReturning[D <: DatabaseType](ecs: Iterable[EC])(using con: DbCon[D], cr: CanReturn[EC, E, D]): Vector[E] =
    if !con.databaseType.supportsInsertReturning then
      insertAll(ecs)
      ecs.toVector.asInstanceOf[Vector[E]]
    else
      handleQuery(insertSql, ecs):
        Using.Manager: use =>
          val ps = use(con.connection.prepareStatement(insertSql, insertGenKeys))
          ecCodec.write(ecs, ps)
          timed:
            batchUpdateResult(ps.executeBatch())
            val rs = use(ps.getGeneratedKeys)
            eCodec.read(rs)

  def insertOnConflict(ec: EC, target: ConflictTarget, action: ConflictAction)(using con: DbCon[? <: SupportsMutations]): Unit =
    action match
      case ConflictAction.DoNothing =>
        val keyColNames = target match
          case ConflictTarget.Columns(cols*) if cols.nonEmpty => cols.map(_.sqlName).toSeq
          case _                                              => Seq(pkColName)
        val whereClause = keyColNames.map(c => s"$c = ?").mkString(" AND ")
        val ecCols = ecColNames.mkString(", ")
        val sql =
          s"INSERT INTO $tableName ($ecCols) SELECT ${ecCodec.queryRepr} WHERE NOT EXISTS (SELECT 1 FROM $tableName WHERE $whereClause)"
        handleQuery(sql, ec):
          Using(con.connection.prepareStatement(sql)): ps =>
            ecCodec.writeSingle(ec, ps)
            val product = ec.asInstanceOf[Product]
            var pos = 1 + ecCodec.cols.length
            for keyCol <- keyColNames do
              val idx = ecColNames.indexOf(keyCol)
              if idx >= 0 then
                val codec = eElemCodecs(idx).asInstanceOf[DbCodec[Any]]
                codec.writeSingle(product.productElement(idx), ps, pos)
                pos += codec.cols.length
            timed(ps.executeUpdate())
      case ConflictAction.DoUpdate(_) =>
        // Use MERGE/upsert — updates all columns on conflict
        val keyColumns = target match
          case ConflictTarget.Columns(cols*) if cols.nonEmpty => cols.map(_.sqlName).mkString(", ")
          case _                                              => pkColName
        val sql = con.databaseType.renderUpsertByPk(tableName, IArray.from(ecColNames), ecCodec.queryRepr, keyColumns)
        handleQuery(sql, ec):
          Using(con.connection.prepareStatement(sql)): ps =>
            ecCodec.writeSingle(ec, ps)
            timed(ps.executeUpdate())

  def insertOnConflictUpdateAll(ec: EC, target: ConflictTarget)(using con: DbCon[? <: SupportsMutations]): Unit =
    val keyColumns = target match
      case ConflictTarget.Columns(cols*) if cols.nonEmpty => cols.map(_.sqlName).mkString(", ")
      case _                                              => pkColName
    val sql = con.databaseType.renderUpsertByPk(tableName, IArray.from(ecColNames), ecCodec.queryRepr, keyColumns)
    handleQuery(sql, ec):
      Using(con.connection.prepareStatement(sql)): ps =>
        ecCodec.writeSingle(ec, ps)
        timed(ps.executeUpdate())

  def insertAllIgnoring(ecs: Iterable[EC])(using con: DbCon[? <: SupportsMutations]): Int =
    val ecCols = ecColNames.mkString(", ")
    val sql =
      s"INSERT INTO $tableName ($ecCols) SELECT ${ecCodec.queryRepr} WHERE NOT EXISTS (SELECT 1 FROM $tableName WHERE $pkColName = ?)"
    handleQuery(sql, ecs):
      Using(con.connection.prepareStatement(sql)): ps =>
        timed:
          var count = 0
          for ec <- ecs do
            ecCodec.writeSingle(ec, ps)
            val product = ec.asInstanceOf[Product]
            val pkCodec = eElemCodecs(idIndex).asInstanceOf[DbCodec[Any]]
            pkCodec.writeSingle(product.productElement(idIndex), ps, 1 + ecCodec.cols.length)
            count += ps.executeUpdate()
          count

end InsertBuilder

object InsertBuilder:
  import scala.deriving.Mirror

  inline given derived[EC, E](using
      inline meta: EntityMeta[E],
      inline ecMirror: Mirror.ProductOf[EC],
      ecCodec: DbCodec[EC]
  ): InsertBuilder[EC, E] = QueryBuilder.into[EC, E]
