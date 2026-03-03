package com.augustnagro.magnum

import java.sql.{Connection, JDBCType, PreparedStatement, ResultSet, Statement}
import java.time.OffsetDateTime
import scala.collection.View
import scala.deriving.Mirror
import scala.reflect.ClassTag
import scala.util.{Failure, Success, Using}

object H2DbType extends DbType:

  def buildRepoDefaults[EC, E, ID](
      tableNameSql: String,
      eElemNames: Seq[String],
      eElemNamesSql: Seq[String],
      eElemCodecs: Seq[DbCodec[?]],
      ecElemNames: Seq[String],
      ecElemNamesSql: Seq[String],
      idIndex: Int
  )(using
      eCodec: DbCodec[E],
      ecCodec: DbCodec[EC],
      idCodec: DbCodec[ID],
      eClassTag: ClassTag[E],
      ecClassTag: ClassTag[EC],
      idClassTag: ClassTag[ID]
  ): RepoDefaults[EC, E, ID] =
    val idName = eElemNamesSql(idIndex)
    val selectKeys = eElemNamesSql.mkString(", ")
    val ecInsertKeys = ecElemNamesSql.mkString("(", ", ", ")")

    val updateKeys: String = eElemNamesSql
      .lazyZip(eElemCodecs)
      .map((sqlName, codec) => sqlName + " = " + codec.queryRepr)
      .patch(idIndex, Seq.empty, 1)
      .mkString(", ")

    val updateCodecs = eElemCodecs
      .patch(idIndex, Seq.empty, 1)
      .appended(idCodec)
      .asInstanceOf[Seq[DbCodec[Any]]]

    val insertGenKeys: Array[String] = Array.from(eElemNamesSql)

    val countSql = s"SELECT count(*) FROM $tableNameSql"
    val countQuery = Frag(countSql, Vector.empty, FragWriter.empty).query[Long]
    val existsByIdSql =
      s"SELECT 1 FROM $tableNameSql WHERE $idName = ${idCodec.queryRepr}"
    val findAllSql = s"SELECT * FROM $tableNameSql"
    val findAllQuery = Frag(findAllSql, Vector.empty, FragWriter.empty).query[E]
    val findByIdSql =
      s"SELECT * FROM $tableNameSql WHERE $idName = ${idCodec.queryRepr}"
    val findAllByIdSql = s"SELECT * FROM $tableNameSql WHERE $idName = ANY(?)"
    val deleteByIdSql =
      s"DELETE FROM $tableNameSql WHERE $idName = ${idCodec.queryRepr}"
    val truncateSql = s"TRUNCATE TABLE $tableNameSql"
    val truncateUpdate =
      Frag(truncateSql, Vector.empty, FragWriter.empty).update
    val insertSql =
      s"INSERT INTO $tableNameSql $ecInsertKeys VALUES (${ecCodec.queryRepr})"
    val updateSql =
      s"UPDATE $tableNameSql SET $updateKeys WHERE $idName = ${idCodec.queryRepr}"

    // upsert pre-computed SQL (H2 uses MERGE INTO syntax)
    val eInsertKeys = eElemNamesSql.mkString("(", ", ", ")")
    val upsertByPkSql =
      s"MERGE INTO $tableNameSql $eInsertKeys KEY ($idName) VALUES (${eCodec.queryRepr})"

    val compositeId = idCodec.cols.distinct.size != 1
    val idFirstTypeName = JDBCType.valueOf(idCodec.cols.head).getName

    def idWriter(id: ID): FragWriter = (ps, pos) =>
      idCodec.writeSingle(id, ps, pos)
      pos + idCodec.cols.length

    new RepoDefaults[EC, E, ID]:
      def count(using con: DbCon[?]): Long = countQuery.run().head

      def existsById(id: ID)(using DbCon[?]): Boolean =
        Frag(existsByIdSql, IArray(id), idWriter(id))
          .query[Int]
          .run()
          .nonEmpty

      def findAll(using DbCon[?]): Vector[E] = findAllQuery.run()

      def findAll(spec: Spec[E])(using DbCon[?]): Vector[E] =
        SpecImpl.Default.findAll(spec, tableNameSql)

      def findById(id: ID)(using DbCon[?]): Option[E] =
        Frag(findByIdSql, IArray(id), idWriter(id))
          .query[E]
          .run()
          .headOption

      def findAllById(ids: Iterable[ID])(using DbCon[?]): Vector[E] =
        if compositeId then
          throw UnsupportedOperationException(
            "Composite ids unsupported for findAllById."
          )
        val idsArray = Array.from[Any](ids)
        Frag(
          findAllByIdSql,
          IArray(idsArray),
          (ps, pos) =>
            val sqlArray =
              ps.getConnection.createArrayOf(idFirstTypeName, idsArray)
            ps.setArray(pos, sqlArray)
            pos + 1
        ).query[E].run()
//        // h2 doesn't support setObject(..) with primitive arrays,
//        // so we need to convert to Array[Object]
//        val builder = Array.newBuilder[Object]
//        if ids.knownSize > -1 then builder.sizeHint(ids.knownSize)
//        for id <- ids do builder += id.asInstanceOf[Object]
//        Sql(findAllByIdSql, Vector(builder.result())).run

      def delete(entity: E)(using DbCon[?]): Unit =
        deleteById(
          entity
            .asInstanceOf[Product]
            .productElement(idIndex)
            .asInstanceOf[ID]
        )

      def deleteById(id: ID)(using DbCon[?]): Unit =
        Frag(deleteByIdSql, IArray(id), idWriter(id)).update.run()

      def truncate()(using DbCon[?]): Unit = truncateUpdate.run()

      def deleteAll(entities: Iterable[E])(using DbCon[?]): BatchUpdateResult =
        deleteAllById(
          entities.map(e => e.asInstanceOf[Product].productElement(idIndex).asInstanceOf[ID])
        )

      def deleteAllById(ids: Iterable[ID])(using
          con: DbCon[?]
      ): BatchUpdateResult =
        handleQuery(deleteByIdSql, ids):
          Using(con.connection.prepareStatement(deleteByIdSql)): ps =>
            idCodec.write(ids, ps)
            timed(batchUpdateResult(ps.executeBatch()))

      def insert(entityCreator: EC)(using con: DbCon[?]): Unit =
        handleQuery(insertSql, entityCreator):
          Using(con.connection.prepareStatement(insertSql)): ps =>
            ecCodec.writeSingle(entityCreator, ps)
            timed(ps.executeUpdate())

      def insertAll(entityCreators: Iterable[EC])(using con: DbCon[?]): Unit =
        handleQuery(insertSql, entityCreators):
          Using(con.connection.prepareStatement(insertSql)): ps =>
            ecCodec.write(entityCreators, ps)
            timed(batchUpdateResult(ps.executeBatch()))

      def insertReturning(entityCreator: EC)(using con: DbCon[?]): E =
        handleQuery(insertSql, entityCreator):
          Using.Manager: use =>
            val ps =
              use(con.connection.prepareStatement(insertSql, insertGenKeys))
            ecCodec.writeSingle(entityCreator, ps)
            timed:
              ps.executeUpdate()
              val rs = use(ps.getGeneratedKeys)
              rs.next()
              eCodec.readSingle(rs)

      def insertAllReturning(
          entityCreators: Iterable[EC]
      )(using con: DbCon[?]): Vector[E] =
        handleQuery(insertSql, entityCreators):
          Using.Manager: use =>
            val ps =
              use(con.connection.prepareStatement(insertSql, insertGenKeys))
            ecCodec.write(entityCreators, ps)
            timed:
              batchUpdateResult(ps.executeBatch())
              val rs = use(ps.getGeneratedKeys)
              eCodec.read(rs)

      def update(entity: E)(using con: DbCon[?]): Unit =
        handleQuery(updateSql, entity):
          Using(con.connection.prepareStatement(updateSql)): ps =>
            val entityValues: Vector[Any] = entity
              .asInstanceOf[Product]
              .productIterator
              .toVector
            // put ID at the end
            val updateValues = entityValues
              .patch(idIndex, Vector.empty, 1)
              .appended(entityValues(idIndex))

            var pos = 1
            for (field, codec) <- updateValues.lazyZip(updateCodecs) do
              codec.writeSingle(field, ps, pos)
              pos += codec.cols.length
            timed(ps.executeUpdate())

      def updatePartial(original: E, current: E)(using con: DbCon[?]): Unit =
        val origProduct = original.asInstanceOf[Product]
        val currProduct = current.asInstanceOf[Product]

        val origId = origProduct.productElement(idIndex)
        val currId = currProduct.productElement(idIndex)
        require(
          origId == currId,
          s"updatePartial requires same id, got $origId != $currId"
        )

        val arity = origProduct.productArity
        val changed = Vector.newBuilder[Int]
        var i = 0
        while i < arity do
          if i != idIndex && origProduct.productElement(i) != currProduct
              .productElement(i)
          then changed += i
          i += 1
        val changedIndices = changed.result()

        if changedIndices.isEmpty then return

        val setClauses = changedIndices
          .map(idx => eElemNamesSql(idx) + " = " + eElemCodecs(idx).queryRepr)
          .mkString(", ")
        val sql =
          s"UPDATE $tableNameSql SET $setClauses WHERE $idName = ${idCodec.queryRepr}"

        handleQuery(sql, current):
          Using(con.connection.prepareStatement(sql)): ps =>
            var pos = 1
            for idx <- changedIndices do
              val codec = eElemCodecs(idx).asInstanceOf[DbCodec[Any]]
              codec.writeSingle(currProduct.productElement(idx), ps, pos)
              pos += codec.cols.length
            idCodec.writeSingle(currId.asInstanceOf[ID], ps, pos)
            timed(ps.executeUpdate())
      end updatePartial

      def updateAll(entities: Iterable[E])(using
          con: DbCon[?]
      ): BatchUpdateResult =
        handleQuery(updateSql, entities):
          Using(con.connection.prepareStatement(updateSql)): ps =>
            for entity <- entities do
              val entityValues: Vector[Any] = entity
                .asInstanceOf[Product]
                .productIterator
                .toVector
              // put ID at the end
              val updateValues = entityValues
                .patch(idIndex, Vector.empty, 1)
                .appended(entityValues(idIndex))

              var pos = 1
              for (field, codec) <- updateValues.lazyZip(updateCodecs) do
                codec.writeSingle(field, ps, pos)
                pos += codec.cols.length
              ps.addBatch()

            timed(batchUpdateResult(ps.executeBatch()))

      private def h2KeyColumns(target: ConflictTarget): String =
        target match
          case ConflictTarget.Columns(cols*) =>
            if cols.isEmpty then idName
            else cols.map(_.sqlName).mkString(", ")
          case ConflictTarget.Constraint(_) =>
            throw UnsupportedOperationException("H2 does not support ON CONFLICT ON CONSTRAINT")
          case ConflictTarget.AnyConflict => idName

      def insertOnConflict(entityCreator: EC, target: ConflictTarget, action: ConflictAction)(using con: DbCon[?]): Unit =
        action match
          case ConflictAction.DoNothing =>
            // Conditional INSERT: skip if a matching row exists
            val keyColNames = target match
              case ConflictTarget.Columns(cols*) if cols.nonEmpty => cols.map(_.sqlName).toSeq
              case _ => Seq(eElemNamesSql(idIndex))
            val whereClause = keyColNames.map(c => s"$c = ?").mkString(" AND ")
            val ecCols = ecElemNamesSql.mkString(", ")
            val sql = s"INSERT INTO $tableNameSql ($ecCols) SELECT ${ecCodec.queryRepr} WHERE NOT EXISTS (SELECT 1 FROM $tableNameSql WHERE $whereClause)"
            handleQuery(sql, entityCreator):
              Using(con.connection.prepareStatement(sql)): ps =>
                ecCodec.writeSingle(entityCreator, ps)
                // Write the key values for the NOT EXISTS subquery
                val product = entityCreator.asInstanceOf[Product]
                var pos = 1 + ecCodec.cols.length
                for keyCol <- keyColNames do
                  val idx = ecElemNamesSql.indexOf(keyCol)
                  if idx >= 0 then
                    val codec = eElemCodecs(idx).asInstanceOf[DbCodec[Any]]
                    codec.writeSingle(product.productElement(idx), ps, pos)
                    pos += codec.cols.length
                timed(ps.executeUpdate())
          case ConflictAction.DoUpdate(_) =>
            // H2 MERGE INTO KEY does an upsert (updates all columns on conflict)
            val keyColumns = h2KeyColumns(target)
            val sql = s"MERGE INTO $tableNameSql $ecInsertKeys KEY ($keyColumns) VALUES (${ecCodec.queryRepr})"
            handleQuery(sql, entityCreator):
              Using(con.connection.prepareStatement(sql)): ps =>
                ecCodec.writeSingle(entityCreator, ps)
                timed(ps.executeUpdate())

      def insertOnConflictUpdateAll(entityCreator: EC, target: ConflictTarget)(using con: DbCon[?]): Unit =
        val keyColumns = h2KeyColumns(target)
        val sql = s"MERGE INTO $tableNameSql $ecInsertKeys KEY ($keyColumns) VALUES (${ecCodec.queryRepr})"
        handleQuery(sql, entityCreator):
          Using(con.connection.prepareStatement(sql)): ps =>
            ecCodec.writeSingle(entityCreator, ps)
            timed(ps.executeUpdate())

      def insertAllIgnoring(entityCreators: Iterable[EC])(using con: DbCon[?]): Int =
        // Conditional INSERT: skip rows where PK already exists
        val ecCols = ecElemNamesSql.mkString(", ")
        val sql = s"INSERT INTO $tableNameSql ($ecCols) SELECT ${ecCodec.queryRepr} WHERE NOT EXISTS (SELECT 1 FROM $tableNameSql WHERE $idName = ?)"
        handleQuery(sql, entityCreators):
          Using(con.connection.prepareStatement(sql)): ps =>
            timed:
              var count = 0
              for ec <- entityCreators do
                ecCodec.writeSingle(ec, ps)
                val product = ec.asInstanceOf[Product]
                val pkCodec = eElemCodecs(idIndex).asInstanceOf[DbCodec[Any]]
                pkCodec.writeSingle(product.productElement(idIndex), ps, 1 + ecCodec.cols.length)
                count += ps.executeUpdate()
              count

      def upsertByPk(entity: E)(using con: DbCon[?]): Unit =
        handleQuery(upsertByPkSql, entity):
          Using(con.connection.prepareStatement(upsertByPkSql)): ps =>
            eCodec.writeSingle(entity, ps)
            timed(ps.executeUpdate())

    end new
  end buildRepoDefaults
end H2DbType
