package com.augustnagro.magnum

import java.sql.{Connection, PreparedStatement, ResultSet, Statement}
import java.time.OffsetDateTime
import scala.collection.View
import scala.deriving.Mirror
import scala.reflect.ClassTag
import scala.util.{Failure, Success, Using}

object MySqlDbType extends DbType:

  private val specImpl = new SpecImpl:
    override def sortSql(sort: Sort): String =
      val column = sort.column
      val nullSort = sort.nullOrder match
        case NullOrder.Default => ""
        case NullOrder.First   => s"$column IS NOT NULL, "
        case NullOrder.Last    => s"$column IS NULL, "
        case null              => throw UnsupportedOperationException()
      val dir = sort.direction match
        case SortOrder.Default => ""
        case SortOrder.Asc     => " ASC"
        case SortOrder.Desc    => " DESC"
      nullSort + column + dir

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

    val insertGenKeys = Array(idName)

    val updateKeys: String = eElemNamesSql
      .lazyZip(eElemCodecs)
      .map((sqlName, codec) => sqlName + " = " + codec.queryRepr)
      .patch(idIndex, Seq.empty, 1)
      .mkString(", ")

    val updateCodecs = eElemCodecs
      .patch(idIndex, Seq.empty, 1)
      .appended(idCodec)
      .asInstanceOf[Seq[DbCodec[Any]]]

    val countSql = s"SELECT count(*) FROM $tableNameSql"
    val countQuery = Frag(countSql, Vector.empty, FragWriter.empty).query[Long]
    val existsByIdSql =
      s"SELECT 1 FROM $tableNameSql WHERE $idName = ${idCodec.queryRepr}"
    val findAllSql = s"SELECT * FROM $tableNameSql"
    val findAllQuery = Frag(findAllSql, Vector.empty, FragWriter.empty).query[E]
    val findByIdSql =
      s"SELECT * FROM $tableNameSql WHERE $idName = ${idCodec.queryRepr}"
    val deleteByIdSql =
      s"DELETE FROM $tableNameSql WHERE $idName = ${idCodec.queryRepr}"
    val truncateSql = s"TRUNCATE TABLE $tableNameSql"
    val truncateUpdate =
      Frag(truncateSql, Vector.empty, FragWriter.empty).update
    val insertSql =
      s"INSERT INTO $tableNameSql $ecInsertKeys VALUES (${ecCodec.queryRepr})"
    val updateSql =
      s"UPDATE $tableNameSql SET $updateKeys WHERE $idName = ${idCodec.queryRepr}"
    val insertAndFindByIdSql = insertSql + "\n" + findByIdSql

    // upsert pre-computed SQL
    val eInsertKeys = eElemNamesSql.mkString("(", ", ", ")")
    val upsertSetClause = eElemNamesSql
      .patch(idIndex, Seq.empty, 1)
      .map(col => s"$col = VALUES($col)")
      .mkString(", ")
    val upsertByPkSql =
      s"INSERT INTO $tableNameSql $eInsertKeys VALUES (${eCodec.queryRepr}) ON DUPLICATE KEY UPDATE $upsertSetClause"
    val ecUpdateAllSetClause = ecElemNamesSql
      .map(col => s"$col = VALUES($col)")
      .mkString(", ")
    val insertIgnoringSql =
      s"INSERT IGNORE INTO $tableNameSql $ecInsertKeys VALUES (${ecCodec.queryRepr})"

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
        specImpl.findAll(spec, tableNameSql)

      def findById(id: ID)(using DbCon[?]): Option[E] =
        Frag(findByIdSql, IArray(id), idWriter(id))
          .query[E]
          .run()
          .headOption

      def findAllById(ids: Iterable[ID])(using DbCon[?]): Vector[E] =
        throw UnsupportedOperationException(
          "MySql does not support 'ANY' keyword, and does not support long IN parameter lists. Use findById in a loop instead."
        )

      def delete(entity: E)(using DbCon[?]): Unit =
        deleteById(
          entity
            .asInstanceOf[Product]
            .productElement(idIndex)
            .asInstanceOf[ID]
        )

      def deleteById(id: ID)(using DbCon[?]): Unit =
        Frag(deleteByIdSql, IArray(id), idWriter(id)).update
          .run()

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
        // unfortunately, mysql only will return auto_incremented keys.
        // it doesn't return default columns, and adding other columns to
        // the insertGenKeys array doesn't change this behavior.
        throw UnsupportedOperationException()

      def insertAllReturning(
          entityCreators: Iterable[EC]
      )(using con: DbCon[?]): Vector[E] =
        throw UnsupportedOperationException()

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
      def insertOnConflict(entityCreator: EC, target: ConflictTarget, action: ConflictAction)(using con: DbCon[?]): Unit =
        target match
          case ConflictTarget.Constraint(_) =>
            throw UnsupportedOperationException("MySQL does not support ON CONFLICT ON CONSTRAINT")
          case _ => ()
        val sql = action match
          case ConflictAction.DoNothing =>
            s"INSERT IGNORE INTO $tableNameSql $ecInsertKeys VALUES (${ecCodec.queryRepr})"
          case ConflictAction.DoUpdate(frag) =>
            s"INSERT INTO $tableNameSql $ecInsertKeys VALUES (${ecCodec.queryRepr}) ON DUPLICATE KEY UPDATE ${frag.sqlString}"
        val fragWriter: FragWriter = action match
          case ConflictAction.DoNothing      => FragWriter.empty
          case ConflictAction.DoUpdate(frag) => frag.writer
        handleQuery(sql, entityCreator):
          Using(con.connection.prepareStatement(sql)): ps =>
            ecCodec.writeSingle(entityCreator, ps)
            fragWriter.write(ps, 1 + ecCodec.cols.length)
            timed(ps.executeUpdate())

      def insertOnConflictUpdateAll(entityCreator: EC, target: ConflictTarget)(using con: DbCon[?]): Unit =
        target match
          case ConflictTarget.Constraint(_) =>
            throw UnsupportedOperationException("MySQL does not support ON CONFLICT ON CONSTRAINT")
          case _ => ()
        val sql = s"INSERT INTO $tableNameSql $ecInsertKeys VALUES (${ecCodec.queryRepr}) ON DUPLICATE KEY UPDATE $ecUpdateAllSetClause"
        handleQuery(sql, entityCreator):
          Using(con.connection.prepareStatement(sql)): ps =>
            ecCodec.writeSingle(entityCreator, ps)
            timed(ps.executeUpdate())

      def insertAllIgnoring(entityCreators: Iterable[EC])(using con: DbCon[?]): Int =
        handleQuery(insertIgnoringSql, entityCreators):
          Using(con.connection.prepareStatement(insertIgnoringSql)): ps =>
            ecCodec.write(entityCreators, ps)
            timed:
              val results = ps.executeBatch()
              results.count(_ > 0)

      def upsertByPk(entity: E)(using con: DbCon[?]): Unit =
        handleQuery(upsertByPkSql, entity):
          Using(con.connection.prepareStatement(upsertByPkSql)): ps =>
            eCodec.writeSingle(entity, ps)
            timed(ps.executeUpdate())

    end new
  end buildRepoDefaults
end MySqlDbType
