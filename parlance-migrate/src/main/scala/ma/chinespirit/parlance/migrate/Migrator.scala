package ma.chinespirit.parlance.migrate

import ma.chinespirit.parlance.{DbCon, DbTx, Transactor, DatabaseType}

import java.sql.{PreparedStatement, ResultSet, Statement, Timestamp}
import java.time.Instant
import scala.util.Using

class Migrator(
    migrations: List[MigrationDef],
    xa: Transactor[?],
    compiler: MigrationCompiler = PostgresCompiler,
    tableName: String = "_parlance_migrations"
):
  // Validate: unique versions
  locally:
    val versions = migrations.map(_.version)
    val dupes = versions.diff(versions.distinct)
    if dupes.nonEmpty then
      throw MigrationError(
        s"Duplicate migration versions: ${dupes.distinct.mkString(", ")}"
      )
    // Validate: sorted order
    if versions != versions.sorted then
      throw MigrationError(
        "Migrations must be provided in sorted version order"
      )

  private val sortedMigrations: List[MigrationDef] = migrations

  private val trackingTableDDL: Migration.CreateTable =
    Migration.CreateTable(
      name = tableName,
      columns = List(
        ColumnDef[Int]("id", ColumnType.Serial).primaryKey,
        ColumnDef[String]("migration", ColumnType.Varchar(512)),
        ColumnDef[Int]("batch", ColumnType.Integer),
        ColumnDef[Instant]("applied_at", ColumnType.TimestampTz())
      ),
      options = TableOptions.empty.copy(ifNotExists = true)
    )

  // --- Public API ---

  /** Apply all pending migrations. */
  def migrate(): MigrateResult =
    xa.transact:
      ensureTrackingTable()
      val applied = loadApplied()
      val appliedKeys = applied.map(_.migration).toSet
      val pending =
        sortedMigrations.filterNot(m => appliedKeys.contains(migrationKey(m)))
      if pending.isEmpty then MigrateResult(0, None, Nil)
      else
        val batch = if applied.isEmpty then 1 else applied.map(_.batch).max + 1
        val newlyApplied = List.newBuilder[AppliedMigration]
        for m <- pending do
          val resolved = resolveTxMode(m, m.up)
          resolved match
            case TxMode.Transactional | TxMode.Auto =>
              m.up.foreach(executeMigration(_))
              val am = insertApplied(migrationKey(m), batch)
              newlyApplied += am
            case TxMode.NonTransactional =>
              throw MigrationError(
                s"Migration ${m.version}_${m.name} requires non-transactional execution " +
                  "but is running inside a shared transaction. " +
                  "This migration contains CONCURRENTLY operations."
              )
        MigrateResult(pending.size, Some(batch), newlyApplied.result())

  /** Apply all pending migrations, using per-migration tx mode resolution. */
  def migrateWithTxControl(): MigrateResult =
    // First determine pending in a read
    val (pending, batch) = xa.connect:
      ensureTrackingTable()
      val applied = loadApplied()
      val appliedKeys = applied.map(_.migration).toSet
      val pend =
        sortedMigrations.filterNot(m => appliedKeys.contains(migrationKey(m)))
      val b = if applied.isEmpty then 1 else applied.map(_.batch).max + 1
      (pend, b)

    if pending.isEmpty then MigrateResult(0, None, Nil)
    else
      val newlyApplied = List.newBuilder[AppliedMigration]
      for m <- pending do
        val resolved = resolveTxMode(m, m.up)
        resolved match
          case TxMode.Transactional | TxMode.Auto =>
            xa.transact:
              m.up.foreach(executeMigration(_))
              val am = insertApplied(migrationKey(m), batch)
              newlyApplied += am
          case TxMode.NonTransactional =>
            xa.connect:
              m.up.foreach(executeMigration(_))
              val am = insertApplied(migrationKey(m), batch)
              newlyApplied += am
      MigrateResult(pending.size, Some(batch), newlyApplied.result())
  end migrateWithTxControl

  /** Roll back the latest batch. */
  def rollback(): RollbackResult =
    xa.transact:
      ensureTrackingTable()
      val applied = loadApplied()
      if applied.isEmpty then RollbackResult(0, Nil)
      else
        val latestBatch = applied.map(_.batch).max
        rollbackBatchInternal(applied, latestBatch)

  /** Roll back a specific batch. */
  def rollbackBatch(batch: Int): RollbackResult =
    xa.transact:
      ensureTrackingTable()
      val applied = loadApplied()
      rollbackBatchInternal(applied, batch)

  /** Roll back the last N individual migrations (across batch boundaries). */
  def rollbackSteps(steps: Int): RollbackResult =
    xa.transact:
      ensureTrackingTable()
      val applied = loadApplied()
      val toRollback = applied.sortBy(_.id).reverse.take(steps)
      doRollback(toRollback)

  /** Roll back all applied migrations. */
  def reset(): RollbackResult =
    rollbackSteps(Int.MaxValue)

  /** Show applied and pending migrations. Read-only. */
  def status(): MigrationStatus =
    xa.connect:
      if !trackingTableExists() then MigrationStatus(Nil, sortedMigrations)
      else
        val applied = loadApplied()
        val appliedKeys = applied.map(_.migration).toSet
        val pending =
          sortedMigrations.filterNot(m => appliedKeys.contains(migrationKey(m)))
        MigrationStatus(applied, pending)

  /** Dry-run: compile pending migrations without executing. Read-only. */
  def pretend(): List[PretendResult] =
    xa.connect:
      val pending =
        if !trackingTableExists() then sortedMigrations
        else
          val applied = loadApplied()
          val appliedKeys = applied.map(_.migration).toSet
          sortedMigrations.filterNot(m => appliedKeys.contains(migrationKey(m)))
      pending.map: m =>
        val sqls = m.up.flatMap:
          case Migration.RawParameterized(sql, _) => List(sql)
          case migration                          => compiler.compile(migration)
        PretendResult(m, sqls)

  // --- Private helpers ---

  private def resolveTxMode(
      m: MigrationDef,
      ops: List[Migration]
  ): TxMode =
    m.txMode match
      case TxMode.Transactional =>
        val hasNonTx = ops.exists(compiler.requiresNonTx)
        if hasNonTx then
          throw MigrationError(
            s"Migration ${m.version}_${m.name} contains CONCURRENTLY ops " +
              "but txMode is forced to Transactional"
          )
        TxMode.Transactional
      case TxMode.NonTransactional =>
        TxMode.NonTransactional
      case TxMode.Auto =>
        val hasNonTx = ops.exists(compiler.requiresNonTx)
        if hasNonTx then TxMode.NonTransactional
        else compiler.defaultTxMode

  private def migrationKey(m: MigrationDef): String =
    s"${m.version}_${m.name}"

  private def ensureTrackingTable()(using con: DbCon[?]): Unit =
    val sqls = compiler.compile(trackingTableDDL)
    val conn = con.connection
    Using.resource(conn.createStatement()): stmt =>
      sqls.foreach(stmt.execute(_))

  private def trackingTableExists()(using con: DbCon[?]): Boolean =
    val conn = con.connection
    Using.resource(
      conn.prepareStatement(
        "SELECT COUNT(*) FROM information_schema.tables WHERE LOWER(table_name) = LOWER(?)"
      )
    ): ps =>
      ps.setString(1, tableName)
      Using.resource(ps.executeQuery()): rs =>
        rs.next()
        rs.getInt(1) > 0

  private def loadApplied()(using con: DbCon[?]): List[AppliedMigration] =
    val conn = con.connection
    Using.resource(conn.createStatement()): stmt =>
      Using.resource(
        stmt.executeQuery(
          s"SELECT id, migration, batch, applied_at FROM $tableName ORDER BY id"
        )
      ): rs =>
        val buf = List.newBuilder[AppliedMigration]
        while rs.next() do
          buf += AppliedMigration(
            id = rs.getInt("id"),
            migration = rs.getString("migration"),
            batch = rs.getInt("batch"),
            appliedAt = rs.getTimestamp("applied_at").toInstant
          )
        buf.result()

  private def insertApplied(key: String, batch: Int)(using
      con: DbCon[?]
  ): AppliedMigration =
    val conn = con.connection
    val now = Instant.now()
    Using.resource(
      conn.prepareStatement(
        s"INSERT INTO $tableName (migration, batch, applied_at) VALUES (?, ?, ?)",
        Statement.RETURN_GENERATED_KEYS
      )
    ): ps =>
      ps.setString(1, key)
      ps.setInt(2, batch)
      ps.setTimestamp(3, Timestamp.from(now))
      ps.executeUpdate()
      Using.resource(ps.getGeneratedKeys): rs =>
        rs.next()
        AppliedMigration(
          id = rs.getInt(1),
          migration = key,
          batch = batch,
          appliedAt = now
        )
  end insertApplied

  private def deleteApplied(id: Int)(using con: DbCon[?]): Unit =
    val conn = con.connection
    Using.resource(
      conn.prepareStatement(s"DELETE FROM $tableName WHERE id = ?")
    ): ps =>
      ps.setInt(1, id)
      ps.executeUpdate()

  private def executeMigration(m: Migration)(using con: DbCon[?]): Unit =
    val conn = con.connection
    m match
      case Migration.RawParameterized(sql, params) =>
        Using.resource(conn.prepareStatement(sql)): ps =>
          params.zipWithIndex.foreach: (param, idx) =>
            ps.setObject(idx + 1, param)
          ps.executeUpdate()
      case other =>
        val sqls = compiler.compile(other)
        Using.resource(conn.createStatement()): stmt =>
          sqls.foreach(stmt.execute(_))

  private def rollbackBatchInternal(
      applied: List[AppliedMigration],
      batch: Int
  )(using tx: DbTx[?]): RollbackResult =
    val batchRecords =
      applied.filter(_.batch == batch).sortBy(_.id).reverse
    doRollback(batchRecords)

  private def doRollback(records: List[AppliedMigration])(using
      tx: DbTx[?]
  ): RollbackResult =
    val migrationsByKey =
      sortedMigrations.map(m => migrationKey(m) -> m).toMap
    for record <- records do
      val migDef = migrationsByKey.getOrElse(
        record.migration,
        throw MigrationError(
          s"Cannot rollback: migration '${record.migration}' not found in provided migrations"
        )
      )
      migDef.down.foreach(executeMigration(_))
      deleteApplied(record.id)
    RollbackResult(records.size, records)

end Migrator
