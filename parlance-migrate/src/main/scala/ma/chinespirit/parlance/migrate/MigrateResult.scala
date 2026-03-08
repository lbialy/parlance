package ma.chinespirit.parlance.migrate

import java.time.Instant

case class AppliedMigration(
    id: Int,
    migration: String,
    batch: Int,
    appliedAt: Instant
)

case class MigrateResult(
    appliedCount: Int,
    batch: Option[Int],
    applied: List[AppliedMigration]
)

case class RollbackResult(
    rolledBackCount: Int,
    rolledBack: List[AppliedMigration]
)

case class MigrationStatus(
    applied: List[AppliedMigration],
    pending: List[MigrationDef]
)

case class PretendResult(
    migrationDef: MigrationDef,
    compiledSql: List[String]
)
