package shared

import ma.chinespirit.parlance.*
import munit.{FunSuite, Location}

import java.time.{LocalTime, OffsetDateTime}
import java.util.UUID

def sharedTests[D <: DatabaseType](suite: FunSuite, xa: () => Transactor[D])(using
    Location,
    DbCodec[UUID],
    DbCodec[Boolean],
    DbCodec[OffsetDateTime],
    DbCodec[BigDecimal],
    DbCodec[LocalTime]
): Unit =
  immutableRepoTests(suite, xa)
  sqlNameTests(suite, xa)
  noIdTests(suite, xa)
  embeddedFragTests(suite, xa)
  multilineFragTests(suite, xa)
  bigDecTests(suite, xa)
  dateTimeTests(suite, xa)
  tupleTests(suite, xa)

def sharedMutationTests[D <: SupportsMutations](suite: FunSuite, xa: () => Transactor[D])(using
    Location,
    DbCodec[UUID],
    DbCodec[Boolean],
    DbCodec[OffsetDateTime],
    DbCodec[BigDecimal],
    DbCodec[LocalTime]
): Unit =
  repoTests(suite, xa)
  partialUpdateTests(suite, xa)
  saveTests(suite, xa)
  entityCreatorTests(suite, xa)
  entityExtensionTests(suite, xa)

def sharedReturningMutationTests[D <: SupportsMutations & SupportsReturning](suite: FunSuite, xa: () => Transactor[D])(using
    Location,
    DbCodec[UUID],
    DbCodec[Boolean],
    DbCodec[OffsetDateTime],
    DbCodec[BigDecimal],
    DbCodec[LocalTime]
): Unit =
  repoReturningTests(suite, xa)
end sharedReturningMutationTests

def sharedMultiColReturningTests[D <: SupportsMutations & SupportsMultiColumnReturningKeys](suite: FunSuite, xa: () => Transactor[D])(using
    Location,
    DbCodec[UUID],
    DbCodec[Boolean],
    DbCodec[OffsetDateTime],
    DbCodec[BigDecimal],
    DbCodec[LocalTime]
): Unit =
  repoMultiColReturningTests(suite, xa)
end sharedMultiColReturningTests

def sharedPartialJoinTests[D <: SupportsPartialJoins](suite: FunSuite, xa: () => Transactor[D])(using
    Location,
    DbCodec[UUID],
    DbCodec[Boolean],
    DbCodec[OffsetDateTime],
    DbCodec[BigDecimal],
    DbCodec[LocalTime]
): Unit =
  optionalProductTests(suite, xa)
end sharedPartialJoinTests
