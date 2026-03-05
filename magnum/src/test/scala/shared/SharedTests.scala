package shared

import com.augustnagro.magnum.*
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
  repoTests(suite, xa)
  partialUpdateTests(suite, xa)
  saveTests(suite, xa)
  entityCreatorTests(suite, xa)
  sqlNameTests(suite, xa)
  noIdTests(suite, xa)
  embeddedFragTests(suite, xa)
  multilineFragTests(suite, xa)
  bigDecTests(suite, xa)
  optionalProductTests(suite, xa)
  dateTimeTests(suite, xa)
  tupleTests(suite, xa)
  entityExtensionTests(suite, xa)
end sharedTests
