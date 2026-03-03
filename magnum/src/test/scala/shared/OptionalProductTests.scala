package shared

import com.augustnagro.magnum.*
import munit.{FunSuite, Location}

import java.time.OffsetDateTime

def optionalProductTests(
    suite: FunSuite,
    dbType: DbType,
    xa: () => Transactor[?]
)(using Location, DbCodec[BigDecimal], DbCodec[OffsetDateTime]): Unit =
  import suite.*

  @Table(dbType, SqlNameMapper.CamelToSnakeCase)
  case class Car(
      model: String,
      @Id id: Long,
      topSpeed: Int,
      @SqlName("vin") vinNumber: Option[Int],
      color: Color,
      created: OffsetDateTime
  ) derives EntityMeta

  @Table(dbType, SqlNameMapper.CamelToSnakeCase)
  case class BigDec(id: Int, myBigDec: Option[BigDecimal]) derives EntityMeta

  test("left join with optional product type"):
    assume(dbType != ClickhouseDbType)
    xa().connect:
      val res = sql"select * from car c left join big_dec bd on bd.id = c.id"
        .query[(Car, Option[BigDec])]
        .run()
      assert(res.exists((_, bigDec) => bigDec.isEmpty))
end optionalProductTests
