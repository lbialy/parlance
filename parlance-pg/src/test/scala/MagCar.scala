import ma.chinespirit.parlance.{DbCodec, Id, SqlNameMapper, Table}
import ma.chinespirit.parlance.pg.PgCodec.given
import ma.chinespirit.parlance.pg.enums.PgStringToScalaEnumSqlArrayCodec

@Table(SqlNameMapper.CamelToSnakeCase)
case class MagCar(
    @Id id: Long,
    textColors: Seq[Color],
    textColorMap: Vector[List[Color]],
    lastService: Option[LastService],
    myJsonB: Option[MyJsonB],
    myXml: Option[MyXml]
) derives DbCodec
