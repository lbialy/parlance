import ma.chinespirit.parlance.{DbCodec, SqlName, SqlNameMapper, Table}

@SqlName("colour")
@Table(SqlNameMapper.CamelToSnakeCase)
enum Color derives DbCodec:
  case RedOrange
  @SqlName("Greenish") case Green
  case Blue
