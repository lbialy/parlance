package ma.chinespirit.parlance

import scala.annotation.StaticAnnotation

class Table(
    val nameMapper: SqlNameMapper = SqlNameMapper.SameCase
) extends StaticAnnotation
