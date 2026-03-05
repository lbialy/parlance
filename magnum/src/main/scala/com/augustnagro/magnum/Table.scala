package com.augustnagro.magnum

import scala.annotation.StaticAnnotation

class Table(
    val nameMapper: SqlNameMapper = SqlNameMapper.SameCase
) extends StaticAnnotation
