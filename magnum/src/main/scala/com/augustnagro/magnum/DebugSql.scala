package com.augustnagro.magnum

private[magnum] object DebugSql:

  def printDebug(frags: Vector[Frag])(using dbCon: DbCon): Unit =
    frags.foreach: frag =>
      println(s"SQL: ${frag.sqlString}")
      if frag.params.nonEmpty then println(s"Params: ${frag.params.mkString(", ")}")

    val tablePattern = "(?i)(?:FROM|JOIN)\\s+(\\w+)".r
    val allTables = frags
      .flatMap(f => tablePattern.findAllMatchIn(f.sqlString).map(_.group(1)))
      .distinct

    val con = dbCon.connection
    for tableName <- allTables do
      val stmt = con.createStatement()
      try
        val rs = stmt.executeQuery(s"SELECT * FROM $tableName")
        val rsMeta = rs.getMetaData
        val colCount = rsMeta.getColumnCount
        val colNames = (1 to colCount).map(rsMeta.getColumnName).toVector

        val rows = Vector.newBuilder[Vector[String]]
        while rs.next() do
          rows += (1 to colCount)
            .map(i => Option(rs.getString(i)).getOrElse("NULL"))
            .toVector
        val allRows = rows.result()

        val widths = colNames.indices.map: i =>
          val dataMax =
            if allRows.isEmpty then 0
            else allRows.map(_(i).length).max
          math.max(colNames(i).length, dataMax)

        def row(cells: Vector[String]): String =
          cells.zipWithIndex
            .map((c, i) => c.padTo(widths(i), ' '))
            .mkString("│ ", " │ ", " │")

        val top = widths.map("─" * _).mkString("┌─", "─┬─", "─┐")
        val mid = widths.map("─" * _).mkString("├─", "─┼─", "─┤")
        val bot = widths.map("─" * _).mkString("└─", "─┴─", "─┘")

        println(s"\n[$tableName] (${allRows.size} rows)")
        println(top)
        println(row(colNames))
        println(mid)
        allRows.foreach(r => println(row(r)))
        println(bot)
      finally stmt.close()
      end try
    end for
  end printDebug

end DebugSql
