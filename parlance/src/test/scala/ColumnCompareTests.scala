import ma.chinespirit.parlance.*

@Table(SqlNameMapper.CamelToSnakeCase)
case class QbScore(@Id id: Long, scoreA: Int, scoreB: Int) derives EntityMeta

trait ColumnCompareTestsDefs:
  self: QbTestBase[?] =>

  // --- SQL fragment tests ---

  test("col === col produces correct SQL"):
    val frag = QueryBuilder
      .from[QbScore]
      .where(c => c.scoreA === c.scoreB)
      .buildWith(databaseType)
    assertEquals(
      frag.sqlString,
      "SELECT id, score_a, score_b FROM qb_score WHERE score_a = score_b"
    )
    assertEquals(frag.params, Seq.empty)

  test("col !== col produces correct SQL"):
    val frag = QueryBuilder
      .from[QbScore]
      .where(c => c.scoreA !== c.scoreB)
      .buildWith(databaseType)
    assertEquals(
      frag.sqlString,
      "SELECT id, score_a, score_b FROM qb_score WHERE score_a <> score_b"
    )

  test("col > col produces correct SQL"):
    val frag = QueryBuilder
      .from[QbScore]
      .where(c => c.scoreA > c.scoreB)
      .buildWith(databaseType)
    assertEquals(
      frag.sqlString,
      "SELECT id, score_a, score_b FROM qb_score WHERE score_a > score_b"
    )

  test("col < col produces correct SQL"):
    val frag = QueryBuilder
      .from[QbScore]
      .where(c => c.scoreA < c.scoreB)
      .buildWith(databaseType)
    assertEquals(
      frag.sqlString,
      "SELECT id, score_a, score_b FROM qb_score WHERE score_a < score_b"
    )

  test("col >= col produces correct SQL"):
    val frag = QueryBuilder
      .from[QbScore]
      .where(c => c.scoreA >= c.scoreB)
      .buildWith(databaseType)
    assertEquals(
      frag.sqlString,
      "SELECT id, score_a, score_b FROM qb_score WHERE score_a >= score_b"
    )

  test("col <= col produces correct SQL"):
    val frag = QueryBuilder
      .from[QbScore]
      .where(c => c.scoreA <= c.scoreB)
      .buildWith(databaseType)
    assertEquals(
      frag.sqlString,
      "SELECT id, score_a, score_b FROM qb_score WHERE score_a <= score_b"
    )

  // --- Integration tests ---

  test("where scoreA === scoreB returns equal rows"):
    val t = xa()
    t.connect:
      val results = QueryBuilder
        .from[QbScore]
        .where(c => c.scoreA === c.scoreB)
        .run()
      assertEquals(results.length, 2)
      val ids = results.map(_.id).sorted
      assertEquals(ids, Vector(2L, 4L))

  test("where scoreA > scoreB returns rows where A wins"):
    val t = xa()
    t.connect:
      val results = QueryBuilder
        .from[QbScore]
        .where(c => c.scoreA > c.scoreB)
        .run()
      assertEquals(results.length, 1)
      assertEquals(results.head.id, 3L)

  test("where scoreA < scoreB returns rows where B wins"):
    val t = xa()
    t.connect:
      val results = QueryBuilder
        .from[QbScore]
        .where(c => c.scoreA < c.scoreB)
        .run()
      assertEquals(results.length, 1)
      assertEquals(results.head.id, 1L)

  test("where scoreA !== scoreB returns non-equal rows"):
    val t = xa()
    t.connect:
      val results = QueryBuilder
        .from[QbScore]
        .where(c => c.scoreA !== c.scoreB)
        .run()
      assertEquals(results.length, 2)
      val ids = results.map(_.id).sorted
      assertEquals(ids, Vector(1L, 3L))

  // --- Overload coexistence: value comparison still works ---

  test("value comparison still works alongside column comparison"):
    val t = xa()
    t.connect:
      val results = QueryBuilder
        .from[QbScore]
        .where(_.scoreA === 10)
        .run()
      assertEquals(results.length, 1)
      assertEquals(results.head.id, 1L)

end ColumnCompareTestsDefs

class ColumnCompareTests extends QbH2TestBase, ColumnCompareTestsDefs:
  val h2Ddls = Seq("/h2/qb-score.sql")
end ColumnCompareTests

class PgColumnCompareTests extends QbPgTestBase, ColumnCompareTestsDefs:
  val pgDdls = Seq("/pg/qb-score.sql")
end PgColumnCompareTests
