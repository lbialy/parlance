import ma.chinespirit.parlance.*

@Table(SqlNameMapper.CamelToSnakeCase)
case class QbItem(@Id id: Long, amount: Int) derives EntityMeta

trait ChunkTestsDefs:
  self: QbTestBase[?] =>

  test("chunk(25) over 100 rows yields exactly 4 batches of 25"):
    val t = xa()
    t.connect:
      val batches = QueryBuilder.from[QbItem].orderBy(_.id).chunk(25).toVector
      assertEquals(batches.size, 4)
      assert(batches.forall(_.size == 25))

  test("chunk collects all rows exactly once"):
    val t = xa()
    t.connect:
      val all = QueryBuilder.from[QbItem].orderBy(_.id).chunk(25).flatMap(identity).toVector
      assertEquals(all.size, 100)
      assertEquals(all.map(_.id), (1L to 100L).toVector)

  test("chunk with .where filter"):
    val t = xa()
    t.connect:
      // amount > 500 means id > 50, so 50 rows
      val batches = QueryBuilder
        .from[QbItem]
        .where(_.amount > 500)
        .orderBy(_.id)
        .chunk(20)
        .toVector
      assertEquals(batches.size, 3) // 20, 20, 10
      assertEquals(batches(0).size, 20)
      assertEquals(batches(1).size, 20)
      assertEquals(batches(2).size, 10)

  test("chunk with .limit respects total maximum"):
    val t = xa()
    t.connect:
      val batches = QueryBuilder
        .from[QbItem]
        .orderBy(_.id)
        .limit(50)
        .chunk(20)
        .toVector
      assertEquals(batches.size, 3) // 20, 20, 10
      assertEquals(batches(0).size, 20)
      assertEquals(batches(1).size, 20)
      assertEquals(batches(2).size, 10)
      val all = batches.flatMap(identity)
      assertEquals(all.size, 50)
      assertEquals(all.map(_.id), (1L to 50L).toVector)

  test("chunk with .offset starts from given offset"):
    val t = xa()
    t.connect:
      // offset 80 means rows 81-100, so 20 rows
      val batches = QueryBuilder
        .from[QbItem]
        .orderBy(_.id)
        .offset(80)
        .chunk(25)
        .toVector
      assertEquals(batches.size, 1)
      assertEquals(batches(0).size, 20)
      assertEquals(batches(0).head.id, 81L)
      assertEquals(batches(0).last.id, 100L)

  test("chunk preserves orderBy within and across batches"):
    val t = xa()
    t.connect:
      val all = QueryBuilder
        .from[QbItem]
        .orderBy(_.id, SortOrder.Desc)
        .chunk(30)
        .flatMap(identity)
        .toVector
      assertEquals(all.size, 100)
      assertEquals(all.map(_.id), (100L to 1L by -1L).toVector)

  test("chunk with batchSize larger than result set returns single batch"):
    val t = xa()
    t.connect:
      val batches = QueryBuilder.from[QbItem].orderBy(_.id).chunk(200).toVector
      assertEquals(batches.size, 1)
      assertEquals(batches(0).size, 100)

  test("chunk on empty result set returns empty iterator"):
    val t = xa()
    t.connect:
      val batches = QueryBuilder
        .from[QbItem]
        .where(_.amount > 99999)
        .chunk(10)
        .toVector
      assertEquals(batches.size, 0)

end ChunkTestsDefs

class ChunkTests extends QbH2TestBase, ChunkTestsDefs:
  val h2Ddls = Seq("/h2/qb-chunk.sql")

class PgChunkTests extends QbPgTestBase, ChunkTestsDefs:
  val pgDdls = Seq("/pg/qb-chunk.sql")
