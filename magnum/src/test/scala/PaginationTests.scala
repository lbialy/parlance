import com.augustnagro.magnum.*

import java.time.LocalDateTime

@Table(H2DbType, SqlNameMapper.CamelToSnakeCase)
case class QbPaginated(@Id id: Long, name: String, score: Int, createdAt: LocalDateTime)
    derives DbCodec,
      TableMeta

class PaginationTests extends QbTestBase:

  val h2Ddls = Seq("/h2/qb-pagination.sql")

  // --- Offset pagination tests ---

  test("paginate(1, 3) returns first page"):
    val t = xa()
    t.connect:
      val page = QueryBuilder.from[QbPaginated].orderBy(_.id).paginate(1, 3)
      assertEquals(page.items.size, 3)
      assertEquals(page.total, 10L)
      assertEquals(page.totalPages, 4)
      assert(page.hasNext)
      assert(!page.hasPrev)
      assertEquals(page.items.map(_.id), Vector(1L, 2L, 3L))

  test("paginate(4, 3) returns last page with 1 item"):
    val t = xa()
    t.connect:
      val page = QueryBuilder.from[QbPaginated].orderBy(_.id).paginate(4, 3)
      assertEquals(page.items.size, 1)
      assertEquals(page.total, 10L)
      assert(!page.hasNext)
      assert(page.hasPrev)
      assertEquals(page.items.map(_.id), Vector(10L))

  test("paginate with WHERE filter"):
    val t = xa()
    t.connect:
      val page = QueryBuilder.from[QbPaginated].where(_.score > 70).orderBy(_.id).paginate(1, 3)
      assertEquals(page.total, 6L) // 90, 85, 95, 80, 75, 88
      assertEquals(page.items.size, 3)
      assert(page.hasNext)

  test("paginate(0, ...) throws"):
    val t = xa()
    t.connect:
      intercept[QueryBuilderException]:
        QueryBuilder.from[QbPaginated].paginate(0, 3)

  test("paginate(..., 0) throws"):
    val t = xa()
    t.connect:
      intercept[QueryBuilderException]:
        QueryBuilder.from[QbPaginated].paginate(1, 0)

  // --- Keyset pagination tests — single column ---

  test("keyset single column: first page"):
    val t = xa()
    t.connect:
      val paginator = QueryBuilder.from[QbPaginated].keysetPaginate(3)(_.asc(_.id))
      val page1 = paginator.run()
      assertEquals(page1.items.size, 3)
      assert(page1.hasMore)
      assertEquals(page1.items.map(_.id), Vector(1L, 2L, 3L))
      assertEquals(page1.nextKey, Some(3L))

  test("keyset single column: second page via after"):
    val t = xa()
    t.connect:
      val paginator = QueryBuilder.from[QbPaginated].keysetPaginate(3)(_.asc(_.id))
      val page1 = paginator.run()
      val page2 = paginator.after(page1.nextKey.get).run()
      assertEquals(page2.items.map(_.id), Vector(4L, 5L, 6L))
      assert(page2.hasMore)
      assertEquals(page2.nextKey, Some(6L))

  test("keyset single column: iterate to end"):
    val t = xa()
    t.connect:
      val paginator = QueryBuilder.from[QbPaginated].keysetPaginate(3)(_.asc(_.id))
      var page = paginator.run()
      var allIds = page.items.map(_.id)
      while page.hasMore do
        page = paginator.after(page.nextKey.get).run()
        allIds = allIds ++ page.items.map(_.id)
      assertEquals(allIds, Vector(1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, 9L, 10L))
      assert(!page.hasMore)
      assertEquals(page.nextKey, None)

  // --- Keyset pagination tests — multi-column ---

  test("keyset multi-column: desc score, asc id"):
    val t = xa()
    t.connect:
      val paginator = QueryBuilder.from[QbPaginated]
        .keysetPaginate(3)(k => k.desc(_.score).asc(_.id))
      val page1 = paginator.run()
      // Top 3 by score desc: Diana(95), Alice(90), Ivy(88)
      assertEquals(page1.items.map(_.name), Vector("Diana", "Alice", "Ivy"))
      assert(page1.hasMore)

  test("keyset multi-column: second page"):
    val t = xa()
    t.connect:
      val paginator = QueryBuilder.from[QbPaginated]
        .keysetPaginate(3)(k => k.desc(_.score).asc(_.id))
      val page1 = paginator.run()
      val key1 = page1.nextKey.get
      val page2 = paginator.after(key1).run()
      // Next 3 by score desc: Bob(85), Frank(80), Grace(75)
      assertEquals(page2.items.map(_.name), Vector("Bob", "Frank", "Grace"))
      assert(page2.hasMore)

  test("keyset multi-column: iterate to end, no overlaps"):
    val t = xa()
    t.connect:
      val paginator = QueryBuilder.from[QbPaginated]
        .keysetPaginate(3)(k => k.desc(_.score).asc(_.id))
      var page = paginator.run()
      var allNames = page.items.map(_.name)
      while page.hasMore do
        page = paginator.after(page.nextKey.get).run()
        allNames = allNames ++ page.items.map(_.name)
      assertEquals(allNames.size, 10)
      assertEquals(allNames.distinct.size, 10) // no overlaps

  // --- Keyset with WHERE ---

  test("keyset with WHERE filter"):
    val t = xa()
    t.connect:
      val paginator = QueryBuilder.from[QbPaginated]
        .where(_.score > 70)
        .keysetPaginate(3)(_.asc(_.id))
      val page1 = paginator.run()
      // score > 70: ids 1(90), 2(85), 4(95), 6(80), 7(75), 9(88)
      assertEquals(page1.items.map(_.id), Vector(1L, 2L, 4L))
      assert(page1.hasMore)
      val page2 = paginator.after(page1.nextKey.get).run()
      assertEquals(page2.items.map(_.id), Vector(6L, 7L, 9L))
      assert(!page2.hasMore)

  // --- Keyset SQL generation ---

  test("keyset SQL contains expanded OR/AND form"):
    val t = xa()
    t.connect:
      val paginator = QueryBuilder.from[QbPaginated]
        .keysetPaginate(3)(k => k.asc(_.id).desc(_.score))
      val page1 = paginator.run()
      // We can't directly inspect SQL, but we verify correctness through behavior
      // After key (3, 70): should get rows where (id > 3) OR (id = 3 AND score < 70)
      val page2 = paginator.after(page1.nextKey.get).run()
      // All items should have id > 3 (since no ties at id=3 with score < 70)
      assert(page2.items.forall(_.id > 3L))

  test("KeysetSql.buildKeysetFrag generates correct SQL for single column"):
    val entry = KeysetColumnEntry(
      new Col[Long]("id", "id"),
      SortOrder.Asc,
      NullOrder.Default,
      summon[DbCodec[Long]],
      0
    )
    val frag = KeysetSql.buildKeysetFrag(Vector(entry), Vector(5L))
    assertEquals(frag.sqlString, "((id > ?))")

  test("KeysetSql.buildKeysetFrag generates correct SQL for two columns"):
    val entry1 = KeysetColumnEntry(
      new Col[Long]("id", "id"),
      SortOrder.Asc,
      NullOrder.Default,
      summon[DbCodec[Long]],
      0
    )
    val entry2 = KeysetColumnEntry(
      new Col[Int]("score", "score"),
      SortOrder.Desc,
      NullOrder.Default,
      summon[DbCodec[Int]],
      2
    )
    val frag = KeysetSql.buildKeysetFrag(Vector(entry1, entry2), Vector(5L, 70))
    assertEquals(frag.sqlString, "((id > ?) OR (id = ? AND score < ?))")

  test("KeysetSql.buildKeysetFrag generates correct SQL for three columns"):
    val entry1 = KeysetColumnEntry(new Col[Long]("a", "a"), SortOrder.Asc, NullOrder.Default, summon[DbCodec[Long]], 0)
    val entry2 = KeysetColumnEntry(new Col[Int]("b", "b"), SortOrder.Desc, NullOrder.Default, summon[DbCodec[Int]], 1)
    val entry3 = KeysetColumnEntry(new Col[Long]("c", "c"), SortOrder.Asc, NullOrder.Default, summon[DbCodec[Long]], 2)
    val frag = KeysetSql.buildKeysetFrag(Vector(entry1, entry2, entry3), Vector(1L, 2, 3L))
    assertEquals(frag.sqlString, "((a > ?) OR (a = ? AND b < ?) OR (a = ? AND b = ? AND c > ?))")

end PaginationTests
