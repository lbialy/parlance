import com.augustnagro.magnum.*

import java.time.OffsetDateTime

@Table(SqlNameMapper.CamelToSnakeCase)
case class TouchableItem(@Id id: Long, name: String, status: String, @updatedAt updatedAt: OffsetDateTime) derives EntityMeta

class TouchTests extends QbTestBase:

  val h2Ddls = Seq("/h2/qb-touch.sql")

  val repo = Repo[TouchableItem, TouchableItem, Long]()

  val epoch = OffsetDateTime.parse("2025-01-01T00:00:00Z")

  // --- QB-level touch ---

  test("QB touch() updates matching rows"):
    val t = xa()
    t.connect:
      val updated = QueryBuilder.from[TouchableItem].where(_.status === "active").touch()
      assertEquals(updated, 2)
      val alpha = QueryBuilder.from[TouchableItem].where(_.id === 1L).firstOrFail()
      val gamma = QueryBuilder.from[TouchableItem].where(_.id === 3L).firstOrFail()
      assert(alpha.updatedAt.isAfter(epoch), s"Expected ${alpha.updatedAt} to be after $epoch")
      assertEquals(gamma.updatedAt, epoch) // unchanged — not active

  test("QB touch() without WHERE updates all rows"):
    val t = xa()
    t.connect:
      val updated = QueryBuilder.from[TouchableItem].touch()
      assertEquals(updated, 3)
      val all = QueryBuilder.from[TouchableItem].run()
      all.foreach: item =>
        assert(item.updatedAt.isAfter(epoch), s"Expected ${item.updatedAt} to be after $epoch")

  test("QB touch() returns 0 when no rows match"):
    val t = xa()
    t.connect:
      val updated = QueryBuilder.from[TouchableItem].where(_.status === "nonexistent").touch()
      assertEquals(updated, 0)

  // --- Repo-level touch ---

  test("Repo touch(entity) updates single entity"):
    val t = xa()
    t.connect:
      val entity = repo.findById(1L).get
      assertEquals(entity.updatedAt, epoch)
      repo.touch(entity)
      val refreshed = repo.findById(1L).get
      assert(refreshed.updatedAt.isAfter(epoch), s"Expected ${refreshed.updatedAt} to be after $epoch")
      // others unchanged
      val gamma = repo.findById(3L).get
      assertEquals(gamma.updatedAt, epoch)

  // --- Compile-time safety ---

  test("touch() does not compile without @updatedAt"):
    val errors = compileErrors("""
      import com.augustnagro.magnum.*
      @Table(SqlNameMapper.CamelToSnakeCase)
      case class NoUpdatedAt(@Id id: Long, name: String) derives EntityMeta
      def test(using DbCon[?]): Unit =
        QueryBuilder.from[NoUpdatedAt].touch()
    """)
    assert(errors.nonEmpty, "Expected compilation error for entity without @updatedAt")

end TouchTests
