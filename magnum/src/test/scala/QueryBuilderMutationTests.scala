import com.augustnagro.magnum.*

@Table(SqlNameMapper.CamelToSnakeCase)
case class QbCounter(@Id id: Long, name: String, status: String, viewCount: Long, score: Int) derives EntityMeta

class QueryBuilderMutationTests extends QbTestBase:

  val h2Ddls = Seq("/h2/qb-mutations.sql")

  // --- delete ---

  test("delete() with WHERE removes matching rows"):
    val t = xa()
    t.connect:
      val deleted = QueryBuilder.from[QbCounter].where(_.status === "draft").delete()
      assertEquals(deleted, 1)
      val remaining = QueryBuilder.from[QbCounter].count()
      assertEquals(remaining, 3L)

  test("delete() without WHERE removes all rows"):
    val t = xa()
    t.connect:
      val deleted = QueryBuilder.from[QbCounter].delete()
      assertEquals(deleted, 4)
      val remaining = QueryBuilder.from[QbCounter].count()
      assertEquals(remaining, 0L)

  test("delete() returns 0 when no rows match"):
    val t = xa()
    t.connect:
      val deleted = QueryBuilder.from[QbCounter].where(_.status === "nonexistent").delete()
      assertEquals(deleted, 0)

  // --- updateUnsafe ---

  test("updateUnsafe(Frag) sets columns on matching rows"):
    val t = xa()
    t.connect:
      val updated = QueryBuilder
        .from[QbCounter]
        .where(_.status === "active")
        .updateUnsafe(sql"status = ${"inactive"}")
      assertEquals(updated, 2)
      val inactive = QueryBuilder.from[QbCounter].where(_.status === "inactive").count()
      assertEquals(inactive, 2L)

  test("updateUnsafe(C => Frag) with typed column access"):
    val t = xa()
    t.connect:
      val updated = QueryBuilder
        .from[QbCounter]
        .where(_.id === 1L)
        .updateUnsafe(c => sql"name = ${"Updated"}")
      assertEquals(updated, 1)
      val row = QueryBuilder.from[QbCounter].where(_.id === 1L).firstOrFail()
      assertEquals(row.name, "Updated")

  test("updateUnsafe returns 0 when no rows match"):
    val t = xa()
    t.connect:
      val updated = QueryBuilder
        .from[QbCounter]
        .where(_.status === "nonexistent")
        .updateUnsafe(sql"name = ${"X"}")
      assertEquals(updated, 0)

  // --- type-safe update ---

  test("update() with named tuple sets columns"):
    val t = xa()
    t.connect:
      val updated = QueryBuilder
        .from[QbCounter]
        .where(_.id === 1L)
        .update((name = "Updated", score = 42))
      assertEquals(updated, 1)
      val row = QueryBuilder.from[QbCounter].where(_.id === 1L).firstOrFail()
      assertEquals(row.name, "Updated")
      assertEquals(row.score, 42)

  test("update() with single field"):
    val t = xa()
    t.connect:
      val updated = QueryBuilder
        .from[QbCounter]
        .where(_.id === 2L)
        .update((status = "inactive"))
      assertEquals(updated, 1)
      val row = QueryBuilder.from[QbCounter].where(_.id === 2L).firstOrFail()
      assertEquals(row.status, "inactive")

  test("update() with WHERE filters correctly"):
    val t = xa()
    t.connect:
      val updated = QueryBuilder
        .from[QbCounter]
        .where(_.status === "active")
        .update((score = 999))
      assertEquals(updated, 2)
      val alpha = QueryBuilder.from[QbCounter].where(_.id === 1L).firstOrFail()
      val beta = QueryBuilder.from[QbCounter].where(_.id === 2L).firstOrFail()
      val gamma = QueryBuilder.from[QbCounter].where(_.id === 3L).firstOrFail()
      assertEquals(alpha.score, 999)
      assertEquals(beta.score, 999)
      assertEquals(gamma.score, 300) // unchanged — not active

  test("update() returns 0 when no rows match"):
    val t = xa()
    t.connect:
      val updated = QueryBuilder
        .from[QbCounter]
        .where(_.status === "nonexistent")
        .update((name = "X"))
      assertEquals(updated, 0)

  // --- compile-time safety: update rejects bad fields/types ---

  test("update() rejects nonexistent field"):
    val errors = compileErrors("""
      import com.augustnagro.magnum.*
      def test(using DbCon[? <: SupportsMutations]): Unit =
        QueryBuilder.from[QbCounter].update((nonexistent = "x"))
    """)
    assert(errors.contains("does not exist on entity"), s"Expected 'does not exist on entity' in: $errors")

  test("update() rejects type mismatch"):
    val errors = compileErrors("""
      import com.augustnagro.magnum.*
      def test(using DbCon[? <: SupportsMutations]): Unit =
        QueryBuilder.from[QbCounter].update((name = 42))
    """)
    assert(errors.contains("Type mismatch") || errors.contains("type mismatch"), s"Expected type mismatch in: $errors")

  // --- increment / decrement ---

  test("increment increments column by amount"):
    val t = xa()
    t.connect:
      val updated = QueryBuilder
        .from[QbCounter]
        .where(_.id === 1L)
        .increment(_.viewCount, 5L)
      assertEquals(updated, 1)
      val row = QueryBuilder.from[QbCounter].where(_.id === 1L).firstOrFail()
      assertEquals(row.viewCount, 15L)

  test("increment with default amount (1)"):
    val t = xa()
    t.connect:
      val updated = QueryBuilder
        .from[QbCounter]
        .where(_.id === 2L)
        .increment(_.viewCount)
      assertEquals(updated, 1)
      val row = QueryBuilder.from[QbCounter].where(_.id === 2L).firstOrFail()
      assertEquals(row.viewCount, 21L)

  test("decrement decrements column by amount"):
    val t = xa()
    t.connect:
      val updated = QueryBuilder
        .from[QbCounter]
        .where(_.id === 3L)
        .decrement(_.viewCount, 10L)
      assertEquals(updated, 1)
      val row = QueryBuilder.from[QbCounter].where(_.id === 3L).firstOrFail()
      assertEquals(row.viewCount, 20L)

  test("increment/decrement with WHERE filters correctly"):
    val t = xa()
    t.connect:
      val updated = QueryBuilder
        .from[QbCounter]
        .where(_.status === "active")
        .increment(_.score, 50)
      assertEquals(updated, 2)
      val alpha = QueryBuilder.from[QbCounter].where(_.id === 1L).firstOrFail()
      val beta = QueryBuilder.from[QbCounter].where(_.id === 2L).firstOrFail()
      val gamma = QueryBuilder.from[QbCounter].where(_.id === 3L).firstOrFail()
      assertEquals(alpha.score, 150)
      assertEquals(beta.score, 250)
      assertEquals(gamma.score, 300) // unchanged — not active

  // --- compile-time safety: increment/decrement reject non-numeric columns ---

  test("increment on String column does not compile"):
    val errors = compileErrors("""
      import com.augustnagro.magnum.*
      def test(using DbCon[? <: SupportsMutations]): Unit =
        QueryBuilder.from[QbCounter].increment(_.name, "x")
    """)
    assert(errors.contains("No given instance of type Numeric[String] was found"))

  test("increment on Instant column does not compile"):
    val errors = compileErrors("""
      import com.augustnagro.magnum.*
      import java.time.Instant
      @Table(SqlNameMapper.CamelToSnakeCase)
      case class Timestamped(@Id id: Long, createdAt: Instant) derives EntityMeta
      def test(using DbCon[? <: SupportsMutations]): Unit =
        QueryBuilder.from[Timestamped].increment(_.createdAt, Instant.now())
    """)
    assert(errors.contains("No given instance of type Numeric[java.time.Instant] was found"))

  test("decrement on String column does not compile"):
    val errors = compileErrors("""
      import com.augustnagro.magnum.*
      def test(using DbCon[? <: SupportsMutations]): Unit =
        QueryBuilder.from[QbCounter].decrement(_.status, "x")
    """)
    assert(errors.contains("No given instance of type Numeric[String] was found"))

end QueryBuilderMutationTests
