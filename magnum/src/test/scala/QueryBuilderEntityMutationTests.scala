import com.augustnagro.magnum.*

// Uses QbMutItem defined in InsertBuilderTests.scala

trait QueryBuilderEntityMutationTestsDefs:
  self: QbTestBase[? <: SupportsMutations] =>

  // --- updateEntity ---

  test("updateEntity updates a single entity by PK"):
    val t = xa()
    t.connect:
      QueryBuilder.from[QbMutItem].updateEntity(QbMutItem(1, "AlphaUpdated", 999))
      val item = QueryBuilder.from[QbMutItem].where(_.id === 1L).firstOrFail()
      assertEquals(item.name, "AlphaUpdated")
      assertEquals(item.amount, 999)

  test("updateEntity leaves other rows unchanged"):
    val t = xa()
    t.connect:
      QueryBuilder.from[QbMutItem].updateEntity(QbMutItem(2, "BetaUpdated", 999))
      val alpha = QueryBuilder.from[QbMutItem].where(_.id === 1L).firstOrFail()
      assertEquals(alpha.name, "Alpha") // unchanged

  // --- updateAllEntities ---

  test("updateAllEntities batch updates multiple entities"):
    val t = xa()
    t.connect:
      val result = QueryBuilder
        .from[QbMutItem]
        .updateAllEntities(
          Seq(QbMutItem(1, "A", 11), QbMutItem(2, "B", 22))
        )
      result match
        case BatchUpdateResult.Success(n)    => assertEquals(n, 2L)
        case BatchUpdateResult.SuccessNoInfo => () // acceptable
      assertEquals(QueryBuilder.from[QbMutItem].where(_.id === 1L).firstOrFail().name, "A")
      assertEquals(QueryBuilder.from[QbMutItem].where(_.id === 2L).firstOrFail().name, "B")
      assertEquals(QueryBuilder.from[QbMutItem].where(_.id === 3L).firstOrFail().name, "Gamma") // unchanged

  // --- updatePartial ---

  test("updatePartial only updates changed fields"):
    val t = xa()
    t.connect:
      val original = QbMutItem(1, "Alpha", 10)
      val current = QbMutItem(1, "AlphaUpdated", 10)
      QueryBuilder.from[QbMutItem].updatePartial(original, current)
      val item = QueryBuilder.from[QbMutItem].where(_.id === 1L).firstOrFail()
      assertEquals(item.name, "AlphaUpdated")
      assertEquals(item.amount, 10) // unchanged

  test("updatePartial is no-op when nothing changed"):
    val t = xa()
    t.connect:
      val original = QbMutItem(1, "Alpha", 10)
      QueryBuilder.from[QbMutItem].updatePartial(original, original)
      val item = QueryBuilder.from[QbMutItem].where(_.id === 1L).firstOrFail()
      assertEquals(item.name, "Alpha")

  test("updatePartial rejects mismatched ids"):
    val t = xa()
    t.connect:
      intercept[IllegalArgumentException]:
        QueryBuilder
          .from[QbMutItem]
          .updatePartial(
            QbMutItem(1, "Alpha", 10),
            QbMutItem(2, "Beta", 20)
          )

  // --- deleteEntity ---

  test("deleteEntity removes a single entity by PK"):
    val t = xa()
    t.connect:
      QueryBuilder.from[QbMutItem].deleteEntity(QbMutItem(1, "Alpha", 10))
      assertEquals(QueryBuilder.from[QbMutItem].count(), 2L)
      assertEquals(QueryBuilder.from[QbMutItem].where(_.id === 1L).first(), None)

  // --- deleteAllEntities ---

  test("deleteAllEntities batch deletes multiple entities"):
    val t = xa()
    t.connect:
      val result = QueryBuilder
        .from[QbMutItem]
        .deleteAllEntities(
          Seq(QbMutItem(1, "Alpha", 10), QbMutItem(2, "Beta", 20))
        )
      result match
        case BatchUpdateResult.Success(n)    => assertEquals(n, 2L)
        case BatchUpdateResult.SuccessNoInfo => ()
      assertEquals(QueryBuilder.from[QbMutItem].count(), 1L)

  // --- deleteAllById ---

  test("deleteAllById batch deletes by IDs"):
    val t = xa()
    t.connect:
      val result = QueryBuilder.from[QbMutItem].deleteAllById[Long](Seq(1L, 3L))
      result match
        case BatchUpdateResult.Success(n)    => assertEquals(n, 2L)
        case BatchUpdateResult.SuccessNoInfo => ()
      assertEquals(QueryBuilder.from[QbMutItem].count(), 1L)
      assertEquals(QueryBuilder.from[QbMutItem].where(_.id === 2L).firstOrFail().name, "Beta")

  // --- truncate ---

  test("truncate removes all rows"):
    val t = xa()
    t.connect:
      QueryBuilder.from[QbMutItem].truncate()
      assertEquals(QueryBuilder.from[QbMutItem].count(), 0L)

  // --- upsertByPk ---

  test("upsertByPk inserts when row absent"):
    val t = xa()
    t.connect:
      QueryBuilder.from[QbMutItem].upsertByPk(QbMutItem(4, "Delta", 40))
      assertEquals(QueryBuilder.from[QbMutItem].count(), 4L)
      assertEquals(QueryBuilder.from[QbMutItem].where(_.id === 4L).firstOrFail().name, "Delta")

  test("upsertByPk updates when row present"):
    val t = xa()
    t.connect:
      QueryBuilder.from[QbMutItem].upsertByPk(QbMutItem(1, "AlphaUpserted", 999))
      val item = QueryBuilder.from[QbMutItem].where(_.id === 1L).firstOrFail()
      assertEquals(item.name, "AlphaUpserted")
      assertEquals(item.amount, 999)

end QueryBuilderEntityMutationTestsDefs

class QueryBuilderEntityMutationTests extends QbH2TestBase, QueryBuilderEntityMutationTestsDefs:
  val h2Ddls = Seq("/h2/qb-entity-mutations.sql")
end QueryBuilderEntityMutationTests

class PgQueryBuilderEntityMutationTests extends QbPgTestBase, QueryBuilderEntityMutationTestsDefs:
  val pgDdls = Seq("/pg/qb-entity-mutations.sql")
end PgQueryBuilderEntityMutationTests
