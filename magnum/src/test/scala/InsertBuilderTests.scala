import com.augustnagro.magnum.*

@Table(SqlNameMapper.CamelToSnakeCase)
case class QbMutItem(@Id id: Long, name: String, amount: Int) derives EntityMeta

@Table(SqlNameMapper.CamelToSnakeCase)
case class QbMutItemCreator(name: String, amount: Int) derives DbCodec

trait InsertBuilderTestsDefs:
  self: QbTestBase[? <: SupportsMutations & SupportsReturning] =>

  // --- insert ---

  test("insert single entity creator"):
    val t = xa()
    t.connect:
      QueryBuilder.into[QbMutItemCreator, QbMutItem].insert(QbMutItemCreator("Delta", 40))
      val all = QueryBuilder.from[QbMutItem].run()
      assertEquals(all.size, 4)
      val delta = all.find(_.name == "Delta").get
      assertEquals(delta.amount, 40)

  test("insertAll multiple entity creators"):
    val t = xa()
    t.connect:
      QueryBuilder
        .into[QbMutItemCreator, QbMutItem]
        .insertAll(
          Seq(QbMutItemCreator("Delta", 40), QbMutItemCreator("Epsilon", 50))
        )
      val all = QueryBuilder.from[QbMutItem].run()
      assertEquals(all.size, 5)

  // --- insertReturning ---

  test("insertReturning returns created entity with generated id"):
    val t = xa()
    t.connect:
      val result = QueryBuilder.into[QbMutItemCreator, QbMutItem].insertReturning(QbMutItemCreator("Delta", 40))
      assertEquals(result.name, "Delta")
      assertEquals(result.amount, 40)
      assert(result.id > 0L)

  test("insertAllReturning returns all created entities"):
    val t = xa()
    t.connect:
      val results = QueryBuilder
        .into[QbMutItemCreator, QbMutItem]
        .insertAllReturning(
          Seq(QbMutItemCreator("Delta", 40), QbMutItemCreator("Epsilon", 50))
        )
      assertEquals(results.size, 2)
      assertEquals(results.map(_.name).toSet, Set("Delta", "Epsilon"))

  // --- insertOnConflict ---

  test("insertOnConflict DoNothing ignores duplicate"):
    val t = xa()
    t.connect:
      QueryBuilder
        .into[QbMutItem, QbMutItem]
        .insertOnConflict(
          QbMutItem(1, "AlphaUpdated", 999),
          ConflictTarget.AnyConflict,
          ConflictAction.DoNothing
        )
      val item = QueryBuilder.from[QbMutItem].where(_.id === 1L).firstOrFail()
      assertEquals(item.amount, 10) // unchanged

  test("insertOnConflict DoNothing inserts when no conflict"):
    val t = xa()
    t.connect:
      QueryBuilder
        .into[QbMutItem, QbMutItem]
        .insertOnConflict(
          QbMutItem(4, "Delta", 40),
          ConflictTarget.AnyConflict,
          ConflictAction.DoNothing
        )
      assertEquals(QueryBuilder.from[QbMutItem].count(), 4L)

  test("insertOnConflictUpdateAll upserts on conflict"):
    val t = xa()
    t.connect:
      QueryBuilder
        .into[QbMutItem, QbMutItem]
        .insertOnConflictUpdateAll(
          QbMutItem(1, "AlphaUpdated", 999),
          ConflictTarget.Columns(Col[Long]("id", "id"))
        )
      val item = QueryBuilder.from[QbMutItem].where(_.id === 1L).firstOrFail()
      assertEquals(item.name, "AlphaUpdated")
      assertEquals(item.amount, 999)

  // --- insertAllIgnoring ---

  test("insertAllIgnoring skips duplicates, returns insert count"):
    val t = xa()
    t.connect:
      val inserted = QueryBuilder
        .into[QbMutItem, QbMutItem]
        .insertAllIgnoring(
          Seq(
            QbMutItem(1, "Alpha", 999), // conflict
            QbMutItem(4, "Delta", 40), // new
            QbMutItem(5, "Epsilon", 50) // new
          )
        )
      assertEquals(inserted, 2)
      assertEquals(QueryBuilder.from[QbMutItem].count(), 5L)
      assertEquals(QueryBuilder.from[QbMutItem].where(_.id === 1L).firstOrFail().amount, 10) // unchanged

end InsertBuilderTestsDefs

class InsertBuilderTests extends QbH2TestBase, InsertBuilderTestsDefs:
  val h2Ddls = Seq("/h2/qb-entity-mutations.sql")
end InsertBuilderTests

class PgInsertBuilderTests extends QbPgTestBase, InsertBuilderTestsDefs:
  val pgDdls = Seq("/pg/qb-entity-mutations.sql")
end PgInsertBuilderTests
