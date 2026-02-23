import com.augustnagro.magnum.*

@Table(H2DbType, SqlNameMapper.CamelToSnakeCase)
case class RepoItem(
    @Id id: Long,
    name: String,
    status: String,
    amount: Int
) derives DbCodec, TableMeta

class RepoQueryTests extends QbTestBase:

  val h2Ddls = Seq("/h2/repo-query.sql")

  val itemRepo = Repo[RepoItem, RepoItem, Long]()

  // --- query basics ---

  test("repo.query.run() returns all entities"):
    val t = xa()
    t.connect:
      val results = itemRepo.query.run()
      assertEquals(results.length, 5)
      assertEquals(results.map(_.id).sorted, Vector(1L, 2L, 3L, 4L, 5L))

  test("repo.query.where(_.field === value).run() type-safe column access"):
    val t = xa()
    t.connect:
      val results = itemRepo.query
        .where(_.status === "active")
        .run()
      assertEquals(results.length, 3)
      assertEquals(results.map(_.id).sorted, Vector(1L, 2L, 4L))

  test("repo.query.where(_.field === value).first() returns Option[E]"):
    val t = xa()
    t.connect:
      val result = itemRepo.query
        .where(_.name === "Alpha")
        .first()
      assert(result.isDefined)
      assertEquals(result.get.id, 1L)

      val missing = itemRepo.query
        .where(_.name === "NonExistent")
        .first()
      assert(missing.isEmpty)

  // --- find / findOrFail ---

  test("repo.find(id) returns Some for existing entity"):
    val t = xa()
    t.connect:
      val result = itemRepo.find(1L)
      assert(result.isDefined)
      assertEquals(result.get.name, "Alpha")

  test("repo.find(id) returns None for missing entity"):
    val t = xa()
    t.connect:
      val result = itemRepo.find(999L)
      assert(result.isEmpty)

  test("repo.findOrFail(id) returns entity for existing id"):
    val t = xa()
    t.connect:
      val result = itemRepo.findOrFail(2L)
      assertEquals(result.name, "Beta")

  test("repo.findOrFail(id) throws for missing id"):
    val t = xa()
    t.connect:
      intercept[QueryBuilderException]:
        itemRepo.findOrFail(999L)

  // --- queryUnscoped ---

  test("repo.queryUnscoped.run() returns all entities"):
    val t = xa()
    t.connect:
      val results = itemRepo.queryUnscoped.run()
      assertEquals(results.length, 5)

  // --- scoped repo ---

  test("scoped repo: query applies scope"):
    val t = xa()
    t.connect:
      val activeScope = new Scope[RepoItem]:
        def apply[C <: Selectable](
            qb: QueryBuilder[HasRoot, RepoItem, C]
        ): QueryBuilder[HasRoot, RepoItem, C] =
          qb.where(sql"status = 'active'".unsafeAsWhere)

      val scopedRepo =
        Repo[RepoItem, RepoItem, Long](Vector(activeScope))

      val results = scopedRepo.query.run()
      assertEquals(results.length, 3)
      assertEquals(results.map(_.id).sorted, Vector(1L, 2L, 4L))

  test("scoped repo: queryUnscoped bypasses scope"):
    val t = xa()
    t.connect:
      val activeScope = new Scope[RepoItem]:
        def apply[C <: Selectable](
            qb: QueryBuilder[HasRoot, RepoItem, C]
        ): QueryBuilder[HasRoot, RepoItem, C] =
          qb.where(sql"status = 'active'".unsafeAsWhere)

      val scopedRepo =
        Repo[RepoItem, RepoItem, Long](Vector(activeScope))

      val results = scopedRepo.queryUnscoped.run()
      assertEquals(results.length, 5)

  test("scoped repo: query preserves type-safe column access after scope"):
    val t = xa()
    t.connect:
      val activeScope = new Scope[RepoItem]:
        def apply[C <: Selectable](
            qb: QueryBuilder[HasRoot, RepoItem, C]
        ): QueryBuilder[HasRoot, RepoItem, C] =
          qb.where(sql"status = 'active'".unsafeAsWhere)

      val scopedRepo =
        Repo[RepoItem, RepoItem, Long](Vector(activeScope))

      val results = scopedRepo.query
        .where(_.amount > 15)
        .run()
      assertEquals(results.length, 2)
      assertEquals(results.map(_.id).sorted, Vector(2L, 4L))

  // --- finalScopes override ---

  test("finalScopes override via mixin trait"):
    val t = xa()
    t.connect:
      val localScope = new Scope[RepoItem]:
        def apply[C <: Selectable](
            qb: QueryBuilder[HasRoot, RepoItem, C]
        ): QueryBuilder[HasRoot, RepoItem, C] =
          qb.where(sql"amount >= 30".unsafeAsWhere)

      val customRepo = new Repo[RepoItem, RepoItem, Long]():
        override def finalScopes: Vector[Scope[RepoItem]] =
          super.finalScopes :+ localScope

      val results = customRepo.query.run()
      assertEquals(results.length, 3)
      assertEquals(results.map(_.id).sorted, Vector(3L, 4L, 5L))

  // --- refresh ---

  test("repo.refresh re-fetches entity from DB"):
    val t = xa()
    t.transact:
      val original = itemRepo.findOrFail(1L)
      assertEquals(original.name, "Alpha")

      sql"UPDATE repo_item SET name = 'AlphaUpdated' WHERE id = 1".update.run()

      val refreshed = itemRepo.refresh(original)
      assertEquals(refreshed.name, "AlphaUpdated")

      // Restore original state
      sql"UPDATE repo_item SET name = 'Alpha' WHERE id = 1".update.run()

  test("repo.refresh throws for deleted entity"):
    val t = xa()
    t.transact:
      val entity = itemRepo.findOrFail(1L)

      sql"DELETE FROM repo_item WHERE id = 1".update.run()

      intercept[QueryBuilderException]:
        itemRepo.refresh(entity)

      // Restore
      sql"INSERT INTO repo_item VALUES (1, 'Alpha', 'active', 10)".update.run()

end RepoQueryTests
