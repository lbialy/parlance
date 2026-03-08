import com.augustnagro.magnum.*

@Table(SqlNameMapper.CamelToSnakeCase)
case class ClPerson(@Id id: Long, name: String) derives EntityMeta

@Table(SqlNameMapper.CamelToSnakeCase)
case class ClTrip(@Id id: Long, destination: String) derives EntityMeta
object ClTrip:
  val owners =
    Relationship.belongsToMany[ClTrip, ClPerson]("cl_trip_owner", "trip_id", "person_id")
  val users =
    Relationship.belongsToMany[ClTrip, ClPerson]("cl_trip_user", "trip_id", "person_id")

@Table(SqlNameMapper.CamelToSnakeCase)
case class ClChecklist(@Id id: Long, tripId: Long, title: String) derives EntityMeta
object ClChecklist:
  val trip = Relationship.belongsTo[ClChecklist, ClTrip](_.tripId, _.id)

@Table(SqlNameMapper.CamelToSnakeCase)
case class ClChecklistItem(@Id id: Long, checklistId: Long, formId: Long, description: String) derives EntityMeta
object ClChecklistItem:
  val checklist = Relationship.belongsTo[ClChecklistItem, ClChecklist](_.checklistId, _.id)

trait WhereHasTestsDefs:
  self: QbTestBase[?] =>

  // --- Relationship (HasMany) tests ---

  test("whereHas returns only authors with books"):
    val t = xa()
    t.connect:
      val results =
        QueryBuilder.from[ElAuthor].whereHas(ElAuthor.books).orderBy(_.name).run()
      assertEquals(results.size, 3)
      assertEquals(results.map(_.name), Vector("Asimov", "Herbert", "Tolkien"))

  test("doesntHave returns only authors without books"):
    val t = xa()
    t.connect:
      val results =
        QueryBuilder.from[ElAuthor].doesntHave(ElAuthor.books).run()
      assertEquals(results.size, 1)
      assertEquals(results.head.name, "Rowling")

  test("whereHas with condition (exact match)"):
    val t = xa()
    t.connect:
      val results = QueryBuilder
        .from[ElAuthor]
        .whereHas(ElAuthor.books)(_.title === "Dune")
        .run()
      assertEquals(results.size, 1)
      assertEquals(results.head.name, "Herbert")

  test("whereHas with condition (LIKE)"):
    val t = xa()
    t.connect:
      val results = QueryBuilder
        .from[ElAuthor]
        .whereHas(ElAuthor.books)(_.title.like("The%"))
        .run()
      assertEquals(results.size, 1)
      assertEquals(results.head.name, "Tolkien")

  test("whereHas + where combined"):
    val t = xa()
    t.connect:
      val results = QueryBuilder
        .from[ElAuthor]
        .where(_.name === "Tolkien")
        .whereHas(ElAuthor.books)
        .run()
      assertEquals(results.size, 1)
      assertEquals(results.head.name, "Tolkien")

  test("whereHas + orderBy + limit"):
    val t = xa()
    t.connect:
      val results = QueryBuilder
        .from[ElAuthor]
        .whereHas(ElAuthor.books)
        .orderBy(_.name)
        .limit(2)
        .run()
      assertEquals(results.size, 2)
      assertEquals(results(0).name, "Asimov")
      assertEquals(results(1).name, "Herbert")

  test("SQL verification: contains EXISTS subquery"):
    val frag = QueryBuilder
      .from[ElAuthor]
      .whereHas(ElAuthor.books)
      .buildWith(databaseType)
    assert(
      frag.sqlString.contains("EXISTS (SELECT 1 FROM"),
      s"SQL should contain EXISTS subquery: ${frag.sqlString}"
    )
    assert(
      frag.sqlString.contains("el_book.author_id = el_author.id"),
      s"SQL should contain correlation: ${frag.sqlString}"
    )

  // --- BelongsToMany tests ---

  test("whereHas(btm) returns users with any roles"):
    val t = xa()
    t.connect:
      val results = QueryBuilder
        .from[PvUser]
        .whereHas(PvUser.roles)
        .orderBy(_.name)
        .run()
      assertEquals(results.size, 3)
      assertEquals(results.map(_.name), Vector("Alice", "Bob", "Dave"))

  test("doesntHave(btm) returns users without roles"):
    val t = xa()
    t.connect:
      val results = QueryBuilder
        .from[PvUser]
        .doesntHave(PvUser.roles)
        .run()
      assertEquals(results.size, 1)
      assertEquals(results.head.name, "Charlie")

  test("whereHas(btm, condition) on target"):
    val t = xa()
    t.connect:
      val results = QueryBuilder
        .from[PvUser]
        .whereHas(PvUser.roles)(_.name === "admin")
        .orderBy(_.name)
        .run()
      assertEquals(results.size, 2)
      assertEquals(results.map(_.name), Vector("Alice", "Dave"))

  test("doesntHave + where combined"):
    val t = xa()
    t.connect:
      val results = QueryBuilder
        .from[PvUser]
        .doesntHave(PvUser.roles)
        .where(_.name === "Charlie")
        .run()
      assertEquals(results.size, 1)
      assertEquals(results.head.name, "Charlie")

  // --- orWhereHas / orDoesntHave tests ---

  test("orWhereHas with HasMany: authors with books OR named Rowling"):
    val t = xa()
    t.connect:
      val results = QueryBuilder
        .from[ElAuthor]
        .where(_.name === "Rowling")
        .orWhereHas(ElAuthor.books)
        .orderBy(_.name)
        .run()
      assertEquals(results.size, 4)
      assertEquals(results.map(_.name), Vector("Asimov", "Herbert", "Rowling", "Tolkien"))

  test("orWhereHas with HasMany + condition"):
    val t = xa()
    t.connect:
      val results = QueryBuilder
        .from[ElAuthor]
        .where(_.name === "Rowling")
        .orWhereHas(ElAuthor.books)(_.title === "Dune")
        .orderBy(_.name)
        .run()
      assertEquals(results.size, 2)
      assertEquals(results.map(_.name), Vector("Herbert", "Rowling"))

  test("orWhereHas with BelongsToMany: users with roles OR named Charlie"):
    val t = xa()
    t.connect:
      val results = QueryBuilder
        .from[PvUser]
        .where(_.name === "Charlie")
        .orWhereHas(PvUser.roles)
        .orderBy(_.name)
        .run()
      assertEquals(results.size, 4)
      assertEquals(results.map(_.name), Vector("Alice", "Bob", "Charlie", "Dave"))

  test("orWhereHas with BelongsToMany + condition"):
    val t = xa()
    t.connect:
      val results = QueryBuilder
        .from[PvUser]
        .where(_.name === "Charlie")
        .orWhereHas(PvUser.roles)(_.name === "admin")
        .orderBy(_.name)
        .run()
      assertEquals(results.size, 3)
      assertEquals(results.map(_.name), Vector("Alice", "Charlie", "Dave"))

  test("orDoesntHave with HasMany"):
    val t = xa()
    t.connect:
      val results = QueryBuilder
        .from[ElAuthor]
        .where(_.name === "Tolkien")
        .orDoesntHave(ElAuthor.books)
        .orderBy(_.name)
        .run()
      assertEquals(results.size, 2)
      assertEquals(results.map(_.name), Vector("Rowling", "Tolkien"))

  test("orDoesntHave with BelongsToMany"):
    val t = xa()
    t.connect:
      val results = QueryBuilder
        .from[PvUser]
        .where(_.name === "Alice")
        .orDoesntHave(PvUser.roles)
        .orderBy(_.name)
        .run()
      assertEquals(results.size, 2)
      assertEquals(results.map(_.name), Vector("Alice", "Charlie"))

  test("SQL verification: orWhereHas contains OR EXISTS"):
    val frag = QueryBuilder
      .from[ElAuthor]
      .where(_.name === "Rowling")
      .orWhereHas(ElAuthor.books)
      .buildWith(databaseType)
    assert(
      frag.sqlString.contains("OR EXISTS (SELECT 1 FROM"),
      s"SQL should contain OR EXISTS subquery: ${frag.sqlString}"
    )

  test("whereHas + orWhereHas chained (AND EXISTS ... OR EXISTS ...)"):
    val t = xa()
    t.connect:
      // authors who have books with title "Dune" OR have books with title starting "The"
      val results = QueryBuilder
        .from[ElAuthor]
        .whereHas(ElAuthor.books)(_.title === "Dune")
        .orWhereHas(ElAuthor.books)(_.title.like("The%"))
        .orderBy(_.name)
        .run()
      assertEquals(results.size, 2)
      assertEquals(results.map(_.name), Vector("Herbert", "Tolkien"))

  // --- Edge cases ---

  test("whereHas on empty result"):
    val t = xa()
    t.connect:
      val results = QueryBuilder
        .from[ElAuthor]
        .where(_.name === "Nobody")
        .whereHas(ElAuthor.books)
        .run()
      assertEquals(results, Vector.empty[ElAuthor])

  test("chained whereHas + doesntHave (contradictory) returns empty"):
    val t = xa()
    t.connect:
      val results = QueryBuilder
        .from[ElAuthor]
        .whereHas(ElAuthor.books)
        .doesntHave(ElAuthor.books)
        .run()
      assertEquals(results, Vector.empty[ElAuthor])

  // --- Nested EXISTS tests ---

  test("SQL verification: nested EXISTS"):
    val frag = QueryBuilder
      .from[ElAuthor]
      .whereHas(ElAuthor.books)(_.whereHas(ElBook.reviews)(_.score >= 4))
      .buildWith(databaseType)
    assert(
      frag.sqlString.contains("EXISTS (SELECT 1 FROM el_book"),
      s"SQL should contain outer EXISTS: ${frag.sqlString}"
    )
    assert(
      frag.sqlString.contains("EXISTS (SELECT 1 FROM el_review"),
      s"SQL should contain inner EXISTS: ${frag.sqlString}"
    )

  test("nested whereHas execution: authors with books that have high-score reviews"):
    val t = xa()
    t.connect:
      // Authors who have books with reviews scoring >= 4
      // Tolkien: The Hobbit(5), Asimov: Foundation(4), Herbert: Dune(5)
      // The Silmarillion has score 3, so still Tolkien qualifies via The Hobbit
      val results = QueryBuilder
        .from[ElAuthor]
        .whereHas(ElAuthor.books)(_.whereHas(ElBook.reviews)(_.score >= 4))
        .orderBy(_.name)
        .run()
      assertEquals(results.size, 3)
      assertEquals(results.map(_.name), Vector("Asimov", "Herbert", "Tolkien"))

  test("nested whereHas with strict threshold"):
    val t = xa()
    t.connect:
      // Authors who have books with reviews scoring >= 5
      // Tolkien: The Hobbit(5), Herbert: Dune(5)
      val results = QueryBuilder
        .from[ElAuthor]
        .whereHas(ElAuthor.books)(_.whereHas(ElBook.reviews)(_.score >= 5))
        .orderBy(_.name)
        .run()
      assertEquals(results.size, 2)
      assertEquals(results.map(_.name), Vector("Herbert", "Tolkien"))

  test("&& inside whereHas condition: title filter AND nested review"):
    val t = xa()
    t.connect:
      // Authors who have books starting with "The" AND that book has reviews with score >= 4
      // The Hobbit has score 5 -> Tolkien qualifies
      // The Silmarillion has score 3 -> doesn't qualify
      val results = QueryBuilder
        .from[ElAuthor]
        .whereHas(ElAuthor.books)(sq => sq.title.like("The%") && sq.whereHas(ElBook.reviews)(_.score >= 4))
        .orderBy(_.name)
        .run()
      assertEquals(results.size, 1)
      assertEquals(results.head.name, "Tolkien")

  test("|| inside whereHas: books titled Dune OR books with score >= 5"):
    val t = xa()
    t.connect:
      // Authors who have (books titled "Dune") OR (books with reviews scoring >= 5)
      // Herbert: Dune (also score 5), Tolkien: The Hobbit score 5
      val results = QueryBuilder
        .from[ElAuthor]
        .whereHas(ElAuthor.books)(sq => (sq.title === "Dune") || sq.whereHas(ElBook.reviews)(_.score >= 5))
        .orderBy(_.name)
        .run()
      assertEquals(results.size, 2)
      assertEquals(results.map(_.name), Vector("Herbert", "Tolkien"))

  test("three-level nesting: author -> book -> review with conditions at each level"):
    val t = xa()
    t.connect:
      // Authors named "Tolkien" who have books with reviews containing "Great"
      val results = QueryBuilder
        .from[ElAuthor]
        .where(_.name === "Tolkien")
        .whereHas(ElAuthor.books)(_.whereHas(ElBook.reviews)(_.body === "Great"))
        .run()
      assertEquals(results.size, 1)
      assertEquals(results.head.name, "Tolkien")

  // --- Eloquent-style deep nesting test ---
  // Port of the original PHP query that motivated this feature:
  //   CheckListItem::where('form_id', $form->id)
  //     ->whereHas('checklist', fn($q) =>
  //       $q->whereHas('trip', fn($q) =>
  //         $q->where(fn($q) =>
  //           $q->whereHas('owners', fn($q) => $q->where('id', $user->id))
  //             ->orWhereHas('users', fn($q) => $q->where('id', $user->id)))))
  //     ->exists();

  test("Eloquent pattern: checklist items accessible by user as owner OR user"):
    val t = xa()
    t.connect:
      val formId = 100L

      // Alice (id=1): owns Paris trip -> checklist 1 -> items 1,2 (form 100)
      // Bob (id=2): owns Tokyo trip -> checklist 2 -> item 3 (form 100)
      //             AND is user of Paris trip -> checklist 1 -> items 1,2 (form 100)
      // Charlie (id=3): owns London trip -> checklist 3 -> item 5 (form 100)
      //                 AND is user of Tokyo trip -> checklist 2 -> item 3 (form 100)

      def hasAccessViaChecklist(userId: Long): Boolean =
        QueryBuilder
          .from[ClChecklistItem]
          .where(_.formId === formId)
          .whereHas(ClChecklistItem.checklist)(sq =>
            sq.whereHas(ClChecklist.trip)(sq2 => sq2.whereHas(ClTrip.owners)(_.id === userId) || sq2.whereHas(ClTrip.users)(_.id === userId))
          )
          .exists()

      // Alice (id=1): owner of Paris -> items 1,2 are form 100 -> true
      assert(hasAccessViaChecklist(1L), "Alice should have access via ownership of Paris trip")

      // Bob (id=2): owner of Tokyo -> item 3 is form 100 -> true
      //             also user of Paris -> items 1,2 are form 100 -> true
      assert(hasAccessViaChecklist(2L), "Bob should have access via Tokyo ownership or Paris usage")

      // Charlie (id=3): owner of London -> item 5 is form 100 -> true
      //                 also user of Tokyo -> item 3 is form 100 -> true
      assert(hasAccessViaChecklist(3L), "Charlie should have access via London ownership or Tokyo usage")

  test("Eloquent pattern: no access for non-existent user"):
    val t = xa()
    t.connect:
      val formId = 100L
      val nonExistentUserId = 999L

      val hasAccess = QueryBuilder
        .from[ClChecklistItem]
        .where(_.formId === formId)
        .whereHas(ClChecklistItem.checklist)(sq =>
          sq.whereHas(ClChecklist.trip)(sq2 =>
            sq2.whereHas(ClTrip.owners)(_.id === nonExistentUserId) || sq2.whereHas(ClTrip.users)(_.id === nonExistentUserId)
          )
        )
        .exists()

      assert(!hasAccess, "Non-existent user should not have access")

  test("Eloquent pattern: different form_id returns false"):
    val t = xa()
    t.connect:
      val nonExistentFormId = 999L

      val hasAccess = QueryBuilder
        .from[ClChecklistItem]
        .where(_.formId === nonExistentFormId)
        .whereHas(ClChecklistItem.checklist)(sq =>
          sq.whereHas(ClChecklist.trip)(sq2 => sq2.whereHas(ClTrip.owners)(_.id === 1L) || sq2.whereHas(ClTrip.users)(_.id === 1L))
        )
        .exists()

      assert(!hasAccess, "Non-existent form should return false")

  test("Eloquent pattern: SQL structure verification"):
    val formId = 100L
    val userId = 1L

    val frag = QueryBuilder
      .from[ClChecklistItem]
      .where(_.formId === formId)
      .whereHas(ClChecklistItem.checklist)(sq =>
        sq.whereHas(ClChecklist.trip)(sq2 => sq2.whereHas(ClTrip.owners)(_.id === userId) || sq2.whereHas(ClTrip.users)(_.id === userId))
      )
      .buildWith(databaseType)

    val sql = frag.sqlString
    // Should have 3 levels of EXISTS nesting + an OR between the two innermost
    assert(sql.contains("EXISTS (SELECT 1 FROM cl_checklist"), s"Missing checklist EXISTS: $sql")
    assert(sql.contains("EXISTS (SELECT 1 FROM cl_trip"), s"Missing trip EXISTS: $sql")
    assert(sql.contains("EXISTS (SELECT 1 FROM cl_trip_owner"), s"Missing trip_owner EXISTS: $sql")
    assert(sql.contains("EXISTS (SELECT 1 FROM cl_trip_user"), s"Missing trip_user EXISTS: $sql")
    assert(sql.contains(" OR "), s"Missing OR between owner/user EXISTS: $sql")

end WhereHasTestsDefs

class WhereHasTests extends QbH2TestBase, WhereHasTestsDefs:
  val h2Ddls = Seq("/h2/qb-where-has.sql")
end WhereHasTests

class PgWhereHasTests extends QbPgTestBase, WhereHasTestsDefs:
  val pgDdls = Seq("/pg/qb-where-has.sql")
end PgWhereHasTests
