import ma.chinespirit.parlance.*

trait RelationshipLoadTestsDefs:
  self: QbTestBase[?] =>

  // BelongsTo: book -> author
  val bookAuthor = Relationship.belongsTo[ElBook, ElAuthor](_.authorId, _.id)

  // --- load: Relationship (BelongsTo, HasOne, HasMany) ---

  test("load BelongsTo returns matching entity"):
    val t = xa()
    t.connect:
      val book = ElBook(1, 1, "The Hobbit")
      val authors = book.load(bookAuthor)
      assertEquals(authors.size, 1)
      assertEquals(authors.head.name, "Tolkien")

  test("loadOne BelongsTo returns Some"):
    val t = xa()
    t.connect:
      val book = ElBook(1, 1, "The Hobbit")
      val author = book.loadOne(bookAuthor)
      assert(author.isDefined)
      assertEquals(author.get.name, "Tolkien")

  test("load HasMany returns related entities"):
    val t = xa()
    t.connect:
      val tolkien = ElAuthor(1, "Tolkien")
      val books = tolkien.load(ElAuthor.books)
      assertEquals(books.size, 2)
      assertEquals(books.map(_.title).toSet, Set("The Hobbit", "The Silmarillion"))

  test("load HasMany with no related returns empty"):
    val t = xa()
    t.connect:
      val rowling = ElAuthor(4, "Rowling")
      val books = rowling.load(ElAuthor.books)
      assertEquals(books, Vector.empty[ElBook])

  test("loadOne on BelongsTo with no match returns None"):
    val t = xa()
    t.connect:
      val book = ElBook(999, 999, "Nonexistent")
      val author = book.loadOne(bookAuthor)
      assertEquals(author, None)

  // --- load: BelongsToMany ---

  test("load BelongsToMany returns related through pivot"):
    val t = xa()
    t.connect:
      val alice = PvUser(1, "Alice")
      val roles = alice.load(PvUser.roles)
      assertEquals(roles.size, 2)
      assertEquals(roles.map(_.name).toSet, Set("admin", "editor"))

  test("load BelongsToMany with no related returns empty"):
    val t = xa()
    t.connect:
      val charlie = PvUser(3, "Charlie")
      val roles = charlie.load(PvUser.roles)
      assertEquals(roles, Vector.empty[PvRole])

  test("loadOne BelongsToMany returns Some"):
    val t = xa()
    t.connect:
      val bob = PvUser(2, "Bob")
      val role = bob.loadOne(PvUser.roles)
      assert(role.isDefined)
      assertEquals(role.get.name, "editor")

  // --- load: HasManyThrough ---

  test("load HasManyThrough returns related through intermediate"):
    val t = xa()
    t.connect:
      val uk = ThCountry(1, "UK")
      val posts = uk.load(ThCountry.posts)
      assertEquals(posts.size, 3)

  test("load HasManyThrough with no intermediate returns empty"):
    val t = xa()
    t.connect:
      val japan = ThCountry(4, "Japan")
      val posts = japan.load(ThCountry.posts)
      assertEquals(posts, Vector.empty[ThPost])

  test("loadOne HasManyThrough returns Some"):
    val t = xa()
    t.connect:
      val us = ThCountry(2, "US")
      val post = us.loadOne(ThCountry.posts)
      assert(post.isDefined)
      assertEquals(post.get.title, "Charlie says hi")

  // --- load: HasOneThrough ---

  test("load HasOneThrough returns related"):
    val t = xa()
    t.connect:
      val mech = ThMechanic(1, "Mech1")
      val owners = mech.load(ThMechanic.owner)
      assertEquals(owners.size, 1)
      assertEquals(owners.head.name, "OwnerA")

  test("load HasOneThrough with no target returns empty"):
    val t = xa()
    t.connect:
      val mech = ThMechanic(2, "Mech2")
      val owners = mech.load(ThMechanic.owner)
      assertEquals(owners, Vector.empty[ThOwner])

  test("loadOne HasOneThrough returns Some"):
    val t = xa()
    t.connect:
      val mech = ThMechanic(1, "Mech1")
      val owner = mech.loadOne(ThMechanic.owner)
      assert(owner.isDefined)
      assertEquals(owner.get.name, "OwnerA")

  test("loadOne HasOneThrough with no target returns None"):
    val t = xa()
    t.connect:
      val mech = ThMechanic(3, "Mech3")
      val owner = mech.loadOne(ThMechanic.owner)
      assertEquals(owner, None)

  // --- load: ComposedRelationship ---

  test("load ComposedRelationship returns related"):
    val t = xa()
    t.connect:
      val post = ViaPost(3, 2, "Bob Post 1")
      val contacts = post.load(ViaPost.contacts)
      assertEquals(contacts.size, 1)
      assertEquals(contacts.head.email, "bob@example.com")

  test("loadOne ComposedRelationship returns Some"):
    val t = xa()
    t.connect:
      val post = ViaPost(3, 2, "Bob Post 1")
      val contact = post.loadOne(ViaPost.contacts)
      assert(contact.isDefined)
      assertEquals(contact.get.email, "bob@example.com")

  test("load ComposedRelationship with no intermediate match returns empty"):
    val t = xa()
    t.connect:
      val post = ViaPost(999, 999, "Nonexistent")
      val contacts = post.load(ViaPost.contacts)
      assertEquals(contacts, Vector.empty[ViaContact])

end RelationshipLoadTestsDefs

class RelationshipLoadTests extends QbH2TestBase, RelationshipLoadTestsDefs:
  val h2Ddls = Seq(
    "/h2/qb-eager-load.sql",
    "/h2/qb-pivot.sql",
    "/h2/qb-through.sql",
    "/h2/qb-via.sql"
  )

class PgRelationshipLoadTests extends QbPgTestBase, RelationshipLoadTestsDefs:
  val pgDdls = Seq(
    "/pg/qb-eager-load.sql",
    "/pg/qb-pivot.sql",
    "/pg/qb-through.sql",
    "/pg/qb-via.sql"
  )
