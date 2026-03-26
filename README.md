# Parlance

High-velocity, opinionated functional ORM inspired by Active Record patterns, laser-focused on productivity.

No dependencies beyond JDBC drivers. Scala 3 only.

## What's in a name?

From Old French *parlance* ("speech"), from Latin *parabola* ("comparison"), from Greek *parabolē* — literally "throwing things side by side." Weirdly perfect for an ORM that maps between two worlds: objects and relations.

## At a glance

- Compile-time derived repositories with full CRUD
- Type-safe `sql""` interpolator with fragment composition
- Fluent query builder with structural typing — `_.firstName` is a real, typed column reference
- Relationships: `belongsTo`, `hasMany`, `belongsToMany`, `hasManyThrough` — with lazy loading and eager loading
- Identity tracking, dirty checking, partial updates
- Active Record-style entity extensions: `.save()`, `.delete()`, `.refresh()`, `.isDirty`
- Scopes, soft deletes, lifecycle observers
- Keyset and offset pagination
- Schema migrations with a typed DSL
- Schema verification against entity definitions
- Supports Postgres, MySQL, SQLite, H2, Oracle, ClickHouse

## 1. Connecting to the Database

Everything in Parlance starts with a `Transactor` — a lightweight wrapper around a `javax.sql.DataSource` that manages connections and transactions.

### Creating a Transactor

```scala
//> using scala 3.8.2
//> using dep "ma.chinespirit::parlance:$VERSION$"
//> using dep "com.h2database:h2:2.3.232"
// compile-only
import ma.chinespirit.parlance.*

val ds = org.h2.jdbcx.JdbcDataSource()
ds.setURL("jdbc:h2:mem:mydb")
ds.setUser("sa")
ds.setPassword("")

val xa = Transactor(H2, ds)
```

The first argument is a database type singleton — one of `Postgres`, `MySQL`, `SQLite`, `H2`, `Oracle`, or `ClickHouse`. This determines dialect-specific SQL generation and which features are available at compile time.

### Connections and Transactions

Use `connect` for auto-commit operations and `transact` when you need a transaction:

```scala
//> using scala 3.8.2
//> using dep "ma.chinespirit::parlance:$VERSION$"
//> using dep "com.h2database:h2:2.3.232"
// compile-only
import ma.chinespirit.parlance.*

val ds = org.h2.jdbcx.JdbcDataSource()
ds.setURL("jdbc:h2:mem:mydb")
ds.setUser("sa")
ds.setPassword("")

val xa = Transactor(H2, ds)

// Auto-commit connection
xa.connect:
  sql"SELECT 1".query[Int].run()

// Transaction — commits on success, rolls back on exception
xa.transact:
  sql"INSERT INTO users (name) VALUES (${"Alice"})".update.run()
  sql"INSERT INTO users (name) VALUES (${"Bob"})".update.run()
```

Both methods provide a context parameter (`DbCon` or `DbTx`) via Scala 3's `?=>` syntax — it's available implicitly to all Parlance operations within the block. `DbTx` extends `DbCon`, so anything that works in `connect` also works in `transact`.

### Connection Configuration

Apply JDBC connection settings to every connection handed out by the transactor:

```scala
//> using scala 3.8.2
//> using dep "ma.chinespirit::parlance:$VERSION$"
//> using dep "com.h2database:h2:2.3.232"
// compile-only
import ma.chinespirit.parlance.*
import java.sql.Connection

val ds = org.h2.jdbcx.JdbcDataSource()
ds.setURL("jdbc:h2:mem:mydb")
ds.setUser("sa")
ds.setPassword("")

val xa = Transactor(H2, ds).withConnectionConfig: conn =>
  conn.setSchema("PUBLIC")

// Or pass it directly at construction
val xa2 = Transactor(H2, ds, (conn: Connection) => conn.setSchema("PUBLIC"))
```

## 2. Entities

Entities are the core abstraction of Parlance. Define your database tables as Scala 3 case classes, work with them as regular objects, and let the ORM handle persistence.

### 2.1 Defining Entities

A Parlance entity is a case class annotated with `@Table` and deriving `EntityMeta`:

```scala
//> using scala 3.8.2
//> using dep "ma.chinespirit::parlance:$VERSION$"
// compile-only
import ma.chinespirit.parlance.*

@Table(SqlNameMapper.CamelToSnakeCase)
case class User(
  @Id id: Long,
  firstName: String,
  lastName: String,
  email: String,
  age: Int
) derives EntityMeta
```

`@Table` sets the naming convention for mapping Scala field names to SQL column names. Three mappers are available:

- `CamelToSnakeCase` — `firstName` becomes `first_name` (most common)
- `CamelToUpperSnakeCase` — `firstName` becomes `FIRST_NAME`
- `SameCase` — `firstName` stays `firstName`

`@Id` marks the primary key. If omitted, the first field is used. Use `@SqlName` to override a generated name for a specific column or table:

```scala
//> using scala 3.8.2
//> using dep "ma.chinespirit::parlance:$VERSION$"
// compile-only
import ma.chinespirit.parlance.*

@SqlName("app_users")
@Table(SqlNameMapper.CamelToSnakeCase)
case class User(
  @Id id: Long,
  @SqlName("full_name") name: String,
  email: String
) derives EntityMeta
```

Composite primary keys are supported — annotate multiple fields with `@Id`:

```scala
//> using scala 3.8.2
//> using dep "ma.chinespirit::parlance:$VERSION$"
// compile-only
import ma.chinespirit.parlance.*

@Table(SqlNameMapper.CamelToSnakeCase)
case class Enrollment(
  @Id studentId: Long,
  @Id courseId: Long,
  grade: Option[String]
) derives EntityMeta
```

#### Creator Pattern

When your table has auto-generated columns (serial IDs, database defaults), define a separate creator class that omits those fields:

```scala
//> using scala 3.8.2
//> using dep "ma.chinespirit::parlance:$VERSION$"
// compile-only
import ma.chinespirit.parlance.*
import java.time.OffsetDateTime

@SqlName("users")
@Table(SqlNameMapper.CamelToSnakeCase)
case class UserCreator(
  firstName: String,
  lastName: String,
  email: String
) extends CreatorOf[User] derives DbCodec

@SqlName("users")
@Table(SqlNameMapper.CamelToSnakeCase)
case class User(
  @Id id: Long,
  firstName: String,
  lastName: String,
  email: String,
  createdAt: OffsetDateTime
) derives EntityMeta
```

The creator must be a structural subset of the entity — this is verified at compile time.

#### Enum and Custom Type Support

Scala 3 enums are mapped as strings automatically:

```scala
//> using scala 3.8.2
//> using dep "ma.chinespirit::parlance:$VERSION$"
// compile-only
import ma.chinespirit.parlance.*

enum Status derives DbCodec:
  case Active, Inactive, Suspended

@Table(SqlNameMapper.CamelToSnakeCase)
case class Account(
  @Id id: Long,
  name: String,
  status: Status
) derives EntityMeta
```

For custom types like opaque types, use `DbCodec.biMap`:

```scala
//> using scala 3.8.2
//> using dep "ma.chinespirit::parlance:$VERSION$"
// compile-only
import ma.chinespirit.parlance.*

opaque type Email = String
object Email:
  def apply(value: String): Email = value
  extension (e: Email) def value: String = e
  given DbCodec[Email] = DbCodec[String].biMap(Email.apply, _.value)

@Table(SqlNameMapper.CamelToSnakeCase)
case class User(
  @Id id: Long,
  name: String,
  email: Email
) derives EntityMeta
```

#### Built-in Supported Types

`DbCodec` derivation is provided out of the box for: `String`, `Boolean`, `Byte`, `Short`, `Int`, `Long`, `Float`, `Double`, `BigDecimal`, `UUID`, `URL`, `Array[Byte]`, `IArray[Byte]`, all `java.time` types (`Instant`, `OffsetDateTime`, `LocalDate`, `LocalTime`, `LocalDateTime`, `ZoneId`), all `java.sql` types (`Date`, `Time`, `Timestamp`, `Blob`, `Clob`, `NClob`, `SQLXML`), `Option[A]` for nullable columns, tuples, and Scala 3 enums.

### 2.2 Working with Entities

Entities loaded from the database carry behavior — you can compare them by identity, track changes, and persist directly from instances. These extensions require a `given Repo` in scope and a database connection.

Here's the entity and setup we'll use throughout this section:

```scala
// file: models.scala - part of entity extensions
//> using scala 3.8.2
//> using dep "ma.chinespirit::parlance:$VERSION$"
//> using dep "com.h2database:h2:2.3.232"
// compile-only
import ma.chinespirit.parlance.*

@Table(SqlNameMapper.CamelToSnakeCase)
case class User(@Id id: Long, name: String, email: String) derives EntityMeta

given userRepo: Repo[User, User, Long] = Repo[User, User, Long]()

val ds =
  val d = org.h2.jdbcx.JdbcDataSource()
  d.setURL("jdbc:h2:mem:test")
  d
val xa = Transactor(H2, ds)
```

Compare entities by primary key (not by reference) and track changes via the identity map — Parlance remembers the state of each entity when it was loaded:

```scala
// file: example.sc - part of entity extensions
import ma.chinespirit.parlance.*

xa.transact:
  val alice = userRepo.findById(1L).get

  // Identity comparison — by primary key, not reference
  val aliceCopy = alice.copy(name = "Alice Updated")
  alice.is(aliceCopy)    // true — same PK
  alice.isNot(aliceCopy) // false

  // Change tracking
  alice.isDirty           // false — unchanged since load
  alice.isClean           // true
  val modified = alice.copy(email = "new@example.com")
  modified.isDirty        // true — differs from loaded snapshot
  modified.getChanges     // Map("email" -> ("old@example.com", "new@example.com"))
  modified.getOriginal    // the original alice as loaded from DB
```

Persist changes directly from entity instances — `save()` does a partial update (only changed fields) when the entity is tracked by the identity map:

```scala
// file: example.sc - part of entity extensions

xa.transact:
  val user = userRepo.findById(1L).get
  val modified = user.copy(email = "new@example.com")
  modified.save()         // partial update — only email changes
  modified.delete()       // delete by PK
  user.refresh()          // re-fetch from database
```

### 2.3 Repositories

Repositories provide the standard CRUD interface. `ImmutableRepo` handles reads, `Repo` adds writes.

#### Read Operations

```scala
//> using scala 3.8.2
//> using dep "ma.chinespirit::parlance:$VERSION$"
//> using dep "com.h2database:h2:2.3.232"
// compile-only
import ma.chinespirit.parlance.*

@Table(SqlNameMapper.CamelToSnakeCase)
case class User(@Id id: Long, name: String, email: String) derives EntityMeta

// Read-only repo
val userRepo = ImmutableRepo[User, Long]()

val ds = org.h2.jdbcx.JdbcDataSource()
ds.setURL("jdbc:h2:mem:test")
val xa = Transactor(H2, ds)

xa.connect:
  userRepo.count              // Long
  userRepo.existsById(1L)     // Boolean
  userRepo.findAll             // Vector[User]
  userRepo.findById(1L)       // Option[User]
  userRepo.findAllById(List(1L, 2L, 3L))  // Vector[User]
  userRepo.findOrFail(1L)     // User (throws if not found)

  // Pagination and chunking via scoped query builder
  userRepo.query.paginate(page = 1, perPage = 20)  // OffsetPage[User]
  userRepo.query.chunk(100)                         // Iterator[Vector[User]]
```

#### Insert, Update, Delete

For write operations, `Repo` takes a creator type, entity type, and ID type. We define the entity and setup once — subsequent code blocks in this section reuse them:

```scala
// file: models.scala - part of repo writes
//> using scala 3.8.2
//> using dep "ma.chinespirit::parlance:$VERSION$"
//> using dep "com.h2database:h2:2.3.232"
// compile-only
import ma.chinespirit.parlance.*

@SqlName("users")
@Table(SqlNameMapper.CamelToSnakeCase)
case class UserCreator(name: String, email: String) extends CreatorOf[User] derives DbCodec

@SqlName("users")
@Table(SqlNameMapper.CamelToSnakeCase)
case class User(@Id id: Long, name: String, email: String) derives EntityMeta

val userRepo = Repo[UserCreator, User, Long]()

val ds =
  val d = org.h2.jdbcx.JdbcDataSource()
  d.setURL("jdbc:h2:mem:test")
  d
val xa = Transactor(H2, ds)
```

Insert entities with `create` (returns the created entity via RETURNING) or `rawInsert` (fire-and-forget). Update with `update` (full) or `updatePartial` (diff-based). Delete individually or in bulk:

```scala
// file: example.sc - part of repo writes
import ma.chinespirit.parlance.*

xa.transact:
  // Insert and return the created entity (uses RETURNING)
  val alice: User = userRepo.create(UserCreator("Alice", "alice@example.com"))

  // Insert without returning
  userRepo.rawInsert(UserCreator("Bob", "bob@example.com"))

  // Batch insert
  userRepo.rawInsertAll(List(
    UserCreator("Charlie", "c@example.com"),
    UserCreator("Diana", "d@example.com")
  ))

  // Batch insert with returning
  val users: Vector[User] = userRepo.rawInsertAllReturning(List(
    UserCreator("Eve", "e@example.com"),
    UserCreator("Frank", "f@example.com")
  ))

  // Update — full row
  userRepo.update(alice.copy(name = "Alice Smith"))

  // Partial update — only changed fields are sent to DB
  val original = userRepo.findById(alice.id).get
  val modified = original.copy(email = "newemail@example.com")
  userRepo.updatePartial(original, modified)

  // Batch update
  userRepo.updateAll(List(
    alice.copy(name = "A"),
    users.head.copy(name = "B")
  ))

  // Delete
  userRepo.delete(alice)
  userRepo.deleteById(2L)
  userRepo.deleteAll(users)
  userRepo.deleteAllById(List(3L, 4L))
  userRepo.truncate()
```

#### Find-or-Create and Update-or-Create

These operations run within a transaction to avoid races:

```scala
// file: example.sc - part of repo writes

xa.transact:
  // Find existing or create new
  val user: User = userRepo.firstOrCreate(
    sql"email = ${"alice@example.com"}".unsafeAsWhere,
    UserCreator("Alice", "alice@example.com")
  )

  // Find and update, or create if not found
  val updated: User = userRepo.updateOrCreate(
    sql"email = ${"bob@example.com"}".unsafeAsWhere,
    UserCreator("Bob", "bob@example.com"),
    existing => existing.copy(name = "Bob Updated")
  )
```

#### Smart Save and Upsert

When the entity type serves as its own creator (no auto-generated columns), `save` inspects the identity map to decide the most efficient persistence strategy:

1. If the entity was loaded in the current session → partial update (only changed fields)
2. If not tracked → checks the database by primary key; if found, partial update
3. If not in the database → inserts

```scala
// file: models.scala - part of repo upsert
//> using scala 3.8.2
//> using dep "ma.chinespirit::parlance:$VERSION$"
//> using dep "com.h2database:h2:2.3.232"
// compile-only
import ma.chinespirit.parlance.*

@Table(SqlNameMapper.CamelToSnakeCase)
case class User(@Id id: Long, name: String, email: String) derives EntityMeta

val userRepo = Repo[User, User, Long]()

val ds =
  val d = org.h2.jdbcx.JdbcDataSource()
  d.setURL("jdbc:h2:mem:test")
  d
val xa = Transactor(H2, ds)
```

```scala
// file: example.sc - part of repo upsert
import ma.chinespirit.parlance.*

xa.transact:
  // Load, modify, save — only changed fields are updated
  val user = userRepo.findById(1L).get
  userRepo.save(user.copy(name = "Updated"))

  // Untracked entity — checks DB, then inserts or updates
  userRepo.save(User(99L, "New User", "new@example.com"))
```

For conflict-based upsert, use `insertOnConflict` to specify the conflict target and action:

```scala
// file: example.sc - part of repo upsert

xa.transact:
  // Insert or do nothing on any conflict
  userRepo.insertOnConflict(
    User(1L, "Alice", "a@example.com"),
    ConflictTarget.AnyConflict,
    ConflictAction.DoNothing
  )

  // Insert or update all columns on conflict
  userRepo.insertOnConflictUpdateAll(
    User(1L, "Alice Updated", "a@example.com"),
    ConflictTarget.AnyConflict
  )

  // Batch insert, skipping conflicting rows
  val inserted: Int = userRepo.insertAllIgnoring(List(
    User(1L, "Alice", "a@example.com"),   // skipped if exists
    User(2L, "Bob", "b@example.com")
  ))
```

#### Scoped Queries

Every repository exposes a query builder through `.query` that automatically applies all registered scopes. Use `.queryUnscoped` or `.queryWithout[S]` to bypass them:

```scala
//> using scala 3.8.2
//> using dep "ma.chinespirit::parlance:$VERSION$"
//> using dep "com.h2database:h2:2.3.232"
// compile-only
import ma.chinespirit.parlance.*
import scala.reflect.ClassTag

class ActiveOnly[E] extends Scope[E]:
  override def conditions(meta: TableMeta[E]): Vector[WhereFrag] =
    Vector(sql"status = ${"active"}".unsafeAsWhere)
  override def key: ClassTag[?] = scala.reflect.classTag[ActiveOnly[?]]

@Table(SqlNameMapper.CamelToSnakeCase)
case class User(@Id id: Long, name: String, status: String) derives EntityMeta

val userRepo = Repo[User, User, Long](
  injectedScopes = Vector(ActiveOnly[User]())
)

val ds = org.h2.jdbcx.JdbcDataSource()
ds.setURL("jdbc:h2:mem:test")
val xa = Transactor(H2, ds)

xa.connect:
  // All three use the query builder but with different scope application:
  userRepo.query.run()                      // WHERE status = 'active'
  userRepo.queryUnscoped.run()              // no scope filtering
  userRepo.queryWithout[ActiveOnly[?]].run() // removes just ActiveOnly
```

### 2.4 Relationships

Relationships are declared once on entity companion objects and used everywhere — for joins, eager loading, lazy loading, and existence checks.

#### Defining Relationships

```scala
//> using scala 3.8.2
//> using dep "ma.chinespirit::parlance:$VERSION$"
// compile-only
import ma.chinespirit.parlance.*

@Table(SqlNameMapper.CamelToSnakeCase)
case class Author(@Id id: Long, name: String) derives EntityMeta
@Table(SqlNameMapper.CamelToSnakeCase)
case class Book(@Id id: Long, title: String, authorId: Long) derives EntityMeta
@Table(SqlNameMapper.CamelToSnakeCase)
case class Tag(@Id id: Long, name: String) derives EntityMeta
@Table(SqlNameMapper.CamelToSnakeCase)
case class Profile(@Id id: Long, authorId: Long, bio: String) derives EntityMeta

object Author:
  // One author has many books (FK on book side)
  val books = Relationship.hasMany[Author, Book](_.id, _.authorId)
  // One author has one profile
  val profile = Relationship.hasOne[Author, Profile](_.id, _.authorId)

object Book:
  // Each book belongs to one author (FK on book side)
  val author = Relationship.belongsTo[Book, Author](_.authorId, _.id)
  // Many-to-many: books have many tags via a pivot table
  val tags = Relationship.belongsToMany[Book, Tag]("book_tags", "book_id", "tag_id")
```

`hasManyThrough` and `hasOneThrough` model indirect relationships via an intermediate table:

```scala
//> using scala 3.8.2
//> using dep "ma.chinespirit::parlance:$VERSION$"
// compile-only
import ma.chinespirit.parlance.*

@Table(SqlNameMapper.CamelToSnakeCase)
case class Country(@Id id: Long, name: String) derives EntityMeta
@Table(SqlNameMapper.CamelToSnakeCase)
case class User(@Id id: Long, name: String, countryId: Long) derives EntityMeta
@Table(SqlNameMapper.CamelToSnakeCase)
case class Post(@Id id: Long, title: String, userId: Long) derives EntityMeta

object Country:
  // Country -> User -> Post (country has many posts through users)
  val posts = Relationship.hasManyThrough[Country, User, Post](_.countryId, _.userId)

@Table(SqlNameMapper.CamelToSnakeCase)
case class Team(@Id id: Long, name: String) derives EntityMeta
@Table(SqlNameMapper.CamelToSnakeCase)
case class Manager(@Id id: Long, name: String, teamId: Long) derives EntityMeta
@Table(SqlNameMapper.CamelToSnakeCase)
case class Office(@Id id: Long, address: String, managerId: Long) derives EntityMeta

object Team:
  // Team -> Manager -> Office (each team has one office through its manager)
  val office = Relationship.hasOneThrough[Team, Manager, Office](_.teamId, _.managerId)
```

Chain relationships with the `via` operator:

```scala
//> using scala 3.8.2
//> using dep "ma.chinespirit::parlance:$VERSION$"
// compile-only
import ma.chinespirit.parlance.*

@Table(SqlNameMapper.CamelToSnakeCase)
case class Author(@Id id: Long, name: String) derives EntityMeta
@Table(SqlNameMapper.CamelToSnakeCase)
case class Book(@Id id: Long, title: String, authorId: Long) derives EntityMeta
@Table(SqlNameMapper.CamelToSnakeCase)
case class Review(@Id id: Long, bookId: Long, rating: Int) derives EntityMeta

object Author:
  val books = Relationship.hasMany[Author, Book](_.id, _.authorId)

object Book:
  val author = Relationship.belongsTo[Book, Author](_.authorId, _.id)
  val reviews = Relationship.hasMany[Book, Review](_.id, _.bookId)

// All reviews for a given author's books:
// Author -> hasMany Books -> hasMany Reviews
val authorReviews = Book.reviews via Author.books
```

#### Eager Loading, Lazy Loading, and Counted Relationships

All three loading patterns share the same entity and relationship definitions:

```scala
// file: models.scala - part of relationship loading
//> using scala 3.8.2
//> using dep "ma.chinespirit::parlance:$VERSION$"
//> using dep "com.h2database:h2:2.3.232"
// compile-only
import ma.chinespirit.parlance.*

@Table(SqlNameMapper.CamelToSnakeCase)
case class Author(@Id id: Long, name: String) derives EntityMeta
@Table(SqlNameMapper.CamelToSnakeCase)
case class Book(@Id id: Long, title: String, authorId: Long) derives EntityMeta
@Table(SqlNameMapper.CamelToSnakeCase)
case class Review(@Id id: Long, bookId: Long, rating: Int) derives EntityMeta

object Author:
  val books = Relationship.hasMany[Author, Book](_.id, _.authorId)

object Book:
  val author = Relationship.belongsTo[Book, Author](_.authorId, _.id)
  val reviews = Relationship.hasMany[Book, Review](_.id, _.bookId)

val ds =
  val d = org.h2.jdbcx.JdbcDataSource()
  d.setURL("jdbc:h2:mem:test")
  d
val xa = Transactor(H2, ds)
```

**Eager loading** fetches related entities in batches, avoiding the N+1 query problem:

```scala
// file: example.sc - part of relationship loading
import ma.chinespirit.parlance.*

xa.connect:
  // Load authors with their books — 2 queries total (not N+1)
  val results: Vector[(Author, Vector[Book])] =
    QueryBuilder.from[Author]
      .withRelated(Author.books)
      .run()

  // Chain multiple eager loads
  val withBooksAndReviews: Vector[(Author, Vector[Book], Vector[Review])] =
    QueryBuilder.from[Author]
      .withRelated(Author.books)
      .withRelated(Book.reviews via Author.books)
      .run()

  // Constrained eager load — only high-rated reviews
  val withGoodReviews: Vector[(Book, Vector[Review])] =
    QueryBuilder.from[Book]
      .withRelated(Book.reviews)(_.rating >= 4)
      .run()
```

**Lazy loading** fetches related entities on demand from an entity instance:

```scala
// file: example.sc - part of relationship loading

xa.connect:
  val author = ImmutableRepo[Author, Long]().findById(1L).get

  // Load all related books
  val books: Vector[Book] = author.load(Author.books)

  // Load single related entity
  val book = ImmutableRepo[Book, Long]().findById(1L).get
  val bookAuthor: Option[Author] = book.loadOne(Book.author)
```

**Counted relationships** include the count of related entities without loading them:

```scala
// file: example.sc - part of relationship loading

xa.connect:
  // Each result is (Author, Long) where Long is the book count
  val authorsWithCount: Vector[(Author, Long)] =
    QueryBuilder.from[Author]
      .withCount(Author.books)
      .run()

  // Constrained count — only count books with matching titles
  val withFilteredCount: Vector[(Author, Long)] =
    QueryBuilder.from[Author]
      .withCount(Author.books)(_.title.like("Published%"))
      .run()
```

#### Pivot Table Management

For many-to-many relationships, manage the pivot table directly:

```scala
//> using scala 3.8.2
//> using dep "ma.chinespirit::parlance:$VERSION$"
//> using dep "com.h2database:h2:2.3.232"
// compile-only
import ma.chinespirit.parlance.*

@Table(SqlNameMapper.CamelToSnakeCase)
case class User(@Id id: Long, name: String) derives EntityMeta
@Table(SqlNameMapper.CamelToSnakeCase)
case class Role(@Id id: Long, name: String) derives EntityMeta

object User:
  val roles = Relationship.belongsToMany[User, Role]("user_roles", "user_id", "role_id")

val ds = org.h2.jdbcx.JdbcDataSource()
ds.setURL("jdbc:h2:mem:test")
val xa = Transactor(H2, ds)

xa.transact:
  val alice = User(1L, "Alice")
  val admin = Role(1L, "admin")
  val editor = Role(2L, "editor")

  // Add associations
  User.roles.attach(alice, admin)         // insert pivot row
  User.roles.attach(alice, editor)

  // Remove associations
  User.roles.detach(alice, editor)        // remove pivot row
  User.roles.detachAll(alice)             // remove all roles for alice

  // Sync — atomically replace the set of associations
  val result: SyncResult = User.roles.sync(alice, List(admin, editor))
  // result.attached, result.detached, result.unchanged
```

When the pivot table has extra columns beyond the two foreign keys, define a pivot entity and use `.withPivot` to eager-load pivot data alongside related entities:

```scala
//> using scala 3.8.2
//> using dep "ma.chinespirit::parlance:$VERSION$"
//> using dep "com.h2database:h2:2.3.232"
// compile-only
import ma.chinespirit.parlance.*

@Table(SqlNameMapper.CamelToSnakeCase)
case class User(@Id id: Long, name: String) derives EntityMeta
@Table(SqlNameMapper.CamelToSnakeCase)
case class Role(@Id id: Long, name: String) derives EntityMeta

// Pivot entity — models the join table with extra columns
@SqlName("user_roles")
@Table(SqlNameMapper.CamelToSnakeCase)
case class UserRole(@Id id: Long, userId: Long, roleId: Long, grantedAt: String) derives EntityMeta

object User:
  val roles = Relationship.belongsToMany[User, Role]("user_roles", "user_id", "role_id")
  // Attach pivot entity for eager loading with pivot data
  val rolesWithPivot = roles.withPivot[UserRole]

val ds = org.h2.jdbcx.JdbcDataSource()
ds.setURL("jdbc:h2:mem:test")
val xa = Transactor(H2, ds)

xa.connect:
  // Eager load roles WITH pivot data — each result includes (Role, UserRole) pairs
  val usersWithRoles = QueryBuilder.from[User]
    .withRelatedAndPivot(User.rolesWithPivot)
    .run()
```

### 2.5 Scopes

Scopes are reusable query modifiers that apply automatically to every repository query. They are the mechanism behind soft deletes, automatic timestamps, and any custom filtering logic.

A scope can contribute WHERE conditions, ORDER BY clauses, SET clauses for INSERT/UPDATE, and DELETE rewriting. Define the entity and scope classes:

```scala
// file: models.scala - part of custom scopes
//> using scala 3.8.2
//> using dep "ma.chinespirit::parlance:$VERSION$"
//> using dep "com.h2database:h2:2.3.232"
// compile-only
import ma.chinespirit.parlance.*
import scala.reflect.ClassTag

@Table(SqlNameMapper.CamelToSnakeCase)
case class Article(@Id id: Long, title: String, status: String) derives EntityMeta

// A scope that filters to only published articles
class PublishedOnly extends Scope[Article]:
  override def conditions(meta: TableMeta[Article]): Vector[WhereFrag] =
    Vector(sql"status = ${"published"}".unsafeAsWhere)
  override def key: ClassTag[?] = scala.reflect.classTag[PublishedOnly]

// A scope that orders by newest first
class NewestFirst extends Scope[Article]:
  override def orderings(meta: TableMeta[Article]): Vector[OrderByFrag] =
    Vector(sql"${meta.columns.find(_.scalaName == "id").get} DESC".unsafeAsOrderBy)
  override def key: ClassTag[?] = scala.reflect.classTag[NewestFirst]

// Mixin trait for scope injection
trait PublishedScope extends HasScopes[Article]:
  self: ImmutableRepo[Article, ?] =>
  abstract override def finalScopes: Vector[Scope[Article]] =
    super.finalScopes :+ PublishedOnly()
```

Scopes can be passed via the constructor or mixed in via a trait:

```scala
// file: example.sc - part of custom scopes
import ma.chinespirit.parlance.*

// Via constructor
val articleRepo = Repo[Article, Article, Long](
  injectedScopes = Vector(PublishedOnly(), NewestFirst())
)

// Via mixin trait
val articleRepo2 = new Repo[Article, Article, Long] with PublishedScope
```

### 2.6 Automatic Timestamps

Mark fields with `@createdAt` and `@updatedAt` to have them automatically populated. Mix in the `Timestamps` trait on your repository:

```scala
//> using scala 3.8.2
//> using dep "ma.chinespirit::parlance:$VERSION$"
//> using dep "com.h2database:h2:2.3.232"
// compile-only
import ma.chinespirit.parlance.*
import java.time.OffsetDateTime

@SqlName("articles")
@Table(SqlNameMapper.CamelToSnakeCase)
case class ArticleCreator(title: String) extends CreatorOf[Article] derives DbCodec

@SqlName("articles")
@Table(SqlNameMapper.CamelToSnakeCase)
case class Article(
  @Id id: Long,
  title: String,
  @createdAt createdAt: OffsetDateTime,  // set on insert
  @updatedAt updatedAt: OffsetDateTime   // set on insert, bumped on update
) derives EntityMeta, HasCreatedAt, HasUpdatedAt

// Timestamps mixin enables auto-population
val articleRepo = new Repo[ArticleCreator, Article, Long]
  with Timestamps[ArticleCreator, Article, Long]

val ds = org.h2.jdbcx.JdbcDataSource()
ds.setURL("jdbc:h2:mem:test")
val xa = Transactor(H2, ds)

xa.transact:
  // create sets both createdAt and updatedAt to CURRENT_TIMESTAMP
  val article = articleRepo.create(ArticleCreator("Hello World"))

  // update bumps only updatedAt
  articleRepo.update(article.copy(title = "Updated Title"))

  // touch bumps updatedAt without changing other fields
  articleRepo.touch(article)
```

Timestamps are implemented as scopes internally, so they compose freely with other scopes and soft deletes.

### 2.7 Soft Deletes

Add a `@deletedAt` field and mix in `SoftDeletes` to make `delete()` set a timestamp instead of removing the row. All queries automatically exclude soft-deleted records.

Here's the entity and repo setup — the `given` with intersection type enables both repo-level and entity-level soft delete operations:

```scala
// file: models.scala - part of soft deletes
//> using scala 3.8.2
//> using dep "ma.chinespirit::parlance:$VERSION$"
//> using dep "com.h2database:h2:2.3.232"
// compile-only
import ma.chinespirit.parlance.*
import java.time.OffsetDateTime

@Table(SqlNameMapper.CamelToSnakeCase)
case class Article(
  @Id id: Long,
  title: String,
  @deletedAt deletedAt: Option[OffsetDateTime]
) derives EntityMeta, HasDeletedAt

given articleRepo: (Repo[Article, Article, Long] & SoftDeletes[Article, Article, Long]) =
  new Repo[Article, Article, Long] with SoftDeletes[Article, Article, Long]

val ds =
  val d = org.h2.jdbcx.JdbcDataSource()
  d.setURL("jdbc:h2:mem:test")
  d
val xa = Transactor(H2, ds)
```

Repository-level operations — soft delete, query including trashed records, restore, and force delete:

```scala
// file: example.sc - part of soft deletes
import ma.chinespirit.parlance.*

xa.transact:
  val article = articleRepo.findById(1L).get

  // Soft delete — sets deletedAt to CURRENT_TIMESTAMP
  articleRepo.delete(article)

  // Now invisible to normal queries
  articleRepo.findById(1L)            // None

  // Query including soft-deleted records
  articleRepo.withTrashed.run()       // all records
  articleRepo.onlyTrashed.run()       // only soft-deleted

  // Check status
  articleRepo.isTrashed(article)      // true

  // Restore — clears deletedAt
  articleRepo.restoreById(1L)

  // Permanent delete — actual DELETE FROM
  articleRepo.forceDeleteById(1L)
```

The same operations work directly on entity instances:

```scala
// file: example.sc - part of soft deletes

xa.transact:
  val article = articleRepo.findById(2L).get
  article.trashed       // false
  article.delete()      // soft delete
  article.restore()     // undelete
  article.forceDelete() // permanent delete
```

Soft deletes and timestamps compose — just mix in both (order doesn't matter):

```scala
//> using scala 3.8.2
//> using dep "ma.chinespirit::parlance:$VERSION$"
// compile-only
import ma.chinespirit.parlance.*
import java.time.OffsetDateTime

@SqlName("articles")
@Table(SqlNameMapper.CamelToSnakeCase)
case class ArticleCreator(title: String) extends CreatorOf[Article] derives DbCodec

@SqlName("articles")
@Table(SqlNameMapper.CamelToSnakeCase)
case class Article(
  @Id id: Long,
  title: String,
  @createdAt createdAt: OffsetDateTime,
  @updatedAt updatedAt: OffsetDateTime,
  @deletedAt deletedAt: Option[OffsetDateTime]
) derives EntityMeta, HasCreatedAt, HasUpdatedAt, HasDeletedAt

val articleRepo = new Repo[ArticleCreator, Article, Long]
  with Timestamps[ArticleCreator, Article, Long]
  with SoftDeletes[ArticleCreator, Article, Long]
```

### 2.8 Lifecycle Observers

Observers let you hook into entity create, update, and delete events. Implement the `RepoObserver` trait and attach observers to a repository:

```scala
//> using scala 3.8.2
//> using dep "ma.chinespirit::parlance:$VERSION$"
//> using dep "com.h2database:h2:2.3.232"
// compile-only
import ma.chinespirit.parlance.*

@SqlName("articles")
@Table(SqlNameMapper.CamelToSnakeCase)
case class ArticleCreator(title: String) extends CreatorOf[Article] derives DbCodec

@SqlName("articles")
@Table(SqlNameMapper.CamelToSnakeCase)
case class Article(@Id id: Long, title: String) derives EntityMeta

class AuditObserver extends RepoObserver[ArticleCreator, Article]:
  override def creating(entity: ArticleCreator | Article)(using DbCon[?]): Unit =
    println(s"Creating article...")

  override def created(entity: Article)(using DbCon[?]): Unit =
    println(s"Created article #${entity.id}: ${entity.title}")

  override def updating(entity: Article)(using DbCon[?]): Unit =
    println(s"Updating article #${entity.id}")

  override def updated(entity: Article)(using DbCon[?]): Unit =
    println(s"Updated article #${entity.id}")

  override def deleting(entity: Article)(using DbCon[?]): Unit =
    println(s"Deleting article #${entity.id}")

  override def deleted(entity: Article)(using DbCon[?]): Unit =
    println(s"Deleted article #${entity.id}")

val articleRepo = Repo[ArticleCreator, Article, Long](
  observers = Vector(AuditObserver())
)
```

When combined with `SoftDeletes`, additional hooks are available:

```scala
//> using scala 3.8.2
//> using dep "ma.chinespirit::parlance:$VERSION$"
// compile-only
import ma.chinespirit.parlance.*
import java.time.OffsetDateTime

@Table(SqlNameMapper.CamelToSnakeCase)
case class Article(
  @Id id: Long,
  title: String,
  @deletedAt deletedAt: Option[OffsetDateTime]
) derives EntityMeta, HasDeletedAt

class SoftDeleteObserver extends RepoObserver[Article, Article]:
  override def trashed(entity: Article)(using DbCon[?]): Unit =
    println(s"Article #${entity.id} was soft-deleted")

  override def forceDeleting(entity: Article)(using DbCon[?]): Unit =
    println(s"About to permanently delete article #${entity.id}")

  override def forceDeleted(entity: Article)(using DbCon[?]): Unit =
    println(s"Permanently deleted article #${entity.id}")

  override def restoring(entity: Article)(using DbCon[?]): Unit =
    println(s"Restoring article #${entity.id}")

  override def restored(entity: Article)(using DbCon[?]): Unit =
    println(s"Restored article #${entity.id}")

val articleRepo = new Repo[Article, Article, Long](
  observers = Vector(SoftDeleteObserver())
) with SoftDeletes[Article, Article, Long]
```

## 3. Query Builder

When repository methods aren't enough, drop into the Query Builder — a type-safe, compile-time verified query construction API. Column names are real, typed references derived from your entity definition; typos are compile errors, not runtime surprises.

### 3.1 Building Queries

All examples in this section share the same entity and setup:

```scala
// file: models.scala - part of query builder basics
//> using scala 3.8.2
//> using dep "ma.chinespirit::parlance:$VERSION$"
//> using dep "com.h2database:h2:2.3.232"
// compile-only
import ma.chinespirit.parlance.*

@Table(SqlNameMapper.CamelToSnakeCase)
case class User(
  @Id id: Long,
  firstName: Option[String],
  lastName: String,
  email: String,
  age: Int
) derives EntityMeta

val ds =
  val d = org.h2.jdbcx.JdbcDataSource()
  d.setURL("jdbc:h2:mem:test")
  d
val xa = Transactor(H2, ds)
```

Start from `QueryBuilder.from[E]` and chain filters, sorting, and limits. The lambda `_.fieldName` gives you a typed column reference — `_.age` is `ColRef[Int]`, so you can't accidentally compare it against a `String`:

```scala
// file: example.sc - part of query builder basics
import ma.chinespirit.parlance.*

xa.connect:
  // Simple filter
  QueryBuilder.from[User]
    .where(_.age > 18)
    .run()                        // Vector[User]

  // Equality
  QueryBuilder.from[User]
    .where(_.firstName === Some("Alice"))
    .firstOrFail()                // User (throws if empty)

  // Combine with && and ||
  QueryBuilder.from[User]
    .where(u => (u.age >= 18) && (u.lastName === "Smith"))
    .run()

  QueryBuilder.from[User]
    .where(u => (u.firstName === Some("Alice")) || (u.firstName === Some("Bob")))
    .run()
```

Column-to-column comparisons — compare two fields of the same type:

```scala
// file: example.sc - part of query builder basics

xa.connect:
  // email and lastName are both String — column-to-column operators
  // require matching types (===, !==, >, <, >=, <=)
  QueryBuilder.from[User]
    .where(u => u.email !== u.lastName)
    .run()
```

More operators — `in`, `between`, null checks, and string matching:

```scala
// file: example.sc - part of query builder basics

xa.connect:
  QueryBuilder.from[User].where(_.age.in(List(25, 30, 35))).run()
  QueryBuilder.from[User].where(_.age.notIn(List(25, 30))).run()
  QueryBuilder.from[User].where(_.age.between(18, 65)).run()
  QueryBuilder.from[User].where(_.firstName.isNull).run()
  QueryBuilder.from[User].where(_.firstName.isNotNull).run()
  QueryBuilder.from[User].where(_.lastName like "Sm%").run()
  QueryBuilder.from[User].where(_.lastName notLike "Sm%").run()
```

`orWhere` adds an OR branch to the predicate:

```scala
// file: example.sc - part of query builder basics

xa.connect:
  // WHERE age > 60 OR first_name = 'Alice'
  QueryBuilder.from[User]
    .where(_.age > 60)
    .orWhere(_.firstName === Some("Alice"))
    .run()
```

Sorting, pagination, and limits:

```scala
// file: example.sc - part of query builder basics

xa.connect:
  // Sort ascending (default)
  QueryBuilder.from[User].orderBy(_.age).run()

  // Sort descending with null ordering
  QueryBuilder.from[User].orderBy(_.firstName, SortOrder.Desc, NullOrder.Last).run()

  // Multiple orderBy calls accumulate
  QueryBuilder.from[User]
    .orderBy(_.lastName)
    .orderBy(_.age, SortOrder.Desc)
    .run()

  // Limit and offset
  QueryBuilder.from[User]
    .orderBy(_.id)
    .limit(10)
    .offset(20)
    .run()

  // Distinct
  QueryBuilder.from[User].distinct.run()
```

Terminal methods — different ways to execute or inspect a query:

```scala
// file: example.sc - part of query builder basics

xa.connect:
  val qb = QueryBuilder.from[User].where(_.age > 18)

  qb.run()             // Vector[User]
  qb.first()           // Option[User]
  qb.firstOrFail()     // User (throws if empty)
  qb.exists()          // Boolean
  qb.count()           // Long
  qb.count(_.email)    // Long — COUNT(email), excludes nulls

  // Aggregates
  qb.sum(_.age)        // Option[Int]
  qb.avg(_.age)        // Option[Double]
  qb.min(_.age)        // Option[Int]
  qb.max(_.age)        // Option[Int]

  // Inspect generated SQL without executing
  qb.build             // Frag — access .sqlString and .params
  qb.debugPrintSql     // prints SQL to stdout, returns the QB for chaining
```

#### Offset Pagination

```scala
// file: example.sc - part of query builder basics

xa.connect:
  val page = QueryBuilder.from[User]
    .orderBy(_.id)
    .paginate(page = 1, perPage = 20)

  page.items         // Vector[User]
  page.total         // Long — total matching rows
  page.totalPages    // Int
  page.hasNext       // Boolean
  page.hasPrev       // Boolean
```

#### Keyset Pagination

Cursor-based pagination for stable, high-performance paging — no offset drift:

```scala
// file: example.sc - part of query builder basics

xa.connect:
  // Single-column cursor: ascending by id
  val paginator = QueryBuilder.from[User]
    .keysetPaginate(20)(_.asc(_.id))

  val page1 = paginator.run()
  // page1.items, page1.hasMore, page1.nextKey

  // Next page: pass the cursor from the previous page
  if page1.hasMore then
    val page2 = paginator.after(page1.nextKey.get).run()

  // Multi-column cursor: score desc, then id asc as tiebreaker
  val multiPaginator = QueryBuilder.from[User]
    .keysetPaginate(20)(k => k.desc(_.age).asc(_.id))

  val mp1 = multiPaginator.run()
```

#### Batch Processing

```scala
// file: example.sc - part of query builder basics

xa.connect:
  // Process all users in batches of 100
  QueryBuilder.from[User]
    .orderBy(_.id)
    .chunk(100)                 // Iterator[Vector[User]]
    .foreach: batch =>
      batch.foreach(u => println(u.email))
```

### 3.2 Projections & Aggregations

Select specific columns and use aggregate functions with GROUP BY / HAVING. The projection uses Scala 3 named tuples — field names become typed column references in subsequent calls:

```scala
// file: models.scala - part of projections
//> using scala 3.8.2
//> using dep "ma.chinespirit::parlance:$VERSION$"
//> using dep "com.h2database:h2:2.3.232"
// compile-only
import ma.chinespirit.parlance.*

@Table(SqlNameMapper.CamelToSnakeCase)
case class Order(
  @Id id: Long,
  customer: String,
  amount: Int,
  status: String
) derives EntityMeta

val ds =
  val d = org.h2.jdbcx.JdbcDataSource()
  d.setURL("jdbc:h2:mem:test")
  d
val xa = Transactor(H2, ds)
```

Select a subset of columns — the result type is the named tuple you defined, not the full entity:

```scala
// file: example.sc - part of projections
import ma.chinespirit.parlance.*

xa.connect:
  // Select specific columns
  val results: Vector[(customer: String, amount: Int)] =
    QueryBuilder.from[Order]
      .select(c => (customer = c.customer, amount = c.amount))
      .run()

  // WHERE applies before the projection
  QueryBuilder.from[Order]
    .where(_.status === "paid")
    .select(c => (customer = c.customer, amount = c.amount))
    .run()
```

Aggregate functions via `Expr` — use them inside `.select` alongside regular columns:

```scala
// file: example.sc - part of projections

xa.connect:
  // COUNT with GROUP BY
  QueryBuilder.from[Order]
    .select(c => (status = c.status, cnt = Expr.count))
    .groupBy(_.status)
    .run()            // Vector[(status: String, cnt: Long)]

  // SUM with GROUP BY
  QueryBuilder.from[Order]
    .select(c => (customer = c.customer, total = Expr.sum(c.amount)))
    .groupBy(_.customer)
    .orderBy(_.customer)
    .run()            // Vector[(customer: String, total: Int)]

  // MIN and MAX
  QueryBuilder.from[Order]
    .select(c => (customer = c.customer, minAmt = Expr.min(c.amount), maxAmt = Expr.max(c.amount)))
    .groupBy(_.customer)
    .orderBy(_.customer)
    .run()
```

`HAVING` filters groups after aggregation — it uses the projected column names, so `_.total` refers to the aggregate you defined:

```scala
// file: example.sc - part of projections

xa.connect:
  QueryBuilder.from[Order]
    .select(c => (customer = c.customer, total = Expr.sum(c.amount)))
    .groupBy(_.customer)
    .having(_.total > 250)
    .run()

  // Multiple GROUP BY columns
  QueryBuilder.from[Order]
    .select(c => (customer = c.customer, status = c.status, cnt = Expr.count))
    .groupBy(_.customer)
    .groupBy(_.status)
    .orderBy(_.customer)
    .orderBy(_.status)
    .run()
```

Projected queries also support `.distinct`, `.limit`, `.offset`, `.first`, and `.orderBy`:

```scala
// file: example.sc - part of projections

xa.connect:
  QueryBuilder.from[Order]
    .select(c => (customer = c.customer, status = c.status))
    .distinct
    .run()

  QueryBuilder.from[Order]
    .select(c => (customer = c.customer, amount = c.amount))
    .orderBy(_.amount, SortOrder.Desc)
    .limit(3)
    .first()         // Option[(customer: String, amount: Int)]
```

For arbitrary SQL expressions that Parlance doesn't model directly, use `Expr.raw`:

```scala
// file: example.sc - part of projections

xa.connect:
  QueryBuilder.from[Order]
    .select(c => (customer = c.customer, upperStatus = Expr.raw[String]("UPPER(status)", "upper_status")))
    .run()
```

### 3.3 Joins

Compose queries across multiple tables using relationship definitions. Joins produce tuples — the result type grows as you add more joins.

All examples in this section share these entities:

```scala
// file: models.scala - part of joins
//> using scala 3.8.2
//> using dep "ma.chinespirit::parlance:$VERSION$"
//> using dep "com.h2database:h2:2.3.232"
// compile-only
import ma.chinespirit.parlance.*

@Table(SqlNameMapper.CamelToSnakeCase)
case class Author(@Id id: Long, name: String, countryId: Long) derives EntityMeta
@Table(SqlNameMapper.CamelToSnakeCase)
case class Book(@Id id: Long, title: String, authorId: Long) derives EntityMeta
@Table(SqlNameMapper.CamelToSnakeCase)
case class Country(@Id id: Long, name: String) derives EntityMeta

object Author:
  val books = Relationship.hasMany[Author, Book](_.id, _.authorId)
  val country = Relationship.belongsTo[Author, Country](_.countryId, _.id)

object Book:
  val author = Relationship.belongsTo[Book, Author](_.authorId, _.id)

val ds =
  val d = org.h2.jdbcx.JdbcDataSource()
  d.setURL("jdbc:h2:mem:test")
  d
val xa = Transactor(H2, ds)
```

Inner join — only rows where both sides match. The result type becomes a tuple `(Book, Author)`:

```scala
// file: example.sc - part of joins
import ma.chinespirit.parlance.*

xa.connect:
  val booksWithAuthors: Vector[(Book, Author)] =
    QueryBuilder.from[Book]
      .join(Book.author)
      .run()
```

Left join — keeps all rows from the left table, right side becomes `Option`:

```scala
// file: example.sc - part of joins

xa.connect:
  val allBooks: Vector[(Book, Option[Author])] =
    QueryBuilder.from[Book]
      .leftJoin(Book.author)
      .run()
```

Use `.of[T]` for inner joins and `.ofLeft[T]` for left joins to access columns from a specific table inside `.select` and `.where`:

```scala
// file: example.sc - part of joins

xa.connect:
  // Project columns from both tables
  val jq = QueryBuilder.from[Book].join(Book.author)
  jq.select(j =>
      (
        title = j.of[Book].title,
        author = j.of[Author].name
      )
    )
    .orderBy(_.title)
    .run()          // Vector[(title: String, author: String)]

  // WHERE on joined columns
  jq.where(jq.of[Author].name === "Tolkien")
    .run()
```

Chain multiple joins — each `.join` appends to the tuple. Parlance auto-aliases tables (`t0`, `t1`, `t2`, ...):

```scala
// file: example.sc - part of joins

xa.connect:
  // 3-table join: Book -> Author -> Country
  QueryBuilder.from[Book]
    .join(Book.author)
    .join(Author.country)
    .select(j =>
      (
        title = j.of[Book].title,
        author = j.of[Author].name,
        country = j.of[Country].name
      )
    )
    .orderBy(_.title)
    .run()

  // GROUP BY + COUNT across joined tables
  QueryBuilder.from[Book]
    .join(Book.author)
    .select(j =>
      (
        author = j.of[Author].name,
        bookCount = Expr.count
      )
    )
    .groupBy(_.author)
    .having(_.bookCount > 1L)
    .orderBy(_.author)
    .run()
```

Joined queries support `.where`, `.orderBy`, `.limit`, `.offset`, `.select`, `.count`, `.exists`, and `.debugPrintSql`. Scope conditions are automatically merged into the JOIN ON clause (not WHERE) for correct LEFT JOIN semantics.

### 3.4 Relationship Existence Filtering

Filter entities based on the existence or count of related records — generates WHERE EXISTS subqueries without loading the related data.

All examples use these entities:

```scala
// file: models.scala - part of existence filtering
//> using scala 3.8.2
//> using dep "ma.chinespirit::parlance:$VERSION$"
//> using dep "com.h2database:h2:2.3.232"
// compile-only
import ma.chinespirit.parlance.*

@Table(SqlNameMapper.CamelToSnakeCase)
case class Author(@Id id: Long, name: String) derives EntityMeta
@Table(SqlNameMapper.CamelToSnakeCase)
case class Book(@Id id: Long, title: String, authorId: Long) derives EntityMeta
@Table(SqlNameMapper.CamelToSnakeCase)
case class Review(@Id id: Long, bookId: Long, score: Int) derives EntityMeta

object Author:
  val books = Relationship.hasMany[Author, Book](_.id, _.authorId)

object Book:
  val reviews = Relationship.hasMany[Book, Review](_.id, _.bookId)

val ds =
  val d = org.h2.jdbcx.JdbcDataSource()
  d.setURL("jdbc:h2:mem:test")
  d
val xa = Transactor(H2, ds)
```

Basic existence checks — `whereHas` keeps rows with related records, `doesntHave` keeps rows without:

```scala
// file: example.sc - part of existence filtering
import ma.chinespirit.parlance.*

xa.connect:
  // Only authors who have at least one book
  QueryBuilder.from[Author]
    .whereHas(Author.books)
    .orderBy(_.name)
    .run()

  // Only authors with no books
  QueryBuilder.from[Author]
    .doesntHave(Author.books)
    .run()
```

Constrained existence — add a condition on the related table inside the subquery:

```scala
// file: example.sc - part of existence filtering

xa.connect:
  // Authors who have a book titled "Dune"
  QueryBuilder.from[Author]
    .whereHas(Author.books)(_.title === "Dune")
    .run()

  // Authors who have books matching a pattern
  QueryBuilder.from[Author]
    .whereHas(Author.books)(_.title like "The%")
    .run()
```

Nest existence checks for deep filtering — the lambda receives a `SubQuery` that supports its own `whereHas`:

```scala
// file: example.sc - part of existence filtering

xa.connect:
  // Authors who have books with high-scoring reviews
  QueryBuilder.from[Author]
    .whereHas(Author.books)(_.whereHas(Book.reviews)(_.score >= 4))
    .orderBy(_.name)
    .run()

  // Combine column conditions with nested existence using && and ||
  QueryBuilder.from[Author]
    .whereHas(Author.books)(sq =>
      sq.title.like("The%") && sq.whereHas(Book.reviews)(_.score >= 4)
    )
    .run()

  QueryBuilder.from[Author]
    .whereHas(Author.books)(sq =>
      (sq.title === "Dune") || sq.whereHas(Book.reviews)(_.score >= 5)
    )
    .run()
```

OR variants — `orWhereHas` and `orDoesntHave` add an OR branch:

```scala
// file: example.sc - part of existence filtering

xa.connect:
  // Authors named "Rowling" OR who have any books
  QueryBuilder.from[Author]
    .where(_.name === "Rowling")
    .orWhereHas(Author.books)
    .orderBy(_.name)
    .run()
```

Count-based filtering with `has` — filter by the *number* of related records:

```scala
// file: example.sc - part of existence filtering

xa.connect:
  // Authors with 2 or more books
  QueryBuilder.from[Author]
    .has(Author.books)(_ >= 2)
    .orderBy(_.name)
    .run()

  // Authors with exactly 0 books (same as doesntHave but explicit)
  QueryBuilder.from[Author]
    .has(Author.books)(_ === 0)
    .run()

  // Constrained count — only count books matching a pattern
  QueryBuilder.from[Author]
    .has(Author.books, _.title like "The%")(_ >= 1)
    .run()
```

### 3.5 Inline Updates & Deletes

Modify data directly from the query builder — useful for bulk operations that don't need entity instances.

```scala
// file: models.scala - part of inline mutations
//> using scala 3.8.2
//> using dep "ma.chinespirit::parlance:$VERSION$"
//> using dep "com.h2database:h2:2.3.232"
// compile-only
import ma.chinespirit.parlance.*

@Table(SqlNameMapper.CamelToSnakeCase)
case class Counter(
  @Id id: Long,
  name: String,
  score: Int,
  viewCount: Long,
  status: String
) derives EntityMeta

val ds =
  val d = org.h2.jdbcx.JdbcDataSource()
  d.setURL("jdbc:h2:mem:test")
  d
val xa = Transactor(H2, ds)
```

Type-safe update with named tuples — field names are checked at compile time:

```scala
// file: example.sc - part of inline mutations
import ma.chinespirit.parlance.*

xa.transact:
  // Update matching rows — returns affected row count
  val affected: Int = QueryBuilder.from[Counter]
    .where(_.id === 1L)
    .update((name = "Updated", score = 42))

  // Update with WHERE filter
  QueryBuilder.from[Counter]
    .where(_.status === "active")
    .update((score = 999))
```

Increment and decrement — compile-time enforced to only work on numeric columns:

```scala
// file: example.sc - part of inline mutations

xa.transact:
  // Increment by a specific amount
  QueryBuilder.from[Counter]
    .where(_.id === 1L)
    .increment(_.viewCount, 5L)

  // Increment by 1 (default)
  QueryBuilder.from[Counter]
    .where(_.id === 2L)
    .increment(_.viewCount)

  // Decrement
  QueryBuilder.from[Counter]
    .where(_.id === 3L)
    .decrement(_.viewCount, 10L)
```

Delete matching rows:

```scala
// file: example.sc - part of inline mutations

xa.transact:
  // Delete with WHERE
  QueryBuilder.from[Counter]
    .where(_.status === "draft")
    .delete()                    // Int — affected row count

  // Delete all (careful!)
  QueryBuilder.from[Counter].delete()
```

### 3.6 Row Locking

Pessimistic locking within transactions. Both the connection type (`DbTx`) and the database capability (`SupportsRowLocks` / `SupportsForShare`) are enforced at compile time — you can't accidentally lock outside a transaction or on a database that doesn't support it.

```scala
//> using scala 3.8.2
//> using dep "ma.chinespirit::parlance:$VERSION$"
//> using dep "com.h2database:h2:2.3.232"
// compile-only
import ma.chinespirit.parlance.*

@Table(SqlNameMapper.CamelToSnakeCase)
case class Account(@Id id: Long, name: String, balance: Int) derives EntityMeta

val ds =
  val d = org.h2.jdbcx.JdbcDataSource()
  d.setURL("jdbc:h2:mem:test")
  d
val xa = Transactor(H2, ds)

xa.transact:                  // DbTx required — compile error in xa.connect
  // Exclusive lock — SELECT ... FOR UPDATE
  val account = QueryBuilder.from[Account]
    .where(_.id === 1L)
    .lockForUpdate
    .first()

  // Shared lock — SELECT ... FOR SHARE
  val accounts = QueryBuilder.from[Account]
    .where(_.balance > 0)
    .orderBy(_.balance)
    .limit(10)
    .forShare
    .run()
```

## 4. SQL Interpolation

The deepest escape hatch — write raw SQL with compile-time parameter safety. Maximum control when the query builder or repositories can't express what you need.

All examples in this section share the same setup:

```scala
// file: models.scala - part of sql interpolation
//> using scala 3.8.2
//> using dep "ma.chinespirit::parlance:$VERSION$"
//> using dep "com.h2database:h2:2.3.232"
// compile-only
import ma.chinespirit.parlance.*

@Table(SqlNameMapper.CamelToSnakeCase)
case class User(@Id id: Long, name: String, email: String, age: Int) derives EntityMeta

val ds =
  val d = org.h2.jdbcx.JdbcDataSource()
  d.setURL("jdbc:h2:mem:test")
  d
val xa = Transactor(H2, ds)
```

### 4.1 Basics

Values interpolated into `sql""` become `?` parameters automatically — no SQL injection, no manual escaping:

```scala
// file: example.sc - part of sql interpolation
import ma.chinespirit.parlance.*

xa.connect:
  val name = "Alice"
  val minAge = 18

  // Values become ? parameters
  sql"SELECT * FROM users WHERE name = $name AND age > $minAge"
    .query[User]
    .run()                    // Vector[User]

  // Single result
  sql"SELECT * FROM users WHERE name = $name"
    .query[User]
    .run()
    .headOption               // Option[User]
```

Terminal methods determine what the SQL does:

```scala
// file: example.sc - part of sql interpolation

xa.connect:
  // Read rows
  sql"SELECT * FROM users".query[User].run()            // Vector[User]

  // Read a scalar
  sql"SELECT count(*) FROM users".query[Int].run().head  // Int

  // Read tuples
  sql"SELECT name, age FROM users"
    .query[(String, Int)]
    .run()                                               // Vector[(String, Int)]

xa.transact:
  // Write rows — returns affected count
  sql"INSERT INTO users (name, email, age) VALUES (${"Bob"}, ${"bob@example.com"}, ${30})"
    .update.run()                                        // Int

  // Write with RETURNING (Postgres, H2, Oracle)
  sql"INSERT INTO users (name, email, age) VALUES (${"Eve"}, ${"eve@example.com"}, ${25})"
    .returning[User].run()                               // Vector[User]

  // returningKeys — for databases that use getGeneratedKeys instead of RETURNING
  sql"INSERT INTO users (name, email, age) VALUES (${"Frank"}, ${"frank@example.com"}, ${28})"
    .returningKeys[Long]("id").run()                     // Vector[Long]
```

Multiline SQL with `stripMargin` — works like Scala's built-in, but on the `Frag`:

```scala
// file: example.sc - part of sql interpolation

xa.connect:
  val minAge = 21
  sql"""SELECT *
       |FROM users
       |WHERE age > $minAge
       |ORDER BY name""".stripMargin
    .query[User]
    .run()
```

### 4.2 Fragment Composition

Embed one `sql""` fragment inside another — parameters merge automatically in the correct order:

```scala
// file: example.sc - part of sql interpolation

xa.connect:
  val activeFilter = sql"age >= ${18}"
  val nameFilter = sql"name != ${"admin"}"

  // Fragments compose — params from both are merged
  sql"SELECT * FROM users WHERE $activeFilter AND $nameFilter"
    .query[User]
    .run()

  // Build filters conditionally
  def userQuery(nameOpt: Option[String])(using DbCon[?]): Vector[User] =
    val base = sql"SELECT * FROM users WHERE age > ${0}"
    val withName = nameOpt match
      case Some(n) => sql"$base AND name = $n"
      case None    => base
    withName.query[User].run()

  userQuery(Some("Alice"))
  userQuery(None)
```

### 4.3 TableInfo — Type-Safe Table References

`TableInfo` gives you compile-time checked column and table references for raw SQL. Column names come from your entity definition, so renames are caught at compile time:

```scala
// file: example.sc - part of sql interpolation

xa.connect:
  val u = TableInfo[User, User, Long]

  // Column references interpolate as literal SQL (not as ? params)
  sql"SELECT ${u.all} FROM $u WHERE ${u.age} > ${18} ORDER BY ${u.name}"
    .query[User]
    .run()

  // Aliased tables for self-joins or multi-table queries
  val u1 = TableInfo[User, User, Long].alias("u1")
  val u2 = TableInfo[User, User, Long].alias("u2")

  sql"""SELECT ${u1.all} FROM $u1
       |JOIN $u2 ON ${u1.email} = ${u2.email}
       |WHERE ${u1.name} != ${u2.name}""".stripMargin
    .query[User]
    .run()
```

### 4.4 Streaming Large Result Sets

For queries that return more rows than you want in memory, use `.iterator` with a fetch size hint:

```scala
// file: example.sc - part of sql interpolation
import scala.util.Using

xa.connect:
  // Iterator-based streaming — rows fetched in batches from the driver
  Using.Manager: 
    implicit use =>
    val rows = sql"SELECT * FROM users ORDER BY id"
      .query[User]
      .iterator(fetchSize = 100)

    rows.foreach: user =>
      println(user.name)
```

## 5. Schema Management (Experimental)

> **Note:** The `parlance-migrate` module is experimental. The API may change significantly in future releases.

### 5.1 Migration DSL

Define and run database schema changes in Scala. Migrations live in the `parlance-migrate` module.

```scala
// file: models.scala - part of migration dsl
//> using scala 3.8.2
//> using dep "ma.chinespirit::parlance-migrate:$VERSION$"
//> using dep "com.h2database:h2:2.3.232"
// compile-only
import ma.chinespirit.parlance.*
import ma.chinespirit.parlance.migrate.*
```

Each migration has a version, a name, and `up`/`down` operations:

```scala
// file: example.sc - part of migration dsl
import ma.chinespirit.parlance.*
import ma.chinespirit.parlance.migrate.*

val v1 = new MigrationDef:
  val version = 1L
  val name = "create_users"
  val up = List(
    createTable("users")(
      (List(
        id(),                                         // bigserial primary key
        column[String]("email").varchar(255).unique,
        column[String]("name").varchar(255),
        column[Int]("age"),
        column[Boolean]("active").default(true)
      ) ++ timestamps())*                             // created_at, updated_at
    )
  )
  val down = List(dropTable("users"))
```

Add related tables with foreign keys, indexes, and constraints:

```scala
// file: example.sc - part of migration dsl

val v2 = new MigrationDef:
  val version = 2L
  val name = "create_posts"
  val up = List(
    createTable("posts")(
      id(),
      column[String]("title").varchar(255),
      column[String]("body"),
      column[Long]("user_id")
        .references("users", "id", FkAction.Cascade, FkAction.NoAction)
    ),
    // Add index separately via ALTER TABLE
    alterTable("posts")(
      addIndex("user_id")
    )
  )
  val down = List(dropTable("posts"))
```

ALTER TABLE operations — add/drop columns, change types, manage constraints:

```scala
// file: example.sc - part of migration dsl

val v3 = new MigrationDef:
  val version = 3L
  val name = "evolve_users"
  val up = List(
    alterTable("users")(
      AlterOp.AddColumn(column[Option[String]]("bio")),
      AlterOp.AddColumn(column[String]("role").varchar(50).default("user")),
      addUniqueIndex("email"),
      AlterOp.AddCheckConstraint("age_positive", "age >= 0")
    )
  )
  val down = List(
    alterTable("users")(
      dropColumn("bio"),
      dropColumn("role"),
      dropIndex("users_email_idx"),
      AlterOp.DropConstraint("age_positive")
    )
  )
```

Table options — temporary tables, IF NOT EXISTS, comments:

```scala
// file: example.sc - part of migration dsl

val v4 = new MigrationDef:
  val version = 4L
  val name = "create_audit_log"
  val up = List(
    createTable("audit_log", TableOptions(
      ifNotExists = true,
      comment = Some("Tracks all entity changes"),
      unlogged = true            // PostgreSQL: skip WAL for performance
    ))(
      id(),
      column[String]("entity_type").varchar(100),
      column[Long]("entity_id"),
      column[String]("action").varchar(20),
      column[java.time.Instant]("performed_at")
    )
  )
  val down = List(dropTableIfExists("audit_log"))
```

PostgreSQL-specific — enum types and extensions:

```scala
// file: example.sc - part of migration dsl

val v5 = new MigrationDef:
  val version = 5L
  val name = "add_status_enum"
  val up = List(
    createEnumType("user_status", "active", "inactive", "suspended"),
    createExtension("uuid-ossp"),
    raw("ALTER TABLE users ADD COLUMN status user_status DEFAULT 'active'")
  )
  val down = List(
    raw("ALTER TABLE users DROP COLUMN status"),
    dropEnumType("user_status"),
    Migration.DropExtension("uuid-ossp")
  )
```

#### Running Migrations

Create a `Migrator` with your migration list and transactor, then apply:

```scala
// file: example.sc - part of migration dsl

val ds =
  val d = org.h2.jdbcx.JdbcDataSource()
  d.setURL("jdbc:h2:mem:migrate_test")
  d
val xa = Transactor(H2, ds)

val migrator = Migrator(
  migrations = List(v1, v2, v3, v4, v5),
  xa = xa,
  compiler = H2Compiler
)

xa.transact:
  // Apply all pending migrations
  val result = migrator.migrate()
  result.appliedCount             // Int
  result.batch                    // Option[Int] — batch number

  // Check what's applied and what's pending
  val status = migrator.status()
  status.applied                  // List[AppliedMigration]
  status.pending                  // List[MigrationDef]

  // Dry run — see the SQL without executing
  val pretend = migrator.pretend()
  pretend.foreach: p =>
    println(s"${p.migrationDef.name}:")
    p.compiledSql.foreach(println)
```

Rollback operations — granular control over undoing migrations:

```scala
// file: example.sc - part of migration dsl

xa.transact:
  migrator.migrate()

  // Roll back the latest batch
  migrator.rollback()

  // Roll back the last N migrations
  migrator.rollbackSteps(2)

  // Roll back a specific batch number
  migrator.rollbackBatch(1)

  // Roll back everything
  migrator.reset()
```

For migrations that include `CONCURRENTLY` operations (which can't run inside a transaction), use `migrateWithTxControl()` — it respects each migration's `txMode`:

```scala
// file: example.sc - part of migration dsl

val concurrentMigration = new MigrationDef:
  val version = 6L
  val name = "add_concurrent_index"
  override val txMode = TxMode.NonTransactional
  val up = List(
    alterTable("users")(
      AlterOp.AddIndex(
        columns = List("email"),
        unique = true,
        concurrently = true
      )
    )
  )
  val down = List(
    alterTable("users")(
      AlterOp.DropIndexConcurrently("users_email_idx")
    )
  )

// Per-migration transaction control
val migrator2 = Migrator(List(v1, concurrentMigration), xa, H2Compiler)
xa.transact:
  migrator2.migrateWithTxControl()
```

### 5.2 Schema Verification

Verify that your entity definitions match the actual database schema — catches drift between code and database at runtime:

```scala
//> using scala 3.8.2
//> using dep "ma.chinespirit::parlance-migrate:$VERSION$"
//> using dep "com.h2database:h2:2.3.232"
// compile-only
import ma.chinespirit.parlance.*
import ma.chinespirit.parlance.migrate.*

@Table(SqlNameMapper.CamelToSnakeCase)
case class User(
  @Id id: Long,
  name: String,
  email: String,
  age: Int
) derives EntityMeta

val ds =
  val d = org.h2.jdbcx.JdbcDataSource()
  d.setURL("jdbc:h2:mem:verify_test")
  d
val xa = Transactor(H2, ds)

xa.connect:
  val result: VerifyResult = verify[User]

  result.isOk          // true if no errors (warnings are ok)
  result.hasErrors     // true if any real mismatches
  result.issues        // List[VerifyIssue] — what's wrong
  println(result.prettyPrint)  // formatted report with pass/fail per column
```

Verification checks:
- Table exists in the database
- All entity fields have corresponding columns
- Column types are compatible with Scala types
- Nullability matches (`Option[T]` ↔ nullable column)
- Primary key columns match
- Extra columns in the database (reported as warnings)

## 6. Database Support & Extensions

### 6.1 Multi-Database Support

Single API across multiple database backends. Database capabilities are enforced at compile time — you can't use unsupported features (e.g., `ilike` on MySQL, mutations on ClickHouse).

| Database   | Mutations | Row Locks        | ILIKE | RETURNING | Arrays |
|------------|-----------|------------------|-------|-----------|--------|
| PostgreSQL | Yes       | FOR UPDATE/SHARE | Yes   | Yes       | Yes    |
| MySQL      | Yes       | FOR UPDATE       | —     | —         | —      |
| SQLite     | Yes       | —                | —     | —         | —      |
| H2         | Yes       | FOR UPDATE/SHARE | Yes   | Yes       | Yes    |
| Oracle     | Yes       | FOR UPDATE       | —     | Yes       | —      |
| ClickHouse | Read-only | —                | —     | —         | —      |

The database type is a type parameter on `Transactor`, `DbCon`, and `DbTx`. Methods that require specific capabilities use type bounds — the compiler rejects code that tries to use an unsupported feature:

```scala
//> using scala 3.8.2
//> using dep "ma.chinespirit::parlance:$VERSION$"
//> using dep "com.h2database:h2:2.3.232"
// compile-only
import ma.chinespirit.parlance.*

@Table(SqlNameMapper.CamelToSnakeCase)
case class User(@Id id: Long, name: String) derives EntityMeta

val ds =
  val d = org.h2.jdbcx.JdbcDataSource()
  d.setURL("jdbc:h2:mem:test")
  d

// H2 supports ILIKE — this compiles
val h2xa = Transactor(H2, ds)
h2xa.connect:
  QueryBuilder.from[User].where(_.name ilike "alice%").run()

// MySQL does NOT support ILIKE — this would be a compile error:
// val myxa = Transactor(MySQL, ds)
// myxa.connect:
//   QueryBuilder.from[User].where(_.name ilike "alice%").run()
//   // error: Cannot prove that DbCon[MySQL] <:< DbCon[? <: SupportsILike]
```

Dialect-specific SQL generation handles LIMIT/OFFSET syntax, upsert semantics, and TRUNCATE behavior per database.

### 6.2 SQL Logging

Configure how SQL statements are logged via `SqlLogger` on the transactor:

```scala
//> using scala 3.8.2
//> using dep "ma.chinespirit::parlance:$VERSION$"
//> using dep "com.h2database:h2:2.3.232"
// compile-only
import ma.chinespirit.parlance.*
import scala.concurrent.duration.*

val ds =
  val d = org.h2.jdbcx.JdbcDataSource()
  d.setURL("jdbc:h2:mem:test")
  d

// No logging
val silent = Transactor(H2, ds, SqlLogger.NoOp)

// Default — TRACE level (full SQL + params), DEBUG level (SQL only)
val verbose = Transactor(H2, ds, SqlLogger.Default)

// Slow query warnings — logs queries slower than threshold
val monitored = Transactor(H2, ds, SqlLogger.logSlowQueries(500.millis))

// Or configure after creation
val xa = Transactor(H2, ds)
  .withSqlLogger(SqlLogger.logSlowQueries(1.second))
```

For custom logging, implement the `SqlLogger` trait:

```scala
//> using scala 3.8.2
//> using dep "ma.chinespirit::parlance:$VERSION$"
// compile-only
import ma.chinespirit.parlance.*

val customLogger = new SqlLogger:
  def log(event: SqlSuccessEvent): Unit =
    println(s"[${event.execTime}] ${event.sql}")

  def exceptionMsg(event: SqlExceptionEvent): String =
    s"Query failed: ${event.sql} — ${event.cause.getMessage}"
```

### 6.3 PostgreSQL Extensions

The `parlance-pg` module provides additional types and codecs for PostgreSQL. Import the givens to enable them:

```scala
//> using scala 3.8.2
//> using dep "ma.chinespirit::parlance-pg:$VERSION$"
//> using dep "org.postgresql:postgresql:42.7.4"
// compile-only
import ma.chinespirit.parlance.*
import ma.chinespirit.parlance.pg.PgCodec.given
```

**Array columns** — use any standard Scala collection type for PostgreSQL arrays:

```scala
//> using scala 3.8.2
//> using dep "ma.chinespirit::parlance-pg:$VERSION$"
//> using dep "org.postgresql:postgresql:42.7.4"
// compile-only
import ma.chinespirit.parlance.*
import ma.chinespirit.parlance.pg.PgCodec.given

@Table(SqlNameMapper.CamelToSnakeCase)
case class Document(
  @Id id: Long,
  tags: Vector[String],          // text[]
  scores: Array[Int],            // integer[]
  metadata: List[Double]         // double precision[]
) derives EntityMeta
```

**Geometric types** — PostgreSQL geometric primitives mapped directly:

```scala
//> using scala 3.8.2
//> using dep "ma.chinespirit::parlance-pg:$VERSION$"
//> using dep "org.postgresql:postgresql:42.7.4"
// compile-only
import ma.chinespirit.parlance.*
import ma.chinespirit.parlance.pg.PgCodec.given
import org.postgresql.geometric.*
import org.postgresql.util.PGInterval

@Table(SqlNameMapper.CamelToSnakeCase)
case class GeoData(
  @Id id: Long,
  location: PGpoint,             // point
  area: PGbox,                   // box
  boundary: PGpolygon,           // polygon
  path: PGpath,                  // path
  segment: PGlseg,              // lseg
  ring: PGcircle,               // circle
  duration: PGInterval           // interval
) derives EntityMeta
```

**JSON/JSONB columns** — implement `JsonDbCodec` or `JsonBDbCodec` with your preferred JSON library. Example with Circe:

```scala
//> using scala 3.8.2
//> using dep "ma.chinespirit::parlance-pg:$VERSION$"
//> using dep "org.postgresql:postgresql:42.7.4"
//> using dep "io.circe::circe-core:0.14.10"
//> using dep "io.circe::circe-parser:0.14.10"
// compile-only
import ma.chinespirit.parlance.*
import ma.chinespirit.parlance.pg.json.*
import io.circe.*, io.circe.syntax.*, io.circe.parser.decode as circeDecode

case class Settings(theme: String, notifications: Boolean) derives Encoder.AsObject, Decoder

// Implement JsonBDbCodec for your type
given DbCodec[Settings] = new JsonBDbCodec[Settings]:
  def encode(a: Settings): String = a.asJson.noSpaces
  def decode(json: String): Settings = circeDecode[Settings](json).fold(throw _, identity)

@Table(SqlNameMapper.CamelToSnakeCase)
case class UserProfile(
  @Id id: Long,
  name: String,
  settings: Settings              // stored as JSONB
) derives EntityMeta
```

**PostgreSQL enums** — Scala 3 enums mapped to PG enum types:

```scala
//> using scala 3.8.2
//> using dep "ma.chinespirit::parlance-pg:$VERSION$"
//> using dep "org.postgresql:postgresql:42.7.4"
// compile-only
import ma.chinespirit.parlance.*
import ma.chinespirit.parlance.pg.PgCodec.given
import ma.chinespirit.parlance.pg.enums.PgEnumToScalaEnumSqlArrayCodec

// Maps to CREATE TYPE status AS ENUM ('active', 'inactive', 'suspended')
enum Status derives DbCodec:
  case Active, Inactive, Suspended

// Arrays of enums work too
@Table(SqlNameMapper.CamelToSnakeCase)
case class User(
  @Id id: Long,
  name: String,
  status: Status,                 // status enum column
  previousStatuses: Vector[Status] // status[] enum array column
) derives EntityMeta
```
