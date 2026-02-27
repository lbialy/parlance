package com.augustnagro.magnum.migrate

import com.augustnagro.magnum.*
import munit.FunSuite
import org.h2.jdbcx.JdbcDataSource

import java.nio.file.Files

// --- Entity definitions at top level (macro requirement) ---

@Table(H2DbType, SqlNameMapper.CamelToSnakeCase)
case class VerifyUser(@Id id: Long, email: String, name: String)
    derives EntityMeta

@Table(H2DbType, SqlNameMapper.CamelToSnakeCase)
case class VerifyPost(@Id id: Long, title: String, body: String, userId: Int)
    derives EntityMeta

@Table(H2DbType, SqlNameMapper.CamelToSnakeCase)
@SqlName("verify_user")
case class VerifyUserWithOptional(
    @Id id: Long,
    email: String,
    bio: Option[String]
) derives EntityMeta

// email is Int but DB has VARCHAR — type mismatch
@Table(H2DbType, SqlNameMapper.CamelToSnakeCase)
case class WrongTypes(@Id id: Long, email: Int) derives EntityMeta

// has a field that doesn't exist in DB
@Table(H2DbType, SqlNameMapper.CamelToSnakeCase)
case class MissingCol(@Id id: Long, email: String, nonExistent: String)
    derives EntityMeta

// references a table that doesn't exist
@Table(H2DbType, SqlNameMapper.SameCase)
@SqlName("no_such_table")
case class NoTable(@Id id: Long) derives EntityMeta

// non-Option field maps to nullable DB column — nullability mismatch
@Table(H2DbType, SqlNameMapper.CamelToSnakeCase)
case class NullMismatchUser(@Id id: Long, email: String, bio: String)
    derives EntityMeta

// Option field maps to NOT NULL DB column — nullability mismatch the other way
@Table(H2DbType, SqlNameMapper.CamelToSnakeCase)
case class OptionNotNullUser(@Id id: Long, email: Option[String])
    derives EntityMeta

// For PK mismatch: entity marks `id` as PK but table has `email` as PK
@Table(H2DbType, SqlNameMapper.SameCase)
@SqlName("pk_mismatch_table")
case class PkMismatchEntity(@Id id: Long, email: String) derives EntityMeta

class VerifyTests extends FunSuite:

  lazy val h2DbPath: String =
    Files.createTempDirectory(null).toAbsolutePath.toString

  def freshDs(): JdbcDataSource =
    val ds = JdbcDataSource()
    ds.setURL("jdbc:h2:" + h2DbPath + ";DB_CLOSE_DELAY=-1")
    ds.setUser("sa")
    ds.setPassword("")
    ds

  lazy val xa: Transactor = Transactor(freshDs())

  override def beforeEach(context: BeforeEach): Unit =
    val conn = freshDs().getConnection
    try
      val stmt = conn.createStatement()
      stmt.execute("DROP ALL OBJECTS")
      stmt.close()
    finally conn.close()

  private def setupSchema(): Unit =
    val m = Migrator(List(createUsers, createPosts), xa, H2Compiler)
    m.migrate()

  // --- Migration fixtures ---

  val createUsers: MigrationDef = new MigrationDef:
    val version = 1L
    val name = "create_verify_users"
    val up = List(
      Migration.CreateTable(
        "verify_user",
        List(
          ColumnDef[Long]("id", ColumnType.BigInt).primaryKey,
          ColumnDef[String]("email", ColumnType.Varchar(255)),
          ColumnDef[String]("name", ColumnType.Varchar(255)),
          ColumnDef[String]("bio", ColumnType.Text).nullable
        )
      )
    )
    val down = List(Migration.DropTable("verify_user"))

  val createPosts: MigrationDef = new MigrationDef:
    val version = 2L
    val name = "create_verify_posts"
    val up = List(
      Migration.CreateTable(
        "verify_post",
        List(
          ColumnDef[Long]("id", ColumnType.BigInt).primaryKey,
          ColumnDef[String]("title", ColumnType.Varchar(255)),
          ColumnDef[String]("body", ColumnType.Text),
          ColumnDef[Int]("user_id", ColumnType.Integer)
        )
      )
    )
    val down = List(Migration.DropTable("verify_post"))

  // --- Tests ---

  test("verify passes for matching entity"):
    setupSchema()
    xa.connect:
      val result = verify[VerifyUser]
      assert(result.isOk, s"Expected isOk but got:\n${result.prettyPrint}")
      assert(!result.hasErrors)

  test("verify passes for entity with correct Option mapping"):
    setupSchema()
    xa.connect:
      val result = verify[VerifyUserWithOptional]
      assert(result.isOk, s"Expected isOk but got:\n${result.prettyPrint}")

  test("all columns present and types compatible"):
    setupSchema()
    xa.connect:
      val result = verify[VerifyPost]
      assert(result.isOk, s"Expected isOk but got:\n${result.prettyPrint}")
      assertEquals(result.columnDetails.size, 4)
      assert(result.columnDetails.forall(_.issue.isEmpty))

  test("TableMissing when table does not exist"):
    // No schema setup — table doesn't exist
    xa.connect:
      val result = verify[NoTable]
      assert(result.hasErrors)
      assertEquals(result.issues.size, 1)
      assert(result.issues.head.isInstanceOf[VerifyIssue.TableMissing])

  test("ColumnMissing when entity has field not in DB"):
    setupSchema()
    xa.connect:
      // MissingCol maps to verify_user table (via CamelToSnakeCase: missing_col)
      // but it expects table "missing_col" — let's create verify_user and use it
      // Actually MissingCol maps to table "missing_col" which doesn't exist.
      // Let me use a raw SQL approach instead.
      val conn = summon[DbCon].connection
      conn
        .createStatement()
        .execute(
          "CREATE TABLE missing_col (id BIGINT PRIMARY KEY, email VARCHAR(255))"
        )
      val result = verify[MissingCol]
      assert(result.hasErrors)
      val missing = result.issues.collect:
        case m: VerifyIssue.ColumnMissing => m
      assertEquals(missing.size, 1)
      assertEquals(missing.head.columnName, "non_existent")

  test("ExtraColumnInDb when DB has column not in entity"):
    setupSchema()
    xa.connect:
      // verify_user has bio column, but VerifyUser entity doesn't have bio
      val result = verify[VerifyUser]
      val extras = result.issues.collect:
        case e: VerifyIssue.ExtraColumnInDb => e
      assertEquals(extras.size, 1)
      assertEquals(extras.head.columnName.toLowerCase, "bio")

  test("verify with extra column is warning, not error"):
    setupSchema()
    xa.connect:
      val result = verify[VerifyUser]
      assert(result.isOk, "ExtraColumnInDb should be a warning, not error")
      assert(!result.hasErrors)

  test("TypeMismatch when entity field type does not match DB column"):
    xa.connect:
      val conn = summon[DbCon].connection
      conn
        .createStatement()
        .execute(
          "CREATE TABLE wrong_types (id BIGINT PRIMARY KEY, email VARCHAR(255))"
        )
      val result = verify[WrongTypes]
      assert(result.hasErrors)
      val mismatches = result.issues.collect:
        case m: VerifyIssue.TypeMismatch => m
      assertEquals(mismatches.size, 1)
      assertEquals(mismatches.head.columnName, "email")

  test("NullabilityMismatch when Option field maps to NOT NULL column"):
    xa.connect:
      val conn = summon[DbCon].connection
      // email is NOT NULL in DB but Option[String] in entity
      conn
        .createStatement()
        .execute(
          "CREATE TABLE option_not_null_user (id BIGINT PRIMARY KEY, email VARCHAR(255) NOT NULL)"
        )
      val result = verify[OptionNotNullUser]
      val nullIssues = result.issues.collect:
        case n: VerifyIssue.NullabilityMismatch => n
      assertEquals(nullIssues.size, 1)
      assertEquals(nullIssues.head.columnName, "email")
      assert(nullIssues.head.isOptionInScala)
      assert(!nullIssues.head.isNullableInDb)

  test("NullabilityMismatch when non-Option field maps to nullable column"):
    setupSchema()
    xa.connect:
      // bio is nullable in DB but String (non-Option) in NullMismatchUser
      // NullMismatchUser maps to table "null_mismatch_user" which doesn't exist
      // Let's create it manually
      val conn = summon[DbCon].connection
      conn
        .createStatement()
        .execute(
          "CREATE TABLE null_mismatch_user (id BIGINT PRIMARY KEY, email VARCHAR(255) NOT NULL, bio TEXT NULL)"
        )
      val result = verify[NullMismatchUser]
      val nullIssues = result.issues.collect:
        case n: VerifyIssue.NullabilityMismatch => n
      assertEquals(nullIssues.size, 1)
      assertEquals(nullIssues.head.columnName, "bio")
      assert(!nullIssues.head.isOptionInScala)
      assert(nullIssues.head.isNullableInDb)

  test("no NullabilityMismatch when Option maps to nullable"):
    setupSchema()
    xa.connect:
      // verify_user.bio is nullable, VerifyUserWithOptional.bio is Option[String]
      val result = verify[VerifyUserWithOptional]
      val nullIssues = result.issues.collect:
        case n: VerifyIssue.NullabilityMismatch => n
      assertEquals(nullIssues.size, 0)

  test("PrimaryKeyMismatch when entity @Id does not match actual PK"):
    xa.connect:
      val conn = summon[DbCon].connection
      // Table has email as PK, but entity has @Id on id
      conn
        .createStatement()
        .execute(
          "CREATE TABLE pk_mismatch_table (id BIGINT NOT NULL, email VARCHAR(255) PRIMARY KEY)"
        )
      val result = verify[PkMismatchEntity]
      assert(result.hasErrors)
      val pkIssues = result.issues.collect:
        case p: VerifyIssue.PrimaryKeyMismatch => p
      assertEquals(pkIssues.size, 1)

  test("prettyPrint produces readable output"):
    setupSchema()
    xa.connect:
      val result = verify[VerifyUser]
      val pp = result.prettyPrint
      assert(pp.contains("verify[VerifyUser]"))
      assert(pp.contains("verify_user"))
      assert(pp.contains("id"))
      assert(pp.contains("email"))

  test("single verify call returns all issues found"):
    xa.connect:
      val conn = summon[DbCon].connection
      conn
        .createStatement()
        .execute(
          "CREATE TABLE wrong_types (id BIGINT PRIMARY KEY, email VARCHAR(255), extra_col TEXT)"
        )
      val result = verify[WrongTypes]
      // Should have: TypeMismatch for email + ExtraColumnInDb for extra_col
      assert(result.issues.size >= 2)
      assert(result.issues.exists(_.isInstanceOf[VerifyIssue.TypeMismatch]))
      assert(result.issues.exists(_.isInstanceOf[VerifyIssue.ExtraColumnInDb]))

end VerifyTests
