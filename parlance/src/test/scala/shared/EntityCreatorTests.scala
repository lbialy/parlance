package shared

import ma.chinespirit.parlance.*
import ma.chinespirit.parlance.SqlException
import munit.{FunSuite, Location}

import scala.util.Using

def entityCreatorTests[D <: SupportsMutations](suite: FunSuite, xa: () => Transactor[D])(using
    Location
): Unit =
  import suite.*

  case class MyUserCreator(firstName: String) extends CreatorOf[MyUser] derives DbCodec

  @Table(SqlNameMapper.CamelToSnakeCase)
  case class MyUser(firstName: String, id: Long) derives EntityMeta

  val userRepo = Repo[MyUserCreator, MyUser, Long]()
  val user = TableInfo[MyUserCreator, MyUser, Long]

  test("insert EntityCreator"):
    xa().connect:
      userRepo.rawInsert(MyUserCreator("Ash"))
      userRepo.rawInsert(MyUserCreator("Steve"))
      assert(userRepo.count == 5L)
      assert(userRepo.findAll.map(_.firstName).contains("Steve"))

  test("insert invalid EntityCreator"):
    intercept[SqlException]:
      xa().connect:
        val invalidUser = MyUserCreator(null)
        userRepo.rawInsert(invalidUser)

  test("insertAll EntityCreator"):
    xa().connect:
      val newUsers = Vector(
        MyUserCreator("Ash"),
        MyUserCreator("Steve"),
        MyUserCreator("Josh")
      )
      userRepo.rawInsertAll(newUsers)
      assert(userRepo.count == 6L)
      assert(
        userRepo.findAll.map(_.firstName).contains(newUsers.last.firstName)
      )

  test("custom insert EntityCreator"):
    xa().connect:
      val u = MyUserCreator("Ash")
      val update =
        sql"insert into $user ${user.insertColumns} values ($u)".update
      assertNoDiff(
        update.frag.sqlString,
        "insert into my_user (first_name) values (?)"
      )
      val rowsInserted = update.run()
      assert(rowsInserted == 1)
      assert(userRepo.count == 4L)
      assert(userRepo.findAll.exists(_.firstName == "Ash"))

  test("custom update EntityCreator"):
    xa().connect:
      val u = userRepo.findAll.head
      val newName = "Ash"
      val update =
        sql"update $user set ${user.firstName} = $newName where ${user.id} = ${u.id}".update
      assertNoDiff(
        update.frag.sqlString,
        "update my_user set first_name = ? where id = ?"
      )
      val rowsUpdated = update.run()
      assert(rowsUpdated == 1)
      assert(userRepo.findAll.exists(_.firstName == "Ash"))

end entityCreatorTests

def entityCreatorReturningTests[D <: SupportsReturning](
    suite: FunSuite,
    xa: () => Transactor[D]
)(using Location): Unit =
  import suite.*

  case class MyUserCreator(firstName: String) extends CreatorOf[MyUser] derives DbCodec

  @Table(SqlNameMapper.CamelToSnakeCase)
  case class MyUser(firstName: String, id: Long) derives EntityMeta

  val userRepo = Repo[MyUserCreator, MyUser, Long]()
  val user = TableInfo[MyUserCreator, MyUser, Long]

  test("insertReturning EntityCreator"):
    xa().connect:
      val user = userRepo.create(MyUserCreator("Ash"))
      assert(user.firstName == "Ash")

  test("insertAllReturning EntityCreator"):
    xa().connect:
      val newUsers = Vector(
        MyUserCreator("Ash"),
        MyUserCreator("Steve"),
        MyUserCreator("Josh")
      )
      val users = userRepo.rawInsertAllReturning(newUsers)
      assert(userRepo.count == 6L)
      assert(users.size == 3)
      assert(users.last.firstName == newUsers.last.firstName)

  test(".returning iterator"):
    xa().connect:
      Using.Manager(implicit use =>
        val it =
          if xa().databaseType == H2 then
            sql"INSERT INTO $user ${user.insertColumns} VALUES ('Bob')"
              .returningKeys[Long](user.id)
              .iterator()
          else
            sql"INSERT INTO $user ${user.insertColumns} VALUES ('Bob') RETURNING ${user.id}"
              .returning[Long]
              .iterator()
        assert(it.size == 1)
      )

end entityCreatorReturningTests
