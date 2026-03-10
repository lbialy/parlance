import ma.chinespirit.parlance.*

import java.time.LocalDateTime

// --- Generic scopes ---

class ActiveOnlyScope[E] extends Scope[E]:
  override def conditions(meta: TableMeta[E]): Vector[WhereFrag] =
    Vector(Frag(s"${meta.tableName}.active = true", Seq.empty, FragWriter.empty).unsafeAsWhere)

class NotDeletedScope[E] extends Scope[E]:
  override def conditions(meta: TableMeta[E]): Vector[WhereFrag] =
    Vector(Frag(s"${meta.tableName}.deleted_at IS NULL", Seq.empty, FragWriter.empty).unsafeAsWhere)

// ============================================================
// Test 1: Eager loading (User -> Profile -> Address, User -> Order)
// ============================================================

@Table(SqlNameMapper.CamelToSnakeCase)
case class DemoUser(@Id id: Long, name: String, active: Boolean, createdAt: LocalDateTime) derives EntityMeta

@Table(SqlNameMapper.CamelToSnakeCase)
case class DemoProfile(@Id id: Long, userId: Long, bio: String) derives EntityMeta

@Table(SqlNameMapper.CamelToSnakeCase)
case class DemoAddress(@Id id: Long, profileId: Long, city: String, street: String) derives EntityMeta

@Table(SqlNameMapper.CamelToSnakeCase)
case class DemoOrder(@Id id: Long, userId: Long, product: String) derives EntityMeta

object DemoUser:
  val profile = Relationship.hasOne[DemoUser, DemoProfile](_.id, _.userId)
  val orders = Relationship.hasMany[DemoUser, DemoOrder](_.id, _.userId)
  val addresses = DemoProfile.addresses via profile
  val repo = ImmutableRepo[DemoUser, Long](Vector(ActiveOnlyScope[DemoUser]()))

object DemoProfile:
  val addresses = Relationship.hasMany[DemoProfile, DemoAddress](_.id, _.profileId)

// ============================================================
// Test 2: JOIN with soft deletes (User -> Profile -> Address)
// ============================================================

@Table(SqlNameMapper.CamelToSnakeCase)
case class DmjUser(@Id id: Long, name: String, active: Boolean, createdAt: LocalDateTime, deletedAt: Option[LocalDateTime])
    derives EntityMeta

@Table(SqlNameMapper.CamelToSnakeCase)
case class DmjProfile(@Id id: Long, userId: Long, bio: String, deletedAt: Option[LocalDateTime]) derives EntityMeta

@Table(SqlNameMapper.CamelToSnakeCase)
case class DmjAddress(@Id id: Long, profileId: Long, city: String, street: String, zipCode: String, deletedAt: Option[LocalDateTime])
    derives EntityMeta

object DmjUser:
  val profile = Relationship.hasOne[DmjUser, DmjProfile](_.id, _.userId)
  val repo = ImmutableRepo[DmjUser, Long](Vector(ActiveOnlyScope(), NotDeletedScope()))

object DmjProfile:
  val addresses = Relationship.hasMany[DmjProfile, DmjAddress](_.id, _.profileId)

// Scoped givens for entities without repos — required in ApplyScopes mode
given Scoped[DemoProfile] = Scoped.none
given Scoped[DemoAddress] = Scoped.none
given Scoped[DemoOrder] = Scoped.none

// Scoped givens so JoinedQuery picks up soft-delete conditions on ON clauses
given Scoped[DmjProfile] with
  def scopes: Vector[Scope[DmjProfile]] = Vector(NotDeletedScope[DmjProfile]())

given Scoped[DmjAddress] with
  def scopes: Vector[Scope[DmjAddress]] = Vector(NotDeletedScope[DmjAddress]())

// ============================================================
// Tests
// ============================================================

class DemoTest extends QbH2TestBase:
  val h2Ddls = Seq("/h2/demo.sql", "/h2/demo-join.sql")

  test("ORM eager loading with ActiveOnly scope matches raw SQL"):
    val t = xa()
    t.connect:
      // === ORM: scoped repo + eager loading + constraint + ordering + pagination ===
      val ormResults = DemoUser.repo.query
        .orderBy(_.createdAt, SortOrder.Desc)
        .limit(3)
        .offset(1)
        .withRelated(DemoUser.addresses)(_.city === "Warsaw")
        .withRelated(DemoUser.orders)
        .run()

      // === Semi-raw SQL: refactor-safe column references via TableInfo ===
      val u = TableInfo[DemoUser, DemoUser, Long]
      val a = TableInfo[DemoAddress, DemoAddress, Long].alias("a")
      val p = TableInfo[DemoProfile, DemoProfile, Long].alias("p")
      val o = TableInfo[DemoOrder, DemoOrder, Long]

      val semiRawUsers = sql"""
        SELECT ${u.all} FROM $u
        WHERE ${u.active} = true
        ORDER BY ${u.createdAt} DESC
        LIMIT 3 OFFSET 1
      """.query[DemoUser].run()

      val semiRawResults = semiRawUsers.map: user =>
        val addresses = sql"""
          SELECT ${a.all} FROM $a
          JOIN $p ON ${p.id} = ${a.profileId}
          WHERE ${p.userId} = ${user.id} AND ${a.city} = 'Warsaw'
        """.query[DemoAddress].run()

        val orders = sql"""
          SELECT ${o.all} FROM $o WHERE ${o.userId} = ${user.id}
        """.query[DemoOrder].run()

        (user, addresses, orders)

      // === Raw SQL: manual multi-query eager loading ===
      val rawUsers = sql"""
        SELECT * FROM demo_user
        WHERE active = true
        ORDER BY created_at DESC
        LIMIT 3 OFFSET 1
      """.query[DemoUser].run()

      val rawResults = rawUsers.map: user =>
        val addresses = sql"""
          SELECT a.* FROM demo_address a
          JOIN demo_profile p ON p.id = a.profile_id
          WHERE p.user_id = ${user.id} AND a.city = 'Warsaw'
        """.query[DemoAddress].run()

        val orders = sql"""
          SELECT * FROM demo_order WHERE user_id = ${user.id}
        """.query[DemoOrder].run()

        (user, addresses, orders)

      // === Verify same shape ===
      assertEquals(ormResults.size, 3)
      assertEquals(semiRawResults.size, 3)
      assertEquals(rawResults.size, 3)

      // Active users by created_at desc: Frank(6), Eve(5), Carol(3), Bob(2), Alice(1)
      // Dave(4) excluded by ActiveOnly scope
      // offset(1) skips Frank, limit(3) takes Eve, Carol, Bob
      val ormUsers = ormResults.map(_._1)
      assertEquals(ormUsers.map(_.name), Vector("Eve", "Carol", "Bob"))
      assertEquals(ormUsers, semiRawResults.map(_._1))
      assertEquals(ormUsers, rawResults.map(_._1))

      // Compare addresses and orders per user
      ormResults
        .zip(semiRawResults)
        .zip(rawResults)
        .foreach { case ((orm, semiRaw), raw) =>
          assertEquals(orm._1, semiRaw._1)
          assertEquals(orm._2.toSet, semiRaw._2.toSet)
          assertEquals(orm._3.toSet, semiRaw._3.toSet)
          assertEquals(orm._1, raw._1)
          assertEquals(orm._2.toSet, raw._2.toSet)
          assertEquals(orm._3.toSet, raw._3.toSet)
        }

      // Eve: 1 Warsaw address (Gdansk filtered out), 1 order
      assertEquals(ormResults(0)._2.size, 1)
      assertEquals(ormResults(0)._2.head.street, "Pulawska")
      assertEquals(ormResults(0)._3.size, 1)
      assertEquals(ormResults(0)._3.head.product, "Thingamajig")

      // Carol: 1 Warsaw address, 1 order
      assertEquals(ormResults(1)._2.size, 1)
      assertEquals(ormResults(1)._2.head.street, "Nowy Swiat")
      assertEquals(ormResults(1)._3.size, 1)
      assertEquals(ormResults(1)._3.head.product, "Doohickey")

      // Bob: 1 Warsaw address (Krakow filtered out), 2 orders
      assertEquals(ormResults(2)._2.size, 1)
      assertEquals(ormResults(2)._2.head.street, "Marszalkowska")
      assertEquals(ormResults(2)._3.size, 2)
      assertEquals(ormResults(2)._3.map(_.product).toSet, Set("Widget", "Gadget"))

  test("ORM joined query with soft deletes + ActiveOnly matches raw SQL"):
    val t = xa()
    t.connect:
      // === ORM: JoinedQuery with scopes providing soft deletes on all tables ===
      // DmjUser.repo.query applies: ActiveOnly (WHERE active = true) + NotDeleted (WHERE deleted_at IS NULL)
      // .join(DmjUser.profile) applies: NotDeleted on profile → ON ... AND deleted_at IS NULL
      // .join(DmjProfile.addresses) applies: NotDeleted on address → ON ... AND deleted_at IS NULL
      val jq = DmjUser.repo.query
        .join(DmjUser.profile)
        .join(DmjProfile.addresses)

      val ormResults = jq
        .where(jq.of[DmjAddress].city === "Warsaw")
        .orderBy(jq.of[DmjUser].createdAt, SortOrder.Desc)
        .orderBy(jq.of[DmjAddress].zipCode)
        .limit(3)
        .offset(1)
        .run()

      // === Semi-raw SQL: refactor-safe column references via TableInfo ===
      val u = TableInfo[DmjUser, DmjUser, Long].alias("u")
      val p = TableInfo[DmjProfile, DmjProfile, Long].alias("p")
      val a = TableInfo[DmjAddress, DmjAddress, Long].alias("a")

      val semiRawResults = sql"""
        SELECT ${u.all}, ${p.all}, ${a.all}
        FROM $u
        INNER JOIN $p ON ${u.id} = ${p.userId} AND ${p.deletedAt} IS NULL
        INNER JOIN $a ON ${p.id} = ${a.profileId} AND ${a.deletedAt} IS NULL
        WHERE ${u.deletedAt} IS NULL
          AND ${u.active} = true
          AND ${a.city} = 'Warsaw'
        ORDER BY ${u.createdAt} DESC, ${a.zipCode} ASC
        LIMIT 3 OFFSET 1
      """.query[(DmjUser, DmjProfile, DmjAddress)].run()

      // === Raw SQL: exact equivalent with manual soft-delete filters ===
      val rawResults = sql"""
        SELECT u.*, p.*, a.*
        FROM dmj_user u
        INNER JOIN dmj_profile p ON u.id = p.user_id AND p.deleted_at IS NULL
        INNER JOIN dmj_address a ON p.id = a.profile_id AND a.deleted_at IS NULL
        WHERE u.deleted_at IS NULL
          AND u.active = true
          AND a.city = 'Warsaw'
        ORDER BY u.created_at DESC, a.zip_code ASC
        LIMIT 3 OFFSET 1
      """.query[(DmjUser, DmjProfile, DmjAddress)].run()

      // === Verify both produce identical results ===
      //
      // Valid rows (all filters applied):
      //   Alice(1): soft-deleted user → excluded
      //   Bob(2):   active, not deleted, profile ok → 1 Warsaw address (Marszalkowska/00-001)
      //   Carol(3): active, not deleted, profile ok → 1 Warsaw address (Nowy Swiat/00-500)
      //   Dave(4):  inactive → excluded
      //   Eve(5):   active, not deleted, profile ok → 2 Warsaw addresses (Mokotowska/00-100, Pulawska/02-600)
      //             (Gdansk filtered by city, Deleted Street filtered by address soft-delete)
      //   Frank(6): active, not deleted, but profile soft-deleted → excluded at join
      //
      // ORDER BY created_at DESC, zip_code ASC:
      //   row 0: Eve   / Mokotowska  / 00-100
      //   row 1: Eve   / Pulawska    / 02-600
      //   row 2: Carol / Nowy Swiat  / 00-500
      //   row 3: Bob   / Marszalkowska / 00-001
      //
      // OFFSET 1 LIMIT 3 → rows 1,2,3

      assertEquals(ormResults.size, 3)
      assertEquals(semiRawResults.size, 3)
      assertEquals(rawResults.size, 3)

      // Row-by-row comparison
      ormResults
        .zip(semiRawResults)
        .zip(rawResults)
        .foreach { case ((orm, semiRaw), raw) =>
          assertEquals(orm._1, semiRaw._1)
          assertEquals(orm._2, semiRaw._2)
          assertEquals(orm._3, semiRaw._3)
          assertEquals(orm._1, raw._1)
          assertEquals(orm._2, raw._2)
          assertEquals(orm._3, raw._3)
        }

      // Row 1 (offset skipped row 0): Eve / Pulawska / 02-600
      assertEquals(ormResults(0)._1.name, "Eve")
      assertEquals(ormResults(0)._3.street, "Pulawska")
      assertEquals(ormResults(0)._3.zipCode, "02-600")

      // Row 2: Carol / Nowy Swiat / 00-500
      assertEquals(ormResults(1)._1.name, "Carol")
      assertEquals(ormResults(1)._3.street, "Nowy Swiat")
      assertEquals(ormResults(1)._3.zipCode, "00-500")

      // Row 3: Bob / Marszalkowska / 00-001
      assertEquals(ormResults(2)._1.name, "Bob")
      assertEquals(ormResults(2)._3.street, "Marszalkowska")
      assertEquals(ormResults(2)._3.zipCode, "00-001")
end DemoTest
