import com.augustnagro.magnum.*

import java.time.OffsetDateTime

@SqlName("sd_user")
@Table(H2DbType, SqlNameMapper.CamelToSnakeCase)
case class SdExtUser(
    @Id id: Long,
    name: String,
    deletedAt: Option[OffsetDateTime]
) derives DbCodec, TableMeta

class EntityExtensionSoftDeleteTests extends QbTestBase:

  val h2Ddls = Seq("/h2/soft-delete.sql")

  given sdRepo: (Repo[SdExtUser, SdExtUser, Long] with SoftDeletes[SdExtUser, SdExtUser, Long]) =
    new Repo[SdExtUser, SdExtUser, Long] with SoftDeletes[SdExtUser, SdExtUser, Long]

  test("entity.trashed returns true for soft-deleted entity"):
    val t = xa()
    t.connect:
      val all = sdRepo.withTrashed.run()
      val carol = all.find(_.name == "Carol").get
      assert(carol.trashed)

  test("entity.trashed returns false for active entity"):
    val t = xa()
    t.connect:
      val alice = sdRepo.findById(1L).get
      assert(!alice.trashed)

  test("entity.forceDelete() hard-deletes"):
    val t = xa()
    t.transact:
      val dave = sdRepo.withTrashed.run().find(_.name == "Dave").get
      dave.forceDelete()

      val all = sdRepo.withTrashed.run()
      assert(!all.exists(_.id == dave.id))

      // Restore for other tests
      sql"INSERT INTO sd_user VALUES (4, 'Dave', TIMESTAMP WITH TIME ZONE '2025-06-15T12:00:00Z')".update
        .run()

  test("entity.restore() restores soft-deleted entity"):
    val t = xa()
    t.transact:
      // Carol (id=3) is soft-deleted
      assert(sdRepo.findById(3L).isEmpty)
      val carol = sdRepo.withTrashed.run().find(_.name == "Carol").get
      carol.restore()
      val restored = sdRepo.findById(3L)
      assert(restored.isDefined)
      assertEquals(restored.get.name, "Carol")

      // Re-soft-delete for other tests
      sdRepo.deleteById(3L)

end EntityExtensionSoftDeleteTests
