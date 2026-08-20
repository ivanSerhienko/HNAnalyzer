package storage

import zio._
import zio.jdbc._
import zio.test._

import java.time.Instant
import java.util.UUID

object BronzeWriterSpec extends ZIOSpecDefault {

  private def readBack(entityId: Long) =
    ZIO.serviceWithZIO[ZConnectionPool] { pool =>
      sql"SELECT entity_type, entity_id, source_listing, poll_id, raw_json FROM bronze WHERE entity_id = $entityId"
        .query[(String, Long, Option[String], UUID, String)]
        .selectOne
        .provideSomeLayer[Any](ZLayer.succeed(pool) >>> transaction)
    }

  def spec = suite("BronzeWriter")(
    test("writes a bronze row that can be read back") {
      val pollId    = UUID.randomUUID()
      val polledAt  = Instant.parse("2026-08-20T12:00:00Z")
      val entityId  = scala.util.Random.nextLong().abs
      val row       = BronzeRow(
        entityType = "story",
        entityId = entityId,
        sourceListing = Some("top"),
        pollId = pollId,
        polledAt = polledAt,
        rawJson = """{"id": 1, "title": "test"}""",
      )

      for {
        _        <- BronzeWriter.write(row)
        readRow  <- readBack(entityId)
      } yield assertTrue(
        readRow.contains(("story", entityId, Some("top"), pollId, """{"id": 1, "title": "test"}""")),
      )
    },
  ).provideLayerShared(Db.pool >+> BronzeWriter.live)
}
