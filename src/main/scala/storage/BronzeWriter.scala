package storage

import zio._
import zio.jdbc._

import java.time.Instant
import java.util.UUID

final case class BronzeRow(
  entityType: String,
  entityId: Long,
  sourceListing: Option[String],
  pollId: UUID,
  polledAt: Instant,
  rawJson: String,
)

trait BronzeWriter {
  def write(row: BronzeRow): Task[Unit]
}

object BronzeWriter {

  final case class Live(pool: ZConnectionPool) extends BronzeWriter {
    override def write(row: BronzeRow): Task[Unit] = {
      val insert: ZIO[ZConnection, Throwable, Unit] =
        sql"""INSERT INTO bronze (entity_type, entity_id, source_listing, poll_id, polled_at, raw_json)
              VALUES (${row.entityType}, ${row.entityId}, ${row.sourceListing}, ${row.pollId}, ${row.polledAt}, ${row.rawJson}::jsonb)""".execute

      insert.provideLayer(ZLayer.succeed(pool) >>> transaction)
    }
  }

  val live: ZLayer[ZConnectionPool, Nothing, BronzeWriter] =
    ZLayer.fromFunction(Live.apply)

  def write(row: BronzeRow): ZIO[BronzeWriter, Throwable, Unit] =
    ZIO.serviceWithZIO[BronzeWriter](_.write(row))
}
