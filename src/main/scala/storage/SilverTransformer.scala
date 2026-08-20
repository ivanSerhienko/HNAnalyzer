package storage

import zio._
import zio.jdbc._

import java.util.UUID

trait SilverTransformer {
  def transformStories(pollId: UUID): Task[Long]
  def transformComments(pollId: UUID): Task[Long]
}

object SilverTransformer {

  final case class Live(pool: ZConnectionPool) extends SilverTransformer {
    private def run(fragment: SqlFragment): Task[Long] =
      fragment.insert.provideLayer(ZLayer.succeed(pool) >>> transaction)

    override def transformStories(pollId: UUID): Task[Long] =
      run(sql"""
        INSERT INTO story_snapshots
          (story_id, poll_id, polled_at, title, url, domain, author, score, comment_count, story_type, submitted_at)
        SELECT
          entity_id,
          poll_id,
          polled_at,
          raw_json ->> 'title',
          raw_json ->> 'url',
          CASE WHEN raw_json ->> 'url' IS NOT NULL
               THEN regexp_replace(
                      substring(raw_json ->> 'url' from '^(?:https?://)?([^/:]+)'),
                      '^www\.', ''
                    )
               ELSE NULL
          END,
          raw_json ->> 'by',
          COALESCE((raw_json ->> 'score')::int, 0),
          COALESCE((raw_json ->> 'descendants')::int, 0),
          source_listing,
          to_timestamp((raw_json ->> 'time')::bigint)
        FROM bronze
        WHERE poll_id = $pollId AND entity_type = 'story'
        ON CONFLICT (story_id, poll_id) DO NOTHING
      """)

    override def transformComments(pollId: UUID): Task[Long] =
      run(sql"""
        INSERT INTO comments (comment_id, story_id, author, text, submitted_at)
        SELECT
          entity_id,
          (raw_json ->> 'parent')::bigint,
          raw_json ->> 'by',
          raw_json ->> 'text',
          to_timestamp((raw_json ->> 'time')::bigint)
        FROM bronze
        WHERE poll_id = $pollId AND entity_type = 'comment'
        ON CONFLICT (comment_id) DO NOTHING
      """)
  }

  val live: ZLayer[ZConnectionPool, Nothing, SilverTransformer] =
    ZLayer.fromFunction(Live.apply)

  def transformStories(pollId: UUID): ZIO[SilverTransformer, Throwable, Long] =
    ZIO.serviceWithZIO[SilverTransformer](_.transformStories(pollId))

  def transformComments(pollId: UUID): ZIO[SilverTransformer, Throwable, Long] =
    ZIO.serviceWithZIO[SilverTransformer](_.transformComments(pollId))
}
