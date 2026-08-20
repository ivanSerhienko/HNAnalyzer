package storage

import zio._
import zio.jdbc._
import zio.test._

import java.time.Instant
import java.util.UUID

object SilverTransformerSpec extends ZIOSpecDefault {

  private def readStorySnapshot(storyId: Long, pollId: UUID) =
    ZIO.serviceWithZIO[ZConnectionPool] { pool =>
      sql"""SELECT title, url, domain, author, score, comment_count, story_type, submitted_at
            FROM story_snapshots WHERE story_id = $storyId AND poll_id = $pollId"""
        .query[(String, Option[String], Option[String], Option[String], Int, Int, String, Instant)]
        .selectOne
        .provideSomeLayer[Any](ZLayer.succeed(pool) >>> transaction)
    }

  private def readComment(commentId: Long) =
    ZIO.serviceWithZIO[ZConnectionPool] { pool =>
      sql"SELECT story_id, author, text, submitted_at FROM comments WHERE comment_id = $commentId"
        .query[(Long, Option[String], Option[String], Instant)]
        .selectOne
        .provideSomeLayer[Any](ZLayer.succeed(pool) >>> transaction)
    }

  private def countStorySnapshots(storyId: Long, pollId: UUID) =
    ZIO.serviceWithZIO[ZConnectionPool] { pool =>
      sql"SELECT count(*) FROM story_snapshots WHERE story_id = $storyId AND poll_id = $pollId"
        .query[Long]
        .selectOne
        .provideSomeLayer[Any](ZLayer.succeed(pool) >>> transaction)
        .map(_.getOrElse(0L))
    }

  def spec = suite("SilverTransformer")(
    test("transforms a bronze story row into a story_snapshot") {
      val pollId   = UUID.randomUUID()
      val polledAt = Instant.parse("2026-08-20T12:00:00Z")
      val storyId  = scala.util.Random.nextLong().abs

      val storyJson =
        s"""{"id":$storyId,"by":"tester","descendants":5,"score":42,"time":1755000000,"title":"A test story","type":"story","url":"https://example.com/article"}"""

      for {
        _        <- BronzeWriter.write(BronzeRow("story", storyId, Some("top"), pollId, polledAt, storyJson))
        _        <- SilverTransformer.transformStories(pollId)
        snapshot <- readStorySnapshot(storyId, pollId)
      } yield assertTrue(
        snapshot.contains(
          (
            "A test story",
            Some("https://example.com/article"),
            Some("example.com"),
            Some("tester"),
            42,
            5,
            "top",
            Instant.ofEpochSecond(1755000000L),
          ),
        ),
      )
    },
    test("strips a leading www. when deriving domain from url") {
      val pollId   = UUID.randomUUID()
      val polledAt = Instant.parse("2026-08-20T12:00:00Z")
      val storyId  = scala.util.Random.nextLong().abs

      val storyJson =
        s"""{"id":$storyId,"by":"tester","descendants":0,"score":1,"time":1755000000,"title":"t","type":"story","url":"https://www.example.com/article"}"""

      for {
        _        <- BronzeWriter.write(BronzeRow("story", storyId, Some("top"), pollId, polledAt, storyJson))
        _        <- SilverTransformer.transformStories(pollId)
        snapshot <- readStorySnapshot(storyId, pollId)
      } yield assertTrue(snapshot.map(_._3) == Some(Some("example.com")))
    },
    test("transforms a bronze comment row into a comment, using parent as story_id") {
      val pollId    = UUID.randomUUID()
      val polledAt  = Instant.parse("2026-08-20T12:00:00Z")
      val storyId   = scala.util.Random.nextLong().abs
      val commentId = scala.util.Random.nextLong().abs

      val commentJson =
        s"""{"id":$commentId,"by":"commenter","parent":$storyId,"text":"nice post","time":1755000100,"type":"comment"}"""

      for {
        _       <- BronzeWriter.write(BronzeRow("comment", commentId, None, pollId, polledAt, commentJson))
        _       <- SilverTransformer.transformComments(pollId)
        comment <- readComment(commentId)
      } yield assertTrue(
        comment.contains((storyId, Some("commenter"), Some("nice post"), Instant.ofEpochSecond(1755000100L))),
      )
    },
    test("re-running the same poll's transform does not duplicate story_snapshot rows") {
      val pollId   = UUID.randomUUID()
      val polledAt = Instant.parse("2026-08-20T12:00:00Z")
      val storyId  = scala.util.Random.nextLong().abs

      val storyJson =
        s"""{"id":$storyId,"by":"tester","descendants":0,"score":1,"time":1755000000,"title":"idempotency check","type":"story"}"""

      for {
        _     <- BronzeWriter.write(BronzeRow("story", storyId, Some("new"), pollId, polledAt, storyJson))
        _     <- SilverTransformer.transformStories(pollId)
        _     <- SilverTransformer.transformStories(pollId)
        count <- countStorySnapshots(storyId, pollId)
      } yield assertTrue(count == 1L)
    },
    test("transformComments' returned row count only reflects newly-inserted rows, not conflicts") {
      val pollId       = UUID.randomUUID()
      val polledAt     = Instant.parse("2026-08-20T12:00:00Z")
      val storyId      = scala.util.Random.nextLong().abs
      val existingId   = scala.util.Random.nextLong().abs
      val newId        = scala.util.Random.nextLong().abs
      val existingJson =
        s"""{"id":$existingId,"by":"a","parent":$storyId,"text":"old","time":1755000000,"type":"comment"}"""
      val newJson      =
        s"""{"id":$newId,"by":"b","parent":$storyId,"text":"new","time":1755000100,"type":"comment"}"""

      for {
        // Seed `existingId` into comments via a first, separate poll cycle.
        seedPollId <- ZIO.succeed(UUID.randomUUID())
        _          <- BronzeWriter.write(BronzeRow("comment", existingId, None, seedPollId, polledAt, existingJson))
        _          <- SilverTransformer.transformComments(seedPollId)

        // Now a batch that re-sees `existingId` (already in comments) alongside a genuinely new `newId`.
        _         <- BronzeWriter.write(BronzeRow("comment", existingId, None, pollId, polledAt, existingJson))
        _         <- BronzeWriter.write(BronzeRow("comment", newId, None, pollId, polledAt, newJson))
        inserted  <- SilverTransformer.transformComments(pollId)
      } yield assertTrue(inserted == 1L)
    },
  ).provideLayerShared(Db.pool >+> (BronzeWriter.live ++ SilverTransformer.live))
}
